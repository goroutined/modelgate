// Package proxy 是转发核心：认证虚拟 key -> 策略解析真实 key -> 透传上游（SSE 流式）-> 记录归档 + 实时广播。
package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/goroutined/modelgate/internal/live"
	"github.com/goroutined/modelgate/internal/provider"
	"github.com/goroutined/modelgate/internal/store"
)

const (
	maxBodyRecord = 8 * 1024 * 1024 // 请求/响应体归档上限；Codex 长上下文请求可达数 MB，太小会导致归档 JSON 无法解析
	maxChunkLen   = 16 * 1024 // 单个 SSE 分包归档上限
)

// Handler 返回代理入口；client 为可替换的 HTTP 客户端（测试注入用）。
type Handler struct {
	store    *store.Store
	hub      *live.Hub
	provider *provider.Source // 可为 nil：无法计价
	client   *http.Client
}

// New 构造代理处理器；prov 可为 nil（跳过计价）。
func New(st *store.Store, hub *live.Hub, prov *provider.Source) *Handler {
	return &Handler{
		store:    st,
		hub:      hub,
		provider: prov,
		client: &http.Client{
			Timeout: 10 * time.Minute, // 长对话/SSE 需要较长时间；真正受限的是每 chunk 间隔
			Transport: &http.Transport{
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  false,
				MaxIdleConnsPerHost: 10,
			},
		},
	}
}

// ServeHTTP 处理所有非 /api、/healthz、/ 的调用请求。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	startMS := start.UnixMilli()

	// 1. 认证：Bearer 虚拟 key
	vkey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	vk, err := h.store.FindVirtualKey(vkey)
	if err != nil || !vk.Enabled {
		http.Error(w, `{"error":"invalid virtual key"}`, http.StatusUnauthorized)
		return
	}

	// 2. 策略解析：v1 固定映射 -> upstream_key_id
	if vk.PolicyID == nil {
		http.Error(w, `{"error":"virtual key 未绑定策略"}`, http.StatusForbidden)
		return
	}
	pol, err := h.store.GetPolicy(*vk.PolicyID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"策略不存在(id=%d)"}`, *vk.PolicyID), http.StatusForbidden)
		return
	}
	if pol.Type != "fixed" {
		http.Error(w, fmt.Sprintf(`{"error":"策略类型 %q 暂不支持"}`, pol.Type), http.StatusNotImplemented)
		return
	}
	var cfg struct {
		UpstreamKeyID int64  `json:"upstream_key_id"`
		Protocol      string `json:"protocol"` // 为空用默认端点
	}
	if err := json.Unmarshal([]byte(pol.Config), &cfg); err != nil || cfg.UpstreamKeyID == 0 {
		http.Error(w, `{"error":"策略 config 无效，需要 upstream_key_id"}`, http.StatusInternalServerError)
		return
	}
	// 协议路由：按请求路径判定客户端实际使用的协议；策略显式指定协议时必须匹配
	pathProto := protoFromPath(r.URL.Path)
	if cfg.Protocol != "" && cfg.Protocol != pathProto {
		http.Error(w, fmt.Sprintf(`{"error":"策略指定协议 %q 与请求路径 %q 的协议 %q 不匹配"}`, cfg.Protocol, r.URL.Path, pathProto), http.StatusBadRequest)
		return
	}
	realKey, baseURL, upstreamID, err := h.store.UpstreamKeyWithUpstream(cfg.UpstreamKeyID, pathProto)
	if err != nil {
		http.Error(w, `{"error":"真实 key 或协议端点不可用：`+sanitizeErr(err)+`"}`, http.StatusServiceUnavailable)
		return
	}

	// 3. 全量读取请求体：转发必须完整（截断会让上游收到非法 JSON），
	//    maxBodyRecord 上限只用于入库归档，不影响转发
	reqBody, _ := io.ReadAll(r.Body)
	model := extractModel(reqBody)
	// 注意：必须在 copyHeaders 之前归档请求头（copyHeaders 会从原 header 删除 Authorization）
	reqHeadersRedacted := redactHeaders(r.Header)

	h.hub.Publish("call_start", map[string]any{
		"call_id": 0, // 占位，结束事件里有真实 id；实时视图主要看 chunk
		"vkey":    vk.Key,
		"model":   model,
		"path":    r.URL.Path,
		"stream":  bodyIsStream(reqBody),
	})

	// 4. 构造上游请求：剥离客户端挂载前缀（/v1、/anthropic）再拼 baseURL，
	//    modellink 的 baseURL 已含版本段（如 …/v4、…/v1），原样拼接会出现 /v1/v1 双重路径
	target := strings.TrimRight(baseURL, "/") + stripMount(r.URL.Path, pathProto)
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	upReq, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(reqBody))
	if err != nil {
		h.recordFailure(vk, upstreamID, cfg.UpstreamKeyID, r, model, string(reqBody), target, err, startMS)
		http.Error(w, `{"error":"构造上游请求失败"}`, http.StatusBadGateway)
		return
	}
	copyHeaders(upReq.Header, r.Header)
	upReq.Header.Set("Authorization", "Bearer "+realKey)
	upReq.Header.Del("Accept-Encoding") // 避免上游 gzip 后我们无法逐 chunk 解析

	resp, err := h.client.Do(upReq)
	upstreamMS := time.Since(start).Milliseconds() // 上游响应头到达（近似发出+首包）
	if err != nil {
		h.recordFailure(vk, upstreamID, cfg.UpstreamKeyID, r, model, string(reqBody), target, err, startMS)
		http.Error(w, fmt.Sprintf(`{"error":"上游请求失败: %s"}`, sanitizeErr(err)), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// 5. 透传响应头与状态码
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	isSSE := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	rec := callRecorder{
		store: h.store, hub: h.hub,
		upstreamName: h.store.UpstreamNameByID(upstreamID),
		cost: func(pid, model string, u provider.Usage) provider.CostResult {
			if h.provider == nil {
				return provider.CostResult{}
			}
			return h.provider.Cost(pid, model, u, start)
		},
		findModel: func() *store.UpstreamModel { return h.store.FindModel(upstreamID, model) },
		vk: vk, upstreamID: upstreamID, keyID: cfg.UpstreamKeyID,
		method: r.Method, path: r.URL.Path, model: model,
		upstreamURL: target,
		reqBody: truncate(string(reqBody), maxBodyRecord), reqHeaders: reqHeadersRedacted,
		respHeaders: redactHeaders(resp.Header),
		startMS: startMS, upstreamMS: upstreamMS, stream: isSSE,
	}

	if isSSE {
		rec.pipeSSE(w, resp.Body, start)
	} else {
		rec.pipePlain(w, resp.Body, resp.StatusCode, start)
	}
	rec.finish(resp.StatusCode)
}

// ---------- 记录器：归档 + 实时广播 ----------

type callRecorder struct {
	store       *store.Store
	hub         *live.Hub
	upstreamName string
	cost        func(providerID, model string, u provider.Usage) provider.CostResult
	findModel   func() *store.UpstreamModel
	vk         *store.VirtualKey
	upstreamID int64
	keyID      int64
	method     string
	path       string
	upstreamURL string
	model      string
	reqBody     string
	reqHeaders  string
	respBody    bytes.Buffer
	respHeaders string
	startMS    int64
	upstreamMS int64
	firstByte  int64 // 上游首字节偏移；0=未读到
	stream     bool
	status     int
	chunks     []store.Chunk
	usage      usageInfo
	callID     int64
}

type usageInfo struct {
	Prompt      int64 `json:"prompt_tokens"`
	Completion  int64 `json:"completion_tokens"`
	Total       int64 `json:"total_tokens"`
	CachedInput int64 // 命中缓存的输入 tokens（cached_tokens）
}

// pipeSSE 逐行透传 SSE：每个 data 行转发、归档、广播，带到达时间偏移。
func (c *callRecorder) pipeSSE(w http.ResponseWriter, body io.Reader, recStart time.Time) {
	flusher, _ := w.(http.Flusher)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	start := time.Now()
	var seq int
	firstByteDone := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if !firstByteDone { // 首字节：第一个数据行到达
			c.firstByte = time.Since(recStart).Milliseconds()
			firstByteDone = true
		}
		// 原样转发（含空行，保持 SSE 语义）
		w.Write(line)
		w.Write([]byte("\n"))
		if flusher != nil {
			flusher.Flush()
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[5:])
		if bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		seq++
		c.usage.mergeUsage(data)
		off := time.Since(start).Milliseconds()
		ch := store.Chunk{Seq: seq, TOffsetMS: off, Data: truncate(string(data), maxChunkLen)}
		c.chunks = append(c.chunks, ch)
		c.hub.Publish("chunk", map[string]any{
			"call_id":     c.callID,
			"vkey":        c.vk.Key,
			"model":       c.model,
			"seq":         seq,
			"t_offset_ms": off,
			"data":        ch.Data,
		})
	}
}

// pipePlain 非流式：整体读回（归档截断）再写回客户端。
func (c *callRecorder) pipePlain(w http.ResponseWriter, body io.Reader, status int, recStart time.Time) {
	c.firstByte = time.Since(recStart).Milliseconds() // 非流式：读完即全部到达
	buf, _ := io.ReadAll(io.LimitReader(body, maxBodyRecord+1))
	c.respBody.Write(buf)
	c.usage.mergeUsage(buf)
	w.Write(buf)
}

// computeCost 计价：modellink 官方价格优先，自定义服务商模型价格回退，均无则返回 nil（未计价）。
func (c *callRecorder) computeCost() (*float64, *float64) {
	if c.usage.Total <= 0 && c.usage.Prompt+c.usage.Completion <= 0 {
		return nil, nil
	}
	if c.cost != nil {
		if r := c.cost(c.upstreamName, c.model, provider.Usage{
			PromptTokens: c.usage.Prompt, CompletionTokens: c.usage.Completion,
			CachedInputTokens: c.usage.CachedInput,
		}); r.OK {
			if r.Points > 0 {
				return nil, &r.Points
			}
			return &r.CNY, nil
		}
	}
	// 回退：自定义服务商模型价格（元/百万 token 手算）
	if m := c.findModel(); m != nil {
		in := float64(c.usage.Prompt) / 1e6
		out := float64(c.usage.Completion) / 1e6
		cacheRead := float64(c.usage.CachedInput) / 1e6
		total := 0.0
		if m.CostCNYIn != nil {
			total += in * *m.CostCNYIn
		}
		if m.CostCNYOut != nil {
			total += out * *m.CostCNYOut
		}
		if m.CostCNYCacheRead != nil {
			total += cacheRead * *m.CostCNYCacheRead
		}
		if total > 0 {
			return &total, nil
		}
	}
	return nil, nil
}

// mergeUsage 尽力从 JSON 中提取 usage 字段（流式取最后一个 chunk 的 usage）。
func (u *usageInfo) mergeUsage(data []byte) {
	type usage struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		InputTokens      int64 `json:"input_tokens"` // anthropic / responses API 的 prompt 对应字段
		CompletionTokens int64 `json:"completion_tokens"`
		OutputTokens     int64 `json:"output_tokens"` // responses API 的 completion 对应字段
		TotalTokens      int64 `json:"total_tokens"`
		PromptDetails    *struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
		InputDetails *struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	}
	// anthropic: usage 里 input/output tokens 单位不同字段名，这里只认 total_tokens>0 的通用结构
	var m struct {
		Usage *usage `json:"usage"`
		// Responses API：最后一个 response.completed 事件里 usage 挂在 response 下
		Response *struct {
			Usage *usage `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &m) == nil {
		got := m.Usage
		if got == nil && m.Response != nil {
			got = m.Response.Usage
		}
		if got != nil && (got.TotalTokens > 0 || (got.InputTokens > 0 && got.OutputTokens > 0)) {
			u.Prompt = got.PromptTokens
			if u.Prompt == 0 {
				u.Prompt = got.InputTokens
			}
			u.Completion = got.CompletionTokens
			if u.Completion == 0 {
				u.Completion = got.OutputTokens
			}
			u.Total = got.TotalTokens
			if u.Total == 0 {
				u.Total = u.Prompt + u.Completion // anthropic 无 total，自行求和
			}
			if got.PromptDetails != nil {
				u.CachedInput = got.PromptDetails.CachedTokens
			}
			if got.InputDetails != nil {
				u.CachedInput = got.InputDetails.CachedTokens
			}
		}
	}
}

// finish 流结束后写库归档。
func (c *callRecorder) finish(status int) {
	end := time.Now().UnixMilli()
	costCNY, costPoints := c.computeCost()
	nc := store.NewCall{
		VirtualKeyID: &c.vk.ID, UpstreamID: &c.upstreamID, UpstreamKeyID: &c.keyID,
		Method: c.method, Path: c.path, UpstreamURL: c.upstreamURL, Model: c.model, StatusCode: &status,
		ReqHeaders: c.reqHeaders, ReqBody: c.reqBody,
		RespHeaders: c.respHeaders, RespBody: truncate(c.respBody.String(), maxBodyRecord),
		Stream: c.stream, ChunkCount: len(c.chunks),
		PromptTokens: &c.usage.Prompt, CompletionTokens: &c.usage.Completion, TotalTokens: &c.usage.Total,
		PromptCached: func() *int64 { if c.usage.CachedInput > 0 { return &c.usage.CachedInput }; return nil }(),
		CostCNY: costCNY, CostPoints: costPoints,
		StartedAt: c.startMS, UpstreamMS: &c.upstreamMS, FirstByteMS: &c.firstByte, EndedAt: &end,
	}
	id, err := c.store.InsertCall(nc)
	if err != nil {
		// 归档失败不影响已完成的调用，仅记录日志
		log.Printf("proxy: 归档调用失败: %v", err)
		return
	}
	// 增量统计（与归档独立，失败不影响调用）
	ok := status >= 200 && status < 400
	c.store.BumpUKStats(c.vk.ID, c.keyID, c.model, c.startMS, ok, c.stream,
		c.usage.Prompt, c.usage.Completion, c.usage.CachedInput, costCNY, costPoints)
	c.callID = id
	if len(c.chunks) > 0 {
		if err := c.store.InsertChunks(id, c.chunks); err != nil {
			log.Printf("proxy: 归档分包失败 call=%d: %v", id, err)
		}
	}
	c.hub.Publish("call_end", map[string]any{
		"call_id": id, "vkey": c.vk.Key, "model": c.model,
		"status_code": status, "chunk_count": len(c.chunks),
		"duration_ms": end - c.startMS,
	})
}

// recordFailure 上游未响应等失败场景也要留痕。
func (h *Handler) recordFailure(vk *store.VirtualKey, upstreamID, keyID int64, r *http.Request, model, reqBody, upstreamURL string, err error, startMS int64) {
	end := time.Now().UnixMilli()
	h.store.InsertCall(store.NewCall{
		VirtualKeyID: &vk.ID, UpstreamID: &upstreamID, UpstreamKeyID: &keyID,
		Method: r.Method, Path: r.URL.Path, UpstreamURL: upstreamURL, Model: model,
		ReqHeaders: redactHeaders(r.Header), ReqBody: truncate(reqBody, maxBodyRecord),
		Error: truncate(sanitizeErr(err), 1024), StartedAt: startMS, EndedAt: &end,
	})
	h.hub.Publish("call_end", map[string]any{
		"vkey": vk.Key, "model": model, "error": sanitizeErr(err), "duration_ms": end - startMS,
	})
}

// ---------- 工具 ----------

// protoFromPath 按客户端请求路径判定协议：/anthropic → anthropic-compatible；
// /responses 结尾 → openai-responses；其余（/v1/chat/completions 等）→ openai-compatible。
func protoFromPath(p string) string {
	if strings.HasPrefix(p, "/anthropic") {
		return "anthropic-compatible"
	}
	if strings.HasSuffix(p, "/responses") {
		return "openai-responses"
	}
	return "openai-compatible"
}

// stripMount 剥离客户端侧协议挂载前缀：客户端 baseURL 是 modelgate 根（/v1、/anthropic），
// 上游 baseURL 已含自己的版本段，二者不能叠加。
func stripMount(p, proto string) string {
	prefix := "/v1"
	if proto == "anthropic-compatible" {
		prefix = "/anthropic"
	}
	p = strings.TrimPrefix(p, prefix)
	if p == "" {
		p = "/"
	}
	return p
}

func copyHeaders(dst, src http.Header) {
	for _, skip := range []string{"Authorization", "Host", "Content-Length", "Connection", "Transfer-Encoding"} {
		src.Del(skip)
	}
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// redactHeaders 归档请求/响应头；Authorization 等敏感值脱敏。
func redactHeaders(h http.Header) string {
	cp := h.Clone()
	for _, k := range []string{"Authorization", "X-Api-Key", "Cookie", "Set-Cookie"} {
		if cp.Get(k) != "" {
			cp.Set(k, "***")
		}
	}
	b, _ := json.Marshal(cp)
	return string(b)
}

// extractModel 从请求体尽力提取 model 字段。
func extractModel(body []byte) string {
	var m struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &m) == nil {
		return m.Model
	}
	return ""
}

func bodyIsStream(body []byte) bool {
	var m struct {
		Stream bool `json:"stream"`
	}
	json.Unmarshal(body, &m)
	return m.Stream
}

// truncate 超长内容截断并加标记。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n...[truncated]"
}

// sanitizeErr 错误信息去掉可能的 URL 凭据，保留可读部分。
func sanitizeErr(err error) string {
	return strings.ReplaceAll(err.Error(), "\n", " ")
}
