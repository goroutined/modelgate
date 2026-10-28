// Package api 提供管理面 REST API，挂在 /api/ 下。
// token 非空时要求 Authorization: Bearer <token>。
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/goroutined/modelgate/internal/provider"
	"github.com/goroutined/modelgate/internal/store"
)

// Server 管理面 API。
type Server struct {
	store   *store.Store
	token   string
	providers *provider.Source
}

// New 构造管理面路由；cat 可为 nil（则服务商数据接口返回不可用）。
func New(st *store.Store, token string, cat *provider.Source) http.Handler {
	s := &Server{store: st, token: token, providers: cat}
	mux := http.NewServeMux()

	// upstreams
	mux.HandleFunc("GET /api/upstreams", list(s.store.ListUpstreams))
	mux.HandleFunc("POST /api/upstreams", s.createUpstream)
	mux.HandleFunc("GET /api/upstreams/{id}", s.getUpstream)
	mux.HandleFunc("PUT /api/upstreams/{id}", s.updateUpstream)
	mux.HandleFunc("DELETE /api/upstreams/{id}", deleteFunc(s.deleteUpstream))

	// 模型定义（自定义服务商维护）
	mux.HandleFunc("GET /api/upstreams/{id}/models", s.listModels)
	mux.HandleFunc("POST /api/upstreams/{id}/models", s.createModel)
	mux.HandleFunc("PUT /api/models/{id}", s.updateModel)
	mux.HandleFunc("DELETE /api/models/{id}", s.deleteModel)

	// 真实 key
	mux.HandleFunc("GET /api/upstream-keys", list(s.store.ListUpstreamKeys))
	mux.HandleFunc("POST /api/upstream-keys", s.createUpstreamKey)
	mux.HandleFunc("PUT /api/upstream-keys/{id}", s.updateUpstreamKey)
	mux.HandleFunc("DELETE /api/upstream-keys/{id}", deleteFunc(s.deleteUpstreamKey))

	// 虚拟 key
	mux.HandleFunc("GET /api/virtual-keys", list(s.store.ListVirtualKeys))
	mux.HandleFunc("POST /api/virtual-keys", s.createVirtualKey)
	mux.HandleFunc("PUT /api/virtual-keys/{id}", s.updateVirtualKey)
	mux.HandleFunc("DELETE /api/virtual-keys/{id}", deleteFunc(s.deleteVirtualKey))

	// 策略
	mux.HandleFunc("GET /api/policies", list(s.store.ListPolicies))
	mux.HandleFunc("POST /api/policies", s.createPolicy)
	mux.HandleFunc("PUT /api/policies/{id}", s.updatePolicy)
	mux.HandleFunc("DELETE /api/policies/{id}", deleteFunc(s.deletePolicy))

	// modellink 官方模型服务商数据（添加服务商/真实 key 时选择用）
	mux.HandleFunc("GET /api/provider/list", s.providerList)
	mux.HandleFunc("GET /api/modellink/providers", s.providerList) // 兼容旧路径
	mux.HandleFunc("POST /api/provider/sync", s.providerSync)

	// 设置中心 / 统计
	mux.HandleFunc("GET /api/settings", s.getSettings)
	mux.HandleFunc("PUT /api/settings", s.putSettings)
	mux.HandleFunc("GET /api/stats", s.getStats)
	mux.HandleFunc("GET /api/stats/overview", s.getStatsOverview)
	mux.HandleFunc("GET /api/stats/vkeys", s.getVKeyStats)
	mux.HandleFunc("GET /api/stats/vkeys/{id}", s.getVKeySeries)
	mux.HandleFunc("GET /api/stats/upstream-keys", s.getUKStats)
	mux.HandleFunc("GET /api/stats/upstream-keys/{id}", s.getUKDetail)
	mux.HandleFunc("GET /api/calls/overview", s.getCallsOverview)
	mux.HandleFunc("POST /api/stats/rebuild", s.rebuildStats)
	mux.HandleFunc("POST /api/cleanup", s.runCleanup)

	// 调用日志
	mux.HandleFunc("GET /api/calls", s.listCalls)
	mux.HandleFunc("GET /api/calls/{id}", s.getCall)
	mux.HandleFunc("GET /api/calls/{id}/chunks", s.getChunks)

	return s.auth(mux)
}

func (s *Server) providerList(w http.ResponseWriter, r *http.Request) {
	if s.providers == nil {
		writeErr(w, http.StatusServiceUnavailable, "modellink 服务商数据未启用")
		return
	}
	provs, err := s.providers.Providers(r.Context())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"providers": nil, "error": "目录加载中或失败: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"providers": provs})
}

func (s *Server) listModels(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	ms, err := s.store.ListUpstreamModels(id)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ms)
}

type modelReq struct {
	ModelID      string   `json:"model_id"`
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	CostCNYIn         *float64 `json:"cost_cny_in"`
	CostCNYOut        *float64 `json:"cost_cny_out"`
	CostCNYCacheRead  *float64 `json:"cost_cny_cache_read"`
	CostCNYCacheWrite *float64 `json:"cost_cny_cache_write"`
	ContextLimit *int64   `json:"context_limit"`
	MaxOutput    *int64   `json:"max_output"`
	Enabled      *bool    `json:"enabled"`
}

func (m *modelReq) toModel() store.UpstreamModel {
	en := m.Enabled == nil || *m.Enabled
	return store.UpstreamModel{
		ModelID: m.ModelID, Name: m.Name, Description: m.Description,
		CostCNYIn: m.CostCNYIn, CostCNYOut: m.CostCNYOut,
		CostCNYCacheRead: m.CostCNYCacheRead, CostCNYCacheWrite: m.CostCNYCacheWrite,
		ContextLimit: m.ContextLimit, MaxOutput: m.MaxOutput, Enabled: en,
	}
}

func (s *Server) createModel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	req, err := decode[modelReq](r)
	if err != nil || req.ModelID == "" {
		writeErr(w, http.StatusBadRequest, "model_id 必填")
		return
	}
	m, err := s.store.CreateModel(id, req.toModel())
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

func (s *Server) updateModel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	req, err := decode[modelReq](r)
	if err != nil || req.ModelID == "" {
		writeErr(w, http.StatusBadRequest, "model_id 必填")
		return
	}
	m, err := s.store.UpdateModel(id, req.toModel())
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) deleteModel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	if err := s.store.DeleteModel(id); err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) providerSync(w http.ResponseWriter, r *http.Request) {
	if s.providers == nil {
		writeErr(w, http.StatusServiceUnavailable, "modellink 服务商数据未启用")
		return
	}
	res, err := s.providers.Sync(r.Context(), s.store)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.AllSettings())
}

func (s *Server) putSettings(w http.ResponseWriter, r *http.Request) {
	var m map[string]string
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		writeErr(w, http.StatusBadRequest, "无效 JSON")
		return
	}
	for k, v := range m {
		if _, ok := store.DefaultSettings[k]; !ok {
			writeErr(w, http.StatusBadRequest, "未知设置项 "+k)
			return
		}
		if err := s.store.SetSetting(k, v); err != nil {
			mapErr(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, s.store.AllSettings())
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("vkey_id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "vkey_id 必填")
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	daily, err := s.store.StatBuckets(id, true, days)
	if err != nil {
		mapErr(w, err)
		return
	}
	hourly, err := s.store.StatBuckets(id, false, days)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"daily": daily, "hourly": hourly})
}

func (s *Server) getStatsOverview(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := s.store.StatOverview(days)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (s *Server) getCallsOverview(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.LogOverview()
	if err != nil {
		mapErr(w, err)
		return
	}
	last := s.store.Setting("log.last_cleanup_at")
	writeJSON(w, http.StatusOK, map[string]any{"keys": rows, "last_cleanup_at": last, "call_count": s.store.CallCount()})
}

func (s *Server) rebuildStats(w http.ResponseWriter, r *http.Request) {
	var id int64
	if v := r.URL.Query().Get("vkey_id"); v != "" {
		id, _ = strconv.ParseInt(v, 10, 64)
	}
	if err := s.store.RebuildStats(id); err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) getVKeyStats(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := s.store.VKeyStats(days)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (s *Server) getVKeySeries(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	daily := r.URL.Query().Get("gran") != "hour"
	rows, err := s.store.StatBuckets(id, daily, days)
	if err != nil {
		mapErr(w, err)
		return
	}
	// 按桶聚合掉 model 维度，前端直接画
	agg := map[string]*store.StatRow{}
	for _, x := range rows {
		k, ok := agg[x.Bucket]
		if !ok {
			k = &store.StatRow{Bucket: x.Bucket}
			agg[x.Bucket] = k
		}
		k.Calls += x.Calls
		k.OK += x.OK
		k.Fail += x.Fail
		k.PromptTokens += x.PromptTokens
		k.CompletionTokens += x.CompletionTokens
		k.CostCNY += x.CostCNY
		k.CostPoints += x.CostPoints
		k.UnpricedCalls += x.UnpricedCalls
		k.PromptCached += x.PromptCached
	}
	out := make([]store.StatRow, 0, len(agg))
	for _, v := range agg {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bucket > out[j].Bucket })
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getUKStats(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := s.store.UKStats(days)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (s *Server) getUKDetail(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	daily := r.URL.Query().Get("gran") != "hour"
	series, err := s.store.UKSeries(id, daily, days)
	if err != nil {
		mapErr(w, err)
		return
	}
	dist, err := s.store.UKVKeyDist(id, days)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"series": series, "vkeys": dist})
}

func (s *Server) runCleanup(w http.ResponseWriter, r *http.Request) {
	res, err := s.store.Cleanup()
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) listCalls(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.CallFilter{Model: q.Get("model"), Status: q.Get("status")}
	if v := q.Get("virtual_key_id"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "virtual_key_id 无效")
			return
		}
		f.VirtualKeyID = &id
	}
	if v := q.Get("from_ms"); v != "" {
		f.FromMS, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := q.Get("to_ms"); v != "" {
		f.ToMS, _ = strconv.ParseInt(v, 10, 64)
	}
	f.Limit, _ = strconv.Atoi(q.Get("limit"))
	f.Offset, _ = strconv.Atoi(q.Get("offset"))
	calls, err := s.store.ListCalls(f)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, calls)
}

func (s *Server) getCall(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	c, err := s.store.GetCall(id)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) getChunks(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	chunks, err := s.store.GetChunks(id)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, chunks)
}

// auth 管理面鉴权中间件；token 为空（本机场景）直接放行。
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" {
			// Bearer 头优先；WebSocket 无法带自定义头，支持 ?token=
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if got == "" {
				got = r.URL.Query().Get("token")
			}
			if got != s.token {
				writeErr(w, http.StatusUnauthorized, "无效的管理令牌")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ---------- 通用小工具 ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func pathID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

// decode 严格解析 body，禁止未知字段，避免误传写错配置。
func decode[T any](r *http.Request) (*T, error) {
	var v T
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}

// mapErr 将业务错误映射为 HTTP 状态码。
func mapErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "资源不存在")
	default:
		writeErr(w, http.StatusBadRequest, err.Error())
	}
}

// ---------- upstreams ----------

type upstreamReq struct {
	Name      string           `json:"name"`
	BaseURL   string           `json:"base_url"` // 兼容：单端点时的地址
	Note      string           `json:"note"`
	Protocol   string           `json:"protocol"`   // 自定义服务商：单协议
	Endpoints  []store.Endpoint `json:"endpoints"`  // 多协议端点（目录导入）
	PricingURL     string           `json:"pricing_url"`
	ConsoleURL string           `json:"console_url"`
}

func (s *Server) createUpstream(w http.ResponseWriter, r *http.Request) {
	req, err := decode[upstreamReq](r)
	if err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name 必填")
		return
	}
	eps := req.Endpoints
	if len(eps) == 0 {
		if req.BaseURL == "" {
			writeErr(w, http.StatusBadRequest, "base_url 或 endpoints 必填")
			return
		}
		proto := req.Protocol
		if proto == "" {
			proto = "openai-compatible"
		}
		eps = []store.Endpoint{{Protocol: proto, API: req.BaseURL, Default: true}}
	}
	u, err := s.store.CreateUpstream(req.Name, req.BaseURL, req.Note, eps, req.PricingURL, req.ConsoleURL)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, u)
}

func (s *Server) getUpstream(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	u, err := s.store.GetUpstream(id)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, u)
}

func (s *Server) updateUpstream(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	req, err := decode[upstreamReq](r)
	if err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name 必填")
		return
	}
	u, err := s.store.UpdateUpstream(id, req.Name, req.Note, req.Endpoints, req.PricingURL, req.ConsoleURL)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, u)
}

func (s *Server) deleteUpstream(r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	return s.store.DeleteUpstream(id)
}

// ---------- upstream keys ----------

type upstreamKeyReq struct {
	UpstreamID int64  `json:"upstream_id"`
	Key        string `json:"key"`
	Label      string `json:"label"`
	Enabled    *bool  `json:"enabled"`
}

func (s *Server) createUpstreamKey(w http.ResponseWriter, r *http.Request) {
	req, err := decode[upstreamKeyReq](r)
	if err != nil || req.Key == "" {
		writeErr(w, http.StatusBadRequest, "key 必填")
		return
	}
	enabled := req.Enabled == nil || *req.Enabled
	k, err := s.store.CreateUpstreamKey(req.UpstreamID, req.Key, req.Label, enabled)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, k)
}

func (s *Server) updateUpstreamKey(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	req, err := decode[upstreamKeyReq](r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	enabled := req.Enabled == nil || *req.Enabled
	k, err := s.store.UpdateUpstreamKey(id, req.Label, enabled)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, k)
}

func (s *Server) deleteUpstreamKey(r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	return s.store.DeleteUpstreamKey(id)
}

// ---------- virtual keys ----------

type virtualKeyReq struct {
	Key           string `json:"key"`
	Name          string `json:"name"`
	PolicyID      *int64 `json:"policy_id"`
	Enabled       *bool  `json:"enabled"`
	// 便捷创建：直接选真实 Key + 协议，后端自动生成 fixed 策略并绑定
	UpstreamKeyID *int64 `json:"upstream_key_id"`
	Protocol      string `json:"protocol"`
}

func (s *Server) createVirtualKey(w http.ResponseWriter, r *http.Request) {
	req, err := decode[virtualKeyReq](r)
	if err != nil || req.Key == "" {
		writeErr(w, http.StatusBadRequest, "key 必填")
		return
	}
	enabled := req.Enabled == nil || *req.Enabled
	policyID := req.PolicyID
	if req.UpstreamKeyID != nil {
		// 便捷路径：自动创建 fixed 策略（key + 协议）
		cfg, _ := json.Marshal(map[string]any{"upstream_key_id": *req.UpstreamKeyID, "protocol": req.Protocol})
		name := "auto-" + req.Key
		if req.Name != "" {
			name = "auto-" + req.Name
		}
		pol, err := s.store.CreatePolicy(name, "fixed", string(cfg))
		if err != nil {
			// 同名策略（重建同名 vkey）：复用已有
			pol, err = s.getOrCreatePolicyByName(name, "fixed", string(cfg))
			if err != nil {
				mapErr(w, err)
				return
			}
		}
		id := pol.ID
		policyID = &id
	}
	v, err := s.store.CreateVirtualKey(req.Key, req.Name, policyID, enabled)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (s *Server) getOrCreatePolicyByName(name, typ, config string) (*store.Policy, error) {
	pols, err := s.store.ListPolicies()
	if err != nil {
		return nil, err
	}
	for _, p := range pols {
		if p.Name == name {
			return s.store.UpdatePolicy(p.ID, name, typ, config)
		}
	}
	return s.store.CreatePolicy(name, typ, config)
}

func (s *Server) updateVirtualKey(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	req, err := decode[virtualKeyReq](r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	enabled := req.Enabled == nil || *req.Enabled
	v, err := s.store.UpdateVirtualKey(id, req.Name, req.PolicyID, enabled)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) deleteVirtualKey(r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	return s.store.DeleteVirtualKey(id)
}

// ---------- policies ----------

type policyReq struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Config string `json:"config"`
}

func (s *Server) createPolicy(w http.ResponseWriter, r *http.Request) {
	req, err := decode[policyReq](r)
	if err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name 必填")
		return
	}
	if req.Type == "" {
		req.Type = "fixed"
	}
	if req.Config == "" {
		req.Config = "{}"
	}
	p, err := s.store.CreatePolicy(req.Name, req.Type, req.Config)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (s *Server) updatePolicy(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无效 id")
		return
	}
	req, err := decode[policyReq](r)
	if err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name 必填")
		return
	}
	if req.Type == "" {
		req.Type = "fixed"
	}
	p, err := s.store.UpdatePolicy(id, req.Name, req.Type, req.Config)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) deletePolicy(r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	return s.store.DeletePolicy(id)
}

// ---------- 泛型适配（方法不能带类型参数，故为包级函数） ----------

// list 适配 ListXxx 方法。
func list[T any](fn func() ([]T, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out, err := fn()
		if err != nil {
			mapErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, out)
	}
}

// deleteFunc 适配 DeleteXxx 方法。
func deleteFunc(fn func(*http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(r); err != nil {
			mapErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}
