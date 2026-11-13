// 调用记录的写入与查询。
package store

import (
	"database/sql"
	"time"
	"errors"
	"fmt"
	"strings"
)

// Call 一次调用的主记录；Body 字段可能被截断（截断后加后缀标记）。
type Call struct {
	ID               int64   `json:"id"`
	VirtualKeyID     *int64  `json:"virtual_key_id"`
	VirtualKey       string  `json:"virtual_key"`
	UpstreamName     string  `json:"upstream"`
	UpstreamKeyLabel string  `json:"upstream_key_label"`
	Method           string  `json:"method"`
	Path             string  `json:"path"`
	UpstreamURL      string  `json:"upstream_url"`
	SessionID        string  `json:"session_id"` // 客户端会话标识（如 Codex Session-Id），空=未识别
	Client           string  `json:"client"`     // 客户端标识（如 Codex Desktop）
	TurnID           string  `json:"turn_id"`    // 轮次标识（如 Codex turn_id），空=未提供
	LastMsgRole      string  `json:"last_msg_role"` // 请求末条消息类型（user/tool/tool_call...），空=未解析
	Model            string  `json:"model"`
	StatusCode       *int    `json:"status_code"`
	ReqHeaders       string  `json:"req_headers"`
	ReqBody          string  `json:"req_body"`
	RespHeaders      string  `json:"resp_headers"`
	RespBody         string  `json:"resp_body"`
	Stream           bool    `json:"stream"`
	ChunkCount       int     `json:"chunk_count"`
	PromptTokens     *int64  `json:"prompt_tokens"`
	CompletionTokens *int64  `json:"completion_tokens"`
	TotalTokens      *int64  `json:"total_tokens"`
	PromptCached     *int64  `json:"prompt_cached_tokens"`
	CostCNY          *float64 `json:"cost_cny"`
	CostPoints       *float64 `json:"cost_points"`
	Error            string  `json:"error"`
	StartedAt        int64   `json:"started_at"`     // 收到请求
	UpstreamMS       *int64  `json:"upstream_ms"`    // 发往上游偏移
	FirstByteMS      *int64  `json:"first_byte_ms"`  // 上游首字节偏移
	EndedAt          *int64  `json:"ended_at"`
	DurationMS       int64   `json:"duration_ms"`
}

// Chunk SSE 分包明细。
type Chunk struct {
	ID        int64  `json:"id"`
	CallID    int64  `json:"call_id"`
	Seq       int    `json:"seq"`
	TOffsetMS int64  `json:"t_offset_ms"`
	Data      string `json:"data"`
}

// NewCall 待写入的调用记录。
type NewCall struct {
	VirtualKeyID     *int64
	UpstreamID       *int64
	UpstreamKeyID    *int64
	Method           string
	Path             string
	UpstreamURL      string
	SessionID        string
	Client           string
	TurnID           string
	LastMsgRole      string
	Model            string
	StatusCode       *int
	ReqHeaders       string
	ReqBody          string
	RespHeaders      string
	RespBody         string
	Stream           bool
	ChunkCount       int
	PromptTokens     *int64
	CompletionTokens *int64
	TotalTokens      *int64
	PromptCached     *int64
	CostCNY          *float64
	CostPoints       *float64
	Error            string
	StartedAt        int64 // 毫秒
	UpstreamMS       *int64
	FirstByteMS      *int64
	EndedAt          *int64
}

// InsertCall 写入调用记录并返回 id。
func (s *Store) InsertCall(c NewCall) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO calls(virtual_key_id, upstream_id, upstream_key_id, method, path, upstream_url, session_id, client, turn_id, last_msg_role, model,
		status_code, req_headers, req_body, resp_headers, resp_body, stream, chunk_count,
		prompt_tokens, completion_tokens, total_tokens, prompt_cached_tokens, cost_cny, cost_points, error,
		started_at, upstream_ms, first_byte_ms, ended_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		c.VirtualKeyID, c.UpstreamID, c.UpstreamKeyID, c.Method, c.Path, c.UpstreamURL, c.SessionID, c.Client, c.TurnID, c.LastMsgRole, c.Model,
		c.StatusCode, c.ReqHeaders, c.ReqBody, c.RespHeaders, c.RespBody, c.Stream, c.ChunkCount,
		c.PromptTokens, c.CompletionTokens, c.TotalTokens, c.PromptCached, c.CostCNY, c.CostPoints, c.Error,
		c.StartedAt, c.UpstreamMS, c.FirstByteMS, c.EndedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// InsertChunks 流结束后批量写入分包明细。
func (s *Store) InsertChunks(callID int64, chunks []Chunk) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO chunks(call_id, seq, t_offset_ms, data) VALUES(?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, c := range chunks {
		if _, err := stmt.Exec(callID, c.Seq, c.TOffsetMS, c.Data); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// CallFilter 日志列表筛选与排序。
type CallFilter struct {
	VirtualKeyID *int64
	Model        string
	Status       string // "ok" / "error"，空为全部
	FromMS       int64  // started_at >= from
	ToMS         int64  // started_at < to
	Limit        int
	Offset       int
}

const callCols = `c.id, c.virtual_key_id, vk.key, u.name, k.label,
	c.method, c.path, c.upstream_url, c.session_id, c.client, c.turn_id, c.last_msg_role, c.model, c.status_code, c.req_headers, c.req_body, c.resp_headers, c.resp_body,
	c.stream, c.chunk_count, c.prompt_tokens, c.completion_tokens, c.total_tokens, c.prompt_cached_tokens,
	c.cost_cny, c.cost_points, c.error,
	c.started_at, c.upstream_ms, c.first_byte_ms, c.ended_at, COALESCE(c.ended_at,0) - c.started_at`

func scanCall(row interface{ Scan(...any) error }) (*Call, error) {
	c := &Call{}
	err := row.Scan(&c.ID, &c.VirtualKeyID, &c.VirtualKey, &c.UpstreamName, &c.UpstreamKeyLabel,
		&c.Method, &c.Path, &c.UpstreamURL, &c.SessionID, &c.Client, &c.TurnID, &c.LastMsgRole, &c.Model, &c.StatusCode, &c.ReqHeaders, &c.ReqBody, &c.RespHeaders, &c.RespBody,
		&c.Stream, &c.ChunkCount, &c.PromptTokens, &c.CompletionTokens, &c.TotalTokens, &c.PromptCached,
		&c.CostCNY, &c.CostPoints, &c.Error,
		&c.StartedAt, &c.UpstreamMS, &c.FirstByteMS, &c.EndedAt, &c.DurationMS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return c, err
}

func (f CallFilter) where() (string, []any) {
	conds := []string{"1=1"}
	var args []any
	if f.VirtualKeyID != nil {
		conds = append(conds, "c.virtual_key_id = ?")
		args = append(args, *f.VirtualKeyID)
	}
	if f.Model != "" {
		conds = append(conds, "c.model LIKE ?")
		args = append(args, "%"+f.Model+"%")
	}
	if f.Status == "ok" {
		conds = append(conds, "c.error = '' AND c.status_code >= 200 AND c.status_code < 400")
	} else if f.Status == "error" {
		conds = append(conds, "(c.error != '' OR c.status_code >= 400 OR c.status_code IS NULL)")
	}
	if f.FromMS > 0 {
		conds = append(conds, "c.started_at >= ?")
		args = append(args, f.FromMS)
	}
	if f.ToMS > 0 {
		conds = append(conds, "c.started_at < ?")
		args = append(args, f.ToMS)
	}
	return strings.Join(conds, " AND "), args
}

// ListCalls 按时间倒序分页查询调用记录；列表不带 body（避免响应过大）。
func (s *Store) ListCalls(f CallFilter) ([]Call, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 50
	}
	where, args := f.where()
	rows, err := s.db.Query(`SELECT `+callCols+` FROM calls c
		LEFT JOIN virtual_keys vk ON vk.id = c.virtual_key_id
		LEFT JOIN upstreams u ON u.id = c.upstream_id
		LEFT JOIN upstream_keys k ON k.id = c.upstream_key_id
		WHERE `+where+` ORDER BY c.started_at DESC LIMIT ? OFFSET ?`,
		append(args, f.Limit, f.Offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Call
	for rows.Next() {
		c, err := scanCall(rows)
		if err != nil {
			return nil, err
		}
		// 列表视图隐藏大 body，详情接口单独取
		c.ReqBody, c.RespBody, c.ReqHeaders, c.RespHeaders = "", "", "", ""
		out = append(out, *c)
	}
	return out, rows.Err()
}

// CallSession 会话分组的调用列表：会话级分页的返回单元。
type CallSession struct {
	SessionID string `json:"session_id"`
	Client    string `json:"client"`
	Calls     []Call `json:"calls"`
}

// ListCallSessions 按 session 分组、组内最新时间倒序分页，返回页数据与会话总数；
// 会话内请求全量返回（单会话量级可控，配合前端 turn 折叠展示）。
func (s *Store) ListCallSessions(vkeyID int64, limit, offset int) ([]CallSession, int, error) {
	if limit <= 0 || limit > 20 {
		limit = 5
	}
	if offset < 0 {
		offset = 0
	}
	var total int
	if err := s.db.QueryRow(`SELECT COUNT(DISTINCT session_id) FROM calls WHERE virtual_key_id = ?`, vkeyID).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.Query(`SELECT session_id FROM calls WHERE virtual_key_id = ?
		GROUP BY session_id ORDER BY MAX(started_at) DESC LIMIT ? OFFSET ?`, vkeyID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if len(ids) == 0 {
		return nil, total, nil
	}
	// 页内会话的请求全量：IN 逐会话聚合，保持分组顺序
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids)+1)
	args = append(args, vkeyID)
	for _, id := range ids {
		args = append(args, id)
	}
	crows, err := s.db.Query(`SELECT `+callCols+` FROM calls c
		LEFT JOIN virtual_keys vk ON vk.id = c.virtual_key_id
		LEFT JOIN upstreams u ON u.id = c.upstream_id
		LEFT JOIN upstream_keys k ON k.id = c.upstream_key_id
		WHERE c.virtual_key_id = ? AND c.session_id IN (`+ph+`) ORDER BY c.started_at DESC`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer crows.Close()
	byID := make(map[string]*CallSession, len(ids))
	order := make([]CallSession, 0, len(ids))
	ptrs := make([]*CallSession, 0, len(ids))
	for _, id := range ids {
		cs := &CallSession{SessionID: id}
		byID[id] = cs
		ptrs = append(ptrs, cs)
	}
	for crows.Next() {
		c, err := scanCall(crows)
		if err != nil {
			return nil, 0, err
		}
		c.ReqBody, c.RespBody, c.ReqHeaders, c.RespHeaders = "", "", "", ""
		cs := byID[c.SessionID]
		if cs.Client == "" {
			cs.Client = c.Client
		}
		cs.Calls = append(cs.Calls, *c)
	}
	for _, p := range ptrs {
		order = append(order, *p)
	}
	return order, total, crows.Err()
}

// GetCall 单条详情（含完整请求/响应）。
func (s *Store) GetCall(id int64) (*Call, error) {
	return scanCall(s.db.QueryRow(`SELECT `+callCols+` FROM calls c
		LEFT JOIN virtual_keys vk ON vk.id = c.virtual_key_id
		LEFT JOIN upstreams u ON u.id = c.upstream_id
		LEFT JOIN upstream_keys k ON k.id = c.upstream_key_id
		WHERE c.id = ?`, id))
}

// GetChunks 某次调用的分包明细。
func (s *Store) GetChunks(callID int64) ([]Chunk, error) {
	rows, err := s.db.Query(`SELECT id, call_id, seq, t_offset_ms, data FROM chunks WHERE call_id=? ORDER BY seq`, callID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.ID, &c.CallID, &c.Seq, &c.TOffsetMS, &c.Data); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpstreamKeyWithUpstream 转发时由 key id 反查 key 与上游地址；protocol 非空时取该协议端点的 api，
// 协议对不上（无此端点）返回 ErrNotFound，调用侧据此拒绝。
func (s *Store) UpstreamKeyWithUpstream(keyID int64, protocol string) (key string, baseURL string, upstreamID int64, err error) {
	row := s.db.QueryRow(`SELECT k.key, u.id FROM upstream_keys k
		JOIN upstreams u ON u.id = k.upstream_id WHERE k.id = ? AND k.enabled = 1`, keyID)
	if err := row.Scan(&key, &upstreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", 0, ErrNotFound
		}
		return "", "", 0, fmt.Errorf("查询 upstream key: %w", err)
	}
	if protocol == "" {
		// 未指定协议：默认端点
		err = s.db.QueryRow(`SELECT api FROM upstream_endpoints WHERE upstream_id=? AND is_default=1`, upstreamID).Scan(&baseURL)
		if errors.Is(err, sql.ErrNoRows) { // 无端点记录时退回 upstreams.base_url
			err = s.db.QueryRow(`SELECT base_url FROM upstreams WHERE id=?`, upstreamID).Scan(&baseURL)
		}
	} else {
		err = s.db.QueryRow(`SELECT api FROM upstream_endpoints WHERE upstream_id=? AND protocol=?`, upstreamID, protocol).Scan(&baseURL)
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", 0, fmt.Errorf("服务商不支持协议 %q: %w", protocol, ErrNotFound)
		}
	}
	if err != nil {
		return "", "", 0, err
	}
	return key, baseURL, upstreamID, nil
}

// BumpCallStats 归档后增量统计一次调用。
func (s *Store) BumpCallStats(vkeyID int64, model string, at int64, ok bool, stream bool, prompt, completion int64, costCNY, costPoints *float64) {
	s.BumpUKStats(vkeyID, 0, model, at, ok, stream, prompt, completion, 0, costCNY, costPoints)
}

// BumpUKStats 真实 Key 维度统计（upstreamKeyID>0 时写 stats_*_uk）；转发归档处传入真实 key。
func (s *Store) BumpUKStats(vkeyID, upstreamKeyID int64, model string, at int64, ok bool, stream bool, prompt, completion, cached int64, costCNY, costPoints *float64) {
	r := StatRow{Calls: 1, PromptTokens: prompt, CompletionTokens: completion}
	r.PromptCached = cached
	if ok {
		r.OK = 1
	} else {
		r.Fail = 1
	}
	if stream {
		r.StreamCalls = 1
	}
	if costCNY != nil {
		r.CostCNY = *costCNY
	}
	if costPoints != nil {
		r.CostPoints = *costPoints
	}
	if costCNY == nil && costPoints == nil && prompt+completion > 0 {
		r.UnpricedCalls = 1
	}
	s.bumpStats(vkeyID, model, time.UnixMilli(at), r)
	if upstreamKeyID > 0 {
		s.bumpUKStats(upstreamKeyID, vkeyID, time.UnixMilli(at), r)
	}
}
