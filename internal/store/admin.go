// Package store 的业务配置 CRUD：upstream / 真实 key / 虚拟 key / 策略。
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrNotFound = errors.New("not found")

func now() int64 { return time.Now().Unix() }

// Endpoint 协议端点：协议与 baseURL 一一对应。
type Endpoint struct {
	Protocol string `json:"protocol"`
	API      string `json:"api"`
	Default  bool   `json:"default"`
}

// Upstream 一个服务商配置；Endpoints 为各协议 baseURL。
// UpstreamModel 模型定义（自定义服务商维护；目录服务商展示 modellink 实时数据）。
type UpstreamModel struct {
	ID           int64   `json:"id"`
	UpstreamID   int64   `json:"upstream_id"`
	ModelID      string  `json:"model_id"`
	Name         string  `json:"name"`
	Description  string  `json:"description"`
	CostCNYIn         *float64 `json:"cost_cny_in"`   // 元/百万token，空=未配置
	CostCNYOut        *float64 `json:"cost_cny_out"`
	CostCNYCacheRead  *float64 `json:"cost_cny_cache_read"`
	CostCNYCacheWrite *float64 `json:"cost_cny_cache_write"`
	ContextLimit *int64  `json:"context_limit"`  // tokens，空=未配置
	MaxOutput    *int64  `json:"max_output"`
	Enabled      bool    `json:"enabled"`
	CreatedAt    int64   `json:"created_at"`
	UpdatedAt    int64   `json:"updated_at"`
}

type Upstream struct {
	ID         int64           `json:"id"`
	Name       string          `json:"name"`
	BaseURL    string          `json:"base_url"`
	Note       string          `json:"note"`
	PricingURL     string          `json:"pricing_url"`
	ConsoleURL string          `json:"console_url"`
	Models     []UpstreamModel `json:"models"`
	Endpoints  []Endpoint      `json:"endpoints"`
	CreatedAt  int64           `json:"created_at"`
	UpdatedAt  int64           `json:"updated_at"`
}

// ValidProtocols 支持的协议集合（与 modellink 对齐）。
var ValidProtocols = map[string]bool{
	"openai-compatible": true, "anthropic-compatible": true, "openai-responses": true,
}

// UpstreamKey 真实 key（资产账号）。
type UpstreamKey struct {
	ID        int64  `json:"id"`
	Upstream  string `json:"upstream"`
	Key       string `json:"key"`
	Label     string `json:"label"`
	Enabled   bool   `json:"enabled"`
	Status    string `json:"status"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// VirtualKey 对外暴露的虚拟 key。
type VirtualKey struct {
	ID        int64  `json:"id"`
	Key       string `json:"key"`
	Name      string `json:"name"`
	PolicyID  *int64 `json:"policy_id"`
	Enabled   bool   `json:"enabled"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// Policy 调用策略；v1 仅 fixed。
type Policy struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Config    string `json:"config"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

func isUnique(err error) bool { return err != nil && strings.Contains(err.Error(), "UNIQUE") }

// ---------- upstreams ----------

// CreateUpstream 创建服务商；endpoints 为空时 base_url 作为 openai-compatible 默认端点。
func (s *Store) CreateUpstream(name, baseURL, note string, endpoints []Endpoint, docURL, consoleURL string) (*Upstream, error) {
	if len(endpoints) == 0 {
		endpoints = []Endpoint{{Protocol: "openai-compatible", API: baseURL, Default: true}}
	}
	hasDefault := false
	for _, e := range endpoints {
		if !ValidProtocols[e.Protocol] {
			return nil, fmt.Errorf("不支持的协议 %q", e.Protocol)
		}
		if e.Default {
			hasDefault = true
		}
	}
	if !hasDefault {
		endpoints[0].Default = true
	}
	def := ""
	for _, e := range endpoints {
		if e.Default {
			def = e.API
			break
		}
	}
	t := now()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	res, err := tx.Exec(`INSERT INTO upstreams(name, base_url, note, pricing_url, console_url, created_at, updated_at) VALUES(?,?,?,?,?,?,?)`,
		name, def, note, docURL, consoleURL, t, t)
	if isUnique(err) {
		tx.Rollback()
		return nil, fmt.Errorf("upstream %q 已存在", name)
	}
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	for _, e := range endpoints {
		d := 0
		if e.Default {
			d = 1
		}
		if _, err := tx.Exec(`INSERT INTO upstream_endpoints(upstream_id, protocol, api, is_default, created_at, updated_at)
			VALUES(?,?,?,?,?,?)`, id, e.Protocol, e.API, d, t, t); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetUpstream(id)
}

func (s *Store) GetUpstream(id int64) (*Upstream, error) {
	u := &Upstream{}
	err := s.db.QueryRow(`SELECT id, name, base_url, note, pricing_url, console_url, created_at, updated_at FROM upstreams WHERE id=?`, id).
		Scan(&u.ID, &u.Name, &u.BaseURL, &u.Note, &u.PricingURL, &u.ConsoleURL, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	u.Models, err = s.listModels(id)
	if err != nil {
		return nil, err
	}
	u.Endpoints, err = s.listEndpoints(id)
	return u, err
}

const modelCols = `id, upstream_id, model_id, name, description, cost_cny_in, cost_cny_out, cost_cny_cache_read, cost_cny_cache_write, context_limit, max_output, enabled, created_at, updated_at`

func scanModel(row interface{ Scan(...any) error }) (*UpstreamModel, error) {
	m := &UpstreamModel{}
	err := row.Scan(&m.ID, &m.UpstreamID, &m.ModelID, &m.Name, &m.Description, &m.CostCNYIn, &m.CostCNYOut,
		&m.CostCNYCacheRead, &m.CostCNYCacheWrite,
		&m.ContextLimit, &m.MaxOutput, &m.Enabled, &m.CreatedAt, &m.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return m, err
}

func (s *Store) listModels(upstreamID int64) ([]UpstreamModel, error) {
	rows, err := s.db.Query(`SELECT `+modelCols+` FROM upstream_models WHERE upstream_id=? ORDER BY id`, upstreamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UpstreamModel{}
	for rows.Next() {
		m, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

func (s *Store) listEndpoints(upstreamID int64) ([]Endpoint, error) {
	rows, err := s.db.Query(`SELECT protocol, api, is_default FROM upstream_endpoints WHERE upstream_id=? ORDER BY id`, upstreamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Endpoint{}
	for rows.Next() {
		var e Endpoint
		var d int
		if err := rows.Scan(&e.Protocol, &e.API, &d); err != nil {
			return nil, err
		}
		e.Default = d == 1
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) ListUpstreams() ([]Upstream, error) {
	rows, err := s.db.Query(`SELECT id, name, base_url, note, pricing_url, console_url, created_at, updated_at FROM upstreams ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// 注意：单连接池下不能在 rows 遍历中再发查询，先收集再补端点
	out := []Upstream{}
	for rows.Next() {
		var u Upstream
		if err := rows.Scan(&u.ID, &u.Name, &u.BaseURL, &u.Note, &u.PricingURL, &u.ConsoleURL, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i := range out {
		if out[i].Endpoints, err = s.listEndpoints(out[i].ID); err != nil {
			return nil, err
		}
		if out[i].Models, err = s.listModels(out[i].ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// IsCustom 是否自定义服务商（custom- 前缀）；非自定义的来自 modellink 官方服务商数据，官方配置只读。
func IsCustom(name string) bool { return strings.HasPrefix(name, "custom-") }

// SyncUpstream 服务商数据同步：只刷新官方数据（端点/链接），返回是否有实际变化。
func (s *Store) SyncUpstream(id int64, endpoints []Endpoint, pricingURL, consoleURL string) (bool, error) {
	// 先比对，无变化不写库
	cur, err := s.GetUpstream(id)
	if err != nil {
		return false, err
	}
	same := cur.PricingURL == pricingURL && cur.ConsoleURL == consoleURL && len(cur.Endpoints) == len(endpoints)
	if same {
		for i, e := range cur.Endpoints {
			if e.Protocol != endpoints[i].Protocol || e.API != endpoints[i].API {
				same = false
				break
			}
		}
	}
	if same {
		return false, nil
	}
	t := now()
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(`UPDATE upstreams SET pricing_url=?, console_url=?, updated_at=? WHERE id=?`,
		pricingURL, consoleURL, t, id); err != nil {
		tx.Rollback()
		return false, err
	}
	if _, err := tx.Exec(`DELETE FROM upstream_endpoints WHERE upstream_id=?`, id); err != nil {
		tx.Rollback()
		return false, err
	}
	for _, e := range endpoints {
		if _, err := tx.Exec(`INSERT INTO upstream_endpoints(upstream_id, protocol, api, is_default, created_at, updated_at)
			VALUES(?,?,?,?,?,?)`, id, e.Protocol, e.API, boolInt(e.Default), t, t); err != nil {
			tx.Rollback()
			return false, err
		}
	}
	if _, err := tx.Exec(`UPDATE upstreams SET base_url=(SELECT api FROM upstream_endpoints WHERE upstream_id=? AND is_default=1 LIMIT 1) WHERE id=?`, id, id); err != nil {
		tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// UpdateUpstream 更新服务商；endpoints 非空时整体替换端点集。
// 官方服务商（非 custom-）配置只读：仅允许改显示名，其余字段保留原值。
func (s *Store) UpdateUpstream(id int64, name, note string, endpoints []Endpoint, docURL, consoleURL string) (*Upstream, error) {
	if cur, err := s.GetUpstream(id); err == nil && !IsCustom(cur.Name) {
		name = cur.Name
		if note == "" {
			note = cur.Note
		}
		endpoints = nil
		docURL, consoleURL = cur.PricingURL, cur.ConsoleURL
	}
	for _, e := range endpoints {
		if !ValidProtocols[e.Protocol] {
			return nil, fmt.Errorf("不支持的协议 %q", e.Protocol)
		}
	}
	def := ""
	for _, e := range endpoints {
		if e.Default {
			def = e.API
		}
	}
	if def == "" && len(endpoints) > 0 {
		endpoints[0].Default = true
		def = endpoints[0].API
	}
	t := now()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	res, err := tx.Exec(`UPDATE upstreams SET name=?, note=?, pricing_url=?, console_url=?, base_url=COALESCE(NULLIF(?, ''), base_url), updated_at=? WHERE id=?`,
		name, note, docURL, consoleURL, def, t, id)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		tx.Rollback()
		return nil, ErrNotFound
	}
	if len(endpoints) > 0 {
		if _, err := tx.Exec(`DELETE FROM upstream_endpoints WHERE upstream_id=?`, id); err != nil {
			tx.Rollback()
			return nil, err
		}
		for _, e := range endpoints {
			d := 0
			if e.Default {
				d = 1
			}
			if _, err := tx.Exec(`INSERT INTO upstream_endpoints(upstream_id, protocol, api, is_default, created_at, updated_at)
				VALUES(?,?,?,?,?,?)`, id, e.Protocol, e.API, d, t, t); err != nil {
				tx.Rollback()
				return nil, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetUpstream(id)
}

func (s *Store) DeleteUpstream(id int64) error {
	res, err := s.db.Exec(`DELETE FROM upstreams WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------- upstream_keys ----------

const upstreamKeyCols = `k.id, u.name, k.key, k.label, k.enabled, k.status, k.created_at, k.updated_at`

func scanUpstreamKey(row interface{ Scan(...any) error }) (*UpstreamKey, error) {
	k := &UpstreamKey{}
	err := row.Scan(&k.ID, &k.Upstream, &k.Key, &k.Label, &k.Enabled, &k.Status, &k.CreatedAt, &k.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return k, err
}

func (s *Store) CreateUpstreamKey(upstreamID int64, key, label string, enabled bool) (*UpstreamKey, error) {
	if _, err := s.GetUpstream(upstreamID); err != nil {
		return nil, err
	}
	t := now()
	res, err := s.db.Exec(`INSERT INTO upstream_keys(upstream_id, key, label, enabled, created_at, updated_at)
		VALUES(?,?,?,?,?,?)`, upstreamID, key, label, enabled, t, t)
	if isUnique(err) {
		return nil, fmt.Errorf("该 key 已存在于 upstream %d", upstreamID)
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return s.GetUpstreamKey(id)
}

func (s *Store) GetUpstreamKey(id int64) (*UpstreamKey, error) {
	row := s.db.QueryRow(`SELECT `+upstreamKeyCols+` FROM upstream_keys k JOIN upstreams u ON u.id=k.upstream_id WHERE k.id=?`, id)
	return scanUpstreamKey(row)
}

func (s *Store) ListUpstreamKeys() ([]UpstreamKey, error) {
	rows, err := s.db.Query(`SELECT ` + upstreamKeyCols + ` FROM upstream_keys k JOIN upstreams u ON u.id=k.upstream_id ORDER BY k.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UpstreamKey
	for rows.Next() {
		k, err := scanUpstreamKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *k)
	}
	return out, rows.Err()
}

// UpdateUpstreamKey 改备注与启停；key 本体不可改（删除重建）。
func (s *Store) UpdateUpstreamKey(id int64, label string, enabled bool) (*UpstreamKey, error) {
	res, err := s.db.Exec(`UPDATE upstream_keys SET label=?, enabled=?, updated_at=? WHERE id=?`,
		label, enabled, now(), id)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, ErrNotFound
	}
	return s.GetUpstreamKey(id)
}

func (s *Store) DeleteUpstreamKey(id int64) error {
	res, err := s.db.Exec(`DELETE FROM upstream_keys WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------- virtual_keys ----------

func scanVirtualKey(row interface{ Scan(...any) error }) (*VirtualKey, error) {
	v := &VirtualKey{}
	err := row.Scan(&v.ID, &v.Key, &v.Name, &v.PolicyID, &v.Enabled, &v.CreatedAt, &v.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return v, err
}

func (s *Store) CreateVirtualKey(key, name string, policyID *int64, enabled bool) (*VirtualKey, error) {
	if policyID != nil {
		if _, err := s.GetPolicy(*policyID); err != nil {
			return nil, err
		}
	}
	t := now()
	res, err := s.db.Exec(`INSERT INTO virtual_keys(key, name, policy_id, enabled, created_at, updated_at)
		VALUES(?,?,?,?,?,?)`, key, name, policyID, enabled, t, t)
	if isUnique(err) {
		return nil, fmt.Errorf("虚拟 key %q 已存在", key)
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return s.GetVirtualKey(id)
}

func (s *Store) GetVirtualKey(id int64) (*VirtualKey, error) {
	return scanVirtualKey(s.db.QueryRow(
		`SELECT id, key, name, policy_id, enabled, created_at, updated_at FROM virtual_keys WHERE id=?`, id))
}

// FindVirtualKey 按虚拟 key 本体查找，转发认证用。
func (s *Store) FindVirtualKey(key string) (*VirtualKey, error) {
	return scanVirtualKey(s.db.QueryRow(
		`SELECT id, key, name, policy_id, enabled, created_at, updated_at FROM virtual_keys WHERE key=?`, key))
}

func (s *Store) ListVirtualKeys() ([]VirtualKey, error) {
	rows, err := s.db.Query(`SELECT id, key, name, policy_id, enabled, created_at, updated_at FROM virtual_keys ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VirtualKey
	for rows.Next() {
		v, err := scanVirtualKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func (s *Store) UpdateVirtualKey(id int64, name string, policyID *int64, enabled bool) (*VirtualKey, error) {
	if policyID != nil {
		if _, err := s.GetPolicy(*policyID); err != nil {
			return nil, err
		}
	}
	res, err := s.db.Exec(`UPDATE virtual_keys SET name=?, policy_id=?, enabled=?, updated_at=? WHERE id=?`,
		name, policyID, enabled, now(), id)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, ErrNotFound
	}
	return s.GetVirtualKey(id)
}

func (s *Store) DeleteVirtualKey(id int64) error {
	res, err := s.db.Exec(`DELETE FROM virtual_keys WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------- policies ----------

func scanPolicy(row interface{ Scan(...any) error }) (*Policy, error) {
	p := &Policy{}
	err := row.Scan(&p.ID, &p.Name, &p.Type, &p.Config, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

// ValidPolicyTypes 策略类型集合；v1 仅 fixed。
var ValidPolicyTypes = map[string]bool{"fixed": true}

func (s *Store) CreatePolicy(name, typ, config string) (*Policy, error) {
	if !ValidPolicyTypes[typ] {
		return nil, fmt.Errorf("未知策略类型 %q", typ)
	}
	t := now()
	res, err := s.db.Exec(`INSERT INTO policies(name, type, config, created_at, updated_at) VALUES(?,?,?,?,?)`,
		name, typ, config, t, t)
	if isUnique(err) {
		return nil, fmt.Errorf("策略 %q 已存在", name)
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return s.GetPolicy(id)
}

func (s *Store) GetPolicy(id int64) (*Policy, error) {
	return scanPolicy(s.db.QueryRow(`SELECT id, name, type, config, created_at, updated_at FROM policies WHERE id=?`, id))
}

func (s *Store) ListPolicies() ([]Policy, error) {
	rows, err := s.db.Query(`SELECT id, name, type, config, created_at, updated_at FROM policies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *Store) UpdatePolicy(id int64, name, typ, config string) (*Policy, error) {
	if !ValidPolicyTypes[typ] {
		return nil, fmt.Errorf("未知策略类型 %q", typ)
	}
	res, err := s.db.Exec(`UPDATE policies SET name=?, type=?, config=?, updated_at=? WHERE id=?`,
		name, typ, config, now(), id)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, ErrNotFound
	}
	return s.GetPolicy(id)
}

func (s *Store) DeletePolicy(id int64) error {
	// 绑定中的虚拟 key 会失效调用，先检查并提示
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM virtual_keys WHERE policy_id=?`, id).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("仍有 %d 个虚拟 key 绑定该策略，先解绑再删除", n)
	}
	res, err := s.db.Exec(`DELETE FROM policies WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

var _ = time.Now // 保留 time 引用（now 已用）


// ---------- upstream_models ----------

// CreateModel 给服务商添加模型定义。
func (s *Store) CreateModel(upstreamID int64, m UpstreamModel) (*UpstreamModel, error) {
	if _, err := s.GetUpstream(upstreamID); err != nil {
		return nil, err
	}
	if m.ModelID == "" {
		return nil, fmt.Errorf("model_id 必填")
	}
	t := now()
	res, err := s.db.Exec(`INSERT INTO upstream_models(upstream_id, model_id, name, description,
		cost_cny_in, cost_cny_out, cost_cny_cache_read, cost_cny_cache_write, context_limit, max_output, enabled, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		upstreamID, m.ModelID, m.Name, m.Description,
		m.CostCNYIn, m.CostCNYOut, m.CostCNYCacheRead, m.CostCNYCacheWrite, m.ContextLimit, m.MaxOutput, m.Enabled, t, t)
	if isUnique(err) {
		return nil, fmt.Errorf("模型 %q 已存在于该服务商", m.ModelID)
	}
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return s.GetModel(id)
}

// ListUpstreamModels 服务商的模型列表。
func (s *Store) ListUpstreamModels(upstreamID int64) ([]UpstreamModel, error) { return s.listModels(upstreamID) }

func (s *Store) GetModel(id int64) (*UpstreamModel, error) {
	return scanModel(s.db.QueryRow(`SELECT `+modelCols+` FROM upstream_models WHERE id=?`, id))
}

func (s *Store) UpdateModel(id int64, m UpstreamModel) (*UpstreamModel, error) {
	res, err := s.db.Exec(`UPDATE upstream_models SET model_id=?, name=?, description=?,
		cost_cny_in=?, cost_cny_out=?, cost_cny_cache_read=?, cost_cny_cache_write=?, context_limit=?, max_output=?, enabled=?, updated_at=? WHERE id=?`,
		m.ModelID, m.Name, m.Description, m.CostCNYIn, m.CostCNYOut, m.CostCNYCacheRead, m.CostCNYCacheWrite, m.ContextLimit, m.MaxOutput, m.Enabled, now(), id)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, ErrNotFound
	}
	return s.GetModel(id)
}

func (s *Store) DeleteModel(id int64) error {
	res, err := s.db.Exec(`DELETE FROM upstream_models WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpstreamNameByID 由 id 查服务商名（计价定位用）。
func (s *Store) UpstreamNameByID(id int64) string {
	var name string
	s.db.QueryRow(`SELECT name FROM upstreams WHERE id=?`, id).Scan(&name)
	return name
}

// FindModel 查自定义服务商的模型定义（计价回退用）。
func (s *Store) FindModel(upstreamID int64, modelID string) *UpstreamModel {
	row := s.db.QueryRow(`SELECT `+modelCols+` FROM upstream_models WHERE upstream_id=? AND model_id=?`, upstreamID, modelID)
	m, err := scanModel(row)
	if err != nil {
		return nil
	}
	return m
}
