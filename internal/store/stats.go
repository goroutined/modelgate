// Package store 的统计与设置：小时/天两粒度增量统计、设置中心、水位线清理。
package store

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"time"
)

// StatRow 一行统计（hourly/daily 共用结构）。
type StatRow struct {
	Bucket           string  `json:"bucket"` // 天 "2026-10-18" 或小时 "2026-10-18 14"
	VirtualKeyID     int64   `json:"virtual_key_id"`
	Model            string  `json:"model"`
	Calls            int64   `json:"calls"`
	OK               int64   `json:"ok"`
	Fail             int64   `json:"fail"`
	StreamCalls      int64   `json:"stream_calls"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	CostCNY          float64 `json:"cost_cny"`
	CostPoints       float64 `json:"cost_points"`
	UnpricedCalls    int64   `json:"unpriced_calls"`
	PromptCached     int64   `json:"prompt_cached_tokens"`
}

const statCols = `calls, ok, fail, stream_calls, prompt_tokens, completion_tokens, cost_cny, cost_points, unpriced_calls, prompt_cached_tokens`

// bumpStats 把一次调用增量累加进小时/天两粒度统计（原子 upsert）。
func (s *Store) bumpStats(vkeyID int64, model string, at time.Time, r StatRow) {
	const dailySQL = `INSERT INTO stats_daily(day, virtual_key_id, model, calls, ok, fail, stream_calls,
		prompt_tokens, completion_tokens, cost_cny, cost_points, unpriced_calls, prompt_cached_tokens)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(day, virtual_key_id, model) DO UPDATE SET
		calls=calls+excluded.calls, ok=ok+excluded.ok, fail=fail+excluded.fail,
		stream_calls=stream_calls+excluded.stream_calls,
		prompt_tokens=prompt_tokens+excluded.prompt_tokens,
		completion_tokens=completion_tokens+excluded.completion_tokens,
		cost_cny=cost_cny+excluded.cost_cny, cost_points=cost_points+excluded.cost_points,
		unpriced_calls=unpriced_calls+excluded.unpriced_calls,
		prompt_cached_tokens=prompt_cached_tokens+excluded.prompt_cached_tokens`
	const hourlySQL = `INSERT INTO stats_hourly(hour, virtual_key_id, model, calls, ok, fail, stream_calls,
		prompt_tokens, completion_tokens, cost_cny, cost_points, unpriced_calls, prompt_cached_tokens)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(hour, virtual_key_id, model) DO UPDATE SET
		calls=calls+excluded.calls, ok=ok+excluded.ok, fail=fail+excluded.fail,
		stream_calls=stream_calls+excluded.stream_calls,
		prompt_tokens=prompt_tokens+excluded.prompt_tokens,
		completion_tokens=completion_tokens+excluded.completion_tokens,
		cost_cny=cost_cny+excluded.cost_cny, cost_points=cost_points+excluded.cost_points,
		unpriced_calls=unpriced_calls+excluded.unpriced_calls,
		prompt_cached_tokens=prompt_cached_tokens+excluded.prompt_cached_tokens`
	vals := []any{r.Calls, r.OK, r.Fail, r.StreamCalls, r.PromptTokens, r.CompletionTokens, r.CostCNY, r.CostPoints, r.UnpricedCalls, r.PromptCached}
	args1 := append([]any{at.Local().Format("2006-01-02"), vkeyID, model}, vals...)
	args2 := append([]any{at.Local().Format("2006-01-02 15"), vkeyID, model}, vals...)
	// 统计失败不影响主流程
	_, _ = s.db.Exec(dailySQL, args1...)
	_, _ = s.db.Exec(hourlySQL, args2...)
}

// bumpUKStats 真实 Key 维度：天粒度带 vKey 维度、小时粒度仅真实 Key。
func (s *Store) bumpUKStats(keyID, vkeyID int64, at time.Time, r StatRow) {
	const dailySQL = `INSERT INTO stats_daily_uk(day, upstream_key_id, virtual_key_id, calls, ok, fail,
		prompt_tokens, completion_tokens, cost_cny, cost_points, unpriced_calls, prompt_cached_tokens)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(day, upstream_key_id, virtual_key_id) DO UPDATE SET
		calls=calls+excluded.calls, ok=ok+excluded.ok, fail=fail+excluded.fail,
		prompt_tokens=prompt_tokens+excluded.prompt_tokens,
		completion_tokens=completion_tokens+excluded.completion_tokens,
		cost_cny=cost_cny+excluded.cost_cny, cost_points=cost_points+excluded.cost_points,
		unpriced_calls=unpriced_calls+excluded.unpriced_calls,
		prompt_cached_tokens=prompt_cached_tokens+excluded.prompt_cached_tokens`
	const hourlySQL = `INSERT INTO stats_hourly_uk(hour, upstream_key_id, calls, ok, fail,
		prompt_tokens, completion_tokens, cost_cny, cost_points, prompt_cached_tokens)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(hour, upstream_key_id) DO UPDATE SET
		calls=calls+excluded.calls, ok=ok+excluded.ok, fail=fail+excluded.fail,
		prompt_tokens=prompt_tokens+excluded.prompt_tokens,
		completion_tokens=completion_tokens+excluded.completion_tokens,
		cost_cny=cost_cny+excluded.cost_cny, cost_points=cost_points+excluded.cost_points,
		prompt_cached_tokens=prompt_cached_tokens+excluded.prompt_cached_tokens`
	// 统计失败不影响主流程
	if _, err := s.db.Exec(dailySQL, []any{at.Local().Format("2006-01-02"), keyID, vkeyID,
		r.Calls, r.OK, r.Fail, r.PromptTokens, r.CompletionTokens, r.CostCNY, r.CostPoints, r.UnpricedCalls, r.PromptCached}...); err != nil {
		log.Printf("store: stats_daily_uk 写入失败 key=%d vkey=%d: %v", keyID, vkeyID, err)
	}
	if _, err := s.db.Exec(hourlySQL, []any{at.Local().Format("2006-01-02 15"), keyID,
		r.Calls, r.OK, r.Fail, r.PromptTokens, r.CompletionTokens, r.CostCNY, r.CostPoints, r.PromptCached}...); err != nil {
		log.Printf("store: stats_hourly_uk 写入失败 key=%d: %v", keyID, err)
	}
}

// StatBuckets 查询某虚拟 Key 的统计（daily=true 天粒度，否则小时）；days>0 时按时间范围过滤。
func (s *Store) StatBuckets(vkeyID int64, daily bool, days int) ([]StatRow, error) {
	table, col := "stats_hourly", "hour"
	if daily {
		table, col = "stats_daily", "day"
	}
	where, args := "virtual_key_id=?", []any{vkeyID}
	if days > 0 {
		cutoff := time.Now().AddDate(0, 0, -days).Format("2006-01-02")
		where += " AND " + col + " >= ?"
		args = append(args, cutoff)
	}
	rows, err := s.db.Query(`SELECT `+col+`, virtual_key_id, model, `+statCols+` FROM `+table+
		` WHERE `+where+` ORDER BY `+col+` DESC, model`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatRow{}
	for rows.Next() {
		var r StatRow
		if err := rows.Scan(&r.Bucket, &r.VirtualKeyID, &r.Model, &r.Calls, &r.OK, &r.Fail,
			&r.StreamCalls, &r.PromptTokens, &r.CompletionTokens, &r.CostCNY, &r.CostPoints, &r.UnpricedCalls, &r.PromptCached); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RebuildStats 从 calls 全量重算两张统计表。
// RebuildStats 从 calls 全量重算统计表（统计是派生数据，calls 原始日志不受影响）。
// vkeyID 参数保留兼容但忽略：统计表间存在维度关联，局部重建容易造成口径不一致。
func (s *Store) RebuildStats(vkeyID int64) error {
	_ = vkeyID
	if _, err := s.db.Exec(`DELETE FROM stats_hourly`); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM stats_daily`); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM stats_daily_uk`); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM stats_hourly_uk`); err != nil {
		return err
	}
	rows, err := s.db.Query(`SELECT virtual_key_id, COALESCE(upstream_key_id,0), model, started_at, COALESCE(status_code,0), stream,
		COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0), COALESCE(cost_cny,0), COALESCE(cost_points,0),
		COALESCE(cost_cny,0)+COALESCE(cost_points,0)=0 AND total_tokens IS NOT NULL,
		COALESCE(prompt_cached_tokens,0)
		FROM calls`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type rec struct {
		vkey, ukey int64
		r          StatRow
		atMS       int64
	}
	var recs []rec
	for rows.Next() {
		var x rec
		var sc, st int
		var unpriced bool
		var cached int64
		if err := rows.Scan(&x.vkey, &x.ukey, &x.r.Model, &x.atMS, &sc, &st,
			&x.r.PromptTokens, &x.r.CompletionTokens, &x.r.CostCNY, &x.r.CostPoints, &unpriced, &cached); err != nil {
			return err
		}
		x.r.PromptCached = cached
		x.r.Calls = 1
		if sc >= 200 && sc < 400 {
			x.r.OK = 1
		} else {
			x.r.Fail = 1
		}
		if st != 0 {
			x.r.StreamCalls = 1
		}
		if unpriced {
			x.r.UnpricedCalls = 1
		}
		recs = append(recs, x)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, x := range recs {
		at := time.UnixMilli(x.atMS)
		s.bumpStats(x.vkey, x.r.Model, at, x.r)
		if x.ukey > 0 {
			s.bumpUKStats(x.ukey, x.vkey, at, x.r)
		}
	}
	return nil
}

// ---------- 设置中心 ----------

// DefaultSettings 内置默认值；未落库的键读默认。
var DefaultSettings = map[string]string{
	"log.high_water":           "100000",
	"log.low_water":            "50000",
	"log.max_age_days":         "90",
	"stats.hour_retention_days": "30",
	"stats.day_retention_days":  "365",
}

// Setting 读取设置（缺省回默认值）。
func (s *Store) Setting(key string) string {
	var v string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key=?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return DefaultSettings[key]
	}
	if err != nil {
		return DefaultSettings[key]
	}
	return v
}

// SettingInt 读取整型设置。
func (s *Store) SettingInt(key string) int64 {
	var n int64
	fmt.Sscanf(s.Setting(key), "%d", &n)
	return n
}

// SetSetting 写设置。
func (s *Store) SetSetting(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO settings(key, value, updated_at) VALUES(?,?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at`,
		key, value, now())
	return err
}

// AllSettings 全部设置（含默认值回填）。
func (s *Store) AllSettings() map[string]string {
	out := map[string]string{}
	for k, v := range DefaultSettings {
		out[k] = v
	}
	rows, err := s.db.Query(`SELECT key, value FROM settings`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			out[k] = v
		}
	}
	return out
}

// CleanupResult 清理结果。
type CleanupResult struct {
	CallsDeleted int64 `json:"calls_deleted"`
	StatsHourlyDeleted int64 `json:"stats_hourly_deleted"`
	StatsDailyDeleted  int64 `json:"stats_daily_deleted"`
}

// Cleanup 按设置执行水位线清理：日志先删超龄，超 high 清到 low；统计按保留期删。
func (s *Store) Cleanup() (CleanupResult, error) {
	var res CleanupResult
	defer func() {
		// 记录清理执行时间（只读展示用，不在设置表单中）
		_ = s.SetSetting("log.last_cleanup_at", fmt.Sprintf("%d", time.Now().UnixMilli()))
	}()
	// 1. 日志：超龄
	if d := s.SettingInt("log.max_age_days"); d > 0 {
		cutoff := time.Now().AddDate(0, 0, -int(d)).UnixMilli()
		r, err := s.db.Exec(`DELETE FROM calls WHERE started_at < ?`, cutoff)
		if err != nil {
			return res, err
		}
		res.CallsDeleted, _ = r.RowsAffected()
	}
	// 2. 日志：条数水位线（超 high 清到 low，删最旧）
	high, low := s.SettingInt("log.high_water"), s.SettingInt("log.low_water")
	if high > 0 && low > 0 && high > low {
		var cnt int64
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM calls`).Scan(&cnt); err != nil {
			return res, err
		}
		if cnt > high {
			r, err := s.db.Exec(`DELETE FROM calls WHERE id IN (
				SELECT id FROM calls ORDER BY started_at ASC LIMIT ?)`, cnt-low)
			if err != nil {
				return res, err
			}
			n, _ := r.RowsAffected()
			res.CallsDeleted += n
		}
	}
	// 3. 统计保留期
	if d := s.SettingInt("stats.hour_retention_days"); d > 0 {
		cutoff := time.Now().AddDate(0, 0, -int(d)).Format("2006-01-02 15")
		r, err := s.db.Exec(`DELETE FROM stats_hourly WHERE hour < ?`, cutoff)
		if err != nil {
			return res, err
		}
		res.StatsHourlyDeleted, _ = r.RowsAffected()
	}
	if d := s.SettingInt("stats.day_retention_days"); d > 0 {
		cutoff := time.Now().AddDate(0, 0, -int(d)).Format("2006-01-02")
		r, err := s.db.Exec(`DELETE FROM stats_daily WHERE day < ?`, cutoff)
		if err != nil {
			return res, err
		}
		res.StatsDailyDeleted, _ = r.RowsAffected()
	}
	return res, nil
}

// CallCount 当前日志条数。
func (s *Store) CallCount() int64 {
	var n int64
	s.db.QueryRow(`SELECT COUNT(*) FROM calls`).Scan(&n)
	return n
}


// StatOverview 各虚拟 Key 的聚合统计（用于统计总览页）。
type StatOverviewRow struct {
	VirtualKeyID     int64   `json:"virtual_key_id"`
	VirtualKey       string  `json:"virtual_key"`
	Name             string  `json:"name"`
	Calls            int64   `json:"calls"`
	OK               int64   `json:"ok"`
	Fail             int64   `json:"fail"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	CostCNY          float64 `json:"cost_cny"`
	CostPoints       float64 `json:"cost_points"`
	UnpricedCalls    int64   `json:"unpriced_calls"`
}

// StatOverview 按天粒度聚合各 Key；days>0 时过滤时间范围。
func (s *Store) StatOverview(days int) ([]StatOverviewRow, error) {
	where, args := "1=1", []any{}
	if days > 0 {
		where = "day >= ?"
		args = append(args, time.Now().AddDate(0, 0, -days).Format("2006-01-02"))
	}
	rows, err := s.db.Query(`SELECT d.virtual_key_id, COALESCE(v.key,''), COALESCE(v.name,''),
		SUM(d.calls), SUM(d.ok), SUM(d.fail), SUM(d.prompt_tokens), SUM(d.completion_tokens),
		SUM(d.cost_cny), SUM(d.cost_points), SUM(d.unpriced_calls)
		FROM stats_daily d LEFT JOIN virtual_keys v ON v.id = d.virtual_key_id
		WHERE `+where+` GROUP BY d.virtual_key_id ORDER BY SUM(d.calls) DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatOverviewRow{}
	for rows.Next() {
		var r StatOverviewRow
		if err := rows.Scan(&r.VirtualKeyID, &r.VirtualKey, &r.Name, &r.Calls, &r.OK, &r.Fail,
			&r.PromptTokens, &r.CompletionTokens, &r.CostCNY, &r.CostPoints, &r.UnpricedCalls); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------- 双维度统计：vKey（含周同比）与真实 Key ----------

// VKeyStatsRow vKey 维度汇总：本期与上一等长周期对比（tokens/费用同比）。
type VKeyStatsRow struct {
	VirtualKeyID int64   `json:"virtual_key_id"`
	VirtualKey   string  `json:"virtual_key"`
	Name         string  `json:"name"`
	Calls        int64   `json:"calls"`
	OK           int64   `json:"ok"`
	Fail         int64   `json:"fail"`
	PromptTokens int64   `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	CostCNY      float64 `json:"cost_cny"`
	CostPoints   float64 `json:"cost_points"`
	UnpricedCalls int64  `json:"unpriced_calls"`
	PromptCached  int64  `json:"prompt_cached_tokens"`
	// 上一等长周期（周同比基准）
	PrevTokens   int64   `json:"prev_tokens"`
	PrevCostCNY  float64 `json:"prev_cost_cny"`
	PrevCalls    int64   `json:"prev_calls"`
}

// rangePair 返回 [本期起点, 前期起点]：本期为最近 days 天（含今天），前期为其紧邻的等长窗口。
func statsRangePair(days int) (curFrom, prevFrom string) {
	if days <= 0 {
		return "", "" // 全部：无同比
	}
	now := time.Now()
	return now.AddDate(0, 0, -(days - 1)).Format("2006-01-02"),
		now.AddDate(0, 0, -(2*days - 1)).Format("2006-01-02")
}

// VKeyStats 各 vKey 汇总 + 上期对比；days<=0 表示全部（无同比）。
func (s *Store) VKeyStats(days int) ([]VKeyStatsRow, error) {
	curFrom, prevFrom := statsRangePair(days)
	agg := func(from, to string) (map[int64]*VKeyStatsRow, error) {
		where, args := "1=1", []any{}
		if from != "" {
			where = "d.day >= ? AND d.day < ?"
			args = append(args, from, to)
		}
		rows, err := s.db.Query(`SELECT d.virtual_key_id, COALESCE(v.key,''), COALESCE(v.name,''),
			SUM(d.calls), SUM(d.ok), SUM(d.fail), SUM(d.prompt_tokens), SUM(d.completion_tokens),
			SUM(d.cost_cny), SUM(d.cost_points), SUM(d.unpriced_calls), SUM(d.prompt_cached_tokens)
			FROM stats_daily d LEFT JOIN virtual_keys v ON v.id=d.virtual_key_id
			WHERE `+where+` GROUP BY d.virtual_key_id`, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[int64]*VKeyStatsRow{}
		for rows.Next() {
			var r VKeyStatsRow
			if err := rows.Scan(&r.VirtualKeyID, &r.VirtualKey, &r.Name, &r.Calls, &r.OK, &r.Fail,
				&r.PromptTokens, &r.CompletionTokens, &r.CostCNY, &r.CostPoints, &r.UnpricedCalls, &r.PromptCached); err != nil {
				return nil, err
			}
			out[r.VirtualKeyID] = &r
		}
		return out, rows.Err()
	}
	today := time.Now().AddDate(0, 0, 1).Format("2006-01-02")
	cur, err := agg(curFrom, today)
	if err != nil {
		return nil, err
	}
	var prev map[int64]*VKeyStatsRow
	if prevFrom != "" {
		if prev, err = agg(prevFrom, curFrom); err != nil {
			return nil, err
		}
	}
	out := []VKeyStatsRow{}
	for id, r := range cur {
		if p := prev[id]; p != nil {
			r.PrevTokens = p.PromptTokens + p.CompletionTokens
			r.PrevCostCNY = p.CostCNY
			r.PrevCalls = p.Calls
		}
		out = append(out, *r)
	}
	// 稳定排序 + id 兜底：相同调用量时顺序固定，避免前端切换粒度时卡片乱跳
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Calls != out[j].Calls {
			return out[i].Calls > out[j].Calls
		}
		return out[i].VirtualKeyID < out[j].VirtualKeyID
	})
	return out, nil
}

// UKStatsRow 真实 Key 维度汇总（含周同比与服务过的 vKey 数）。
type UKStatsRow struct {
	UpstreamKeyID int64   `json:"id"`
	Upstream      string  `json:"upstream"`
	Label         string  `json:"label"`
	Enabled       bool    `json:"enabled"`
	VKeyCount     int64   `json:"vkey_count"`
	Calls         int64   `json:"calls"`
	OK            int64   `json:"ok"`
	Fail          int64   `json:"fail"`
	PromptTokens  int64   `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	CostCNY       float64 `json:"cost_cny"`
	CostPoints    float64 `json:"cost_points"`
	PromptCached  int64   `json:"prompt_cached_tokens"`
	PrevCostCNY   float64 `json:"prev_cost_cny"`
	PrevTokens    int64   `json:"prev_tokens"`
	PrevCalls     int64   `json:"prev_calls"`
}

// UKStats 各真实 Key 汇总 + 上期对比。
func (s *Store) UKStats(days int) ([]UKStatsRow, error) {
	curFrom, prevFrom := statsRangePair(days)
	agg := func(from, to string) (map[int64]*UKStatsRow, error) {
		where, args := "1=1", []any{}
		if from != "" {
			where = "d.day >= ? AND d.day < ?"
			args = append(args, from, to)
		}
		rows, err := s.db.Query(`SELECT d.upstream_key_id, COALESCE(u.name,''), COALESCE(k.label,''),
			COUNT(DISTINCT d.virtual_key_id), SUM(d.calls), SUM(d.ok), SUM(d.fail),
			SUM(d.prompt_tokens), SUM(d.completion_tokens), SUM(d.cost_cny), SUM(d.cost_points), SUM(d.prompt_cached_tokens)
			FROM stats_daily_uk d
			LEFT JOIN upstream_keys k ON k.id=d.upstream_key_id
			LEFT JOIN upstreams u ON u.id=k.upstream_id
			WHERE `+where+` GROUP BY d.upstream_key_id`, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[int64]*UKStatsRow{}
		for rows.Next() {
			var r UKStatsRow
			if err := rows.Scan(&r.UpstreamKeyID, &r.Upstream, &r.Label, &r.VKeyCount, &r.Calls, &r.OK, &r.Fail,
				&r.PromptTokens, &r.CompletionTokens, &r.CostCNY, &r.CostPoints, &r.PromptCached); err != nil {
				return nil, err
			}
			out[r.UpstreamKeyID] = &r
		}
		return out, rows.Err()
	}
	today := time.Now().AddDate(0, 0, 1).Format("2006-01-02")
	cur, err := agg(curFrom, today)
	if err != nil {
		return nil, err
	}
	var prev map[int64]*UKStatsRow
	if prevFrom != "" {
		if prev, err = agg(prevFrom, curFrom); err != nil {
			return nil, err
		}
	}
	// 真实 Key 是否启用也带上，便于列表过滤
	enabled := map[int64]bool{}
	er, err := s.db.Query(`SELECT id, enabled FROM upstream_keys`)
	if err == nil {
		defer er.Close()
		for er.Next() {
			var id int64
			var en bool
			if er.Scan(&id, &en) == nil {
				enabled[id] = en
			}
		}
	}
	out := []UKStatsRow{}
	for id, r := range cur {
		if p := prev[id]; p != nil {
			r.PrevCostCNY = p.CostCNY
			r.PrevTokens = p.PromptTokens + p.CompletionTokens
			r.PrevCalls = p.Calls
		}
		r.Enabled = enabled[id]
		out = append(out, *r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CostCNY != out[j].CostCNY {
			return out[i].CostCNY > out[j].CostCNY
		}
		return out[i].UpstreamKeyID < out[j].UpstreamKeyID
	})
	return out, nil
}

// UKSeries 某真实 Key 的按天/按小时序列。
func (s *Store) UKSeries(keyID int64, daily bool, days int) ([]StatRow, error) {
	table, col := "stats_hourly_uk", "hour"
	if daily {
		table, col = "stats_daily_uk", "day"
	}
	where, args := "upstream_key_id=?", []any{keyID}
	if days > 0 {
		cutoff := time.Now().AddDate(0, 0, -(days - 1)).Format("2006-01-02")
		where += " AND " + col + " >= ?"
		args = append(args, cutoff)
	}
	rows, err := s.db.Query(`SELECT `+col+`, SUM(calls), SUM(ok), SUM(fail), SUM(prompt_tokens),
		SUM(completion_tokens), SUM(cost_cny), SUM(cost_points), SUM(prompt_cached_tokens) FROM `+table+` WHERE `+where+
		` GROUP BY `+col+` ORDER BY `+col+` DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatRow{}
	for rows.Next() {
		var r StatRow
		if err := rows.Scan(&r.Bucket, &r.Calls, &r.OK, &r.Fail, &r.PromptTokens, &r.CompletionTokens, &r.CostCNY, &r.CostPoints, &r.PromptCached); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UKVKeyDist 某真实 Key 被 vKey 使用的分布。
func (s *Store) UKVKeyDist(keyID int64, days int) ([]VKeyStatsRow, error) {
	where, args := "d.upstream_key_id=?", []any{keyID}
	if days > 0 {
		where += " AND d.day >= ?"
		args = append(args, time.Now().AddDate(0, 0, -(days-1)).Format("2006-01-02"))
	}
	rows, err := s.db.Query(`SELECT d.virtual_key_id, COALESCE(v.key,''), COALESCE(v.name,''),
		SUM(d.calls), SUM(d.ok), SUM(d.fail), SUM(d.prompt_tokens), SUM(d.completion_tokens),
		SUM(d.cost_cny), SUM(d.cost_points), SUM(d.unpriced_calls), SUM(d.prompt_cached_tokens)
		FROM stats_daily_uk d LEFT JOIN virtual_keys v ON v.id=d.virtual_key_id
		WHERE `+where+` GROUP BY d.virtual_key_id ORDER BY SUM(d.cost_cny) DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []VKeyStatsRow{}
	for rows.Next() {
		var r VKeyStatsRow
		if err := rows.Scan(&r.VirtualKeyID, &r.VirtualKey, &r.Name, &r.Calls, &r.OK, &r.Fail,
			&r.PromptTokens, &r.CompletionTokens, &r.CostCNY, &r.CostPoints, &r.UnpricedCalls, &r.PromptCached); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LogOverview 各虚拟 Key 的日志量与用量（日志总览页）。
type LogOverviewRow struct {
	VirtualKeyID int64  `json:"virtual_key_id"`
	VirtualKey   string `json:"virtual_key"`
	Name         string `json:"name"`
	Calls        int64  `json:"calls"`
	Tokens       int64  `json:"tokens"`
	LastAt       int64  `json:"last_at"`
}

// LogOverview 聚合各 Key 的原始日志量。
func (s *Store) LogOverview() ([]LogOverviewRow, error) {
	rows, err := s.db.Query(`SELECT c.virtual_key_id, COALESCE(v.key,''), COALESCE(v.name,''),
		COUNT(*), COALESCE(SUM(c.total_tokens),0), COALESCE(MAX(c.started_at),0)
		FROM calls c LEFT JOIN virtual_keys v ON v.id = c.virtual_key_id
		WHERE c.virtual_key_id IS NOT NULL GROUP BY c.virtual_key_id ORDER BY MAX(c.started_at) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LogOverviewRow{}
	for rows.Next() {
		var r LogOverviewRow
		if err := rows.Scan(&r.VirtualKeyID, &r.VirtualKey, &r.Name, &r.Calls, &r.Tokens, &r.LastAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
