-- 真实 Key 维度统计（写时预聚合，空间换时间）
-- 天粒度带 vKey 维度：覆盖"按天趋势"与"被哪些 vKey 使用"两个视图
CREATE TABLE IF NOT EXISTS stats_daily_uk (
    day             TEXT NOT NULL,           -- "2026-10-19"
    upstream_key_id INTEGER NOT NULL REFERENCES upstream_keys(id) ON DELETE CASCADE,
    virtual_key_id  INTEGER NOT NULL REFERENCES virtual_keys(id) ON DELETE CASCADE,
    calls           INTEGER NOT NULL DEFAULT 0,
    ok              INTEGER NOT NULL DEFAULT 0,
    fail            INTEGER NOT NULL DEFAULT 0,
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    cost_cny        REAL NOT NULL DEFAULT 0,
    cost_points     REAL NOT NULL DEFAULT 0,
    unpriced_calls  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, upstream_key_id, virtual_key_id)
);
CREATE INDEX IF NOT EXISTS idx_stats_daily_uk_key ON stats_daily_uk(upstream_key_id, day);

-- 小时粒度只按真实 Key（不带 vKey 维度，控制行数），看消耗时间段分布
CREATE TABLE IF NOT EXISTS stats_hourly_uk (
    hour            TEXT NOT NULL,           -- "2026-10-19 14"
    upstream_key_id INTEGER NOT NULL REFERENCES upstream_keys(id) ON DELETE CASCADE,
    calls           INTEGER NOT NULL DEFAULT 0,
    ok              INTEGER NOT NULL DEFAULT 0,
    fail            INTEGER NOT NULL DEFAULT 0,
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    cost_cny        REAL NOT NULL DEFAULT 0,
    cost_points     REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (hour, upstream_key_id)
);
