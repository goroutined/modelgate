-- modelgate 初始 schema（最终版）：业务配置 + 模型定义 + 调用归档
-- 业务配置（服务商/端点/模型/真实key/虚拟key/策略）存 SQLite，管理页可改、不重启生效。

CREATE TABLE upstreams (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,        -- 服务商标识（目录 ID 或 custom- 前缀）
    base_url     TEXT NOT NULL DEFAULT '',    -- 默认端点 api 的冗余，转发用
    note         TEXT NOT NULL DEFAULT '',    -- 显示名
    console_url  TEXT NOT NULL DEFAULT '',    -- 控制台链接
    pricing_url  TEXT NOT NULL DEFAULT '',    -- 定价页链接
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

-- 协议端点：协议与 baseURL 一一对应，虚拟 Key 按协议选择
CREATE TABLE upstream_endpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_id INTEGER NOT NULL REFERENCES upstreams(id) ON DELETE CASCADE,
    protocol    TEXT NOT NULL,  -- openai-compatible / anthropic-compatible / openai-responses
    api         TEXT NOT NULL,
    is_default  INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    UNIQUE(upstream_id, protocol)
);

-- 模型定义：目录服务商展示用 modellink 实时数据；自定义服务商在此维护
CREATE TABLE upstream_models (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_id   INTEGER NOT NULL REFERENCES upstreams(id) ON DELETE CASCADE,
    model_id      TEXT NOT NULL,              -- 调用名，如 deepseek-v4
    name          TEXT NOT NULL DEFAULT '',   -- 显示名
    description   TEXT NOT NULL DEFAULT '',
    cost_cny_in         REAL,                 -- 输入价格 元/百万token
    cost_cny_out        REAL,                 -- 输出价格 元/百万token
    cost_cny_cache_read REAL,                 -- 缓存命中（读）价格
    cost_cny_cache_write REAL,                -- 缓存写入价格
    context_limit INTEGER,                    -- 上下文 tokens
    max_output    INTEGER,                    -- 最大输出 tokens
    enabled       INTEGER NOT NULL DEFAULT 1,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    UNIQUE(upstream_id, model_id)
);

CREATE TABLE upstream_keys (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_id INTEGER NOT NULL REFERENCES upstreams(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,                -- 真实 key，日志脱敏
    label       TEXT NOT NULL DEFAULT '',     -- 资产账号备注
    enabled     INTEGER NOT NULL DEFAULT 1,
    status      TEXT NOT NULL DEFAULT 'ok',   -- ok / cooldown / disabled
    cooldown_until INTEGER,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    UNIQUE(upstream_id, key)
);

CREATE TABLE virtual_keys (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    key         TEXT NOT NULL UNIQUE,         -- 对外暴露的虚拟 key
    name        TEXT NOT NULL DEFAULT '',
    policy_id   INTEGER,
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- v1 策略：固定映射；type 预留 round_robin/random/weighted
CREATE TABLE policies (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    type        TEXT NOT NULL DEFAULT 'fixed',
    config      TEXT NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- 一次调用一条主记录；SSE 分包明细拆表
CREATE TABLE calls (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    virtual_key_id  INTEGER REFERENCES virtual_keys(id),
    upstream_id     INTEGER REFERENCES upstreams(id),
    upstream_key_id INTEGER REFERENCES upstream_keys(id),
    method          TEXT NOT NULL,
    path            TEXT NOT NULL,
    model           TEXT NOT NULL DEFAULT '',
    status_code     INTEGER,
    req_headers     TEXT NOT NULL DEFAULT '',
    req_body        TEXT,
    resp_headers    TEXT NOT NULL DEFAULT '',
    resp_body       TEXT,
    stream          INTEGER NOT NULL DEFAULT 0,
    chunk_count     INTEGER NOT NULL DEFAULT 0,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    total_tokens      INTEGER,
    cost_cny        REAL,                    -- 人民币费用（元），NULL=无法计价
    cost_points     REAL,                    -- 积分制服务商的积分消耗
    error           TEXT NOT NULL DEFAULT '',
    started_at      INTEGER NOT NULL,        -- 收到请求（毫秒）
    upstream_ms     INTEGER,                 -- 发往上游的偏移毫秒
    first_byte_ms   INTEGER,                 -- 收到上游首字节的偏移毫秒
    ended_at        INTEGER
);
CREATE INDEX idx_calls_started ON calls(started_at DESC);
CREATE INDEX idx_calls_vkey ON calls(virtual_key_id, started_at DESC);
CREATE INDEX idx_calls_model ON calls(model);

-- 统计：小时/天两粒度，call_end 增量 upsert；与日志独立，日志清理不影响
CREATE TABLE stats_hourly (
    hour            TEXT NOT NULL,           -- "2026-10-18 14"
    virtual_key_id  INTEGER NOT NULL REFERENCES virtual_keys(id) ON DELETE CASCADE,
    model           TEXT NOT NULL,
    calls           INTEGER NOT NULL DEFAULT 0,
    ok              INTEGER NOT NULL DEFAULT 0,
    fail            INTEGER NOT NULL DEFAULT 0,
    stream_calls    INTEGER NOT NULL DEFAULT 0,
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    cost_cny        REAL NOT NULL DEFAULT 0,
    cost_points     REAL NOT NULL DEFAULT 0,
    unpriced_calls  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(hour, virtual_key_id, model)
);
CREATE TABLE stats_daily (
    day             TEXT NOT NULL,           -- "2026-10-18"
    virtual_key_id  INTEGER NOT NULL REFERENCES virtual_keys(id) ON DELETE CASCADE,
    model           TEXT NOT NULL,
    calls           INTEGER NOT NULL DEFAULT 0,
    ok              INTEGER NOT NULL DEFAULT 0,
    fail            INTEGER NOT NULL DEFAULT 0,
    stream_calls    INTEGER NOT NULL DEFAULT 0,
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    cost_cny        REAL NOT NULL DEFAULT 0,
    cost_points     REAL NOT NULL DEFAULT 0,
    unpriced_calls  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(day, virtual_key_id, model)
);

-- 设置中心（key-value）
CREATE TABLE settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE chunks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id     INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    t_offset_ms INTEGER NOT NULL,
    data        TEXT NOT NULL,
    UNIQUE(call_id, seq)
);
