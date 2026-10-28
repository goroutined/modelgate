-- 统计表补充缓存命中 tokens（缓存命中率指标）
ALTER TABLE stats_daily ADD COLUMN prompt_cached_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE stats_hourly ADD COLUMN prompt_cached_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE stats_daily_uk ADD COLUMN prompt_cached_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE stats_hourly_uk ADD COLUMN prompt_cached_tokens INTEGER NOT NULL DEFAULT 0;
