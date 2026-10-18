-- 调用记录补充 prompt 缓存命中 tokens（多协议 cache 命中率展示）
ALTER TABLE calls ADD COLUMN prompt_cached_tokens INTEGER;
