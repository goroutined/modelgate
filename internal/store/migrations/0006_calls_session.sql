-- 调用记录补客户端会话标识，用于日志按会话分组（如 Codex 的 Session-Id）。
-- 字段是通用概念不绑定客户端；新客户端在识别规则中扩展即可。
ALTER TABLE calls ADD COLUMN session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE calls ADD COLUMN client TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_calls_session ON calls(virtual_key_id, session_id, started_at DESC);

-- 历史回填：从已归档请求头提取（当前仅 Codex 规则）；
-- LIKE 先过滤无该头的请求，避免全表 json_extract
UPDATE calls SET
  session_id = COALESCE(json_extract(req_headers, '$."Session-Id"[0]'), ''),
  client     = COALESCE(json_extract(req_headers, '$."Originator"[0]'), '')
WHERE session_id = '' AND req_headers LIKE '%Session-Id%' AND json_valid(req_headers);
