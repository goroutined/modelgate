-- 调用记录补轮次标识（Codex X-Codex-Turn-Metadata.turn_id），
-- 支撑日志会话分组下的二层 turn 分组；同为通用概念，新客户端在识别规则中提取。
ALTER TABLE calls ADD COLUMN turn_id TEXT NOT NULL DEFAULT '';

-- 历史回填：两层 json_extract 取 turn_id；嵌套 json_valid 防护截断/残缺数据
UPDATE calls SET
  turn_id = COALESCE(json_extract(json_extract(req_headers, '$."X-Codex-Turn-Metadata"[0]'), '$.turn_id'), '')
WHERE turn_id = '' AND req_headers LIKE '%X-Codex-Turn-Metadata%'
  AND json_valid(req_headers)
  AND json_valid(json_extract(req_headers, '$."X-Codex-Turn-Metadata"[0]'));
