-- DeepSeek Harness（dsh）历史会话回填：复用 0006 的 session_id/client 列；
-- dsh 无轮次头，turn_id 保持空（前端归「未分轮」段）。
UPDATE calls SET
  session_id = COALESCE(json_extract(req_headers, '$."X-Deepseek-Harness-Session-Id"[0]'), ''),
  client     = 'DeepSeek Harness'
WHERE session_id = '' AND req_headers LIKE '%X-Deepseek-Harness-Session-Id%'
  AND json_valid(req_headers);
