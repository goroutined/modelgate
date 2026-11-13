-- 调用记录补「最后一条消息类型」：日志列表快速判断请求意图（新输入/工具结果回传等）。
-- 取请求体 messages/input 数组末项的 role（Responses typed item 映射为统一角色）。
ALTER TABLE calls ADD COLUMN last_msg_role TEXT NOT NULL DEFAULT '';

-- 历史回填：截断的 req_body json_valid 失败会安全跳过
UPDATE calls SET last_msg_role = CASE
  WHEN json_type(req_body, '$.messages') = 'array' AND json_array_length(req_body, '$.messages') > 0
    THEN COALESCE(json_extract(req_body, '$.messages[#-1].role'), '')
  WHEN json_type(req_body, '$.input') = 'array' AND json_array_length(req_body, '$.input') > 0 THEN
    CASE json_extract(req_body, '$.input[#-1].type')
      WHEN 'function_call' THEN 'tool_call'
      WHEN 'custom_tool_call' THEN 'tool_call'
      WHEN 'function_call_output' THEN 'tool'
      WHEN 'custom_tool_call_output' THEN 'tool'
      WHEN 'reasoning' THEN 'reasoning'
      WHEN 'message' THEN COALESCE(json_extract(req_body, '$.input[#-1].role'), 'message')
      ELSE COALESCE(json_extract(req_body, '$.input[#-1].type'), '')
    END
  ELSE ''
END
WHERE last_msg_role = '' AND json_valid(req_body);
