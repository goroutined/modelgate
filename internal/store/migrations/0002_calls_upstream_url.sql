-- 调用记录补充转发目标 URL，便于排查路径拼接与协议路由问题
ALTER TABLE calls ADD COLUMN upstream_url TEXT NOT NULL DEFAULT '';
