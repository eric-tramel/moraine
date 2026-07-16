-- Rebuild existing MCP open session heads so `source` is the configured
-- ingest source name rather than a payload-local value from session metadata.
INSERT INTO moraine.mcp_open_dirty_sessions (session_id, dirty_revision, observed_at)
SELECT session_id, generateSnowflakeID(), now64(3)
FROM moraine.mcp_open_sessions FINAL
WHERE session_id != '';
