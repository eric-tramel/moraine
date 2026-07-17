-- Rebuild existing OMP session heads so source titles and privacy-safe
-- dispatched-session labels are reflected by the MCP open read model.
INSERT INTO moraine.mcp_open_dirty_sessions
  (session_id, dirty_revision, observed_at)
SELECT session_id, generateSnowflakeID(), now64(3)
FROM (
  SELECT DISTINCT session_id
  FROM moraine.events FINAL
  WHERE notEmpty(session_id)
    AND source_name = 'omp'
);
