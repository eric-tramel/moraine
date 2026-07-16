INSERT INTO moraine.mcp_open_dirty_sessions
  (session_id, dirty_revision, observed_at)
SELECT session_id, generateSnowflakeID(), now64(3)
FROM (
  SELECT DISTINCT session_id
  FROM moraine.events FINAL
  WHERE notEmpty(session_id)
);

INSERT INTO moraine.mcp_open_projection_state
  (state_key, ready, generation, backfill_cursor)
VALUES ('global', 0, generateSnowflakeID(), '');
