-- Preserve the distinct metadata precedence contracts of MCP session listing
-- and bounded open without returning list pages to canonical-event scans.
ALTER TABLE moraine.mcp_open_publication_headers
  ADD COLUMN IF NOT EXISTS list_title String AFTER session_summary,
  ADD COLUMN IF NOT EXISTS list_session_summary String AFTER list_title;

-- Existing development databases may already contain headers projected by the
-- pre-035 schema. The MCP read model is derived from canonical live events, so
-- invalidate and rebuild the complete compatibility set as one unit.
INSERT INTO moraine.mcp_open_projection_state
  (state_key, ready, generation, backfill_cursor)
VALUES ('global', 0, generateSnowflakeID(), '');

TRUNCATE TABLE moraine.mcp_open_events;
TRUNCATE TABLE moraine.mcp_open_turns;
TRUNCATE TABLE moraine.mcp_open_sessions;
TRUNCATE TABLE moraine.mcp_open_publication_headers;
TRUNCATE TABLE moraine.mcp_open_generation_readiness;
TRUNCATE TABLE moraine.mcp_open_backfill_plans;

INSERT INTO moraine.mcp_open_projection_state
  (state_key, ready, generation, backfill_cursor)
VALUES ('global', 0, generateSnowflakeID(), '');
