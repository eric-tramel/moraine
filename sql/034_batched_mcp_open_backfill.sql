-- Replace the unreleased per-session historical MCP projection with a
-- crash-resumable batch plan. Migration 033 deliberately marked the read
-- model unavailable; while that marker is still false, every row below is
-- derived and can be rebuilt from the canonical live-event relation.

CREATE TABLE IF NOT EXISTS moraine.mcp_open_backfill_plans (
  session_id String,
  candidate_publication_id String,
  slot UInt8,
  candidate_generation UInt64,
  source_revision UInt64,
  dirty_revision UInt64,
  required_source_heads Array(Tuple(
    source_host String,
    source_name String,
    source_file String,
    source_generation UInt32,
    publication_revision UInt64
  )),
  required_heads_fingerprint String,
  phase UInt8,
  plan_revision UInt64
)
ENGINE = ReplacingMergeTree(plan_revision)
ORDER BY (session_id);

-- The pre-034 projector can leave multiple inactive generations and hundreds
-- of tiny parts after a failed page. These relations are a derived cache, and
-- migration 033 has already made them unavailable to MCP readers. Reset the
-- whole compatibility set together so no stale readiness row can authorize a
-- header whose children were discarded.
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
