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

-- SUPERSEDED 2026-08-01 (issue #603 WI-10, `sql/041`). The `mcp_open_*`
-- projection this file names is dropped by migration 041; the code that ships
-- 041 carries no projector, no v1 reader and no `mcp_open` reclaim executor,
-- so every present-tense description of that machinery here records the state
-- when this file was written, not a running component. Nothing else in this
-- file is edited and every statement in it still executes verbatim — a fresh
-- install creates the family and 041 drops it in the same migrate pass. A
-- released migration is immutable and the runner keys applied migrations by
-- `(version, name)` with no content checksum, so an upgraded host never
-- re-reads this file: this note is append-only, and it is here for the
-- operator who reads the file on a fresh install.
--
-- It sits at the FOOT of this file on purpose. Source elsewhere in the tree
-- cites these statements by `sql/NNN:LINE`, and a note at the head moves
-- every one of those citations silently; appended here, no existing line
-- number changes. `a_cross_file_line_citation_resolves_to_what_it_claims`
-- is what keeps them honest either way.
--
-- The TRUNCATEs above stay legal at their own version: `classify_at_version`
-- judges a migration against the table roster as of that version, so 034 is
-- exactly as legal as when it was written, while any migration ordered after
-- 041 that names the family is a `never_delete` finding.
