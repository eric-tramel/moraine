-- Canonical read indexes for the issue-598 v2 `open` reader.
--
-- Three content-free indexes plus a readiness/coverage control table make
-- session discovery and event location cheap without a full-content session
-- cache: a scalar session directory, a versioned exact-event locator, and a
-- content-free session-order navigation index. Materialized views maintain all
-- three from every `moraine.events` insert block in O(delta) work.
--
-- This migration is purely additive: no TRUNCATE, no reset of any existing
-- `mcp_open_*` projection state (deliberate contrast to migrations 029/033/034/
-- 035). It installs the tables and MVs while traffic is served; the one-shot
-- backfill and readiness publication run separately (issue #598 WI-03). The
-- legacy `mcp_open_*` projector keeps running as the compatibility reconciler
-- until the projector-retirement PR.
--
-- Every derivation expression below is transcribed from the shared authority in
-- `moraine_clickhouse::canonical_derivations` using the REAL `events` column
-- names (`actor_kind`/`event_kind`, not the projector's aliased
-- `actor_role`/`event_class`). The migration content tests in `lib.rs` pin each
-- MV body to that authority so the three consumers cannot drift.

-- ---------------------------------------------------------------------------
-- Content-free discovery scalars, keyed by source generation + session +
-- conservative filter dimensions. Aggregate states let block-level inserts
-- merge without losing historical bounds. `origin_cwd_state` is a full
-- AggregateFunction state because SimpleAggregateFunction cannot express
-- argMinIf; AggregatingMergeTree mixes both column kinds.
--
-- `mode_hint` is presence-ranked recall-only (R3): the MV freezes the internal
-- tool-name list at migration time, so consumers may prefilter on it but never
-- precision-filter; a release that extends the tool-name allowlist ships a
-- follow-up migration recreating the directory MV. `observed_events` is a
-- physical-insert counter, documented hint-only (over-counts under partial
-- block retry; never an exact live count).
CREATE TABLE IF NOT EXISTS moraine.mcp_session_directory (
  session_id String,
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  harness LowCardinality(String),
  mode_hint SimpleAggregateFunction(max, UInt8),
  min_observed_event_time SimpleAggregateFunction(min, DateTime64(3)),
  max_observed_event_time SimpleAggregateFunction(max, DateTime64(3)),
  observed_events SimpleAggregateFunction(sum, UInt64),
  origin_cwd_state AggregateFunction(argMinIf, String, Tuple(DateTime64(3), String), UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (session_id, source_host, source_name, source_file, source_generation, harness);

-- ---------------------------------------------------------------------------
-- Versioned exact-event seek: point lookup by `event_uid` returns the session
-- plus the deterministic `sort_time` + offset/line needed to jump directly into
-- the navigation index at the event's exact ordering tuple. RMT(event_version)
-- collapses re-inserts of the same uid; PK leads with `event_uid` for the seek.
CREATE TABLE IF NOT EXISTS moraine.mcp_event_locator (
  event_uid String,
  source_host String,
  event_version UInt64,
  session_id String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  sort_time DateTime64(3)
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (event_uid, source_host);

-- ---------------------------------------------------------------------------
-- Content-free session-order navigation index. Every maintained column is a
-- fixed per-event scalar: sort keys, kinds, identifiers, versions, and two
-- precomputed boolean flags that are pure per-row functions. Nothing running
-- (event_order, turn_seq, event_ordinal, prev/next links, prefix counts) lives
-- here; those are reader-derived. No text, payloads, summaries, arrays, or turn
-- bodies.
--
-- `sort_time` is the deterministic v2 order key (CH-parsed record_ts else the
-- epoch sentinel, never ingested_at), so a re-insert of the same uid yields the
-- identical PK and RMT(event_version) collapses it: no ghost rows. `display_time`
-- keeps the v1 display/aggregate expression for response fidelity. The PK equals
-- the authoritative ordering tuple with `sort_time` substituted for event_time,
-- so keyset predicates and closed `sort_time` windows prune granules.
-- Session-hash partitioning keeps each session in one partition so FINAL merges
-- locally and range reads stay ordered.
CREATE TABLE IF NOT EXISTS moraine.mcp_event_navigation (
  session_id String,
  sort_time DateTime64(3),
  source_host String,
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  event_uid String,
  event_version UInt64,
  source_name LowCardinality(String),
  event_ts DateTime64(3),
  display_time DateTime64(3),
  event_kind LowCardinality(String),
  actor_kind LowCardinality(String),
  payload_type LowCardinality(String),
  turn_index UInt32,
  tool_call_id String,
  tool_name LowCardinality(String),
  tool_phase LowCardinality(String),
  op_status LowCardinality(String),
  item_id String,
  harness LowCardinality(String),
  inference_provider LowCardinality(String),
  cwd LowCardinality(String),
  is_user_message UInt8,
  is_metadata_bearing UInt8
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, sort_time, source_host, source_file, source_generation, source_offset, source_line_no, event_uid);

-- ---------------------------------------------------------------------------
-- Coverage cursor + per-consumer readiness + audit record. RMT(generation) with
-- snowflake generations; `cursor` carries a JSON payload (backfill cursor / audit
-- outcome / promote provenance). Deliberately NOT `mcp_open_projection_state`
-- (which existing migrations reset). This is the published core-reader/readiness
-- milestone that #599/#597 consume.
CREATE TABLE IF NOT EXISTS moraine.mcp_read_index_state (
  state_key LowCardinality(String),
  ready UInt8,
  generation UInt64,
  cursor String
)
ENGINE = ReplacingMergeTree(generation)
ORDER BY (state_key);

-- ---------------------------------------------------------------------------
-- Materialized views: join-free, subquery-free, dictionary-free per-row scalar
-- expressions firing on every `moraine.events` insert block. All three store
-- ALL generations (no publication join inside any MV — publication gating is
-- read-time via the pinned as-of head join) and skip blank session ids (R3).
-- MV failure must fail the insert loudly: `materialized_views_ignore_errors`
-- is deliberately NOT set.

-- Directory MV: one partial-aggregate row per (session, source generation,
-- harness) group per insert block. `mode_hint` uses the shared per-event mode
-- rank; the bounds aggregate the v1 DISPLAY time expression (parity with the
-- list window filter); `observed_events` counts physical inserts;
-- `origin_cwd_state` is the argMinIf state read back with argMinIfMerge.
CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_session_directory_from_events
TO moraine.mcp_session_directory AS
SELECT
  session_id,
  source_host,
  source_name,
  source_file,
  source_generation,
  harness,
  max(multiIf(payload_type = 'web_search_call' OR payload_type = 'search_results_received' OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch')), 3, source_name = 'codex-mcp' OR (lowerUTF8(trimBoth(tool_name)) IN ('search', 'search_sessions', 'open', 'list_sessions', 'file_attention') OR (length(splitByString('__', lowerUTF8(trimBoth(tool_name)))) = 3 AND arrayElement(splitByString('__', lowerUTF8(trimBoth(tool_name))), 1) = 'mcp' AND arrayElement(splitByString('__', lowerUTF8(trimBoth(tool_name))), 2) = 'moraine' AND arrayElement(splitByString('__', lowerUTF8(trimBoth(tool_name))), 3) IN ('search', 'search_sessions', 'open', 'list_sessions', 'file_attention'))), 2, event_kind IN ('tool_call', 'tool_result') OR payload_type = 'tool_use', 1, 0)) AS mode_hint,
  min(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS min_observed_event_time,
  max(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS max_observed_event_time,
  count() AS observed_events,
  argMinIfState(cwd, tuple(event_ts, event_uid), cwd != '') AS origin_cwd_state
FROM moraine.events
WHERE notEmpty(session_id)
GROUP BY session_id, source_host, source_name, source_file, source_generation, harness;

-- Locator MV: plain per-row projection; `sort_time` is the deterministic order
-- key so a re-insert version-collapses on the identical PK.
CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_locator_from_events
TO moraine.mcp_event_locator AS
SELECT
  event_uid,
  source_host,
  event_version,
  session_id,
  source_name,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time
FROM moraine.events
WHERE notEmpty(session_id);

-- Navigation MV: plain per-row projection of every fixed scalar. `sort_time`
-- (deterministic order key), `display_time` (v1 display expr), and the two
-- precomputed flags are the only computed columns; `is_metadata_bearing` reads
-- payload_json of the in-memory insert block only (no storage read, per-row
-- deterministic).
CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_navigation_from_events
TO moraine.mcp_event_navigation AS
SELECT
  session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_host,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  event_uid,
  event_version,
  source_name,
  event_ts,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind,
  actor_kind,
  payload_type,
  turn_index,
  tool_call_id,
  tool_name,
  tool_phase,
  op_status,
  item_id,
  harness,
  inference_provider,
  cwd,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))) AS is_metadata_bearing
FROM moraine.events
WHERE notEmpty(session_id);

-- ---------------------------------------------------------------------------
-- Guarded state seeds (pattern from migration 027): insert the control rows
-- only when absent so a re-run never resets an in-progress backfill or a
-- published readiness flag.
--   core_indexes  : backfill sweep cursor + coverage-ready flag
--   core_audit    : post-sweep overlap-audit outcome JSON
--   open_v2       : one-way `open` cutover flag (cursor records how it was set)
INSERT INTO moraine.mcp_read_index_state (state_key, ready, generation, cursor)
SELECT 'core_indexes', 0, 0, ''
WHERE NOT EXISTS (
  SELECT 1
  FROM moraine.mcp_read_index_state FINAL
  WHERE state_key = 'core_indexes'
);

INSERT INTO moraine.mcp_read_index_state (state_key, ready, generation, cursor)
SELECT 'core_audit', 0, 0, ''
WHERE NOT EXISTS (
  SELECT 1
  FROM moraine.mcp_read_index_state FINAL
  WHERE state_key = 'core_audit'
);

INSERT INTO moraine.mcp_read_index_state (state_key, ready, generation, cursor)
SELECT 'open_v2', 0, 0, ''
WHERE NOT EXISTS (
  SELECT 1
  FROM moraine.mcp_read_index_state FINAL
  WHERE state_key = 'open_v2'
);

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
-- Specifically: the line above that says the legacy projector "keeps running
-- as the compatibility reconciler until the projector-retirement PR" is
-- answered by `sql/041` — WI-10 *is* that PR. 036's own claim to be purely
-- additive is unaffected; it still adds only tables and materialized views,
-- and every derivation expression it transcribes is still pinned to
-- `moraine_clickhouse::canonical_derivations` by the migration content tests.
