-- Content-free seek indexes for page-aware canonical MCP open.
-- `events` remains the sole text/payload authority. These relations retain
-- only ordering, version, turn-boundary, and metadata-presence scalars.
DROP VIEW IF EXISTS moraine.mv_mcp_event_navigation_seek_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_turn_seek_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_version_seek_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_session_mutation_signals_from_events;
DROP TABLE IF EXISTS moraine.mcp_event_navigation_seek;
DROP TABLE IF EXISTS moraine.mcp_event_turn_seek;
DROP TABLE IF EXISTS moraine.mcp_event_version_seek;
DROP TABLE IF EXISTS moraine.mcp_session_mutation_signals;


CREATE TABLE IF NOT EXISTS moraine.mcp_event_navigation_seek (
  session_id String,
  sort_time DateTime64(3),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  emission_index UInt32,
  event_uid String,
  event_version UInt64,
  event_ts DateTime64(3),
  turn_index UInt32,
  is_user_message UInt8,
  display_time DateTime64(3),
  event_kind LowCardinality(String),
  actor_kind LowCardinality(String),
  payload_type LowCardinality(String),
  tool_call_id String,
  tool_name LowCardinality(String),
  tool_phase LowCardinality(String),
  op_status LowCardinality(String),
  item_id String,
  harness LowCardinality(String),
  inference_provider LowCardinality(String),
  source_name LowCardinality(String),
  is_metadata_bearing UInt8,
  has_session_title UInt8,
  has_session_name UInt8,
  has_session_summary UInt8,
  has_session_slug UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  session_id,
  sort_time,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  emission_index,
  event_uid,
  event_version
) SETTINGS index_granularity = 256;

CREATE TABLE IF NOT EXISTS moraine.mcp_event_turn_seek (
  session_id String,
  turn_index UInt32,
  sort_time DateTime64(3),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  emission_index UInt32,
  event_uid String,
  event_version UInt64,
  event_ts DateTime64(3),
  is_user_message UInt8,
  display_time DateTime64(3),
  event_kind LowCardinality(String),
  actor_kind LowCardinality(String),
  payload_type LowCardinality(String),
  tool_call_id String,
  tool_name LowCardinality(String),
  tool_phase LowCardinality(String),
  op_status LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  session_id,
  turn_index,
  sort_time,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  emission_index,
  event_uid,
  event_version
) SETTINGS index_granularity = 256;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_navigation_seek_from_events
TO moraine.mcp_event_navigation_seek AS
SELECT
  session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  event_uid,
  event_version,
  event_ts,
  turn_index,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind,
  actor_kind,
  payload_type,
  tool_call_id,
  tool_name,
  tool_phase,
  op_status,
  item_id,
  harness,
  inference_provider,
  source_name,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))) AS is_metadata_bearing,
  toUInt8(notEmpty(JSONExtractString(payload_json, 'title'))) AS has_session_title,
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'name'))) AS has_session_name,
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'summary'))) AS has_session_summary,
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'slug'))) AS has_session_slug
FROM moraine.events
WHERE notEmpty(session_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_turn_seek_from_events
TO moraine.mcp_event_turn_seek AS
SELECT
  session_id,
  turn_index,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  event_uid,
  event_version,
  event_ts,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind,
  actor_kind,
  payload_type,
  tool_call_id,
  tool_name,
  tool_phase,
  op_status
FROM moraine.events
WHERE notEmpty(session_id) AND turn_index > 0;

INSERT INTO moraine.mcp_event_navigation_seek
SELECT
  session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')),
  event_uid,
  event_version,
  event_ts,
  turn_index,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at),
  event_kind,
  actor_kind,
  payload_type,
  tool_call_id,
  tool_name,
  tool_phase,
  op_status,
  item_id,
  harness,
  inference_provider,
  source_name,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))),
  toUInt8(notEmpty(JSONExtractString(payload_json, 'title'))),
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'name'))),
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'summary'))),
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'slug')))
FROM moraine.events FINAL
WHERE notEmpty(session_id)
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824,
  max_bytes_before_external_sort = 67108864;

INSERT INTO moraine.mcp_event_turn_seek
SELECT
  session_id,
  turn_index,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')),
  event_uid,
  event_version,
  event_ts,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at),
  event_kind,
  actor_kind,
  payload_type,
  tool_call_id,
  tool_name,
  tool_phase,
  op_status
FROM moraine.events FINAL
WHERE notEmpty(session_id) AND turn_index > 0
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824,
  max_bytes_before_external_sort = 67108864;

CREATE TABLE IF NOT EXISTS moraine.mcp_event_version_seek (
  event_uid String,
  event_version UInt64,
  session_id String,
  sort_time DateTime64(3),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  emission_index UInt32
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY event_uid
SETTINGS index_granularity = 64;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_version_seek_from_events
TO moraine.mcp_event_version_seek AS
SELECT
  event_uid,
  event_version,
  session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index
FROM moraine.events
WHERE notEmpty(session_id);

INSERT INTO moraine.mcp_event_version_seek
SELECT
  event_uid,
  event_version,
  session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index'))
FROM moraine.events FINAL
WHERE notEmpty(session_id)
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824,
  max_bytes_before_external_sort = 67108864;

CREATE TABLE IF NOT EXISTS moraine.mcp_session_mutation_signals (
  session_id String,
  unique_versions AggregateFunction(uniqExact, UInt64),
  fingerprint_a AggregateFunction(sumDistinct, UInt64),
  fingerprint_b AggregateFunction(sumDistinct, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY session_id
SETTINGS index_granularity = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_session_mutation_signals_from_events
TO moraine.mcp_session_mutation_signals AS
WITH
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  cityHash64(event_uid, event_version, sort_time, source_file, source_generation, source_offset, source_line_no, emission_index) AS row_hash_a,
  sipHash64(event_uid, event_version, sort_time, source_file, source_generation, source_offset, source_line_no, emission_index) AS row_hash_b
SELECT
  session_id,
  uniqExactState(row_hash_a) AS unique_versions,
  sumDistinctState(row_hash_a) AS fingerprint_a,
  sumDistinctState(row_hash_b) AS fingerprint_b
FROM moraine.events
WHERE notEmpty(session_id)
GROUP BY session_id;

INSERT INTO moraine.mcp_session_mutation_signals
WITH
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  cityHash64(event_uid, event_version, sort_time, source_file, source_generation, source_offset, source_line_no, emission_index) AS row_hash_a,
  sipHash64(event_uid, event_version, sort_time, source_file, source_generation, source_offset, source_line_no, emission_index) AS row_hash_b
SELECT
  session_id,
  uniqExactState(row_hash_a),
  sumDistinctState(row_hash_a),
  sumDistinctState(row_hash_b)
FROM moraine.events FINAL
WHERE notEmpty(session_id)
GROUP BY session_id
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824,
  max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864;
