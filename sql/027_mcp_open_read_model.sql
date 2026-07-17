-- Canonical, entity-keyed read model for typed MCP open.
--
-- Complete affected sessions are rebuilt into the inactive child slot. The
-- session row is inserted last and is therefore the publication barrier. Every
-- child read filters both slot and generation; an incomplete rebuild cannot
-- replace or become visible through the active slot.

CREATE TABLE IF NOT EXISTS moraine.mcp_open_sessions (
  session_id String,
  slot UInt8,
  generation UInt64,
  source_revision UInt64,
  dirty_revision UInt64,
  first_event_time DateTime64(3),
  last_event_time DateTime64(3),
  total_turns UInt32,
  total_events UInt64,
  user_messages UInt64,
  assistant_messages UInt64,
  tool_calls UInt64,
  tool_results UInt64,
  mode LowCardinality(String),
  first_event_uid String,
  last_event_uid String,
  last_actor_role LowCardinality(String),
  title String,
  source String,
  harness LowCardinality(String),
  inference_provider LowCardinality(String),
  session_slug String,
  session_summary String,
  completed UInt8,
  terminal_event_uid String,
  origin_cwd String,
  projected_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id);

CREATE TABLE IF NOT EXISTS moraine.mcp_open_turns (
  session_id String,
  slot UInt8,
  generation UInt64,
  turn_seq UInt32,
  turn_id String DEFAULT '',
  started_at DateTime64(3) DEFAULT toDateTime64(0, 3),
  ended_at DateTime64(3) DEFAULT toDateTime64(0, 3),
  total_events UInt64 DEFAULT 0,
  user_messages UInt64 DEFAULT 0,
  assistant_messages UInt64 DEFAULT 0,
  tool_calls UInt64 DEFAULT 0,
  tool_results UInt64 DEFAULT 0,
  reasoning_items UInt64 DEFAULT 0,
  user_input_summary_source String DEFAULT '',
  final_response_summary_source String DEFAULT '',
  user_input_summary_is_payload UInt8 DEFAULT 0,
  final_response_summary_is_payload UInt8 DEFAULT 0,
  user_input_event_uid String DEFAULT '',
  user_input_event_order UInt64 DEFAULT 0,
  user_input_event_time DateTime64(3) DEFAULT toDateTime64(0, 3),
  user_input_event_type LowCardinality(String) DEFAULT '',
  final_response_event_uid String DEFAULT '',
  final_response_event_order UInt64 DEFAULT 0,
  final_response_event_time DateTime64(3) DEFAULT toDateTime64(0, 3),
  final_response_event_type LowCardinality(String) DEFAULT '',
  tools_called Array(String) DEFAULT [],
  normalized_event_types Array(String) DEFAULT [],
  completed UInt8 DEFAULT 0,
  terminal_event_uid String DEFAULT '',
  first_event_uid String DEFAULT '',
  first_event_order UInt64 DEFAULT 0,
  first_event_time DateTime64(3) DEFAULT toDateTime64(0, 3),
  first_event_type LowCardinality(String) DEFAULT '',
  last_event_uid String DEFAULT '',
  last_event_order UInt64 DEFAULT 0,
  last_event_time DateTime64(3) DEFAULT toDateTime64(0, 3),
  last_event_type LowCardinality(String) DEFAULT '',
  previous_turn_seq UInt32 DEFAULT 0,
  previous_turn_id String DEFAULT '',
  previous_turn_started_at DateTime64(3) DEFAULT toDateTime64(0, 3),
  previous_turn_ended_at DateTime64(3) DEFAULT toDateTime64(0, 3),
  next_turn_seq UInt32 DEFAULT 0,
  next_turn_id String DEFAULT '',
  next_turn_started_at DateTime64(3) DEFAULT toDateTime64(0, 3),
  next_turn_ended_at DateTime64(3) DEFAULT toDateTime64(0, 3),
  event_summaries_json String DEFAULT '[]',
  projected_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, slot, turn_seq);

CREATE TABLE IF NOT EXISTS moraine.mcp_open_events (
  event_uid String,
  slot UInt8,
  generation UInt64,
  session_id String,
  event_order UInt64,
  turn_seq UInt32,
  event_time DateTime64(3),
  actor_role LowCardinality(String),
  event_class LowCardinality(String),
  payload_type LowCardinality(String),
  event_type LowCardinality(String),
  event_ordinal UInt32,
  call_id String,
  name LowCardinality(String),
  phase LowCardinality(String),
  item_id String,
  source_ref String,
  text_content String,
  payload_json String,
  token_usage_json String,
  endpoint_kind LowCardinality(String),
  token_usage_buckets Map(String, UInt64),
  token_usage_native_units Map(String, Float64),
  previous_event_uid String,
  next_event_uid String,
  projected_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY cityHash64(event_uid) % 64
ORDER BY (event_uid, slot);

CREATE TABLE IF NOT EXISTS moraine.mcp_open_dirty_sessions (
  session_id String,
  dirty_revision UInt64,
  observed_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(dirty_revision)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_open_dirty_sessions_from_events
TO moraine.mcp_open_dirty_sessions AS
SELECT
  session_id,
  generateSnowflakeID() AS dirty_revision,
  now64(3) AS observed_at
FROM moraine.events
WHERE notEmpty(session_id)
GROUP BY session_id;

CREATE TABLE IF NOT EXISTS moraine.mcp_open_projection_state (
  state_key LowCardinality(String),
  ready UInt8,
  generation UInt64,
  backfill_cursor String,
  updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(generation)
ORDER BY (state_key);

INSERT INTO moraine.mcp_open_projection_state (state_key, ready, generation, backfill_cursor)
SELECT 'global', 0, 0, ''
WHERE NOT EXISTS (
  SELECT 1
  FROM moraine.mcp_open_projection_state FINAL
  WHERE state_key = 'global'
);
