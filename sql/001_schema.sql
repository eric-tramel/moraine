CREATE DATABASE IF NOT EXISTS moraine;

CREATE TABLE IF NOT EXISTS moraine.raw_events (
  ingested_at DateTime64(3) DEFAULT now64(3),
  source_name LowCardinality(String),
  provider LowCardinality(String),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  record_ts String,
  top_type LowCardinality(String),
  session_id String,
  raw_json String,
  raw_json_hash UInt64,
  event_uid String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_name, source_file, source_generation, source_offset, source_line_no, event_uid);

CREATE TABLE IF NOT EXISTS moraine.events (
  ingested_at DateTime64(3) DEFAULT now64(3),
  event_uid String,
  session_id String,
  session_date Date,
  source_name LowCardinality(String),
  provider LowCardinality(String),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  source_ref String,
  record_ts String,
  event_ts DateTime64(3),
  event_kind LowCardinality(String),
  actor_kind LowCardinality(String),
  payload_type LowCardinality(String),
  op_kind LowCardinality(String),
  op_status LowCardinality(String),
  request_id String,
  trace_id String,
  turn_index UInt32,
  item_id String,
  tool_call_id String,
  parent_tool_call_id String,
  origin_event_id String,
  origin_tool_call_id String,
  tool_name LowCardinality(String),
  tool_phase LowCardinality(String),
  tool_error UInt8,
  agent_run_id String,
  agent_label String,
  coord_group_id String,
  coord_group_label String,
  is_substream UInt8,
  model LowCardinality(String),
  input_tokens UInt32,
  output_tokens UInt32,
  cache_read_tokens UInt32,
  cache_write_tokens UInt32,
  latency_ms UInt32,
  retry_count UInt16,
  service_tier LowCardinality(String),
  content_types Array(String),
  has_reasoning UInt8,
  text_content String,
  text_preview String,
  payload_json String,
  token_usage_json String,
  event_version UInt64,
  CONSTRAINT events_event_kind_domain CHECK event_kind IN (
    'session_meta', 'turn_context', 'message', 'tool_call', 'tool_result', 'reasoning',
    'event_msg', 'compacted_raw', 'progress', 'system', 'summary', 'queue_operation',
    'file_history_snapshot', 'unknown'
  ),
  CONSTRAINT events_payload_type_domain CHECK payload_type IN (
    'session_meta', 'turn_context', 'message', 'function_call', 'function_call_output',
    'custom_tool_call', 'custom_tool_call_output', 'web_search_call', 'reasoning',
    'response_item', 'event_msg', 'user_message', 'agent_message', 'agent_reasoning',
    'token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed',
    'search_results_received', 'compacted', 'thinking', 'tool_use', 'tool_result', 'text',
    'progress', 'system', 'summary', 'queue-operation', 'file-history-snapshot', 'unknown'
  )
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, event_ts, source_name, source_file, source_generation, source_offset, source_line_no, event_uid);

CREATE TABLE IF NOT EXISTS moraine.event_links (
  ingested_at DateTime64(3) DEFAULT now64(3),
  event_uid String,
  linked_event_uid String,
  linked_external_id String,
  link_type LowCardinality(String),
  session_id String,
  provider LowCardinality(String),
  source_name LowCardinality(String),
  metadata_json String,
  event_version UInt64,
  CONSTRAINT event_links_link_type_domain CHECK link_type IN (
    'parent_event', 'compacted_parent', 'parent_uuid', 'tool_use_id', 'source_tool_assistant',
    'unknown'
  )
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, event_uid, link_type, linked_event_uid);

CREATE TABLE IF NOT EXISTS moraine.tool_io (
  ingested_at DateTime64(3) DEFAULT now64(3),
  event_uid String,
  session_id String,
  provider LowCardinality(String),
  source_name LowCardinality(String),
  tool_call_id String,
  parent_tool_call_id String,
  tool_name LowCardinality(String),
  tool_phase LowCardinality(String),
  tool_error UInt8,
  input_json String,
  output_json String,
  output_text String,
  input_bytes UInt32,
  output_bytes UInt32,
  input_preview String,
  output_preview String,
  io_hash UInt64,
  source_ref String,
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, tool_call_id, event_uid);

CREATE TABLE IF NOT EXISTS moraine.ingest_errors (
  ingested_at DateTime64(3) DEFAULT now64(3),
  source_name LowCardinality(String),
  provider LowCardinality(String),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  error_kind LowCardinality(String),
  error_text String,
  raw_fragment String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_name, source_file, source_generation, source_offset, source_line_no);

CREATE TABLE IF NOT EXISTS moraine.ingest_checkpoints (
  updated_at DateTime64(3) DEFAULT now64(3),
  source_name LowCardinality(String),
  source_file String,
  source_inode UInt64,
  source_generation UInt32,
  last_offset UInt64,
  last_line_no UInt64,
  status LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(updated_at)
ORDER BY (source_name, source_file, source_generation);
