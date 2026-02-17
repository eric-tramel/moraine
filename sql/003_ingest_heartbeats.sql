CREATE TABLE IF NOT EXISTS moraine.ingest_heartbeats (
  ts DateTime64(3) DEFAULT now64(3),
  host String,
  service_version String,
  queue_depth UInt64,
  files_active UInt32,
  files_watched UInt32,
  rows_raw_written UInt64,
  rows_events_written UInt64,
  rows_errors_written UInt64,
  flush_latency_ms UInt32,
  append_to_visible_p50_ms UInt32,
  append_to_visible_p95_ms UInt32,
  last_error String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, host);
