ALTER TABLE cortex.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS watcher_backend String DEFAULT 'unknown';

ALTER TABLE cortex.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS watcher_error_count UInt64 DEFAULT 0;

ALTER TABLE cortex.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS watcher_reset_count UInt64 DEFAULT 0;

ALTER TABLE cortex.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS watcher_last_reset_unix_ms UInt64 DEFAULT 0;
