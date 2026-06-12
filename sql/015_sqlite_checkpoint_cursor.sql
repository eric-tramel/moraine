-- SQLite-polling checkpoint state for database-backed ingest sources
-- (issue #361). File-backed sources track byte offsets in `last_offset`;
-- SQLite sources need structured per-database cursor state instead:
--
--   * `cursor_json`        — authoritative poll cursor (kv hash map, file
--                            stat fingerprint, last error kind). Empty for
--                            jsonl/session_json sources.
--   * `source_fingerprint` — identity hash of the underlying database file,
--                            used to detect wholesale DB replacement.
--   * `schema_fingerprint` — hash of the relevant SQLite schema, used to
--                            distinguish schema drift from data changes.
--
-- Every statement below is individually idempotent: the migration runner
-- re-executes the whole file if any statement fails mid-way.

ALTER TABLE moraine.ingest_checkpoints
  ADD COLUMN IF NOT EXISTS cursor_json String DEFAULT '';

ALTER TABLE moraine.ingest_checkpoints
  ADD COLUMN IF NOT EXISTS source_fingerprint UInt64 DEFAULT 0;

ALTER TABLE moraine.ingest_checkpoints
  ADD COLUMN IF NOT EXISTS schema_fingerprint UInt64 DEFAULT 0;
