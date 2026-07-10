-- Per-backend mirror sink status on ingest heartbeats: a JSON-encoded
-- `{backend_name: status}` map (statuses: connecting / ok / lagging /
-- unreachable / disabled_skew) written by the default sink so /api/v1/health
-- can surface the health of every named-backend tee. Empty for installs
-- without named backends.
--
-- Every statement below is individually idempotent: the migration runner
-- re-executes the whole file if any statement fails mid-way.

ALTER TABLE moraine.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS backend_sinks String DEFAULT '';
