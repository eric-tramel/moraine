-- Host that wrote each ingest checkpoint row. Backend mirror sinks share one
-- ingest_checkpoints table per team backend, so rows must be scoped per host:
-- without it, identical absolute session paths on two machines clobber each
-- other's offsets (silent skips on the mirror) and every teammate's files
-- look deleted to the catch-up replay pass. The host joins the sort key so
-- the ReplacingMergeTree dedups per (file, host) instead of collapsing one
-- member's progress with another's. The default backend stays single-writer
-- and keeps writing rows with an empty host.
--
-- Adding the column and extending the sort key must happen in one ALTER
-- (ClickHouse only accepts new sorting-key columns added in the same query),
-- and the column must carry no DEFAULT clause (defaulted columns are
-- forbidden in sorting-key extensions) — the String type default is ''
-- for old rows and host-less inserts alike. The statement is idempotent:
-- ADD COLUMN IF NOT EXISTS is a no-op on re-run and MODIFY ORDER BY to the
-- already-current key is accepted.

ALTER TABLE moraine.ingest_checkpoints
  ADD COLUMN IF NOT EXISTS host String,
  MODIFY ORDER BY (source_name, source_file, source_generation, host);
