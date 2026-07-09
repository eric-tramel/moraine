-- Stable person identity that produced each ingested record. This is distinct
-- from checkpoint `host`: one author can write from multiple machines, while
-- host continues to scope mirror checkpoints per machine.
--
-- Every statement below is individually idempotent: the migration runner
-- re-executes the whole file if any statement fails mid-way.

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS author String DEFAULT '';

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS author String DEFAULT '';
