-- Person identity of the installation that produced each record, stamped at
-- ingest from `[identity].author` (issue #381). Distinct from `host`: one
-- person's author is identical across all their machines, while host stays
-- per-machine. Empty when the install has no identity configured — local
-- private data needs no attribution. Not propagated into the search index
-- tables (deferred to the team-usability phase).
--
-- Every statement below is individually idempotent: the migration runner
-- re-executes the whole file if any statement fails mid-way.

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS author String DEFAULT '';

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS author String DEFAULT '';
