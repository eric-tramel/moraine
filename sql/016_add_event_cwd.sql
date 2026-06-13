-- Working directory of the agent session that produced each record, extracted
-- from session content by the ingest normalizer (record-level where the
-- harness provides it, e.g. claude-code; session-level fallback where only the
-- session metadata carries it, e.g. codex session_meta). Harnesses without a
-- discoverable cwd leave it empty. Used for per-project backend routing; not
-- propagated into the search index tables.
--
-- Every statement below is individually idempotent: the migration runner
-- re-executes the whole file if any statement fails mid-way.

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS cwd String DEFAULT '';

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS cwd String DEFAULT '';
