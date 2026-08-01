-- Issue #603 WI-10: retire the legacy `mcp_open_*` projection storage.
--
-- This migration drops the eight physical tables, one materialized view, and
-- two plain views of the v1 `open` read model. The canonical read indexes
-- (migration 036) replaced every reader of this family; the code that ships
-- this migration contains no v1 reader, no projector, and no `mcp_open`
-- reclaim executor, so nothing can read or write these relations again.
--
-- GATED BY THE MIGRATION RUNNER (not by SQL). The runner refuses to apply
-- this migration until the issue-598 cutover is durable — `open_v2.ready = 1`
-- in `mcp_read_index_state` — or the family holds no PROJECTED rows (a fresh
-- install, where migrations 027–035 created the family moments ago and
-- projected nothing into it). On a host that never cut over, the runner defers
-- this migration with a named, actionable error, runs the canonical sweep +
-- audit, and then applies it in the same startup once `open_v2` publishes.
--
-- "No projected rows", not "no rows": migrations 027, 029, 033, 034 and 035
-- seed `mcp_open_projection_state` without reading any corpus — seven
-- `INSERT`s, none of which reads one; 027's is guarded, but only against the
-- marker table itself — so that marker table is non-empty on every store that
-- ran them, and "the projection holds no rows" would be unreachable. Measured
-- on a real ClickHouse 25.12.5.44 server with 001–040 applied to an empty
-- database, the family held 2 rows / 392 B and all of it was that marker (2
-- rather than 7 because ReplacingMergeTree had already collapsed the seeds;
-- the count varies with merge state and is never zero). The gate therefore
-- sums rows over the SEVEN content tables only. The byte figure the preflight
-- note reports still covers all eight, because all eight are what the drop
-- returns. The split is derived from these files rather than restated, by
-- `the_bookkeeping_table_is_the_one_the_migrations_seed_without_reading_data`.
--
-- Deferral rather than refusal, because `open_v2.ready = 1` is published only
-- after the canonical coverage sweep completes AND the persisted overlap audit
-- passes: a store that has not published it is still answering `open` out of
-- this projection, so dropping the family there would delete the only read
-- model that store has. Deferring costs such an operator one startup. If it
-- keeps deferring, the recovery is `moraine db core-index rebuild`, then
-- `moraine db core-index status` to confirm `open_v2` published, then
-- `moraine db migrate` — the same recipe the deferral's own error text
-- carries. See `ClickHouseClient::retirement_gate` and
-- `docs/operations/canonical-read-indexes.md`.
--
-- LEDGER SETTLEMENT (settle-by-drop). The `mcp_open_orphan` and
-- `mcp_open_retired_lineage` reclaim scopes CAN hold unsettled units
-- (`claimed`/`deleting`) on a host whose drain was interrupted, and this
-- statement exists for that host. It is DEFENSIVE, not corrective: on a host
-- with no unsettled unit in either scope it matches zero rows and does
-- nothing, which is what it does on the reference host — measured 2026-08-01,
-- that ledger holds 406 `mcp_open_orphan` units and 8
-- `mcp_open_retired_lineage` units and every one of them is already `done`.
--
-- Where the statement does match, it settles rather than refusing to upgrade
-- behind a drain the drop itself makes pointless: a reclaim unit's contract is
-- "the rows it names are no longer present", and dropping the tables satisfies
-- that contract for every unit at once. The phase written is `abandoned`, not
-- `done`: `done` is the driver's assertion that it executed the unit's
-- deletes, and fabricating that in a migration would falsify the audit trail.
-- `abandoned` says exactly what happened — the unit will never be driven,
-- because its entire target set was dropped. The SELECT reads FINAL so a
-- re-run (a crash between this statement and the drops re-applies the whole
-- migration) selects nothing the first pass already settled.
--
-- The projected constants deliberately carry NO aliases: ClickHouse resolves
-- a SELECT-list alias inside WHERE, so `'abandoned' AS phase` would make
-- `phase IN ('claimed', 'deleting')` compare the literal against itself and
-- settle nothing. Executed against a ClickHouse 25.12.5.44 server — the
-- version this product ships: with the alias the statement inserts zero rows;
-- without it, exactly the unsettled units. Positional column mapping through
-- the explicit column list needs no alias.
--
-- BYTES RETURNED. `DROP TABLE ... SYNC` deletes the parts before returning —
-- no merge deferral, unlike the lightweight DELETEs of the reclaim path, and
-- no `database_atomic_delay_before_drop_table_sec` wait either. The runner
-- measures `sum(data_compressed_bytes)` over the family's ACTIVE
-- `system.parts` rows immediately before applying this migration and reports
-- that figure in the migration's PREFLIGHT note. Compressed column bytes, not
-- `bytes_on_disk`: the two differ by the marks and checksum files, which is
-- roughly 8 MiB across this family on the reference host — enough to move the
-- rendered figure, so the column is named rather than left to "on disk". The
-- note opens with the words
--
--   retiring the legacy mcp_open_* projection: dropping the family returns
--
-- and then the measured size, row count and table count. That opening is the
-- `RETIREMENT_PROCEED_NOTE_PREFIX` constant the runner formats the note with,
-- and `the_migration_header_quotes_the_note_the_runner_emits` pins this
-- quotation against it — so an operator grepping their log for the words in
-- this header finds the line moraine actually printed, and the two cannot
-- drift apart while this file stays immutable.
--
-- The only footprint figure this header can honestly carry is a measurement,
-- not a forecast. The reference host held 28.36 GiB across 25,114,507 rows in
-- the eight tables when this migration was written (2026-08-01, read with the
-- runner's own query shape — `sum(data_compressed_bytes)` over active
-- `system.parts`), and 28.36 GiB is what the preflight note would have
-- rendered at that instant. Treat it as the scale of the thing; the preflight
-- note is the authority at drop time.
-- `the_header_footprint_figure_is_what_the_runner_would_print` pins this
-- figure against `retired_family_footprint_sql` and `format_binary_bytes`, so
-- the header and the runner cannot drift onto different columns again.
--
-- DROP ORDER. `mv_mcp_open_dirty_sessions_from_events` goes FIRST, before the
-- `mcp_open_dirty_sessions` it writes into. While that materialized view is
-- attached, every `INSERT INTO moraine.events` — the canonical bucket-1 ingest
-- write path — also writes through it, so dropping the target first makes
-- every ingest insert fail `Code: 60 ... (UNKNOWN_TABLE)` until the view goes.
-- Executed against a ClickHouse 25.12.5.44 server: with the target dropped
-- first the insert fails and its row is lost; in the shipped order an insert
-- issued between the two statements succeeds.
--
-- Then `mcp_open_projection_state`: it is the v1 readiness flag, so any
-- straggling down-level reader fails its readiness probe immediately and
-- cleanly instead of half-reading a disappearing projection. Then the child
-- tables, then `mcp_open_publication_headers` last — the same children-first,
-- authorization-parent-last discipline the reclaim driver uses, so a reader
-- that somehow pinned a header mid-drop finds children missing (a typed
-- retry/miss) rather than an authorized-but-partial answer.

INSERT INTO moraine.storage_reclaim_ledger
  (reclaim_id, scope, source_host, source_name, source_file, source_generation,
   session_id, candidate_generation, phase, estimated_rows, estimated_bytes,
   claimed_at, ledger_revision)
SELECT
  reclaim_id, scope, source_host, source_name, source_file, source_generation,
  session_id, candidate_generation, 'abandoned', estimated_rows,
  estimated_bytes, claimed_at, generateSnowflakeID()
FROM moraine.storage_reclaim_ledger FINAL
WHERE scope IN ('mcp_open_orphan', 'mcp_open_retired_lineage')
  AND phase IN ('claimed', 'deleting');

DROP VIEW IF EXISTS moraine.mv_mcp_open_dirty_sessions_from_events;

DROP VIEW IF EXISTS moraine.v_mcp_open_publication_headers;

DROP VIEW IF EXISTS moraine.v_current_mcp_open_generation_readiness;

DROP TABLE IF EXISTS moraine.mcp_open_projection_state SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_events SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_turns SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_sessions SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_dirty_sessions SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_generation_readiness SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_backfill_plans SYNC;

DROP TABLE IF EXISTS moraine.mcp_open_publication_headers SYNC;
