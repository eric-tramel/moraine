-- Issue #603 WI-04: the storage reclaim ledger.
--
-- Purely additive: one `CREATE TABLE IF NOT EXISTS`. No TRUNCATE, no DELETE,
-- no view or materialized-view recreation, no reset of any projection state.
-- Re-runs are no-ops. **This migration installs no executor and reclaims
-- nothing**; it exists so that the durable state the reclaimer needs is in
-- place, and reviewable, before anything is ever deleted.
--
-- WHY A LEDGER AT ALL. The existing `mcp_open` reclaim path derives its target
-- set *from headers* and then deletes the headers first. A crash between the
-- two statements strands the child rows forever, because the set can never be
-- re-derived from what remains: no header means no probe can see them, and no
-- reader can authorize them either. The reference host carries ~10.9M such
-- rows (58% of `mcp_open_events`), still accumulating, dating from the PR-604
-- incident.
--
-- Making the claimed set durable *independently of the rows it names* is what
-- lets the new driver delete children first and the parent last. A crash then
-- leaves an intact, still-authorizable snapshot rather than an unreachable
-- orphan, and the next run re-drives the unit from this table. Reader safety
-- is preserved by the safety horizon and the anti-join — a unit is already
-- non-live before it is claimed — not by delete ordering.
--
-- ENGINE CHOICE. `ReplacingMergeTree(ledger_revision)` keyed on
-- `(scope, reclaim_id)`: a phase advance is an INSERT carrying a fresher
-- snowflake revision, never an UPDATE and never a DELETE. That keeps the whole
-- lifecycle append-only, so a partially applied phase transition is impossible
-- and the ledger itself needs no mutation machinery to maintain.
--
-- NOT PARTITIONED. The table is bounded by the number of units ever claimed
-- and each row is a few hundred bytes; a partition key would add merge work
-- for no reclaim benefit, and the ledger is classified `never_delete` anyway
-- (`crates/moraine-clickhouse/src/storage_class.rs`) — a reclaimer able to
-- delete its own ledger would reintroduce exactly the stranded-child bug this
-- table exists to fix.

CREATE TABLE IF NOT EXISTS moraine.storage_reclaim_ledger (
  -- Snowflake, one per claimed unit.
  reclaim_id           String,
  -- `mcp_open_orphan` | `read_index_generation` | `canonical_generation`.
  -- Kept as a string rather than an enum so adding a scope in a later work
  -- item is a code change, not a schema change.
  scope                LowCardinality(String),
  -- Source identity for generation-scoped units. Empty/zero for units scoped
  -- by (session, candidate generation) instead.
  source_host          String,
  source_name          String,
  source_file          String,
  source_generation    UInt32,
  -- Session identity for snapshot-scoped units. Empty for generation-scoped
  -- units.
  session_id           String,
  candidate_generation UInt64,
  -- `claimed` | `deleting` | `done` | `abandoned`.
  phase                LowCardinality(String),
  -- Written at claim time from `system.parts` ranges, never from a FINAL scan.
  -- These are estimates and every surface that reports them says so: nothing
  -- is partitioned by `source_generation`, so a lightweight DELETE masks rows
  -- and returns bytes only when a background merge rewrites the part.
  estimated_rows       UInt64,
  estimated_bytes      UInt64,
  claimed_at           DateTime64(3) DEFAULT now64(3),
  ledger_revision      UInt64
)
ENGINE = ReplacingMergeTree(ledger_revision)
ORDER BY (scope, reclaim_id);

-- SUPERSEDED 2026-08-01 (issue #603 WI-10, `sql/041`). The `mcp_open_*`
-- projection this file names is dropped by migration 041; the code that ships
-- 041 carries no projector, no v1 reader and no `mcp_open` reclaim executor,
-- so every present-tense description of that machinery here records the state
-- when this file was written, not a running component. Nothing else in this
-- file is edited and every statement in it still executes verbatim — a fresh
-- install creates the family and 041 drops it in the same migrate pass. A
-- released migration is immutable and the runner keys applied migrations by
-- `(version, name)` with no content checksum, so an upgraded host never
-- re-reads this file: this note is append-only, and it is here for the
-- operator who reads the file on a fresh install.
--
-- It sits at the FOOT of this file on purpose. Source elsewhere in the tree
-- cites these statements by `sql/NNN:LINE`, and a note at the head moves
-- every one of those citations silently; appended here, no existing line
-- number changes. `a_cross_file_line_citation_resolves_to_what_it_claims`
-- is what keeps them honest either way.
--
-- Two specifics. (1) The "WHY A LEDGER AT ALL" paragraph is historical: the
-- `mcp_open` reclaim path it describes is gone from the binary and the ~10.9M
-- orphan `mcp_open_events` rows it counts went with the table (`sql/041`). The
-- argument for the ledger's shape is unaffected — the live
-- `read_index_generation` and `canonical_generation` executors delete children
-- first and the parent last for exactly the reason recorded above. (2) The
-- `scope` column comment above lists `mcp_open_orphan`. No executor can write
-- that value any more (`ReclaimScope::parse` rejects the retired strings), but
-- it is still a legitimate historical value of the column: `sql/041` settles
-- surviving units of that scope as `abandoned` rather than deleting them, and
-- this table is classified `never_delete`, so a host that ran the legacy
-- reclaimer keeps those rows forever. Read that comment as the column's
-- domain, not as the set of scopes a live executor produces.
