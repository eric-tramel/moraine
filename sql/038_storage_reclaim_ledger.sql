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
