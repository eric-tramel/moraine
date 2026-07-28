-- Issue #603 WI-08: bound bucket-4 operational telemetry.
--
-- There is no TTL anywhere in this repository before this migration, and no
-- retention configuration of any kind. Four Moraine tables grow without bound
-- and nothing reads more than the newest few rows of any of them:
--
--   * `ingest_heartbeats`      — only the LATEST row is read; one row every 5 s
--                                per host (~17 280/day). 1.34M rows on the
--                                reference host.
--   * `search_query_log`       — the single reader is a rolling 7-day hot-query
--                                window, so a 30-day horizon carries 4x
--                                headroom over the only horizon any code
--                                depends on.
--   * `search_hit_log`         — no readers; up to `result_limit` rows per
--                                query, so the highest-volume telemetry table
--                                by construction.
--   * `search_interaction_log` — no writer and no reader (migration 020 already
--                                says so); zero active parts on the reference
--                                host.
--
-- THIS MIGRATION DELETES NOTHING WHEN IT IS APPLIED (issue #603 §4 S5).
--
-- A plain `MODIFY TTL ts + INTERVAL 30 DAY DELETE` would, because
-- `materialize_ttl_after_modify` defaults to 1: the ALTER mutates existing
-- parts immediately and every row older than the horizon disappears on
-- upgrade. That is exactly the regression S5 names — "an upgrade that silently
-- drops three months of an operator's telemetry on first boot" — and the spec
-- is explicit that retention is never silently shortened during upgrade.
--
-- So the TTL is expressed against a `retention_anchor` column rather than
-- against the row's own timestamp:
--
--   1. `ADD COLUMN retention_anchor DateTime DEFAULT now()`
--   2. `MATERIALIZE COLUMN retention_anchor` stamps every EXISTING row with
--      the migration's own wall clock, synchronously.
--   3. `MODIFY TTL retention_anchor + INTERVAL 30 DAY DELETE`.
--
-- The effective horizon for a pre-existing row is therefore
-- `max(30 days, time since this migration ran)` measured from upgrade, and for
-- a newly inserted row it is 30 days from insert (its anchor defaults to the
-- insert clock). First application deletes nothing; steady state is bounded.
-- A row inserted with a backdated `ts` lives 30 days from INSERT rather than
-- from `ts`, which errs toward keeping data — the correct direction for a
-- guard whose failure mode is deleting too much.
--
-- The plan's OQ-4 recommends S5 for `ingest_heartbeats` and `search_query_log`
-- only, and plain TTLs for the other two "no readers, no operator
-- expectation" tables. This migration applies S5 to all four instead: the two
-- tables OQ-4 would treat plainly hold 7 927 and 0 rows on the reference host,
-- so the anchor costs nothing there, and applying it uniformly makes "this
-- migration removes no Moraine row" a property of the whole file rather than
-- of two statements in it. The headroom WI-08 actually buys comes from the
-- three ClickHouse system logs in `config/clickhouse.xml`, which are ~2.0 GiB
-- of rows older than 30 days on the reference host against ~9 MiB for all four
-- tables here.
--
-- `mutations_sync = 1` on the MATERIALIZE statements is load-bearing and is
-- NOT the setting the reclaimer is forbidden to use (that prohibition is about
-- lightweight cleanup DELETEs, which must stay cancellable). Here the TTL must
-- not be able to observe an unmaterialized anchor, so the stamp completes
-- before the ALTER that consumes it. Migration 020 uses the same setting for
-- the same reason.
--
-- HORIZON. 30 days is baked in as a literal because migration SQL is static
-- text. `retention.telemetry_horizon_days` reports the effective policy and is
-- asserted equal to this literal by
-- `migration_039_ttl_horizon_matches_the_configured_default`, so the config
-- default and the shipped TTL cannot drift apart silently.
--
-- Additive otherwise: no TRUNCATE, no DELETE, no view or MV recreation, no
-- reset of any projection state. Touches no bucket-1, bucket-2, or
-- never-delete table.

ALTER TABLE moraine.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS retention_anchor DateTime DEFAULT now();
ALTER TABLE moraine.ingest_heartbeats
  MATERIALIZE COLUMN retention_anchor SETTINGS mutations_sync = 1;
ALTER TABLE moraine.ingest_heartbeats
  MODIFY TTL retention_anchor + INTERVAL 30 DAY DELETE;

ALTER TABLE moraine.search_query_log
  ADD COLUMN IF NOT EXISTS retention_anchor DateTime DEFAULT now();
ALTER TABLE moraine.search_query_log
  MATERIALIZE COLUMN retention_anchor SETTINGS mutations_sync = 1;
ALTER TABLE moraine.search_query_log
  MODIFY TTL retention_anchor + INTERVAL 30 DAY DELETE;

ALTER TABLE moraine.search_hit_log
  ADD COLUMN IF NOT EXISTS retention_anchor DateTime DEFAULT now();
ALTER TABLE moraine.search_hit_log
  MATERIALIZE COLUMN retention_anchor SETTINGS mutations_sync = 1;
ALTER TABLE moraine.search_hit_log
  MODIFY TTL retention_anchor + INTERVAL 30 DAY DELETE;

ALTER TABLE moraine.search_interaction_log
  ADD COLUMN IF NOT EXISTS retention_anchor DateTime DEFAULT now();
ALTER TABLE moraine.search_interaction_log
  MATERIALIZE COLUMN retention_anchor SETTINGS mutations_sync = 1;
ALTER TABLE moraine.search_interaction_log
  MODIFY TTL retention_anchor + INTERVAL 30 DAY DELETE;
