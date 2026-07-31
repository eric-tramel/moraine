-- Issue #603 WI-09 — DDL for the canonical-generation reclaim fixture, split
-- from the inserts so the fresh-stack case can be proved against exactly this
-- schema with nothing in it (step 1 of the recipe below).
--
-- Reproduce (ClickHouse 25.12.5.44, the version-matched binary the stack
-- ships):
--
--   CH=~/.local/lib/moraine/clickhouse/current/bin/clickhouse
--   # 1. Fresh stack: the probe returns NOTHING against empty tables.
--   F=$(mktemp -d)
--   "$CH" local --path "$F" --multiquery < fixture_canonical_ddl.sql
--   "$CH" local --path "$F" --multiquery < expected/canonical.sql   # zero rows
--   # 2. Populated stack, uninterrupted unit: probe, then this build's
--   #    deletes + censuses.
--   D=$(mktemp -d)
--   "$CH" local --path "$D" --multiquery < fixture_canonical_ddl.sql
--   "$CH" local --path "$D" --multiquery < fixture_canonical.sql
--   "$CH" local --path "$D" --multiquery < expected/canonical.sql
--   "$CH" local --path "$D" --multiquery < expected/canonical_delete.sql
--   # 3. Populated stack, crash + re-drive: the interrupt script applies only
--   #    the satellite deletes (the process death between the satellite and
--   #    canonical deletes); the full delete script then RE-RUNS over the
--   #    half-state as the §3.2 re-drive — its satellite deletes remove zero
--   #    additional rows and the unit completes canonical-last.
--   R=$(mktemp -d)
--   "$CH" local --path "$R" --multiquery < fixture_canonical_ddl.sql
--   "$CH" local --path "$R" --multiquery < fixture_canonical.sql
--   "$CH" local --path "$R" --multiquery < expected/canonical_interrupt.sql
--   "$CH" local --path "$R" --multiquery < expected/canonical_delete.sql
--   # 4. Dual-attributed uid across two source NAMES (plan §7.2 G-DUALUID's
--   #    shape): the uid material excludes source_name, so one physical line
--   #    reachable through two overlapping source definitions on one host
--   #    shares ONE uid under two names; the retired name's unit must not
--   #    take the live name's rows.
--   X=$(mktemp -d)
--   "$CH" local --path "$X" --multiquery < fixture_canonical_ddl.sql
--   "$CH" local --path "$X" --multiquery < fixture_canonical_dualuid.sql
--   "$CH" local --path "$X" --multiquery < expected/canonical.sql
--   "$CH" local --path "$X" --multiquery < expected/canonical_dualuid_delete.sql
--
-- ## The DDL below is NOT the schema of record
--
-- Same contract as `fixture_read_index_ddl.sql`: each table is reduced to the
-- columns the probe, the deletes, and the censuses read, but every ENGINE,
-- ORDER BY and PARTITION BY is the shipped one (sql/001 + sql/004 as amended
-- by sql/032), because the extent arguments depend on them:
--
-- * `events` — ReplacingMergeTree(event_version) partitioned by
--   toYYYYMM(ingested_at) with `ingested_at` deliberately NOT in the sort
--   key, so FINAL dedups a re-ingested row across monthly partitions
--   (sql/019). The fixture's live generation carries exactly that shape: one
--   uid inserted in two months at two versions, physically two rows, one
--   under FINAL.
-- * `search_documents` — ORDER BY (event_uid, source_host): one row serves
--   both attributions of a double-attributed uid (hazard H2), which the
--   fixture's superseded generation carries.
-- * `search_postings` — ORDER BY (term, doc_id, source_host): the posting's
--   own source_file/source_generation are back-filled type defaults on
--   pre-032 rows (hazard H1), which the fixture's `/pre032.jsonl` posting
--   carries under a LIVE document.
-- * `tool_io` / `event_links` — no generation column at all (hazard H3), but
--   a `source_name` (sql/001), which the uid-set delete binds alongside the
--   host: the uid excludes both discriminators, so one physical line under
--   two overlapping source definitions carries one uid under two names (the
--   G-DUALUID shape the dual-uid fixture executes). Fixture uids are
--   prefixed `g<generation>u…` / `dU…` so the physical census can attribute
--   them after their parent `events` rows are gone.
--
-- The two publication views are stood in for by tables because
-- `clickhouse local` has nothing to derive them from; sql/031 is their
-- schema of record.

CREATE DATABASE IF NOT EXISTS moraine;

CREATE TABLE moraine.events (
  ingested_at DateTime64(3),
  event_uid String,
  session_id String,
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  event_ts DateTime64(3),
  event_version UInt64
) ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, event_ts, source_name, source_file, source_generation, source_offset, source_line_no, event_uid, source_host);

CREATE TABLE moraine.raw_events (
  ingested_at DateTime64(3),
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  event_uid String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_name, source_file, source_generation, source_offset, source_line_no, event_uid, source_host);

CREATE TABLE moraine.ingest_errors (
  ingested_at DateTime64(3),
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_name, source_file, source_generation, source_offset, source_line_no, source_host);

CREATE TABLE moraine.search_documents (
  doc_version UInt64,
  ingested_at DateTime64(3),
  event_uid String,
  session_id String,
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_ref String,
  doc_len UInt32
) ENGINE = ReplacingMergeTree(doc_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (event_uid, source_host);

CREATE TABLE moraine.search_postings (
  post_version UInt64,
  term String,
  doc_id String,
  session_id String,
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_ref String,
  doc_len UInt32,
  tf UInt32
) ENGINE = ReplacingMergeTree(post_version)
PARTITION BY cityHash64(term) % 32
ORDER BY (term, doc_id, source_host);

CREATE TABLE moraine.tool_io (
  ingested_at DateTime64(3),
  session_id String,
  tool_call_id String,
  event_uid String,
  source_host String,
  source_name LowCardinality(String),
  event_version UInt64,
  source_event_version UInt64
) ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, tool_call_id, event_uid, source_host);

CREATE TABLE moraine.event_links (
  ingested_at DateTime64(3),
  session_id String,
  event_uid String,
  link_type String,
  linked_event_uid String,
  source_host String,
  source_name LowCardinality(String),
  event_version UInt64,
  source_event_version UInt64
) ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (session_id, event_uid, link_type, linked_event_uid, source_host);

CREATE TABLE moraine.v_current_published_source_generations (
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  publication_revision UInt64,
  published_at DateTime64(3)
) ENGINE = MergeTree ORDER BY (source_host, source_name, source_file);

CREATE TABLE moraine.v_published_source_generation_history (
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  publication_revision UInt64,
  published_at DateTime64(3)
) ENGINE = MergeTree ORDER BY (source_host, source_name, source_file, source_generation);
