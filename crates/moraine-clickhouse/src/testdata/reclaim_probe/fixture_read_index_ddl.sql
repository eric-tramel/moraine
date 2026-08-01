-- Issue #603 WI-07 — DDL for the read-index reclaim fixture, split from the
-- inserts so the fresh-stack case can be proved against exactly this schema
-- with nothing in it (step 2 of the recipe below).
--
-- Reproduce (ClickHouse 25.12.5.44, the version-matched binary the stack
-- ships):
--
--   CH=~/.local/lib/moraine/clickhouse/current/bin/clickhouse
--   # 1. Fresh stack: the probe returns NOTHING against empty tables.
--   F=$(mktemp -d)
--   "$CH" local --path "$F" --multiquery < fixture_read_index_ddl.sql
--   "$CH" local --path "$F" --multiquery < expected/read_index.sql   # zero rows
--   # 2. Populated stack: probe, then this build's deletes + censuses.
--   D=$(mktemp -d)
--   "$CH" local --path "$D" --multiquery < fixture_read_index_ddl.sql
--   "$CH" local --path "$D" --multiquery < fixture_read_index.sql
--   "$CH" local --path "$D" --multiquery < expected/read_index.sql
--   "$CH" local --path "$D" --multiquery < expected/read_index_delete.sql
--
-- ## The DDL below is NOT the schema of record
--
-- Same contract as `fixture_canonical_ddl.sql`: each table is reduced to the
-- columns the probe and the deletes read, but every ENGINE, ORDER BY and
-- PARTITION BY is the shipped one (`sql/036`), because the tuple-delete's
-- extent argument and the collapse behaviour both depend on them. The two
-- publication views are stood in for by tables because `clickhouse local` has
-- nothing to derive them from; `sql/031` is their schema of record.

CREATE DATABASE IF NOT EXISTS moraine;

CREATE TABLE moraine.mcp_event_navigation (
  session_id String,
  sort_time DateTime64(3),
  source_host String,
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  event_uid String,
  event_version UInt64,
  source_name LowCardinality(String)
) ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, sort_time, source_host, source_file, source_generation, source_offset, source_line_no, event_uid);

CREATE TABLE moraine.mcp_event_locator (
  event_uid String,
  source_host String,
  event_version UInt64,
  session_id String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32
) ENGINE = ReplacingMergeTree(event_version)
ORDER BY (event_uid, source_host);

CREATE TABLE moraine.mcp_session_directory (
  session_id String,
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  harness LowCardinality(String)
) ENGINE = AggregatingMergeTree
ORDER BY (session_id, source_host, source_name, source_file, source_generation, harness);

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
  source_generation UInt32
) ENGINE = MergeTree ORDER BY (source_host, source_name, source_file, source_generation);
