-- Atomic source-generation publication control (issue #602).
--
-- Every relation in this migration is append/replacement based.  There are no
-- deletes: published generation history must remain available so readers can
-- reconstruct the exact head set captured at an earlier publication revision.

CREATE TABLE IF NOT EXISTS moraine.published_source_generations (
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  publication_revision UInt64,
  publisher_id String,
  operation_id String,
  published_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(publication_revision)
ORDER BY (source_host, source_name, source_file, source_generation);

CREATE TABLE IF NOT EXISTS moraine.ingest_checkpoint_transitions (
  host String,
  source_name LowCardinality(String),
  source_file String,
  inode UInt64,
  source_generation UInt32,
  last_offset UInt64,
  last_line UInt64,
  cursor_json String DEFAULT '',
  source_fingerprint UInt64 DEFAULT 0,
  schema_fingerprint UInt64 DEFAULT 0,
  sqlite_mtime_ns Int64 DEFAULT 0,
  sqlite_size UInt64 DEFAULT 0,
  checkpoint_revision UInt64,
  operation_id String,
  lifecycle LowCardinality(String),
  protocol_version UInt16,
  scan_inode UInt64,
  scan_boundary UInt64,
  policy_fingerprint String,
  final_scan_complete UInt8,
  block_reason String,
  compatibility_prepared UInt8,
  backend_caught_up UInt8,
  append_batch_id String,
  cache_epoch UInt64,
  updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(checkpoint_revision)
ORDER BY (host, source_name, source_file, source_generation);

CREATE TABLE IF NOT EXISTS moraine.source_generation_publication_readiness (
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  readiness_revision UInt64,
  checkpoint_revision UInt64,
  operation_id String,
  complete UInt8,
  block_reason String,
  compatibility_prepared UInt8,
  backend_caught_up UInt8,
  manifest_digest String,
  updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(readiness_revision)
ORDER BY (source_host, source_name, source_file, source_generation);

CREATE TABLE IF NOT EXISTS moraine.ingest_append_control (
  host String,
  control_revision UInt64,
  cache_epoch UInt64,
  state LowCardinality(String),
  batch_id String,
  publisher_id String,
  manifest_json String,
  insert_only UInt8,
  updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(control_revision)
ORDER BY (host);

CREATE TABLE IF NOT EXISTS moraine.publication_diagnostic_events (
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  diagnostic_kind LowCardinality(String),
  diagnostic_revision UInt64,
  detail String,
  active UInt8,
  observed_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(diagnostic_revision)
ORDER BY (source_host, source_name, source_file, diagnostic_kind);

-- Collapse response-loss retries for one generation, while preserving a row
-- for every generation ever published.  Consumers reconstruct an as-of head
-- set by applying a revision bound to this view and grouping by source key.
CREATE VIEW IF NOT EXISTS moraine.v_published_source_generation_history AS
SELECT
  source_host,
  source_name,
  source_file,
  source_generation,
  tupleElement(publication, 1) AS publication_revision,
  tupleElement(publication, 2) AS publisher_id,
  tupleElement(publication, 3) AS operation_id,
  tupleElement(publication, 4) AS published_at
FROM
(
  SELECT
    source_host,
    source_name,
    source_file,
    source_generation,
    argMax(
      tuple(publication_revision, publisher_id, operation_id, published_at),
      tuple(publication_revision, publisher_id, operation_id)
    ) AS publication
  FROM moraine.published_source_generations
  GROUP BY source_host, source_name, source_file, source_generation
);

CREATE VIEW IF NOT EXISTS moraine.v_current_published_source_generations AS
SELECT
  source_host,
  source_name,
  source_file,
  tupleElement(publication, 1) AS source_generation,
  tupleElement(publication, 2) AS publication_revision,
  tupleElement(publication, 3) AS publisher_id,
  tupleElement(publication, 4) AS operation_id,
  tupleElement(publication, 5) AS published_at
FROM
(
  SELECT
    source_host,
    source_name,
    source_file,
    argMax(
      tuple(
        source_generation,
        publication_revision,
        publisher_id,
        operation_id,
        published_at
      ),
      tuple(publication_revision, source_generation, publisher_id, operation_id)
    ) AS publication
  FROM moraine.v_published_source_generation_history
  GROUP BY source_host, source_name, source_file
);

-- Each helper loads one complete causal tuple with one argMax.  Independent
-- argMax expressions are intentionally avoided: equal legacy timestamps used
-- to permit fields from different checkpoint rows to be mixed together.
CREATE VIEW IF NOT EXISTS moraine.v_current_ingest_checkpoint_transitions AS
SELECT
  host,
  source_name,
  source_file,
  tupleElement(checkpoint, 1) AS inode,
  tupleElement(checkpoint, 2) AS source_generation,
  tupleElement(checkpoint, 3) AS last_offset,
  tupleElement(checkpoint, 4) AS last_line,
  tupleElement(checkpoint, 5) AS cursor_json,
  tupleElement(checkpoint, 6) AS source_fingerprint,
  tupleElement(checkpoint, 7) AS schema_fingerprint,
  tupleElement(checkpoint, 8) AS sqlite_mtime_ns,
  tupleElement(checkpoint, 9) AS sqlite_size,
  tupleElement(checkpoint, 10) AS checkpoint_revision,
  tupleElement(checkpoint, 11) AS operation_id,
  tupleElement(checkpoint, 12) AS lifecycle,
  tupleElement(checkpoint, 13) AS protocol_version,
  tupleElement(checkpoint, 14) AS scan_inode,
  tupleElement(checkpoint, 15) AS scan_boundary,
  tupleElement(checkpoint, 16) AS policy_fingerprint,
  tupleElement(checkpoint, 17) AS final_scan_complete,
  tupleElement(checkpoint, 18) AS block_reason,
  tupleElement(checkpoint, 19) AS compatibility_prepared,
  tupleElement(checkpoint, 20) AS backend_caught_up,
  tupleElement(checkpoint, 21) AS append_batch_id,
  tupleElement(checkpoint, 22) AS cache_epoch,
  tupleElement(checkpoint, 23) AS updated_at
FROM
(
  SELECT
    host,
    source_name,
    source_file,
    argMax(
      tuple(
        inode,
        source_generation,
        last_offset,
        last_line,
        cursor_json,
        source_fingerprint,
        schema_fingerprint,
        sqlite_mtime_ns,
        sqlite_size,
        checkpoint_revision,
        operation_id,
        lifecycle,
        protocol_version,
        scan_inode,
        scan_boundary,
        policy_fingerprint,
        final_scan_complete,
        block_reason,
        compatibility_prepared,
        backend_caught_up,
        append_batch_id,
        cache_epoch,
        updated_at
      ),
      tuple(source_generation, checkpoint_revision, operation_id)
    ) AS checkpoint
  FROM moraine.ingest_checkpoint_transitions
  GROUP BY host, source_name, source_file
);

CREATE VIEW IF NOT EXISTS moraine.v_current_source_generation_publication_readiness AS
SELECT
  source_host,
  source_name,
  source_file,
  source_generation,
  tupleElement(readiness, 1) AS readiness_revision,
  tupleElement(readiness, 2) AS checkpoint_revision,
  tupleElement(readiness, 3) AS operation_id,
  tupleElement(readiness, 4) AS complete,
  tupleElement(readiness, 5) AS block_reason,
  tupleElement(readiness, 6) AS compatibility_prepared,
  tupleElement(readiness, 7) AS backend_caught_up,
  tupleElement(readiness, 8) AS manifest_digest,
  tupleElement(readiness, 9) AS updated_at
FROM
(
  SELECT
    source_host,
    source_name,
    source_file,
    source_generation,
    argMax(
      tuple(
        readiness_revision,
        checkpoint_revision,
        operation_id,
        complete,
        block_reason,
        compatibility_prepared,
        backend_caught_up,
        manifest_digest,
        updated_at
      ),
      tuple(readiness_revision, operation_id)
    ) AS readiness
  FROM moraine.source_generation_publication_readiness
  GROUP BY source_host, source_name, source_file, source_generation
);

CREATE VIEW IF NOT EXISTS moraine.v_current_ingest_append_control AS
SELECT
  host,
  tupleElement(control, 1) AS control_revision,
  tupleElement(control, 2) AS cache_epoch,
  tupleElement(control, 3) AS state,
  tupleElement(control, 4) AS batch_id,
  tupleElement(control, 5) AS publisher_id,
  tupleElement(control, 6) AS manifest_json,
  tupleElement(control, 7) AS insert_only,
  tupleElement(control, 8) AS updated_at
FROM
(
  SELECT
    host,
    argMax(
      tuple(
        control_revision,
        cache_epoch,
        state,
        batch_id,
        publisher_id,
        manifest_json,
        insert_only,
        updated_at
      ),
      tuple(control_revision, publisher_id, batch_id, state)
    ) AS control
  FROM moraine.ingest_append_control
  GROUP BY host
);

-- Seed the default-local cache fence.  Named/shared hosts initialize their own
-- row through the serialized publication actor.
INSERT INTO moraine.ingest_append_control
  (host, control_revision, cache_epoch, state, batch_id, publisher_id, manifest_json, insert_only)
SELECT '', 0, 0, 'idle', '', 'migration-031', '{}', 0
WHERE NOT EXISTS (
  SELECT 1 FROM moraine.v_current_ingest_append_control WHERE host = ''
);

-- Deterministically lift legacy checkpoints into causal transitions.  If two
-- distinct rows share the greatest millisecond timestamp for one generation,
-- that generation is blocked rather than selecting a synthetic mixed cursor.
INSERT INTO moraine.ingest_checkpoint_transitions
SELECT
  legacy.host,
  legacy.source_name,
  legacy.source_file,
  tupleElement(legacy.chosen, 1) AS inode,
  legacy.source_generation,
  tupleElement(legacy.chosen, 2) AS last_offset,
  tupleElement(legacy.chosen, 3) AS last_line,
  tupleElement(legacy.chosen, 5) AS cursor_json,
  tupleElement(legacy.chosen, 6) AS source_fingerprint,
  tupleElement(legacy.chosen, 7) AS schema_fingerprint,
  toInt64(0) AS sqlite_mtime_ns,
  toUInt64(0) AS sqlite_size,
  greatest(toUInt64(toUnixTimestamp64Milli(legacy.latest_updated_at)), toUInt64(1)) AS checkpoint_revision,
  concat('legacy:', hex(cityHash64(
    legacy.host, legacy.source_name, legacy.source_file, legacy.source_generation
  ))) AS operation_id,
  if(legacy.latest_variants > 1, 'error', tupleElement(legacy.chosen, 4)) AS lifecycle,
  toUInt16(0) AS protocol_version,
  tupleElement(legacy.chosen, 1) AS scan_inode,
  tupleElement(legacy.chosen, 2) AS scan_boundary,
  '' AS policy_fingerprint,
  toUInt8(legacy.latest_variants = 1 AND tupleElement(legacy.chosen, 4) = 'active') AS final_scan_complete,
  if(legacy.latest_variants > 1, 'legacy_equal_timestamp_ambiguity', '') AS block_reason,
  toUInt8(legacy.latest_variants = 1 AND tupleElement(legacy.chosen, 4) = 'active') AS compatibility_prepared,
  toUInt8(legacy.latest_variants = 1 AND tupleElement(legacy.chosen, 4) = 'active') AS backend_caught_up,
  '' AS append_batch_id,
  toUInt64(0) AS cache_epoch,
  legacy.latest_updated_at AS updated_at
FROM
(
  SELECT
    host,
    source_name,
    source_file,
    source_generation,
    latest_updated_at,
    argMax(
      tuple(
        source_inode,
        last_offset,
        last_line_no,
        status,
        cursor_json,
        source_fingerprint,
        schema_fingerprint
      ),
      tuple(
        source_inode,
        last_offset,
        last_line_no,
        status,
        cursor_json,
        source_fingerprint,
        schema_fingerprint
      )
    ) AS chosen,
    uniqExact(tuple(
      source_inode,
      last_offset,
      last_line_no,
      status,
      cursor_json,
      source_fingerprint,
      schema_fingerprint
    )) AS latest_variants
  FROM
  (
    SELECT
      *,
      max(updated_at) OVER (
        PARTITION BY host, source_name, source_file, source_generation
      ) AS latest_updated_at
    FROM moraine.ingest_checkpoints
  )
  WHERE updated_at = latest_updated_at
  GROUP BY host, source_name, source_file, source_generation, latest_updated_at
) AS legacy
WHERE tuple(
  legacy.host,
  toString(legacy.source_name),
  legacy.source_file,
  legacy.source_generation
) NOT IN
(
  SELECT tuple(host, toString(source_name), source_file, source_generation)
  FROM moraine.ingest_checkpoint_transitions
  GROUP BY host, source_name, source_file, source_generation
);

-- Publish the greatest unambiguous active legacy generation for each source.
-- Only unpublished keys enter the window, and the allocation starts above the
-- durable global maximum.  Thus an interrupted/response-lost rerun neither
-- reuses an existing revision nor consumes a second one for an inserted head.
INSERT INTO moraine.published_source_generations
  (source_host, source_name, source_file, source_generation, publication_revision,
   publisher_id, operation_id, published_at)
WITH
  (
    SELECT ifNull(max(history.publication_revision), toUInt64(0))
    FROM moraine.v_published_source_generation_history AS history
  ) AS base_revision
SELECT
  candidate.host AS source_host,
  candidate.source_name,
  candidate.source_file,
  candidate.source_generation,
  base_revision + toUInt64(row_number() OVER (
    ORDER BY candidate.host, candidate.source_name, candidate.source_file
  )) AS publication_revision,
  'migration-031' AS publisher_id,
  concat('legacy-head:', hex(cityHash64(
    candidate.host,
    candidate.source_name,
    candidate.source_file,
    candidate.source_generation
  ))) AS operation_id,
  candidate.updated_at AS published_at
FROM
(
  SELECT
    host,
    source_name,
    source_file,
    tupleElement(active_checkpoint, 1) AS source_generation,
    tupleElement(active_checkpoint, 2) AS updated_at
  FROM
  (
    SELECT
      host,
      source_name,
      source_file,
      argMax(
        tuple(source_generation, updated_at),
        tuple(source_generation, checkpoint_revision)
      ) AS active_checkpoint
    FROM moraine.ingest_checkpoint_transitions FINAL
    WHERE lifecycle = 'active'
      AND final_scan_complete = 1
      AND block_reason = ''
    GROUP BY host, source_name, source_file
  )
) AS candidate
WHERE tuple(
  candidate.host,
  toString(candidate.source_name),
  candidate.source_file,
  candidate.source_generation
) NOT IN
(
  SELECT tuple(source_host, toString(source_name), source_file, source_generation)
  FROM moraine.v_published_source_generation_history
);

-- Legacy heads were already reader-visible before this protocol existed.
-- Record their deterministic upgrade readiness so repair does not mistake a
-- successfully migrated head for an unpublished candidate.  Migration 033
-- invalidates and rebuilds the compatibility cache from canonical live rows.
INSERT INTO moraine.source_generation_publication_readiness
  (source_host, source_name, source_file, source_generation, readiness_revision,
   checkpoint_revision, operation_id, complete, block_reason,
   compatibility_prepared, backend_caught_up, manifest_digest, updated_at)
SELECT
  head.source_host,
  head.source_name,
  head.source_file,
  head.source_generation,
  head.publication_revision AS readiness_revision,
  checkpoint.checkpoint_revision,
  concat('legacy-readiness:', hex(cityHash64(
    head.source_host, head.source_name, head.source_file, head.source_generation
  ))) AS operation_id,
  toUInt8(1) AS complete,
  '' AS block_reason,
  toUInt8(1) AS compatibility_prepared,
  toUInt8(1) AS backend_caught_up,
  'legacy-migration-031' AS manifest_digest,
  checkpoint.updated_at
FROM moraine.v_current_published_source_generations AS head
INNER JOIN
(
  SELECT * FROM moraine.ingest_checkpoint_transitions FINAL
) AS checkpoint
  ON checkpoint.host = head.source_host
 AND checkpoint.source_name = head.source_name
 AND checkpoint.source_file = head.source_file
 AND checkpoint.source_generation = head.source_generation
WHERE tuple(
  head.source_host,
  toString(head.source_name),
  head.source_file,
  head.source_generation
) NOT IN
(
  SELECT tuple(source_host, toString(source_name), source_file, source_generation)
  FROM moraine.v_current_source_generation_publication_readiness
);
