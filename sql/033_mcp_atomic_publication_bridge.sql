-- Atomic MCP compatibility publication bridge (#602).
--
-- The existing A/B children remain the compatibility materialization.  A
-- candidate-scoped scalar header is now the authority for selecting a child
-- generation; a session-only row can no longer publish replayed content.

ALTER TABLE moraine.mcp_open_turns
  ADD COLUMN IF NOT EXISTS candidate_generation UInt64 AFTER generation,
  MODIFY ORDER BY (session_id, slot, turn_seq, candidate_generation);

ALTER TABLE moraine.mcp_open_events
  ADD COLUMN IF NOT EXISTS source_host String AFTER event_uid,
  ADD COLUMN IF NOT EXISTS candidate_generation UInt64 AFTER generation,
  MODIFY ORDER BY (event_uid, slot, source_host, candidate_generation);

CREATE TABLE IF NOT EXISTS moraine.mcp_open_publication_headers (
  session_id String,
  candidate_publication_id String,
  slot UInt8,
  generation UInt64,
  source_revision UInt64,
  dirty_revision UInt64,
  first_event_time DateTime64(3),
  last_event_time DateTime64(3),
  total_turns UInt32,
  total_events UInt64,
  user_messages UInt64,
  assistant_messages UInt64,
  tool_calls UInt64,
  tool_results UInt64,
  mode LowCardinality(String),
  first_event_uid String,
  last_event_uid String,
  last_actor_role LowCardinality(String),
  title String,
  source String,
  harness LowCardinality(String),
  inference_provider LowCardinality(String),
  session_slug String,
  session_summary String,
  completed UInt8,
  terminal_event_uid String,
  origin_cwd String,
  tombstone UInt8,
  required_source_heads Array(Tuple(
    source_host String,
    source_name String,
    source_file String,
    source_generation UInt32,
    publication_revision UInt64
  )),
  required_heads_fingerprint String,
  header_revision UInt64,
  publisher_id String,
  operation_id String,
  prepared_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(header_revision)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, candidate_publication_id);

CREATE TABLE IF NOT EXISTS moraine.mcp_open_generation_readiness (
  candidate_publication_id String,
  source_host String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  readiness_revision UInt64,
  operation_id String,
  affected_session_count UInt64,
  prepared_session_count UInt64,
  tombstone_count UInt64,
  required_source_heads Array(Tuple(
    source_host String,
    source_name String,
    source_file String,
    source_generation UInt32,
    publication_revision UInt64
  )),
  required_heads_fingerprint String,
  candidate_digest String,
  ready UInt8,
  block_reason String,
  prepared_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(readiness_revision)
ORDER BY (
  candidate_publication_id,
  source_host,
  source_name,
  source_file,
  source_generation
);

CREATE VIEW IF NOT EXISTS moraine.v_mcp_open_publication_headers AS
SELECT *
FROM moraine.mcp_open_publication_headers FINAL;

CREATE VIEW IF NOT EXISTS moraine.v_current_mcp_open_generation_readiness AS
SELECT
  candidate_publication_id,
  source_host,
  source_name,
  source_file,
  source_generation,
  tupleElement(readiness, 1) AS readiness_revision,
  tupleElement(readiness, 2) AS operation_id,
  tupleElement(readiness, 3) AS affected_session_count,
  tupleElement(readiness, 4) AS prepared_session_count,
  tupleElement(readiness, 5) AS tombstone_count,
  tupleElement(readiness, 6) AS required_source_heads,
  tupleElement(readiness, 7) AS required_heads_fingerprint,
  tupleElement(readiness, 8) AS candidate_digest,
  tupleElement(readiness, 9) AS ready,
  tupleElement(readiness, 10) AS block_reason,
  tupleElement(readiness, 11) AS prepared_at
FROM
(
  SELECT
    candidate_publication_id,
    source_host,
    source_name,
    source_file,
    source_generation,
    argMax(
      tuple(
        readiness_revision,
        operation_id,
        affected_session_count,
        prepared_session_count,
        tombstone_count,
        required_source_heads,
        required_heads_fingerprint,
        candidate_digest,
        ready,
        block_reason,
        prepared_at
      ),
      readiness_revision
    ) AS readiness
  FROM moraine.mcp_open_generation_readiness
  GROUP BY
    candidate_publication_id,
    source_host,
    source_name,
    source_file,
    source_generation
);

-- Existing session-only heads cannot prove a complete publication dependency
-- set.  Fail closed to the canonical live-event fallback and enqueue every
-- currently live session for one compatibility rebuild after migration.
INSERT INTO moraine.mcp_open_dirty_sessions
  (session_id, dirty_revision, observed_at)
SELECT session_id, generateSnowflakeID(), now64(3)
FROM
(
  SELECT DISTINCT session_id
  FROM moraine.v_live_events
  WHERE notEmpty(session_id)
);

INSERT INTO moraine.mcp_open_projection_state
  (state_key, ready, generation, backfill_cursor)
VALUES ('global', 0, generateSnowflakeID(), '');

DROP VIEW IF EXISTS moraine.v_publication_diagnostics;

-- One bounded, sanitized health row.  Detailed paths, manifests, operation
-- IDs, and publisher identities remain available only in audit relations.
CREATE VIEW moraine.v_publication_diagnostics AS
SELECT
  ambiguous_hostless_rows,
  replaying_generations,
  blocked_generations,
  append_preparations,
  blocked_append_preparations,
  mirror_catchup_pending,
  writer_conflicts,
  arraySlice(arraySort(arrayFilter(issue -> issue != '', [
    if(ambiguous_hostless_rows > 0, 'legacy_host_ambiguity', ''),
    if(replaying_generations > 0, 'generation_replaying', ''),
    if(blocked_generations > 0, 'generation_blocked', ''),
    if(append_preparations > 0, 'append_preparing', ''),
    if(blocked_append_preparations > 0, 'append_blocked', ''),
    if(mirror_catchup_pending > 0, 'mirror_catchup_pending', ''),
    if(writer_conflicts > 0, 'writer_conflict', '')
  ])), 1, 32) AS issues
FROM
(
  SELECT
    toUInt64((
      SELECT count()
      FROM moraine.publication_diagnostic_events FINAL
      WHERE active = 1 AND diagnostic_kind = 'legacy_host_ambiguity'
    )) + countIf(
      checkpoint_block_reason = 'legacy_equal_timestamp_ambiguity'
    ) AS ambiguous_hostless_rows,
    countIf(checkpoint_lifecycle = 'replaying') AS replaying_generations,
    countIf(readiness_block_reason != '') AS blocked_generations,
    toUInt64((
      SELECT count()
      FROM moraine.v_current_ingest_append_control
      WHERE state = 'preparing'
    )) AS append_preparations,
    toUInt64((
      SELECT count()
      FROM moraine.v_current_ingest_append_control
      WHERE state = 'blocked'
    )) AS blocked_append_preparations,
    countIf(
      readiness_complete = 1 AND readiness_backend_caught_up = 0
    ) AS mirror_catchup_pending,
    toUInt64((
      SELECT count()
      FROM moraine.publication_diagnostic_events FINAL
      WHERE active = 1 AND diagnostic_kind = 'writer_conflict'
    )) AS writer_conflicts
  FROM
  (
    -- A historical generation remains durable for audit and as-of recovery,
    -- but it must stop affecting current health as soon as a newer checkpoint
    -- generation supersedes it.  Readiness is therefore authorized by the
    -- exact generation selected by the current checkpoint tuple.
    SELECT
      checkpoint.lifecycle AS checkpoint_lifecycle,
      checkpoint.block_reason AS checkpoint_block_reason,
      readiness.block_reason AS readiness_block_reason,
      readiness.complete AS readiness_complete,
      readiness.backend_caught_up AS readiness_backend_caught_up
    FROM moraine.v_current_ingest_checkpoint_transitions AS checkpoint
    LEFT JOIN moraine.v_current_source_generation_publication_readiness AS readiness
      ON readiness.source_host = checkpoint.host
     AND readiness.source_name = checkpoint.source_name
     AND readiness.source_file = checkpoint.source_file
     AND readiness.source_generation = checkpoint.source_generation
  ) AS current_source_state
);
