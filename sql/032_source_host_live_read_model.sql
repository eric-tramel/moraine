-- Host-aware physical identity and publication-authorized live views (#602).
--
-- Candidate generations must coexist physically with their currently
-- published predecessor.  The new host discriminator also prevents identical
-- UIDs written by two shared-backend hosts from collapsing after FINAL.

DROP VIEW IF EXISTS moraine.v_session_summary;
DROP VIEW IF EXISTS moraine.v_turn_summary;
DROP VIEW IF EXISTS moraine.v_conversation_trace;
DROP VIEW IF EXISTS moraine.v_all_events;
DROP VIEW IF EXISTS moraine.search_term_stats;
DROP VIEW IF EXISTS moraine.search_corpus_stats;
DROP VIEW IF EXISTS moraine.v_live_search_postings;
DROP VIEW IF EXISTS moraine.v_live_search_documents;
DROP VIEW IF EXISTS moraine.v_live_tool_io;
DROP VIEW IF EXISTS moraine.v_live_event_links;
DROP VIEW IF EXISTS moraine.v_live_events;

DROP VIEW IF EXISTS moraine.mv_mcp_open_dirty_sessions_from_events;
DROP VIEW IF EXISTS moraine.mv_search_conversation_terms;
DROP VIEW IF EXISTS moraine.mv_search_postings;
DROP VIEW IF EXISTS moraine.mv_search_documents_from_events;

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,
  MODIFY ORDER BY (
    source_name, source_file, source_generation, source_offset,
    source_line_no, event_uid, source_host
  );

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,
  MODIFY ORDER BY (
    session_id, event_ts, source_name, source_file, source_generation,
    source_offset, source_line_no, event_uid, source_host
  );

-- Event UIDs already include the source generation.  Appending the host keeps
-- identical UIDs from different shared-backend writers independent.  A
-- derived event_version remains its own replacement-order token;
-- source_event_version binds it to the exact canonical event revision.
ALTER TABLE moraine.event_links
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,
  ADD COLUMN IF NOT EXISTS source_event_version UInt64 DEFAULT 0 AFTER event_version,
  MODIFY ORDER BY (
    session_id, event_uid, link_type, linked_event_uid, source_host
  );

ALTER TABLE moraine.tool_io
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,
  ADD COLUMN IF NOT EXISTS source_event_version UInt64 DEFAULT 0 AFTER event_version,
  MODIFY ORDER BY (
    session_id, tool_call_id, event_uid, source_host
  );

ALTER TABLE moraine.ingest_errors
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,
  MODIFY ORDER BY (
    source_name, source_file, source_generation, source_offset,
    source_line_no, source_host
  );

-- Event/document IDs include generation, so the added host is the missing
-- cross-writer discriminator while ReplacingMergeTree still collapses mutable
-- same-generation versions.
ALTER TABLE moraine.search_documents
  MODIFY COLUMN source_name LowCardinality(String);

ALTER TABLE moraine.search_documents
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_date,
  MODIFY ORDER BY (
    event_uid, source_host
  );

ALTER TABLE moraine.search_postings
  MODIFY COLUMN source_name LowCardinality(String);

ALTER TABLE moraine.search_postings
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id,
  ADD COLUMN IF NOT EXISTS source_file String AFTER inference_provider,
  ADD COLUMN IF NOT EXISTS source_generation UInt32 AFTER source_file,
  MODIFY ORDER BY (
    term, doc_id, source_host
  );

-- Legacy derived rows used independently sampled wall-clock versions.  Bind
-- each current derived replacement key to the closest event revision that was
-- already present when the derived row was created.  A copied row increments
-- only the derived replacement token so it wins FINAL while retaining the
-- canonical owner revision separately.  Rows without a causal predecessor
-- (including a reserved zero-version event) remain at source_event_version =
-- 0 and therefore fail closed.  The zero filters also make a retry after
-- response loss idempotent.
INSERT INTO moraine.event_links
  (ingested_at, source_host, event_uid, linked_event_uid, linked_external_id,
   link_type, session_id, harness, inference_provider, source_name,
   metadata_json, event_version, source_event_version)
SELECT
  l.ingested_at,
  l.source_host,
  l.event_uid,
  l.linked_event_uid,
  l.linked_external_id,
  l.link_type,
  l.session_id,
  l.harness,
  l.inference_provider,
  l.source_name,
  l.metadata_json,
  l.event_version + toUInt64(1) AS event_version,
  e.event_version AS source_event_version
FROM
(
  SELECT *
  FROM moraine.event_links FINAL
  WHERE source_event_version = 0
    AND event_version < toUInt64(18446744073709551615)
) AS l
ASOF INNER JOIN
(
  SELECT source_host, event_uid, event_version
  FROM moraine.events FINAL
  WHERE event_version > 0
  ORDER BY source_host, event_uid, event_version
) AS e
  ON l.source_host = e.source_host
 AND l.event_uid = e.event_uid
 AND l.event_version >= e.event_version;

INSERT INTO moraine.tool_io
  (ingested_at, source_host, event_uid, session_id, harness,
   inference_provider, source_name, tool_call_id, parent_tool_call_id,
   tool_name, tool_phase, tool_error, input_json, output_json, output_text,
   input_bytes, output_bytes, input_preview, output_preview, io_hash,
   project_id, repo_rel_path, worktree_root, source_ref, event_version,
   source_event_version)
SELECT
  t.ingested_at,
  t.source_host,
  t.event_uid,
  t.session_id,
  t.harness,
  t.inference_provider,
  t.source_name,
  t.tool_call_id,
  t.parent_tool_call_id,
  t.tool_name,
  t.tool_phase,
  t.tool_error,
  t.input_json,
  t.output_json,
  t.output_text,
  t.input_bytes,
  t.output_bytes,
  t.input_preview,
  t.output_preview,
  t.io_hash,
  t.project_id,
  t.repo_rel_path,
  t.worktree_root,
  t.source_ref,
  t.event_version + toUInt64(1) AS event_version,
  e.event_version AS source_event_version
FROM
(
  SELECT *
  FROM moraine.tool_io FINAL
  WHERE source_event_version = 0
    AND event_version < toUInt64(18446744073709551615)
) AS t
ASOF INNER JOIN
(
  SELECT source_host, event_uid, event_version
  FROM moraine.events FINAL
  WHERE event_version > 0
  ORDER BY source_host, event_uid, event_version
) AS e
  ON t.source_host = e.source_host
 AND t.event_uid = e.event_uid
 AND t.event_version >= e.event_version;

-- The sole current-generation authorization relation.  Readiness and
-- checkpoint status deliberately do not participate in this view.
CREATE VIEW moraine.v_live_events AS
SELECT e.*
FROM
(
  SELECT * FROM moraine.events FINAL
) AS e
ALL INNER JOIN moraine.v_current_published_source_generations AS h
  ON e.source_host = h.source_host
 AND e.source_name = h.source_name
 AND e.source_file = h.source_file
 AND e.source_generation = h.source_generation;

-- Generation-qualified UID authorization is necessary but not sufficient for
-- mutable stable UIDs: only a derived row bound to the current canonical event
-- revision is live.
CREATE VIEW moraine.v_live_event_links AS
SELECT l.*
FROM
(
  SELECT * FROM moraine.event_links FINAL
) AS l
ALL INNER JOIN
(
  SELECT source_host, event_uid, event_version
  FROM moraine.v_live_events
) AS e
  ON l.source_host = e.source_host
 AND l.event_uid = e.event_uid
 AND l.source_event_version != 0
 AND l.source_event_version = e.event_version;

CREATE VIEW moraine.v_live_tool_io AS
SELECT t.*
FROM
(
  SELECT * FROM moraine.tool_io FINAL
) AS t
ALL INNER JOIN
(
  SELECT source_host, event_uid, event_version
  FROM moraine.v_live_events
) AS e
  ON t.source_host = e.source_host
 AND t.event_uid = e.event_uid
 AND t.source_event_version != 0
 AND t.source_event_version = e.event_version;

CREATE MATERIALIZED VIEW moraine.mv_search_documents_from_events
TO moraine.search_documents
AS
-- Emit one replacement version for every event revision.  Empty and
-- otherwise unsearchable text is a document tombstone: it supersedes older
-- searchable text while the postings MV below naturally emits no terms.
SELECT
  event_version AS doc_version,
  ingested_at,
  event_uid,
  origin_event_id AS compacted_parent_uid,
  session_id,
  session_date,
  source_host,
  source_name,
  harness,
  inference_provider,
  endpoint_kind,
  source_file,
  source_generation,
  source_line_no,
  source_offset,
  source_ref,
  record_ts,
  event_kind AS event_class,
  payload_type,
  actor_kind AS actor_role,
  tool_name AS name,
  if(tool_phase != '', tool_phase, op_status) AS phase,
  text_content,
  payload_json,
  token_usage_json,
  token_usage_buckets,
  token_usage_native_units
FROM moraine.events;

CREATE MATERIALIZED VIEW moraine.mv_search_postings
TO moraine.search_postings
AS
SELECT
  d.doc_version AS post_version,
  d.term,
  d.event_uid AS doc_id,
  d.session_id,
  d.source_host,
  d.source_name,
  d.harness,
  d.inference_provider,
  d.source_file,
  d.source_generation,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len,
  toUInt16(count()) AS tf
FROM
(
  SELECT
    doc_version,
    event_uid,
    session_id,
    source_host,
    source_name,
    harness,
    inference_provider,
    source_file,
    source_generation,
    event_class,
    payload_type,
    actor_role,
    name,
    phase,
    source_ref,
    doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.search_documents
  WHERE doc_len > 0
) AS d
WHERE lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY
  d.doc_version,
  d.term,
  d.event_uid,
  d.session_id,
  d.source_host,
  d.source_name,
  d.harness,
  d.inference_provider,
  d.source_file,
  d.source_generation,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len;

CREATE MATERIALIZED VIEW moraine.mv_search_conversation_terms
TO moraine.search_conversation_terms
AS
SELECT
  term,
  session_id,
  toUInt64(tf) AS tf_sum,
  toUInt32(1) AS event_freq
FROM moraine.search_postings;

-- An interrupted older copy of this migration may have reconciled documents
-- while mv_search_postings was absent, then failed in the former corpus-wide
-- posting rebuild.  Replay only current document versions that still have no
-- posting under their complete pre-existing identity.  The IN subquery reads
-- identity columns only; the outer primary-key lookup tokenizes the small
-- stranded set and inserts through the conversation-terms MV.  Existing
-- legacy postings deliberately match without source_file/source_generation
-- because ALTER filled those new columns with type defaults.  Grace hash
-- bounds the identity membership table and spills partitions instead of
-- allowing a large legacy posting corpus to consume server memory.
INSERT INTO moraine.search_postings
  (post_version, term, doc_id, session_id, source_host, source_name, harness,
   inference_provider, source_file, source_generation, event_class,
   payload_type, actor_role, name, phase, source_ref, doc_len, tf)
WITH missing_document_ids AS
(
  SELECT missing.event_uid, missing.source_host
  FROM
  (
    SELECT
      event_uid,
      source_host,
      source_name,
      session_id,
      source_ref,
      doc_version,
      doc_len
    FROM moraine.search_documents FINAL
  ) AS missing
  LEFT ANTI JOIN
  (
    SELECT
      doc_id,
      source_host,
      source_name,
      session_id,
      source_ref,
      post_version
    FROM moraine.search_postings
  ) AS p
    ON missing.source_host = p.source_host
   AND missing.source_name = p.source_name
   AND missing.session_id = p.session_id
   AND missing.source_ref = p.source_ref
   AND missing.event_uid = p.doc_id
   AND missing.doc_version = p.post_version
  WHERE missing.doc_len > 0
)
SELECT
  d.doc_version AS post_version,
  d.term,
  d.event_uid AS doc_id,
  d.session_id,
  d.source_host,
  d.source_name,
  d.harness,
  d.inference_provider,
  d.source_file,
  d.source_generation,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len,
  toUInt16(count()) AS tf
FROM
(
  SELECT
    doc_version,
    event_uid,
    session_id,
    source_host,
    source_name,
    harness,
    inference_provider,
    source_file,
    source_generation,
    event_class,
    payload_type,
    actor_role,
    name,
    phase,
    source_ref,
    doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.search_documents FINAL
  WHERE doc_len > 0
    AND (event_uid, source_host) IN
    (SELECT event_uid, source_host FROM missing_document_ids)
) AS d
WHERE lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY
  d.doc_version,
  d.term,
  d.event_uid,
  d.session_id,
  d.source_host,
  d.source_name,
  d.harness,
  d.inference_provider,
  d.source_file,
  d.source_generation,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len
SETTINGS
  join_algorithm = 'grace_hash',
  grace_hash_join_initial_buckets = 32,
  max_bytes_in_join = 268435456;

-- Reconcile every current event version that is missing from the document
-- index.  Both search MVs are live before this INSERT, so only genuinely
-- missing documents are tokenized into postings.  This also creates
-- zero-length tombstones for revisions skipped by older MVs and closes the
-- migration window while the MVs above were absent.  Concurrent duplicate
-- insertion is safe under the versioned ReplacingMergeTree keys.
INSERT INTO moraine.search_documents
  (doc_version, ingested_at, event_uid, compacted_parent_uid, session_id,
   session_date, source_host, source_name, harness, inference_provider,
   endpoint_kind, source_file, source_generation, source_line_no,
   source_offset, source_ref, record_ts, event_class, payload_type, actor_role,
   name, phase, text_content, payload_json, token_usage_json,
   token_usage_buckets, token_usage_native_units)
SELECT
  e.event_version AS doc_version,
  e.ingested_at,
  e.event_uid,
  e.origin_event_id AS compacted_parent_uid,
  e.session_id,
  e.session_date,
  e.source_host,
  e.source_name,
  e.harness,
  e.inference_provider,
  e.endpoint_kind,
  e.source_file,
  e.source_generation,
  e.source_line_no,
  e.source_offset,
  e.source_ref,
  e.record_ts,
  e.event_kind AS event_class,
  e.payload_type,
  e.actor_kind AS actor_role,
  e.tool_name AS name,
  if(e.tool_phase != '', e.tool_phase, e.op_status) AS phase,
  e.text_content,
  e.payload_json,
  e.token_usage_json,
  e.token_usage_buckets,
  e.token_usage_native_units
FROM
(
  SELECT * FROM moraine.events FINAL
) AS e
LEFT ANTI JOIN
(
  SELECT source_host, event_uid, doc_version
  FROM moraine.search_documents
) AS d
  ON e.source_host = d.source_host
 AND e.event_uid = d.event_uid
 AND e.event_version = d.doc_version;

-- Existing posting rows already carry the document's session, source name,
-- source ref, generation-qualified event UID, and exact version.  Adding
-- `FINAL` selects the latest document or empty-text tombstone for each
-- generation-qualified event identity. The document's own source tuple is
-- sufficient to authorize that identity against the central published head;
-- no canonical-event rescan belongs on the ranking hot path.
--
-- Keep the explicit projection below because ClickHouse omits MATERIALIZED
-- columns from `SELECT *` by default. Do not regenerate the historical index
-- here: that would re-tokenize and rewrite every posting during startup,
-- proportional to the full corpus.
CREATE VIEW moraine.v_live_search_documents AS
SELECT d.*
FROM
(
  -- ClickHouse omits MATERIALIZED columns from `SELECT *` by default. Keep
  -- this projection explicit so the live view preserves the complete
  -- search_documents contract consumed by repository ranking/hydration.
  SELECT
    doc_version,
    ingested_at,
    event_uid,
    compacted_parent_uid,
    session_id,
    session_date,
    source_host,
    source_name,
    harness,
    inference_provider,
    endpoint_kind,
    source_file,
    source_generation,
    source_line_no,
    source_offset,
    source_ref,
    record_ts,
    event_class,
    payload_type,
    actor_role,
    name,
    phase,
    text_content,
    payload_json,
    token_usage_json,
    token_usage_buckets,
    token_usage_native_units,
    doc_len,
    has_codex_mcp
  FROM moraine.search_documents FINAL
) AS d
ALL INNER JOIN moraine.v_current_published_source_generations AS h
  ON d.source_host = h.source_host
 AND d.source_name = h.source_name
 AND d.source_file = h.source_file
 AND d.source_generation = h.source_generation
WHERE d.doc_len > 0;

CREATE VIEW moraine.v_live_search_postings AS
SELECT
  p.post_version,
  p.term,
  p.doc_id,
  p.session_id,
  d.source_host AS source_host,
  d.source_name AS source_name,
  p.harness,
  p.inference_provider,
  d.source_file AS source_file,
  d.source_generation AS source_generation,
  p.event_class,
  p.payload_type,
  p.actor_role,
  p.name,
  p.phase,
  p.source_ref,
  p.doc_len,
  p.tf
FROM
(
  SELECT * FROM moraine.search_postings FINAL
) AS p
ALL INNER JOIN
(
  SELECT
    source_host,
    source_name,
    source_file,
    source_generation,
    session_id,
    source_ref,
    event_uid,
    doc_version
  FROM moraine.v_live_search_documents
) AS d
  ON p.source_host = d.source_host
 AND p.source_name = d.source_name
 AND p.session_id = d.session_id
 AND p.source_ref = d.source_ref
 AND p.doc_id = d.event_uid
 AND p.post_version = d.doc_version;

-- Live corpus/term statistics are computed only after event -> document ->
-- posting version authorization.  Inactive high-TF rows cannot affect BM25.
CREATE VIEW moraine.search_term_stats AS
SELECT
  term,
  toUInt64(count()) AS docs
FROM moraine.v_live_search_postings
GROUP BY term;

CREATE VIEW moraine.search_corpus_stats AS
SELECT
  toUInt8(0) AS bucket,
  toUInt64(count()) AS docs,
  toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len
FROM moraine.v_live_search_documents;

-- Preserve the public trace/summary schemas while changing their base to the
-- authorized live-event relation.
CREATE VIEW moraine.v_all_events AS
SELECT
  ingested_at,
  event_uid,
  origin_event_id AS compacted_parent_uid,
  session_id,
  session_date,
  source_file,
  source_inode,
  source_generation,
  source_line_no,
  source_offset,
  source_ref,
  record_ts,
  event_kind AS event_class,
  payload_type,
  actor_kind AS actor_role,
  toString(turn_index) AS turn_id,
  item_id,
  tool_call_id AS call_id,
  tool_name AS name,
  if(tool_phase != '', tool_phase, op_status) AS phase,
  text_content,
  payload_json,
  token_usage_json,
  endpoint_kind,
  token_usage_buckets,
  token_usage_native_units,
  event_version
FROM moraine.v_live_events;

CREATE VIEW moraine.v_conversation_trace AS
SELECT
  session_id,
  session_date,
  event_uid,
  compacted_parent_uid,
  source_file,
  source_generation,
  source_line_no,
  source_offset,
  source_ref,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS event_time,
  row_number() OVER (
    PARTITION BY session_id
    ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at),
      source_file, source_generation, source_offset, source_line_no, event_uid
  ) AS event_order,
  if(
    toUInt32OrZero(turn_id) > 0,
    toUInt32OrZero(turn_id),
    greatest(
      toUInt32(1),
      toUInt32(
        sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER (
          PARTITION BY session_id
          ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at),
            source_file, source_generation, source_offset, source_line_no, event_uid
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
      )
    )
  ) AS turn_seq,
  turn_id,
  actor_role,
  event_class,
  payload_type,
  call_id,
  name,
  phase,
  item_id,
  text_content,
  payload_json,
  token_usage_json,
  endpoint_kind,
  token_usage_buckets,
  token_usage_native_units
FROM moraine.v_all_events;

CREATE VIEW moraine.v_turn_summary AS
SELECT
  session_id,
  turn_seq,
  anyIf(turn_id, turn_id != '') AS turn_id,
  min(event_time) AS started_at,
  max(event_time) AS ended_at,
  count() AS total_events,
  countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,
  countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,
  countIf(event_class = 'tool_call') AS tool_calls,
  countIf(event_class = 'tool_result') AS tool_results,
  countIf(event_class = 'reasoning') AS reasoning_items
FROM moraine.v_conversation_trace
GROUP BY session_id, turn_seq;

CREATE VIEW moraine.v_session_summary AS
SELECT
  session_id,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  max(turn_seq) AS total_turns,
  count() AS total_events,
  countIf(event_class = 'tool_call') AS tool_calls,
  countIf(event_class = 'tool_result') AS tool_results,
  countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,
  countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages
FROM moraine.v_conversation_trace
GROUP BY session_id;

-- Same-generation appends are dirtied only when their source head is already
-- published.  Replacement replay chunks are reconciled once at activation.
CREATE MATERIALIZED VIEW moraine.mv_mcp_open_dirty_sessions_from_events
TO moraine.mcp_open_dirty_sessions AS
SELECT
  e.session_id,
  generateSnowflakeID() AS dirty_revision,
  now64(3) AS observed_at
FROM moraine.events AS e
ALL INNER JOIN moraine.v_current_published_source_generations AS h
  ON e.source_host = h.source_host
 AND e.source_name = h.source_name
 AND e.source_file = h.source_file
 AND e.source_generation = h.source_generation
WHERE notEmpty(e.session_id)
GROUP BY e.session_id;
