DROP VIEW IF EXISTS cortex.mv_search_documents_from_normalized;
DROP VIEW IF EXISTS cortex.mv_search_documents_from_compacted;
DROP VIEW IF EXISTS cortex.mv_search_documents_from_events;
DROP VIEW IF EXISTS cortex.mv_search_postings;
DROP VIEW IF EXISTS cortex.mv_search_term_stats;
DROP VIEW IF EXISTS cortex.mv_search_corpus_stats;

CREATE TABLE IF NOT EXISTS cortex.search_documents (
  doc_version UInt64,
  ingested_at DateTime64(3),
  event_uid String,
  compacted_parent_uid String,
  session_id String,
  session_date Date,
  source_name LowCardinality(String) DEFAULT '',
  provider LowCardinality(String) DEFAULT '',
  source_file String,
  source_generation UInt32,
  source_line_no UInt64,
  source_offset UInt64,
  source_ref String,
  record_ts String,
  event_class LowCardinality(String),
  payload_type LowCardinality(String),
  actor_role LowCardinality(String),
  name LowCardinality(String),
  phase LowCardinality(String),
  text_content String,
  payload_json String,
  token_usage_json String,
  doc_len UInt32 MATERIALIZED toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')))
)
ENGINE = ReplacingMergeTree(doc_version)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (event_uid);

ALTER TABLE cortex.search_documents
  ADD COLUMN IF NOT EXISTS source_name LowCardinality(String) DEFAULT '' AFTER session_date;
ALTER TABLE cortex.search_documents
  ADD COLUMN IF NOT EXISTS provider LowCardinality(String) DEFAULT '' AFTER source_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS cortex.mv_search_documents_from_events
TO cortex.search_documents
AS
SELECT
  event_version AS doc_version,
  ingested_at,
  event_uid,
  origin_event_id AS compacted_parent_uid,
  session_id,
  session_date,
  source_name,
  provider,
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
  token_usage_json
FROM cortex.events
WHERE lengthUTF8(replaceRegexpAll(text_content, '\\s+', '')) > 0;

CREATE TABLE IF NOT EXISTS cortex.search_postings (
  post_version UInt64,
  term String,
  doc_id String,
  session_id String,
  source_name LowCardinality(String) DEFAULT '',
  provider LowCardinality(String) DEFAULT '',
  event_class LowCardinality(String),
  payload_type LowCardinality(String),
  actor_role LowCardinality(String),
  name LowCardinality(String),
  phase LowCardinality(String),
  source_ref String,
  doc_len UInt32,
  tf UInt16
)
ENGINE = ReplacingMergeTree(post_version)
PARTITION BY cityHash64(term) % 32
ORDER BY (term, doc_id);

ALTER TABLE cortex.search_postings
  ADD COLUMN IF NOT EXISTS source_name LowCardinality(String) DEFAULT '' AFTER session_id;
ALTER TABLE cortex.search_postings
  ADD COLUMN IF NOT EXISTS provider LowCardinality(String) DEFAULT '' AFTER source_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS cortex.mv_search_postings
TO cortex.search_postings
AS
SELECT
  d.doc_version AS post_version,
  d.term,
  d.event_uid AS doc_id,
  d.session_id,
  d.source_name,
  d.provider,
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
    source_name,
    provider,
    event_class,
    payload_type,
    actor_role,
    name,
    phase,
    source_ref,
    doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM cortex.search_documents
  WHERE doc_len > 0
) AS d
WHERE lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY
  d.doc_version,
  d.term,
  d.event_uid,
  d.session_id,
  d.source_name,
  d.provider,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len;

DROP TABLE IF EXISTS cortex.search_term_stats;

CREATE VIEW IF NOT EXISTS cortex.search_term_stats
AS
SELECT
  term,
  toUInt64(count()) AS docs
FROM cortex.search_postings FINAL
GROUP BY term;

DROP TABLE IF EXISTS cortex.search_corpus_stats;

CREATE VIEW IF NOT EXISTS cortex.search_corpus_stats
AS
SELECT
  toUInt8(0) AS bucket,
  toUInt64(count()) AS docs,
  toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len
FROM cortex.search_documents FINAL
WHERE doc_len > 0;

CREATE TABLE IF NOT EXISTS cortex.search_query_log (
  ts DateTime64(3) DEFAULT now64(3),
  query_id String,
  source LowCardinality(String),
  session_hint String,
  raw_query String,
  normalized_terms Array(String),
  term_count UInt16,
  result_limit UInt16,
  min_should_match UInt16,
  min_score Float64,
  include_tool_events UInt8,
  exclude_codex_mcp UInt8,
  response_ms UInt32,
  result_count UInt16,
  metadata_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, query_id);

CREATE TABLE IF NOT EXISTS cortex.search_hit_log (
  ts DateTime64(3) DEFAULT now64(3),
  query_id String,
  rank UInt16,
  event_uid String,
  session_id String,
  source_name LowCardinality(String) DEFAULT '',
  provider LowCardinality(String) DEFAULT '',
  score Float64,
  matched_terms UInt16,
  doc_len UInt32,
  event_class LowCardinality(String),
  payload_type LowCardinality(String),
  actor_role LowCardinality(String),
  name LowCardinality(String),
  source_ref String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (query_id, rank, ts);

ALTER TABLE cortex.search_hit_log
  ADD COLUMN IF NOT EXISTS source_name LowCardinality(String) DEFAULT '' AFTER session_id;
ALTER TABLE cortex.search_hit_log
  ADD COLUMN IF NOT EXISTS provider LowCardinality(String) DEFAULT '' AFTER source_name;

CREATE TABLE IF NOT EXISTS cortex.search_interaction_log (
  ts DateTime64(3) DEFAULT now64(3),
  interaction_id String,
  query_id String,
  event_uid String,
  action LowCardinality(String),
  actor String,
  value Float64,
  note String,
  metadata_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, query_id, event_uid, action);
