-- Canonical content authority: `events` is the only runtime relation that stores
-- event text or JSON payloads. Read indexes retain fixed-width navigation and
-- ranking metadata; callers hydrate only selected event winners from `events`.

DROP VIEW IF EXISTS moraine.search_term_stats;
DROP VIEW IF EXISTS moraine.search_corpus_stats;
DROP VIEW IF EXISTS moraine.mv_search_postings;
DROP VIEW IF EXISTS moraine.mv_search_documents_from_events;
DROP VIEW IF EXISTS moraine.mv_search_conversation_terms;
DROP VIEW IF EXISTS moraine.mv_mcp_open_dirty_sessions_from_events;
DROP VIEW IF EXISTS moraine.mv_file_attention_project_roots_from_tool_io;
DROP VIEW IF EXISTS moraine.mv_file_attention_project_roots_from_events;

DROP TABLE IF EXISTS moraine.search_conversation_terms;

-- Preserve tool detail on its canonical event before dropping the side table.
INSERT INTO moraine.events
SELECT e.* REPLACE (
  e.event_version + toUInt64(1) AS event_version,
  JSONMergePatch(
    if(JSONType(e.payload_json) = 'Object',
       e.payload_json,
       toJSONString(tuple(e.payload_json AS source_payload))),
    concat('{"moraine_tool_io":', t.tool_json, '}')
  ) AS payload_json
)
FROM moraine.events AS e FINAL
ALL INNER JOIN
(
  SELECT
    event_uid,
    argMax(
      concat(
        '{"tool_call_id":', toJSONString(tool_call_id),
        ',"parent_tool_call_id":', toJSONString(parent_tool_call_id),
        ',"tool_name":', toJSONString(tool_name),
        ',"tool_phase":', toJSONString(tool_phase),
        ',"tool_error":', toString(tool_error),
        ',"input_json":', toJSONString(input_json),
        ',"output_json":', toJSONString(output_json),
        ',"output_text":', toJSONString(output_text),
        ',"input_bytes":', toString(input_bytes),
        ',"output_bytes":', toString(output_bytes),
        ',"input_preview":', toJSONString(input_preview),
        ',"output_preview":', toJSONString(output_preview),
        ',"io_hash":', toString(io_hash),
        ',"project_id":', toJSONString(project_id),
        ',"repo_rel_path":', toJSONString(repo_rel_path),
        ',"worktree_root":', toJSONString(worktree_root),
        ',"source_ref":', toJSONString(source_ref),
        '}'
      ),
      tuple(event_version, tool_call_id)
    ) AS tool_json
  FROM moraine.tool_io FINAL
  GROUP BY event_uid
) AS t ON t.event_uid = e.event_uid
WHERE NOT JSONHas(e.payload_json, 'moraine_tool_io');

CREATE TABLE IF NOT EXISTS moraine.mcp_session_directory (
  session_id String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  harness LowCardinality(String),
  mode_hint SimpleAggregateFunction(max, UInt8),
  min_observed_event_time SimpleAggregateFunction(min, DateTime64(3)),
  max_observed_event_time SimpleAggregateFunction(max, DateTime64(3)),
  observed_events SimpleAggregateFunction(sum, UInt64),
  origin_cwd_state AggregateFunction(argMinIf, String, Tuple(DateTime64(3), String), UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (session_id, source_name, source_file, source_generation, harness);

CREATE TABLE IF NOT EXISTS moraine.mcp_event_locator (
  event_uid String,
  event_version UInt64,
  ingested_at DateTime64(3),
  session_id String,
  source_name LowCardinality(String),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  sort_time DateTime64(3),
  doc_len UInt32,
  text_digest String,
  payload_phase LowCardinality(String),
  project_id String,
  repo_rel_path String,
  worktree_root String,
  path_tokens Array(String),
  has_codex_mcp UInt8
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY event_uid;

CREATE TABLE IF NOT EXISTS moraine.mcp_event_navigation (
  session_id String,
  sort_time DateTime64(3),
  source_file String,
  source_generation UInt32,
  source_offset UInt64,
  source_line_no UInt64,
  event_uid String,
  event_version UInt64,
  source_name LowCardinality(String),
  event_ts DateTime64(3),
  display_time DateTime64(3),
  event_kind LowCardinality(String),
  actor_kind LowCardinality(String),
  payload_type LowCardinality(String),
  turn_index UInt32,
  tool_call_id String,
  tool_name LowCardinality(String),
  tool_phase LowCardinality(String),
  op_status LowCardinality(String),
  item_id String,
  harness LowCardinality(String),
  inference_provider LowCardinality(String),
  cwd LowCardinality(String),
  is_user_message UInt8,
  is_metadata_bearing UInt8
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, event_uid);

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_session_directory_from_events
TO moraine.mcp_session_directory AS
SELECT
  session_id,
  source_name,
  source_file,
  source_generation,
  harness,
  max(multiIf(
    payload_type IN ('web_search_call', 'search_results_received') OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch')), 3,
    source_name = 'codex-mcp' OR match(lowerUTF8(trimBoth(tool_name)), '^(search|search_sessions|open|list_sessions|file_attention|mcp__moraine__(search|search_sessions|open|list_sessions|file_attention))$'), 2,
    event_kind IN ('tool_call', 'tool_result') OR payload_type = 'tool_use', 1,
    0
  )) AS mode_hint,
  min(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS min_observed_event_time,
  max(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS max_observed_event_time,
  count() AS observed_events,
  argMinIfState(cwd, tuple(event_ts, event_uid), cwd != '') AS origin_cwd_state
FROM moraine.events
WHERE notEmpty(session_id)
GROUP BY session_id, source_name, source_file, source_generation, harness;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_locator_from_events
TO moraine.mcp_event_locator AS
WITH JSONExtractString(payload_json, 'moraine_tool_io', 'input_json') AS tool_input
SELECT
  event_uid,
  event_version,
  ingested_at,
  session_id,
  source_name,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
  hex(SHA256(text_content)) AS text_digest,
  JSONExtractString(payload_json, 'phase') AS payload_phase,
  JSONExtractString(payload_json, 'moraine_tool_io', 'project_id') AS project_id,
  JSONExtractString(payload_json, 'moraine_tool_io', 'repo_rel_path') AS repo_rel_path,
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') AS worktree_root,
  arrayFilter(path -> path != '', arrayDistinct(arrayConcat(
    extractAll(tool_input, '[A-Za-z0-9_./-]+'),
    extractAll(tool_input, '"((?:[^"\\\\]|\\\\.)*)"'),
    [
      JSONExtractString(tool_input, 'file_path'),
      JSONExtractString(tool_input, 'notebook_path'),
      JSONExtractString(tool_input, 'path'),
      JSONExtractString(tool_input, 'target_file'),
      JSONExtractString(tool_input, 'relativeWorkspacePath'),
      JSONExtractString(tool_input, 'relative_workspace_path'),
      JSONExtractString(tool_input, 'filepath'),
      JSONExtractString(tool_input, 'file'),
      JSONExtractString(tool_input, 'filename')
    ]
  ))) AS path_tokens,
  toUInt8(positionCaseInsensitiveUTF8(payload_json, 'codex-mcp') > 0) AS has_codex_mcp
FROM moraine.events
WHERE notEmpty(session_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_mcp_event_navigation_from_events
TO moraine.mcp_event_navigation AS
SELECT
  session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file,
  source_generation,
  source_offset,
  source_line_no,
  event_uid,
  event_version,
  source_name,
  event_ts,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind,
  actor_kind,
  payload_type,
  turn_index,
  tool_call_id,
  tool_name,
  tool_phase,
  op_status,
  item_id,
  harness,
  inference_provider,
  cwd,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))) AS is_metadata_bearing
FROM moraine.events
WHERE notEmpty(session_id);

-- Postings now carry all fixed-width filters and are produced directly from the
-- in-memory canonical insert block. Full content never lands in this table.
DROP TABLE IF EXISTS moraine.search_postings;
CREATE TABLE moraine.search_postings (
  post_version UInt64,
  term String,
  doc_id String,
  session_id String,
  source_name LowCardinality(String),
  harness LowCardinality(String),
  inference_provider LowCardinality(String),
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

CREATE MATERIALIZED VIEW moraine.mv_search_postings
TO moraine.search_postings AS
SELECT
  d.event_version AS post_version,
  d.term,
  d.event_uid AS doc_id,
  d.session_id,
  d.source_name,
  d.harness,
  d.inference_provider,
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
    event_version,
    event_uid,
    session_id,
    source_name,
    harness,
    inference_provider,
    event_kind AS event_class,
    payload_type,
    actor_kind AS actor_role,
    tool_name AS name,
    if(tool_phase != '', tool_phase, op_status) AS phase,
    source_ref,
    toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.events
) AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY d.event_version, d.term, d.event_uid, d.session_id, d.source_name,
  d.harness, d.inference_provider, d.event_class, d.payload_type, d.actor_role,
  d.name, d.phase, d.source_ref, d.doc_len;

CREATE VIEW moraine.search_term_stats AS
SELECT term, toUInt64(count()) AS docs
FROM moraine.search_postings FINAL
GROUP BY term;

CREATE VIEW moraine.search_corpus_stats AS
SELECT toUInt8(0) AS bucket, toUInt64(count()) AS docs,
  toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len
FROM moraine.mcp_event_locator FINAL
WHERE doc_len > 0;

CREATE MATERIALIZED VIEW moraine.mv_file_attention_project_roots_from_events
TO moraine.file_attention_project_roots AS
SELECT
  JSONExtractString(payload_json, 'moraine_tool_io', 'project_id') AS project_id,
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') AS worktree_root,
  event_version AS observed_version
FROM moraine.events
WHERE startsWith(JSONExtractString(payload_json, 'moraine_tool_io', 'project_id'), 'git:')
  AND JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') != '';

-- Backfill the newly installed indexes before any legacy relation is retired.
INSERT INTO moraine.mcp_session_directory
SELECT
  session_id, source_name, source_file, source_generation, harness,
  max(multiIf(
    payload_type IN ('web_search_call', 'search_results_received') OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch')), 3,
    source_name = 'codex-mcp' OR match(lowerUTF8(trimBoth(tool_name)), '^(search|search_sessions|open|list_sessions|file_attention|mcp__moraine__(search|search_sessions|open|list_sessions|file_attention))$'), 2,
    event_kind IN ('tool_call', 'tool_result') OR payload_type = 'tool_use', 1, 0
  )) AS mode_hint,
  min(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)),
  max(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)),
  count(),
  argMinIfState(cwd, tuple(event_ts, event_uid), cwd != '')
FROM moraine.events FINAL
WHERE notEmpty(session_id)
GROUP BY session_id, source_name, source_file, source_generation, harness;

INSERT INTO moraine.mcp_event_locator
WITH JSONExtractString(payload_json, 'moraine_tool_io', 'input_json') AS tool_input
SELECT event_uid, event_version, ingested_at, session_id, source_name, source_file,
  source_generation, source_offset, source_line_no,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))),
  hex(SHA256(text_content)), JSONExtractString(payload_json, 'phase'),
  JSONExtractString(payload_json, 'moraine_tool_io', 'project_id'),
  JSONExtractString(payload_json, 'moraine_tool_io', 'repo_rel_path'),
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root'),
  arrayFilter(path -> path != '', arrayDistinct(arrayConcat(
    extractAll(tool_input, '[A-Za-z0-9_./-]+'),
    extractAll(tool_input, '"((?:[^"\\\\]|\\\\.)*)"'),
    [
      JSONExtractString(tool_input, 'file_path'),
      JSONExtractString(tool_input, 'notebook_path'),
      JSONExtractString(tool_input, 'path'),
      JSONExtractString(tool_input, 'target_file'),
      JSONExtractString(tool_input, 'relativeWorkspacePath'),
      JSONExtractString(tool_input, 'relative_workspace_path'),
      JSONExtractString(tool_input, 'filepath'),
      JSONExtractString(tool_input, 'file'),
      JSONExtractString(tool_input, 'filename')
    ]
  ))),
  toUInt8(positionCaseInsensitiveUTF8(payload_json, 'codex-mcp') > 0)
FROM moraine.events FINAL
WHERE notEmpty(session_id);

INSERT INTO moraine.mcp_event_navigation
SELECT session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file, source_generation, source_offset, source_line_no, event_uid,
  event_version, source_name, event_ts,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), event_kind,
  actor_kind, payload_type, turn_index, tool_call_id, tool_name, tool_phase,
  op_status, item_id, harness, inference_provider, cwd,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change')))
FROM moraine.events FINAL
WHERE notEmpty(session_id);

INSERT INTO moraine.search_postings
SELECT d.event_version, d.term, d.event_uid, d.session_id, d.source_name,
  d.harness, d.inference_provider, d.event_class, d.payload_type, d.actor_role,
  d.name, d.phase, d.source_ref, d.doc_len, toUInt16(count())
FROM
(
  SELECT event_version, event_uid, session_id, source_name, harness,
    inference_provider, event_kind AS event_class, payload_type,
    actor_kind AS actor_role, tool_name AS name,
    if(tool_phase != '', tool_phase, op_status) AS phase, source_ref,
    toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.events FINAL
) AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY d.event_version, d.term, d.event_uid, d.session_id, d.source_name,
  d.harness, d.inference_provider, d.event_class, d.payload_type, d.actor_role,
  d.name, d.phase, d.source_ref, d.doc_len
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864;

INSERT INTO moraine.file_attention_project_roots
SELECT project_id, worktree_root, max(observed_version)
FROM
(
  SELECT JSONExtractString(payload_json, 'moraine_tool_io', 'project_id') AS project_id,
    JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') AS worktree_root,
    event_version AS observed_version
  FROM moraine.events FINAL
)
WHERE startsWith(project_id, 'git:') AND worktree_root != ''
GROUP BY project_id, worktree_root;

-- Nothing below is a runtime content authority after this point.
DROP TABLE IF EXISTS moraine.search_documents;
DROP TABLE IF EXISTS moraine.tool_io;
DROP TABLE IF EXISTS moraine.mcp_open_events;
DROP TABLE IF EXISTS moraine.mcp_open_turns;
DROP TABLE IF EXISTS moraine.mcp_open_sessions;
DROP TABLE IF EXISTS moraine.mcp_open_publication_headers;
DROP TABLE IF EXISTS moraine.mcp_open_generation_readiness;
DROP TABLE IF EXISTS moraine.mcp_open_dirty_sessions;
DROP TABLE IF EXISTS moraine.mcp_open_backfill_plans;
DROP TABLE IF EXISTS moraine.mcp_open_projection_state;
