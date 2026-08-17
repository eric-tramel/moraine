-- Generation-preserving canonical storage and publication-authorized live views (#602).
-- Writers are quiesced by the migration command. Candidate replay rows must coexist
-- physically with the published predecessor until the source head advances.

DROP VIEW IF EXISTS moraine.mv_mcp_event_locator_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_navigation_from_events;
DROP VIEW IF EXISTS moraine.mv_search_postings;
DROP VIEW IF EXISTS moraine.mv_file_attention_project_roots_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_navigation_seek_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_turn_seek_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_version_seek_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_session_mutation_signals_from_events;
DROP TABLE IF EXISTS moraine.mcp_session_mutation_signals;

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at;

ALTER TABLE moraine.ingest_errors
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at;

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at;
ALTER TABLE moraine.event_links
  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,
  ADD COLUMN IF NOT EXISTS source_file String AFTER source_name,
  ADD COLUMN IF NOT EXISTS source_generation UInt32 AFTER source_file;
ALTER TABLE moraine.search_postings
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id,
  ADD COLUMN IF NOT EXISTS source_file String AFTER inference_provider,
  ADD COLUMN IF NOT EXISTS source_generation UInt32 AFTER source_file;
ALTER TABLE moraine.mcp_event_locator
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id;
ALTER TABLE moraine.mcp_event_navigation
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id;
ALTER TABLE moraine.mcp_event_navigation_seek
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id;
ALTER TABLE moraine.mcp_event_turn_seek
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id,
  ADD COLUMN IF NOT EXISTS source_name LowCardinality(String) AFTER source_host;
ALTER TABLE moraine.mcp_event_version_seek
  ADD COLUMN IF NOT EXISTS source_host String AFTER session_id,
  ADD COLUMN IF NOT EXISTS source_name LowCardinality(String) AFTER source_host;

DROP TABLE IF EXISTS moraine.mcp_event_navigation_seek_generation_preserving_036;
CREATE TABLE moraine.mcp_event_navigation_seek_generation_preserving_036
AS moraine.mcp_event_navigation_seek
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  session_id, sort_time, source_file, source_generation, source_offset,
  source_line_no, emission_index, event_uid, event_version, source_host
);

DROP TABLE IF EXISTS moraine.mcp_event_turn_seek_generation_preserving_036;
CREATE TABLE moraine.mcp_event_turn_seek_generation_preserving_036
AS moraine.mcp_event_turn_seek
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  session_id, turn_index, sort_time, source_file, source_generation,
  source_offset, source_line_no, emission_index, event_uid, event_version,
  source_host, source_name
);

DROP TABLE IF EXISTS moraine.mcp_event_version_seek_generation_preserving_036;
CREATE TABLE moraine.mcp_event_version_seek_generation_preserving_036
AS moraine.mcp_event_version_seek
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(event_uid) % 64
ORDER BY (
  event_uid, source_host, source_name, source_file, source_generation,
  event_version
);


-- Existing hostless canonical rows can be attributed only when a logical
-- source belongs to at most one shared host. Refuse an ambiguous cutover
-- rather than silently hiding or duplicating the pre-upgrade corpus.
SELECT throwIf(
  count() > 0,
  'cannot migrate hostless canonical rows: a source is published by multiple shared hosts; replay that backend from a clean publication state'
)
FROM
(
  SELECT source_name, source_file
  FROM moraine.v_current_published_source_generations
  WHERE source_host != ''
  GROUP BY source_name, source_file
  HAVING uniqExact(source_host) > 1
);

DROP TABLE IF EXISTS moraine.events_generation_preserving_036;
CREATE TABLE moraine.events_generation_preserving_036
AS moraine.events
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  source_host, source_name, source_file, source_generation,
  session_id, event_uid
);
INSERT INTO moraine.events_generation_preserving_036
SELECT events.* REPLACE (
  if(events.source_host = '', heads.source_host, events.source_host) AS source_host
)
FROM moraine.events AS events FINAL
LEFT JOIN
(
  SELECT
    source_name,
    source_file,
    any(source_host) AS source_host
  FROM moraine.v_current_published_source_generations
  GROUP BY source_name, source_file
) AS heads
  ON heads.source_name = events.source_name
 AND heads.source_file = events.source_file
SETTINGS max_insert_threads = 1, max_threads = 1, max_memory_usage = 1073741824;
OPTIMIZE TABLE moraine.events_generation_preserving_036 FINAL
SETTINGS max_threads = 1, max_memory_usage = 1073741824;

DROP TABLE IF EXISTS moraine.event_links_generation_preserving_036;
CREATE TABLE moraine.event_links_generation_preserving_036
AS moraine.event_links
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  source_host, source_name, source_file, source_generation, session_id,
  event_uid, link_type, linked_event_uid, linked_external_id
);
INSERT INTO moraine.event_links_generation_preserving_036
SELECT links.* REPLACE (
  if(links.source_host = '', owners.source_host, links.source_host) AS source_host,
  if(links.source_file = '', owners.source_file, links.source_file) AS source_file,
  if(links.source_generation = 0, owners.source_generation, links.source_generation)
    AS source_generation
)
FROM moraine.event_links AS links FINAL
LEFT JOIN
(
  SELECT
    source_name,
    event_uid,
    tupleElement(owner, 1) AS source_host,
    tupleElement(owner, 2) AS source_file,
    tupleElement(owner, 3) AS source_generation
  FROM
  (
    SELECT
      source_name,
      event_uid,
      argMax(
        tuple(source_host, source_file, source_generation),
        tuple(source_generation, event_version)
      ) AS owner
    FROM moraine.events_generation_preserving_036 FINAL
    GROUP BY source_name, event_uid
  )
) AS owners
  ON owners.source_name = links.source_name
 AND owners.event_uid = links.event_uid
SETTINGS max_insert_threads = 1, max_threads = 1, max_memory_usage = 1073741824;
OPTIMIZE TABLE moraine.event_links_generation_preserving_036 FINAL
SETTINGS max_threads = 1, max_memory_usage = 1073741824;

DROP TABLE IF EXISTS moraine.search_postings_generation_preserving_036;
CREATE TABLE moraine.search_postings_generation_preserving_036
AS moraine.search_postings
ENGINE = ReplacingMergeTree(post_version)
PARTITION BY cityHash64(term) % 32
ORDER BY (
  term, source_host, source_name, source_file, source_generation, doc_id
);

DROP TABLE IF EXISTS moraine.mcp_event_locator_generation_preserving_036;
CREATE TABLE moraine.mcp_event_locator_generation_preserving_036
AS moraine.mcp_event_locator
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(event_uid) % 64
ORDER BY (
  source_host, source_name, source_file, source_generation, event_uid
);

DROP TABLE IF EXISTS moraine.mcp_event_navigation_generation_preserving_036;
CREATE TABLE moraine.mcp_event_navigation_generation_preserving_036
AS moraine.mcp_event_navigation
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (
  source_host, source_name, source_file, source_generation, session_id, event_uid
);

-- Canonical content and source-bearing indexes cut over independently. Every
-- step is rerunnable: a retry simply rebuilds from whichever table is current.
EXCHANGE TABLES moraine.event_links AND moraine.event_links_generation_preserving_036;
EXCHANGE TABLES moraine.events AND moraine.events_generation_preserving_036;

-- Rebuild every source-bearing seek index before one metadata cutover. This
-- remains retry-safe even when a previous migration attempt stopped after
-- adding the host columns.

INSERT INTO moraine.mcp_event_navigation_seek_generation_preserving_036
  (session_id, source_host, sort_time, source_file, source_generation,
   source_offset, source_line_no, emission_index, event_uid, event_version,
   event_ts, turn_index, is_user_message, display_time, event_kind, actor_kind,
   payload_type, tool_call_id, tool_name, tool_phase, op_status, item_id,
   harness, inference_provider, source_name, is_metadata_bearing,
   has_session_title, has_session_name, has_session_summary, has_session_slug)
SELECT
  session_id, source_host,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')), event_uid,
  event_version, event_ts, turn_index,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at),
  event_kind, actor_kind, payload_type, tool_call_id, tool_name, tool_phase,
  op_status, item_id, harness, inference_provider, source_name,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))),
  toUInt8(notEmpty(JSONExtractString(payload_json, 'title'))),
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'name'))),
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'summary'))),
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'slug')))
FROM moraine.events FINAL
WHERE notEmpty(session_id);

INSERT INTO moraine.mcp_event_turn_seek_generation_preserving_036
  (session_id, source_host, source_name, turn_index, sort_time, source_file,
   source_generation, source_offset, source_line_no, emission_index, event_uid,
   event_version, event_ts, is_user_message, display_time, event_kind,
   actor_kind, payload_type, tool_call_id, tool_name, tool_phase, op_status)
SELECT
  session_id, source_host, source_name, turn_index,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')), event_uid,
  event_version, event_ts,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at),
  event_kind, actor_kind, payload_type, tool_call_id, tool_name, tool_phase,
  op_status
FROM moraine.events FINAL
WHERE notEmpty(session_id) AND turn_index > 0;

INSERT INTO moraine.mcp_event_version_seek_generation_preserving_036
  (event_uid, event_version, session_id, source_host, source_name, sort_time,
   source_file, source_generation, source_offset, source_line_no, emission_index)
SELECT
  event_uid, event_version, session_id, source_host, source_name,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index'))
FROM moraine.events FINAL
WHERE notEmpty(session_id);

INSERT INTO moraine.mcp_event_locator_generation_preserving_036
WITH JSONExtractString(payload_json, 'moraine_tool_io', 'input_json') AS tool_input
SELECT
  event_uid, event_version, ingested_at, session_id, source_host, source_name,
  source_file, source_generation, source_offset, source_line_no,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))),
  hex(SHA256(text_content)), JSONExtractString(payload_json, 'phase'),
  JSONExtractString(payload_json, 'moraine_tool_io', 'project_id'),
  JSONExtractString(payload_json, 'moraine_tool_io', 'repo_rel_path'),
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root'),
  arrayFilter(path -> path != '', arrayDistinct(arrayConcat(
    extractAll(tool_input, '"(?:file_path|notebook_path|path|target_file|relativeWorkspacePath|relative_workspace_path|filepath|file|filename)"[[:space:]]*:[[:space:]]*"((?:[^"\\\\]|\\\\.)*)"'),
    extractAll(if(JSONExtractString(tool_input, 'command') != '', JSONExtractString(tool_input, 'command'), JSONExtractString(tool_input, 'cmd')), '(?:^|[[:space:]''"`=(])((?:/|\\./|\\.\\./)?[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+|[A-Za-z0-9_-]+\\.[A-Za-z0-9_.-]+)(?:[[:space:]''"`,;|&<>)]|$)'),
    [JSONExtractString(tool_input, 'file_path'), JSONExtractString(tool_input, 'notebook_path'), JSONExtractString(tool_input, 'path'), JSONExtractString(tool_input, 'target_file'), JSONExtractString(tool_input, 'relativeWorkspacePath'), JSONExtractString(tool_input, 'relative_workspace_path'), JSONExtractString(tool_input, 'filepath'), JSONExtractString(tool_input, 'file'), JSONExtractString(tool_input, 'filename')]
  ))),
  toUInt8(positionCaseInsensitiveUTF8(payload_json, 'codex-mcp') > 0)
FROM moraine.events FINAL
WHERE notEmpty(session_id);

INSERT INTO moraine.mcp_event_navigation_generation_preserving_036
SELECT
  session_id, source_host,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')), event_uid,
  event_version, source_name, event_ts,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), event_kind,
  actor_kind, payload_type, turn_index, tool_call_id, tool_name, tool_phase,
  op_status, item_id, harness, inference_provider, cwd,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change')))
FROM moraine.events FINAL
WHERE notEmpty(session_id);

INSERT INTO moraine.search_postings_generation_preserving_036
  (post_version, term, doc_id, session_id, source_host, source_name, harness,
   inference_provider, source_file, source_generation, event_class, payload_type,
   actor_role, name, phase, source_ref, doc_len, tf)
SELECT
  d.event_version, d.term, d.event_uid, d.session_id, d.source_host,
  d.source_name, d.harness, d.inference_provider, d.source_file,
  d.source_generation, d.event_class, d.payload_type, d.actor_role,
  d.name, d.phase, d.source_ref, d.doc_len, toUInt16(count())
FROM
(
  SELECT
    event_version, event_uid, session_id, source_host, source_name, harness,
    inference_provider, source_file, source_generation,
    event_kind AS event_class, payload_type, actor_kind AS actor_role,
    tool_name AS name, if(tool_phase != '', tool_phase, op_status) AS phase,
    source_ref,
    toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.events FINAL
) AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_insert_threads = 1, max_threads = 1, max_memory_usage = 1073741824;

EXCHANGE TABLES moraine.mcp_event_locator AND moraine.mcp_event_locator_generation_preserving_036;
EXCHANGE TABLES moraine.mcp_event_navigation AND moraine.mcp_event_navigation_generation_preserving_036;
EXCHANGE TABLES moraine.search_postings AND moraine.search_postings_generation_preserving_036;
EXCHANGE TABLES moraine.mcp_event_navigation_seek AND moraine.mcp_event_navigation_seek_generation_preserving_036;
EXCHANGE TABLES moraine.mcp_event_turn_seek AND moraine.mcp_event_turn_seek_generation_preserving_036;
EXCHANGE TABLES moraine.mcp_event_version_seek AND moraine.mcp_event_version_seek_generation_preserving_036;

CREATE OR REPLACE VIEW moraine.v_live_events AS
SELECT *
FROM
(
  SELECT e.*
  FROM moraine.events AS e FINAL
  ALL INNER JOIN moraine.v_readable_published_source_generations AS h
    ON e.source_host = h.source_host
   AND e.source_name = h.source_name
   AND e.source_file = h.source_file
   AND e.source_generation = h.source_generation
)
ORDER BY source_host, session_id, event_uid, event_version DESC
LIMIT 1 BY source_host, session_id, event_uid;

CREATE OR REPLACE VIEW moraine.v_live_event_links AS
SELECT l.*
FROM moraine.event_links AS l FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON l.source_host = e.source_host
 AND l.source_name = e.source_name
 AND l.source_file = e.source_file
 AND l.source_generation = e.source_generation
 AND l.session_id = e.session_id
 AND l.event_uid = e.event_uid;

CREATE OR REPLACE VIEW moraine.v_live_mcp_event_locator AS
SELECT l.*
FROM moraine.mcp_event_locator AS l FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON l.source_host = e.source_host
 AND l.source_name = e.source_name
 AND l.source_file = e.source_file
 AND l.source_generation = e.source_generation
 AND l.session_id = e.session_id
 AND l.event_uid = e.event_uid
 AND l.event_version = e.event_version;

CREATE OR REPLACE VIEW moraine.v_live_mcp_event_navigation AS
SELECT n.*
FROM moraine.mcp_event_navigation AS n FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON n.source_host = e.source_host
 AND n.source_name = e.source_name
 AND n.source_file = e.source_file
 AND n.source_generation = e.source_generation
 AND n.session_id = e.session_id
 AND n.event_uid = e.event_uid
 AND n.event_version = e.event_version;

CREATE OR REPLACE VIEW moraine.v_live_search_postings AS
SELECT p.*
FROM moraine.search_postings AS p FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON p.source_host = e.source_host
 AND p.source_name = e.source_name
 AND p.source_file = e.source_file
 AND p.source_generation = e.source_generation
 AND p.session_id = e.session_id
 AND p.doc_id = e.event_uid
 AND p.post_version = e.event_version;

CREATE OR REPLACE VIEW moraine.v_live_mcp_event_navigation_seek AS
SELECT s.*
FROM moraine.mcp_event_navigation_seek AS s FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON s.source_host = e.source_host
 AND s.source_name = e.source_name
 AND s.source_file = e.source_file
 AND s.source_generation = e.source_generation
 AND s.session_id = e.session_id
 AND s.event_uid = e.event_uid
 AND s.event_version = e.event_version;

CREATE OR REPLACE VIEW moraine.v_live_mcp_event_turn_seek AS
SELECT s.*
FROM moraine.mcp_event_turn_seek AS s FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON s.source_host = e.source_host
 AND s.source_name = e.source_name
 AND s.source_file = e.source_file
 AND s.source_generation = e.source_generation
 AND s.session_id = e.session_id
 AND s.event_uid = e.event_uid
 AND s.event_version = e.event_version;

CREATE OR REPLACE VIEW moraine.v_live_mcp_event_version_seek AS
SELECT s.*
FROM moraine.mcp_event_version_seek AS s FINAL
ALL INNER JOIN moraine.v_live_events AS e
  ON s.source_host = e.source_host
 AND s.source_name = e.source_name
 AND s.source_file = e.source_file
 AND s.source_generation = e.source_generation
 AND s.session_id = e.session_id
 AND s.event_uid = e.event_uid
 AND s.event_version = e.event_version;

CREATE OR REPLACE VIEW moraine.search_term_stats AS
SELECT term, toUInt64(uniqExact(doc_id)) AS docs
FROM moraine.v_live_search_postings
GROUP BY term;

CREATE OR REPLACE VIEW moraine.search_corpus_stats AS
SELECT toUInt8(0) AS bucket, toUInt64(count()) AS docs,
  toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len
FROM
(
  SELECT event_uid, max(doc_len) AS doc_len
  FROM moraine.v_live_mcp_event_locator
  GROUP BY event_uid
)
WHERE doc_len > 0;

CREATE OR REPLACE VIEW moraine.v_all_events AS
SELECT
  ingested_at, event_uid, origin_event_id AS compacted_parent_uid, session_id,
  session_date, source_file, source_inode, source_generation, source_line_no,
  source_offset, source_ref, record_ts, event_kind AS event_class, payload_type,
  actor_kind AS actor_role, toString(turn_index) AS turn_id, item_id,
  tool_call_id AS call_id, tool_name AS name,
  if(tool_phase != '', tool_phase, op_status) AS phase, text_content,
  payload_json, token_usage_json, endpoint_kind, token_usage_buckets,
  token_usage_native_units, event_version
FROM moraine.v_live_events;

CREATE MATERIALIZED VIEW moraine.mv_mcp_event_locator_from_events
TO moraine.mcp_event_locator AS
WITH JSONExtractString(payload_json, 'moraine_tool_io', 'input_json') AS tool_input
SELECT event_uid, event_version, ingested_at, session_id, source_host, source_name,
  source_file, source_generation, source_offset, source_line_no,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
  hex(SHA256(text_content)) AS text_digest,
  JSONExtractString(payload_json, 'phase') AS payload_phase,
  JSONExtractString(payload_json, 'moraine_tool_io', 'project_id') AS project_id,
  JSONExtractString(payload_json, 'moraine_tool_io', 'repo_rel_path') AS repo_rel_path,
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') AS worktree_root,
  arrayFilter(path -> path != '', arrayDistinct(arrayConcat(
    extractAll(tool_input, '"(?:file_path|notebook_path|path|target_file|relativeWorkspacePath|relative_workspace_path|filepath|file|filename)"[[:space:]]*:[[:space:]]*"((?:[^"\\\\]|\\\\.)*)"'),
    extractAll(if(JSONExtractString(tool_input, 'command') != '', JSONExtractString(tool_input, 'command'), JSONExtractString(tool_input, 'cmd')), '(?:^|[[:space:]''"`=(])((?:/|\\./|\\.\\./)?[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+|[A-Za-z0-9_-]+\\.[A-Za-z0-9_.-]+)(?:[[:space:]''"`,;|&<>)]|$)'),
    [JSONExtractString(tool_input, 'file_path'), JSONExtractString(tool_input, 'notebook_path'), JSONExtractString(tool_input, 'path'), JSONExtractString(tool_input, 'target_file'), JSONExtractString(tool_input, 'relativeWorkspacePath'), JSONExtractString(tool_input, 'relative_workspace_path'), JSONExtractString(tool_input, 'filepath'), JSONExtractString(tool_input, 'file'), JSONExtractString(tool_input, 'filename')]
  ))) AS path_tokens,
  toUInt8(positionCaseInsensitiveUTF8(payload_json, 'codex-mcp') > 0) AS has_codex_mcp
FROM moraine.events
WHERE notEmpty(session_id);

CREATE MATERIALIZED VIEW moraine.mv_mcp_event_navigation_from_events
TO moraine.mcp_event_navigation AS
SELECT session_id, source_host,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  event_uid, event_version, source_name, event_ts,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind, actor_kind, payload_type, turn_index, tool_call_id, tool_name,
  tool_phase, op_status, item_id, harness, inference_provider, cwd,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))) AS is_metadata_bearing
FROM moraine.events
WHERE notEmpty(session_id);

CREATE MATERIALIZED VIEW moraine.mv_search_postings
TO moraine.search_postings AS
SELECT d.event_version AS post_version, d.term, d.event_uid AS doc_id,
  d.session_id, d.source_host, d.source_name, d.harness, d.inference_provider,
  d.source_file, d.source_generation, d.event_class, d.payload_type,
  d.actor_role, d.name, d.phase, d.source_ref, d.doc_len,
  toUInt16(count()) AS tf
FROM
(
  SELECT event_version, event_uid, session_id, source_host, source_name, harness,
    inference_provider, source_file, source_generation,
    event_kind AS event_class, payload_type, actor_kind AS actor_role,
    tool_name AS name, if(tool_phase != '', tool_phase, op_status) AS phase,
    source_ref,
    toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.events
) AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY ALL;

CREATE MATERIALIZED VIEW moraine.mv_mcp_event_navigation_seek_from_events
TO moraine.mcp_event_navigation_seek AS
SELECT
  session_id, source_host,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  event_uid, event_version, event_ts, turn_index,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind, actor_kind, payload_type, tool_call_id, tool_name, tool_phase,
  op_status, item_id, harness, inference_provider, source_name,
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))) AS is_metadata_bearing,
  toUInt8(notEmpty(JSONExtractString(payload_json, 'title'))) AS has_session_title,
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'name'))) AS has_session_name,
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'summary'))) AS has_session_summary,
  toUInt8(event_kind = 'session_meta' AND notEmpty(JSONExtractString(payload_json, 'slug'))) AS has_session_slug
FROM moraine.events
WHERE notEmpty(session_id);

CREATE MATERIALIZED VIEW moraine.mv_mcp_event_turn_seek_from_events
TO moraine.mcp_event_turn_seek AS
SELECT
  session_id, source_host, source_name, turn_index,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index,
  event_uid, event_version, event_ts,
  toUInt8(actor_kind = 'user' AND event_kind = 'message') AS is_user_message,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS display_time,
  event_kind, actor_kind, payload_type, tool_call_id, tool_name, tool_phase,
  op_status
FROM moraine.events
WHERE notEmpty(session_id) AND turn_index > 0;

CREATE MATERIALIZED VIEW moraine.mv_mcp_event_version_seek_from_events
TO moraine.mcp_event_version_seek AS
SELECT event_uid, event_version, session_id, source_host, source_name,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)) AS sort_time,
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index
FROM moraine.events
WHERE notEmpty(session_id);

CREATE MATERIALIZED VIEW moraine.mv_file_attention_project_roots_from_events
TO moraine.file_attention_project_roots AS
SELECT JSONExtractString(payload_json, 'moraine_tool_io', 'project_id') AS project_id,
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') AS worktree_root,
  event_version AS observed_version
FROM moraine.events
WHERE startsWith(JSONExtractString(payload_json, 'moraine_tool_io', 'project_id'), 'git:')
  AND JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') != '';

DROP TABLE IF EXISTS moraine.events_generation_preserving_036;
DROP TABLE IF EXISTS moraine.event_links_generation_preserving_036;
DROP TABLE IF EXISTS moraine.search_postings_generation_preserving_036;
DROP TABLE IF EXISTS moraine.mcp_event_locator_generation_preserving_036;
DROP TABLE IF EXISTS moraine.mcp_event_navigation_generation_preserving_036;
