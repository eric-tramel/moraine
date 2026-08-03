-- Replay-stable canonical identity.
--
-- Source generation, file, line and offset remain provenance, but no longer
-- participate in canonical replacement identity. Writers are quiesced by the
-- CLI before this migration. Every staging object is disposable: rerunning the
-- migration rebuilds it from the currently installed canonical tables, whether
-- those tables still use the v1 key or already use the v2 key.

DROP VIEW IF EXISTS moraine.mv_mcp_event_locator_from_events;
DROP VIEW IF EXISTS moraine.mv_mcp_event_navigation_from_events;
DROP VIEW IF EXISTS moraine.mv_search_postings;
DROP VIEW IF EXISTS moraine.mv_file_attention_project_roots_from_events;
DROP VIEW IF EXISTS moraine.events_replay_source_033;
DROP VIEW IF EXISTS moraine.event_uid_base_source_033;
DROP VIEW IF EXISTS moraine.search_postings_source_033;

ALTER TABLE moraine.mcp_event_navigation
  ADD COLUMN IF NOT EXISTS emission_index UInt32 AFTER source_line_no;


DROP TABLE IF EXISTS moraine.event_uid_map_033;
DROP TABLE IF EXISTS moraine.event_uid_lookup_033;
DROP TABLE IF EXISTS moraine.events_replay_stable_033;
DROP TABLE IF EXISTS moraine.event_links_replay_stable_033;

-- Writers are quiesced by the CLI. Compact the source replacement keys once
-- so every subsequent rebuild can stream physical rows without SELECT FINAL.
OPTIMIZE TABLE moraine.events FINAL
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824;

CREATE VIEW moraine.event_uid_base_source_033 AS
WITH
  ['createdAt', 'created_at', 'cwd', 'directory', 'lastUpdatedAt', 'last_updated',
   'moraine_emission_index', 'moraine_semantic_occurrence', 'moraine_tool_io',
   'project_id', 'repo_rel_path', 'request_event_uid', 'session_start', 'source_ref',
   'time_created', 'timestamp', 'updated_at', 'workspacePath', 'worktree_root'
  ] AS excluded_payload_fields,
  concat(
    '{',
    arrayStringConcat(
      arrayMap(
        item -> concat(toJSONString(item.1), ':', item.2),
        arraySort(
          item -> item.1,
          arrayFilter(
            item -> NOT has(excluded_payload_fields, item.1),
            JSONExtractKeysAndValuesRaw(payload_json)
          )
        )
      ),
      ','
    ),
    '}'
  ) AS semantic_payload,
  [
    'moraine:event:v2',
    author,
    toString(harness),
    toString(inference_provider),
    session_id,
    event_kind,
    actor_kind,
    payload_type,
    op_kind,
    op_status,
    request_id,
    trace_id,
    toString(turn_index),
    item_id,
    tool_call_id,
    parent_tool_call_id,
    origin_tool_call_id,
    tool_name,
    tool_phase,
    toString(tool_error),
    agent_run_id,
    agent_label,
    coord_group_id,
    coord_group_label,
    toString(is_substream),
    toString(model),
    toString(endpoint_kind),
    toString(input_tokens),
    toString(output_tokens),
    toString(cache_read_tokens),
    toString(cache_write_tokens),
    toString(latency_ms),
    toString(retry_count),
    toString(service_tier),
    toJSONString(content_types),
    toString(has_reasoning),
    text_content,
    arrayStringConcat(arrayMap(
      key -> concat(
        toString(length(key)), ':', key,
        toString(length(toString(token_usage_buckets[key]))), ':',
        toString(token_usage_buckets[key])
      ),
      arraySort(mapKeys(token_usage_buckets))
    )),
    arrayStringConcat(arrayMap(
      key -> concat(
        toString(length(key)), ':', key,
        toString(length(toString(token_usage_native_units[key]))), ':',
        toString(token_usage_native_units[key])
      ),
      arraySort(mapKeys(token_usage_native_units))
    )),
    semantic_payload,
    JSONExtractString(payload_json, 'moraine_tool_io', 'tool_call_id'),
    JSONExtractString(payload_json, 'moraine_tool_io', 'parent_tool_call_id'),
    JSONExtractString(payload_json, 'moraine_tool_io', 'tool_name'),
    JSONExtractString(payload_json, 'moraine_tool_io', 'tool_phase'),
    if(
      JSONHas(payload_json, 'moraine_tool_io', 'tool_error'),
      toString(JSONExtractUInt(payload_json, 'moraine_tool_io', 'tool_error')),
      ''
    ),
    JSONExtractString(payload_json, 'moraine_tool_io', 'input_json'),
    JSONExtractString(payload_json, 'moraine_tool_io', 'output_json'),
    JSONExtractString(payload_json, 'moraine_tool_io', 'output_text')
  ] AS identity_fields
SELECT
  event_uid AS old_event_uid,
  source_name,
  source_file,
  source_generation,
  source_line_no,
  source_offset,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_semantic_occurrence'))
    AS existing_occurrence,
  lower(hex(SHA256(arrayStringConcat(arrayMap(
    value -> concat(toString(length(value)), ':', value),
    identity_fields
  ))))) AS base_event_uid
-- The source table was compacted above, so each physical row is one event
-- occurrence observed before this migration.
FROM moraine.events;

CREATE TABLE moraine.event_uid_map_033 (
  old_event_uid String,
  new_event_uid String,
  semantic_occurrence UInt32
)
ENGINE = MergeTree
ORDER BY old_event_uid;

-- Completed-cutover retries already carry their occurrence. Preserve those
-- assignments without adding another corpus-sized window state.
INSERT INTO moraine.event_uid_map_033
SELECT
  old_event_uid,
  lower(hex(SHA256(concat(
    toString(length('moraine:event:occurrence:v1')), ':', 'moraine:event:occurrence:v1',
    toString(length(base_event_uid)), ':', base_event_uid,
    toString(length(toString(existing_occurrence))), ':', toString(existing_occurrence)
  )))) AS new_event_uid,
  existing_occurrence AS semantic_occurrence
FROM moraine.event_uid_base_source_033
WHERE existing_occurrence > 0
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 8192,
  min_insert_block_size_bytes = 16777216,
  max_threads = 1,
  max_memory_usage = 1073741824;

-- Fresh schema-032 rows are unmarked. Rank only this legacy population; on a
-- completed-cutover retry it contains unique base events and needs no sort.
INSERT INTO moraine.event_uid_map_033
SELECT
  old_event_uid,
  if(
    duplicate_count = 1,
    base_event_uid,
    lower(hex(SHA256(concat(
      toString(length('moraine:event:occurrence:v1')), ':', 'moraine:event:occurrence:v1',
      toString(length(base_event_uid)), ':', base_event_uid,
      toString(length(toString(occurrence))), ':', toString(occurrence)
    ))))
  ) AS new_event_uid,
  if(duplicate_count = 1, 0, toUInt32(occurrence)) AS semantic_occurrence
FROM (
  SELECT
    *,
    count() OVER (
      PARTITION BY source_name, source_file, source_generation, source_line_no,
        source_offset, base_event_uid
    ) AS duplicate_count,
    row_number() OVER (
      PARTITION BY source_name, source_file, source_generation, source_line_no,
        source_offset, base_event_uid
      ORDER BY old_event_uid
    ) AS occurrence
  FROM moraine.event_uid_base_source_033
  WHERE existing_occurrence = 0
)
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 8192,
  min_insert_block_size_bytes = 16777216,
  max_bytes_before_external_sort = 67108864,
  max_threads = 1,
  max_memory_usage = 1073741824;

DROP VIEW moraine.event_uid_base_source_033;

CREATE TABLE moraine.event_uid_lookup_033 (
  old_event_uid String,
  new_event_uid String,
  semantic_occurrence UInt32
)
ENGINE = Join(ANY, LEFT, old_event_uid);

INSERT INTO moraine.event_uid_lookup_033
SELECT old_event_uid, new_event_uid, semantic_occurrence
FROM moraine.event_uid_map_033
SETTINGS max_block_size = 1024,
  max_threads = 1,
  max_memory_usage = 1073741824;

CREATE TABLE moraine.events_replay_stable_033
AS moraine.events
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, event_uid);

CREATE VIEW moraine.events_replay_source_033 AS
WITH
  if(
    toString(e.harness) = 'qwen-code'
      AND e.actor_kind = 'assistant'
      AND JSONHas(e.payload_json, 'part')
      AND NOT JSONHas(e.payload_json, 'moraine_emission_index'),
    JSONMergePatch(
      e.payload_json,
      concat(
        '{"moraine_emission_index":',
        toString(reinterpretAsUInt32(reverse(unhex(substring(e.event_uid, 1, 8)))) + 1),
        '}'
      )
    ),
    e.payload_json
  ) AS emission_payload,
  joinGet(
    'moraine.event_uid_lookup_033',
    'semantic_occurrence',
    e.event_uid
  ) AS semantic_occurrence,
  if(
    semantic_occurrence = 0,
    emission_payload,
    JSONMergePatch(
      emission_payload,
      concat('{"moraine_semantic_occurrence":', toString(semantic_occurrence), '}')
    )
  ) AS occurrence_payload,
  joinGet(
    'moraine.event_uid_lookup_033',
    'new_event_uid',
    JSONExtractString(e.payload_json, 'request_event_uid')
  ) AS request_event_uid
SELECT e.* REPLACE (
  joinGet('moraine.event_uid_lookup_033', 'new_event_uid', e.event_uid) AS event_uid,
  coalesce(
    nullIf(joinGet('moraine.event_uid_lookup_033', 'new_event_uid', e.origin_event_id), ''),
    e.origin_event_id
  ) AS origin_event_id,
  if(
    empty(request_event_uid),
    occurrence_payload,
    JSONMergePatch(
      occurrence_payload,
      concat('{"request_event_uid":', toJSONString(request_event_uid), '}')
    )
  ) AS payload_json
)
FROM moraine.events AS e FINAL;

-- Rebuild target partitions in bounded groups. A single INSERT spanning all 64
-- partitions retains one MergeTree write stream per partition and grows with
-- the corpus even when the SELECT side is streamed.
INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 0
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 1
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 2
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 3
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 4
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 5
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 6
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.events_replay_stable_033
SELECT *
FROM moraine.events_replay_source_033
WHERE intDiv(cityHash64(session_id) % 64, 8) = 7
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

DROP VIEW moraine.events_replay_source_033;

-- Make replacement deterministic before the cutover so downstream rebuilds
-- and canonical readers do not need FINAL to hide transient duplicate parts.
OPTIMIZE TABLE moraine.events_replay_stable_033 FINAL
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824;

CREATE TABLE moraine.event_links_replay_stable_033
AS moraine.event_links
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(session_id) % 64
ORDER BY (session_id, event_uid, link_type, linked_event_uid, linked_external_id);

INSERT INTO moraine.event_links_replay_stable_033
SELECT l.* REPLACE (
  coalesce(
    nullIf(joinGet('moraine.event_uid_lookup_033', 'new_event_uid', l.event_uid), ''),
    l.event_uid
  ) AS event_uid,
  coalesce(
    nullIf(joinGet('moraine.event_uid_lookup_033', 'new_event_uid', l.linked_event_uid), ''),
    l.linked_event_uid
  ) AS linked_event_uid
)
FROM moraine.event_links AS l FINAL
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

OPTIMIZE TABLE moraine.event_links_replay_stable_033 FINAL
SETTINGS max_threads = 1,
  max_memory_usage = 1073741824;

DROP TABLE moraine.event_uid_lookup_033;



-- Each EXCHANGE is atomic. Links move first so a failure between the two
-- cutovers is restart-safe: events still provide the old-to-new UID map, while
-- already-rewritten links pass through unchanged on retry. Exchanging events
-- first would strand old link UIDs after a partial cutover.
EXCHANGE TABLES
  moraine.event_links AND moraine.event_links_replay_stable_033;
EXCHANGE TABLES
  moraine.events AND moraine.events_replay_stable_033;

CREATE OR REPLACE VIEW moraine.v_conversation_trace AS
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
      source_file, source_generation, source_offset, source_line_no,
      toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')), event_uid
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
            source_file, source_generation, source_offset, source_line_no,
            toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')), event_uid
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

TRUNCATE TABLE moraine.mcp_event_locator;
TRUNCATE TABLE moraine.mcp_event_navigation;
TRUNCATE TABLE moraine.search_postings;

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
    extractAll(tool_input, '"(?:file_path|notebook_path|path|target_file|relativeWorkspacePath|relative_workspace_path|filepath|file|filename)"[[:space:]]*:[[:space:]]*"((?:[^"\\\\]|\\\\.)*)"'),
    extractAll(if(JSONExtractString(tool_input, 'command') != '', JSONExtractString(tool_input, 'command'), JSONExtractString(tool_input, 'cmd')), '(?:^|[[:space:]''"`=(])((?:/|\\./|\\.\\./)?[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+|[A-Za-z0-9_-]+\\.[A-Za-z0-9_.-]+)(?:[[:space:]''"`,;|&<>)]|$)'),
    [JSONExtractString(tool_input, 'file_path'), JSONExtractString(tool_input, 'notebook_path'), JSONExtractString(tool_input, 'path'), JSONExtractString(tool_input, 'target_file'), JSONExtractString(tool_input, 'relativeWorkspacePath'), JSONExtractString(tool_input, 'relative_workspace_path'), JSONExtractString(tool_input, 'filepath'), JSONExtractString(tool_input, 'file'), JSONExtractString(tool_input, 'filename')]
  ))),
  toUInt8(positionCaseInsensitiveUTF8(payload_json, 'codex-mcp') > 0)
FROM moraine.events
WHERE notEmpty(session_id)
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.mcp_event_navigation
SELECT session_id,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3)),
  source_file, source_generation, source_offset, source_line_no,
  toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')), event_uid,
  event_version, source_name, event_ts,
  ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), event_kind,
  actor_kind, payload_type, turn_index, tool_call_id, tool_name, tool_phase,
  op_status, item_id, harness, inference_provider, cwd,
  toUInt8(actor_kind = 'user' AND event_kind = 'message'),
  toUInt8(event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change')))
FROM moraine.events
WHERE notEmpty(session_id)
SETTINGS max_block_size = 1024,
  preferred_max_column_in_block_size_bytes = 33554432,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

CREATE VIEW moraine.search_postings_source_033 AS
SELECT event_version, arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term,
  event_uid, session_id, source_name, harness, inference_provider,
  event_kind AS event_class, payload_type, actor_kind AS actor_role,
  tool_name AS name, if(tool_phase != '', tool_phase, op_status) AS phase,
  source_ref,
  toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len
FROM moraine.events;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 0
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 1
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 2
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 3
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 4
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 5
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 6
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 7
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 8
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 9
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 10
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 11
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 12
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 13
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 14
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

INSERT INTO moraine.search_postings
SELECT d.*, toUInt16(count())
FROM moraine.search_postings_source_033 AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
  AND intDiv(cityHash64(d.session_id) % 64, 4) = 15
GROUP BY ALL
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864,
  max_block_size = 1024,
  min_insert_block_size_rows = 0,
  min_insert_block_size_bytes = 0,
  max_insert_threads = 1,
  max_threads = 1,
  max_memory_usage = 1073741824;

DROP VIEW moraine.search_postings_source_033;

CREATE MATERIALIZED VIEW moraine.mv_mcp_event_locator_from_events
TO moraine.mcp_event_locator AS
WITH JSONExtractString(payload_json, 'moraine_tool_io', 'input_json') AS tool_input
SELECT event_uid, event_version, ingested_at, session_id, source_name, source_file,
  source_generation, source_offset, source_line_no,
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
SELECT session_id,
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
  d.session_id, d.source_name, d.harness, d.inference_provider,
  d.event_class, d.payload_type, d.actor_role, d.name, d.phase,
  d.source_ref, d.doc_len, toUInt16(count()) AS tf
FROM
(
  SELECT event_version, event_uid, session_id, source_name, harness,
    inference_provider, event_kind AS event_class, payload_type,
    actor_kind AS actor_role, tool_name AS name,
    if(tool_phase != '', tool_phase, op_status) AS phase, source_ref,
    toUInt32(length(extractAll(lowerUTF8(text_content), '[a-z0-9_]+'))) AS doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.events
) AS d
WHERE d.doc_len > 0 AND lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY d.event_version, d.term, d.event_uid, d.session_id, d.source_name,
  d.harness, d.inference_provider, d.event_class, d.payload_type, d.actor_role,
  d.name, d.phase, d.source_ref, d.doc_len;

CREATE MATERIALIZED VIEW moraine.mv_file_attention_project_roots_from_events
TO moraine.file_attention_project_roots AS
SELECT JSONExtractString(payload_json, 'moraine_tool_io', 'project_id') AS project_id,
  JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') AS worktree_root,
  event_version AS observed_version
FROM moraine.events
WHERE startsWith(JSONExtractString(payload_json, 'moraine_tool_io', 'project_id'), 'git:')
  AND JSONExtractString(payload_json, 'moraine_tool_io', 'worktree_root') != '';

DROP TABLE IF EXISTS moraine.events_replay_stable_033;
DROP TABLE IF EXISTS moraine.event_links_replay_stable_033;
DROP TABLE IF EXISTS moraine.event_uid_lookup_033;
DROP TABLE IF EXISTS moraine.event_uid_map_033;
