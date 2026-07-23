INSERT INTO `moraine`.mcp_open_publication_headers
(session_id, candidate_publication_id, slot, generation, source_revision, dirty_revision, first_event_time,
last_event_time, total_turns, total_events, user_messages, assistant_messages,
tool_calls, tool_results, mode, first_event_uid, last_event_uid, last_actor_role,
title, source, harness, inference_provider, session_slug, session_summary,
list_title, list_session_summary, completed, terminal_event_uid, origin_cwd,
tombstone, required_source_heads, required_heads_fingerprint, header_revision,
publisher_id, operation_id)
WITH
canonical AS (
SELECT
ingested_at, event_uid, session_id, source_host, source_name, harness, inference_provider, source_file,
source_generation, source_line_no, source_offset, source_ref, record_ts, event_ts,
event_kind AS event_class, actor_kind AS actor_role, payload_type, turn_index, toString(turn_index) AS turn_id, item_id,
tool_call_id AS call_id, tool_name AS name, if(tool_phase != '', tool_phase, op_status) AS phase,
text_content, payload_json, token_usage_json, endpoint_kind, token_usage_buckets,
token_usage_native_units, cwd, event_version
FROM `moraine`.events AS e FINAL
PREWHERE e.session_id = 'session-a'
WHERE (e.source_host = 'host-a' AND e.source_name = 'codex' AND e.source_file = '/sessions/a.jsonl' AND e.source_generation = 3) OR (e.source_host = 'host-b' AND e.source_name = 'codex' AND e.source_file = '/sessions/b.jsonl' AND e.source_generation = 7)
),
canonical_revision AS (
SELECT if(count() = 0, toUInt64(0), toUInt64(cityHash64(arraySort(groupArray(tuple(event_uid, event_version)))))) AS source_revision
FROM canonical
),
ordered AS (
SELECT canonical.*,
ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS event_time,
row_number() OVER canonical_window AS event_order,
if(toUInt32(turn_index) > 0, toUInt32(turn_index), greatest(toUInt32(1), toUInt32(sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER canonical_rows))) AS turn_seq,
revision.source_revision AS source_revision
FROM canonical
CROSS JOIN canonical_revision AS revision
WHERE revision.source_revision = 42

WINDOW
canonical_window AS (PARTITION BY session_id ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_host, source_file, source_generation, source_offset, source_line_no, event_uid),
canonical_rows AS (PARTITION BY session_id ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_host, source_file, source_generation, source_offset, source_line_no, event_uid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
typed AS (
SELECT *, multiIf(lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg'))), 'user_input',lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg'))), 'assistant_response',lowerUTF8(actor_role) != 'system' AND (event_class = 'reasoning' OR payload_type IN ('agent_reasoning', 'reasoning', 'thinking')), 'reasoning',lowerUTF8(actor_role) != 'system' AND (event_class = 'tool_call' OR payload_type IN ('tool_use', 'function_call', 'custom_tool_call', 'web_search_call')), 'tool_call',lowerUTF8(actor_role) != 'system' AND (event_class = 'tool_result' OR payload_type IN ('tool_result', 'function_call_output', 'custom_tool_call_output', 'search_results_received')), 'tool_response',event_class IN ('compacted_raw', 'summary') OR payload_type IN ('compacted', 'summary'), 'compaction',event_class = 'queue_operation' OR payload_type IN ('task_started', 'task_complete', 'turn_aborted', 'item_completed', 'queue-operation'), 'runtime',lowerUTF8(actor_role) = 'system' OR event_class IN ('system', 'progress', 'file_history_snapshot') OR payload_type IN ('system', 'progress', 'file-history-snapshot', 'file_history_snapshot'), 'system','unknown') AS event_type,
empty(trimBoth(text_content)) AS summary_is_payload,
if(summary_is_payload,
leftUTF8(payload_json, 131071),
leftUTF8(text_content, 65536)) AS summary_source,
if(notEmpty(trimBoth(name)), trimBoth(name), trimBoth(call_id)) AS tool_label
FROM ordered
),
enriched AS (
SELECT *,
toUInt32(row_number() OVER (PARTITION BY session_id, turn_seq ORDER BY event_order, event_uid)) AS event_ordinal,
lagInFrame(event_uid, 1, '') OVER event_window AS previous_event_uid,
leadInFrame(event_uid, 1, '') OVER event_window AS next_event_uid
FROM typed
WINDOW event_window AS (PARTITION BY session_id ORDER BY event_order, event_uid ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
summarized AS (
SELECT *, CAST(tuple(
event_uid, event_order, toString(event_time),
toInt64(toUnixTimestamp64Milli(event_time)), actor_role, event_class, payload_type,
event_type, call_id, name, phase, summary_source, toUInt8(summary_is_payload)
), 'Tuple(event_uid String, event_order UInt64, event_time String, event_unix_ms Int64, actor_role String, event_class String, payload_type String, event_type String, call_id String, name String, phase String, summary_source String, summary_is_payload UInt8)') AS event_summary
FROM enriched
),
header AS (
SELECT
session_id, max(projected.source_revision) AS source_revision, min(event_time) AS first_event_time,
max(event_time) AS last_event_time, toUInt32(max(turn_seq)) AS total_turns, count() AS total_events,
countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,
countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,
countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,
multiIf(
countIf(payload_type = 'web_search_call' OR payload_type = 'search_results_received' OR (payload_type = 'tool_use' AND name IN ('WebSearch', 'WebFetch'))) > 0, 'web_search',
countIf(source_name = 'codex-mcp' OR (lowerUTF8(trimBoth(name)) IN ('search', 'search_sessions', 'open', 'list_sessions', 'file_attention') OR (length(splitByString('__', lowerUTF8(trimBoth(name)))) = 3 AND arrayElement(splitByString('__', lowerUTF8(trimBoth(name))), 1) = 'mcp' AND arrayElement(splitByString('__', lowerUTF8(trimBoth(name))), 2) = 'moraine' AND arrayElement(splitByString('__', lowerUTF8(trimBoth(name))), 3) IN ('search', 'search_sessions', 'open', 'list_sessions', 'file_attention')))) > 0, 'mcp_internal',
countIf(event_class IN ('tool_call', 'tool_result') OR payload_type = 'tool_use') > 0, 'tool_calling', 'chat') AS mode,
argMin(event_uid, tuple(event_time, event_order, event_uid)) AS first_event_uid,
argMax(event_uid, tuple(event_time, event_order, event_uid)) AS last_event_uid,
argMax(actor_role, tuple(event_time, event_order, event_uid)) AS last_actor_role,
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''),
tuple(event_ts, event_uid), event_class = 'session_meta'
OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))), '') AS latest_metadata_title,
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''),
tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_metadata_name,
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'summary'), ''),
tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_metadata_summary,
ifNull(argMaxIf(coalesce(nullIf(JSONExtractString(payload_json, 'title'), ''), nullIf(JSONExtractString(payload_json, 'name'), ''), nullIf(JSONExtractString(payload_json, 'summary'), '')),
tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_session_meta_title,
ifNull(argMaxIf(coalesce(nullIf(JSONExtractString(payload_json, 'summary'), ''), nullIf(JSONExtractString(payload_json, 'title'), ''), nullIf(JSONExtractString(payload_json, 'name'), '')),
tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_session_meta_summary,
ifNull(argMinIf(nullIf(trimBoth(replaceRegexpOne(arrayElement(splitByChar('/', replaceAll(source_file, '\\', '/')), -1), '[.]jsonl$', '')), ''),
tuple(event_ts, event_uid), source_name = 'omp' AND notEmpty(session_id)
AND endsWith(source_file, '.jsonl')
AND NOT endsWith(source_file, concat(session_id, '.jsonl'))), '') AS omp_dispatch_title,
ifNull(argMax(nullIf(source_name, ''), tuple(event_ts, event_uid)), '') AS source,
ifNull(argMax(nullIf(harness, ''), tuple(event_ts, event_uid)), '') AS harness,
ifNull(argMax(nullIf(inference_provider, ''), tuple(event_ts, event_uid)), '') AS inference_provider,
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS session_slug,
ifNull(argMinIf(cwd, tuple(event_ts, event_uid), cwd != ''), '') AS origin_cwd
FROM enriched AS projected
GROUP BY session_id
HAVING source_revision = 42
),
current_dirty AS (
SELECT if(count() = 0, toUInt64(0), toUInt64(max(dirty.dirty_revision))) AS dirty_revision
FROM `moraine`.mcp_open_dirty_sessions AS dirty FINAL
WHERE dirty.session_id = 'session-a'
),
terminal AS (
SELECT
session_id, argMax(completed, turn_seq) AS completed, argMax(terminal_event_uid, turn_seq) AS terminal_event_uid
FROM `moraine`.mcp_open_turns FINAL
WHERE session_id = 'session-a' AND slot = 1 AND generation = 1000 AND turn_seq > 0
GROUP BY session_id
)
SELECT
h.session_id, 'append:session-a:1000', 1, 1000, h.source_revision, 7,
h.first_event_time, h.last_event_time, h.total_turns, h.total_events,
h.user_messages, h.assistant_messages, h.tool_calls, h.tool_results, h.mode,
h.first_event_uid, h.last_event_uid, h.last_actor_role,
if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.latest_metadata_summary, ''), nullIf(h.omp_dispatch_title, ''), ''), coalesce(nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), '')), h.source,
h.harness, h.inference_provider, h.session_slug,
if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_summary, ''), nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.omp_dispatch_title, ''), ''), coalesce(nullIf(h.latest_metadata_summary, ''), nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), '')),
if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.latest_metadata_summary, ''), nullIf(h.omp_dispatch_title, ''), ''), h.latest_session_meta_title),
if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_summary, ''), nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.omp_dispatch_title, ''), ''), h.latest_session_meta_summary),
ifNull(t.completed, 0), ifNull(t.terminal_event_uid, ''), h.origin_cwd,
toUInt8(0), [tuple('host-a', 'codex', '/sessions/a.jsonl', toUInt32(3), toUInt64(10)), tuple('host-b', 'codex', '/sessions/b.jsonl', toUInt32(7), toUInt64(11))], '45bd553c7753b4750173c77d7cae03d534b1ff0f9a5a2c6dd3cd9858dca98a6c', 1000,
'pub-1', 'op-1'
FROM header AS h
CROSS JOIN current_dirty AS d
LEFT JOIN terminal AS t ON t.session_id = h.session_id
WHERE d.dirty_revision = 7