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
)