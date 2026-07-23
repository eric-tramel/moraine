INSERT INTO `moraine`.mcp_open_turns
(session_id, slot, candidate_generation, generation, turn_seq, turn_id, started_at, ended_at,
total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,
user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,
user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,
final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,
tools_called, normalized_event_types, completed, terminal_event_uid,
first_event_uid, first_event_order, first_event_time, first_event_type,
last_event_uid, last_event_order, last_event_time, last_event_type,
previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,
next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json)
WITH
plans AS (
SELECT session_id, candidate_publication_id, slot, candidate_generation, source_revision,
dirty_revision, required_source_heads, required_heads_fingerprint
FROM `moraine`.mcp_open_backfill_plans FINAL
WHERE session_id IN ['session-a', 'session-b'] AND phase = 1
),
canonical AS (
SELECT
e.ingested_at, e.event_uid, e.session_id, e.source_host, e.source_name, e.harness,
e.inference_provider, e.source_file, e.source_generation, e.source_line_no,
e.source_offset, e.source_ref, e.record_ts, e.event_ts,
e.event_kind AS event_class, e.actor_kind AS actor_role, e.payload_type,
e.turn_index, toString(e.turn_index) AS turn_id, e.item_id,
e.tool_call_id AS call_id, e.tool_name AS name,
if(e.tool_phase != '', e.tool_phase, e.op_status) AS phase,
e.text_content, e.payload_json, e.token_usage_json, e.endpoint_kind,
e.token_usage_buckets, e.token_usage_native_units, e.cwd, e.event_version,
p.candidate_publication_id, p.slot AS candidate_slot,
p.candidate_generation, p.source_revision AS planned_source_revision,
p.dirty_revision, p.required_source_heads, p.required_heads_fingerprint
FROM `moraine`.events AS e FINAL
INNER JOIN plans AS p ON p.session_id = e.session_id
PREWHERE e.session_id IN ['session-a', 'session-b']
WHERE arrayExists(head ->
e.source_host = tupleElement(head, 1)
AND e.source_name = tupleElement(head, 2)
AND e.source_file = tupleElement(head, 3)
AND e.source_generation = tupleElement(head, 4),
p.required_source_heads)
),
ordered AS (
SELECT canonical.*,
ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS event_time,
row_number() OVER canonical_window AS event_order,
if(toUInt32(turn_index) > 0, toUInt32(turn_index), greatest(toUInt32(1), toUInt32(sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER canonical_rows))) AS turn_seq,
toUInt64(planned_source_revision) AS source_revision
FROM canonical

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
turn_rows AS (
SELECT
session_id, any(candidate_slot) AS candidate_slot, any(candidate_generation) AS candidate_generation,
turn_seq, anyIf(turn_id, turn_id != '') AS turn_id,
min(event_time) AS started_at, max(event_time) AS ended_at, count() AS total_events,
countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,
countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,
countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,
countIf(event_class = 'reasoning') AS reasoning_items,
argMinIf(summary_source, tuple(event_order, event_uid), (lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))) AS user_input_summary_source,
argMaxIf(summary_source, tuple(event_order, event_uid), ((lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))) AND lowerUTF8(phase) != 'commentary')) AS final_response_summary_source,
argMinIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), (lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))) AS user_input_summary_is_payload,
argMaxIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), ((lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))) AND lowerUTF8(phase) != 'commentary')) AS final_response_summary_is_payload,
argMinIf(event_uid, tuple(event_order, event_uid), (lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))) AS user_input_event_uid,
argMinIf(event_order, tuple(event_order, event_uid), (lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))) AS user_input_event_order,
argMinIf(event_time, tuple(event_order, event_uid), (lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))) AS user_input_event_time,
argMinIf(event_type, tuple(event_order, event_uid), (lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))) AS user_input_event_type,
argMaxIf(event_uid, tuple(event_order, event_uid), ((lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))) AND lowerUTF8(phase) != 'commentary')) AS final_response_event_uid,
argMaxIf(event_order, tuple(event_order, event_uid), ((lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))) AND lowerUTF8(phase) != 'commentary')) AS final_response_event_order,
argMaxIf(event_time, tuple(event_order, event_uid), ((lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))) AND lowerUTF8(phase) != 'commentary')) AS final_response_event_time,
argMaxIf(event_type, tuple(event_order, event_uid), ((lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))) AND lowerUTF8(phase) != 'commentary')) AS final_response_event_type,
arrayDistinct(arrayMap(x -> x.2, arraySort(groupArrayIf(tuple(event_order, tool_label), event_type = 'tool_call' AND tool_label != '')))) AS tools_called,
arrayDistinct(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_type))))) AS normalized_event_types,
argMaxIf(toUInt8(payload_type = 'task_complete'), tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS completed,
argMaxIf(event_uid, tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS terminal_event_uid,
argMin(event_uid, tuple(event_order, event_uid)) AS first_event_uid,
argMin(event_order, tuple(event_order, event_uid)) AS first_event_order,
argMin(event_time, tuple(event_order, event_uid)) AS first_event_time,
argMin(event_type, tuple(event_order, event_uid)) AS first_event_type,
argMax(event_uid, tuple(event_order, event_uid)) AS last_event_uid,
argMax(event_order, tuple(event_order, event_uid)) AS last_event_order,
argMax(event_time, tuple(event_order, event_uid)) AS last_event_time,
argMax(event_type, tuple(event_order, event_uid)) AS last_event_type,
toJSONString(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_summary))))) AS event_summaries_json
FROM summarized
GROUP BY session_id, turn_seq
),
turn_neighbors AS (
SELECT *,
lagInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS previous_turn_seq,
lagInFrame(turn_id, 1, '') OVER turn_window AS previous_turn_id,
lagInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_started_at,
lagInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_ended_at,
leadInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS next_turn_seq,
leadInFrame(turn_id, 1, '') OVER turn_window AS next_turn_id,
leadInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_started_at,
leadInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_ended_at
FROM turn_rows
WINDOW turn_window AS (PARTITION BY session_id ORDER BY turn_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT
session_id, candidate_slot, candidate_generation, candidate_generation, turn_seq, turn_id, started_at, ended_at,
total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,
user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,
user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,
final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,
tools_called, normalized_event_types, completed, terminal_event_uid,
first_event_uid, first_event_order, first_event_time, first_event_type,
last_event_uid, last_event_order, last_event_time, last_event_type,
previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,
next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json
FROM turn_neighbors AS projected
         LEFT ANTI JOIN (
           SELECT session_id, slot, candidate_generation, turn_seq
           FROM `moraine`.mcp_open_turns
           PREWHERE session_id IN ['session-a', 'session-b']
         ) AS existing
           ON existing.session_id = projected.session_id
          AND existing.slot = projected.candidate_slot
          AND existing.candidate_generation = projected.candidate_generation
          AND existing.turn_seq = projected.turn_seq