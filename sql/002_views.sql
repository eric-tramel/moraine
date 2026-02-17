DROP VIEW IF EXISTS moraine.v_session_summary;
DROP VIEW IF EXISTS moraine.v_turn_summary;
DROP VIEW IF EXISTS moraine.v_conversation_trace;
DROP VIEW IF EXISTS moraine.v_all_events;

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
  event_version
FROM moraine.events;

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
    ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_file, source_generation, source_offset, source_line_no, event_uid
  ) AS event_order,
  if(
    toUInt32OrZero(turn_id) > 0,
    toUInt32OrZero(turn_id),
    greatest(
      toUInt32(1),
      toUInt32(
        sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER (
          PARTITION BY session_id
          ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_file, source_generation, source_offset, source_line_no, event_uid
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
  token_usage_json
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
