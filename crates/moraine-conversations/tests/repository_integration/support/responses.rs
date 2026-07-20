use serde_json::json;

use super::mock_clickhouse::ScriptedResponse;

pub(crate) fn json_each_row(rows: serde_json::Value) -> String {
    match rows {
        serde_json::Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                out.push_str(&item.to_string());
                out.push('\n');
            }
            out
        }
        value => format!("{value}\n"),
    }
}

pub(crate) fn json_envelope(rows: serde_json::Value) -> String {
    json!({ "data": rows }).to_string()
}

pub(crate) fn session_analytics_summary_row() -> serde_json::Value {
    json!({
        "session_id": "analytics-session",
        "first_event_time": "2026-06-01 10:00:00.000",
        "first_event_unix_ms": 1780308000000_i64,
        "last_event_time": "2026-06-01 10:05:00.000",
        "last_event_unix_ms": 1780308300000_i64,
        "total_turns": 2_u32,
        "total_events": 6_u64,
        "user_messages": 1_u64,
        "assistant_messages": 2_u64,
        "tool_calls": 1_u64,
        "tool_results": 1_u64,
        "mode": "tool_calling",
        "session_slug": "analytics-slug",
        "session_summary": "Analytics fixture session"
    })
}

pub(crate) fn session_analytics_turn_rows() -> serde_json::Value {
    json!([
        {
            "session_id": "analytics-session",
            "turn_seq": 1_u32,
            "turn_id": "turn-one",
            "started_at": "2026-06-01 10:00:00.000",
            "started_at_unix_ms": 1780308000000_i64,
            "ended_at": "2026-06-01 10:04:00.000",
            "ended_at_unix_ms": 1780308240000_i64,
            "total_events": 5_u64,
            "user_messages": 1_u64,
            "assistant_messages": 1_u64,
            "tool_calls": 1_u64,
            "tool_results": 1_u64,
            "reasoning_items": 1_u64
        },
        {
            "session_id": "analytics-session",
            "turn_seq": 2_u32,
            "turn_id": "turn-two",
            "started_at": "2026-06-01 10:05:00.000",
            "started_at_unix_ms": 1780308300000_i64,
            "ended_at": "2026-06-01 10:05:00.000",
            "ended_at_unix_ms": 1780308300000_i64,
            "total_events": 1_u64,
            "user_messages": 0_u64,
            "assistant_messages": 1_u64,
            "tool_calls": 0_u64,
            "tool_results": 0_u64,
            "reasoning_items": 0_u64
        }
    ])
}

pub(crate) fn session_analytics_event_rows() -> serde_json::Value {
    json!([
        {
            "session_id": "analytics-session",
            "event_order": 1_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000000_i64,
            "event_class": "message",
            "actor_role": "user",
            "payload_type": "text",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 0_u32,
            "model": " GPT-X ",
            "endpoint_kind": "",
            "input_tokens": 5_u32,
            "output_tokens": 2_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "first user fallback",
            "text_content": " ",
            "tool_args_json": "",
            "harness": "codex",
            "source_name": "codex-jsonl",
            "trace_id": "trace-454"
        },
        {
            "session_id": "analytics-session",
            "event_order": 2_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000100_i64,
            "event_class": "message",
            "actor_role": "assistant",
            "payload_type": "text",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 42_u32,
            "model": "GPT-X",
            "endpoint_kind": "generation",
            "input_tokens": 99_u32,
            "output_tokens": 99_u32,
            "cache_read_tokens": 99_u32,
            "cache_write_tokens": 99_u32,
            "token_usage_buckets": { "reasoning": 7_u64 },
            "token_usage_native_units": { "usd": 0.5, "zero": 0.0 },
            "text_preview": "clipped answer",
            "text_content": "full answer",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 3_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000200_i64,
            "event_class": "tool_call",
            "actor_role": "assistant",
            "payload_type": "tool_use",
            "call_id": "call-read",
            "tool_name": "Read",
            "tool_error": 1_u8,
            "latency_ms": 17_u32,
            "model": "GPT-X",
            "endpoint_kind": "",
            "input_tokens": 0_u32,
            "output_tokens": 0_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "",
            "tool_args_json": "{\"path\":\"src/lib.rs\"}",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 4_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000800_i64,
            "event_class": "tool_result",
            "actor_role": "tool",
            "payload_type": "tool_result",
            "call_id": "call-read",
            "tool_name": "Read",
            "tool_error": 1_u8,
            "latency_ms": 999_u32,
            "model": "",
            "endpoint_kind": "",
            "input_tokens": 0_u32,
            "output_tokens": 0_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "read failed",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 5_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000900_i64,
            "event_class": "reasoning",
            "actor_role": "assistant",
            "payload_type": "thinking",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 0_u32,
            "model": "GPT-X",
            "endpoint_kind": "",
            "input_tokens": 0_u32,
            "output_tokens": 0_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "consider the result",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 6_u64,
            "turn_seq": 2_u32,
            "event_unix_ms": 1780308300000_i64,
            "event_class": "message",
            "actor_role": "assistant",
            "payload_type": "text",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 0_u32,
            "model": "Other",
            "endpoint_kind": "generation",
            "input_tokens": 0_u32,
            "output_tokens": 3_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "second turn",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        }
    ])
}

pub(crate) fn analytics_responses(
    window_literal: &'static str,
    interval_literal: &'static str,
    tokens: serde_json::Value,
    turns: serde_json::Value,
    concurrency: serde_json::Value,
) -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::rows(
            &[
                "toInt64(toUnixTimestamp(now())) AS database_now_unix",
                window_literal,
                "FROM `moraine`.`v_published_source_generation_history` AS history",
                "AND notEmpty(trimBoth(e.model))",
                "FORMAT JSONEachRow",
            ],
            json!([{
                "scan_from_unix": 100_000_u64,
                "scan_to_unix": 200_000_u64,
                "display_to_unix": 200_000_u64
            }]),
        ),
        ScriptedResponse::rows(
            &[
                interval_literal,
                "FROM `moraine`.`v_published_source_generation_history` AS history",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= 100000",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) <= 200000",
                "ARRAY JOIN mapKeys(e.token_usage_buckets)",
                "ORDER BY bucket_unix ASC, model ASC, endpoint_kind ASC, bucket ASC",
                "FORMAT JSONEachRow",
            ],
            tokens,
        ),
        ScriptedResponse::rows(
            &[
                interval_literal,
                "toUInt64(uniqExact(tuple(e.session_id, e.request_id))) AS turns",
                "FROM `moraine`.`v_published_source_generation_history` AS history",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= 100000",
                "ORDER BY bucket_unix ASC, model ASC",
                "FORMAT JSONEachRow",
            ],
            turns,
        ),
        ScriptedResponse::rows(
            &[
                interval_literal,
                "toUInt64(uniqExact(session_stream_key)) AS concurrent_sessions",
                "FROM `moraine`.`v_published_source_generation_history` AS history",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= 100000",
                "ORDER BY bucket_unix ASC",
                "FORMAT JSONEachRow",
            ],
            concurrency,
        ),
    ]
}

pub(crate) fn heartbeat_columns(optional: &[&str]) -> serde_json::Value {
    let mut names = vec![
        "ts",
        "host",
        "service_version",
        "queue_depth",
        "files_active",
        "files_watched",
        "rows_raw_written",
        "rows_events_written",
        "rows_errors_written",
        "flush_latency_ms",
        "append_to_visible_p50_ms",
        "append_to_visible_p95_ms",
        "last_error",
    ];
    names.extend_from_slice(optional);
    serde_json::Value::Array(
        names
            .into_iter()
            .map(|name| json!({ "name": name }))
            .collect(),
    )
}

pub(crate) fn heartbeat_row() -> serde_json::Value {
    json!({
        "ts": "2026-06-01 10:00:00.000",
        "ts_unix_ms": 1780308000000_i64,
        "host": "ingest-host",
        "service_version": "0.17.1",
        "queue_depth": 4_u64,
        "files_active": 2_u32,
        "files_watched": 9_u32,
        "rows_raw_written": 100_u64,
        "rows_events_written": 90_u64,
        "rows_errors_written": 1_u64,
        "flush_latency_ms": 12_u32,
        "append_to_visible_p50_ms": 20_u32,
        "append_to_visible_p95_ms": 40_u32,
        "last_error": "",
        "watcher_backend": null,
        "watcher_error_count": null,
        "watcher_reset_count": null,
        "watcher_last_reset_unix_ms": null,
        "backend_sinks": null
    })
}

pub(crate) fn turn_summary_row(
    session_id: &str,
    turn_seq: u32,
    total_events: u64,
    user_messages: u64,
    assistant_messages: u64,
    tool_calls: u64,
    tool_results: u64,
    reasoning_items: u64,
) -> serde_json::Value {
    json!({
        "session_id": session_id,
        "turn_seq": turn_seq,
        "turn_id": turn_seq.to_string(),
        "started_at": format!("2026-02-01 10:{:02}:00", turn_seq),
        "started_at_unix_ms": 1769940000000_i64 + i64::from(turn_seq) * 60_000,
        "ended_at": format!("2026-02-01 10:{:02}:30", turn_seq),
        "ended_at_unix_ms": 1769940030000_i64 + i64::from(turn_seq) * 60_000,
        "total_events": total_events,
        "user_messages": user_messages,
        "assistant_messages": assistant_messages,
        "tool_calls": tool_calls,
        "tool_results": tool_results,
        "reasoning_items": reasoning_items,
    })
}

pub(crate) fn trace_event_row(
    session_id: &str,
    event_uid: &str,
    event_order: u64,
    turn_seq: u32,
    actor_role: &str,
    event_class: &str,
    payload_type: &str,
    text_content: &str,
    payload_json: &str,
    name: &str,
    call_id: &str,
) -> serde_json::Value {
    json!({
        "session_id": session_id,
        "event_uid": event_uid,
        "event_order": event_order,
        "turn_seq": turn_seq,
        "turn_index": turn_seq,
        "event_time": format!("2026-02-01 10:00:{:02}", event_order),
        "event_unix_ms": 1769940000000_i64 + (event_order as i64) * 1_000,
        "source_name": "codex",
        "actor_role": actor_role,
        "event_class": event_class,
        "payload_type": payload_type,
        "call_id": call_id,
        "name": name,
        "phase": "",
        "item_id": format!("item-{event_uid}"),
        "source_ref": format!("/tmp/{session_id}.jsonl:1:{event_order}"),
        "text_content": text_content,
        "payload_json": payload_json,
        "token_usage_json": "{}"
    })
}
