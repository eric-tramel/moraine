use serde_json::{json, Value};

fn nav_row(
    session_id: &str,
    event_uid: &str,
    index: u64,
    turn_index: u32,
    actor_kind: &str,
    event_kind: &str,
    payload_type: &str,
    tool_name: &str,
) -> Value {
    let second = index.min(59);
    let event_time = format!("2026-02-01 10:00:{second:02}.000");
    json!({
        "session_id": session_id,
        "event_uid": event_uid,
        "event_version": 100_u64 + index,
        "sort_time": event_time,
        "source_file": format!("/tmp/{session_id}.jsonl"),
        "source_generation": 1_u32,
        "source_offset": index,
        "source_line_no": index,
        "event_time": event_time,
        "event_unix_ms": 1_769_940_000_000_i64 + (index as i64 * 1_000),
        "event_kind": event_kind,
        "actor_kind": actor_kind,
        "payload_type": payload_type,
        "turn_index": turn_index,
        "tool_call_id": if tool_name.is_empty() { "" } else { "call-open" },
        "tool_name": tool_name,
        "phase": "",
        "item_id": "",
        "harness": "codex",
        "inference_provider": "openai",
        "source_name": if session_id == "sess-open" { "codex-source" } else { "fixture" },
        "is_user_message": u8::from(actor_kind == "user" && event_kind == "message"),
        "is_metadata_bearing": u8::from(index == 1)
    })
}

pub(crate) fn navigation_rows(session_id: &str) -> Vec<Value> {
    let specs: Vec<(&str, u32, &str, &str, &str, &str)> = match session_id {
        "sess-open" => vec![
            ("evt-open-1", 1, "user", "message", "text", ""),
            (
                "evt-open-2",
                1,
                "assistant",
                "tool_call",
                "tool_use",
                "search_repo",
            ),
            (
                "evt-open-3",
                1,
                "tool",
                "tool_result",
                "tool_result",
                "search_repo",
            ),
            ("evt-open-4", 1, "assistant", "message", "text", ""),
            ("evt-open-5", 1, "system", "runtime", "task_complete", ""),
            ("evt-open-6", 2, "user", "message", "text", ""),
            ("evt-open-7", 2, "assistant", "message", "text", ""),
            ("evt-open-8", 2, "system", "runtime", "task_complete", ""),
        ],
        "sess-incomplete" => vec![
            ("evt-inc-0", 1, "user", "message", "text", ""),
            ("evt-inc-1", 1, "assistant", "message", "text", ""),
            ("evt-inc-2", 2, "user", "message", "text", ""),
            (
                "evt-inc-3",
                2,
                "assistant",
                "tool_call",
                "tool_use",
                "inspect",
            ),
            (
                "evt-inc-4",
                2,
                "tool",
                "tool_result",
                "tool_result",
                "inspect",
            ),
        ],
        "sess-event" => vec![
            ("evt-event-1", 1, "user", "message", "text", ""),
            ("evt-open-full", 1, "assistant", "message", "text", ""),
            ("evt-event-3", 1, "system", "runtime", "task_complete", ""),
            ("evt-event-4", 2, "assistant", "message", "text", ""),
        ],
        "sess-out-of-scope" => vec![("evt-out-of-scope", 1, "user", "message", "text", "")],
        _ => Vec::new(),
    };
    specs
        .into_iter()
        .enumerate()
        .map(|(offset, (uid, turn, actor, kind, payload, tool))| {
            nav_row(
                session_id,
                uid,
                offset as u64 + 1,
                turn,
                actor,
                kind,
                payload,
                tool,
            )
        })
        .collect()
}

pub(crate) fn hydrated_rows(session_id: &str) -> Vec<Value> {
    navigation_rows(session_id)
        .into_iter()
        .map(|nav| {
            let uid = nav["event_uid"].as_str().unwrap_or_default();
            let text = match uid {
                "evt-open-1" => "How should repository open models work?",
                "evt-open-4" => "First answer with repository context.",
                "evt-open-6" => "Continue.",
                "evt-open-7" => "Done.",
                "evt-inc-0" => "Earlier turn.",
                "evt-inc-1" => "Earlier answer.",
                "evt-inc-2" => "Run the incomplete workflow.",
                "evt-inc-4" => "inspection output",
                "evt-event-1" => "question before full event",
                "evt-open-full" => "This is the full available event content that must not be clipped by the repository open model.",
                "evt-event-4" => "next turn",
                _ => "",
            };
            let payload = match uid {
                "evt-open-1" => "{\"title\":\"Open model session\",\"slug\":\"open-model-session\"}",
                "evt-open-full" => "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}",
                _ => "{}",
            };
            json!({
                "event_uid": uid,
                "source_ref": "",
                "text_content": text,
                "payload_json": payload,
                "token_usage_json": "",
                "endpoint_kind": "",
                "token_usage_buckets": {},
                "token_usage_native_units": {}
            })
        })
        .collect()
}
