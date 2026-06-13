//! Integration test for the Hermes live-session (`session_json`) ingest path.
//!
//! Unlike the ShareGPT trajectory fixture (`hermes_fixture.rs`), this test
//! drives `normalize_record` with the synthetic `session_meta` /
//! `session_message` records the dispatcher builds when it reads a Hermes
//! session file from `~/.hermes/sessions/*.json`.

use std::path::PathBuf;

use moraine_ingest_core::normalize::normalize_record;
use serde_json::{json, Value};

fn fixture_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .join("..")
        .join("..")
        .join("fixtures")
        .join("hermes")
        .join("sessions")
        .join("session_20260418_142200_live01.json")
}

fn load_fixture() -> Value {
    let body = std::fs::read_to_string(fixture_path()).expect("read hermes session fixture");
    serde_json::from_str(&body).expect("parse hermes session fixture")
}

fn infer_vendor_from_base_url(base_url: &str) -> &'static str {
    let lower = base_url.to_ascii_lowercase();
    if lower.contains("anthropic.com") {
        "anthropic"
    } else if lower.contains("openai.com") {
        "openai"
    } else {
        ""
    }
}

fn compose_model(model: &str, base_url: &str) -> String {
    if model.contains('/') || model.is_empty() {
        return model.to_string();
    }
    let vendor = infer_vendor_from_base_url(base_url);
    if vendor.is_empty() {
        model.to_string()
    } else {
        format!("{vendor}/{model}")
    }
}

fn build_session_meta_record(session_doc: &Value) -> Value {
    json!({
        "type": "session_meta",
        "timestamp": session_doc.get("session_start").cloned().unwrap_or(Value::Null),
        "session_id": session_doc.get("session_id").cloned().unwrap_or(Value::Null),
        "model": compose_model(
            session_doc.get("model").and_then(Value::as_str).unwrap_or(""),
            session_doc.get("base_url").and_then(Value::as_str).unwrap_or(""),
        ),
        "base_url": session_doc.get("base_url").cloned().unwrap_or(Value::Null),
        "platform": session_doc.get("platform").cloned().unwrap_or(Value::Null),
        "session_start": session_doc.get("session_start").cloned().unwrap_or(Value::Null),
        "last_updated": session_doc.get("last_updated").cloned().unwrap_or(Value::Null),
        "system_prompt": session_doc.get("system_prompt").cloned().unwrap_or(Value::Null),
        "tools": session_doc.get("tools").cloned().unwrap_or(Value::Null),
        "message_count": session_doc.get("message_count").cloned().unwrap_or(Value::Null),
    })
}

fn build_session_message_record(session_doc: &Value, message: &Value, message_index: u64) -> Value {
    json!({
        "type": "session_message",
        "timestamp": session_doc.get("last_updated").cloned().unwrap_or(Value::Null),
        "session_id": session_doc.get("session_id").cloned().unwrap_or(Value::Null),
        "model": compose_model(
            session_doc.get("model").and_then(Value::as_str).unwrap_or(""),
            session_doc.get("base_url").and_then(Value::as_str).unwrap_or(""),
        ),
        "base_url": session_doc.get("base_url").cloned().unwrap_or(Value::Null),
        "platform": session_doc.get("platform").cloned().unwrap_or(Value::Null),
        "message_index": message_index,
        "message": message,
    })
}

fn normalize_all(session_doc: &Value) -> Vec<moraine_ingest_core::model::NormalizedRecord> {
    let source_file = fixture_path().to_string_lossy().to_string();
    let messages = session_doc
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::new();
    // First the session_meta pseudo-record (line_no=0).
    let meta = build_session_meta_record(session_doc);
    out.push(
        normalize_record(
            &meta,
            "ci-hermes",
            "hermes",
            &source_file,
            0,
            1,
            0,
            0,
            "",
            "",
            "",
        )
        .expect("session_meta normalizes"),
    );
    for (idx, message) in messages.iter().enumerate() {
        let record = build_session_message_record(session_doc, message, idx as u64);
        out.push(
            normalize_record(
                &record,
                "ci-hermes",
                "hermes",
                &source_file,
                0,
                1,
                (idx + 1) as u64,
                0,
                "",
                "",
                "",
            )
            .expect("session_message normalizes"),
        );
    }
    out
}

#[test]
fn session_meta_emits_a_single_session_meta_event_with_vendor_split() {
    let doc = load_fixture();
    let normalized = normalize_all(&doc);
    let meta = &normalized[0];

    assert!(
        meta.error_rows.is_empty(),
        "no error rows: {:?}",
        meta.error_rows
    );
    assert_eq!(meta.event_rows.len(), 1, "exactly one session_meta event");

    let evt = &meta.event_rows[0];
    assert_eq!(
        evt.get("event_kind").and_then(Value::as_str),
        Some("session_meta")
    );
    assert_eq!(
        evt.get("inference_provider").and_then(Value::as_str),
        Some("anthropic"),
        "vendor inferred from base_url https://api.anthropic.com",
    );
    assert_eq!(
        evt.get("model").and_then(Value::as_str),
        Some("claude-opus-4-6"),
        "model stripped of vendor prefix at event level",
    );
    assert_eq!(
        evt.get("session_id").and_then(Value::as_str),
        Some("hermes:20260418_142200_live01"),
        "session_id namespaced with `hermes:`",
    );
}

#[test]
fn session_messages_map_openai_chat_roles_to_trace_events() {
    let doc = load_fixture();
    let normalized = normalize_all(&doc);

    // Index 0 is the session_meta pseudo-record; messages start at index 1.
    let user_msg = &normalized[1];
    assert_eq!(user_msg.event_rows.len(), 1);
    let user_evt = &user_msg.event_rows[0];
    assert_eq!(
        user_evt.get("event_kind").and_then(Value::as_str),
        Some("message")
    );
    assert_eq!(
        user_evt.get("actor_kind").and_then(Value::as_str),
        Some("user")
    );
    assert!(user_evt
        .get("text_content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .contains("today's date"));

    // Message index 1: assistant with reasoning + tool_calls → 2 events
    // (reasoning, tool_call). Content is empty so no assistant message event.
    let assistant_msg = &normalized[2];
    assert_eq!(
        assistant_msg.event_rows.len(),
        2,
        "reasoning + tool_call events expected, got {:?}",
        assistant_msg
            .event_rows
            .iter()
            .map(|r| r.get("event_kind").and_then(Value::as_str))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        assistant_msg.event_rows[0]
            .get("event_kind")
            .and_then(Value::as_str),
        Some("reasoning"),
    );
    assert_eq!(
        assistant_msg.event_rows[0]
            .get("payload_type")
            .and_then(Value::as_str),
        Some("reasoning"),
    );
    assert_eq!(
        assistant_msg.event_rows[0].get("content_types"),
        Some(&json!(["reasoning"])),
    );
    assert_eq!(
        assistant_msg.event_rows[0]
            .get("has_reasoning")
            .and_then(Value::as_u64),
        Some(1),
    );
    let tool_call_evt = &assistant_msg.event_rows[1];
    assert_eq!(
        tool_call_evt.get("event_kind").and_then(Value::as_str),
        Some("tool_call")
    );
    assert_eq!(
        tool_call_evt.get("tool_name").and_then(Value::as_str),
        Some("shell")
    );
    assert_eq!(
        tool_call_evt.get("tool_call_id").and_then(Value::as_str),
        Some("call_date_001"),
    );
    assert_eq!(
        assistant_msg.tool_rows.len(),
        1,
        "tool_io row emitted for the request phase",
    );
    assert_eq!(
        assistant_msg.tool_rows[0]
            .get("tool_phase")
            .and_then(Value::as_str),
        Some("request"),
    );

    // Message index 2: tool result
    let tool_msg = &normalized[3];
    assert_eq!(tool_msg.event_rows.len(), 1);
    let tool_evt = &tool_msg.event_rows[0];
    assert_eq!(
        tool_evt.get("event_kind").and_then(Value::as_str),
        Some("tool_result")
    );
    assert_eq!(
        tool_evt.get("tool_call_id").and_then(Value::as_str),
        Some("call_date_001")
    );
    assert_eq!(tool_msg.tool_rows.len(), 1);
    assert_eq!(
        tool_msg.tool_rows[0]
            .get("tool_phase")
            .and_then(Value::as_str),
        Some("response"),
    );

    // Message index 3: assistant with content + finish_reason=stop
    let final_msg = &normalized[4];
    assert_eq!(final_msg.event_rows.len(), 1);
    let final_evt = &final_msg.event_rows[0];
    assert_eq!(
        final_evt.get("event_kind").and_then(Value::as_str),
        Some("message")
    );
    assert_eq!(
        final_evt.get("actor_kind").and_then(Value::as_str),
        Some("assistant")
    );
    assert_eq!(
        final_evt.get("op_status").and_then(Value::as_str),
        Some("stop")
    );
    assert!(final_evt
        .get("text_content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .contains("April 18, 2026"));
}

#[test]
fn session_messages_share_a_stable_session_id_prefix() {
    let doc = load_fixture();
    let normalized = normalize_all(&doc);
    let expected = "hermes:20260418_142200_live01";
    for (i, rec) in normalized.iter().enumerate() {
        for row in &rec.event_rows {
            assert_eq!(
                row.get("session_id").and_then(Value::as_str),
                Some(expected),
                "record {i} session_id must be stable across messages",
            );
        }
    }
}

#[test]
fn event_uids_are_stable_across_repeated_reads() {
    // Re-normalizing the same session file must produce identical event_uids:
    // this is the idempotency anchor that lets the ClickHouse ReplacingMergeTree
    // dedupe re-emits when the atomic-rename save cycle causes us to re-read
    // already-seen messages.
    let doc = load_fixture();
    let first = normalize_all(&doc);
    let second = normalize_all(&doc);
    assert_eq!(first.len(), second.len());
    for (i, (a, b)) in first.iter().zip(second.iter()).enumerate() {
        let uids_a: Vec<&str> = a
            .event_rows
            .iter()
            .map(|r| r.get("event_uid").and_then(Value::as_str).unwrap_or(""))
            .collect();
        let uids_b: Vec<&str> = b
            .event_rows
            .iter()
            .map(|r| r.get("event_uid").and_then(Value::as_str).unwrap_or(""))
            .collect();
        assert_eq!(
            uids_a, uids_b,
            "record {i} event_uids must match across repeated reads",
        );
    }
}

#[test]
fn growth_from_two_to_four_messages_adds_only_new_events() {
    // Mirror the live-session case: the first read sees only the first 2
    // messages, the second read sees all 4. The event_uids for the first two
    // messages must match across reads.
    let doc = load_fixture();
    let mut small = doc.clone();
    let messages = small
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .unwrap();
    messages.truncate(2);
    small["message_count"] = json!(2);

    let small_norm = normalize_all(&small);
    let full_norm = normalize_all(&doc);

    // session_meta + first 2 messages = 3 records in the small version.
    assert_eq!(small_norm.len(), 3);
    assert_eq!(full_norm.len(), 5);

    // session_meta + user message event_uids should match (messages 0..2 in the
    // session file are the same bytes in both reads).
    for idx in 1..=2 {
        let small_uids: Vec<&str> = small_norm[idx]
            .event_rows
            .iter()
            .map(|r| r.get("event_uid").and_then(Value::as_str).unwrap_or(""))
            .collect();
        let full_uids: Vec<&str> = full_norm[idx]
            .event_rows
            .iter()
            .map(|r| r.get("event_uid").and_then(Value::as_str).unwrap_or(""))
            .collect();
        assert_eq!(
            small_uids,
            full_uids,
            "message index {} event_uids diverged across file growth",
            idx - 1,
        );
    }
}
