//! Integration test that exercises `normalize_record` end-to-end against a
//! real Hermes ShareGPT trajectory fixture on disk. Unlike the in-module unit
//! test, this test consumes the fixture through the public crate surface the
//! ingestor uses at runtime.

use std::path::PathBuf;

use moraine_ingest_core::normalize::normalize_record;
use serde_json::Value;

fn fixture_path() -> PathBuf {
    // `CARGO_MANIFEST_DIR` resolves to `crates/moraine-ingest-core`. The
    // fixture lives at the repo root so the e2e CI stack can optionally
    // reuse it (same convention as `fixtures/codex/...` and
    // `fixtures/claude/...`).
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .join("..")
        .join("..")
        .join("fixtures")
        .join("hermes")
        .join("trajectories")
        .join("001.jsonl")
}

fn normalize_fixture_lines() -> Vec<moraine_ingest_core::model::NormalizedRecord> {
    let path = fixture_path();
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read hermes fixture at {}: {e}", path.display()));

    let mut offset: u64 = 0;
    let mut out = Vec::new();
    for (idx, raw_line) in body.split_inclusive('\n').enumerate() {
        let line_no: u64 = (idx as u64) + 1;
        let line_bytes = raw_line.len() as u64;
        let offset_this = offset;
        offset += line_bytes;

        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let record: Value = serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!("fixture line {line_no} is not valid JSON: {e}"));

        let normalized = normalize_record(
            &record,
            "ci-hermes",
            "hermes",
            path.to_str().unwrap(),
            /* source_inode */ 42,
            /* source_generation */ 1,
            line_no,
            offset_this,
            /* session_hint */ "",
            /* model_hint */ "",
        )
        .expect("hermes fixture line should normalize");
        out.push(normalized);
    }
    out
}

#[test]
fn hermes_fixture_round_trips_vendor_model_split_and_tool_io() {
    let normalized = normalize_fixture_lines();
    assert_eq!(
        normalized.len(),
        1,
        "fixture should contain exactly one trajectory line"
    );
    let rec = &normalized[0];

    assert!(
        rec.error_rows.is_empty(),
        "no error rows expected: {:?}",
        rec.error_rows
    );

    // raw_row carries the parsed harness + vendor split.
    assert_eq!(
        rec.raw_row.get("harness").and_then(Value::as_str),
        Some("hermes"),
        "raw_row harness must be `hermes` (not `claude-code` or `anthropic`)",
    );
    assert_eq!(
        rec.raw_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("anthropic"),
        "raw_row inference_provider must be parsed from `anthropic/...`",
    );
    assert_eq!(
        rec.raw_row.get("top_type").and_then(Value::as_str),
        Some("trajectory"),
    );
    let session_id = rec
        .raw_row
        .get("session_id")
        .and_then(Value::as_str)
        .expect("raw_row session_id");
    assert!(
        session_id.starts_with("hermes:"),
        "hermes trajectories derive a synthetic `hermes:<uid>` session id; got {session_id}",
    );

    // Every event row carries harness=hermes and inference_provider=anthropic.
    assert!(!rec.event_rows.is_empty(), "trajectory should emit events");
    for (i, row) in rec.event_rows.iter().enumerate() {
        assert_eq!(
            row.get("harness").and_then(Value::as_str),
            Some("hermes"),
            "event row {i} missing correct harness",
        );
        assert_eq!(
            row.get("inference_provider").and_then(Value::as_str),
            Some("anthropic"),
            "event row {i} missing parsed inference_provider",
        );
    }

    // Verbatim-model assertion: the final assistant message must carry
    // `claude-sonnet-4.6`, NOT `anthropic/claude-sonnet-4.6` (vendor is split
    // off) and NOT some dot-mangled version like `claude-sonnet-4-6`.
    let final_message = rec
        .event_rows
        .iter()
        .find(|row| {
            row.get("text_content")
                .and_then(Value::as_str)
                .map(|t| t.contains("rainy in Boston"))
                .unwrap_or(false)
        })
        .expect("final assistant reply");
    assert_eq!(
        final_message.get("model").and_then(Value::as_str),
        Some("claude-sonnet-4.6"),
        "model must be the verbatim right-hand side of vendor/model split",
    );

    // Reasoning (from <think>) uses the canonical reasoning metadata shape.
    let reasoning = rec
        .event_rows
        .iter()
        .find(|row| row.get("event_kind") == Some(&Value::String("reasoning".to_string())))
        .expect("reasoning row");
    assert_eq!(
        reasoning.get("payload_type").and_then(Value::as_str),
        Some("reasoning"),
    );
    assert_eq!(
        reasoning.get("content_types"),
        Some(&serde_json::json!(["reasoning"])),
    );
    assert_eq!(
        reasoning.get("has_reasoning").and_then(Value::as_u64),
        Some(1),
    );

    // tool_call event carries the tool_call_id and name from the embedded JSON.
    let tool_call = rec
        .event_rows
        .iter()
        .find(|row| row.get("event_kind") == Some(&Value::String("tool_call".to_string())))
        .expect("tool_call row");
    assert_eq!(
        tool_call.get("tool_name").and_then(Value::as_str),
        Some("weather"),
    );
    assert_eq!(
        tool_call.get("tool_call_id").and_then(Value::as_str),
        Some("call_abc123"),
    );

    // Matching tool_result row is tied to the same tool_call_id.
    let tool_result = rec
        .event_rows
        .iter()
        .find(|row| {
            row.get("event_kind") == Some(&Value::String("tool_result".to_string()))
                && row.get("tool_call_id") == Some(&Value::String("call_abc123".to_string()))
        })
        .expect("tool_result row");
    assert_eq!(
        tool_result.get("tool_name").and_then(Value::as_str),
        Some("weather"),
    );

    // tool_io rows: one request + one response, both paired to call_abc123
    // and carrying harness=hermes + inference_provider=anthropic.
    assert!(
        !rec.tool_rows.is_empty(),
        "trajectory should emit tool_io rows"
    );
    let request_row = rec
        .tool_rows
        .iter()
        .find(|row| row.get("tool_phase") == Some(&Value::String("request".to_string())))
        .expect("tool_io request row");
    assert_eq!(
        request_row.get("tool_call_id").and_then(Value::as_str),
        Some("call_abc123"),
    );
    assert_eq!(
        request_row.get("harness").and_then(Value::as_str),
        Some("hermes"),
    );
    assert_eq!(
        request_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("anthropic"),
    );

    let response_row = rec
        .tool_rows
        .iter()
        .find(|row| row.get("tool_phase") == Some(&Value::String("response".to_string())))
        .expect("tool_io response row");
    assert_eq!(
        response_row.get("tool_call_id").and_then(Value::as_str),
        Some("call_abc123"),
    );
    assert_eq!(
        response_row.get("harness").and_then(Value::as_str),
        Some("hermes"),
    );
    assert_eq!(
        response_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("anthropic"),
    );

    // Surfaced model_hint should be the verbatim split model, not the full
    // `vendor/model` string.
    assert_eq!(rec.model_hint, "claude-sonnet-4.6");
}

#[test]
fn hermes_vendor_split_keeps_cloud_prefixed_models_as_followup() {
    // Document the current (accepted) behavior for cloud-prefixed vendor
    // strings: we split on the first `/`, so `bedrock/anthropic/...` becomes
    // inference_provider=bedrock, model=anthropic/claude-opus-4-5. Future work
    // can teach the normalizer to re-nest these; flagged in the PR body.
    let record = serde_json::json!({
        "timestamp": "2026-03-30T14:22:31.000000",
        "model": "bedrock/anthropic/claude-opus-4-5",
        "conversations": [
            {"from": "human", "value": "hi"},
            {"from": "gpt", "value": "hello"}
        ]
    });

    let out = normalize_record(
        &record,
        "ci-hermes",
        "hermes",
        "/tmp/hermes/fake.jsonl",
        7,
        1,
        1,
        0,
        "",
        "",
    )
    .expect("normalize");

    assert_eq!(
        out.raw_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("bedrock"),
    );
    let assistant = out
        .event_rows
        .iter()
        .find(|row| row.get("text_content") == Some(&Value::String("hello".to_string())))
        .expect("assistant row");
    assert_eq!(
        assistant.get("model").and_then(Value::as_str),
        Some("anthropic/claude-opus-4-5"),
    );
}

#[test]
fn hermes_vendor_split_handles_model_without_slash() {
    let record = serde_json::json!({
        "timestamp": "2026-03-30T14:22:31.000000",
        "model": "claude-sonnet-4.6",
        "conversations": [
            {"from": "human", "value": "hi"},
            {"from": "gpt", "value": "hello"}
        ]
    });

    let out = normalize_record(
        &record,
        "ci-hermes",
        "hermes",
        "/tmp/hermes/fake.jsonl",
        7,
        1,
        1,
        0,
        "",
        "",
    )
    .expect("normalize");

    assert_eq!(
        out.raw_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some(""),
        "no slash = empty inference_provider",
    );
    let assistant = out
        .event_rows
        .iter()
        .find(|row| row.get("text_content") == Some(&Value::String("hello".to_string())))
        .expect("assistant row");
    assert_eq!(
        assistant.get("model").and_then(Value::as_str),
        Some("claude-sonnet-4.6"),
    );
}
