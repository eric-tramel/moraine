use moraine_ingest_core::normalize::normalize_record;
use serde_json::{json, Value};
use std::collections::HashMap;

fn assert_canonical_reasoning_metadata(row: &Value) {
    let expected_content_types = json!(["reasoning"]);
    assert_eq!(
        row.get("event_kind").and_then(Value::as_str),
        Some("reasoning")
    );
    assert_eq!(
        row.get("payload_type").and_then(Value::as_str),
        Some("reasoning")
    );
    assert_eq!(row.get("content_types"), Some(&expected_content_types));
    assert_eq!(row.get("has_reasoning").and_then(Value::as_u64), Some(1));
}

#[test]
fn codex_tool_call_normalization() {
    let record = json!({
        "timestamp": "2026-02-14T02:28:00.000Z",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "call_id": "call_123",
            "name": "Read",
            "arguments": "{\"path\":\"README.md\"}"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/13/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
        123,
        1,
        42,
        1024,
        "",
        "",
        "",
    )
    .expect("codex tool call should normalize");

    assert_eq!(out.event_rows.len(), 1);
    assert_eq!(out.tool_rows.len(), 1);
    assert!(out.error_rows.is_empty());
    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        row.get("event_kind").unwrap().as_str().unwrap(),
        "tool_call"
    );
    assert_eq!(row.get("tool_name").unwrap().as_str().unwrap(), "Read");
}

#[test]
fn codex_turn_context_promotes_model_and_turn_id() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:42.191Z",
        "type": "turn_context",
        "payload": {
            "turn_id": "019c5f6a-49bd-7920-ac67-1dd8e33b0e95",
            "model": "gpt-5.3-codex"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        1,
        1,
        "",
        "",
        "",
    )
    .expect("codex turn context should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        row.get("payload_type").unwrap().as_str().unwrap(),
        "turn_context"
    );
    assert_eq!(row.get("model").unwrap().as_str().unwrap(), "gpt-5.3-codex");
    assert_eq!(
        row.get("request_id").unwrap().as_str().unwrap(),
        "019c5f6a-49bd-7920-ac67-1dd8e33b0e95"
    );
    assert_eq!(
        row.get("item_id").unwrap().as_str().unwrap(),
        "019c5f6a-49bd-7920-ac67-1dd8e33b0e95"
    );
}

#[test]
fn codex_token_count_promotes_usage_fields() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:50.838Z",
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "last_token_usage": {
                    "input_tokens": 65323,
                    "output_tokens": 445,
                    "cached_input_tokens": 58624,
                    "input_tokens_details": {
                        "image_tokens": 20,
                        "audio_tokens": 3
                    },
                    "output_tokens_details": {
                        "reasoning_tokens": 100
                    }
                }
            },
            "rate_limits": {
                "limit_name": "GPT-5.3-Codex-Spark",
                "limit_id": "codex_bengalfox",
                "plan_type": "pro"
            }
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        2,
        2,
        "",
        "",
        "",
    )
    .expect("codex token count should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        row.get("payload_type").unwrap().as_str().unwrap(),
        "token_count"
    );
    assert_eq!(row.get("input_tokens").unwrap().as_u64().unwrap(), 65323);
    assert_eq!(row.get("output_tokens").unwrap().as_u64().unwrap(), 445);
    assert_eq!(
        row.get("cache_read_tokens").unwrap().as_u64().unwrap(),
        58624
    );
    assert_eq!(
        row.get("endpoint_kind").and_then(Value::as_str),
        Some("generation")
    );
    let buckets = row
        .get("token_usage_buckets")
        .and_then(Value::as_object)
        .expect("canonical token buckets");
    assert_eq!(
        buckets.get("input_text").and_then(Value::as_u64),
        Some(6676)
    );
    assert_eq!(
        buckets.get("input_cache_read").and_then(Value::as_u64),
        Some(58624)
    );
    assert_eq!(buckets.get("input_image").and_then(Value::as_u64), Some(20));
    assert_eq!(buckets.get("input_audio").and_then(Value::as_u64), Some(3));
    assert_eq!(
        buckets.get("output_text").and_then(Value::as_u64),
        Some(345)
    );
    assert_eq!(buckets.get("reasoning").and_then(Value::as_u64), Some(100));
    assert_eq!(
        row.get("model").unwrap().as_str().unwrap(),
        "gpt-5.3-codex-spark"
    );
    assert_eq!(row.get("service_tier").unwrap().as_str().unwrap(), "pro");
    assert!(!row
        .get("token_usage_json")
        .unwrap()
        .as_str()
        .unwrap()
        .is_empty());
}

#[test]
fn codex_token_count_alias_codex_maps_to_xhigh() {
    let record = json!({
        "timestamp": "2026-02-15T04:52:55.538Z",
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "last_token_usage": {
                    "input_tokens": 72636,
                    "output_tokens": 285,
                    "cached_input_tokens": 70784
                }
            },
            "rate_limits": {
                "limit_id": "codex",
                "limit_name": null,
                "plan_type": "pro"
            }
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        4,
        4,
        "",
        "",
        "",
    )
    .expect("codex token count alias should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        row.get("model").unwrap().as_str().unwrap(),
        "gpt-5.3-codex-xhigh"
    );
}

#[test]
fn codex_custom_tool_call_promotes_tool_fields() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:50.838Z",
        "type": "response_item",
        "payload": {
            "type": "custom_tool_call",
            "call_id": "call_abc",
            "name": "apply_patch",
            "status": "completed",
            "input": "*** Begin Patch\n*** End Patch\n"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        3,
        3,
        "",
        "",
        "",
    )
    .expect("codex custom tool call should normalize");

    assert_eq!(out.event_rows.len(), 1);
    assert_eq!(out.tool_rows.len(), 1);
    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        row.get("event_kind").unwrap().as_str().unwrap(),
        "tool_call"
    );
    assert_eq!(
        row.get("tool_call_id").unwrap().as_str().unwrap(),
        "call_abc"
    );
    assert_eq!(
        row.get("tool_name").unwrap().as_str().unwrap(),
        "apply_patch"
    );
    assert_eq!(row.get("op_status").unwrap().as_str().unwrap(), "completed");
}

#[test]
fn codex_reasoning_branches_use_canonical_metadata() {
    let records = [
        json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "response_item",
            "payload": {
                "type": "reasoning",
                "id": "rs_1",
                "summary": [{"type": "summary_text", "text": "think through the request"}]
            }
        }),
        json!({
            "timestamp": "2026-02-15T03:50:51.838Z",
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "top-level reasoning"}]
        }),
        json!({
            "timestamp": "2026-02-15T03:50:52.838Z",
            "type": "compacted",
            "payload": {
                "replacement_history": [
                    {
                        "type": "reasoning",
                        "summary": [{"type": "summary_text", "text": "compacted reasoning"}]
                    }
                ]
            }
        }),
    ];

    for (idx, record) in records.iter().enumerate() {
        let out = normalize_record(
            record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            idx as u64 + 20,
            idx as u64 + 20,
            "",
            "",
            "",
        )
        .expect("codex reasoning record should normalize");

        assert!(
            out.error_rows.is_empty(),
            "unexpected errors for case {idx}: {:?}",
            out.error_rows
        );
        let reasoning = out
            .event_rows
            .iter()
            .find(|row| row.get("event_kind").and_then(Value::as_str) == Some("reasoning"))
            .expect("reasoning row");
        assert_canonical_reasoning_metadata(reasoning);
    }
}

#[test]
fn claude_tool_use_and_result_blocks() {
    let record = json!({
        "type": "assistant",
        "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
        "uuid": "assistant-1",
        "parentUuid": "user-1",
        "requestId": "req-1",
        "timestamp": "2026-01-19T15:58:41.421Z",
        "message": {
            "model": "claude-opus-4-5-20251101",
            "role": "assistant",
            "usage": {
                "input_tokens": 9,
                "output_tokens": 5,
                "cache_creation_input_tokens": 19630,
                "cache_read_input_tokens": 0,
                "service_tier": "standard"
            },
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "WebFetch",
                    "input": {"url": "https://example.com"}
                },
                {
                    "type": "text",
                    "text": "done"
                }
            ]
        }
    });

    let out = normalize_record(
        &record,
        "claude",
        "claude-code",
        "/Users/eric/.claude/projects/p1/s1.jsonl",
        55,
        2,
        10,
        100,
        "",
        "",
        "",
    )
    .expect("claude event should normalize");

    assert_eq!(out.event_rows.len(), 2);
    assert_eq!(out.tool_rows.len(), 1);

    let first = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        first.get("event_kind").unwrap().as_str().unwrap(),
        "tool_call"
    );
    assert_eq!(
        first.get("harness").unwrap().as_str().unwrap(),
        "claude-code"
    );
    assert_eq!(
        first.get("inference_provider").unwrap().as_str().unwrap(),
        "anthropic"
    );
    let buckets = first
        .get("token_usage_buckets")
        .and_then(Value::as_object)
        .expect("canonical token buckets");
    assert_eq!(buckets.get("input_text").and_then(Value::as_u64), Some(9));
    assert_eq!(
        buckets.get("input_cache_write").and_then(Value::as_u64),
        Some(19630)
    );
    assert!(out.error_rows.is_empty());
}

#[test]
fn claude_reasoning_block_uses_canonical_metadata() {
    let record = json!({
        "type": "assistant",
        "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
        "uuid": "assistant-2",
        "parentUuid": "user-1",
        "requestId": "req-2",
        "timestamp": "2026-01-19T15:58:41.421Z",
        "message": {
            "model": "claude-opus-4-5-20251101",
            "role": "assistant",
            "content": [
                {
                    "type": "thinking",
                    "thinking": "I should answer directly."
                }
            ]
        }
    });

    let out = normalize_record(
        &record,
        "claude",
        "claude-code",
        "/Users/eric/.claude/projects/p1/s1.jsonl",
        55,
        2,
        12,
        120,
        "",
        "",
        "",
    )
    .expect("claude reasoning event should normalize");

    assert_eq!(out.event_rows.len(), 1);
    assert!(out.error_rows.is_empty());
    assert_canonical_reasoning_metadata(&out.event_rows[0]);
}

#[test]
fn invalid_timestamp_uses_epoch_and_emits_timestamp_parse_error() {
    let record = json!({
        "timestamp": "not-a-timestamp",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "call_id": "call_bad_ts",
            "name": "Read",
            "arguments": "{}"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        9,
        2,
        7,
        99,
        "",
        "",
        "",
    )
    .expect("codex event with invalid timestamp should normalize");

    let event_row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        event_row.get("event_ts").unwrap().as_str().unwrap(),
        "1970-01-01 00:00:00.000"
    );
    assert_eq!(
        event_row.get("session_date").unwrap().as_str().unwrap(),
        "1970-01-01"
    );

    assert_eq!(out.error_rows.len(), 1);
    let error = out.error_rows[0].as_object().unwrap();
    assert_eq!(
        error.get("error_kind").unwrap().as_str().unwrap(),
        "timestamp_parse_error"
    );
    assert_eq!(
        error.get("raw_fragment").unwrap().as_str().unwrap(),
        "not-a-timestamp"
    );
}

#[test]
fn missing_codex_timestamp_infers_rollout_file_timestamp() {
    let record = json!({
        "type": "function_call",
        "call_id": "call_legacy_rollout",
        "name": "shell",
        "arguments": "{}"
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2025/09/21/rollout-2025-09-21T17-12-48-6ce8b66e-8a97-441b-a606-16d2a0c27083.jsonl",
        9,
        1,
        7,
        99,
        "",
        "",
        "",
    )
    .expect("legacy codex rollout record should normalize");

    assert!(out.error_rows.is_empty());
    let raw_row = out.raw_row.as_object().unwrap();
    assert_eq!(
        raw_row.get("record_ts").unwrap().as_str().unwrap(),
        "2025-09-21T17:12:48.000Z"
    );
    let event_row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        event_row.get("event_ts").unwrap().as_str().unwrap(),
        "2025-09-21 17:12:48.000"
    );
    assert_eq!(
        event_row.get("session_date").unwrap().as_str().unwrap(),
        "2025-09-21"
    );
}

#[test]
fn invalid_timestamp_preserves_session_date_from_source_path() {
    let record = json!({
        "timestamp": "still-not-a-timestamp",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "call_id": "call_bad_ts",
            "name": "Read",
            "arguments": "{}"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/16/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        11,
        4,
        12,
        144,
        "",
        "",
        "",
    )
    .expect("codex event should normalize while preserving session date from path");

    let event_row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        event_row.get("event_ts").unwrap().as_str().unwrap(),
        "1970-01-01 00:00:00.000"
    );
    assert_eq!(
        event_row.get("session_date").unwrap().as_str().unwrap(),
        "2026-02-16"
    );
    assert_eq!(out.error_rows.len(), 1);
}

#[test]
fn unknown_harness_is_rejected() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:42.191Z",
        "type": "turn_context",
    });

    let err = normalize_record(
        &record,
        "unknown",
        "unknown",
        "/tmp/sessions/session-1.jsonl",
        1,
        1,
        1,
        1,
        "",
        "",
        "",
    )
    .expect_err("unknown harness should be rejected");

    assert!(
        err.to_string().contains("unsupported harness"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn legacy_claude_harness_value_is_rejected() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:42.191Z",
        "type": "assistant",
        "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
        "uuid": "assistant-1",
        "message": {"role": "assistant", "content": "done"}
    });

    let err = normalize_record(
        &record,
        "claude",
        "claude",
        "/Users/eric/.claude/projects/p1/s1.jsonl",
        1,
        1,
        1,
        1,
        "",
        "",
        "",
    )
    .expect_err("legacy `claude` harness value should be rejected");

    assert!(
        err.to_string().contains("unsupported harness"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn codex_event_populates_inference_provider_openai() {
    let record = json!({
        "timestamp": "2026-02-14T02:28:00.000Z",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "call_id": "call_ip",
            "name": "Read",
            "arguments": "{}"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/14/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
        10,
        1,
        1,
        1,
        "",
        "",
        "",
    )
    .expect("codex event should normalize");

    assert_eq!(
        out.raw_row
            .get("inference_provider")
            .unwrap()
            .as_str()
            .unwrap(),
        "openai"
    );
    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(row.get("harness").unwrap().as_str().unwrap(), "codex");
    assert_eq!(
        row.get("inference_provider").unwrap().as_str().unwrap(),
        "openai"
    );
    let tool_row = out.tool_rows[0].as_object().unwrap();
    assert_eq!(
        tool_row
            .get("inference_provider")
            .unwrap()
            .as_str()
            .unwrap(),
        "openai"
    );
}

#[test]
fn claude_links_split_event_uids_from_external_ids() {
    let record = json!({
        "type": "assistant",
        "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
        "uuid": "assistant-2",
        "parentUuid": "user-parent-2",
        "toolUseID": "toolu_42",
        "sourceToolAssistantUUID": "assistant-root-1",
        "requestId": "req-2",
        "timestamp": "2026-01-19T15:59:41.421Z",
        "message": {
            "role": "assistant",
            "content": "done"
        }
    });

    let out = normalize_record(
        &record,
        "claude",
        "claude-code",
        "/Users/eric/.claude/projects/p1/s1.jsonl",
        55,
        2,
        11,
        101,
        "",
        "",
        "",
    )
    .expect("claude assistant record should normalize");

    assert_eq!(out.link_rows.len(), 3);

    let by_type = out
        .link_rows
        .iter()
        .map(|row| {
            let obj = row.as_object().expect("link row object");
            let link_type = obj
                .get("link_type")
                .and_then(|v| v.as_str())
                .expect("link_type")
                .to_string();
            (link_type, obj.clone())
        })
        .collect::<HashMap<_, _>>();

    let parent = by_type.get("parent_uuid").expect("parent_uuid link");
    assert_eq!(
        parent
            .get("linked_external_id")
            .and_then(|v| v.as_str())
            .unwrap(),
        "user-parent-2"
    );
    assert_eq!(
        parent
            .get("linked_event_uid")
            .and_then(|v| v.as_str())
            .unwrap(),
        ""
    );

    let tool_use = by_type.get("tool_use_id").expect("tool_use_id link");
    assert_eq!(
        tool_use
            .get("linked_external_id")
            .and_then(|v| v.as_str())
            .unwrap(),
        "toolu_42"
    );
    assert_eq!(
        tool_use
            .get("linked_event_uid")
            .and_then(|v| v.as_str())
            .unwrap(),
        ""
    );

    let source_tool = by_type
        .get("source_tool_assistant")
        .expect("source_tool_assistant link");
    assert_eq!(
        source_tool
            .get("linked_external_id")
            .and_then(|v| v.as_str())
            .unwrap(),
        "assistant-root-1"
    );
    assert_eq!(
        source_tool
            .get("linked_event_uid")
            .and_then(|v| v.as_str())
            .unwrap(),
        ""
    );
}

#[test]
fn codex_compacted_parent_link_uses_event_uid_target() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:50.838Z",
        "type": "compacted",
        "payload": {
            "replacement_history": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "hello"}
                    ]
                }
            ]
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        12,
        12,
        "",
        "",
        "",
    )
    .expect("compacted record should normalize");

    let compacted_uid = out.event_rows[0]
        .get("event_uid")
        .and_then(|v| v.as_str())
        .expect("compacted event uid");
    let link = out.link_rows[0].as_object().expect("compacted link");

    assert_eq!(
        link.get("link_type").and_then(|v| v.as_str()).unwrap(),
        "compacted_parent"
    );
    assert_eq!(
        link.get("linked_event_uid")
            .and_then(|v| v.as_str())
            .unwrap(),
        compacted_uid
    );
    assert_eq!(
        link.get("linked_external_id")
            .and_then(|v| v.as_str())
            .unwrap(),
        ""
    );
}

#[test]
fn codex_unknown_payload_type_is_canonicalized() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:50.838Z",
        "type": "response_item",
        "payload": {
            "type": "brand_new_payload_type",
            "body": "x"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        5,
        5,
        "",
        "",
        "",
    )
    .expect("record should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(row.get("event_kind").unwrap().as_str().unwrap(), "unknown");
    assert_eq!(
        row.get("payload_type").unwrap().as_str().unwrap(),
        "unknown"
    );
}

#[test]
fn codex_event_msg_known_operational_payload_type_is_preserved() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:50.838Z",
        "type": "event_msg",
        "payload": {
            "type": "task_started",
            "status": "in_progress"
        }
    });

    let out = normalize_record(
        &record,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        6,
        6,
        "",
        "",
        "",
    )
    .expect("record should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(
        row.get("event_kind").unwrap().as_str().unwrap(),
        "event_msg"
    );
    assert_eq!(
        row.get("payload_type").unwrap().as_str().unwrap(),
        "task_started"
    );
}

#[test]
fn claude_progress_unknown_payload_type_moves_to_unknown_and_preserves_op_kind() {
    let record = json!({
        "timestamp": "2026-02-15T03:50:50.838Z",
        "type": "progress",
        "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
        "data": {
            "type": "provider_extension_step"
        },
        "status": "ok"
    });

    let out = normalize_record(
        &record,
        "claude",
        "claude-code",
        "/Users/eric/.claude/projects/p1/s1.jsonl",
        1,
        1,
        6,
        6,
        "",
        "",
        "",
    )
    .expect("record should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(row.get("event_kind").unwrap().as_str().unwrap(), "progress");
    assert_eq!(
        row.get("payload_type").unwrap().as_str().unwrap(),
        "unknown"
    );
    assert_eq!(
        row.get("op_kind").unwrap().as_str().unwrap(),
        "provider_extension_step"
    );
}

#[test]
fn hermes_sharegpt_trajectory_normalizes_messages_and_tool_io() {
    let record = json!({
        "timestamp": "2026-03-30T14:22:31.456789",
        "model": "anthropic/claude-sonnet-4.6",
        "prompt_index": 7,
        "completed": true,
        "partial": false,
        "api_calls": 1,
        "conversations": [
            {
                "from": "system",
                "value": "You are a careful assistant."
            },
            {
                "from": "human",
                "value": "Find the weather in Boston."
            },
            {
                "from": "gpt",
                "value": "<think>Need to search first.</think>\n<tool_call>{\"name\":\"weather\",\"arguments\":{\"location\":\"Boston, MA\"}}</tool_call>"
            },
            {
                "from": "tool",
                "value": "<tool_response>{\"tool_call_id\":\"call_abc123\",\"name\":\"weather\",\"content\":{\"forecast\":\"rain\"}}</tool_response>"
            },
            {
                "from": "gpt",
                "value": "It looks rainy in Boston."
            }
        ]
    });

    let out = normalize_record(
        &record,
        "hermes-batch",
        "hermes",
        "/tmp/hermes/batch-output.jsonl",
        1,
        1,
        1,
        128,
        "",
        "",
        "",
    )
    .expect("hermes record should normalize");

    assert!(
        out.error_rows.is_empty(),
        "unexpected errors: {:?}",
        out.error_rows
    );
    assert_eq!(out.event_rows.len(), 7);
    assert_eq!(out.tool_rows.len(), 2);

    let session_id = out.session_hint.clone();
    assert!(session_id.starts_with("hermes:"));
    assert_eq!(
        out.raw_row
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap(),
        session_id
    );
    assert_eq!(
        out.raw_row.get("top_type").and_then(Value::as_str).unwrap(),
        "trajectory"
    );

    let meta = out.event_rows[0].as_object().expect("session meta row");
    assert_eq!(
        meta.get("event_kind").and_then(Value::as_str),
        Some("session_meta")
    );
    assert_eq!(
        meta.get("op_status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        meta.get("record_ts").and_then(Value::as_str),
        Some("2026-03-30T14:22:31.456789Z")
    );

    let user_message = out
        .event_rows
        .iter()
        .find(|row| {
            row.get("actor_kind") == Some(&json!("user"))
                && row.get("event_kind") == Some(&json!("message"))
        })
        .expect("user message row");
    assert_eq!(
        user_message.get("turn_index").and_then(Value::as_u64),
        Some(1)
    );

    let reasoning = out
        .event_rows
        .iter()
        .find(|row| row.get("event_kind") == Some(&json!("reasoning")))
        .expect("reasoning row");
    assert_canonical_reasoning_metadata(reasoning);

    let tool_call = out
        .event_rows
        .iter()
        .find(|row| row.get("event_kind") == Some(&json!("tool_call")))
        .expect("tool call row");
    assert_eq!(
        tool_call.get("tool_name").and_then(Value::as_str),
        Some("weather")
    );
    assert_eq!(
        tool_call.get("tool_call_id").and_then(Value::as_str),
        Some("call_abc123")
    );

    let tool_result = out
        .event_rows
        .iter()
        .find(|row| {
            row.get("event_kind") == Some(&json!("tool_result"))
                && row.get("tool_call_id") == Some(&json!("call_abc123"))
        })
        .expect("tool result row");
    assert_eq!(
        tool_result.get("tool_name").and_then(Value::as_str),
        Some("weather")
    );
    assert!(tool_result
        .get("record_ts")
        .and_then(Value::as_str)
        .unwrap()
        .ends_with('Z'));

    let final_message = out
        .event_rows
        .iter()
        .find(|row| row.get("text_content") == Some(&json!("It looks rainy in Boston.")))
        .expect("final assistant message");
    assert_eq!(
        final_message.get("model").and_then(Value::as_str),
        Some("claude-sonnet-4.6"),
        "vendor/model split should strip the leading `anthropic/` from model",
    );
    assert_eq!(
        final_message
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("anthropic"),
        "inference_provider should be parsed from the record's vendor prefix",
    );
    // raw_row and tool/link rows should carry the same parsed vendor.
    assert_eq!(
        out.raw_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("anthropic"),
    );

    let tool_request = out
        .tool_rows
        .iter()
        .find(|row| row.get("tool_phase") == Some(&json!("request")))
        .expect("tool request row");
    assert_eq!(
        tool_request.get("tool_call_id").and_then(Value::as_str),
        Some("call_abc123")
    );
    let tool_response = out
        .tool_rows
        .iter()
        .find(|row| row.get("tool_phase") == Some(&json!("response")))
        .expect("tool response row");
    assert_eq!(
        tool_response.get("output_text").and_then(Value::as_str),
        Some("{\"forecast\":\"rain\"}")
    );
}

#[test]
fn kimi_cli_status_update_stamps_placeholder_model() {
    let record = json!({
        "timestamp": 1776735761.27701_f64,
        "message": {
            "type": "StatusUpdate",
            "payload": {
                "message_id": "msg_abc",
                "context_usage": 0.42,
                "token_usage": {
                    "input_other": 1234,
                    "input_cache_read": 56,
                    "input_cache_creation": 78,
                    "output": 90
                }
            }
        }
    });

    let out = normalize_record(
        &record,
        "kimi-cli",
        "kimi-cli",
        "/Users/eric/.kimi/sessions/work-abc/sess-xyz/wire.jsonl",
        1,
        1,
        5,
        500,
        "",
        "",
        "",
    )
    .expect("kimi status update should normalize");

    assert_eq!(out.event_rows.len(), 1);
    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(row.get("model").and_then(Value::as_str), Some("kimi-cli"));
    // input_tokens sums input_other + input_cache_read + input_cache_creation
    // (1234 + 56 + 78), per #275.
    assert_eq!(row.get("input_tokens").and_then(Value::as_u64), Some(1368));
    assert_eq!(row.get("output_tokens").and_then(Value::as_u64), Some(90));
    let buckets = row
        .get("token_usage_buckets")
        .and_then(Value::as_object)
        .expect("canonical token buckets");
    assert_eq!(
        buckets.get("input_text").and_then(Value::as_u64),
        Some(1234)
    );
    assert_eq!(
        buckets.get("input_cache_read").and_then(Value::as_u64),
        Some(56)
    );
    assert_eq!(
        buckets.get("input_cache_write").and_then(Value::as_u64),
        Some(78)
    );
    assert_eq!(buckets.get("output_text").and_then(Value::as_u64), Some(90));
    assert_eq!(out.model_hint, "kimi-cli");
}

#[test]
fn kimi_cli_content_part_stamps_placeholder_model() {
    let record = json!({
        "timestamp": 1776735761.27701_f64,
        "message": {
            "type": "ContentPart",
            "payload": {
                "type": "text",
                "text": "hello there"
            }
        }
    });

    let out = normalize_record(
        &record,
        "kimi-cli",
        "kimi-cli",
        "/Users/eric/.kimi/sessions/work-abc/sess-xyz/wire.jsonl",
        1,
        1,
        6,
        600,
        "",
        "",
        "",
    )
    .expect("kimi content part should normalize");

    let row = out.event_rows[0].as_object().unwrap();
    assert_eq!(row.get("model").and_then(Value::as_str), Some("kimi-cli"));
}

#[test]
fn claude_code_record_level_cwd_wins_over_hint() {
    let record = json!({
        "type": "user",
        "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
        "cwd": "/work/claude-demo",
        "uuid": "user-1",
        "timestamp": "2026-01-19T15:58:40.000Z",
        "message": {"role": "user", "content": "hello"}
    });

    let out = normalize_record(
        &record,
        "claude",
        "claude-code",
        "/Users/eric/.claude/projects/demo/7c666c01-d38e-4658-8650-854ffb5b626e.jsonl",
        1,
        1,
        1,
        0,
        "",
        "",
        "/somewhere/else",
    )
    .expect("claude record should normalize");

    assert_eq!(
        out.raw_row.get("cwd").and_then(Value::as_str),
        Some("/work/claude-demo")
    );
    for row in &out.event_rows {
        assert_eq!(
            row.get("cwd").and_then(Value::as_str),
            Some("/work/claude-demo")
        );
    }
    assert_eq!(out.cwd_hint, "/work/claude-demo");
}

#[test]
fn codex_session_meta_cwd_flows_to_later_records_via_hint() {
    let meta = json!({
        "timestamp": "2026-02-15T03:50:40.000Z",
        "type": "session_meta",
        "payload": {
            "id": "019c5f6a-49bd-7920-ac67-1dd8e33b0e95",
            "cwd": "/repo",
            "cli_version": "0.5.3"
        }
    });

    let meta_out = normalize_record(
        &meta,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        1,
        0,
        "",
        "",
        "",
    )
    .expect("codex session meta should normalize");

    assert_eq!(
        meta_out.raw_row.get("cwd").and_then(Value::as_str),
        Some("/repo")
    );
    assert_eq!(meta_out.cwd_hint, "/repo");

    // A follow-on record carries no cwd of its own; the chained hint from the
    // session meta record fills it in (mirrors the dispatch loop).
    let item = json!({
        "timestamp": "2026-02-15T03:50:43.000Z",
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "hi"}]
        }
    });

    let item_out = normalize_record(
        &item,
        "codex",
        "codex",
        "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
        1,
        1,
        2,
        100,
        &meta_out.session_hint,
        &meta_out.model_hint,
        &meta_out.cwd_hint,
    )
    .expect("codex response item should normalize");

    assert_eq!(
        item_out.raw_row.get("cwd").and_then(Value::as_str),
        Some("/repo")
    );
    for row in &item_out.event_rows {
        assert_eq!(row.get("cwd").and_then(Value::as_str), Some("/repo"));
    }
    assert_eq!(item_out.cwd_hint, "/repo");
}

#[test]
fn cursor_composer_workspace_path_becomes_cwd() {
    // Shape produced by `synthesize_cursor_sqlite_record` for composerData
    // rows: `workspaceIdentifier.uri.fsPath` is surfaced as `workspacePath`.
    let record = json!({
        "type": "cursor_composer",
        "sessionId": "11111111-2222-4333-8444-555555555555",
        "timestamp": "2026-05-08T02:04:37.751Z",
        "messageCount": 5,
        "name": "Demo refactor session",
        "workspacePath": "/Users/demo/project"
    });

    let out = normalize_record(
        &record,
        "cursor-sqlite",
        "cursor",
        "/fixtures/cursor/User/globalStorage/state.vscdb",
        1,
        1,
        1,
        0,
        "",
        "",
        "",
    )
    .expect("cursor composer should normalize");

    assert_eq!(
        out.raw_row.get("cwd").and_then(Value::as_str),
        Some("/Users/demo/project")
    );
    for row in &out.event_rows {
        assert_eq!(
            row.get("cwd").and_then(Value::as_str),
            Some("/Users/demo/project")
        );
    }
    assert_eq!(out.cwd_hint, "/Users/demo/project");
}

#[test]
fn pi_session_header_cwd_is_extracted() {
    let record = json!({
        "type": "session",
        "version": 3,
        "id": "11111111-2222-4333-8444-555555555555",
        "timestamp": "2026-05-08T02:00:00.000Z",
        "cwd": "/work/pi-demo"
    });

    let out = normalize_record(
        &record,
        "pi",
        "pi-coding-agent",
        "/Users/eric/.pi/agent/sessions/demo/2026-05-08T02-00-00-000Z_11111111-2222-4333-8444-555555555555.jsonl",
        1,
        1,
        1,
        0,
        "",
        "",
        "",
    )
    .expect("pi session header should normalize");

    assert_eq!(
        out.raw_row.get("cwd").and_then(Value::as_str),
        Some("/work/pi-demo")
    );
    assert_eq!(out.cwd_hint, "/work/pi-demo");
}

#[test]
fn current_omp_event_types_normalize_with_pi_adapter() {
    let cases = [
        (
            json!({
                "type": "custom",
                "customType": "tool_execution_start",
                "data": {"toolCallId": "call-1", "toolName": "read"},
                "id": "custom-1",
                "parentId": "parent-1",
                "timestamp": "2026-07-10T02:45:40.169Z"
            }),
            "system",
        ),
        (
            json!({
                "type": "mode_change",
                "id": "mode-1",
                "parentId": "custom-1",
                "timestamp": "2026-07-10T02:45:40.170Z",
                "mode": "goal",
                "data": {"goal": {"status": "active"}}
            }),
            "unknown",
        ),
        (
            json!({
                "type": "custom_message",
                "customType": "irc:incoming",
                "content": "peer update",
                "display": true,
                "id": "custom-message-1",
                "parentId": "mode-1",
                "timestamp": "2026-07-10T02:45:40.171Z"
            }),
            "message",
        ),
        (
            json!({
                "type": "mcp_tool_selection",
                "id": "selection-1",
                "parentId": "custom-message-1",
                "timestamp": "2026-07-10T02:45:40.172Z",
                "selectedToolNames": ["mcp__example"]
            }),
            "unknown",
        ),
        (
            json!({
                "type": "compaction",
                "id": "compaction-1",
                "parentId": "selection-1",
                "timestamp": "2026-07-10T02:45:40.173Z",
                "summary": "Earlier context",
                "firstKeptEntryId": "kept-1"
            }),
            "summary",
        ),
    ];

    for (record, expected_event_kind) in cases {
        let out = normalize_record(
            &record,
            "omp",
            "pi-coding-agent",
            "/Users/demo/.omp/agent/sessions/project/2026-07-10T02-43-10-562Z_11111111-2222-4333-8444-555555555555.jsonl",
            1,
            1,
            2,
            0,
            "11111111-2222-4333-8444-555555555555",
            "",
            "/work/omp-demo",
        )
        .expect("OMP record should normalize with the Pi adapter");

        assert_eq!(
            out.raw_row.get("source_name").and_then(Value::as_str),
            Some("omp")
        );
        assert_eq!(
            out.raw_row.get("harness").and_then(Value::as_str),
            Some("pi-coding-agent")
        );
        assert_eq!(out.event_rows.len(), 1);
        assert_eq!(
            out.event_rows[0].get("event_kind").and_then(Value::as_str),
            Some(expected_event_kind)
        );
    }
}

#[test]
fn harnesses_without_cwd_emit_empty_cwd() {
    let kimi = json!({
        "timestamp": 1775953946.2_f64,
        "message": {
            "type": "ContentPart",
            "payload": {"type": "text", "text": "hello"}
        }
    });
    let kimi_out = normalize_record(
        &kimi,
        "kimi-cli",
        "kimi-cli",
        "/Users/eric/.kimi/sessions/work-abc/sess-xyz/wire.jsonl",
        1,
        1,
        2,
        50,
        "",
        "",
        "",
    )
    .expect("kimi record should normalize");
    assert_eq!(
        kimi_out.raw_row.get("cwd").and_then(Value::as_str),
        Some("")
    );
    assert_eq!(kimi_out.cwd_hint, "");

    let hermes = json!({
        "timestamp": "2026-03-30T14:22:31.456789",
        "model": "anthropic/claude-sonnet-4.6",
        "conversations": [{"from": "human", "value": "hi"}]
    });
    let hermes_out = normalize_record(
        &hermes,
        "hermes",
        "hermes",
        "/Users/eric/hermes/trajectories/001.jsonl",
        1,
        1,
        1,
        0,
        "",
        "",
        "",
    )
    .expect("hermes record should normalize");
    assert_eq!(
        hermes_out.raw_row.get("cwd").and_then(Value::as_str),
        Some("")
    );
    assert_eq!(hermes_out.cwd_hint, "");

    for row in kimi_out.event_rows.iter().chain(&hermes_out.event_rows) {
        assert_eq!(row.get("cwd").and_then(Value::as_str), Some(""));
    }
}
