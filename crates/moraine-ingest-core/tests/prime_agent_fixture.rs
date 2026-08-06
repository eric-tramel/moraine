use std::path::{Path, PathBuf};

use moraine_ingest_core::model::NormalizedRecord;
use moraine_ingest_core::normalize::normalize_record;
use serde_json::{json, Value};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn normalize_fixture(
    fixture: &Path,
    source_name: &str,
    source_file: &str,
) -> Vec<NormalizedRecord> {
    let body = std::fs::read_to_string(fixture)
        .unwrap_or_else(|error| panic!("read {}: {error}", fixture.display()));
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut cwd_hint = String::new();
    let mut offset = 0_u64;
    let mut records = Vec::new();

    for (index, raw_line) in body.split_inclusive('\n').enumerate() {
        let start_offset = offset;
        offset += raw_line.len() as u64;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("{}:{}: {error}", fixture.display(), index + 1));
        let normalized = normalize_record(
            &record,
            source_name,
            "prime-agent",
            source_file,
            1,
            1,
            index as u64 + 1,
            start_offset,
            &session_hint,
            &model_hint,
            &cwd_hint,
        )
        .unwrap_or_else(|error| panic!("normalize {}:{}: {error:#}", fixture.display(), index + 1));
        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        cwd_hint = normalized.cwd_hint.clone();
        records.push(normalized);
    }
    records
}

fn row_count(records: &[NormalizedRecord], field: fn(&NormalizedRecord) -> &[Value]) -> usize {
    records.iter().map(|record| field(record).len()).sum()
}

fn token_total(records: &[NormalizedRecord], field: &str) -> u64 {
    records
        .iter()
        .flat_map(|record| &record.event_rows)
        .filter_map(|event| event.get(field).and_then(Value::as_u64))
        .sum()
}

#[test]
fn prime_root_and_child_fixtures_preserve_conversation_and_hierarchy() {
    let root_id = "12345678-1234-4234-8234-123456789abc";
    let child_id = "abcdefab-cdef-4abc-8def-abcdefabcdef";
    let fixtures = repo_root().join("fixtures").join("prime-agent");
    let roots = normalize_fixture(
        &fixtures.join("session.jsonl"),
        "prime-agent",
        &format!("/fixtures/prime-agent/sessions/{root_id}.jsonl"),
    );
    let children = normalize_fixture(
        &fixtures.join("child.jsonl"),
        "prime-agent-subagents",
        &format!("/fixtures/prime-agent/session-artifacts/{root_id}/sub-ce0de280/{child_id}.jsonl"),
    );

    assert_eq!(roots.len(), 21);
    assert_eq!(row_count(&roots, |record| &record.event_rows), 13);
    assert_eq!(row_count(&roots, |record| &record.link_rows), 11);
    assert_eq!(row_count(&roots, |record| &record.tool_rows), 2);
    assert_eq!(token_total(&roots, "input_tokens"), 22);
    assert_eq!(token_total(&roots, "output_tokens"), 8);
    assert_eq!(token_total(&roots, "cache_read_tokens"), 2);
    assert_eq!(token_total(&roots, "cache_write_tokens"), 1);
    assert!(roots.iter().all(|record| {
        record.raw_row.get("session_id").and_then(Value::as_str) == Some(root_id)
            && record.raw_row.get("cwd").and_then(Value::as_str) == Some("/work/prime-demo")
    }));

    assert!(roots
        .iter()
        .flat_map(|record| &record.event_rows)
        .any(|event| event
            .get("content_types")
            .and_then(Value::as_array)
            .is_some_and(|types| types.iter().any(|kind| kind.as_str() == Some("image")))));

    let raw_only_types = [
        "service_tier_change",
        "custom",
        "agent_status",
        "child_usage_attributed",
        "session_state",
        "git_state",
        "future_prime_record",
    ];
    for record in &roots {
        let top_type = record
            .raw_row
            .get("top_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if raw_only_types.contains(&top_type) {
            assert!(record.event_rows.is_empty(), "{top_type} must be raw-only");
            assert!(record.link_rows.is_empty(), "{top_type} must emit no links");
            assert!(record.tool_rows.is_empty(), "{top_type} must emit no tools");
        }
    }
    assert_eq!(
        roots
            .iter()
            .flat_map(|record| &record.event_rows)
            .filter(|event| event
                .get("text_content")
                .and_then(Value::as_str)
                .is_some_and(|text| text.contains("Visible agent handoff")))
            .count(),
        1
    );
    assert_eq!(
        roots
            .iter()
            .flat_map(|record| &record.event_rows)
            .filter(|event| event.get("text_content").and_then(Value::as_str)
                == Some("Default-visible custom note"))
            .count(),
        1,
        "custom_message records default to visible when display is absent"
    );
    let normalized_json = serde_json::to_string(
        &roots
            .iter()
            .flat_map(|record| &record.event_rows)
            .collect::<Vec<_>>(),
    )
    .expect("serialize root events");
    for hidden in [
        "HIDDEN_CUSTOM_SENTINEL",
        "HIDDEN_GOAL_SENTINEL",
        "HIDDEN_ROLE_SENTINEL",
        "HIDDEN_BLOCK_SENTINEL",
        "HIDDEN_STATUS_SENTINEL",
        "HIDDEN_FUTURE_SENTINEL",
    ] {
        assert!(
            !normalized_json.contains(hidden),
            "normalized rows leaked {hidden}"
        );
    }

    assert_eq!(children.len(), 5);
    assert_eq!(row_count(&children, |record| &record.event_rows), 4);
    assert_eq!(row_count(&children, |record| &record.link_rows), 3);
    assert_eq!(row_count(&children, |record| &record.tool_rows), 0);
    assert_eq!(token_total(&children, "input_tokens"), 8);
    assert_eq!(token_total(&children, "output_tokens"), 3);
    assert_eq!(token_total(&children, "cache_read_tokens"), 1);
    assert!(children.iter().all(|record| {
        record.raw_row.get("session_id").and_then(Value::as_str) == Some(child_id)
            && record.raw_row.get("cwd").and_then(Value::as_str) == Some("/work/prime-demo")
    }));
    assert!(children
        .iter()
        .flat_map(|record| &record.event_rows)
        .all(|event| event.get("is_substream").and_then(Value::as_u64) == Some(1)));

    let parent_links = children
        .iter()
        .flat_map(|record| &record.link_rows)
        .filter(|link| link.get("link_type").and_then(Value::as_str) == Some("subagent_parent"))
        .collect::<Vec<_>>();
    assert_eq!(parent_links.len(), 1);
    assert_eq!(
        parent_links[0]
            .get("linked_external_id")
            .and_then(Value::as_str),
        Some(root_id)
    );
    assert!(children
        .last()
        .expect("status record")
        .event_rows
        .is_empty());
}

#[test]
fn prime_hierarchy_requires_canonical_child_metadata_and_repeated_blocks_stay_distinct() {
    let child_id = "abcdefab-cdef-4abc-8def-abcdefabcdef";
    let invalid_child = json!({
        "type": "session",
        "version": 3,
        "id": child_id,
        "timestamp": "2026-08-06T02:50:25.466Z",
        "cwd": "/work/prime-demo",
        "parentSession": "/tmp/not-a-uuid.jsonl",
        "rlmDepth": 1
    });
    let invalid = normalize_record(
        &invalid_child,
        "prime-agent-subagents",
        "prime-agent",
        "/fixtures/sub-ce0de280/abcdefab-cdef-4abc-8def-abcdefabcdef.jsonl",
        1,
        1,
        1,
        0,
        "",
        "",
        "",
    )
    .expect("normalize invalid-parent child");
    assert!(invalid
        .link_rows
        .iter()
        .all(|link| link.get("link_type").and_then(Value::as_str) != Some("subagent_parent")));

    let root_fork = json!({
        "type": "session",
        "version": 3,
        "id": child_id,
        "timestamp": "2026-08-06T02:50:25.466Z",
        "cwd": "/work/prime-demo",
        "parentSession": "/tmp/12345678-1234-4234-8234-123456789abc.jsonl",
        "rlmDepth": 1
    });
    let root = normalize_record(
        &root_fork,
        "prime-agent",
        "prime-agent",
        "/fixtures/sessions/abcdefab-cdef-4abc-8def-abcdefabcdef.jsonl",
        1,
        1,
        1,
        0,
        "",
        "",
        "",
    )
    .expect("normalize root-source fork");
    assert!(root.link_rows.is_empty());
    assert_eq!(
        root.event_rows[0]
            .get("is_substream")
            .and_then(Value::as_u64),
        Some(0)
    );

    let repeated = json!({
        "type": "message",
        "id": "1234abcd",
        "parentId": null,
        "timestamp": "2026-08-06T02:50:26.000Z",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "same"},
                {"type": "text", "text": "same"}
            ],
            "provider": "openai-codex",
            "model": "gpt-5.6-sol",
            "usage": {"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0},
            "stopReason": "stop",
            "timestamp": 1785984626000_i64
        }
    });
    let repeated = normalize_record(
        &repeated,
        "prime-agent",
        "prime-agent",
        "/fixtures/sessions/12345678-1234-4234-8234-123456789abc.jsonl",
        1,
        1,
        2,
        100,
        "12345678-1234-4234-8234-123456789abc",
        "",
        "/work/prime-demo",
    )
    .expect("normalize repeated sibling blocks");
    assert_eq!(repeated.event_rows.len(), 2);
    let first = repeated.event_rows[0]
        .get("event_uid")
        .and_then(Value::as_str)
        .expect("first event uid");
    let second = repeated.event_rows[1]
        .get("event_uid")
        .and_then(Value::as_str)
        .expect("second event uid");
    assert_ne!(first, second);
    assert_eq!(token_total(&[repeated], "input_tokens"), 1);
}
