use std::collections::BTreeSet;
use std::path::PathBuf;

use moraine_ingest_core::model::NormalizedRecord;
use moraine_ingest_core::normalize::normalize_record;
use serde_json::Value;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("qwen-code")
        .join("session.jsonl")
}

fn normalize_fixture() -> Vec<NormalizedRecord> {
    let path = fixture_path();
    let body = std::fs::read_to_string(&path).expect("read Qwen fixture");
    let mut offset = 0_u64;
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut cwd_hint = String::new();
    let mut rows = Vec::new();

    for (index, raw_line) in body.split_inclusive('\n').enumerate() {
        let source_offset = offset;
        offset = offset.saturating_add(raw_line.len() as u64);
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line).expect("valid Qwen fixture JSON");
        let normalized = normalize_record(
            &record,
            "qwen-code",
            "qwen-code",
            path.to_str().expect("UTF-8 fixture path"),
            1,
            1,
            index as u64 + 1,
            source_offset,
            &session_hint,
            &model_hint,
            &cwd_hint,
        )
        .expect("Qwen fixture record normalizes");
        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        cwd_hint = normalized.cwd_hint.clone();
        rows.push(normalized);
    }
    rows
}

fn row_by_uuid<'a>(rows: &'a [NormalizedRecord], uuid: &str) -> &'a NormalizedRecord {
    rows.iter()
        .find(|row| {
            row.raw_row
                .get("raw_json")
                .and_then(Value::as_str)
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .and_then(|raw| raw.get("uuid").and_then(Value::as_str).map(str::to_string))
                .as_deref()
                == Some(uuid)
        })
        .unwrap_or_else(|| panic!("fixture row {uuid}"))
}

fn event_by_item_id<'a>(rows: &'a [NormalizedRecord], item_id: &str) -> &'a Value {
    rows.iter()
        .flat_map(|row| row.event_rows.iter())
        .find(|event| event.get("item_id").and_then(Value::as_str) == Some(item_id))
        .unwrap_or_else(|| panic!("fixture event {item_id}"))
}

#[test]
fn qwen_fixture_preserves_raw_tree_and_bounds_searchable_expansion() {
    let rows = normalize_fixture();
    assert_eq!(rows.len(), 21, "one normalized raw row per fixture record");
    assert!(rows.iter().all(|row| !row.raw_row.is_null()));
    assert!(rows.iter().all(|row| {
        row.raw_row.get("harness").and_then(Value::as_str) == Some("qwen-code")
            && row.raw_row.get("source_name").and_then(Value::as_str) == Some("qwen-code")
            && row.raw_row.get("session_id").and_then(Value::as_str) == Some("qwen-session-001")
            && row.raw_row.get("cwd").and_then(Value::as_str) == Some("/workspace/qwen-demo")
            && row
                .raw_row
                .get("inference_provider")
                .and_then(Value::as_str)
                == Some("")
    }));

    for uuid in [
        "qwen-future",
        "qwen-telemetry",
        "qwen-snapshot",
        "qwen-binary",
    ] {
        let row = row_by_uuid(&rows, uuid);
        assert!(row.event_rows.is_empty(), "{uuid} must remain raw-only");
        assert!(row.link_rows.is_empty(), "{uuid} must not fabricate a link");
        assert!(
            row.tool_rows.is_empty(),
            "{uuid} must not fabricate tool I/O"
        );
    }

    let compression = row_by_uuid(&rows, "qwen-compression");
    assert_eq!(compression.event_rows.len(), 1);
    assert_eq!(compression.event_rows[0]["event_kind"], "summary");
    assert_eq!(compression.event_rows[0]["payload_type"], "compacted");
    assert_eq!(compression.event_rows[0]["text_content"], "");
    assert!(!compression.event_rows[0]["payload_json"]
        .as_str()
        .expect("compression payload")
        .contains("compressedHistory"));

    let abandoned = event_by_item_id(&rows, "qwen-abandoned");
    let replacement = event_by_item_id(&rows, "qwen-replacement-a");
    assert_eq!(
        abandoned["text_content"],
        "This answer belongs to the abandoned branch."
    );
    assert_eq!(replacement["text_content"], "Replacement branch answer.");
    let linked_parent = |event: &Value| {
        rows.iter()
            .flat_map(|row| row.link_rows.iter())
            .find(|link| link["event_uid"] == event["event_uid"])
            .and_then(|link| link.get("linked_external_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    assert_eq!(linked_parent(abandoned).as_deref(), Some("qwen-midturn"));
    assert_eq!(
        linked_parent(replacement).as_deref(),
        Some("qwen-replacement-u")
    );
    assert_eq!(event_by_item_id(&rows, "qwen-rewind")["op_kind"], "rewind");
}

#[test]
fn qwen_fixture_maps_ordered_parts_tools_tokens_title_and_errors() {
    let rows = normalize_fixture();
    let assistant = row_by_uuid(&rows, "qwen-a1");
    assert_eq!(assistant.event_rows.len(), 4);
    assert_eq!(assistant.event_rows[0]["event_kind"], "reasoning");
    assert_eq!(
        assistant.event_rows[1]["text_content"],
        "I will search first."
    );
    assert_eq!(assistant.event_rows[2]["event_kind"], "tool_call");
    assert_eq!(
        assistant.event_rows[2]["tool_name"],
        "mcp__moraine__search_sessions"
    );
    assert_eq!(
        assistant.event_rows[3]["text_content"],
        "The search request is ready."
    );
    assert_eq!(assistant.event_rows[0]["input_tokens"], 120);
    assert_eq!(assistant.event_rows[0]["output_tokens"], 80);
    assert_eq!(assistant.event_rows[0]["cache_read_tokens"], 20);
    assert_eq!(
        assistant.event_rows[0]["token_usage_buckets"]["input_image"],
        4
    );
    assert_eq!(
        assistant.event_rows[0]["token_usage_buckets"]["output_audio"],
        11
    );
    assert_eq!(
        assistant.event_rows[0]["token_usage_buckets"]["reasoning"],
        7
    );
    assert_eq!(
        assistant.event_rows[0]["token_usage_buckets"]["server_tool_use"],
        5
    );
    assert!(assistant.event_rows[1..]
        .iter()
        .all(|event| event["input_tokens"] == 0 && event["output_tokens"] == 0));
    assert!(assistant.event_rows.iter().all(|event| {
        event["model"] == "qwen3-coder-plus" && event["inference_provider"] == ""
    }));

    let successful_result = row_by_uuid(&rows, "qwen-r1");
    assert_eq!(successful_result.event_rows.len(), 1);
    assert_eq!(successful_result.tool_rows.len(), 1);
    assert_eq!(
        successful_result.event_rows[0]["tool_call_id"],
        "qwen-call-search"
    );
    assert_eq!(successful_result.event_rows[0]["tool_error"], 0);
    assert_eq!(successful_result.event_rows[0]["latency_ms"], 25);
    assert_eq!(
        successful_result.tool_rows[0]["output_text"],
        "Three matching sessions."
    );

    let failed_result = row_by_uuid(&rows, "qwen-r2");
    assert_eq!(
        failed_result.event_rows[0]["tool_call_id"],
        "qwen-call-fail"
    );
    assert_eq!(failed_result.event_rows[0]["tool_error"], 1);
    assert_eq!(failed_result.tool_rows.len(), 1);

    let uuid_fallback = row_by_uuid(&rows, "qwen-r3");
    assert_eq!(uuid_fallback.event_rows[0]["tool_call_id"], "qwen-r3");
    assert_eq!(uuid_fallback.tool_rows[0]["tool_call_id"], "qwen-r3");

    let title = event_by_item_id(&rows, "qwen-title");
    assert_eq!(title["event_kind"], "session_meta");
    let title_payload: Value =
        serde_json::from_str(title["payload_json"].as_str().expect("title payload JSON"))
            .expect("title payload parses");
    assert_eq!(title_payload["title"], "Qwen Fixture Contract");

    let malformed = row_by_uuid(&rows, "qwen-bad-ts");
    assert_eq!(malformed.event_rows.len(), 1);
    assert_eq!(malformed.error_rows.len(), 1);
    assert_eq!(
        malformed.error_rows[0]["error_kind"],
        "timestamp_parse_error"
    );
    assert_eq!(
        rows.iter().map(|row| row.error_rows.len()).sum::<usize>(),
        1
    );
}

#[test]
fn qwen_fixture_normalization_is_uid_deterministic() {
    let first = normalize_fixture();
    let second = normalize_fixture();

    let identity_set = |rows: &[NormalizedRecord]| -> BTreeSet<String> {
        let events = rows
            .iter()
            .flat_map(|row| row.event_rows.iter())
            .map(|row| format!("event:{}", row["event_uid"].as_str().expect("event uid")));
        let links = rows.iter().flat_map(|row| row.link_rows.iter()).map(|row| {
            format!(
                "link:{}:{}:{}",
                row["event_uid"].as_str().expect("link event uid"),
                row["linked_external_id"].as_str().expect("external id"),
                row["link_type"].as_str().expect("link type")
            )
        });
        let tools = rows.iter().flat_map(|row| row.tool_rows.iter()).map(|row| {
            format!(
                "tool:{}:{}:{}",
                row["event_uid"].as_str().expect("tool event uid"),
                row["tool_call_id"].as_str().expect("tool call id"),
                row["tool_phase"].as_str().expect("tool phase")
            )
        });
        events.chain(links).chain(tools).collect()
    };

    assert_eq!(identity_set(&first), identity_set(&second));
}
