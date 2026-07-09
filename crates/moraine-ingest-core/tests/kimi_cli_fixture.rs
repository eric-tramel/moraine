use std::collections::HashSet;
use std::path::PathBuf;

use moraine_ingest_core::normalize::normalize_record;
use serde_json::Value;

const NESTED_PARENT_ID: &str = "11111111-2222-4333-8444-555555555555";
const NESTED_SUBAGENT_ID: &str = "agent-abcdef01";
const NESTED_PARENT_FIXTURE: &str = "project-273/11111111-2222-4333-8444-555555555555/wire.jsonl";
const NESTED_SUBAGENT_FIXTURE: &str = concat!(
    "project-273/11111111-2222-4333-8444-555555555555/",
    "subagents/agent-abcdef01/wire.jsonl"
);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("kimi-cli")
        .join(name)
}

fn fixture_records(name: &str) -> (PathBuf, Vec<(u64, u64, Value)>) {
    let path = fixture_path(name);
    let body = std::fs::read_to_string(&path).expect("read fixture");
    let mut offset = 0_u64;
    let records = body
        .split_inclusive('\n')
        .enumerate()
        .filter_map(|(idx, raw_line)| {
            let start_offset = offset;
            offset = offset.saturating_add(raw_line.len() as u64);
            let line = raw_line.trim();
            if line.is_empty() {
                return None;
            }
            let record = serde_json::from_str(line).expect("valid fixture line");
            Some((idx as u64 + 1, start_offset, record))
        })
        .collect();
    (path, records)
}

fn normalize_lines(name: &str) -> Vec<moraine_ingest_core::model::NormalizedRecord> {
    let (path, records) = fixture_records(name);
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut cwd_hint = String::new();

    records
        .into_iter()
        .map(|(line_no, source_offset, record)| {
            let normalized = normalize_record(
                &record,
                "ci-kimi",
                "kimi-cli",
                path.to_str().unwrap(),
                1,
                1,
                line_no,
                source_offset,
                &session_hint,
                &model_hint,
                &cwd_hint,
            )
            .expect("kimi fixture normalizes");
            session_hint = normalized.session_hint.clone();
            model_hint = normalized.model_hint.clone();
            cwd_hint = normalized.cwd_hint.clone();
            normalized
        })
        .collect()
}

#[test]
fn kimi_wire_fixture_maps_messages_tools_and_tokens() {
    let rows = normalize_lines("wire.jsonl");
    assert!(rows.iter().all(|row| row.error_rows.is_empty()));

    // The leading `{"type":"metadata",...}` header is a per-file protocol
    // marker, not a session event — normalize_record returns an empty
    // NormalizedRecord for it so no raw/event rows are emitted.
    assert!(rows[0].raw_row.is_null());
    assert!(rows[0].event_rows.is_empty());

    assert_eq!(
        rows[1].raw_row.get("harness").and_then(Value::as_str),
        Some("kimi-cli")
    );
    assert_eq!(
        rows[1]
            .raw_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("moonshot")
    );
    assert!(rows[1].session_hint.starts_with("kimi-cli:kimi-cli"));

    let user = &rows[1].event_rows[0];
    assert_eq!(
        user.get("event_kind").and_then(Value::as_str),
        Some("message")
    );
    assert_eq!(user.get("actor_kind").and_then(Value::as_str), Some("user"));
    assert_eq!(
        user.get("record_ts").and_then(Value::as_str),
        Some("2026-04-12T00:32:24.549974Z")
    );

    let reasoning = &rows[3].event_rows[0];
    assert_eq!(
        reasoning.get("event_kind").and_then(Value::as_str),
        Some("reasoning")
    );
    assert_eq!(
        reasoning.get("payload_type").and_then(Value::as_str),
        Some("reasoning")
    );
    assert_eq!(
        reasoning.get("content_types"),
        Some(&serde_json::json!(["reasoning"]))
    );
    assert_eq!(
        reasoning.get("has_reasoning").and_then(Value::as_u64),
        Some(1)
    );

    let tool_call = &rows[5].event_rows[0];
    assert_eq!(
        tool_call.get("event_kind").and_then(Value::as_str),
        Some("tool_call")
    );
    assert_eq!(
        tool_call.get("tool_name").and_then(Value::as_str),
        Some("ReadFile")
    );
    assert_eq!(rows[5].tool_rows.len(), 1);

    let tool_result = &rows[6].event_rows[0];
    assert_eq!(
        tool_result.get("event_kind").and_then(Value::as_str),
        Some("tool_result")
    );
    assert_eq!(rows[6].tool_rows.len(), 1);

    let usage = &rows[7].event_rows[0];
    assert_eq!(
        usage.get("payload_type").and_then(Value::as_str),
        Some("token_count")
    );
    // input_tokens = input_other + input_cache_read + input_cache_creation
    // (10 + 2 + 1). cache reads/writes are a subset of total input, not
    // additional on top — see issue #272.
    assert_eq!(usage.get("input_tokens").and_then(Value::as_u64), Some(13));
    assert_eq!(usage.get("output_tokens").and_then(Value::as_u64), Some(5));
    assert_eq!(
        usage.get("cache_read_tokens").and_then(Value::as_u64),
        Some(2)
    );
    assert_eq!(
        usage.get("cache_write_tokens").and_then(Value::as_u64),
        Some(1)
    );

    // Every emitted Kimi event row should carry the harness-slug placeholder
    // model ("kimi-cli") so they are visible in tokens-by-model analytics —
    // the wire schema does not expose the underlying model name.
    for record in &rows {
        for event in &record.event_rows {
            assert_eq!(
                event.get("model").and_then(Value::as_str),
                Some("kimi-cli"),
                "every kimi event should carry a placeholder model"
            );
        }
    }
}

#[test]
fn kimi_subagent_event_emits_raw_row_but_no_normalized_event() {
    // SubagentEvent envelopes on the parent wire duplicate events that are
    // already captured via the sub-agent's own wire.jsonl. They must not
    // produce a normalized event row (which would double-count every
    // sub-agent event as `progress` on the parent, see #271), but the raw
    // row is preserved so the byte-level trace remains intact.
    let rows = normalize_lines("wire.jsonl");
    let subagent_row = rows
        .iter()
        .find(|row| {
            row.raw_row
                .get("top_type")
                .and_then(Value::as_str)
                .map(|k| k == "SubagentEvent")
                .unwrap_or(false)
        })
        .expect("fixture contains a SubagentEvent record");

    assert!(!subagent_row.raw_row.is_null());
    assert!(subagent_row.event_rows.is_empty());
    assert!(subagent_row.tool_rows.is_empty());
    assert!(subagent_row.error_rows.is_empty());
}

#[test]
fn kimi_events_do_not_reuse_raw_record_uid() {
    let rows = normalize_lines("wire.jsonl");
    let mut event_uids = HashSet::new();

    for row in &rows {
        // Skipped records (e.g. the metadata header) have no raw_row; ignore
        // them here — they also have no event_rows to check.
        let Some(raw_uid) = row.raw_row.get("event_uid").and_then(Value::as_str) else {
            assert!(row.event_rows.is_empty());
            continue;
        };
        for event in &row.event_rows {
            let event_uid = event
                .get("event_uid")
                .and_then(Value::as_str)
                .expect("event uid");
            assert_ne!(raw_uid, event_uid);
            assert!(event_uids.insert(event_uid.to_string()));
        }
    }
}

#[test]
fn kimi_subagent_wire_stays_standalone_and_links_only_its_first_event_to_parent() {
    let rows = normalize_lines(NESTED_SUBAGENT_FIXTURE);

    let metadata_header = &rows[0];
    assert!(metadata_header.raw_row.is_null());
    assert!(metadata_header.event_rows.is_empty());
    assert!(metadata_header.link_rows.is_empty());

    let first = &rows[1];
    let subagent_session_id = format!("kimi-cli:{NESTED_SUBAGENT_ID}");
    assert_eq!(first.session_hint, subagent_session_id);
    assert_eq!(
        first.raw_row.get("session_id").and_then(Value::as_str),
        Some(subagent_session_id.as_str())
    );
    assert_eq!(first.event_rows.len(), 1);
    let first_event = &first.event_rows[0];
    assert_eq!(
        first_event.get("source_line_no").and_then(Value::as_u64),
        Some(3),
        "TurnBegin must remain the link anchor after a blank physical line"
    );
    assert_eq!(
        first_event.get("session_id").and_then(Value::as_str),
        Some(subagent_session_id.as_str())
    );
    assert_eq!(
        first_event.get("event_kind").and_then(Value::as_str),
        Some("message")
    );
    assert_eq!(
        first_event.get("actor_kind").and_then(Value::as_str),
        Some("user")
    );
    assert_eq!(
        first_event.get("text_content").and_then(Value::as_str),
        Some("Inspect the delegated subsystem.")
    );

    assert_eq!(first.link_rows.len(), 1);
    let link = &first.link_rows[0];
    let first_event_uid = first_event
        .get("event_uid")
        .and_then(Value::as_str)
        .expect("first subagent event uid");
    let parent_session_id = format!("kimi-cli:{NESTED_PARENT_ID}");
    assert_eq!(
        link.get("event_uid").and_then(Value::as_str),
        Some(first_event_uid)
    );
    assert_eq!(
        link.get("linked_event_uid").and_then(Value::as_str),
        Some("")
    );
    assert_eq!(
        link.get("linked_external_id").and_then(Value::as_str),
        Some(parent_session_id.as_str())
    );
    assert_eq!(
        link.get("link_type").and_then(Value::as_str),
        Some("subagent_parent")
    );
    assert_eq!(
        link.get("session_id").and_then(Value::as_str),
        Some(subagent_session_id.as_str())
    );
    assert_eq!(
        link.get("metadata_json").and_then(Value::as_str),
        Some("{}")
    );

    let later = &rows[2];
    assert_eq!(later.session_hint, subagent_session_id);
    assert_eq!(later.event_rows.len(), 1);
    assert_eq!(
        later.event_rows[0]
            .get("session_id")
            .and_then(Value::as_str),
        Some(subagent_session_id.as_str())
    );
    assert_eq!(
        later.event_rows[0]
            .get("text_content")
            .and_then(Value::as_str),
        Some("The delegated subsystem is healthy.")
    );
    assert!(later.link_rows.is_empty());
    let second_turn = &rows[3];
    assert_eq!(second_turn.session_hint, subagent_session_id);
    assert_eq!(second_turn.event_rows.len(), 1);
    assert_eq!(
        second_turn.event_rows[0]
            .get("source_line_no")
            .and_then(Value::as_u64),
        Some(5)
    );
    assert_eq!(
        second_turn.event_rows[0]
            .get("text_content")
            .and_then(Value::as_str),
        Some("Review the delegated findings.")
    );
    assert!(second_turn.link_rows.is_empty());

    let second_turn_reply = &rows[4];
    assert_eq!(second_turn_reply.session_hint, subagent_session_id);
    assert_eq!(second_turn_reply.event_rows.len(), 1);
    assert_eq!(
        second_turn_reply.event_rows[0]
            .get("text_content")
            .and_then(Value::as_str),
        Some("The second turn is complete.")
    );
    assert!(second_turn_reply.link_rows.is_empty());
    assert_eq!(rows.iter().map(|row| row.link_rows.len()).sum::<usize>(), 1);
}

#[test]
fn kimi_parent_wire_keeps_parent_session_id_and_emits_no_subagent_link() {
    let rows = normalize_lines(NESTED_PARENT_FIXTURE);

    let metadata_header = &rows[0];
    assert!(metadata_header.raw_row.is_null());
    assert!(metadata_header.event_rows.is_empty());

    let parent_session_id = format!("kimi-cli:{NESTED_PARENT_ID}");
    let first_event = &rows[1].event_rows[0];
    assert_eq!(
        first_event.get("event_kind").and_then(Value::as_str),
        Some("message")
    );
    assert_eq!(
        first_event.get("text_content").and_then(Value::as_str),
        Some("Coordinate the delegated investigation.")
    );

    for row in rows.iter().skip(1) {
        assert_eq!(row.session_hint, parent_session_id);
        assert_eq!(
            row.raw_row.get("session_id").and_then(Value::as_str),
            Some(parent_session_id.as_str())
        );
        for event in &row.event_rows {
            assert_eq!(
                event.get("session_id").and_then(Value::as_str),
                Some(parent_session_id.as_str())
            );
        }
        assert!(row.link_rows.is_empty());
    }
}

#[test]
fn kimi_subagent_parent_link_preserves_whitespace_in_parent_path_component() {
    let turn_begin = serde_json::json!({
        "timestamp": 1775954004.5_f64,
        "message": {
            "type": "TurnBegin",
            "payload": {"user_input": "Preserve the parent identity."}
        }
    });
    let parent_source_file = "/Users/test/.kimi/sessions/project-hash/  parent uuid  /wire.jsonl";
    let subagent_source_file = concat!(
        "/Users/test/.kimi/sessions/project-hash/  parent uuid  /",
        "subagents/agent-space/wire.jsonl"
    );

    let parent = normalize_record(
        &turn_begin,
        "ci-kimi",
        "kimi-cli",
        parent_source_file,
        1,
        1,
        2,
        1,
        "",
        "",
        "",
    )
    .expect("parent TurnBegin normalizes");
    let subagent = normalize_record(
        &turn_begin,
        "ci-kimi",
        "kimi-cli",
        subagent_source_file,
        1,
        1,
        2,
        1,
        "",
        "",
        "",
    )
    .expect("subagent TurnBegin normalizes");

    assert_eq!(parent.session_hint, "kimi-cli:  parent uuid  ");
    assert_eq!(subagent.session_hint, "kimi-cli:agent-space");
    assert_eq!(subagent.link_rows.len(), 1);
    assert_eq!(
        subagent.link_rows[0]
            .get("linked_external_id")
            .and_then(Value::as_str),
        Some(parent.session_hint.as_str())
    );
}

#[test]
fn kimi_incremental_resume_does_not_relink_a_later_turn_begin() {
    let (path, records) = fixture_records(NESTED_SUBAGENT_FIXTURE);
    let mut turn_begins = records.iter().filter(|(_, _, record)| {
        record
            .get("message")
            .and_then(|message| message.get("type"))
            .and_then(Value::as_str)
            == Some("TurnBegin")
    });
    let _ = turn_begins
        .next()
        .expect("fixture has an initial TurnBegin");
    let (line_no, source_offset, second_turn) =
        turn_begins.next().expect("fixture has a second TurnBegin");
    assert!(turn_begins.next().is_none());
    assert_eq!(*line_no, 5);

    let resumed = normalize_record(
        second_turn,
        "ci-kimi",
        "kimi-cli",
        path.to_str().unwrap(),
        1,
        1,
        *line_no,
        *source_offset,
        "",
        "",
        "",
    )
    .expect("incrementally resumed TurnBegin normalizes");

    assert_eq!(resumed.session_hint, "kimi-cli:agent-abcdef01");
    assert_eq!(resumed.event_rows.len(), 1);
    assert_eq!(
        resumed.event_rows[0]
            .get("source_offset")
            .and_then(Value::as_u64),
        Some(*source_offset)
    );
    assert_eq!(
        resumed.event_rows[0]
            .get("text_content")
            .and_then(Value::as_str),
        Some("Review the delegated findings.")
    );
    assert!(
        resumed.link_rows.is_empty(),
        "prefix scan must suppress the parent link when resuming with empty hints"
    );
}
