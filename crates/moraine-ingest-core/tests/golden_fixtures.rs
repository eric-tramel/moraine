use std::path::{Path, PathBuf};

use moraine_ingest_core::model::NormalizedRecord;
use moraine_ingest_core::normalize::normalize_record;
use serde_json::{json, Value};

const UPDATE_ENV: &str = "MORAINE_UPDATE_INGEST_GOLDENS";

struct GoldenCase {
    name: &'static str,
    harness: &'static str,
    source_name: &'static str,
    fixture_rel: &'static str,
    source_file: &'static str,
    format: GoldenFormat,
}

#[derive(Clone, Copy)]
enum GoldenFormat {
    Jsonl,
    HermesSessionJson,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn golden_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("goldens")
        .join("source_normalization")
}

fn fixture_path(rel: &str) -> PathBuf {
    repo_root().join(rel)
}

fn normalize_jsonl(case: &GoldenCase) -> Vec<Value> {
    let path = fixture_path(case.fixture_rel);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()));
    let mut offset = 0_u64;
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut records = Vec::new();

    for (idx, raw_line) in body.split_inclusive('\n').enumerate() {
        let line_no = idx as u64 + 1;
        let start_offset = offset;
        offset = offset.saturating_add(raw_line.len() as u64);
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let record: Value = serde_json::from_str(trimmed)
            .unwrap_or_else(|err| panic!("fixture {} line {line_no}: {err}", path.display()));
        let normalized = normalize_record(
            &record,
            case.source_name,
            case.harness,
            case.source_file,
            1,
            1,
            line_no,
            start_offset,
            &session_hint,
            &model_hint,
        )
        .unwrap_or_else(|err| panic!("normalize {} line {line_no}: {err:#}", case.name));
        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        records.push(snapshot_record(normalized));
    }

    records
}

fn normalize_hermes_session_json(case: &GoldenCase) -> Vec<Value> {
    let path = fixture_path(case.fixture_rel);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()));
    let session_doc: Value = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("parse fixture {}: {err}", path.display()));
    let messages = session_doc
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut synthetic_records = Vec::<(u64, Value)>::new();
    synthetic_records.push((0, build_hermes_session_meta_record(&session_doc)));
    for (idx, message) in messages.iter().enumerate() {
        synthetic_records.push((
            idx as u64 + 1,
            build_hermes_session_message_record(&session_doc, message, idx as u64),
        ));
    }

    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut records = Vec::new();
    for (line_no, record) in synthetic_records {
        let normalized = normalize_record(
            &record,
            case.source_name,
            case.harness,
            case.source_file,
            0,
            1,
            line_no,
            0,
            &session_hint,
            &model_hint,
        )
        .unwrap_or_else(|err| panic!("normalize {} pseudo-line {line_no}: {err:#}", case.name));
        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        records.push(snapshot_record(normalized));
    }

    records
}

fn build_hermes_session_meta_record(session_doc: &Value) -> Value {
    json!({
        "type": "session_meta",
        "timestamp": session_doc.get("session_start").cloned().unwrap_or(Value::Null),
        "session_id": session_doc.get("session_id").cloned().unwrap_or(Value::Null),
        "model": compose_hermes_model(
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

fn build_hermes_session_message_record(
    session_doc: &Value,
    message: &Value,
    message_index: u64,
) -> Value {
    json!({
        "type": "session_message",
        "timestamp": session_doc.get("last_updated").cloned().unwrap_or(Value::Null),
        "session_id": session_doc.get("session_id").cloned().unwrap_or(Value::Null),
        "model": compose_hermes_model(
            session_doc.get("model").and_then(Value::as_str).unwrap_or(""),
            session_doc.get("base_url").and_then(Value::as_str).unwrap_or(""),
        ),
        "base_url": session_doc.get("base_url").cloned().unwrap_or(Value::Null),
        "platform": session_doc.get("platform").cloned().unwrap_or(Value::Null),
        "message_index": message_index,
        "message": message,
    })
}

fn compose_hermes_model(model: &str, base_url: &str) -> String {
    if model.contains('/') || model.is_empty() {
        return model.to_string();
    }
    match infer_vendor_from_base_url(base_url) {
        "" => model.to_string(),
        vendor => format!("{vendor}/{model}"),
    }
}

fn infer_vendor_from_base_url(base_url: &str) -> &'static str {
    let lower = base_url.to_ascii_lowercase();
    if lower.contains("anthropic.com") {
        "anthropic"
    } else if lower.contains("openai.com") || lower.contains("openai.azure.com") {
        "openai"
    } else if lower.contains("openrouter") {
        "openrouter"
    } else if lower.contains("bedrock") {
        "bedrock"
    } else if lower.contains("googleapis") || lower.contains("google.com") {
        "google"
    } else {
        ""
    }
}

fn snapshot_record(record: NormalizedRecord) -> Value {
    json!({
        "raw_rows": optional_row(record.raw_row),
        "event_rows": redact_rows(record.event_rows),
        "link_rows": redact_rows(record.link_rows),
        "tool_rows": redact_rows(record.tool_rows),
        "error_rows": redact_rows(record.error_rows),
        "session_hint": record.session_hint,
        "model_hint": record.model_hint,
    })
}

fn optional_row(row: Value) -> Vec<Value> {
    if row.is_null() {
        Vec::new()
    } else {
        vec![redact_dynamic(row)]
    }
}

fn redact_rows(rows: Vec<Value>) -> Vec<Value> {
    rows.into_iter().map(redact_dynamic).collect()
}

fn redact_dynamic(mut value: Value) -> Value {
    match &mut value {
        Value::Object(obj) => {
            if obj.contains_key("event_version") {
                obj.insert("event_version".to_string(), json!("<event_version>"));
            }
            for nested in obj.values_mut() {
                redact_dynamic_in_place(nested);
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_dynamic_in_place(item);
            }
        }
        _ => {}
    }
    value
}

fn redact_dynamic_in_place(value: &mut Value) {
    match value {
        Value::Object(obj) => {
            if obj.contains_key("event_version") {
                obj.insert("event_version".to_string(), json!("<event_version>"));
            }
            for nested in obj.values_mut() {
                redact_dynamic_in_place(nested);
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_dynamic_in_place(item);
            }
        }
        _ => {}
    }
}

fn assert_or_update_golden(path: &Path, actual: &Value) {
    let actual_pretty = format!(
        "{}\n",
        serde_json::to_string_pretty(actual).expect("serialize golden")
    );

    if std::env::var_os(UPDATE_ENV).is_some() {
        std::fs::create_dir_all(path.parent().expect("golden parent")).expect("create golden dir");
        std::fs::write(path, actual_pretty).expect("write golden");
        return;
    }

    let expected_pretty = std::fs::read_to_string(path).unwrap_or_else(|err| {
        panic!(
            "read golden {}: {err}; rerun with {UPDATE_ENV}=1 to create it",
            path.display()
        )
    });
    assert_eq!(
        expected_pretty,
        actual_pretty,
        "golden fixture drifted: {}",
        path.display()
    );
}

#[test]
fn source_normalization_golden_fixtures_are_stable() {
    // Only `event_version` is redacted. UIDs, source refs, timestamps, token
    // maps, links, tool phases, model/provider fields, hints, and row counts
    // remain part of the committed snapshot.
    let cases = [
        GoldenCase {
            name: "codex",
            harness: "codex",
            source_name: "golden-codex",
            fixture_rel: "fixtures/codex/session.jsonl",
            source_file: "/fixtures/codex/session.jsonl",
            format: GoldenFormat::Jsonl,
        },
        GoldenCase {
            name: "claude_code",
            harness: "claude-code",
            source_name: "golden-claude-code",
            fixture_rel: "fixtures/claude-code/session.jsonl",
            source_file: "/fixtures/claude-code/session.jsonl",
            format: GoldenFormat::Jsonl,
        },
        GoldenCase {
            name: "kimi_cli",
            harness: "kimi-cli",
            source_name: "golden-kimi-cli",
            fixture_rel: "fixtures/kimi-cli/wire.jsonl",
            source_file: "/fixtures/kimi-cli/wire.jsonl",
            format: GoldenFormat::Jsonl,
        },
        GoldenCase {
            name: "hermes_trajectory",
            harness: "hermes",
            source_name: "golden-hermes-trajectory",
            fixture_rel: "fixtures/hermes/trajectories/001.jsonl",
            source_file: "/fixtures/hermes/trajectories/001.jsonl",
            format: GoldenFormat::Jsonl,
        },
        GoldenCase {
            name: "hermes_session_json",
            harness: "hermes",
            source_name: "golden-hermes-session",
            fixture_rel: "fixtures/hermes/sessions/session_20260418_142200_live01.json",
            source_file: "/fixtures/hermes/sessions/session_20260418_142200_live01.json",
            format: GoldenFormat::HermesSessionJson,
        },
    ];

    for case in cases {
        let records = match case.format {
            GoldenFormat::Jsonl => normalize_jsonl(&case),
            GoldenFormat::HermesSessionJson => normalize_hermes_session_json(&case),
        };
        let actual = json!({
            "name": case.name,
            "harness": case.harness,
            "source_file": case.source_file,
            "records": records,
        });
        assert_or_update_golden(&golden_root().join(format!("{}.json", case.name)), &actual);
    }
}
