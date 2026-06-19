use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use moraine_ingest_core::model::NormalizedRecord;
use moraine_ingest_core::normalize::normalize_record;
use moraine_ingest_core::sqlite_poll::synthesize_cursor_sqlite_record;
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
    /// kv fixture rows ({"key", "value"} JSONL) fed through the production
    /// `synthesize_cursor_sqlite_record` synthesis, exactly as the
    /// `cursor_sqlite` poller does for changed `state.vscdb` rows.
    CursorSqlite,
}

const SCHEMA_SQL: &str = include_str!("../../../sql/001_schema.sql");
const TOKEN_SQL: &str = include_str!("../../../sql/014_harmonized_token_accounting.sql");

const EVENT_REQUIRED_STRING_FIELDS: &[&str] = &[
    "event_uid",
    "session_id",
    "session_date",
    "cwd",
    "source_name",
    "harness",
    "inference_provider",
    "source_file",
    "source_ref",
    "record_ts",
    "event_ts",
    "event_kind",
    "actor_kind",
    "payload_type",
    "op_kind",
    "op_status",
    "request_id",
    "trace_id",
    "item_id",
    "tool_call_id",
    "parent_tool_call_id",
    "origin_event_id",
    "origin_tool_call_id",
    "tool_name",
    "tool_phase",
    "agent_run_id",
    "agent_label",
    "coord_group_id",
    "coord_group_label",
    "model",
    "endpoint_kind",
    "service_tier",
    "text_content",
    "text_preview",
    "payload_json",
    "token_usage_json",
];

const EVENT_REQUIRED_U64_FIELDS: &[&str] = &[
    "source_inode",
    "source_generation",
    "source_line_no",
    "source_offset",
    "turn_index",
    "tool_error",
    "is_substream",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "latency_ms",
    "retry_count",
    "has_reasoning",
    "event_version",
];

const TOKEN_BUCKET_KEYS: &[&str] = &[
    "input_text",
    "output_text",
    "input_cache_read",
    "input_cache_write",
    "input_image",
    "output_image",
    "input_audio",
    "output_audio",
    "reasoning",
    "server_tool_use",
    "embedding_input_text",
    "embedding_input_image",
    "other",
];

const TOKEN_NATIVE_UNIT_KEYS: &[&str] = &[
    "input_image_pixels",
    "output_image_pixels",
    "input_audio_seconds",
    "output_audio_seconds",
    "input_images",
    "output_images",
];

fn golden_cases() -> [GoldenCase; 9] {
    [
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
            name: "opencode",
            harness: "opencode",
            source_name: "golden-opencode",
            fixture_rel: "fixtures/opencode/session.jsonl",
            source_file: "/fixtures/opencode/session.jsonl",
            format: GoldenFormat::Jsonl,
        },
        GoldenCase {
            name: "cursor",
            harness: "cursor",
            source_name: "golden-cursor",
            fixture_rel: "fixtures/cursor/projects/demo/agent-transcripts/11111111-2222-4333-8444-555555555555/11111111-2222-4333-8444-555555555555.jsonl",
            source_file: "/fixtures/cursor/projects/demo/agent-transcripts/11111111-2222-4333-8444-555555555555/11111111-2222-4333-8444-555555555555.jsonl",
            format: GoldenFormat::Jsonl,
        },
        GoldenCase {
            name: "cursor_sqlite",
            harness: "cursor",
            source_name: "golden-cursor-sqlite",
            fixture_rel: "fixtures/cursor/state-vscdb-kv.jsonl",
            // The poller's source_file is the canonical database path.
            source_file: "/fixtures/cursor/User/globalStorage/state.vscdb",
            format: GoldenFormat::CursorSqlite,
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
        GoldenCase {
            name: "pi_coding_agent",
            harness: "pi-coding-agent",
            source_name: "golden-pi",
            fixture_rel: "fixtures/pi/session.jsonl",
            source_file: "/fixtures/pi/session.jsonl",
            format: GoldenFormat::Jsonl,
        },
    ]
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

fn normalize_jsonl(case: &GoldenCase) -> Vec<NormalizedRecord> {
    let path = fixture_path(case.fixture_rel);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()));
    let mut offset = 0_u64;
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut cwd_hint = String::new();
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
            &cwd_hint,
        )
        .unwrap_or_else(|err| panic!("normalize {} line {line_no}: {err:#}", case.name));
        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        cwd_hint = normalized.cwd_hint.clone();
        records.push(normalized);
    }

    records
}

fn normalize_hermes_session_json(case: &GoldenCase) -> Vec<NormalizedRecord> {
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
    let mut cwd_hint = String::new();
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
            &cwd_hint,
        )
        .unwrap_or_else(|err| panic!("normalize {} pseudo-line {line_no}: {err:#}", case.name));
        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        cwd_hint = normalized.cwd_hint.clone();
        records.push(normalized);
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

fn normalize_case(case: &GoldenCase) -> Vec<NormalizedRecord> {
    match case.format {
        GoldenFormat::Jsonl => normalize_jsonl(case),
        GoldenFormat::HermesSessionJson => normalize_hermes_session_json(case),
        GoldenFormat::CursorSqlite => normalize_cursor_sqlite(case),
    }
}

/// Mirrors `sqlite_poll::process_cursor_sqlite_db`: each fixture row is one
/// `cursorDiskKV` entry; synthesis and the stable source coordinates come
/// from the production code, so this golden cannot drift from the poller.
fn normalize_cursor_sqlite(case: &GoldenCase) -> Vec<NormalizedRecord> {
    let path = fixture_path(case.fixture_rel);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()));

    let mut records = Vec::new();
    for (idx, line) in body.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_str(trimmed)
            .unwrap_or_else(|err| panic!("fixture {} line {}: {err}", path.display(), idx + 1));
        let key = row
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("fixture line {} is missing `key`", idx + 1));
        let value_bytes = serde_json::to_vec(row.get("value").unwrap_or(&Value::Null))
            .expect("serialize fixture value");

        let Some(synthetic) = synthesize_cursor_sqlite_record(key, &value_bytes) else {
            // Skipped rows (ghost composers, placeholder bubbles, out-of-scope
            // key families) appear in the golden as empty snapshots so scope
            // changes are visible in review.
            records.push(NormalizedRecord::default());
            continue;
        };

        let normalized = normalize_record(
            &synthetic.record,
            case.source_name,
            case.harness,
            case.source_file,
            1,
            1,
            synthetic.source_line_no,
            synthetic.source_offset,
            "",
            "",
            "",
        )
        .unwrap_or_else(|err| panic!("normalize {} kv row {}: {err:#}", case.name, idx + 1));
        records.push(normalized);
    }

    records
}

fn snapshot_record(record: &NormalizedRecord) -> Value {
    json!({
        "raw_rows": optional_row(record.raw_row.clone()),
        "event_rows": redact_rows(record.event_rows.clone()),
        "link_rows": redact_rows(record.link_rows.clone()),
        "tool_rows": redact_rows(record.tool_rows.clone()),
        "error_rows": redact_rows(record.error_rows.clone()),
        "session_hint": record.session_hint,
        "model_hint": record.model_hint,
        "cwd_hint": record.cwd_hint,
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

fn sql_domain(constraint_name: &str) -> HashSet<String> {
    let marker = format!("CONSTRAINT {constraint_name} CHECK");
    let after_marker = SCHEMA_SQL
        .split(&marker)
        .nth(1)
        .unwrap_or_else(|| panic!("missing SQL constraint {constraint_name}"));
    let after_in = after_marker
        .split_once("IN (")
        .unwrap_or_else(|| panic!("missing IN clause for {constraint_name}"))
        .1;
    let values = after_in
        .split_once(')')
        .unwrap_or_else(|| panic!("unterminated IN clause for {constraint_name}"))
        .0;

    values
        .split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            let quoted = trimmed.strip_prefix('\'')?.split_once('\'')?.0;
            Some(quoted.to_string())
        })
        .collect()
}

fn assert_known_harness_fixture_coverage(cases: &[GoldenCase]) {
    let case_harnesses = cases
        .iter()
        .map(|case| case.harness)
        .collect::<HashSet<_>>();
    let config_harnesses = moraine_config::KNOWN_INGEST_HARNESSES
        .iter()
        .copied()
        .collect::<HashSet<_>>();

    assert_eq!(
        case_harnesses, config_harnesses,
        "golden/contract fixtures must cover every known ingest harness"
    );
}

fn assert_token_keys_match_sql() {
    for key in TOKEN_BUCKET_KEYS {
        assert!(
            SCHEMA_SQL.contains(&format!("'{key}'")) && TOKEN_SQL.contains(&format!("'{key}'")),
            "token bucket key `{key}` must be present in schema and token migration SQL"
        );
    }
    for key in TOKEN_NATIVE_UNIT_KEYS {
        assert!(
            SCHEMA_SQL.contains(&format!("'{key}'")) && TOKEN_SQL.contains(&format!("'{key}'")),
            "token native-unit key `{key}` must be present in schema and token migration SQL"
        );
    }
}

fn assert_string(row: &Value, field: &str, context: &str) {
    assert!(
        row.get(field).and_then(Value::as_str).is_some(),
        "{context}.{field} must be a string, got {:?}",
        row.get(field)
    );
}

fn assert_u64(row: &Value, field: &str, context: &str) {
    assert!(
        row.get(field).and_then(Value::as_u64).is_some(),
        "{context}.{field} must be an unsigned integer, got {:?}",
        row.get(field)
    );
}

fn assert_string_array(row: &Value, field: &str, context: &str) {
    let values = row.get(field).and_then(Value::as_array).unwrap_or_else(|| {
        panic!(
            "{context}.{field} must be an array, got {:?}",
            row.get(field)
        )
    });
    for value in values {
        assert!(
            value.as_str().is_some(),
            "{context}.{field} must contain only strings, got {value:?}"
        );
    }
}

fn assert_required_event_shape(
    row: &Value,
    context: &str,
    endpoint_domain: &HashSet<String>,
    event_domain: &HashSet<String>,
    payload_domain: &HashSet<String>,
) {
    for field in EVENT_REQUIRED_STRING_FIELDS {
        assert_string(row, field, context);
    }
    for field in EVENT_REQUIRED_U64_FIELDS {
        assert_u64(row, field, context);
    }
    assert_string_array(row, "content_types", context);
    assert_numeric_map(
        row,
        "token_usage_buckets",
        TOKEN_BUCKET_KEYS,
        false,
        context,
    );
    assert_numeric_map(
        row,
        "token_usage_native_units",
        TOKEN_NATIVE_UNIT_KEYS,
        true,
        context,
    );

    assert_domain(row, "endpoint_kind", endpoint_domain, context);
    assert_domain(row, "event_kind", event_domain, context);
    assert_domain(row, "payload_type", payload_domain, context);
    assert_looks_like_uid(row, "event_uid", context);
}

fn assert_domain(row: &Value, field: &str, allowed: &HashSet<String>, context: &str) {
    let value = row
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{context}.{field} must be a string"));
    assert!(
        allowed.contains(value),
        "{context}.{field} `{value}` is outside SQL domain {allowed:?}"
    );
}

fn assert_numeric_map(
    row: &Value,
    field: &str,
    expected_keys: &[&str],
    allow_float: bool,
    context: &str,
) {
    let map = row
        .get(field)
        .and_then(Value::as_object)
        .unwrap_or_else(|| {
            panic!(
                "{context}.{field} must be an object, got {:?}",
                row.get(field)
            )
        });
    let actual_keys = map.keys().map(String::as_str).collect::<HashSet<_>>();
    let expected_keys = expected_keys.iter().copied().collect::<HashSet<_>>();
    assert_eq!(actual_keys, expected_keys, "{context}.{field} keys drifted");
    for (key, value) in map {
        let numeric = value.as_u64().is_some() || (allow_float && value.as_f64().is_some());
        assert!(
            numeric,
            "{context}.{field}.{key} must be numeric, got {value:?}"
        );
    }
}

fn assert_looks_like_uid(row: &Value, field: &str, context: &str) {
    let uid = row
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{context}.{field} must be a string"));
    assert_eq!(uid.len(), 64, "{context}.{field} must be a sha256 hex uid");
    assert!(
        uid.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{context}.{field} must be hex, got {uid}"
    );
}

fn assert_raw_row_shape(row: &Value, case: &GoldenCase, context: &str) {
    for field in [
        "source_name",
        "harness",
        "inference_provider",
        "cwd",
        "source_file",
        "record_ts",
        "top_type",
        "session_id",
        "raw_json",
        "event_uid",
    ] {
        assert_string(row, field, context);
    }
    for field in [
        "source_inode",
        "source_generation",
        "source_line_no",
        "source_offset",
        "raw_json_hash",
    ] {
        assert_u64(row, field, context);
    }
    assert_eq!(
        row.get("harness").and_then(Value::as_str),
        Some(case.harness)
    );
    assert_eq!(
        row.get("source_name").and_then(Value::as_str),
        Some(case.source_name)
    );
    assert_eq!(
        row.get("source_file").and_then(Value::as_str),
        Some(case.source_file)
    );
    assert_looks_like_uid(row, "event_uid", context);
}

fn assert_link_row_shape(
    row: &Value,
    context: &str,
    link_domain: &HashSet<String>,
    event_uids: &HashSet<String>,
) {
    for field in [
        "event_uid",
        "linked_event_uid",
        "linked_external_id",
        "link_type",
        "session_id",
        "harness",
        "inference_provider",
        "source_name",
        "metadata_json",
    ] {
        assert_string(row, field, context);
    }
    assert_u64(row, "event_version", context);
    assert_domain(row, "link_type", link_domain, context);

    let event_uid = row.get("event_uid").and_then(Value::as_str).unwrap();
    assert!(
        event_uids.contains(event_uid),
        "{context}.event_uid `{event_uid}` must point at an emitted event"
    );
    let linked_event_uid = row.get("linked_event_uid").and_then(Value::as_str).unwrap();
    let linked_external_id = row
        .get("linked_external_id")
        .and_then(Value::as_str)
        .unwrap();
    if linked_event_uid.is_empty() {
        assert!(
            !linked_external_id.is_empty(),
            "{context} must carry either linked_event_uid or linked_external_id"
        );
    } else {
        assert!(
            linked_external_id.is_empty(),
            "{context} event links must not also carry an external id"
        );
        assert!(
            event_uids.contains(linked_event_uid),
            "{context}.linked_event_uid `{linked_event_uid}` must point at an emitted event"
        );
    }
}

fn assert_tool_row_shape(row: &Value, context: &str, event_uids: &HashSet<String>) {
    for field in [
        "event_uid",
        "session_id",
        "harness",
        "inference_provider",
        "source_name",
        "tool_call_id",
        "parent_tool_call_id",
        "tool_name",
        "tool_phase",
        "input_json",
        "output_json",
        "output_text",
        "input_preview",
        "output_preview",
        "source_ref",
    ] {
        assert_string(row, field, context);
    }
    for field in [
        "tool_error",
        "input_bytes",
        "output_bytes",
        "io_hash",
        "event_version",
    ] {
        assert_u64(row, field, context);
    }
    let event_uid = row.get("event_uid").and_then(Value::as_str).unwrap();
    assert!(
        event_uids.contains(event_uid),
        "{context}.event_uid `{event_uid}` must point at an emitted event"
    );
    let phase = row.get("tool_phase").and_then(Value::as_str).unwrap();
    assert!(
        matches!(phase, "request" | "response"),
        "{context}.tool_phase must be request or response, got `{phase}`"
    );
}

fn assert_error_row_shape(row: &Value, case: &GoldenCase, context: &str) {
    for field in [
        "source_name",
        "harness",
        "inference_provider",
        "source_file",
        "error_kind",
        "error_text",
        "raw_fragment",
    ] {
        assert_string(row, field, context);
    }
    for field in [
        "source_inode",
        "source_generation",
        "source_line_no",
        "source_offset",
    ] {
        assert_u64(row, field, context);
    }
    assert_eq!(
        row.get("harness").and_then(Value::as_str),
        Some(case.harness)
    );
}

fn assert_tool_phase_pairing(records: &[NormalizedRecord], case: &GoldenCase) {
    let mut phases_by_call = HashMap::<String, HashSet<String>>::new();
    for record in records {
        for row in &record.tool_rows {
            let tool_call_id = row
                .get("tool_call_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if tool_call_id.is_empty() {
                continue;
            }
            let phase = row
                .get("tool_phase")
                .and_then(Value::as_str)
                .unwrap_or_default();
            phases_by_call
                .entry(tool_call_id.to_string())
                .or_default()
                .insert(phase.to_string());
        }
    }

    for (tool_call_id, phases) in phases_by_call {
        if phases.contains("response") {
            assert!(
                phases.contains("request"),
                "{} tool_call_id `{tool_call_id}` has a response without a request",
                case.name
            );
        }
    }
}

fn assert_source_contract(
    case: &GoldenCase,
    records: &[NormalizedRecord],
    endpoint_domain: &HashSet<String>,
    event_domain: &HashSet<String>,
    payload_domain: &HashSet<String>,
    link_domain: &HashSet<String>,
) {
    let mut saw_raw = false;
    let mut saw_event = false;

    for (record_idx, record) in records.iter().enumerate() {
        let context = format!("{} record {record_idx}", case.name);
        let event_uids = record
            .event_rows
            .iter()
            .map(|row| {
                row.get("event_uid")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| panic!("{context} event row missing event_uid"))
                    .to_string()
            })
            .collect::<HashSet<_>>();

        if !record.raw_row.is_null() {
            saw_raw = true;
            assert_raw_row_shape(&record.raw_row, case, &format!("{context}.raw_row"));
        } else {
            assert!(
                record.event_rows.is_empty(),
                "{context} skipped raw rows must not emit events"
            );
        }

        for (row_idx, row) in record.event_rows.iter().enumerate() {
            saw_event = true;
            let row_context = format!("{context}.event_rows[{row_idx}]");
            assert_required_event_shape(
                row,
                &row_context,
                endpoint_domain,
                event_domain,
                payload_domain,
            );
            assert_eq!(
                row.get("harness").and_then(Value::as_str),
                Some(case.harness)
            );
            assert_eq!(
                row.get("source_name").and_then(Value::as_str),
                Some(case.source_name)
            );
            assert_eq!(
                row.get("source_file").and_then(Value::as_str),
                Some(case.source_file)
            );
        }

        for (row_idx, row) in record.link_rows.iter().enumerate() {
            assert_link_row_shape(
                row,
                &format!("{context}.link_rows[{row_idx}]"),
                link_domain,
                &event_uids,
            );
        }

        for (row_idx, row) in record.tool_rows.iter().enumerate() {
            assert_tool_row_shape(row, &format!("{context}.tool_rows[{row_idx}]"), &event_uids);
        }

        for (row_idx, row) in record.error_rows.iter().enumerate() {
            assert_error_row_shape(row, case, &format!("{context}.error_rows[{row_idx}]"));
        }
    }

    assert!(saw_raw, "{} must exercise at least one raw row", case.name);
    assert!(
        saw_event,
        "{} must exercise at least one normalized event row",
        case.name
    );
    assert_tool_phase_pairing(records, case);
}

#[test]
fn source_normalization_golden_fixtures_are_stable() {
    // Only `event_version` is redacted. UIDs, source refs, timestamps, token
    // maps, links, tool phases, model/provider fields, hints, and row counts
    // remain part of the committed snapshot.
    let cases = golden_cases();
    assert_known_harness_fixture_coverage(&cases);

    for case in cases {
        let records = normalize_case(&case);
        let snapshot_records = records.iter().map(snapshot_record).collect::<Vec<_>>();
        let actual = json!({
            "name": case.name,
            "harness": case.harness,
            "source_file": case.source_file,
            "records": snapshot_records,
        });
        assert_or_update_golden(&golden_root().join(format!("{}.json", case.name)), &actual);
    }
}

#[test]
fn source_normalization_fixtures_satisfy_schema_contracts() {
    let cases = golden_cases();
    assert_known_harness_fixture_coverage(&cases);
    assert_token_keys_match_sql();

    let endpoint_domain = sql_domain("events_endpoint_kind_domain");
    let event_domain = sql_domain("events_event_kind_domain");
    let payload_domain = sql_domain("events_payload_type_domain");
    let link_domain = sql_domain("event_links_link_type_domain");

    for case in cases {
        let records = normalize_case(&case);
        assert_source_contract(
            &case,
            &records,
            &endpoint_domain,
            &event_domain,
            &payload_domain,
            &link_domain,
        );
    }
}
