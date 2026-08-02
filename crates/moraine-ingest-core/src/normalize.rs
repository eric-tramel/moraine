use crate::model::{NormalizedRecord, RowBatch};
use crate::sources::shared::{
    compact_json, event_uid, infer_rollout_record_ts_from_file, parse_event_ts, raw_hash,
    resolve_model_hint, truncate_chars, RecordContext, UNPARSEABLE_EVENT_TS,
};
use crate::sources::{registry, NormalizedPartials, Preflight, SourceRecordContext};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write as _;

pub use crate::sources::shared::{infer_session_date_from_file, infer_session_id_from_file};

#[allow(clippy::too_many_arguments)]
pub fn normalize_record(
    record: &Value,
    source_name: &str,
    harness: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
    model_hint: &str,
    cwd_hint: &str,
) -> Result<NormalizedRecord> {
    normalize_record_with_ts_hint(
        record,
        source_name,
        harness,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        session_hint,
        model_hint,
        cwd_hint,
        "",
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn normalize_record_with_ts_hint(
    record: &Value,
    source_name: &str,
    harness: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
    model_hint: &str,
    cwd_hint: &str,
    record_ts_hint: &str,
) -> Result<NormalizedRecord> {
    let sources = registry();
    let source = if sources.is_known(harness) {
        sources
            .get(harness)
            .expect("known source should resolve to a registered source")
    } else {
        return Err(anyhow!(
            "unsupported harness `{}`; expected one of: {}",
            harness.trim(),
            sources.known_harnesses().join(", ")
        ));
    };

    let record = match source.preflight(record) {
        Preflight::Keep(record) => record,
        Preflight::Skip => return Ok(NormalizedRecord::default()),
    };

    let harness_name = source.harness();
    let metadata = source.source_metadata(record);
    let source_record_ts = source.record_ts(record);
    let record_ts = resolve_record_ts(harness_name, source_file, &source_record_ts, record_ts_hint);
    let (event_ts, event_ts_parse_failed) = parse_event_ts(&record_ts);
    let top_type = source.top_type(record);
    let session_date = infer_session_date_from_file(source_file, &record_ts);

    let raw_json = compact_json(record);
    let base_uid = event_uid(
        source_file,
        source_generation,
        source_line_no,
        source_offset,
        &raw_json,
        "raw",
    );

    let source_ctx = SourceRecordContext {
        source_name,
        source_file,
        session_hint,
        top_type: &top_type,
        base_uid: &base_uid,
    };
    let session_id = source.session_id(record, &source_ctx);
    let cwd = resolve_cwd(&source.cwd(record), cwd_hint);

    let raw_row = json!({
        "source_name": source_name,
        "harness": harness_name,
        "inference_provider": metadata.inference_provider,
        "cwd": cwd,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "record_ts": record_ts,
        "top_type": top_type,
        "session_id": session_id,
        "raw_json": raw_json,
        "raw_json_hash": raw_hash(&raw_json),
        "event_uid": base_uid,
    });

    let mut error_rows = Vec::<Value>::new();
    if event_ts_parse_failed {
        error_rows.push(json!({
            "source_name": source_name,
            "harness": harness_name,
            "inference_provider": metadata.inference_provider,
            "source_file": source_file,
            "source_inode": source_inode,
            "source_generation": source_generation,
            "source_line_no": source_line_no,
            "source_offset": source_offset,
            "error_kind": "timestamp_parse_error",
            "error_text": format!(
                "timestamp is missing or not supported ISO8601/RFC3339; used {} UTC fallback",
                UNPARSEABLE_EVENT_TS
            ),
            "raw_fragment": truncate_chars(&record_ts, 20_000),
        }));
    }

    let ctx = RecordContext {
        source_name,
        harness: harness_name,
        inference_provider: &metadata.inference_provider,
        session_id: &session_id,
        session_hint,
        session_date: &session_date,
        cwd: &cwd,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        record_ts: &record_ts,
        event_ts: &event_ts,
    };

    let mut partials = source.normalize(record, &ctx, &top_type, &base_uid, model_hint);
    let folded_identity = fold_tool_payloads_into_events(&mut partials)?;
    finalize_event_identities(&mut partials, &folded_identity)?;
    let hint_fallback = if metadata.model_hint_fallback.is_empty() {
        model_hint
    } else {
        metadata.model_hint_fallback.as_str()
    };
    let model_hint = resolve_model_hint(&partials.event_rows, harness_name, hint_fallback);

    Ok(NormalizedRecord {
        raw_row,
        event_rows: partials.event_rows,
        link_rows: partials.link_rows,
        tool_rows: partials.tool_rows,
        error_rows,
        session_hint: session_id,
        model_hint,
        cwd_hint: cwd,
    })
}

/// Preserve tool request/response detail on its canonical event. `tool_rows`
/// remain in the normalized result for adapter/redaction compatibility, but
/// the sink no longer persists the retired `tool_io` relation.
struct FoldedEventIdentity {
    semantic_payload: String,
    tool_fields: Vec<String>,
}

fn fold_tool_payloads_into_events(
    partials: &mut NormalizedPartials,
) -> Result<HashMap<String, FoldedEventIdentity>> {
    let event_indexes = partials
        .event_rows
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            event
                .get("event_uid")
                .and_then(Value::as_str)
                .map(|uid| (uid.to_string(), index))
        })
        .collect::<HashMap<_, _>>();
    let mut identities = HashMap::with_capacity(partials.tool_rows.len());

    for tool in &partials.tool_rows {
        let tool_object = tool
            .as_object()
            .ok_or_else(|| anyhow!("normalized tool_io row is not an object"))?;
        let event_uid = tool_object
            .get("event_uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.is_empty())
            .ok_or_else(|| anyhow!("normalized tool_io row is missing a nonempty event_uid"))?;
        let event_index = event_indexes.get(event_uid).copied().ok_or_else(|| {
            anyhow!("normalized tool_io row references event_uid `{event_uid}` without an owner")
        })?;
        let event = partials.event_rows[event_index]
            .as_object_mut()
            .ok_or_else(|| anyhow!("normalized event `{event_uid}` is not an object"))?;

        let source_payload = event
            .get("payload_json")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| {
                serde_json::from_str::<Value>(value)
                    .unwrap_or_else(|_| Value::String(value.to_string()))
            })
            .unwrap_or(Value::Null);
        let mut canonical_payload = match source_payload {
            Value::Object(object) => object,
            value => {
                let mut object = serde_json::Map::new();
                if !value.is_null() {
                    object.insert("source_payload".to_string(), value);
                }
                object
            }
        };
        let tool_payload = tool_object
            .iter()
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "event_uid"
                        | "event_version"
                        | "session_id"
                        | "source_name"
                        | "harness"
                        | "record_ts"
                )
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<serde_json::Map<_, _>>();
        canonical_payload.insert("moraine_tool_io".to_string(), Value::Object(tool_payload));
        let mut semantic_payload = Value::Object(canonical_payload.clone());
        sanitize_semantic_payload(&mut semantic_payload);
        let semantic_payload =
            serde_json::to_string(&semantic_payload).unwrap_or_else(|_| "{}".to_string());
        let tool_fields = TOOL_IDENTITY_FIELDS
            .iter()
            .map(|field| encode_identity_value(tool_object.get(*field)))
            .collect();
        identities.insert(
            event_uid.to_string(),
            FoldedEventIdentity {
                semantic_payload,
                tool_fields,
            },
        );
        for key in [
            "tool_call_id",
            "parent_tool_call_id",
            "tool_name",
            "tool_phase",
            "tool_error",
        ] {
            if let Some(value) = tool_object.get(key) {
                event.insert(key.to_string(), value.clone());
            }
        }
        for key in ["project_id", "repo_rel_path", "worktree_root"] {
            if let Some(value) = tool_object
                .get(key)
                .filter(|value| value.as_str().is_some_and(|text| !text.is_empty()))
            {
                event.insert(key.to_string(), value.clone());
            }
        }
        event.insert(
            "payload_json".to_string(),
            Value::String(Value::Object(canonical_payload).to_string()),
        );
    }
    Ok(identities)
}
const EVENT_IDENTITY_DOMAIN: &str = "moraine:event:v2";
const EVENT_IDENTITY_FIELDS: &[&str] = &[
    "author",
    "harness",
    "inference_provider",
    "session_id",
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
    "origin_tool_call_id",
    "tool_name",
    "tool_phase",
    "tool_error",
    "agent_run_id",
    "agent_label",
    "coord_group_id",
    "coord_group_label",
    "is_substream",
    "model",
    "endpoint_kind",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "latency_ms",
    "retry_count",
    "service_tier",
    "content_types",
    "has_reasoning",
    "text_content",
];
const TOOL_IDENTITY_FIELDS: &[&str] = &[
    "tool_call_id",
    "parent_tool_call_id",
    "tool_name",
    "tool_phase",
    "tool_error",
    "input_json",
    "output_json",
    "output_text",
];
const SEMANTIC_PAYLOAD_EXCLUDED_FIELDS: &[&str] = &[
    "createdAt",
    "created_at",
    "cwd",
    "directory",
    "lastUpdatedAt",
    "last_updated",
    "moraine_tool_io",
    "moraine_emission_index",
    "project_id",
    "repo_rel_path",
    "request_event_uid",
    "session_start",
    "source_ref",
    "time_created",
    "timestamp",
    "updated_at",
    "workspacePath",
    "worktree_root",
];

/// Replaces adapter-local provisional keys only after every canonical field and
/// folded tool payload is present, then rewrites same-record event references.
fn finalize_event_identities(
    partials: &mut NormalizedPartials,
    folded_identity: &HashMap<String, FoldedEventIdentity>,
) -> Result<()> {
    finalize_identity_rows(
        &mut partials.event_rows,
        &mut partials.link_rows,
        &mut partials.tool_rows,
        |old_uid, event| semantic_event_uid(event, folded_identity.get(old_uid)),
    )
}

/// Recomputes canonical identity after sink-specific enrichment and redaction,
/// then rewrites internal references using identities finalized for this source
/// scan, including dependencies emitted in an earlier bounded chunk.
pub(crate) fn finalize_batch_event_identities(
    batch: &mut RowBatch,
    uid_map: &mut HashMap<String, String>,
) -> Result<()> {
    finalize_identity_rows_with_map(
        &mut batch.event_rows,
        &mut batch.link_rows,
        &mut batch.tool_rows,
        uid_map,
        |_, event| semantic_event_uid(event, None),
    )?;
    Ok(())
}

fn finalize_identity_rows<F>(
    event_rows: &mut [Value],
    link_rows: &mut [Value],
    tool_rows: &mut [Value],
    final_uid: F,
) -> Result<()>
where
    F: FnMut(&str, &Value) -> Result<String>,
{
    let mut uid_map = HashMap::with_capacity(event_rows.len());
    finalize_identity_rows_with_map(event_rows, link_rows, tool_rows, &mut uid_map, final_uid)
}

fn finalize_identity_rows_with_map<F>(
    event_rows: &mut [Value],
    link_rows: &mut [Value],
    tool_rows: &mut [Value],
    uid_map: &mut HashMap<String, String>,
    mut final_uid: F,
) -> Result<()>
where
    F: FnMut(&str, &Value) -> Result<String>,
{
    uid_map.reserve(event_rows.len());
    for event in event_rows.iter() {
        let old_uid = event
            .get("event_uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.is_empty())
            .ok_or_else(|| anyhow!("normalized event is missing a nonempty event_uid"))?;
        uid_map.insert(old_uid.to_string(), final_uid(old_uid, event)?);
    }

    for event in event_rows {
        rewrite_uid_field(event, "event_uid", uid_map);
        rewrite_uid_field(event, "origin_event_id", uid_map);
    }
    for tool in tool_rows {
        rewrite_uid_field(tool, "event_uid", uid_map);
    }
    for link in link_rows {
        rewrite_uid_field(link, "event_uid", uid_map);
        rewrite_uid_field(link, "linked_event_uid", uid_map);
    }
    Ok(())
}

fn rewrite_uid_field(row: &mut Value, field: &str, uid_map: &HashMap<String, String>) {
    let Some(object) = row.as_object_mut() else {
        return;
    };
    let Some(old_uid) = object.get(field).and_then(Value::as_str) else {
        return;
    };
    if let Some(new_uid) = uid_map.get(old_uid) {
        object.insert(field.to_string(), Value::String(new_uid.clone()));
    }
}

fn semantic_event_uid(
    event: &Value,
    folded_identity: Option<&FoldedEventIdentity>,
) -> Result<String> {
    let object = event
        .as_object()
        .ok_or_else(|| anyhow!("normalized event is not an object"))?;
    let mut hasher = Sha256::new();
    hash_identity_field(&mut hasher, EVENT_IDENTITY_DOMAIN.as_bytes());
    for field in EVENT_IDENTITY_FIELDS {
        hash_json_identity_value(&mut hasher, object.get(*field));
    }
    hash_numeric_map_identity(&mut hasher, object.get("token_usage_buckets"), |value| {
        value.as_u64().unwrap_or(0).to_string()
    });
    hash_numeric_map_identity(
        &mut hasher,
        object.get("token_usage_native_units"),
        |value| value.as_f64().unwrap_or(0.0).to_string(),
    );
    if let Some(identity) = folded_identity {
        hash_identity_field(&mut hasher, identity.semantic_payload.as_bytes());
        for field in &identity.tool_fields {
            hash_identity_field(&mut hasher, field.as_bytes());
        }
    } else {
        let (payload, tool_fields) = semantic_payload_parts(object.get("payload_json"));
        hash_identity_field(&mut hasher, payload.as_bytes());
        for field in tool_fields {
            hash_identity_field(&mut hasher, field.as_bytes());
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn hash_identity_field(hasher: &mut Sha256, value: &[u8]) {
    let mut digits = [0_u8; 20];
    let mut cursor = digits.len();
    let mut length = value.len();
    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (length % 10) as u8;
        length /= 10;
        if length == 0 {
            break;
        }
    }
    hasher.update(&digits[cursor..]);
    hasher.update(b":");
    hasher.update(value);
}

fn hash_json_identity_value(hasher: &mut Sha256, value: Option<&Value>) {
    match value {
        Some(Value::String(value)) => hash_identity_field(hasher, value.as_bytes()),
        Some(value) => {
            let encoded = compact_json(value);
            hash_identity_field(hasher, encoded.as_bytes());
        }
        None => hash_identity_field(hasher, b""),
    }
}

fn hash_numeric_map_identity<F>(hasher: &mut Sha256, value: Option<&Value>, encode_value: F)
where
    F: Fn(&Value) -> String,
{
    let Some(values) = value.and_then(Value::as_object) else {
        hash_identity_field(hasher, b"");
        return;
    };
    let mut keys = values.keys().collect::<Vec<_>>();
    keys.sort_unstable();
    let mut encoded = String::with_capacity(values.len() * 24);
    for key in keys {
        append_identity_component(&mut encoded, key);
        let value = encode_value(&values[key]);
        append_identity_component(&mut encoded, &value);
    }
    hash_identity_field(hasher, encoded.as_bytes());
}

fn append_identity_component(output: &mut String, value: &str) {
    write!(output, "{}:", value.len()).expect("writing to String cannot fail");
    output.push_str(value);
}

fn encode_identity_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(value) => compact_json(value),
        None => String::new(),
    }
}

fn semantic_payload_parts(value: Option<&Value>) -> (String, Vec<String>) {
    let Some(payload) = value.and_then(Value::as_str) else {
        return (
            String::new(),
            vec![String::new(); TOOL_IDENTITY_FIELDS.len()],
        );
    };
    let Ok(mut parsed) = serde_json::from_str::<Value>(payload) else {
        return (
            payload.to_string(),
            vec![String::new(); TOOL_IDENTITY_FIELDS.len()],
        );
    };
    let tool = parsed
        .get("moraine_tool_io")
        .and_then(Value::as_object)
        .cloned();
    sanitize_semantic_payload(&mut parsed);
    let tool_fields = TOOL_IDENTITY_FIELDS
        .iter()
        .map(|field| encode_identity_value(tool.as_ref().and_then(|tool| tool.get(*field))))
        .collect();
    (compact_json(&parsed), tool_fields)
}

fn sanitize_semantic_payload(payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    for field in SEMANTIC_PAYLOAD_EXCLUDED_FIELDS {
        object.remove(*field);
    }
}
/// Record-level cwd wins; otherwise fall back to the session-level hint
/// chained in by the caller. Whitespace-only values count as absent.
fn resolve_cwd(record_cwd: &str, cwd_hint: &str) -> String {
    let trimmed = record_cwd.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }

    cwd_hint.trim().to_string()
}

fn resolve_record_ts(
    harness: &str,
    source_file: &str,
    source_record_ts: &str,
    record_ts_hint: &str,
) -> String {
    let trimmed = source_record_ts.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }

    let hint = record_ts_hint.trim();
    if !hint.is_empty() {
        return hint.to_string();
    }

    if harness == "codex" {
        if let Some(file_ts) = infer_rollout_record_ts_from_file(source_file) {
            return file_ts;
        }
    }

    String::new()
}

#[cfg(test)]
mod tests {
    use super::{finalize_batch_event_identities, normalize_record, semantic_event_uid};
    use crate::model::RowBatch;
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;

    #[test]
    fn semantic_uid_matches_the_migration_vector_and_ignores_derived_payload_values() {
        let mut event = json!({
            "author": "",
            "harness": "nac",
            "inference_provider": "",
            "session_id": "replay-fixture-session",
            "event_kind": "tool_call",
            "actor_kind": "assistant",
            "payload_type": "tool_use",
            "op_kind": "",
            "op_status": "",
            "request_id": "",
            "trace_id": "",
            "item_id": "",
            "tool_call_id": "call-stable",
            "parent_tool_call_id": "",
            "origin_tool_call_id": "",
            "tool_name": "Read",
            "tool_phase": "request",
            "tool_error": 0,
            "agent_run_id": "",
            "agent_label": "",
            "coord_group_id": "",
            "coord_group_label": "",
            "is_substream": 0,
            "model": "",
            "endpoint_kind": "generation",
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
            "latency_ms": 0,
            "retry_count": 0,
            "service_tier": "",
            "content_types": [],
            "has_reasoning": 0,
            "text_content": "read src/lib.rs",
            "payload_json": "{\"logical_id\":\"request-stable\",\"type\":\"tool_request\"}",
            "token_usage_buckets": {},
            "token_usage_native_units": {}
        });
        const EXPECTED: &str = "da350fd4a9138ce5528da94d85c2c2753a741cb16617ec3c2fe4a86754166f4a";
        assert_eq!(
            semantic_event_uid(&event, None).expect("base uid"),
            EXPECTED
        );

        let without_excluded_fields =
            semantic_event_uid(&event, None).expect("absent metadata uid");
        event["payload_json"] = json!(
            "{\"moraine_emission_index\":1,\"timestamp\":\"2026-01-01T00:00:00Z\",\"type\":\"tool_request\",\"logical_id\":\"request-stable\"}"
        );
        assert_eq!(
            semantic_event_uid(&event, None).expect("present metadata uid"),
            without_excluded_fields,
            "excluded metadata must hash identically whether absent or present"
        );

        event["session_id"] = json!("replay-stable-session");
        event["text_content"] = json!("read src/lib.rs");
        event["token_usage_buckets"] = json!({
            "embedding_input_image": 0, "embedding_input_text": 0,
            "input_audio": 0, "input_cache_read": 0, "input_cache_write": 0,
            "input_image": 0, "input_text": 0, "other": 0,
            "output_audio": 0, "output_image": 0, "output_text": 0,
            "reasoning": 0, "server_tool_use": 0
        });
        event["token_usage_native_units"] = json!({
            "input_audio_seconds": 0.0, "input_image_pixels": 0.0, "input_images": 0.0,
            "output_audio_seconds": 0.0, "output_image_pixels": 0.0, "output_images": 0.0
        });
        event["payload_json"] = json!(
            "{\"logical_id\":\"request-stable\",\"moraine_tool_io\":{\"tool_call_id\":\"call-stable\",\"parent_tool_call_id\":\"\",\"tool_name\":\"Read\",\"tool_phase\":\"request\",\"tool_error\":0,\"input_json\":\"\",\"output_json\":\"\",\"output_text\":\"\",\"project_id\":\"git:first\",\"repo_rel_path\":\"src/lib.rs\",\"worktree_root\":\"/old\",\"source_ref\":\"/old/store.db:1:10\"},\"type\":\"tool_request\"}"
        );
        assert_eq!(
            semantic_event_uid(&event, None).expect("schema-033 migration vector"),
            "9e4f03ed718cc58482613c6cea7f65435f15cd26c46c87291a16431ad310c3f6"
        );
        event["session_id"] = json!("replay-fixture-session");
        event["token_usage_buckets"] = json!({});
        event["token_usage_native_units"] = json!({});
        event["payload_json"] = json!(
            "{\"logical_id\":\"request-stable\",\"moraine_tool_io\":{\"project_id\":\"git:first\",\"source_ref\":\"/old/store.db:1:10\",\"worktree_root\":\"/old\"},\"request_event_uid\":\"old-request-1\",\"type\":\"tool_request\"}"
        );
        let first = semantic_event_uid(&event, None).expect("first derived payload uid");
        event["payload_json"] = json!(
            "{\"logical_id\":\"request-stable\",\"moraine_tool_io\":{\"project_id\":\"git:second\",\"source_ref\":\"/new/store.db:2:40\",\"worktree_root\":\"/new\"},\"request_event_uid\":\"old-request-2\",\"type\":\"tool_request\"}"
        );
        assert_eq!(
            semantic_event_uid(&event, None).expect("second derived payload uid"),
            first
        );

        event["author"] = json!("é");
        event["session_id"] = json!("séssion:|\0");
        event["text_content"] = json!("λ:🙂\0|");
        event["payload_json"] = json!("{\"message\":\"雪:|\\u0000\"}");
        event["content_types"] = json!(["text", "reasoning"]);
        event["input_tokens"] = json!(12);
        event["token_usage_buckets"] = json!({"input_text": 12, "reasoning": 3});
        event["token_usage_native_units"] = json!({"input_images": 1.5});
        const UNICODE_EXPECTED: &str =
            "1f01770f4e1c2c55b629e5a0f5b4aafbecc2995eff18d8905ce0eb765c4e70a1";
        assert_eq!(
            semantic_event_uid(&event, None).expect("Unicode and delimiter UID"),
            UNICODE_EXPECTED
        );
        event
            .as_object_mut()
            .expect("event object")
            .remove("inference_provider");
        assert_eq!(
            semantic_event_uid(&event, None).expect("absent field UID"),
            UNICODE_EXPECTED,
            "absent and empty optional scalar fields share one encoding"
        );
    }
    #[test]
    fn semantic_uid_preimage_contract_is_pinned_independently() {
        let expected_preimage = concat!(
            "16:moraine:event:v20:3:nac0:22:replay-fixture-session9:tool_call",
            "9:assistant8:tool_use0:0:0:0:0:11:call-stable0:0:4:Read7:request",
            "1:00:0:0:0:1:00:10:generation1:01:01:01:01:01:00:2:[]1:0",
            "15:read src/lib.rs0:0:53:{\"logical_id\":\"request-stable\",",
            "\"type\":\"tool_request\"}0:0:0:0:0:0:0:0:"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(expected_preimage.as_bytes())),
            "da350fd4a9138ce5528da94d85c2c2753a741cb16617ec3c2fe4a86754166f4a"
        );
    }

    #[test]
    fn semantic_uid_ignores_source_location_payload_metadata() {
        let base = json!({
            "event_uid": "provisional",
            "harness": "codex",
            "session_id": "stable-session",
            "event_kind": "message",
            "actor_kind": "user",
            "payload_type": "text",
            "text_content": "stable content",
            "payload_json": "{\"cwd\":\"/old/worktree\",\"message\":\"stable content\",\"source_ref\":\"/old/session.jsonl:1:0\",\"timestamp\":\"2026-01-01T00:00:00Z\"}"
        });
        let mut relocated = base.clone();
        relocated["payload_json"] = json!(
            "{\"cwd\":\"/new/worktree\",\"message\":\"stable content\",\"source_ref\":\"/new/session.jsonl:99:400\",\"timestamp\":\"2026-07-01T00:00:00Z\"}"
        );
        assert_eq!(
            semantic_event_uid(&base, None).expect("base semantic UID"),
            semantic_event_uid(&relocated, None).expect("relocated semantic UID")
        );
    }

    #[test]
    fn sink_finalization_rekeys_redacted_enriched_events_and_cross_batch_references() {
        let request = json!({
            "event_uid": "request-provisional",
            "author": "team",
            "harness": "nac",
            "session_id": "stable-session",
            "event_kind": "tool_call",
            "actor_kind": "assistant",
            "payload_type": "tool_use",
            "tool_call_id": "call-1",
            "tool_name": "Read",
            "tool_phase": "request",
            "latency_ms": 17,
            "text_content": "[REDACTED]",
            "payload_json": "{\"message\":\"[REDACTED]\",\"moraine_tool_io\":{\"input_json\":\"{\\\"path\\\":\\\"[REDACTED]\\\"}\",\"tool_call_id\":\"call-1\",\"tool_name\":\"Read\",\"tool_phase\":\"request\"}}"
        });
        let response = json!({
            "event_uid": "response-provisional",
            "origin_event_id": "request-provisional",
            "author": "team",
            "harness": "nac",
            "session_id": "stable-session",
            "event_kind": "tool_result",
            "actor_kind": "tool",
            "payload_type": "tool_result",
            "tool_call_id": "call-1",
            "tool_name": "Read",
            "tool_phase": "response",
            "latency_ms": 19,
            "text_content": "[REDACTED]",
            "payload_json": "{\"request_event_uid\":\"request-provisional\",\"moraine_tool_io\":{\"output_text\":\"[REDACTED]\",\"tool_call_id\":\"call-1\",\"tool_name\":\"Read\",\"tool_phase\":\"response\"}}"
        });
        let mut pre_redaction_request = request.clone();
        pre_redaction_request["text_content"] = json!("super-secret");
        pre_redaction_request["payload_json"] =
            json!("{\"message\":\"super-secret\",\"moraine_tool_io\":{\"input_json\":\"{\\\"path\\\":\\\"super-secret\\\"}\",\"tool_call_id\":\"call-1\",\"tool_name\":\"Read\",\"tool_phase\":\"request\"}}");
        let pre_redaction_uid =
            semantic_event_uid(&pre_redaction_request, None).expect("pre-redaction request UID");
        let expected_request_uid =
            semantic_event_uid(&request, None).expect("expected final request UID");
        let expected_response_uid =
            semantic_event_uid(&response, None).expect("expected final response UID");
        let mut uid_map = HashMap::new();
        let mut request_batch = RowBatch::default();
        request_batch.event_rows = vec![request];
        request_batch.tool_rows = vec![json!({"event_uid": "request-provisional"})];
        finalize_batch_event_identities(&mut request_batch, &mut uid_map)
            .expect("finalize request chunk");

        let mut response_batch = RowBatch::default();
        response_batch.event_rows = vec![response];
        response_batch.link_rows = vec![json!({
            "event_uid": "request-provisional",
            "linked_event_uid": "response-provisional"
        })];
        finalize_batch_event_identities(&mut response_batch, &mut uid_map)
            .expect("finalize response chunk");

        assert_eq!(
            request_batch.event_rows[0]["event_uid"],
            expected_request_uid
        );
        assert_eq!(
            response_batch.event_rows[0]["event_uid"],
            expected_response_uid
        );
        assert_ne!(
            request_batch.event_rows[0]["event_uid"], pre_redaction_uid,
            "pre-redaction values must not survive in the persisted identity"
        );
        assert_eq!(
            response_batch.event_rows[0]["origin_event_id"],
            expected_request_uid
        );
        assert_eq!(
            request_batch.tool_rows[0]["event_uid"],
            expected_request_uid
        );
        assert_eq!(
            response_batch.link_rows[0]["event_uid"],
            expected_request_uid
        );
        assert_eq!(
            response_batch.link_rows[0]["linked_event_uid"],
            expected_response_uid
        );
        assert!(
            !serde_json::to_string(&request_batch.event_rows)
                .expect("serialize final events")
                .contains("super-secret"),
            "the finalized identity input must contain only post-redaction values"
        );
    }

    #[test]
    fn folded_tool_identity_matches_migration_recomputation() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_parity",
                "name": "Read",
                "arguments": "{\"path\":\"src/lib.rs\"}"
            }
        });
        let normalized = normalize_record(
            &record,
            "codex",
            "codex",
            "/tmp/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
            1,
            1,
            1,
            0,
            "",
            "",
            "",
        )
        .expect("normalize folded tool event");
        let event = normalized.event_rows.first().expect("tool event");
        let recomputed = semantic_event_uid(event, None).expect("migration-style UID");
        assert_eq!(
            event.get("event_uid").and_then(Value::as_str),
            Some(recomputed.as_str())
        );
    }
}
