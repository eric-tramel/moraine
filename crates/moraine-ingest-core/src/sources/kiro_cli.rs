use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

pub(crate) static KIRO_CLI: KiroCli = KiroCli;

pub(crate) struct KiroCli;

impl IngestSource for KiroCli {
    fn harness(&self) -> &'static str {
        "kiro-cli"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        // Kiro's model catalogue spans multiple vendors. Treat Kiro as the
        // inference gateway and preserve the selected catalogue model verbatim.
        Some("kiro")
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        SourceMetadata {
            inference_provider: "kiro".to_string(),
            model_hint_fallback: kiro_record_model(record),
        }
    }

    fn record_ts(&self, record: &Value) -> String {
        match kiro_kind(record) {
            "SessionMeta" => to_str(record.pointer("/data/created_at")),
            "Prompt" => kiro_prompt_timestamp(record),
            _ => String::new(),
        }
    }

    fn top_type(&self, record: &Value) -> String {
        kiro_kind(record).to_string()
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let explicit = to_str(record.pointer("/data/session_id"));
        if !explicit.is_empty() {
            explicit
        } else if !ctx.session_hint.is_empty() {
            ctx.session_hint.to_string()
        } else {
            infer_session_id_from_file(ctx.source_file)
        }
    }

    fn cwd(&self, record: &Value) -> String {
        to_str(record.pointer("/data/cwd"))
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        normalize_kiro_record(record, ctx, top_type, base_uid, model_hint)
    }
}

#[derive(Debug)]
pub(crate) struct KiroSessionMetadata {
    record: Option<Value>,
    fingerprint: u64,
    error: Option<String>,
    session_id: String,
    cwd: String,
    model: String,
    created_at: String,
}

impl KiroSessionMetadata {
    pub(crate) fn record(&self) -> Option<&Value> {
        self.record.as_ref()
    }

    pub(crate) fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn cwd(&self) -> &str {
        &self.cwd
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    pub(crate) fn created_at(&self) -> &str {
        &self.created_at
    }
}

pub(crate) fn load_kiro_session_metadata(source_file: &str) -> KiroSessionMetadata {
    let sidecar = kiro_sidecar_path(source_file);
    let fallback_session_id = infer_session_id_from_file(source_file);
    let body = match std::fs::read(&sidecar) {
        Ok(body) => body,
        Err(exc) if exc.kind() == ErrorKind::NotFound => {
            return KiroSessionMetadata {
                record: None,
                fingerprint: 0,
                error: None,
                session_id: fallback_session_id,
                cwd: String::new(),
                model: String::new(),
                created_at: String::new(),
            };
        }
        Err(exc) => {
            return KiroSessionMetadata {
                record: None,
                fingerprint: 0,
                error: Some(format!(
                    "failed to read Kiro session metadata {}: {exc}",
                    sidecar.display()
                )),
                session_id: fallback_session_id,
                cwd: String::new(),
                model: String::new(),
                created_at: String::new(),
            };
        }
    };

    let fingerprint = fingerprint_bytes(&body);
    let document: Value = match serde_json::from_slice(&body) {
        Ok(Value::Object(document)) => Value::Object(document),
        Ok(_) => {
            return invalid_kiro_session_metadata(
                fallback_session_id,
                fingerprint,
                &sidecar,
                "expected a JSON object",
            );
        }
        Err(exc) => {
            return invalid_kiro_session_metadata(
                fallback_session_id,
                fingerprint,
                &sidecar,
                &exc.to_string(),
            );
        }
    };

    let session_id = non_empty_or(
        to_str(document.get("session_id")),
        fallback_session_id.as_str(),
    );
    let cwd = to_str(document.get("cwd"));
    let created_at = to_str(document.get("created_at"));
    let updated_at = to_str(document.get("updated_at"));
    let title = to_str(document.get("title"));
    let agent_name = to_str(document.pointer("/session_state/agent_name"));
    let model = to_str(document.pointer("/session_state/rts_model_state/model_info/model_id"));
    let turns = document
        .pointer("/session_state/conversation_metadata/user_turn_metadatas")
        .and_then(Value::as_array);
    let turn_count = turns.map_or(0, Vec::len);
    let (input_tokens, output_tokens, credits) = turns
        .map(|turns| {
            turns
                .iter()
                .fold((0u64, 0u64, 0.0f64), |(input, output, credits), turn| {
                    (
                        input.saturating_add(to_u64(turn.get("input_token_count"))),
                        output.saturating_add(to_u64(turn.get("output_token_count"))),
                        credits + kiro_turn_credits(turn),
                    )
                })
        })
        .unwrap_or_default();

    let record = json!({
        "version": "moraine-v1",
        "kind": "SessionMeta",
        "data": {
            "session_id": session_id,
            "cwd": cwd,
            "created_at": created_at,
            "updated_at": updated_at,
            "title": title,
            "agent_name": agent_name,
            "model": model,
            "turn_count": turn_count,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "credits": credits,
        }
    });

    KiroSessionMetadata {
        record: Some(record),
        fingerprint,
        error: None,
        session_id,
        cwd,
        model,
        created_at,
    }
}

fn invalid_kiro_session_metadata(
    session_id: String,
    fingerprint: u64,
    sidecar: &Path,
    reason: &str,
) -> KiroSessionMetadata {
    KiroSessionMetadata {
        record: None,
        fingerprint,
        error: Some(format!(
            "failed to parse Kiro session metadata {}: {reason}",
            sidecar.display()
        )),
        session_id,
        cwd: String::new(),
        model: String::new(),
        created_at: String::new(),
    }
}

fn kiro_sidecar_path(source_file: &str) -> PathBuf {
    Path::new(source_file).with_extension("json")
}

fn fingerprint_bytes(body: &[u8]) -> u64 {
    let digest = Sha256::digest(body);
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest always has at least eight bytes"),
    )
}

fn non_empty_or(value: String, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value
    }
}

fn normalize_kiro_record(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    match top_type {
        "SessionMeta" => handle_kiro_session_meta(record, &mut emitter),
        "Prompt" => handle_kiro_prompt(record, model_hint, &mut emitter),
        "AssistantMessage" => handle_kiro_assistant(record, model_hint, &mut emitter),
        "ToolResults" => handle_kiro_tool_results(record, model_hint, &mut emitter),
        "Compaction" => handle_kiro_compaction(record, base_uid, model_hint, &mut emitter),
        _ => handle_kiro_unknown(record, top_type, base_uid, model_hint, &mut emitter),
    }
    emitter.finish()
}

fn handle_kiro_session_meta(record: &Value, emitter: &mut SourceEmitter<'_>) {
    let data = record.get("data").unwrap_or_else(null_value);
    let session_id = to_str(data.get("session_id"));
    let uid = emitter.uid(&format!("kiro-cli:session:{session_id}"), "session_meta");
    let event = stamp_kiro_model(
        emitter
            .event_for_json(
                &uid,
                "session_meta",
                "session_meta",
                "system",
                &to_str(data.get("title")),
                data,
            )
            .item_id(session_id)
            .agent_label(to_str(data.get("agent_name")))
            .input_tokens(to_u32(data.get("input_tokens")))
            .output_tokens(to_u32(data.get("output_tokens")))
            .token_usage_native_units(kiro_native_units(
                data.get("credits")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            )),
        &to_str(data.get("model")),
    );
    emitter.push_event(event);
}

fn kiro_native_units(credits: f64) -> Value {
    let mut native_units = token_native_units(&[]);
    native_units["credits"] = json!(credits);
    native_units
}

fn kiro_turn_credits(turn: &Value) -> f64 {
    turn.get("metering_usage")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|usage| {
            usage.get("unit").and_then(Value::as_str) == Some("credit")
                || usage.get("unitPlural").and_then(Value::as_str) == Some("credits")
        })
        .filter_map(|usage| usage.get("value").and_then(Value::as_f64))
        .sum()
}

fn handle_kiro_prompt(record: &Value, model_hint: &str, emitter: &mut SourceEmitter<'_>) {
    let message_id = kiro_message_id(record);
    for (index, block) in kiro_content(record).iter().enumerate() {
        match kiro_block_kind(block) {
            "toolResult" => {
                emit_kiro_tool_result(record, block, index, &message_id, model_hint, emitter)
            }
            "text" | "image" => {
                let uid = kiro_block_uid(emitter, &message_id, index, kiro_block_kind(block));
                let content_type = kiro_content_type(block);
                let event = stamp_kiro_model(
                    emitter
                        .event_for_json(
                            &uid,
                            "message",
                            "user_message",
                            "user",
                            &kiro_block_text(block),
                            block,
                        )
                        .content_types([content_type])
                        .item_id(message_id.clone()),
                    model_hint,
                );
                emitter.push_event(event);
            }
            _ => emit_kiro_unknown_block(block, index, &message_id, "user", model_hint, emitter),
        }
    }
}

fn handle_kiro_assistant(record: &Value, model_hint: &str, emitter: &mut SourceEmitter<'_>) {
    let message_id = kiro_message_id(record);
    let record_model = kiro_record_model(record);
    let model = if record_model.is_empty() {
        model_hint
    } else {
        record_model.as_str()
    };

    for (index, block) in kiro_content(record).iter().enumerate() {
        let block_kind = kiro_block_kind(block);
        let uid = kiro_block_uid(emitter, &message_id, index, block_kind);
        match block_kind {
            "thinking" => {
                let event = stamp_kiro_model(
                    emitter
                        .event_for_json(
                            &uid,
                            "reasoning",
                            "reasoning",
                            "assistant",
                            &kiro_block_text(block),
                            block,
                        )
                        .content_types(["reasoning"])
                        .has_reasoning(true)
                        .item_id(message_id.clone()),
                    model,
                );
                emitter.push_event(event);
            }
            "toolUse" => emit_kiro_tool_use(block, &uid, &message_id, model, emitter),
            "text" | "image" => {
                let event = stamp_kiro_model(
                    emitter
                        .event_for_json(
                            &uid,
                            "message",
                            "agent_message",
                            "assistant",
                            &kiro_block_text(block),
                            block,
                        )
                        .content_types([kiro_content_type(block)])
                        .item_id(message_id.clone()),
                    model,
                );
                emitter.push_event(event);
            }
            _ => emit_kiro_unknown_block(block, index, &message_id, "assistant", model, emitter),
        }
    }
}

fn handle_kiro_tool_results(record: &Value, model_hint: &str, emitter: &mut SourceEmitter<'_>) {
    let message_id = kiro_message_id(record);
    for (index, block) in kiro_content(record).iter().enumerate() {
        if kiro_block_kind(block) == "toolResult" {
            emit_kiro_tool_result(record, block, index, &message_id, model_hint, emitter);
        } else {
            emit_kiro_unknown_block(block, index, &message_id, "tool", model_hint, emitter);
        }
    }
}

fn emit_kiro_tool_use(
    block: &Value,
    uid: &str,
    message_id: &str,
    model: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let data = block.get("data").unwrap_or_else(null_value);
    let tool_call_id = to_str(data.get("toolUseId"));
    let tool_name = to_str(data.get("name"));
    let input = data.get("input").unwrap_or_else(null_value);
    let input_json = compact_json(input);
    let input_text = {
        let text = extract_message_text(input);
        if text.is_empty() {
            input_json.clone()
        } else {
            text
        }
    };
    let event = stamp_kiro_model(
        emitter
            .event_for_json(
                uid,
                "tool_call",
                "tool_use",
                "assistant",
                &input_text,
                block,
            )
            .content_types(["tool_use"])
            .item_id(message_id.to_string())
            .tool_call_id(tool_call_id.clone())
            .tool_name(tool_name.clone()),
        model,
    );
    emitter.push_event(event);
    emitter.push_tool_request(uid, &tool_call_id, "", &tool_name, &input_json);
}

fn emit_kiro_tool_result(
    record: &Value,
    block: &Value,
    index: usize,
    message_id: &str,
    model: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let data = block.get("data").unwrap_or_else(null_value);
    let tool_call_id = to_str(data.get("toolUseId"));
    let status = to_str(data.get("status"));
    let tool_error = u8::from(!matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "" | "success" | "ok"
    ));
    let output = data.get("content").unwrap_or_else(null_value);
    let output_json = compact_json(output);
    let output_text = kiro_value_text(output);
    let tool_name = kiro_result_tool_name(record, &tool_call_id);
    let uid = kiro_block_uid(emitter, message_id, index, "toolResult");
    let event = stamp_kiro_model(
        emitter
            .event_for_json(
                &uid,
                "tool_result",
                "tool_result",
                "tool",
                &output_text,
                block,
            )
            .content_types(["tool_result"])
            .item_id(message_id.to_string())
            .tool_call_id(tool_call_id.clone())
            .tool_name(tool_name.clone())
            .tool_error(tool_error)
            .op_status(status),
        model,
    );
    emitter.push_event(event);
    emitter.push_tool_response(
        &uid,
        &tool_call_id,
        "",
        &tool_name,
        tool_error,
        "",
        &output_json,
        &output_text,
    );
}

fn handle_kiro_compaction(
    record: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let data = record.get("data").unwrap_or_else(null_value);
    let payload = json!({
        "summary": data.get("summary").cloned().unwrap_or(Value::Null),
        "strategy": data.get("strategy").cloned().unwrap_or(Value::Null),
    });
    let event = stamp_kiro_model(
        emitter.event_for_json(
            base_uid,
            "summary",
            "compacted",
            "system",
            &to_str(data.get("summary")),
            &payload,
        ),
        model_hint,
    );
    emitter.push_event(event);
}

fn handle_kiro_unknown(
    record: &Value,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let payload_type = if top_type.is_empty() {
        "unknown"
    } else {
        top_type
    };
    let event = stamp_kiro_model(
        emitter.event_for_json(
            base_uid,
            "unknown",
            payload_type,
            "system",
            &kiro_value_text(record),
            record,
        ),
        model_hint,
    );
    emitter.push_event(event);
}

fn emit_kiro_unknown_block(
    block: &Value,
    index: usize,
    message_id: &str,
    actor: &str,
    model: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let uid = kiro_block_uid(emitter, message_id, index, kiro_block_kind(block));
    let event = stamp_kiro_model(
        emitter
            .event_for_json(
                &uid,
                "unknown",
                "unknown",
                actor,
                &kiro_block_text(block),
                block,
            )
            .item_id(message_id.to_string()),
        model,
    );
    emitter.push_event(event);
}

fn stamp_kiro_model(event: EventBuilder, model: &str) -> EventBuilder {
    let model = canonicalize_model("kiro-cli", model);
    if model.is_empty() {
        event
    } else {
        event.model(model)
    }
}

fn kiro_kind(record: &Value) -> &str {
    record
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn kiro_message_id(record: &Value) -> String {
    to_str(record.pointer("/data/message_id"))
}

fn kiro_content(record: &Value) -> &[Value] {
    record
        .pointer("/data/content")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn kiro_block_kind(block: &Value) -> &str {
    block
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn kiro_block_uid(
    emitter: &SourceEmitter<'_>,
    message_id: &str,
    index: usize,
    block_kind: &str,
) -> String {
    emitter.uid(
        &format!("kiro-cli:message:{message_id}:block:{index}"),
        if block_kind.is_empty() {
            "unknown"
        } else {
            block_kind
        },
    )
}

fn kiro_content_type(block: &Value) -> &'static str {
    match kiro_block_kind(block) {
        "image" => "image",
        "thinking" => "reasoning",
        "toolUse" => "tool_use",
        "toolResult" => "tool_result",
        _ => "text",
    }
}

fn kiro_block_text(block: &Value) -> String {
    match kiro_block_kind(block) {
        "text" => to_str(block.get("data")),
        "thinking" => to_str(block.pointer("/data/text")),
        "toolResult" => kiro_value_text(block.pointer("/data/content").unwrap_or_else(null_value)),
        _ => kiro_value_text(block.get("data").unwrap_or_else(null_value)),
    }
}

fn kiro_value_text(value: &Value) -> String {
    fn walk(value: &Value, output: &mut Vec<String>) {
        match value {
            Value::String(text) if !text.trim().is_empty() => output.push(text.clone()),
            Value::Array(items) => {
                for item in items {
                    walk(item, output);
                }
            }
            Value::Object(object) => {
                if object.contains_key("kind") && object.contains_key("data") {
                    walk(object.get("data").unwrap_or_else(null_value), output);
                    return;
                }
                for key in ["text", "message", "output", "stdout", "stderr", "content"] {
                    if let Some(value) = object.get(key) {
                        walk(value, output);
                    }
                }
            }
            _ => {}
        }
    }

    let mut output = Vec::new();
    walk(value, &mut output);
    truncate_chars(&output.join("\n"), TEXT_LIMIT)
}

fn kiro_record_model(record: &Value) -> String {
    let session_model = to_str(record.pointer("/data/model"));
    if !session_model.is_empty() {
        return canonicalize_model("kiro-cli", &session_model);
    }

    for block in kiro_content(record) {
        let model = to_str(block.pointer("/data/modelId"));
        if !model.is_empty() {
            return canonicalize_model("kiro-cli", &model);
        }
    }
    String::new()
}

fn kiro_result_tool_name(record: &Value, tool_call_id: &str) -> String {
    let Some(tool_kind) = record
        .pointer("/data/results")
        .and_then(Value::as_object)
        .and_then(|results| results.get(tool_call_id))
        .and_then(|result| result.pointer("/tool/kind"))
        .and_then(Value::as_object)
    else {
        return String::new();
    };

    if let Some(built_in) = tool_kind.get("BuiltIn").and_then(Value::as_object) {
        return built_in.keys().next().cloned().unwrap_or_default();
    }
    if let Some(mcp) = tool_kind.get("Mcp") {
        let server = to_str(mcp.get("serverName"));
        let tool = to_str(mcp.get("toolName"));
        return match (server.is_empty(), tool.is_empty()) {
            (false, false) => format!("{server}.{tool}"),
            (true, false) => tool,
            _ => server,
        };
    }
    tool_kind.keys().next().cloned().unwrap_or_default()
}

fn kiro_prompt_timestamp(record: &Value) -> String {
    let timestamp = record.pointer("/data/meta/timestamp");
    let (decimal, fallback) = match timestamp {
        Some(Value::Number(number)) => (number.to_string(), number.as_f64()),
        Some(Value::String(value)) => {
            let trimmed = value.trim().to_string();
            let fallback = trimmed.parse::<f64>().ok();
            (trimmed, fallback)
        }
        _ => return String::new(),
    };

    format_unix_seconds_decimal(&decimal)
        .or_else(|| fallback.and_then(format_unix_seconds_ts))
        .unwrap_or_default()
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::normalize_record_with_ts_hint;
    use crate::sources::shared::parse_record_ts;

    #[test]
    fn missing_sidecar_falls_back_to_transcript_session_id() {
        let source_file = std::env::temp_dir()
            .join("11111111-2222-4333-8444-555555555555.jsonl")
            .to_string_lossy()
            .to_string();
        let metadata = load_kiro_session_metadata(&source_file);

        assert!(metadata.record().is_none());
        assert!(metadata.error().is_none());
        assert_eq!(metadata.fingerprint(), 0);
        assert_eq!(
            metadata.session_id(),
            "11111111-2222-4333-8444-555555555555"
        );
        assert!(metadata.cwd().is_empty());
        assert!(metadata.model().is_empty());
    }

    #[test]
    fn paired_fixture_loads_sidecar_and_normalizes_with_session_hints() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/kiro/session.jsonl");
        let source_file = path.to_string_lossy().to_string();
        let metadata = load_kiro_session_metadata(&source_file);

        assert!(metadata.error().is_none());
        assert_ne!(metadata.fingerprint(), 0);
        assert_eq!(
            metadata.session_id(),
            "11111111-2222-4333-8444-555555555555"
        );
        assert_eq!(metadata.cwd(), "/work/kiro-demo");
        assert_eq!(metadata.model(), "claude-sonnet-4");
        assert_eq!(metadata.created_at(), "2026-05-28T20:26:40Z");

        let mut session_hint = metadata.session_id().to_string();
        let mut model_hint = metadata.model().to_string();
        let mut cwd_hint = metadata.cwd().to_string();
        let mut record_ts_hint = metadata.created_at().to_string();
        let mut records = Vec::new();

        let session_meta = normalize_record_with_ts_hint(
            metadata.record().expect("synthetic session metadata"),
            "kiro-fixture",
            "kiro-cli",
            &source_file,
            1,
            1,
            0,
            0,
            &session_hint,
            &model_hint,
            &cwd_hint,
            &record_ts_hint,
        )
        .expect("normalize Kiro session metadata");
        session_hint = session_meta.session_hint.clone();
        model_hint = session_meta.model_hint.clone();
        cwd_hint = session_meta.cwd_hint.clone();
        records.push(session_meta);

        let body = std::fs::read_to_string(&path).expect("read Kiro transcript fixture");
        let mut offset = 0u64;
        for (index, raw_line) in body.split_inclusive('\n').enumerate() {
            let start_offset = offset;
            offset = offset.saturating_add(raw_line.len() as u64);
            let record: Value = serde_json::from_str(raw_line.trim())
                .unwrap_or_else(|error| panic!("parse Kiro fixture line {}: {error}", index + 1));
            let normalized = normalize_record_with_ts_hint(
                &record,
                "kiro-fixture",
                "kiro-cli",
                &source_file,
                1,
                1,
                index as u64 + 1,
                start_offset,
                &session_hint,
                &model_hint,
                &cwd_hint,
                &record_ts_hint,
            )
            .unwrap_or_else(|error| panic!("normalize Kiro fixture line {}: {error:#}", index + 1));
            if let Some(record_ts) = normalized.raw_row.get("record_ts").and_then(Value::as_str) {
                if parse_record_ts(record_ts).is_some() {
                    record_ts_hint = record_ts.to_string();
                }
            }
            session_hint = normalized.session_hint.clone();
            model_hint = normalized.model_hint.clone();
            cwd_hint = normalized.cwd_hint.clone();
            records.push(normalized);
        }

        assert_eq!(records.len(), 7, "session metadata plus six JSONL records");
        assert!(records.iter().all(|record| record.error_rows.is_empty()));
        assert!(records.iter().all(|record| {
            record.raw_row.get("session_id").and_then(Value::as_str)
                == Some("11111111-2222-4333-8444-555555555555")
                && record.raw_row.get("cwd").and_then(Value::as_str) == Some("/work/kiro-demo")
        }));

        let events = records
            .iter()
            .flat_map(|record| record.event_rows.iter())
            .collect::<Vec<_>>();
        let tools = records
            .iter()
            .flat_map(|record| record.tool_rows.iter())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 10);
        assert_eq!(tools.len(), 2);
        assert!(events.iter().all(|event| {
            event.get("model").and_then(Value::as_str) == Some("claude-sonnet-4")
        }));
        let session_event = events
            .iter()
            .find(|event| event.get("event_kind").and_then(Value::as_str) == Some("session_meta"))
            .expect("session metadata event");
        assert_eq!(
            session_event.get("input_tokens").and_then(Value::as_u64),
            Some(150)
        );
        assert_eq!(
            session_event.get("output_tokens").and_then(Value::as_u64),
            Some(63)
        );
        assert_eq!(
            session_event
                .pointer("/token_usage_native_units/credits")
                .and_then(Value::as_f64),
            Some(0.03125)
        );
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.get("tool_phase").and_then(Value::as_str))
                .collect::<Vec<_>>(),
            vec![Some("request"), Some("response")]
        );
    }
}
