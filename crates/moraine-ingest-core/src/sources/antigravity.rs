use super::shared::*;
use super::{
    emitter::SourceEmitter, IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext,
};
use regex::Regex;
use serde_json::Value;
use std::path::{Component, Path};
use std::sync::OnceLock;

pub(crate) static ANTIGRAVITY: Antigravity = Antigravity;

pub(crate) struct Antigravity;

fn null_value<'a>() -> &'a Value {
    static NULL: Value = Value::Null;
    &NULL
}

fn uuid_pattern() -> &'static Regex {
    static UUID_RE: OnceLock<Regex> = OnceLock::new();
    UUID_RE.get_or_init(|| {
        Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
            .expect("valid uuid regex")
    })
}

pub(crate) fn infer_antigravity_session_id(source_file: &str) -> String {
    let path = Path::new(source_file);
    for component in path.components() {
        if let Component::Normal(os_str) = component {
            if let Some(s) = os_str.to_str() {
                if uuid_pattern().is_match(s) {
                    return s.to_string();
                }
            }
        }
    }
    infer_session_id_from_file(source_file)
}

fn extract_antigravity_user_prompt(content: &str) -> String {
    let trimmed = content.trim();
    if let Some(start) = trimmed.find("<USER_REQUEST>") {
        let after = &trimmed[start + "<USER_REQUEST>".len()..];
        if let Some(end) = after.find("</USER_REQUEST>") {
            let inner = after[..end].trim();
            if !inner.is_empty() {
                return inner.to_string();
            }
        }
    }

    let mut result = trimmed.to_string();
    for tag in [
        "ADDITIONAL_METADATA",
        "USER_SETTINGS_CHANGE",
        "SYSTEM_MESSAGE",
        "TASK_INSTRUCTIONS",
    ] {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        while let Some(start) = result.find(&open) {
            if let Some(end) = result[start..].find(&close) {
                result.replace_range(start..start + end + close.len(), "");
            } else {
                break;
            }
        }
    }
    let cleaned = result.trim();
    if cleaned.is_empty() {
        trimmed.to_string()
    } else {
        cleaned.to_string()
    }
}

fn extract_antigravity_model(record: &Value, model_hint: &str) -> String {
    let m = to_str(record.get("model"));
    if !m.is_empty() {
        return canonicalize_model("antigravity", &m);
    }
    if !model_hint.is_empty() {
        return canonicalize_model("antigravity", model_hint);
    }
    let content = to_str(record.get("content"));
    if content.contains("Model Selection") {
        if let Some(pos) = content.find("`Model Selection` from None to ") {
            let after = &content[pos + "`Model Selection` from None to ".len()..];
            if let Some(end) = after.find('.') {
                let model_name = &after[..end];
                let canonical = canonicalize_model("antigravity", model_name);
                if !canonical.is_empty() {
                    return canonical;
                }
            }
        }
    }
    // Preserve the historical fallback until Antigravity gets a replay-safe
    // migration path for already-ingested sessions whose event identity
    // included this placeholder model.
    "gemini-3.7-flash".to_string()
}

fn estimate_tokens_from_chars(text: &str) -> u64 {
    (text.chars().count().saturating_add(3) / 4) as u64
}

impl IngestSource for Antigravity {
    fn harness(&self) -> &'static str {
        "antigravity"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("google")
    }

    fn record_ts(&self, record: &Value) -> String {
        let created_at = to_str(record.get("created_at"));
        if !created_at.is_empty() {
            created_at
        } else {
            to_str(record.get("timestamp"))
        }
    }

    fn top_type(&self, record: &Value) -> String {
        to_str(record.get("type"))
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        SourceMetadata {
            inference_provider: self
                .default_inference_provider()
                .unwrap_or_default()
                .to_string(),
            model_hint_fallback: to_str(record.get("model")),
        }
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let session_id = to_str(record.get("session_id"));
        if !session_id.trim().is_empty() {
            return session_id;
        }
        let conversation_id = to_str(record.get("conversation_id"));
        if !conversation_id.trim().is_empty() {
            return conversation_id;
        }
        if !ctx.session_hint.is_empty() {
            return ctx.session_hint.to_string();
        }
        infer_antigravity_session_id(ctx.source_file)
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        let mut emitter = SourceEmitter::new(ctx);
        normalize_antigravity_record(record, top_type, base_uid, model_hint, &mut emitter);
        emitter.finish()
    }
}

fn normalize_antigravity_record(
    record: &Value,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let step_index = to_u32(record.get("step_index"));
    let content = to_str(record.get("content"));
    let item_id = format!("step:{step_index}");
    let request_id = format!("step:{step_index}");
    let model = extract_antigravity_model(record, model_hint);

    match top_type {
        "USER_INPUT" | "USER" => {
            let clean_content = extract_antigravity_user_prompt(&content);
            let prompt_tokens = estimate_tokens_from_chars(&clean_content);
            let accounting = TokenAccounting::generation(prompt_tokens, 0, 0, 0);

            let event = emitter
                .event_for_json(
                    base_uid,
                    "message",
                    "message",
                    "user",
                    &clean_content,
                    record,
                )
                .turn_index(step_index)
                .item_id(&item_id)
                .request_id(&request_id)
                .model(&model)
                .token_accounting(accounting)
                .content_types(["text"]);
            emitter.push_event(event);
        }
        "PLANNER_RESPONSE" => {
            let tool_calls = record
                .get("tool_calls")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            let has_tool_calls = !tool_calls.is_empty();
            let mut content_types = Vec::new();
            if !content.is_empty() {
                content_types.push("text");
            }
            if has_tool_calls {
                content_types.push("tool_use");
            }
            if content_types.is_empty() {
                content_types.push("text");
            }

            let output_tokens = estimate_tokens_from_chars(&content);
            let accounting = TokenAccounting::generation(0, output_tokens, 0, 0);

            let event = emitter
                .event_for_json(
                    base_uid,
                    "message",
                    "message",
                    "assistant",
                    &content,
                    record,
                )
                .turn_index(step_index)
                .item_id(&item_id)
                .request_id(&request_id)
                .model(&model)
                .token_accounting(accounting)
                .content_types(content_types);
            emitter.push_event(event);

            for (i, call) in tool_calls.iter().enumerate() {
                let tool_name = to_str(call.get("name"));
                let call_id = {
                    let id = to_str(call.get("id"));
                    if id.is_empty() {
                        let cid = to_str(call.get("call_id"));
                        if cid.is_empty() {
                            format!("step:{step_index}:call:{i}")
                        } else {
                            cid
                        }
                    } else {
                        id
                    }
                };
                let input = call.get("args").unwrap_or_else(null_value);
                let input_json = compact_json(input);
                let call_uid = emitter.uid_for_json(call, &format!("antigravity:call:{i}"));
                let call_tokens = estimate_tokens_from_chars(&input_json);
                let call_accounting = TokenAccounting::generation(0, call_tokens, 0, 0);

                let call_event = emitter
                    .event_for_json(
                        &call_uid,
                        "tool_call",
                        "tool_call",
                        "assistant",
                        &extract_message_text(input),
                        call,
                    )
                    .turn_index(step_index)
                    .item_id(&call_id)
                    .request_id(&request_id)
                    .model(&model)
                    .tool_call_id(&call_id)
                    .tool_name(&tool_name)
                    .tool_phase("request")
                    .token_accounting(call_accounting)
                    .content_types(["tool_call"]);
                emitter.push_event(call_event);

                emitter.push_tool_request(&call_uid, &call_id, "", &tool_name, &input_json);
            }
        }
        "CHECKPOINT" | "CONVERSATION_HISTORY" | "SYSTEM" => {
            let event = emitter
                .event_for_json(
                    base_uid,
                    "session_meta",
                    "checkpoint",
                    "system",
                    &content,
                    record,
                )
                .turn_index(step_index)
                .item_id(&item_id)
                .content_types(["text"]);
            emitter.push_event(event);
        }
        other => {
            // Check if this is a tool execution result (e.g. VIEW_FILE, RUN_COMMAND, LIST_DIR, etc.)
            let status = to_str(record.get("status"));
            let is_error = status.eq_ignore_ascii_case("ERROR");
            let tool_error = if is_error { 1 } else { 0 };
            let tool_name = other.to_ascii_lowercase();
            let call_id = {
                let id = to_str(record.get("call_id"));
                if id.is_empty() {
                    let cid = to_str(record.get("id"));
                    if cid.is_empty() {
                        format!("step:{step_index}")
                    } else {
                        cid
                    }
                } else {
                    id
                }
            };
            let output_json = compact_json(record);
            let result_tokens = estimate_tokens_from_chars(&content);
            let accounting = TokenAccounting::generation(result_tokens, 0, 0, 0);

            emitter.push_tool_response(
                base_uid,
                &call_id,
                "",
                &tool_name,
                tool_error,
                "",
                &output_json,
                &content,
            );

            let event = emitter
                .event_for_json(
                    base_uid,
                    "tool_result",
                    "tool_result",
                    "tool",
                    &content,
                    record,
                )
                .turn_index(step_index)
                .item_id(&item_id)
                .request_id(&request_id)
                .model(&model)
                .tool_name(&tool_name)
                .tool_call_id(&call_id)
                .tool_phase("response")
                .tool_error(tool_error)
                .token_accounting(accounting)
                .content_types(["tool_result"]);

            emitter.push_event(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::normalize_record;
    use serde_json::json;

    const SOURCE_FILE: &str =
        "/fixtures/antigravity/11111111-2222-4333-8444-555555555555/transcript.jsonl";

    fn source_ctx<'a>(
        source_file: &'a str,
        session_hint: &'a str,
        top_type: &'a str,
    ) -> SourceRecordContext<'a> {
        SourceRecordContext {
            source_name: "antigravity",
            source_file,
            session_hint,
            top_type,
            base_uid: "raw:base",
        }
    }

    fn normalize(record: Value) -> crate::model::NormalizedRecord {
        normalize_record(
            &record,
            "antigravity",
            "antigravity",
            SOURCE_FILE,
            1,
            0,
            1,
            0,
            "",
            "",
            "",
        )
        .expect("normalize Antigravity record")
    }

    #[test]
    fn explicit_session_fields_override_path_uuid() {
        assert_eq!(
            ANTIGRAVITY.session_id(
                &json!({
                    "type": "SYSTEM",
                    "session_id": "session-123",
                    "conversation_id": "conversation-ignored",
                }),
                &source_ctx(SOURCE_FILE, "", "SYSTEM"),
            ),
            "session-123"
        );
        assert_eq!(
            ANTIGRAVITY.session_id(
                &json!({
                    "type": "SYSTEM",
                    "conversation_id": "conversation-123",
                }),
                &source_ctx(SOURCE_FILE, "", "SYSTEM"),
            ),
            "conversation-123"
        );
    }

    #[test]
    fn conversation_history_and_system_normalize_as_checkpoints() {
        for top_type in ["CONVERSATION_HISTORY", "SYSTEM"] {
            let normalized = normalize(json!({
                "type": top_type,
                "created_at": "2026-08-15T19:10:06.000Z",
                "content": "checkpoint text",
            }));
            assert_eq!(normalized.event_rows.len(), 1, "{top_type}");
            let event = &normalized.event_rows[0];
            assert_eq!(event["event_kind"], "session_meta", "{top_type}");
            assert_eq!(event["payload_type"], "unknown", "{top_type}");
            assert_eq!(event["actor_kind"], "system", "{top_type}");
            assert_eq!(event["text_content"], "checkpoint text", "{top_type}");
        }
    }
}
