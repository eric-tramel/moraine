use super::shared::*;
use super::{
    emitter::SourceEmitter, IngestSource, NormalizedPartials, Preflight, SourceMetadata,
    SourceRecordContext,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::OnceLock;
use urlencoding::decode as url_decode;

pub(crate) static GROK: Grok = Grok;

pub(crate) struct Grok;

impl IngestSource for Grok {
    fn harness(&self) -> &'static str {
        "grok"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("xai")
    }

    fn preflight<'a>(&self, record: &'a Value) -> Preflight<'a> {
        match to_str(record.get("type")).as_str() {
            // Huge system prompts and synthetic harness noise should not flood search.
            "system" => Preflight::Skip,
            "user" if record.get("synthetic_reason").is_some() => Preflight::Skip,
            _ => Preflight::Keep(record),
        }
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        let mut meta = SourceMetadata::new("xai");
        let model = to_str(record.get("model_id"));
        if !model.is_empty() {
            meta.model_hint_fallback = model;
        }
        meta
    }

    fn record_ts(&self, record: &Value) -> String {
        let direct = to_str(record.get("timestamp"));
        if !direct.is_empty() {
            return direct;
        }
        String::new()
    }

    fn session_id(&self, _record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if !ctx.session_hint.is_empty() {
            return ctx.session_hint.to_string();
        }
        grok_session_id_from_path(ctx.source_file).unwrap_or_else(|| {
            let inferred = infer_session_id_from_file(ctx.source_file);
            if inferred.is_empty() {
                String::new()
            } else {
                format!("grok:{inferred}")
            }
        })
    }

    fn jsonl_carries_cwd(&self) -> bool {
        true
    }

    fn cwd_from_source_file(&self, source_file: &str) -> String {
        grok_cwd_from_path(source_file)
            .or_else(|| grok_summary_field(source_file, &["info", "cwd"]))
            .unwrap_or_default()
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        normalize_grok_record(record, ctx, top_type, base_uid, model_hint)
    }
}

fn normalize_grok_record(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    match top_type {
        "user" => handle_user(record, base_uid, &mut emitter),
        "assistant" => handle_assistant(record, base_uid, model_hint, &mut emitter),
        "tool_result" => handle_tool_result(record, base_uid, &mut emitter),
        "reasoning" => handle_reasoning(record, base_uid, &mut emitter),
        "backend_tool_call" => handle_backend_tool(record, base_uid, &mut emitter),
        other => handle_unknown(record, other, base_uid, &mut emitter),
    }
    emitter.finish()
}

fn handle_user(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let text = extract_user_text(record.get("content").unwrap_or(&Value::Null));
    let event = emitter
        .event_for_json(base_uid, "message", "user_message", "user", &text, record)
        .content_types(["text"]);
    emitter.push_event(event);
}

fn handle_assistant(
    record: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let model = first_non_empty(&[
        to_str(record.get("model_id")),
        canonicalize_model("grok", model_hint),
    ]);
    let content = extract_message_text(record.get("content").unwrap_or(&Value::Null));
    if !content.trim().is_empty() {
        let mut event = emitter
            .event_for_json(
                base_uid,
                "message",
                "agent_message",
                "assistant",
                &content,
                record,
            )
            .content_types(["text"]);
        if !model.is_empty() {
            event = event.model(&model);
        }
        emitter.push_event(event);
    }

    if let Some(Value::Array(calls)) = record.get("tool_calls") {
        for (idx, call) in calls.iter().enumerate() {
            let tool_call_id = to_str(call.get("id"));
            let tool_name = to_str(call.get("name"));
            let args_raw = to_str(call.get("arguments"));
            let input_json = if args_raw.trim().is_empty() {
                "{}".to_string()
            } else if parse_json_string(&args_raw).is_some() {
                args_raw.clone()
            } else {
                compact_json(&json!({ "arguments": args_raw }))
            };
            let input_text = if args_raw.is_empty() {
                tool_name.clone()
            } else {
                args_raw
            };
            let uid = emitter.uid_for_json(call, &format!("grok:tool_call:{idx}"));
            let mut event = emitter
                .event_for_json(
                    &uid,
                    "tool_call",
                    "tool_use",
                    "assistant",
                    &input_text,
                    call,
                )
                .content_types(["tool_use"])
                .tool_call_id(&tool_call_id)
                .tool_name(&tool_name);
            if !model.is_empty() {
                event = event.model(&model);
            }
            emitter.push_event(event);
            emitter.push_tool_request(&uid, &tool_call_id, "", &tool_name, &input_json);
        }
    }
}

fn handle_tool_result(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let tool_call_id = to_str(record.get("tool_call_id"));
    let content = record.get("content").unwrap_or(&Value::Null);
    let output_text = extract_message_text(content);
    let output_json = match content {
        Value::String(s) => compact_json(&json!(s)),
        other => compact_json(other),
    };
    let event = emitter
        .event_for_json(
            base_uid,
            "tool_result",
            "tool_result",
            "tool",
            &output_text,
            record,
        )
        .content_types(["tool_result"])
        .tool_call_id(&tool_call_id);
    emitter.push_event(event);
    emitter.push_tool_response(
        base_uid,
        &tool_call_id,
        "",
        "",
        0,
        "",
        &output_json,
        &output_text,
    );
}

fn handle_reasoning(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let text = extract_reasoning_text(record);
    if text.trim().is_empty() {
        return;
    }
    let event = emitter
        .event_for_json(
            base_uid,
            "reasoning",
            "thinking",
            "assistant",
            &text,
            record,
        )
        .content_types(["reasoning"])
        .has_reasoning(true);
    emitter.push_event(event);
}

fn handle_backend_tool(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let kind = record.get("kind").unwrap_or(&Value::Null);
    let tool_name = first_non_empty(&[
        to_str(kind.get("tool_type")),
        "backend_tool".to_string(),
    ]);
    let tool_call_id = to_str(kind.get("id"));
    let action = kind.get("action").unwrap_or(&Value::Null);
    let input_json = compact_json(action);
    let input_text = extract_message_text(action);
    let event = emitter
        .event_for_json(
            base_uid,
            "tool_call",
            "tool_use",
            "assistant",
            &input_text,
            record,
        )
        .content_types(["tool_use"])
        .tool_call_id(&tool_call_id)
        .tool_name(&tool_name)
        .op_kind("backend_tool_call");
    emitter.push_event(event);
    emitter.push_tool_request(base_uid, &tool_call_id, "", &tool_name, &input_json);
}

fn handle_unknown(record: &Value, top_type: &str, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let payload_type = if top_type.is_empty() {
        "unknown"
    } else {
        top_type
    };
    let event = emitter.event_for_json(
        base_uid,
        "unknown",
        payload_type,
        "system",
        &extract_message_text(record),
        record,
    );
    emitter.push_event(event);
}

fn extract_user_text(content: &Value) -> String {
    let text = extract_message_text(content);
    // Prefer the inner user_query body when present.
    if let Some(start) = text.find("<user_query>") {
        if let Some(end) = text.find("</user_query>") {
            let inner = text[start + "<user_query>".len()..end].trim();
            if !inner.is_empty() {
                return truncate_chars(inner, TEXT_LIMIT);
            }
        }
    }
    text
}

fn extract_reasoning_text(record: &Value) -> String {
    match record.get("summary") {
        Some(Value::Array(items)) => {
            let mut parts = Vec::new();
            for item in items {
                let t = to_str(item.get("text"));
                if !t.is_empty() {
                    parts.push(t);
                } else {
                    let nested = extract_message_text(item);
                    if !nested.is_empty() {
                        parts.push(nested);
                    }
                }
            }
            truncate_chars(&parts.join("\n"), TEXT_LIMIT)
        }
        Some(other) => extract_message_text(other),
        None => String::new(),
    }
}

fn first_non_empty(values: &[String]) -> String {
    values
        .iter()
        .find(|v| !v.trim().is_empty())
        .cloned()
        .unwrap_or_default()
}

/// Session directory is the parent of `chat_history.jsonl`.
pub(crate) fn grok_session_id_from_path(source_file: &str) -> Option<String> {
    let path = Path::new(source_file);
    let parent = path.parent()?.file_name()?.to_str()?;
    if parent.is_empty() {
        return None;
    }
    // Prefer namespaced IDs only when the parent is already a UUID-looking id;
    // always prefix to avoid colliding with other harness session UUIDs.
    Some(format!("grok:{parent}"))
}

/// Grandparent path segment is URL-encoded cwd (`%2Fhome%2F...`).
pub(crate) fn grok_cwd_from_path(source_file: &str) -> Option<String> {
    let path = Path::new(source_file);
    let encoded = path.parent()?.parent()?.file_name()?.to_str()?;
    let decoded = url_decode(encoded).ok()?.into_owned();
    if decoded.starts_with('/') {
        Some(decoded)
    } else {
        None
    }
}

fn grok_summary_path(source_file: &str) -> Option<std::path::PathBuf> {
    Some(Path::new(source_file).parent()?.join("summary.json"))
}

fn grok_summary_field(source_file: &str, path: &[&str]) -> Option<String> {
    let summary_path = grok_summary_path(source_file)?;
    let text = std::fs::read_to_string(summary_path).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let mut cur = &value;
    for key in path {
        cur = cur.get(*key)?;
    }
    let s = to_str(Some(cur));
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Fallback timestamp when chat_history lines lack `timestamp`.
pub(crate) fn grok_summary_record_ts(source_file: &str) -> Option<String> {
    static CACHE: OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    if let Ok(guard) = cache.lock() {
        if let Some(ts) = guard.get(source_file) {
            return Some(ts.clone());
        }
    }

    let ts = grok_summary_field(source_file, &["created_at"])
        .or_else(|| grok_summary_field(source_file, &["updated_at"]))
        .or_else(|| grok_summary_field(source_file, &["last_active_at"]))?;

    // Normalize to RFC3339-ish by trimming nanosecond-only noise if needed.
    let ts = if parse_record_ts(&ts).is_some() {
        ts
    } else {
        // e.g. 2026-07-18T10:00:00.000000000Z — chrono may accept; if not, trim.
        let trimmed = ts
            .trim()
            .trim_end_matches('Z')
            .split('.')
            .next()
            .unwrap_or(ts.trim());
        format!("{trimmed}.000Z")
    };

    if let Ok(mut guard) = cache.lock() {
        guard.insert(source_file.to_string(), ts.clone());
    }
    Some(ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_session_id_and_cwd_from_path() {
        let path =
            "/home/u/.grok/sessions/%2Fwork%2Fdemo/11111111-2222-4333-8444-555555555555/chat_history.jsonl";
        assert_eq!(
            grok_session_id_from_path(path).as_deref(),
            Some("grok:11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(grok_cwd_from_path(path).as_deref(), Some("/work/demo"));
    }
}
