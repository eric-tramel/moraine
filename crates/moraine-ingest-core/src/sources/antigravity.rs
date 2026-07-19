use super::shared::*;
use super::{
    emitter::SourceEmitter, IngestSource, NormalizedPartials, Preflight, SourceRecordContext,
};
use serde_json::Value;
use std::path::Path;

pub(crate) static ANTIGRAVITY: Antigravity = Antigravity;

pub(crate) struct Antigravity;

/// Tool-like step types emitted as tool results by the Antigravity brain transcript.
const TOOL_STEP_TYPES: &[&str] = &[
    "VIEW_FILE",
    "LIST_DIRECTORY",
    "RUN_COMMAND",
    "SEARCH_WEB",
    "CODE_ACTION",
    "ASK_QUESTION",
    "GENERIC",
    "READ_FILE",
    "WRITE_FILE",
    "EDIT_FILE",
    "FIND",
    "GREP",
    "BROWSER",
    "TERMINAL",
];

impl IngestSource for Antigravity {
    fn harness(&self) -> &'static str {
        "antigravity"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("google")
    }

    fn preflight<'a>(&self, record: &'a Value) -> Preflight<'a> {
        match to_str(record.get("type")).as_str() {
            "CONVERSATION_HISTORY" | "EPHEMERAL_MESSAGE" | "SYSTEM_MESSAGE" => Preflight::Skip,
            _ => Preflight::Keep(record),
        }
    }

    fn record_ts(&self, record: &Value) -> String {
        to_str(record.get("created_at"))
    }

    fn session_id(&self, _record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if !ctx.session_hint.is_empty() {
            return ctx.session_hint.to_string();
        }
        antigravity_session_id_from_path(ctx.source_file).unwrap_or_else(|| {
            let inferred = infer_session_id_from_file(ctx.source_file);
            if inferred.is_empty() {
                String::new()
            } else {
                format!("antigravity:{inferred}")
            }
        })
    }

    fn jsonl_carries_cwd(&self) -> bool {
        // Cwd is recovered from path heuristics / sidecar when available, not
        // from individual transcript lines.
        true
    }

    fn cwd_from_source_file(&self, source_file: &str) -> String {
        antigravity_cwd_from_path_or_content(source_file).unwrap_or_default()
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        _model_hint: &str,
    ) -> NormalizedPartials {
        normalize_antigravity_record(record, ctx, top_type, base_uid)
    }
}

fn normalize_antigravity_record(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    match top_type {
        "USER_INPUT" => handle_user_input(record, base_uid, &mut emitter),
        "PLANNER_RESPONSE" => handle_planner_response(record, base_uid, &mut emitter),
        "CHECKPOINT" => handle_checkpoint(record, base_uid, &mut emitter),
        "INVOKE_SUBAGENT" => handle_invoke_subagent(record, base_uid, &mut emitter),
        "ERROR_MESSAGE" => handle_error_message(record, base_uid, &mut emitter),
        other if is_tool_step(other) => handle_tool_step(record, other, base_uid, &mut emitter),
        other => handle_unknown(record, other, base_uid, &mut emitter),
    }
    emitter.finish()
}

fn is_tool_step(top_type: &str) -> bool {
    TOOL_STEP_TYPES.iter().any(|t| *t == top_type)
        || (!top_type.is_empty()
            && top_type
                .chars()
                .all(|c| c.is_ascii_uppercase() || c == '_')
            && !matches!(
                top_type,
                "USER_INPUT"
                    | "PLANNER_RESPONSE"
                    | "CHECKPOINT"
                    | "INVOKE_SUBAGENT"
                    | "ERROR_MESSAGE"
                    | "CONVERSATION_HISTORY"
                    | "EPHEMERAL_MESSAGE"
                    | "SYSTEM_MESSAGE"
            ))
}

fn handle_user_input(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let text = extract_user_request(to_str(record.get("content")));
    let event = emitter
        .event_for_json(base_uid, "message", "user_message", "user", &text, record)
        .content_types(["text"])
        .item_id(step_index(record));
    emitter.push_event(event);
}

fn handle_planner_response(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let thinking = to_str(record.get("thinking"));
    if !thinking.trim().is_empty() {
        let uid = emitter.uid_for_json(record, "antigravity:thinking");
        let event = emitter
            .event_for_json(
                &uid,
                "reasoning",
                "thinking",
                "assistant",
                &thinking,
                record,
            )
            .content_types(["reasoning"])
            .has_reasoning(true)
            .item_id(step_index(record));
        emitter.push_event(event);
    }

    let content = to_str(record.get("content"));
    if !content.trim().is_empty() {
        let event = emitter
            .event_for_json(
                base_uid,
                "message",
                "agent_message",
                "assistant",
                &content,
                record,
            )
            .content_types(["text"])
            .item_id(step_index(record));
        emitter.push_event(event);
    }

    if let Some(Value::Array(calls)) = record.get("tool_calls") {
        for (idx, call) in calls.iter().enumerate() {
            let tool_name = to_str(call.get("name"));
            let args = call.get("args").unwrap_or(&Value::Null);
            let input_json = compact_json(args);
            let input_text = extract_message_text(args);
            let tool_call_id = format!(
                "antigravity:{}:{}:{}",
                step_index(record),
                tool_name,
                idx
            );
            let uid = emitter.uid_for_json(call, &format!("antigravity:tool_call:{idx}"));
            let event = emitter
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
                .tool_name(&tool_name)
                .item_id(step_index(record));
            emitter.push_event(event);
            emitter.push_tool_request(&uid, &tool_call_id, "", &tool_name, &input_json);
        }
    }
}

fn handle_tool_step(
    record: &Value,
    top_type: &str,
    base_uid: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let tool_name = top_type.to_ascii_lowercase();
    let content = to_str(record.get("content"));
    let tool_call_id = format!("antigravity:{}:{}", step_index(record), tool_name);
    // Transcript tool steps are result-only; emit a paired request so tool IO
    // rows stay request/response complete for the schema contract.
    let input_json = compact_json(&serde_json::json!({
        "tool": tool_name,
        "step_index": step_index(record),
    }));
    let event = emitter
        .event_for_json(
            base_uid,
            "tool_result",
            "tool_result",
            "tool",
            &content,
            record,
        )
        .content_types(["tool_result"])
        .tool_call_id(&tool_call_id)
        .tool_name(&tool_name)
        .item_id(step_index(record));
    emitter.push_event(event);
    emitter
        .push_tool_request(base_uid, &tool_call_id, "", &tool_name, &input_json)
        .push_tool_response(
            base_uid,
            &tool_call_id,
            "",
            &tool_name,
            0,
            &input_json,
            &compact_json(record),
            &content,
        );
}

fn handle_checkpoint(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let content = to_str(record.get("content"));
    let event = emitter
        .event_for_json(base_uid, "summary", "compacted", "system", &content, record)
        .item_id(step_index(record))
        .op_kind("checkpoint");
    emitter.push_event(event);
}

fn handle_invoke_subagent(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let content = to_str(record.get("content"));
    let event = emitter
        .event_for_json(
            base_uid,
            "system",
            "system",
            "system",
            &content,
            record,
        )
        .item_id(step_index(record))
        .op_kind("invoke_subagent");
    emitter.push_event(event);

    if let Some(child_id) = extract_subagent_conversation_id(&content) {
        let linked = format!("antigravity:{child_id}");
        emitter.push_external_link(base_uid, &linked, "subagent_parent", "{}");
    }
}

fn handle_error_message(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let content = first_non_empty(&[
        to_str(record.get("content")),
        to_str(record.get("error")),
    ]);
    let event = emitter
        .event_for_json(base_uid, "system", "system", "system", &content, record)
        .item_id(step_index(record))
        .op_kind("error_message")
        .op_status("error");
    emitter.push_event(event);
}

fn handle_unknown(record: &Value, top_type: &str, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let payload_type = if top_type.is_empty() {
        "unknown"
    } else {
        top_type
    };
    let event = emitter
        .event_for_json(
            base_uid,
            "unknown",
            payload_type,
            "system",
            &to_str(record.get("content")),
            record,
        )
        .item_id(step_index(record));
    emitter.push_event(event);
}

fn step_index(record: &Value) -> String {
    match record.get("step_index") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn extract_user_request(content: String) -> String {
    if let Some(start) = content.find("<USER_REQUEST>") {
        if let Some(end) = content.find("</USER_REQUEST>") {
            let inner = content[start + "<USER_REQUEST>".len()..end].trim();
            if !inner.is_empty() {
                return truncate_chars(inner, TEXT_LIMIT);
            }
        }
    }
    truncate_chars(&content, TEXT_LIMIT)
}

fn extract_subagent_conversation_id(content: &str) -> Option<String> {
    // Content often embeds a JSON object with conversationId.
    if let Some(idx) = content.find("conversationId") {
        let tail = &content[idx..];
        // Match "conversationId": "uuid"
        let re = regex::Regex::new(
            r#"(?i)conversationId["']?\s*[:=]\s*["']([0-9a-fA-F-]{36})["']"#,
        )
        .ok()?;
        if let Some(cap) = re.captures(tail) {
            return Some(cap[1].to_string());
        }
    }
    // Or a pure JSON line.
    for line in content.lines() {
        let trimmed = line.trim().trim_start_matches(',').trim();
        if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
            let id = to_str(value.get("conversationId"));
            if !id.is_empty() {
                return Some(id);
            }
        }
    }
    None
}

fn first_non_empty(values: &[String]) -> String {
    values
        .iter()
        .find(|v| !v.trim().is_empty())
        .cloned()
        .unwrap_or_default()
}

/// Brain layout: `.../brain/<conversation-id>/.system_generated/logs/transcript.jsonl`
pub(crate) fn antigravity_session_id_from_path(source_file: &str) -> Option<String> {
    let path = Path::new(source_file);
    // Walk up looking for a UUID-looking directory that sits under `brain`.
    let mut cur = path.parent();
    while let Some(dir) = cur {
        let name = dir.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if looks_like_uuid(name) {
            let parent_name = dir
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("");
            if parent_name == "brain" || parent_name.is_empty() || looks_like_uuid(name) {
                return Some(format!("antigravity:{name}"));
            }
        }
        cur = dir.parent();
    }
    None
}

fn looks_like_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes[8] == b'-'
        && bytes[13] == b'-'
        && bytes[18] == b'-'
        && bytes[23] == b'-'
        && value.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

fn antigravity_cwd_from_path_or_content(source_file: &str) -> Option<String> {
    // Best-effort: scan a few transcript lines for absolute file:// or path hints.
    let file = std::fs::File::open(source_file).ok()?;
    use std::io::{BufRead, BufReader};
    let reader = BufReader::new(file);
    for line in reader.lines().take(40) {
        let Ok(line) = line else { continue };
        if let Some(cwd) = extract_cwd_hint_from_text(&line) {
            return Some(cwd);
        }
    }
    None
}

fn extract_cwd_hint_from_text(text: &str) -> Option<String> {
    // file:///work/foo or DirectoryPath":"/work/foo
    if let Some(idx) = text.find("file://") {
        let rest = &text[idx + "file://".len()..];
        let path: String = rest
            .chars()
            .take_while(|c| !c.is_whitespace() && *c != '`' && *c != '"' && *c != '\'')
            .collect();
        if path.starts_with('/') {
            // Prefer project root if a file path was given.
            let p = Path::new(&path);
            if p.extension().is_some() {
                return p.parent().map(|d| d.to_string_lossy().into_owned());
            }
            return Some(path);
        }
    }
    for key in ["DirectoryPath", "directoryPath", "workspace"] {
        let needle = format!("\"{key}\"");
        if let Some(idx) = text.find(&needle) {
            let after = &text[idx + needle.len()..];
            if let Some(q1) = after.find('"') {
                let rest = &after[q1 + 1..];
                if let Some(q2) = rest.find('"') {
                    let value = &rest[..q2];
                    let cleaned = value.trim().trim_matches('\\').trim_matches('"');
                    if cleaned.starts_with('/') {
                        return Some(cleaned.to_string());
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_session_id_from_brain_path() {
        let path = "/home/u/.gemini/antigravity-cli/brain/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee/.system_generated/logs/transcript.jsonl";
        assert_eq!(
            antigravity_session_id_from_path(path).as_deref(),
            Some("antigravity:aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        );
    }

    #[test]
    fn extracts_subagent_id() {
        let content = r#"Created:\n{\n  "conversationId":  "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"\n}"#;
        assert_eq!(
            extract_subagent_conversation_id(content).as_deref(),
            Some("bbbbbbbb-cccc-4ddd-8eee-ffffffffffff")
        );
    }
}
