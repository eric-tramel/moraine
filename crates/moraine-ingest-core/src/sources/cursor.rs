use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext,
};
use serde_json::{json, Value};

pub(crate) static CURSOR: Cursor = Cursor;

pub(crate) struct Cursor;

impl IngestSource for Cursor {
    fn harness(&self) -> &'static str {
        "cursor"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("cursor")
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        let mut metadata = SourceMetadata::new("cursor");
        metadata.model_hint_fallback = cursor_model_hint(record);
        metadata
    }

    fn record_ts(&self, record: &Value) -> String {
        cursor_record_ts(record)
    }

    fn cwd(&self, record: &Value) -> String {
        // The cursor_sqlite poller surfaces `workspaceIdentifier.uri.fsPath`
        // as `workspacePath` on synthesized composer records; bubbles and
        // file-backed transcripts carry no working directory of their own.
        to_str(record.get("workspacePath"))
    }

    fn top_type(&self, record: &Value) -> String {
        let role = cursor_role(record);
        if !role.is_empty() {
            return role;
        }

        first_string(record, &["type"])
            .or_else(|| {
                record
                    .get("message")
                    .and_then(|message| first_string(message, &["type", "kind"]))
            })
            .unwrap_or_else(|| "message".to_string())
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        first_string(
            record,
            &[
                "conversationId",
                "conversation_id",
                "sessionId",
                "session_id",
                "threadId",
                "thread_id",
                "chatId",
                "chat_id",
            ],
        )
        .or_else(|| {
            record.get("message").and_then(|message| {
                first_string(
                    message,
                    &[
                        "conversationId",
                        "conversation_id",
                        "sessionId",
                        "session_id",
                        "threadId",
                        "thread_id",
                        "chatId",
                        "chat_id",
                    ],
                )
            })
        })
        .or_else(|| (!ctx.session_hint.is_empty()).then(|| ctx.session_hint.to_string()))
        .or_else(|| {
            let inferred = infer_session_id_from_file(ctx.source_file);
            (!inferred.is_empty()).then_some(inferred)
        })
        .unwrap_or_else(|| format!("cursor:{}", ctx.base_uid))
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        // Synthetic records produced by the cursor_sqlite poller route by
        // top-level type (the Hermes precedent); JSONL agent transcripts fall
        // through to the shape-based normalizer below.
        match top_type {
            "cursor_composer" => normalize_cursor_composer(record, ctx),
            "cursor_bubble" => normalize_cursor_sqlite_bubble(record, ctx),
            _ => normalize_cursor_record(record, ctx, top_type, base_uid, model_hint),
        }
    }
}

/// Normalizes a `cursor_composer` synthetic record (one per `composerData:`
/// row) into a `session_meta` event. The payload keeps `title`/`name` so the
/// MCP session-info extraction surfaces Cursor's own session name.
fn normalize_cursor_composer(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let session_id = first_string(record, &["sessionId"]).unwrap_or_default();
    let uid = emitter.uid(
        &format!("cursor_sqlite:composer:{session_id}"),
        "session_meta",
    );

    let name = first_string(record, &["name", "title"]).unwrap_or_default();
    let subtitle = first_string(record, &["subtitle"]).unwrap_or_default();
    let text = match (name.is_empty(), subtitle.is_empty()) {
        (false, false) => format!("{name} — {subtitle}"),
        (false, true) => name,
        (true, false) => subtitle,
        (true, true) => String::new(),
    };

    let event = emitter.event_for_json(
        &uid,
        "session_meta",
        "session_meta",
        "system",
        &text,
        record,
    );
    emitter.push_event(event);
    emitter.finish()
}

/// Normalizes a `cursor_bubble` synthetic record (one per `bubbleId:` row).
///
/// Bubbles mutate in place while Cursor streams (text grows, tool status
/// flips `pending` → `completed`), so every event UID here is derived from
/// the bubble's logical identity — never its payload — letting a re-emit
/// replace the prior row via `ReplacingMergeTree(event_version)`.
fn normalize_cursor_sqlite_bubble(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let session_id = first_string(record, &["sessionId"]).unwrap_or_default();
    let bubble_id = first_string(record, &["bubbleId"]).unwrap_or_default();
    let identity = format!("cursor_sqlite:bubble:{session_id}:{bubble_id}");
    let bubble_type = record
        .get("bubbleType")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let request_id = first_string(record, &["requestId"]).unwrap_or_default();

    let stamp = |event: EventBuilder| {
        event
            .request_id(request_id.clone())
            .trace_id(request_id.clone())
            .item_id(bubble_id.clone())
    };

    if let Some(thinking_text) = record
        .pointer("/thinking/text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        let uid = emitter.uid(&identity, "reasoning");
        let payload = record.get("thinking").cloned().unwrap_or(Value::Null);
        let event = stamp(emitter.event_for_json(
            &uid,
            "reasoning",
            "reasoning",
            "assistant",
            thinking_text,
            &payload,
        ))
        .has_reasoning(true)
        .content_types(["reasoning"]);
        emitter.push_event(event);
    }

    if let Some(text) = record
        .get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        let (suffix, payload_type, actor) = if bubble_type == 1 {
            ("user_message", "user_message", "user")
        } else {
            ("agent_message", "agent_message", "assistant")
        };
        let uid = emitter.uid(&identity, suffix);
        let event = stamp(emitter.event_for_json(
            &uid,
            "message",
            payload_type,
            actor,
            text,
            &json!({ "text": text }),
        ))
        .content_types(["text"]);
        emitter.push_event(event);
    }

    if let Some(tool_data) = record.get("toolFormerData").filter(|v| v.is_object()) {
        normalize_cursor_tool_former(&identity, tool_data, &stamp, &mut emitter);
    }

    emitter.finish()
}

/// Emits the tool-call (and, once the call reached a terminal status, the
/// tool-result) events plus `tool_io` rows for one `toolFormerData` payload.
fn normalize_cursor_tool_former(
    identity: &str,
    tool_data: &Value,
    stamp: &dyn Fn(EventBuilder) -> EventBuilder,
    emitter: &mut SourceEmitter<'_>,
) {
    let tool_name = cursor_former_tool_name(tool_data);
    let status = first_string(tool_data, &["status"])
        .unwrap_or_default()
        .to_ascii_lowercase();
    // Cursor joins two ids with a literal newline ("call_…\nfc_…"); the
    // first line is the stable call id.
    let tool_call_id = first_string(tool_data, &["toolCallId"])
        .and_then(|raw| raw.lines().next().map(ToOwned::to_owned))
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| identity.to_string());

    let input = tool_data
        .get("params")
        .filter(|v| !v.is_null())
        .or_else(|| tool_data.get("rawArgs").filter(|v| !v.is_null()))
        .cloned()
        .unwrap_or(Value::Null);
    let input_json = compact_json(&input);
    let input_text = cursor_text_or_json(&input);

    let use_uid = emitter.uid(identity, "tool_use");
    let event = stamp(emitter.event_for_json(
        &use_uid,
        "tool_call",
        "tool_use",
        "assistant",
        &input_text,
        tool_data,
    ))
    .content_types(["tool_use"])
    .tool_call_id(tool_call_id.clone())
    .tool_name(tool_name.clone())
    .op_status(status.clone());
    emitter.push_event(event);
    emitter.push_tool_request(&use_uid, &tool_call_id, "", &tool_name, &input_json);
    emit_cursor_reference_links(&use_uid, &input, emitter);

    // `result` can be legitimately absent on a completed call (ripgrep keeps
    // its summary in additionalData; `await` has neither params nor result),
    // so terminality is decided by status alone.
    let terminal = matches!(
        status.as_str(),
        "completed" | "error" | "errored" | "cancelled" | "canceled" | "aborted" | "rejected"
    );
    if !terminal {
        return;
    }

    let output = tool_data
        .get("result")
        .filter(|v| !v.is_null())
        .or_else(|| tool_data.get("error").filter(|v| !v.is_null()))
        .or_else(|| tool_data.get("additionalData").filter(|v| !v.is_null()))
        .cloned()
        .unwrap_or(Value::Null);
    let output_json = compact_json(&output);
    let output_text = cursor_text_or_json(&output);
    let rejected = output
        .get("rejected")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let tool_error = u8::from(status != "completed" || rejected);

    let result_uid = emitter.uid(identity, "tool_result");
    let event = stamp(emitter.event_for_json(
        &result_uid,
        "tool_result",
        "tool_result",
        "tool",
        &output_text,
        &output,
    ))
    .content_types(["tool_result"])
    .tool_call_id(tool_call_id.clone())
    .tool_name(tool_name.clone())
    .op_status(status.clone())
    .tool_error(tool_error);
    emitter.push_event(event);
    emitter.push_tool_response(
        &result_uid,
        &tool_call_id,
        "",
        &tool_name,
        tool_error,
        &input_json,
        &output_json,
        &output_text,
    );
}

/// Tool name for a `toolFormerData` payload. MCP calls degrade to `"mcp--"`
/// on some errors; recover the real name from `rawArgs`/`params` when the
/// recorded one is degenerate.
fn cursor_former_tool_name(tool_data: &Value) -> String {
    let name = first_string(tool_data, &["name"]).unwrap_or_default();
    if !name.is_empty() && name != "mcp--" && name != "mcp-" {
        return name;
    }

    let from_raw_args = tool_data
        .get("rawArgs")
        .and_then(|raw| first_string(raw, &["name"]))
        .map(|tool| format!("mcp:{tool}"));
    if let Some(recovered) = from_raw_args {
        return recovered;
    }

    if name.is_empty() {
        "unknown".to_string()
    } else {
        name
    }
}

fn normalize_cursor_record(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> NormalizedPartials {
    let cursor_record = CursorRecord::new(record, top_type, base_uid, model_hint);
    let mut emitter = SourceEmitter::new(ctx);

    match cursor_record.content() {
        Value::Array(blocks) if !blocks.is_empty() => {
            for (idx, block) in blocks.iter().enumerate() {
                normalize_cursor_content_block(&cursor_record, idx, block, &mut emitter);
            }
        }
        Value::String(text) if !text.trim().is_empty() => {
            emit_cursor_scalar_message(&cursor_record, text, &mut emitter);
        }
        content if !content.is_null() => {
            let text = cursor_text_or_json(content);
            emit_cursor_scalar_message(&cursor_record, &text, &mut emitter);
        }
        _ => emit_cursor_unknown(&cursor_record, &mut emitter),
    }

    emitter.finish()
}

struct CursorRecord<'a> {
    record: &'a Value,
    message: &'a Value,
    top_type: &'a str,
    base_uid: &'a str,
    role: String,
    model: String,
    request_id: String,
    item_id: String,
}

impl<'a> CursorRecord<'a> {
    fn new(record: &'a Value, top_type: &'a str, base_uid: &'a str, model_hint: &str) -> Self {
        let message = record.get("message").unwrap_or(record);
        let model = cursor_model_hint(record);
        let model = if model.is_empty() {
            canonicalize_model("cursor", model_hint)
        } else {
            model
        };

        Self {
            record,
            message,
            top_type,
            base_uid,
            role: cursor_role(record),
            model,
            request_id: first_string(record, &["requestId", "request_id", "turnId", "turn_id"])
                .or_else(|| {
                    first_string(message, &["requestId", "request_id", "turnId", "turn_id"])
                })
                .unwrap_or_default(),
            item_id: first_string(record, &["id", "messageId", "message_id"])
                .or_else(|| first_string(message, &["id", "messageId", "message_id"]))
                .unwrap_or_default(),
        }
    }

    fn content(&self) -> &'a Value {
        self.message
            .get("content")
            .or_else(|| self.record.get("content"))
            .or_else(|| self.message.get("text"))
            .or_else(|| self.record.get("text"))
            .unwrap_or_else(null_value)
    }

    fn actor(&self) -> &'static str {
        match self.role.as_str() {
            "user" => "user",
            "assistant" => "assistant",
            "tool" => "tool",
            "system" | "runtime" => "system",
            _ => match self.top_type {
                "user" => "user",
                "tool" => "tool",
                "system" | "runtime" => "system",
                _ => "assistant",
            },
        }
    }

    fn message_payload_type(&self) -> &'static str {
        match self.actor() {
            "user" => "user_message",
            "assistant" => "agent_message",
            "system" => "system",
            "tool" => "tool_result",
            _ => "message",
        }
    }

    fn stamp_common(&self, event: EventBuilder) -> EventBuilder {
        event
            .model(self.model.clone())
            .request_id(self.request_id.clone())
            .trace_id(self.request_id.clone())
            .item_id(self.item_id.clone())
    }
}

fn normalize_cursor_content_block(
    record: &CursorRecord<'_>,
    idx: usize,
    block: &Value,
    emitter: &mut SourceEmitter<'_>,
) {
    let block_type = cursor_block_type(block);
    match block_type.as_str() {
        "tool_use" | "tool_call" | "function_call" => {
            emit_cursor_tool_use(record, idx, block, emitter);
        }
        "tool_result" | "function_call_output" | "tool_call_result" => {
            emit_cursor_tool_result(record, idx, block, emitter);
        }
        "thinking" | "reasoning" => emit_cursor_reasoning(record, idx, block, emitter),
        _ => emit_cursor_text_block(record, idx, block, &block_type, emitter),
    }
}

fn emit_cursor_scalar_message(
    record: &CursorRecord<'_>,
    text: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let event = record.stamp_common(emitter.event(
        record.base_uid,
        "message",
        record.message_payload_type(),
        record.actor(),
        text,
        &compact_json(record.message),
    ));
    emitter.push_event(event);
}

fn emit_cursor_text_block(
    record: &CursorRecord<'_>,
    idx: usize,
    block: &Value,
    block_type: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let uid = cursor_block_uid(emitter, block, idx, "text");
    let text = cursor_text_or_json(block);
    let content_type = if block_type.is_empty() {
        "text"
    } else {
        block_type
    };
    let event = record
        .stamp_common(emitter.event_for_json(
            &uid,
            "message",
            record.message_payload_type(),
            record.actor(),
            &text,
            block,
        ))
        .content_types([content_type.to_string()]);
    emitter.push_event(event);
    emit_cursor_reference_links(&uid, block, emitter);
}

fn emit_cursor_reasoning(
    record: &CursorRecord<'_>,
    idx: usize,
    block: &Value,
    emitter: &mut SourceEmitter<'_>,
) {
    let uid = cursor_block_uid(emitter, block, idx, "reasoning");
    let text = cursor_text_or_json(block);
    let event = record
        .stamp_common(emitter.event_for_json(
            &uid,
            "reasoning",
            "reasoning",
            "assistant",
            &text,
            block,
        ))
        .has_reasoning(true)
        .content_types(["reasoning"]);
    emitter.push_event(event);
}

fn emit_cursor_tool_use(
    record: &CursorRecord<'_>,
    idx: usize,
    block: &Value,
    emitter: &mut SourceEmitter<'_>,
) {
    let uid = cursor_block_uid(emitter, block, idx, "tool_use");
    let tool_name = cursor_tool_name(block);
    let tool_call_id = cursor_tool_call_id(block).unwrap_or_else(|| uid.clone());
    let input = cursor_tool_input(block);
    let input_json = compact_json(input);
    let input_text = cursor_text_or_json(input);

    let event = record
        .stamp_common(emitter.event_for_json(
            &uid,
            "tool_call",
            "tool_use",
            "assistant",
            &input_text,
            block,
        ))
        .content_types(["tool_use"])
        .tool_call_id(tool_call_id.clone())
        .tool_name(tool_name.clone())
        .item_id(first_string(block, &["id"]).unwrap_or_default());

    emitter.push_event(event);
    emitter.push_tool_request(&uid, &tool_call_id, "", &tool_name, &input_json);
    emit_cursor_reference_links(&uid, block, emitter);
}

fn emit_cursor_tool_result(
    record: &CursorRecord<'_>,
    idx: usize,
    block: &Value,
    emitter: &mut SourceEmitter<'_>,
) {
    let uid = cursor_block_uid(emitter, block, idx, "tool_result");
    let tool_call_id = cursor_tool_call_id(block).unwrap_or_default();
    let tool_name = cursor_tool_name(block);
    let output = block
        .get("content")
        .or_else(|| block.get("output"))
        .or_else(|| block.get("result"))
        .unwrap_or(block);
    let output_json = compact_json(output);
    let output_text = cursor_text_or_json(output);
    let tool_error = to_u8_bool(block.get("is_error").or_else(|| block.get("error")));

    let event = record
        .stamp_common(emitter.event_for_json(
            &uid,
            "tool_result",
            "tool_result",
            "tool",
            &output_text,
            block,
        ))
        .content_types(["tool_result"])
        .tool_call_id(tool_call_id.clone())
        .tool_name(tool_name.clone())
        .tool_error(tool_error);

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
    emit_cursor_reference_links(&uid, output, emitter);
}

fn emit_cursor_unknown(record: &CursorRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let event = record.stamp_common(emitter.event_for_json(
        record.base_uid,
        "unknown",
        "unknown",
        record.actor(),
        &cursor_text_or_json(record.record),
        record.record,
    ));
    emitter.push_event(event);
}

fn emit_cursor_reference_links(event_uid: &str, value: &Value, emitter: &mut SourceEmitter<'_>) {
    let refs = prioritized_cursor_references(value);
    if refs.is_empty() {
        return;
    }

    let primary = &refs[0];
    let all_references = refs
        .iter()
        .map(|reference| {
            json!({
                "reference_kind": reference.kind,
                "field_path": reference.field_path,
                "target": reference.target,
            })
        })
        .collect::<Vec<_>>();
    let metadata = json!({
        "source": "cursor",
        "reference_kind": primary.kind,
        "field_path": primary.field_path,
        "references": all_references,
    });
    emitter.push_external_link(
        event_uid,
        &primary.target,
        "unknown",
        &compact_json(&metadata),
    );
}

#[derive(Debug, Clone)]
struct CursorReference {
    kind: &'static str,
    field_path: String,
    target: String,
}

fn cursor_references(value: &Value) -> Vec<CursorReference> {
    let mut refs = Vec::<CursorReference>::new();
    collect_cursor_references(value, "$", &mut refs);
    refs
}

fn prioritized_cursor_references(value: &Value) -> Vec<CursorReference> {
    let mut refs = cursor_references(value);
    refs.sort_by(|a, b| {
        cursor_reference_priority(a)
            .cmp(&cursor_reference_priority(b))
            .then_with(|| a.field_path.cmp(&b.field_path))
            .then_with(|| a.target.cmp(&b.target))
    });
    refs.dedup_by(|a, b| a.kind == b.kind && a.field_path == b.field_path && a.target == b.target);
    refs
}

fn cursor_reference_priority(reference: &CursorReference) -> u8 {
    let field = reference.field_path.as_str();
    match reference.kind {
        "file" if field.ends_with(".path") || field.ends_with(".file_path") => 0,
        "file" => 1,
        "code" => 2,
        "url" => 3,
        _ => 4,
    }
}

fn collect_cursor_references(value: &Value, path: &str, refs: &mut Vec<CursorReference>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}.{key}");
                if let Some(target) = child.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    if let Some(kind) = cursor_reference_kind(key, target) {
                        refs.push(CursorReference {
                            kind,
                            field_path: child_path.clone(),
                            target: target.to_string(),
                        });
                    }
                }
                collect_cursor_references(child, &child_path, refs);
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                collect_cursor_references(child, &format!("{path}.{idx}"), refs);
            }
        }
        _ => {}
    }
}

fn cursor_reference_kind(key: &str, target: &str) -> Option<&'static str> {
    let key = key.to_ascii_lowercase();
    if matches!(
        key.as_str(),
        "path"
            | "filename"
            | "file"
            | "filepath"
            | "file_path"
            | "target_file"
            | "target_directory"
            | "working_directory"
            | "glob_pattern"
    ) {
        return Some("file");
    }

    if key == "url" || target.starts_with("http://") || target.starts_with("https://") {
        return Some("url");
    }

    if matches!(key.as_str(), "range" | "selection" | "symbol" | "code") {
        return Some("code");
    }

    None
}

fn cursor_block_uid(
    emitter: &SourceEmitter<'_>,
    block: &Value,
    idx: usize,
    suffix: &str,
) -> String {
    emitter.uid_for_json(block, &format!("cursor:block:{idx}:{suffix}"))
}

fn cursor_block_type(block: &Value) -> String {
    first_string(block, &["type", "kind"])
        .unwrap_or_else(|| "text".to_string())
        .to_ascii_lowercase()
}

fn cursor_tool_name(block: &Value) -> String {
    first_string(block, &["name", "tool_name", "toolName"])
        .or_else(|| {
            block
                .get("function")
                .and_then(|function| first_string(function, &["name"]))
        })
        .unwrap_or_default()
}

fn cursor_tool_call_id(block: &Value) -> Option<String> {
    first_string(
        block,
        &[
            "id",
            "tool_use_id",
            "toolUseId",
            "tool_call_id",
            "toolCallId",
            "call_id",
            "callId",
        ],
    )
}

fn cursor_tool_input(block: &Value) -> &Value {
    block
        .get("input")
        .or_else(|| block.get("arguments"))
        .or_else(|| {
            block
                .get("function")
                .and_then(|function| function.get("arguments"))
        })
        .unwrap_or_else(null_value)
}

fn cursor_text_or_json(value: &Value) -> String {
    let text = extract_message_text(value);
    if text.is_empty() && !value.is_null() {
        compact_json(value)
    } else {
        text
    }
}

fn cursor_record_ts(record: &Value) -> String {
    first_string(
        record,
        &[
            "timestamp",
            "createdAt",
            "created_at",
            "updatedAt",
            "updated_at",
            "time",
        ],
    )
    .or_else(|| {
        record.get("message").and_then(|message| {
            first_string(
                message,
                &[
                    "timestamp",
                    "createdAt",
                    "created_at",
                    "updatedAt",
                    "updated_at",
                    "time",
                ],
            )
        })
    })
    .unwrap_or_default()
}

fn cursor_model_hint(record: &Value) -> String {
    let raw = first_string(
        record,
        &[
            "model",
            "modelName",
            "model_name",
            "modelId",
            "model_id",
            "composerModel",
        ],
    )
    .or_else(|| {
        record.get("message").and_then(|message| {
            first_string(
                message,
                &[
                    "model",
                    "modelName",
                    "model_name",
                    "modelId",
                    "model_id",
                    "composerModel",
                ],
            )
        })
    })
    .unwrap_or_default();
    canonicalize_model("cursor", &raw)
}

fn cursor_role(record: &Value) -> String {
    first_string(record, &["role"])
        .or_else(|| {
            record
                .get("message")
                .and_then(|message| first_string(message, &["role"]))
        })
        .map(|role| role.trim().to_ascii_lowercase())
        .unwrap_or_default()
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(value_to_non_empty_string)
            .map(ToOwned::to_owned)
    })
}

fn value_to_non_empty_string(value: &Value) -> Option<&str> {
    value.as_str().map(str::trim).filter(|s| !s.is_empty())
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}
