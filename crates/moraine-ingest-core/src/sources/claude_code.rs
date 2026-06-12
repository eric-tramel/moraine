use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceRecordContext,
};
use serde_json::{json, Value};

pub(crate) static CLAUDE_CODE: ClaudeCode = ClaudeCode;

pub(crate) struct ClaudeCode;

impl IngestSource for ClaudeCode {
    fn harness(&self) -> &'static str {
        "claude-code"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("anthropic")
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let session_id = to_str(record.get("sessionId"));
        if !session_id.is_empty() {
            session_id
        } else if ctx.session_hint.is_empty() {
            infer_session_id_from_file(ctx.source_file)
        } else {
            ctx.session_hint.to_string()
        }
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        _model_hint: &str,
    ) -> NormalizedPartials {
        normalize_claude_event(record, ctx, top_type, base_uid)
    }
}

fn normalize_claude_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
) -> NormalizedPartials {
    let record = ClaudeRecord::new(record, top_type, base_uid);
    let mut emitter = SourceEmitter::new(ctx);

    if record.is_message_record() {
        normalize_claude_message_record(&record, &mut emitter);
    } else {
        normalize_claude_operational_record(&record, &mut emitter);
    }

    let mut partials = emitter.finish();
    append_claude_record_links(&record, ctx, &mut partials);
    partials
}

/// The working directory a Claude Code record reports, or `""` when absent.
///
/// Claude Code stamps `cwd` as a top-level field on its conversational lines
/// (user/assistant/tool messages) but not on every operational line
/// (`mode`, `file-history-snapshot`, …), and — unlike codex/pi/cursor — emits
/// no dedicated session-scoped record. The normalized content events keep
/// only their content block as payload, so the cwd never reaches storage on
/// its own. The ingest dispatch loop reads it here and synthesizes one
/// `session_meta` per session (see `build_claude_cwd_session_meta`) so
/// `--project-only` MCP scoping can derive a claude session's origin
/// directory the same way it does for the other harnesses.
pub(crate) fn claude_record_cwd(record: &Value) -> String {
    to_str(record.get("cwd"))
}

/// Build a synthetic `session_meta` event row carrying `cwd`, derived from a
/// real event row of the same record (`template`) so it inherits the correct
/// `session_id`, timestamps, and source coordinates. The dispatch loop emits
/// exactly one of these per session per scan, on the first cwd-bearing
/// record, so it lands at a real conversational event's timestamp (keeping
/// `first_event_time` honest) rather than once per line.
///
/// `--project-only` scoping resolves a session's origin with
/// `argMinIf(first non-empty cwd)`, so the at-most-one extra row a later
/// incremental scan may add is deduplicated at query time.
pub(crate) fn build_claude_cwd_session_meta(template: &Value, cwd: &str) -> Option<Value> {
    let mut row = template.as_object()?.clone();

    let source_file = template.get("source_file").and_then(Value::as_str)?;
    let source_generation = template
        .get("source_generation")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    let source_line_no = template
        .get("source_line_no")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let source_offset = template
        .get("source_offset")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let uid = event_uid(
        source_file,
        source_generation,
        source_line_no,
        source_offset,
        cwd,
        "claude:session_meta:cwd",
    );

    let payload_json = compact_json(&json!({ "cwd": cwd }));
    // Core identity/content fields define a session_meta event and must always
    // be present.
    for (key, value) in [
        ("event_uid", uid.as_str()),
        ("event_kind", "session_meta"),
        ("payload_type", "session_meta"),
        ("actor_kind", "system"),
        ("text_content", cwd),
        ("text_preview", cwd),
        ("payload_json", payload_json.as_str()),
    ] {
        row.insert(key.to_string(), Value::String(value.to_string()));
    }
    // Clear every field carried over from the conversational template that
    // would misrepresent a session_meta event (it has no model, tool, agent,
    // or content semantics of its own). Only touch keys the template actually
    // has, so the synthetic row keeps the exact column set of a normal event.
    for key in [
        "item_id",
        "tool_call_id",
        "parent_tool_call_id",
        "origin_event_id",
        "origin_tool_call_id",
        "tool_name",
        "tool_phase",
        "model",
        "agent_run_id",
        "agent_label",
        "coord_group_id",
        "coord_group_label",
        "request_id",
        "trace_id",
        "op_kind",
        "op_status",
        "service_tier",
        "token_usage_json",
    ] {
        clear_if_present(&mut row, key, Value::String(String::new()));
    }
    if row.contains_key("content_types") {
        row.insert("content_types".to_string(), json!([]));
    }
    for key in ["has_reasoning", "is_substream", "tool_error"] {
        clear_if_present(&mut row, key, json!(0u8));
    }
    for key in ["input_tokens", "output_tokens", "latency_ms", "turn_index"] {
        clear_if_present(&mut row, key, json!(0u32));
    }

    Some(Value::Object(row))
}

fn clear_if_present(row: &mut serde_json::Map<String, Value>, key: &str, value: Value) {
    if row.contains_key(key) {
        row.insert(key.to_string(), value);
    }
}

struct ClaudeRecord<'a> {
    record: &'a Value,
    message: &'a Value,
    top_type: &'a str,
    base_uid: &'a str,
    parent_uuid: String,
    request_id: String,
    trace_id: String,
    agent_run_id: String,
    agent_label: String,
    coord_group_label: String,
    is_substream: u8,
    model: String,
    token_accounting: TokenAccounting,
    service_tier: String,
    item_id: String,
    parent_tool_call_id: String,
    origin_event_id: String,
    origin_tool_call_id: String,
    tool_use_id: String,
    tool_phase: String,
}

impl<'a> ClaudeRecord<'a> {
    fn new(record: &'a Value, top_type: &'a str, base_uid: &'a str) -> Self {
        let message = record.get("message").unwrap_or_else(null_value);
        let usage = message.get("usage").unwrap_or_else(null_value);
        let input_tokens = to_u32(usage.get("input_tokens"));
        let output_tokens = to_u32(usage.get("output_tokens"));
        let cache_read_tokens = to_u32(usage.get("cache_read_input_tokens"));
        let cache_write_tokens = to_u32(usage.get("cache_creation_input_tokens"));
        let reasoning_tokens = to_u64(
            usage
                .get("reasoning_tokens")
                .or_else(|| usage.get("thinking_tokens"))
                .or_else(|| {
                    usage
                        .get("output_tokens_details")
                        .and_then(|details| details.get("reasoning_tokens"))
                }),
        );
        let server_tool_use_tokens = sum_numeric_object(usage.get("server_tool_use"));
        let output_text =
            (output_tokens as u64).saturating_sub(reasoning_tokens + server_tool_use_tokens);
        let token_accounting = TokenAccounting::new(TokenEndpointKind::Generation)
            .with_buckets(&[
                ("input_text", input_tokens as u64),
                ("output_text", output_text),
                ("input_cache_read", cache_read_tokens as u64),
                ("input_cache_write", cache_write_tokens as u64),
                ("reasoning", reasoning_tokens),
                ("server_tool_use", server_tool_use_tokens),
            ])
            .with_legacy_scalars(
                input_tokens as u64,
                output_tokens as u64,
                cache_read_tokens as u64,
                cache_write_tokens as u64,
            );

        Self {
            record,
            message,
            top_type,
            base_uid,
            parent_uuid: to_str(record.get("parentUuid")),
            request_id: to_str(record.get("requestId")),
            trace_id: to_str(record.get("requestId")),
            agent_run_id: to_str(record.get("agentId")),
            agent_label: to_str(record.get("agentName")),
            coord_group_label: to_str(record.get("teamName")),
            is_substream: to_u8_bool(record.get("isSidechain")),
            model: canonicalize_model("claude-code", &to_str(message.get("model"))),
            token_accounting,
            service_tier: to_str(usage.get("service_tier")),
            item_id: to_str(record.get("uuid")),
            parent_tool_call_id: to_str(record.get("parentToolUseID")),
            origin_event_id: to_str(record.get("sourceToolAssistantUUID")),
            origin_tool_call_id: to_str(record.get("sourceToolUseID")),
            tool_use_id: to_str(record.get("toolUseID")),
            tool_phase: to_str(record.get("stop_reason")),
        }
    }

    fn is_message_record(&self) -> bool {
        self.top_type == "assistant" || self.top_type == "user"
    }

    fn stamp_common_event(&self, event: EventBuilder) -> EventBuilder {
        event
            .request_id(self.request_id.clone())
            .trace_id(self.trace_id.clone())
            .agent_run_id(self.agent_run_id.clone())
            .agent_label(self.agent_label.clone())
            .coord_group_label(self.coord_group_label.clone())
            .substream(self.is_substream != 0)
            .model(self.model.clone())
            .token_accounting(self.token_accounting.clone())
            .service_tier(self.service_tier.clone())
            .item_id(self.item_id.clone())
            .origin_event_id(self.origin_event_id.clone())
            .origin_tool_call_id(self.origin_tool_call_id.clone())
    }

    fn stamp_content_block_event(&self, event: EventBuilder) -> EventBuilder {
        self.stamp_common_event(event)
            .parent_tool_call_id(self.parent_tool_call_id.clone())
            .origin_tool_call_id(self.origin_tool_call_id.clone())
            .tool_phase(self.tool_phase.clone())
    }
}

fn normalize_claude_message_record(record: &ClaudeRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let actor = claude_actor_for_message(record.top_type, record.message);
    let content = record.message.get("content").unwrap_or_else(null_value);

    match content {
        Value::Array(items) if !items.is_empty() => {
            normalize_claude_content_blocks(record, emitter, actor, items);
        }
        _ => normalize_claude_scalar_message(record, emitter, actor),
    }
}

fn claude_actor_for_message(top_type: &str, message: &Value) -> &'static str {
    if top_type == "assistant" || to_str(message.get("role")) == "assistant" {
        "assistant"
    } else {
        "user"
    }
}

fn normalize_claude_content_blocks(
    record: &ClaudeRecord<'_>,
    emitter: &mut SourceEmitter<'_>,
    actor: &str,
    items: &[Value],
) {
    for (idx, item) in items.iter().enumerate() {
        normalize_claude_content_block(record, emitter, actor, idx, item);
    }
}

fn normalize_claude_content_block(
    record: &ClaudeRecord<'_>,
    emitter: &mut SourceEmitter<'_>,
    actor: &str,
    idx: usize,
    item: &Value,
) {
    let block_type = to_str(item.get("type"));
    let suffix = format!("claude:block:{idx}");
    let block_uid = emitter.uid_for_json(item, &suffix);
    let event = match block_type.as_str() {
        "thinking" => handle_claude_thinking_block(record, emitter, &block_uid, item),
        "tool_use" => handle_claude_tool_use_block(record, emitter, &block_uid, item),
        "tool_result" => handle_claude_tool_result_block(record, emitter, &block_uid, item),
        _ => handle_claude_text_block(record, emitter, &block_uid, item, actor, &block_type),
    };
    emitter.push_event(event);
}

fn handle_claude_thinking_block(
    record: &ClaudeRecord<'_>,
    emitter: &SourceEmitter<'_>,
    block_uid: &str,
    item: &Value,
) -> EventBuilder {
    record.stamp_content_block_event(
        emitter
            .event_for_json(
                block_uid,
                "reasoning",
                "reasoning",
                "assistant",
                &extract_message_text(item),
                item,
            )
            .has_reasoning(true)
            .content_types(["reasoning"]),
    )
}

fn handle_claude_tool_use_block(
    record: &ClaudeRecord<'_>,
    emitter: &mut SourceEmitter<'_>,
    block_uid: &str,
    item: &Value,
) -> EventBuilder {
    let tool_call_id = to_str(item.get("id"));
    let tool_name = to_str(item.get("name"));
    let input = item.get("input").unwrap_or_else(null_value);
    let input_json = compact_json(input);
    let event = record.stamp_content_block_event(
        emitter
            .event_for_json(
                block_uid,
                "tool_call",
                "tool_use",
                "assistant",
                &extract_message_text(input),
                item,
            )
            .content_types(["tool_use"])
            .tool_call_id(tool_call_id.clone())
            .tool_name(tool_name.clone()),
    );

    emitter.push_tool_request(
        block_uid,
        &tool_call_id,
        &record.parent_tool_call_id,
        &tool_name,
        &input_json,
    );

    event
}

fn handle_claude_tool_result_block(
    record: &ClaudeRecord<'_>,
    emitter: &mut SourceEmitter<'_>,
    block_uid: &str,
    item: &Value,
) -> EventBuilder {
    let tool_call_id = to_str(item.get("tool_use_id"));
    let output = item.get("content").unwrap_or_else(null_value);
    let output_json = compact_json(output);
    let output_text = extract_message_text(output);
    let tool_error = to_u8_bool(item.get("is_error"));
    let event = record.stamp_content_block_event(
        emitter
            .event_for_json(
                block_uid,
                "tool_result",
                "tool_result",
                "tool",
                &output_text,
                item,
            )
            .content_types(["tool_result"])
            .tool_call_id(tool_call_id.clone())
            .tool_error(tool_error),
    );

    emitter.push_tool_response(
        block_uid,
        &tool_call_id,
        &record.parent_tool_call_id,
        "",
        tool_error,
        "",
        &output_json,
        &output_text,
    );

    event
}

fn handle_claude_text_block(
    record: &ClaudeRecord<'_>,
    emitter: &SourceEmitter<'_>,
    block_uid: &str,
    item: &Value,
    actor: &str,
    block_type: &str,
) -> EventBuilder {
    let payload_type = if block_type.is_empty() {
        "text"
    } else {
        block_type
    };
    let mut event = emitter.event_for_json(
        block_uid,
        "message",
        payload_type,
        actor,
        &extract_message_text(item),
        item,
    );
    if !block_type.is_empty() {
        event = event.content_types([block_type]);
    }
    record.stamp_content_block_event(event)
}

fn normalize_claude_scalar_message(
    record: &ClaudeRecord<'_>,
    emitter: &mut SourceEmitter<'_>,
    actor: &str,
) {
    let content = record.message.get("content").unwrap_or_else(null_value);
    let event = record
        .stamp_common_event(emitter.event(
            record.base_uid,
            "message",
            "message",
            actor,
            &extract_message_text(record.message),
            &compact_json(record.record),
        ))
        .content_types(extract_content_types(content));
    emitter.push_event(event);
}

fn normalize_claude_operational_record(record: &ClaudeRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let event_kind = claude_event_kind_for_top_type(record.top_type);
    let payload_type = claude_payload_type(record);
    let event_payload_type = if payload_type.is_empty() {
        record.top_type
    } else {
        payload_type.as_str()
    };
    let event = record
        .stamp_common_event(emitter.event(
            record.base_uid,
            event_kind,
            event_payload_type,
            "system",
            &extract_message_text(record.record),
            &compact_json(record.record),
        ))
        .op_kind(payload_type)
        .op_status(to_str(record.record.get("status")))
        .latency_ms(to_u32(record.record.get("durationMs")))
        .retry_count(to_u16(record.record.get("retryAttempt")));
    emitter.push_event(event);
}

fn claude_event_kind_for_top_type(top_type: &str) -> &'static str {
    match top_type {
        "progress" => "progress",
        "system" => "system",
        "summary" => "summary",
        "queue-operation" => "queue_operation",
        "file-history-snapshot" => "file_history_snapshot",
        _ => "unknown",
    }
}

fn claude_payload_type(record: &ClaudeRecord<'_>) -> String {
    if record.top_type == "progress" {
        to_str(record.record.get("data").and_then(|data| data.get("type")))
    } else if record.top_type == "system" {
        to_str(record.record.get("subtype"))
    } else {
        record.top_type.to_string()
    }
}

fn append_claude_record_links(
    record: &ClaudeRecord<'_>,
    ctx: &RecordContext<'_>,
    partials: &mut NormalizedPartials,
) {
    let event_uids = partials
        .event_rows
        .iter()
        .filter_map(|row| row.get("event_uid").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();

    if !record.parent_uuid.is_empty() {
        for event_uid in &event_uids {
            partials.push_link(build_external_link_row(
                ctx,
                event_uid,
                &record.parent_uuid,
                "parent_uuid",
                "{}",
            ));
        }
    }

    let Some(first_uid) = event_uids.first() else {
        return;
    };

    if !record.tool_use_id.is_empty() {
        partials.push_link(build_external_link_row(
            ctx,
            first_uid,
            &record.tool_use_id,
            "tool_use_id",
            "{}",
        ));
    }

    if !record.origin_event_id.is_empty() {
        partials.push_link(build_external_link_row(
            ctx,
            first_uid,
            &record.origin_event_id,
            "source_tool_assistant",
            "{}",
        ));
    }
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}
