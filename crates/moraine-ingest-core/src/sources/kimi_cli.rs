use super::shared::*;
use super::{
    emitter::SourceEmitter, record_view::RecordView, IngestSource, NormalizedPartials, Preflight,
    SourceRecordContext,
};
use serde_json::{json, Map, Value};
use std::io::{BufRead, BufReader, Read};

pub(crate) static KIMI_CLI: KimiCli = KimiCli;

pub(crate) struct KimiCli;

impl IngestSource for KimiCli {
    fn harness(&self) -> &'static str {
        "kimi-cli"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("moonshot")
    }

    fn preflight<'a>(&self, record: &'a Value) -> Preflight<'a> {
        if record.get("message").is_none()
            && record.get("type").and_then(Value::as_str) == Some("metadata")
        {
            Preflight::Skip
        } else {
            Preflight::Keep(record)
        }
    }

    fn record_ts(&self, record: &Value) -> String {
        kimi_wire_record_ts(record)
    }

    fn top_type(&self, record: &Value) -> String {
        let message_type = to_str(record.get("message").and_then(|v| v.get("type")));
        if !message_type.is_empty() {
            message_type
        } else {
            to_str(record.get("type"))
        }
    }

    fn session_id(&self, _record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        kimi_session_id(ctx.source_file, ctx.session_hint)
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        _model_hint: &str,
    ) -> NormalizedPartials {
        normalize_kimi_cli_wire_event(record, ctx, top_type, base_uid)
    }
}

fn kimi_session_id(source_file: &str, session_hint: &str) -> String {
    if !session_hint.is_empty() {
        return session_hint.to_string();
    }
    let path = std::path::Path::new(source_file);
    if let Some(parent) = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
    {
        if !parent.is_empty() {
            return format!("kimi-cli:{parent}");
        }
    }
    infer_session_id_from_file(source_file)
}

fn kimi_subagent_parent_session_id(source_file: &str) -> Option<String> {
    let path = std::path::Path::new(source_file);
    if path.file_name()?.to_str()? != "wire.jsonl" {
        return None;
    }

    let agent_dir = path.parent()?;
    let subagents_dir = agent_dir.parent()?;
    if subagents_dir.file_name()?.to_str()? != "subagents" {
        return None;
    }

    let parent_session_id = subagents_dir.parent()?.file_name()?.to_str()?;
    if parent_session_id.is_empty() {
        return None;
    }

    Some(format!("kimi-cli:{parent_session_id}"))
}

fn has_prior_kimi_turn_begin(source_file: &str, source_offset: u64) -> bool {
    let Ok(file) = std::fs::File::open(source_file) else {
        return false;
    };

    BufReader::new(file)
        .take(source_offset)
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| serde_json::from_str::<Value>(&line).ok())
        .any(|record| {
            record
                .get("message")
                .and_then(|message| message.get("type"))
                .and_then(Value::as_str)
                == Some("TurnBegin")
        })
}

fn link_kimi_subagent_to_parent(
    ctx: &RecordContext<'_>,
    wire: &KimiWireRecord<'_>,
    partials: &mut NormalizedPartials,
) {
    // A Kimi wire file can contain multiple turns. Inspect only the already
    // consumed prefix so this relationship is attached to the stream's first
    // TurnBegin across blank lines, skipped records, and incremental resumes.
    if wire.msg_type() != "TurnBegin"
        || !ctx.session_hint.is_empty()
        || has_prior_kimi_turn_begin(ctx.source_file, ctx.source_offset)
    {
        return;
    }

    let Some(parent_session_id) = kimi_subagent_parent_session_id(ctx.source_file) else {
        return;
    };
    let Some(event_uid) = partials.event_rows.first().and_then(|event| {
        event
            .get("event_uid")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }) else {
        return;
    };

    partials.link_rows.push(build_external_link_row(
        ctx,
        &event_uid,
        &parent_session_id,
        "subagent_parent",
        "{}",
    ));
}

/// Kimi wire records carry `timestamp` as a unix-seconds float
/// (e.g. `1775953944.549974`). Convert to the RFC3339 string the shared
/// `parse_event_ts` path expects; return "" when the field is absent so
/// the caller can decide how to handle it (the leading `metadata` header
/// line is skipped upstream, so no real event reaches this function
/// without a timestamp in practice).
///
/// Parses the decimal string form first to preserve sub-microsecond
/// precision — `as_f64()` would round `1775953944.549974` down by a tick.
fn kimi_wire_record_ts(record: &Value) -> String {
    let (decimal, fallback) = match record.get("timestamp") {
        Some(Value::Number(n)) => (n.to_string(), n.as_f64()),
        Some(Value::String(s)) => {
            let trimmed = s.trim().to_string();
            let f = trimmed.parse::<f64>().ok();
            (trimmed, f)
        }
        _ => return String::new(),
    };
    format_unix_seconds_decimal(&decimal)
        .or_else(|| fallback.and_then(format_unix_seconds_ts))
        .unwrap_or_default()
}

fn kimi_cli_event_uid(
    emitter: &SourceEmitter<'_>,
    record_fingerprint: &str,
    suffix: &str,
) -> String {
    emitter.uid(record_fingerprint, &format!("kimi-cli:{suffix}"))
}

fn normalize_kimi_cli_wire_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    _base_uid: &str,
) -> NormalizedPartials {
    let wire = KimiWireRecord::new(record, top_type);
    let mut emitter = SourceEmitter::new(ctx);

    dispatch_kimi_wire_record(&wire, &mut emitter);

    let mut partials = emitter.finish();
    link_kimi_subagent_to_parent(ctx, &wire, &mut partials);
    stamp_kimi_placeholder_model(&mut partials.event_rows);
    partials
}

struct KimiWireRecord<'a> {
    msg_type: String,
    payload: &'a Value,
    payload_json: String,
}

impl<'a> KimiWireRecord<'a> {
    fn new(record: &'a Value, top_type: &str) -> Self {
        let view = RecordView::new(record);
        let message = view.get("message").unwrap_or(record);
        let message_view = RecordView::new(message);
        let message_type = message_view.string_field("type");
        let msg_type = if message_type.is_empty() {
            top_type.to_string()
        } else {
            message_type
        };
        let payload = message_view.get("payload").unwrap_or(record);
        let payload_json = compact_json(payload);

        Self {
            msg_type,
            payload,
            payload_json,
        }
    }

    fn msg_type(&self) -> &str {
        &self.msg_type
    }

    fn payload(&self) -> &'a Value {
        self.payload
    }

    fn payload_json(&self) -> &str {
        &self.payload_json
    }

    fn payload_field(&self, key: &str) -> Option<&'a Value> {
        self.payload.get(key)
    }
}

fn dispatch_kimi_wire_record<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    match wire.msg_type() {
        "TurnBegin" | "SteerInput" => handle_kimi_turn_begin(wire, emitter),
        "TurnEnd" => {
            handle_kimi_generic_progress(wire, emitter, "wire:turn_end", "summary", "", "summary")
        }
        "StepBegin" => handle_kimi_step_begin(wire, emitter),
        "StepInterrupted" => handle_kimi_generic_progress(
            wire,
            emitter,
            "wire:step_interrupted",
            "progress",
            &extract_message_text(wire.payload()),
            "progress",
        ),
        "ContentPart" => handle_kimi_content_part(wire, emitter),
        "ToolCall" => handle_kimi_tool_call(wire, emitter),
        "ToolCallPart" => handle_kimi_tool_call_part(wire, emitter),
        "ToolResult" => handle_kimi_tool_result(wire, emitter),
        "StatusUpdate" => handle_kimi_status_update(wire, emitter),
        "CompactionBegin" | "CompactionEnd" => {
            handle_kimi_generic_progress(wire, emitter, "wire:compaction", "summary", "", "summary")
        }
        "HookTriggered" | "HookResolved" => handle_kimi_hook_event(wire, emitter),
        "SubagentEvent" => handle_kimi_subagent_event(),
        "MCPLoadingBegin" | "MCPLoadingEnd" | "MCPStatusSnapshot" | "BtwBegin" | "BtwEnd"
        | "Notification" | "PlanDisplay" | "ApprovalRequest" | "ApprovalResponse"
        | "QuestionRequest" | "QuestionResponse" => {
            let suffix = format!("wire:{}", wire.msg_type());
            handle_kimi_generic_progress(
                wire,
                emitter,
                &suffix,
                "progress",
                &extract_message_text(wire.payload()),
                "progress",
            );
        }
        _ => handle_kimi_generic_progress(
            wire,
            emitter,
            "wire:unknown",
            "unknown",
            &extract_message_text(wire.payload()),
            "unknown",
        ),
    }
}

fn handle_kimi_turn_begin<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let input = wire.payload_field("user_input").unwrap_or_else(null_value);
    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:user_input");
    let event = emitter
        .event(
            &uid,
            "message",
            "user_message",
            "user",
            &extract_message_text(input),
            wire.payload_json(),
        )
        .content_types(extract_content_types(input));
    emitter.push_event(event);
}

fn handle_kimi_step_begin<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:step_begin");
    let event = emitter
        .event(
            &uid,
            "progress",
            "progress",
            "system",
            "",
            wire.payload_json(),
        )
        .turn_index(to_u32(wire.payload_field("n")))
        .op_kind("step_begin");
    emitter.push_event(event);
}

fn handle_kimi_content_part<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let part_type = to_str(wire.payload_field("type"));
    match part_type.as_str() {
        "think" => {
            let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:content:think");
            let text = to_str(wire.payload_field("think"));
            let event = emitter
                .event(
                    &uid,
                    "reasoning",
                    "reasoning",
                    "assistant",
                    &text,
                    wire.payload_json(),
                )
                .has_reasoning(true)
                .content_types(["reasoning"]);
            emitter.push_event(event);
        }
        _ => {
            let text = if let Some(text) = wire.payload_field("text") {
                extract_message_text(text)
            } else {
                extract_message_text(wire.payload())
            };
            let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:content:text");
            let mut event = emitter.event(
                &uid,
                "message",
                "agent_message",
                "assistant",
                &text,
                wire.payload_json(),
            );
            if !part_type.is_empty() {
                event = event.content_types([part_type]);
            }
            emitter.push_event(event);
        }
    }
}

fn handle_kimi_tool_call<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let function = wire.payload_field("function").unwrap_or_else(null_value);
    let tool_name = to_str(function.get("name"));
    let arguments = to_str(function.get("arguments"));
    let tool_call_id = to_str(wire.payload_field("id"));
    let args = parse_json_string(&arguments).unwrap_or_else(|| {
        if arguments.is_empty() {
            Value::Object(Map::new())
        } else {
            json!({ "raw": arguments })
        }
    });
    let input_json = compact_json(&args);
    let input_text = {
        let extracted = extract_message_text(&args);
        if extracted.is_empty() {
            input_json.clone()
        } else {
            extracted
        }
    };

    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:tool_call");
    let event = emitter
        .event(
            &uid,
            "tool_call",
            "tool_use",
            "assistant",
            &input_text,
            wire.payload_json(),
        )
        .content_types(["tool_use"])
        .tool_call_id(tool_call_id.clone())
        .tool_name(tool_name.clone());
    emitter.push_event(event);
    emitter.push_tool_request(&uid, &tool_call_id, "", &tool_name, &input_json);
}

fn handle_kimi_tool_call_part<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:tool_call_part");
    let args_part = to_str(wire.payload_field("arguments_part"));
    let event = emitter
        .event(
            &uid,
            "progress",
            "tool_use",
            "assistant",
            &args_part,
            wire.payload_json(),
        )
        .op_kind("tool_call_part");
    emitter.push_event(event);
}

fn handle_kimi_tool_result<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let tool_call_id = to_str(wire.payload_field("tool_call_id"));
    let return_value = wire
        .payload_field("return_value")
        .unwrap_or_else(null_value);
    let is_error = to_u8_bool(return_value.get("is_error"));
    let output = to_str(return_value.get("output"));
    let message_text = to_str(return_value.get("message"));

    let output_text = if !output.is_empty() {
        output.clone()
    } else {
        message_text.clone()
    };
    let output_json = compact_json(return_value);

    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:tool_result");
    let event = emitter
        .event(
            &uid,
            "tool_result",
            "tool_result",
            "tool",
            &output_text,
            wire.payload_json(),
        )
        .content_types(["tool_result"])
        .tool_call_id(tool_call_id.clone())
        .tool_error(is_error);
    emitter.push_event(event);
    emitter.push_tool_response(
        &uid,
        &tool_call_id,
        "",
        "",
        is_error,
        "",
        &output_json,
        &output_text,
    );
}

fn handle_kimi_status_update<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let token_usage = wire.payload_field("token_usage").unwrap_or_else(null_value);
    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), "wire:status_update");
    let event = emitter
        .event(
            &uid,
            "event_msg",
            "token_count",
            "system",
            "",
            wire.payload_json(),
        )
        .token_accounting(TokenAccounting::kimi_generation(Some(token_usage)))
        .item_id(to_str(wire.payload_field("message_id")));
    emitter.push_event(event);
}

fn handle_kimi_hook_event<'a>(wire: &KimiWireRecord<'a>, emitter: &mut SourceEmitter<'_>) {
    let event = to_str(wire.payload_field("event"));
    let target = to_str(wire.payload_field("target"));
    let text = if !event.is_empty() && !target.is_empty() {
        format!("{event}: {target}")
    } else {
        event
    };
    handle_kimi_generic_progress(wire, emitter, "wire:hook", "event_msg", &text, "event_msg");
}

fn handle_kimi_generic_progress(
    wire: &KimiWireRecord<'_>,
    emitter: &mut SourceEmitter<'_>,
    suffix: &str,
    kind: &str,
    text: &str,
    payload_type: &str,
) {
    let uid = kimi_cli_event_uid(emitter, wire.payload_json(), suffix);
    let event = emitter
        .event(
            &uid,
            kind,
            payload_type,
            "system",
            text,
            wire.payload_json(),
        )
        .op_kind(wire.msg_type());
    emitter.push_event(event);
}

fn handle_kimi_subagent_event() {
    // Kimi writes every sub-agent event twice: once as a SubagentEvent envelope
    // on the parent wire, and once as the real event inside
    // `<session>/subagents/<agent_id>/wire.jsonl`. Emitting a normalized row
    // here would double-count every sub-agent event as `progress` on the parent
    // session (see #271). Skip the normalized row; the raw_events row is still
    // written from the enclosing dispatcher, so the byte-level trace is
    // preserved.
}

fn stamp_kimi_placeholder_model(events: &mut [Value]) {
    // Why: the Kimi wire schema (MoonshotAI/kimi-cli wire/types.py) has no
    // record that carries the active model name, and neither do the sibling
    // context.jsonl / state.json files. Without a placeholder, every Kimi event
    // has an empty `model` and the monitor's tokens-by-model analytics (which
    // filters out empty-model rows) never shows them. Stamping the harness slug
    // here guarantees a single "kimi-cli" series so Kimi usage is at least
    // visible cross-harness, even though we can't distinguish between
    // configured Kimi models.
    for row in events {
        if let Some(obj) = row.as_object_mut() {
            obj.insert("model".to_string(), json!("kimi-cli"));
        }
    }
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}
