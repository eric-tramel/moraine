use super::shared::*;
use super::{IngestSource, NormalizedPartials, Preflight, SourceRecordContext};
use serde_json::{json, Map, Value};

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
        normalize_kimi_cli_wire_event(record, ctx, top_type, base_uid).into()
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

fn kimi_cli_event_uid(ctx: &RecordContext<'_>, record_fingerprint: &str, suffix: &str) -> String {
    event_uid(
        ctx.source_file,
        ctx.source_generation,
        ctx.source_line_no,
        ctx.source_offset,
        record_fingerprint,
        &format!("kimi-cli:{suffix}"),
    )
}

fn normalize_kimi_cli_wire_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    _base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let message = record.get("message").unwrap_or(record);
    let msg_type = {
        let message_type = to_str(message.get("type"));
        if message_type.is_empty() {
            top_type.to_string()
        } else {
            message_type
        }
    };
    let payload = message.get("payload").unwrap_or(record);
    let payload_json = compact_json(payload);

    let mut push_progress = |suffix: &str, kind: &str, text: String, payload_type: &str| {
        let uid = kimi_cli_event_uid(ctx, &payload_json, suffix);
        let mut row = base_event_obj(
            ctx,
            &uid,
            kind,
            payload_type,
            "system",
            &text,
            &payload_json,
        );
        row.insert("op_kind".to_string(), json!(msg_type));
        events.push(Value::Object(row));
    };

    match msg_type.as_str() {
        "TurnBegin" | "SteerInput" => {
            let input = payload.get("user_input").unwrap_or(&Value::Null);
            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:user_input");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "message",
                "user_message",
                "user",
                &extract_message_text(input),
                &payload_json,
            );
            row.insert(
                "content_types".to_string(),
                json!(extract_content_types(input)),
            );
            events.push(Value::Object(row));
        }
        "TurnEnd" => {
            push_progress("wire:turn_end", "summary", String::new(), "summary");
        }
        "StepBegin" => {
            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:step_begin");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "progress",
                "progress",
                "system",
                "",
                &payload_json,
            );
            row.insert("turn_index".to_string(), json!(to_u32(payload.get("n"))));
            row.insert("op_kind".to_string(), json!("step_begin"));
            events.push(Value::Object(row));
        }
        "StepInterrupted" => {
            push_progress(
                "wire:step_interrupted",
                "progress",
                extract_message_text(payload),
                "progress",
            );
        }
        "ContentPart" => {
            let part_type = to_str(payload.get("type"));
            match part_type.as_str() {
                "think" => {
                    let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:content:think");
                    let text = to_str(payload.get("think"));
                    let mut row = base_event_obj(
                        ctx,
                        &uid,
                        "reasoning",
                        "reasoning",
                        "assistant",
                        &text,
                        &payload_json,
                    );
                    mark_reasoning_metadata(&mut row);
                    events.push(Value::Object(row));
                }
                _ => {
                    let text = if let Some(text) = payload.get("text") {
                        extract_message_text(text)
                    } else {
                        extract_message_text(payload)
                    };
                    let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:content:text");
                    let mut row = base_event_obj(
                        ctx,
                        &uid,
                        "message",
                        "agent_message",
                        "assistant",
                        &text,
                        &payload_json,
                    );
                    if !part_type.is_empty() {
                        row.insert("content_types".to_string(), json!([part_type]));
                    }
                    events.push(Value::Object(row));
                }
            }
        }
        "ToolCall" => {
            let function = payload.get("function").unwrap_or(&Value::Null);
            let tool_name = to_str(function.get("name"));
            let arguments = to_str(function.get("arguments"));
            let tool_call_id = to_str(payload.get("id"));
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

            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:tool_call");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "tool_call",
                "tool_use",
                "assistant",
                &input_text,
                &payload_json,
            );
            row.insert("content_types".to_string(), json!(["tool_use"]));
            row.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
            row.insert("tool_name".to_string(), json!(tool_name.clone()));
            events.push(Value::Object(row));

            tools.push(build_tool_row(
                ctx,
                &uid,
                &tool_call_id,
                "",
                &tool_name,
                "request",
                0,
                &input_json,
                "",
                "",
            ));
        }
        "ToolCallPart" => {
            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:tool_call_part");
            let args_part = to_str(payload.get("arguments_part"));
            let mut row = base_event_obj(
                ctx,
                &uid,
                "progress",
                "tool_use",
                "assistant",
                &args_part,
                &payload_json,
            );
            row.insert("op_kind".to_string(), json!("tool_call_part"));
            events.push(Value::Object(row));
        }
        "ToolResult" => {
            let tool_call_id = to_str(payload.get("tool_call_id"));
            let return_value = payload.get("return_value").unwrap_or(&Value::Null);
            let is_error = to_u8_bool(return_value.get("is_error"));
            let output = to_str(return_value.get("output"));
            let message_text = to_str(return_value.get("message"));

            let output_text = if !output.is_empty() {
                output.clone()
            } else {
                message_text.clone()
            };
            let output_json = compact_json(return_value);

            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:tool_result");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "tool_result",
                "tool_result",
                "tool",
                &output_text,
                &payload_json,
            );
            row.insert("content_types".to_string(), json!(["tool_result"]));
            row.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
            row.insert("tool_error".to_string(), json!(is_error));
            events.push(Value::Object(row));

            tools.push(build_tool_row(
                ctx,
                &uid,
                &tool_call_id,
                "",
                "",
                "response",
                is_error,
                "",
                &output_json,
                &output_text,
            ));
        }
        "StatusUpdate" => {
            let token_usage = payload.get("token_usage").unwrap_or(&Value::Null);
            let input_other = to_u32(token_usage.get("input_other"));
            let input_cache_read = to_u32(token_usage.get("input_cache_read"));
            let input_cache_creation = to_u32(token_usage.get("input_cache_creation"));
            let output = to_u32(token_usage.get("output"));

            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:status_update");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "event_msg",
                "token_count",
                "system",
                "",
                &payload_json,
            );
            row.insert(
                "input_tokens".to_string(),
                json!(input_other + input_cache_read + input_cache_creation),
            );
            row.insert("output_tokens".to_string(), json!(output));
            row.insert("cache_read_tokens".to_string(), json!(input_cache_read));
            row.insert(
                "cache_write_tokens".to_string(),
                json!(input_cache_creation),
            );
            stamp_token_accounting(
                &mut row,
                "generation",
                generation_token_buckets(
                    input_other as u64,
                    output as u64,
                    input_cache_read as u64,
                    input_cache_creation as u64,
                ),
                token_native_units(&[]),
            );
            row.insert(
                "token_usage_json".to_string(),
                json!(compact_json(token_usage)),
            );
            row.insert(
                "item_id".to_string(),
                json!(to_str(payload.get("message_id"))),
            );
            events.push(Value::Object(row));
        }
        "CompactionBegin" | "CompactionEnd" => {
            push_progress("wire:compaction", "summary", String::new(), "summary");
        }
        "HookTriggered" | "HookResolved" => {
            let event = to_str(payload.get("event"));
            let target = to_str(payload.get("target"));
            let text = if !event.is_empty() && !target.is_empty() {
                format!("{event}: {target}")
            } else {
                event
            };
            push_progress("wire:hook", "event_msg", text, "event_msg");
        }
        "SubagentEvent" => {
            // Kimi writes every sub-agent event twice: once as a SubagentEvent
            // envelope on the parent wire, and once as the real event inside
            // `<session>/subagents/<agent_id>/wire.jsonl`. Emitting a normalized
            // row here would double-count every sub-agent event as `progress`
            // on the parent session (see #271). Skip the normalized row; the
            // raw_events row is still written from the enclosing dispatcher,
            // so the byte-level trace is preserved.
        }
        "MCPLoadingBegin" | "MCPLoadingEnd" | "MCPStatusSnapshot" | "BtwBegin" | "BtwEnd"
        | "Notification" | "PlanDisplay" | "ApprovalRequest" | "ApprovalResponse"
        | "QuestionRequest" | "QuestionResponse" => {
            push_progress(
                &format!("wire:{msg_type}"),
                "progress",
                extract_message_text(payload),
                "progress",
            );
        }
        _ => {
            push_progress(
                "wire:unknown",
                "unknown",
                extract_message_text(payload),
                "unknown",
            );
        }
    }

    // Why: the Kimi wire schema (MoonshotAI/kimi-cli wire/types.py) has no
    // record that carries the active model name, and neither do the sibling
    // context.jsonl / state.json files. Without a placeholder, every Kimi
    // event has an empty `model` and the monitor's tokens-by-model analytics
    // (which filters out empty-model rows) never shows them. Stamping the
    // harness slug here guarantees a single "kimi-cli" series so Kimi usage
    // is at least visible cross-harness, even though we can't distinguish
    // between configured Kimi models.
    for row in events.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            obj.insert("model".to_string(), json!("kimi-cli"));
        }
    }

    (events, links, tools)
}
