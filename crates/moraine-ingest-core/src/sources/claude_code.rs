use super::shared::*;
use super::{IngestSource, NormalizedPartials, SourceRecordContext};
use serde_json::{json, Map, Value};

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
        normalize_claude_event(record, ctx, top_type, base_uid).into()
    }
}

fn normalize_claude_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let mut links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let parent_uuid = to_str(record.get("parentUuid"));
    let request_id = to_str(record.get("requestId"));
    let trace_id = to_str(record.get("requestId"));
    let agent_run_id = to_str(record.get("agentId"));
    let agent_label = to_str(record.get("agentName"));
    let coord_group_label = to_str(record.get("teamName"));
    let is_substream = to_u8_bool(record.get("isSidechain"));

    let message = record.get("message").cloned().unwrap_or(Value::Null);
    let msg_role = to_str(message.get("role"));
    let model = canonicalize_model("claude-code", &to_str(message.get("model")));

    let usage = message.get("usage").cloned().unwrap_or(Value::Null);
    let input_tokens = to_u32(usage.get("input_tokens"));
    let output_tokens = to_u32(usage.get("output_tokens"));
    let cache_read_tokens = to_u32(usage.get("cache_read_input_tokens"));
    let cache_write_tokens = to_u32(usage.get("cache_creation_input_tokens"));
    let service_tier = to_str(usage.get("service_tier"));
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
    let canonical_output_text =
        (output_tokens as u64).saturating_sub(reasoning_tokens + server_tool_use_tokens);
    let canonical_buckets = token_buckets(&[
        ("input_text", input_tokens as u64),
        ("output_text", canonical_output_text),
        ("input_cache_read", cache_read_tokens as u64),
        ("input_cache_write", cache_write_tokens as u64),
        ("reasoning", reasoning_tokens),
        ("server_tool_use", server_tool_use_tokens),
    ]);

    let stamp_common = |obj: &mut Map<String, Value>| {
        obj.insert("request_id".to_string(), json!(request_id.clone()));
        obj.insert("trace_id".to_string(), json!(trace_id.clone()));
        obj.insert("agent_run_id".to_string(), json!(agent_run_id.clone()));
        obj.insert("agent_label".to_string(), json!(agent_label.clone()));
        obj.insert(
            "coord_group_label".to_string(),
            json!(coord_group_label.clone()),
        );
        obj.insert("is_substream".to_string(), json!(is_substream));
        obj.insert("model".to_string(), json!(model.clone()));
        obj.insert("input_tokens".to_string(), json!(input_tokens));
        obj.insert("output_tokens".to_string(), json!(output_tokens));
        obj.insert("cache_read_tokens".to_string(), json!(cache_read_tokens));
        obj.insert("cache_write_tokens".to_string(), json!(cache_write_tokens));
        stamp_token_accounting(
            obj,
            "generation",
            canonical_buckets.clone(),
            token_native_units(&[]),
        );
        obj.insert("service_tier".to_string(), json!(service_tier.clone()));
        obj.insert("item_id".to_string(), json!(to_str(record.get("uuid"))));
        obj.insert(
            "origin_event_id".to_string(),
            json!(to_str(record.get("sourceToolAssistantUUID"))),
        );
        obj.insert(
            "origin_tool_call_id".to_string(),
            json!(to_str(record.get("sourceToolUseID"))),
        );
    };

    if top_type == "assistant" || top_type == "user" {
        let actor = if top_type == "assistant" {
            "assistant"
        } else if msg_role == "assistant" {
            "assistant"
        } else {
            "user"
        };

        let content = message.get("content").cloned().unwrap_or_else(|| {
            record
                .get("message")
                .and_then(|m| m.get("content"))
                .cloned()
                .unwrap_or(Value::Null)
        });

        match content {
            Value::Array(items) if !items.is_empty() => {
                for (idx, item) in items.iter().enumerate() {
                    let block_type = to_str(item.get("type"));
                    let suffix = format!("claude:block:{}", idx);
                    let block_uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &compact_json(item),
                        &suffix,
                    );

                    let mut row = match block_type.as_str() {
                        "thinking" => {
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "reasoning",
                                "reasoning",
                                "assistant",
                                &extract_message_text(item),
                                &compact_json(item),
                            );
                            mark_reasoning_metadata(&mut r);
                            r
                        }
                        "tool_use" => {
                            let tool_call_id = to_str(item.get("id"));
                            let tool_name = to_str(item.get("name"));
                            let input_json =
                                compact_json(item.get("input").unwrap_or(&Value::Null));
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "tool_call",
                                "tool_use",
                                "assistant",
                                &extract_message_text(item.get("input").unwrap_or(&Value::Null)),
                                &compact_json(item),
                            );
                            r.insert("content_types".to_string(), json!(["tool_use"]));
                            r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                            r.insert("tool_name".to_string(), json!(tool_name.clone()));
                            tools.push(build_tool_row(
                                ctx,
                                &block_uid,
                                &tool_call_id,
                                &to_str(record.get("parentToolUseID")),
                                &tool_name,
                                "request",
                                0,
                                &input_json,
                                "",
                                "",
                            ));
                            r
                        }
                        "tool_result" => {
                            let tool_call_id = to_str(item.get("tool_use_id"));
                            let output_json =
                                compact_json(item.get("content").unwrap_or(&Value::Null));
                            let output_text =
                                extract_message_text(item.get("content").unwrap_or(&Value::Null));
                            let tool_error = to_u8_bool(item.get("is_error"));
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "tool_result",
                                "tool_result",
                                "tool",
                                &output_text,
                                &compact_json(item),
                            );
                            r.insert("content_types".to_string(), json!(["tool_result"]));
                            r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                            r.insert("tool_error".to_string(), json!(tool_error));
                            tools.push(build_tool_row(
                                ctx,
                                &block_uid,
                                &tool_call_id,
                                &to_str(record.get("parentToolUseID")),
                                "",
                                "response",
                                tool_error,
                                "",
                                &output_json,
                                &output_text,
                            ));
                            r
                        }
                        _ => {
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "message",
                                if block_type.is_empty() {
                                    "text"
                                } else {
                                    block_type.as_str()
                                },
                                actor,
                                &extract_message_text(item),
                                &compact_json(item),
                            );
                            if !block_type.is_empty() {
                                r.insert("content_types".to_string(), json!([block_type]));
                            }
                            r
                        }
                    };

                    stamp_common(&mut row);
                    row.insert(
                        "parent_tool_call_id".to_string(),
                        json!(to_str(record.get("parentToolUseID"))),
                    );
                    row.insert(
                        "origin_tool_call_id".to_string(),
                        json!(to_str(record.get("sourceToolUseID"))),
                    );
                    row.insert(
                        "tool_phase".to_string(),
                        json!(to_str(record.get("stop_reason"))),
                    );
                    events.push(Value::Object(row));

                    if !parent_uuid.is_empty() {
                        links.push(build_external_link_row(
                            ctx,
                            &block_uid,
                            &parent_uuid,
                            "parent_uuid",
                            "{}",
                        ));
                    }
                }
            }
            _ => {
                let text = extract_message_text(&message);
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "message",
                    "message",
                    actor,
                    &text,
                    &compact_json(record),
                );
                row.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(
                        message.get("content").unwrap_or(&Value::Null)
                    )),
                );
                stamp_common(&mut row);
                events.push(Value::Object(row));
                if !parent_uuid.is_empty() {
                    links.push(build_external_link_row(
                        ctx,
                        base_uid,
                        &parent_uuid,
                        "parent_uuid",
                        "{}",
                    ));
                }
            }
        }
    } else {
        let event_kind = match top_type {
            "progress" => "progress",
            "system" => "system",
            "summary" => "summary",
            "queue-operation" => "queue_operation",
            "file-history-snapshot" => "file_history_snapshot",
            _ => "unknown",
        };

        let payload_type = if top_type == "progress" {
            to_str(record.get("data").and_then(|d| d.get("type")))
        } else if top_type == "system" {
            to_str(record.get("subtype"))
        } else {
            top_type.to_string()
        };

        let mut row = base_event_obj(
            ctx,
            base_uid,
            event_kind,
            if payload_type.is_empty() {
                top_type
            } else {
                payload_type.as_str()
            },
            "system",
            &extract_message_text(record),
            &compact_json(record),
        );
        row.insert("op_kind".to_string(), json!(payload_type));
        row.insert("op_status".to_string(), json!(to_str(record.get("status"))));
        row.insert(
            "latency_ms".to_string(),
            json!(to_u32(record.get("durationMs"))),
        );
        row.insert(
            "retry_count".to_string(),
            json!(to_u16(record.get("retryAttempt"))),
        );
        stamp_common(&mut row);
        events.push(Value::Object(row));

        if !parent_uuid.is_empty() {
            links.push(build_external_link_row(
                ctx,
                base_uid,
                &parent_uuid,
                "parent_uuid",
                "{}",
            ));
        }
    }

    if !events.is_empty() {
        let tool_use_id = to_str(record.get("toolUseID"));
        if !tool_use_id.is_empty() {
            if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
                links.push(build_external_link_row(
                    ctx,
                    uid,
                    &tool_use_id,
                    "tool_use_id",
                    "{}",
                ));
            }
        }

        let source_tool_assistant = to_str(record.get("sourceToolAssistantUUID"));
        if !source_tool_assistant.is_empty() {
            if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
                links.push(build_external_link_row(
                    ctx,
                    uid,
                    &source_tool_assistant,
                    "source_tool_assistant",
                    "{}",
                ));
            }
        }
    }

    (events, links, tools)
}
