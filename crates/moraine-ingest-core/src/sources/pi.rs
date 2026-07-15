use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext,
};
use serde_json::{json, Value};

pub(crate) static PI_CODING_AGENT: PiCodingAgent = PiCodingAgent;

pub(crate) struct PiCodingAgent;

impl IngestSource for PiCodingAgent {
    fn harness(&self) -> &'static str {
        "pi-coding-agent"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        pi_source_metadata(record)
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if ctx.top_type == "session" {
            let session_id = to_str(record.get("id"));
            if !session_id.is_empty() {
                return session_id;
            }
        }

        if ctx.session_hint.is_empty() {
            infer_session_id_from_file(ctx.source_file)
        } else {
            ctx.session_hint.to_string()
        }
    }

    fn jsonl_carries_cwd(&self) -> bool {
        true
    }

    fn cwd(&self, record: &Value) -> String {
        // Only the `session` header record carries the working directory;
        // later records inherit it via the normalizer's session-level fallback.
        to_str(record.get("cwd"))
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        normalize_pi_record(record, ctx, top_type, base_uid, model_hint)
    }
}

fn pi_source_metadata(record: &Value) -> SourceMetadata {
    match to_str(record.get("type")).as_str() {
        "model_change" => SourceMetadata {
            inference_provider: to_str(record.get("provider")),
            model_hint_fallback: to_str(record.get("modelId")),
        },
        "message" => {
            let message = record.get("message").unwrap_or_else(null_value);
            SourceMetadata {
                inference_provider: to_str(message.get("provider")),
                model_hint_fallback: to_str(message.get("model")),
            }
        }
        _ => SourceMetadata::new(""),
    }
}

fn normalize_pi_record(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);

    match top_type {
        "session" => handle_pi_session(record, base_uid, &mut emitter),
        "message" => handle_pi_message(record, base_uid, model_hint, &mut emitter),
        "model_change" => handle_pi_model_change(record, base_uid, &mut emitter),
        "thinking_level_change" => handle_pi_thinking_level_change(record, base_uid, &mut emitter),
        "compaction" => handle_pi_compaction(record, base_uid, &mut emitter),
        "branch_summary" => handle_pi_branch_summary(record, base_uid, &mut emitter),
        "custom" => handle_pi_custom(record, base_uid, &mut emitter),
        "custom_message" => handle_pi_custom_message(record, base_uid, &mut emitter),
        "label" => handle_pi_label(record, base_uid, &mut emitter),
        "session_info" => handle_pi_session_info(record, base_uid, &mut emitter),
        _ => handle_pi_unknown(record, top_type, base_uid, &mut emitter),
    }

    let mut partials = emitter.finish();
    append_pi_parent_links(record, ctx, &mut partials);
    partials
}

fn handle_pi_session(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event_for_json(
            base_uid,
            "session_meta",
            "session_meta",
            "system",
            &to_str(record.get("cwd")),
            record,
        )
        .item_id(to_str(record.get("id")));
    emitter.push_event(event);
}

fn handle_pi_message(
    record: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let message = record.get("message").unwrap_or_else(null_value);
    match to_str(message.get("role")).as_str() {
        "user" => handle_pi_user_message(record, message, base_uid, model_hint, emitter),
        "assistant" => handle_pi_assistant_message(record, message, base_uid, model_hint, emitter),
        "toolResult" => {
            handle_pi_tool_result_message(record, message, base_uid, model_hint, emitter)
        }
        "bashExecution" => {
            handle_pi_bash_execution_message(record, message, base_uid, model_hint, emitter)
        }
        "custom" => handle_pi_custom_role_message(record, message, base_uid, model_hint, emitter),
        "branchSummary" => {
            handle_pi_branch_summary_message(record, message, base_uid, model_hint, emitter)
        }
        "compactionSummary" => {
            handle_pi_compaction_summary_message(record, message, base_uid, model_hint, emitter)
        }
        _ => handle_pi_unknown_message(record, message, base_uid, model_hint, emitter),
    }
}

fn handle_pi_user_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let content = message.get("content").unwrap_or_else(null_value);
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "message",
                "user_message",
                "user",
                &extract_message_text(content),
                message,
            )
            .content_types(pi_content_types(content))
            .item_id(to_str(record.get("id"))),
        message,
        model_hint,
    );
    emitter.push_event(event);
}

fn handle_pi_assistant_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let content = message.get("content").unwrap_or_else(null_value);
    let mut token_accounting = pi_token_accounting(message);
    let mut emitted = 0usize;

    if let Value::Array(blocks) = content {
        for (idx, block) in blocks.iter().enumerate() {
            let block_type = to_str(block.get("type"));
            let block_uid = emitter.uid_for_json(block, &format!("pi:assistant:block:{idx}"));
            let event = match block_type.as_str() {
                "thinking" => handle_pi_assistant_thinking_block(
                    message,
                    block,
                    &block_uid,
                    model_hint,
                    token_accounting.take(),
                    emitter,
                ),
                "toolCall" => handle_pi_assistant_tool_call_block(
                    message,
                    block,
                    &block_uid,
                    model_hint,
                    token_accounting.take(),
                    emitter,
                ),
                _ => handle_pi_assistant_text_block(
                    message,
                    block,
                    &block_uid,
                    &block_type,
                    model_hint,
                    token_accounting.take(),
                    emitter,
                ),
            };
            emitter.push_event(event.item_id(to_str(record.get("id"))));
            emitted += 1;
        }
    }

    if emitted == 0 {
        let event = pi_stamp_message_common(
            emitter
                .event_for_json(
                    base_uid,
                    "message",
                    "agent_message",
                    "assistant",
                    &extract_message_text(content),
                    message,
                )
                .content_types(pi_content_types(content))
                .item_id(to_str(record.get("id"))),
            message,
            model_hint,
        );
        emitter.push_event(pi_stamp_token_accounting(event, token_accounting.take()));
    }
}

fn handle_pi_assistant_text_block(
    message: &Value,
    block: &Value,
    block_uid: &str,
    block_type: &str,
    model_hint: &str,
    token_accounting: Option<TokenAccounting>,
    emitter: &SourceEmitter<'_>,
) -> EventBuilder {
    let payload_type = if block_type.is_empty() {
        "text"
    } else {
        block_type
    };
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                block_uid,
                "message",
                payload_type,
                "assistant",
                &extract_message_text(block),
                block,
            )
            .content_types(pi_content_types(block)),
        message,
        model_hint,
    );
    pi_stamp_token_accounting(event, token_accounting)
}

fn handle_pi_assistant_thinking_block(
    message: &Value,
    block: &Value,
    block_uid: &str,
    model_hint: &str,
    token_accounting: Option<TokenAccounting>,
    emitter: &SourceEmitter<'_>,
) -> EventBuilder {
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                block_uid,
                "reasoning",
                "thinking",
                "assistant",
                &extract_message_text(block),
                block,
            )
            .content_types(["reasoning"])
            .has_reasoning(true),
        message,
        model_hint,
    );
    pi_stamp_token_accounting(event, token_accounting)
}

fn handle_pi_assistant_tool_call_block(
    message: &Value,
    block: &Value,
    block_uid: &str,
    model_hint: &str,
    token_accounting: Option<TokenAccounting>,
    emitter: &mut SourceEmitter<'_>,
) -> EventBuilder {
    let tool_call_id = to_str(block.get("id"));
    let tool_name = to_str(block.get("name"));
    let input = block.get("arguments").unwrap_or_else(null_value);
    let input_json = compact_json(input);
    let input_text = {
        let text = extract_message_text(input);
        if text.is_empty() {
            input_json.clone()
        } else {
            text
        }
    };

    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                block_uid,
                "tool_call",
                "tool_use",
                "assistant",
                &input_text,
                block,
            )
            .content_types(["tool_use"])
            .tool_call_id(tool_call_id.clone())
            .tool_name(tool_name.clone()),
        message,
        model_hint,
    );
    emitter.push_tool_request(block_uid, &tool_call_id, "", &tool_name, &input_json);
    pi_stamp_token_accounting(event, token_accounting)
}

fn handle_pi_tool_result_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let content = message.get("content").unwrap_or_else(null_value);
    let tool_call_id = to_str(message.get("toolCallId"));
    let tool_name = to_str(message.get("toolName"));
    let output_text = extract_message_text(content);
    let output_json = compact_json(message);
    let tool_error = to_u8_bool(message.get("isError"));

    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "tool_result",
                "tool_result",
                "tool",
                &output_text,
                message,
            )
            .content_types(pi_content_types(content))
            .item_id(to_str(record.get("id")))
            .tool_call_id(tool_call_id.clone())
            .tool_name(tool_name.clone())
            .tool_error(tool_error),
        message,
        model_hint,
    );
    emitter.push_event(event);
    emitter.push_tool_response(
        base_uid,
        &tool_call_id,
        "",
        &tool_name,
        tool_error,
        "",
        &output_json,
        &output_text,
    );
}

fn handle_pi_bash_execution_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let command = to_str(message.get("command"));
    let output = to_str(message.get("output"));
    let tool_call_id = to_str(record.get("id"));
    let tool_error =
        u8::from(to_u32(message.get("exitCode")) != 0 || to_u8_bool(message.get("cancelled")) != 0);
    let input_json = compact_json(&json!({ "command": command }));
    let output_json = compact_json(message);

    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "tool_result",
                "tool_result",
                "tool",
                &output,
                message,
            )
            .content_types(["tool_result"])
            .item_id(to_str(record.get("id")))
            .tool_call_id(tool_call_id.clone())
            .tool_name("bash")
            .tool_error(tool_error),
        message,
        model_hint,
    );
    emitter.push_event(event);
    emitter
        .push_tool_request(base_uid, &tool_call_id, "", "bash", &input_json)
        .push_tool_response(
            base_uid,
            &tool_call_id,
            "",
            "bash",
            tool_error,
            &input_json,
            &output_json,
            &output,
        );
}

fn handle_pi_custom_role_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let content = message.get("content").unwrap_or_else(null_value);
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "message",
                "message",
                "system",
                &extract_message_text(content),
                message,
            )
            .content_types(pi_content_types(content))
            .item_id(to_str(record.get("id")))
            .op_kind(to_str(message.get("customType"))),
        message,
        model_hint,
    );
    emitter.push_event(event);
}

fn handle_pi_branch_summary_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "summary",
                "summary",
                "system",
                &to_str(message.get("summary")),
                message,
            )
            .item_id(to_str(record.get("id")))
            .origin_event_id(to_str(message.get("fromId"))),
        message,
        model_hint,
    );
    emitter.push_event(event);
}

fn handle_pi_compaction_summary_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "summary",
                "compacted",
                "system",
                &to_str(message.get("summary")),
                message,
            )
            .item_id(to_str(record.get("id"))),
        message,
        model_hint,
    );
    emitter.push_event(event);
}

fn handle_pi_unknown_message(
    record: &Value,
    message: &Value,
    base_uid: &str,
    model_hint: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let event = pi_stamp_message_common(
        emitter
            .event_for_json(
                base_uid,
                "unknown",
                "message",
                "system",
                &extract_message_text(message),
                message,
            )
            .item_id(to_str(record.get("id"))),
        message,
        model_hint,
    );
    emitter.push_event(event);
}

fn handle_pi_model_change(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let provider = to_str(record.get("provider"));
    let model = canonicalize_model("pi-coding-agent", &to_str(record.get("modelId")));
    let text = if provider.is_empty() {
        model.clone()
    } else {
        format!("{provider}/{model}")
    };
    let event = emitter
        .event_for_json(base_uid, "system", "system", "system", &text, record)
        .item_id(to_str(record.get("id")))
        .op_kind("model_change")
        .model(model);
    emitter.push_event(event);
}

fn handle_pi_thinking_level_change(
    record: &Value,
    base_uid: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let event = emitter
        .event_for_json(
            base_uid,
            "system",
            "thinking",
            "system",
            &to_str(record.get("thinkingLevel")),
            record,
        )
        .item_id(to_str(record.get("id")))
        .op_kind("thinking_level_change");
    emitter.push_event(event);
}

fn handle_pi_compaction(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event_for_json(
            base_uid,
            "summary",
            "compacted",
            "system",
            &to_str(record.get("summary")),
            record,
        )
        .item_id(to_str(record.get("id")))
        .origin_event_id(to_str(record.get("firstKeptEntryId")));
    emitter.push_event(event);
}

fn handle_pi_branch_summary(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event_for_json(
            base_uid,
            "summary",
            "summary",
            "system",
            &to_str(record.get("summary")),
            record,
        )
        .item_id(to_str(record.get("id")))
        .origin_event_id(to_str(record.get("fromId")));
    emitter.push_event(event);
}

fn handle_pi_custom(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event_for_json(
            base_uid,
            "system",
            "system",
            "system",
            &extract_message_text(record.get("data").unwrap_or_else(null_value)),
            record,
        )
        .item_id(to_str(record.get("id")))
        .op_kind(to_str(record.get("customType")));
    emitter.push_event(event);
}

fn handle_pi_custom_message(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let content = record.get("content").unwrap_or_else(null_value);
    let event = emitter
        .event_for_json(
            base_uid,
            "message",
            "message",
            "system",
            &extract_message_text(content),
            record,
        )
        .content_types(pi_content_types(content))
        .item_id(to_str(record.get("id")))
        .op_kind(to_str(record.get("customType")));
    emitter.push_event(event);
}

fn handle_pi_label(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event_for_json(
            base_uid,
            "system",
            "system",
            "system",
            &to_str(record.get("label")),
            record,
        )
        .item_id(to_str(record.get("id")))
        .op_kind("label")
        .origin_event_id(to_str(record.get("targetId")));
    emitter.push_event(event);
}

fn handle_pi_session_info(record: &Value, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event_for_json(
            base_uid,
            "session_meta",
            "session_meta",
            "system",
            &to_str(record.get("name")),
            record,
        )
        .item_id(to_str(record.get("id")))
        .op_kind("session_info");
    emitter.push_event(event);
}

fn handle_pi_unknown(
    record: &Value,
    top_type: &str,
    base_uid: &str,
    emitter: &mut SourceEmitter<'_>,
) {
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

fn pi_stamp_message_common(event: EventBuilder, message: &Value, model_hint: &str) -> EventBuilder {
    let model = canonicalize_model("pi-coding-agent", &to_str(message.get("model")));
    let model = if model.is_empty() {
        canonicalize_model("pi-coding-agent", model_hint)
    } else {
        model
    };
    let event = if model.is_empty() {
        event
    } else {
        event.model(model)
    };

    let stop_reason = to_str(message.get("stopReason"));
    if stop_reason.is_empty() {
        event
    } else {
        event.op_status(stop_reason)
    }
}

fn pi_stamp_token_accounting(
    event: EventBuilder,
    token_accounting: Option<TokenAccounting>,
) -> EventBuilder {
    if let Some(token_accounting) = token_accounting {
        event.token_accounting(token_accounting)
    } else {
        event
    }
}

fn pi_token_accounting(message: &Value) -> Option<TokenAccounting> {
    let usage = message.get("usage")?;
    if usage.is_null() {
        return None;
    }

    Some(
        TokenAccounting::generation(
            to_u64(usage.get("input")),
            to_u64(usage.get("output")),
            to_u64(usage.get("cacheRead")),
            to_u64(usage.get("cacheWrite")),
        )
        .with_raw_usage(Some(usage)),
    )
}

fn pi_content_types(content: &Value) -> Vec<String> {
    match content {
        Value::String(value) => {
            if value.trim().is_empty() {
                Vec::new()
            } else {
                vec!["text".to_string()]
            }
        }
        Value::Array(items) => {
            let mut out = items
                .iter()
                .filter_map(|item| match to_str(item.get("type")).as_str() {
                    "" => None,
                    "toolCall" => Some("tool_use".to_string()),
                    "thinking" => Some("reasoning".to_string()),
                    other => Some(other.to_string()),
                })
                .collect::<Vec<_>>();
            out.sort();
            out.dedup();
            out
        }
        Value::Object(_) => match to_str(content.get("type")).as_str() {
            "" => Vec::new(),
            "toolCall" => vec!["tool_use".to_string()],
            "thinking" => vec!["reasoning".to_string()],
            other => vec![other.to_string()],
        },
        _ => Vec::new(),
    }
}

fn append_pi_parent_links(
    record: &Value,
    ctx: &RecordContext<'_>,
    partials: &mut NormalizedPartials,
) {
    let parent_id = to_str(record.get("parentId"));
    if parent_id.is_empty() {
        return;
    }

    let event_uids = partials
        .event_rows
        .iter()
        .filter_map(|row| row.get("event_uid").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();

    for event_uid in event_uids {
        partials.push_link(build_external_link_row(
            ctx,
            &event_uid,
            &parent_id,
            "parent_event",
            "{}",
        ));
    }
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}
