use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceRecordContext,
};
use serde_json::{json, Map, Value};

pub(crate) static CODEX: Codex = Codex;

pub(crate) struct Codex;

impl IngestSource for Codex {
    fn harness(&self) -> &'static str {
        "codex"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("openai")
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if ctx.top_type == "session_meta" {
            let payload = record.get("payload").cloned().unwrap_or(Value::Null);
            let payload_id = to_str(payload.get("id"));
            if !payload_id.is_empty() {
                return payload_id;
            }
        }

        if ctx.session_hint.is_empty() {
            infer_session_id_from_file(ctx.source_file)
        } else {
            ctx.session_hint.to_string()
        }
    }

    fn cwd(&self, record: &Value) -> String {
        // Codex carries the working directory in `payload.cwd` on
        // `session_meta` (and on `turn_context` in newer rollouts); other
        // records inherit it via the normalizer's session-level fallback.
        to_str(record.pointer("/payload/cwd"))
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        normalize_codex_event(record, ctx, top_type, base_uid, model_hint)
    }
}

fn normalize_codex_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> NormalizedPartials {
    let codex_record = build_codex_record(record, top_type, base_uid, model_hint);
    let mut emitter = SourceEmitter::new(ctx);

    route_codex_record(&codex_record, &mut emitter);

    let mut partials = emitter.finish();
    stamp_codex_model_fallbacks(&codex_record, &mut partials.event_rows);
    append_codex_parent_link(&codex_record, ctx, &mut partials);
    partials
}

struct CodexRecord<'a> {
    record: &'a Value,
    top_type: &'a str,
    base_uid: &'a str,
    model_hint: &'a str,
    payload: Value,
    payload_obj: Map<String, Value>,
    payload_json: String,
}

impl<'a> CodexRecord<'a> {
    fn payload(&self) -> &Value {
        &self.payload
    }

    fn payload_field(&self, key: &str) -> Option<&Value> {
        self.payload_obj.get(key)
    }

    fn payload_json(&self) -> &str {
        &self.payload_json
    }

    fn payload_type(&self) -> String {
        to_str(self.payload_field("type"))
    }
}

fn build_codex_record<'a>(
    record: &'a Value,
    top_type: &'a str,
    base_uid: &'a str,
    model_hint: &'a str,
) -> CodexRecord<'a> {
    let payload = record.get("payload").cloned().unwrap_or(Value::Null);
    let payload_obj = payload.as_object().cloned().unwrap_or_else(Map::new);
    let payload_json = compact_json(&Value::Object(payload_obj.clone()));

    CodexRecord {
        record,
        top_type,
        base_uid,
        model_hint,
        payload,
        payload_obj,
        payload_json,
    }
}

fn route_codex_record(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    match record.top_type {
        "session_meta" => normalize_codex_session_meta(record, emitter),
        "turn_context" => normalize_codex_turn_context(record, emitter),
        "response_item" => normalize_codex_response_item(record, emitter),
        "event_msg" => normalize_codex_event_msg(record, emitter),
        "compacted" => normalize_codex_compacted(record, emitter),
        "message" | "function_call" | "function_call_output" | "reasoning" => {
            normalize_codex_legacy_top_level(record, emitter)
        }
        _ => normalize_codex_unknown_top_level(record, emitter),
    }
}

fn normalize_codex_session_meta(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let event = emitter
        .event(
            record.base_uid,
            "session_meta",
            "session_meta",
            "system",
            "",
            record.payload_json(),
        )
        .item_id(to_str(record.payload_field("id")));
    emitter.push_event(event);
}

fn normalize_codex_turn_context(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let mut event = emitter
        .event(
            record.base_uid,
            "turn_context",
            "turn_context",
            "system",
            "",
            record.payload_json(),
        )
        .turn_index(to_u32(record.payload_field("turn_id")));

    let turn_id = to_str(record.payload_field("turn_id"));
    if !turn_id.is_empty() {
        event = event.request_id(turn_id.clone()).item_id(turn_id);
    }

    let model = canonicalize_model("codex", &to_str(record.payload_field("model")));
    if !model.is_empty() {
        event = event.model(model);
    }

    emitter.push_event(event);
}

fn normalize_codex_response_item(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let payload_type = record.payload_type();
    match payload_type.as_str() {
        "message" => handle_codex_response_message(record, emitter),
        "function_call" => handle_codex_function_call(
            record.payload(),
            record.base_uid,
            record.payload_json(),
            emitter,
        ),
        "function_call_output" => handle_codex_function_call_output(
            record.payload(),
            record.base_uid,
            record.payload_json(),
            emitter,
        ),
        "custom_tool_call" => handle_codex_custom_tool_call(record, emitter),
        "custom_tool_call_output" => handle_codex_custom_tool_call_output(record, emitter),
        "web_search_call" => handle_codex_web_search_call(record, emitter),
        "reasoning" => handle_codex_reasoning(
            record.payload(),
            record.base_uid,
            record.payload_json(),
            true,
            emitter,
        ),
        _ => handle_codex_unknown_response_item(record, &payload_type, emitter),
    }
}

fn handle_codex_response_message(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    emit_codex_message(
        record.payload(),
        record.base_uid,
        record.payload_json(),
        true,
        emitter,
    );
}

fn emit_codex_message(
    item: &Value,
    event_uid: &str,
    payload_json: &str,
    include_response_fields: bool,
    emitter: &mut SourceEmitter<'_>,
) {
    let role = to_str(item.get("role"));
    let content = item.get("content").unwrap_or_else(null_value);
    let mut event = emitter
        .event(
            event_uid,
            "message",
            "message",
            if role.is_empty() {
                "assistant"
            } else {
                role.as_str()
            },
            &extract_message_text(content),
            payload_json,
        )
        .content_types(extract_content_types(content));

    if include_response_fields {
        event = event
            .item_id(to_str(item.get("id")))
            .op_status(to_str(item.get("phase")));
    }

    emitter.push_event(event);
}

fn handle_codex_function_call(
    item: &Value,
    event_uid: &str,
    payload_json: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let args = to_str(item.get("arguments"));
    let call_id = to_str(item.get("call_id"));
    let name = to_str(item.get("name"));
    let event = emitter
        .event(
            event_uid,
            "tool_call",
            "function_call",
            "assistant",
            &args,
            payload_json,
        )
        .tool_call_id(call_id.clone())
        .tool_name(name.clone());
    emitter.push_event(event);
    emitter.push_tool_request(event_uid, &call_id, "", &name, &args);
}

fn handle_codex_function_call_output(
    item: &Value,
    event_uid: &str,
    payload_json: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let output = to_str(item.get("output"));
    let call_id = to_str(item.get("call_id"));
    let output_json = compact_json(item.get("output").unwrap_or_else(null_value));
    let event = emitter
        .event(
            event_uid,
            "tool_result",
            "function_call_output",
            "tool",
            &output,
            payload_json,
        )
        .tool_call_id(call_id.clone());
    emitter.push_event(event);
    emitter.push_tool_response(event_uid, &call_id, "", "", 0, "", &output_json, &output);
}

fn handle_codex_custom_tool_call(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let input = to_str(record.payload_field("input"));
    let call_id = to_str(record.payload_field("call_id"));
    let name = to_str(record.payload_field("name"));
    let status = to_str(record.payload_field("status"));
    let event = emitter
        .event(
            record.base_uid,
            "tool_call",
            "custom_tool_call",
            "assistant",
            &input,
            record.payload_json(),
        )
        .tool_call_id(call_id.clone())
        .tool_name(name.clone())
        .op_status(status);
    emitter.push_event(event);
    emitter.push_tool_request(record.base_uid, &call_id, "", &name, &input);
}

fn handle_codex_custom_tool_call_output(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let output = to_str(record.payload_field("output"));
    let call_id = to_str(record.payload_field("call_id"));
    let status = to_str(record.payload_field("status"));
    let output_json = serde_json::from_str::<Value>(&output)
        .map(|parsed| compact_json(&parsed))
        .unwrap_or_else(|_| {
            compact_json(record.payload_field("output").unwrap_or_else(null_value))
        });

    let event = emitter
        .event(
            record.base_uid,
            "tool_result",
            "custom_tool_call_output",
            "tool",
            &output,
            record.payload_json(),
        )
        .tool_call_id(call_id.clone())
        .op_status(status);
    emitter.push_event(event);
    emitter.push_tool_response(
        record.base_uid,
        &call_id,
        "",
        "",
        0,
        "",
        &output_json,
        &output,
    );
}

fn handle_codex_web_search_call(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let action = record
        .payload_field("action")
        .cloned()
        .unwrap_or(Value::Null);
    let action_type = to_str(action.get("type"));
    let status = to_str(record.payload_field("status"));
    let event = emitter
        .event(
            record.base_uid,
            "tool_call",
            "web_search_call",
            "assistant",
            &extract_message_text(&action),
            record.payload_json(),
        )
        .tool_name("web_search")
        .op_kind(action_type)
        .op_status(status.clone())
        .tool_phase(status);
    emitter.push_event(event);
}

fn handle_codex_reasoning(
    item: &Value,
    event_uid: &str,
    payload_json: &str,
    include_item_id: bool,
    emitter: &mut SourceEmitter<'_>,
) {
    let summary = item.get("summary").cloned().unwrap_or(Value::Null);
    let mut event = emitter
        .event(
            event_uid,
            "reasoning",
            "reasoning",
            "assistant",
            &extract_message_text(&summary),
            payload_json,
        )
        .has_reasoning(true)
        .content_types(["reasoning"]);

    if include_item_id {
        event = event.item_id(to_str(item.get("id")));
    }

    emitter.push_event(event);
}

fn handle_codex_unknown_response_item(
    record: &CodexRecord<'_>,
    payload_type: &str,
    emitter: &mut SourceEmitter<'_>,
) {
    let payload_type = if payload_type.is_empty() {
        "response_item"
    } else {
        payload_type
    };
    let event = emitter.event(
        record.base_uid,
        "unknown",
        payload_type,
        "system",
        &extract_message_text(record.payload()),
        record.payload_json(),
    );
    emitter.push_event(event);
}

fn normalize_codex_event_msg(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let payload_type = record.payload_type();
    let actor = match payload_type.as_str() {
        "user_message" => "user",
        "agent_message" | "agent_reasoning" => "assistant",
        _ => "system",
    };
    let payload_type_for_row = if payload_type.is_empty() {
        "event_msg"
    } else {
        payload_type.as_str()
    };

    let mut event = emitter.event(
        record.base_uid,
        "event_msg",
        payload_type_for_row,
        actor,
        &extract_message_text(record.payload()),
        record.payload_json(),
    );

    let turn_id = to_str(record.payload_field("turn_id"));
    if !turn_id.is_empty() {
        event = event.request_id(turn_id.clone()).item_id(turn_id);
    }

    let status = to_str(record.payload_field("status"));
    if !status.is_empty() {
        event = event.op_status(status);
    }

    if payload_type == "token_count" {
        event = stamp_codex_token_count(record, event);
    } else if payload_type == "agent_reasoning" {
        event = event.has_reasoning(true).content_types(["reasoning"]);
    }

    emitter.push_event(event);
}

fn stamp_codex_token_count(record: &CodexRecord<'_>, event: EventBuilder) -> EventBuilder {
    let usage = record
        .payload_field("info")
        .and_then(|v| v.get("last_token_usage"));
    let input_tokens = to_u32(usage.and_then(|v| v.get("input_tokens")));
    let output_tokens = to_u32(usage.and_then(|v| v.get("output_tokens")));
    let cache_read_tokens = to_u32(
        usage
            .and_then(|v| v.get("cached_input_tokens"))
            .or_else(|| usage.and_then(|v| v.get("cache_read_input_tokens"))),
    );
    let cache_write_tokens = to_u32(
        usage
            .and_then(|v| v.get("cache_creation_input_tokens"))
            .or_else(|| usage.and_then(|v| v.get("cache_write_input_tokens"))),
    );
    let canonical_buckets = openai_generation_token_buckets(usage);
    let accounting =
        TokenAccounting::from_parts("generation", canonical_buckets, token_native_units(&[]))
            .with_legacy_scalars(
                input_tokens as u64,
                output_tokens as u64,
                cache_read_tokens as u64,
                cache_write_tokens as u64,
            )
            .with_raw_usage_json(record.payload_json().to_string());

    let mut event = event.token_accounting(accounting).service_tier(to_str(
        record
            .payload_field("rate_limits")
            .and_then(|v| v.get("plan_type")),
    ));

    let resolved_model = resolve_codex_token_count_model(record);
    if !resolved_model.is_empty() {
        event = event.model(resolved_model);
    }

    event
}

fn resolve_codex_token_count_model(record: &CodexRecord<'_>) -> String {
    let model = to_str(
        record
            .payload_field("rate_limits")
            .and_then(|v| v.get("limit_name")),
    );
    let fallback_model = to_str(record.payload_field("model"));
    let fallback_limit_id = to_str(
        record
            .payload_field("rate_limits")
            .and_then(|v| v.get("limit_id")),
    );

    if !model.is_empty() {
        canonicalize_model("codex", &model)
    } else if !fallback_model.is_empty() {
        canonicalize_model("codex", &fallback_model)
    } else if !fallback_limit_id.is_empty() {
        canonicalize_model("codex", &fallback_limit_id)
    } else {
        canonicalize_model("codex", record.model_hint)
    }
}

fn normalize_codex_compacted(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let event = emitter.event(
        record.base_uid,
        "compacted_raw",
        "compacted",
        "system",
        "",
        record.payload_json(),
    );
    emitter.push_event(event);

    if let Some(Value::Array(items)) = record.payload_field("replacement_history") {
        for (idx, item) in items.iter().enumerate() {
            normalize_codex_compacted_item(record, item, idx, emitter);
        }
    }
}

fn normalize_codex_compacted_item(
    record: &CodexRecord<'_>,
    item: &Value,
    idx: usize,
    emitter: &mut SourceEmitter<'_>,
) {
    let item_uid = emitter.uid_for_json(item, &format!("compacted:{}", idx));
    let item_type = to_str(item.get("type"));

    let (kind, payload_type, actor, text) = match item_type.as_str() {
        "message" => (
            "message".to_string(),
            "message".to_string(),
            to_str(item.get("role")),
            extract_message_text(item.get("content").unwrap_or_else(null_value)),
        ),
        "function_call" => (
            "tool_call".to_string(),
            "function_call".to_string(),
            "assistant".to_string(),
            to_str(item.get("arguments")),
        ),
        "function_call_output" => (
            "tool_result".to_string(),
            "function_call_output".to_string(),
            "tool".to_string(),
            to_str(item.get("output")),
        ),
        "reasoning" => (
            "reasoning".to_string(),
            "reasoning".to_string(),
            "assistant".to_string(),
            extract_message_text(item.get("summary").unwrap_or_else(null_value)),
        ),
        _ => (
            "unknown".to_string(),
            if item_type.is_empty() {
                "unknown".to_string()
            } else {
                item_type.clone()
            },
            "system".to_string(),
            extract_message_text(item),
        ),
    };

    let actor = if actor.is_empty() {
        "assistant"
    } else {
        actor.as_str()
    };
    let item_json = compact_json(item);
    let mut event = emitter
        .event(&item_uid, &kind, &payload_type, actor, &text, &item_json)
        .origin_event_id(record.base_uid);
    if kind == "reasoning" {
        event = event.has_reasoning(true).content_types(["reasoning"]);
    }
    emitter.push_event(event);
    emitter.push_event_link(&item_uid, record.base_uid, "compacted_parent", "{}");
}

fn normalize_codex_legacy_top_level(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let payload_json = compact_json(record.record);
    match record.top_type {
        "message" => emit_codex_message(
            record.record,
            record.base_uid,
            &payload_json,
            false,
            emitter,
        ),
        "function_call" => {
            handle_codex_function_call(record.record, record.base_uid, &payload_json, emitter)
        }
        "function_call_output" => handle_codex_function_call_output(
            record.record,
            record.base_uid,
            &payload_json,
            emitter,
        ),
        "reasoning" => handle_codex_reasoning(
            record.record,
            record.base_uid,
            &payload_json,
            false,
            emitter,
        ),
        _ => {}
    }
}

fn normalize_codex_unknown_top_level(record: &CodexRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let payload_type = if record.top_type.is_empty() {
        "unknown"
    } else {
        record.top_type
    };
    let event = emitter.event(
        record.base_uid,
        "unknown",
        payload_type,
        "system",
        &extract_message_text(record.record),
        &compact_json(record.record),
    );
    emitter.push_event(event);
}

fn stamp_codex_model_fallbacks(record: &CodexRecord<'_>, events: &mut [Value]) {
    let payload_model = canonicalize_model("codex", &to_str(record.payload_field("model")));
    let inherited_model = canonicalize_model("codex", record.model_hint);

    for event in events {
        if let Some(row) = event.as_object_mut() {
            let row_model = canonicalize_model("codex", &to_str(row.get("model")));
            let resolved_model = if !row_model.is_empty() {
                row_model
            } else if !payload_model.is_empty() {
                payload_model.clone()
            } else {
                inherited_model.clone()
            };

            if !resolved_model.is_empty() {
                row.insert("model".to_string(), json!(resolved_model));
            }
        }
    }
}

fn append_codex_parent_link(
    record: &CodexRecord<'_>,
    ctx: &RecordContext<'_>,
    partials: &mut NormalizedPartials,
) {
    let parent = to_str(record.record.get("parent_id"));
    if parent.is_empty() || partials.event_rows.is_empty() {
        return;
    }

    if let Some(uid) = partials.event_rows[0]
        .get("event_uid")
        .and_then(|v| v.as_str())
    {
        partials.push_link(build_external_link_row(
            ctx,
            uid,
            &parent,
            "parent_event",
            "{}",
        ));
    }
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}
