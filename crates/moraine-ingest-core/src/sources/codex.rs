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
        // A rollout file records exactly one thread and its name carries that
        // thread's id. Forked and sub-agent rollouts replay the PARENT
        // thread's `session_meta` further down the same file, so honoring
        // every header's `payload.id` rebinds the file to its parent from that
        // line on — and only for a scan that reached it, which makes the same
        // line resolve differently on a resume than on a full read. Attribution
        // must be a function of (file, line) alone: the filename wins, and a
        // header may name the thread only while no identity is established and
        // the filename carries none.
        let own_thread_id = infer_session_id_from_file(ctx.source_file);
        if !own_thread_id.is_empty() {
            return own_thread_id;
        }
        if ctx.session_hint.is_empty() && ctx.top_type == "session_meta" {
            let payload_id = to_str(record.pointer("/payload/id"));
            if !payload_id.is_empty() {
                return payload_id;
            }
        }
        ctx.session_hint.to_string()
    }

    fn jsonl_carries_cwd(&self) -> bool {
        true
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
    append_codex_thread_lineage(&codex_record, ctx, &mut partials);
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

/// Codex writes a forked or sub-agent thread to its own rollout file, so the
/// child keeps its own session and the parent relationship travels as an
/// explicit `subagent_parent` external link (the `kimi-cli` precedent) rather
/// than by folding the child's records into the parent's transcript. Only the
/// file's own header describes this file's lineage: a replayed parent header
/// names a different thread and contributes nothing.
fn append_codex_thread_lineage(
    record: &CodexRecord<'_>,
    ctx: &RecordContext<'_>,
    partials: &mut NormalizedPartials,
) {
    if record.top_type != "session_meta" {
        return;
    }

    let own_thread_id = to_str(record.payload_field("id"));
    if own_thread_id.is_empty() || own_thread_id != ctx.session_id {
        return;
    }

    let parent_thread_id = codex_parent_thread_id(record);
    if parent_thread_id.is_empty() || parent_thread_id == own_thread_id {
        return;
    }

    let Some(uid) = partials
        .event_rows
        .first()
        .and_then(|row| row.get("event_uid"))
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };

    let metadata_json = compact_json(&json!({
        "thread_source": to_str(record.payload_field("thread_source")),
        "parent_thread_id": to_str(record.payload_field("parent_thread_id")),
        "forked_from_id": to_str(record.payload_field("forked_from_id")),
    }));
    partials.push_link(build_external_link_row(
        ctx,
        &uid,
        &parent_thread_id,
        "subagent_parent",
        &metadata_json,
    ));
}

/// `parent_thread_id` is the sub-agent spawn edge and `forked_from_id` the
/// fork edge; `payload.session_id` names the originating thread on older
/// rollouts that carry neither, and equals `payload.id` on a plain session.
fn codex_parent_thread_id(record: &CodexRecord<'_>) -> String {
    let parent_thread_id = to_str(record.payload_field("parent_thread_id"));
    if !parent_thread_id.is_empty() {
        return parent_thread_id;
    }

    let forked_from_id = to_str(record.payload_field("forked_from_id"));
    if !forked_from_id.is_empty() {
        return forked_from_id;
    }

    to_str(record.payload_field("session_id"))
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::normalize_record;

    const OWN_THREAD: &str = "019f81a9-8226-7c71-a7de-5a0992207ab6";
    const PARENT_THREAD: &str = "019f7fe1-3b94-7fa2-856c-79946cb89dd2";
    const ROLLOUT: &str =
        "/sessions/2026/07/20/rollout-2026-07-20T18-33-17-019f81a9-8226-7c71-a7de-5a0992207ab6.jsonl";
    /// Older layouts (and hand-copied rollouts) whose name carries no thread id.
    const UNNAMED_ROLLOUT: &str = "/sessions/2026/07/20/rollout.jsonl";

    fn own_header() -> Value {
        json!({
            "type": "session_meta",
            "timestamp": "2026-07-20T18:33:17.019Z",
            "payload": {
                "id": OWN_THREAD,
                "session_id": PARENT_THREAD,
                "forked_from_id": PARENT_THREAD,
                "parent_thread_id": PARENT_THREAD,
                "thread_source": "subagent",
                "cwd": "/repo",
            }
        })
    }

    /// Codex replays the spawning thread's own header into the child rollout.
    fn replayed_parent_header() -> Value {
        json!({
            "type": "session_meta",
            "timestamp": "2026-07-20T18:33:18.019Z",
            "payload": {
                "id": PARENT_THREAD,
                "session_id": PARENT_THREAD,
                "thread_source": "user",
                "cwd": "/repo",
            }
        })
    }

    fn normalize(
        record: &Value,
        source_file: &str,
        line_no: u64,
        session_hint: &str,
    ) -> crate::model::NormalizedRecord {
        normalize_record(
            record,
            "test-codex",
            "codex",
            source_file,
            1,
            1,
            line_no,
            line_no * 100,
            session_hint,
            "",
            "",
        )
        .expect("codex record should normalize")
    }

    fn lineage_links(record: &crate::model::NormalizedRecord) -> Vec<&Value> {
        record
            .link_rows
            .iter()
            .filter(|link| link["link_type"] == "subagent_parent")
            .collect()
    }

    #[test]
    fn replayed_parent_header_never_rebinds_the_rollout() {
        // Whatever identity the pass carried, the answer is the file's own
        // thread — the property the duplicate-uid defect violated.
        for hint in ["", OWN_THREAD, PARENT_THREAD] {
            assert_eq!(
                normalize(&replayed_parent_header(), ROLLOUT, 2, hint).session_hint,
                OWN_THREAD
            );
        }
        assert_eq!(
            normalize(&own_header(), ROLLOUT, 1, "").session_hint,
            OWN_THREAD
        );
        assert_eq!(
            normalize(
                &json!({
                    "type": "event_msg",
                    "timestamp": "2026-07-20T18:33:21.019Z",
                    "payload": {"type": "agent_message", "message": "delegated step"}
                }),
                ROLLOUT,
                3,
                PARENT_THREAD,
            )
            .session_hint,
            OWN_THREAD
        );
    }

    #[test]
    fn an_unnamed_rollout_is_named_by_its_first_header_only() {
        assert_eq!(
            normalize(&own_header(), UNNAMED_ROLLOUT, 1, "").session_hint,
            OWN_THREAD
        );
        // Identity established: a later header naming another thread loses.
        assert_eq!(
            normalize(&replayed_parent_header(), UNNAMED_ROLLOUT, 2, OWN_THREAD).session_hint,
            OWN_THREAD
        );
    }

    #[test]
    fn the_own_header_carries_the_parent_relationship() {
        let normalized = normalize(&own_header(), ROLLOUT, 1, "");
        let links = lineage_links(&normalized);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["linked_external_id"], PARENT_THREAD);
        assert_eq!(links[0]["session_id"], OWN_THREAD);
        assert_eq!(links[0]["event_uid"], normalized.event_rows[0]["event_uid"]);

        let metadata: Value = serde_json::from_str(links[0]["metadata_json"].as_str().unwrap())
            .expect("lineage metadata is JSON");
        assert_eq!(metadata["thread_source"], "subagent");
        assert_eq!(metadata["parent_thread_id"], PARENT_THREAD);
        assert_eq!(metadata["forked_from_id"], PARENT_THREAD);
    }

    #[test]
    fn a_replayed_parent_header_contributes_no_lineage() {
        let normalized = normalize(&replayed_parent_header(), ROLLOUT, 2, OWN_THREAD);
        assert!(lineage_links(&normalized).is_empty());
    }

    #[test]
    fn a_plain_session_has_no_parent() {
        let plain = json!({
            "type": "session_meta",
            "timestamp": "2026-07-20T18:33:17.019Z",
            "payload": {"id": OWN_THREAD, "session_id": OWN_THREAD, "cwd": "/repo"}
        });
        let normalized = normalize(&plain, ROLLOUT, 1, "");
        assert_eq!(normalized.session_hint, OWN_THREAD);
        assert!(lineage_links(&normalized).is_empty());
    }
}
