use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext,
};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

pub(crate) static OPENCODE: Opencode = Opencode;

pub(crate) struct Opencode;

impl IngestSource for Opencode {
    fn harness(&self) -> &'static str {
        "opencode"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        let provider = first_text([
            record.get("providerID"),
            record.get("message_provider_id"),
            record.pointer("/model/providerID"),
            record.pointer("/model/providerId"),
            record.pointer("/model/provider"),
            record.pointer("/data/providerID"),
            record.pointer("/data/model/providerID"),
            record.pointer("/data/model/providerId"),
            record.pointer("/data/model/provider"),
        ]);
        let model = first_text([
            record.get("modelID"),
            record.get("message_model_id"),
            record.pointer("/model/id"),
            record.pointer("/model/modelID"),
            record.pointer("/model/modelId"),
            record.pointer("/data/modelID"),
            record.pointer("/data/model/id"),
            record.pointer("/data/model/modelID"),
            record.pointer("/data/model/modelId"),
        ]);

        SourceMetadata {
            inference_provider: if provider.is_empty() {
                "opencode".to_string()
            } else {
                provider
            },
            model_hint_fallback: model,
        }
    }

    fn record_ts(&self, record: &Value) -> String {
        let raw = record
            .get("timestamp")
            .or_else(|| record.get("time_created"))
            .or_else(|| record.pointer("/time/created"))
            .or_else(|| record.pointer("/data/time/created"));
        epoch_ms_or_string_ts(raw)
    }

    fn jsonl_carries_cwd(&self) -> bool {
        true
    }

    fn cwd(&self, record: &Value) -> String {
        first_text([
            record.get("cwd"),
            record.pointer("/path/cwd"),
            record.pointer("/data/path/cwd"),
            record.get("directory"),
        ])
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let mut session_id = first_text([record.get("session_id"), record.get("sessionID")]);
        if session_id.is_empty() && ctx.top_type == "opencode_session" {
            session_id = to_str(record.get("id"));
        }
        if !session_id.is_empty() {
            return session_id;
        }
        if ctx.session_hint.is_empty() {
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
        let mut emitter = SourceEmitter::new(ctx);
        match top_type {
            "opencode_session" => emit_session(record, &mut emitter),
            "opencode_message" => emit_message(record, &mut emitter),
            "opencode_part" => emit_part(record, &mut emitter),
            "opencode_session_message" => emit_session_message(record, &mut emitter),
            _ => emit_unknown(record, top_type, base_uid, &mut emitter),
        }
        emitter.finish()
    }
}

fn first_text<const N: usize>(values: [Option<&Value>; N]) -> String {
    values
        .into_iter()
        .map(to_str)
        .find(|value| !value.is_empty())
        .unwrap_or_default()
}

fn epoch_ms_or_string_ts(value: Option<&Value>) -> String {
    match value {
        Some(Value::Number(number)) => number
            .as_i64()
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(|dt| format_record_ts(&dt))
            .unwrap_or_default(),
        Some(Value::String(raw)) => raw.clone(),
        _ => String::new(),
    }
}

fn record_data(record: &Value) -> &Value {
    record.get("data").unwrap_or(record)
}

fn opencode_uid(
    record: &Value,
    table: &str,
    fallback_fingerprint: &str,
    suffix: &str,
    emitter: &SourceEmitter<'_>,
) -> String {
    let identity = stable_record_id(record)
        .map(|id| format!("opencode_sqlite:{table}:{id}"))
        .unwrap_or_else(|| fallback_fingerprint.to_string());
    emitter.uid(&identity, suffix)
}

fn stable_record_id(record: &Value) -> Option<String> {
    let id = to_str(record.get("id"));
    (!id.is_empty()).then_some(id)
}

fn emit_session(record: &Value, emitter: &mut SourceEmitter<'_>) {
    let data = record_data(record);
    let title = first_text([record.get("title"), data.get("title")]);
    let model = model_string(record);
    let uid = opencode_uid(
        record,
        "session",
        &compact_json(record),
        "session_meta",
        emitter,
    );
    let mut event = emitter
        .event_for_json(
            &uid,
            "session_meta",
            "session_meta",
            "system",
            &title,
            record,
        )
        .item_id(to_str(record.get("id")))
        .agent_label(first_text([record.get("agent"), data.get("agent")]));
    if !model.is_empty() {
        event = event.model(model);
    }
    event = stamp_session_tokens(record, event);
    emitter.push_event(event);
}

fn emit_message(record: &Value, emitter: &mut SourceEmitter<'_>) {
    let data = record_data(record);
    let role = to_str(data.get("role"));
    let text = message_text(data);
    if text.is_empty() {
        return;
    }
    let actor = actor_for_role(&role);
    let uid = opencode_uid(record, "message", &compact_json(record), "message", emitter);
    let mut event = emitter
        .event_for_json(&uid, "message", "message", actor, &text, record)
        .item_id(to_str(record.get("id")))
        .origin_event_id(to_str(data.get("parentID")))
        .agent_label(to_str(data.get("agent")));
    let model = model_string(record);
    if !model.is_empty() {
        event = event.model(model);
    }
    if let Some(tokens) = data.get("tokens") {
        event = stamp_opencode_tokens(tokens, event);
    }
    emitter.push_event(event);
}

fn emit_part(record: &Value, emitter: &mut SourceEmitter<'_>) {
    let data = record_data(record);
    let part_type = to_str(data.get("type"));
    if part_type == "tool" {
        emit_tool_part(record, data, emitter);
        return;
    }

    let message_role = to_str(record.get("message_role"));
    let (event_kind, payload_type, actor) = part_event_shape(&part_type, &message_role);
    let text = part_text(data);
    let uid = opencode_uid(
        record,
        "part",
        &compact_json(record),
        &format!("part:{part_type}"),
        emitter,
    );
    let mut event = emitter
        .event_for_json(&uid, event_kind, payload_type, actor, &text, record)
        .item_id(to_str(record.get("id")))
        .origin_event_id(to_str(record.get("message_id")))
        .op_kind(part_type.clone());
    if part_type == "reasoning" {
        event = event.has_reasoning(true).content_types(["reasoning"]);
    } else if payload_type != "unknown" {
        event = event.content_types([payload_type]);
    }
    if let Some(tokens) = data.get("tokens") {
        event = stamp_opencode_tokens(tokens, event);
    }
    let model = model_string(record);
    if !model.is_empty() {
        event = event.model(model);
    }
    emitter.push_event(event);
}

fn emit_tool_part(record: &Value, data: &Value, emitter: &mut SourceEmitter<'_>) {
    let identity = format!(
        "opencode_sqlite:part:{}",
        stable_record_id(record).unwrap_or_else(|| compact_json(record))
    );
    let call_id = first_text([data.get("callID"), data.get("id")]);
    let tool_name = first_text([data.get("tool"), data.get("name")]);
    let status = tool_status(data);
    let input = tool_input(data).cloned().unwrap_or(Value::Null);
    let input_json = compact_json(&input);
    let input_text = text_or_json(&input);
    let model = model_string(record);

    let request_uid = emitter.uid(&identity, "tool_use");
    let mut request = emitter
        .event_for_json(
            &request_uid,
            "tool_call",
            "tool_use",
            "assistant",
            &input_text,
            data,
        )
        .item_id(to_str(record.get("id")))
        .origin_event_id(to_str(record.get("message_id")))
        .op_kind("tool")
        .op_status(status.clone())
        .content_types(["tool_use"])
        .tool_call_id(call_id.clone())
        .tool_name(tool_name.clone());
    if !model.is_empty() {
        request = request.model(model.clone());
    }
    emitter.push_event(request);
    emitter.push_tool_request(&request_uid, &call_id, "", &tool_name, &input_json);

    if !tool_part_is_terminal(data, &status) {
        return;
    }

    let output = tool_output(data).cloned().unwrap_or(Value::Null);
    let output_json = compact_json(&output);
    let output_text = text_or_json(&output);
    let tool_error =
        u8::from(tool_error_value(data).is_some() || (!status.is_empty() && status != "completed"));
    let result_uid = emitter.uid(&identity, "tool_result");
    let mut result = emitter
        .event_for_json(
            &result_uid,
            "tool_result",
            "tool_result",
            "tool",
            &output_text,
            &output,
        )
        .item_id(to_str(record.get("id")))
        .origin_event_id(to_str(record.get("message_id")))
        .op_kind("tool")
        .op_status(status)
        .content_types(["tool_result"])
        .tool_call_id(call_id.clone())
        .tool_name(tool_name.clone())
        .tool_error(tool_error);
    if !model.is_empty() {
        result = result.model(model);
    }
    emitter.push_event(result);
    emitter.push_tool_response(
        &result_uid,
        &call_id,
        "",
        &tool_name,
        tool_error,
        &input_json,
        &output_json,
        &output_text,
    );
}

fn emit_session_message(record: &Value, emitter: &mut SourceEmitter<'_>) {
    let data = record_data(record);
    let message_type = first_text([record.get("message_type"), data.get("type")]);
    let (event_kind, payload_type, actor) = session_message_event_shape(&message_type);
    let text = message_text(data);
    let uid = opencode_uid(
        record,
        "session_message",
        &compact_json(record),
        &format!("session_message:{message_type}"),
        emitter,
    );
    let mut event = emitter
        .event_for_json(&uid, event_kind, payload_type, actor, &text, record)
        .item_id(to_str(record.get("id")))
        .op_kind(message_type);
    let model = model_string(record);
    if !model.is_empty() {
        event = event.model(model);
    }
    emitter.push_event(event);
}

fn emit_unknown(record: &Value, top_type: &str, base_uid: &str, emitter: &mut SourceEmitter<'_>) {
    emitter.push_event(emitter.event_for_json(base_uid, "unknown", top_type, "", "", record));
}

fn actor_for_role(role: &str) -> &str {
    match role {
        "user" => "user",
        "assistant" => "assistant",
        "tool" => "tool",
        "system" => "system",
        _ => "",
    }
}

fn message_text(data: &Value) -> String {
    first_text_scalar([data.get("text"), data.get("content")])
}

fn first_text_scalar<const N: usize>(values: [Option<&Value>; N]) -> String {
    values
        .into_iter()
        .find_map(|value| match value {
            Some(Value::String(text)) if !text.is_empty() => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn part_text(data: &Value) -> String {
    first_text([
        data.get("text"),
        data.get("content"),
        data.get("output"),
        data.pointer("/state/output"),
        data.pointer("/state/title"),
        data.get("reason"),
    ])
}

fn text_or_json(value: &Value) -> String {
    to_str(Some(value))
}

fn part_event_shape(
    part_type: &str,
    message_role: &str,
) -> (&'static str, &'static str, &'static str) {
    match part_type {
        "text" => ("message", "text", actor_for_part_role(message_role)),
        "reasoning" => ("reasoning", "reasoning", "assistant"),
        "tool-result" => ("tool_result", "tool_result", "tool"),
        "step-start" | "step-finish" => ("progress", "progress", "system"),
        "file" | "patch" | "snapshot" => {
            ("file_history_snapshot", "file-history-snapshot", "system")
        }
        "compaction" => ("compacted_raw", "compacted", "system"),
        "retry" | "subtask" | "agent" => ("event_msg", "event_msg", "system"),
        _ => ("unknown", "unknown", ""),
    }
}

fn session_message_event_shape(message_type: &str) -> (&'static str, &'static str, &'static str) {
    match message_type {
        "compaction" | "compacted" => ("compacted_raw", "compacted", "system"),
        "summary" => ("summary", "summary", "system"),
        "shell" => ("system", "system", "tool"),
        _ => ("system", "system", "system"),
    }
}

fn actor_for_part_role(role: &str) -> &'static str {
    match role {
        "user" => "user",
        "assistant" => "assistant",
        "tool" => "tool",
        "system" => "system",
        _ => "assistant",
    }
}

fn tool_status(data: &Value) -> String {
    first_text([data.pointer("/state/status"), data.get("status")]).to_ascii_lowercase()
}

fn tool_input(data: &Value) -> Option<&Value> {
    data.get("input").or_else(|| data.pointer("/state/input"))
}

fn tool_output(data: &Value) -> Option<&Value> {
    data.get("output")
        .or_else(|| data.pointer("/state/output"))
        .or_else(|| tool_error_value(data))
}

fn tool_error_value(data: &Value) -> Option<&Value> {
    data.get("error").or_else(|| data.pointer("/state/error"))
}

fn tool_part_is_terminal(data: &Value, status: &str) -> bool {
    tool_output(data).is_some()
        || matches!(
            status,
            "completed" | "error" | "errored" | "cancelled" | "canceled" | "aborted" | "rejected"
        )
}

fn model_string(value: &Value) -> String {
    let raw = first_text([
        value.get("modelID"),
        value.get("message_model_id"),
        value.pointer("/model/id"),
        value.pointer("/model/modelID"),
        value.pointer("/model/modelId"),
        value.pointer("/data/modelID"),
        value.pointer("/data/model/id"),
        value.pointer("/data/model/modelID"),
        value.pointer("/data/model/modelId"),
        value.get("model"),
    ]);
    canonicalize_model("opencode", &raw)
}

fn stamp_session_tokens(record: &Value, event: EventBuilder) -> EventBuilder {
    let tokens = json!({
        "input_tokens": to_u64(record.get("tokens_input")),
        "output_tokens": to_u64(record.get("tokens_output")),
        "output_tokens_details": {
            "reasoning_tokens": to_u64(record.get("tokens_reasoning"))
        },
        "input_tokens_details": {
            "cached_tokens": to_u64(record.get("tokens_cache_read")),
            "cache_creation_tokens": to_u64(record.get("tokens_cache_write"))
        }
    });
    stamp_opencode_tokens(&tokens, event)
}

fn stamp_opencode_tokens(tokens: &Value, event: EventBuilder) -> EventBuilder {
    let input_text = to_u64(tokens.get("input").or_else(|| tokens.get("input_tokens")));
    let output_text = to_u64(tokens.get("output").or_else(|| tokens.get("output_tokens")));
    let cache_read = to_u64(
        tokens
            .pointer("/cache/read")
            .or_else(|| tokens.pointer("/input_tokens_details/cached_tokens")),
    );
    let cache_write = to_u64(
        tokens
            .pointer("/cache/write")
            .or_else(|| tokens.pointer("/input_tokens_details/cache_creation_tokens")),
    );
    let reasoning = to_u64(
        tokens
            .get("reasoning")
            .or_else(|| tokens.pointer("/output_tokens_details/reasoning_tokens")),
    );
    let usage = json!({
        "input_tokens": input_text + cache_read + cache_write,
        "output_tokens": output_text + reasoning,
        "input_tokens_details": {
            "text_tokens": input_text,
            "cached_tokens": cache_read,
            "cache_creation_tokens": cache_write
        },
        "output_tokens_details": {
            "text_tokens": output_text,
            "reasoning_tokens": reasoning
        }
    });
    event.token_accounting(TokenAccounting::openai_generation(Some(&usage)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::normalize_record;

    fn normalize(
        record: Value,
        line_no: u64,
        session_hint: &str,
    ) -> crate::model::NormalizedRecord {
        normalize_record(
            &record,
            "test-opencode",
            "opencode",
            "/fixtures/opencode/opencode.db",
            1,
            1,
            line_no,
            line_no * 100,
            session_hint,
            "",
            "",
        )
        .expect("opencode record should normalize")
    }

    #[test]
    fn normalizes_session_message_and_tool_part_records() {
        let session = normalize(
            json!({
                "type": "opencode_session",
                "id": "ses_demo",
                "directory": "/work/demo",
                "title": "Demo session",
                "agent": "build",
                "model": {"id": "glm-5.2", "providerID": "zai-coding-plan"},
                "tokens_input": 10,
                "tokens_output": 4,
                "tokens_reasoning": 2,
                "tokens_cache_read": 3,
                "time_created": 1780000000000_i64
            }),
            1,
            "",
        );
        assert_eq!(session.session_hint, "ses_demo");
        assert_eq!(session.cwd_hint, "/work/demo");
        assert_eq!(session.event_rows[0]["event_kind"], "session_meta");
        assert_eq!(session.event_rows[0]["text_content"], "Demo session");
        assert_eq!(session.event_rows[0]["input_tokens"], 13);
        assert_eq!(session.event_rows[0]["output_tokens"], 6);
        assert_eq!(session.event_rows[0]["cache_read_tokens"], 3);

        let message = normalize(
            json!({
                "type": "opencode_message",
                "id": "msg_1",
                "session_id": "ses_demo",
                "time_created": 1780000001000_i64,
                "data": {
                    "role": "assistant",
                    "agent": "build",
                    "text": "I can inspect that.",
                    "modelID": "glm-5.2",
                    "providerID": "zai-coding-plan",
                    "tokens": {"input": 20, "output": 5, "reasoning": 1, "cache": {"read": 7, "write": 0}}
                }
            }),
            2,
            "ses_demo",
        );
        assert_eq!(message.event_rows[0]["actor_kind"], "assistant");
        assert_eq!(message.event_rows[0]["text_content"], "I can inspect that.");
        assert_eq!(message.event_rows[0]["input_tokens"], 27);
        assert_eq!(message.event_rows[0]["output_tokens"], 6);
        assert_eq!(message.event_rows[0]["cache_read_tokens"], 7);

        let summary_only_message = normalize(
            json!({
                "type": "opencode_message",
                "id": "msg_summary",
                "session_id": "ses_demo",
                "time_created": 1780000001500_i64,
                "data": {
                    "role": "user",
                    "summary": {"diffs": []}
                }
            }),
            3,
            "ses_demo",
        );
        assert!(
            summary_only_message.event_rows.is_empty(),
            "message envelope summaries are metadata, not transcript text"
        );

        let part = normalize(
            json!({
                "type": "opencode_part",
                "id": "part_1",
                "message_id": "msg_1",
                "session_id": "ses_demo",
                "time_created": 1780000002000_i64,
                "data": {
                    "type": "tool",
                    "callID": "tool_1",
                    "tool": "bash",
                    "input": {"cmd": "pwd"},
                    "output": "/work/demo"
                }
            }),
            4,
            "ses_demo",
        );
        assert_eq!(part.event_rows[0]["payload_type"], "tool_use");
        assert_eq!(part.tool_rows.len(), 2);
        assert_eq!(part.tool_rows[0]["tool_phase"], "request");
        assert_eq!(part.tool_rows[1]["tool_phase"], "response");

        let model_switch = normalize(
            json!({
                "type": "opencode_session_message",
                "id": "sm_model",
                "session_id": "ses_demo",
                "message_type": "model-switched",
                "time_created": 1780000003000_i64,
                "data": {
                    "model": {"id": "glm-5.2", "providerID": "zai-coding-plan"}
                }
            }),
            5,
            "ses_demo",
        );
        assert_eq!(model_switch.event_rows[0]["event_kind"], "system");
        assert_eq!(model_switch.event_rows[0]["payload_type"], "system");
        assert_eq!(model_switch.event_rows[0]["op_kind"], "model-switched");
        assert_eq!(model_switch.event_rows[0]["model"], "glm-5.2");
        assert_eq!(
            model_switch.event_rows[0]["inference_provider"],
            "zai-coding-plan"
        );

        let shell = normalize(
            json!({
                "type": "opencode_session_message",
                "id": "sm_shell",
                "session_id": "ses_demo",
                "message_type": "shell",
                "time_created": 1780000003100_i64,
                "data": {
                    "type": "shell",
                    "text": "shell initialized"
                }
            }),
            6,
            "ses_demo",
        );
        assert_eq!(shell.event_rows[0]["event_kind"], "system");
        assert_eq!(shell.event_rows[0]["actor_kind"], "tool");
        assert_eq!(shell.event_rows[0]["text_content"], "shell initialized");
    }
}
