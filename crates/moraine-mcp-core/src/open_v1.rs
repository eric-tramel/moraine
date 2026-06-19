use crate::contract::{
    ContractError, McpEntityKind, McpEventId, McpId, McpSessionId, McpTurnId, OpenV1Args,
    Performance, ToolEnvelope, ToolError, ToolErrorCode, ToolErrorEnvelope, OPEN_TOOL,
};
use crate::AppState;
use anyhow::{Context, Result};
use moraine_conversations::{
    ConversationRepository, McpEventOpen, McpEventRef, McpEventSummary, McpSessionOpen,
    McpTurnCompact, McpTurnOpen, McpTurnRef, SessionMetadata, TraceEvent,
};
use serde_json::{json, Map, Value};
use std::time::Instant;

const OPEN_EVENT_SLA_TARGET_MS: u64 = 500;
const OPEN_TURN_SLA_TARGET_MS: u64 = 750;
const OPEN_SESSION_SLA_TARGET_MS: u64 = 1_000;
const SUMMARY_PREVIEW_CHARS: usize = 240;

impl AppState {
    pub(crate) async fn open_v1(&self, arguments: Value) -> Result<Value> {
        let started_at = Instant::now();
        let raw_request = request_from_arguments(&arguments);

        let args: OpenV1Args = match serde_json::from_value(arguments) {
            Ok(args) => args,
            Err(err) => {
                return error_tool_response(
                    raw_request,
                    ToolError {
                        code: ToolErrorCode::InvalidRequest,
                        message: format!("open expects {{\"id\": string}}: {err}"),
                        details: Some(json!({ "field": "id" })),
                    },
                    started_at,
                    OPEN_EVENT_SLA_TARGET_MS,
                );
            }
        };

        let canonical = match args.validate() {
            Ok(canonical) => canonical,
            Err(err) => {
                return contract_error_tool_response(
                    raw_request,
                    err,
                    started_at,
                    OPEN_EVENT_SLA_TARGET_MS,
                );
            }
        };

        let sla_target_ms = open_sla_target_ms(&canonical.id);
        let request = request_for_id(&canonical.id);

        match &canonical.id {
            McpId::Session(id) => match self.repo.get_mcp_session(id.raw_session_id()).await {
                Ok(Some(session)) => match open_session_data(&session) {
                    Ok((data, warnings)) => {
                        success_tool_response(request, data, warnings, started_at, sla_target_ms)
                    }
                    Err(err) => internal_error_tool_response(
                        request,
                        format!("failed to shape session open response: {err:#}"),
                        started_at,
                        sla_target_ms,
                    ),
                },
                Ok(None) => not_found_tool_response(
                    request,
                    McpEntityKind::Session,
                    &canonical.id.to_string(),
                    started_at,
                    sla_target_ms,
                ),
                Err(err) => repo_error_tool_response(request, err, started_at, sla_target_ms),
            },
            McpId::Turn(id) => {
                let (session_id, turn_seq) = id.decode();
                match self.repo.get_mcp_turn(session_id, turn_seq).await {
                    Ok(Some(turn)) => match open_turn_data(&turn) {
                        Ok((data, warnings)) => success_tool_response(
                            request,
                            data,
                            warnings,
                            started_at,
                            sla_target_ms,
                        ),
                        Err(err) => internal_error_tool_response(
                            request,
                            format!("failed to shape turn open response: {err:#}"),
                            started_at,
                            sla_target_ms,
                        ),
                    },
                    Ok(None) => not_found_tool_response(
                        request,
                        McpEntityKind::Turn,
                        &canonical.id.to_string(),
                        started_at,
                        sla_target_ms,
                    ),
                    Err(err) => repo_error_tool_response(request, err, started_at, sla_target_ms),
                }
            }
            McpId::Event(id) => match self.repo.get_mcp_event(id.raw_event_uid()).await {
                Ok(Some(event)) => match open_event_data(&event, None) {
                    Ok((data, warnings)) => {
                        success_tool_response(request, data, warnings, started_at, sla_target_ms)
                    }
                    Err(err) => internal_error_tool_response(
                        request,
                        format!("failed to shape event open response: {err:#}"),
                        started_at,
                        sla_target_ms,
                    ),
                },
                Ok(None) => not_found_tool_response(
                    request,
                    McpEntityKind::Event,
                    &canonical.id.to_string(),
                    started_at,
                    sla_target_ms,
                ),
                Err(err) => repo_error_tool_response(request, err, started_at, sla_target_ms),
            },
        }
    }
}

fn open_sla_target_ms(id: &McpId) -> u64 {
    match id.kind() {
        McpEntityKind::Session => OPEN_SESSION_SLA_TARGET_MS,
        McpEntityKind::Turn => OPEN_TURN_SLA_TARGET_MS,
        McpEntityKind::Event => OPEN_EVENT_SLA_TARGET_MS,
    }
}

fn request_from_arguments(arguments: &Value) -> Value {
    match arguments {
        Value::Object(object) => object
            .get("id")
            .map(|id| json!({ "id": id }))
            .unwrap_or_else(|| json!({})),
        Value::Null => json!({}),
        other => other.clone(),
    }
}

fn request_for_id(id: &McpId) -> Value {
    json!({ "id": id.to_string() })
}

fn success_tool_response(
    request: Value,
    data: Value,
    warnings: Vec<String>,
    started_at: Instant,
    sla_target_ms: u64,
) -> Result<Value> {
    let performance = Performance::from_elapsed(started_at.elapsed(), sla_target_ms);
    let envelope =
        ToolEnvelope::success(OPEN_TOOL, request, data, performance).with_warnings(warnings);
    let payload = serde_json::to_value(envelope).context("failed to encode open envelope")?;
    Ok(tool_result(open_result_text(&payload), payload, false))
}

fn contract_error_tool_response(
    request: Value,
    error: ContractError,
    started_at: Instant,
    sla_target_ms: u64,
) -> Result<Value> {
    let details = error
        .details()
        .cloned()
        .or_else(|| Some(json!({ "field": "id" })));
    error_tool_response(
        request,
        ToolError {
            code: error.code(),
            message: error.message().to_string(),
            details,
        },
        started_at,
        sla_target_ms,
    )
}

fn not_found_tool_response(
    request: Value,
    kind: McpEntityKind,
    id: &str,
    started_at: Instant,
    sla_target_ms: u64,
) -> Result<Value> {
    error_tool_response(
        request,
        ToolError {
            code: ToolErrorCode::NotFound,
            message: format!("{kind} not found"),
            details: Some(json!({ "id": id })),
        },
        started_at,
        sla_target_ms,
    )
}

fn repo_error_tool_response(
    request: Value,
    error: moraine_conversations::RepoError,
    started_at: Instant,
    sla_target_ms: u64,
) -> Result<Value> {
    error_tool_response(
        request,
        ToolError {
            code: ToolErrorCode::InternalError,
            message: format!("repository error: {error}"),
            details: None,
        },
        started_at,
        sla_target_ms,
    )
}

fn internal_error_tool_response(
    request: Value,
    message: String,
    started_at: Instant,
    sla_target_ms: u64,
) -> Result<Value> {
    error_tool_response(
        request,
        ToolError {
            code: ToolErrorCode::InternalError,
            message,
            details: None,
        },
        started_at,
        sla_target_ms,
    )
}

fn error_tool_response(
    request: Value,
    error: ToolError,
    started_at: Instant,
    sla_target_ms: u64,
) -> Result<Value> {
    let performance = Performance::from_elapsed(started_at.elapsed(), sla_target_ms);
    let envelope = ToolErrorEnvelope::error(OPEN_TOOL, request, error, performance);
    let payload = serde_json::to_value(envelope).context("failed to encode open error envelope")?;
    Ok(tool_result(open_result_text(&payload), payload, false))
}

fn tool_result(text: String, payload: Value, is_error: bool) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": payload,
        "isError": is_error
    })
}

fn open_result_text(payload: &Value) -> String {
    if let Some(error) = payload.get("error") {
        let code = error.get("code").and_then(Value::as_str).unwrap_or("error");
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("open failed");
        return format!("open failed ({code}): {message}");
    }

    let kind = payload
        .pointer("/data/kind")
        .and_then(Value::as_str)
        .unwrap_or("object");
    let id = payload
        .pointer("/request/id")
        .and_then(Value::as_str)
        .unwrap_or("");
    if id.is_empty() {
        format!("Opened {kind}.")
    } else {
        format!("Opened {kind} {id}.")
    }
}

fn open_session_data(session: &McpSessionOpen) -> Result<(Value, Vec<String>)> {
    let session_id = encode_session_id(&session.metadata.session_id)?;
    let terminal_event_id = encode_optional_event_id(session.terminal_event_uid.as_deref())?;
    let turns = session
        .turns
        .iter()
        .map(open_session_turn_summary)
        .collect::<Result<Vec<_>>>()?;

    let data = json!({
        "kind": "session",
        "session": {
            "id": session_id,
            "title": session.title,
            "source": session.source,
            "started_at": format_unix_ms(session.metadata.first_event_unix_ms),
            "updated_at": format_unix_ms(session.metadata.last_event_unix_ms),
            "completed": session.completed,
            "terminal_event_id": terminal_event_id,
            "turn_count": session.metadata.total_turns,
            "event_count": session.metadata.total_events,
            "mode": session.metadata.mode.as_str(),
            "harness": session.harness,
            "inference_provider": session.inference_provider,
            "session_slug": session.session_slug,
            "session_summary": session.session_summary
        },
        "turns": turns,
        "traversal": {
            "previous_session_id": null,
            "next_session_id": null
        }
    });

    Ok((data, Vec::new()))
}

fn open_session_turn_summary(turn: &McpTurnCompact) -> Result<Value> {
    let turn_id = encode_turn_id(&turn.metadata.session_id, turn.metadata.turn_seq)?;
    let terminal_event_id = encode_optional_event_id(turn.terminal_event_uid.as_deref())?;
    let user_input_event_id = encode_event_ref_id(turn.user_input_event.as_ref())?;
    let final_response_event_id = encode_event_ref_id(turn.final_response_event.as_ref())?;
    let user_input = compact_text_content(
        user_input_event_id.as_deref(),
        turn.user_input_summary.as_deref(),
    );
    let final_response = compact_text_content(
        final_response_event_id.as_deref(),
        turn.final_response_summary.as_deref(),
    );

    Ok(json!({
        "id": turn_id,
        "ordinal": turn.metadata.turn_seq,
        "completed": turn.completed,
        "terminal_event_id": terminal_event_id,
        "event_count": turn.metadata.total_events,
        "started_at": format_unix_ms(turn.metadata.started_at_unix_ms),
        "updated_at": format_unix_ms(turn.metadata.ended_at_unix_ms),
        "user_input": user_input,
        "final_response": final_response,
        "tools_called": turn.tools_called,
        "event_types": turn.normalized_event_types,
        "open": {
            "turn_id": turn_id,
            "terminal_event_id": terminal_event_id
        }
    }))
}

fn open_turn_data(turn: &McpTurnOpen) -> Result<(Value, Vec<String>)> {
    let turn_id = encode_turn_id(&turn.metadata.session_id, turn.metadata.turn_seq)?;
    let session_id = encode_session_id(&turn.metadata.session_id)?;
    let terminal_event_id = encode_optional_event_id(turn.terminal_event_uid.as_deref())?;
    let user_input_event = turn
        .events
        .iter()
        .find(|event| event.event_type == "user_input");
    let final_response_event = turn
        .events
        .iter()
        .rev()
        .find(|event| event.event_type == "assistant_response");
    let user_input = compact_text_content(
        user_input_event
            .map(|event| encode_event_id(&event.event_uid))
            .transpose()?
            .as_deref(),
        turn.user_input_summary
            .as_deref()
            .or_else(|| user_input_event.and_then(|event| event.text_preview.as_deref())),
    );
    let final_response = compact_text_content(
        final_response_event
            .map(|event| encode_event_id(&event.event_uid))
            .transpose()?
            .as_deref(),
        turn.final_response_summary
            .as_deref()
            .or_else(|| final_response_event.and_then(|event| event.text_preview.as_deref())),
    );
    let events = turn
        .events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            open_turn_event_summary(event, index + 1, turn.terminal_event_uid.as_deref())
        })
        .collect::<Result<Vec<_>>>()?;
    let warnings = Vec::new();

    let data = json!({
        "kind": "turn",
        "turn": {
            "id": turn_id,
            "session_id": session_id,
            "ordinal": turn.metadata.turn_seq,
            "completed": turn.completed,
            "terminal_event_id": terminal_event_id,
            "event_count": turn.metadata.total_events,
            "started_at": format_unix_ms(turn.metadata.started_at_unix_ms),
            "updated_at": format_unix_ms(turn.metadata.ended_at_unix_ms)
        },
        "session": {
            "id": session_id,
            "title": null,
            "source": null
        },
        "summary": {
            "user_input": user_input,
            "final_response": final_response,
            "tools_called": turn.tools_called,
            "event_types": turn.normalized_event_types
        },
        "events": events,
        "traversal": {
            "session_id": session_id,
            "previous_turn_id": encode_turn_ref_id(turn.previous_turn.as_ref())?,
            "next_turn_id": encode_turn_ref_id(turn.next_turn.as_ref())?,
            "first_event_id": encode_event_ref_id(turn.first_event.as_ref())?,
            "last_event_id": encode_event_ref_id(turn.last_event.as_ref())?
        }
    });

    Ok((data, warnings))
}

fn open_turn_event_summary(
    event: &McpEventSummary,
    ordinal: usize,
    terminal_event_uid: Option<&str>,
) -> Result<Value> {
    let id = encode_event_id(&event.event_uid)?;
    let tool_name = tool_name_for(&event.event_type, &event.name, &event.call_id);
    let summary = event
        .text_preview
        .as_deref()
        .map(|text| compact_text_line(text, SUMMARY_PREVIEW_CHARS))
        .unwrap_or_default();

    Ok(json!({
        "id": id,
        "ordinal": ordinal,
        "type": event.event_type,
        "timestamp": format_repository_timestamp(&event.event_time),
        "terminal": terminal_event_uid == Some(event.event_uid.as_str()),
        "tool_name": tool_name,
        "model": null,
        "summary": summary,
        "truncated": event.text_preview.as_deref().is_some_and(looks_truncated)
    }))
}

fn open_event_data(
    event: &McpEventOpen,
    turn_state: Option<&McpTurnOpen>,
) -> Result<(Value, Vec<String>)> {
    let trace = &event.event;
    let event_id = encode_event_id(&trace.event_uid)?;
    let session_id = encode_session_id(&trace.session_id)?;
    let turn_id = encode_turn_id(&trace.session_id, trace.turn_seq)?;
    let terminal = turn_state
        .and_then(|turn| turn.terminal_event_uid.as_deref())
        .map(|terminal_event_uid| terminal_event_uid == trace.event_uid)
        .or_else(|| {
            event
                .turn_terminal_event_uid
                .as_deref()
                .map(|terminal_event_uid| terminal_event_uid == trace.event_uid)
        })
        .unwrap_or_else(|| is_terminal_payload(&trace.payload_type));
    let turn_completed = turn_state
        .map(|turn| turn.completed)
        .unwrap_or(event.turn_completed);
    let payload = parse_payload_json(&trace.payload_json);
    let model = payload
        .as_ref()
        .and_then(|payload| extract_string_field(payload, &["model", "model_name"]));
    let originating_model = payload.as_ref().and_then(|payload| {
        extract_string_field(payload, &["originating_model", "model", "model_name"])
    });
    let tool_name = tool_name_for(&event.event_type, &trace.name, &trace.call_id);
    let content = full_event_content(
        trace,
        &event.event_type,
        tool_name.as_deref(),
        payload.as_ref(),
    );

    let data = json!({
        "kind": "event",
        "event": {
            "id": event_id,
            "session_id": session_id,
            "turn_id": turn_id,
            "ordinal": event.event_ordinal,
            "type": event.event_type,
            "timestamp": format_repository_timestamp(&trace.event_time),
            "terminal": terminal,
            "model": model,
            "originating_model": originating_model,
            "tool_name": tool_name
        },
        "content": content,
        "session": session_summary(&event.parent_session, event.parent_session_source.as_deref())?,
        "turn": {
            "id": turn_id,
            "ordinal": event.parent_turn.turn_seq,
            "completed": turn_completed
        },
        "traversal": {
            "session_id": session_id,
            "turn_id": turn_id,
            "previous_event_id": encode_event_ref_id(event.previous_event.as_ref())?,
            "next_event_id": encode_event_ref_id(event.next_event.as_ref())?,
            "previous_turn_id": encode_turn_ref_id(event.previous_turn.as_ref())?,
            "next_turn_id": encode_turn_ref_id(event.next_turn.as_ref())?
        }
    });

    Ok((data, Vec::new()))
}

fn session_summary(metadata: &SessionMetadata, source: Option<&str>) -> Result<Value> {
    Ok(json!({
        "id": encode_session_id(&metadata.session_id)?,
        "title": null,
        "source": source,
        "started_at": format_unix_ms(metadata.first_event_unix_ms),
        "updated_at": format_unix_ms(metadata.last_event_unix_ms),
        "turn_count": metadata.total_turns,
        "event_count": metadata.total_events,
        "mode": metadata.mode.as_str()
    }))
}

fn compact_text_content(event_id: Option<&str>, text: Option<&str>) -> Value {
    match text.map(str::trim).filter(|text| !text.is_empty()) {
        Some(text) => {
            let text = compact_text_line(text, SUMMARY_PREVIEW_CHARS);
            json!({
                "event_id": event_id,
                "text": text,
                "truncated": looks_truncated(&text)
            })
        }
        None => Value::Null,
    }
}

fn full_event_content(
    event: &TraceEvent,
    event_type: &str,
    tool_name: Option<&str>,
    payload: Option<&Value>,
) -> Value {
    let text = full_event_text(event, payload);
    let format = match event_type {
        "tool_call" => "tool_call",
        "tool_response" => "tool_response",
        "reasoning" => "reasoning",
        _ => "text",
    };
    let mut content = Map::new();
    content.insert("format".to_string(), Value::String(format.to_string()));
    content.insert("text".to_string(), Value::String(text));
    content.insert("truncated".to_string(), Value::Bool(false));

    if let Some(tool_name) = tool_name {
        content.insert(
            "tool_name".to_string(),
            Value::String(tool_name.to_string()),
        );
    }
    if format == "tool_call" {
        content.insert(
            "arguments".to_string(),
            payload
                .and_then(extract_tool_arguments)
                .cloned()
                .or_else(|| payload.cloned())
                .unwrap_or(Value::Null),
        );
    }
    if format == "tool_response" {
        if let Some(exit_code) = payload.and_then(extract_exit_code) {
            content.insert("exit_code".to_string(), json!(exit_code));
        }
    }
    if let Some(payload) = payload {
        content.insert("payload".to_string(), payload.clone());
    } else if !event.payload_json.trim().is_empty() {
        content.insert(
            "payload_json".to_string(),
            Value::String(event.payload_json.clone()),
        );
    }
    if !event.token_usage_json.trim().is_empty() {
        content.insert(
            "token_usage_json".to_string(),
            Value::String(event.token_usage_json.clone()),
        );
    }
    if !event.token_usage_buckets.is_empty() {
        content.insert(
            "token_usage_buckets".to_string(),
            json!(event.token_usage_buckets),
        );
    }
    if !event.token_usage_native_units.is_empty() {
        content.insert(
            "token_usage_native_units".to_string(),
            json!(event.token_usage_native_units),
        );
    }

    Value::Object(content)
}

fn full_event_text(event: &TraceEvent, payload: Option<&Value>) -> String {
    let text = event.text_content.trim();
    if !text.is_empty() {
        return event.text_content.clone();
    }

    if let Some(payload) = payload {
        return serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
    }

    event.payload_json.clone()
}

fn parse_payload_json(payload_json: &str) -> Option<Value> {
    let payload_json = payload_json.trim();
    if payload_json.is_empty() {
        return None;
    }
    serde_json::from_str(payload_json).ok()
}

fn extract_tool_arguments(payload: &Value) -> Option<&Value> {
    let object = payload.as_object()?;
    for key in ["arguments", "args", "input", "parameters"] {
        if let Some(value) = object.get(key) {
            return Some(value);
        }
    }
    None
}

fn extract_exit_code(payload: &Value) -> Option<i64> {
    let object = payload.as_object()?;
    for key in ["exit_code", "exitCode", "status_code", "code"] {
        if let Some(value) = object.get(key).and_then(Value::as_i64) {
            return Some(value);
        }
    }
    None
}

fn extract_string_field(payload: &Value, names: &[&str]) -> Option<String> {
    let object = payload.as_object()?;
    for name in names {
        if let Some(value) = object
            .get(*name)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some(value.to_string());
        }
    }
    None
}

fn tool_name_for(event_type: &str, name: &str, call_id: &str) -> Option<String> {
    if !matches!(event_type, "tool_call" | "tool_response") {
        return None;
    }
    non_empty(name).or_else(|| non_empty(call_id))
}

fn is_terminal_payload(payload_type: &str) -> bool {
    matches!(payload_type, "task_complete" | "turn_aborted")
}

fn format_unix_ms(unix_ms: i64) -> String {
    crate::contract::format_rfc3339_utc_millis(unix_ms)
}

fn format_repository_timestamp(timestamp: &str) -> String {
    let trimmed = timestamp.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.contains('T') && trimmed.ends_with('Z') {
        return trimmed.to_string();
    }

    let Some((date, time)) = trimmed.split_once(' ') else {
        return trimmed.to_string();
    };
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    if date.len() != 10 || clock.len() != 8 {
        return trimmed.to_string();
    }
    let millis = if fraction.is_empty() {
        "000".to_string()
    } else {
        let mut millis = fraction.chars().take(3).collect::<String>();
        while millis.len() < 3 {
            millis.push('0');
        }
        millis
    };
    format!("{date}T{clock}.{millis}Z")
}

fn encode_session_id(raw_session_id: &str) -> Result<String> {
    Ok(McpSessionId::from_raw_session_id(raw_session_id)
        .context("invalid repository session id")?
        .to_string())
}

fn encode_turn_id(raw_session_id: &str, turn_seq: u32) -> Result<String> {
    Ok(
        McpTurnId::from_raw_session_id_and_turn_seq(raw_session_id, turn_seq)
            .context("invalid repository turn id")?
            .to_string(),
    )
}

fn encode_event_id(raw_event_uid: &str) -> Result<String> {
    Ok(McpEventId::from_raw_event_uid(raw_event_uid)
        .context("invalid repository event id")?
        .to_string())
}

fn encode_optional_event_id(raw_event_uid: Option<&str>) -> Result<Option<String>> {
    raw_event_uid.map(encode_event_id).transpose()
}

fn encode_turn_ref_id(turn: Option<&McpTurnRef>) -> Result<Option<String>> {
    turn.map(|turn| encode_turn_id(&turn.session_id, turn.turn_seq))
        .transpose()
}

fn encode_event_ref_id(event: Option<&McpEventRef>) -> Result<Option<String>> {
    event
        .map(|event| encode_event_id(&event.event_uid))
        .transpose()
}

fn compact_text_line(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        return compact;
    }

    let mut trimmed: String = compact.chars().take(max_chars.saturating_sub(3)).collect();
    trimmed.push_str("...");
    trimmed
}

fn looks_truncated(text: &str) -> bool {
    text.ends_with("...")
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_conversations::{ConversationMode, TurnSummary};
    use std::collections::BTreeMap;

    #[test]
    fn session_open_uses_typed_ids_and_compact_turns() {
        let session = McpSessionOpen {
            metadata: session_metadata(),
            title: Some("Investigate startup".to_string()),
            source: Some("codex".to_string()),
            harness: Some("cli".to_string()),
            inference_provider: Some("openai".to_string()),
            session_slug: Some("startup".to_string()),
            session_summary: Some("Session summary".to_string()),
            completed: true,
            terminal_event_uid: Some("event-final".to_string()),
            turns: vec![McpTurnCompact {
                metadata: turn_summary(),
                user_input_summary: Some("Please check the failing monitor startup.".to_string()),
                final_response_summary: Some("Fixed the startup guard.".to_string()),
                user_input_event: Some(event_ref("event-user", 1)),
                final_response_event: Some(event_ref("event-final", 3)),
                tools_called: vec!["exec_command".to_string()],
                normalized_event_types: vec![
                    "user_input".to_string(),
                    "tool_call".to_string(),
                    "assistant_response".to_string(),
                ],
                completed: true,
                terminal_event_uid: Some("event-final".to_string()),
                first_event: Some(event_ref("event-user", 1)),
                last_event: Some(event_ref("event-final", 3)),
            }],
        };

        let (data, warnings) = open_session_data(&session).expect("session data");
        assert!(warnings.is_empty());
        assert_eq!(data["kind"], "session");
        assert_eq!(
            data["session"]["id"],
            encode_session_id("session-a").unwrap()
        );
        assert_eq!(
            data["turns"][0]["id"],
            encode_turn_id("session-a", 1).unwrap()
        );
        assert_eq!(
            data["turns"][0]["terminal_event_id"],
            encode_event_id("event-final").unwrap()
        );
        assert!(data["turns"][0].get("payload_json").is_none());
        assert_eq!(
            data["turns"][0]["user_input"]["event_id"],
            encode_event_id("event-user").unwrap()
        );
        assert_eq!(
            data["turns"][0]["final_response"]["event_id"],
            encode_event_id("event-final").unwrap()
        );
    }

    #[test]
    fn turn_open_returns_compact_event_handles_without_payloads() {
        let turn = McpTurnOpen {
            metadata: turn_summary(),
            events: vec![
                event_summary("event-user", "user_input", "user", "", ""),
                event_summary("event-tool", "tool_call", "", "exec_command", "call-1"),
                event_summary("event-final", "assistant_response", "assistant", "", ""),
            ],
            user_input_summary: Some("Please check the failing monitor startup.".to_string()),
            final_response_summary: Some("Fixed the startup guard.".to_string()),
            tools_called: vec!["exec_command".to_string()],
            normalized_event_types: vec![
                "user_input".to_string(),
                "tool_call".to_string(),
                "assistant_response".to_string(),
            ],
            completed: true,
            terminal_event_uid: Some("event-final".to_string()),
            previous_turn: None,
            next_turn: Some(McpTurnRef {
                session_id: "session-a".to_string(),
                turn_seq: 2,
                turn_id: "raw-turn-2".to_string(),
                started_at: "2026-04-29 12:00:00".to_string(),
                ended_at: "2026-04-29 12:01:00".to_string(),
            }),
            first_event: Some(event_ref("event-user", 1)),
            last_event: Some(event_ref("event-final", 3)),
        };

        let (data, warnings) = open_turn_data(&turn).expect("turn data");
        assert_eq!(data["kind"], "turn");
        assert_eq!(data["events"][1]["tool_name"], "exec_command");
        assert_eq!(data["events"][2]["terminal"], true);
        assert!(data["events"][0].get("payload").is_none());
        assert_eq!(
            data["traversal"]["next_turn_id"],
            encode_turn_id("session-a", 2).unwrap()
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn event_open_includes_full_tool_call_content() {
        let event = McpEventOpen {
            event: TraceEvent {
                session_id: "session-a".to_string(),
                event_uid: "event-tool".to_string(),
                event_order: 2,
                turn_seq: 1,
                event_time: "2026-04-29 12:00:01.123".to_string(),
                actor_role: "assistant".to_string(),
                event_class: "tool_call".to_string(),
                payload_type: "tool_call".to_string(),
                call_id: "call-1".to_string(),
                name: "exec_command".to_string(),
                phase: "".to_string(),
                item_id: "".to_string(),
                source_ref: "".to_string(),
                text_content: "".to_string(),
                payload_json:
                    r#"{"arguments":{"cmd":"cargo test","workdir":"/repo"},"model":"gpt-5"}"#
                        .to_string(),
                token_usage_json: "".to_string(),
                endpoint_kind: "".to_string(),
                token_usage_buckets: BTreeMap::new(),
                token_usage_native_units: BTreeMap::new(),
            },
            event_type: "tool_call".to_string(),
            event_ordinal: 2,
            turn_completed: true,
            turn_terminal_event_uid: Some("event-final".to_string()),
            parent_session: session_metadata(),
            parent_session_source: Some("claude".to_string()),
            parent_turn: turn_summary(),
            previous_event: Some(event_ref("event-user", 1)),
            next_event: Some(event_ref("event-final", 3)),
            previous_turn: None,
            next_turn: None,
        };

        let (data, warnings) = open_event_data(&event, None).expect("event data");
        assert_eq!(data["kind"], "event");
        assert_eq!(data["event"]["tool_name"], "exec_command");
        assert_eq!(data["event"]["ordinal"], 2);
        assert_eq!(data["event"]["timestamp"], "2026-04-29T12:00:01.123Z");
        assert_eq!(data["session"]["source"], "claude");
        assert_eq!(data["event"]["model"], "gpt-5");
        assert_eq!(data["content"]["format"], "tool_call");
        assert_eq!(data["content"]["arguments"]["cmd"], "cargo test");
        assert_eq!(data["content"]["truncated"], false);
        assert_eq!(
            data["traversal"]["previous_event_id"],
            encode_event_id("event-user").unwrap()
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn error_response_contains_spec_error_envelope_as_structured_content() {
        let result = error_tool_response(
            json!({ "id": "nope" }),
            ToolError {
                code: ToolErrorCode::InvalidId,
                message: "bad id".to_string(),
                details: Some(json!({ "field": "id" })),
            },
            Instant::now(),
            OPEN_EVENT_SLA_TARGET_MS,
        )
        .expect("error response");

        assert_eq!(result["isError"], false);
        assert_eq!(
            result["structuredContent"]["schema_version"],
            "moraine.mcp.error.v1"
        );
        assert_eq!(result["structuredContent"]["error"]["code"], "invalid_id");
    }

    fn session_metadata() -> SessionMetadata {
        SessionMetadata {
            session_id: "session-a".to_string(),
            first_event_time: "2026-04-29 12:00:00".to_string(),
            first_event_unix_ms: 1_777_463_200_000,
            last_event_time: "2026-04-29 12:05:00".to_string(),
            last_event_unix_ms: 1_777_463_500_000,
            total_turns: 1,
            total_events: 3,
            user_messages: 1,
            assistant_messages: 1,
            tool_calls: 1,
            tool_results: 0,
            mode: ConversationMode::ToolCalling,
            first_event_uid: "event-user".to_string(),
            last_event_uid: "event-final".to_string(),
            last_actor_role: "assistant".to_string(),
        }
    }

    fn turn_summary() -> TurnSummary {
        TurnSummary {
            session_id: "session-a".to_string(),
            turn_seq: 1,
            turn_id: "raw-turn-1".to_string(),
            started_at: "2026-04-29 12:00:00".to_string(),
            started_at_unix_ms: 1_777_463_200_000,
            ended_at: "2026-04-29 12:05:00".to_string(),
            ended_at_unix_ms: 1_777_463_500_000,
            total_events: 3,
            user_messages: 1,
            assistant_messages: 1,
            tool_calls: 1,
            tool_results: 0,
            reasoning_items: 0,
        }
    }

    fn event_ref(event_uid: &str, event_order: u64) -> McpEventRef {
        McpEventRef {
            session_id: "session-a".to_string(),
            event_uid: event_uid.to_string(),
            event_order,
            turn_seq: 1,
            event_time: "2026-04-29 12:00:00".to_string(),
            event_type: "user_input".to_string(),
        }
    }

    fn event_summary(
        event_uid: &str,
        event_type: &str,
        actor_role: &str,
        name: &str,
        call_id: &str,
    ) -> McpEventSummary {
        McpEventSummary {
            session_id: "session-a".to_string(),
            event_uid: event_uid.to_string(),
            event_order: 1,
            turn_seq: 1,
            event_time: "2026-04-29 12:00:00".to_string(),
            actor_role: actor_role.to_string(),
            event_class: event_type.to_string(),
            payload_type: event_type.to_string(),
            event_type: event_type.to_string(),
            call_id: call_id.to_string(),
            name: name.to_string(),
            phase: "".to_string(),
            text_preview: Some(format!("{event_type} preview")),
        }
    }
}
