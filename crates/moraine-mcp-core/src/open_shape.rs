//! Response shaping for the `open` tool family.
//!
//! Born as the issue-598 v1 `open` module; the v2 canonical reader
//! (`open_v2.rs`) reused its byte-identical response-shaping and id/text
//! formatters so both readers emitted the same tool JSON (design D2). Issue
//! #603 WI-10 retired the v1 engine — its paging, cursor, and
//! generation-pinned snapshot logic went with the projection they read — and
//! what remains here is the shared shaping contract the canonical reader
//! serves.

use crate::contract::{
    ContractError, McpEntityKind, McpEventId, McpSessionId, McpTurnId, Performance, ToolEnvelope,
    ToolError, ToolErrorCode, ToolErrorEnvelope, OPEN_TOOL,
};
use crate::{handled_tool_error_result, tool_success_result};
use anyhow::{Context, Result};
use moraine_conversations::{
    McpEventOpen, McpEventRef, McpEventSummary, McpSessionOpen, McpTurnCompact, McpTurnOpen,
    McpTurnRef, SessionMetadata, TraceEvent,
};
use serde_json::{json, Map, Value};
use std::time::Instant;

const SUMMARY_PREVIEW_CHARS: usize = 240;
const ENCRYPTED_REASONING_SUMMARY: &str = "[encrypted reasoning omitted]";
const SUMMARY_MAX_TOOLS: usize = 25;
const SUMMARY_TOOL_NAME_CHARS: usize = 120;

/// The half-open slice of a paged open response plus its continuation token,
/// as the v2 reader selects it.
#[derive(Debug)]
pub(crate) struct PageSelection {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) next_cursor: Option<String>,
}

pub(crate) fn request_from_arguments(arguments: &Value) -> Value {
    match arguments {
        Value::Object(object) => {
            let mut request = Map::new();
            for field in ["id", "limit", "cursor"] {
                if let Some(value) = object.get(field) {
                    let value = if field == "cursor"
                        && value.as_str().is_some_and(|cursor| cursor.len() > 4096)
                    {
                        Value::String("<oversized>".to_string())
                    } else {
                        value.clone()
                    };
                    request.insert(field.to_string(), value);
                }
            }
            Value::Object(request)
        }
        Value::Null => json!({}),
        other => other.clone(),
    }
}

pub(crate) fn success_tool_response(
    request: Value,
    data: Value,
    warnings: Vec<String>,
    started_at: Instant,
) -> Result<Value> {
    let performance = Performance::from_elapsed(started_at.elapsed());
    let envelope =
        ToolEnvelope::success(OPEN_TOOL, request, data, performance).with_warnings(warnings);
    let payload = serde_json::to_value(envelope).context("failed to encode open envelope")?;
    Ok(tool_success_result(open_result_text(&payload), payload))
}

pub(crate) fn contract_error_tool_response(
    request: Value,
    error: ContractError,
    started_at: Instant,
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
    )
}

pub(crate) fn not_found_tool_response(
    request: Value,
    kind: McpEntityKind,
    id: &str,
    started_at: Instant,
) -> Result<Value> {
    error_tool_response(
        request,
        ToolError {
            code: ToolErrorCode::NotFound,
            message: format!("{kind} not found"),
            details: Some(json!({ "id": id })),
        },
        started_at,
    )
}

pub(crate) fn repo_error_tool_response(
    request: Value,
    error: moraine_conversations::RepoError,
    started_at: Instant,
) -> Result<Value> {
    match error {
        // Structured retryable/budget verdicts keep their dedicated wire
        // codes and details through the shared mapper (issue #600 W7 maps
        // deadline/resource errors at BOTH mapping sites).
        error @ (moraine_conversations::RepoError::ReadModelChanged
        | moraine_conversations::RepoError::DeadlineExceeded { .. }
        | moraine_conversations::RepoError::ResourceExhausted { .. }) => {
            let error = crate::repo_error_to_contract_error(error);
            error_tool_response(
                request,
                ToolError {
                    code: error.code(),
                    message: error.message().to_string(),
                    details: error.details().cloned(),
                },
                started_at,
            )
        }
        error => error_tool_response(
            request,
            ToolError {
                code: ToolErrorCode::InternalError,
                message: format!("repository error: {error}"),
                details: None,
            },
            started_at,
        ),
    }
}

pub(crate) fn internal_error_tool_response(
    request: Value,
    message: String,
    started_at: Instant,
) -> Result<Value> {
    error_tool_response(
        request,
        ToolError {
            code: ToolErrorCode::InternalError,
            message,
            details: None,
        },
        started_at,
    )
}

pub(crate) fn error_tool_response(
    request: Value,
    error: ToolError,
    started_at: Instant,
) -> Result<Value> {
    let performance = Performance::from_elapsed(started_at.elapsed());
    let envelope = ToolErrorEnvelope::error(OPEN_TOOL, request, error, performance);
    let payload = serde_json::to_value(envelope).context("failed to encode open error envelope")?;
    Ok(handled_tool_error_result(
        open_result_text(&payload),
        payload,
    ))
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
        .or_else(|| payload.pointer("/data/turn/id").and_then(Value::as_str))
        .or_else(|| payload.pointer("/data/event/id").and_then(Value::as_str))
        .or_else(|| payload.pointer("/data/session/id").and_then(Value::as_str))
        .unwrap_or("");
    if kind == "event" {
        return format!("Opened event {id}.");
    }

    let (children, total) = if kind == "session" {
        (
            payload
                .pointer("/data/turns")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0),
            payload
                .pointer("/data/session/turn_count")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        )
    } else {
        (
            payload
                .pointer("/data/events")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0),
            payload
                .pointer("/data/turn/event_count")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        )
    };
    let unit = if kind == "session" { "turns" } else { "events" };
    let expanded =
        payload.pointer("/request/limit").is_some() || payload.pointer("/request/cursor").is_some();
    if !expanded {
        format!(
            "Opened {kind} {id} summary only. {total} {unit} available; call open with id and limit to expand."
        )
    } else if payload
        .pointer("/data/next_cursor")
        .and_then(Value::as_str)
        .is_some()
    {
        format!("Opened {kind} {id} with {children} {unit}. More are available with next_cursor.")
    } else {
        format!("Opened {kind} {id} with the final {children} {unit}.")
    }
}

pub(crate) fn open_session_data(
    session: &McpSessionOpen,
    page: Option<&PageSelection>,
) -> Result<(Value, Vec<String>)> {
    let session_id = encode_session_id(&session.metadata.session_id)?;
    let terminal_event_id = encode_optional_event_id(session.terminal_event_uid.as_deref())?;
    let (start, end) = page.map(|page| (page.start, page.end)).unwrap_or((0, 0));
    let turns = session.turns[start..end]
        .iter()
        .map(open_session_turn_summary)
        .collect::<Result<Vec<_>>>()?;
    let first_turn_id = session
        .turns
        .first()
        .map(|turn| encode_turn_id(&turn.metadata.session_id, turn.metadata.turn_seq))
        .transpose()?;
    let last_turn_id = session
        .turns
        .last()
        .map(|turn| encode_turn_id(&turn.metadata.session_id, turn.metadata.turn_seq))
        .transpose()?;

    let data = json!({
        "kind": "session",
        "session": {
            "id": session_id,
            "title": compact_optional_line(session.title.as_deref(), SUMMARY_PREVIEW_CHARS),
            "source": compact_optional_line(session.source.as_deref(), SUMMARY_TOOL_NAME_CHARS),
            "started_at": format_unix_ms(session.metadata.first_event_unix_ms),
            "updated_at": format_unix_ms(session.metadata.last_event_unix_ms),
            "completed": session.completed,
            "terminal_event_id": terminal_event_id,
            "turn_count": session.metadata.total_turns,
            "event_count": session.metadata.total_events,
            "mode": session.metadata.mode.as_str(),
            "harness": compact_optional_line(session.harness.as_deref(), SUMMARY_TOOL_NAME_CHARS),
            "inference_provider": compact_optional_line(session.inference_provider.as_deref(), SUMMARY_TOOL_NAME_CHARS),
            "session_slug": compact_optional_line(session.session_slug.as_deref(), SUMMARY_PREVIEW_CHARS),
            "session_summary": compact_optional_line(session.session_summary.as_deref(), SUMMARY_PREVIEW_CHARS)
        },
        "turns": turns,
        "next_cursor": page.and_then(|page| page.next_cursor.as_deref()),
        "traversal": {
            "previous_session_id": null,
            "next_session_id": null,
            "first_turn_id": first_turn_id,
            "last_turn_id": last_turn_id
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
    let (tools_called, tools_called_truncated) = compact_tools(&turn.tools_called);

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
        "tools_called": tools_called,
        "tools_called_truncated": tools_called_truncated,
        "event_types": turn.normalized_event_types,
        "open": {
            "turn_id": turn_id,
            "terminal_event_id": terminal_event_id
        }
    }))
}

/// The v1 turn-open shaper, kept under `cfg(test)` as the byte-parity oracle
/// for `open_v2_turn_data` (`open_v2::tests` diffs the two shapers field by
/// field). No production path calls it since issue #603 WI-10 retired the v1
/// reader; the v2 shaper owns the live turn-page response.
#[cfg(test)]
pub(crate) fn open_turn_data(
    turn: &McpTurnOpen,
    page: Option<&PageSelection>,
) -> Result<(Value, Vec<String>)> {
    let turn_id = encode_turn_id(&turn.metadata.session_id, turn.metadata.turn_seq)?;
    let session_id = encode_session_id(&turn.metadata.session_id)?;
    let terminal_event_id = encode_optional_event_id(turn.terminal_event_uid.as_deref())?;
    let user_input = compact_text_content(
        encode_event_ref_id(turn.user_input_event.as_ref())?.as_deref(),
        turn.user_input_summary.as_deref(),
    );
    let final_response = compact_text_content(
        encode_event_ref_id(turn.final_response_event.as_ref())?.as_deref(),
        turn.final_response_summary.as_deref(),
    );
    let (tools_called, tools_called_truncated) = compact_tools(&turn.tools_called);
    let (start, end) = page.map(|page| (page.start, page.end)).unwrap_or((0, 0));
    let events = turn.events[start..end]
        .iter()
        .enumerate()
        .map(|(index, event)| {
            open_turn_event_summary(event, start + index + 1, turn.terminal_event_uid.as_deref())
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
            "source": turn.parent_session_source
        },
        "summary": {
            "user_input": user_input,
            "final_response": final_response,
            "tools_called": tools_called,
            "tools_called_truncated": tools_called_truncated,
            "event_types": turn.normalized_event_types
        },
        "events": events,
        "next_cursor": page.and_then(|page| page.next_cursor.as_deref()),
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

pub(crate) fn open_turn_event_summary(
    event: &McpEventSummary,
    ordinal: usize,
    terminal_event_uid: Option<&str>,
) -> Result<Value> {
    let id = encode_event_id(&event.event_uid)?;
    let tool_name = tool_name_for(&event.event_type, &event.name, &event.call_id)
        .map(|name| compact_text_line(&name, SUMMARY_TOOL_NAME_CHARS));
    let (summary, summary_truncated) = compact_event_summary(event);

    Ok(json!({
        "id": id,
        "ordinal": ordinal,
        "type": event.event_type,
        "timestamp": format_unix_ms(event.event_unix_ms),
        "terminal": terminal_event_uid == Some(event.event_uid.as_str()),
        "tool_name": tool_name,
        "model": null,
        "summary": summary,
        "truncated": summary_truncated
    }))
}

fn compact_event_summary(event: &McpEventSummary) -> (String, bool) {
    let Some(text) = event.text_preview.as_deref() else {
        return (String::new(), false);
    };
    if event.event_type == "reasoning" && has_encrypted_content_field(text) {
        return (ENCRYPTED_REASONING_SUMMARY.to_string(), false);
    }

    (
        compact_text_line(text, SUMMARY_PREVIEW_CHARS),
        looks_truncated(text),
    )
}

fn has_encrypted_content_field(text: &str) -> bool {
    let text = text.trim_start();
    if !text.starts_with('{') {
        return false;
    }

    const FIELD: &str = "\"encrypted_content\"";
    text.match_indices(FIELD)
        .any(|(index, _)| text[index + FIELD.len()..].trim_start().starts_with(':'))
}

pub(crate) fn open_event_data(
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
            "timestamp": format_unix_ms(trace.event_unix_ms),
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

pub(crate) fn compact_text_content(event_id: Option<&str>, text: Option<&str>) -> Value {
    match text.map(str::trim).filter(|text| !text.is_empty()) {
        Some(text) => {
            let text = compact_text_line(text, SUMMARY_PREVIEW_CHARS);
            json!({
                "event_id": event_id,
                "text": text,
                "truncated": looks_truncated(&text)
            })
        }
        None if event_id.is_some() => json!({
            "event_id": event_id,
            "text": null,
            "truncated": false
        }),
        None => Value::Null,
    }
}

fn compact_optional_line(text: Option<&str>, max_chars: usize) -> Option<String> {
    text.map(str::trim)
        .filter(|text| !text.is_empty())
        .map(|text| compact_text_line(text, max_chars))
}

pub(crate) fn compact_tools(tools: &[String]) -> (Vec<String>, bool) {
    let compact = tools
        .iter()
        .take(SUMMARY_MAX_TOOLS)
        .map(|tool| compact_text_line(tool, SUMMARY_TOOL_NAME_CHARS))
        .collect::<Vec<_>>();
    (compact, tools.len() > SUMMARY_MAX_TOOLS)
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

pub(crate) fn format_unix_ms(unix_ms: i64) -> String {
    crate::contract::format_rfc3339_utc_millis(unix_ms)
}

pub(crate) fn encode_session_id(raw_session_id: &str) -> Result<String> {
    Ok(McpSessionId::from_raw_session_id(raw_session_id)
        .context("invalid repository session id")?
        .to_string())
}

pub(crate) fn encode_turn_id(raw_session_id: &str, turn_seq: u32) -> Result<String> {
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

pub(crate) fn encode_optional_event_id(raw_event_uid: Option<&str>) -> Result<Option<String>> {
    raw_event_uid.map(encode_event_id).transpose()
}

pub(crate) fn encode_turn_ref_id(turn: Option<&McpTurnRef>) -> Result<Option<String>> {
    turn.map(|turn| encode_turn_id(&turn.session_id, turn.turn_seq))
        .transpose()
}

pub(crate) fn encode_event_ref_id(event: Option<&McpEventRef>) -> Result<Option<String>> {
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
    fn compact_text_keeps_known_event_handle_without_preview_text() {
        let event_id = encode_event_id("event-empty").expect("event id");
        let compact = compact_text_content(Some(&event_id), None);

        assert_eq!(compact["event_id"], event_id);
        assert_eq!(compact["text"], Value::Null);
        assert_eq!(compact["truncated"], false);
    }

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

        let page = PageSelection {
            start: 0,
            end: 1,
            next_cursor: None,
        };
        let (data, warnings) = open_session_data(&session, Some(&page)).expect("session data");
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
            parent_session_source: Some("codex".to_string()),
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

        let page = PageSelection {
            start: 0,
            end: 3,
            next_cursor: None,
        };
        let (data, warnings) = open_turn_data(&turn, Some(&page)).expect("turn data");
        assert_eq!(data["kind"], "turn");
        assert_eq!(data["session"]["source"], "codex");
        assert_eq!(data["events"][1]["tool_name"], "exec_command");
        assert_eq!(data["events"][2]["terminal"], true);
        assert!(data["events"][0].get("payload").is_none());
        assert_eq!(
            data["traversal"]["next_turn_id"],
            encode_turn_id("session-a", 2).unwrap()
        );
        assert!(warnings.is_empty());
    }

    /// Successor to the retired v1-flow test
    /// `turn_expansion_omits_encrypted_reasoning_payloads`: the shaping
    /// contract (not the reader) is what redacts encrypted reasoning, so the
    /// guard drives the shared shaper both readers used and the canonical
    /// reader still does.
    #[test]
    fn turn_shaping_omits_encrypted_reasoning_payloads() {
        let encrypted_content = format!("encrypted-{}", "x".repeat(1_000));
        let mut reasoning = event_summary("event-think", "reasoning", "assistant", "", "");
        reasoning.event_class = "reasoning".to_string();
        reasoning.payload_type = "reasoning".to_string();
        reasoning.text_preview = Some(format!(
            r#"{{"type":"reasoning","summary":[],"encrypted_content":"{encrypted_content}"}}"#
        ));
        let turn = McpTurnOpen {
            metadata: turn_summary(),
            events: vec![
                event_summary("event-user", "user_input", "user", "", ""),
                reasoning,
                event_summary("event-final", "assistant_response", "assistant", "", ""),
            ],
            parent_session_source: Some("codex".to_string()),
            user_input_summary: Some("Please think quietly.".to_string()),
            final_response_summary: Some("Done thinking.".to_string()),
            user_input_event: Some(event_ref("event-user", 1)),
            final_response_event: Some(event_ref("event-final", 3)),
            tools_called: Vec::new(),
            normalized_event_types: vec![
                "user_input".to_string(),
                "reasoning".to_string(),
                "assistant_response".to_string(),
            ],
            completed: true,
            terminal_event_uid: Some("event-final".to_string()),
            previous_turn: None,
            next_turn: None,
            first_event: Some(event_ref("event-user", 1)),
            last_event: Some(event_ref("event-final", 3)),
        };

        let page = PageSelection {
            start: 0,
            end: 3,
            next_cursor: None,
        };
        let (data, _warnings) = open_turn_data(&turn, Some(&page)).expect("turn data");
        assert_eq!(
            data["events"][1]["summary"],
            json!(ENCRYPTED_REASONING_SUMMARY)
        );
        assert_eq!(data["events"][1]["truncated"], json!(false));
        let response = serde_json::to_string(&data).expect("serialized data");
        assert!(!response.contains("encrypted_content"));
        assert!(!response.contains(&encrypted_content));
    }

    #[test]
    fn event_open_includes_full_tool_call_content() {
        let event = McpEventOpen {
            event: TraceEvent {
                session_id: "session-a".to_string(),
                event_uid: "event-tool".to_string(),
                event_order: 2,
                turn_seq: 1,
                event_time: "2026-04-29 08:00:01.123".to_string(),
                event_unix_ms: 1_777_464_001_123,
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
        )
        .expect("error response");

        assert_eq!(result["isError"], true);
        assert_eq!(
            result["structuredContent"]["schema_version"],
            "moraine.mcp.error.v1"
        );
        assert_eq!(result["structuredContent"]["error"]["code"], "invalid_id");
    }

    #[test]
    fn repository_error_response_only_special_cases_publication_changes() {
        for (repo_error, expected_message) in [
            (
                moraine_conversations::RepoError::InvalidArgument("bad argument".to_string()),
                "repository error: invalid argument: bad argument",
            ),
            (
                moraine_conversations::RepoError::InvalidCursor("bad cursor".to_string()),
                "repository error: invalid cursor: bad cursor",
            ),
            (
                moraine_conversations::RepoError::Backend("backend failed".to_string()),
                "repository error: backend error: backend failed",
            ),
            (
                moraine_conversations::RepoError::Internal("internal failed".to_string()),
                "repository error: internal error: internal failed",
            ),
        ] {
            let result = repo_error_tool_response(json!({}), repo_error, Instant::now())
                .expect("handled repository error");
            let error = &result["structuredContent"]["error"];

            assert_eq!(error["code"], "internal_error");
            assert_eq!(error["message"], expected_message);
            assert!(error.get("details").is_none());
        }

        let result = repo_error_tool_response(
            json!({}),
            moraine_conversations::RepoError::ReadModelChanged,
            Instant::now(),
        )
        .expect("handled publication change");
        let error = &result["structuredContent"]["error"];
        assert_eq!(error["code"], "internal_error");
        assert_eq!(error["details"]["reason"], "read_model_refresh");
        assert_eq!(error["details"]["retryable"], true);
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
            event_unix_ms: 1_777_464_000_000,
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
