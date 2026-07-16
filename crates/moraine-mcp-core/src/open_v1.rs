use crate::contract::{
    decode_open_cursor, encode_open_cursor, CanonicalOpenV1Args, ContractError, McpEntityKind,
    McpEventId, McpId, McpSessionId, McpTurnId, OpenCursor, OpenCursorAfter, OpenV1Args,
    Performance, ToolEnvelope, ToolError, ToolErrorCode, ToolErrorEnvelope, OPEN_MIN_LIMIT,
    OPEN_TOOL,
};
use crate::{handled_tool_error_result, tool_success_result, AppState};
use anyhow::{Context, Result};
use moraine_conversations::{
    McpEventOpen, McpEventRef, McpEventSummary, McpSessionOpen, McpTurnCompact, McpTurnOpen,
    McpTurnRef, SessionMetadata, TraceEvent,
};
use serde_json::{json, Map, Value};
use std::time::Instant;

const OPEN_EVENT_SLA_TARGET_MS: u64 = 500;
const OPEN_TURN_SLA_TARGET_MS: u64 = 750;
const OPEN_SESSION_SLA_TARGET_MS: u64 = 1_000;
const SUMMARY_PREVIEW_CHARS: usize = 240;
const SUMMARY_MAX_TOOLS: usize = 25;
const SUMMARY_TOOL_NAME_CHARS: usize = 120;
const OPEN_CURSOR_VERSION: u8 = 1;

#[derive(Debug)]
struct ResolvedOpen {
    id: McpId,
    mode: OpenMode,
    request: Value,
}

#[derive(Debug)]
enum OpenMode {
    Summary,
    Page {
        limit: u16,
        after: Option<OpenCursorAfter>,
        expected_snapshot: Option<(u8, u64)>,
    },
}

#[derive(Debug)]
struct PageSelection {
    start: usize,
    end: usize,
    next_cursor: Option<String>,
}

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
                        message: format!(
                            "open expects id, optional limit, or a continuation cursor: {err}"
                        ),
                        details: None,
                    },
                    started_at,
                    OPEN_EVENT_SLA_TARGET_MS,
                );
            }
        };

        let canonical = match args.validate(self.cfg.mcp.max_results) {
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

        let resolved = match resolve_open(canonical, self.cfg.mcp.max_results) {
            Ok(resolved) => resolved,
            Err(err) => {
                return contract_error_tool_response(
                    raw_request,
                    err,
                    started_at,
                    OPEN_EVENT_SLA_TARGET_MS,
                );
            }
        };
        let sla_target_ms = open_sla_target_ms(&resolved.id);
        let request = resolved.request.clone();

        match &resolved.id {
            McpId::Session(id) => match self.repo.get_mcp_session(id.raw_session_id()).await {
                Ok(Some(session)) => {
                    if let Err(error) = validate_snapshot(&resolved.mode, session.snapshot.as_ref())
                    {
                        return contract_error_tool_response(
                            request,
                            error,
                            started_at,
                            sla_target_ms,
                        );
                    }
                    let page = match session_page(&resolved, &session) {
                        Ok(page) => page,
                        Err(error) => {
                            return contract_error_tool_response(
                                request,
                                error,
                                started_at,
                                sla_target_ms,
                            );
                        }
                    };
                    match open_session_data(&session, page.as_ref()) {
                        Ok((data, warnings)) => success_tool_response(
                            request,
                            data,
                            warnings,
                            started_at,
                            sla_target_ms,
                        ),
                        Err(err) => internal_error_tool_response(
                            request,
                            format!("failed to shape session open response: {err:#}"),
                            started_at,
                            sla_target_ms,
                        ),
                    }
                }
                Ok(None) => not_found_tool_response(
                    request,
                    McpEntityKind::Session,
                    &resolved.id.to_string(),
                    started_at,
                    sla_target_ms,
                ),
                Err(err) => repo_error_tool_response(request, err, started_at, sla_target_ms),
            },
            McpId::Turn(id) => {
                let (session_id, turn_seq) = id.decode();
                let turn_result = if matches!(resolved.mode, OpenMode::Page { .. }) {
                    self.repo.get_mcp_turn(session_id, turn_seq).await
                } else {
                    self.repo.get_mcp_turn_summary(session_id, turn_seq).await
                };
                match turn_result {
                    Ok(Some(turn)) => {
                        if let Err(error) =
                            validate_snapshot(&resolved.mode, turn.snapshot.as_ref())
                        {
                            return contract_error_tool_response(
                                request,
                                error,
                                started_at,
                                sla_target_ms,
                            );
                        }
                        let page = match turn_page(&resolved, &turn) {
                            Ok(page) => page,
                            Err(error) => {
                                return contract_error_tool_response(
                                    request,
                                    error,
                                    started_at,
                                    sla_target_ms,
                                );
                            }
                        };
                        match open_turn_data(&turn, page.as_ref()) {
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
                        }
                    }
                    Ok(None) => not_found_tool_response(
                        request,
                        McpEntityKind::Turn,
                        &resolved.id.to_string(),
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
                    &resolved.id.to_string(),
                    started_at,
                    sla_target_ms,
                ),
                Err(err) => repo_error_tool_response(request, err, started_at, sla_target_ms),
            },
        }
    }
}

fn resolve_open(
    args: CanonicalOpenV1Args,
    max_results: u16,
) -> crate::contract::ContractResult<ResolvedOpen> {
    match args {
        CanonicalOpenV1Args::Initial { id, limit } => {
            let request = match limit {
                Some(limit) => json!({ "id": id.to_string(), "limit": limit }),
                None => json!({ "id": id.to_string() }),
            };
            Ok(ResolvedOpen {
                id,
                mode: limit.map_or(OpenMode::Summary, |limit| OpenMode::Page {
                    limit,
                    after: None,
                    expected_snapshot: None,
                }),
                request,
            })
        }
        CanonicalOpenV1Args::Continue { cursor } => {
            let decoded = decode_open_cursor(&cursor)?;
            if decoded.version != OPEN_CURSOR_VERSION {
                return Err(invalid_cursor(
                    "cursor version is not supported; reopen the target",
                ));
            }
            let max_limit = max_results.max(OPEN_MIN_LIMIT);
            if !(OPEN_MIN_LIMIT..=max_limit).contains(&decoded.limit) {
                return Err(invalid_cursor(
                    "cursor page size is invalid; reopen the target",
                ));
            }
            let id: McpId = decoded
                .target_id
                .parse()
                .map_err(|_| invalid_cursor("cursor target is invalid; reopen the target"))?;
            if !matches!(
                (&id, &decoded.after),
                (McpId::Session(_), OpenCursorAfter::Turn { .. })
                    | (McpId::Turn(_), OpenCursorAfter::Event { .. })
            ) {
                return Err(invalid_cursor(
                    "cursor target kind is invalid; reopen the target",
                ));
            }
            Ok(ResolvedOpen {
                id,
                mode: OpenMode::Page {
                    limit: decoded.limit,
                    after: Some(decoded.after),
                    expected_snapshot: Some((decoded.snapshot_slot, decoded.snapshot_generation)),
                },
                request: json!({ "cursor": cursor }),
            })
        }
    }
}

fn invalid_cursor(message: impl Into<String>) -> ContractError {
    ContractError::new(ToolErrorCode::InvalidRequest, message)
        .with_details(json!({ "field": "cursor" }))
}

fn snapshot_tuple(
    snapshot: Option<&moraine_conversations::McpOpenSnapshot>,
) -> crate::contract::ContractResult<(u8, u64)> {
    snapshot
        .map(|snapshot| (snapshot.slot, snapshot.generation))
        .ok_or_else(|| invalid_cursor("pagination snapshot is unavailable; reopen the target"))
}

fn validate_snapshot(
    mode: &OpenMode,
    actual: Option<&moraine_conversations::McpOpenSnapshot>,
) -> crate::contract::ContractResult<()> {
    let OpenMode::Page {
        expected_snapshot: Some(expected),
        ..
    } = mode
    else {
        return Ok(());
    };
    if *expected != snapshot_tuple(actual)? {
        return Err(invalid_cursor(
            "cursor snapshot is stale; reopen the target to restart expansion",
        ));
    }
    Ok(())
}

fn session_page(
    resolved: &ResolvedOpen,
    session: &McpSessionOpen,
) -> crate::contract::ContractResult<Option<PageSelection>> {
    let OpenMode::Page { limit, after, .. } = &resolved.mode else {
        return Ok(None);
    };
    let start = match after.as_ref() {
        None => 0,
        Some(OpenCursorAfter::Turn { turn_seq }) => session
            .turns
            .iter()
            .position(|turn| turn.metadata.turn_seq == *turn_seq)
            .map(|index| index + 1)
            .ok_or_else(|| invalid_cursor("cursor position no longer exists; reopen the target"))?,
        Some(_) => return Err(invalid_cursor("cursor does not match a session page")),
    };
    let end = start
        .saturating_add(usize::from(*limit))
        .min(session.turns.len());
    let next_cursor = if end < session.turns.len() {
        let anchor = session.turns.get(end - 1).ok_or_else(|| {
            invalid_cursor("cursor page is empty before the end; reopen the target")
        })?;
        let (slot, generation) = snapshot_tuple(session.snapshot.as_ref())?;
        Some(encode_open_cursor(&OpenCursor {
            version: OPEN_CURSOR_VERSION,
            target_id: resolved.id.to_string(),
            limit: *limit,
            snapshot_slot: slot,
            snapshot_generation: generation,
            after: OpenCursorAfter::Turn {
                turn_seq: anchor.metadata.turn_seq,
            },
        })?)
    } else {
        None
    };
    Ok(Some(PageSelection {
        start,
        end,
        next_cursor,
    }))
}

fn turn_page(
    resolved: &ResolvedOpen,
    turn: &McpTurnOpen,
) -> crate::contract::ContractResult<Option<PageSelection>> {
    let OpenMode::Page { limit, after, .. } = &resolved.mode else {
        return Ok(None);
    };
    let start = match after.as_ref() {
        None => 0,
        Some(OpenCursorAfter::Event {
            event_order,
            event_uid,
        }) => turn
            .events
            .iter()
            .position(|event| event.event_order == *event_order && event.event_uid == *event_uid)
            .map(|index| index + 1)
            .ok_or_else(|| invalid_cursor("cursor position no longer exists; reopen the target"))?,
        Some(_) => return Err(invalid_cursor("cursor does not match a turn page")),
    };
    let end = start
        .saturating_add(usize::from(*limit))
        .min(turn.events.len());
    let next_cursor = if end < turn.events.len() {
        let anchor = turn.events.get(end - 1).ok_or_else(|| {
            invalid_cursor("cursor page is empty before the end; reopen the target")
        })?;
        let (slot, generation) = snapshot_tuple(turn.snapshot.as_ref())?;
        Some(encode_open_cursor(&OpenCursor {
            version: OPEN_CURSOR_VERSION,
            target_id: resolved.id.to_string(),
            limit: *limit,
            snapshot_slot: slot,
            snapshot_generation: generation,
            after: OpenCursorAfter::Event {
                event_order: anchor.event_order,
                event_uid: anchor.event_uid.clone(),
            },
        })?)
    } else {
        None
    };
    Ok(Some(PageSelection {
        start,
        end,
        next_cursor,
    }))
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
    Ok(tool_success_result(open_result_text(&payload), payload))
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

fn open_session_data(
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

fn open_turn_data(
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
            "source": null
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

fn open_turn_event_summary(
    event: &McpEventSummary,
    ordinal: usize,
    terminal_event_uid: Option<&str>,
) -> Result<Value> {
    let id = encode_event_id(&event.event_uid)?;
    let tool_name = tool_name_for(&event.event_type, &event.name, &event.call_id)
        .map(|name| compact_text_line(&name, SUMMARY_TOOL_NAME_CHARS));
    let summary = event
        .text_preview
        .as_deref()
        .map(|text| compact_text_line(text, SUMMARY_PREVIEW_CHARS))
        .unwrap_or_default();

    Ok(json!({
        "id": id,
        "ordinal": ordinal,
        "type": event.event_type,
        "timestamp": format_unix_ms(event.event_unix_ms),
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

fn compact_tools(tools: &[String]) -> (Vec<String>, bool) {
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

fn format_unix_ms(unix_ms: i64) -> String {
    crate::contract::format_rfc3339_utc_millis(unix_ms)
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
    use moraine_config::AppConfig;
    use moraine_conversations::{
        ConversationMode, InMemoryConversationRepository, InMemoryConversationResponses,
        McpOpenSnapshot, RepoConfig, TurnSummary,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;

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
            snapshot: None,
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
            snapshot: None,
        };

        let page = PageSelection {
            start: 0,
            end: 3,
            next_cursor: None,
        };
        let (data, warnings) = open_turn_data(&turn, Some(&page)).expect("turn data");
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

    #[tokio::test]
    async fn large_turn_is_summary_first_and_fully_cursor_expandable() {
        let turn = large_turn(110, 7);
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_turn: Some(Ok(Some(turn))),
                ..InMemoryConversationResponses::default()
            },
        ));
        let state = AppState::embedded(AppConfig::default(), repository);
        let turn_id = encode_turn_id("session-a", 1).expect("turn id");

        let summary = state
            .open_v1(json!({ "id": turn_id }))
            .await
            .expect("summary open");
        let summary_data = &summary["structuredContent"]["data"];
        assert_eq!(summary_data["events"], json!([]));
        assert_eq!(summary_data["next_cursor"], Value::Null);
        assert_eq!(summary_data["turn"]["event_count"], json!(110));
        assert_eq!(
            summary_data["summary"]["user_input"]["event_id"],
            encode_event_id("event-000").expect("user event id")
        );
        assert_eq!(
            summary_data["summary"]["final_response"]["event_id"],
            encode_event_id("event-109").expect("final event id")
        );
        assert!(serde_json::to_vec(&summary).expect("summary json").len() < 8_000);

        let mut request = json!({ "id": turn_id, "limit": 17 });
        let mut opened_ids = Vec::new();
        let mut ordinals = Vec::new();
        loop {
            let result = state.open_v1(request).await.expect("paged open");
            assert_eq!(result["isError"], json!(false));
            let data = &result["structuredContent"]["data"];
            for event in data["events"].as_array().expect("event page") {
                opened_ids.push(event["id"].as_str().expect("event id").to_string());
                ordinals.push(event["ordinal"].as_u64().expect("event ordinal"));
            }
            let Some(cursor) = data["next_cursor"].as_str() else {
                break;
            };
            request = json!({ "cursor": cursor });
        }

        assert_eq!(opened_ids.len(), 110);
        assert_eq!(
            opened_ids,
            (0..110)
                .map(|index| encode_event_id(&format!("event-{index:03}")).expect("event id"))
                .collect::<Vec<_>>()
        );
        assert_eq!(ordinals, (1..=110).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn session_turns_use_the_same_summary_and_cursor_contract() {
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_session: Some(Ok(Some(session_with_turns(3, 11)))),
                ..InMemoryConversationResponses::default()
            },
        ));
        let state = AppState::embedded(AppConfig::default(), repository);
        let session_id = encode_session_id("session-a").expect("session id");

        let summary = state
            .open_v1(json!({ "id": session_id }))
            .await
            .expect("session summary");
        let data = &summary["structuredContent"]["data"];
        assert_eq!(data["turns"], json!([]));
        assert_eq!(data["next_cursor"], Value::Null);
        assert_eq!(
            data["traversal"]["first_turn_id"],
            encode_turn_id("session-a", 1).expect("first turn")
        );
        assert_eq!(
            data["traversal"]["last_turn_id"],
            encode_turn_id("session-a", 3).expect("last turn")
        );

        let first = state
            .open_v1(json!({ "id": session_id, "limit": 2 }))
            .await
            .expect("first session page");
        let first_data = &first["structuredContent"]["data"];
        assert_eq!(first_data["turns"].as_array().expect("turns").len(), 2);
        let cursor = first_data["next_cursor"].as_str().expect("session cursor");
        let final_page = state
            .open_v1(json!({ "cursor": cursor }))
            .await
            .expect("final session page");
        let final_data = &final_page["structuredContent"]["data"];
        assert_eq!(final_data["turns"].as_array().expect("turns").len(), 1);
        assert_eq!(final_data["turns"][0]["ordinal"], json!(3));
        assert_eq!(final_data["next_cursor"], Value::Null);
    }

    #[tokio::test]
    async fn continuation_rejects_a_changed_projection_snapshot() {
        let first_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_turn: Some(Ok(Some(large_turn(3, 7)))),
                ..InMemoryConversationResponses::default()
            },
        ));
        let first_state = AppState::embedded(AppConfig::default(), first_repository);
        let turn_id = encode_turn_id("session-a", 1).expect("turn id");
        let first_page = first_state
            .open_v1(json!({ "id": turn_id, "limit": 1 }))
            .await
            .expect("first page");
        let cursor = first_page["structuredContent"]["data"]["next_cursor"]
            .as_str()
            .expect("next cursor")
            .to_string();

        let changed_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_turn: Some(Ok(Some(large_turn(3, 8)))),
                ..InMemoryConversationResponses::default()
            },
        ));
        let changed_state = AppState::embedded(AppConfig::default(), changed_repository);
        let result = changed_state
            .open_v1(json!({ "cursor": cursor }))
            .await
            .expect("stale cursor response");

        assert_eq!(result["isError"], json!(true));
        assert_eq!(
            result["structuredContent"]["error"]["code"],
            json!("invalid_request")
        );
        assert!(result["structuredContent"]["error"]["message"]
            .as_str()
            .expect("message")
            .contains("stale"));
    }

    #[tokio::test]
    async fn continuation_rejects_malformed_and_mismatched_cursors_with_recovery_guidance() {
        let state = AppState::embedded(
            AppConfig::default(),
            Arc::new(InMemoryConversationRepository::with_responses(
                RepoConfig::default(),
                InMemoryConversationResponses::default(),
            )),
        );
        let turn_id = encode_turn_id("session-a", 1).expect("turn id");
        let event_id = encode_event_id("event-a").expect("event id");
        let invalid_cursors = [
            "not+url-safe".to_string(),
            encode_open_cursor(&OpenCursor {
                version: OPEN_CURSOR_VERSION + 1,
                target_id: turn_id.clone(),
                limit: 1,
                snapshot_slot: 0,
                snapshot_generation: 1,
                after: OpenCursorAfter::Event {
                    event_order: 1,
                    event_uid: "event-a".to_string(),
                },
            })
            .expect("unsupported-version cursor"),
            encode_open_cursor(&OpenCursor {
                version: OPEN_CURSOR_VERSION,
                target_id: event_id,
                limit: 1,
                snapshot_slot: 0,
                snapshot_generation: 1,
                after: OpenCursorAfter::Event {
                    event_order: 1,
                    event_uid: "event-a".to_string(),
                },
            })
            .expect("wrong-target cursor"),
            encode_open_cursor(&OpenCursor {
                version: OPEN_CURSOR_VERSION,
                target_id: turn_id,
                limit: 1,
                snapshot_slot: 0,
                snapshot_generation: 1,
                after: OpenCursorAfter::Turn { turn_seq: 1 },
            })
            .expect("wrong-anchor cursor"),
        ];

        for cursor in invalid_cursors {
            let result = state
                .open_v1(json!({ "cursor": cursor }))
                .await
                .expect("handled cursor error");
            assert_eq!(result["isError"], true);
            assert_eq!(
                result["structuredContent"]["error"]["code"],
                "invalid_request"
            );
            assert!(result["structuredContent"]["error"]["message"]
                .as_str()
                .expect("cursor error message")
                .contains("reopen"));
        }
    }

    #[tokio::test]
    async fn expansion_does_not_issue_a_cursor_without_a_snapshot() {
        let mut turn = large_turn(3, 7);
        turn.snapshot = None;
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_turn: Some(Ok(Some(turn))),
                ..InMemoryConversationResponses::default()
            },
        ));
        let state = AppState::embedded(AppConfig::default(), repository);
        let turn_id = encode_turn_id("session-a", 1).expect("turn id");

        let result = state
            .open_v1(json!({ "id": turn_id, "limit": 1 }))
            .await
            .expect("handled missing snapshot");
        assert_eq!(result["isError"], true);
        assert!(result["structuredContent"]["error"]["message"]
            .as_str()
            .expect("snapshot error message")
            .contains("snapshot is unavailable"));
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

        assert_eq!(result["isError"], true);
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

    fn large_turn(event_count: usize, generation: u64) -> McpTurnOpen {
        let mut events = (0..event_count)
            .map(|index| {
                let event_type = if index == 0 {
                    "user_input"
                } else if index + 1 == event_count {
                    "assistant_response"
                } else {
                    "tool_response"
                };
                let actor_role = if index == 0 {
                    "user"
                } else if index + 1 == event_count {
                    "assistant"
                } else {
                    "tool"
                };
                let mut event =
                    event_summary(&format!("event-{index:03}"), event_type, actor_role, "", "");
                event.event_order = index as u64 + 1;
                event
            })
            .collect::<Vec<_>>();
        if events.len() > 2 {
            events[1].event_order = events[0].event_order;
        }
        let last_index = event_count.saturating_sub(1);
        McpTurnOpen {
            metadata: TurnSummary {
                total_events: event_count as u64,
                ..turn_summary()
            },
            events,
            user_input_summary: Some("Please inspect this large turn.".to_string()),
            final_response_summary: Some("The large-turn work is complete.".to_string()),
            user_input_event: Some(event_ref("event-000", 1)),
            final_response_event: Some(event_ref(
                &format!("event-{last_index:03}"),
                event_count as u64,
            )),
            tools_called: vec!["exec_command".to_string(), "apply_patch".to_string()],
            normalized_event_types: vec![
                "user_input".to_string(),
                "tool_response".to_string(),
                "assistant_response".to_string(),
            ],
            completed: true,
            terminal_event_uid: Some(format!("event-{last_index:03}")),
            previous_turn: None,
            next_turn: None,
            first_event: Some(event_ref("event-000", 1)),
            last_event: Some(event_ref(
                &format!("event-{last_index:03}"),
                event_count as u64,
            )),
            snapshot: Some(McpOpenSnapshot {
                slot: 1,
                generation,
            }),
        }
    }

    fn session_with_turns(turn_count: u32, generation: u64) -> McpSessionOpen {
        let turns = (1..=turn_count)
            .map(|turn_seq| {
                let mut metadata = turn_summary();
                metadata.turn_seq = turn_seq;
                metadata.turn_id = format!("raw-turn-{turn_seq}");
                McpTurnCompact {
                    metadata,
                    user_input_summary: Some(format!("Question {turn_seq}")),
                    final_response_summary: Some(format!("Answer {turn_seq}")),
                    user_input_event: Some(event_ref(
                        &format!("turn-{turn_seq}-user"),
                        u64::from(turn_seq) * 2 - 1,
                    )),
                    final_response_event: Some(event_ref(
                        &format!("turn-{turn_seq}-final"),
                        u64::from(turn_seq) * 2,
                    )),
                    tools_called: Vec::new(),
                    normalized_event_types: vec![
                        "user_input".to_string(),
                        "assistant_response".to_string(),
                    ],
                    completed: true,
                    terminal_event_uid: Some(format!("turn-{turn_seq}-final")),
                    first_event: Some(event_ref(
                        &format!("turn-{turn_seq}-user"),
                        u64::from(turn_seq) * 2 - 1,
                    )),
                    last_event: Some(event_ref(
                        &format!("turn-{turn_seq}-final"),
                        u64::from(turn_seq) * 2,
                    )),
                }
            })
            .collect();
        McpSessionOpen {
            metadata: SessionMetadata {
                total_turns: turn_count,
                ..session_metadata()
            },
            title: Some("Paged session".to_string()),
            source: Some("codex".to_string()),
            harness: Some("codex".to_string()),
            inference_provider: Some("openai".to_string()),
            session_slug: Some("paged-session".to_string()),
            session_summary: Some("A session with several turns.".to_string()),
            turns,
            completed: true,
            terminal_event_uid: Some(format!("turn-{turn_count}-final")),
            snapshot: Some(McpOpenSnapshot {
                slot: 0,
                generation,
            }),
        }
    }
}
