//! issue-598 WI-07: the v2 canonical `open` tool module.
//!
//! `open_v2` is the tool-facing paging/cursor layer over the shared page-aware
//! canonical repository reader (WI-06, `moraine_conversations`
//! `canonical_open_*` methods). It mints and validates the version-tolerant
//! [`OpenCursorV2`] continuation token, maps a repository staleness
//! [`CanonicalReadOutcome::Reopen`] to the structured stale-reopen response, and
//! assembles tool-visible pages with **identical page boundaries to v1**
//! (session pages break after a turn; turn pages break after an
//! `(event_order, event_uid)` anchor).
//!
//! Response shaping reuses `open_shape`'s formatters — byte-identical to the
//! retired v1 reader's output (design D2). The staleness decision itself is a
//! pure function inside the repository reader (`plan_continuation`); this
//! module only translates its outcomes to the wire.
//!
//! This module is on the `open` dispatch path via [`AppState::open`] (WI-08):
//! `call_tool` routes here whenever the backend's cached `open_v2` readiness
//! admits reads. Since issue #603 WI-10 retired the v1 projection this is the
//! only reader; an unready backend fails typed in the dispatch instead.

use crate::contract::{
    classify_open_cursor, encode_open_cursor_v2, CanonicalOpenV1Args, ContractError, McpEntityKind,
    McpId, OpenCursorClassified, OpenCursorV2, OpenV1Args, ToolError, ToolErrorCode,
    OPEN_CURSOR_MAX_CHARS, OPEN_CURSOR_STALE_REOPEN_MESSAGE, OPEN_CURSOR_V2_VERSION,
    OPEN_MIN_LIMIT,
};
use crate::open_shape::{
    compact_text_content, compact_tools, contract_error_tool_response, encode_event_ref_id,
    encode_optional_event_id, encode_session_id, encode_turn_id, encode_turn_ref_id,
    error_tool_response, format_unix_ms, internal_error_tool_response, not_found_tool_response,
    open_event_data, open_session_data, open_turn_event_summary, repo_error_tool_response,
    request_from_arguments, success_tool_response, PageSelection,
};
use crate::{request_started_at, AppState};
use anyhow::Result;
use moraine_conversations::{
    CanonicalContinuation, CanonicalReadOutcome, CanonicalSessionPage, CanonicalTurnPage,
    McpTurnOpen,
};
use serde_json::{json, Value};
use std::time::Instant;

#[derive(Debug)]
struct ResolvedOpenV2 {
    id: McpId,
    mode: OpenV2Mode,
    request: Value,
}

/// The v2 request mode. `Summary` mirrors v1's summary-first open (no `limit`);
/// `Page` carries the page size and, for a continuation, the decoded repository
/// [`CanonicalContinuation`] handed straight back to the reader.
#[derive(Debug)]
enum OpenV2Mode {
    Summary,
    Page {
        limit: u16,
        // Boxed so the carried session header (design §5.5) inside the
        // continuation does not bloat this transient mode enum.
        after: Option<Box<CanonicalContinuation>>,
    },
}

impl AppState {
    pub(crate) async fn open_v2(&self, arguments: Value) -> Result<Value> {
        let started_at = request_started_at();
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
                );
            }
        };

        let canonical = match args.validate(self.cfg.mcp.max_results) {
            Ok(canonical) => canonical,
            Err(err) => return contract_error_tool_response(raw_request, err, started_at),
        };

        let resolved = match resolve_open_v2(canonical, self.cfg.mcp.max_results) {
            Ok(resolved) => resolved,
            Err(err) => return contract_error_tool_response(raw_request, err, started_at),
        };
        let request = resolved.request.clone();

        match &resolved.id {
            McpId::Session(id) => {
                self.open_v2_session(id.raw_session_id(), &resolved, request, started_at)
                    .await
            }
            McpId::Turn(id) => {
                let (session_id, turn_seq) = id.decode();
                self.open_v2_turn(session_id, turn_seq, &resolved, request, started_at)
                    .await
            }
            McpId::Event(id) => {
                self.open_v2_event(id.raw_event_uid(), &resolved, request, started_at)
                    .await
            }
        }
    }

    async fn open_v2_session(
        &self,
        session_id: &str,
        resolved: &ResolvedOpenV2,
        request: Value,
        started_at: Instant,
    ) -> Result<Value> {
        // Summary mode has no live page, but the shared reader has no
        // header-only entry point, so fetch a single-turn page and render its
        // header with an empty turn list (matching v1's summary contract).
        let (limit, after) = repo_page_args(&resolved.mode);
        let outcome = self
            .repo
            .canonical_open_session_page(session_id, limit, after)
            .await;
        let page = match outcome {
            Ok(Some(CanonicalReadOutcome::Page(page))) => page,
            Ok(Some(CanonicalReadOutcome::Reopen)) => {
                return reopen_tool_response(request, started_at)
            }
            Ok(None) => {
                return not_found_tool_response(
                    request,
                    McpEntityKind::Session,
                    &resolved.id.to_string(),
                    started_at,
                )
            }
            Err(err) => return repo_error_tool_response(request, err, started_at),
        };

        match shape_session_page(&resolved.mode, &resolved.id, &page) {
            Ok((data, warnings)) => success_tool_response(request, data, warnings, started_at),
            Err(err) => internal_error_tool_response(
                request,
                format!("failed to shape session open response: {err:#}"),
                started_at,
            ),
        }
    }

    async fn open_v2_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
        resolved: &ResolvedOpenV2,
        request: Value,
        started_at: Instant,
    ) -> Result<Value> {
        let include_events = matches!(resolved.mode, OpenV2Mode::Page { .. });
        let (limit, after) = repo_page_args(&resolved.mode);
        let outcome = self
            .repo
            .canonical_open_turn_page(session_id, turn_seq, limit, include_events, after)
            .await;
        let page = match outcome {
            Ok(Some(CanonicalReadOutcome::Page(page))) => page,
            Ok(Some(CanonicalReadOutcome::Reopen)) => {
                return reopen_tool_response(request, started_at)
            }
            Ok(None) => {
                return not_found_tool_response(
                    request,
                    McpEntityKind::Turn,
                    &resolved.id.to_string(),
                    started_at,
                )
            }
            Err(err) => return repo_error_tool_response(request, err, started_at),
        };

        match shape_turn_page(&resolved.mode, &resolved.id, &page) {
            Ok((data, warnings)) => success_tool_response(request, data, warnings, started_at),
            Err(err) => internal_error_tool_response(
                request,
                format!("failed to shape turn open response: {err:#}"),
                started_at,
            ),
        }
    }

    async fn open_v2_event(
        &self,
        event_uid: &str,
        resolved: &ResolvedOpenV2,
        request: Value,
        started_at: Instant,
    ) -> Result<Value> {
        match self.repo.canonical_open_event(event_uid).await {
            Ok(Some(event)) => match open_event_data(&event, None) {
                Ok((data, warnings)) => success_tool_response(request, data, warnings, started_at),
                Err(err) => internal_error_tool_response(
                    request,
                    format!("failed to shape event open response: {err:#}"),
                    started_at,
                ),
            },
            Ok(None) => not_found_tool_response(
                request,
                McpEntityKind::Event,
                &resolved.id.to_string(),
                started_at,
            ),
            Err(err) => repo_error_tool_response(request, err, started_at),
        }
    }
}

/// The `(limit, after)` a repository page call needs for a resolved mode.
/// Summary mode requests a single turn (the reader has no header-only path) and
/// its returned turns/continuation are discarded by the summary shaper.
fn repo_page_args(mode: &OpenV2Mode) -> (u16, Option<CanonicalContinuation>) {
    match mode {
        OpenV2Mode::Summary => (1, None),
        OpenV2Mode::Page { limit, after } => (*limit, after.as_ref().map(|cont| (**cont).clone())),
    }
}

fn resolve_open_v2(
    args: CanonicalOpenV1Args,
    max_results: u16,
) -> crate::contract::ContractResult<ResolvedOpenV2> {
    match args {
        CanonicalOpenV1Args::Initial { id, limit } => {
            let request = match limit {
                Some(limit) => json!({ "id": id.to_string(), "limit": limit }),
                None => json!({ "id": id.to_string() }),
            };
            Ok(ResolvedOpenV2 {
                mode: limit.map_or(OpenV2Mode::Summary, |limit| OpenV2Mode::Page {
                    limit,
                    after: None,
                }),
                id,
                request,
            })
        }
        CanonicalOpenV1Args::Continue { cursor } => {
            let decoded = match classify_open_cursor(&cursor)? {
                // A legacy v1 token (pre/post flip) reopens deterministically
                // with the promised stale-cursor wording, never "malformed".
                OpenCursorClassified::V1Legacy => return Err(stale_reopen_cursor()),
                OpenCursorClassified::V2(cursor) => *cursor,
            };
            debug_assert_eq!(decoded.version, OPEN_CURSOR_V2_VERSION);
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
            // Only session and turn traversals paginate; an event cursor cannot
            // exist, so reject it rather than seek a nonexistent page.
            if matches!(id, McpId::Event(_)) {
                return Err(invalid_cursor(
                    "cursor target kind is invalid; reopen the target",
                ));
            }
            Ok(ResolvedOpenV2 {
                id,
                mode: OpenV2Mode::Page {
                    limit: decoded.limit,
                    after: Some(Box::new(decoded.continuation)),
                },
                request: json!({ "cursor": cursor }),
            })
        }
    }
}

/// Shape one `open(session)` page into the v1-identical tool JSON. Page mode
/// renders the returned page's turns and mints the next continuation cursor from
/// the page continuation; summary mode renders header-only with no turns and no
/// cursor.
fn shape_session_page(
    mode: &OpenV2Mode,
    target_id: &McpId,
    page: &CanonicalSessionPage,
) -> Result<(Value, Vec<String>)> {
    match mode {
        OpenV2Mode::Summary => open_session_data(&page.session, None),
        OpenV2Mode::Page { limit, .. } => {
            let next_cursor = mint_cursor(target_id, *limit, page.continuation.as_ref())?;
            let selection = PageSelection {
                start: 0,
                end: page.session.turns.len(),
                next_cursor,
            };
            open_session_data(&page.session, Some(&selection))
        }
    }
}

/// Shape one `open(turn)` page into the v1-identical tool JSON. Event ordinals
/// continue turn-globally across pages via the anchor's within-turn ordinal.
fn shape_turn_page(
    mode: &OpenV2Mode,
    target_id: &McpId,
    page: &CanonicalTurnPage,
) -> Result<(Value, Vec<String>)> {
    match mode {
        OpenV2Mode::Summary => open_v2_turn_data(&page.turn, 0, None),
        OpenV2Mode::Page { limit, after } => {
            let ordinal_offset = after
                .as_ref()
                .map(|cont| cont.after.event_ordinal as usize)
                .unwrap_or(0);
            let next_cursor = mint_cursor(target_id, *limit, page.continuation.as_ref())?;
            open_v2_turn_data(&page.turn, ordinal_offset, next_cursor)
        }
    }
}

/// Mint the next-page continuation token from a repository continuation, or
/// `None` when the traversal is complete.
fn mint_cursor(
    target_id: &McpId,
    limit: u16,
    continuation: Option<&CanonicalContinuation>,
) -> Result<Option<String>> {
    continuation
        .map(|continuation| {
            let mut cursor = OpenCursorV2 {
                version: OPEN_CURSOR_V2_VERSION,
                target_id: target_id.to_string(),
                limit,
                continuation: continuation.clone(),
            };
            let token = encode_open_cursor_v2(&cursor)
                .map_err(|err| anyhow::anyhow!("failed to encode continuation cursor: {err}"))?;
            // The carried session header (design §5.5) can push the encoded
            // cursor past OPEN_CURSOR_MAX_CHARS (enforced on the decode path).
            // Drop it and re-encode: the next page recomputes the header (one
            // session-wide pass) rather than failing the traversal (design §6
            // carry-drop).
            if token.len() > OPEN_CURSOR_MAX_CHARS && cursor.continuation.session_carry.is_some() {
                cursor.continuation.session_carry = None;
                encode_open_cursor_v2(&cursor)
                    .map_err(|err| anyhow::anyhow!("failed to encode continuation cursor: {err}"))
            } else {
                Ok(token)
            }
        })
        .transpose()
}

/// The v2 turn-open shaper: identical JSON to `open_shape::open_turn_data` except
/// the events are already the page slice, ordinals continue from
/// `ordinal_offset` (the anchor's within-turn ordinal), and `next_cursor` is the
/// v2 continuation token.
fn open_v2_turn_data(
    turn: &McpTurnOpen,
    ordinal_offset: usize,
    next_cursor: Option<String>,
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
    let events = turn
        .events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            open_turn_event_summary(
                event,
                ordinal_offset + index + 1,
                turn.terminal_event_uid.as_deref(),
            )
        })
        .collect::<Result<Vec<_>>>()?;

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
        "next_cursor": next_cursor,
        "traversal": {
            "session_id": session_id,
            "previous_turn_id": encode_turn_ref_id(turn.previous_turn.as_ref())?,
            "next_turn_id": encode_turn_ref_id(turn.next_turn.as_ref())?,
            "first_event_id": encode_event_ref_id(turn.first_event.as_ref())?,
            "last_event_id": encode_event_ref_id(turn.last_event.as_ref())?
        }
    });

    Ok((data, Vec::new()))
}

fn reopen_tool_response(request: Value, started_at: Instant) -> Result<Value> {
    contract_error_tool_response(request, stale_reopen_cursor(), started_at)
}

fn invalid_cursor(message: impl Into<String>) -> ContractError {
    ContractError::new(ToolErrorCode::InvalidRequest, message)
        .with_details(json!({ "field": "cursor" }))
}

fn stale_reopen_cursor() -> ContractError {
    invalid_cursor(OPEN_CURSOR_STALE_REOPEN_MESSAGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{McpSessionId, McpTurnId};
    use crate::open_shape::{open_session_data, open_turn_data};
    use moraine_config::AppConfig;
    use moraine_conversations::{
        CanonicalReadAnchor, CanonicalSessionSignals, ConversationMode,
        InMemoryConversationRepository, InMemoryConversationResponses, McpEventRef,
        McpEventSummary, McpSessionOpen, McpTurnCompact, McpTurnRef, RepoConfig, SessionMetadata,
        TurnSummary,
    };
    use std::sync::Arc;

    // --- fixtures ----------------------------------------------------------

    fn session_metadata(total_turns: u32) -> SessionMetadata {
        SessionMetadata {
            session_id: "session-a".to_string(),
            first_event_time: "2026-04-29 12:00:00".to_string(),
            first_event_unix_ms: 1_777_463_200_000,
            last_event_time: "2026-04-29 12:05:00".to_string(),
            last_event_unix_ms: 1_777_463_500_000,
            total_turns,
            total_events: u64::from(total_turns) * 2,
            user_messages: u64::from(total_turns),
            assistant_messages: u64::from(total_turns),
            tool_calls: 0,
            tool_results: 0,
            mode: ConversationMode::Chat,
            first_event_uid: "turn-1-user".to_string(),
            last_event_uid: format!("turn-{total_turns}-final"),
            last_actor_role: "assistant".to_string(),
        }
    }

    fn turn_metadata(turn_seq: u32) -> TurnSummary {
        TurnSummary {
            session_id: "session-a".to_string(),
            turn_seq,
            turn_id: format!("raw-turn-{turn_seq}"),
            started_at: "2026-04-29 12:00:00".to_string(),
            started_at_unix_ms: 1_777_463_200_000,
            ended_at: "2026-04-29 12:05:00".to_string(),
            ended_at_unix_ms: 1_777_463_500_000,
            total_events: 2,
            user_messages: 1,
            assistant_messages: 1,
            tool_calls: 0,
            tool_results: 0,
            reasoning_items: 0,
        }
    }

    fn event_ref(event_uid: &str, event_order: u64, turn_seq: u32) -> McpEventRef {
        McpEventRef {
            session_id: "session-a".to_string(),
            event_uid: event_uid.to_string(),
            event_order,
            turn_seq,
            event_time: "2026-04-29 12:00:00".to_string(),
            event_type: "user_input".to_string(),
        }
    }

    fn compact_turn(turn_seq: u32) -> McpTurnCompact {
        McpTurnCompact {
            metadata: turn_metadata(turn_seq),
            user_input_summary: Some(format!("Question {turn_seq}")),
            final_response_summary: Some(format!("Answer {turn_seq}")),
            user_input_event: Some(event_ref(
                &format!("turn-{turn_seq}-user"),
                u64::from(turn_seq) * 2 - 1,
                turn_seq,
            )),
            final_response_event: Some(event_ref(
                &format!("turn-{turn_seq}-final"),
                u64::from(turn_seq) * 2,
                turn_seq,
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
                turn_seq,
            )),
            last_event: Some(event_ref(
                &format!("turn-{turn_seq}-final"),
                u64::from(turn_seq) * 2,
                turn_seq,
            )),
        }
    }

    fn full_session(turn_count: u32) -> McpSessionOpen {
        McpSessionOpen {
            metadata: session_metadata(turn_count),
            title: Some("Paged session".to_string()),
            source: Some("codex".to_string()),
            harness: Some("codex".to_string()),
            inference_provider: Some("openai".to_string()),
            session_slug: Some("paged-session".to_string()),
            session_summary: Some("A session with several turns.".to_string()),
            turns: (1..=turn_count).map(compact_turn).collect(),
            completed: true,
            terminal_event_uid: Some(format!("turn-{turn_count}-final")),
        }
    }

    fn event_summary(index: usize, event_type: &str, actor_role: &str) -> McpEventSummary {
        McpEventSummary {
            session_id: "session-a".to_string(),
            event_uid: format!("event-{index:03}"),
            event_order: index as u64 + 1,
            turn_seq: 1,
            event_time: "2026-04-29 12:00:00".to_string(),
            event_unix_ms: 1_777_464_000_000,
            actor_role: actor_role.to_string(),
            event_class: event_type.to_string(),
            payload_type: event_type.to_string(),
            event_type: event_type.to_string(),
            call_id: String::new(),
            name: String::new(),
            phase: String::new(),
            text_preview: Some(format!("{event_type} preview")),
        }
    }

    fn full_turn(event_count: usize) -> McpTurnOpen {
        let events = (0..event_count)
            .map(|index| {
                let (event_type, actor) = if index == 0 {
                    ("user_input", "user")
                } else if index + 1 == event_count {
                    ("assistant_response", "assistant")
                } else {
                    ("tool_response", "tool")
                };
                event_summary(index, event_type, actor)
            })
            .collect::<Vec<_>>();
        let last = event_count.saturating_sub(1);
        McpTurnOpen {
            metadata: TurnSummary {
                total_events: event_count as u64,
                ..turn_metadata(1)
            },
            events,
            parent_session_source: Some("codex".to_string()),
            user_input_summary: Some("Inspect this turn.".to_string()),
            final_response_summary: Some("Work complete.".to_string()),
            user_input_event: Some(event_ref("event-000", 1, 1)),
            final_response_event: Some(event_ref(
                &format!("event-{last:03}"),
                event_count as u64,
                1,
            )),
            tools_called: vec!["exec_command".to_string()],
            normalized_event_types: vec![
                "user_input".to_string(),
                "tool_response".to_string(),
                "assistant_response".to_string(),
            ],
            completed: true,
            terminal_event_uid: Some(format!("event-{last:03}")),
            previous_turn: None,
            next_turn: Some(McpTurnRef {
                session_id: "session-a".to_string(),
                turn_seq: 2,
                turn_id: "raw-turn-2".to_string(),
                started_at: "2026-04-29 12:06:00".to_string(),
                ended_at: "2026-04-29 12:07:00".to_string(),
            }),
            first_event: Some(event_ref("event-000", 1, 1)),
            last_event: Some(event_ref(
                &format!("event-{last:03}"),
                event_count as u64,
                1,
            )),
        }
    }

    fn sample_continuation(after_turn_seq: u32, event_ordinal: u32) -> CanonicalContinuation {
        CanonicalContinuation {
            signals: CanonicalSessionSignals {
                pinned_revision: 5,
                heads_fingerprint: "f".repeat(64),
                observed_sum: 42,
                min_bound_ms: 1_777_463_200_000,
                max_bound_ms: 1_777_463_500_000,
            },
            after: CanonicalReadAnchor {
                sort_time_ms: 1_777_463_400_000,
                source_host: "host-1".to_string(),
                source_file: "codex/session.jsonl".to_string(),
                source_generation: 1,
                source_offset: 1024,
                source_line_no: 40,
                event_uid: "anchor-event".to_string(),
                event_order: 4,
                turn_seq: after_turn_seq,
                prefix_user_message_count: 2,
                event_ordinal,
            },
            after_turn_seq,
            session_carry: None,
        }
    }

    fn session_id() -> McpId {
        McpId::Session(McpSessionId::from_raw_session_id("session-a").unwrap())
    }

    fn turn_id() -> McpId {
        McpId::Turn(McpTurnId::from_raw_session_id_and_turn_seq("session-a", 1).unwrap())
    }

    // --- resolve -----------------------------------------------------------

    #[test]
    fn resolve_maps_initial_summary_and_page_modes() {
        let summary = resolve_open_v2(
            CanonicalOpenV1Args::Initial {
                id: turn_id(),
                limit: None,
            },
            50,
        )
        .expect("summary resolve");
        assert!(matches!(summary.mode, OpenV2Mode::Summary));

        let page = resolve_open_v2(
            CanonicalOpenV1Args::Initial {
                id: turn_id(),
                limit: Some(7),
            },
            50,
        )
        .expect("page resolve");
        assert!(matches!(
            page.mode,
            OpenV2Mode::Page {
                limit: 7,
                after: None
            }
        ));
    }

    #[test]
    fn resolve_continues_v2_cursor() {
        let target = McpTurnId::from_raw_session_id_and_turn_seq("session-a", 1)
            .unwrap()
            .to_string();
        let continuation = sample_continuation(1, 3);
        let token = encode_open_cursor_v2(&OpenCursorV2 {
            version: OPEN_CURSOR_V2_VERSION,
            target_id: target,
            limit: 9,
            continuation: continuation.clone(),
        })
        .unwrap();

        let resolved =
            resolve_open_v2(CanonicalOpenV1Args::Continue { cursor: token }, 50).expect("resolve");
        match resolved.mode {
            OpenV2Mode::Page {
                limit,
                after: Some(after),
            } => {
                assert_eq!(limit, 9);
                assert_eq!(*after, continuation);
            }
            other => panic!("expected page continuation, got {other:?}"),
        }
        assert!(matches!(resolved.id, McpId::Turn(_)));
    }

    #[test]
    fn resolve_reopens_legacy_v1_token() {
        // A pre/post-flip v1 cursor must resolve to the deterministic
        // stale-reopen guidance, not a "cursor version" or "malformed" error.
        let v1_token = crate::contract::encode_open_cursor(&crate::contract::OpenCursor {
            version: 1,
            target_id: McpTurnId::from_raw_session_id_and_turn_seq("session-a", 1)
                .unwrap()
                .to_string(),
            limit: 5,
            snapshot_slot: 0,
            snapshot_generation: 1,
            after: crate::contract::OpenCursorAfter::Event {
                event_order: 1,
                event_uid: "event-1".to_string(),
            },
        })
        .unwrap();
        let err = resolve_open_v2(CanonicalOpenV1Args::Continue { cursor: v1_token }, 50)
            .expect_err("legacy token reopens");
        assert_eq!(err.code(), ToolErrorCode::InvalidRequest);
        assert!(err.message().contains("stale"));
        assert!(err.message().contains("reopen"));
    }

    #[test]
    fn resolve_rejects_event_target_cursor() {
        let event_id = crate::contract::McpEventId::from_raw_event_uid("event-1")
            .unwrap()
            .to_string();
        let token = encode_open_cursor_v2(&OpenCursorV2 {
            version: OPEN_CURSOR_V2_VERSION,
            target_id: event_id,
            limit: 5,
            continuation: sample_continuation(0, 1),
        })
        .unwrap();
        let err = resolve_open_v2(CanonicalOpenV1Args::Continue { cursor: token }, 50)
            .expect_err("event cursor rejected");
        assert_eq!(err.code(), ToolErrorCode::InvalidRequest);
        assert!(err.message().contains("target kind"));
    }

    // --- page-boundary equivalence vs v1 ----------------------------------

    #[test]
    fn session_page_matches_v1_turn_boundaries_and_mints_decodable_cursor() {
        // The v2 session page must present exactly the same page of turns v1
        // would for the same boundary, and its next_cursor must round-trip back
        // to the repository continuation.
        let full = full_session(3);
        let (v1_data, _) = open_session_data(
            &full,
            Some(&PageSelection {
                start: 0,
                end: 2,
                next_cursor: Some("legacy".to_string()),
            }),
        )
        .expect("v1 session page");

        let mut page_session = full.clone();
        page_session.turns = full.turns[0..2].to_vec();
        let continuation = sample_continuation(2, 0);
        let page = CanonicalSessionPage {
            session: page_session,
            continuation: Some(continuation.clone()),
        };
        let (v2_data, _) = shape_session_page(
            &OpenV2Mode::Page {
                limit: 2,
                after: None,
            },
            &session_id(),
            &page,
        )
        .expect("v2 session page");

        assert_eq!(v2_data["kind"], "session");
        assert_eq!(v1_data["kind"], v2_data["kind"]);
        // The page boundary — which turns render — is byte-identical to v1.
        assert_eq!(v1_data["turns"], v2_data["turns"]);
        assert_eq!(v1_data["session"], v2_data["session"]);

        let cursor_token = v2_data["next_cursor"].as_str().expect("v2 next_cursor");
        match classify_open_cursor(cursor_token).expect("classify minted cursor") {
            OpenCursorClassified::V2(decoded) => {
                assert_eq!(decoded.limit, 2);
                assert_eq!(decoded.continuation, continuation);
                assert_eq!(decoded.target_id, session_id().to_string());
            }
            other => panic!("expected v2 cursor, got {other:?}"),
        }
    }

    #[test]
    fn session_summary_has_no_turns_and_no_cursor() {
        let mut page_session = full_session(3);
        // Summary fetches a single turn from the reader; the shaper drops it.
        page_session.turns = full_session(3).turns[0..1].to_vec();
        let page = CanonicalSessionPage {
            session: page_session,
            continuation: Some(sample_continuation(1, 0)),
        };
        let (data, _) =
            shape_session_page(&OpenV2Mode::Summary, &session_id(), &page).expect("summary page");
        assert_eq!(data["kind"], "session");
        assert_eq!(data["turns"], json!([]));
        assert_eq!(data["next_cursor"], Value::Null);
        assert_eq!(data["session"]["turn_count"], 3);
    }

    #[test]
    fn oversized_session_carry_is_dropped_to_fit_the_cursor() {
        // The carried session header (design §5.5) can push the encoded cursor
        // past OPEN_CURSOR_MAX_CHARS; mint drops it rather than failing the
        // traversal, and the next page recomputes the header (design §6).
        let mut continuation = sample_continuation(3, 0);
        continuation.session_carry = Some("x".repeat(OPEN_CURSOR_MAX_CHARS * 2));
        let token = mint_cursor(&session_id(), 25, Some(&continuation))
            .expect("mint drops the oversized carry")
            .expect("a continuation mints a token");
        assert!(token.len() <= OPEN_CURSOR_MAX_CHARS);
        match classify_open_cursor(&token).expect("classify minted cursor") {
            OpenCursorClassified::V2(decoded) => {
                assert!(decoded.continuation.session_carry.is_none());
                // Everything else survives the drop.
                assert_eq!(decoded.continuation.after, continuation.after);
                assert_eq!(decoded.limit, 25);
            }
            other => panic!("expected v2 cursor, got {other:?}"),
        }
    }

    #[test]
    fn turn_page_event_ordinals_continue_across_pages_like_v1() {
        // v1 numbers a turn's events globally (start + index + 1). v2 slices the
        // page but continues ordinals from the anchor's within-turn ordinal, so
        // the second page must match v1's second-page ordinals exactly.
        let full = full_turn(5);
        let (v1_data, _) = open_turn_data(
            &full,
            Some(&PageSelection {
                start: 3,
                end: 5,
                next_cursor: None,
            }),
        )
        .expect("v1 turn page 2");

        let mut page_turn = full.clone();
        page_turn.events = full.events[3..5].to_vec();
        let (v2_data, _) = shape_turn_page(
            &OpenV2Mode::Page {
                limit: 2,
                after: Some(Box::new(sample_continuation(1, 3))),
            },
            &turn_id(),
            &CanonicalTurnPage {
                turn: page_turn,
                continuation: None,
            },
        )
        .expect("v2 turn page 2");

        assert_eq!(v1_data["events"], v2_data["events"]);
        assert_eq!(v2_data["events"][0]["ordinal"], 4);
        assert_eq!(v2_data["events"][1]["ordinal"], 5);
        assert_eq!(v1_data["traversal"], v2_data["traversal"]);
    }

    #[test]
    fn turn_summary_has_no_events_and_no_cursor() {
        let mut page_turn = full_turn(4);
        page_turn.events = Vec::new();
        let (data, _) = shape_turn_page(
            &OpenV2Mode::Summary,
            &turn_id(),
            &CanonicalTurnPage {
                turn: page_turn,
                continuation: None,
            },
        )
        .expect("turn summary");
        assert_eq!(data["kind"], "turn");
        assert_eq!(data["events"], json!([]));
        assert_eq!(data["next_cursor"], Value::Null);
        assert_eq!(data["turn"]["event_count"], 4);
    }

    // --- outcome mapping ---------------------------------------------------

    #[test]
    fn reopen_maps_to_structured_stale_reopen() {
        let response =
            reopen_tool_response(json!({ "cursor": "abc" }), Instant::now()).expect("reopen");
        assert_eq!(response["isError"], true);
        let error = &response["structuredContent"]["error"];
        assert_eq!(error["code"], "invalid_request");
        assert!(error["message"].as_str().unwrap().contains("stale"));
        assert!(error["message"].as_str().unwrap().contains("reopen"));
        assert_eq!(error["details"]["field"], "cursor");
    }

    #[tokio::test]
    async fn unavailable_reader_maps_to_internal_error() {
        // Against a repository without the v2 reader, the typed backend error
        // surfaces as internal_error — never a silent empty page.
        let repo = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses::default(),
        ));
        let state = AppState::embedded(AppConfig::default(), repo);
        let response = state
            .open_v2(json!({ "id": "session:c2Vzc2lvbi1h", "limit": 5 }))
            .await
            .expect("handled response");
        assert_eq!(response["isError"], true);
        assert_eq!(
            response["structuredContent"]["error"]["code"],
            "internal_error"
        );
    }

    #[tokio::test]
    async fn invalid_cursor_is_rejected_before_the_repository() {
        let repo = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses::default(),
        ));
        let state = AppState::embedded(AppConfig::default(), repo);
        let response = state
            .open_v2(json!({ "cursor": "not+url-safe" }))
            .await
            .expect("handled response");
        assert_eq!(response["isError"], true);
        assert_eq!(
            response["structuredContent"]["error"]["code"],
            "invalid_request"
        );
        assert!(response["structuredContent"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reopen"));
    }
}
