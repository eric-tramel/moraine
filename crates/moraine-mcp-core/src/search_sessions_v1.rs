use super::{
    handled_tool_error_result, internal_id_error, repo_error_to_contract_error,
    tool_success_result, AppState,
};
use crate::contract::{
    format_rfc3339_utc_millis, CanonicalSearchSessionsArgs, ContractError, McpEventId, McpId,
    McpSessionId, McpTurnId, Performance, PerformanceBuilder, SearchSessionsArgs, ToolEnvelope,
    ToolErrorCode, ToolErrorEnvelope, SEARCH_SESSIONS_SLA_TARGET_MS, SEARCH_SESSIONS_TOOL,
};
use anyhow::{Context, Result};
use moraine_conversations::{
    McpEventType as RepoMcpEventType, McpTurnOpen, RepoError, SearchMcpEventHit,
    SearchMcpEventsQuery, SearchMcpEventsResult, SessionMetadata,
};
use serde_json::{json, Value};
use std::collections::HashMap;

const MAX_SNIPPET_CHARS: usize = 600;

impl AppState {
    pub(crate) async fn search_sessions_v1(&self, arguments: Value) -> Result<Value> {
        let perf = Performance::builder(SEARCH_SESSIONS_SLA_TARGET_MS);
        let raw_request = arguments.clone();

        let args = match parse_search_sessions_args(arguments) {
            Ok(args) => args,
            Err(error) => {
                return encode_search_sessions_error(raw_request, error, perf.finish());
            }
        };

        let canonical_request = canonical_request_json(&args)?;
        let mut cache = SearchLookupCache::default();

        if let Some(scope_error) = self
            .validate_search_scope(&args, canonical_request.clone(), &perf, &mut cache)
            .await?
        {
            return Ok(scope_error);
        }

        let repo_query = SearchMcpEventsQuery {
            query: args.query.clone(),
            n_hits: Some(args.n_hits),
            session_id: scoped_session_id(&args).map(ToOwned::to_owned),
            turn_seq: scoped_turn_seq(&args),
            event_types: Some(
                args.event_types
                    .iter()
                    .copied()
                    .map(repo_event_type)
                    .collect(),
            ),
            min_score: None,
            min_should_match: None,
        };

        let search_result = match self.repo.search_mcp_events(repo_query).await {
            Ok(result) => result,
            Err(error) => {
                return encode_search_sessions_error(
                    canonical_request,
                    repo_error_to_contract_error(error),
                    perf.finish(),
                );
            }
        };

        let performance = perf.finish();
        let warnings = search_warnings(&search_result);
        let data = match search_sessions_data_json(&args, &search_result, &cache) {
            Ok(data) => data,
            Err(error) => {
                return encode_search_sessions_error(canonical_request, error, performance);
            }
        };

        let payload = serde_json::to_value(
            ToolEnvelope::success(SEARCH_SESSIONS_TOOL, canonical_request, data, performance)
                .with_warnings(warnings),
        )
        .context("failed to encode search_sessions response envelope")?;
        Ok(tool_success_result(
            format_search_sessions_text(&payload),
            payload,
        ))
    }

    async fn validate_search_scope(
        &self,
        args: &CanonicalSearchSessionsArgs,
        request: Value,
        perf: &PerformanceBuilder,
        cache: &mut SearchLookupCache,
    ) -> Result<Option<Value>> {
        match args.within_id.as_ref() {
            None => Ok(None),
            Some(McpId::Session(session_id)) => {
                let raw_session_id = session_id.raw_session_id();
                match load_session_metadata(self, cache, raw_session_id).await {
                    Ok(Some(_)) => Ok(None),
                    Ok(None) => encode_search_sessions_error(
                        request.clone(),
                        not_found_error("session", session_id.to_string()),
                        perf.finish(),
                    )
                    .map(Some),
                    Err(error) => encode_search_sessions_error(
                        request.clone(),
                        repo_error_to_contract_error(error),
                        perf.finish(),
                    )
                    .map(Some),
                }
            }
            Some(McpId::Turn(turn_id)) => {
                let (raw_session_id, turn_seq) = turn_id.decode();
                match load_turn_open(self, cache, raw_session_id, turn_seq).await {
                    Ok(Some(_)) => Ok(None),
                    Ok(None) => encode_search_sessions_error(
                        request.clone(),
                        not_found_error("turn", turn_id.to_string()),
                        perf.finish(),
                    )
                    .map(Some),
                    Err(error) => encode_search_sessions_error(
                        request.clone(),
                        repo_error_to_contract_error(error),
                        perf.finish(),
                    )
                    .map(Some),
                }
            }
            Some(McpId::Event(_)) => unreachable!("contract validation rejects event scope"),
        }
    }
}

#[derive(Default)]
struct SearchLookupCache {
    sessions: HashMap<String, SessionMetadata>,
    turns: HashMap<(String, u32), McpTurnOpen>,
}

async fn load_session_metadata(
    state: &AppState,
    cache: &mut SearchLookupCache,
    session_id: &str,
) -> Result<Option<SessionMetadata>, RepoError> {
    if let Some(metadata) = cache.sessions.get(session_id) {
        return Ok(Some(metadata.clone()));
    }

    let metadata = state.repo.get_session_metadata(session_id).await?;
    if let Some(metadata) = &metadata {
        cache
            .sessions
            .insert(session_id.to_string(), metadata.clone());
    }

    Ok(metadata)
}

async fn load_turn_open(
    state: &AppState,
    cache: &mut SearchLookupCache,
    session_id: &str,
    turn_seq: u32,
) -> Result<Option<McpTurnOpen>, RepoError> {
    let key = (session_id.to_string(), turn_seq);
    if let Some(turn) = cache.turns.get(&key) {
        return Ok(Some(turn.clone()));
    }

    let turn = state.repo.get_mcp_turn(session_id, turn_seq).await?;
    if let Some(turn) = &turn {
        cache.turns.insert(key, turn.clone());
    }

    Ok(turn)
}

fn parse_search_sessions_args(
    arguments: Value,
) -> Result<CanonicalSearchSessionsArgs, ContractError> {
    serde_json::from_value::<SearchSessionsArgs>(arguments)
        .map_err(|error| {
            ContractError::new(
                ToolErrorCode::InvalidRequest,
                "search_sessions expects a JSON object with a string query",
            )
            .with_details(json!({ "serde_error": error.to_string() }))
        })?
        .validate()
}

fn scoped_session_id(args: &CanonicalSearchSessionsArgs) -> Option<&str> {
    match args.within_id.as_ref() {
        Some(McpId::Session(session_id)) => Some(session_id.raw_session_id()),
        Some(McpId::Turn(turn_id)) => Some(turn_id.raw_session_id()),
        _ => None,
    }
}

fn scoped_turn_seq(args: &CanonicalSearchSessionsArgs) -> Option<u32> {
    args.within_id.as_ref()?.as_turn().map(McpTurnId::turn_seq)
}

fn repo_event_type(event_type: crate::contract::McpEventType) -> RepoMcpEventType {
    match event_type {
        crate::contract::McpEventType::UserInput => RepoMcpEventType::UserInput,
        crate::contract::McpEventType::AssistantResponse => RepoMcpEventType::AssistantResponse,
        crate::contract::McpEventType::Reasoning => RepoMcpEventType::Reasoning,
        crate::contract::McpEventType::ToolCall => RepoMcpEventType::ToolCall,
        crate::contract::McpEventType::ToolResponse => RepoMcpEventType::ToolResponse,
        crate::contract::McpEventType::Compaction => RepoMcpEventType::Compaction,
        crate::contract::McpEventType::System => RepoMcpEventType::System,
        crate::contract::McpEventType::Runtime => RepoMcpEventType::Runtime,
        crate::contract::McpEventType::Unknown => RepoMcpEventType::Unknown,
    }
}

fn canonical_request_json(args: &CanonicalSearchSessionsArgs) -> Result<Value> {
    serde_json::to_value(args).context("failed to encode canonical search_sessions request")
}

fn search_sessions_data_json(
    args: &CanonicalSearchSessionsArgs,
    result: &SearchMcpEventsResult,
    cache: &SearchLookupCache,
) -> Result<Value, ContractError> {
    let results = result
        .hits
        .iter()
        .map(|hit| search_hit_json(hit, cache))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(json!({
        "result_count": results.len(),
        "limit": args.n_hits,
        "truncated": result.truncated || result.stats.limit_capped,
        "results": results,
    }))
}

fn search_hit_json(
    hit: &SearchMcpEventHit,
    cache: &SearchLookupCache,
) -> Result<Value, ContractError> {
    let event_id = mcp_event_id(&hit.event_uid)?;
    let turn_id = mcp_turn_id(&hit.session_id, hit.turn_seq)?;
    let session_id = mcp_session_id(&hit.session_id)?;
    let turn_key = (hit.session_id.clone(), hit.turn_seq);
    let turn = cache.turns.get(&turn_key);
    let metadata = cache.sessions.get(&hit.session_id);
    let latest_turn = metadata.and_then(|metadata| {
        cache
            .turns
            .get(&(hit.session_id.clone(), metadata.total_turns))
    });
    let (snippet, snippet_truncated) = compact_snippet(&hit.snippet, hit.snippet_truncated);

    let terminal_from_payload = is_terminal_payload_type(&hit.payload_type);
    let turn_completed = turn
        .map(|turn| turn.completed)
        .unwrap_or(terminal_from_payload);
    let turn_event_count = turn
        .map(|turn| turn.metadata.total_events)
        .unwrap_or(hit.turn_event_count);
    let event_terminal = turn
        .and_then(|turn| turn.terminal_event_uid.as_deref())
        .map(|terminal_event_uid| terminal_event_uid == hit.event_uid)
        .unwrap_or(terminal_from_payload);
    let session_completed = latest_turn
        .map(|turn| turn.completed)
        .or_else(|| {
            metadata
                .and_then(|metadata| {
                    cache
                        .turns
                        .get(&(hit.session_id.clone(), metadata.total_turns))
                })
                .map(|turn| turn.completed)
        })
        .unwrap_or(false);

    Ok(json!({
        "rank": hit.rank,
        "score": score_unit(hit.score),
        "id": event_id,
        "event": {
            "id": event_id,
            "type": hit.event_type.as_str(),
            "timestamp": format_rfc3339_utc_millis(hit.event_unix_ms),
            "ordinal": hit.event_ordinal,
            "terminal": event_terminal,
        },
        "turn": {
            "id": turn_id,
            "ordinal": hit.turn_ordinal,
            "completed": turn_completed,
            "event_count": turn_event_count,
        },
        "session": {
            "id": session_id,
            "title": hit.session_title.as_deref().or(hit.session_slug.as_deref()),
            "source": hit.source_name.as_deref().or(hit.harness.as_deref()),
            "started_at": metadata
                .map(|metadata| format_rfc3339_utc_millis(metadata.first_event_unix_ms))
                .or_else(|| {
                    hit.session_started_at
                        .as_deref()
                        .map(format_repository_timestamp)
                }),
            "updated_at": metadata
                .map(|metadata| format_rfc3339_utc_millis(metadata.last_event_unix_ms))
                .or_else(|| {
                    hit.session_updated_at
                        .as_deref()
                        .map(format_repository_timestamp)
                }),
            "completed": session_completed,
        },
        "snippet": {
            "text": snippet,
            "truncated": snippet_truncated,
        },
        "open": {
            "event_id": event_id,
            "turn_id": turn_id,
            "session_id": session_id,
        }
    }))
}

fn mcp_session_id(session_id: &str) -> Result<String, ContractError> {
    McpSessionId::from_raw_session_id(session_id)
        .map(|id| id.to_string())
        .map_err(internal_id_error)
}

fn mcp_turn_id(session_id: &str, turn_seq: u32) -> Result<String, ContractError> {
    McpTurnId::from_raw_session_id_and_turn_seq(session_id, turn_seq)
        .map(|id| id.to_string())
        .map_err(internal_id_error)
}

fn mcp_event_id(event_uid: &str) -> Result<String, ContractError> {
    McpEventId::from_raw_event_uid(event_uid)
        .map(|id| id.to_string())
        .map_err(internal_id_error)
}

fn compact_snippet(snippet: &str, already_truncated: bool) -> (String, bool) {
    let trimmed = snippet.trim();
    if trimmed.chars().count() <= MAX_SNIPPET_CHARS {
        return (trimmed.to_string(), already_truncated);
    }

    let mut compact = trimmed
        .chars()
        .take(MAX_SNIPPET_CHARS.saturating_sub(3))
        .collect::<String>();
    compact.push_str("...");
    (compact, true)
}

fn score_unit(score: f64) -> f64 {
    if score.is_finite() {
        score.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn is_terminal_payload_type(payload_type: &str) -> bool {
    matches!(payload_type, "task_complete" | "turn_aborted")
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

fn search_warnings(result: &SearchMcpEventsResult) -> Vec<String> {
    let mut warnings = Vec::new();
    if result.stats.limit_capped {
        warnings.push(format!(
            "repository capped n_hits from {} to {}",
            result.stats.requested_n_hits, result.stats.effective_n_hits
        ));
    }
    warnings
}

fn encode_search_sessions_error(
    request: Value,
    error: ContractError,
    performance: Performance,
) -> Result<Value> {
    let payload = serde_json::to_value(ToolErrorEnvelope::error(
        SEARCH_SESSIONS_TOOL,
        request,
        error,
        performance,
    ))
    .context("failed to encode search_sessions error envelope")?;
    Ok(handled_tool_error_result(
        format_search_sessions_error_text(&payload),
        payload,
    ))
}

fn format_search_sessions_text(payload: &Value) -> String {
    let request = payload.get("request").unwrap_or(&Value::Null);
    let query = request.get("query").and_then(Value::as_str).unwrap_or("");
    let data = payload.get("data").unwrap_or(&Value::Null);
    let result_count = data
        .get("result_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut lines = vec![format!(
        "Found {result_count} search hit(s) for \"{query}\"."
    )];

    if let Some(results) = data.get("results").and_then(Value::as_array) {
        for result in results.iter().take(10) {
            let rank = result.get("rank").and_then(Value::as_u64).unwrap_or(0);
            let score = result.get("score").and_then(Value::as_f64).unwrap_or(0.0);
            let event_type = result
                .pointer("/event/type")
                .and_then(Value::as_str)
                .unwrap_or("event");
            let event_id = result
                .pointer("/open/event_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            let title = result
                .pointer("/session/title")
                .and_then(Value::as_str)
                .unwrap_or("untitled session");
            let snippet = result
                .pointer("/snippet/text")
                .and_then(Value::as_str)
                .unwrap_or("");
            lines.push(format!(
                "{rank}. {event_type} score={score:.2} {title}: {snippet} ({event_id})"
            ));
        }
    }

    lines.join("\n")
}

fn format_search_sessions_error_text(payload: &Value) -> String {
    let error = payload.get("error").unwrap_or(&Value::Null);
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("internal_error");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("search_sessions failed");
    format!("search_sessions error ({code}): {message}")
}

fn not_found_error(kind: &str, id: String) -> ContractError {
    ContractError::new(ToolErrorCode::NotFound, format!("{kind} not found"))
        .with_details(json!({ "kind": kind, "id": id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{McpEventType, OpenV1Args, SearchSessionsArgs};

    #[test]
    fn parses_and_canonicalizes_search_sessions_args() {
        let args = parse_search_sessions_args(json!({
            "query": "  cargo test failure  ",
            "event_types": ["tool_response", "user_input", "tool_response"],
            "n_hits": 3
        }))
        .expect("valid args");

        assert_eq!(args.query, "cargo test failure");
        assert_eq!(
            args.event_types,
            vec![McpEventType::UserInput, McpEventType::ToolResponse]
        );
        assert_eq!(args.n_hits, 3);
    }

    #[test]
    fn rejects_event_scope_before_repository_search() {
        let event_id = McpEventId::from_raw_event_uid("evt-1")
            .expect("event id")
            .to_string();
        let error = parse_search_sessions_args(json!({
            "query": "migration",
            "within_id": event_id
        }))
        .expect_err("event scope rejected");

        assert_eq!(error.code(), ToolErrorCode::InvalidRequest);
    }

    #[test]
    fn shapes_search_hit_with_mcp_ids_and_no_full_payloads() {
        let hit = SearchMcpEventHit {
            rank: 1,
            event_uid: "evt-1".to_string(),
            session_id: "sess-1".to_string(),
            event_type: RepoMcpEventType::ToolResponse,
            event_time: "2026-04-29 18:42:31.125".to_string(),
            event_unix_ms: 1_777_487_751_125,
            turn_seq: 2,
            turn_ordinal: 2,
            event_order: 5,
            event_ordinal: 3,
            turn_event_count: 4,
            session_started_at: None,
            session_updated_at: None,
            session_title: Some("Fix tests".to_string()),
            session_slug: None,
            session_summary: None,
            source_name: Some("codex".to_string()),
            harness: None,
            inference_provider: None,
            event_class: "tool_result".to_string(),
            payload_type: "tool_result".to_string(),
            actor_role: "tool".to_string(),
            tool_name: Some("cargo".to_string()),
            tool_phase: None,
            call_id: None,
            item_id: None,
            model: None,
            endpoint_kind: None,
            source_ref: None,
            snippet: "cargo test passed".to_string(),
            snippet_truncated: false,
            text_content: Some("full content should not appear".to_string()),
            payload_json: Some("{\"large\":true}".to_string()),
            score: 0.7,
            raw_score: 12.0,
            matched_terms: 2,
            doc_len: 42,
        };
        let mut cache = SearchLookupCache::default();
        cache.sessions.insert(
            "sess-1".to_string(),
            SessionMetadata {
                session_id: "sess-1".to_string(),
                first_event_time: "unused".to_string(),
                first_event_unix_ms: 1_777_487_700_000,
                last_event_time: "unused".to_string(),
                last_event_unix_ms: 1_777_487_800_000,
                total_turns: 2,
                total_events: 8,
                user_messages: 1,
                assistant_messages: 1,
                tool_calls: 1,
                tool_results: 1,
                mode: moraine_conversations::ConversationMode::Chat,
                first_event_uid: "evt-0".to_string(),
                last_event_uid: "evt-1".to_string(),
                last_actor_role: "assistant".to_string(),
            },
        );
        cache.turns.insert(
            ("sess-1".to_string(), 2),
            McpTurnOpen {
                metadata: moraine_conversations::TurnSummary {
                    session_id: "sess-1".to_string(),
                    turn_seq: 2,
                    turn_id: "turn-2".to_string(),
                    started_at: "unused".to_string(),
                    started_at_unix_ms: 1_777_487_740_000,
                    ended_at: "unused".to_string(),
                    ended_at_unix_ms: 1_777_487_800_000,
                    total_events: 4,
                    user_messages: 1,
                    assistant_messages: 1,
                    tool_calls: 1,
                    tool_results: 1,
                    reasoning_items: 0,
                },
                events: Vec::new(),
                user_input_summary: None,
                final_response_summary: None,
                tools_called: Vec::new(),
                normalized_event_types: Vec::new(),
                completed: true,
                terminal_event_uid: Some("evt-1".to_string()),
                previous_turn: None,
                next_turn: None,
                first_event: None,
                last_event: None,
            },
        );

        let shaped = search_hit_json(&hit, &cache).expect("shape hit");

        assert_eq!(shaped["id"], "event:ZXZ0LTE");
        assert_eq!(shaped["open"]["turn_id"], "turn:c2Vzcy0x:2");
        assert_eq!(shaped["event"]["type"], "tool_response");
        assert_eq!(shaped["event"]["terminal"], true);
        assert_eq!(shaped["turn"]["completed"], true);
        assert_eq!(shaped["session"]["completed"], true);
        assert!(shaped.get("text_content").is_none());
        assert!(shaped.get("payload_json").is_none());
    }

    #[test]
    fn shaped_search_open_ids_are_valid_open_args() {
        let hit = SearchMcpEventHit {
            rank: 1,
            event_uid: "evt-open".to_string(),
            session_id: "sess-open".to_string(),
            event_type: RepoMcpEventType::AssistantResponse,
            event_time: "2026-04-29 18:42:31.125".to_string(),
            event_unix_ms: 1_777_487_751_125,
            turn_seq: 3,
            turn_ordinal: 3,
            event_order: 7,
            event_ordinal: 2,
            turn_event_count: 2,
            session_started_at: None,
            session_updated_at: None,
            session_title: None,
            session_slug: None,
            session_summary: None,
            source_name: None,
            harness: None,
            inference_provider: None,
            event_class: "message".to_string(),
            payload_type: "message".to_string(),
            actor_role: "assistant".to_string(),
            tool_name: None,
            tool_phase: None,
            call_id: None,
            item_id: None,
            model: None,
            endpoint_kind: None,
            source_ref: None,
            snippet: "assistant answer".to_string(),
            snippet_truncated: false,
            text_content: None,
            payload_json: None,
            score: 0.9,
            raw_score: 10.0,
            matched_terms: 1,
            doc_len: 12,
        };
        let shaped = search_hit_json(&hit, &SearchLookupCache::default()).expect("shape hit");

        for field in ["event_id", "turn_id", "session_id"] {
            let open_id = shaped["open"][field]
                .as_str()
                .expect("search result includes open id")
                .to_string();
            let canonical = OpenV1Args {
                id: Some(open_id.clone()),
            }
            .validate()
            .expect("open accepts search result id");

            assert_eq!(canonical.id.to_string(), open_id);
        }
    }

    #[test]
    fn parse_errors_return_invalid_request() {
        let error =
            parse_search_sessions_args(json!({ "n_hits": 1 })).expect_err("missing query rejected");

        assert_eq!(error.code(), ToolErrorCode::InvalidRequest);
    }

    #[test]
    fn contract_validation_rejects_blank_query() {
        let error = SearchSessionsArgs {
            query: "   ".to_string(),
            within_id: None,
            event_types: None,
            n_hits: None,
        }
        .validate()
        .expect_err("blank query rejected");

        assert_eq!(error.code(), ToolErrorCode::InvalidRequest);
    }
}
