use super::{
    handled_tool_error_result, internal_id_error, repo_error_to_contract_error,
    request_performance, tool_success_result, AppState,
};
use crate::contract::{
    format_rfc3339_utc_millis, CanonicalListSessionsArgs, ContractError, ListSessionsArgs,
    ListSessionsMode, ListSessionsSort, McpSessionId, Performance, ToolEnvelope, ToolErrorCode,
    ToolErrorEnvelope, LIST_SESSIONS_DEADLINE_MS, LIST_SESSIONS_TOOL,
};
use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use moraine_conversations::{
    ConversationListSort as RepoListSort, ConversationMode as RepoConversationMode,
    McpSessionListFilter, McpSessionListItem, Page, PageRequest,
};
use serde_json::{json, Value};
use tokio::time::{timeout, Duration};
use tracing::warn;

impl AppState {
    pub(crate) async fn list_sessions_v1(&self, arguments: Value) -> Result<Value> {
        let perf = request_performance();
        let raw_request = arguments.clone();

        let args = match parse_list_sessions_args(arguments, self.cfg.mcp.max_results) {
            Ok(args) => args,
            Err(error) => {
                return encode_list_sessions_error(raw_request, error, perf.finish());
            }
        };
        let canonical_request = canonical_request_json(&args);
        let repo_filter = McpSessionListFilter {
            start_unix_ms: args.start_unix_ms,
            end_unix_ms: args.end_unix_ms,
            mode: args.mode.map(repo_mode),
            harness: args.harness.clone(),
            source_name: args.source.clone(),
            sort: repo_sort(args.sort),
        };
        let repo_page = PageRequest {
            limit: args.limit,
            cursor: args.cursor.clone(),
        };

        let page_result = timeout(
            Duration::from_millis(LIST_SESSIONS_DEADLINE_MS),
            self.repo.list_mcp_sessions(repo_filter, repo_page),
        )
        .await;

        let page = match page_result {
            Ok(Ok(page)) => page,
            Ok(Err(error)) => {
                return encode_list_sessions_error(
                    canonical_request,
                    repo_error_to_contract_error(error),
                    perf.finish(),
                );
            }
            Err(_) => {
                return encode_list_sessions_error(
                    canonical_request,
                    ContractError::new(
                        ToolErrorCode::DeadlineExceeded,
                        "list_sessions exceeded its response deadline",
                    )
                    .with_details(json!({ "deadline_ms": LIST_SESSIONS_DEADLINE_MS })),
                    perf.finish(),
                );
            }
        };

        let performance = perf.finish();
        let data = match list_sessions_data_json(&args, &page) {
            Ok(data) => data,
            Err(error) => {
                return encode_list_sessions_error(canonical_request, error, performance);
            }
        };

        let payload = serde_json::to_value(ToolEnvelope::success(
            LIST_SESSIONS_TOOL,
            canonical_request,
            data,
            performance,
        ))
        .context("failed to encode list_sessions response envelope")?;
        Ok(tool_success_result(
            format_list_sessions_text(&payload),
            payload,
        ))
    }
}

fn parse_list_sessions_args(
    arguments: Value,
    max_results: u16,
) -> Result<CanonicalListSessionsArgs, ContractError> {
    serde_json::from_value::<ListSessionsArgs>(arguments)
        .map_err(|error| {
            ContractError::new(
                ToolErrorCode::InvalidRequest,
                "list_sessions expects a JSON object with start_datetime and end_datetime",
            )
            .with_details(json!({ "serde_error": error.to_string() }))
        })?
        .validate(max_results)
}

fn canonical_request_json(args: &CanonicalListSessionsArgs) -> Value {
    json!({
        "start_datetime": args.start_datetime,
        "end_datetime": args.end_datetime,
        "mode": args.mode.map(|mode| mode.as_str()),
        "harness": args.harness.as_deref(),
        "source": args.source.as_deref(),
        "sort": args.sort.as_str(),
        "limit": args.limit,
        "cursor": args.cursor.as_deref(),
    })
}

fn repo_mode(mode: ListSessionsMode) -> RepoConversationMode {
    match mode {
        ListSessionsMode::WebSearch => RepoConversationMode::WebSearch,
        ListSessionsMode::McpInternal => RepoConversationMode::McpInternal,
        ListSessionsMode::ToolCalling => RepoConversationMode::ToolCalling,
        ListSessionsMode::Chat => RepoConversationMode::Chat,
    }
}

fn repo_sort(sort: ListSessionsSort) -> RepoListSort {
    match sort {
        ListSessionsSort::Asc => RepoListSort::Asc,
        ListSessionsSort::Desc => RepoListSort::Desc,
    }
}

fn list_sessions_data_json(
    args: &CanonicalListSessionsArgs,
    page: &Page<McpSessionListItem>,
) -> Result<Value, ContractError> {
    // Skip (and warn about) rows whose identifier can't be encoded — e.g. the
    // empty-`session_id` orphans left by Workflow journals ingested before the
    // exclusion landed (#386). One malformed row must never fail the whole
    // page. Rank advances only for kept rows so it stays contiguous (1..=N).
    let mut sessions = Vec::with_capacity(page.items.len());
    for session in &page.items {
        // Rank is 1-based over KEPT rows so it stays contiguous when a row is
        // skipped — `sessions.len()` only grows on a successful push.
        let rank = sessions.len() + 1;
        match session_json(rank, session) {
            Ok(value) => sessions.push(value),
            Err(error) => warn!(
                session_id = %session.session_id,
                error = %error,
                "list_sessions: skipping session row with an invalid identifier"
            ),
        }
    }

    Ok(json!({
        "result_count": sessions.len(),
        "limit": args.limit,
        "truncated": page.next_cursor.is_some(),
        "sessions": sessions,
        "next_cursor": page.next_cursor.as_deref(),
    }))
}

fn session_json(rank: usize, session: &McpSessionListItem) -> Result<Value, ContractError> {
    let session_id = mcp_session_id(&session.session_id)?;
    let display_label = session_display_label(session);

    Ok(json!({
        "rank": rank,
        "id": session_id,
        "session": {
            "id": session_id,
            "title": session.title.as_deref(),
            "display_label": display_label,
            "harness": session.harness.as_deref(),
            "source": session.source.as_deref(),
            "started_at": format_rfc3339_utc_millis(session.first_event_unix_ms),
            "updated_at": format_rfc3339_utc_millis(session.last_event_unix_ms),
            "completed": session.completed,
            "turn_count": session.total_turns,
            "event_count": session.total_events,
            "mode": session.mode.as_str(),
            "session_slug": session.session_slug.as_deref(),
            "session_summary": session.session_summary.as_deref(),
        },
        "open": {
            "session_id": session_id,
        }
    }))
}

fn session_display_label(session: &McpSessionListItem) -> String {
    if let Some(label) = session
        .title
        .as_deref()
        .filter(|label| !label.trim().is_empty())
        .or(session.session_summary.as_deref())
        .filter(|label| !label.trim().is_empty())
        .or(session.session_slug.as_deref())
        .filter(|label| !label.trim().is_empty())
    {
        return label.to_string();
    }

    let harness = readable_harness(session.harness.as_deref().unwrap_or("session"));
    let mode = readable_mode(session.mode.as_str());
    let updated_at = compact_utc_datetime(session.last_event_unix_ms);
    let turns = pluralize(session.total_turns as u64, "turn", "turns");

    format!("{harness}, {mode}, {updated_at}, {turns}")
}

fn readable_harness(harness: &str) -> &str {
    match harness {
        "codex" => "Codex",
        "claude-code" => "Claude Code",
        "cursor" => "Cursor",
        "hermes" => "Hermes",
        "kimi-cli" => "Kimi CLI",
        "opencode" => "OpenCode",
        "pi-coding-agent" => "Pi Coding Agent",
        "qwen-code" => "Qwen Code",
        _ => harness,
    }
}

fn readable_mode(mode: &str) -> &'static str {
    match mode {
        "web_search" => "web search",
        "tool_calling" => "tool session",
        "mcp_internal" => "MCP session",
        "chat" => "chat",
        _ => "session",
    }
}

fn compact_utc_datetime(unix_ms: i64) -> String {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let Some(datetime) = Utc.timestamp_millis_opt(unix_ms).single() else {
        return format_rfc3339_utc_millis(unix_ms);
    };
    let month_name = MONTHS
        .get(datetime.month0() as usize)
        .copied()
        .unwrap_or("Jan");

    format!(
        "{month_name} {} {:02}:{:02} UTC",
        datetime.day(),
        datetime.hour(),
        datetime.minute()
    )
}

fn pluralize(count: u64, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn mcp_session_id(session_id: &str) -> Result<String, ContractError> {
    McpSessionId::from_raw_session_id(session_id)
        .map(|id| id.to_string())
        .map_err(internal_id_error)
}

fn encode_list_sessions_error(
    request: Value,
    error: ContractError,
    performance: Performance,
) -> Result<Value> {
    let payload = serde_json::to_value(ToolErrorEnvelope::error(
        LIST_SESSIONS_TOOL,
        request,
        error,
        performance,
    ))
    .context("failed to encode list_sessions error envelope")?;
    Ok(handled_tool_error_result(
        format_list_sessions_error_text(&payload),
        payload,
    ))
}

fn format_list_sessions_text(payload: &Value) -> String {
    let request = payload.get("request").unwrap_or(&Value::Null);
    let start_datetime = request
        .get("start_datetime")
        .and_then(Value::as_str)
        .unwrap_or("");
    let end_datetime = request
        .get("end_datetime")
        .and_then(Value::as_str)
        .unwrap_or("");
    let data = payload.get("data").unwrap_or(&Value::Null);
    let result_count = data
        .get("result_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut lines = vec![format!(
        "Found {result_count} session(s) overlapping {start_datetime} to {end_datetime}."
    )];

    if let Some(sessions) = data.get("sessions").and_then(Value::as_array) {
        for session in sessions.iter().take(10) {
            let rank = session.get("rank").and_then(Value::as_u64).unwrap_or(0);
            let session_id = session
                .pointer("/open/session_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            let title = session
                .pointer("/session/title")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    session
                        .pointer("/session/session_summary")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                })
                .or_else(|| {
                    session
                        .pointer("/session/session_slug")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                });
            let display_label = session
                .pointer("/session/display_label")
                .and_then(Value::as_str)
                .unwrap_or("session");
            let updated_at = session
                .pointer("/session/updated_at")
                .and_then(Value::as_str)
                .unwrap_or("");
            let mode = session
                .pointer("/session/mode")
                .and_then(Value::as_str)
                .unwrap_or("chat");
            if let Some(title) = title {
                lines.push(format!(
                    "{rank}. {updated_at} {mode} {title} ({session_id})"
                ));
            } else {
                lines.push(format!("{rank}. {display_label} ({session_id})"));
            }
        }
    }

    if data.get("next_cursor").and_then(Value::as_str).is_some() {
        lines.push("More sessions are available with next_cursor.".to_string());
    }

    lines.join("\n")
}

fn format_list_sessions_error_text(payload: &Value) -> String {
    let error = payload.get("error").unwrap_or(&Value::Null);
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("internal_error");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("list_sessions failed");
    format!("list_sessions error ({code}): {message}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::AppConfig;
    use moraine_conversations::{
        ConversationMode, InMemoryConversationRepository, InMemoryConversationResponses, RepoConfig,
    };
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[test]
    fn deadline_envelope_is_a_handled_tool_error() {
        let result = encode_list_sessions_error(
            json!({}),
            ContractError::new(
                ToolErrorCode::DeadlineExceeded,
                "list_sessions exceeded its response deadline",
            ),
            Performance::builder().finish(),
        )
        .expect("deadline response");

        assert_eq!(result["isError"], true);
        assert_eq!(
            result["structuredContent"]["schema_version"],
            crate::contract::ERROR_SCHEMA_VERSION
        );
        assert_eq!(
            result["structuredContent"]["error"]["code"],
            "deadline_exceeded"
        );
    }

    #[test]
    fn parses_and_canonicalizes_list_sessions_args() {
        let args = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T17:00:00Z",
                "mode": "chat",
                "sort": "asc",
                "limit": 12,
                "cursor": "abc"
            }),
            50,
        )
        .expect("valid args");

        assert_eq!(args.start_unix_ms, 1_777_554_000_000);
        assert_eq!(args.end_unix_ms, 1_777_568_400_000);
        assert_eq!(args.mode, Some(ListSessionsMode::Chat));
        assert_eq!(args.sort, ListSessionsSort::Asc);
        assert_eq!(args.limit, 12);
        assert_eq!(args.cursor.as_deref(), Some("abc"));
    }

    #[test]
    fn rejects_missing_unknown_and_naive_datetimes() {
        let missing = parse_list_sessions_args(json!({}), 50).expect_err("missing rejected");
        assert_eq!(missing.code(), ToolErrorCode::InvalidRequest);

        let unknown = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "extra": true
            }),
            50,
        )
        .expect_err("unknown field rejected");
        assert_eq!(unknown.code(), ToolErrorCode::InvalidRequest);

        let naive = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00",
                "end_datetime": "2026-04-30T13:00:00-04:00"
            }),
            50,
        )
        .expect_err("naive datetime rejected");
        assert_eq!(naive.code(), ToolErrorCode::InvalidRequest);
    }

    #[test]
    fn rejects_non_increasing_range_and_limit_over_cap() {
        let range = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T13:00:00Z",
                "end_datetime": "2026-04-30T13:00:00Z"
            }),
            50,
        )
        .expect_err("empty range rejected");
        assert_eq!(range.code(), ToolErrorCode::InvalidRequest);

        let limit = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "limit": 51
            }),
            50,
        )
        .expect_err("limit cap rejected");
        assert_eq!(limit.code(), ToolErrorCode::InvalidRequest);
    }

    #[test]
    fn rejects_negative_limit_with_field_specific_message() {
        let error = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "limit": -1
            }),
            25,
        )
        .expect_err("negative limit rejected");

        assert_eq!(error.code(), ToolErrorCode::InvalidRequest);
        assert_eq!(error.message(), "limit must be between 1 and 25");
    }

    #[tokio::test]
    async fn shapes_session_metadata_only_with_open_handle() {
        let page = Page {
            items: vec![McpSessionListItem {
                session_id: "sess-open".to_string(),
                first_event_time: "2026-04-30 09:00:00".to_string(),
                first_event_unix_ms: 1_777_554_000_000,
                last_event_time: "2026-04-30 09:10:00".to_string(),
                last_event_unix_ms: 1_777_554_600_000,
                total_turns: 3,
                total_events: 17,
                mode: ConversationMode::ToolCalling,
                completed: true,
                title: Some("Build failure triage".to_string()),
                source: Some("codex".to_string()),
                harness: Some("codex".to_string()),
                session_slug: Some("build-failure".to_string()),
                session_summary: Some("Build failure triage summary.".to_string()),
            }],
            next_cursor: None,
        };
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                list_mcp_sessions: Some(Ok(page)),
                ..InMemoryConversationResponses::default()
            },
        ));
        let state = AppState::embedded(AppConfig::default(), repository.clone());

        let result = state
            .list_sessions_v1(json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "harness": " codex ",
                "source": " codex ",
                "limit": 1
            }))
            .await
            .expect("list sessions");
        assert_eq!(result["isError"], json!(false));

        let data = &result["structuredContent"]["data"];
        let first = &data["sessions"][0];
        assert_eq!(first["id"], json!("session:c2Vzcy1vcGVu"));
        assert_eq!(first["session"]["harness"], json!("codex"));
        assert!(first["session"].get("originator").is_none());
        assert!(first["session"].get("project").is_none());
        assert_eq!(
            first["session"]["display_label"],
            json!("Build failure triage")
        );
        assert_eq!(first["open"]["session_id"], first["session"]["id"]);
        assert_eq!(first["session"]["turn_count"], json!(3));
        assert_eq!(first["session"]["event_count"], json!(17));
        assert_eq!(
            first["session"]["started_at"],
            json!("2026-04-30T13:00:00.000Z")
        );
        assert_eq!(
            first["session"]["updated_at"],
            json!("2026-04-30T13:10:00.000Z")
        );
        assert!(first.get("snippet").is_none());
        assert!(first.get("events").is_none());
        assert!(first.get("payload_json").is_none());

        let calls = repository.calls();
        assert_eq!(calls.list_mcp_sessions.len(), 1);
        let (filter, page) = &calls.list_mcp_sessions[0];
        assert_eq!(filter.start_unix_ms, 1_777_554_000_000);
        assert_eq!(filter.end_unix_ms, 1_777_568_400_000);
        assert_eq!(filter.mode, None);
        assert_eq!(filter.sort, RepoListSort::Desc);
        assert_eq!(filter.harness.as_deref(), Some("codex"));
        assert_eq!(filter.source_name.as_deref(), Some("codex"));
        assert_eq!(page.limit, 1);
        assert_eq!(page.cursor, None);
    }

    #[test]
    fn untitled_session_gets_privacy_safe_display_label() {
        let args = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "limit": 50
            }),
            50,
        )
        .expect("valid args");
        let page = Page {
            items: vec![McpSessionListItem {
                session_id: "sess-open".to_string(),
                first_event_time: "2026-04-30 09:00:00".to_string(),
                first_event_unix_ms: 1_777_554_000_000,
                last_event_time: "2026-04-30 09:10:00".to_string(),
                last_event_unix_ms: 1_777_554_600_000,
                total_turns: 29,
                total_events: 768,
                mode: ConversationMode::WebSearch,
                completed: false,
                title: None,
                source: Some("codex".to_string()),
                harness: Some("codex".to_string()),
                session_slug: None,
                session_summary: None,
            }],
            next_cursor: None,
        };

        let data = list_sessions_data_json(&args, &page).expect("page should encode");
        let session = &data["sessions"][0]["session"];
        assert_eq!(
            session["display_label"],
            json!("Codex, web search, Apr 30 13:10 UTC, 29 turns")
        );
        assert!(session.get("originator").is_none());
        assert!(session.get("project").is_none());

        let payload = json!({
            "request": canonical_request_json(&args),
            "data": data,
        });
        let text = format_list_sessions_text(&payload);
        assert!(text.contains("Codex, web search, Apr 30 13:10 UTC, 29 turns"));
        assert!(!text.contains("untitled session"));
    }

    #[test]
    fn blank_title_and_summary_fall_through_to_slug() {
        let args = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "limit": 50
            }),
            50,
        )
        .expect("valid args");
        let page = Page {
            items: vec![McpSessionListItem {
                session_id: "sess-open".to_string(),
                first_event_time: "2026-04-30 09:00:00".to_string(),
                first_event_unix_ms: 1_777_554_000_000,
                last_event_time: "2026-04-30 09:10:00".to_string(),
                last_event_unix_ms: 1_777_554_600_000,
                total_turns: 29,
                total_events: 768,
                mode: ConversationMode::Chat,
                completed: false,
                title: Some("   ".to_string()),
                source: Some("codex".to_string()),
                harness: Some("codex".to_string()),
                session_slug: Some("useful-slug".to_string()),
                session_summary: Some("\t\n".to_string()),
            }],
            next_cursor: None,
        };

        let data = list_sessions_data_json(&args, &page).expect("page should encode");
        assert_eq!(
            data["sessions"][0]["session"]["display_label"],
            json!("useful-slug")
        );

        let payload = json!({
            "request": canonical_request_json(&args),
            "data": data,
        });
        let text = format_list_sessions_text(&payload);
        assert!(text.contains("useful-slug"));
        assert!(!text.contains("chat     "));
    }

    #[test]
    fn readable_harness_covers_canonical_harness_ids() {
        let cases = [
            ("codex", "Codex"),
            ("claude-code", "Claude Code"),
            ("cursor", "Cursor"),
            ("hermes", "Hermes"),
            ("kimi-cli", "Kimi CLI"),
            ("opencode", "OpenCode"),
            ("pi-coding-agent", "Pi Coding Agent"),
            ("qwen-code", "Qwen Code"),
            ("future-harness", "future-harness"),
        ];

        for (raw, expected) in cases {
            assert_eq!(readable_harness(raw), expected);
        }

        let covered: BTreeSet<&str> = cases
            .iter()
            .map(|(raw, _)| *raw)
            .filter(|raw| *raw != "future-harness")
            .collect();
        let known: BTreeSet<&str> = moraine_config::KNOWN_INGEST_HARNESSES
            .iter()
            .copied()
            .collect();
        assert_eq!(covered, known);
    }

    #[test]
    fn skips_session_rows_with_empty_session_id() {
        let args = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "limit": 50
            }),
            50,
        )
        .expect("valid args");

        let item = |session_id: &str| McpSessionListItem {
            session_id: session_id.to_string(),
            first_event_time: "2026-04-30 13:00:00".to_string(),
            first_event_unix_ms: 1_777_554_000_000,
            last_event_time: "2026-04-30 13:10:00".to_string(),
            last_event_unix_ms: 1_777_554_600_000,
            total_turns: 1,
            total_events: 1,
            mode: ConversationMode::ToolCalling,
            completed: true,
            title: None,
            source: Some("claude-code".to_string()),
            harness: Some("claude-code".to_string()),
            session_slug: None,
            session_summary: None,
        };

        // A leading empty-session_id orphan (the #386 junk) must be dropped,
        // not fail the whole page; the valid row survives with a contiguous
        // rank of 1.
        let page = Page {
            items: vec![item(""), item("real-session")],
            next_cursor: None,
        };

        let data = list_sessions_data_json(&args, &page).expect("page must not fail on a bad row");
        assert_eq!(data["result_count"], json!(1));
        assert_eq!(
            data["sessions"].as_array().expect("sessions array").len(),
            1
        );
        let kept = &data["sessions"][0];
        assert_eq!(kept["rank"], json!(1));
        assert_eq!(
            kept["id"],
            json!(McpSessionId::from_raw_session_id("real-session")
                .expect("valid id")
                .to_string())
        );
    }

    #[test]
    fn skips_trailing_and_whitespace_rows_and_preserves_cursor() {
        let args = parse_list_sessions_args(
            json!({
                "start_datetime": "2026-04-30T09:00:00-04:00",
                "end_datetime": "2026-04-30T13:00:00-04:00",
                "limit": 50
            }),
            50,
        )
        .expect("valid args");

        let item = |session_id: &str| McpSessionListItem {
            session_id: session_id.to_string(),
            first_event_time: "2026-04-30 13:00:00".to_string(),
            first_event_unix_ms: 1_777_554_000_000,
            last_event_time: "2026-04-30 13:10:00".to_string(),
            last_event_unix_ms: 1_777_554_600_000,
            total_turns: 1,
            total_events: 1,
            mode: ConversationMode::ToolCalling,
            completed: true,
            title: None,
            source: Some("claude-code".to_string()),
            harness: Some("claude-code".to_string()),
            session_slug: None,
            session_summary: None,
        };

        // A whitespace-only id (rejected by the contract's trim check) and a
        // TRAILING empty-id orphan are both dropped; the valid row keeps rank
        // 1, and the repo-provided cursor is passed through untouched.
        let page = Page {
            items: vec![item("real-session"), item("   "), item("")],
            next_cursor: Some("opaque-cursor".to_string()),
        };

        let data = list_sessions_data_json(&args, &page).expect("page must not fail on bad rows");
        assert_eq!(data["result_count"], json!(1));
        assert_eq!(
            data["sessions"].as_array().expect("sessions array").len(),
            1
        );
        assert_eq!(data["sessions"][0]["rank"], json!(1));
        assert_eq!(data["truncated"], json!(true));
        assert_eq!(data["next_cursor"], json!("opaque-cursor"));
    }
}
