#![allow(clippy::collapsible_if, clippy::too_many_arguments)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::ClickHouseConfig;
use moraine_conversations::{
    AnalyticsRange, ClickHouseConversationRepository, ConversationListFilter, ConversationListSort,
    ConversationMode, ConversationRepository, ConversationSearchQuery, FileAttentionQuery,
    McpEventType, McpSessionListFilter, PageRequest, RepoConfig, RepoError, SearchEventKind,
    SearchEventsQuery, SearchMcpEventsQuery, SessionAnalyticsQuery, SessionEventsDirection,
    SessionEventsQuery, SessionLookback, SessionMetadataSearchQuery, SessionOriginScope,
    SessionStep, StoreProbe, TablePreviewQuery,
};
use serde_json::json;
use tokio::sync::Notify;

#[derive(Clone)]
struct ScriptedBarrier {
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

#[derive(Clone)]
struct ScriptedResponse {
    required: Vec<&'static str>,
    forbidden: Vec<&'static str>,
    status: StatusCode,
    body: String,
    barrier: Option<ScriptedBarrier>,
}

impl ScriptedResponse {
    fn rows(required: &[&'static str], rows: serde_json::Value) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::OK,
            barrier: None,
            body: json_each_row(rows),
        }
    }

    fn raw(required: &[&'static str], body: impl Into<String>) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::OK,
            barrier: None,
            body: body.into(),
        }
    }

    fn failure(required: &[&'static str], message: &'static str) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
            barrier: None,
            body: message.to_string(),
        }
    }

    fn forbidding(mut self, forbidden: &[&'static str]) -> Self {
        self.forbidden = forbidden.to_vec();
        self
    }

    fn blocked(mut self, reached: Arc<Notify>, release: Arc<Notify>) -> Self {
        self.barrier = Some(ScriptedBarrier { reached, release });
        self
    }
}

#[derive(Clone, Default)]
struct MockOptions {
    omit_second_snippet_row: bool,
    scripted_responses: Vec<ScriptedResponse>,
}

#[derive(Default)]
struct MockState {
    queries: Mutex<Vec<String>>,
    options: MockOptions,
    scripted_responses: Mutex<Option<VecDeque<ScriptedResponse>>>,
}

fn test_clickhouse_config(url: String) -> ClickHouseConfig {
    ClickHouseConfig {
        url,
        database: "moraine".to_string(),
        username: "default".to_string(),
        password: String::new(),
        timeout_seconds: 5.0,
        async_insert: true,
        wait_for_async_insert: true,
        allow_newer_server: false,
    }
}

fn json_each_row(rows: serde_json::Value) -> String {
    match rows {
        serde_json::Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                out.push_str(&item.to_string());
                out.push('\n');
            }
            out
        }
        value => format!("{value}\n"),
    }
}

fn json_envelope(rows: serde_json::Value) -> String {
    json!({ "data": rows }).to_string()
}

fn session_analytics_summary_row() -> serde_json::Value {
    json!({
        "session_id": "analytics-session",
        "first_event_time": "2026-06-01 10:00:00.000",
        "first_event_unix_ms": 1780308000000_i64,
        "last_event_time": "2026-06-01 10:05:00.000",
        "last_event_unix_ms": 1780308300000_i64,
        "total_turns": 2_u32,
        "total_events": 6_u64,
        "user_messages": 1_u64,
        "assistant_messages": 2_u64,
        "tool_calls": 1_u64,
        "tool_results": 1_u64,
        "mode": "tool_calling",
        "session_slug": "analytics-slug",
        "session_summary": "Analytics fixture session"
    })
}

fn session_analytics_turn_rows() -> serde_json::Value {
    json!([
        {
            "session_id": "analytics-session",
            "turn_seq": 1_u32,
            "turn_id": "turn-one",
            "started_at": "2026-06-01 10:00:00.000",
            "started_at_unix_ms": 1780308000000_i64,
            "ended_at": "2026-06-01 10:04:00.000",
            "ended_at_unix_ms": 1780308240000_i64,
            "total_events": 5_u64,
            "user_messages": 1_u64,
            "assistant_messages": 1_u64,
            "tool_calls": 1_u64,
            "tool_results": 1_u64,
            "reasoning_items": 1_u64
        },
        {
            "session_id": "analytics-session",
            "turn_seq": 2_u32,
            "turn_id": "turn-two",
            "started_at": "2026-06-01 10:05:00.000",
            "started_at_unix_ms": 1780308300000_i64,
            "ended_at": "2026-06-01 10:05:00.000",
            "ended_at_unix_ms": 1780308300000_i64,
            "total_events": 1_u64,
            "user_messages": 0_u64,
            "assistant_messages": 1_u64,
            "tool_calls": 0_u64,
            "tool_results": 0_u64,
            "reasoning_items": 0_u64
        }
    ])
}

fn session_analytics_event_rows() -> serde_json::Value {
    json!([
        {
            "session_id": "analytics-session",
            "event_order": 1_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000000_i64,
            "event_class": "message",
            "actor_role": "user",
            "payload_type": "text",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 0_u32,
            "model": " GPT-X ",
            "endpoint_kind": "",
            "input_tokens": 5_u32,
            "output_tokens": 2_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "first user fallback",
            "text_content": " ",
            "tool_args_json": "",
            "harness": "codex",
            "source_name": "codex-jsonl",
            "trace_id": "trace-454"
        },
        {
            "session_id": "analytics-session",
            "event_order": 2_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000100_i64,
            "event_class": "message",
            "actor_role": "assistant",
            "payload_type": "text",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 42_u32,
            "model": "GPT-X",
            "endpoint_kind": "generation",
            "input_tokens": 99_u32,
            "output_tokens": 99_u32,
            "cache_read_tokens": 99_u32,
            "cache_write_tokens": 99_u32,
            "token_usage_buckets": { "reasoning": 7_u64 },
            "token_usage_native_units": { "usd": 0.5, "zero": 0.0 },
            "text_preview": "clipped answer",
            "text_content": "full answer",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 3_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000200_i64,
            "event_class": "tool_call",
            "actor_role": "assistant",
            "payload_type": "tool_use",
            "call_id": "call-read",
            "tool_name": "Read",
            "tool_error": 1_u8,
            "latency_ms": 17_u32,
            "model": "GPT-X",
            "endpoint_kind": "",
            "input_tokens": 0_u32,
            "output_tokens": 0_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "",
            "tool_args_json": "{\"path\":\"src/lib.rs\"}",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 4_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000800_i64,
            "event_class": "tool_result",
            "actor_role": "tool",
            "payload_type": "tool_result",
            "call_id": "call-read",
            "tool_name": "Read",
            "tool_error": 1_u8,
            "latency_ms": 999_u32,
            "model": "",
            "endpoint_kind": "",
            "input_tokens": 0_u32,
            "output_tokens": 0_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "read failed",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 5_u64,
            "turn_seq": 1_u32,
            "event_unix_ms": 1780308000900_i64,
            "event_class": "reasoning",
            "actor_role": "assistant",
            "payload_type": "thinking",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 0_u32,
            "model": "GPT-X",
            "endpoint_kind": "",
            "input_tokens": 0_u32,
            "output_tokens": 0_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "consider the result",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        },
        {
            "session_id": "analytics-session",
            "event_order": 6_u64,
            "turn_seq": 2_u32,
            "event_unix_ms": 1780308300000_i64,
            "event_class": "message",
            "actor_role": "assistant",
            "payload_type": "text",
            "call_id": "",
            "tool_name": "",
            "tool_error": 0_u8,
            "latency_ms": 0_u32,
            "model": "Other",
            "endpoint_kind": "generation",
            "input_tokens": 0_u32,
            "output_tokens": 3_u32,
            "cache_read_tokens": 0_u32,
            "cache_write_tokens": 0_u32,
            "token_usage_buckets": {},
            "token_usage_native_units": {},
            "text_preview": "",
            "text_content": "second turn",
            "tool_args_json": "",
            "harness": "",
            "source_name": "",
            "trace_id": ""
        }
    ])
}

fn analytics_responses(
    window_literal: &'static str,
    interval_literal: &'static str,
    tokens: serde_json::Value,
    turns: serde_json::Value,
    concurrency: serde_json::Value,
) -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::rows(
            &[
                "toInt64(toUnixTimestamp(now())) AS database_now_unix",
                window_literal,
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
                "AND notEmpty(trimBoth(e.model))",
                "FORMAT JSONEachRow",
            ],
            json!([{
                "scan_from_unix": 100_000_u64,
                "scan_to_unix": 200_000_u64,
                "display_to_unix": 200_000_u64
            }]),
        ),
        ScriptedResponse::rows(
            &[
                interval_literal,
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= 100000",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) <= 200000",
                "ARRAY JOIN mapKeys(e.token_usage_buckets)",
                "ORDER BY bucket_unix ASC, model ASC, endpoint_kind ASC, bucket ASC",
                "FORMAT JSONEachRow",
            ],
            tokens,
        ),
        ScriptedResponse::rows(
            &[
                interval_literal,
                "toUInt64(uniqExact(tuple(e.session_id, e.request_id))) AS turns",
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= 100000",
                "ORDER BY bucket_unix ASC, model ASC",
                "FORMAT JSONEachRow",
            ],
            turns,
        ),
        ScriptedResponse::rows(
            &[
                interval_literal,
                "toUInt64(uniqExact(session_stream_key)) AS concurrent_sessions",
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
                "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= 100000",
                "ORDER BY bucket_unix ASC",
                "FORMAT JSONEachRow",
            ],
            concurrency,
        ),
    ]
}

fn heartbeat_columns(optional: &[&str]) -> serde_json::Value {
    let mut names = vec![
        "ts",
        "host",
        "service_version",
        "queue_depth",
        "files_active",
        "files_watched",
        "rows_raw_written",
        "rows_events_written",
        "rows_errors_written",
        "flush_latency_ms",
        "append_to_visible_p50_ms",
        "append_to_visible_p95_ms",
        "last_error",
    ];
    names.extend_from_slice(optional);
    serde_json::Value::Array(
        names
            .into_iter()
            .map(|name| json!({ "name": name }))
            .collect(),
    )
}

fn heartbeat_row() -> serde_json::Value {
    json!({
        "ts": "2026-06-01 10:00:00.000",
        "ts_unix_ms": 1780308000000_i64,
        "host": "ingest-host",
        "service_version": "0.17.1",
        "queue_depth": 4_u64,
        "files_active": 2_u32,
        "files_watched": 9_u32,
        "rows_raw_written": 100_u64,
        "rows_events_written": 90_u64,
        "rows_errors_written": 1_u64,
        "flush_latency_ms": 12_u32,
        "append_to_visible_p50_ms": 20_u32,
        "append_to_visible_p95_ms": 40_u32,
        "last_error": "",
        "watcher_backend": null,
        "watcher_error_count": null,
        "watcher_reset_count": null,
        "watcher_last_reset_unix_ms": null,
        "backend_sinks": null
    })
}

fn turn_summary_row(
    session_id: &str,
    turn_seq: u32,
    total_events: u64,
    user_messages: u64,
    assistant_messages: u64,
    tool_calls: u64,
    tool_results: u64,
    reasoning_items: u64,
) -> serde_json::Value {
    json!({
        "session_id": session_id,
        "turn_seq": turn_seq,
        "turn_id": turn_seq.to_string(),
        "started_at": format!("2026-02-01 10:{:02}:00", turn_seq),
        "started_at_unix_ms": 1769940000000_i64 + i64::from(turn_seq) * 60_000,
        "ended_at": format!("2026-02-01 10:{:02}:30", turn_seq),
        "ended_at_unix_ms": 1769940030000_i64 + i64::from(turn_seq) * 60_000,
        "total_events": total_events,
        "user_messages": user_messages,
        "assistant_messages": assistant_messages,
        "tool_calls": tool_calls,
        "tool_results": tool_results,
        "reasoning_items": reasoning_items,
    })
}

fn trace_event_row(
    session_id: &str,
    event_uid: &str,
    event_order: u64,
    turn_seq: u32,
    actor_role: &str,
    event_class: &str,
    payload_type: &str,
    text_content: &str,
    payload_json: &str,
    name: &str,
    call_id: &str,
) -> serde_json::Value {
    json!({
        "session_id": session_id,
        "event_uid": event_uid,
        "event_order": event_order,
        "turn_seq": turn_seq,
        "turn_index": turn_seq,
        "event_time": format!("2026-02-01 10:00:{:02}", event_order),
        "event_unix_ms": 1769940000000_i64 + (event_order as i64) * 1_000,
        "source_name": "codex",
        "actor_role": actor_role,
        "event_class": event_class,
        "payload_type": payload_type,
        "call_id": call_id,
        "name": name,
        "phase": "",
        "item_id": format!("item-{event_uid}"),
        "source_ref": format!("/tmp/{session_id}.jsonl:1:{event_order}"),
        "text_content": text_content,
        "payload_json": payload_json,
        "token_usage_json": "{}"
    })
}

async fn spawn_mock_server(options: MockOptions) -> (String, Arc<MockState>) {
    async fn handler(
        State(state): State<Arc<MockState>>,
        Query(params): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> (StatusCode, String) {
        if headers.get("content-length").is_none() {
            return (
                StatusCode::LENGTH_REQUIRED,
                "missing content-length".to_string(),
            );
        }

        let query = params.get("query").cloned().unwrap_or_default();
        state
            .queries
            .lock()
            .expect("query lock")
            .push(query.clone());

        let scripted_response = {
            let mut scripted = state
                .scripted_responses
                .lock()
                .expect("scripted response lock");
            scripted.as_mut().map(|responses| responses.pop_front())
        };
        if let Some(scripted_response) = scripted_response {
            let Some(response) = scripted_response else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unexpected extra query: {query}"),
                );
            };
            let missing = response
                .required
                .iter()
                .filter(|required| !query.contains(**required))
                .copied()
                .collect::<Vec<_>>();
            let forbidden = response
                .forbidden
                .iter()
                .filter(|forbidden| query.contains(**forbidden))
                .copied()
                .collect::<Vec<_>>();
            if !missing.is_empty() || !forbidden.is_empty() {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "script mismatch; missing={missing:?}; forbidden={forbidden:?}; query={query}"
                    ),
                );
            }
            if let Some(barrier) = response.barrier {
                barrier.reached.notify_one();
                barrier.release.notified().await;
            }
            return (response.status, response.body);
        }

        // --project-only origin-scope gate: the point lookup issued by
        // `session_in_scope`. Sessions whose ID contains "out-of-scope" are
        // outside the scope; everything else is inside it. Only the
        // standalone gate query starts with this prefix — list/search
        // queries embed the same subquery but match their own branches.
        if query.starts_with("SELECT session_id FROM (")
            && query.contains("argMin(cwd, tuple(event_ts, event_uid))")
        {
            let session_id = query
                .split("session_id = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("");
            if session_id.is_empty() || session_id.contains("out-of-scope") {
                return (StatusCode::OK, json_each_row(json!([])));
            }
            return (
                StatusCode::OK,
                json_each_row(json!([{ "session_id": session_id }])),
            );
        }

        if query.contains("argMax(session_id, doc_version) AS session_id")
            && query.contains("WHERE event_uid = 'evt-out-of-scope'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([{ "session_id": "sess-out-of-scope" }])),
            );
        }

        if query.contains("FROM `moraine`.`tool_io` FINAL")
            && query.contains("repo_rel_path = 'crates/foo.rs'")
            && query.contains("project_id = 'project-a'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess-normalized",
                        "event_uid": "evt-normalized",
                        "tool_call_id": "call-normalized",
                        "harness": "codex",
                        "tool_name": "edit",
                        "tool_phase": "request",
                        "matched_path": "/worktree-a/crates/foo.rs",
                        "match_kind": "path_suffix",
                        "worktree_root": "/worktree-a",
                        "cwd": "/worktree-a",
                        "event_unix_ms": 1769940100000_i64,
                        "event_order": 20_u64,
                        "turn_seq": 2_u32,
                        "input_preview": "{\"not_the_path\":\"hidden\"}",
                        "output_preview": ""
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`tool_io` FINAL")
            && query.contains("JSONExtractString(input_json, 'path')")
            && query.contains("crates/foo.rs")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess-normalized",
                        "event_uid": "evt-normalized",
                        "tool_call_id": "call-normalized",
                        "harness": "codex",
                        "tool_name": "edit",
                        "tool_phase": "request",
                        "matched_path": "/legacy-would-have-won/crates/foo.rs",
                        "match_kind": "path_suffix",
                        "worktree_root": "/legacy-would-have-won",
                        "cwd": "/legacy-would-have-won",
                        "event_unix_ms": 1769940100000_i64,
                        "event_order": 20_u64,
                        "turn_seq": 2_u32,
                        "input_preview": "{\"path\":\"/legacy-would-have-won/crates/foo.rs\"}",
                        "output_preview": ""
                    },
                    {
                        "session_id": "sess-legacy",
                        "event_uid": "evt-legacy",
                        "tool_call_id": "call-legacy",
                        "harness": "codex",
                        "tool_name": "bash",
                        "tool_phase": "request",
                        "matched_path": "",
                        "match_kind": "shell_path",
                        "worktree_root": "",
                        "cwd": "/legacy",
                        "event_unix_ms": 1769940000000_i64,
                        "event_order": 10_u64,
                        "turn_seq": 1_u32,
                        "input_preview": "{\"command\":\"rg crates/foo.rs\"}",
                        "output_preview": ""
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("AS completed")
            && query.contains("latest_terminal_payload_type")
        {
            if query.contains("s.session_id < 'sess_b'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_a",
                            "first_event_time": "2026-01-01 10:00:00",
                            "first_event_unix_ms": 1767261600000_i64,
                            "last_event_time": "2026-01-01 10:10:00",
                            "last_event_unix_ms": 1767262200000_i64,
                            "total_turns": 2,
                            "total_events": 20,
                            "mode": "web_search",
                            "completed": 0_u8,
                            "title": "",
                            "source": "codex",
                            "session_slug": "",
                            "session_summary": ""
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3,
                        "total_events": 30,
                        "mode": "web_search",
                        "completed": 1_u8,
                        "title": "Session C title",
                        "source": "codex",
                        "session_slug": "project-c",
                        "session_summary": "Session C summary"
                    },
                    {
                        "session_id": "sess_b",
                        "first_event_time": "2026-01-02 10:00:00",
                        "first_event_unix_ms": 1767348000000_i64,
                        "last_event_time": "2026-01-02 10:10:00",
                        "last_event_unix_ms": 1767348600000_i64,
                        "total_turns": 2,
                        "total_events": 22,
                        "mode": "web_search",
                        "completed": 1_u8,
                        "title": "Session B title",
                        "source": "codex",
                        "session_slug": "project-b",
                        "session_summary": "Session B summary"
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "total_turns": 2,
                        "total_events": 20,
                        "mode": "web_search",
                        "completed": 0_u8,
                        "title": "",
                        "source": "codex",
                        "session_slug": "",
                        "session_summary": ""
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("ORDER BY s.last_event_time DESC")
        {
            if query.contains("s.session_id < 'sess_b'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_a",
                            "first_event_time": "2026-01-01 10:00:00",
                            "first_event_unix_ms": 1767261600000_i64,
                            "last_event_time": "2026-01-01 10:10:00",
                            "last_event_unix_ms": 1767262200000_i64,
                            "total_turns": 2,
                            "total_events": 20,
                            "user_messages": 4,
                            "assistant_messages": 4,
                            "tool_calls": 2,
                            "tool_results": 2,
                            "mode": "web_search"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3,
                        "total_events": 30,
                        "user_messages": 6,
                        "assistant_messages": 6,
                        "tool_calls": 3,
                        "tool_results": 3,
                        "mode": "web_search",
                        "session_slug": "project-c",
                        "session_summary": "Session C summary"
                    },
                    {
                        "session_id": "sess_b",
                        "first_event_time": "2026-01-02 10:00:00",
                        "first_event_unix_ms": 1767348000000_i64,
                        "last_event_time": "2026-01-02 10:10:00",
                        "last_event_unix_ms": 1767348600000_i64,
                        "total_turns": 2,
                        "total_events": 22,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "total_turns": 2,
                        "total_events": 20,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("argMin(event_uid, tuple(event_time, event_order, event_uid))")
            && query.contains("argMax(actor_role, tuple(event_time, event_order, event_uid))")
            && query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE s.session_id =")
        {
            if query.contains("WHERE s.session_id = 'sess-missing'") {
                return (StatusCode::OK, json_each_row(json!([])));
            }

            if query.contains("WHERE s.session_id = 'sess-empty'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess-empty",
                            "first_event_time": "2026-01-04 09:00:00",
                            "first_event_unix_ms": 1767517200000_i64,
                            "last_event_time": "2026-01-04 09:01:00",
                            "last_event_unix_ms": 1767517260000_i64,
                            "total_turns": 1_u32,
                            "total_events": 2_u64,
                            "user_messages": 1_u64,
                            "assistant_messages": 1_u64,
                            "tool_calls": 0_u64,
                            "tool_results": 0_u64,
                            "mode": "chat",
                            "first_event_uid": "",
                            "last_event_uid": "",
                            "last_actor_role": ""
                        }
                    ])),
                );
            }

            if query.contains("WHERE s.session_id = 'sess-open'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess-open",
                            "first_event_time": "2026-02-01 10:01:00",
                            "first_event_unix_ms": 1769940060000_i64,
                            "last_event_time": "2026-02-01 10:02:30",
                            "last_event_unix_ms": 1769940150000_i64,
                            "total_turns": 2_u32,
                            "total_events": 8_u64,
                            "user_messages": 2_u64,
                            "assistant_messages": 2_u64,
                            "tool_calls": 1_u64,
                            "tool_results": 1_u64,
                            "mode": "tool_calling",
                            "first_event_uid": "evt-open-1",
                            "last_event_uid": "evt-open-8",
                            "last_actor_role": "system"
                        }
                    ])),
                );
            }

            if query.contains("WHERE s.session_id = 'sess-event'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess-event",
                            "first_event_time": "2026-02-01 10:00:01",
                            "first_event_unix_ms": 1769940001000_i64,
                            "last_event_time": "2026-02-01 10:00:04",
                            "last_event_unix_ms": 1769940004000_i64,
                            "total_turns": 2_u32,
                            "total_events": 4_u64,
                            "user_messages": 1_u64,
                            "assistant_messages": 1_u64,
                            "tool_calls": 0_u64,
                            "tool_results": 0_u64,
                            "mode": "chat",
                            "first_event_uid": "evt-event-1",
                            "last_event_uid": "evt-event-4",
                            "last_actor_role": "assistant"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3_u32,
                        "total_events": 30_u64,
                        "user_messages": 6_u64,
                        "assistant_messages": 6_u64,
                        "tool_calls": 3_u64,
                        "tool_results": 3_u64,
                        "mode": "web_search",
                        "first_event_uid": "evt-c-1",
                        "last_event_uid": "evt-c-42",
                        "last_actor_role": "assistant"
                    }
                ])),
            );
        }

        if query.contains("FROM (SELECT * FROM `moraine`.`events` FINAL)")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("AS title")
            && query.contains("AS session_summary")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "title": "Open model session",
                        "source": "codex-source",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "session_slug": "open-model-session",
                        "session_summary": "Session summary from metadata"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("ORDER BY s.last_event_time ASC")
        {
            if query.contains("s.session_id > 'sess_b'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_c",
                            "first_event_time": "2026-01-03 10:00:00",
                            "first_event_unix_ms": 1767434400000_i64,
                            "last_event_time": "2026-01-03 10:10:00",
                            "last_event_unix_ms": 1767435000000_i64,
                            "total_turns": 3,
                            "total_events": 30,
                            "user_messages": 6,
                            "assistant_messages": 6,
                            "tool_calls": 3,
                            "tool_results": 3,
                            "mode": "web_search"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "total_turns": 2,
                        "total_events": 20,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_b",
                        "first_event_time": "2026-01-02 10:00:00",
                        "first_event_unix_ms": 1767348000000_i64,
                        "last_event_time": "2026-01-02 10:10:00",
                        "last_event_unix_ms": 1767348600000_i64,
                        "total_turns": 2,
                        "total_events": 22,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3,
                        "total_events": 30,
                        "user_messages": 6,
                        "assistant_messages": 6,
                        "tool_calls": 3,
                        "tool_results": 3,
                        "mode": "web_search"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary`")
            && query.contains("WHERE session_id IN")
            && query.contains("toString(first_event_time) AS first_event_time")
            && query.contains("toString(last_event_time) AS last_event_time")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "last_event_time": "2026-01-03 10:10:00"
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "last_event_time": "2026-01-01 10:10:00"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`search_corpus_stats`") {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "docs": 100_u64,
                        "total_doc_len": 5000_u64
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`search_term_stats`") {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    { "term": "hello", "df": 20_u64 },
                    { "term": "world", "df": 10_u64 }
                ])),
            );
        }

        if query.contains("AS mcp_event_type")
            && query.contains("AS raw_score")
            && query.contains("FROM `moraine`.`search_postings` AS p")
        {
            let assistant_row = json!({
                "event_uid": "evt-c-42",
                "session_id": "sess_c",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_c.jsonl:1:42",
                "doc_len": 19_u32,
                "text_preview": "best assistant event in session c",
                "text_content": "best assistant event in session c with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-c\"}",
                "mcp_event_type": "assistant_response",
                "raw_score": 12.5,
                "matched_terms": 2_u64,
                "event_time": "2026-01-03 10:02:00",
                "event_unix_ms": 1767434520000_i64,
                "event_order": 42_u64,
                "turn_seq": 2_u32
            });
            let user_row = json!({
                "event_uid": "evt-c-user",
                "session_id": "sess_c",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "user",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_c.jsonl:1:41",
                "doc_len": 15_u32,
                "text_preview": "user asked about hello world",
                "text_content": "user asked about hello world in a prompt",
                "payload_json": "{\"type\":\"message\",\"role\":\"user\"}",
                "mcp_event_type": "user_input",
                "raw_score": 11.0,
                "matched_terms": 2_u64,
                "event_time": "2026-01-03 10:01:00",
                "event_unix_ms": 1767434460000_i64,
                "event_order": 41_u64,
                "turn_seq": 2_u32
            });
            let tool_row = json!({
                "event_uid": "evt-c-tool",
                "session_id": "sess_c",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "tool_result",
                "payload_type": "tool_result",
                "actor_role": "tool",
                "name": "bash",
                "phase": "completed",
                "source_ref": "/tmp/sess_c.jsonl:1:40",
                "doc_len": 21_u32,
                "text_preview": "cargo test failure output",
                "text_content": "cargo test failure output with stack details",
                "payload_json": "{\"tool\":\"bash\",\"status\":\"failed\"}",
                "mcp_event_type": "tool_response",
                "raw_score": 13.0,
                "matched_terms": 2_u64,
                "event_time": "2026-01-03 10:00:30",
                "event_unix_ms": 1767434430000_i64,
                "event_order": 40_u64,
                "turn_seq": 2_u32
            });
            let session_a_row = json!({
                "event_uid": "evt-a-11",
                "session_id": "sess_a",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_a.jsonl:1:11",
                "doc_len": 13_u32,
                "text_preview": "weaker assistant event in session a",
                "text_content": "weaker assistant event in session a with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-a\"}",
                "mcp_event_type": "assistant_response",
                "raw_score": 7.0,
                "matched_terms": 1_u64,
                "event_time": "2026-01-01 10:02:00",
                "event_unix_ms": 1767261720000_i64,
                "event_order": 11_u64,
                "turn_seq": 1_u32
            });
            let session_b_row = json!({
                "event_uid": "evt-b-9",
                "session_id": "sess_b",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_b.jsonl:1:9",
                "doc_len": 9_u32,
                "text_preview": "third assistant event",
                "text_content": "third assistant event with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-b\"}",
                "mcp_event_type": "assistant_response",
                "raw_score": 6.0,
                "matched_terms": 1_u64,
                "event_time": "2026-01-02 10:02:00",
                "event_unix_ms": 1767348120000_i64,
                "event_order": 9_u64,
                "turn_seq": 1_u32
            });

            let filter_clause = query
                .split_once("WHERE ")
                .and_then(|(_, tail)| tail.split_once("GROUP BY p.doc_id"))
                .map(|(filter, _)| filter)
                .unwrap_or(query.as_str());

            if query.contains("tr.session_id = 'sess_c' AND tr.turn_seq = 2") {
                return (StatusCode::OK, json_each_row(json!([tool_row])));
            }
            if query.contains("d.session_id = 'sess_a'") {
                return (StatusCode::OK, json_each_row(json!([session_a_row])));
            }
            if filter_clause.contains("lowerUTF8(d.actor_role) = 'user'")
                && !filter_clause.contains("lowerUTF8(d.actor_role) = 'assistant'")
            {
                return (StatusCode::OK, json_each_row(json!([user_row])));
            }
            if filter_clause.contains("lowerUTF8(d.actor_role) = 'assistant'")
                && !filter_clause.contains("lowerUTF8(d.actor_role) = 'user'")
            {
                return (StatusCode::OK, json_each_row(json!([assistant_row])));
            }
            if query.contains("LIMIT 3") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([assistant_row, session_a_row, session_b_row])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([assistant_row, session_a_row])),
            );
        }

        if query.contains("FROM `moraine`.`search_conversation_terms` AS ct") {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "score": 8.0,
                        "matched_terms": 2_u16
                    },
                    {
                        "session_id": "sess_a",
                        "score": 5.0,
                        "matched_terms": 1_u16
                    }
                ])),
            );
        }

        if query.contains("GROUP BY e.session_id")
            && query.contains("FROM `moraine`.`search_postings` AS p")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "harness": "codex",
                        "score": 12.5,
                        "matched_terms": 2_u16,
                        "event_count_considered": 3_u32,
                        "best_event_uid": "evt-c-42",
                        "snippet": "best match from session c"
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "harness": "codex",
                        "score": 7.0,
                        "matched_terms": 1_u16,
                        "event_count_considered": 2_u32,
                        "best_event_uid": "evt-a-11",
                        "snippet": "weaker match from session a"
                    }
                ])),
            );
        }

        if query.contains("GROUP BY p.doc_id")
            && query.contains("ORDER BY score DESC, event_uid ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "event_uid": "evt-c-42",
                        "session_id": "sess_c",
                        "source_name": "codex",
                        "harness": "codex",
                        "event_class": "message",
                        "payload_type": "text",
                        "actor_role": "assistant",
                        "name": "",
                        "phase": "",
                        "source_ref": "/tmp/sess_c.jsonl:1:42",
                        "doc_len": 19,
                        "text_preview": "best event in session c",
                        "text_content": "best event in session c with extra context",
                        "payload_json": "{\"type\":\"message\",\"topic\":\"session-c\"}",
                        "score": 12.5,
                        "matched_terms": 2_u64
                    },
                    {
                        "event_uid": "evt-a-11",
                        "session_id": "sess_a",
                        "source_name": "codex",
                        "harness": "codex",
                        "event_class": "message",
                        "payload_type": "text",
                        "actor_role": "assistant",
                        "name": "",
                        "phase": "",
                        "source_ref": "/tmp/sess_a.jsonl:1:11",
                        "doc_len": 13,
                        "text_preview": "weaker event in session a",
                        "text_content": "weaker event in session a with extra context",
                        "payload_json": "{\"type\":\"message\",\"topic\":\"session-a\"}",
                        "score": 7.0,
                        "matched_terms": 1_u64
                    }
                ])),
            );
        }

        if query.contains("WHERE event_uid IN")
            && query.contains("GROUP BY event_uid")
            && query.contains("AS text_content")
            && query.contains("AS payload_json")
            && query.contains("AS event_class")
            && query.contains("AS actor_role")
        {
            let mut rows = vec![json!({
                "event_uid": "evt-c-42",
                "snippet": "best match from session c",
                "text_content": "best match from session c with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-c\"}",
                "event_class": "message",
                "actor_role": "assistant"
            })];
            if !state.options.omit_second_snippet_row {
                rows.push(json!({
                    "event_uid": "evt-a-11",
                    "snippet": "weaker match from session a",
                    "text_content": "weaker match from session a with extra context",
                    "payload_json": "{\"type\":\"message\",\"topic\":\"session-a\"}",
                    "event_class": "message",
                    "actor_role": "assistant"
                }));
            }
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("WHERE event_kind = 'session_meta'")
            && query.contains("GROUP BY session_id")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "harness": "codex",
                        "session_slug": "project-c",
                        "session_summary": "Session C summary"
                    },
                    {
                        "session_id": "sess_a",
                        "harness": "codex",
                        "session_slug": "",
                        "session_summary": ""
                    }
                ])),
            );
        }

        if query.contains("WITH\n  ['rare','summary'] AS q_terms")
            && query.contains("FROM (SELECT * FROM `moraine`.`events` FINAL) AS e")
            && query.contains("WHERE e.event_kind = 'session_meta'")
            && query.contains("AS meta_event_uid")
            && query.contains("AS matched_terms")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_meta_summary",
                        "first_event_time": "2026-01-05 10:00:00",
                        "first_event_unix_ms": 1767607200000_i64,
                        "last_event_time": "2026-01-05 10:15:00",
                        "last_event_unix_ms": 1767608100000_i64,
                        "total_turns": 4_u32,
                        "total_events": 18_u64,
                        "user_messages": 5_u64,
                        "assistant_messages": 5_u64,
                        "tool_calls": 1_u64,
                        "tool_results": 1_u64,
                        "mode": "chat",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "session_slug": "rare-summary-session",
                        "session_summary": "Rare summary-only session about metadata discovery.",
                        "meta_event_uid": "meta-rare-1",
                        "score": 5.0,
                        "matched_terms": 2_u16,
                        "metadata_text": "{\"summary\":\"Rare summary-only session about metadata discovery.\"}"
                    }
                ])),
            );
        }

        if query.contains("row_number() OVER")
            && query.contains("PARTITION BY session_id, turn_seq")
            && query.contains("WHERE tr.event_uid IN")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "event_uid": "evt-c-tool",
                        "event_time": "2026-01-03 10:00:30",
                        "event_unix_ms": 1767434430000_i64,
                        "event_order": 40_u64,
                        "turn_seq": 2_u32,
                        "event_ordinal": 1_u32,
                        "turn_event_count": 3_u64,
                        "call_id": "call-bash-1",
                        "item_id": "item-tool",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-c-user",
                        "event_time": "2026-01-03 10:01:00",
                        "event_unix_ms": 1767434460000_i64,
                        "event_order": 41_u64,
                        "turn_seq": 2_u32,
                        "event_ordinal": 2_u32,
                        "turn_event_count": 3_u64,
                        "call_id": "",
                        "item_id": "item-user",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-c-42",
                        "event_time": "2026-01-03 10:02:00",
                        "event_unix_ms": 1767434520000_i64,
                        "event_order": 42_u64,
                        "turn_seq": 2_u32,
                        "event_ordinal": 3_u32,
                        "turn_event_count": 3_u64,
                        "call_id": "",
                        "item_id": "item-assistant",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-a-11",
                        "event_time": "2026-01-01 10:02:00",
                        "event_unix_ms": 1767261720000_i64,
                        "event_order": 11_u64,
                        "turn_seq": 1_u32,
                        "event_ordinal": 1_u32,
                        "turn_event_count": 1_u64,
                        "call_id": "",
                        "item_id": "item-a",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-b-9",
                        "event_time": "2026-01-02 10:02:00",
                        "event_unix_ms": 1767348120000_i64,
                        "event_order": 9_u64,
                        "turn_seq": 1_u32,
                        "event_ordinal": 1_u32,
                        "turn_event_count": 1_u64,
                        "call_id": "",
                        "item_id": "item-b",
                        "model": "gpt-5.3-codex"
                    }
                ])),
            );
        }

        if query.contains("SELECT session_id, event_order, turn_seq")
            && query.contains("WHERE event_uid = 'evt-open-full'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess-event",
                        "event_order": 2_u64,
                        "turn_seq": 1_u32
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("ORDER BY turn_seq ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    turn_summary_row("sess-open", 1, 5, 1, 1, 1, 1, 0),
                    turn_summary_row("sess-open", 2, 3, 1, 1, 0, 0, 0)
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
            && query.contains("WHERE session_id = 'sess-open'")
            && !query.contains("turn_seq =")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-open",
                        "evt-open-1",
                        1,
                        1,
                        "user",
                        "message",
                        "text",
                        "How should repository open models work?",
                        "{\"text\":\"How should repository open models work?\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-2",
                        2,
                        1,
                        "assistant",
                        "tool_call",
                        "function_call",
                        "",
                        "{\"name\":\"search_repo\"}",
                        "search_repo",
                        "call-search"
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-3",
                        3,
                        1,
                        "tool",
                        "tool_result",
                        "function_call_output",
                        "repo results",
                        "{\"result\":\"repo results\"}",
                        "search_repo",
                        "call-search"
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-4",
                        4,
                        1,
                        "assistant",
                        "message",
                        "text",
                        "First answer with repository context.",
                        "{\"text\":\"First answer with repository context.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-5",
                        5,
                        1,
                        "system",
                        "event_msg",
                        "task_complete",
                        "",
                        "{\"status\":\"complete\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-6",
                        6,
                        2,
                        "user",
                        "message",
                        "text",
                        "Confirm the final shape.",
                        "{\"text\":\"Confirm the final shape.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-7",
                        7,
                        2,
                        "assistant",
                        "message",
                        "text",
                        "Final response summary text.",
                        "{\"text\":\"Final response summary text.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-8",
                        8,
                        2,
                        "system",
                        "event_msg",
                        "task_complete",
                        "",
                        "{\"status\":\"complete\"}",
                        "",
                        ""
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete'")
            && query.contains("ORDER BY turn_seq ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    turn_summary_row("sess-incomplete", 1, 2, 1, 1, 0, 0, 0),
                    turn_summary_row("sess-incomplete", 2, 3, 1, 0, 1, 1, 0)
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq = 2")
            && query.contains("LIMIT 1")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row(
                    "sess-incomplete",
                    2,
                    3,
                    1,
                    0,
                    1,
                    1,
                    0
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-incomplete'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
            && !query.contains("turn_seq =")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-1",
                        1,
                        1,
                        "user",
                        "message",
                        "text",
                        "Previous turn.",
                        "{\"text\":\"Previous turn.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-2",
                        2,
                        2,
                        "user",
                        "message",
                        "text",
                        "Run the incomplete workflow.",
                        "{\"text\":\"Run the incomplete workflow.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-3",
                        3,
                        2,
                        "assistant",
                        "tool_call",
                        "function_call",
                        "",
                        "{\"name\":\"inspect\"}",
                        "inspect",
                        "call-inspect"
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-4",
                        4,
                        2,
                        "tool",
                        "tool_result",
                        "function_call_output",
                        "inspection output",
                        "{\"ok\":true}",
                        "inspect",
                        "call-inspect"
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq = 2")
            && query.contains("ORDER BY event_order ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-2",
                        2,
                        2,
                        "user",
                        "message",
                        "text",
                        "Run the incomplete workflow.",
                        "{\"text\":\"Run the incomplete workflow.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-3",
                        3,
                        2,
                        "assistant",
                        "tool_call",
                        "function_call",
                        "",
                        "{\"name\":\"inspect\"}",
                        "inspect",
                        "call-inspect"
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-4",
                        4,
                        2,
                        "tool",
                        "tool_result",
                        "function_call_output",
                        "inspection output",
                        "{\"ok\":true}",
                        "inspect",
                        "call-inspect"
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq < 2")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row(
                    "sess-incomplete",
                    1,
                    2,
                    1,
                    1,
                    0,
                    0,
                    0
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq > 2")
        {
            return (StatusCode::OK, json_each_row(json!([])));
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query.contains("ORDER BY turn_seq ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    turn_summary_row("sess-event", 1, 3, 1, 1, 0, 0, 0),
                    turn_summary_row("sess-event", 2, 1, 0, 1, 0, 0, 0)
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE event_uid = 'evt-open-full'")
            && query.contains("ORDER BY event_order DESC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row("sess-event", "evt-open-full", 2, 1, "assistant", "message", "text", "This is the full available event content that must not be clipped by the repository open model.", "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}", "", "")
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event' AND event_order = 2")
            && query.contains("event_uid = 'evt-open-full'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row("sess-event", "evt-open-full", 2, 1, "assistant", "message", "text", "This is the full available event content that must not be clipped by the repository open model.", "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}", "", "")
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event' AND turn_seq = 1")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row("sess-event", 1, 3, 1, 1, 0, 0, 0)])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query
                .contains("event_order < 2 OR (event_order = 2 AND event_uid < 'evt-open-full')")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([trace_event_row(
                    "sess-event",
                    "evt-event-1",
                    1,
                    1,
                    "user",
                    "message",
                    "text",
                    "question before full event",
                    "{\"text\":\"question before full event\"}",
                    "",
                    ""
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query
                .contains("event_order > 2 OR (event_order = 2 AND event_uid > 'evt-open-full')")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([trace_event_row(
                    "sess-event",
                    "evt-event-3",
                    3,
                    1,
                    "system",
                    "event_msg",
                    "task_complete",
                    "",
                    "{\"status\":\"complete\"}",
                    "",
                    ""
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event' AND turn_seq < 1")
        {
            return (StatusCode::OK, json_each_row(json!([])));
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event' AND turn_seq > 1")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row("sess-event", 2, 1, 0, 1, 0, 0, 0)])),
            );
        }

        if query.contains("FROM `moraine`.`search_documents`")
            && query.contains("WHERE event_uid = 'evt-open-full'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([{ "session_id": "sess-event" }])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-event",
                        "evt-event-1",
                        1,
                        1,
                        "user",
                        "message",
                        "text",
                        "question before full event",
                        "{\"text\":\"question before full event\"}",
                        "",
                        ""
                    ),
                    trace_event_row("sess-event", "evt-open-full", 2, 1, "assistant", "message", "text", "This is the full available event content that must not be clipped by the repository open model.", "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}", "", ""),
                    trace_event_row(
                        "sess-event",
                        "evt-event-3",
                        3,
                        1,
                        "system",
                        "event_msg",
                        "task_complete",
                        "",
                        "{\"status\":\"complete\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-event",
                        "evt-event-4",
                        4,
                        2,
                        "assistant",
                        "message",
                        "text",
                        "next turn",
                        "{\"text\":\"next turn\"}",
                        "",
                        ""
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess_c'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
        {
            if query.contains("event_order > 2 OR (event_order = 2 AND event_uid > 'evt-2')") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_c",
                            "event_uid": "evt-3",
                            "event_order": 3_u64,
                            "turn_seq": 2_u32,
                            "event_time": "2026-01-03 10:02:00",
                            "actor_role": "assistant",
                            "event_class": "message",
                            "payload_type": "text",
                            "call_id": "",
                            "name": "",
                            "phase": "",
                            "item_id": "itm-3",
                            "source_ref": "/tmp/sess_c.jsonl:1:3",
                            "text_content": "assistant answer",
                            "payload_json": "{\"text\":\"assistant answer\"}",
                            "token_usage_json": "{}"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "event_uid": "evt-1",
                        "event_order": 1_u64,
                        "turn_seq": 1_u32,
                        "event_time": "2026-01-03 10:00:00",
                        "actor_role": "user",
                        "event_class": "message",
                        "payload_type": "text",
                        "call_id": "",
                        "name": "",
                        "phase": "",
                        "item_id": "itm-1",
                        "source_ref": "/tmp/sess_c.jsonl:1:1",
                        "text_content": "user question",
                        "payload_json": "{\"text\":\"user question\"}",
                        "token_usage_json": "{}"
                    },
                    {
                        "session_id": "sess_c",
                        "event_uid": "evt-2",
                        "event_order": 2_u64,
                        "turn_seq": 1_u32,
                        "event_time": "2026-01-03 10:01:00",
                        "actor_role": "assistant",
                        "event_class": "reasoning",
                        "payload_type": "text",
                        "call_id": "",
                        "name": "",
                        "phase": "",
                        "item_id": "itm-2",
                        "source_ref": "/tmp/sess_c.jsonl:1:2",
                        "text_content": "assistant reasoning",
                        "payload_json": "{\"text\":\"assistant reasoning\"}",
                        "token_usage_json": "{}"
                    },
                    {
                        "session_id": "sess_c",
                        "event_uid": "evt-3",
                        "event_order": 3_u64,
                        "turn_seq": 2_u32,
                        "event_time": "2026-01-03 10:02:00",
                        "actor_role": "assistant",
                        "event_class": "message",
                        "payload_type": "text",
                        "call_id": "",
                        "name": "",
                        "phase": "",
                        "item_id": "itm-3",
                        "source_ref": "/tmp/sess_c.jsonl:1:3",
                        "text_content": "assistant answer",
                        "payload_json": "{\"text\":\"assistant answer\"}",
                        "token_usage_json": "{}"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess_c'")
            && query.contains("ORDER BY event_order DESC, event_uid DESC")
        {
            if query.contains("event_class = 'message'") {
                if query.contains("event_order < 3 OR (event_order = 3 AND event_uid < 'evt-3')") {
                    return (
                        StatusCode::OK,
                        json_each_row(json!([
                            {
                                "session_id": "sess_c",
                                "event_uid": "evt-1",
                                "event_order": 1_u64,
                                "turn_seq": 1_u32,
                                "event_time": "2026-01-03 10:00:00",
                                "actor_role": "user",
                                "event_class": "message",
                                "payload_type": "text",
                                "call_id": "",
                                "name": "",
                                "phase": "",
                                "item_id": "itm-1",
                                "source_ref": "/tmp/sess_c.jsonl:1:1",
                                "text_content": "user question",
                                "payload_json": "{\"text\":\"user question\"}",
                                "token_usage_json": "{}"
                            }
                        ])),
                    );
                }

                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_c",
                            "event_uid": "evt-3",
                            "event_order": 3_u64,
                            "turn_seq": 2_u32,
                            "event_time": "2026-01-03 10:02:00",
                            "actor_role": "assistant",
                            "event_class": "message",
                            "payload_type": "text",
                            "call_id": "",
                            "name": "",
                            "phase": "",
                            "item_id": "itm-3",
                            "source_ref": "/tmp/sess_c.jsonl:1:3",
                            "text_content": "assistant answer",
                            "payload_json": "{\"text\":\"assistant answer\"}",
                            "token_usage_json": "{}"
                        },
                        {
                            "session_id": "sess_c",
                            "event_uid": "evt-1",
                            "event_order": 1_u64,
                            "turn_seq": 1_u32,
                            "event_time": "2026-01-03 10:00:00",
                            "actor_role": "user",
                            "event_class": "message",
                            "payload_type": "text",
                            "call_id": "",
                            "name": "",
                            "phase": "",
                            "item_id": "itm-1",
                            "source_ref": "/tmp/sess_c.jsonl:1:1",
                            "text_content": "user question",
                            "payload_json": "{\"text\":\"user question\"}",
                            "token_usage_json": "{}"
                        }
                    ])),
                );
            }
        }

        (StatusCode::OK, json_each_row(json!([])))
    }

    let scripted_responses = (!options.scripted_responses.is_empty())
        .then(|| options.scripted_responses.iter().cloned().collect());
    let state = Arc::new(MockState {
        queries: Mutex::default(),
        options,
        scripted_responses: Mutex::new(scripted_responses),
    });
    let app = Router::new()
        .route("/", get(handler).post(handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("listener addr");

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{}", addr), state)
}

async fn build_repo_with_max_results(
    max_results: u16,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(max_results, MockOptions::default()).await
}

async fn build_repo_with_options(
    max_results: u16,
    options: MockOptions,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    let (base_url, state) = spawn_mock_server(options).await;
    let client =
        ClickHouseClient::new(test_clickhouse_config(base_url)).expect("valid clickhouse client");

    let repo = ClickHouseConversationRepository::new(
        client,
        RepoConfig {
            max_results,
            ..RepoConfig::default()
        },
    );

    (repo, state)
}

async fn build_scripted_repo(
    scripted_responses: Vec<ScriptedResponse>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            scripted_responses,
            ..MockOptions::default()
        },
    )
    .await
}

fn assert_script_consumed(state: &MockState, expected_requests: usize) {
    let queries = state.queries.lock().expect("queries lock");
    assert_eq!(
        queries.len(),
        expected_requests,
        "captured queries: {queries:#?}"
    );
    drop(queries);
    let scripted = state
        .scripted_responses
        .lock()
        .expect("scripted response lock");
    assert!(
        scripted
            .as_ref()
            .is_some_and(std::collections::VecDeque::is_empty),
        "unconsumed scripted responses remain"
    );
}

async fn build_repo() -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_max_results(100).await
}

/// Repository with a `--project-only` session origin scope configured.
async fn build_scoped_repo(roots: &[&str]) -> (ClickHouseConversationRepository, Arc<MockState>) {
    let (base_url, state) = spawn_mock_server(MockOptions::default()).await;
    let client =
        ClickHouseClient::new(test_clickhouse_config(base_url)).expect("valid clickhouse client");

    let repo = ClickHouseConversationRepository::new(
        client,
        RepoConfig {
            max_results: 100,
            session_scope: SessionOriginScope::from_roots(roots.iter().copied()),
            ..RepoConfig::default()
        },
    );

    (repo, state)
}

/// Regression helper for issue #253: ClickHouse 25.12's new analyzer treats
/// `any(column) AS column` as a nested aggregate because the inner `column`
/// binds to the alias expression. Returns true if the SQL contains that
/// buggy self-alias pattern for the given column (with word-boundary checks
/// on either side, so `t.column` prefixes and `column_raw` suffixes don't
/// trigger false positives).
fn sql_self_aliases_aggregate(sql: &str, column: &str) -> bool {
    let needle = format!("any({column}) AS {column}");
    sql.match_indices(&needle).any(|(idx, _)| {
        let head = sql[..idx].chars().next_back();
        let tail = sql[idx + needle.len()..].chars().next();
        let head_word =
            matches!(head, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.');
        let tail_word = matches!(tail, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_');
        !head_word && !tail_word
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_merges_normalized_exact_lookup_with_suffix_fallback() {
    let (repo, state) = build_repo().await;

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-normalized".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            derive_worktree_roots: true,
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        })
        .await
        .expect("file_attention query succeeds");

    assert_eq!(touches.len(), 2);
    assert_eq!(touches[0].session_id, "sess-normalized");
    assert_eq!(touches[0].event_uid, "evt-normalized");
    assert_eq!(touches[0].tool_call_id, "call-normalized");
    assert_eq!(touches[0].matched_path, "/worktree-a/crates/foo.rs");
    assert_eq!(touches[0].worktree_root, "/worktree-a");
    assert_eq!(touches[0].match_kind, "path_suffix");
    assert_eq!(touches[1].session_id, "sess-legacy");
    assert_eq!(touches[1].event_uid, "evt-legacy");
    assert_eq!(touches[1].match_kind, "shell_path");

    let queries = state.queries.lock().expect("queries lock").clone();
    let exact_query = queries
        .iter()
        .find(|query| query.contains("repo_rel_path = 'crates/foo.rs'"))
        .expect("normalized exact file_attention query should be captured");
    assert!(exact_query.contains("project_id = 'project-a'"));
    assert!(exact_query.contains("tool_phase = 'request'"));
    assert!(
        !exact_query.contains("JSONExtractString(input_json"),
        "exact lookup should not depend on legacy JSON suffix extraction: {exact_query}"
    );

    let fallback_query = queries
        .iter()
        .find(|query| query.contains("JSONExtractString(input_json, 'path')"))
        .expect("Tier-0 suffix file_attention query should be captured");
    assert!(fallback_query.contains("crates/foo.rs"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_conversations_applies_filters_and_cursor_pagination() {
    let (repo, state) = build_repo().await;

    let filter = ConversationListFilter {
        from_unix_ms: Some(1767261600000_i64),
        to_unix_ms: Some(1767500000000_i64),
        mode: Some(ConversationMode::WebSearch),
        sort: ConversationListSort::Desc,
    };

    let first = repo
        .list_conversations(
            filter.clone(),
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("first page");

    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].session_id, "sess_c");
    assert_eq!(first.items[1].session_id, "sess_b");
    assert_eq!(first.items[0].session_slug.as_deref(), Some("project-c"));
    assert_eq!(
        first.items[0].session_summary.as_deref(),
        Some("Session C summary")
    );
    assert!(first.next_cursor.is_some());

    let second = repo
        .list_conversations(
            filter,
            PageRequest {
                limit: 2,
                cursor: first.next_cursor,
            },
        )
        .await
        .expect("second page");

    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].session_id, "sess_a");
    assert!(second.next_cursor.is_none());

    let queries = state.queries.lock().expect("queries lock").clone();
    let list_query = queries
        .iter()
        .find(|q| {
            q.contains("FROM `moraine`.`v_session_summary` AS s")
                && q.contains("ORDER BY s.last_event_time")
        })
        .expect("list query should be captured");

    assert!(list_query.contains("ifNull(m.mode, 'chat') = 'web_search'"));
    assert!(list_query.contains("JSONExtractString(payload_json, 'summary')"));
    assert!(list_query.contains("s.session_id AS session_id"));
    assert!(list_query.contains("AS session_slug"));
    assert!(list_query.contains("toUnixTimestamp64Milli(s.last_event_time) >= 1767261600000"));
    assert!(list_query.contains("toUnixTimestamp64Milli(s.last_event_time) < 1767500000000"));
    assert!(list_query.contains("ORDER BY s.last_event_time DESC, s.session_id DESC"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_conversations_supports_ascending_sort_with_deterministic_cursor() {
    let (repo, state) = build_repo().await;

    let filter = ConversationListFilter {
        from_unix_ms: Some(1767261600000_i64),
        to_unix_ms: Some(1767500000000_i64),
        mode: Some(ConversationMode::WebSearch),
        sort: ConversationListSort::Asc,
    };

    let first = repo
        .list_conversations(
            filter.clone(),
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("first page");

    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].session_id, "sess_a");
    assert_eq!(first.items[1].session_id, "sess_b");
    assert!(first.next_cursor.is_some());

    let second = repo
        .list_conversations(
            filter,
            PageRequest {
                limit: 2,
                cursor: first.next_cursor,
            },
        )
        .await
        .expect("second page");

    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].session_id, "sess_c");
    assert!(second.next_cursor.is_none());

    let queries = state.queries.lock().expect("queries lock").clone();
    let first_query = queries
        .iter()
        .find(|q| q.contains("ORDER BY s.last_event_time ASC, s.session_id ASC"))
        .expect("ascending list query should be captured");
    assert!(first_query.contains("ifNull(m.mode, 'chat') = 'web_search'"));

    let paged_query = queries
        .iter()
        .find(|q| q.contains("s.session_id > 'sess_b'"))
        .expect("ascending pagination query should include deterministic cursor predicate");
    assert!(paged_query.contains("toUnixTimestamp64Milli(s.last_event_time) > 1767348600000"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_conversations_rejects_cursor_when_sort_changes() {
    let (repo, _state) = build_repo().await;

    let desc_filter = ConversationListFilter {
        from_unix_ms: Some(1767261600000_i64),
        to_unix_ms: Some(1767500000000_i64),
        mode: Some(ConversationMode::WebSearch),
        sort: ConversationListSort::Desc,
    };
    let asc_filter = ConversationListFilter {
        sort: ConversationListSort::Asc,
        ..desc_filter.clone()
    };

    let first = repo
        .list_conversations(
            desc_filter,
            PageRequest {
                limit: 1,
                cursor: None,
            },
        )
        .await
        .expect("first page");
    let cursor = first.next_cursor.expect("next cursor");

    let err = repo
        .list_conversations(
            asc_filter,
            PageRequest {
                limit: 1,
                cursor: Some(cursor),
            },
        )
        .await
        .expect_err("sort mismatch should fail");

    assert_eq!(
        err.to_string(),
        "invalid cursor: cursor does not match current conversation filter"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_uses_overlap_filter_and_cursor_pagination() {
    let (repo, state) = build_repo().await;

    let filter = McpSessionListFilter {
        start_unix_ms: 1767261600000_i64,
        end_unix_ms: 1767500000000_i64,
        mode: Some(ConversationMode::WebSearch),
        sort: ConversationListSort::Desc,
    };

    let first = repo
        .list_mcp_sessions(
            filter.clone(),
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("first page");

    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].session_id, "sess_c");
    assert_eq!(first.items[0].title.as_deref(), Some("Session C title"));
    assert_eq!(first.items[0].source.as_deref(), Some("codex"));
    assert!(first.items[0].completed);
    assert_eq!(first.items[1].session_id, "sess_b");
    assert!(first.next_cursor.is_some());

    let second = repo
        .list_mcp_sessions(
            filter,
            PageRequest {
                limit: 2,
                cursor: first.next_cursor,
            },
        )
        .await
        .expect("second page");

    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].session_id, "sess_a");
    assert!(!second.items[0].completed);
    assert!(second.next_cursor.is_none());

    let queries = state.queries.lock().expect("queries lock").clone();
    let list_query = queries
        .iter()
        .find(|q| q.contains("AS completed") && q.contains("latest_terminal_payload_type"))
        .expect("list_sessions query should be captured");

    assert!(list_query.contains("toUnixTimestamp64Milli(s.last_event_time) >= 1767261600000"));
    assert!(list_query.contains("toUnixTimestamp64Milli(s.first_event_time) < 1767500000000"));
    assert!(list_query.contains("ifNull(m.mode, 'chat') = 'web_search'"));
    assert!(list_query.contains("ORDER BY s.last_event_time DESC, s.session_id DESC"));
    assert!(list_query.contains("payload_type IN ('task_complete', 'turn_aborted')"));
    // Blank session_id rows are filtered at the source so they never consume a
    // LIMIT slot or anchor the keyset cursor (#386).
    assert!(list_query.contains("notEmpty(trimBoth(s.session_id))"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_rejects_cursor_filter_mismatch() {
    let (repo, _state) = build_repo().await;

    let base_filter = McpSessionListFilter {
        start_unix_ms: 1767261600000_i64,
        end_unix_ms: 1767500000000_i64,
        mode: Some(ConversationMode::WebSearch),
        sort: ConversationListSort::Desc,
    };

    let first = repo
        .list_mcp_sessions(
            base_filter.clone(),
            PageRequest {
                limit: 1,
                cursor: None,
            },
        )
        .await
        .expect("first page");
    let cursor = first.next_cursor.expect("next cursor");

    let err = repo
        .list_mcp_sessions(
            McpSessionListFilter {
                mode: Some(ConversationMode::Chat),
                ..base_filter
            },
            PageRequest {
                limit: 1,
                cursor: Some(cursor),
            },
        )
        .await
        .expect_err("filter mismatch should fail");

    assert_eq!(
        err.to_string(),
        "invalid cursor: cursor does not match current list_sessions filter"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_applies_session_origin_scope() {
    let (repo, state) = build_scoped_repo(&["/work/project"]).await;

    repo.list_mcp_sessions(
        McpSessionListFilter {
            start_unix_ms: 1767261600000_i64,
            end_unix_ms: 1767500000000_i64,
            mode: None,
            sort: ConversationListSort::Desc,
        },
        PageRequest {
            limit: 5,
            cursor: None,
        },
    )
    .await
    .expect("scoped list_mcp_sessions");

    let queries = state.queries.lock().expect("queries lock").clone();
    let list_query = queries
        .iter()
        .find(|q| q.contains("AS completed") && q.contains("latest_terminal_payload_type"))
        .expect("list_sessions query should be captured");

    assert!(list_query.contains("s.session_id IN (SELECT session_id FROM ("));
    assert!(list_query.contains("argMin(cwd, tuple(event_ts, event_uid)) AS origin_cwd"));
    assert!(list_query.contains("WHERE cwd != ''"));
    assert!(list_query.contains("origin_cwd = '/work/project'"));
    assert!(list_query.contains("startsWith(origin_cwd, '/work/project/')"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_rejects_cursor_from_differently_scoped_server() {
    let (unscoped_repo, _state) = build_repo().await;
    let (scoped_repo, _scoped_state) = build_scoped_repo(&["/work/project"]).await;

    let filter = McpSessionListFilter {
        start_unix_ms: 1767261600000_i64,
        end_unix_ms: 1767500000000_i64,
        mode: Some(ConversationMode::WebSearch),
        sort: ConversationListSort::Desc,
    };

    let first = unscoped_repo
        .list_mcp_sessions(
            filter.clone(),
            PageRequest {
                limit: 1,
                cursor: None,
            },
        )
        .await
        .expect("first page from unscoped repo");
    let cursor = first.next_cursor.expect("next cursor");

    let err = scoped_repo
        .list_mcp_sessions(
            filter,
            PageRequest {
                limit: 1,
                cursor: Some(cursor),
            },
        )
        .await
        .expect_err("cursor minted without the scope must be rejected");

    assert_eq!(
        err.to_string(),
        "invalid cursor: cursor does not match current list_sessions filter"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_applies_session_origin_scope() {
    let (repo, state) = build_scoped_repo(&["/work/project"]).await;

    repo.search_mcp_events(SearchMcpEventsQuery {
        query: "hello world".to_string(),
        n_hits: Some(10),
        event_types: Some(vec![
            McpEventType::UserInput,
            McpEventType::AssistantResponse,
        ]),
        min_score: Some(0.0),
        min_should_match: Some(1),
        ..SearchMcpEventsQuery::default()
    })
    .await
    .expect("scoped search_mcp_events");

    let queries = state.queries.lock().expect("queries lock").clone();
    let search_query = queries
        .iter()
        .find(|q| q.contains("AS mcp_event_type") && q.contains("AS raw_score"))
        .expect("search query should be captured");

    assert!(search_query.contains("d.session_id IN (SELECT session_id FROM ("));
    assert!(search_query.contains("origin_cwd = '/work/project'"));
    assert!(search_query.contains("startsWith(origin_cwd, '/work/project/')"));
}

#[tokio::test(flavor = "multi_thread")]
async fn scoped_point_lookups_hide_out_of_scope_sessions() {
    let (repo, state) = build_scoped_repo(&["/work/project"]).await;

    let metadata = repo
        .get_session_metadata("sess-out-of-scope")
        .await
        .expect("metadata query succeeds");
    assert!(metadata.is_none(), "out-of-scope metadata must be hidden");

    let session = repo
        .get_mcp_session("sess-out-of-scope")
        .await
        .expect("session query succeeds");
    assert!(session.is_none(), "out-of-scope session must be hidden");

    let turn = repo
        .get_mcp_turn("sess-out-of-scope", 1)
        .await
        .expect("turn query succeeds");
    assert!(turn.is_none(), "out-of-scope turn must be hidden");

    let event = repo
        .get_mcp_event("evt-out-of-scope")
        .await
        .expect("event query succeeds");
    assert!(event.is_none(), "out-of-scope event must be hidden");

    let queries = state.queries.lock().expect("queries lock").clone();
    let gate_queries: Vec<&String> = queries
        .iter()
        .filter(|q| {
            q.starts_with("SELECT session_id FROM (")
                && q.contains("argMin(cwd, tuple(event_ts, event_uid))")
        })
        .collect();
    assert!(
        gate_queries.len() >= 4,
        "each point lookup should run the scope gate, saw {}",
        gate_queries.len()
    );
    assert!(gate_queries
        .iter()
        .any(|q| q.contains("session_id = 'sess-out-of-scope'")));
}

#[tokio::test(flavor = "multi_thread")]
async fn scoped_point_lookups_serve_in_scope_sessions() {
    let (repo, state) = build_scoped_repo(&["/work/project"]).await;

    let session = repo
        .get_mcp_session("sess-open")
        .await
        .expect("session query succeeds")
        .expect("in-scope session is served");
    assert_eq!(session.metadata.session_id, "sess-open");

    // The positive result is cached: a second lookup must not re-run the gate.
    let _ = repo
        .get_session_metadata("sess-open")
        .await
        .expect("metadata query succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let gate_queries: Vec<&String> = queries
        .iter()
        .filter(|q| {
            q.starts_with("SELECT session_id FROM (") && q.contains("session_id = 'sess-open'")
        })
        .collect();
    assert_eq!(
        gate_queries.len(),
        1,
        "in-scope verdicts should be cached after the first gate query"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_returns_stable_summary_fields() {
    let (repo, state) = build_repo().await;

    let metadata = repo
        .get_session_metadata("sess_c")
        .await
        .expect("session metadata query succeeds")
        .expect("session metadata exists");

    assert_eq!(metadata.session_id, "sess_c");
    assert_eq!(metadata.first_event_time, "2026-01-03 10:00:00");
    assert_eq!(metadata.last_event_time, "2026-01-03 10:10:00");
    assert_eq!(metadata.total_events, 30);
    assert_eq!(metadata.total_turns, 3);
    assert_eq!(metadata.user_messages, 6);
    assert_eq!(metadata.assistant_messages, 6);
    assert_eq!(metadata.first_event_uid, "evt-c-1");
    assert_eq!(metadata.last_event_uid, "evt-c-42");
    assert_eq!(metadata.last_actor_role, "assistant");
    assert_eq!(metadata.mode, ConversationMode::WebSearch);

    let queries = state.queries.lock().expect("queries lock").clone();
    let metadata_query = queries
        .iter()
        .find(|q| q.contains("argMin(event_uid, tuple(event_time, event_order, event_uid))"))
        .expect("session metadata query should be captured");
    assert!(
        metadata_query.contains("argMax(actor_role, tuple(event_time, event_order, event_uid))")
    );
    assert!(metadata_query.contains("WHERE s.session_id = 'sess_c'"));
    // Regression: event_order exists only in v_conversation_trace, never in
    // moraine.events. The mode subquery legitimately reads from events, so
    // scope this check to the argMin/argMax subquery by asserting the
    // v_conversation_trace table is immediately above the event_uid argMin.
    let metadata_subquery_slice = metadata_query
        .split_once("argMin(event_uid, tuple(event_time")
        .and_then(|(head, _)| head.rsplit_once("SELECT"))
        .map(|(_, tail)| tail)
        .expect("metadata subquery head should be present");
    assert!(
        !metadata_subquery_slice.contains("FROM `moraine`.`events`"),
        "argMin/argMax subquery must read from v_conversation_trace, not events: {metadata_query}",
    );
    assert!(
        metadata_query.contains("FROM `moraine`.`v_conversation_trace`"),
        "metadata subquery must read event_order from v_conversation_trace: {metadata_query}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_returns_none_for_missing_session() {
    let (repo, _state) = build_repo().await;

    let metadata = repo
        .get_session_metadata("sess-missing")
        .await
        .expect("session metadata query succeeds");
    assert!(metadata.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_rejects_invalid_session_id() {
    let (repo, _state) = build_repo().await;

    let err = repo
        .get_session_metadata("sess bad")
        .await
        .expect_err("invalid session_id should fail");
    assert!(matches!(err, RepoError::InvalidArgument(_)));
    assert_eq!(
        err.to_string(),
        "invalid argument: session_id contains unsupported characters"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_keeps_empty_boundary_fields_when_summary_exists() {
    let (repo, _state) = build_repo().await;

    let metadata = repo
        .get_session_metadata("sess-empty")
        .await
        .expect("session metadata query succeeds")
        .expect("session metadata exists");

    assert_eq!(metadata.session_id, "sess-empty");
    assert_eq!(metadata.mode, ConversationMode::Chat);
    assert!(metadata.first_event_uid.is_empty());
    assert!(metadata.last_event_uid.is_empty());
    assert!(metadata.last_actor_role.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_includes_turn_summaries_and_latest_completion() {
    let (repo, state) = build_repo().await;

    let session = repo
        .get_mcp_session("sess-open")
        .await
        .expect("mcp session open succeeds")
        .expect("mcp session exists");

    assert_eq!(session.metadata.session_id, "sess-open");
    assert_eq!(session.metadata.mode, ConversationMode::ToolCalling);
    assert_eq!(session.title.as_deref(), Some("Open model session"));
    assert_eq!(session.source.as_deref(), Some("codex-source"));
    assert_eq!(session.session_slug.as_deref(), Some("open-model-session"));
    assert_eq!(session.turns.len(), 2);
    assert!(session.completed);
    assert_eq!(session.terminal_event_uid.as_deref(), Some("evt-open-8"));

    let first_turn = &session.turns[0];
    assert_eq!(first_turn.metadata.turn_seq, 1);
    assert!(first_turn.completed);
    assert_eq!(first_turn.terminal_event_uid.as_deref(), Some("evt-open-5"));
    assert_eq!(
        first_turn.user_input_summary.as_deref(),
        Some("How should repository open models work?")
    );
    assert_eq!(
        first_turn.final_response_summary.as_deref(),
        Some("First answer with repository context.")
    );
    assert_eq!(first_turn.tools_called, vec!["search_repo"]);
    assert_eq!(
        first_turn.normalized_event_types,
        vec![
            "user_input",
            "tool_call",
            "tool_response",
            "assistant_response",
            "runtime"
        ]
    );
    assert_eq!(
        first_turn
            .first_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-open-1")
    );
    assert_eq!(
        first_turn
            .last_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-open-5")
    );

    let queries = state.queries.lock().expect("queries lock").clone();
    assert!(queries.iter().any(|query| {
        query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_turn_returns_compact_events_and_incomplete_state() {
    let (repo, _state) = build_repo().await;

    let turn = repo
        .get_mcp_turn("sess-incomplete", 2)
        .await
        .expect("mcp turn open succeeds")
        .expect("mcp turn exists");

    assert_eq!(turn.metadata.session_id, "sess-incomplete");
    assert_eq!(turn.metadata.turn_seq, 2);
    assert_eq!(turn.events.len(), 3);
    assert_eq!(turn.events[0].event_uid, "evt-inc-2");
    assert_eq!(turn.events[0].event_type, "user_input");
    assert_eq!(
        turn.events[0].text_preview.as_deref(),
        Some("Run the incomplete workflow.")
    );
    assert_eq!(turn.events[1].event_type, "tool_call");
    assert_eq!(turn.events[2].event_type, "tool_response");
    assert_eq!(
        turn.user_input_summary.as_deref(),
        Some("Run the incomplete workflow.")
    );
    assert!(turn.final_response_summary.is_none());
    assert_eq!(turn.tools_called, vec!["inspect"]);
    assert_eq!(
        turn.normalized_event_types,
        vec!["user_input", "tool_call", "tool_response"]
    );
    assert!(!turn.completed);
    assert!(turn.terminal_event_uid.is_none());
    assert_eq!(
        turn.previous_turn.as_ref().map(|turn| turn.turn_seq),
        Some(1)
    );
    assert!(turn.next_turn.is_none());
    assert_eq!(
        turn.first_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-inc-2")
    );
    assert_eq!(
        turn.last_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-inc-4")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_event_returns_full_content_and_navigation_refs() {
    let (repo, _state) = build_repo().await;

    let event = repo
        .get_mcp_event("evt-open-full")
        .await
        .expect("mcp event open succeeds")
        .expect("mcp event exists");

    assert_eq!(event.event.event_uid, "evt-open-full");
    assert_eq!(event.event_type, "assistant_response");
    assert_eq!(event.event.session_id, "sess-event");
    assert_eq!(event.event.turn_seq, 1);
    assert_eq!(
        event.event.text_content,
        "This is the full available event content that must not be clipped by the repository open model."
    );
    assert_eq!(
        event.event.payload_json,
        "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}"
    );
    assert_eq!(event.parent_session.session_id, "sess-event");
    assert_eq!(event.parent_turn.turn_seq, 1);
    assert_eq!(
        event
            .previous_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-event-1")
    );
    assert_eq!(
        event
            .next_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-event-3")
    );
    assert!(event.previous_turn.is_none());
    assert_eq!(event.next_turn.as_ref().map(|turn| turn.turn_seq), Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_session_metadata_returns_summary_only_matches() {
    let (repo, _state) = build_repo().await;

    let result = repo
        .search_session_metadata(SessionMetadataSearchQuery {
            query: "rare summary".to_string(),
            limit: Some(10),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: None,
            to_unix_ms: None,
            mode: None,
            session_id: None,
        })
        .await
        .expect("search session metadata");

    assert_eq!(result.query, "rare summary");
    assert_eq!(result.terms, vec!["rare", "summary"]);
    assert_eq!(result.stats.requested_limit, 10);
    assert_eq!(result.stats.effective_limit, 10);
    assert!(!result.stats.limit_capped);
    assert_eq!(result.stats.result_count, 1);

    let hit = &result.hits[0];
    assert_eq!(hit.rank, 1);
    assert_eq!(hit.session_id, "sess_meta_summary");
    assert_eq!(hit.first_event_time.as_deref(), Some("2026-01-05 10:00:00"));
    assert_eq!(hit.first_event_unix_ms, Some(1767607200000_i64));
    assert_eq!(hit.last_event_time.as_deref(), Some("2026-01-05 10:15:00"));
    assert_eq!(hit.last_event_unix_ms, Some(1767608100000_i64));
    assert_eq!(hit.total_turns, Some(4));
    assert_eq!(hit.total_events, Some(18));
    assert_eq!(hit.user_messages, Some(5));
    assert_eq!(hit.assistant_messages, Some(5));
    assert_eq!(hit.tool_calls, Some(1));
    assert_eq!(hit.tool_results, Some(1));
    assert_eq!(hit.mode, Some(ConversationMode::Chat));
    assert_eq!(hit.harness.as_deref(), Some("codex"));
    assert_eq!(hit.inference_provider.as_deref(), Some("openai"));
    assert_eq!(hit.session_slug.as_deref(), Some("rare-summary-session"));
    assert_eq!(
        hit.session_summary.as_deref(),
        Some("Rare summary-only session about metadata discovery.")
    );
    assert_eq!(hit.meta_event_uid.as_deref(), Some("meta-rare-1"));
    assert_eq!(hit.score, 5.0);
    assert_eq!(hit.matched_terms, 2);
    assert_eq!(
        hit.snippet.as_deref(),
        Some("Rare summary-only session about metadata discovery.")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn search_session_metadata_applies_time_mode_filters_and_caps_limit() {
    let (repo, state) = build_repo_with_max_results(5).await;

    let result = repo
        .search_session_metadata(SessionMetadataSearchQuery {
            query: "rare summary".to_string(),
            limit: Some(25),
            min_score: Some(1.5),
            min_should_match: Some(2),
            from_unix_ms: Some(1767600000000_i64),
            to_unix_ms: Some(1767610000000_i64),
            mode: Some(ConversationMode::Chat),
            session_id: Some("sess_meta_summary".to_string()),
        })
        .await
        .expect("search session metadata");

    assert_eq!(result.stats.requested_limit, 25);
    assert_eq!(result.stats.effective_limit, 5);
    assert!(result.stats.limit_capped);

    let queries = state.queries.lock().expect("queries lock").clone();
    let metadata_search_query = queries
        .iter()
        .find(|q| {
            q.contains("FROM (SELECT * FROM `moraine`.`events` FINAL) AS e")
                && q.contains("WHERE e.event_kind = 'session_meta'")
                && q.contains("AS meta_event_uid")
        })
        .expect("session metadata search query should be captured");

    assert!(metadata_search_query.contains("meta.matched_terms >= 2"));
    assert!(metadata_search_query.contains("meta.score >= 1.500000"));
    assert!(!metadata_search_query.contains("search_postings"));
    assert!(metadata_search_query
        .contains("toUnixTimestamp64Milli(s.last_event_time) >= 1767600000000"));
    assert!(
        metadata_search_query.contains("toUnixTimestamp64Milli(s.last_event_time) < 1767610000000")
    );
    assert!(metadata_search_query.contains("ifNull(m.mode, 'chat') = 'chat'"));
    assert!(metadata_search_query.contains("meta.session_id = 'sess_meta_summary'"));
    assert!(metadata_search_query.contains("LIMIT 5"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_returns_ranked_session_hits_and_expected_sql_shape() {
    let (repo, state) = build_repo().await;

    let result = repo
        .search_conversations(ConversationSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: Some(1767261600000_i64),
            to_unix_ms: Some(1767500000000_i64),
            mode: Some(ConversationMode::Chat),
            include_tool_events: Some(true),
            exclude_codex_mcp: Some(false),
        })
        .await
        .expect("search conversations");

    assert_eq!(result.hits.len(), 2);
    assert_eq!(result.hits[0].session_id, "sess_c");
    assert_eq!(
        result.hits[0].first_event_time.as_deref(),
        Some("2026-01-03 10:00:00")
    );
    assert_eq!(result.hits[0].first_event_unix_ms, Some(1767434400000_i64));
    assert_eq!(
        result.hits[0].last_event_time.as_deref(),
        Some("2026-01-03 10:10:00")
    );
    assert_eq!(result.hits[0].last_event_unix_ms, Some(1767435000000_i64));
    assert_eq!(result.hits[0].harness.as_deref(), Some("codex"));
    assert_eq!(result.hits[0].session_slug.as_deref(), Some("project-c"));
    assert_eq!(
        result.hits[0].session_summary.as_deref(),
        Some("Session C summary")
    );
    assert_eq!(result.hits[0].best_event_uid.as_deref(), Some("evt-c-42"));
    assert_eq!(
        result.hits[0].text_preview.as_deref(),
        Some("best match from session c")
    );
    assert_eq!(
        result.hits[0].text_content.as_deref(),
        Some("best match from session c with extra context")
    );
    assert_eq!(
        result.hits[0].payload_json.as_deref(),
        Some("{\"type\":\"message\",\"topic\":\"session-c\"}")
    );
    assert_eq!(result.hits[1].session_id, "sess_a");
    assert_eq!(
        result.hits[1].first_event_time.as_deref(),
        Some("2026-01-01 10:00:00")
    );
    assert_eq!(result.hits[1].harness.as_deref(), Some("codex"));
    assert_eq!(result.hits[1].session_slug, None);
    assert_eq!(result.hits[1].session_summary, None);
    assert_eq!(
        result.hits[1].text_content.as_deref(),
        Some("weaker match from session a with extra context")
    );
    assert_eq!(result.stats.requested_limit, 10);
    assert_eq!(result.stats.effective_limit, 10);
    assert!(!result.stats.limit_capped);

    let queries = state.queries.lock().expect("queries lock").clone();
    let agg_query = queries
        .iter()
        .find(|q| q.contains("GROUP BY e.session_id"))
        .expect("aggregated conversation query should be captured");

    assert!(agg_query.contains("argMax(e.event_uid, e.event_score)"));
    assert!(agg_query.contains("ANY LEFT JOIN `moraine`.`v_session_summary` AS s"));
    assert!(agg_query.contains("p.session_id IN ['sess_c','sess_a']"));
    assert!(agg_query.contains("ifNull(m.mode, 'chat') = 'chat'"));
    assert!(agg_query.contains("toUnixTimestamp64Milli(d.ingested_at) >= 1767261600000"));
    assert!(agg_query.contains("toUnixTimestamp64Milli(d.ingested_at) < 1767500000000"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_reports_capped_limit_metadata() {
    let (repo, _state) = build_repo_with_max_results(25).await;

    let result = repo
        .search_conversations(ConversationSearchQuery {
            query: "hello world".to_string(),
            limit: Some(100),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: None,
            to_unix_ms: None,
            mode: None,
            include_tool_events: Some(true),
            exclude_codex_mcp: Some(false),
        })
        .await
        .expect("search conversations");

    assert_eq!(result.stats.requested_limit, 100);
    assert_eq!(result.stats.effective_limit, 25);
    assert!(result.stats.limit_capped);
}

#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_falls_back_to_row_snippet_for_text_preview() {
    let (repo, _state) = build_repo_with_options(
        100,
        MockOptions {
            omit_second_snippet_row: true,
            ..MockOptions::default()
        },
    )
    .await;

    let result = repo
        .search_conversations(ConversationSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: Some(1767261600000_i64),
            to_unix_ms: Some(1767500000000_i64),
            mode: Some(ConversationMode::Chat),
            include_tool_events: Some(true),
            exclude_codex_mcp: Some(false),
        })
        .await
        .expect("search conversations");

    assert_eq!(
        result.hits[1].snippet.as_deref(),
        Some("weaker match from session a")
    );
    assert_eq!(
        result.hits[1].text_preview.as_deref(),
        Some("weaker match from session a")
    );
    assert!(result.hits[1].text_content.is_none());
    assert!(result.hits[1].payload_json.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_snippet_query_avoids_self_aliased_aggregates() {
    let (repo, state) = build_repo().await;

    let _ = repo
        .search_conversations(ConversationSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: Some(1767261600000_i64),
            to_unix_ms: Some(1767500000000_i64),
            mode: Some(ConversationMode::Chat),
            include_tool_events: Some(true),
            exclude_codex_mcp: Some(false),
        })
        .await
        .expect("search conversations");

    let queries = state.queries.lock().expect("queries lock").clone();
    let snippet_query = queries
        .iter()
        .find(|q| {
            q.contains("WHERE event_uid IN")
                && q.contains("GROUP BY event_uid")
                && q.contains("AS text_content")
        })
        .expect("snippet hydration query should be captured");

    // Regression for issue #253: aliasing `any(text_content) AS text_content`
    // in the same SELECT makes the ClickHouse 25.12 analyzer resolve
    // `text_content` to the alias expression, producing nested aggregates
    // (ILLEGAL_AGGREGATION). Keep aggregate and output alias names disjoint.
    for column in ["text_content", "payload_json", "event_class", "actor_role"] {
        assert!(
            !sql_self_aliases_aggregate(snippet_query, column),
            "snippet query must not self-alias `any({column}) AS {column}`: {snippet_query}",
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_without_mode_filter_skips_mode_join() {
    let (repo, state) = build_repo().await;

    let _ = repo
        .search_conversations(ConversationSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: Some(1767261600000_i64),
            to_unix_ms: Some(1767500000000_i64),
            mode: None,
            include_tool_events: Some(true),
            exclude_codex_mcp: Some(false),
        })
        .await
        .expect("search conversations");

    let queries = state.queries.lock().expect("queries lock").clone();
    let agg_query = queries
        .iter()
        .find(|q| q.contains("GROUP BY e.session_id"))
        .expect("aggregated conversation query should be captured");

    assert!(!agg_query.contains("ANY LEFT JOIN ("));
    assert!(!agg_query.contains("ifNull(m.mode"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_without_time_window_uses_postings_only_fast_path() {
    let (repo, state) = build_repo().await;

    let _ = repo
        .search_conversations(ConversationSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            min_score: Some(0.0),
            min_should_match: Some(1),
            from_unix_ms: None,
            to_unix_ms: None,
            mode: None,
            include_tool_events: Some(true),
            exclude_codex_mcp: Some(false),
        })
        .await
        .expect("search conversations");

    let queries = state.queries.lock().expect("queries lock").clone();
    let agg_query = queries
        .iter()
        .find(|q| q.contains("GROUP BY e.session_id"))
        .expect("aggregated conversation query should be captured");

    assert!(agg_query.contains("PREWHERE"));
    assert!(agg_query.contains("bitCount(groupBitOr(e.term_mask))"));
    assert!(!agg_query.contains("JOIN `moraine`.`search_documents` AS d"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_events_includes_session_time_bounds() {
    let (repo, _state) = build_repo().await;

    let result = repo
        .search_events(SearchEventsQuery {
            query: "hello world".to_string(),
            source: Some("integration-test".to_string()),
            limit: Some(10),
            session_id: None,
            session_ids: None,
            min_score: Some(0.0),
            min_should_match: Some(1),
            include_tool_events: Some(true),
            event_kinds: None,
            exclude_codex_mcp: Some(false),
            bypass_cache: Some(true),
            strategy_hint: None,
        })
        .await
        .expect("search events");

    assert_eq!(result.hits.len(), 2);
    assert_eq!(result.hits[0].session_id, "sess_c");
    assert_eq!(result.hits[0].first_event_time, "2026-01-03 10:00:00");
    assert_eq!(result.hits[0].last_event_time, "2026-01-03 10:10:00");
    assert_eq!(
        result.hits[0].text_content.as_deref(),
        Some("best event in session c with extra context")
    );
    assert_eq!(
        result.hits[0].payload_json.as_deref(),
        Some("{\"type\":\"message\",\"topic\":\"session-c\"}")
    );
    assert_eq!(result.hits[1].session_id, "sess_a");
    assert_eq!(result.hits[1].first_event_time, "2026-01-01 10:00:00");
    assert_eq!(result.hits[1].last_event_time, "2026-01-01 10:10:00");
}

#[tokio::test(flavor = "multi_thread")]
async fn search_events_documents_subquery_avoids_self_aliased_aggregates() {
    let (repo, state) = build_repo().await;

    let _ = repo
        .search_events(SearchEventsQuery {
            query: "hello world".to_string(),
            source: Some("integration-test".to_string()),
            limit: Some(10),
            session_id: None,
            session_ids: None,
            min_score: Some(0.0),
            min_should_match: Some(1),
            include_tool_events: Some(true),
            event_kinds: None,
            exclude_codex_mcp: Some(false),
            bypass_cache: Some(true),
            strategy_hint: None,
        })
        .await
        .expect("search events");

    let queries = state.queries.lock().expect("queries lock").clone();
    // Defensive coverage for the same class of bug as issue #253: the
    // `documents_join_sql` and `documents_source_sql` inner subqueries used
    // to self-alias aggregates (`any(text_content) AS text_content`), which
    // ClickHouse 25.12's analyzer rejects as nested aggregates. They now
    // qualify inner column references via an `AS t` table alias.
    let documents_subqueries: Vec<&String> = queries
        .iter()
        .filter(|q| q.contains("GROUP BY p.doc_id") || q.contains("FROM (SELECT\n  t.event_uid"))
        .collect();
    assert!(
        !documents_subqueries.is_empty(),
        "expected at least one search_events query to be captured; got {queries:?}",
    );
    for query in documents_subqueries {
        for column in [
            "session_id",
            "source_name",
            "harness",
            "inference_provider",
            "event_class",
            "payload_type",
            "actor_role",
            "name",
            "phase",
            "source_ref",
            "doc_len",
            "text_content",
            "payload_json",
            "has_codex_mcp",
        ] {
            assert!(
                !sql_self_aliases_aggregate(query, column),
                "search_events query must not self-alias `any({column}) AS {column}`: {query}",
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_global_search_with_enriched_hits() {
    let (repo, _state) = build_repo().await;

    let result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(10),
            event_types: Some(vec![
                McpEventType::ToolResponse,
                McpEventType::UserInput,
                McpEventType::AssistantResponse,
            ]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("search mcp events");

    assert_eq!(result.hits.len(), 2);
    assert!(!result.truncated);
    assert_eq!(
        result.event_types,
        vec![
            McpEventType::UserInput,
            McpEventType::AssistantResponse,
            McpEventType::ToolResponse
        ]
    );
    assert_eq!(result.hits[0].event_uid, "evt-c-42");
    assert_eq!(result.hits[0].event_type, McpEventType::AssistantResponse);
    assert_eq!(result.hits[0].session_id, "sess_c");
    assert_eq!(
        result.hits[0].session_title.as_deref(),
        Some("Session C summary")
    );
    assert_eq!(result.hits[0].source_name.as_deref(), Some("codex"));
    assert_eq!(result.hits[0].event_time, "2026-01-03 10:02:00");
    assert_eq!(result.hits[0].event_order, 42);
    assert_eq!(result.hits[0].raw_score, 12.5);
    assert_eq!(result.hits[0].model.as_deref(), Some("gpt-5.3-codex"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_session_scoped_search() {
    let (repo, state) = build_repo().await;

    let result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            session_id: Some("sess_a".to_string()),
            event_types: Some(vec![McpEventType::AssistantResponse]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("session-scoped mcp event search");

    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].session_id, "sess_a");
    assert_eq!(result.hits[0].event_uid, "evt-a-11");

    let queries = state.queries.lock().expect("queries lock").clone();
    let search_query = queries
        .iter()
        .find(|q| q.contains("AS mcp_event_type") && q.contains("d.session_id = 'sess_a'"))
        .expect("session-scoped search query should be captured");
    assert!(search_query.contains("d.session_id = 'sess_a'"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_turn_scoped_search() {
    let (repo, state) = build_repo().await;

    let result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "cargo failure".to_string(),
            n_hits: Some(5),
            session_id: Some("sess_c".to_string()),
            turn_seq: Some(2),
            event_types: Some(vec![McpEventType::ToolResponse]),
            min_score: Some(0.0),
            min_should_match: Some(1),
        })
        .await
        .expect("turn-scoped mcp event search");

    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].event_uid, "evt-c-tool");
    assert_eq!(result.hits[0].event_type, McpEventType::ToolResponse);
    assert_eq!(result.hits[0].turn_seq, 2);
    assert_eq!(result.hits[0].event_ordinal, 1);
    assert_eq!(result.hits[0].turn_event_count, 3);
    assert_eq!(result.hits[0].tool_name.as_deref(), Some("bash"));
    assert_eq!(result.hits[0].call_id.as_deref(), Some("call-bash-1"));

    let queries = state.queries.lock().expect("queries lock").clone();
    let search_query = queries
        .iter()
        .find(|q| {
            q.contains("AS mcp_event_type")
                && q.contains("tr.session_id = 'sess_c' AND tr.turn_seq = 2")
        })
        .expect("turn-scoped search query should be captured");
    assert!(search_query.contains("tr.session_id = 'sess_c' AND tr.turn_seq = 2"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_event_type_filter_distinguishes_user_and_assistant_messages() {
    let (repo, state) = build_repo().await;

    let user_result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![McpEventType::UserInput]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("user input search");
    let assistant_result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![McpEventType::AssistantResponse]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("assistant response search");

    assert_eq!(user_result.hits.len(), 1);
    assert_eq!(user_result.hits[0].event_uid, "evt-c-user");
    assert_eq!(user_result.hits[0].event_type, McpEventType::UserInput);
    assert_eq!(user_result.hits[0].actor_role, "user");
    assert_eq!(assistant_result.hits.len(), 1);
    assert_eq!(assistant_result.hits[0].event_uid, "evt-c-42");
    assert_eq!(
        assistant_result.hits[0].event_type,
        McpEventType::AssistantResponse
    );
    assert_eq!(assistant_result.hits[0].actor_role, "assistant");

    let queries = state.queries.lock().expect("queries lock").clone();
    assert!(queries.iter().any(|q| {
        q.contains("AS mcp_event_type") && q.contains("lowerUTF8(d.actor_role) = 'user'")
    }));
    assert!(queries.iter().any(|q| {
        q.contains("AS mcp_event_type") && q.contains("lowerUTF8(d.actor_role) = 'assistant'")
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_fetches_limit_plus_one_for_truncation() {
    let (repo, state) = build_repo().await;

    let result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(2),
            event_types: Some(vec![
                McpEventType::UserInput,
                McpEventType::AssistantResponse,
                McpEventType::ToolResponse,
            ]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("truncated mcp event search");

    assert_eq!(result.hits.len(), 2);
    assert!(result.truncated);
    assert!(result.stats.truncated);
    assert_eq!(result.stats.effective_n_hits, 2);

    let queries = state.queries.lock().expect("queries lock").clone();
    let search_query = queries
        .iter()
        .find(|q| q.contains("AS mcp_event_type") && q.contains("LIMIT 3"))
        .expect("search query should fetch limit plus one");
    assert!(search_query.contains("LIMIT 3"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_reports_event_ordinal_within_turn() {
    let (repo, _state) = build_repo().await;

    let result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![McpEventType::AssistantResponse]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("mcp event search");

    let hit = result
        .hits
        .iter()
        .find(|hit| hit.event_uid == "evt-c-42")
        .expect("assistant event hit");
    assert_eq!(hit.turn_seq, 2);
    assert_eq!(hit.event_order, 42);
    assert_eq!(hit.event_ordinal, 3);
    assert_eq!(hit.turn_event_count, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_supports_forward_cursor_pagination() {
    let (repo, state) = build_repo().await;

    let first = repo
        .list_session_events(
            SessionEventsQuery {
                session_id: "sess_c".to_string(),
                direction: SessionEventsDirection::Forward,
                event_kinds: None,
            },
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("first page");

    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].event_uid, "evt-1");
    assert_eq!(first.items[1].event_uid, "evt-2");
    assert!(first.next_cursor.is_some());

    let second = repo
        .list_session_events(
            SessionEventsQuery {
                session_id: "sess_c".to_string(),
                direction: SessionEventsDirection::Forward,
                event_kinds: None,
            },
            PageRequest {
                limit: 2,
                cursor: first.next_cursor,
            },
        )
        .await
        .expect("second page");

    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].event_uid, "evt-3");
    assert!(second.next_cursor.is_none());

    let queries = state.queries.lock().expect("queries lock").clone();
    let initial_query = queries
        .iter()
        .find(|q| q.contains("ORDER BY event_order ASC, event_uid ASC") && q.contains("LIMIT 3"))
        .expect("initial page query should be captured");
    assert!(initial_query.contains("WHERE session_id = 'sess_c'"));

    let paged_query = queries
        .iter()
        .find(|q| q.contains("event_order > 2 OR (event_order = 2 AND event_uid > 'evt-2')"))
        .expect("cursor query should include deterministic pagination clause");
    assert!(paged_query.contains("ORDER BY event_order ASC, event_uid ASC"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_supports_reverse_direction_and_event_kind_filter() {
    let (repo, state) = build_repo().await;

    let page = repo
        .list_session_events(
            SessionEventsQuery {
                session_id: "sess_c".to_string(),
                direction: SessionEventsDirection::Reverse,
                event_kinds: Some(vec![SearchEventKind::Message]),
            },
            PageRequest {
                limit: 5,
                cursor: None,
            },
        )
        .await
        .expect("reverse page");

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].event_uid, "evt-3");
    assert_eq!(page.items[1].event_uid, "evt-1");
    assert!(page.next_cursor.is_none());

    let queries = state.queries.lock().expect("queries lock").clone();
    let reverse_query = queries
        .iter()
        .find(|q| q.contains("ORDER BY event_order DESC, event_uid DESC"))
        .expect("reverse query should be captured");
    assert!(reverse_query.contains("event_class = 'message'"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_supports_reverse_cursor_pagination() {
    let (repo, state) = build_repo().await;

    let first = repo
        .list_session_events(
            SessionEventsQuery {
                session_id: "sess_c".to_string(),
                direction: SessionEventsDirection::Reverse,
                event_kinds: Some(vec![SearchEventKind::Message]),
            },
            PageRequest {
                limit: 1,
                cursor: None,
            },
        )
        .await
        .expect("first reverse page");

    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].event_uid, "evt-3");
    assert!(first.next_cursor.is_some());

    let second = repo
        .list_session_events(
            SessionEventsQuery {
                session_id: "sess_c".to_string(),
                direction: SessionEventsDirection::Reverse,
                event_kinds: Some(vec![SearchEventKind::Message]),
            },
            PageRequest {
                limit: 1,
                cursor: first.next_cursor,
            },
        )
        .await
        .expect("second reverse page");

    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].event_uid, "evt-1");
    assert!(second.next_cursor.is_none());

    let queries = state.queries.lock().expect("queries lock").clone();
    let paged_query = queries
        .iter()
        .find(|q| q.contains("event_order < 3 OR (event_order = 3 AND event_uid < 'evt-3')"))
        .expect("reverse cursor query should include deterministic pagination clause");
    assert!(paged_query.contains("ORDER BY event_order DESC, event_uid DESC"));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_rejects_cursor_with_mismatched_direction() {
    let (repo, state) = build_repo().await;
    let cursor = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&json!({
            "last_event_order": 3_u64,
            "last_event_uid": "evt-3",
            "session_id": "sess_c",
            "direction": "reverse",
            "filter_sig": "session=sess_c;direction=reverse;event_kinds=__none__"
        }))
        .expect("serialize cursor"),
    );

    let err = repo
        .list_session_events(
            SessionEventsQuery {
                session_id: "sess_c".to_string(),
                direction: SessionEventsDirection::Forward,
                event_kinds: None,
            },
            PageRequest {
                limit: 2,
                cursor: Some(cursor),
            },
        )
        .await
        .expect_err("mismatched direction cursor must fail");

    assert_eq!(
        err.to_string(),
        "invalid cursor: cursor direction does not match requested direction"
    );
    assert!(state.queries.lock().expect("queries lock").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn session_analytics_assembles_canonical_views_and_public_steps() {
    let responses = vec![
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_session_summary` AS s",
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS meta_events",
                "WHERE meta_events.event_kind = 'session_meta'",
                "WHERE notEmpty(trimBoth(s.session_id))",
                "ORDER BY s.last_event_time DESC, s.session_id DESC",
                "LIMIT 1",
                "FORMAT JSONEachRow",
            ],
            json!([session_analytics_summary_row()]),
        )
        .forbidding(&["now() - INTERVAL"]),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_turn_summary`",
                "WHERE session_id IN ['analytics-session']",
                "ORDER BY session_id ASC, turn_seq ASC",
                "FORMAT JSONEachRow",
            ],
            session_analytics_turn_rows(),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_conversation_trace` AS t",
                "LEFT JOIN (SELECT * FROM `moraine`.`events` FINAL) AS e ON e.event_uid = t.event_uid",
                "WHERE t.session_id IN ['analytics-session']",
                "ORDER BY t.session_id ASC, t.event_order ASC",
                "FORMAT JSONEachRow",
            ],
            session_analytics_event_rows(),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;
    let repo: Arc<dyn ConversationRepository> = Arc::new(repo);

    let sessions = repo
        .list_session_analytics(SessionAnalyticsQuery {
            lookback: SessionLookback::All,
            limit: 0,
        })
        .await
        .expect("session analytics succeeds");

    assert_eq!(sessions.len(), 1);
    let session = &sessions[0];
    assert_eq!(session.summary.session_id, "analytics-session");
    assert_eq!(session.summary.total_turns, 2);
    assert_eq!(session.summary.mode, ConversationMode::ToolCalling);
    assert_eq!(
        session.summary.session_slug.as_deref(),
        Some("analytics-slug")
    );
    assert_eq!(
        session.summary.session_summary.as_deref(),
        Some("Analytics fixture session")
    );
    assert_eq!(session.harness, "codex");
    assert_eq!(session.source_name, "codex-jsonl");
    assert_eq!(session.trace_id, "trace-454");
    assert_eq!(session.first_user_text, "first user fallback");
    assert_eq!(session.models, vec!["gpt-x", "other"]);
    assert_eq!(
        session
            .turns
            .iter()
            .map(|turn| turn.summary.turn_seq)
            .collect::<Vec<_>>(),
        vec![1, 2],
        "canonical view turn numbers remain one-based"
    );

    let first_turn = &session.turns[0];
    assert_eq!(first_turn.model, "GPT-X");
    assert_eq!(first_turn.token_usage_buckets.get("input_text"), Some(&5));
    assert_eq!(first_turn.token_usage_buckets.get("output_text"), Some(&2));
    assert_eq!(first_turn.token_usage_buckets.get("reasoning"), Some(&7));
    assert_eq!(first_turn.steps.len(), 4);
    assert!(matches!(
        &first_turn.steps[0],
        SessionStep::User { text, .. } if text == "first user fallback"
    ));
    match &first_turn.steps[1] {
        SessionStep::Assistant {
            text,
            endpoint_kind,
            latency_ms,
            token_usage_buckets,
            token_usage_native_units,
            ..
        } => {
            assert_eq!(text, "full answer");
            assert_eq!(endpoint_kind, "generation");
            assert_eq!(*latency_ms, Some(42));
            assert_eq!(token_usage_buckets.get("reasoning"), Some(&7));
            assert!(!token_usage_buckets.contains_key("input_text"));
            assert_eq!(token_usage_native_units.get("usd"), Some(&0.5));
            assert!(!token_usage_native_units.contains_key("zero"));
        }
        other => panic!("expected assistant step, got {other:?}"),
    }
    match &first_turn.steps[2] {
        SessionStep::ToolCall {
            tool_name,
            call_id,
            arguments,
            latency_ms,
            is_error,
            result,
            ..
        } => {
            assert_eq!(tool_name, "Read");
            assert_eq!(call_id, "call-read");
            assert_eq!(arguments, &json!({ "path": "src/lib.rs" }));
            assert_eq!(*latency_ms, Some(17));
            assert!(*is_error);
            let result = result.as_ref().expect("paired tool result");
            assert_eq!(result.text, "read failed");
            assert_eq!(result.latency_ms, 600);
            assert!(result.is_error);
        }
        other => panic!("expected tool call step, got {other:?}"),
    }
    assert!(matches!(
        &first_turn.steps[3],
        SessionStep::Thinking { text, .. } if text == "consider the result"
    ));
    assert_eq!(
        session.turns[1].token_usage_buckets.get("output_text"),
        Some(&3)
    );
    assert_script_consumed(&state, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn session_analytics_empty_and_timed_limit_sql_are_deterministic() {
    let (repo, state) = build_scripted_repo(vec![ScriptedResponse::rows(
        &[
            "FROM `moraine`.`v_session_summary` AS s",
            "AND s.last_event_time >= now() - INTERVAL 86400 SECOND",
            "ORDER BY s.last_event_time DESC, s.session_id DESC",
            "LIMIT 200",
            "FORMAT JSONEachRow",
        ],
        json!([]),
    )])
    .await;

    let sessions = repo
        .list_session_analytics(SessionAnalyticsQuery {
            lookback: SessionLookback::TwentyFourHours,
            limit: u16::MAX,
        })
        .await
        .expect("empty analytics succeeds");
    assert!(sessions.is_empty());
    assert_script_consumed(&state, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn session_analytics_propagates_each_wire_stage_error() {
    let scenarios = vec![
        vec![ScriptedResponse::failure(
            &["FROM `moraine`.`v_session_summary` AS s"],
            "session stage summary failed",
        )],
        vec![
            ScriptedResponse::rows(
                &["FROM `moraine`.`v_session_summary` AS s"],
                json!([session_analytics_summary_row()]),
            ),
            ScriptedResponse::failure(
                &["FROM `moraine`.`v_turn_summary`"],
                "session stage turns failed",
            ),
        ],
        vec![
            ScriptedResponse::rows(
                &["FROM `moraine`.`v_session_summary` AS s"],
                json!([session_analytics_summary_row()]),
            ),
            ScriptedResponse::rows(
                &["FROM `moraine`.`v_turn_summary`"],
                session_analytics_turn_rows(),
            ),
            ScriptedResponse::failure(
                &["FROM `moraine`.`v_conversation_trace` AS t"],
                "session stage events failed",
            ),
        ],
    ];

    for (stage, responses) in scenarios.into_iter().enumerate() {
        let (repo, state) = build_scripted_repo(responses).await;
        let error = repo
            .list_session_analytics(SessionAnalyticsQuery {
                lookback: SessionLookback::All,
                limit: 1,
            })
            .await
            .expect_err("wire stage error propagates");
        assert!(
            error.to_string().contains("session stage"),
            "stage {stage} error: {error}"
        );
        assert_script_consumed(&state, stage + 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_24h_uses_exact_four_request_canonical_wire_contract() {
    let responses = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([
            {
                "bucket_unix": 150_000_u64,
                "model": "gpt-5.3-codex-xhigh",
                "endpoint_kind": "responses",
                "bucket": "input_text",
                "tokens": 12_u64
            },
            {
                "bucket_unix": 153_600_u64,
                "model": "other",
                "endpoint_kind": "messages",
                "bucket": "output_text",
                "tokens": 8_u64
            }
        ]),
        json!([{
            "bucket_unix": 150_000_u64,
            "model": "gpt-5.3-codex-xhigh",
            "turns": 3_u64
        }]),
        json!([{
            "bucket_unix": 150_000_u64,
            "concurrent_sessions": 2_u64
        }]),
    );
    let (repo, state) = build_scripted_repo(responses).await;

    let snapshot = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("24h analytics succeeds");

    assert_eq!(snapshot.window.range, AnalyticsRange::TwentyFourHours);
    assert_eq!(snapshot.window.window_seconds, 86_400);
    assert_eq!(snapshot.window.bucket_seconds, 3_600);
    assert_eq!(snapshot.window.from_unix, 113_600);
    assert_eq!(snapshot.window.to_unix, 200_000);
    assert_eq!(snapshot.tokens.len(), 2);
    assert_eq!(snapshot.tokens[0].model, "gpt-5.3-codex-xhigh");
    assert_eq!(snapshot.tokens[0].tokens, 12);
    assert_eq!(snapshot.tokens[1].bucket, "output_text");
    assert_eq!(snapshot.turns[0].turns, 3);
    assert_eq!(snapshot.concurrent_sessions[0].concurrent_sessions, 2);
    assert_script_consumed(&state, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_all_six_ranges_use_distinct_wire_keys() {
    let cases = [
        (
            AnalyticsRange::FifteenMinutes,
            "toInt64(900)",
            "INTERVAL 60 SECOND",
        ),
        (
            AnalyticsRange::OneHour,
            "toInt64(3600)",
            "INTERVAL 300 SECOND",
        ),
        (
            AnalyticsRange::SixHours,
            "toInt64(21600)",
            "INTERVAL 900 SECOND",
        ),
        (
            AnalyticsRange::TwentyFourHours,
            "toInt64(86400)",
            "INTERVAL 3600 SECOND",
        ),
        (
            AnalyticsRange::SevenDays,
            "toInt64(604800)",
            "INTERVAL 21600 SECOND",
        ),
        (
            AnalyticsRange::ThirtyDays,
            "toInt64(2592000)",
            "INTERVAL 86400 SECOND",
        ),
    ];
    let mut responses = Vec::new();
    for (_, window, interval) in cases {
        responses.extend(analytics_responses(
            window,
            interval,
            json!([]),
            json!([]),
            json!([]),
        ));
    }
    let (repo, state) = build_scripted_repo(responses).await;

    for (range, _, _) in cases {
        let snapshot = repo
            .analytics_series(range)
            .await
            .expect("distinct analytics range succeeds");
        assert_eq!(snapshot.window.range, range);
    }
    assert_script_consumed(&state, 24);
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_cache_hit_and_distinct_key_are_request_count_proven() {
    let mut responses = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    responses.extend(analytics_responses(
        "toInt64(3600)",
        "INTERVAL 300 SECOND",
        json!([]),
        json!([]),
        json!([]),
    ));
    let (repo, state) = build_scripted_repo(responses).await;
    let clone = repo.clone();

    let first = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("cache miss succeeds");
    let hit = clone
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("same-key cache hit succeeds");
    assert_eq!(first, hit);
    assert_eq!(state.queries.lock().expect("queries lock").len(), 4);

    let distinct = repo
        .analytics_series(AnalyticsRange::OneHour)
        .await
        .expect("distinct-key cache miss succeeds");
    assert_eq!(distinct.window.range, AnalyticsRange::OneHour);
    assert_eq!(distinct.window.window_seconds, 3_600);
    assert_eq!(distinct.window.bucket_seconds, 300);
    assert_script_consumed(&state, 8);
}

#[tokio::test]
async fn analytics_expired_entry_refills_from_the_repository() {
    let mut responses = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([{
            "bucket_unix": 100_000_u64,
            "model": "model",
            "endpoint_kind": "generation",
            "bucket": "input_text",
            "tokens": 1_u64
        }]),
        json!([]),
        json!([]),
    );
    responses.extend(analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([{
            "bucket_unix": 100_000_u64,
            "model": "model",
            "endpoint_kind": "generation",
            "bucket": "input_text",
            "tokens": 2_u64
        }]),
        json!([]),
        json!([]),
    ));
    let (repo, state) = build_scripted_repo(responses).await;

    let first = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("initial cache fill succeeds");
    assert_eq!(first.tokens[0].tokens, 1);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();
    let refilled = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("stale cache entry refills");
    assert_eq!(refilled.tokens[0].tokens, 2);
    assert_script_consumed(&state, 8);
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_stage_error_is_not_cached_and_empty_retry_succeeds() {
    let success = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    let mut responses = vec![
        success[0].clone(),
        ScriptedResponse::failure(
            &[
                "ARRAY JOIN mapKeys(e.token_usage_buckets)",
                "FORMAT JSONEachRow",
            ],
            "analytics token stage failed",
        ),
    ];
    responses.extend(success);
    let (repo, state) = build_scripted_repo(responses).await;

    let error = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect_err("token stage failure propagates");
    assert!(error.to_string().contains("analytics token stage failed"));

    let retry = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("failed load is not cached");
    assert!(retry.tokens.is_empty());
    assert!(retry.turns.is_empty());
    assert!(retry.concurrent_sessions.is_empty());
    assert_script_consumed(&state, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_anchor_decode_and_late_stage_errors_are_not_cached() {
    let success = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    let scenarios = [
        (
            "anchor",
            vec![ScriptedResponse::failure(
                &["database_now_unix", "FORMAT JSONEachRow"],
                "analytics anchor stage failed",
            )],
            5,
        ),
        (
            "decode",
            vec![ScriptedResponse::raw(
                &["database_now_unix", "FORMAT JSONEachRow"],
                "not-json\n",
            )],
            5,
        ),
        (
            "turn",
            vec![
                success[0].clone(),
                success[1].clone(),
                ScriptedResponse::failure(
                    &[
                        "uniqExact(tuple(e.session_id, e.request_id))",
                        "FORMAT JSONEachRow",
                    ],
                    "analytics turn stage failed",
                ),
            ],
            7,
        ),
        (
            "concurrency",
            vec![
                success[0].clone(),
                success[1].clone(),
                success[2].clone(),
                ScriptedResponse::failure(
                    &["uniqExact(session_stream_key)", "FORMAT JSONEachRow"],
                    "analytics concurrency stage failed",
                ),
            ],
            8,
        ),
    ];

    for (stage, mut responses, expected_requests) in scenarios {
        responses.extend(success.clone());
        let (repo, state) = build_scripted_repo(responses).await;
        repo.analytics_series(AnalyticsRange::TwentyFourHours)
            .await
            .expect_err(stage);
        let retry = repo
            .analytics_series(AnalyticsRange::TwentyFourHours)
            .await
            .unwrap_or_else(|error| panic!("{stage} retry failed: {error}"));
        assert!(retry.tokens.is_empty());
        assert!(retry.turns.is_empty());
        assert!(retry.concurrent_sessions.is_empty());
        assert_script_consumed(&state, expected_requests);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_same_key_concurrency_coalesces_to_one_wire_load() {
    let (repo, state) = build_scripted_repo(analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    ))
    .await;
    let repo = Arc::new(repo);

    let (left, right) = tokio::join!(
        repo.analytics_series(AnalyticsRange::TwentyFourHours),
        repo.analytics_series(AnalyticsRange::TwentyFourHours)
    );
    assert_eq!(
        left.expect("first concurrent load"),
        right.expect("coalesced load")
    );
    assert_script_consumed(&state, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_different_ranges_fill_concurrently() {
    let first_anchor_reached = Arc::new(Notify::new());
    let first_anchor_release = Arc::new(Notify::new());
    let second_anchor_reached = Arc::new(Notify::new());
    let second_anchor_release = Arc::new(Notify::new());
    let first_fill_reached_last_stage = Arc::new(Notify::new());
    let first_fill_release_last_stage = Arc::new(Notify::new());
    let anchor_row = json!([{
        "scan_from_unix": 100_000_u64,
        "scan_to_unix": 200_000_u64,
        "display_to_unix": 200_000_u64
    }]);
    let generic_anchor = |reached: Arc<Notify>, release: Arc<Notify>| {
        ScriptedResponse::rows(
            &[
                "toInt64(toUnixTimestamp(now())) AS database_now_unix",
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
            ],
            anchor_row.clone(),
        )
        .blocked(reached, release)
    };
    let token = || {
        ScriptedResponse::rows(
            &[
                "ARRAY JOIN mapKeys(e.token_usage_buckets)",
                "FORMAT JSONEachRow",
            ],
            json!([]),
        )
    };
    let turns = || {
        ScriptedResponse::rows(
            &[
                "uniqExact(tuple(e.session_id, e.request_id))",
                "FORMAT JSONEachRow",
            ],
            json!([]),
        )
    };
    let concurrency = || {
        ScriptedResponse::rows(
            &["uniqExact(session_stream_key)", "FORMAT JSONEachRow"],
            json!([]),
        )
    };
    let responses = vec![
        generic_anchor(first_anchor_reached.clone(), first_anchor_release.clone()),
        generic_anchor(second_anchor_reached.clone(), second_anchor_release.clone()),
        token(),
        turns(),
        concurrency().blocked(
            first_fill_reached_last_stage.clone(),
            first_fill_release_last_stage.clone(),
        ),
        token(),
        turns(),
        concurrency(),
    ];
    let (repo, state) = build_scripted_repo(responses).await;
    let repo = Arc::new(repo);

    let one_hour_repo = repo.clone();
    let one_hour = tokio::spawn(async move {
        one_hour_repo
            .analytics_series(AnalyticsRange::OneHour)
            .await
    });
    let day_repo = repo.clone();
    let day = tokio::spawn(async move {
        day_repo
            .analytics_series(AnalyticsRange::TwentyFourHours)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), first_anchor_reached.notified())
        .await
        .expect("first range reached its anchor");
    tokio::time::timeout(Duration::from_secs(2), second_anchor_reached.notified())
        .await
        .expect("second range reached its anchor before the first was released");

    first_anchor_release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        first_fill_reached_last_stage.notified(),
    )
    .await
    .expect("first range reached its final stage");
    first_fill_release_last_stage.notify_one();
    second_anchor_release.notify_one();

    let (one_hour, day) = tokio::join!(one_hour, day);
    assert_eq!(
        one_hour
            .expect("one-hour task joined")
            .expect("one-hour fill succeeds")
            .window
            .range,
        AnalyticsRange::OneHour
    );
    assert_eq!(
        day.expect("day task joined")
            .expect("day fill succeeds")
            .window
            .range,
        AnalyticsRange::TwentyFourHours
    );
    assert_script_consumed(&state, 8);
}

#[tokio::test(flavor = "multi_thread")]
async fn analytics_cancelled_fill_releases_slot_and_recovery_is_cached() {
    let success = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut responses = vec![success[0].clone().blocked(reached.clone(), release.clone())];
    responses.extend(success);
    let (repo, state) = build_scripted_repo(responses).await;
    let repo = Arc::new(repo);

    let first_repo = repo.clone();
    let first = tokio::spawn(async move {
        first_repo
            .analytics_series(AnalyticsRange::TwentyFourHours)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), reached.notified())
        .await
        .expect("scripted analytics load reached the wire barrier");
    first.abort();
    release.notify_one();
    assert!(first
        .await
        .expect_err("fill task was aborted")
        .is_cancelled());

    let recovered = tokio::time::timeout(
        Duration::from_secs(2),
        repo.analytics_series(AnalyticsRange::TwentyFourHours),
    )
    .await
    .expect("cancelled fill released the range slot")
    .expect("recovery fill succeeds");
    let hit = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("recovery result was cached");
    assert_eq!(recovered, hit);
    assert_script_consumed(&state, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn web_feed_covers_variants_precedence_limit_order_and_canonical_source() {
    let response = ScriptedResponse::rows(
        &[
            "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
            "e.payload_type = 'web_search_call'",
            "e.payload_type = 'tool_use' AND e.tool_name IN ('WebSearch', 'WebFetch')",
            "e.payload_type = 'search_results_received'",
            "JSONExtractString(e.payload_json, 'action', 'query')",
            "JSONExtractString(e.payload_json, 'input', 'query')",
            "JSONExtractString(e.payload_json, 'data', 'query')",
            "JSONExtractString(e.payload_json, 'action', 'url')",
            "JSONExtractString(e.payload_json, 'input', 'url')",
            "ORDER BY e.event_ts DESC, e.event_uid DESC",
            "LIMIT 1000",
            "FORMAT JSONEachRow",
        ],
        json!([
            {
                "event_time": "2026-06-01 12:03:00",
                "harness": "codex",
                "source_name": "codex-jsonl",
                "session_id": "web-3",
                "model": "gpt-5",
                "action": "search_results_received",
                "search_query": "data query",
                "result_url": "",
                "source_ref": "/tmp/web:3"
            },
            {
                "event_time": "2026-06-01 12:02:00",
                "harness": "claude-code",
                "source_name": "claude-jsonl",
                "session_id": "web-2",
                "model": "claude",
                "action": "open_page",
                "search_query": "",
                "result_url": "https://example.test/page",
                "source_ref": "/tmp/web:2"
            },
            {
                "event_time": "2026-06-01 12:01:00",
                "harness": "codex",
                "source_name": "codex-jsonl",
                "session_id": "web-1",
                "model": "gpt-5",
                "action": "search",
                "search_query": "action path wins",
                "result_url": "https://example.test/result",
                "source_ref": "/tmp/web:1"
            }
        ]),
    );
    let (repo, state) = build_scripted_repo(vec![response]).await;

    let events = repo
        .list_web_searches(u16::MAX)
        .await
        .expect("web feed succeeds");

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].session_id, "web-3");
    assert_eq!(events[0].search_query, "data query");
    assert_eq!(events[1].action, "open_page");
    assert_eq!(events[1].result_url, "https://example.test/page");
    assert_eq!(events[2].search_query, "action path wins");
    assert_eq!(events[2].source_ref, "/tmp/web:1");
    assert_script_consumed(&state, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn web_feed_propagates_backend_and_json_each_row_decode_errors() {
    let scenarios = [
        ScriptedResponse::failure(
            &["FROM (SELECT * FROM `moraine`.`events` FINAL) AS e"],
            "web feed backend failed",
        ),
        ScriptedResponse::raw(
            &[
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
                "FORMAT JSONEachRow",
            ],
            "not-json\n",
        ),
    ];

    for (index, response) in scenarios.into_iter().enumerate() {
        let (repo, state) = build_scripted_repo(vec![response]).await;
        let error = repo
            .list_web_searches(0)
            .await
            .expect_err("web feed failure propagates");
        if index == 0 {
            assert!(error.to_string().contains("web feed backend failed"));
        } else {
            assert!(error.to_string().contains("failed to parse JSONEachRow"));
        }
        assert_script_consumed(&state, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_reports_missing_table_without_read_query() {
    let (repo, state) = build_scripted_repo(vec![ScriptedResponse::rows(
        &[
            "SELECT name",
            "FROM system.columns",
            "WHERE database = 'moraine' AND table = 'ingest_heartbeats'",
            "ORDER BY position ASC",
            "FORMAT JSONEachRow",
        ],
        json!([]),
    )])
    .await;

    let heartbeat = repo
        .latest_ingest_heartbeat()
        .await
        .expect("missing heartbeat table is optional");
    assert!(!heartbeat.table_present);
    assert!(heartbeat.latest.is_none());
    assert_script_consumed(&state, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_reports_present_but_empty_table() {
    let responses = vec![
        ScriptedResponse::rows(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            heartbeat_columns(&[]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`ingest_heartbeats`",
                "ORDER BY `ts` DESC, `host` DESC",
                "LIMIT 1",
                "FORMAT JSONEachRow",
            ],
            json!([]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let heartbeat = repo
        .latest_ingest_heartbeat()
        .await
        .expect("empty heartbeat table succeeds");
    assert!(heartbeat.table_present);
    assert!(heartbeat.latest.is_none());
    assert_script_consumed(&state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_pre_017_row_uses_null_projections_for_absent_columns() {
    let mut row = heartbeat_row();
    row["service_version"] = json!("0.16.9");
    let responses = vec![
        ScriptedResponse::rows(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            heartbeat_columns(&[]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`ingest_heartbeats`",
                "CAST(NULL, 'Nullable(String)') AS `watcher_backend`",
                "CAST(NULL, 'Nullable(UInt64)') AS `watcher_error_count`",
                "CAST(NULL, 'Nullable(UInt64)') AS `watcher_reset_count`",
                "CAST(NULL, 'Nullable(UInt64)') AS `watcher_last_reset_unix_ms`",
                "CAST(NULL, 'Nullable(String)') AS `backend_sinks`",
                "FORMAT JSONEachRow",
            ],
            json!([row]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let heartbeat = repo
        .latest_ingest_heartbeat()
        .await
        .expect("pre-0.17 heartbeat succeeds");
    let latest = heartbeat.latest.expect("heartbeat row");
    assert_eq!(latest.service_version, "0.16.9");
    assert_eq!(latest.queue_depth, 4);
    assert!(latest.watcher_backend.is_none());
    assert!(latest.watcher_error_count.is_none());
    assert!(latest.watcher_reset_count.is_none());
    assert!(latest.watcher_last_reset_unix_ms.is_none());
    assert!(latest.backend_sinks.is_none());

    let queries = state.queries.lock().expect("queries lock");
    let read_query = &queries[1];
    for absent in [
        "`watcher_backend`,",
        "`watcher_error_count`,",
        "`watcher_reset_count`,",
        "`watcher_last_reset_unix_ms`,",
        "`backend_sinks`",
    ] {
        assert!(
            !read_query.lines().any(|line| line.trim() == absent),
            "absent column projected directly: {absent}\n{read_query}"
        );
    }
    drop(queries);
    assert_script_consumed(&state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_partial_watcher_schema_reads_only_present_optional_columns() {
    let mut row = heartbeat_row();
    row["watcher_backend"] = json!("fsevents");
    row["watcher_error_count"] = json!(3_u64);
    let responses = vec![
        ScriptedResponse::rows(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            heartbeat_columns(&["watcher_backend", "watcher_error_count"]),
        ),
        ScriptedResponse::rows(
            &[
                "`watcher_backend`,",
                "`watcher_error_count`,",
                "CAST(NULL, 'Nullable(UInt64)') AS `watcher_reset_count`",
                "CAST(NULL, 'Nullable(UInt64)') AS `watcher_last_reset_unix_ms`",
                "CAST(NULL, 'Nullable(String)') AS `backend_sinks`",
                "FROM `moraine`.`ingest_heartbeats`",
                "FORMAT JSONEachRow",
            ],
            json!([row]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let latest = repo
        .latest_ingest_heartbeat()
        .await
        .expect("partial watcher heartbeat succeeds")
        .latest
        .expect("heartbeat row");
    assert_eq!(latest.watcher_backend.as_deref(), Some("fsevents"));
    assert_eq!(latest.watcher_error_count, Some(3));
    assert!(latest.watcher_reset_count.is_none());
    assert!(latest.backend_sinks.is_none());
    assert_script_consumed(&state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_post_017_backend_sinks_handles_blank_valid_and_malformed_values() {
    let cases = [
        ("", json!({})),
        (
            "{\"clickhouse\":{\"healthy\":true},\"sqlite\":{\"healthy\":false}}",
            json!({
                "clickhouse": { "healthy": true },
                "sqlite": { "healthy": false }
            }),
        ),
        ("malformed-backend-sinks", json!("malformed-backend-sinks")),
    ];
    let optional = [
        "watcher_backend",
        "watcher_error_count",
        "watcher_reset_count",
        "watcher_last_reset_unix_ms",
        "backend_sinks",
    ];

    for (raw, expected) in cases {
        let mut row = heartbeat_row();
        row["watcher_backend"] = json!("fsevents");
        row["watcher_error_count"] = json!(1_u64);
        row["watcher_reset_count"] = json!(2_u64);
        row["watcher_last_reset_unix_ms"] = json!(1780307999000_u64);
        row["backend_sinks"] = json!(raw);
        let responses = vec![
            ScriptedResponse::rows(
                &["FROM system.columns", "FORMAT JSONEachRow"],
                heartbeat_columns(&optional),
            ),
            ScriptedResponse::rows(
                &[
                    "`watcher_backend`,",
                    "`watcher_error_count`,",
                    "`watcher_reset_count`,",
                    "`watcher_last_reset_unix_ms`,",
                    "`backend_sinks`",
                    "FROM `moraine`.`ingest_heartbeats`",
                    "FORMAT JSONEachRow",
                ],
                json!([row]),
            ),
        ];
        let (repo, state) = build_scripted_repo(responses).await;

        let latest = repo
            .latest_ingest_heartbeat()
            .await
            .expect("post-0.17 heartbeat succeeds")
            .latest
            .expect("heartbeat row");
        assert_eq!(latest.watcher_backend.as_deref(), Some("fsevents"));
        assert_eq!(latest.watcher_reset_count, Some(2));
        assert_eq!(latest.backend_sinks, Some(expected));
        assert_script_consumed(&state, 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_propagates_column_probe_and_latest_read_errors() {
    let (probe_repo, probe_state) = build_scripted_repo(vec![ScriptedResponse::failure(
        &["FROM system.columns", "FORMAT JSONEachRow"],
        "heartbeat column probe failed",
    )])
    .await;
    let probe_error = probe_repo
        .latest_ingest_heartbeat()
        .await
        .expect_err("column probe failure propagates");
    assert!(probe_error
        .to_string()
        .contains("heartbeat column probe failed"));
    assert_script_consumed(&probe_state, 1);

    let (read_repo, read_state) = build_scripted_repo(vec![
        ScriptedResponse::rows(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            heartbeat_columns(&[]),
        ),
        ScriptedResponse::failure(
            &["FROM `moraine`.`ingest_heartbeats`", "FORMAT JSONEachRow"],
            "heartbeat latest read failed",
        ),
    ])
    .await;
    let read_error = read_repo
        .latest_ingest_heartbeat()
        .await
        .expect_err("heartbeat read failure propagates");
    assert!(read_error
        .to_string()
        .contains("heartbeat latest read failed"));
    assert_script_consumed(&read_state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_summaries_map_metadata_and_active_part_counts() {
    let responses = vec![
        ScriptedResponse::rows(
            &[
                "SELECT name, engine, toUInt8(is_temporary) AS is_temporary",
                "FROM system.tables",
                "WHERE database = 'moraine'",
                "ORDER BY name ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                {
                    "name": "events",
                    "engine": "ReplacingMergeTree",
                    "is_temporary": 0_u8
                },
                {
                    "name": "scratch",
                    "engine": "Memory",
                    "is_temporary": 1_u8
                },
                {
                    "name": "v_session_summary",
                    "engine": "View",
                    "is_temporary": 0_u8
                }
            ]),
        ),
        ScriptedResponse::rows(
            &[
                "SELECT table, toUInt64(sum(rows)) AS rows",
                "FROM system.parts",
                "WHERE database = 'moraine' AND active",
                "GROUP BY table",
                "ORDER BY table ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                { "table": "events", "rows": 42_u64 },
                { "table": "scratch", "rows": 7_u64 }
            ]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let summaries = repo
        .list_table_summaries()
        .await
        .expect("table summaries succeed");

    assert!(summaries.row_counts_error.is_none());
    assert_eq!(summaries.tables.len(), 3);
    assert_eq!(summaries.tables[0].name, "events");
    assert_eq!(summaries.tables[0].engine, "ReplacingMergeTree");
    assert!(!summaries.tables[0].is_temporary);
    assert_eq!(summaries.tables[0].rows, 42);
    assert!(summaries.tables[1].is_temporary);
    assert_eq!(summaries.tables[1].rows, 7);
    assert_eq!(summaries.tables[2].rows, 0);
    assert_script_consumed(&state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_summaries_return_metadata_when_parts_probe_fails() {
    let responses = vec![
        ScriptedResponse::rows(
            &["FROM system.tables", "FORMAT JSONEachRow"],
            json!([{
                "name": "events",
                "engine": "ReplacingMergeTree",
                "is_temporary": 0_u8
            }]),
        ),
        ScriptedResponse::failure(
            &["FROM system.parts", "FORMAT JSONEachRow"],
            "active parts probe failed",
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let summaries = repo
        .list_table_summaries()
        .await
        .expect("parts failure is partial");
    assert_eq!(summaries.tables.len(), 1);
    assert_eq!(summaries.tables[0].rows, 0);
    assert!(summaries
        .row_counts_error
        .as_deref()
        .is_some_and(|message| message.contains("active parts probe failed")));
    assert_script_consumed(&state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_rejects_invalid_identifier_before_any_http_call() {
    let (repo, state) = build_repo().await;

    let error = repo
        .preview_table(TablePreviewQuery {
            table: "events; DROP TABLE events".to_string(),
            limit: 25,
        })
        .await
        .expect_err("invalid table identifier is rejected");

    assert!(error
        .to_string()
        .contains("table must match [A-Za-z_][A-Za-z0-9_]*"));
    assert!(state.queries.lock().expect("queries lock").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_clamps_limit_and_maps_ordered_schema_and_json_values() {
    let responses = vec![
        ScriptedResponse::rows(
            &[
                "SELECT name, type AS type_name, default_expression",
                "FROM system.columns",
                "WHERE database = 'moraine' AND table = 'events'",
                "ORDER BY position ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                {
                    "name": "id",
                    "type_name": "UInt64",
                    "default_expression": ""
                },
                {
                    "name": "name",
                    "type_name": "String",
                    "default_expression": "'anonymous'"
                }
            ]),
        ),
        ScriptedResponse::rows(
            &[
                "SELECT * FROM `moraine`.`events`",
                "ORDER BY `id`, `name`",
                "LIMIT 500",
                "FORMAT JSONEachRow",
            ],
            json!([
                { "id": 1_u64, "name": "alpha", "nested": { "ok": true } },
                { "id": 2_u64, "name": "beta", "nullable": null }
            ]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let preview = repo
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: u16::MAX,
        })
        .await
        .expect("table preview succeeds");

    assert_eq!(preview.table, "events");
    assert_eq!(preview.limit, 500);
    assert_eq!(preview.schema.len(), 2);
    assert_eq!(preview.schema[0].name, "id");
    assert_eq!(preview.schema[0].type_name, "UInt64");
    assert_eq!(preview.schema[1].default_expression, "'anonymous'");
    assert_eq!(preview.rows[0]["nested"]["ok"], json!(true));
    assert_eq!(preview.rows[1]["nullable"], serde_json::Value::Null);
    assert_script_consumed(&state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_propagates_schema_and_row_stage_errors() {
    let (schema_repo, schema_state) = build_scripted_repo(vec![ScriptedResponse::failure(
        &["FROM system.columns", "FORMAT JSONEachRow"],
        "preview schema failed",
    )])
    .await;
    let schema_error = schema_repo
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: 10,
        })
        .await
        .expect_err("schema failure propagates");
    assert!(schema_error.to_string().contains("preview schema failed"));
    assert_script_consumed(&schema_state, 1);

    let (row_repo, row_state) = build_scripted_repo(vec![
        ScriptedResponse::rows(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            json!([{
                "name": "id",
                "type_name": "UInt64",
                "default_expression": ""
            }]),
        ),
        ScriptedResponse::failure(
            &["SELECT * FROM `moraine`.`events` ORDER BY `id` LIMIT 10 FORMAT JSONEachRow"],
            "preview row read failed",
        ),
    ])
    .await;
    let row_error = row_repo
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: 10,
        })
        .await
        .expect_err("row failure propagates");
    assert!(row_error.to_string().contains("preview row read failed"));
    assert_script_consumed(&row_state, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn store_health_maps_all_successful_probe_facts() {
    let responses = vec![
        ScriptedResponse::raw(&["SELECT 1"], "1\n"),
        ScriptedResponse::raw(
            &["SELECT version() AS version"],
            json_envelope(json!([{ "version": "25.8.1.1" }])),
        ),
        ScriptedResponse::rows(
            &[
                "FROM system.databases",
                "WHERE name = 'moraine'",
                "FORMAT JSONEachRow",
            ],
            json!([{ "exists": 1_u8 }]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM system.metrics",
                "WHERE metric IN ('TCPConnection', 'HTTPConnection', 'MySQLConnection', 'PostgreSQLConnection', 'InterserverConnection')",
                "ORDER BY metric ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                { "metric": "HTTPConnection", "value": 2_u64 },
                { "metric": "InterserverConnection", "value": 5_u64 },
                { "metric": "MySQLConnection", "value": 3_u64 },
                { "metric": "PostgreSQLConnection", "value": 4_u64 },
                { "metric": "TCPConnection", "value": 7_u64 },
                { "metric": "TCPConnection", "value": 1_u64 }
            ]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let health = repo
        .read_store_health()
        .await
        .expect("health probes succeed");

    assert!(matches!(health.ping, StoreProbe::Available(ms) if ms >= 0.0));
    assert_eq!(
        health.version,
        StoreProbe::Available("25.8.1.1".to_string())
    );
    assert_eq!(health.database_exists, StoreProbe::Available(true));
    match health.connections {
        StoreProbe::Available(metrics) => {
            assert_eq!(metrics.tcp, 8);
            assert_eq!(metrics.http, 2);
            assert_eq!(metrics.mysql, 3);
            assert_eq!(metrics.postgres, 4);
            assert_eq!(metrics.interserver, 5);
            assert_eq!(metrics.total, 22);
        }
        other => panic!("expected connection metrics, got {other:?}"),
    }
    assert_script_consumed(&state, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn store_health_keeps_each_probe_failure_independent() {
    let responses = vec![
        ScriptedResponse::failure(&["SELECT 1"], "health ping failed"),
        ScriptedResponse::failure(&["SELECT version() AS version"], "health version failed"),
        ScriptedResponse::failure(&["FROM system.databases"], "health database failed"),
        ScriptedResponse::failure(&["FROM system.metrics"], "health connections failed"),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let health = repo
        .read_store_health()
        .await
        .expect("probe failures are facts, not method errors");

    assert!(matches!(
        health.ping,
        StoreProbe::Failed { ref message } if message.contains("health ping failed")
    ));
    assert!(matches!(
        health.version,
        StoreProbe::Failed { ref message } if message.contains("health version failed")
    ));
    assert!(matches!(
        health.database_exists,
        StoreProbe::Failed { ref message } if message.contains("health database failed")
    ));
    assert!(matches!(
        health.connections,
        StoreProbe::Failed { ref message } if message.contains("health connections failed")
    ));
    assert_script_consumed(&state, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn diagnostics_maps_doctor_partial_report_and_ping_short_circuit() {
    let required_tables = json!([
        { "name": "raw_events" },
        { "name": "events" },
        { "name": "event_links" },
        { "name": "tool_io" },
        { "name": "ingest_checkpoints" },
        { "name": "ingest_heartbeats" },
        { "name": "search_documents" },
        { "name": "search_postings" },
        { "name": "search_conversation_terms" },
        { "name": "search_term_stats" },
        { "name": "search_corpus_stats" },
        { "name": "search_query_log" },
        { "name": "search_hit_log" },
        { "name": "search_interaction_log" },
        { "name": "schema_migrations" }
    ]);
    let responses = vec![
        ScriptedResponse::raw(&["SELECT 1"], "1\n"),
        ScriptedResponse::failure(
            &["SELECT version() AS version"],
            "doctor version probe failed",
        ),
        ScriptedResponse::raw(
            &[
                "SELECT toUInt8(count() > 0) AS exists FROM system.databases WHERE name = 'moraine'",
            ],
            json_envelope(json!([{ "exists": 1_u8 }])),
        ),
        ScriptedResponse::failure(
            &["SELECT version FROM `moraine`.schema_migrations GROUP BY version"],
            "doctor ledger read failed",
        ),
        ScriptedResponse::raw(
            &["SELECT name FROM system.tables WHERE database = 'moraine'"],
            json_envelope(required_tables),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let diagnostics = repo
        .read_store_diagnostics()
        .await
        .expect("doctor partial report maps");

    assert!(diagnostics.healthy);
    assert!(diagnostics.version.is_none());
    assert_eq!(diagnostics.database, "moraine");
    assert!(diagnostics.database_exists);
    assert!(diagnostics.applied_schema_versions.is_empty());
    assert!(!diagnostics.pending_schema_versions.is_empty());
    assert_eq!(diagnostics.missing_tables, vec!["ingest_errors"]);
    assert_eq!(diagnostics.errors.len(), 2);
    assert!(diagnostics.errors[0].contains("version query failed"));
    assert!(diagnostics.errors[0].contains("doctor version probe failed"));
    assert!(diagnostics.errors[1].contains("failed to read migration ledger"));
    assert!(diagnostics.errors[1].contains("doctor ledger read failed"));
    assert_script_consumed(&state, 5);

    let (down_repo, down_state) = build_scripted_repo(vec![ScriptedResponse::failure(
        &["SELECT 1"],
        "doctor ping unavailable",
    )])
    .await;
    let down = down_repo
        .read_store_diagnostics()
        .await
        .expect("doctor ping failure returns partial report");
    assert!(!down.healthy);
    assert!(down.version.is_none());
    assert!(!down.database_exists);
    assert_eq!(down.database, "moraine");
    assert!(down.applied_schema_versions.is_empty());
    assert!(down.pending_schema_versions.is_empty());
    assert!(down.missing_tables.is_empty());
    assert_eq!(down.errors.len(), 1);
    assert!(down.errors[0].contains("ping failed"));
    assert!(down.errors[0].contains("doctor ping unavailable"));
    assert_script_consumed(&down_state, 1);
}
