use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::ClickHouseConfig;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig, SessionOriginScope};
use serde_json::json;
use tokio::sync::Notify;

use super::responses::{json_each_row, trace_event_row, turn_summary_row};

#[derive(Clone)]
pub(crate) struct ScriptedBarrier {
    pub(crate) reached: Arc<Notify>,
    pub(crate) release: Arc<Notify>,
}

#[derive(Clone)]
pub(crate) struct QueryBarrier {
    pub(crate) required: Vec<&'static str>,
    pub(crate) reached: Arc<Notify>,
    pub(crate) release: Arc<Notify>,
}

#[derive(Clone)]
pub(crate) struct ScriptedResponse {
    pub(crate) required: Vec<&'static str>,
    pub(crate) forbidden: Vec<&'static str>,
    pub(crate) status: StatusCode,
    pub(crate) body: String,
    pub(crate) barrier: Option<ScriptedBarrier>,
}

impl ScriptedResponse {
    pub(crate) fn rows(required: &[&'static str], rows: serde_json::Value) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::OK,
            barrier: None,
            body: json_each_row(rows),
        }
    }

    pub(crate) fn raw(required: &[&'static str], body: impl Into<String>) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::OK,
            barrier: None,
            body: body.into(),
        }
    }

    pub(crate) fn failure(required: &[&'static str], message: &'static str) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
            barrier: None,
            body: message.to_string(),
        }
    }

    pub(crate) fn forbidding(mut self, forbidden: &[&'static str]) -> Self {
        self.forbidden = forbidden.to_vec();
        self
    }

    pub(crate) fn blocked(mut self, reached: Arc<Notify>, release: Arc<Notify>) -> Self {
        self.barrier = Some(ScriptedBarrier { reached, release });
        self
    }
}

#[derive(Clone, Default)]
pub(crate) struct MockOptions {
    pub(crate) omit_second_snippet_row: bool,
    pub(crate) scripted_responses: Vec<ScriptedResponse>,
    pub(crate) unordered_scripted_responses: bool,
    pub(crate) query_barrier: Option<QueryBarrier>,
}

#[derive(Default)]
pub(crate) struct MockState {
    pub(crate) queries: Mutex<Vec<String>>,
    pub(crate) options: MockOptions,
    pub(crate) scripted_responses: Mutex<Option<VecDeque<ScriptedResponse>>>,
}

pub(crate) fn test_clickhouse_config(url: String) -> ClickHouseConfig {
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
pub(crate) async fn spawn_mock_server(options: MockOptions) -> (String, Arc<MockState>) {
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

        if let Some(barrier) = state.options.query_barrier.clone() {
            if barrier
                .required
                .iter()
                .all(|required| query.contains(*required))
            {
                barrier.reached.notify_one();
                barrier.release.notified().await;
            }
        }

        let scripted_response = {
            let mut scripted = state
                .scripted_responses
                .lock()
                .expect("scripted response lock");
            scripted.as_mut().map(|responses| {
                if state.options.unordered_scripted_responses {
                    responses
                        .iter()
                        .position(|response| {
                            response
                                .required
                                .iter()
                                .all(|required| query.contains(*required))
                                && response
                                    .forbidden
                                    .iter()
                                    .all(|forbidden| !query.contains(*forbidden))
                        })
                        .and_then(|position| responses.remove(position))
                } else {
                    responses.pop_front()
                }
            })
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

        if query.starts_with("INSERT INTO `moraine`.`file_attention_project_roots`") {
            return (StatusCode::OK, String::new());
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
            && !query.contains("JSONExtractString(input_json")
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
                        "tool_name": "read",
                        "tool_phase": "request",
                        "matched_path": "crates/foo.rs",
                        "match_kind": "path_suffix",
                        "worktree_root": "/legacy",
                        "cwd": "/legacy",
                        "event_unix_ms": 1769940000000_i64,
                        "event_order": 10_u64,
                        "turn_seq": 1_u32,
                        "input_preview": "{\"path\":\"crates/foo.rs\"}",
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
                            "last_actor_role": "system",
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

        if query.contains("FROM `moraine`.`v_session_summary` AS ss")
            && query.contains("WHERE session_id IN")
            && query.contains("toString(ss.first_event_time) AS first_event_time")
            && query.contains("toString(ss.last_event_time) AS last_event_time")
            && query.contains("toUnixTimestamp64Milli(ss.first_event_time)")
            && query.contains("toUnixTimestamp64Milli(ss.last_event_time)")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "last_event_time": "2026-01-03 10:10:00",
                        "first_event_unix_ms": 1_767_434_400_000_i64,
                        "last_event_unix_ms": 1_767_435_000_000_i64
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "last_event_time": "2026-01-01 10:10:00",
                        "first_event_unix_ms": 1_767_261_600_000_i64,
                        "last_event_unix_ms": 1_767_262_200_000_i64
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
                            "event_unix_ms": 1_767_434_520_000_i64,
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
                        "event_unix_ms": 1_767_434_400_000_i64,
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
                        "event_unix_ms": 1_767_434_460_000_i64,
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
                        "event_unix_ms": 1_767_434_520_000_i64,
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
                                "event_unix_ms": 1_767_434_400_000_i64,
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
                            "event_unix_ms": 1_767_434_520_000_i64,
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
                            "event_unix_ms": 1_767_434_400_000_i64,
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

pub(crate) async fn build_repo_with_max_results(
    max_results: u16,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(max_results, MockOptions::default()).await
}

pub(crate) async fn build_repo_with_options(
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

pub(crate) async fn build_scripted_repo(
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

pub(crate) async fn build_unordered_scripted_repo(
    scripted_responses: Vec<ScriptedResponse>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            scripted_responses,
            unordered_scripted_responses: true,
            ..MockOptions::default()
        },
    )
    .await
}
pub(crate) async fn build_repo() -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_max_results(100).await
}

/// Repository with a `--project-only` session origin scope configured.
pub(crate) async fn build_scoped_repo(
    roots: &[&str],
) -> (ClickHouseConversationRepository, Arc<MockState>) {
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
