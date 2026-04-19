use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    ClickHouseConversationRepository, ConversationListFilter, ConversationListSort,
    ConversationMode, ConversationRepository, ConversationSearchQuery, PageRequest, RepoConfig,
    RepoError, SearchEventKind, SearchEventsQuery, SessionEventsDirection, SessionEventsQuery,
};
use serde_json::json;

#[derive(Clone, Default)]
struct MockOptions {
    omit_second_snippet_row: bool,
}

#[derive(Default)]
struct MockState {
    queries: Mutex<Vec<String>>,
    options: MockOptions,
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
            && query.contains("argMin(event_uid, tuple(event_ts, event_order, event_uid))")
            && query.contains("argMax(actor_role, tuple(event_ts, event_order, event_uid))")
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

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("WHERE s.session_id IN")
            && query.contains("toString(s.first_event_time) AS first_event_time")
            && query.contains("toString(s.last_event_time) AS last_event_time")
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

        if query.contains("GROUP BY e.session_id") {
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

    let state = Arc::new(MockState {
        queries: Mutex::default(),
        options,
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

async fn build_repo() -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_max_results(100).await
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
        .find(|q| q.contains("FROM `moraine`.`v_session_summary` AS s"))
        .expect("list query should be captured");

    assert!(list_query.contains("ifNull(m.mode, 'chat') = 'web_search'"));
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
        .find(|q| q.contains("argMin(event_uid, tuple(event_ts, event_order, event_uid))"))
        .expect("session metadata query should be captured");
    assert!(metadata_query.contains("argMax(actor_role, tuple(event_ts, event_order, event_uid))"));
    assert!(metadata_query.contains("WHERE s.session_id = 'sess_c'"));
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
            min_score: Some(0.0),
            min_should_match: Some(1),
            include_tool_events: Some(true),
            event_kinds: None,
            exclude_codex_mcp: Some(false),
            disable_cache: Some(true),
            search_strategy: None,
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
            min_score: Some(0.0),
            min_should_match: Some(1),
            include_tool_events: Some(true),
            event_kinds: None,
            exclude_codex_mcp: Some(false),
            disable_cache: Some(true),
            search_strategy: None,
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
