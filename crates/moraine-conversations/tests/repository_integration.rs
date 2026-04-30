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
    ConversationMode, ConversationRepository, ConversationSearchQuery, McpEventType, PageRequest,
    RepoConfig, RepoError, SearchEventKind, SearchEventsQuery, SearchMcpEventsQuery,
    SessionEventsDirection, SessionEventsQuery, SessionMetadataSearchQuery,
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

        if query.contains("FROM `moraine`.`events`")
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
            && query.contains("FROM `moraine`.`events` AS e")
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

        if ((query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("ORDER BY event_order ASC"))
            || (query.contains("FROM `moraine`.`events`")
                && query.contains("ORDER BY resolved_event_time")))
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

        if query.contains("FROM `moraine`.`events`")
            && query.contains("WHERE session_id = 'sess-incomplete'")
            && query.contains("ORDER BY resolved_event_time")
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

        if query.contains("FROM `moraine`.`events`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query.contains("ORDER BY resolved_event_time")
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
        query.contains("FROM `moraine`.`events`")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("ORDER BY resolved_event_time")
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
            q.contains("FROM `moraine`.`events` AS e")
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
