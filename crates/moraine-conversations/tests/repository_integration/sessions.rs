use super::*;

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
        harness: Some("codex".to_string()),
        source_name: Some("codex".to_string()),
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
    assert_eq!(first.items[0].harness.as_deref(), Some("codex"));
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
    assert!(list_query.contains("ifNull(r.mode, 'chat') = 'web_search'"));
    assert!(list_query.contains("r.latest_harness = 'codex'"));
    assert!(list_query.contains("r.latest_source_name = 'codex'"));
    assert!(list_query.contains("ifNull(r.latest_source_name, '') AS source"));
    assert!(list_query.contains("ORDER BY w.last_event_unix_ms DESC, w.session_id DESC"));
    assert!(list_query.contains("payload_type IN ('task_complete', 'turn_aborted')"));
    // Blank session_id rows are filtered at the source so they never consume a
    // LIMIT slot or anchor the keyset cursor (#386).
    assert!(list_query.contains("notEmpty(trimBoth(s.session_id))"));
    assert!(list_query.contains("window_sessions AS ("));
    assert!(list_query.contains("candidate_sessions AS ("));
    assert_eq!(
        list_query
            .matches("session_id IN (SELECT session_id FROM window_sessions)")
            .count(),
        1,
        "all event-backed fields must share one window-bounded aggregate"
    );
}
#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_rejects_cursor_filter_mismatch() {
    let (repo, _state) = build_repo().await;

    let base_filter = McpSessionListFilter {
        start_unix_ms: 1767261600000_i64,
        end_unix_ms: 1767500000000_i64,
        mode: Some(ConversationMode::WebSearch),
        harness: None,
        source_name: None,
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
                ..base_filter.clone()
            },
            PageRequest {
                limit: 1,
                cursor: Some(cursor.clone()),
            },
        )
        .await
        .expect_err("filter mismatch should fail");

    assert_eq!(
        err.to_string(),
        "invalid cursor: cursor does not match current list_sessions filter"
    );

    let err = repo
        .list_mcp_sessions(
            McpSessionListFilter {
                source_name: Some("__none__".to_string()),
                ..base_filter
            },
            PageRequest {
                limit: 1,
                cursor: Some(cursor),
            },
        )
        .await
        .expect_err("absent and literal sentinel source filters must not share a cursor");

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
            harness: None,
            source_name: None,
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
    let window_sessions = list_query
        .split_once("event_rollups AS (")
        .map(|(window, _)| window)
        .expect("list query must define event rollups after the session window");
    assert!(window_sessions.contains("ORDER BY s.last_event_time DESC, s.session_id DESC"));
    assert!(window_sessions.contains("LIMIT 6"));
}
#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_rejects_cursor_from_differently_scoped_server() {
    let (unscoped_repo, _state) = build_repo().await;
    let (scoped_repo, _scoped_state) = build_scoped_repo(&["/work/project"]).await;

    let filter = McpSessionListFilter {
        start_unix_ms: 1767261600000_i64,
        end_unix_ms: 1767500000000_i64,
        mode: Some(ConversationMode::WebSearch),
        harness: None,
        source_name: None,
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
    let legacy_gate_count = queries
        .iter()
        .filter(|query| {
            query.starts_with("SELECT session_id FROM (")
                && query.contains("argMin(cwd, tuple(event_ts, event_uid))")
        })
        .count();
    assert_eq!(
        legacy_gate_count, 1,
        "only the legacy metadata lookup should need the canonical scope gate"
    );
    assert!(
        queries
            .iter()
            .filter(|query| {
                query.contains("FROM `moraine`.`mcp_open_sessions`")
                    && query.contains("session_id = 'sess-out-of-scope'")
            })
            .count()
            >= 3
    );
    assert!(
        !queries
            .iter()
            .any(|query| query.contains("FROM `moraine`.`mcp_open_turns`")),
        "an out-of-scope committed session must be rejected before child rows are read"
    );
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

    let listed_turns = repo
        .list_turns(
            "sess-open",
            TurnListFilter::default(),
            PageRequest::default(),
        )
        .await
        .expect("turn list projection succeeds");
    assert_eq!(listed_turns.items.len(), 2);

    let opened_turn = repo
        .get_turn("sess-incomplete", 2)
        .await
        .expect("turn detail projection succeeds")
        .expect("turn detail exists");
    assert_eq!(opened_turn.summary.turn_seq, 2);

    let queries = state.queries.lock().expect("queries lock").clone();
    let open_turn_query = queries
        .iter()
        .find(|query| {
            query.contains("FROM `moraine`.`mcp_open_turns`")
                && query.contains("WHERE t.session_id = 'sess-open'")
        })
        .expect("session open must read its committed turn projection");
    assert!(open_turn_query.contains("t.slot = 0 AND t.generation = 100"));
    assert!(open_turn_query.contains("ORDER BY t.turn_seq ASC"));
    assert!(!open_turn_query.contains("v_conversation_trace"));
    assert!(!queries.iter().any(|query| {
        query.contains("v_conversation_trace")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
    }));
    let legacy_turn_summary_queries = queries
        .iter()
        .filter(|query| query.contains("FROM `moraine`.`v_turn_summary`"))
        .collect::<Vec<_>>();
    assert_eq!(
        legacy_turn_summary_queries.len(),
        2,
        "only the explicit turn-list and turn-detail calls use legacy detail projections"
    );
    for query in legacy_turn_summary_queries {
        assert_typed_turn_timestamp_projection(query);
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_uses_only_bounded_projection_queries() {
    let (repo, state) = build_repo().await;

    let session = repo
        .get_mcp_session("sess-open")
        .await
        .expect("bounded session open succeeds")
        .expect("session exists");
    assert_eq!(session.metadata.session_id, "sess-open");

    let queries = state.queries.lock().expect("queries lock").clone();
    assert_eq!(queries.len(), 4);
    assert!(queries[0].contains("mcp_open_projection_state"));
    assert!(queries[1].contains("FROM `moraine`.`mcp_open_sessions`"));
    assert!(queries[1].contains("WHERE s.session_id = 'sess-open'"));
    assert!(queries[2].contains("FROM `moraine`.`mcp_open_turns`"));
    assert!(queries[2]
        .contains("WHERE t.session_id = 'sess-open' AND t.slot = 0 AND t.generation = 100"));
    assert!(queries[3].contains("FROM `moraine`.`mcp_open_sessions`"));
    assert!(queries
        .iter()
        .all(|query| !query.contains("v_conversation_trace") && !query.contains("events FINAL")));
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_retries_when_projection_head_changes_during_open() {
    let mut generation_100 = session_row("sess-open").expect("fixture session");
    generation_100["generation"] = json!(100_u64);
    let mut generation_101 = generation_100.clone();
    generation_101["slot"] = json!(1_u8);
    generation_101["generation"] = json!(101_u64);
    let responses = vec![
        ScriptedResponse::rows(
            &["mcp_open_projection_state", "state_key = 'global'"],
            json!([{ "ready": 1_u8 }]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-open'",
            ],
            json!([generation_100]),
        ),
        ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_open_turns`", "t.generation = 100"],
            json!(turn_rows("sess-open", None)),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-open'",
            ],
            json!([generation_101.clone()]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-open'",
            ],
            json!([generation_101.clone()]),
        ),
        ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_open_turns`", "t.generation = 101"],
            json!(turn_rows("sess-open", None)),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-open'",
            ],
            json!([generation_101]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let session = repo
        .get_mcp_session("sess-open")
        .await
        .expect("snapshot retry succeeds")
        .expect("session exists");

    assert_eq!(session.turns.len(), 2);
    assert_script_consumed(&state, 7);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_missing_committed_header_skips_child_queries() {
    let responses = vec![
        ScriptedResponse::rows(
            &["mcp_open_projection_state", "state_key = 'global'"],
            json!([{ "ready": 1_u8 }]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "WHERE s.session_id = 'sess-missing-projection'",
            ],
            json!([]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let session = repo
        .get_mcp_session("sess-missing-projection")
        .await
        .expect("missing committed header is a not-found result");
    assert!(session.is_none());
    assert_script_consumed(&state, 2);
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
    assert_eq!(turn.parent_session_source.as_deref(), Some("fixture"));
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
    assert_eq!(
        turn.snapshot.as_ref().map(|snapshot| snapshot.generation),
        Some(100)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_turn_summary_skips_projected_event_json_and_keeps_handles() {
    let (repo, state) = build_repo().await;

    let turn = repo
        .get_mcp_turn_summary("sess-incomplete", 2)
        .await
        .expect("mcp turn summary succeeds")
        .expect("mcp turn exists");

    assert!(turn.events.is_empty());
    assert_eq!(
        turn.user_input_event
            .as_ref()
            .map(|event| event.event_uid.as_str()),
        Some("evt-inc-2")
    );
    assert!(turn.final_response_event.is_none());
    assert_eq!(turn.tools_called, vec!["inspect"]);
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

    let queries = state.queries.lock().expect("queries lock").clone();
    let turn_query = queries
        .iter()
        .find(|query| {
            query.contains("FROM `moraine`.`mcp_open_turns`")
                && query.contains("t.session_id = 'sess-incomplete'")
        })
        .expect("turn query captured");
    assert!(turn_query.contains("'[]' AS event_summaries_json"));
    assert!(!turn_query.contains("  event_summaries_json AS event_summaries_json"));
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_event_retries_stale_lookup_generation() {
    let mut stale_lookup = event_lookup("evt-open-full").expect("fixture event lookup");
    stale_lookup["generation"] = json!(100_u64);
    let mut current_lookup = stale_lookup.clone();
    current_lookup["generation"] = json!(101_u64);
    let mut current_session = session_row("sess-event").expect("fixture session");
    current_session["generation"] = json!(101_u64);
    let responses = vec![
        ScriptedResponse::rows(
            &["mcp_open_projection_state", "state_key = 'global'"],
            json!([{ "ready": 1_u8 }]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_events` FINAL",
                "event_uid = 'evt-open-full'",
            ],
            json!([stale_lookup]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-event'",
            ],
            json!([current_session.clone()]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_events` FINAL",
                "event_uid = 'evt-open-full'",
            ],
            json!([current_lookup]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-event'",
            ],
            json!([current_session.clone()]),
        ),
        ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_open_events`", "previous_event_uid"],
            json!([full_event_row("evt-open-full").expect("fixture full event")]),
        ),
        ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_open_turns`", "t.generation = 101"],
            json!(turn_rows("sess-event", Some(1))),
        ),
        ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_open_events` FINAL", "event_uid IN"],
            json!(event_ref_rows()),
        ),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`mcp_open_sessions`",
                "s.session_id = 'sess-event'",
            ],
            json!([current_session]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let event = repo
        .get_mcp_event("evt-open-full")
        .await
        .expect("stale event lookup retries")
        .expect("event exists");

    assert_eq!(event.event.event_uid, "evt-open-full");
    assert_script_consumed(&state, 9);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_event_returns_full_content_and_navigation_refs() {
    let (repo, state) = build_repo().await;

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

    let queries = state.queries.lock().expect("queries lock").clone();
    assert_eq!(queries.len(), 7);
    assert!(queries.iter().all(|query| {
        query.contains("mcp_open_projection_state")
            || query.contains("mcp_open_sessions")
            || query.contains("mcp_open_turns")
            || query.contains("mcp_open_events")
    }));
    assert!(queries
        .iter()
        .all(|query| !query.contains("v_conversation_trace")));
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
    assert!(first.items.iter().all(|event| event.event_unix_ms > 0));
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
    assert!(
        initial_query.contains("toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms")
    );

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
