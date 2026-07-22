use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_distinguishes_unready_and_dirty_projection_snapshots() {
    for (projection_ready, projection_clean) in [(0_u8, 1_u8), (1_u8, 0_u8)] {
        let metadata = json!({
            "row_kind": 1_u8,
            "event_uid": "",
            "session_id": "",
            "slot": 0_u8,
            "generation": 0_u64,
            "raw_score": 0.0,
            "matched_terms": 0_u64,
            "event_unix_ms": 0_i64,
            "docs": 100_u64,
            "total_doc_len": 5000_u64,
            "scope_exists": 1_u8,
            "projection_ready": projection_ready,
            "projection_clean": projection_clean
        });
        let attempts = if projection_clean == 0 { 4 } else { 1 };
        let responses = (0..attempts)
            .map(|_| ScriptedResponse::rows(&["toUInt8(0) AS row_kind"], json!([metadata.clone()])))
            .collect();
        let (repo, state) = build_scripted_repo(responses).await;

        let error = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "projection health".to_string(),
                n_hits: Some(5),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect_err("unhealthy projection must fail closed");
        if projection_ready == 0 {
            assert!(error.to_string().contains("not ready"), "{error}");
        } else {
            assert!(matches!(error, RepoError::ReadModelChanged));
        }
        assert_script_consumed(&state, attempts);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_immediate_retry_finishes_within_request_deadline() {
    let (repo, state) = build_repo_with_options(
        100,
        MockOptions {
            dirty_projection_on_first_candidate: true,
            ..MockOptions::default()
        },
    )
    .await;
    let query = || SearchMcpEventsQuery {
        query: "active ingest".to_string(),
        n_hits: Some(10),
        min_score: Some(0.0),
        min_should_match: Some(1),
        ..SearchMcpEventsQuery::default()
    };

    let budget = interactive_test_budget(4.0);
    let retry = tokio::time::timeout(
        Duration::from_secs(4),
        QueryEnvelope::new("request", QueryClass::Interactive, &budget)
            .scope(repo.search_mcp_events(query())),
    )
    .await
    .expect("bounded internal retry must finish inside the request deadline")
    .expect("dirty-then-published operation must succeed");
    assert_eq!(retry.hits.len(), 2);

    let queries = state.queries.lock().expect("queries lock");
    let candidate_queries = queries
        .iter()
        .filter(|query| {
            query.contains("toUInt8(0) AS row_kind") && query.contains("term_postings AS (")
        })
        .collect::<Vec<_>>();
    assert_eq!(candidate_queries.len(), 2);
    assert!(candidate_queries[0].contains("search_corpus_stats"));
    assert!(!candidate_queries[1].contains("search_corpus_stats"));
    assert!(candidate_queries[1].contains("tuple(toUInt64(100), toUInt64(5000)) AS corpus_stats"));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_applies_session_origin_scope() {
    let (repo, state) = build_scoped_repo(&["/work/s.origin_cwd/project"]).await;

    repo.search_mcp_events(SearchMcpEventsQuery {
        query: "hello world".to_string(),
        n_hits: Some(10),
        session_id: Some("sess_a".to_string()),
        event_types: Some(vec![
            McpEventType::UserInput,
            McpEventType::AssistantResponse,
        ]),
        harness: Some("claude-code".to_string()),
        source_name: Some("claude".to_string()),
        min_score: Some(0.0),
        min_should_match: Some(1),
        ..SearchMcpEventsQuery::default()
    })
    .await
    .expect("scoped search_mcp_events");

    let queries = state.queries.lock().expect("queries lock").clone();
    let search_query = queries
        .iter()
        .find(|q| q.contains("toUInt8(0) AS row_kind") && q.contains("AS raw_score"))
        .expect("search query should be captured");

    assert!(search_query.contains("s.origin_cwd = '/work/s.origin_cwd/project'"));
    assert!(search_query.contains("startsWith(s.origin_cwd, '/work/s.origin_cwd/project/')"));
    assert!(search_query.contains("scope_s.origin_cwd = '/work/s.origin_cwd/project'"));
    assert!(!search_query.contains("'/work/scope_s.origin_cwd/project'"));
    assert!(search_query.contains("p.harness = 'claude-code'"));
    assert!(search_query.contains("p.source_name = 'claude'"));
}
#[tokio::test(flavor = "multi_thread")]
async fn search_session_metadata_returns_summary_only_matches() {
    let (repo, state) = build_repo().await;

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

    let snapshot_queries = state
        .publication_snapshot_queries
        .lock()
        .expect("publication snapshot query lock");
    assert_eq!(snapshot_queries.len(), 2);
    assert!(snapshot_queries[0].contains("moraine:publication_snapshot:capture"));
    assert!(snapshot_queries[1].contains("moraine:publication_snapshot:revalidate"));
}
#[tokio::test(flavor = "multi_thread")]
async fn search_session_metadata_applies_time_mode_filters_and_caps_limit() {
    let (repo, state) = build_repo_with_max_results(5).await;

    let result = ConversationRepository::search_session_metadata(
        &repo,
        SessionMetadataSearchQuery {
            query: "rare summary".to_string(),
            limit: Some(25),
            min_score: Some(1.5),
            min_should_match: Some(2),
            from_unix_ms: Some(1767600000000_i64),
            to_unix_ms: Some(1767610000000_i64),
            mode: Some(ConversationMode::Chat),
            session_id: Some("sess_meta_summary".to_string()),
        },
    )
    .await
    .expect("search session metadata");

    assert_eq!(result.stats.requested_limit, 25);
    assert_eq!(result.stats.effective_limit, 5);
    assert!(result.stats.limit_capped);

    let queries = state.queries.lock().expect("queries lock").clone();
    let metadata_search_query = queries
        .iter()
        .find(|q| {
            q.contains("WHERE e.event_kind = 'session_meta'") && q.contains("AS meta_event_uid")
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

    let snapshot_queries = state
        .publication_snapshot_queries
        .lock()
        .expect("publication snapshot query lock");
    assert_eq!(snapshot_queries.len(), 2);
    assert!(snapshot_queries[0].contains("moraine:publication_snapshot:capture"));
    assert!(snapshot_queries[1].contains("moraine:publication_snapshot:revalidate"));
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

    assert!(agg_query.contains(
        "argMax(\n      tuple(e.source_host, e.event_uid),\n      tuple(e.event_score, e.event_uid, e.source_host)\n    ) AS best_event_identity"
    ));
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
            q.contains("WHERE document.event_uid IN")
                && q.contains("GROUP BY document.source_host, document.event_uid")
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

    assert!(agg_query.contains("FROM `moraine`.`v_live_search_postings` AS p"));
    assert!(agg_query.contains("WHERE p.term IN"));
    assert!(!agg_query.contains("PREWHERE"));
    assert!(agg_query.contains("bitCount(groupBitOr(e.term_mask))"));
    assert!(!agg_query.contains("JOIN `moraine`.`search_documents` AS d"));
}
#[tokio::test(flavor = "multi_thread")]
async fn search_events_includes_session_time_bounds() {
    let (repo, state) = build_repo().await;

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
    let queries = state.queries.lock().expect("queries lock");
    let df_query = queries
        .iter()
        .find(|query| query.contains("toUInt64(uniqExact") && query.contains(" AS df"))
        .expect("document-frequency query should be captured");
    assert!(
        df_query.contains("uniqExact(tuple(source_host, doc_id))"),
        "document frequency must use host-qualified document identity: {df_query}"
    );
    assert!(!df_query.contains("uniqExact(doc_id)"));
    let bounds_query = queries
        .iter()
        .find(|query| query.contains("FROM `moraine`.`v_session_summary` AS ss"))
        .expect("session time bounds query should be captured");
    assert!(
        bounds_query.contains(
            "toInt64(toUnixTimestamp64Milli(ss.first_event_time)) AS first_event_unix_ms"
        ),
        "first-event epoch must use the qualified typed source: {bounds_query}"
    );
    assert!(
        bounds_query
            .contains("toInt64(toUnixTimestamp64Milli(ss.last_event_time)) AS last_event_unix_ms"),
        "last-event epoch must use the qualified typed source: {bounds_query}"
    );
    assert!(!bounds_query.contains("toUnixTimestamp64Milli(first_event_time)"));
    assert!(!bounds_query.contains("toUnixTimestamp64Milli(last_event_time)"));
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
                McpEventType::ToolCall,
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
            McpEventType::ToolCall,
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
async fn search_mcp_events_runs_reads_under_the_envelope_request_id() {
    let (repo, state) = build_repo().await;
    let cancellation_token = "mcp-search-cancel-test";

    let budget = interactive_test_budget(15.0);
    let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
    let request_id = envelope.request_id().to_string();

    let result = envelope
        .scope(repo.search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            cancellation_token: Some(cancellation_token.to_string()),
            n_hits: Some(2),
            event_types: Some(vec![
                McpEventType::UserInput,
                McpEventType::AssistantResponse,
            ]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        }))
        .await
        .expect("envelope-scoped mcp event search");

    // The caller token is still reported in the result, but the transport
    // owns statement ids: every read runs as `{request_id}-{seq}`, never as
    // the caller token (the envelope wins over caller query_id params).
    // Spawned telemetry inserts carry their own `moraine-telemetry-` ids
    // (task-locals do not cross spawn), so restrict to the search reads.
    assert_eq!(result.query_id, cancellation_token);
    let queries = state.queries.lock().expect("queries lock").clone();
    let query_ids = state.query_ids.lock().expect("query id lock").clone();
    assert!(!query_ids.is_empty());
    let child_prefix = format!("{request_id}-");
    let observed = queries
        .iter()
        .zip(query_ids.iter())
        .filter(|(query, _)| !query.trim_start().starts_with("INSERT INTO"))
        .map(|(_, query_id)| query_id.as_deref().expect("query id"))
        .collect::<Vec<_>>();
    assert!(!observed.is_empty());
    assert!(observed
        .iter()
        .all(|query_id| query_id.starts_with(&child_prefix)));
    assert!(observed
        .iter()
        .all(|query_id| !query_id.contains(cancellation_token)));
    assert_eq!(
        observed
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        observed.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unenveloped_search_reads_run_without_budget_params_pre_flip() {
    let (repo, state) = build_repo().await;

    // Pre-flip permissive posture (amendment A2): without an active envelope
    // the statements execute exactly as before the envelope existed — no
    // query ids, no injected deadline.
    repo.search_mcp_events(SearchMcpEventsQuery {
        query: "hello world".to_string(),
        n_hits: Some(2),
        event_types: Some(vec![
            McpEventType::UserInput,
            McpEventType::AssistantResponse,
        ]),
        min_score: Some(0.0),
        min_should_match: Some(1),
        ..SearchMcpEventsQuery::default()
    })
    .await
    .expect("unenveloped search still succeeds pre-flip");

    // Spawned telemetry inserts get their own Administrative envelope even
    // when the request ran unenveloped, so restrict to the search reads.
    let queries = state.queries.lock().expect("queries lock").clone();
    let query_ids = state.query_ids.lock().expect("query id lock").clone();
    let request_params = state.request_params.lock().expect("request params lock");
    let reads = queries
        .iter()
        .enumerate()
        .filter(|(_, query)| !query.trim_start().starts_with("INSERT INTO"))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert!(!reads.is_empty());
    for index in reads {
        assert!(query_ids[index].is_none());
        assert!(!request_params[index].contains_key("max_execution_time"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn envelope_scope_passes_remaining_deadline_to_every_read() {
    let (repo, state) = build_repo().await;

    let budget = interactive_test_budget(2.0);
    QueryEnvelope::new("request", QueryClass::Interactive, &budget)
        .scope(repo.search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            cancellation_token: Some("nested-search-query".to_string()),
            n_hits: Some(2),
            event_types: Some(vec![McpEventType::AssistantResponse]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        }))
        .await
        .expect("deadline-scoped search");

    // Restrict to the two search reads: spawned telemetry inserts may or may
    // not have landed yet and carry their own administrative deadline.
    let queries = state.queries.lock().expect("queries lock").clone();
    let request_params = state.request_params.lock().expect("request params lock");
    let read_params = queries
        .iter()
        .zip(request_params.iter())
        .filter(|(query, _)| !query.trim_start().starts_with("INSERT INTO"))
        .map(|(_, params)| params)
        .collect::<Vec<_>>();
    assert_eq!(read_params.len(), 2);
    for params in read_params {
        let remaining = params["max_execution_time"]
            .parse::<f64>()
            .expect("numeric remaining ClickHouse deadline");
        assert!(remaining > 0.0 && remaining <= 2.0);
        assert_eq!(params["timeout_overflow_mode"], "throw");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_uses_one_candidate_and_one_bounded_detail_query() {
    let (repo, state) = build_repo().await;

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
        .expect("two-stage search succeeds");
    assert_eq!(result.hits.len(), 2);

    let queries = state.queries.lock().expect("queries lock");
    assert_eq!(queries.len(), 2, "cold search must issue exactly two reads");
    assert!(queries[0].contains("toUInt8(0) AS row_kind"));
    assert!(queries[0].contains("ORDER BY raw_score DESC, event_unix_ms DESC, event_uid ASC"));
    assert_eq!(
        queries[0].matches("search_corpus_stats").count(),
        1,
        "scalar corpus metadata must expand exactly once"
    );
    assert!(queries[0].contains("mcp_open_dirty_sessions"));
    assert!(queries[0].contains("WHERE notEmpty(session_id)"));
    assert!(queries[0].contains("live_session_ids AS ("));
    assert!(queries[0].contains("session_id IN (SELECT session_id FROM live_session_ids)"));
    assert!(queries[0].contains("AS projection_clean"));
    assert!(!queries[0].contains("matching_doc_ids AS ("));
    assert!(!queries[0].contains("projected_candidates AS ("));
    assert_eq!(
        queries[0]
            .matches("FROM `moraine`.`v_live_search_postings` AS p FINAL")
            .count(),
        1,
        "candidate ranking must expand the live postings view once"
    );
    assert!(queries[0].contains("ALL INNER JOIN `moraine`.`mcp_open_events` AS e FINAL"));
    assert!(queries[0].contains("ON e.source_host = p.source_host"));
    assert!(queries[0].contains("AND e.event_uid = p.doc_id"));
    assert!(queries[0].contains("AND e.session_id = s.session_id"));
    assert!(queries[0].contains("AND e.slot = s.slot"));
    assert!(queries[0].contains("AND e.generation = s.generation"));
    assert!(queries[0].contains("GROUP BY p.doc_id, p.source_host"));
    assert!(queries[0].contains("greatest(toFloat64(corpus_docs), toFloat64(p.df))"));
    assert!(!queries[0].contains("uniqExact"));
    assert!(queries[1].contains("SELECT arrayJoin(['sess_a','sess_c']) AS session_id"));
    assert!(!queries[1].contains("FROM `moraine`.`search_postings`"));
    for alias in ["h", "e", "dirty"] {
        assert!(queries[1].contains(&format!(
            "{alias}.session_id IN (SELECT session_id FROM candidate_session_ids)"
        )));
    }
    assert!(queries[1].contains("documents AS ("));
    assert!(queries[1].contains("candidate_heads AS ("));
    assert!(queries[1].contains("tupleElement(candidate, 1) AS source_host"));
    assert!(queries[1].contains("sessions.generation = candidate.generation"));
    assert!(queries[1].contains("WHERE (document.source_host, document.event_uid) IN ("));
    assert!(queries[1].contains("ON projected_events.source_host = candidate.source_host"));
    assert!(queries[1].contains("argMax(leftUTF8(document.text_content"));
    assert!(queries[1].contains("argMax(leftUTF8(document.payload_json"));
    assert!(!queries[1].contains("argMax(leftUTF8(text_content"));
    assert!(!queries[1].contains("argMax(leftUTF8(payload_json"));
    assert!(!queries[1].contains("leftUTF8(argMax(text_content"));
    assert!(!queries[1].contains("leftUTF8(argMax(payload_json"));
    assert!(
        queries.iter().all(|query| query
            .lines()
            .filter(|line| line.contains("INNER JOIN"))
            .all(|line| line.trim_start().starts_with("ALL INNER JOIN"))),
        "search stages must explicitly preserve inner-join multiplicity"
    );
    assert!(queries.iter().all(|query| {
        !query.contains("v_conversation_trace")
            && !query.contains("v_session_summary")
            && !query.contains("event_kind = 'session_meta'")
    }));
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
        .find(|q| q.contains("toUInt8(0) AS row_kind") && q.contains("p.session_id = 'sess_a'"))
        .expect("session-scoped search query should be captured");
    assert!(search_query.contains("p.session_id = 'sess_a'"));
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_turn_scoped_search() {
    let (repo, state) = build_repo().await;

    let result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "cargo failure".to_string(),
            cancellation_token: None,
            n_hits: Some(5),
            session_id: Some("sess_c".to_string()),
            turn_seq: Some(2),
            event_types: Some(vec![McpEventType::ToolResponse]),
            harness: None,
            source_name: None,
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
            q.contains("toUInt8(0) AS row_kind")
                && q.contains("e.session_id = 'sess_c' AND e.turn_seq = 2")
        })
        .expect("turn-scoped search query should be captured");
    assert!(search_query.contains("e.session_id = 'sess_c' AND e.turn_seq = 2"));
    assert!(search_query.contains("ALL INNER JOIN `moraine`.`mcp_open_turns` AS scope_t FINAL"));
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_returns_explicit_tool_event_filters() {
    let (repo, _state) = build_repo().await;

    let tool_call_result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![McpEventType::ToolCall]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("tool-call search");
    let tool_response_result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![McpEventType::ToolResponse]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("tool-response search");
    let mixed_result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![McpEventType::UserInput, McpEventType::ToolCall]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("mixed message and tool search");

    assert_eq!(tool_call_result.hits.len(), 1);
    assert_eq!(tool_call_result.hits[0].event_type, McpEventType::ToolCall);
    assert_eq!(tool_call_result.hits[0].event_uid, "evt-c-tool-call");
    assert_eq!(tool_response_result.hits.len(), 1);
    assert_eq!(
        tool_response_result.hits[0].event_type,
        McpEventType::ToolResponse
    );
    assert_eq!(tool_response_result.hits[0].event_uid, "evt-c-tool");
    assert_eq!(
        mixed_result
            .hits
            .iter()
            .map(|hit| hit.event_type)
            .collect::<Vec<_>>(),
        vec![McpEventType::ToolCall, McpEventType::UserInput]
    );
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
    let message_result = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(5),
            event_types: Some(vec![
                McpEventType::UserInput,
                McpEventType::AssistantResponse,
            ]),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("message-only search");

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
    assert!(message_result.hits.iter().all(|hit| matches!(
        hit.event_type,
        McpEventType::UserInput | McpEventType::AssistantResponse
    )));
    assert_eq!(
        message_result.event_types,
        vec![McpEventType::UserInput, McpEventType::AssistantResponse]
    );

    let queries = state.queries.lock().expect("queries lock").clone();
    assert!(queries.iter().any(|q| {
        q.contains("toUInt8(0) AS row_kind") && q.contains("lowerUTF8(p.actor_role) = 'user'")
    }));
    assert!(queries.iter().any(|q| {
        q.contains("toUInt8(0) AS row_kind") && q.contains("lowerUTF8(p.actor_role) = 'assistant'")
    }));
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_deduplicates_before_limit_and_reports_truncation() {
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
    assert_eq!(result.hits[0].event_uid, "evt-c-42");
    assert_eq!(result.hits[1].event_uid, "evt-a-11");
    assert!(result
        .hits
        .iter()
        .all(|hit| hit.event_uid != "evt-c-duplicate"));
    assert!(result.truncated);
    assert!(result.stats.truncated);
    assert_eq!(result.stats.effective_n_hits, 2);

    let queries = state.queries.lock().expect("queries lock").clone();
    let first_candidate_query = queries
        .iter()
        .find(|query| {
            query.contains("toUInt8(0) AS row_kind") && query.contains("LIMIT 3 OFFSET 0")
        })
        .expect("first bounded candidate page");
    assert!(!first_candidate_query.contains("text_content"));
    assert!(!first_candidate_query.contains("SHA256"));
    assert!(queries.iter().any(|query| {
        query.contains("toUInt8(0) AS row_kind") && query.contains("LIMIT 3 OFFSET 3")
    }));
    assert!(queries.iter().any(|query| {
        query.contains("hex(SHA256(projected_events.text_content)) AS text_content_digest")
    }));
    assert!(queries.iter().any(|query| {
        query.contains("JSONExtractString(document.payload_json, 'phase')")
            && query.contains("AS payload_phase")
    }));
    assert!(queries
        .iter()
        .any(|query| { query.contains("documents AS (") && query.contains("'evt-b-9'") }));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_rejects_projection_changes_between_candidate_pages() {
    let (repo, state) = build_repo_with_options(
        100,
        MockOptions {
            change_projection_revision_on_second_search_page: true,
            ..MockOptions::default()
        },
    )
    .await;

    let error = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(2),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect_err("candidate paging must reject a changed projection revision");

    assert!(matches!(error, RepoError::ReadModelChanged));
    let queries = state.queries.lock().expect("queries lock");
    assert!(queries
        .iter()
        .any(|query| query.contains("LIMIT 3 OFFSET 3")));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_classifies_hydration_projection_movement() {
    let (repo, _state) = build_repo_with_options(
        100,
        MockOptions {
            omit_first_mcp_detail_row: true,
            ..MockOptions::default()
        },
    )
    .await;

    let error = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(2),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect_err("missing pinned detail must report projection movement");

    assert!(matches!(error, RepoError::ReadModelChanged));
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_bounds_duplicate_candidate_pages() {
    let (repo, state) = build_repo_with_options(
        100,
        MockOptions {
            repeat_duplicate_search_pages: true,
            ..MockOptions::default()
        },
    )
    .await;

    let error = repo
        .search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(2),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect_err("duplicate paging must stop at the request work budget");

    assert!(
        error.to_string().contains("scan budget exhausted"),
        "{error}"
    );
    let queries = state.queries.lock().expect("queries lock");
    let candidate_queries = queries
        .iter()
        .filter(|query| query.contains("toUInt8(0) AS row_kind"))
        .count();
    assert_eq!(candidate_queries, 16);
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
