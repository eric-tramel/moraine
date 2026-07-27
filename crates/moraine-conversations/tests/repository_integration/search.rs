use super::*;
use moraine_conversations::ClickHouseConversationRepository;

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_distinguishes_unready_and_dirty_projection_snapshots() {
    scoped(async {
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
                .map(|_| {
                    ScriptedResponse::rows(&["toUInt8(0) AS row_kind"], json!([metadata.clone()]))
                })
                .collect();
            let (repo, state) = build_scripted_repo_with_readiness(responses, false).await;

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
    })
    .await;
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
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_returns_ranked_session_hits_and_expected_sql_shape() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_reports_capped_limit_metadata() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_falls_back_to_row_snippet_for_text_preview() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_snippet_query_avoids_self_aliased_aggregates() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_without_mode_filter_skips_mode_join() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_conversations_without_time_window_uses_postings_only_fast_path() {
    scoped(async {
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

        // Issue #597 B6: ONE document population. The scoring statement reads
        // the same locator-authorized `term_postings` relation that
        // `bounded_term_df_map` counts `df` over, not the document-authorized
        // `v_live_search_postings` view.
        assert!(agg_query.contains("FROM term_postings AS p"));
        assert!(!agg_query.contains("FROM `moraine`.`v_live_search_postings` AS p"));
        assert!(agg_query.contains("WHERE p.term IN"));
        assert!(agg_query.contains("AND l.event_version = p.post_version"));
        assert!(!agg_query.contains("PREWHERE"));
        assert!(agg_query.contains("bitCount(groupBitOr(e.term_mask))"));
        assert!(!agg_query.contains("JOIN `moraine`.`search_documents` AS d"));
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_events_includes_session_time_bounds() {
    scoped(async {
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
            bounds_query.contains(
                "toInt64(toUnixTimestamp64Milli(ss.last_event_time)) AS last_event_unix_ms"
            ),
            "last-event epoch must use the qualified typed source: {bounds_query}"
        );
        assert!(!bounds_query.contains("toUnixTimestamp64Milli(first_event_time)"));
        assert!(!bounds_query.contains("toUnixTimestamp64Milli(last_event_time)"));
    })
    .await;
}
/// WI-09, the defect the issue names: "activity in session A must never disable
/// search in session B".
///
/// The v1 engine gates every request on TWO corpus-global scalars —
/// `projection_ready` and `projection_clean` — and `projection_clean` is
/// `countIf(dirty.dirty_revision > published.dirty_revision) = 0` over EVERY
/// live session. One actively-ingesting session therefore returned
/// `ReadModelChanged` for every other session's search, and
/// `run_publication_consistent_scoped` retried the whole operation four times
/// before surfacing `internal_error`.
///
/// The v2 engine has no global gate. Validity is proven per row instead, and
/// twice: by the locator version join during ranking, and by the candidate's
/// presence at the same `event_version` in live navigation during derivation.
/// The mock's `dirty_projection_on_first_candidate` makes the projection report
/// itself dirty; under v1 that is fatal, under v2 it is not even read.
///
/// MUTATION: re-introduce either gate into `search_mcp_event_page_v2` and this
/// fails.
#[tokio::test(flavor = "multi_thread")]
async fn v2_search_is_unaffected_by_a_dirty_projection() {
    scoped(async {
        let query = || SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(2),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        };

        // v1, dirty backend: fails closed for everyone, and burns all four
        // `run_publication_consistent_scoped` attempts doing it.
        let dirty_metadata = json!({
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
            "projection_ready": 1_u8,
            "projection_clean": 0_u8
        });
        let (v1_repo, v1_state) = build_scripted_repo_with_readiness(
            (0..4)
                .map(|_| {
                    ScriptedResponse::rows(
                        &["toUInt8(0) AS row_kind"],
                        json!([dirty_metadata.clone()]),
                    )
                })
                .collect(),
            false,
        )
        .await;
        let v1_error = v1_repo
            .search_mcp_events(query())
            .await
            .expect_err("the projected-header engine fails closed on a dirty projection");
        assert_script_consumed(&v1_state, 4);
        assert!(
            matches!(v1_error, RepoError::ReadModelChanged),
            "the v1 behaviour this issue removes must still be reproducible, or \
             the v2 assertion below proves nothing: {v1_error}"
        );

        // v2, same dirty backend: serves.
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                dirty_projection_on_first_candidate: true,
                open_v2_reader_ready: Some(true),
                ..MockOptions::default()
            },
        )
        .await;
        let result = repo
            .search_mcp_events(query())
            .await
            .expect("a dirty projection cannot disable the canonical engine");

        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.hits[0].event_uid, "evt-c-42");
        assert_eq!(result.hits[1].event_uid, "evt-a-11");
        assert!(!result.incomplete_due_to_candidate_budget);
        assert!(result.scope_exists);

        // Winner-only hydration really did decorate the hits: `model` comes
        // from the bounded wide read that retired the uid-only `models` CTE,
        // and the per-turn scalars from the batched turn aggregate.
        let top = &result.hits[0];
        assert_eq!(top.model.as_deref(), Some("gpt-5.3-codex"));
        assert_eq!(top.endpoint_kind.as_deref(), Some("generation"));
        assert_eq!(top.turn_event_count, 3);
        assert!(top.turn_completed);
        assert_eq!(top.turn_terminal_event_uid.as_deref(), Some("evt-c-42"));
        assert_eq!(top.session_title.as_deref(), Some("Session C title"));
        assert_eq!(top.session_started_at_unix_ms, Some(1_767_434_400_000));

        // …and `session_completed` is the SESSION's last-turn flag
        // (`argMax(turn_completed, turn_seq)`, v1's two-level rule), NOT the
        // hit's own turn. The sess_c hit sits in turn 2, which IS complete,
        // while the session's last turn (3) is not — so a reader that reports
        // the hit's own turn as the session's state says `true` here.
        assert!(top.turn_completed);
        assert!(
            !result.hits[0].session_completed,
            "session_completed must be the session's last-turn flag, not the \
             matched turn's"
        );
        assert!(!result.hits[1].session_completed);

        let queries = state.queries.lock().expect("queries lock").clone();
        assert!(
            !queries.is_empty(),
            "the v2 engine must have issued statements"
        );
        for query in &queries {
            assert!(
                !query.contains("mcp_open_"),
                "no v2 search statement may read the projection:\n{query}"
            );
        }
        // The statement budget: 1 ranking + 1 derivation + 1 dedup keys + 4
        // winner hydration, plus a cold corpus-stats refresh. The retired loop
        // issued up to 32, and up to 129 across the ReadModelChanged retries.
        assert!(
            queries.len() <= 9,
            "bounded search must stay inside its statement budget, got {}: {queries:?}",
            queries.len()
        );
    })
    .await;
}

/// §2.3 / #539. The dedup-key read is a THIRD version check, against
/// `search_documents` at the candidate's exact `post_version`. Its absence is
/// NOT "no digest available": it means the document revision that produced the
/// posting is gone, so the posting is stale.
///
/// It also has to be a drop rather than a default, because v2 dedups BEFORE
/// hydration: `text_content` is empty on every row at that point, so
/// `mcp_search_rows_are_equivalent`'s empty-digest fallback
/// (`a.text_content == b.text_content`) would report every digest-less pair as
/// identical content and collapse unrelated events into one.
///
/// MUTATION: replace the `let Some(dedup) = … else { continue }` with a
/// defaulted empty `SearchDedupKeyRow`; this fails — the stale candidate is
/// served.
#[tokio::test(flavor = "multi_thread")]
async fn a_candidate_without_a_live_document_revision_is_dropped() {
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                omit_dedup_key_for_second_candidate: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(2),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("a stale candidate is a silent drop, never an error");

        let hits = result
            .hits
            .iter()
            .map(|hit| hit.event_uid.as_str())
            .collect::<Vec<_>>();
        assert!(
            !hits.contains(&"evt-a-11"),
            "the candidate with no live document revision must not be served: {hits:?}"
        );
        // …and the rest of the page is still served — a stale candidate is a
        // per-row drop, never a request-level failure.
        assert_eq!(hits, vec!["evt-c-42", "evt-b-9"]);
        // Not an error, and not `incomplete` either: the window was not
        // saturated, so this is the complete answer.
        assert!(!result.incomplete_due_to_candidate_budget);
    })
    .await;
}

/// F3. `oracle_exact` was the caller-facing door into the unbounded exact
/// aggregation, and it is REFUSED rather than silently downgraded to the
/// bounded path: a caller that depended on exact-scan semantics learns the
/// capability is gone instead of quietly getting different numbers.
///
/// MUTATION: delete the `SearchStrategyHint::Exact` guard in
/// `search_events_impl` and this fails — the request succeeds, and (worse) it
/// succeeds by running the bounded path under a name that promises otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn search_events_refuses_the_retired_exact_oracle_strategy() {
    scoped(async {
        let (repo, state) = build_repo().await;

        let error = repo
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
                strategy_hint: Some(SearchStrategyHint::Exact),
            })
            .await
            .expect_err("oracle_exact was retired");

        assert!(
            matches!(error, RepoError::InvalidArgument(ref message) if message.contains("#597")),
            "{error}"
        );
        // It is refused BEFORE any statement runs, so a retired strategy cannot
        // spend a corpus scan on its way to the error.
        assert!(
            state.queries.lock().expect("queries lock").is_empty(),
            "the refusal must precede every ClickHouse statement"
        );
    })
    .await;
}

/// WI-06's contract, asserted on the statements the request actually issued.
///
/// Each assertion names the single production edit that breaks it:
///
/// * dropping the locator join -> the `l.event_version = p.post_version`
///   assertion (a `search_documents` version join alone cannot see an event
///   revision whose document row never landed);
/// * moving a user filter into `term_postings` -> the `df`-CTE assertion (that
///   would silently move every BM25 score, because `df` is computed in the same
///   CTE);
/// * projecting content in ranking, or restoring the F1 fallback -> the
///   content-free assertion;
/// * restoring the refill loop -> the one-ranking-statement count.
#[tokio::test(flavor = "multi_thread")]
async fn search_events_ranks_once_from_postings_and_reads_no_content() {
    scoped(async {
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

        let queries = state.queries.lock().expect("queries lock").clone();
        let ranking: Vec<&String> = queries
            .iter()
            .filter(|query| query.contains("term_postings AS ("))
            .collect();
        assert_eq!(
            ranking.len(),
            1,
            "exactly one bounded ranking pass per request; got {}: {queries:?}",
            ranking.len()
        );
        let ranking = ranking[0];

        // The locator join, on the exact key the spec names.
        assert!(ranking.contains("FROM `moraine`.`mcp_event_locator` AS l FINAL"));
        assert!(ranking.contains("AND l.event_version = p.post_version"));
        // Published generations enter the locator scan as a tuple-IN, never a
        // join: a join blocks KeyCondition pruning on the locator primary key.
        assert!(ranking.contains(
            "AND (l.source_host, l.source_name, l.source_file, l.source_generation) IN (SELECT"
        ));
        assert!(!ranking.contains("ALL INNER JOIN (SELECT\n    history.source_host"));
        // The locator scan is pruned by the query's own posting doc ids. Without
        // this it is an O(E) index scan -- one whole-corpus scan traded for
        // another.
        assert!(ranking.contains("WHERE l.event_uid IN (\n      SELECT pruned.doc_id"));

        // `search_postings` is read WITHOUT `FINAL` and collapsed explicitly on
        // a key that CARRIES the attribution: the table's sort key is
        // `(term, doc_id, source_host)`, so `FINAL` would discard one of a
        // double-attributed uid's two rows inside the scan (issue #597 C1).
        assert!(!ranking.contains("`search_postings` AS p FINAL"));
        assert!(ranking.contains("GROUP BY p.term, p.doc_id, p.source_host, p.session_id"));

        // `df` is corpus-wide: computed inside `term_postings`, and NO user
        // filter may appear there. It counts DISTINCT DOCUMENTS, so it stays a
        // subset count of `docs` even though the relation under it is
        // attribution-grain.
        let (df_cte, projection) = ranking
            .split_once("\nSELECT\n  p.event_uid AS event_uid,")
            .expect("ranking statement has a CTE and a projection");
        assert!(df_cte.contains(
            "toUInt64(uniqExact(tuple(p.event_uid, p.source_host)) OVER (PARTITION BY p.term)) AS df"
        ));
        assert_eq!(
            df_cte.matches(" OVER (").count(),
            1,
            "the df window must be the only window in the ranking CTE: {df_cte}"
        );
        // The postings WHERE is term membership and NOTHING else: the clause
        // runs straight into the collapse's GROUP BY with no conjunct between.
        let postings_where = df_cte
            .rsplit_once("    WHERE p.term IN ['hello','world']")
            .expect("live_postings filters on term membership")
            .1;
        assert_eq!(
            postings_where.trim_start().lines().next(),
            Some("GROUP BY p.term, p.doc_id, p.source_host, p.session_id"),
            "no user filter may live inside the df CTEs, but found `{postings_where}`"
        );
        assert!(projection.contains("WHERE p.payload_type != 'token_count'"));

        // Ranking is content-free, and nothing in the request aggregates the
        // document view without a bounded identity predicate.
        for forbidden in ["text_content", "payload_json", "v_live_search_documents"] {
            assert!(
                !ranking.contains(forbidden),
                "bounded ranking must not read `{forbidden}`: {ranking}"
            );
        }
        // UNCONDITIONAL (issue #597 C3): every statement that opens the
        // document corpus is keyed by the ranked identities. There is no
        // exemption, and adding one is how a corpus-sized document read gets
        // back onto the interactive path — this assertion was relaxed once, for
        // exactly that, and the scan it was relaxed for is gone again.
        //
        // Corpus scalars do not appear here at all: `docs`/`avgdl` are read
        // from `search_corpus_stats`, which is a maintained view, names no
        // relation in the statement text, and is cached for
        // `CORPUS_STATS_CACHE_TTL` per publication token.
        for query in &queries {
            if query.contains("`v_live_search_documents`") {
                assert!(
                    query.contains("requested_documents AS requested"),
                    "every document read must be keyed by the ranked identities: {query}"
                );
            }
        }
        let corpus_stats_reads = queries
            .iter()
            .filter(|query| query.contains("`moraine`.`search_corpus_stats`"))
            .count();
        assert_eq!(
            corpus_stats_reads, 1,
            "corpus stats are read at most once per request: {queries:?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_events_documents_subquery_avoids_self_aliased_aggregates() {
    scoped(async {
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
            .filter(|q| q.contains("FROM (SELECT\n  t.source_host"))
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_global_search_with_enriched_hits() {
    scoped(async {
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
    })
    .await;
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
async fn unenveloped_search_fails_closed_without_reaching_the_server() {
    let (repo, state) = build_repo().await;

    // Post-flip (issue #600 W12): without an active envelope the transport
    // refuses the statement with a typed EnvelopeError::Missing before any
    // bytes reach the server; at the repository boundary that surfaces as a
    // backend error naming the missing envelope (a caller bug, not a budget
    // outcome).
    let error = repo
        .search_mcp_events(SearchMcpEventsQuery {
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
        .expect_err("unenveloped search must fail closed");
    match &error {
        RepoError::Backend(message) => {
            assert!(
                message.contains("no active query envelope"),
                "backend error should name the missing envelope: {message}"
            );
        }
        other => panic!("expected Backend error for a missing envelope, got {other:?}"),
    }

    // Nothing reached the mock server: fail-closed means refused client-side.
    let queries = state.queries.lock().expect("queries lock");
    assert!(
        queries.is_empty(),
        "no statement may reach the server without an envelope: {queries:?}"
    );
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
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_session_scoped_search() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_supports_turn_scoped_search() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_returns_explicit_tool_event_filters() {
    scoped(async {
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
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_event_type_filter_distinguishes_user_and_assistant_messages() {
    scoped(async {
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
            q.contains("toUInt8(0) AS row_kind")
                && q.contains("lowerUTF8(p.actor_role) = 'assistant'")
        }));
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_deduplicates_before_limit_and_reports_truncation() {
    scoped(async {
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

        assert!(
            !result.incomplete_due_to_candidate_budget,
            "a window that was NOT saturated returned the whole ranking; \
             `truncated` is not `incomplete`"
        );

        let queries = state.queries.lock().expect("queries lock").clone();
        // ONE window of `mcp_candidate_fetch_size(n_hits + 1) = 3 * 3`, no
        // OFFSET. The retired shape was `LIMIT 3 OFFSET 0` followed by
        // `LIMIT 3 OFFSET 3`, up to sixteen times.
        let candidate_queries = queries
            .iter()
            .filter(|query| query.contains("toUInt8(0) AS row_kind"))
            .collect::<Vec<_>>();
        assert_eq!(candidate_queries.len(), 1, "{queries:?}");
        let first_candidate_query = candidate_queries[0];
        assert!(first_candidate_query.contains("LIMIT 9"));
        assert!(!first_candidate_query.contains("OFFSET"));
        assert!(!first_candidate_query.contains("text_content"));
        assert!(!first_candidate_query.contains("SHA256"));
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
    })
    .await;
}

/// The retired contract was "a projection revision that moves BETWEEN candidate
/// pages is `ReadModelChanged`". There are no pages to compare any more — which
/// is the point — so this asserts the property that replaced it: ONE ranking
/// statement per request, with no `OFFSET`, even when the window is full enough
/// that the old code would have paged.
///
/// MUTATION: restore the offset loop (issue a second ranking statement with
/// `OFFSET`); the count assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_issues_exactly_one_ranking_statement() {
    scoped(async {
        // Over BOTH engines: this guard used to run only against the retired
        // v1 path, where a v2 refill-loop regression could not reach it.
        for path in SearchPath::ALL {
            let (repo, state) = path.repo().await;

            let result = repo
                .search_mcp_events(SearchMcpEventsQuery {
                    query: "hello world".to_string(),
                    n_hits: Some(2),
                    min_score: Some(0.0),
                    min_should_match: Some(1),
                    ..SearchMcpEventsQuery::default()
                })
                .await
                .unwrap_or_else(|error| panic!("{path:?}: bounded mcp event search: {error}"));
            assert!(!result.incomplete_due_to_candidate_budget, "{path:?}");

            let queries = state.queries.lock().expect("queries lock").clone();
            let ranking = queries
                .iter()
                .filter(|query| path.is_ranking_statement(query))
                .count();
            assert_eq!(
                ranking, 1,
                "{path:?}: one bounded ranking pass per request: {queries:?}"
            );
            assert!(
                !queries.iter().any(|query| query.contains(" OFFSET ")),
                "{path:?}: the refill loop's OFFSET paging must not come back: {queries:?}"
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_classifies_hydration_projection_movement() {
    scoped(async {
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
    })
    .await;
}

/// The converted refill-budget test. A saturated candidate window whose members
/// all collapse now returns the surviving valid hits plus
/// `incomplete_due_to_candidate_budget`, instead of re-running the whole ranking
/// statement 16 times and then failing with
/// `backend("duplicate scan budget exhausted")` -> wire `internal_error`.
///
/// MUTATIONS, each of which fails a named assertion here:
/// * make the marker unconditional (drop the `saturated &&` conjunct) -> the
///   non-saturated case in `search_mcp_events_issues_exactly_one_ranking_statement`
///   fails;
/// * drop the `short` conjunct -> this test's hit count is 1 < 3, so the marker
///   would still be set, but see `candidate_budget_marker_requires_both_saturation_and_shortfall`
///   for the unit-level proof of each conjunct;
/// * restore the loop -> the statement-count assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_marks_a_saturated_collapsing_window_incomplete() {
    scoped(async {
        for path in SearchPath::ALL {
            let (repo, state) = path
                .repo_with(MockOptions {
                    saturate_candidate_window: true,
                    ..MockOptions::default()
                })
                .await;

            let result = repo
                .search_mcp_events(SearchMcpEventsQuery {
                    query: "hello world".to_string(),
                    n_hits: Some(2),
                    min_score: Some(0.0),
                    min_should_match: Some(1),
                    ..SearchMcpEventsQuery::default()
                })
                .await
                .unwrap_or_else(|error| {
                    panic!("{path:?}: a saturated collapsing window is not an error: {error}")
                });

            assert!(
                result.incomplete_due_to_candidate_budget,
                "{path:?}: a saturated window that dedups short must report the budget marker"
            );
            // The hits that ARE returned are valid and are a true ranking prefix.
            assert_eq!(result.hits.len(), 1, "{path:?}");
            assert_eq!(result.hits[0].event_uid, "evt-sat-0", "{path:?}");

            let queries = state.queries.lock().expect("queries lock");
            let candidate_queries = queries
                .iter()
                .filter(|query| path.is_ranking_statement(query))
                .count();
            assert_eq!(
                candidate_queries, 1,
                "{path:?}: the 16-page refill loop must not come back"
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_reports_event_ordinal_within_turn() {
    scoped(async {
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
    })
    .await;
}

// ---------------------------------------------------------------------------
// Issue #597 B4: the engine matrix.
//
// `build_repo()` pins `open_v2_reader_ready = false`, so every MCP search
// fixture written before this issue exercises the RETIRED v1 engine and cannot
// fail on a v2 regression. The matrix below runs ONE assertion set over BOTH
// engines against ONE mock corpus (`mcp_search_detail_row`), the way #599's
// `ListPath` matrix does for session listing.
//
// MUTATION: break either engine's hit assembly and the matrix names the engine
// that broke.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchPath {
    /// `open_v2.ready = 0`: the projected-header engine, still the fallback.
    Projection,
    /// `open_v2.ready = 1`: the bounded canonical engine this issue ships.
    Canonical,
}

impl SearchPath {
    const ALL: [SearchPath; 2] = [SearchPath::Projection, SearchPath::Canonical];

    async fn repo(self) -> (ClickHouseConversationRepository, Arc<MockState>) {
        self.repo_with(MockOptions::default()).await
    }

    async fn repo_with(
        self,
        options: MockOptions,
    ) -> (ClickHouseConversationRepository, Arc<MockState>) {
        build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(self == SearchPath::Canonical),
                ..options
            },
        )
        .await
    }

    /// The ranking pass's statement signature on this engine. v1 carries the
    /// synthetic `row_kind` metadata row; v2 projects the locator's
    /// `post_version`.
    fn is_ranking_statement(self, query: &str) -> bool {
        match self {
            SearchPath::Projection => {
                query.contains("toUInt8(0) AS row_kind") && query.contains("term_postings AS (")
            }
            SearchPath::Canonical => {
                query.contains("term_postings AS (") && query.contains("AS post_version")
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_semantics_are_identical_on_both_engines() {
    scoped(async {
        for path in SearchPath::ALL {
            let (repo, _state) = path.repo().await;

            // 1. Global search, enriched hits.
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
                .unwrap_or_else(|error| panic!("{path:?}: global search: {error}"));
            assert_eq!(result.hits.len(), 2, "{path:?}");
            assert!(!result.truncated, "{path:?}");
            let top = &result.hits[0];
            assert_eq!(top.event_uid, "evt-c-42", "{path:?}");
            assert_eq!(top.session_id, "sess_c", "{path:?}");
            assert_eq!(top.event_type, McpEventType::AssistantResponse, "{path:?}");
            assert_eq!(top.source_name.as_deref(), Some("codex"), "{path:?}");
            assert_eq!(top.event_time, "2026-01-03 10:02:00", "{path:?}");
            assert_eq!(top.event_order, 42, "{path:?}");
            assert_eq!(top.turn_seq, 2, "{path:?}");
            assert_eq!(top.event_ordinal, 3, "{path:?}");
            assert_eq!(top.turn_event_count, 3, "{path:?}");
            assert_eq!(top.raw_score, 12.5, "{path:?}");
            assert_eq!(top.model.as_deref(), Some("gpt-5.3-codex"), "{path:?}");
            assert_eq!(
                top.item_id.as_deref(),
                Some("item-evt-c-42"),
                "{path:?}: the wide fields must come from the winner's own row"
            );

            // 2. Session scope.
            let scoped_result = repo
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
                .unwrap_or_else(|error| panic!("{path:?}: session-scoped search: {error}"));
            assert_eq!(scoped_result.hits.len(), 1, "{path:?}");
            assert_eq!(scoped_result.hits[0].session_id, "sess_a", "{path:?}");
            assert_eq!(scoped_result.hits[0].event_uid, "evt-a-11", "{path:?}");
            assert!(scoped_result.scope_exists, "{path:?}");

            // 3. Turn scope.
            let turn_result = repo
                .search_mcp_events(SearchMcpEventsQuery {
                    query: "cargo failure".to_string(),
                    n_hits: Some(5),
                    session_id: Some("sess_c".to_string()),
                    turn_seq: Some(2),
                    event_types: Some(vec![McpEventType::ToolResponse]),
                    min_score: Some(0.0),
                    min_should_match: Some(1),
                    ..SearchMcpEventsQuery::default()
                })
                .await
                .unwrap_or_else(|error| panic!("{path:?}: turn-scoped search: {error}"));
            assert_eq!(turn_result.hits.len(), 1, "{path:?}");
            let turn_hit = &turn_result.hits[0];
            assert_eq!(turn_hit.event_uid, "evt-c-tool", "{path:?}");
            assert_eq!(turn_hit.event_type, McpEventType::ToolResponse, "{path:?}");
            assert_eq!(turn_hit.turn_seq, 2, "{path:?}");
            assert_eq!(turn_hit.event_ordinal, 1, "{path:?}");
            assert_eq!(turn_hit.turn_event_count, 3, "{path:?}");
            assert_eq!(turn_hit.tool_name.as_deref(), Some("bash"), "{path:?}");
            assert_eq!(turn_hit.call_id.as_deref(), Some("call-bash-1"), "{path:?}");

            // 4. A turn that does not exist is `not_found`, not "zero hits".
            let missing_turn = repo
                .search_mcp_events(SearchMcpEventsQuery {
                    query: "hello world".to_string(),
                    n_hits: Some(5),
                    session_id: Some("sess_c".to_string()),
                    turn_seq: Some(97),
                    min_score: Some(0.0),
                    min_should_match: Some(1),
                    ..SearchMcpEventsQuery::default()
                })
                .await
                .unwrap_or_else(|error| panic!("{path:?}: missing-turn search: {error}"));
            assert!(
                !missing_turn.scope_exists,
                "{path:?}: a turn that does not exist must report scope_exists = false"
            );

            // 5. #539 dedup happens BEFORE the limit, and `truncated` is not
            //    `incomplete`.
            let deduped = repo
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
                .unwrap_or_else(|error| panic!("{path:?}: dedup search: {error}"));
            assert_eq!(
                deduped
                    .hits
                    .iter()
                    .map(|hit| hit.event_uid.as_str())
                    .collect::<Vec<_>>(),
                vec!["evt-c-42", "evt-a-11"],
                "{path:?}: the codex mirror must collapse into its canonical row"
            );
            assert!(deduped.truncated, "{path:?}");
            assert_eq!(deduped.stats.effective_n_hits, 2, "{path:?}");
            assert!(
                !deduped.incomplete_due_to_candidate_budget,
                "{path:?}: a window that was not saturated returned the whole \
                 ranking; `truncated` is not `incomplete`"
            );
        }
    })
    .await;
}

/// B1 / #608. `event_uid` is content-addressed over
/// `source_file|source_generation|source_line_no|source_offset|
/// record_fingerprint` and deliberately EXCLUDES `session_id`, so one uid
/// legitimately exists under two sessions — 19,846 of them on the reference
/// host. Every read after ranking is therefore keyed by
/// `(source_host, session_id, event_uid)`, and the derivation and hydration
/// statements filter on that same tuple.
///
/// This is the HYDRATION half. The ranking half — whether both attributions
/// reach hydration at all — is
/// [`a_double_attributed_uid_survives_the_ranking_collapse`].
///
/// MUTATION (any one of these; each fails this test):
///   * key `derivation_by_identity` on `(source_host, event_uid)` — the two
///     derivations collapse, one candidate is hydrated against the OTHER
///     session's turn/order and the other is dropped;
///   * key `wide_by_identity` on `(source_host, event_uid)` — one winner gets
///     the other session's `item_id`/`text`;
///   * `dedup_by_document.remove(...)` instead of `.get(...)` — the second
///     candidate is starved of its digest and dropped as if it were stale.
///
/// NOT a mutation this test detects: dropping the `(session_id, event_uid)`
/// tuple from `build_search_candidate_derivation_sql` /
/// `build_search_wide_hydration_sql`. A uid-only filter returns the SUPERSET
/// (every candidate session that carries the uid), and the maps above are
/// keyed by the triple, so the extra rows are ignored and the answer is
/// unchanged. That tuple's job is bounding the read, and its guard is the
/// shape test `canonical_search_reads_are_session_qualified`.
#[tokio::test(flavor = "multi_thread")]
async fn a_uid_attributed_to_two_sessions_hydrates_each_against_its_own_session() {
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                shared_event_uid_across_sessions: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(5),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("a double-attributed uid is two valid hits, not an error");

        let attributions = result
            .hits
            .iter()
            .map(|hit| (hit.session_id.as_str(), hit.event_uid.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            result.hits.len(),
            2,
            "both attributions must survive: {attributions:?}"
        );
        // The premise: ONE uid string, TWO sessions. If the fixture ever stops
        // being that shape, every assertion below is satisfiable by a
        // uid-keyed implementation and this test proves nothing.
        assert!(
            result.hits.iter().all(|hit| hit.event_uid == "evt-shared"),
            "the fixture must be one uid under two sessions: {attributions:?}"
        );
        let by_session = |session_id: &str| {
            result
                .hits
                .iter()
                .find(|hit| hit.session_id == session_id)
                .unwrap_or_else(|| panic!("a hit for {session_id}"))
        };
        // Each hit's derived ordering is its OWN session's, not the other's.
        let c = by_session("sess_c");
        assert_eq!(c.turn_seq, 2, "sess_c hit derived from the wrong session");
        assert_eq!(c.event_order, 42);
        assert_eq!(c.event_ordinal, 3);
        assert_eq!(c.turn_event_count, 3);
        assert_eq!(
            c.item_id.as_deref(),
            Some("item-evt-shared-c"),
            "sess_c hit hydrated from the wrong session's row"
        );

        let a = by_session("sess_a");
        assert_eq!(a.turn_seq, 1, "sess_a hit derived from the wrong session");
        assert_eq!(a.event_order, 11);
        assert_eq!(a.event_ordinal, 1);
        assert_eq!(a.turn_event_count, 1);
        assert_eq!(
            a.item_id.as_deref(),
            Some("item-evt-shared-a"),
            "sess_a hit hydrated from the wrong session's row"
        );
    })
    .await;
}

/// C1 / #608, the RANKING half — the one B1 left open.
///
/// `search_postings` physically carries BOTH attributions of a
/// double-attributed uid: `mv_search_postings` groups by `session_id`
/// (`sql/032`), so one physical line ingested into two sessions produces two
/// posting rows per term, identical in `(term, doc_id, source_host)` and
/// differing only in `session_id`.
///
/// The table is `ReplacingMergeTree(post_version) ORDER BY (term, doc_id,
/// source_host)` and **`session_id` is not in that key**, so `FROM
/// search_postings FINAL` collapses those two rows to ONE arbitrary
/// attribution inside the scan — before the locator join, before any filter,
/// and before the ranked `GROUP BY … p.session_id` could keep them apart. The
/// hydration guards above cannot see that: they only ever receive one
/// candidate. The mock models the replacement semantics, so this test observes
/// the same loss a server would produce.
///
/// MUTATION (a): `FROM {postings} AS p FINAL` in `bounded_ranking_ctes` —
/// fails here with one hit instead of two.
/// MUTATION (b): drop `p.session_id` from `live_postings`' `GROUP BY` — same.
///
/// Both mutations leave every hydration-half assertion green, which is why
/// this case is its own test rather than a paragraph in the one above.
#[tokio::test(flavor = "multi_thread")]
async fn a_double_attributed_uid_survives_the_ranking_collapse() {
    scoped(async {
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                shared_event_uid_across_sessions: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(5),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("a double-attributed uid is two valid hits, not an error");

        let attributions = result
            .hits
            .iter()
            .map(|hit| (hit.session_id.as_str(), hit.event_uid.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            attributions,
            vec![("sess_c", "evt-shared"), ("sess_a", "evt-shared")],
            "both attributions must survive the postings collapse"
        );

        // …and the reason they survived, in the statement the server saw.
        let queries = state.queries.lock().expect("queries lock").clone();
        let ranking = queries
            .iter()
            .find(|query| {
                query.contains("term_postings AS (")
                    && query.contains("toUInt64(any(p.event_version)) AS post_version")
            })
            .expect("the request issued a bounded ranking pass");
        assert!(
            !ranking.contains("`search_postings` AS p FINAL"),
            "`FINAL` collapses on a key that omits `session_id`: {ranking}"
        );
        assert!(
            ranking.contains("GROUP BY p.term, p.doc_id, p.source_host, p.session_id"),
            "the postings collapse must carry the attribution: {ranking}"
        );
    })
    .await;
}

/// B2. A scoped caller must not learn that a session outside
/// `cfg.session_scope` exists. v1 filtered its `scope_state_sql` by
/// `origin_cwd`, so an out-of-scope session id answered `scope_exists = 0` and
/// the tool returned `not_found` — indistinguishable from a session id that
/// never existed. Dropping the predicate turns that into `scope_exists = 1`
/// plus zero hits, which is a disclosure.
///
/// MUTATION: delete the scope branch in `build_search_scope_exists_sql` (or in
/// `build_search_turn_event_uids_sql` for the turn half); the mock stops seeing
/// the `argMinIfMerge(d.origin_cwd_state)` / `AS session_origin_cwd` gate,
/// answers "exists", and this fails.
#[tokio::test(flavor = "multi_thread")]
async fn scope_existence_does_not_disclose_a_session_outside_the_project_scope() {
    scoped(async {
        let (repo, _state) = build_scoped_directory_repo(&["/repo"]).await;

        for turn_seq in [None, Some(1)] {
            let result = repo
                .search_mcp_events(SearchMcpEventsQuery {
                    query: "hello world".to_string(),
                    n_hits: Some(5),
                    // `sess_a` is out of `/repo`.
                    session_id: Some("sess_a".to_string()),
                    turn_seq,
                    min_score: Some(0.0),
                    min_should_match: Some(1),
                    ..SearchMcpEventsQuery::default()
                })
                .await
                .expect("an out-of-scope session is not-found, never an error");
            assert!(
                !result.scope_exists,
                "turn_seq={turn_seq:?}: a scoped caller must not learn that an \
                 out-of-scope session exists"
            );
            assert!(result.hits.is_empty(), "turn_seq={turn_seq:?}");
        }
    })
    .await;
}

/// B3(b). The directory recall filter in ranking is NOT scope enforcement: it
/// reads `argMinIfMerge(origin_cwd_state)` off the directory, while the hit's
/// scope is decided by the navigation `argMinIf(cwd, …)`. The two disagree
/// whenever a later generation carries a different `cwd`, so a candidate the
/// recall filter admitted must still be re-checked exactly.
///
/// MUTATION: delete the `if let Some(scope) = scope.as_ref()` block in
/// `search_mcp_event_page_v2`; this fails — the out-of-scope hit is served.
#[tokio::test(flavor = "multi_thread")]
async fn project_scope_is_re_checked_exactly_after_ranking() {
    scoped(async {
        let (repo, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_cwd_for_second_candidate: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(5),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("an out-of-scope candidate is a drop, never an error");

        let hits = result
            .hits
            .iter()
            .map(|hit| hit.event_uid.as_str())
            .collect::<Vec<_>>();
        assert!(
            !hits.contains(&"evt-a-11"),
            "a candidate whose exact origin cwd is outside the configured \
             scope must be dropped: {hits:?}"
        );
        assert!(hits.contains(&"evt-c-42"), "{hits:?}");
    })
    .await;
}

/// B3(c). Above `MAX_TURN_SCOPE_UIDS` the ranking pass cannot carry the turn's
/// uid literal set, so ranking recall degrades to `p.session_id = '…'` and the
/// TURN is enforced only by the exact Phase 4 re-check against the derived
/// `turn_seq`.
///
/// MUTATION: delete the `if let Some(turn_seq) = turn_seq { … }` re-check in
/// `search_mcp_event_page_v2`; this fails — an event from turn 2 is served for
/// a turn-3 request.
#[tokio::test(flavor = "multi_thread")]
async fn turn_scope_is_re_checked_exactly_when_the_uid_set_overflows() {
    scoped(async {
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                turn_scope_uid_overflow: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(5),
                session_id: Some("sess_c".to_string()),
                // `evt-c-42` lives in turn 2.
                turn_seq: Some(3),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("turn scope above the uid cap is correct, not an error");

        // The turn EXISTS (the derivation returned uids), so this is zero hits
        // rather than not-found.
        assert!(result.scope_exists);
        assert!(
            result.hits.is_empty(),
            "an event outside the requested turn must never be served: {:?}",
            result
                .hits
                .iter()
                .map(|hit| (hit.event_uid.as_str(), hit.turn_seq))
                .collect::<Vec<_>>()
        );

        // …and the ranking statement really did fall back to session recall,
        // or the re-check above would be unreachable and this test vacuous.
        let queries = state.queries.lock().expect("queries lock").clone();
        let ranking = queries
            .iter()
            .find(|query| query.contains("term_postings AS (") && query.contains("AS post_version"))
            .expect("a ranking statement");
        assert!(ranking.contains("p.session_id = 'sess_c'"), "{ranking}");
        assert!(
            !ranking.contains("p.event_uid IN ["),
            "the uid literal set must be dropped above the cap:\n{ranking}"
        );
    })
    .await;
}

/// B3(d) / #539. Two genuinely different events that share session, turn, event
/// type and timestamp are kept apart by the content digest and by nothing else.
/// v2 dedups BEFORE hydration, so `text_content` is empty on every row at that
/// point and `mcp_search_rows_are_equivalent`'s empty-digest fallback
/// (`a.text_content == b.text_content`) would report them identical.
///
/// The failure a missing digest produces is therefore a MASS COLLAPSE, not a
/// missed collapse — which is why it needs its own fixture rather than riding
/// on the mirror-collapse tests.
///
/// MUTATION: set `text_content_digest: String::new()` in
/// `canonical_candidate_row` (or stop projecting `d.text_digest` in
/// `build_search_dedup_keys_sql`); this fails with one hit instead of two.
#[tokio::test(flavor = "multi_thread")]
async fn two_distinct_events_in_one_turn_are_kept_apart_by_the_dedup_digest() {
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                two_distinct_events_in_one_turn: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(5),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("two distinct events are two hits");

        let mut hits = result
            .hits
            .iter()
            .map(|hit| hit.event_uid.as_str())
            .collect::<Vec<_>>();
        hits.sort_unstable();
        assert_eq!(
            hits,
            vec!["evt-c-42", "evt-c-twin"],
            "two events that differ only in content must not collapse"
        );
    })
    .await;
}

/// B3(e). The candidate derivation is a SECOND, INDEPENDENT version check: the
/// locator and the navigation index are maintained by two different
/// materialized views from the same `events` insert block, so a candidate the
/// locator authorized at `post_version` that navigation carries at a different
/// `event_version` is a proven mid-flight row and must be dropped.
///
/// MUTATION: delete the `derived.event_version != candidate.post_version`
/// guard; this fails — the stale candidate is served.
#[tokio::test(flavor = "multi_thread")]
async fn a_candidate_navigation_carries_at_another_version_is_dropped() {
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                stale_navigation_version_for_second_candidate: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(5),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("a version-drifted candidate is a drop, never an error");

        let hits = result
            .hits
            .iter()
            .map(|hit| hit.event_uid.as_str())
            .collect::<Vec<_>>();
        assert!(
            !hits.contains(&"evt-a-11"),
            "a candidate navigation carries at another version must be \
             dropped: {hits:?}"
        );
        assert!(hits.contains(&"evt-c-42"), "{hits:?}");
    })
    .await;
}

/// B6, corrected by C2/C4. ONE BM25 document population, ONE corpus
/// statement, ONE cache slot — on every backend.
///
/// `docs`/`avgdl` come from `search_corpus_stats` for `search_events`,
/// `search_conversations`, v2 MCP search and v1 MCP search alike. The design
/// (§2.6, OQ-2) decided against additionally semi-joining `mcp_event_locator`
/// there: it is an O(D)x O(E) scan for two scalars, on the interactive path,
/// and it was briefly shipped — that is correction C2. `df` therefore counts a
/// locator-authorized SUBSET of `docs`, and `bm25_sum_expression`'s
/// `greatest(corpus_docs, df)` is what absorbs the MV-lag window in which a
/// term's `df` can transiently exceed `docs`.
///
/// Because there is one population there is one cache slot, and a single
/// `Option` is the correct shape rather than a two-way eviction race (C4).
///
/// MUTATION: reintroduce a second corpus statement (a
/// `build_live_corpus_stats_sql`, or a readiness-latch branch that reads a
/// differently-authorized relation); the corpus-relation assertion fails
/// naming the statement.
#[tokio::test(flavor = "multi_thread")]
async fn bounded_search_reads_one_corpus_statement_on_every_backend() {
    scoped(async {
        for reader_ready in [false, true] {
            let (repo, state) = build_repo_with_options(
                100,
                MockOptions {
                    open_v2_reader_ready: Some(reader_ready),
                    ..MockOptions::default()
                },
            )
            .await;

            let conversations = repo
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
                .unwrap_or_else(|error| panic!("ready={reader_ready}: {error}"));
            assert_eq!(
                conversations.stats.docs, DOCUMENT_AUTHORIZED_DOCS,
                "ready={reader_ready}: conversation search takes the one \
                 corpus-scalar pair"
            );
            assert_eq!(conversations.stats.avgdl, 50.0, "ready={reader_ready}");

            let events = repo
                .search_events(SearchEventsQuery {
                    query: "hello world".to_string(),
                    limit: Some(10),
                    min_score: Some(0.0),
                    min_should_match: Some(1),
                    ..SearchEventsQuery::default()
                })
                .await
                .unwrap_or_else(|error| panic!("ready={reader_ready}: {error}"));
            assert_eq!(
                events.stats.docs, DOCUMENT_AUTHORIZED_DOCS,
                "ready={reader_ready}: so does event search"
            );
            assert_eq!(events.stats.avgdl, 50.0, "ready={reader_ready}");

            // …from exactly ONE statement, over exactly ONE relation, shared
            // by both calls through the publication-token-keyed cache.
            let queries = state.queries.lock().expect("queries lock").clone();
            let corpus_reads: Vec<&String> = queries
                .iter()
                .filter(|query| query.contains("AS total_doc_len"))
                .collect();
            assert_eq!(
                corpus_reads.len(),
                1,
                "ready={reader_ready}: corpus scalars are read once per \
                 publication revision, not once per call: {queries:?}"
            );
            assert!(
                corpus_reads[0].contains("FROM `moraine`.`search_corpus_stats`"),
                "ready={reader_ready}: the corpus statement is the maintained \
                 view, not a second corpus-sized relation: {}",
                corpus_reads[0]
            );
            for query in &queries {
                assert!(
                    !(query.contains("AS total_doc_len")
                        && (query.contains("`mcp_event_locator`")
                            || query.contains("`v_live_search_documents`"))),
                    "ready={reader_ready}: corpus scalars must not open a \
                     second corpus-sized relation on the interactive path: {query}"
                );
            }
        }
    })
    .await;
}

/// C4, the other half. The corpus-stats cache is a SINGLE slot, and that is
/// correct precisely because there is one population: the entry conversation
/// search writes is the entry v1 MCP search reads, so the slot is shared
/// rather than fought over.
///
/// The v1 engine inlines `search_corpus_stats` into its own ranking statement
/// only when the cache is COLD, which is what makes the reuse observable in
/// the statement text.
///
/// MUTATION: give the cache entry a per-caller identity again (or make
/// `cached_corpus_stats` always miss); v1 re-reads the corpus and this fails.
#[tokio::test(flavor = "multi_thread")]
async fn the_corpus_stats_cache_slot_is_shared_by_every_search_path() {
    scoped(async {
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(false),
                ..MockOptions::default()
            },
        )
        .await;

        // Prime the slot from the v2 side.
        let conversations = repo
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
            .expect("conversation search");
        assert_eq!(conversations.stats.docs, DOCUMENT_AUTHORIZED_DOCS);

        // v1 MCP search reads the SAME slot.
        let mcp = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(2),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("v1 mcp search");
        assert_eq!(mcp.stats.docs, DOCUMENT_AUTHORIZED_DOCS);

        let queries = state.queries.lock().expect("queries lock").clone();
        let v1_ranking: Vec<&String> = queries
            .iter()
            .filter(|query| {
                query.contains("toUInt8(0) AS row_kind") && query.contains("term_postings AS (")
            })
            .collect();
        assert_eq!(v1_ranking.len(), 1, "{queries:?}");
        assert!(
            !v1_ranking[0].contains("search_corpus_stats"),
            "v1 must reuse the primed slot instead of inlining a second \
             corpus read: {}",
            v1_ranking[0]
        );
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.contains("FROM `moraine`.`search_corpus_stats`"))
                .count(),
            1,
            "one corpus read for both engines: {queries:?}"
        );
    })
    .await;
}
