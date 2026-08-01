use super::*;
use moraine_conversations::ClickHouseConversationRepository;

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_applies_session_origin_scope() {
    scoped(async {
        let (repo, state) = build_scoped_repo(&["/work/s.origin_cwd/project"]).await;

        // The canonical engine splits the scope decision: the session's
        // origin roots gate the Phase 0 scope-existence point read (which
        // answers `scope_exists = false` here, because the fixture session's
        // origin is outside the configured root), while the exact
        // harness/source filters ride the bounded ranking pass of a global
        // search on the same scoped repository.
        let scoped_result = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(10),
                session_id: Some("sess_a".to_string()),
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
        assert!(
            !scoped_result.scope_exists,
            "an out-of-root session must read as absent under the scoped repo"
        );

        repo.search_mcp_events(SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(10),
            harness: Some("claude-code".to_string()),
            source_name: Some("claude".to_string()),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("filtered global search_mcp_events");

        let queries = state.queries.lock().expect("queries lock").clone();
        let scope_query = queries
            .iter()
            .find(|q| q.contains("AS scope_exists"))
            .expect("scope-existence query should be captured");
        assert!(scope_query.contains("scoped.origin_cwd = '/work/s.origin_cwd/project'"));
        assert!(
            scope_query.contains("startsWith(scoped.origin_cwd, '/work/s.origin_cwd/project/')")
        );
        let ranking_query = queries
            .iter()
            .find(|q| q.contains("term_postings AS (") && q.contains("AS raw_score"))
            .expect("ranking query should be captured");
        assert!(ranking_query.contains("p.harness = 'claude-code'"));
        assert!(ranking_query.contains("p.source_name = 'claude'"));
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
/// The retired v1 engine gated every request on two corpus-global scalars —
/// `projection_ready` and `projection_clean`, the latter
/// `countIf(dirty.dirty_revision > published.dirty_revision) = 0` over EVERY
/// live session — so one actively-ingesting session returned
/// `ReadModelChanged` for every other session's search, and
/// `run_publication_consistent_scoped` retried the whole operation four times
/// before surfacing `internal_error`. The canonical engine has no global gate:
/// validity is proven per row, twice, by the locator version join during
/// ranking and by the candidate's presence at the same `event_version` in live
/// navigation during derivation.
///
/// **What this test still proves is narrower than its old name claimed.** The
/// mock option that fed a dirty projection scalar (`dirty_projection_on_first_
/// candidate`) went with the v1 mock handler it was read by — that handler was
/// proved unreachable, so the option was inert and "a regression that
/// re-introduced a global gate would fail here" was not true. What remains is
/// a full-shape assertion on one canonical search: the ranked page, its
/// winner-only hydration decoration, and the two-level `session_completed`
/// rule. The absence of a global gate is now covered where it is decidable —
/// `every_v2_search_builder_is_free_of_the_projection`
/// (`search_canonical.rs`), which reads the statements the builders emit.
#[tokio::test(flavor = "multi_thread")]
async fn a_canonical_search_page_carries_its_winner_hydration_and_session_flags() {
    scoped(async {
        let query = || SearchMcpEventsQuery {
            query: "hello world".to_string(),
            n_hits: Some(2),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchMcpEventsQuery::default()
        };

        // A dirty projection was fatal under the retired v1 engine (it failed
        // every request closed); the canonical engine never read the dirtiness
        // relation, and since WI-10 the relation itself is dropped.
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                ..MockOptions::default()
            },
        )
        .await;
        let result = repo
            .search_mcp_events(query())
            .await
            .expect("the canonical engine serves a ranked page");

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

        // `search_postings` is read WITH `FINAL` — the table's own
        // `ReplacingMergeTree(post_version)` collapse — and never with a
        // hand-rolled `GROUP BY` over the postings scan. The three ranking
        // relations are all keyed on `(event_uid, source_host)`, so ranking is
        // DOCUMENT-grained and there is no per-attribution distinction for a
        // `GROUP BY` to preserve (issue #597 D1). Keeping the read a streaming
        // merging read is also what keeps the term-key prune worth having.
        assert!(ranking.contains("FROM `moraine`.`search_postings` AS p FINAL"));
        assert!(
            !ranking.contains("GROUP BY p.term, p.doc_id"),
            "the postings scan must stay a merging read, not a hash \
             aggregation: {ranking}"
        );

        // `df` is corpus-wide: computed inside `term_postings`, and NO user
        // filter may appear there. `count()` is an exact document count because
        // `FINAL` plus the locator equi-join leave at most one row per
        // `(term, event_uid, source_host)`.
        let (df_cte, projection) = ranking
            .split_once("\nSELECT\n  p.event_uid AS event_uid,")
            .expect("ranking statement has a CTE and a projection");
        assert!(df_cte.contains("toUInt64(count() OVER (PARTITION BY p.term)) AS df"));
        assert_eq!(
            df_cte.matches(" OVER (").count(),
            1,
            "the df window must be the only window in the ranking CTE: {df_cte}"
        );
        // The `term_postings` WHERE is term membership and NOTHING else: the
        // clause runs to the CTE's closing paren with no conjunct after it.
        // This is an EXACT-form assertion, not a containment check — a
        // containment check is satisfied by a predicate appended below the term
        // clause, which is precisely the mutation it has to catch.
        let term_postings_where = df_cte
            .rsplit_once("    WHERE p.term IN ['hello','world']")
            .expect("term_postings filters on term membership")
            .1;
        assert_eq!(
            term_postings_where.trim(),
            ")",
            "no user filter may live inside the df CTE, but found `{term_postings_where}`"
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
        // The canonical metadata fold answers per-field-latest from the
        // session's metadata-bearing events, so the TITLE field is the title
        // event's value — the retired v1 header folded the summary in here.
        assert_eq!(
            result.hits[0].session_title.as_deref(),
            Some("Session C title")
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
    // The bounded canonical pipeline: corpus stats, ranking, dedup
    // derivation, digest keys, batched totals, metadata, turn scalars, and
    // the wide winner read — every one carries the envelope's remaining
    // deadline.
    assert_eq!(read_params.len(), 8);
    for params in read_params {
        let remaining = params["max_execution_time"]
            .parse::<f64>()
            .expect("numeric remaining ClickHouse deadline");
        assert!(remaining > 0.0 && remaining <= 2.0);
        assert_eq!(params["timeout_overflow_mode"], "throw");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_cold_read_set_is_exactly_the_bounded_pipeline() {
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
            .expect("bounded search succeeds");
        assert_eq!(result.hits.len(), 2);

        // The whole cold pipeline, exactly: one corpus-stats read, one
        // bounded ranking pass, one dedup derivation, one digest-key read,
        // one batched-totals read, one metadata read, one turn-scalar read,
        // and one wide winner read. No refill loop, no OFFSET paging, and —
        // since issue #603 WI-10 dropped the projection — no `mcp_open_*`
        // relation anywhere in the set.
        let queries = state.queries.lock().expect("queries lock");
        let reads: Vec<&String> = queries
            .iter()
            .filter(|query| !query.trim_start().starts_with("INSERT INTO"))
            .collect();
        assert_eq!(reads.len(), 8, "cold search must issue exactly eight reads");
        assert_eq!(
            reads
                .iter()
                .filter(|query| query.contains("term_postings AS ("))
                .count(),
            1,
            "one bounded ranking pass"
        );
        assert_eq!(
            reads
                .iter()
                .filter(|query| query.contains("FROM `moraine`.`search_corpus_stats`"))
                .count(),
            1,
            "one corpus-stats read"
        );
        for query in queries.iter() {
            assert!(!query.contains(" OFFSET "), "no refill paging: {query}");
            assert!(!query.contains("mcp_open_"), "no projection read: {query}");
            assert!(
                !query.contains("v_conversation_trace") && !query.contains("v_session_summary"),
                "no legacy view chain: {query}"
            );
        }
        assert!(
            queries.iter().all(|query| query
                .lines()
                .filter(|line| line.contains("INNER JOIN"))
                .all(|line| line.trim_start().starts_with("ALL INNER JOIN"))),
            "search stages must explicitly preserve inner-join multiplicity"
        );
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
            .find(|q| q.contains("term_postings AS (") && q.contains("p.session_id = 'sess_a'"))
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

        // The canonical turn scope is two-sided: Phase 0 derives the turn's
        // live uid set from the navigation index, and the ranking pass binds
        // its candidates to that uid set — never to a projection turn row.
        let queries = state.queries.lock().expect("queries lock").clone();
        assert!(
            queries
                .iter()
                .any(|q| q.contains("AS turn_seq") && q.contains("n.session_id = 'sess_c'")),
            "the turn-uid derivation should be captured: {queries:#?}"
        );
        let ranking_query = queries
            .iter()
            .find(|q| q.contains("term_postings AS (") && q.contains("p.session_id = 'sess_c'"))
            .expect("turn-scoped ranking query should be captured");
        assert!(ranking_query.contains("p.event_uid IN ["));
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
            q.contains("term_postings AS (") && q.contains("lowerUTF8(p.actor_role) = 'user'")
        }));
        assert!(queries.iter().any(|q| {
            q.contains("term_postings AS (") && q.contains("lowerUTF8(p.actor_role) = 'assistant'")
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
            .filter(|query| query.contains("term_postings AS ("))
            .collect::<Vec<_>>();
        assert_eq!(candidate_queries.len(), 1, "{queries:?}");
        let first_candidate_query = candidate_queries[0];
        assert!(first_candidate_query.contains("LIMIT 9"));
        assert!(!first_candidate_query.contains("OFFSET"));
        assert!(!first_candidate_query.contains("text_content"));
        assert!(!first_candidate_query.contains("SHA256"));
        // Dedup keys ride the stored per-document digest, and the collapsing
        // window includes every ranked candidate — 'evt-b-9' among them.
        assert!(queries.iter().any(|query| {
            query.contains("AS text_content_digest")
                && query.contains("AS payload_phase")
                && query.contains("'evt-b-9'")
        }));
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
        // Through the engine matrix. This guard used to run only against the
        // retired v1 path, where a v2 refill-loop regression could not reach
        // it; the matrix is one entry wide today, and running through it is
        // what keeps that true by construction rather than by memory.
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
async fn search_mcp_events_serves_a_winner_whose_wide_row_moved_without_fabricating() {
    // The retired v1 engine turned a missing pinned detail row into
    // `ReadModelChanged`; the canonical engine's wide read is a best-effort
    // fill within the pinned publication revision, so a winner whose wide row
    // moved between ranking and hydration is still served — from its
    // content-free ranking identity, with NO fabricated content — and every
    // other winner hydrates normally.
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                omit_first_mcp_detail_row: true,
                open_v2_reader_ready: Some(true),
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
            .expect("a moved wide row must not fail the whole page");

        assert_eq!(result.hits.len(), 2);
        let starved = &result.hits[0];
        assert_eq!(starved.event_uid, "evt-c-42");
        assert!(
            starved.text_content.is_none() && starved.snippet.is_empty(),
            "the starved winner must not carry fabricated content: {starved:?}"
        );
        let hydrated = &result.hits[1];
        assert_eq!(hydrated.event_uid, "evt-a-11");
        assert!(
            hydrated.text_content.is_some(),
            "the un-starved winner still hydrates: {hydrated:?}"
        );
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
// It was introduced because every MCP search fixture written before #597 ran
// against the v1 engine and could not fail on a v2 regression; ONE assertion
// set over ONE mock corpus (`mcp_search_detail_row`) run through the matrix
// fixed that, the way #599's `ListPath` matrix does for session listing.
//
// Issue #603 WI-10 retired v1, so the matrix is one entry wide. It is kept
// rather than inlined for two reasons: an assertion failure still names the
// path it ran under, and a future second read path inherits every test in the
// matrix instead of being bolted on beside them. `ListPath` is one entry wide
// for the same reason.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchPath {
    /// `open_v2.ready = 1`: the bounded canonical engine — the only engine
    /// since issue #603 WI-10 retired the projected-header fallback. The
    /// matrix shape survives so a future second engine inherits every test
    /// unchanged; the unpublished state is a typed refusal, pinned by
    /// [`search_mcp_events_refuses_typed_while_indexes_unpublished`].
    Canonical,
}

impl SearchPath {
    const ALL: [SearchPath; 1] = [SearchPath::Canonical];

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
                open_v2_reader_ready: Some(true),
                ..options
            },
        )
        .await
    }

    /// The ranking pass's statement signature on this engine: v2 projects the
    /// locator's `post_version`.
    fn is_ranking_statement(self, query: &str) -> bool {
        match self {
            SearchPath::Canonical => {
                query.contains("term_postings AS (") && query.contains("AS post_version")
            }
        }
    }
}

/// Issue #603 WI-10: with the projected-header engine retired, an unpublished
/// store refuses typed — naming the sweep — instead of silently serving the
/// fallback (which no longer exists) or a confident empty answer.
#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_refuses_typed_while_indexes_unpublished() {
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(false),
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
            .expect_err("an unpublished store must refuse search");
        match error {
            RepoError::Backend(message) => {
                assert!(
                    message.contains("not ready") && message.contains("moraine db migrate"),
                    "the refusal must name the sweep: {message}"
                );
            }
            other => panic!("expected the typed unready refusal, got {other:?}"),
        }
    })
    .await;
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

/// C1 / D1 / #608. A double-attributed uid ranks ONCE, at DOCUMENT grain, and
/// that is the design rather than a compromise.
///
/// `event_uid` is content-addressed over
/// `source_file|source_generation|source_line_no|source_offset|
/// record_fingerprint` and deliberately EXCLUDES `session_id`, so a physical
/// line that ingest attributed to two sessions is one uid under two session ids
/// — 19,846 of them on the reference host — and it is ONE DOCUMENT: one file,
/// one generation, one line, one byte range, one fingerprint. BM25 scores
/// documents; `df` and `docs` are document counts.
///
/// The read model can represent nothing else. `search_documents` is
/// `ReplacingMergeTree(doc_version) ORDER BY (event_uid, source_host)` and is
/// the MV source for `search_postings`, which is
/// `ReplacingMergeTree(post_version) ORDER BY (term, doc_id, source_host)`;
/// `mcp_event_locator` is `ReplacingMergeTree(event_version) ORDER BY
/// (event_uid, source_host)`. `session_id` is in none of those sort keys, so a
/// second attribution is destroyed at MERGE time — not merely when a query says
/// `FINAL` — and, before any merge runs, the ranking statement's
/// `l.event_version = p.post_version` join keeps only the revision the locator
/// authorizes. That locator row is the same authority the `open_v2` exact-event
/// seek uses, so search and open resolve the uid to the SAME session; a ranking
/// that returned both would let a user follow a hit into a different session
/// than the one the hit named.
///
/// The user-visible consequence, stated rather than left to be rediscovered: a
/// search scoped to the LOSING session of a double-attributed uid does not
/// return that uid. That is ~1% of the reference corpus, it is the shipping
/// behaviour on a real server, and it is an INGEST defect owned by #608 — one
/// of the two session ids is simply wrong. The read model must not mirror it.
///
/// MUTATION (a): re-introduce a per-attribution `GROUP BY p.term, p.doc_id,
/// p.source_host, p.session_id` over the postings scan in
/// `bounded_ranking_ctes` — fails on the statement-shape assertions.
/// MUTATION (b): drop `FINAL` from that scan — same.
#[tokio::test(flavor = "multi_thread")]
async fn a_double_attributed_uid_ranks_once_at_document_grain() {
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
            .expect("a double-attributed uid is a valid hit, not an error");

        let attributions = result
            .hits
            .iter()
            .map(|hit| (hit.session_id.as_str(), hit.event_uid.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            attributions,
            vec![("sess_c", "evt-shared")],
            "one physical line is one document and ranks once, under the \
             attribution the locator authorizes"
        );

        // …and the statement the server saw is the table's own replacement,
        // not a hand-rolled one that would key the result on a distinction
        // `search_postings` cannot durably hold.
        let queries = state.queries.lock().expect("queries lock").clone();
        let ranking = queries
            .iter()
            .find(|query| {
                query.contains("term_postings AS (")
                    && query.contains("toUInt64(any(p.event_version)) AS post_version")
            })
            .expect("the request issued a bounded ranking pass");
        assert!(
            ranking.contains("FROM `moraine`.`search_postings` AS p FINAL"),
            "the postings scan is a merging read on the table's own \
             ReplacingMergeTree(post_version): {ranking}"
        );
        assert!(
            !ranking.contains("GROUP BY p.term, p.doc_id"),
            "a per-attribution collapse returns a cardinality that depends on \
             background merge scheduling — non-repeatable search results: \
             {ranking}"
        );
        assert!(
            ranking.contains("AND l.event_version = p.post_version"),
            "the version join is what closes the pre-merge window: {ranking}"
        );
    })
    .await;
}

/// B1 / #608, the HYDRATION half — and the reason the post-ranking reads are
/// keyed on `(source_host, session_id, event_uid)` even though RANKING is
/// document-grained.
///
/// The two sides of the search path have different grains, and that asymmetry
/// is the whole content of this test. Ranking reads the search index, which is
/// keyed on `(event_uid, source_host)` and emits ONE candidate per document
/// (see `a_double_attributed_uid_ranks_once_at_document_grain`). Everything
/// after ranking reads CANONICAL relations — `moraine.events` and
/// `mcp_event_navigation` both lead their primary key with `session_id` — which
/// genuinely carry BOTH attributions of a double-attributed uid. A uid-only map
/// key therefore lets the OTHER session's derivation and wide row overwrite the
/// candidate's own, and the hit silently reports the wrong turn, the wrong
/// ordering and the wrong `item_id`.
///
/// The mock reproduces exactly that: the ranking arm serves one candidate
/// (`sess_c`), and the derivation and wide arms serve the superset — every
/// session that carries the uid — which is what a real canonical read returns.
///
/// MUTATION (either one; each fails this test):
///   * key `derivation_by_identity` on `(source_host, event_uid)` — the
///     candidate is hydrated against `sess_a`'s turn/order;
///   * key `wide_by_identity` on `(source_host, event_uid)` — the candidate
///     gets `sess_a`'s `item_id`.
///
/// NOT a mutation this test detects: dropping the `(session_id, event_uid)`
/// tuple from `build_search_candidate_derivation_sql` /
/// `build_search_wide_hydration_sql`. A uid-only filter returns the SUPERSET,
/// and the maps above are keyed by the triple, so the extra rows are ignored
/// and the answer is unchanged. That tuple's job is bounding the read, and its
/// guard is the shape test `canonical_search_reads_are_session_qualified`.
///
/// Also NOT detected any more: `dedup_by_document.remove(...)` instead of
/// `.get(...)`. With document-grained ranking there is at most one candidate
/// per `(source_host, event_uid)`, so consuming the entry starves nobody.
/// `.get()` is kept because it is the shape that stays correct if that ever
/// changes, but it no longer has a behavioural guard and the ledger records
/// that rather than claiming one.
#[tokio::test(flavor = "multi_thread")]
async fn a_double_attributed_uid_hydrates_against_the_session_it_ranked_under() {
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
            .expect("a double-attributed uid is a valid hit, not an error");

        let attributions = result
            .hits
            .iter()
            .map(|hit| (hit.session_id.as_str(), hit.event_uid.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            result.hits.len(),
            1,
            "ranking is document-grained: {attributions:?}"
        );
        let hit = &result.hits[0];
        // The premise: the uid the hit reports is the SHARED one, and the
        // session it reports is the ranked attribution. If the fixture ever
        // stops being one uid under two sessions, every assertion below is
        // satisfiable by a uid-keyed implementation and this test proves
        // nothing.
        assert_eq!(hit.event_uid, "evt-shared", "{attributions:?}");
        assert_eq!(hit.session_id, "sess_c", "{attributions:?}");

        // Everything derived and hydrated is `sess_c`'s, even though the
        // canonical reads also returned `sess_a`'s row for this uid.
        assert_eq!(hit.turn_seq, 2, "hydrated against the wrong session's turn");
        assert_eq!(hit.event_order, 42);
        assert_eq!(hit.event_ordinal, 3);
        assert_eq!(hit.turn_event_count, 3);
        assert_eq!(
            hit.item_id.as_deref(),
            Some("item-evt-shared-c"),
            "hydrated from the wrong session's row"
        );
        // The negative form, so the assertions above cannot pass by coincidence
        // if the fixture values ever converge.
        assert_ne!(hit.item_id.as_deref(), Some("item-evt-shared-a"));
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
/// `search_conversations` and canonical MCP search alike — and did for the
/// retired v1 MCP search too. The design
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
/// search writes is the entry canonical MCP search reads, so the slot is
/// shared rather than fought over.
///
/// The ranking statement inlines `search_corpus_stats` only when the cache is
/// COLD, which is what makes the reuse observable in the statement text — the
/// assertion below is that the MCP ranking statement does NOT carry it, and
/// that exactly one corpus read reached the backend for both searches.
#[tokio::test(flavor = "multi_thread")]
async fn the_corpus_stats_cache_slot_is_shared_by_every_search_path() {
    scoped(async {
        let (repo, state) = build_repo().await;

        // Prime the slot from the conversation-search side.
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

        // The canonical MCP event search reads the SAME slot.
        let mcp = repo
            .search_mcp_events(SearchMcpEventsQuery {
                query: "hello world".to_string(),
                n_hits: Some(2),
                min_score: Some(0.0),
                min_should_match: Some(1),
                ..SearchMcpEventsQuery::default()
            })
            .await
            .expect("canonical mcp search");
        assert_eq!(mcp.stats.docs, DOCUMENT_AUTHORIZED_DOCS);

        let queries = state.queries.lock().expect("queries lock").clone();
        let mcp_ranking: Vec<&String> = queries
            .iter()
            .filter(|query| {
                query.contains("term_postings AS (") && query.contains("AS post_version")
            })
            .collect();
        assert_eq!(mcp_ranking.len(), 1, "{queries:?}");
        assert!(
            !mcp_ranking[0].contains("search_corpus_stats"),
            "the mcp search must reuse the primed slot instead of inlining a \
             second corpus read: {}",
            mcp_ranking[0]
        );
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.contains("FROM `moraine`.`search_corpus_stats`"))
                .count(),
            1,
            "one corpus read for both paths: {queries:?}"
        );
    })
    .await;
}
