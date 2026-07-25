use super::*;
use moraine_conversations::{ClickHouseConversationRepository, McpSessionListItem, Page};

#[tokio::test(flavor = "multi_thread")]
async fn publication_snapshot_combines_head_and_fence_round_trips() {
    scoped(async {
        let (repo, state) = build_repo().await;

        repo.list_conversations(ConversationListFilter::default(), PageRequest::default())
            .await
            .expect("list conversations with a stable publication snapshot");

        let queries = state
            .publication_snapshot_queries
            .lock()
            .expect("publication snapshot query lock");
        assert_eq!(
            queries.len(),
            2,
            "capture and revalidation should each use one request"
        );
        assert!(queries[0].contains("moraine:publication_snapshot:capture"));
        assert!(queries[0].contains("moraine:append_fence:capture"));
        assert!(queries[1].contains("moraine:publication_snapshot:revalidate"));
        assert!(queries[1].contains("moraine:append_fence:revalidate"));
        assert!(queries
            .iter()
            .all(|query| query.matches("UNION ALL").count() == 1));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_conversations_applies_filters_and_cursor_pagination() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_conversations_supports_ascending_sort_with_deterministic_cursor() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_conversations_rejects_cursor_when_sort_changes() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_uses_overlap_filter_and_cursor_pagination() {
    scoped(async {
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
        let public_items =
            serde_json::to_string(&first.items).expect("serialize public list items");
        assert!(!public_items.contains("\"originator\":"));
        assert!(!public_items.contains("\"project\":"));
        assert!(!public_items.contains("acme-secret-merger"));
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
            .find(|q| q.contains("current_headers AS") && q.contains("AS completed"))
            .expect("list_sessions query should be captured");

        assert!(list_query.contains("toUnixTimestamp64Milli(s.last_event_time) >= 1767261600000"));
        assert!(list_query.contains("toUnixTimestamp64Milli(s.first_event_time) < 1767500000000"));
        assert!(list_query.contains("s.mode = 'web_search'"));
        assert!(list_query.contains("s.harness = 'codex'"));
        assert!(list_query.contains("s.source = 'codex'"));
        assert!(list_query.contains("s.source AS source"));
        assert!(list_query.contains("FROM `moraine`.`mcp_open_publication_headers` AS h FINAL"));
        assert!(list_query.contains("FROM `moraine`.`mcp_open_dirty_sessions` FINAL"));
        assert!(list_query
            .contains("FROM `moraine`.`v_published_source_generation_history` AS history"));
        assert!(list_query.contains("length(h.required_source_heads) > 0"));
        assert!(list_query.contains("required_head -> has(captured_heads, required_head)"));
        assert!(list_query.contains("h.dirty_revision = ifNull(d.dirty_revision, toUInt64(0))"));
        assert!(list_query.contains("ORDER BY s.last_event_time DESC, s.session_id DESC"));
        // Blank session_id rows are filtered at the source so they never consume a
        // LIMIT slot or anchor the keyset cursor (#386).
        assert!(list_query.contains("notEmpty(trimBoth(s.session_id))"));
        assert!(
            !list_query.contains("v_live_events") && !list_query.contains("v_session_summary"),
            "list_sessions must not reconstruct projected metadata from canonical events"
        );
    })
    .await;
}
/// The issue-599 filter: every dimension populated, so the assertions below
/// exercise each one's recall predicate and exact re-check.
fn directory_filter() -> McpSessionListFilter {
    McpSessionListFilter {
        start_unix_ms: 1767261600000_i64,
        end_unix_ms: 1767500000000_i64,
        mode: Some(ConversationMode::WebSearch),
        harness: Some("codex".to_string()),
        source_name: Some("codex".to_string()),
        sort: ConversationListSort::Desc,
    }
}

/// The two `list_sessions` implementations behind
/// [`ClickHouseConversationRepository::list_mcp_sessions`].
///
/// The semantics-preservation checklist (issue-599 §4) is a contract on the
/// OPERATION, not on one implementation of it, so every shared assertion runs
/// against both. The mock serves the same three fixture sessions to each path,
/// which is what makes a field-for-field page comparison meaningful.
#[derive(Copy, Clone, Debug)]
enum ListPath {
    /// Pre-#599 `mcp_open_publication_headers` reader (`open_v2` unpublished).
    Headers,
    /// Issue-599 `mcp_session_directory` reader (`open_v2` published).
    Directory,
}

impl ListPath {
    const ALL: [Self; 2] = [Self::Headers, Self::Directory];

    async fn repo(self) -> (ClickHouseConversationRepository, Arc<MockState>) {
        match self {
            Self::Headers => build_repo().await,
            Self::Directory => build_directory_repo().await,
        }
    }

    async fn scoped_repo(
        self,
        roots: &[&str],
    ) -> (ClickHouseConversationRepository, Arc<MockState>) {
        match self {
            Self::Headers => build_scoped_repo(roots).await,
            Self::Directory => build_scoped_directory_repo(roots).await,
        }
    }
}

/// The issue-599 §4 semantics-preservation checklist, expressed as behavioral
/// assertions so it runs unchanged against either implementation: argument
/// validation, overlap filtering, keyset continuation, the exact
/// cursor-mismatch contract string, and the additive monitor facets. Returns
/// the first page so the caller can compare the two paths directly.
async fn assert_shared_list_sessions_semantics(
    repo: &ClickHouseConversationRepository,
    path: ListPath,
) -> Page<McpSessionListItem> {
    let filter = directory_filter();

    // Argument validation runs before any read on both paths.
    for (start_unix_ms, end_unix_ms) in [(1_767_500_000_000_i64, 1_767_261_600_000_i64), (7, 7)] {
        let error = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    start_unix_ms,
                    end_unix_ms,
                    ..filter.clone()
                },
                PageRequest {
                    limit: 2,
                    cursor: None,
                },
            )
            .await
            .expect_err("an empty or inverted window must be rejected");
        assert_eq!(
            error.to_string(),
            "invalid argument: start_unix_ms must be strictly less than end_unix_ms",
            "{path:?}"
        );
    }

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

    assert_eq!(first.items.len(), 2, "{path:?}");
    assert_eq!(first.items[0].session_id, "sess_c", "{path:?}");
    assert_eq!(first.items[1].session_id, "sess_b", "{path:?}");
    assert_eq!(
        first.items[0].title.as_deref(),
        Some("Session C title"),
        "{path:?}"
    );
    assert_eq!(
        first.items[0].session_summary.as_deref(),
        Some("Session C summary"),
        "{path:?}"
    );
    assert_eq!(
        first.items[0].session_slug.as_deref(),
        Some("project-c"),
        "{path:?}"
    );
    assert_eq!(first.items[0].source.as_deref(), Some("codex"), "{path:?}");
    assert_eq!(first.items[0].harness.as_deref(), Some("codex"), "{path:?}");
    assert_eq!(first.items[0].total_turns, 3, "{path:?}");
    assert_eq!(first.items[0].total_events, 30, "{path:?}");
    assert_eq!(first.items[0].mode, ConversationMode::WebSearch, "{path:?}");
    assert!(first.items[0].completed, "{path:?}");
    assert_eq!(
        first.items[0].inference_provider.as_deref(),
        Some("openai"),
        "{path:?}"
    );
    assert_eq!(first.items[0].tool_calls, 6, "{path:?}");

    // Private projection columns never reach the public item on either path.
    let public_items = serde_json::to_string(&first.items).expect("serialize public list items");
    assert!(!public_items.contains("\"originator\":"), "{path:?}");
    assert!(!public_items.contains("\"project\":"), "{path:?}");
    assert!(!public_items.contains("acme-secret-merger"), "{path:?}");

    let cursor = first.next_cursor.clone().expect("next cursor");

    let second = repo
        .list_mcp_sessions(
            filter.clone(),
            PageRequest {
                limit: 2,
                cursor: Some(cursor.clone()),
            },
        )
        .await
        .expect("second page");
    assert_eq!(second.items.len(), 1, "{path:?}");
    assert_eq!(second.items[0].session_id, "sess_a", "{path:?}");
    assert!(!second.items[0].completed, "{path:?}");
    assert!(second.items[0].title.is_none(), "{path:?}");
    assert!(second.next_cursor.is_none(), "{path:?}");

    // The exact contract string, for a changed filter dimension and for the
    // absent-vs-sentinel distinction the tool tests depend on.
    for changed in [
        McpSessionListFilter {
            mode: Some(ConversationMode::Chat),
            ..filter.clone()
        },
        McpSessionListFilter {
            source_name: Some("__none__".to_string()),
            ..filter.clone()
        },
    ] {
        let error = repo
            .list_mcp_sessions(
                changed,
                PageRequest {
                    limit: 2,
                    cursor: Some(cursor.clone()),
                },
            )
            .await
            .expect_err("a cursor must not resume under a different filter");
        assert_eq!(
            error.to_string(),
            "invalid cursor: cursor does not match current list_sessions filter",
            "{path:?}"
        );
    }

    first
}

#[tokio::test(flavor = "multi_thread")]
async fn list_sessions_semantics_are_identical_on_both_paths() {
    scoped(async {
        let mut pages: Vec<(ListPath, Page<McpSessionListItem>)> = Vec::new();
        for path in ListPath::ALL {
            let (repo, _state) = path.repo().await;
            pages.push((
                path,
                assert_shared_list_sessions_semantics(&repo, path).await,
            ));
        }

        // Same filter, same fixture corpus: the two readers must agree on the
        // session set and on every per-item field.
        //
        // The CURSOR is deliberately excluded from that comparison and asserted
        // to differ instead. The two paths anchor on different values — the
        // header path on the projector's exact aggregate, the directory path on
        // the live-generation `max_observed_event_time` it also reports — so a
        // token is only meaningful to the path that minted it. Comparing the
        // tokens for equality would re-assert the false premise this test used
        // to carry, and would fail the moment the anchors legitimately diverge.
        let (reference_path, reference) = &pages[0];
        for (path, page) in &pages[1..] {
            assert_eq!(
                serde_json::to_value(&page.items).expect("serialize items"),
                serde_json::to_value(&reference.items).expect("serialize items"),
                "{path:?} diverged from {reference_path:?}"
            );
            assert_eq!(
                page.next_cursor.is_some(),
                reference.next_cursor.is_some(),
                "{path:?} and {reference_path:?} must agree on whether more pages exist"
            );
            if let (Some(theirs), Some(ours)) = (&page.next_cursor, &reference.next_cursor) {
                assert_ne!(
                    theirs, ours,
                    "{path:?} must mint a path-tagged token distinct from {reference_path:?}"
                );
            }
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_sessions_rejects_foreign_scope_cursors_on_both_paths() {
    scoped(async {
        for path in ListPath::ALL {
            let (unscoped_repo, _unscoped_state) = path.repo().await;
            let (scoped_repo, _scoped_state) = path.scoped_repo(&["/work/project"]).await;
            let filter = directory_filter();

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

            let error = scoped_repo
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
                error.to_string(),
                "invalid cursor: cursor does not match current list_sessions filter",
                "{path:?}"
            );
        }
    })
    .await;
}

/// A batched-totals fixture row for a session with no metadata and one turn.
fn totals_row(session_id: &str, harness: &str) -> serde_json::Value {
    json!({
        "session_id": session_id,
        "total_events": 4_u64,
        "tool_calls": 1_u64,
        "max_override": 0_u32,
        "counter_user_messages": 1_u64,
        "first_event_time": "2026-01-02 10:00:00",
        "first_event_unix_ms": 1_767_348_000_000_i64,
        "last_event_time": "2026-01-02 10:10:00",
        "last_event_unix_ms": 1_767_348_600_000_i64,
        "origin_cwd": "/repo",
        "source": "codex",
        "harness": harness,
        "inference_provider": "openai",
        "omp_dispatch_title": "",
        "mode": "chat"
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_path_emits_the_content_free_candidate_page() {
    scoped(async {
        // The page's OUTPUT is asserted against the projected-header page by
        // `list_sessions_semantics_are_identical_on_both_paths`; this test
        // pins the SQL that produces it.
        let (repo, state) = build_directory_repo().await;

        repo.list_mcp_sessions(
            directory_filter(),
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("directory page");

        let queries = state.queries.lock().expect("queries lock").clone();
        let directory_query = queries
            .iter()
            .find(|query| query.contains("FROM `moraine`.`mcp_session_directory` AS d"))
            .expect("directory candidate query should be captured");
        assert!(directory_query.contains("cand_last_ms >= 1767261600000"));
        assert!(directory_query.contains("cand_first_ms < 1767500000000"));
        assert!(directory_query.contains("mode_hint >= 3"));
        assert!(directory_query.contains("has(harnesses, 'codex')"));
        assert!(directory_query.contains("has(sources, 'codex')"));
        assert!(directory_query.contains("notEmpty(trimBoth(d.session_id))"));
        assert!(directory_query.contains("argMinIfMerge(d.origin_cwd_state)"));
        assert!(directory_query.contains("ORDER BY cand_last_ms DESC, session_id DESC"));
        // One statement fetches the page's whole hydration budget
        // (hydration_chunk_size(2) = 6, x MAX_HYDRATION_CHUNKS = 24).
        assert!(directory_query.contains("LIMIT 24"));
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.contains("FROM `moraine`.`mcp_session_directory` AS d"))
                .count(),
            1,
            "Phase A must run once per page — a second pass re-aggregates the whole directory: {queries:#?}"
        );

        // The events relation is opened only for the bounded metadata read, and
        // only with its session filter INSIDE the derived table: `SELECT e.*`
        // republishes every wide column, so an outer-only filter is a
        // whole-corpus FINAL scan that no column-name grep can see.
        for query in queries
            .iter()
            .filter(|query| query.contains("FROM `moraine`.`events` AS e FINAL"))
        {
            assert!(
                query.contains(
                    "AND published.source_generation = e.source_generation\nWHERE e.session_id IN ["
                ),
                "unpruned events scan on the discovery path: {query}"
            );
        }

        // The whole point of the cutover: no statement of the page touches the
        // projector, the legacy view chain, or transcript content.
        for query in &queries {
            assert!(
                !query.contains("mcp_open_"),
                "list_sessions must not read the projector: {query}"
            );
            assert!(
                !query.contains("v_session_summary")
                    && !query.contains("v_conversation_trace")
                    && !query.contains("v_turn_summary"),
                "list_sessions must not read the legacy view chain: {query}"
            );
            assert!(
                !query.contains("text_content"),
                "list_sessions must not read transcript content: {query}"
            );
            assert!(
                !query.contains("argMin(cwd"),
                "list_sessions must not run the corpus-wide scope subquery: {query}"
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_uses_batched_hydration() {
    scoped(async {
        let (repo, state) = build_directory_repo().await;

        repo.list_mcp_sessions(
            directory_filter(),
            PageRequest {
                limit: 25,
                cursor: None,
            },
        )
        .await
        .expect("directory page");

        let queries = state.queries.lock().expect("queries lock").clone();
        // One directory pass plus three batched hydration statements. The
        // ceiling is 1 + MAX_HYDRATION_CHUNKS x 3 = 13; anything above it means
        // a per-session loop or a repeated directory aggregation crept back in.
        assert_eq!(queries.len(), 4, "captured queries: {queries:#?}");
        let hydration = queries
            .iter()
            .filter(|query| query.contains("['sess_c','sess_b','sess_a']"))
            .count();
        assert_eq!(
            hydration, 3,
            "totals, metadata and terminal must each batch the whole chunk: {queries:#?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_applies_session_origin_scope_exactly() {
    scoped(async {
        let (repo, state) = build_scoped_directory_repo(&["/work/project"]).await;

        repo.list_mcp_sessions(
            McpSessionListFilter {
                mode: None,
                harness: None,
                source_name: None,
                ..directory_filter()
            },
            PageRequest {
                limit: 5,
                cursor: None,
            },
        )
        .await
        .expect("scoped directory page");

        let queries = state.queries.lock().expect("queries lock").clone();
        let directory_query = queries
            .iter()
            .find(|query| query.contains("FROM `moraine`.`mcp_session_directory` AS d"))
            .expect("directory candidate query should be captured");
        assert!(directory_query.contains("origin_cwd = '/work/project'"));
        assert!(directory_query.contains("startsWith(origin_cwd, '/work/project/')"));
        assert!(
            !directory_query.contains("argMin(cwd"),
            "scope is served by the merged directory state, not a corpus scan"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_never_prefilters_mcp_internal_mode() {
    scoped(async {
        let (repo, state) = build_directory_repo().await;

        repo.list_mcp_sessions(
            McpSessionListFilter {
                mode: Some(ConversationMode::McpInternal),
                harness: None,
                source_name: None,
                ..directory_filter()
            },
            PageRequest {
                limit: 5,
                cursor: None,
            },
        )
        .await
        .expect("mcp_internal page");

        let queries = state.queries.lock().expect("queries lock").clone();
        let directory_query = queries
            .iter()
            .find(|query| query.contains("FROM `moraine`.`mcp_session_directory` AS d"))
            .expect("directory candidate query should be captured");
        // sql/036:156 freezes the internal-tool allowlist inside the MV body,
        // so a session using a tool added later carries a hint BELOW its live
        // rank. Any hint predicate would silently drop it.
        assert!(
            !directory_query.contains("mode_hint >="),
            "mcp_internal must not push a mode_hint predicate: {directory_query}"
        );
    })
    .await;
}

/// One Phase-A candidate row.
/// Render a millisecond instant the way ClickHouse renders `DateTime64(3)`.
fn format_directory_display_time(unix_ms: i64) -> String {
    let secs = unix_ms.div_euclid(1_000);
    let millis = unix_ms.rem_euclid(1_000);
    let days = secs.div_euclid(86_400);
    let time_of_day = secs.rem_euclid(86_400);
    // 1970-01-01 + `days`, via the civil-from-days algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{millis:03}",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60
    )
}

fn candidate_row(session_id: &str, cand_last_ms: i64) -> serde_json::Value {
    // `cand_last_time` is the display form of the same instant: the directory
    // path orders by `cand_last_ms` and reports its display form, so a fixture
    // that let them describe different instants would not model the real row.
    json!({
        "session_id": session_id,
        "cand_last_ms": cand_last_ms,
        "cand_last_time": format_directory_display_time(cand_last_ms),
    })
}

/// [`totals_row`] with explicit exact `display_time` bounds, for the case where
/// hydration's `max(display_time)` is BELOW the directory's
/// `max(max_observed_event_time)` — what a superseded event version leaves
/// behind, since the directory aggregate can never retract one.
fn totals_row_at(
    session_id: &str,
    first_event_unix_ms: i64,
    last_event_unix_ms: i64,
) -> serde_json::Value {
    let mut row = totals_row(session_id, "codex");
    row["first_event_unix_ms"] = json!(first_event_unix_ms);
    row["last_event_unix_ms"] = json!(last_event_unix_ms);
    row
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_rechecks_origin_scope_against_the_hydrated_cwd() {
    scoped(async {
        // Phase A's `argMinIfMerge(origin_cwd_state)` is a RECALL filter: it
        // merges the directory's live-generation rows, which cannot see a
        // superseded version the way navigation read `FINAL` can. So a
        // candidate can survive Phase A while its exact origin cwd lies
        // OUTSIDE the configured scope.
        //
        // Scope decides what a caller is allowed to see, so the exact re-check
        // must drop it. Scripting the candidate directly is what models "Phase
        // A admitted it" without having to reproduce the aggregate skew.
        let responses = {
            let mut responses = vec![ScriptedResponse::rows(
                &["FROM `moraine`.`mcp_session_directory` AS d"],
                json!([
                    candidate_row("sess-inside", 1_767_400_000_000_i64),
                    candidate_row("sess-outside", 1_767_350_000_000_i64),
                ]),
            )];
            let mut inside = totals_row("sess-inside", "codex");
            inside["origin_cwd"] = json!("/work/project/sub");
            let mut outside = totals_row("sess-outside", "codex");
            outside["origin_cwd"] = json!("/work/other");
            responses.extend(hydration_script(
                "'sess-inside','sess-outside'",
                json!([inside, outside]),
            ));
            responses
        };
        let (repo, _state) =
            build_scoped_scripted_directory_repo(&["/work/project"], responses).await;

        let page = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    mode: None,
                    harness: None,
                    source_name: None,
                    ..directory_filter()
                },
                PageRequest {
                    limit: 5,
                    cursor: None,
                },
            )
            .await
            .expect("scoped directory page");

        let served: Vec<&str> = page
            .items
            .iter()
            .map(|item| item.session_id.as_str())
            .collect();
        assert_eq!(
            served,
            vec!["sess-inside"],
            "a candidate whose EXACT origin cwd is outside the scope must not be served"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_orders_and_anchors_on_the_directory_keyset() {
    scoped(async {
        // The directory aggregate and the hydrated aggregate are different
        // numbers whenever an event version has been superseded. Ordering and
        // the cursor MUST use the directory value — it is the only one the next
        // page's `HAVING` can compare against — and the item REPORTS that same
        // directory value, so the page is sorted by the field it returns (B1).
        //
        // sess-p: directory 1_767_400_000_000, exact 1_767_300_000_000
        // sess-q: directory 1_767_350_000_000, exact 1_767_350_000_000
        // Keyset DESC is (p, q); hydrated DESC would be (q, p).
        let responses = {
            let mut responses = vec![ScriptedResponse::rows(
                &["FROM `moraine`.`mcp_session_directory` AS d", "LIMIT 16"],
                json!([
                    candidate_row("sess-p", 1_767_400_000_000_i64),
                    candidate_row("sess-q", 1_767_350_000_000_i64),
                ]),
            )];
            responses.extend(hydration_script(
                "'sess-p','sess-q'",
                json!([
                    totals_row_at("sess-p", 1_767_290_000_000_i64, 1_767_300_000_000_i64),
                    totals_row_at("sess-q", 1_767_340_000_000_i64, 1_767_350_000_000_i64),
                ]),
            ));
            responses
        };
        let (repo, _state) = build_scripted_directory_repo(responses).await;

        let page = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    mode: None,
                    source_name: None,
                    ..directory_filter()
                },
                PageRequest {
                    limit: 1,
                    cursor: None,
                },
            )
            .await
            .expect("keyset page");

        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].session_id, "sess-p",
            "survivors must be ordered by the directory keyset, not the hydrated timestamp"
        );
        // B1: the response reports the value the page was ORDERED and KEYSET
        // by, so it is sorted by the field it returns. sess-p's directory
        // aggregate (1_767_400_000_000) sits above its hydrated exact value
        // (1_767_300_000_000) — the re-inserted-event case — and it is the
        // aggregate that is reported.
        assert_eq!(
            page.items[0].last_event_unix_ms, 1_767_400_000_000_i64,
            "the item must report the directory aggregate it was ordered by"
        );

        let cursor = page.next_cursor.expect("next cursor");
        let payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&cursor).expect("cursor is base64"))
                .expect("cursor payload is json");
        assert_eq!(payload["session_id"], json!("sess-p"));
        assert_eq!(
            payload["last_event_unix_ms"],
            json!(1_767_400_000_000_i64),
            "the cursor must be minted from the directory keyset value; anchoring on the \
             hydrated value skips every session whose aggregate falls between the two"
        );
    })
    .await;
}

/// The three batched hydration responses for one chunk, in issue order. `ids`
/// is a substring that must appear in each statement's `session_id IN` array so
/// the script cannot silently accept a mis-chunked batch.
fn hydration_script(ids: &'static str, totals: serde_json::Value) -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::rows(&["AS counter_user_messages", ids], totals),
        ScriptedResponse::rows(&["n.is_metadata_bearing = 1", ids], json!([])),
        ScriptedResponse::rows(&["GROUP BY session_id, turn_seq", ids], json!([])),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_advances_through_eliminated_chunks() {
    scoped(async {
        // limit 1 -> hydration_chunk_size(1) == 4, candidate_fetch_size(1) == 16.
        // ONE Phase-A pass fetches 8 candidates; the first hydration chunk is
        // eliminated wholesale by the exact harness re-filter, so the page must
        // hydrate the next chunk rather than return empty — WITHOUT re-running
        // the directory aggregation.
        let candidates = (1..=4)
            .map(|i| candidate_row(&format!("sess-x{i}"), 1_767_400_000_000_i64 - i))
            .chain((1..=4).map(|i| candidate_row(&format!("sess-y{i}"), 1_767_300_000_000_i64 - i)))
            .collect::<Vec<_>>();
        let mut responses = vec![ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_session_directory` AS d", "LIMIT 16"],
            json!(candidates),
        )];
        responses.extend(hydration_script(
            "'sess-x1','sess-x2','sess-x3','sess-x4'",
            json!((1..=4)
                .map(|i| totals_row(&format!("sess-x{i}"), "claude-code"))
                .collect::<Vec<_>>()),
        ));
        responses.extend(hydration_script(
            "'sess-y1','sess-y2','sess-y3','sess-y4'",
            json!([
                totals_row("sess-y1", "codex"),
                totals_row("sess-y2", "claude-code"),
                totals_row("sess-y3", "claude-code"),
                totals_row("sess-y4", "claude-code"),
            ]),
        ));
        let (repo, state) = build_scripted_directory_repo(responses).await;

        let page = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    mode: None,
                    source_name: None,
                    ..directory_filter()
                },
                PageRequest {
                    limit: 1,
                    cursor: None,
                },
            )
            .await
            .expect("chunked page");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].session_id, "sess-y1");
        // The candidate page was short, so the directory is exhausted: no cursor.
        assert!(page.next_cursor.is_none());
        // 1 directory + 2 x 3 hydration. A second directory statement here
        // would be a full re-aggregation of the whole table.
        assert_script_consumed(&state, 7);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_directory_page_exhausts_the_hydration_budget_with_a_cursor() {
    scoped(async {
        // Every candidate is eliminated by the exact re-filter. Once the whole
        // over-fetch is hydrated the page returns empty but WITH a cursor, so
        // the caller can keep going instead of reading it as "no more results".
        // The cursor anchors on the last RESOLVED CANDIDATE, which is what
        // guarantees the next request makes progress rather than re-examining
        // the same rejects forever.
        let candidates = (1..=16_i64)
            .map(|i| candidate_row(&format!("sess-c{i:02}"), 1_767_400_000_000_i64 - i))
            .collect::<Vec<_>>();
        let mut responses = vec![ScriptedResponse::rows(
            &["FROM `moraine`.`mcp_session_directory` AS d", "LIMIT 16"],
            json!(candidates),
        )];
        for chunk in 0..4_i64 {
            let ids: &'static str = match chunk {
                0 => "'sess-c01','sess-c02','sess-c03','sess-c04'",
                1 => "'sess-c05','sess-c06','sess-c07','sess-c08'",
                2 => "'sess-c09','sess-c10','sess-c11','sess-c12'",
                _ => "'sess-c13','sess-c14','sess-c15','sess-c16'",
            };
            responses.extend(hydration_script(
                ids,
                json!((1..=4_i64)
                    .map(|i| totals_row(&format!("sess-c{:02}", chunk * 4 + i), "claude-code"))
                    .collect::<Vec<_>>()),
            ));
        }
        let (repo, state) = build_scripted_directory_repo(responses).await;

        let page = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    mode: None,
                    source_name: None,
                    ..directory_filter()
                },
                PageRequest {
                    limit: 1,
                    cursor: None,
                },
            )
            .await
            .expect("budget-exhausted page");

        assert!(page.items.is_empty());
        let cursor = page
            .next_cursor
            .expect("an exhausted hydration budget must still hand back a continuation");
        let payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&cursor).expect("cursor is base64"))
                .expect("cursor payload is json");
        assert_eq!(payload["session_id"], json!("sess-c16"));
        assert_eq!(
            payload["last_event_unix_ms"],
            json!(1_767_400_000_000_i64 - 16)
        );
        // 1 directory pass + MAX_HYDRATION_CHUNKS x 3 hydration = 13, the
        // ceiling. The pre-fix shape ran Phase A once per chunk for 16.
        assert_script_consumed(&state, 13);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_accepts_legacy_cursors_and_rejects_unknown_versions() {
    scoped(async {
        let (repo, _state) = build_directory_repo().await;
        let filter = directory_filter();

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
        let cursor = first.next_cursor.expect("next cursor");
        let payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&cursor).expect("cursor is base64"))
                .expect("cursor payload is json");
        assert_eq!(payload["version"], json!(2));

        let retoken = |version: Option<u64>| {
            let mut payload = payload.clone();
            match version {
                Some(version) => payload["version"] = json!(version),
                None => {
                    payload
                        .as_object_mut()
                        .expect("cursor object")
                        .remove("version");
                }
            }
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("re-encode cursor"))
        };

        // This repository serves the DIRECTORY path. A pre-#599 token carries
        // no `version` key, which decodes to the header path's version — so it
        // is a token this path did not mint, and it must be REFUSED rather
        // than resumed. The two paths anchor on different values (the header
        // path on the projector's exact aggregate, the directory path on the
        // live-generation `max_observed_event_time` it also reports), so
        // resuming it here would silently skip every session whose two anchors
        // straddle it — a gap the caller cannot see. A mismatch is recoverable:
        // restart the feed.
        let error = repo
            .list_mcp_sessions(
                filter.clone(),
                PageRequest {
                    limit: 2,
                    cursor: Some(retoken(None)),
                },
            )
            .await
            .expect_err("a header-minted token must not resume on the directory path");
        assert!(
            error
                .to_string()
                .contains("different list_sessions read path"),
            "cross-path cursor must report a path mismatch, got: {error}"
        );

        // The path's OWN token still resumes, so the rejection above is about
        // provenance and not a blanket refusal.
        let resumed = repo
            .list_mcp_sessions(
                filter.clone(),
                PageRequest {
                    limit: 2,
                    // 2 = MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY (the
                    // cursor module is crate-private, so the literal stands in
                    // here the same way the rejected versions below do).
                    cursor: Some(retoken(Some(2))),
                },
            )
            .await
            .expect("a directory-minted token resumes on the directory path");
        assert!(resumed.items.len() <= 2);

        for version in [1_u64, 3] {
            let error = repo
                .list_mcp_sessions(
                    filter.clone(),
                    PageRequest {
                        limit: 2,
                        cursor: Some(retoken(Some(version))),
                    },
                )
                .await
                .expect_err("unknown cursor versions are rejected");
            assert_eq!(
                error.to_string(),
                format!("invalid cursor: unsupported list_sessions cursor version {version}")
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_sessions_gates_on_the_same_readiness_key_as_the_open_cutover() {
    scoped(async {
        let (repo, state) = build_directory_repo().await;

        repo.list_mcp_sessions(
            directory_filter(),
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("directory page");

        let probes = state
            .readiness_probe_queries
            .lock()
            .expect("readiness probe lock")
            .clone();
        let state_read = probes
            .iter()
            .find(|query| !query.contains("FROM system.tables"))
            .expect("readiness probe should be captured");
        // `open_v2`, not `core_indexes`: the weaker coverage key is published
        // before the overlap audit runs, so gating on it would let listing read
        // indexes the `open` path refuses.
        assert!(
            state_read.contains("WHERE state_key = 'open_v2'"),
            "list_sessions must share the open cutover's readiness authority: {state_read}"
        );
        // Latched per repository: paging does not re-probe.
        repo.list_mcp_sessions(
            directory_filter(),
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .await
        .expect("second directory page");
        assert_eq!(
            state
                .readiness_probe_queries
                .lock()
                .expect("readiness probe lock")
                .len(),
            probes.len(),
            "the readiness verdict must be latched for this repository and its clones"
        );
    })
    .await;
}

/// A NEGATIVE readiness verdict must not be latched.
///
/// Readiness is monotonic once published, so caching `true` keeps the flip
/// one-way for the process. Caching `false` is a different claim: the backfill
/// publishes readiness later, and a pinned negative would hold every reader on
/// the fallback — and the monitor's page route on a hard 503 — until the daemon
/// is restarted.
#[tokio::test(flavor = "multi_thread")]
async fn an_unready_backend_reprobes_rather_than_pinning_itself_to_the_fallback() {
    scoped(async {
        let (repo, state) = build_repo().await;

        for _ in 0..2 {
            repo.list_mcp_sessions(
                directory_filter(),
                PageRequest {
                    limit: 2,
                    cursor: None,
                },
            )
            .await
            .expect("header-path page");
        }

        let probes = state
            .readiness_probe_queries
            .lock()
            .expect("readiness probe lock")
            .len();
        assert!(
            probes >= 2,
            "a not-ready backend must re-probe so it can adopt readiness without a restart, \
             saw {probes} probe(s)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_rejects_cursor_filter_mismatch() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_applies_session_origin_scope() {
    scoped(async {
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
            .find(|q| q.contains("current_headers AS") && q.contains("AS completed"))
            .expect("list_sessions query should be captured");

        assert!(list_query.contains("s.origin_cwd = '/work/project'"));
        assert!(list_query.contains("startsWith(s.origin_cwd, '/work/project/')"));
        assert!(list_query.contains("ORDER BY s.last_event_time DESC, s.session_id DESC"));
        assert!(list_query.contains("LIMIT 6"));
        assert!(!list_query.contains("argMin(cwd"));
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_mcp_sessions_rejects_cursor_from_differently_scoped_server() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn scoped_point_lookups_hide_out_of_scope_sessions() {
    scoped(async {
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
                    query.contains("FROM `moraine`.`mcp_open_publication_headers`")
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn scoped_point_lookups_serve_in_scope_sessions() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_returns_stable_summary_fields() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_returns_none_for_missing_session() {
    scoped(async {
        let (repo, _state) = build_repo().await;

        let metadata = repo
            .get_session_metadata("sess-missing")
            .await
            .expect("session metadata query succeeds");
        assert!(metadata.is_none());
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_rejects_invalid_session_id() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_session_metadata_keeps_empty_boundary_fields_when_summary_exists() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_includes_turn_summaries_and_latest_completion() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_uses_only_bounded_projection_queries() {
    scoped(async {
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
        assert!(queries[1].contains("FROM `moraine`.`mcp_open_publication_headers`"));
        assert!(queries[1].contains("WHERE s.session_id = 'sess-open'"));
        assert!(queries[2].contains("FROM `moraine`.`mcp_open_turns`"));
        assert!(queries[2]
            .contains("WHERE t.session_id = 'sess-open' AND t.slot = 0 AND t.generation = 100"));
        assert!(queries[3].contains("FROM `moraine`.`mcp_open_publication_headers`"));
        assert!(queries
            .iter()
            .all(|query| !query.contains("v_conversation_trace")));
        assert!(queries[1].contains("required_source_heads"));
        assert!(queries[1].contains("v_published_source_generation_history"));
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_retries_when_projection_head_changes_during_open() {
    scoped(async {
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
                    "FROM `moraine`.`mcp_open_publication_headers`",
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
                    "FROM `moraine`.`mcp_open_publication_headers`",
                    "s.session_id = 'sess-open'",
                ],
                json!([generation_101.clone()]),
            ),
            ScriptedResponse::rows(
                &[
                    "FROM `moraine`.`mcp_open_publication_headers`",
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
                    "FROM `moraine`.`mcp_open_publication_headers`",
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
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_session_missing_committed_header_skips_child_queries() {
    scoped(async {
        let responses = vec![
            ScriptedResponse::rows(
                &["mcp_open_projection_state", "state_key = 'global'"],
                json!([{ "ready": 1_u8 }]),
            ),
            ScriptedResponse::rows(
                &[
                    "FROM `moraine`.`mcp_open_publication_headers`",
                    "WHERE s.session_id = 'sess-missing-projection'",
                ],
                json!([]),
            ),
            ScriptedResponse::rows(
                &[
                    "toUInt8(count() > 0) AS exists",
                    "e.session_id = 'sess-missing-projection'",
                ],
                json!([{ "exists": 0_u8 }]),
            ),
        ];
        let (repo, state) = build_scripted_repo(responses).await;

        let session = repo
            .get_mcp_session("sess-missing-projection")
            .await
            .expect("missing committed header is a not-found result");
        assert!(session.is_none());
        assert_script_consumed(&state, 3);
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_turn_returns_compact_events_and_incomplete_state() {
    scoped(async {
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
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_turn_summary_skips_projected_event_json_and_keeps_handles() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_event_retries_stale_lookup_generation() {
    scoped(async {
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
                    "FROM `moraine`.`mcp_open_publication_headers`",
                    "s.session_id = 'sess-event'",
                ],
                json!([current_session.clone()]),
            ),
            ScriptedResponse::rows(
                &[
                    "toUInt8(count() > 0) AS exists",
                    "e.event_uid = 'evt-open-full'",
                ],
                json!([{ "exists": 1_u8 }]),
            ),
            ScriptedResponse::rows(
                &["mcp_open_projection_state", "state_key = 'global'"],
                json!([{ "ready": 1_u8 }]),
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
                    "FROM `moraine`.`mcp_open_publication_headers`",
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
                &["FROM `moraine`.`mcp_open_events` FINAL", "event_order IN"],
                json!(event_ref_rows()),
            ),
            ScriptedResponse::rows(
                &[
                    "FROM `moraine`.`mcp_open_publication_headers`",
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
        assert_script_consumed(&state, 11);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mcp_event_returns_full_content_and_navigation_refs() {
    scoped(async {
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
                || query.contains("mcp_open_publication_headers")
                || query.contains("mcp_open_turns")
                || query.contains("mcp_open_events")
        }));
        assert!(queries
            .iter()
            .all(|query| !query.contains("v_conversation_trace")));
        let lookup_query = queries
            .iter()
            .find(|query| query.contains("ORDER BY generation DESC, source_host ASC"))
            .expect("host-qualified MCP event lookup query");
        assert!(lookup_query.contains("SELECT\n  source_host,\n  event_uid"));
        let event_query = queries
            .iter()
            .find(|query| query.contains("previous_event_uid"))
            .expect("host-qualified MCP event content query");
        assert!(event_query.contains("e.source_host = 'host-a'"));
        assert!(event_query.contains("e.session_id = 'sess-event'"));
        let neighbor_query = queries
            .iter()
            .find(|query| query.contains("event_order IN"))
            .expect("order-qualified MCP event neighbor query");
        assert!(neighbor_query.contains("session_id = 'sess-event'"));
        assert!(neighbor_query.contains("event_order IN [1, 3]"));
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_supports_forward_cursor_pagination() {
    scoped(async {
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
            .find(|q| {
                q.contains("ORDER BY event_order ASC, event_uid ASC") && q.contains("LIMIT 3")
            })
            .expect("initial page query should be captured");
        assert!(initial_query.contains("WHERE session_id = 'sess_c'"));
        assert!(initial_query
            .contains("toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms"));

        let paged_query = queries
            .iter()
            .find(|q| q.contains("event_order > 2 OR (event_order = 2 AND event_uid > 'evt-2')"))
            .expect("cursor query should include deterministic pagination clause");
        assert!(paged_query.contains("ORDER BY event_order ASC, event_uid ASC"));
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_supports_reverse_direction_and_event_kind_filter() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_supports_reverse_cursor_pagination() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn list_session_events_rejects_cursor_with_mismatched_direction() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn web_feed_covers_variants_precedence_limit_order_and_canonical_source() {
    scoped(async {
        let response = ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_published_source_generation_history` AS history",
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn web_feed_propagates_backend_and_json_each_row_decode_errors() {
    scoped(async {
        let scenarios = [
            ScriptedResponse::failure(
                &["FROM `moraine`.`v_published_source_generation_history` AS history"],
                "web feed backend failed",
            ),
            ScriptedResponse::raw(
                &[
                    "FROM `moraine`.`v_published_source_generation_history` AS history",
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
    })
    .await;
}
