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
/// OPERATION. Until issue #603 WI-10 retired the projected-header fallback it
/// ran against both readers; the directory reader is the only one left, and
/// the checklist stays behavioral so a future second implementation inherits
/// it unchanged.
#[derive(Copy, Clone, Debug)]
enum ListPath {
    /// Issue-599 `mcp_session_directory` reader (`open_v2` published).
    Directory,
}

impl ListPath {
    const ALL: [Self; 1] = [Self::Directory];

    async fn repo(self) -> (ClickHouseConversationRepository, Arc<MockState>) {
        match self {
            Self::Directory => build_directory_repo().await,
        }
    }

    async fn scoped_repo(
        self,
        roots: &[&str],
    ) -> (ClickHouseConversationRepository, Arc<MockState>) {
        match self {
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
async fn list_sessions_semantics_hold_on_the_directory_path() {
    scoped(async {
        for path in ListPath::ALL {
            let (repo, _state) = path.repo().await;
            let first = assert_shared_list_sessions_semantics(&repo, path).await;
            assert!(first.next_cursor.is_some(), "{path:?}");
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
        // The page's OUTPUT is asserted by the shared issue-599 §4 checklist
        // (`list_sessions_semantics_hold_on_the_directory_path`); this test
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

/// The directory display form paired with [`candidate_row`]'s default keyset,
/// for candidates whose RENDERING is not what the test is about.
const CANDIDATE_DISPLAY_TIME: &str = "2026-01-01 10:10:00";

/// One directory candidate row. Content-free and time-free apart from the
/// keyset: `cand_last_ms` is what the page orders by, keysets on, mints its
/// cursor from AND reports, and `cand_last_time` is that same value's display
/// form from the same aggregate.
fn candidate_row(session_id: &str, cand_last_ms: i64) -> serde_json::Value {
    candidate_row_at(session_id, cand_last_ms, CANDIDATE_DISPLAY_TIME)
}

/// [`candidate_row`] with the two halves of the keyset paired explicitly, for
/// the tests that assert WHICH statement a rendered timestamp came from.
fn candidate_row_at(
    session_id: &str,
    cand_last_ms: i64,
    cand_last_time: &str,
) -> serde_json::Value {
    json!({
        "session_id": session_id,
        "cand_last_ms": cand_last_ms,
        "cand_last_time": cand_last_time,
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
        // numbers whenever an event version has been superseded, and ONE of
        // them is this operation's `updated_at` (issue-599 B1):
        //
        // * ORDERING, the CURSOR and the ITEM all read the directory value. It
        //   is the only one the next page's `HAVING` can compare against, so
        //   anchoring on anything else skips every session whose aggregate
        //   falls between — and the published contract says the response is
        //   sorted by the `updated_at` it reports, which is only true if the
        //   two are the same number.
        // * The hydrated value stays internal: Phase C re-filters the requested
        //   window against it, and nothing renders it.
        //
        // sess-p: directory 1_767_400_000_000, exact 1_767_300_000_000
        // sess-q: directory 1_767_350_000_000, exact 1_767_350_000_000
        // Keyset DESC is (p, q); hydrated DESC would be (q, p).
        let responses = {
            let mut responses = vec![ScriptedResponse::rows(
                &["FROM `moraine`.`mcp_session_directory` AS d", "LIMIT 16"],
                json!([
                    candidate_row_at("sess-p", 1_767_400_000_000_i64, "2026-01-02 22:26:40.000"),
                    candidate_row_at("sess-q", 1_767_350_000_000_i64, "2026-01-02 08:33:20.000"),
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
        // The RENDERED timestamp is the directory keyset, in both the millis and
        // its display form — the value the page is ORDERED and PAGED by, which
        // is what `docs/mcp-search-interface-spec.md` publishes. sess-p's
        // directory aggregate (1_767_400_000_000) sits above its hydrated exact
        // value (1_767_300_000_000), the re-inserted-event case, and the item
        // reports the former.
        //
        // MUTATION: report `totals.last_event_unix_ms` from
        // `hydrated_session_summary` instead of `keyset.last_unix_ms`. The run
        // panics FIRST on the order assertion above (`left: "sess-q", right:
        // "sess-p"`), never reaching this one; neutralise the preceding asserts
        // and the cursor assertion fails too, minting `sess-q`.
        //
        // An earlier revision of this comment claimed the reverse — that the
        // cursor assertion survives, so the page is "ordered by a number it does
        // not return". That diagnosis is inverted. Since round 4 the rendered
        // field IS the sort key and the cursor source, so a mutation moves all
        // three together: ordering and rendering stay in agreement, on the wrong
        // value, and the cursor is minted from a number Phase A's `HAVING`
        // cannot compare against — the page-2 skip this test exists to prevent.
        assert_eq!(
            page.items[0].last_event_unix_ms, 1_767_400_000_000_i64,
            "the item must report the value the page is ordered and paged by"
        );
        assert_eq!(
            page.items[0].last_event_time, "2026-01-02 22:26:40.000",
            "the display form must come from the same directory aggregate as the millis"
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

        // This repository serves the DIRECTORY path — the only path since
        // issue #603 WI-10. A pre-#599 token carries no `version` key, which
        // decodes to the retired header path's version, so it is a token this
        // path did not mint and it must be REFUSED rather than resumed. The
        // two paths anchored on different values (the header path on the
        // projector's exact aggregate, the directory path on the
        // live-generation `max_observed_event_time` it also reports), so
        // resuming one here would silently skip every session whose two
        // anchors straddle it — a gap the caller cannot see. A mismatch is
        // recoverable: restart the feed.
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

// ---------------------------------------------------------------------------
// issue-599 WI-09 — session DISCOVERY BY CONTENT.
// ---------------------------------------------------------------------------

/// The happy path, end to end through the mock: issue #597's bounded ranking
/// picks the candidates, the issue-599 hydration and fold turn them into the
/// SAME summary type the time-ordered feed serves, and ranked order survives.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_returns_ranked_summaries_from_the_shared_discovery_fold() {
    scoped(async {
        let (repo, _state) = build_directory_repo().await;

        let result = repo
            .search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(10),
                ..SessionSearchQuery::default()
            })
            .await
            .expect("whole-corpus session search");

        // `evt-c-42` outranks `evt-a-11` (12.5 vs 7.0), so `sess_c` leads. The
        // response is session-grained: two ranked EVENTS in two sessions become
        // two sessions, deduplicated by the session a hit sits in.
        assert_eq!(
            result
                .sessions
                .iter()
                .map(|session| session.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess_c", "sess_a"],
        );

        // Every summary field the feed reports is hydrated, not carried from
        // the ranking row. `total_turns`, `tool_calls` and the title chain only
        // exist after Phase B.
        let leader = &result.sessions[0];
        assert_eq!(leader.total_events, 30);
        assert_eq!(leader.tool_calls, 6);
        assert_eq!(leader.title.as_deref(), Some("Session C title"));
        assert_eq!(leader.session_slug.as_deref(), Some("project-c"));
        assert_eq!(leader.last_event_unix_ms, 1_767_435_000_000);
        assert_eq!(leader.last_event_time, "2026-01-03 10:10:00");
        assert!(!result.incomplete);
        // Nothing was cut: two ranked sessions for a requested ten, both
        // hydrated and both disclosed.
        assert!(!result.truncated);
        assert!(!result.hits_truncated);
        assert!(!result.dropped);
    })
    .await;
}

/// **The dedup guard.** Ranking is EVENT grained and a matching session is
/// normally matched several times. `two_distinct_events_in_one_turn` ranks two
/// genuinely different events INSIDE `sess_c`, which is the shape every other
/// fixture lacked — and without which the guard could not fail.
///
/// MUTATION: replace `if seen.insert(...) { ranked_session_ids.push(...) }` in
/// `search_session_summaries_impl` with an unconditional push; `sess_c` is then
/// returned twice, `result_count` counts hits while claiming to count sessions,
/// and this fails on the very first assertion.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_returns_one_row_per_session_when_a_session_is_hit_twice() {
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
            .search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(10),
                ..SessionSearchQuery::default()
            })
            .await
            .expect("whole-corpus session search");

        assert_eq!(
            result
                .sessions
                .iter()
                .map(|session| session.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess_c"],
            "two ranked hits inside one session are one result row",
        );
        // Two hits collapsed into one session is not "more sessions existed".
        assert!(!result.truncated);
        assert!(!result.dropped);
    })
    .await;
}

/// The hit-to-session fan-in the over-fetch exists for, with `limit` set below
/// the number of ranked HITS: the ranking is asked for `limit x
/// SESSION_SEARCH_HITS_PER_SESSION` events precisely because several of them
/// land in one session.
///
/// MUTATION: clamp the internal hit budget back to `self.cfg.max_results` —
/// `session_search_hit_budget(limit).min(max_results)` in
/// `search_session_summaries_impl` — the shipped shape before this fix. The
/// fixture's `max_results` is already 1, so the ranking then receives
/// `n_hits = 1`, sees only `evt-c-42`, and the assertion that the ranking was
/// asked for MORE hits than the sessions requested fails.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_over_fetches_hits_because_hits_cluster_inside_a_session() {
    scoped(async {
        // `max_results == 1` is the shape that made the old clamp inert: it is
        // simultaneously the session limit and the hit ceiling.
        let (repo, state) = build_repo_with_options(
            1,
            MockOptions {
                open_v2_reader_ready: Some(true),
                two_distinct_events_in_one_turn: true,
                ..MockOptions::default()
            },
        )
        .await;

        let result = repo
            .search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(1),
                ..SessionSearchQuery::default()
            })
            .await
            .expect("whole-corpus session search");

        assert_eq!(
            result
                .sessions
                .iter()
                .map(|session| session.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess_c"],
        );

        // The ranking statement's candidate window is `3 x (n_hits + 1)`
        // (`mcp_candidate_fetch_size`), so the internal hit budget is
        // observable in the SQL. One session was requested against a backend
        // whose `max_results` is also 1, and the budget must still be
        // `1 x SESSION_SEARCH_HITS_PER_SESSION = 4` hits: `3 x (4 + 1) = 15`.
        // The pre-fix shape clamped the budget to `max_results`, giving
        // `n_hits = 1` and a window of `3 x (1 + 1) = 6`.
        let queries = state.queries.lock().expect("queries lock").clone();
        let ranking = queries
            .iter()
            .find(|query| query.contains("FROM term_postings AS p"))
            .expect("a bounded ranking statement");
        let window = ranking
            .rsplit_once("\nLIMIT ")
            .and_then(|(_, tail)| tail.split_whitespace().next())
            .and_then(|value| value.parse::<u32>().ok())
            .expect("a ranking LIMIT");
        assert_eq!(
            window, 15,
            "the internal hit budget must not collapse to the caller-facing `max_results`",
        );
    })
    .await;
}

/// A ranked session the exact re-check removed is not silently absent. Nothing
/// refills it, so an answer shorter than `limit` must say it is a strict subset
/// of what the ranking offered rather than reading as "the corpus holds one".
///
/// MUTATION: hard-code `dropped: false` in `search_session_summaries_impl`;
/// the scoped arm below then claims completeness and this fails.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_reports_that_the_exact_recheck_shortened_the_answer() {
    scoped(async {
        let query = || SessionSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            ..SessionSearchQuery::default()
        };

        let (intact_repo, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_hydrated_cwd_for_sess_a: false,
                ..MockOptions::default()
            },
        )
        .await;
        let intact = intact_repo
            .search_session_summaries(query())
            .await
            .expect("in-scope search");
        assert_eq!(intact.sessions.len(), 2);
        assert!(
            !intact.dropped,
            "control arm: nothing was removed, so nothing may be reported as removed",
        );

        let (shortened_repo, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_hydrated_cwd_for_sess_a: true,
                ..MockOptions::default()
            },
        )
        .await;
        let shortened = shortened_repo
            .search_session_summaries(query())
            .await
            .expect("scoped search");
        assert_eq!(shortened.sessions.len(), 1);
        assert!(
            shortened.dropped,
            "an answer shortened by the exact re-check must not claim completeness",
        );
        // And it is NOT reported as "raise your limit": nothing more would come.
        assert!(!shortened.truncated);
    })
    .await;
}

/// **The readiness guard.** While the issue-598 canonical read indexes are
/// unpublished, `mcp_event_navigation` is EMPTY. Hydrating discovery from it
/// anyway answers every query with `sessions: []` — a confident "the whole
/// corpus was searched and nothing matched" — for a corpus that may hold
/// thousands of sessions. Before issue #603 WI-10 that also diverged from
/// `list_mcp_sessions`, which was still serving those very sessions from the
/// projected headers; with the fallback retired the requirement is stricter,
/// not weaker: both discovery surfaces must branch on the same latch and both
/// must REFUSE, because neither has anything else to read.
///
/// The fixture models **a store whose backfill has not started**: the canonical
/// relations are empty for every reader. That is the pre-backfill worst case,
/// and the shape that makes the branch observable at all. A not-ready store can
/// also be PARTIALLY backfilled — `open_v2.ready` is published only after the
/// coverage sweep completes and the overlap audit passes, so rows can be
/// present while coverage is incomplete — but that regime is out of scope on
/// both discovery surfaces precisely because both refuse the canonical read
/// model until the latch flips, rather than reading it and answering short.
///
/// MUTATION: delete the `if canonical_ready` branch in
/// `search_session_summaries_impl` and always call
/// `hydrate_session_list_chunk`; the mock's canonical relations answer nothing
/// on a not-ready backend and the first assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_serves_summaries_while_the_canonical_indexes_are_unpublished() {
    scoped(async {
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(false),
                ..MockOptions::default()
            },
        )
        .await;

        // Since issue #603 WI-10 retired the projected-header fallback, an
        // unpublished read index is a typed refusal on BOTH discovery
        // surfaces — never a confident empty answer, and never a divergence
        // between the two.
        let error = repo
            .search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(10),
                ..SessionSearchQuery::default()
            })
            .await
            .expect_err("a not-ready store must refuse, not serve an empty match set");
        assert!(
            error
                .to_string()
                .contains("canonical read indexes are not ready"),
            "{error}"
        );

        let feed_error = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    start_unix_ms: 0,
                    end_unix_ms: 1_800_000_000_000,
                    mode: None,
                    harness: None,
                    source_name: None,
                    sort: ConversationListSort::Desc,
                },
                PageRequest::default(),
            )
            .await
            .expect_err("the sibling surface refuses in the same regime");
        assert!(
            feed_error
                .to_string()
                .contains("canonical read indexes are not ready"),
            "{feed_error}"
        );

        let queries = state.queries.lock().expect("queries lock").clone();
        assert!(
            !queries
                .iter()
                .any(|query| query.contains("current_headers AS") || query.contains("mcp_open_")),
            "a refusal must not touch the retired projection: {queries:?}",
        );
    })
    .await;
}

/// **The scope guard, wired.** A ranked hit is not permission to disclose the
/// session it sits in.
///
/// This check is REDUNDANT with issue #597's own Phase 4 re-check, and
/// deliberately so. Both compute the identical
/// `ifNull(argMinIf(n.cwd, tuple(n.event_ts, n.event_uid), n.cwd != ''), '')`
/// over `navigation_live_from()` — `build_session_totals_batch_sql` and
/// `build_search_candidate_derivation_sql`'s `session_cwd` CTE — and the whole
/// request is pinned to one publication, so in production the two values are
/// the same value and cannot disagree. The fold-level check is kept anyway so
/// that ONE function decides disclosure for BOTH discovery surfaces: a
/// per-surface copy is a second place for a scope rule to rot, and a scope rule
/// that rots discloses sessions. The coherent authority for the rule itself is
/// the unit test `search_never_discloses_a_session_outside_the_configured_scope`.
///
/// The fixture below therefore drives an input combination the pinned
/// publication makes physically unreachable — `/repo` from the derivation arm
/// and `/elsewhere` from the totals arm inside one request — because that is
/// the only way to isolate the fold-level guard from the ranking-level one. It
/// is not a claim that the two relations can disagree.
///
/// The in-scope arm runs first and is not decoration: it proves the mock's
/// totals arm is still matching and still serving `sess_a`, so the scoped arm's
/// shorter answer can only be the guard.
///
/// MUTATION: delete the `if let Some(scope) = session_scope` block in
/// `ClickHouseConversationRepository::hydrated_session_summary`, or pass `None`
/// for the scope from `search_session_summaries_impl`; the scoped arm below
/// then discloses `sess_a` and this fails.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_never_discloses_a_session_outside_the_project_scope() {
    scoped(async {
        let query = || SessionSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            ..SessionSearchQuery::default()
        };

        let (in_scope_repo, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_hydrated_cwd_for_sess_a: false,
                ..MockOptions::default()
            },
        )
        .await;
        let in_scope = in_scope_repo
            .search_session_summaries(query())
            .await
            .expect("in-scope search");
        assert_eq!(
            in_scope
                .sessions
                .iter()
                .map(|session| session.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess_c", "sess_a"],
            "control arm: both ranked sessions hydrate inside /repo",
        );

        let (scoped_repo, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_hydrated_cwd_for_sess_a: true,
                ..MockOptions::default()
            },
        )
        .await;
        let scoped_result = scoped_repo
            .search_session_summaries(query())
            .await
            .expect("scoped search");
        assert_eq!(
            scoped_result
                .sessions
                .iter()
                .map(|session| session.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess_c"],
            "a session whose hydrated origin_cwd is outside the scope must not be disclosed",
        );
    })
    .await;
}

/// Ranking hydrates snippets, `text_content` and `payload_json` to score and
/// preview events. None of it may reach a discovery caller — the search
/// response is the same navigation-scalar-and-label shape the feed proved flat
/// under 50x fatter transcripts (issue-599 §5.3).
///
/// MUTATION: add any content-bearing field to `SessionSearchResults` (or to
/// `McpSessionListItem`) and populate it from `ranked.hits`; this fails.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_results_carry_no_transcript_content() {
    scoped(async {
        let (repo, _state) = build_directory_repo().await;

        let result = repo
            .search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(10),
                ..SessionSearchQuery::default()
            })
            .await
            .expect("whole-corpus session search");
        assert!(!result.sessions.is_empty(), "fixture must return sessions");

        // Serialize the whole result and assert on the KEYS, so a future field
        // cannot smuggle content in under a name this test never heard of.
        let payload = serde_json::to_value(&result).expect("serializable results");
        let mut keys = Vec::new();
        collect_keys(&payload, &mut keys);
        for forbidden in [
            "snippet",
            "text_content",
            "text_preview",
            "payload_json",
            "events",
            "turns",
            "hits",
        ] {
            assert!(
                !keys.iter().any(|key| key == forbidden),
                "search results must not carry {forbidden:?}: {payload}"
            );
        }

        // And the corpus fixture's own message bodies, by value.
        let rendered = payload.to_string();
        for body in [
            "best assistant event in session c with extra context",
            "weaker assistant event in session a with extra context",
        ] {
            assert!(
                !rendered.contains(body),
                "search results leaked message content {body:?}: {rendered}"
            );
        }
    })
    .await;
}

fn collect_keys(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, nested) in map {
                out.push(key.clone());
                collect_keys(nested, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_keys(item, out);
            }
        }
        _ => {}
    }
}

/// **The cross-surface agreement guard, through REAL hydration (issue-599 B1).**
///
/// The two surfaces select different sessions — one by recency, one by
/// relevance — but for a session BOTH return they must describe it identically.
/// The monitor derives `status` from `last_event_unix_ms` against a 60 s
/// activity window, so a disagreement there is not cosmetic: it renders one
/// session `active` in the feed and `completed` in search, from one store, at
/// one instant.
///
/// The fixture is what gives this test teeth. `directory_aggregate_ahead_of_
/// hydration_for_sess_a` puts `sess_a`'s Phase-A `cand_last_ms` exactly one
/// activity window above its hydrated `last_event_unix_ms` — the
/// re-inserted-event regime, and the only regime in which "which value does the
/// feed render?" is answerable. With the two equal (every other fixture) this
/// assertion cannot fail however the code is written.
///
/// MUTATION: give the SEARCH arm an `updated_at` of its own again — in
/// `search_session_summaries_impl`'s canonical arm, replace
/// `keysets.get(session_id.as_str())?.keyset()` with a `SessionKeyset` built
/// from `hydrated.get(session_id.as_str())?.totals.last_event_unix_ms`. The
/// search then reports 1_767_262_200_000 while the feed reports
/// 1_767_262_260_000 and this fails.
///
/// Note the mutation has to be applied to ONE surface: reporting the hydrated
/// value from the shared fold changes both, they agree again, and this test
/// stays green while `both_discovery_paths_report_the_directory_keyset_they_page_by`
/// and `list_mcp_sessions_directory_page_orders_and_anchors_on_the_directory_keyset`
/// are the ones that catch it.
#[tokio::test(flavor = "multi_thread")]
async fn both_discovery_surfaces_describe_one_session_identically() {
    scoped(async {
        let (repo, _state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(true),
                directory_aggregate_ahead_of_hydration_for_sess_a: true,
                ..MockOptions::default()
            },
        )
        .await;

        let feed = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    start_unix_ms: 0,
                    end_unix_ms: 1_800_000_000_000,
                    mode: None,
                    harness: None,
                    source_name: None,
                    sort: ConversationListSort::Desc,
                },
                PageRequest {
                    limit: 25,
                    cursor: None,
                },
            )
            .await
            .expect("session feed");

        let searched = repo
            .search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(10),
                ..SessionSearchQuery::default()
            })
            .await
            .expect("whole-corpus session search");

        let overlap: Vec<(&McpSessionListItem, &McpSessionListItem)> = searched
            .sessions
            .iter()
            .filter_map(|ranked| {
                feed.items
                    .iter()
                    .find(|listed| listed.session_id == ranked.session_id)
                    .map(|listed| (ranked, listed))
            })
            .collect();
        assert!(
            overlap
                .iter()
                .any(|(ranked, _)| ranked.session_id == "sess_a"),
            "the fixture must put the skewed session on BOTH surfaces, or this proves nothing",
        );
        for (ranked, listed) in overlap {
            assert_eq!(
                ranked, listed,
                "the two discovery surfaces described {} differently",
                ranked.session_id,
            );
        }
    })
    .await;
}

/// **The `dropped` disclosure guard (issue-599 WI-09).**
///
/// `dropped` plus `truncated` makes `limit - result_count` an exact count of
/// what the answer withheld. That is acceptable only because project scope can
/// never be one of the causes: ranking applies the configured scope while it
/// is still choosing candidates, so an out-of-scope session never enters the
/// ranked set and cannot be subtracted from it afterwards. Were it
/// otherwise, `?q=term&limit=50` on a `--project-only` backend would report an
/// exact, per-term count of activity outside the caller's scope.
///
/// The two arms of this test are the two ways a session can leave the answer:
///
/// * scope, removed DURING ranking — the answer is shorter and `dropped` stays
///   false, because nothing was subtracted after ranking;
/// * the hydrated-scope re-check, which is defence in depth and physically
///   unreachable under one pinned publication (see
///   `session_search_never_discloses_a_session_outside_the_project_scope`), and
///   is driven here only to show it is not what carries the count.
///
/// MUTATION: delete the `if let Some(scope)` block in
/// `search_mcp_event_page_v2`; `sess_a` then reaches the ranked set, is removed
/// by the post-hydration re-check instead, and the first arm's
/// `assert!(!dropped)` fails.
///
/// An earlier revision of this comment offered "delete the origin-scope push
/// in the v1 event-search builder" as an equivalent recipe. It was never
/// executed, and it could not fail this test: that builder served the
/// pre-cutover projected-header path, while this fixture sets
/// `open_v2_reader_ready: Some(true)` and therefore issues the canonical v2
/// statement, which never carried the clause. Issue #603 WI-10 deleted the
/// builder outright; scope on the surviving event-search path is guarded by
/// `search::search_mcp_events_applies_session_origin_scope`, not here.
///
/// That mutation only reaches `dropped` because the first arm ALSO sets
/// `out_of_scope_hydrated_cwd_for_sess_a`. Without it the ranking mutation is
/// still observable — `sess_a` gets disclosed and the `assert_eq!` above fails
/// first — but the run never evaluates the `dropped` assertion, so the bit this
/// test is named for would be pinned by nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_project_scope_removal_never_reaches_the_dropped_bit() {
    scoped(async {
        let query = || SessionSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            ..SessionSearchQuery::default()
        };

        // Scope removes `sess_a`'s only hit while RANKING. The answer is one
        // session shorter than the unscoped control…
        let (ranking_scoped, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_cwd_for_second_candidate: true,
                // Armed but unreachable while ranking does its job: `sess_a`
                // never gets hydrated, so this cannot affect the answer. It is
                // what makes the ranking guard's removal show up as `dropped`
                // rather than as an earlier, different failure.
                out_of_scope_hydrated_cwd_for_sess_a: true,
                ..MockOptions::default()
            },
        )
        .await;
        let ranked_out = ranking_scoped
            .search_session_summaries(query())
            .await
            .expect("scoped search");
        assert_eq!(
            ranked_out
                .sessions
                .iter()
                .map(|session| session.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess_c"],
            "an out-of-scope session must not be disclosed",
        );
        // …and `dropped` stays clear, so the count of what scope withheld is
        // not derivable from the envelope.
        assert!(
            !ranked_out.dropped,
            "a scope removal happens before ranking answers and must not be reported as a \
             post-ranking subtraction",
        );

        // The post-hydration re-check is what `dropped` reports, and it is a
        // different input entirely.
        let (hydration_scoped, _state) = build_scoped_directory_repo_with_options(
            &["/repo"],
            MockOptions {
                out_of_scope_hydrated_cwd_for_sess_a: true,
                ..MockOptions::default()
            },
        )
        .await;
        let hydrated_out = hydration_scoped
            .search_session_summaries(query())
            .await
            .expect("scoped search");
        assert_eq!(hydrated_out.sessions.len(), 1);
        assert!(hydrated_out.dropped);
    })
    .await;
}

/// **The over-fetch ceiling, at every reachable `limit` REGION (issue-599
/// WI-09, issue #597 §1).**
///
/// The internal hit budget sets the ranking's candidate window — the most
/// expensive stage of the request — so it is asserted here as the SQL the
/// request actually issued, not as the arithmetic
/// `session_search_hit_budget_holds_its_bounds_across_the_whole_reachable_domain`
/// already pins in a unit test.
///
/// The three cases are the three shapes of that function, and `limit = 50` is
/// the one the previous tests missed entirely — which is exactly where the
/// previous expression collapsed the fan-in to 1:1 and made `truncated`
/// unsettable.
///
/// MUTATION: delete the `.min(SESSION_SEARCH_HIT_BUDGET_MAX)` clamp in
/// `session_search_hit_budget`; the `limit = 25` window becomes
/// `min(3 x 101, 256) = 256` and that case fails.
/// MUTATION: raise `SESSION_SEARCH_HITS_PER_SESSION` from 4 to 100; the clamp
/// absorbs it at `limit = 25` and `limit = 50`, but `limit = 1` budgets 50
/// instead of 4 and its window becomes 153, so the first case fails. Covering
/// the small-`limit` region is what makes this test see that constant at all —
/// the single-shape test it replaced could not.
#[tokio::test(flavor = "multi_thread")]
async fn session_search_over_fetch_is_bounded_at_every_reachable_limit_region() {
    scoped(async {
        // `max_results = 25` is the shipped default (`config/moraine.toml`) and
        // 25 is what the monitor's search client asks for, so the backend here
        // is configured wide enough to let every region through and the
        // per-case `limit` is what varies.
        for (limit, expected_window, label) in [
            // budget = min(1 x 4, 50).max(2) = 4; 3 x (4 + 1) = 15.
            (1_u16, 15_u32, "the 4x fan-in region"),
            // budget = min(25 x 4, 50).max(37) = 50; 3 x (50 + 1) = 153.
            (25, 153, "the shipped default shape"),
            // budget = min(50 x 4, 50).max(75) = 75; 3 x (75 + 1) = 228. The
            // 1.5x floor, not the ceiling, is what decides this one.
            (50, 228, "the largest page a caller may ask for"),
        ] {
            let (repo, state) = build_repo_with_options(
                50,
                MockOptions {
                    open_v2_reader_ready: Some(true),
                    ..MockOptions::default()
                },
            )
            .await;

            repo.search_session_summaries(SessionSearchQuery {
                query: "hello world".to_string(),
                limit: Some(limit),
                ..SessionSearchQuery::default()
            })
            .await
            .expect("whole-corpus session search");

            let queries = state.queries.lock().expect("queries lock").clone();
            let ranking = queries
                .iter()
                .find(|query| query.contains("FROM term_postings AS p"))
                .expect("a bounded ranking statement");
            let window = ranking
                .rsplit_once("\nLIMIT ")
                .and_then(|(_, tail)| tail.split_whitespace().next())
                .and_then(|value| value.parse::<u32>().ok())
                .expect("a ranking LIMIT");
            assert_eq!(
                window, expected_window,
                "{label} (limit {limit}): candidate window changed",
            );
            assert!(
                window < 256,
                "{label} (limit {limit}): must not sit on the hard candidate ceiling: {window}",
            );
            // The fan-in is what the window is FOR: a window of `3 x (limit + 1)`
            // would mean the ranking was asked for exactly `limit` hits, which
            // bounds the distinct sessions at `limit` and makes
            // `truncated: ranked_sessions > limit` structurally unsettable.
            assert!(
                window > 3 * (u32::from(limit) + 1),
                "{label} (limit {limit}): the fan-in collapsed to 1:1",
            );
        }
    })
    .await;
}

/// **The one-verdict guard (issue-599 WI-09).**
///
/// A content search consults the issue-598 readiness latch at TWO points —
/// before ranking and before hydration — and both must see the SAME verdict.
/// That is still two consultations after issue #603 WI-10; what changed is
/// what disagreement costs. Before the retirement a split verdict ranked over
/// the `mcp_open_*` projection and hydrated from the canonical navigation
/// index. Now there is no second read model, so a split verdict inside one
/// request means it refuses at one step and serves at the other — a request
/// that is half-answered on a store the operator was told is not ready.
/// Probing separately also costs a second `mcp_read_index_state` point read on
/// every search of a not-ready store, which is exactly the store that cannot
/// afford it.
///
/// The backend is declared NOT ready on purpose. A positive verdict latches on
/// first success (`canonical_list_path_ready`), so a second probe would be
/// served from the latch and this count could not see it.
///
/// MUTATION: restore `if self.canonical_list_path_ready().await` inside
/// `search_mcp_event_page` (or drop `canonical_ready` from
/// `McpEventRankingOptions`); the count becomes 2 and this fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_content_search_resolves_canonical_readiness_exactly_once() {
    scoped(async {
        let (repo, state) = build_repo_with_options(
            100,
            MockOptions {
                open_v2_reader_ready: Some(false),
                ..MockOptions::default()
            },
        )
        .await;

        repo.search_session_summaries(SessionSearchQuery {
            query: "hello world".to_string(),
            limit: Some(10),
            ..SessionSearchQuery::default()
        })
        .await
        .expect_err("a not-ready store refuses content discovery (issue #603 WI-10)");

        // One logical probe is TWO statements — the `system.tables` existence
        // check and the state read itself (`ClickHouseClient::read_index_state`)
        // — so counting the state read alone is what counts VERDICTS.
        let probes = state
            .readiness_probe_queries
            .lock()
            .expect("readiness probe lock")
            .iter()
            .filter(|query| !query.contains("FROM system.tables"))
            .count();
        assert_eq!(
            probes, 1,
            "ranking and hydration must share one readiness verdict, not probe for one each",
        );
    })
    .await;
}
