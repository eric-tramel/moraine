//! Issue #600: typed budget errors at the repository boundary.
//!
//! Deadline/kill/resource failures from the transport map to
//! `RepoError::DeadlineExceeded` / `RepoError::ResourceExhausted`; every
//! other backend failure keeps the opaque `Backend` mapping; consistency
//! retries stay reserved for `ReadModelChanged`; and scope outcomes stay
//! scope outcomes (amendment A11).

use super::*;

fn interactive_budget_with_cap(
    deadline_seconds: f64,
    statement_cap: u32,
) -> moraine_config::ValidatedQueryBudget {
    let defaults = moraine_config::QueryBudgetsConfig::default();
    let cfg = moraine_config::QueryBudgetsConfig {
        interactive: moraine_config::QueryBudgetClassConfig {
            deadline_seconds,
            statement_cap,
            ..defaults.interactive
        },
        ..defaults
    };
    moraine_config::ValidatedQueryBudgets::from_config(&cfg)
        .expect("test budget validates")
        .interactive
}

#[tokio::test(flavor = "multi_thread")]
async fn server_timeout_maps_to_deadline_exceeded() {
    scoped(async {
        let (repo, state) = build_scripted_repo(vec![ScriptedResponse::failure(
            &[],
            "Code: 159. DB::Exception: Timeout exceeded: elapsed 2.0 seconds",
        )])
        .await;

        let error = repo
            .latest_ingest_heartbeat()
            .await
            .expect_err("timed-out read must fail");
        match error {
            RepoError::DeadlineExceeded { budget_note } => {
                assert!(
                    budget_note.contains("159"),
                    "note should carry the server code: {budget_note}"
                );
            }
            other => panic!("expected DeadlineExceeded, got {other:?}"),
        }
        assert_script_consumed(&state, 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_query_maps_to_deadline_exceeded() {
    scoped(async {
        let (repo, state) = build_scripted_repo(vec![ScriptedResponse::failure(
            &[],
            "Code: 394. DB::Exception: Query was cancelled",
        )])
        .await;

        let error = repo
            .latest_ingest_heartbeat()
            .await
            .expect_err("killed read must fail");
        assert!(
            matches!(error, RepoError::DeadlineExceeded { .. }),
            "expected DeadlineExceeded for a killed query, got {error:?}"
        );
        assert_script_consumed(&state, 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn server_resource_limits_map_to_resource_exhausted() {
    scoped(async {
        for body in [
            "Code: 241. DB::Exception: Memory limit (for query) exceeded",
            "Code: 396. DB::Exception: Limit for rows or bytes to read exceeded",
        ] {
            let (repo, state) =
                build_scripted_repo(vec![ScriptedResponse::failure(&[], body)]).await;
            let error = repo
                .latest_ingest_heartbeat()
                .await
                .expect_err("resource-limited read must fail");
            assert!(
                matches!(error, RepoError::ResourceExhausted { .. }),
                "expected ResourceExhausted for {body:?}, got {error:?}"
            );
            assert_script_consumed(&state, 1);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unclassified_server_errors_stay_backend_errors() {
    scoped(async {
        for body in [
            "Code: 60. DB::Exception: Table moraine.missing does not exist",
            "no exception code in this body at all",
        ] {
            let (repo, state) =
                build_scripted_repo(vec![ScriptedResponse::failure(&[], body)]).await;
            let error = repo
                .latest_ingest_heartbeat()
                .await
                .expect_err("failed read must fail");
            match error {
                RepoError::Backend(message) => {
                    assert!(
                        message.contains(body),
                        "message should keep the body: {message}"
                    );
                }
                other => panic!("expected Backend for {body:?}, got {other:?}"),
            }
            assert_script_consumed(&state, 1);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_budget_errors_take_the_consistency_non_retry_arm() {
    scoped(async {
        // Only ReadModelChanged re-runs the publication-consistent operation.
        // A typed deadline error must propagate after exactly one attempt: one
        // snapshot capture, one operation read, no revalidation, no retry.
        let (repo, state) = build_scripted_repo(vec![ScriptedResponse::failure(
            &[],
            "Code: 159. DB::Exception: Timeout exceeded: elapsed 2.0 seconds",
        )])
        .await;

        let error = repo
            .list_mcp_sessions(
                McpSessionListFilter {
                    start_unix_ms: 1767261600000_i64,
                    end_unix_ms: 1767500000000_i64,
                    mode: None,
                    harness: None,
                    source_name: None,
                    sort: ConversationListSort::Desc,
                },
                PageRequest {
                    limit: 2,
                    cursor: None,
                },
            )
            .await
            .expect_err("timed-out consistent read must fail");
        assert!(
            matches!(error, RepoError::DeadlineExceeded { .. }),
            "expected DeadlineExceeded, got {error:?}"
        );

        assert_script_consumed(&state, 1);
        let snapshot_queries = state
            .publication_snapshot_queries
            .lock()
            .expect("publication snapshot query lock");
        assert_eq!(
            snapshot_queries.len(),
            1,
            "no revalidation and no retry after a typed budget error"
        );
        assert!(snapshot_queries[0].contains("moraine:publication_snapshot:capture"));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exhausted_statement_cap_surfaces_as_resource_exhausted() {
    // Cap 1: the publication snapshot capture consumes the only statement,
    // so the operation's first read is refused locally — no server statement
    // is issued for it — and the typed error propagates without a retry.
    let (repo, state) = build_repo().await;
    let budget = interactive_budget_with_cap(30.0, 1);

    let error = QueryEnvelope::new("request", QueryClass::Interactive, &budget)
        .scope(repo.list_mcp_sessions(
            McpSessionListFilter {
                start_unix_ms: 1767261600000_i64,
                end_unix_ms: 1767500000000_i64,
                mode: None,
                harness: None,
                source_name: None,
                sort: ConversationListSort::Desc,
            },
            PageRequest {
                limit: 2,
                cursor: None,
            },
        ))
        .await
        .expect_err("cap-exhausted read must fail");
    match error {
        RepoError::ResourceExhausted { budget_note } => {
            assert!(
                budget_note.contains("cap"),
                "note should name the statement cap: {budget_note}"
            );
        }
        other => panic!("expected ResourceExhausted, got {other:?}"),
    }

    // The refused statement never reached the server.
    let queries = state.queries.lock().expect("queries lock");
    assert!(
        queries.is_empty(),
        "locally refused statements must not reach the server: {queries:#?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scope_outcomes_stay_scope_outcomes_under_an_active_envelope() {
    // Amendment A11: budget classification must never reshape scope/auth
    // outcomes — an out-of-scope session is still Ok(None), not a typed
    // budget error, when the request runs inside an envelope.
    let (repo, _state) = build_scoped_repo(&["/work/project"]).await;
    let budget = interactive_test_budget(15.0);

    let session = QueryEnvelope::new("request", QueryClass::Interactive, &budget)
        .scope(repo.get_mcp_session("sess-out-of-scope"))
        .await
        .expect("scope rejection is not an error");
    assert!(session.is_none(), "out-of-scope session must stay hidden");
}
