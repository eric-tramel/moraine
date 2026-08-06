use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn clickhouse_categories_map_to_distinct_repository_errors() {
    let scenarios = [
        (159, "deadline", "deadline_exceeded"),
        (241, "resource", "resource_exhausted"),
        (999, "backend", "backend"),
    ];

    for (code, label, expected) in scenarios {
        let response = ScriptedResponse::failure(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            Box::leak(format!("Code: {code}. DB::Exception: {label}").into_boxed_str()),
        );
        let (repo, state) = build_scripted_repo(vec![response]).await;
        let error = repo
            .latest_ingest_heartbeat()
            .await
            .expect_err("typed ClickHouse failure");
        match expected {
            "deadline_exceeded" => assert!(matches!(error, RepoError::DeadlineExceeded(_))),
            "resource_exhausted" => assert!(matches!(error, RepoError::ResourceExhausted(_))),
            "backend" => assert!(matches!(error, RepoError::Backend(_))),
            _ => unreachable!(),
        }
        assert_script_consumed(&state, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_cancellation_maps_to_cancelled_without_egress() {
    let (repo, state) = build_repo().await;
    let owner = QueryOwner::new(&repo.runtime(), QueryWorkload::Mcp).expect("owner");
    owner.cancel(QueryCause::Explicit);

    let error = owner
        .scope(repo.unowned().latest_ingest_heartbeat())
        .await
        .expect_err("cancelled owner fails closed");

    assert!(matches!(error, RepoError::Cancelled(_)));
    assert!(state.queries.lock().expect("query lock").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_owner_maps_to_backend_without_egress() {
    let (repo, state) = build_repo().await;

    let error = repo
        .unowned()
        .latest_ingest_heartbeat()
        .await
        .expect_err("missing owner fails closed");

    assert!(
        matches!(error, RepoError::Backend(message) if message.contains("explicit QueryOwner"))
    );
    assert!(state.queries.lock().expect("query lock").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn detached_search_telemetry_starts_a_background_owner() {
    let (repo, state) = build_repo().await;
    let owner = QueryOwner::new(&repo.runtime(), QueryWorkload::Mcp).expect("request owner");

    owner
        .scope(repo.unowned().search_events(SearchEventsQuery {
            query: "hello world".to_string(),
            source: Some("ownership-test".to_string()),
            limit: Some(2),
            min_score: Some(0.0),
            min_should_match: Some(1),
            ..SearchEventsQuery::default()
        }))
        .await
        .expect("search succeeds");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let has_background = state
                .query_ids
                .lock()
                .expect("query id lock")
                .iter()
                .flatten()
                .any(|query_id| query_id.starts_with("moraine-background-"));
            if has_background {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background telemetry egress");

    let query_ids = state.query_ids.lock().expect("query id lock");
    assert!(query_ids
        .iter()
        .flatten()
        .any(|id| id.starts_with("moraine-mcp-")));
    assert!(query_ids
        .iter()
        .flatten()
        .any(|id| id.starts_with("moraine-background-")));
}
