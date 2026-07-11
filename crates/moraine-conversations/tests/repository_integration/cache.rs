use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn search_mcp_events_result_cache_reuses_repository_across_requests() {
    let (repo, state) = build_repo().await;
    let query = SearchMcpEventsQuery {
        query: "hello world".to_string(),
        n_hits: Some(2),
        session_id: Some("sess_c".to_string()),
        event_types: Some(vec![
            McpEventType::UserInput,
            McpEventType::AssistantResponse,
            McpEventType::ToolResponse,
        ]),
        min_score: Some(0.0),
        min_should_match: Some(1),
        ..SearchMcpEventsQuery::default()
    };

    let first = repo
        .search_mcp_events(query.clone())
        .await
        .expect("first MCP event search");
    assert!(first.truncated);
    assert!(first.stats.truncated);
    let requests_after_first = state.queries.lock().expect("queries lock").len();
    assert!(requests_after_first > 0);

    let second = repo
        .search_mcp_events(query.clone())
        .await
        .expect("cached MCP event search");
    assert_ne!(first.query_id, second.query_id);
    assert!(second.truncated);
    assert!(second.stats.truncated);
    assert_eq!(
        state.queries.lock().expect("queries lock").len(),
        requests_after_first,
        "identical MCP search should issue no additional ClickHouse requests"
    );

    let mut first_payload = serde_json::to_value(&first).expect("serialize first result");
    first_payload
        .as_object_mut()
        .expect("result object")
        .remove("query_id");
    first_payload["stats"]
        .as_object_mut()
        .expect("stats object")
        .remove("took_ms");
    let mut second_payload = serde_json::to_value(&second).expect("serialize second result");
    second_payload
        .as_object_mut()
        .expect("result object")
        .remove("query_id");
    second_payload["stats"]
        .as_object_mut()
        .expect("stats object")
        .remove("took_ms");
    assert_eq!(first_payload, second_payload);

    let requests_before_changed_option = state.queries.lock().expect("queries lock").len();
    repo.search_mcp_events(SearchMcpEventsQuery {
        turn_seq: Some(2),
        ..query
    })
    .await
    .expect("MCP event search with changed turn scope");
    assert!(
        state.queries.lock().expect("queries lock").len() > requests_before_changed_option,
        "changed normalized search semantics must miss the result cache"
    );
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_cache_hit_and_distinct_key_are_request_count_proven() {
    let mut responses = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    responses.extend(analytics_responses(
        "toInt64(3600)",
        "INTERVAL 300 SECOND",
        json!([]),
        json!([]),
        json!([]),
    ));
    let (repo, state) = build_scripted_repo(responses).await;
    let clone = repo.clone();

    let first = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("cache miss succeeds");
    let hit = clone
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("same-key cache hit succeeds");
    assert_eq!(first, hit);
    assert_eq!(state.queries.lock().expect("queries lock").len(), 4);

    let distinct = repo
        .analytics_series(AnalyticsRange::OneHour)
        .await
        .expect("distinct-key cache miss succeeds");
    assert_eq!(distinct.window.range, AnalyticsRange::OneHour);
    assert_eq!(distinct.window.window_seconds, 3_600);
    assert_eq!(distinct.window.bucket_seconds, 300);
    assert_script_consumed(&state, 8);
}
#[tokio::test]
async fn analytics_expired_entry_refills_from_the_repository() {
    let mut responses = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([{
            "bucket_unix": 100_000_u64,
            "model": "model",
            "endpoint_kind": "generation",
            "bucket": "input_text",
            "tokens": 1_u64
        }]),
        json!([]),
        json!([]),
    );
    responses.extend(analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([{
            "bucket_unix": 100_000_u64,
            "model": "model",
            "endpoint_kind": "generation",
            "bucket": "input_text",
            "tokens": 2_u64
        }]),
        json!([]),
        json!([]),
    ));
    let (repo, state) = build_scripted_repo(responses).await;

    let first = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("initial cache fill succeeds");
    assert_eq!(first.tokens[0].tokens, 1);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();
    let refilled = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("stale cache entry refills");
    assert_eq!(refilled.tokens[0].tokens, 2);
    assert_script_consumed(&state, 8);
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_stage_error_is_not_cached_and_empty_retry_succeeds() {
    let success = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    let mut responses = vec![
        success[0].clone(),
        ScriptedResponse::failure(
            &[
                "ARRAY JOIN mapKeys(e.token_usage_buckets)",
                "FORMAT JSONEachRow",
            ],
            "analytics token stage failed",
        ),
    ];
    responses.extend(success);
    let (repo, state) = build_scripted_repo(responses).await;

    let error = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect_err("token stage failure propagates");
    assert!(error.to_string().contains("analytics token stage failed"));

    let retry = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("failed load is not cached");
    assert!(retry.tokens.is_empty());
    assert!(retry.turns.is_empty());
    assert!(retry.concurrent_sessions.is_empty());
    assert_script_consumed(&state, 6);
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_anchor_decode_and_late_stage_errors_are_not_cached() {
    let success = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    let scenarios = [
        (
            "anchor",
            vec![ScriptedResponse::failure(
                &["database_now_unix", "FORMAT JSONEachRow"],
                "analytics anchor stage failed",
            )],
            5,
        ),
        (
            "decode",
            vec![ScriptedResponse::raw(
                &["database_now_unix", "FORMAT JSONEachRow"],
                "not-json\n",
            )],
            5,
        ),
        (
            "turn",
            vec![
                success[0].clone(),
                success[1].clone(),
                ScriptedResponse::failure(
                    &[
                        "uniqExact(tuple(e.session_id, e.request_id))",
                        "FORMAT JSONEachRow",
                    ],
                    "analytics turn stage failed",
                ),
            ],
            7,
        ),
        (
            "concurrency",
            vec![
                success[0].clone(),
                success[1].clone(),
                success[2].clone(),
                ScriptedResponse::failure(
                    &["uniqExact(session_stream_key)", "FORMAT JSONEachRow"],
                    "analytics concurrency stage failed",
                ),
            ],
            8,
        ),
    ];

    for (stage, mut responses, expected_requests) in scenarios {
        responses.extend(success.clone());
        let (repo, state) = build_scripted_repo(responses).await;
        repo.analytics_series(AnalyticsRange::TwentyFourHours)
            .await
            .expect_err(stage);
        let retry = repo
            .analytics_series(AnalyticsRange::TwentyFourHours)
            .await
            .unwrap_or_else(|error| panic!("{stage} retry failed: {error}"));
        assert!(retry.tokens.is_empty());
        assert!(retry.turns.is_empty());
        assert!(retry.concurrent_sessions.is_empty());
        assert_script_consumed(&state, expected_requests);
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_same_key_concurrency_coalesces_to_one_wire_load() {
    let (repo, state) = build_scripted_repo(analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    ))
    .await;
    let repo = Arc::new(repo);

    let (left, right) = tokio::join!(
        repo.analytics_series(AnalyticsRange::TwentyFourHours),
        repo.analytics_series(AnalyticsRange::TwentyFourHours)
    );
    assert_eq!(
        left.expect("first concurrent load"),
        right.expect("coalesced load")
    );
    assert_script_consumed(&state, 4);
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_different_ranges_fill_concurrently() {
    let first_anchor_reached = Arc::new(Notify::new());
    let first_anchor_release = Arc::new(Notify::new());
    let second_anchor_reached = Arc::new(Notify::new());
    let second_anchor_release = Arc::new(Notify::new());
    let first_fill_reached_last_stage = Arc::new(Notify::new());
    let first_fill_release_last_stage = Arc::new(Notify::new());
    let anchor_row = json!([{
        "scan_from_unix": 100_000_u64,
        "scan_to_unix": 200_000_u64,
        "display_to_unix": 200_000_u64
    }]);
    let generic_anchor = |reached: Arc<Notify>, release: Arc<Notify>| {
        ScriptedResponse::rows(
            &[
                "toInt64(toUnixTimestamp(now())) AS database_now_unix",
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS e",
            ],
            anchor_row.clone(),
        )
        .blocked(reached, release)
    };
    let token = || {
        ScriptedResponse::rows(
            &[
                "ARRAY JOIN mapKeys(e.token_usage_buckets)",
                "FORMAT JSONEachRow",
            ],
            json!([]),
        )
    };
    let turns = || {
        ScriptedResponse::rows(
            &[
                "uniqExact(tuple(e.session_id, e.request_id))",
                "FORMAT JSONEachRow",
            ],
            json!([]),
        )
    };
    let concurrency = || {
        ScriptedResponse::rows(
            &["uniqExact(session_stream_key)", "FORMAT JSONEachRow"],
            json!([]),
        )
    };
    let responses = vec![
        generic_anchor(first_anchor_reached.clone(), first_anchor_release.clone()),
        generic_anchor(second_anchor_reached.clone(), second_anchor_release.clone()),
        token(),
        turns(),
        concurrency().blocked(
            first_fill_reached_last_stage.clone(),
            first_fill_release_last_stage.clone(),
        ),
        token(),
        turns(),
        concurrency(),
    ];
    let (repo, state) = build_scripted_repo(responses).await;
    let repo = Arc::new(repo);

    let one_hour_repo = repo.clone();
    let one_hour = tokio::spawn(async move {
        one_hour_repo
            .analytics_series(AnalyticsRange::OneHour)
            .await
    });
    let day_repo = repo.clone();
    let day = tokio::spawn(async move {
        day_repo
            .analytics_series(AnalyticsRange::TwentyFourHours)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), first_anchor_reached.notified())
        .await
        .expect("first range reached its anchor");
    tokio::time::timeout(Duration::from_secs(2), second_anchor_reached.notified())
        .await
        .expect("second range reached its anchor before the first was released");

    first_anchor_release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        first_fill_reached_last_stage.notified(),
    )
    .await
    .expect("first range reached its final stage");
    first_fill_release_last_stage.notify_one();
    second_anchor_release.notify_one();

    let (one_hour, day) = tokio::join!(one_hour, day);
    assert_eq!(
        one_hour
            .expect("one-hour task joined")
            .expect("one-hour fill succeeds")
            .window
            .range,
        AnalyticsRange::OneHour
    );
    assert_eq!(
        day.expect("day task joined")
            .expect("day fill succeeds")
            .window
            .range,
        AnalyticsRange::TwentyFourHours
    );
    assert_script_consumed(&state, 8);
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_cancelled_fill_releases_slot_and_recovery_is_cached() {
    let success = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([]),
        json!([]),
        json!([]),
    );
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut responses = vec![success[0].clone().blocked(reached.clone(), release.clone())];
    responses.extend(success);
    let (repo, state) = build_scripted_repo(responses).await;
    let repo = Arc::new(repo);

    let first_repo = repo.clone();
    let first = tokio::spawn(async move {
        first_repo
            .analytics_series(AnalyticsRange::TwentyFourHours)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), reached.notified())
        .await
        .expect("scripted analytics load reached the wire barrier");
    first.abort();
    release.notify_one();
    assert!(first
        .await
        .expect_err("fill task was aborted")
        .is_cancelled());

    let recovered = tokio::time::timeout(
        Duration::from_secs(2),
        repo.analytics_series(AnalyticsRange::TwentyFourHours),
    )
    .await
    .expect("cancelled fill released the range slot")
    .expect("recovery fill succeeds");
    let hit = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("recovery result was cached");
    assert_eq!(recovered, hit);
    assert_script_consumed(&state, 5);
}
