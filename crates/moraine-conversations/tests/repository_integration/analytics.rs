use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn session_analytics_assembles_canonical_views_and_public_steps() {
    let responses = vec![
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_session_summary` AS s",
                "FROM (SELECT * FROM `moraine`.`events` FINAL) AS meta_events",
                "WHERE meta_events.event_kind = 'session_meta'",
                "WHERE notEmpty(trimBoth(s.session_id))",
                "ORDER BY s.last_event_time DESC, s.session_id DESC",
                "LIMIT 1",
                "FORMAT JSONEachRow",
            ],
            json!([session_analytics_summary_row()]),
        )
        .forbidding(&["now() - INTERVAL"]),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_turn_summary`",
                "toInt64(toUnixTimestamp64Milli(parseDateTime64BestEffort(toString(started_at), 3, 'UTC'))) AS started_at_unix_ms",
                "toInt64(toUnixTimestamp64Milli(parseDateTime64BestEffort(toString(ended_at), 3, 'UTC'))) AS ended_at_unix_ms",
                "WHERE session_id IN ['analytics-session']",
                "ORDER BY session_id ASC, turn_seq ASC",
                "FORMAT JSONEachRow",
            ],
            session_analytics_turn_rows(),
        )
        .forbidding(&[
            "toInt64(toUnixTimestamp64Milli(started_at)) AS started_at_unix_ms",
            "toInt64(toUnixTimestamp64Milli(ended_at)) AS ended_at_unix_ms",
        ]),
        ScriptedResponse::rows(
            &[
                "FROM `moraine`.`v_conversation_trace` AS t",
                "toInt64(toUnixTimestamp64Milli(t.event_time)) AS event_unix_ms",
                "LEFT JOIN (SELECT * FROM `moraine`.`events` FINAL) AS e ON e.event_uid = t.event_uid",
                "WHERE t.session_id IN ['analytics-session']",
                "ORDER BY t.session_id ASC, t.event_order ASC",
                "FORMAT JSONEachRow",
            ],
            session_analytics_event_rows(),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;
    let repo: Arc<dyn ConversationRepository> = Arc::new(repo);

    let sessions = repo
        .list_session_analytics(SessionAnalyticsQuery {
            lookback: SessionLookback::All,
            limit: 0,
        })
        .await
        .expect("session analytics succeeds");

    assert_eq!(sessions.len(), 1);
    let session = &sessions[0];
    assert_eq!(session.summary.session_id, "analytics-session");
    assert_eq!(session.summary.total_turns, 2);
    assert_eq!(session.summary.mode, ConversationMode::ToolCalling);
    assert_eq!(
        session.summary.session_slug.as_deref(),
        Some("analytics-slug")
    );
    assert_eq!(
        session.summary.session_summary.as_deref(),
        Some("Analytics fixture session")
    );
    assert_eq!(session.harness, "codex");
    assert_eq!(session.source_name, "codex-jsonl");
    assert_eq!(session.trace_id, "trace-454");
    assert_eq!(session.first_user_text, "first user fallback");
    assert_eq!(session.models, vec!["gpt-x", "other"]);
    assert_eq!(
        session
            .turns
            .iter()
            .map(|turn| turn.summary.turn_seq)
            .collect::<Vec<_>>(),
        vec![1, 2],
        "canonical view turn numbers remain one-based"
    );

    let first_turn = &session.turns[0];
    assert_eq!(first_turn.model, "GPT-X");
    assert_eq!(first_turn.token_usage_buckets.get("input_text"), Some(&5));
    assert_eq!(first_turn.token_usage_buckets.get("output_text"), Some(&2));
    assert_eq!(first_turn.token_usage_buckets.get("reasoning"), Some(&7));
    assert_eq!(first_turn.steps.len(), 4);
    assert!(matches!(
        &first_turn.steps[0],
        SessionStep::User { text, .. } if text == "first user fallback"
    ));
    match &first_turn.steps[1] {
        SessionStep::Assistant {
            text,
            endpoint_kind,
            latency_ms,
            token_usage_buckets,
            token_usage_native_units,
            ..
        } => {
            assert_eq!(text, "full answer");
            assert_eq!(endpoint_kind, "generation");
            assert_eq!(*latency_ms, Some(42));
            assert_eq!(token_usage_buckets.get("reasoning"), Some(&7));
            assert!(!token_usage_buckets.contains_key("input_text"));
            assert_eq!(token_usage_native_units.get("usd"), Some(&0.5));
            assert!(!token_usage_native_units.contains_key("zero"));
        }
        other => panic!("expected assistant step, got {other:?}"),
    }
    match &first_turn.steps[2] {
        SessionStep::ToolCall {
            tool_name,
            call_id,
            arguments,
            latency_ms,
            is_error,
            result,
            ..
        } => {
            assert_eq!(tool_name, "Read");
            assert_eq!(call_id, "call-read");
            assert_eq!(arguments, &json!({ "path": "src/lib.rs" }));
            assert_eq!(*latency_ms, Some(17));
            assert!(*is_error);
            let result = result.as_ref().expect("paired tool result");
            assert_eq!(result.text, "read failed");
            assert_eq!(result.latency_ms, 600);
            assert!(result.is_error);
        }
        other => panic!("expected tool call step, got {other:?}"),
    }
    assert!(matches!(
        &first_turn.steps[3],
        SessionStep::Thinking { text, .. } if text == "consider the result"
    ));
    assert_eq!(
        session.turns[1].token_usage_buckets.get("output_text"),
        Some(&3)
    );
    assert_script_consumed(&state, 3);
}
#[tokio::test(flavor = "multi_thread")]
async fn session_analytics_empty_and_timed_limit_sql_are_deterministic() {
    let (repo, state) = build_scripted_repo(vec![ScriptedResponse::rows(
        &[
            "FROM `moraine`.`v_session_summary` AS s",
            "AND s.last_event_time >= now() - INTERVAL 86400 SECOND",
            "ORDER BY s.last_event_time DESC, s.session_id DESC",
            "LIMIT 200",
            "FORMAT JSONEachRow",
        ],
        json!([]),
    )])
    .await;

    let sessions = repo
        .list_session_analytics(SessionAnalyticsQuery {
            lookback: SessionLookback::TwentyFourHours,
            limit: u16::MAX,
        })
        .await
        .expect("empty analytics succeeds");
    assert!(sessions.is_empty());
    assert_script_consumed(&state, 1);
}
#[tokio::test(flavor = "multi_thread")]
async fn session_analytics_propagates_each_wire_stage_error() {
    let scenarios = vec![
        vec![ScriptedResponse::failure(
            &["FROM `moraine`.`v_session_summary` AS s"],
            "session stage summary failed",
        )],
        vec![
            ScriptedResponse::rows(
                &["FROM `moraine`.`v_session_summary` AS s"],
                json!([session_analytics_summary_row()]),
            ),
            ScriptedResponse::failure(
                &["FROM `moraine`.`v_turn_summary`"],
                "session stage turns failed",
            ),
        ],
        vec![
            ScriptedResponse::rows(
                &["FROM `moraine`.`v_session_summary` AS s"],
                json!([session_analytics_summary_row()]),
            ),
            ScriptedResponse::rows(
                &["FROM `moraine`.`v_turn_summary`"],
                session_analytics_turn_rows(),
            ),
            ScriptedResponse::failure(
                &["FROM `moraine`.`v_conversation_trace` AS t"],
                "session stage events failed",
            ),
        ],
    ];

    for (stage, responses) in scenarios.into_iter().enumerate() {
        let (repo, state) = build_scripted_repo(responses).await;
        let error = repo
            .list_session_analytics(SessionAnalyticsQuery {
                lookback: SessionLookback::All,
                limit: 1,
            })
            .await
            .expect_err("wire stage error propagates");
        assert!(
            error.to_string().contains("session stage"),
            "stage {stage} error: {error}"
        );
        assert_script_consumed(&state, stage + 1);
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_24h_uses_exact_four_request_canonical_wire_contract() {
    let responses = analytics_responses(
        "toInt64(86400)",
        "INTERVAL 3600 SECOND",
        json!([
            {
                "bucket_unix": 150_000_u64,
                "model": "gpt-5.3-codex-xhigh",
                "endpoint_kind": "responses",
                "bucket": "input_text",
                "tokens": 12_u64
            },
            {
                "bucket_unix": 153_600_u64,
                "model": "other",
                "endpoint_kind": "messages",
                "bucket": "output_text",
                "tokens": 8_u64
            }
        ]),
        json!([{
            "bucket_unix": 150_000_u64,
            "model": "gpt-5.3-codex-xhigh",
            "turns": 3_u64
        }]),
        json!([{
            "bucket_unix": 150_000_u64,
            "concurrent_sessions": 2_u64
        }]),
    );
    let (repo, state) = build_scripted_repo(responses).await;

    let snapshot = repo
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .expect("24h analytics succeeds");

    assert_eq!(snapshot.window.range, AnalyticsRange::TwentyFourHours);
    assert_eq!(snapshot.window.window_seconds, 86_400);
    assert_eq!(snapshot.window.bucket_seconds, 3_600);
    assert_eq!(snapshot.window.from_unix, 113_600);
    assert_eq!(snapshot.window.to_unix, 200_000);
    assert_eq!(snapshot.tokens.len(), 2);
    assert_eq!(snapshot.tokens[0].model, "gpt-5.3-codex-xhigh");
    assert_eq!(snapshot.tokens[0].tokens, 12);
    assert_eq!(snapshot.tokens[1].bucket, "output_text");
    assert_eq!(snapshot.turns[0].turns, 3);
    assert_eq!(snapshot.concurrent_sessions[0].concurrent_sessions, 2);
    assert_script_consumed(&state, 4);
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_all_six_ranges_use_distinct_wire_keys() {
    let cases = [
        (
            AnalyticsRange::FifteenMinutes,
            "toInt64(900)",
            "INTERVAL 60 SECOND",
        ),
        (
            AnalyticsRange::OneHour,
            "toInt64(3600)",
            "INTERVAL 300 SECOND",
        ),
        (
            AnalyticsRange::SixHours,
            "toInt64(21600)",
            "INTERVAL 900 SECOND",
        ),
        (
            AnalyticsRange::TwentyFourHours,
            "toInt64(86400)",
            "INTERVAL 3600 SECOND",
        ),
        (
            AnalyticsRange::SevenDays,
            "toInt64(604800)",
            "INTERVAL 21600 SECOND",
        ),
        (
            AnalyticsRange::ThirtyDays,
            "toInt64(2592000)",
            "INTERVAL 86400 SECOND",
        ),
    ];
    let mut responses = Vec::new();
    for (_, window, interval) in cases {
        responses.extend(analytics_responses(
            window,
            interval,
            json!([]),
            json!([]),
            json!([]),
        ));
    }
    let (repo, state) = build_scripted_repo(responses).await;

    for (range, _, _) in cases {
        let snapshot = repo
            .analytics_series(range)
            .await
            .expect("distinct analytics range succeeds");
        assert_eq!(snapshot.window.range, range);
    }
    assert_script_consumed(&state, 24);
}
