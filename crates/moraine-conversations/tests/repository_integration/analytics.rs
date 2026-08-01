use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn analytics_24h_uses_exact_four_request_canonical_wire_contract() {
    scoped(async {
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
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn analytics_all_six_ranges_use_distinct_wire_keys() {
    scoped(async {
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
    })
    .await;
}
