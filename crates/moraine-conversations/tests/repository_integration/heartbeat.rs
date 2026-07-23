use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_reports_missing_table_without_read_query() {
    scoped(async {
        let (repo, state) = build_scripted_repo(vec![ScriptedResponse::rows(
            &[
                "SELECT name",
                "FROM system.columns",
                "WHERE database = 'moraine' AND table = 'ingest_heartbeats'",
                "ORDER BY position ASC",
                "FORMAT JSONEachRow",
            ],
            json!([]),
        )])
        .await;

        let heartbeat = repo
            .latest_ingest_heartbeat()
            .await
            .expect("missing heartbeat table is optional");
        assert!(!heartbeat.table_present);
        assert!(heartbeat.latest.is_none());
        assert_script_consumed(&state, 1);
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_reports_present_but_empty_table() {
    scoped(async {
        let responses = vec![
            ScriptedResponse::rows(
                &["FROM system.columns", "FORMAT JSONEachRow"],
                heartbeat_columns(&[]),
            ),
            ScriptedResponse::rows(
                &[
                    "FROM `moraine`.`ingest_heartbeats`",
                    "ORDER BY `ts` DESC, `host` DESC",
                    "LIMIT 1",
                    "FORMAT JSONEachRow",
                ],
                json!([]),
            ),
        ];
        let (repo, state) = build_scripted_repo(responses).await;

        let heartbeat = repo
            .latest_ingest_heartbeat()
            .await
            .expect("empty heartbeat table succeeds");
        assert!(heartbeat.table_present);
        assert!(heartbeat.latest.is_none());
        assert_script_consumed(&state, 2);
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_pre_017_row_uses_null_projections_for_absent_columns() {
    scoped(async {
        let mut row = heartbeat_row();
        row["service_version"] = json!("0.16.9");
        let responses = vec![
            ScriptedResponse::rows(
                &["FROM system.columns", "FORMAT JSONEachRow"],
                heartbeat_columns(&[]),
            ),
            ScriptedResponse::rows(
                &[
                    "FROM `moraine`.`ingest_heartbeats`",
                    "CAST(NULL, 'Nullable(String)') AS `watcher_backend`",
                    "CAST(NULL, 'Nullable(UInt64)') AS `watcher_error_count`",
                    "CAST(NULL, 'Nullable(UInt64)') AS `watcher_reset_count`",
                    "CAST(NULL, 'Nullable(UInt64)') AS `watcher_last_reset_unix_ms`",
                    "CAST(NULL, 'Nullable(String)') AS `backend_sinks`",
                    "FORMAT JSONEachRow",
                ],
                json!([row]),
            ),
        ];
        let (repo, state) = build_scripted_repo(responses).await;

        let heartbeat = repo
            .latest_ingest_heartbeat()
            .await
            .expect("pre-0.17 heartbeat succeeds");
        let latest = heartbeat.latest.expect("heartbeat row");
        assert_eq!(latest.service_version, "0.16.9");
        assert_eq!(latest.queue_depth, 4);
        assert!(latest.watcher_backend.is_none());
        assert!(latest.watcher_error_count.is_none());
        assert!(latest.watcher_reset_count.is_none());
        assert!(latest.watcher_last_reset_unix_ms.is_none());
        assert!(latest.backend_sinks.is_none());

        let queries = state.queries.lock().expect("queries lock");
        let read_query = &queries[1];
        for absent in [
            "`watcher_backend`,",
            "`watcher_error_count`,",
            "`watcher_reset_count`,",
            "`watcher_last_reset_unix_ms`,",
            "`backend_sinks`",
        ] {
            assert!(
                !read_query.lines().any(|line| line.trim() == absent),
                "absent column projected directly: {absent}\n{read_query}"
            );
        }
        drop(queries);
        assert_script_consumed(&state, 2);
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_partial_watcher_schema_reads_only_present_optional_columns() {
    scoped(async {
        let mut row = heartbeat_row();
        row["watcher_backend"] = json!("fsevents");
        row["watcher_error_count"] = json!(3_u64);
        let responses = vec![
            ScriptedResponse::rows(
                &["FROM system.columns", "FORMAT JSONEachRow"],
                heartbeat_columns(&["watcher_backend", "watcher_error_count"]),
            ),
            ScriptedResponse::rows(
                &[
                    "`watcher_backend`,",
                    "`watcher_error_count`,",
                    "CAST(NULL, 'Nullable(UInt64)') AS `watcher_reset_count`",
                    "CAST(NULL, 'Nullable(UInt64)') AS `watcher_last_reset_unix_ms`",
                    "CAST(NULL, 'Nullable(String)') AS `backend_sinks`",
                    "FROM `moraine`.`ingest_heartbeats`",
                    "FORMAT JSONEachRow",
                ],
                json!([row]),
            ),
        ];
        let (repo, state) = build_scripted_repo(responses).await;

        let latest = repo
            .latest_ingest_heartbeat()
            .await
            .expect("partial watcher heartbeat succeeds")
            .latest
            .expect("heartbeat row");
        assert_eq!(latest.watcher_backend.as_deref(), Some("fsevents"));
        assert_eq!(latest.watcher_error_count, Some(3));
        assert!(latest.watcher_reset_count.is_none());
        assert!(latest.backend_sinks.is_none());
        assert_script_consumed(&state, 2);
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_post_017_backend_sinks_handles_blank_valid_and_malformed_values() {
    scoped(async {
        let cases = [
            ("", json!({})),
            (
                "{\"clickhouse\":{\"healthy\":true},\"sqlite\":{\"healthy\":false}}",
                json!({
                    "clickhouse": { "healthy": true },
                    "sqlite": { "healthy": false }
                }),
            ),
            ("malformed-backend-sinks", json!("malformed-backend-sinks")),
        ];
        let optional = [
            "watcher_backend",
            "watcher_error_count",
            "watcher_reset_count",
            "watcher_last_reset_unix_ms",
            "backend_sinks",
        ];

        for (raw, expected) in cases {
            let mut row = heartbeat_row();
            row["watcher_backend"] = json!("fsevents");
            row["watcher_error_count"] = json!(1_u64);
            row["watcher_reset_count"] = json!(2_u64);
            row["watcher_last_reset_unix_ms"] = json!(1780307999000_u64);
            row["backend_sinks"] = json!(raw);
            let responses = vec![
                ScriptedResponse::rows(
                    &["FROM system.columns", "FORMAT JSONEachRow"],
                    heartbeat_columns(&optional),
                ),
                ScriptedResponse::rows(
                    &[
                        "`watcher_backend`,",
                        "`watcher_error_count`,",
                        "`watcher_reset_count`,",
                        "`watcher_last_reset_unix_ms`,",
                        "`backend_sinks`",
                        "FROM `moraine`.`ingest_heartbeats`",
                        "FORMAT JSONEachRow",
                    ],
                    json!([row]),
                ),
            ];
            let (repo, state) = build_scripted_repo(responses).await;

            let latest = repo
                .latest_ingest_heartbeat()
                .await
                .expect("post-0.17 heartbeat succeeds")
                .latest
                .expect("heartbeat row");
            assert_eq!(latest.watcher_backend.as_deref(), Some("fsevents"));
            assert_eq!(latest.watcher_reset_count, Some(2));
            assert_eq!(latest.backend_sinks, Some(expected));
            assert_script_consumed(&state, 2);
        }
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_propagates_column_probe_and_latest_read_errors() {
    scoped(async {
        let (probe_repo, probe_state) = build_scripted_repo(vec![ScriptedResponse::failure(
            &["FROM system.columns", "FORMAT JSONEachRow"],
            "heartbeat column probe failed",
        )])
        .await;
        let probe_error = probe_repo
            .latest_ingest_heartbeat()
            .await
            .expect_err("column probe failure propagates");
        assert!(probe_error
            .to_string()
            .contains("heartbeat column probe failed"));
        assert_script_consumed(&probe_state, 1);

        let (read_repo, read_state) = build_scripted_repo(vec![
            ScriptedResponse::rows(
                &["FROM system.columns", "FORMAT JSONEachRow"],
                heartbeat_columns(&[]),
            ),
            ScriptedResponse::failure(
                &["FROM `moraine`.`ingest_heartbeats`", "FORMAT JSONEachRow"],
                "heartbeat latest read failed",
            ),
        ])
        .await;
        let read_error = read_repo
            .latest_ingest_heartbeat()
            .await
            .expect_err("heartbeat read failure propagates");
        assert!(read_error
            .to_string()
            .contains("heartbeat latest read failed"));
        assert_script_consumed(&read_state, 2);
    })
    .await;
}
