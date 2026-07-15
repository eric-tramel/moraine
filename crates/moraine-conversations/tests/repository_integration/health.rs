use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn store_health_maps_all_successful_probe_facts() {
    let responses = vec![
        ScriptedResponse::raw(&["SELECT 1"], "1\n"),
        ScriptedResponse::raw(
            &["SELECT version() AS version"],
            json_envelope(json!([{ "version": "25.8.1.1" }])),
        ),
        ScriptedResponse::rows(
            &[
                "FROM system.databases",
                "WHERE name = 'moraine'",
                "FORMAT JSONEachRow",
            ],
            json!([{ "exists": 1_u8 }]),
        ),
        ScriptedResponse::rows(
            &[
                "FROM system.metrics",
                "WHERE metric IN ('TCPConnection', 'HTTPConnection', 'MySQLConnection', 'PostgreSQLConnection', 'InterserverConnection')",
                "ORDER BY metric ASC",
                "FORMAT JSONEachRow",
            ],
            json!([
                { "metric": "HTTPConnection", "value": 2_u64 },
                { "metric": "InterserverConnection", "value": 5_u64 },
                { "metric": "MySQLConnection", "value": 3_u64 },
                { "metric": "PostgreSQLConnection", "value": 4_u64 },
                { "metric": "TCPConnection", "value": 7_u64 },
                { "metric": "TCPConnection", "value": 1_u64 }
            ]),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let health = repo
        .read_store_health()
        .await
        .expect("health probes succeed");

    assert!(matches!(health.ping, StoreProbe::Available(ms) if ms >= 0.0));
    assert_eq!(
        health.version,
        StoreProbe::Available("25.8.1.1".to_string())
    );
    assert_eq!(health.database_exists, StoreProbe::Available(true));
    match health.connections {
        StoreProbe::Available(metrics) => {
            assert_eq!(metrics.tcp, 8);
            assert_eq!(metrics.http, 2);
            assert_eq!(metrics.mysql, 3);
            assert_eq!(metrics.postgres, 4);
            assert_eq!(metrics.interserver, 5);
            assert_eq!(metrics.total, 22);
        }
        other => panic!("expected connection metrics, got {other:?}"),
    }
    assert_script_consumed(&state, 4);
}
#[tokio::test(flavor = "multi_thread")]
async fn store_health_keeps_each_probe_failure_independent() {
    let responses = vec![
        ScriptedResponse::failure(&["SELECT 1"], "health ping failed"),
        ScriptedResponse::failure(&["SELECT version() AS version"], "health version failed"),
        ScriptedResponse::failure(&["FROM system.databases"], "health database failed"),
        ScriptedResponse::failure(&["FROM system.metrics"], "health connections failed"),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let health = repo
        .read_store_health()
        .await
        .expect("probe failures are facts, not method errors");

    assert!(matches!(
        health.ping,
        StoreProbe::Failed { ref message } if message.contains("health ping failed")
    ));
    assert!(matches!(
        health.version,
        StoreProbe::Failed { ref message } if message.contains("health version failed")
    ));
    assert!(matches!(
        health.database_exists,
        StoreProbe::Failed { ref message } if message.contains("health database failed")
    ));
    assert!(matches!(
        health.connections,
        StoreProbe::Failed { ref message } if message.contains("health connections failed")
    ));
    assert_script_consumed(&state, 4);
}
#[tokio::test(flavor = "multi_thread")]
async fn diagnostics_maps_doctor_partial_report_and_ping_short_circuit() {
    let required_tables = json!([
        { "name": "raw_events" },
        { "name": "events" },
        { "name": "event_links" },
        { "name": "tool_io" },
        { "name": "ingest_checkpoints" },
        { "name": "ingest_heartbeats" },
        { "name": "search_documents" },
        { "name": "search_postings" },
        { "name": "search_conversation_terms" },
        { "name": "search_term_stats" },
        { "name": "search_corpus_stats" },
        { "name": "search_query_log" },
        { "name": "search_hit_log" },
        { "name": "search_interaction_log" },
        { "name": "schema_migrations" }
    ]);
    let responses = vec![
        ScriptedResponse::raw(&["SELECT 1"], "1\n"),
        ScriptedResponse::failure(
            &["SELECT version() AS version"],
            "doctor version probe failed",
        ),
        ScriptedResponse::raw(
            &[
                "SELECT toUInt8(count() > 0) AS exists FROM system.databases WHERE name = 'moraine'",
            ],
            json_envelope(json!([{ "exists": 1_u8 }])),
        ),
        ScriptedResponse::failure(
            &["SELECT version FROM `moraine`.schema_migrations GROUP BY version"],
            "doctor ledger read failed",
        ),
        ScriptedResponse::raw(
            &["SELECT name FROM system.tables WHERE database = 'moraine'"],
            json_envelope(required_tables),
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let diagnostics = repo
        .read_store_diagnostics()
        .await
        .expect("doctor partial report maps");

    assert!(diagnostics.healthy);
    assert!(diagnostics.version.is_none());
    assert_eq!(diagnostics.database, "moraine");
    assert!(diagnostics.database_exists);
    assert!(diagnostics.applied_schema_versions.is_empty());
    assert!(!diagnostics.pending_schema_versions.is_empty());
    assert_eq!(
        diagnostics.missing_tables,
        vec![
            "ingest_errors",
            "mcp_open_sessions",
            "mcp_open_turns",
            "mcp_open_events",
            "mcp_open_dirty_sessions",
            "mcp_open_projection_state",
        ]
    );
    assert_eq!(diagnostics.errors.len(), 2);
    assert!(diagnostics.errors[0].contains("version query failed"));
    assert!(diagnostics.errors[0].contains("doctor version probe failed"));
    assert!(diagnostics.errors[1].contains("failed to read migration ledger"));
    assert!(diagnostics.errors[1].contains("doctor ledger read failed"));
    assert_script_consumed(&state, 5);

    let (down_repo, down_state) = build_scripted_repo(vec![ScriptedResponse::failure(
        &["SELECT 1"],
        "doctor ping unavailable",
    )])
    .await;
    let down = down_repo
        .read_store_diagnostics()
        .await
        .expect("doctor ping failure returns partial report");
    assert!(!down.healthy);
    assert!(down.version.is_none());
    assert!(!down.database_exists);
    assert_eq!(down.database, "moraine");
    assert!(down.applied_schema_versions.is_empty());
    assert!(down.pending_schema_versions.is_empty());
    assert!(down.missing_tables.is_empty());
    assert_eq!(down.errors.len(), 1);
    assert!(down.errors[0].contains("ping failed"));
    assert!(down.errors[0].contains("doctor ping unavailable"));
    assert_script_consumed(&down_state, 1);
}
