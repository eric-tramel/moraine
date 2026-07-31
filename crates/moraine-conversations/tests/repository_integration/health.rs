use super::*;

/// The ten statements a fully successful `read_store_health()` issues, in
/// order. Extracted so more than one test can drive the same healthy backend:
/// the storage probe's contents are asserted by
/// `store_health_maps_all_successful_probe_facts` and its `[retention]` policy
/// by `the_storage_probe_reports_the_operators_retention_policy`, and a script
/// copied into two places is a script the two can disagree about.
fn healthy_probe_script() -> Vec<ScriptedResponse> {
    vec![
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
            ScriptedResponse::rows(
                &["FROM `moraine`.v_publication_diagnostics", "FORMAT JSONEachRow"],
                json!([{
                    "ambiguous_hostless_rows": 0_u64,
                    "replaying_generations": 2_u64,
                    "blocked_generations": 0_u64,
                    "append_preparations": 1_u64,
                    "blocked_append_preparations": 0_u64,
                    "mirror_catchup_pending": 3_u64,
                    "writer_conflicts": 0_u64,
                    "issues": ["mirror host-b catching up"]
                }]),
            ),
            // Core-index probe (issue #598): each of the three read_index_state
            // accessors first checks whether mcp_read_index_state exists. On a
            // reachable-but-not-yet-migrated database every check reports absent,
            // so the probe maps to Available with all flags false.
            ScriptedResponse::rows(
                &["FROM system.tables", "mcp_read_index_state"],
                json!([{"value": "0"}]),
            ),
            ScriptedResponse::rows(
                &["FROM system.tables", "mcp_read_index_state"],
                json!([{"value": "0"}]),
            ),
            ScriptedResponse::rows(
                &["FROM system.tables", "mcp_read_index_state"],
                json!([{"value": "0"}]),
            ),
            // Storage probe (issue #603 WI-02): exactly two statements — one
            // `system.parts` aggregate and one `system.disks` read. It touches
            // no user relation, which is what makes it safe on the health path.
            ScriptedResponse::rows(
                &["FROM system.parts", "AND active", "GROUP BY table"],
                json!([
                    {
                        "name": "events",
                        "rows": 1_990_776_u64,
                        "compressed_bytes": 4_787_723_965_u64,
                        "uncompressed_bytes": 11_420_351_515_u64,
                        "active_parts": 24_u64,
                        "oldest_retained_unix": 1_771_597_005_i64,
                        "oldest_retained_text": "2026-02-20T14:16:45Z"
                    },
                    {
                        "name": "mcp_open_turns",
                        "rows": 234_694_u64,
                        "compressed_bytes": 14_356_000_000_u64,
                        "uncompressed_bytes": 40_000_000_000_u64,
                        "active_parts": 303_u64,
                        "oldest_retained_unix": 0_i64,
                        "oldest_retained_text": "1970-01-01T00:00:00Z"
                    },
                    {
                        "name": "mystery_table",
                        "rows": 1_u64,
                        "compressed_bytes": 2_u64,
                        "uncompressed_bytes": 3_u64,
                        "active_parts": 1_u64,
                        "oldest_retained_unix": 0_i64,
                        "oldest_retained_text": "1970-01-01T00:00:00Z"
                    }
                ]),
            ),
            ScriptedResponse::rows(
                &["FROM system.disks"],
                json!([{ "free_bytes": 11_780_276_224_u64, "total_bytes": 994_662_584_320_u64 }]),
            ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn store_health_maps_all_successful_probe_facts() {
    scoped(async {
        let (repo, state) = build_scripted_repo(healthy_probe_script()).await;

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
        match health.publication {
            StoreProbe::Available(diagnostics) => {
                assert!(diagnostics.is_healthy());
                assert_eq!(diagnostics.replaying_generations, 2);
                assert_eq!(diagnostics.append_preparations, 1);
                assert_eq!(diagnostics.mirror_catchup_pending, 3);
            }
            other => panic!("expected publication diagnostics, got {other:?}"),
        }
        assert!(
            matches!(&health.core_index, StoreProbe::Available(ci)
                if !ci.core_indexes_ready
                    && !ci.open_v2_ready
                    && ci.open_v2_provenance.is_none()
                    && ci.backfill_cursor_age_ms.is_none()
                    && ci.audit_outcome.is_none()),
            "unmigrated core-index probe maps to Available/all-false, got {:?}",
            health.core_index
        );
        // Storage probe (issue #603 WI-02): buckets are folded from the
        // classification, the epoch sentinel is reported as "no oldest row"
        // rather than 1970, and an unclassified table lands in NO bucket and
        // is surfaced by name.
        let storage = match &health.storage {
            StoreProbe::Available(storage) => storage,
            other => panic!("expected an available storage report, got {other:?}"),
        };
        assert_eq!(storage.tables.len(), 3);
        let events = storage
            .tables
            .iter()
            .find(|table| table.name == "events")
            .expect("events row");
        assert_eq!(events.class, Some(TableClass::CanonicalHistory));
        assert_eq!(
            events.oldest_retained.as_deref(),
            Some("2026-02-20T14:16:45Z")
        );
        let turns = storage
            .tables
            .iter()
            .find(|table| table.name == "mcp_open_turns")
            .expect("turns row");
        assert_eq!(turns.class, Some(TableClass::Derived));
        assert_eq!(
            turns.oldest_retained, None,
            "a hash-partitioned table reports NO oldest row, never the epoch"
        );
        assert_eq!(storage.unclassified_tables(), vec!["mystery_table"]);
        assert!(storage
            .notes
            .iter()
            .any(|note| note.contains("mystery_table")));

        let canonical = storage
            .bucket(TableClass::CanonicalHistory)
            .expect("canonical bucket");
        assert_eq!(canonical.tables, 1);
        assert_eq!(canonical.rows, 1_990_776);
        // The unclassified table contributes to no bucket: it must never be
        // silently folded into "derived".
        assert_eq!(
            storage.total_compressed_bytes(),
            4_787_723_965 + 14_356_000_000
        );
        let disk = storage.disk.expect("disk headroom");
        assert_eq!(disk.free_bytes, 11_780_276_224);
        assert_eq!(disk.total_bytes, 994_662_584_320);
        // Stock configuration authorizes deleting nothing.
        assert!(storage.destructive_policies().is_empty());

        assert_script_consumed(&state, 10);
    })
    .await;
}

/// **G-HEALTH-RETENTION.** The store-health storage probe reports the
/// **operator's** `[retention]` policy, not the built-in defaults.
/// Denomination: the policy entries on the returned `StorageReport`.
///
/// `RepoConfig::retention` exists for exactly this and says so — "carried so
/// the store-health storage probe reports the policy an operator actually
/// configured rather than the built-in defaults \[…\] surfacing it through
/// `/api/v1/health` is what keeps it from being invisible". Nothing drove it.
/// Found by the round-6 sweep of every entry point that reaches `storage_report`
/// / `reclaim_*`, after the same defect was closed at four CLI call sites; this
/// is the fifth instance and the only one outside `apps/moraine`.
///
/// This surface deletes nothing and authorizes nothing — that is why it is a
/// reporting guard rather than a safety one. What it protects is an operator's
/// only way to confirm from outside the process that a configured bucket-1
/// horizon took effect. A monitor that always answers "default" for a host
/// configured to prune history is worse than one that does not answer.
///
/// MUTATION (executed 2026-07-28): pass `&RetentionConfig::default()` to
/// `storage_report` from `ClickHouseConversationRepository::read_storage_probe`
/// => FAILS here, and only here: with this one test skipped the same mutation
/// leaves the remaining 1601 workspace tests green. **Lower bound, and the
/// finding.**
#[tokio::test(flavor = "multi_thread")]
async fn the_storage_probe_reports_the_operators_retention_policy() {
    scoped(async {
        // A configured bucket-1 horizon: the one class of policy that can lead
        // to user history being deleted, and absent on a stock config.
        let retention = moraine_config::RetentionConfig {
            canonical_history_horizon_days: Some(30.0),
            ..moraine_config::RetentionConfig::default()
        };
        let (repo, state) =
            build_scripted_repo_with_retention(healthy_probe_script(), retention).await;

        let health = repo
            .read_store_health()
            .await
            .expect("health probes succeed");
        let storage = match &health.storage {
            StoreProbe::Available(storage) => storage,
            other => panic!("expected an available storage report, got {other:?}"),
        };

        let destructive = storage.destructive_policies();
        assert_eq!(
            destructive.len(),
            1,
            "a configured canonical horizon must surface as destructive: {:#?}",
            storage.policy
        );
        let entry = destructive[0];
        assert_eq!(entry.class, TableClass::CanonicalHistory);
        assert_eq!(
            entry.horizon_seconds,
            Some(30.0 * 24.0 * 60.0 * 60.0),
            "the reported horizon must be the configured one"
        );
        assert_eq!(
            entry.source,
            moraine_clickhouse::storage_report::POLICY_SOURCE_CONFIGURED,
            "a configured horizon reported as `default` is the defect this guards"
        );

        assert_script_consumed(&state, 10);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_health_keeps_each_probe_failure_independent() {
    scoped(async {
        let responses = vec![
            ScriptedResponse::failure(&["SELECT 1"], "health ping failed"),
            ScriptedResponse::failure(&["SELECT version() AS version"], "health version failed"),
            ScriptedResponse::failure(&["FROM system.databases"], "health database failed"),
            ScriptedResponse::failure(&["FROM system.metrics"], "health connections failed"),
            ScriptedResponse::failure(
                &["FROM `moraine`.v_publication_diagnostics"],
                "health publication failed",
            ),
            // Core-index probe: the first read_index_state existence check
            // fails, so the whole probe short-circuits to Failed (one request).
            ScriptedResponse::failure(
                &["FROM system.tables", "mcp_read_index_state"],
                "health core-index failed",
            ),
            // The storage probe's first statement fails, so the whole probe
            // degrades to Failed without failing the health report.
            ScriptedResponse::failure(&["FROM system.parts"], "health storage failed"),
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
        assert!(matches!(
            health.publication,
            StoreProbe::Failed { ref message } if message.contains("health publication failed")
        ));
        assert!(matches!(
            health.core_index,
            StoreProbe::Failed { ref message } if message.contains("health core-index failed")
        ));
        assert!(matches!(
            health.storage,
            StoreProbe::Failed { ref message } if message.contains("health storage failed")
        ));
        assert_script_consumed(&state, 7);
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn diagnostics_maps_doctor_partial_report_and_ping_short_circuit() {
    scoped(async {
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
            { "name": "schema_migrations" },
            { "name": "v_publication_diagnostics" }
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
            ScriptedResponse::rows(
                &["FROM `moraine`.v_publication_diagnostics", "FORMAT JSONEachRow"],
                json!([{
                    "ambiguous_hostless_rows": 4_u64,
                    "replaying_generations": 1_u64,
                    "blocked_generations": 2_u64,
                    "append_preparations": 3_u64,
                    "blocked_append_preparations": 1_u64,
                    "mirror_catchup_pending": 1_u64,
                    "writer_conflicts": 1_u64,
                    "issues": ["legacy ownership ambiguous", "publisher conflict"]
                }]),
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
        let publication = diagnostics
            .publication
            .as_ref()
            .expect("publication diagnostics map");
        assert!(!publication.is_healthy());
        assert_eq!(publication.ambiguous_hostless_rows, 4);
        assert_eq!(publication.blocked_generations, 2);
        assert_eq!(publication.writer_conflicts, 1);
        assert_eq!(
            diagnostics.missing_tables,
            vec![
                "ingest_errors",
                "mcp_open_sessions",
                "mcp_open_turns",
                "mcp_open_events",
                "mcp_open_dirty_sessions",
                "mcp_open_projection_state",
                "mcp_open_publication_headers",
                "mcp_open_generation_readiness",
                "mcp_open_backfill_plans",
                "published_source_generations",
                "ingest_checkpoint_transitions",
                "source_generation_publication_readiness",
                "ingest_append_control",
                "publication_diagnostic_events",
                "mcp_session_directory",
                "mcp_event_locator",
                "mcp_event_navigation",
                "mcp_read_index_state",
                "mv_mcp_session_directory_from_events",
                "mv_mcp_event_locator_from_events",
                "mv_mcp_event_navigation_from_events",
                "v_published_source_generation_history",
                "v_current_published_source_generations",
                "v_current_ingest_checkpoint_transitions",
                "v_current_source_generation_publication_readiness",
                "v_current_ingest_append_control",
                "v_live_events",
                "v_live_event_links",
                "v_live_tool_io",
                "v_live_search_documents",
                "v_live_search_postings",
                "v_mcp_open_publication_headers",
                "v_current_mcp_open_generation_readiness",
                // issue #603 WI-04: migration 038's ledger is part of the
                // schema handshake, so a database without it reports it
                // missing rather than silently tolerating a reclaimer with no
                // durable claim set.
                "storage_reclaim_ledger",
            ]
        );
        assert_eq!(diagnostics.errors.len(), 2);
        assert!(diagnostics.errors[0].contains("version query failed"));
        assert!(diagnostics.errors[0].contains("doctor version probe failed"));
        assert!(diagnostics.errors[1].contains("failed to read migration ledger"));
        assert!(diagnostics.errors[1].contains("doctor ledger read failed"));
        assert_script_consumed(&state, 6);

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
    })
    .await;
}
