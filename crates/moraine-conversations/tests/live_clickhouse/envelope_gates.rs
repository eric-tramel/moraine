//! Issue #600 exit-gate live tests: prove on a real ClickHouse that every
//! data statement runs inside a query envelope (`moraine-*` id + finite
//! server budget), that abandonment converges on a bounded KILL within the
//! 10-second cancellation budget (amendment A13), that one logical operation
//! shares one absolute deadline / statement cap / cumulative read allowance,
//! and that the interactive memory budget spills instead of OOMing the
//! server.
//!
//! The root `live_clickhouse.rs` owns the `#[tokio::test]` wrappers (so the
//! `run-live-test` `--exact` paths stay flat) and scopes each body under the
//! shared live-fixture envelope; the operations under test build their own
//! Interactive-class envelopes exactly the way the MCP and monitor
//! boundaries do.

use super::*;
use anyhow::ensure;
use moraine_clickhouse::ClickHouseErrorKind;
use moraine_conversations::{
    envelope_error_kind, unenveloped_statement_count, AllowanceResource, EnvelopeError,
    FileAttentionQuery, SearchEventsQuery, SearchMcpEventsQuery,
};

/// Bundled-default class deadlines the gates assert against (seconds).
const INTERACTIVE_DEADLINE_SECONDS: f64 = 15.0;
const ADMINISTRATIVE_DEADLINE_SECONDS: f64 = 5.0;
/// `%.3f` formatting plus the 1.0s server floor allow tiny non-monotonic
/// jitter between a budget and the value the server logged.
const BUDGET_EPSILON: f64 = 0.005;
/// Amendment A13: an abandoned statement must leave `system.processes`
/// within ten seconds of the drop.
const CANCELLATION_BUDGET: Duration = Duration::from_secs(10);

/// A deliberately slow, cheaply killable statement: one row per block, 50ms
/// of sleep per row, ~120s total — far beyond every timeout the gates use,
/// and cancellable between blocks.
const SLOW_SQL: &str = "SELECT sum(sleepEachRow(0.05)) FROM numbers(2400) \
     SETTINGS max_block_size = 1, max_threads = 1 FORMAT JSONEachRow";

/// A group-by whose ~6M-key aggregation state (hundreds of MiB) exceeds both
/// the 32 MiB spill threshold and the 128 MiB no-spill ceiling used below.
const SPILL_SQL: &str = "SELECT k, count() AS c FROM \
     (SELECT intHash64(number) % 6000000 AS k FROM numbers(12000000)) \
     GROUP BY k FORMAT Null";

fn interactive_budget_with(
    adjust: impl FnOnce(&mut QueryBudgetClassConfig),
) -> ValidatedQueryBudget {
    let mut config = QueryBudgetsConfig::default();
    adjust(&mut config.interactive);
    ValidatedQueryBudgets::from_config(&config)
        .expect("gate budget must validate")
        .interactive
}

async fn flush_query_log(clickhouse: &ClickHouseClient) -> Result<()> {
    clickhouse
        .request_text("SYSTEM FLUSH LOGS", None, None, false, None)
        .await
        .context("failed to flush system logs")?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct SettingRow {
    query_id: String,
    max_execution_time: String,
}

/// Query-log rows (one per finished/failed statement) matching `predicate`,
/// with the server-side execution budget each statement carried.
async fn query_log_settings(
    clickhouse: &ClickHouseClient,
    predicate: &str,
) -> Result<Vec<SettingRow>> {
    clickhouse
        .query_rows(
            &format!(
                "SELECT query_id, Settings['max_execution_time'] AS max_execution_time \
                 FROM system.query_log \
                 WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart') \
                   AND {predicate} \
                 ORDER BY query_id FORMAT JSONEachRow"
            ),
            Some("system"),
        )
        .await
        .with_context(|| format!("failed to read query-log settings where {predicate}"))
}

#[derive(Debug, Deserialize)]
struct CountRow {
    value: u64,
}

async fn count_query(clickhouse: &ClickHouseClient, sql: &str) -> Result<u64> {
    Ok(clickhouse
        .query_rows::<CountRow>(sql, Some("system"))
        .await
        .with_context(|| format!("count query failed: {sql}"))?
        .into_iter()
        .next()
        .context("count query returned no row")?
        .value)
}

/// Distinct statements of one request that reached the server (any terminal
/// query-log row type).
async fn distinct_statement_count(clickhouse: &ClickHouseClient, request_id: &str) -> Result<u64> {
    count_query(
        clickhouse,
        &format!(
            "SELECT toUInt64(uniqExact(query_id)) AS value FROM system.query_log \
             WHERE type != 'QueryStart' AND startsWith(query_id, '{request_id}-') \
             FORMAT JSONEachRow"
        ),
    )
    .await
}

async fn processes_with_prefix(clickhouse: &ClickHouseClient, request_id: &str) -> Result<u64> {
    count_query(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM system.processes \
             WHERE query_id = '{request_id}' OR startsWith(query_id, '{request_id}-') \
             FORMAT JSONEachRow"
        ),
    )
    .await
}

/// Poll `system.processes` until the request's statements are present
/// (`want_present`) or gone (`!want_present`), failing after `timeout`.
async fn wait_for_prefix_presence(
    clickhouse: &ClickHouseClient,
    request_id: &str,
    want_present: bool,
    timeout: Duration,
    what: &str,
) -> Result<Duration> {
    let started = Instant::now();
    loop {
        let running = processes_with_prefix(clickhouse, request_id).await?;
        if (running > 0) == want_present {
            return Ok(started.elapsed());
        }
        if started.elapsed() >= timeout {
            bail!(
                "timed out after {timeout:?} waiting for {what} \
                 (request id {request_id}, running statements {running})"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn parse_seconds(query_id: &str, raw: &str) -> Result<f64> {
    let value: f64 = raw.parse().with_context(|| {
        format!("statement {query_id} carried unparseable max_execution_time {raw:?}")
    })?;
    ensure!(
        value.is_finite() && value > 0.0,
        "statement {query_id} must carry a finite positive max_execution_time, got {raw:?}"
    );
    Ok(value)
}

/// Gate 1 (`run-live-test envelope-query-log`): after `SYSTEM FLUSH LOGS`,
/// every data statement from list/open/search/file-attention plus the
/// monitor-path reads carries a `moraine-*` query id with a finite
/// interactive execution budget in `system.query_log`; the KILL and
/// telemetry one-shots report their own finite administrative budgets
/// separately; and zero statements against the owned database ran without a
/// `moraine-*` envelope id.
pub(super) async fn query_log_coverage() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before envelope query-log gate").await?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate envelope query-log gate database")?;
        install_schema_fixture(&clickhouse, &database).await?;
        publish_missing_schema_fixture_sources(&clickhouse, &database).await?;
        clickhouse
            .backfill_mcp_open_read_model()
            .await
            .context("failed to project envelope query-log gate fixtures")?;
        let repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let interactive = default_interactive_budget();

        QueryEnvelope::new("gate-list", QueryClass::Interactive, &interactive)
            .scope(repository.list_mcp_sessions(
                McpSessionListFilter {
                    start_unix_ms: 0,
                    end_unix_ms: 4_102_444_800_000,
                    mode: None,
                    sort: ConversationListSort::Asc,
                    harness: None,
                    source_name: None,
                },
                PageRequest {
                    limit: 50,
                    cursor: None,
                },
            ))
            .await
            .context("enveloped list_mcp_sessions failed")?;

        QueryEnvelope::new("gate-open", QueryClass::Interactive, &interactive)
            .scope(repository.get_mcp_session("issue454-dedup-session"))
            .await
            .context("enveloped get_mcp_session failed")?
            .context("envelope gate fixture session missing")?;

        QueryEnvelope::new("gate-search", QueryClass::Interactive, &interactive)
            .scope(repository.search_events(SearchEventsQuery {
                query: "repository".to_string(),
                ..SearchEventsQuery::default()
            }))
            .await
            .context("enveloped search_events failed")?;

        QueryEnvelope::new("gate-mcpsearch", QueryClass::Interactive, &interactive)
            .scope(repository.search_mcp_events(SearchMcpEventsQuery {
                query: "repository".to_string(),
                ..SearchMcpEventsQuery::default()
            }))
            .await
            .context("enveloped search_mcp_events failed")?;

        QueryEnvelope::new("gate-fa", QueryClass::Interactive, &interactive)
            .scope(repository.file_attention(FileAttentionQuery {
                cancellation_token: "envelope-gate-file-attention".to_string(),
                rel: "src/main.rs".to_string(),
                normalized_project_id: None,
                normalized_project_roots: Vec::new(),
                derive_legacy_roots: false,
                apply_project_scope: false,
                start_unix_ms: None,
                end_unix_ms: None,
                harness: None,
                source_name: None,
                tool: None,
                mutations_only: false,
                max_rows: 100,
                execution_budget_secs: 10,
            }))
            .await
            .context("enveloped file_attention failed")?;

        QueryEnvelope::new("gate-monitor", QueryClass::Interactive, &interactive)
            .scope(async {
                repository.latest_ingest_heartbeat().await?;
                repository.list_table_summaries().await?;
                repository.read_store_health().await
            })
            .await
            .context("enveloped monitor-path reads failed")?;

        // One explicit cancel: runs as its own Administrative-class bounded
        // prefix KILL regardless of the target existing.
        repository
            .cancel_query(&format!("moraine-gate-absent-{}", std::process::id()))
            .await
            .context("enveloped cancel_query failed")?;

        // The KILL landed synchronously; search telemetry lands from a
        // detached task with its own Administrative envelope. Poll until
        // both flushed into the query log.
        let pid = std::process::id();
        let kill_like = format!("moraine-kill-{pid}-%");
        let telemetry_like = format!("moraine-telemetry-{pid}-%");
        let deadline = Instant::now() + Duration::from_secs(30);
        let (kill_rows, telemetry_rows) = loop {
            flush_query_log(&clickhouse).await?;
            let kill_rows =
                query_log_settings(&clickhouse, &format!("query_id LIKE '{kill_like}'")).await?;
            let telemetry_rows =
                query_log_settings(&clickhouse, &format!("query_id LIKE '{telemetry_like}'"))
                    .await?;
            if !kill_rows.is_empty() && !telemetry_rows.is_empty() {
                break (kill_rows, telemetry_rows);
            }
            if Instant::now() >= deadline {
                bail!(
                    "administrative statements missing from system.query_log after 30s: \
                     kill rows {}, telemetry rows {}",
                    kill_rows.len(),
                    telemetry_rows.len()
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        for row in kill_rows.iter().chain(telemetry_rows.iter()) {
            let budget = parse_seconds(&row.query_id, &row.max_execution_time)?;
            ensure!(
                budget <= ADMINISTRATIVE_DEADLINE_SECONDS + BUDGET_EPSILON,
                "administrative statement {} must stay within the {ADMINISTRATIVE_DEADLINE_SECONDS}s \
                 admin budget, carried {budget}",
                row.query_id
            );
        }

        // Every statement that touched the owned database: all enveloped,
        // interactive request kinds within the interactive budget.
        let data_rows = query_log_settings(
            &clickhouse,
            &format!(
                "current_database = '{}' AND is_initial_query = 1",
                database.as_str()
            ),
        )
        .await?;
        let stray: Vec<&str> = data_rows
            .iter()
            .filter(|row| !row.query_id.starts_with("moraine-"))
            .map(|row| row.query_id.as_str())
            .collect();
        ensure!(
            stray.is_empty(),
            "statements against the owned database ran without a moraine-* envelope id: {stray:?}"
        );
        for row in &data_rows {
            parse_seconds(&row.query_id, &row.max_execution_time)?;
        }
        for kind in [
            "gate-list",
            "gate-open",
            "gate-search",
            "gate-mcpsearch",
            "gate-fa",
            "gate-monitor",
        ] {
            let prefix = format!("moraine-{kind}-");
            let matching: Vec<&SettingRow> = data_rows
                .iter()
                .filter(|row| row.query_id.starts_with(&prefix))
                .collect();
            ensure!(
                !matching.is_empty(),
                "no data statement recorded for the {kind} operation"
            );
            for row in matching {
                let budget = parse_seconds(&row.query_id, &row.max_execution_time)?;
                ensure!(
                    budget <= INTERACTIVE_DEADLINE_SECONDS + BUDGET_EPSILON,
                    "interactive statement {} must stay within the \
                     {INTERACTIVE_DEADLINE_SECONDS}s class budget, carried {budget}",
                    row.query_id
                );
            }
        }

        ensure!(
            unenveloped_statement_count() == 0,
            "the transport must never execute an unenveloped statement"
        );
        Ok(())
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census =
        assert_owned_database_census_empty(&clickhouse, "after envelope query-log gate").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

/// Gate 2 (`run-live-test envelope-cancellation`): an abandoned long
/// statement — whether its task is aborted mid-flight or an inner future is
/// dropped by a `tokio::time::timeout` (the #576 orphan shape) — disappears
/// from `system.processes` within the 10s cancellation budget, while a
/// statement that completes normally attracts no KILL at all.
pub(super) async fn abandoned_query_cancelled() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before envelope cancellation gate").await?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate envelope cancellation gate database")?;
        let gate_budget = interactive_budget_with(|class| class.deadline_seconds = 60.0);

        // (a) Abandoned mid-flight: abort the task running the statement.
        let envelope = QueryEnvelope::new("gate-abandon", QueryClass::Interactive, &gate_budget);
        let abandon_request_id = envelope.request_id().to_string();
        let task_client = clickhouse.clone();
        let task_database = database.as_str().to_string();
        let task = tokio::spawn(envelope.scope(async move {
            task_client
                .request_text(SLOW_SQL, None, Some(&task_database), false, None)
                .await
        }));
        wait_for_prefix_presence(
            &clickhouse,
            &abandon_request_id,
            true,
            Duration::from_secs(30),
            "the slow statement to appear in system.processes",
        )
        .await?;
        task.abort();
        let _ = task.await;
        let cancelled_after = wait_for_prefix_presence(
            &clickhouse,
            &abandon_request_id,
            false,
            CANCELLATION_BUDGET,
            "the abandoned statement to leave system.processes",
        )
        .await?;
        eprintln!("abandoned statement cancelled after {cancelled_after:?} (budget 10s)");

        // (b) Internal timeout: the inner future is dropped while the outer
        // request scope completes normally — exactly the historical #576
        // orphan pattern. The per-statement drop guard must fire.
        let envelope = QueryEnvelope::new("gate-timeout", QueryClass::Interactive, &gate_budget);
        let timeout_request_id = envelope.request_id().to_string();
        envelope
            .scope(async {
                let raced = tokio::time::timeout(
                    Duration::from_millis(500),
                    clickhouse.request_text(SLOW_SQL, None, Some(database.as_str()), false, None),
                )
                .await;
                ensure!(
                    raced.is_err(),
                    "slow statement unexpectedly finished inside the 500ms tokio timeout"
                );
                Ok(())
            })
            .await?;
        let timed_out_after = wait_for_prefix_presence(
            &clickhouse,
            &timeout_request_id,
            false,
            CANCELLATION_BUDGET,
            "the timed-out statement to leave system.processes",
        )
        .await?;
        eprintln!("timed-out statement cancelled after {timed_out_after:?} (budget 10s)");
        // Prove the statement actually reached the server before the drop,
        // so the disappearance above was a real cancellation.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            flush_query_log(&clickhouse).await?;
            if distinct_statement_count(&clickhouse, &timeout_request_id).await? >= 1 {
                break;
            }
            if Instant::now() >= deadline {
                bail!("the timed-out statement never appeared in system.query_log");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // (c) A completed statement's guards disarm: no spurious KILL.
        let envelope = QueryEnvelope::new("gate-complete", QueryClass::Interactive, &gate_budget);
        let complete_request_id = envelope.request_id().to_string();
        envelope
            .scope(clickhouse.request_text(
                "SELECT 1 FORMAT JSONEachRow",
                None,
                Some(database.as_str()),
                false,
                None,
            ))
            .await
            .context("completed gate statement failed")?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        flush_query_log(&clickhouse).await?;
        let spurious = count_query(
            &clickhouse,
            &format!(
                "SELECT toUInt64(count()) AS value FROM system.query_log \
                 WHERE query LIKE 'KILL QUERY%' AND query LIKE '%{complete_request_id}%' \
                 FORMAT JSONEachRow"
            ),
        )
        .await?;
        ensure!(
            spurious == 0,
            "a completed statement attracted {spurious} spurious KILL statement(s)"
        );
        Ok(())
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census =
        assert_owned_database_census_empty(&clickhouse, "after envelope cancellation gate").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

/// Gate 3 (`run-live-test envelope-shared-budget`): one logical operation
/// shares one absolute deadline, one fixed statement cap, and one cumulative
/// read allowance — exhausting any of them fails typed `resource_exhausted`
/// (or `deadline_exceeded`) without issuing further statements, and a
/// multi-statement repository operation's child statements share the
/// `{request}-{seq}` prefix with non-increasing server deadlines.
pub(super) async fn shared_budget_and_statement_cap() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before envelope budget gate").await?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate envelope budget gate database")?;
        install_schema_fixture(&clickhouse, &database).await?;
        publish_missing_schema_fixture_sources(&clickhouse, &database).await?;
        clickhouse
            .backfill_mcp_open_read_model()
            .await
            .context("failed to project envelope budget gate fixtures")?;

        // (a) Fixed statement cap: the fourth statement is refused locally
        // and never reaches the server.
        let cap_budget = interactive_budget_with(|class| {
            class.statement_cap = 3;
            class.deadline_seconds = 30.0;
        });
        let envelope = QueryEnvelope::new("gate-cap", QueryClass::Interactive, &cap_budget);
        let cap_request_id = envelope.request_id().to_string();
        envelope
            .scope(async {
                for index in 0..3 {
                    clickhouse
                        .request_text(
                            "SELECT 1 FORMAT JSONEachRow",
                            None,
                            Some(database.as_str()),
                            false,
                            None,
                        )
                        .await
                        .with_context(|| format!("capped statement {index} failed"))?;
                }
                let error = clickhouse
                    .request_text(
                        "SELECT 1 FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .err()
                    .context("the statement beyond the cap must be refused")?;
                match error.downcast_ref::<EnvelopeError>() {
                    Some(EnvelopeError::StatementCapExceeded { cap: 3 }) => {}
                    other => bail!("expected StatementCapExceeded {{cap: 3}}, got {other:?}"),
                }
                ensure!(
                    envelope_error_kind(&error) == Some(ClickHouseErrorKind::ResourceExhausted),
                    "statement-cap refusal must classify as resource_exhausted"
                );
                Ok(())
            })
            .await?;
        flush_query_log(&clickhouse).await?;
        let cap_statements = distinct_statement_count(&clickhouse, &cap_request_id).await?;
        ensure!(
            cap_statements == 3,
            "exactly 3 capped statements may reach the server, found {cap_statements}"
        );

        // The same refusal end-to-end through the repository: a whole
        // operation under an exhausted budget fails typed
        // RepoError::ResourceExhausted.
        let capped_repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let tight_budget = interactive_budget_with(|class| class.statement_cap = 1);
        let envelope = QueryEnvelope::new("gate-repo-cap", QueryClass::Interactive, &tight_budget);
        let repo_cap_request_id = envelope.request_id().to_string();
        let refused = envelope
            .scope(capped_repository.get_mcp_session("issue454-dedup-session"))
            .await;
        match refused {
            Err(RepoError::ResourceExhausted { .. }) => {}
            other => bail!(
                "a repository operation with an exhausted statement budget must fail typed \
                 resource_exhausted, got {other:?}"
            ),
        }
        flush_query_log(&clickhouse).await?;
        let repo_cap_statements =
            distinct_statement_count(&clickhouse, &repo_cap_request_id).await?;
        ensure!(
            repo_cap_statements <= 1,
            "an exhausted operation must not issue further statements, found {repo_cap_statements}"
        );

        // (b) Absolute deadline: after it passes, the next statement fails
        // fast client-side; nothing new reaches the server.
        let deadline_budget = interactive_budget_with(|class| class.deadline_seconds = 2.0);
        let envelope =
            QueryEnvelope::new("gate-deadline", QueryClass::Interactive, &deadline_budget);
        let deadline_request_id = envelope.request_id().to_string();
        envelope
            .scope(async {
                clickhouse
                    .request_text(
                        "SELECT 1 FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .context("statement inside the deadline failed")?;
                tokio::time::sleep(Duration::from_millis(2_200)).await;
                let error = clickhouse
                    .request_text(
                        "SELECT 1 FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .err()
                    .context("the statement after the absolute deadline must be refused")?;
                match error.downcast_ref::<EnvelopeError>() {
                    Some(EnvelopeError::DeadlineExpired { .. }) => {}
                    other => bail!("expected DeadlineExpired, got {other:?}"),
                }
                ensure!(
                    envelope_error_kind(&error) == Some(ClickHouseErrorKind::DeadlineExceeded),
                    "deadline refusal must classify as deadline_exceeded"
                );
                Ok(())
            })
            .await?;
        flush_query_log(&clickhouse).await?;
        let deadline_statements =
            distinct_statement_count(&clickhouse, &deadline_request_id).await?;
        ensure!(
            deadline_statements == 1,
            "only the pre-deadline statement may reach the server, found {deadline_statements}"
        );

        // (c) One multi-statement operation shares one absolute deadline:
        // child ids share the request prefix, stay under the class cap, and
        // their server deadlines never increase across the sequence.
        let repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let envelope = QueryEnvelope::new(
            "gate-shared",
            QueryClass::Interactive,
            &default_interactive_budget(),
        );
        let shared_request_id = envelope.request_id().to_string();
        envelope
            .scope(repository.get_mcp_session("issue454-dedup-session"))
            .await
            .context("shared-budget get_mcp_session failed")?
            .context("shared-budget fixture session missing")?;
        flush_query_log(&clickhouse).await?;
        let shared_rows = query_log_settings(
            &clickhouse,
            &format!("startsWith(query_id, '{shared_request_id}-') AND type = 'QueryFinish'"),
        )
        .await?;
        let mut sequenced = Vec::with_capacity(shared_rows.len());
        for row in &shared_rows {
            let sequence: u64 = row
                .query_id
                .rsplit('-')
                .next()
                .unwrap_or_default()
                .parse()
                .with_context(|| format!("child id {} has no sequence suffix", row.query_id))?;
            sequenced.push((
                sequence,
                parse_seconds(&row.query_id, &row.max_execution_time)?,
            ));
        }
        sequenced.sort_by_key(|(sequence, _)| *sequence);
        ensure!(
            sequenced.len() >= 2,
            "a bounded open must issue at least two statements, found {}",
            sequenced.len()
        );
        ensure!(
            sequenced.len() <= 256,
            "a bounded open must stay under the interactive statement cap, found {}",
            sequenced.len()
        );
        for pair in sequenced.windows(2) {
            let (previous_seq, previous_budget) = pair[0];
            let (next_seq, next_budget) = pair[1];
            ensure!(
                next_budget <= previous_budget + BUDGET_EPSILON,
                "shared deadline must never grow across one operation: \
                 statement {next_seq} carried {next_budget}s after statement {previous_seq} \
                 carried {previous_budget}s"
            );
            ensure!(
                next_budget <= INTERACTIVE_DEADLINE_SECONDS + BUDGET_EPSILON,
                "statement {next_seq} exceeded the interactive class budget: {next_budget}s"
            );
        }

        // (d) Cumulative read allowance: the second statement's server
        // ceiling shrinks to what the first left over, an over-limit read
        // fails typed resource_exhausted server-side, and a fully exhausted
        // allowance refuses admission without issuing a statement.
        let rows_budget = interactive_budget_with(|class| {
            class.read_rows = 1_000;
            class.statement_cap = 8;
            class.deadline_seconds = 30.0;
        });
        let envelope = QueryEnvelope::new("gate-allowance", QueryClass::Interactive, &rows_budget);
        let allowance_request_id = envelope.request_id().to_string();
        envelope
            .scope(async {
                clickhouse
                    .request_text(
                        "SELECT sum(number) AS s FROM numbers(900) FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .context("statement consuming 900 of 1000 allowance rows failed")?;
                let overrun = clickhouse
                    .request_text(
                        "SELECT sum(number) AS s FROM numbers(1000) FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .err()
                    .context("a read beyond the remaining allowance must fail")?;
                ensure!(
                    envelope_error_kind(&overrun) == Some(ClickHouseErrorKind::ResourceExhausted),
                    "the server-enforced read ceiling must classify as resource_exhausted, \
                     got: {overrun:#}"
                );
                clickhouse
                    .request_text(
                        "SELECT sum(number) AS s FROM numbers(100) FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .context("statement draining the final 100 allowance rows failed")?;
                let refused = clickhouse
                    .request_text(
                        "SELECT 1 FORMAT JSONEachRow",
                        None,
                        Some(database.as_str()),
                        false,
                        None,
                    )
                    .await
                    .err()
                    .context("a statement after allowance exhaustion must be refused")?;
                match refused.downcast_ref::<EnvelopeError>() {
                    Some(EnvelopeError::AllowanceExhausted {
                        resource: AllowanceResource::Rows,
                        budget: 1_000,
                    }) => {}
                    other => {
                        bail!("expected AllowanceExhausted {{rows, budget: 1000}}, got {other:?}")
                    }
                }
                ensure!(
                    envelope_error_kind(&refused) == Some(ClickHouseErrorKind::ResourceExhausted),
                    "allowance exhaustion must classify as resource_exhausted"
                );
                Ok(())
            })
            .await?;
        flush_query_log(&clickhouse).await?;
        let allowance_statements =
            distinct_statement_count(&clickhouse, &allowance_request_id).await?;
        ensure!(
            allowance_statements == 3,
            "exactly 3 allowance statements may reach the server, found {allowance_statements}"
        );
        #[derive(Debug, Deserialize)]
        struct ReadCeilingRow {
            max_rows_to_read: String,
        }
        let ceiling = clickhouse
            .query_rows::<ReadCeilingRow>(
                &format!(
                    "SELECT Settings['max_rows_to_read'] AS max_rows_to_read \
                     FROM system.query_log \
                     WHERE query_id = '{allowance_request_id}-1' AND type != 'QueryStart' \
                     LIMIT 1 FORMAT JSONEachRow"
                ),
                Some("system"),
            )
            .await
            .context("failed to read the second statement's row ceiling")?
            .into_iter()
            .next()
            .context("second allowance statement missing from system.query_log")?;
        let ceiling: u64 = ceiling.max_rows_to_read.parse().with_context(|| {
            format!(
                "unparseable max_rows_to_read {:?}",
                ceiling.max_rows_to_read
            )
        })?;
        ensure!(
            ceiling > 0 && ceiling <= 100,
            "the second statement's max_rows_to_read must equal the remaining allowance \
             (<= 100), carried {ceiling}"
        );
        Ok(())
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census =
        assert_owned_database_census_empty(&clickhouse, "after envelope budget gate").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

/// Gate 4 (`run-live-test envelope-spill`): a group-by whose state exceeds
/// the interactive memory budget completes via external aggregation under
/// spill thresholds, and with spill effectively disabled the same query
/// fails typed `resource_exhausted` from the server's memory ceiling (code
/// 241) without OOMing the server or leaving an orphan.
pub(super) async fn spill_and_memory_ceiling() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before envelope spill gate").await?;

    let outcome = async {
        // The synthetic corpus is generated by `numbers()`; the database
        // only needs to exist as a statement context.
        clickhouse
            .request_text(
                &format!("CREATE DATABASE IF NOT EXISTS `{}`", database.as_str()),
                None,
                None,
                false,
                None,
            )
            .await
            .context("failed to create envelope spill gate database")?;

        // (a) Spill path: 256 MiB ceiling, 32 MiB spill threshold — the
        // aggregate must finish by writing external aggregation parts.
        let spill_budget = interactive_budget_with(|class| {
            class.deadline_seconds = 90.0;
            class.memory_bytes = 256 * 1024 * 1024;
            class.spill_bytes = 32 * 1024 * 1024;
            class.read_rows = 1_000_000_000;
        });
        let envelope = QueryEnvelope::new("gate-spill", QueryClass::Interactive, &spill_budget);
        let spill_request_id = envelope.request_id().to_string();
        envelope
            .scope(clickhouse.request_text(SPILL_SQL, None, Some(database.as_str()), false, None))
            .await
            .context("the spilling aggregate must complete under the interactive budget")?;
        flush_query_log(&clickhouse).await?;

        #[derive(Debug, Deserialize)]
        struct SpillRow {
            spilled_parts: u64,
            group_by_threshold: String,
            exception_code: i32,
        }
        let spill_row = clickhouse
            .query_rows::<SpillRow>(
                &format!(
                    "SELECT \
                       toUInt64(ProfileEvents['ExternalAggregationWritePart'] \
                         + ProfileEvents['ExternalSortWritePart']) AS spilled_parts, \
                       Settings['max_bytes_before_external_group_by'] AS group_by_threshold, \
                       exception_code \
                     FROM system.query_log \
                     WHERE query_id = '{spill_request_id}-0' AND type = 'QueryFinish' \
                     FORMAT JSONEachRow"
                ),
                Some("system"),
            )
            .await
            .context("failed to read spill evidence from system.query_log")?
            .into_iter()
            .next()
            .context("spilling aggregate missing from system.query_log")?;
        ensure!(
            spill_row.exception_code == 0,
            "the spilling aggregate must finish cleanly, exception code {}",
            spill_row.exception_code
        );
        ensure!(
            spill_row.spilled_parts > 0,
            "the aggregate must exercise external aggregation/sort under the 32 MiB \
             spill threshold, wrote {} parts",
            spill_row.spilled_parts
        );
        ensure!(
            spill_row.group_by_threshold == "33554432",
            "the statement must carry the envelope's spill threshold, got {:?}",
            spill_row.group_by_threshold
        );

        // (b) Memory ceiling with spill effectively disabled (threshold ==
        // ceiling): the same aggregate fails typed resource_exhausted from
        // server code 241 and leaves no orphan behind.
        let ceiling_budget = interactive_budget_with(|class| {
            class.deadline_seconds = 90.0;
            class.memory_bytes = 128 * 1024 * 1024;
            class.spill_bytes = 128 * 1024 * 1024;
            class.read_rows = 1_000_000_000;
        });
        let envelope = QueryEnvelope::new("gate-oom", QueryClass::Interactive, &ceiling_budget);
        let ceiling_request_id = envelope.request_id().to_string();
        let error = envelope
            .scope(clickhouse.request_text(SPILL_SQL, None, Some(database.as_str()), false, None))
            .await
            .err()
            .context("the aggregate must fail under the no-spill memory ceiling")?;
        ensure!(
            envelope_error_kind(&error) == Some(ClickHouseErrorKind::ResourceExhausted),
            "the memory ceiling must classify as resource_exhausted, got: {error:#}"
        );
        let orphans = processes_with_prefix(&clickhouse, &ceiling_request_id).await?;
        ensure!(
            orphans == 0,
            "the memory-limited statement must not linger in system.processes, found {orphans}"
        );
        // The failure is the server's structured ceiling, not a client
        // abandonment.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            flush_query_log(&clickhouse).await?;
            let code = clickhouse
                .query_rows::<CountRow>(
                    &format!(
                        "SELECT toUInt64(exception_code) AS value FROM system.query_log \
                         WHERE query_id = '{ceiling_request_id}-0' AND type != 'QueryStart' \
                         ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSONEachRow"
                    ),
                    Some("system"),
                )
                .await
                .context("failed to read the memory-limited statement's exception code")?
                .into_iter()
                .next()
                .map(|row| row.value);
            match code {
                Some(241) => break,
                Some(0) | None if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                other => bail!(
                    "the memory-limited statement must fail with server code 241 \
                     (MEMORY_LIMIT_EXCEEDED), found {other:?}"
                ),
            }
        }

        // The server survived both phases.
        clickhouse
            .ping()
            .await
            .context("ClickHouse must stay healthy after the memory-ceiling phase")?;
        Ok(())
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census = assert_owned_database_census_empty(&clickhouse, "after envelope spill gate").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}
