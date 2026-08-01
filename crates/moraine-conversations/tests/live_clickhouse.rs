use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::{
    AppConfig, ClickHouseConfig, QueryBudgetClassConfig, QueryBudgetsConfig, ValidatedQueryBudget,
    ValidatedQueryBudgets, DEFAULT_BACKEND_NAME, QUERY_MEMORY_BACKSTOP_BYTES,
};
use moraine_conversations::{
    AnalyticsRange, BackendRepositoryRouter, CanonicalReadOutcome,
    ClickHouseConversationRepository, ConversationListSort, ConversationRepository,
    McpSessionListFilter, McpSessionOpen, McpTurnOpen, PageRequest, QueryClass, QueryEnvelope,
    RepoConfig, RepoError, SessionLookback, TurnListFilter,
};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use uuid::Uuid;

#[path = "live_clickhouse/bounded_search.rs"]
mod bounded_search;
#[path = "live_clickhouse/canonical_open_benchmark.rs"]
mod canonical_open_benchmark;
#[path = "live_clickhouse/canonical_open_parity.rs"]
mod canonical_open_parity;
#[path = "live_clickhouse/envelope_gates.rs"]
mod envelope_gates;
#[path = "live_clickhouse/search_host_identity.rs"]
mod search_host_identity;
#[path = "live_clickhouse/session_list_benchmark.rs"]
mod session_list_benchmark;
#[path = "live_clickhouse/session_list_parity.rs"]
mod session_list_parity;
#[path = "live_clickhouse/source_publication.rs"]
mod source_publication;
#[path = "live_clickhouse/source_publication_migration.rs"]
mod source_publication_migration;
#[path = "live_clickhouse/source_publication_process.rs"]
mod source_publication_process;
#[path = "live_clickhouse/support.rs"]
mod support;

use support::{
    Cardinality, MonitorAnalyticsResponse, MonitorResponse, MonitorSessionsResponse,
    SemanticComparison, SemanticObservation,
};

const OWNED_DATABASE_PREFIX: &str = "moraine_test_";
const SANDBOX_CONFIG_PATH: &str = "/sandbox/moraine.toml";
const CLICKHOUSE_URL_ENV: &str = "MORAINE_BENCH_CLICKHOUSE_URL";

#[derive(Debug)]
struct LivePrerequisites {
    clickhouse_url: String,
    clickhouse_username: String,
    clickhouse_password: String,
    sandbox_id: String,
}

impl LivePrerequisites {
    fn load() -> Result<Self> {
        let opt_in = read_required_env("MORAINE_ALLOW_DESTRUCTIVE_TESTS")?;
        let sandbox_id = read_required_env("MORAINE_LIVE_TEST_SANDBOX_ID")?;
        let clickhouse_url = read_required_env(CLICKHOUSE_URL_ENV)?;
        let marker = fs::read_to_string(SANDBOX_CONFIG_PATH).with_context(|| {
            format!("failed to read wrapper-owned sandbox marker {SANDBOX_CONFIG_PATH}")
        })?;
        validate_prerequisites(&opt_in, &sandbox_id, &clickhouse_url, &marker)?;
        Ok(Self {
            clickhouse_url,
            clickhouse_username: read_optional_env("MORAINE_BENCH_CLICKHOUSE_USER", "default")?,
            clickhouse_password: read_optional_env("MORAINE_BENCH_CLICKHOUSE_PASSWORD", "")?,
            sandbox_id,
        })
    }

    fn clickhouse_config(&self, database: &OwnedDatabaseName) -> ClickHouseConfig {
        ClickHouseConfig {
            url: self.clickhouse_url.clone(),
            database: database.as_str().to_string(),
            username: self.clickhouse_username.clone(),
            password: self.clickhouse_password.clone(),
            ..ClickHouseConfig::default()
        }
    }
}

fn read_required_env(name: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) => bail!("{name} must not be empty"),
        Err(env::VarError::NotPresent) => bail!("{name} is required"),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must contain valid UTF-8"),
    }
}

fn read_optional_env(name: &str, default: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.to_string()),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must contain valid UTF-8"),
    }
}

fn validate_prerequisites(
    opt_in: &str,
    sandbox_id: &str,
    clickhouse_url: &str,
    sandbox_config: &str,
) -> Result<()> {
    if opt_in != "1" {
        bail!("MORAINE_ALLOW_DESTRUCTIVE_TESTS must equal 1");
    }
    if !valid_sandbox_id(sandbox_id) {
        bail!("MORAINE_LIVE_TEST_SANDBOX_ID must match sb-[a-f0-9]{{6}}, got {sandbox_id:?}");
    }
    let url = Url::parse(clickhouse_url)
        .with_context(|| format!("{CLICKHOUSE_URL_ENV} is not a valid URL"))?;
    if url.scheme() != "http"
        || url.host_str() != Some("clickhouse")
        || url.port_or_known_default() != Some(8123)
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        bail!(
            "{CLICKHOUSE_URL_ENV} must be the credential-free wrapper-owned endpoint http://clickhouse:8123"
        );
    }
    let expected_marker =
        format!("# Generated by scripts/dev/sandbox/moraine-sandbox for {sandbox_id}");
    if sandbox_config.lines().next() != Some(expected_marker.as_str()) {
        bail!("sandbox config marker does not own {sandbox_id}");
    }
    if !sandbox_config
        .lines()
        .any(|line| line.trim() == "url = \"http://clickhouse:8123\"")
    {
        bail!("sandbox config does not identify the wrapper-owned ClickHouse endpoint");
    }
    Ok(())
}

fn valid_sandbox_id(value: &str) -> bool {
    value.len() == 9
        && value.starts_with("sb-")
        && value[3..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnedDatabaseName(String);

impl OwnedDatabaseName {
    fn generate() -> Self {
        let database = Self(format!(
            "{OWNED_DATABASE_PREFIX}{}",
            Uuid::new_v4().simple()
        ));
        debug_assert!(validate_owned_database_name(database.as_str()).is_ok());
        database
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_owned_database_name(value: &str) -> Result<()> {
    if value.is_empty() || value == "moraine" {
        bail!("refusing unsafe live-test database name {value:?}");
    }
    let suffix = value
        .strip_prefix(OWNED_DATABASE_PREFIX)
        .context("live-test database must use the moraine_test_<uuid> prefix")?;
    if suffix.len() != 32
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("live-test database suffix must be 32 lowercase ASCII hexadecimal digits");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("live-test database must contain only ASCII letters, digits, and underscore");
    }
    Ok(())
}

fn cleanup_statement(database: &OwnedDatabaseName) -> String {
    format!("DROP DATABASE IF EXISTS `{}` SYNC", database.as_str())
}

fn write_pre_mutation_diagnostic(
    output: &mut impl Write,
    database: &OwnedDatabaseName,
    sandbox_id: &str,
) -> Result<()> {
    writeln!(output, "live ClickHouse database: {}", database.as_str())
        .context("failed to emit owned live database identity")?;
    writeln!(
        output,
        "cleanup: sandbox={} endpoint=<redacted-owned-clickhouse> query={}",
        sandbox_id,
        cleanup_statement(database)
    )
    .context("failed to emit owned live database cleanup diagnostic")?;
    Ok(())
}

fn prepare_owned_database_identity_with_writer(
    sandbox_id: &str,
    output: &mut impl Write,
) -> Result<OwnedDatabaseName> {
    let database = OwnedDatabaseName::generate();
    validate_owned_database_name(database.as_str())?;
    write_pre_mutation_diagnostic(output, &database, sandbox_id)?;
    Ok(database)
}

fn prepare_owned_database_identity(sandbox_id: &str) -> Result<OwnedDatabaseName> {
    prepare_owned_database_identity_with_writer(sandbox_id, &mut io::stderr().lock())
}

async fn cleanup_database(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    clickhouse
        .request_text(
            &cleanup_statement(database),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .with_context(|| format!("failed to remove owned database {}", database.as_str()))?;
    Ok(())
}

#[derive(Deserialize)]
struct DatabaseNameRow {
    name: String,
}

async fn owned_database_census(clickhouse: &ClickHouseClient) -> Result<Vec<String>> {
    let rows: Vec<DatabaseNameRow> = clickhouse
        .query_rows(
            "SELECT name FROM system.databases \
             WHERE startsWith(name, 'moraine_test_') \
             ORDER BY name FORMAT JSONEachRow",
            Some("system"),
        )
        .await
        .context("failed to census owned live-test databases")?;
    Ok(rows.into_iter().map(|row| row.name).collect())
}

async fn assert_owned_database_census_empty(
    clickhouse: &ClickHouseClient,
    phase: &str,
) -> Result<()> {
    let databases = owned_database_census(clickhouse).await?;
    if !databases.is_empty() {
        bail!(
            "{phase} owned-resource census expected zero {OWNED_DATABASE_PREFIX}* databases, found {databases:?}"
        );
    }
    Ok(())
}

fn finish_with_cleanup(outcome: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup_error)) => {
            Err(cleanup_error.context("live ClickHouse teardown failed"))
        }
        (Err(error), Err(cleanup_error)) => Err(anyhow!(
            "{error:#}; live ClickHouse teardown also failed: {cleanup_error:#}"
        )),
    }
}

fn live_client(
    prerequisites: &LivePrerequisites,
    database: &OwnedDatabaseName,
) -> Result<ClickHouseClient> {
    ClickHouseClient::new(prerequisites.clickhouse_config(database))
        .context("failed to construct owned live ClickHouse client")
}

/// Generous Migration-class budget for live-test fixture setup, verification
/// reads, and teardown. The transport fails closed on unenveloped statements
/// (issue #600 W12), so every raw statement a live test issues must ride an
/// explicit envelope. Migration class arms no drop-guard KILLs, keeping
/// fixture DDL and teardown immune to spurious cancellation while every
/// statement still carries a finite server budget.
fn live_fixture_budget() -> ValidatedQueryBudget {
    let config = QueryBudgetsConfig {
        migration: QueryBudgetClassConfig {
            deadline_seconds: 7_200.0,
            memory_bytes: QUERY_MEMORY_BACKSTOP_BYTES,
            spill_bytes: QUERY_MEMORY_BACKSTOP_BYTES / 4,
            read_rows: 1_000_000_000_000,
            read_bytes: 1_000_000_000_000_000,
            statement_cap: 1_000_000,
        },
        ..QueryBudgetsConfig::default()
    };
    ValidatedQueryBudgets::from_config(&config)
        .expect("live fixture budget must validate")
        .migration
}

/// Run one live test body under the shared fixture envelope. Operations
/// under test build their own class envelopes inside this scope, exactly the
/// way production boundaries nest inside a process that already has ambient
/// work running.
async fn with_live_fixture_envelope<F: std::future::Future>(f: F) -> F::Output {
    QueryEnvelope::new(
        "live-fixture",
        QueryClass::Migration,
        &live_fixture_budget(),
    )
    .scope(f)
    .await
}

/// The bundled-default Interactive budget, for scoping request-shaped live
/// operations exactly the way the MCP and monitor boundaries do.
fn default_interactive_budget() -> ValidatedQueryBudget {
    ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
        .expect("bundled default query budgets are valid")
        .interactive
}

async fn install_schema_fixture(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let database = database.as_str();
    let insert = format!(
        r#"INSERT INTO `{database}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, op_kind, request_id,
  trace_id, turn_index, model, input_tokens, output_tokens, text_content, payload_json,
  event_version
)
VALUES
(
  addMonths(now64(3), -1), 'issue454-dedup', 'issue454-dedup-session', today(),
  'fixture', 'codex', 'fixture-dedup', 'fixture-old', '2026-01-02T03:04:04.500Z',
  toStartOfMinute(now64(3)), 'message', 'assistant', 'text', '', 'issue454-request', 'issue454-trace',
  1, 'old-model', 11, 0, 'old', '{{}}', 1
),
(
  now64(3), 'issue454-dedup', 'issue454-dedup-session', today(), 'fixture', 'codex',
  'fixture-dedup', 'fixture-new', '2026-01-02T03:04:05.678Z', toStartOfMinute(now64(3)), 'message',
  'assistant', 'text', '', 'issue454-request', 'issue454-trace', 1, 'new-model',
  13, 0, 'new', '{{}}', 2
),
(
  now64(3), 'issue454-web-a', 'issue454-web-session', today(), 'fixture', 'codex',
  'fixture-web', 'issue454-web-a', '2026-01-02T03:05:00.000Z', now64(3),
  'tool_call', 'assistant', 'web_search_call', 'search', '', 'issue454-trace', 1,
  '', 0, 0, '', '{{"action":{{"query":"a"}}}}', 1
),
(
  now64(3), 'issue454-web-z', 'issue454-web-session', today(), 'fixture', 'codex',
  'fixture-web', 'issue454-web-z', '2026-01-02T03:05:00.000Z', now64(3),
  'tool_call', 'assistant', 'web_search_call', 'search', '', 'issue454-trace', 1,
  '', 0, 0, '', '{{"action":{{"query":"z"}}}}', 1
)"#
    );
    clickhouse
        .request_text(&insert, None, Some(database), false, None)
        .await
        .context("failed to insert live-schema fixtures")?;
    let commentary_insert = format!(
        r#"INSERT INTO `{database}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, op_kind, op_status,
  request_id, trace_id, turn_index, model, input_tokens, output_tokens, text_content,
  payload_json, event_version
)
VALUES
(
  now64(3), 'issue549-user', 'issue549-commentary-session', today(), 'fixture', 'codex',
  'fixture-commentary', 'issue549-user', '2026-07-15T20:00:00.000Z', now64(3),
  'message', 'user', 'message', '', '', 'issue549-request', 'issue549-trace', 1,
  '', 0, 0, 'Check the repository.', '{{}}', 1
),
(
  now64(3), 'issue549-commentary', 'issue549-commentary-session', today(), 'fixture',
  'codex', 'fixture-commentary', 'issue549-commentary', '2026-07-15T20:00:01.000Z',
  now64(3), 'message', 'assistant', 'message', '', 'commentary', 'issue549-request',
  'issue549-trace', 1, '', 0, 0, 'I am still checking the repository.', '{{}}', 1
)"#
    );
    clickhouse
        .request_text(&commentary_insert, None, Some(database), false, None)
        .await
        .context("failed to insert commentary projection fixture")?;
    let omp_insert = format!(
        r#"INSERT INTO `{database}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, op_kind, request_id,
  trace_id, turn_index, model, input_tokens, output_tokens, text_content, payload_json,
  event_version
)
VALUES
(
  now64(3), 'issue568-explicit-title', 'issue568-explicit-session', today(),
  'omp', 'pi-coding-agent', '/tmp/omp/ExplicitTask.jsonl', 'issue568-explicit-title',
  '2026-07-17T01:00:00.000Z', '2026-07-17 01:00:00.000', 'unknown', 'system',
  'unknown', 'title_change', '', 'issue568-explicit-trace', 1, '', 0, 0,
  'Explicit OMP Title', '{{"type":"title_change","title":"Explicit OMP Title"}}', 1
),
(
  now64(3), 'issue568-later-name', 'issue568-explicit-session', today(),
  'omp', 'pi-coding-agent', '/tmp/omp/ExplicitTask.jsonl', 'issue568-later-name',
  '2026-07-17T01:00:01.000Z', '2026-07-17 01:00:01.000', 'session_meta', 'system',
  'session_meta', 'session_info', '', 'issue568-explicit-trace', 1, '', 0, 0,
  '', '{{"name":"Later Lower Priority Name"}}', 1
),
(
  now64(3), 'issue568-windows-fallback', 'issue568-windows-session', today(),
  'omp', 'pi-coding-agent',
  'C:\\Users\\alice\\.omp\\agent\\sessions\\project\\ReviewScope-2.jsonl',
  'issue568-windows-fallback', '2026-07-17T01:01:00.000Z', '2026-07-17 01:01:00.000',
  'message', 'user', 'user_message', '', '', 'issue568-windows-trace', 1, '', 0, 0,
  'private prompt', '{{}}', 1
),
(
  now64(3), 'issue568-main-file', '11111111-2222-4333-8444-555555555568', today(),
  'omp', 'pi-coding-agent',
  '/tmp/omp/11111111-2222-4333-8444-555555555568.jsonl',
  'issue568-main-file', '2026-07-17T01:02:00.000Z', '2026-07-17 01:02:00.000',
  'message', 'user', 'user_message', '', '', 'issue568-main-trace', 1, '', 0, 0,
  'private main prompt', '{{}}', 1
),
(
  now64(3), 'issue568-pi-title', 'issue568-pi-session', today(),
  'pi', 'pi-coding-agent', '/tmp/pi/session.jsonl', 'issue568-pi-title',
  '2026-07-17T01:03:00.000Z', '2026-07-17 01:03:00.000', 'session_meta', 'system',
  'session_meta', 'session', '', 'issue568-pi-trace', 1, '', 0, 0,
  'Pi Title', '{{"title":"Pi Title"}}', 1
),
(
  now64(3), 'issue568-pi-name', 'issue568-pi-session', today(),
  'pi', 'pi-coding-agent', '/tmp/pi/session.jsonl', 'issue568-pi-name',
  '2026-07-17T01:03:01.000Z', '2026-07-17 01:03:01.000', 'session_meta', 'system',
  'session_meta', 'session_info', '', 'issue568-pi-trace', 1, '', 0, 0,
  '', '{{"name":"Later Pi Name"}}', 1
),
(
  now64(3), 'issue568-pi-summary', 'issue568-pi-summary-session', today(),
  'pi', 'pi-coding-agent', '/tmp/pi/summary-session.jsonl', 'issue568-pi-summary',
  '2026-07-17T01:04:00.000Z', '2026-07-17 01:04:00.000', 'session_meta', 'system',
  'session_meta', 'session_info', '', 'issue568-pi-summary-trace', 1, '', 0, 0,
  '', '{{"summary":"Pi Summary"}}', 1
)"#
    );
    clickhouse
        .request_text(&omp_insert, None, Some(database), false, None)
        .await
        .context("failed to insert OMP session metadata fixtures")?;
    Ok(())
}

async fn publish_missing_schema_fixture_sources(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let database = database.as_str();
    let publish = format!(
        r#"INSERT INTO `{database}`.`published_source_generations`
(
  source_host, source_name, source_file, source_generation, publication_revision,
  publisher_id, operation_id, published_at
)
WITH
  (
    SELECT ifNull(max(publication_revision), toUInt64(0))
    FROM `{database}`.`v_published_source_generation_history`
  ) AS base_revision
SELECT
  source_host,
  source_name,
  source_file,
  source_generation,
  base_revision + toUInt64(row_number() OVER (
    ORDER BY source_host, source_name, source_file
  )) AS publication_revision,
  'live-schema-fixture' AS publisher_id,
  concat('live-schema-fixture:', hex(cityHash64(
    source_host, source_name, source_file, source_generation
  ))) AS operation_id,
  now64(3) AS published_at
FROM
(
  SELECT
    source_host,
    source_name,
    source_file,
    max(source_generation) AS source_generation
  FROM `{database}`.`events` FINAL
  WHERE tuple(source_host, toString(source_name), source_file) NOT IN
  (
    SELECT tuple(source_host, toString(source_name), source_file)
    FROM `{database}`.`v_current_published_source_generations`
  )
  GROUP BY source_host, source_name, source_file
)"#
    );
    clickhouse
        .request_text(&publish, None, Some(database), false, None)
        .await
        .context("failed to publish live-schema fixture source heads")?;

    #[derive(Deserialize)]
    struct MissingHeadCount {
        value: u64,
    }
    let missing = clickhouse
        .query_rows::<MissingHeadCount>(
            &format!(
                r#"SELECT count() AS value
FROM
(
  SELECT DISTINCT source_host, source_name, source_file, source_generation
  FROM `{database}`.`events` FINAL
) AS events
LEFT ANTI JOIN `{database}`.`v_current_published_source_generations` AS heads
  ON events.source_host = heads.source_host
 AND events.source_name = heads.source_name
 AND events.source_file = heads.source_file
 AND events.source_generation = heads.source_generation
FORMAT JSONEachRow"#
            ),
            Some(database),
        )
        .await
        .context("failed to verify live-schema fixture source heads")?
        .into_iter()
        .next()
        .context("live-schema fixture source-head verification returned no row")?
        .value;
    if missing != 0 {
        bail!("live-schema fixture has {missing} unpublished source generation(s)");
    }
    Ok(())
}

/// Publish-and-verify variant for REPLACEMENT fixtures: publishes each
/// source's max generation (same statement as
/// `publish_missing_schema_fixture_sources`) but verifies only that every
/// source's LATEST generation is published — superseded generations are the
/// expected residue of a replacement and stay unpublished by design.
async fn publish_replaced_schema_fixture_sources(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let database = database.as_str();
    // Unlike `publish_missing_schema_fixture_sources` (which only covers
    // never-published sources), a replacement must ADVANCE an existing head:
    // publish the latest events generation for every source whose current
    // published generation is behind it. A plain LEFT JOIN yields default
    // values (generation 0) for never-published sources, so one predicate
    // covers both never-published and superseded heads.
    let publish = format!(
        r#"INSERT INTO `{database}`.`published_source_generations`
(
  source_host, source_name, source_file, source_generation, publication_revision,
  publisher_id, operation_id, published_at
)
WITH
  (
    SELECT ifNull(max(publication_revision), toUInt64(0))
    FROM `{database}`.`v_published_source_generation_history`
  ) AS base_revision
SELECT
  latest.source_host,
  latest.source_name,
  latest.source_file,
  latest.source_generation,
  base_revision + toUInt64(row_number() OVER (
    ORDER BY latest.source_host, latest.source_name, latest.source_file
  )) AS publication_revision,
  'live-schema-fixture' AS publisher_id,
  concat('live-schema-fixture:', hex(cityHash64(
    latest.source_host, latest.source_name, latest.source_file, latest.source_generation
  ))) AS operation_id,
  now64(3) AS published_at
FROM
(
  SELECT
    source_host,
    source_name,
    source_file,
    max(source_generation) AS source_generation
  FROM `{database}`.`events` FINAL
  GROUP BY source_host, source_name, source_file
) AS latest
LEFT JOIN `{database}`.`v_current_published_source_generations` AS heads
  ON heads.source_host = latest.source_host
 AND toString(heads.source_name) = toString(latest.source_name)
 AND heads.source_file = latest.source_file
WHERE heads.source_generation < latest.source_generation"#
    );
    clickhouse
        .request_text(&publish, None, Some(database), false, None)
        .await
        .context("failed to publish replaced-fixture source heads")?;
    #[derive(Deserialize)]
    struct MissingHeadCount {
        value: u64,
    }
    let missing_latest = clickhouse
        .query_rows::<MissingHeadCount>(
            &format!(
                r#"SELECT count() AS value
FROM
(
  SELECT source_host, source_name, source_file, max(source_generation) AS source_generation
  FROM `{database}`.`events` FINAL
  GROUP BY source_host, source_name, source_file
) AS latest
LEFT ANTI JOIN `{database}`.`v_current_published_source_generations` AS heads
  ON latest.source_host = heads.source_host
 AND latest.source_name = heads.source_name
 AND latest.source_file = heads.source_file
 AND latest.source_generation = heads.source_generation
FORMAT JSONEachRow"#
            ),
            Some(database),
        )
        .await
        .context("failed to verify replaced-fixture latest source heads")?
        .into_iter()
        .next()
        .context("replaced-fixture head verification returned no row")?
        .value;
    if missing_latest != 0 {
        bail!("replaced fixture has {missing_latest} source(s) whose latest generation is unpublished");
    }
    Ok(())
}

async fn assert_omp_session_metadata(repository: &ClickHouseConversationRepository) -> Result<()> {
    let listed = repository
        .list_mcp_sessions(
            McpSessionListFilter {
                start_unix_ms: 0,
                end_unix_ms: 4_102_444_800_000,
                mode: None,
                sort: ConversationListSort::Asc,
                harness: None,
                source_name: None,
            },
            PageRequest {
                limit: 100,
                cursor: None,
            },
        )
        .await
        .context("failed to list OMP session metadata fixtures")?;

    let explicit = listed
        .items
        .iter()
        .find(|session| session.session_id == "issue568-explicit-session")
        .context("explicit OMP session missing from listing")?;
    assert_eq!(explicit.title.as_deref(), Some("Explicit OMP Title"));
    assert_eq!(
        explicit.session_summary.as_deref(),
        Some("Explicit OMP Title")
    );

    let windows = listed
        .items
        .iter()
        .find(|session| session.session_id == "issue568-windows-session")
        .context("Windows-path OMP session missing from listing")?;
    assert_eq!(windows.title.as_deref(), Some("ReviewScope-2"));
    assert_eq!(windows.session_summary.as_deref(), Some("ReviewScope-2"));

    let main = listed
        .items
        .iter()
        .find(|session| session.session_id == "11111111-2222-4333-8444-555555555568")
        .context("UUID main-file OMP session missing from listing")?;
    assert!(main.title.is_none());
    assert!(main.session_summary.is_none());

    let pi = listed
        .items
        .iter()
        .find(|session| session.session_id == "issue568-pi-session")
        .context("Pi precedence fixture missing from listing")?;
    assert_eq!(pi.title.as_deref(), Some("Later Pi Name"));
    assert_eq!(pi.session_summary.as_deref(), Some("Later Pi Name"));

    let pi_summary = listed
        .items
        .iter()
        .find(|session| session.session_id == "issue568-pi-summary-session")
        .context("Pi summary-only fixture missing from listing")?;
    assert_eq!(pi_summary.title.as_deref(), Some("Pi Summary"));
    assert_eq!(pi_summary.session_summary.as_deref(), Some("Pi Summary"));

    for (session_id, expected_title, expected_summary) in [
        (
            "issue568-explicit-session",
            Some("Explicit OMP Title"),
            Some("Explicit OMP Title"),
        ),
        (
            "issue568-windows-session",
            Some("ReviewScope-2"),
            Some("ReviewScope-2"),
        ),
        ("11111111-2222-4333-8444-555555555568", None, None),
        ("issue568-pi-session", Some("Pi Title"), Some("Pi Title")),
        ("issue568-pi-summary-session", None, Some("Pi Summary")),
    ] {
        let opened = canonical_session(repository, session_id)
            .await?
            .with_context(|| format!("session metadata fixture {session_id} missing"))?;
        assert_eq!(opened.title.as_deref(), expected_title);
        assert_eq!(opened.session_summary.as_deref(), expected_summary);
    }

    Ok(())
}

/// One canonical `open(session)` page big enough to hold any fixture session —
/// the root suite's stand-in for the retired v1 `get_mcp_session` (issue #603
/// WI-10; the paging contract itself is the parity gate's subject).
async fn canonical_session(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
) -> Result<Option<McpSessionOpen>> {
    match repository
        .canonical_open_session_page(session_id, 500, None)
        .await
        .with_context(|| format!("canonical session open failed for {session_id}"))?
    {
        None => Ok(None),
        Some(CanonicalReadOutcome::Reopen) => {
            bail!("unexpected reopen during quiescent canonical open of {session_id}")
        }
        Some(CanonicalReadOutcome::Page(page)) => Ok(Some(page.session)),
    }
}

/// One canonical `open(turn)` page big enough to hold any fixture turn.
async fn canonical_turn(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
    turn_seq: u32,
) -> Result<Option<McpTurnOpen>> {
    match repository
        .canonical_open_turn_page(session_id, turn_seq, 500, true, None)
        .await
        .with_context(|| format!("canonical turn open failed for {session_id}/{turn_seq}"))?
    {
        None => Ok(None),
        Some(CanonicalReadOutcome::Reopen) => {
            bail!("unexpected reopen during quiescent canonical turn open of {session_id}")
        }
        Some(CanonicalReadOutcome::Page(page)) => Ok(Some(page.turn)),
    }
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_schema_semantics_and_teardown() -> Result<()> {
    with_live_fixture_envelope(live_schema_semantics_and_teardown_body()).await
}

async fn live_schema_semantics_and_teardown_body() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before mutation").await?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate temporary live-schema database")?;
        let repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());

        let empty = repository
            .analytics_series(AnalyticsRange::FifteenMinutes)
            .await
            .context("empty-window analytics read failed")?;
        assert!(empty.tokens.is_empty());
        assert!(empty.turns.is_empty());
        assert!(empty.concurrent_sessions.is_empty());
        let empty_heartbeat = repository
            .latest_ingest_heartbeat()
            .await
            .context("empty heartbeat read failed")?;
        assert!(empty_heartbeat.table_present);
        assert!(empty_heartbeat.latest.is_none());

        clickhouse
            .request_text(
                &format!(
                    "ALTER TABLE `{}`.`ingest_heartbeats` DROP COLUMN IF EXISTS backend_sinks",
                    database.as_str()
                ),
                None,
                Some(database.as_str()),
                false,
                None,
            )
            .await
            .context("failed to remove optional heartbeat column")?;
        let legacy_repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let legacy_heartbeat = legacy_repository
            .latest_ingest_heartbeat()
            .await
            .context("legacy heartbeat read failed")?;
        assert!(legacy_heartbeat.table_present);
        assert!(legacy_heartbeat.latest.is_none());

        install_schema_fixture(&clickhouse, &database).await?;
        // This fixture writes physical rows directly rather than exercising the
        // ingest publication actor. Mirror the actor's final authorization step
        // so canonical live views can observe only the fixture's published rows.
        publish_missing_schema_fixture_sources(&clickhouse, &database).await?;
        clickhouse
            .backfill_canonical_read_indexes(
                true,
                &live_fixture_budget(),
                &default_interactive_budget(),
                |_| {},
            )
            .await
            .context("failed to index live-schema fixtures for canonical open")?;
        assert_omp_session_metadata(&repository).await?;
        let commentary_turn = canonical_turn(&repository, "issue549-commentary-session", 1)
            .await?
            .context("commentary turn missing")?;
        assert!(!commentary_turn.completed);
        assert!(commentary_turn.terminal_event_uid.is_none());
        assert!(commentary_turn.final_response_summary.is_none());
        assert!(commentary_turn.events.iter().any(|event| {
            event.event_uid == "issue549-commentary"
                && event.event_type == "assistant_response"
                && event.phase == "commentary"
        }));

        #[derive(Deserialize)]
        struct TurnTimestampTypeRow {
            started_at_type: String,
            ended_at_type: String,
        }
        let turn_timestamp_types: Vec<TurnTimestampTypeRow> = clickhouse
            .query_rows(
                &format!(
                    "SELECT toTypeName(started_at) AS started_at_type, \
                     toTypeName(ended_at) AS ended_at_type \
                     FROM `{}`.`v_turn_summary` \
                     WHERE session_id = 'issue454-dedup-session' \
                     FORMAT JSONEachRow",
                    database.as_str()
                ),
                Some(database.as_str()),
            )
            .await
            .context("turn summary timestamp type query failed")?;
        assert_eq!(turn_timestamp_types.len(), 1);
        assert!(
            turn_timestamp_types[0]
                .started_at_type
                .starts_with("DateTime64(3"),
            "started_at must remain typed: {}",
            turn_timestamp_types[0].started_at_type
        );
        assert!(
            turn_timestamp_types[0]
                .ended_at_type
                .starts_with("DateTime64(3"),
            "ended_at must remain typed: {}",
            turn_timestamp_types[0].ended_at_type
        );

        #[derive(Deserialize)]
        struct ModelRow {
            model: String,
        }
        let canonical: Vec<ModelRow> = clickhouse
            .query_rows(
                &format!(
                    "SELECT model FROM (SELECT * FROM `{}`.`events` FINAL \
                     SETTINGS do_not_merge_across_partitions_select_final = 0) \
                     WHERE event_uid = 'issue454-dedup' FORMAT JSONEachRow",
                    database.as_str()
                ),
                Some(database.as_str()),
            )
            .await
            .context("canonical cross-partition fixture query failed")?;
        assert_eq!(canonical.len(), 1);
        assert_eq!(canonical[0].model, "new-model");
        let stale_predicate: Vec<ModelRow> = clickhouse
            .query_rows(
                &format!(
                    "SELECT model FROM (SELECT * FROM `{}`.`events` FINAL \
                     SETTINGS do_not_merge_across_partitions_select_final = 0) \
                     WHERE event_uid = 'issue454-dedup' AND model = 'old-model' \
                     FORMAT JSONEachRow",
                    database.as_str()
                ),
                Some(database.as_str()),
            )
            .await
            .context("canonical outer-predicate query failed")?;
        assert!(stale_predicate.is_empty());

        let populated_repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let analytics = populated_repository
            .analytics_series(AnalyticsRange::FifteenMinutes)
            .await
            .context("populated analytics read failed")?;
        assert!(analytics
            .tokens
            .iter()
            .any(|point| point.model == "new-model"));
        assert!(!analytics
            .tokens
            .iter()
            .any(|point| point.model == "old-model"));
        let listed = populated_repository
            .list_mcp_sessions(
                McpSessionListFilter {
                    start_unix_ms: 0,
                    end_unix_ms: 4_102_444_800_000,
                    mode: None,
                    sort: ConversationListSort::Desc,
                    harness: None,
                    source_name: None,
                },
                PageRequest {
                    limit: 100,
                    cursor: None,
                },
            )
            .await
            .context("populated session listing failed")?;
        let deduped = listed
            .items
            .iter()
            .find(|session| session.session_id == "issue454-dedup-session")
            .context("deduped fixture session missing")?;
        assert_eq!(deduped.total_events, 1);
        const FIXTURE_EVENT_UNIX_MS: i64 = 1_767_323_045_678;
        assert_eq!(deduped.first_event_unix_ms, FIXTURE_EVENT_UNIX_MS);
        assert_eq!(deduped.last_event_unix_ms, FIXTURE_EVENT_UNIX_MS);

        let listed_turns = populated_repository
            .list_turns(
                "issue454-dedup-session",
                TurnListFilter::default(),
                PageRequest::default(),
            )
            .await
            .context("typed turn list projection failed")?;
        assert_eq!(listed_turns.items.len(), 1);
        assert_eq!(
            listed_turns.items[0].started_at_unix_ms,
            FIXTURE_EVENT_UNIX_MS
        );
        assert_eq!(
            listed_turns.items[0].ended_at_unix_ms,
            FIXTURE_EVENT_UNIX_MS
        );

        let mcp_session = canonical_session(&populated_repository, "issue454-dedup-session")
            .await?
            .context("MCP fixture session missing")?;
        assert_eq!(mcp_session.turns.len(), 1);
        assert_eq!(
            mcp_session.turns[0].metadata.started_at_unix_ms,
            FIXTURE_EVENT_UNIX_MS
        );
        let mcp_turn = canonical_turn(&populated_repository, "issue454-dedup-session", 1)
            .await?
            .context("bounded MCP turn missing")?;
        assert_eq!(mcp_turn.events.len(), 1);
        assert_eq!(mcp_turn.events[0].text_preview.as_deref(), Some("new"));
        let mcp_event = populated_repository
            .canonical_open_event("issue454-dedup")
            .await
            .context("bounded MCP event read failed")?
            .context("bounded MCP event missing")?;
        assert_eq!(mcp_event.event.text_content, "new");
        assert_eq!(mcp_event.parent_turn.turn_seq, 1);
        let turn = populated_repository
            .get_turn("issue454-dedup-session", 1)
            .await
            .context("view-only turn read failed")?
            .context("view-only fixture turn missing")?;
        assert_eq!(turn.summary.total_events, 1);
        assert_eq!(turn.summary.started_at_unix_ms, FIXTURE_EVENT_UNIX_MS);
        assert_eq!(turn.summary.ended_at_unix_ms, FIXTURE_EVENT_UNIX_MS);
        assert_eq!(turn.events.len(), 1);
        assert_eq!(turn.events[0].text_content, "new");
        let web = populated_repository
            .list_web_searches(1000)
            .await
            .context("same-millisecond web feed read failed")?;
        let fixture_refs = web
            .iter()
            .filter(|event| event.session_id == "issue454-web-session")
            .map(|event| event.source_ref.as_str())
            .collect::<Vec<_>>();
        assert_eq!(fixture_refs, vec!["issue454-web-z", "issue454-web-a"]);

        clickhouse
            .request_text(
                &format!(
                    r#"INSERT INTO `{0}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, op_kind, request_id,
  trace_id, turn_index, model, input_tokens, output_tokens, text_content, payload_json,
  event_version
)
SELECT
  now64(3), 'issue454-dedup', 'issue454-dedup-session', today(), 'fixture', 'codex',
  'fixture-dedup', 'fixture-final', '2026-01-02T03:04:06.789Z',
  (SELECT any(event_ts) FROM `{0}`.`events` FINAL WHERE event_uid = 'issue454-dedup'),
  'message', 'assistant', 'text', '', 'issue454-request', 'issue454-trace', 1,
  'final-model', 17, 0, 'replacement-final', '{{}}', 3"#,
                    database.as_str()
                ),
                None,
                Some(database.as_str()),
                false,
                None,
            )
            .await
            .context("failed to insert canonical replacement fixture")?;
        // No projector refresh step exists any more (issue #603 WI-10): the
        // migration-036 MVs fired on the insert itself and RMT(event_version)
        // collapses the re-inserted uid, so the canonical readers serve the
        // replacement immediately.
        let replaced_turn = canonical_turn(&populated_repository, "issue454-dedup-session", 1)
            .await?
            .context("replacement MCP turn missing")?;
        assert_eq!(
            replaced_turn.final_response_summary.as_deref(),
            Some("replacement-final")
        );
        let replaced_event = populated_repository
            .canonical_open_event("issue454-dedup")
            .await
            .context("replacement MCP event read failed")?
            .context("replacement MCP event missing")?;
        assert_eq!(replaced_event.event.text_content, "replacement-final");

        clickhouse
            .request_text(
                &format!(
                    r#"INSERT INTO `{0}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, op_kind, request_id,
  trace_id, turn_index, model, input_tokens, output_tokens, text_content, payload_json,
  event_version
)
SELECT
  now64(3), 'issue454-dedup', 'issue454-dedup-session', today(), 'fixture', 'codex',
  'fixture-dedup', 'fixture-resumed', '2026-01-02T03:04:07.890Z',
  (SELECT any(event_ts) FROM `{0}`.`events` FINAL WHERE event_uid = 'issue454-dedup'),
  'message', 'assistant', 'text', '', 'issue454-request', 'issue454-trace', 1,
  'resumed-model', 19, 0, 'replacement-resumed', '{{}}', 4"#,
                    database.as_str()
                ),
                None,
                Some(database.as_str()),
                false,
                None,
            )
            .await
            .context("failed to insert another canonical replacement")?;
        // The v1 dirty-fence-then-backfill cycle retired with the projector:
        // a higher-version re-insert of the same uid is visible to the
        // canonical readers as soon as the insert lands (the locator and
        // navigation MVs fired on it), with no intermediate dirty state.
        let resumed_event = populated_repository
            .canonical_open_event("issue454-dedup")
            .await
            .context("resumed MCP event read failed")?
            .context("resumed MCP event missing")?;
        assert_eq!(resumed_event.event.text_content, "replacement-resumed");
        clickhouse
            .request_text(
                &format!(
                    r#"INSERT INTO `{}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, turn_index,
  text_content, payload_json, event_version
)
VALUES
  (now64(3), 'issue532-order-a', 'issue532-order-session', today(), 'fixture', 'codex',
   'fixture-order', 'order-a-v1', '2026-07-14T18:00:00.000Z', toDateTime64('2026-07-14 18:00:00', 3),
   'message', 'user', 'text', 0, 'first user input', '{{}}', 1),
  (now64(3), 'issue532-order-b', 'issue532-order-session', today(), 'fixture', 'codex',
   'fixture-order', 'order-b-v1', '2026-07-14T18:00:01.000Z', toDateTime64('2026-07-14 18:00:00', 3),
   'message', 'assistant', 'text', 0, 'initial assistant response', '{{}}', 1),
  (now64(3), 'issue532-order-c', 'issue532-order-session', today(), 'fixture', 'codex',
   'fixture-order', 'order-c-v1', '2026-07-14T18:00:02.000Z', toDateTime64('2026-07-14 18:00:00', 3),
   'queue_operation', 'assistant', 'task_complete', 0, '', '{{}}', 1)"#,
                    database.as_str()
                ),
                None,
                Some(database.as_str()),
                false,
                None,
            )
            .await
            .context("failed to insert ordering fixture")?;
        publish_missing_schema_fixture_sources(&clickhouse, &database).await?;
        clickhouse
            .request_text(
                &format!(
                    r#"INSERT INTO `{}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, record_ts, event_ts, event_kind, actor_kind, payload_type, turn_index,
  text_content, payload_json, event_version
)
VALUES
  (now64(3), 'issue532-order-b', 'issue532-order-session', today(), 'fixture', 'codex',
   'fixture-order', 'order-b-v2', '2026-07-14T17:59:59.000Z', toDateTime64('2026-07-14 18:00:00', 3),
   'message', 'user', 'text', 0, 'replacement user input', '{{}}', 2)"#,
                    database.as_str()
                ),
                None,
                Some(database.as_str()),
                false,
                None,
            )
            .await
            .context("failed to insert ordering replacement")?;

        let reordered = populated_repository
            .canonical_open_event("issue532-order-b")
            .await
            .context("reordered MCP event read failed")?
            .context("reordered MCP event missing")?;
        assert_eq!(reordered.event.event_order, 1);
        assert_eq!(reordered.event.turn_seq, 1);
        assert_eq!(reordered.event.text_content, "replacement user input");
        assert_eq!(
            reordered
                .next_event
                .as_ref()
                .map(|event| event.event_uid.as_str()),
            Some("issue532-order-a")
        );
        assert_eq!(
            reordered.next_turn.as_ref().map(|turn| turn.turn_seq),
            Some(2)
        );
        let reordered_session = canonical_session(&populated_repository, "issue532-order-session")
            .await?
            .context("reordered MCP session missing")?;
        assert_eq!(reordered_session.turns.len(), 2);
        assert!(reordered_session.completed);
        Ok(())
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census = assert_owned_database_census_empty(&clickhouse, "after cleanup").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

fn direct_analytics_semantics(
    snapshot: moraine_conversations::AnalyticsSnapshot,
) -> Result<SemanticObservation> {
    let payload = json!({
        "tokens": snapshot.tokens,
        "turns": snapshot.turns,
        "concurrent_sessions": snapshot.concurrent_sessions,
    });
    SemanticObservation::new(
        Cardinality::AnalyticsSeries {
            tokens: payload["tokens"].as_array().map_or(0, Vec::len),
            turns: payload["turns"].as_array().map_or(0, Vec::len),
            concurrent_sessions: payload["concurrent_sessions"]
                .as_array()
                .map_or(0, Vec::len),
        },
        &payload,
    )
}

/// The repository side of the session-feed parity gate. Both sides now read
/// `list_mcp_sessions`, so this observation is over the shared operation's own
/// item rather than the projector's analytics row (issue-599 §5.7).
fn direct_sessions_semantics(
    sessions: &[moraine_conversations::McpSessionListItem],
) -> Result<SemanticObservation> {
    let payload = Value::Array(
        sessions
            .iter()
            .map(|session| {
                json!({
                    "session_id": session.session_id,
                    "harness": session.harness.clone().unwrap_or_default(),
                    "started_at_unix_ms": session.first_event_unix_ms,
                    "ended_at_unix_ms": session.last_event_unix_ms,
                    "mode": session.mode.as_str(),
                    "turn_count": session.total_turns,
                    "event_count": session.total_events,
                    "tool_call_count": session.tool_calls,
                })
            })
            .collect(),
    );
    SemanticObservation::new(
        Cardinality::Sessions {
            sessions: sessions.len(),
        },
        &payload,
    )
}

async fn monitor_semantics<T: MonitorResponse>(
    client: &Client,
    url: &Url,
) -> Result<SemanticObservation> {
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("monitor request failed: {url}"))?;
    let status = response.status();
    let body = response.bytes().await.context("monitor body read failed")?;
    if !status.is_success() {
        bail!(
            "monitor returned HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    serde_json::from_slice::<T>(&body)
        .context("monitor returned invalid JSON")?
        .into_semantics()
}

async fn start_owned_monitor(
    repository: ClickHouseConversationRepository,
) -> Result<(
    Url,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
    std::path::PathBuf,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind monitor listener")?;
    let port = listener.local_addr()?.port();

    let base = Url::parse(&format!("http://127.0.0.1:{port}/api/v1/"))?;
    let health_url = base.join("health")?;
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let injected: Arc<dyn ConversationRepository> = Arc::new(repository);
    let router = Arc::new(BackendRepositoryRouter::from_preloaded_for_testing(
        Arc::new(AppConfig::default()),
        [(DEFAULT_BACKEND_NAME.to_string(), injected)],
    )?);

    let static_dir =
        env::temp_dir().join(format!("moraine-live-monitor-{}", Uuid::new_v4().simple()));
    fs::create_dir(&static_dir).context("failed to create monitor static directory")?;
    if let Err(error) = fs::write(static_dir.join("index.html"), "<!doctype html>") {
        let _ = fs::remove_dir_all(&static_dir);
        return Err(error).context("failed to create monitor static index");
    }

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_static_dir = static_dir.clone();
    let server = tokio::spawn(async move {
        moraine_monitor_core::run_server_with_listener(
            router,
            listener,
            server_static_dir,
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.get(health_url.clone()).send().await {
            Ok(response) if response.status().is_success() => break,
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(20)).await,
            _ => {
                let _ = shutdown_tx.send(());
                let _ = server.await;
                let _ = fs::remove_dir_all(&static_dir);
                bail!("owned monitor did not become ready before timeout");
            }
        }
    }
    Ok((base, shutdown_tx, server, static_dir))
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_monitor_repository_semantic_parity() -> Result<()> {
    with_live_fixture_envelope(live_monitor_repository_semantic_parity_body()).await
}

async fn live_monitor_repository_semantic_parity_body() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before mutation").await?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate parity database")?;
        install_schema_fixture(&clickhouse, &database).await?;

        let monitor_repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let direct_repository =
            ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
        let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
        let (base, shutdown, server, static_dir) = start_owned_monitor(monitor_repository).await?;

        let parity = async {
            let monitor_analytics = monitor_semantics::<MonitorAnalyticsResponse>(
                &client,
                &base.join("analytics?range=24h")?,
            )
            .await?;
            let direct_analytics = direct_analytics_semantics(
                direct_repository
                    .analytics_series(AnalyticsRange::TwentyFourHours)
                    .await?,
            )?;
            let analytics_comparison =
                SemanticComparison::compare(&monitor_analytics, &direct_analytics);
            if !analytics_comparison.passed() {
                bail!("monitor/repository analytics semantic mismatch: {analytics_comparison:?}");
            }

            // The same window the monitor derives from `since=30d`, and the
            // same effective limit its second clamp applies.
            let now_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before the unix epoch")?
                .as_millis() as i64;
            let lookback_ms = i64::from(
                SessionLookback::ThirtyDays
                    .window_seconds()
                    .context("30d lookback has a window")?,
            ) * 1_000;
            let monitor_sessions = monitor_semantics::<MonitorSessionsResponse>(
                &client,
                &base.join("sessions?since=30d&limit=50")?,
            )
            .await?;
            let direct_sessions = direct_sessions_semantics(
                &direct_repository
                    .list_mcp_sessions(
                        McpSessionListFilter {
                            start_unix_ms: (now_unix_ms - lookback_ms).max(0),
                            end_unix_ms: now_unix_ms + 1,
                            mode: None,
                            sort: ConversationListSort::Desc,
                            harness: None,
                            source_name: None,
                        },
                        PageRequest {
                            limit: RepoConfig::default().max_results,
                            cursor: None,
                        },
                    )
                    .await?
                    .items,
            )?;
            let sessions_comparison =
                SemanticComparison::compare(&monitor_sessions, &direct_sessions);
            if !sessions_comparison.passed() {
                bail!("monitor/repository sessions semantic mismatch: {sessions_comparison:?}");
            }
            Ok(())
        }
        .await;

        let _ = shutdown.send(());
        let server_result = match server.await {
            Ok(result) => result,
            Err(error) => Err(anyhow!("owned monitor task failed to join: {error}")),
        };
        let static_cleanup = fs::remove_dir_all(&static_dir)
            .context("failed to remove owned monitor static directory");
        let monitor_cleanup = finish_with_cleanup(server_result, static_cleanup);
        finish_with_cleanup(parity, monitor_cleanup)
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census = assert_owned_database_census_empty(&clickhouse, "after cleanup").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_source_publication_cutover_crash_recovery() -> Result<()> {
    with_live_fixture_envelope(live_source_publication_cutover_crash_recovery_body()).await
}

async fn live_source_publication_cutover_crash_recovery_body() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before mutation").await?;

    let outcome = async {
        source_publication_migration::run(&clickhouse, &database).await?;
        cleanup_database(&clickhouse, &database)
            .await
            .context("failed to reset legacy-migration fixture database")?;

        let migration_started = Instant::now();
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate source-publication database")?;
        let migration_ms = migration_started.elapsed().as_millis() as u64;
        source_publication::run(&clickhouse, &database, migration_ms).await?;
        source_publication_process::run(&clickhouse, &database).await
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census = assert_owned_database_census_empty(&clickhouse, "after cleanup").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}

// Issue #600 exit-gate live tests (design-600.md LIVE TEST PLAN). The bodies
// live in live_clickhouse/envelope_gates.rs; these root-level wrappers keep
// the exact `--exact` libtest paths that scripts/dev/sandbox/run-live-test
// dispatches on.

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_envelope_query_log_coverage() -> Result<()> {
    with_live_fixture_envelope(envelope_gates::query_log_coverage()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_envelope_abandoned_query_cancelled() -> Result<()> {
    with_live_fixture_envelope(envelope_gates::abandoned_query_cancelled()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_envelope_shared_budget_and_statement_cap() -> Result<()> {
    with_live_fixture_envelope(envelope_gates::shared_budget_and_statement_cap()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_envelope_spill_and_memory_ceiling() -> Result<()> {
    with_live_fixture_envelope(envelope_gates::spill_and_memory_ceiling()).await
}

// Issue #598 WI-10 exit-gate live tests (design-598-final.md LIVE TEST PLAN).
// The bodies live in live_clickhouse/canonical_open_parity.rs; these root-level
// wrappers keep the exact `--exact` libtest paths that
// scripts/dev/sandbox/run-live-test dispatches on.

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_canonical_open_parity() -> Result<()> {
    with_live_fixture_envelope(canonical_open_parity::parity()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_canonical_open_locator() -> Result<()> {
    with_live_fixture_envelope(canonical_open_parity::locator()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_canonical_open_continuation() -> Result<()> {
    with_live_fixture_envelope(canonical_open_parity::continuation()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_canonical_open_fence() -> Result<()> {
    with_live_fixture_envelope(canonical_open_parity::fence()).await
}

// Issue #598 WI-11 append-to-visible probe (design-598-final LIVE TEST PLAN,
// BINDING D8: the clock starts at durable canonical insert acknowledgment).
// Wired as the pre-declared `append-to-visible` / `fsync-to-open-valid` modes.
#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_canonical_open_append_to_visible() -> Result<()> {
    with_live_fixture_envelope(canonical_open_parity::append_to_visible()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_canonical_open_fsync_to_open_valid() -> Result<()> {
    with_live_fixture_envelope(canonical_open_parity::fsync_to_open_valid()).await
}

// Issue #598 WI-09 boundedness benchmark (design-598-final LIVE TEST PLAN §6).
// Wired as the pre-declared `canonical-open-bench` run-live-test mode.
#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse, destructive opt-in, and ~2 GB free"]
async fn live_canonical_open_boundedness_benchmark() -> Result<()> {
    with_live_fixture_envelope(canonical_open_benchmark::boundedness()).await
}

// Issue #599 WI-07 exit-gate live tests (599-discovery-cutover.md §5.1-§5.3).
// The bodies live in live_clickhouse/session_list_{parity,benchmark}.rs; these
// root-level wrappers keep the exact `--exact` libtest paths that
// scripts/dev/sandbox/run-live-test dispatches on.

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_session_list_directory_parity() -> Result<()> {
    with_live_fixture_envelope(session_list_parity::directory_parity()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_session_list_query_log_clean() -> Result<()> {
    with_live_fixture_envelope(session_list_parity::query_log_clean()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse, destructive opt-in, and ~2 GB free"]
async fn live_session_list_boundedness_benchmark() -> Result<()> {
    with_live_fixture_envelope(session_list_benchmark::boundedness()).await
}

// Issue #597 exit-gate live tests (plans/597-bounded-search.md §6.3, which
// makes live EXECUTION a hard rule for this work: a shape test cannot catch an
// outer reference to a column the inner derived table fails to project, and
// this epic has shipped that defect twice). The bodies live in
// live_clickhouse/bounded_search.rs; these root-level wrappers keep the exact
// `--exact` libtest paths that scripts/dev/sandbox/run-live-test dispatches on.

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_bounded_search_statement_execution() -> Result<()> {
    with_live_fixture_envelope(bounded_search::statement_execution()).await
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_bounded_search_double_attribution() -> Result<()> {
    with_live_fixture_envelope(bounded_search::double_attribution()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const MARKER: &str = "# Generated by scripts/dev/sandbox/moraine-sandbox for sb-a1b2c3\n[clickhouse]\nurl = \"http://clickhouse:8123\"\ndatabase = \"moraine\"\n";

    #[test]
    fn destructive_prerequisites_fail_closed_before_sql() {
        assert!(validate_prerequisites("", "sb-a1b2c3", "http://clickhouse:8123", MARKER).is_err());
        assert!(
            validate_prerequisites("1", "sb-zzzzzz", "http://clickhouse:8123", MARKER).is_err()
        );
        assert!(validate_prerequisites("1", "sb-a1b2c3", "http://127.0.0.1:8123", MARKER).is_err());
        assert!(validate_prerequisites(
            "1",
            "sb-a1b2c3",
            "http://user:secret@clickhouse:8123",
            MARKER
        )
        .is_err());
        assert!(
            validate_prerequisites("1", "sb-a1b2c3", "http://clickhouse:8123", "wrong marker")
                .is_err()
        );
        validate_prerequisites("1", "sb-a1b2c3", "http://clickhouse:8123", MARKER).unwrap();
    }

    #[test]
    fn concurrent_live_bodies_generate_distinct_internal_database_identities() {
        use std::sync::Barrier;
        use std::thread;

        let start = Arc::new(Barrier::new(3));
        let handles = ["sb-a1b2c3", "sb-d4e5f6"].map(|sandbox_id| {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                let mut diagnostic = Vec::new();
                start.wait();
                let database =
                    prepare_owned_database_identity_with_writer(sandbox_id, &mut diagnostic)
                        .unwrap();
                (sandbox_id, database, String::from_utf8(diagnostic).unwrap())
            })
        });
        start.wait();
        let [(first_sandbox, first, first_diagnostic), (second_sandbox, second, second_diagnostic)] =
            handles.map(|handle| handle.join().unwrap());

        assert_ne!(first, second);
        for (sandbox_id, database, diagnostic) in [
            (first_sandbox, first, first_diagnostic),
            (second_sandbox, second, second_diagnostic),
        ] {
            validate_owned_database_name(database.as_str()).unwrap();
            assert_eq!(database.as_str().len(), OWNED_DATABASE_PREFIX.len() + 32);
            assert!(diagnostic.starts_with(&format!(
                "live ClickHouse database: {}\n",
                database.as_str()
            )));
            assert!(diagnostic.contains(&format!("cleanup: sandbox={sandbox_id} ")));
            assert!(diagnostic.contains(&cleanup_statement(&database)));
        }

        for unsafe_name in [
            "",
            "moraine",
            "moraine_test_",
            "moraine_test_xyz",
            "moraine_test_ABCDEF0123456789abcdef0123456789",
            "other_0123456789abcdef0123456789abcdef",
            "moraine_test_0123456789abcdef0123456789abcde;DROP",
        ] {
            assert!(
                validate_owned_database_name(unsafe_name).is_err(),
                "accepted {unsafe_name:?}"
            );
        }
    }

    #[test]
    fn cleanup_statement_can_only_be_built_from_validated_owned_name() {
        let database = OwnedDatabaseName::generate();
        assert_eq!(
            cleanup_statement(&database),
            format!("DROP DATABASE IF EXISTS `{}` SYNC", database.as_str())
        );
    }

    #[test]
    fn cleanup_composition_preserves_original_and_cleanup_errors() {
        let original = finish_with_cleanup(Err(anyhow!("query failed")), Ok(()))
            .unwrap_err()
            .to_string();
        assert_eq!(original, "query failed");
        let cleanup = finish_with_cleanup(Ok(()), Err(anyhow!("drop failed")))
            .unwrap_err()
            .to_string();
        assert!(cleanup.contains("live ClickHouse teardown failed"));
        let both = finish_with_cleanup(
            Err(anyhow!("assertion failed")),
            Err(anyhow!("drop failed")),
        )
        .unwrap_err()
        .to_string();
        assert!(both.contains("assertion failed"));
        assert!(both.contains("drop failed"));
    }
}
