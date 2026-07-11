use anyhow::{bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::ClickHouseConfig;
use moraine_conversations::{
    AnalyticsRange, ClickHouseConversationRepository, ConversationRepository, RepoConfig,
    SessionAnalyticsQuery, SessionLookback,
};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "analytics_latency/support.rs"]
mod support;
const SEMANTIC_CHECKS_PER_SAMPLE: usize = 8;

use support::{
    AttemptCounts, Cardinality, Cli, DatasetIdentity, DatasetManifest, MeasurementReport,
    Measurements, MonitorAnalyticsResponse, MonitorResponse, MonitorSessionsResponse,
    ProtocolReportInput, SemanticComparison, SemanticObservation, Source, TimedObservation,
    ANALYTICS_24H_SCENARIO, SESSIONS_30D50_SCENARIO,
};

struct BenchmarkConfig {
    monitor_analytics_url: Url,
    monitor_sessions_url: Url,
    dataset_manifest: PathBuf,
    clickhouse: ClickHouseClient,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self> {
        let defaults = ClickHouseConfig::default();
        let monitor_url = required_env("MORAINE_BENCH_MONITOR_URL")?;
        let clickhouse_url = required_env("MORAINE_BENCH_CLICKHOUSE_URL")?;
        let database = optional_env("MORAINE_BENCH_CLICKHOUSE_DATABASE", &defaults.database)?;
        let dataset_manifest = PathBuf::from(required_env("MORAINE_BENCH_DATASET_MANIFEST")?);
        if database.is_empty() {
            bail!("MORAINE_BENCH_CLICKHOUSE_DATABASE must not be empty");
        }
        let username = optional_env("MORAINE_BENCH_CLICKHOUSE_USER", &defaults.username)?;
        let password = optional_env("MORAINE_BENCH_CLICKHOUSE_PASSWORD", &defaults.password)?;
        let monitor_base =
            Url::parse(&monitor_url).context("MORAINE_BENCH_MONITOR_URL is not a valid URL")?;
        let monitor_analytics_url = endpoint(&monitor_base, "/api/v1/analytics?range=24h")?;
        let monitor_sessions_url = endpoint(&monitor_base, "/api/v1/sessions?since=30d&limit=50")?;
        let clickhouse = ClickHouseClient::new(ClickHouseConfig {
            url: clickhouse_url,
            database,
            username,
            password,
            ..defaults
        })
        .context("failed to construct ClickHouse client")?;
        Ok(Self {
            monitor_analytics_url,
            monitor_sessions_url,
            dataset_manifest,
            clickhouse,
        })
    }

    fn fresh_repository(&self) -> ClickHouseConversationRepository {
        ClickHouseConversationRepository::new(
            self.clickhouse.clone(),
            RepoConfig {
                max_results: 50,
                ..RepoConfig::default()
            },
        )
    }
}

fn required_env(name: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) => bail!("{name} must not be empty"),
        Err(env::VarError::NotPresent) => bail!("{name} is required"),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must contain valid UTF-8"),
    }
}

fn optional_env(name: &str, default: &str) -> Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.to_string()),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must contain valid UTF-8"),
    }
}

fn endpoint(base: &Url, path_and_query: &str) -> Result<Url> {
    base.join(path_and_query)
        .with_context(|| format!("failed to construct monitor endpoint {path_and_query}"))
}

async fn sample_monitor<T: MonitorResponse>(
    client: &Client,
    url: &Url,
    label: &str,
) -> Result<TimedObservation> {
    let started = Instant::now();
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("{label} request failed"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("{label} response body failed"))?;
    if !status.is_success() {
        bail!("{label} returned HTTP {status}");
    }
    let semantics = serde_json::from_slice::<T>(&body)
        .with_context(|| format!("{label} returned invalid JSON"))?
        .into_semantics()?;
    Ok(TimedObservation {
        elapsed: started.elapsed(),
        semantics,
    })
}

async fn sample_direct_analytics(
    repository: &ClickHouseConversationRepository,
) -> Result<TimedObservation> {
    let started = Instant::now();
    let snapshot = repository
        .analytics_series(AnalyticsRange::TwentyFourHours)
        .await
        .context("direct analytics repository request failed")?;
    let payload = json!({
        "tokens": snapshot.tokens,
        "turns": snapshot.turns,
        "concurrent_sessions": snapshot.concurrent_sessions,
    });
    let semantics = SemanticObservation::new(
        Cardinality::AnalyticsSeries {
            tokens: payload["tokens"].as_array().map_or(0, Vec::len),
            turns: payload["turns"].as_array().map_or(0, Vec::len),
            concurrent_sessions: payload["concurrent_sessions"]
                .as_array()
                .map_or(0, Vec::len),
        },
        &payload,
    )?;
    Ok(TimedObservation {
        elapsed: started.elapsed(),
        semantics,
    })
}

async fn sample_direct_sessions(
    repository: &ClickHouseConversationRepository,
) -> Result<TimedObservation> {
    let started = Instant::now();
    let sessions = repository
        .list_session_analytics(SessionAnalyticsQuery {
            lookback: SessionLookback::ThirtyDays,
            limit: 50,
        })
        .await
        .context("direct sessions repository request failed")?;
    let payload = Value::Array(
        sessions
            .iter()
            .map(|session| {
                json!({
                    "session_id": session.summary.session_id,
                    "harness": session.harness,
                    "started_at_unix_ms": session.summary.first_event_unix_ms,
                    "ended_at_unix_ms": session.summary.last_event_unix_ms,
                    "models": session.models,
                    "trace_id": session.trace_id,
                    "turns": session.turns.len(),
                    "tool_calls": session.summary.tool_calls,
                })
            })
            .collect(),
    );
    let semantics = SemanticObservation::new(
        Cardinality::Sessions {
            sessions: sessions.len(),
        },
        &payload,
    )?;
    Ok(TimedObservation {
        elapsed: started.elapsed(),
        semantics,
    })
}

#[derive(Deserialize)]
struct SerializedCorpusRow {
    serialized_row: String,
}

async fn load_dataset_identity(config: &BenchmarkConfig) -> Result<DatasetIdentity> {
    let database = &config.clickhouse.config().database;
    let rows = config
        .clickhouse
        .query_rows::<SerializedCorpusRow>(
            "SELECT toJSONString(tuple(*)) AS serialized_row \
             FROM events FINAL FORMAT JSONEachRow",
            Some(database),
        )
        .await
        .context("failed to fingerprint canonical events corpus")?;
    Ok(DatasetIdentity::from_serialized_rows(
        rows.into_iter().map(|row| row.serialized_row),
    ))
}

struct MatrixReports {
    counts: AttemptCounts,
    analytics_monitor: MeasurementReport,
    analytics_repository: MeasurementReport,
    sessions_monitor: MeasurementReport,
    sessions_repository: MeasurementReport,
    analytics_repository_warm: MeasurementReport,
    diagnostics: Vec<Value>,
    failure_details: Vec<Value>,
    setup_failed: bool,
}

impl MatrixReports {
    fn setup_failure(planned: usize, code: &'static str) -> Result<Self> {
        Ok(Self {
            counts: AttemptCounts::new(planned)?,
            analytics_monitor: Measurements::default().report(),
            analytics_repository: Measurements::default().report(),
            sessions_monitor: Measurements::default().report(),
            sessions_repository: Measurements::default().report(),
            analytics_repository_warm: Measurements::default().report(),
            diagnostics: vec![json!({"code": code})],
            failure_details: vec![json!({"code": code})],
            setup_failed: true,
        })
    }

    fn semantic_passed(&self) -> bool {
        !self.setup_failed
            && self.counts.attempted() == self.counts.planned()
            && self.counts.errors() == 0
    }
}

struct MatrixSample {
    analytics_monitor: TimedObservation,
    analytics_repository: TimedObservation,
    sessions_monitor: TimedObservation,
    sessions_repository: TimedObservation,
    analytics_repository_warm: TimedObservation,
}

struct SampleFailure {
    code: &'static str,
    error: anyhow::Error,
}

impl SampleFailure {
    fn new(code: &'static str, error: anyhow::Error) -> Self {
        Self { code, error }
    }
}

async fn run_sample(
    config: &BenchmarkConfig,
    client: &Client,
    warm_repository: &ClickHouseConversationRepository,
    iteration: usize,
) -> std::result::Result<MatrixSample, SampleFailure> {
    let cold_analytics_repository = config.fresh_repository();
    let cold_sessions_repository = config.fresh_repository();
    let (analytics_monitor, analytics_repository, sessions_monitor, sessions_repository) =
        if iteration.is_multiple_of(2) {
            let analytics_monitor = sample_monitor::<MonitorAnalyticsResponse>(
                client,
                &config.monitor_analytics_url,
                "monitor analytics",
            )
            .await
            .map_err(|error| SampleFailure::new("monitor-analytics-error", error))?;
            let analytics_repository = sample_direct_analytics(&cold_analytics_repository)
                .await
                .map_err(|error| SampleFailure::new("repository-analytics-error", error))?;
            let sessions_monitor = sample_monitor::<MonitorSessionsResponse>(
                client,
                &config.monitor_sessions_url,
                "monitor sessions",
            )
            .await
            .map_err(|error| SampleFailure::new("monitor-sessions-error", error))?;
            let sessions_repository = sample_direct_sessions(&cold_sessions_repository)
                .await
                .map_err(|error| SampleFailure::new("repository-sessions-error", error))?;
            (
                analytics_monitor,
                analytics_repository,
                sessions_monitor,
                sessions_repository,
            )
        } else {
            let analytics_repository = sample_direct_analytics(&cold_analytics_repository)
                .await
                .map_err(|error| SampleFailure::new("repository-analytics-error", error))?;
            let analytics_monitor = sample_monitor::<MonitorAnalyticsResponse>(
                client,
                &config.monitor_analytics_url,
                "monitor analytics",
            )
            .await
            .map_err(|error| SampleFailure::new("monitor-analytics-error", error))?;
            let sessions_repository = sample_direct_sessions(&cold_sessions_repository)
                .await
                .map_err(|error| SampleFailure::new("repository-sessions-error", error))?;
            let sessions_monitor = sample_monitor::<MonitorSessionsResponse>(
                client,
                &config.monitor_sessions_url,
                "monitor sessions",
            )
            .await
            .map_err(|error| SampleFailure::new("monitor-sessions-error", error))?;
            (
                analytics_monitor,
                analytics_repository,
                sessions_monitor,
                sessions_repository,
            )
        };
    let analytics_repository_warm = sample_direct_analytics(warm_repository)
        .await
        .map_err(|error| SampleFailure::new("repository-warm-analytics-error", error))?;
    Ok(MatrixSample {
        analytics_monitor,
        analytics_repository,
        sessions_monitor,
        sessions_repository,
        analytics_repository_warm,
    })
}

fn validate_sample(
    sample: &MatrixSample,
    manifest: &DatasetManifest,
    analytics_monitor: &Measurements,
    analytics_repository: &Measurements,
    sessions_monitor: &Measurements,
    sessions_repository: &Measurements,
    analytics_repository_warm: &Measurements,
) -> std::result::Result<(), SampleFailure> {
    manifest
        .verify_observation(
            ANALYTICS_24H_SCENARIO,
            &sample.analytics_monitor.semantics,
            "monitor analytics",
        )
        .and_then(|()| {
            manifest.verify_observation(
                ANALYTICS_24H_SCENARIO,
                &sample.analytics_repository.semantics,
                "repository analytics",
            )
        })
        .and_then(|()| {
            manifest.verify_observation(
                SESSIONS_30D50_SCENARIO,
                &sample.sessions_monitor.semantics,
                "monitor sessions",
            )
        })
        .and_then(|()| {
            manifest.verify_observation(
                SESSIONS_30D50_SCENARIO,
                &sample.sessions_repository.semantics,
                "repository sessions",
            )
        })
        .and_then(|()| {
            manifest.verify_observation(
                ANALYTICS_24H_SCENARIO,
                &sample.analytics_repository_warm.semantics,
                "warm repository analytics",
            )
        })
        .map_err(|error| SampleFailure::new("semantic-seed-oracle-mismatch", error))?;
    analytics_monitor
        .validate(&sample.analytics_monitor, "monitor analytics")
        .and_then(|()| {
            analytics_repository.validate(&sample.analytics_repository, "repository analytics")
        })
        .and_then(|()| sessions_monitor.validate(&sample.sessions_monitor, "monitor sessions"))
        .and_then(|()| {
            sessions_repository.validate(&sample.sessions_repository, "repository sessions")
        })
        .and_then(|()| {
            analytics_repository_warm.validate(
                &sample.analytics_repository_warm,
                "warm repository analytics",
            )
        })
        .map_err(|error| SampleFailure::new("semantic-oracle-instability", error))?;
    let comparisons = [
        SemanticComparison::compare(
            &sample.analytics_monitor.semantics,
            &sample.analytics_repository.semantics,
        ),
        SemanticComparison::compare(
            &sample.sessions_monitor.semantics,
            &sample.sessions_repository.semantics,
        ),
        SemanticComparison::compare(
            &sample.analytics_repository.semantics,
            &sample.analytics_repository_warm.semantics,
        ),
    ];
    if comparisons.iter().any(|comparison| !comparison.passed()) {
        return Err(SampleFailure::new(
            "semantic-parity-mismatch",
            anyhow::anyhow!("supplemental monitor/repository semantic parity failed"),
        ));
    }
    Ok(())
}

async fn run_warmup(config: &BenchmarkConfig, client: &Client, iteration: usize) -> Result<()> {
    let analytics_repository = config.fresh_repository();
    let sessions_repository = config.fresh_repository();
    if iteration.is_multiple_of(2) {
        sample_monitor::<MonitorAnalyticsResponse>(
            client,
            &config.monitor_analytics_url,
            "monitor analytics warmup",
        )
        .await?;
        sample_direct_analytics(&analytics_repository).await?;
        sample_monitor::<MonitorSessionsResponse>(
            client,
            &config.monitor_sessions_url,
            "monitor sessions warmup",
        )
        .await?;
        sample_direct_sessions(&sessions_repository).await?;
    } else {
        sample_direct_analytics(&analytics_repository).await?;
        sample_monitor::<MonitorAnalyticsResponse>(
            client,
            &config.monitor_analytics_url,
            "monitor analytics warmup",
        )
        .await?;
        sample_direct_sessions(&sessions_repository).await?;
        sample_monitor::<MonitorSessionsResponse>(
            client,
            &config.monitor_sessions_url,
            "monitor sessions warmup",
        )
        .await?;
    }
    Ok(())
}

async fn run_matrix(
    config: &BenchmarkConfig,
    client: &Client,
    manifest: &DatasetManifest,
    warmups: usize,
    samples: usize,
) -> Result<MatrixReports> {
    for iteration in 0..warmups {
        if let Err(error) = run_warmup(config, client, iteration).await {
            eprintln!(
                "analytics_latency warmup {} failed: {error:#}",
                iteration + 1
            );
            return MatrixReports::setup_failure(samples, "warmup-error");
        }
    }

    let warm_repository = config.fresh_repository();
    if let Err(error) = sample_direct_analytics(&warm_repository).await {
        eprintln!("analytics_latency warm-cache priming failed: {error:#}");
        return MatrixReports::setup_failure(samples, "warm-cache-priming-error");
    }
    let mut counts = AttemptCounts::new(samples)?;
    let mut analytics_monitor = Measurements::default();
    let mut analytics_repository = Measurements::default();
    let mut sessions_monitor = Measurements::default();
    let mut sessions_repository = Measurements::default();
    let mut analytics_repository_warm = Measurements::default();
    let mut diagnostics = Vec::new();
    let mut failure_details = Vec::new();

    for iteration in 0..samples {
        let outcome = run_sample(config, client, &warm_repository, iteration)
            .await
            .and_then(|sample| {
                validate_sample(
                    &sample,
                    manifest,
                    &analytics_monitor,
                    &analytics_repository,
                    &sessions_monitor,
                    &sessions_repository,
                    &analytics_repository_warm,
                )?;
                Ok(sample)
            });
        match outcome {
            Ok(sample) => {
                analytics_monitor.record(sample.analytics_monitor, "monitor analytics")?;
                analytics_repository.record(sample.analytics_repository, "repository analytics")?;
                sessions_monitor.record(sample.sessions_monitor, "monitor sessions")?;
                sessions_repository.record(sample.sessions_repository, "repository sessions")?;
                analytics_repository_warm.record(
                    sample.analytics_repository_warm,
                    "warm repository analytics",
                )?;
                counts.record_success()?;
            }
            Err(failure) => {
                eprintln!(
                    "analytics_latency sample {} {}: {:#}",
                    iteration + 1,
                    failure.code,
                    failure.error
                );
                counts.record_error()?;
                diagnostics.push(json!({"code": failure.code}));
                failure_details.push(json!({
                    "attempt": iteration + 1,
                    "code": failure.code,
                }));
            }
        }
    }

    Ok(MatrixReports {
        counts,
        analytics_monitor: analytics_monitor.report(),
        analytics_repository: analytics_repository.report(),
        sessions_monitor: sessions_monitor.report(),
        sessions_repository: sessions_repository.report(),
        analytics_repository_warm: analytics_repository_warm.report(),
        diagnostics,
        failure_details,
        setup_failed: false,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn detail_path(output: &Path) -> std::path::PathBuf {
    let name = output
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("analytics-latency");
    output.with_file_name(format!("{name}.diagnostics.json"))
}

async fn execute() -> Result<()> {
    let cli = Cli::parse_from(env::args())?;
    let source = Source::collect()?;
    let config = BenchmarkConfig::from_env()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to construct monitor HTTP client")?;
    let dataset = load_dataset_identity(&config).await?;
    let (mut reports, dataset_manifest_verified) =
        match DatasetManifest::load(&config.dataset_manifest).and_then(|manifest| {
            dataset.verify_manifest(&manifest)?;
            Ok(manifest)
        }) {
            Ok(manifest) => (
                run_matrix(
                    &config,
                    &client,
                    &manifest,
                    cli.profile.warmups(),
                    cli.profile.samples(),
                )
                .await?,
                true,
            ),
            Err(error) => {
                eprintln!("analytics_latency dataset verification failed: {error:#}");
                (
                    MatrixReports::setup_failure(
                        cli.profile.samples(),
                        "dataset-manifest-verification-error",
                    )?,
                    false,
                )
            }
        };
    if dataset_manifest_verified && !reports.setup_failed {
        let failure_code = match load_dataset_identity(&config).await {
            Ok(observed) if observed != dataset => {
                eprintln!("analytics_latency dataset changed during benchmark execution");
                Some("dataset-changed-during-run")
            }
            Ok(_) => None,
            Err(error) => {
                eprintln!("analytics_latency post-run dataset verification failed: {error:#}");
                Some("dataset-post-verification-error")
            }
        };
        if let Some(code) = failure_code {
            reports.setup_failed = true;
            reports.diagnostics.push(json!({"code": code}));
            reports.failure_details.push(json!({"code": code}));
        }
    }

    let analytics_comparison = reports
        .analytics_monitor
        .semantics
        .as_ref()
        .zip(reports.analytics_repository.semantics.as_ref())
        .map(|(monitor, repository)| SemanticComparison::compare(monitor, repository));
    let sessions_comparison = reports
        .sessions_monitor
        .semantics
        .as_ref()
        .zip(reports.sessions_repository.semantics.as_ref())
        .map(|(monitor, repository)| SemanticComparison::compare(monitor, repository));
    let warm_comparison = reports
        .analytics_repository
        .semantics
        .as_ref()
        .zip(reports.analytics_repository_warm.semantics.as_ref())
        .map(|(cold, warm)| SemanticComparison::compare(cold, warm));
    let analytics_timing = reports
        .analytics_monitor
        .p95_ms
        .zip(reports.analytics_repository.p95_ms)
        .map(|(monitor, repository)| support::timing_comparison(monitor, repository))
        .transpose()?;
    let sessions_timing = reports
        .sessions_monitor
        .p95_ms
        .zip(reports.sessions_repository.p95_ms)
        .map(|(monitor, repository)| support::timing_comparison(monitor, repository))
        .transpose()?;
    let semantic_passed = reports.semantic_passed();
    let passed_checks = reports
        .counts
        .successful()
        .saturating_mul(SEMANTIC_CHECKS_PER_SAMPLE);
    let failed_checks = reports.counts.errors() + usize::from(reports.setup_failed);

    let details = json!({
        "profile": cli.profile,
        "warmups": cli.profile.warmups(),
        "paired_iterations": cli.profile.samples(),
        "counts": {
            "planned": reports.counts.planned(),
            "attempted": reports.counts.attempted(),
            "successful": reports.counts.successful(),
            "errors": reports.counts.errors(),
        },
        "dataset": dataset,
        "failures": reports.failure_details,
        "analytics": {
            "supplemental_parity": analytics_comparison,
            "monitor": {
                "p50_ms": reports.analytics_monitor.p50_ms,
                "p95_ms": reports.analytics_monitor.p95_ms,
            },
            "repository_cold": {
                "p50_ms": reports.analytics_repository.p50_ms,
                "p95_ms": reports.analytics_repository.p95_ms,
            },
            "diagnostic_p95_comparison": analytics_timing,
        },
        "sessions": {
            "supplemental_parity": sessions_comparison,
            "monitor": {
                "p50_ms": reports.sessions_monitor.p50_ms,
                "p95_ms": reports.sessions_monitor.p95_ms,
            },
            "repository_cold": {
                "p50_ms": reports.sessions_repository.p50_ms,
                "p95_ms": reports.sessions_repository.p95_ms,
            },
            "diagnostic_p95_comparison": sessions_timing,
        },
        "warm_repository_analytics": {
            "supplemental_parity": warm_comparison,
            "p50_ms": reports.analytics_repository_warm.p50_ms,
            "p95_ms": reports.analytics_repository_warm.p95_ms,
        },
        "timing_status": "not_evaluated",
    });
    let details_path = detail_path(&cli.output);
    support::write_json_atomic(&details_path, &details)?;
    let detail_bytes = fs::read(&details_path).context("failed to read written diagnostics")?;
    let artifact_file = details_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("diagnostics file name is not UTF-8")?;

    let mut measurements = BTreeMap::new();
    measurements.insert(
        "analytics_monitor_http_latency_ms".to_string(),
        reports.analytics_monitor.latency_ms,
    );
    measurements.insert(
        "analytics_repository_cold_latency_ms".to_string(),
        reports.analytics_repository.latency_ms,
    );
    measurements.insert(
        "sessions_monitor_http_latency_ms".to_string(),
        reports.sessions_monitor.latency_ms,
    );
    measurements.insert(
        "sessions_repository_cold_latency_ms".to_string(),
        reports.sessions_repository.latency_ms,
    );
    measurements.insert(
        "analytics_repository_warm_latency_ms".to_string(),
        reports.analytics_repository_warm.latency_ms,
    );
    let mut diagnostics = reports.diagnostics;
    diagnostics.push(json!({
        "code": if semantic_passed { "semantic-validation-pass" } else { "semantic-validation-fail" },
    }));
    if reports.counts.successful() < reports.counts.planned() {
        diagnostics.push(json!({"code": "insufficient-samples"}));
    }
    diagnostics.push(json!({"code": "timing-not-evaluated"}));
    diagnostics
        .push(json!({"code": "comparison-details", "artifact_id": "analytics-comparison-details"}));
    let report = support::build_protocol_report(ProtocolReportInput {
        profile: cli.profile,
        source,
        planned: reports.counts.planned(),
        attempted: reports.counts.attempted(),
        successful: reports.counts.successful(),
        errors: reports.counts.errors(),
        measurements,
        semantic_passed,
        passed_checks,
        failed_checks,
        dataset_fingerprint: dataset.fingerprint,
        dataset_cardinality: dataset.cardinality,
        diagnostics,
        artifacts: vec![json!({
            "id": "analytics-comparison-details",
            "kind": "benchmark-diagnostics",
            "path": artifact_file,
            "sha256": sha256_hex(&detail_bytes),
        })],
    })?;
    support::write_json_atomic(&cli.output, &report)?;
    println!("{}", cli.output.display());
    if !semantic_passed {
        bail!("benchmark semantic validation failed; timing remains non-blocking");
    }
    Ok(())
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to construct benchmark runtime");
    if let Err(error) = runtime.block_on(execute()) {
        eprintln!("analytics_latency failed: {error:#}");
        std::process::exit(1);
    }
}
