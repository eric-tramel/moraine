use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::ClickHouseConfig;
use moraine_conversations::{
    AnalyticsRange, ClickHouseConversationRepository, ConversationRepository, RepoConfig,
    SessionAnalyticsQuery, SessionLookback,
};
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_SAMPLES: usize = 60;
const DEFAULT_WARMUPS: usize = 5;
const MAX_P95_REGRESSION_RATIO: f64 = 0.20;

#[derive(Debug)]
struct BenchmarkConfig {
    monitor_url: String,
    clickhouse_url: String,
    clickhouse_database: String,
    clickhouse_username: String,
    clickhouse_password: String,
    samples: usize,
    warmups: usize,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self> {
        let defaults = ClickHouseConfig::default();
        let monitor_url = required_env("MORAINE_BENCH_MONITOR_URL")?;
        let clickhouse_url = required_env("MORAINE_BENCH_CLICKHOUSE_URL")?;
        let clickhouse_database =
            optional_env("MORAINE_BENCH_CLICKHOUSE_DATABASE", defaults.database)?;
        let clickhouse_username = optional_env("MORAINE_BENCH_CLICKHOUSE_USER", defaults.username)?;
        let clickhouse_password =
            optional_env("MORAINE_BENCH_CLICKHOUSE_PASSWORD", defaults.password)?;
        let samples = usize_env("MORAINE_BENCH_SAMPLES", DEFAULT_SAMPLES)?;
        let warmups = usize_env("MORAINE_BENCH_WARMUPS", DEFAULT_WARMUPS)?;

        if samples == 0 {
            bail!("MORAINE_BENCH_SAMPLES must be at least 1");
        }
        if clickhouse_database.is_empty() {
            bail!("MORAINE_BENCH_CLICKHOUSE_DATABASE must not be empty");
        }

        Url::parse(&clickhouse_url).with_context(|| {
            format!("MORAINE_BENCH_CLICKHOUSE_URL is not a valid URL: {clickhouse_url}")
        })?;

        Ok(Self {
            monitor_url,
            clickhouse_url,
            clickhouse_database,
            clickhouse_username,
            clickhouse_password,
            samples,
            warmups,
        })
    }

    fn clickhouse_config(&self) -> ClickHouseConfig {
        ClickHouseConfig {
            url: self.clickhouse_url.clone(),
            database: self.clickhouse_database.clone(),
            username: self.clickhouse_username.clone(),
            password: self.clickhouse_password.clone(),
            ..ClickHouseConfig::default()
        }
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

fn optional_env(name: &str, default: String) -> Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must contain valid UTF-8"),
    }
}

fn usize_env(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("{name} must be a non-negative integer, got {value:?}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must contain valid UTF-8"),
    }
}

struct BenchmarkContext {
    http: Client,
    monitor_analytics_url: Url,
    monitor_sessions_url: Url,
    clickhouse: ClickHouseClient,
}

impl BenchmarkContext {
    fn new(config: &BenchmarkConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("failed to construct monitor HTTP client")?;
        let monitor_analytics_url =
            monitor_endpoint(&config.monitor_url, "/api/v1/analytics?range=24h")?;
        let monitor_sessions_url =
            monitor_endpoint(&config.monitor_url, "/api/v1/sessions?since=30d&limit=50")?;
        let clickhouse = ClickHouseClient::new(config.clickhouse_config())
            .context("failed to construct ClickHouse client")?;

        Ok(Self {
            http,
            monitor_analytics_url,
            monitor_sessions_url,
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

fn monitor_endpoint(base: &str, path_and_query: &str) -> Result<Url> {
    let endpoint = format!("{}{}", base.trim_end_matches('/'), path_and_query);
    Url::parse(&endpoint).with_context(|| format!("invalid monitor endpoint URL: {endpoint}"))
}

fn report_origin(raw: &str, label: &str) -> Result<String> {
    let url = Url::parse(raw).with_context(|| format!("{label} is not a valid URL"))?;
    let origin = url.origin().ascii_serialization();
    if origin == "null" {
        bail!("{label} must have an HTTP(S) origin");
    }
    Ok(format!("{origin}/"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Arm {
    MonitorHttp,
    DirectRepository,
}

impl Arm {
    const fn other(self) -> Self {
        match self {
            Self::MonitorHttp => Self::DirectRepository,
            Self::DirectRepository => Self::MonitorHttp,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    Analytics24h,
    Sessions30d50,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::Analytics24h => "analytics_24h",
            Self::Sessions30d50 => "sessions_30d_limit_50",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Cardinality {
    AnalyticsSeries {
        tokens: usize,
        turns: usize,
        concurrent_sessions: usize,
    },
    Sessions {
        sessions: usize,
    },
}

fn normalized_digest(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(&canonicalize_json(value))
        .context("failed to serialize normalized benchmark payload")?;
    let hash = bytes
        .into_iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            hash.wrapping_mul(0x100000001b3) ^ u64::from(byte)
        });
    Ok(format!("fnv1a64:{hash:016x}"))
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => {
            let mut values = values.iter().map(canonicalize_json).collect::<Vec<_>>();
            values.sort_by_cached_key(|value| serde_json::to_string(value).unwrap_or_default());
            Value::Array(values)
        }
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            let mut normalized = Map::new();
            for (key, value) in entries {
                normalized.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(normalized)
        }
        scalar => scalar.clone(),
    }
}

fn monitor_session_projection(session: &Value) -> Result<Value> {
    let session_id = session
        .get("id")
        .and_then(Value::as_str)
        .context("monitor session missing string id")?;
    let harness = session
        .pointer("/harness/id")
        .and_then(Value::as_str)
        .context("monitor session missing harness.id")?;
    let started_at = session
        .get("startedAt")
        .and_then(Value::as_i64)
        .context("monitor session missing startedAt")?;
    let ended_at = session
        .get("endedAt")
        .and_then(Value::as_i64)
        .context("monitor session missing endedAt")?;
    let models = session
        .get("models")
        .and_then(Value::as_array)
        .context("monitor session missing models")?;
    let trace_id = session
        .get("traceId")
        .and_then(Value::as_str)
        .context("monitor session missing traceId")?;
    let turns = session
        .get("turns")
        .and_then(Value::as_array)
        .context("monitor session missing turns")?;
    let tool_calls = session
        .get("totalToolCalls")
        .and_then(Value::as_u64)
        .context("monitor session missing totalToolCalls")?;

    Ok(json!({
        "session_id": session_id,
        "harness": harness,
        "started_at_unix_ms": started_at,
        "ended_at_unix_ms": ended_at,
        "models": models,
        "trace_id": trace_id,
        "turns": turns.len(),
        "tool_calls": tool_calls,
    }))
}

#[derive(Deserialize)]
struct MonitorAnalyticsResponse {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    series: Option<MonitorAnalyticsSeries>,
}

#[derive(Deserialize)]
struct MonitorAnalyticsSeries {
    tokens: Vec<Value>,
    turns: Vec<Value>,
    concurrent_sessions: Vec<Value>,
}

#[derive(Deserialize)]
struct MonitorSessionsResponse {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    sessions: Option<Vec<Value>>,
}

trait MonitorResponse: DeserializeOwned {
    fn into_semantics(self) -> Result<(Cardinality, String)>;
}

impl MonitorResponse for MonitorAnalyticsResponse {
    fn into_semantics(self) -> Result<(Cardinality, String)> {
        if !self.ok {
            bail!(
                "monitor analytics response had ok=false: {}",
                self.error.as_deref().unwrap_or("missing error message")
            );
        }
        let series = self
            .series
            .context("monitor analytics response had ok=true but no series")?;
        let cardinality = Cardinality::AnalyticsSeries {
            tokens: series.tokens.len(),
            turns: series.turns.len(),
            concurrent_sessions: series.concurrent_sessions.len(),
        };
        let payload = json!({
            "tokens": series.tokens,
            "turns": series.turns,
            "concurrent_sessions": series.concurrent_sessions,
        });
        Ok((cardinality, normalized_digest(&payload)?))
    }
}

impl MonitorResponse for MonitorSessionsResponse {
    fn into_semantics(self) -> Result<(Cardinality, String)> {
        if !self.ok {
            bail!(
                "monitor sessions response had ok=false: {}",
                self.error.as_deref().unwrap_or("missing error message")
            );
        }
        let sessions = self
            .sessions
            .context("monitor sessions response had ok=true but no sessions")?;
        let cardinality = Cardinality::Sessions {
            sessions: sessions.len(),
        };
        let payload = Value::Array(
            sessions
                .iter()
                .map(monitor_session_projection)
                .collect::<Result<Vec<_>>>()?,
        );
        Ok((cardinality, normalized_digest(&payload)?))
    }
}

struct TimedSample {
    elapsed: Duration,
    cardinality: Cardinality,
    normalized_digest: String,
}

async fn sample_monitor<T>(client: &Client, url: &Url, label: &str) -> Result<TimedSample>
where
    T: MonitorResponse,
{
    let started = Instant::now();
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("{label} request failed: {url}"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("{label} response body failed: {url}"))?;
    let payload = serde_json::from_slice::<T>(&body).with_context(|| {
        format!(
            "{label} returned invalid JSON from {url}: {}",
            String::from_utf8_lossy(&body)
        )
    })?;
    if !status.is_success() {
        bail!(
            "{label} returned HTTP {status} from {url}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let (cardinality, normalized_digest) = payload.into_semantics()?;

    Ok(TimedSample {
        elapsed: started.elapsed(),
        cardinality,
        normalized_digest,
    })
}

async fn sample_direct_analytics(
    repository: &ClickHouseConversationRepository,
) -> Result<TimedSample> {
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
    let cardinality = Cardinality::AnalyticsSeries {
        tokens: payload["tokens"].as_array().map_or(0, Vec::len),
        turns: payload["turns"].as_array().map_or(0, Vec::len),
        concurrent_sessions: payload["concurrent_sessions"]
            .as_array()
            .map_or(0, Vec::len),
    };
    Ok(TimedSample {
        elapsed: started.elapsed(),
        cardinality,
        normalized_digest: normalized_digest(&payload)?,
    })
}

async fn sample_direct_sessions(
    repository: &ClickHouseConversationRepository,
) -> Result<TimedSample> {
    let started = Instant::now();
    let sessions = repository
        .list_session_analytics(SessionAnalyticsQuery {
            lookback: SessionLookback::ThirtyDays,
            limit: 50,
        })
        .await
        .context("direct sessions repository request failed")?;

    let cardinality = Cardinality::Sessions {
        sessions: sessions.len(),
    };
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
    Ok(TimedSample {
        elapsed: started.elapsed(),
        cardinality,
        normalized_digest: normalized_digest(&payload)?,
    })
}

async fn take_sample(
    context: &BenchmarkContext,
    scenario: Scenario,
    arm: Arm,
) -> Result<TimedSample> {
    match (scenario, arm) {
        (Scenario::Analytics24h, Arm::MonitorHttp) => {
            sample_monitor::<MonitorAnalyticsResponse>(
                &context.http,
                &context.monitor_analytics_url,
                "monitor analytics",
            )
            .await
        }
        (Scenario::Sessions30d50, Arm::MonitorHttp) => {
            sample_monitor::<MonitorSessionsResponse>(
                &context.http,
                &context.monitor_sessions_url,
                "monitor sessions",
            )
            .await
        }
        (Scenario::Analytics24h, Arm::DirectRepository) => {
            let repository = context.fresh_repository();
            sample_direct_analytics(&repository).await
        }
        (Scenario::Sessions30d50, Arm::DirectRepository) => {
            let repository = context.fresh_repository();
            sample_direct_sessions(&repository).await
        }
    }
}

#[derive(Default)]
struct Measurements {
    elapsed: Vec<Duration>,
    cardinality: Option<Cardinality>,
    normalized_digest: Option<String>,
}

impl Measurements {
    fn record(&mut self, sample: TimedSample, scenario: Scenario, arm: Arm) -> Result<()> {
        let TimedSample {
            elapsed,
            cardinality,
            normalized_digest,
        } = sample;
        if let Some(expected) = &self.cardinality {
            if expected != &cardinality {
                bail!(
                    "{} {:?} cardinality changed after {} successful samples: expected {:?}, got {:?}",
                    scenario.name(),
                    arm,
                    self.elapsed.len(),
                    expected,
                    cardinality
                );
            }
        } else {
            self.cardinality = Some(cardinality);
        }
        if let Some(expected) = &self.normalized_digest {
            if expected != &normalized_digest {
                bail!(
                    "{} {:?} normalized digest changed after {} successful samples: expected {}, got {}",
                    scenario.name(),
                    arm,
                    self.elapsed.len(),
                    expected,
                    normalized_digest
                );
            }
        } else {
            self.normalized_digest = Some(normalized_digest);
        }
        self.elapsed.push(elapsed);
        Ok(())
    }

    fn report(&self, expected_samples: usize) -> Result<ArmReport> {
        if self.elapsed.len() != expected_samples {
            bail!(
                "expected exactly {expected_samples} successful samples, got {}",
                self.elapsed.len()
            );
        }
        let elapsed_ms: Vec<f64> = self
            .elapsed
            .iter()
            .map(|duration| duration.as_secs_f64() * 1_000.0)
            .collect();
        let p50_ms = type7_percentile(&elapsed_ms, 0.50)
            .ok_or_else(|| anyhow!("unable to compute p50 from successful samples"))?;
        let p95_ms = type7_percentile(&elapsed_ms, 0.95)
            .ok_or_else(|| anyhow!("unable to compute p95 from successful samples"))?;
        let cardinality = self
            .cardinality
            .clone()
            .context("successful samples had no cardinality")?;
        let normalized_digest = self
            .normalized_digest
            .clone()
            .context("successful samples had no normalized digest")?;

        Ok(ArmReport {
            successful_samples: self.elapsed.len(),
            cardinality,
            normalized_digest,
            p50_ms,
            p95_ms,
        })
    }
}

#[derive(Serialize)]
struct ArmReport {
    successful_samples: usize,
    cardinality: Cardinality,
    normalized_digest: String,
    p50_ms: f64,
    p95_ms: f64,
}

#[derive(Serialize)]
struct CardinalityDifference {
    monitor_http: Cardinality,
    direct_repository: Cardinality,
}

#[derive(Serialize)]
struct DigestDifference {
    monitor_http: String,
    direct_repository: String,
}

#[derive(Serialize)]
struct P95Comparison {
    direct_minus_monitor_ms: f64,
    absolute_delta_ms: f64,
    direct_minus_monitor_percent: f64,
    absolute_delta_percent: f64,
    direct_to_monitor_ratio: f64,
    allowed_max_regression_percent: f64,
    passed: bool,
}

#[derive(Serialize)]
struct ScenarioReport {
    scenario: &'static str,
    initial_order: [Arm; 2],
    monitor_http: ArmReport,
    direct_repository_cold: ArmReport,
    cardinality_match: bool,
    normalized_digest_match: bool,
    semantic_cardinality_difference: Option<CardinalityDifference>,
    normalized_digest_difference: Option<DigestDifference>,
    p95_comparison: P95Comparison,
    passed: bool,
}

async fn run_scenario(
    context: &BenchmarkContext,
    scenario: Scenario,
    warmups: usize,
    samples: usize,
    initial_arm: Arm,
) -> Result<ScenarioReport> {
    for iteration in 0..warmups {
        let first = if iteration % 2 == 0 {
            initial_arm
        } else {
            initial_arm.other()
        };
        take_sample(context, scenario, first)
            .await
            .with_context(|| {
                format!(
                    "{} warmup {} {:?} failed",
                    scenario.name(),
                    iteration + 1,
                    first
                )
            })?;
        take_sample(context, scenario, first.other())
            .await
            .with_context(|| {
                format!(
                    "{} warmup {} {:?} failed",
                    scenario.name(),
                    iteration + 1,
                    first.other()
                )
            })?;
    }

    let mut monitor = Measurements::default();
    let mut direct = Measurements::default();
    for iteration in 0..samples {
        let first = if iteration % 2 == 0 {
            initial_arm
        } else {
            initial_arm.other()
        };
        for arm in [first, first.other()] {
            let sample = take_sample(context, scenario, arm).await.with_context(|| {
                format!(
                    "{} measured sample {} {:?} failed",
                    scenario.name(),
                    iteration + 1,
                    arm
                )
            })?;
            match arm {
                Arm::MonitorHttp => monitor.record(sample, scenario, arm)?,
                Arm::DirectRepository => direct.record(sample, scenario, arm)?,
            }
        }
    }

    let monitor_http = monitor.report(samples)?;
    let direct_repository_cold = direct.report(samples)?;
    let cardinality_match = monitor_http.cardinality == direct_repository_cold.cardinality;
    let semantic_cardinality_difference = (!cardinality_match).then(|| CardinalityDifference {
        monitor_http: monitor_http.cardinality.clone(),
        direct_repository: direct_repository_cold.cardinality.clone(),
    });
    let normalized_digest_match =
        monitor_http.normalized_digest == direct_repository_cold.normalized_digest;
    let normalized_digest_difference = (!normalized_digest_match).then(|| DigestDifference {
        monitor_http: monitor_http.normalized_digest.clone(),
        direct_repository: direct_repository_cold.normalized_digest.clone(),
    });
    let ratio = ratio_assessment(
        monitor_http.p95_ms,
        direct_repository_cold.p95_ms,
        MAX_P95_REGRESSION_RATIO,
    )
    .ok_or_else(|| {
        anyhow!(
            "{} p95 ratio rejected zero or invalid monitor baseline: {} ms",
            scenario.name(),
            monitor_http.p95_ms
        )
    })?;
    let direct_minus_monitor_ms = direct_repository_cold.p95_ms - monitor_http.p95_ms;
    let p95_comparison = P95Comparison {
        direct_minus_monitor_ms,
        absolute_delta_ms: direct_minus_monitor_ms.abs(),
        direct_minus_monitor_percent: ratio.signed_delta_ratio * 100.0,
        absolute_delta_percent: ratio.absolute_delta_ratio * 100.0,
        direct_to_monitor_ratio: ratio.ratio,
        allowed_max_regression_percent: MAX_P95_REGRESSION_RATIO * 100.0,
        passed: ratio.passed,
    };

    Ok(ScenarioReport {
        scenario: scenario.name(),
        initial_order: [initial_arm, initial_arm.other()],
        monitor_http,
        direct_repository_cold,
        cardinality_match,
        semantic_cardinality_difference,
        normalized_digest_match,
        normalized_digest_difference,
        passed: cardinality_match && normalized_digest_match && p95_comparison.passed,
        p95_comparison,
    })
}

#[derive(Serialize)]
struct WarmAnalyticsReport {
    successful_samples: usize,
    cardinality: Cardinality,
    normalized_digest: String,
    p95_ms: f64,
}

async fn run_warm_analytics_cache(
    context: &BenchmarkContext,
    warmups: usize,
    samples: usize,
) -> Result<WarmAnalyticsReport> {
    let repository = context.fresh_repository();
    let priming_calls = warmups.max(1);
    for warmup in 0..priming_calls {
        sample_direct_analytics(&repository)
            .await
            .with_context(|| format!("warm analytics cache priming call {} failed", warmup + 1))?;
    }

    let mut measurements = Measurements::default();
    for sample_number in 0..samples {
        let sample = sample_direct_analytics(&repository)
            .await
            .with_context(|| {
                format!(
                    "warm analytics cache measured sample {} failed",
                    sample_number + 1
                )
            })?;
        measurements.record(sample, Scenario::Analytics24h, Arm::DirectRepository)?;
    }
    let report = measurements.report(samples)?;

    Ok(WarmAnalyticsReport {
        successful_samples: report.successful_samples,
        cardinality: report.cardinality,
        normalized_digest: report.normalized_digest,
        p95_ms: report.p95_ms,
    })
}

#[derive(Serialize)]
struct BenchmarkReport {
    benchmark: &'static str,
    monitor_url: String,
    clickhouse_url: String,
    clickhouse_database: String,
    warmups: usize,
    samples_per_arm: usize,
    max_p95_regression_percent: f64,
    scenarios: Vec<ScenarioReport>,
    warm_analytics_cache: WarmAnalyticsReport,
    passed: bool,
}

#[tokio::test]
#[ignore = "requires live monitor and ClickHouse sandbox URLs"]
async fn live_monitor_vs_repository_latency() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let context = BenchmarkContext::new(&config)?;

    let analytics = run_scenario(
        &context,
        Scenario::Analytics24h,
        config.warmups,
        config.samples,
        Arm::MonitorHttp,
    )
    .await?;
    let sessions = run_scenario(
        &context,
        Scenario::Sessions30d50,
        config.warmups,
        config.samples,
        Arm::DirectRepository,
    )
    .await?;
    let warm_analytics_cache =
        run_warm_analytics_cache(&context, config.warmups, config.samples).await?;
    let passed = analytics.passed && sessions.passed;
    let report = BenchmarkReport {
        benchmark: "monitor_http_vs_direct_clickhouse_conversation_repository",
        monitor_url: report_origin(&config.monitor_url, "MORAINE_BENCH_MONITOR_URL")?,
        clickhouse_url: report_origin(&config.clickhouse_url, "MORAINE_BENCH_CLICKHOUSE_URL")?,
        clickhouse_database: config.clickhouse_database,
        warmups: config.warmups,
        samples_per_arm: config.samples,
        max_p95_regression_percent: MAX_P95_REGRESSION_RATIO * 100.0,
        scenarios: vec![analytics, sessions],
        warm_analytics_cache,
        passed,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("failed to serialize benchmark report")?
    );

    if !report.passed {
        bail!("one or more scenarios failed semantic parity or the 20% p95 regression ceiling");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "creates and tears down a temporary database on a live ClickHouse sandbox"]
async fn live_schema_semantics_and_teardown() -> Result<()> {
    let config = BenchmarkConfig::from_env()?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_millis();
    let database = format!("moraine_issue454_schema_{}_{}", std::process::id(), suffix);
    let mut clickhouse_config = config.clickhouse_config();
    clickhouse_config.database = database.clone();
    let clickhouse = ClickHouseClient::new(clickhouse_config)
        .context("failed to construct live-schema ClickHouse client")?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate temporary live-schema database")?;
        let repository = ClickHouseConversationRepository::new(
            clickhouse.clone(),
            RepoConfig::default(),
        );

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
                    "ALTER TABLE `{database}`.`ingest_heartbeats` DROP COLUMN IF EXISTS backend_sinks"
                ),
                None,
                Some(&database),
                false,
                None,
            )
            .await
            .context("failed to remove optional heartbeat column")?;
        let legacy_repository = ClickHouseConversationRepository::new(
            clickhouse.clone(),
            RepoConfig::default(),
        );
        let legacy_heartbeat = legacy_repository
            .latest_ingest_heartbeat()
            .await
            .context("legacy heartbeat read failed")?;
        assert!(legacy_heartbeat.table_present);
        assert!(legacy_heartbeat.latest.is_none());

        let insert = format!(
            r#"INSERT INTO `{database}`.`events`
(
  ingested_at, event_uid, session_id, session_date, source_name, harness, source_file,
  source_ref, event_ts, event_kind, actor_kind, payload_type, op_kind, request_id,
  trace_id, turn_index, model, input_tokens, output_tokens, text_content, payload_json,
  event_version
)
VALUES
(
  addMonths(now64(3), -1), 'issue454-dedup', 'issue454-dedup-session', today(),
  'fixture', 'codex', 'fixture-dedup', 'fixture-old', now64(3), 'message',
  'assistant', 'text', '', 'issue454-request', 'issue454-trace', 1, 'old-model',
  11, 0, 'old', '{{}}', 1
),
(
  now64(3), 'issue454-dedup', 'issue454-dedup-session', today(), 'fixture', 'codex',
  'fixture-dedup', 'fixture-new', now64(3), 'message', 'assistant', 'text', '',
  'issue454-request', 'issue454-trace', 1, 'new-model', 13, 0, 'new', '{{}}', 2
),
(
  now64(3), 'issue454-web-a', 'issue454-web-session', today(), 'fixture', 'codex',
  'fixture-web', 'issue454-web-a', now64(3), 'tool_call', 'assistant',
  'web_search_call', 'search', '', 'issue454-trace', 1, '', 0, 0, '',
  '{{"action":{{"query":"a"}}}}', 1
),
(
  now64(3), 'issue454-web-z', 'issue454-web-session', today(), 'fixture', 'codex',
  'fixture-web', 'issue454-web-z', now64(3), 'tool_call', 'assistant',
  'web_search_call', 'search', '', 'issue454-trace', 1, '', 0, 0, '',
  '{{"action":{{"query":"z"}}}}', 1
)"#
        );
        clickhouse
            .request_text(&insert, None, Some(&database), false, None)
            .await
            .context("failed to insert live-schema fixtures")?;

        #[derive(Deserialize)]
        struct ModelRow {
            model: String,
        }
        let canonical: Vec<ModelRow> = clickhouse
            .query_rows(
                &format!(
                    "SELECT model FROM (SELECT * FROM `{database}`.`events` FINAL \
                     SETTINGS do_not_merge_across_partitions_select_final = 0) \
                     WHERE event_uid = 'issue454-dedup' FORMAT JSONEachRow"
                ),
                Some(&database),
            )
            .await
            .context("canonical cross-partition fixture query failed")?;
        assert_eq!(canonical.len(), 1);
        assert_eq!(canonical[0].model, "new-model");
        let stale_predicate: Vec<ModelRow> = clickhouse
            .query_rows(
                &format!(
                    "SELECT model FROM (SELECT * FROM `{database}`.`events` FINAL \
                     SETTINGS do_not_merge_across_partitions_select_final = 0) \
                     WHERE event_uid = 'issue454-dedup' AND model = 'old-model' \
                     FORMAT JSONEachRow"
                ),
                Some(&database),
            )
            .await
            .context("canonical outer-predicate query failed")?;
        assert!(stale_predicate.is_empty());

        let populated_repository = ClickHouseConversationRepository::new(
            clickhouse.clone(),
            RepoConfig::default(),
        );
        let analytics = populated_repository
            .analytics_series(AnalyticsRange::FifteenMinutes)
            .await
            .context("populated analytics read failed")?;
        assert!(analytics.tokens.iter().any(|point| point.model == "new-model"));
        assert!(!analytics.tokens.iter().any(|point| point.model == "old-model"));
        let sessions = populated_repository
            .list_session_analytics(SessionAnalyticsQuery {
                lookback: SessionLookback::All,
                limit: 50,
            })
            .await
            .context("populated session analytics read failed")?;
        let deduped = sessions
            .iter()
            .find(|session| session.summary.session_id == "issue454-dedup-session")
            .context("deduped fixture session missing")?;
        assert_eq!(deduped.summary.total_events, 1);
        assert_eq!(deduped.models, vec!["new-model"]);
        let turn = populated_repository
            .get_turn("issue454-dedup-session", 1)
            .await
            .context("view-only turn read failed")?
            .context("view-only fixture turn missing")?;
        assert_eq!(turn.summary.total_events, 1);
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

        Ok::<_, anyhow::Error>(())
    }
    .await;

    let cleanup = clickhouse
        .request_text(
            &format!("DROP DATABASE IF EXISTS `{database}` SYNC"),
            None,
            Some("system"),
            false,
            None,
        )
        .await;
    match (outcome, cleanup) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) => Err(error),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error.context("live-schema teardown failed")),
        (Err(error), Err(cleanup_error)) => Err(anyhow!(
            "{error:#}; live-schema teardown also failed: {cleanup_error:#}"
        )),
    }
}

fn type7_percentile(samples: &[f64], probability: f64) -> Option<f64> {
    if samples.is_empty()
        || !probability.is_finite()
        || !(0.0..=1.0).contains(&probability)
        || samples.iter().any(|sample| !sample.is_finite())
    {
        return None;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.len() == 1 {
        return sorted.first().copied();
    }

    let index = (sorted.len() - 1) as f64 * probability;
    let lower = index.floor() as usize;
    let fraction = index - lower as f64;
    let upper = (lower + 1).min(sorted.len() - 1);
    Some(sorted[lower] + fraction * (sorted[upper] - sorted[lower]))
}

#[derive(Debug, Clone, Copy)]
struct RatioAssessment {
    ratio: f64,
    signed_delta_ratio: f64,
    absolute_delta_ratio: f64,
    passed: bool,
}

fn ratio_assessment(
    baseline: f64,
    candidate: f64,
    max_regression_ratio: f64,
) -> Option<RatioAssessment> {
    if !baseline.is_finite()
        || baseline <= 0.0
        || !candidate.is_finite()
        || candidate < 0.0
        || !max_regression_ratio.is_finite()
        || max_regression_ratio < 0.0
    {
        return None;
    }

    let ratio = candidate / baseline;
    let signed_delta_ratio = ratio - 1.0;
    let upper_bound = baseline * (1.0 + max_regression_ratio);
    Some(RatioAssessment {
        ratio,
        signed_delta_ratio,
        absolute_delta_ratio: signed_delta_ratio.abs(),
        passed: candidate <= upper_bound,
    })
}

#[cfg(test)]
mod tests {
    use super::{normalized_digest, ratio_assessment, report_origin, type7_percentile};
    use serde_json::json;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn type7_percentile_rejects_empty_samples() {
        assert_eq!(type7_percentile(&[], 0.95), None);
    }

    #[test]
    fn type7_percentile_returns_singleton() {
        assert_eq!(type7_percentile(&[7.25], 0.50), Some(7.25));
        assert_eq!(type7_percentile(&[7.25], 0.95), Some(7.25));
    }

    #[test]
    fn type7_percentile_interpolates_between_adjacent_samples() {
        let samples = [4.0, 1.0, 3.0, 2.0];
        assert_close(type7_percentile(&samples, 0.25).unwrap(), 1.75);
        assert_close(type7_percentile(&samples, 0.50).unwrap(), 2.50);
        assert_close(type7_percentile(&samples, 0.95).unwrap(), 3.85);
    }

    #[test]
    fn ratio_gate_rejects_zero_baseline() {
        assert!(ratio_assessment(0.0, 0.0, 0.20).is_none());
        assert!(ratio_assessment(0.0, 1.0, 0.20).is_none());
    }

    #[test]
    fn ratio_gate_accepts_speedups_and_the_twenty_percent_regression_threshold() {
        let faster = ratio_assessment(10.0, 5.0, 0.20).unwrap();
        let threshold = ratio_assessment(10.0, 12.0, 0.20).unwrap();
        assert!(faster.passed);
        assert!(threshold.passed);
        assert_close(faster.ratio, 0.5);
        assert_close(threshold.ratio, 1.2);
    }

    #[test]
    fn ratio_gate_rejects_only_regressions_over_twenty_percent() {
        assert!(ratio_assessment(10.0, 7.999, 0.20).unwrap().passed);
        assert!(!ratio_assessment(10.0, 12.001, 0.20).unwrap().passed);
    }
    #[test]
    fn normalized_digest_ignores_object_and_collection_order_but_detects_content_changes() {
        let left = json!({
            "series": [
                {"bucket": 2, "tokens": 7},
                {"bucket": 1, "tokens": 3}
            ],
            "range": "24h"
        });
        let reordered = json!({
            "range": "24h",
            "series": [
                {"tokens": 3, "bucket": 1},
                {"tokens": 7, "bucket": 2}
            ]
        });
        let changed = json!({
            "range": "24h",
            "series": [
                {"bucket": 1, "tokens": 3},
                {"bucket": 2, "tokens": 8}
            ]
        });

        assert_eq!(
            normalized_digest(&left).unwrap(),
            normalized_digest(&reordered).unwrap()
        );
        assert_ne!(
            normalized_digest(&left).unwrap(),
            normalized_digest(&changed).unwrap()
        );
    }

    #[test]
    fn report_origin_strips_credentials_paths_and_queries() {
        assert_eq!(
            report_origin(
                "https://user:secret@example.test:9443/signed/path?token=secret",
                "test URL"
            )
            .unwrap(),
            "https://example.test:9443/"
        );
    }
}
