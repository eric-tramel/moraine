use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::ClickHouseConfig;
use moraine_conversations::{
    AnalyticsRange, ClickHouseConversationRepository, ConversationRepository, RepoConfig,
    SessionAnalyticsQuery, SessionLookback,
};
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::time::{Duration, Instant};

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
            monitor_endpoint(&config.monitor_url, "/api/analytics?range=24h")?;
        let monitor_sessions_url =
            monitor_endpoint(&config.monitor_url, "/api/sessions?since=30d&limit=50")?;
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
    fn into_cardinality(self) -> Result<Cardinality>;
}

impl MonitorResponse for MonitorAnalyticsResponse {
    fn into_cardinality(self) -> Result<Cardinality> {
        if !self.ok {
            bail!(
                "monitor analytics response had ok=false: {}",
                self.error.as_deref().unwrap_or("missing error message")
            );
        }
        let series = self
            .series
            .context("monitor analytics response had ok=true but no series")?;
        Ok(Cardinality::AnalyticsSeries {
            tokens: series.tokens.len(),
            turns: series.turns.len(),
            concurrent_sessions: series.concurrent_sessions.len(),
        })
    }
}

impl MonitorResponse for MonitorSessionsResponse {
    fn into_cardinality(self) -> Result<Cardinality> {
        if !self.ok {
            bail!(
                "monitor sessions response had ok=false: {}",
                self.error.as_deref().unwrap_or("missing error message")
            );
        }
        let sessions = self
            .sessions
            .context("monitor sessions response had ok=true but no sessions")?;
        Ok(Cardinality::Sessions {
            sessions: sessions.len(),
        })
    }
}

struct TimedSample {
    elapsed: Duration,
    cardinality: Cardinality,
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
    let cardinality = payload.into_cardinality()?;

    Ok(TimedSample {
        elapsed: started.elapsed(),
        cardinality,
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

    Ok(TimedSample {
        elapsed: started.elapsed(),
        cardinality: Cardinality::AnalyticsSeries {
            tokens: snapshot.tokens.len(),
            turns: snapshot.turns.len(),
            concurrent_sessions: snapshot.concurrent_sessions.len(),
        },
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

    Ok(TimedSample {
        elapsed: started.elapsed(),
        cardinality: Cardinality::Sessions {
            sessions: sessions.len(),
        },
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
}

impl Measurements {
    fn record(&mut self, sample: TimedSample, scenario: Scenario, arm: Arm) -> Result<()> {
        if let Some(expected) = &self.cardinality {
            if expected != &sample.cardinality {
                bail!(
                    "{} {:?} cardinality changed after {} successful samples: expected {:?}, got {:?}",
                    scenario.name(),
                    arm,
                    self.elapsed.len(),
                    expected,
                    sample.cardinality
                );
            }
        } else {
            self.cardinality = Some(sample.cardinality.clone());
        }
        self.elapsed.push(sample.elapsed);
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

        Ok(ArmReport {
            successful_samples: self.elapsed.len(),
            cardinality,
            p50_ms,
            p95_ms,
        })
    }
}

#[derive(Serialize)]
struct ArmReport {
    successful_samples: usize,
    cardinality: Cardinality,
    p50_ms: f64,
    p95_ms: f64,
}

#[derive(Serialize)]
struct CardinalityDifference {
    monitor_http: Cardinality,
    direct_repository: Cardinality,
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
    semantic_cardinality_difference: Option<CardinalityDifference>,
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
        passed: p95_comparison.passed,
        p95_comparison,
    })
}

#[derive(Serialize)]
struct WarmAnalyticsReport {
    successful_samples: usize,
    cardinality: Cardinality,
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
        monitor_url: config.monitor_url,
        clickhouse_url: config.clickhouse_url,
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
        bail!("one or more cold scenarios exceeded the 20% p95 regression ceiling");
    }
    Ok(())
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
    use super::{ratio_assessment, type7_percentile};

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
}
