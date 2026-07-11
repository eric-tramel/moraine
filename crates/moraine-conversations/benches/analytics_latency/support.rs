use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const SCHEMA_VERSION: &str = "moraine-benchmark-v1";
pub const ANALYTICS_24H_SCENARIO: &str = "analytics-24h";
pub const SESSIONS_30D50_SCENARIO: &str = "sessions-30d50";
const REQUIRED_SEMANTIC_SCENARIOS: [&str; 2] = [ANALYTICS_24H_SCENARIO, SESSIONS_30D50_SCENARIO];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Smoke,
    Full,
}

impl Profile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
        }
    }

    pub const fn warmups(self) -> usize {
        match self {
            Self::Smoke => 1,
            Self::Full => 5,
        }
    }

    pub const fn samples(self) -> usize {
        match self {
            Self::Smoke => 2,
            Self::Full => 60,
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "full" => Ok(Self::Full),
            _ => bail!("unsupported profile {value:?}; expected smoke or full"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub profile: Profile,
    pub output: PathBuf,
}

impl Cli {
    pub fn parse_from(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let _program = args.next();
        let mut profile = None;
        let mut output = None;
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--profile" => {
                    if profile.is_some() {
                        bail!("--profile may be specified only once");
                    }
                    let value = args.next().context("--profile requires smoke or full")?;
                    profile = Some(Profile::parse(&value)?);
                }
                "--output" => {
                    if output.is_some() {
                        bail!("--output may be specified only once");
                    }
                    let value = args.next().context("--output requires a path")?;
                    if value.is_empty() {
                        bail!("--output path must not be empty");
                    }
                    output = Some(PathBuf::from(value));
                }
                "--help" | "-h" => bail!(
                    "usage: analytics_latency --profile <smoke|full> --output <artifact.json>"
                ),
                _ => bail!("unexpected argument {argument:?}"),
            }
        }
        Ok(Self {
            profile: profile.context("--profile is required")?,
            output: output.context("--output is required")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Cardinality {
    AnalyticsSeries {
        tokens: usize,
        turns: usize,
        concurrent_sessions: usize,
    },
    Sessions {
        sessions: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticObservation {
    pub cardinality: Cardinality,
    pub digest: String,
}

impl SemanticObservation {
    pub fn new(cardinality: Cardinality, payload: &Value) -> Result<Self> {
        Ok(Self {
            cardinality,
            digest: normalized_digest(payload)?,
        })
    }

    fn validate(&self) -> Result<()> {
        let digest = self
            .digest
            .strip_prefix("fnv1a64:")
            .context("semantic digest must use the fnv1a64 prefix")?;
        if digest.len() != 16
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("semantic digest must be fnv1a64:<16 lowercase hex digits>");
        }
        Ok(())
    }
}

pub fn verify_expected_semantics(
    expected: &SemanticObservation,
    observed: &SemanticObservation,
    label: &str,
) -> Result<()> {
    expected.validate()?;
    observed.validate()?;
    if expected.cardinality != observed.cardinality {
        bail!("{label} cardinality did not match the seed manifest oracle");
    }
    if expected.digest != observed.digest {
        bail!("{label} digest did not match the seed manifest oracle");
    }
    Ok(())
}
pub fn normalized_digest(value: &Value) -> Result<String> {
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

pub fn monitor_session_projection(session: &Value) -> Result<Value> {
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

#[derive(Debug, Deserialize)]
pub struct MonitorAnalyticsResponse {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub series: Option<MonitorAnalyticsSeries>,
}

#[derive(Debug, Deserialize)]
pub struct MonitorAnalyticsSeries {
    pub tokens: Vec<Value>,
    pub turns: Vec<Value>,
    pub concurrent_sessions: Vec<Value>,
}

#[derive(Debug, Deserialize)]
pub struct MonitorSessionsResponse {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub sessions: Option<Vec<Value>>,
}

pub trait MonitorResponse: serde::de::DeserializeOwned {
    fn into_semantics(self) -> Result<SemanticObservation>;
}

impl MonitorResponse for MonitorAnalyticsResponse {
    fn into_semantics(self) -> Result<SemanticObservation> {
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
        SemanticObservation::new(
            cardinality,
            &json!({
                "tokens": series.tokens,
                "turns": series.turns,
                "concurrent_sessions": series.concurrent_sessions,
            }),
        )
    }
}

impl MonitorResponse for MonitorSessionsResponse {
    fn into_semantics(self) -> Result<SemanticObservation> {
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
        SemanticObservation::new(cardinality, &payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetIdentity {
    pub fingerprint: String,
    pub cardinality: usize,
}

impl DatasetIdentity {
    pub fn from_serialized_rows(rows: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Self {
        let mut rows = rows
            .into_iter()
            .map(|row| row.as_ref().to_vec())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        let mut hasher = Sha256::new();
        hasher.update(b"moraine-analytics-corpus-v1\0");
        hasher.update((rows.len() as u64).to_le_bytes());
        for row in &rows {
            hasher.update((row.len() as u64).to_le_bytes());
            hasher.update(row);
        }
        Self {
            fingerprint: format!("sha256:{:x}", hasher.finalize()),
            cardinality: rows.len(),
        }
    }

    pub fn verify_manifest(&self, manifest: &DatasetManifest) -> Result<()> {
        self.validate()?;
        manifest.validate()?;
        if self.cardinality != manifest.cardinality {
            bail!(
                "dataset cardinality differs from manifest: expected {}, observed {}",
                manifest.cardinality,
                self.cardinality
            );
        }
        if self.fingerprint != manifest.fingerprint {
            bail!("dataset fingerprint differs from manifest");
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        validate_dataset_identity(&self.fingerprint, self.cardinality)
    }
}

fn validate_dataset_identity(fingerprint: &str, cardinality: usize) -> Result<()> {
    if !fingerprint.starts_with("sha256:")
        || fingerprint.len() != 71
        || !fingerprint[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("dataset fingerprint must be sha256:<64 lowercase hex digits>");
    }
    if cardinality == 0 {
        bail!("dataset manifest must identify at least one seeded row");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub fingerprint: String,
    pub cardinality: usize,
    pub expected_semantics: BTreeMap<String, SemanticObservation>,
}

impl DatasetManifest {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read dataset manifest {}", path.display()))?;
        let manifest = serde_json::from_slice::<Self>(&bytes)
            .with_context(|| format!("invalid dataset manifest {}", path.display()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn expected(&self, scenario: &str) -> Result<&SemanticObservation> {
        self.expected_semantics
            .get(scenario)
            .with_context(|| format!("dataset manifest missing expected scenario {scenario:?}"))
    }

    pub fn verify_observation(
        &self,
        scenario: &str,
        observed: &SemanticObservation,
        label: &str,
    ) -> Result<()> {
        verify_expected_semantics(self.expected(scenario)?, observed, label)
    }

    fn validate(&self) -> Result<()> {
        validate_dataset_identity(&self.fingerprint, self.cardinality)?;
        for scenario in REQUIRED_SEMANTIC_SCENARIOS {
            let expected = self.expected(scenario)?;
            expected.validate()?;
            let kind_matches = matches!(
                (scenario, &expected.cardinality),
                (ANALYTICS_24H_SCENARIO, Cardinality::AnalyticsSeries { .. })
                    | (SESSIONS_30D50_SCENARIO, Cardinality::Sessions { .. })
            );
            if !kind_matches {
                bail!("dataset manifest scenario {scenario:?} has the wrong cardinality kind");
            }
        }
        if self.expected_semantics.len() != REQUIRED_SEMANTIC_SCENARIOS.len() {
            bail!(
                "dataset manifest must contain exactly the expected semantic scenarios: {}",
                REQUIRED_SEMANTIC_SCENARIOS.join(", ")
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptCounts {
    planned: usize,
    attempted: usize,
    successful: usize,
    errors: usize,
}

impl AttemptCounts {
    pub fn new(planned: usize) -> Result<Self> {
        if planned == 0 {
            bail!("planned sample count must be nonzero");
        }
        Ok(Self {
            planned,
            attempted: 0,
            successful: 0,
            errors: 0,
        })
    }

    pub fn record_success(&mut self) -> Result<()> {
        self.record(true)
    }

    pub fn record_error(&mut self) -> Result<()> {
        self.record(false)
    }

    fn record(&mut self, successful: bool) -> Result<()> {
        if self.attempted == self.planned {
            bail!("cannot record more outcomes than planned attempts");
        }
        self.attempted += 1;
        if successful {
            self.successful += 1;
        } else {
            self.errors += 1;
        }
        Ok(())
    }

    pub const fn planned(self) -> usize {
        self.planned
    }

    pub const fn attempted(self) -> usize {
        self.attempted
    }

    pub const fn successful(self) -> usize {
        self.successful
    }

    pub const fn errors(self) -> usize {
        self.errors
    }
}

#[derive(Debug)]
pub struct TimedObservation {
    pub elapsed: Duration,
    pub semantics: SemanticObservation,
}

#[derive(Debug, Default)]
pub struct Measurements {
    elapsed: Vec<Duration>,
    semantics: Option<SemanticObservation>,
}

impl Measurements {
    pub fn validate(&self, sample: &TimedObservation, label: &str) -> Result<()> {
        if let Some(expected) = &self.semantics {
            if expected != &sample.semantics {
                bail!(
                    "{label} semantics changed after {} successful samples: expected {:?}, got {:?}",
                    self.elapsed.len(),
                    expected,
                    sample.semantics
                );
            }
        }
        Ok(())
    }

    pub fn record(&mut self, sample: TimedObservation, label: &str) -> Result<()> {
        self.validate(&sample, label)?;
        if self.semantics.is_none() {
            self.semantics = Some(sample.semantics);
        }
        self.elapsed.push(sample.elapsed);
        Ok(())
    }

    pub fn report(&self) -> MeasurementReport {
        let latency_ms = self
            .elapsed
            .iter()
            .map(|duration| duration.as_secs_f64() * 1_000.0)
            .collect::<Vec<_>>();
        MeasurementReport {
            p50_ms: type7_percentile(&latency_ms, 0.50),
            p95_ms: type7_percentile(&latency_ms, 0.95),
            latency_ms,
            semantics: self.semantics.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MeasurementReport {
    pub latency_ms: Vec<f64>,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    #[serde(skip)]
    pub semantics: Option<SemanticObservation>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SemanticComparison {
    pub cardinality_match: bool,
    pub digest_match: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitor_cardinality: Option<Cardinality>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_cardinality: Option<Cardinality>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitor_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_digest: Option<String>,
}

impl SemanticComparison {
    pub fn compare(monitor: &SemanticObservation, repository: &SemanticObservation) -> Self {
        let cardinality_match = monitor.cardinality == repository.cardinality;
        let digest_match = monitor.digest == repository.digest;
        Self {
            cardinality_match,
            digest_match,
            monitor_cardinality: (!cardinality_match).then(|| monitor.cardinality.clone()),
            repository_cardinality: (!cardinality_match).then(|| repository.cardinality.clone()),
            monitor_digest: (!digest_match).then(|| monitor.digest.clone()),
            repository_digest: (!digest_match).then(|| repository.digest.clone()),
        }
    }

    pub const fn passed(&self) -> bool {
        self.cardinality_match && self.digest_match
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct TimingComparison {
    pub repository_minus_monitor_ms: f64,
    pub absolute_delta_ms: f64,
    pub repository_to_monitor_ratio: Option<f64>,
}

pub fn timing_comparison(monitor_ms: f64, repository_ms: f64) -> Result<TimingComparison> {
    if !monitor_ms.is_finite() || monitor_ms < 0.0 {
        bail!("monitor timing must be finite and non-negative");
    }
    if !repository_ms.is_finite() || repository_ms < 0.0 {
        bail!("repository timing must be finite and non-negative");
    }
    let delta = repository_ms - monitor_ms;
    Ok(TimingComparison {
        repository_minus_monitor_ms: delta,
        absolute_delta_ms: delta.abs(),
        repository_to_monitor_ratio: (monitor_ms > 0.0).then_some(repository_ms / monitor_ms),
    })
}

pub fn type7_percentile(samples: &[f64], probability: f64) -> Option<f64> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Source {
    pub git_commit: String,
    pub dirty: bool,
}

impl Source {
    pub fn collect() -> Result<Self> {
        let commit = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .context("failed to execute git rev-parse")?;
        if !commit.status.success() {
            bail!("git rev-parse HEAD failed");
        }
        let git_commit = String::from_utf8(commit.stdout)
            .context("git commit was not UTF-8")?
            .trim()
            .to_string();
        if git_commit.len() != 40
            || !git_commit
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("git rev-parse returned an invalid commit");
        }
        let status = Command::new("git")
            .args(["status", "--porcelain"])
            .output()
            .context("failed to execute git status")?;
        if !status.status.success() {
            bail!("git status --porcelain failed");
        }
        Ok(Self {
            git_commit,
            dirty: !status.stdout.is_empty(),
        })
    }
}

pub struct ProtocolReportInput {
    pub profile: Profile,
    pub source: Source,
    pub planned: usize,
    pub attempted: usize,
    pub successful: usize,
    pub errors: usize,
    pub measurements: BTreeMap<String, Vec<f64>>,
    pub semantic_passed: bool,
    pub passed_checks: usize,
    pub failed_checks: usize,
    pub dataset_fingerprint: String,
    pub dataset_cardinality: usize,
    pub diagnostics: Vec<Value>,
    pub artifacts: Vec<Value>,
}

pub fn build_protocol_report(input: ProtocolReportInput) -> Result<Value> {
    if input.planned == 0 {
        bail!("planned sample count must be nonzero");
    }
    if input.attempted != input.successful + input.errors {
        bail!("attempted must equal successful plus errors");
    }
    if input.attempted > input.planned {
        bail!("attempted must not exceed planned");
    }
    if input.measurements.is_empty() {
        bail!("measurements must not be empty");
    }
    for (name, samples) in &input.measurements {
        if !measurement_name_is_valid(name) {
            bail!("invalid unit-suffixed measurement name {name:?}");
        }
        if samples.len() != input.successful {
            bail!(
                "measurement {name:?} has {} samples, expected {}",
                samples.len(),
                input.successful
            );
        }
        if samples
            .iter()
            .any(|sample| !sample.is_finite() || *sample < 0.0)
        {
            bail!("measurement {name:?} contains an invalid sample");
        }
    }
    if !input.dataset_fingerprint.starts_with("sha256:")
        || input.dataset_fingerprint.len() != 71
        || !input.dataset_fingerprint[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("dataset fingerprint must be sha256:<64 lowercase hex digits>");
    }
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "benchmark_id": "analytics-latency",
        "scenario_id": "monitor-repository-analytics-matrix",
        "source": input.source,
        "build": {
            "profile": "release",
            "target": format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        },
        "runner": {
            "os": std::env::consts::OS,
            "cpu_class": std::env::consts::ARCH,
        },
        "scenario": {
            "profile": input.profile.name(),
            "workload_id": "analytics-24h_sessions-30d50",
            "measured_boundary": "monitor-http-and-direct-repository",
            "dimensions": {
                "dataset_backed": true,
                "cache_sensitive": true,
                "concurrent": false,
                "request_producing": false,
            },
            "fingerprints": {
                "dataset": {
                    "fingerprint": input.dataset_fingerprint,
                    "cardinality": input.dataset_cardinality,
                },
                "cache_state": "mixed",
            },
        },
        "samples": {
            "planned": input.planned,
            "attempted": input.attempted,
            "successful": input.successful,
            "errors": input.errors,
            "measurements": input.measurements,
        },
        "semantic": {
            "status": if input.semantic_passed { "pass" } else { "fail" },
        },
        "timing": {
            "status": "not_evaluated",
            "non_blocking": true,
        },
        "metrics": {
            "quality": {
                "oracle_status": if input.semantic_passed { "pass" } else { "fail" },
                "passed_checks": input.passed_checks,
                "failed_checks": input.failed_checks,
                "error_rate": input.errors as f64 / input.attempted.max(1) as f64,
            },
        },
        "diagnostics": input.diagnostics,
        "artifacts": input.artifacts,
    }))
}

fn measurement_name_is_valid(name: &str) -> bool {
    let unit = [
        "_ns", "_us", "_ms", "_seconds", "_bytes", "_count", "_ratio", "_percent",
    ]
    .iter()
    .any(|unit| name.ends_with(unit));
    unit && name
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

pub fn write_json_atomic(path: &Path, value: &Value) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_nanos();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("output path must end in a UTF-8 file name")?;
    let temporary =
        path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), stamp));
    let bytes = serde_json::to_vec_pretty(value).context("failed to serialize benchmark JSON")?;
    if let Err(error) = fs::write(&temporary, bytes) {
        let _ = fs::remove_file(&temporary);
        return Err(error).with_context(|| {
            format!(
                "failed to write temporary benchmark output {}",
                temporary.display()
            )
        });
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error)
            .with_context(|| format!("failed to publish benchmark output {}", path.display()));
    }
    Ok(())
}

#[allow(dead_code, unused_imports)]
#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    fn seed_manifest(dataset: DatasetIdentity) -> DatasetManifest {
        DatasetManifest {
            fingerprint: dataset.fingerprint,
            cardinality: dataset.cardinality,
            expected_semantics: BTreeMap::from([
                (
                    ANALYTICS_24H_SCENARIO.to_string(),
                    SemanticObservation {
                        cardinality: Cardinality::AnalyticsSeries {
                            tokens: 1,
                            turns: 1,
                            concurrent_sessions: 1,
                        },
                        digest: "fnv1a64:bf9d0d146fa2b87a".to_string(),
                    },
                ),
                (
                    SESSIONS_30D50_SCENARIO.to_string(),
                    SemanticObservation {
                        cardinality: Cardinality::Sessions { sessions: 1 },
                        digest: "fnv1a64:a5be61e61b8cb8f8".to_string(),
                    },
                ),
            ]),
        }
    }

    #[test]
    fn profiles_have_stable_smoke_and_full_workloads() {
        assert_eq!(Profile::Smoke.name(), "smoke");
        assert_eq!(Profile::Smoke.warmups(), 1);
        assert_eq!(Profile::Smoke.samples(), 2);
        assert_eq!(Profile::Full.name(), "full");
        assert_eq!(Profile::Full.warmups(), 5);
        assert_eq!(Profile::Full.samples(), 60);
    }

    #[test]
    fn cli_requires_explicit_profile_and_output() {
        assert!(Cli::parse_from(["bench".into(), "--profile".into(), "smoke".into()]).is_err());
        assert!(Cli::parse_from(["bench".into(), "--output".into(), "out.json".into()]).is_err());
        assert!(Cli::parse_from([
            "bench".into(),
            "--profile".into(),
            "quick".into(),
            "--output".into(),
            "out.json".into()
        ])
        .is_err());
        let cli = Cli::parse_from([
            "bench".into(),
            "--profile".into(),
            "full".into(),
            "--output".into(),
            "out.json".into(),
        ])
        .unwrap();
        assert_eq!(cli.profile, Profile::Full);
        assert_eq!(cli.output, PathBuf::from("out.json"));
    }

    #[test]
    fn type7_percentile_rejects_empty_or_invalid_samples() {
        assert_eq!(type7_percentile(&[], 0.95), None);
        assert_eq!(type7_percentile(&[f64::NAN], 0.95), None);
        assert_eq!(type7_percentile(&[1.0], 1.01), None);
    }

    #[test]
    fn type7_percentile_interpolates_and_handles_singletons() {
        assert_eq!(type7_percentile(&[7.25], 0.95), Some(7.25));
        let samples = [4.0, 1.0, 3.0, 2.0];
        assert_close(type7_percentile(&samples, 0.25).unwrap(), 1.75);
        assert_close(type7_percentile(&samples, 0.50).unwrap(), 2.50);
        assert_close(type7_percentile(&samples, 0.95).unwrap(), 3.85);
    }

    #[test]
    fn normalized_digest_is_order_independent_but_content_sensitive() {
        let left = json!({"series": [{"bucket": 2, "tokens": 7}, {"bucket": 1, "tokens": 3}], "range": "24h"});
        let reordered = json!({"range": "24h", "series": [{"tokens": 3, "bucket": 1}, {"tokens": 7, "bucket": 2}]});
        let changed = json!({"range": "24h", "series": [{"bucket": 1, "tokens": 3}, {"bucket": 2, "tokens": 8}]});
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
    fn measurements_reject_cardinality_or_digest_instability() {
        let first =
            SemanticObservation::new(Cardinality::Sessions { sessions: 1 }, &json!([{"id": "a"}]))
                .unwrap();
        let changed = SemanticObservation::new(
            Cardinality::Sessions { sessions: 2 },
            &json!([{"id": "a"}, {"id": "b"}]),
        )
        .unwrap();
        let mut measurements = Measurements::default();
        measurements
            .record(
                TimedObservation {
                    elapsed: Duration::from_millis(1),
                    semantics: first,
                },
                "sessions",
            )
            .unwrap();
        assert!(measurements
            .record(
                TimedObservation {
                    elapsed: Duration::from_millis(2),
                    semantics: changed
                },
                "sessions"
            )
            .is_err());
    }

    #[test]
    fn identical_but_wrong_arms_fail_the_seed_manifest_oracle() {
        let manifest = seed_manifest(DatasetIdentity::from_serialized_rows([b"owned-seed-row"]));
        let monitor = SemanticObservation::new(
            Cardinality::Sessions { sessions: 1 },
            &json!([{"session_id": "identically-wrong"}]),
        )
        .unwrap();
        let repository = monitor.clone();

        assert!(SemanticComparison::compare(&monitor, &repository).passed());
        assert!(manifest
            .verify_observation(SESSIONS_30D50_SCENARIO, &monitor, "monitor sessions")
            .is_err());
        assert!(manifest
            .verify_observation(SESSIONS_30D50_SCENARIO, &repository, "repository sessions")
            .is_err());
        let serialized = serde_json::to_string(&manifest).unwrap();
        assert!(!serialized.contains("seed-owner-private-value"));
        assert!(serialized.contains("\"cardinality\""));
        assert!(serialized.contains("\"digest\""));
        let round_trip: DatasetManifest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(round_trip, manifest);
        let mut unsafe_manifest = serde_json::to_value(&manifest).unwrap();
        unsafe_manifest
            .pointer_mut("/expected_semantics/sessions-30d50")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("payload".to_string(), json!("private seed data"));
        assert!(serde_json::from_value::<DatasetManifest>(unsafe_manifest).is_err());
    }

    #[test]
    fn manifest_missing_an_expected_scenario_fails_closed() {
        let mut manifest =
            seed_manifest(DatasetIdentity::from_serialized_rows([b"owned-seed-row"]));
        manifest.expected_semantics.remove(SESSIONS_30D50_SCENARIO);
        let error = manifest.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("missing expected scenario \"sessions-30d50\""));
    }

    #[test]
    fn timing_comparison_is_diagnostic_without_a_threshold() {
        let comparison = timing_comparison(10.0, 25.0).unwrap();
        assert_close(comparison.repository_minus_monitor_ms, 15.0);
        assert_close(comparison.repository_to_monitor_ratio.unwrap(), 2.5);
        assert!(timing_comparison(f64::NAN, 1.0).is_err());
        assert!(timing_comparison(1.0, -1.0).is_err());
    }

    fn protocol_input() -> ProtocolReportInput {
        ProtocolReportInput {
            profile: Profile::Smoke,
            source: Source {
                git_commit: "a".repeat(40),
                dirty: true,
            },
            planned: 2,
            attempted: 2,
            successful: 2,
            errors: 0,
            measurements: BTreeMap::from([
                ("monitor_latency_ms".to_string(), vec![1.0, 2.0]),
                ("repository_latency_ms".to_string(), vec![0.5, 0.75]),
            ]),
            semantic_passed: true,
            passed_checks: 3,
            failed_checks: 0,
            dataset_fingerprint: format!("sha256:{}", "b".repeat(64)),
            dataset_cardinality: 7,
            diagnostics: vec![json!({"code": "timing-not-evaluated"})],
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn protocol_report_enforces_counts_and_non_blocking_timing() {
        let report = build_protocol_report(protocol_input()).unwrap();
        assert_eq!(report["schema_version"], SCHEMA_VERSION);
        assert_eq!(report["samples"]["planned"], 2);
        assert_eq!(report["samples"]["successful"], 2);
        assert_eq!(report["semantic"]["status"], "pass");
        assert_eq!(report["timing"]["status"], "not_evaluated");
        assert_eq!(report["timing"]["non_blocking"], true);
        assert!(report["timing"].get("comparison_policy").is_none());
    }

    #[test]
    fn protocol_report_rejects_count_and_measurement_contradictions() {
        let mut input = protocol_input();
        input.errors = 1;
        assert!(build_protocol_report(input).is_err());
        let mut input = protocol_input();
        input
            .measurements
            .insert("monitor_latency_ms".to_string(), vec![1.0]);
        assert!(build_protocol_report(input).is_err());
        let mut input = protocol_input();
        input
            .measurements
            .insert("missing_unit".to_string(), vec![1.0, 2.0]);
        assert!(build_protocol_report(input).is_err());
    }

    #[test]
    fn attempt_counts_accumulate_every_planned_outcome() {
        let mut counts = AttemptCounts::new(3).unwrap();
        counts.record_success().unwrap();
        counts.record_error().unwrap();
        counts.record_success().unwrap();
        assert_eq!(counts.planned(), 3);
        assert_eq!(counts.attempted(), 3);
        assert_eq!(counts.successful(), 2);
        assert_eq!(counts.errors(), 1);
        assert_eq!(counts.attempted(), counts.successful() + counts.errors());
        assert!(counts.record_error().is_err());
    }

    #[test]
    fn partial_failure_artifact_preserves_counts_and_raw_successful_series() {
        let mut input = protocol_input();
        input.attempted = 2;
        input.successful = 1;
        input.errors = 1;
        input.measurements = BTreeMap::from([
            ("monitor_latency_ms".to_string(), vec![1.25]),
            ("repository_latency_ms".to_string(), vec![0.75]),
        ]);
        input.semantic_passed = false;
        input.passed_checks = 3;
        input.failed_checks = 1;
        input.diagnostics = vec![
            json!({"code": "monitor-analytics-error"}),
            json!({"code": "timing-not-evaluated"}),
        ];
        let artifact = build_protocol_report(input).unwrap();
        assert_eq!(artifact["samples"]["planned"], 2);
        assert_eq!(artifact["samples"]["attempted"], 2);
        assert_eq!(artifact["samples"]["successful"], 1);
        assert_eq!(artifact["samples"]["errors"], 1);
        assert_eq!(
            artifact["samples"]["measurements"]["monitor_latency_ms"],
            json!([1.25])
        );
        assert_eq!(artifact["semantic"]["status"], "fail");
        assert_eq!(artifact["timing"]["status"], "not_evaluated");
        assert_eq!(artifact["timing"]["non_blocking"], true);
        let root = std::env::temp_dir().join(format!(
            "moraine-benchmark-failure-artifact-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let output = root.join("artifact.json");
        write_json_atomic(&output, &artifact).unwrap();
        let validator =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/bench/benchmark_protocol.py");
        let validation = Command::new("python3")
            .arg(validator)
            .arg("validate")
            .arg(&output)
            .output()
            .unwrap();
        assert!(
            validation.status.success(),
            "validator stderr: {}",
            String::from_utf8_lossy(&validation.stderr)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corpus_identity_is_order_independent_and_changes_with_content_or_cardinality() {
        let baseline = DatasetIdentity::from_serialized_rows([
            br#"{"event_uid":"b","text":"second"}"#.as_slice(),
            br#"{"event_uid":"a","text":"first"}"#.as_slice(),
        ]);
        let reordered = DatasetIdentity::from_serialized_rows([
            br#"{"event_uid":"a","text":"first"}"#.as_slice(),
            br#"{"event_uid":"b","text":"second"}"#.as_slice(),
        ]);
        let content_changed = DatasetIdentity::from_serialized_rows([
            br#"{"event_uid":"a","text":"changed"}"#.as_slice(),
            br#"{"event_uid":"b","text":"second"}"#.as_slice(),
        ]);
        let cardinality_changed = DatasetIdentity::from_serialized_rows([
            br#"{"event_uid":"a","text":"first"}"#.as_slice(),
        ]);
        assert_eq!(baseline, reordered);
        assert_ne!(baseline.fingerprint, content_changed.fingerprint);
        assert_ne!(baseline.fingerprint, cardinality_changed.fingerprint);
        assert_ne!(baseline.cardinality, cardinality_changed.cardinality);
        baseline.verify_manifest(&seed_manifest(reordered)).unwrap();
        assert!(baseline
            .verify_manifest(&seed_manifest(content_changed))
            .is_err());
        assert!(baseline
            .verify_manifest(&seed_manifest(cardinality_changed))
            .is_err());
    }

    #[test]
    fn atomic_output_reports_unwritable_parent() {
        let root = std::env::temp_dir().join(format!(
            "moraine-benchmark-output-failure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&root, b"not a directory").unwrap();
        let error = write_json_atomic(&root.join("result.json"), &json!({"ok": true})).unwrap_err();
        assert!(error
            .to_string()
            .contains("failed to create output directory"));
        fs::remove_file(root).unwrap();
    }

    #[test]
    fn atomic_output_removes_temporary_file_when_publish_fails() {
        let root = std::env::temp_dir().join(format!(
            "moraine-benchmark-publish-failure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("result.json")).unwrap();
        let error = write_json_atomic(&root.join("result.json"), &json!({"ok": true})).unwrap_err();
        assert!(error
            .to_string()
            .contains("failed to publish benchmark output"));
        let entries = fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![std::ffi::OsString::from("result.json")]);
        fs::remove_dir_all(root).unwrap();
    }
}
