use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct SemanticComparison {
    pub cardinality_match: bool,
    pub digest_match: bool,
    pub monitor_cardinality: Option<Cardinality>,
    pub repository_cardinality: Option<Cardinality>,
    pub monitor_digest: Option<String>,
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

fn normalized_digest(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(&canonicalize_json(value))
        .context("failed to serialize normalized live-test payload")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_comparison_detects_cardinality_and_payload_drift() {
        let expected = SemanticObservation::new(
            Cardinality::Sessions { sessions: 1 },
            &json!([{"session_id": "expected"}]),
        )
        .unwrap();
        let cardinality_drift = SemanticObservation::new(
            Cardinality::Sessions { sessions: 2 },
            &json!([{"session_id": "expected"}]),
        )
        .unwrap();
        let payload_drift = SemanticObservation::new(
            Cardinality::Sessions { sessions: 1 },
            &json!([{"session_id": "different"}]),
        )
        .unwrap();

        assert!(!SemanticComparison::compare(&expected, &cardinality_drift).passed());
        assert!(!SemanticComparison::compare(&expected, &payload_drift).passed());
        assert!(SemanticComparison::compare(&expected, &expected).passed());
    }

    #[test]
    fn monitor_sessions_normalize_order_and_reject_incomplete_shapes() {
        let first = json!({
            "id": "session-a",
            "harness": {"id": "claude"},
            "startedAt": 1,
            "endedAt": 2,
            "models": ["model-a"],
            "traceId": "trace-a",
            "turns": [{"id": 1}],
            "totalToolCalls": 3
        });
        let second = json!({
            "id": "session-b",
            "harness": {"id": "codex"},
            "startedAt": 3,
            "endedAt": 4,
            "models": ["model-b"],
            "traceId": "trace-b",
            "turns": [],
            "totalToolCalls": 0
        });
        let forward = MonitorSessionsResponse {
            ok: true,
            error: None,
            sessions: Some(vec![first.clone(), second.clone()]),
        }
        .into_semantics()
        .unwrap();
        let reverse = MonitorSessionsResponse {
            ok: true,
            error: None,
            sessions: Some(vec![second, first]),
        }
        .into_semantics()
        .unwrap();

        assert_eq!(forward, reverse);
        assert!(MonitorSessionsResponse {
            ok: true,
            error: None,
            sessions: Some(vec![json!({"id": "incomplete"})]),
        }
        .into_semantics()
        .is_err());
    }
}
