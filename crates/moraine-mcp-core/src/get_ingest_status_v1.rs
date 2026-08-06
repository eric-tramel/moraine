use super::{
    handled_tool_error_result, repo_error_to_contract_error, request_performance,
    tool_success_result, AppState,
};
use crate::contract::{
    ContractError, GetIngestStatusArgs, Performance, ToolEnvelope, ToolErrorCode,
    ToolErrorEnvelope, GET_INGEST_STATUS_TOOL,
};
use anyhow::{Context, Result};
use moraine_conversations::{
    IngestAlert, IngestCondition, IngestConditionState, IngestConditionType, IngestEta,
    IngestHistoryPoint, IngestRate, IngestStatus, IngestStatusRead,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const INGEST_STATUS_HISTORY_LIMIT: u16 = 61;

#[derive(Debug, Serialize)]
struct McpIngestStatus {
    observed_at_unix_ms: i64,
    current: Option<McpIngestCurrent>,
    conditions: Vec<McpIngestCondition>,
    alerts: Vec<McpIngestAlert>,
    rate: Option<McpIngestRate>,
    eta: Option<McpIngestEta>,
    history: Vec<McpIngestHistoryPoint>,
}

#[derive(Debug, Serialize)]
struct McpIngestCurrent {
    heartbeat_observed_at_unix_ms: i64,
    queue_depth: u64,
    files_active: u32,
    files_watched: u32,
    progress: Option<McpIngestProgress>,
}

#[derive(Debug, Serialize)]
struct McpIngestProgress {
    discovery_complete: bool,
    queue_capacity: u64,
    sink_pending_rows: u64,
    sink_pending_bytes: u64,
    sink_retrying: bool,
    oldest_pending_unix_ms: u64,
    last_durable_progress_unix_ms: u64,
    files_total: u64,
    files_completed: u64,
    bytes_total: u64,
    bytes_completed: u64,
}

#[derive(Debug, Serialize)]
struct McpIngestCondition {
    condition_type: IngestConditionType,
    state: IngestConditionState,
    reason: String,
    observed_at_unix_ms: i64,
}

#[derive(Debug, Serialize)]
struct McpIngestAlert {
    code: moraine_conversations::IngestAlertCode,
    observed_at_unix_ms: i64,
}

#[derive(Debug, Serialize)]
struct McpIngestRate {
    bytes_per_second: f64,
    sample_seconds: u64,
}

#[derive(Debug, Serialize)]
struct McpIngestEta {
    scope: String,
    low_seconds: u64,
    high_seconds: u64,
}

#[derive(Debug, Serialize)]
struct McpIngestHistoryPoint {
    ts_unix_ms: i64,
    queue_depth: u64,
    files_active: u32,
    queue_capacity: u64,
    sink_pending_rows: u64,
    sink_retrying: bool,
    discovery_complete: bool,
    files_total: u64,
    files_completed: u64,
    bytes_total: u64,
    bytes_completed: u64,
}

impl AppState {
    pub(crate) async fn get_ingest_status_v1(&self, arguments: Value) -> Result<Value> {
        let perf = request_performance();
        let request = json!({});
        if serde_json::from_value::<GetIngestStatusArgs>(arguments).is_err() {
            return encode_error(
                request,
                ContractError::new(
                    ToolErrorCode::InvalidRequest,
                    "get_ingest_status expects an empty JSON object",
                ),
                perf.finish(),
            );
        }

        let read = match self.read_ingest_status(INGEST_STATUS_HISTORY_LIMIT).await {
            Ok(read) => read,
            Err(error) => {
                return encode_error(request, repo_error_to_contract_error(error), perf.finish());
            }
        };
        let status = McpIngestStatus::from(read.derive(unix_now_ms()));
        let data = serde_json::to_value(&status).context("failed to encode ingestion status")?;
        let payload = serde_json::to_value(ToolEnvelope::success(
            GET_INGEST_STATUS_TOOL,
            request,
            data,
            perf.finish(),
        ))
        .context("failed to encode get_ingest_status response envelope")?;
        Ok(tool_success_result(format_status_text(&status), payload))
    }

    pub(crate) async fn read_ingest_status(
        &self,
        history_limit: u16,
    ) -> moraine_conversations::RepoResult<IngestStatusRead> {
        self.repo.ingest_status(history_limit).await
    }
}

impl From<IngestStatus> for McpIngestStatus {
    fn from(status: IngestStatus) -> Self {
        Self {
            observed_at_unix_ms: status.observed_at_unix_ms,
            current: status.heartbeat.latest.as_ref().map(McpIngestCurrent::from),
            conditions: status
                .conditions
                .into_iter()
                .map(McpIngestCondition::from)
                .collect(),
            alerts: status
                .alerts
                .into_iter()
                .map(McpIngestAlert::from)
                .collect(),
            rate: status.rate.map(McpIngestRate::from),
            eta: status.eta.map(McpIngestEta::from),
            history: status
                .history
                .into_iter()
                .map(McpIngestHistoryPoint::from)
                .collect(),
        }
    }
}

impl From<&moraine_conversations::IngestHeartbeat> for McpIngestCurrent {
    fn from(heartbeat: &moraine_conversations::IngestHeartbeat) -> Self {
        Self {
            heartbeat_observed_at_unix_ms: heartbeat.ts_unix_ms,
            queue_depth: heartbeat.queue_depth,
            files_active: heartbeat.files_active,
            files_watched: heartbeat.files_watched,
            progress: heartbeat.progress.as_ref().map(McpIngestProgress::from),
        }
    }
}

impl From<&moraine_conversations::IngestProgressSnapshot> for McpIngestProgress {
    fn from(progress: &moraine_conversations::IngestProgressSnapshot) -> Self {
        Self {
            discovery_complete: progress.discovery_complete,
            queue_capacity: progress.queue_capacity,
            sink_pending_rows: progress.sink_pending_rows,
            sink_pending_bytes: progress.sink_pending_bytes,
            sink_retrying: progress.sink_retrying,
            oldest_pending_unix_ms: progress.oldest_pending_unix_ms,
            last_durable_progress_unix_ms: progress.last_durable_progress_unix_ms,
            files_total: progress.files_total,
            files_completed: progress.files_completed,
            bytes_total: progress.bytes_total,
            bytes_completed: progress.bytes_completed,
        }
    }
}

impl From<IngestCondition> for McpIngestCondition {
    fn from(condition: IngestCondition) -> Self {
        Self {
            condition_type: condition.condition_type,
            state: condition.state,
            reason: condition.reason,
            observed_at_unix_ms: condition.observed_at_unix_ms,
        }
    }
}

impl From<IngestAlert> for McpIngestAlert {
    fn from(alert: IngestAlert) -> Self {
        Self {
            code: alert.code,
            observed_at_unix_ms: alert.observed_at_unix_ms,
        }
    }
}

impl From<IngestRate> for McpIngestRate {
    fn from(rate: IngestRate) -> Self {
        Self {
            bytes_per_second: rate.bytes_per_second,
            sample_seconds: rate.sample_seconds,
        }
    }
}

impl From<IngestEta> for McpIngestEta {
    fn from(eta: IngestEta) -> Self {
        Self {
            scope: eta.scope,
            low_seconds: eta.low_seconds,
            high_seconds: eta.high_seconds,
        }
    }
}

impl From<IngestHistoryPoint> for McpIngestHistoryPoint {
    fn from(point: IngestHistoryPoint) -> Self {
        Self {
            ts_unix_ms: point.ts_unix_ms,
            queue_depth: point.queue_depth,
            files_active: point.files_active,
            queue_capacity: point.queue_capacity,
            sink_pending_rows: point.sink_pending_rows,
            sink_retrying: point.sink_retrying,
            discovery_complete: point.discovery_complete,
            files_total: point.files_total,
            files_completed: point.files_completed,
            bytes_total: point.bytes_total,
            bytes_completed: point.bytes_completed,
        }
    }
}

pub(crate) fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn condition_state(status: &McpIngestStatus, kind: IngestConditionType) -> (&'static str, &str) {
    status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == kind)
        .map(|condition| {
            let state = match condition.state {
                IngestConditionState::True => "ok",
                IngestConditionState::False => "degraded",
                IngestConditionState::Unknown => "unknown",
            };
            (state, condition.reason.as_str())
        })
        .unwrap_or(("unknown", "condition_missing"))
}

fn format_status_text(status: &McpIngestStatus) -> String {
    let (health, health_reason) = condition_state(status, IngestConditionType::Health);
    let (coverage, coverage_reason) = condition_state(status, IngestConditionType::Coverage);
    let (freshness, freshness_reason) = condition_state(status, IngestConditionType::Freshness);
    let (readiness, readiness_reason) = condition_state(status, IngestConditionType::Readiness);
    let mut lines = vec![format!(
        "Ingestion health={health} ({health_reason}); coverage={coverage} ({coverage_reason}); freshness={freshness} ({freshness_reason}); readiness={readiness} ({readiness_reason})."
    )];

    if let Some(progress) = status
        .current
        .as_ref()
        .and_then(|current| current.progress.as_ref())
    {
        let percent = if progress.bytes_total > 0 {
            progress.bytes_completed as f64 * 100.0 / progress.bytes_total as f64
        } else if progress.files_total > 0 {
            progress.files_completed as f64 * 100.0 / progress.files_total as f64
        } else {
            100.0
        };
        lines.push(format!(
            "Historical startup snapshot: {}/{} files, {}/{} bytes ({percent:.1}%).",
            progress.files_completed,
            progress.files_total,
            progress.bytes_completed,
            progress.bytes_total
        ));
    }
    if let Some(rate) = &status.rate {
        lines.push(format!(
            "Durable checkpoint rate: {:.0} bytes/s over {}s.",
            rate.bytes_per_second, rate.sample_seconds
        ));
    }
    if let Some(eta) = &status.eta {
        lines.push(format!(
            "Estimated {} completion: {}-{}s.",
            eta.scope, eta.low_seconds, eta.high_seconds
        ));
    }
    if !status.alerts.is_empty() {
        lines.push(format!("Active alerts: {}.", status.alerts.len()));
    }
    lines.join("\n")
}

fn encode_error(request: Value, error: ContractError, performance: Performance) -> Result<Value> {
    let payload = serde_json::to_value(ToolErrorEnvelope::error(
        GET_INGEST_STATUS_TOOL,
        request,
        error,
        performance,
    ))
    .context("failed to encode get_ingest_status error envelope")?;
    Ok(handled_tool_error_result(
        "Unable to read ingestion status.".to_string(),
        payload,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::AppConfig;
    use moraine_conversations::{
        InMemoryConversationRepository, InMemoryConversationResponses, IngestCoverageBasis,
        IngestHeartbeat, IngestHeartbeatRead, IngestProgressSnapshot, IngestSourceProgress,
        RepoConfig, RepoError,
    };
    use std::sync::Arc;
    use std::time::Duration;

    fn raw_heartbeat(ts_unix_ms: i64, marker: &str) -> IngestHeartbeat {
        IngestHeartbeat {
            ts: format!("{marker}:raw timestamp"),
            ts_unix_ms,
            host: format!("/Users/alice/private/{marker}.jsonl"),
            service_version: format!("{marker}:service-version"),
            queue_depth: 3,
            files_active: 2,
            files_watched: 4,
            rows_raw_written: 10,
            rows_events_written: 9,
            rows_errors_written: 1,
            flush_latency_ms: 5,
            append_to_visible_p50_ms: 6,
            append_to_visible_p95_ms: 7,
            last_error: format!("{marker}:raw repository error password=hunter2"),
            watcher_backend: Some(format!("{marker}:watcher diagnostics")),
            watcher_error_count: Some(11),
            watcher_reset_count: Some(12),
            watcher_last_reset_unix_ms: Some(13),
            backend_sinks: Some(json!({
                "credential": format!("{marker}:SECRET_BACKEND_TOKEN"),
                "source_path": format!("/private/{marker}.db"),
            })),
            progress: Some(IngestProgressSnapshot {
                schema_version: 1,
                instance_id: format!("{marker}:instance"),
                run_started_unix_ms: 1,
                snapshot_unix_ms: 2,
                discovery_complete: true,
                queue_capacity: 64,
                sink_pending_rows: 8,
                sink_pending_bytes: 1_024,
                sink_retrying: false,
                oldest_pending_unix_ms: 3,
                last_durable_progress_unix_ms: 4,
                files_total: 10,
                files_completed: 8,
                bytes_total: 10_000,
                bytes_completed: 8_000,
                sources: vec![IngestSourceProgress {
                    source_name: format!("/Users/alice/private/{marker}.jsonl"),
                    format: format!("{marker}:raw-format"),
                    coverage_basis: IngestCoverageBasis::Bytes,
                    files_total: 10,
                    files_completed: 8,
                    bytes_total: 10_000,
                    bytes_completed: 8_000,
                    coverage_degraded: false,
                }],
            }),
        }
    }

    fn assert_no_markers(value: &Value, markers: &[&str]) {
        match value {
            Value::Array(values) => {
                for value in values {
                    assert_no_markers(value, markers);
                }
            }
            Value::Object(values) => {
                for (key, value) in values {
                    for marker in markers {
                        assert!(
                            !key.contains(marker),
                            "private marker {marker:?} leaked in key {key:?}"
                        );
                    }
                    assert_no_markers(value, markers);
                }
            }
            Value::String(text) => {
                for marker in markers {
                    assert!(
                        !text.contains(marker),
                        "private marker {marker:?} leaked in {text:?}"
                    );
                }
            }
            _ => {}
        }
    }

    fn assert_no_forbidden_status_keys(value: &Value) {
        const FORBIDDEN: &[&str] = &[
            "heartbeat",
            "host",
            "service_version",
            "last_error",
            "watcher_backend",
            "watcher_error_count",
            "watcher_reset_count",
            "watcher_last_reset_unix_ms",
            "backend_sinks",
            "sources",
            "source_name",
            "instance_id",
            "run_started_unix_ms",
            "snapshot_unix_ms",
        ];
        match value {
            Value::Array(values) => {
                for value in values {
                    assert_no_forbidden_status_keys(value);
                }
            }
            Value::Object(values) => {
                for (key, value) in values {
                    assert!(
                        !FORBIDDEN.contains(&key.as_str()),
                        "forbidden status field {key:?} was serialized"
                    );
                    assert_no_forbidden_status_keys(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn explicit_status_projection_redacts_latest_and_raw_history_recursively() {
        let latest_marker = "SECRET_LATEST_TOKEN";
        let history_marker = "SECRET_HISTORY_TOKEN";
        let latest = raw_heartbeat(120_000, latest_marker);
        let mut history = raw_heartbeat(60_000, history_marker);
        history
            .progress
            .as_mut()
            .expect("history progress")
            .bytes_completed = 4_000;
        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest),
            },
            history: vec![history],
        }
        .derive(120_001);
        let safe_status = McpIngestStatus::from(status);
        let data = serde_json::to_value(&safe_status).expect("safe status JSON");
        let payload = serde_json::to_value(ToolEnvelope::success(
            GET_INGEST_STATUS_TOOL,
            json!({}),
            data,
            Performance::from_elapsed(Duration::ZERO),
        ))
        .expect("status envelope");
        let response = tool_success_result(format_status_text(&safe_status), payload);

        assert_no_markers(
            &response,
            &[
                latest_marker,
                history_marker,
                "password=hunter2",
                "SECRET_BACKEND_TOKEN",
                "/Users/alice/private",
                "/private/",
                "raw repository error",
                "watcher diagnostics",
            ],
        );
        assert_no_forbidden_status_keys(&response["structuredContent"]["data"]);
        assert_eq!(
            response["structuredContent"]["data"]["history"][0],
            json!({
                "ts_unix_ms": 60_000,
                "queue_depth": 3,
                "files_active": 2,
                "queue_capacity": 64,
                "sink_pending_rows": 8,
                "sink_retrying": false,
                "discovery_complete": true,
                "files_total": 10,
                "files_completed": 8,
                "bytes_total": 10_000,
                "bytes_completed": 4_000,
            })
        );
    }

    #[tokio::test]
    async fn invalid_arguments_are_not_reflected_in_the_error_envelope() {
        let state = AppState::embedded(
            AppConfig::default(),
            Arc::new(InMemoryConversationRepository::default()),
        );
        let response = state
            .get_ingest_status_v1(json!({
                "credential": "SECRET_INVALID_REQUEST",
                "path": "/Users/alice/private/source.jsonl",
            }))
            .await
            .expect("invalid request response");

        assert_eq!(response["isError"], true);
        assert_eq!(
            response["structuredContent"]["error"]["code"],
            "invalid_request"
        );
        assert_eq!(response["structuredContent"]["request"], json!({}));
        assert_no_markers(
            &response,
            &["SECRET_INVALID_REQUEST", "/Users/alice/private"],
        );
    }

    #[tokio::test]
    async fn repository_failure_returns_sanitized_backend_failure() {
        let raw_error = "SECRET_REPOSITORY_ERROR /private/source.db password=hunter2";
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                latest_ingest_heartbeat: Some(Err(RepoError::backend(raw_error))),
                ..InMemoryConversationResponses::default()
            },
        ));
        let state = AppState::embedded(AppConfig::default(), repository);
        let response = state
            .get_ingest_status_v1(json!({}))
            .await
            .expect("status unavailable response");

        assert_eq!(response["isError"], true);
        assert_eq!(
            response["structuredContent"]["error"],
            json!({
                "code": "backend_failure",
                "message": "backend request failed",
            })
        );
        assert_no_markers(
            &response,
            &[
                "SECRET_REPOSITORY_ERROR",
                "/private/source.db",
                "password=hunter2",
            ],
        );
    }
}
