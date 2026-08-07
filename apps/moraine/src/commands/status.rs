use crate::managed_clickhouse::{
    active_clickhouse_source, managed_clickhouse_bin, managed_clickhouse_checksum_state,
    managed_clickhouse_version,
};
use crate::paths::RuntimePaths;
use crate::process::{
    backend_endpoint_status, backend_http_connect_host, legacy_service_running_read_only,
    service_running_read_only, BackendEndpointStatus, LEGACY_MCP_PID_FILE, LEGACY_MONITOR_PID_FILE,
};
use crate::render::{
    HeartbeatSnapshot, ServiceRuntimeState, ServiceRuntimeStatus, StatusDataSource, StatusSnapshot,
};
use crate::service::Service;
use anyhow::{bail, Context, Result};
use moraine_clickhouse::{DoctorReport, QueryPressureSnapshot};
use moraine_config::AppConfig;
use moraine_conversations::{
    ConversationRepository, IngestHeartbeatRead, IngestStatus, StoreDiagnostics,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const STATUS_API_TIMEOUT: Duration = Duration::from_secs(2);
const STATUS_API_MAX_RESPONSE_BYTES: usize = 256 * 1024;
fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[derive(Debug, serde::Deserialize)]
#[serde(transparent)]
struct RequiredNullable<T>(Option<T>);

#[derive(Debug, serde::Deserialize)]
struct DaemonErrorResponse {
    ok: bool,
    code: String,
    error: String,
}

#[derive(Debug)]
struct DaemonStatusReadError {
    error: anyhow::Error,
    compatibility_fallback: bool,
}

impl DaemonStatusReadError {
    fn fatal(error: anyhow::Error) -> Self {
        Self {
            error,
            compatibility_fallback: false,
        }
    }

    fn compatibility(error: anyhow::Error) -> Self {
        Self {
            error,
            compatibility_fallback: true,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct DaemonStatusResponse {
    ok: bool,
    clickhouse: DaemonClickhouseStatus,
    database: DaemonDatabaseStatus,
    ingestor: DaemonIngestorStatus,
    #[serde(default)]
    ingest_status: Option<IngestStatus>,
    #[serde(default)]
    query_pressure: Option<QueryPressureSnapshot>,
}

#[derive(Debug, serde::Deserialize)]
struct DaemonClickhouseStatus {
    url: String,
    database: String,
    healthy: bool,
    version: RequiredNullable<String>,
    error: RequiredNullable<String>,
}

#[derive(Debug, serde::Deserialize)]
struct DaemonDatabaseStatus {
    exists: bool,
}

#[derive(Debug, serde::Deserialize)]
struct DaemonIngestorStatus {
    present: bool,
    latest: RequiredNullable<DaemonHeartbeat>,
}

#[derive(Debug, serde::Deserialize)]
struct DaemonHeartbeat {
    ts: String,
    queue_depth: u64,
    files_active: u64,
}

struct StatusData {
    report: DoctorReport,
    heartbeat: HeartbeatSnapshot,
    ingest_status: Option<IngestStatus>,
    query_pressure: Option<QueryPressureSnapshot>,
    source: StatusDataSource,
    fallback_note: Option<String>,
    clickhouse_health_url: String,
}

fn service_runtime_running(services: &[ServiceRuntimeStatus], service: Service) -> bool {
    services
        .iter()
        .find(|row| row.service == service)
        .and_then(|row| row.pid)
        .is_some()
}

fn clickhouse_runtime_running(services: &[ServiceRuntimeStatus]) -> bool {
    service_runtime_running(services, Service::ClickHouse)
}

fn managed_runtime_status(service: Service, pid: Option<u32>) -> ServiceRuntimeStatus {
    ServiceRuntimeStatus {
        service,
        pid,
        state: if pid.is_some() {
            ServiceRuntimeState::Running
        } else {
            ServiceRuntimeState::Stopped
        },
        socket_listening: None,
        http_listening: None,
    }
}

fn backend_runtime_status(
    pid: Option<u32>,
    endpoints: BackendEndpointStatus,
) -> ServiceRuntimeStatus {
    let state = match (
        pid.is_some(),
        endpoints.socket_listening,
        endpoints.http_listening,
    ) {
        (true, true, true) => ServiceRuntimeState::Running,
        (false, true, true) => ServiceRuntimeState::Unmanaged,
        (false, false, false) => ServiceRuntimeState::Stopped,
        _ => ServiceRuntimeState::Partial,
    };
    ServiceRuntimeStatus {
        service: Service::Backend,
        pid,
        state,
        socket_listening: Some(endpoints.socket_listening),
        http_listening: Some(endpoints.http_listening),
    }
}

fn format_http_url(host: &str, port: u16) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

fn monitor_runtime_url(cfg: &AppConfig) -> String {
    format_http_url(&cfg.backend.bind, cfg.monitor.port)
}
fn monitor_api_status_url(cfg: &AppConfig) -> String {
    format!(
        "{}/api/v1/status?history=120",
        format_http_url(
            backend_http_connect_host(&cfg.monitor.host),
            cfg.monitor.port
        )
    )
}

fn daemon_status_data(payload: DaemonStatusResponse) -> Result<StatusData> {
    if !payload.ok {
        bail!("daemon API reported ok=false");
    }
    let version_present = payload.clickhouse.version.0.is_some();
    let error_present = payload.clickhouse.error.0.is_some();
    if payload.clickhouse.healthy && (!payload.database.exists || !version_present || error_present)
    {
        bail!("daemon API returned contradictory healthy ClickHouse fields");
    }
    if !payload.clickhouse.healthy && !error_present {
        bail!("daemon API returned an unhealthy ClickHouse without an error");
    }

    let latest = payload.ingestor.latest.0;
    if payload.ingestor.present != latest.is_some() {
        bail!("daemon API returned inconsistent ingestor presence");
    }
    let heartbeat = match latest {
        Some(latest) => HeartbeatSnapshot::Available {
            latest: latest.ts,
            queue_depth: latest.queue_depth,
            files_active: latest.files_active,
            watcher_backend: "unknown".to_string(),
            watcher_error_count: 0,
            watcher_reset_count: 0,
            watcher_last_reset_unix_ms: 0,
        },
        None => HeartbeatSnapshot::Unavailable,
    };
    let report = DoctorReport {
        clickhouse_healthy: payload.clickhouse.healthy,
        clickhouse_version: payload.clickhouse.version.0,
        database: payload.clickhouse.database,
        database_exists: payload.database.exists,
        applied_migrations: Vec::new(),
        pending_migrations: Vec::new(),
        missing_tables: Vec::new(),
        errors: payload.clickhouse.error.0.into_iter().collect(),
    };

    Ok(StatusData {
        report,
        heartbeat,
        ingest_status: payload.ingest_status,
        query_pressure: payload.query_pressure,
        source: StatusDataSource::DaemonApi,
        fallback_note: None,
        clickhouse_health_url: payload.clickhouse.url,
    })
}

async fn read_daemon_status(
    cfg: &AppConfig,
    timeout: Duration,
) -> std::result::Result<StatusData, DaemonStatusReadError> {
    let api_url = monitor_api_status_url(cfg);
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .context("build daemon status API client")
        .map_err(DaemonStatusReadError::fatal)?;
    let mut response = client
        .get(&api_url)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .with_context(|| format!("request {api_url}"))
        .map_err(DaemonStatusReadError::fatal)?;
    let status = response.status();

    // A listening pre-unification monitor has no canonical v1 route. This is
    // the only HTTP status that proves the request was rejected before query
    // admission and is therefore safe to serve through the compatibility path.
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(DaemonStatusReadError::compatibility(anyhow::anyhow!(
            "request {api_url} returned {status}"
        )));
    }

    let oversized = response
        .content_length()
        .is_some_and(|length| length > STATUS_API_MAX_RESPONSE_BYTES as u64);
    if oversized {
        let error = anyhow::anyhow!(
            "daemon status API response exceeds {STATUS_API_MAX_RESPONSE_BYTES} bytes"
        );
        return Err(if status.is_success() {
            DaemonStatusReadError::compatibility(error)
        } else {
            DaemonStatusReadError::fatal(error)
        });
    }
    let capacity = response
        .content_length()
        .unwrap_or_default()
        .min(STATUS_API_MAX_RESPONSE_BYTES as u64) as usize;
    let mut body = Vec::with_capacity(capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("read {api_url} response"))
        .map_err(DaemonStatusReadError::fatal)?
    {
        if chunk.len() > STATUS_API_MAX_RESPONSE_BYTES - body.len() {
            let error = anyhow::anyhow!(
                "daemon status API response exceeds {STATUS_API_MAX_RESPONSE_BYTES} bytes"
            );
            return Err(if status.is_success() {
                DaemonStatusReadError::compatibility(error)
            } else {
                DaemonStatusReadError::fatal(error)
            });
        }
        body.extend_from_slice(&chunk);
    }

    if !status.is_success() {
        let error = match serde_json::from_slice::<DaemonErrorResponse>(&body) {
            Ok(payload) if !payload.ok => anyhow::anyhow!(
                "daemon status API returned {status} ({}): {}",
                payload.code,
                payload.error
            ),
            _ => anyhow::anyhow!("daemon status API returned {status}"),
        };
        return Err(DaemonStatusReadError::fatal(error));
    }

    let payload = serde_json::from_slice(&body)
        .with_context(|| format!("decode {api_url} response as JSON"))
        .map_err(DaemonStatusReadError::compatibility)?;
    daemon_status_data(payload).map_err(DaemonStatusReadError::compatibility)
}

fn build_status_notes(
    services: &[ServiceRuntimeStatus],
    report: &DoctorReport,
    clickhouse_url: &str,
) -> Vec<String> {
    let clickhouse_running = clickhouse_runtime_running(services);
    let mut notes = Vec::new();

    if report.clickhouse_healthy && !clickhouse_running {
        notes.push(format!(
            "database health checks query clickhouse.url ({clickhouse_url}); endpoint is healthy while managed clickhouse runtime is stopped"
        ));
    }

    if !report.clickhouse_healthy && clickhouse_running {
        notes.push(format!(
            "managed clickhouse runtime is running, but health checks against clickhouse.url ({clickhouse_url}) are failing"
        ));
    }

    if let Some(backend) = services
        .iter()
        .find(|service| service.service == Service::Backend)
    {
        match backend.state {
            ServiceRuntimeState::Partial => notes.push(format!(
                "backend is partially available (managed pid: {}, MCP socket: {}, monitor HTTP: {})",
                backend
                    .pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                backend.socket_listening.unwrap_or(false),
                backend.http_listening.unwrap_or(false)
            )),
            ServiceRuntimeState::Unmanaged => notes.push(
                "backend endpoints are serving, but no managed backend PID is tracked".to_string(),
            ),
            ServiceRuntimeState::Running | ServiceRuntimeState::Stopped => {}
        }
    }
    notes
}

fn doctor_report(diagnostics: StoreDiagnostics) -> DoctorReport {
    DoctorReport {
        clickhouse_healthy: diagnostics.healthy,
        clickhouse_version: diagnostics.version,
        database: diagnostics.database,
        database_exists: diagnostics.database_exists,
        applied_migrations: diagnostics.applied_schema_versions,
        pending_migrations: diagnostics.pending_schema_versions,
        missing_tables: diagnostics.missing_tables,
        errors: diagnostics.errors,
    }
}

fn heartbeat_snapshot(read: IngestHeartbeatRead) -> HeartbeatSnapshot {
    match read.latest {
        Some(heartbeat) => HeartbeatSnapshot::Available {
            latest: heartbeat.ts,
            queue_depth: heartbeat.queue_depth,
            files_active: u64::from(heartbeat.files_active),
            watcher_backend: heartbeat
                .watcher_backend
                .unwrap_or_else(|| "unknown".to_string()),
            watcher_error_count: heartbeat.watcher_error_count.unwrap_or(0),
            watcher_reset_count: heartbeat.watcher_reset_count.unwrap_or(0),
            watcher_last_reset_unix_ms: heartbeat.watcher_last_reset_unix_ms.unwrap_or(0),
        },
        None => HeartbeatSnapshot::Unavailable,
    }
}

async fn read_repository_status(
    repository: &dyn ConversationRepository,
) -> Result<(DoctorReport, HeartbeatSnapshot, Option<IngestStatus>)> {
    let report = doctor_report(repository.read_store_diagnostics().await?);
    let (heartbeat, ingest_status) = match repository.ingest_status(120).await {
        Ok(read) => {
            let heartbeat = heartbeat_snapshot(read.heartbeat.clone());
            (heartbeat, Some(read.derive(unix_now_ms())))
        }
        Err(err) => (
            HeartbeatSnapshot::Error {
                message: err.to_string(),
            },
            None,
        ),
    };
    Ok((report, heartbeat, ingest_status))
}
async fn read_preferred_status(
    cfg: &AppConfig,
    repository: &dyn ConversationRepository,
    api_available: bool,
    timeout: Duration,
) -> Result<StatusData> {
    if api_available {
        match read_daemon_status(cfg, timeout).await {
            Ok(status) => return Ok(status),
            Err(error) if error.compatibility_fallback => {
                let (report, heartbeat, ingest_status) = read_repository_status(repository).await?;
                return Ok(StatusData {
                    report,
                    heartbeat,
                    ingest_status,
                    query_pressure: None,
                    source: StatusDataSource::DirectDb,
                    fallback_note: Some(format!(
                        "daemon status API is incompatible ({}); using direct DB fallback",
                        error.error
                    )),
                    clickhouse_health_url: cfg.clickhouse.url.clone(),
                });
            }
            Err(error) => return Err(error.error),
        }
    }

    let (report, heartbeat, ingest_status) = read_repository_status(repository).await?;
    Ok(StatusData {
        report,
        heartbeat,
        ingest_status,
        query_pressure: None,
        source: StatusDataSource::DirectDb,
        fallback_note: None,
        clickhouse_health_url: cfg.clickhouse.url.clone(),
    })
}

pub(super) async fn cmd_status(
    paths: &RuntimePaths,
    cfg: &AppConfig,
    repository: &dyn ConversationRepository,
) -> Result<StatusSnapshot> {
    let backend_endpoints = backend_endpoint_status(cfg);
    let services = vec![
        managed_runtime_status(
            Service::ClickHouse,
            service_running_read_only(paths, Service::ClickHouse),
        ),
        managed_runtime_status(
            Service::Ingest,
            service_running_read_only(paths, Service::Ingest),
        ),
        backend_runtime_status(
            service_running_read_only(paths, Service::Backend),
            backend_endpoints,
        ),
    ];
    let managed_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let (source, source_path) = active_clickhouse_source(paths);
    let StatusData {
        report,
        heartbeat,
        source: data_source,
        ingest_status,
        fallback_note,
        query_pressure,
        clickhouse_health_url,
    } = read_preferred_status(
        cfg,
        repository,
        backend_endpoints.http_listening,
        STATUS_API_TIMEOUT,
    )
    .await?;
    let mut status_notes = build_status_notes(&services, &report, &clickhouse_health_url);
    if let Some(note) = fallback_note {
        status_notes.push(note);
    }
    for (name, pid_file) in [
        ("monitor", LEGACY_MONITOR_PID_FILE),
        ("MCP", LEGACY_MCP_PID_FILE),
    ] {
        if let Some(pid) = legacy_service_running_read_only(paths, pid_file) {
            status_notes.push(format!(
                "legacy managed {name} process (pid {pid}) is still tracked; run `moraine down` before starting the unified backend"
            ));
        }
    }
    let monitor_url = backend_endpoints
        .http_listening
        .then(|| monitor_runtime_url(cfg));

    Ok(StatusSnapshot {
        services,
        monitor_url,
        data_source,
        managed_clickhouse_installed: managed_server.exists(),
        managed_clickhouse_path: managed_server.display().to_string(),
        managed_clickhouse_version: managed_clickhouse_version(paths),
        clickhouse_active_source: source.to_string(),
        clickhouse_active_source_path: source_path.map(|path| path.display().to_string()),
        managed_clickhouse_checksum: managed_clickhouse_checksum_state(cfg, paths),
        clickhouse_health_url,
        status_notes,
        doctor: report,
        ingest_status,
        query_pressure,
        heartbeat,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_conversations::{
        InMemoryConversationRepository, InMemoryConversationResponses, IngestHeartbeat, RepoConfig,
        RepoResult,
    };
    use serde_json::{json, Value};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Instant;

    fn test_config(monitor_port: u16) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.monitor.host = "127.0.0.1".to_string();
        cfg.monitor.port = monitor_port;
        cfg
    }

    fn test_repository() -> InMemoryConversationRepository {
        InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                latest_ingest_heartbeat: Some(Ok(IngestHeartbeatRead::default())),
                read_store_diagnostics: Some(Ok(StoreDiagnostics {
                    healthy: true,
                    version: Some("direct-db-version".to_string()),
                    database: "direct_db".to_string(),
                    database_exists: true,
                    applied_schema_versions: vec!["001".to_string()],
                    pending_schema_versions: Vec::new(),
                    missing_tables: Vec::new(),
                    errors: Vec::new(),
                })),
                ..InMemoryConversationResponses::default()
            },
        )
    }

    fn daemon_status_body(healthy: bool) -> String {
        json!({
            "ok": true,
            "clickhouse": {
                "url": "http://api-clickhouse:8123",
                "database": "api_db",
                "healthy": healthy,
                "version": "26.1.2.3",
                "error": if healthy {
                    Value::Null
                } else {
                    Value::String("API-reported database failure".to_string())
                }
            },
            "database": {"exists": true},
            "ingestor": {
                "present": true,
                "latest": {
                    "ts": "2026-07-10 12:34:56.789",
                    "queue_depth": 17,
                    "files_active": 2
                }
            }
        })
        .to_string()
    }

    fn spawn_api_response_with_status(
        status: &str,
        body: &str,
        delay: Duration,
    ) -> (u16, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind daemon API fixture");
        let port = listener.local_addr().expect("daemon API address").port();
        let status = status.to_string();
        let body = body.to_string();
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept daemon API request");
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .expect("set request timeout");
            let mut request = [0_u8; 2048];
            let request_len = stream.read(&mut request).expect("read daemon API request");
            thread::sleep(delay);
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
            String::from_utf8_lossy(&request[..request_len]).into_owned()
        });
        (port, worker)
    }

    fn spawn_api_response(body: &str, delay: Duration) -> (u16, thread::JoinHandle<String>) {
        spawn_api_response_with_status("200 OK", body, delay)
    }

    async fn status_json(heartbeat: RepoResult<IngestHeartbeatRead>) -> Value {
        let mut cfg = AppConfig::default();
        let test_root = std::env::temp_dir().join(format!(
            "moraine-status-unit-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        cfg.runtime.root_dir = test_root.display().to_string();
        cfg.runtime.logs_dir = test_root.join("logs").display().to_string();
        cfg.runtime.pids_dir = test_root.join("run").display().to_string();
        cfg.runtime.service_bin_dir = test_root.join("services").display().to_string();
        cfg.runtime.managed_clickhouse_dir = test_root.join("managed").display().to_string();
        cfg.mcp.central_socket_path = test_root.join("mcp.sock").display().to_string();
        cfg.backend.bind = "127.0.0.1".to_string();
        cfg.monitor.port = 9;
        let paths = crate::paths::runtime_paths(&cfg);
        let repository = InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                latest_ingest_heartbeat: Some(heartbeat),
                read_store_diagnostics: Some(Ok(StoreDiagnostics {
                    healthy: true,
                    version: Some("25.8.1.1".to_string()),
                    database: "moraine".to_string(),
                    database_exists: true,
                    applied_schema_versions: vec!["001".to_string()],
                    pending_schema_versions: Vec::new(),
                    missing_tables: Vec::new(),
                    errors: Vec::new(),
                })),
                ..InMemoryConversationResponses::default()
            },
        );
        let snapshot = cmd_status(&paths, &cfg, &repository)
            .await
            .expect("collect status");
        serde_json::to_value(snapshot).expect("serialize status")
    }

    fn stale_heartbeat() -> IngestHeartbeat {
        IngestHeartbeat {
            ts: "2000-01-01 00:00:00.000".to_string(),
            ts_unix_ms: 946_684_800_000,
            host: "old-host".to_string(),
            service_version: "0.1.0".to_string(),
            queue_depth: 7,
            files_active: 3,
            files_watched: 9,
            rows_raw_written: 11,
            rows_events_written: 10,
            rows_errors_written: 1,
            flush_latency_ms: 12,
            append_to_visible_p50_ms: 13,
            append_to_visible_p95_ms: 14,
            last_error: String::new(),
            watcher_backend: None,
            watcher_error_count: None,
            watcher_reset_count: None,
            watcher_last_reset_unix_ms: None,
            backend_sinks: None,
            progress: None,
        }
    }

    #[tokio::test]
    async fn daemon_api_precedes_direct_db_even_when_api_reports_unhealthy() {
        let repository = test_repository();
        let (port, worker) =
            spawn_api_response(&daemon_status_body(false), Duration::from_millis(0));
        let status = read_preferred_status(
            &test_config(port),
            &repository,
            true,
            Duration::from_secs(1),
        )
        .await
        .expect("read preferred daemon status");

        assert_eq!(status.source, StatusDataSource::DaemonApi);
        assert_eq!(status.clickhouse_health_url, "http://api-clickhouse:8123");
        assert!(!status.report.clickhouse_healthy);
        assert_eq!(status.report.database, "api_db");
        assert_eq!(status.report.errors, vec!["API-reported database failure"]);
        assert!(matches!(
            status.heartbeat,
            HeartbeatSnapshot::Available {
                ref latest,
                queue_depth: 17,
                files_active: 2,
                ..
            } if latest == "2026-07-10 12:34:56.789"
        ));
        let calls = repository.calls();
        assert_eq!(calls.read_store_diagnostics, 0);
        assert_eq!(calls.latest_ingest_heartbeat, 0);
        assert!(calls.ingest_status.is_empty());
        let request = worker.join().expect("daemon API worker");
        assert!(
            request.starts_with("GET /api/v1/status?history=120 HTTP/1.1"),
            "{request}"
        );
        assert!(request.contains("accept: application/json"), "{request}");
    }

    #[tokio::test]
    async fn malformed_and_partial_api_responses_fall_back_to_direct_db() {
        for body in ["{", r#"{"ok":true}"#] {
            let repository = test_repository();
            let (port, worker) = spawn_api_response(body, Duration::from_millis(0));
            let status = read_preferred_status(
                &test_config(port),
                &repository,
                true,
                Duration::from_secs(1),
            )
            .await
            .expect("fall back after invalid daemon response");

            assert_eq!(status.source, StatusDataSource::DirectDb);
            assert_eq!(status.report.database, "direct_db");
            assert!(
                status
                    .fallback_note
                    .as_deref()
                    .is_some_and(|note| note.contains("using direct DB fallback")),
                "{:?}",
                status.fallback_note
            );
            let calls = repository.calls();
            assert_eq!(calls.read_store_diagnostics, 1);
            assert_eq!(calls.latest_ingest_heartbeat, 1);
            assert_eq!(calls.ingest_status, vec![120]);
            let request = worker.join().expect("daemon API worker");
            assert!(request.starts_with("GET /api/v1/status?history=120 HTTP/1.1"));
        }
    }
    #[tokio::test]
    async fn contradictory_api_health_fields_fall_back_to_direct_db() {
        let mut missing_version: Value =
            serde_json::from_str(&daemon_status_body(true)).expect("valid fixture");
        missing_version["clickhouse"]["version"] = Value::Null;
        let mut healthy_with_error: Value =
            serde_json::from_str(&daemon_status_body(true)).expect("valid fixture");
        healthy_with_error["clickhouse"]["error"] = Value::String("contradiction".to_string());
        let mut unhealthy_without_error: Value =
            serde_json::from_str(&daemon_status_body(false)).expect("valid fixture");
        unhealthy_without_error["clickhouse"]["error"] = Value::Null;

        for payload in [missing_version, healthy_with_error, unhealthy_without_error] {
            let repository = test_repository();
            let (port, worker) = spawn_api_response(&payload.to_string(), Duration::from_millis(0));
            let status = read_preferred_status(
                &test_config(port),
                &repository,
                true,
                Duration::from_secs(1),
            )
            .await
            .expect("fall back after contradictory daemon response");

            assert_eq!(status.source, StatusDataSource::DirectDb);
            let calls = repository.calls();
            assert_eq!(calls.read_store_diagnostics, 1);
            assert_eq!(calls.latest_ingest_heartbeat, 1);
            assert_eq!(calls.ingest_status, vec![120]);
            worker.join().expect("daemon API worker");
        }
    }

    #[tokio::test]
    async fn oversized_api_response_falls_back_before_buffering_the_body() {
        let repository = test_repository();
        let body = "x".repeat(STATUS_API_MAX_RESPONSE_BYTES + 1);
        let (port, worker) = spawn_api_response(&body, Duration::from_millis(0));
        let status = read_preferred_status(
            &test_config(port),
            &repository,
            true,
            Duration::from_secs(1),
        )
        .await
        .expect("fall back after oversized daemon response");

        assert_eq!(status.source, StatusDataSource::DirectDb);
        assert!(
            status
                .fallback_note
                .as_deref()
                .is_some_and(|note| note.contains("exceeds 262144 bytes")),
            "{:?}",
            status.fallback_note
        );
        let calls = repository.calls();
        assert_eq!(calls.read_store_diagnostics, 1);
        assert_eq!(calls.latest_ingest_heartbeat, 1);
        assert_eq!(calls.ingest_status, vec![120]);
        worker.join().expect("daemon API worker");
    }

    #[tokio::test]
    async fn daemon_api_timeout_is_bounded_without_direct_db_replay() {
        let repository = test_repository();
        let (port, worker) =
            spawn_api_response(&daemon_status_body(true), Duration::from_millis(300));
        let started = Instant::now();
        let error = read_preferred_status(
            &test_config(port),
            &repository,
            true,
            Duration::from_millis(20),
        )
        .await
        .err()
        .expect("daemon transport timeout must remain visible");
        let elapsed = started.elapsed();

        assert!(error.to_string().contains("request http://127.0.0.1:"));
        assert!(
            elapsed < Duration::from_millis(250),
            "API timeout took {elapsed:?}"
        );
        let calls = repository.calls();
        assert_eq!(calls.read_store_diagnostics, 0);
        assert_eq!(calls.latest_ingest_heartbeat, 0);
        assert!(calls.ingest_status.is_empty());
        worker.join().expect("daemon API worker");
    }

    #[tokio::test]
    async fn typed_daemon_failures_remain_visible_without_direct_db_replay() {
        for (status_line, code) in [
            ("499 Client Closed Request", "cancelled"),
            ("504 Gateway Timeout", "deadline_exceeded"),
            ("429 Too Many Requests", "resource_exhausted"),
            ("503 Service Unavailable", "backend_failure"),
        ] {
            let repository = test_repository();
            let body = json!({"ok": false, "code": code, "error": "typed failure"}).to_string();
            let (port, worker) =
                spawn_api_response_with_status(status_line, &body, Duration::from_millis(0));

            let error = read_preferred_status(
                &test_config(port),
                &repository,
                true,
                Duration::from_secs(1),
            )
            .await
            .err()
            .expect("typed daemon failure must remain visible");

            assert!(error.to_string().contains(code), "{error:#}");
            assert!(error.to_string().contains("typed failure"), "{error:#}");
            let calls = repository.calls();
            assert_eq!(calls.read_store_diagnostics, 0);
            assert_eq!(calls.latest_ingest_heartbeat, 0);
            assert!(calls.ingest_status.is_empty());
            worker.join().expect("daemon API worker");
        }
    }

    #[tokio::test]
    async fn legacy_daemon_without_canonical_status_route_uses_direct_db() {
        let repository = test_repository();
        let (port, worker) =
            spawn_api_response_with_status("404 Not Found", "not found", Duration::from_millis(0));
        let status = read_preferred_status(
            &test_config(port),
            &repository,
            true,
            Duration::from_secs(1),
        )
        .await
        .expect("legacy daemon remains compatible");

        assert_eq!(status.source, StatusDataSource::DirectDb);
        assert!(status
            .fallback_note
            .as_deref()
            .is_some_and(|note| note.contains("is incompatible")));
        let calls = repository.calls();
        assert_eq!(calls.read_store_diagnostics, 1);
        assert_eq!(calls.latest_ingest_heartbeat, 1);
        assert_eq!(calls.ingest_status, vec![120]);
        worker.join().expect("daemon API worker");
    }

    #[tokio::test]
    async fn unavailable_daemon_uses_direct_db_without_api_failure_warning() {
        let repository = test_repository();
        let status = read_preferred_status(
            &test_config(9),
            &repository,
            false,
            Duration::from_millis(20),
        )
        .await
        .expect("read direct fallback status");

        assert_eq!(status.source, StatusDataSource::DirectDb);
        assert!(status.fallback_note.is_none());
        let calls = repository.calls();
        assert_eq!(calls.read_store_diagnostics, 1);
        assert_eq!(calls.latest_ingest_heartbeat, 1);
        assert_eq!(calls.ingest_status, vec![120]);
    }

    #[tokio::test]
    async fn healthy_status_preserves_stale_heartbeat_json_output() {
        let status = status_json(Ok(IngestHeartbeatRead {
            table_present: true,
            latest: Some(stale_heartbeat()),
        }))
        .await;

        assert_eq!(status["data_source"], "direct_db");
        assert_eq!(
            status["doctor"],
            json!({
                "clickhouse_healthy": true,
                "clickhouse_version": "25.8.1.1",
                "database": "moraine",
                "database_exists": true,
                "applied_migrations": ["001"],
                "pending_migrations": [],
                "missing_tables": [],
                "errors": []
            })
        );
        assert_eq!(
            status["heartbeat"],
            json!({
                "state": "available",
                "latest": "2000-01-01 00:00:00.000",
                "queue_depth": 7,
                "files_active": 3,
                "watcher_backend": "unknown",
                "watcher_error_count": 0,
                "watcher_reset_count": 0,
                "watcher_last_reset_unix_ms": 0
            })
        );
    }

    #[tokio::test]
    async fn missing_heartbeat_preserves_unavailable_json_output() {
        for table_present in [false, true] {
            let status = status_json(Ok(IngestHeartbeatRead {
                table_present,
                latest: None,
            }))
            .await;
            assert_eq!(
                status["heartbeat"],
                json!({"state": "unavailable"}),
                "table_present={table_present}"
            );
        }
    }

    fn test_doctor_report(clickhouse_healthy: bool) -> DoctorReport {
        DoctorReport {
            clickhouse_healthy,
            clickhouse_version: None,
            database: "moraine".to_string(),
            database_exists: true,
            applied_migrations: Vec::new(),
            pending_migrations: Vec::new(),
            missing_tables: Vec::new(),
            errors: Vec::new(),
        }
    }

    #[test]
    fn build_status_notes_flags_healthy_external_clickhouse() {
        let services = vec![managed_runtime_status(Service::ClickHouse, None)];
        let report = test_doctor_report(true);
        let notes = build_status_notes(&services, &report, "http://127.0.0.1:8123");
        assert_eq!(notes.len(), 1);
        assert!(
            notes[0].contains("endpoint is healthy while managed clickhouse runtime is stopped")
        );
        assert!(notes[0].contains("http://127.0.0.1:8123"));
    }

    #[test]
    fn build_status_notes_flags_unhealthy_managed_clickhouse() {
        let services = vec![managed_runtime_status(Service::ClickHouse, Some(4242))];
        let report = test_doctor_report(false);
        let notes = build_status_notes(&services, &report, "http://127.0.0.1:8123");
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("managed clickhouse runtime is running"));
        assert!(notes[0].contains("are failing"));
        assert!(notes[0].contains("http://127.0.0.1:8123"));
    }

    #[test]
    fn monitor_runtime_url_uses_configured_bind() {
        let mut cfg = AppConfig::default();
        cfg.backend.bind = "127.0.0.1".to_string();
        cfg.monitor.port = 18080;
        assert_eq!(monitor_runtime_url(&cfg), "http://127.0.0.1:18080");
    }

    #[test]
    fn monitor_runtime_url_wraps_ipv6_host() {
        let mut cfg = AppConfig::default();
        cfg.backend.bind = "::1".to_string();
        cfg.monitor.port = 18080;
        assert_eq!(monitor_runtime_url(&cfg), "http://[::1]:18080");
    }

    #[test]
    fn backend_runtime_state_uses_pid_and_both_endpoints() {
        let status = |pid, socket_listening, http_listening| {
            backend_runtime_status(
                pid,
                BackendEndpointStatus {
                    socket_listening,
                    http_listening,
                },
            )
            .state
        };

        assert_eq!(status(Some(200), true, true), ServiceRuntimeState::Running);
        assert_eq!(status(None, true, true), ServiceRuntimeState::Unmanaged);
        assert_eq!(status(None, false, false), ServiceRuntimeState::Stopped);
        assert_eq!(status(Some(200), true, false), ServiceRuntimeState::Partial);
        assert_eq!(status(None, false, true), ServiceRuntimeState::Partial);
    }
}
