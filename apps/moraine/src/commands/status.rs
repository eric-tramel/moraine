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
use moraine_clickhouse::{DoctorReport, PublicationDiagnostics, QueryClass, QueryEnvelope};
use moraine_config::{AppConfig, ValidatedQueryBudgets};
use moraine_conversations::{ConversationRepository, IngestHeartbeatRead, StoreDiagnostics};
use std::time::Duration;

const STATUS_API_TIMEOUT: Duration = Duration::from_secs(2);
const STATUS_API_MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Debug, serde::Deserialize)]
#[serde(transparent)]
struct RequiredNullable<T>(Option<T>);

#[derive(Debug, serde::Deserialize)]
struct DaemonStatusResponse {
    ok: bool,
    clickhouse: DaemonClickhouseStatus,
    #[serde(default)]
    publication: Option<DaemonPublicationStatus>,
    #[serde(default)]
    query_budgets: Option<DaemonQueryBudgets>,
    database: DaemonDatabaseStatus,
    ingestor: DaemonIngestorStatus,
}

/// Additive `query_budgets` telemetry block from the daemon status/health
/// API (issue #600 W11). Optional end-to-end: pre-envelope daemons omit it
/// and the CLI stays silent rather than falling back to direct DB reads.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
struct DaemonQueryBudgets {
    deadline_exceeded: u64,
    resource_exhausted: u64,
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
struct DaemonPublicationStatus {
    available: bool,
    healthy: bool,
    ambiguous_hostless_rows: Option<u64>,
    replaying_generations: Option<u64>,
    blocked_generations: Option<u64>,
    append_preparations: Option<u64>,
    blocked_append_preparations: Option<u64>,
    mirror_catchup_pending: Option<u64>,
    writer_conflicts: Option<u64>,
    #[serde(default)]
    issues: Vec<String>,
    error: Option<String>,
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
    source: StatusDataSource,
    fallback_note: Option<String>,
    clickhouse_health_url: String,
    /// Daemon-reported budget-exhaustion telemetry; `None` on direct-DB
    /// fallback (the CLI process has no meaningful counters of its own) and
    /// for daemons that predate the block.
    query_budgets: Option<DaemonQueryBudgets>,
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
        "{}/api/v1/status",
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

    let mut publication_error = None;
    let publication = match payload.publication {
        Some(publication) if publication.available => {
            if publication.error.is_some() {
                bail!("daemon API returned available publication diagnostics with an error");
            }
            let diagnostics = PublicationDiagnostics {
                ambiguous_hostless_rows: publication
                    .ambiguous_hostless_rows
                    .context("daemon API omitted ambiguous_hostless_rows")?,
                replaying_generations: publication
                    .replaying_generations
                    .context("daemon API omitted replaying_generations")?,
                blocked_generations: publication
                    .blocked_generations
                    .context("daemon API omitted blocked_generations")?,
                append_preparations: publication
                    .append_preparations
                    .context("daemon API omitted append_preparations")?,
                blocked_append_preparations: publication
                    .blocked_append_preparations
                    .context("daemon API omitted blocked_append_preparations")?,
                mirror_catchup_pending: publication
                    .mirror_catchup_pending
                    .context("daemon API omitted mirror_catchup_pending")?,
                writer_conflicts: publication
                    .writer_conflicts
                    .context("daemon API omitted writer_conflicts")?,
                issues: publication.issues,
            };
            if publication.healthy != diagnostics.is_healthy() {
                bail!("daemon API returned contradictory publication health fields");
            }
            Some(diagnostics)
        }
        Some(publication) => {
            if publication.healthy || publication.error.is_none() {
                bail!("daemon API returned contradictory unavailable publication diagnostics");
            }
            publication_error = publication.error;
            None
        }
        None => None,
    };

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
    let mut errors = payload.clickhouse.error.0.into_iter().collect::<Vec<_>>();
    if let Some(error) = publication_error {
        errors.push(format!("publication diagnostics unavailable: {error}"));
    }
    let report = DoctorReport {
        clickhouse_healthy: payload.clickhouse.healthy,
        clickhouse_version: payload.clickhouse.version.0,
        database: payload.clickhouse.database,
        database_exists: payload.database.exists,
        applied_migrations: Vec::new(),
        pending_migrations: Vec::new(),
        missing_tables: Vec::new(),
        publication,
        errors,
    };

    Ok(StatusData {
        report,
        heartbeat,
        source: StatusDataSource::DaemonApi,
        fallback_note: None,
        clickhouse_health_url: payload.clickhouse.url,
        query_budgets: payload.query_budgets,
    })
}

/// One-line budget summary for `moraine status` (issue #600 W11). `None`
/// (omit silently) when the daemon telemetry is unavailable or when nothing
/// has been rejected — the line exists to flag repeated exhaustion, not to
/// narrate healthy traffic.
fn budget_status_note(query_budgets: Option<DaemonQueryBudgets>) -> Option<String> {
    let budgets = query_budgets?;
    if budgets.deadline_exceeded == 0 && budgets.resource_exhausted == 0 {
        return None;
    }
    Some(format!(
        "query budgets: {} deadline / {} resource rejections since start",
        budgets.deadline_exceeded, budgets.resource_exhausted
    ))
}

async fn read_daemon_status(cfg: &AppConfig, timeout: Duration) -> Result<StatusData> {
    let api_url = monitor_api_status_url(cfg);
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .context("build daemon status API client")?;
    let mut response = client
        .get(&api_url)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .with_context(|| format!("request {api_url}"))?
        .error_for_status()
        .with_context(|| format!("request {api_url}"))?;
    if response
        .content_length()
        .is_some_and(|length| length > STATUS_API_MAX_RESPONSE_BYTES as u64)
    {
        bail!("daemon status API response exceeds {STATUS_API_MAX_RESPONSE_BYTES} bytes");
    }
    let capacity = response
        .content_length()
        .unwrap_or_default()
        .min(STATUS_API_MAX_RESPONSE_BYTES as u64) as usize;
    let mut body = Vec::with_capacity(capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("read {api_url} response"))?
    {
        if chunk.len() > STATUS_API_MAX_RESPONSE_BYTES - body.len() {
            bail!("daemon status API response exceeds {STATUS_API_MAX_RESPONSE_BYTES} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    let payload = serde_json::from_slice(&body)
        .with_context(|| format!("decode {api_url} response as JSON"))?;
    daemon_status_data(payload)
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
    if let Some(publication) = &report.publication {
        if publication.replaying_generations > 0 {
            notes.push(format!(
                "{} source generation(s) are replaying behind published heads",
                publication.replaying_generations
            ));
        }
        if publication.append_preparations > 0 {
            notes.push(format!(
                "{} append preparation(s) are fenced from strict live reads",
                publication.append_preparations
            ));
        }
        if publication.blocked_append_preparations > 0 {
            notes.push(format!(
                "{} append preparation(s) are blocked and remain fail-closed",
                publication.blocked_append_preparations
            ));
        }
        if publication.mirror_catchup_pending > 0 {
            notes.push(format!(
                "{} mirror publication(s) are waiting for catch-up",
                publication.mirror_catchup_pending
            ));
        }
        if !publication.is_healthy() {
            notes.push(format!(
                "publication is degraded (ambiguous hostless rows: {}, blocked generations: {}, blocked append preparations: {}, writer conflicts: {})",
                publication.ambiguous_hostless_rows,
                publication.blocked_generations,
                publication.blocked_append_preparations,
                publication.writer_conflicts
            ));
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
        publication: diagnostics.publication,
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
    budgets: &ValidatedQueryBudgets,
) -> Result<(DoctorReport, HeartbeatSnapshot)> {
    // Status direct-DB reads are an Interactive-class operation (issue #600,
    // amendment A6): one envelope covers both diagnostics reads so they share
    // an absolute deadline and read allowance.
    QueryEnvelope::new_with_admin_budget(
        "status",
        QueryClass::Interactive,
        &budgets.interactive,
        &budgets.administrative,
    )
    .scope(async {
        let report = doctor_report(repository.read_store_diagnostics().await?);
        let heartbeat = match repository.latest_ingest_heartbeat().await {
            Ok(read) => heartbeat_snapshot(read),
            Err(err) => HeartbeatSnapshot::Error {
                message: err.to_string(),
            },
        };
        Ok((report, heartbeat))
    })
    .await
}
async fn read_preferred_status(
    cfg: &AppConfig,
    repository: &dyn ConversationRepository,
    budgets: &ValidatedQueryBudgets,
    api_available: bool,
    timeout: Duration,
) -> Result<StatusData> {
    if api_available {
        match read_daemon_status(cfg, timeout).await {
            Ok(status) => return Ok(status),
            Err(error) => {
                let (report, heartbeat) = read_repository_status(repository, budgets).await?;
                return Ok(StatusData {
                    report,
                    heartbeat,
                    source: StatusDataSource::DirectDb,
                    fallback_note: Some(format!(
                        "daemon status API failed ({error:#}); using direct DB fallback"
                    )),
                    clickhouse_health_url: cfg.clickhouse.url.clone(),
                    query_budgets: None,
                });
            }
        }
    }

    let (report, heartbeat) = read_repository_status(repository, budgets).await?;
    Ok(StatusData {
        report,
        heartbeat,
        source: StatusDataSource::DirectDb,
        fallback_note: None,
        clickhouse_health_url: cfg.clickhouse.url.clone(),
        query_budgets: None,
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
        fallback_note,
        clickhouse_health_url,
        query_budgets,
    } = read_preferred_status(
        cfg,
        repository,
        &super::query_budgets(cfg),
        backend_endpoints.http_listening,
        STATUS_API_TIMEOUT,
    )
    .await?;
    let mut status_notes = build_status_notes(&services, &report, &clickhouse_health_url);
    if let Some(note) = budget_status_note(query_budgets) {
        status_notes.push(note);
    }
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

    fn test_budgets() -> ValidatedQueryBudgets {
        crate::commands::query_budgets(&AppConfig::default())
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
                    publication: Some(PublicationDiagnostics::default()),
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
            "publication": {
                "available": true,
                "healthy": true,
                "ambiguous_hostless_rows": 0,
                "replaying_generations": 0,
                "blocked_generations": 0,
                "append_preparations": 0,
                "blocked_append_preparations": 0,
                "mirror_catchup_pending": 0,
                "writer_conflicts": 0,
                "issues": []
            },
            "query_budgets": {
                "requests": 12,
                "statements": 48,
                "deadline_exceeded": 0,
                "resource_exhausted": 0,
                "unenveloped_statements": 0
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

    #[test]
    fn daemon_publication_compatibility_accepts_legacy_and_zero_counts() {
        let complete: DaemonStatusResponse =
            serde_json::from_str(&daemon_status_body(true)).expect("complete daemon fixture");
        let complete = daemon_status_data(complete).expect("accept zero publication counts");
        assert_eq!(complete.source, StatusDataSource::DaemonApi);
        assert_eq!(
            complete.report.publication,
            Some(PublicationDiagnostics::default())
        );
        assert!(crate::commands::doctor_is_healthy(&complete.report));

        let mut legacy: Value =
            serde_json::from_str(&daemon_status_body(true)).expect("legacy daemon fixture");
        legacy
            .as_object_mut()
            .expect("daemon response object")
            .remove("publication");
        let legacy: DaemonStatusResponse =
            serde_json::from_value(legacy).expect("legacy daemon response schema");
        let legacy = daemon_status_data(legacy).expect("accept legacy daemon response");
        assert_eq!(legacy.source, StatusDataSource::DaemonApi);
        assert!(legacy.report.publication.is_none());
        assert!(!crate::commands::doctor_is_healthy(&legacy.report));
    }

    #[test]
    fn budget_note_prints_only_for_nonzero_daemon_rejections() {
        assert_eq!(budget_status_note(None), None);
        assert_eq!(
            budget_status_note(Some(DaemonQueryBudgets {
                deadline_exceeded: 0,
                resource_exhausted: 0,
            })),
            None
        );
        assert_eq!(
            budget_status_note(Some(DaemonQueryBudgets {
                deadline_exceeded: 3,
                resource_exhausted: 1,
            }))
            .as_deref(),
            Some("query budgets: 3 deadline / 1 resource rejections since start")
        );
    }

    #[test]
    fn daemon_status_data_carries_optional_query_budget_telemetry() {
        // Block present with zero rejections (the shared fixture): captured,
        // but the note stays silent.
        let zeros: DaemonStatusResponse =
            serde_json::from_str(&daemon_status_body(true)).expect("daemon fixture");
        let zeros = daemon_status_data(zeros).expect("daemon status with zero budgets");
        assert!(zeros.query_budgets.is_some());
        assert_eq!(budget_status_note(zeros.query_budgets), None);

        // Nonzero counts surface the one-line summary.
        let mut nonzero: Value =
            serde_json::from_str(&daemon_status_body(true)).expect("daemon fixture");
        nonzero["query_budgets"]["deadline_exceeded"] = json!(2);
        nonzero["query_budgets"]["resource_exhausted"] = json!(5);
        let nonzero: DaemonStatusResponse =
            serde_json::from_value(nonzero).expect("nonzero budget fixture");
        let nonzero = daemon_status_data(nonzero).expect("daemon status with rejections");
        assert_eq!(
            budget_status_note(nonzero.query_budgets).as_deref(),
            Some("query budgets: 2 deadline / 5 resource rejections since start")
        );

        // Pre-envelope daemons omit the whole block: silently absent, and the
        // response still parses (additive contract).
        let mut legacy: Value =
            serde_json::from_str(&daemon_status_body(true)).expect("daemon fixture");
        legacy
            .as_object_mut()
            .expect("daemon response object")
            .remove("query_budgets");
        let legacy: DaemonStatusResponse =
            serde_json::from_value(legacy).expect("legacy daemon response schema");
        let legacy = daemon_status_data(legacy).expect("legacy daemon status");
        assert!(legacy.query_budgets.is_none());
        assert_eq!(budget_status_note(legacy.query_budgets), None);
    }

    fn spawn_api_response(body: &str, delay: Duration) -> (u16, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind daemon API fixture");
        let port = listener.local_addr().expect("daemon API address").port();
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
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
            String::from_utf8_lossy(&request[..request_len]).into_owned()
        });
        (port, worker)
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
                    publication: Some(PublicationDiagnostics::default()),
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
            &test_budgets(),
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
        assert!(status
            .report
            .publication
            .as_ref()
            .is_some_and(PublicationDiagnostics::is_healthy));
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
        let request = worker.join().expect("daemon API worker");
        assert!(
            request.starts_with("GET /api/v1/status HTTP/1.1"),
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
                &test_budgets(),
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
            let request = worker.join().expect("daemon API worker");
            assert!(request.starts_with("GET /api/v1/status HTTP/1.1"));
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
                &test_budgets(),
                true,
                Duration::from_secs(1),
            )
            .await
            .expect("fall back after contradictory daemon response");

            assert_eq!(status.source, StatusDataSource::DirectDb);
            let calls = repository.calls();
            assert_eq!(calls.read_store_diagnostics, 1);
            assert_eq!(calls.latest_ingest_heartbeat, 1);
            worker.join().expect("daemon API worker");
        }
    }

    #[tokio::test]
    async fn contradictory_publication_fields_fall_back_to_direct_db() {
        let mut healthy_with_block =
            serde_json::from_str::<Value>(&daemon_status_body(true)).expect("valid fixture");
        healthy_with_block["publication"]["blocked_generations"] = json!(1);
        let mut unavailable_without_error =
            serde_json::from_str::<Value>(&daemon_status_body(true)).expect("valid fixture");
        unavailable_without_error["publication"]["available"] = json!(false);
        unavailable_without_error["publication"]["healthy"] = json!(false);

        for payload in [healthy_with_block, unavailable_without_error] {
            let repository = test_repository();
            let (port, worker) = spawn_api_response(&payload.to_string(), Duration::ZERO);
            let status = read_preferred_status(
                &test_config(port),
                &repository,
                &test_budgets(),
                true,
                Duration::from_secs(1),
            )
            .await
            .expect("fall back after contradictory publication response");

            assert_eq!(status.source, StatusDataSource::DirectDb);
            assert_eq!(repository.calls().read_store_diagnostics, 1);
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
            &test_budgets(),
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
        worker.join().expect("daemon API worker");
    }

    #[tokio::test]
    async fn daemon_api_timeout_is_bounded_and_falls_back() {
        let repository = test_repository();
        let (port, worker) =
            spawn_api_response(&daemon_status_body(true), Duration::from_millis(300));
        let started = Instant::now();
        let status = read_preferred_status(
            &test_config(port),
            &repository,
            &test_budgets(),
            true,
            Duration::from_millis(20),
        )
        .await
        .expect("fall back after daemon timeout");
        let elapsed = started.elapsed();

        assert_eq!(status.source, StatusDataSource::DirectDb);
        assert!(
            elapsed < Duration::from_millis(250),
            "API timeout took {elapsed:?}"
        );
        let calls = repository.calls();
        assert_eq!(calls.read_store_diagnostics, 1);
        assert_eq!(calls.latest_ingest_heartbeat, 1);
        worker.join().expect("daemon API worker");
    }

    #[tokio::test]
    async fn unavailable_daemon_uses_direct_db_without_api_failure_warning() {
        let repository = test_repository();
        let status = read_preferred_status(
            &test_config(9),
            &repository,
            &test_budgets(),
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
                "publication": {
                    "ambiguous_hostless_rows": 0,
                    "replaying_generations": 0,
                    "blocked_generations": 0,
                    "append_preparations": 0,
                    "blocked_append_preparations": 0,
                    "mirror_catchup_pending": 0,
                    "writer_conflicts": 0,
                    "issues": []
                },
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
            publication: Some(PublicationDiagnostics::default()),
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
    fn build_status_notes_reports_publication_progress_and_degradation() {
        let mut report = test_doctor_report(true);
        report.publication = Some(PublicationDiagnostics {
            ambiguous_hostless_rows: 2,
            replaying_generations: 3,
            blocked_generations: 1,
            append_preparations: 4,
            blocked_append_preparations: 2,
            mirror_catchup_pending: 5,
            writer_conflicts: 1,
            issues: Vec::new(),
        });

        let notes = build_status_notes(
            &[managed_runtime_status(Service::ClickHouse, Some(4242))],
            &report,
            "http://127.0.0.1:8123",
        );
        assert!(notes
            .iter()
            .any(|note| note.contains("3 source generation")));
        assert!(notes
            .iter()
            .any(|note| note.contains("4 append preparation")));
        assert!(notes
            .iter()
            .any(|note| note.contains("2 append preparation")));
        assert!(notes
            .iter()
            .any(|note| note.contains("5 mirror publication")));
        assert!(notes
            .iter()
            .any(|note| note.contains("publication is degraded")));
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
