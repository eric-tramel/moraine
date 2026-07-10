use crate::managed_clickhouse::{
    active_clickhouse_source, managed_clickhouse_bin, managed_clickhouse_checksum_state,
    managed_clickhouse_version,
};
use crate::paths::RuntimePaths;
use crate::process::{
    backend_endpoint_status, legacy_service_running, service_running, BackendEndpointStatus,
    LEGACY_MCP_PID_FILE, LEGACY_MONITOR_PID_FILE,
};
use crate::render::{HeartbeatSnapshot, ServiceRuntimeState, ServiceRuntimeStatus, StatusSnapshot};
use crate::service::Service;
use anyhow::Result;
use moraine_clickhouse::DoctorReport;
use moraine_config::AppConfig;
use moraine_conversations::{ConversationRepository, IngestHeartbeatRead, StoreDiagnostics};

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
    format_http_url(&cfg.monitor.host, cfg.monitor.port)
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
) -> Result<(DoctorReport, HeartbeatSnapshot)> {
    let report = doctor_report(repository.read_store_diagnostics().await?);
    let heartbeat = match repository.latest_ingest_heartbeat().await {
        Ok(read) => heartbeat_snapshot(read),
        Err(err) => HeartbeatSnapshot::Error {
            message: err.to_string(),
        },
    };
    Ok((report, heartbeat))
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
            service_running(paths, Service::ClickHouse),
        ),
        managed_runtime_status(Service::Ingest, service_running(paths, Service::Ingest)),
        backend_runtime_status(service_running(paths, Service::Backend), backend_endpoints),
    ];
    let managed_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let (source, source_path) = active_clickhouse_source(paths);
    let (report, heartbeat) = read_repository_status(repository).await?;
    let clickhouse_health_url = cfg.clickhouse.url.clone();
    let mut status_notes = build_status_notes(&services, &report, &clickhouse_health_url);
    for (name, pid_file) in [
        ("monitor", LEGACY_MONITOR_PID_FILE),
        ("MCP", LEGACY_MCP_PID_FILE),
    ] {
        if let Some(pid) = legacy_service_running(paths, pid_file) {
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
        cfg.monitor.host = "127.0.0.1".to_string();
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
        }
    }

    #[tokio::test]
    async fn healthy_status_preserves_stale_heartbeat_json_output() {
        let status = status_json(Ok(IngestHeartbeatRead {
            table_present: true,
            latest: Some(stale_heartbeat()),
        }))
        .await;

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
        cfg.monitor.host = "127.0.0.1".to_string();
        cfg.monitor.port = 18080;
        assert_eq!(monitor_runtime_url(&cfg), "http://127.0.0.1:18080");
    }

    #[test]
    fn monitor_runtime_url_wraps_ipv6_host() {
        let mut cfg = AppConfig::default();
        cfg.monitor.host = "::1".to_string();
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
