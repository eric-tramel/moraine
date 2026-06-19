use anyhow::Result;
use moraine_clickhouse::{ClickHouseClient, DoctorReport};
use moraine_config::AppConfig;
use serde::Deserialize;

use super::cmd_db_doctor;
use crate::managed_clickhouse::{
    active_clickhouse_source, managed_clickhouse_bin, managed_clickhouse_checksum_state,
    managed_clickhouse_version,
};
use crate::paths::RuntimePaths;
use crate::process::service_running;
use crate::render::{HeartbeatSnapshot, ServiceRuntimeStatus, StatusSnapshot};
use crate::service::Service;

#[derive(Debug, Deserialize)]
struct HeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
    #[serde(default)]
    watcher_backend: String,
    #[serde(default)]
    watcher_error_count: u64,
    #[serde(default)]
    watcher_reset_count: u64,
    #[serde(default)]
    watcher_last_reset_unix_ms: u64,
}

#[derive(Debug, Deserialize)]
struct LegacyHeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
}

async fn query_heartbeat(cfg: &AppConfig) -> Result<Option<HeartbeatRow>> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let query = format!(
        "SELECT \
            toString(max(ts)) AS latest, \
            toUInt64(argMax(queue_depth, ts)) AS queue_depth, \
            toUInt64(argMax(files_active, ts)) AS files_active, \
            toString(argMax(watcher_backend, ts)) AS watcher_backend, \
            toUInt64(argMax(watcher_error_count, ts)) AS watcher_error_count, \
            toUInt64(argMax(watcher_reset_count, ts)) AS watcher_reset_count, \
            toUInt64(argMax(watcher_last_reset_unix_ms, ts)) AS watcher_last_reset_unix_ms \
         FROM {db}.ingest_heartbeats"
    );

    match ch.query_json_data::<HeartbeatRow>(&query, None).await {
        Ok(rows) => Ok(rows.into_iter().next()),
        Err(_) => {
            let legacy_query = format!(
                "SELECT toString(max(ts)) AS latest, toUInt64(argMax(queue_depth, ts)) AS queue_depth, toUInt64(argMax(files_active, ts)) AS files_active FROM {db}.ingest_heartbeats"
            );
            let rows: Vec<LegacyHeartbeatRow> = ch.query_json_data(&legacy_query, None).await?;
            Ok(rows.into_iter().next().map(|row| HeartbeatRow {
                latest: row.latest,
                queue_depth: row.queue_depth,
                files_active: row.files_active,
                watcher_backend: "unknown".to_string(),
                watcher_error_count: 0,
                watcher_reset_count: 0,
                watcher_last_reset_unix_ms: 0,
            }))
        }
    }
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
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

fn monitor_runtime_running(services: &[ServiceRuntimeStatus]) -> bool {
    service_runtime_running(services, Service::Monitor)
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

    notes
}

pub(super) async fn cmd_status(paths: &RuntimePaths, cfg: &AppConfig) -> Result<StatusSnapshot> {
    let services = [
        Service::ClickHouse,
        Service::Ingest,
        Service::Monitor,
        Service::Mcp,
    ]
    .iter()
    .copied()
    .map(|service| ServiceRuntimeStatus {
        service,
        pid: service_running(paths, service),
    })
    .collect::<Vec<_>>();
    let managed_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let (source, source_path) = active_clickhouse_source(paths);
    let report = cmd_db_doctor(cfg).await?;
    let clickhouse_health_url = cfg.clickhouse.url.clone();
    let status_notes = build_status_notes(&services, &report, &clickhouse_health_url);
    let monitor_url = monitor_runtime_running(&services).then(|| monitor_runtime_url(cfg));
    let heartbeat = match query_heartbeat(cfg).await {
        Ok(Some(row)) => HeartbeatSnapshot::Available {
            latest: row.latest,
            queue_depth: row.queue_depth,
            files_active: row.files_active,
            watcher_backend: row.watcher_backend,
            watcher_error_count: row.watcher_error_count,
            watcher_reset_count: row.watcher_reset_count,
            watcher_last_reset_unix_ms: row.watcher_last_reset_unix_ms,
        },
        Ok(None) => HeartbeatSnapshot::Unavailable,
        Err(err) => HeartbeatSnapshot::Error {
            message: err.to_string(),
        },
    };

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
        let services = vec![ServiceRuntimeStatus {
            service: Service::ClickHouse,
            pid: None,
        }];
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
        let services = vec![ServiceRuntimeStatus {
            service: Service::ClickHouse,
            pid: Some(4242),
        }];
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
    fn monitor_runtime_running_checks_monitor_pid() {
        let services = vec![
            ServiceRuntimeStatus {
                service: Service::ClickHouse,
                pid: Some(100),
            },
            ServiceRuntimeStatus {
                service: Service::Monitor,
                pid: Some(200),
            },
        ];
        assert!(monitor_runtime_running(&services));

        let stopped_monitor = vec![ServiceRuntimeStatus {
            service: Service::Monitor,
            pid: None,
        }];
        assert!(!monitor_runtime_running(&stopped_monitor));
    }
}
