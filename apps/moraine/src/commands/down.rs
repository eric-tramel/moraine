use anyhow::Result;
use std::fs;
use std::process::ExitCode;

use moraine_config::AppConfig;

use crate::paths::runtime_paths;
use crate::process::{
    backend_endpoint_status, stop_legacy_service, stop_service, LEGACY_MCP_PID_FILE,
    LEGACY_MONITOR_PID_FILE,
};
use crate::render::{render_down, CliOutput, DownSnapshot};
use crate::service::Service;

pub(super) fn handle(output: &CliOutput, cfg: &AppConfig) -> Result<ExitCode> {
    let paths = runtime_paths(cfg);
    let mut stopped = Vec::new();
    for service in stop_order() {
        if stop_service(&paths, service)? {
            stopped.push(service);
        }
        if service == Service::Backend {
            // Retire PID-tracked daemons from the pre-backend topology only
            // after the canonical backend has been stopped.
            let _ = stop_legacy_service(&paths, LEGACY_MCP_PID_FILE)?;
            let _ = stop_legacy_service(&paths, LEGACY_MONITOR_PID_FILE)?;
        }
    }
    let endpoints = backend_endpoint_status(cfg);
    if !endpoints.socket_listening {
        let _ = fs::remove_file(&cfg.mcp.central_socket_path);
    }
    let warning = (endpoints.socket_listening || endpoints.http_listening).then(|| {
        format!(
            "backend endpoint(s) are still listening but are not tracked by a PID file \
             (MCP socket: {}, monitor HTTP: {}), so they were not stopped; \
             stop the owning process manually and re-run `moraine down`",
            endpoints.socket_listening, endpoints.http_listening
        )
    });
    render_down(output, &DownSnapshot { stopped, warning })?;
    Ok(ExitCode::SUCCESS)
}

fn stop_order() -> [Service; 3] {
    [Service::Backend, Service::Ingest, Service::ClickHouse]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn down_stop_order_is_backend_ingest_clickhouse() {
        assert_eq!(
            stop_order(),
            [Service::Backend, Service::Ingest, Service::ClickHouse]
        );
    }
}
