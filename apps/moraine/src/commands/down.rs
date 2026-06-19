use anyhow::Result;
use std::fs;
use std::process::ExitCode;

use moraine_config::AppConfig;

use crate::paths::runtime_paths;
use crate::process::{central_socket_live, stop_service};
use crate::render::{render_down, CliOutput, DownSnapshot};
use crate::service::Service;

pub(super) fn handle(output: &CliOutput, cfg: &AppConfig) -> Result<ExitCode> {
    let paths = runtime_paths(cfg);
    let mut stopped = Vec::new();
    for service in stop_order() {
        if stop_service(&paths, service)? {
            stopped.push(service);
        }
    }
    let warning = if central_socket_live(&cfg.mcp.central_socket_path) {
        Some(format!(
            "an MCP server is still listening on {} but is not tracked by a \
             PID file, so it was not stopped and its socket was left in place; \
             stop that process manually and re-run `moraine down`",
            cfg.mcp.central_socket_path
        ))
    } else {
        let _ = fs::remove_file(&cfg.mcp.central_socket_path);
        None
    };
    render_down(output, &DownSnapshot { stopped, warning })?;
    Ok(ExitCode::SUCCESS)
}

fn stop_order() -> [Service; 4] {
    [
        Service::Mcp,
        Service::Monitor,
        Service::Ingest,
        Service::ClickHouse,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn down_stop_order_is_mcp_monitor_ingest_clickhouse() {
        assert_eq!(
            stop_order(),
            [
                Service::Mcp,
                Service::Monitor,
                Service::Ingest,
                Service::ClickHouse,
            ]
        );
    }
}
