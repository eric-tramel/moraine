use anyhow::Result;
use moraine_config::AppConfig;
use std::process::ExitCode;

use crate::cli::UpArgs;
use crate::managed_clickhouse::start_clickhouse;
use crate::paths::{ensure_runtime_dirs, runtime_paths, RuntimePaths};
use crate::process::{preflight_required_service_binaries, start_background_service};
use crate::render::{render_up, CliOutput, UpSnapshot};
use crate::service::Service;

use super::{cmd_db_migrate, conversation_repository, status::cmd_status};

pub(super) async fn handle_args(
    output: &CliOutput,
    config_path: &std::path::Path,
    cfg: &AppConfig,
    args: &UpArgs,
) -> Result<ExitCode> {
    let paths = runtime_paths(cfg);
    let services_to_start = selected_up_services(args, cfg);
    start_selected_services(output, config_path, cfg, &paths, services_to_start).await
}

async fn start_selected_services(
    output: &CliOutput,
    config_path: &std::path::Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    services_to_start: Vec<Service>,
) -> Result<ExitCode> {
    preflight_required_service_binaries(&services_to_start, paths)?;
    ensure_runtime_dirs(paths)?;

    let clickhouse = start_clickhouse(config_path, cfg, paths).await?;
    let migrations = cmd_db_migrate(cfg).await?;

    let mut started_services = Vec::new();
    for service in services_to_start {
        let extra_args = up_service_extra_args(service, cfg);
        started_services.push(start_background_service(
            service,
            config_path,
            cfg,
            paths,
            &extra_args,
        )?);
    }

    let repository = conversation_repository(cfg)?;
    let status = cmd_status(paths, cfg, &repository).await?;
    let snapshot = UpSnapshot {
        clickhouse,
        migrations,
        services: started_services,
        status,
    };
    render_up(output, &snapshot)?;
    Ok(ExitCode::SUCCESS)
}

fn selected_up_services(args: &UpArgs, cfg: &AppConfig) -> Vec<Service> {
    let mut services = Vec::new();
    if !args.no_ingest {
        services.push(Service::Ingest);
    }
    if args.monitor || cfg.runtime.start_monitor_on_up {
        services.push(Service::Monitor);
    }
    if args.mcp || cfg.runtime.start_mcp_on_up || cfg.mcp.start_central_on_up {
        services.push(Service::Mcp);
    }
    services
}

fn up_service_extra_args(service: Service, cfg: &AppConfig) -> Vec<String> {
    if service == Service::Mcp {
        vec![
            "--serve".to_string(),
            "socket".to_string(),
            "--socket".to_string(),
            cfg.mcp.central_socket_path.clone(),
        ]
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_up_services_preserves_order_and_flags() {
        let mut cfg = AppConfig::default();
        cfg.runtime.start_monitor_on_up = false;
        cfg.runtime.start_mcp_on_up = false;
        cfg.mcp.start_central_on_up = false;

        assert_eq!(
            selected_up_services(
                &UpArgs {
                    no_ingest: false,
                    monitor: false,
                    mcp: false,
                },
                &cfg
            ),
            vec![Service::Ingest]
        );

        assert_eq!(
            selected_up_services(
                &UpArgs {
                    no_ingest: true,
                    monitor: true,
                    mcp: true,
                },
                &cfg
            ),
            vec![Service::Monitor, Service::Mcp]
        );
    }

    #[test]
    fn up_injects_mcp_socket_server_args_only_for_mcp() {
        let mut cfg = AppConfig::default();
        cfg.mcp.central_socket_path = "/tmp/moraine-test.sock".to_string();

        assert_eq!(
            up_service_extra_args(Service::Mcp, &cfg),
            vec![
                "--serve".to_string(),
                "socket".to_string(),
                "--socket".to_string(),
                "/tmp/moraine-test.sock".to_string(),
            ]
        );
        assert!(up_service_extra_args(Service::Ingest, &cfg).is_empty());
    }
}
