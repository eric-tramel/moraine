use anyhow::Result;
use moraine_clickhouse::QueryRuntime;
use moraine_config::AppConfig;
use std::process::ExitCode;

use crate::cli::{ClickhouseArgs, ClickhouseCommand};
use crate::managed_clickhouse::{
    cmd_clickhouse_install, cmd_clickhouse_status, cmd_clickhouse_uninstall,
    run_supervised_clickhouse,
};
use crate::paths::RuntimePaths;
use crate::render::{render_clickhouse_status, state_label, CliOutput};

pub(super) async fn handle(
    cfg: &AppConfig,
    paths: &RuntimePaths,
    args: ClickhouseArgs,
    query_runtime: &QueryRuntime,
) -> Result<ExitCode> {
    match args.command {
        ClickhouseCommand::Install(install) => {
            let output = CliOutput::from_options(install.output.output, false);
            let version = install
                .version
                .unwrap_or_else(|| cfg.runtime.clickhouse_version.clone());
            let installed = cmd_clickhouse_install(paths, &version, install.force).await?;
            if output.is_json() {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "installed_path": installed.display().to_string(),
                        "version": version,
                        "force": install.force,
                    }))?
                );
            } else {
                output.section(
                    "Managed ClickHouse Install",
                    &[
                        format!("installed binary: {}", installed.display()),
                        format!("version: {version}"),
                        format!("force: {}", state_label(install.force)),
                    ],
                );
            }
            Ok(ExitCode::SUCCESS)
        }
        ClickhouseCommand::Status(args) => {
            let output = CliOutput::from_options(args.output, false);
            let snapshot = cmd_clickhouse_status(cfg, paths);
            render_clickhouse_status(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        ClickhouseCommand::Supervise => run_supervised_clickhouse(cfg, paths, query_runtime).await,
        ClickhouseCommand::Uninstall(args) => {
            let output = CliOutput::from_options(args.output, false);
            let removed = cmd_clickhouse_uninstall(paths)?;
            if output.is_json() {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "removed_path": removed
                    }))?
                );
            } else {
                output.section(
                    "Managed ClickHouse Uninstall",
                    &[format!("removed: {removed}")],
                );
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}
