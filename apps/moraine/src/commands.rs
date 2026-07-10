mod down;
mod export;
mod logs;
mod schema;
mod setup;
mod status;
mod up;

use anyhow::{bail, Context, Result};
use moraine_clickhouse::{ClickHouseClient, DoctorReport};
use moraine_config::AppConfig;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig};
use std::path::PathBuf;
use std::process::ExitCode;

use crate::cli::{
    Cli, CliCommand, ClickhouseCommand, ConfigCommand, DbCommand, ExportCommand, OutputFormat,
    RunArgs, SchemaCommand,
};
use crate::managed_clickhouse::{
    cmd_clickhouse_install, cmd_clickhouse_status, cmd_clickhouse_uninstall,
    run_foreground_clickhouse, run_supervised_clickhouse,
};
use crate::paths::{load_cfg, runtime_paths};
use crate::process::{require_service_binary, service_args_with_defaults};
use crate::render::{
    render_clickhouse_status, render_db_doctor, render_db_migrate, render_logs, state_label,
    CliOutput, MigrationOutcome,
};
use crate::service::Service;

pub(crate) async fn dispatch(cli: Cli, output: CliOutput) -> Result<ExitCode> {
    match cli.command {
        CliCommand::Up(args) => {
            let (config_path, cfg) = load_cfg(cli.config.clone())?;
            up::handle_args(&output, &config_path, &cfg, &args).await
        }
        CliCommand::Down => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            down::handle(&output, &cfg)
        }
        CliCommand::Status => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let repository = conversation_repository(&cfg)?;
            let snapshot = status::cmd_status(&paths, &cfg, &repository).await?;
            crate::render::render_status(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Logs(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let snapshot = logs::collect_logs(&paths, args.service, args.lines)?;
            render_logs(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Export(args) => {
            if cli.output != OutputFormat::Auto {
                bail!(
                    "moraine export always writes JSONL row data to stdout and metadata to stderr; use --format jsonl instead of global --output"
                );
            }
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                ExportCommand::Events(events) => export::events(&cfg, events).await,
            }
        }
        CliCommand::Schema(args) => match args.command {
            SchemaCommand::Analytics(analytics) => {
                schema::render_analytics(&analytics)?;
                Ok(ExitCode::SUCCESS)
            }
        },
        CliCommand::Db(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                DbCommand::Migrate => {
                    let outcome = cmd_db_migrate(&cfg).await?;
                    render_db_migrate(&output, &outcome)?;
                    Ok(ExitCode::SUCCESS)
                }
                DbCommand::Doctor => {
                    let report = cmd_db_doctor(&cfg).await?;
                    render_db_doctor(&output, &report)?;
                    if doctor_is_healthy(&report) {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
            }
        }
        CliCommand::Clickhouse(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            match args.command {
                ClickhouseCommand::Install(install) => {
                    let version = install
                        .version
                        .unwrap_or_else(|| cfg.runtime.clickhouse_version.clone());
                    let installed = cmd_clickhouse_install(&paths, &version, install.force).await?;
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
                ClickhouseCommand::Status => {
                    let snapshot = cmd_clickhouse_status(&cfg, &paths);
                    render_clickhouse_status(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                ClickhouseCommand::Supervise => run_supervised_clickhouse(&cfg, &paths).await,
                ClickhouseCommand::Uninstall => {
                    let removed = cmd_clickhouse_uninstall(&paths)?;
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
        CliCommand::Config(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                ConfigCommand::Get(get) => {
                    let value = cmd_config_get(&cfg, &get.key)?;
                    if output.is_json() {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "key": get.key,
                                "value": value,
                            }))?
                        );
                    } else {
                        println!("{value}");
                    }
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Setup(args) => setup::handle(&output, cli.config.clone(), args),
        CliCommand::Run(run) => run_service(cli.config.clone(), run).await,
    }
}

async fn run_service(global_config: Option<PathBuf>, run: RunArgs) -> Result<ExitCode> {
    let (inline_config, passthrough) = parse_config_flag(&run.args)?;
    let raw_config = inline_config.or(global_config);
    let (config_path, cfg) = load_cfg(raw_config)?;
    let paths = runtime_paths(&cfg);
    if run.service == Service::ClickHouse {
        return run_foreground_clickhouse(&cfg, &paths).await;
    }

    let binary = require_service_binary(run.service, &paths)?;
    let args = service_args_with_defaults(run.service, &config_path, &cfg, &paths, &passthrough);

    let status = std::process::Command::new(binary)
        .args(args)
        .status()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to run {}", run.service.name()))?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

fn conversation_repository(cfg: &AppConfig) -> Result<ClickHouseConversationRepository> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    Ok(ClickHouseConversationRepository::new(
        ch,
        RepoConfig::default(),
    ))
}

// Deliberate shared-read-layer exception: `db *`/`doctor` are storage administration,
// while `export` owns a versioned row contract and schema-skew gate. Those paths keep
// direct ClickHouse access; operational status reads go through ConversationRepository.

async fn cmd_db_migrate(cfg: &AppConfig) -> Result<MigrationOutcome> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let applied = ch.run_migrations().await?;
    Ok(MigrationOutcome { applied })
}

async fn cmd_db_doctor(cfg: &AppConfig) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ch.doctor_report().await
}

fn parse_config_flag(args: &[String]) -> Result<(Option<PathBuf>, Vec<String>)> {
    let mut raw_config = None;
    let mut rest = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 >= args.len() {
                bail!("--config requires a path");
            }
            raw_config = Some(PathBuf::from(args[i + 1].clone()));
            i += 2;
            continue;
        }

        rest.push(args[i].clone());
        i += 1;
    }

    Ok((raw_config, rest))
}

fn cmd_config_get(cfg: &AppConfig, key: &str) -> Result<String> {
    match key {
        "backend.start_on_up" => Ok(cfg.backend.start_on_up.to_string()),
        "clickhouse.url" => Ok(cfg.clickhouse.url.clone()),
        "clickhouse.database" => Ok(cfg.clickhouse.database.clone()),
        _ => bail!(
            "unsupported config key '{}'; supported keys: backend.start_on_up, clickhouse.url, clickhouse.database",
            key
        ),
    }
}

fn doctor_is_healthy(report: &DoctorReport) -> bool {
    report.clickhouse_healthy
        && report.database_exists
        && report.pending_migrations.is_empty()
        && report.missing_tables.is_empty()
        && report.errors.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::OutputMode;

    fn plain_output() -> CliOutput {
        CliOutput {
            mode: OutputMode::Plain,
            verbose: false,
            unicode: false,
            width: 100,
        }
    }

    #[test]
    fn parse_config_flag_preserves_inline_config_and_rest() {
        let args = vec![
            "--config".to_string(),
            "/tmp/moraine.toml".to_string(),
            "--stdio".to_string(),
        ];
        let (config, rest) = parse_config_flag(&args).expect("parse config");
        assert_eq!(config, Some(PathBuf::from("/tmp/moraine.toml")));
        assert_eq!(rest, vec!["--stdio".to_string()]);
    }

    #[test]
    fn parse_config_flag_rejects_dangling_config() {
        let err = parse_config_flag(&["--config".to_string()]).expect_err("dangling config");
        assert!(err.to_string().contains("--config requires a path"));
    }

    #[test]
    fn cmd_config_get_returns_supported_keys() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "http://127.0.0.1:18123".to_string();
        cfg.clickhouse.database = "analytics".to_string();

        assert_eq!(
            cmd_config_get(&cfg, "clickhouse.url").expect("url"),
            "http://127.0.0.1:18123"
        );
        assert_eq!(
            cmd_config_get(&cfg, "clickhouse.database").expect("database"),
            "analytics"
        );
        cfg.backend.start_on_up = true;
        assert_eq!(
            cmd_config_get(&cfg, "backend.start_on_up").expect("backend switch"),
            "true"
        );
    }

    #[test]
    fn cmd_config_get_rejects_unknown_key() {
        let cfg = AppConfig::default();
        let err = cmd_config_get(&cfg, "runtime.root_dir").expect_err("unknown key");
        assert!(err.to_string().contains("unsupported config key"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_rejects_global_output_for_export_before_loading_config() {
        let cli = Cli {
            config: Some(PathBuf::from("/definitely/missing/moraine.toml")),
            output: OutputFormat::Json,
            verbose: false,
            command: CliCommand::Export(Box::new(crate::cli::ExportArgs {
                command: ExportCommand::Events(crate::cli::ExportEventsArgs {
                    format: crate::cli::ExportRowFormat::Jsonl,
                    columns: None,
                    include_sensitive: false,
                    limit: None,
                    all: true,
                    since: None,
                    until: None,
                    session_id: Vec::new(),
                    harness: Vec::new(),
                    source_name: Vec::new(),
                    project_id: Vec::new(),
                    cwd_prefix: Vec::new(),
                    worktree_root: Vec::new(),
                    repo_rel_path: Vec::new(),
                    event_kind: Vec::new(),
                    payload_type: Vec::new(),
                    actor_kind: Vec::new(),
                    model_name: Vec::new(),
                    tool_name: Vec::new(),
                    tool_error_only: false,
                }),
            })),
        };

        let err = dispatch(cli, plain_output())
            .await
            .expect_err("export must reject explicit output");
        assert!(err.to_string().contains("use --format"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_schema_analytics_is_config_free() {
        let cli = Cli {
            config: Some(PathBuf::from("/definitely/missing/moraine.toml")),
            output: OutputFormat::Auto,
            verbose: false,
            command: CliCommand::Schema(crate::cli::SchemaArgs {
                command: SchemaCommand::Analytics(crate::cli::SchemaAnalyticsArgs { json: true }),
            }),
        };

        let code = dispatch(cli, plain_output())
            .await
            .expect("schema command should not load config");
        assert_eq!(code, ExitCode::SUCCESS);
    }
}
