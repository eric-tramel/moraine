mod down;
mod export;
mod logs;
mod schema;
mod setup;
mod status;
mod up;

use anyhow::{bail, Context, Result};
use moraine_clickhouse::{
    ClickHouseClient, DoctorReport, MigrationProgress, QueryOwner, QueryRuntime, QueryWorkload,
};
use moraine_config::AppConfig;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use crate::cli::{
    Cli, CliCommand, ClickhouseCommand, ConfigCommand, DbCommand, ExportCommand, OutputFormat,
    RunArgs, SchemaCommand,
};
use crate::managed_clickhouse::{
    cmd_clickhouse_install, cmd_clickhouse_status, cmd_clickhouse_uninstall,
    run_foreground_clickhouse, run_supervised_clickhouse,
};
use crate::paths::{load_cfg, runtime_paths, RuntimePaths};
use crate::process::{
    lock_storage_migration, lock_storage_writer_launch, pid_path, remove_pid_if_matches,
    require_service_binary, service_args_with_defaults, service_running_read_only,
    write_pid_exclusive, StorageGateGuard,
};
use crate::render::{
    render_clickhouse_status, render_db_doctor, render_db_migrate, render_logs, state_label,
    CliOutput, MigrationOutcome,
};
use crate::service::Service;

pub(super) const WRITER_BARRIER_MIGRATIONS: [&str; 2] = ["031", "033"];
pub(super) const WRITER_BARRIER_SERVICES: [Service; 3] =
    [Service::Backend, Service::Ingest, Service::Mcp];

pub(crate) async fn dispatch(
    cli: Cli,
    output: CliOutput,
    query_runtime: &QueryRuntime,
) -> Result<ExitCode> {
    match cli.command {
        CliCommand::Up(args) => {
            let (config_path, cfg) = load_cfg(cli.config.clone())?;
            up::handle_args(&output, &config_path, &cfg, &args, query_runtime).await
        }
        CliCommand::Down => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            down::handle(&output, &cfg)
        }
        CliCommand::Status => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let repository = conversation_repository(&cfg, query_runtime)?;
            let owner = QueryOwner::new(query_runtime, QueryWorkload::Administrative)?;
            let snapshot = owner
                .scope(status::cmd_status(&paths, &cfg, &repository))
                .await?;
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
                ExportCommand::Events(events) => export::events(&cfg, events, query_runtime).await,
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
                    let outcome = cmd_db_migrate(&cfg, &runtime_paths(&cfg), query_runtime).await?;
                    render_db_migrate(&output, &outcome)?;
                    Ok(ExitCode::SUCCESS)
                }
                DbCommand::Doctor => {
                    let report = cmd_db_doctor(&cfg, query_runtime).await?;
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
                ClickhouseCommand::Supervise => {
                    run_supervised_clickhouse(&cfg, &paths, query_runtime).await
                }
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

    let writer_gate = lock_storage_writer_launch(&paths, run.service)?;
    let binary = require_service_binary(run.service, &paths)?;
    let args = service_args_with_defaults(
        run.service,
        config_path.as_path(),
        &cfg,
        &paths,
        &passthrough,
    );

    let mut command = Command::new(binary);
    command.args(args);
    if let Some((key, value)) = config_path.child_origin_environment() {
        command.env(key, value);
    }
    run_registered_foreground_child(
        command,
        &pid_path(&paths, run.service),
        run.service.name(),
        writer_gate,
    )
}

fn run_registered_foreground_child(
    mut command: Command,
    pid_file: &Path,
    service_name: &str,
    writer_gate: Option<StorageGateGuard>,
) -> Result<ExitCode> {
    if let Some(parent) = pid_file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create PID directory {}", parent.display()))?;
    }
    let mut child = command
        .spawn()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to run {service_name}"))?;
    let child_pid = child.id();
    if let Err(error) = write_pid_exclusive(pid_file, child_pid) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    drop(writer_gate);
    let result = child
        .wait()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to wait for {service_name}"));
    remove_pid_if_matches(pid_file, child_pid);
    let status = result?;
    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

fn conversation_repository(
    cfg: &AppConfig,
    query_runtime: &QueryRuntime,
) -> Result<ClickHouseConversationRepository> {
    let ch = ClickHouseClient::new_with_runtime(cfg.clickhouse.clone(), query_runtime.clone())?;
    Ok(ClickHouseConversationRepository::new(
        ch,
        RepoConfig::default(),
    ))
}

// Deliberate shared-read-layer exception: `db *`/`doctor` are storage administration,
// while `export` owns a versioned row contract and schema-skew gate. Those paths keep
// direct ClickHouse access; operational status reads go through ConversationRepository.

pub(super) fn writer_barrier_required(missing_migrations: &[String]) -> bool {
    missing_migrations
        .iter()
        .any(|version| WRITER_BARRIER_MIGRATIONS.contains(&version.as_str()))
}

fn ensure_standalone_migration_quiescent_with<F>(
    missing_migrations: &[String],
    mut running_pid: F,
) -> Result<()>
where
    F: FnMut(Service) -> Option<u32>,
{
    if !writer_barrier_required(missing_migrations) {
        return Ok(());
    }

    let active = WRITER_BARRIER_SERVICES
        .into_iter()
        .filter_map(|service| {
            running_pid(service).map(|pid| format!("{} (pid {pid})", service.name()))
        })
        .collect::<Vec<_>>();
    if !active.is_empty() {
        bail!(
            "cannot apply a canonical storage migration while tracked Moraine services are running: {}; \
             run `moraine down` first so the storage cutover snapshot is quiescent",
            active.join(", ")
        );
    }

    Ok(())
}

async fn migrate_database_with_progress<F>(
    cfg: &AppConfig,
    query_runtime: &QueryRuntime,
    mut on_progress: F,
) -> Result<MigrationOutcome>
where
    F: FnMut(MigrationProgress),
{
    let ch = ClickHouseClient::new_with_runtime(cfg.clickhouse.clone(), query_runtime.clone())?;
    let owner = QueryOwner::new(query_runtime, QueryWorkload::Migration)?;
    let applied = owner
        .scope(ch.run_migrations_with_progress(|event| {
            on_progress(event);
        }))
        .await?;
    Ok(MigrationOutcome { applied })
}

pub(super) async fn migrate_database_for_up<F>(
    cfg: &AppConfig,
    query_runtime: &QueryRuntime,
    on_progress: F,
) -> Result<MigrationOutcome>
where
    F: FnMut(MigrationProgress),
{
    migrate_database_with_progress(cfg, query_runtime, on_progress).await
}

async fn cmd_db_migrate(
    cfg: &AppConfig,
    paths: &RuntimePaths,
    query_runtime: &QueryRuntime,
) -> Result<MigrationOutcome> {
    let ch = ClickHouseClient::new_with_runtime(cfg.clickhouse.clone(), query_runtime.clone())?;
    let owner = QueryOwner::new(query_runtime, QueryWorkload::Migration)?;
    owner
        .scope(async {
            let schema_skew = ch.schema_skew().await?;
            let _migration_gate = writer_barrier_required(&schema_skew.missing_on_server)
                .then(|| lock_storage_migration(paths))
                .transpose()?;
            ensure_standalone_migration_quiescent_with(
                &schema_skew.missing_on_server,
                |service| service_running_read_only(paths, service),
            )?;
            let applied = ch.run_migrations().await?;
            Ok(MigrationOutcome { applied })
        })
        .await
}

async fn cmd_db_doctor(cfg: &AppConfig, query_runtime: &QueryRuntime) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new_with_runtime(cfg.clickhouse.clone(), query_runtime.clone())?;
    let owner = QueryOwner::new(query_runtime, QueryWorkload::Administrative)?;
    owner.scope(ch.doctor_report()).await
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

        if let Some(path) = args[i].strip_prefix("--config=") {
            if path.is_empty() {
                bail!("--config requires a path");
            }
            raw_config = Some(PathBuf::from(path));
            i += 1;
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

pub(super) fn doctor_is_healthy(report: &DoctorReport) -> bool {
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
    fn parse_config_flag_supports_equals_form_and_argument_order() {
        let args = vec![
            "--config=/tmp/first.toml".to_string(),
            "--config".to_string(),
            "/tmp/second.toml".to_string(),
            "--host=127.0.0.1".to_string(),
        ];
        let (config, rest) = parse_config_flag(&args).expect("parse config");
        assert_eq!(config, Some(PathBuf::from("/tmp/second.toml")));
        assert_eq!(rest, vec!["--host=127.0.0.1".to_string()]);

        let err = parse_config_flag(&["--config=".to_string()]).expect_err("empty equals config");
        assert!(err.to_string().contains("--config requires a path"));
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

    #[test]
    fn cmd_config_get_rejects_backend_auth_token_without_exposing_value() {
        const TOKEN_SENTINEL: &str = "moraine-secret-token-sentinel-462";
        let mut cfg = AppConfig::default();
        cfg.backend.auth_token = Some(TOKEN_SENTINEL.to_string());

        let err = cmd_config_get(&cfg, "backend.auth_token").expect_err("secret key");
        let message = err.to_string();
        assert!(message.contains("unsupported config key"));
        assert!(!message.contains(TOKEN_SENTINEL));
    }

    #[test]
    fn standalone_migration_requires_quiescence_for_storage_cutovers() {
        ensure_standalone_migration_quiescent_with(&["032".to_string()], |_| {
            panic!("non-cutover migrations must not inspect service PIDs")
        })
        .expect("032 alone needs no writer barrier");

        for migration in ["031", "033"] {
            let mut inspected = Vec::new();
            let error =
                ensure_standalone_migration_quiescent_with(&[migration.to_string()], |service| {
                    inspected.push(service);
                    match service {
                        Service::Backend => Some(41),
                        Service::Ingest => Some(42),
                        Service::Mcp => Some(43),
                        Service::ClickHouse => None,
                    }
                })
                .expect_err("live tracked services must block a storage cutover");

            assert_eq!(
                inspected,
                vec![Service::Backend, Service::Ingest, Service::Mcp]
            );
            assert!(error.to_string().contains("backend (pid 41)"));
            assert!(error.to_string().contains("ingest (pid 42)"));
            assert!(error.to_string().contains("mcp (pid 43)"));
            assert!(error.to_string().contains("moraine down"));
        }
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

        let err = dispatch(cli, plain_output(), &QueryRuntime::new())
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

        let code = dispatch(cli, plain_output(), &QueryRuntime::new())
            .await
            .expect("schema command should not load config");
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[cfg(unix)]
    #[test]
    fn foreground_child_is_registered_for_storage_cutover_barriers() {
        let root =
            std::env::temp_dir().join(format!("moraine-foreground-pid-{}", std::process::id()));
        let pid_file = root.join("run/ingest.pid");
        let mut command = Command::new("/bin/sh");
        command
            .env("PID_FILE", &pid_file)
            .args(["-c", "sleep 0.1; test \"$(cat \"$PID_FILE\")\" = \"$$\""]);

        let code = run_registered_foreground_child(command, &pid_file, "ingest", None)
            .expect("run registered foreground child");

        assert_eq!(code, ExitCode::SUCCESS);
        assert!(
            !pid_file.exists(),
            "completed child must remove its PID file"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
