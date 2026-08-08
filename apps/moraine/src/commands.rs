mod clickhouse;
mod config;
mod db;
mod down;
mod export;
mod logs;
mod run;
mod schema;
mod setup;
mod status;
mod up;

use anyhow::Result;
use moraine_clickhouse::{
    ClickHouseClient, DoctorReport, MigrationProgress, QueryOwner, QueryRuntime, QueryWorkload,
};
use moraine_config::AppConfig;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig};
use std::process::ExitCode;

use crate::cli::{Cli, CliCommand, ExportCommand, SchemaCommand};
use crate::paths::{load_cfg, runtime_paths};
use crate::render::{render_logs, CliOutput, MigrationOutcome};
use crate::service::Service;

pub(super) const WRITER_BARRIER_MIGRATIONS: [&str; 2] = ["031", "033"];
pub(super) const WRITER_BARRIER_SERVICES: [Service; 3] =
    [Service::Backend, Service::Ingest, Service::Mcp];

pub(crate) async fn dispatch(cli: Cli, query_runtime: &QueryRuntime) -> Result<ExitCode> {
    let Cli { config, command } = cli;
    match command {
        CliCommand::Up(args) => {
            let output = CliOutput::from_options(args.render.output.output, args.render.verbose);
            let (config_path, cfg) = load_cfg(config)?;
            up::handle_args(&output, &config_path, &cfg, &args, query_runtime).await
        }
        CliCommand::Down(args) => {
            let output = CliOutput::from_options(args.output, false);
            let (_, cfg) = load_cfg(config)?;
            down::handle(&output, &cfg)
        }
        CliCommand::Status(args) => {
            let output = CliOutput::from_options(args.output.output, args.verbose);
            let (_, cfg) = load_cfg(config)?;
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
            let output = CliOutput::from_options(args.output.output, false);
            let (_, cfg) = load_cfg(config)?;
            let paths = runtime_paths(&cfg);
            let snapshot = logs::collect_logs(&paths, args.service.map(Into::into), args.lines)?;
            render_logs(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Export(args) => {
            let (_, cfg) = load_cfg(config)?;
            match args.command {
                ExportCommand::Events(events) => export::events(&cfg, events, query_runtime).await,
            }
        }
        CliCommand::Schema(args) => match args.command {
            SchemaCommand::Analytics => {
                schema::render_analytics()?;
                Ok(ExitCode::SUCCESS)
            }
        },
        CliCommand::Db(args) => {
            let (_, cfg) = load_cfg(config)?;
            db::handle(&cfg, args, query_runtime).await
        }
        CliCommand::Clickhouse(args) => {
            let (_, cfg) = load_cfg(config)?;
            let paths = runtime_paths(&cfg);
            clickhouse::handle(&cfg, &paths, args, query_runtime).await
        }
        CliCommand::Config(args) => {
            let (_, cfg) = load_cfg(config)?;
            config::handle(&cfg, args)
        }
        CliCommand::Setup(args) => {
            let output = CliOutput::from_options(args.render.output.output, args.render.verbose);
            setup::handle(&output, config, args.command).await
        }
        CliCommand::Run(args) => run::handle(config, args).await,
    }
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
    use std::path::PathBuf;

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_schema_analytics_is_config_free() {
        let cli = Cli {
            config: Some(PathBuf::from("/definitely/missing/moraine.toml")),
            command: CliCommand::Schema(crate::cli::SchemaArgs {
                command: SchemaCommand::Analytics,
            }),
        };

        let code = dispatch(cli, &QueryRuntime::new())
            .await
            .expect("schema command should not load config");
        assert_eq!(code, ExitCode::SUCCESS);
    }
}
