use anyhow::{bail, Result};
use moraine_clickhouse::{ClickHouseClient, DoctorReport, QueryOwner, QueryRuntime, QueryWorkload};
use moraine_config::AppConfig;
use std::process::ExitCode;

use super::{doctor_is_healthy, writer_barrier_required, WRITER_BARRIER_SERVICES};
use crate::cli::{DbArgs, DbCommand};
use crate::paths::{runtime_paths, RuntimePaths};
use crate::process::{lock_storage_migration, service_running_read_only};
use crate::render::{render_db_doctor, render_db_migrate, CliOutput, MigrationOutcome};
use crate::service::Service;

pub(super) async fn handle(
    cfg: &AppConfig,
    args: DbArgs,
    query_runtime: &QueryRuntime,
) -> Result<ExitCode> {
    let output = CliOutput::from_options(args.output.output, false);
    match args.command {
        DbCommand::Migrate => {
            let outcome = migrate(cfg, &runtime_paths(cfg), query_runtime).await?;
            render_db_migrate(&output, &outcome)?;
            Ok(ExitCode::SUCCESS)
        }
        DbCommand::Doctor => {
            let report = doctor(cfg, query_runtime).await?;
            render_db_doctor(&output, &report)?;
            if doctor_is_healthy(&report) {
                Ok(ExitCode::SUCCESS)
            } else {
                Ok(ExitCode::from(1))
            }
        }
    }
}

async fn migrate(
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

async fn doctor(cfg: &AppConfig, query_runtime: &QueryRuntime) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new_with_runtime(cfg.clickhouse.clone(), query_runtime.clone())?;
    let owner = QueryOwner::new(query_runtime, QueryWorkload::Administrative)?;
    owner.scope(ch.doctor_report()).await
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
