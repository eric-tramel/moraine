mod down;
mod export;
mod logs;
mod schema;
mod setup;
mod status;
mod up;

use anyhow::{bail, Context, Result};
use moraine_clickhouse::{
    ClickHouseClient, CoreIndexAuditOutcome, CoreIndexBackfillProgress, DoctorReport,
    MigrationProgress, OpenV2PromotionOutcome, PublicationDiagnostics, QueryClass, QueryEnvelope,
    ReadIndexState, STATE_KEY_CORE_INDEXES, STATE_KEY_OPEN_V2,
};
use moraine_config::{
    AppConfig, OpenReaderMode, OpenReaderResolution, QueryBudgetsConfig, ValidatedQueryBudgets,
};
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{
    Cli, CliCommand, ClickhouseCommand, ConfigCommand, CoreIndexCommand, DbCommand, ExportCommand,
    OutputFormat, RunArgs, SchemaCommand,
};
use crate::managed_clickhouse::{
    cmd_clickhouse_install, cmd_clickhouse_status, cmd_clickhouse_uninstall,
    run_foreground_clickhouse, run_supervised_clickhouse,
};
use crate::paths::{load_cfg, runtime_paths};
use crate::process::{require_service_binary, service_args_with_defaults};
use crate::render::{
    render_clickhouse_status, render_core_index_status, render_db_doctor, render_db_migrate,
    render_logs, state_label, CliOutput, CoreIndexReport, MigrationOutcome,
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
                    let core_index = gather_core_index_report(&cfg).await;
                    render_db_doctor(&output, &report, &core_index)?;
                    if doctor_is_healthy(&report) {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
                DbCommand::CoreIndex(args) => match args.command {
                    CoreIndexCommand::Status => {
                        let report = gather_core_index_report(&cfg).await;
                        render_core_index_status(&output, &report)?;
                        Ok(ExitCode::SUCCESS)
                    }
                    CoreIndexCommand::Rebuild => cmd_db_core_index_rebuild(&cfg, &output).await,
                    CoreIndexCommand::Promote(promote) => {
                        cmd_db_core_index_promote(&cfg, &output, promote.force).await
                    }
                },
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

/// The validated `[query_budgets]` every CLI query envelope is built from
/// (issue #600 W8). `load_cfg` already rejects invalid budgets, so the
/// fallback only fires for programmatically-built configs (tests); it keeps
/// the command usable on the bundled defaults instead of failing on a budget
/// shape the loader would have rejected anyway.
pub(crate) fn query_budgets(cfg: &AppConfig) -> ValidatedQueryBudgets {
    ValidatedQueryBudgets::from_config(&cfg.query_budgets).unwrap_or_else(|error| {
        eprintln!("warning: invalid [query_budgets]; CLI envelopes use bundled defaults: {error}");
        ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
            .expect("bundled default query budgets are valid")
    })
}

// Deliberate shared-read-layer exception: `db *`/`doctor` are storage administration,
// while `export` owns a versioned row contract and schema-skew gate. Those paths keep
// direct ClickHouse access; operational status reads go through ConversationRepository.

#[derive(Debug, Clone, Copy)]
pub(super) enum DatabaseProgress {
    Migration(MigrationProgress),
    ReconciliationInspecting,
    ReconciliationStarted {
        historical: bool,
    },
    ReconciliationAdvanced {
        processed: usize,
    },
    ReconciliationFinished {
        processed: usize,
    },
    /// Canonical read-index (issue #598) backfill progress. Runs after and
    /// outside the v1 read-model backfill.
    CoreIndex(CoreIndexBackfillProgress),
}

async fn migrate_database_with_progress<F>(
    cfg: &AppConfig,
    mut on_progress: F,
) -> Result<MigrationOutcome>
where
    F: FnMut(DatabaseProgress),
{
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);
    let applied = ch
        .run_migrations_with_progress_and_budget(&budgets.migration, |event| {
            on_progress(DatabaseProgress::Migration(event));
        })
        .await?;
    on_progress(DatabaseProgress::ReconciliationInspecting);

    // The ready probe and read-model backfill previously ran unenveloped
    // here; migrate/up wraps them in one Migration-class envelope whose
    // absolute deadline honors the operator client timeout (amendments
    // A5/A6). The migration runner above scopes its own per-statement
    // envelopes and is unaffected. A backfill that exceeds this budget fails
    // with a typed error and stays retryable: its cursor is persisted after
    // every page.
    ch.migration_envelope(&budgets.migration)
        .scope(async {
            let historical = !ch.mcp_open_read_model_ready().await?;
            on_progress(DatabaseProgress::ReconciliationStarted { historical });

            let mut processed = 0;
            ch.backfill_mcp_open_read_model_with_progress(|refreshed_sessions| {
                processed = refreshed_sessions;
                on_progress(DatabaseProgress::ReconciliationAdvanced {
                    processed: refreshed_sessions,
                });
            })
            .await
            .context("failed to backfill MCP open read model")?;
            on_progress(DatabaseProgress::ReconciliationFinished { processed });
            Ok::<_, anyhow::Error>(())
        })
        .await?;

    // Issue #598 WI-03: sweep the pre-existing corpus into the migration-036
    // canonical read indexes, then audit + publish readiness. This is sequenced
    // AFTER and OUTSIDE the v1 backfill envelope above (BINDING D5): the sweep
    // scopes its OWN Migration-class batch envelope per page rather than sharing
    // one envelope whose deadline would cap the sum of every page's time.
    //
    // The migrate/up path only ever targets the default single-owner local
    // backend (config: the default backend is "the only backend moraine
    // migrates"), so the publication mode is Local and open_v2 may auto-publish
    // (BINDING D3). The predicate is threaded explicitly so a future
    // shared-target migrate path cannot silently flip the consumer flag.
    let publication_mode_is_local = true;
    ch.backfill_canonical_read_indexes(
        publication_mode_is_local,
        &budgets.migration,
        &budgets.administrative,
        |event| on_progress(DatabaseProgress::CoreIndex(event)),
    )
    .await
    .context("failed to backfill canonical read indexes")?;

    Ok(MigrationOutcome { applied })
}

pub(super) async fn migrate_database_for_up<F>(
    cfg: &AppConfig,
    on_progress: F,
) -> Result<MigrationOutcome>
where
    F: FnMut(DatabaseProgress),
{
    migrate_database_with_progress(cfg, on_progress).await
}

async fn cmd_db_migrate(cfg: &AppConfig) -> Result<MigrationOutcome> {
    let mut historical = false;
    migrate_database_with_progress(cfg, |event| match event {
        DatabaseProgress::Migration(_) => {}
        DatabaseProgress::ReconciliationInspecting => {}
        DatabaseProgress::ReconciliationStarted {
            historical: required,
        } => {
            historical = required;
            if historical {
                eprintln!(
                    "Building the MCP open read model from existing sessions; this one-time step may take several minutes."
                );
            }
        }
        DatabaseProgress::ReconciliationAdvanced { processed } => {
            if historical {
                eprintln!("  projected {processed} sessions");
            }
        }
        DatabaseProgress::ReconciliationFinished { .. } => {
            if historical {
                eprintln!("MCP open read model ready.");
            }
        }
        DatabaseProgress::CoreIndex(event) => match event {
            CoreIndexBackfillProgress::Starting { resuming } => {
                eprintln!(
                    "Building canonical read indexes (issue #598){}.",
                    if resuming { ", resuming" } else { "" }
                );
            }
            CoreIndexBackfillProgress::PageIndexed {
                pages,
                events_indexed,
            } => {
                eprintln!("  swept {pages} pages ({events_indexed} events)");
            }
            CoreIndexBackfillProgress::Auditing => {
                eprintln!("  auditing canonical read-index coverage");
            }
            CoreIndexBackfillProgress::Published {
                core_indexes,
                open_v2,
            } => {
                if core_indexes {
                    eprintln!(
                        "Canonical read indexes ready (open v2 reader: {}).",
                        if open_v2 { "published" } else { "unpublished" }
                    );
                } else {
                    eprintln!(
                        "Canonical read indexes installed but not published (overlap audit did not pass)."
                    );
                }
            }
            CoreIndexBackfillProgress::AlreadyComplete => {}
        },
    })
    .await
}

async fn cmd_db_doctor(cfg: &AppConfig) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);
    // Doctor is an Interactive-class read (amendment A6 — deliberately not
    // Administrative, whose tiny caps only fit KILL/telemetry one-shots):
    // the report spans ping, version, ledger, table, and publication reads.
    QueryEnvelope::new_with_admin_budget(
        "doctor",
        QueryClass::Interactive,
        &budgets.interactive,
        &budgets.administrative,
    )
    .scope(ch.doctor_report())
    .await
}

/// The CLI operates on the default single-owner Local backend, so open-reader
/// resolution and any `open_v2` publication here is Local-mode (matching the
/// migrate path's `publication_mode_is_local = true`, BINDING D3).
const CLI_PUBLICATION_MODE_IS_LOCAL: bool = true;

/// Raw `mcp_read_index_state` reads gathered for operator surfacing.
struct RawCoreIndexState {
    core_indexes: Option<ReadIndexState>,
    open_v2: Option<ReadIndexState>,
    audit: Option<CoreIndexAuditOutcome>,
}

async fn read_core_index_state(ch: &ClickHouseClient) -> Result<RawCoreIndexState> {
    Ok(RawCoreIndexState {
        core_indexes: ch.read_index_state(STATE_KEY_CORE_INDEXES).await?,
        open_v2: ch.read_index_state(STATE_KEY_OPEN_V2).await?,
        audit: ch.core_index_audit_outcome().await?,
    })
}

/// Best-effort gather of canonical read-index (issue #598) readiness for
/// operator surfacing (`status`, `doctor`, `core-index status`). Never fails:
/// an unreachable ClickHouse yields an "unavailable" report so the surrounding
/// command still renders. An un-migrated but reachable database yields a
/// "reachable, not ready" report.
pub(super) async fn gather_core_index_report(cfg: &AppConfig) -> CoreIndexReport {
    let configured = cfg.mcp.open_reader;
    let Ok(ch) = ClickHouseClient::new(cfg.clickhouse.clone()) else {
        return unavailable_core_index_report(configured);
    };
    let budgets = query_budgets(cfg);
    let gathered = QueryEnvelope::new_with_admin_budget(
        "core-index-status",
        QueryClass::Interactive,
        &budgets.interactive,
        &budgets.administrative,
    )
    .scope(read_core_index_state(&ch))
    .await;
    match gathered {
        Ok(state) => build_core_index_report(configured, state),
        Err(_) => unavailable_core_index_report(configured),
    }
}

fn unavailable_core_index_report(configured: OpenReaderMode) -> CoreIndexReport {
    CoreIndexReport {
        available: false,
        core_indexes_ready: false,
        open_v2_ready: false,
        open_v2_provenance: None,
        backfill_cursor_age_seconds: None,
        audit: None,
        configured_open_reader: configured.as_str().to_string(),
        effective_open_reader: "unknown".to_string(),
        open_reader_override: false,
        open_reader_note: None,
    }
}

fn build_core_index_report(
    configured: OpenReaderMode,
    state: RawCoreIndexState,
) -> CoreIndexReport {
    let core_indexes_ready = state
        .core_indexes
        .as_ref()
        .is_some_and(|row| row.ready == 1);
    let open_v2_row = state.open_v2.as_ref();
    let open_v2_ready = open_v2_row.is_some_and(|row| row.ready == 1);
    let open_v2_provenance = open_v2_row
        .and_then(|row| (open_v2_ready && !row.cursor.is_empty()).then(|| row.cursor.clone()));
    let backfill_cursor_age_seconds = state
        .core_indexes
        .as_ref()
        .and_then(|row| snowflake_age_seconds(row.generation));

    let resolution = configured.resolve(open_v2_ready, CLI_PUBLICATION_MODE_IS_LOCAL);
    let (effective, override_active, note) = match resolution {
        OpenReaderResolution::V1 { config_override } => (
            "v1",
            config_override,
            config_override.then(|| {
                "v1 forced by [mcp] open_reader (kill-switch); v2 readiness ignored".to_string()
            }),
        ),
        OpenReaderResolution::V2 { config_override } => (
            "v2",
            config_override,
            config_override.then(|| "v2 forced by [mcp] open_reader".to_string()),
        ),
        OpenReaderResolution::ForcedV2Unready => (
            "error",
            true,
            Some(
                "[mcp] open_reader = \"v2\" but the core read indexes are not ready; \
                 open will fail (run `moraine db core-index rebuild`)"
                    .to_string(),
            ),
        ),
    };

    CoreIndexReport {
        available: true,
        core_indexes_ready,
        open_v2_ready,
        open_v2_provenance,
        backfill_cursor_age_seconds,
        audit: state.audit,
        configured_open_reader: configured.as_str().to_string(),
        effective_open_reader: effective.to_string(),
        open_reader_override: override_active,
        open_reader_note: note,
    }
}

/// Decode the age, in seconds, of a ClickHouse `generateSnowflakeID()` value.
/// The top 41 bits are milliseconds since the Unix epoch, so `generation >> 22`
/// is the write time. Seed rows carry `generation = 0`; any value too small to
/// be a real snowflake is treated as "not yet swept" (`None`).
fn snowflake_age_seconds(generation: u64) -> Option<i64> {
    let write_ms = (generation >> 22) as i64;
    if write_ms == 0 {
        return None;
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    Some((now_ms - write_ms) / 1000)
}

async fn cmd_db_core_index_rebuild(cfg: &AppConfig, output: &CliOutput) -> Result<ExitCode> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);

    eprintln!("Resetting canonical read indexes (truncate + clear readiness).");
    // The reset is a handful of quick TRUNCATE/INSERT statements — one Migration
    // envelope over the batch is fine (contrast the backfill, whose per-page
    // envelopes below are required so a page deadline caps one page, not the
    // sum of all pages, BINDING D5).
    ch.migration_envelope(&budgets.migration)
        .scope(ch.reset_canonical_read_indexes())
        .await
        .context("failed to reset canonical read indexes")?;

    // Rerun the backfill exactly as the migrate path does — reuse the WI-03
    // engine, which scopes its OWN per-page Migration envelopes internally, so
    // it must NOT be wrapped in an additional envelope here.
    let outcome = ch
        .backfill_canonical_read_indexes(
            CLI_PUBLICATION_MODE_IS_LOCAL,
            &budgets.migration,
            &budgets.administrative,
            render_core_index_progress,
        )
        .await
        .context("failed to rebuild canonical read indexes")?;

    let report = gather_core_index_report(cfg).await;
    if output.is_json() {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "rebuilt": true,
                "pages": outcome.pages,
                "events_indexed": outcome.events_indexed,
                "core_indexes_published": outcome.core_indexes_published,
                "open_v2_published": outcome.open_v2_published,
                "core_index": report,
            }))?
        );
    } else {
        render_core_index_status(output, &report)?;
    }
    Ok(ExitCode::SUCCESS)
}

fn render_core_index_progress(event: CoreIndexBackfillProgress) {
    match event {
        CoreIndexBackfillProgress::Starting { resuming } => eprintln!(
            "Rebuilding canonical read indexes{}.",
            if resuming { ", resuming" } else { "" }
        ),
        CoreIndexBackfillProgress::PageIndexed {
            pages,
            events_indexed,
        } => eprintln!("  swept {pages} pages ({events_indexed} events)"),
        CoreIndexBackfillProgress::Auditing => {
            eprintln!("  auditing canonical read-index coverage")
        }
        CoreIndexBackfillProgress::Published {
            core_indexes,
            open_v2,
        } => {
            if core_indexes {
                eprintln!(
                    "Canonical read indexes ready (open v2 reader: {}).",
                    if open_v2 { "published" } else { "unpublished" }
                );
            } else {
                eprintln!(
                    "Canonical read indexes installed but not published (overlap audit did not pass)."
                );
            }
        }
        CoreIndexBackfillProgress::AlreadyComplete => {}
    }
}

async fn cmd_db_core_index_promote(
    cfg: &AppConfig,
    output: &CliOutput,
    force: bool,
) -> Result<ExitCode> {
    if !force {
        // Promotion switches EVERY reader of this backend onto the canonical v2
        // reader; a downlevel (v1-only) reader would then fail. Require an
        // explicit confirmation before publishing.
        let lines = vec![
            "Promotion publishes open_v2.ready=1 for this backend.".to_string(),
            "Every `open` consumer of this backend must be v2-capable (this build or newer);"
                .to_string(),
            "a downlevel reader will fail after promotion.".to_string(),
            "Re-run with --force to confirm and publish.".to_string(),
        ];
        output.section("Canonical Read Indexes: Promote", &lines);
        return Ok(ExitCode::from(1));
    }

    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);
    let outcome: OpenV2PromotionOutcome = ch
        .migration_envelope(&budgets.migration)
        .scope(ch.promote_open_v2_reader())
        .await
        .context("failed to promote the open v2 reader")?;

    let report = gather_core_index_report(cfg).await;
    let succeeded = outcome.promoted || outcome.already_promoted;
    if output.is_json() {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "promoted": outcome.promoted,
                "already_promoted": outcome.already_promoted,
                "core_indexes_ready": outcome.core_indexes_ready,
                "audit_passed": outcome.audit_passed,
                "core_index": report,
            }))?
        );
    } else {
        let mut lines = Vec::new();
        if outcome.already_promoted {
            lines.push("open v2 reader was already promoted (no change).".to_string());
        } else if outcome.promoted {
            lines.push("Published open_v2.ready=1 (provenance: operator-promote).".to_string());
        } else {
            lines.push("Refused to promote: the core read indexes are not ready.".to_string());
            lines.push(format!(
                "  core_indexes.ready={}, overlap_audit={}",
                state_label(outcome.core_indexes_ready),
                if outcome.audit_passed { "pass" } else { "fail" },
            ));
            lines.push("  Run `moraine db core-index rebuild` first.".to_string());
        }
        output.section("Canonical Read Indexes: Promote", &lines);
    }

    if succeeded {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::from(1))
    }
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
        && report
            .publication
            .as_ref()
            .is_some_and(PublicationDiagnostics::is_healthy)
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
    fn doctor_health_distinguishes_publication_progress_from_blocking_state() {
        let mut report = DoctorReport {
            clickhouse_healthy: true,
            clickhouse_version: Some("25.8".to_string()),
            database: "moraine".to_string(),
            database_exists: true,
            applied_migrations: Vec::new(),
            pending_migrations: Vec::new(),
            missing_tables: Vec::new(),
            publication: Some(PublicationDiagnostics {
                replaying_generations: 2,
                append_preparations: 1,
                mirror_catchup_pending: 1,
                ..PublicationDiagnostics::default()
            }),
            errors: Vec::new(),
        };
        assert!(doctor_is_healthy(&report));

        report
            .publication
            .as_mut()
            .expect("publication diagnostics")
            .blocked_append_preparations = 1;
        assert!(!doctor_is_healthy(&report));

        report.publication = None;
        assert!(!doctor_is_healthy(&report));
    }

    fn state_row(state_key: &str, ready: u8, generation: u64, cursor: &str) -> ReadIndexState {
        ReadIndexState {
            state_key: state_key.to_string(),
            ready,
            generation,
            cursor: cursor.to_string(),
        }
    }

    /// A real ClickHouse snowflake for `write_ms` (top 41 bits = ms since epoch).
    fn snowflake_for(write_ms: u64) -> u64 {
        write_ms << 22
    }

    #[test]
    fn build_core_index_report_auto_selects_v2_when_ready() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let state = RawCoreIndexState {
            core_indexes: Some(state_row(
                STATE_KEY_CORE_INDEXES,
                1,
                snowflake_for(now_ms - 5_000),
                "{}",
            )),
            open_v2: Some(state_row(
                STATE_KEY_OPEN_V2,
                1,
                snowflake_for(now_ms),
                "auto-local",
            )),
            audit: Some(CoreIndexAuditOutcome {
                passed: true,
                ..CoreIndexAuditOutcome::default()
            }),
        };
        let report = build_core_index_report(OpenReaderMode::Auto, state);
        assert!(report.available);
        assert!(report.core_indexes_ready);
        assert!(report.open_v2_ready);
        assert_eq!(report.open_v2_provenance.as_deref(), Some("auto-local"));
        assert_eq!(report.effective_open_reader, "v2");
        assert!(!report.open_reader_override);
        assert!(report.open_reader_note.is_none());
        // ~5s old, allowing scheduling slack.
        let age = report.backfill_cursor_age_seconds.expect("age");
        assert!((4..=8).contains(&age), "age was {age}");
    }

    #[test]
    fn build_core_index_report_v1_override_beats_ready_indexes() {
        let state = RawCoreIndexState {
            core_indexes: Some(state_row(STATE_KEY_CORE_INDEXES, 1, snowflake_for(1), "{}")),
            open_v2: Some(state_row(
                STATE_KEY_OPEN_V2,
                1,
                snowflake_for(1),
                "auto-local",
            )),
            audit: Some(CoreIndexAuditOutcome {
                passed: true,
                ..CoreIndexAuditOutcome::default()
            }),
        };
        let report = build_core_index_report(OpenReaderMode::V1, state);
        assert_eq!(report.effective_open_reader, "v1");
        assert!(report.open_reader_override);
        assert!(report
            .open_reader_note
            .as_deref()
            .is_some_and(|note| note.contains("kill-switch")));
    }

    #[test]
    fn build_core_index_report_forced_v2_unready_is_an_error() {
        let state = RawCoreIndexState {
            core_indexes: Some(state_row(STATE_KEY_CORE_INDEXES, 0, 0, "")),
            open_v2: Some(state_row(STATE_KEY_OPEN_V2, 0, 0, "")),
            audit: None,
        };
        let report = build_core_index_report(OpenReaderMode::V2, state);
        assert_eq!(report.effective_open_reader, "error");
        assert!(report.open_reader_override);
        assert!(report
            .open_reader_note
            .as_deref()
            .is_some_and(|note| note.contains("not ready")));
        // Seed generation 0 renders no age.
        assert!(report.backfill_cursor_age_seconds.is_none());
    }

    #[test]
    fn build_core_index_report_hides_provenance_when_not_promoted() {
        // open_v2 row exists but is not ready: provenance is withheld.
        let state = RawCoreIndexState {
            core_indexes: Some(state_row(STATE_KEY_CORE_INDEXES, 1, snowflake_for(1), "{}")),
            open_v2: Some(state_row(STATE_KEY_OPEN_V2, 0, 0, "operator-promote")),
            audit: Some(CoreIndexAuditOutcome {
                passed: true,
                ..CoreIndexAuditOutcome::default()
            }),
        };
        let report = build_core_index_report(OpenReaderMode::Auto, state);
        assert!(!report.open_v2_ready);
        assert!(report.open_v2_provenance.is_none());
        // Ready core indexes but open_v2 not published: auto stays on v1.
        assert_eq!(report.effective_open_reader, "v1");
        assert!(!report.open_reader_override);
    }

    #[test]
    fn snowflake_age_rejects_seed_and_decodes_real_ids() {
        assert!(snowflake_age_seconds(0).is_none());
        // A tiny non-zero value whose top 41 bits are still zero is a seed, not
        // a snowflake.
        assert!(snowflake_age_seconds(123).is_none());
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let age = snowflake_age_seconds(snowflake_for(now_ms - 10_000)).expect("age");
        assert!((9..=13).contains(&age), "age was {age}");
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
