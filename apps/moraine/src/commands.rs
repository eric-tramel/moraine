mod down;
mod export;
mod logs;
mod schema;
mod setup;
mod status;
mod up;

use anyhow::{bail, Context, Result};
use moraine_clickhouse::{
    reclaim, ClickHouseClient, CoreIndexAuditOutcome, CoreIndexBackfillProgress, DoctorReport,
    MigrationProgress, OpenV2PromotionOutcome, PublicationDiagnostics, QueryClass, QueryEnvelope,
    ReadIndexState, ReclaimPlan, ReclaimScope, ReclaimStatusReport, StorageReport,
    OPEN_V2_PROVENANCE_OPERATOR_PROMOTE, STATE_KEY_CORE_INDEXES, STATE_KEY_OPEN_V2,
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
    OutputFormat, ReclaimCommand, RunArgs, SchemaCommand,
};
use crate::managed_clickhouse::{
    cmd_clickhouse_install, cmd_clickhouse_status, cmd_clickhouse_uninstall,
    run_foreground_clickhouse, run_supervised_clickhouse,
};
use crate::paths::{load_cfg, runtime_paths};
use crate::process::{require_service_binary, service_args_with_defaults};
use crate::render::{
    render_clickhouse_status, render_core_index_status, render_db_doctor, render_db_migrate,
    render_logs, render_reclaim_plan, render_reclaim_refusal, render_reclaim_status, state_label,
    CliOutput, CoreIndexReport, MigrationOutcome,
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
                    let storage = gather_storage_report(&cfg).await;
                    render_db_doctor(&output, &report, &core_index, storage.as_ref())?;
                    if doctor_is_healthy(&report) {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
                DbCommand::Reclaim(args) => match args.command {
                    ReclaimCommand::Status(status) => {
                        let report = gather_reclaim_status(&cfg).await;
                        render_reclaim_status(&output, &report, status.json)?;
                        Ok(ExitCode::SUCCESS)
                    }
                    ReclaimCommand::Plan(plan) => cmd_db_reclaim_plan(&cfg, &output, plan).await,
                    ReclaimCommand::Run(run) => cmd_db_reclaim_run(&cfg, &output, run).await,
                },
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

#[derive(Debug, Clone)]
pub(super) enum DatabaseProgress {
    Migration(MigrationProgress),
    /// Canonical read-index (issue #598) backfill progress.
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
    let mut deferred = false;
    let mut applied = ch
        .run_migrations_with_progress_and_budget(&budgets.migration, |event| {
            if matches!(event, MigrationProgress::Deferred { .. }) {
                deferred = true;
            }
            on_progress(DatabaseProgress::Migration(event));
        })
        .await?;

    // Issue #598 WI-03: sweep the pre-existing corpus into the migration-036
    // canonical read indexes, then audit + publish readiness. The sweep scopes
    // its OWN Migration-class batch envelope per page rather than sharing one
    // envelope whose deadline would cap the sum of every page's time.
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

    // Issue #603 WI-10: the retirement migration (041) defers until `open_v2`
    // publishes, and the canonical sweep above is exactly what publishes it —
    // so a deferred pass is retried once in the same startup and a healthy
    // host retires its legacy projection without a second invocation. A pass
    // that defers AGAIN (the audit did not pass, so readiness was withheld) is
    // surfaced through the same Deferred progress event, whose reason names
    // the recovery recipe; the run still succeeds and the projection bytes
    // stay untouched until the audit is fixed.
    if deferred {
        let retried = ch
            .run_migrations_with_progress_and_budget(&budgets.migration, |event| {
                on_progress(DatabaseProgress::Migration(event));
            })
            .await?;
        applied.extend(retried);
    }

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
    migrate_database_with_progress(cfg, |event| match event {
        // Preflight and deferral are the two operator-facing migration
        // events (issue #603 WI-10): the retirement's reclaimed-bytes note,
        // and the named actionable reason when its precondition is unmet.
        DatabaseProgress::Migration(MigrationProgress::Preflight { note, .. }) => {
            eprintln!("{note}");
        }
        DatabaseProgress::Migration(MigrationProgress::Deferred {
            version,
            name,
            reason,
        }) => {
            eprintln!("warning: migration {version} ({name}) deferred: {reason}");
        }
        DatabaseProgress::Migration(_) => {}
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

/// The CLI operates on the default single-owner Local backend, so any
/// `open_v2` publication here is Local-mode (matching the migrate path's
/// `publication_mode_is_local = true`, BINDING D3). Since issue #603 WI-10
/// this gates only readiness *publication*; reader resolution no longer
/// consults it.
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

/// Best-effort gather of the issue #603 storage/reclaim surface for operator
/// display (`status`, `db doctor`, `db reclaim status`).
///
/// Never fails: an unreachable ClickHouse yields `available: false` with the
/// error attached, so the surrounding command still renders. Storage state is
/// transient and must not fail the doctor exit code — the same contract
/// `gather_core_index_report` established.
///
/// `&cfg.retention`, never a default: `reclaim_status` reports the effective
/// `[retention]` policy *and* plans every scope through it, so a defaulted
/// config here makes the status panel disagree with the run in both the policy
/// block and the unit counts. Guarded by
/// `tests::the_status_report_uses_the_operators_retention_horizon`.
pub(super) async fn gather_reclaim_status(cfg: &AppConfig) -> ReclaimStatusReport {
    let Ok(ch) = ClickHouseClient::new(cfg.clickhouse.clone()) else {
        return unavailable_reclaim_status("ClickHouse client could not be constructed");
    };
    let budgets = query_budgets(cfg);
    ch.reclaim_status(&cfg.retention, &budgets.background, &budgets.administrative)
        .await
}

fn unavailable_reclaim_status(message: &str) -> ReclaimStatusReport {
    ReclaimStatusReport {
        available: false,
        storage: None,
        ledger: Default::default(),
        reclaimable: Vec::new(),
        registered_executors: reclaim::registered_executors(),
        denomination: reclaim::estimated_bytes_note(),
        error: Some(message.to_string()),
    }
}

/// The storage half of the reclaim status, for the `status` panel and the
/// doctor block. `None` when the backend is unreachable.
pub(super) async fn gather_storage_report(cfg: &AppConfig) -> Option<StorageReport> {
    gather_reclaim_status(cfg).await.storage
}

/// Resolve a `--scope` argument, listing the valid values on a typo rather
/// than falling back to "all" — silently widening a destructive command's
/// scope is the one failure this parser must not have.
fn parse_reclaim_scope(raw: &str) -> Result<ReclaimScope> {
    ReclaimScope::parse(raw.trim()).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown reclaim scope `{raw}`; valid scopes: {}",
            ReclaimScope::ALL
                .iter()
                .map(|scope| scope.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

async fn cmd_db_reclaim_plan(
    cfg: &AppConfig,
    output: &CliOutput,
    args: crate::cli::ReclaimPlanArgs,
) -> Result<ExitCode> {
    let scopes = match args.scope.as_deref() {
        Some(raw) => vec![parse_reclaim_scope(raw)?],
        None => ReclaimScope::ALL.to_vec(),
    };
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);
    // `&cfg.retention`, never a default: the horizon the planner reports must
    // be the horizon the run deletes at, or the unit count an operator reads
    // is not the unit count the run claims. Guarded by
    // `tests::the_planner_uses_the_operators_retention_horizon`.
    let plan: ReclaimPlan = reclaim::plan_envelope(&budgets.background, &budgets.administrative)
        .scope(ch.reclaim_plan(&cfg.retention, &scopes))
        .await?;
    render_reclaim_plan(output, &plan, args.json)?;
    Ok(ExitCode::SUCCESS)
}

/// `moraine db reclaim run`.
///
/// Two refusals, in this order:
///
/// 1. **Unconfirmed.** The unforced path prints exactly which scope and which
///    tables would be touched and exits non-zero, following the `--force`
///    ceremony precedent. `moraine export events --format jsonl` is named as
///    the pre-destructive safety valve.
/// 2. **Unauthorized.** A bucket-1/2 scope additionally refuses unless the
///    matching `[retention]` key is present, and says which key is missing.
///
/// In this build every scope then refuses again for a third reason: no
/// executor is registered. Nothing is deleted by any path.
///
/// All three of the ceremony's parts — the `--confirm` gate, the authority
/// gate reached through `reclaim_run`, and the non-zero exit — are guarded by
/// `tests::the_unconfirmed_run_ceremony_is_enforced_end_to_end` and
/// `tests::a_refusal_exits_non_zero`, which drive this function rather than
/// re-deriving its decisions. `cli::tests::clap_parses_reclaim_subcommands`
/// proves only that `--confirm` parses and defaults to false; it says nothing
/// about whether anything reads it.
///
/// The two arguments this function chooses for the reclaimer — the retention
/// config and the trigger — are each observed by a guard that drives this
/// function against a stand-in server and reads what reached it:
/// `tests::the_operator_run_uses_the_operators_retention_horizon` and
/// `tests::the_operators_run_is_not_gated_on_free_disk`. The envelope is built
/// by `reclaim::run_preamble_envelope`, so its cap is not an argument here at
/// all.
async fn cmd_db_reclaim_run(
    cfg: &AppConfig,
    output: &CliOutput,
    args: crate::cli::ReclaimRunArgs,
) -> Result<ExitCode> {
    let scope = parse_reclaim_scope(&args.scope)?;
    if !args.confirm {
        render_reclaim_refusal(output, scope, args.json)?;
        return Ok(ExitCode::from(RECLAIM_REFUSAL_EXIT_CODE));
    }
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);
    // The preamble runs here; each claimed unit opens its own
    // `UNIT_STATEMENT_CAP` envelope inside (plan §3.7: one envelope per unit,
    // so a deadline caps one unit rather than a 64-unit sweep).
    let outcome = reclaim::run_preamble_envelope(&budgets.background, &budgets.administrative)
        // `&cfg.retention`, never a default: the horizon is the only thing
        // separating this collector from a prepare in flight, because
        // `prepare` writes children first and the header last. An operator who
        // widened `retention.derived_horizon_hours` because their host
        // publishes slowly must get *their* horizon here, not the stock 24h.
        //
        // `Operator`: this path is reached only after `--confirm`, by somebody
        // looking at the host. It is deliberately **not** subject to the
        // free-disk refusal the maintenance tick applies — an operator may
        // need to reclaim precisely because the disk is full, and refusing
        // them here would leave no way forward.
        .scope(ch.reclaim_run(
            &cfg.retention,
            scope,
            reclaim::ReclaimTrigger::Operator,
            &budgets.background,
            &budgets.administrative,
        ))
        .await?;
    crate::render::render_reclaim_outcome(output, &outcome, args.json)?;
    Ok(ExitCode::from(reclaim_run_exit_code(&outcome)))
}

/// Exit status of a `moraine db reclaim run` that never reached an executor.
pub(crate) const RECLAIM_REFUSAL_EXIT_CODE: u8 = 1;

/// Exit status for a rendered reclaim outcome.
///
/// A refusal is not a success: `NoExecutor` exits non-zero so a script that
/// expects reclamation to have happened notices that it did not. `Blocked` is
/// the same case — hazard H9 is precisely that "blocked" and "nothing to do"
/// were indistinguishable, and an exit code that cannot tell them apart is
/// that bug at the process boundary.
pub(crate) fn reclaim_run_exit_code(outcome: &moraine_clickhouse::ReclaimOutcome) -> u8 {
    match outcome {
        // `LowDisk` joins the refusals for the same reason `Blocked` does:
        // reclamation did not happen, and a script that assumed it did must
        // notice. It is unreachable from this command today — the CLI runs as
        // `Operator`, which does not check free space — and is matched here so
        // that stays a deliberate decision rather than a wildcard's accident.
        moraine_clickhouse::ReclaimOutcome::NoExecutor { .. }
        | moraine_clickhouse::ReclaimOutcome::Blocked { .. }
        | moraine_clickhouse::ReclaimOutcome::LowDisk { .. } => RECLAIM_REFUSAL_EXIT_CODE,
        moraine_clickhouse::ReclaimOutcome::Idle { .. }
        | moraine_clickhouse::ReclaimOutcome::Settled { .. } => 0,
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

    // Post-WI-10 resolution: one reader, gated on readiness alone. A config
    // still saying `v1` is accepted; the note surfaces the retirement (the
    // same string the MCP backend logs), and `open_reader_override` flags it
    // so the status panel renders the note prominently.
    //
    // `open_reader_override` is exactly `retired_v1_requested` on BOTH arms.
    // With one reader left, `auto` and `v2` are synonyms that force nothing,
    // so `v1` is the only configured value the resolution declines to honor.
    // Hardcoding `true` on the unready arm would tell a default `auto` install
    // it carries a config override it does not have — and post-041 that is the
    // NORMAL state between `migrate` and the first sweep, and of every host
    // whose audit fails. Prominence does not need the lie: the concise status
    // branch already ORs in `effective_open_reader == "error"`.
    let resolution = configured.resolve(open_v2_ready);
    let (effective, override_active, note) = match resolution {
        OpenReaderResolution::V2 {
            retired_v1_requested,
        } => (
            "v2",
            retired_v1_requested,
            retired_v1_requested.then(|| OpenReaderMode::RETIRED_V1_NOTE.to_string()),
        ),
        OpenReaderResolution::Unready {
            retired_v1_requested,
        } => (
            "error",
            retired_v1_requested,
            Some(if retired_v1_requested {
                format!(
                    "the canonical read indexes are not ready; open will fail (run `moraine db \
                     migrate`, or `moraine db core-index rebuild`). Also: {}",
                    OpenReaderMode::RETIRED_V1_NOTE
                )
            } else {
                "the canonical read indexes are not ready; open will fail (run `moraine db \
                 migrate`, or `moraine db core-index rebuild`)"
                    .to_string()
            }),
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

/// The rebuild's `publication_mode_is_local` (BINDING D3). The CLI targets the
/// config's default backend, which is Local — except that an `open_v2` row
/// published with the operator-promote provenance marks the backend as operated
/// through the Shared promote ceremony, and a rebuild must not bypass that
/// ceremony by silently republishing with `auto-local` provenance: the doc's
/// rebuild-then-re-promote step is the operator's verification gate.
fn rebuild_publication_mode_is_local(prior_open_v2: Option<&ReadIndexState>) -> bool {
    CLI_PUBLICATION_MODE_IS_LOCAL
        && !prior_open_v2
            .is_some_and(|row| row.ready == 1 && row.cursor == OPEN_V2_PROVENANCE_OPERATOR_PROMOTE)
}

/// What `moraine db core-index rebuild` prints before it touches anything.
///
/// Readiness revocation only reaches processes started AFTER it: a running
/// backend/MCP process samples `open_v2` readiness once at construction and
/// keeps that answer for its lifetime (WI-08), so it would go on resolving v2
/// against the truncated indexes for the whole re-sweep.
///
/// The ACTION is a restart. The REASON is **not** that a restarted process
/// falls back to something — issue #603 WI-10 deleted the v1 reader, so a
/// restarted process re-reads the readiness this rebuild just revoked and
/// answers `open` with the typed unready error. Failing loudly beats serving
/// silently truncated sessions; that is the whole argument, and the message
/// must make it rather than promise a reader that no longer exists.
///
/// A `&[&str]` rather than four `eprintln!`s so the claim is testable: this is
/// the recovery path named by the retirement gate's deferral reason, by
/// `build_core_index_report`'s unready note, and by
/// `docs/operations/canonical-read-indexes.md`.
const REBUILD_RESTART_WARNING: &[&str] = &[
    "WARNING: already-running backend/MCP processes cache the open v2 reader for their",
    "         process lifetime and will keep serving it against the truncated indexes.",
    "         Restart them now (`moraine down && moraine up`): a restarted process reads",
    "         the readiness this rebuild revokes and refuses `open` with a typed error",
    "         until the rebuild republishes it. The v1 reader is retired (issue #603",
    "         WI-10), so there is nothing to fall back to — refusing loudly is the point.",
];

async fn cmd_db_core_index_rebuild(cfg: &AppConfig, output: &CliOutput) -> Result<ExitCode> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let budgets = query_budgets(cfg);

    for line in REBUILD_RESTART_WARNING {
        eprintln!("{line}");
    }

    let prior_open_v2 = ch
        .migration_envelope(&budgets.migration)
        .scope(ch.read_index_state(STATE_KEY_OPEN_V2))
        .await
        .context("failed to read open_v2 state before the core-index rebuild")?;
    let publication_mode_is_local = rebuild_publication_mode_is_local(prior_open_v2.as_ref());
    if !publication_mode_is_local {
        eprintln!("open_v2 was published via `core-index promote`; this rebuild will not auto-publish it.");
        eprintln!(
            "Verify the rebuilt indexes, then re-run `moraine db core-index promote --force`."
        );
    }

    eprintln!("Resetting canonical read indexes (readiness revoked, then truncate).");
    // The reset is a handful of quick INSERT/TRUNCATE statements — one Migration
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
            publication_mode_is_local,
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
                "backfill_in_flight": outcome.backfill_in_flight,
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
            if outcome.backfill_in_flight {
                lines.push(
                    "  A backfill/rebuild sweep is in flight (page cursor persisted); \
                     let it finish, then re-run promote."
                        .to_string(),
                );
            } else {
                lines.push("  Run `moraine db core-index rebuild` first.".to_string());
            }
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

    /// A config whose ClickHouse endpoint is a port nothing listens on, so any
    /// path that reaches the network fails loudly rather than passing.
    fn offline_config(retention: moraine_config::RetentionConfig) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "http://127.0.0.1:1".to_string();
        cfg.clickhouse.timeout_seconds = 1.0;
        cfg.retention = retention;
        cfg
    }

    fn reclaim_run_args(scope: &str, confirm: bool) -> crate::cli::ReclaimRunArgs {
        crate::cli::ReclaimRunArgs {
            scope: scope.to_string(),
            confirm,
            json: false,
        }
    }

    // ---- a ClickHouse stand-in for the reclaim CLI paths -----------------
    //
    // The guards below must observe **what reached the server**. Every
    // mutation they exist for leaves this file's own decisions intact and
    // changes only an argument handed to the reclaimer, so a test that re-reads
    // `cfg.retention` next to the call site passes for all of them — that is
    // exactly how four free arguments survived three sweeps of the janitor's
    // call sites.
    //
    // Deliberately not the `axum` stand-in the library crates use: adding
    // `axum` to this binary's dev-dependencies would add a package edge to
    // `Cargo.lock`, and `--locked` is part of the gate. The two transport
    // profiles `ClickHouseClient` uses are the whole protocol surface these
    // paths need — the statement arrives in the URL `query` parameter for short
    // reads and in the body for anything longer (the candidate probes are all
    // body-carried), and every read here is answered from `system.disks` or
    // with an empty result set.

    /// Records the statement of every request; answers `system.disks` with a
    /// configurable free-space row, which is the one server answer any reclaim
    /// CLI path branches on.
    #[derive(Clone)]
    struct ReclaimServerMock {
        statements: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        free_bytes: u64,
    }

    impl ReclaimServerMock {
        fn with_free_bytes(free_bytes: u64) -> Self {
            Self {
                statements: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                free_bytes,
            }
        }

        /// Comfortably above `RECLAIM_MIN_FREE_BYTES`, so nothing in the path
        /// declines for headroom.
        fn roomy() -> Self {
            Self::with_free_bytes(moraine_clickhouse::reclaim::RECLAIM_MIN_FREE_BYTES * 4)
        }

        fn statements(&self) -> Vec<String> {
            self.statements
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn count(&self, needle: &str) -> usize {
            self.statements()
                .iter()
                .filter(|statement| statement.contains(needle))
                .count()
        }

        fn answer(&self, statement: &str) -> String {
            if statement.contains("system.disks") {
                format!(
                    "{{\"free_bytes\":{},\"total_bytes\":{}}}\n",
                    self.free_bytes,
                    self.free_bytes.saturating_mul(2)
                )
            } else {
                String::new()
            }
        }
    }

    async fn spawn_reclaim_server_mock(mock: ReclaimServerMock) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind reclaim stand-in listener");
        let addr = listener.local_addr().expect("reclaim stand-in addr");
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let mock = mock.clone();
                tokio::spawn(async move { serve_reclaim_request(stream, mock).await });
            }
        });
        format!("http://{addr}")
    }

    async fn serve_reclaim_request(mut stream: tokio::net::TcpStream, mock: ReclaimServerMock) {
        let Some(statement) = read_http_statement(&mut stream).await else {
            return;
        };
        let body = mock.answer(&statement);
        mock.statements
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(statement);
        write_http_response(&mut stream, 200, &body).await;
    }

    /// Read one ClickHouse HTTP request off `stream` and return the SQL
    /// statement it carries (URL `query` parameter for short reads, request
    /// body for anything longer — the two transport profiles
    /// `ClickHouseClient` uses).
    async fn read_http_statement(stream: &mut tokio::net::TcpStream) -> Option<String> {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();
        let mut chunk = [0_u8; 8192];
        let header_end = loop {
            if let Some(end) = buf
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|start| start + 4)
            {
                break end;
            }
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(read) => buf.extend_from_slice(&chunk[..read]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let content_length = head
            .lines()
            .skip(1)
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        while buf.len() < header_end + content_length {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(read) => buf.extend_from_slice(&chunk[..read]),
            }
        }

        let target = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();
        let from_url = url_query_param(&target, "query").unwrap_or_default();
        if from_url.trim().is_empty() {
            Some(
                String::from_utf8_lossy(&buf[header_end..])
                    .trim()
                    .to_string(),
            )
        } else {
            Some(from_url.trim().to_string())
        }
    }

    async fn write_http_response(stream: &mut tokio::net::TcpStream, status: u16, body: &str) {
        use tokio::io::AsyncWriteExt;

        let reason = if status == 200 { "OK" } else { "Error" };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    fn url_query_param(target: &str, name: &str) -> Option<String> {
        let (_, query) = target.split_once('?')?;
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| percent_decode(value))
        })
    }

    fn percent_decode(raw: &str) -> String {
        let bytes = raw.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'+' => {
                    out.push(b' ');
                    index += 1;
                }
                b'%' if index + 3 <= bytes.len() => {
                    match u8::from_str_radix(&raw[index + 1..index + 3], 16) {
                        Ok(byte) => {
                            out.push(byte);
                            index += 3;
                        }
                        Err(_) => {
                            out.push(bytes[index]);
                            index += 1;
                        }
                    }
                }
                byte => {
                    out.push(byte);
                    index += 1;
                }
            }
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    /// A config pointed at the stand-in, carrying `retention` verbatim.
    fn mock_config(url: String, retention: moraine_config::RetentionConfig) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = url;
        cfg.clickhouse.timeout_seconds = 10.0;
        cfg.retention = retention;
        cfg
    }

    // ---- a steerable ClickHouse stand-in for the migrate/`up` database path

    /// A steering function: maps one recorded statement to the HTTP status
    /// and body the stand-in answers with.
    type StatementAnswer = dyn Fn(&str) -> (u16, String) + Send + Sync;

    /// Statement-steered stand-in for `migrate_database_with_progress`:
    /// records every statement and lets `respond` choose each answer, so a
    /// test can let the schema migrations and the canonical sweep succeed
    /// while exactly one v1 projection statement fails the way the reference
    /// host's did. Same raw-TCP transport rationale as `ReclaimServerMock`.
    #[derive(Clone)]
    struct MigrateServerMock {
        statements: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        respond: std::sync::Arc<StatementAnswer>,
    }

    impl MigrateServerMock {
        fn new<F>(respond: F) -> Self
        where
            F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        {
            Self {
                statements: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                respond: std::sync::Arc::new(respond),
            }
        }

        fn count<P>(&self, predicate: P) -> usize
        where
            P: Fn(&str) -> bool,
        {
            self.statements
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|statement| predicate(statement))
                .count()
        }
    }

    async fn spawn_migrate_server_mock(mock: MigrateServerMock) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind migrate stand-in listener");
        let addr = listener.local_addr().expect("migrate stand-in addr");
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let mock = mock.clone();
                tokio::spawn(async move { serve_migrate_request(stream, mock).await });
            }
        });
        format!("http://{addr}")
    }

    async fn serve_migrate_request(mut stream: tokio::net::TcpStream, mock: MigrateServerMock) {
        let Some(statement) = read_http_statement(&mut stream).await else {
            return;
        };
        let (status, body) = (mock.respond)(&statement);
        mock.statements
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(statement);
        write_http_response(&mut stream, status, &body).await;
    }

    /// Default stand-in answer: JSONEachRow readers accept an empty body,
    /// while FORMAT JSON envelope readers (the migration preflight) need an
    /// empty `data` envelope. Mutations parse nothing.
    fn empty_result(statement: &str) -> (u16, String) {
        if statement.trim_start().starts_with("SELECT") && !statement.contains("FORMAT JSONEachRow")
        {
            return (200, "{\"data\":[]}".to_string());
        }
        (200, String::new())
    }

    /// One of migration 041's eight family drops, as it reaches the wire.
    fn is_retirement_drop(statement: &str) -> bool {
        statement.starts_with("DROP TABLE IF EXISTS") && statement.contains(".mcp_open_")
    }

    /// Migration 041's settle-by-drop ledger append.
    fn is_ledger_settle(statement: &str) -> bool {
        statement.starts_with("INSERT INTO") && statement.contains(".storage_reclaim_ledger")
    }

    /// The canonical sweep's durable `open_v2` readiness publication.
    fn is_open_v2_publish(statement: &str) -> bool {
        statement.contains("('open_v2', 1, generateSnowflakeID(), 'auto-local')")
    }

    /// The runner's `system.parts` footprint probe for the retired family, as
    /// it reaches the wire.
    fn is_family_footprint_probe(statement: &str) -> bool {
        statement.contains("system.parts") && statement.contains("'mcp_open_projection_state'")
    }

    /// What the family holds on the host a shape describes: the four figures
    /// `retired_family_footprint_sql` reports, in one place so a shape cannot
    /// declare a total and a content split that could not coexist.
    #[derive(Clone, Copy)]
    struct FamilyFootprint {
        family_rows: u64,
        family_bytes: u64,
        content_rows: u64,
        content_bytes: u64,
    }

    /// The marker five of the family's migrations seed without reading a corpus,
    /// measured on a real ClickHouse 25.12.5.44 server with bundled migrations
    /// 001–040 applied to an empty database (2026-08-01):
    /// `mcp_open_projection_state` held 2 rows / 392 B across 2 active parts,
    /// and the other seven retired tables held nothing. The count is merge
    /// state (ReplacingMergeTree collapsing seven seeds), never zero.
    ///
    /// Every shape below carries it, because every store that ran 027–035 has
    /// it. A shape whose `family_*` omitted it would be describing a store
    /// that cannot exist — and that is precisely the fiction that made the
    /// fresh-install arm look reachable when it was not.
    const BOOKKEEPING_SEED_ROWS: u64 = 2;
    const BOOKKEEPING_SEED_BYTES: u64 = 392;

    impl FamilyFootprint {
        /// A store that projected `content_rows` / `content_bytes`, plus the
        /// data-independent marker seed every store carries.
        fn projected(content_rows: u64, content_bytes: u64) -> Self {
            Self {
                family_rows: content_rows + BOOKKEEPING_SEED_ROWS,
                family_bytes: content_bytes + BOOKKEEPING_SEED_BYTES,
                content_rows,
                content_bytes,
            }
        }

        /// A fresh install: 027–035 ran, nothing was ever projected.
        fn fresh_install() -> Self {
            Self::projected(0, 0)
        }
    }

    /// Steering for a host in a given retirement shape: answers the
    /// mcp_read_index_state existence probe and the `open_v2` state read from
    /// a flag the sweep's own publish statement flips, and the `system.parts`
    /// footprint probe from the shape's [`FamilyFootprint`]. Everything else
    /// answers `empty_result`.
    ///
    /// The footprint arm matches on `system.parts` alone rather than on a
    /// particular table literal, so a change to the family list cannot quietly
    /// unsteer it and drop the walk back onto `empty_result`. It cannot fall
    /// through silently either way: `retired_family_footprint` treats an
    /// absent row as an error, so an unanswered probe aborts the migrate call
    /// instead of reading as an empty family.
    fn retirement_shape_steering(
        published: std::sync::Arc<std::sync::atomic::AtomicBool>,
        footprint: FamilyFootprint,
        publish_flips_readiness: bool,
    ) -> impl Fn(&str) -> (u16, String) + Send + Sync + 'static {
        move |statement: &str| {
            if is_open_v2_publish(statement) && publish_flips_readiness {
                published.store(true, std::sync::atomic::Ordering::SeqCst);
                return (200, String::new());
            }
            if statement.contains("name = 'mcp_read_index_state'") {
                return (200, "{\"value\":\"1\"}\n".to_string());
            }
            if statement.contains("state_key = 'open_v2'") {
                let ready = u8::from(published.load(std::sync::atomic::Ordering::SeqCst));
                return (
                    200,
                    format!(
                        "{{\"state_key\":\"open_v2\",\"ready\":{ready},\"generation\":\"4400000000000000\",\"cursor\":\"\"}}\n"
                    ),
                );
            }
            if statement.contains("system.parts") && statement.contains("mcp_open_") {
                let FamilyFootprint {
                    family_rows,
                    family_bytes,
                    content_rows,
                    content_bytes,
                } = footprint;
                return (
                    200,
                    format!(
                        "{{\"family_rows\":{family_rows},\"family_bytes\":{family_bytes},\
                          \"content_rows\":{content_rows},\"content_bytes\":{content_bytes}}}\n"
                    ),
                );
            }
            empty_result(statement)
        }
    }

    /// The three retirement host shapes (issue #603 WI-10), walked as the
    /// migrate/`up` sequence walks them against the stand-in server.
    ///
    /// **Shape (a) — already cut over** (the reference host: `open_v2.ready =
    /// 1`, family populated, drain mid-flight): migration 041 applies in the
    /// FIRST pass, its preflight note reports the compressed column bytes the
    /// drop returns, the eight drops and the settle-by-drop ledger append reach
    /// the wire, and nothing defers.
    ///
    /// MUTATION (executed 2026-07-31): invert the `ready` arm of
    /// `retirement_gate` (treat a published host as unpublished) => FAILS
    /// here on `deferred == 0` (the cut-over host defers instead of
    /// retiring), and `a_never_cut_over_host_defers_retirement_until_the_
    /// sweep_publishes` fails with it on the drop ordering.
    ///
    /// MUTATION (executed 2026-08-01): reword the note's column clause back to
    /// the pre-E3 "of on-disk bytes" => FAILS here on the whole-note equality.
    /// Under review round 6's `contains("20.70 GiB") && contains("23920
    /// rows")` it PASSED, which is how a quoted note in the PR body came to
    /// name a column the runner does not sum. **The wording half.**
    #[tokio::test]
    async fn a_cut_over_host_retires_the_projection_in_the_first_pass() {
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mock = MigrateServerMock::new(retirement_shape_steering(
            published,
            FamilyFootprint::projected(23_918, 22_221_999_608),
            false,
        ));
        let cfg = mock_config(
            spawn_migrate_server_mock(mock.clone()).await,
            moraine_config::RetentionConfig::default(),
        );

        let mut events: Vec<DatabaseProgress> = Vec::new();
        let outcome = migrate_database_with_progress(&cfg, |event| events.push(event))
            .await
            .expect("a cut-over host migrates cleanly");
        assert!(
            outcome.applied.iter().any(|version| version == "041"),
            "the retirement migration applied: {:?}",
            outcome.applied
        );

        let deferred = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    DatabaseProgress::Migration(MigrationProgress::Deferred { .. })
                )
            })
            .count();
        assert_eq!(deferred, 0, "a cut-over host never defers: {events:?}");
        let preflights: Vec<&String> = events
            .iter()
            .filter_map(|event| match event {
                DatabaseProgress::Migration(MigrationProgress::Preflight {
                    version: "041",
                    note,
                }) => Some(note),
                _ => None,
            })
            .collect();
        assert_eq!(preflights.len(), 1, "{events:?}");
        // The note WHOLE. `contains("20.70 GiB")` plus `contains("23920 rows")`
        // left every other word of the sentence free to drift, and it did: the
        // PR body quoted a pre-E3 "returns 20.70 GiB on disk" that the runner
        // had already stopped printing, and both substrings still matched.
        // What the note names is the COLUMN it summed, which is the whole
        // point of E3 — `sum(data_compressed_bytes)` and `sum(bytes_on_disk)`
        // differ by ~8 MiB across this family — so the clause naming it is
        // load-bearing, not decoration.
        //
        // This is also the other half of the sql/041 header pin. `sql/041`'s
        // header quotes RETIREMENT_PROCEED_NOTE_PREFIX (`storage_class::tests::
        // the_migration_header_quotes_the_note_the_runner_emits`); this is what
        // proves the constant is also what an operator sees. A migration is
        // immutable once released, so a quoted operator string moraine does not
        // print can never be corrected in place — only prevented.
        assert_eq!(
            preflights[0].as_str(),
            format!(
                "{}20.70 GiB of compressed column data (23920 rows across 8 tables; \
                 sum(data_compressed_bytes) over active system.parts, measured immediately \
                 before the drop — DROP TABLE ... SYNC deletes the parts before returning, \
                 so unlike a reclaim DELETE none of it is merge-deferred)",
                moraine_clickhouse::RETIREMENT_PROCEED_NOTE_PREFIX,
            ),
        );
        assert_eq!(mock.count(is_retirement_drop), 8, "all eight family drops");
        assert_eq!(mock.count(is_ledger_settle), 1, "the settle-by-drop append");
        let settle = {
            let statements = mock
                .statements
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            statements
                .iter()
                .find(|statement| is_ledger_settle(statement))
                .expect("settle statement recorded")
                .clone()
        };
        // Settle-by-drop semantics (mid-drain safe): the append advances the
        // two retired scopes' unsettled units to `abandoned`, never `done`.
        assert!(settle.contains("'abandoned'"), "{settle}");
        assert!(settle.contains("'mcp_open_orphan'"), "{settle}");
        assert!(settle.contains("'mcp_open_retired_lineage'"), "{settle}");
        assert!(settle.contains("'claimed', 'deleting'"), "{settle}");
        assert!(!settle.contains("'done'"), "{settle}");
    }

    /// **Shape (b) — never cut over, projection populated**: the first pass
    /// defers 041 with the named actionable reason, the canonical sweep runs
    /// and publishes `open_v2`, and the same startup's retry pass applies the
    /// retirement — every family drop reaches the wire strictly AFTER the
    /// readiness publication. This is the plan's deferred-cutover recovery
    /// recipe as one startup.
    ///
    /// MUTATION (executed 2026-07-31): drop the `if deferred` retry from
    /// `migrate_database_with_progress` => FAILS here ("041" never applied;
    /// zero drops on the wire) while the run still returns Ok — exactly the
    /// silent-deferral this walk exists to forbid.
    #[tokio::test]
    async fn a_never_cut_over_host_defers_retirement_until_the_sweep_publishes() {
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mock = MigrateServerMock::new(retirement_shape_steering(
            published,
            FamilyFootprint::projected(23_918, 22_221_999_608),
            true,
        ));
        let cfg = mock_config(
            spawn_migrate_server_mock(mock.clone()).await,
            moraine_config::RetentionConfig::default(),
        );

        let mut events: Vec<DatabaseProgress> = Vec::new();
        let outcome = migrate_database_with_progress(&cfg, |event| events.push(event))
            .await
            .expect("the deferred retirement retries in the same startup");
        assert!(
            outcome.applied.iter().any(|version| version == "041"),
            "the retry pass applied the retirement: {:?}",
            outcome.applied
        );

        let deferrals: Vec<&String> = events
            .iter()
            .filter_map(|event| match event {
                DatabaseProgress::Migration(MigrationProgress::Deferred {
                    version: "041",
                    reason,
                    ..
                }) => Some(reason),
                _ => None,
            })
            .collect();
        assert_eq!(deferrals.len(), 1, "{events:?}");
        assert!(
            deferrals[0].contains("has not cut over")
                && deferrals[0].contains("open_v2 is unpublished")
                && deferrals[0].contains("moraine db core-index rebuild"),
            "the deferral names the recovery recipe: {}",
            deferrals[0]
        );
        // The figure the deferral quotes is what would be LOST, so it counts
        // the seven content tables (23 918 rows) — not the family total
        // (23 920), which includes the marker seed no store is without. The
        // fresh-install walk is the arm that fails when the gate reads the
        // total; this is the arm that fails when the REASON does.
        assert!(
            deferrals[0].contains("23918 projected rows"),
            "the deferral reports the projected content it would lose, not the family total: {}",
            deferrals[0]
        );
        assert!(
            !deferrals[0].contains("23920"),
            "the family total is not what a deferral is about: {}",
            deferrals[0]
        );

        // Ordering on the wire: readiness published strictly before any drop.
        let statements = mock
            .statements
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let publish_at = statements
            .iter()
            .position(|statement| is_open_v2_publish(statement))
            .expect("the sweep published open_v2");
        let drop_positions: Vec<usize> = statements
            .iter()
            .enumerate()
            .filter_map(|(index, statement)| is_retirement_drop(statement).then_some(index))
            .collect();
        assert_eq!(drop_positions.len(), 8, "all eight family drops ran");
        assert!(
            drop_positions.iter().all(|index| *index > publish_at),
            "every drop must follow the readiness publication: publish at {publish_at}, \
             drops at {drop_positions:?}"
        );
    }

    /// **Shape (b), audit never passes**: the sweep runs but readiness stays
    /// withheld, so the retry pass defers AGAIN, the run still succeeds, and
    /// not one family drop reaches the wire — the projection bytes stay
    /// untouched until the operator fixes the audit (the reason keeps naming
    /// the recipe).
    #[tokio::test]
    async fn a_host_whose_audit_never_passes_keeps_its_projection_bytes() {
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // publish_flips_readiness = false: even if a publish statement
        // arrives, readiness stays 0 — the withheld-audit shape.
        let mock = MigrateServerMock::new(retirement_shape_steering(
            published,
            FamilyFootprint::projected(23_918, 22_221_999_608),
            false,
        ));
        let cfg = mock_config(
            spawn_migrate_server_mock(mock.clone()).await,
            moraine_config::RetentionConfig::default(),
        );

        let mut events: Vec<DatabaseProgress> = Vec::new();
        let outcome = migrate_database_with_progress(&cfg, |event| events.push(event))
            .await
            .expect("a still-deferred retirement is not a startup failure");
        assert!(
            !outcome.applied.iter().any(|version| version == "041"),
            "041 must not apply while open_v2 is unpublished: {:?}",
            outcome.applied
        );
        let deferrals = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    DatabaseProgress::Migration(MigrationProgress::Deferred { version: "041", .. })
                )
            })
            .count();
        assert_eq!(deferrals, 2, "both passes defer: {events:?}");
        assert_eq!(
            mock.count(is_retirement_drop),
            0,
            "no family drop may run before the cutover is durable"
        );
    }

    /// **Shape (c) — fresh install**: migrations 027–035 created the family
    /// moments earlier in the same pass and projected nothing into it, so the
    /// gate's no-projected-content arm applies 041 immediately — no deferral,
    /// and the note says the drop loses nothing. The canonical sweep then
    /// publishes over the empty corpus.
    ///
    /// The steering is a MEASUREMENT, not a convenience. Through review round
    /// 4 this walk ran on the unsteered `empty_result`, whose footprint answer
    /// is no rows at all; the zero it asserted was an artifact of the stand-in
    /// rather than a property of the schema, and against a real server the arm
    /// it claims to cover was unreachable. Executed 2026-08-01 against a
    /// ClickHouse 25.12.5.44 server with bundled migrations 001–040 applied to
    /// an empty database, the family holds 2 rows / 392 B — every byte of it
    /// `mcp_open_projection_state`, which migrations 027, 029, 033, 034 and
    /// 035 seed without reading a corpus across seven `INSERT`s (027's is the
    /// `WHERE NOT EXISTS` guard, 029's is a bare `VALUES`).
    /// [`FamilyFootprint::fresh_install`] is that measurement, and with it the
    /// round-4 build DEFERS here:
    ///
    /// ```text
    /// DEFERRED 041: … this store has not cut over to the canonical read
    /// indexes (open_v2 is unpublished) and the projection still holds 2 rows
    /// (392 B) … run `moraine db core-index rebuild` …
    /// ```
    ///
    /// The note is asserted WHOLE, not by three `contains`. Review round 6
    /// asserted three substrings of it and shipped "migrations 027 and 029" —
    /// two of the five — in the words between them, where nothing could see
    /// it. The seed clause is INTERPOLATED from
    /// [`moraine_clickhouse::BOOKKEEPING_SEED_CLAUSE`] rather than transcribed,
    /// which splits the job cleanly in two: this walk pins that an operator is
    /// shown that clause, and
    /// `storage_class::tests::the_bookkeeping_table_is_the_one_the_migrations_seed_without_reading_data`
    /// pins that the clause is what the migrations actually do. Rewording the
    /// constant therefore does NOT fail here — it fails there, which is the
    /// half that can tell right from wrong.
    ///
    /// MUTATION (executed 2026-08-01): gate `retirement_gate`'s third arm on
    /// `rows == 0` (the family total) instead of `content_rows == 0` => FAILS
    /// here on `a fresh install never defers`, and on that assertion ONLY:
    /// `cargo test -p moraine --locked --no-fail-fast` gives 247 passed / 1
    /// failed and one panic. The `041 applied on the first pass` assertion is
    /// checked FIRST and PASSES, because the mutation defers 041 in pass one
    /// and the retried pass applies it, so `outcome.applied` still carries
    /// `041` — the event dump the panic prints ends in
    /// `Applied { … version: "041" … }`. A mutation record that names two
    /// failures where the run produces one is claiming coverage the guard
    /// does not have. **The behaviour half.**
    ///
    /// MUTATION (executed 2026-08-01): delete `{BOOKKEEPING_SEED_CLAUSE}` from
    /// `retirement_gate`'s note, leaving "…the 2-row (392 B)
    /// mcp_open_projection_state marker, so the drop loses nothing" => FAILS
    /// here on the whole-note equality. Under round 6's three `contains` it
    /// passed. **The wording half.**
    ///
    /// What this walk does NOT cover, and must not be read as covering: WHICH
    /// tables the footprint statement sums. The steering answers the probe
    /// with four figures directly, so the stand-in never reports a table list.
    /// Through review round 6 this docstring claimed the opposite — that
    /// flipping `mcp_open_projection_state`'s `holds_projected_content` "FAILS
    /// here identically". Executed 2026-08-01 across `cargo test -p moraine -p
    /// moraine-clickhouse --locked --no-fail-fast`, that flip gives 511 passed
    /// / 1 failed: all 248 `moraine` tests pass, this walk among them, and the
    /// single failure is the derivation guard named above. This walk covers the
    /// gate's BEHAVIOUR under a measured footprint; the split itself is that
    /// guard's, and saying otherwise made a guard name a promise it did not
    /// keep.
    #[tokio::test]
    async fn a_fresh_install_retires_the_empty_projection_without_deferring() {
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mock = MigrateServerMock::new(retirement_shape_steering(
            published,
            FamilyFootprint::fresh_install(),
            true,
        ));
        let cfg = mock_config(
            spawn_migrate_server_mock(mock.clone()).await,
            moraine_config::RetentionConfig::default(),
        );

        let mut events: Vec<DatabaseProgress> = Vec::new();
        let outcome = migrate_database_with_progress(&cfg, |event| events.push(event))
            .await
            .expect("a fresh install migrates cleanly");
        assert!(
            outcome.applied.iter().any(|version| version == "041"),
            "041 applied on the first pass: {:?}",
            outcome.applied
        );
        assert!(
            !events.iter().any(|event| matches!(
                event,
                DatabaseProgress::Migration(MigrationProgress::Deferred { .. })
            )),
            "a fresh install never defers: {events:?}"
        );
        let note = events
            .iter()
            .find_map(|event| match event {
                DatabaseProgress::Migration(MigrationProgress::Preflight {
                    version: "041",
                    note,
                }) => Some(note.clone()),
                _ => None,
            })
            .expect("the retirement preflight note fires");
        // The note WHOLE, not three substrings of it. Everything the
        // fresh-install note has to say has to be in it: nothing projected,
        // the marker rows named rather than hidden (an operator who greps
        // `system.parts` after this note must not find two rows the note
        // claimed were not there), the seeding migrations named correctly, and
        // the reason the drop is safe. The seed clause is INTERPOLATED from
        // the constant the derivation guard checks rather than transcribed, so
        // this walk pins that the operator sees that clause while the
        // derivation pins that the clause is true.
        assert_eq!(
            note,
            format!(
                "retiring the legacy mcp_open_* projection: the family holds no projected \
                 rows — only the 2-row (392 B) mcp_open_projection_state marker {}, so the \
                 drop loses nothing (canonical read-index readiness publishes when the sweep \
                 next runs)",
                moraine_clickhouse::BOOKKEEPING_SEED_CLAUSE,
            ),
        );
        // The walk is steered, not defaulted: the footprint probe reached the
        // wire and this shape answered it.
        assert_eq!(
            mock.count(is_family_footprint_probe),
            1,
            "the gate must have measured the family exactly once"
        );
        assert_eq!(mock.count(is_retirement_drop), 8);
        let published: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                DatabaseProgress::CoreIndex(CoreIndexBackfillProgress::Published {
                    core_indexes,
                    open_v2,
                }) => Some((*core_indexes, *open_v2)),
                _ => None,
            })
            .collect();
        assert_eq!(
            published,
            vec![(true, true)],
            "the canonical sweep published, open_v2 auto-flipped"
        );
    }

    /// Steers the canonical readiness publish to a server error while every
    /// other statement succeeds, so the sweep — not the v1 projection, and
    /// not the retirement gate — is the thing that fails.
    fn canonical_publish_failure(statement: &str) -> (u16, String) {
        if statement.contains("('core_indexes', 1, generateSnowflakeID()") {
            return (
                500,
                "Code: 241. DB::Exception: injected canonical publish failure".to_string(),
            );
        }
        // The migration pass has to get PAST the retirement gate for the sweep
        // to be reached at all, so the footprint probe is answered with the
        // fresh-install measurement. An unanswered probe is an error now, and
        // this test would then be asserting the wrong abort.
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        retirement_shape_steering(published, FamilyFootprint::fresh_install(), false)(statement)
    }

    /// The non-fatal catch covers the v1 projection ONLY. A canonical-sweep
    /// failure must still abort startup: the sweep is the read path the
    /// cutover makes load-bearing, and degrading past it would leave readers
    /// with neither a fresh v1 projection nor a published canonical index.
    /// This pins the fatal/non-fatal boundary from the side the two tests
    /// above cannot see.
    ///
    /// MUTATION (executed 2026-07-31): swallow the
    /// `backfill_canonical_read_indexes` error at its call site in
    /// `migrate_database_with_progress` (`let _ = ...await;`) — this test
    /// fails: the function returns Ok and the error context never surfaces.
    #[tokio::test]
    async fn a_canonical_sweep_failure_still_aborts_startup() {
        let mock = MigrateServerMock::new(canonical_publish_failure);
        let cfg = mock_config(
            spawn_migrate_server_mock(mock.clone()).await,
            moraine_config::RetentionConfig::default(),
        );

        let error = migrate_database_with_progress(&cfg, |_| {})
            .await
            .expect_err("a canonical sweep failure must abort, not degrade");
        let chain = format!("{error:#}");
        assert!(
            chain.contains("failed to backfill canonical read indexes"),
            "the abort must carry the canonical-sweep context, got: {chain}"
        );
    }

    /// A `[retention]` whose derived horizon differs from the stock one, so a
    /// defaulted config cannot pass by coincidence.
    fn widened_retention() -> moraine_config::RetentionConfig {
        moraine_config::RetentionConfig {
            derived_horizon_hours: 72.0,
            ..moraine_config::RetentionConfig::default()
        }
    }

    fn horizon_seconds(retention: &moraine_config::RetentionConfig) -> u64 {
        retention.derived_horizon_seconds().max(0.0) as u64
    }

    /// The configured and stock horizons, asserted distinct: without this the
    /// "a default did not reach the server" half of every horizon guard below
    /// is vacuous.
    fn distinct_horizons(retention: &moraine_config::RetentionConfig) -> (u64, u64) {
        let configured = horizon_seconds(retention);
        let stock = horizon_seconds(&moraine_config::RetentionConfig::default());
        assert_ne!(
            configured, stock,
            "the test horizon must differ from the default, or a defaulted config passes"
        );
        (configured, stock)
    }

    /// Statement fragment unique to each registered scope's candidate probe —
    /// the same signatures `sink::tests::RECLAIM_PROBE_SIGNATURES` uses, so
    /// "the probe was issued" means the same thing on both surfaces. (The two
    /// retired `mcp_open` scopes' signatures left with their executors,
    /// issue #603 WI-10.)
    const READ_INDEX_PROBE_SIGNATURE: &str = "ri_rollup";
    const CANONICAL_PROBE_SIGNATURE: &str = "cg_rollup";

    /// Registered scopes `retention` authorizes to probe. Both remaining
    /// scopes are registered, so the count varies with the config alone:
    /// one without the protected keys, two with both set.
    fn authorized_probing_scopes(retention: &moraine_config::RetentionConfig) -> usize {
        moraine_clickhouse::ReclaimScope::ALL
            .into_iter()
            .filter(|scope| {
                moraine_clickhouse::reclaim::executor_for(*scope).is_some()
                    && moraine_clickhouse::ReclaimAuthority::for_scope(*scope, retention).is_ok()
            })
            .count()
    }

    /// **G-CONFIRM.** Fails for: an unconfirmed `moraine db reclaim run`
    /// proceeding.
    /// Denomination: end-to-end behaviour of `cmd_db_reclaim_run`, not a
    /// re-derivation of its decision.
    ///
    /// The probe is `--scope canonical_generation` under a stock config,
    /// because that is the one scope whose two outcomes are distinguishable
    /// from outside: refused it returns `Ok`, proceeded it reaches
    /// `reclaim_run`'s authority check and returns `Err` naming the missing
    /// key. No stdout capture is needed, and nothing touches the network on
    /// either path.
    ///
    /// MUTATION (executed 2026-07-27): change `if !args.confirm` to
    /// `if false` in `cmd_db_reclaim_run` => FAILS here (the unconfirmed call
    /// returns `Err`). Before this test, that mutation left the CLI suite at
    /// 229/0 and an unconfirmed run against user history proceeded with no
    /// test noticing. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-27): change it to `if true` (refuse even a
    /// confirmed run) => FAILS on the confirmed half below, which requires the
    /// authority refusal to be reached. **Upper bound.**
    ///
    /// MUTATION (executed 2026-07-27): change it to `if !args.json` => FAILS
    /// on both halves. **Width: the gate reads `confirm` and nothing else.**
    #[tokio::test(flavor = "multi_thread")]
    async fn the_unconfirmed_run_ceremony_is_enforced_end_to_end() {
        let cfg = offline_config(moraine_config::RetentionConfig::default());

        // Unconfirmed: refused locally, exit non-zero, no authority check and
        // no client construction.
        let code = cmd_db_reclaim_run(
            &cfg,
            &plain_output(),
            reclaim_run_args("canonical_generation", false),
        )
        .await
        .expect("an unconfirmed run must refuse, not error");
        assert_eq!(code, ExitCode::from(RECLAIM_REFUSAL_EXIT_CODE));

        // Confirmed: the gate lets it through to `reclaim_run`, whose S2
        // authority check refuses this scope under a stock config and names
        // the key. This is what proves the gate above is `confirm` and not a
        // blanket refusal.
        let error = cmd_db_reclaim_run(
            &cfg,
            &plain_output(),
            reclaim_run_args("canonical_generation", true),
        )
        .await
        .expect_err("a confirmed run of an unconfigured bucket-1 scope must refuse");
        assert!(
            error
                .to_string()
                .contains("retention.canonical_history_horizon_days"),
            "{error:#}"
        );
    }

    /// **G-EXITCODE.** Fails for: a reclaim refusal exiting zero.
    /// Denomination: the `ExitCode` the command actually returns.
    ///
    /// The report's claim — "a script expecting reclamation notices it did not
    /// happen" — had no test behind it.
    ///
    /// MUTATION (executed 2026-07-27): change `ExitCode::from(1)` on the
    /// unconfirmed path to `ExitCode::SUCCESS` => FAILS in
    /// `the_unconfirmed_run_ceremony_is_enforced_end_to_end`.
    ///
    /// MUTATION (executed 2026-07-27): make `reclaim_run_exit_code` return `0`
    /// for `NoExecutor` => FAILS here on the pure mapping. (When first
    /// executed this also failed an end-to-end no-executor call; that call
    /// was retired with WI-09, which registered the last executor — the
    /// variant is now reachable only in a downgrade build, and the guard on
    /// it is the mapping row below.) **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-27): make it return `1` for every variant =>
    /// FAILS on the `Idle`/`Settled` rows, so "always fail" is not a passing
    /// "fix". **Upper bound, and width: each variant is named.**
    ///
    /// MUTATION (executed 2026-07-28): move `LowDisk` to the `0` arm => FAILS
    /// on the low-disk row. **Width: the variant is unreachable from this
    /// command today — the CLI runs as `Operator`, which skips the free-space
    /// check — so its arm was matched deliberately and asserted by nothing. A
    /// later trigger change makes it reachable, and the silent-success
    /// mapping it would have inherited is precisely what
    /// `RECLAIM_REFUSAL_EXIT_CODE` exists to prevent.**
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refusal_exits_non_zero() {
        // The `NoExecutor` refusal is no longer reachable end-to-end: every
        // scope has an executor as of WI-09, so an authorized confirmed run
        // reaches the network by design. What remains checkable end-to-end is
        // that a fully-registered build still refuses locally where it must —
        // `the_unconfirmed_run_ceremony_is_enforced_end_to_end` holds the
        // unconfirmed exit code and the missing-key error — while this test
        // pins the outcome→exit-code mapping variant by variant, `NoExecutor`
        // included, because a downgrade build reaches it again and the
        // silent-success mapping is what `RECLAIM_REFUSAL_EXIT_CODE` exists
        // to prevent.
        assert!(
            moraine_clickhouse::ReclaimScope::ALL
                .into_iter()
                .all(|scope| moraine_clickhouse::reclaim::executor_for(scope).is_some()),
            "if a scope lost its executor, restore the end-to-end no-executor probe this \
             test carried before WI-09"
        );

        // And the mapping itself, variant by variant.
        use moraine_clickhouse::ReclaimOutcome;
        let scope = moraine_clickhouse::ReclaimScope::ReadIndexGeneration;
        assert_eq!(
            reclaim_run_exit_code(&ReclaimOutcome::NoExecutor {
                scope,
                message: String::new()
            }),
            1
        );
        assert_eq!(
            reclaim_run_exit_code(&ReclaimOutcome::Blocked {
                scope,
                pending_mutations: 3
            }),
            1,
            "H9: a blocked run must not be indistinguishable from a successful one at the process \
             boundary either"
        );
        assert_eq!(
            reclaim_run_exit_code(&ReclaimOutcome::LowDisk {
                scope,
                free_bytes: 1,
                required_bytes: 2
            }),
            RECLAIM_REFUSAL_EXIT_CODE,
            "a run that declined for free disk reclaimed nothing, and a script that assumed it \
             did must notice"
        );
        assert_eq!(reclaim_run_exit_code(&ReclaimOutcome::Idle { scope }), 0);
        assert_eq!(
            reclaim_run_exit_code(&ReclaimOutcome::Settled {
                scope,
                units: 0,
                estimated_rows: 0,
                redriven: 0,
                failed: 0,
                abandoned: 0,
                denomination: String::new()
            }),
            0
        );
    }

    /// An unknown `--scope` must not widen to "everything".
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_scope_is_an_error_rather_than_a_default() {
        let cfg = offline_config(moraine_config::RetentionConfig::default());
        let error = cmd_db_reclaim_run(&cfg, &plain_output(), reclaim_run_args("everything", true))
            .await
            .expect_err("an unknown scope must not run");
        let rendered = error.to_string();
        assert!(rendered.contains("unknown reclaim scope"), "{rendered}");
        for scope in moraine_clickhouse::ReclaimScope::ALL {
            assert!(rendered.contains(scope.as_str()), "{rendered}");
        }
    }

    /// **G-CLI-RUN-RETENTION.** The operator's `run` drives the reclaimer with
    /// the **operator's** `[retention]` config, observed by the horizon that
    /// reached the candidate probe.
    /// Denomination: the `toIntervalSecond(…)` literal on the issued probe.
    ///
    /// This is the destructive path: `run --confirm` is the one call site that
    /// deletes on demand. `sink::tests::the_unattended_janitor_uses_the_
    /// operators_retention_horizon` closes exactly this for the janitor and
    /// states the consequence, which is identical here: the horizon is the only
    /// thing separating the reclaimer from a source generation whose
    /// supersession is still settling. An operator who widens
    /// `retention.derived_horizon_hours` because their host publishes slowly
    /// would have got the stock 24h on `reclaim run --confirm`, and the
    /// reclaimer would have collected every generation superseded between 24h
    /// and their configured horizon.
    ///
    /// MUTATION (executed 2026-07-28): pass `&RetentionConfig::default()` to
    /// `reclaim_run` from `cmd_db_reclaim_run` => FAILS here, and **only**
    /// here: re-run with this one test skipped, the same mutation
    /// leaves the rest of the workspace green — measured 2026-07-28 as "the remaining 1601", a denominator this PR moved (WI-10 deletes tests with the code they covered) and did NOT re-measure; the isolation was observed, the current count was not. **Lower bound, and the finding.**
    #[tokio::test(flavor = "multi_thread")]
    async fn the_operator_run_uses_the_operators_retention_horizon() {
        let retention = widened_retention();
        let (configured, stock) = distinct_horizons(&retention);
        let mock = ReclaimServerMock::roomy();
        let cfg = mock_config(spawn_reclaim_server_mock(mock.clone()).await, retention);

        let code = cmd_db_reclaim_run(
            &cfg,
            &plain_output(),
            reclaim_run_args("read_index_generation", true),
        )
        .await
        .expect("a confirmed run of a registered bucket-3 scope reaches the server");
        assert_eq!(
            code,
            ExitCode::SUCCESS,
            "nothing to reclaim is not a refusal"
        );

        assert!(
            mock.count(READ_INDEX_PROBE_SIGNATURE) > 0,
            "the run never issued its candidate probe"
        );
        assert_eq!(
            mock.count(&format!("toIntervalSecond({configured})")),
            1,
            "the run's probe must carry the configured horizon, not a default"
        );
        assert_eq!(
            mock.count(&format!("toIntervalSecond({stock})")),
            0,
            "a defaulted horizon reached the server"
        );
    }

    /// **G-CLI-TRIGGER.** The operator's `run` is an *attended* one, observed by
    /// what it did on a nearly-full disk rather than by reading the token at
    /// the call site.
    /// Denomination: the statements the command issued after the disk read.
    ///
    /// `ReclaimTrigger::Operator` is a single token in `cmd_db_reclaim_run`,
    /// and it is the entire "an operator may need to reclaim precisely because
    /// the disk is full" story this module repeats in four places.
    /// `reclaim::tests::only_the_unattended_trigger_refuses_to_start_on_a_full_disk`
    /// proves the two triggers differ, but it calls `reclaim_run` directly with
    /// each one — nothing asserted which token the **operator's** tick passes.
    /// It is the exact mirror of the janitor finding `sink::tests::the_
    /// unattended_janitor_declines_to_reclaim_on_a_nearly_full_disk` closed.
    ///
    /// Fail-closed in direction — the mutation refuses rather than deletes —
    /// which is why it needs a test rather than an incident: it silently
    /// removes the only reclamation route left on a host that has run out of
    /// disk.
    ///
    /// MUTATION (executed 2026-07-28): change `cmd_db_reclaim_run`'s
    /// `ReclaimTrigger::Operator` to `Maintenance` => FAILS here twice: the
    /// probe is never issued and the command exits `RECLAIM_REFUSAL_EXIT_CODE`.
    /// With this one test skipped the same mutation
    /// leaves the rest of the workspace green — measured 2026-07-28 as "the remaining 1601", a denominator this PR moved (WI-10 deletes tests with the code they covered) and did NOT re-measure; the isolation was observed, the current count was not. **Lower bound, and the finding.**
    ///
    /// MUTATION (executed 2026-07-28): raise the stand-in's free space above
    /// `RECLAIM_MIN_FREE_BYTES` => FAILS here, which is the point: this test
    /// distinguishes "ran despite the disk" from "the disk was never low", and
    /// `the_operator_run_uses_the_operators_retention_horizon` above holds the
    /// roomy-disk side. **Width.**
    #[tokio::test(flavor = "multi_thread")]
    async fn the_operators_run_is_not_gated_on_free_disk() {
        // One gigabyte free: below the 10 GiB the unattended trigger requires.
        let mock = ReclaimServerMock::with_free_bytes(1024 * 1024 * 1024);
        assert!(
            mock.free_bytes < moraine_clickhouse::reclaim::RECLAIM_MIN_FREE_BYTES,
            "the stand-in must report less headroom than the unattended gate demands"
        );
        let cfg = mock_config(
            spawn_reclaim_server_mock(mock.clone()).await,
            moraine_config::RetentionConfig::default(),
        );

        let code = cmd_db_reclaim_run(
            &cfg,
            &plain_output(),
            reclaim_run_args("read_index_generation", true),
        )
        .await
        .expect("a confirmed operator run must not error on a full disk");

        // It got as far as the free-space read — otherwise this would pass for
        // a run that never reached the preamble at all.
        assert!(
            mock.count("system.disks") > 0,
            "the reclaim preamble never read free space"
        );
        assert!(
            mock.count(READ_INDEX_PROBE_SIGNATURE) > 0,
            "the operator's run declined for free disk"
        );
        assert_eq!(
            code,
            ExitCode::SUCCESS,
            "a run that reached an empty candidate set is not a refusal"
        );
    }

    /// **G-CLI-PLAN-RETENTION.** The dry-run planner is driven with the
    /// operator's `[retention]` config, observed by the horizon that reached
    /// the probe.
    /// Denomination: the `toIntervalSecond(…)` literal on the issued probe.
    ///
    /// The planner is not a lesser surface for being read-only: `plan` and
    /// `run` probe through the same `reclaim_candidates` call precisely so the
    /// plan an operator reads is the plan a run claims. A plan computed at the
    /// stock horizon while the run deletes at the configured one reports a unit
    /// count the run does not claim, which is the one thing a dry run exists
    /// not to do.
    ///
    /// MUTATION (executed 2026-07-28): pass `&RetentionConfig::default()` to
    /// `reclaim_plan` from `cmd_db_reclaim_plan` => FAILS here, and only here:
    /// with this one test skipped the same mutation
    /// leaves the rest of the workspace green — measured 2026-07-28 as "the remaining 1601", a denominator this PR moved (WI-10 deletes tests with the code they covered) and did NOT re-measure; the isolation was observed, the current count was not. **Lower bound, and the finding.**
    #[tokio::test(flavor = "multi_thread")]
    async fn the_planner_uses_the_operators_retention_horizon() {
        let retention = widened_retention();
        let (configured, stock) = distinct_horizons(&retention);
        let mock = ReclaimServerMock::roomy();
        let cfg = mock_config(spawn_reclaim_server_mock(mock.clone()).await, retention);

        let code = cmd_db_reclaim_plan(
            &cfg,
            &plain_output(),
            crate::cli::ReclaimPlanArgs {
                scope: None,
                json: false,
            },
        )
        .await
        .expect("a full-scope plan reaches the server");
        assert_eq!(code, ExitCode::SUCCESS);

        // Every scope the config authorizes probes; the rest issue no
        // statement at all. `widened_retention` carries no protected key, so
        // the three bucket-3 scopes probe at the derived horizon and the
        // canonical scope — registered as of WI-09 — is refused into a note
        // instead of a probe, which is itself asserted: a plan that probed
        // user history under an unauthorized config would be the S2 refusal
        // failing at this call site.
        let probing = authorized_probing_scopes(&cfg.retention);
        assert_eq!(
            probing, 1,
            "no protected key is set, so canonical must not probe"
        );
        assert!(mock.count(READ_INDEX_PROBE_SIGNATURE) > 0);
        assert_eq!(
            mock.count(CANONICAL_PROBE_SIGNATURE),
            0,
            "an unauthorized canonical scope issued its probe"
        );
        assert_eq!(
            mock.count(&format!("toIntervalSecond({configured})")),
            probing,
            "every authorized scope's probe must carry the configured horizon"
        );
        assert_eq!(
            mock.count(&format!("toIntervalSecond({stock})")),
            0,
            "a defaulted horizon reached the server"
        );
    }

    /// **G-CLI-STATUS-RETENTION.** `moraine db reclaim status` — and the
    /// `status` panel and `db doctor` block that share `gather_reclaim_status`
    /// — are driven with the operator's `[retention]` config.
    /// Denomination: the `toIntervalSecond(…)` literal on the issued probes,
    /// plus the report's own availability.
    ///
    /// `reclaim_status` reports the effective retention policy *and* plans
    /// every scope through it, so a defaulted config here makes the panel
    /// disagree with the run twice over: in the policy block an operator reads
    /// to confirm their config took effect, and in the unit counts.
    ///
    /// MUTATION (executed 2026-07-28): pass `&RetentionConfig::default()` to
    /// `reclaim_status` from `gather_reclaim_status` => FAILS here, and only
    /// here: with this one test skipped the same mutation
    /// leaves the rest of the workspace green — measured 2026-07-28 as "the remaining 1601", a denominator this PR moved (WI-10 deletes tests with the code they covered) and did NOT re-measure; the isolation was observed, the current count was not. **Lower bound, and the finding.**
    #[tokio::test(flavor = "multi_thread")]
    async fn the_status_report_uses_the_operators_retention_horizon() {
        let retention = widened_retention();
        let (configured, stock) = distinct_horizons(&retention);
        let mock = ReclaimServerMock::roomy();
        let cfg = mock_config(spawn_reclaim_server_mock(mock.clone()).await, retention);

        let report = gather_reclaim_status(&cfg).await;
        assert!(
            report.available,
            "status did not complete: {:?}",
            report.error
        );

        let probing = authorized_probing_scopes(&cfg.retention);
        assert_eq!(
            probing, 1,
            "no protected key is set, so canonical must not probe"
        );
        assert_eq!(
            mock.count(CANONICAL_PROBE_SIGNATURE),
            0,
            "an unauthorized canonical scope issued its probe"
        );
        assert_eq!(
            mock.count(&format!("toIntervalSecond({configured})")),
            probing,
            "every authorized scope's probe must carry the configured horizon"
        );
        assert_eq!(
            mock.count(&format!("toIntervalSecond({stock})")),
            0,
            "a defaulted horizon reached the server"
        );
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
    fn rebuild_withholds_auto_publish_for_operator_promoted_backends() {
        // An open_v2 row published via the promote ceremony marks the backend
        // as Shared-operated: the rebuild must not republish it as auto-local.
        let promoted = state_row(
            STATE_KEY_OPEN_V2,
            1,
            snowflake_for(1),
            OPEN_V2_PROVENANCE_OPERATOR_PROMOTE,
        );
        assert!(!rebuild_publication_mode_is_local(Some(&promoted)));

        // Auto-local publications and unpublished/absent rows keep the CLI's
        // Local auto-publish.
        let auto_local = state_row(STATE_KEY_OPEN_V2, 1, snowflake_for(1), "auto-local");
        assert!(rebuild_publication_mode_is_local(Some(&auto_local)));
        let unpublished = state_row(STATE_KEY_OPEN_V2, 0, 0, "");
        assert!(rebuild_publication_mode_is_local(Some(&unpublished)));
        // A zeroed row that still carries the promote provenance is not
        // published (ready = 0): the Local gate stays on.
        let revoked = state_row(
            STATE_KEY_OPEN_V2,
            0,
            snowflake_for(2),
            OPEN_V2_PROVENANCE_OPERATOR_PROMOTE,
        );
        assert!(rebuild_publication_mode_is_local(Some(&revoked)));
        assert!(rebuild_publication_mode_is_local(None));
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

    /// Issue #603 WI-10 config compatibility at the status surface: a config
    /// still saying `v1` reports the v2 reader as effective and renders the
    /// retirement note (the same string the MCP backend logs).
    #[test]
    fn build_core_index_report_retired_v1_selector_serves_v2_with_the_note() {
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
        assert_eq!(report.effective_open_reader, "v2");
        assert!(report.open_reader_override);
        assert_eq!(
            report.open_reader_note.as_deref(),
            Some(OpenReaderMode::RETIRED_V1_NOTE)
        );
    }

    #[test]
    fn build_core_index_report_unready_is_an_error_carrying_the_recovery_note() {
        let state = RawCoreIndexState {
            core_indexes: Some(state_row(STATE_KEY_CORE_INDEXES, 0, 0, "")),
            open_v2: Some(state_row(STATE_KEY_OPEN_V2, 0, 0, "")),
            audit: None,
        };
        let report = build_core_index_report(OpenReaderMode::V2, state);
        assert_eq!(report.effective_open_reader, "error");
        // `v2` is a synonym for `auto` after issue #603 WI-10: it forces
        // nothing, so it is not an override. Prominence comes from the
        // effective reader being `error`, which the concise status branch ORs
        // in on its own.
        assert!(!report.open_reader_override);
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
        // Ready core indexes but open_v2 not published: with the v1 reader
        // retired (issue #603 WI-10) there is nothing to stay on — the
        // surface reports the typed-unready state. It is NOT an override: the
        // config says `auto` and contains no reader selection to override.
        // Post-041 this is the normal state of a fresh install between
        // `migrate` and the first sweep.
        assert_eq!(report.effective_open_reader, "error");
        assert!(!report.open_reader_override);
    }

    /// **The `open_reader_override` denominator.** The flag renders as
    /// "(config override)" on the operator's reader line, so it is a claim
    /// about the contents of their `moraine.toml`. After issue #603 WI-10 it
    /// is true for exactly one configured value — the retired `v1` — because
    /// `auto` and `v2` are synonyms that force nothing and an unready store is
    /// a readiness fact rather than a config one.
    ///
    /// Exhaustive over `OpenReaderMode` × readiness rather than sampled, so
    /// neither direction can drift: a hardcoded `true` on the unready arm
    /// (which is what shipped into round 1, and made
    /// `build_core_index_report_hides_provenance_when_not_promoted` certify
    /// the wrong claim) fails on the `auto`/`v2` unready rows, and a hardcoded
    /// `false` fails on both `v1` rows.
    ///
    /// MUTATION (executed 2026-08-01): restore `true` on the `Unready` arm of
    /// `build_core_index_report` => FAILS here on `(auto, unready)`.
    #[test]
    fn the_open_reader_override_flag_marks_exactly_the_retired_v1_config() {
        for (configured, ready, expected_override) in [
            (OpenReaderMode::Auto, true, false),
            (OpenReaderMode::Auto, false, false),
            (OpenReaderMode::V2, true, false),
            (OpenReaderMode::V2, false, false),
            (OpenReaderMode::V1, true, true),
            (OpenReaderMode::V1, false, true),
        ] {
            let state = RawCoreIndexState {
                core_indexes: Some(state_row(
                    STATE_KEY_CORE_INDEXES,
                    u8::from(ready),
                    snowflake_for(1),
                    "{}",
                )),
                open_v2: Some(state_row(
                    STATE_KEY_OPEN_V2,
                    u8::from(ready),
                    snowflake_for(1),
                    "auto-local",
                )),
                audit: None,
            };
            let report = build_core_index_report(configured, state);
            assert_eq!(
                report.open_reader_override,
                expected_override,
                "configured={}, open_v2 ready={ready}",
                configured.as_str()
            );
            assert_eq!(
                report.effective_open_reader,
                if ready { "v2" } else { "error" },
                "configured={}, open_v2 ready={ready}",
                configured.as_str()
            );
        }
    }

    /// The rebuild's operator warning states a REASON, and after issue #603
    /// WI-10 that reason cannot be "they serve v1" — this PR deletes the v1
    /// reader, so a restarted process reads the readiness the rebuild just
    /// revoked and refuses `open` with the typed unready error.
    ///
    /// The message shipped byte-identical to its pre-WI-10 text through round
    /// 1 and nothing was red, because four `eprintln!` calls have no seam.
    ///
    /// MUTATION (executed 2026-08-01): restore the original fourth line
    /// ("… so they serve v1 until this rebuild republishes readiness") =>
    /// FAILS here on the no-fallback assertion.
    #[test]
    fn the_rebuild_warning_promises_a_typed_refusal_not_a_v1_fallback() {
        let warning = REBUILD_RESTART_WARNING.join(" ");
        // The action survives: restart, with the command an operator can run.
        assert!(warning.contains("moraine down && moraine up"), "{warning}");
        // The reason is the typed refusal, and the retirement is named.
        assert!(warning.contains("typed error"), "{warning}");
        assert!(warning.contains("#603"), "{warning}");
        // No surface may promise a reader this build does not contain.
        for forbidden in ["serve v1", "serves v1", "serving v1", "fall back to v1"] {
            assert!(
                !warning.contains(forbidden),
                "the v1 reader is retired; `{forbidden}` promises a fallback that does not \
                 exist: {warning}"
            );
        }
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
