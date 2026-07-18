use anyhow::Result;
use dialoguer::console::Style;
use moraine_clickhouse::MigrationProgress;
use moraine_config::AppConfig;
use std::future::Future;
use std::io::Write;
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};
use tokio::time::{interval_at, Instant as TokioInstant, MissedTickBehavior};

use crate::cli::UpArgs;
use crate::managed_clickhouse::{start_clickhouse_with_progress, ClickHouseStartupProgress};
use crate::paths::{ensure_runtime_dirs, runtime_paths, RuntimePaths};
use crate::process::{
    preflight_required_service_binaries, start_background_service, StartOutcome, StartState,
};
#[cfg(test)]
use crate::progress::ProgressStyle;
use crate::progress::ProgressTree;
use crate::render::{render_up, CliOutput, MigrationOutcome, StatusSnapshot, UpSnapshot};
use crate::service::Service;

use super::{
    conversation_repository, doctor_is_healthy, migrate_database_for_up, status::cmd_status,
    DatabaseProgress,
};

const PROGRESS_REFRESH: Duration = Duration::from_millis(250);

const FAILURE_LOG_LINE_LIMIT: usize = 10;

fn sanitize_log_line(line: &str) -> String {
    let mut sanitized = String::with_capacity(line.len());
    for character in line.chars() {
        if character.is_control() {
            sanitized.extend(character.escape_default());
        } else {
            sanitized.push(character);
        }
    }
    sanitized
}

enum DatabaseActivity {
    Inspecting {
        started: Instant,
    },
    Migration {
        index: usize,
        total: usize,
        name: &'static str,
        started: Instant,
    },
    ReconciliationInspection {
        started: Instant,
    },
    Reconciliation {
        processed: usize,
        started: Instant,
    },
}

struct StartupProgress<W> {
    tree: ProgressTree<W>,
    started_at: Instant,
    database_activity: Option<DatabaseActivity>,
    launched: Vec<StartOutcome>,
    verbose: bool,
}

impl StartupProgress<std::io::Stderr> {
    fn from_output(output: &CliOutput) -> Self {
        Self::new(ProgressTree::from_output(output), output.verbose)
    }
}

impl<W: Write> StartupProgress<W> {
    fn new(tree: ProgressTree<W>, verbose: bool) -> Self {
        Self {
            tree,
            started_at: Instant::now(),
            database_activity: None,
            launched: Vec::new(),
            verbose,
        }
    }

    fn startup_plan(&mut self, services: &[Service]) {
        self.tree.start("Starting Moraine");
        let mut selected = vec!["ClickHouse", "database"];
        if services.contains(&Service::Ingest) {
            selected.push("ingest");
        }
        if services.contains(&Service::Backend) {
            selected.push("backend");
        }
        let mut detail = selected.join(", ");
        if !services.contains(&Service::Ingest) {
            detail.push_str("; ingest skipped by --no-ingest");
        }
        if !services.contains(&Service::Backend) {
            detail.push_str("; backend not selected");
        }
        self.tree.phase("Startup plan", Some(&detail));
    }

    fn phase(&mut self, label: &str, detail: &str) {
        self.tree.phase(label, Some(detail));
    }

    fn success_step(&mut self, label: &str, detail: Option<&str>) {
        self.tree
            .step("✓", "[ok]", label, detail, Style::new().green());
    }

    fn info_step(&mut self, label: &str, detail: Option<&str>) {
        self.tree.step("•", "*", label, detail, Style::new().cyan());
    }

    fn skipped_step(&mut self, label: &str, detail: Option<&str>) {
        self.tree
            .step("–", "[-]", label, detail, Style::new().yellow());
    }

    fn clickhouse_wait(&mut self, event: ClickHouseStartupProgress) {
        match event {
            ClickHouseStartupProgress::ProbingEndpoint { elapsed, timeout } => {
                self.tree.active(&format!(
                    "Probing configured ClickHouse endpoint · {:.1}s / {:.1}s",
                    elapsed.as_secs_f64(),
                    timeout.as_secs_f64()
                ));
            }
            ClickHouseStartupProgress::EndpointProbeFinished => self.tree.clear_active(),
            ClickHouseStartupProgress::WaitingForHealth { elapsed, timeout } => {
                self.tree.active(&format!(
                    "Waiting for ClickHouse health · {:.1}s / {:.1}s",
                    elapsed.as_secs_f64(),
                    timeout.as_secs_f64()
                ));
            }
        }
    }

    fn clickhouse_outcome(&mut self, outcome: &StartOutcome) {
        self.tree.clear_active();
        let pid = outcome.pid.map(|pid| format!("pid {pid}"));
        match outcome.state {
            StartState::Started => {
                self.launched.push(outcome.clone());
                self.success_step(
                    "ClickHouse ready",
                    Some(&format!(
                        "managed supervisor started{}",
                        pid.as_deref()
                            .map(|value| format!(" · {value}"))
                            .unwrap_or_default()
                    )),
                );
            }
            StartState::AlreadyRunning => {
                self.success_step(
                    "ClickHouse ready",
                    Some(&format!(
                        "managed supervisor already running{}",
                        pid.as_deref()
                            .map(|value| format!(" · {value}"))
                            .unwrap_or_default()
                    )),
                );
            }
            StartState::AlreadyServing => {
                self.success_step("ClickHouse ready", Some("healthy unmanaged endpoint"));
            }
        }
        if self.verbose {
            if let Some(path) = &outcome.log_path {
                self.info_step("ClickHouse log", Some(path));
            }
        }
    }

    fn database_start(&mut self) {
        self.phase("Database", "inspecting schema and migrations");
        self.database_activity = Some(DatabaseActivity::Inspecting {
            started: Instant::now(),
        });
        self.database_tick();
    }

    fn database_event(&mut self, event: DatabaseProgress) {
        match event {
            DatabaseProgress::Migration(MigrationProgress::Plan { applied, pending }) => {
                self.database_activity = None;
                if pending == 0 {
                    self.success_step(
                        "Database schema current",
                        Some(&format!("{applied} migrations applied")),
                    );
                } else {
                    self.info_step(
                        "Migration plan",
                        Some(&format!("{applied} applied · {pending} pending")),
                    );
                }
            }
            DatabaseProgress::Migration(MigrationProgress::Started {
                index, total, name, ..
            }) => {
                self.database_activity = Some(DatabaseActivity::Migration {
                    index,
                    total,
                    name,
                    started: Instant::now(),
                });
                self.database_tick();
            }
            DatabaseProgress::Migration(MigrationProgress::Applied {
                index, total, name, ..
            }) => {
                let elapsed = match self.database_activity.take() {
                    Some(DatabaseActivity::Migration { started, .. }) => started.elapsed(),
                    _ => Duration::ZERO,
                };
                self.success_step(
                    name,
                    Some(&format!(
                        "migration {index}/{total} applied · {:.1}s",
                        elapsed.as_secs_f64()
                    )),
                );
            }
            DatabaseProgress::ReconciliationInspecting => {
                self.database_activity = Some(DatabaseActivity::ReconciliationInspection {
                    started: Instant::now(),
                });
                self.database_tick();
            }
            DatabaseProgress::ReconciliationStarted { historical } => {
                self.tree.phase(
                    "MCP read model",
                    Some(if historical {
                        "building from existing sessions"
                    } else {
                        "reconciling changed sessions"
                    }),
                );
                self.database_activity = Some(DatabaseActivity::Reconciliation {
                    processed: 0,
                    started: Instant::now(),
                });
                self.database_tick();
            }
            DatabaseProgress::ReconciliationAdvanced { processed } => {
                if let Some(DatabaseActivity::Reconciliation {
                    processed: current, ..
                }) = &mut self.database_activity
                {
                    *current = processed;
                }
                self.database_tick();
            }
            DatabaseProgress::ReconciliationFinished { processed } => {
                let elapsed = match self.database_activity.take() {
                    Some(DatabaseActivity::Reconciliation { started, .. }) => started.elapsed(),
                    _ => Duration::ZERO,
                };
                self.success_step(
                    "MCP read model ready",
                    Some(&format!(
                        "{processed} sessions processed · {:.1}s",
                        elapsed.as_secs_f64()
                    )),
                );
            }
        }
    }

    fn database_tick(&mut self) {
        let label = match &self.database_activity {
            Some(DatabaseActivity::Inspecting { started }) => {
                format!(
                    "Reading migration ledger · {:.1}s",
                    started.elapsed().as_secs_f64()
                )
            }
            Some(DatabaseActivity::Migration {
                index,
                total,
                name,
                started,
            }) => format!(
                "[{index}/{total}] {name} · {:.1}s",
                started.elapsed().as_secs_f64()
            ),
            Some(DatabaseActivity::ReconciliationInspection { started }) => format!(
                "Inspecting MCP read model · {:.1}s",
                started.elapsed().as_secs_f64()
            ),
            Some(DatabaseActivity::Reconciliation { processed, started }) => format!(
                "Reconciling sessions · {processed} processed · {:.1}s",
                started.elapsed().as_secs_f64()
            ),
            None => return,
        };
        self.tree.active(&label);
    }

    fn service_outcome(&mut self, outcome: &StartOutcome) {
        let pid = outcome
            .pid
            .map(|pid| format!("pid {pid}"))
            .unwrap_or_else(|| "unmanaged".to_string());
        match outcome.state {
            StartState::Started => {
                self.launched.push(outcome.clone());
                self.success_step(&format!("{} launched", outcome.service.name()), Some(&pid));
            }
            StartState::AlreadyRunning => self.skipped_step(
                &format!("{} already running", outcome.service.name()),
                Some(&pid),
            ),
            StartState::AlreadyServing => self.skipped_step(
                &format!("{} already serving", outcome.service.name()),
                Some("unmanaged"),
            ),
        }
        if self.verbose {
            if let Some(path) = &outcome.log_path {
                self.info_step(&format!("{} log", outcome.service.name()), Some(path));
            }
        }
    }

    fn status_collected(&mut self, status: &StatusSnapshot, elapsed: Duration) {
        self.tree.clear_active();
        let healthy = doctor_is_healthy(&status.doctor);
        let detail = format!(
            "database {} · {:.1}s",
            if healthy {
                "healthy"
            } else {
                "needs attention"
            },
            elapsed.as_secs_f64()
        );
        if healthy {
            self.success_step("Status collected", Some(&detail));
        } else {
            self.tree.step(
                "!",
                "[warn]",
                "Status collected",
                Some(&detail),
                Style::new().yellow(),
            );
        }
        if let Some(url) = &status.monitor_url {
            self.info_step("Monitor", Some(url));
        }
    }

    fn finish_success(&mut self) {
        self.tree.finish(
            true,
            &format!(
                "Moraine startup actions complete · {:.1}s · startup summary follows",
                self.started_at.elapsed().as_secs_f64()
            ),
        );
    }

    fn finish_error(&mut self, paths: &RuntimePaths) {
        self.tree.clear_active();
        if !self.launched.is_empty() {
            let launched = self
                .launched
                .iter()
                .map(|outcome| outcome.service.name())
                .collect::<Vec<_>>()
                .join(", ");
            self.tree.step(
                "!",
                "[warn]",
                "Services were launched",
                Some(&format!(
                    "{launched} may still be running · run `moraine down` to stop them"
                )),
                Style::new().yellow(),
            );
            let mut remaining_log_lines = FAILURE_LOG_LINE_LIMIT;
            for outcome in &self.launched {
                if let Some(path) = &outcome.log_path {
                    self.tree.step(
                        "i",
                        "[info]",
                        &format!("{} logs", outcome.service.name()),
                        Some(&format!(
                            "{path} · `moraine logs {} --lines 200`",
                            outcome.service.name()
                        )),
                        Style::new().cyan(),
                    );
                }
                if !self.verbose || remaining_log_lines == 0 {
                    continue;
                }
                let Ok(snapshot) =
                    super::logs::collect_logs(paths, Some(outcome.service), remaining_log_lines)
                else {
                    continue;
                };
                for section in snapshot
                    .sections
                    .into_iter()
                    .rev()
                    .filter(|section| section.exists && !section.lines.is_empty())
                {
                    self.tree.step(
                        "i",
                        "[info]",
                        &format!("{} log source", outcome.service.name()),
                        Some(&section.path),
                        Style::new().cyan(),
                    );
                    for line in section.lines.into_iter().take(remaining_log_lines) {
                        let line = sanitize_log_line(&line);
                        self.tree.step(
                            "|",
                            "|",
                            outcome.service.name(),
                            Some(&line),
                            Style::new().bright().black(),
                        );
                        remaining_log_lines -= 1;
                    }
                    if remaining_log_lines == 0 {
                        break;
                    }
                }
            }
        }
        self.tree.finish(false, "Moraine startup failed");
    }

    #[cfg(test)]
    fn into_inner(self) -> W {
        self.tree.into_inner()
    }
}

pub(super) async fn handle_args(
    output: &CliOutput,
    config_path: &std::path::Path,
    cfg: &AppConfig,
    args: &UpArgs,
) -> Result<ExitCode> {
    let paths = runtime_paths(cfg);
    let services_to_start = selected_up_services(args, cfg);
    let mut progress = StartupProgress::from_output(output);
    progress.startup_plan(&services_to_start);

    match start_selected_services(config_path, cfg, &paths, services_to_start, &mut progress).await
    {
        Ok(snapshot) => {
            progress.finish_success();
            render_up(output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            progress.finish_error(&paths);
            Err(error)
        }
    }
}

async fn start_selected_services<W: Write>(
    config_path: &std::path::Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    services_to_start: Vec<Service>,
    progress: &mut StartupProgress<W>,
) -> Result<UpSnapshot> {
    progress.phase(
        "Preflight",
        "checking service binaries and runtime directories",
    );
    preflight_required_service_binaries(&services_to_start, paths)?;
    ensure_runtime_dirs(paths)?;
    progress.success_step("Preflight complete", None);

    progress.phase("ClickHouse", "inspecting endpoint and managed runtime");
    let clickhouse = start_clickhouse_with_progress(config_path, cfg, paths, |event| {
        progress.clickhouse_wait(event);
    })
    .await?;
    progress.clickhouse_outcome(&clickhouse);

    progress.database_start();
    let migrations = drive_database_progress(cfg, progress).await?;

    progress.phase("Services", "starting selected Moraine processes");
    let mut started_services = Vec::with_capacity(services_to_start.len());
    for service in services_to_start {
        let outcome = start_background_service(service, config_path, cfg, paths, &[])?;
        progress.service_outcome(&outcome);
        started_services.push(outcome);
    }

    progress.phase(
        "Verification",
        "collecting final runtime and database status",
    );
    let repository = conversation_repository(cfg)?;
    let status_started = Instant::now();
    let status = drive_status_progress(
        cmd_status(paths, cfg, &repository),
        status_started,
        progress,
    )
    .await?;
    progress.status_collected(&status, status_started.elapsed());

    Ok(UpSnapshot {
        clickhouse,
        migrations,
        services: started_services,
        status,
    })
}

async fn drive_database_progress<W: Write>(
    cfg: &AppConfig,
    progress: &mut StartupProgress<W>,
) -> Result<MigrationOutcome> {
    let (sender, receiver) = mpsc::channel();
    let migration = migrate_database_for_up(cfg, move |event| {
        let _ = sender.send(event);
    });
    tokio::pin!(migration);
    let mut ticker = interval_at(TokioInstant::now() + PROGRESS_REFRESH, PROGRESS_REFRESH);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            result = &mut migration => {
                drain_database_events(&receiver, progress);
                return result;
            }
            _ = ticker.tick() => {
                drain_database_events(&receiver, progress);
                progress.database_tick();
            }
        }
    }
}

fn drain_database_events<W: Write>(
    receiver: &Receiver<DatabaseProgress>,
    progress: &mut StartupProgress<W>,
) {
    while let Ok(event) = receiver.try_recv() {
        progress.database_event(event);
    }
}

async fn drive_status_progress<F, W>(
    status: F,
    started: Instant,
    progress: &mut StartupProgress<W>,
) -> Result<StatusSnapshot>
where
    F: Future<Output = Result<StatusSnapshot>>,
    W: Write,
{
    tokio::pin!(status);
    let mut ticker = interval_at(TokioInstant::now() + PROGRESS_REFRESH, PROGRESS_REFRESH);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut status => return result,
            _ = ticker.tick() => {
                progress.tree.active(&format!(
                    "Collecting status · {:.1}s",
                    started.elapsed().as_secs_f64()
                ));
            }
        }
    }
}

fn selected_up_services(args: &UpArgs, cfg: &AppConfig) -> Vec<Service> {
    let mut services = Vec::new();
    if !args.no_ingest {
        services.push(Service::Ingest);
    }
    if args.backend || args.monitor || args.mcp || cfg.backend.start_on_up {
        services.push(Service::Backend);
    }
    services
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::OutputMode;

    fn test_output(mode: OutputMode, unicode: bool) -> CliOutput {
        CliOutput {
            mode,
            verbose: false,
            unicode,
            width: 100,
        }
    }

    #[test]
    fn startup_progress_is_stderr_tty_only_and_json_safe() {
        let rich = ProgressStyle::from_capabilities(&test_output(OutputMode::Rich, true), true);
        let mut progress = StartupProgress::new(ProgressTree::new(rich, Vec::new()), false);
        progress.startup_plan(&[Service::Ingest]);
        progress.phase("Database", "checking");
        progress.tree.active("Working");
        progress.success_step("Done", None);
        progress.finish_success();
        let rendered = String::from_utf8(progress.into_inner()).expect("progress utf8");
        assert!(rendered.contains("Starting Moraine"));
        assert!(rendered.contains("backend not selected"));
        assert!(rendered.contains("\u{1b}[2K"));

        for mode in [OutputMode::Rich, OutputMode::Json] {
            let style = ProgressStyle::from_capabilities(
                &test_output(mode, true),
                mode == OutputMode::Json,
            );
            let mut progress = StartupProgress::new(ProgressTree::new(style, Vec::new()), false);
            progress.startup_plan(&[]);
            progress.finish_success();
            assert!(progress.into_inner().is_empty());
        }
    }

    #[test]
    fn plain_progress_has_no_cursor_control_bytes() {
        let style = ProgressStyle::from_capabilities(&test_output(OutputMode::Plain, false), true);
        let mut progress = StartupProgress::new(ProgressTree::new(style, Vec::new()), false);
        progress.startup_plan(&[]);
        progress.tree.active("Waiting");
        progress.tree.active("Waiting longer");
        progress.finish_success();
        let rendered = String::from_utf8(progress.into_inner()).expect("progress utf8");
        assert!(!rendered.contains('\r'));
        assert!(!rendered.contains("\u{1b}["));
        assert_eq!(rendered.matches("Waiting").count(), 1);
    }

    #[test]
    fn migration_is_applied_only_after_applied_event() {
        let style = ProgressStyle::from_capabilities(&test_output(OutputMode::Plain, true), true);
        let mut progress = StartupProgress::new(ProgressTree::new(style, Vec::new()), false);
        progress.startup_plan(&[]);
        progress.database_event(DatabaseProgress::Migration(MigrationProgress::Plan {
            applied: 29,
            pending: 1,
        }));
        progress.database_event(DatabaseProgress::Migration(MigrationProgress::Started {
            index: 1,
            total: 1,
            version: "030",
            name: "030_refresh_omp_session_metadata.sql",
        }));
        let before = String::from_utf8_lossy(progress.tree.bytes());
        assert!(!before.contains("migration 1/1 applied"));
        progress.database_event(DatabaseProgress::Migration(MigrationProgress::Applied {
            index: 1,
            total: 1,
            version: "030",
            name: "030_refresh_omp_session_metadata.sql",
        }));
        let rendered = String::from_utf8(progress.into_inner()).expect("progress utf8");
        assert!(rendered.contains("migration 1/1 applied"));
    }

    #[test]
    fn failed_startup_reports_launched_services_and_bounded_safe_logs() {
        let root = std::env::temp_dir().join(format!("moraine-up-progress-{}", std::process::id()));
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.display().to_string();
        cfg.runtime.logs_dir = root.join("logs").display().to_string();
        cfg.runtime.pids_dir = root.join("pids").display().to_string();
        let paths = runtime_paths(&cfg);
        ensure_runtime_dirs(&paths).expect("create temporary runtime directories");
        let ingest_log = crate::process::log_path(&paths, Service::Ingest);
        let backend_log = crate::process::log_path(&paths, Service::Backend);
        std::fs::write(
            &ingest_log,
            "\u{1b}[2Jdiagnostic-01\ndiagnostic-02\ndiagnostic-03\ndiagnostic-04\ndiagnostic-05\ndiagnostic-06\ndiagnostic-07\ndiagnostic-08\n",
        )
        .expect("write temporary ingest log");
        std::fs::write(
            &backend_log,
            "diagnostic-09\ndiagnostic-10\ndiagnostic-11\ndiagnostic-12\ndiagnostic-13\ndiagnostic-14\ndiagnostic-15\ndiagnostic-16\n",
        )
        .expect("write temporary backend log");
        let style = ProgressStyle::from_capabilities(&test_output(OutputMode::Plain, true), true);
        let mut progress = StartupProgress::new(ProgressTree::new(style, Vec::new()), true);
        progress.startup_plan(&[Service::Ingest, Service::Backend]);
        for (service, pid, log_path) in [
            (Service::Ingest, 42, &ingest_log),
            (Service::Backend, 43, &backend_log),
        ] {
            progress.service_outcome(&StartOutcome {
                service,
                state: StartState::Started,
                pid: Some(pid),
                log_path: Some(log_path.display().to_string()),
            });
        }

        progress.finish_error(&paths);
        let rendered = String::from_utf8(progress.into_inner()).expect("progress utf8");
        std::fs::remove_dir_all(&root).expect("remove temporary runtime directories");

        assert!(rendered.contains("Services were launched"));
        assert!(rendered.contains("may still be running"));
        assert!(rendered.contains("run `moraine down`"));
        assert!(rendered.contains("`moraine logs ingest --lines 200`"));
        assert_eq!(
            rendered.matches("diagnostic-").count(),
            FAILURE_LOG_LINE_LIMIT
        );
        assert!(rendered.contains(r"\u{1b}[2Jdiagnostic-01"));
        assert!(!rendered.contains('\u{1b}'));
        assert!(rendered.contains("Moraine startup failed"));
    }

    #[test]
    fn selected_up_services_uses_only_normalized_backend_switch_and_deduplicates_aliases() {
        let mut cfg = AppConfig::default();
        cfg.backend.start_on_up = false;
        cfg.runtime.start_monitor_on_up = true;
        cfg.runtime.start_mcp_on_up = true;
        cfg.mcp.start_central_on_up = true;

        assert_eq!(
            selected_up_services(
                &UpArgs {
                    no_ingest: false,
                    backend: false,
                    monitor: false,
                    mcp: false,
                },
                &cfg
            ),
            vec![Service::Ingest]
        );

        for (backend, monitor, mcp) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (true, true, true),
        ] {
            assert_eq!(
                selected_up_services(
                    &UpArgs {
                        no_ingest: true,
                        backend,
                        monitor,
                        mcp,
                    },
                    &cfg
                ),
                vec![Service::Backend],
                "backend={backend} monitor={monitor} mcp={mcp}"
            );
        }

        cfg.backend.start_on_up = true;
        assert_eq!(
            selected_up_services(
                &UpArgs {
                    no_ingest: false,
                    backend: false,
                    monitor: false,
                    mcp: false,
                },
                &cfg
            ),
            vec![Service::Ingest, Service::Backend]
        );
    }
}
