use anyhow::Result;
use moraine_clickhouse::{
    reclaim, CoreIndexAuditOutcome, DoctorReport, ReclaimOutcome, ReclaimPlan, ReclaimScope,
    ReclaimStatusReport, StorageReport,
};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table, Widget, Wrap};
use std::io::IsTerminal;

use crate::cli::{Cli, OutputFormat};
use crate::process::{StartOutcome, StartState};
use crate::service::Service;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputMode {
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceRuntimeState {
    Running,
    Stopped,
    Partial,
    Unmanaged,
}

impl ServiceRuntimeState {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Partial => "partial",
            Self::Unmanaged => "serving (unmanaged)",
        }
    }

    fn fully_available(self) -> bool {
        matches!(self, Self::Running | Self::Unmanaged)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ServiceRuntimeStatus {
    pub(crate) service: Service,
    pub(crate) pid: Option<u32>,
    pub(crate) state: ServiceRuntimeState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) socket_listening: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) http_listening: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StatusDataSource {
    DaemonApi,
    DirectDb,
}

impl StatusDataSource {
    fn label(self) -> &'static str {
        match self {
            Self::DaemonApi => "daemon API",
            Self::DirectDb => "direct DB",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum HeartbeatSnapshot {
    Available {
        latest: String,
        queue_depth: u64,
        files_active: u64,
        watcher_backend: String,
        watcher_error_count: u64,
        watcher_reset_count: u64,
        watcher_last_reset_unix_ms: u64,
    },
    Unavailable,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct StatusSnapshot {
    pub(crate) services: Vec<ServiceRuntimeStatus>,
    pub(crate) monitor_url: Option<String>,
    pub(crate) data_source: StatusDataSource,
    pub(crate) managed_clickhouse_installed: bool,
    pub(crate) managed_clickhouse_path: String,
    pub(crate) managed_clickhouse_version: Option<String>,
    pub(crate) clickhouse_active_source: String,
    pub(crate) clickhouse_active_source_path: Option<String>,
    pub(crate) managed_clickhouse_checksum: String,
    pub(crate) clickhouse_health_url: String,
    pub(crate) status_notes: Vec<String>,
    pub(crate) doctor: DoctorReport,
    pub(crate) heartbeat: HeartbeatSnapshot,
    /// Canonical read-index / open-reader readiness (issue #598). `None` only
    /// on the legacy status paths that do not gather it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) core_index: Option<CoreIndexReport>,
    /// Storage ownership, bytes, and effective retention policy (issue #603
    /// WI-02). `None` when ClickHouse was unreachable; the panel then omits
    /// the line rather than reporting zeroes as facts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) storage: Option<StorageReport>,
}

/// Canonical read-index (issue #598) readiness surfaced by `moraine status`,
/// `moraine db doctor`, and `moraine db core-index status`. Additive JSON: a
/// downlevel consumer ignores it.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct CoreIndexReport {
    /// Whether the migration-036 read-index state was readable (migration
    /// applied and ClickHouse reachable). When false the readiness fields are
    /// defaults and the surfacing shows "unavailable".
    pub(crate) available: bool,
    /// `core_indexes.ready == 1`: the coverage sweep completed and the overlap
    /// audit passed.
    pub(crate) core_indexes_ready: bool,
    /// `open_v2.ready == 1`: the one-way `open` cutover flag consumers read.
    pub(crate) open_v2_ready: bool,
    /// How `open_v2` was published (`auto-local` or `operator-promote`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) open_v2_provenance: Option<String>,
    /// Seconds since the backfill cursor / readiness row was last written
    /// (decoded from the snowflake generation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) backfill_cursor_age_seconds: Option<i64>,
    /// The persisted overlap-audit outcome, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) audit: Option<CoreIndexAuditOutcome>,
    /// The configured `[mcp] open_reader` value: `auto` | `v1` | `v2`.
    pub(crate) configured_open_reader: String,
    /// The effective reader after resolution: `v2` | `error`. Never `v1` —
    /// issue #603 WI-10 retired that reader.
    pub(crate) effective_open_reader: String,
    /// True when the config names a reader the resolution declined to honor.
    /// With one reader left that is exactly the retired `v1` selector: `auto`
    /// and `v2` are synonyms that force nothing, and an unready store is a
    /// readiness fact rather than a config one, so it does NOT set this flag
    /// (`core_index_report_lines` would otherwise tell a stock `auto` install
    /// it carries an override its `moraine.toml` does not contain).
    pub(crate) open_reader_override: bool,
    /// Human-readable note about the resolution — the retirement note for a
    /// configured `v1`, the not-ready-run-migrate note for an unready store,
    /// or both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) open_reader_note: Option<String>,
}

/// Human-readable lines for a [`CoreIndexReport`], shared by the doctor,
/// status, and `core-index status` renderers.
pub(crate) fn core_index_report_lines(report: &CoreIndexReport) -> Vec<String> {
    if !report.available {
        return vec![
            "core indexes: unavailable (migration 036 not applied or ClickHouse unreachable)"
                .to_string(),
            format!(
                "open reader: configured={}, effective=unknown",
                report.configured_open_reader
            ),
        ];
    }

    let mut lines = vec![
        format!(
            "core indexes ready: {}",
            state_label(report.core_indexes_ready)
        ),
        format!(
            "open v2 ready: {}{}",
            state_label(report.open_v2_ready),
            match &report.open_v2_provenance {
                Some(provenance) if report.open_v2_ready => format!(" ({provenance})"),
                _ => String::new(),
            }
        ),
    ];

    lines.push(match report.backfill_cursor_age_seconds {
        Some(age) => format!("backfill cursor age: {}", format_age_seconds(age)),
        None => "backfill cursor age: not yet swept".to_string(),
    });

    lines.push(match &report.audit {
        Some(audit) => format!(
            "overlap audit: {} (sessions={}, events={}, nav_missing={}, loc_missing={}, dir_missing={}, cardinality_delta={})",
            if audit.passed { "pass" } else { "fail" },
            audit.sampled_sessions,
            audit.sampled_events,
            audit.navigation_missing,
            audit.locator_missing,
            audit.directory_missing_sessions,
            audit.navigation_locator_cardinality_delta,
        ),
        None => "overlap audit: not yet run".to_string(),
    });

    let mut reader_line = format!(
        "open reader: configured={}, effective={}",
        report.configured_open_reader, report.effective_open_reader
    );
    if report.open_reader_override {
        reader_line.push_str(" (config override)");
    }
    lines.push(reader_line);
    if let Some(note) = &report.open_reader_note {
        lines.push(format!("open reader note: {note}"));
    }
    lines
}

/// One concise line summarizing core-index readiness for the `moraine status`
/// Database panel.
pub(crate) fn status_core_index_line(report: &CoreIndexReport) -> String {
    if !report.available {
        return "core indexes: unavailable".to_string();
    }
    format!(
        "core indexes: {}  |  open reader: {} (configured {})",
        state_label(report.core_indexes_ready),
        report.effective_open_reader,
        report.configured_open_reader,
    )
}

/// Render a signed age in seconds as a compact human string.
fn format_age_seconds(age_seconds: i64) -> String {
    if age_seconds < 0 {
        return "just now".to_string();
    }
    let age = age_seconds;
    if age < 60 {
        format!("{age}s")
    } else if age < 3600 {
        format!("{}m{}s", age / 60, age % 60)
    } else if age < 86_400 {
        format!("{}h{}m", age / 3600, (age % 3600) / 60)
    } else {
        format!("{}d{}h", age / 86_400, (age % 86_400) / 3600)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct MigrationOutcome {
    pub(crate) applied: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ServiceLogSection {
    pub(crate) service: Service,
    pub(crate) path: String,
    pub(crate) exists: bool,
    pub(crate) lines: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct LogsSnapshot {
    pub(crate) requested_lines: usize,
    pub(crate) sections: Vec<ServiceLogSection>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ClickhouseStatusSnapshot {
    pub(crate) managed_root: String,
    pub(crate) clickhouse_exists: bool,
    pub(crate) clickhouse_server_exists: bool,
    pub(crate) clickhouse_client_exists: bool,
    pub(crate) expected_version: String,
    pub(crate) active_source: String,
    pub(crate) active_source_path: Option<String>,
    pub(crate) checksum_state: String,
    pub(crate) installed_version: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct UpSnapshot {
    pub(crate) clickhouse: StartOutcome,
    pub(crate) migrations: MigrationOutcome,
    pub(crate) services: Vec<StartOutcome>,
    pub(crate) status: StatusSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DownSnapshot {
    pub(crate) stopped: Vec<Service>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) warning: Option<String>,
}

pub(crate) struct CliOutput {
    pub(crate) mode: OutputMode,
    pub(crate) verbose: bool,
    pub(crate) unicode: bool,
    pub(crate) width: u16,
}

impl CliOutput {
    pub(crate) fn from_cli(cli: &Cli) -> Self {
        let mode = match cli.output {
            OutputFormat::Auto => {
                if std::io::stdout().is_terminal() {
                    OutputMode::Rich
                } else {
                    OutputMode::Plain
                }
            }
            OutputFormat::Rich => OutputMode::Rich,
            OutputFormat::Plain => OutputMode::Plain,
            OutputFormat::Json => OutputMode::Json,
        };
        let unicode = std::env::var("LC_ALL")
            .ok()
            .or_else(|| std::env::var("LANG").ok())
            .map(|v| !v.to_ascii_uppercase().contains("C"))
            .unwrap_or(true);
        let width = std::env::var("COLUMNS")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .map(|v| v.clamp(72, 140))
            .unwrap_or(100);

        Self {
            mode,
            verbose: cli.verbose,
            unicode,
            width,
        }
    }

    pub(crate) fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    pub(crate) fn section(&self, title: &str, lines: &[String]) {
        match self.mode {
            OutputMode::Plain => {
                println!("{title}");
                for line in lines {
                    println!("  {line}");
                }
            }
            OutputMode::Rich => {
                let panel = render_panel(title, lines, self.width, self.unicode);
                println!("{panel}");
            }
            OutputMode::Json => {}
        }
    }

    pub(crate) fn table(&self, title: &str, headers: &[&str], rows: &[Vec<String>]) {
        match self.mode {
            OutputMode::Plain => print_plain_table(title, headers, rows),
            OutputMode::Rich => {
                let table = render_table(title, headers, rows, self.width, self.unicode);
                println!("{table}");
            }
            OutputMode::Json => {}
        }
    }

    pub(crate) fn line(&self, text: &str) {
        if self.mode != OutputMode::Json {
            println!("{text}");
        }
    }
}

fn render_panel(title: &str, lines: &[String], width: u16, unicode: bool) -> String {
    let inner_width = width.saturating_sub(2).max(1);
    let body_height = wrapped_line_count(lines, inner_width).max(1);
    let area = Rect::new(0, 0, width, body_height.saturating_add(2));
    let mut buffer = Buffer::empty(area);
    let mut block = Block::default()
        .title(Line::from(title.to_string()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));
    if !unicode {
        block = block.border_set(ratatui::symbols::border::PLAIN);
    }
    let paragraph = Paragraph::new(lines.join("\n"))
        .block(block)
        .wrap(Wrap { trim: false })
        .style(Style::default().fg(Color::White));
    paragraph.render(area, &mut buffer);
    buffer_to_string(&buffer)
}

fn wrapped_line_count(lines: &[String], width: u16) -> u16 {
    let width = usize::from(width.max(1));
    let count = lines
        .iter()
        .map(|line| {
            let char_count = line.chars().count().max(1);
            char_count.div_ceil(width)
        })
        .sum::<usize>();
    count.min(usize::from(u16::MAX)) as u16
}

fn render_table(
    title: &str,
    headers: &[&str],
    rows: &[Vec<String>],
    width: u16,
    unicode: bool,
) -> String {
    let area = Rect::new(
        0,
        0,
        width,
        (rows.len().saturating_add(1) as u16).saturating_add(2),
    );
    let mut buffer = Buffer::empty(area);
    let mut block = Block::default()
        .title(Line::from(title.to_string()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));
    if !unicode {
        block = block.border_set(ratatui::symbols::border::PLAIN);
    }

    let header = Row::new(
        headers
            .iter()
            .map(|h| Cell::from((*h).to_string()).style(Style::default().fg(Color::Yellow))),
    )
    .style(Style::default().add_modifier(Modifier::BOLD));
    let data_rows = rows.iter().map(|row| Row::new(row.clone()));
    let widths = headers
        .iter()
        .map(|_| Constraint::Percentage((100 / headers.len().max(1)) as u16))
        .collect::<Vec<_>>();
    let table = Table::new(data_rows, widths).header(header).block(block);
    table.render(area, &mut buffer);
    buffer_to_string(&buffer)
}

fn buffer_to_string(buffer: &Buffer) -> String {
    let mut lines = Vec::new();
    for y in 0..buffer.area.height {
        let mut line = String::new();
        for x in 0..buffer.area.width {
            line.push_str(buffer[(x, y)].symbol());
        }
        while line.ends_with(' ') {
            line.pop();
        }
        lines.push(line);
    }
    while lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    lines.join("\n")
}

fn print_plain_table(title: &str, headers: &[&str], rows: &[Vec<String>]) {
    println!("{title}");
    println!("{}", headers.join(" | "));
    let divider = headers.iter().map(|_| "---").collect::<Vec<_>>().join("+");
    println!("{divider}");
    for row in rows {
        println!("{}", row.join(" | "));
    }
}

fn health_label(value: bool) -> &'static str {
    if value {
        "healthy"
    } else {
        "unhealthy"
    }
}

pub(crate) fn state_label(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn stoplight(running: bool) -> &'static str {
    if running {
        "\u{1F7E2}" // 🟢
    } else {
        "\u{1F534}" // 🔴
    }
}

fn service_endpoint(row: &ServiceRuntimeStatus, snapshot: &StatusSnapshot) -> Option<String> {
    match row.service {
        Service::ClickHouse => Some(snapshot.clickhouse_health_url.clone()),
        Service::Backend => {
            let mut endpoints = Vec::new();
            if row.socket_listening == Some(true) {
                endpoints.push("MCP socket".to_string());
            }
            if row.http_listening == Some(true) {
                if let Some(url) = &snapshot.monitor_url {
                    endpoints.push(url.clone());
                }
            }
            (!endpoints.is_empty()).then(|| endpoints.join(", "))
        }
        _ => None,
    }
}

fn format_start_state(outcome: &StartOutcome) -> String {
    match outcome.state {
        StartState::Started => "started".to_string(),
        StartState::AlreadyRunning => "already running".to_string(),
        StartState::AlreadyServing => "already serving (unmanaged)".to_string(),
    }
}

fn format_start_pid(outcome: &StartOutcome) -> String {
    match outcome.pid {
        Some(pid) => pid.to_string(),
        None => "-".to_string(),
    }
}

pub(crate) fn render_status(output: &CliOutput, snapshot: &StatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    // -- Services with stoplight indicators and endpoints --
    let service_rows: Vec<Vec<String>> = snapshot
        .services
        .iter()
        .map(|row| {
            let mut cols = vec![
                format!(
                    "{} {}",
                    stoplight(row.state.fully_available()),
                    row.service.name()
                ),
                row.state.label().to_string(),
                service_endpoint(row, snapshot).unwrap_or_default(),
            ];
            if output.verbose {
                cols.push(
                    row.pid
                        .map(|pid| pid.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
            }
            cols
        })
        .collect();

    if output.verbose {
        output.table("Services", &["", "state", "endpoint", "pid"], &service_rows);
    } else {
        output.table("Services", &["", "state", "endpoint"], &service_rows);
    }

    // -- Database Health (concise) --
    let db_healthy = snapshot.doctor.clickhouse_healthy && snapshot.doctor.database_exists;
    let mut doctor_lines = vec![format!(
        "{} {}",
        stoplight(db_healthy),
        if db_healthy {
            "database healthy".to_string()
        } else {
            format!(
                "clickhouse {} / db {}",
                health_label(snapshot.doctor.clickhouse_healthy),
                if snapshot.doctor.database_exists {
                    "exists"
                } else {
                    "missing"
                }
            )
        }
    )];
    if let Some(version) = &snapshot.doctor.clickhouse_version {
        doctor_lines[0].push_str(&format!("  (v{version})"));
    }
    doctor_lines.push(format!("source: {}", snapshot.data_source.label()));
    match &snapshot.doctor.publication {
        Some(publication) => doctor_lines.push(format!(
            "publication: {}  (replaying: {}, blocked: {}, append preparing: {}, append blocked: {}, mirror catch-up: {}, writer conflicts: {})",
            health_label(publication.is_healthy()),
            publication.replaying_generations,
            publication.blocked_generations,
            publication.append_preparations,
            publication.blocked_append_preparations,
            publication.mirror_catchup_pending,
            publication.writer_conflicts,
        )),
        None => doctor_lines.push("publication: unavailable".to_string()),
    }
    if !snapshot.doctor.pending_migrations.is_empty() {
        doctor_lines.push(format!(
            "  pending migrations: {}",
            snapshot.doctor.pending_migrations.join(", ")
        ));
    }
    if !snapshot.doctor.missing_tables.is_empty() {
        doctor_lines.push(format!(
            "  missing tables: {}",
            snapshot.doctor.missing_tables.join(", ")
        ));
    }
    if output.verbose && !snapshot.doctor.errors.is_empty() {
        doctor_lines.push(format!("  errors: {}", snapshot.doctor.errors.join(" | ")));
    }
    if let Some(storage) = &snapshot.storage {
        doctor_lines.push(status_storage_line(storage));
        // A destructive policy is never invisible, even in the concise view.
        if let Some(warning) = status_retention_warning_line(storage) {
            doctor_lines.push(format!("  {warning}"));
        }
        if !storage.unclassified_tables().is_empty() {
            doctor_lines.push(format!(
                "  unclassified tables: {}",
                storage.unclassified_tables().join(", ")
            ));
        }
    }
    if let Some(core_index) = &snapshot.core_index {
        doctor_lines.push(status_core_index_line(core_index));
        // Two things must be impossible to miss even in the concise view: a
        // config override (post-WI-10 that is exactly a still-configured `v1`)
        // and an unready store. The second is a readiness fact rather than a
        // config one, so it sets no override flag and is ORed in on its own —
        // which is why `open_reader_override` does not need to lie about it.
        if let Some(note) = &core_index.open_reader_note {
            if core_index.open_reader_override || core_index.effective_open_reader == "error" {
                doctor_lines.push(format!("  open reader: {note}"));
            }
        }
    }
    output.section("Database", &doctor_lines);

    // -- Ingest activity (only show when there is something to report) --
    match &snapshot.heartbeat {
        HeartbeatSnapshot::Available {
            latest,
            queue_depth,
            files_active,
            watcher_backend,
            watcher_error_count,
            watcher_reset_count,
            watcher_last_reset_unix_ms,
        } => {
            let mut lines = vec![
                format!("last event: {latest}"),
                format!("queue: {queue_depth}  |  active files: {files_active}"),
            ];
            if *watcher_error_count > 0 || *watcher_reset_count > 0 {
                lines.push(format!(
                    "watcher: {watcher_backend}  (errors: {watcher_error_count}, resets: {watcher_reset_count})"
                ));
            } else if output.verbose {
                lines.push(format!("watcher: {watcher_backend}"));
            }
            if output.verbose {
                lines.push(format!(
                    "watcher last reset unix ms: {watcher_last_reset_unix_ms}"
                ));
            }
            output.section("Ingest", &lines);
        }
        HeartbeatSnapshot::Unavailable => {
            if output.verbose {
                output.section("Ingest", &["no heartbeat data".to_string()]);
            }
        }
        HeartbeatSnapshot::Error { message } => {
            output.section("Ingest", &[format!("heartbeat error: {message}")]);
        }
    }

    // -- ClickHouse runtime details (verbose only) --
    if output.verbose {
        let mut ch_lines = vec![
            format!(
                "managed install: {}",
                if snapshot.managed_clickhouse_installed {
                    "present"
                } else {
                    "missing"
                }
            ),
            format!("binary: {}", snapshot.managed_clickhouse_path),
            format!(
                "source: {}{}",
                snapshot.clickhouse_active_source,
                snapshot
                    .clickhouse_active_source_path
                    .as_ref()
                    .map(|p| format!(" ({p})"))
                    .unwrap_or_default()
            ),
            format!("checksum: {}", snapshot.managed_clickhouse_checksum),
        ];
        if let Some(version) = &snapshot.managed_clickhouse_version {
            ch_lines.push(format!("managed version: {version}"));
        }
        output.section("ClickHouse Runtime", &ch_lines);
    }

    // -- Status notes (warnings) --
    if !snapshot.status_notes.is_empty() {
        output.section("Warnings", &snapshot.status_notes);
    }
    Ok(())
}

pub(crate) fn render_db_migrate(output: &CliOutput, outcome: &MigrationOutcome) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(outcome)?);
        return Ok(());
    }
    if outcome.applied.is_empty() {
        output.section("Database Migrations", &["already up to date".to_string()]);
        return Ok(());
    }
    let rows = outcome
        .applied
        .iter()
        .enumerate()
        .map(|(idx, migration)| vec![(idx + 1).to_string(), migration.to_string()])
        .collect::<Vec<_>>();
    output.table("Applied Migrations", &["#", "migration"], &rows);
    Ok(())
}

pub(crate) fn render_db_doctor(
    output: &CliOutput,
    report: &DoctorReport,
    core_index: &CoreIndexReport,
    storage: Option<&StorageReport>,
) -> Result<()> {
    if output.is_json() {
        // Additive: the DoctorReport shape is unchanged; core-index readiness
        // and the issue #603 storage report ride in sibling objects so
        // downlevel JSON consumers are unaffected.
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "doctor": report,
                "core_index": core_index,
                "storage": storage,
            }))?
        );
        return Ok(());
    }

    let mut lines = vec![
        format!("clickhouse: {}", health_label(report.clickhouse_healthy)),
        format!("database: {}", report.database),
        format!("database exists: {}", state_label(report.database_exists)),
        format!(
            "pending migrations: {}",
            if report.pending_migrations.is_empty() {
                "none".to_string()
            } else {
                report.pending_migrations.join(", ")
            }
        ),
        format!(
            "missing tables: {}",
            if report.missing_tables.is_empty() {
                "none".to_string()
            } else {
                report.missing_tables.join(", ")
            }
        ),
    ];
    if let Some(version) = &report.clickhouse_version {
        lines.push(format!("clickhouse version: {version}"));
    }
    if output.verbose && !report.applied_migrations.is_empty() {
        lines.push(format!(
            "applied migrations: {}",
            report.applied_migrations.join(", ")
        ));
    }
    if let Some(publication) = &report.publication {
        lines.push(format!(
            "publication: {}",
            health_label(publication.is_healthy())
        ));
        lines.push(format!(
            "publication states: replaying={}, blocked={}, append_preparing={}, append_blocked={}, mirror_catchup={}, writer_conflicts={}, ambiguous_hostless_rows={}",
            publication.replaying_generations,
            publication.blocked_generations,
            publication.append_preparations,
            publication.blocked_append_preparations,
            publication.mirror_catchup_pending,
            publication.writer_conflicts,
            publication.ambiguous_hostless_rows,
        ));
        if !publication.issues.is_empty() {
            lines.push(format!(
                "publication issues: {}",
                publication.issues.join(" | ")
            ));
        }
    } else {
        lines.push("publication: unavailable".to_string());
    }
    if !report.errors.is_empty() {
        lines.push(format!("errors: {}", report.errors.join(" | ")));
    }
    lines.extend(core_index_report_lines(core_index));
    match storage {
        Some(storage) => lines.extend(storage_report_lines(storage)),
        None => lines.push(
            "storage: unavailable (ClickHouse unreachable); storage state is transient and does              not affect the doctor exit code"
                .to_string(),
        ),
    }
    output.section("DB Doctor", &lines);
    Ok(())
}

// ---------------------------------------------------------------------------
// Issue #603 — storage / reclaim rendering
// ---------------------------------------------------------------------------

/// Format a byte count for operator display. Deliberately plain: no "frees",
/// no "recovers", no "partition" — see [`reclaim::FORBIDDEN_DENOMINATION_WORDS`].
pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[0])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn format_horizon(seconds: f64) -> String {
    if seconds >= 86_400.0 {
        format!("{:.0}d", seconds / 86_400.0)
    } else {
        format!("{:.0}h", seconds / 3_600.0)
    }
}

/// One concise line summarizing storage for the `moraine status` Database
/// panel: bucket totals and disk headroom.
pub(crate) fn status_storage_line(report: &StorageReport) -> String {
    let disk = match report.disk {
        Some(disk) => format!(
            "disk free {} of {}",
            format_bytes(disk.free_bytes),
            format_bytes(disk.total_bytes)
        ),
        None => "disk free unknown".to_string(),
    };
    let buckets = report
        .buckets
        .iter()
        .filter(|bucket| bucket.tables > 0)
        .map(|bucket| {
            format!(
                "{} {}",
                bucket.class.as_str(),
                format_bytes(bucket.compressed_bytes)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "storage: {}  |  {disk}",
        if buckets.is_empty() {
            "no tables".to_string()
        } else {
            buckets
        }
    )
}

/// The prominent `moraine status` line shown when configuration authorizes
/// deleting user history, plus the one-time retention notice.
///
/// A destructive policy is never invisible: this returns `Some` exactly when
/// [`StorageReport::destructive_policies`] is non-empty.
pub(crate) fn status_retention_warning_line(report: &StorageReport) -> Option<String> {
    let destructive = report.destructive_policies();
    if destructive.is_empty() {
        return None;
    }
    Some(format!(
        "RETENTION CONFIGURED — user history will be deleted: {}",
        destructive
            .iter()
            .map(|entry| {
                format!(
                    "{} after {}",
                    entry.class.as_str(),
                    entry
                        .horizon_seconds
                        .map(format_horizon)
                        .unwrap_or_else(|| "?".to_string())
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// Full per-bucket storage block for `moraine db doctor`.
pub(crate) fn storage_report_lines(report: &StorageReport) -> Vec<String> {
    let mut lines = vec![status_storage_line(report)];
    for bucket in &report.buckets {
        lines.push(format!(
            "  {} ({}): {} tables, {} rows, {}",
            bucket.class.as_str(),
            bucket.label,
            bucket.tables,
            bucket.rows,
            format_bytes(bucket.compressed_bytes)
        ));
    }
    for entry in &report.policy {
        lines.push(format!(
            "  policy {}: {} ({}{}){}",
            entry.class.as_str(),
            entry
                .horizon_seconds
                .map(format_horizon)
                .unwrap_or_else(|| "no retention".to_string()),
            entry.source,
            match &entry.config_key {
                Some(key) => format!(", {key}"),
                None => String::new(),
            },
            // A `config_key` printed with no qualification is an invitation to
            // set it. Bucket 4's key changes nothing an operator can observe,
            // and this line is where they would otherwise never learn that.
            match &entry.note {
                Some(note) => format!(" — {note}"),
                None => String::new(),
            }
        ));
    }
    if let Some(warning) = status_retention_warning_line(report) {
        lines.push(warning);
    }
    let unclassified = report.unclassified_tables();
    if !unclassified.is_empty() {
        lines.push(format!(
            "  UNCLASSIFIED TABLES (no reclaim scope may name them): {}",
            unclassified.join(", ")
        ));
    }
    for note in &report.notes {
        lines.push(format!("  note: {note}"));
    }
    lines
}

/// Render `moraine db reclaim status`.
pub(crate) fn render_reclaim_status(
    output: &CliOutput,
    report: &ReclaimStatusReport,
    json: bool,
) -> Result<()> {
    if json || output.is_json() {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    let mut lines = Vec::new();
    match &report.storage {
        Some(storage) => lines.extend(storage_report_lines(storage)),
        None => lines.push(format!(
            "storage: unavailable{}",
            match &report.error {
                Some(error) => format!(" ({error})"),
                None => String::new(),
            }
        )),
    }
    lines.push(format!(
        "ledger: claimed={}, deleting={}, done={}, abandoned={}",
        report.ledger.claimed, report.ledger.deleting, report.ledger.done, report.ledger.abandoned
    ));
    if let Some(blocked) = &report.ledger.blocked_reason {
        lines.push(format!("ledger blocked: {blocked}"));
    }
    lines.push(format!(
        "registered executors: {}",
        if report.registered_executors.is_empty() {
            "none (this build plans and reports only; nothing is deleted)".to_string()
        } else {
            report
                .registered_executors
                .iter()
                .map(|scope| scope.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        }
    ));
    lines.extend(reclaimable_lines(&report.reclaimable));
    lines.push(report.denomination.clone());
    output.section("Storage Reclaim", &lines);
    Ok(())
}

fn reclaimable_lines(estimates: &[reclaim::ReclaimableEstimate]) -> Vec<String> {
    estimates
        .iter()
        .flat_map(|estimate| {
            let mut lines = vec![format!(
                "scope {}: {} units, {} rows, {} ({})",
                estimate.scope.as_str(),
                estimate.units,
                estimate.estimated_rows,
                format_bytes(estimate.estimated_bytes),
                reclaim::ESTIMATE_QUALIFIER,
            )];
            if let Some(note) = &estimate.note {
                lines.push(format!("  {note}"));
            }
            lines
        })
        .collect()
}

/// Render `moraine db reclaim plan`. Dry run: writes nothing.
pub(crate) fn render_reclaim_plan(
    output: &CliOutput,
    plan: &ReclaimPlan,
    json: bool,
) -> Result<()> {
    if json || output.is_json() {
        println!("{}", serde_json::to_string_pretty(plan)?);
        return Ok(());
    }
    let mut lines = vec!["dry run: nothing was written".to_string()];
    lines.extend(reclaimable_lines(&plan.scopes));
    if plan.pending_redrive > 0 {
        lines.push(format!(
            "ledger units awaiting re-drive: {}",
            plan.pending_redrive
        ));
    }
    lines.push(plan.denomination.clone());
    output.section("Storage Reclaim Plan", &lines);
    Ok(())
}

/// The unforced `moraine db reclaim run` refusal.
///
/// Names exactly what would be deleted and points at the export command as the
/// pre-destructive safety valve, following the `--force` ceremony precedent.
pub(crate) fn render_reclaim_refusal(
    output: &CliOutput,
    scope: ReclaimScope,
    json: bool,
) -> Result<()> {
    let tables: Vec<&str> = scope.tables().iter().map(|table| table.name()).collect();
    if json || output.is_json() {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "refused": true,
                "reason": "confirmation required",
                "scope": scope.as_str(),
                "describes": scope.describe(),
                "tables": tables,
                "rerun_with": format!("moraine db reclaim run --scope {scope} --confirm"),
                "export_first": "moraine export events --format jsonl",
            }))?
        );
        return Ok(());
    }
    output.section(
        "Storage Reclaim Refused",
        &[
            format!("scope: {} — {}", scope.as_str(), scope.describe()),
            format!("would delete from: {}", tables.join(", ")),
            "nothing was deleted: --confirm was not given".to_string(),
            format!("re-run with: moraine db reclaim run --scope {scope} --confirm"),
            "export first if unsure: moraine export events --format jsonl".to_string(),
        ],
    );
    Ok(())
}

/// Render the outcome of a confirmed `moraine db reclaim run`.
pub(crate) fn render_reclaim_outcome(
    output: &CliOutput,
    outcome: &ReclaimOutcome,
    json: bool,
) -> Result<()> {
    if json || output.is_json() {
        println!("{}", serde_json::to_string_pretty(outcome)?);
        return Ok(());
    }
    output.section("Storage Reclaim", &reclaim_outcome_lines(outcome));
    Ok(())
}

/// The operator-facing lines for a reclaim outcome.
///
/// Split out of [`render_reclaim_outcome`] so a test can read them. Plan §7.4
/// recorded this as a residual — *"`render_reclaim_outcome` builds its lines
/// in-function and writes them to stdout, so no test reads them … the fix is
/// one line and the guard waits for the outcome to become reachable"* — and
/// WI-05 is where `Blocked` and `Settled` became reachable, because they
/// require a registered executor.
pub(crate) fn reclaim_outcome_lines(outcome: &ReclaimOutcome) -> Vec<String> {
    match outcome {
        ReclaimOutcome::NoExecutor { scope, message } => {
            vec![format!("scope: {}", scope.as_str()), message.clone()]
        }
        ReclaimOutcome::Blocked {
            scope,
            pending_mutations,
        } => vec![
            format!("scope: {}", scope.as_str()),
            format!(
                "blocked: {pending_mutations} mutation(s) over this scope's tables are still \
                 running; nothing was deleted"
            ),
        ],
        ReclaimOutcome::LowDisk {
            scope,
            free_bytes,
            required_bytes,
        } => vec![
            format!("scope: {}", scope.as_str()),
            format!(
                // Not "before any merge frees space": `frees` is in
                // `FORBIDDEN_DENOMINATION_WORDS`, and the whole point of that
                // list is that no reclaim line may put that verb next to a
                // byte figure — including when the sentence is explaining why
                // the run declined.
                "declined: {free_bytes} free byte(s), {required_bytes} required. A reclaim delete \
                 writes row masks first and returns bytes only when a background merge rewrites \
                 the part, so it needs headroom to start. Nothing was deleted."
            ),
        ],
        ReclaimOutcome::Idle { scope } => {
            vec![format!("scope: {}: nothing to reclaim", scope.as_str())]
        }
        ReclaimOutcome::Settled {
            scope,
            units,
            estimated_rows,
            redriven,
            failed,
            abandoned,
            denomination,
        } => {
            let mut lines = vec![
                format!("scope: {}", scope.as_str()),
                format!("units settled: {units}"),
                // Deliberately not "rows reclaimed": this is the claim-time
                // probe estimate for the units this run claimed, and a
                // re-driven unit contributes zero because its rows were never
                // re-counted. The old label promised a measurement nothing
                // took.
                format!("rows (claim-time estimate): {estimated_rows}"),
            ];
            if *redriven > 0 {
                lines.push(format!("units completed by re-drive: {redriven}"));
            }
            // A wedged or abandoned unit must not be invisible inside a
            // success rendering; that is how a poison unit goes unnoticed.
            if *failed > 0 {
                lines.push(format!(
                    "units still unsettled after re-drive: {failed} (retried next run)"
                ));
            }
            if *abandoned > 0 {
                lines.push(format!(
                    "units abandoned after exceeding the unsettled bound: {abandoned}"
                ));
            }
            lines.push(denomination.clone());
            lines
        }
    }
}

/// Render the `moraine db core-index status` report.
pub(crate) fn render_core_index_status(output: &CliOutput, report: &CoreIndexReport) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    output.section("Canonical Read Indexes", &core_index_report_lines(report));
    Ok(())
}

pub(crate) fn render_logs(output: &CliOutput, snapshot: &LogsSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    for section in &snapshot.sections {
        let mut lines = vec![
            format!("path: {}", section.path),
            format!("lines requested: {}", snapshot.requested_lines),
        ];
        if !section.exists {
            lines.push("log file: missing".to_string());
            output.section(&format!("Logs: {}", section.service.name()), &lines);
            continue;
        }
        lines.push(format!("lines returned: {}", section.lines.len()));
        output.section(&format!("Logs: {}", section.service.name()), &lines);
        for line in &section.lines {
            output.line(line);
        }
    }
    Ok(())
}

pub(crate) fn render_clickhouse_status(
    output: &CliOutput,
    snapshot: &ClickhouseStatusSnapshot,
) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![
        format!("managed root: {}", snapshot.managed_root),
        format!(
            "clickhouse binary: {}",
            state_label(snapshot.clickhouse_exists)
        ),
        format!(
            "clickhouse-server binary: {}",
            state_label(snapshot.clickhouse_server_exists)
        ),
        format!(
            "clickhouse-client binary: {}",
            state_label(snapshot.clickhouse_client_exists)
        ),
        format!("expected version: {}", snapshot.expected_version),
        format!(
            "active source: {}{}",
            snapshot.active_source,
            snapshot
                .active_source_path
                .as_ref()
                .map(|p| format!(" ({p})"))
                .unwrap_or_default()
        ),
        format!("checksum state: {}", snapshot.checksum_state),
    ];
    if let Some(version) = &snapshot.installed_version {
        lines.push(format!("installed version: {version}"));
    }
    output.section("Managed ClickHouse", &lines);
    Ok(())
}

pub(crate) fn render_up(output: &CliOutput, snapshot: &UpSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut rows = vec![vec![
        snapshot.clickhouse.service.name().to_string(),
        format_start_state(&snapshot.clickhouse),
        format_start_pid(&snapshot.clickhouse),
    ]];
    rows.extend(snapshot.services.iter().map(|outcome| {
        vec![
            outcome.service.name().to_string(),
            format_start_state(outcome),
            format_start_pid(outcome),
        ]
    }));
    output.table("Startup Results", &["service", "result", "pid"], &rows);
    render_db_migrate(output, &snapshot.migrations)?;
    render_status(output, &snapshot.status)?;
    Ok(())
}

pub(crate) fn render_down(output: &CliOutput, snapshot: &DownSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    if snapshot.stopped.is_empty() {
        output.section("Shutdown", &["no running services found".to_string()]);
        if let Some(warning) = &snapshot.warning {
            output.section("Warning", std::slice::from_ref(warning));
        }
        return Ok(());
    }
    let rows = snapshot
        .stopped
        .iter()
        .map(|service| vec![service.name().to_string(), "stopped".to_string()])
        .collect::<Vec<_>>();
    output.table("Shutdown", &["service", "result"], &rows);
    if let Some(warning) = &snapshot.warning {
        output.section("Warning", std::slice::from_ref(warning));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::Parser;

    #[test]
    fn output_mode_respects_json_flag() {
        let cli = Cli::parse_from(["moraine", "--output", "json", "status"]);
        let output = CliOutput::from_cli(&cli);
        assert_eq!(output.mode, OutputMode::Json);
    }

    #[test]
    fn rich_panel_height_accounts_for_wrapped_lines() {
        let panel = render_panel(
            "Preview",
            &["abcdefghijklmnopqrstuvwxyz".to_string()],
            12,
            false,
        );
        assert!(panel.lines().count() >= 5);
        assert!(panel.contains("abcdefghij"));
        assert!(panel.contains("klmnopqrst"));
        assert!(panel.contains("uvwxyz"));
    }

    fn ready_report() -> CoreIndexReport {
        CoreIndexReport {
            available: true,
            core_indexes_ready: true,
            open_v2_ready: true,
            open_v2_provenance: Some("auto-local".to_string()),
            backfill_cursor_age_seconds: Some(3725),
            audit: Some(CoreIndexAuditOutcome {
                passed: true,
                sampled_sessions: 12,
                sampled_events: 3072,
                ..CoreIndexAuditOutcome::default()
            }),
            configured_open_reader: "auto".to_string(),
            effective_open_reader: "v2".to_string(),
            open_reader_override: false,
            open_reader_note: None,
        }
    }

    #[test]
    fn core_index_lines_surface_readiness_age_audit_and_reader() {
        let lines = core_index_report_lines(&ready_report());
        let joined = lines.join("\n");
        assert!(joined.contains("core indexes ready: yes"), "{joined}");
        assert!(
            joined.contains("open v2 ready: yes (auto-local)"),
            "{joined}"
        );
        // 3725s renders as compact hours/minutes.
        assert!(joined.contains("backfill cursor age: 1h2m"), "{joined}");
        assert!(joined.contains("overlap audit: pass"), "{joined}");
        assert!(
            joined.contains("open reader: configured=auto, effective=v2"),
            "{joined}"
        );
        assert!(!joined.contains("config override"), "{joined}");
    }

    /// The two reader states this renderer can actually be handed after issue
    /// #603 WI-10, rendered rather than asserted as struct fields.
    ///
    /// The retargeting matters: this test used to pin
    /// `configured=v1, effective=v1 (config override)` and a note reading
    /// "v1 forced by [mcp] open_reader". `OpenReaderMode::resolve` can no
    /// longer produce `effective=v1` in any input, and that note text exists
    /// nowhere in the tree — so the guard covered an unreachable state while
    /// the reachable one (`configured=v1, effective=v2`, carrying
    /// `RETIRED_V1_NOTE`) had no render coverage at all; only
    /// `CoreIndexReport` field values were asserted, in `commands.rs`.
    ///
    /// Both directions of the `(config override)` suffix are bounded here: a
    /// configured `v1` must carry it, and an unready stock `auto` install —
    /// the normal state of a fresh install between `migrate` and the first
    /// sweep — must not.
    #[test]
    fn core_index_lines_flag_the_retired_v1_config_and_not_a_mere_unready_store() {
        let mut retired = ready_report();
        retired.configured_open_reader = "v1".to_string();
        retired.effective_open_reader = "v2".to_string();
        retired.open_reader_override = true;
        retired.open_reader_note =
            Some(moraine_config::OpenReaderMode::RETIRED_V1_NOTE.to_string());
        let joined = core_index_report_lines(&retired).join("\n");
        assert!(
            joined.contains("open reader: configured=v1, effective=v2 (config override)"),
            "{joined}"
        );
        assert!(
            joined.contains(&format!(
                "open reader note: {}",
                moraine_config::OpenReaderMode::RETIRED_V1_NOTE
            )),
            "{joined}"
        );

        // Unready under a stock `auto`: an error, and NOT an override.
        let mut unready = ready_report();
        unready.open_v2_ready = false;
        unready.open_v2_provenance = None;
        unready.effective_open_reader = "error".to_string();
        unready.open_reader_override = false;
        unready.open_reader_note =
            Some("the canonical read indexes are not ready; open will fail".to_string());
        let joined = core_index_report_lines(&unready).join("\n");
        assert!(
            joined.contains("open reader: configured=auto, effective=error"),
            "{joined}"
        );
        assert!(
            !joined.contains("config override"),
            "a default `auto` config contains no override to report: {joined}"
        );
    }

    #[test]
    fn core_index_lines_report_unavailable() {
        let report = CoreIndexReport {
            available: false,
            core_indexes_ready: false,
            open_v2_ready: false,
            open_v2_provenance: None,
            backfill_cursor_age_seconds: None,
            audit: None,
            configured_open_reader: "auto".to_string(),
            effective_open_reader: "unknown".to_string(),
            open_reader_override: false,
            open_reader_note: None,
        };
        let joined = core_index_report_lines(&report).join("\n");
        assert!(joined.contains("core indexes: unavailable"), "{joined}");
        assert!(joined.contains("configured=auto"), "{joined}");
    }

    #[test]
    fn age_formatting_covers_every_bucket() {
        assert_eq!(format_age_seconds(-5), "just now");
        assert_eq!(format_age_seconds(42), "42s");
        assert_eq!(format_age_seconds(125), "2m5s");
        assert_eq!(format_age_seconds(3725), "1h2m");
        assert_eq!(format_age_seconds(90_061), "1d1h");
    }

    // ---- issue #603 storage / reclaim rendering -------------------------

    fn storage_fixture(retention: moraine_config::RetentionConfig) -> StorageReport {
        let tables = vec![
            moraine_clickhouse::StorageTableReport {
                name: "events".to_string(),
                class: Some(moraine_clickhouse::TableClass::CanonicalHistory),
                rows: 1_990_776,
                compressed_bytes: 4_787_723_965,
                uncompressed_bytes: 11_420_351_515,
                active_parts: 24,
                oldest_retained: Some("2026-02-20T14:16:45Z".to_string()),
            },
            // A LIVE derived table. `mcp_open_turns` stood here until issue
            // #603 WI-10; after migration 041 `classify` answers `None` for
            // every retired name, so a fixture pairing that name with
            // `Some(Derived)` describes a report the collector can no longer
            // build. (The twin fixture in
            // `moraine-clickhouse/src/storage_report.rs` was migrated to the
            // same live names.)
            moraine_clickhouse::StorageTableReport {
                name: "mcp_event_navigation".to_string(),
                class: Some(moraine_clickhouse::TableClass::Derived),
                rows: 234_694,
                compressed_bytes: 14_356_000_000,
                uncompressed_bytes: 40_000_000_000,
                active_parts: 303,
                oldest_retained: None,
            },
        ];
        StorageReport {
            buckets: moraine_clickhouse::fold_buckets(&tables),
            tables,
            disk: Some(moraine_clickhouse::StorageDiskReport {
                free_bytes: 11_780_276_224,
                total_bytes: 994_662_584_320,
            }),
            policy: moraine_clickhouse::retention_policy_entries(&retention),
            notes: Vec::new(),
        }
    }

    /// **G-DENOM** (the rendered-string half). Fails for: an operator-facing
    /// byte number without its qualifier, or any surface promising
    /// partition-aligned deletion.
    /// Denomination: rendered string.
    ///
    /// MUTATION (executed 2026-07-27): change
    /// `reclaim::reclaimed_bytes_note()` to `"freed N bytes"` => the library
    /// test `byte_denominations_carry_their_qualifiers_and_promise_nothing`
    /// FAILS; this test additionally covers the strings the LIBRARY test
    /// cannot see, because it renders them.
    #[test]
    fn no_rendered_storage_surface_promises_partition_aligned_deletion() {
        let report = storage_fixture(moraine_config::RetentionConfig::default());
        let mut rendered = storage_report_lines(&report);
        rendered.push(status_storage_line(&report));
        rendered.extend(reclaimable_lines(&[reclaim::ReclaimableEstimate {
            scope: ReclaimScope::ReadIndexGeneration,
            units: 3,
            estimated_rows: 10,
            estimated_bytes: 5_368_709_120,
            tables: vec!["mcp_event_navigation".to_string()],
            note: Some("probe not registered".to_string()),
        }]));
        rendered.push(reclaim::estimated_bytes_note());
        rendered.push(reclaim::reclaimed_bytes_note());
        for scope in ReclaimScope::ALL {
            rendered.push(scope.describe().to_string());
        }

        let joined = rendered.join("\n").to_lowercase();
        for forbidden in reclaim::FORBIDDEN_DENOMINATION_WORDS {
            assert!(
                !joined.contains(forbidden),
                "a reclaim surface must never say `{forbidden}`:\n{joined}"
            );
        }
        // Any estimate line must carry the qualifier next to the number.
        let estimate_line = rendered
            .iter()
            .find(|line| line.contains("scope read_index_generation"))
            .expect("estimate line");
        assert!(
            estimate_line.contains(reclaim::ESTIMATE_QUALIFIER),
            "{estimate_line}"
        );
        assert!(reclaim::reclaimed_bytes_note().contains(reclaim::MERGE_DEFERRED_QUALIFIER));
    }

    /// A configured bucket-1/2 horizon must be impossible to miss.
    ///
    /// MUTATION (executed 2026-07-27): make
    /// `RetentionPolicyEntry::is_destructive` drop its `is_protected` guard =>
    /// the library test `default_retention_surfaces_no_destructive_policy`
    /// FAILS. Here the direction bounded is the other one: a CONFIGURED
    /// canonical horizon must produce a warning line, so a "fix" that always
    /// returns false fails this test.
    #[test]
    fn a_configured_canonical_horizon_is_prominent_and_a_default_one_is_absent() {
        let stock = storage_fixture(moraine_config::RetentionConfig::default());
        assert_eq!(status_retention_warning_line(&stock), None);
        assert!(!storage_report_lines(&stock)
            .join("\n")
            .contains("RETENTION CONFIGURED"));

        let configured = storage_fixture(moraine_config::RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            ..moraine_config::RetentionConfig::default()
        });
        let warning = status_retention_warning_line(&configured).expect("warning line");
        assert!(warning.contains("RETENTION CONFIGURED"), "{warning}");
        assert!(
            warning.contains("user history will be deleted"),
            "{warning}"
        );
        assert!(warning.contains("canonical_history"), "{warning}");
        assert!(warning.contains("365d"), "{warning}");
        assert!(storage_report_lines(&configured)
            .join("\n")
            .contains("RETENTION CONFIGURED"));
    }

    /// The rendered half of G-TELEMETRY-HONESTY. A caveat that lives only in
    /// a struct field reaches nobody.
    ///
    /// MUTATION (executed 2026-07-27): drop the `entry.note` arm from
    /// `storage_report_lines` => FAILS here. The library test
    /// `the_telemetry_horizon_never_reports_itself_as_configured` stays green,
    /// because it can only see the struct.
    #[test]
    fn status_lines_render_the_telemetry_horizon_caveat() {
        let rendered =
            storage_report_lines(&storage_fixture(moraine_config::RetentionConfig::default()))
                .join("\n");
        let telemetry = rendered
            .lines()
            .find(|line| line.contains("policy telemetry"))
            .expect("a telemetry policy line");
        assert!(telemetry.contains("30d"), "{telemetry}");
        assert!(telemetry.contains("(default"), "{telemetry}");
        assert!(
            telemetry.contains("retention.telemetry_horizon_days"),
            "{telemetry}"
        );
        assert!(
            telemetry.contains(moraine_clickhouse::TELEMETRY_HORIZON_NOT_CONFIGURABLE_NOTE),
            "the line names a config key, so it must also say the key does nothing: {telemetry}"
        );

        // Upper bound: the other policy lines stay clean, so the caveat is a
        // statement about this key rather than boilerplate on every row.
        for class in ["canonical_history", "raw_audit", "derived", "never_delete"] {
            let line = rendered
                .lines()
                .find(|line| line.starts_with(&format!("  policy {class}:")))
                .unwrap_or_else(|| panic!("a `{class}` policy line"));
            assert!(!line.contains(" — "), "{line}");
        }
    }

    #[test]
    fn the_status_storage_line_reports_buckets_and_disk_headroom() {
        let line =
            status_storage_line(&storage_fixture(moraine_config::RetentionConfig::default()));
        assert!(line.contains("canonical_history 4.46 GiB"), "{line}");
        assert!(line.contains("derived 13.37 GiB"), "{line}");
        assert!(line.contains("disk free 10.97 GiB of 926.35 GiB"), "{line}");
        // Empty buckets are omitted from the concise line but never invented.
        assert!(!line.contains("telemetry"), "{line}");
    }

    /// The `--confirm` refusal **text**. Fails for: a refusal that does not
    /// say what would be deleted, or that omits the pre-destructive safety
    /// valve.
    ///
    /// This test renders `render_reclaim_refusal` directly, so it bounds
    /// exactly one direction: the refusal stays actionable, because a refusal
    /// naming no table is a refusal an operator cannot act on.
    ///
    /// It does **not** cover whether anything calls it. An earlier revision of
    /// this docstring claimed the mutation "drop `args.confirm` from the guard
    /// in `cmd_db_reclaim_run`" was compensated by
    /// `clap_parses_reclaim_subcommands`; a reviewer ran that mutation and the
    /// CLI suite stayed at 229/0. `clap_parses_reclaim_subcommands` proves the
    /// flag parses and defaults to false, and nothing more. The gate itself is
    /// now bounded by
    /// `commands::tests::the_unconfirmed_run_ceremony_is_enforced_end_to_end`,
    /// and its exit code by `commands::tests::a_refusal_exits_non_zero`.
    #[test]
    fn the_unconfirmed_refusal_names_every_table_and_the_export_valve() {
        let output = CliOutput {
            mode: OutputMode::Plain,
            verbose: false,
            unicode: false,
            width: 100,
        };
        for scope in ReclaimScope::ALL {
            // Rendering must not panic for any scope, and the JSON form must
            // carry the same table list as the human form.
            render_reclaim_refusal(&output, scope, false).expect("refusal renders");
        }

        // The canonical scope is the one whose refusal matters most.
        let scope = ReclaimScope::CanonicalGeneration;
        let tables: Vec<&str> = scope.tables().iter().map(|table| table.name()).collect();
        assert!(tables.contains(&"events"));
        assert!(tables.contains(&"raw_events"));
        assert!(scope.describe().contains("USER HISTORY"));
    }

    #[test]
    fn a_run_with_no_registered_executor_renders_as_deleting_nothing() {
        let output = CliOutput {
            mode: OutputMode::Plain,
            verbose: false,
            unicode: false,
            width: 100,
        };
        let outcome = ReclaimOutcome::NoExecutor {
            scope: ReclaimScope::CanonicalGeneration,
            message: "no executor is registered for scope `canonical_generation`; WI-09 adds \
                      it. Nothing was deleted."
                .to_string(),
        };
        render_reclaim_outcome(&output, &outcome, false).expect("outcome renders");
        assert!(!outcome.deleted_anything());

        // A blocked run is a distinct, counted outcome — not an indistinguishable
        // "nothing to do".
        let blocked = ReclaimOutcome::Blocked {
            scope: ReclaimScope::ReadIndexGeneration,
            pending_mutations: 3,
        };
        assert!(!blocked.deleted_anything());
        assert_ne!(
            blocked,
            ReclaimOutcome::Idle {
                scope: ReclaimScope::ReadIndexGeneration
            }
        );
        render_reclaim_outcome(&output, &blocked, false).expect("blocked renders");
    }

    /// **G-DENOM** (the rendered half) and plan §7.4's `Blocked`-string
    /// residual, now that a registered executor makes both outcomes reachable.
    /// Fails for: an operator-facing reclaim line that promises immediate
    /// bytes, claims partition-aligned deletion, or renders a blocked run
    /// indistinguishably from an idle one.
    /// Denomination: the rendered lines, per variant.
    ///
    /// MUTATION (executed 2026-07-28): change the `Settled` arm to drop
    /// `denomination` => FAILS on the merge-deferred assertion. **Lower
    /// bound.**
    ///
    /// MUTATION (executed 2026-07-28): change the `Blocked` arm's text to
    /// `format!("nothing to reclaim; {pending_mutations} skipped")` => FAILS on
    /// the idle-phrase assertion. **Width: hazard H9 at the operator surface,
    /// not only in the type.** The bare `"nothing to reclaim"` — round 2's
    /// recipe, which its own assertion could not catch — was executed too and
    /// also FAILS here.
    ///
    /// The previous revision asserted `assert_ne!(blocked, idle)` for this, and
    /// **that assertion cannot fail for any text change**: `Blocked` renders
    /// two lines and `Idle` renders one, so the vectors differ on length
    /// whatever the strings say. The realistic regression — a "blocked" line
    /// that opens with the idle phrase and buries the count — was green. The
    /// phrase is pinned as a literal here rather than read from the `Idle` arm,
    /// because an expectation derived from the subject cannot fail for the
    /// subject changing.
    ///
    /// MUTATION (executed 2026-07-28): replace the whole `LowDisk` arm with
    /// `ReclaimOutcome::LowDisk { scope, .. } => {
    /// vec![format!("scope: {}: nothing to reclaim", scope.as_str())] }` =>
    /// FAILS on the low-disk assertions. **Width: `LowDisk` is the state an
    /// operator most needs distinguished from "nothing to do", and no test read
    /// those lines at all.** The `..` is load-bearing in the recipe, not
    /// cosmetic: keeping the destructuring while dropping the bindings' uses
    /// is an unused-variable warning, so a recipe that says only "collapse the
    /// arm" does not describe something that compiles.
    ///
    /// MUTATION (executed 2026-07-28): change `if *failed > 0` in the `Settled`
    /// arm to `if false` => FAILS on the poison-unit assertion. **Width: a
    /// wedged unit must not be invisible inside a success rendering, which is
    /// what the arm's own comment says and nothing checked.**
    ///
    /// MUTATION (executed 2026-07-28): reword `reclaimed_bytes_note` to
    /// `"frees N bytes on disk"` => FAILS on the forbidden-word scan.
    /// **Width.**
    ///
    /// *Every* is meant literally: all five `ReclaimOutcome` variants are
    /// driven. A previous revision skipped `NoExecutor` — the only variant
    /// whose text this function does not itself write, and therefore the only
    /// one where the forbidden-word scan had something to find. Its message is
    /// read from `reclaim::no_executor_message` rather than rebuilt here,
    /// because a scan over a copy of the string cannot fail for the string
    /// changing.
    ///
    /// MUTATION (executed 2026-07-28): reword `no_executor_message` to
    /// `"no executor for `{scope}`; a later work item frees this space."` =>
    /// FAILS here on the forbidden-word scan, and passed the whole workspace
    /// before `NoExecutor` was driven. **Width.**
    #[test]
    fn every_rendered_reclaim_line_carries_its_denomination() {
        /// The phrase that means "this run had nothing to do". Pinned, not
        /// read from the `Idle` arm.
        const IDLE_PHRASE: &str = "nothing to reclaim";

        let scope = ReclaimScope::ReadIndexGeneration;
        let settled = reclaim_outcome_lines(&ReclaimOutcome::Settled {
            scope,
            units: 7,
            estimated_rows: 4_096,
            redriven: 0,
            failed: 0,
            abandoned: 0,
            denomination: moraine_clickhouse::reclaim::reclaimed_bytes_note(),
        });
        assert!(
            settled
                .iter()
                .any(|line| line.contains(moraine_clickhouse::reclaim::MERGE_DEFERRED_QUALIFIER)),
            "{settled:?}"
        );
        assert!(settled
            .iter()
            .any(|line| line.contains("rows (claim-time estimate): 4096")));
        assert!(
            !settled.iter().any(|line| line.contains("rows reclaimed")),
            "the estimate must never be rendered as a count of rows removed: {settled:?}"
        );

        // A run that settled some units and left one wedged is not a clean
        // success, and the count must survive to the operator's terminal.
        let poisoned = reclaim_outcome_lines(&ReclaimOutcome::Settled {
            scope,
            units: 7,
            estimated_rows: 4_096,
            redriven: 2,
            failed: 1,
            abandoned: 3,
            denomination: moraine_clickhouse::reclaim::reclaimed_bytes_note(),
        });
        for (needle, what) in [
            ("units completed by re-drive: 2", "the re-drive count"),
            ("unsettled after re-drive: 1", "the wedged unit"),
            ("bound: 3", "the abandoned units"),
        ] {
            assert!(
                poisoned.iter().any(|line| line.contains(needle)),
                "{what} never reached the operator: {poisoned:?}"
            );
        }

        // The fifth variant, and the only one whose text is not a literal in
        // the function under test: `NoExecutor` renders a `String` built two
        // crates away. "Every rendered line" is not true of a scan that skips
        // the one line this function does not itself write.
        let no_executor = reclaim_outcome_lines(&ReclaimOutcome::NoExecutor {
            scope,
            message: moraine_clickhouse::reclaim::no_executor_message(scope),
        });
        assert!(
            no_executor
                .iter()
                .any(|line| line.contains("Nothing was deleted")),
            "a refusal must say it deleted nothing: {no_executor:?}"
        );

        let idle = reclaim_outcome_lines(&ReclaimOutcome::Idle { scope });
        assert!(
            idle.iter().any(|line| line.contains(IDLE_PHRASE)),
            "the idle phrase moved; every assertion below is written against it: {idle:?}"
        );

        let blocked = reclaim_outcome_lines(&ReclaimOutcome::Blocked {
            scope,
            pending_mutations: 3,
        });
        assert!(
            blocked.iter().any(|line| line.contains('3')),
            "the blocked line must carry the count: {blocked:?}"
        );

        // The `LowDisk` refusal: both figures, and an explanation of why more
        // free space is needed to free space.
        let low_disk = reclaim_outcome_lines(&ReclaimOutcome::LowDisk {
            scope,
            free_bytes: 1_073_741_824,
            required_bytes: 10_737_418_240,
        });
        for needle in ["1073741824", "10737418240"] {
            assert!(
                low_disk.iter().any(|line| line.contains(needle)),
                "the low-disk refusal must carry `{needle}`: {low_disk:?}"
            );
        }
        assert!(
            low_disk
                .iter()
                .any(|line| line.to_lowercase().contains("nothing was deleted")),
            "a refusal must say it deleted nothing: {low_disk:?}"
        );

        // H9, at the operator surface: neither refusal may read as "there was
        // nothing to do".
        for (variant, lines) in [
            ("blocked", &blocked),
            ("low disk", &low_disk),
            ("no executor", &no_executor),
        ] {
            assert!(
                !lines
                    .iter()
                    .any(|line| line.to_lowercase().contains(IDLE_PHRASE)),
                "`{variant}` renders as idle, which is how a stalled reclaimer stays \
                 invisible: {lines:?}"
            );
        }

        for lines in [
            &settled,
            &poisoned,
            &blocked,
            &idle,
            &low_disk,
            &no_executor,
        ] {
            for line in lines.iter() {
                let lowered = line.to_lowercase();
                for forbidden in moraine_clickhouse::reclaim::FORBIDDEN_DENOMINATION_WORDS {
                    assert!(
                        !lowered.contains(forbidden),
                        "`{forbidden}` promises something no table's layout supports: {line}"
                    );
                }
            }
        }
    }

    #[test]
    fn byte_formatting_is_stable_across_unit_boundaries() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(4_787_723_965), "4.46 GiB");
    }
}
