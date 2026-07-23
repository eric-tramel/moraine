use anyhow::Result;
use moraine_clickhouse::DoctorReport;
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

pub(crate) fn render_db_doctor(output: &CliOutput, report: &DoctorReport) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(report)?);
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
    output.section("DB Doctor", &lines);
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
}
