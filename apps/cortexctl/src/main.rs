use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use cortex_clickhouse::{ClickHouseClient, DoctorReport};
use cortex_config::AppConfig;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table, Widget, Wrap};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

const CLICKHOUSE_TEMPLATE: &str = include_str!("../../../config/clickhouse.xml");
const USERS_TEMPLATE: &str = include_str!("../../../config/users.xml");

const DEFAULT_CLICKHOUSE_TAG: &str = "v25.12.5.44-stable";
const CH_URL_MACOS_X86_64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-macos";
const CH_SHA_MACOS_X86_64: &str =
    "8035b4b7905147156192216cc6937a29d0cd2775d481b5f297cdc11058cb68c4";
const CH_URL_MACOS_AARCH64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-macos-aarch64";
const CH_SHA_MACOS_AARCH64: &str =
    "1a0edc37c6e5aa6c06a7cb00c8f8edd83a0df02f643e29185a8b3934eb860ac4";
const CH_URL_LINUX_X86_64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-common-static-25.12.5.44-amd64.tgz";
const CH_SHA_LINUX_X86_64: &str =
    "3756d8b061f97abd79621df1a586f6ba777e8787696f21d82bc488ce5dbca2d7";
const CH_URL_LINUX_AARCH64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-common-static-25.12.5.44-arm64.tgz";
const CH_SHA_LINUX_AARCH64: &str =
    "3d227e50109b0dab330ee2230f46d76f0360f1a61956443c37de5b7651fb488b";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Auto,
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "cortexctl",
    about = "Unified runtime control plane for Cortex services"
)]
struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Auto)]
    output: OutputFormat,
    #[arg(long, global = true, default_value_t = false)]
    verbose: bool,
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    Up(UpArgs),
    Down,
    Status,
    Logs(LogsArgs),
    Db(DbArgs),
    Clickhouse(ClickhouseArgs),
    Service(ServiceArgs),
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct UpArgs {
    #[arg(long)]
    no_ingest: bool,
    #[arg(long)]
    monitor: bool,
    #[arg(long)]
    mcp: bool,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(value_enum)]
    service: Option<Service>,
    #[arg(long, default_value_t = 200)]
    lines: usize,
}

#[derive(Debug, Args)]
struct DbArgs {
    #[command(subcommand)]
    command: DbCommand,
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    Migrate,
    Doctor,
}

#[derive(Debug, Args)]
struct ClickhouseArgs {
    #[command(subcommand)]
    command: ClickhouseCommand,
}

#[derive(Debug, Subcommand)]
enum ClickhouseCommand {
    Install(ClickhouseInstallArgs),
    Status,
    Uninstall,
}

#[derive(Debug, Args)]
struct ClickhouseInstallArgs {
    #[arg(long)]
    force: bool,
    #[arg(long)]
    version: Option<String>,
}

#[derive(Debug, Args)]
struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    Install(ServiceInstallArgs),
    Uninstall(ServiceUninstallArgs),
    Status,
}

#[derive(Debug, Args)]
struct ServiceInstallArgs {
    #[arg(long)]
    enable: bool,
    #[arg(long)]
    start: bool,
}

#[derive(Debug, Args)]
struct ServiceUninstallArgs {
    #[arg(long)]
    disable: bool,
    #[arg(long)]
    stop: bool,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(value_enum)]
    service: Service,
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        num_args = 0..
    )]
    args: Vec<String>,
}

#[derive(Clone)]
struct RuntimePaths {
    root: PathBuf,
    logs_dir: PathBuf,
    pids_dir: PathBuf,
    clickhouse_root: PathBuf,
    clickhouse_config: PathBuf,
    clickhouse_users: PathBuf,
    service_bin_dir: PathBuf,
    managed_clickhouse_dir: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum Service {
    #[value(name = "clickhouse")]
    ClickHouse,
    #[value(name = "ingest")]
    Ingest,
    #[value(name = "monitor")]
    Monitor,
    #[value(name = "mcp")]
    Mcp,
}

impl Service {
    fn name(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::Ingest => "ingest",
            Self::Monitor => "monitor",
            Self::Mcp => "mcp",
        }
    }

    fn pid_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.pid",
            Self::Ingest => "ingest.pid",
            Self::Monitor => "monitor.pid",
            Self::Mcp => "mcp.pid",
        }
    }

    fn log_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.log",
            Self::Ingest => "ingest.log",
            Self::Monitor => "monitor.log",
            Self::Mcp => "mcp.log",
        }
    }

    fn binary_name(self) -> Option<&'static str> {
        match self {
            Self::ClickHouse => None,
            Self::Ingest => Some("cortex-ingest"),
            Self::Monitor => Some("cortex-monitor"),
            Self::Mcp => Some("cortex-mcp"),
        }
    }

    fn launchd_label(self) -> &'static str {
        match self {
            Self::ClickHouse => "com.eric.cortex.clickhouse",
            Self::Ingest => "com.eric.cortex.ingest",
            Self::Monitor => "com.eric.cortex.monitor",
            Self::Mcp => "com.eric.cortex.mcp",
        }
    }

    fn systemd_unit(self) -> &'static str {
        match self {
            Self::ClickHouse => "cortex-clickhouse.service",
            Self::Ingest => "cortex-ingest.service",
            Self::Monitor => "cortex-monitor.service",
            Self::Mcp => "cortex-mcp.service",
        }
    }
}

#[derive(Debug, Deserialize)]
struct HeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
    #[serde(default)]
    watcher_backend: String,
    #[serde(default)]
    watcher_error_count: u64,
    #[serde(default)]
    watcher_reset_count: u64,
    #[serde(default)]
    watcher_last_reset_unix_ms: u64,
}

#[derive(Debug, Deserialize)]
struct LegacyHeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
}

#[derive(Clone, Copy, Debug)]
struct ClickHouseAsset {
    url: &'static str,
    sha256: &'static str,
    is_archive: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceRuntimeStatus {
    service: Service,
    pid: Option<u32>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum HeartbeatSnapshot {
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
struct StatusSnapshot {
    services: Vec<ServiceRuntimeStatus>,
    managed_clickhouse_installed: bool,
    managed_clickhouse_path: String,
    managed_clickhouse_version: Option<String>,
    clickhouse_active_source: String,
    clickhouse_active_source_path: Option<String>,
    managed_clickhouse_checksum: String,
    doctor: DoctorReport,
    heartbeat: HeartbeatSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MigrationOutcome {
    applied: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceLogSection {
    service: Service,
    path: String,
    exists: bool,
    lines: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct LogsSnapshot {
    requested_lines: usize,
    sections: Vec<ServiceLogSection>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClickhouseStatusSnapshot {
    managed_root: String,
    clickhouse_exists: bool,
    clickhouse_server_exists: bool,
    clickhouse_client_exists: bool,
    expected_version: String,
    active_source: String,
    active_source_path: Option<String>,
    checksum_state: String,
    installed_version: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceInstallSnapshot {
    platform: String,
    install_dir: String,
    services: Vec<Service>,
    enable: bool,
    start: bool,
    notes: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceUninstallSnapshot {
    platform: String,
    disable: bool,
    stop: bool,
    services: Vec<Service>,
    notes: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceStatusRow {
    service: Service,
    installed: Option<bool>,
    loaded: Option<bool>,
    enabled: Option<bool>,
    active: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceStatusSnapshot {
    platform: String,
    linger_user: Option<String>,
    linger_state: Option<String>,
    rows: Vec<ServiceStatusRow>,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StartState {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, serde::Serialize)]
struct StartOutcome {
    service: Service,
    state: StartState,
    pid: u32,
    log_path: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct UpSnapshot {
    clickhouse: StartOutcome,
    migrations: MigrationOutcome,
    services: Vec<StartOutcome>,
    status: StatusSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
struct DownSnapshot {
    stopped: Vec<Service>,
}

struct CliOutput {
    mode: OutputMode,
    verbose: bool,
    unicode: bool,
    width: u16,
}

impl CliOutput {
    fn from_cli(cli: &Cli) -> Self {
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

    fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    fn section(&self, title: &str, lines: &[String]) {
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

    fn table(&self, title: &str, headers: &[&str], rows: &[Vec<String>]) {
        match self.mode {
            OutputMode::Plain => print_plain_table(title, headers, rows),
            OutputMode::Rich => {
                let table = render_table(title, headers, rows, self.width, self.unicode);
                println!("{table}");
            }
            OutputMode::Json => {}
        }
    }

    fn line(&self, text: &str) {
        if self.mode != OutputMode::Json {
            println!("{text}");
        }
    }
}

fn render_panel(title: &str, lines: &[String], width: u16, unicode: bool) -> String {
    let area = Rect::new(0, 0, width, (lines.len().max(1) as u16).saturating_add(2));
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

fn runtime_paths(cfg: &AppConfig) -> RuntimePaths {
    let root = PathBuf::from(&cfg.runtime.root_dir);
    let clickhouse_root = root.join("clickhouse");

    RuntimePaths {
        root,
        logs_dir: PathBuf::from(&cfg.runtime.logs_dir),
        pids_dir: PathBuf::from(&cfg.runtime.pids_dir),
        clickhouse_config: clickhouse_root.join("config.xml"),
        clickhouse_users: clickhouse_root.join("users.xml"),
        clickhouse_root,
        service_bin_dir: PathBuf::from(&cfg.runtime.service_bin_dir),
        managed_clickhouse_dir: PathBuf::from(&cfg.runtime.managed_clickhouse_dir),
    }
}

fn ensure_runtime_dirs(paths: &RuntimePaths) -> Result<()> {
    fs::create_dir_all(&paths.root)
        .with_context(|| format!("failed to create {}", paths.root.display()))?;
    fs::create_dir_all(&paths.logs_dir)
        .with_context(|| format!("failed to create {}", paths.logs_dir.display()))?;
    fs::create_dir_all(&paths.pids_dir)
        .with_context(|| format!("failed to create {}", paths.pids_dir.display()))?;

    fs::create_dir_all(paths.clickhouse_root.join("data"))?;
    fs::create_dir_all(paths.clickhouse_root.join("tmp"))?;
    fs::create_dir_all(paths.clickhouse_root.join("log"))?;
    fs::create_dir_all(paths.clickhouse_root.join("user_files"))?;
    fs::create_dir_all(paths.clickhouse_root.join("format_schemas"))?;

    Ok(())
}

fn pid_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    paths.pids_dir.join(service.pid_file())
}

fn log_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    paths.logs_dir.join(service.log_file())
}

fn read_pid(path: &Path) -> Option<u32> {
    let text = fs::read_to_string(path).ok()?;
    text.trim().parse::<u32>().ok()
}

fn write_pid(path: &Path, pid: u32) -> Result<()> {
    fs::write(path, format!("{}\n", pid))
        .with_context(|| format!("failed to write pid file {}", path.display()))
}

fn is_pid_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn ensure_pid_fresh(path: &Path) {
    if let Some(pid) = read_pid(path) {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(path);
        }
    }
}

fn service_running(paths: &RuntimePaths, service: Service) -> Option<u32> {
    let path = pid_path(paths, service);
    ensure_pid_fresh(&path);
    let pid = read_pid(&path)?;
    if is_pid_running(pid) {
        Some(pid)
    } else {
        None
    }
}

fn stop_service(paths: &RuntimePaths, service: Service) -> Result<bool> {
    let path = pid_path(paths, service);
    let Some(pid) = read_pid(&path) else {
        return Ok(false);
    };

    if !is_pid_running(pid) {
        let _ = fs::remove_file(path);
        return Ok(false);
    }

    let _ = Command::new("kill").arg(pid.to_string()).status();

    for _ in 0..20 {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(&path);
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
    let _ = fs::remove_file(path);
    Ok(true)
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

fn load_cfg(raw_config: Option<PathBuf>) -> Result<(PathBuf, AppConfig)> {
    let config_path = cortex_config::resolve_config_path(raw_config);
    let cfg = cortex_config::load_config(&config_path)
        .with_context(|| format!("failed to load config {}", config_path.display()))?;
    Ok((config_path, cfg))
}

fn clickhouse_ports_from_url(cfg: &AppConfig) -> Result<(u16, u16, u16)> {
    let parsed = reqwest::Url::parse(&cfg.clickhouse.url)
        .with_context(|| format!("invalid clickhouse.url '{}'", cfg.clickhouse.url))?;
    let http_port = parsed.port_or_known_default().ok_or_else(|| {
        anyhow!(
            "clickhouse.url '{}' must include a known port",
            cfg.clickhouse.url
        )
    })?;
    let tcp_port = http_port
        .checked_add(877)
        .ok_or_else(|| anyhow!("derived clickhouse tcp port overflow from {}", http_port))?;
    let interserver_http_port = http_port.checked_add(886).ok_or_else(|| {
        anyhow!(
            "derived clickhouse interserver port overflow from {}",
            http_port
        )
    })?;
    Ok((http_port, tcp_port, interserver_http_port))
}

fn materialize_clickhouse_config(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    let (http_port, tcp_port, interserver_http_port) = clickhouse_ports_from_url(cfg)?;
    let rendered_clickhouse = CLICKHOUSE_TEMPLATE
        .replace("__CORTEX_HOME__", &cfg.runtime.root_dir)
        .replace("__CLICKHOUSE_HTTP_PORT__", &http_port.to_string())
        .replace("__CLICKHOUSE_TCP_PORT__", &tcp_port.to_string())
        .replace(
            "__CLICKHOUSE_INTERSERVER_HTTP_PORT__",
            &interserver_http_port.to_string(),
        );
    let rendered_users = USERS_TEMPLATE.replace("__CORTEX_HOME__", &cfg.runtime.root_dir);

    fs::write(&paths.clickhouse_config, rendered_clickhouse).with_context(|| {
        format!(
            "failed writing clickhouse config {}",
            paths.clickhouse_config.display()
        )
    })?;
    fs::write(&paths.clickhouse_users, rendered_users).with_context(|| {
        format!(
            "failed writing users config {}",
            paths.clickhouse_users.display()
        )
    })?;

    Ok(())
}

fn managed_clickhouse_bin(paths: &RuntimePaths, binary: &str) -> PathBuf {
    paths.managed_clickhouse_dir.join("bin").join(binary)
}

fn managed_clickhouse_checksum_file(paths: &RuntimePaths) -> PathBuf {
    paths.managed_clickhouse_dir.join("SHA256")
}

fn detect_host_target() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        (os, arch) => bail!(
            "unsupported platform for managed ClickHouse: {} {}",
            os,
            arch
        ),
    }
}

fn clickhouse_asset_for_target(version: &str, target: &str) -> Result<ClickHouseAsset> {
    if version != DEFAULT_CLICKHOUSE_TAG {
        bail!(
            "unsupported ClickHouse version {}; this build supports {}",
            version,
            DEFAULT_CLICKHOUSE_TAG
        );
    }

    match target {
        "x86_64-apple-darwin" => Ok(ClickHouseAsset {
            url: CH_URL_MACOS_X86_64,
            sha256: CH_SHA_MACOS_X86_64,
            is_archive: false,
        }),
        "aarch64-apple-darwin" => Ok(ClickHouseAsset {
            url: CH_URL_MACOS_AARCH64,
            sha256: CH_SHA_MACOS_AARCH64,
            is_archive: false,
        }),
        "x86_64-unknown-linux-gnu" => Ok(ClickHouseAsset {
            url: CH_URL_LINUX_X86_64,
            sha256: CH_SHA_LINUX_X86_64,
            is_archive: true,
        }),
        "aarch64-unknown-linux-gnu" => Ok(ClickHouseAsset {
            url: CH_URL_LINUX_AARCH64,
            sha256: CH_SHA_LINUX_AARCH64,
            is_archive: true,
        }),
        other => bail!("unsupported ClickHouse target: {}", other),
    }
}

fn clickhouse_asset_for_host(version: &str) -> Result<ClickHouseAsset> {
    clickhouse_asset_for_target(version, detect_host_target()?)
}

async fn download_to_path(url: &str, dest: &Path) -> Result<()> {
    let client = Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download {}", url))?
        .error_for_status()
        .with_context(|| format!("download failed for {}", url))?;

    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed reading response body for {}", url))?;

    fs::write(dest, &bytes).with_context(|| format!("failed writing {}", dest.display()))
}

fn sha256_hex(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("failed opening {} for checksum", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = std::io::Read::read(&mut file, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let digest = hasher.finalize();
    Ok(digest.iter().map(|b| format!("{:02x}", b)).collect())
}

fn find_file_named(root: &Path, name: &str) -> Result<Option<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };

            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }

            if path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s == name)
            {
                return Ok(Some(path));
            }
        }
    }

    Ok(None)
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn ensure_symlink(target: &Path, link: &Path) -> Result<()> {
    if link.exists() {
        let _ = fs::remove_file(link);
    }
    symlink(target, link).with_context(|| {
        format!(
            "failed creating symlink {} -> {}",
            link.display(),
            target.display()
        )
    })
}

#[cfg(not(unix))]
fn ensure_symlink(target: &Path, link: &Path) -> Result<()> {
    fs::copy(target, link)?;
    Ok(())
}

async fn install_managed_clickhouse(
    paths: &RuntimePaths,
    version: &str,
    force: bool,
) -> Result<PathBuf> {
    let asset = clickhouse_asset_for_host(version)?;

    let bin_dir = paths.managed_clickhouse_dir.join("bin");
    let clickhouse = bin_dir.join("clickhouse");
    let clickhouse_server = bin_dir.join("clickhouse-server");

    if clickhouse_server.exists() && !force {
        return Ok(clickhouse_server);
    }

    fs::create_dir_all(paths.root.join("tmp"))
        .with_context(|| format!("failed to create {}", paths.root.join("tmp").display()))?;

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let download = paths
        .root
        .join("tmp")
        .join(format!("clickhouse-download-{}", stamp));
    let extract_dir = paths
        .root
        .join("tmp")
        .join(format!("clickhouse-extract-{}", stamp));

    download_to_path(asset.url, &download).await?;

    let digest = sha256_hex(&download)?;
    if digest != asset.sha256 {
        bail!(
            "managed ClickHouse checksum mismatch: expected {}, got {}",
            asset.sha256,
            digest
        );
    }

    fs::create_dir_all(&extract_dir)?;

    let staged_binary = if asset.is_archive {
        let status = Command::new("tar")
            .env("LC_ALL", "C")
            .arg("-xzf")
            .arg(&download)
            .arg("-C")
            .arg(&extract_dir)
            .status()
            .context("failed to run tar while installing ClickHouse")?;
        if !status.success() {
            bail!("failed to extract ClickHouse archive");
        }

        find_file_named(&extract_dir, "clickhouse")?
            .ok_or_else(|| anyhow!("extracted ClickHouse archive missing clickhouse binary"))?
    } else {
        download.clone()
    };

    fs::create_dir_all(&bin_dir)
        .with_context(|| format!("failed creating {}", bin_dir.display()))?;

    fs::copy(&staged_binary, &clickhouse)
        .with_context(|| format!("failed writing {}", clickhouse.display()))?;
    make_executable(&clickhouse)?;

    ensure_symlink(&clickhouse, &clickhouse_server)?;
    ensure_symlink(&clickhouse, &bin_dir.join("clickhouse-client"))?;

    fs::write(
        paths.managed_clickhouse_dir.join("VERSION"),
        format!("{}\n", version),
    )?;
    fs::write(
        managed_clickhouse_checksum_file(paths),
        format!("{}\n", digest),
    )?;

    let _ = fs::remove_file(download);
    let _ = fs::remove_dir_all(extract_dir);

    Ok(clickhouse_server)
}

fn clickhouse_from_path_available() -> bool {
    Command::new("clickhouse-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn resolve_clickhouse_server_command(
    cfg: &AppConfig,
    paths: &RuntimePaths,
) -> Result<PathBuf> {
    let managed = managed_clickhouse_bin(paths, "clickhouse-server");
    if managed.exists() {
        return Ok(managed);
    }

    if clickhouse_from_path_available() {
        return Ok(PathBuf::from("clickhouse-server"));
    }

    if cfg.runtime.clickhouse_auto_install {
        install_managed_clickhouse(paths, &cfg.runtime.clickhouse_version, false).await?;
        let managed = managed_clickhouse_bin(paths, "clickhouse-server");
        if managed.exists() {
            return Ok(managed);
        }
    }

    bail!(
        "clickhouse-server is not installed or not on PATH (managed install dir: {})",
        paths.managed_clickhouse_dir.display()
    )
}

async fn wait_for_clickhouse(cfg: &AppConfig) -> Result<()> {
    let client = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let timeout = Duration::from_secs_f64(cfg.runtime.clickhouse_start_timeout_seconds.max(1.0));
    let interval = Duration::from_millis(cfg.runtime.healthcheck_interval_ms.max(100));
    let start = Instant::now();

    loop {
        if client.ping().await.is_ok() {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "clickhouse did not become healthy within {:.1}s",
                timeout.as_secs_f64()
            );
        }

        sleep(interval).await;
    }
}

async fn start_clickhouse(cfg: &AppConfig, paths: &RuntimePaths) -> Result<StartOutcome> {
    if let Some(pid) = service_running(paths, Service::ClickHouse) {
        return Ok(StartOutcome {
            service: Service::ClickHouse,
            state: StartState::AlreadyRunning,
            pid,
            log_path: Some(log_path(paths, Service::ClickHouse).display().to_string()),
        });
    }

    let server_bin = resolve_clickhouse_server_command(cfg, paths).await?;

    materialize_clickhouse_config(cfg, paths)?;

    let logfile = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(paths, Service::ClickHouse))
        .context("failed to open clickhouse log file")?;
    let logfile_err = logfile
        .try_clone()
        .context("failed to clone clickhouse log file")?;

    let child = Command::new(&server_bin)
        .arg("--config-file")
        .arg(&paths.clickhouse_config)
        .stdout(Stdio::from(logfile))
        .stderr(Stdio::from(logfile_err))
        .spawn()
        .with_context(|| format!("failed to start {}", server_bin.display()))?;

    write_pid(&pid_path(paths, Service::ClickHouse), child.id())?;

    wait_for_clickhouse(cfg).await?;
    Ok(StartOutcome {
        service: Service::ClickHouse,
        state: StartState::Started,
        pid: child.id(),
        log_path: Some(log_path(paths, Service::ClickHouse).display().to_string()),
    })
}

async fn run_foreground_clickhouse(cfg: &AppConfig, paths: &RuntimePaths) -> Result<ExitCode> {
    ensure_runtime_dirs(paths)?;
    let server_bin = resolve_clickhouse_server_command(cfg, paths).await?;
    materialize_clickhouse_config(cfg, paths)?;

    let status = Command::new(server_bin)
        .arg("--config-file")
        .arg(&paths.clickhouse_config)
        .status()
        .context("failed to run clickhouse-server")?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

fn env_flag_enabled(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn source_tree_mode_enabled() -> bool {
    env_flag_enabled("CORTEX_SOURCE_TREE_MODE")
}

fn resolve_service_binary(service: Service, paths: &RuntimePaths) -> PathBuf {
    let Some(name) = service.binary_name() else {
        return PathBuf::from(service.name());
    };

    if let Ok(dir) = std::env::var("CORTEX_SERVICE_BIN_DIR") {
        let path = PathBuf::from(dir).join(name);
        if path.exists() {
            return path;
        }
    }

    let configured = paths.service_bin_dir.join(name);
    if configured.exists() {
        return configured;
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(name);
            if sibling.exists() {
                return sibling;
            }
        }
    }

    if source_tree_mode_enabled() {
        if let Some(project_bin) = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("target").join("debug").join(name))
        {
            if project_bin.exists() {
                return project_bin;
            }
        }
    }

    PathBuf::from(name)
}

fn contains_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn monitor_dir_candidates(root: &Path) -> [PathBuf; 2] {
    [
        root.join("web").join("monitor").join("dist"),
        root.join("web").join("monitor"),
    ]
}

fn resolve_monitor_static_dir(paths: &RuntimePaths) -> Option<PathBuf> {
    if let Ok(value) = std::env::var("CORTEX_MONITOR_STATIC_DIR") {
        let path = PathBuf::from(value);
        if path.exists() {
            return Some(path);
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(bin_dir) = exe.parent() {
            if let Some(bundle_root) = bin_dir.parent() {
                for candidate in monitor_dir_candidates(bundle_root) {
                    if candidate.exists() {
                        return Some(candidate);
                    }
                }
            }
        }
    }

    if let Some(bundle_root) = paths.service_bin_dir.parent() {
        for candidate in monitor_dir_candidates(bundle_root) {
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    if source_tree_mode_enabled() {
        if let Some(dev_path) = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(PathBuf::from)
        {
            for candidate in monitor_dir_candidates(&dev_path) {
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
    }

    None
}

fn service_args_with_defaults(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    passthrough: &[String],
) -> Vec<String> {
    let mut args = Vec::new();

    if !contains_flag(passthrough, "--config") {
        args.push("--config".to_string());
        args.push(cfg_path.to_string_lossy().to_string());
    }

    if service == Service::Monitor {
        if !contains_flag(passthrough, "--host") {
            args.push("--host".to_string());
            args.push(cfg.monitor.host.clone());
        }
        if !contains_flag(passthrough, "--port") {
            args.push("--port".to_string());
            args.push(cfg.monitor.port.to_string());
        }
        if !contains_flag(passthrough, "--static-dir") {
            if let Some(static_dir) = resolve_monitor_static_dir(paths) {
                args.push("--static-dir".to_string());
                args.push(static_dir.to_string_lossy().to_string());
            }
        }
    }

    args.extend(passthrough.iter().cloned());
    args
}

fn start_background_service(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    extra_args: &[String],
) -> Result<StartOutcome> {
    if service == Service::ClickHouse {
        bail!("clickhouse is not managed by service launcher; use `cortexctl up`");
    }

    if let Some(pid) = service_running(paths, service) {
        return Ok(StartOutcome {
            service,
            state: StartState::AlreadyRunning,
            pid,
            log_path: Some(log_path(paths, service).display().to_string()),
        });
    }

    let binary = resolve_service_binary(service, paths);

    let logfile = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(paths, service))
        .with_context(|| format!("failed to open {} log", service.name()))?;
    let logfile_err = logfile
        .try_clone()
        .with_context(|| format!("failed to clone {} log", service.name()))?;

    let args = service_args_with_defaults(service, cfg_path, cfg, paths, extra_args);

    let child = Command::new(&binary)
        .args(args)
        .stdout(Stdio::from(logfile))
        .stderr(Stdio::from(logfile_err))
        .spawn()
        .with_context(|| format!("failed to start {}", service.name()))?;

    write_pid(&pid_path(paths, service), child.id())?;
    Ok(StartOutcome {
        service,
        state: StartState::Started,
        pid: child.id(),
        log_path: Some(log_path(paths, service).display().to_string()),
    })
}

async fn run_foreground_service(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    passthrough_args: &[String],
) -> Result<ExitCode> {
    if service == Service::ClickHouse {
        return run_foreground_clickhouse(cfg, paths).await;
    }

    let binary = resolve_service_binary(service, paths);
    let args = service_args_with_defaults(service, cfg_path, cfg, paths, passthrough_args);

    let status = Command::new(binary)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {}", service.name()))?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

async fn cmd_db_migrate(cfg: &AppConfig) -> Result<MigrationOutcome> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let applied = ch.run_migrations().await?;
    Ok(MigrationOutcome { applied })
}

async fn cmd_db_doctor(cfg: &AppConfig) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ch.doctor_report().await
}

async fn query_heartbeat(cfg: &AppConfig) -> Result<Option<HeartbeatRow>> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let query = format!(
        "SELECT \
            toString(max(ts)) AS latest, \
            toUInt64(argMax(queue_depth, ts)) AS queue_depth, \
            toUInt64(argMax(files_active, ts)) AS files_active, \
            toString(argMax(watcher_backend, ts)) AS watcher_backend, \
            toUInt64(argMax(watcher_error_count, ts)) AS watcher_error_count, \
            toUInt64(argMax(watcher_reset_count, ts)) AS watcher_reset_count, \
            toUInt64(argMax(watcher_last_reset_unix_ms, ts)) AS watcher_last_reset_unix_ms \
         FROM {db}.ingest_heartbeats"
    );

    match ch.query_json_data::<HeartbeatRow>(&query, None).await {
        Ok(rows) => Ok(rows.into_iter().next()),
        Err(_) => {
            let legacy_query = format!(
                "SELECT toString(max(ts)) AS latest, toUInt64(argMax(queue_depth, ts)) AS queue_depth, toUInt64(argMax(files_active, ts)) AS files_active FROM {db}.ingest_heartbeats"
            );
            let rows: Vec<LegacyHeartbeatRow> = ch.query_json_data(&legacy_query, None).await?;
            Ok(rows.into_iter().next().map(|row| HeartbeatRow {
                latest: row.latest,
                queue_depth: row.queue_depth,
                files_active: row.files_active,
                watcher_backend: "unknown".to_string(),
                watcher_error_count: 0,
                watcher_reset_count: 0,
                watcher_last_reset_unix_ms: 0,
            }))
        }
    }
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn managed_clickhouse_version(paths: &RuntimePaths) -> Option<String> {
    fs::read_to_string(paths.managed_clickhouse_dir.join("VERSION"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn managed_clickhouse_checksum(paths: &RuntimePaths) -> Option<String> {
    fs::read_to_string(managed_clickhouse_checksum_file(paths))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn managed_clickhouse_checksum_state(cfg: &AppConfig, paths: &RuntimePaths) -> String {
    let Some(stored) = managed_clickhouse_checksum(paths) else {
        return "unknown (missing checksum metadata)".to_string();
    };

    let expected = match clickhouse_asset_for_host(&cfg.runtime.clickhouse_version) {
        Ok(asset) => asset.sha256,
        Err(exc) => return format!("unknown ({})", exc),
    };

    if stored == expected {
        "verified".to_string()
    } else {
        format!("mismatch (expected {}, got {})", expected, stored)
    }
}

fn active_clickhouse_source(paths: &RuntimePaths) -> (&'static str, Option<PathBuf>) {
    let managed = managed_clickhouse_bin(paths, "clickhouse-server");
    if managed.exists() {
        return ("managed", Some(managed));
    }
    if clickhouse_from_path_available() {
        return ("path", Some(PathBuf::from("clickhouse-server")));
    }
    ("missing", None)
}

async fn cmd_status(paths: &RuntimePaths, cfg: &AppConfig) -> Result<StatusSnapshot> {
    let services = [
        Service::ClickHouse,
        Service::Ingest,
        Service::Monitor,
        Service::Mcp,
    ]
    .iter()
    .copied()
    .map(|service| ServiceRuntimeStatus {
        service,
        pid: service_running(paths, service),
    })
    .collect::<Vec<_>>();
    let managed_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let (source, source_path) = active_clickhouse_source(paths);
    let report = cmd_db_doctor(cfg).await?;
    let heartbeat = match query_heartbeat(cfg).await {
        Ok(Some(row)) => HeartbeatSnapshot::Available {
            latest: row.latest,
            queue_depth: row.queue_depth,
            files_active: row.files_active,
            watcher_backend: row.watcher_backend,
            watcher_error_count: row.watcher_error_count,
            watcher_reset_count: row.watcher_reset_count,
            watcher_last_reset_unix_ms: row.watcher_last_reset_unix_ms,
        },
        Ok(None) => HeartbeatSnapshot::Unavailable,
        Err(err) => HeartbeatSnapshot::Error {
            message: err.to_string(),
        },
    };

    Ok(StatusSnapshot {
        services,
        managed_clickhouse_installed: managed_server.exists(),
        managed_clickhouse_path: managed_server.display().to_string(),
        managed_clickhouse_version: managed_clickhouse_version(paths),
        clickhouse_active_source: source.to_string(),
        clickhouse_active_source_path: source_path.map(|path| path.display().to_string()),
        managed_clickhouse_checksum: managed_clickhouse_checksum_state(cfg, paths),
        doctor: report,
        heartbeat,
    })
}

fn tail_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read log file {}", path.display()))?;
    let mut collected = content
        .lines()
        .rev()
        .take(lines)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    collected.reverse();
    Ok(collected)
}

fn collect_logs(
    paths: &RuntimePaths,
    service: Option<Service>,
    lines: usize,
) -> Result<LogsSnapshot> {
    let targets = match service {
        Some(svc) => vec![svc],
        None => vec![
            Service::ClickHouse,
            Service::Ingest,
            Service::Monitor,
            Service::Mcp,
        ],
    };

    let mut sections = Vec::new();
    for svc in targets {
        let path = log_path(paths, svc);
        let path_string = path.display().to_string();
        if !path.exists() {
            sections.push(ServiceLogSection {
                service: svc,
                path: path_string,
                exists: false,
                lines: Vec::new(),
            });
            continue;
        }
        sections.push(ServiceLogSection {
            service: svc,
            path: path_string,
            exists: true,
            lines: tail_lines(&path, lines)?,
        });
    }

    Ok(LogsSnapshot {
        requested_lines: lines,
        sections,
    })
}

fn current_exe_path() -> Result<PathBuf> {
    std::env::current_exe().context("failed to resolve current executable")
}

fn selected_boot_services(cfg: &AppConfig) -> Vec<Service> {
    let mut services = vec![Service::ClickHouse, Service::Ingest];
    if cfg.runtime.start_monitor_on_up {
        services.push(Service::Monitor);
    }
    if cfg.runtime.start_mcp_on_up {
        services.push(Service::Mcp);
    }
    services
}

fn shell_escape_single(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn uid_string() -> Result<String> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context("failed to run id -u")?;
    if !output.status.success() {
        bail!("id -u failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn launch_agents_dir() -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join("Library").join("LaunchAgents"))
}

fn summarize_command_output(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let candidate = if !stderr.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };

    if candidate.is_empty() {
        if let Some(code) = output.status.code() {
            format!("exit code {}", code)
        } else {
            "terminated by signal".to_string()
        }
    } else {
        candidate
            .lines()
            .next()
            .unwrap_or(candidate)
            .trim()
            .to_string()
    }
}

fn launchctl_output(args: &[String]) -> Result<Output> {
    Command::new("launchctl")
        .args(args)
        .output()
        .with_context(|| format!("failed launchctl {}", args.join(" ")))
}

fn launchctl_list_loaded(label: &str) -> bool {
    launchctl_output(&["list".to_string(), label.to_string()])
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn launchd_plist(
    service: Service,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    cfg_path: &Path,
    cortexctl_exe: &Path,
) -> String {
    let mut args = vec![
        cortexctl_exe.to_string_lossy().to_string(),
        "run".to_string(),
        service.name().to_string(),
        "--config".to_string(),
        cfg_path.to_string_lossy().to_string(),
    ];

    if service == Service::Monitor {
        args.push("--host".to_string());
        args.push(cfg.monitor.host.clone());
        args.push("--port".to_string());
        args.push(cfg.monitor.port.to_string());
    }

    let args_xml = args
        .iter()
        .map(|arg| format!("    <string>{}</string>", shell_escape_single(arg)))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{args_xml}
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>CORTEX_CONFIG</key>
    <string>{config}</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>{cwd}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{stdout}</string>
  <key>StandardErrorPath</key>
  <string>{stderr}</string>
</dict>
</plist>
",
        label = service.launchd_label(),
        args_xml = args_xml,
        config = shell_escape_single(&cfg_path.to_string_lossy()),
        cwd = shell_escape_single(&cfg.runtime.root_dir),
        stdout = shell_escape_single(&paths.logs_dir.join(format!("{}.out.log", service.name())).to_string_lossy()),
        stderr = shell_escape_single(&paths.logs_dir.join(format!("{}.err.log", service.name())).to_string_lossy()),
    )
}

fn install_launchd_services(
    cfg: &AppConfig,
    paths: &RuntimePaths,
    cfg_path: &Path,
    enable: bool,
    start: bool,
) -> Result<ServiceInstallSnapshot> {
    let services = selected_boot_services(cfg);
    let agents_dir = launch_agents_dir()?;
    fs::create_dir_all(&agents_dir)?;
    let domain = format!("gui/{}", uid_string()?);
    let cortexctl_exe = current_exe_path()?;

    for service in &services {
        let service = *service;
        let plist_path = agents_dir.join(format!("{}.plist", service.launchd_label()));
        let plist = launchd_plist(service, cfg, paths, cfg_path, &cortexctl_exe);
        fs::write(&plist_path, plist)?;

        let label_domain = format!("{}/{}", domain, service.launchd_label());

        let _ = launchctl_output(&["bootout".to_string(), label_domain.clone()]);

        let status = launchctl_output(&[
            "bootstrap".to_string(),
            domain.clone(),
            plist_path.to_string_lossy().to_string(),
        ])
        .with_context(|| format!("failed launchctl bootstrap for {}", service.name()))?;
        if !status.status.success() {
            let load_status = launchctl_output(&[
                "load".to_string(),
                "-w".to_string(),
                plist_path.to_string_lossy().to_string(),
            ])
            .with_context(|| format!("failed launchctl load for {}", service.name()))?;
            if !load_status.status.success() {
                bail!(
                    "launchctl bootstrap/load failed for {} (bootstrap: {}, load: {})",
                    service.name(),
                    summarize_command_output(&status),
                    summarize_command_output(&load_status)
                );
            }
        }

        if enable {
            let output = launchctl_output(&["enable".to_string(), label_domain.clone()])?;
            if !output.status.success() {
                bail!(
                    "launchctl enable failed for {} ({})",
                    service.name(),
                    summarize_command_output(&output)
                );
            }
        }
        if start {
            let output = launchctl_output(&[
                "kickstart".to_string(),
                "-k".to_string(),
                label_domain.clone(),
            ])?;
            if !output.status.success() {
                bail!(
                    "launchctl kickstart failed for {} ({})",
                    service.name(),
                    summarize_command_output(&output)
                );
            }
        }

        if !launchctl_list_loaded(service.launchd_label()) {
            bail!(
                "launchd service install did not load {} (label {})",
                service.name(),
                service.launchd_label()
            );
        }
    }

    Ok(ServiceInstallSnapshot {
        platform: "macos-launchd".to_string(),
        install_dir: agents_dir.display().to_string(),
        services,
        enable,
        start,
        notes: vec![],
    })
}

fn uninstall_launchd_services(
    cfg: &AppConfig,
    disable: bool,
    stop: bool,
) -> Result<ServiceUninstallSnapshot> {
    let services = selected_boot_services(cfg);
    let agents_dir = launch_agents_dir()?;
    let domain = format!("gui/{}", uid_string()?);

    for service in &services {
        let service = *service;
        if stop {
            let _ = Command::new("launchctl")
                .arg("bootout")
                .arg(format!("{}/{}", domain, service.launchd_label()))
                .status();
        }
        if disable {
            let _ = Command::new("launchctl")
                .arg("disable")
                .arg(format!("{}/{}", domain, service.launchd_label()))
                .status();
        }

        let plist_path = agents_dir.join(format!("{}.plist", service.launchd_label()));
        let _ = fs::remove_file(plist_path);
    }

    Ok(ServiceUninstallSnapshot {
        platform: "macos-launchd".to_string(),
        disable,
        stop,
        services,
        notes: vec![format!(
            "removed launchd entries under {}",
            agents_dir.display()
        )],
    })
}

fn status_launchd_services(cfg: &AppConfig) -> Result<ServiceStatusSnapshot> {
    let services = selected_boot_services(cfg);
    let agents_dir = launch_agents_dir()?;

    let mut rows = Vec::new();
    for service in &services {
        let service = *service;
        let plist_path = agents_dir.join(format!("{}.plist", service.launchd_label()));
        let installed = plist_path.exists();
        let loaded = launchctl_list_loaded(service.launchd_label());
        rows.push(ServiceStatusRow {
            service,
            installed: Some(installed),
            loaded: Some(loaded),
            enabled: None,
            active: None,
        });
    }

    Ok(ServiceStatusSnapshot {
        platform: "macos-launchd".to_string(),
        linger_user: None,
        linger_state: None,
        rows,
    })
}

fn systemd_user_dir() -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("systemd")
        .join("user"))
}

fn systemd_escape(value: &str) -> String {
    if value.contains([' ', '\t', '\n', '"', '\\']) {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value.to_string()
    }
}

fn systemd_unit_content(service: Service, cfg_path: &Path, cortexctl_exe: &Path) -> String {
    let exec_args = vec![
        systemd_escape(&cortexctl_exe.to_string_lossy()),
        "run".to_string(),
        service.name().to_string(),
        "--config".to_string(),
        systemd_escape(&cfg_path.to_string_lossy()),
    ];

    let exec = exec_args.join(" ");

    let deps = if service == Service::ClickHouse {
        String::new()
    } else {
        "After=cortex-clickhouse.service\nRequires=cortex-clickhouse.service\n".to_string()
    };

    format!(
        "[Unit]
Description=Cortex {name} service
{deps}
[Service]
Type=simple
ExecStart={exec}
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
",
        name = service.name(),
        deps = deps,
        exec = exec,
    )
}

fn current_username() -> Result<String> {
    if let Ok(user) = std::env::var("USER") {
        if !user.trim().is_empty() {
            return Ok(user);
        }
    }

    let output = Command::new("id")
        .arg("-un")
        .output()
        .context("failed to resolve current user via id -un")?;
    if !output.status.success() {
        bail!("id -un failed");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn loginctl_linger_state(user: &str) -> Result<Option<bool>> {
    let output = match Command::new("loginctl")
        .arg("show-user")
        .arg(user)
        .arg("--property=Linger")
        .arg("--value")
        .output()
    {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };

    if !output.status.success() {
        return Ok(None);
    }

    let value = String::from_utf8_lossy(&output.stdout)
        .trim()
        .to_ascii_lowercase();
    Ok(match value.as_str() {
        "yes" | "true" => Some(true),
        "no" | "false" => Some(false),
        _ => None,
    })
}

fn ensure_systemd_linger() -> Result<Vec<String>> {
    let user = current_username()?;
    let mut notes = Vec::new();
    match loginctl_linger_state(&user)? {
        Some(true) => {
            notes.push(format!("systemd linger enabled for user {user}"));
        }
        Some(false) => {
            let output = Command::new("loginctl")
                .arg("enable-linger")
                .arg(&user)
                .output();
            match output {
                Ok(output) if output.status.success() => {
                    notes.push(format!("systemd linger enabled for user {user}"));
                }
                Ok(output) => {
                    notes.push(format!(
                        "warning: failed to enable systemd linger for {} ({}). Run: sudo loginctl enable-linger {}",
                        user,
                        summarize_command_output(&output),
                        user
                    ));
                }
                Err(exc) => {
                    notes.push(format!(
                        "warning: failed to run loginctl enable-linger for {} ({}). Run: sudo loginctl enable-linger {}",
                        user,
                        exc,
                        user
                    ));
                }
            }
        }
        None => {
            notes.push(format!(
                "warning: unable to query systemd linger state. For pre-login service startup run: sudo loginctl enable-linger {}",
                user
            ));
        }
    }

    Ok(notes)
}

fn systemctl_user(args: &[&str]) -> Result<()> {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .with_context(|| format!("failed to run systemctl --user {}", args.join(" ")))?;

    if !output.status.success() {
        bail!(
            "systemctl --user {} failed ({})",
            args.join(" "),
            summarize_command_output(&output)
        );
    }

    Ok(())
}

fn install_systemd_services(
    cfg: &AppConfig,
    cfg_path: &Path,
    enable: bool,
    start: bool,
) -> Result<ServiceInstallSnapshot> {
    let services = selected_boot_services(cfg);
    let unit_dir = systemd_user_dir()?;
    fs::create_dir_all(&unit_dir)?;
    let cortexctl_exe = current_exe_path()?;

    for service in &services {
        let unit = systemd_unit_content(*service, cfg_path, &cortexctl_exe);
        fs::write(unit_dir.join(service.systemd_unit()), unit)?;
    }

    let notes = ensure_systemd_linger()?;
    systemctl_user(&["daemon-reload"])?;

    for service in &services {
        if enable {
            systemctl_user(&["enable", service.systemd_unit()])?;
        }
        if start {
            systemctl_user(&["start", service.systemd_unit()])?;
        }
    }

    Ok(ServiceInstallSnapshot {
        platform: "linux-systemd-user".to_string(),
        install_dir: unit_dir.display().to_string(),
        services,
        enable,
        start,
        notes,
    })
}

fn uninstall_systemd_services(
    cfg: &AppConfig,
    disable: bool,
    stop: bool,
) -> Result<ServiceUninstallSnapshot> {
    let services = selected_boot_services(cfg);
    for service in &services {
        if stop {
            let _ = systemctl_user(&["stop", service.systemd_unit()]);
        }
        if disable {
            let _ = systemctl_user(&["disable", service.systemd_unit()]);
        }
    }

    let unit_dir = systemd_user_dir()?;
    for service in &services {
        let _ = fs::remove_file(unit_dir.join(service.systemd_unit()));
    }

    let _ = systemctl_user(&["daemon-reload"]);

    Ok(ServiceUninstallSnapshot {
        platform: "linux-systemd-user".to_string(),
        disable,
        stop,
        services,
        notes: vec![format!("removed units from {}", unit_dir.display())],
    })
}

fn status_systemd_services(cfg: &AppConfig) -> Result<ServiceStatusSnapshot> {
    let services = selected_boot_services(cfg);
    let mut linger_user = None;
    let mut linger_state = None;
    if let Ok(user) = current_username() {
        linger_user = Some(user.clone());
        linger_state = match loginctl_linger_state(&user) {
            Ok(Some(enabled)) => Some(enabled.to_string()),
            Ok(None) => Some("unknown".to_string()),
            Err(exc) => Some(format!("error ({exc})")),
        };
    }

    let mut rows = Vec::new();
    for service in &services {
        let enabled = Command::new("systemctl")
            .arg("--user")
            .arg("is-enabled")
            .arg(service.systemd_unit())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        let active = Command::new("systemctl")
            .arg("--user")
            .arg("is-active")
            .arg(service.systemd_unit())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        rows.push(ServiceStatusRow {
            service: *service,
            installed: None,
            loaded: None,
            enabled: Some(enabled),
            active: Some(active),
        });
    }

    Ok(ServiceStatusSnapshot {
        platform: "linux-systemd-user".to_string(),
        linger_user,
        linger_state,
        rows,
    })
}

async fn cmd_service_install(
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    enable: bool,
    start: bool,
) -> Result<ServiceInstallSnapshot> {
    ensure_runtime_dirs(paths)?;
    let _ = resolve_clickhouse_server_command(cfg, paths).await?;

    match std::env::consts::OS {
        "macos" => install_launchd_services(cfg, paths, cfg_path, enable, start),
        "linux" => install_systemd_services(cfg, cfg_path, enable, start),
        other => bail!("unsupported OS for service install: {}", other),
    }
}

fn cmd_service_uninstall(
    cfg: &AppConfig,
    disable: bool,
    stop: bool,
) -> Result<ServiceUninstallSnapshot> {
    match std::env::consts::OS {
        "macos" => uninstall_launchd_services(cfg, disable, stop),
        "linux" => uninstall_systemd_services(cfg, disable, stop),
        other => bail!("unsupported OS for service uninstall: {}", other),
    }
}

fn cmd_service_status(cfg: &AppConfig) -> Result<ServiceStatusSnapshot> {
    match std::env::consts::OS {
        "macos" => status_launchd_services(cfg),
        "linux" => status_systemd_services(cfg),
        other => bail!("unsupported OS for service status: {}", other),
    }
}

async fn cmd_clickhouse_install(
    paths: &RuntimePaths,
    version: &str,
    force: bool,
) -> Result<PathBuf> {
    ensure_runtime_dirs(paths)?;
    let installed = install_managed_clickhouse(paths, version, force).await?;
    Ok(installed)
}

fn cmd_clickhouse_status(cfg: &AppConfig, paths: &RuntimePaths) -> ClickhouseStatusSnapshot {
    let clickhouse = managed_clickhouse_bin(paths, "clickhouse");
    let clickhouse_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let clickhouse_client = managed_clickhouse_bin(paths, "clickhouse-client");
    let (active_source, active_source_path) = active_clickhouse_source(paths);

    ClickhouseStatusSnapshot {
        managed_root: paths.managed_clickhouse_dir.display().to_string(),
        clickhouse_exists: clickhouse.exists(),
        clickhouse_server_exists: clickhouse_server.exists(),
        clickhouse_client_exists: clickhouse_client.exists(),
        expected_version: cfg.runtime.clickhouse_version.clone(),
        active_source: active_source.to_string(),
        active_source_path: active_source_path.map(|path| path.display().to_string()),
        checksum_state: managed_clickhouse_checksum_state(cfg, paths),
        installed_version: managed_clickhouse_version(paths),
    }
}

fn cmd_clickhouse_uninstall(paths: &RuntimePaths) -> Result<String> {
    if paths.managed_clickhouse_dir.exists() {
        fs::remove_dir_all(&paths.managed_clickhouse_dir).with_context(|| {
            format!("failed removing {}", paths.managed_clickhouse_dir.display())
        })?;
    }

    Ok(paths.managed_clickhouse_dir.display().to_string())
}

fn health_label(value: bool) -> &'static str {
    if value {
        "healthy"
    } else {
        "unhealthy"
    }
}

fn state_label(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn format_start_state(outcome: &StartOutcome) -> String {
    match outcome.state {
        StartState::Started => "started".to_string(),
        StartState::AlreadyRunning => "already running".to_string(),
    }
}

fn render_status(output: &CliOutput, snapshot: &StatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    let service_rows = snapshot
        .services
        .iter()
        .map(|row| {
            vec![
                row.service.name().to_string(),
                if row.pid.is_some() {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                row.pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ]
        })
        .collect::<Vec<_>>();
    output.table("Services", &["service", "state", "pid"], &service_rows);

    let mut clickhouse_lines = vec![
        format!(
            "managed install: {}",
            if snapshot.managed_clickhouse_installed {
                "present"
            } else {
                "missing"
            }
        ),
        format!("managed binary: {}", snapshot.managed_clickhouse_path),
        format!(
            "active source: {}{}",
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
        clickhouse_lines.push(format!("managed version: {version}"));
    }
    output.section("ClickHouse Runtime", &clickhouse_lines);

    let mut doctor_lines = vec![
        format!(
            "clickhouse: {}",
            health_label(snapshot.doctor.clickhouse_healthy)
        ),
        format!(
            "database exists: {}",
            state_label(snapshot.doctor.database_exists)
        ),
        format!(
            "pending migrations: {}",
            if snapshot.doctor.pending_migrations.is_empty() {
                "none".to_string()
            } else {
                snapshot.doctor.pending_migrations.join(", ")
            }
        ),
        format!(
            "missing tables: {}",
            if snapshot.doctor.missing_tables.is_empty() {
                "none".to_string()
            } else {
                snapshot.doctor.missing_tables.join(", ")
            }
        ),
    ];
    if let Some(version) = &snapshot.doctor.clickhouse_version {
        doctor_lines.push(format!("server version: {version}"));
    }
    if output.verbose && !snapshot.doctor.errors.is_empty() {
        doctor_lines.push(format!("errors: {}", snapshot.doctor.errors.join(" | ")));
    }
    output.section("Database Health", &doctor_lines);

    let heartbeat_lines = match &snapshot.heartbeat {
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
                format!("latest: {latest}"),
                format!("queue depth: {queue_depth}"),
                format!("files active: {files_active}"),
                format!("watcher backend: {watcher_backend}"),
                format!("watcher errors: {watcher_error_count}"),
                format!("watcher resets: {watcher_reset_count}"),
            ];
            if output.verbose {
                lines.push(format!(
                    "watcher last reset unix ms: {watcher_last_reset_unix_ms}"
                ));
            }
            lines
        }
        HeartbeatSnapshot::Unavailable => vec!["heartbeat unavailable".to_string()],
        HeartbeatSnapshot::Error { message } => vec![format!("heartbeat error: {message}")],
    };
    output.section("Ingest Heartbeat", &heartbeat_lines);
    Ok(())
}

fn render_db_migrate(output: &CliOutput, outcome: &MigrationOutcome) -> Result<()> {
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

fn doctor_is_healthy(report: &DoctorReport) -> bool {
    report.clickhouse_healthy
        && report.database_exists
        && report.pending_migrations.is_empty()
        && report.missing_tables.is_empty()
        && report.errors.is_empty()
}

fn render_db_doctor(output: &CliOutput, report: &DoctorReport) -> Result<()> {
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
    if !report.errors.is_empty() {
        lines.push(format!("errors: {}", report.errors.join(" | ")));
    }
    output.section("DB Doctor", &lines);
    Ok(())
}

fn render_logs(output: &CliOutput, snapshot: &LogsSnapshot) -> Result<()> {
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

fn render_clickhouse_status(output: &CliOutput, snapshot: &ClickhouseStatusSnapshot) -> Result<()> {
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

fn render_service_install(output: &CliOutput, snapshot: &ServiceInstallSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![
        format!("platform: {}", snapshot.platform),
        format!("install dir: {}", snapshot.install_dir),
        format!(
            "services: {}",
            snapshot
                .services
                .iter()
                .map(|svc| svc.name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("enable: {}", state_label(snapshot.enable)),
        format!("start: {}", state_label(snapshot.start)),
    ];
    lines.extend(snapshot.notes.iter().cloned());
    output.section("Service Install", &lines);
    Ok(())
}

fn render_service_uninstall(output: &CliOutput, snapshot: &ServiceUninstallSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![
        format!("platform: {}", snapshot.platform),
        format!("disable: {}", state_label(snapshot.disable)),
        format!("stop: {}", state_label(snapshot.stop)),
        format!(
            "services: {}",
            snapshot
                .services
                .iter()
                .map(|svc| svc.name())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    lines.extend(snapshot.notes.iter().cloned());
    output.section("Service Uninstall", &lines);
    Ok(())
}

fn render_service_status(output: &CliOutput, snapshot: &ServiceStatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![format!("platform: {}", snapshot.platform)];
    if let (Some(user), Some(state)) = (&snapshot.linger_user, &snapshot.linger_state) {
        lines.push(format!("systemd linger ({user}): {state}"));
    }
    output.section("Service Manager", &lines);

    let headers = if snapshot.platform.starts_with("macos") {
        vec!["service", "installed", "loaded"]
    } else {
        vec!["service", "enabled", "active"]
    };
    let rows = snapshot
        .rows
        .iter()
        .map(|row| {
            if snapshot.platform.starts_with("macos") {
                vec![
                    row.service.name().to_string(),
                    row.installed.map(state_label).unwrap_or("-").to_string(),
                    row.loaded.map(state_label).unwrap_or("-").to_string(),
                ]
            } else {
                vec![
                    row.service.name().to_string(),
                    row.enabled.map(state_label).unwrap_or("-").to_string(),
                    row.active.map(state_label).unwrap_or("-").to_string(),
                ]
            }
        })
        .collect::<Vec<_>>();
    output.table("Services", &headers, &rows);
    Ok(())
}

fn render_up(output: &CliOutput, snapshot: &UpSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut rows = vec![vec![
        snapshot.clickhouse.service.name().to_string(),
        format_start_state(&snapshot.clickhouse),
        snapshot.clickhouse.pid.to_string(),
    ]];
    rows.extend(snapshot.services.iter().map(|outcome| {
        vec![
            outcome.service.name().to_string(),
            format_start_state(outcome),
            outcome.pid.to_string(),
        ]
    }));
    output.table("Startup Results", &["service", "result", "pid"], &rows);
    render_db_migrate(output, &snapshot.migrations)?;
    render_status(output, &snapshot.status)?;
    Ok(())
}

fn render_down(output: &CliOutput, snapshot: &DownSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    if snapshot.stopped.is_empty() {
        output.section("Shutdown", &["no running services found".to_string()]);
        return Ok(());
    }
    let rows = snapshot
        .stopped
        .iter()
        .map(|service| vec![service.name().to_string(), "stopped".to_string()])
        .collect::<Vec<_>>();
    output.table("Shutdown", &["service", "result"], &rows);
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<ExitCode> {
    let cli = Cli::parse();
    let output = CliOutput::from_cli(&cli);

    match cli.command {
        CliCommand::Up(args) => {
            let (config_path, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            ensure_runtime_dirs(&paths)?;

            let clickhouse = start_clickhouse(&cfg, &paths).await?;
            let migrations = cmd_db_migrate(&cfg).await?;

            let mut started_services = Vec::new();
            if !args.no_ingest {
                started_services.push(start_background_service(
                    Service::Ingest,
                    &config_path,
                    &cfg,
                    &paths,
                    &[],
                )?);
            }
            if args.monitor || cfg.runtime.start_monitor_on_up {
                started_services.push(start_background_service(
                    Service::Monitor,
                    &config_path,
                    &cfg,
                    &paths,
                    &[],
                )?);
            }
            if args.mcp || cfg.runtime.start_mcp_on_up {
                started_services.push(start_background_service(
                    Service::Mcp,
                    &config_path,
                    &cfg,
                    &paths,
                    &[],
                )?);
            }

            let status = cmd_status(&paths, &cfg).await?;
            let snapshot = UpSnapshot {
                clickhouse,
                migrations,
                services: started_services,
                status,
            };
            render_up(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Down => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let mut stopped = Vec::new();
            for service in [
                Service::Mcp,
                Service::Monitor,
                Service::Ingest,
                Service::ClickHouse,
            ] {
                if stop_service(&paths, service)? {
                    stopped.push(service);
                }
            }
            render_down(&output, &DownSnapshot { stopped })?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Status => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let snapshot = cmd_status(&paths, &cfg).await?;
            render_status(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Logs(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let snapshot = collect_logs(&paths, args.service, args.lines)?;
            render_logs(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
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
        CliCommand::Service(args) => {
            let (config_path, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            match args.command {
                ServiceCommand::Install(install) => {
                    let explicit = install.enable || install.start;
                    let enable = if explicit { install.enable } else { true };
                    let start = if explicit { install.start } else { true };
                    let snapshot =
                        cmd_service_install(&config_path, &cfg, &paths, enable, start).await?;
                    render_service_install(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                ServiceCommand::Uninstall(uninstall) => {
                    let explicit = uninstall.disable || uninstall.stop;
                    let disable = if explicit { uninstall.disable } else { true };
                    let stop = if explicit { uninstall.stop } else { true };
                    let snapshot = cmd_service_uninstall(&cfg, disable, stop)?;
                    render_service_uninstall(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                ServiceCommand::Status => {
                    let snapshot = cmd_service_status(&cfg)?;
                    render_service_status(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Run(run) => {
            let (inline_config, passthrough) = parse_config_flag(&run.args)?;
            let raw_config = inline_config.or(cli.config.clone());
            let (config_path, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            run_foreground_service(run.service, &config_path, &cfg, &paths, &passthrough).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("cortexctl-{name}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn write_file(path: &Path) {
        fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
        fs::write(path, b"#!/bin/sh\n").expect("write file");
    }

    #[test]
    fn clickhouse_asset_selection_covers_target_matrix() {
        let linux_x64 =
            clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "x86_64-unknown-linux-gnu")
                .expect("linux x64");
        assert_eq!(linux_x64.url, CH_URL_LINUX_X86_64);
        assert!(linux_x64.is_archive);

        let linux_arm =
            clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "aarch64-unknown-linux-gnu")
                .expect("linux arm");
        assert_eq!(linux_arm.url, CH_URL_LINUX_AARCH64);
        assert!(linux_arm.is_archive);

        let mac_x64 = clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "x86_64-apple-darwin")
            .expect("mac x64");
        assert_eq!(mac_x64.url, CH_URL_MACOS_X86_64);
        assert!(!mac_x64.is_archive);

        let mac_arm = clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "aarch64-apple-darwin")
            .expect("mac arm");
        assert_eq!(mac_arm.url, CH_URL_MACOS_AARCH64);
        assert!(!mac_arm.is_archive);
    }

    #[test]
    fn clickhouse_asset_rejects_unsupported_version() {
        let err = clickhouse_asset_for_target("v0.0.0", "x86_64-unknown-linux-gnu")
            .expect_err("unsupported version");
        assert!(
            err.to_string().contains("unsupported ClickHouse version"),
            "{}",
            err
        );
    }

    #[test]
    fn clap_parses_clickhouse_install_flags() {
        let cli = Cli::parse_from([
            "cortexctl",
            "clickhouse",
            "install",
            "--version",
            "v25.12.5.44-stable",
            "--force",
        ]);
        match cli.command {
            CliCommand::Clickhouse(ClickhouseArgs {
                command: ClickhouseCommand::Install(install),
            }) => {
                assert!(install.force);
                assert_eq!(install.version.as_deref(), Some("v25.12.5.44-stable"));
            }
            _ => panic!("expected clickhouse install command"),
        }
    }

    #[test]
    fn clap_parses_run_passthrough_args() {
        let cli = Cli::parse_from([
            "cortexctl",
            "--output",
            "plain",
            "run",
            "mcp",
            "--",
            "--stdio",
            "--transport",
            "jsonrpc",
        ]);
        match cli.command {
            CliCommand::Run(run) => {
                assert_eq!(run.service, Service::Mcp);
                assert_eq!(
                    run.args,
                    vec![
                        "--stdio".to_string(),
                        "--transport".to_string(),
                        "jsonrpc".to_string(),
                    ]
                );
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn output_mode_respects_json_flag() {
        let cli = Cli::parse_from(["cortexctl", "--output", "json", "status"]);
        let output = CliOutput::from_cli(&cli);
        assert_eq!(output.mode, OutputMode::Json);
    }

    #[test]
    fn clickhouse_ports_follow_url_port() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "http://127.0.0.1:18123".to_string();
        let ports = clickhouse_ports_from_url(&cfg).expect("ports");
        assert_eq!(ports, (18123, 19000, 19009));
    }

    #[test]
    fn clickhouse_ports_require_valid_url() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "not-a-url".to_string();
        let err = clickhouse_ports_from_url(&cfg).expect_err("invalid url");
        assert!(err.to_string().contains("invalid clickhouse.url"));
    }

    #[test]
    fn resolve_service_binary_prefers_env_then_config() {
        let root = temp_dir("resolver");
        let env_dir = root.join("env");
        let cfg_dir = root.join("cfg");
        let env_bin = env_dir.join("cortex-ingest");
        let cfg_bin = cfg_dir.join("cortex-ingest");
        write_file(&env_bin);
        write_file(&cfg_bin);

        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = cfg_dir.to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        std::env::remove_var("CORTEX_SOURCE_TREE_MODE");
        std::env::set_var("CORTEX_SERVICE_BIN_DIR", &env_dir);
        assert_eq!(resolve_service_binary(Service::Ingest, &paths), env_bin);

        std::env::remove_var("CORTEX_SERVICE_BIN_DIR");
        assert_eq!(resolve_service_binary(Service::Ingest, &paths), cfg_bin);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_service_binary_falls_back_to_path_name() {
        let root = temp_dir("resolver-path");
        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = root.join("missing").to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        std::env::remove_var("CORTEX_SERVICE_BIN_DIR");
        std::env::remove_var("CORTEX_SOURCE_TREE_MODE");
        let resolved = resolve_service_binary(Service::Mcp, &paths);
        assert_eq!(resolved, PathBuf::from("cortex-mcp"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn systemd_unit_content_has_expected_dependencies() {
        let cfg_path = PathBuf::from("/tmp/cortex.toml");
        let cortexctl = PathBuf::from("/tmp/cortexctl");

        let clickhouse = systemd_unit_content(Service::ClickHouse, &cfg_path, &cortexctl);
        assert!(clickhouse
            .contains("ExecStart=/tmp/cortexctl run clickhouse --config /tmp/cortex.toml"));
        assert!(!clickhouse.contains("After=cortex-clickhouse.service"));

        let ingest = systemd_unit_content(Service::Ingest, &cfg_path, &cortexctl);
        assert!(ingest.contains("After=cortex-clickhouse.service"));
        assert!(ingest.contains("Requires=cortex-clickhouse.service"));
    }

    #[test]
    fn launchd_plist_contains_label_and_program_args() {
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = "/tmp/cortex".to_string();
        cfg.runtime.logs_dir = "/tmp/cortex/logs".to_string();
        let paths = runtime_paths(&cfg);
        let plist = launchd_plist(
            Service::Monitor,
            &cfg,
            &paths,
            Path::new("/tmp/cortex.toml"),
            Path::new("/tmp/cortexctl"),
        );

        assert!(plist.contains("<string>com.eric.cortex.monitor</string>"));
        assert!(plist.contains("<string>/tmp/cortexctl</string>"));
        assert!(plist.contains("<string>run</string>"));
        assert!(plist.contains("<string>monitor</string>"));
        assert!(plist.contains("<string>--config</string>"));
        assert!(plist.contains("<string>/tmp/cortex.toml</string>"));
    }

    #[test]
    fn managed_checksum_state_reports_verified() {
        let root = temp_dir("checksum");
        let managed_dir = root.join("managed");
        fs::create_dir_all(&managed_dir).expect("managed dir");

        let mut cfg = AppConfig::default();
        cfg.runtime.managed_clickhouse_dir = managed_dir.to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        let expected = clickhouse_asset_for_host(&cfg.runtime.clickhouse_version)
            .expect("host asset")
            .sha256;
        fs::write(
            managed_clickhouse_checksum_file(&paths),
            format!("{expected}\n"),
        )
        .expect("write checksum");

        assert_eq!(managed_clickhouse_checksum_state(&cfg, &paths), "verified");
        let _ = fs::remove_dir_all(root);
    }
}
