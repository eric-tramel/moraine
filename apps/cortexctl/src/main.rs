use anyhow::{anyhow, bail, Context, Result};
use cortex_clickhouse::{ClickHouseClient, DoctorReport};
use cortex_config::AppConfig;
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Service {
    ClickHouse,
    Ingest,
    Monitor,
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
}

#[derive(Clone, Copy)]
struct ClickHouseAsset {
    url: &'static str,
    sha256: &'static str,
    is_archive: bool,
}

fn usage() {
    eprintln!(
        "usage:
  cortexctl up [--config <path>] [--no-ingest] [--monitor] [--mcp]
  cortexctl down [--config <path>]
  cortexctl status [--config <path>]
  cortexctl logs [service] [--lines <n>] [--config <path>]
  cortexctl db migrate [--config <path>]
  cortexctl db doctor [--config <path>]
  cortexctl clickhouse install [--force] [--config <path>]
  cortexctl clickhouse status [--config <path>]
  cortexctl clickhouse uninstall [--config <path>]
  cortexctl service install [--enable] [--start] [--config <path>]
  cortexctl service uninstall [--disable] [--stop] [--config <path>]
  cortexctl service status [--config <path>]
  cortexctl run clickhouse|ingest|monitor|mcp [--config <path>] [args...]"
    );
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

fn materialize_clickhouse_config(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    let rendered_clickhouse = CLICKHOUSE_TEMPLATE.replace("__CORTEX_HOME__", &cfg.runtime.root_dir);
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

fn clickhouse_asset(cfg: &AppConfig) -> Result<ClickHouseAsset> {
    if cfg.runtime.clickhouse_version != DEFAULT_CLICKHOUSE_TAG {
        bail!(
            "unsupported runtime.clickhouse_version {}; this build supports {}",
            cfg.runtime.clickhouse_version,
            DEFAULT_CLICKHOUSE_TAG
        );
    }

    match detect_host_target()? {
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
        other => bail!("unsupported target: {}", other),
    }
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
    cfg: &AppConfig,
    paths: &RuntimePaths,
    force: bool,
) -> Result<PathBuf> {
    let asset = clickhouse_asset(cfg)?;

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
        format!("{}\n", cfg.runtime.clickhouse_version),
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
        install_managed_clickhouse(cfg, paths, false).await?;
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

async fn start_clickhouse(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    if let Some(pid) = service_running(paths, Service::ClickHouse) {
        println!("clickhouse already running (pid {})", pid);
        return Ok(());
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
    Ok(())
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

fn resolve_service_binary(service: Service, cfg: &AppConfig, paths: &RuntimePaths) -> PathBuf {
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

    if let Some(project_bin) = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("target").join("debug").join(name))
    {
        if project_bin.exists() {
            return project_bin;
        }
    }

    let _ = cfg;
    PathBuf::from(name)
}

fn contains_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
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
                let candidate = bundle_root.join("web").join("monitor");
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
    }

    if let Some(bundle_root) = paths.service_bin_dir.parent() {
        let candidate = bundle_root.join("web").join("monitor");
        if candidate.exists() {
            return Some(candidate);
        }
    }

    if let Some(dev_path) = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("web").join("monitor"))
    {
        if dev_path.exists() {
            return Some(dev_path);
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
) -> Result<()> {
    if service == Service::ClickHouse {
        bail!("clickhouse is not managed by service launcher; use `cortexctl up`");
    }

    if let Some(pid) = service_running(paths, service) {
        println!("{} already running (pid {})", service.name(), pid);
        return Ok(());
    }

    let binary = resolve_service_binary(service, cfg, paths);

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
    println!(
        "{} started (pid {}) log={} ",
        service.name(),
        child.id(),
        log_path(paths, service).display()
    );

    Ok(())
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

    let binary = resolve_service_binary(service, cfg, paths);
    let args = service_args_with_defaults(service, cfg_path, cfg, paths, passthrough_args);

    let status = Command::new(binary)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {}", service.name()))?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

async fn cmd_db_migrate(cfg: &AppConfig) -> Result<()> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let applied = ch.run_migrations().await?;
    if applied.is_empty() {
        println!("migrations already up to date");
    } else {
        println!("applied migrations: {}", applied.join(", "));
    }
    Ok(())
}

async fn cmd_db_doctor(cfg: &AppConfig) -> Result<DoctorReport> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ch.doctor_report().await
}

async fn query_heartbeat(cfg: &AppConfig) -> Result<Option<HeartbeatRow>> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let query = format!(
        "SELECT toString(max(ts)) AS latest, toUInt64(argMax(queue_depth, ts)) AS queue_depth, toUInt64(argMax(files_active, ts)) AS files_active FROM {db}.ingest_heartbeats"
    );

    let rows: Vec<HeartbeatRow> = ch.query_json_data(&query, None).await?;
    Ok(rows.into_iter().next())
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

async fn cmd_status(paths: &RuntimePaths, cfg: &AppConfig) -> Result<()> {
    for service in [
        Service::ClickHouse,
        Service::Ingest,
        Service::Monitor,
        Service::Mcp,
    ] {
        match service_running(paths, service) {
            Some(pid) => println!("{}: running (pid {})", service.name(), pid),
            None => println!("{}: stopped", service.name()),
        }
    }

    let managed_server = managed_clickhouse_bin(paths, "clickhouse-server");
    println!(
        "managed clickhouse: {} ({})",
        if managed_server.exists() {
            "installed"
        } else {
            "missing"
        },
        managed_server.display()
    );
    if let Some(version) = managed_clickhouse_version(paths) {
        println!("managed clickhouse version: {}", version);
    }

    let report = cmd_db_doctor(cfg).await?;
    println!("clickhouse healthy: {}", report.clickhouse_healthy);
    if let Some(version) = report.clickhouse_version {
        println!("clickhouse version: {}", version);
    }
    println!("database exists: {}", report.database_exists);
    println!(
        "pending migrations: {}",
        report.pending_migrations.join(",")
    );
    println!("missing tables: {}", report.missing_tables.join(","));
    if !report.errors.is_empty() {
        println!("doctor errors: {}", report.errors.join(" | "));
    }

    match query_heartbeat(cfg).await {
        Ok(Some(row)) => {
            println!("ingest heartbeat latest: {}", row.latest);
            println!("ingest queue depth: {}", row.queue_depth);
            println!("ingest files active: {}", row.files_active);
        }
        Ok(None) => println!("ingest heartbeat: unavailable"),
        Err(err) => println!("ingest heartbeat error: {}", err),
    }

    Ok(())
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

fn print_logs(paths: &RuntimePaths, service: Option<Service>, lines: usize) -> Result<()> {
    let targets = match service {
        Some(svc) => vec![svc],
        None => vec![
            Service::ClickHouse,
            Service::Ingest,
            Service::Monitor,
            Service::Mcp,
        ],
    };

    for svc in targets {
        let path = log_path(paths, svc);
        println!("== {} ({}) ==", svc.name(), path.display());
        if !path.exists() {
            println!("<no log file>");
            continue;
        }

        for line in tail_lines(&path, lines)? {
            println!("{}", line);
        }
    }

    Ok(())
}

fn parse_service(name: &str) -> Option<Service> {
    match name {
        "clickhouse" => Some(Service::ClickHouse),
        "ingest" => Some(Service::Ingest),
        "monitor" => Some(Service::Monitor),
        "mcp" => Some(Service::Mcp),
        _ => None,
    }
}

fn parse_logs_args(args: &[String]) -> Result<(Option<Service>, usize, Option<PathBuf>)> {
    let mut service = None;
    let mut lines = 200usize;

    let mut i = 0usize;
    let mut raw_config = None;
    while i < args.len() {
        match args[i].as_str() {
            "--lines" => {
                if i + 1 >= args.len() {
                    bail!("--lines requires a number");
                }
                lines = args[i + 1]
                    .parse::<usize>()
                    .map_err(|e| anyhow!("invalid --lines value: {e}"))?;
                i += 2;
            }
            "--config" => {
                if i + 1 >= args.len() {
                    bail!("--config requires a path");
                }
                raw_config = Some(PathBuf::from(args[i + 1].clone()));
                i += 2;
            }
            other => {
                if service.is_none() {
                    service = parse_service(other);
                    if service.is_none() {
                        bail!("unknown service: {}", other);
                    }
                } else {
                    bail!("unexpected argument: {}", other);
                }
                i += 1;
            }
        }
    }

    Ok((service, lines, raw_config))
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
) -> Result<()> {
    let services = selected_boot_services(cfg);
    let agents_dir = launch_agents_dir()?;
    fs::create_dir_all(&agents_dir)?;
    let domain = format!("gui/{}", uid_string()?);
    let cortexctl_exe = current_exe_path()?;

    for service in services {
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

    println!("launchd services installed in {}", agents_dir.display());
    Ok(())
}

fn uninstall_launchd_services(cfg: &AppConfig, disable: bool, stop: bool) -> Result<()> {
    let services = selected_boot_services(cfg);
    let agents_dir = launch_agents_dir()?;
    let domain = format!("gui/{}", uid_string()?);

    for service in services {
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

    println!("launchd service entries removed");
    Ok(())
}

fn status_launchd_services(cfg: &AppConfig) -> Result<()> {
    let services = selected_boot_services(cfg);
    let agents_dir = launch_agents_dir()?;

    for service in services {
        let plist_path = agents_dir.join(format!("{}.plist", service.launchd_label()));
        let installed = plist_path.exists();
        let loaded = launchctl_list_loaded(service.launchd_label());

        println!(
            "{}: installed={} loaded={}",
            service.name(),
            installed,
            loaded
        );
    }

    Ok(())
}

fn systemd_user_dir() -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("systemd")
        .join("user"))
}

fn systemd_unit_content(service: Service, cfg_path: &Path, cortexctl_exe: &Path) -> String {
    let exec_args = vec![
        cortexctl_exe.to_string_lossy().to_string(),
        "run".to_string(),
        service.name().to_string(),
        "--config".to_string(),
        cfg_path.to_string_lossy().to_string(),
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
) -> Result<()> {
    let services = selected_boot_services(cfg);
    let unit_dir = systemd_user_dir()?;
    fs::create_dir_all(&unit_dir)?;
    let cortexctl_exe = current_exe_path()?;

    for service in &services {
        let unit = systemd_unit_content(*service, cfg_path, &cortexctl_exe);
        fs::write(unit_dir.join(service.systemd_unit()), unit)?;
    }

    systemctl_user(&["daemon-reload"])?;

    for service in &services {
        if enable {
            systemctl_user(&["enable", service.systemd_unit()])?;
        }
        if start {
            systemctl_user(&["start", service.systemd_unit()])?;
        }
    }

    println!("systemd user services installed in {}", unit_dir.display());
    Ok(())
}

fn uninstall_systemd_services(cfg: &AppConfig, disable: bool, stop: bool) -> Result<()> {
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

    println!("systemd user services removed");
    Ok(())
}

fn status_systemd_services(cfg: &AppConfig) -> Result<()> {
    let services = selected_boot_services(cfg);

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

        println!("{}: enabled={} active={}", service.name(), enabled, active);
    }

    Ok(())
}

async fn cmd_service_install(
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    enable: bool,
    start: bool,
) -> Result<()> {
    ensure_runtime_dirs(paths)?;
    let _ = resolve_clickhouse_server_command(cfg, paths).await?;

    match std::env::consts::OS {
        "macos" => install_launchd_services(cfg, paths, cfg_path, enable, start),
        "linux" => install_systemd_services(cfg, cfg_path, enable, start),
        other => bail!("unsupported OS for service install: {}", other),
    }
}

fn cmd_service_uninstall(cfg: &AppConfig, disable: bool, stop: bool) -> Result<()> {
    match std::env::consts::OS {
        "macos" => uninstall_launchd_services(cfg, disable, stop),
        "linux" => uninstall_systemd_services(cfg, disable, stop),
        other => bail!("unsupported OS for service uninstall: {}", other),
    }
}

fn cmd_service_status(cfg: &AppConfig) -> Result<()> {
    match std::env::consts::OS {
        "macos" => status_launchd_services(cfg),
        "linux" => status_systemd_services(cfg),
        other => bail!("unsupported OS for service status: {}", other),
    }
}

fn parse_service_install_flags(args: &[String]) -> Result<(bool, bool)> {
    let mut enable = false;
    let mut start = false;
    let mut explicit = false;

    for arg in args {
        match arg.as_str() {
            "--enable" => {
                explicit = true;
                enable = true;
            }
            "--start" => {
                explicit = true;
                start = true;
            }
            _ => bail!("unexpected service install arg: {}", arg),
        }
    }

    if !explicit {
        enable = true;
        start = true;
    }

    Ok((enable, start))
}

fn parse_service_uninstall_flags(args: &[String]) -> Result<(bool, bool)> {
    let mut disable = false;
    let mut stop = false;
    let mut explicit = false;

    for arg in args {
        match arg.as_str() {
            "--disable" => {
                explicit = true;
                disable = true;
            }
            "--stop" => {
                explicit = true;
                stop = true;
            }
            _ => bail!("unexpected service uninstall arg: {}", arg),
        }
    }

    if !explicit {
        disable = true;
        stop = true;
    }

    Ok((disable, stop))
}

async fn cmd_clickhouse_install(cfg: &AppConfig, paths: &RuntimePaths, force: bool) -> Result<()> {
    ensure_runtime_dirs(paths)?;
    let installed = install_managed_clickhouse(cfg, paths, force).await?;
    println!("managed clickhouse installed: {}", installed.display());
    Ok(())
}

fn cmd_clickhouse_status(paths: &RuntimePaths) {
    let clickhouse = managed_clickhouse_bin(paths, "clickhouse");
    let clickhouse_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let clickhouse_client = managed_clickhouse_bin(paths, "clickhouse-client");

    println!("managed root: {}", paths.managed_clickhouse_dir.display());
    println!("clickhouse: {}", clickhouse.exists());
    println!("clickhouse-server: {}", clickhouse_server.exists());
    println!("clickhouse-client: {}", clickhouse_client.exists());

    if let Some(version) = managed_clickhouse_version(paths) {
        println!("version: {}", version);
    }
}

fn cmd_clickhouse_uninstall(paths: &RuntimePaths) -> Result<()> {
    if paths.managed_clickhouse_dir.exists() {
        fs::remove_dir_all(&paths.managed_clickhouse_dir).with_context(|| {
            format!("failed removing {}", paths.managed_clickhouse_dir.display())
        })?;
    }

    println!(
        "managed clickhouse removed: {}",
        paths.managed_clickhouse_dir.display()
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<ExitCode> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        usage();
        return Ok(ExitCode::from(2));
    }

    let command = args.remove(0);

    match command.as_str() {
        "up" => {
            let (raw_config, rest) = parse_config_flag(&args)?;
            let (config_path, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            ensure_runtime_dirs(&paths)?;

            let no_ingest = rest.iter().any(|arg| arg == "--no-ingest");
            let force_monitor = rest.iter().any(|arg| arg == "--monitor");
            let force_mcp = rest.iter().any(|arg| arg == "--mcp");

            start_clickhouse(&cfg, &paths).await?;
            cmd_db_migrate(&cfg).await?;

            if !no_ingest {
                start_background_service(Service::Ingest, &config_path, &cfg, &paths, &[])?;
            }
            if force_monitor || cfg.runtime.start_monitor_on_up {
                start_background_service(Service::Monitor, &config_path, &cfg, &paths, &[])?;
            }
            if force_mcp || cfg.runtime.start_mcp_on_up {
                start_background_service(Service::Mcp, &config_path, &cfg, &paths, &[])?;
            }

            cmd_status(&paths, &cfg).await?;
            Ok(ExitCode::SUCCESS)
        }
        "down" => {
            let (raw_config, rest) = parse_config_flag(&args)?;
            if !rest.is_empty() {
                bail!("unexpected arguments for down: {}", rest.join(" "));
            }
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);

            for service in [
                Service::Mcp,
                Service::Monitor,
                Service::Ingest,
                Service::ClickHouse,
            ] {
                if stop_service(&paths, service)? {
                    println!("stopped {}", service.name());
                }
            }

            Ok(ExitCode::SUCCESS)
        }
        "status" => {
            let (raw_config, rest) = parse_config_flag(&args)?;
            if !rest.is_empty() {
                bail!("unexpected arguments for status: {}", rest.join(" "));
            }
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            cmd_status(&paths, &cfg).await?;
            Ok(ExitCode::SUCCESS)
        }
        "logs" => {
            let (service, lines, raw_config) = parse_logs_args(&args)?;
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            print_logs(&paths, service, lines)?;
            Ok(ExitCode::SUCCESS)
        }
        "db" => {
            if args.is_empty() {
                usage();
                return Ok(ExitCode::from(2));
            }

            let sub = args.remove(0);
            let (raw_config, rest) = parse_config_flag(&args)?;
            if !rest.is_empty() {
                bail!("unexpected db args: {}", rest.join(" "));
            }

            let (_, cfg) = load_cfg(raw_config)?;

            match sub.as_str() {
                "migrate" => {
                    cmd_db_migrate(&cfg).await?;
                    Ok(ExitCode::SUCCESS)
                }
                "doctor" => {
                    let report = cmd_db_doctor(&cfg).await?;
                    println!("{}", serde_json::to_string_pretty(&report)?);
                    let healthy = report.clickhouse_healthy
                        && report.database_exists
                        && report.pending_migrations.is_empty()
                        && report.missing_tables.is_empty()
                        && report.errors.is_empty();

                    if healthy {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
                _ => {
                    usage();
                    Ok(ExitCode::from(2))
                }
            }
        }
        "clickhouse" => {
            if args.is_empty() {
                usage();
                return Ok(ExitCode::from(2));
            }

            let sub = args.remove(0);
            let (raw_config, rest) = parse_config_flag(&args)?;
            let (_, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);

            match sub.as_str() {
                "install" => {
                    let mut force = false;
                    for arg in rest {
                        match arg.as_str() {
                            "--force" => force = true,
                            _ => bail!("unexpected clickhouse install arg: {}", arg),
                        }
                    }
                    cmd_clickhouse_install(&cfg, &paths, force).await?;
                    Ok(ExitCode::SUCCESS)
                }
                "status" => {
                    if !rest.is_empty() {
                        bail!("unexpected clickhouse status args: {}", rest.join(" "));
                    }
                    cmd_clickhouse_status(&paths);
                    Ok(ExitCode::SUCCESS)
                }
                "uninstall" => {
                    if !rest.is_empty() {
                        bail!("unexpected clickhouse uninstall args: {}", rest.join(" "));
                    }
                    cmd_clickhouse_uninstall(&paths)?;
                    Ok(ExitCode::SUCCESS)
                }
                _ => {
                    usage();
                    Ok(ExitCode::from(2))
                }
            }
        }
        "service" => {
            if args.is_empty() {
                usage();
                return Ok(ExitCode::from(2));
            }

            let sub = args.remove(0);
            let (raw_config, rest) = parse_config_flag(&args)?;
            let (config_path, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);

            match sub.as_str() {
                "install" => {
                    let (enable, start) = parse_service_install_flags(&rest)?;
                    cmd_service_install(&config_path, &cfg, &paths, enable, start).await?;
                    Ok(ExitCode::SUCCESS)
                }
                "uninstall" => {
                    let (disable, stop) = parse_service_uninstall_flags(&rest)?;
                    cmd_service_uninstall(&cfg, disable, stop)?;
                    Ok(ExitCode::SUCCESS)
                }
                "status" => {
                    if !rest.is_empty() {
                        bail!("unexpected service status args: {}", rest.join(" "));
                    }
                    cmd_service_status(&cfg)?;
                    Ok(ExitCode::SUCCESS)
                }
                _ => {
                    usage();
                    Ok(ExitCode::from(2))
                }
            }
        }
        "run" => {
            if args.is_empty() {
                usage();
                return Ok(ExitCode::from(2));
            }

            let service_name = args.remove(0);
            let (raw_config, passthrough) = parse_config_flag(&args)?;
            let (config_path, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);

            let Some(service) = parse_service(&service_name) else {
                usage();
                return Ok(ExitCode::from(2));
            };

            run_foreground_service(service, &config_path, &cfg, &paths, &passthrough).await
        }
        _ => {
            usage();
            Ok(ExitCode::from(2))
        }
    }
}
