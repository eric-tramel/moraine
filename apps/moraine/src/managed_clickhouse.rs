use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use reqwest::{Client, Url};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

use crate::paths::{ensure_runtime_dirs, RuntimePaths};
use crate::process::{
    cleanup_legacy_clickhouse_pipe_log, log_path, pid_path, service_running, write_pid,
    StartOutcome, StartState,
};
use crate::render::ClickhouseStatusSnapshot;
use crate::service::Service;

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

#[derive(Clone, Copy, Debug)]
struct ClickHouseAsset {
    url: &'static str,
    sha256: &'static str,
    is_archive: bool,
}

fn clickhouse_ports_from_url(cfg: &AppConfig) -> Result<(u16, u16, u16)> {
    let parsed = Url::parse(&cfg.clickhouse.url)
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

fn clickhouse_url_is_local(cfg: &AppConfig) -> Result<bool> {
    let parsed = Url::parse(&cfg.clickhouse.url)
        .with_context(|| format!("invalid clickhouse.url '{}'", cfg.clickhouse.url))?;
    let Some(host) = parsed.host_str() else {
        bail!(
            "clickhouse.url '{}' must include a host",
            cfg.clickhouse.url
        );
    };

    Ok(matches!(
        host.to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1" | "[::1]" | "0.0.0.0" | "::" | "[::]"
    ))
}

fn materialize_clickhouse_config(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    let (http_port, tcp_port, interserver_http_port) = clickhouse_ports_from_url(cfg)?;
    let rendered_clickhouse = CLICKHOUSE_TEMPLATE
        .replace("__MORAINE_HOME__", &cfg.runtime.root_dir)
        .replace("__CLICKHOUSE_HTTP_PORT__", &http_port.to_string())
        .replace("__CLICKHOUSE_TCP_PORT__", &tcp_port.to_string())
        .replace(
            "__CLICKHOUSE_INTERSERVER_HTTP_PORT__",
            &interserver_http_port.to_string(),
        );
    let rendered_users = USERS_TEMPLATE.replace("__MORAINE_HOME__", &cfg.runtime.root_dir);

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

pub(crate) fn managed_clickhouse_bin(paths: &RuntimePaths, binary: &str) -> PathBuf {
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

async fn download_to_path(url: &str, dest: &Path, label: &str) -> Result<()> {
    let client = Client::new();
    let mut response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download {}", url))?
        .error_for_status()
        .with_context(|| format!("download failed for {}", url))?;

    let total = response.content_length();
    let show_progress = std::io::stderr().is_terminal();

    let mut file = std::fs::File::create(dest)
        .with_context(|| format!("failed writing {}", dest.display()))?;
    let mut downloaded: u64 = 0;
    let mut last_render = Instant::now();

    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("failed reading response body for {}", url))?
    {
        file.write_all(&chunk)
            .with_context(|| format!("failed writing {}", dest.display()))?;
        downloaded += chunk.len() as u64;

        if show_progress && last_render.elapsed() >= Duration::from_millis(150) {
            render_download_progress(label, downloaded, total, false);
            last_render = Instant::now();
        }
    }

    if show_progress {
        render_download_progress(label, downloaded, total, true);
    }

    Ok(())
}

fn render_download_progress(label: &str, done: u64, total: Option<u64>, done_flag: bool) {
    const MIB: f64 = 1024.0 * 1024.0;
    let done_mb = done as f64 / MIB;
    match total {
        Some(t) if t > 0 => {
            let total_mb = t as f64 / MIB;
            let pct = ((done as f64 / t as f64) * 100.0).min(100.0);
            eprint!("\r  {label}: {done_mb:>6.1} / {total_mb:>6.1} MiB  ({pct:>5.1}%)");
        }
        _ => {
            eprint!("\r  {label}: {done_mb:>6.1} MiB");
        }
    }
    if done_flag {
        eprintln!();
    } else {
        std::io::stderr().flush().ok();
    }
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

fn path_ends_with_components(path: &Path, suffix: &[&str]) -> bool {
    let mut components = path.components().rev();
    for expected in suffix.iter().rev() {
        match components
            .next()
            .and_then(|component| component.as_os_str().to_str())
        {
            Some(component) if component == *expected => {}
            _ => return false,
        }
    }

    true
}

fn find_file_ending_with(root: &Path, suffix: &[&str]) -> Result<Option<PathBuf>> {
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

            if path_ends_with_components(&path, suffix) {
                return Ok(Some(path));
            }
        }
    }

    Ok(None)
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

    download_to_path(asset.url, &download, &format!("ClickHouse {version}")).await?;

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

        find_file_ending_with(&extract_dir, &["usr", "bin", "clickhouse"])?
            .or(find_file_named(&extract_dir, "clickhouse")?)
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

    let version = &cfg.runtime.clickhouse_version;
    let should_install = if cfg.runtime.clickhouse_auto_install {
        eprintln!(
            "managed ClickHouse not found; auto-installing {version}.\n\
             one-time ~175 MiB download + extract (progress shown below).\n\
             set runtime.clickhouse_auto_install = false in your config to disable.",
        );
        true
    } else {
        prompt_install_clickhouse(version)?
    };

    if should_install {
        install_managed_clickhouse(paths, version, false).await?;
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

fn prompt_install_clickhouse(version: &str) -> Result<bool> {
    let stdin = std::io::stdin();
    if !stdin.is_terminal() || !std::io::stderr().is_terminal() {
        bail!(
            "managed ClickHouse is not installed and runtime.clickhouse_auto_install = false.\n\
             cannot prompt in non-interactive mode.\n\
             remediation:\n\
             - run `moraine clickhouse install` explicitly, or\n\
             - set runtime.clickhouse_auto_install = true in your config"
        );
    }

    loop {
        eprint!("managed ClickHouse {version} is not installed. install now? [Y/n] ");
        std::io::stderr().flush().ok();

        let mut input = String::new();
        stdin
            .read_line(&mut input)
            .context("failed to read confirmation from stdin")?;

        match input.trim().to_ascii_lowercase().as_str() {
            "" | "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => eprintln!("please answer 'y' or 'n'."),
        }
    }
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

pub(crate) async fn start_clickhouse(
    cfg: &AppConfig,
    paths: &RuntimePaths,
) -> Result<StartOutcome> {
    if let Some(pid) = service_running(paths, Service::ClickHouse) {
        return Ok(StartOutcome {
            service: Service::ClickHouse,
            state: StartState::AlreadyRunning,
            pid: Some(pid),
            log_path: Some(log_path(paths, Service::ClickHouse).display().to_string()),
        });
    }

    let url_is_local = clickhouse_url_is_local(cfg)?;
    let client = ClickHouseClient::new(cfg.clickhouse.clone())?;
    match client.ping().await {
        Ok(()) => {
            return Ok(StartOutcome {
                service: Service::ClickHouse,
                state: StartState::AlreadyServing,
                pid: None,
                log_path: None,
            });
        }
        Err(err) if !url_is_local => {
            bail!(
                "clickhouse.url '{}' points at a non-local endpoint, but the endpoint is not healthy: {err}. start or repair that ClickHouse endpoint before running `moraine up`; Moraine only starts managed ClickHouse for local URLs",
                cfg.clickhouse.url
            );
        }
        Err(_) => {}
    }

    cleanup_legacy_clickhouse_pipe_log(paths);

    let server_bin = resolve_clickhouse_server_command(cfg, paths).await?;

    materialize_clickhouse_config(cfg, paths)?;

    let child = Command::new(&server_bin)
        .arg("--config-file")
        .arg(&paths.clickhouse_config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to start {}", server_bin.display()))?;

    write_pid(&pid_path(paths, Service::ClickHouse), child.id())?;

    wait_for_clickhouse(cfg).await?;
    Ok(StartOutcome {
        service: Service::ClickHouse,
        state: StartState::Started,
        pid: Some(child.id()),
        log_path: Some(log_path(paths, Service::ClickHouse).display().to_string()),
    })
}

pub(crate) async fn run_foreground_clickhouse(
    cfg: &AppConfig,
    paths: &RuntimePaths,
) -> Result<ExitCode> {
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

pub(crate) fn managed_clickhouse_version(paths: &RuntimePaths) -> Option<String> {
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

pub(crate) fn managed_clickhouse_checksum_state(cfg: &AppConfig, paths: &RuntimePaths) -> String {
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

pub(crate) fn active_clickhouse_source(paths: &RuntimePaths) -> (&'static str, Option<PathBuf>) {
    let managed = managed_clickhouse_bin(paths, "clickhouse-server");
    if managed.exists() {
        return ("managed", Some(managed));
    }
    if clickhouse_from_path_available() {
        return ("path", Some(PathBuf::from("clickhouse-server")));
    }
    ("missing", None)
}

pub(crate) async fn cmd_clickhouse_install(
    paths: &RuntimePaths,
    version: &str,
    force: bool,
) -> Result<PathBuf> {
    ensure_runtime_dirs(paths)?;
    let installed = install_managed_clickhouse(paths, version, force).await?;
    Ok(installed)
}

pub(crate) fn cmd_clickhouse_status(
    cfg: &AppConfig,
    paths: &RuntimePaths,
) -> ClickhouseStatusSnapshot {
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

pub(crate) fn cmd_clickhouse_uninstall(paths: &RuntimePaths) -> Result<String> {
    if paths.managed_clickhouse_dir.exists() {
        fs::remove_dir_all(&paths.managed_clickhouse_dir).with_context(|| {
            format!("failed removing {}", paths.managed_clickhouse_dir.display())
        })?;
    }

    Ok(paths.managed_clickhouse_dir.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::AppConfig;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("moraine-{name}-{stamp}"));
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
    fn find_file_ending_with_prefers_usr_bin_clickhouse() {
        let root = temp_dir("find-file-ending-with");
        let completion = root.join("pkg/usr/share/bash-completion/completions/clickhouse");
        let binary = root.join("pkg/usr/bin/clickhouse");
        write_file(&completion);
        write_file(&binary);

        let resolved = find_file_ending_with(&root, &["usr", "bin", "clickhouse"])
            .expect("resolve clickhouse path")
            .expect("clickhouse path");

        assert_eq!(resolved, binary);
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
    fn clickhouse_url_is_local_accepts_loopback_hosts() {
        for url in [
            "http://localhost:8123",
            "http://127.0.0.1:8123",
            "http://[::1]:8123",
            "http://0.0.0.0:8123",
            "http://[::]:8123",
        ] {
            let mut cfg = AppConfig::default();
            cfg.clickhouse.url = url.to_string();
            assert!(clickhouse_url_is_local(&cfg).expect(url), "{url}");
        }
    }

    #[test]
    fn clickhouse_url_is_local_rejects_remote_hosts() {
        for url in [
            "http://clickhouse.local:8123",
            "https://clickhouse.example.com:8443",
            "http://192.168.1.50:8123",
        ] {
            let mut cfg = AppConfig::default();
            cfg.clickhouse.url = url.to_string();
            assert!(!clickhouse_url_is_local(&cfg).expect(url), "{url}");
        }
    }

    #[test]
    fn managed_checksum_state_reports_verified() {
        let root = temp_dir("checksum");
        let managed_dir = root.join("managed");
        fs::create_dir_all(&managed_dir).expect("managed dir");

        let mut cfg = AppConfig::default();
        cfg.runtime.managed_clickhouse_dir = managed_dir.to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        let expected = clickhouse_asset_for_host(&cfg.runtime.clickhouse_version)
            .expect("host asset")
            .sha256;
        fs::write(
            managed_clickhouse_checksum_file(&paths),
            format!(
                "{expected}
"
            ),
        )
        .expect("write checksum");

        assert_eq!(managed_clickhouse_checksum_state(&cfg, &paths), "verified");
        let _ = fs::remove_dir_all(root);
    }
}
