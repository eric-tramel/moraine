use anyhow::{bail, Context, Result};
use moraine_config::AppConfig;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::paths::RuntimePaths;
use crate::service::Service;

pub(crate) const LEGACY_MONITOR_PID_FILE: &str = "monitor.pid";
pub(crate) const LEGACY_MCP_PID_FILE: &str = "mcp.pid";

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StartState {
    Started,
    AlreadyRunning,
    /// A complete service endpoint is healthy but not launcher-tracked, such as
    /// an external ClickHouse endpoint or a backend whose PID file was lost.
    /// Nothing was spawned.
    AlreadyServing,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct StartOutcome {
    pub(crate) service: Service,
    pub(crate) state: StartState,
    /// `None` when the service is alive but not launcher-tracked
    /// (`StartState::AlreadyServing`).
    pub(crate) pid: Option<u32>,
    pub(crate) log_path: Option<String>,
}

pub(crate) fn pid_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    paths.pids_dir.join(service.pid_file())
}

pub(crate) fn clickhouse_supervisor_log_path(paths: &RuntimePaths) -> PathBuf {
    paths.logs_dir.join("clickhouse-supervisor.log")
}

fn clickhouse_internal_log_path(paths: &RuntimePaths) -> PathBuf {
    paths
        .clickhouse_root
        .join("log")
        .join("clickhouse-server.log")
}

fn legacy_clickhouse_pipe_log_path(paths: &RuntimePaths) -> PathBuf {
    paths.logs_dir.join(Service::ClickHouse.log_file())
}

pub(crate) fn cleanup_legacy_clickhouse_pipe_log(paths: &RuntimePaths) {
    let legacy_log = legacy_clickhouse_pipe_log_path(paths);
    let should_remove = fs::metadata(&legacy_log)
        .map(|metadata| metadata.is_file())
        .unwrap_or(false);
    if should_remove {
        let _ = fs::remove_file(legacy_log);
    }
}

pub(crate) fn log_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    match service {
        Service::ClickHouse => clickhouse_internal_log_path(paths),
        Service::Ingest | Service::Backend | Service::Mcp => {
            paths.logs_dir.join(service.log_file())
        }
    }
}

fn read_pid(path: &Path) -> Option<u32> {
    let text = fs::read_to_string(path).ok()?;
    text.trim().parse::<u32>().ok()
}

pub(crate) fn write_pid(path: &Path, pid: u32) -> Result<()> {
    fs::write(path, format!("{}\n", pid))
        .with_context(|| format!("failed to write pid file {}", path.display()))
}

pub(crate) fn remove_pid_if_matches(path: &Path, expected_pid: u32) {
    if read_pid(path) == Some(expected_pid) {
        let _ = fs::remove_file(path);
    }
}

fn is_pid_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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

pub(crate) fn service_running(paths: &RuntimePaths, service: Service) -> Option<u32> {
    let path = pid_path(paths, service);
    ensure_pid_fresh(&path);
    let pid = read_pid(&path)?;
    if is_pid_running(pid) {
        Some(pid)
    } else {
        None
    }
}

pub(crate) fn legacy_service_running(paths: &RuntimePaths, pid_file: &str) -> Option<u32> {
    let path = paths.pids_dir.join(pid_file);
    ensure_pid_fresh(&path);
    let pid = read_pid(&path)?;
    is_pid_running(pid).then_some(pid)
}

/// True when something is currently accepting connections on the central MCP
/// socket. Used alongside (not instead of) the PID file: the daemon can
/// outlive a deleted or stale PID file, and acting on the file alone either
/// spawns a useless second daemon (`up`) or unlinks a live server's socket,
/// orphaning it unreachably (`down`).
pub(crate) fn central_socket_live(socket_path: &str) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::net::UnixStream::connect(socket_path).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = socket_path;
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackendEndpointStatus {
    pub(crate) socket_listening: bool,
    pub(crate) http_listening: bool,
}

fn backend_http_connect_host(host: &str) -> &str {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        _ => host,
    }
}

pub(crate) fn backend_http_live(host: &str, port: u16) -> bool {
    let connect_host = backend_http_connect_host(host);
    let authority = if connect_host.contains(':') {
        format!("[{connect_host}]:{port}")
    } else {
        format!("{connect_host}:{port}")
    };
    let Ok(addresses) = (connect_host, port).to_socket_addrs() else {
        return false;
    };

    for address in addresses {
        let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(200))
        else {
            continue;
        };
        let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
        let request = format!("GET / HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
        if stream.write_all(request.as_bytes()).is_err() {
            continue;
        }
        let mut prefix = [0_u8; 5];
        if stream.read_exact(&mut prefix).is_ok() && prefix == *b"HTTP/" {
            return true;
        }
    }
    false
}

pub(crate) fn backend_endpoint_status(cfg: &AppConfig) -> BackendEndpointStatus {
    BackendEndpointStatus {
        socket_listening: central_socket_live(&cfg.mcp.central_socket_path),
        http_listening: backend_http_live(&cfg.monitor.host, cfg.monitor.port),
    }
}

fn stop_pid_path(path: &Path, wait_attempts: usize) -> Result<bool> {
    let Some(pid) = read_pid(path) else {
        return Ok(false);
    };

    if !is_pid_running(pid) {
        let _ = fs::remove_file(path);
        return Ok(false);
    }

    let _ = Command::new("kill")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    for _ in 0..wait_attempts {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(path);
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = fs::remove_file(path);
    Ok(true)
}

pub(crate) fn stop_service(paths: &RuntimePaths, service: Service) -> Result<bool> {
    let wait_attempts = if service == Service::ClickHouse {
        50
    } else {
        20
    };
    stop_pid_path(&pid_path(paths, service), wait_attempts)
}

pub(crate) fn stop_legacy_service(paths: &RuntimePaths, pid_file: &str) -> Result<bool> {
    stop_pid_path(&paths.pids_dir.join(pid_file), 20)
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
    env_flag_enabled("MORAINE_SOURCE_TREE_MODE")
}

#[derive(Debug, Clone)]
struct ServiceBinaryProbe {
    source: &'static str,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct ServiceBinaryResolution {
    binary_name: String,
    resolved_path: Option<PathBuf>,
    checked_paths: Vec<ServiceBinaryProbe>,
}

fn resolve_service_binary(service: Service, paths: &RuntimePaths) -> ServiceBinaryResolution {
    let name = service.binary_name().unwrap_or(service.name()).to_string();
    let mut checked_paths = Vec::new();

    let mut check = |source: &'static str, path: PathBuf| {
        if path.exists() {
            Some(path)
        } else {
            checked_paths.push(ServiceBinaryProbe { source, path });
            None
        }
    };

    if let Ok(dir) = std::env::var("MORAINE_SERVICE_BIN_DIR") {
        if let Some(path) = check("MORAINE_SERVICE_BIN_DIR", PathBuf::from(dir).join(&name)) {
            return ServiceBinaryResolution {
                binary_name: name,
                resolved_path: Some(path),
                checked_paths,
            };
        }
    }

    if let Some(path) = check("runtime.service_bin_dir", paths.service_bin_dir.join(&name)) {
        return ServiceBinaryResolution {
            binary_name: name,
            resolved_path: Some(path),
            checked_paths,
        };
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            if let Some(path) = check("moraine sibling", dir.join(&name)) {
                return ServiceBinaryResolution {
                    binary_name: name,
                    resolved_path: Some(path),
                    checked_paths,
                };
            }
        }
    }

    if source_tree_mode_enabled() {
        if let Some(project_bin) = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("target").join("debug").join(&name))
        {
            if let Some(path) = check("source-tree mode target/debug", project_bin) {
                return ServiceBinaryResolution {
                    binary_name: name,
                    resolved_path: Some(path),
                    checked_paths,
                };
            }
        }
    }

    ServiceBinaryResolution {
        binary_name: name,
        resolved_path: None,
        checked_paths,
    }
}

pub(crate) fn require_service_binary(service: Service, paths: &RuntimePaths) -> Result<PathBuf> {
    let resolution = resolve_service_binary(service, paths);
    if let Some(path) = resolution.resolved_path {
        return Ok(path);
    }

    let checked = if resolution.checked_paths.is_empty() {
        "- (no probe paths)".to_string()
    } else {
        resolution
            .checked_paths
            .iter()
            .map(|probe| format!("- {} ({})", probe.path.display(), probe.source))
            .collect::<Vec<_>>()
            .join("\n")
    };

    bail!(
        "required service binary `{}` for `{}` was not found.\nchecked:\n{}\nremediation:\n- install Moraine service binaries so `{}` exists under `runtime.service_bin_dir` (`{}`)\n- or set `MORAINE_SERVICE_BIN_DIR` to a directory containing `{}`\n- for source builds run `cargo build --workspace --locked` and set `MORAINE_SOURCE_TREE_MODE=1`\n`moraine` does not fall back to PATH for service binaries.",
        resolution.binary_name,
        service.name(),
        checked,
        resolution.binary_name,
        paths.service_bin_dir.display(),
        resolution.binary_name
    );
}

pub(crate) fn preflight_required_service_binaries(
    services: &[Service],
    paths: &RuntimePaths,
) -> Result<()> {
    for service in services {
        if *service == Service::ClickHouse {
            continue;
        }
        require_service_binary(*service, paths)?;
    }
    Ok(())
}

fn contains_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| {
        arg == flag
            || arg
                .strip_prefix(flag)
                .is_some_and(|rest| rest.starts_with('='))
    })
}

fn monitor_dist_candidate(root: &Path) -> PathBuf {
    root.join("web").join("monitor").join("dist")
}

const MONITOR_DIST_ENV_KEYS: &[&str] = &["MORAINE_MONITOR_DIST", "MORAINE_MONITOR_STATIC_DIR"];

pub(crate) fn resolve_monitor_static_dir(paths: &RuntimePaths) -> Option<PathBuf> {
    for key in MONITOR_DIST_ENV_KEYS {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let path = PathBuf::from(trimmed);
            if path.exists() {
                return Some(path);
            }
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(bin_dir) = exe.parent() {
            if let Some(bundle_root) = bin_dir.parent() {
                let candidate = monitor_dist_candidate(bundle_root);
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
    }

    if let Some(bundle_root) = paths.service_bin_dir.parent() {
        let candidate = monitor_dist_candidate(bundle_root);
        if candidate.exists() {
            return Some(candidate);
        }
    }

    if source_tree_mode_enabled() {
        if let Some(dev_path) = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(PathBuf::from)
        {
            let candidate = monitor_dist_candidate(&dev_path);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    None
}

pub(crate) fn service_args_with_defaults(
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

    if service == Service::Backend {
        if !contains_flag(passthrough, "--serve") {
            args.push("--serve".to_string());
            args.push("socket".to_string());
        }
        if !contains_flag(passthrough, "--socket") {
            args.push("--socket".to_string());
            args.push(cfg.mcp.central_socket_path.clone());
        }
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

pub(crate) fn start_background_service(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    extra_args: &[String],
) -> Result<StartOutcome> {
    if service == Service::ClickHouse {
        bail!("clickhouse is not managed by service launcher; use `moraine up`");
    }

    if let Some(pid) = service_running(paths, service) {
        return Ok(StartOutcome {
            service,
            state: StartState::AlreadyRunning,
            pid: Some(pid),
            log_path: Some(log_path(paths, service).display().to_string()),
        });
    }

    if service == Service::Backend {
        let tracked_legacy = [
            ("monitor", LEGACY_MONITOR_PID_FILE),
            ("mcp", LEGACY_MCP_PID_FILE),
        ]
        .into_iter()
        .filter_map(|(name, pid_file)| {
            legacy_service_running(paths, pid_file).map(|pid| format!("{name} (pid {pid})"))
        })
        .collect::<Vec<_>>();
        if !tracked_legacy.is_empty() {
            bail!(
                "cannot start backend while legacy managed service(s) are still running: {}; run `moraine down` first",
                tracked_legacy.join(", ")
            );
        }

        let endpoints = backend_endpoint_status(cfg);
        if endpoints.socket_listening && endpoints.http_listening {
            return Ok(StartOutcome {
                service,
                state: StartState::AlreadyServing,
                pid: None,
                log_path: Some(log_path(paths, service).display().to_string()),
            });
        }
        if endpoints.socket_listening || endpoints.http_listening {
            bail!(
                "cannot start backend while only part of its endpoint pair is already serving (MCP socket: {}, monitor HTTP: {}); stop the conflicting process and re-run `moraine up`",
                endpoints.socket_listening,
                endpoints.http_listening
            );
        }
    }

    let binary = require_service_binary(service, paths)?;

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
        // Background services never read stdin; closing it avoids inheriting the
        // launcher's terminal or pipe descriptors.
        .stdin(Stdio::null())
        .stdout(Stdio::from(logfile))
        .stderr(Stdio::from(logfile_err))
        .spawn()
        .with_context(|| format!("failed to start {}", service.name()))?;

    write_pid(&pid_path(paths, service), child.id())?;
    Ok(StartOutcome {
        service,
        state: StartState::Started,
        pid: Some(child.id()),
        log_path: Some(log_path(paths, service).display().to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::AppConfig;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    static ENV_VAR_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvVarGuard {
        fn capture(key: &'static str) -> Self {
            Self {
                key,
                original: std::env::var_os(key),
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.original {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn lock_env_vars() -> MutexGuard<'static, ()> {
        ENV_VAR_LOCK.lock().expect("env-var lock poisoned")
    }

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
    fn clickhouse_logs_use_internal_rotating_path() {
        let root = temp_dir("clickhouse-log-path");
        let logs_dir = root.join("logs");

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();
        cfg.runtime.logs_dir = logs_dir.to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        assert_eq!(
            log_path(&paths, Service::ClickHouse),
            root.join("clickhouse/log/clickhouse-server.log")
        );
        assert_eq!(
            clickhouse_supervisor_log_path(&paths),
            logs_dir.join("clickhouse-supervisor.log")
        );
        assert_eq!(
            log_path(&paths, Service::Ingest),
            logs_dir.join("ingest.log")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn remove_pid_if_matches_preserves_new_owner() {
        let root = temp_dir("compare-remove-pid");
        let path = root.join("service.pid");
        write_pid(&path, 42).expect("write pid");

        remove_pid_if_matches(&path, 41);
        assert_eq!(
            fs::read_to_string(&path).expect("preserved pid").trim(),
            "42"
        );

        remove_pid_if_matches(&path, 42);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_legacy_clickhouse_pipe_log_removes_legacy_file() {
        let root = temp_dir("legacy-clickhouse-log");
        let logs_dir = root.join("logs");
        fs::create_dir_all(&logs_dir).expect("create logs dir");

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();
        cfg.runtime.logs_dir = logs_dir.to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        let legacy_log = legacy_clickhouse_pipe_log_path(&paths);
        fs::write(&legacy_log, b"legacy clickhouse stdout").expect("write legacy log");
        assert!(legacy_log.exists());

        cleanup_legacy_clickhouse_pipe_log(&paths);
        assert!(!legacy_log.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_service_binary_prefers_env_then_config() {
        let _env_lock = lock_env_vars();
        let _service_bin_dir_guard = EnvVarGuard::capture("MORAINE_SERVICE_BIN_DIR");
        let _source_tree_mode_guard = EnvVarGuard::capture("MORAINE_SOURCE_TREE_MODE");

        let root = temp_dir("resolver");
        let env_dir = root.join("env");
        let cfg_dir = root.join("cfg");
        let env_bin = env_dir.join("moraine-ingest");
        let cfg_bin = cfg_dir.join("moraine-ingest");
        write_file(&env_bin);
        write_file(&cfg_bin);

        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = cfg_dir.to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        std::env::remove_var("MORAINE_SOURCE_TREE_MODE");
        std::env::set_var("MORAINE_SERVICE_BIN_DIR", &env_dir);
        assert_eq!(
            resolve_service_binary(Service::Ingest, &paths).resolved_path,
            Some(env_bin.clone())
        );

        std::env::remove_var("MORAINE_SERVICE_BIN_DIR");
        assert_eq!(
            resolve_service_binary(Service::Ingest, &paths).resolved_path,
            Some(cfg_bin)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_backend_binary_reports_moraine_mcp_without_path_fallback() {
        let _env_lock = lock_env_vars();
        let _service_bin_dir_guard = EnvVarGuard::capture("MORAINE_SERVICE_BIN_DIR");
        let _source_tree_mode_guard = EnvVarGuard::capture("MORAINE_SOURCE_TREE_MODE");

        let root = temp_dir("resolver-path");
        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = root.join("missing").to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        std::env::remove_var("MORAINE_SERVICE_BIN_DIR");
        std::env::remove_var("MORAINE_SOURCE_TREE_MODE");
        let resolved = resolve_service_binary(Service::Backend, &paths);
        assert_eq!(resolved.binary_name, "moraine-mcp");
        assert!(resolved.resolved_path.is_none());
        assert!(resolved
            .checked_paths
            .iter()
            .any(|probe| probe.source == "runtime.service_bin_dir"
                && probe.path == paths.service_bin_dir.join("moraine-mcp")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn require_service_binary_includes_remediation() {
        let _env_lock = lock_env_vars();
        let _service_bin_dir_guard = EnvVarGuard::capture("MORAINE_SERVICE_BIN_DIR");
        let _source_tree_mode_guard = EnvVarGuard::capture("MORAINE_SOURCE_TREE_MODE");

        let root = temp_dir("resolver-remediation");
        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = root.join("missing").to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        std::env::remove_var("MORAINE_SERVICE_BIN_DIR");
        std::env::remove_var("MORAINE_SOURCE_TREE_MODE");
        let err = require_service_binary(Service::Mcp, &paths).expect_err("missing mcp binary");
        let message = err.to_string();
        assert!(message.contains("required service binary `moraine-mcp`"));
        assert!(message.contains("runtime.service_bin_dir"));
        assert!(message.contains("MORAINE_SERVICE_BIN_DIR"));
        assert!(message.contains("cargo build --workspace --locked"));
        assert!(message.contains("does not fall back to PATH"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn service_args_with_defaults_injects_complete_backend_daemon_args() {
        let root = temp_dir("service-args");
        let config_path = root.join("moraine.toml");
        let static_dir = root.join("static");
        fs::create_dir_all(&static_dir).expect("static dir");

        let mut cfg = AppConfig::default();
        cfg.mcp.central_socket_path = "/tmp/moraine-test.sock".to_string();
        cfg.monitor.host = "127.0.0.1".to_string();
        cfg.monitor.port = 18080;
        cfg.runtime.service_bin_dir = root.join("bin").to_string_lossy().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        let _env_lock = lock_env_vars();
        let _monitor_dist_guard = EnvVarGuard::capture("MORAINE_MONITOR_DIST");
        std::env::set_var("MORAINE_MONITOR_DIST", &static_dir);

        let args = service_args_with_defaults(Service::Backend, &config_path, &cfg, &paths, &[]);
        assert_eq!(
            args,
            vec![
                "--config".to_string(),
                config_path.to_string_lossy().to_string(),
                "--serve".to_string(),
                "socket".to_string(),
                "--socket".to_string(),
                "/tmp/moraine-test.sock".to_string(),
                "--host".to_string(),
                "127.0.0.1".to_string(),
                "--port".to_string(),
                "18080".to_string(),
                "--static-dir".to_string(),
                static_dir.to_string_lossy().to_string(),
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn service_args_for_mcp_run_do_not_inject_server_args() {
        let root = temp_dir("mcp-run-args");
        let config_path = root.join("moraine.toml");
        let cfg = AppConfig::default();
        let paths = crate::paths::runtime_paths(&cfg);

        let args = service_args_with_defaults(Service::Mcp, &config_path, &cfg, &paths, &[]);
        assert_eq!(
            args,
            vec![
                "--config".to_string(),
                config_path.to_string_lossy().to_string()
            ]
        );
        assert!(!args.iter().any(|arg| arg == "--serve"));
        assert!(!args.iter().any(|arg| arg == "socket"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn central_socket_live_detects_listener_vs_dead_socket() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let sock = PathBuf::from(format!(
            "/tmp/moraine-csl-{}-{stamp}.sock",
            std::process::id()
        ));
        let sock_str = sock.to_string_lossy().to_string();

        assert!(!central_socket_live(&sock_str));

        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind socket");
        assert!(central_socket_live(&sock_str));
        let (accepted, _) = listener.accept().expect("accept liveness probe");
        drop(accepted);

        drop(listener);
        let deadline = Instant::now() + Duration::from_secs(2);
        while central_socket_live(&sock_str) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!central_socket_live(&sock_str));

        let _ = fs::remove_file(&sock);
    }

    #[cfg(unix)]
    #[test]
    fn socket_only_backend_endpoint_is_partial_not_already_serving() {
        let root = PathBuf::from(format!("/tmp/moraine-bso-{}", std::process::id()));
        let socket = root.join("b.sock");
        fs::create_dir_all(&root).expect("create short socket directory");
        let _listener =
            std::os::unix::net::UnixListener::bind(&socket).expect("bind backend socket");

        let mut cfg = AppConfig::default();
        cfg.mcp.central_socket_path = socket.display().to_string();
        cfg.monitor.host = "127.0.0.1".to_string();
        cfg.monitor.port = 0;
        cfg.runtime.pids_dir = root.join("run").display().to_string();
        cfg.runtime.logs_dir = root.join("logs").display().to_string();
        cfg.runtime.service_bin_dir = root.join("bin").display().to_string();
        let paths = crate::paths::runtime_paths(&cfg);

        let err = start_background_service(
            Service::Backend,
            &root.join("config.toml"),
            &cfg,
            &paths,
            &[],
        )
        .expect_err("partial endpoint pair must not be accepted as a complete backend");
        assert!(err.to_string().contains("only part of its endpoint pair"));

        let _ = fs::remove_dir_all(root);
    }
}
