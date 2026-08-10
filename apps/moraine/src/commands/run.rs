use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Duration;

use crate::cli::RunArgs;
use crate::managed_clickhouse::run_foreground_clickhouse;
use crate::paths::{load_cfg, runtime_paths, RuntimePaths};
use crate::process::{
    lock_storage_writer_launch, pid_path, remove_pid_if_matches, require_service_binary,
    service_args_with_defaults, write_pid_exclusive, StorageGateGuard,
};
use crate::service::Service;

pub(super) async fn handle(global_config: Option<PathBuf>, run: RunArgs) -> Result<ExitCode> {
    let (inline_config, passthrough) = parse_config_flag(&run.args)?;
    let raw_config = inline_config.or(global_config);
    let (config_path, cfg) = load_cfg(raw_config)?;
    let paths = runtime_paths(&cfg);
    if run.service == Service::ClickHouse {
        return run_foreground_clickhouse(&cfg, &paths).await;
    }

    let writer_gate = lock_storage_writer_launch(&paths, run.service)?;
    let binary = require_service_binary(run.service, &paths)?;
    let args = service_args_with_defaults(
        run.service,
        config_path.as_path(),
        &cfg,
        &paths,
        &passthrough,
    );

    let mut command = Command::new(binary);
    command.args(args);
    if let Some((key, value)) = config_path.child_origin_environment() {
        command.env(key, value);
    }
    let pid_file = foreground_pid_path(&paths, run.service);
    run_foreground_child(
        command,
        pid_file.as_deref(),
        run.service.name(),
        writer_gate,
    )
    .await
}

fn foreground_pid_path(paths: &RuntimePaths, service: Service) -> Option<PathBuf> {
    (service != Service::Mcp).then(|| pid_path(paths, service))
}

async fn run_foreground_child(
    mut command: Command,
    pid_file: Option<&Path>,
    service_name: &str,
    mut writer_gate: Option<StorageGateGuard>,
) -> Result<ExitCode> {
    if let Some(parent) = pid_file.and_then(Path::parent) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create PID directory {}", parent.display()))?;
    }
    configure_transient_child_cleanup(&mut command, pid_file.is_none());
    let mut shutdown = pid_file
        .is_none()
        .then(TransientShutdownSignals::install)
        .transpose()?;
    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to run {service_name}"))?;
    let child_pid = child
        .id()
        .ok_or_else(|| anyhow::anyhow!("{service_name} child has no process ID"))?;
    if let Some(pid_file) = pid_file {
        if let Err(error) = write_pid_exclusive(pid_file, child_pid) {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error);
        }
        drop(writer_gate.take());
    }
    let result = if let Some(shutdown) = shutdown.as_mut() {
        wait_for_transient_child(&mut child, child_pid, service_name, shutdown).await
    } else {
        child
            .wait()
            .await
            .map_err(anyhow::Error::from)
            .with_context(|| format!("failed to wait for {service_name}"))
    };
    drop(writer_gate);
    if let Some(pid_file) = pid_file {
        remove_pid_if_matches(pid_file, child_pid);
    }
    let status = result?;
    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

#[cfg(target_os = "linux")]
fn configure_transient_child_cleanup(command: &mut Command, transient: bool) {
    use std::os::unix::process::CommandExt;

    if !transient {
        return;
    }
    let parent_pid = unsafe { libc::getpid() };
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != parent_pid {
                libc::raise(libc::SIGKILL);
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_transient_child_cleanup(_command: &mut Command, _transient: bool) {}

#[cfg(unix)]
struct TransientShutdownSignals {
    terminate: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TransientShutdownSignals {
    fn install() -> Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};

        Ok(Self {
            terminate: signal(SignalKind::terminate())
                .context("failed to install transient child SIGTERM handler")?,
            interrupt: signal(SignalKind::interrupt())
                .context("failed to install transient child SIGINT handler")?,
        })
    }

    async fn recv(&mut self) -> i32 {
        tokio::select! {
            biased;
            _ = self.terminate.recv() => libc::SIGTERM,
            _ = self.interrupt.recv() => libc::SIGINT,
        }
    }
}

#[cfg(not(unix))]
struct TransientShutdownSignals;

#[cfg(not(unix))]
impl TransientShutdownSignals {
    fn install() -> Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> i32 {
        let _ = tokio::signal::ctrl_c().await;
        0
    }
}

async fn wait_for_transient_child(
    child: &mut tokio::process::Child,
    child_pid: u32,
    service_name: &str,
    shutdown: &mut TransientShutdownSignals,
) -> Result<std::process::ExitStatus> {
    tokio::select! {
        result = child.wait() => {
            return result
                .map_err(anyhow::Error::from)
                .with_context(|| format!("failed to wait for {service_name}"));
        }
        signal = shutdown.recv() => {
            #[cfg(unix)]
            if unsafe { libc::kill(child_pid as i32, signal) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).context(format!(
                        "failed to forward shutdown signal to {service_name}"
                    ));
                }
            }
            #[cfg(not(unix))]
            child.start_kill().with_context(|| format!(
                "failed to stop {service_name} after Ctrl-C"
            ))?;
        }
    }

    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(result) => result
            .map_err(anyhow::Error::from)
            .with_context(|| format!("failed to wait for {service_name} shutdown")),
        Err(_) => {
            eprintln!("{service_name} did not stop within 5 seconds; killing it");
            child
                .start_kill()
                .with_context(|| format!("failed to kill {service_name}"))?;
            child
                .wait()
                .await
                .map_err(anyhow::Error::from)
                .with_context(|| format!("failed to wait for killed {service_name}"))
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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

        let err = parse_config_flag(&["--config=".to_string()]).expect_err("empty config");
        assert!(err.to_string().contains("--config requires a path"));
    }

    #[test]
    fn parse_config_flag_rejects_dangling_config() {
        let err = parse_config_flag(&["--config".to_string()]).expect_err("dangling config");
        assert!(err.to_string().contains("--config requires a path"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn foreground_child_is_registered_for_storage_cutover_barriers() {
        let root =
            std::env::temp_dir().join(format!("moraine-foreground-pid-{}", std::process::id()));
        let pid_file = root.join("run/ingest.pid");
        let mut command = Command::new("/bin/sh");
        command
            .env("PID_FILE", &pid_file)
            .args(["-c", "sleep 0.1; test \"$(cat \"$PID_FILE\")\" = \"$$\""]);

        let code = run_foreground_child(command, Some(&pid_file), "ingest", None)
            .await
            .expect("run registered foreground child");

        assert_eq!(code, ExitCode::SUCCESS);
        assert!(!pid_file.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn mcp_foreground_clients_do_not_own_a_singleton_pid() {
        let cfg = moraine_config::AppConfig::default();
        let paths = runtime_paths(&cfg);

        assert!(foreground_pid_path(&paths, Service::Mcp).is_none());
        assert_eq!(
            foreground_pid_path(&paths, Service::Ingest),
            Some(pid_path(&paths, Service::Ingest))
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pidless_mcp_client_holds_storage_gate_for_its_lifetime() {
        let root = std::env::temp_dir().join(format!("moraine-mcp-gate-{}", std::process::id()));
        let mut cfg = moraine_config::AppConfig::default();
        cfg.runtime.pids_dir = root.join("run").to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);
        let marker = root.join("client-ready");
        std::fs::create_dir_all(&root).expect("create test root");

        let writer_gate = crate::process::lock_storage_writer_launch(&paths, Service::Mcp)
            .expect("acquire MCP launch gate")
            .expect("MCP is a possible storage writer");
        let client_marker = marker.clone();
        let client = tokio::spawn(async move {
            let mut command = Command::new("/bin/sh");
            command
                .env("READY", client_marker)
                .args(["-c", "touch \"$READY\"; sleep 0.2"]);
            run_foreground_child(command, None, "mcp", Some(writer_gate))
                .await
                .expect("run pidless MCP client")
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !marker.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(marker.exists(), "MCP client did not start");

        let migration_paths = paths.clone();
        let (migration_tx, migration_rx) = std::sync::mpsc::channel();
        let migration = std::thread::spawn(move || {
            let gate = crate::process::lock_storage_migration(&migration_paths)
                .expect("acquire migration gate");
            migration_tx.send(()).expect("report migration gate");
            drop(gate);
        });
        assert!(migration_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());

        assert_eq!(client.await.expect("join MCP client"), ExitCode::SUCCESS);
        migration_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("migration must proceed after MCP exits");
        migration.join().expect("join migration contender");
        let _ = std::fs::remove_dir_all(root);
    }
}
