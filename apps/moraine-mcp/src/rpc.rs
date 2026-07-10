use crate::cli::ServeMode;
use anyhow::{anyhow, bail, Context, Result};
use moraine_config::AppConfig;
use moraine_mcp_core::SessionOriginScope;
use std::path::PathBuf;
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};

#[derive(Debug, Default)]
pub struct BackendOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub static_dir: Option<PathBuf>,
}

type ServiceExit = (&'static str, Result<()>);

enum ShutdownCause {
    Signal(Result<()>),
    Service(Option<Result<ServiceExit, JoinError>>),
}

/// Build the appropriate Tokio runtime for the chosen mode and drive it.
///
/// Socket mode is the unified backend composition root: it constructs one
/// repository and concurrently hosts both MCP-over-Unix-socket and monitor
/// HTTP surfaces. The stdio arms remain unchanged and continue to select a
/// lightweight proxy runtime or the embedded fallback runtime.
pub fn run(
    cfg: AppConfig,
    serve_mode: ServeMode,
    session_scope: Option<SessionOriginScope>,
    options: BackendOptions,
) -> Result<()> {
    match serve_mode {
        ServeMode::Socket => {
            if session_scope.is_some() {
                bail!("--project-only cannot be combined with --serve socket: the shared central server serves sessions from every project");
            }
            let socket_path = PathBuf::from(&cfg.mcp.central_socket_path);
            let host = options.host.unwrap_or_else(|| cfg.monitor.host.clone());
            let port = options.port.unwrap_or(cfg.monitor.port);
            let static_dir = moraine_monitor_core::resolve_static_dir(options.static_dir);
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            runtime.block_on(run_backend(cfg, socket_path, host, port, static_dir))
        }
        // Only the unscoped, central-enabled path can become a thin proxy, so
        // it is the only one that gets the lightweight current-thread runtime.
        ServeMode::Stdio if session_scope.is_none() && cfg.mcp.use_central_server => {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build current-thread runtime")?;
            runtime.block_on(moraine_mcp_core::run_mcp_entry(cfg, None))
        }
        ServeMode::Stdio => {
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            runtime.block_on(moraine_mcp_core::run_mcp_entry(cfg, session_scope))
        }
    }
}

async fn run_backend(
    cfg: AppConfig,
    socket_path: PathBuf,
    host: String,
    port: u16,
    static_dir: PathBuf,
) -> Result<()> {
    // This is the daemon mode's sole repository factory call. Both services
    // receive clones of this exact Arc, sharing its HTTP pool and caches.
    let user_agent = format!(
        "moraine-backend/{} (pid={})",
        env!("CARGO_PKG_VERSION"),
        std::process::id()
    );
    let repository = moraine_mcp_core::build_repository_with_user_agent(&cfg, None, user_agent)
        .context("failed to build backend conversation repository")?;
    let socket_repository = repository.clone();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut services = JoinSet::new();

    let socket_cfg = cfg.clone();
    let socket_shutdown = shutdown_rx.clone();
    services.spawn(async move {
        let result = moraine_mcp_core::run_socket_with_repository(
            socket_cfg,
            socket_repository,
            socket_path,
            wait_for_shutdown(socket_shutdown),
        )
        .await;
        ("MCP socket service", result)
    });

    services.spawn(async move {
        let result = moraine_monitor_core::run_server_with_repository(
            cfg,
            repository,
            host,
            port,
            static_dir,
            wait_for_shutdown(shutdown_rx),
        )
        .await;
        ("monitor HTTP service", result)
    });

    // Exactly one process-level signal future supervises both services.
    let cause = tokio::select! {
        signal = shutdown_signal() => ShutdownCause::Signal(signal),
        service = services.join_next() => ShutdownCause::Service(service),
    };
    let _ = shutdown_tx.send(true);

    let primary = match cause {
        ShutdownCause::Signal(signal) => signal.context("backend shutdown signal handler failed"),
        ShutdownCause::Service(Some(service)) => service_exit_result(service, false),
        ShutdownCause::Service(None) => Err(anyhow!(
            "backend service supervisor ended without a running service"
        )),
    };

    let mut shutdown_failure = None;
    while let Some(service) = services.join_next().await {
        if let Err(error) = service_exit_result(service, true) {
            shutdown_failure.get_or_insert(error);
        }
    }

    primary?;
    if let Some(error) = shutdown_failure {
        return Err(error);
    }
    Ok(())
}

fn service_exit_result(
    service: Result<ServiceExit, JoinError>,
    shutdown_expected: bool,
) -> Result<()> {
    let (name, result) = service.context("backend service task failed")?;
    match result {
        Ok(()) if shutdown_expected => Ok(()),
        Ok(()) => Err(anyhow!("{name} stopped unexpectedly")),
        Err(error) => Err(error).with_context(|| format!("{name} failed")),
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate =
        signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;
    let mut interrupt =
        signal(SignalKind::interrupt()).context("failed to install SIGINT handler")?;
    tokio::select! {
        received = terminate.recv() => {
            received.ok_or_else(|| anyhow!("SIGTERM handler closed before receiving a signal"))?;
        }
        received = interrupt.recv() => {
            received.ok_or_else(|| anyhow!("SIGINT handler closed before receiving a signal"))?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("failed to install Ctrl-C handler")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::net::TcpListener;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "moraine-mcp-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    fn short_socket_path() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        PathBuf::from(format!(
            "/tmp/moraine-mcp-{}-{stamp}.sock",
            std::process::id()
        ))
    }

    #[test]
    fn monitor_bind_failure_cancels_socket_and_cleans_path() {
        let occupied = TcpListener::bind(("127.0.0.1", 0)).expect("occupy TCP port");
        let port = occupied.local_addr().expect("occupied address").port();
        let static_dir = temp_path("partial-bind-static");
        fs::create_dir_all(&static_dir).expect("create static directory");
        fs::write(static_dir.join("index.html"), "<!doctype html>").expect("write index");
        let socket_path = short_socket_path();
        let _ = fs::remove_file(&socket_path);

        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let error = runtime
            .block_on(run_backend(
                AppConfig::default(),
                socket_path.clone(),
                "127.0.0.1".to_string(),
                port,
                static_dir.clone(),
            ))
            .expect_err("occupied HTTP port must fail unified backend startup");

        assert!(
            format!("{error:#}").contains("address already in use"),
            "unexpected error: {error:#}"
        );
        assert!(
            !socket_path.exists(),
            "MCP socket must be cleaned when HTTP bind fails"
        );

        let _ = fs::remove_dir_all(static_dir);
    }
}
