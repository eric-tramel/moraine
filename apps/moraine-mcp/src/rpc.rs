use crate::cli::ServeMode;
use anyhow::{anyhow, bail, Context, Result};
use moraine_config::AppConfig;
use moraine_conversations::{BackendRepositoryRouter, RepoConfig};
#[cfg(unix)]
use moraine_mcp_core::PrivateRouteNegotiation;
use moraine_mcp_core::SessionOriginScope;
use std::{net::IpAddr, path::PathBuf, sync::Arc};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tracing::{debug, warn};

#[derive(Debug, Default)]
pub struct BackendOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub static_dir: Option<PathBuf>,
}

fn is_explicit_loopback_bind(bind: &str) -> bool {
    let ip_literal = bind
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(bind);
    ip_literal
        .parse::<IpAddr>()
        .map(|address| address.is_loopback())
        .unwrap_or(false)
}

fn normalize_listener_bind(bind: String) -> String {
    if bind.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{bind}]")
    } else {
        bind
    }
}

fn validate_backend_bind_policy(bind: &str, auth_token: Option<&str>) -> Result<()> {
    let has_nonempty_token = auth_token
        .map(|token| !token.trim().is_empty())
        .unwrap_or(false);
    if is_explicit_loopback_bind(bind) || has_nonempty_token {
        return Ok(());
    }

    bail!(
        "refusing backend startup before binding: effective HTTP bind `{bind}` is not an explicit loopback IP and backend.auth_token is missing or empty; configure backend.auth_token for experimental non-loopback binding (HTTP request authentication is not enabled)"
    )
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
            let host = options.host.unwrap_or_else(|| cfg.backend.bind.clone());
            let port = options.port.unwrap_or(cfg.monitor.port);
            let static_dir = moraine_monitor_core::resolve_static_dir(options.static_dir);
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            runtime.block_on(run_backend(cfg, socket_path, host, port, static_dir))
        }
        // Only the unscoped, central-enabled path can become a thin proxy, so
        ServeMode::Stdio if session_scope.is_none() && cfg.mcp.use_central_server => {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build current-thread runtime")?;
            runtime.block_on(run_stdio_entry(cfg, None))
        }
        ServeMode::Stdio => {
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            runtime.block_on(run_stdio_entry(cfg, session_scope))
        }
    }
}

fn repository_config(cfg: &AppConfig, session_scope: Option<SessionOriginScope>) -> RepoConfig {
    RepoConfig {
        max_results: cfg.mcp.max_results,
        preview_chars: cfg.mcp.preview_chars,
        default_context_before: cfg.mcp.default_context_before,
        default_context_after: cfg.mcp.default_context_after,
        default_include_tool_events: cfg.mcp.default_include_tool_events,
        default_exclude_codex_mcp: cfg.mcp.default_exclude_codex_mcp,
        async_log_writes: cfg.mcp.async_log_writes,
        bm25_k1: cfg.bm25.k1,
        bm25_b: cfg.bm25.b,
        bm25_default_min_score: cfg.bm25.default_min_score,
        bm25_default_min_should_match: cfg.bm25.default_min_should_match,
        bm25_max_query_terms: cfg.bm25.max_query_terms,
        session_scope,
    }
}

fn backend_router(
    cfg: Arc<AppConfig>,
    session_scope: Option<SessionOriginScope>,
    role: &str,
) -> Result<Arc<BackendRepositoryRouter>> {
    let user_agent = format!(
        "{role}/{} (pid={})",
        env!("CARGO_PKG_VERSION"),
        std::process::id()
    );
    Ok(Arc::new(BackendRepositoryRouter::new(
        cfg.clone(),
        repository_config(&cfg, session_scope),
        user_agent,
    )?))
}

async fn run_stdio_entry(cfg: AppConfig, session_scope: Option<SessionOriginScope>) -> Result<()> {
    let cwd = std::env::current_dir()
        .map(|dir| dir.to_string_lossy().into_owned())
        .unwrap_or_default();

    #[cfg(unix)]
    if session_scope.is_none() && cfg.mcp.use_central_server {
        use tokio::net::UnixStream;

        let socket_path = PathBuf::from(&cfg.mcp.central_socket_path);
        let connect_deadline = std::time::Duration::from_millis(cfg.mcp.central_connect_timeout_ms);
        match tokio::time::timeout(connect_deadline, UnixStream::connect(&socket_path)).await {
            Ok(Ok(stream)) => {
                let route_deadline = moraine_mcp_core::private_route_deadline(&cfg)?;
                match moraine_mcp_core::negotiate_private_route(stream, &cwd, route_deadline).await {
                    PrivateRouteNegotiation::Accepted(connection) => {
                        debug!(
                            "proxying stdio to central MCP server at {}",
                            socket_path.display()
                        );
                        return moraine_mcp_core::run_proxy(connection).await;
                    }
                    PrivateRouteNegotiation::Rejected { message } => {
                        return Err(anyhow!(message))
                            .context("central backend rejected the project route");
                    }
                    PrivateRouteNegotiation::Incompatible { reason } => warn!(
                        "central MCP server at {} cannot negotiate project routing ({}); using embedded server",
                        socket_path.display(),
                        reason
                    ),
                }
            }
            Ok(Err(error)) => warn!(
                "central MCP server unreachable at {} ({}); using embedded server",
                socket_path.display(),
                error
            ),
            Err(_) => warn!(
                "central MCP server connect timed out at {} after {}ms; using embedded server",
                socket_path.display(),
                cfg.mcp.central_connect_timeout_ms
            ),
        }
    }

    let cfg = Arc::new(cfg);
    let router = backend_router(cfg.clone(), session_scope, "moraine-mcp")?;
    let backend = router
        .repository_for_project_dir(Some(&cwd))
        .await
        .context("failed to construct embedded conversation repository")?;
    let repository = backend.repository().clone();
    drop(backend);
    drop(router);
    moraine_mcp_core::run_stdio_with_repository(Arc::unwrap_or_clone(cfg), repository).await
}

async fn run_backend(
    cfg: AppConfig,
    socket_path: PathBuf,
    host: String,
    port: u16,
    static_dir: PathBuf,
) -> Result<()> {
    validate_backend_bind_policy(&host, cfg.backend.auth_token.as_deref())?;
    let host = normalize_listener_bind(host);
    // This is the daemon mode's sole routing/factory owner. Both services
    // receive clones of this exact router Arc; each selected backend therefore
    // shares one checked client, repository, and cache set across MCP and HTTP.
    let cfg = Arc::new(cfg);
    let router = backend_router(cfg.clone(), None, "moraine-backend")
        .context("failed to build backend repository router")?;
    router
        .default_repository()
        .await
        .context("failed to build default backend conversation repository")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut services = JoinSet::new();

    let socket_cfg = cfg.clone();
    let socket_router = router.clone();
    let socket_shutdown = shutdown_rx.clone();
    services.spawn(async move {
        let result = moraine_mcp_core::run_socket_with_router(
            socket_cfg,
            socket_router,
            socket_path,
            wait_for_shutdown(socket_shutdown),
        )
        .await;
        ("MCP socket service", result)
    });

    services.spawn(async move {
        let result = moraine_monitor_core::run_server_with_router(
            router,
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
    fn backend_bind_policy_allows_explicit_loopback_without_token() {
        for bind in ["127.0.0.1", "127.42.0.9", "::1", "[::1]"] {
            validate_backend_bind_policy(bind, None)
                .unwrap_or_else(|error| panic!("loopback `{bind}` rejected: {error:#}"));
        }
    }

    #[test]
    fn listener_bind_normalizes_bare_ipv6_without_changing_other_hosts() {
        assert_eq!(normalize_listener_bind("::1".to_string()), "[::1]");
        assert_eq!(normalize_listener_bind("::".to_string()), "[::]");
        assert_eq!(normalize_listener_bind("[::1]".to_string()), "[::1]");
        assert_eq!(
            normalize_listener_bind("127.0.0.1".to_string()),
            "127.0.0.1"
        );
        assert_eq!(
            normalize_listener_bind("localhost".to_string()),
            "localhost"
        );
    }

    #[test]
    fn backend_bind_policy_rejects_non_loopback_and_ambiguous_hosts_without_token() {
        for bind in [
            "0.0.0.0",
            "::",
            "[::]",
            "192.168.1.20",
            "203.0.113.10",
            "2001:db8::1",
            "::ffff:127.0.0.1",
            "localhost",
            "example.test",
            "",
            "[::1",
            "::1]",
            "127.0.0.1 ",
        ] {
            let error = validate_backend_bind_policy(bind, None)
                .expect_err("non-loopback or ambiguous bind must require a token");
            let message = error.to_string();
            assert!(message.contains(bind), "bind missing from error: {message}");
            assert!(
                message.contains("backend.auth_token"),
                "token key missing from error: {message}"
            );
            assert!(
                message.contains("before binding"),
                "pre-bind refusal missing from error: {message}"
            );
        }
    }

    #[test]
    fn backend_bind_policy_treats_empty_and_whitespace_tokens_as_missing() {
        for token in [None, Some(""), Some(" "), Some(" \t\n")] {
            validate_backend_bind_policy("0.0.0.0", token)
                .expect_err("empty token must not permit a non-loopback bind");
        }
    }

    #[test]
    fn backend_bind_policy_accepts_nonempty_token_as_startup_prerequisite() {
        for bind in ["0.0.0.0", "::", "192.0.2.10", "localhost"] {
            validate_backend_bind_policy(bind, Some("sandbox-only-guard-token"))
                .unwrap_or_else(|error| panic!("guarded bind `{bind}` rejected: {error:#}"));
        }
    }

    #[test]
    fn non_loopback_without_token_refuses_before_backend_services_start() {
        let probe = TcpListener::bind(("127.0.0.1", 0)).expect("reserve probe port");
        let port = probe.local_addr().expect("probe address").port();
        drop(probe);
        let socket_path = short_socket_path();
        let _ = fs::remove_file(&socket_path);
        let static_dir = temp_path("pre-bind-refusal-static");

        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let mut cfg = AppConfig::default();
        cfg.backend.bind = "0.0.0.0".to_string();
        let error = runtime
            .block_on(run_backend(
                cfg,
                socket_path.clone(),
                "0.0.0.0".to_string(),
                port,
                static_dir,
            ))
            .expect_err("unguarded non-loopback bind must refuse startup");

        let message = format!("{error:#}");
        assert!(message.contains("0.0.0.0"));
        assert!(message.contains("backend.auth_token"));
        assert!(message.contains("before binding"));
        assert!(
            !socket_path.exists(),
            "MCP socket must not be created before policy refusal"
        );
        let rebound = TcpListener::bind(("0.0.0.0", port))
            .expect("HTTP port must remain available after pre-bind refusal");
        drop(rebound);
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
