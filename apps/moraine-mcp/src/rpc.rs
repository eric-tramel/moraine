use crate::cli::ServeMode;
use anyhow::{bail, Context, Result};
use moraine_config::AppConfig;
use moraine_mcp_core::SessionOriginScope;
use std::path::PathBuf;
use tokio::runtime::Builder;
use tracing::debug;

/// Build the appropriate tokio runtime for the chosen mode and drive it.
///
/// - `--serve socket`: the shared central server. Multi-threaded runtime so it
///   can fan many concurrent connections across cores.
/// - stdio with `--project-only`: an embedded server carrying the session
///   scope. The central proxy forwards bytes verbatim and the central server
///   is shared across projects, so a scoped session can never be served
///   through it — scoped sessions always run embedded.
/// - default (stdio) with central enabled: a thin proxy (or embedded fallback)
///   for one agent session. A current-thread runtime is sufficient — the agent
///   issues one RPC at a time — and keeps the per-session footprint to ~1
///   thread, which is the whole point at hundreds of sessions.
/// - default (stdio) with central disabled: the legacy embedded server on a
///   multi-threaded runtime (byte-for-byte the pre-central behavior).
pub fn run(
    cfg: AppConfig,
    serve_mode: ServeMode,
    session_scope: Option<SessionOriginScope>,
) -> Result<()> {
    match serve_mode {
        ServeMode::Socket => {
            if session_scope.is_some() {
                bail!("--project-only cannot be combined with --serve socket: the shared central server serves sessions from every project");
            }
            let socket = PathBuf::from(&cfg.mcp.central_socket_path);
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            rt.block_on(moraine_mcp_core::run_socket(cfg, socket))
        }
        ServeMode::Stdio if session_scope.is_some() => {
            if cfg.mcp.use_central_server {
                debug!("--project-only set; serving embedded instead of proxying to the central MCP server");
            }
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            rt.block_on(moraine_mcp_core::run_stdio(cfg, session_scope))
        }
        ServeMode::Stdio if cfg.mcp.use_central_server => {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build current-thread runtime")?;
            rt.block_on(moraine_mcp_core::run_mcp_entry(cfg))
        }
        ServeMode::Stdio => {
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            rt.block_on(moraine_mcp_core::run_stdio(cfg, None))
        }
    }
}
