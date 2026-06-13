use crate::cli::ServeMode;
use anyhow::{bail, Context, Result};
use moraine_config::AppConfig;
use moraine_mcp_core::SessionOriginScope;
use std::path::PathBuf;
use tokio::runtime::Builder;

/// Build the appropriate tokio runtime for the chosen mode and drive it.
///
/// - `--serve socket`: the shared central server. Multi-threaded runtime so it
///   can fan many concurrent connections across cores.
/// - default (stdio) with central enabled: a thin proxy (or embedded fallback)
///   for one agent session. A current-thread runtime is sufficient — the agent
///   issues one RPC at a time — and keeps the per-session footprint to ~1
///   thread, which is the whole point at hundreds of sessions.
/// - default (stdio) with central disabled: the embedded server on a
///   multi-threaded runtime (pre-central behavior).
///
/// Both stdio arms go through `run_mcp_entry`, which first resolves per-project
/// routing for the process cwd: a cwd routed to a non-default backend serves
/// embedded against that backend regardless of the central toggle. With no
/// route it honors `use_central_server`.
///
/// `--project-only` (`session_scope`) is orthogonal to routing: it forces an
/// embedded server (the scope can't ride the shared central proxy) but still
/// honors the cwd→backend route, so a scoped session queries whatever backend
/// its directory routes to. It is therefore threaded into `run_mcp_entry`, and
/// only the unscoped + central-enabled path keeps the lightweight
/// current-thread proxy runtime.
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
        // Only the unscoped, central-enabled path can become a thin proxy, so
        // it is the only one that gets the lightweight current-thread runtime.
        ServeMode::Stdio if session_scope.is_none() && cfg.mcp.use_central_server => {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build current-thread runtime")?;
            rt.block_on(moraine_mcp_core::run_mcp_entry(cfg, None))
        }
        ServeMode::Stdio => {
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            rt.block_on(moraine_mcp_core::run_mcp_entry(cfg, session_scope))
        }
    }
}
