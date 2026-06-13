use crate::cli::ServeMode;
use anyhow::{Context, Result};
use moraine_config::AppConfig;
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
/// Both stdio arms go through `run_mcp_entry`, which first resolves
/// per-project routing for the process cwd: a cwd routed to a non-default
/// backend serves embedded against that backend regardless of the central
/// toggle. With no route it honors `use_central_server` exactly as before.
pub fn run(cfg: AppConfig, serve_mode: ServeMode) -> Result<()> {
    match serve_mode {
        ServeMode::Socket => {
            let socket = PathBuf::from(&cfg.mcp.central_socket_path);
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build multi-threaded runtime")?;
            rt.block_on(moraine_mcp_core::run_socket(cfg, socket))
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
            rt.block_on(moraine_mcp_core::run_mcp_entry(cfg))
        }
    }
}
