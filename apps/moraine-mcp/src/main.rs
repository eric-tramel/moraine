mod cli;
mod format {
    pub mod prose;
}
mod rpc;
mod sql {
    pub mod builders;
    pub mod logging;
}
mod tokenize;
mod tools {
    pub mod open;
    pub mod search;
}

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

// No `#[tokio::main]`: the runtime flavor depends on the mode. The central
// server wants a multi-threaded runtime, but a thin per-session proxy only
// needs a single thread — that single-thread choice is what keeps the
// per-agent footprint small at scale. `rpc::run` builds the right runtime.
fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        // Logs MUST go to stderr: stdout is the MCP JSON-RPC stream (for the
        // embedded server and the proxy), and any log line written there would
        // corrupt the protocol for the connected agent.
        .with_writer(std::io::stderr)
        .init();

    let args = cli::parse_args()?;
    if args.help {
        cli::usage();
        return Ok(());
    }
    let mut cfg = moraine_config::load_config(&args.config_path)
        .with_context(|| format!("failed to load config {}", args.config_path.display()))?;

    // An explicit --socket overrides the configured central socket path, so the
    // daemon and its clients can be pointed at the same path from the launcher.
    if let Some(socket) = &args.socket_override {
        cfg.mcp.central_socket_path = socket.to_string_lossy().to_string();
    }

    // The scope is anchored to the launch directory, so it must be resolved
    // before anything has a chance to chdir.
    let session_scope = if args.project_only {
        Some(moraine_mcp_core::project_scope_from_launch_dir()?)
    } else {
        None
    };

    rpc::run(
        cfg,
        args.serve_mode,
        session_scope,
        rpc::BackendOptions {
            host: args.host,
            port: args.port,
            static_dir: args.static_dir,
        },
    )
}
