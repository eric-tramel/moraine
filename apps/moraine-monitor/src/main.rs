mod api {
    pub mod analytics;
    pub mod health;
    pub mod status;
    pub mod tables;
    pub mod web_searches;
}
mod cli;
mod server;
mod sql_safety;
mod static_assets;

use anyhow::{Context, Result};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = cli::parse_args();
    let cfg = moraine_config::load_config(&args.config_path)
        .with_context(|| format!("failed to load config {}", args.config_path.display()))?;

    let host = args.host.unwrap_or_else(|| cfg.monitor.host.clone());
    let port = args.port.unwrap_or(cfg.monitor.port);

    server::run(cfg, host, port, args.static_dir).await
}
