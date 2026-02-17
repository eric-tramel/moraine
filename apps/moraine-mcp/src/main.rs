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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args = cli::parse_args();
    let cfg = moraine_config::load_config(&args.config_path)
        .with_context(|| format!("failed to load config {}", args.config_path.display()))?;

    rpc::run(cfg).await
}
