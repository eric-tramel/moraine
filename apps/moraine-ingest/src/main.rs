mod cli;

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
    let config = moraine_config::load_config(&args.config_path)
        .with_context(|| format!("failed to load config {}", args.config_path.display()))?;

    moraine_ingest_core::run_ingestor(config).await
}
