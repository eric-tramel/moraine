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
    let config_display = args.config_path.display().to_string();
    let (_, config) = moraine_config::load_resolved_config(args.config_path)
        .with_context(|| format!("failed to load config {config_display}"))?;

    moraine_ingest_core::run_ingestor(config).await
}
