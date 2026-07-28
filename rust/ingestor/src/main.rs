mod clickhouse;
mod config;
mod ingestor;
mod model;
mod normalize;

use crate::config::{load_config, resolve_config_path};
use crate::ingestor::run_ingestor;
use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

fn parse_config_path() -> Option<PathBuf> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;

    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(value) = args.next() {
                config_path = Some(PathBuf::from(value));
            }
        }
    }

    config_path
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let config_path = resolve_config_path(parse_config_path());
    let config_display = config_path.display().to_string();
    let (_, config) = load_config(config_path)
        .with_context(|| format!("failed to load config {config_display}"))?;

    run_ingestor(config).await
}
