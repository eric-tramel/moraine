mod clickhouse;
mod config;
mod ingestor;
mod model;
mod normalize;

use crate::config::load_config;
use crate::ingestor::run_ingestor;
use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

fn parse_config_path() -> PathBuf {
    let mut args = std::env::args().skip(1);
    let mut config_path = default_config_path();

    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(value) = args.next() {
                config_path = PathBuf::from(value);
            }
        }
    }

    config_path
}

fn default_config_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let path = PathBuf::from(home).join(".cortex").join("config.toml");
        if path.exists() {
            return path;
        }
    }

    PathBuf::from("config/cortex.toml")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let config_path = parse_config_path();
    let config = load_config(&config_path)
        .with_context(|| format!("failed to load config {}", config_path.display()))?;

    run_ingestor(config).await
}
