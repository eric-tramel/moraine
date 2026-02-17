use anyhow::Result;
use std::path::Path;

pub use moraine_config::{AppConfig, ClickHouseConfig, IngestConfig, IngestSource};

pub fn expand_path(path: &str) -> String {
    moraine_config::expand_path(path)
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    moraine_config::load_config(path)
}
