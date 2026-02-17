use anyhow::Result;
use std::path::PathBuf;

pub use moraine_config::{AppConfig, ClickHouseConfig};

pub fn load_config(raw_path: Option<PathBuf>) -> Result<AppConfig> {
    let path = moraine_config::resolve_config_path(raw_path);
    moraine_config::load_config(path)
}
