use anyhow::Result;
use std::path::PathBuf;

pub use moraine_config::{AppConfig, ClickHouseConfig};

pub fn load_config(raw_path: Option<PathBuf>) -> Result<AppConfig> {
    let resolved = moraine_config::resolve_monitor_config_path(raw_path);
    moraine_config::load_resolved_config(resolved).map(|(_, config)| config)
}
