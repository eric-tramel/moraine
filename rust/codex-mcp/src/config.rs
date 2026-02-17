use anyhow::Result;
use std::path::{Path, PathBuf};

pub use moraine_config::{AppConfig, Bm25Config, ClickHouseConfig, McpConfig};

pub fn resolve_config_path(raw_path: Option<PathBuf>) -> PathBuf {
    moraine_config::resolve_mcp_config_path(raw_path)
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig> {
    moraine_config::load_config(path)
}
