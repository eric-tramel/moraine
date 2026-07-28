use anyhow::Result;
use std::path::PathBuf;

pub use moraine_config::{
    AppConfig, ClickHouseConfig, IngestConfig, IngestSource, ResolvedConfigPath,
};

pub fn expand_path(path: &str) -> String {
    moraine_config::expand_path(path)
}

pub fn resolve_config_path(raw_path: Option<PathBuf>) -> ResolvedConfigPath {
    moraine_config::resolve_ingest_config_path(raw_path)
}

pub fn load_config(
    resolved: ResolvedConfigPath,
) -> Result<(moraine_config::LoadedConfigPath, AppConfig)> {
    moraine_config::load_resolved_config(resolved)
}
