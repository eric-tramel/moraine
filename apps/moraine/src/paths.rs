use anyhow::{Context, Result};
use moraine_config::AppConfig;
use std::fs;
use std::path::PathBuf;

#[derive(Clone)]
pub(crate) struct RuntimePaths {
    pub(crate) root: PathBuf,
    pub(crate) logs_dir: PathBuf,
    pub(crate) pids_dir: PathBuf,
    pub(crate) clickhouse_root: PathBuf,
    pub(crate) clickhouse_config: PathBuf,
    pub(crate) clickhouse_users: PathBuf,
    pub(crate) service_bin_dir: PathBuf,
    pub(crate) managed_clickhouse_dir: PathBuf,
}

pub(crate) fn runtime_paths(cfg: &AppConfig) -> RuntimePaths {
    let root = PathBuf::from(&cfg.runtime.root_dir);
    let clickhouse_root = root.join("clickhouse");

    RuntimePaths {
        root,
        logs_dir: PathBuf::from(&cfg.runtime.logs_dir),
        pids_dir: PathBuf::from(&cfg.runtime.pids_dir),
        clickhouse_config: clickhouse_root.join("config.xml"),
        clickhouse_users: clickhouse_root.join("users.xml"),
        clickhouse_root,
        service_bin_dir: PathBuf::from(&cfg.runtime.service_bin_dir),
        managed_clickhouse_dir: PathBuf::from(&cfg.runtime.managed_clickhouse_dir),
    }
}

pub(crate) fn ensure_runtime_dirs(paths: &RuntimePaths) -> Result<()> {
    fs::create_dir_all(&paths.root)
        .with_context(|| format!("failed to create {}", paths.root.display()))?;
    fs::create_dir_all(&paths.logs_dir)
        .with_context(|| format!("failed to create {}", paths.logs_dir.display()))?;
    fs::create_dir_all(&paths.pids_dir)
        .with_context(|| format!("failed to create {}", paths.pids_dir.display()))?;

    fs::create_dir_all(paths.clickhouse_root.join("data"))?;
    fs::create_dir_all(paths.clickhouse_root.join("tmp"))?;
    fs::create_dir_all(paths.clickhouse_root.join("log"))?;
    fs::create_dir_all(paths.clickhouse_root.join("user_files"))?;
    fs::create_dir_all(paths.clickhouse_root.join("format_schemas"))?;

    Ok(())
}

pub(crate) fn load_cfg(raw_config: Option<PathBuf>) -> Result<(PathBuf, AppConfig)> {
    let config_path = moraine_config::resolve_config_path(raw_config);
    let cfg = moraine_config::load_config(&config_path)
        .with_context(|| format!("failed to load config {}", config_path.display()))?;
    Ok((config_path, cfg))
}
