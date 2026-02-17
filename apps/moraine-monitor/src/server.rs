use anyhow::Result;
use moraine_config::AppConfig;
use std::path::PathBuf;

pub async fn run(cfg: AppConfig, host: String, port: u16, static_dir: PathBuf) -> Result<()> {
    moraine_monitor_core::run_server(cfg, host, port, static_dir).await
}
