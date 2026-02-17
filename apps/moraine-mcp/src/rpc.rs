use anyhow::Result;
use moraine_config::AppConfig;

pub async fn run(cfg: AppConfig) -> Result<()> {
    moraine_mcp_core::run_stdio(cfg).await
}
