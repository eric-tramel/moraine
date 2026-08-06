mod cli;

use anyhow::{Context, Result};
use moraine_clickhouse::QueryRuntime;
use std::future::Future;
use tracing_subscriber::EnvFilter;

async fn run_and_drain<T>(runtime: QueryRuntime, future: impl Future<Output = T>) -> T {
    let result = future.await;
    runtime.close_and_drain().await;
    result
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args = cli::parse_args();
    let config_display = args.config_path.display().to_string();
    let (_, config) = moraine_config::load_resolved_config(args.config_path)
        .with_context(|| format!("failed to load config {config_display}"))?;

    let query_runtime = QueryRuntime::new();
    run_and_drain(
        query_runtime.clone(),
        moraine_ingest_core::run_ingestor(config, query_runtime),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::run_and_drain;
    use moraine_clickhouse::{QueryCause, QueryOwner, QueryRuntime, QueryWorkload};

    #[tokio::test]
    async fn runtime_root_drains_after_ingestor_error() {
        let runtime = QueryRuntime::new();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Background)
            .expect("create active ingest owner");

        let result: Result<(), &'static str> =
            run_and_drain(runtime.clone(), async { Err("ingestor failed") }).await;

        assert_eq!(result, Err("ingestor failed"));
        assert!(runtime.is_closing());
        assert_eq!(runtime.active_owner_count(), 0);
        assert_eq!(owner.cause(), Some(QueryCause::Shutdown));
    }
}
