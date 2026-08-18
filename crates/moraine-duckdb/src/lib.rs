//! Experimental single-process DuckDB storage for Moraine.
//!
//! This crate is deliberately a leaf adapter. The public crates define the
//! read and ingest contracts; only the unified `moraine-mcp` binary links the
//! bundled DuckDB library and owns a store for its complete lifetime.

mod connection;
mod error;
mod migrations;
mod model;
mod read;
mod repository;
mod security;
mod write;

pub use connection::{DuckDbStore, DuckDbStoreOptions};
pub use error::{DuckDbError, DuckDbResult};
pub use migrations::{bundled_migrations, AppliedMigration, Migration};

pub async fn run_embedded_ingest(
    config: moraine_config::AppConfig,
    query_runtime: moraine_ingest_core::QueryRuntime,
    store: std::sync::Arc<DuckDbStore>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let semantic_store: std::sync::Arc<dyn moraine_ingest_core::store::IngestStore> = store;
    moraine_ingest_core::run_ingestor_with_store(config, query_runtime, semantic_store, shutdown)
        .await
}
