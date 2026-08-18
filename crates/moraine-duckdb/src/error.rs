use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DuckDbError {
    #[error("invalid DuckDB storage path: {0}")]
    InvalidPath(String),
    #[error("DuckDB store is already owned by another Moraine process")]
    AlreadyOwned,
    #[error("DuckDB schema is newer than this Moraine build (database={database}, supported={supported})")]
    NewerSchema { database: u32, supported: u32 },
    #[error("DuckDB migration {version} checksum does not match the bundled migration")]
    MigrationChecksum { version: u32 },
    #[error("DuckDB migration ledger is inconsistent: {0}")]
    MigrationInvariant(String),
    #[error("failed to access DuckDB storage at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Database(#[from] duckdb::Error),
    #[error("DuckDB blocking job failed: {0}")]
    BlockingJob(String),
    #[error("DuckDB store is shutting down")]
    ShuttingDown,
}

pub type DuckDbResult<T> = Result<T, DuckDbError>;
