use duckdb::{params, Connection};
use moraine_duckdb::{DuckDbError, DuckDbStore, DuckDbStoreOptions};

async fn initialized_store() -> (tempfile::TempDir, DuckDbStoreOptions) {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());
    let store = DuckDbStore::open(options.clone())
        .await
        .expect("initialize store");
    store.close().await.expect("close initialized store");
    (root, options)
}

#[tokio::test]
async fn checksum_mismatch_is_rejected_without_rewriting_the_ledger() {
    let (_root, options) = initialized_store().await;
    let connection = Connection::open(&options.database_path).expect("open database directly");
    connection
        .execute(
            "UPDATE moraine_schema_migrations SET checksum = ? WHERE version = 1",
            params!["tampered"],
        )
        .expect("tamper checksum");
    drop(connection);

    let error = match DuckDbStore::open(options.clone()).await {
        Ok(_) => panic!("checksum mismatch must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        DuckDbError::MigrationChecksum { version: 1 }
    ));
    let connection = Connection::open(options.database_path).expect("reopen database directly");
    let checksum: String = connection
        .query_row(
            "SELECT checksum FROM moraine_schema_migrations WHERE version = 1",
            [],
            |row| row.get(0),
        )
        .expect("read checksum");
    assert_eq!(checksum, "tampered");
}

#[tokio::test]
async fn newer_schema_is_rejected_without_writes() {
    let (_root, options) = initialized_store().await;
    let connection = Connection::open(&options.database_path).expect("open database directly");
    connection
        .execute(
            "INSERT INTO moraine_schema_migrations VALUES (2, '002_future.sql', 'future', current_timestamp)",
            [],
        )
        .expect("insert future migration");
    drop(connection);

    let error = match DuckDbStore::open(options.clone()).await {
        Ok(_) => panic!("newer schema must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        DuckDbError::NewerSchema {
            database: 2,
            supported: 1
        }
    ));
    let connection = Connection::open(options.database_path).expect("reopen database directly");
    let count: u64 = connection
        .query_row(
            "SELECT count(*) FROM moraine_schema_migrations",
            [],
            |row| row.get(0),
        )
        .expect("count migrations");
    assert_eq!(count, 2);
}

#[tokio::test]
async fn foreign_database_is_rejected() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());
    std::fs::create_dir_all(
        options
            .database_path
            .parent()
            .expect("database parent directory"),
    )
    .expect("create database parent");
    let connection = Connection::open(&options.database_path).expect("create foreign database");
    connection
        .execute_batch("CREATE TABLE unrelated(value INTEGER)")
        .expect("create foreign table");
    drop(connection);

    let error = match DuckDbStore::open(options).await {
        Ok(_) => panic!("foreign database must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, DuckDbError::MigrationInvariant(_)));
}

#[tokio::test]
async fn empty_partial_ledger_is_rejected() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());
    std::fs::create_dir_all(
        options
            .database_path
            .parent()
            .expect("database parent directory"),
    )
    .expect("create database parent");
    let connection = Connection::open(&options.database_path).expect("create partial database");
    connection
        .execute_batch(
            "CREATE TABLE moraine_schema_migrations (version UINTEGER, filename VARCHAR, checksum VARCHAR, applied_at TIMESTAMP)",
        )
        .expect("create empty ledger");
    drop(connection);

    let error = match DuckDbStore::open(options).await {
        Ok(_) => panic!("partial ledger must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, DuckDbError::MigrationInvariant(_)));
}
