use std::fs;

use moraine_duckdb::{DuckDbError, DuckDbStore, DuckDbStoreOptions};

#[tokio::test]
async fn fresh_open_migrates_and_cleanly_reopens() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());

    let store = DuckDbStore::open(options.clone())
        .await
        .expect("fresh store opens");
    assert_eq!(store.schema_version().expect("schema version"), 1);
    assert_eq!(store.applied_migrations().len(), 1);
    assert_eq!(store.reader_count(), 2);
    store.close().await.expect("clean close");
    drop(store);

    let reopened = DuckDbStore::open(options)
        .await
        .expect("closed store reopens");
    assert_eq!(reopened.schema_version().expect("schema version"), 1);
    reopened.close().await.expect("second clean close");
}

#[tokio::test]
async fn lifetime_owner_rejects_a_second_store() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());
    let first = DuckDbStore::open(options.clone())
        .await
        .expect("first store opens");

    let error = match DuckDbStore::open(options).await {
        Ok(_) => panic!("second store must not open"),
        Err(error) => error,
    };
    assert!(matches!(error, DuckDbError::AlreadyOwned));
    first.close().await.expect("clean close");
}

#[tokio::test]
async fn unsafe_paths_are_rejected_before_database_creation() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let mut options = DuckDbStoreOptions::for_runtime_root(root.path());
    options.database_path = root.path().join("../escaped.duckdb");

    let error = match DuckDbStore::open(options).await {
        Ok(_) => panic!("parent traversal must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, DuckDbError::InvalidPath(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_storage_component_is_rejected() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().expect("temporary runtime root");
    let outside = tempfile::tempdir().expect("outside directory");
    symlink(outside.path(), root.path().join("duckdb")).expect("create symlink");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());

    let error = match DuckDbStore::open(options).await {
        Ok(_) => panic!("symlinked store path must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, DuckDbError::InvalidPath(_)));
    assert!(fs::read_dir(outside.path())
        .expect("read outside")
        .next()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_runtime_root_is_rejected() {
    use std::os::unix::fs::symlink;

    let parent = tempfile::tempdir().expect("temporary parent");
    let outside = tempfile::tempdir().expect("outside directory");
    let root = parent.path().join("runtime-link");
    symlink(outside.path(), &root).expect("create root symlink");

    let error = match DuckDbStore::open(DuckDbStoreOptions::for_runtime_root(&root)).await {
        Ok(_) => panic!("symlinked runtime root must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, DuckDbError::InvalidPath(_)));
    assert!(fs::read_dir(outside.path())
        .expect("read outside")
        .next()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn database_and_directories_are_private() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());
    let database = options.database_path.clone();
    let temp = options.temp_directory.clone();
    let store = DuckDbStore::open(options).await.expect("store opens");

    assert_eq!(
        fs::metadata(&database)
            .expect("database metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(temp)
            .expect("temp metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    store.close().await.expect("clean close");
}
