use std::fs;

use moraine_duckdb::{DuckDbStore, DuckDbStoreOptions};
use moraine_ingest_core::model::Checkpoint;
use moraine_ingest_core::store::{
    IngestStore, IngestWrite, PreparedIngestBatch, PublicationTransition,
};
use serde_json::json;

fn transition() -> PublicationTransition {
    let checkpoint = Checkpoint {
        source_name: "codex".to_string(),
        source_file: "/fixtures/backup.jsonl".to_string(),
        source_generation: 1,
        last_offset: 128,
        checkpoint_revision: 1,
        operation_id: "backup-publication".to_string(),
        status: "ready".to_string(),
        ..Checkpoint::default()
    };
    PublicationTransition {
        source_name: checkpoint.source_name.clone(),
        source_file: checkpoint.source_file.clone(),
        source_generation: 1,
        operation_id: checkpoint.operation_id.clone(),
        checkpoint_revision: 1,
        publication_revision: 1,
        cache_epoch: 1,
        checkpoint,
    }
}

fn batch() -> PreparedIngestBatch {
    PreparedIngestBatch {
        event_rows: vec![json!({
            "event_uid": "backup-event",
            "event_version": 1,
            "source_name": "codex",
            "source_file": "/fixtures/backup.jsonl",
            "source_generation": 1,
            "source_line_no": 1,
            "session_id": "backup-session",
            "turn_id": "backup-turn",
            "event_kind": "message",
            "event_ts": "2026-08-18 03:04:36.000",
            "text_content": "cold backup retrieval digest",
            "payload_json": "{\"text\":\"cold backup retrieval digest\"}",
        })],
        checkpoint: Some(transition().checkpoint),
        ..PreparedIngestBatch::default()
    }
}

async fn retrieval_digest(store: &std::sync::Arc<DuckDbStore>) -> String {
    let head = store
        .clone()
        .publication_head("codex", "/fixtures/backup.jsonl")
        .await
        .expect("publication head");
    let ids = store
        .clone()
        .current_search_event_ids("retrieval", 10)
        .await
        .expect("search ids");
    let count = store
        .clone()
        .current_event_count()
        .await
        .expect("current event count");
    format!("{head:?}:{ids:?}:{count}")
}

#[tokio::test]
async fn cold_backup_restore_reopens_with_identical_retrieval_digest() {
    let source_root = tempfile::tempdir().expect("source runtime root");
    let source_options = DuckDbStoreOptions::for_runtime_root(source_root.path());
    let source_database = source_options.database_path.clone();
    let store = DuckDbStore::open(source_options.clone())
        .await
        .expect("open source store");
    store
        .clone()
        .write(IngestWrite::PublishReplay {
            transition: transition(),
            batch: Box::new(batch()),
        })
        .await
        .expect("publish backup fixture");
    let before = retrieval_digest(&store).await;
    store.close().await.expect("checkpoint source store");
    drop(store);

    let wal = source_database.with_file_name(format!(
        "{}.wal",
        source_database.file_name().unwrap().to_string_lossy()
    ));
    assert!(!wal.exists(), "clean checkpoint must remove the live WAL");
    let lock_probe = DuckDbStore::open(source_options)
        .await
        .expect("closed source owner lock is free");
    lock_probe.close().await.expect("close lock probe");
    drop(lock_probe);

    let restore_root = tempfile::tempdir().expect("restore runtime root");
    let restore_options = DuckDbStoreOptions::for_runtime_root(restore_root.path());
    fs::create_dir_all(
        restore_options
            .database_path
            .parent()
            .expect("restore database parent"),
    )
    .expect("create private restore directory");
    fs::copy(&source_database, &restore_options.database_path).expect("copy closed database");
    let restored = DuckDbStore::open(restore_options)
        .await
        .expect("open restored store");
    assert_eq!(retrieval_digest(&restored).await, before);
    restored.close().await.expect("close restored store");
}
