use moraine_duckdb::{DuckDbStore, DuckDbStoreOptions};
use moraine_ingest_core::model::Checkpoint;
use moraine_ingest_core::store::{
    IngestCommitKind, IngestStore, IngestWrite, PreparedIngestBatch, PublicationTransition,
};
use serde_json::json;

fn checkpoint(generation: u32, revision: u64, operation_id: &str) -> Checkpoint {
    Checkpoint {
        source_name: "codex".to_string(),
        source_file: "/fixtures/session.jsonl".to_string(),
        source_generation: generation,
        last_offset: revision * 100,
        checkpoint_revision: revision,
        operation_id: operation_id.to_string(),
        status: "ready".to_string(),
        ..Checkpoint::default()
    }
}

fn transition(generation: u32, revision: u64, operation_id: &str) -> PublicationTransition {
    PublicationTransition {
        source_name: "codex".to_string(),
        source_file: "/fixtures/session.jsonl".to_string(),
        source_generation: generation,
        operation_id: operation_id.to_string(),
        checkpoint_revision: revision,
        publication_revision: revision,
        cache_epoch: revision,
        checkpoint: checkpoint(generation, revision, operation_id),
    }
}

fn batch(generation: u32, uid: &str, text: &str, operation_id: &str) -> PreparedIngestBatch {
    PreparedIngestBatch {
        event_rows: vec![json!({
            "event_uid": uid,
            "event_version": 1,
            "source_name": "codex",
            "source_file": "/fixtures/session.jsonl",
            "source_generation": generation,
            "source_line_no": generation,
            "session_id": "session-1",
            "turn_id": "turn-1",
            "event_kind": "message",
            "event_ts": "2026-08-18 03:04:36.000",
            "text_content": text,
            "payload_json": format!("{{\"text\":\"{text}\"}}"),
        })],
        checkpoint: Some(checkpoint(generation, generation as u64, operation_id)),
        ..PreparedIngestBatch::default()
    }
}

#[tokio::test]
async fn replacement_publication_is_atomic_and_current_head_filtered() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let options = DuckDbStoreOptions::for_runtime_root(root.path());
    let store = DuckDbStore::open(options.clone())
        .await
        .expect("open store");

    store
        .clone()
        .write(IngestWrite::BeginReplay(transition(1, 1, "prepare-1")))
        .await
        .expect("prepare generation 1");
    assert_eq!(
        store
            .clone()
            .publication_head("codex", "/fixtures/session.jsonl")
            .await
            .expect("head query"),
        None
    );
    assert_eq!(store.clone().current_event_count().await.expect("count"), 0);

    let ack = store
        .clone()
        .write(IngestWrite::PublishReplay {
            transition: transition(1, 2, "publish-1"),
            batch: Box::new(batch(1, "event-old", "alpha old", "publish-1")),
        })
        .await
        .expect("publish generation 1");
    assert_eq!(ack.kind, IngestCommitKind::ReplayPublished);
    assert_eq!(
        store
            .clone()
            .publication_head("codex", "/fixtures/session.jsonl")
            .await
            .expect("published head"),
        Some((1, 2, 2))
    );
    assert_eq!(
        store
            .clone()
            .stored_event_count()
            .await
            .expect("stored count"),
        1
    );
    assert_eq!(
        store
            .clone()
            .current_event_count()
            .await
            .expect("event count"),
        1
    );
    assert!(store.clone().posting_count().await.expect("posting count") > 0);
    assert_eq!(
        store
            .clone()
            .current_search_event_ids("alpha", 10)
            .await
            .expect("search generation 1"),
        vec!["event-old"]
    );

    store
        .clone()
        .write(IngestWrite::BeginReplay(transition(2, 3, "prepare-2")))
        .await
        .expect("prepare generation 2");
    assert_eq!(
        store
            .clone()
            .current_search_event_ids("alpha", 10)
            .await
            .expect("old head remains readable while candidate is prepared"),
        vec!["event-old"]
    );

    store
        .clone()
        .write(IngestWrite::PublishReplay {
            transition: transition(2, 4, "publish-2"),
            batch: Box::new(batch(2, "event-new", "alpha new", "publish-2")),
        })
        .await
        .expect("publish generation 2");
    assert_eq!(
        store
            .clone()
            .publication_head("codex", "/fixtures/session.jsonl")
            .await
            .expect("head query"),
        Some((2, 4, 4))
    );
    assert_eq!(store.clone().current_event_count().await.expect("count"), 1);
    assert_eq!(
        store
            .clone()
            .current_search_event_ids("alpha", 10)
            .await
            .expect("search generation 2"),
        vec!["event-new"]
    );

    store.clone().shutdown().await.expect("clean shutdown");
    drop(store);
    let reopened = DuckDbStore::open(options).await.expect("reopen store");
    assert_eq!(
        reopened
            .clone()
            .current_search_event_ids("alpha", 10)
            .await
            .expect("search after reopen"),
        vec!["event-new"]
    );
    reopened.shutdown().await.expect("final shutdown");
}

#[tokio::test]
async fn same_generation_append_becomes_visible_only_after_durable_commit() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let store = DuckDbStore::open(DuckDbStoreOptions::for_runtime_root(root.path()))
        .await
        .expect("open store");
    store
        .clone()
        .write(IngestWrite::PublishReplay {
            transition: transition(1, 1, "publish-1"),
            batch: Box::new(batch(1, "event-1", "first", "publish-1")),
        })
        .await
        .expect("publish initial generation");

    let ack = store
        .clone()
        .write(IngestWrite::Append(batch(
            1,
            "event-2",
            "second searchable",
            "append-2",
        )))
        .await
        .expect("append commit");
    assert_eq!(ack.kind, IngestCommitKind::Append);
    assert_eq!(
        store
            .clone()
            .current_search_event_ids("searchable", 10)
            .await
            .expect("search append"),
        vec!["event-2"]
    );
    assert_eq!(store.clone().current_event_count().await.expect("count"), 2);
    store.shutdown().await.expect("clean shutdown");
}
