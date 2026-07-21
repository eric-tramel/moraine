use moraine_ingest_core::model::{Checkpoint, CheckpointLifecycle};
use serde_json::json;

#[test]
fn legacy_checkpoint_payloads_default_publication_fields_fail_closed() {
    let checkpoint: Checkpoint = serde_json::from_value(json!({
        "source_name": "codex",
        "source_file": "/fixtures/session.jsonl",
        "source_inode": 41,
        "source_generation": 1,
        "last_offset": 128,
        "last_line_no": 3,
        "status": "active"
    }))
    .expect("legacy checkpoint remains readable during a rolling schema upgrade");

    assert_eq!(checkpoint.checkpoint_revision, 0);
    assert!(checkpoint.operation_id.is_empty());
    assert!(!checkpoint.final_scan_complete);
    assert!(!checkpoint.compatibility_prepared);
    assert!(!checkpoint.backend_caught_up);
    assert!(checkpoint.block_reason.is_empty());
    assert!(checkpoint.append_batch_id.is_empty());
    assert_eq!(checkpoint.cache_epoch, 0);
}

#[test]
fn causal_checkpoint_payload_round_trips_as_one_transition() {
    let checkpoint = Checkpoint {
        source_name: "codex".to_string(),
        source_file: "/fixtures/session.jsonl".to_string(),
        source_inode: 73,
        source_generation: 2,
        last_offset: 2_048,
        last_line_no: 17,
        status: CheckpointLifecycle::Error.to_string(),
        cursor_json: r#"{"rowid":17}"#.to_string(),
        source_fingerprint: 101,
        schema_fingerprint: 202,
        policy_fingerprint: "normalization-v2".to_string(),
        checkpoint_revision: 9,
        operation_id: "cp-retry-stable".to_string(),
        scan_inode: 73,
        scan_boundary: 2_048,
        final_scan_complete: false,
        block_reason: "candidate projection incomplete".to_string(),
        compatibility_prepared: false,
        backend_caught_up: true,
        append_batch_id: "append-abc".to_string(),
        cache_epoch: 8,
    };

    let encoded = serde_json::to_value(&checkpoint).expect("serialize causal checkpoint");
    let decoded: Checkpoint =
        serde_json::from_value(encoded).expect("deserialize causal checkpoint");

    assert_eq!(decoded.source_generation, 2);
    assert_eq!(decoded.checkpoint_revision, 9);
    assert_eq!(decoded.operation_id, "cp-retry-stable");
    assert_eq!(decoded.status, "error");
    assert_eq!(decoded.lifecycle().unwrap(), CheckpointLifecycle::Error);
    assert_eq!(decoded.block_reason, "candidate projection incomplete");
    assert!(!decoded.final_scan_complete);
    assert!(!decoded.compatibility_prepared);
    assert!(decoded.backend_caught_up);
    assert_eq!(decoded.cache_epoch, 8);
}

#[test]
fn unknown_checkpoint_lifecycle_fails_deserialization() {
    let error = serde_json::from_value::<Checkpoint>(json!({
        "source_name": "codex",
        "source_file": "/fixtures/session.jsonl",
        "source_inode": 41,
        "source_generation": 2,
        "last_offset": 128,
        "last_line_no": 3,
        "status": "paused"
    }))
    .expect_err("unknown lifecycle must fail closed");

    assert!(error.to_string().contains("unknown variant `paused`"));
}

#[test]
fn checkpoint_lifecycle_api_preserves_the_existing_wire_format() {
    let mut checkpoint = Checkpoint {
        source_name: "codex".to_string(),
        source_file: "/fixtures/session.jsonl".to_string(),
        ..Checkpoint::default()
    };
    checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);

    assert_eq!(
        checkpoint.lifecycle().unwrap(),
        CheckpointLifecycle::Replaying
    );
    assert_eq!(
        serde_json::to_value(&checkpoint).unwrap()["status"],
        json!("replaying")
    );
    assert!("paused".parse::<CheckpointLifecycle>().is_err());
}
