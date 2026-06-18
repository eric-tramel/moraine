use super::*;
use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, RowBatch};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

fn unique_opencode_db_path(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("moraine-opencode-poll-{name}-{suffix}.db"))
}

fn create_opencode_db(path: &PathBuf) -> Connection {
    let connection = Connection::open(path).expect("create opencode fixture db");
    connection
        .execute_batch(
            r#"
                CREATE TABLE session (
                  id text PRIMARY KEY,
                  project_id text NOT NULL,
                  parent_id text,
                  slug text NOT NULL,
                  directory text NOT NULL,
                  title text NOT NULL,
                  version text NOT NULL,
                  share_url text,
                  summary_additions integer DEFAULT 0,
                  summary_deletions integer DEFAULT 0,
                  summary_files integer DEFAULT 0,
                  summary_diffs text,
                  time_created integer NOT NULL,
                  time_updated integer NOT NULL,
                  workspace_id text,
                  path text,
                  agent text,
                  model text,
                  cost real DEFAULT 0 NOT NULL,
                  tokens_input integer DEFAULT 0 NOT NULL,
                  tokens_output integer DEFAULT 0 NOT NULL,
                  tokens_reasoning integer DEFAULT 0 NOT NULL,
                  tokens_cache_read integer DEFAULT 0 NOT NULL,
                  tokens_cache_write integer DEFAULT 0 NOT NULL,
                  metadata text
                );
                CREATE TABLE project (
                  id text PRIMARY KEY,
                  name text NOT NULL,
                  time_created integer NOT NULL,
                  time_updated integer NOT NULL
                );
                CREATE TABLE message (
                  id text PRIMARY KEY,
                  session_id text NOT NULL,
                  time_created integer NOT NULL,
                  time_updated integer NOT NULL,
                  data text NOT NULL
                );
                CREATE TABLE part (
                  id text PRIMARY KEY,
                  message_id text NOT NULL,
                  session_id text NOT NULL,
                  time_created integer NOT NULL,
                  time_updated integer NOT NULL,
                  data text NOT NULL
                );
                CREATE TABLE session_message (
                  id text PRIMARY KEY,
                  session_id text NOT NULL,
                  type text NOT NULL,
                  time_created integer NOT NULL,
                  time_updated integer NOT NULL,
                  data text NOT NULL,
                  seq integer NOT NULL
                );
                CREATE TABLE credential (
                  id text PRIMARY KEY,
                  value text NOT NULL,
                  time_created integer NOT NULL,
                  time_updated integer NOT NULL
                );
                "#,
        )
        .expect("create opencode tables");
    connection
}

fn seed_opencode_db(path: &PathBuf) -> Connection {
    let connection = create_opencode_db(path);
    connection
            .execute(
                "INSERT INTO session (
                   id, project_id, parent_id, slug, directory, title, version, time_created, time_updated,
                   share_url, summary_additions, summary_deletions, summary_files, summary_diffs,
                   workspace_id, path, agent, model, cost, tokens_input, tokens_output, tokens_reasoning,
                   tokens_cache_read, tokens_cache_write, metadata
                 ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, 0, 2, ?10, NULL, NULL, ?11, ?12, 0, 10, 4, 2, 3, 1, NULL)",
                rusqlite::params![
                    "ses_demo",
                    "proj_demo",
                    "demo",
                    "/work/opencode-demo",
                    "OpenCode DB fixture",
                    "0.0.0-test",
                    1780000000000_i64,
                    1780000004000_i64,
                    "https://opencode.example/share/ses_demo",
                    serde_json::to_string(&json!([{"path": "src/main.rs"}])).unwrap(),
                    "build",
                    serde_json::to_string(&json!({"id": "glm-5.2", "providerID": "zai-coding-plan"})).unwrap(),
                ],
            )
            .expect("insert session");
    connection
        .execute(
            "INSERT INTO project (id, name, time_created, time_updated) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "proj_demo",
                "OpenCode demo project",
                1780000000000_i64,
                1780000004000_i64,
            ],
        )
        .expect("insert project");

    let insert_message = |id: &str, created: i64, data: Value| {
        connection
                .execute(
                    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        id,
                        "ses_demo",
                        created,
                        created,
                        serde_json::to_string(&data).unwrap(),
                    ],
                )
                .expect("insert message");
    };
    insert_message(
        "msg_user",
        1780000001000_i64,
        json!({
            "role": "user",
            "summary": "Please inspect the project.",
            "time": {"created": 1780000001000_i64},
            "agent": "build"
        }),
    );
    insert_message(
        "msg_assistant",
        1780000001500_i64,
        json!({
            "role": "assistant",
            "parentID": "msg_user",
            "text": "I can inspect that.",
            "path": {"cwd": "/work/opencode-demo", "root": "/work/opencode-demo"},
            "modelID": "glm-5.2",
            "providerID": "zai-coding-plan",
            "tokens": {"input": 20, "output": 5, "reasoning": 1, "cache": {"read": 7, "write": 0}},
            "finish": "stop"
        }),
    );

    let insert_part = |id: &str, message_id: &str, created: i64, data: Value| {
        connection
                .execute(
                    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        id,
                        message_id,
                        "ses_demo",
                        created,
                        created,
                        serde_json::to_string(&data).unwrap(),
                    ],
                )
                .expect("insert part");
    };
    insert_part(
        "part_text",
        "msg_assistant",
        1780000002000_i64,
        json!({"type": "text", "text": "I can inspect that.", "time": {"start": 1780000002000_i64}}),
    );
    insert_part(
        "part_reasoning",
        "msg_assistant",
        1780000002100_i64,
        json!({"type": "reasoning", "text": "Need to list files.", "time": {"start": 1780000002100_i64}}),
    );
    insert_part(
        "part_tool_done",
        "msg_assistant",
        1780000002200_i64,
        json!({
            "type": "tool",
            "callID": "tool_done",
            "tool": "bash",
            "state": {
                "status": "completed",
                "input": {"cmd": "pwd"},
                "output": "/work/opencode-demo"
            }
        }),
    );
    insert_part(
        "part_tool_error",
        "msg_assistant",
        1780000002300_i64,
        json!({
            "type": "tool",
            "callID": "tool_error",
            "tool": "bash",
            "state": {
                "status": "error",
                "input": {"cmd": "cat missing.txt"},
                "error": "No such file or directory"
            }
        }),
    );
    insert_part(
        "part_step_finish",
        "msg_assistant",
        1780000002400_i64,
        json!({
            "type": "step-finish",
            "reason": "stop",
            "tokens": {"input": 20, "output": 5, "reasoning": 1, "cache": {"read": 7, "write": 0}},
            "cost": 0.001
        }),
    );
    connection
            .execute(
                "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    "part_tool_pending",
                    "msg_assistant",
                    "ses_demo",
                    1780000002500_i64,
                    1780000002500_i64,
                    serde_json::to_string(&json!({
                        "type": "tool",
                        "callID": "tool_pending",
                        "tool": "bash",
                        "state": {
                            "status": "pending",
                            "input": {"cmd": "ls"}
                        }
                    })).unwrap(),
                ],
            )
            .expect("insert pending part");
    connection
            .execute(
                "INSERT INTO session_message (id, session_id, type, time_created, time_updated, data, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    "sm_model",
                    "ses_demo",
                    "model-switched",
                    1780000003000_i64,
                    1780000003000_i64,
                    serde_json::to_string(&json!({"model": {"id": "glm-5.2", "providerID": "zai-coding-plan"}})).unwrap(),
                    1_i64,
                ],
            )
            .expect("insert session_message");
    connection
            .execute(
                "INSERT INTO credential (id, value, time_created, time_updated) VALUES ('cred_1', 'secret-token', 1, 1)",
                [],
            )
            .expect("insert credential");
    connection
}

fn opencode_sqlite_work(path: &Path) -> WorkItem {
    WorkItem {
        source_name: "opencode-sqlite-test".to_string(),
        harness: "opencode".to_string(),
        format: SOURCE_FORMAT_OPENCODE_SQLITE.to_string(),
        path: path.to_string_lossy().to_string(),
    }
}

async fn drain_batches(rx: &mut mpsc::Receiver<SinkMessage>) -> Vec<RowBatch> {
    let mut out = Vec::new();
    while let Ok(Some(SinkMessage::Batch(batch))) =
        timeout(Duration::from_millis(50), rx.recv()).await
    {
        out.push(batch);
    }
    out
}

async fn run_opencode_poll(
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
) -> Vec<RowBatch> {
    let config = moraine_config::AppConfig::default();
    let metrics = Arc::new(Metrics::default());
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);

    process_opencode_sqlite_db(&config, work, checkpoints.clone(), sink_tx, &metrics)
        .await
        .expect("opencode_sqlite poll should succeed");
    let batches = drain_batches(&mut sink_rx).await;

    if let Some(cp) = batches.last().and_then(|batch| batch.checkpoint.clone()) {
        let key = checkpoint_key(&cp.source_name, &cp.source_file);
        checkpoints.write().await.insert(key, cp);
    }
    batches
}

fn all_event_rows(batches: &[RowBatch]) -> Vec<Value> {
    batches
        .iter()
        .flat_map(|batch| batch.event_rows.iter().cloned())
        .collect()
}

fn event_uid_by_kind(rows: &[Value], event_kind: &str) -> Vec<String> {
    rows.iter()
        .filter(|row| row.get("event_kind").and_then(Value::as_str) == Some(event_kind))
        .filter_map(|row| row.get("event_uid").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.to_string_lossy()));
    let _ = std::fs::remove_file(format!("{}-shm", path.to_string_lossy()));
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_first_poll_emits_allowlisted_conversation_rows() {
    let path = unique_opencode_db_path("first-poll");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let batches = run_opencode_poll(&work, &checkpoints).await;

    let raw_rows: Vec<Value> = batches
        .iter()
        .flat_map(|batch| batch.raw_rows.iter().cloned())
        .collect();
    let event_rows = all_event_rows(&batches);
    assert_eq!(
        raw_rows.len(),
        10,
        "session/messages/parts/session_message rows"
    );
    assert!(
        event_rows.iter().any(
            |row| row.get("harness").and_then(Value::as_str) == Some("opencode")
                && row.get("event_kind").and_then(Value::as_str) == Some("session_meta")
                && row.get("text_content").and_then(Value::as_str) == Some("OpenCode DB fixture")
        ),
        "expected OpenCode session metadata event"
    );
    assert!(
        event_rows
            .iter()
            .any(|row| row.get("text_content").and_then(Value::as_str)
                == Some("Please inspect the project.")),
        "expected user message text"
    );
    assert!(
        event_rows
            .iter()
            .any(|row| row.get("text_content").and_then(Value::as_str)
                == Some("I can inspect that.")),
        "expected assistant message text"
    );
    assert_eq!(
        event_uid_by_kind(&event_rows, "reasoning").len(),
        1,
        "reasoning part emits a reasoning event"
    );
    assert_eq!(
        event_uid_by_kind(&event_rows, "tool_call").len(),
        3,
        "completed, error, and pending tool parts each emit a call"
    );
    assert_eq!(
        event_uid_by_kind(&event_rows, "tool_result").len(),
        2,
        "only terminal tool parts emit result events"
    );
    let tool_rows: Vec<Value> = batches
        .iter()
        .flat_map(|batch| batch.tool_rows.iter().cloned())
        .collect();
    assert_eq!(
        tool_rows.len(),
        5,
        "three requests plus completed/error responses"
    );
    assert!(
        tool_rows
            .iter()
            .any(|row| row.get("tool_error").and_then(Value::as_u64) == Some(1)),
        "error tool part emits an errored response"
    );
    assert!(
        event_rows.iter().any(|row| {
            row.get("event_kind").and_then(Value::as_str) == Some("progress")
                && row.get("input_tokens").and_then(Value::as_u64) == Some(20)
                && row.get("output_tokens").and_then(Value::as_u64) == Some(5)
        }),
        "step-finish part stamps token usage"
    );
    let checkpoint = batches
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("checkpoint");
    let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor_json parses");
    assert_eq!(
        cursor
            .pointer("/tables/message/watermark_ms")
            .and_then(Value::as_i64),
        Some(1780000001500_i64),
        "message cursor uses time_updated"
    );
    assert!(
        cursor.get("row_hashes").is_none(),
        "OpenCode uses tuple table cursors, not row hashes"
    );
    let serialized = serde_json::to_string(&raw_rows).expect("serialize raw rows");
    assert!(
        !serialized.contains("secret-token"),
        "credential table content must never be read or emitted"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_unchanged_db_is_a_noop_then_mutation_reemits_row() {
    let path = unique_opencode_db_path("mutation");
    let db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let first = run_opencode_poll(&work, &checkpoints).await;
    let first_rows = all_event_rows(&first);
    assert!(!first_rows.is_empty());
    let first_pending_tool_uid = first_rows
        .iter()
        .find(|row| {
            row.get("event_kind").and_then(Value::as_str) == Some("tool_call")
                && row.get("tool_call_id").and_then(Value::as_str) == Some("tool_pending")
        })
        .and_then(|row| row.get("event_uid").and_then(Value::as_str))
        .expect("pending tool call event")
        .to_string();

    let second = run_opencode_poll(&work, &checkpoints).await;
    assert!(
        all_event_rows(&second).is_empty(),
        "unchanged poll is empty"
    );

    db.execute(
        "UPDATE part SET data = ?1, time_updated = ?2 WHERE id = 'part_tool_pending'",
        rusqlite::params![
            serde_json::to_string(&json!({
                "type": "tool",
                "callID": "tool_pending",
                "tool": "bash",
                "state": {
                    "status": "completed",
                    "input": {"cmd": "ls"},
                    "output": "README.md\nCargo.toml"
                }
            }))
            .unwrap(),
            1780000005000_i64,
        ],
    )
    .expect("update message");
    let third = run_opencode_poll(&work, &checkpoints).await;
    let rows = all_event_rows(&third);
    assert_eq!(rows.len(), 2, "changed tool part emits call + result");
    let third_tool_uid = rows
        .iter()
        .find(|row| row.get("event_kind").and_then(Value::as_str) == Some("tool_call"))
        .and_then(|row| row.get("event_uid").and_then(Value::as_str))
        .expect("reemitted tool_call");
    assert_eq!(
        third_tool_uid, first_pending_tool_uid,
        "logical tool-call UID survives row mutation"
    );
    assert_eq!(
        event_uid_by_kind(&rows, "tool_result").len(),
        1,
        "completed pending tool emits its result side"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_schema_mismatch_emits_one_error_and_preserves_cursor() {
    let path = unique_opencode_db_path("schema-mismatch");
    let db = Connection::open(&path).expect("create db");
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY);
             CREATE TABLE message (id TEXT PRIMARY KEY);
             CREATE TABLE part (id TEXT PRIMARY KEY);
             CREATE TABLE session_message (id TEXT PRIMARY KEY);",
    )
    .expect("create incomplete opencode schema");
    drop(db);

    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));

    let first = run_opencode_poll(&work, &checkpoints).await;
    let first_errors: Vec<Value> = first
        .iter()
        .flat_map(|batch| batch.error_rows.iter().cloned())
        .collect();
    assert_eq!(first_errors.len(), 1);
    assert_eq!(
        first_errors[0].get("error_kind").and_then(Value::as_str),
        Some(ERROR_KIND_SCHEMA)
    );
    let first_checkpoint = first
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("first failure persists error marker");
    assert_eq!(
        first_checkpoint.last_offset, 0,
        "schema errors must not advance the data cursor"
    );
    assert!(
        serde_json::from_str::<Value>(&first_checkpoint.cursor_json)
            .expect("cursor_json parses")
            .get("tables")
            .and_then(Value::as_object)
            .map(|tables| tables.is_empty())
            .unwrap_or(true),
        "schema mismatch must not stamp table watermarks"
    );

    let second = run_opencode_poll(&work, &checkpoints).await;
    let second_errors: usize = second.iter().map(|batch| batch.error_rows.len()).sum();
    assert_eq!(
        second_errors, 0,
        "persistent OpenCode schema mismatch is reported once"
    );

    cleanup(&path);
}
