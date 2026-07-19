use super::*;
use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, RowBatch};
use moraine_config::SourceFormat;
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
                CREATE TABLE event_sequence (
                  aggregate_id text PRIMARY KEY,
                  seq integer NOT NULL,
                  owner_id text
                );
                CREATE TABLE event (
                  id text PRIMARY KEY,
                  aggregate_id text NOT NULL,
                  seq integer NOT NULL,
                  type text NOT NULL,
                  data text NOT NULL
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
            "summary": {"diffs": []},
            "time": {"created": 1780000001000_i64},
            "agent": "build",
            "model": {"modelID": "glm-5.2", "providerID": "zai-coding-plan"}
        }),
    );
    insert_message(
        "msg_assistant",
        1780000001500_i64,
        json!({
            "role": "assistant",
            "parentID": "msg_user",
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
        "part_user_text",
        "msg_user",
        1780000001100_i64,
        json!({"type": "text", "text": "Please inspect the project.", "time": {"start": 1780000001100_i64}}),
    );
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
                "INSERT INTO session_message (id, session_id, type, time_created, time_updated, data, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    "sm_shell",
                    "ses_demo",
                    "shell",
                    1780000003050_i64,
                    1780000003050_i64,
                    serde_json::to_string(&json!({"type": "shell", "text": "shell initialized"})).unwrap(),
                    2_i64,
                ],
            )
            .expect("insert shell session_message");
    connection
            .execute(
                "INSERT INTO session_message (id, session_id, type, time_created, time_updated, data, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    "sm_user_projection",
                    "ses_demo",
                    "user",
                    1780000003100_i64,
                    1780000003100_i64,
                    serde_json::to_string(&json!({"type": "user", "text": "duplicated session_message user text"})).unwrap(),
                    3_i64,
                ],
            )
            .expect("insert user session_message projection");
    connection
            .execute(
                "INSERT INTO session_message (id, session_id, type, time_created, time_updated, data, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    "sm_assistant_projection",
                    "ses_demo",
                    "assistant",
                    1780000003200_i64,
                    1780000003200_i64,
                    serde_json::to_string(&json!({"type": "assistant", "text": "duplicated session_message assistant text"})).unwrap(),
                    4_i64,
                ],
            )
            .expect("insert assistant session_message projection");
    let mut event_seq = 0_i64;
    let mut insert_event = |event_type: &str, data: Value| {
        let seq = event_seq;
        event_seq += 1;
        connection
            .execute(
                "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    format!("evt_{seq:03}"),
                    "ses_demo",
                    seq,
                    event_type,
                    serde_json::to_string(&data).unwrap(),
                ],
            )
            .expect("insert event");
    };
    insert_event(
        "session.created.1",
        json!({
            "sessionID": "ses_demo",
            "info": {
                "id": "ses_demo",
                "projectID": "proj_demo",
                "slug": "demo",
                "directory": "/work/opencode-demo",
                "title": "OpenCode DB fixture",
                "shareURL": "https://opencode.example/share/ses_demo",
                "summary": {"additions": 1, "deletions": 0, "files": 2, "diffs": [{"path": "src/main.rs"}]},
                "agent": "build",
                "model": {"id": "glm-5.2", "providerID": "zai-coding-plan"},
                "tokens": {"input": 10, "output": 4, "reasoning": 2, "cache": {"read": 3, "write": 1}},
                "version": "0.0.0-test",
                "time": {"created": 1780000000000_i64, "updated": 1780000004000_i64}
            }
        }),
    );
    insert_event(
        "message.updated.1",
        json!({
            "sessionID": "ses_demo",
            "info": {
                "id": "msg_user",
                "sessionID": "ses_demo",
                "role": "user",
                "summary": {"diffs": []},
                "time": {"created": 1780000001000_i64},
                "agent": "build",
                "model": {"modelID": "glm-5.2", "providerID": "zai-coding-plan"}
            }
        }),
    );
    insert_event(
        "message.part.updated.1",
        json!({
            "sessionID": "ses_demo",
            "part": {
                "id": "part_user_text",
                "messageID": "msg_user",
                "sessionID": "ses_demo",
                "type": "text",
                "text": "Please inspect the project.",
                "time": {"start": 1780000001100_i64}
            },
            "time": 1780000001100_i64
        }),
    );
    insert_event(
        "message.updated.1",
        json!({
            "sessionID": "ses_demo",
            "info": {
                "id": "msg_assistant",
                "sessionID": "ses_demo",
                "parentID": "msg_user",
                "role": "assistant",
                "path": {"cwd": "/work/opencode-demo", "root": "/work/opencode-demo"},
                "modelID": "glm-5.2",
                "providerID": "zai-coding-plan",
                "tokens": {"input": 20, "output": 5, "reasoning": 1, "cache": {"read": 7, "write": 0}},
                "finish": "stop",
                "time": {"created": 1780000001500_i64}
            }
        }),
    );
    for (id, created, data) in [
        (
            "part_text",
            1780000002000_i64,
            json!({"id": "part_text", "messageID": "msg_assistant", "sessionID": "ses_demo", "type": "text", "text": "I can inspect that.", "time": {"start": 1780000002000_i64}}),
        ),
        (
            "part_reasoning",
            1780000002100_i64,
            json!({"id": "part_reasoning", "messageID": "msg_assistant", "sessionID": "ses_demo", "type": "reasoning", "text": "Need to list files.", "time": {"start": 1780000002100_i64}}),
        ),
        (
            "part_tool_done",
            1780000002200_i64,
            json!({"id": "part_tool_done", "messageID": "msg_assistant", "sessionID": "ses_demo", "type": "tool", "callID": "tool_done", "tool": "bash", "state": {"status": "completed", "input": {"cmd": "pwd"}, "output": "/work/opencode-demo", "time": {"start": 1780000002190_i64, "end": 1780000002200_i64}}}),
        ),
        (
            "part_tool_error",
            1780000002300_i64,
            json!({"id": "part_tool_error", "messageID": "msg_assistant", "sessionID": "ses_demo", "type": "tool", "callID": "tool_error", "tool": "bash", "state": {"status": "error", "input": {"cmd": "cat missing.txt"}, "error": "No such file or directory"}}),
        ),
        (
            "part_step_finish",
            1780000002400_i64,
            json!({"id": "part_step_finish", "messageID": "msg_assistant", "sessionID": "ses_demo", "type": "step-finish", "reason": "stop", "tokens": {"input": 20, "output": 5, "reasoning": 1, "cache": {"read": 7, "write": 0}}, "cost": 0.001}),
        ),
        (
            "part_tool_pending",
            1780000002500_i64,
            json!({"id": "part_tool_pending", "messageID": "msg_assistant", "sessionID": "ses_demo", "type": "tool", "callID": "tool_pending", "tool": "bash", "state": {"status": "pending", "input": {"cmd": "ls"}}}),
        ),
    ] {
        insert_event(
            "message.part.updated.1",
            json!({"sessionID": "ses_demo", "part": data, "time": created}),
        );
        assert!(!id.is_empty());
    }
    insert_event(
        "session.next.model.switched.1",
        json!({
            "timestamp": 1780000003000_i64,
            "sessionID": "ses_demo",
            "messageID": "sm_model",
            "model": {"id": "glm-5.2", "providerID": "zai-coding-plan"}
        }),
    );
    insert_event(
        "shell",
        json!({
            "timestamp": 1780000003050_i64,
            "sessionID": "ses_demo",
            "type": "shell",
            "text": "shell initialized"
        }),
    );
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?1, ?2, NULL)",
            rusqlite::params!["ses_demo", event_seq - 1],
        )
        .expect("insert event_sequence");
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
        format: SourceFormat::OpenCodeSqlite,
        source_glob: String::new(),
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
    run_opencode_poll_with_state(work, checkpoints, &VolatilePollMap::new()).await
}

async fn run_opencode_poll_with_state(
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: &VolatilePollMap,
) -> Vec<RowBatch> {
    let config = moraine_config::AppConfig::default();
    let metrics = Arc::new(Metrics::default());
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);

    process_opencode_sqlite_db(
        &config,
        work,
        checkpoints.clone(),
        poll_state,
        sink_tx,
        &metrics,
    )
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
async fn opencode_sqlite_real_db_smoke_from_env() {
    let Some(path) = std::env::var_os("MORAINE_OPENCODE_REAL_DB") else {
        return;
    };
    let path = PathBuf::from(path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));

    let batches = run_opencode_poll(&work, &checkpoints).await;
    let errors: Vec<Value> = batches
        .iter()
        .flat_map(|batch| batch.error_rows.iter().cloned())
        .collect();
    assert!(
        errors.is_empty(),
        "real OpenCode DB smoke errors: {errors:?}"
    );

    let raw_rows: Vec<Value> = batches
        .iter()
        .flat_map(|batch| batch.raw_rows.iter().cloned())
        .collect();
    assert!(
        !raw_rows.is_empty(),
        "real OpenCode DB smoke should emit synthetic raw rows"
    );

    let event_rows = all_event_rows(&batches);
    assert!(
        !event_rows.is_empty(),
        "real OpenCode DB smoke should normalize at least one event row"
    );
    assert!(
        event_rows
            .iter()
            .any(|row| row.get("harness").and_then(Value::as_str) == Some("opencode")),
        "normalized rows should retain the opencode harness"
    );

    let checkpoint = batches
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("real OpenCode smoke checkpoint");
    let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor_json parses");
    let expected_sequences = {
        let connection = Connection::open(&path).expect("open real OpenCode smoke db");
        let mut stmt = connection
            .prepare("SELECT aggregate_id, seq FROM event_sequence ORDER BY aggregate_id")
            .expect("prepare event_sequence query");
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .expect("query event_sequence")
        .collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .expect("read event_sequence")
    };
    let cursor_sequences = cursor
        .get("aggregate_sequences")
        .and_then(Value::as_object)
        .expect("cursor has aggregate sequences")
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value.as_i64().expect("aggregate sequence is an integer"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        cursor_sequences, expected_sequences,
        "real OpenCode smoke should scan each aggregate through event_sequence"
    );
    assert!(
        cursor
            .get("aggregate_sequences")
            .and_then(Value::as_object)
            .is_some_and(|seqs| !seqs.is_empty()),
        "real OpenCode smoke should persist aggregate sequence cursors"
    );
    assert!(
        cursor.get("session_contexts").is_none() && cursor.get("message_contexts").is_none(),
        "real OpenCode smoke cursor should stay bounded"
    );
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
        11,
        "session/messages/parts/relevant session_message rows"
    );
    let tool_done_raw: Value = raw_rows
        .iter()
        .find_map(|row| {
            let raw_json = row.get("raw_json").and_then(Value::as_str)?;
            let parsed: Value = serde_json::from_str(raw_json).ok()?;
            (parsed.get("id").and_then(Value::as_str) == Some("part_tool_done")).then_some(parsed)
        })
        .expect("part_tool_done raw row");
    assert_eq!(
        tool_done_raw.get("time_created").and_then(Value::as_i64),
        Some(1780000002190_i64),
        "tool parts use state.time.start when OpenCode provides it"
    );
    assert_eq!(
        tool_done_raw.get("time_updated").and_then(Value::as_i64),
        Some(1780000002200_i64),
        "tool parts use state.time.end when OpenCode provides it"
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
        event_rows.iter().any(|row| {
            row.get("text_content").and_then(Value::as_str) == Some("Please inspect the project.")
                && row.get("actor_kind").and_then(Value::as_str) == Some("user")
                && row.get("cwd").and_then(Value::as_str) == Some("/work/opencode-demo")
                && row.get("model").and_then(Value::as_str) == Some("glm-5.2")
                && row.get("inference_provider").and_then(Value::as_str) == Some("zai-coding-plan")
                && row
                    .get("payload_json")
                    .and_then(Value::as_str)
                    .map(|payload| payload.contains("\"id\":\"part_user_text\""))
                    .unwrap_or(false)
        }),
        "user-owned text part must stay attributed to the user"
    );
    assert!(
        event_rows
            .iter()
            .any(|row| row.get("text_content").and_then(Value::as_str)
                == Some("I can inspect that.")),
        "expected assistant message text"
    );
    assert!(
        event_rows.iter().any(|row| {
            row.get("text_content").and_then(Value::as_str) == Some("I can inspect that.")
                && row.get("model").and_then(Value::as_str) == Some("glm-5.2")
                && row.get("cwd").and_then(Value::as_str) == Some("/work/opencode-demo")
        }),
        "message and part rows should carry model/cwd context"
    );
    assert!(
        !event_rows.iter().any(|row| row
            .get("text_content")
            .and_then(Value::as_str)
            .map(|text| text.starts_with("duplicated session_message"))
            .unwrap_or(false)),
        "conversation-shaped session_message projections should not duplicate message/part rows"
    );
    assert!(
        !event_rows.iter().any(|row| {
            row.get("op_kind").and_then(Value::as_str) == Some("shell")
                || row.get("text_content").and_then(Value::as_str) == Some("shell initialized")
        }),
        "unknown OpenCode event types are ignored until they are explicitly mapped"
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
    assert!(
        event_rows.iter().any(|row| {
            row.get("event_kind").and_then(Value::as_str) == Some("tool_call")
                && row.get("tool_call_id").and_then(Value::as_str) == Some("tool_done")
                && row.get("model").and_then(Value::as_str) == Some("glm-5.2")
        }),
        "tool part events should carry model context"
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
                && row.get("input_tokens").and_then(Value::as_u64) == Some(27)
                && row.get("output_tokens").and_then(Value::as_u64) == Some(6)
        }),
        "step-finish part stamps token usage"
    );
    let checkpoint = batches
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("checkpoint");
    let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor_json parses");
    assert!(
        cursor
            .pointer("/aggregate_sequences/ses_demo")
            .and_then(Value::as_i64)
            .is_some(),
        "OpenCode uses the append-only event sequence cursor"
    );
    assert!(
        cursor.get("session_contexts").is_none() && cursor.get("message_contexts").is_none(),
        "OpenCode checkpoints stay bounded to aggregate sequence cursors"
    );
    let serialized = serde_json::to_string(&raw_rows).expect("serialize raw rows");
    assert!(
        !serialized.contains("secret-token"),
        "credential table content must never be read or emitted"
    );
    assert!(
        !serialized.contains("shell initialized"),
        "unknown event payloads should not be copied into raw rows"
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

    let next_seq: i64 = db
        .query_row(
            "SELECT seq + 1 FROM event_sequence WHERE aggregate_id = 'ses_demo'",
            [],
            |row| row.get(0),
        )
        .expect("read next sequence");
    db.execute(
        "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_demo', ?2, 'message.part.updated.1', ?3)",
        rusqlite::params![
            "evt_pending_completed",
            next_seq,
            serde_json::to_string(&json!({
                "sessionID": "ses_demo",
                "part": {
                    "id": "part_tool_pending",
                    "messageID": "msg_assistant",
                    "sessionID": "ses_demo",
                    "type": "tool",
                    "callID": "tool_pending",
                    "tool": "bash",
                    "state": {
                        "status": "completed",
                        "input": {"cmd": "ls"},
                        "output": "README.md\nCargo.toml",
                        "time": {"start": 1780000003400_i64, "end": 1780000003500_i64}
                    }
                },
                "time": 1780000003500_i64
            }))
            .unwrap(),
        ],
    )
    .expect("append part completion event");
    db.execute(
        "UPDATE event_sequence SET seq = ?1 WHERE aggregate_id = 'ses_demo'",
        rusqlite::params![next_seq + 1],
    )
    .expect("advance event sequence ahead of visible rows");
    let third = run_opencode_poll(&work, &checkpoints).await;
    let rows = all_event_rows(&third);
    assert_eq!(rows.len(), 2, "changed tool part emits call + result");
    let third_tool_call = rows
        .iter()
        .find(|row| row.get("event_kind").and_then(Value::as_str) == Some("tool_call"))
        .expect("reemitted tool_call");
    let third_tool_uid = third_tool_call
        .get("event_uid")
        .and_then(Value::as_str)
        .expect("reemitted tool_call uid");
    assert_eq!(
        third_tool_uid, first_pending_tool_uid,
        "logical tool-call UID survives row mutation"
    );
    assert_eq!(
        third_tool_call.get("model").and_then(Value::as_str),
        Some("glm-5.2"),
        "incremental part events rebuild model context from prior event history"
    );
    assert_eq!(
        third_tool_call.get("cwd").and_then(Value::as_str),
        Some("/work/opencode-demo"),
        "incremental part events rebuild cwd context from prior event history"
    );
    assert_eq!(
        event_uid_by_kind(&rows, "tool_result").len(),
        1,
        "completed pending tool emits its result side"
    );
    let third_checkpoint = third
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("third checkpoint");
    let third_cursor: Value =
        serde_json::from_str(&third_checkpoint.cursor_json).expect("third cursor parses");
    assert_eq!(
        third_cursor
            .pointer("/aggregate_sequences/ses_demo")
            .and_then(Value::as_i64),
        Some(next_seq),
        "checkpoint records the last observed event row, not a sequence index that is ahead"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_irrelevant_write_persists_no_checkpoint() {
    let path = unique_opencode_db_path("noop-checkpoint");
    let db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));

    let first = run_opencode_poll(&work, &checkpoints).await;
    assert!(!first.is_empty());
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let baseline = checkpoints
        .read()
        .await
        .get(&cp_key)
        .cloned()
        .expect("committed checkpoint after first poll");

    // A write that never touches the event tables (issue #443): the stat
    // fingerprint moves but the event cursor cannot advance.
    db.execute(
        "INSERT INTO credential (id, value, time_created, time_updated) \
         VALUES ('cred_noise', 'rotated', 1780000004000, 1780000004000)",
        [],
    )
    .expect("write non-event row");

    let second = run_opencode_poll(&work, &checkpoints).await;
    assert!(
        second.is_empty(),
        "a no-op scan must send nothing durable; got {} batches",
        second.len()
    );
    let after = checkpoints
        .read()
        .await
        .get(&cp_key)
        .cloned()
        .expect("checkpoint survives no-op poll");
    assert_eq!(
        baseline.last_offset, after.last_offset,
        "no-op scan must not advance the poll sequence"
    );

    cleanup(&path);
}

#[test]
fn opencode_sqlite_scan_paginates_past_single_event_page() {
    let path = unique_opencode_db_path("many-events");
    let connection = create_opencode_db(&path);
    let event_count = SCAN_PAGE_SIZE + 5;

    for seq in 0..event_count {
        connection
            .execute(
                "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_many', ?2, 'message.part.updated.1', ?3)",
                rusqlite::params![
                    format!("evt_many_{seq:04}"),
                    seq as i64,
                    serde_json::to_string(&json!({
                        "sessionID": "ses_many",
                        "part": {
                            "id": format!("part_many_{seq:04}"),
                            "messageID": "msg_many",
                            "sessionID": "ses_many",
                            "type": "text",
                            "text": format!("page text {seq}")
                        },
                        "time": 1780000100000_i64 + seq as i64
                    }))
                    .unwrap(),
                ],
            )
            .expect("insert many-event page row");
    }
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_many', ?1, NULL)",
            rusqlite::params![(event_count - 1) as i64],
        )
        .expect("insert many-event sequence");
    drop(connection);

    let outcome =
        scan_opencode_database(path.to_str().expect("utf-8 path"), &OpenCodeState::fresh());
    let (records, new_state, relevant_rows) = match outcome {
        OpenCodeScanOutcome::Scanned {
            records,
            new_state,
            relevant_rows,
            ..
        } => (records, new_state, relevant_rows),
        OpenCodeScanOutcome::Failed {
            error_kind,
            error_text,
        } => panic!("scan failed: {error_kind}: {error_text}"),
    };
    assert_eq!(records.len(), event_count);
    assert_eq!(relevant_rows, event_count as u64);
    assert_eq!(
        new_state.aggregate_sequences.get("ses_many").copied(),
        Some((event_count - 1) as i64),
        "scan must advance through all event pages"
    );

    cleanup(&path);
}

#[test]
fn opencode_sqlite_project_dir_stays_on_first_absolute_session_directory() {
    let path = unique_opencode_db_path("sticky-project-dir");
    let connection = seed_opencode_db(&path);
    let last_seq: i64 = connection
        .query_row(
            "SELECT seq FROM event_sequence WHERE aggregate_id = 'ses_demo'",
            [],
            |row| row.get(0),
        )
        .expect("read event sequence");
    let next_seq = last_seq + 1;
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_session_cd', 'ses_demo', ?1, 'session.updated.1', ?2)",
            rusqlite::params![
                next_seq,
                serde_json::to_string(&json!({
                    "sessionID": "ses_demo",
                    "info": {
                        "id": "ses_demo",
                        "directory": "/work/after-cd",
                        "title": "Moved session",
                        "time": {
                            "created": 1780000000000_i64,
                            "updated": 1780000005000_i64
                        }
                    }
                }))
                .unwrap(),
            ],
        )
        .expect("insert session directory update");
    connection
        .execute(
            "UPDATE event_sequence SET seq = ?1 WHERE aggregate_id = 'ses_demo'",
            rusqlite::params![next_seq],
        )
        .expect("advance event sequence");
    drop(connection);

    let outcome =
        scan_opencode_database(path.to_str().expect("utf-8 path"), &OpenCodeState::fresh());
    let records = match outcome {
        OpenCodeScanOutcome::Scanned { records, .. } => records,
        OpenCodeScanOutcome::Failed {
            error_kind,
            error_text,
        } => panic!("scan failed: {error_kind}: {error_text}"),
    };
    assert!(!records.is_empty());
    assert!(
        records
            .iter()
            .all(|record| record.project_dir == "/work/opencode-demo"),
        "every record must retain the session's first absolute directory"
    );
    assert!(
        records.iter().any(|record| {
            record.record.get("type").and_then(Value::as_str) == Some("opencode_session")
                && record.record.get("directory").and_then(Value::as_str) == Some("/work/after-cd")
        }),
        "fixture must retain the later cwd on the raw session update"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_replays_rows_when_exclusions_change() {
    let path = unique_opencode_db_path("exclusion-replay");
    drop(seed_opencode_db(&path));
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
    let metrics = Arc::new(Metrics::default());
    let poll_state = VolatilePollMap::new();

    let mut excluded_config = moraine_config::AppConfig::default();
    excluded_config.ingest.exclude_project_dirs = vec!["/work/opencode-demo/**".to_string()];
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
    process_opencode_sqlite_db(
        &excluded_config,
        &work,
        checkpoints.clone(),
        &poll_state,
        sink_tx,
        &metrics,
    )
    .await
    .expect("excluded OpenCode poll");
    let excluded_batches = drain_batches(&mut sink_rx).await;
    assert!(
        all_event_rows(&excluded_batches).is_empty(),
        "excluded session rows must not reach the sink"
    );
    let checkpoint = excluded_batches
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("excluded poll must persist its cursor");
    checkpoints.write().await.insert(
        checkpoint_key(&checkpoint.source_name, &checkpoint.source_file),
        checkpoint,
    );

    let included_config = moraine_config::AppConfig::default();
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
    process_opencode_sqlite_db(
        &included_config,
        &work,
        checkpoints,
        &poll_state,
        sink_tx,
        &metrics,
    )
    .await
    .expect("OpenCode replay after exclusion removal");
    let replayed = drain_batches(&mut sink_rx).await;
    assert!(
        !all_event_rows(&replayed).is_empty(),
        "removing exclusions must replay previously skipped rows"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_ignores_events_above_sequence_bound() {
    let path = unique_opencode_db_path("future-row-bound");
    let connection = create_opencode_db(&path);
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_bound', 0, 'session.created.1', ?2)",
            rusqlite::params![
                "evt_visible",
                serde_json::to_string(&json!({
                    "sessionID": "ses_bound",
                    "info": {
                        "id": "ses_bound",
                        "directory": "/work/opencode-bound",
                        "title": "Visible OpenCode session",
                        "time": {"created": 1780000200000_i64, "updated": 1780000200000_i64}
                    }
                }))
                .unwrap(),
            ],
        )
        .expect("insert visible event");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_bound', 1, 'session.created.1', ?2)",
            rusqlite::params![
                "evt_future",
                serde_json::to_string(&json!({
                    "sessionID": "ses_bound",
                    "info": {
                        "id": "ses_bound",
                        "directory": "/work/opencode-bound",
                        "title": "Future OpenCode session",
                        "time": {"created": 1780000201000_i64, "updated": 1780000201000_i64},
                        "blob": "A".repeat(SCAN_PAGE_MAX_BYTES + 1)
                    }
                }))
                .unwrap(),
            ],
        )
        .expect("insert future oversized event");
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_bound', 0, NULL)",
            [],
        )
        .expect("insert bounded sequence");
    drop(connection);

    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let batches = run_opencode_poll(&work, &checkpoints).await;
    let event_rows = all_event_rows(&batches);
    assert!(
        event_rows.iter().any(|row| {
            row.get("event_kind").and_then(Value::as_str) == Some("session_meta")
                && row.get("text_content").and_then(Value::as_str)
                    == Some("Visible OpenCode session")
        }),
        "visible event under event_sequence bound should emit"
    );
    assert!(
        !event_rows
            .iter()
            .any(|row| row.get("text_content").and_then(Value::as_str)
                == Some("Future OpenCode session")),
        "future event above event_sequence bound should not emit"
    );
    let error_rows: usize = batches.iter().map(|batch| batch.error_rows.len()).sum();
    assert_eq!(
        error_rows, 0,
        "future oversized rows above event_sequence bound should not trip scan limits"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_sequence_regression_resets_aggregate_cursor() {
    let path = unique_opencode_db_path("sequence-regression");
    let db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));

    let first = run_opencode_poll(&work, &checkpoints).await;
    let first_checkpoint = first
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("first checkpoint");
    let first_cursor: Value =
        serde_json::from_str(&first_checkpoint.cursor_json).expect("first cursor parses");
    assert!(
        first_cursor
            .pointer("/aggregate_sequences/ses_demo")
            .and_then(Value::as_i64)
            .is_some_and(|seq| seq > 0),
        "fixture first poll advances aggregate sequence"
    );

    db.execute("DELETE FROM event", [])
        .expect("clear previous event history");
    db.execute(
        "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_demo', 0, 'session.created.1', ?2)",
        rusqlite::params![
            "evt_reset_session",
            serde_json::to_string(&json!({
                "sessionID": "ses_demo",
                "info": {
                    "id": "ses_demo",
                    "directory": "/work/opencode-reset",
                    "title": "Reset OpenCode session",
                    "model": {"id": "glm-5.2", "providerID": "zai-coding-plan"},
                    "time": {"created": 1780000010000_i64, "updated": 1780000010000_i64}
                }
            }))
            .unwrap(),
        ],
    )
    .expect("insert reset session event");
    db.execute(
        "UPDATE event_sequence SET seq = 0 WHERE aggregate_id = 'ses_demo'",
        [],
    )
    .expect("regress event sequence");

    let second = run_opencode_poll(&work, &checkpoints).await;
    let second_rows = all_event_rows(&second);
    assert!(
        second_rows.iter().any(|row| {
            row.get("event_kind").and_then(Value::as_str) == Some("session_meta")
                && row.get("text_content").and_then(Value::as_str) == Some("Reset OpenCode session")
        }),
        "sequence regression must reread the aggregate from seq 0"
    );
    let second_checkpoint = second
        .last()
        .and_then(|batch| batch.checkpoint.clone())
        .expect("second checkpoint");
    let second_cursor: Value =
        serde_json::from_str(&second_checkpoint.cursor_json).expect("second cursor parses");
    assert_eq!(
        second_cursor
            .pointer("/aggregate_sequences/ses_demo")
            .and_then(Value::as_i64),
        Some(0),
        "regressed aggregate cursor is replaced with the current sequence"
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
    let cursor_json =
        serde_json::from_str::<Value>(&first_checkpoint.cursor_json).expect("cursor_json parses");
    assert!(
        cursor_json
            .get("aggregate_sequences")
            .and_then(Value::as_object)
            .is_none_or(|seqs| seqs.is_empty()),
        "schema mismatch must not stamp event cursors"
    );

    let second = run_opencode_poll(&work, &checkpoints).await;
    let second_errors: usize = second.iter().map(|batch| batch.error_rows.len()).sum();
    assert_eq!(
        second_errors, 0,
        "persistent OpenCode schema mismatch is reported once"
    );

    cleanup(&path);
}

#[test]
fn opencode_long_string_sanitizer_preserves_text_and_elides_binary_like_payloads() {
    let long_text = "long searchable prompt ".repeat(OPENCODE_LONG_BINARY_STRING_CHARS / 10);
    let mut text_value = json!({"text": long_text.clone()});
    elide_binary_like_strings(&mut text_value);
    assert_eq!(
        text_value.pointer("/text").and_then(Value::as_str),
        Some(long_text.as_str()),
        "long human-readable text should stay searchable"
    );

    let mut binary_value = json!({"image": "A".repeat(OPENCODE_LONG_BINARY_STRING_CHARS + 10)});
    elide_binary_like_strings(&mut binary_value);
    let sanitized = binary_value
        .pointer("/image")
        .and_then(Value::as_str)
        .expect("sanitized image string");
    assert!(
        sanitized.contains("elided") && sanitized.len() < 1_000,
        "binary-like payload should be elided, got {} bytes",
        sanitized.len()
    );
}
