use super::*;
use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, RowBatch};
use crate::WorkTrigger;
use moraine_config::SourceFormat;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
                -- Verbatim from a live `~/.local/share/opencode/opencode.db`.
                -- Without them every scan the unit tests measure is a full
                -- table scan, so any cost budget calibrated here would be
                -- calibrated against a plan production never runs
                -- (issue #601 §1.2). Asserted by
                -- `opencode_fixture_uses_the_production_event_index`.
                CREATE UNIQUE INDEX `event_aggregate_seq_idx`
                  ON `event` (`aggregate_id`,`seq`);
                CREATE INDEX `event_aggregate_type_seq_idx`
                  ON `event` (`aggregate_id`,`type`,`seq`);
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

/// The shipped-default fast-path budget: what a production poll runs with
/// when nothing in `[ingest.sqlite]` is overridden.
fn default_opencode_budget() -> ScanBudget {
    ScanBudget::fast_path(&moraine_config::SqliteIngestConfig::default())
}

fn opencode_sqlite_work(path: &Path) -> WorkItem {
    WorkItem {
        source_name: "opencode-sqlite-test".to_string(),
        harness: "opencode".to_string(),
        format: SourceFormat::OpenCodeSqlite,
        source_glob: String::new(),
        path: path.to_string_lossy().to_string(),
        trigger: WorkTrigger::Watcher,
    }
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
    drive_opencode_poll(&config, work, checkpoints, poll_state).await
}

async fn drive_opencode_poll(
    config: &moraine_config::AppConfig,
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: &VolatilePollMap,
) -> Vec<RowBatch> {
    drive_opencode_poll_with_metrics(
        config,
        work,
        checkpoints,
        poll_state,
        &Arc::new(Metrics::default()),
    )
    .await
}

/// Shares one `Metrics` across several polls so a test can observe what a poll
/// actually read (`sqlite_poll_*_total`) rather than only what it emitted.
async fn drive_opencode_poll_with_metrics(
    config: &moraine_config::AppConfig,
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: &VolatilePollMap,
    metrics: &Arc<Metrics>,
) -> Vec<RowBatch> {
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
    let process = process_opencode_sqlite_db(
        config,
        work,
        checkpoints.clone(),
        poll_state,
        sink_tx,
        metrics,
    );
    tokio::pin!(process);
    let mut batches = Vec::new();
    let mut finalized = None;
    loop {
        tokio::select! {
            result = &mut process => {
                result.expect("opencode_sqlite poll should succeed");
                break;
            }
            message = sink_rx.recv() => match message.expect("opencode test sink remains open") {
                SinkMessage::Batch(batch) => batches.push(batch),
                SinkMessage::BeginReplay { transition, ack }
                | SinkMessage::BlockReplay { transition, ack }
                | SinkMessage::MirrorCaughtUp { transition, ack } => {
                    let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                        checkpoint_revision: 1,
                        operation_id: transition.checkpoint.operation_id,
                    }));
                }
                SinkMessage::FinalizeReplay { transition, ack } => {
                    finalized = Some(transition.checkpoint);
                    let _ = ack.send(Ok(
                        crate::publication::FinalizeReplayOutcome::Published(
                            crate::publication::PublicationAck {
                                checkpoint_revision: 2,
                                publication_revision: 1,
                                already_published: false,
                            },
                        ),
                    ));
                }
            }
        }
    }
    while let Ok(message) = sink_rx.try_recv() {
        if let SinkMessage::Batch(batch) = message {
            batches.push(batch);
        }
    }

    if let Some(cp) =
        finalized.or_else(|| batches.last().and_then(|batch| batch.checkpoint.clone()))
    {
        let key = checkpoint_key(&cp.source_name, &cp.source_file);
        checkpoints.write().await.insert(key, cp);
    }
    batches
}

/// Runs one poll, acknowledging every barrier, and reports whether it emitted
/// a `BlockReplay`. Unlike `drive_opencode_poll_with_metrics` it persists the
/// checkpoint a barrier carried, which is the only way a *blocked* replay's
/// state reaches the next poll.
async fn drive_opencode_block_poll(
    config: &moraine_config::AppConfig,
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: &VolatilePollMap,
    metrics: &Arc<Metrics>,
) -> bool {
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
    let process = process_opencode_sqlite_db(
        config,
        work,
        checkpoints.clone(),
        poll_state,
        sink_tx,
        metrics,
    );
    tokio::pin!(process);
    let mut batches = Vec::new();
    let mut committed = None;
    let mut blocked = false;
    loop {
        tokio::select! {
            result = &mut process => {
                result.expect("opencode_sqlite replay poll should succeed");
                break;
            }
            message = sink_rx.recv() => match message.expect("opencode test sink remains open") {
                SinkMessage::Batch(batch) => batches.push(batch),
                SinkMessage::BlockReplay { transition, ack } => {
                    blocked = true;
                    committed = Some(transition.checkpoint.clone());
                    let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                        checkpoint_revision: 1,
                        operation_id: transition.checkpoint.operation_id,
                    }));
                }
                SinkMessage::BeginReplay { transition, ack }
                | SinkMessage::MirrorCaughtUp { transition, ack } => {
                    committed = Some(transition.checkpoint.clone());
                    let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                        checkpoint_revision: 1,
                        operation_id: transition.checkpoint.operation_id,
                    }));
                }
                SinkMessage::FinalizeReplay { transition, ack } => {
                    committed = Some(transition.checkpoint.clone());
                    let _ = ack.send(Ok(
                        crate::publication::FinalizeReplayOutcome::Published(
                            crate::publication::PublicationAck {
                                checkpoint_revision: 2,
                                publication_revision: 1,
                                already_published: false,
                            },
                        ),
                    ));
                }
            }
        }
    }
    while let Ok(message) = sink_rx.try_recv() {
        if let SinkMessage::Batch(batch) = message {
            batches.push(batch);
        }
    }
    if let Some(cp) = committed.or_else(|| batches.last().and_then(|b| b.checkpoint.clone())) {
        let key = checkpoint_key(&cp.source_name, &cp.source_file);
        checkpoints.write().await.insert(key, cp);
    }
    blocked
}

/// Issue #601 §2.5. The OpenCode half of the `record_blocked_replay` call-site
/// gap: `record_blocked_replay` is guarded at its implementation, and Cursor's
/// call site is guarded by `blocked_replay_retries_climb_the_failure_ladder`,
/// but inserting a `poll_state.clear()` before opencode.rs's call passed the
/// whole suite. A `clear` there restarts the streak on every retry, which pins
/// the path at the 15 s floor and re-reads the entire event history — plus a
/// durable `BeginReplay`/`BlockReplay` pair — every 15 s indefinitely, because
/// the trigger (a record failing `normalize_record`) is deterministic.
///
/// Fails for: `clear`ing volatile state before `record_blocked_replay`, or
/// dropping the call entirely.
#[tokio::test]
async fn a_normalization_blocked_opencode_replay_climbs_the_failure_ladder() {
    let path = unique_opencode_db_path("block-ladder");
    let _db = seed_opencode_db(&path);
    // `normalize_record` rejects an unregistered harness outright, so every
    // synthesized record fails and a replacement replay blocks durably.
    let work = WorkItem {
        harness: "not-a-registered-harness".to_string(),
        ..opencode_sqlite_work(&path)
    };
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());

    let config = moraine_config::AppConfig::default();
    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert!(checkpoints.read().await.contains_key(&cp_key));

    // Changing the exclusion set starts a replacement replay that cannot
    // finish, because nothing normalizes.
    let mut replaying = moraine_config::AppConfig::default();
    replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
    let block_poll = |config: moraine_config::AppConfig| {
        let work = work.clone();
        let checkpoints = checkpoints.clone();
        let poll_state = poll_state.clone();
        let metrics = metrics.clone();
        async move {
            drive_opencode_block_poll(&config, &work, &checkpoints, &poll_state, &metrics).await
        }
    };

    assert!(
        block_poll(replaying.clone()).await,
        "the replacement replay must block durably"
    );
    assert_eq!(
        poll_state.consecutive_failed_scans(&cp_key),
        1,
        "entering the blocked state starts the ladder"
    );

    assert!(!block_poll(replaying.clone()).await, "throttled");

    poll_state.age_for_tests(&cp_key, std::time::Duration::from_secs(16));
    assert!(block_poll(replaying.clone()).await);
    assert_eq!(
        poll_state.consecutive_failed_scans(&cp_key),
        2,
        "a repeat block must extend the streak, not restart it — a `clear()` \
         before `record_blocked_replay` pins it at 1"
    );

    poll_state.age_for_tests(&cp_key, std::time::Duration::from_secs(16));
    assert!(
        !block_poll(replaying).await,
        "the second window is 30 s; a blocked replay must not re-read the whole \
         event history every 15 s forever"
    );

    cleanup(&path);
}

/// Issue #601 §2.5/§3.2. The OpenCode half of the same rule: its `Failed` arm
/// must classify a mixed-snapshot rejection as contention (no fault streak, so
/// no 15-minute suppression of an actively written store) while still moving
/// the clock that throttles a contended replay's durable barrier.
///
/// Fails for: calling `record_failed_scan` directly from the `Failed` arm, or
/// dropping the classification entirely.
#[tokio::test]
async fn a_contended_opencode_scan_moves_the_contention_clock_not_the_fault_ladder() {
    let path = unique_opencode_db_path("contention-classification");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let config = moraine_config::AppConfig::default();
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());

    crate::sqlite_poll::contention_injection::arm(&work.path, 1);
    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert_eq!(
        metrics
            .sqlite_scan_failures_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the armed scan must actually reach the mixed-snapshot arm"
    );
    assert_eq!(
        poll_state.consecutive_failed_scans(&cp_key),
        0,
        "contention must not climb OpenCode's fault ladder"
    );
    assert_eq!(
        poll_state.consecutive_contended_scans(&cp_key),
        1,
        "…but it must leave the clock that throttles the replay barrier"
    );

    crate::sqlite_poll::contention_injection::disarm(&work.path);
    cleanup(&path);
}

/// Issue #601 §3.2/§6 — OpenCode's half of the call-site guard. The classifier
/// test above proves the contention clock *moves*; this proves the clock does
/// not reach ordinary polls.
///
/// §2.5's `|| !failure_retry_due` disjunct on the cheap short-circuit was
/// outcome-redundant with `should_skip_poll` until §3.2 put the contention
/// clock inside `failure_retry_due` and deliberately left it out of
/// `should_skip_poll`. Adding it now would stop scanning an actively written
/// OpenCode store for up to 60 s — the prompt-visibility regression the
/// exemption exists to prevent, on a store that is contended precisely because
/// a session is streaming parts into it.
///
/// The end-to-end delivery half of this rule is
/// `an_ordinary_poll_of_a_contended_database_is_not_throttled` on the Cursor
/// adapter; this one pins the throttle at OpenCode's own call site.
///
/// Fails for: adding `|| !poll_state.failure_retry_due(..)` to the cheap
/// short-circuit (the second scan never runs), or moving the contention clock
/// into `should_skip_poll`.
#[tokio::test]
async fn an_ordinary_poll_of_a_contended_opencode_store_is_not_throttled() {
    let path = unique_opencode_db_path("contended-ordinary-poll");
    let db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let config = moraine_config::AppConfig::default();
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());

    let cold =
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert!(!cold.is_empty(), "the cold poll emits the fixture");

    // The session is live, so the next two scans lose the bracket to the
    // writer. Each poll is preceded by a real write, as a contended store's
    // would be.
    crate::sqlite_poll::contention_injection::arm(&work.path, 2);
    touch_opencode_store(&db, "cred_contended_one");
    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert_eq!(poll_state.consecutive_contended_scans(&cp_key), 1);
    assert_eq!(
        poll_state.consecutive_failed_scans(&cp_key),
        0,
        "contention is not a fault (§3.2)"
    );

    // The very next tick, far inside the contention window the barrier is now
    // serving, must still read the store.
    touch_opencode_store(&db, "cred_contended_two");
    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert_eq!(
        poll_state.consecutive_contended_scans(&cp_key),
        2,
        "an ordinary poll of a contended OpenCode store must not be throttled — \
         the second scan has to run at the ordinary poll cadence"
    );
    assert_eq!(
        metrics
            .sqlite_scan_failures_total
            .load(std::sync::atomic::Ordering::Relaxed),
        2,
        "…and must reach the scan, not return before it"
    );

    crate::sqlite_poll::contention_injection::disarm(&work.path);
    cleanup(&path);
}

/// Writes a row no event cursor can advance on, so the stat fingerprint moves
/// without changing what a scan would emit.
fn touch_opencode_store(db: &Connection, credential_id: &str) {
    db.execute(
        "INSERT INTO credential (id, value, time_created, time_updated) \
         VALUES (?1, 'rotated', 1780000004000, 1780000004000)",
        rusqlite::params![credential_id],
    )
    .expect("write non-event row");
}

async fn capture_begin_replay_checkpoint(
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
) -> Checkpoint {
    let config = moraine_config::AppConfig::default();
    let metrics = Arc::new(Metrics::default());
    let poll_state = VolatilePollMap::new();
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(4);
    let process = process_opencode_sqlite_db(
        &config,
        work,
        checkpoints.clone(),
        &poll_state,
        sink_tx,
        &metrics,
    );
    tokio::pin!(process);
    let message = tokio::select! {
        result = &mut process => {
            panic!("opencode poll completed before BeginReplay: {result:?}");
        }
        message = sink_rx.recv() => message.expect("opencode replay channel remains open"),
    };
    match message {
        SinkMessage::BeginReplay { transition, .. } => transition.checkpoint,
        other => panic!("expected BeginReplay, observed {other:?}"),
    }
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

// `opencode_sqlite_real_db_smoke_from_env` lived here and is **deleted**
// (issue #601 §3.1 Change 6, open question 6).
//
// It opened with `let Some(path) = std::env::var_os("MORAINE_OPENCODE_REAL_DB")
// else { return; };` and that variable appeared exactly once in the entire
// repository — that read. Every one of its 83 lines was dead and the test had
// always been green: the eighth instance of this epic's signature bug, a guard
// that cannot fail. The plan is explicit that it must be resolved rather than
// left, and gives two options: wire the variable into a live gate that fails
// when unset, or delete it.
//
// Deleted, because the fixture-based gates cover the same ground more
// precisely and without needing a real OpenCode database on the host:
//
// - per-aggregate watermark persistence → `opencode_sqlite_second_poll_is_a_noop`
//   and `opencode_incremental_scan_currently_re_reads_the_whole_aggregate`;
// - normalization and harness attribution → the first-poll row assertions below;
// - the "cursor stays bounded" assertion it ended on → restated by §3.1 Change 4
//   as a size budget, because WI-06 *requires* persisting reconstruction
//   context and an absence check would directly contradict it.

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
    // §3.1 Change 4 restates the old "no context maps" absence check as a size
    // budget: WI-06 *requires* persisting the reconstruction context (it is
    // what lets the page loop start at the watermark), and what stays bounded
    // is the serialized footprint, enforced by `evict_contexts_to_fit`.
    assert!(
        cursor.pointer("/session_contexts/ses_demo").is_some(),
        "the reconstruction context must persist in the cursor (§3.1 Change 2)"
    );
    assert!(
        checkpoint.cursor_json.len() <= 4 * 1024 * 1024,
        "the persisted cursor stays under the context byte ceiling"
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

    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &default_opencode_budget(),
        &mut ScanLedger::default(),
    );
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

    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &default_opencode_budget(),
        &mut ScanLedger::default(),
    );
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
    let poll_state = VolatilePollMap::new();

    let mut excluded_config = moraine_config::AppConfig::default();
    excluded_config.ingest.exclude_project_dirs = vec!["/work/opencode-demo/**".to_string()];
    let excluded_batches =
        drive_opencode_poll(&excluded_config, &work, &checkpoints, &poll_state).await;
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
    let replayed = drive_opencode_poll(&included_config, &work, &checkpoints, &poll_state).await;
    assert!(
        !all_event_rows(&replayed).is_empty(),
        "removing exclusions must replay previously skipped rows"
    );

    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn opencode_sqlite_can_queue_oversized_replay_row_before_final_checkpoint() {
    let path = unique_opencode_db_path("sink-limit-envelope");
    let connection = create_opencode_db(&path);
    let payload_bytes = crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES + 1024 * 1024;
    assert!(payload_bytes < SCAN_PAGE_MAX_BYTES);
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_oversized', 'ses_oversized', 0, 'session.created.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_oversized",
                "info": {
                    "id": "ses_oversized",
                    "directory": "/work/opencode-oversized",
                    "title": "Oversized OpenCode replay",
                    "time": {
                        "created": 1780000200000_i64,
                        "updated": 1780000200000_i64
                    },
                    // Metadata is retained by the OpenCode projection. Spaces
                    // keep this textual payload out of the binary elision
                    // heuristic while the raw SQLite row remains under 32 MiB.
                    "metadata": "word ".repeat(payload_bytes / 5 + 1)
                }
            }))
            .unwrap()],
        )
        .expect("insert sink-oversized OpenCode event");
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) \
             VALUES ('ses_oversized', 0, NULL)",
            [],
        )
        .expect("insert oversized event sequence");
    drop(connection);

    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();

    let mut excluded = moraine_config::AppConfig::default();
    excluded.ingest.exclude_project_dirs = vec!["/work/opencode-oversized/**".to_string()];
    let excluded_batches = drive_opencode_poll(&excluded, &work, &checkpoints, &poll_state).await;
    assert!(all_event_rows(&excluded_batches).is_empty());

    let included = moraine_config::AppConfig::default();
    let replay_batches = drive_opencode_poll(&included, &work, &checkpoints, &poll_state).await;
    let oversized_index = replay_batches
        .iter()
        .position(|batch| {
            batch.raw_rows.iter().any(|row| {
                serde_json::to_vec(row).is_ok_and(|encoded| {
                    encoded.len() > crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
                })
            })
        })
        .expect("OpenCode scanner emits a sink-oversized row under its page cap");
    let checkpoint_index = replay_batches
        .iter()
        .position(|batch| batch.checkpoint.is_some())
        .expect("OpenCode replay queues a final checkpoint");
    assert!(oversized_index < checkpoint_index);
    assert!(replay_batches[oversized_index].checkpoint.is_none());

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

    let replaying_checkpoint = capture_begin_replay_checkpoint(&work, &checkpoints).await;
    assert_eq!(
        replaying_checkpoint.source_generation,
        first_checkpoint.source_generation + 1,
        "rewind BeginReplay advances exactly once"
    );
    assert_eq!(replaying_checkpoint.status, "replaying");
    checkpoints.write().await.insert(
        checkpoint_key(&work.source_name, &work.path),
        replaying_checkpoint,
    );

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
    assert_eq!(
        second_checkpoint.source_generation,
        first_checkpoint.source_generation + 1,
        "sequence regression must replay through a replacement generation"
    );
    let durable_checkpoint = checkpoints
        .read()
        .await
        .get(&checkpoint_key(&work.source_name, &work.path))
        .cloned()
        .expect("finalized replay checkpoint");
    assert_eq!(durable_checkpoint.status, "active");
    assert!(durable_checkpoint.final_scan_complete);
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

/// Issue #601 §1.2 / WI-01. The live `opencode.db` carries
/// `event_aggregate_seq_idx` and `event_aggregate_type_seq_idx`; the fixture
/// carried neither, so every scan these unit tests measured was a full table
/// scan and any cost budget calibrated here would have been miscalibrated in
/// both directions.
///
/// Asserting on the query plan rather than on `sqlite_master` alone is what
/// makes this a guard: dropping either index from the fixture flips `SEARCH …
/// USING INDEX` to `SCAN event` and fails here — the calibration mutation the
/// plan's checklist names for G3. **Both** indexes are plan-asserted; asserting
/// one by `EXPLAIN` and the other only by its presence in `sqlite_master` would
/// leave the type-scoped read (§3.1 Change 3's bounded context rebuild)
/// uncalibrated, which is what shipped first.
///
/// The paged statement comes from `OPENCODE_EVENT_PAGE_SQL`, the constant the
/// adapter itself pages with — a copy here could drift from the query that
/// actually runs and certify a plan nothing uses.
///
/// Note what §1 corrects about the second index: on the live database SQLite
/// picks `event_aggregate_seq_idx` even for the type-scoped form, because the
/// `IN` list is not a leading-column equality. So the type-scoped assertion
/// below requires *an* index, not specifically the type-scoped one; what it
/// rules out is the full table scan the fixture used to force. The second index
/// is present because production has it, not because it is load-bearing here.
/// Issue #601 §3.2/§6. `build_event_row` reads four columns OpenCode declares
/// NOT NULL. The `ScanLedger` refactor routed all four through a helper that
/// absorbed NULL into `""`, which for `id`/`aggregate_id` is not a lossy field
/// but a **broken identity**: they are the material behind
/// `source_line_no`/`source_offset` and therefore behind `event_uid` (§6), so
/// two drifted rows would synthesize records that collide. `data` going empty
/// additionally reports `data_bytes = 0`, under-charging the ledger every
/// budget in §2.1 is denominated on.
///
/// Fails for: reading any of the four through a NULL-absorbing helper.
#[test]
fn a_null_in_a_required_event_column_fails_the_scan() {
    // Positional, matching the `event` projection: id, aggregate_id, seq,
    // type, data.
    let baseline = ["'evt_1'", "'ses_demo'", "1", "'session.created.1'", "'{}'"];
    let connection = Connection::open_in_memory().expect("in-memory probe database");
    let read = |values: &[&str]| {
        let sql = format!("SELECT {}", values.join(", "));
        connection.query_row(&sql, [], |row| {
            let mut ledger = ScanLedger::default();
            Ok(build_event_row(row, &mut ledger).map(|_| ()))
        })
    };

    read(&baseline)
        .expect("probe query")
        .expect("the baseline row must read cleanly");

    for (index, name) in [(0, "id"), (1, "aggregate_id"), (3, "type"), (4, "data")] {
        let mut values = baseline;
        values[index] = "NULL";
        let error = read(&values)
            .expect("probe query")
            .err()
            .unwrap_or_else(|| panic!("a NULL {name} must fail the scan, not become \"\""));
        assert!(
            format!("{error:#}").contains("Invalid column type"),
            "a NULL {name} must surface as a column-type error; got {error:#}"
        );
    }
}

#[test]
fn opencode_fixture_uses_the_production_event_index() {
    let path = unique_opencode_db_path("fixture-index");
    let connection = create_opencode_db(&path);

    let mut indexes: Vec<String> = {
        let mut stmt = connection
            .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'event'")
            .expect("prepare index query");
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query indexes");
        rows.map(|row| row.expect("index name")).collect()
    };
    indexes.sort();
    assert!(
        indexes.iter().any(|name| name == "event_aggregate_seq_idx"),
        "fixture must carry the production event index; found {indexes:?}"
    );
    assert!(
        indexes
            .iter()
            .any(|name| name == "event_aggregate_type_seq_idx"),
        "fixture must carry the production type-scoped index; found {indexes:?}"
    );

    // The exact statement `scan_opencode_rows` pages with, taken from the
    // adapter's own constant.
    let plan: Vec<String> = {
        let mut stmt = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                super::OPENCODE_EVENT_PAGE_SQL
            ))
            .expect("prepare explain");
        let rows = stmt
            .query_map(rusqlite::params!["ses_demo", -1_i64, 1_i64, 8_i64], |row| {
                row.get::<_, String>(3)
            })
            .expect("query plan");
        rows.map(|row| row.expect("plan detail")).collect()
    };
    let detail = plan.join("; ");
    assert!(
        detail.contains("USING INDEX event_aggregate_seq_idx"),
        "the paged event scan must use the production index, not a table scan; \
         plan was: {detail}"
    );
    assert!(
        !detail.contains("SCAN event"),
        "the paged event scan must not degrade to a full table scan; plan was: {detail}"
    );

    // The type-scoped read §3.1 Change 3 introduces for the bounded context
    // rebuild. Presence in `sqlite_master` says nothing about the plan, so this
    // is exercised rather than assumed: the measurement that justifies the
    // rebuild (95 rows / 47 KB / 0.51 ms versus 639 / 6.9 MB / 32.8 ms) is only
    // meaningful against an index-driven plan.
    let type_scoped_plan: Vec<String> = {
        let mut stmt = connection
            .prepare(
                "EXPLAIN QUERY PLAN SELECT id, aggregate_id, seq, type, data FROM event \
                 WHERE aggregate_id = ?1 \
                 AND type IN ('session.created.1','session.updated.1','message.updated.1') \
                 ORDER BY seq",
            )
            .expect("prepare type-scoped explain");
        let rows = stmt
            .query_map(rusqlite::params!["ses_demo"], |row| row.get::<_, String>(3))
            .expect("type-scoped plan");
        rows.map(|row| row.expect("plan detail")).collect()
    };
    let type_scoped = type_scoped_plan.join("; ");
    assert!(
        type_scoped.contains("USING INDEX event_aggregate_seq_idx")
            || type_scoped.contains("USING INDEX event_aggregate_type_seq_idx"),
        "the type-scoped context rebuild must be index-driven; plan was: {type_scoped}"
    );
    assert!(
        !type_scoped.contains("SCAN event"),
        "the type-scoped context rebuild must not degrade to a full table scan; \
         plan was: {type_scoped}"
    );

    drop(connection);
    cleanup(&path);
}

/// Issue #601 §2.0 / WI-01, OpenCode arm, restated by WI-06 (§3.1 Change 5).
/// Payload bytes are exactly the lengths of what SQLite materialized in the
/// page loop — the per-aggregate `sum(length(data))` preflight that used to
/// double a cold scan's byte cost is gone — and the census axis carries schema
/// validation plus **one** `event_sequence` read, where WI-01's ledger showed
/// two.
///
/// These equalities are what stop either duplication from silently returning:
/// re-adding the preflight, or a second `opencode_aggregate_sequences` call,
/// moves an exact number.
#[test]
fn opencode_ledger_charges_payload_bytes_at_the_read_site() {
    let path = unique_opencode_db_path("ledger-read-site");
    let connection = seed_opencode_db(&path);
    let expected_bytes: i64 = connection
        .query_row(
            "SELECT coalesce(sum(length(CAST(e.id AS BLOB)) \
             + length(CAST(e.aggregate_id AS BLOB)) \
             + length(CAST(e.type AS BLOB)) \
             + length(CAST(e.data AS BLOB))), 0) \
             FROM event e JOIN event_sequence s ON s.aggregate_id = e.aggregate_id \
             WHERE e.seq <= s.seq",
            [],
            |row| row.get(0),
        )
        .expect("sum event payload bytes");
    let expected_rows: i64 = connection
        .query_row(
            "SELECT count(*) FROM event e JOIN event_sequence s \
             ON s.aggregate_id = e.aggregate_id WHERE e.seq <= s.seq",
            [],
            |row| row.get(0),
        )
        .expect("count read events");
    let aggregate_id_bytes: i64 = connection
        .query_row(
            "SELECT coalesce(sum(length(CAST(aggregate_id AS BLOB))), 0) FROM event_sequence",
            [],
            |row| row.get(0),
        )
        .expect("sum aggregate id bytes");
    let aggregates: i64 = connection
        .query_row("SELECT count(*) FROM event_sequence", [], |row| row.get(0))
        .expect("count aggregates");
    drop(connection);

    let mut ledger = ScanLedger::default();
    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &default_opencode_budget(),
        &mut ledger,
    );
    let records = match outcome {
        OpenCodeScanOutcome::Scanned { records, .. } => records,
        OpenCodeScanOutcome::Failed {
            error_kind,
            error_text,
        } => panic!("ledger scan failed: {error_kind}: {error_text}"),
    };

    assert!(
        expected_rows > 0 && expected_bytes > 0,
        "fixture must have events"
    );
    assert_eq!(
        ledger.payload_bytes, expected_bytes as u64,
        "payload bytes are exactly the materialized column lengths; more means \
         a duplicate read path (the retired preflight) came back, fewer means \
         a read bypassed the ledger"
    );
    assert_eq!(ledger.payload_rows, expected_rows as u64);
    assert_eq!(ledger.rows_emitted, records.len() as u64);
    assert_ne!(
        ledger.rows_emitted, ledger.payload_rows,
        "[DIVERGENT FIXTURE] events read and records emitted must differ, or the \
         two axes are interchangeable and neither is tested"
    );
    let schema_census = {
        let connection = crate::sqlite_poll::open_read_only(path.to_str().expect("utf-8 path"))
            .expect("reopen for schema census");
        crate::sqlite_poll::expected_schema_census(&connection, &["event", "event_sequence"])
    };
    assert!(schema_census.census_rows > 0);
    assert_eq!(
        ledger.census_rows,
        schema_census.census_rows + aggregates as u64,
        "one event_sequence read per scan (§3.1 Change 5), not two"
    );
    assert_eq!(
        ledger.census_bytes,
        schema_census.census_bytes + aggregate_id_bytes as u64
    );

    cleanup(&path);
}

/// The inversion of WI-01's calibration baseline (`opencode_incremental_scan_
/// currently_re_reads_the_whole_aggregate`), exactly as that test demanded:
/// with the page loop starting at the persisted watermark (§3.1 Change 1) and
/// the reconstruction context persisted (Change 2), one appended event costs
/// **exactly that event** — not the aggregate's history and not a context
/// rebuild.
///
/// Fails for: restoring `let mut last_seq = -1;`, a context path that replays
/// history on a warm scan, or any warm-scan read that bypasses the ledger.
#[test]
fn opencode_fast_path_reads_only_the_appended_event() {
    let path = unique_opencode_db_path("incremental-fast-path");
    let connection = seed_opencode_db(&path);

    let mut ledger = ScanLedger::default();
    let cold = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &default_opencode_budget(),
        &mut ledger,
    );
    let OpenCodeScanOutcome::Scanned { new_state, .. } = cold else {
        panic!("cold scan should succeed");
    };
    assert!(
        !new_state.session_contexts.is_empty(),
        "the cold scan must persist the reconstruction context (§3.1 Change 2)"
    );

    let next_seq: i64 = connection
        .query_row(
            "SELECT seq + 1 FROM event_sequence WHERE aggregate_id = 'ses_demo'",
            [],
            |row| row.get(0),
        )
        .expect("read next sequence");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_baseline_append', 'ses_demo', ?1, 'session.updated.1', ?2)",
            rusqlite::params![
                next_seq,
                serde_json::to_string(&json!({
                    "info": {
                        "id": "ses_demo",
                        "directory": "/work/opencode-demo",
                        "title": "OpenCode DB fixture",
                        "time": {"created": 1780000000000_i64, "updated": 1780000009000_i64}
                    }
                }))
                .unwrap()
            ],
        )
        .expect("append one event");
    connection
        .execute(
            "UPDATE event_sequence SET seq = ?1 WHERE aggregate_id = 'ses_demo'",
            rusqlite::params![next_seq],
        )
        .expect("advance sequence");
    let appended_bytes: i64 = connection
        .query_row(
            "SELECT length(CAST(id AS BLOB)) + length(CAST(aggregate_id AS BLOB)) \
             + length(CAST(type AS BLOB)) + length(CAST(data AS BLOB)) \
             FROM event WHERE id = 'evt_baseline_append'",
            [],
            |row| row.get(0),
        )
        .expect("measure appended event");
    drop(connection);

    let mut incremental = ScanLedger::default();
    let warm = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &new_state,
        &default_opencode_budget(),
        &mut incremental,
    );
    assert!(matches!(warm, OpenCodeScanOutcome::Scanned { .. }));
    assert_eq!(
        incremental.payload_rows, 1,
        "one appended event is one payload row"
    );
    assert_eq!(
        incremental.payload_bytes, appended_bytes as u64,
        "one appended event costs exactly that event's bytes (WI-01's baseline \
         measured the whole aggregate's history here)"
    );

    cleanup(&path);
}

fn census_rows(metrics: &Arc<Metrics>) -> u64 {
    metrics
        .sqlite_poll_census_rows_total
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn poll_payload_rows(metrics: &Arc<Metrics>) -> u64 {
    metrics
        .sqlite_poll_payload_rows_total
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Issue #601 §2.0. The ledger is caller-owned so that a scan which reads and
/// then fails is still charged. Every OpenCode ledger test destructured
/// `Scanned` and panicked otherwise, so the guarantee was unverified on every
/// failure arm.
///
/// The failure driven is the headline scenario itself: a mixed-snapshot
/// rejection *after* the page loop read everything — "a scan that reads and
/// then loses the bracket is still charged". (WI-01's version reached a
/// failure through the `MAX_OPENCODE_RELEVANT_ROWS` preflight ceiling; WI-06
/// retired both the ceiling and the preflight.)
///
/// Fails for: resetting or rebuilding the ledger on a `scan_opencode_database`
/// failure arm.
#[test]
fn a_failed_opencode_scan_still_reports_the_bytes_it_had_already_read() {
    let path = unique_opencode_db_path("failure-arm-ledger");
    let connection = seed_opencode_db(&path);
    let expected_bytes: i64 = connection
        .query_row(
            "SELECT coalesce(sum(length(CAST(e.id AS BLOB)) \
             + length(CAST(e.aggregate_id AS BLOB)) \
             + length(CAST(e.type AS BLOB)) \
             + length(CAST(e.data AS BLOB))), 0) \
             FROM event e JOIN event_sequence s ON s.aggregate_id = e.aggregate_id \
             WHERE e.seq <= s.seq",
            [],
            |row| row.get(0),
        )
        .expect("sum event payload bytes");
    drop(connection);
    let db_path = path.to_str().expect("utf-8 path");

    crate::sqlite_poll::contention_injection::arm(db_path, 1);
    let mut ledger = ScanLedger::default();
    let outcome = scan_opencode_database(
        db_path,
        &OpenCodeState::fresh(),
        &default_opencode_budget(),
        &mut ledger,
    );
    crate::sqlite_poll::contention_injection::disarm(db_path);
    let OpenCodeScanOutcome::Failed { error_kind, .. } = outcome else {
        panic!("the armed scan must lose the mixed-snapshot bracket");
    };
    assert_eq!(error_kind, crate::sqlite_poll::ERROR_KIND_MIXED_SNAPSHOT);
    assert_eq!(
        ledger.payload_bytes, expected_bytes as u64,
        "the whole cold read had been paid before the bracket rejected it, and \
         every byte of it must survive onto the failure arm"
    );
    assert!(
        ledger.census_rows > 0,
        "and schema validation plus the event_sequence census are charged too"
    );

    cleanup(&path);
}

/// Issue #601 §2.0. `record_scan_ledger` folds every poll's ledger into the
/// host-global counters, and until now nothing read those counters — no-opping
/// all four call sites left the suite green, which is the exact
/// unfailable-counter pattern this issue exists to remove.
///
/// **[DIVERGENT FIXTURE]** the third poll is arranged so the *rewind preflight*
/// runs and the scan does not: the durable cursor's stat is stale (so the
/// preflight is due) while the volatile entry covers the current stat (so
/// `should_skip_poll` returns before the scan). That isolates
/// `process_opencode_sqlite_db`'s preflight `record_scan_ledger` — the one call
/// site a payload-only counter could never observe.
///
/// Fails for: no-opping either `record_scan_ledger` call site in the OpenCode
/// adapter, or folding only the payload axis into `Metrics`.
#[tokio::test(flavor = "multi_thread")]
async fn opencode_poll_charges_its_ledger_to_the_shared_metrics() {
    let path = unique_opencode_db_path("metrics-wiring");
    let connection = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());
    let config = moraine_config::AppConfig::default();

    let first =
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert!(!first.is_empty(), "the cold poll must emit");
    assert!(
        poll_payload_rows(&metrics) > 0,
        "the scan's ledger must reach the shared counters"
    );
    assert!(census_rows(&metrics) > 0);

    // Move the stat without changing any event, so the durable cursor is stale
    // and the rewind preflight becomes due on the next poll.
    connection
        .execute(
            "INSERT INTO credential (id, value, time_created, time_updated) \
             VALUES ('cred_churn', 'x', 1, 1)",
            [],
        )
        .expect("irrelevant write");
    drop(connection);

    let generation = checkpoints
        .read()
        .await
        .get(&cp_key)
        .map(|cp| cp.source_generation)
        .expect("committed checkpoint");
    let current_stat =
        crate::sqlite_poll::stat_fingerprint(&work.path).expect("fixture stat fingerprint");
    poll_state.record_noop_scan(&cp_key, generation, current_stat);

    let payload_before = poll_payload_rows(&metrics);
    let census_before = census_rows(&metrics);
    let third =
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert!(third.is_empty(), "the volatile entry must skip the scan");
    assert_eq!(
        poll_payload_rows(&metrics),
        payload_before,
        "the scan really was skipped, so nothing charged the payload axis"
    );
    assert!(
        census_rows(&metrics) > census_before,
        "but the rewind preflight still ran and still owes its census; a poll \
         that reads must never be invisible to the counters"
    );

    cleanup(&path);
}

/// Issue #601 §2.1(2) / §2.5. The failure backoff has to gate the **whole
/// poll**, not just the barrier and the scan behind it.
///
/// The rewind preflight opens the database and walks `event_sequence`, and it
/// used to run *ahead* of the blocked-replay throttle — so a durably blocked
/// OpenCode database still paid for a read, and still charged metrics, on every
/// tick while the barrier and the scan were correctly suppressed. Cheap today,
/// but "gate the barrier, not just the scan" has to apply to the whole poll or
/// the next read added ahead of the throttle inherits the same hole.
///
/// **[DIVERGENT FIXTURE]** the durable cursor's stat is stale, so the preflight
/// is genuinely due; without that the preflight would not run either way and
/// the two behaviours would coincide.
///
/// Fails for: moving the throttle back below the preflight.
#[tokio::test(flavor = "multi_thread")]
async fn a_throttled_blocked_replay_does_not_run_the_rewind_preflight() {
    let path = unique_opencode_db_path("throttled-preflight");
    let connection = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());
    let config = moraine_config::AppConfig::default();

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;

    // Move the stat without touching any event, so the preflight is due.
    connection
        .execute(
            "INSERT INTO credential (id, value, time_created, time_updated) \
             VALUES ('cred_churn', 'x', 1, 1)",
            [],
        )
        .expect("irrelevant write");
    drop(connection);

    // Durably block the source and start its failure backoff, exactly as a
    // failed replacement replay would.
    let generation = {
        let mut map = checkpoints.write().await;
        let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
        checkpoint.status = "error".to_string();
        checkpoint.block_reason = "seeded blocked replay".to_string();
        checkpoint.final_scan_complete = false;
        checkpoint.source_generation
    };
    poll_state.record_failed_scan(&cp_key, generation);
    assert!(
        !poll_state.failure_retry_due(&cp_key, generation),
        "the fixture must actually be inside the backoff window, or this guard \
         cannot fail"
    );

    let census_before = census_rows(&metrics);
    let payload_before = poll_payload_rows(&metrics);
    let batches =
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert!(batches.is_empty(), "a throttled poll sends nothing");
    assert_eq!(
        poll_payload_rows(&metrics),
        payload_before,
        "the scan is throttled"
    );
    assert_eq!(
        census_rows(&metrics),
        census_before,
        "and so is the preflight — a throttled poll must not open the database \
         at all"
    );

    cleanup(&path);
}

/// Drives one poll against a durably blocked OpenCode source that is inside its
/// failure-backoff window, and returns the checkpoint afterwards.
///
/// `mutate` seeds the blocked state and may adjust the checkpoint further (a
/// replaced inode, say). The volatile entry is put into the backoff window
/// *after* it runs, and the fixture asserts the window is real, so a test built
/// on this cannot pass by accident.
async fn poll_a_throttled_blocked_replay(
    config: &moraine_config::AppConfig,
    work: &WorkItem,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    mutate: impl FnOnce(&mut Checkpoint),
) -> Checkpoint {
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());
    let cold_config = moraine_config::AppConfig::default();

    drive_opencode_poll_with_metrics(&cold_config, work, checkpoints, &poll_state, &metrics).await;

    let generation = {
        let mut map = checkpoints.write().await;
        let checkpoint = map
            .get_mut(&cp_key)
            .expect("the cold poll commits a checkpoint");
        checkpoint.status = "error".to_string();
        checkpoint.block_reason = "seeded blocked replay".to_string();
        checkpoint.final_scan_complete = false;
        mutate(checkpoint);
        checkpoint.source_generation
    };
    poll_state.record_failed_scan(&cp_key, generation);
    assert!(
        !poll_state.failure_retry_due(&cp_key, generation),
        "the fixture must actually be inside the backoff window, or these \
         guards cannot fail"
    );

    drive_opencode_poll_with_metrics(config, work, checkpoints, &poll_state, &metrics).await;

    checkpoints
        .read()
        .await
        .get(&cp_key)
        .cloned()
        .expect("the checkpoint survives the retry")
}

/// Issue #601 §2.1(2)/§2.5 — the **width** of OpenCode's blocked-replay
/// throttle gate, first conjunct.
///
/// This gate is not the redundant one it looks like. The Cursor and NAC gates
/// carry `&& !starts_replacement` *below* the generation bump, so
/// `failure_retry_due` is asked about a generation the volatile entry has never
/// seen and returns `true` whatever the conjunct says — dropping it there is an
/// equivalent mutant. OpenCode's gate has to sit **above** the bump, because
/// the bump depends on `sequence_rewound` and that is what the rewind preflight
/// this gate exists to suppress computes. So `checkpoint.source_generation` is
/// still the old value here and `failure_retry_due` genuinely returns `false`.
///
/// Consequence of dropping `&& !generation_changed`: an OpenCode store that was
/// durably blocked and is then **replaced** — a new inode, so every logical
/// identity and every event UID starts over — is ignored for up to
/// `FAILURE_BACKOFF_MAX` (15 minutes) instead of replaying on the next tick.
///
/// **[DIVERGENT FIXTURE]** the volatile entry is genuinely inside the backoff
/// window (asserted), so the gate's last conjunct is true and the first is the
/// only thing holding the poll open.
///
/// Fails for: dropping `&& !generation_changed` from the throttle gate.
#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_opencode_database_bypasses_the_blocked_replay_throttle() {
    let path = unique_opencode_db_path("replaced-under-throttle");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let config = moraine_config::AppConfig::default();

    let mut generation_before = 0;
    let after = poll_a_throttled_blocked_replay(&config, &work, &checkpoints, |checkpoint| {
        generation_before = checkpoint.source_generation;
        // The database was replaced under the block. Recording a different
        // inode is what a replacement looks like to the poll, and is
        // deterministic where deleting and recreating the file is not.
        checkpoint.source_inode = checkpoint.source_inode.wrapping_add(1);
    })
    .await;

    assert_eq!(
        after.source_generation,
        generation_before + 1,
        "a replaced database is a new logical identity and must start its \
         generation at once, not after a 15-minute backoff"
    );
    assert_eq!(after.status, "active");
    assert!(after.final_scan_complete);
    assert!(after.block_reason.is_empty());

    cleanup(&path);
}

/// Issue #601 §2.1(2)/§2.5 — the same gate's **second** conjunct.
///
/// Consequence of dropping `&& !exclusions_changed`: an operator who widens or
/// narrows `exclude_project_dirs` to unblock a source waits out the failure
/// backoff before the policy takes effect, up to 15 minutes after the config
/// change. The width argument is the same as its sibling above — this gate runs
/// above the generation bump, so the fall-through conjunct cannot cover it.
///
/// Fails for: dropping `&& !exclusions_changed` from the throttle gate.
#[tokio::test(flavor = "multi_thread")]
async fn an_exclusion_change_bypasses_the_opencode_blocked_replay_throttle() {
    let path = unique_opencode_db_path("exclusions-under-throttle");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));

    // The cold poll inside the helper runs under the default policy; the
    // throttled retry runs under a changed one.
    let mut changed = moraine_config::AppConfig::default();
    changed.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];

    let mut generation_before = 0;
    let after = poll_a_throttled_blocked_replay(&changed, &work, &checkpoints, |checkpoint| {
        generation_before = checkpoint.source_generation;
    })
    .await;

    assert_eq!(
        after.source_generation,
        generation_before + 1,
        "a changed exclusion policy replays immediately; rows skipped under \
         the prior policy must not wait out a failure backoff"
    );
    assert_eq!(after.status, "active");
    assert!(after.final_scan_complete);
    assert!(after.block_reason.is_empty());

    cleanup(&path);
}

/// Issue #601 §2.1(2) — OpenCode's half of "a durable `BeginReplay` barrier is
/// never sent with no scan behind it". The Cursor site is pinned by
/// `blocked_replay_scans_behind_its_barrier`; the identical guard at OpenCode
/// was not, and dropping `!replacement_replay` there was green.
///
/// `should_skip_poll` runs *after* the barrier, and a blocked-replay retry
/// reuses its generation, so a volatile entry covering the current stat skips
/// the scan while the barrier has already been persisted — the source stays in
/// `replaying` forever, one barrier per tick.
///
/// **[DIVERGENT FIXTURE]** the volatile entry must genuinely cover the current
/// stat; with an uncovered stat the skip never triggers and the two behaviours
/// coincide.
///
/// Fails for: dropping the `!replacement_replay` guard on the
/// `should_skip_poll` call.
#[tokio::test(flavor = "multi_thread")]
async fn an_opencode_blocked_replay_scans_behind_its_barrier() {
    let path = unique_opencode_db_path("blocked-replay-scan");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let config = moraine_config::AppConfig::default();
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;

    let generation = {
        let mut map = checkpoints.write().await;
        let checkpoint = map
            .get_mut(&cp_key)
            .expect("the cold poll commits a checkpoint");
        checkpoint.status = "error".to_string();
        checkpoint.block_reason = "seeded blocked replay".to_string();
        checkpoint.final_scan_complete = false;
        checkpoint.source_generation
    };

    // And make the volatile entry claim the current stat as covered, which is
    // the only state in which the post-barrier skip can fire.
    let current_stat = stat_fingerprint(&work.path).expect("fixture stat");
    poll_state.record_noop_scan(&cp_key, generation, current_stat);
    assert!(
        poll_state.should_skip_poll(&cp_key, generation, &current_stat),
        "the fixture must actually reach the skip condition, or this guard \
         cannot fail"
    );

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;

    let after = checkpoints
        .read()
        .await
        .get(&cp_key)
        .cloned()
        .expect("the checkpoint survives the retry");
    assert_eq!(
        after.status, "active",
        "a blocked replay retry must run its scan and finalize, not send a \
         barrier and skip"
    );
    assert!(after.final_scan_complete);
    assert!(after.block_reason.is_empty());

    cleanup(&path);
}

/// Issue #601 §2.5 — the other direction on the same call. Cursor's
/// `failed_scan_backs_off_instead_of_rescanning_every_tick` fails when the
/// `should_skip_poll` call is removed outright; OpenCode had no equivalent, so
/// disabling its call was green and the §2.5 defect could come straight back
/// on this adapter alone.
///
/// Denominated on an **observed scan count**, never on absence of
/// `ingest_errors` rows: those are rate-limited by `state.last_error`, so
/// `opencode_sqlite_schema_mismatch_emits_one_error_and_preserves_cursor` stays
/// green no matter how many scans ran.
///
/// Fails for: removing the `should_skip_poll` call, or reverting
/// `record_failed_scan` to its `get_mut`-only form (a first failing scan has no
/// prior entry to modify).
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_opencode_scan_backs_off_instead_of_rescanning_every_tick() {
    let path = unique_opencode_db_path("failure-backoff-first-scan");
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
    let config = moraine_config::AppConfig::default();
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());

    for _ in 0..10 {
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    }

    let failures = metrics
        .sqlite_scan_failures_total
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        failures >= 1,
        "the first tick must actually attempt the scan"
    );
    assert!(
        failures <= 2,
        "10 ticks against an OpenCode database whose first scan fails must not \
         run 10 scans; observed {failures}"
    );

    cleanup(&path);
}

/// Issue #601 §2.1(2) — OpenCode's half of the `retry_blocked_replay` narrowing
/// width. See the Cursor twin
/// `a_crash_interrupted_replay_resumes_from_its_replaying_status` for the
/// argument; here the predicate additionally feeds the blocked-replay throttle
/// gate, so its width is load-bearing for code this PR adds.
///
/// **[DIVERGENT FIXTURE]** the stat is unchanged since the cold poll, which is
/// what a crashed replay looks like and what makes the two behaviours diverge:
/// with the disjunct the cursor is reset and the scan runs, without it the
/// unchanged stat and `event_scan_complete` return early.
///
/// Fails for: dropping the `checkpoint.status == "replaying"` disjunct.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_interrupted_opencode_replay_resumes_from_its_replaying_status() {
    let path = unique_opencode_db_path("replaying-status-resume");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let config = moraine_config::AppConfig::default();
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;

    // Exactly what a crash between `BeginReplay` and `FinalizeReplay` leaves:
    // `replaying`, no error, no block reason.
    {
        let mut map = checkpoints.write().await;
        let checkpoint = map
            .get_mut(&cp_key)
            .expect("the cold poll commits a checkpoint");
        checkpoint.status = "replaying".to_string();
        checkpoint.block_reason.clear();
        checkpoint.final_scan_complete = false;
    }

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;

    let after = checkpoints
        .read()
        .await
        .get(&cp_key)
        .cloned()
        .expect("the checkpoint survives the retry");
    assert_eq!(
        after.status, "active",
        "an interrupted replay must resume and finish, not be relabelled"
    );
    assert!(
        after.final_scan_complete,
        "a resumed replay finalizes; without the `replaying` disjunct the poll \
         returns on the unchanged stat and the source is stuck"
    );

    cleanup(&path);
}

/// Plan §7.2 F2, the widening direction, at the OpenCode call site: an
/// `error`-status checkpoint with **no block reason** is an ordinary
/// transient failure marker, and the poll it precedes is an ordinary poll.
/// Widening `retry_blocked_replay` to a bare `status == "error"` turns it
/// into a blocked-replacement retry: the watermark resets and every event
/// replays behind a fresh `BeginReplay`. The unchanged fixture emits nothing
/// on the correct path, so any re-emission fails this test.
///
/// MUTATION (executed 2026-07-31): drop
/// `&& !checkpoint.block_reason.is_empty()` from OpenCode's
/// `retry_blocked_replay` — this test fails (events replay); RED was confirmed
/// in a filtered run, so suite-wide isolation is not claimed.
#[tokio::test(flavor = "multi_thread")]
async fn an_error_marker_without_a_block_reason_is_not_retried_as_a_blocked_opencode_replay() {
    let path = unique_opencode_db_path("error-marker-width");
    let _db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));

    let first = run_opencode_poll(&work, &checkpoints).await;
    assert!(!all_event_rows(&first).is_empty());

    // Rewrite the committed checkpoint into a transient-error marker: the
    // shape the non-replay failure arm persists.
    {
        let mut map = checkpoints.write().await;
        let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
        checkpoint.status = "error".to_string();
        checkpoint.block_reason.clear();
        let mut state = OpenCodeState::parse(&checkpoint.cursor_json);
        state.last_error = crate::sqlite_poll::ERROR_KIND_SCAN.to_string();
        checkpoint.cursor_json = state.serialize();
    }

    let batches = run_opencode_poll(&work, &checkpoints).await;
    assert!(
        all_event_rows(&batches).is_empty(),
        "an ordinary error marker must clear through an ordinary scan; \
         re-emission means the poll was retried as a blocked replay"
    );
    let map = checkpoints.read().await;
    assert_eq!(
        map.get(&cp_key).expect("checkpoint").status,
        "active",
        "the transient marker clears once a scan succeeds"
    );

    cleanup(&path);
}

/// Gate G1b (§8): the fast path must not replay part events. **[DIVERGENT
/// FIXTURE]** 500 part events of ~16 KB each (~8 MB) against a dozen
/// context-bearing events of ~1 KB — if part events were small, a full replay
/// would pass the byte budget and the gate could not fail.
///
/// Two halves, one per §3.1 mechanism:
///
/// - **persisted context** (Change 2): with the cold cursor intact, one
///   appended part event costs far under 128 KiB;
/// - **bounded rebuild** (Change 3): with the contexts stripped (an evicted
///   or pre-upgrade cursor), the type-scoped rebuild reads the context events
///   only — still far under 128 KiB — and the emitted part is enriched from
///   the rebuilt context, proving the rebuild is real and not just cheap.
///
/// Fails for: restoring `last_seq = -1`, or a context rebuild that drops the
/// `type` filter (8 MB of part events crosses the 128 KiB line immediately).
#[test]
fn opencode_fast_path_does_not_replay_part_events() {
    let path = unique_opencode_db_path("g1b-part-replay");
    let connection = create_opencode_db(&path);
    let part_padding = "p".repeat(16 * 1024);
    let context_padding = "c".repeat(1024);
    connection.execute_batch("BEGIN").expect("begin bulk seed");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_bulk_session', 'ses_bulk', 0, 'session.created.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_bulk",
                "info": {
                    "id": "ses_bulk",
                    "directory": "/work/bulk",
                    "title": "bulk session",
                    "model": {"id": "glm-5.2", "providerID": "zai-coding-plan"},
                    "time": {"created": 1780000000000_i64, "updated": 1780000000000_i64}
                }
            }))
            .unwrap()],
        )
        .expect("insert bulk session event");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_bulk_message', 'ses_bulk', 1, 'message.updated.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_bulk",
                "info": {
                    "id": "msg_bulk",
                    "sessionID": "ses_bulk",
                    "role": "assistant",
                    "modelID": "glm-5.2",
                    "providerID": "zai-coding-plan",
                    "note": context_padding,
                    "time": {"created": 1780000000100_i64}
                }
            }))
            .unwrap()],
        )
        .expect("insert bulk message event");
    {
        let mut stmt = connection
            .prepare(
                "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_bulk', ?2, ?3, ?4)",
            )
            .expect("prepare bulk insert");
        for idx in 0..500i64 {
            stmt.execute(rusqlite::params![
                format!("evt_bulk_part_{idx:04}"),
                2 + idx,
                "message.part.updated.1",
                serde_json::to_string(&json!({
                    "sessionID": "ses_bulk",
                    "part": {
                        "id": format!("part_bulk_{idx:04}"),
                        "messageID": "msg_bulk",
                        "sessionID": "ses_bulk",
                        "type": "text",
                        "text": part_padding,
                        "time": {"start": 1780000001000_i64 + idx}
                    },
                    "time": 1780000001000_i64 + idx
                }))
                .unwrap(),
            ])
            .expect("insert bulk part event");
        }
        // Ten more small context events, per the G1b fixture spec.
        for idx in 0..10i64 {
            stmt.execute(rusqlite::params![
                format!("evt_bulk_ctx_{idx:02}"),
                502 + idx,
                "message.updated.1",
                serde_json::to_string(&json!({
                    "sessionID": "ses_bulk",
                    "info": {
                        "id": "msg_bulk",
                        "sessionID": "ses_bulk",
                        "role": "assistant",
                        "modelID": "glm-5.2",
                        "providerID": "zai-coding-plan",
                        "note": context_padding,
                        "time": {"created": 1780000002000_i64 + idx}
                    }
                }))
                .unwrap(),
            ])
            .expect("insert bulk context event");
        }
    }
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_bulk', 511, NULL)",
            [],
        )
        .expect("insert bulk sequence");
    connection
        .execute_batch("COMMIT")
        .expect("commit bulk seed");

    let mut cold_ledger = ScanLedger::default();
    let cold = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &default_opencode_budget(),
        &mut cold_ledger,
    );
    let OpenCodeScanOutcome::Scanned { new_state, .. } = cold else {
        panic!("cold scan should succeed");
    };

    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_bulk_append', 'ses_bulk', 512, 'message.part.updated.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_bulk",
                "part": {
                    "id": "part_bulk_append",
                    "messageID": "msg_bulk",
                    "sessionID": "ses_bulk",
                    "type": "text",
                    "text": part_padding,
                    "time": {"start": 1780000003000_i64}
                },
                "time": 1780000003000_i64
            }))
            .unwrap()],
        )
        .expect("append part event");
    connection
        .execute(
            "UPDATE event_sequence SET seq = 512 WHERE aggregate_id = 'ses_bulk'",
            [],
        )
        .expect("advance bulk sequence");
    drop(connection);

    // Half one: persisted context intact.
    let mut warm_ledger = ScanLedger::default();
    let warm = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &new_state,
        &default_opencode_budget(),
        &mut warm_ledger,
    );
    assert!(matches!(warm, OpenCodeScanOutcome::Scanned { .. }));
    assert!(
        warm_ledger.payload_bytes < 128 * 1024,
        "one appended part event must not replay 8 MB of part history; read {} bytes",
        warm_ledger.payload_bytes
    );

    // Half two: contexts stripped — the bounded rebuild path.
    let mut stripped = (*new_state).clone();
    stripped.session_contexts.clear();
    stripped.message_contexts.clear();
    let mut rebuild_ledger = ScanLedger::default();
    let rebuilt = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &stripped,
        &default_opencode_budget(),
        &mut rebuild_ledger,
    );
    let OpenCodeScanOutcome::Scanned { records, .. } = rebuilt else {
        panic!("rebuild scan should succeed");
    };
    assert!(
        rebuild_ledger.payload_bytes < 128 * 1024,
        "the type-scoped rebuild must read context events only; read {} bytes",
        rebuild_ledger.payload_bytes
    );
    let part = records
        .iter()
        .find(|record| record.record.get("type").and_then(Value::as_str) == Some("opencode_part"))
        .expect("the appended part re-emits");
    assert_eq!(
        part.record.get("message_role").and_then(Value::as_str),
        Some("assistant"),
        "the emitted part must be enriched from the rebuilt context — a rebuild \
         that reads nothing would pass the byte bound and emit an unenriched part"
    );
    assert_eq!(
        part.project_dir, "/work/bulk",
        "project_dir derivation must survive the rebuild (it drives exclusion \
         and backend routing — §3.1 calls this a correctness bug, not cosmetic)"
    );

    cleanup(&path);
}

/// Gate G3 (§8): an OpenCode session growing to N events has bounded per-poll
/// incremental work. Parameterized over N ∈ {100, 1,000, 10,000} events in
/// one aggregate, appending one identical event at each size: the appended
/// event's cost at N = 10,000 must be within 2× of N = 100.
///
/// The fixture carries the production indexes (WI-01) — without them this
/// measurement would be of a full table scan and miscalibrated in both
/// directions (the §8 calibration mutation).
///
/// Fails for: reintroducing linear replay, or a context rebuild proportional
/// to history.
#[test]
fn opencode_incremental_work_is_flat_in_aggregate_length() {
    let path = unique_opencode_db_path("g3-flat");
    let connection = create_opencode_db(&path);
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_flat_session', 'ses_flat', 0, 'session.created.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_flat",
                "info": {
                    "id": "ses_flat",
                    "directory": "/work/flat",
                    "title": "flat session",
                    "time": {"created": 1780000000000_i64, "updated": 1780000000000_i64}
                }
            }))
            .unwrap()],
        )
        .expect("insert flat session event");
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_flat', 0, NULL)",
            [],
        )
        .expect("insert flat sequence");

    let mut filled = 0i64;
    let mut fill_to = |target: i64| {
        connection.execute_batch("BEGIN").expect("begin fill");
        {
            let mut stmt = connection
                .prepare(
                    "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'ses_flat', ?2, 'message.part.updated.1', ?3)",
                )
                .expect("prepare fill");
            while filled < target {
                let seq = filled + 1;
                stmt.execute(rusqlite::params![
                    format!("evt_flat_{seq:06}"),
                    seq,
                    serde_json::to_string(&json!({
                        "sessionID": "ses_flat",
                        "part": {
                            "id": format!("part_flat_{seq:06}"),
                            "messageID": "msg_flat",
                            "sessionID": "ses_flat",
                            "type": "text",
                            "text": format!("flat body {seq}"),
                            "time": {"start": 1780000000000_i64 + seq}
                        },
                        "time": 1780000000000_i64 + seq
                    }))
                    .unwrap(),
                ])
                .expect("insert fill event");
                filled += 1;
            }
        }
        connection
            .execute(
                "UPDATE event_sequence SET seq = ?1 WHERE aggregate_id = 'ses_flat'",
                rusqlite::params![target],
            )
            .expect("advance fill sequence");
        connection.execute_batch("COMMIT").expect("commit fill");
    };

    let db_path = path.to_str().expect("utf-8 path").to_string();
    let mut state = OpenCodeState::fresh();
    let mut measured = Vec::new();
    for target in [100i64, 1_000, 10_000] {
        fill_to(target);
        // Catch up to the new history so the *next* append is incremental.
        // Looped: one poll's work budget legitimately bounds a single scan
        // (2,000 rows at the shipped default), and production covers a large
        // backlog across successive polls exactly like this.
        loop {
            let mut catch_up = ScanLedger::default();
            let caught =
                scan_opencode_database(&db_path, &state, &default_opencode_budget(), &mut catch_up);
            let OpenCodeScanOutcome::Scanned { new_state, .. } = caught else {
                panic!("catch-up scan should succeed at N = {target}");
            };
            state = *new_state;
            if !state.pending_coverage {
                break;
            }
        }

        // The measured append: byte-identical at every N.
        fill_to(target + 1);
        let mut ledger = ScanLedger::default();
        let scanned =
            scan_opencode_database(&db_path, &state, &default_opencode_budget(), &mut ledger);
        let OpenCodeScanOutcome::Scanned { new_state, .. } = scanned else {
            panic!("incremental scan should succeed at N = {target}");
        };
        state = *new_state;
        measured.push(ledger.payload_bytes);
    }

    let at_100 = measured[0];
    let at_10_000 = measured[2];
    assert!(at_100 > 0, "the appended event must cost something");
    assert!(
        at_10_000 <= 2 * at_100,
        "per-poll incremental work must be flat in aggregate length: \
         {at_100} bytes at N=100 vs {at_10_000} bytes at N=10,000"
    );

    cleanup(&path);
}

/// Gate G8b (§8) and §3.1 Change 7: an aggregate *disappearing* from
/// `event_sequence` (one deleted old session) is not a rewind. It must drop
/// its watermark and context entries durably — which is also §7.2 F1's
/// `new_state == prior_state_covered` conjunct for OpenCode: the deletion
/// emits nothing and moves only the structural state, so only that comparison
/// keeps the checkpoint from being suppressed — and it must not bump the
/// generation or re-ingest anything.
///
/// The genuine-regression side (a `seq` that moves backwards still routes
/// through `begin_database_replay`) is pinned by
/// `opencode_sqlite_sequence_regression_resets_aggregate_cursor`, which stays
/// green beside this test — together they bound the `is_some_and` fix from
/// both directions.
///
/// Fails for: the old `is_none_or` conflation (a bump and a full re-ingest),
/// or dropping the state conjunct from `scan_is_noop` (the drop never
/// persists).
#[tokio::test(flavor = "multi_thread")]
async fn opencode_disappearing_aggregate_is_not_a_generation_bump() {
    let path = unique_opencode_db_path("g8b-disappearance");
    let db = seed_opencode_db(&path);
    db.execute(
        "INSERT INTO event (id, aggregate_id, seq, type, data) \
         VALUES ('evt_second_session', 'ses_second', 0, 'session.created.1', ?1)",
        rusqlite::params![serde_json::to_string(&json!({
            "sessionID": "ses_second",
            "info": {
                "id": "ses_second",
                "directory": "/work/second",
                "title": "second session",
                "time": {"created": 1780000200000_i64, "updated": 1780000200000_i64}
            }
        }))
        .unwrap()],
    )
    .expect("insert second aggregate event");
    db.execute(
        "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_second', 0, NULL)",
        [],
    )
    .expect("insert second sequence");

    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());
    let config = moraine_config::AppConfig::default();

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    let generation_before = checkpoints
        .read()
        .await
        .get(&cp_key)
        .expect("cold checkpoint")
        .source_generation;

    // The old session is deleted wholesale: events and sequence row.
    db.execute("DELETE FROM event WHERE aggregate_id = 'ses_second'", [])
        .expect("delete second events");
    db.execute(
        "DELETE FROM event_sequence WHERE aggregate_id = 'ses_second'",
        [],
    )
    .expect("delete second sequence");
    drop(db);

    let rows_before = poll_payload_rows(&metrics);
    let batches =
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
    assert_eq!(raw_rows, 0, "a disappearance emits nothing");
    assert_eq!(
        poll_payload_rows(&metrics),
        rows_before,
        "and re-ingests nothing — no payload row is read"
    );
    let map = checkpoints.read().await;
    let checkpoint = map.get(&cp_key).expect("post-disappearance checkpoint");
    assert_eq!(
        checkpoint.source_generation, generation_before,
        "a disappearance must not bump the generation"
    );
    let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor parses");
    assert!(
        cursor.pointer("/aggregate_sequences/ses_second").is_none(),
        "the vanished aggregate's watermark drops durably"
    );
    assert!(
        cursor.pointer("/session_contexts/ses_second").is_none(),
        "and its context entry drops with it (§3.1 Change 7)"
    );
    assert!(
        cursor.pointer("/aggregate_sequences/ses_demo").is_some(),
        "the surviving aggregate's watermark is untouched"
    );

    cleanup(&path);
}

/// Plan §7.2 F1, OpenCode's `schema_fingerprint == checkpoint.schema_fingerprint`
/// conjunct of `scan_is_noop`: a schema change with no row changes emits
/// nothing, and only the fingerprint comparison keeps its checkpoint from
/// being suppressed and the drift re-discovered on every later poll.
///
/// Fails for: dropping the schema conjunct from OpenCode's `scan_is_noop`.
#[tokio::test(flavor = "multi_thread")]
async fn an_opencode_schema_change_with_no_row_changes_still_persists_its_checkpoint() {
    let path = unique_opencode_db_path("schema-conjunct");
    let db = seed_opencode_db(&path);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let config = moraine_config::AppConfig::default();

    drive_opencode_poll(&config, &work, &checkpoints, &poll_state).await;
    let fingerprint_before = checkpoints
        .read()
        .await
        .get(&cp_key)
        .expect("cold checkpoint")
        .schema_fingerprint;

    db.execute("ALTER TABLE event ADD COLUMN extra text", [])
        .expect("add drifting column");
    drop(db);

    let batches = drive_opencode_poll(&config, &work, &checkpoints, &poll_state).await;
    let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
    assert_eq!(raw_rows, 0, "a schema change alone emits nothing");
    let fingerprint_after = checkpoints
        .read()
        .await
        .get(&cp_key)
        .expect("post-drift checkpoint")
        .schema_fingerprint;
    assert_ne!(
        fingerprint_before, fingerprint_after,
        "the moved fingerprint must persist durably, not be suppressed as a \
         noop and re-discovered on every later poll"
    );

    cleanup(&path);
}

/// Gate G4, row 4 (§8) — the OpenCode ceiling retirement WI-06 owns (§7.1
/// D9): a store past any former ceiling keeps ingesting recent work with
/// `coverage_degraded`, and no `sqlite_cursor_too_large` row is ever minted.
///
/// **[DIVERGENT FIXTURE]** the newest work must not be what the old any-order
/// scan reached first: the cold never-read aggregate sorts *first* by
/// `aggregate_id` (`aaa_cold`), the actively written known aggregate sorts
/// *last* (`zzz_active`) — so a budget that binds after the delta class only
/// emits the new events if delta-before-cold ordering (§2.3's table row for
/// OpenCode: `event_sequence.seq > persisted seq` is the exact recency
/// signal) actually works.
///
/// Fails for: a budget that fails the scan instead of degrading (the old
/// `TooLarge` latch), bounded progress that walks `aggregate_id` order
/// blindly, or a degraded poll that loses the cold aggregate's debt.
#[tokio::test(flavor = "multi_thread")]
async fn an_opencode_store_over_budget_still_ingests_the_newest_events_first() {
    let path = unique_opencode_db_path("g4-budget-order");
    let db = Connection::open(&path).expect("create db");
    drop(db);
    let db = seed_opencode_db(&path);

    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let mut config = moraine_config::AppConfig::default();

    // Cold-cover the demo aggregate first, unbudgeted.
    drive_opencode_poll(&config, &work, &checkpoints, &poll_state).await;

    // A big cold aggregate that sorts before everything...
    db.execute_batch("BEGIN").expect("begin cold seed");
    {
        let mut stmt = db
            .prepare(
                "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, 'aaa_cold', ?2, 'message.part.updated.1', ?3)",
            )
            .expect("prepare cold insert");
        for seq in 0..40i64 {
            stmt.execute(rusqlite::params![
                format!("evt_cold_{seq:04}"),
                seq,
                serde_json::to_string(&json!({
                    "sessionID": "aaa_cold",
                    "part": {
                        "id": format!("part_cold_{seq:04}"),
                        "messageID": "msg_cold",
                        "sessionID": "aaa_cold",
                        "type": "text",
                        "text": format!("cold body {seq}"),
                        "time": {"start": 1780000100000_i64 + seq}
                    },
                    "time": 1780000100000_i64 + seq
                }))
                .unwrap(),
            ])
            .expect("insert cold event");
        }
    }
    db.execute(
        "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('aaa_cold', 39, NULL)",
        [],
    )
    .expect("insert cold sequence");
    db.execute_batch("COMMIT").expect("commit cold seed");
    // ...and fresh delta work on the covered aggregate that sorts after it.
    let next_seq: i64 = db
        .query_row(
            "SELECT seq + 1 FROM event_sequence WHERE aggregate_id = 'ses_demo'",
            [],
            |row| row.get(0),
        )
        .expect("read next demo sequence");
    db.execute(
        "INSERT INTO event (id, aggregate_id, seq, type, data) \
         VALUES ('evt_recent_work', 'ses_demo', ?1, 'message.part.updated.1', ?2)",
        rusqlite::params![
            next_seq,
            serde_json::to_string(&json!({
                "sessionID": "ses_demo",
                "part": {
                    "id": "part_recent_work",
                    "messageID": "msg_assistant",
                    "sessionID": "ses_demo",
                    "type": "text",
                    "text": "the newest work in the store",
                    "time": {"start": 1780000200000_i64}
                },
                "time": 1780000200000_i64
            }))
            .unwrap()
        ],
    )
    .expect("append recent work");
    db.execute(
        "UPDATE event_sequence SET seq = ?1 WHERE aggregate_id = 'ses_demo'",
        rusqlite::params![next_seq],
    )
    .expect("advance demo sequence");
    drop(db);

    // A budget the cold aggregate alone would exhaust many times over.
    config.ingest.sqlite.fast_path_max_payload_rows = 2;
    let batches = drive_opencode_poll(&config, &work, &checkpoints, &poll_state).await;
    let event_rows = all_event_rows(&batches);
    assert!(
        event_rows
            .iter()
            .any(|row| row.get("text_content").and_then(Value::as_str)
                == Some("the newest work in the store")),
        "the delta class must be read before the cold debt — recent work keeps \
         flowing while history is over budget"
    );
    let error_rows: usize = batches.iter().map(|batch| batch.error_rows.len()).sum();
    assert_eq!(
        error_rows, 0,
        "history size is a degradation, never an error (no TooLarge rows)"
    );
    let map = checkpoints.read().await;
    let checkpoint = map.get(&cp_key).expect("degraded checkpoint");
    let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor parses");
    assert_eq!(
        cursor.get("pending_coverage").and_then(Value::as_bool),
        Some(true),
        "the cold debt is a durable resume marker"
    );
    assert_eq!(
        cursor
            .pointer("/aggregate_sequences/ses_demo")
            .and_then(Value::as_i64),
        Some(next_seq),
        "the delta aggregate's watermark reaches the new event"
    );

    cleanup(&path);
}

/// §2.3's "continue next poll" for OpenCode, end to end, with **no further
/// writes**: a 1-row budget against two cold aggregates converges to full
/// coverage across resumed polls — the persisted resume marker keeps the
/// quiet store scanning — then clears the marker durably and quiesces, with
/// `cursor_json` omitting the flag entirely once false (§2.6 byte-identity).
///
/// Fails for: dropping the `!pending_coverage` conjunct from the cheap
/// short-circuit (the unchanged stat ends every later poll and the remainder
/// is unreachable forever), or a budget break that loses the remainder.
#[tokio::test(flavor = "multi_thread")]
async fn a_degraded_opencode_cold_ingest_completes_without_new_writes() {
    let path = unique_opencode_db_path("d6-cold-converges");
    let db = Connection::open(&path).expect("create db");
    drop(db);
    let connection = create_opencode_db(&path);
    for (aggregate, base) in [("ses_a", 1780000100000_i64), ("ses_b", 1780000200000_i64)] {
        connection
            .execute(
                "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, ?2, 0, 'session.created.1', ?3)",
                rusqlite::params![
                    format!("evt_{aggregate}_session"),
                    aggregate,
                    serde_json::to_string(&json!({
                        "sessionID": aggregate,
                        "info": {
                            "id": aggregate,
                            "directory": format!("/work/{aggregate}"),
                            "title": format!("session {aggregate}"),
                            "time": {"created": base, "updated": base}
                        }
                    }))
                    .unwrap(),
                ],
            )
            .expect("insert session event");
        for seq in 1..3i64 {
            connection
                .execute(
                    "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, ?2, ?3, 'message.part.updated.1', ?4)",
                    rusqlite::params![
                        format!("evt_{aggregate}_{seq}"),
                        aggregate,
                        seq,
                        serde_json::to_string(&json!({
                            "sessionID": aggregate,
                            "part": {
                                "id": format!("part_{aggregate}_{seq}"),
                                "messageID": format!("msg_{aggregate}"),
                                "sessionID": aggregate,
                                "type": "text",
                                "text": format!("{aggregate} body {seq}"),
                                "time": {"start": base + seq}
                            },
                            "time": base + seq
                        }))
                        .unwrap(),
                    ],
                )
                .expect("insert part event");
        }
        connection
            .execute(
                "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?1, 2, NULL)",
                rusqlite::params![aggregate],
            )
            .expect("insert sequence");
    }
    drop(connection);

    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());
    let mut config = moraine_config::AppConfig::default();
    config.ingest.sqlite.fast_path_max_payload_rows = 1;

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    {
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("cold poll persists");
        assert!(
            checkpoint.cursor_json.contains("pending_coverage"),
            "the resume marker must be durable"
        );
    }

    // No touches: convergence must come from the marker alone.
    let mut polls = 1;
    loop {
        drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        polls += 1;
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("checkpoint");
        let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor parses");
        if cursor.get("pending_coverage").is_none() {
            assert_eq!(
                cursor
                    .pointer("/aggregate_sequences/ses_a")
                    .and_then(Value::as_i64),
                Some(2),
                "every ses_a event covered"
            );
            assert_eq!(
                cursor
                    .pointer("/aggregate_sequences/ses_b")
                    .and_then(Value::as_i64),
                Some(2),
                "every ses_b event covered"
            );
            break;
        }
        assert!(
            polls <= 12,
            "a 1-row budget against 6 events must converge; still pending \
             after {polls} polls"
        );
    }

    // Quiesce: coverage complete, stat unchanged — the next poll must not scan.
    let rows_before = poll_payload_rows(&metrics);
    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
    assert_eq!(
        poll_payload_rows(&metrics),
        rows_before,
        "a covered, unchanged store must short-circuit"
    );

    cleanup(&path);
}

/// The OpenCode twin of `a_replacement_replay_reads_past_the_fast_path_budget`
/// (§7.1 D5): a replacement replay ignores the fast-path budget, because its
/// finalize publishes the generation whole and a degraded replay would
/// publish a hole through #602.
///
/// Fails for: passing the fast-path budget on OpenCode replays.
#[tokio::test(flavor = "multi_thread")]
async fn an_opencode_replacement_replay_reads_past_the_fast_path_budget() {
    let path = unique_opencode_db_path("d5-replay-unbudgeted");
    let db = seed_opencode_db(&path);
    let full_seq: i64 = db
        .query_row(
            "SELECT seq FROM event_sequence WHERE aggregate_id = 'ses_demo'",
            [],
            |row| row.get(0),
        )
        .expect("read full sequence");
    drop(db);
    let work = opencode_sqlite_work(&path);
    let cp_key = checkpoint_key(&work.source_name, &work.path);
    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
    let poll_state = VolatilePollMap::new();
    let metrics = Arc::new(Metrics::default());
    let mut config = moraine_config::AppConfig::default();
    config.ingest.sqlite.fast_path_max_payload_rows = 1;

    drive_opencode_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;

    // A changed exclusion set starts a replacement replay under the same
    // tight budget; the replay must ignore it.
    let mut replaying = config.clone();
    replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
    let rows_before = poll_payload_rows(&metrics);
    drive_opencode_poll_with_metrics(&replaying, &work, &checkpoints, &poll_state, &metrics).await;
    assert!(
        poll_payload_rows(&metrics) - rows_before > 1,
        "the replay must read past the 1-row budget"
    );
    let map = checkpoints.read().await;
    let checkpoint = map.get(&cp_key).expect("finalized replay checkpoint");
    assert_eq!(checkpoint.status, "active");
    let cursor: Value = serde_json::from_str(&checkpoint.cursor_json).expect("cursor parses");
    assert_eq!(
        cursor
            .pointer("/aggregate_sequences/ses_demo")
            .and_then(Value::as_i64),
        Some(full_seq),
        "the replay must cover the whole aggregate, budget notwithstanding"
    );
    assert!(
        cursor.get("pending_coverage").is_none(),
        "a finalized replay owes nothing"
    );

    cleanup(&path);
}

/// §3.1 Change 4, the runaway side: contexts past either ceiling evict whole
/// aggregates — session context and that aggregate's message contexts
/// together, in the documented ascending-`aggregate_id` order — instead of
/// failing anything. This pins the eviction *algorithm* at the method; the
/// scan-path wiring (that a scan actually calls it before persisting) is
/// `an_opencode_scan_evicts_contexts_over_the_ceiling_before_persisting`.
///
/// Fails for: an `evict_contexts_to_fit` that returns without evicting, one
/// that evicts message contexts but strands the session context (or vice
/// versa), or a descending order (the highest aggregate evicts first).
#[test]
fn an_opencode_context_over_its_ceiling_evicts_whole_aggregates_instead_of_failing() {
    let mut state = OpenCodeState::fresh();
    for aggregate in ["agg_a", "agg_b", "agg_c"] {
        state.session_contexts.insert(
            aggregate.to_string(),
            OpenCodeSessionContext {
                directory: format!("/work/{aggregate}"),
                model: None,
            },
        );
        let mut messages = BTreeMap::new();
        for idx in 0..4 {
            messages.insert(
                format!("msg_{aggregate}_{idx}"),
                OpenCodeMessageContext {
                    role: "assistant".to_string(),
                    ..OpenCodeMessageContext::default()
                },
            );
        }
        state
            .message_contexts
            .insert(aggregate.to_string(), messages);
    }
    // 15 entries (3 sessions + 12 messages); a 6-entry ceiling forces two
    // whole aggregates out.
    let evicted = state.evict_contexts_to_fit(6, usize::MAX);
    assert_eq!(
        evicted, 10,
        "two whole aggregates — 2 sessions + 8 messages"
    );
    assert!(
        !state.session_contexts.contains_key("agg_a")
            && !state.message_contexts.contains_key("agg_a"),
        "eviction is whole-aggregate and ascending: agg_a goes first"
    );
    assert!(
        !state.session_contexts.contains_key("agg_b")
            && !state.message_contexts.contains_key("agg_b"),
        "agg_b goes second"
    );
    assert!(
        state.session_contexts.contains_key("agg_c")
            && state.message_contexts.contains_key("agg_c"),
        "the highest aggregate survives whole"
    );
}

/// §3.1 Change 4, the starvation side: contexts exactly at their ceiling
/// evict nothing — the `<=` boundary — so the ceiling cannot chew through
/// state it was meant to protect.
///
/// Fails for: `<` at either fit check.
#[test]
fn an_opencode_context_at_its_ceiling_evicts_nothing() {
    let mut state = OpenCodeState::fresh();
    state.session_contexts.insert(
        "agg_a".to_string(),
        OpenCodeSessionContext {
            directory: "/work/agg_a".to_string(),
            model: None,
        },
    );
    let mut messages = BTreeMap::new();
    messages.insert("msg_a_0".to_string(), OpenCodeMessageContext::default());
    state.message_contexts.insert("agg_a".to_string(), messages);
    let bytes = serde_json::to_string(&(&state.session_contexts, &state.message_contexts))
        .expect("serialize contexts")
        .len();

    let before = state.clone();
    let evicted = state.evict_contexts_to_fit(2, bytes);
    assert_eq!(evicted, 0, "a payload exactly at its ceiling is kept");
    assert_eq!(state, before, "and nothing was disturbed");
}

/// §2.3's un-processable single row, OpenCode arm (the rewrite of the old
/// `TooLarge` row latch): one event larger than `SCAN_PAGE_MAX_BYTES` gets
/// one error row, is skipped, and the watermark advances past it — so the
/// report is one-shot and the aggregate keeps flowing.
///
/// Fails for: restoring the scan-failing `TooLarge` arm, a skip that stalls
/// the watermark (the report repeats forever), or losing the neighbors.
#[test]
fn an_oversized_opencode_event_is_skipped_and_advanced_past() {
    let path = unique_opencode_db_path("oversized-event");
    let connection = create_opencode_db(&path);
    let oversized = "x".repeat(SCAN_PAGE_MAX_BYTES + 1);
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_before', 'ses_big', 0, 'session.created.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_big",
                "info": {
                    "id": "ses_big",
                    "directory": "/work/big",
                    "title": "session before the oversized row",
                    "time": {"created": 1780000100000_i64, "updated": 1780000100000_i64}
                }
            }))
            .unwrap()],
        )
        .expect("insert leading event");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_oversized', 'ses_big', 1, 'message.part.updated.1', ?1)",
            rusqlite::params![format!("{{\"blob\":\"{oversized}\"}}")],
        )
        .expect("insert oversized event");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_after', 'ses_big', 2, 'message.part.updated.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_big",
                "part": {
                    "id": "part_after",
                    "messageID": "msg_big",
                    "sessionID": "ses_big",
                    "type": "text",
                    "text": "the neighbor after the oversized row",
                    "time": {"start": 1780000100002_i64}
                },
                "time": 1780000100002_i64
            }))
            .unwrap()],
        )
        .expect("insert trailing event");
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES ('ses_big', 2, NULL)",
            [],
        )
        .expect("insert sequence");
    drop(connection);

    // Unbudgeted on purpose: the oversized row's bytes are honestly charged,
    // so the default byte budget would bind right after it and defer the
    // neighbor to the next poll — correct, but the budget tests' subject.
    // This test isolates the §2.3 row-skip semantics.
    let mut ledger = ScanLedger::default();
    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &ScanBudget::unbounded(),
        &mut ledger,
    );
    let OpenCodeScanOutcome::Scanned {
        records,
        new_state,
        row_errors,
        ..
    } = outcome
    else {
        panic!("an oversized event degrades per §2.3; it must not fail the scan");
    };
    assert_eq!(row_errors.len(), 1, "one error row for the skipped event");
    assert_eq!(
        row_errors[0].error_kind,
        crate::sqlite_poll::ERROR_KIND_ROW_TOO_LARGE
    );
    assert_eq!(
        new_state.aggregate_sequences.get("ses_big").copied(),
        Some(2),
        "the watermark advances past the skipped event to the last observed row"
    );
    let serialized = serde_json::to_string(&records.iter().map(|r| &r.record).collect::<Vec<_>>())
        .expect("serialize records");
    assert!(
        serialized.contains("the neighbor after the oversized row"),
        "the events around the skipped row still flow"
    );

    // One-shot: the watermark is past the oversized row, so the warm scan
    // neither re-reads nor re-reports it.
    let mut warm_ledger = ScanLedger::default();
    let warm = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &new_state,
        &default_opencode_budget(),
        &mut warm_ledger,
    );
    let OpenCodeScanOutcome::Scanned {
        row_errors: warm_errors,
        ..
    } = warm
    else {
        panic!("warm scan should succeed");
    };
    assert!(warm_errors.is_empty(), "the report must be one-shot");
    assert_eq!(
        warm_ledger.payload_rows, 0,
        "nothing is re-read behind the advanced watermark"
    );

    cleanup(&path);
}

/// The **byte** axis of the OpenCode work budget, at both of its call sites
/// (WI-05's `a_nac_byte_budget_binds_at_both_call_sites` precedent): the
/// rows-axis budget tests cannot see a call site that stops feeding the byte
/// argument — `is_exhausted_by(ledger.payload_rows, 0)` at either site leaves
/// them green — so each site gets its own divergent fixture: few large events
/// under a byte-bounded, row-unbounded budget.
///
/// Scenario one pins the **between-aggregate** check: two cold aggregates of
/// one event each. The second aggregate must never be materialized —
/// `payload_rows` stays 1, because a site fed zero bytes falls through to the
/// in-page check, which charges the second aggregate's first row before it
/// can bind. Scenario two pins the **in-page** check: one aggregate of two
/// events — the second event is charged one row and no bytes (the
/// honest-ledger rule) and stays unread behind the committed watermark.
///
/// MUTATION (executed 2026-07-31): pass `0` for the byte argument at the
/// between-aggregate call site — fails (scenario one charges 2 payload
/// rows). Same at the in-page call site — fails (scenario one passes whole,
/// then scenario two's watermark reaches seq 1). Each RED was confirmed in
/// a filtered run, failing at its own scenario's named assertion — which is
/// what makes the sites separately pinned.
#[test]
fn an_opencode_byte_budget_binds_at_both_call_sites() {
    let byte_budget = ScanBudget {
        max_payload_rows: u64::MAX,
        max_payload_bytes: 1,
    };

    // Scenario one: the between-aggregate site.
    let path = unique_opencode_db_path("byte-budget-between-aggregates");
    let connection = create_opencode_db(&path);
    for aggregate in ["agg_one", "agg_two"] {
        connection
            .execute(
                "INSERT INTO event (id, aggregate_id, seq, type, data) \
                 VALUES (?1, ?2, 0, 'session.created.1', ?3)",
                rusqlite::params![
                    format!("evt_{aggregate}"),
                    aggregate,
                    serde_json::to_string(&json!({
                        "sessionID": aggregate,
                        "info": {
                            "id": aggregate,
                            "directory": format!("/work/{aggregate}"),
                            "title": format!("byte budget {aggregate}"),
                            "time": {"created": 1780000100000_i64, "updated": 1780000100000_i64}
                        }
                    }))
                    .unwrap(),
                ],
            )
            .expect("insert session event");
        connection
            .execute(
                "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?1, 0, NULL)",
                rusqlite::params![aggregate],
            )
            .expect("insert sequence");
    }
    drop(connection);

    let mut ledger = ScanLedger::default();
    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &byte_budget,
        &mut ledger,
    );
    let OpenCodeScanOutcome::Scanned {
        records, new_state, ..
    } = outcome
    else {
        panic!("a byte-bound scan degrades; it must not fail");
    };
    assert_eq!(
        ledger.payload_rows, 1,
        "the byte budget must bind at the between-aggregate site: the second \
         aggregate is never materialized (a site fed 0 bytes falls through \
         to the in-page check, which charges the second aggregate's first row)"
    );
    let serialized = serde_json::to_string(&records.iter().map(|r| &r.record).collect::<Vec<_>>())
        .expect("serialize records");
    assert!(serialized.contains("byte budget agg_one"));
    assert!(
        !serialized.contains("byte budget agg_two"),
        "the byte-skipped aggregate must not emit"
    );
    assert!(ledger.coverage_degraded);
    assert!(
        new_state.pending_coverage,
        "the byte-skipped aggregate is a durable coverage debt"
    );
    assert!(
        !new_state.aggregate_sequences.contains_key("agg_two"),
        "a skipped never-read aggregate stays absent so a later poll re-detects it"
    );
    cleanup(&path);

    // Scenario two: the in-page site.
    let path = unique_opencode_db_path("byte-budget-in-page");
    let connection = create_opencode_db(&path);
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_paged_session', 'ses_paged', 0, 'session.created.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_paged",
                "info": {
                    "id": "ses_paged",
                    "directory": "/work/paged",
                    "title": "the first event pays the byte budget",
                    "time": {"created": 1780000100000_i64, "updated": 1780000100000_i64}
                }
            }))
            .unwrap()],
        )
        .expect("insert first event");
    connection
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) \
             VALUES ('evt_paged_part', 'ses_paged', 1, 'message.part.updated.1', ?1)",
            rusqlite::params![serde_json::to_string(&json!({
                "sessionID": "ses_paged",
                "part": {
                    "id": "part_paged",
                    "messageID": "msg_paged",
                    "sessionID": "ses_paged",
                    "type": "text",
                    "text": "the second event stays unread",
                    "time": {"start": 1780000100001_i64}
                },
                "time": 1780000100001_i64
            }))
            .unwrap()],
        )
        .expect("insert second event");
    connection
        .execute(
            "INSERT INTO event_sequence (aggregate_id, seq, owner_id) \
             VALUES ('ses_paged', 1, NULL)",
            [],
        )
        .expect("insert sequence");
    let first_event_bytes: i64 = connection
        .query_row(
            "SELECT length(CAST(id AS BLOB)) + length(CAST(aggregate_id AS BLOB)) \
             + length(CAST(type AS BLOB)) + length(CAST(data AS BLOB)) \
             FROM event WHERE id = 'evt_paged_session'",
            [],
            |row| row.get(0),
        )
        .expect("measure first event");
    drop(connection);

    let mut ledger = ScanLedger::default();
    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &OpenCodeState::fresh(),
        &byte_budget,
        &mut ledger,
    );
    let OpenCodeScanOutcome::Scanned {
        records, new_state, ..
    } = outcome
    else {
        panic!("a byte-bound scan degrades; it must not fail");
    };
    assert_eq!(
        new_state.aggregate_sequences.get("ses_paged").copied(),
        Some(0),
        "the byte budget must bind at the in-page site: the watermark stops \
         at the last processed event"
    );
    assert_eq!(
        ledger.payload_rows, 2,
        "the bound row was materialized before the budget could be consulted: \
         one row on the rows axis"
    );
    assert_eq!(
        ledger.payload_bytes, first_event_bytes as u64,
        "and no bytes — the bound row's columns are never taken"
    );
    let serialized = serde_json::to_string(&records.iter().map(|r| &r.record).collect::<Vec<_>>())
        .expect("serialize records");
    assert!(
        !serialized.contains("the second event stays unread"),
        "the event behind the byte bound must not emit"
    );
    assert!(
        new_state.pending_coverage,
        "the unread tail is a durable coverage debt"
    );
    cleanup(&path);
}

/// §3.1 Change 4's **scan-path wiring**. The two method-level ceiling tests
/// above pin the eviction *algorithm* (wholeness, order, boundary), not the
/// production call inside `scan_opencode_rows` — delete that call and both
/// stay green while nothing bounds the persisted contexts hashed into the
/// #602 transition digest. This pin goes through `scan_opencode_database`
/// with the production constants: a prior cursor carrying a context payload
/// past `MAX_OPENCODE_CONTEXT_BYTES` must come out of the scan evicted and
/// under the ceiling.
///
/// MUTATION (executed 2026-07-31): replace the `evict_contexts_to_fit` call
/// in `scan_opencode_rows` with `let evicted = 0u64;` — this test fails
/// (nothing evicts; the persisted payload stays over its ceiling) while both
/// method-level ceiling tests stay green, which is the round-2 finding this
/// pin closes.
#[test]
fn an_opencode_scan_evicts_contexts_over_the_ceiling_before_persisting() {
    let path = unique_opencode_db_path("ceiling-scan-path");
    let connection = create_opencode_db(&path);
    for aggregate in ["agg_bloat", "ses_kept"] {
        connection
            .execute(
                "INSERT INTO event (id, aggregate_id, seq, type, data) \
                 VALUES (?1, ?2, 0, 'session.created.1', ?3)",
                rusqlite::params![
                    format!("evt_{aggregate}"),
                    aggregate,
                    serde_json::to_string(&json!({
                        "sessionID": aggregate,
                        "info": {
                            "id": aggregate,
                            "directory": format!("/work/{aggregate}"),
                            "title": format!("ceiling {aggregate}"),
                            "time": {"created": 1780000100000_i64, "updated": 1780000100000_i64}
                        }
                    }))
                    .unwrap(),
                ],
            )
            .expect("insert session event");
        connection
            .execute(
                "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?1, 0, NULL)",
                rusqlite::params![aggregate],
            )
            .expect("insert sequence");
    }
    drop(connection);

    // A covered cursor (both watermarks current, so the scan reads nothing)
    // whose carried context payload exceeds the production byte ceiling —
    // only the scan's own eviction call can bring it back under.
    let mut prior = OpenCodeState::fresh();
    prior.event_scan_complete = true;
    for aggregate in ["agg_bloat", "ses_kept"] {
        prior.aggregate_sequences.insert(aggregate.to_string(), 0);
    }
    prior.session_contexts.insert(
        "agg_bloat".to_string(),
        OpenCodeSessionContext {
            directory: "x".repeat(MAX_OPENCODE_CONTEXT_BYTES + 1024),
            model: None,
        },
    );
    prior.session_contexts.insert(
        "ses_kept".to_string(),
        OpenCodeSessionContext {
            directory: "/work/ses_kept".to_string(),
            model: None,
        },
    );

    let mut ledger = ScanLedger::default();
    let outcome = scan_opencode_database(
        path.to_str().expect("utf-8 path"),
        &prior,
        &default_opencode_budget(),
        &mut ledger,
    );
    let OpenCodeScanOutcome::Scanned { new_state, .. } = outcome else {
        panic!("a ceiling crossing evicts per §2.3; it must not fail the scan");
    };
    assert_eq!(
        ledger.evicted_entries, 1,
        "the scan itself must evict the over-ceiling context before persisting"
    );
    assert!(
        ledger.coverage_degraded,
        "eviction reports itself as degraded coverage"
    );
    assert!(
        !new_state.session_contexts.contains_key("agg_bloat"),
        "whole-aggregate, ascending: the bloated aggregate goes"
    );
    assert!(
        new_state.session_contexts.contains_key("ses_kept"),
        "the aggregate above it survives whole"
    );
    let persisted =
        serde_json::to_string(&(&new_state.session_contexts, &new_state.message_contexts))
            .expect("serialize contexts")
            .len();
    assert!(
        persisted <= MAX_OPENCODE_CONTEXT_BYTES,
        "the persisted context payload must fit the production ceiling; got {persisted} bytes"
    );
    assert!(
        !new_state.pending_coverage,
        "context eviction is not coverage debt — events stay covered and \
         enrichment rebuilds on demand"
    );

    cleanup(&path);
}
