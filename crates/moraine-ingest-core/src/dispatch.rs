use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, NormalizedRecord, RowBatch};
use crate::normalize::{normalize_record, normalize_record_with_ts_hint};
use crate::sources::shared::{format_record_ts, parse_record_ts};
use crate::{DispatchState, Metrics, SinkMessage, WorkItem};
use anyhow::{Context, Result};
use moraine_config::{
    is_workflow_journal_path, map_tracked_path, AppConfig, SOURCE_FORMAT_CURSOR_SQLITE,
    SOURCE_FORMAT_OPENCODE_SQLITE, SOURCE_FORMAT_SESSION_JSON,
};
use serde_json::{json, Value};
use std::collections::HashMap;
#[cfg(not(unix))]
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, OwnedSemaphorePermit, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

#[cfg(not(unix))]
use same_file::Handle;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(not(unix))]
use std::time::UNIX_EPOCH;

/// Session-json sources read whole-file snapshots that are atomically replaced
/// every save, so the inode churns. We pin a stable synthetic identity here so
/// the checkpoint key and `event_uid` derivation stay stable across saves.
const SESSION_JSON_INODE: u64 = 0;
const SESSION_JSON_GENERATION: u32 = 1;
const CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT: usize = 10 * 1024 * 1024;
/// Keep per-line JSONL rows below ClickHouse's hard JSONEachRow object limit
/// after Moraine wraps the source record into raw/event rows. The default
/// ingest batch byte budget is 8 MiB; capping source lines there leaves room
/// for the row envelope and escaped `raw_json` string.
const DEFAULT_JSONL_SOURCE_LINE_BYTE_LIMIT: usize = 8 * 1024 * 1024;
const ERROR_KIND_SOURCE_LINE_TOO_LARGE: &str = "jsonl_source_line_too_large";
const ERROR_KIND_NORMALIZED_ROW_TOO_LARGE: &str = "jsonl_normalized_row_too_large";

/// A work item is processable only when its path is already the canonical
/// tracked path for its format (sidecar paths are canonicalized at the
/// watcher; anything else here is a stray event for an untracked file).
fn work_path_is_canonical(work: &WorkItem) -> bool {
    map_tracked_path(&work.format, &work.path).as_deref() == Some(work.path.as_str())
}

/// The single gate before a path becomes ingest work: every entry point
/// (backfill, reconcile, and the live watcher via the debounce task) funnels
/// through `enqueue_work`, which calls this. A path is ingestable only when it
/// is the canonical tracked path for its format AND it is not an
/// orchestration-internal trace that merely shares a session source's
/// glob/extension.
///
/// The only excluded class today is Claude Code `Workflow` journals (issue
/// #386): the recursive `~/.claude/projects/**/*.jsonl` glob (and the
/// recursive watcher) pick them up, but they carry no `sessionId` and would
/// normalize to empty-`session_id` junk that breaks `list_sessions`. Filtering
/// here — rather than tightening the glob — also catches live watcher writes,
/// which never consult the glob. The exclusion is scoped to the `claude-code`
/// harness so a same-named file under any other configured source is never
/// silently dropped.
fn work_item_is_ingestable(work: &WorkItem) -> bool {
    if !work_path_is_canonical(work) {
        debug!(
            "dropping non-canonical work item {} (format {})",
            work.path, work.format
        );
        return false;
    }
    if work.harness == "claude-code" && is_workflow_journal_path(&work.path) {
        debug!(
            "skipping workflow orchestration journal {} (no sessionId; issue #386)",
            work.path
        );
        return false;
    }
    true
}

/// Best-effort mapping from a Hermes session `base_url` to an inference
/// provider vendor. The session file only carries the bare model name (e.g.
/// `claude-opus-4-6`), so we pre-prepend the vendor here to match the
/// `vendor/model` convention the Hermes normalizer expects and downstream
/// `inference_provider` queries need.
fn infer_vendor_from_base_url(base_url: &str) -> &'static str {
    let lower = base_url.to_ascii_lowercase();
    if lower.contains("anthropic.com") {
        "anthropic"
    } else if lower.contains("openai.com") || lower.contains("openai.azure.com") {
        "openai"
    } else if lower.contains("openrouter") {
        "openrouter"
    } else if lower.contains("bedrock") {
        "bedrock"
    } else if lower.contains("googleapis") || lower.contains("google.com") {
        "google"
    } else {
        ""
    }
}

fn compose_hermes_model(model: &str, base_url: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.contains('/') {
        return trimmed.to_string();
    }
    let vendor = infer_vendor_from_base_url(base_url);
    if vendor.is_empty() {
        trimmed.to_string()
    } else {
        format!("{}/{}", vendor, trimmed)
    }
}

fn jsonl_source_line_byte_limit(config: &AppConfig) -> usize {
    config
        .ingest
        .max_batch_bytes
        .clamp(1, DEFAULT_JSONL_SOURCE_LINE_BYTE_LIMIT)
}

fn oversized_source_line_error_row(
    work: &WorkItem,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    line_bytes: usize,
    limit_bytes: usize,
) -> Value {
    json!({
        "source_name": work.source_name,
        "harness": work.harness,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "error_kind": ERROR_KIND_SOURCE_LINE_TOO_LARGE,
        "error_text": format!(
            "source line is {line_bytes} bytes, exceeding the {limit_bytes} byte JSONL ingest limit; skipped before normalization"
        ),
        "raw_fragment": json!({
            "action": "skipped",
            "line_bytes": line_bytes,
            "limit_bytes": limit_bytes,
        }).to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SerializedRowSize {
    table: &'static str,
    bytes: usize,
}

fn oversized_normalized_row_error_row(
    work: &WorkItem,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    line_bytes: usize,
    row_size: &SerializedRowSize,
) -> Value {
    json!({
        "source_name": work.source_name,
        "harness": work.harness,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "error_kind": ERROR_KIND_NORMALIZED_ROW_TOO_LARGE,
        "error_text": format!(
            "{} row serializes to {} bytes, exceeding the {} byte ClickHouse JSON object limit; skipped before insert",
            row_size.table,
            row_size.bytes,
            CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT
        ),
        "raw_fragment": json!({
            "action": "skipped",
            "line_bytes": line_bytes,
            "serialized_row_table": row_size.table,
            "serialized_row_bytes": row_size.bytes,
            "limit_bytes": CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT,
        }).to_string(),
    })
}

fn serialized_json_object_bytes(row: &Value) -> usize {
    serde_json::to_vec(row)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

fn largest_serialized_normalized_row(normalized: &NormalizedRecord) -> Option<SerializedRowSize> {
    let mut largest: Option<SerializedRowSize> = None;

    let mut observe = |table: &'static str, row: &Value| {
        if row.is_null() {
            return;
        }
        let bytes = serialized_json_object_bytes(row);
        if largest.as_ref().is_none_or(|current| bytes > current.bytes) {
            largest = Some(SerializedRowSize { table, bytes });
        }
    };

    observe("raw_events", &normalized.raw_row);
    for row in &normalized.event_rows {
        observe("events", row);
    }
    for row in &normalized.link_rows {
        observe("event_links", row);
    }
    for row in &normalized.tool_rows {
        observe("tool_io", row);
    }
    for row in &normalized.error_rows {
        observe("ingest_errors", row);
    }

    largest
}

enum JsonlLineRead {
    Eof,
    Normal { buf: Vec<u8>, bytes_read: usize },
    Oversized { bytes_read: usize },
}

fn read_bounded_jsonl_line<R: BufRead>(
    reader: &mut R,
    max_bytes: usize,
) -> std::io::Result<JsonlLineRead> {
    let mut buf = Vec::<u8>::new();
    let mut bytes_read = 0usize;
    let mut oversized = false;

    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if bytes_read == 0 {
                return Ok(JsonlLineRead::Eof);
            }
            return if oversized {
                Ok(JsonlLineRead::Oversized { bytes_read })
            } else {
                Ok(JsonlLineRead::Normal { buf, bytes_read })
            };
        }

        let newline_pos = available.iter().position(|byte| *byte == b'\n');
        let take = newline_pos.map_or(available.len(), |pos| pos + 1);
        let crosses_limit = !oversized && bytes_read.saturating_add(take) > max_bytes;

        if crosses_limit {
            oversized = true;
            buf.clear();
        } else if !oversized {
            buf.extend_from_slice(&available[..take]);
        }

        reader.consume(take);
        bytes_read = bytes_read.saturating_add(take);

        if newline_pos.is_some() {
            return if oversized {
                Ok(JsonlLineRead::Oversized { bytes_read })
            } else {
                Ok(JsonlLineRead::Normal { buf, bytes_read })
            };
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_chunk_if_batch_exceeds_limits(
    batch: &mut RowBatch,
    config: &AppConfig,
    sink_tx: &mpsc::Sender<SinkMessage>,
    work: &WorkItem,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    offset: u64,
    line_no: u64,
    context: &'static str,
) -> Result<()> {
    if !batch.exceeds_limits(config.ingest.batch_size, config.ingest.max_batch_bytes) {
        return Ok(());
    }

    let mut chunk = batch.drain_to_chunk();
    chunk.checkpoint = Some(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode,
        source_generation,
        last_offset: offset,
        last_line_no: line_no,
        status: "active".to_string(),
        ..Default::default()
    });

    sink_tx
        .send(SinkMessage::Batch(chunk))
        .await
        .with_context(|| format!("sink channel closed while sending {context}"))
}

/// Per-session cursor used to derive model-side latency for Claude Code
/// assistant turns. We stamp `latency_ms` on the first assistant event of a
/// record when the immediately preceding event in the same session was a
/// tool_result — that interval is bounded on both ends by machine events
/// (tool harness → model provider → next assistant block), so it cleanly
/// represents server-side processing with no human-in-the-loop noise.
#[derive(Clone, Copy)]
struct SessionCursor {
    prev_event_ts_ms: i64,
    prev_was_tool_result: bool,
}

fn parse_event_ts_ms(event_ts: &str) -> Option<i64> {
    chrono::NaiveDateTime::parse_from_str(event_ts, "%Y-%m-%d %H:%M:%S%.3f")
        .ok()
        .map(|dt| dt.and_utc().timestamp_millis())
}

fn infer_initial_record_ts_hint(source_file: &str, offset: u64) -> Option<String> {
    let mut file = std::fs::File::open(source_file).ok()?;
    file.seek(SeekFrom::Start(offset)).ok()?;

    let mut reader = BufReader::new(file);
    loop {
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader.read_until(b'\n', &mut buf).ok()?;
        if bytes_read == 0 {
            break;
        }

        let text = String::from_utf8_lossy(&buf);
        if text.trim().is_empty() {
            continue;
        }

        let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let Some(record_ts) = parsed.get("timestamp").and_then(Value::as_str) else {
            continue;
        };
        let record_ts = record_ts.trim();
        if parse_record_ts(record_ts).is_some() {
            return Some(record_ts.to_string());
        }
    }

    std::fs::metadata(source_file)
        .ok()
        .and_then(|meta| meta.modified().ok())
        .map(|modified| {
            let dt: chrono::DateTime<chrono::Utc> = modified.into();
            format_record_ts(&dt)
        })
}

/// Best-effort session-level cwd recovered from the head of a file that is
/// being resumed mid-stream. Harnesses like codex and pi only carry the
/// working directory on the session header (line 1); after a restart the
/// resumed tail would otherwise stamp empty `cwd` values and leave backend
/// route resolution blind to the session's directory. Bounded so the peek
/// stays cheap on every resumed-file processing pass.
fn infer_initial_cwd_hint(source_file: &str, harness: &str) -> Option<String> {
    const MAX_HEAD_LINES: usize = 25;
    const MAX_HEAD_BYTES: u64 = 512 * 1024;

    let source = crate::sources::registry().get(harness)?;
    let file = std::fs::File::open(source_file).ok()?;
    let mut reader = BufReader::new(file.take(MAX_HEAD_BYTES));

    for _ in 0..MAX_HEAD_LINES {
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader.read_until(b'\n', &mut buf).ok()?;
        if bytes_read == 0 {
            break;
        }

        let text = String::from_utf8_lossy(&buf);
        let Ok(record) = serde_json::from_str::<Value>(text.trim()) else {
            continue;
        };
        let cwd = source.cwd(&record);
        let trimmed = cwd.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    None
}

/// Post-process event rows from a single Claude Code record:
///   * if the session's prior event was a `tool_result`, stamp `latency_ms`
///     on the first assistant-actor event in this record (= wall-clock time
///     the model provider spent between `tool_result received` and
///     `first block of assistant response produced`);
///   * advance the per-session cursor for the next record.
///
/// No-op for non-claude harnesses or empty row sets. The stamped value is
/// clamped to u32 (>49 days saturates).
fn enrich_claude_model_latency(
    harness: &str,
    event_rows: &mut [Value],
    cursors: &mut HashMap<String, SessionCursor>,
) {
    if harness != "claude-code" || event_rows.is_empty() {
        return;
    }

    let session_id = event_rows[0]
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if session_id.is_empty() {
        return;
    }

    let ts_ms = match event_rows[0]
        .get("event_ts")
        .and_then(|v| v.as_str())
        .and_then(parse_event_ts_ms)
    {
        Some(ms) => ms,
        None => return,
    };

    let any_tool_result = event_rows
        .iter()
        .any(|r| r.get("event_kind").and_then(|v| v.as_str()) == Some("tool_result"));

    if let Some(cursor) = cursors.get(&session_id) {
        if cursor.prev_was_tool_result && ts_ms > cursor.prev_event_ts_ms {
            if let Some(idx) = event_rows
                .iter()
                .position(|r| r.get("actor_kind").and_then(|v| v.as_str()) == Some("assistant"))
            {
                let delta = (ts_ms - cursor.prev_event_ts_ms).max(0) as u64;
                let capped = delta.min(u32::MAX as u64) as u32;
                if let Some(obj) = event_rows[idx].as_object_mut() {
                    obj.insert("latency_ms".to_string(), json!(capped));
                }
            }
        }
    }

    // Only advance the cursor from events that participate in the turn
    // sequence (user/assistant/tool). System/progress rows are out-of-band
    // and must not reset the tool_result → assistant chain.
    let touches_turn = event_rows.iter().any(|r| {
        matches!(
            r.get("actor_kind").and_then(|v| v.as_str()),
            Some("user") | Some("assistant") | Some("tool")
        )
    });
    if touches_turn {
        cursors.insert(
            session_id,
            SessionCursor {
                prev_event_ts_ms: ts_ms,
                prev_was_tool_result: any_tool_result,
            },
        );
    }
}

pub(crate) fn spawn_debounce_task(
    config: AppConfig,
    mut rx: mpsc::UnboundedReceiver<WorkItem>,
    process_tx: mpsc::Sender<WorkItem>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let debounce = Duration::from_millis(config.ingest.debounce_ms.max(5));
        let mut pending = HashMap::<String, (WorkItem, Instant)>::new();
        let mut tick = tokio::time::interval(Duration::from_millis(
            (config.ingest.debounce_ms / 2).max(10),
        ));

        loop {
            tokio::select! {
                maybe_work = rx.recv() => {
                    match maybe_work {
                        Some(work) => {
                            pending.insert(work.key(), (work, Instant::now()));
                        }
                        None => break,
                    }
                }
                _ = tick.tick() => {
                    if pending.is_empty() {
                        continue;
                    }

                    let now = Instant::now();
                    let ready: Vec<String> = pending
                        .iter()
                        .filter_map(|(key, (_, seen_at))| {
                            if now.duration_since(*seen_at) >= debounce {
                                Some(key.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    for key in ready {
                        if let Some((work, _)) = pending.remove(&key) {
                            // `enqueue_work` is the single ingestability gate;
                            // it early-returns on non-ingestable items, so no
                            // pre-check is needed here.
                            enqueue_work(work, &process_tx, &dispatch, &metrics).await;
                        }
                    }
                }
            }
        }
    })
}

pub(crate) async fn enqueue_work(
    work: WorkItem,
    process_tx: &mpsc::Sender<WorkItem>,
    dispatch: &Arc<Mutex<DispatchState>>,
    metrics: &Arc<Metrics>,
) {
    if !work_item_is_ingestable(&work) {
        return;
    }

    let key = work.key();
    let mut should_send = false;
    {
        let mut state = dispatch.lock().expect("dispatch mutex poisoned");
        state.item_by_key.insert(key.clone(), work.clone());
        if state.inflight.contains(&key) {
            state.dirty.insert(key.clone());
        } else if state.pending.insert(key.clone()) {
            should_send = true;
        }
    }

    if should_send && process_tx.send(work).await.is_ok() {
        metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn complete_work(key: &str, dispatch: &Arc<Mutex<DispatchState>>) -> Option<WorkItem> {
    let mut state = dispatch.lock().expect("dispatch mutex poisoned");
    state.inflight.remove(key);

    if state.dirty.remove(key) {
        if state.pending.insert(key.to_string()) {
            return state.item_by_key.get(key).cloned();
        }
        return None;
    }

    if !state.pending.contains(key) && !state.inflight.contains(key) && !state.dirty.contains(key) {
        state.item_by_key.remove(key);
    }

    None
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_work_item(
    config: AppConfig,
    work: WorkItem,
    permit: OwnedSemaphorePermit,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<crate::SinkMessage>,
    process_tx: mpsc::Sender<WorkItem>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) {
    let key = work.key();

    if let Err(exc) = process_file(&config, &work, checkpoints, sink_tx, &metrics).await {
        error!(
            "failed processing {}:{}: {exc}",
            work.source_name, work.path
        );
        *metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned") = exc.to_string();
    }

    let reschedule = complete_work(&key, &dispatch);

    // Release before the reschedule `send`; holding it across a full
    // `process_tx` would deadlock the processor loop (issue #215).
    drop(permit);

    if let Some(item) = reschedule {
        if process_tx.send(item).await.is_ok() {
            metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) async fn process_file(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    if work.format == SOURCE_FORMAT_SESSION_JSON {
        return process_session_json_file(config, work, checkpoints, sink_tx, metrics).await;
    }
    if work.format == SOURCE_FORMAT_CURSOR_SQLITE {
        return crate::sqlite_poll::process_cursor_sqlite_db(
            config,
            work,
            checkpoints,
            sink_tx,
            metrics,
        )
        .await;
    }
    if work.format == SOURCE_FORMAT_OPENCODE_SQLITE {
        return crate::sqlite_poll::process_opencode_sqlite_db(
            config,
            work,
            checkpoints,
            sink_tx,
            metrics,
        )
        .await;
    }

    let source_file = &work.path;

    let meta = match std::fs::metadata(source_file) {
        Ok(meta) => meta,
        Err(exc) => {
            debug!("metadata missing for {}: {}", source_file, exc);
            return Ok(());
        }
    };

    let inode = source_inode_for_file(source_file, &meta);

    let file_size = meta.len();
    let cp_key = checkpoint_key(&work.source_name, source_file);
    let committed = { checkpoints.read().await.get(&cp_key).cloned() };

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: 1,
        last_offset: 0,
        last_line_no: 0,
        status: "active".to_string(),
        ..Default::default()
    });

    let mut generation_changed = false;
    if checkpoint.source_inode != inode || file_size < checkpoint.last_offset {
        checkpoint.source_inode = inode;
        checkpoint.source_generation = checkpoint.source_generation.saturating_add(1).max(1);
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
        checkpoint.status = "active".to_string();
        generation_changed = true;
    }

    if file_size == checkpoint.last_offset && !generation_changed {
        return Ok(());
    }

    let mut file = std::fs::File::open(source_file)
        .with_context(|| format!("failed to open {}", source_file))?;
    file.seek(SeekFrom::Start(checkpoint.last_offset))
        .with_context(|| format!("failed to seek {}", source_file))?;

    let mut reader = BufReader::new(file);
    let mut offset = checkpoint.last_offset;
    let mut line_no = checkpoint.last_line_no;
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    // Resuming mid-file restarts the hint chain after the session header,
    // so recover the session-level cwd from the file head when needed.
    let mut cwd_hint = if checkpoint.last_offset > 0 {
        infer_initial_cwd_hint(source_file, &work.harness).unwrap_or_default()
    } else {
        String::new()
    };
    let mut record_ts_hint =
        infer_initial_record_ts_hint(source_file, checkpoint.last_offset).unwrap_or_default();
    let mut session_cursors: HashMap<String, SessionCursor> = HashMap::new();

    let mut batch = RowBatch::default();
    let source_line_byte_limit = jsonl_source_line_byte_limit(config);

    loop {
        let start_offset = offset;
        let read = read_bounded_jsonl_line(&mut reader, source_line_byte_limit)
            .with_context(|| format!("failed reading {}", source_file))?;
        let (buf, bytes_read) = match read {
            JsonlLineRead::Eof => break,
            JsonlLineRead::Normal { buf, bytes_read } => (Some(buf), bytes_read),
            JsonlLineRead::Oversized { bytes_read } => (None, bytes_read),
        };

        offset = offset.saturating_add(bytes_read as u64);
        line_no = line_no.saturating_add(1);

        let Some(buf) = buf else {
            warn!(
                source_file,
                source_line_no = line_no,
                source_offset = start_offset,
                line_bytes = bytes_read,
                limit_bytes = source_line_byte_limit,
                "skipping oversized JSONL source line before normalization"
            );
            batch.push_error_row(oversized_source_line_error_row(
                work,
                source_file,
                inode,
                checkpoint.source_generation,
                line_no,
                start_offset,
                bytes_read,
                source_line_byte_limit,
            ));
            batch.lines_processed = batch.lines_processed.saturating_add(1);

            send_chunk_if_batch_exceeds_limits(
                &mut batch,
                config,
                &sink_tx,
                work,
                source_file,
                inode,
                checkpoint.source_generation,
                offset,
                line_no,
                "oversized-line chunk",
            )
            .await?;

            continue;
        };

        let mut text = String::from_utf8_lossy(&buf).to_string();
        if text.ends_with('\n') {
            text.pop();
        }

        if text.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str::<Value>(&text) {
            Ok(value) if value.is_object() => value,
            Ok(_) => {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "json_parse_error",
                    "error_text": "Expected JSON object",
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
            Err(exc) => {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "json_parse_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
        };

        let mut normalized = match normalize_record_with_ts_hint(
            &parsed,
            &work.source_name,
            &work.harness,
            source_file,
            inode,
            checkpoint.source_generation,
            line_no,
            start_offset,
            &session_hint,
            &model_hint,
            &cwd_hint,
            &record_ts_hint,
        ) {
            Ok(normalized) => normalized,
            Err(exc) => {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "normalize_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
        };

        if let Some(row_size) = largest_serialized_normalized_row(&normalized) {
            if row_size.bytes > CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT {
                warn!(
                    source_file,
                    source_line_no = line_no,
                    source_offset = start_offset,
                    line_bytes = bytes_read,
                    serialized_row_table = row_size.table,
                    serialized_row_bytes = row_size.bytes,
                    limit_bytes = CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT,
                    "skipping JSONL source line whose normalized row is too large for ClickHouse"
                );
                batch.push_error_row(oversized_normalized_row_error_row(
                    work,
                    source_file,
                    inode,
                    checkpoint.source_generation,
                    line_no,
                    start_offset,
                    bytes_read,
                    &row_size,
                ));
                batch.lines_processed = batch.lines_processed.saturating_add(1);

                send_chunk_if_batch_exceeds_limits(
                    &mut batch,
                    config,
                    &sink_tx,
                    work,
                    source_file,
                    inode,
                    checkpoint.source_generation,
                    offset,
                    line_no,
                    "oversized-normalized-row chunk",
                )
                .await?;

                continue;
            }
        }

        if let Some(record_ts) = normalized.raw_row.get("record_ts").and_then(Value::as_str) {
            if parse_record_ts(record_ts).is_some() {
                record_ts_hint = record_ts.to_string();
            }
        }

        enrich_claude_model_latency(
            &work.harness,
            &mut normalized.event_rows,
            &mut session_cursors,
        );

        session_hint = normalized.session_hint.clone();
        model_hint = normalized.model_hint.clone();
        cwd_hint = normalized.cwd_hint.clone();
        // A null `raw_row` means the normalizer deliberately skipped the
        // record (e.g. the Kimi wire metadata header). Advance the line
        // counter and checkpoint, but emit nothing downstream — passing a
        // `Value::Null` through to ClickHouse breaks the whole JSONEachRow
        // batch with "expected '{' before: 'null'".
        batch.extend_normalized(normalized);
        batch.lines_processed = batch.lines_processed.saturating_add(1);

        send_chunk_if_batch_exceeds_limits(
            &mut batch,
            config,
            &sink_tx,
            work,
            source_file,
            inode,
            checkpoint.source_generation,
            offset,
            line_no,
            "chunk",
        )
        .await?;
    }

    let final_checkpoint = Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: checkpoint.source_generation,
        last_offset: offset,
        last_line_no: line_no,
        status: "active".to_string(),
        ..Default::default()
    };

    if batch.row_count() > 0 || generation_changed || offset != checkpoint.last_offset {
        batch.checkpoint = Some(final_checkpoint);
        sink_tx
            .send(SinkMessage::Batch(batch))
            .await
            .context("sink channel closed while sending final batch")?;
    }

    if metrics.queue_depth.load(Ordering::Relaxed) == 0 {
        debug!(
            "{}:{} caught up at offset {}",
            work.source_name, source_file, offset
        );
    }

    Ok(())
}

/// Process a Hermes live-session file (single JSON document, rewritten in
/// place via atomic rename every save). Each message in `messages[]` is
/// normalized independently, with the checkpoint's `last_line_no` acting as a
/// "last-emitted message index" cursor. We pin a synthetic inode/generation so
/// event_uids remain stable across saves, and rely on the ClickHouse
/// ReplacingMergeTree on `events` to dedupe any re-emits.
async fn process_session_json_file(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let source_file = &work.path;

    let body = match std::fs::read_to_string(source_file) {
        Ok(body) => body,
        Err(exc) => {
            debug!("session_json read skipped {}: {}", source_file, exc);
            return Ok(());
        }
    };
    let file_size = body.len() as u64;

    if body.trim().is_empty() {
        return Ok(());
    }

    let session_doc: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(exc) => {
            // Atomic-rename keeps the on-disk file consistent, so a parse error
            // likely means the writer is still warming up or the file is
            // corrupted. Emit an error row and move on — we'll try again on the
            // next modify event.
            warn!(source_file, "session_json parse failed; skipping: {}", exc);
            let error_row = json!({
                "source_name": work.source_name,
                "harness": work.harness,
                "source_file": source_file,
                "source_inode": SESSION_JSON_INODE,
                "source_generation": SESSION_JSON_GENERATION,
                "source_line_no": 0u64,
                "source_offset": 0u64,
                "error_kind": "json_parse_error",
                "error_text": exc.to_string(),
                "raw_fragment": truncate(&body, 20_000),
            });
            let mut batch = RowBatch::default();
            batch.push_error_row(error_row);
            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending session_json parse error")?;
            return Ok(());
        }
    };

    let messages = session_doc
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let message_count = messages.len() as u64;

    let cp_key = checkpoint_key(&work.source_name, source_file);
    let committed = { checkpoints.read().await.get(&cp_key).cloned() };

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: SESSION_JSON_INODE,
        source_generation: SESSION_JSON_GENERATION,
        last_offset: 0,
        last_line_no: 0,
        status: "active".to_string(),
        ..Default::default()
    });

    // Re-pin the synthetic identity on every run — older checkpoints written
    // before this code path existed may carry real inode/generation values.
    checkpoint.source_inode = SESSION_JSON_INODE;
    checkpoint.source_generation = SESSION_JSON_GENERATION;

    let already_emitted = checkpoint.last_line_no;
    if message_count < already_emitted {
        // Hermes's writer guards against this ("never overwrite a larger
        // session log with fewer messages"), so we treat it as a spurious
        // read. Don't rewind — leave the checkpoint alone.
        debug!(
            source_file,
            current = message_count,
            last_emitted = already_emitted,
            "session_json shrank; ignoring",
        );
        return Ok(());
    }

    // On the very first run for a session file, also emit the session_meta
    // pseudo-record so downstream consumers see harness/model/platform up front.
    let mut synthetic_records: Vec<(u64, Value)> = Vec::new();
    if already_emitted == 0 {
        synthetic_records.push((0, build_session_meta_record(&session_doc)));
    }
    for idx in already_emitted..message_count {
        let msg = &messages[idx as usize];
        synthetic_records.push((
            idx + 1,
            build_session_message_record(&session_doc, msg, idx),
        ));
    }

    if synthetic_records.is_empty() && file_size == checkpoint.last_offset {
        return Ok(());
    }

    let mut batch = RowBatch::default();
    let mut session_hint = String::new();
    let mut model_hint = String::new();
    let mut cwd_hint = String::new();

    for (line_no, record) in synthetic_records {
        let raw_json = serde_json::to_string(&record).unwrap_or_else(|_| "{}".to_string());
        match normalize_record(
            &record,
            &work.source_name,
            &work.harness,
            source_file,
            SESSION_JSON_INODE,
            SESSION_JSON_GENERATION,
            line_no,
            0,
            &session_hint,
            &model_hint,
            &cwd_hint,
        ) {
            Ok(normalized) => {
                session_hint = normalized.session_hint;
                model_hint = normalized.model_hint;
                cwd_hint = normalized.cwd_hint;
                // A null `raw_row` is the normalizer's "skip this record"
                // signal (e.g. Kimi wire metadata header). Still count the
                // line for checkpointing, but don't emit a null row — it
                // would poison the JSONEachRow batch at flush time.
                if !normalized.raw_row.is_null() {
                    batch.push_raw_row(normalized.raw_row);
                }
                batch.extend_event_rows(normalized.event_rows);
                batch.extend_link_rows(normalized.link_rows);
                batch.extend_tool_rows(normalized.tool_rows);
                batch.extend_error_rows(normalized.error_rows);
                batch.lines_processed = batch.lines_processed.saturating_add(1);
            }
            Err(exc) => {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": SESSION_JSON_INODE,
                    "source_generation": SESSION_JSON_GENERATION,
                    "source_line_no": line_no,
                    "source_offset": 0u64,
                    "error_kind": "normalize_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&raw_json, 20_000),
                }));
            }
        }

        if batch.exceeds_limits(config.ingest.batch_size, config.ingest.max_batch_bytes) {
            let chunk = batch.drain_to_chunk();
            sink_tx
                .send(SinkMessage::Batch(chunk))
                .await
                .context("sink channel closed while sending session_json chunk")?;
        }
    }

    let final_checkpoint = Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: SESSION_JSON_INODE,
        source_generation: SESSION_JSON_GENERATION,
        last_offset: file_size,
        last_line_no: message_count,
        status: "active".to_string(),
        ..Default::default()
    };

    if batch.row_count() > 0
        || message_count != already_emitted
        || file_size != checkpoint.last_offset
    {
        batch.checkpoint = Some(final_checkpoint);
        sink_tx
            .send(SinkMessage::Batch(batch))
            .await
            .context("sink channel closed while sending final session_json batch")?;
    }

    if metrics.queue_depth.load(Ordering::Relaxed) == 0 {
        debug!(
            "{}:{} session_json caught up at message_count={}",
            work.source_name, source_file, message_count
        );
    }

    Ok(())
}

fn build_session_meta_record(session_doc: &Value) -> Value {
    let session_id = session_doc
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let base_url = session_doc
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let model = compose_hermes_model(
        session_doc
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        &base_url,
    );
    let platform = session_doc
        .get("platform")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let session_start = session_doc
        .get("session_start")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let last_updated = session_doc
        .get("last_updated")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let system_prompt = session_doc
        .get("system_prompt")
        .cloned()
        .unwrap_or(Value::Null);
    let tools = session_doc.get("tools").cloned().unwrap_or(Value::Null);
    let message_count = session_doc
        .get("message_count")
        .cloned()
        .unwrap_or(Value::Null);

    // `timestamp` is expected top-level by normalize_record for event_ts
    // derivation. We prefer the session start; callers can always fall back to
    // `record_ts` on the raw row if needed.
    let timestamp = if !session_start.is_empty() {
        session_start.clone()
    } else {
        last_updated.clone()
    };

    json!({
        "type": "session_meta",
        "timestamp": timestamp,
        "session_id": session_id,
        "model": model,
        "base_url": base_url,
        "platform": platform,
        "session_start": session_start,
        "last_updated": last_updated,
        "system_prompt": system_prompt,
        "tools": tools,
        "message_count": message_count,
    })
}

fn build_session_message_record(session_doc: &Value, message: &Value, message_index: u64) -> Value {
    let session_id = session_doc
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let base_url = session_doc
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let model = compose_hermes_model(
        session_doc
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        &base_url,
    );
    let platform = session_doc
        .get("platform")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let last_updated = session_doc
        .get("last_updated")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let session_start = session_doc
        .get("session_start")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let timestamp = if !last_updated.is_empty() {
        last_updated.clone()
    } else {
        session_start.clone()
    };

    json!({
        "type": "session_message",
        "timestamp": timestamp,
        "session_id": session_id,
        "model": model,
        "base_url": base_url,
        "platform": platform,
        "message_index": message_index,
        "message": message,
    })
}

pub(crate) fn source_inode_for_file(source_file: &str, meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        let _ = source_file;
        meta.ino()
    }

    #[cfg(not(unix))]
    {
        non_unix_source_inode(source_file, meta)
    }
}

#[cfg(not(unix))]
fn non_unix_source_inode(source_file: &str, meta: &std::fs::Metadata) -> u64 {
    if let Ok(handle) = Handle::from_path(source_file) {
        let id = hash_identity(&handle);
        if id != 0 {
            return id;
        }
    }

    // Fallback when a platform file handle identity is unavailable.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    source_file.hash(&mut hasher);
    if let Ok(created_at) = meta.created() {
        if let Ok(since_epoch) = created_at.duration_since(UNIX_EPOCH) {
            since_epoch.as_nanos().hash(&mut hasher);
        }
    }

    let id = hasher.finish();
    if id == 0 {
        1
    } else {
        id
    }
}

#[cfg(not(unix))]
fn hash_identity(value: &impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn truncate(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        complete_work, compose_hermes_model, enqueue_work, enrich_claude_model_latency,
        infer_vendor_from_base_url, jsonl_source_line_byte_limit, process_file,
        process_session_json_file, run_work_item, source_inode_for_file, work_item_is_ingestable,
        work_path_is_canonical, SessionCursor, CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT,
        ERROR_KIND_NORMALIZED_ROW_TOO_LARGE, ERROR_KIND_SOURCE_LINE_TOO_LARGE,
        SESSION_JSON_GENERATION, SESSION_JSON_INODE,
    };
    use crate::model::Checkpoint;
    use crate::{DispatchState, Metrics, SinkMessage, WorkItem};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::{mpsc, RwLock, Semaphore};
    use tokio::time::timeout;

    fn sample_work(path: &str) -> WorkItem {
        WorkItem {
            source_name: "test-source".to_string(),
            harness: "test-harness".to_string(),
            format: "jsonl".to_string(),
            path: path.to_string(),
        }
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("moraine-dispatch-{name}-{suffix}.jsonl"))
    }

    #[test]
    fn complete_work_prunes_idle_item() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let work = sample_work("/tmp/idle.jsonl");
        let key = work.key();

        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.inflight.insert(key.clone());
            state.item_by_key.insert(key.clone(), work);
        }

        let reschedule = complete_work(&key, &dispatch);
        assert!(reschedule.is_none());

        let state = dispatch.lock().expect("dispatch mutex poisoned");
        assert!(!state.inflight.contains(&key));
        assert!(!state.pending.contains(&key));
        assert!(!state.dirty.contains(&key));
        assert!(!state.item_by_key.contains_key(&key));
    }

    #[test]
    fn complete_work_reschedules_dirty_item() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let work = sample_work("/tmp/dirty.jsonl");
        let key = work.key();

        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.inflight.insert(key.clone());
            state.dirty.insert(key.clone());
            state.item_by_key.insert(key.clone(), work.clone());
        }

        let reschedule = complete_work(&key, &dispatch);
        assert_eq!(
            reschedule.as_ref().map(|item| item.path.as_str()),
            Some(work.path.as_str())
        );

        let state = dispatch.lock().expect("dispatch mutex poisoned");
        assert!(!state.inflight.contains(&key));
        assert!(!state.dirty.contains(&key));
        assert!(state.pending.contains(&key));
        assert!(state.item_by_key.contains_key(&key));
    }

    #[test]
    fn complete_work_keeps_item_when_still_pending() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let work = sample_work("/tmp/pending.jsonl");
        let key = work.key();

        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.insert(key.clone());
            state.item_by_key.insert(key.clone(), work);
        }

        let reschedule = complete_work(&key, &dispatch);
        assert!(reschedule.is_none());

        let state = dispatch.lock().expect("dispatch mutex poisoned");
        assert!(state.pending.contains(&key));
        assert!(state.item_by_key.contains_key(&key));
    }

    #[test]
    fn source_inode_is_stable_for_same_file() {
        let path = unique_test_file("identity-stable");
        fs::write(&path, "{\"line\":1}\n").expect("write initial file");
        let source_file = path.to_string_lossy().to_string();

        let first_meta = fs::metadata(&path).expect("metadata for initial file");
        let first_id = source_inode_for_file(&source_file, &first_meta);
        assert_ne!(first_id, 0);

        fs::write(&path, "{\"line\":1}\n{\"line\":2}\n").expect("append file content");
        let second_meta = fs::metadata(&path).expect("metadata after append");
        let second_id = source_inode_for_file(&source_file, &second_meta);

        let _ = fs::remove_file(&path);
        assert_eq!(first_id, second_id);
    }

    #[test]
    fn source_inode_changes_when_file_is_replaced() {
        let path = unique_test_file("identity-replaced");
        let replacement = unique_test_file("identity-replacement");
        fs::write(&path, "{\"line\":1}\n").expect("write original file");
        let source_file = path.to_string_lossy().to_string();

        let original_meta = fs::metadata(&path).expect("metadata for original file");
        let original_id = source_inode_for_file(&source_file, &original_meta);
        assert_ne!(original_id, 0);

        fs::write(&replacement, "{\"line\":99}\n").expect("write replacement file");
        fs::rename(&replacement, &path).expect("replace file via rename");

        let replaced_meta = fs::metadata(&path).expect("metadata for replaced file");
        let replaced_id = source_inode_for_file(&source_file, &replaced_meta);

        let _ = fs::remove_file(&path);
        assert_ne!(original_id, replaced_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_work_item_releases_permit_before_reschedule_send() {
        let path = unique_test_file("reschedule-no-deadlock");
        fs::write(&path, "").expect("write empty jsonl");
        let work = WorkItem {
            source_name: "test-source".to_string(),
            harness: "test-harness".to_string(),
            format: "jsonl".to_string(),
            path: path.to_string_lossy().to_string(),
        };
        let key = work.key();

        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.inflight.insert(key.clone());
            state.dirty.insert(key.clone());
            state.item_by_key.insert(key.clone(), work.clone());
        }

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());

        let (sink_tx, _sink_rx) = mpsc::channel::<SinkMessage>(8);
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(1);
        process_tx
            .send(work.clone())
            .await
            .expect("prime process_tx so reschedule send will block");

        let sem = Arc::new(Semaphore::new(1));
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("acquire initial permit");

        let task = tokio::spawn(run_work_item(
            config,
            work,
            permit,
            checkpoints,
            sink_tx,
            process_tx,
            dispatch,
            metrics,
        ));

        let released = timeout(Duration::from_millis(500), sem.acquire()).await;
        assert!(
            released.is_ok(),
            "permit must be released before the reschedule `send` blocks on a full channel"
        );

        process_rx.recv().await.expect("priming item");

        let rescheduled = timeout(Duration::from_millis(500), process_rx.recv())
            .await
            .expect("rescheduled send should complete once channel drains")
            .expect("rescheduled work item delivered");
        assert_eq!(rescheduled.key(), key);

        task.await.expect("run_work_item task should finish");

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn work_path_canonical_check_matches_format() {
        let jsonl = WorkItem {
            source_name: "s".to_string(),
            harness: "hermes".to_string(),
            format: "jsonl".to_string(),
            path: "/tmp/x.jsonl".to_string(),
        };
        assert!(work_path_is_canonical(&jsonl));

        let session = WorkItem {
            source_name: "s".to_string(),
            harness: "hermes".to_string(),
            format: "session_json".to_string(),
            path: "/tmp/session_x.json".to_string(),
        };
        assert!(work_path_is_canonical(&session));
        // session_json format must NOT pick up .jsonl files
        let wrong = WorkItem {
            path: "/tmp/x.jsonl".to_string(),
            ..session.clone()
        };
        assert!(!work_path_is_canonical(&wrong));

        let sqlite = WorkItem {
            source_name: "s".to_string(),
            harness: "cursor".to_string(),
            format: "cursor_sqlite".to_string(),
            path: "/tmp/User/state.vscdb".to_string(),
        };
        assert!(work_path_is_canonical(&sqlite));
        // Sidecars are canonicalized upstream; a sidecar path reaching the
        // dispatcher directly is dropped rather than processed.
        let sidecar = WorkItem {
            path: "/tmp/User/state.vscdb-wal".to_string(),
            ..sqlite.clone()
        };
        assert!(!work_path_is_canonical(&sidecar));
    }

    #[test]
    fn workflow_journals_are_not_ingestable_but_sessions_and_subagents_are() {
        let claude = |path: &str| WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: "jsonl".to_string(),
            path: path.to_string(),
        };
        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";

        // The orphan workflow journal is rejected even though it is the
        // canonical path for the jsonl format (issue #386).
        let journal = claude(&format!(
            "{proj}/{sid}/subagents/workflows/wf_12dc2994-7e9/journal.jsonl"
        ));
        assert!(work_path_is_canonical(&journal));
        assert!(!work_item_is_ingestable(&journal));

        // Real sessions and both kinds of subagent transcripts stay ingestible.
        assert!(work_item_is_ingestable(&claude(&format!(
            "{proj}/{sid}.jsonl"
        ))));
        assert!(work_item_is_ingestable(&claude(&format!(
            "{proj}/{sid}/subagents/workflows/wf_8dc1b543-8da/agent-a38ca143465605620.jsonl"
        ))));
        assert!(work_item_is_ingestable(&claude(&format!(
            "{proj}/{sid}/subagents/agent-a5a524a7f876aa747.jsonl"
        ))));

        // The exclusion is scoped to claude-code: the same path under another
        // harness/source must not be silently dropped.
        let codex_journal = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: "jsonl".to_string(),
            path: format!("{proj}/{sid}/subagents/workflows/wf_x/journal.jsonl"),
        };
        assert!(work_item_is_ingestable(&codex_journal));
    }

    /// End-to-end through the dispatch gate: a workflow journal enqueued from
    /// any entry point (backfill/reconcile/watcher all call `enqueue_work`)
    /// must never reach the processor channel or the dispatch state, while a
    /// real session transcript does. This is the behavior that keeps the
    /// empty-`session_id` junk out of ClickHouse.
    #[tokio::test(flavor = "multi_thread")]
    async fn enqueue_work_drops_workflow_journals_before_processing() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let journal = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: "jsonl".to_string(),
            path: format!("{proj}/{sid}/subagents/workflows/wf_12dc2994-7e9/journal.jsonl"),
        };

        enqueue_work(journal.clone(), &process_tx, &dispatch, &metrics).await;

        assert!(
            process_rx.try_recv().is_err(),
            "workflow journal must not be forwarded to the processor"
        );
        {
            let state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(state.pending.is_empty(), "no pending work for a journal");
            assert!(
                !state.item_by_key.contains_key(&journal.key()),
                "journal must not be tracked in dispatch state"
            );
        }
        assert_eq!(metrics.queue_depth.load(Ordering::Relaxed), 0);

        // A real session transcript from the same source is forwarded.
        let session = WorkItem {
            path: format!("{proj}/{sid}.jsonl"),
            ..journal.clone()
        };
        enqueue_work(session.clone(), &process_tx, &dispatch, &metrics).await;
        let forwarded = process_rx
            .try_recv()
            .expect("real session transcript must be forwarded");
        assert_eq!(forwarded.key(), session.key());
        assert_eq!(metrics.queue_depth.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn base_url_vendor_inference_covers_common_providers() {
        assert_eq!(
            infer_vendor_from_base_url("https://api.anthropic.com"),
            "anthropic"
        );
        assert_eq!(
            infer_vendor_from_base_url("https://api.openai.com/v1"),
            "openai"
        );
        assert_eq!(
            infer_vendor_from_base_url("https://openrouter.ai"),
            "openrouter"
        );
        assert_eq!(infer_vendor_from_base_url(""), "");
        assert_eq!(
            infer_vendor_from_base_url("https://unknown.example.com"),
            ""
        );
    }

    #[test]
    fn compose_hermes_model_prepends_vendor_when_bare() {
        assert_eq!(
            compose_hermes_model("claude-opus-4-6", "https://api.anthropic.com"),
            "anthropic/claude-opus-4-6",
        );
        // Already vendor-qualified — leave it alone.
        assert_eq!(
            compose_hermes_model("openai/gpt-5", "https://api.anthropic.com"),
            "openai/gpt-5",
        );
        // No vendor we can recognize — bare model survives.
        assert_eq!(
            compose_hermes_model("some-model", "https://weird.local/"),
            "some-model",
        );
    }

    fn write_session_file(path: &PathBuf, messages: &[serde_json::Value]) {
        let doc = serde_json::json!({
            "session_id": "20260418_live_test",
            "model": "claude-opus-4-6",
            "base_url": "https://api.anthropic.com",
            "platform": "cli",
            "session_start": "2026-04-18T12:00:00.000000",
            "last_updated": "2026-04-18T12:00:00.000000",
            "system_prompt": "you are a test agent",
            "tools": [],
            "message_count": messages.len(),
            "messages": messages,
        });
        let body = serde_json::to_string_pretty(&doc).unwrap();
        std::fs::write(path, body).expect("write session file");
    }

    fn unique_session_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("moraine-session-{name}-{suffix}.json"))
    }

    async fn drain_batches(rx: &mut mpsc::Receiver<SinkMessage>) -> Vec<crate::model::RowBatch> {
        let mut out = Vec::new();
        while let Ok(Some(SinkMessage::Batch(batch))) =
            timeout(Duration::from_millis(50), rx.recv()).await
        {
            out.push(batch);
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_inherits_codex_timestamp_for_legacy_rollout_records() {
        let path = unique_test_file("rollout-2025-09-21T17-12-48-legacy");
        fs::write(
            &path,
            [
                json!({
                    "id": "6ce8b66e-8a97-441b-a606-16d2a0c27083",
                    "timestamp": "2025-09-21T17:12:48.127Z",
                    "instructions": null
                })
                .to_string(),
                json!({
                    "type": "function_call",
                    "call_id": "call_legacy_rollout",
                    "name": "shell",
                    "arguments": "{}"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write legacy rollout fixture");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: "jsonl".to_string(),
            path: source_file,
        };

        process_file(&config, &work, checkpoints, sink_tx, &metrics)
            .await
            .expect("legacy codex file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 2);
        assert!(
            batch
                .error_rows
                .iter()
                .all(|row| row.get("error_kind").and_then(Value::as_str)
                    != Some("timestamp_parse_error")),
            "legacy timestamp inheritance should avoid timestamp_parse_error rows"
        );
        assert_eq!(
            batch.raw_rows[1]
                .get("record_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2025-09-21T17:12:48.127Z"
        );
        assert_eq!(
            batch.event_rows[1]
                .get("event_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2025-09-21 17:12:48.127"
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_recovers_session_cwd_when_resuming_mid_file() {
        let path = unique_test_file("codex-resume-cwd");
        let header = serde_json::json!({
            "type": "session_meta",
            "timestamp": "2026-04-18T20:43:51.069Z",
            "payload": {
                "id": "codex-session-1",
                "cwd": "/repo"
            }
        })
        .to_string();
        let tail = serde_json::json!({
            "type": "function_call",
            "timestamp": "2026-04-18T20:43:52.069Z",
            "call_id": "call_resumed",
            "name": "shell",
            "arguments": "{}"
        })
        .to_string();
        fs::write(&path, format!("{header}\n{tail}\n")).expect("write codex resume fixture");

        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: "jsonl".to_string(),
            path: source_file.clone(),
        };

        // Simulate a restart that already ingested the session header: the
        // checkpoint sits past line 1, so the in-stream cwd hint chain never
        // sees `payload.cwd` and must be recovered from the file head.
        let meta = fs::metadata(&path).expect("fixture metadata");
        let inode = source_inode_for_file(&source_file, &meta);
        let committed = Checkpoint {
            source_name: work.source_name.clone(),
            source_file: source_file.clone(),
            source_inode: inode,
            source_generation: 1,
            last_offset: (header.len() + 1) as u64,
            last_line_no: 1,
            status: "active".to_string(),
            ..Default::default()
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        {
            let mut guard = checkpoints.write().await;
            guard.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &source_file),
                committed,
            );
        }

        let config = moraine_config::AppConfig::default();
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        process_file(&config, &work, checkpoints, sink_tx, &metrics)
            .await
            .expect("resumed codex file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 1, "only the tail record re-emits");
        assert_eq!(
            batch.raw_rows[0].get("cwd").and_then(Value::as_str),
            Some("/repo"),
            "resumed records inherit the session cwd from the file head"
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_infers_leading_claude_metadata_timestamp() {
        let path = unique_test_file("claude-leading-metadata");
        fs::write(
            &path,
            [
                json!({
                    "type": "permission-mode",
                    "sessionId": "session-with-leading-metadata",
                    "permissionMode": "acceptEdits"
                })
                .to_string(),
                json!({
                    "type": "file-history-snapshot",
                    "messageId": "msg_1",
                    "isSnapshotUpdate": true,
                    "snapshot": {}
                })
                .to_string(),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-18T20:43:51.069Z",
                    "uuid": "00a635eb-f13f-4a0e-9898-a3ad7b71ca47",
                    "parentUuid": null,
                    "sessionId": "session-with-leading-metadata",
                    "message": {
                        "role": "user",
                        "content": "hello"
                    }
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write claude metadata fixture");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: "jsonl".to_string(),
            path: source_file,
        };

        process_file(&config, &work, checkpoints, sink_tx, &metrics)
            .await
            .expect("claude file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 3);
        assert!(
            batch
                .error_rows
                .iter()
                .all(|row| row.get("error_kind").and_then(Value::as_str)
                    != Some("timestamp_parse_error")),
            "leading metadata should inherit the first parseable record timestamp"
        );
        assert_eq!(
            batch.raw_rows[0]
                .get("record_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2026-04-18T20:43:51.069Z"
        );
        assert_eq!(
            batch.raw_rows[1]
                .get("record_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2026-04-18T20:43:51.069Z"
        );
        assert!(
            batch.event_rows.iter().all(|row| {
                row.get("event_ts").and_then(Value::as_str) == Some("2026-04-18 20:43:51.069")
            }),
            "metadata and message events should share the inferred timestamp"
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_reports_pi_malformed_jsonl_without_dropping_valid_rows() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("fixtures")
            .join("pi")
            .join("malformed.jsonl");
        let source_file = path.to_string_lossy().to_string();

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let work = WorkItem {
            source_name: "pi".to_string(),
            harness: "pi-coding-agent".to_string(),
            format: "jsonl".to_string(),
            path: source_file,
        };

        process_file(&config, &work, checkpoints, sink_tx, &metrics)
            .await
            .expect("pi fixture should process around malformed line");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 2);
        assert_eq!(batch.event_rows.len(), 2);
        assert_eq!(batch.error_rows.len(), 1);
        assert_eq!(
            batch.error_rows[0]
                .get("error_kind")
                .and_then(Value::as_str),
            Some("json_parse_error")
        );
        assert_eq!(
            batch.raw_rows[0].get("harness").and_then(Value::as_str),
            Some("pi-coding-agent")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_quarantines_oversized_jsonl_line_and_continues() {
        let path = unique_test_file("codex-oversized-line");
        let first = json!({
            "type": "session_meta",
            "timestamp": "2026-06-27T10:00:00.000Z",
            "payload": {
                "id": "codex-oversized-line-session",
                "cwd": "/repo"
            }
        })
        .to_string();
        let line_limit = 4096usize;
        let oversized_output = "x".repeat(line_limit + 1024);
        let oversized = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:01.000Z",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_too_large",
                "output": oversized_output,
            }
        })
        .to_string();
        let third = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:02.000Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "after the oversized line"
                    }
                ]
            }
        })
        .to_string();
        let body = format!("{first}\n{oversized}\n{third}\n");
        let oversized_offset = (first.len() + 1) as u64;
        let oversized_line_bytes = oversized.len() + 1;
        let final_offset = body.len() as u64;
        fs::write(&path, body).expect("write oversized codex fixture");

        let mut config = moraine_config::AppConfig::default();
        config.ingest.max_batch_bytes = line_limit;
        assert_eq!(jsonl_source_line_byte_limit(&config), line_limit);

        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: "jsonl".to_string(),
            path: source_file.clone(),
        };

        process_file(&config, &work, checkpoints, sink_tx, &metrics)
            .await
            .expect("oversized codex file should process around the large line");

        let batches = drain_batches(&mut sink_rx).await;
        assert!(!batches.is_empty(), "expected at least one sink batch");
        let raw_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .collect();
        let event_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.event_rows.iter())
            .collect();
        let error_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter())
            .collect();

        assert_eq!(raw_rows.len(), 2, "the oversized line emits no raw row");
        assert!(
            raw_rows
                .iter()
                .all(|row| row.get("source_line_no").and_then(Value::as_u64) != Some(2)),
            "line 2 must be quarantined instead of normalized"
        );
        assert_eq!(
            raw_rows[1].get("source_line_no").and_then(Value::as_u64),
            Some(3),
            "the line after the oversized record must still normalize"
        );
        assert!(
            event_rows
                .iter()
                .any(|row| row.get("text_content").and_then(Value::as_str)
                    == Some("after the oversized line")),
            "subsequent JSONL lines must continue processing"
        );

        assert_eq!(error_rows.len(), 1);
        let error = error_rows[0];
        assert_eq!(
            error.get("error_kind").and_then(Value::as_str),
            Some(ERROR_KIND_SOURCE_LINE_TOO_LARGE)
        );
        assert_eq!(
            error.get("source_file").and_then(Value::as_str),
            Some(source_file.as_str())
        );
        assert_eq!(error.get("source_line_no").and_then(Value::as_u64), Some(2));
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(oversized_offset)
        );
        assert!(error
            .get("error_text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains(&oversized_line_bytes.to_string())));
        let raw_fragment = error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .expect("oversized line error should include compact metadata");
        assert!(
            raw_fragment.len() < 256,
            "oversized quarantine metadata must stay compact"
        );
        let fragment: Value =
            serde_json::from_str(raw_fragment).expect("raw_fragment should be JSON metadata");
        assert_eq!(
            fragment.get("line_bytes").and_then(Value::as_u64),
            Some(oversized_line_bytes as u64)
        );
        assert_eq!(
            fragment.get("limit_bytes").and_then(Value::as_u64),
            Some(line_limit as u64)
        );
        assert_eq!(
            fragment.get("action").and_then(Value::as_str),
            Some("skipped")
        );

        let final_checkpoint = batches
            .iter()
            .filter_map(|batch| batch.checkpoint.as_ref())
            .max_by_key(|checkpoint| checkpoint.last_offset)
            .expect("oversized line processing should emit a checkpoint");
        assert_eq!(final_checkpoint.last_offset, final_offset);
        assert_eq!(final_checkpoint.last_line_no, 3);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_quarantines_rows_that_expand_past_clickhouse_object_limit() {
        let path = unique_test_file("codex-expanded-row-too-large");
        let first = json!({
            "type": "session_meta",
            "timestamp": "2026-06-27T10:00:00.000Z",
            "payload": {
                "id": "codex-expanded-row-too-large-session",
                "cwd": "/repo"
            }
        })
        .to_string();
        let backslash_count = (CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT / 4) + 200_000;
        let escaped_heavy_output = "\\".repeat(backslash_count);
        let expanded = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:01.000Z",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_expands_too_large",
                "output": escaped_heavy_output,
            }
        })
        .to_string();
        assert!(
            expanded.len() < jsonl_source_line_byte_limit(&moraine_config::AppConfig::default()),
            "fixture must stay below the source-line cap to exercise serialized row sizing"
        );
        let third = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:02.000Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "after the expanded row"
                    }
                ]
            }
        })
        .to_string();
        let body = format!("{first}\n{expanded}\n{third}\n");
        let expanded_offset = (first.len() + 1) as u64;
        let expanded_line_bytes = expanded.len() + 1;
        let final_offset = body.len() as u64;
        fs::write(&path, body).expect("write expanded-row codex fixture");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: "jsonl".to_string(),
            path: source_file.clone(),
        };

        process_file(&config, &work, checkpoints, sink_tx, &metrics)
            .await
            .expect("expanded-row codex file should process around the unsafe row");

        let batches = drain_batches(&mut sink_rx).await;
        assert!(!batches.is_empty(), "expected at least one sink batch");
        let raw_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .collect();
        let event_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.event_rows.iter())
            .collect();
        let error_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter())
            .collect();

        assert_eq!(raw_rows.len(), 2, "the expanded row emits no raw row");
        assert!(
            raw_rows
                .iter()
                .all(|row| row.get("source_line_no").and_then(Value::as_u64) != Some(2)),
            "line 2 must be quarantined instead of inserted"
        );
        assert!(
            event_rows
                .iter()
                .any(|row| row.get("text_content").and_then(Value::as_str)
                    == Some("after the expanded row")),
            "subsequent JSONL lines must continue processing"
        );

        assert_eq!(error_rows.len(), 1);
        let error = error_rows[0];
        assert_eq!(
            error.get("error_kind").and_then(Value::as_str),
            Some(ERROR_KIND_NORMALIZED_ROW_TOO_LARGE)
        );
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(expanded_offset)
        );
        let raw_fragment = error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .expect("expanded row error should include compact metadata");
        assert!(
            raw_fragment.len() < 320,
            "expanded-row quarantine metadata must stay compact"
        );
        let fragment: Value =
            serde_json::from_str(raw_fragment).expect("raw_fragment should be JSON metadata");
        assert_eq!(
            fragment.get("line_bytes").and_then(Value::as_u64),
            Some(expanded_line_bytes as u64)
        );
        assert!(
            fragment
                .get("serialized_row_bytes")
                .and_then(Value::as_u64)
                .is_some_and(|bytes| bytes > CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT as u64),
            "quarantine should record the unsafe serialized row size"
        );
        assert_eq!(
            fragment.get("limit_bytes").and_then(Value::as_u64),
            Some(CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT as u64)
        );

        let final_checkpoint = batches
            .iter()
            .filter_map(|batch| batch.checkpoint.as_ref())
            .max_by_key(|checkpoint| checkpoint.last_offset)
            .expect("expanded-row processing should emit a checkpoint");
        assert_eq!(final_checkpoint.last_offset, final_offset);
        assert_eq!(final_checkpoint.last_line_no, 3);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_session_json_emits_only_new_messages_on_growth() {
        let path = unique_session_file("growth");
        let source_file = path.to_string_lossy().to_string();

        // First snapshot: just a user turn.
        let msgs_v1 = vec![serde_json::json!({
            "role": "user",
            "content": "hello"
        })];
        write_session_file(&path, &msgs_v1);

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        let work = WorkItem {
            source_name: "hermes-live".to_string(),
            harness: "hermes".to_string(),
            format: "session_json".to_string(),
            path: source_file.clone(),
        };

        process_session_json_file(
            &config,
            &work,
            checkpoints.clone(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("first session_json run");

        let batches_v1 = drain_batches(&mut sink_rx).await;
        assert_eq!(batches_v1.len(), 1, "single flushed batch on first run");
        let b1 = &batches_v1[0];
        // session_meta + user message = 2 event rows.
        assert_eq!(b1.event_rows.len(), 2, "session_meta + user message events");
        assert_eq!(
            b1.checkpoint.as_ref().expect("checkpoint").last_line_no,
            1,
            "checkpoint advances to message_count=1",
        );
        // Apply the checkpoint like the sink would.
        let cp = b1.checkpoint.as_ref().unwrap().clone();
        {
            let mut guard = checkpoints.write().await;
            guard.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &source_file),
                cp,
            );
        }

        let first_uids: Vec<String> = b1
            .event_rows
            .iter()
            .map(|r| {
                r.get("event_uid")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
            .collect();

        // Grow the file to 2 messages. We intentionally rewrite via plain write
        // (no atomic rename here since that's already covered by the fact that
        // we pin SESSION_JSON_INODE=0).
        let msgs_v2 = vec![
            serde_json::json!({ "role": "user", "content": "hello" }),
            serde_json::json!({ "role": "assistant", "content": "hi there" }),
        ];
        write_session_file(&path, &msgs_v2);

        process_session_json_file(&config, &work, checkpoints.clone(), sink_tx, &metrics)
            .await
            .expect("second session_json run");

        let batches_v2 = drain_batches(&mut sink_rx).await;
        assert_eq!(batches_v2.len(), 1, "second run flushed a single batch");
        let b2 = &batches_v2[0];
        // Only the newly-appeared assistant message should emit this time
        // (session_meta was already emitted on the first run).
        assert_eq!(
            b2.event_rows.len(),
            1,
            "only the new assistant message emits on the second run",
        );
        assert_eq!(
            b2.event_rows[0].get("actor_kind").and_then(Value::as_str),
            Some("assistant"),
        );
        assert_eq!(
            b2.checkpoint.as_ref().expect("checkpoint").last_line_no,
            2,
            "checkpoint advances to message_count=2",
        );

        // New row's uid must not collide with any of the first-run uids.
        let new_uid = b2.event_rows[0]
            .get("event_uid")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        assert!(
            !first_uids.contains(&new_uid),
            "second-run uid collides with first-run",
        );

        // Sanity: pinned synthetic inode/generation preserved on the checkpoint.
        let cp2 = b2.checkpoint.as_ref().unwrap();
        assert_eq!(cp2.source_inode, SESSION_JSON_INODE);
        assert_eq!(cp2.source_generation, SESSION_JSON_GENERATION);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_session_json_is_a_noop_when_nothing_changes() {
        let path = unique_session_file("noop");
        let source_file = path.to_string_lossy().to_string();
        let msgs = vec![serde_json::json!({
            "role": "user",
            "content": "stable"
        })];
        write_session_file(&path, &msgs);

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        let work = WorkItem {
            source_name: "hermes-live".to_string(),
            harness: "hermes".to_string(),
            format: "session_json".to_string(),
            path: source_file.clone(),
        };

        process_session_json_file(
            &config,
            &work,
            checkpoints.clone(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("first run");
        let first = drain_batches(&mut sink_rx).await;
        assert_eq!(first.len(), 1);
        let cp = first[0].checkpoint.as_ref().unwrap().clone();
        {
            let mut guard = checkpoints.write().await;
            guard.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &source_file),
                cp,
            );
        }

        // Second run on unchanged file → no batches sent.
        process_session_json_file(&config, &work, checkpoints.clone(), sink_tx, &metrics)
            .await
            .expect("second run");
        let second = drain_batches(&mut sink_rx).await;
        assert!(
            second.is_empty(),
            "unchanged file should produce zero batches; got {} batches",
            second.len(),
        );

        let _ = fs::remove_file(&path);
    }

    fn event_row(session_id: &str, event_ts: &str, event_kind: &str, actor_kind: &str) -> Value {
        json!({
            "session_id": session_id,
            "event_ts": event_ts,
            "event_kind": event_kind,
            "actor_kind": actor_kind,
            "latency_ms": 0u32,
        })
    }

    fn latency_of(row: &Value) -> u64 {
        row.get("latency_ms").and_then(|v| v.as_u64()).unwrap_or(0)
    }

    #[test]
    fn latency_enrichment_stamps_assistant_after_tool_result() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s1";

        // 1) tool_result at T0.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "tool_result",
            "tool",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);
        assert_eq!(latency_of(&rows[0]), 0, "tool_result itself is untouched");

        // 2) assistant turn 4.25s later: thinking + tool_use, same event_ts.
        let mut rows = vec![
            event_row(session, "2026-04-19 12:00:04.250", "reasoning", "assistant"),
            event_row(session, "2026-04-19 12:00:04.250", "tool_call", "assistant"),
        ];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);
        assert_eq!(
            latency_of(&rows[0]),
            4250,
            "first assistant block carries the model latency"
        );
        assert_eq!(
            latency_of(&rows[1]),
            0,
            "subsequent blocks in the same turn are not double-stamped"
        );
    }

    #[test]
    fn latency_enrichment_skips_fresh_user_prompt() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s2";

        // User typed a prompt.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "message",
            "user",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        // Assistant replies 10s later — gap is human typing + model time.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:10.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        assert_eq!(
            latency_of(&rows[0]),
            0,
            "assistant after fresh user prompt must not be stamped (ambiguous wait)"
        );
    }

    #[test]
    fn latency_enrichment_resets_after_user_breaks_chain() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s3";

        // tool_result → user prompt → assistant: chain broken by user.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "tool_result",
            "tool",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:05.000",
            "message",
            "user",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:07.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        assert_eq!(
            latency_of(&rows[0]),
            0,
            "intervening user prompt breaks the tool_result → assistant chain"
        );
    }

    #[test]
    fn latency_enrichment_skips_non_claude_harness() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s4";

        // Seed cursor as if a tool_result happened.
        cursors.insert(
            session.to_string(),
            SessionCursor {
                prev_event_ts_ms: 1_000,
                prev_was_tool_result: true,
            },
        );

        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:10.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("codex", &mut rows, &mut cursors);
        assert_eq!(latency_of(&rows[0]), 0, "non-claude harness is a no-op");
    }

    #[test]
    fn latency_enrichment_ignores_system_events_when_advancing_cursor() {
        // A progress/system event between tool_result and assistant must
        // NOT reset the cursor — otherwise we'd lose valid latency data.
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s5";

        // 1) tool_result
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "tool_result",
            "tool",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        // 2) out-of-band system event (no turn actor)
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.500",
            "system",
            "system",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        // 3) assistant response 3s after the tool_result
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:03.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        assert_eq!(
            latency_of(&rows[0]),
            3000,
            "system event should not reset the tool_result → assistant chain"
        );
    }
}
