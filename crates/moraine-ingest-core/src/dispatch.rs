use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use crate::{DispatchState, Metrics, SinkMessage, WorkItem};
use anyhow::{Context, Result};
use moraine_config::{AppConfig, SOURCE_FORMAT_SESSION_JSON};
use serde_json::{json, Value};
use std::collections::HashMap;
#[cfg(not(unix))]
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
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

fn work_extension(work: &WorkItem) -> &'static str {
    if work.format == SOURCE_FORMAT_SESSION_JSON {
        "json"
    } else {
        "jsonl"
    }
}

fn path_matches_extension(path: &str, extension: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        == Some(extension)
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
                            if !path_matches_extension(&work.path, work_extension(&work)) {
                                continue;
                            }

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
    if !path_matches_extension(&work.path, work_extension(&work)) {
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

    if should_send {
        if process_tx.send(work).await.is_ok() {
            metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        }
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
    let mut session_cursors: HashMap<String, SessionCursor> = HashMap::new();

    let mut batch = RowBatch::default();

    loop {
        let start_offset = offset;
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader
            .read_until(b'\n', &mut buf)
            .with_context(|| format!("failed reading {}", source_file))?;

        if bytes_read == 0 {
            break;
        }

        offset = offset.saturating_add(bytes_read as u64);
        line_no = line_no.saturating_add(1);

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
                batch.error_rows.push(json!({
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
                batch.error_rows.push(json!({
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

        let mut normalized = match normalize_record(
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
        ) {
            Ok(normalized) => normalized,
            Err(exc) => {
                batch.error_rows.push(json!({
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

        enrich_claude_model_latency(
            &work.harness,
            &mut normalized.event_rows,
            &mut session_cursors,
        );

        session_hint = normalized.session_hint;
        model_hint = normalized.model_hint;
        batch.raw_rows.push(normalized.raw_row);
        batch.event_rows.extend(normalized.event_rows);
        batch.link_rows.extend(normalized.link_rows);
        batch.tool_rows.extend(normalized.tool_rows);
        batch.error_rows.extend(normalized.error_rows);
        batch.lines_processed = batch.lines_processed.saturating_add(1);

        if batch.row_count() >= config.ingest.batch_size {
            let mut chunk = RowBatch::default();
            chunk.raw_rows = std::mem::take(&mut batch.raw_rows);
            chunk.event_rows = std::mem::take(&mut batch.event_rows);
            chunk.link_rows = std::mem::take(&mut batch.link_rows);
            chunk.tool_rows = std::mem::take(&mut batch.tool_rows);
            chunk.error_rows = std::mem::take(&mut batch.error_rows);
            chunk.lines_processed = batch.lines_processed;
            batch.lines_processed = 0;
            chunk.checkpoint = Some(Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.to_string(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: offset,
                last_line_no: line_no,
                status: "active".to_string(),
            });

            sink_tx
                .send(SinkMessage::Batch(chunk))
                .await
                .context("sink channel closed while sending chunk")?;
        }
    }

    let final_checkpoint = Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: checkpoint.source_generation,
        last_offset: offset,
        last_line_no: line_no,
        status: "active".to_string(),
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
            batch.error_rows.push(error_row);
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
        ) {
            Ok(normalized) => {
                session_hint = normalized.session_hint;
                model_hint = normalized.model_hint;
                batch.raw_rows.push(normalized.raw_row);
                batch.event_rows.extend(normalized.event_rows);
                batch.link_rows.extend(normalized.link_rows);
                batch.tool_rows.extend(normalized.tool_rows);
                batch.error_rows.extend(normalized.error_rows);
                batch.lines_processed = batch.lines_processed.saturating_add(1);
            }
            Err(exc) => {
                batch.error_rows.push(json!({
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

        if batch.row_count() >= config.ingest.batch_size {
            let mut chunk = RowBatch::default();
            chunk.raw_rows = std::mem::take(&mut batch.raw_rows);
            chunk.event_rows = std::mem::take(&mut batch.event_rows);
            chunk.link_rows = std::mem::take(&mut batch.link_rows);
            chunk.tool_rows = std::mem::take(&mut batch.tool_rows);
            chunk.error_rows = std::mem::take(&mut batch.error_rows);
            chunk.lines_processed = batch.lines_processed;
            batch.lines_processed = 0;
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

fn source_inode_for_file(source_file: &str, meta: &std::fs::Metadata) -> u64 {
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
        complete_work, compose_hermes_model, enrich_claude_model_latency,
        infer_vendor_from_base_url, path_matches_extension, process_session_json_file,
        run_work_item, source_inode_for_file, work_extension, SessionCursor,
        SESSION_JSON_GENERATION, SESSION_JSON_INODE,
    };
    use crate::model::Checkpoint;
    use crate::{DispatchState, Metrics, SinkMessage, WorkItem};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
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
    fn work_extension_matches_format() {
        let jsonl = WorkItem {
            source_name: "s".to_string(),
            harness: "hermes".to_string(),
            format: "jsonl".to_string(),
            path: "/tmp/x.jsonl".to_string(),
        };
        assert_eq!(work_extension(&jsonl), "jsonl");
        assert!(path_matches_extension(&jsonl.path, work_extension(&jsonl)));

        let session = WorkItem {
            source_name: "s".to_string(),
            harness: "hermes".to_string(),
            format: "session_json".to_string(),
            path: "/tmp/session_x.json".to_string(),
        };
        assert_eq!(work_extension(&session), "json");
        assert!(path_matches_extension(
            &session.path,
            work_extension(&session)
        ));
        // session_json format must NOT pick up .jsonl files
        let wrong = WorkItem {
            path: "/tmp/x.jsonl".to_string(),
            ..session.clone()
        };
        assert!(!path_matches_extension(&wrong.path, work_extension(&wrong)));
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
