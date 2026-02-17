use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use crate::{DispatchState, Metrics, SinkMessage, WorkItem};
use anyhow::{Context, Result};
use moraine_config::AppConfig;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::debug;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

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
                            if !work.path.ends_with(".jsonl") {
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
    if !work.path.ends_with(".jsonl") {
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

pub(crate) async fn process_file(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let source_file = &work.path;

    let meta = match std::fs::metadata(source_file) {
        Ok(meta) => meta,
        Err(exc) => {
            debug!("metadata missing for {}: {}", source_file, exc);
            return Ok(());
        }
    };

    #[cfg(unix)]
    let inode = meta.ino();
    #[cfg(not(unix))]
    let inode = 0u64;

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
                    "provider": work.provider,
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
                    "provider": work.provider,
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

        let normalized = match normalize_record(
            &parsed,
            &work.source_name,
            &work.provider,
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
                    "provider": work.provider,
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

fn truncate(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::complete_work;
    use crate::{DispatchState, WorkItem};
    use std::sync::{Arc, Mutex};

    fn sample_work(path: &str) -> WorkItem {
        WorkItem {
            source_name: "test-source".to_string(),
            provider: "test-provider".to_string(),
            path: path.to_string(),
        }
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
}
