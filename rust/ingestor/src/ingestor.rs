use crate::clickhouse::ClickHouseClient;
use crate::config::{AppConfig, IngestSource};
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use anyhow::{Context, Result};
use glob::glob;
use notify::{Event, RecursiveMode, Watcher};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[derive(Debug, Clone)]
struct WorkItem {
    source_name: String,
    provider: String,
    path: String,
}

impl WorkItem {
    fn key(&self) -> String {
        format!("{}\n{}", self.source_name, self.path)
    }
}

#[derive(Default)]
struct DispatchState {
    pending: HashSet<String>,
    inflight: HashSet<String>,
    dirty: HashSet<String>,
    item_by_key: HashMap<String, WorkItem>,
}

#[derive(Default)]
struct Metrics {
    raw_rows_written: AtomicU64,
    event_rows_written: AtomicU64,
    err_rows_written: AtomicU64,
    last_flush_ms: AtomicU64,
    flush_failures: AtomicU64,
    queue_depth: AtomicU64,
    last_error: Mutex<String>,
}

#[derive(Debug)]
enum SinkMessage {
    Batch(RowBatch),
}

pub async fn run_ingestor(config: AppConfig) -> Result<()> {
    let enabled_sources: Vec<IngestSource> = config
        .ingest
        .sources
        .iter()
        .filter(|src| src.enabled)
        .cloned()
        .collect();

    if enabled_sources.is_empty() {
        return Err(anyhow::anyhow!(
            "no enabled ingest sources found in config.ingest.sources"
        ));
    }

    let clickhouse = ClickHouseClient::new(config.clickhouse.clone())?;
    clickhouse.ping().await.context("clickhouse ping failed")?;

    let checkpoint_map = clickhouse
        .load_checkpoints()
        .await
        .context("failed to load checkpoints from clickhouse")?;

    info!(
        "loaded {} checkpoints across {} sources",
        checkpoint_map.len(),
        enabled_sources.len()
    );

    let checkpoints = Arc::new(RwLock::new(checkpoint_map));
    let dispatch = Arc::new(Mutex::new(DispatchState::default()));
    let metrics = Arc::new(Metrics::default());

    let process_queue_capacity = config
        .ingest
        .max_inflight_batches
        .saturating_mul(16)
        .max(1024);
    let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(process_queue_capacity);
    let (sink_tx, sink_rx) =
        mpsc::channel::<SinkMessage>(config.ingest.max_inflight_batches.max(16));
    let (watch_path_tx, watch_path_rx) = mpsc::unbounded_channel::<WorkItem>();

    let sink_handle = spawn_sink_task(
        config.clone(),
        clickhouse.clone(),
        checkpoints.clone(),
        metrics.clone(),
        sink_rx,
        dispatch.clone(),
    );

    let sem = Arc::new(Semaphore::new(config.ingest.max_file_workers.max(1)));
    let processor_handle = {
        let process_tx_clone = process_tx.clone();
        let sink_tx_clone = sink_tx.clone();
        let checkpoints_clone = checkpoints.clone();
        let dispatch_clone = dispatch.clone();
        let sem_clone = sem.clone();
        let metrics_clone = metrics.clone();
        let cfg_clone = config.clone();

        tokio::spawn(async move {
            while let Some(work) = process_rx.recv().await {
                metrics_clone.queue_depth.fetch_sub(1, Ordering::Relaxed);
                let key = work.key();

                {
                    let mut state = dispatch_clone.lock().expect("dispatch mutex poisoned");
                    state.pending.remove(&key);
                    state.inflight.insert(key.clone());
                }

                let permit = match sem_clone.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };

                let sink_tx_worker = sink_tx_clone.clone();
                let process_tx_worker = process_tx_clone.clone();
                let checkpoints_worker = checkpoints_clone.clone();
                let dispatch_worker = dispatch_clone.clone();
                let cfg_worker = cfg_clone.clone();
                let metrics_worker = metrics_clone.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(exc) = process_file(
                        &cfg_worker,
                        &work,
                        checkpoints_worker,
                        sink_tx_worker,
                        &metrics_worker,
                    )
                    .await
                    {
                        error!(
                            "failed processing {}:{}: {exc}",
                            work.source_name, work.path
                        );
                        *metrics_worker
                            .last_error
                            .lock()
                            .expect("metrics last_error mutex poisoned") = exc.to_string();
                    }

                    let mut reschedule: Option<WorkItem> = None;
                    {
                        let mut state = dispatch_worker.lock().expect("dispatch mutex poisoned");
                        state.inflight.remove(&key);
                        if state.dirty.remove(&key) {
                            if state.pending.insert(key.clone()) {
                                reschedule = state.item_by_key.get(&key).cloned();
                            }
                        }
                    }

                    if let Some(item) = reschedule {
                        if process_tx_worker.send(item).await.is_ok() {
                            metrics_worker.queue_depth.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        })
    };

    let debounce_handle = spawn_debounce_task(
        config.clone(),
        watch_path_rx,
        process_tx.clone(),
        dispatch.clone(),
        metrics.clone(),
    );

    let reconcile_handle = spawn_reconcile_task(
        config.clone(),
        enabled_sources.clone(),
        process_tx.clone(),
        dispatch.clone(),
        metrics.clone(),
    );

    let watcher_threads = spawn_watcher_threads(enabled_sources.clone(), watch_path_tx)?;

    if config.ingest.backfill_on_start {
        for source in &enabled_sources {
            let files = enumerate_jsonl_files(&source.glob)?;
            info!(
                "startup backfill queueing {} files for source={}",
                files.len(),
                source.name
            );
            for path in files {
                enqueue_work(
                    WorkItem {
                        source_name: source.name.clone(),
                        provider: source.provider.clone(),
                        path,
                    },
                    &process_tx,
                    &dispatch,
                    &metrics,
                )
                .await;
            }
        }
    }

    info!("rust ingestor running; waiting for shutdown signal");
    tokio::signal::ctrl_c()
        .await
        .context("signal handler failed")?;
    info!("shutdown signal received");

    drop(process_tx);
    drop(sink_tx);

    debounce_handle.abort();
    reconcile_handle.abort();
    processor_handle.abort();
    sink_handle.abort();

    for handle in watcher_threads {
        let _ = handle.thread().id();
    }

    Ok(())
}

fn spawn_watcher_threads(
    sources: Vec<IngestSource>,
    tx: mpsc::UnboundedSender<WorkItem>,
) -> Result<Vec<std::thread::JoinHandle<()>>> {
    let mut handles = Vec::<std::thread::JoinHandle<()>>::new();

    for source in sources {
        let source_name = source.name.clone();
        let provider = source.provider.clone();
        let watch_root = std::path::PathBuf::from(source.watch_root.clone());
        let tx_clone = tx.clone();

        info!(
            "starting watcher on {} (source={}, provider={})",
            watch_root.display(),
            source_name,
            provider
        );

        let handle = std::thread::spawn(move || {
            let (event_tx, event_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();

            let mut watcher = match notify::recommended_watcher(move |res| {
                let _ = event_tx.send(res);
            }) {
                Ok(watcher) => watcher,
                Err(exc) => {
                    eprintln!(
                        "[moraine-rust] failed to create watcher for {}: {exc}",
                        source_name
                    );
                    return;
                }
            };

            if let Err(exc) = watcher.watch(watch_root.as_path(), RecursiveMode::Recursive) {
                eprintln!(
                    "[moraine-rust] failed to watch {} ({}): {exc}",
                    watch_root.display(),
                    source_name
                );
                return;
            }

            loop {
                match event_rx.recv() {
                    Ok(Ok(event)) => {
                        for path in event.paths {
                            let _ = tx_clone.send(WorkItem {
                                source_name: source_name.clone(),
                                provider: provider.clone(),
                                path: path.to_string_lossy().to_string(),
                            });
                        }
                    }
                    Ok(Err(exc)) => {
                        eprintln!("[moraine-rust] watcher event error ({source_name}): {exc}");
                    }
                    Err(_) => break,
                }
            }
        });

        handles.push(handle);
    }

    Ok(handles)
}

fn spawn_debounce_task(
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

fn spawn_reconcile_task(
    config: AppConfig,
    sources: Vec<IngestSource>,
    process_tx: mpsc::Sender<WorkItem>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs_f64(config.ingest.reconcile_interval_seconds.max(5.0));
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;
            for source in &sources {
                match enumerate_jsonl_files(&source.glob) {
                    Ok(paths) => {
                        debug!(
                            "reconcile scanning {} files for source={}",
                            paths.len(),
                            source.name
                        );
                        for path in paths {
                            enqueue_work(
                                WorkItem {
                                    source_name: source.name.clone(),
                                    provider: source.provider.clone(),
                                    path,
                                },
                                &process_tx,
                                &dispatch,
                                &metrics,
                            )
                            .await;
                        }
                    }
                    Err(exc) => {
                        warn!("reconcile scan failed for source={}: {exc}", source.name);
                    }
                }
            }
        }
    })
}

fn spawn_sink_task(
    config: AppConfig,
    clickhouse: ClickHouseClient,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<SinkMessage>,
    dispatch: Arc<Mutex<DispatchState>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut raw_rows = Vec::<Value>::new();
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut tool_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::<String, Checkpoint>::new();

        let flush_interval =
            Duration::from_secs_f64(config.ingest.flush_interval_seconds.max(0.05));
        let heartbeat_interval =
            Duration::from_secs_f64(config.ingest.heartbeat_interval_seconds.max(1.0));

        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);

        loop {
            tokio::select! {
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some(SinkMessage::Batch(batch)) => {
                            raw_rows.extend(batch.raw_rows);
                            event_rows.extend(batch.event_rows);
                            link_rows.extend(batch.link_rows);
                            tool_rows.extend(batch.tool_rows);
                            error_rows.extend(batch.error_rows);
                            if let Some(cp) = batch.checkpoint {
                                merge_checkpoint(&mut checkpoint_updates, cp);
                            }

                            let total_rows = raw_rows.len() + event_rows.len() + link_rows.len() + tool_rows.len() + error_rows.len();
                            if total_rows >= config.ingest.batch_size {
                                flush_pending(
                                    &clickhouse,
                                    &checkpoints,
                                    &metrics,
                                    &mut raw_rows,
                                    &mut event_rows,
                                    &mut link_rows,
                                    &mut tool_rows,
                                    &mut error_rows,
                                    &mut checkpoint_updates,
                                ).await;
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if !(raw_rows.is_empty() && event_rows.is_empty() && link_rows.is_empty() && tool_rows.is_empty() && error_rows.is_empty() && checkpoint_updates.is_empty()) {
                        flush_pending(
                            &clickhouse,
                            &checkpoints,
                            &metrics,
                            &mut raw_rows,
                            &mut event_rows,
                            &mut link_rows,
                            &mut tool_rows,
                            &mut error_rows,
                            &mut checkpoint_updates,
                        ).await;
                    }
                }
                _ = heartbeat_tick.tick() => {
                    let files_active = {
                        let state = dispatch.lock().expect("dispatch mutex poisoned");
                        state.inflight.len() as u32
                    };
                    let files_watched = checkpoints.read().await.len() as u32;
                    let last_error = {
                        metrics
                            .last_error
                            .lock()
                            .expect("metrics last_error mutex poisoned")
                            .clone()
                    };

                    let heartbeat = json!({
                        "host": host_name(),
                        "service_version": env!("CARGO_PKG_VERSION"),
                        "queue_depth": metrics.queue_depth.load(Ordering::Relaxed),
                        "files_active": files_active,
                        "files_watched": files_watched,
                        "rows_raw_written": metrics.raw_rows_written.load(Ordering::Relaxed),
                        "rows_events_written": metrics.event_rows_written.load(Ordering::Relaxed),
                        "rows_errors_written": metrics.err_rows_written.load(Ordering::Relaxed),
                        "flush_latency_ms": metrics.last_flush_ms.load(Ordering::Relaxed) as u32,
                        "append_to_visible_p50_ms": 0u32,
                        "append_to_visible_p95_ms": 0u32,
                        "last_error": last_error,
                    });

                    if let Err(exc) = clickhouse.insert_json_rows("ingest_heartbeats", &[heartbeat]).await {
                        warn!("heartbeat insert failed: {exc}");
                    }
                }
            }
        }

        if !(raw_rows.is_empty()
            && event_rows.is_empty()
            && link_rows.is_empty()
            && tool_rows.is_empty()
            && error_rows.is_empty()
            && checkpoint_updates.is_empty())
        {
            flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut tool_rows,
                &mut error_rows,
                &mut checkpoint_updates,
            )
            .await;
        }
    })
}

fn checkpoint_key(source_name: &str, source_file: &str) -> String {
    format!("{}\n{}", source_name, source_file)
}

fn merge_checkpoint(pending: &mut HashMap<String, Checkpoint>, checkpoint: Checkpoint) {
    let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
    match pending.get(&key) {
        None => {
            pending.insert(key, checkpoint);
        }
        Some(existing) => {
            let replace = checkpoint.source_generation > existing.source_generation
                || (checkpoint.source_generation == existing.source_generation
                    && checkpoint.last_offset >= existing.last_offset);
            if replace {
                pending.insert(key, checkpoint);
            }
        }
    }
}

async fn flush_pending(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    raw_rows: &mut Vec<Value>,
    event_rows: &mut Vec<Value>,
    link_rows: &mut Vec<Value>,
    tool_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
) {
    let started = Instant::now();

    let checkpoint_rows: Vec<Value> = checkpoint_updates
        .values()
        .map(|cp| {
            json!({
                "source_name": cp.source_name,
                "source_file": cp.source_file,
                "source_inode": cp.source_inode,
                "source_generation": cp.source_generation,
                "last_offset": cp.last_offset,
                "last_line_no": cp.last_line_no,
                "status": cp.status,
            })
        })
        .collect();

    let flush_result = async {
        clickhouse.insert_json_rows("raw_events", raw_rows).await?;
        clickhouse.insert_json_rows("events", event_rows).await?;
        clickhouse.insert_json_rows("event_links", link_rows).await?;
        clickhouse.insert_json_rows("tool_io", tool_rows).await?;
        clickhouse.insert_json_rows("ingest_errors", error_rows).await?;
        clickhouse
            .insert_json_rows("ingest_checkpoints", &checkpoint_rows)
            .await?;
        Result::<()>::Ok(())
    }
    .await;

    match flush_result {
        Ok(()) => {
            metrics
                .raw_rows_written
                .fetch_add(raw_rows.len() as u64, Ordering::Relaxed);
            metrics
                .event_rows_written
                .fetch_add(event_rows.len() as u64, Ordering::Relaxed);
            metrics
                .err_rows_written
                .fetch_add(error_rows.len() as u64, Ordering::Relaxed);
            metrics
                .last_flush_ms
                .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);

            {
                let mut state = checkpoints.write().await;
                for cp in checkpoint_updates.values() {
                    let key = checkpoint_key(&cp.source_name, &cp.source_file);
                    state.insert(key, cp.clone());
                }
            }

            raw_rows.clear();
            event_rows.clear();
            link_rows.clear();
            tool_rows.clear();
            error_rows.clear();
            checkpoint_updates.clear();
        }
        Err(exc) => {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = exc.to_string();
            warn!("flush failed: {exc}");
        }
    }
}

async fn enqueue_work(
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

async fn process_file(
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

        let normalized = normalize_record(
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
        );

        session_hint = normalized.session_hint;
        model_hint = normalized.model_hint;
        batch.raw_rows.push(normalized.raw_row);
        batch.event_rows.extend(normalized.event_rows);
        batch.link_rows.extend(normalized.link_rows);
        batch.tool_rows.extend(normalized.tool_rows);
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

fn enumerate_jsonl_files(glob_pattern: &str) -> Result<Vec<String>> {
    let mut files = Vec::<String>::new();
    for entry in glob(glob_pattern).with_context(|| format!("invalid glob: {}", glob_pattern))? {
        let path = match entry {
            Ok(path) => path,
            Err(exc) => {
                warn!("glob iteration error: {exc}");
                continue;
            }
        };

        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            files.push(path.to_string_lossy().to_string());
        }
    }
    files.sort();
    Ok(files)
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "localhost".to_string())
}
