mod checkpoint;
mod dispatch;
mod heartbeat;
pub mod model;
pub mod normalize;
mod reconcile;
mod sink;
mod watch;

use crate::dispatch::{complete_work, enqueue_work, process_file, spawn_debounce_task};
use crate::model::RowBatch;
use crate::reconcile::spawn_reconcile_task;
use crate::sink::spawn_sink_task;
use crate::watch::{enumerate_jsonl_files, spawn_watcher_threads};
use anyhow::{Context, Result};
use cortex_clickhouse::ClickHouseClient;
use cortex_config::{AppConfig, IngestSource};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{error, info};

pub(crate) const WATCHER_BACKEND_UNKNOWN: u64 = 0;
pub(crate) const WATCHER_BACKEND_NATIVE: u64 = 1;
pub(crate) const WATCHER_BACKEND_POLL: u64 = 2;
pub(crate) const WATCHER_BACKEND_MIXED: u64 = 3;

#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    pub(crate) source_name: String,
    pub(crate) provider: String,
    pub(crate) path: String,
}

impl WorkItem {
    pub(crate) fn key(&self) -> String {
        format!("{}\n{}", self.source_name, self.path)
    }
}

#[derive(Default)]
pub(crate) struct DispatchState {
    pub(crate) pending: HashSet<String>,
    pub(crate) inflight: HashSet<String>,
    pub(crate) dirty: HashSet<String>,
    pub(crate) item_by_key: HashMap<String, WorkItem>,
}

#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) raw_rows_written: AtomicU64,
    pub(crate) event_rows_written: AtomicU64,
    pub(crate) err_rows_written: AtomicU64,
    pub(crate) last_flush_ms: AtomicU64,
    pub(crate) flush_failures: AtomicU64,
    pub(crate) queue_depth: AtomicU64,
    pub(crate) watcher_error_count: AtomicU64,
    pub(crate) watcher_reset_count: AtomicU64,
    pub(crate) watcher_last_reset_unix_ms: AtomicU64,
    pub(crate) watcher_backend_state: AtomicU64,
    pub(crate) last_error: Mutex<String>,
}

#[derive(Debug)]
pub(crate) enum SinkMessage {
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

    let checkpoint_map = load_checkpoints(&clickhouse)
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

                    let reschedule = complete_work(&key, &dispatch_worker);

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

    let watcher_threads =
        spawn_watcher_threads(enabled_sources.clone(), watch_path_tx, metrics.clone())?;

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

#[derive(Deserialize)]
struct CheckpointRow {
    source_name: String,
    source_file: String,
    source_inode: u64,
    source_generation: u32,
    last_offset: u64,
    last_line_no: u64,
    status: String,
}

async fn load_checkpoints(
    clickhouse: &ClickHouseClient,
) -> Result<HashMap<String, model::Checkpoint>> {
    let query = format!(
        "SELECT \
            source_name, \
            source_file, \
            toUInt64(argMax(source_inode, updated_at)) AS source_inode, \
            toUInt32(argMax(source_generation, updated_at)) AS source_generation, \
            toUInt64(argMax(last_offset, updated_at)) AS last_offset, \
            toUInt64(argMax(last_line_no, updated_at)) AS last_line_no, \
            argMax(status, updated_at) AS status \
         FROM {}.ingest_checkpoints \
         GROUP BY source_name, source_file",
        clickhouse.config().database
    );

    let rows: Vec<CheckpointRow> = clickhouse.query_rows(&query, None).await?;
    let mut map = HashMap::<String, model::Checkpoint>::new();

    for row in rows {
        map.insert(
            format!("{}\n{}", row.source_name, row.source_file),
            model::Checkpoint {
                source_name: row.source_name,
                source_file: row.source_file,
                source_inode: row.source_inode,
                source_generation: row.source_generation.max(1),
                last_offset: row.last_offset,
                last_line_no: row.last_line_no,
                status: row.status,
            },
        );
    }

    Ok(map)
}
