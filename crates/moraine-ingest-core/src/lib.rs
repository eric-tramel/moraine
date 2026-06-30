mod checkpoint;
mod dispatch;
mod heartbeat;
pub mod model;
pub mod normalize;
mod reconcile;
mod redaction;
mod sink;
mod sources;
pub mod sqlite_poll;
mod tee;
mod watch;

use crate::checkpoint::checkpoint_key;
use crate::dispatch::{enqueue_work, run_work_item, spawn_debounce_task};
use crate::model::RowBatch;
use crate::reconcile::spawn_reconcile_task;
use crate::redaction::{RedactionAudit, SecretRedactor};
use crate::sink::{spawn_sink_task, SinkRole};
use crate::tee::{
    spawn_tee_router, RedactionContext, RouteResolver, SharedRouteResolver, StatusRegistry,
};
use crate::watch::{enumerate_tracked_files, spawn_watcher_threads};
use anyhow::{Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::{AppConfig, IngestSource, DEFAULT_BACKEND_NAME};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{info, warn};

pub(crate) const WATCHER_BACKEND_UNKNOWN: u64 = 0;
pub(crate) const WATCHER_BACKEND_NATIVE: u64 = 1;
pub(crate) const WATCHER_BACKEND_POLL: u64 = 2;
pub(crate) const WATCHER_BACKEND_MIXED: u64 = 3;

#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    pub(crate) source_name: String,
    pub(crate) harness: String,
    pub(crate) format: String,
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
    pub(crate) append_to_visible_p50_ms: AtomicU64,
    pub(crate) append_to_visible_p95_ms: AtomicU64,
    pub(crate) flush_failures: AtomicU64,
    pub(crate) queue_depth: AtomicU64,
    pub(crate) watcher_registrations: AtomicU64,
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

    let checkpoint_cursor_columns = checkpoint_cursor_columns_available(&clickhouse)
        .await
        .context("failed to inspect ingest_checkpoints columns")?;
    if !checkpoint_cursor_columns {
        warn!(
            "ingest_checkpoints is missing the SQLite cursor columns (migration 015); \
             SQLite poll cursors will not persist until `moraine db migrate` runs \
             AND this ingest service restarts (the column probe is startup-only) — \
             until then every restart re-scans Cursor/OpenCode databases from scratch and \
             re-appends their rows to raw_events"
        );
    }

    let checkpoint_map = load_checkpoints(&clickhouse, checkpoint_cursor_columns, None)
        .await
        .context("failed to load checkpoints from clickhouse")?;

    info!(
        "loaded {} checkpoints across {} sources",
        checkpoint_map.len(),
        enabled_sources.len()
    );

    let has_named_backends = config
        .backends
        .keys()
        .any(|name| name != DEFAULT_BACKEND_NAME);
    let heartbeat_backend_column = if has_named_backends {
        let available = heartbeat_column_available(&clickhouse, "backend_sinks")
            .await
            .context("failed to inspect ingest_heartbeats columns")?;
        if !available {
            warn!(
                "ingest_heartbeats is missing the backend_sinks column (migration 017); \
                 per-backend mirror sink status will not appear in heartbeats until \
                 `moraine db migrate` runs and this ingest service restarts"
            );
        }
        available
    } else {
        false
    };
    let heartbeat_redactions_column = heartbeat_column_available(&clickhouse, "redactions_total")
        .await
        .context("failed to inspect ingest_heartbeats redaction columns")?;
    if !heartbeat_redactions_column {
        warn!(
            "ingest_heartbeats is missing the redactions_total column (migration 022); \
             redaction audit counts will not appear in heartbeats until \
             `moraine db migrate` runs and this ingest service restarts"
        );
    }

    let checkpoints = Arc::new(RwLock::new(checkpoint_map));
    let dispatch = Arc::new(Mutex::new(DispatchState::default()));
    let metrics = Arc::new(Metrics::default());
    let redactor = Arc::new(
        SecretRedactor::new(&config.redaction).context("failed to initialize secret redaction")?,
    );
    let redaction_audit = Arc::new(RedactionAudit::default());
    if config.redaction.dangerously_skip_secret_redaction_ignored {
        warn!(
            "ignoring redaction.dangerously_skip_secret_redaction from non-home config; only \
             $HOME/.moraine/config.toml may disable local secret redaction"
        );
    }
    if config.redaction.dangerously_skip_secret_redaction {
        warn!(
            "local/default secret redaction disabled by home config; mirror backend egress remains redacted"
        );
    }

    let process_queue_capacity = config
        .ingest
        .max_inflight_batches
        .saturating_mul(16)
        .max(1024);
    let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(process_queue_capacity);
    let (sink_tx, sink_rx) =
        mpsc::channel::<SinkMessage>(config.ingest.max_inflight_batches.max(1));
    let (watch_path_tx, watch_path_rx) = mpsc::unbounded_channel::<WorkItem>();

    // The tee router sits between the processors and the default sink: the
    // default sink still receives everything (same backpressure as before,
    // one channel hop later), while sessions routed to named backends are
    // additionally mirrored to per-backend sinks that can never stall it.
    let (default_sink_tx, default_sink_rx) =
        mpsc::channel::<SinkMessage>(config.ingest.max_inflight_batches.max(1));
    let shared_config = Arc::new(config.clone());
    let resolver: SharedRouteResolver =
        Arc::new(Mutex::new(RouteResolver::new(shared_config.clone())));
    let backend_statuses: StatusRegistry = Arc::new(Mutex::new(BTreeMap::new()));

    let sink_handle = spawn_sink_task(
        config.clone(),
        clickhouse.clone(),
        checkpoints.clone(),
        metrics.clone(),
        default_sink_rx,
        checkpoint_cursor_columns,
        SinkRole::Default {
            dispatch: dispatch.clone(),
            backends: backend_statuses.clone(),
            backend_sinks_column: heartbeat_backend_column,
            redactions_column: heartbeat_redactions_column,
            redactions: redaction_audit.clone(),
        },
    );

    let router_handle = spawn_tee_router(
        shared_config,
        enabled_sources.clone(),
        sink_rx,
        default_sink_tx,
        resolver,
        backend_statuses,
        RedactionContext::new(redactor, redaction_audit),
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

                tokio::spawn(run_work_item(
                    cfg_clone.clone(),
                    work,
                    permit,
                    checkpoints_clone.clone(),
                    sink_tx_clone.clone(),
                    process_tx_clone.clone(),
                    dispatch_clone.clone(),
                    metrics_clone.clone(),
                ));
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
        let mut backfill_sources = Vec::new();
        for source in &enabled_sources {
            let files = enumerate_tracked_files(&source.glob, &source.format)?;
            info!(
                "startup backfill queueing {} files for source={} (format={})",
                files.len(),
                source.name,
                source.format
            );
            backfill_sources.push((source.clone(), files.into_iter()));
        }

        loop {
            let mut queued_any = false;
            for (source, files) in &mut backfill_sources {
                if let Some(path) = files.next() {
                    queued_any = true;
                    enqueue_work(
                        WorkItem {
                            source_name: source.name.clone(),
                            harness: source.harness.clone(),
                            format: source.format.clone(),
                            path,
                        },
                        &process_tx,
                        &dispatch,
                        &metrics,
                    )
                    .await;
                }
            }

            if !queued_any {
                break;
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
    router_handle.abort();
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
    #[serde(default)]
    cursor_json: String,
    #[serde(default)]
    source_fingerprint: u64,
    #[serde(default)]
    schema_fingerprint: u64,
}

/// Returns whether `ingest_checkpoints` carries the SQLite cursor columns
/// added by migration 015. Selecting them unconditionally would abort startup
/// for every source on a database that has not run `moraine db migrate` yet.
async fn checkpoint_cursor_columns_available(clickhouse: &ClickHouseClient) -> Result<bool> {
    #[derive(Deserialize)]
    struct CountRow {
        column_count: u64,
    }

    let query = format!(
        "SELECT toUInt64(count()) AS column_count \
         FROM system.columns \
         WHERE database = '{}' AND table = 'ingest_checkpoints' AND name = 'cursor_json'",
        clickhouse.config().database.replace('\'', "\\'")
    );
    let rows: Vec<CountRow> = clickhouse.query_rows(&query, None).await?;
    Ok(rows
        .first()
        .map(|row| row.column_count > 0)
        .unwrap_or(false))
}

/// Returns whether `ingest_heartbeats` carries an optional heartbeat column.
/// Inserting optional columns unconditionally would break every heartbeat on a
/// database that has not run the corresponding `moraine db migrate` yet.
async fn heartbeat_column_available(clickhouse: &ClickHouseClient, column: &str) -> Result<bool> {
    #[derive(Deserialize)]
    struct CountRow {
        column_count: u64,
    }

    let query = format!(
        "SELECT toUInt64(count()) AS column_count \
         FROM system.columns \
         WHERE database = '{}' AND table = 'ingest_heartbeats' AND name = '{}'",
        clickhouse.config().database.replace('\'', "\\'"),
        column.replace('\'', "\\'")
    );
    let rows: Vec<CountRow> = clickhouse.query_rows(&query, None).await?;
    Ok(rows
        .first()
        .map(|row| row.column_count > 0)
        .unwrap_or(false))
}

/// Builds the checkpoint-load query. `host` scopes the load to one host's
/// rows: backend mirror sinks share an `ingest_checkpoints` table per team
/// backend (host column added by migration 018), and loading another host's
/// rows would adopt offsets/generations for files this host never wrote.
/// The default backend passes `None` — it is single-writer, its rows carry
/// no host, and the column may not exist before `moraine db migrate` runs.
fn checkpoints_query(database: &str, cursor_columns: bool, host: Option<&str>) -> String {
    let cursor_column_selects = if cursor_columns {
        ", argMax(cursor_json, updated_at) AS cursor_json \
         , toUInt64(argMax(source_fingerprint, updated_at)) AS source_fingerprint \
         , toUInt64(argMax(schema_fingerprint, updated_at)) AS schema_fingerprint"
    } else {
        ", '' AS cursor_json \
         , toUInt64(0) AS source_fingerprint \
         , toUInt64(0) AS schema_fingerprint"
    };

    let host_filter = match host {
        Some(host) => format!(
            " WHERE host = '{}'",
            host.replace('\\', "\\\\").replace('\'', "\\'")
        ),
        None => String::new(),
    };

    format!(
        "SELECT \
            source_name, \
            source_file, \
            toUInt64(argMax(source_inode, updated_at)) AS source_inode, \
            toUInt32(argMax(source_generation, updated_at)) AS source_generation, \
            toUInt64(argMax(last_offset, updated_at)) AS last_offset, \
            toUInt64(argMax(last_line_no, updated_at)) AS last_line_no, \
            argMax(status, updated_at) AS status \
            {} \
         FROM {}.ingest_checkpoints{} \
         GROUP BY source_name, source_file",
        cursor_column_selects, database, host_filter
    )
}

async fn load_checkpoints(
    clickhouse: &ClickHouseClient,
    cursor_columns: bool,
    host: Option<&str>,
) -> Result<HashMap<String, model::Checkpoint>> {
    let query = checkpoints_query(&clickhouse.config().database, cursor_columns, host);
    let rows: Vec<CheckpointRow> = clickhouse.query_rows(&query, None).await?;
    let mut map = HashMap::<String, model::Checkpoint>::new();

    for row in rows {
        let key = checkpoint_key(&row.source_name, &row.source_file);
        map.insert(
            key,
            model::Checkpoint {
                source_name: row.source_name,
                source_file: row.source_file,
                source_inode: row.source_inode,
                source_generation: row.source_generation.max(1),
                last_offset: row.last_offset,
                last_line_no: row.last_line_no,
                status: row.status,
                cursor_json: row.cursor_json,
                source_fingerprint: row.source_fingerprint,
                schema_fingerprint: row.schema_fingerprint,
            },
        );
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::checkpoints_query;

    #[test]
    fn checkpoints_query_without_host_keeps_legacy_shape() {
        let query = checkpoints_query("moraine", true, None);
        assert!(
            !query.contains("host"),
            "the default backend must not reference the migration-018 column: {query}"
        );
        assert!(query.contains("FROM moraine.ingest_checkpoints "));
    }

    #[test]
    fn checkpoints_query_scopes_backend_loads_to_one_host() {
        let query = checkpoints_query("moraine_team", true, Some("dev-box"));
        assert!(
            query.contains("WHERE host = 'dev-box'"),
            "backend loads must never adopt another team member's checkpoints: {query}"
        );
    }

    #[test]
    fn checkpoints_query_escapes_hostile_host_names() {
        let query = checkpoints_query("moraine_team", false, Some("a'b\\c"));
        assert!(query.contains("WHERE host = 'a\\'b\\\\c'"));
    }
}
