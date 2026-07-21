mod checkpoint;
mod dispatch;
mod heartbeat;
pub mod model;
pub mod normalize;
pub(crate) mod publication;
mod publication_identity;
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
pub(crate) use crate::publication::{
    CheckpointTransition, FinalizeReplayOutcome, ReplayBarrierAck,
};
use crate::publication::{PublicationActor, PublicationOwnerLock};
use crate::publication_identity::{PublicationIdentity, PUBLICATION_HOST_ID_FILE_NAME};
use crate::reconcile::spawn_reconcile_task;
use crate::redaction::{RedactionAudit, SecretRedactor};
use crate::sink::{spawn_sink_task, SinkAuthorConfig, SinkRole};
use crate::tee::{
    spawn_tee_router, RedactionContext, RouteResolver, SharedRouteResolver, StatusRegistry,
};
use crate::watch::{enumerate_tracked_files, spawn_watcher_threads};
use anyhow::{Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::{
    AppConfig, ClickHouseConfig, IngestSource, SourceFormat, DEFAULT_BACKEND_NAME,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, RwLock, Semaphore};
use tracing::{info, warn};

pub(crate) const WATCHER_BACKEND_UNKNOWN: u64 = 0;
pub(crate) const WATCHER_BACKEND_NATIVE: u64 = 1;
pub(crate) const WATCHER_BACKEND_POLL: u64 = 2;
pub(crate) const WATCHER_BACKEND_MIXED: u64 = 3;

#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    pub(crate) source_name: String,
    pub(crate) harness: String,
    pub(crate) format: SourceFormat,
    pub(crate) source_glob: String,
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
    /// A durable replay-start barrier. The sink flushes all earlier batches,
    /// persists the causal replay transition synchronously, then acknowledges.
    BeginReplay {
        transition: CheckpointTransition,
        ack: oneshot::Sender<std::result::Result<ReplayBarrierAck, String>>,
    },
    /// Final source-boundary validation and publication. The head switch is
    /// the last durable operation before acknowledgement.
    FinalizeReplay {
        transition: CheckpointTransition,
        ack: oneshot::Sender<std::result::Result<FinalizeReplayOutcome, String>>,
    },
    /// Persist a fail-closed replay transition without changing source heads.
    BlockReplay {
        transition: CheckpointTransition,
        ack: oneshot::Sender<std::result::Result<ReplayBarrierAck, String>>,
    },
    /// Persist mirror catch-up readiness through the ordered sink channel.
    MirrorCaughtUp {
        transition: CheckpointTransition,
        ack: oneshot::Sender<std::result::Result<ReplayBarrierAck, String>>,
    },
}

fn build_ingest_clickhouse_client(config: ClickHouseConfig) -> Result<ClickHouseClient> {
    let user_agent = format!(
        "moraine-ingest/{} (pid={})",
        env!("CARGO_PKG_VERSION"),
        std::process::id()
    );
    ClickHouseClient::new_with_user_agent(config, user_agent)
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

    let has_named_backends = config
        .backends
        .keys()
        .any(|name| name != DEFAULT_BACKEND_NAME);
    // Config loading expands `ingest.state_dir` before the runtime receives
    // it. Establish one identity before the owner lock or any sink/backend
    // task, then thread this exact value through every writer.
    let publication_identity = PublicationIdentity::load_or_create(&config.ingest.state_dir)
        .context("failed to establish durable publication host identity")?;
    if publication_identity.was_created() {
        if has_named_backends {
            warn!(
                publication_host_id = publication_identity.host_id(),
                identity_file = %std::path::Path::new(&config.ingest.state_dir)
                    .join(PUBLICATION_HOST_ID_FILE_NAME)
                    .display(),
                "created durable publication host identity; legacy shared-backend checkpoint/source host keys derived from HOSTNAME or USER are deliberately not adopted, remain separate, and may require backend cleanup before mirror catch-up replays tracked sources under the new identity"
            );
        } else {
            info!(
                publication_host_id = publication_identity.host_id(),
                "created durable publication host identity"
            );
        }
    }

    let clickhouse = build_ingest_clickhouse_client(config.clickhouse.clone())?;
    clickhouse.ping().await.context("clickhouse ping failed")?;

    // The advisory lock is acquired before any sink can write. It rejects a
    // duplicate local publisher instead of allowing two processes to race
    // `max(revision) + 1` for the same host identity. The diagnostic is
    // durable so monitor/doctor can explain the fail-closed startup.
    let _publication_owner = match PublicationOwnerLock::acquire(
        &config.ingest.state_dir,
        publication_identity.publisher_id(),
    ) {
        Ok(owner) => {
            if let Err(error) = record_publication_writer_conflict(&clickhouse, false, "").await {
                warn!("failed to clear publication writer-conflict diagnostic: {error}");
            }
            owner
        }
        Err(lock_error) => {
            let detail = "another local ingest process owns this publication identity";
            if let Err(error) = record_publication_writer_conflict(&clickhouse, true, detail).await
            {
                warn!("failed to record publication writer-conflict diagnostic: {error}");
            }
            return Err(lock_error).context("failed to acquire publication ownership");
        }
    };

    for (table, column) in [
        ("published_source_generations", "publication_revision"),
        ("ingest_checkpoint_transitions", "checkpoint_revision"),
        (
            "source_generation_publication_readiness",
            "readiness_revision",
        ),
        ("ingest_append_control", "control_revision"),
        ("publication_diagnostic_events", "diagnostic_revision"),
        ("mcp_open_publication_headers", "candidate_publication_id"),
        ("mcp_open_generation_readiness", "candidate_publication_id"),
        ("raw_events", "source_host"),
        ("events", "source_host"),
        ("event_links", "source_host"),
        ("tool_io", "source_host"),
        ("ingest_errors", "source_host"),
    ] {
        if !table_column_available(&clickhouse, table, column)
            .await
            .with_context(|| {
                format!("failed to inspect required publication column {table}.{column}")
            })?
        {
            return Err(anyhow::anyhow!(
                "publication-aware ingest requires {table}.{column}; run `moraine db migrate` before restarting ingest"
            ));
        }
    }

    let startup_publication = PublicationActor::new(
        clickhouse.clone(),
        String::new(),
        publication_identity.publisher_id().to_string(),
    );
    if startup_publication
        .block_orphaned_append_on_startup()
        .await
        .context("failed to repair append publication control")?
    {
        warn!(
            "an unresolved append manifest remains blocked after restart; affected strict reads stay fail-closed"
        );
    }

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

    let raw_events_author_column = table_column_available(&clickhouse, "raw_events", "author")
        .await
        .context("failed to inspect raw_events author column")?;
    let events_author_column = table_column_available(&clickhouse, "events", "author")
        .await
        .context("failed to inspect events author column")?;
    if !raw_events_author_column || !events_author_column {
        warn!(
            raw_events_author_column,
            events_author_column,
            "raw_events/events are missing the author column (migration 024); \
             author attribution will be omitted for missing local/default columns until \
             `moraine db migrate` runs and this ingest service restarts"
        );
    }
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
        publication_identity.clone(),
        SinkAuthorConfig::new(
            config.identity.author.clone(),
            raw_events_author_column,
            events_author_column,
        ),
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
        publication_identity,
    );

    let sem = Arc::new(Semaphore::new(config.ingest.max_file_workers.max(1)));
    // Per-pipeline volatile sqlite poll state (issue #443): shared by every
    // work item, dropped with the pipeline so restarts start from durable
    // state only.
    let sqlite_poll_state = crate::sqlite_poll::VolatilePollMap::new();
    let processor_handle = {
        let process_tx_clone = process_tx.clone();
        let sink_tx_clone = sink_tx.clone();
        let checkpoints_clone = checkpoints.clone();
        let poll_state_clone = sqlite_poll_state.clone();
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
                    poll_state_clone.clone(),
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
            let files = enumerate_tracked_files(&source.glob, source.format)?;
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
                            format: source.format,
                            source_glob: source.glob.clone(),
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

async fn record_publication_writer_conflict(
    clickhouse: &ClickHouseClient,
    active: bool,
    detail: &str,
) -> Result<()> {
    let revision = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    clickhouse
        .insert_json_rows_sync(
            "publication_diagnostic_events",
            &[serde_json::json!({
                "source_host": "",
                "source_name": "",
                "source_file": "",
                "diagnostic_kind": "writer_conflict",
                "diagnostic_revision": revision,
                "detail": detail,
                "active": u8::from(active),
            })],
        )
        .await
        .context("failed to write publication writer-conflict diagnostic")
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

#[derive(Deserialize)]
struct CausalCheckpointRow {
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
    checkpoint_revision: u64,
    operation_id: String,
    #[serde(default)]
    scan_inode: u64,
    #[serde(default)]
    scan_boundary: u64,
    #[serde(default)]
    policy_fingerprint: String,
    #[serde(default)]
    final_scan_complete: u8,
    #[serde(default)]
    block_reason: String,
    #[serde(default)]
    compatibility_prepared: u8,
    #[serde(default)]
    backend_caught_up: u8,
    #[serde(default)]
    append_batch_id: String,
    #[serde(default)]
    cache_epoch: u64,
}

/// Returns whether `ingest_checkpoints` carries the SQLite cursor columns
/// added by migration 015. Selecting them unconditionally would abort startup
/// for every source on a database that has not run `moraine db migrate` yet.
async fn checkpoint_cursor_columns_available(clickhouse: &ClickHouseClient) -> Result<bool> {
    table_column_available(clickhouse, "ingest_checkpoints", "cursor_json").await
}

/// Returns whether `ingest_heartbeats` carries an optional heartbeat column.
/// Inserting optional columns unconditionally would break every heartbeat on a
/// database that has not run the corresponding `moraine db migrate` yet.
async fn heartbeat_column_available(clickhouse: &ClickHouseClient, column: &str) -> Result<bool> {
    table_column_available(clickhouse, "ingest_heartbeats", column).await
}

/// Returns whether a table carries a column. Optional-write paths use this at
/// startup because ClickHouse JSONEachRow rejects unknown fields at insert time.
async fn table_column_available(
    clickhouse: &ClickHouseClient,
    table: &str,
    column: &str,
) -> Result<bool> {
    #[derive(Deserialize)]
    struct CountRow {
        column_count: u64,
    }

    let query = format!(
        "SELECT toUInt64(count()) AS column_count \
         FROM system.columns \
         WHERE database = '{}' AND table = '{}' AND name = '{}'",
        clickhouse.config().database.replace('\'', "\\'"),
        table.replace('\'', "\\'"),
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

/// Loads one indivisible causal checkpoint tuple. Tuple-valued `argMax`
/// avoids manufacturing a state from independently selected equal-timestamp
/// legacy columns.
fn causal_checkpoints_query(database: &str, host: Option<&str>) -> String {
    let host = host
        .unwrap_or_default()
        .replace('\\', "\\\\")
        .replace('\'', "\\'");
    format!(
        "SELECT \
            source_name, source_file, \
            toUInt64(tupleElement(current, 1)) AS source_inode, \
            toUInt32(tupleElement(current, 2)) AS source_generation, \
            toUInt64(tupleElement(current, 3)) AS last_offset, \
            toUInt64(tupleElement(current, 4)) AS last_line_no, \
            tupleElement(current, 5) AS status, \
            tupleElement(current, 6) AS cursor_json, \
            toUInt64(tupleElement(current, 7)) AS source_fingerprint, \
            toUInt64(tupleElement(current, 8)) AS schema_fingerprint, \
            toUInt64(tupleElement(current, 9)) AS checkpoint_revision, \
            tupleElement(current, 10) AS operation_id, \
            toUInt64(tupleElement(current, 11)) AS scan_inode, \
            toUInt64(tupleElement(current, 12)) AS scan_boundary, \
            tupleElement(current, 13) AS policy_fingerprint, \
            toUInt8(tupleElement(current, 14)) AS final_scan_complete, \
            tupleElement(current, 15) AS block_reason, \
            toUInt8(tupleElement(current, 16)) AS compatibility_prepared, \
            toUInt8(tupleElement(current, 17)) AS backend_caught_up, \
            tupleElement(current, 18) AS append_batch_id, \
            toUInt64(tupleElement(current, 19)) AS cache_epoch \
         FROM (SELECT source_name, source_file, \
            argMax(tuple(inode, source_generation, last_offset, last_line, lifecycle, \
                         cursor_json, source_fingerprint, schema_fingerprint, checkpoint_revision, \
                         operation_id, scan_inode, scan_boundary, policy_fingerprint, \
                         final_scan_complete, block_reason, compatibility_prepared, \
                         backend_caught_up, append_batch_id, cache_epoch), \
                   tuple(source_generation, checkpoint_revision)) AS current \
            FROM {}.ingest_checkpoint_transitions WHERE host = '{}' \
            GROUP BY source_name, source_file)",
        database, host
    )
}

async fn load_checkpoints(
    clickhouse: &ClickHouseClient,
    cursor_columns: bool,
    host: Option<&str>,
) -> Result<HashMap<String, model::Checkpoint>> {
    if table_column_available(
        clickhouse,
        "ingest_checkpoint_transitions",
        "checkpoint_revision",
    )
    .await?
    {
        let query = causal_checkpoints_query(&clickhouse.config().database, host);
        let rows: Vec<CausalCheckpointRow> = clickhouse.query_rows(&query, None).await?;
        let mut map = HashMap::<String, model::Checkpoint>::new();
        for row in rows {
            let key = checkpoint_key(&row.source_name, &row.source_file);
            let checkpoint = model::Checkpoint {
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
                policy_fingerprint: row.policy_fingerprint,
                checkpoint_revision: row.checkpoint_revision,
                operation_id: row.operation_id,
                scan_inode: row.scan_inode,
                scan_boundary: row.scan_boundary,
                final_scan_complete: row.final_scan_complete != 0,
                block_reason: row.block_reason,
                compatibility_prepared: row.compatibility_prepared != 0,
                backend_caught_up: row.backend_caught_up != 0,
                append_batch_id: row.append_batch_id,
                cache_epoch: row.cache_epoch,
            };
            checkpoint.lifecycle().with_context(|| {
                format!(
                    "invalid lifecycle for checkpoint {}:{}",
                    checkpoint.source_name, checkpoint.source_file
                )
            })?;
            map.insert(key, checkpoint);
        }
        return Ok(map);
    }

    let query = checkpoints_query(&clickhouse.config().database, cursor_columns, host);
    let rows: Vec<CheckpointRow> = clickhouse.query_rows(&query, None).await?;
    let mut map = HashMap::<String, model::Checkpoint>::new();

    for row in rows {
        let key = checkpoint_key(&row.source_name, &row.source_file);
        let checkpoint = model::Checkpoint {
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
            ..model::Checkpoint::default()
        };
        checkpoint.lifecycle().with_context(|| {
            format!(
                "invalid lifecycle for checkpoint {}:{}",
                checkpoint.source_name, checkpoint.source_file
            )
        })?;
        map.insert(key, checkpoint);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::{build_ingest_clickhouse_client, causal_checkpoints_query, checkpoints_query};
    use axum::{extract::State, http::HeaderMap, routing::post, Router};
    use std::sync::{Arc, Mutex};
    #[tokio::test]
    async fn ingest_clickhouse_requests_carry_process_identity() {
        async fn handler(
            State(user_agents): State<Arc<Mutex<Vec<String>>>>,
            headers: HeaderMap,
        ) -> &'static str {
            let user_agent = headers
                .get("user-agent")
                .expect("ingest request must carry a user-agent")
                .to_str()
                .expect("user-agent must be text")
                .to_string();
            user_agents
                .lock()
                .expect("user-agent capture mutex poisoned")
                .push(user_agent);
            "1"
        }

        let user_agents = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/", post(handler))
            .with_state(user_agents.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind user-agent capture listener");
        let address = listener.local_addr().expect("listener address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let clickhouse = build_ingest_clickhouse_client(moraine_config::ClickHouseConfig {
            url: format!("http://{address}"),
            ..moraine_config::ClickHouseConfig::default()
        })
        .expect("construct ingest ClickHouse client");
        clickhouse.ping().await.expect("ingest ClickHouse ping");

        assert_eq!(
            user_agents
                .lock()
                .expect("user-agent capture mutex poisoned")
                .as_slice(),
            &[format!(
                "moraine-ingest/{} (pid={})",
                env!("CARGO_PKG_VERSION"),
                std::process::id()
            )]
        );
    }

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

    #[test]
    fn causal_checkpoint_restart_prefers_generation_before_late_revision() {
        let query = causal_checkpoints_query("moraine", Some("dev-box"));
        assert!(
            query.contains("tuple(source_generation, checkpoint_revision)"),
            "a late stale-generation transition must not beat the greatest generation: {query}"
        );
        assert_eq!(
            query.matches("argMax(tuple(").count(),
            1,
            "the restart row must come from one indivisible causal tuple"
        );
    }
}
