use crate::checkpoint::merge_checkpoint;
use crate::heartbeat::host_name;
use crate::model::Checkpoint;
use crate::tee::{
    backend_sinks_json, filter_batch_for_backend, BackendSinkCell, ReplayFloor,
    SharedRouteResolver, StatusRegistry,
};
use crate::{
    DispatchState, Metrics, SinkMessage, WATCHER_BACKEND_MIXED, WATCHER_BACKEND_NATIVE,
    WATCHER_BACKEND_POLL,
};
use chrono::{DateTime, Utc};
use moraine_clickhouse::ClickHouseClient;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// What a sink task is writing for. The flush/checkpoint/retry machinery is
/// identical for both; the role decides intake filtering, heartbeats, and
/// health transitions.
pub(crate) enum SinkRole {
    /// The always-on local sink: receives everything and owns heartbeats.
    Default {
        dispatch: Arc<Mutex<DispatchState>>,
        /// Mirror sink status cells, surfaced through the heartbeat.
        backends: StatusRegistry,
        /// Whether `ingest_heartbeats` carries the `backend_sinks` column
        /// (migration 017). Pre-017 schemas reject unknown columns at insert
        /// time, so the field is attached only when the column exists.
        backend_sinks_column: bool,
    },
    /// A mirror sink for one named backend: filters intake down to the
    /// sessions routed to it and reports flush health on its status cell.
    Backend {
        cell: Arc<BackendSinkCell>,
        resolver: SharedRouteResolver,
        /// Signalled on the lagging/unreachable -> ok transition so the
        /// backend's supervisor schedules a catch-up replay pass.
        replay_notify: Arc<Notify>,
        /// Checkpoint floor handed to that pass, captured at the same
        /// transition (see `note_flush_outcome` for why it must be taken
        /// here, inside the sink task, and not when the supervisor wakes).
        replay_floor: ReplayFloor,
    },
}

/// Backend-role health bookkeeping after a flush attempt; no-op for the
/// default sink. Recovery requires a drained queue so one successful flush
/// mid-backlog does not trigger a replay storm.
async fn note_flush_outcome(
    role: &SinkRole,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    flush_ok: bool,
    queue_drained: bool,
) {
    let SinkRole::Backend {
        cell,
        replay_notify,
        replay_floor,
        ..
    } = role
    else {
        return;
    };

    if !flush_ok {
        if cell.mark_flush_failure() {
            warn!(
                backend = cell.name(),
                "backend flush failing; marked unreachable (live mirroring pauses; \
                 catch-up replay runs once it recovers)"
            );
        }
        return;
    }

    if queue_drained && cell.mark_recovered() {
        // Capture the replay floor BEFORE signalling the supervisor. This
        // sink task is the checkpoint map's only writer and has not yet
        // accepted any post-recovery batch, so the snapshot cannot contain
        // a live-forwarded checkpoint that jumps past the outage's dropped
        // span — the gap the scheduled pass exists to close. Snapshotting
        // when the supervisor wakes instead would race the next live flush.
        let floor = checkpoints.read().await.clone();
        *replay_floor.lock().expect("replay floor mutex poisoned") = Some(floor);
        info!(
            backend = cell.name(),
            dropped_batches = cell.dropped_batches(),
            "backend drained after lagging/unreachable; scheduling catch-up replay"
        );
        replay_notify.notify_one();
    }
}

fn watcher_backend_label(value: u64) -> &'static str {
    match value {
        WATCHER_BACKEND_NATIVE => "native",
        WATCHER_BACKEND_POLL => "poll",
        WATCHER_BACKEND_MIXED => "mixed",
        _ => "unknown",
    }
}

fn saturating_u64_to_u32(value: u64) -> u32 {
    value.min(u32::MAX as u64) as u32
}

fn duration_from_config_seconds(seconds: f64, minimum_seconds: f64, field_name: &str) -> Duration {
    if !seconds.is_finite() {
        warn!("non-finite config value for `{field_name}` ({seconds}); using {minimum_seconds}");
        return Duration::from_secs_f64(minimum_seconds);
    }

    let sanitized_seconds = seconds.max(minimum_seconds);
    Duration::try_from_secs_f64(sanitized_seconds).unwrap_or_else(|_| {
        warn!(
            "out-of-range config value for `{field_name}` ({sanitized_seconds}); using {minimum_seconds}"
        );
        Duration::from_secs_f64(minimum_seconds)
    })
}

fn append_to_visible_percentile(sorted_latencies_ms: &[u64], quantile: f64) -> u64 {
    debug_assert!(!sorted_latencies_ms.is_empty());
    let rank = ((sorted_latencies_ms.len() as f64) * quantile).ceil() as usize;
    sorted_latencies_ms[rank.saturating_sub(1).min(sorted_latencies_ms.len() - 1)]
}

fn compute_append_to_visible_stats(
    raw_rows: &[Value],
    visible_at: DateTime<Utc>,
) -> Option<(u32, u32)> {
    let mut latencies_ms: Vec<u64> = raw_rows
        .iter()
        .filter_map(|row| row.get("record_ts").and_then(Value::as_str))
        .filter_map(|record_ts| DateTime::parse_from_rfc3339(record_ts).ok())
        .map(|record_ts| {
            visible_at
                .signed_duration_since(record_ts.with_timezone(&Utc))
                .num_milliseconds()
                .max(0) as u64
        })
        .collect();

    if latencies_ms.is_empty() {
        return None;
    }

    latencies_ms.sort_unstable();
    let p50 = append_to_visible_percentile(&latencies_ms, 0.50);
    let p95 = append_to_visible_percentile(&latencies_ms, 0.95);
    Some((saturating_u64_to_u32(p50), saturating_u64_to_u32(p95)))
}

const SINK_JSON_OBJECT_TOO_LARGE: &str = "sink_json_object_too_large";
const MAX_SINK_ERROR_TEXT_CHARS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkStage {
    RawEvents,
    Events,
    EventLinks,
    ToolIo,
    IngestErrors,
    IngestCheckpoints,
}

impl SinkStage {
    fn table(self) -> &'static str {
        match self {
            Self::RawEvents => "raw_events",
            Self::Events => "events",
            Self::EventLinks => "event_links",
            Self::ToolIo => "tool_io",
            Self::IngestErrors => "ingest_errors",
            Self::IngestCheckpoints => "ingest_checkpoints",
        }
    }
}

#[derive(Debug, Clone)]
struct SinkFlushFailure {
    stage: SinkStage,
    error_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SinkFlushErrorKey {
    stage: SinkStage,
    error_kind: &'static str,
    source_name: String,
    source_file: String,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
}

#[derive(Debug, Clone)]
struct SinkFlushError {
    stage: SinkStage,
    rows_pending: usize,
    source_name: String,
    harness: String,
    inference_provider: String,
    source_file: String,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    error_kind: &'static str,
    error_text: String,
}

impl SinkFlushError {
    fn key(&self) -> SinkFlushErrorKey {
        SinkFlushErrorKey {
            stage: self.stage,
            error_kind: self.error_kind,
            source_name: self.source_name.clone(),
            source_file: self.source_file.clone(),
            source_generation: self.source_generation,
            source_line_no: self.source_line_no,
            source_offset: self.source_offset,
        }
    }

    fn into_row(self) -> Value {
        let table = self.stage.table();
        json!({
            "source_name": self.source_name,
            "harness": self.harness,
            "inference_provider": self.inference_provider,
            "source_file": self.source_file,
            "source_inode": self.source_inode,
            "source_generation": self.source_generation,
            "source_line_no": self.source_line_no,
            "source_offset": self.source_offset,
            "error_kind": self.error_kind,
            "error_text": self.error_text,
            "raw_fragment": format!(
                "sink_stage={}; rows_pending={}; original_payload_omitted=true",
                table, self.rows_pending
            ),
        })
    }
}

fn classify_sink_flush_error(error_text: &str) -> Option<&'static str> {
    let lower = error_text.to_ascii_lowercase();
    if lower.contains("json object extremely large") {
        Some(SINK_JSON_OBJECT_TOO_LARGE)
    } else {
        None
    }
}

fn concise_sink_error_text(table: &str, error_text: &str) -> String {
    let mut out = format!("ClickHouse {table} JSONEachRow flush failed: ");
    let mut last_was_space = false;

    for ch in error_text.chars() {
        let ch = if ch.is_whitespace() { ' ' } else { ch };
        if ch == ' ' {
            if last_was_space {
                continue;
            }
            last_was_space = true;
        } else {
            last_was_space = false;
        }

        if out.len().saturating_add(ch.len_utf8()) > MAX_SINK_ERROR_TEXT_CHARS {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }

    out
}

fn row_string(row: &Value, key: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn row_u64(row: &Value, key: &str) -> u64 {
    row.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn row_u32(row: &Value, key: &str) -> u32 {
    row_u64(row, key).min(u32::MAX as u64) as u32
}

fn sink_flush_error_from_row(
    stage: SinkStage,
    rows: &[Value],
    error_kind: &'static str,
    raw_error_text: &str,
) -> Option<SinkFlushError> {
    let row = rows.first()?;
    Some(SinkFlushError {
        stage,
        rows_pending: rows.len(),
        source_name: row_string(row, "source_name"),
        harness: row_string(row, "harness"),
        inference_provider: row_string(row, "inference_provider"),
        source_file: row_string(row, "source_file"),
        source_inode: row_u64(row, "source_inode"),
        source_generation: row_u32(row, "source_generation"),
        source_line_no: row_u64(row, "source_line_no"),
        source_offset: row_u64(row, "source_offset"),
        error_kind,
        error_text: concise_sink_error_text(stage.table(), raw_error_text),
    })
}

fn sink_flush_error_from_checkpoint(
    stage: SinkStage,
    checkpoint_updates: &HashMap<String, Checkpoint>,
    error_kind: &'static str,
    raw_error_text: &str,
) -> Option<SinkFlushError> {
    let checkpoint = checkpoint_updates.values().next()?;
    Some(SinkFlushError {
        stage,
        rows_pending: checkpoint_updates.len(),
        source_name: checkpoint.source_name.clone(),
        harness: String::new(),
        inference_provider: String::new(),
        source_file: checkpoint.source_file.clone(),
        source_inode: checkpoint.source_inode,
        source_generation: checkpoint.source_generation,
        source_line_no: checkpoint.last_line_no,
        source_offset: checkpoint.last_offset,
        error_kind,
        error_text: concise_sink_error_text(stage.table(), raw_error_text),
    })
}

fn pending_sink_flush_error(
    failure: &SinkFlushFailure,
    rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
    error_kind: &'static str,
) -> Option<SinkFlushError> {
    match failure.stage {
        SinkStage::RawEvents
        | SinkStage::Events
        | SinkStage::EventLinks
        | SinkStage::ToolIo
        | SinkStage::IngestErrors => {
            sink_flush_error_from_row(failure.stage, rows, error_kind, &failure.error_text)
        }
        SinkStage::IngestCheckpoints => sink_flush_error_from_checkpoint(
            failure.stage,
            checkpoint_updates,
            error_kind,
            &failure.error_text,
        ),
    }
}

fn pending_rows_for_stage<'a>(
    stage: SinkStage,
    raw_rows: &'a [Value],
    event_rows: &'a [Value],
    link_rows: &'a [Value],
    tool_rows: &'a [Value],
    error_rows: &'a [Value],
) -> &'a [Value] {
    match stage {
        SinkStage::RawEvents => raw_rows,
        SinkStage::Events => event_rows,
        SinkStage::EventLinks => link_rows,
        SinkStage::ToolIo => tool_rows,
        SinkStage::IngestErrors => error_rows,
        SinkStage::IngestCheckpoints => &[],
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_classified_sink_flush_error(
    clickhouse: &ClickHouseClient,
    metrics: &Arc<Metrics>,
    raw_rows: &[Value],
    event_rows: &[Value],
    link_rows: &[Value],
    tool_rows: &[Value],
    error_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
    failure: &SinkFlushFailure,
    last_reported: &mut Option<SinkFlushErrorKey>,
) {
    let Some(error_kind) = classify_sink_flush_error(&failure.error_text) else {
        return;
    };
    let rows = pending_rows_for_stage(
        failure.stage,
        raw_rows,
        event_rows,
        link_rows,
        tool_rows,
        error_rows,
    );
    let Some(error) = pending_sink_flush_error(failure, rows, checkpoint_updates, error_kind)
    else {
        return;
    };

    let key = error.key();
    if last_reported.as_ref() == Some(&key) {
        return;
    }

    let table = error.stage.table();
    let row = error.into_row();
    match clickhouse.insert_json_rows("ingest_errors", &[row]).await {
        Ok(()) => {
            metrics.err_rows_written.fetch_add(1, Ordering::Relaxed);
            *last_reported = Some(key);
            warn!(
                sink_stage = table,
                error_kind, "recorded durable ingest_errors row for sink flush failure"
            );
        }
        Err(exc) => {
            warn!(
                sink_stage = table,
                error_kind,
                "failed to record durable ingest_errors row for sink flush failure: {exc}"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_flush_outcome(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    role: &SinkRole,
    flush_result: &std::result::Result<(), SinkFlushFailure>,
    queue_drained: bool,
    raw_rows: &[Value],
    event_rows: &[Value],
    link_rows: &[Value],
    tool_rows: &[Value],
    error_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
    last_reported_sink_error: &mut Option<SinkFlushErrorKey>,
) {
    let flush_ok = flush_result.is_ok();
    note_flush_outcome(role, checkpoints, flush_ok, queue_drained).await;
    if let Err(failure) = flush_result {
        record_classified_sink_flush_error(
            clickhouse,
            metrics,
            raw_rows,
            event_rows,
            link_rows,
            tool_rows,
            error_rows,
            checkpoint_updates,
            failure,
            last_reported_sink_error,
        )
        .await;
    } else {
        *last_reported_sink_error = None;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_sink_task(
    config: moraine_config::AppConfig,
    clickhouse: ClickHouseClient,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<SinkMessage>,
    checkpoint_cursor_columns: bool,
    role: SinkRole,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut raw_rows = Vec::<Value>::new();
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut tool_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::<String, Checkpoint>::new();
        let mut pending_batch_bytes = 0usize;
        let mut last_reported_sink_error = None::<SinkFlushErrorKey>;

        // Mirror sinks share one ingest_checkpoints table per team backend,
        // so their rows are scoped per host (migration 018; guaranteed
        // present by the schema handshake). The default backend stays
        // single-writer and keeps writing host-less rows, which also keeps
        // it working before `moraine db migrate` adds the column locally.
        let checkpoint_host = match &role {
            SinkRole::Backend { .. } => host_name(),
            SinkRole::Default { .. } => String::new(),
        };

        let flush_interval = duration_from_config_seconds(
            config.ingest.flush_interval_seconds,
            0.05,
            "ingest.flush_interval_seconds",
        );
        let heartbeat_interval = duration_from_config_seconds(
            config.ingest.heartbeat_interval_seconds,
            1.0,
            "ingest.heartbeat_interval_seconds",
        );
        let retry_backoff = duration_from_config_seconds(
            config.ingest.flush_interval_seconds * 2.0,
            0.25,
            "ingest.flush_interval_seconds * 2.0",
        );

        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);
        let mut throttling_flush_retries = false;

        loop {
            if throttling_flush_retries
                && has_pending_data(
                    &raw_rows,
                    &event_rows,
                    &link_rows,
                    &tool_rows,
                    &error_rows,
                    &checkpoint_updates,
                )
            {
                let flush_result = flush_pending(
                    &clickhouse,
                    &checkpoints,
                    &metrics,
                    &mut raw_rows,
                    &mut event_rows,
                    &mut link_rows,
                    &mut tool_rows,
                    &mut error_rows,
                    &mut checkpoint_updates,
                    checkpoint_cursor_columns,
                    &checkpoint_host,
                )
                .await;
                handle_flush_outcome(
                    &clickhouse,
                    &checkpoints,
                    &metrics,
                    &role,
                    &flush_result,
                    rx.is_empty(),
                    &raw_rows,
                    &event_rows,
                    &link_rows,
                    &tool_rows,
                    &error_rows,
                    &checkpoint_updates,
                    &mut last_reported_sink_error,
                )
                .await;
                if flush_result.is_ok() {
                    pending_batch_bytes = 0;
                    throttling_flush_retries = false;
                    info!("flush retry succeeded; resuming sink intake");
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(retry_backoff) => {}
                        _ = heartbeat_tick.tick() => {
                            emit_heartbeat(&clickhouse, &metrics, &role).await;
                        }
                    }
                }
                continue;
            }

            tokio::select! {
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some(SinkMessage::Batch(batch)) => {
                            // Mirror sinks only buffer the sessions routed to
                            // them; the default sink takes batches whole.
                            let batch = match &role {
                                SinkRole::Backend { cell, resolver, .. } => {
                                    let mut resolver =
                                        resolver.lock().expect("route resolver mutex poisoned");
                                    filter_batch_for_backend(&batch, cell.name(), &mut resolver)
                                }
                                SinkRole::Default { .. } => batch,
                            };
                            pending_batch_bytes =
                                pending_batch_bytes.saturating_add(batch.approx_bytes());
                            raw_rows.extend(batch.raw_rows);
                            event_rows.extend(batch.event_rows);
                            link_rows.extend(batch.link_rows);
                            tool_rows.extend(batch.tool_rows);
                            error_rows.extend(batch.error_rows);
                            if let Some(cp) = batch.checkpoint {
                                merge_checkpoint(&mut checkpoint_updates, cp);
                            }

                            let total_rows = raw_rows.len() + event_rows.len() + link_rows.len() + tool_rows.len() + error_rows.len();
                            if total_rows >= config.ingest.batch_size
                                || pending_batch_bytes >= config.ingest.max_batch_bytes.max(1)
                            {
                                let flush_result = flush_pending(
                                    &clickhouse,
                                    &checkpoints,
                                    &metrics,
                                    &mut raw_rows,
                                    &mut event_rows,
                                    &mut link_rows,
                                    &mut tool_rows,
                                    &mut error_rows,
                                    &mut checkpoint_updates,
                                    checkpoint_cursor_columns,
                                    &checkpoint_host,
                                ).await;
                                handle_flush_outcome(
                                    &clickhouse,
                                    &checkpoints,
                                    &metrics,
                                    &role,
                                    &flush_result,
                                    rx.is_empty(),
                                    &raw_rows,
                                    &event_rows,
                                    &link_rows,
                                    &tool_rows,
                                    &error_rows,
                                    &checkpoint_updates,
                                    &mut last_reported_sink_error,
                                ).await;
                                if flush_result.is_err() {
                                    if !throttling_flush_retries {
                                        warn!(
                                            "flush failed; pausing sink intake and retrying pending rows every {} ms",
                                            retry_backoff.as_millis()
                                        );
                                    }
                                    throttling_flush_retries = true;
                                } else {
                                    pending_batch_bytes = 0;
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if has_pending_data(&raw_rows, &event_rows, &link_rows, &tool_rows, &error_rows, &checkpoint_updates) {
                        let flush_result = flush_pending(
                            &clickhouse,
                            &checkpoints,
                            &metrics,
                            &mut raw_rows,
                            &mut event_rows,
                            &mut link_rows,
                            &mut tool_rows,
                            &mut error_rows,
                            &mut checkpoint_updates,
                            checkpoint_cursor_columns,
                            &checkpoint_host,
                        ).await;
                        handle_flush_outcome(
                            &clickhouse,
                            &checkpoints,
                            &metrics,
                            &role,
                            &flush_result,
                            rx.is_empty(),
                            &raw_rows,
                            &event_rows,
                            &link_rows,
                            &tool_rows,
                            &error_rows,
                            &checkpoint_updates,
                            &mut last_reported_sink_error,
                        ).await;
                        if flush_result.is_err() {
                            if !throttling_flush_retries {
                                warn!(
                                    "flush failed; pausing sink intake and retrying pending rows every {} ms",
                                    retry_backoff.as_millis()
                                );
                            }
                            throttling_flush_retries = true;
                        } else {
                            pending_batch_bytes = 0;
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    emit_heartbeat(&clickhouse, &metrics, &role).await;
                }
            }
        }

        if has_pending_data(
            &raw_rows,
            &event_rows,
            &link_rows,
            &tool_rows,
            &error_rows,
            &checkpoint_updates,
        ) {
            let flush_result = flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut tool_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                checkpoint_cursor_columns,
                &checkpoint_host,
            )
            .await;
            handle_flush_outcome(
                &clickhouse,
                &checkpoints,
                &metrics,
                &role,
                &flush_result,
                true,
                &raw_rows,
                &event_rows,
                &link_rows,
                &tool_rows,
                &error_rows,
                &checkpoint_updates,
                &mut last_reported_sink_error,
            )
            .await;
        }
    })
}

fn has_pending_data(
    raw_rows: &[Value],
    event_rows: &[Value],
    link_rows: &[Value],
    tool_rows: &[Value],
    error_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
) -> bool {
    !(raw_rows.is_empty()
        && event_rows.is_empty()
        && link_rows.is_empty()
        && tool_rows.is_empty()
        && error_rows.is_empty()
        && checkpoint_updates.is_empty())
}

/// Default-role only: mirror sinks write rows into databases this host does
/// not own, so the ingest heartbeat (including per-backend mirror status)
/// always lands in the default backend.
async fn emit_heartbeat(clickhouse: &ClickHouseClient, metrics: &Arc<Metrics>, role: &SinkRole) {
    let SinkRole::Default {
        dispatch,
        backends,
        backend_sinks_column,
    } = role
    else {
        return;
    };

    let files_active = {
        let state = dispatch.lock().expect("dispatch mutex poisoned");
        state.inflight.len() as u32
    };
    let files_watched = metrics.watcher_registrations.load(Ordering::Relaxed) as u32;
    let last_error = {
        metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .clone()
    };
    let watcher_backend =
        watcher_backend_label(metrics.watcher_backend_state.load(Ordering::Relaxed));

    let mut heartbeat = json!({
        "host": host_name(),
        "service_version": env!("CARGO_PKG_VERSION"),
        "queue_depth": metrics.queue_depth.load(Ordering::Relaxed),
        "files_active": files_active,
        "files_watched": files_watched,
        "rows_raw_written": metrics.raw_rows_written.load(Ordering::Relaxed),
        "rows_events_written": metrics.event_rows_written.load(Ordering::Relaxed),
        "rows_errors_written": metrics.err_rows_written.load(Ordering::Relaxed),
        "flush_latency_ms": saturating_u64_to_u32(metrics.last_flush_ms.load(Ordering::Relaxed)),
        "append_to_visible_p50_ms": saturating_u64_to_u32(metrics.append_to_visible_p50_ms.load(Ordering::Relaxed)),
        "append_to_visible_p95_ms": saturating_u64_to_u32(metrics.append_to_visible_p95_ms.load(Ordering::Relaxed)),
        "watcher_backend": watcher_backend,
        "watcher_error_count": metrics.watcher_error_count.load(Ordering::Relaxed),
        "watcher_reset_count": metrics.watcher_reset_count.load(Ordering::Relaxed),
        "watcher_last_reset_unix_ms": metrics.watcher_last_reset_unix_ms.load(Ordering::Relaxed),
        "last_error": last_error,
    });

    // Attached only when at least one mirror sink exists AND the column is
    // present (pre-017 schemas reject unknown columns at insert time).
    if *backend_sinks_column {
        if let Some(statuses) = backend_sinks_json(backends) {
            if let Some(obj) = heartbeat.as_object_mut() {
                obj.insert("backend_sinks".to_string(), json!(statuses));
            }
        }
    }

    if let Err(exc) = clickhouse
        .insert_json_rows("ingest_heartbeats", &[heartbeat])
        .await
    {
        warn!("heartbeat insert failed: {exc}");
    }
}

#[allow(clippy::too_many_arguments)]
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
    checkpoint_cursor_columns: bool,
    checkpoint_host: &str,
) -> std::result::Result<(), SinkFlushFailure> {
    let started = Instant::now();

    let checkpoint_rows: Vec<Value> = checkpoint_updates
        .values()
        .map(|cp| {
            let mut row = json!({
                "source_name": cp.source_name,
                "source_file": cp.source_file,
                "source_inode": cp.source_inode,
                "source_generation": cp.source_generation,
                "last_offset": cp.last_offset,
                "last_line_no": cp.last_line_no,
                "status": cp.status,
            });
            // Older schemas (pre-015) reject unknown columns at insert time,
            // so the SQLite cursor fields are attached only when present.
            if checkpoint_cursor_columns {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("cursor_json".to_string(), json!(cp.cursor_json));
                    obj.insert(
                        "source_fingerprint".to_string(),
                        json!(cp.source_fingerprint),
                    );
                    obj.insert(
                        "schema_fingerprint".to_string(),
                        json!(cp.schema_fingerprint),
                    );
                }
            }
            // Backend-role only (migration 018): scopes rows in a shared
            // team table to this host. Empty for the default sink.
            if !checkpoint_host.is_empty() {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("host".to_string(), json!(checkpoint_host));
                }
            }
            row
        })
        .collect();

    let flush_result = async {
        if !raw_rows.is_empty() {
            clickhouse
                .insert_json_rows("raw_events", raw_rows)
                .await
                .map_err(|exc| SinkFlushFailure {
                    stage: SinkStage::RawEvents,
                    error_text: exc.to_string(),
                })?;
            metrics
                .raw_rows_written
                .fetch_add(raw_rows.len() as u64, Ordering::Relaxed);
            if let Some((p50_ms, p95_ms)) = compute_append_to_visible_stats(raw_rows, Utc::now()) {
                metrics
                    .append_to_visible_p50_ms
                    .store(p50_ms as u64, Ordering::Relaxed);
                metrics
                    .append_to_visible_p95_ms
                    .store(p95_ms as u64, Ordering::Relaxed);
            }
            raw_rows.clear();
        }

        if !event_rows.is_empty() {
            clickhouse
                .insert_json_rows("events", event_rows)
                .await
                .map_err(|exc| SinkFlushFailure {
                    stage: SinkStage::Events,
                    error_text: exc.to_string(),
                })?;
            metrics
                .event_rows_written
                .fetch_add(event_rows.len() as u64, Ordering::Relaxed);
            event_rows.clear();
        }

        if !link_rows.is_empty() {
            clickhouse
                .insert_json_rows("event_links", link_rows)
                .await
                .map_err(|exc| SinkFlushFailure {
                    stage: SinkStage::EventLinks,
                    error_text: exc.to_string(),
                })?;
            link_rows.clear();
        }

        if !tool_rows.is_empty() {
            clickhouse
                .insert_json_rows("tool_io", tool_rows)
                .await
                .map_err(|exc| SinkFlushFailure {
                    stage: SinkStage::ToolIo,
                    error_text: exc.to_string(),
                })?;
            tool_rows.clear();
        }

        if !error_rows.is_empty() {
            clickhouse
                .insert_json_rows("ingest_errors", error_rows)
                .await
                .map_err(|exc| SinkFlushFailure {
                    stage: SinkStage::IngestErrors,
                    error_text: exc.to_string(),
                })?;
            metrics
                .err_rows_written
                .fetch_add(error_rows.len() as u64, Ordering::Relaxed);
            error_rows.clear();
        }

        if !checkpoint_rows.is_empty() {
            clickhouse
                .insert_json_rows("ingest_checkpoints", &checkpoint_rows)
                .await
                .map_err(|exc| SinkFlushFailure {
                    stage: SinkStage::IngestCheckpoints,
                    error_text: exc.to_string(),
                })?;

            {
                // Offset-monotone merge, not a blind insert: a backend map
                // has two producers (live router batches and catch-up
                // replay), so an out-of-order flush must never regress —
                // or, for replay overlapping fresher live data, rewind —
                // what the map already committed.
                let mut state = checkpoints.write().await;
                for cp in checkpoint_updates.values() {
                    merge_checkpoint(&mut state, cp.clone());
                }
            }
            checkpoint_updates.clear();
        }

        metrics
            .last_flush_ms
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        std::result::Result::<(), SinkFlushFailure>::Ok(())
    }
    .await;

    match flush_result {
        Ok(()) => Ok(()),
        Err(failure) => {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = failure.error_text.clone();
            warn!(
                sink_stage = failure.stage.table(),
                "flush failed: {}", failure.error_text
            );
            Err(failure)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::checkpoint_key;
    use crate::model::RowBatch;
    use axum::{
        extract::{Query, State},
        http::StatusCode,
        routing::post,
        Router,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use tokio::time::timeout;

    #[derive(Clone, Default)]
    struct MockClickHouseState {
        calls_by_table: Arc<Mutex<HashMap<String, usize>>>,
        rows_by_table: Arc<Mutex<HashMap<String, Vec<Value>>>>,
        fail_once_by_table: Arc<Mutex<HashMap<String, usize>>>,
        failure_body_by_table: Arc<Mutex<HashMap<String, String>>>,
    }

    impl MockClickHouseState {
        fn with_single_failure(table: &str) -> Self {
            Self::with_single_failure_body(table, format!("intentional failure for {table}"))
        }

        fn with_single_failure_body(table: &str, body: impl Into<String>) -> Self {
            let state = Self::default();
            state
                .fail_once_by_table
                .lock()
                .expect("mock fail_once mutex poisoned")
                .insert(table.to_string(), 1);
            state
                .failure_body_by_table
                .lock()
                .expect("mock failure body mutex poisoned")
                .insert(table.to_string(), body.into());
            state
        }

        fn call_count(&self, table: &str) -> usize {
            *self
                .calls_by_table
                .lock()
                .expect("mock calls mutex poisoned")
                .get(table)
                .unwrap_or(&0)
        }

        fn rows(&self, table: &str) -> Vec<Value> {
            self.rows_by_table
                .lock()
                .expect("mock rows mutex poisoned")
                .get(table)
                .cloned()
                .unwrap_or_default()
        }

        fn failure_body(&self, table: &str) -> String {
            self.failure_body_by_table
                .lock()
                .expect("mock failure body mutex poisoned")
                .get(table)
                .cloned()
                .unwrap_or_else(|| format!("intentional failure for {table}"))
        }
    }

    fn inserted_table_name(query: &str) -> Option<&'static str> {
        if query.contains("`raw_events`") {
            Some("raw_events")
        } else if query.contains("`event_links`") {
            Some("event_links")
        } else if query.contains("`tool_io`") {
            Some("tool_io")
        } else if query.contains("`ingest_errors`") {
            Some("ingest_errors")
        } else if query.contains("`ingest_checkpoints`") {
            Some("ingest_checkpoints")
        } else if query.contains("`events`") {
            Some("events")
        } else {
            None
        }
    }

    async fn mock_clickhouse_handler(
        State(state): State<MockClickHouseState>,
        Query(params): Query<HashMap<String, String>>,
        body: String,
    ) -> (StatusCode, String) {
        let query = params.get("query").cloned().unwrap_or_default();
        let Some(table) = inserted_table_name(&query) else {
            return (
                StatusCode::BAD_REQUEST,
                format!("unexpected query payload: {query}"),
            );
        };

        {
            let mut calls = state
                .calls_by_table
                .lock()
                .expect("mock calls mutex poisoned");
            *calls.entry(table.to_string()).or_insert(0) += 1;
        }
        {
            let mut fail_once = state
                .fail_once_by_table
                .lock()
                .expect("mock fail_once mutex poisoned");
            if let Some(remaining) = fail_once.get_mut(table) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return (StatusCode::INTERNAL_SERVER_ERROR, state.failure_body(table));
                }
            }
        }

        {
            let mut rows = state
                .rows_by_table
                .lock()
                .expect("mock rows mutex poisoned");
            let entry = rows.entry(table.to_string()).or_default();
            for line in body.lines().filter(|line| !line.trim().is_empty()) {
                if let Ok(row) = serde_json::from_str::<Value>(line) {
                    entry.push(row);
                }
            }
        }

        (StatusCode::OK, String::new())
    }

    async fn spawn_mock_clickhouse(
        fail_once_table: &str,
    ) -> (ClickHouseClient, MockClickHouseState) {
        let state = MockClickHouseState::with_single_failure(fail_once_table);
        spawn_mock_clickhouse_with_state(state).await
    }

    async fn spawn_mock_clickhouse_with_failure_body(
        fail_once_table: &str,
        body: impl Into<String>,
    ) -> (ClickHouseClient, MockClickHouseState) {
        let state = MockClickHouseState::with_single_failure_body(fail_once_table, body);
        spawn_mock_clickhouse_with_state(state).await
    }

    async fn spawn_mock_clickhouse_with_state(
        state: MockClickHouseState,
    ) -> (ClickHouseClient, MockClickHouseState) {
        let app = Router::new()
            .route("/", post(mock_clickhouse_handler))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock clickhouse listener");
        let addr = listener.local_addr().expect("mock listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = moraine_config::AppConfig::default();
        config.clickhouse.url = format!("http://{}", addr);
        config.clickhouse.timeout_seconds = 1.0;
        let clickhouse = ClickHouseClient::new(config.clickhouse)
            .expect("mock clickhouse client should initialize");

        (clickhouse, state)
    }

    fn sample_checkpoint() -> Checkpoint {
        Checkpoint {
            source_name: "source-a".to_string(),
            source_file: "/tmp/source-a.jsonl".to_string(),
            source_inode: 42,
            source_generation: 1,
            last_offset: 100,
            last_line_no: 3,
            status: "active".to_string(),
            ..Default::default()
        }
    }

    fn single_row_batch(id: u64) -> SinkMessage {
        let mut batch = RowBatch::default();
        batch.raw_rows.push(json!({ "id": id }));
        SinkMessage::Batch(batch)
    }

    fn default_role() -> SinkRole {
        SinkRole::Default {
            dispatch: Arc::new(Mutex::new(DispatchState::default())),
            backends: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            backend_sinks_column: false,
        }
    }

    #[tokio::test]
    async fn failed_flush_throttles_sink_consumption() {
        let mut config = moraine_config::AppConfig::default();
        config.clickhouse.url = "http://127.0.0.1:1".to_string();
        config.clickhouse.timeout_seconds = 1.0;
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 0.05;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let clickhouse = ClickHouseClient::new(config.clickhouse.clone())
            .expect("clickhouse client should initialize");
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel(1);

        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            metrics,
            rx,
            true,
            default_role(),
        );

        tx.send(single_row_batch(1))
            .await
            .expect("first send should succeed");
        tx.send(single_row_batch(2))
            .await
            .expect("second send should succeed");

        let third_send = timeout(Duration::from_millis(350), tx.send(single_row_batch(3))).await;
        assert!(
            third_send.is_err(),
            "third send should block while sink retries failed flushes"
        );

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_retries_only_unfinished_tables() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("events").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let mut raw_rows = vec![json!({
            "record_ts": "2026-02-17T00:00:01.000Z",
            "event_uid": "evt-1"
        })];
        let mut event_rows = vec![json!({"event_uid": "evt-1"})];
        let mut link_rows = Vec::<Value>::new();
        let mut tool_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::new();
        let checkpoint = sample_checkpoint();
        checkpoint_updates.insert(
            checkpoint_key(&checkpoint.source_name, &checkpoint.source_file),
            checkpoint.clone(),
        );

        let first_attempt = flush_pending(
            &clickhouse,
            &checkpoints,
            &metrics,
            &mut raw_rows,
            &mut event_rows,
            &mut link_rows,
            &mut tool_rows,
            &mut error_rows,
            &mut checkpoint_updates,
            true,
            "",
        )
        .await;
        assert!(
            first_attempt.is_err(),
            "first flush should fail at events stage"
        );

        assert!(raw_rows.is_empty(), "raw rows should not be retried");
        assert_eq!(
            event_rows.len(),
            1,
            "event rows remain pending after failure"
        );
        assert_eq!(
            checkpoint_updates.len(),
            1,
            "checkpoint update must remain pending until checkpoint flush succeeds"
        );
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        let second_attempt = flush_pending(
            &clickhouse,
            &checkpoints,
            &metrics,
            &mut raw_rows,
            &mut event_rows,
            &mut link_rows,
            &mut tool_rows,
            &mut error_rows,
            &mut checkpoint_updates,
            true,
            "",
        )
        .await;
        assert!(
            second_attempt.is_ok(),
            "second flush should complete remaining stages"
        );

        assert!(event_rows.is_empty());
        assert!(checkpoint_updates.is_empty());
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        assert_eq!(mock_state.call_count("raw_events"), 1);
        assert_eq!(mock_state.call_count("events"), 2);
        assert_eq!(mock_state.call_count("ingest_checkpoints"), 1);

        let state = checkpoints.read().await;
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        assert!(
            state.contains_key(&checkpoint_key_value),
            "checkpoint cache should advance after checkpoint stage succeeds"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn classified_sink_flush_failure_records_compact_ingest_error_once() {
        let clickhouse_error = format!(
            "Code: 117. DB::Exception: JSON object extremely large, expected <=10485760 bytes. {}",
            "detail ".repeat(300)
        );
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_failure_body("raw_events", clickhouse_error).await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let mut raw_rows = vec![json!({
            "source_name": "codex-jsonl",
            "harness": "codex",
            "inference_provider": "openai",
            "source_file": "/tmp/codex-session.jsonl",
            "source_inode": 77u64,
            "source_generation": 2u32,
            "source_line_no": 9u64,
            "source_offset": 1234u64,
            "record_ts": "2026-02-17T00:00:01.000Z",
            "raw_json": "payload-marker-that-must-not-be-copied",
            "event_uid": "evt-oversized",
        })];
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut tool_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::new();

        let flush_result = flush_pending(
            &clickhouse,
            &checkpoints,
            &metrics,
            &mut raw_rows,
            &mut event_rows,
            &mut link_rows,
            &mut tool_rows,
            &mut error_rows,
            &mut checkpoint_updates,
            true,
            "",
        )
        .await;
        let failure = flush_result.expect_err("raw_events flush should fail");
        assert_eq!(failure.stage, SinkStage::RawEvents);

        let mut last_reported = None;
        record_classified_sink_flush_error(
            &clickhouse,
            &metrics,
            &raw_rows,
            &event_rows,
            &link_rows,
            &tool_rows,
            &error_rows,
            &checkpoint_updates,
            &failure,
            &mut last_reported,
        )
        .await;
        record_classified_sink_flush_error(
            &clickhouse,
            &metrics,
            &raw_rows,
            &event_rows,
            &link_rows,
            &tool_rows,
            &error_rows,
            &checkpoint_updates,
            &failure,
            &mut last_reported,
        )
        .await;

        let error_rows = mock_state.rows("ingest_errors");
        assert_eq!(
            error_rows.len(),
            1,
            "same pending sink failure should be reported once"
        );
        let row = &error_rows[0];
        assert_eq!(
            row.get("source_name").and_then(Value::as_str),
            Some("codex-jsonl")
        );
        assert_eq!(row.get("harness").and_then(Value::as_str), Some("codex"));
        assert_eq!(
            row.get("inference_provider").and_then(Value::as_str),
            Some("openai")
        );
        assert_eq!(
            row.get("source_file").and_then(Value::as_str),
            Some("/tmp/codex-session.jsonl")
        );
        assert_eq!(row.get("source_inode").and_then(Value::as_u64), Some(77));
        assert_eq!(
            row.get("source_generation").and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(row.get("source_line_no").and_then(Value::as_u64), Some(9));
        assert_eq!(row.get("source_offset").and_then(Value::as_u64), Some(1234));
        assert_eq!(
            row.get("error_kind").and_then(Value::as_str),
            Some(SINK_JSON_OBJECT_TOO_LARGE)
        );

        let recorded_text = row
            .get("error_text")
            .and_then(Value::as_str)
            .expect("error_text");
        assert!(recorded_text.contains("Code: 117"));
        assert!(recorded_text.len() <= MAX_SINK_ERROR_TEXT_CHARS + 3);
        assert_eq!(
            row.get("raw_fragment").and_then(Value::as_str),
            Some("sink_stage=raw_events; rows_pending=1; original_payload_omitted=true")
        );
        assert!(
            !serde_json::to_string(row)
                .expect("serialize recorded row")
                .contains("payload-marker-that-must-not-be-copied"),
            "durable sink error must not copy the rejected source payload"
        );
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(mock_state.call_count("ingest_errors"), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn generic_clickhouse_code_117_is_not_labeled_as_oversized_json() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let metrics = Arc::new(Metrics::default());
        let raw_rows = vec![json!({
            "source_name": "codex-jsonl",
            "harness": "codex",
            "source_file": "/tmp/codex-session.jsonl",
            "source_inode": 77u64,
            "source_generation": 2u32,
            "source_line_no": 9u64,
            "source_offset": 1234u64,
        })];
        let checkpoint_updates = HashMap::new();
        let failure = SinkFlushFailure {
            stage: SinkStage::RawEvents,
            error_text: "Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow"
                .to_string(),
        };
        let mut last_reported = None;

        record_classified_sink_flush_error(
            &clickhouse,
            &metrics,
            &raw_rows,
            &[],
            &[],
            &[],
            &[],
            &checkpoint_updates,
            &failure,
            &mut last_reported,
        )
        .await;

        assert!(
            mock_state.rows("ingest_errors").is_empty(),
            "generic Code 117 errors should not be mislabeled as oversized JSON"
        );
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 0);
        assert!(last_reported.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_sink_filters_intake_to_routed_sessions() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;

        let mut route_config = moraine_config::AppConfig::default();
        route_config.backends.insert(
            "team-ch".to_string(),
            moraine_config::ClickHouseConfig::default(),
        );
        route_config.routes.push(moraine_config::RouteConfig {
            dir: "/work/team/**".to_string(),
            backend: "team-ch".to_string(),
            mode: moraine_config::ROUTE_MODE_MIRROR.to_string(),
        });
        let resolver: SharedRouteResolver = Arc::new(Mutex::new(crate::tee::RouteResolver::new(
            Arc::new(route_config),
        )));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);

        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            metrics,
            rx,
            true,
            SinkRole::Backend {
                cell: Arc::new(BackendSinkCell::new("team-ch")),
                resolver,
                replay_notify: Arc::new(Notify::new()),
                replay_floor: Arc::new(Mutex::new(None)),
            },
        );

        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({
            "session_id": "team-session",
            "cwd": "/work/team/project",
            "record_ts": "2026-02-17T00:00:01.000Z",
        }));
        batch.push_raw_row(json!({
            "session_id": "other-session",
            "cwd": "/home/other",
            "record_ts": "2026-02-17T00:00:01.000Z",
        }));
        batch.checkpoint = Some(sample_checkpoint());
        tx.send(SinkMessage::Batch(batch))
            .await
            .expect("send mixed batch");
        drop(tx); // close the channel so the final flush runs deterministically

        timeout(Duration::from_secs(5), handle)
            .await
            .expect("backend sink should finish")
            .expect("backend sink task should not panic");

        let raw_rows = mock_state.rows("raw_events");
        assert_eq!(raw_rows.len(), 1, "only the routed session's rows flush");
        assert_eq!(
            raw_rows[0].get("session_id").and_then(Value::as_str),
            Some("team-session")
        );
        let checkpoint_rows = mock_state.rows("ingest_checkpoints");
        assert_eq!(
            checkpoint_rows.len(),
            1,
            "the checkpoint lands in the backend's own database even though \
             part of the batch was filtered away"
        );
        assert_eq!(
            checkpoint_rows[0].get("host").and_then(Value::as_str),
            Some(host_name().as_str()),
            "backend checkpoint rows are host-scoped: team backends share one \
             ingest_checkpoints table across members"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_map_merge_is_offset_monotone() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let mut raw_rows = Vec::<Value>::new();
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut tool_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        let ahead = sample_checkpoint(); // offset 100
        let key = checkpoint_key(&ahead.source_name, &ahead.source_file);
        let mut behind = sample_checkpoint();
        behind.last_offset = 50;

        for cp in [ahead, behind] {
            let mut checkpoint_updates = HashMap::new();
            checkpoint_updates.insert(key.clone(), cp);
            assert!(flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut tool_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
            )
            .await
            .is_ok());
        }

        // A backend map has two producers (live router batches and replay),
        // so an out-of-order flush must never rewind the committed offset —
        // the replay pass would otherwise re-trust a stale floor.
        let state = checkpoints.read().await;
        assert_eq!(
            state.get(&key).map(|cp| cp.last_offset),
            Some(100),
            "a lower-offset flush must not regress the committed map"
        );
        assert!(
            mock_state.rows("ingest_checkpoints").len() == 2,
            "both checkpoint rows still flush; only the map merge is monotone"
        );
        let default_role_rows = mock_state.rows("ingest_checkpoints");
        assert!(
            default_role_rows
                .iter()
                .all(|row| row.get("host").is_none()),
            "an empty checkpoint host (default role) must omit the column"
        );
    }

    #[tokio::test]
    async fn recovery_captures_the_replay_floor_before_post_recovery_flushes() {
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status(crate::tee::BackendSinkStatus::Lagging);
        let replay_notify = Arc::new(Notify::new());
        let replay_floor: ReplayFloor = Arc::new(Mutex::new(None));
        let role = SinkRole::Backend {
            cell,
            resolver: Arc::new(Mutex::new(crate::tee::RouteResolver::new(Arc::new(
                moraine_config::AppConfig::default(),
            )))),
            replay_notify: replay_notify.clone(),
            replay_floor: replay_floor.clone(),
        };

        let checkpoint = sample_checkpoint(); // offset 100
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let checkpoints = Arc::new(RwLock::new(HashMap::from([(
            key.clone(),
            checkpoint.clone(),
        )])));

        note_flush_outcome(&role, &checkpoints, true, true).await;

        // A live batch flushed after recovery jumps the map past the outage
        // gap; the floor captured at the transition must not move with it.
        {
            let mut state = checkpoints.write().await;
            let entry = state.get_mut(&key).expect("checkpoint present");
            entry.last_offset = 900;
        }

        let floor = replay_floor
            .lock()
            .expect("replay floor mutex poisoned")
            .take()
            .expect("recovery must capture a replay floor");
        assert_eq!(
            floor.get(&key).map(|cp| cp.last_offset),
            Some(100),
            "the floor reflects what the backend had flushed at recovery"
        );
        assert!(
            timeout(Duration::from_millis(100), replay_notify.notified())
                .await
                .is_ok(),
            "recovery must schedule a replay pass"
        );
    }

    #[test]
    fn compute_append_to_visible_stats_uses_real_record_timestamps() {
        let visible_at = DateTime::parse_from_rfc3339("2026-02-17T00:00:10.000Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let raw_rows = vec![
            json!({"record_ts": "2026-02-17T00:00:00.000Z"}),
            json!({"record_ts": "2026-02-17T00:00:05.000Z"}),
            json!({"record_ts": "2026-02-17T00:00:09.000Z"}),
        ];

        let (p50, p95) = compute_append_to_visible_stats(&raw_rows, visible_at)
            .expect("expected percentile stats");

        assert_eq!(p50, 5_000);
        assert_eq!(p95, 10_000);
    }

    #[test]
    fn compute_append_to_visible_stats_returns_none_for_unparseable_rows() {
        let visible_at = DateTime::parse_from_rfc3339("2026-02-17T00:00:10.000Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let raw_rows = vec![
            json!({"record_ts": "not-a-timestamp"}),
            json!({"record_ts": ""}),
            json!({}),
        ];

        let stats = compute_append_to_visible_stats(&raw_rows, visible_at);
        assert!(stats.is_none());
    }

    #[test]
    fn compute_append_to_visible_stats_clamps_future_timestamps_to_zero() {
        let visible_at = DateTime::parse_from_rfc3339("2026-02-17T00:00:10.000Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let raw_rows = vec![json!({"record_ts": "2026-02-17T00:00:20.000Z"})];

        let (p50, p95) = compute_append_to_visible_stats(&raw_rows, visible_at)
            .expect("expected percentile stats");

        assert_eq!(p50, 0);
        assert_eq!(p95, 0);
    }

    #[test]
    fn duration_from_config_seconds_clamps_to_minimum() {
        let duration = duration_from_config_seconds(0.001, 0.05, "ingest.flush_interval_seconds");
        assert_eq!(duration, Duration::from_millis(50));
    }

    #[test]
    fn duration_from_config_seconds_handles_non_finite_values() {
        let nan = duration_from_config_seconds(f64::NAN, 0.05, "ingest.flush_interval_seconds");
        let pos_inf =
            duration_from_config_seconds(f64::INFINITY, 0.05, "ingest.flush_interval_seconds");
        let neg_inf =
            duration_from_config_seconds(f64::NEG_INFINITY, 0.05, "ingest.flush_interval_seconds");

        assert_eq!(nan, Duration::from_millis(50));
        assert_eq!(pos_inf, Duration::from_millis(50));
        assert_eq!(neg_inf, Duration::from_millis(50));
    }
}
