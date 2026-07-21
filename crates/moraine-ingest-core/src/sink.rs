use crate::checkpoint::merge_checkpoint;
use crate::heartbeat::host_name;
use crate::model::{Checkpoint, CheckpointLifecycle, RowBatch};
use crate::publication::{
    AppendManifest, CheckpointTransition, FinalizeReplayOutcome, PublicationActor,
};
use crate::publication_identity::PublicationIdentity;
use crate::redaction::{RedactionAudit, SecretRedactor};
use crate::sources::shared::truncate_chars;
use crate::tee::{
    backend_sinks_json, filter_batch_for_backend, BackendSinkCell, BackendSinkStatus, ReplayFloor,
    SharedRouteResolver, StatusRegistry,
};
use crate::{
    DispatchState, Metrics, SinkMessage, WATCHER_BACKEND_MIXED, WATCHER_BACKEND_NATIVE,
    WATCHER_BACKEND_POLL,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use moraine_clickhouse::{is_oversized_json_each_row_insert_error, ClickHouseClient};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub(crate) const CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES: usize = 10 * 1024 * 1024;
const OVERSIZED_ROW_FRAGMENT_CHARS: usize = 20_000;
const SINK_JSON_OBJECT_TOO_LARGE: &str = "sink_json_object_too_large";
const MAX_SINK_ERROR_TEXT_CHARS: usize = 1_000;

const INGEST_ACK_TRACE_TARGET: &str = "moraine_ingest_ack";

/// A sink can flush a source row before the adapter's final checkpoint batch
/// arrives. Keep row-level quarantine state across those flushes so the later
/// checkpoint and publication barrier cannot make an incomplete generation
/// active.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SourceGenerationKey {
    source_name: String,
    source_file: String,
    source_generation: u64,
}

impl SourceGenerationKey {
    fn from_source(source: &IngestErrorSource) -> Option<Self> {
        if source.source_name.is_empty()
            || source.source_file.is_empty()
            || source.source_generation == 0
        {
            return None;
        }
        Some(Self {
            source_name: source.source_name.clone(),
            source_file: source.source_file.clone(),
            source_generation: source.source_generation,
        })
    }

    fn from_checkpoint(checkpoint: &Checkpoint) -> Self {
        Self {
            source_name: checkpoint.source_name.clone(),
            source_file: checkpoint.source_file.clone(),
            source_generation: u64::from(checkpoint.source_generation),
        }
    }

    fn same_source(&self, checkpoint: &Checkpoint) -> bool {
        self.source_name == checkpoint.source_name && self.source_file == checkpoint.source_file
    }
}

type QuarantinedGenerations = HashMap<SourceGenerationKey, usize>;
type EventOwnershipIndex = HashMap<(String, u64), Vec<IngestErrorSource>>;

#[derive(Default)]
struct QuarantineOwnership {
    by_event_uid: EventOwnershipIndex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct IngestAckObservation {
    batch_sequence: u64,
    event_identity_digests: Vec<String>,
    ack_monotonic_ns: u64,
}

struct IngestAckObserver {
    enabled: bool,
    next_batch_sequence: AtomicU64,
    clock: fn() -> Option<u64>,
    #[cfg(test)]
    captured: Mutex<Vec<IngestAckObservation>>,
}

impl IngestAckObserver {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            next_batch_sequence: AtomicU64::new(1),
            clock: monotonic_timestamp_ns,
            #[cfg(test)]
            captured: Mutex::new(Vec::new()),
        }
    }

    #[cfg(test)]
    fn with_clock(enabled: bool, clock: fn() -> Option<u64>) -> Self {
        Self {
            clock,
            ..Self::new(enabled)
        }
    }

    fn observe(&self, event_identity_digests: &[String]) {
        if !self.enabled || event_identity_digests.is_empty() {
            return;
        }
        let Some(ack_monotonic_ns) = (self.clock)() else {
            warn!("ingest acknowledgement observation skipped: monotonic clock unavailable");
            return;
        };
        let observation = IngestAckObservation {
            batch_sequence: self.next_batch_sequence.fetch_add(1, Ordering::Relaxed),
            event_identity_digests: event_identity_digests.to_vec(),
            ack_monotonic_ns,
        };
        let encoded_digests = serde_json::to_string(&observation.event_identity_digests)
            .expect("serializing string digests cannot fail");
        info!(
            target: INGEST_ACK_TRACE_TARGET,
            batch_sequence = observation.batch_sequence,
            event_identity_digests = encoded_digests.as_str(),
            ack_monotonic_ns = observation.ack_monotonic_ns,
            "ingest batch acknowledged"
        );
        #[cfg(test)]
        self.captured
            .lock()
            .expect("ack observation capture mutex poisoned")
            .push(observation);
    }

    #[cfg(test)]
    fn captured(&self) -> Vec<IngestAckObservation> {
        self.captured
            .lock()
            .expect("ack observation capture mutex poisoned")
            .clone()
    }
}

#[derive(Default)]
struct PendingAckBatch {
    event_identity_digests: Vec<String>,
    projection_session_ids: BTreeSet<String>,
}

impl PendingAckBatch {
    fn extend(&mut self, event_rows: &[Value]) {
        self.event_identity_digests
            .extend(event_rows.iter().filter_map(event_identity_digest));
    }

    fn clear(&mut self) {
        self.event_identity_digests.clear();
        self.projection_session_ids.clear();
    }

    fn extend_projection(&mut self, session_ids: &BTreeSet<String>) {
        self.projection_session_ids
            .extend(session_ids.iter().cloned());
    }
}

#[derive(Default)]
struct PendingFlush {
    raw_rows: Vec<Value>,
    event_rows: Vec<Value>,
    link_rows: Vec<Value>,
    tool_rows: Vec<Value>,
    error_rows: Vec<Value>,
    checkpoint_updates: HashMap<String, Checkpoint>,
    acknowledgements: PendingAckBatch,
    quarantined_generations: QuarantinedGenerations,
}

impl PendingFlush {
    fn extend(&mut self, batch: RowBatch) {
        self.raw_rows.extend(batch.raw_rows);
        self.event_rows.extend(batch.event_rows);
        self.link_rows.extend(batch.link_rows);
        self.tool_rows.extend(batch.tool_rows);
        self.error_rows.extend(batch.error_rows);
        if let Some(checkpoint) = batch.checkpoint {
            merge_checkpoint(&mut self.checkpoint_updates, checkpoint);
        }
    }

    fn row_count(&self) -> usize {
        self.raw_rows.len()
            + self.event_rows.len()
            + self.link_rows.len()
            + self.tool_rows.len()
            + self.error_rows.len()
    }

    fn has_data(&self) -> bool {
        self.row_count() != 0 || !self.checkpoint_updates.is_empty()
    }
}

struct FlushContext<'a> {
    clickhouse: &'a ClickHouseClient,
    checkpoints: &'a Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &'a Arc<Metrics>,
    checkpoint_cursor_columns: bool,
    checkpoint_host: &'a str,
    ack_observer: &'a IngestAckObserver,
    publication: Option<&'a PublicationActor>,
}

#[derive(Clone, Copy)]
struct FlushOptions {
    publication_ready: bool,
}

fn event_identity_digest(row: &Value) -> Option<String> {
    let event_uid = row.get("event_uid")?.as_str()?;
    let mut hasher = Sha256::new();
    hasher.update(event_uid.as_bytes());
    Some(format!("{:x}", hasher.finalize()))
}

fn monotonic_timestamp_ns() -> Option<u64> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `timestamp` points to a valid writable `timespec`; CLOCK_MONOTONIC
    // has no additional preconditions and is supported on Moraine's Unix hosts.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
        return None;
    }
    let seconds = u64::try_from(timestamp.tv_sec).ok()?;
    let nanoseconds = u64::try_from(timestamp.tv_nsec).ok()?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanoseconds))
}

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
        /// Whether `ingest_heartbeats` carries the `redactions_total` column
        /// (migration 022). Redaction still runs without this audit surface.
        redactions_column: bool,
        redactions: Arc<RedactionAudit>,
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
        redactor: Arc<SecretRedactor>,
        redactions: Arc<RedactionAudit>,
    },
}

fn publication_ready_for_role(role: &SinkRole) -> bool {
    match role {
        SinkRole::Default { .. } => true,
        SinkRole::Backend { cell, .. } => cell.publication_ready(),
    }
}

fn publication_ready_for_sink(role: &SinkRole, shared_legacy_ambiguous: bool) -> bool {
    !shared_legacy_ambiguous && publication_ready_for_role(role)
}

fn publication_host_for_role<'a>(role: &SinkRole, identity: &'a PublicationIdentity) -> &'a str {
    match role {
        SinkRole::Backend { .. } => identity.host_id(),
        SinkRole::Default { .. } => "",
    }
}

fn account_barrier_flush(flushed: bool, pending_batch_bytes: &mut usize) -> bool {
    if flushed {
        *pending_batch_bytes = 0;
    }
    flushed
}

#[derive(Clone, Debug)]
pub(crate) struct SinkAuthorConfig {
    author: String,
    raw_events_column: bool,
    events_column: bool,
}

impl SinkAuthorConfig {
    pub(crate) fn new(author: String, raw_events_column: bool, events_column: bool) -> Self {
        Self {
            author,
            raw_events_column,
            events_column,
        }
    }

    pub(crate) fn fully_supported(author: String) -> Self {
        Self::new(author, true, true)
    }

    fn apply_to_batch(&self, batch: &mut RowBatch) {
        batch.stamp_author(&self.author, self.raw_events_column, self.events_column);
    }
}

fn stamp_source_host(batch: &mut RowBatch, source_host: &str) {
    for row in batch
        .raw_rows
        .iter_mut()
        .chain(&mut batch.event_rows)
        .chain(&mut batch.link_rows)
        .chain(&mut batch.tool_rows)
        .chain(&mut batch.error_rows)
    {
        if let Some(object) = row.as_object_mut() {
            object.insert("source_host".to_string(), json!(source_host));
        }
    }
    batch.recompute_approx_bytes();
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_sink_task(
    config: moraine_config::AppConfig,
    clickhouse: ClickHouseClient,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<SinkMessage>,
    checkpoint_cursor_columns: bool,
    publication_identity: PublicationIdentity,
    author: SinkAuthorConfig,
    role: SinkRole,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut pending = PendingFlush::default();
        let mut pending_batch_bytes = 0usize;
        let ack_observer = IngestAckObserver::new(
            config.ingest.ack_observation && matches!(&role, SinkRole::Default { .. }),
        );

        // Mirror sinks share one ingest_checkpoints table per team backend,
        // so their rows are scoped per host (migration 018; guaranteed
        // present by the schema handshake). The default backend stays
        // single-writer and keeps writing host-less rows, which also keeps
        // it working before `moraine db migrate` adds the column locally.
        let checkpoint_host = publication_host_for_role(&role, &publication_identity).to_string();
        let publication_actor = PublicationActor::new(
            clickhouse.clone(),
            checkpoint_host.clone(),
            publication_identity.publisher_id().to_string(),
        );
        let default_publication_ready = matches!(&role, SinkRole::Default { .. });
        if !default_publication_ready {
            match publication_actor.block_orphaned_append_on_startup().await {
                Ok(true) => {
                    warn!(
                        "an unresolved shared-backend append manifest remains blocked after restart"
                    );
                }
                Ok(false) => {}
                Err(error) => {
                    let detail = format!(
                        "failed to repair shared-backend append publication control: {error}"
                    );
                    warn!("{detail}");
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = detail;
                    if let SinkRole::Backend { cell, .. } = &role {
                        cell.set_status(BackendSinkStatus::Unreachable);
                    }
                    return;
                }
            }
        }
        let shared_legacy_ambiguous = if default_publication_ready {
            false
        } else {
            match publication_actor.diagnose_shared_legacy_ambiguity().await {
                Ok(ambiguous) => {
                    if ambiguous {
                        warn!(
                            "shared backend contains hostless legacy events; publication remains fail-closed"
                        );
                    }
                    ambiguous
                }
                Err(error) => {
                    warn!("failed to prove shared legacy ownership; publication remains fail-closed: {error}");
                    true
                }
            }
        };
        if default_publication_ready {
            let repair_candidates = checkpoints
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            if let Err(error) = publication_actor
                .repair_ready_publications(repair_candidates)
                .await
            {
                warn!("startup publication repair failed closed: {error}");
                *metrics
                    .last_error
                    .lock()
                    .expect("metrics last_error mutex poisoned") = error.to_string();
            }
        }

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
        let flush_context = FlushContext {
            clickhouse: &clickhouse,
            checkpoints: &checkpoints,
            metrics: &metrics,
            checkpoint_cursor_columns,
            checkpoint_host: &checkpoint_host,
            ack_observer: &ack_observer,
            publication: Some(&publication_actor),
        };

        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);
        let mut throttling_flush_retries = false;

        loop {
            if throttling_flush_retries && pending.has_data() {
                let flush_ok = flush_pending_with_publication(
                    &flush_context,
                    &mut pending,
                    FlushOptions {
                        publication_ready: publication_ready_for_sink(
                            &role,
                            shared_legacy_ambiguous,
                        ),
                    },
                )
                .await;
                note_flush_outcome(&role, &checkpoints, flush_ok, rx.is_empty()).await;
                if flush_ok {
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
                            let mut batch = match &role {
                                SinkRole::Backend {
                                    cell,
                                    resolver,
                                    redactor,
                                    redactions,
                                    ..
                                } => {
                                    let mut resolver =
                                        resolver.lock().expect("route resolver mutex poisoned");
                                    let mut batch =
                                        filter_batch_for_backend(&batch, cell.name(), &mut resolver);
                                    let report = redactor.redact_batch(&mut batch);
                                    redactions.record(&report);
                                    batch
                                }
                                SinkRole::Default { .. } => batch,
                            };
                            stamp_source_host(&mut batch, &checkpoint_host);
                            author.apply_to_batch(&mut batch);
                            pending_batch_bytes =
                                pending_batch_bytes.saturating_add(batch.approx_bytes());
                            pending.extend(batch);

                            if pending.row_count() >= config.ingest.batch_size
                                || pending_batch_bytes >= config.ingest.max_batch_bytes.max(1)
                            {
                                let flush_ok = flush_pending_with_publication(
                                    &flush_context,
                                    &mut pending,
                                    FlushOptions {
                                        publication_ready: publication_ready_for_sink(
                                            &role,
                                            shared_legacy_ambiguous,
                                        ),
                                    },
                                ).await;
                                note_flush_outcome(&role, &checkpoints, flush_ok, rx.is_empty()).await;
                                if !flush_ok {
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
                        Some(SinkMessage::BeginReplay { mut transition, ack }) => {
                            let flushed = account_barrier_flush(
                                flush_for_barrier(
                                    &flush_context,
                                    &mut pending,
                                    FlushOptions {
                                        publication_ready: publication_ready_for_sink(
                                            &role,
                                            shared_legacy_ambiguous,
                                        ),
                                    },
                                )
                                .await,
                                &mut pending_batch_bytes,
                            );
                            let result = if !flushed {
                                throttling_flush_retries = true;
                                Err("failed to flush batches preceding begin-replay barrier".to_string())
                            } else {
                                match transition.validate_begin_replay() {
                                    Err(error) => Err(error.to_string()),
                                    Ok(()) => match publication_actor.persist_transition(&mut transition).await {
                                        Ok(barrier) => {
                                            merge_checkpoint(
                                                &mut *checkpoints.write().await,
                                                transition.checkpoint.clone(),
                                            );
                                            clear_quarantines_before_replay(
                                                &mut pending.quarantined_generations,
                                                &transition.checkpoint,
                                            );
                                            Ok(barrier)
                                        }
                                        Err(error) => Err(error.to_string()),
                                    },
                                }
                            };
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::FinalizeReplay { mut transition, ack }) => {
                            let flushed = account_barrier_flush(
                                flush_for_barrier(
                                    &flush_context,
                                    &mut pending,
                                    FlushOptions {
                                        publication_ready: publication_ready_for_sink(
                                            &role,
                                            shared_legacy_ambiguous,
                                        ),
                                    },
                                )
                                .await,
                                &mut pending_batch_bytes,
                            );
                            let result = if !flushed {
                                throttling_flush_retries = true;
                                Err("failed to flush replay batches before final publication".to_string())
                            } else if let Err(error) = validate_latest_replay_checkpoint(
                                &checkpoints,
                                &pending.quarantined_generations,
                                &transition,
                            )
                            .await
                            {
                                Err(error)
                            } else {
                                let publication_ready = publication_ready_for_sink(
                                    &role,
                                    shared_legacy_ambiguous,
                                );
                                transition.set_backend_caught_up(publication_ready);
                                if !publication_ready {
                                    let staged = match publication_actor
                                        .persist_staged_mirror_final(&mut transition)
                                        .await
                                    {
                                        Ok(_) => {
                                            merge_checkpoint(
                                                &mut *checkpoints.write().await,
                                                transition.checkpoint.clone(),
                                            );
                                            clear_quarantine_for_generation(
                                                &mut pending.quarantined_generations,
                                                &transition.checkpoint,
                                            );
                                            Ok(FinalizeReplayOutcome::StagedForMirror)
                                        }
                                        Err(error) => Err(error.to_string()),
                                    };
                                    let _ = ack.send(staged);
                                    continue;
                                }
                                match publication_actor.publish_final(&mut transition).await {
                                    Ok(publication) => {
                                        merge_checkpoint(
                                            &mut *checkpoints.write().await,
                                            transition.checkpoint.clone(),
                                        );
                                        clear_quarantine_for_generation(
                                            &mut pending.quarantined_generations,
                                            &transition.checkpoint,
                                        );
                                        Ok(FinalizeReplayOutcome::Published(publication))
                                    }
                                    Err(error) => Err(error.to_string()),
                                }
                            };
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::BlockReplay { mut transition, ack }) => {
                            let flushed = account_barrier_flush(
                                flush_for_barrier(
                                    &flush_context,
                                    &mut pending,
                                    FlushOptions {
                                        publication_ready: publication_ready_for_sink(
                                            &role,
                                            shared_legacy_ambiguous,
                                        ),
                                    },
                                )
                                .await,
                                &mut pending_batch_bytes,
                            );
                            let result = if !flushed {
                                throttling_flush_retries = true;
                                Err("failed to flush batches preceding replay-block barrier".to_string())
                            } else if transition.checkpoint.block_reason.is_empty() {
                                Err("blocked replay transition requires a block reason".to_string())
                            } else {
                                transition
                                    .checkpoint
                                    .set_lifecycle(CheckpointLifecycle::Error);
                                match publication_actor.persist_transition(&mut transition).await {
                                    Ok(barrier) => {
                                        merge_checkpoint(
                                            &mut *checkpoints.write().await,
                                            transition.checkpoint.clone(),
                                        );
                                        Ok(barrier)
                                    }
                                    Err(error) => Err(error.to_string()),
                                }
                            };
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::MirrorCaughtUp { mut transition, ack }) => {
                            let flushed = account_barrier_flush(
                                flush_for_barrier(
                                    &flush_context,
                                    &mut pending,
                                    FlushOptions {
                                        publication_ready: publication_ready_for_sink(
                                            &role,
                                            shared_legacy_ambiguous,
                                        ),
                                    },
                                )
                                .await,
                                &mut pending_batch_bytes,
                            );
                            let mut result = if flushed && shared_legacy_ambiguous {
                                Err(
                                    "shared legacy source ownership is ambiguous; publication remains disabled"
                                        .to_string(),
                                )
                            } else if flushed {
                                let key = crate::checkpoint::checkpoint_key(
                                    &transition.source.source_name,
                                    &transition.source.source_file,
                                );
                                let latest = checkpoints.read().await.get(&key).cloned();
                                let latest_transition = latest
                                    .map(|mut latest| {
                                    // The barrier may have waited behind replay
                                    // batches. Persist readiness against the
                                    // cursor that actually flushed, never the
                                    // placeholder captured by the supervisor.
                                    latest.checkpoint_revision = 0;
                                    latest.operation_id.clear();
                                        CheckpointTransition::try_from_checkpoint(latest)
                                    })
                                    .transpose()
                                    .map_err(|error| error.to_string());
                                match latest_transition {
                                    Err(error) => Err(error),
                                    Ok(latest) => {
                                        if let Some(latest) = latest {
                                            transition = latest;
                                        }
                                        transition.set_backend_caught_up(true);
                                        publication_actor
                                            .persist_mirror_caught_up(&mut transition)
                                            .await
                                            .map_err(|error| error.to_string())
                                    }
                                }
                            } else {
                                throttling_flush_retries = true;
                                Err("failed to flush batches preceding mirror catch-up barrier".to_string())
                            };
                            if result.is_ok() {
                                merge_checkpoint(
                                    &mut *checkpoints.write().await,
                                    transition.checkpoint.clone(),
                                );
                                let repair_candidates =
                                    checkpoints.read().await.values().cloned().collect::<Vec<_>>();
                                if let Err(error) = publication_actor
                                    .repair_ready_publications(repair_candidates)
                                    .await
                                {
                                    let failure = format!(
                                        "mirror catch-up is durable but publication repair failed: {error}"
                                    );
                                    result = match publication_actor
                                        .record_publication_repair_failure(&transition)
                                        .await
                                    {
                                        Ok(()) => Err(failure),
                                        Err(diagnostic_error) => Err(format!(
                                            "{failure}; failed to persist publication-repair diagnostic: {diagnostic_error}"
                                        )),
                                    };
                                }
                            }
                            let _ = ack.send(result);
                        }
                        None => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if pending.has_data() {
                        let flush_ok = flush_pending_with_publication(
                            &flush_context,
                            &mut pending,
                            FlushOptions {
                                publication_ready: publication_ready_for_sink(
                                    &role,
                                    shared_legacy_ambiguous,
                                ),
                            },
                        ).await;
                        note_flush_outcome(&role, &checkpoints, flush_ok, rx.is_empty()).await;
                        if !flush_ok {
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

        if pending.has_data() {
            let flush_ok = flush_pending_with_publication(
                &flush_context,
                &mut pending,
                FlushOptions {
                    publication_ready: publication_ready_for_sink(&role, shared_legacy_ambiguous),
                },
            )
            .await;
            note_flush_outcome(&role, &checkpoints, flush_ok, true).await;
        }
    })
}

fn direct_row_source(row: &Value) -> Option<IngestErrorSource> {
    let source = ingest_error_source_from_row(row);
    SourceGenerationKey::from_source(&source).map(|_| source)
}

fn push_event_ownership_candidate(
    index: &mut EventOwnershipIndex,
    event_uid: &str,
    event_version: u64,
    source: IngestErrorSource,
) {
    if event_uid.is_empty() || event_version == 0 {
        return;
    }
    let source_key = SourceGenerationKey::from_source(&source)
        .expect("ownership candidates always have a generation key");
    let candidates = index
        .entry((event_uid.to_string(), event_version))
        .or_default();
    if !candidates
        .iter()
        .any(|candidate| SourceGenerationKey::from_source(candidate).as_ref() == Some(&source_key))
    {
        candidates.push(source);
    }
}

fn quarantine_ownership(event_rows: &[Value]) -> QuarantineOwnership {
    let mut ownership = QuarantineOwnership::default();
    // A derived row is causally owned by the exact event version selected by
    // the live views, not merely by a reusable event UID.
    for row in event_rows {
        let Some(source) = direct_row_source(row) else {
            continue;
        };
        let Some(event_uid) = row.get("event_uid").and_then(Value::as_str) else {
            continue;
        };
        let event_version = row_u64(row, "event_version").unwrap_or_default();
        push_event_ownership_candidate(
            &mut ownership.by_event_uid,
            event_uid,
            event_version,
            source,
        );
    }
    ownership
}

fn quarantine_sources_for_row(
    row: &Value,
    ownership: &QuarantineOwnership,
) -> Vec<IngestErrorSource> {
    if let Some(source) = direct_row_source(row) {
        return vec![source];
    }
    if let (Some(event_uid), Some(source_event_version)) = (
        row.get("event_uid").and_then(Value::as_str),
        row_u64(row, "source_event_version").filter(|version| *version != 0),
    ) {
        if let Some(sources) = ownership
            .by_event_uid
            .get(&(event_uid.to_string(), source_event_version))
        {
            return sources.clone();
        }
    }
    Vec::new()
}

#[derive(Default)]
struct QuarantineSummary {
    rows: usize,
    generations: QuarantinedGenerations,
}

impl QuarantineSummary {
    fn merge(&mut self, other: Self) {
        self.rows = self.rows.saturating_add(other.rows);
        for (source, rows) in other.generations {
            self.generations
                .entry(source)
                .and_modify(|total| *total = total.saturating_add(rows))
                .or_insert(rows);
        }
    }
}

struct OversizedRowPlan {
    index: usize,
    row_bytes: usize,
    sources: Vec<IngestErrorSource>,
}

fn plan_oversized_rows(
    table: &str,
    rows: &[Value],
    ownership: &QuarantineOwnership,
) -> anyhow::Result<Vec<OversizedRowPlan>> {
    let mut plan = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let row_bytes = serde_json::to_vec(&row)
            .with_context(|| format!("failed to encode {table} row for oversized-row guard"))?
            .len();
        if row_bytes > CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES {
            let sources = quarantine_sources_for_row(row, ownership);
            if sources.is_empty() {
                let event_uid = row
                    .get("event_uid")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                anyhow::bail!(
                    "cannot quarantine oversized {table} row without an exact source generation owner (event_uid={event_uid:?})"
                );
            }
            plan.push(OversizedRowPlan {
                index,
                row_bytes,
                sources,
            });
        }
    }
    Ok(plan)
}

fn apply_oversized_row_plan(
    table: &str,
    rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    plan: Vec<OversizedRowPlan>,
) -> QuarantineSummary {
    let mut kept = Vec::with_capacity(rows.len().saturating_sub(plan.len()));
    let mut quarantined = QuarantineSummary::default();
    let mut plan = plan.into_iter().peekable();

    for (index, row) in std::mem::take(rows).into_iter().enumerate() {
        let Some(oversized) = plan.next_if(|entry| entry.index == index) else {
            kept.push(row);
            continue;
        };

        for source in &oversized.sources {
            error_rows.push(oversized_row_error(
                table,
                &row,
                oversized.row_bytes,
                Some(source),
            ));
            let source = SourceGenerationKey::from_source(source)
                .expect("planned quarantine sources always have a generation key");
            quarantined
                .generations
                .entry(source)
                .and_modify(|total| *total = total.saturating_add(1))
                .or_insert(1);
        }
        quarantined.rows = quarantined.rows.saturating_add(1);
    }

    *rows = kept;
    quarantined
}

#[allow(clippy::too_many_arguments)]
fn quarantine_oversized_pending_rows(
    raw_rows: &mut Vec<Value>,
    event_rows: &mut Vec<Value>,
    link_rows: &mut Vec<Value>,
    tool_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
) -> anyhow::Result<QuarantineSummary> {
    let ownership = quarantine_ownership(event_rows);

    // Resolve every oversized row before mutating any table. An unscoped row
    // stays pending and blocks the flush without contaminating other sources.
    let raw_plan = plan_oversized_rows("raw_events", raw_rows, &ownership)?;
    let event_plan = plan_oversized_rows("events", event_rows, &ownership)?;
    let link_plan = plan_oversized_rows("event_links", link_rows, &ownership)?;
    let tool_plan = plan_oversized_rows("tool_io", tool_rows, &ownership)?;
    let error_plan = plan_oversized_rows("ingest_errors", error_rows, &ownership)?;

    let mut summary = QuarantineSummary::default();
    let mut compacted_error_rows = Vec::new();
    summary.merge(apply_oversized_row_plan(
        "ingest_errors",
        error_rows,
        &mut compacted_error_rows,
        error_plan,
    ));
    error_rows.extend(compacted_error_rows);
    summary.merge(apply_oversized_row_plan(
        "raw_events",
        raw_rows,
        error_rows,
        raw_plan,
    ));
    summary.merge(apply_oversized_row_plan(
        "events", event_rows, error_rows, event_plan,
    ));
    summary.merge(apply_oversized_row_plan(
        "event_links",
        link_rows,
        error_rows,
        link_plan,
    ));
    summary.merge(apply_oversized_row_plan(
        "tool_io", tool_rows, error_rows, tool_plan,
    ));
    Ok(summary)
}

fn quarantine_reason(rows: usize) -> String {
    format!("{rows} oversized row(s) quarantined during generation flush")
}

fn remember_quarantined_generations(
    state: &mut QuarantinedGenerations,
    quarantined: QuarantinedGenerations,
) {
    for (source, rows) in quarantined {
        state
            .entry(source)
            .and_modify(|total| *total = total.saturating_add(rows))
            .or_insert(rows);
    }
}

fn apply_quarantine_to_checkpoints(
    state: &QuarantinedGenerations,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
) {
    for checkpoint in checkpoint_updates.values_mut() {
        let source = SourceGenerationKey::from_checkpoint(checkpoint);
        let Some(rows) = state.get(&source).copied() else {
            continue;
        };
        checkpoint.set_lifecycle(CheckpointLifecycle::Error);
        checkpoint.final_scan_complete = false;
        checkpoint.compatibility_prepared = false;
        checkpoint.backend_caught_up = false;
        checkpoint.block_reason = quarantine_reason(rows);
    }
}

fn clear_quarantines_before_replay(state: &mut QuarantinedGenerations, checkpoint: &Checkpoint) {
    let generation = u64::from(checkpoint.source_generation);
    state.retain(|source, _| {
        !(source.same_source(checkpoint) && source.source_generation <= generation)
    });
}

fn clear_quarantine_for_generation(state: &mut QuarantinedGenerations, checkpoint: &Checkpoint) {
    state.remove(&SourceGenerationKey::from_checkpoint(checkpoint));
}

async fn validate_latest_replay_checkpoint(
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    quarantined_generations: &QuarantinedGenerations,
    transition: &CheckpointTransition,
) -> std::result::Result<(), String> {
    let source = SourceGenerationKey::from_checkpoint(&transition.checkpoint);
    if let Some(rows) = quarantined_generations.get(&source) {
        return Err(format!(
            "final publication rejected: {}",
            quarantine_reason(*rows)
        ));
    }

    let key = crate::checkpoint::checkpoint_key(
        &transition.source.source_name,
        &transition.source.source_file,
    );
    let state = checkpoints.read().await;
    let Some(latest) = state.get(&key) else {
        return Err("final publication rejected: no durable replay checkpoint".to_string());
    };

    let latest_lifecycle = latest.lifecycle().map_err(|error| {
        format!("final publication rejected: invalid durable checkpoint: {error}")
    })?;
    if latest_lifecycle == CheckpointLifecycle::Error || !latest.block_reason.is_empty() {
        let reason = if latest.block_reason.is_empty() {
            "latest replay checkpoint is in error state"
        } else {
            latest.block_reason.as_str()
        };
        return Err(format!("final publication rejected: {reason}"));
    }
    if !matches!(
        latest_lifecycle,
        CheckpointLifecycle::Replaying | CheckpointLifecycle::Active
    ) {
        return Err(format!(
            "final publication rejected: latest replay checkpoint has {} lifecycle",
            latest_lifecycle
        ));
    }

    let candidate = &transition.checkpoint;
    if latest.source_generation != candidate.source_generation
        || latest.source_inode != candidate.source_inode
        || latest.last_offset != candidate.last_offset
        || latest.last_line_no != candidate.last_line_no
        || latest.cursor_json != candidate.cursor_json
        || latest.source_fingerprint != candidate.source_fingerprint
        || latest.schema_fingerprint != candidate.schema_fingerprint
        || latest.scan_inode != candidate.scan_inode
        || latest.scan_boundary != candidate.scan_boundary
        || latest.policy_fingerprint != candidate.policy_fingerprint
    {
        return Err(
            "final publication rejected: latest durable replay checkpoint does not match candidate"
                .to_string(),
        );
    }
    if latest_lifecycle == CheckpointLifecycle::Active && !latest.final_scan_complete {
        return Err(
            "final publication rejected: latest active checkpoint is not scan-complete".to_string(),
        );
    }
    Ok(())
}

fn oversized_row_error(
    table: &str,
    row: &Value,
    row_bytes: usize,
    source: Option<&IngestErrorSource>,
) -> Value {
    let fallback_source;
    let source = match source {
        Some(source) => source,
        None => {
            fallback_source = ingest_error_source_from_row(row);
            &fallback_source
        }
    };
    json!({
        "source_host": row.get("source_host").and_then(Value::as_str).unwrap_or_default(),
        "source_name": source.source_name,
        "harness": source.harness,
        "source_file": source.source_file,
        "source_inode": source.source_inode,
        "source_generation": source.source_generation,
        "source_line_no": source.source_line_no,
        "source_offset": source.source_offset,
        "error_kind": "oversized_json_row",
        "error_text": format!(
            "skipped {table} row: serialized JSONEachRow object is {row_bytes} bytes, above ClickHouse limit of {CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES} bytes"
        ),
        "raw_fragment": oversized_row_fragment(row),
    })
}

fn parse_source_ref(row: &Value) -> Option<(String, u64, u64)> {
    let source_ref = row.get("source_ref").and_then(Value::as_str)?;
    let mut parts = source_ref.rsplitn(3, ':');
    let line_no = parts.next()?.parse::<u64>().ok()?;
    let source_generation = parts.next()?.parse::<u64>().ok()?;
    let source_file = parts.next()?.to_string();
    Some((source_file, source_generation, line_no))
}

fn row_str(row: &Value, key: &str) -> Option<String> {
    row.get(key).and_then(Value::as_str).map(ToOwned::to_owned)
}

fn row_u64(row: &Value, key: &str) -> Option<u64> {
    row.get(key).and_then(Value::as_u64)
}

fn oversized_row_fragment(row: &Value) -> String {
    for key in [
        "raw_json",
        "payload_json",
        "output_json",
        "input_json",
        "output_text",
        "text_content",
        "metadata_json",
    ] {
        if let Some(value) = row
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            return truncate_chars(value, OVERSIZED_ROW_FRAGMENT_CHARS);
        }
    }

    serde_json::to_string(row)
        .map(|value| truncate_chars(&value, OVERSIZED_ROW_FRAGMENT_CHARS))
        .unwrap_or_default()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SinkStage {
    RawEvents,
    Events,
    McpOpenProjection,
    EventLinks,
    ToolIo,
    IngestErrors,
    IngestCheckpoints,
    AppendControl,
}

impl SinkStage {
    fn table(self) -> &'static str {
        match self {
            Self::RawEvents => "raw_events",
            Self::Events => "events",
            Self::McpOpenProjection => "mcp_open_projection",
            Self::EventLinks => "event_links",
            Self::ToolIo => "tool_io",
            Self::IngestErrors => "ingest_errors",
            Self::IngestCheckpoints => "ingest_checkpoints",
            Self::AppendControl => "ingest_append_control",
        }
    }
}

struct SinkFlushFailure {
    stage: SinkStage,
    error: anyhow::Error,
}

impl SinkFlushFailure {
    fn new(stage: SinkStage, error: anyhow::Error) -> Self {
        Self { stage, error }
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
struct IngestErrorSource {
    source_name: String,
    harness: String,
    source_file: String,
    source_inode: u64,
    source_generation: u64,
    source_line_no: u64,
    source_offset: u64,
}

#[derive(Clone, Copy)]
struct PendingSinkData<'a> {
    raw_rows: &'a [Value],
    event_rows: &'a [Value],
    link_rows: &'a [Value],
    tool_rows: &'a [Value],
    error_rows: &'a [Value],
    checkpoint_updates: &'a HashMap<String, Checkpoint>,
}

impl PendingSinkData<'_> {
    fn source_for(self, stage: SinkStage) -> (IngestErrorSource, &'static str) {
        let stage_rows = match stage {
            SinkStage::RawEvents => self.raw_rows,
            SinkStage::Events => self.event_rows,
            SinkStage::McpOpenProjection => self.event_rows,
            SinkStage::EventLinks => self.link_rows,
            SinkStage::ToolIo => self.tool_rows,
            SinkStage::IngestErrors => self.error_rows,
            SinkStage::IngestCheckpoints => &[],
            SinkStage::AppendControl => &[],
        };

        if let [row] = stage_rows {
            return (ingest_error_source_from_row(row), "row");
        }

        if let Some(source) = self
            .checkpoint_updates
            .values()
            .next()
            .map(ingest_error_source_from_checkpoint)
        {
            return (source, "checkpoint");
        }

        (IngestErrorSource::default(), "batch")
    }

    fn stage_len(self, stage: SinkStage) -> usize {
        match stage {
            SinkStage::RawEvents => self.raw_rows.len(),
            SinkStage::Events => self.event_rows.len(),
            SinkStage::McpOpenProjection => self.event_rows.len(),
            SinkStage::EventLinks => self.link_rows.len(),
            SinkStage::ToolIo => self.tool_rows.len(),
            SinkStage::IngestErrors => self.error_rows.len(),
            SinkStage::IngestCheckpoints => self.checkpoint_updates.len(),
            SinkStage::AppendControl => self.checkpoint_updates.len(),
        }
    }

    fn total_len(self) -> usize {
        self.raw_rows.len()
            + self.event_rows.len()
            + self.link_rows.len()
            + self.tool_rows.len()
            + self.error_rows.len()
            + self.checkpoint_updates.len()
    }
}

fn ingest_error_source_from_row(row: &Value) -> IngestErrorSource {
    let source_ref = parse_source_ref(row);
    IngestErrorSource {
        source_name: row_str(row, "source_name").unwrap_or_default(),
        harness: row_str(row, "harness").unwrap_or_default(),
        source_file: row_str(row, "source_file")
            .or_else(|| source_ref.as_ref().map(|parts| parts.0.clone()))
            .unwrap_or_default(),
        source_inode: row_u64(row, "source_inode").unwrap_or_default(),
        source_generation: row_u64(row, "source_generation")
            .or_else(|| source_ref.as_ref().map(|parts| parts.1))
            .unwrap_or_default(),
        source_line_no: row_u64(row, "source_line_no")
            .or_else(|| source_ref.as_ref().map(|parts| parts.2))
            .unwrap_or_default(),
        source_offset: row_u64(row, "source_offset").unwrap_or_default(),
    }
}

fn ingest_error_source_from_checkpoint(checkpoint: &Checkpoint) -> IngestErrorSource {
    IngestErrorSource {
        source_name: checkpoint.source_name.clone(),
        source_file: checkpoint.source_file.clone(),
        source_inode: checkpoint.source_inode,
        source_generation: checkpoint.source_generation as u64,
        source_line_no: checkpoint.last_line_no,
        source_offset: checkpoint.last_offset,
        ..Default::default()
    }
}

fn non_retryable_sink_error_row(
    stage: SinkStage,
    error_text: &str,
    pending: PendingSinkData<'_>,
) -> Value {
    let (source, source_scope) = pending.source_for(stage);
    let stage_pending_rows = pending.stage_len(stage);
    let total_pending_rows = pending.total_len();

    json!({
        "source_name": source.source_name,
        "harness": source.harness,
        "source_file": source.source_file,
        "source_inode": source.source_inode,
        "source_generation": source.source_generation,
        "source_line_no": source.source_line_no,
        "source_offset": source.source_offset,
        "error_kind": SINK_JSON_OBJECT_TOO_LARGE,
        "error_text": truncate_chars(error_text, MAX_SINK_ERROR_TEXT_CHARS),
        "raw_fragment": format!(
            "source_scope={source_scope}; sink_stage={}; stage_pending_rows={stage_pending_rows}; total_pending_rows={total_pending_rows}; checkpoint_updates={}; original_payload_omitted=true",
            stage.table(),
            pending.checkpoint_updates.len()
        ),
    })
}

async fn record_non_retryable_sink_error(
    clickhouse: &ClickHouseClient,
    metrics: &Arc<Metrics>,
    stage: SinkStage,
    error_text: &str,
    pending: PendingSinkData<'_>,
) -> anyhow::Result<()> {
    let row = non_retryable_sink_error_row(stage, error_text, pending);

    clickhouse
        .insert_json_rows("ingest_errors", &[row])
        .await
        .with_context(|| {
            format!(
                "failed to record non-retryable sink error for {}",
                stage.table()
            )
        })?;
    metrics.err_rows_written.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn clear_pending_sink_data(
    raw_rows: &mut Vec<Value>,
    event_rows: &mut Vec<Value>,
    link_rows: &mut Vec<Value>,
    tool_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
) {
    raw_rows.clear();
    event_rows.clear();
    link_rows.clear();
    tool_rows.clear();
    error_rows.clear();
    checkpoint_updates.clear();
}

async fn commit_checkpoint_updates(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    checkpoint_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
    publication: Option<&PublicationActor>,
    publication_ready: bool,
) -> anyhow::Result<()> {
    if checkpoint_rows.is_empty() {
        return Ok(());
    }

    let mut committed = Vec::with_capacity(checkpoint_updates.len());
    for checkpoint in checkpoint_updates.values() {
        let mut transition = CheckpointTransition::try_from_checkpoint(checkpoint.clone())?;
        let initial_local = transition.checkpoint.lifecycle()? == CheckpointLifecycle::Active
            && transition.checkpoint.source_generation == 1
            && checkpoint_rows.iter().all(|row| {
                row.get("host")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .is_empty()
            });
        if initial_local {
            // Empty host is the local backend and is already caught up with
            // itself. A mirror must receive the ordered catch-up barrier.
            transition.set_backend_caught_up(true);
        } else if !publication_ready {
            // Adapter checkpoints describe source-scan completeness, not
            // mirror coverage. Until the ordered catch-up barrier is durable,
            // a backend transition must stay staged even if the adapter set
            // its ordinary generation-1 readiness defaults to true.
            transition.set_backend_caught_up(false);
        }
        if let Some(publication) = publication {
            if !initial_local {
                publication.persist_transition(&mut transition).await?;
            }
        }
        committed.push(transition);
    }

    clickhouse
        .insert_json_rows_sync("ingest_checkpoints", checkpoint_rows)
        .await
        .context("failed to insert ingest checkpoint rows")?;

    if let Some(publication) = publication {
        for transition in &mut committed {
            if transition.checkpoint.backend_caught_up
                && transition.checkpoint.source_generation == 1
            {
                publication.publish_initial_if_absent(transition).await?;
            }
        }
    }

    // Offset-monotone merge, not a blind insert: a backend map has two
    // producers (live router batches and catch-up replay), so an out-of-order
    // flush must never regress what the map already committed.
    let mut state = checkpoints.write().await;
    for transition in committed {
        merge_checkpoint(&mut state, transition.checkpoint);
    }
    Ok(())
}

/// Default-role only: mirror sinks write rows into databases this host does
/// not own, so the ingest heartbeat (including per-backend mirror status)
/// always lands in the default backend.
async fn emit_heartbeat(clickhouse: &ClickHouseClient, metrics: &Arc<Metrics>, role: &SinkRole) {
    let SinkRole::Default {
        dispatch,
        backends,
        backend_sinks_column,
        redactions_column,
        redactions,
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

    if *redactions_column {
        let counts = redactions.snapshot();
        if !counts.is_empty() {
            if let Some(obj) = heartbeat.as_object_mut() {
                let counts_json = serde_json::to_string(&counts).unwrap_or_default();
                obj.insert("redactions_total".to_string(), Value::String(counts_json));
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

#[cfg(test)]
async fn flush_pending(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    pending: &mut PendingFlush,
) -> bool {
    let observer = IngestAckObserver::new(false);
    let context = FlushContext {
        clickhouse,
        checkpoints,
        metrics,
        checkpoint_cursor_columns: true,
        checkpoint_host: "",
        ack_observer: &observer,
        publication: None,
    };
    flush_pending_inner(
        &context,
        pending,
        FlushOptions {
            publication_ready: true,
        },
    )
    .await
}

#[cfg(test)]
async fn flush_pending_with_ack(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    pending: &mut PendingFlush,
    ack_observer: &IngestAckObserver,
) -> bool {
    let context = FlushContext {
        clickhouse,
        checkpoints,
        metrics,
        checkpoint_cursor_columns: true,
        checkpoint_host: "",
        ack_observer,
        publication: None,
    };
    flush_pending_inner(
        &context,
        pending,
        FlushOptions {
            publication_ready: true,
        },
    )
    .await
}

async fn flush_pending_with_publication(
    context: &FlushContext<'_>,
    pending: &mut PendingFlush,
    options: FlushOptions,
) -> bool {
    debug_assert!(context.publication.is_some());
    flush_pending_inner(context, pending, options).await
}

async fn flush_for_barrier(
    context: &FlushContext<'_>,
    pending: &mut PendingFlush,
    options: FlushOptions,
) -> bool {
    if !pending.has_data() {
        return true;
    }
    flush_pending_with_publication(context, pending, options).await
}

async fn flush_pending_inner(
    context: &FlushContext<'_>,
    pending: &mut PendingFlush,
    options: FlushOptions,
) -> bool {
    let clickhouse = context.clickhouse;
    let checkpoints = context.checkpoints;
    let metrics = context.metrics;
    let checkpoint_cursor_columns = context.checkpoint_cursor_columns;
    let checkpoint_host = context.checkpoint_host;
    let ack_observer = context.ack_observer;
    let publication = context.publication;
    let publication_ready = options.publication_ready;
    let PendingFlush {
        raw_rows,
        event_rows,
        link_rows,
        tool_rows,
        error_rows,
        checkpoint_updates,
        acknowledgements: pending_ack,
        quarantined_generations,
    } = pending;
    let started = Instant::now();

    // An oversized row may have been drained into an earlier checkpoint-less
    // chunk. Apply its fail-closed state before append/publication
    // classification sees the adapter's later checkpoint.
    apply_quarantine_to_checkpoints(quarantined_generations, checkpoint_updates);

    // Size/ownership validation must precede append classification. Otherwise
    // an unowned row could fail after `begin_append` and leave unrelated
    // readers behind a preparing fence.
    let quarantined = match quarantine_oversized_pending_rows(
        raw_rows, event_rows, link_rows, tool_rows, error_rows,
    ) {
        Ok(quarantined) => quarantined,
        Err(exc) => {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = exc.to_string();
            warn!("flush failed during oversized-row guard: {exc}");
            return false;
        }
    };
    remember_quarantined_generations(quarantined_generations, quarantined.generations);
    apply_quarantine_to_checkpoints(quarantined_generations, checkpoint_updates);
    let blocks_pending_generation = checkpoint_updates.values().any(|checkpoint| {
        quarantined_generations.contains_key(&SourceGenerationKey::from_checkpoint(checkpoint))
    });
    if quarantined.rows > 0 {
        warn!(
            rows = quarantined.rows,
            max_bytes = CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES,
            "quarantined oversized rows before ClickHouse insert"
        );
    }

    let mut append_manifest = AppendManifest::from_pending(
        checkpoint_host,
        raw_rows,
        event_rows,
        link_rows,
        tool_rows,
        error_rows,
        checkpoint_updates.values().cloned(),
    );
    let mut append_fence = None;
    let projection_all_sources = publication.is_none() || checkpoint_updates.is_empty();
    let mut live_projection_sources = BTreeSet::<(String, String, String, u32)>::new();
    if let Some(publication) = publication {
        let mut has_live_append = false;
        for checkpoint in checkpoint_updates.values() {
            let transition = match CheckpointTransition::try_from_checkpoint(checkpoint.clone()) {
                Ok(transition) => transition,
                Err(error) => {
                    metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = error.to_string();
                    warn!("refusing to flush checkpoint with invalid lifecycle: {error}");
                    return false;
                }
            };
            let published = match publication
                .has_published_generation(
                    &transition.source,
                    transition.checkpoint.source_generation,
                )
                .await
            {
                Ok(published) => published,
                Err(error) => {
                    metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = error.to_string();
                    warn!("failed to inspect current source head before flush: {error}");
                    return false;
                }
            };
            let active = match transition.checkpoint.lifecycle() {
                Ok(lifecycle) => {
                    lifecycle == CheckpointLifecycle::Active
                        && transition.checkpoint.block_reason.is_empty()
                }
                Err(error) => {
                    metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = error.to_string();
                    warn!("refusing to flush checkpoint with invalid lifecycle: {error}");
                    return false;
                }
            };
            if active && published {
                live_projection_sources.insert((
                    checkpoint_host.to_string(),
                    transition.source.source_name.clone(),
                    transition.source.source_file.clone(),
                    transition.checkpoint.source_generation,
                ));
            }
            has_live_append |= active && published;
        }
        if has_live_append {
            if let Err(error) = publication.prove_insert_only(&mut append_manifest).await {
                // Classification is an optimization only. Query failure is
                // fail-closed to the backend-wide hard fence, not data loss.
                append_manifest.insert_only = false;
                warn!("append insert-only preflight failed; using hard fence: {error}");
            }
            append_fence = match publication.begin_append(&append_manifest).await {
                Ok(fence) => fence,
                Err(error) => {
                    metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = error.to_string();
                    warn!("failed to establish append cache fence: {error}");
                    return false;
                }
            };
            if let Some((batch_id, cache_epoch)) = append_fence.as_ref() {
                let Some(target_epoch) = cache_epoch.checked_add(1) else {
                    metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") =
                        "append cache epoch exhausted".to_string();
                    return false;
                };
                for checkpoint in checkpoint_updates.values_mut() {
                    checkpoint.append_batch_id = batch_id.clone();
                    checkpoint.cache_epoch = target_epoch;
                }
            }
        }
    }

    for checkpoint in checkpoint_updates.values() {
        if let Err(error) = checkpoint.lifecycle() {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = error.to_string();
            warn!("refusing to flush checkpoint with invalid lifecycle: {error}");
            return false;
        }
    }

    let mut checkpoint_rows: Vec<Value> = checkpoint_updates
        .values()
        .map(|cp| {
            let lifecycle = cp
                .lifecycle()
                .expect("checkpoint lifecycles were validated before serialization");
            let mut row = json!({
                "source_name": cp.source_name,
                "source_file": cp.source_file,
                "source_inode": cp.source_inode,
                "source_generation": cp.source_generation,
                "last_offset": cp.last_offset,
                "last_line_no": cp.last_line_no,
                "status": lifecycle.as_str(),
            });
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
            if !checkpoint_host.is_empty() {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("host".to_string(), json!(checkpoint_host));
                }
            }
            row
        })
        .collect();

    let projected_session_ids = event_rows
        .iter()
        .filter(|row| {
            projection_all_sources
                || live_projection_sources.contains(&(
                    row.get("source_host")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    row.get("source_name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    row.get("source_file")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    row.get("source_generation")
                        .and_then(Value::as_u64)
                        .and_then(|value| u32::try_from(value).ok())
                        .unwrap_or_default(),
                ))
        })
        .filter_map(|row| {
            row.get("session_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|session_id| !session_id.is_empty())
        .collect::<BTreeSet<_>>();

    let flush_result = async {
        if !raw_rows.is_empty() {
            let insert = if publication.is_some() {
                clickhouse
                    .insert_json_rows_sync("raw_events", raw_rows)
                    .await
            } else {
                clickhouse.insert_json_rows("raw_events", raw_rows).await
            };
            insert.map_err(|error| SinkFlushFailure::new(SinkStage::RawEvents, error))?;
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
                .insert_json_rows_sync("events", event_rows)
                .await
                .map_err(|error| SinkFlushFailure::new(SinkStage::Events, error))?;
            metrics
                .event_rows_written
                .fetch_add(event_rows.len() as u64, Ordering::Relaxed);
            if ack_observer.enabled {
                pending_ack.extend(event_rows);
            }
            pending_ack.extend_projection(&projected_session_ids);
            event_rows.clear();
        }

        if !link_rows.is_empty() {
            let insert = if publication.is_some() {
                clickhouse
                    .insert_json_rows_sync("event_links", link_rows)
                    .await
            } else {
                clickhouse.insert_json_rows("event_links", link_rows).await
            };
            insert.map_err(|error| SinkFlushFailure::new(SinkStage::EventLinks, error))?;
            link_rows.clear();
        }

        if !tool_rows.is_empty() {
            let insert = if publication.is_some() {
                clickhouse.insert_json_rows_sync("tool_io", tool_rows).await
            } else {
                clickhouse.insert_json_rows("tool_io", tool_rows).await
            };
            insert.map_err(|error| SinkFlushFailure::new(SinkStage::ToolIo, error))?;
            tool_rows.clear();
        }

        if !error_rows.is_empty() {
            let insert = if publication.is_some() {
                clickhouse
                    .insert_json_rows_sync("ingest_errors", error_rows)
                    .await
            } else {
                clickhouse
                    .insert_json_rows("ingest_errors", error_rows)
                    .await
            };
            insert.map_err(|error| SinkFlushFailure::new(SinkStage::IngestErrors, error))?;
            metrics
                .err_rows_written
                .fetch_add(error_rows.len() as u64, Ordering::Relaxed);
            error_rows.clear();
        }

        // Compatibility preparation is dependency-ordered: canonical events,
        // links, tools, and errors are durable before the candidate is built;
        // the causal checkpoint and source head remain later stages.
        if !pending_ack.projection_session_ids.is_empty() {
            clickhouse
                .refresh_mcp_open_read_model(
                    pending_ack
                        .projection_session_ids
                        .iter()
                        .map(String::as_str),
                )
                .await
                .map_err(|error| SinkFlushFailure::new(SinkStage::McpOpenProjection, error))?;
            pending_ack.projection_session_ids.clear();
        }

        if !checkpoint_rows.is_empty() {
            commit_checkpoint_updates(
                clickhouse,
                checkpoints,
                &checkpoint_rows,
                checkpoint_updates,
                publication,
                publication_ready,
            )
            .await
            .map_err(|error| SinkFlushFailure::new(SinkStage::IngestCheckpoints, error))?;
        }

        if let (Some(publication), Some((batch_id, _))) = (publication, append_fence.as_ref()) {
            if quarantined.rows > 0 || blocks_pending_generation {
                publication
                    .block_append(batch_id, &append_manifest)
                    .await
                    .map_err(|error| SinkFlushFailure::new(SinkStage::AppendControl, error))?;
            } else {
                publication
                    .commit_append(batch_id)
                    .await
                    .map_err(|error| SinkFlushFailure::new(SinkStage::AppendControl, error))?;
            }
        }
        checkpoint_updates.clear();

        metrics
            .last_flush_ms
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok::<(), SinkFlushFailure>(())
    }
    .await;

    match flush_result {
        Ok(()) => {
            ack_observer.observe(&pending_ack.event_identity_digests);
            pending_ack.clear();
            true
        }
        Err(failure) => {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            let error_text = failure.error.to_string();
            if failure.stage != SinkStage::McpOpenProjection
                && is_oversized_json_each_row_insert_error(&failure.error)
            {
                let block_reason = format!(
                    "non-retryable sink failure at {}: {error_text}",
                    failure.stage.table()
                );
                for checkpoint in checkpoint_updates.values_mut() {
                    checkpoint.set_lifecycle(CheckpointLifecycle::Error);
                    checkpoint.final_scan_complete = false;
                    checkpoint.block_reason = block_reason.clone();
                }
                for row in &mut checkpoint_rows {
                    if let Some(object) = row.as_object_mut() {
                        object.insert("status".to_string(), json!("error"));
                    }
                }
                let last_error = format!(
                    "non-retryable sink flush failure at {}: {error_text}",
                    failure.stage.table()
                );
                *metrics
                    .last_error
                    .lock()
                    .expect("metrics last_error mutex poisoned") = last_error;
                warn!(
                    sink_stage = failure.stage.table(),
                    "non-retryable oversized JSONEachRow insert failure; recording compact ingest error, committing checkpoint, and dropping pending batch: {}",
                    failure.error
                );
                if let Err(error) = record_non_retryable_sink_error(
                    clickhouse,
                    metrics,
                    failure.stage,
                    &error_text,
                    PendingSinkData {
                        raw_rows,
                        event_rows,
                        link_rows,
                        tool_rows,
                        error_rows,
                        checkpoint_updates,
                    },
                )
                .await
                {
                    let last_error = format!(
                        "failed to record non-retryable sink error at {}: {error}",
                        failure.stage.table()
                    );
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = last_error.clone();
                    warn!(
                        sink_stage = failure.stage.table(),
                        original_error = %failure.error,
                        "{last_error}"
                    );
                    return false;
                }
                if let Err(error) = commit_checkpoint_updates(
                    clickhouse,
                    checkpoints,
                    &checkpoint_rows,
                    checkpoint_updates,
                    publication,
                    publication_ready,
                )
                .await
                {
                    let last_error = format!(
                        "failed to checkpoint after non-retryable sink failure at {}: {error}",
                        failure.stage.table()
                    );
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = last_error.clone();
                    warn!(
                        sink_stage = failure.stage.table(),
                        original_error = %failure.error,
                        "{last_error}"
                    );
                    return false;
                }
                if let (Some(publication), Some((batch_id, _))) =
                    (publication, append_fence.as_ref())
                {
                    if let Err(error) = publication.block_append(batch_id, &append_manifest).await {
                        warn!("failed to block append fence after non-retryable sink failure: {error}");
                        return false;
                    }
                }
                metrics
                    .last_flush_ms
                    .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                clear_pending_sink_data(
                    raw_rows,
                    event_rows,
                    link_rows,
                    tool_rows,
                    error_rows,
                    checkpoint_updates,
                );
                pending_ack.clear();
                return true;
            }

            *metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned") = error_text;
            warn!(
                sink_stage = failure.stage.table(),
                "flush failed: {}", failure.error
            );
            false
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
        queries: Arc<Mutex<Vec<String>>>,
        fail_once_by_table: Arc<Mutex<HashMap<String, usize>>>,
        fail_always_by_table: Arc<Mutex<HashMap<String, String>>>,
        append_control_response: Arc<Mutex<Option<String>>>,
        current_source_head_response: Arc<Mutex<Option<String>>>,
        live_event_exists: Arc<Mutex<bool>>,
    }

    impl MockClickHouseState {
        fn with_single_failure(table: &str) -> Self {
            let state = Self::default();
            state
                .fail_once_by_table
                .lock()
                .expect("mock fail_once mutex poisoned")
                .insert(table.to_string(), 1);
            state
        }

        fn with_permanent_failure(table: &str, body: &str) -> Self {
            let state = Self::default();
            state.fail_permanently(table, body);
            state
        }

        fn fail_permanently(&self, table: &str, body: &str) {
            self.fail_always_by_table
                .lock()
                .expect("mock fail_always mutex poisoned")
                .insert(table.to_string(), body.to_string());
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

        fn max_u64(&self, table: &str, field: &str) -> u64 {
            self.rows(table)
                .into_iter()
                .filter_map(|row| row.get(field).and_then(Value::as_u64))
                .max()
                .unwrap_or(0)
        }

        fn seed_rows(&self, table: &str, rows: Vec<Value>) {
            self.rows_by_table
                .lock()
                .expect("mock rows mutex poisoned")
                .insert(table.to_string(), rows);
        }

        fn query_count(&self, needle: &str) -> usize {
            self.queries
                .lock()
                .expect("mock queries mutex poisoned")
                .iter()
                .filter(|query| query.contains(needle))
                .count()
        }

        fn set_live_event_exists(&self, exists: bool) {
            *self
                .live_event_exists
                .lock()
                .expect("mock live_event_exists mutex poisoned") = exists;
        }

        fn set_append_control_response(&self, response: Value) {
            *self
                .append_control_response
                .lock()
                .expect("mock append_control_response mutex poisoned") =
                Some(format!("{response}\n"));
        }

        fn set_current_source_head_response(&self, response: Value) {
            *self
                .current_source_head_response
                .lock()
                .expect("mock current_source_head_response mutex poisoned") =
                Some(format!("{response}\n"));
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
        } else if query.contains("`ingest_heartbeats`") {
            Some("ingest_heartbeats")
        } else if query.contains("`events`") {
            Some("events")
        } else if query.contains("`ingest_checkpoint_transitions`") {
            Some("ingest_checkpoint_transitions")
        } else if query.contains("`source_generation_publication_readiness`") {
            Some("source_generation_publication_readiness")
        } else if query.contains("`publication_diagnostic_events`") {
            Some("publication_diagnostic_events")
        } else if query.contains("`ingest_append_control`") {
            Some("ingest_append_control")
        } else {
            None
        }
    }

    async fn mock_clickhouse_handler(
        State(state): State<MockClickHouseState>,
        Query(params): Query<HashMap<String, String>>,
        body: String,
    ) -> (StatusCode, String) {
        let query = params
            .get("query")
            .map(String::as_str)
            .unwrap_or(body.as_str());
        state
            .queries
            .lock()
            .expect("mock queries mutex poisoned")
            .push(query.to_string());
        if query.contains("v_current_ingest_append_control") {
            return (
                StatusCode::OK,
                state
                    .append_control_response
                    .lock()
                    .expect("mock append_control_response mutex poisoned")
                    .clone()
                    .unwrap_or_default(),
            );
        }
        if query.contains("FROM `moraine`.v_current_published_source_generations")
            && query.contains("WHERE source_host =")
        {
            return (
                StatusCode::OK,
                state
                    .current_source_head_response
                    .lock()
                    .expect("mock current_source_head_response mutex poisoned")
                    .clone()
                    .unwrap_or_default(),
            );
        }
        if query.contains("maxIf(transitions.checkpoint_revision") {
            let rows = state.rows("ingest_checkpoint_transitions");
            let max_revision = rows
                .iter()
                .filter_map(|row| row.get("checkpoint_revision").and_then(Value::as_u64))
                .max()
                .unwrap_or(0);
            let operation_revision = rows
                .iter()
                .filter(|row| {
                    row.get("operation_id")
                        .and_then(Value::as_str)
                        .is_some_and(|operation_id| {
                            query.contains(&format!("transitions.operation_id = '{operation_id}'"))
                        })
                })
                .filter_map(|row| row.get("checkpoint_revision").and_then(Value::as_u64))
                .max()
                .unwrap_or(0);
            let row = format!(
                "{{\"operation_revision\":{operation_revision},\"max_revision\":{max_revision}}}"
            );
            return (
                StatusCode::OK,
                if params
                    .get("default_format")
                    .is_some_and(|format| format == "JSON")
                {
                    format!("{{\"data\":[{row}]}}\n")
                } else {
                    format!("{row}\n")
                },
            );
        }
        if query.contains("max(readiness.readiness_revision)) AS revision") {
            let revision = state.max_u64(
                "source_generation_publication_readiness",
                "readiness_revision",
            );
            return (StatusCode::OK, format!("{{\"revision\":{revision}}}\n"));
        }
        if query.contains("toUInt8(1) AS existing") && query.contains("v_live_events") {
            let existing = *state
                .live_event_exists
                .lock()
                .expect("mock live_event_exists mutex poisoned");
            let json_envelope = params
                .get("default_format")
                .is_some_and(|format| format == "JSON");
            return (
                StatusCode::OK,
                match (existing, json_envelope) {
                    (true, true) => "{\"data\":[{\"existing\":1}]}\n".to_string(),
                    (true, false) => "{\"existing\":1}\n".to_string(),
                    (false, true) => "{\"data\":[]}\n".to_string(),
                    (false, false) => String::new(),
                },
            );
        }
        if query.contains("existing_count") {
            return (StatusCode::OK, "{\"existing_count\":0}\n".to_string());
        }
        if query.contains("SELECT") && !query.contains("INSERT INTO") {
            return (StatusCode::OK, String::new());
        }
        let Some(table) = inserted_table_name(query) else {
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
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("intentional failure for {table}"),
                    );
                }
            }
        }
        {
            let fail_always = state
                .fail_always_by_table
                .lock()
                .expect("mock fail_always mutex poisoned");
            if let Some(body) = fail_always.get(table) {
                return (StatusCode::BAD_REQUEST, body.clone());
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
            status: CheckpointLifecycle::Active.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn successful_drain_clears_bytes_before_empty_block_reason_rejection() {
        let mut pending_batch_bytes = 4_096;
        let flushed = account_barrier_flush(true, &mut pending_batch_bytes);
        let transition = CheckpointTransition::blocked(&sample_checkpoint(), "");

        let result: std::result::Result<(), String> = if !flushed {
            Err("failed to flush batches preceding replay-block barrier".to_string())
        } else if transition.checkpoint.block_reason.is_empty() {
            Err("blocked replay transition requires a block reason".to_string())
        } else {
            Ok(())
        };

        assert_eq!(pending_batch_bytes, 0);
        assert_eq!(
            result.unwrap_err(),
            "blocked replay transition requires a block reason"
        );
    }

    #[test]
    fn successful_drain_clears_bytes_before_shared_legacy_ambiguity_rejection() {
        let mut pending_batch_bytes = 4_096;
        let flushed = account_barrier_flush(true, &mut pending_batch_bytes);
        let shared_legacy_ambiguous = true;

        let result: std::result::Result<(), String> = if flushed && shared_legacy_ambiguous {
            Err(
                "shared legacy source ownership is ambiguous; publication remains disabled"
                    .to_string(),
            )
        } else {
            Ok(())
        };

        assert_eq!(pending_batch_bytes, 0);
        assert_eq!(
            result.unwrap_err(),
            "shared legacy source ownership is ambiguous; publication remains disabled"
        );
    }

    fn single_row_batch(id: u64) -> SinkMessage {
        let mut batch = RowBatch::default();
        batch.raw_rows.push(json!({ "id": id }));
        SinkMessage::Batch(batch)
    }

    fn test_redactor() -> Arc<SecretRedactor> {
        Arc::new(
            SecretRedactor::new(&moraine_config::RedactionConfig::default())
                .expect("test redactor compiles"),
        )
    }

    fn test_redaction_audit() -> Arc<RedactionAudit> {
        Arc::new(RedactionAudit::default())
    }

    fn test_publication_identity() -> PublicationIdentity {
        PublicationIdentity::for_test("11111111-1111-4111-8111-111111111111")
    }

    fn test_backend_role(cell: Arc<BackendSinkCell>) -> SinkRole {
        SinkRole::Backend {
            cell,
            resolver: Arc::new(Mutex::new(crate::tee::RouteResolver::new(Arc::new(
                moraine_config::AppConfig::default(),
            )))),
            replay_notify: Arc::new(Notify::new()),
            replay_floor: Arc::new(Mutex::new(None)),
            redactor: test_redactor(),
            redactions: test_redaction_audit(),
        }
    }

    fn default_role() -> SinkRole {
        SinkRole::Default {
            dispatch: Arc::new(Mutex::new(DispatchState::default())),
            backends: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            backend_sinks_column: false,
            redactions_column: false,
            redactions: test_redaction_audit(),
        }
    }

    fn fixed_monotonic_timestamp() -> Option<u64> {
        Some(42_000_000)
    }

    #[test]
    fn ack_observation_is_disabled_by_default_and_content_free() {
        assert!(!moraine_config::AppConfig::default().ingest.ack_observation);
        let row = json!({
            "event_uid": "event-1",
            "source_file": "/secret/session.jsonl",
            "raw_json": "payload-marker-that-must-not-leak",
        });
        let mut pending = PendingAckBatch::default();
        pending.extend(&[row]);
        assert_eq!(
            pending.event_identity_digests,
            ["ce36863f51b6baf9d16397ffb3e9af506b284a816f72d487e55943c1fd974d6d"]
        );

        let disabled = IngestAckObserver::with_clock(false, fixed_monotonic_timestamp);
        disabled.observe(&pending.event_identity_digests);
        assert!(disabled.captured().is_empty());

        let enabled = IngestAckObserver::with_clock(true, fixed_monotonic_timestamp);
        enabled.observe(&pending.event_identity_digests);
        let captured = enabled.captured();
        assert_eq!(captured.len(), 1);
        let encoded = serde_json::to_value(&captured[0]).expect("serialize observation");
        assert_eq!(
            encoded
                .as_object()
                .expect("observation object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "ack_monotonic_ns".to_string(),
                "batch_sequence".to_string(),
                "event_identity_digests".to_string(),
            ]
            .into_iter()
            .collect()
        );
        let serialized = serde_json::to_string(&captured[0]).expect("serialize observation");
        for forbidden in [
            "event-1",
            "/secret/session.jsonl",
            "payload-marker-that-must-not-leak",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "leaked forbidden content: {forbidden}"
            );
        }
    }

    #[test]
    fn batched_ack_correlation_preserves_identity_multiplicity_and_sequence() {
        let observer = IngestAckObserver::with_clock(true, fixed_monotonic_timestamp);
        let mut pending = PendingAckBatch::default();
        pending.extend(&[
            json!({"event_uid": "event-1"}),
            json!({"event_uid": "event-2"}),
            json!({"event_uid": "event-1"}),
        ]);
        observer.observe(&pending.event_identity_digests);
        pending.clear();
        pending.extend(&[json!({"event_uid": "event-2"})]);
        observer.observe(&pending.event_identity_digests);

        let captured = observer.captured();
        assert_eq!(
            captured
                .iter()
                .map(|item| item.batch_sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(captured[0].event_identity_digests.len(), 3);
        assert_eq!(
            captured[0].event_identity_digests[0],
            captured[0].event_identity_digests[2]
        );
        assert_eq!(captured[0].ack_monotonic_ns, 42_000_000);
        assert!(captured[1].ack_monotonic_ns >= captured[0].ack_monotonic_ns);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn acknowledgement_waits_for_the_entire_flush_and_survives_retry() {
        let (clickhouse, _state) = spawn_mock_clickhouse("event_links").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let observer = IngestAckObserver::with_clock(true, fixed_monotonic_timestamp);
        let mut pending = PendingFlush {
            raw_rows: vec![json!({"event_uid": "event-1"})],
            event_rows: vec![json!({"event_uid": "event-1"})],
            link_rows: vec![json!({"event_uid": "event-1"})],
            ..PendingFlush::default()
        };

        assert!(
            !flush_pending_with_ack(&clickhouse, &checkpoints, &metrics, &mut pending, &observer,)
                .await
        );
        assert!(observer.captured().is_empty());
        assert_eq!(pending.acknowledgements.event_identity_digests.len(), 1);

        assert!(
            flush_pending_with_ack(&clickhouse, &checkpoints, &metrics, &mut pending, &observer,)
                .await
        );
        let captured = observer.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].batch_sequence, 1);
        assert!(pending.acknowledgements.event_identity_digests.is_empty());
    }

    #[test]
    fn author_is_stamped_only_on_raw_and_event_rows() {
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({"event_uid": "raw-1"}));
        batch.extend_event_rows(vec![json!({"event_uid": "event-1"})]);
        batch.extend_link_rows(vec![json!({"event_uid": "link-1"})]);
        batch.extend_tool_rows(vec![json!({"event_uid": "tool-1"})]);
        batch.push_error_row(json!({"error_kind": "parse"}));

        batch.stamp_author("alice@example.com", true, true);

        assert_eq!(
            batch.raw_rows[0].get("author").and_then(Value::as_str),
            Some("alice@example.com")
        );
        assert_eq!(
            batch.event_rows[0].get("author").and_then(Value::as_str),
            Some("alice@example.com")
        );
        assert!(batch.link_rows[0].get("author").is_none());
        assert!(batch.tool_rows[0].get("author").is_none());
        assert!(batch.error_rows[0].get("author").is_none());
    }

    #[test]
    fn author_is_stripped_per_table_when_default_schema_lacks_column() {
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({"event_uid": "raw-1", "author": "old"}));
        batch.extend_event_rows(vec![json!({"event_uid": "event-1", "author": "old"})]);

        batch.stamp_author("alice@example.com", false, true);

        assert!(batch.raw_rows[0].get("author").is_none());
        assert_eq!(
            batch.event_rows[0].get("author").and_then(Value::as_str),
            Some("alice@example.com")
        );
    }

    #[test]
    fn empty_author_is_a_no_op_even_when_schema_supports_it() {
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({"event_uid": "raw-1"}));
        batch.extend_event_rows(vec![json!({"event_uid": "event-1"})]);

        batch.stamp_author("", true, true);

        assert!(batch.raw_rows[0].get("author").is_none());
        assert!(batch.event_rows[0].get("author").is_none());
    }

    const CLICKHOUSE_OVERSIZED_JSON_OBJECT_ERROR: &str =
        "Code: 117. DB::Exception: Size of JSON object at position 104890103 is extremely large. \
         Expected not greater than 10485760 bytes, but current is 104890103 bytes per row. \
         While executing ParallelParsingBlockInputFormat.";

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
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
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

        let checkpoint = sample_checkpoint();
        let mut pending = PendingFlush {
            raw_rows: vec![json!({
                "record_ts": "2026-02-17T00:00:01.000Z",
                "event_uid": "evt-1"
            })],
            event_rows: vec![json!({"event_uid": "evt-1"})],
            checkpoint_updates: HashMap::from([(
                checkpoint_key(&checkpoint.source_name, &checkpoint.source_file),
                checkpoint.clone(),
            )]),
            ..PendingFlush::default()
        };

        let first_attempt = flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await;
        assert!(!first_attempt, "first flush should fail at events stage");

        assert!(
            pending.raw_rows.is_empty(),
            "raw rows should not be retried"
        );
        assert_eq!(
            pending.event_rows.len(),
            1,
            "event rows remain pending after failure"
        );
        assert_eq!(
            pending.checkpoint_updates.len(),
            1,
            "checkpoint update must remain pending until checkpoint flush succeeds"
        );
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        let second_attempt = flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await;
        assert!(
            second_attempt,
            "second flush should complete remaining stages"
        );

        assert!(pending.event_rows.is_empty());
        assert!(pending.checkpoint_updates.is_empty());
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
    async fn flush_pending_records_non_retryable_sink_oversized_json_and_clears_batch() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::with_permanent_failure(
                "raw_events",
                CLICKHOUSE_OVERSIZED_JSON_OBJECT_ERROR,
            ))
            .await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut pending = PendingFlush {
            raw_rows: vec![json!({
                "source_name": "source-a",
                "harness": "codex",
                "source_file": "/tmp/source-a.jsonl",
                "source_inode": 42u64,
                "source_generation": 1u32,
                "source_line_no": 7u64,
                "source_offset": 2048u64,
                "record_ts": "2026-02-17T00:00:01.000Z",
                "top_type": "response_item",
                "session_id": "session-a",
                "raw_json": "payload-marker-that-must-not-be-copied",
                "raw_json_hash": 1u64,
                "event_uid": "evt-fallback",
            })],
            event_rows: vec![json!({"event_uid": "evt-fallback"})],
            checkpoint_updates: HashMap::from([(checkpoint_key_value.clone(), checkpoint)]),
            ..PendingFlush::default()
        };

        assert!(
            flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "non-retryable oversized JSONEachRow failures should clear the stuck batch"
        );

        assert!(pending.raw_rows.is_empty());
        assert!(pending.event_rows.is_empty());
        assert!(pending.checkpoint_updates.is_empty());
        assert_eq!(mock_state.call_count("raw_events"), 1);
        assert_eq!(
            mock_state.call_count("events"),
            0,
            "the failed stage stops the normal flush sequence"
        );
        assert_eq!(mock_state.call_count("ingest_errors"), 1);
        assert_eq!(
            mock_state.call_count("ingest_checkpoints"),
            1,
            "the skipped batch checkpoint should commit after the compact error row"
        );

        let persisted_errors = mock_state.rows("ingest_errors");
        assert_eq!(persisted_errors.len(), 1);
        let error = &persisted_errors[0];
        assert_eq!(
            error.get("error_kind").and_then(Value::as_str),
            Some(SINK_JSON_OBJECT_TOO_LARGE)
        );
        assert_eq!(
            error.get("source_file").and_then(Value::as_str),
            Some("/tmp/source-a.jsonl")
        );
        assert_eq!(error.get("source_line_no").and_then(Value::as_u64), Some(7));
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(2048)
        );
        assert!(error
            .get("error_text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains("Size of JSON object")));
        let raw_fragment = error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .expect("fallback error should carry a compact fragment");
        assert!(raw_fragment.contains("sink_stage=raw_events"));
        assert!(raw_fragment.contains("source_scope=row"));
        assert!(raw_fragment.contains("original_payload_omitted=true"));
        assert!(
            !raw_fragment.contains("payload-marker"),
            "fallback error must not copy the original row payload"
        );
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        let last_error = metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .clone();
        assert!(last_error.contains("non-retryable sink flush failure"));
        assert!(last_error.contains("raw_events"));

        let state = checkpoints.read().await;
        assert!(
            state.contains_key(&checkpoint_key_value),
            "the fallback should advance the checkpoint after recording the compact sink error"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_keeps_non_retryable_batch_when_error_record_fails() {
        let mock_state = MockClickHouseState::default();
        mock_state.fail_permanently("raw_events", CLICKHOUSE_OVERSIZED_JSON_OBJECT_ERROR);
        mock_state.fail_permanently("ingest_errors", "intentional ingest_errors failure");
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(mock_state).await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut pending = PendingFlush {
            raw_rows: vec![json!({
                "source_name": "source-a",
                "source_file": "/tmp/source-a.jsonl",
                "source_line_no": 7u64,
                "source_offset": 2048u64,
                "record_ts": "2026-02-17T00:00:01.000Z",
                "top_type": "response_item",
                "session_id": "session-a",
                "raw_json": "{}",
                "raw_json_hash": 1u64,
                "event_uid": "evt-error-record-fails",
            })],
            event_rows: vec![json!({"event_uid": "evt-error-record-fails"})],
            checkpoint_updates: HashMap::from([(checkpoint_key_value, checkpoint)]),
            ..PendingFlush::default()
        };

        assert!(
            !flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "fallback should remain retryable when the compact error row cannot be written"
        );

        assert_eq!(pending.raw_rows.len(), 1);
        assert_eq!(pending.event_rows.len(), 1);
        assert_eq!(pending.checkpoint_updates.len(), 1);
        assert_eq!(mock_state.call_count("ingest_errors"), 1);
        assert_eq!(mock_state.call_count("ingest_checkpoints"), 0);
        assert!(mock_state.rows("ingest_errors").is_empty());
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        let last_error = metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .clone();
        assert!(last_error.contains("failed to record non-retryable sink error"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_checkpoints_after_later_stage_non_retryable_fallback() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::with_permanent_failure(
                "events",
                CLICKHOUSE_OVERSIZED_JSON_OBJECT_ERROR,
            ))
            .await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut pending = PendingFlush {
            raw_rows: vec![json!({
                "record_ts": "2026-02-17T00:00:01.000Z",
                "event_uid": "evt-later-stage",
                "raw_json": "{}",
            })],
            event_rows: vec![json!({
                "event_uid": "evt-later-stage",
                "source_name": "source-a",
                "source_file": "/tmp/source-a.jsonl",
                "source_line_no": 17u64,
                "source_offset": 4096u64,
            })],
            checkpoint_updates: HashMap::from([(checkpoint_key_value.clone(), checkpoint)]),
            ..PendingFlush::default()
        };

        assert!(
            flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "non-retryable fallback after a partial commit should finish by checkpointing"
        );

        assert!(pending.raw_rows.is_empty());
        assert!(pending.event_rows.is_empty());
        assert!(pending.checkpoint_updates.is_empty());
        assert_eq!(mock_state.rows("raw_events").len(), 1);
        assert!(mock_state.rows("events").is_empty());
        assert_eq!(mock_state.rows("ingest_errors").len(), 1);
        assert_eq!(mock_state.rows("ingest_checkpoints").len(), 1);
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        let error = &mock_state.rows("ingest_errors")[0];
        assert_eq!(
            error.get("source_line_no").and_then(Value::as_u64),
            Some(17)
        );
        assert!(error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .is_some_and(|fragment| fragment.contains("source_scope=row")
                && fragment.contains("sink_stage=events")));

        let state = checkpoints.read().await;
        assert!(
            state.contains_key(&checkpoint_key_value),
            "checkpoint cache should advance after later-stage fallback"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_uses_checkpoint_source_for_multi_row_non_retryable_fallback() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::with_permanent_failure(
                "raw_events",
                CLICKHOUSE_OVERSIZED_JSON_OBJECT_ERROR,
            ))
            .await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let mut checkpoint = sample_checkpoint();
        checkpoint.last_line_no = 99;
        checkpoint.last_offset = 8192;
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut pending = PendingFlush {
            raw_rows: vec![
                json!({
                    "source_name": "source-a",
                    "source_file": "/tmp/source-a.jsonl",
                    "source_line_no": 7u64,
                    "source_offset": 2048u64,
                    "record_ts": "2026-02-17T00:00:01.000Z",
                    "event_uid": "evt-first",
                    "raw_json": "{}",
                }),
                json!({
                    "source_name": "source-a",
                    "source_file": "/tmp/source-a.jsonl",
                    "source_line_no": 8u64,
                    "source_offset": 4096u64,
                    "record_ts": "2026-02-17T00:00:02.000Z",
                    "event_uid": "evt-second",
                    "raw_json": "{}",
                }),
            ],
            checkpoint_updates: HashMap::from([(checkpoint_key_value, checkpoint)]),
            ..PendingFlush::default()
        };

        assert!(
            flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "multi-row non-retryable fallback should be batch-handled"
        );

        let persisted_errors = mock_state.rows("ingest_errors");
        assert_eq!(persisted_errors.len(), 1);
        let error = &persisted_errors[0];
        assert_eq!(
            error.get("source_line_no").and_then(Value::as_u64),
            Some(99)
        );
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(8192)
        );
        assert!(error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .is_some_and(|fragment| fragment.contains("source_scope=checkpoint")
                && fragment.contains("stage_pending_rows=2")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_retries_other_code_117_failures() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::with_permanent_failure(
                "raw_events",
                "Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow: \
                 unexpected_column",
            ))
            .await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut pending = PendingFlush {
            raw_rows: vec![json!({
                "source_name": "source-a",
                "harness": "codex",
                "source_file": "/tmp/source-a.jsonl",
                "source_inode": 42u64,
                "source_generation": 1u32,
                "source_line_no": 7u64,
                "source_offset": 2048u64,
                "record_ts": "2026-02-17T00:00:01.000Z",
                "top_type": "response_item",
                "session_id": "session-a",
                "raw_json": "{}",
                "raw_json_hash": 1u64,
                "event_uid": "evt-retry",
            })],
            event_rows: vec![json!({"event_uid": "evt-retry"})],
            checkpoint_updates: HashMap::from([(checkpoint_key_value, checkpoint)]),
            ..PendingFlush::default()
        };

        assert!(
            !flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "other ClickHouse Code 117 failures should remain retryable"
        );

        assert_eq!(pending.raw_rows.len(), 1);
        assert_eq!(pending.event_rows.len(), 1);
        assert_eq!(pending.checkpoint_updates.len(), 1);
        assert!(mock_state.rows("ingest_errors").is_empty());
        assert_eq!(mock_state.call_count("ingest_errors"), 0);
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);

        let last_error = metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .clone();
        assert!(last_error.contains("Unknown field"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_quarantines_oversized_raw_rows() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut pending = PendingFlush {
            raw_rows: vec![json!({
                "source_name": "source-a",
                "harness": "codex",
                "source_file": "/tmp/source-a.jsonl",
                "source_inode": 42u64,
                "source_generation": 1u32,
                "source_line_no": 7u64,
                "source_offset": 2048u64,
                "record_ts": "2026-02-17T00:00:01.000Z",
                "top_type": "response_item",
                "session_id": "session-a",
                "raw_json": "x".repeat(CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES),
                "raw_json_hash": 1u64,
                "event_uid": "evt-oversized",
            })],
            checkpoint_updates: HashMap::from([(checkpoint_key_value.clone(), checkpoint)]),
            ..PendingFlush::default()
        };

        assert!(
            flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "oversized rows should be quarantined without failing the flush"
        );

        assert!(
            mock_state.rows("raw_events").is_empty(),
            "oversized raw row must not be sent to raw_events"
        );
        let error_rows = mock_state.rows("ingest_errors");
        assert_eq!(error_rows.len(), 1, "one compact ingest error is written");
        let error = &error_rows[0];
        assert_eq!(
            error.get("error_kind").and_then(Value::as_str),
            Some("oversized_json_row")
        );
        assert_eq!(
            error.get("source_file").and_then(Value::as_str),
            Some("/tmp/source-a.jsonl")
        );
        assert_eq!(error.get("source_line_no").and_then(Value::as_u64), Some(7));
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(2048)
        );
        assert_eq!(
            error
                .get("raw_fragment")
                .and_then(Value::as_str)
                .map(str::len),
            Some(OVERSIZED_ROW_FRAGMENT_CHARS)
        );
        assert_eq!(mock_state.rows("ingest_checkpoints").len(), 1);
        assert!(pending.checkpoint_updates.is_empty());
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 1);

        let state = checkpoints.read().await;
        assert!(
            state.contains_key(&checkpoint_key_value),
            "checkpoint cache should advance after quarantining the row"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_quarantines_oversized_tool_rows() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let mut pending = PendingFlush {
            tool_rows: vec![json!({
                "event_uid": "evt-tool",
                "session_id": "session-a",
                "harness": "codex",
                "source_name": "source-a",
                "tool_call_id": "call-1",
                "tool_name": "shell",
                "tool_phase": "response",
                "output_json": "x".repeat(CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES),
                "source_ref": "/tmp/source-a.jsonl:3:17",
                "event_version": 1u64,
            })],
            ..PendingFlush::default()
        };

        assert!(
            flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await,
            "oversized tool rows should be quarantined without failing the flush"
        );

        assert!(
            mock_state.rows("tool_io").is_empty(),
            "oversized tool row must not be sent to tool_io"
        );
        let error_rows = mock_state.rows("ingest_errors");
        assert_eq!(error_rows.len(), 1, "one compact ingest error is written");
        let error = &error_rows[0];
        assert_eq!(
            error.get("source_file").and_then(Value::as_str),
            Some("/tmp/source-a.jsonl")
        );
        assert_eq!(
            error.get("source_generation").and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            error.get("source_line_no").and_then(Value::as_u64),
            Some(17)
        );
        assert_eq!(error.get("source_inode").and_then(Value::as_u64), Some(0));
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_unowned_derived_row_blocks_without_poisoning_an_unrelated_source() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let mut candidate_a = sample_checkpoint();
        candidate_a.source_name = "cursor-state".to_string();
        candidate_a.source_file = "/tmp/cursor-a.vscdb".to_string();
        candidate_a.source_generation = 2;
        let mut unrelated_b = candidate_a.clone();
        unrelated_b.source_file = "/tmp/cursor-b.vscdb".to_string();
        unrelated_b.source_generation = 5;
        let candidate_a_key = checkpoint_key(&candidate_a.source_name, &candidate_a.source_file);
        let unrelated_b_key = checkpoint_key(&unrelated_b.source_name, &unrelated_b.source_file);
        checkpoints.write().await.extend([
            (candidate_a_key, candidate_a.clone()),
            (unrelated_b_key.clone(), unrelated_b.clone()),
        ]);

        let mut pending = PendingFlush {
            link_rows: vec![json!({
                "event_uid": "missing-owner",
                "source_event_version": 7u64,
                "source_name": "cursor-state",
                "metadata_json": "x".repeat(CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES),
            })],
            checkpoint_updates: HashMap::from([(unrelated_b_key, unrelated_b.clone())]),
            ..PendingFlush::default()
        };

        let observer = IngestAckObserver::new(false);
        let actor = PublicationActor::new(
            clickhouse.clone(),
            String::new(),
            "test-publisher".to_string(),
        );
        let context = FlushContext {
            clickhouse: &clickhouse,
            checkpoints: &checkpoints,
            metrics: &metrics,
            checkpoint_cursor_columns: true,
            checkpoint_host: "",
            ack_observer: &observer,
            publication: Some(&actor),
        };
        assert!(
            !flush_pending_with_publication(
                &context,
                &mut pending,
                FlushOptions {
                    publication_ready: true,
                },
            )
            .await,
            "missing exact ownership must fail before any row or fence mutation"
        );
        assert_eq!(pending.link_rows.len(), 1, "the required row is retained");
        assert_eq!(pending.checkpoint_updates.len(), 1);
        assert!(pending.quarantined_generations.is_empty());
        assert!(mock_state.rows("event_links").is_empty());
        assert!(mock_state.rows("ingest_errors").is_empty());
        assert!(mock_state.rows("ingest_checkpoints").is_empty());
        assert!(mock_state.rows("ingest_append_control").is_empty());
        let state = checkpoints.read().await;
        assert_eq!(state.len(), 2);
        assert_eq!(
            state
                .get(&checkpoint_key(
                    &candidate_a.source_name,
                    &candidate_a.source_file,
                ))
                .expect("candidate A remains cached")
                .lifecycle()
                .unwrap(),
            candidate_a.lifecycle().unwrap()
        );
        assert_eq!(
            state
                .get(&checkpoint_key(
                    &unrelated_b.source_name,
                    &unrelated_b.source_file,
                ))
                .expect("unrelated B remains cached")
                .lifecycle()
                .unwrap(),
            unrelated_b.lifecycle().unwrap()
        );
        drop(state);
        assert!(metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned")
            .contains("without an exact source generation owner"));
    }

    #[test]
    fn quarantine_source_attribution_covers_links_tools_and_errors() {
        let event = json!({
            "event_uid": "owner-event",
            "event_version": 7u64,
            "source_name": "cursor-state",
            "source_file": "/tmp/cursor-state.vscdb",
            "source_inode": 42u64,
            "source_generation": 3u64,
            "source_line_no": 17u64,
            "source_offset": 2048u64,
        });
        let other_version = json!({
            "event_uid": "owner-event",
            "event_version": 8u64,
            "source_name": "cursor-state",
            "source_file": "/tmp/other-cursor-state.vscdb",
            "source_inode": 84u64,
            "source_generation": 5u64,
            "source_line_no": 23u64,
            "source_offset": 4096u64,
        });
        let link = json!({
            "event_uid": "owner-event",
            "source_event_version": 7u64,
            "source_name": "cursor-state",
            "metadata_json": "{}",
        });
        let tool = json!({
            "event_uid": "tool-event",
            "source_name": "cursor-state",
            "source_ref": "/tmp/cursor-state.vscdb:3:18",
        });
        let error = json!({
            "source_name": "cursor-state",
            "source_file": "/tmp/cursor-state.vscdb",
            "source_generation": 3u64,
            "source_line_no": 19u64,
        });
        let owners = quarantine_ownership(&[event, other_version]);

        let link_sources = quarantine_sources_for_row(&link, &owners);
        let [link_source] = link_sources.as_slice() else {
            panic!("link must resolve to exactly one owner source");
        };
        assert_eq!(link_source.source_file, "/tmp/cursor-state.vscdb");
        assert_eq!(link_source.source_generation, 3);
        assert_eq!(link_source.source_line_no, 17);

        let tool_sources = quarantine_sources_for_row(&tool, &owners);
        let [tool_source] = tool_sources.as_slice() else {
            panic!("tool must resolve to exactly one source_ref");
        };
        assert_eq!(tool_source.source_file, "/tmp/cursor-state.vscdb");
        assert_eq!(tool_source.source_generation, 3);
        assert_eq!(tool_source.source_line_no, 18);

        let error_sources = quarantine_sources_for_row(&error, &owners);
        let [error_source] = error_sources.as_slice() else {
            panic!("error must resolve to exactly one direct source");
        };
        assert_eq!(error_source.source_file, "/tmp/cursor-state.vscdb");
        assert_eq!(error_source.source_generation, 3);
        assert_eq!(error_source.source_line_no, 19);
    }

    async fn assert_sqlite_sized_quarantine_blocks_queued_finalization(
        source_name: &str,
        harness: &str,
        source_file: &str,
    ) {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        let payload_bytes = 11 * 1024 * 1024;
        assert!(payload_bytes > CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES);
        assert!(payload_bytes < crate::sqlite_poll::SCAN_PAGE_MAX_BYTES);

        let mut checkpoint = sample_checkpoint();
        checkpoint.source_name = source_name.to_string();
        checkpoint.source_file = source_file.to_string();
        checkpoint.source_generation = 2;
        checkpoint.last_offset = 2;
        checkpoint.last_line_no = 1;
        checkpoint.scan_inode = checkpoint.source_inode;
        checkpoint.scan_boundary = checkpoint.last_offset;
        checkpoint.policy_fingerprint = format!("{source_name}-sqlite-policy");

        let begin = CheckpointTransition::begin_replay(
            &checkpoint,
            checkpoint.source_inode,
            checkpoint.scan_boundary,
            checkpoint.policy_fingerprint.clone(),
        );
        crate::publication::send_begin_replay(&tx, begin)
            .await
            .expect("begin replacement replay");

        // SQLite adapters drain a batch as soon as it crosses max_batch_bytes,
        // then send the causal checkpoint in a later batch. This row is below
        // their 32 MiB scan-page cap but above ClickHouse's 10 MiB object cap.
        let mut oversized = RowBatch::default();
        oversized.push_raw_row(json!({
            "source_name": source_name,
            "harness": harness,
            "source_file": source_file,
            "source_inode": checkpoint.source_inode,
            "source_generation": checkpoint.source_generation,
            "source_line_no": 1u64,
            "source_offset": 1u64,
            "record_ts": "2026-07-20T00:00:00.000Z",
            "raw_json": "x".repeat(payload_bytes),
            "event_uid": format!("{source_name}-oversized"),
        }));
        tx.send(SinkMessage::Batch(oversized))
            .await
            .expect("queue checkpoint-less oversized SQLite chunk");

        let mut replay_checkpoint = checkpoint.clone();
        replay_checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        replay_checkpoint.final_scan_complete = false;
        replay_checkpoint.compatibility_prepared = false;
        replay_checkpoint.backend_caught_up = false;
        let mut final_batch = RowBatch::default();
        final_batch.checkpoint = Some(replay_checkpoint.clone());
        tx.send(SinkMessage::Batch(final_batch))
            .await
            .expect("queue final SQLite replay checkpoint");

        let final_transition = CheckpointTransition::finalize_replay(
            &replay_checkpoint,
            replay_checkpoint.source_inode,
            replay_checkpoint.scan_boundary,
            replay_checkpoint.policy_fingerprint.clone(),
        );
        let error = crate::publication::send_finalize_replay(&tx, final_transition)
            .await
            .expect_err("sink-side quarantine must reject queued publication");
        assert!(
            error.to_string().contains("oversized row(s) quarantined"),
            "unexpected finalization error: {error:#}"
        );

        assert!(
            mock_state.rows("published_source_generations").is_empty(),
            "an incomplete replacement generation must never publish"
        );
        assert_eq!(mock_state.rows("ingest_errors").len(), 1);
        let key = checkpoint_key(source_name, source_file);
        let committed = checkpoints
            .read()
            .await
            .get(&key)
            .cloned()
            .expect("quarantined replay checkpoint is durable");
        assert_eq!(committed.lifecycle().unwrap(), CheckpointLifecycle::Error);
        assert!(!committed.final_scan_complete);
        assert!(!committed.compatibility_prepared);
        assert!(!committed.backend_caught_up);
        assert!(committed
            .block_reason
            .contains("oversized row(s) quarantined"));

        drop(tx);
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("sink should stop")
            .expect("sink task should not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cursor_sqlite_sized_quarantine_cannot_be_overwritten_by_finalize() {
        assert_sqlite_sized_quarantine_blocks_queued_finalization(
            "cursor-state",
            "cursor",
            "/tmp/cursor-state.vscdb",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn opencode_sqlite_sized_quarantine_cannot_be_overwritten_by_finalize() {
        assert_sqlite_sized_quarantine_blocks_queued_finalization(
            "opencode-state",
            "opencode",
            "/tmp/opencode.db",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cursor_link_only_quarantine_inherits_owner_and_blocks_finalize() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        let mut checkpoint = sample_checkpoint();
        checkpoint.source_name = "cursor-reference-state".to_string();
        checkpoint.source_file = "/tmp/cursor-reference-state.vscdb".to_string();
        checkpoint.source_generation = 2;
        checkpoint.last_offset = 2;
        checkpoint.last_line_no = 1;
        checkpoint.scan_inode = checkpoint.source_inode;
        checkpoint.scan_boundary = checkpoint.last_offset;
        checkpoint.policy_fingerprint = "cursor-reference-policy".to_string();
        let begin = CheckpointTransition::begin_replay(
            &checkpoint,
            checkpoint.source_inode,
            checkpoint.scan_boundary,
            checkpoint.policy_fingerprint.clone(),
        );
        crate::publication::send_begin_replay(&tx, begin)
            .await
            .expect("begin Cursor reference replay");

        let owner_uid = "cursor-reference-owner";
        let raw_row = json!({
            "source_name": checkpoint.source_name,
            "harness": "cursor",
            "source_file": checkpoint.source_file,
            "source_inode": checkpoint.source_inode,
            "source_generation": checkpoint.source_generation,
            "source_line_no": 1u64,
            "source_offset": 1u64,
            "record_ts": "2026-07-20T00:00:00.000Z",
            "raw_json": "{\"small\":true}",
            "event_uid": "cursor-reference-raw",
        });
        assert!(
            serde_json::to_vec(&raw_row).unwrap().len() < CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
        );
        let event_row = json!({
            "event_uid": owner_uid,
            "event_version": 7u64,
            "session_id": "cursor-reference-session",
            "source_name": checkpoint.source_name,
            "source_file": checkpoint.source_file,
            "source_inode": checkpoint.source_inode,
            "source_generation": checkpoint.source_generation,
            "source_line_no": 1u64,
            "source_offset": 1u64,
        });
        // event_links intentionally has no source_file/source_generation or
        // source_ref columns. The sink must resolve it through its owner UID.
        let link_row = json!({
            "event_uid": owner_uid,
            "linked_event_uid": "",
            "linked_external_id": "/tmp/primary.rs",
            "link_type": "unknown",
            "session_id": "cursor-reference-session",
            "harness": "cursor",
            "source_name": checkpoint.source_name,
            "metadata_json": "x".repeat(CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES),
            "event_version": 9u64,
            "source_event_version": 7u64,
        });
        assert!(
            serde_json::to_vec(&link_row).unwrap().len()
                > CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
        );
        let mut candidate = RowBatch::default();
        candidate.push_raw_row(raw_row);
        candidate.extend_event_rows([event_row]);
        candidate.extend_link_rows([link_row]);
        tx.send(SinkMessage::Batch(candidate))
            .await
            .expect("queue Cursor link-expansion batch");

        let mut replay_checkpoint = checkpoint.clone();
        replay_checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        replay_checkpoint.final_scan_complete = false;
        replay_checkpoint.compatibility_prepared = false;
        replay_checkpoint.backend_caught_up = false;
        let mut final_batch = RowBatch::default();
        final_batch.checkpoint = Some(replay_checkpoint.clone());
        tx.send(SinkMessage::Batch(final_batch))
            .await
            .expect("queue final Cursor replay checkpoint");

        let final_transition = CheckpointTransition::finalize_replay(
            &replay_checkpoint,
            replay_checkpoint.source_inode,
            replay_checkpoint.scan_boundary,
            replay_checkpoint.policy_fingerprint.clone(),
        );
        let error = crate::publication::send_finalize_replay(&tx, final_transition)
            .await
            .expect_err("link-only quarantine must reject queued publication");
        assert!(error.to_string().contains("oversized row(s) quarantined"));
        assert!(mock_state.rows("event_links").is_empty());
        assert!(mock_state.rows("published_source_generations").is_empty());
        let errors = mock_state.rows("ingest_errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].get("source_file").and_then(Value::as_str),
            Some("/tmp/cursor-reference-state.vscdb")
        );
        assert_eq!(
            errors[0].get("source_generation").and_then(Value::as_u64),
            Some(2)
        );

        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let committed = checkpoints
            .read()
            .await
            .get(&key)
            .cloned()
            .expect("link quarantine checkpoint is durable");
        assert_eq!(committed.lifecycle().unwrap(), CheckpointLifecycle::Error);
        assert!(committed
            .block_reason
            .contains("oversized row(s) quarantined"));

        drop(tx);
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("sink should stop")
            .expect("sink task should not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_link_owner_quarantines_every_candidate_generation() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        let make_checkpoint = |source_file: &str, source_inode: u64| {
            let mut checkpoint = sample_checkpoint();
            checkpoint.source_name = "cursor-reference-state".to_string();
            checkpoint.source_file = source_file.to_string();
            checkpoint.source_inode = source_inode;
            checkpoint.source_generation = 2;
            checkpoint.last_offset = 2;
            checkpoint.last_line_no = 1;
            checkpoint.scan_inode = source_inode;
            checkpoint.scan_boundary = checkpoint.last_offset;
            checkpoint.policy_fingerprint = "cursor-reference-policy".to_string();
            checkpoint
        };
        let candidates = [
            make_checkpoint("/tmp/cursor-reference-a.vscdb", 101),
            make_checkpoint("/tmp/cursor-reference-b.vscdb", 202),
        ];

        for checkpoint in &candidates {
            let begin = CheckpointTransition::begin_replay(
                checkpoint,
                checkpoint.source_inode,
                checkpoint.scan_boundary,
                checkpoint.policy_fingerprint.clone(),
            );
            crate::publication::send_begin_replay(&tx, begin)
                .await
                .expect("begin candidate replay");
        }

        let owner_uid = "shared-cursor-reference-owner";
        let mut batch = RowBatch::default();
        batch.extend_event_rows(candidates.iter().map(|checkpoint| {
            json!({
                "event_uid": owner_uid,
                "event_version": 7u64,
                "session_id": "cursor-reference-session",
                "source_name": checkpoint.source_name,
                "source_file": checkpoint.source_file,
                "source_inode": checkpoint.source_inode,
                "source_generation": checkpoint.source_generation,
                "source_line_no": 1u64,
                "source_offset": 1u64,
            })
        }));
        batch.extend_link_rows([json!({
            "event_uid": owner_uid,
            "linked_event_uid": "",
            "linked_external_id": "/tmp/primary.rs",
            "link_type": "unknown",
            "session_id": "cursor-reference-session",
            "harness": "cursor",
            "source_name": "cursor-reference-state",
            "metadata_json": "x".repeat(CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES),
            "event_version": 9u64,
            "source_event_version": 7u64,
        })]);
        assert!(batch.checkpoint.is_none());
        assert!(
            serde_json::to_vec(&batch.link_rows[0]).unwrap().len()
                > CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
        );
        tx.send(SinkMessage::Batch(batch))
            .await
            .expect("queue checkpoint-less ambiguous link batch");

        for checkpoint in &candidates {
            let mut replay_checkpoint = checkpoint.clone();
            replay_checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
            replay_checkpoint.final_scan_complete = false;
            replay_checkpoint.compatibility_prepared = false;
            replay_checkpoint.backend_caught_up = false;
            let mut final_batch = RowBatch::default();
            final_batch.checkpoint = Some(replay_checkpoint.clone());
            tx.send(SinkMessage::Batch(final_batch))
                .await
                .expect("queue candidate final replay checkpoint");

            let final_transition = CheckpointTransition::finalize_replay(
                &replay_checkpoint,
                replay_checkpoint.source_inode,
                replay_checkpoint.scan_boundary,
                replay_checkpoint.policy_fingerprint.clone(),
            );
            let error = crate::publication::send_finalize_replay(&tx, final_transition)
                .await
                .expect_err("every possible link owner must be blocked from publication");
            assert!(error.to_string().contains("oversized row(s) quarantined"));
        }

        assert!(mock_state.rows("event_links").is_empty());
        assert!(mock_state.rows("published_source_generations").is_empty());
        let errors = mock_state.rows("ingest_errors");
        assert_eq!(errors.len(), 2, "one compact error per candidate owner");
        let error_files = errors
            .iter()
            .filter_map(|row| row.get("source_file").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            error_files,
            BTreeSet::from([
                "/tmp/cursor-reference-a.vscdb",
                "/tmp/cursor-reference-b.vscdb",
            ])
        );
        for checkpoint in &candidates {
            let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
            let committed = checkpoints
                .read()
                .await
                .get(&key)
                .cloned()
                .expect("candidate quarantine checkpoint is durable");
            assert_eq!(committed.lifecycle().unwrap(), CheckpointLifecycle::Error);
            assert!(committed
                .block_reason
                .contains("oversized row(s) quarantined"));
        }

        drop(tx);
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("sink should stop")
            .expect("sink task should not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_transition_allocates_from_one_aggregate_lookup() {
        let state = MockClickHouseState::default();
        state.seed_rows(
            "ingest_checkpoint_transitions",
            vec![
                json!({"checkpoint_revision": 4, "operation_id": "earlier-a"}),
                json!({"checkpoint_revision": 7, "operation_id": "earlier-b"}),
            ],
        );
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let actor = PublicationActor::new(
            clickhouse,
            "test-host".to_string(),
            "test-publisher".to_string(),
        );
        let mut transition =
            CheckpointTransition::try_from_checkpoint(sample_checkpoint()).unwrap();

        let ack = actor
            .persist_transition(&mut transition)
            .await
            .expect("persist checkpoint transition");

        assert_eq!(ack.checkpoint_revision, 8);
        assert_eq!(transition.checkpoint.checkpoint_revision, 8);
        assert_eq!(mock_state.rows("ingest_checkpoint_transitions").len(), 3);
        assert_eq!(
            mock_state.query_count("maxIf(transitions.checkpoint_revision"),
            1,
            "operation idempotence and max revision should share one query"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_transition_response_loss_retry_reuses_original_revision() {
        let mut transition =
            CheckpointTransition::try_from_checkpoint(sample_checkpoint()).unwrap();
        let operation_id = transition.checkpoint.operation_id.clone();
        let state = MockClickHouseState::default();
        state.seed_rows(
            "ingest_checkpoint_transitions",
            vec![
                json!({"checkpoint_revision": 3, "operation_id": operation_id}),
                json!({"checkpoint_revision": 8, "operation_id": "later-operation"}),
            ],
        );
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let actor = PublicationActor::new(
            clickhouse,
            "test-host".to_string(),
            "test-publisher".to_string(),
        );

        let ack = actor
            .persist_transition(&mut transition)
            .await
            .expect("retry persisted transition after response loss");

        assert_eq!(ack.checkpoint_revision, 3);
        assert_eq!(transition.checkpoint.checkpoint_revision, 3);
        assert_eq!(
            mock_state.rows("ingest_checkpoint_transitions").len(),
            2,
            "retry must not append a duplicate transition"
        );
        assert_eq!(
            mock_state.query_count("maxIf(transitions.checkpoint_revision"),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_transition_revision_overflow_remains_an_error() {
        let state = MockClickHouseState::default();
        state.seed_rows(
            "ingest_checkpoint_transitions",
            vec![json!({
                "checkpoint_revision": u64::MAX,
                "operation_id": "different-operation",
            })],
        );
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let actor = PublicationActor::new(
            clickhouse,
            "test-host".to_string(),
            "test-publisher".to_string(),
        );
        let mut transition =
            CheckpointTransition::try_from_checkpoint(sample_checkpoint()).unwrap();

        let error = actor
            .persist_transition(&mut transition)
            .await
            .expect_err("checkpoint revision overflow must fail closed");

        assert!(error.to_string().contains("checkpoint revision exhausted"));
        assert_eq!(mock_state.rows("ingest_checkpoint_transitions").len(), 1);
        assert_eq!(
            mock_state.query_count("maxIf(transitions.checkpoint_revision"),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_only_proof_stops_after_the_first_matching_live_event() {
        let state = MockClickHouseState::default();
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let actor = PublicationActor::new(
            clickhouse,
            "test-host".to_string(),
            "test-publisher".to_string(),
        );
        let mut manifest = AppendManifest::default();
        manifest.event_uids.insert("event-1".to_string());
        manifest.session_ids.insert("session-1".to_string());

        assert!(actor
            .prove_insert_only(&mut manifest)
            .await
            .expect("prove absent replacement keys"));

        mock_state.set_live_event_exists(true);
        assert!(!actor
            .prove_insert_only(&mut manifest)
            .await
            .expect("detect an existing replacement key"));
        assert_eq!(mock_state.query_count("toUInt8(1) AS existing"), 2);
        assert_eq!(mock_state.query_count("v_live_events"), 2);
        assert_eq!(mock_state.query_count("LIMIT 1"), 2);
        assert_eq!(mock_state.query_count("count()) AS existing_count"), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unpublished_replay_batch_does_not_schedule_mcp_projection_refresh() {
        let state = MockClickHouseState::default();
        state.set_current_source_head_response(json!({
            "source_generation": 1,
            "publication_revision": 7,
            "publisher_id": "test-publisher",
            "operation_id": "generation-one",
        }));
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let actor = PublicationActor::new(
            clickhouse.clone(),
            String::new(),
            "test-publisher".to_string(),
        );
        assert!(
            actor
                .has_published_generation(
                    &crate::publication::SourceKey::local("source-a", "/tmp/source-a.jsonl",),
                    1,
                )
                .await
                .expect("read mocked generation-one source head"),
            "the mock must prove that generation one is published"
        );
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let mut replay = sample_checkpoint();
        replay.source_generation = 2;
        replay.set_lifecycle(CheckpointLifecycle::Replaying);
        replay.final_scan_complete = false;
        replay.compatibility_prepared = false;
        let key = checkpoint_key(&replay.source_name, &replay.source_file);
        let observer = IngestAckObserver::new(false);
        let mut pending = PendingFlush {
            event_rows: vec![json!({
                "event_uid": "replacement-event",
                "event_version": 2,
                "session_id": "replacement-session",
                "source_host": "",
                "source_name": "source-a",
                "source_file": "/tmp/source-a.jsonl",
                "source_generation": 2,
            })],
            checkpoint_updates: HashMap::from([(key, replay)]),
            ..PendingFlush::default()
        };
        let context = FlushContext {
            clickhouse: &clickhouse,
            checkpoints: &checkpoints,
            metrics: &metrics,
            checkpoint_cursor_columns: true,
            checkpoint_host: "",
            ack_observer: &observer,
            publication: Some(&actor),
        };

        assert!(
            flush_pending_with_publication(
                &context,
                &mut pending,
                FlushOptions {
                    publication_ready: true,
                },
            )
            .await,
            "the replay batch itself should still flush durably"
        );

        assert_eq!(mock_state.rows("events").len(), 1);
        assert_eq!(
            mock_state.query_count("WITH current_heads AS"),
            0,
            "an unpublished replacement chunk must not invoke the MCP projector"
        );
        assert!(pending.acknowledgements.projection_session_ids.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_initial_checkpoint_stays_staged_before_catch_up_barrier() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::default()).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let publication_identity = test_publication_identity();
        let expected_host = publication_identity.host_id().to_string();
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            metrics,
            rx,
            true,
            publication_identity,
            SinkAuthorConfig::fully_supported(String::new()),
            test_backend_role(cell),
        );

        let mut checkpoint = sample_checkpoint();
        checkpoint.final_scan_complete = true;
        checkpoint.compatibility_prepared = true;
        checkpoint.backend_caught_up = true;
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut batch = RowBatch::default();
        batch.checkpoint = Some(checkpoint);
        tx.send(SinkMessage::Batch(batch))
            .await
            .expect("send initial backend checkpoint");
        drop(tx);

        timeout(Duration::from_secs(5), handle)
            .await
            .expect("backend sink should finish")
            .expect("backend sink task should not panic");

        let committed = checkpoints
            .read()
            .await
            .get(&key)
            .cloned()
            .expect("backend checkpoint committed");
        assert!(
            !committed.backend_caught_up,
            "adapter readiness must be revoked until MirrorCaughtUp is ordered and durable"
        );
        let transitions = mock_state.rows("ingest_checkpoint_transitions");
        assert_eq!(transitions.len(), 1);
        assert_eq!(
            transitions[0]
                .get("backend_caught_up")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            transitions[0].get("host").and_then(Value::as_str),
            Some(expected_host.as_str())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_replay_finalization_returns_typed_staged_success() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::default()).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let mut checkpoint = sample_checkpoint();
        checkpoint.source_generation = 2;
        checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        checkpoint.scan_inode = checkpoint.source_inode;
        checkpoint.scan_boundary = checkpoint.last_offset;
        checkpoint.policy_fingerprint = "policy-fingerprint".to_string();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let checkpoints = Arc::new(RwLock::new(HashMap::from([(key, checkpoint.clone())])));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            metrics,
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            test_backend_role(cell),
        );

        let transition = CheckpointTransition::finalize_replay(
            &checkpoint,
            checkpoint.source_inode,
            checkpoint.last_offset,
            "policy-fingerprint",
        );
        assert_eq!(
            crate::publication::send_finalize_replay(&tx, transition)
                .await
                .expect("staged mirror finalization is successful"),
            FinalizeReplayOutcome::StagedForMirror
        );
        assert!(
            mock_state.rows("published_source_generations").is_empty(),
            "a staged mirror finalization must not publish its source head"
        );
        let readiness = mock_state.rows("source_generation_publication_readiness");
        assert_eq!(readiness.len(), 1);
        let transitions = mock_state.rows("ingest_checkpoint_transitions");
        assert_eq!(transitions.len(), 1);
        assert_eq!(
            readiness[0].get("complete").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            readiness[0]
                .get("source_generation")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            readiness[0]
                .get("backend_caught_up")
                .and_then(Value::as_u64),
            Some(0),
            "staged completion must remain visible as mirror catch-up pending"
        );
        assert_eq!(
            readiness[0]
                .get("compatibility_prepared")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            readiness[0].get("block_reason").and_then(Value::as_str),
            Some("")
        );
        assert_eq!(
            readiness[0].get("checkpoint_revision"),
            transitions[0].get("checkpoint_revision")
        );
        assert_eq!(
            readiness[0].get("operation_id"),
            transitions[0].get("operation_id")
        );

        drop(tx);
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("backend sink should finish")
            .expect("backend sink task should not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_staged_final_waits_for_durable_readiness_before_ack() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::with_permanent_failure(
                "source_generation_publication_readiness",
                "intentional staged-readiness failure",
            ))
            .await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let mut checkpoint = sample_checkpoint();
        checkpoint.source_generation = 2;
        checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        checkpoint.scan_inode = checkpoint.source_inode;
        checkpoint.scan_boundary = checkpoint.last_offset;
        checkpoint.policy_fingerprint = "policy-fingerprint".to_string();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let checkpoints = Arc::new(RwLock::new(HashMap::from([(key, checkpoint.clone())])));
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            test_backend_role(Arc::new(BackendSinkCell::new("team-ch"))),
        );

        let transition = CheckpointTransition::finalize_replay(
            &checkpoint,
            checkpoint.source_inode,
            checkpoint.last_offset,
            "policy-fingerprint",
        );
        let error = crate::publication::send_finalize_replay(&tx, transition)
            .await
            .expect_err("readiness failure must reject staged success");
        assert!(
            error
                .to_string()
                .contains("failed to persist source-generation publication readiness"),
            "unexpected staged-readiness error: {error:#}"
        );
        assert_eq!(
            mock_state.rows("ingest_checkpoint_transitions").len(),
            1,
            "the checkpoint may commit before the readiness failure"
        );
        assert!(mock_state
            .rows("source_generation_publication_readiness")
            .is_empty());
        assert_eq!(
            checkpoints
                .read()
                .await
                .values()
                .next()
                .map(|checkpoint| checkpoint.lifecycle().unwrap()),
            Some(CheckpointLifecycle::Replaying),
            "failed readiness must not merge or acknowledge the staged checkpoint"
        );
        assert!(mock_state.rows("published_source_generations").is_empty());

        drop(tx);
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("backend sink should finish")
            .expect("backend sink task should not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn staged_mirror_readiness_reports_pending_until_catch_up_supersedes_it() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::default()).await;
        let identity = test_publication_identity();
        let expected_host = identity.host_id().to_string();
        let actor = PublicationActor::new(
            clickhouse,
            expected_host.clone(),
            identity.publisher_id().to_string(),
        );
        let mut checkpoint = sample_checkpoint();
        checkpoint.source_generation = 2;
        let mut transition = CheckpointTransition::finalize_replay(
            &checkpoint,
            checkpoint.source_inode,
            checkpoint.last_offset,
            "policy-fingerprint",
        );

        let staged = actor
            .persist_staged_mirror_final(&mut transition)
            .await
            .expect("persist staged mirror final");
        let staged_operation_id = transition.checkpoint.operation_id.clone();
        let staged_readiness = mock_state.rows("source_generation_publication_readiness");
        assert_eq!(staged_readiness.len(), 1);
        let staged_row = &staged_readiness[0];
        assert_eq!(staged_row.get("complete").and_then(Value::as_u64), Some(1));
        assert_eq!(
            staged_row.get("backend_caught_up").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            staged_row
                .get("checkpoint_revision")
                .and_then(Value::as_u64),
            Some(staged.checkpoint_revision)
        );
        assert_eq!(
            staged_row.get("source_host").and_then(Value::as_str),
            Some(expected_host.as_str())
        );
        let staged_revision = staged_row
            .get("readiness_revision")
            .and_then(Value::as_u64)
            .expect("staged readiness revision");
        assert!(
            staged_row.get("complete").and_then(Value::as_u64) == Some(1)
                && staged_row.get("backend_caught_up").and_then(Value::as_u64) == Some(0),
            "the diagnostics predicate must report a pending mirror"
        );

        transition.set_backend_caught_up(true);
        let caught_up = actor
            .persist_mirror_caught_up(&mut transition)
            .await
            .expect("persist mirror catch-up");
        assert_ne!(transition.checkpoint.operation_id, staged_operation_id);
        assert!(caught_up.checkpoint_revision > staged.checkpoint_revision);

        let mut readiness = mock_state.rows("source_generation_publication_readiness");
        readiness.sort_by_key(|row| {
            row.get("readiness_revision")
                .and_then(Value::as_u64)
                .unwrap_or_default()
        });
        assert_eq!(readiness.len(), 2);
        let current = readiness.last().expect("current readiness row");
        assert!(current
            .get("readiness_revision")
            .and_then(Value::as_u64)
            .is_some_and(|revision| revision > staged_revision));
        assert_eq!(current.get("complete").and_then(Value::as_u64), Some(1));
        assert_eq!(
            current.get("backend_caught_up").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            current
                .get("compatibility_prepared")
                .and_then(Value::as_u64),
            Some(0),
            "catch-up readiness precedes compatibility publication"
        );
        assert!(
            !(current.get("complete").and_then(Value::as_u64) == Some(1)
                && current.get("backend_caught_up").and_then(Value::as_u64) == Some(0)),
            "the current diagnostics predicate must clear mirror catch-up pending"
        );

        actor
            .record_publication_repair_failure(&transition)
            .await
            .expect("persist publication-repair diagnostic");
        let mut readiness = mock_state.rows("source_generation_publication_readiness");
        readiness.sort_by_key(|row| {
            row.get("readiness_revision")
                .and_then(Value::as_u64)
                .unwrap_or_default()
        });
        let diagnostic = readiness.last().expect("diagnostic readiness row");
        assert_eq!(
            diagnostic.get("block_reason").and_then(Value::as_str),
            Some("publication_repair_failed")
        );
        assert_eq!(
            diagnostic.get("backend_caught_up").and_then(Value::as_u64),
            Some(1),
            "publication failure must not roll back durable mirror catch-up"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_sink_repairs_its_orphaned_append_before_intake() {
        let state = MockClickHouseState::default();
        state.set_append_control_response(json!({
            "control_revision": 7,
            "cache_epoch": 3,
            "state": "preparing",
            "batch_id": "append-from-prior-process",
            "publisher_id": "host:prior-pid",
            "manifest_json": "{invalid",
            "insert_only": 1,
        }));
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let publication_identity = test_publication_identity();
        let expected_host = publication_identity.host_id().to_string();
        let handle = spawn_sink_task(
            config,
            clickhouse,
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(Metrics::default()),
            rx,
            true,
            publication_identity,
            SinkAuthorConfig::fully_supported(String::new()),
            test_backend_role(cell),
        );
        drop(tx);

        timeout(Duration::from_secs(5), handle)
            .await
            .expect("backend sink should finish")
            .expect("backend sink task should not panic");

        let controls = mock_state.rows("ingest_append_control");
        assert_eq!(
            controls.len(),
            1,
            "orphan repair writes one terminal control"
        );
        assert_eq!(
            controls[0].get("state").and_then(Value::as_str),
            Some("blocked")
        );
        assert_eq!(
            controls[0].get("batch_id").and_then(Value::as_str),
            Some("append-from-prior-process")
        );
        assert_eq!(
            controls[0].get("host").and_then(Value::as_str),
            Some(expected_host.as_str()),
            "each shared backend repairs this host's own append fence"
        );
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
        let redactions = test_redaction_audit();
        let (tx, rx) = mpsc::channel::<SinkMessage>(8);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let publication_identity = test_publication_identity();
        let expected_host = publication_identity.host_id().to_string();

        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            metrics,
            rx,
            true,
            publication_identity,
            SinkAuthorConfig::fully_supported(String::new()),
            SinkRole::Backend {
                cell: cell.clone(),
                resolver: resolver.clone(),
                replay_notify: Arc::new(Notify::new()),
                replay_floor: Arc::new(Mutex::new(None)),
                redactor: test_redactor(),
                redactions: redactions.clone(),
            },
        );

        // The supervisor normally performs this sequence after its replay
        // pass. A backend sink starts fail-closed; routing becomes converged
        // only after the durable catch-up barrier is acknowledged.
        let mut catch_up_checkpoint = sample_checkpoint();
        catch_up_checkpoint.source_name = "catch-up-placeholder".to_string();
        catch_up_checkpoint.source_file = "/tmp/catch-up-placeholder.jsonl".to_string();
        catch_up_checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        catch_up_checkpoint.final_scan_complete = false;
        let catch_up = CheckpointTransition::try_from_checkpoint(catch_up_checkpoint).unwrap();
        crate::publication::send_mirror_caught_up(&tx, catch_up)
            .await
            .expect("initial mirror catch-up barrier");
        cell.mark_publication_ready();
        assert!(cell.publication_ready());

        let raw_secret = "ghp_abcdefghijklmnopqrstuvwxyzABCDE12345";
        let mut batch = RowBatch::default();
        batch.push_raw_row(json!({
            "session_id": "team-session",
            "cwd": "/work/team/project",
            "record_ts": "2026-02-17T00:00:01.000Z",
            "raw_json": format!("{{\"token\":\"{raw_secret}\"}}"),
            "raw_json_hash": 1u64,
        }));
        batch.push_raw_row(json!({
            "session_id": "other-session",
            "cwd": "/home/other",
            "record_ts": "2026-02-17T00:00:01.000Z",
            "raw_json": format!("{{\"token\":\"{raw_secret}\"}}"),
            "raw_json_hash": 1u64,
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
        assert_eq!(
            raw_rows[0].get("source_host").and_then(Value::as_str),
            Some(expected_host.as_str()),
            "shared physical rows use the durable publication identity"
        );
        assert_eq!(
            raw_rows[0].get("raw_json").and_then(Value::as_str),
            Some("{\"token\":\"[REDACTED:github-token]\"}"),
            "backend sink backstop redacts the remote copy after route filtering"
        );
        assert_eq!(
            redactions.snapshot().get("github-token"),
            Some(&1),
            "backend redactions are counted without secret values"
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
            Some(expected_host.as_str()),
            "backend checkpoint rows are host-scoped: team backends share one \
             ingest_checkpoints table across members"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_map_merge_is_offset_monotone() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("unused-table").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let ahead = sample_checkpoint(); // offset 100
        let key = checkpoint_key(&ahead.source_name, &ahead.source_file);
        let mut behind = sample_checkpoint();
        behind.last_offset = 50;

        for cp in [ahead, behind] {
            let mut pending = PendingFlush {
                checkpoint_updates: HashMap::from([(key.clone(), cp)]),
                ..PendingFlush::default()
            };
            assert!(flush_pending(&clickhouse, &checkpoints, &metrics, &mut pending).await);
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
            redactor: test_redactor(),
            redactions: test_redaction_audit(),
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
    fn durable_identity_scopes_only_shared_backend_rows() {
        let identity = test_publication_identity();
        let backend = test_backend_role(Arc::new(BackendSinkCell::new("team-ch")));

        assert_eq!(
            publication_host_for_role(&backend, &identity),
            identity.host_id()
        );
        assert_eq!(
            publication_host_for_role(&default_role(), &identity),
            "",
            "default-local physical rows retain the hostless source_host contract"
        );
    }

    #[test]
    fn backend_publication_readiness_tracks_each_catch_up_revocation() {
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let role = SinkRole::Backend {
            cell: cell.clone(),
            resolver: Arc::new(Mutex::new(crate::tee::RouteResolver::new(Arc::new(
                moraine_config::AppConfig::default(),
            )))),
            replay_notify: Arc::new(Notify::new()),
            replay_floor: Arc::new(Mutex::new(None)),
            redactor: test_redactor(),
            redactions: test_redaction_audit(),
        };

        assert!(!publication_ready_for_role(&role));
        assert!(!publication_ready_for_sink(&role, false));
        cell.mark_publication_ready();
        assert!(publication_ready_for_role(&role));
        assert!(publication_ready_for_sink(&role, false));
        assert!(
            !publication_ready_for_sink(&role, true),
            "legacy ownership ambiguity must override a stale ready latch"
        );
        cell.request_catch_up();
        assert!(
            !publication_ready_for_role(&role),
            "the sink must observe a later mirror gap instead of retaining its first ready latch"
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
