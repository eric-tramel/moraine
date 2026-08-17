use crate::checkpoint::merge_checkpoint;
use crate::heartbeat::host_name;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::finalize_batch_event_identities;
use crate::publication::{AppendManifest, CheckpointLifecycle, PublicationActor};
use crate::publication_identity::PublicationIdentity;
use crate::redaction::{RedactionAudit, SecretRedactor};
use crate::sources::shared::truncate_chars;
use crate::tee::{
    backend_sinks_json, filter_batch_for_backend, BackendSinkCell, SharedRouteResolver,
    StatusRegistry,
};
use crate::{
    DispatchState, Metrics, SinkMessage, WATCHER_BACKEND_MIXED, WATCHER_BACKEND_NATIVE,
    WATCHER_BACKEND_POLL,
};
use anyhow::Context;
use moraine_clickhouse::{
    is_oversized_json_each_row_insert_error, ClickHouseClient, QueryOwner, QueryWorkload,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn stamp_source_host(batch: &mut RowBatch, source_host: &str) {
    for row in batch
        .raw_rows
        .iter_mut()
        .chain(&mut batch.event_rows)
        .chain(&mut batch.link_rows)
        .chain(&mut batch.error_rows)
    {
        if let Some(object) = row.as_object_mut() {
            object.insert(
                "source_host".to_string(),
                Value::String(source_host.to_string()),
            );
        }
    }
}
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IdentityScope {
    source_name: String,
    source_file: String,
    source_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum IdentityLane {
    Durable,
    BackendLive,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IdentityMapKey {
    scope: IdentityScope,
    lane: IdentityLane,
}

fn batch_identity_scope(batch: &RowBatch) -> Option<IdentityScope> {
    if let Some(checkpoint) = batch.checkpoint.as_ref() {
        return Some(IdentityScope {
            source_name: checkpoint.source_name.clone(),
            source_file: checkpoint.source_file.clone(),
            source_generation: u64::from(checkpoint.source_generation),
        });
    }
    let event = batch.event_rows.first()?;
    Some(IdentityScope {
        source_name: event.get("source_name")?.as_str()?.to_string(),
        source_file: event.get("source_file")?.as_str()?.to_string(),
        source_generation: event.get("source_generation")?.as_u64()?,
    })
}
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES: usize = 10 * 1024 * 1024;
const OVERSIZED_ROW_FRAGMENT_CHARS: usize = 20_000;
const SINK_JSON_OBJECT_TOO_LARGE: &str = "sink_json_object_too_large";
const MAX_SINK_ERROR_TEXT_CHARS: usize = 1_000;

const INGEST_ACK_TRACE_TARGET: &str = "moraine_ingest_ack";

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
    append_manifest: Option<AppendManifest>,
    visibility_samples: Vec<(Instant, usize)>,
}

impl PendingAckBatch {
    fn extend(&mut self, event_rows: &[Value]) {
        self.event_identity_digests
            .extend(event_rows.iter().filter_map(event_identity_digest));
    }

    fn observe_pending_rows(&mut self, row_count: usize) {
        if row_count > 0 {
            self.visibility_samples.push((Instant::now(), row_count));
        }
    }

    fn clear(&mut self) {
        self.event_identity_digests.clear();
        self.append_manifest = None;
        self.visibility_samples.clear();
    }
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
        /// Whether `ingest_heartbeats` carries migration 034 progress payloads.
        progress_column: bool,
        redactions: Arc<RedactionAudit>,
    },
    /// A mirror sink for one named backend: filters intake down to the
    /// sessions routed to it and reports flush health on its status cell.
    Backend {
        cell: Arc<BackendSinkCell>,
        resolver: SharedRouteResolver,
        /// Signalled on the lagging/unreachable -> catching-up transition so the
        /// backend's supervisor schedules a catch-up replay pass.
        replay_notify: Arc<Notify>,
        redactor: Arc<SecretRedactor>,
        redactions: Arc<RedactionAudit>,
    },
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

/// Backend-role health bookkeeping after a flush attempt; no-op for the
/// default sink. Recovery requires a drained queue so one successful flush
/// mid-backlog does not trigger a replay storm.
fn note_flush_outcome(role: &SinkRole, flush_ok: bool, queue_drained: bool) {
    let SinkRole::Backend {
        cell,
        replay_notify,
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

fn append_to_visible_percentile(
    sorted_latencies_ms: &[(u64, usize)],
    total_rows: usize,
    quantile: f64,
) -> u64 {
    debug_assert!(!sorted_latencies_ms.is_empty());
    let rank = ((total_rows as f64) * quantile).ceil() as usize;
    let mut cumulative = 0usize;
    for (latency_ms, rows) in sorted_latencies_ms {
        cumulative = cumulative.saturating_add(*rows);
        if cumulative >= rank.max(1) {
            return *latency_ms;
        }
    }
    sorted_latencies_ms
        .last()
        .map_or(0, |(latency, _)| *latency)
}

fn compute_append_to_visible_stats(
    samples: &[(Instant, usize)],
    visible_at: Instant,
) -> Option<(u32, u32)> {
    let total_rows = samples.iter().map(|(_, rows)| *rows).sum::<usize>();
    if total_rows == 0 {
        return None;
    }
    let mut latencies_ms = samples
        .iter()
        .map(|(observed_at, rows)| {
            (
                visible_at
                    .checked_duration_since(*observed_at)
                    .unwrap_or_default()
                    .as_millis() as u64,
                *rows,
            )
        })
        .collect::<Vec<_>>();
    latencies_ms.sort_unstable_by_key(|(latency, _)| *latency);
    let p50 = append_to_visible_percentile(&latencies_ms, total_rows, 0.50);
    let p95 = append_to_visible_percentile(&latencies_ms, total_rows, 0.95);
    Some((saturating_u64_to_u32(p50), saturating_u64_to_u32(p95)))
}

fn publication_host_for_role(role: &SinkRole) -> String {
    match role {
        SinkRole::Backend { .. } => host_name(),
        SinkRole::Default { .. } => String::new(),
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
    publication_identity: PublicationIdentity,
    author: SinkAuthorConfig,
    role: SinkRole,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut raw_rows = Vec::<Value>::new();
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let mut checkpoint_updates = HashMap::<String, Checkpoint>::new();
        let mut pending_batch_bytes = 0usize;
        let ack_observer = IngestAckObserver::new(
            config.ingest.ack_observation && matches!(&role, SinkRole::Default { .. }),
        );
        let mut pending_ack = PendingAckBatch::default();
        let mut pending_flush_barrier: Option<tokio::sync::oneshot::Sender<()>> = None;
        let mut identity_maps = HashMap::<IdentityMapKey, HashMap<String, String>>::new();
        let mut rejected_scopes = HashSet::<IdentityMapKey>::new();

        // Shared backend rows and checkpoints use the existing machine-host
        // scope. The local/default store remains hostless.
        let checkpoint_host = publication_host_for_role(&role);
        let publication_actor = PublicationActor::new(
            clickhouse.clone(),
            checkpoint_host.clone(),
            publication_identity.publisher_id().to_string(),
        );
        let startup_result: anyhow::Result<()> =
            match QueryOwner::new(&clickhouse.runtime(), QueryWorkload::Background) {
                Ok(owner) => {
                    owner
                        .scope(async {
                            publication_actor.block_orphaned_append_on_startup().await?;
                            publication_actor.diagnose_shared_legacy_ambiguity().await?;
                            let durable_checkpoints = checkpoints
                                .read()
                                .await
                                .values()
                                .cloned()
                                .collect::<Vec<_>>();
                            publication_actor
                                .repair_ready_publications(durable_checkpoints)
                                .await?;
                            Ok(())
                        })
                        .await
                }
                Err(error) => Err(error.into()),
            };
        if let Err(error) = startup_result {
            tracing::error!("failed to initialize source publication state: {error:#}");
            return;
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

        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);
        let mut throttling_flush_retries = false;

        macro_rules! flush_publication_barrier {
            () => {{
                let flush_ok = if has_pending_data(
                    &raw_rows,
                    &event_rows,
                    &link_rows,
                    &error_rows,
                    &checkpoint_updates,
                    &pending_ack,
                ) {
                    flush_pending_with_ack(
                        &clickhouse,
                        &checkpoints,
                        Some(&publication_actor),
                        &metrics,
                        &mut raw_rows,
                        &mut event_rows,
                        &mut link_rows,
                        &mut error_rows,
                        &mut checkpoint_updates,
                        checkpoint_cursor_columns,
                        &checkpoint_host,
                        &ack_observer,
                        &mut pending_ack,
                    )
                    .await
                } else {
                    true
                };
                note_flush_outcome(&role, flush_ok, true);
                if flush_ok {
                    pending_batch_bytes = 0;
                }
                flush_ok
            }};
        }

        macro_rules! publication_operation {
            ($operation:expr) => {{
                match QueryOwner::new(&clickhouse.runtime(), QueryWorkload::Background) {
                    Ok(owner) => owner
                        .scope($operation)
                        .await
                        .map_err(|error| format!("{error:#}")),
                    Err(error) => Err(format!("{error:#}")),
                }
            }};
        }
        loop {
            if throttling_flush_retries
                && has_pending_data(
                    &raw_rows,
                    &event_rows,
                    &link_rows,
                    &error_rows,
                    &checkpoint_updates,
                    &pending_ack,
                )
            {
                let flush_ok = flush_pending_with_ack(
                    &clickhouse,
                    &checkpoints,
                    Some(&publication_actor),
                    &metrics,
                    &mut raw_rows,
                    &mut event_rows,
                    &mut link_rows,
                    &mut error_rows,
                    &mut checkpoint_updates,
                    checkpoint_cursor_columns,
                    &checkpoint_host,
                    &ack_observer,
                    &mut pending_ack,
                )
                .await;
                let barrier_waiting = pending_flush_barrier.is_some();
                note_flush_outcome(&role, flush_ok, barrier_waiting || rx.is_empty());
                if flush_ok {
                    pending_batch_bytes = 0;
                    throttling_flush_retries = false;
                    if let Some(barrier) = pending_flush_barrier.take() {
                        let _ = barrier.send(());
                    }
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
                        Some(message @ (SinkMessage::Batch(_) | SinkMessage::BackendBatch { .. })) => {
                            let (batch, persist_checkpoint, backend_epoch, identity_lane) = match message {
                                SinkMessage::Batch(batch) => {
                                    (batch, true, None, IdentityLane::Durable)
                                }
                                SinkMessage::BackendBatch {
                                    batch,
                                    epoch,
                                    persist_checkpoint,
                                } => {
                                    let SinkRole::Backend { cell, .. } = &role else {
                                        tracing::error!(
                                            "default sink received a backend-only batch"
                                        );
                                        continue;
                                    };
                                    (
                                        batch,
                                        persist_checkpoint
                                            && cell.can_persist_backend_checkpoint(epoch),
                                        Some(epoch),
                                        IdentityLane::BackendLive,
                                    )
                                }
                                SinkMessage::BeginReplay { .. }
                                | SinkMessage::FinalizeReplay { .. }
                                | SinkMessage::BlockReplay { .. }
                                | SinkMessage::MirrorCaughtUp { .. }
                                | SinkMessage::FlushBarrier(_) => unreachable!(),
                            };
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
                            let identity_scope = batch_identity_scope(&batch);
                            let identity_key = identity_scope.clone().map(|scope| IdentityMapKey {
                                scope,
                                lane: identity_lane,
                            });
                            let backend_role = matches!(&role, SinkRole::Backend { .. });
                            let scan_complete = batch.checkpoint.is_some();
                            if backend_role
                                && identity_key
                                    .as_ref()
                                    .is_some_and(|key| rejected_scopes.contains(key))
                            {
                                if scan_complete {
                                    if let Some(key) = identity_key.as_ref() {
                                        rejected_scopes.remove(key);
                                        identity_maps.remove(key);
                                    }
                                }
                                continue;
                            }
                            let finalization = if let Some(key) = identity_key.as_ref() {
                                finalize_batch_event_identities(
                                    &mut batch,
                                    identity_maps.entry(key.clone()).or_default(),
                                )
                            } else {
                                finalize_batch_event_identities(
                                    &mut batch,
                                    &mut HashMap::new(),
                                )
                            };
                            if let Err(error) = finalization {
                                if let SinkRole::Backend {
                                    cell,
                                    replay_notify,
                                    ..
                                } = &role
                                {
                                    if cell.invalidate_checkpoint_authority(backend_epoch) {
                                        replay_notify.notify_one();
                                    }
                                }
                                if let Some(key) = identity_key.as_ref() {
                                    identity_maps.remove(key);
                                    if backend_role && !scan_complete {
                                        rejected_scopes.insert(key.clone());
                                    }
                                }
                                warn!(
                                    "rejecting malformed normalized batch before insert: {error:#}"
                                );
                                continue;
                            }
                            if scan_complete {
                                if let Some(key) = identity_key.as_ref() {
                                    identity_maps.remove(key);
                                    rejected_scopes.remove(key);
                                }
                            }
                            pending_ack.observe_pending_rows(batch.raw_rows.len());
                            pending_batch_bytes =
                                pending_batch_bytes.saturating_add(batch.approx_bytes());
                            raw_rows.extend(batch.raw_rows);
                            event_rows.extend(batch.event_rows);
                            link_rows.extend(batch.link_rows);
                            error_rows.extend(batch.error_rows);
                            if persist_checkpoint {
                                if let Some(cp) = batch.checkpoint {
                                    merge_checkpoint(&mut checkpoint_updates, cp);
                                }
                            }

                            let total_rows =
                                raw_rows.len() + event_rows.len() + link_rows.len() + error_rows.len();
                            metrics
                                .sink_pending_rows
                                .store(total_rows as u64, Ordering::Relaxed);
                            metrics
                                .sink_pending_bytes
                                .store(pending_batch_bytes as u64, Ordering::Relaxed);
                            if total_rows >= config.ingest.batch_size
                                || pending_batch_bytes >= config.ingest.max_batch_bytes.max(1)
                            {
                                let flush_ok = flush_pending_with_ack(
                                    &clickhouse,
                                    &checkpoints,
                                    Some(&publication_actor),
                                    &metrics,
                                    &mut raw_rows,
                                    &mut event_rows,
                                    &mut link_rows,
                                    &mut error_rows,
                                    &mut checkpoint_updates,
                                    checkpoint_cursor_columns,
                                    &checkpoint_host,
                    &ack_observer,
                    &mut pending_ack,
                                ).await;
                                note_flush_outcome(&role, flush_ok, rx.is_empty());
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
                        Some(SinkMessage::BeginReplay {
                            mut transition,
                            ack,
                        }) => {
                            let result = if flush_publication_barrier!() {
                                publication_operation!(
                                    publication_actor.persist_transition(&mut transition)
                                )
                            } else {
                                Err("failed to flush rows before replay-start checkpoint".to_string())
                            };
                            if result.is_ok() {
                                let mut current = checkpoints.write().await;
                                merge_checkpoint(&mut current, transition.checkpoint.clone());
                            }
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::FinalizeReplay {
                            mut transition,
                            ack,
                        }) => {
                            let result = if flush_publication_barrier!() {
                                if matches!(&role, SinkRole::Backend { .. }) {
                                    publication_operation!(
                                        publication_actor
                                            .persist_staged_mirror_final(&mut transition)
                                    )
                                    .map(|_| crate::FinalizeReplayOutcome::StagedForMirror)
                                } else {
                                    transition.set_backend_caught_up(true);
                                    publication_operation!(
                                        publication_actor.publish_final(&mut transition)
                                    )
                                    .map(crate::FinalizeReplayOutcome::Published)
                                }
                            } else {
                                Err("failed to flush final replay rows before publication".to_string())
                            };
                            if result.is_ok() {
                                let mut current = checkpoints.write().await;
                                merge_checkpoint(&mut current, transition.checkpoint.clone());
                            }
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::BlockReplay {
                            mut transition,
                            ack,
                        }) => {
                            let result = if flush_publication_barrier!() {
                                publication_operation!(
                                    publication_actor.persist_transition(&mut transition)
                                )
                            } else {
                                Err("failed to flush rows before blocked replay checkpoint".to_string())
                            };
                            if result.is_ok() {
                                let mut current = checkpoints.write().await;
                                merge_checkpoint(&mut current, transition.checkpoint.clone());
                            }
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::MirrorCaughtUp { ack }) => {
                            let result = if flush_publication_barrier!() {
                                // The durable transition map, not only this
                                // process's staging cache, is authoritative.
                                // A restart may lose the cache after the final
                                // scan but before the fixed-point catch-up.
                                let mut staged = checkpoints
                                    .read()
                                    .await
                                    .values()
                                    .filter(|checkpoint| {
                                        checkpoint.source_generation > 1
                                            && checkpoint.status
                                                == CheckpointLifecycle::Active.as_str()
                                            && checkpoint.block_reason.is_empty()
                                            && checkpoint.final_scan_complete
                                            && !checkpoint.backend_caught_up
                                    })
                                    .cloned()
                                    .map(crate::CheckpointTransition::from_checkpoint)
                                    .collect::<Vec<_>>();
                                let published = publication_operation!(async {
                                    for transition in &mut staged {
                                        transition.set_backend_caught_up(true);
                                        publication_actor.publish_final(transition).await?;
                                    }
                                    Ok::<_, anyhow::Error>(staged)
                                });
                                match published {
                                    Ok(staged) => {
                                        let mut current = checkpoints.write().await;
                                        for transition in staged {
                                            merge_checkpoint(
                                                &mut current,
                                                transition.checkpoint,
                                            );
                                        }
                                        Ok(())
                                    }
                                    Err(error) => Err(error),
                                }
                            } else {
                                Err("failed to flush mirror rows before publication".to_string())
                            };
                            let _ = ack.send(result);
                        }
                        Some(SinkMessage::FlushBarrier(barrier)) => {
                            if has_pending_data(
                                &raw_rows,
                                &event_rows,
                                &link_rows,
                                &error_rows,
                                &checkpoint_updates,
                                &pending_ack,
                            ) {
                                let flush_ok = flush_pending_with_ack(
                                    &clickhouse,
                                    &checkpoints,
                                    Some(&publication_actor),
                                    &metrics,
                                    &mut raw_rows,
                                    &mut event_rows,
                                    &mut link_rows,
                                    &mut error_rows,
                                    &mut checkpoint_updates,
                                    checkpoint_cursor_columns,
                                    &checkpoint_host,
                                    &ack_observer,
                                    &mut pending_ack,
                                )
                                .await;
                                // The barrier is the ordered drain point for
                                // every earlier message, even if later live
                                // deltas are already queued behind it.
                                note_flush_outcome(&role, flush_ok, true);
                                if flush_ok {
                                    pending_batch_bytes = 0;
                                    let _ = barrier.send(());
                                } else {
                                    pending_flush_barrier = Some(barrier);
                                    throttling_flush_retries = true;
                                }
                            } else {
                                // FIFO delivery means all preceding batches
                                // have already crossed the durable flush path.
                                note_flush_outcome(&role, true, true);
                                let _ = barrier.send(());
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_tick.tick() => {
                    if has_pending_data(&raw_rows, &event_rows, &link_rows, &error_rows, &checkpoint_updates, &pending_ack) {
                        let flush_ok = flush_pending_with_ack(
                            &clickhouse,
                            &checkpoints,
                            Some(&publication_actor),
                            &metrics,
                            &mut raw_rows,
                            &mut event_rows,
                            &mut link_rows,
                            &mut error_rows,
                            &mut checkpoint_updates,
                            checkpoint_cursor_columns,
                            &checkpoint_host,
                    &ack_observer,
                    &mut pending_ack,
                        ).await;
                        note_flush_outcome(&role, flush_ok, rx.is_empty());
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

        if has_pending_data(
            &raw_rows,
            &event_rows,
            &link_rows,
            &error_rows,
            &checkpoint_updates,
            &pending_ack,
        ) {
            let flush_ok = flush_pending_with_ack(
                &clickhouse,
                &checkpoints,
                Some(&publication_actor),
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                checkpoint_cursor_columns,
                &checkpoint_host,
                &ack_observer,
                &mut pending_ack,
            )
            .await;
            note_flush_outcome(&role, flush_ok, true);
            if flush_ok {
                if let Some(barrier) = pending_flush_barrier.take() {
                    let _ = barrier.send(());
                }
            }
        }
    })
}

fn has_pending_data(
    raw_rows: &[Value],
    event_rows: &[Value],
    link_rows: &[Value],
    error_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
    pending_ack: &PendingAckBatch,
) -> bool {
    !(raw_rows.is_empty()
        && event_rows.is_empty()
        && link_rows.is_empty()
        && error_rows.is_empty()
        && checkpoint_updates.is_empty()
        && pending_ack.append_manifest.is_none())
}

fn quarantine_oversized_rows(
    table: &str,
    rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
) -> anyhow::Result<usize> {
    let mut kept = Vec::with_capacity(rows.len());
    let mut quarantined = 0usize;

    for row in std::mem::take(rows) {
        let row_bytes = serde_json::to_vec(&row)
            .with_context(|| format!("failed to encode {table} row for oversized-row guard"))?
            .len();
        if row_bytes > CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES {
            error_rows.push(oversized_row_error(table, &row, row_bytes));
            quarantined = quarantined.saturating_add(1);
        } else {
            kept.push(row);
        }
    }

    *rows = kept;
    Ok(quarantined)
}

fn oversized_row_error(table: &str, row: &Value, row_bytes: usize) -> Value {
    let source = ingest_error_source_from_row(row);
    json!({
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
    EventLinks,
    IngestErrors,
    IngestCheckpoints,
}

impl SinkStage {
    fn table(self) -> &'static str {
        match self {
            Self::RawEvents => "raw_events",
            Self::Events => "events",
            Self::EventLinks => "event_links",
            Self::IngestErrors => "ingest_errors",
            Self::IngestCheckpoints => "ingest_checkpoints",
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

#[derive(Default)]
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
    error_rows: &'a [Value],
    checkpoint_updates: &'a HashMap<String, Checkpoint>,
}

impl PendingSinkData<'_> {
    fn source_for(self, stage: SinkStage) -> (IngestErrorSource, &'static str) {
        let stage_rows = match stage {
            SinkStage::RawEvents => self.raw_rows,
            SinkStage::Events => self.event_rows,
            SinkStage::EventLinks => self.link_rows,
            SinkStage::IngestErrors => self.error_rows,
            SinkStage::IngestCheckpoints => &[],
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
            SinkStage::EventLinks => self.link_rows.len(),
            SinkStage::IngestErrors => self.error_rows.len(),
            SinkStage::IngestCheckpoints => self.checkpoint_updates.len(),
        }
    }

    fn total_len(self) -> usize {
        self.raw_rows.len()
            + self.event_rows.len()
            + self.link_rows.len()
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
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
) {
    raw_rows.clear();
    event_rows.clear();
    link_rows.clear();
    error_rows.clear();
    checkpoint_updates.clear();
}

async fn commit_checkpoint_updates(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    checkpoint_rows: &[Value],
    checkpoint_updates: &HashMap<String, Checkpoint>,
    sqlite_cursor_durable: bool,
) -> anyhow::Result<()> {
    if checkpoint_rows.is_empty() {
        return Ok(());
    }

    clickhouse
        .insert_json_rows_sync("ingest_checkpoints", checkpoint_rows)
        .await
        .context("failed to insert ingest checkpoint rows")?;

    // Offset-monotone merge, not a blind insert: a backend map has two
    // producers (live router batches and catch-up replay), so an out-of-order
    // flush must never regress what the map already committed.
    {
        let mut state = checkpoints.write().await;
        for cp in checkpoint_updates.values() {
            merge_checkpoint(&mut state, cp.clone());
        }
    }
    metrics
        .progress
        .lock()
        .expect("ingest progress mutex poisoned")
        .acknowledge(checkpoint_updates, sqlite_cursor_durable);
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
        progress_column,
        redactions,
    } = role
    else {
        return;
    };

    let (files_active, oldest_pending_unix_ms) = {
        let state = dispatch.lock().expect("dispatch mutex poisoned");
        (
            state.inflight.len() as u32,
            state
                .pending_since_unix_ms
                .values()
                .copied()
                .min()
                .unwrap_or(0),
        )
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
        "service_version": moraine_config::BUILD_VERSION,
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
    if *progress_column {
        let progress_json = metrics
            .progress
            .lock()
            .expect("ingest progress mutex poisoned")
            .to_json(
                oldest_pending_unix_ms,
                metrics.sink_pending_rows.load(Ordering::Relaxed),
                metrics.sink_pending_bytes.load(Ordering::Relaxed),
                metrics.sink_retrying.load(Ordering::Relaxed),
            );
        if let Some(obj) = heartbeat.as_object_mut() {
            obj.insert("progress_json".to_string(), Value::String(progress_json));
        }
    }

    let insert = match QueryOwner::new(&clickhouse.runtime(), QueryWorkload::Background) {
        Ok(owner) => {
            owner
                .scope(clickhouse.insert_json_rows("ingest_heartbeats", &[heartbeat]))
                .await
        }
        Err(error) => Err(error.into()),
    };
    if let Err(exc) = insert {
        warn!("heartbeat insert failed: {exc}");
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn flush_pending(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    metrics: &Arc<Metrics>,
    raw_rows: &mut Vec<Value>,
    event_rows: &mut Vec<Value>,
    link_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
    checkpoint_cursor_columns: bool,
    checkpoint_host: &str,
) -> bool {
    let observer = IngestAckObserver::new(false);
    let mut pending_ack = PendingAckBatch::default();
    flush_pending_with_ack(
        clickhouse,
        checkpoints,
        None,
        metrics,
        raw_rows,
        event_rows,
        link_rows,
        error_rows,
        checkpoint_updates,
        checkpoint_cursor_columns,
        checkpoint_host,
        &observer,
        &mut pending_ack,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn flush_pending_with_ack(
    clickhouse: &ClickHouseClient,
    checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    publication_actor: Option<&PublicationActor>,
    metrics: &Arc<Metrics>,
    raw_rows: &mut Vec<Value>,
    event_rows: &mut Vec<Value>,
    link_rows: &mut Vec<Value>,
    error_rows: &mut Vec<Value>,
    checkpoint_updates: &mut HashMap<String, Checkpoint>,
    checkpoint_cursor_columns: bool,
    checkpoint_host: &str,
    ack_observer: &IngestAckObserver,
    pending_ack: &mut PendingAckBatch,
) -> bool {
    let append_fence = if let Some(actor) = publication_actor {
        let preparation = match QueryOwner::new(&clickhouse.runtime(), QueryWorkload::Background) {
            Ok(owner) => owner
                .scope(async {
                    let manifest = if let Some(manifest) = &pending_ack.append_manifest {
                        manifest.clone()
                    } else {
                        let mut manifest = AppendManifest::from_pending(
                            checkpoint_host,
                            raw_rows,
                            event_rows,
                            link_rows,
                            &[],
                            error_rows,
                            checkpoint_updates.values().cloned(),
                        );
                        actor.prove_insert_only(&mut manifest).await?;
                        pending_ack.append_manifest = Some(manifest.clone());
                        manifest
                    };
                    actor
                        .begin_append(&manifest)
                        .await
                        .map(|fence| (fence, manifest))
                })
                .await
                .map(|(fence, manifest)| fence.map(|fence| (fence, manifest))),
            Err(error) => Err(error.into()),
        };
        match preparation {
            Ok(Some(((batch_id, cache_epoch), manifest))) => {
                let Some(target_epoch) = cache_epoch.checked_add(1) else {
                    warn!("append cache epoch exhausted at {cache_epoch}");
                    return false;
                };
                for checkpoint in checkpoint_updates.values_mut() {
                    checkpoint.append_batch_id = batch_id.clone();
                    checkpoint.cache_epoch = target_epoch;
                }
                Some((batch_id, manifest))
            }
            Ok(None) => None,
            Err(error) => {
                metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                *metrics
                    .last_error
                    .lock()
                    .expect("metrics last_error mutex poisoned") = error.to_string();
                warn!("failed to prepare append publication: {error:#}");
                return false;
            }
        }
    } else {
        None
    };
    let started = Instant::now();
    metrics.sink_pending_rows.store(
        (raw_rows.len() + event_rows.len() + link_rows.len() + error_rows.len()) as u64,
        Ordering::Relaxed,
    );

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

    let quarantined = match (|| -> anyhow::Result<usize> {
        Ok(
            quarantine_oversized_rows("raw_events", raw_rows, error_rows)?
                + quarantine_oversized_rows("events", event_rows, error_rows)?
                + quarantine_oversized_rows("event_links", link_rows, error_rows)?,
        )
    })() {
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
    if quarantined > 0 {
        warn!(
            rows = quarantined,
            max_bytes = CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES,
            "quarantined oversized rows before ClickHouse insert"
        );
    }

    let first_pending_stage = if !raw_rows.is_empty() {
        SinkStage::RawEvents
    } else if !event_rows.is_empty() {
        SinkStage::Events
    } else if !link_rows.is_empty() {
        SinkStage::EventLinks
    } else if !error_rows.is_empty() {
        SinkStage::IngestErrors
    } else {
        SinkStage::IngestCheckpoints
    };
    let flush_result = match QueryOwner::new(&clickhouse.runtime(), QueryWorkload::Background) {
        Ok(owner) => {
            owner
                .scope(async {
                    if !raw_rows.is_empty() {
                        clickhouse
                            .insert_json_rows_sync("raw_events", raw_rows)
                            .await
                            .map_err(|error| SinkFlushFailure::new(SinkStage::RawEvents, error))?;
                        metrics
                            .raw_rows_written
                            .fetch_add(raw_rows.len() as u64, Ordering::Relaxed);
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
                        event_rows.clear();
                    }

                    if !link_rows.is_empty() {
                        clickhouse
                            .insert_json_rows_sync("event_links", link_rows)
                            .await
                            .map_err(|error| SinkFlushFailure::new(SinkStage::EventLinks, error))?;
                        link_rows.clear();
                    }

                    if !error_rows.is_empty() {
                        clickhouse
                            .insert_json_rows_sync("ingest_errors", error_rows)
                            .await
                            .map_err(|error| {
                                SinkFlushFailure::new(SinkStage::IngestErrors, error)
                            })?;
                        metrics
                            .err_rows_written
                            .fetch_add(error_rows.len() as u64, Ordering::Relaxed);
                        error_rows.clear();
                    }

                    if !checkpoint_rows.is_empty() {
                        commit_checkpoint_updates(
                            clickhouse,
                            checkpoints,
                            metrics,
                            &checkpoint_rows,
                            checkpoint_updates,
                            checkpoint_cursor_columns,
                        )
                        .await
                        .map_err(|error| {
                            SinkFlushFailure::new(SinkStage::IngestCheckpoints, error)
                        })?;
                        if let Some(actor) = publication_actor {
                            for checkpoint in checkpoint_updates.values().cloned() {
                                let mut transition =
                                    crate::CheckpointTransition::from_checkpoint(checkpoint);
                                actor.persist_transition(&mut transition).await.map_err(
                                    |error| {
                                        SinkFlushFailure::new(SinkStage::IngestCheckpoints, error)
                                    },
                                )?;
                                if transition.lifecycle == CheckpointLifecycle::Active
                                    && transition.checkpoint.source_generation == 1
                                {
                                    actor
                                        .publish_initial_if_absent(&mut transition)
                                        .await
                                        .map_err(|error| {
                                            SinkFlushFailure::new(
                                                SinkStage::IngestCheckpoints,
                                                error,
                                            )
                                        })?;
                                }
                            }
                        }
                        checkpoint_updates.clear();
                    }

                    if let (Some(actor), Some((batch_id, manifest))) =
                        (publication_actor, append_fence.as_ref())
                    {
                        actor
                            .commit_append(batch_id, Some(&manifest.source_keys))
                            .await
                            .map_err(|error| {
                                SinkFlushFailure::new(SinkStage::IngestCheckpoints, error)
                            })?;
                    }
                    if let Some((p50_ms, p95_ms)) = compute_append_to_visible_stats(
                        &pending_ack.visibility_samples,
                        Instant::now(),
                    ) {
                        metrics
                            .append_to_visible_p50_ms
                            .store(p50_ms as u64, Ordering::Relaxed);
                        metrics
                            .append_to_visible_p95_ms
                            .store(p95_ms as u64, Ordering::Relaxed);
                    }

                    metrics
                        .last_flush_ms
                        .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                    Ok::<(), SinkFlushFailure>(())
                })
                .await
        }
        Err(error) => Err(SinkFlushFailure::new(first_pending_stage, error.into())),
    };

    match flush_result {
        Ok(()) => {
            metrics.sink_pending_rows.store(0, Ordering::Relaxed);
            metrics.sink_pending_bytes.store(0, Ordering::Relaxed);
            metrics.sink_retrying.store(false, Ordering::Relaxed);
            ack_observer.observe(&pending_ack.event_identity_digests);
            pending_ack.clear();
            true
        }
        Err(failure) => {
            metrics.sink_retrying.store(true, Ordering::Relaxed);
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            let error_text = failure.error.to_string();
            if is_oversized_json_each_row_insert_error(&failure.error) {
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
                let repair_result =
                    match QueryOwner::new(&clickhouse.runtime(), QueryWorkload::Background) {
                        Ok(owner) => {
                            owner
                                .scope(async {
                                    record_non_retryable_sink_error(
                                        clickhouse,
                                        metrics,
                                        failure.stage,
                                        &error_text,
                                        PendingSinkData {
                                            raw_rows,
                                            event_rows,
                                            link_rows,
                                            error_rows,
                                            checkpoint_updates,
                                        },
                                    )
                                    .await?;
                                    commit_checkpoint_updates(
                                        clickhouse,
                                        checkpoints,
                                        metrics,
                                        &checkpoint_rows,
                                        checkpoint_updates,
                                        checkpoint_cursor_columns,
                                    )
                                    .await
                                    .with_context(|| {
                                        format!(
                                    "failed to checkpoint after non-retryable sink failure at {}",
                                    failure.stage.table()
                                )
                                    })?;
                                    if let (Some(actor), Some((batch_id, manifest))) =
                                        (publication_actor, append_fence.as_ref())
                                    {
                                        actor
                                            .commit_append(batch_id, Some(&manifest.source_keys))
                                            .await
                                            .context(
                                                "failed to resolve append fence after non-retryable sink failure",
                                            )?;
                                    }
                                    Ok::<(), anyhow::Error>(())
                                })
                                .await
                        }
                        Err(error) => Err(error.into()),
                    };
                if let Err(error) = repair_result {
                    let last_error = error.to_string();
                    *metrics
                        .last_error
                        .lock()
                        .expect("metrics last_error mutex poisoned") = last_error.clone();
                    warn!(
                        sink_stage = failure.stage.table(),
                        original_error = %failure.error,
                        "non-retryable sink repair failed: {last_error}"
                    );
                    return false;
                }
                metrics
                    .last_flush_ms
                    .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                clear_pending_sink_data(
                    raw_rows,
                    event_rows,
                    link_rows,
                    error_rows,
                    checkpoint_updates,
                );
                pending_ack.clear();
                metrics.sink_pending_rows.store(0, Ordering::Relaxed);
                metrics.sink_pending_bytes.store(0, Ordering::Relaxed);
                metrics.sink_retrying.store(false, Ordering::Relaxed);
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
    use crate::CheckpointTransition;
    use axum::{
        extract::{Query, State},
        http::StatusCode,
        routing::post,
        Router,
    };
    use serde_json::json;
    use tokio::time::timeout;

    #[derive(Clone, Default)]
    struct MockClickHouseState {
        calls_by_table: Arc<Mutex<HashMap<String, usize>>>,
        rows_by_table: Arc<Mutex<HashMap<String, Vec<Value>>>>,
        fail_once_by_table: Arc<Mutex<HashMap<String, usize>>>,
        fail_always_by_table: Arc<Mutex<HashMap<String, String>>>,
        query_ids: Arc<Mutex<Vec<String>>>,
        queries: Arc<Mutex<Vec<String>>>,
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

        fn seed_rows(&self, table: &str, rows: Vec<Value>) {
            self.rows_by_table
                .lock()
                .expect("mock rows mutex poisoned")
                .insert(table.to_string(), rows);
        }

        fn max_u64(&self, table: &str, field: &str) -> u64 {
            self.rows(table)
                .into_iter()
                .filter_map(|row| row.get(field).and_then(Value::as_u64))
                .max()
                .unwrap_or(0)
        }

        fn query_ids(&self) -> Vec<String> {
            self.query_ids
                .lock()
                .expect("mock query ids mutex poisoned")
                .clone()
        }

        fn queries(&self) -> Vec<String> {
            self.queries
                .lock()
                .expect("mock queries mutex poisoned")
                .clone()
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
        } else if query.contains("`published_source_generations`") {
            Some("published_source_generations")
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
        if query.contains("FROM system.workloads") {
            return (StatusCode::OK, "0".to_string());
        }
        state
            .query_ids
            .lock()
            .expect("mock query ids mutex poisoned")
            .push(
                params
                    .get("query_id")
                    .expect("sink request must carry a query id")
                    .clone(),
            );
        if query.contains("v_current_ingest_append_control") {
            let rows = state
                .rows("ingest_append_control")
                .into_iter()
                .max_by_key(|row| {
                    row.get("control_revision")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                })
                .into_iter()
                .collect::<Vec<_>>();
            return (StatusCode::OK, json!({"data": rows}).to_string());
        }
        if query.contains("v_current_ingest_checkpoint_transitions AS checkpoint") {
            return (
                StatusCode::OK,
                json!({"data": state.rows("append_checkpoint_proofs")}).to_string(),
            );
        }
        if query.contains("v_current_published_source_generations") {
            let rows = state
                .rows("published_source_generations")
                .into_iter()
                .max_by_key(|row| {
                    row.get("publication_revision")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                })
                .into_iter()
                .collect::<Vec<_>>();
            return (StatusCode::OK, json!({"data": rows}).to_string());
        }
        if query.contains("AS checkpoint_revision") && query.contains("transitions.operation_id") {
            let revision = state
                .rows("ingest_checkpoint_transitions")
                .into_iter()
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
            return (
                StatusCode::OK,
                json!({"data": [{"checkpoint_revision": revision}]}).to_string(),
            );
        }
        if query.contains("max(transitions.checkpoint_revision)") {
            let revision = state.max_u64("ingest_checkpoint_transitions", "checkpoint_revision");
            return (
                StatusCode::OK,
                json!({"data": [{"revision": revision}]}).to_string(),
            );
        }
        if query.contains("max(heads.publication_revision)") {
            let revision = state.max_u64("published_source_generations", "publication_revision");
            return (
                StatusCode::OK,
                json!({"data": [{"revision": revision}]}).to_string(),
            );
        }
        if query.contains("max(readiness.readiness_revision)") {
            let revision = state.max_u64(
                "source_generation_publication_readiness",
                "readiness_revision",
            );
            return (
                StatusCode::OK,
                json!({"data": [{"revision": revision}]}).to_string(),
            );
        }
        if query.contains("existing_count") {
            return (
                StatusCode::OK,
                json!({"data": [{"existing_count": 0}]}).to_string(),
            );
        }
        if query.contains("SELECT") && !query.contains("INSERT INTO") {
            return (StatusCode::OK, json!({"data": []}).to_string());
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
            status: "active".to_string(),
            ..Default::default()
        }
    }

    fn seed_interrupted_append(
        state: &MockClickHouseState,
        identity: &PublicationIdentity,
        checkpoint: &Checkpoint,
        cache_epoch: u64,
    ) -> (AppendManifest, String) {
        let manifest = AppendManifest::from_pending(
            "",
            &[],
            &[],
            &[],
            &[],
            &[],
            std::iter::once(checkpoint.clone()),
        );
        let batch_id = manifest.batch_id();
        state.seed_rows(
            "ingest_append_control",
            vec![json!({
                "host": "",
                "control_revision": 1,
                "cache_epoch": cache_epoch,
                "state": "preparing",
                "batch_id": batch_id,
                "publisher_id": identity.publisher_id(),
                "manifest_json": serde_json::to_string(&manifest).expect("serialize manifest"),
                "insert_only": 0,
            })],
        );
        (manifest, batch_id)
    }

    async fn wait_for_append_control_rows(state: &MockClickHouseState, expected: usize) {
        timeout(Duration::from_secs(2), async {
            while state.rows("ingest_append_control").len() < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("append-control startup recovery should complete");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_recovery_commits_only_a_fully_checkpointed_append() {
        let state = MockClickHouseState::default();
        let identity = test_publication_identity();
        let checkpoint = sample_checkpoint();
        let (_manifest, batch_id) = seed_interrupted_append(&state, &identity, &checkpoint, 7);
        state.seed_rows(
            "append_checkpoint_proofs",
            vec![json!({
                "source_host": "",
                "source_name": checkpoint.source_name,
                "source_file": checkpoint.source_file,
                "checkpoint_revision": 4,
                "source_generation": checkpoint.source_generation,
                "published_source_generation": checkpoint.source_generation,
                "last_offset": checkpoint.last_offset,
                "last_line": checkpoint.last_line_no,
                "lifecycle": "active",
                "block_reason": "",
                "append_batch_id": batch_id,
                "cache_epoch": 8,
            })],
        );
        let (clickhouse, state) = spawn_mock_clickhouse_with_state(state).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let (_tx, rx) = mpsc::channel(1);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(Metrics::default()),
            rx,
            true,
            identity,
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        wait_for_append_control_rows(&state, 2).await;
        let latest = state
            .rows("ingest_append_control")
            .into_iter()
            .max_by_key(|row| row["control_revision"].as_u64().unwrap_or(0))
            .expect("recovered append control");
        assert_eq!(latest["state"], "idle");
        assert_eq!(latest["cache_epoch"], 8);
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_recovery_keeps_a_missing_checkpoint_append_fenced() {
        let state = MockClickHouseState::default();
        let identity = test_publication_identity();
        let checkpoint = sample_checkpoint();
        let (_manifest, batch_id) = seed_interrupted_append(&state, &identity, &checkpoint, 7);
        let (clickhouse, state) = spawn_mock_clickhouse_with_state(state).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.heartbeat_interval_seconds = 60.0;
        let (_tx, rx) = mpsc::channel(1);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(Metrics::default()),
            rx,
            true,
            identity,
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        wait_for_append_control_rows(&state, 2).await;
        let latest = state
            .rows("ingest_append_control")
            .into_iter()
            .max_by_key(|row| row["control_revision"].as_u64().unwrap_or(0))
            .expect("reopened append control");
        assert_eq!(latest["state"], "preparing");
        assert_eq!(latest["batch_id"], batch_id);
        assert_eq!(latest["cache_epoch"], 7);
        let manifest: AppendManifest =
            serde_json::from_str(latest["manifest_json"].as_str().expect("manifest JSON"))
                .expect("recovery manifest");
        assert!(manifest.recovery_pending);
        handle.abort();
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

    fn default_role() -> SinkRole {
        SinkRole::Default {
            dispatch: Arc::new(Mutex::new(DispatchState::default())),
            backends: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            backend_sinks_column: false,
            redactions_column: false,
            progress_column: false,
            redactions: test_redaction_audit(),
        }
    }

    fn backend_role(cell: Arc<BackendSinkCell>, replay_notify: Arc<Notify>) -> SinkRole {
        let mut config = moraine_config::AppConfig::default();
        config.backends.insert(
            "team-ch".to_string(),
            moraine_config::ClickHouseConfig::default(),
        );
        config.routes.push(moraine_config::RouteConfig {
            dir: "/work/team/**".to_string(),
            backend: "team-ch".to_string(),
            mode: moraine_config::ROUTE_MODE_MIRROR.to_string(),
        });
        SinkRole::Backend {
            cell,
            resolver: Arc::new(Mutex::new(crate::tee::RouteResolver::new(Arc::new(config)))),
            replay_notify,
            redactor: test_redactor(),
            redactions: test_redaction_audit(),
        }
    }

    fn routed_batch(id: u64, checkpoint: Checkpoint) -> RowBatch {
        let mut batch = RowBatch::default();
        batch.raw_rows.push(json!({
            "id": id,
            "session_id": "team-session",
            "cwd": "/work/team/project",
        }));
        batch.checkpoint = Some(checkpoint);
        batch
    }

    fn backend_batch(batch: RowBatch, epoch: u64, persist_checkpoint: bool) -> SinkMessage {
        SinkMessage::BackendBatch {
            batch,
            epoch,
            persist_checkpoint,
        }
    }

    fn fixed_monotonic_timestamp() -> Option<u64> {
        Some(42_000_000)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_initializes_append_control_for_shared_publication_host() {
        let (clickhouse, state) = spawn_mock_clickhouse("").await;
        let runtime = clickhouse.runtime();
        let actor = PublicationActor::new(
            clickhouse,
            "host-a".to_string(),
            "host-a:publisher-a".to_string(),
        );
        let owner =
            QueryOwner::new(&runtime, QueryWorkload::Background).expect("create startup owner");

        assert!(!owner
            .scope(actor.block_orphaned_append_on_startup())
            .await
            .expect("initialize append control"));
        let rows = state.rows("ingest_append_control");
        let initialized = rows.last().expect("append control initialized");
        assert_eq!(
            initialized.get("host").and_then(Value::as_str),
            Some("host-a")
        );
        assert_eq!(
            initialized.get("publisher_id").and_then(Value::as_str),
            Some("host-a:publisher-a")
        );
        assert_eq!(
            initialized.get("state").and_then(Value::as_str),
            Some("idle")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spawned_flush_establishes_one_background_owner_for_all_statements() {
        let (clickhouse, state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::default()).await;
        let runtime = clickhouse.runtime();
        let caller = QueryOwner::new(&runtime, QueryWorkload::Internal)
            .expect("create unrelated caller owner");

        let task = caller
            .scope(async move {
                tokio::spawn(async move {
                    let checkpoints = Arc::new(RwLock::new(HashMap::new()));
                    let metrics = Arc::new(Metrics::default());
                    let mut raw_rows = vec![json!({"event_uid": "raw-1"})];
                    let mut event_rows = vec![json!({"event_uid": "event-1"})];
                    let mut link_rows = Vec::new();
                    let mut error_rows = Vec::new();
                    let mut checkpoint_updates = HashMap::new();
                    flush_pending(
                        &clickhouse,
                        &checkpoints,
                        &metrics,
                        &mut raw_rows,
                        &mut event_rows,
                        &mut link_rows,
                        &mut error_rows,
                        &mut checkpoint_updates,
                        true,
                        "",
                    )
                    .await
                })
                .await
                .expect("spawned flush task joins")
            })
            .await;
        assert!(task, "spawned flush succeeds");

        let query_ids = state.query_ids();
        assert_eq!(
            query_ids.len(),
            2,
            "raw and event inserts share one attempt"
        );
        assert!(
            query_ids
                .iter()
                .all(|id| id.starts_with("moraine-background-")),
            "spawned operation must establish its own background owner: {query_ids:?}"
        );
        let logical_ids: Vec<_> = query_ids
            .iter()
            .map(|id| id.rsplit_once('-').expect("child query suffix").0)
            .collect();
        assert_eq!(logical_ids[0], logical_ids[1]);
        assert_eq!(runtime.active_owner_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_establishes_a_background_owner() {
        let (clickhouse, state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::default()).await;
        let runtime = clickhouse.runtime();

        emit_heartbeat(&clickhouse, &Arc::new(Metrics::default()), &default_role()).await;

        let query_ids = state.query_ids();
        assert_eq!(query_ids.len(), 1);
        assert!(query_ids[0].starts_with("moraine-background-"));
        assert_eq!(runtime.active_owner_count(), 0);
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
        let mut pending_ack = PendingAckBatch::default();
        let mut raw_rows = vec![json!({"event_uid": "event-1"})];
        let mut event_rows = vec![json!({"event_uid": "event-1"})];
        let mut link_rows = vec![json!({"event_uid": "event-1"})];
        let mut error_rows = Vec::new();
        let mut checkpoint_updates = HashMap::new();

        assert!(
            !flush_pending_with_ack(
                &clickhouse,
                &checkpoints,
                None,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
                &observer,
                &mut pending_ack,
            )
            .await
        );
        assert!(observer.captured().is_empty());
        assert_eq!(pending_ack.event_identity_digests.len(), 1);

        assert!(
            flush_pending_with_ack(
                &clickhouse,
                &checkpoints,
                None,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
                &observer,
                &mut pending_ack,
            )
            .await
        );
        let captured = observer.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].batch_sequence, 1);
        assert!(pending_ack.event_identity_digests.is_empty());
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
        let (clickhouse, _state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::with_permanent_failure(
                "raw_events",
                "intentional raw-event outage",
            ))
            .await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 0.05;
        config.ingest.heartbeat_interval_seconds = 60.0;
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
    async fn flush_barrier_waits_for_subthreshold_checkpoint_durability() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            metrics,
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        let checkpoint = sample_checkpoint();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut batch = RowBatch::default();
        batch.checkpoint = Some(checkpoint.clone());
        tx.send(SinkMessage::Batch(batch))
            .await
            .expect("queue checkpoint-only replay batch");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue replay barrier");

        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("barrier acknowledgement timeout")
            .expect("sink closed before barrier acknowledgement");

        assert_eq!(
            checkpoints.read().await.get(&key).map(|cp| cp.last_offset),
            Some(checkpoint.last_offset),
            "barrier ACK requires the in-memory durable checkpoint map to advance"
        );
        assert_eq!(
            mock_state.rows("ingest_checkpoints").len(),
            1,
            "barrier must force a subthreshold checkpoint insert"
        );

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits after channel close")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_barrier_ack_survives_retry_and_never_precedes_checkpoint() {
        let state = MockClickHouseState::with_permanent_failure(
            "ingest_checkpoints",
            "intentional checkpoint outage",
        );
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 0.05;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            metrics.clone(),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        let checkpoint = sample_checkpoint();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut batch = RowBatch::default();
        batch.checkpoint = Some(checkpoint);
        tx.send(SinkMessage::Batch(batch))
            .await
            .expect("queue replay");
        let (barrier_tx, mut barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue barrier");

        let deadline = Instant::now() + Duration::from_secs(1);
        while mock_state.call_count("ingest_checkpoints") == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            mock_state.call_count("ingest_checkpoints") > 0,
            "sink_finished={}, last_error={:?}, queries before checkpoint insert: {:?}",
            handle.is_finished(),
            metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned"),
            mock_state
                .queries
                .lock()
                .expect("mock queries mutex poisoned")
        );
        assert!(
            timeout(Duration::from_millis(50), &mut barrier_rx)
                .await
                .is_err(),
            "failed checkpoint insert must not acknowledge the barrier"
        );
        mock_state
            .fail_always_by_table
            .lock()
            .expect("mock fail_always mutex poisoned")
            .remove("ingest_checkpoints");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("barrier retry timeout")
            .expect("barrier sender dropped");
        assert_eq!(
            checkpoints.read().await.get(&key).map(|cp| cp.last_offset),
            Some(100)
        );
        assert_eq!(mock_state.rows("ingest_checkpoints").len(), 1);

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_replay_publishes_only_after_fixed_point_barrier() {
        let (clickhouse, mock_state) =
            spawn_mock_clickhouse_with_state(MockClickHouseState::default()).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;
        let state_dir = std::env::temp_dir().join(format!(
            "moraine-publication-sink-test-{}",
            uuid::Uuid::new_v4()
        ));
        config.ingest.state_dir = state_dir.to_string_lossy().into_owned();
        let mut checkpoint = sample_checkpoint();
        checkpoint.source_generation = 2;
        checkpoint.status = CheckpointLifecycle::Replaying.as_str().to_string();
        checkpoint.scan_inode = checkpoint.source_inode;
        checkpoint.scan_boundary = checkpoint.last_offset;
        checkpoint.policy_fingerprint = "policy-fingerprint".to_string();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let checkpoints = Arc::new(RwLock::new(HashMap::from([(key, checkpoint.clone())])));
        let (tx, rx) = mpsc::channel(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(
                Arc::new(BackendSinkCell::new("team-ch")),
                Arc::new(Notify::new()),
            ),
        );

        let transition = CheckpointTransition::finalize_replay(
            &checkpoint,
            checkpoint.source_inode,
            checkpoint.last_offset,
            "policy-fingerprint",
        );
        let staged = crate::publication::send_finalize_replay(&tx, transition).await;
        if let Err(error) = staged.as_ref() {
            drop(tx);
            let sink_result = timeout(Duration::from_secs(2), handle).await;
            panic!(
                "stage mirror replay: {error:#}; sink: {sink_result:?}; queries: {:?}",
                mock_state.queries()
            );
        }
        assert_eq!(
            staged.expect("checked staged outcome"),
            crate::FinalizeReplayOutcome::StagedForMirror
        );
        assert!(
            mock_state.rows("published_source_generations").is_empty(),
            "a staged mirror replay must keep the previous generation published"
        );
        let staged_readiness = mock_state.rows("source_generation_publication_readiness");
        assert_eq!(staged_readiness.len(), 1);
        assert_eq!(
            staged_readiness[0]
                .get("backend_caught_up")
                .and_then(Value::as_u64),
            Some(0)
        );

        crate::publication::send_mirror_caught_up(&tx)
            .await
            .expect("publish staged replay after fixed-point barrier");
        let published = mock_state.rows("published_source_generations");
        assert_eq!(published.len(), 1);
        assert_eq!(
            published[0]
                .get("source_generation")
                .and_then(Value::as_u64),
            Some(2)
        );
        let compatibility_checkpoints = mock_state.rows("ingest_checkpoints");
        assert_eq!(compatibility_checkpoints.len(), 1);
        assert_eq!(
            compatibility_checkpoints[0]
                .get("source_generation")
                .and_then(Value::as_u64),
            Some(2),
            "publication must advance the durable compatibility checkpoint"
        );
        assert_eq!(
            compatibility_checkpoints[0]
                .get("host")
                .and_then(Value::as_str),
            Some(host_name().as_str()),
            "shared backend compatibility checkpoints remain host-scoped"
        );
        let readiness = mock_state.rows("source_generation_publication_readiness");
        assert_eq!(readiness.len(), 2);
        assert_eq!(
            readiness[1]
                .get("backend_caught_up")
                .and_then(Value::as_u64),
            Some(1)
        );

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_barrier_reports_rejected_batch_and_withholds_its_checkpoint() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            metrics,
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            default_role(),
        );

        let checkpoint = sample_checkpoint();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut malformed = RowBatch::default();
        malformed
            .event_rows
            .push(json!({"session_id": "missing-event-uid"}));
        malformed.checkpoint = Some(checkpoint);
        tx.send(SinkMessage::Batch(malformed))
            .await
            .expect("queue malformed replay batch");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue barrier");

        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("barrier timeout")
            .expect("barrier sender dropped");
        assert!(checkpoints.read().await.get(&key).is_none());
        assert!(mock_state.rows("ingest_checkpoints").is_empty());

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn default_sink_does_not_quarantine_the_rest_of_a_rejected_scan() {
        let (clickhouse, _) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(4);
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

        let checkpoint = sample_checkpoint();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut malformed_non_final = RowBatch::default();
        malformed_non_final.event_rows.push(json!({
            "session_id": "default-session",
            "source_name": checkpoint.source_name,
            "source_file": checkpoint.source_file,
            "source_generation": checkpoint.source_generation,
        }));
        tx.send(SinkMessage::Batch(malformed_non_final))
            .await
            .expect("queue malformed default chunk");

        let mut final_boundary = RowBatch::default();
        final_boundary.checkpoint = Some(checkpoint.clone());
        tx.send(SinkMessage::Batch(final_boundary))
            .await
            .expect("queue default scan boundary");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue default barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("default barrier timeout")
            .expect("default barrier sender dropped");

        assert_eq!(
            checkpoints.read().await.get(&key).map(|cp| cp.last_offset),
            Some(checkpoint.last_offset),
            "mirror recovery bookkeeping must not change default-sink scan behavior"
        );

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_delta_preserves_scan_boundary_without_persisting_checkpoint() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status_for_test(crate::tee::BackendSinkStatus::CatchingUp);
        let (tx, rx) = mpsc::channel(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            metrics,
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(cell, Arc::new(Notify::new())),
        );

        let checkpoint = sample_checkpoint();
        let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut malformed_non_final = RowBatch::default();
        malformed_non_final.event_rows.push(json!({
            "session_id": "team-session",
            "source_name": checkpoint.source_name,
            "source_file": checkpoint.source_file,
            "source_generation": checkpoint.source_generation,
            "cwd": "/work/team/project",
        }));
        tx.send(backend_batch(malformed_non_final, 0, false))
            .await
            .expect("queue rejected non-final delta");

        let mut final_boundary = RowBatch::default();
        final_boundary.checkpoint = Some(checkpoint.clone());
        tx.send(backend_batch(final_boundary, 0, false))
            .await
            .expect("queue final delta boundary");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue first barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("first barrier timeout")
            .expect("first barrier sender dropped");
        assert!(checkpoints.read().await.get(&key).is_none());
        assert!(mock_state.rows("ingest_checkpoints").is_empty());

        let mut next_scan = RowBatch::default();
        next_scan.event_rows.push(json!({
            "event_uid": "next-scan-event",
            "session_id": "team-session",
            "source_name": checkpoint.source_name,
            "source_file": checkpoint.source_file,
            "source_generation": checkpoint.source_generation,
            "harness": "test",
            "event_kind": "message",
            "actor_kind": "user",
            "payload_type": "text",
            "text_content": "recovered",
            "cwd": "/work/team/project",
        }));
        next_scan.checkpoint = Some(checkpoint);
        tx.send(backend_batch(next_scan, 1, false))
            .await
            .expect("queue next scan delta");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue second barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("second barrier timeout")
            .expect("second barrier sender dropped");
        assert!(checkpoints.read().await.get(&key).is_none());
        assert!(mock_state.rows("ingest_checkpoints").is_empty());
        assert_eq!(mock_state.rows("events").len(), 1);

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_and_live_identity_maps_are_isolated() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status_for_test(crate::tee::BackendSinkStatus::CatchingUp);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(8);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(cell, Arc::new(Notify::new())),
        );

        let checkpoint = sample_checkpoint();
        let source_fields = json!({
            "source_name": checkpoint.source_name,
            "source_file": checkpoint.source_file,
            "source_generation": checkpoint.source_generation,
        });
        let mut replay_request = RowBatch::default();
        replay_request.event_rows.push(json!({
            "event_uid": "request-provisional",
            "harness": "nac",
            "session_id": "team-session",
            "event_kind": "tool_call",
            "actor_kind": "assistant",
            "payload_type": "tool_use",
            "tool_call_id": "call-1",
            "tool_name": "Read",
            "tool_phase": "request",
            "text_content": "request",
            "payload_json": "{}",
            "cwd": "/work/team/project",
            "source_name": source_fields["source_name"],
            "source_file": source_fields["source_file"],
            "source_generation": source_fields["source_generation"],
        }));
        tx.send(SinkMessage::Batch(replay_request))
            .await
            .expect("queue non-final replay request");

        let mut live_boundary = RowBatch::default();
        live_boundary.checkpoint = Some(checkpoint.clone());
        tx.send(backend_batch(live_boundary, 0, false))
            .await
            .expect("queue interleaved live scan boundary");

        let mut replay_response = RowBatch::default();
        replay_response.event_rows.push(json!({
            "event_uid": "response-provisional",
            "origin_event_id": "request-provisional",
            "harness": "nac",
            "session_id": "team-session",
            "event_kind": "tool_result",
            "actor_kind": "tool",
            "payload_type": "tool_result",
            "tool_call_id": "call-1",
            "tool_name": "Read",
            "tool_phase": "response",
            "text_content": "response",
            "payload_json": "{\"request_event_uid\":\"request-provisional\"}",
            "cwd": "/work/team/project",
            "source_name": source_fields["source_name"],
            "source_file": source_fields["source_file"],
            "source_generation": source_fields["source_generation"],
        }));
        replay_response.checkpoint = Some(checkpoint);
        tx.send(SinkMessage::Batch(replay_response))
            .await
            .expect("queue final replay response");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue identity barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("identity barrier timeout")
            .expect("identity barrier sender dropped");

        let events = mock_state.rows("events");
        let request_uid = events
            .iter()
            .find(|event| event["event_kind"] == "tool_call")
            .and_then(|event| event["event_uid"].as_str())
            .expect("persisted request UID");
        let response = events
            .iter()
            .find(|event| event["event_kind"] == "tool_result")
            .expect("persisted response");
        assert_eq!(response["origin_event_id"], request_uid);
        assert_ne!(response["origin_event_id"], "request-provisional");

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_delta_cannot_advance_checkpoint_across_restart_gap() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status_for_test(crate::tee::BackendSinkStatus::CatchingUp);
        let (tx, rx) = mpsc::channel(8);
        let handle = spawn_sink_task(
            config.clone(),
            clickhouse.clone(),
            checkpoints.clone(),
            metrics,
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(cell, Arc::new(Notify::new())),
        );

        let mut replay_checkpoint = sample_checkpoint();
        replay_checkpoint.last_offset = 50;
        let key = checkpoint_key(
            &replay_checkpoint.source_name,
            &replay_checkpoint.source_file,
        );
        tx.send(SinkMessage::Batch(routed_batch(
            1,
            replay_checkpoint.clone(),
        )))
        .await
        .expect("queue replay gap");
        let mut live_checkpoint = replay_checkpoint.clone();
        live_checkpoint.last_offset = 100;
        tx.send(backend_batch(
            routed_batch(2, live_checkpoint.clone()),
            0,
            false,
        ))
        .await
        .expect("queue live delta");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue first barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("first barrier timeout")
            .expect("first barrier sender dropped");
        assert_eq!(
            checkpoints.read().await.get(&key).map(|cp| cp.last_offset),
            Some(50),
            "a live delta must not over-assert coverage beyond the replayed gap"
        );

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("first sink exits")
            .expect("first sink does not panic");

        let reloaded = Arc::new(RwLock::new(checkpoints.read().await.clone()));
        let restarted_cell = Arc::new(BackendSinkCell::new("team-ch"));
        restarted_cell.set_status_for_test(crate::tee::BackendSinkStatus::CatchingUp);
        let (tx, rx) = mpsc::channel(4);
        let restarted = spawn_sink_task(
            config,
            clickhouse,
            reloaded.clone(),
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(restarted_cell, Arc::new(Notify::new())),
        );
        tx.send(SinkMessage::Batch(routed_batch(2, live_checkpoint)))
            .await
            .expect("replay missing delta after restart");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue restart barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("restart barrier timeout")
            .expect("restart barrier sender dropped");
        assert_eq!(
            reloaded.read().await.get(&key).map(|cp| cp.last_offset),
            Some(100),
            "restart replay must close the gap before advancing its checkpoint"
        );
        assert_eq!(mock_state.rows("raw_events").len(), 3);

        drop(tx);
        timeout(Duration::from_secs(2), restarted)
            .await
            .expect("restarted sink exits")
            .expect("restarted sink does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejected_backend_batch_revokes_queued_checkpoint_authority() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("").await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = usize::MAX;
        config.ingest.flush_interval_seconds = 60.0;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status_for_test(crate::tee::BackendSinkStatus::Ok);
        let replay_notify = Arc::new(Notify::new());
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints.clone(),
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(cell.clone(), replay_notify.clone()),
        );

        let mut malformed_delta = RowBatch::default();
        malformed_delta.event_rows.push(json!({
            "session_id": "team-session",
            "cwd": "/work/team/project",
        }));
        tx.send(backend_batch(malformed_delta, 0, false))
            .await
            .expect("queue stale catch-up delta");

        let mut live_checkpoint = sample_checkpoint();
        live_checkpoint.last_offset = 200;
        let key = checkpoint_key(&live_checkpoint.source_name, &live_checkpoint.source_file);
        tx.send(backend_batch(
            routed_batch(2, live_checkpoint.clone()),
            0,
            true,
        ))
        .await
        .expect("queue post-promotion live batch");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue invalidation barrier");

        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("invalidation barrier timeout")
            .expect("invalidation barrier sender dropped");
        assert_eq!(
            cell.status(),
            crate::tee::BackendSinkStatus::CatchingUp,
            "a rejected stale delta must revoke the prior promotion"
        );
        assert!(checkpoints.read().await.get(&key).is_none());
        assert!(mock_state.rows("ingest_checkpoints").is_empty());
        assert!(
            timeout(Duration::from_millis(200), replay_notify.notified())
                .await
                .is_ok(),
            "rejection after promotion must wake replay"
        );

        tx.send(SinkMessage::Batch(routed_batch(2, live_checkpoint)))
            .await
            .expect("queue recovery replay");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue recovery barrier");
        timeout(Duration::from_secs(2), barrier_rx)
            .await
            .expect("recovery barrier timeout")
            .expect("recovery barrier sender dropped");
        assert_eq!(
            checkpoints.read().await.get(&key).map(|cp| cp.last_offset),
            Some(200)
        );

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_barrier_recovers_after_retry_drains_with_barrier_queued() {
        let state =
            MockClickHouseState::with_permanent_failure("raw_events", "intentional backend outage");
        let (clickhouse, mock_state) = spawn_mock_clickhouse_with_state(state).await;
        let mut config = moraine_config::AppConfig::default();
        config.ingest.batch_size = 1;
        config.ingest.flush_interval_seconds = 0.05;
        config.ingest.heartbeat_interval_seconds = 60.0;

        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status_for_test(crate::tee::BackendSinkStatus::Ok);
        let replay_notify = Arc::new(Notify::new());
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            Arc::new(Metrics::default()),
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            backend_role(cell.clone(), replay_notify.clone()),
        );

        tx.send(SinkMessage::Batch(routed_batch(1, sample_checkpoint())))
            .await
            .expect("queue failing batch");
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
        tx.send(SinkMessage::FlushBarrier(barrier_tx))
            .await
            .expect("queue barrier behind failed flush");

        let deadline = Instant::now() + Duration::from_secs(2);
        while cell.status() != crate::tee::BackendSinkStatus::Unreachable
            && Instant::now() < deadline
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            cell.status(),
            crate::tee::BackendSinkStatus::Unreachable,
            "the first flush must place the backend in unreachable"
        );
        mock_state
            .fail_always_by_table
            .lock()
            .expect("mock fail_always mutex poisoned")
            .remove("raw_events");

        timeout(Duration::from_secs(3), barrier_rx)
            .await
            .expect("barrier recovery timeout")
            .expect("barrier sender dropped");
        assert_eq!(
            cell.status(),
            crate::tee::BackendSinkStatus::CatchingUp,
            "an empty ordered barrier must complete recovery after retry drained"
        );
        assert!(
            timeout(Duration::from_millis(200), replay_notify.notified())
                .await
                .is_ok(),
            "barrier recovery must wake the catch-up supervisor"
        );

        drop(tx);
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("sink exits")
            .expect("sink task does not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_pending_retries_only_unfinished_tables_under_original_fence() {
        let (clickhouse, mock_state) = spawn_mock_clickhouse("events").await;
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let publication_identity = test_publication_identity();
        let publication_actor = PublicationActor::new(
            clickhouse.clone(),
            String::new(),
            publication_identity.publisher_id().to_string(),
        );
        let ack_observer = IngestAckObserver::new(false);
        let mut pending_ack = PendingAckBatch::default();

        let mut raw_rows = vec![json!({
            "source_name": "source-a",
            "source_file": "/tmp/source-a.jsonl",
            "source_generation": 1,
            "record_ts": "2026-02-17T00:00:01.000Z",
            "event_uid": "evt-1"
        })];
        let mut event_rows = vec![json!({
            "source_name": "source-a",
            "source_file": "/tmp/source-a.jsonl",
            "source_generation": 1,
            "event_uid": "evt-1"
        })];
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();
        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value.clone(), checkpoint)]);

        let first_attempt = flush_pending_with_ack(
            &clickhouse,
            &checkpoints,
            Some(&publication_actor),
            &metrics,
            &mut raw_rows,
            &mut event_rows,
            &mut link_rows,
            &mut error_rows,
            &mut checkpoint_updates,
            true,
            "",
            &ack_observer,
            &mut pending_ack,
        )
        .await;
        assert!(!first_attempt, "first flush should fail at events stage");
        assert!(raw_rows.is_empty(), "raw rows should not be retried");
        assert_eq!(event_rows.len(), 1, "event rows remain pending");
        assert_eq!(checkpoint_updates.len(), 1, "checkpoint remains pending");
        let retained_batch_id = pending_ack
            .append_manifest
            .as_ref()
            .expect("original append manifest retained")
            .batch_id();

        let second_attempt = flush_pending_with_ack(
            &clickhouse,
            &checkpoints,
            Some(&publication_actor),
            &metrics,
            &mut raw_rows,
            &mut event_rows,
            &mut link_rows,
            &mut error_rows,
            &mut checkpoint_updates,
            true,
            "",
            &ack_observer,
            &mut pending_ack,
        )
        .await;
        assert!(
            second_attempt,
            "retry should complete under original fence: {}",
            metrics
                .last_error
                .lock()
                .expect("metrics last_error mutex poisoned")
        );
        assert!(event_rows.is_empty());
        assert!(checkpoint_updates.is_empty());
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.flush_failures.load(Ordering::Relaxed), 1);
        assert_eq!(mock_state.call_count("raw_events"), 1);
        assert_eq!(mock_state.call_count("events"), 2);
        assert_eq!(mock_state.call_count("ingest_checkpoints"), 1);

        let append_controls = mock_state.rows("ingest_append_control");
        assert!(append_controls.iter().all(|row| {
            row.get("batch_id").and_then(Value::as_str) == Some(retained_batch_id.as_str())
        }));
        assert_eq!(
            append_controls
                .last()
                .and_then(|row| row.get("state"))
                .and_then(Value::as_str),
            Some("idle"),
            "retried staged flush must quiesce its original fence"
        );
        assert!(
            checkpoints.read().await.contains_key(&checkpoint_key_value),
            "checkpoint cache should advance after retry"
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
        let publication_identity = test_publication_identity();
        let publication_actor = PublicationActor::new(
            clickhouse.clone(),
            String::new(),
            publication_identity.publisher_id().to_string(),
        );
        let ack_observer = IngestAckObserver::new(false);
        let mut pending_ack = PendingAckBatch::default();

        let checkpoint = sample_checkpoint();
        let checkpoint_key_value = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value.clone(), checkpoint)]);
        let mut raw_rows = vec![json!({
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
        })];
        let mut event_rows = vec![json!({"event_uid": "evt-fallback"})];
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        assert!(
            flush_pending_with_ack(
                &clickhouse,
                &checkpoints,
                Some(&publication_actor),
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
                &ack_observer,
                &mut pending_ack,
            )
            .await,
            "non-retryable oversized JSONEachRow failures should clear the stuck batch"
        );

        assert!(raw_rows.is_empty());
        assert!(event_rows.is_empty());
        assert!(checkpoint_updates.is_empty());
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
        let append_controls = mock_state.rows("ingest_append_control");
        assert_eq!(
            append_controls
                .last()
                .and_then(|row| row.get("state"))
                .and_then(Value::as_str),
            Some("idle"),
            "dropping a non-retryable batch must quiesce its prepared append fence"
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
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value, checkpoint)]);
        let mut raw_rows = vec![json!({
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
        })];
        let mut event_rows = vec![json!({"event_uid": "evt-error-record-fails"})];
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        assert!(
            !flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
            )
            .await,
            "fallback should remain retryable when the compact error row cannot be written"
        );

        assert_eq!(raw_rows.len(), 1);
        assert_eq!(event_rows.len(), 1);
        assert_eq!(checkpoint_updates.len(), 1);
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
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value.clone(), checkpoint)]);
        let mut raw_rows = vec![json!({
            "record_ts": "2026-02-17T00:00:01.000Z",
            "event_uid": "evt-later-stage",
            "raw_json": "{}",
        })];
        let mut event_rows = vec![json!({
            "event_uid": "evt-later-stage",
            "source_name": "source-a",
            "source_file": "/tmp/source-a.jsonl",
            "source_line_no": 17u64,
            "source_offset": 4096u64,
        })];
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        assert!(
            flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
            )
            .await,
            "non-retryable fallback after a partial commit should finish by checkpointing"
        );

        assert!(raw_rows.is_empty());
        assert!(event_rows.is_empty());
        assert!(checkpoint_updates.is_empty());
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
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value, checkpoint)]);
        let mut raw_rows = vec![
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
        ];
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        assert!(
            flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
            )
            .await,
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
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value, checkpoint)]);
        let mut raw_rows = vec![json!({
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
        })];
        let mut event_rows = vec![json!({"event_uid": "evt-retry"})];
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        assert!(
            !flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
            )
            .await,
            "other ClickHouse Code 117 failures should remain retryable"
        );

        assert_eq!(raw_rows.len(), 1);
        assert_eq!(event_rows.len(), 1);
        assert_eq!(checkpoint_updates.len(), 1);
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
        let mut checkpoint_updates = HashMap::from([(checkpoint_key_value.clone(), checkpoint)]);
        let mut raw_rows = vec![json!({
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
        })];
        let mut event_rows = Vec::<Value>::new();
        let mut link_rows = Vec::<Value>::new();
        let mut error_rows = Vec::<Value>::new();

        assert!(
            flush_pending(
                &clickhouse,
                &checkpoints,
                &metrics,
                &mut raw_rows,
                &mut event_rows,
                &mut link_rows,
                &mut error_rows,
                &mut checkpoint_updates,
                true,
                "",
            )
            .await,
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
        assert!(checkpoint_updates.is_empty());
        assert_eq!(metrics.raw_rows_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.err_rows_written.load(Ordering::Relaxed), 1);

        let state = checkpoints.read().await;
        assert!(
            state.contains_key(&checkpoint_key_value),
            "checkpoint cache should advance after quarantining the row"
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

        let handle = spawn_sink_task(
            config,
            clickhouse,
            checkpoints,
            metrics,
            rx,
            true,
            test_publication_identity(),
            SinkAuthorConfig::fully_supported(String::new()),
            SinkRole::Backend {
                cell: Arc::new(BackendSinkCell::new("team-ch")),
                resolver,
                replay_notify: Arc::new(Notify::new()),
                redactor: test_redactor(),
                redactions: redactions.clone(),
            },
        );

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
        let mut error_rows = Vec::<Value>::new();

        let ahead = sample_checkpoint(); // offset 100
        let key = checkpoint_key(&ahead.source_name, &ahead.source_file);
        let mut behind = sample_checkpoint();
        behind.last_offset = 50;

        for cp in [ahead, behind] {
            let mut checkpoint_updates = HashMap::new();
            checkpoint_updates.insert(key.clone(), cp);
            assert!(
                flush_pending(
                    &clickhouse,
                    &checkpoints,
                    &metrics,
                    &mut raw_rows,
                    &mut event_rows,
                    &mut link_rows,
                    &mut error_rows,
                    &mut checkpoint_updates,
                    true,
                    "",
                )
                .await
            );
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
    async fn durable_drain_enters_catch_up_and_notifies_supervisor() {
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status_for_test(crate::tee::BackendSinkStatus::Lagging);
        let replay_notify = Arc::new(Notify::new());
        let role = SinkRole::Backend {
            cell: cell.clone(),
            resolver: Arc::new(Mutex::new(crate::tee::RouteResolver::new(Arc::new(
                moraine_config::AppConfig::default(),
            )))),
            replay_notify: replay_notify.clone(),
            redactor: test_redactor(),
            redactions: test_redaction_audit(),
        };

        note_flush_outcome(&role, true, true);

        assert_eq!(
            cell.status(),
            crate::tee::BackendSinkStatus::CatchingUp,
            "durable drain must pause live admission until replay's barrier"
        );
        assert!(
            timeout(Duration::from_millis(100), replay_notify.notified())
                .await
                .is_ok(),
            "recovery must schedule a replay pass"
        );
    }

    #[test]
    fn compute_append_to_visible_stats_uses_ingest_observation_times() {
        let visible_at = Instant::now();
        let samples = vec![
            (visible_at - Duration::from_secs(10), 1),
            (visible_at - Duration::from_secs(5), 1),
            (visible_at - Duration::from_secs(1), 1),
        ];

        let (p50, p95) =
            compute_append_to_visible_stats(&samples, visible_at).expect("percentile stats");

        assert_eq!(p50, 5_000);
        assert_eq!(p95, 10_000);
    }

    #[test]
    fn compute_append_to_visible_stats_weights_batch_row_counts() {
        let visible_at = Instant::now();
        let samples = vec![
            (visible_at - Duration::from_secs(10), 1),
            (visible_at - Duration::from_secs(1), 19),
        ];

        let (p50, p95) =
            compute_append_to_visible_stats(&samples, visible_at).expect("percentile stats");

        assert_eq!(p50, 1_000);
        assert_eq!(p95, 1_000);
        assert!(compute_append_to_visible_stats(&[], visible_at).is_none());
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
