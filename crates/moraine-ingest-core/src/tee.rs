//! Multi-backend tee for routed sessions.
//!
//! The default sink always receives every batch; a session whose sticky
//! working directory matches a route is *additionally* mirrored to that
//! route's named backend, which runs its own sink task writing rows AND
//! `ingest_checkpoints` into the backend's own database.
//!
//! The default path must never stall on a remote: forwarding toward a
//! backend uses `try_send` into a bounded queue, and overflow marks the
//! backend lagging and stops queueing. Because the source session files are
//! durable on disk they act as the WAL — a targeted replay pass (driven by
//! the backend's own checkpoints, reusing `process_file`) closes any gap at
//! startup and whenever a lagging/unreachable backend drains again.

use crate::dispatch::process_file;
use crate::heartbeat::host_name;
use crate::model::{Checkpoint, RowBatch};
use crate::redaction::{RedactionAudit, SecretRedactor};
use crate::sink::{spawn_sink_task, SinkRole};
use crate::watch::enumerate_tracked_files;
use crate::{Metrics, SinkMessage, WorkItem};
use moraine_clickhouse::{enforce_remote_schema_policy, ClickHouseClient};
use moraine_config::{AppConfig, ClickHouseConfig, IngestSource, DEFAULT_BACKEND_NAME};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Backoff between schema-handshake attempts against a backend that is not
/// answering. Failed handshakes never affect the default sink.
const HANDSHAKE_RETRY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackendSinkStatus {
    /// Constructed but the schema handshake has not completed yet.
    Connecting,
    Ok,
    /// Mirror queue overflowed; live forwarding is paused until the sink
    /// drains, then catch-up replay recovers the dropped span.
    Lagging,
    /// The backend is not answering (handshake or flush failures).
    Unreachable,
    /// Schema skew policy rejected the backend; terminal until restart.
    DisabledSkew,
    /// `[identity].author` is unset but this backend is a mirror target;
    /// terminal until an author is configured and ingest restarts. Rows
    /// keep landing on the default sink and catch-up replay backfills the
    /// backend once it starts (issue #381).
    DisabledMissingIdentity,
}

impl BackendSinkStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Ok => "ok",
            Self::Lagging => "lagging",
            Self::Unreachable => "unreachable",
            Self::DisabledSkew => "disabled_skew",
            Self::DisabledMissingIdentity => "disabled_missing_identity",
        }
    }
}

/// Shared per-backend state: status transitions are driven by the router
/// (overflow), the backend's sink task (flush outcomes), and its supervisor
/// (handshake); the default sink's heartbeat reads it.
pub(crate) struct BackendSinkCell {
    name: String,
    status: Mutex<BackendSinkStatus>,
    /// Sink task running post-handshake; the router only forwards to live sinks.
    live: AtomicBool,
    /// Routed batches that were not mirrored (overflow or non-ok status).
    /// Catch-up replay recovers the data; the count is diagnostic.
    dropped_batches: AtomicU64,
}

impl BackendSinkCell {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            status: Mutex::new(BackendSinkStatus::Connecting),
            live: AtomicBool::new(false),
            dropped_batches: AtomicU64::new(0),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn status(&self) -> BackendSinkStatus {
        *self.status.lock().expect("backend status mutex poisoned")
    }

    pub(crate) fn set_status(&self, status: BackendSinkStatus) {
        *self.status.lock().expect("backend status mutex poisoned") = status;
    }

    fn is_live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }

    fn mark_live(&self) {
        self.live.store(true, Ordering::Relaxed);
    }

    fn record_drop(&self) {
        self.dropped_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dropped_batches(&self) -> u64 {
        self.dropped_batches.load(Ordering::Relaxed)
    }

    /// Router-side: queue overflowed. Returns true on the Ok -> Lagging
    /// transition so the caller can warn once instead of once per batch.
    fn mark_overflow(&self) -> bool {
        let mut status = self.status.lock().expect("backend status mutex poisoned");
        if *status == BackendSinkStatus::Ok {
            *status = BackendSinkStatus::Lagging;
            return true;
        }
        false
    }

    /// Sink-side: a flush failed. Returns true on the transition to
    /// Unreachable so the caller can warn once per outage.
    pub(crate) fn mark_flush_failure(&self) -> bool {
        let mut status = self.status.lock().expect("backend status mutex poisoned");
        match *status {
            BackendSinkStatus::Ok | BackendSinkStatus::Lagging => {
                *status = BackendSinkStatus::Unreachable;
                true
            }
            _ => false,
        }
    }

    /// Sink-side: a flush succeeded with an empty queue. Returns true on the
    /// transition back to Ok so the caller can schedule catch-up replay.
    pub(crate) fn mark_recovered(&self) -> bool {
        let mut status = self.status.lock().expect("backend status mutex poisoned");
        match *status {
            BackendSinkStatus::Lagging | BackendSinkStatus::Unreachable => {
                *status = BackendSinkStatus::Ok;
                true
            }
            _ => false,
        }
    }
}

/// Backend status cells keyed by backend name, shared with the default
/// sink's heartbeat. `BTreeMap` keeps the heartbeat JSON deterministic.
pub(crate) type StatusRegistry = Arc<Mutex<BTreeMap<String, Arc<BackendSinkCell>>>>;

/// Checkpoint floor for the next catch-up replay pass, written by the
/// backend's sink task at the recovery transition and taken by its
/// supervisor when the pass is scheduled. Replay must never resume from the
/// live-updating checkpoint map: live batches carry the default pipeline's
/// offsets, and adopting one for a file the pass has not covered yet would
/// silently skip the very span the pass exists to recover.
pub(crate) type ReplayFloor = Arc<Mutex<Option<HashMap<String, Checkpoint>>>>;

/// JSON-encoded `{backend: status}` map for the heartbeat's `backend_sinks`
/// column; `None` when no backend sinks exist (so default-only installs
/// never reference the migration-017 column).
pub(crate) fn backend_sinks_json(registry: &StatusRegistry) -> Option<String> {
    let cells = registry.lock().expect("backend registry mutex poisoned");
    if cells.is_empty() {
        return None;
    }
    let map: serde_json::Map<String, Value> = cells
        .iter()
        .map(|(name, cell)| {
            (
                name.clone(),
                Value::String(cell.status().as_str().to_string()),
            )
        })
        .collect();
    Some(Value::Object(map).to_string())
}

pub(crate) type SharedRouteResolver = Arc<Mutex<RouteResolver>>;

/// Repo `.moraine.toml` lookup, injectable so resolver tests run without a
/// filesystem walk-up.
type RepoBackendLookup = Box<dyn Fn(&str) -> Option<String> + Send>;

/// Resolves a session to its mirror backend (or none) and pins the answer:
/// the first non-empty cwd observed for a session in this process decides
/// its route, so a mid-session `cd` never splits a session across backends.
/// Home-config `[[routes]]` win over repo `.moraine.toml` references, and an
/// unknown repo-referenced name warns once and falls back to default-only
/// (the trust boundary: a cloned repo can only name backends the user
/// already configured).
pub(crate) struct RouteResolver {
    config: Arc<AppConfig>,
    /// False when only the default backend is configured: resolution (and
    /// the repo-file filesystem walk in particular) is skipped entirely.
    routing_enabled: bool,
    repo_lookup: RepoBackendLookup,
    /// session_id -> pinned backend name (None = default only). Entries are
    /// only created once a non-empty cwd has been seen for the session.
    sticky: HashMap<String, Option<String>>,
    warned_unknown: HashSet<String>,
}

impl RouteResolver {
    pub(crate) fn new(config: Arc<AppConfig>) -> Self {
        Self::with_repo_lookup(
            config,
            Box::new(|dir| moraine_config::find_repo_backend_ref(dir)),
        )
    }

    fn with_repo_lookup(config: Arc<AppConfig>, repo_lookup: RepoBackendLookup) -> Self {
        let routing_enabled = config
            .backends
            .keys()
            .any(|name| name != DEFAULT_BACKEND_NAME);
        Self {
            config,
            routing_enabled,
            repo_lookup,
            sticky: HashMap::new(),
            warned_unknown: HashSet::new(),
        }
    }

    /// Mirror backend for one row's session, given the cwd the row carries
    /// (empty for rows without one, e.g. link/tool rows).
    pub(crate) fn resolve(&mut self, session_id: &str, cwd: &str) -> Option<String> {
        if !self.routing_enabled || session_id.is_empty() {
            return None;
        }
        if let Some(pinned) = self.sticky.get(session_id) {
            return pinned.clone();
        }
        let cwd = cwd.trim();
        if cwd.is_empty() {
            // Not cached: a later row may carry the session's cwd.
            return None;
        }
        let resolved = self.resolve_cwd(cwd);
        self.sticky.insert(session_id.to_string(), resolved.clone());
        resolved
    }

    fn resolve_cwd(&mut self, cwd: &str) -> Option<String> {
        if let Some(route) = self.config.route_for_dir(cwd) {
            let backend = route.backend.clone();
            return self.validate_backend_name(backend, cwd);
        }
        if let Some(name) = (self.repo_lookup)(cwd) {
            return self.validate_backend_name(name, cwd);
        }
        None
    }

    fn validate_backend_name(&mut self, name: String, cwd: &str) -> Option<String> {
        if name == DEFAULT_BACKEND_NAME {
            // Explicitly routing to default is a no-op: it already mirrors.
            return None;
        }
        if self.config.backends.contains_key(&name) {
            return Some(name);
        }
        if self.warned_unknown.insert(name.clone()) {
            warn!(
                backend = %name,
                cwd,
                "route names a backend with no [backends.{}] entry in the home config; \
                 sessions resolve to the default backend only",
                name
            );
        }
        None
    }
}

/// Distinct mirror backends referenced by any row in the batch. Iterates
/// cwd-bearing rows (raw, then event) before cwd-less link/tool rows so the
/// sticky cwd is pinned before session-only rows resolve against it.
fn collect_targets(batch: &RowBatch, resolver: &mut RouteResolver) -> BTreeSet<String> {
    let mut targets = BTreeSet::new();
    for row in batch
        .raw_rows
        .iter()
        .chain(&batch.event_rows)
        .chain(&batch.link_rows)
        .chain(&batch.tool_rows)
    {
        if let Some(name) = row_route(row, resolver) {
            targets.insert(name);
        }
    }
    targets
}

fn row_route(row: &Value, resolver: &mut RouteResolver) -> Option<String> {
    let session_id = row.get("session_id").and_then(Value::as_str).unwrap_or("");
    let cwd = row.get("cwd").and_then(Value::as_str).unwrap_or("");
    resolver.resolve(session_id, cwd)
}

/// Sub-batch destined for one backend: rows whose session resolves to it.
/// Error rows are never mirrored (they describe files, not sessions). The
/// checkpoint is kept even when zero rows match — it records "this backend
/// has seen everything routed to it from this file up to here", which is
/// what lets replay passes converge instead of rereading unrouted files
/// (some sessions never carry a cwd at all, so deferring the carry until
/// resolution would re-read their files forever). Known limitation: rows
/// filtered while their session's route is still unresolved are covered by
/// that checkpoint too, so a session pinning to this backend late arrives
/// without the unresolved prefix. The harness adapters keep that window
/// effectively empty — record-level cwd (claude_code), file-head recovery
/// (codex/pi), composer workspace stamping on bubbles (cursor_sqlite).
pub(crate) fn filter_batch_for_backend(
    batch: &RowBatch,
    backend: &str,
    resolver: &mut RouteResolver,
) -> RowBatch {
    let routed = |row: &Value, resolver: &mut RouteResolver| {
        row_route(row, resolver).as_deref() == Some(backend)
    };

    let mut out = RowBatch::default();
    // Raw and event rows carry cwd and must feed the resolver before the
    // cwd-less link/tool rows are judged (same ordering as collect_targets).
    for row in &batch.raw_rows {
        if routed(row, resolver) {
            out.push_raw_row(row.clone());
        }
    }
    out.extend_event_rows(
        batch
            .event_rows
            .iter()
            .filter(|row| routed(row, resolver))
            .cloned(),
    );
    out.extend_link_rows(
        batch
            .link_rows
            .iter()
            .filter(|row| routed(row, resolver))
            .cloned(),
    );
    out.extend_tool_rows(
        batch
            .tool_rows
            .iter()
            .filter(|row| routed(row, resolver))
            .cloned(),
    );
    out.checkpoint = batch.checkpoint.clone();
    out.lines_processed = batch.lines_processed;
    out
}

struct BackendHandle {
    tx: mpsc::Sender<SinkMessage>,
    cell: Arc<BackendSinkCell>,
}

#[derive(Clone)]
pub(crate) struct RedactionContext {
    redactor: Arc<SecretRedactor>,
    audit: Arc<RedactionAudit>,
}

impl RedactionContext {
    pub(crate) fn new(redactor: Arc<SecretRedactor>, audit: Arc<RedactionAudit>) -> Self {
        Self { redactor, audit }
    }
}

#[derive(Clone)]
struct TeeRouterContext {
    config: Arc<AppConfig>,
    sources: Arc<Vec<IngestSource>>,
    resolver: SharedRouteResolver,
    registry: StatusRegistry,
    redaction: RedactionContext,
}

/// Routes every batch from the processors to the default sink (always,
/// awaited — the existing backpressure) and tees full batches toward mirror
/// backends via `try_send` (never awaited — a slow remote cannot stall the
/// default path). Backend sinks filter their intake themselves, so the
/// router only decides *whether* a batch is relevant to a backend.
pub(crate) fn spawn_tee_router(
    config: Arc<AppConfig>,
    sources: Vec<IngestSource>,
    mut rx: mpsc::Receiver<SinkMessage>,
    default_tx: mpsc::Sender<SinkMessage>,
    resolver: SharedRouteResolver,
    registry: StatusRegistry,
    redaction: RedactionContext,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let context = TeeRouterContext {
            config,
            sources: Arc::new(sources),
            resolver,
            registry,
            redaction,
        };
        let mut handles = HashMap::<String, BackendHandle>::new();

        // Backends named by home-config routes start eagerly so handshake
        // failures surface at startup. Backends referenced only by repo
        // `.moraine.toml` files are constructed on first resolution — we
        // cannot know at startup which configured backends repos will name,
        // and eagerly replaying never-routed backends would write
        // checkpoints into databases the user never routed to.
        let eager: BTreeSet<String> = context
            .config
            .routes
            .iter()
            .map(|route| route.backend.clone())
            .filter(|name| name != DEFAULT_BACKEND_NAME)
            .collect();
        for name in eager {
            ensure_backend(&name, &context, &mut handles);
        }

        while let Some(SinkMessage::Batch(mut batch)) = rx.recv().await {
            redact_default_batch_if_enabled(&context.config, &context.redaction, &mut batch);

            let targets = {
                let mut resolver = context
                    .resolver
                    .lock()
                    .expect("route resolver mutex poisoned");
                collect_targets(&batch, &mut resolver)
            };

            for name in &targets {
                if let Some(handle) = ensure_backend(name, &context, &mut handles) {
                    forward_to_backend(handle, &batch);
                }
            }

            if default_tx.send(SinkMessage::Batch(batch)).await.is_err() {
                break;
            }
        }
    })
}

fn redact_default_batch_if_enabled(
    config: &AppConfig,
    redaction: &RedactionContext,
    batch: &mut RowBatch,
) {
    if config.redaction.dangerously_skip_secret_redaction {
        return;
    }

    let report = redaction.redactor.redact_batch(batch);
    redaction.audit.record(&report);
}

/// Looks up (or lazily constructs) the handle for a named backend. Returns
/// `None` only for names without a `[backends.*]` entry, which the resolver
/// already refuses to produce — this is a defensive double-check.
fn ensure_backend<'a>(
    name: &str,
    context: &TeeRouterContext,
    handles: &'a mut HashMap<String, BackendHandle>,
) -> Option<&'a BackendHandle> {
    if !handles.contains_key(name) {
        let backend_cfg = context.config.backends.get(name)?.clone();
        let cell = Arc::new(BackendSinkCell::new(name));
        let queue_capacity = context.config.ingest.max_inflight_batches.max(1);
        let (tx, backend_rx) = mpsc::channel::<SinkMessage>(queue_capacity);

        context
            .registry
            .lock()
            .expect("backend registry mutex poisoned")
            .insert(name.to_string(), cell.clone());

        info!(backend = name, "starting mirror sink for backend");
        spawn_backend_supervisor(
            backend_cfg,
            cell.clone(),
            tx.clone(),
            backend_rx,
            context.clone(),
        );

        handles.insert(name.to_string(), BackendHandle { tx, cell });
    }
    handles.get(name)
}

fn forward_to_backend(handle: &BackendHandle, batch: &RowBatch) {
    if !handle.cell.is_live() || handle.cell.status() != BackendSinkStatus::Ok {
        // Not accepting (connecting / lagging / unreachable / disabled):
        // drop the mirror copy. Replay recovers it from the source file.
        handle.cell.record_drop();
        return;
    }

    match handle.tx.try_send(SinkMessage::Batch(batch.clone())) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            handle.cell.record_drop();
            if handle.cell.mark_overflow() {
                warn!(
                    backend = handle.cell.name(),
                    "mirror queue full; backend marked lagging and live mirroring paused \
                     (catch-up replay will recover the gap once it drains)"
                );
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            handle.cell.record_drop();
            if handle.cell.mark_flush_failure() {
                warn!(
                    backend = handle.cell.name(),
                    "mirror sink channel closed unexpectedly; backend marked unreachable"
                );
            }
        }
    }
}

/// Owns one backend's lifecycle: schema handshake (moraine NEVER migrates a
/// non-default backend — skew disables the sink loudly instead), checkpoint
/// load from the backend's own database, sink task spawn, then catch-up
/// replay passes re-armed on every recovery.
fn spawn_backend_supervisor(
    backend_cfg: ClickHouseConfig,
    cell: Arc<BackendSinkCell>,
    replay_tx: mpsc::Sender<SinkMessage>,
    backend_rx: mpsc::Receiver<SinkMessage>,
    context: TeeRouterContext,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let name = cell.name().to_string();

        // Identity is required to mirror (issue #381): rows on a shared
        // backend that nobody can attribute would silently anonymize team
        // data, which is worse than refusing to send it. Gated before any
        // network contact; terminal until restart, like schema skew. The
        // default sink is unaffected, and catch-up replay backfills this
        // backend once an author is configured.
        if context.config.identity.author.is_empty() {
            cell.set_status(BackendSinkStatus::DisabledMissingIdentity);
            error!(
                backend = %name,
                "mirror sink disabled: [identity].author is not set in the moraine config; \
                 routed sessions still ingest into the default backend, and catch-up replay \
                 will backfill this backend after you set an author and restart the ingest \
                 service"
            );
            return;
        }

        let allow_newer_server = backend_cfg.allow_newer_server;
        let client = match ClickHouseClient::new(backend_cfg) {
            Ok(client) => client,
            Err(exc) => {
                cell.set_status(BackendSinkStatus::Unreachable);
                error!(backend = %name, "failed to construct clickhouse client: {exc}");
                return;
            }
        };

        loop {
            match client.schema_skew().await {
                Ok(skew) => {
                    match enforce_remote_schema_policy(&name, &skew, allow_newer_server) {
                        Ok(()) => break,
                        Err(exc) => {
                            // Terminal until restart; the default sink is unaffected.
                            cell.set_status(BackendSinkStatus::DisabledSkew);
                            error!(backend = %name, "backend sink disabled (schema skew): {exc}");
                            return;
                        }
                    }
                }
                Err(exc) => {
                    cell.set_status(BackendSinkStatus::Unreachable);
                    warn!(
                        backend = %name,
                        "schema handshake failed; retrying in {}s: {exc}",
                        HANDSHAKE_RETRY.as_secs()
                    );
                    tokio::time::sleep(HANDSHAKE_RETRY).await;
                }
            }
        }

        // The backend's own checkpoints drive both its sink and its replay.
        // The handshake guarantees every bundled migration is applied on the
        // server, so the SQLite cursor columns (015) and the host scoping
        // column (018) are always present. Loading is host-filtered: team
        // backends share one ingest_checkpoints table across members, and
        // another host's rows must never decide what THIS host replays.
        let host = host_name();
        let durable = loop {
            match crate::load_checkpoints(&client, true, Some(&host)).await {
                Ok(map) => break map,
                Err(exc) => {
                    cell.set_status(BackendSinkStatus::Unreachable);
                    warn!(
                        backend = %name,
                        "failed to load backend checkpoints; retrying in {}s: {exc}",
                        HANDSHAKE_RETRY.as_secs()
                    );
                    tokio::time::sleep(HANDSHAKE_RETRY).await;
                }
            }
        };
        // The durable checkpoints, snapshotted before the sink can advance
        // them, are the first replay pass's floor: the pass must resume
        // from state the backend has actually flushed, never from the live
        // map (see `ReplayFloor`).
        let mut floor = Arc::new(RwLock::new(durable.clone()));
        let checkpoints = Arc::new(RwLock::new(durable));

        let metrics = Arc::new(Metrics::default());
        let replay_notify = Arc::new(Notify::new());
        let replay_floor: ReplayFloor = Arc::new(Mutex::new(None));
        spawn_sink_task(
            context.config.as_ref().clone(),
            client,
            checkpoints.clone(),
            metrics.clone(),
            backend_rx,
            true,
            SinkRole::Backend {
                cell: cell.clone(),
                resolver: context.resolver.clone(),
                replay_notify: replay_notify.clone(),
                replay_floor: replay_floor.clone(),
                redactor: context.redaction.redactor.clone(),
                redactions: context.redaction.audit.clone(),
            },
        );

        // Go live before replaying: every batch the router forwards from
        // here on covers file state newer than the snapshot floor above, so
        // the pass and live mirroring overlap rather than gap — the pass
        // closes everything between the floor and "now" (duplicate spans
        // collapse via event-UID ReplacingMergeTree, like the rest of the
        // pipeline).
        cell.set_status(BackendSinkStatus::Ok);
        cell.mark_live();
        info!(backend = %name, "backend sink live; starting catch-up replay");

        let mut reported_lost = HashSet::<String>::new();
        loop {
            run_replay_pass(
                &context.config,
                context.sources.as_slice(),
                &floor,
                &replay_tx,
                &metrics,
                &name,
                &mut reported_lost,
            )
            .await;
            // Re-arm on the next lagging/unreachable -> ok recovery. The
            // notify holds a permit, so a recovery that fires mid-pass
            // immediately schedules another pass for the span it missed.
            replay_notify.notified().await;
            // Adopt the floor the sink captured at the recovery transition
            // (by now the live map may already carry post-recovery
            // checkpoints that jump the dropped span). A missing capture
            // keeps the previous, lower floor — which only ever means
            // re-reading more, never skipping.
            if let Some(captured) = replay_floor
                .lock()
                .expect("replay floor mutex poisoned")
                .take()
            {
                floor = Arc::new(RwLock::new(captured));
            }
            info!(backend = %name, "backend recovered; replaying to catch up");
        }
    })
}

/// One targeted catch-up pass: re-enqueue every tracked file against a
/// floor snapshot of the backend's own checkpoints (never the live map the
/// sink advances — see `ReplayFloor`). `process_file` early-exits for
/// caught-up files (a stat + offset compare), so a pass over a converged
/// tree is cheap; files the backend has never seen are read in full,
/// filtered at the sink, and checkpointed so the next pass's floor skips
/// them.
#[allow(clippy::too_many_arguments)]
async fn run_replay_pass(
    config: &AppConfig,
    sources: &[IngestSource],
    floor: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    tx: &mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
    backend: &str,
    reported_lost: &mut HashSet<String>,
) {
    for source in sources {
        let files = match enumerate_tracked_files(&source.glob, &source.format) {
            Ok(files) => files,
            Err(exc) => {
                warn!(
                    backend,
                    source = %source.name,
                    "replay enumerate failed: {exc}"
                );
                continue;
            }
        };

        // A file deleted before this backend caught up is lost to this
        // backend only (the default sink already ingested it live). The
        // floor only ever holds this host's checkpoints (load is
        // host-filtered), so teammates' files on a shared backend never
        // trip this warning.
        {
            let enumerated: HashSet<&str> = files.iter().map(String::as_str).collect();
            let map = floor.read().await;
            for cp in map.values() {
                if cp.source_name == source.name
                    && !enumerated.contains(cp.source_file.as_str())
                    && reported_lost.insert(cp.source_file.clone())
                {
                    warn!(
                        backend,
                        file = %cp.source_file,
                        "source file deleted; rows appended after offset {} never reached \
                         this backend and are lost to it",
                        cp.last_offset
                    );
                }
            }
        }

        // Replay reads against this backend's own floor, not the live
        // pipeline's, so it gets a fresh volatile map: every file scans.
        let poll_state = crate::sqlite_poll::VolatilePollMap::new();
        for path in files {
            let work = WorkItem {
                source_name: source.name.clone(),
                harness: source.harness.clone(),
                format: source.format.clone(),
                path,
            };
            // Sends are awaited: replay backpressure is bounded by the
            // backend's own queue and never touches the default path.
            if let Err(exc) = process_file(
                config,
                &work,
                floor.clone(),
                &poll_state,
                tx.clone(),
                metrics,
            )
            .await
            {
                warn!(
                    backend,
                    source = %work.source_name,
                    path = %work.path,
                    "targeted replay failed: {exc}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::checkpoint_key;
    use crate::model::Checkpoint;
    use axum::{
        extract::{Query, State},
        http::StatusCode,
        routing::post,
        Router,
    };
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::time::timeout;

    fn team_config() -> Arc<AppConfig> {
        let mut config = AppConfig::default();
        let team = ClickHouseConfig {
            database: "moraine_team".to_string(),
            ..Default::default()
        };
        config.backends.insert("team-ch".to_string(), team);
        config.routes.push(moraine_config::RouteConfig {
            dir: "/work/team/**".to_string(),
            backend: "team-ch".to_string(),
            mode: moraine_config::ROUTE_MODE_MIRROR.to_string(),
        });
        // Mirror targets require an identity (issue #381); supervisor tests
        // that exercise post-gate behavior must get past the gate.
        config.identity.author = "alice@example.com".to_string();
        Arc::new(config)
    }

    fn resolver_with_lookup(
        config: Arc<AppConfig>,
        lookup: impl Fn(&str) -> Option<String> + Send + 'static,
    ) -> RouteResolver {
        RouteResolver::with_repo_lookup(config, Box::new(lookup))
    }

    fn row(session_id: &str, cwd: &str) -> Value {
        json!({ "session_id": session_id, "cwd": cwd })
    }

    #[test]
    fn home_route_wins_over_repo_reference() {
        let mut config = (*team_config()).clone();
        config
            .backends
            .insert("repo-ch".to_string(), ClickHouseConfig::default());
        let mut resolver = resolver_with_lookup(Arc::new(config), |_| Some("repo-ch".to_string()));

        assert_eq!(
            resolver.resolve("s1", "/work/team/project"),
            Some("team-ch".to_string()),
            "home-config routes take precedence over repo .moraine.toml refs"
        );
    }

    #[test]
    fn repo_reference_resolves_when_no_home_route_matches() {
        let mut resolver = resolver_with_lookup(team_config(), |dir| {
            assert_eq!(dir, "/elsewhere/project");
            Some("team-ch".to_string())
        });

        assert_eq!(
            resolver.resolve("s1", "/elsewhere/project"),
            Some("team-ch".to_string())
        );
    }

    #[test]
    fn unknown_repo_backend_warns_once_and_resolves_to_default_only() {
        let mut resolver =
            resolver_with_lookup(team_config(), |_| Some("not-configured".to_string()));

        assert_eq!(resolver.resolve("s1", "/elsewhere/a"), None);
        assert_eq!(resolver.resolve("s2", "/elsewhere/b"), None);
        assert_eq!(
            resolver.warned_unknown.len(),
            1,
            "unknown backend name warns once per name, not once per session"
        );
    }

    #[test]
    fn first_non_empty_cwd_pins_the_session_route() {
        let mut resolver = resolver_with_lookup(team_config(), |_| None);

        assert_eq!(
            resolver.resolve("s1", "/work/team/project"),
            Some("team-ch".to_string())
        );
        // A mid-session `cd` away from the routed dir must not split the session.
        assert_eq!(
            resolver.resolve("s1", "/elsewhere"),
            Some("team-ch".to_string())
        );

        // And the reverse: a session pinned to default-only stays there.
        assert_eq!(resolver.resolve("s2", "/elsewhere"), None);
        assert_eq!(resolver.resolve("s2", "/work/team/project"), None);
    }

    #[test]
    fn empty_cwd_defers_resolution_instead_of_pinning() {
        let mut resolver = resolver_with_lookup(team_config(), |_| None);

        assert_eq!(resolver.resolve("s1", ""), None);
        assert_eq!(
            resolver.resolve("s1", "/work/team/project"),
            Some("team-ch".to_string()),
            "rows before the session's cwd is known must not pin default-only"
        );
    }

    #[test]
    fn default_backend_name_is_never_a_mirror_target() {
        let mut config = (*team_config()).clone();
        config.routes.insert(
            0,
            moraine_config::RouteConfig {
                dir: "/work/local/**".to_string(),
                backend: DEFAULT_BACKEND_NAME.to_string(),
                mode: moraine_config::ROUTE_MODE_MIRROR.to_string(),
            },
        );
        let mut resolver = resolver_with_lookup(Arc::new(config), |_| None);

        assert_eq!(resolver.resolve("s1", "/work/local/project"), None);
    }

    #[test]
    fn resolution_is_skipped_entirely_without_named_backends() {
        let config = Arc::new(AppConfig::default());
        let mut resolver = resolver_with_lookup(config, |_| {
            panic!("repo lookup must not run when only the default backend exists")
        });

        assert_eq!(resolver.resolve("s1", "/anywhere"), None);
        assert!(resolver.sticky.is_empty());
    }

    fn mixed_session_batch() -> RowBatch {
        let mut batch = RowBatch::default();
        batch.push_raw_row(row("team-session", "/work/team/project"));
        batch.push_raw_row(row("other-session", "/home/other"));
        batch.extend_event_rows(vec![
            row("team-session", "/work/team/project"),
            row("other-session", "/home/other"),
        ]);
        // Link/tool rows carry session_id but no cwd; they must follow the
        // sticky route pinned by the raw rows above.
        batch.extend_link_rows(vec![row("team-session", ""), row("other-session", "")]);
        batch.extend_tool_rows(vec![row("team-session", ""), row("other-session", "")]);
        batch.push_error_row(json!({ "error_kind": "json_parse_error" }));
        batch.checkpoint = Some(Checkpoint {
            source_name: "src".to_string(),
            source_file: "/tmp/a.jsonl".to_string(),
            last_offset: 10,
            status: "active".to_string(),
            ..Default::default()
        });
        batch
    }

    #[test]
    fn filter_batch_keeps_only_the_backends_sessions() {
        let mut resolver = resolver_with_lookup(team_config(), |_| None);
        let batch = mixed_session_batch();

        let filtered = filter_batch_for_backend(&batch, "team-ch", &mut resolver);

        assert_eq!(filtered.raw_rows.len(), 1);
        assert_eq!(filtered.event_rows.len(), 1);
        assert_eq!(filtered.link_rows.len(), 1);
        assert_eq!(filtered.tool_rows.len(), 1);
        for rows in [
            &filtered.raw_rows,
            &filtered.event_rows,
            &filtered.link_rows,
            &filtered.tool_rows,
        ] {
            assert_eq!(
                rows[0].get("session_id").and_then(Value::as_str),
                Some("team-session")
            );
        }
        assert!(
            filtered.error_rows.is_empty(),
            "error rows describe files, not sessions; they are never mirrored"
        );
        assert!(filtered.checkpoint.is_some());
    }

    #[test]
    fn filter_batch_with_no_matches_still_carries_the_checkpoint() {
        let mut resolver = resolver_with_lookup(team_config(), |_| None);
        let mut batch = RowBatch::default();
        batch.push_raw_row(row("other-session", "/home/other"));
        batch.checkpoint = Some(Checkpoint {
            source_name: "src".to_string(),
            source_file: "/tmp/a.jsonl".to_string(),
            last_offset: 10,
            status: "active".to_string(),
            ..Default::default()
        });

        let filtered = filter_batch_for_backend(&batch, "team-ch", &mut resolver);

        assert_eq!(filtered.row_count(), 0);
        assert!(
            filtered.checkpoint.is_some(),
            "checkpoint must advance so replay passes converge on unrouted files"
        );
    }

    #[test]
    fn collect_targets_finds_backends_across_row_kinds() {
        let mut resolver = resolver_with_lookup(team_config(), |_| None);
        let batch = mixed_session_batch();

        let targets = collect_targets(&batch, &mut resolver);
        assert_eq!(
            targets.into_iter().collect::<Vec<_>>(),
            vec!["team-ch".to_string()]
        );
    }

    #[test]
    fn cell_transitions_follow_the_recovery_protocol() {
        let cell = BackendSinkCell::new("team-ch");
        assert_eq!(cell.status(), BackendSinkStatus::Connecting);

        cell.set_status(BackendSinkStatus::Ok);
        assert!(
            cell.mark_overflow(),
            "first overflow transitions to lagging"
        );
        assert!(!cell.mark_overflow(), "subsequent overflows are silent");
        assert_eq!(cell.status(), BackendSinkStatus::Lagging);

        assert!(cell.mark_flush_failure());
        assert!(!cell.mark_flush_failure());
        assert_eq!(cell.status(), BackendSinkStatus::Unreachable);

        assert!(cell.mark_recovered());
        assert!(!cell.mark_recovered(), "recovery from ok is a no-op");
        assert_eq!(cell.status(), BackendSinkStatus::Ok);
    }

    #[test]
    fn disabled_skew_is_terminal() {
        let cell = BackendSinkCell::new("team-ch");
        cell.set_status(BackendSinkStatus::DisabledSkew);

        assert!(!cell.mark_overflow());
        assert!(!cell.mark_flush_failure());
        assert!(!cell.mark_recovered());
        assert_eq!(cell.status(), BackendSinkStatus::DisabledSkew);
    }

    #[test]
    fn disabled_missing_identity_is_terminal_and_named() {
        let cell = BackendSinkCell::new("team-ch");
        cell.set_status(BackendSinkStatus::DisabledMissingIdentity);

        assert!(!cell.mark_overflow());
        assert!(!cell.mark_flush_failure());
        assert!(!cell.mark_recovered());
        assert_eq!(cell.status(), BackendSinkStatus::DisabledMissingIdentity);
        assert_eq!(
            BackendSinkStatus::DisabledMissingIdentity.as_str(),
            "disabled_missing_identity"
        );
    }

    #[test]
    fn backend_sinks_json_is_deterministic_and_absent_when_empty() {
        let registry: StatusRegistry = Arc::new(Mutex::new(BTreeMap::new()));
        assert_eq!(backend_sinks_json(&registry), None);

        let lagging = Arc::new(BackendSinkCell::new("b-lagging"));
        lagging.set_status(BackendSinkStatus::Lagging);
        let ok = Arc::new(BackendSinkCell::new("a-ok"));
        ok.set_status(BackendSinkStatus::Ok);
        {
            let mut cells = registry.lock().expect("registry mutex poisoned");
            cells.insert("b-lagging".to_string(), lagging);
            cells.insert("a-ok".to_string(), ok);
        }

        assert_eq!(
            backend_sinks_json(&registry).as_deref(),
            Some(r#"{"a-ok":"ok","b-lagging":"lagging"}"#)
        );
    }

    #[tokio::test]
    async fn forward_never_blocks_and_marks_overflow_as_lagging() {
        let (tx, mut backend_rx) = mpsc::channel::<SinkMessage>(1);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        cell.set_status(BackendSinkStatus::Ok);
        cell.mark_live();
        let handle = BackendHandle {
            tx,
            cell: cell.clone(),
        };

        let batch = mixed_session_batch();
        forward_to_backend(&handle, &batch); // fills the queue
        forward_to_backend(&handle, &batch); // overflow -> lagging + drop
        forward_to_backend(&handle, &batch); // skipped while lagging

        assert_eq!(cell.status(), BackendSinkStatus::Lagging);
        assert_eq!(cell.dropped_batches(), 2);
        assert!(backend_rx.try_recv().is_ok(), "first batch was queued");
        assert!(
            backend_rx.try_recv().is_err(),
            "overflowed batches are dropped, not queued"
        );
    }

    #[tokio::test]
    async fn forward_skips_backends_that_are_not_live() {
        let (tx, mut backend_rx) = mpsc::channel::<SinkMessage>(4);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let handle = BackendHandle {
            tx,
            cell: cell.clone(),
        };

        forward_to_backend(&handle, &mixed_session_batch());

        assert!(backend_rx.try_recv().is_err());
        assert_eq!(cell.dropped_batches(), 1);
        assert_eq!(
            cell.status(),
            BackendSinkStatus::Connecting,
            "dropping while connecting is not an overflow"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_keeps_default_path_flowing_while_identity_gate_disables_mirror() {
        let mut config = (*team_config()).clone();
        config.identity.author = String::new();
        let config = Arc::new(config);
        let resolver: SharedRouteResolver =
            Arc::new(Mutex::new(RouteResolver::new(config.clone())));
        let registry: StatusRegistry = Arc::new(Mutex::new(BTreeMap::new()));
        let (router_tx, router_rx) = mpsc::channel::<SinkMessage>(4);
        let (default_tx, mut default_rx) = mpsc::channel::<SinkMessage>(4);

        let handle = spawn_tee_router(
            config,
            Vec::new(),
            router_rx,
            default_tx,
            resolver,
            registry.clone(),
            RedactionContext::new(test_redactor(), test_redaction_audit()),
        );

        router_tx
            .send(SinkMessage::Batch(mixed_session_batch()))
            .await
            .expect("send batch to router");

        let SinkMessage::Batch(batch) = timeout(Duration::from_secs(5), default_rx.recv())
            .await
            .expect("default batch within timeout")
            .expect("default batch present");
        assert_eq!(
            batch.raw_rows.len(),
            2,
            "the default sink receives the full batch while the mirror is gated"
        );

        let cell = registry
            .lock()
            .expect("registry mutex poisoned")
            .get("team-ch")
            .cloned()
            .expect("routed backend is registered even when gated");
        assert!(
            wait_for_status(
                &cell,
                BackendSinkStatus::DisabledMissingIdentity,
                Duration::from_secs(5)
            )
            .await,
            "the routed backend must surface disabled_missing_identity"
        );
        assert!(
            cell.dropped_batches() >= 1,
            "the routed copy is dropped, not queued, while disabled"
        );

        handle.abort();
    }

    async fn wait_for_status(
        cell: &Arc<BackendSinkCell>,
        expected: BackendSinkStatus,
        wait: Duration,
    ) -> bool {
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            if cell.status() == expected {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cell.status() == expected
    }

    fn backend_cfg_for(url: String) -> ClickHouseConfig {
        ClickHouseConfig {
            url,
            timeout_seconds: 1.0,
            ..Default::default()
        }
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

    fn test_router_context(
        config: Arc<AppConfig>,
        resolver: SharedRouteResolver,
    ) -> TeeRouterContext {
        TeeRouterContext {
            config,
            sources: Arc::new(Vec::new()),
            resolver,
            registry: Arc::new(Mutex::new(BTreeMap::new())),
            redaction: RedactionContext::new(test_redactor(), test_redaction_audit()),
        }
    }

    fn secret_batch() -> RowBatch {
        let mut batch = RowBatch::default();
        batch.push_raw_row(serde_json::json!({
            "session_id": "session-a",
            "cwd": "/work/team/project",
            "raw_json": "{\"token\":\"ghp_abcdefghijklmnopqrstuvwxyzABCDE12345\"}",
            "raw_json_hash": 1u64,
        }));
        batch
    }

    #[test]
    fn default_gate_redacts_when_enabled() {
        let config = AppConfig::default();
        let redactor = test_redactor();
        let redactions = test_redaction_audit();
        let redaction = RedactionContext::new(redactor, redactions.clone());
        let mut batch = secret_batch();

        redact_default_batch_if_enabled(&config, &redaction, &mut batch);

        assert_eq!(
            batch.raw_rows[0].get("raw_json").and_then(Value::as_str),
            Some("{\"token\":\"[REDACTED:github-token]\"}")
        );
        assert_eq!(redactions.snapshot().get("github-token"), Some(&1));
    }

    #[test]
    fn default_gate_honors_local_skip() {
        let mut config = AppConfig::default();
        config.redaction.dangerously_skip_secret_redaction = true;
        let redactor = test_redactor();
        let redactions = test_redaction_audit();
        let redaction = RedactionContext::new(redactor, redactions.clone());
        let mut batch = secret_batch();

        redact_default_batch_if_enabled(&config, &redaction, &mut batch);

        assert_eq!(
            batch.raw_rows[0].get("raw_json").and_then(Value::as_str),
            Some("{\"token\":\"ghp_abcdefghijklmnopqrstuvwxyzABCDE12345\"}")
        );
        assert!(redactions.snapshot().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supervisor_marks_unreachable_backend_without_going_live() {
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let (tx, rx) = mpsc::channel::<SinkMessage>(1);
        let config = team_config();
        let resolver: SharedRouteResolver =
            Arc::new(Mutex::new(RouteResolver::new(config.clone())));

        let handle = spawn_backend_supervisor(
            backend_cfg_for("http://127.0.0.1:1".to_string()),
            cell.clone(),
            tx,
            rx,
            test_router_context(config, resolver),
        );

        assert!(
            wait_for_status(
                &cell,
                BackendSinkStatus::Unreachable,
                Duration::from_secs(5)
            )
            .await,
            "connection-refused handshake must surface as unreachable"
        );
        assert!(!cell.is_live());
        handle.abort();
    }

    #[derive(Clone, Default)]
    struct SkewMockState {
        write_statements: Arc<Mutex<Vec<String>>>,
        queries: Arc<AtomicUsize>,
    }

    async fn skew_mock_handler(
        State(state): State<SkewMockState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> (StatusCode, String) {
        let query = params.get("query").cloned().unwrap_or_default();
        state.queries.fetch_add(1, Ordering::Relaxed);

        if query.contains("INSERT") || query.contains("CREATE") || query.contains("ALTER") {
            state
                .write_statements
                .lock()
                .expect("mock writes mutex poisoned")
                .push(query);
            return (StatusCode::OK, String::new());
        }

        if query.contains("system.tables") {
            return (StatusCode::OK, json!({"data": [{"exists": 1}]}).to_string());
        }
        if query.contains("schema_migrations") {
            // Server is AHEAD: every bundled version plus an unknown one.
            let mut versions: Vec<Value> = moraine_clickhouse::bundled_migrations()
                .iter()
                .map(|m| json!({"version": m.version}))
                .collect();
            versions.push(json!({"version": "999"}));
            return (StatusCode::OK, json!({ "data": versions }).to_string());
        }
        (StatusCode::OK, json!({"data": []}).to_string())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supervisor_disables_skewed_backend_without_writing_to_it() {
        let state = SkewMockState::default();
        let app = Router::new()
            .route("/", post(skew_mock_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind skew mock listener");
        let addr = listener.local_addr().expect("skew mock addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let (tx, rx) = mpsc::channel::<SinkMessage>(1);
        let config = team_config();
        let resolver: SharedRouteResolver =
            Arc::new(Mutex::new(RouteResolver::new(config.clone())));

        let handle = spawn_backend_supervisor(
            backend_cfg_for(format!("http://{}", addr)), // allow_newer_server = false
            cell.clone(),
            tx,
            rx,
            test_router_context(config, resolver),
        );

        assert!(
            wait_for_status(
                &cell,
                BackendSinkStatus::DisabledSkew,
                Duration::from_secs(5)
            )
            .await,
            "server-ahead skew with allow_newer_server=false must disable the sink"
        );
        assert!(!cell.is_live());
        assert!(
            state
                .write_statements
                .lock()
                .expect("mock writes mutex poisoned")
                .is_empty(),
            "a disabled backend must never receive writes"
        );
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supervisor_disables_backend_without_identity_before_any_contact() {
        let state = SkewMockState::default();
        let app = Router::new()
            .route("/", post(skew_mock_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind identity mock listener");
        let addr = listener.local_addr().expect("identity mock addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = (*team_config()).clone();
        config.identity.author = String::new();
        let config = Arc::new(config);
        let cell = Arc::new(BackendSinkCell::new("team-ch"));
        let (tx, rx) = mpsc::channel::<SinkMessage>(1);
        let resolver: SharedRouteResolver =
            Arc::new(Mutex::new(RouteResolver::new(config.clone())));

        let handle = spawn_backend_supervisor(
            backend_cfg_for(format!("http://{}", addr)),
            cell.clone(),
            tx,
            rx,
            test_router_context(config, resolver),
        );

        assert!(
            wait_for_status(
                &cell,
                BackendSinkStatus::DisabledMissingIdentity,
                Duration::from_secs(5)
            )
            .await,
            "an empty [identity].author with a mirror target must disable the sink"
        );
        assert!(!cell.is_live());
        assert_eq!(
            state.queries.load(Ordering::Relaxed),
            0,
            "a sink disabled for missing identity must never contact the backend \
             (a checkpoint written while disabled would make later catch-up skip data)"
        );
        assert!(state
            .write_statements
            .lock()
            .expect("mock writes mutex poisoned")
            .is_empty());
        handle.abort();
    }

    fn unique_replay_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("moraine-tee-{name}-{suffix}.jsonl"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_pass_reads_from_the_backends_own_checkpoints() {
        let path = unique_replay_file("replay");
        fs::write(
            &path,
            [
                json!({
                    "type": "user",
                    "timestamp": "2026-04-18T20:43:51.069Z",
                    "uuid": "u1",
                    "sessionId": "team-session",
                    "cwd": "/work/team/project",
                    "message": {"role": "user", "content": "hello"}
                })
                .to_string(),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-18T20:43:52.069Z",
                    "uuid": "u2",
                    "sessionId": "team-session",
                    "cwd": "/work/team/project",
                    "message": {"role": "user", "content": "again"}
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write replay fixture");

        let config = AppConfig::default();
        let sources = vec![IngestSource {
            name: "claude".to_string(),
            harness: "claude-code".to_string(),
            enabled: true,
            glob: path.to_string_lossy().to_string(),
            watch_root: std::env::temp_dir().to_string_lossy().to_string(),
            format: "jsonl".to_string(),
        }];
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (tx, mut rx) = mpsc::channel::<SinkMessage>(16);
        let mut reported_lost = HashSet::new();

        run_replay_pass(
            &config,
            &sources,
            &checkpoints,
            &tx,
            &metrics,
            "team-ch",
            &mut reported_lost,
        )
        .await;

        let SinkMessage::Batch(batch) = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("replay batch within timeout")
            .expect("replay batch present");
        assert_eq!(batch.raw_rows.len(), 2);
        let checkpoint = batch.checkpoint.clone().expect("replay checkpoint");

        // Apply the checkpoint like the backend's sink would; a second pass
        // must early-exit without producing batches.
        {
            let mut map = checkpoints.write().await;
            map.insert(
                checkpoint_key(&checkpoint.source_name, &checkpoint.source_file),
                checkpoint,
            );
        }
        run_replay_pass(
            &config,
            &sources,
            &checkpoints,
            &tx,
            &metrics,
            "team-ch",
            &mut reported_lost,
        )
        .await;
        assert!(
            timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err(),
            "caught-up files must not re-emit batches"
        );

        let _ = fs::remove_file(&path);
    }
}
