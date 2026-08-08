use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, Weak,
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const QUERY_CLEANUP_GRACE: Duration = Duration::from_secs(5);
const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const CLEANUP_RETRY: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryWorkload {
    Mcp,
    Monitor,
    Internal,
    Export,
    Migration,
    Background,
    Administrative,
}

impl QueryWorkload {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mcp => "mcp",
            Self::Monitor => "monitor",
            Self::Internal => "internal",
            Self::Export => "export",
            Self::Migration => "migration",
            Self::Background => "background",
            Self::Administrative => "administrative",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QueryResourceProfile {
    Interactive,
    Background,
    Migration,
    Administrative,
}

impl QueryResourceProfile {
    const ALL: [Self; 4] = [
        Self::Interactive,
        Self::Background,
        Self::Migration,
        Self::Administrative,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Interactive => 0,
            Self::Background => 1,
            Self::Migration => 2,
            Self::Administrative => 3,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Background => "background",
            Self::Migration => "migration",
            Self::Administrative => "administrative",
        }
    }

    const fn max_running(self) -> usize {
        match self {
            Self::Interactive => 4,
            Self::Background => 2,
            Self::Migration => 1,
            Self::Administrative => 2,
        }
    }

    const fn max_queued(self) -> usize {
        match self {
            Self::Interactive => 16,
            Self::Background => 8,
            Self::Migration => 1,
            Self::Administrative => 8,
        }
    }

    pub(crate) const fn workload_name(self) -> &'static str {
        match self {
            Self::Interactive => "moraine_interactive",
            Self::Background => "moraine_background",
            Self::Migration => "moraine_migration",
            Self::Administrative => "moraine_administrative",
        }
    }

    pub(crate) const fn memory_bytes(self) -> u64 {
        match self {
            Self::Interactive | Self::Background => 268_435_456,
            Self::Migration => 1_073_741_824,
            Self::Administrative => 134_217_728,
        }
    }

    pub(crate) const fn spill_bytes(self) -> u64 {
        self.memory_bytes() / 4
    }

    pub(crate) const fn temporary_disk_bytes(self) -> u64 {
        match self {
            Self::Interactive | Self::Background => 536_870_912,
            Self::Migration => 2_147_483_648,
            Self::Administrative => 134_217_728,
        }
    }
}

impl From<QueryWorkload> for QueryResourceProfile {
    fn from(workload: QueryWorkload) -> Self {
        match workload {
            QueryWorkload::Mcp | QueryWorkload::Monitor | QueryWorkload::Export => {
                Self::Interactive
            }
            QueryWorkload::Background => Self::Background,
            QueryWorkload::Migration => Self::Migration,
            QueryWorkload::Internal | QueryWorkload::Administrative => Self::Administrative,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryProfilePressure {
    pub running: u64,
    pub queued: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPressureSnapshot {
    pub scope: String,
    pub interactive: QueryProfilePressure,
    pub background: QueryProfilePressure,
    pub migration: QueryProfilePressure,
    pub administrative: QueryProfilePressure,
    pub resource_limit_events: u64,
}

impl fmt::Display for QueryWorkload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The first reason an operation became terminal. The first recorded cause wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCause {
    Explicit,
    Disconnect,
    Shutdown,
    Abandoned,
    Deadline,
    Busy,
    Backend,
    ResourceExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerLifecycle {
    Active,
    CompletionPending,
    Completed,
    Cleaning,
    Terminal,
}

#[derive(Clone)]
pub struct QueryRuntime {
    inner: Arc<QueryRuntimeInner>,
}

struct QueryRuntimeInner {
    state: Mutex<QueryRuntimeState>,
    tx: mpsc::UnboundedSender<SupervisorMessage>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<SupervisorMessage>>>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    changed: watch::Sender<u64>,
    close_started: Mutex<Option<Instant>>,
    close_lock: tokio::sync::Mutex<()>,
    close: CancellationToken,
    admission: [Arc<ProfileAdmission>; 4],
    resource_limit_events: AtomicU64,
}

struct QueryRuntimeState {
    active: HashMap<Uuid, Arc<OwnerState>>,
    closing: bool,
}

struct ProfileAdmission {
    running_slots: Arc<Semaphore>,
    total_slots: Arc<Semaphore>,
    running: Arc<AtomicU64>,
    queued: Arc<AtomicU64>,
    rejected: AtomicU64,
}

impl ProfileAdmission {
    fn new(profile: QueryResourceProfile) -> Arc<Self> {
        Arc::new(Self {
            running_slots: Arc::new(Semaphore::new(profile.max_running())),
            total_slots: Arc::new(Semaphore::new(profile.max_running() + profile.max_queued())),
            running: Arc::new(AtomicU64::new(0)),
            queued: Arc::new(AtomicU64::new(0)),
            rejected: AtomicU64::new(0),
        })
    }

    fn pressure(&self) -> QueryProfilePressure {
        QueryProfilePressure {
            running: self.running.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
        }
    }

    fn close(&self) {
        self.running_slots.close();
        self.total_slots.close();
    }
}

struct AdmissionGauge {
    value: Arc<AtomicU64>,
}

impl AdmissionGauge {
    fn new(value: Arc<AtomicU64>) -> Self {
        value.fetch_add(1, Ordering::Relaxed);
        Self { value }
    }
}

impl Drop for AdmissionGauge {
    fn drop(&mut self) {
        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            });
    }
}

pub(crate) struct StatementAdmission {
    _total_slot: OwnedSemaphorePermit,
    _running_slot: OwnedSemaphorePermit,
    _running_gauge: AdmissionGauge,
}

enum SupervisorMessage {
    Cleanup(Arc<OwnerState>),
    Deadline(Arc<OwnerState>),
    Stop,
}

impl Default for QueryRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryRuntime {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel();
        let (changed, _) = watch::channel(0);
        Self {
            inner: Arc::new(QueryRuntimeInner {
                state: Mutex::new(QueryRuntimeState {
                    active: HashMap::new(),
                    closing: false,
                }),
                tx,
                receiver: Mutex::new(Some(receiver)),
                supervisor: Mutex::new(None),
                changed,
                close_started: Mutex::new(None),
                close_lock: tokio::sync::Mutex::new(()),
                close: CancellationToken::new(),
                admission: QueryResourceProfile::ALL.map(ProfileAdmission::new),
                resource_limit_events: AtomicU64::new(0),
            }),
        }
    }

    fn ensure_supervisor(&self) -> Result<(), ClickHouseError> {
        let mut slot = self
            .inner
            .supervisor
            .lock()
            .expect("supervisor mutex poisoned");
        if slot.is_some() {
            return Ok(());
        }
        let receiver = self
            .inner
            .receiver
            .lock()
            .expect("supervisor receiver mutex poisoned")
            .take()
            .ok_or_else(|| ClickHouseError::ownership("query supervisor is not available"))?;
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ClickHouseError::ownership("a Tokio runtime is required to create a query owner")
        })?;
        let weak = Arc::downgrade(&self.inner);
        *slot = Some(handle.spawn(supervisor_loop(weak, receiver)));
        Ok(())
    }

    pub fn is_closing(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("query runtime state poisoned")
            .closing
    }

    pub fn active_owner_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("query runtime state poisoned")
            .active
            .len()
    }

    pub fn pressure_snapshot(&self) -> QueryPressureSnapshot {
        let pressure =
            |profile: QueryResourceProfile| self.inner.admission[profile.index()].pressure();
        QueryPressureSnapshot {
            scope: "process".to_string(),
            interactive: pressure(QueryResourceProfile::Interactive),
            background: pressure(QueryResourceProfile::Background),
            migration: pressure(QueryResourceProfile::Migration),
            administrative: pressure(QueryResourceProfile::Administrative),
            resource_limit_events: self.inner.resource_limit_events.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_resource_limit_event(&self) {
        self.inner
            .resource_limit_events
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_rejection(&self, profile: QueryResourceProfile) {
        self.inner.admission[profile.index()]
            .rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn admit_statement(
        &self,
        owner: &QueryOwner,
    ) -> Result<StatementAdmission, ClickHouseError> {
        owner.ensure_runtime(self)?;
        let profile = QueryResourceProfile::from(owner.workload());
        let admission = &self.inner.admission[profile.index()];
        let total_slot = match admission.total_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) if self.is_closing() => {
                return Err(ClickHouseError::cancelled("query runtime is closing"));
            }
            Err(_) => {
                admission.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(ClickHouseError::busy(format!(
                    "{} query admission queue is full",
                    profile.as_str()
                )));
            }
        };

        let running_slot = match admission.running_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let queued = AdmissionGauge::new(admission.queued.clone());
                let acquire = admission.running_slots.clone().acquire_owned();
                tokio::pin!(acquire);
                let result = tokio::select! {
                    permit = &mut acquire => permit.map_err(|_| {
                        ClickHouseError::cancelled("query runtime admission is closed")
                    }),
                    _ = owner.state.token.cancelled() => Err(error_for_cause(
                        owner.cause().unwrap_or(QueryCause::Abandoned),
                        "query admission was cancelled",
                    )),
                    _ = self.inner.close.cancelled() => Err(ClickHouseError::cancelled(
                        "query runtime closed during admission",
                    )),
                };
                drop(queued);
                result?
            }
        };
        let running_gauge = AdmissionGauge::new(admission.running.clone());
        Ok(StatementAdmission {
            _total_slot: total_slot,
            _running_slot: running_slot,
            _running_gauge: running_gauge,
        })
    }

    fn same_domain(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Reject new owners, cancel every active owner, and bound all query cleanup
    /// and deadline tasks by the runtime's single five-second shutdown wall.
    pub async fn close_and_drain(&self) {
        let _close_guard = self.inner.close_lock.lock().await;
        let started = {
            let mut value = self
                .inner
                .close_started
                .lock()
                .expect("close state mutex poisoned");
            *value.get_or_insert_with(Instant::now)
        };
        let deadline = started + QUERY_CLEANUP_GRACE;
        let _ = self.ensure_supervisor();

        // Closing admission and the active-owner snapshot share one lock. An
        // owner is therefore either present in this snapshot or observes the
        // closed state and is rejected.
        let owners: Vec<_> = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("query runtime state poisoned");
            state.closing = true;
            self.inner.close.cancel();
            for (index, admission) in self.inner.admission.iter().enumerate() {
                if index != QueryResourceProfile::Administrative.index() {
                    admission.close();
                }
            }
            state.active.values().cloned().collect()
        };
        for owner in owners {
            owner.cancel(QueryCause::Shutdown);
        }

        // Subscribe before checking the durable active map. A removal between
        // the check and changed().await advances the watch version and cannot
        // be missed.
        let mut changed = self.inner.changed.subscribe();
        loop {
            if self.active_owner_count() == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, changed.changed())
                .await
                .is_err()
            {
                break;
            }
        }

        let _ = self.inner.tx.send(SupervisorMessage::Stop);
        let handle = self
            .inner
            .supervisor
            .lock()
            .expect("supervisor mutex poisoned")
            .take();
        if let Some(mut handle) = handle {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }

        // The shutdown wall is terminal even when the administrative backend
        // was unavailable. Do not retain operations past the composition root.
        let remaining: Vec<_> = self
            .inner
            .state
            .lock()
            .expect("query runtime state poisoned")
            .active
            .values()
            .cloned()
            .collect();
        for owner in remaining {
            owner.finish_terminal();
        }
    }
}

impl QueryRuntimeInner {
    fn remove(&self, id: Uuid) {
        let removed = self
            .state
            .lock()
            .expect("query runtime state poisoned")
            .active
            .remove(&id)
            .is_some();
        if removed {
            self.changed.send_modify(|version| {
                *version = version.wrapping_add(1);
            });
        }
    }

    async fn admit_administrative(
        &self,
        wall: Instant,
    ) -> Result<StatementAdmission, ClickHouseError> {
        let profile = QueryResourceProfile::Administrative;
        let admission = &self.admission[profile.index()];
        let total_slot = match admission.total_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                admission.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(ClickHouseError::busy(
                    "administrative query admission queue is full",
                ));
            }
        };
        let running_slot = match admission.running_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let queued = AdmissionGauge::new(admission.queued.clone());
                let permit =
                    tokio::time::timeout_at(wall, admission.running_slots.clone().acquire_owned())
                        .await
                        .map_err(|_| {
                            ClickHouseError::cancelled("administrative query admission timed out")
                        })?
                        .map_err(|_| {
                            ClickHouseError::cancelled("administrative query admission is closed")
                        })?;
                drop(queued);
                permit
            }
        };
        let running_gauge = AdmissionGauge::new(admission.running.clone());
        Ok(StatementAdmission {
            _total_slot: total_slot,
            _running_slot: running_slot,
            _running_gauge: running_gauge,
        })
    }

    fn classify_administrative_code(&self, code: Option<u32>) -> ClickHouseErrorCategory {
        match code {
            Some(202 | 745) => {
                self.admission[QueryResourceProfile::Administrative.index()]
                    .rejected
                    .fetch_add(1, Ordering::Relaxed);
                ClickHouseErrorCategory::Busy
            }
            Some(158 | 241 | 243 | 307 | 396) => {
                self.resource_limit_events.fetch_add(1, Ordering::Relaxed);
                ClickHouseErrorCategory::ResourceExhausted
            }
            Some(159 | 160 | 209) => ClickHouseErrorCategory::DeadlineExceeded,
            _ => ClickHouseErrorCategory::Backend,
        }
    }
}

pub struct QueryOwner {
    state: Arc<OwnerState>,
    // Guards/streams keep the runtime domain alive without creating a cycle:
    // the runtime's active map stores OwnerState, not QueryOwner.
    runtime: QueryRuntime,
}

impl fmt::Debug for QueryOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryOwner")
            .field("logical_id", &self.state.logical_id)
            .field("workload", &self.state.workload)
            .field("deadline", &self.state.deadline)
            .field("cause", &self.cause())
            .finish()
    }
}

struct OwnerState {
    id: Uuid,
    logical_id: String,
    workload: QueryWorkload,
    deadline: Option<Instant>,
    runtime: Weak<QueryRuntimeInner>,
    tx: mpsc::UnboundedSender<SupervisorMessage>,
    token: CancellationToken,
    next_child: AtomicU64,
    data: Mutex<OwnerData>,
    cleanup_queued: AtomicBool,
    terminal: CancellationToken,
}

struct OwnerData {
    lifecycle: OwnerLifecycle,
    cause: Option<QueryCause>,
    children: HashMap<String, ChildRecord>,
}

#[derive(Clone)]
struct ChildRecord {
    backend: Arc<AdminBackend>,
    attempted: bool,
}

type CleanupBackendBatch = (Arc<AdminBackend>, Vec<(String, bool)>);

impl QueryOwner {
    pub fn new(
        runtime: &QueryRuntime,
        workload: QueryWorkload,
    ) -> Result<Arc<Self>, ClickHouseError> {
        Self::new_inner(runtime, workload, None)
    }

    pub fn with_deadline(
        runtime: &QueryRuntime,
        workload: QueryWorkload,
        absolute_deadline: Instant,
    ) -> Result<Arc<Self>, ClickHouseError> {
        Self::new_inner(runtime, workload, Some(absolute_deadline))
    }

    fn new_inner(
        runtime: &QueryRuntime,
        workload: QueryWorkload,
        deadline: Option<Instant>,
    ) -> Result<Arc<Self>, ClickHouseError> {
        runtime.ensure_supervisor()?;
        let id = Uuid::new_v4();
        let state = Arc::new(OwnerState {
            id,
            logical_id: format!("moraine-{}-{}", workload.as_str(), id.simple()),
            workload,
            deadline,
            runtime: Arc::downgrade(&runtime.inner),
            tx: runtime.inner.tx.clone(),
            token: CancellationToken::new(),
            next_child: AtomicU64::new(0),
            data: Mutex::new(OwnerData {
                lifecycle: OwnerLifecycle::Active,
                cause: None,
                children: HashMap::new(),
            }),
            cleanup_queued: AtomicBool::new(false),
            terminal: CancellationToken::new(),
        });
        {
            let mut runtime_state = runtime
                .inner
                .state
                .lock()
                .expect("query runtime state poisoned");
            if runtime_state.closing {
                return Err(ClickHouseError::ownership("query runtime is closing"));
            }
            runtime_state.active.insert(id, state.clone());
        }
        runtime.inner.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
        if deadline.is_some() {
            let _ = runtime
                .inner
                .tx
                .send(SupervisorMessage::Deadline(state.clone()));
        }
        Ok(Arc::new(Self {
            state,
            runtime: runtime.clone(),
        }))
    }

    pub fn logical_id(&self) -> &str {
        &self.state.logical_id
    }

    pub fn workload(&self) -> QueryWorkload {
        self.state.workload
    }

    pub fn runtime(&self) -> QueryRuntime {
        self.runtime.clone()
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.state.deadline
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.state.token.clone()
    }

    pub fn cause(&self) -> Option<QueryCause> {
        self.state.data.lock().expect("owner state poisoned").cause
    }

    pub fn cancel(&self, cause: QueryCause) {
        self.state.cancel(cause);
    }

    pub fn scope<F>(self: &Arc<Self>, future: F) -> OwnerGuard<F>
    where
        F: Future,
    {
        OwnerGuard {
            inner: Box::pin(CURRENT_OWNER.scope(self.clone(), future)),
            owner: self.clone(),
            ready: false,
        }
    }

    pub fn current() -> Option<Arc<Self>> {
        CURRENT_OWNER.try_with(Clone::clone).ok()
    }

    pub(crate) fn remaining(&self) -> Result<Option<Duration>, ClickHouseError> {
        let Some(deadline) = self.state.deadline else {
            return Ok(None);
        };
        let now = Instant::now();
        if now >= deadline {
            self.state.cancel(QueryCause::Deadline);
            return Err(ClickHouseError::deadline(
                "ClickHouse query deadline exceeded",
            ));
        }
        Ok(Some(deadline - now))
    }

    pub(crate) fn ensure_runtime(&self, runtime: &QueryRuntime) -> Result<(), ClickHouseError> {
        if self.runtime.same_domain(runtime) {
            Ok(())
        } else {
            Err(ClickHouseError::ownership(
                "query owner belongs to a different query runtime",
            ))
        }
    }

    pub(crate) fn register_statement(
        &self,
        runtime: &QueryRuntime,
        backend: Arc<AdminBackend>,
        admission: StatementAdmission,
    ) -> Result<StatementTicket, ClickHouseError> {
        self.ensure_runtime(runtime)?;
        let sequence = self
            .state
            .next_child
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ClickHouseError::ownership("query owner child sequence exhausted"))?;
        let child_id = format!("{}-{}", self.logical_id(), sequence);
        let mut data = self.state.data.lock().expect("owner state poisoned");
        if data.cause.is_some()
            || matches!(
                data.lifecycle,
                OwnerLifecycle::Completed | OwnerLifecycle::Cleaning | OwnerLifecycle::Terminal
            )
        {
            return Err(error_for_cause(
                data.cause.unwrap_or(QueryCause::Abandoned),
                "query owner is no longer active",
            ));
        }
        data.children.insert(
            child_id.clone(),
            ChildRecord {
                backend,
                attempted: false,
            },
        );
        Ok(StatementTicket {
            owner: self.state.clone(),
            child_id,
            armed: true,
            _admission: Some(admission),
        })
    }
}

tokio::task_local! {
    static CURRENT_OWNER: Arc<QueryOwner>;
}

pub struct OwnerGuard<F: Future> {
    inner: Pin<Box<tokio::task::futures::TaskLocalFuture<Arc<QueryOwner>, F>>>,
    owner: Arc<QueryOwner>,
    ready: bool,
}

impl<F: Future> Future for OwnerGuard<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                self.ready = true;
                self.owner.state.complete_scope();
                Poll::Ready(output)
            }
        }
    }
}

impl<F: Future> Drop for OwnerGuard<F> {
    fn drop(&mut self) {
        if !self.ready {
            self.owner.state.cancel(QueryCause::Abandoned);
        }
    }
}

pub(crate) struct StatementTicket {
    owner: Arc<OwnerState>,
    child_id: String,
    armed: bool,
    _admission: Option<StatementAdmission>,
}

impl StatementTicket {
    pub(crate) fn query_id(&self) -> &str {
        &self.child_id
    }

    pub(crate) fn mark_attempted(&self) {
        if let Some(child) = self
            .owner
            .data
            .lock()
            .expect("owner state poisoned")
            .children
            .get_mut(&self.child_id)
        {
            child.attempted = true;
        }
    }

    pub(crate) fn succeed(&mut self) {
        if self.armed {
            self.armed = false;
            self.owner.child_succeeded(&self.child_id);
        }
    }

    pub(crate) fn succeed_reusing_admission(&mut self) -> StatementAdmission {
        self.succeed();
        self._admission
            .take()
            .expect("statement admission can only be reused once")
    }

    pub(crate) fn fail(&mut self, cause: QueryCause) {
        if self.armed {
            self.armed = false;
            self.owner.cancel(cause);
        }
    }
}

impl Drop for StatementTicket {
    fn drop(&mut self) {
        if self.armed {
            self.owner.cancel(QueryCause::Abandoned);
        }
    }
}

impl OwnerState {
    fn complete_scope(&self) {
        let remove = {
            let mut data = self.data.lock().expect("owner state poisoned");
            if data.cause.is_some() {
                false
            } else if data.children.is_empty() {
                data.lifecycle = OwnerLifecycle::Completed;
                true
            } else {
                data.lifecycle = OwnerLifecycle::CompletionPending;
                false
            }
        };
        if remove {
            self.remove_from_runtime();
            self.terminal.cancel();
        }
    }

    fn child_succeeded(&self, child_id: &str) {
        let remove = {
            let mut data = self.data.lock().expect("owner state poisoned");
            data.children.remove(child_id);
            if data.children.is_empty()
                && data.cause.is_none()
                && data.lifecycle == OwnerLifecycle::CompletionPending
            {
                data.lifecycle = OwnerLifecycle::Completed;
                true
            } else {
                false
            }
        };
        if remove {
            self.remove_from_runtime();
            self.terminal.cancel();
        }
    }

    fn cancel(self: &Arc<Self>, cause: QueryCause) {
        {
            let mut data = self.data.lock().expect("owner state poisoned");
            if matches!(
                data.lifecycle,
                OwnerLifecycle::Completed | OwnerLifecycle::Terminal
            ) {
                return;
            }
            if data.cause.is_none() {
                data.cause = Some(cause);
            }
            data.lifecycle = OwnerLifecycle::Cleaning;
        }
        self.token.cancel();
        if !self.cleanup_queued.swap(true, Ordering::AcqRel) {
            // A failed send cannot lose the job: the runtime's strong active map
            // remains the shutdown snapshot backstop.
            let _ = self.tx.send(SupervisorMessage::Cleanup(self.clone()));
        }
    }

    fn finish_terminal(&self) {
        {
            let mut data = self.data.lock().expect("owner state poisoned");
            data.children.clear();
            data.lifecycle = OwnerLifecycle::Terminal;
        }
        self.remove_from_runtime();
        self.terminal.cancel();
    }

    fn remove_from_runtime(&self) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.remove(self.id);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseErrorCategory {
    Cancelled,
    DeadlineExceeded,
    Busy,
    ResourceExhausted,
    Backend,
    OwnershipViolation,
}

#[derive(Debug)]
pub struct ClickHouseError {
    category: ClickHouseErrorCategory,
    context: String,
    status: Option<StatusCode>,
    exception_code: Option<u32>,
    exception_detail: Option<String>,
    source: Option<reqwest::Error>,
}

impl ClickHouseError {
    pub fn category(&self) -> ClickHouseErrorCategory {
        self.category
    }
    pub fn status(&self) -> Option<StatusCode> {
        self.status
    }
    pub fn exception_code(&self) -> Option<u32> {
        self.exception_code
    }
    /// A bounded, single-line ClickHouse exception detail with stack traces
    /// and control characters removed. It never contains the request URL.
    pub fn exception_detail(&self) -> Option<&str> {
        self.exception_detail.as_deref()
    }
    pub(crate) fn ownership(context: impl Into<String>) -> Self {
        Self::new(ClickHouseErrorCategory::OwnershipViolation, context)
    }
    pub(crate) fn cancelled(context: impl Into<String>) -> Self {
        Self::new(ClickHouseErrorCategory::Cancelled, context)
    }
    pub(crate) fn deadline(context: impl Into<String>) -> Self {
        Self::new(ClickHouseErrorCategory::DeadlineExceeded, context)
    }
    pub(crate) fn busy(context: impl Into<String>) -> Self {
        Self::new(ClickHouseErrorCategory::Busy, context)
    }
    pub(crate) fn resource(context: impl Into<String>) -> Self {
        Self::new(ClickHouseErrorCategory::ResourceExhausted, context)
    }
    pub(crate) fn backend(context: impl Into<String>) -> Self {
        Self::new(ClickHouseErrorCategory::Backend, context)
    }
    pub(crate) fn transport(context: impl Into<String>, source: reqwest::Error) -> Self {
        let mut value = Self::backend(context);
        value.source = Some(source.without_url());
        value
    }
    pub(crate) fn response(
        category: ClickHouseErrorCategory,
        context: impl Into<String>,
        status: StatusCode,
        exception_code: Option<u32>,
        exception_detail: Option<&str>,
    ) -> Self {
        let mut value = Self::new(category, context);
        value.status = Some(status);
        value.exception_code = exception_code;
        value.exception_detail = exception_detail.and_then(sanitize_exception_detail);
        value
    }
    pub(crate) fn exception(
        category: ClickHouseErrorCategory,
        context: impl Into<String>,
        exception_code: u32,
        exception_detail: Option<&str>,
    ) -> Self {
        let mut value = Self::new(category, context);
        value.exception_code = Some(exception_code);
        value.exception_detail = exception_detail.and_then(sanitize_exception_detail);
        value
    }
    fn new(category: ClickHouseErrorCategory, context: impl Into<String>) -> Self {
        Self {
            category,
            context: context.into(),
            status: None,
            exception_code: None,
            exception_detail: None,
            source: None,
        }
    }
}

impl fmt::Display for ClickHouseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.context)?;
        if let Some(status) = self.status {
            write!(f, " (HTTP {status})")?;
        }
        if let Some(code) = self.exception_code {
            write!(f, " (ClickHouse code {code})")?;
        }
        if let Some(detail) = &self.exception_detail {
            write!(f, ": {detail}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClickHouseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

fn sanitize_exception_detail(detail: &str) -> Option<String> {
    const MAX_CHARS: usize = 512;
    let first_line = detail.lines().next()?.trim();
    if first_line.is_empty() {
        return None;
    }
    let mut sanitized = first_line
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>();
    if sanitized.chars().count() > MAX_CHARS {
        sanitized = sanitized.chars().take(MAX_CHARS - 3).collect();
        sanitized.push_str("...");
    }
    Some(sanitized)
}

pub(crate) fn error_for_cause(cause: QueryCause, context: impl Into<String>) -> ClickHouseError {
    match cause {
        QueryCause::Deadline => ClickHouseError::deadline(context),
        QueryCause::Busy => ClickHouseError::busy(context),
        QueryCause::ResourceExhausted => ClickHouseError::resource(context),
        QueryCause::Backend => ClickHouseError::backend(context),
        QueryCause::Explicit
        | QueryCause::Disconnect
        | QueryCause::Shutdown
        | QueryCause::Abandoned => ClickHouseError::cancelled(context),
    }
}

pub(crate) struct AdminBackend {
    key: Uuid,
    url: Url,
    username: String,
    password: String,
    client: Client,
    sequence: AtomicU64,
    owner_id: Uuid,
    managed_workloads_available: AtomicBool,
    runtime: Weak<QueryRuntimeInner>,
}

impl AdminBackend {
    pub(crate) fn new(
        url: Url,
        username: String,
        password: String,
        default_headers: reqwest::header::HeaderMap,
        runtime: &QueryRuntime,
    ) -> Result<Arc<Self>, ClickHouseError> {
        let client = Client::builder()
            .connect_timeout(ADMIN_CONNECT_TIMEOUT)
            .default_headers(default_headers)
            .build()
            .map_err(|error| {
                ClickHouseError::transport(
                    "failed to construct ClickHouse administrative client",
                    error,
                )
            })?;
        Ok(Arc::new(Self {
            key: Uuid::new_v4(),
            url,
            username,
            password,
            client,
            sequence: AtomicU64::new(0),
            owner_id: Uuid::new_v4(),
            managed_workloads_available: AtomicBool::new(false),
            runtime: Arc::downgrade(&runtime.inner),
        }))
    }

    pub(crate) fn set_managed_workloads_available(&self, available: bool) {
        self.managed_workloads_available
            .store(available, Ordering::Relaxed);
    }

    pub(crate) async fn execute(
        &self,
        query: &str,
        wall: Instant,
    ) -> Result<String, ClickHouseError> {
        self.execute_inner(query, wall, true, true, None).await
    }

    pub(crate) async fn execute_unprofiled(
        &self,
        query: &str,
        timeout: Duration,
    ) -> Result<String, ClickHouseError> {
        self.execute_inner(query, Instant::now() + timeout, false, true, None)
            .await
    }

    pub(crate) async fn probe_unprofiled(
        &self,
        query: &str,
        timeout: Duration,
        query_id: &str,
    ) -> Result<String, ClickHouseError> {
        self.execute_inner(
            query,
            Instant::now() + timeout,
            false,
            false,
            Some(query_id),
        )
        .await
    }

    async fn execute_inner(
        &self,
        query: &str,
        wall: Instant,
        cleanup: bool,
        accounted: bool,
        query_id: Option<&str>,
    ) -> Result<String, ClickHouseError> {
        let operation = if cleanup {
            "ClickHouse cleanup"
        } else {
            "ClickHouse administrative request"
        };
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| ClickHouseError::cancelled("query runtime is unavailable"))?;
        let admission = if accounted {
            Some(runtime.admit_administrative(wall).await?)
        } else {
            None
        };
        let now = Instant::now();
        if now >= wall {
            return Err(ClickHouseError::cancelled(format!(
                "{operation} wall expired"
            )));
        }
        let remaining = wall - now;
        let request_timeout = if cleanup {
            remaining.min(ADMIN_REQUEST_TIMEOUT)
        } else {
            remaining
        };
        let generated_query_id;
        let query_id = if let Some(query_id) = query_id {
            query_id
        } else {
            let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
            generated_query_id = format!(
                "moraine-administrative-{}-{sequence}",
                self.owner_id.simple()
            );
            &generated_query_id
        };
        let mut url = self.url.clone();
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("query", query);
            pairs.append_pair("query_id", query_id);
            pairs.append_pair("replace_running_query", "0");
            if cleanup && self.managed_workloads_available.load(Ordering::Relaxed) {
                let profile = QueryResourceProfile::Administrative;
                pairs.append_pair("workload", profile.workload_name());
                pairs.append_pair("max_memory_usage", &profile.memory_bytes().to_string());
                pairs.append_pair(
                    "max_bytes_before_external_group_by",
                    &profile.spill_bytes().to_string(),
                );
                pairs.append_pair(
                    "max_bytes_before_external_sort",
                    &profile.spill_bytes().to_string(),
                );
                pairs.append_pair("max_bytes_ratio_before_external_group_by", "0");
                pairs.append_pair("max_bytes_ratio_before_external_sort", "0");
                pairs.append_pair(
                    "max_temporary_data_on_disk_size_for_query",
                    &profile.temporary_disk_bytes().to_string(),
                );
            }
        }
        let mut request = self
            .client
            .post(url)
            .timeout(request_timeout)
            .header(reqwest::header::CONTENT_LENGTH, 0)
            .body(Vec::new());
        if !self.username.is_empty() {
            request = request.basic_auth(&self.username, Some(&self.password));
        }
        let response = request
            .send()
            .await
            .map_err(|error| ClickHouseError::transport(format!("{operation} failed"), error))?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            ClickHouseError::transport(format!("failed to read {operation} response"), error)
        })?;
        drop(admission);
        if !status.is_success() {
            let code = extract_exception_code(&body);
            return Err(ClickHouseError::response(
                runtime.classify_administrative_code(code),
                format!("{operation} was rejected"),
                status,
                code,
                Some(&body),
            ));
        }
        Ok(body)
    }
}

async fn supervisor_loop(
    runtime: Weak<QueryRuntimeInner>,
    mut receiver: mpsc::UnboundedReceiver<SupervisorMessage>,
) {
    let mut jobs = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                if let Err(error) = result {
                    tracing::warn!(error = %error, "ClickHouse query supervisor job failed");
                }
            }
            message = receiver.recv() => match message {
                Some(SupervisorMessage::Cleanup(owner)) => {
                    jobs.spawn(cleanup_owner(owner));
                }
                Some(SupervisorMessage::Deadline(owner)) => {
                    jobs.spawn(async move {
                        let Some(deadline) = owner.deadline else { return; };
                        tokio::select! {
                            _ = tokio::time::sleep_until(deadline) => owner.cancel(QueryCause::Deadline),
                            _ = owner.terminal.cancelled() => {}
                        }
                    });
                }
                Some(SupervisorMessage::Stop) | None => break,
            }
        }
    }
    let wall = Instant::now() + QUERY_CLEANUP_GRACE;
    while !jobs.is_empty() {
        if tokio::time::timeout_at(wall, jobs.join_next())
            .await
            .is_err()
        {
            jobs.abort_all();
            break;
        }
    }
    while jobs.join_next().await.is_some() {}
    if let Some(runtime) = runtime.upgrade() {
        runtime
            .changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

async fn cleanup_owner(owner: Arc<OwnerState>) {
    let wall = Instant::now() + QUERY_CLEANUP_GRACE;
    let mut last_admin_error: Option<String> = None;
    let mut observed_visible = HashSet::<String>::new();
    let mut bounded_failure = false;
    loop {
        let records = {
            owner
                .data
                .lock()
                .expect("owner state poisoned")
                .children
                .iter()
                .map(|(id, child)| (id.clone(), child.clone()))
                .collect::<Vec<_>>()
        };
        if records.is_empty() {
            break;
        }
        if Instant::now() >= wall {
            bounded_failure = true;
            break;
        }

        let mut by_backend: HashMap<Uuid, CleanupBackendBatch> = HashMap::new();
        for (id, child) in records {
            by_backend
                .entry(child.backend.key)
                .or_insert_with(|| (child.backend.clone(), Vec::new()))
                .1
                .push((id, child.attempted));
        }

        let mut unresolved = false;
        for (_key, (backend, children)) in by_backend {
            let quoted = children
                .iter()
                .map(|(id, _)| format!("'{id}'"))
                .collect::<Vec<_>>()
                .join(",");
            let kill = format!("KILL QUERY WHERE query_id IN ({quoted}) ASYNC");
            if let Err(error) = backend.execute(&kill, wall).await {
                last_admin_error = Some(error.to_string());
                unresolved = true;
                continue;
            }
            let poll = format!(
                "SELECT query_id FROM system.processes WHERE query_id IN ({quoted}) FORMAT JSONEachRow"
            );
            match backend.execute(&poll, wall).await {
                Ok(body) => match parse_process_query_ids(&body) {
                    Ok(visible_ids) => {
                        for (id, attempted) in children {
                            if visible_ids.contains(&id) {
                                observed_visible.insert(id);
                                unresolved = true;
                            } else if attempted && !observed_visible.contains(&id) {
                                // An absent first poll is not terminal. HTTP headers
                                // can precede process visibility, so every attempted
                                // unfinished child is retried through the shared
                                // visibility wall until it is observed and then gone.
                                unresolved = true;
                            }
                        }
                    }
                    Err(error) => {
                        last_admin_error = Some(error.to_string());
                        unresolved = true;
                    }
                },
                Err(error) => {
                    last_admin_error = Some(error.to_string());
                    unresolved = true;
                }
            }
        }

        if !unresolved {
            break;
        }
        if Instant::now() >= wall {
            bounded_failure = true;
            break;
        }
        tokio::time::sleep_until((Instant::now() + CLEANUP_RETRY).min(wall)).await;
    }
    if bounded_failure {
        tracing::warn!(
            owner_id = %owner.logical_id,
            grace_ms = QUERY_CLEANUP_GRACE.as_millis(),
            error = last_admin_error.as_deref().unwrap_or("active child did not reach a terminal visibility state"),
            "ClickHouse query cleanup did not complete within the bounded grace"
        );
    }
    owner.finish_terminal();
}

fn parse_process_query_ids(body: &str) -> Result<HashSet<String>, ClickHouseError> {
    let mut ids = HashSet::new();
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line).map_err(|_| {
            ClickHouseError::backend("ClickHouse cleanup process poll returned malformed JSON")
        })?;
        let id = value
            .get("query_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ClickHouseError::backend(
                    "ClickHouse cleanup process poll omitted the query_id field",
                )
            })?;
        ids.insert(id.to_string());
    }
    Ok(ids)
}

pub(crate) fn extract_exception_code(body: &str) -> Option<u32> {
    let marker = "Code:";
    let start = body.find(marker)? + marker.len();
    let digits = body[start..]
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_racing_with_close_is_rejected_or_shutdown_owned() {
        let executor = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime");
        let handle = executor.handle().clone();
        let runtime = QueryRuntime::new();

        // Hold the common state lock until both admission and close are
        // waiting on it. Whichever transition wins after release must produce
        // one of the two valid outcomes.
        let state = runtime.inner.state.lock().expect("query runtime state");
        let admission_runtime = runtime.clone();
        let admission_handle = handle.clone();
        let admission = std::thread::spawn(move || {
            let _entered = admission_handle.enter();
            QueryOwner::new(&admission_runtime, QueryWorkload::Internal)
        });

        let wait_started = std::time::Instant::now();
        while runtime
            .inner
            .supervisor
            .lock()
            .expect("supervisor state")
            .is_none()
        {
            assert!(
                wait_started.elapsed() < Duration::from_secs(1),
                "admission did not reach the runtime state lock"
            );
            std::thread::yield_now();
        }

        let close_runtime = runtime.clone();
        let close = handle.spawn(async move {
            close_runtime.close_and_drain().await;
        });
        let wait_started = std::time::Instant::now();
        while runtime
            .inner
            .close_started
            .lock()
            .expect("close state")
            .is_none()
        {
            assert!(
                wait_started.elapsed() < Duration::from_secs(1),
                "close did not reach the runtime state lock"
            );
            std::thread::yield_now();
        }

        drop(state);
        let admitted = admission.join().expect("admission thread");
        executor.block_on(close).expect("close task");

        match admitted {
            Ok(owner) => assert_eq!(owner.cause(), Some(QueryCause::Shutdown)),
            Err(error) => assert_eq!(
                error.category(),
                ClickHouseErrorCategory::OwnershipViolation
            ),
        }
        assert!(runtime.is_closing());
        assert_eq!(runtime.active_owner_count(), 0);
        assert!(QueryOwner::new(&runtime, QueryWorkload::Internal).is_err());
    }

    #[tokio::test]
    async fn completed_deadline_owner_does_not_hold_drain_until_deadline() {
        let runtime = QueryRuntime::new();
        let owner = QueryOwner::with_deadline(
            &runtime,
            QueryWorkload::Internal,
            Instant::now() + Duration::from_secs(60),
        )
        .expect("deadline owner");

        // On the current-thread runtime, completion occurs before the
        // supervisor can begin its deadline job. Terminal state must therefore
        // be durable rather than an edge-triggered notification.
        owner.scope(async {}).await;
        assert_eq!(runtime.active_owner_count(), 0);
        tokio::time::timeout(Duration::from_millis(250), runtime.close_and_drain())
            .await
            .expect("completed deadline owner must not hold shutdown open");
    }
    #[test]
    fn workloads_map_to_fixed_resource_profiles() {
        for workload in [
            QueryWorkload::Mcp,
            QueryWorkload::Monitor,
            QueryWorkload::Export,
        ] {
            assert_eq!(
                QueryResourceProfile::from(workload),
                QueryResourceProfile::Interactive
            );
        }
        assert_eq!(
            QueryResourceProfile::from(QueryWorkload::Background),
            QueryResourceProfile::Background
        );
        assert_eq!(
            QueryResourceProfile::from(QueryWorkload::Migration),
            QueryResourceProfile::Migration
        );
        for workload in [QueryWorkload::Internal, QueryWorkload::Administrative] {
            assert_eq!(
                QueryResourceProfile::from(workload),
                QueryResourceProfile::Administrative
            );
        }
        for profile in QueryResourceProfile::ALL {
            assert!(profile.max_running() > 0);
            assert!(profile.max_queued() > 0);
            assert!(profile.spill_bytes() < profile.memory_bytes());
            assert!(profile.temporary_disk_bytes() > 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn interactive_admission_is_bounded_and_cancellation_aware() {
        let runtime = QueryRuntime::new();
        let mut running_owners = Vec::new();
        let mut running_permits = Vec::new();
        for _ in 0..QueryResourceProfile::Interactive.max_running() {
            let owner = QueryOwner::new(&runtime, QueryWorkload::Mcp).expect("running owner");
            running_permits.push(
                runtime
                    .admit_statement(&owner)
                    .await
                    .expect("running admission"),
            );
            running_owners.push(owner);
        }

        let mut queued_owners = Vec::new();
        let mut queued_tasks = Vec::new();
        for _ in 0..QueryResourceProfile::Interactive.max_queued() {
            let owner = QueryOwner::new(&runtime, QueryWorkload::Mcp).expect("queued owner");
            let task_owner = owner.clone();
            let task_runtime = runtime.clone();
            queued_tasks.push(tokio::spawn(async move {
                task_runtime.admit_statement(&task_owner).await
            }));
            queued_owners.push(owner);
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime.pressure_snapshot().interactive.queued
                    == QueryResourceProfile::Interactive.max_queued() as u64
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all waiters become visible");

        let rejected_owner = QueryOwner::new(&runtime, QueryWorkload::Mcp).expect("rejected owner");
        let rejected = match runtime.admit_statement(&rejected_owner).await {
            Ok(_) => panic!("bounded queue must reject excess work"),
            Err(error) => error,
        };
        assert_eq!(rejected.category(), ClickHouseErrorCategory::Busy);
        let pressure = runtime.pressure_snapshot();
        assert_eq!(
            pressure.interactive.running,
            QueryResourceProfile::Interactive.max_running() as u64
        );
        assert_eq!(
            pressure.interactive.queued,
            QueryResourceProfile::Interactive.max_queued() as u64
        );
        assert_eq!(pressure.interactive.rejected, 1);

        for owner in &queued_owners {
            owner.cancel(QueryCause::Explicit);
        }
        for task in queued_tasks {
            let error = match task.await.expect("queued admission task") {
                Ok(_) => panic!("cancelled waiter must not be admitted"),
                Err(error) => error,
            };
            assert_eq!(error.category(), ClickHouseErrorCategory::Cancelled);
        }
        drop(running_permits);
        for owner in running_owners {
            owner.cancel(QueryCause::Explicit);
        }
        rejected_owner.cancel(QueryCause::Explicit);
        runtime.close_and_drain().await;
        let pressure = runtime.pressure_snapshot();
        assert_eq!(pressure.interactive.running, 0);
        assert_eq!(pressure.interactive.queued, 0);
    }

    #[tokio::test]
    async fn administrative_transport_uses_reported_capacity_and_error_counters() {
        let runtime = QueryRuntime::new();
        let wall = Instant::now() + Duration::from_secs(1);
        let first = runtime
            .inner
            .admit_administrative(wall)
            .await
            .expect("first administrative admission");
        let second = runtime
            .inner
            .admit_administrative(wall)
            .await
            .expect("second administrative admission");
        let inner = Arc::clone(&runtime.inner);
        let queued = tokio::spawn(async move { inner.admit_administrative(wall).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.pressure_snapshot().administrative.queued != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("administrative waiter becomes visible");
        assert_eq!(runtime.pressure_snapshot().administrative.running, 2);

        drop(first);
        let third = queued
            .await
            .expect("queued task")
            .expect("queued administrative admission");
        assert_eq!(runtime.pressure_snapshot().administrative.queued, 0);
        assert_eq!(runtime.pressure_snapshot().administrative.running, 2);
        drop((second, third));

        assert_eq!(
            runtime.inner.classify_administrative_code(Some(745)),
            ClickHouseErrorCategory::Busy
        );
        assert_eq!(
            runtime.inner.classify_administrative_code(Some(241)),
            ClickHouseErrorCategory::ResourceExhausted
        );
        let pressure = runtime.pressure_snapshot();
        assert_eq!(pressure.administrative.rejected, 1);
        assert_eq!(pressure.resource_limit_events, 1);
    }
}
