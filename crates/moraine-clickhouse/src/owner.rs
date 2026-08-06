use reqwest::{Client, StatusCode, Url};
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
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const QUERY_CLEANUP_GRACE: Duration = Duration::from_secs(5);
const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const CLEANUP_RETRY: Duration = Duration::from_millis(50);
const ADMIN_CONCURRENCY: usize = 8;

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
    active: Mutex<HashMap<Uuid, Arc<OwnerState>>>,
    tx: mpsc::UnboundedSender<SupervisorMessage>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<SupervisorMessage>>>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    closing: AtomicBool,
    changed: Notify,
    close_started: Mutex<Option<Instant>>,
    close_lock: tokio::sync::Mutex<()>,
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
        Self {
            inner: Arc::new(QueryRuntimeInner {
                active: Mutex::new(HashMap::new()),
                tx,
                receiver: Mutex::new(Some(receiver)),
                supervisor: Mutex::new(None),
                closing: AtomicBool::new(false),
                changed: Notify::new(),
                close_started: Mutex::new(None),
                close_lock: tokio::sync::Mutex::new(()),
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
        self.inner.closing.load(Ordering::Acquire)
    }

    pub fn active_owner_count(&self) -> usize {
        self.inner.active.lock().expect("owner map poisoned").len()
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
        self.inner.closing.store(true, Ordering::Release);
        let _ = self.ensure_supervisor();

        let owners: Vec<_> = self
            .inner
            .active
            .lock()
            .expect("owner map poisoned")
            .values()
            .cloned()
            .collect();
        for owner in owners {
            owner.cancel(QueryCause::Shutdown);
        }

        loop {
            if self.active_owner_count() == 0 {
                break;
            }
            let notified = self.inner.changed.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
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
            .active
            .lock()
            .expect("owner map poisoned")
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
        self.active.lock().expect("owner map poisoned").remove(&id);
        self.changed.notify_waiters();
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
    terminal: Notify,
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
    headers_received: bool,
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
        if runtime.inner.closing.load(Ordering::Acquire) {
            return Err(ClickHouseError::ownership("query runtime is closing"));
        }
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
            terminal: Notify::new(),
        });
        runtime
            .inner
            .active
            .lock()
            .expect("owner map poisoned")
            .insert(id, state.clone());
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

    pub(crate) fn register_statement(
        &self,
        backend: Arc<AdminBackend>,
    ) -> Result<StatementTicket, ClickHouseError> {
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
                headers_received: false,
            },
        );
        Ok(StatementTicket {
            owner: self.state.clone(),
            child_id,
            armed: true,
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

    pub(crate) fn mark_headers_received(&self) {
        if let Some(child) = self
            .owner
            .data
            .lock()
            .expect("owner state poisoned")
            .children
            .get_mut(&self.child_id)
        {
            child.headers_received = true;
        }
    }

    pub(crate) fn succeed(&mut self) {
        if self.armed {
            self.armed = false;
            self.owner.child_succeeded(&self.child_id);
        }
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
            self.terminal.notify_waiters();
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
            self.terminal.notify_waiters();
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
        self.terminal.notify_waiters();
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
    semaphore: Semaphore,
    sequence: AtomicU64,
    owner_id: Uuid,
}

impl AdminBackend {
    pub(crate) fn new(
        url: Url,
        username: String,
        password: String,
        default_headers: reqwest::header::HeaderMap,
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
            semaphore: Semaphore::new(ADMIN_CONCURRENCY),
            sequence: AtomicU64::new(0),
            owner_id: Uuid::new_v4(),
        }))
    }

    async fn execute(&self, query: &str, wall: Instant) -> Result<String, ClickHouseError> {
        let permit = tokio::time::timeout_at(wall, self.semaphore.acquire())
            .await
            .map_err(|_| ClickHouseError::cancelled("ClickHouse cleanup semaphore timed out"))?
            .map_err(|_| ClickHouseError::cancelled("ClickHouse cleanup semaphore closed"))?;
        let now = Instant::now();
        if now >= wall {
            return Err(ClickHouseError::cancelled(
                "ClickHouse cleanup wall expired",
            ));
        }
        let timeout = (wall - now).min(ADMIN_REQUEST_TIMEOUT);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let query_id = format!(
            "moraine-administrative-{}-{sequence}",
            self.owner_id.simple()
        );
        let mut url = self.url.clone();
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("query", query);
            pairs.append_pair("query_id", &query_id);
            pairs.append_pair("replace_running_query", "0");
        }
        let mut request = self
            .client
            .post(url)
            .timeout(timeout)
            .header(reqwest::header::CONTENT_LENGTH, 0)
            .body(Vec::new());
        if !self.username.is_empty() {
            request = request.basic_auth(&self.username, Some(&self.password));
        }
        let response = request.send().await.map_err(|error| {
            ClickHouseError::transport("ClickHouse cleanup request failed", error)
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            ClickHouseError::transport("failed to read ClickHouse cleanup response", error)
        })?;
        drop(permit);
        if !status.is_success() {
            return Err(ClickHouseError::response(
                ClickHouseErrorCategory::Backend,
                "ClickHouse cleanup request was rejected",
                status,
                extract_exception_code(&body),
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
                            _ = owner.terminal.notified() => {}
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
        runtime.changed.notify_waiters();
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
