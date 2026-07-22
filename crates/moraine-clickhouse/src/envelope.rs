//! Mandatory query envelope for issue #600: every ClickHouse statement a
//!
//! # Task-local scoping and `tokio::spawn`
//!
//! The active envelope rides a tokio task-local: it follows `.await`s within
//! one task but does NOT propagate into `tokio::spawn`, `spawn_blocking`, or
//! any executor handoff. A spawned worker (projection worker, publication
//! actor, janitor, backfill task) must establish its own class envelope via
//! [`QueryEnvelope::new`] + [`QueryEnvelope::scope`]; statements issued from
//! an unscoped spawn run unenveloped pre-flip and fail closed post-flip.
//! logical Moraine operation issues shares one absolute deadline, one fixed
//! statement cap, and one cumulative read allowance, and every abandonment
//! path converges on a bounded best-effort `KILL QUERY`.
//!
//! The envelope is carried by a tokio task-local so the transport
//! (`ClickHouseClient::request_builder`) can enforce it without any
//! repository signature changes. Budgets are constructible only from
//! [`ValidatedQueryBudget`] (amendment A9), so an unlimited envelope is
//! unrepresentable here as well as in config.

use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use moraine_config::{QueryBudgetsConfig, ValidatedQueryBudget, ValidatedQueryBudgets};

use crate::{escape_literal, ClickHouseClient, ClickHouseErrorKind, ClickHouseHttpError};

/// Hard wall-clock bound on any spawned cancellation task (amendment A4/A13):
/// a KILL that cannot finish inside this window is abandoned rather than
/// allowed to block shutdown; the server's own `max_execution_time` on the
/// orphaned statement remains the authoritative backstop.
const KILL_WALL: Duration = Duration::from_secs(5);

/// Floor for the per-statement `max_execution_time` actually sent to the
/// server. Some ClickHouse builds floor fractional seconds to integers, and a
/// floored 0 means unlimited — the exact opposite of the envelope's job.
/// Admission still fails fast at zero remaining (no 0.001s floor there); this
/// only lets an admitted sub-second statement overrun the absolute deadline
/// by less than a second, after which the next admission fails.
pub(crate) const MIN_SERVER_EXECUTION_SECONDS: f64 = 1.0;

/// Process-wide count of statements issued without an active envelope.
/// Pre-flip (before the fail-closed work item) these execute exactly as
/// today; the counter is the telemetry that gates the flip (amendment A2).
static UNENVELOPED_STATEMENTS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_unenveloped_statement() {
    UNENVELOPED_STATEMENTS.fetch_add(1, Ordering::Relaxed);
}

/// Total statements this process has issued outside any query envelope.
pub fn unenveloped_statement_count() -> u64 {
    UNENVELOPED_STATEMENTS.load(Ordering::Relaxed)
}

/// Administrative budget used for cancellation when the caller did not wire
/// an operator-configured one: the bundled `[query_budgets.administrative]`
/// defaults, validated once.
fn default_administrative_budget() -> ValidatedQueryBudget {
    static ADMIN: OnceLock<ValidatedQueryBudget> = OnceLock::new();
    *ADMIN.get_or_init(|| {
        ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
            .expect("bundled default query budgets are valid")
            .administrative
    })
}

/// Admission/telemetry class of a logical operation. Export is a budget
/// choice (`query_budgets.export`), not a class: exports run as
/// [`QueryClass::Migration`] per amendment A6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryClass {
    Interactive,
    Background,
    Migration,
    Administrative,
}

impl QueryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Background => "background",
            Self::Migration => "migration",
            Self::Administrative => "administrative",
        }
    }

    /// Whether abandoned statements of this class get a best-effort KILL.
    /// Migration statements rely on server `max_execution_time` only —
    /// killing DDL mid-flight risks partial state (amendment A5) — and
    /// Administrative statements are themselves KILLs/telemetry one-shots, so
    /// guarding them would recurse (a dropped KILL spawning another KILL).
    fn arms_cancel_guards(self) -> bool {
        matches!(self, Self::Interactive | Self::Background)
    }
}

/// Which cumulative read allowance ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AllowanceResource {
    Rows,
    Bytes,
}

impl AllowanceResource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rows => "read_rows",
            Self::Bytes => "read_bytes",
        }
    }
}

/// Typed envelope failure. Attached as the root of the returned
/// `anyhow::Error` when statement admission fails, so callers can classify
/// with [`envelope_error_kind`] or a direct downcast.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EnvelopeError {
    /// No envelope is active on this task.
    Missing,
    /// The request's absolute deadline has passed; the statement was refused
    /// client-side without reaching the server.
    DeadlineExpired { budget: Duration },
    /// The request already issued its fixed maximum number of statements.
    StatementCapExceeded { cap: u32 },
    /// The request's cumulative read allowance is spent.
    AllowanceExhausted {
        resource: AllowanceResource,
        budget: u64,
    },
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "no active query envelope"),
            Self::DeadlineExpired { budget } => write!(
                f,
                "query envelope deadline expired (budget {:.3}s)",
                budget.as_secs_f64()
            ),
            Self::StatementCapExceeded { cap } => {
                write!(f, "query envelope statement cap exceeded (cap {cap})")
            }
            Self::AllowanceExhausted { resource, budget } => write!(
                f,
                "query envelope {} allowance exhausted (budget {budget})",
                resource.as_str()
            ),
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// Budget-relevant kind of the envelope or ClickHouse error in `error`'s
/// chain, if any. Extends [`crate::clickhouse_error_kind`] with local
/// admission failures so callers classify both through one helper.
pub fn envelope_error_kind(error: &anyhow::Error) -> Option<ClickHouseErrorKind> {
    for cause in error.chain() {
        if let Some(envelope_error) = cause.downcast_ref::<EnvelopeError>() {
            return Some(match envelope_error {
                EnvelopeError::DeadlineExpired { .. } => ClickHouseErrorKind::DeadlineExceeded,
                EnvelopeError::StatementCapExceeded { .. }
                | EnvelopeError::AllowanceExhausted { .. } => {
                    ClickHouseErrorKind::ResourceExhausted
                }
                EnvelopeError::Missing => ClickHouseErrorKind::Other,
            });
        }
        if let Some(http_error) = cause.downcast_ref::<ClickHouseHttpError>() {
            return Some(http_error.kind());
        }
    }
    None
}

/// Point-in-time view of one envelope's accounting, for telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EnvelopeStatsSnapshot {
    /// Statements admitted (issued or about to be issued).
    pub statements: u64,
    /// Rows consumed from the cumulative allowance (trustworthy summaries
    /// only: enveloped, buffered, non-insert statements).
    pub rows_consumed: u64,
    pub bytes_consumed: u64,
    /// Remaining cumulative allowances.
    pub rows_remaining: u64,
    pub bytes_remaining: u64,
    /// Local admissions refused for an expired deadline, plus server
    /// statements classified as deadline-exceeded.
    pub deadline_exceeded: u64,
    /// Local admissions refused for cap/allowance exhaustion, plus server
    /// statements classified as resource-exhausted.
    pub resource_exhausted: u64,
}

#[derive(Default)]
struct EnvelopeStats {
    statements: AtomicU64,
    rows_consumed: AtomicU64,
    bytes_consumed: AtomicU64,
    deadline_exceeded: AtomicU64,
    resource_exhausted: AtomicU64,
}

/// A ClickHouse backend that executed at least one statement of this
/// request; cancellation KILLs through every stamped target (amendment A1).
struct CancelTarget {
    endpoint: String,
    database: String,
    client: ClickHouseClient,
}

/// State shared between a request-level envelope and its narrowed scopes:
/// narrowing tightens only the deadline, never resets ids, sequence,
/// allowances, or the statement cap.
struct EnvelopeShared {
    request_id: Arc<str>,
    class: QueryClass,
    sequence: AtomicU32,
    statement_cap: u32,
    rows_remaining: AtomicU64,
    bytes_remaining: AtomicU64,
    rows_budget: u64,
    bytes_budget: u64,
    stats: EnvelopeStats,
    cancel_targets: Mutex<Vec<CancelTarget>>,
    admin_budget: ValidatedQueryBudget,
}

tokio::task_local! {
    static ACTIVE_ENVELOPE: Arc<QueryEnvelope>;
}

/// The non-optional execution envelope for one logical operation. All
/// mutable state is atomics behind one `Arc`; narrowed scopes share it.
///
/// The manual `Debug` exists because stamped cancel targets hold
/// `ClickHouseClient`s, which are not `Debug`.
pub struct QueryEnvelope {
    shared: Arc<EnvelopeShared>,
    deadline: Instant,
    /// The class deadline budget, kept for structured error payloads.
    budget_deadline: Duration,
    memory_bytes: u64,
    spill_bytes: u64,
}

impl std::fmt::Debug for QueryEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryEnvelope")
            .field("request_id", &self.shared.request_id)
            .field("class", &self.shared.class)
            .field("deadline", &self.deadline)
            .field("statement_cap", &self.shared.statement_cap)
            .finish_non_exhaustive()
    }
}

/// Result of admitting one statement against the envelope.
#[derive(Debug)]
pub(crate) struct StatementAdmission {
    pub(crate) query_id: String,
    pub(crate) remaining: Duration,
    pub(crate) rows_remaining: u64,
    pub(crate) bytes_remaining: u64,
}

impl QueryEnvelope {
    /// Envelope with the bundled-default administrative budget for its
    /// cancellation statements. `kind` becomes part of the request id
    /// (`moraine-{kind}-{pid}-{nanos}-{seq}`).
    pub fn new(kind: &str, class: QueryClass, budget: &ValidatedQueryBudget) -> Arc<Self> {
        Self::new_with_admin_budget(kind, class, budget, &default_administrative_budget())
    }

    /// Envelope whose cancellation statements run under the supplied
    /// (operator-configured) administrative budget.
    pub fn new_with_admin_budget(
        kind: &str,
        class: QueryClass,
        budget: &ValidatedQueryBudget,
        admin_budget: &ValidatedQueryBudget,
    ) -> Arc<Self> {
        let deadline_budget = budget.deadline();
        Arc::new(Self {
            shared: Arc::new(EnvelopeShared {
                request_id: generate_request_id(kind).into(),
                class,
                sequence: AtomicU32::new(0),
                statement_cap: budget.statement_cap(),
                rows_remaining: AtomicU64::new(budget.read_rows()),
                bytes_remaining: AtomicU64::new(budget.read_bytes()),
                rows_budget: budget.read_rows(),
                bytes_budget: budget.read_bytes(),
                stats: EnvelopeStats::default(),
                cancel_targets: Mutex::new(Vec::new()),
                admin_budget: *admin_budget,
            }),
            deadline: Instant::now() + deadline_budget,
            budget_deadline: deadline_budget,
            memory_bytes: budget.memory_bytes(),
            spill_bytes: budget.spill_bytes(),
        })
    }

    /// The envelope active on this task, or [`EnvelopeError::Missing`].
    pub fn current() -> std::result::Result<Arc<Self>, EnvelopeError> {
        ACTIVE_ENVELOPE
            .try_with(Arc::clone)
            .map_err(|_| EnvelopeError::Missing)
    }

    pub fn request_id(&self) -> &str {
        &self.shared.request_id
    }

    pub fn class(&self) -> QueryClass {
        self.shared.class
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Remaining absolute deadline, failing fast once expired.
    pub fn remaining(&self) -> std::result::Result<Duration, EnvelopeError> {
        match self.deadline.checked_duration_since(Instant::now()) {
            Some(remaining) if !remaining.is_zero() => Ok(remaining),
            _ => Err(EnvelopeError::DeadlineExpired {
                budget: self.budget_deadline,
            }),
        }
    }

    pub fn admin_budget(&self) -> &ValidatedQueryBudget {
        &self.shared.admin_budget
    }

    pub fn stats(&self) -> EnvelopeStatsSnapshot {
        let stats = &self.shared.stats;
        EnvelopeStatsSnapshot {
            statements: stats.statements.load(Ordering::Relaxed),
            rows_consumed: stats.rows_consumed.load(Ordering::Relaxed),
            bytes_consumed: stats.bytes_consumed.load(Ordering::Relaxed),
            rows_remaining: self.shared.rows_remaining.load(Ordering::Relaxed),
            bytes_remaining: self.shared.bytes_remaining.load(Ordering::Relaxed),
            deadline_exceeded: stats.deadline_exceeded.load(Ordering::Relaxed),
            resource_exhausted: stats.resource_exhausted.load(Ordering::Relaxed),
        }
    }

    /// Run `f` with this envelope active. A request-level cleanup guard is
    /// armed for the duration: if the scope future is dropped before
    /// completion (client disconnect, outer timeout, shutdown), one bounded
    /// prefix KILL (`query_id = X OR startsWith(query_id, 'X-')`) is spawned
    /// through every stamped backend. Completing normally — with either
    /// output — disarms it; the transport's per-statement guards remain the
    /// primary cancellation path (amendment A4).
    pub async fn scope<F: Future>(self: Arc<Self>, f: F) -> F::Output {
        let mut guard = RequestCleanupGuard {
            envelope: Some(Arc::clone(&self)),
        };
        let output = ACTIVE_ENVELOPE.scope(self, f).await;
        guard.envelope = None;
        output
    }

    /// Run `f` under the active envelope with the remaining deadline
    /// tightened to at most `deadline_cap` from now. Request id, statement
    /// sequence, statement cap, and read allowances are the parent's —
    /// narrowing can only tighten, never reset (so a tool cannot widen its
    /// budget by re-scoping). Without an active envelope `f` runs unchanged,
    /// matching the pre-flip permissive posture (amendment A2).
    pub async fn scope_narrowed<F: Future>(deadline_cap: Duration, f: F) -> F::Output {
        let Ok(parent) = Self::current() else {
            return f.await;
        };
        // A cap large enough to overflow Instant means "no extra tightening";
        // unchecked `Instant + Duration` would abort the process instead.
        let capped = Instant::now()
            .checked_add(deadline_cap)
            .unwrap_or(parent.deadline);
        let narrowed = Arc::new(Self {
            shared: Arc::clone(&parent.shared),
            deadline: parent.deadline.min(capped),
            budget_deadline: parent.budget_deadline,
            memory_bytes: parent.memory_bytes,
            spill_bytes: parent.spill_bytes,
        });
        ACTIVE_ENVELOPE.scope(narrowed, f).await
    }

    pub(crate) fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    pub(crate) fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    /// Admit one statement: deadline first (fail fast, no floor), then the
    /// statement cap, then the cumulative read allowances. Allocates the
    /// child query id `{request_id}-{seq}`, preserving the KILL prefix
    /// contract.
    pub(crate) fn admit_statement(&self) -> std::result::Result<StatementAdmission, EnvelopeError> {
        let stats = &self.shared.stats;
        let remaining = match self.remaining() {
            Ok(remaining) => remaining,
            Err(error) => {
                stats.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        let sequence = self.shared.sequence.fetch_add(1, Ordering::Relaxed);
        if sequence >= self.shared.statement_cap {
            stats.resource_exhausted.fetch_add(1, Ordering::Relaxed);
            return Err(EnvelopeError::StatementCapExceeded {
                cap: self.shared.statement_cap,
            });
        }
        let rows_remaining = self.shared.rows_remaining.load(Ordering::Relaxed);
        if rows_remaining == 0 {
            stats.resource_exhausted.fetch_add(1, Ordering::Relaxed);
            return Err(EnvelopeError::AllowanceExhausted {
                resource: AllowanceResource::Rows,
                budget: self.shared.rows_budget,
            });
        }
        let bytes_remaining = self.shared.bytes_remaining.load(Ordering::Relaxed);
        if bytes_remaining == 0 {
            stats.resource_exhausted.fetch_add(1, Ordering::Relaxed);
            return Err(EnvelopeError::AllowanceExhausted {
                resource: AllowanceResource::Bytes,
                budget: self.shared.bytes_budget,
            });
        }
        stats.statements.fetch_add(1, Ordering::Relaxed);
        Ok(StatementAdmission {
            query_id: format!("{}-{sequence}", self.shared.request_id),
            remaining,
            rows_remaining,
            bytes_remaining,
        })
    }

    /// Decrement the cumulative allowances from a trustworthy
    /// `X-ClickHouse-Summary` (saturating).
    pub(crate) fn consume(&self, rows: u64, bytes: u64) {
        if rows > 0 {
            let _ = self.shared.rows_remaining.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |value| Some(value.saturating_sub(rows)),
            );
            self.shared
                .stats
                .rows_consumed
                .fetch_add(rows, Ordering::Relaxed);
        }
        if bytes > 0 {
            let _ = self.shared.bytes_remaining.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |value| Some(value.saturating_sub(bytes)),
            );
            self.shared
                .stats
                .bytes_consumed
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record the backend that executes this request's statements so
    /// cancellation targets the right server(s); deduplicated by endpoint
    /// URL + database (amendment A1).
    pub(crate) fn stamp_cancel_target(&self, client: &ClickHouseClient) {
        let endpoint = client.config().url.clone();
        let database = client.config().database.clone();
        let mut targets = match self.shared.cancel_targets.lock() {
            Ok(targets) => targets,
            Err(poisoned) => poisoned.into_inner(),
        };
        if targets
            .iter()
            .any(|target| target.endpoint == endpoint && target.database == database)
        {
            return;
        }
        targets.push(CancelTarget {
            endpoint,
            database,
            client: client.clone(),
        });
    }

    /// Bump per-envelope telemetry for a server-side failure already
    /// classified by [`crate::clickhouse_error_kind`].
    pub(crate) fn note_server_error_kind(&self, kind: Option<ClickHouseErrorKind>) {
        match kind {
            Some(ClickHouseErrorKind::DeadlineExceeded) => {
                self.shared
                    .stats
                    .deadline_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(ClickHouseErrorKind::ResourceExhausted) => {
                self.shared
                    .stats
                    .resource_exhausted
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Whether the transport should arm a per-statement drop guard for this
    /// envelope's statements.
    pub(crate) fn arms_cancel_guards(&self) -> bool {
        self.shared.class.arms_cancel_guards()
    }

    fn statements_issued(&self) -> u64 {
        self.shared.stats.statements.load(Ordering::Relaxed)
    }

    fn cancel_target_clients(&self) -> Vec<ClickHouseClient> {
        let targets = match self.shared.cancel_targets.lock() {
            Ok(targets) => targets,
            Err(poisoned) => poisoned.into_inner(),
        };
        targets.iter().map(|target| target.client.clone()).collect()
    }
}

/// Request-level cleanup armed by [`QueryEnvelope::scope`]. Fires only when
/// the scope future is dropped before completion.
struct RequestCleanupGuard {
    envelope: Option<Arc<QueryEnvelope>>,
}

impl Drop for RequestCleanupGuard {
    fn drop(&mut self) {
        let Some(envelope) = self.envelope.take() else {
            return;
        };
        if !envelope.arms_cancel_guards() || envelope.statements_issued() == 0 {
            return;
        }
        let clients = envelope.cancel_target_clients();
        if clients.is_empty() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let request_id = Arc::clone(&envelope.shared.request_id);
        let admin_budget = envelope.shared.admin_budget;
        handle.spawn(async move {
            for client in clients {
                let _ = kill_query_prefix(&client, &request_id, &admin_budget).await;
            }
        });
    }
}

/// Per-statement cancel-on-drop guard, armed by the transport for
/// Interactive/Background statements. Dropping it while armed spawns a
/// detached bounded `KILL QUERY WHERE query_id = '<child>' SYNC`.
pub(crate) struct StatementDropGuard {
    armed: Option<StatementKill>,
}

struct StatementKill {
    query_id: String,
    client: ClickHouseClient,
    admin_budget: ValidatedQueryBudget,
}

impl StatementDropGuard {
    pub(crate) fn new(
        query_id: String,
        client: ClickHouseClient,
        admin_budget: ValidatedQueryBudget,
    ) -> Self {
        Self {
            armed: Some(StatementKill {
                query_id,
                client,
                admin_budget,
            }),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = None;
    }
}

impl Drop for StatementDropGuard {
    fn drop(&mut self) {
        let Some(kill) = self.armed.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            let _ = kill_query_id(&kill.client, &kill.query_id, &kill.admin_budget).await;
        });
    }
}

/// Bounded best-effort KILL of every statement belonging to `request_id`:
/// the id itself and all `{request_id}-{seq}` children (the prefix contract
/// shared with the legacy `cancel_query` path). Runs under an
/// Administrative-class envelope built from `admin_budget` and a hard
/// [`KILL_WALL`] client wall, so cancellation can never block shutdown.
pub async fn kill_query_prefix(
    client: &ClickHouseClient,
    request_id: &str,
    admin_budget: &ValidatedQueryBudget,
) -> Result<()> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Ok(());
    }
    let child_prefix = format!("{request_id}-");
    let sql = format!(
        "KILL QUERY WHERE query_id = {} OR startsWith(query_id, {}) SYNC",
        escape_literal(request_id),
        escape_literal(&child_prefix)
    );
    run_bounded_kill(client, sql, admin_budget).await
}

/// Bounded best-effort KILL of one exact query id (a single enveloped child
/// statement).
async fn kill_query_id(
    client: &ClickHouseClient,
    query_id: &str,
    admin_budget: &ValidatedQueryBudget,
) -> Result<()> {
    let sql = format!(
        "KILL QUERY WHERE query_id = {} SYNC",
        escape_literal(query_id)
    );
    run_bounded_kill(client, sql, admin_budget).await
}

async fn run_bounded_kill(
    client: &ClickHouseClient,
    sql: String,
    admin_budget: &ValidatedQueryBudget,
) -> Result<()> {
    let envelope = QueryEnvelope::new_with_admin_budget(
        "kill",
        QueryClass::Administrative,
        admin_budget,
        admin_budget,
    );
    tokio::time::timeout(
        KILL_WALL,
        envelope.scope(async move { client.request_text(&sql, None, None, false, None).await }),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "bounded KILL abandoned after {:.0}s wall",
            KILL_WALL.as_secs_f64()
        )
    })?
    .context("bounded KILL statement failed")?;
    Ok(())
}

/// Statement cap for a batch-shaped envelope (amendment A7): a fixed
/// overhead plus a per-item cost, computed at envelope creation for that
/// batch so no legitimate batch can exceed its own cap. Saturates at
/// `u32::MAX` and never returns below 1.
pub fn batch_statement_cap(fixed: u32, per_item: u32, items: usize) -> u32 {
    let items = u32::try_from(items).unwrap_or(u32::MAX);
    fixed.saturating_add(per_item.saturating_mul(items)).max(1)
}

fn generate_request_id(kind: &str) -> String {
    static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let sequence = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    // Same shape as the mcp-core generator this supersedes:
    // "moraine-{kind}-{pid}-{nanos}-{seq}". Child statements append "-{seq}",
    // so `KILL ... startsWith(query_id, '{id}-')` targets exactly one request.
    format!(
        "moraine-{}-{}-{nanos}-{sequence}",
        sanitize_kind(kind),
        std::process::id()
    )
}

/// Keep request-id kinds to lowercase alphanumerics and dashes so ids stay
/// safe inside `KILL ... startsWith(...)` literals and query-log filters.
fn sanitize_kind(kind: &str) -> String {
    let sanitized: String = kind
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "request".to_string()
    } else {
        sanitized
    }
}

/// Test-only budget factory shared with the transport tests in `lib.rs`.
#[cfg(test)]
pub(crate) fn test_budget(
    deadline_seconds: f64,
    statement_cap: u32,
    read_rows: u64,
    read_bytes: u64,
) -> ValidatedQueryBudget {
    let cfg = QueryBudgetsConfig {
        interactive: moraine_config::QueryBudgetClassConfig {
            deadline_seconds,
            memory_bytes: 64 * 1024 * 1024,
            spill_bytes: 8 * 1024 * 1024,
            read_rows,
            read_bytes,
            statement_cap,
        },
        ..QueryBudgetsConfig::default()
    };
    ValidatedQueryBudgets::from_config(&cfg)
        .expect("test budget must validate")
        .interactive
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_matches_generator_shape_and_child_prefix_contract() {
        let budget = test_budget(30.0, 8, 1_000, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let id = envelope.request_id().to_string();
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts[0], "moraine");
        assert_eq!(parts[1], "request");
        assert_eq!(parts[2], std::process::id().to_string());
        assert!(parts[3].parse::<u128>().is_ok(), "nanos segment: {id}");
        assert!(parts[4].parse::<u64>().is_ok(), "sequence segment: {id}");

        let first = envelope.admit_statement().expect("first admission");
        let second = envelope.admit_statement().expect("second admission");
        assert_eq!(first.query_id, format!("{id}-0"));
        assert_eq!(second.query_id, format!("{id}-1"));
        assert!(first.query_id.starts_with(&format!("{id}-")));
    }

    #[test]
    fn request_ids_are_unique_across_envelopes() {
        let budget = test_budget(30.0, 8, 1_000, 1_000_000);
        let first = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let second = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        assert_ne!(first.request_id(), second.request_id());
    }

    #[test]
    fn sanitize_kind_replaces_unsafe_characters() {
        assert_eq!(sanitize_kind("Monitor Read!"), "monitor-read-");
        assert_eq!(sanitize_kind(""), "request");
        assert_eq!(sanitize_kind("kill"), "kill");
    }

    #[test]
    fn statement_cap_is_enforced() {
        let budget = test_budget(30.0, 2, 1_000, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        envelope.admit_statement().expect("statement 1");
        envelope.admit_statement().expect("statement 2");
        assert_eq!(
            envelope.admit_statement().unwrap_err(),
            EnvelopeError::StatementCapExceeded { cap: 2 }
        );
        assert_eq!(envelope.stats().resource_exhausted, 1);
        assert_eq!(envelope.stats().statements, 2);
    }

    #[test]
    fn expired_deadline_fails_fast_before_cap_check() {
        let budget = test_budget(0.005, 1, 1_000, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        envelope
            .admit_statement()
            .expect("statement inside deadline");
        std::thread::sleep(Duration::from_millis(20));
        // The cap is also exhausted now, but the deadline check comes first.
        let error = envelope.admit_statement().unwrap_err();
        assert!(
            matches!(error, EnvelopeError::DeadlineExpired { .. }),
            "expected DeadlineExpired, got {error:?}"
        );
        assert_eq!(envelope.stats().deadline_exceeded, 1);
    }

    #[test]
    fn statement_cap_fires_before_allowance_exhaustion() {
        let budget = test_budget(30.0, 1, 100, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        envelope.admit_statement().expect("statement 1");
        envelope.consume(100, 0);
        assert_eq!(
            envelope.admit_statement().unwrap_err(),
            EnvelopeError::StatementCapExceeded { cap: 1 }
        );
    }

    #[test]
    fn exhausted_row_allowance_refuses_admission() {
        let budget = test_budget(30.0, 8, 100, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let admission = envelope.admit_statement().expect("statement 1");
        assert_eq!(admission.rows_remaining, 100);
        envelope.consume(100, 10);
        assert_eq!(
            envelope.admit_statement().unwrap_err(),
            EnvelopeError::AllowanceExhausted {
                resource: AllowanceResource::Rows,
                budget: 100,
            }
        );
    }

    #[test]
    fn exhausted_byte_allowance_refuses_admission() {
        let budget = test_budget(30.0, 8, 1_000, 2_048);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        envelope.admit_statement().expect("statement 1");
        envelope.consume(10, 2_048);
        assert_eq!(
            envelope.admit_statement().unwrap_err(),
            EnvelopeError::AllowanceExhausted {
                resource: AllowanceResource::Bytes,
                budget: 2_048,
            }
        );
    }

    #[test]
    fn consume_saturates_and_reports_in_stats() {
        let budget = test_budget(30.0, 8, 100, 1_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        envelope.consume(60, 400);
        envelope.consume(90, 900);
        let stats = envelope.stats();
        assert_eq!(stats.rows_consumed, 150);
        assert_eq!(stats.bytes_consumed, 1_300);
        assert_eq!(stats.rows_remaining, 0);
        assert_eq!(stats.bytes_remaining, 0);
    }

    #[tokio::test]
    async fn current_is_missing_outside_scope() {
        assert_eq!(
            QueryEnvelope::current().unwrap_err(),
            EnvelopeError::Missing
        );
    }

    #[tokio::test]
    async fn scope_installs_envelope_and_narrowing_shares_budget() {
        let budget = test_budget(30.0, 8, 1_000, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let request_id = envelope.request_id().to_string();
        let outer_deadline = envelope.deadline();
        Arc::clone(&envelope)
            .scope(async move {
                let active = QueryEnvelope::current().expect("envelope active");
                assert_eq!(active.request_id(), request_id);
                let first = active.admit_statement().expect("outer statement");
                assert_eq!(first.query_id, format!("{request_id}-0"));

                QueryEnvelope::scope_narrowed(Duration::from_secs(2), async {
                    let narrowed = QueryEnvelope::current().expect("narrowed envelope");
                    // Same request id and shared sequence: no budget reset.
                    assert_eq!(narrowed.request_id(), request_id);
                    let second = narrowed.admit_statement().expect("narrowed statement");
                    assert_eq!(second.query_id, format!("{request_id}-1"));
                    // Deadline tightened to roughly the 2s cap.
                    assert!(narrowed.deadline() < outer_deadline);
                    narrowed.consume(123, 456);
                })
                .await;

                // Consumption inside the narrowed scope drained the shared
                // allowance and stats.
                let stats = active.stats();
                assert_eq!(stats.statements, 2);
                assert_eq!(stats.rows_consumed, 123);
                assert_eq!(stats.bytes_consumed, 456);
            })
            .await;
    }

    #[tokio::test]
    async fn scope_narrowed_can_only_tighten_the_deadline() {
        let budget = test_budget(1.0, 8, 1_000, 1_000_000);
        let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let outer_deadline = envelope.deadline();
        envelope
            .scope(async move {
                // A cap far larger than the remaining deadline must not widen it.
                QueryEnvelope::scope_narrowed(Duration::from_secs(3600), async {
                    let narrowed = QueryEnvelope::current().expect("narrowed envelope");
                    assert_eq!(narrowed.deadline(), outer_deadline);
                })
                .await;
            })
            .await;
    }

    #[tokio::test]
    async fn scope_narrowed_without_envelope_runs_future_unchanged() {
        let value = QueryEnvelope::scope_narrowed(Duration::from_secs(1), async {
            assert!(QueryEnvelope::current().is_err());
            7
        })
        .await;
        assert_eq!(value, 7);
    }

    #[test]
    fn batch_statement_cap_arithmetic_and_saturation() {
        assert_eq!(batch_statement_cap(4, 4, 256), 4 + 4 * 256);
        assert_eq!(batch_statement_cap(2, 0, 10_000), 2);
        assert_eq!(batch_statement_cap(0, 0, 0), 1);
        assert_eq!(batch_statement_cap(u32::MAX, 8, usize::MAX), u32::MAX);
    }

    #[test]
    fn envelope_error_kind_classifies_local_and_server_failures() {
        let deadline = anyhow::Error::new(EnvelopeError::DeadlineExpired {
            budget: Duration::from_secs(15),
        });
        assert_eq!(
            envelope_error_kind(&deadline),
            Some(ClickHouseErrorKind::DeadlineExceeded)
        );

        let cap = anyhow::Error::new(EnvelopeError::StatementCapExceeded { cap: 4 })
            .context("outer context");
        assert_eq!(
            envelope_error_kind(&cap),
            Some(ClickHouseErrorKind::ResourceExhausted)
        );

        let allowance = anyhow::Error::new(EnvelopeError::AllowanceExhausted {
            resource: AllowanceResource::Bytes,
            budget: 10,
        });
        assert_eq!(
            envelope_error_kind(&allowance),
            Some(ClickHouseErrorKind::ResourceExhausted)
        );

        let missing = anyhow::Error::new(EnvelopeError::Missing);
        assert_eq!(
            envelope_error_kind(&missing),
            Some(ClickHouseErrorKind::Other)
        );

        assert_eq!(envelope_error_kind(&anyhow!("unrelated")), None);
    }
}
