use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::AHashMap as HashMap;
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use moraine_clickhouse::{
    envelope_error_kind, ClickHouseClient, ClickHouseErrorKind, ClickHouseHttpError, EnvelopeError,
    QueryClass, QueryEnvelope,
};
use moraine_config::{QueryBudgetsConfig, ValidatedQueryBudget, ValidatedQueryBudgets};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::warn;
use uuid::Uuid;

const REPOSITORY_READ_SETTINGS: [(&str, &str); 1] =
    [("do_not_merge_across_partitions_select_final", "0")];

/// Compatibility shim over the transport query envelope (issue #600).
///
/// Query ids are owned by the transport now: every enveloped statement runs
/// as `{request_id}-{seq}` and the caller-supplied id here is ignored. The
/// function survives only so boundaries that have not yet migrated to
/// [`QueryEnvelope::scope`] keep compiling; without an active envelope the
/// statements fail closed at the transport (amendment A2, post-flip).
pub async fn with_repository_query_id<F>(_query_id: String, future: F) -> F::Output
where
    F: Future,
{
    future.await
}

/// Compatibility shim over [`QueryEnvelope::scope_narrowed`]: tightens the
/// active envelope's deadline to at most `deadline` — narrowing only, never
/// resetting ids, statement caps, or allowances. The caller-supplied id is
/// ignored (the transport owns query ids); without an active envelope the
/// future runs unchanged.
pub async fn with_repository_query_deadline<F>(
    _query_id: String,
    deadline: Instant,
    future: F,
) -> F::Output
where
    F: Future,
{
    let cap = deadline.saturating_duration_since(Instant::now());
    QueryEnvelope::scope_narrowed(cap, future).await
}

/// The administrative budget for repository-issued cancellation and telemetry
/// statements: the active envelope's configured one when present, else the
/// validated bundled default.
pub(crate) fn administrative_query_budget() -> ValidatedQueryBudget {
    if let Ok(envelope) = QueryEnvelope::current() {
        return *envelope.admin_budget();
    }
    default_administrative_query_budget()
}

pub(crate) fn default_administrative_query_budget() -> ValidatedQueryBudget {
    static ADMIN: OnceLock<ValidatedQueryBudget> = OnceLock::new();
    *ADMIN.get_or_init(|| {
        ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
            .expect("bundled default query budgets are valid")
            .administrative
    })
}

use crate::cursor::{
    decode_cursor, encode_cursor, ConversationCursor, McpSessionListCursor, SessionEventCursor,
    TurnCursor,
};
use crate::domain::{
    is_user_facing_content_event, AnalyticsConcurrencyPoint, AnalyticsRange, AnalyticsSnapshot,
    AnalyticsTokenPoint, AnalyticsTurnPoint, AnalyticsWindow, CanonicalContinuation,
    CanonicalReadAnchor, CanonicalReadOutcome, CanonicalSessionPage, CanonicalSessionSignals,
    CanonicalTurnPage, Conversation, ConversationDetailOptions, ConversationListFilter,
    ConversationListSort, ConversationMode, ConversationSearchHit, ConversationSearchQuery,
    ConversationSearchResults, ConversationSearchStats, ConversationSummary, FileAttentionQuery,
    FileAttentionTouch, McpEventOpen, McpEventRef, McpEventSummary, McpEventType,
    McpSessionListFilter, McpSessionListItem, McpSessionOpen, McpTurnCompact, McpTurnOpen,
    McpTurnRef, OpenContext, OpenEvent, OpenEventRequest, Page, PageRequest, RepoConfig,
    SearchEventHit, SearchEventKind, SearchEventsQuery, SearchEventsResult, SearchEventsStats,
    SearchMcpEventHit, SearchMcpEventsQuery, SearchMcpEventsResult, SearchMcpEventsStats,
    SearchStrategyHint, SessionAnalytics, SessionAnalyticsQuery, SessionEventsDirection,
    SessionEventsQuery, SessionMetadata, SessionOriginScope, SessionSearchQuery,
    SessionSearchResults, SessionStep, SessionTurn, ToolResult, TraceEvent, Turn, TurnListFilter,
    TurnSummary, WebSearchEvent,
};
use crate::error::{RepoError, RepoResult};
use crate::repo::ConversationRepository;

mod analytics;
mod cache;
mod canonical_list;
mod canonical_open;
mod consistency;
mod file_attention;
mod helpers;
mod list;
mod mcp_open_read;
mod open;
mod operations;
mod repo_impl;
mod rows;
mod scope;
mod search;
mod search_canonical;
mod sql;

#[cfg(test)]
mod tests;

use cache::*;
use consistency::*;
use helpers::*;
use rows::*;
use sql::*;

#[derive(Clone)]
pub struct ClickHouseConversationRepository {
    ch: ClickHouseClient,
    cfg: RepoConfig,
    publication_mode: PublicationConsistencyMode,
    stats_cache: Arc<RwLock<SearchStatsCache>>,
    search_cache: Arc<RwLock<HashMap<String, SearchEventsCacheEntry>>>,
    mcp_search_cache: Arc<RwLock<HashMap<String, SearchMcpEventsCacheEntry>>>,
    search_doc_extra_cache: Arc<RwLock<HashMap<String, SearchDocExtraCacheEntry>>>,
    analytics_cache: Arc<[Mutex<Option<AnalyticsCacheEntry>>; ANALYTICS_RANGE_COUNT]>,
    /// Sessions already proven to fall inside `cfg.session_scope`, mapped to
    /// the publication token under which that proof was obtained. A new proof
    /// replaces the prior revision for the same session, and the total entry
    /// count is bounded. Negative results are not cached.
    scoped_session_cache: Arc<RwLock<HashMap<String, String>>>,
    /// One-shot canonical read-index readiness latch (issue #598 `open_v2`),
    /// gating the issue-599 directory session-listing path.
    /// See [`Self::canonical_list_path_ready`].
    canonical_list_ready: Arc<OnceLock<bool>>,
}

impl ClickHouseConversationRepository {
    pub fn new(ch: ClickHouseClient, cfg: RepoConfig) -> Self {
        Self::new_with_publication_mode(ch, cfg, PublicationConsistencyMode::Local)
    }

    pub(crate) fn new_shared(ch: ClickHouseClient, cfg: RepoConfig) -> Self {
        Self::new_with_publication_mode(ch, cfg, PublicationConsistencyMode::Shared)
    }

    pub(crate) fn new_with_publication_mode(
        ch: ClickHouseClient,
        cfg: RepoConfig,
        publication_mode: PublicationConsistencyMode,
    ) -> Self {
        Self {
            ch,
            cfg,
            publication_mode,
            stats_cache: Arc::new(RwLock::new(SearchStatsCache::default())),
            search_cache: Arc::new(RwLock::new(HashMap::new())),
            mcp_search_cache: Arc::new(RwLock::new(HashMap::new())),
            search_doc_extra_cache: Arc::new(RwLock::new(HashMap::new())),
            analytics_cache: Arc::new(std::array::from_fn(|_| Mutex::new(None))),
            scoped_session_cache: Arc::new(RwLock::new(HashMap::new())),
            canonical_list_ready: Arc::new(OnceLock::new()),
        }
    }

    pub fn config(&self) -> &RepoConfig {
        &self.cfg
    }

    /// Whether session listing may use the issue-599 directory path.
    ///
    /// The gate is the issue-598 `open_v2` readiness key — the SAME authority
    /// the `open` cutover uses ([`crate::probe_open_v2_ready`]), deliberately
    /// not the weaker `core_indexes` coverage flag. `open_v2.ready` is
    /// published only when the backfill sweep has completed AND the persisted
    /// overlap audit passed (`ClickHouseClient::promote_open_v2_reader`), so
    /// gating on `core_indexes` alone would let listing read indexes the
    /// `open` path refuses — including a store whose audit is failing. Both
    /// cutovers read `mcp_session_directory` / `mcp_event_navigation`; one
    /// readiness authority, one verdict.
    ///
    /// The verdict is latched on the first SUCCESSFUL probe, for this
    /// repository and every clone of it — one instance per routed backend, so
    /// in practice one point read per backend per process.
    ///
    /// A transport failure is deliberately NOT latched: it degrades to the
    /// header path and re-probes on the next request. That diverges from the
    /// `open` cutover, where [`crate::probe_open_v2_ready`] collapses an
    /// unreachable backend to `false` and the caller's `OnceCell` latches it
    /// for the process. The divergence is intentional and one-way-safe: that
    /// probe runs once at startup and cannot retry, while this one is already
    /// on a request path that can. Both latch a genuine `false`, so neither
    /// gate can flap open and shut mid-process.
    pub(super) async fn canonical_list_path_ready(&self) -> bool {
        if let Some(ready) = self.canonical_list_ready.get() {
            return *ready;
        }
        match self.ch.open_v2_reader_ready().await {
            Ok(ready) => {
                // Latch ONLY the positive. Readiness is monotonic once
                // published, so caching `true` is safe and keeps the flip
                // one-way for the process. Caching `false` is not the same
                // thing: readiness becomes true when the backfill publishes,
                // and a latched negative would pin every reader to the
                // fallback — and the monitor's page route to a hard 503 —
                // until the daemon is restarted. While not ready this costs
                // one point-read of `mcp_read_index_state` per call.
                if ready {
                    let _ = self.canonical_list_ready.set(true);
                }
                ready
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "open_v2 reader readiness probe failed; session listing stays on the projected-header path"
                );
                false
            }
        }
    }

    pub(super) fn table_ref(&self, table: &str) -> String {
        format!(
            "{}.{}",
            sql_identifier(&self.ch.config().database),
            sql_identifier(table)
        )
    }

    pub(super) async fn query_rows<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> AnyResult<Vec<T>> {
        // Query ids, deadlines, and budget settings are injected by the
        // transport from the active QueryEnvelope; the repository only adds
        // its read-semantics settings.
        self.ch
            .query_rows_with_params(query, database, &REPOSITORY_READ_SETTINGS)
            .await
    }

    pub(super) async fn query_rows_with_params<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
        params: &[(&str, &str)],
    ) -> AnyResult<Vec<T>> {
        // The transport owns the budget parameters: it strips
        // caller-supplied envelope keys and min-merges any caller
        // max_execution_time (preserving file_attention's tighter
        // per-statement cap); without an active envelope the statement is
        // refused before it reaches the server.
        let mut request_params = Vec::with_capacity(params.len() + REPOSITORY_READ_SETTINGS.len());
        request_params.extend_from_slice(params);
        request_params.extend_from_slice(&REPOSITORY_READ_SETTINGS);
        self.ch
            .query_rows_with_params(query, database, &request_params)
            .await
    }

    pub(super) fn map_backend<T>(&self, result: AnyResult<T>) -> RepoResult<T> {
        result.map_err(classify_backend_error)
    }
}

/// Classify a transport failure at the repository boundary (amendment A11):
/// budget-shaped failures — local envelope admission refusals and server
/// timeout/kill/memory/rows-or-bytes errors — become the typed
/// [`RepoError::DeadlineExceeded`] / [`RepoError::ResourceExhausted`]
/// variants; everything else keeps the historical opaque `Backend` mapping.
///
/// Scope/auth/not-found outcomes never travel through this function: they
/// are `Ok(None)`/empty results upstream, so this classification cannot
/// reshape them by construction.
pub(super) fn classify_backend_error(error: anyhow::Error) -> RepoError {
    match envelope_error_kind(&error) {
        Some(ClickHouseErrorKind::DeadlineExceeded) | Some(ClickHouseErrorKind::QueryKilled) => {
            RepoError::deadline_exceeded(budget_note(&error))
        }
        Some(ClickHouseErrorKind::ResourceExhausted) => {
            RepoError::resource_exhausted(budget_note(&error))
        }
        _ => RepoError::backend(format!("{error:#}")),
    }
}

/// Human-readable budget context for a typed budget failure: the envelope's
/// own admission error when the statement was refused locally, else the
/// server exception code annotated with the active envelope's class and
/// request id when one is available.
fn budget_note(error: &anyhow::Error) -> String {
    for cause in error.chain() {
        if let Some(envelope_error) = cause.downcast_ref::<EnvelopeError>() {
            return envelope_error.to_string();
        }
        if let Some(http_error) = cause.downcast_ref::<ClickHouseHttpError>() {
            let code = http_error
                .code()
                .map_or_else(|| "unknown".to_string(), |code| code.to_string());
            return match QueryEnvelope::current() {
                Ok(envelope) => format!(
                    "clickhouse code {code} under {} budget (request {})",
                    envelope.class().as_str(),
                    envelope.request_id()
                ),
                Err(_) => format!("clickhouse code {code}"),
            };
        }
    }
    format!("{error:#}")
}
