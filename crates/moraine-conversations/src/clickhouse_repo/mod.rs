use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::AHashMap as HashMap;
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use moraine_clickhouse::ClickHouseClient;
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::warn;
use uuid::Uuid;

const REPOSITORY_READ_SETTINGS: [(&str, &str); 1] =
    [("do_not_merge_across_partitions_select_final", "0")];

#[derive(Clone)]
struct ActiveMcpQueryId {
    base: Arc<str>,
    sequence: Arc<AtomicU64>,
    deadline: Option<Instant>,
}

impl ActiveMcpQueryId {
    fn new(base: String, deadline: Option<Instant>) -> Self {
        Self {
            base: base.into(),
            sequence: Arc::new(AtomicU64::new(0)),
            deadline,
        }
    }

    fn next(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}-{sequence}", self.base)
    }

    fn remaining_execution_seconds(&self) -> Option<String> {
        self.deadline.map(|deadline| {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| Duration::from_millis(1));
            format!("{:.3}", remaining.as_secs_f64().max(0.001))
        })
    }
}

tokio::task_local! {
    static ACTIVE_MCP_QUERY_ID: ActiveMcpQueryId;
}

pub async fn with_repository_query_id<F>(query_id: String, future: F) -> F::Output
where
    F: Future,
{
    let inherited_deadline = ACTIVE_MCP_QUERY_ID
        .try_with(|context| context.deadline)
        .ok()
        .flatten();
    ACTIVE_MCP_QUERY_ID
        .scope(ActiveMcpQueryId::new(query_id, inherited_deadline), future)
        .await
}

pub async fn with_repository_query_deadline<F>(
    query_id: String,
    deadline: Instant,
    future: F,
) -> F::Output
where
    F: Future,
{
    ACTIVE_MCP_QUERY_ID
        .scope(ActiveMcpQueryId::new(query_id, Some(deadline)), future)
        .await
}

use crate::cursor::{
    decode_cursor, encode_cursor, ConversationCursor, McpSessionListCursor, SessionEventCursor,
    TurnCursor,
};
use crate::domain::{
    is_user_facing_content_event, AnalyticsConcurrencyPoint, AnalyticsRange, AnalyticsSnapshot,
    AnalyticsTokenPoint, AnalyticsTurnPoint, AnalyticsWindow, Conversation,
    ConversationDetailOptions, ConversationListFilter, ConversationListSort, ConversationMode,
    ConversationSearchHit, ConversationSearchQuery, ConversationSearchResults,
    ConversationSearchStats, ConversationSummary, FileAttentionQuery, FileAttentionTouch,
    McpEventOpen, McpEventRef, McpEventSummary, McpEventType, McpSessionListFilter,
    McpSessionListItem, McpSessionOpen, McpTurnCompact, McpTurnOpen, McpTurnRef, OpenContext,
    OpenEvent, OpenEventRequest, Page, PageRequest, RepoConfig, SearchEventHit, SearchEventKind,
    SearchEventsQuery, SearchEventsResult, SearchEventsStats, SearchMcpEventHit,
    SearchMcpEventsQuery, SearchMcpEventsResult, SearchMcpEventsStats, SearchStrategyHint,
    SessionAnalytics, SessionAnalyticsQuery, SessionEventsDirection, SessionEventsQuery,
    SessionMetadata, SessionMetadataSearchHit, SessionMetadataSearchQuery,
    SessionMetadataSearchResults, SessionMetadataSearchStats, SessionOriginScope, SessionStep,
    SessionTurn, ToolResult, TraceEvent, Turn, TurnListFilter, TurnSummary, WebSearchEvent,
};
use crate::error::{RepoError, RepoResult};
use crate::repo::ConversationRepository;

mod analytics;
mod cache;
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
mod sql;

#[cfg(test)]
mod tests;

use cache::*;
use helpers::*;
use rows::*;
use sql::*;

#[derive(Clone)]
pub struct ClickHouseConversationRepository {
    ch: ClickHouseClient,
    cfg: RepoConfig,
    stats_cache: Arc<RwLock<SearchStatsCache>>,
    search_cache: Arc<RwLock<HashMap<String, SearchEventsCacheEntry>>>,
    mcp_search_cache: Arc<RwLock<HashMap<String, SearchMcpEventsCacheEntry>>>,
    term_postings_cache: Arc<RwLock<HashMap<String, TermPostingsCacheEntry>>>,
    search_doc_extra_cache: Arc<RwLock<HashMap<String, SearchDocExtraCacheEntry>>>,
    analytics_cache: Arc<[Mutex<Option<AnalyticsCacheEntry>>; ANALYTICS_RANGE_COUNT]>,
    /// Sessions already proven to fall inside `cfg.session_scope`. A session's
    /// origin directory is its first recorded cwd and never changes, so
    /// positive results are cacheable forever. Negative results are NOT
    /// cached: a freshly started session may not have ingested its first
    /// cwd-bearing event yet.
    scoped_session_cache: Arc<RwLock<std::collections::HashSet<String>>>,
}

impl ClickHouseConversationRepository {
    pub fn new(ch: ClickHouseClient, cfg: RepoConfig) -> Self {
        Self {
            ch,
            cfg,
            stats_cache: Arc::new(RwLock::new(SearchStatsCache::default())),
            search_cache: Arc::new(RwLock::new(HashMap::new())),
            mcp_search_cache: Arc::new(RwLock::new(HashMap::new())),
            term_postings_cache: Arc::new(RwLock::new(HashMap::new())),
            search_doc_extra_cache: Arc::new(RwLock::new(HashMap::new())),
            analytics_cache: Arc::new(std::array::from_fn(|_| Mutex::new(None))),
            scoped_session_cache: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }

    pub fn config(&self) -> &RepoConfig {
        &self.cfg
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
        if let Ok((query_id, remaining)) = ACTIVE_MCP_QUERY_ID
            .try_with(|context| (context.next(), context.remaining_execution_seconds()))
        {
            let mut params = vec![("query_id", query_id.as_str())];
            if let Some(remaining) = remaining.as_deref() {
                params.push(("max_execution_time", remaining));
                params.push(("timeout_overflow_mode", "throw"));
            }
            params.extend_from_slice(&REPOSITORY_READ_SETTINGS);
            self.ch
                .query_rows_with_params(query, database, &params)
                .await
        } else {
            self.ch
                .query_rows_with_params(query, database, &REPOSITORY_READ_SETTINGS)
                .await
        }
    }

    pub(super) async fn query_rows_with_params<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
        params: &[(&str, &str)],
    ) -> AnyResult<Vec<T>> {
        let context = ACTIVE_MCP_QUERY_ID
            .try_with(|context| (context.next(), context.remaining_execution_seconds()))
            .ok();
        let query_id = context.as_ref().map(|(query_id, _)| query_id.as_str());
        let remaining = context
            .as_ref()
            .and_then(|(_, remaining)| remaining.as_deref());
        let has_query_id = params.iter().any(|(name, _)| *name == "query_id");
        let effective_execution_time = remaining.map(|remaining| {
            let remaining = remaining.parse::<f64>().unwrap_or(0.001).max(0.001);
            let caller_limit = params
                .iter()
                .find_map(|(name, value)| (*name == "max_execution_time").then_some(*value))
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| value.is_finite() && *value > 0.0);
            format!(
                "{:.3}",
                caller_limit
                    .map_or(remaining, |limit| limit.min(remaining))
                    .max(0.001)
            )
        });
        let enforce_deadline = effective_execution_time.is_some();
        let mut request_params = Vec::with_capacity(
            params.len()
                + REPOSITORY_READ_SETTINGS.len()
                + usize::from(!has_query_id && query_id.is_some())
                + 2 * usize::from(enforce_deadline),
        );
        request_params.extend(params.iter().copied().filter(|(name, _)| {
            !enforce_deadline || (*name != "max_execution_time" && *name != "timeout_overflow_mode")
        }));
        if !has_query_id {
            if let Some(query_id) = query_id {
                request_params.push(("query_id", query_id));
            }
        }
        if let Some(effective_execution_time) = effective_execution_time.as_deref() {
            request_params.push(("max_execution_time", effective_execution_time));
            request_params.push(("timeout_overflow_mode", "throw"));
        }
        request_params.extend_from_slice(&REPOSITORY_READ_SETTINGS);
        self.ch
            .query_rows_with_params(query, database, &request_params)
            .await
    }

    pub(super) fn map_backend<T>(&self, result: AnyResult<T>) -> RepoResult<T> {
        result.map_err(|err| RepoError::backend(format!("{err:#}")))
    }
}
