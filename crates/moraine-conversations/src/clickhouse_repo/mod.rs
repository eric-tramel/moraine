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
}

impl ActiveMcpQueryId {
    fn new(base: String) -> Self {
        Self {
            base: base.into(),
            sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}-{sequence}", self.base)
    }
}

tokio::task_local! {
    static ACTIVE_MCP_QUERY_ID: ActiveMcpQueryId;
}

pub async fn with_repository_query_id<F>(query_id: String, future: F) -> F::Output
where
    F: Future,
{
    ACTIVE_MCP_QUERY_ID
        .scope(ActiveMcpQueryId::new(query_id), future)
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
        if let Ok(query_id) = ACTIVE_MCP_QUERY_ID.try_with(ActiveMcpQueryId::next) {
            let params = [("query_id", query_id.as_str()), REPOSITORY_READ_SETTINGS[0]];
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
        let query_id = ACTIVE_MCP_QUERY_ID.try_with(ActiveMcpQueryId::next).ok();
        let has_query_id = params.iter().any(|(name, _)| *name == "query_id");
        let mut request_params = Vec::with_capacity(
            params.len() + REPOSITORY_READ_SETTINGS.len() + usize::from(!has_query_id),
        );
        request_params.extend_from_slice(params);
        if !has_query_id {
            if let Some(query_id) = query_id.as_deref() {
                request_params.push(("query_id", query_id));
            }
        }
        request_params.extend_from_slice(&REPOSITORY_READ_SETTINGS);
        self.ch
            .query_rows_with_params(query, database, &request_params)
            .await
    }

    pub(super) fn map_backend<T>(&self, result: AnyResult<T>) -> RepoResult<T> {
        result.map_err(|err| RepoError::backend(err.to_string()))
    }
}
