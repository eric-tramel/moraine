use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ahash::AHashMap as HashMap;
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use moraine_clickhouse::{
    ClickHouseClient, ClickHouseError, ClickHouseErrorCategory, QueryOwner, QueryWorkload,
};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::warn;
use uuid::Uuid;

const REPOSITORY_READ_SETTINGS: [(&str, &str); 1] =
    [("do_not_merge_across_partitions_select_final", "0")];

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
mod canonical_open;
mod consistency;
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
    term_postings_cache: Arc<RwLock<HashMap<String, TermPostingsCacheEntry>>>,
    search_doc_extra_cache: Arc<RwLock<HashMap<String, SearchDocExtraCacheEntry>>>,
    analytics_cache: Arc<[Mutex<Option<AnalyticsCacheEntry>>; ANALYTICS_RANGE_COUNT]>,
    /// Sessions already proven to fall inside `cfg.session_scope`, mapped to
    /// the publication token under which the proof was obtained. A new proof
    /// replaces the prior revision for the same session; negative results are
    /// never cached.
    scoped_session_cache: Arc<RwLock<HashMap<String, String>>>,
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
            term_postings_cache: Arc::new(RwLock::new(HashMap::new())),
            search_doc_extra_cache: Arc::new(RwLock::new(HashMap::new())),
            analytics_cache: Arc::new(std::array::from_fn(|_| Mutex::new(None))),
            scoped_session_cache: Arc::new(RwLock::new(HashMap::new())),
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
        let mut request_params = Vec::with_capacity(params.len() + REPOSITORY_READ_SETTINGS.len());
        request_params.extend_from_slice(params);
        request_params.extend_from_slice(&REPOSITORY_READ_SETTINGS);
        self.ch
            .query_rows_with_params(query, database, &request_params)
            .await
    }

    pub(super) fn map_backend<T>(&self, result: AnyResult<T>) -> RepoResult<T> {
        result.map_err(map_clickhouse_error)
    }
}

fn map_clickhouse_error(error: anyhow::Error) -> RepoError {
    let message = format!("{error:#}");
    let category = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ClickHouseError>())
        .map(ClickHouseError::category);
    match category {
        Some(ClickHouseErrorCategory::Cancelled) => RepoError::cancelled(message),
        Some(ClickHouseErrorCategory::DeadlineExceeded) => RepoError::deadline_exceeded(message),
        Some(ClickHouseErrorCategory::Busy) => RepoError::busy(message),
        Some(ClickHouseErrorCategory::ResourceExhausted) => RepoError::resource_exhausted(message),
        Some(ClickHouseErrorCategory::Backend | ClickHouseErrorCategory::OwnershipViolation)
        | None => RepoError::backend(message),
    }
}
