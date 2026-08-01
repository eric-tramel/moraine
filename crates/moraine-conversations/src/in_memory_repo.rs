use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;

use crate::domain::{
    AnalyticsRange, AnalyticsSnapshot, AnalyticsWindow, CanonicalContinuation,
    CanonicalReadOutcome, CanonicalSessionPage, IngestHeartbeatRead, StoreDiagnostics, StoreHealth,
    TablePreview, TablePreviewQuery, TableSummaries, WebSearchEvent,
};
use crate::domain::{
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationMode,
    ConversationSearchQuery, ConversationSearchResults, ConversationSearchStats,
    ConversationSummary, FileAttentionQuery, FileAttentionTouch, McpEventType,
    McpSessionListFilter, McpSessionListItem, OpenContext, OpenEventRequest, Page, PageRequest,
    RepoConfig, SearchEventsQuery, SearchEventsResult, SearchEventsStats, SearchMcpEventsQuery,
    SearchMcpEventsResult, SearchMcpEventsStats, SessionEventsQuery, SessionMetadata,
    SessionSearchQuery, SessionSearchResults, TraceEvent, Turn, TurnListFilter, TurnSummary,
};
use crate::error::{RepoError, RepoResult};
use crate::repo::ConversationRepository;

/// One programmed outcome of [`ConversationRepository::canonical_open_session_page`].
pub type CanonicalSessionPageResponse =
    RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>>;

/// Configurable responses returned by [`InMemoryConversationRepository`].
///
/// An unset response uses the fake's empty or not-found default. Tests can set
/// an `Err` response to exercise repository failure paths without a live
/// ClickHouse server.
#[derive(Debug, Clone, Default)]
pub struct InMemoryConversationResponses {
    pub list_conversations: Option<RepoResult<Page<ConversationSummary>>>,
    pub get_conversation: Option<RepoResult<Option<Conversation>>>,
    pub get_session_metadata: Option<RepoResult<Option<SessionMetadata>>>,
    pub list_mcp_sessions: Option<RepoResult<Page<McpSessionListItem>>>,
    /// A static keyset corpus: the page served for a given continuation token,
    /// with `""` keying the uncursored first page. Takes precedence over
    /// [`Self::list_mcp_sessions`], so a caller that drops the cursor is served
    /// page 1 again and the overlap is visible to the test.
    pub list_mcp_sessions_by_cursor: BTreeMap<String, RepoResult<Page<McpSessionListItem>>>,
    pub search_session_summaries: Option<RepoResult<SessionSearchResults>>,
    pub canonical_open_session_page: Option<CanonicalSessionPageResponse>,
    /// A static session traversal: the page served after a given
    /// `after_turn_seq`, with `0` keying the uncursored first page. Takes
    /// precedence over [`Self::canonical_open_session_page`].
    pub canonical_open_session_page_after_turn: BTreeMap<u32, CanonicalSessionPageResponse>,
    /// The `open_v2` verdict the fake reports to
    /// [`ConversationRepository::canonical_reader_ready`]. Unset reads as
    /// ready: the fake serves programmed canonical pages, so a consumer test
    /// exercises the reader unless it deliberately asks for a not-ready
    /// backend.
    pub canonical_reader_ready: Option<bool>,
    pub list_turns: Option<RepoResult<Page<TurnSummary>>>,
    pub get_turn: Option<RepoResult<Option<Turn>>>,
    pub open_event: Option<RepoResult<OpenContext>>,
    pub list_session_events: Option<RepoResult<Page<TraceEvent>>>,
    pub search_events: Option<RepoResult<SearchEventsResult>>,
    pub search_mcp_events: Option<RepoResult<SearchMcpEventsResult>>,
    pub search_conversations: Option<RepoResult<ConversationSearchResults>>,
    pub file_attention: Option<RepoResult<Vec<FileAttentionTouch>>>,
    pub prewarm_mcp_search_state: Option<RepoResult<()>>,
    pub cancel_query: Option<RepoResult<()>>,
    pub analytics_series: Option<RepoResult<AnalyticsSnapshot>>,
    pub list_web_searches: Option<RepoResult<Vec<WebSearchEvent>>>,
    pub latest_ingest_heartbeat: Option<RepoResult<IngestHeartbeatRead>>,
    pub list_table_summaries: Option<RepoResult<TableSummaries>>,
    pub preview_table: Option<RepoResult<TablePreview>>,
    pub read_store_health: Option<RepoResult<StoreHealth>>,
    pub read_store_diagnostics: Option<RepoResult<StoreDiagnostics>>,
}

/// Arguments observed by an [`InMemoryConversationRepository`].
#[derive(Debug, Clone, Default)]
pub struct InMemoryConversationCalls {
    pub list_conversations: Vec<(ConversationListFilter, PageRequest)>,
    pub get_conversation: Vec<(String, ConversationDetailOptions)>,
    pub get_session_metadata: Vec<String>,
    pub list_mcp_sessions: Vec<(McpSessionListFilter, PageRequest)>,
    pub search_session_summaries: Vec<SessionSearchQuery>,
    pub canonical_open_session_page: Vec<(String, u16, Option<CanonicalContinuation>)>,
    pub list_turns: Vec<(String, TurnListFilter, PageRequest)>,
    pub get_turn: Vec<(String, u32)>,
    pub open_event: Vec<OpenEventRequest>,
    pub list_session_events: Vec<(SessionEventsQuery, PageRequest)>,
    pub search_events: Vec<SearchEventsQuery>,
    pub search_mcp_events: Vec<SearchMcpEventsQuery>,
    pub search_conversations: Vec<ConversationSearchQuery>,
    pub file_attention: Vec<FileAttentionQuery>,
    pub prewarm_mcp_search_state: usize,
    pub cancel_query: Vec<String>,
    pub analytics_series: Vec<AnalyticsRange>,
    pub list_web_searches: Vec<u16>,
    pub latest_ingest_heartbeat: usize,
    pub list_table_summaries: usize,
    pub preview_table: Vec<TablePreviewQuery>,
    pub read_store_health: usize,
    pub read_store_diagnostics: usize,
}

/// In-memory, programmable implementation of [`ConversationRepository`].
///
/// This fake is exported from `moraine-conversations` so downstream crates can
/// exercise repository consumers without mocking the ClickHouse HTTP wire.
#[derive(Debug)]
pub struct InMemoryConversationRepository {
    config: RepoConfig,
    responses: InMemoryConversationResponses,
    calls: Mutex<InMemoryConversationCalls>,
    /// `list_sessions` token -> the filter signature it was minted under. The
    /// real repository carries this inside the token; the fake keeps it beside
    /// the token so it can refuse the same way (see
    /// [`session_list_filter_sig`]).
    minted_session_cursors: Mutex<BTreeMap<String, String>>,
}

impl InMemoryConversationRepository {
    pub fn new(config: RepoConfig) -> Self {
        Self::with_responses(config, InMemoryConversationResponses::default())
    }

    pub fn with_responses(config: RepoConfig, responses: InMemoryConversationResponses) -> Self {
        Self {
            config,
            responses,
            calls: Mutex::new(InMemoryConversationCalls::default()),
            minted_session_cursors: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn calls(&self) -> InMemoryConversationCalls {
        mutex_lock(&self.calls).clone()
    }

    fn record(&self, update: impl FnOnce(&mut InMemoryConversationCalls)) {
        update(&mut mutex_lock(&self.calls));
    }

    fn empty_search_events(&self, query: &SearchEventsQuery) -> SearchEventsResult {
        let requested_limit = query.limit.unwrap_or(self.config.max_results).max(1);
        SearchEventsResult {
            query_id: "in-memory".to_string(),
            query: query.query.clone(),
            terms: Vec::new(),
            stats: SearchEventsStats {
                docs: 0,
                avgdl: 0.0,
                took_ms: 0,
                result_count: 0,
                requested_limit,
                effective_limit: requested_limit.min(self.config.max_results),
                limit_capped: requested_limit > self.config.max_results,
            },
            hits: Vec::new(),
        }
    }

    fn empty_search_mcp_events(&self, query: &SearchMcpEventsQuery) -> SearchMcpEventsResult {
        let requested_n_hits = query.n_hits.unwrap_or(self.config.max_results).max(1);
        SearchMcpEventsResult {
            query_id: query
                .cancellation_token
                .clone()
                .unwrap_or_else(|| "in-memory".to_string()),
            query: query.query.clone(),
            terms: Vec::new(),
            event_types: query
                .event_types
                .clone()
                .unwrap_or_else(McpEventType::default_search_types),
            // The default fake contains no sessions or turns. Unscoped empty
            // search is valid; a scoped lookup therefore reports not found.
            scope_exists: query.session_id.is_none(),
            truncated: false,
            incomplete_due_to_candidate_budget: false,
            stats: SearchMcpEventsStats {
                docs: 0,
                avgdl: 0.0,
                took_ms: 0,
                result_count: 0,
                requested_n_hits,
                effective_n_hits: requested_n_hits.min(self.config.max_results),
                limit_capped: requested_n_hits > self.config.max_results,
                truncated: false,
            },
            hits: Vec::new(),
        }
    }

    fn empty_conversation_search(
        &self,
        query: &ConversationSearchQuery,
    ) -> ConversationSearchResults {
        let requested_limit = query.limit.unwrap_or(self.config.max_results).max(1);
        ConversationSearchResults {
            query_id: "in-memory".to_string(),
            query: query.query.clone(),
            terms: Vec::new(),
            stats: ConversationSearchStats {
                docs: 0,
                avgdl: 0.0,
                took_ms: 0,
                result_count: 0,
                requested_limit,
                effective_limit: requested_limit.min(self.config.max_results),
                limit_capped: requested_limit > self.config.max_results,
            },
            hits: Vec::new(),
        }
    }
}

impl Default for InMemoryConversationRepository {
    fn default() -> Self {
        Self::new(RepoConfig::default())
    }
}

macro_rules! response_or {
    ($self:expr, $field:ident, $default:expr) => {{
        match &$self.responses.$field {
            Some(result) => result.clone(),
            None => Ok($default),
        }
    }};
}

#[async_trait]
impl ConversationRepository for InMemoryConversationRepository {
    fn config(&self) -> &RepoConfig {
        &self.config
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        self.record(|calls| calls.prewarm_mcp_search_state += 1);
        response_or!(self, prewarm_mcp_search_state, ())
    }
    async fn analytics_series(&self, range: AnalyticsRange) -> RepoResult<AnalyticsSnapshot> {
        self.record(|calls| calls.analytics_series.push(range));
        response_or!(
            self,
            analytics_series,
            AnalyticsSnapshot {
                window: AnalyticsWindow {
                    range,
                    window_seconds: range.window_seconds(),
                    bucket_seconds: range.bucket_seconds(),
                    from_unix: 0,
                    to_unix: 0,
                },
                tokens: Vec::new(),
                turns: Vec::new(),
                concurrent_sessions: Vec::new(),
            }
        )
    }

    async fn list_web_searches(&self, limit: u16) -> RepoResult<Vec<WebSearchEvent>> {
        self.record(|calls| calls.list_web_searches.push(limit));
        response_or!(self, list_web_searches, Vec::new())
    }

    async fn latest_ingest_heartbeat(&self) -> RepoResult<IngestHeartbeatRead> {
        self.record(|calls| calls.latest_ingest_heartbeat += 1);
        response_or!(
            self,
            latest_ingest_heartbeat,
            IngestHeartbeatRead::default()
        )
    }

    async fn list_table_summaries(&self) -> RepoResult<TableSummaries> {
        self.record(|calls| calls.list_table_summaries += 1);
        response_or!(self, list_table_summaries, TableSummaries::default())
    }

    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview> {
        self.record(|calls| calls.preview_table.push(query.clone()));
        let normalized_limit = query.normalized_limit();
        response_or!(
            self,
            preview_table,
            TablePreview {
                table: query.table,
                limit: normalized_limit,
                schema: Vec::new(),
                rows: Vec::new(),
            }
        )
    }

    async fn read_store_health(&self) -> RepoResult<StoreHealth> {
        self.record(|calls| calls.read_store_health += 1);
        response_or!(self, read_store_health, StoreHealth::default())
    }

    async fn read_store_diagnostics(&self) -> RepoResult<StoreDiagnostics> {
        self.record(|calls| calls.read_store_diagnostics += 1);
        response_or!(self, read_store_diagnostics, StoreDiagnostics::default())
    }

    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<ConversationSummary>> {
        self.record(|calls| calls.list_conversations.push((filter, page)));
        response_or!(
            self,
            list_conversations,
            Page {
                items: Vec::new(),
                next_cursor: None,
            }
        )
    }

    async fn get_conversation(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        self.record(|calls| calls.get_conversation.push((session_id.to_string(), opts)));
        response_or!(self, get_conversation, None)
    }

    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>> {
        self.record(|calls| calls.get_session_metadata.push(session_id.to_string()));
        response_or!(self, get_session_metadata, None)
    }

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        let filter_sig = session_list_filter_sig(&filter);
        let cursor = page.cursor.clone().unwrap_or_default();
        self.record(|calls| calls.list_mcp_sessions.push((filter, page)));

        // The real repository's cursor contract, modeled: a token is bound to
        // the filter that minted it and is refused under any other. A fake that
        // dispatched on the token string alone could not fail a caller that
        // re-derives its filter between pages, so no unit test guarding that
        // could ever be red.
        let minted_sig = mutex_lock(&self.minted_session_cursors)
            .get(&cursor)
            .cloned();
        if minted_sig.is_some_and(|minted| minted != filter_sig) {
            return Err(RepoError::invalid_cursor(
                "cursor does not match current list_sessions filter",
            ));
        }

        let result = match self.responses.list_mcp_sessions_by_cursor.get(&cursor) {
            Some(keyed) => keyed.clone(),
            None => response_or!(
                self,
                list_mcp_sessions,
                Page {
                    items: Vec::new(),
                    next_cursor: None,
                }
            ),
        };
        if let Ok(Page {
            next_cursor: Some(next),
            ..
        }) = &result
        {
            mutex_lock(&self.minted_session_cursors).insert(next.clone(), filter_sig);
        }
        result
    }

    async fn search_session_summaries(
        &self,
        query: SessionSearchQuery,
    ) -> RepoResult<SessionSearchResults> {
        self.record(|calls| calls.search_session_summaries.push(query.clone()));
        response_or!(
            self,
            search_session_summaries,
            SessionSearchResults {
                query_id: query
                    .cancellation_token
                    .clone()
                    .unwrap_or_else(|| "in-memory".to_string()),
                query: query.query.clone(),
                terms: Vec::new(),
                sessions: Vec::new(),
                truncated: false,
                hits_truncated: false,
                incomplete: false,
                dropped: false,
            }
        )
    }

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        self.record(|calls| {
            calls
                .list_turns
                .push((session_id.to_string(), filter, page))
        });
        response_or!(
            self,
            list_turns,
            Page {
                items: Vec::new(),
                next_cursor: None,
            }
        )
    }

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        self.record(|calls| calls.get_turn.push((session_id.to_string(), turn_seq)));
        response_or!(self, get_turn, None)
    }

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        self.record(|calls| calls.open_event.push(req.clone()));
        response_or!(
            self,
            open_event,
            OpenContext {
                found: false,
                event_uid: req.event_uid,
                session_id: String::new(),
                target_event_order: 0,
                turn_seq: 0,
                before: req.before.unwrap_or(self.config.default_context_before),
                after: req.after.unwrap_or(self.config.default_context_after),
                events: Vec::new(),
            }
        )
    }

    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        self.record(|calls| calls.list_session_events.push((query, page)));
        response_or!(
            self,
            list_session_events,
            Page {
                items: Vec::new(),
                next_cursor: None,
            }
        )
    }

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        self.record(|calls| calls.search_events.push(query.clone()));
        response_or!(self, search_events, self.empty_search_events(&query))
    }

    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        self.record(|calls| calls.search_mcp_events.push(query.clone()));
        response_or!(
            self,
            search_mcp_events,
            self.empty_search_mcp_events(&query)
        )
    }

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        self.record(|calls| calls.search_conversations.push(query.clone()));
        response_or!(
            self,
            search_conversations,
            self.empty_conversation_search(&query)
        )
    }

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        self.record(|calls| calls.file_attention.push(query));
        response_or!(self, file_attention, Vec::new())
    }

    async fn cancel_query(&self, query_id: &str) -> RepoResult<()> {
        self.record(|calls| calls.cancel_query.push(query_id.to_string()));
        response_or!(self, cancel_query, ())
    }

    async fn canonical_reader_ready(&self) -> bool {
        self.responses.canonical_reader_ready.unwrap_or(true)
    }

    async fn canonical_open_session_page(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> CanonicalSessionPageResponse {
        let after_turn_seq = after
            .as_ref()
            .map(|continuation| continuation.after_turn_seq)
            .unwrap_or_default();
        self.record(|calls| {
            calls
                .canonical_open_session_page
                .push((session_id.to_string(), limit, after))
        });
        if let Some(keyed) = self
            .responses
            .canonical_open_session_page_after_turn
            .get(&after_turn_seq)
        {
            return keyed.clone();
        }
        // Unprogrammed, the fake keeps the trait's typed failure instead of
        // answering `Ok(None)`: a consumer wired to a repository with no v2
        // reader must see that, not an empty session.
        match &self.responses.canonical_open_session_page {
            Some(result) => result.clone(),
            None => Err(RepoError::backend(
                "canonical v2 open reader is not available on this repository",
            )),
        }
    }
}

/// The `list_sessions` filter dimensions a continuation token is bound to.
///
/// `ClickHouseConversationRepository::mcp_session_list_filter_sig` builds the
/// same JSON tuple and embeds it in the token it mints. The fake reproduces the
/// query dimensions; it has no configured `session_scope`, which is the one
/// term it cannot mirror.
fn session_list_filter_sig(filter: &McpSessionListFilter) -> String {
    serde_json::to_string(&(
        filter.start_unix_ms,
        filter.end_unix_ms,
        filter.mode.map(ConversationMode::as_str),
        filter.harness.as_deref(),
        filter.source_name.as_deref(),
        filter.sort.as_str(),
    ))
    .expect("list_sessions filter contains only serializable primitives")
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::domain::{
        AnalyticsRange, AnalyticsSnapshot, AnalyticsWindow, CoreIndexHealth, IngestHeartbeatRead,
        PublicationDiagnostics, StoreConnectionMetrics, StoreDiagnostics, StoreHealth, StoreProbe,
        TablePreview, TablePreviewQuery, TableSummaries, WebSearchEvent,
    };
    use crate::error::RepoError;

    use super::{
        ConversationRepository, InMemoryConversationRepository, InMemoryConversationResponses,
    };

    #[tokio::test]
    async fn a_list_sessions_cursor_is_refused_when_the_filter_that_minted_it_moved() {
        use crate::domain::{ConversationListSort, McpSessionListFilter, Page, PageRequest};
        use std::collections::BTreeMap;

        // The real repository embeds the filter signature in the token and
        // refuses a mismatch. The fake must be able to refuse the same way, or
        // no consumer test could ever exhibit a caller that re-derives its
        // filter between pages.
        let filter = |start_unix_ms: i64| McpSessionListFilter {
            start_unix_ms,
            end_unix_ms: 1_771_243_203_901,
            mode: None,
            sort: ConversationListSort::Desc,
            harness: None,
            source_name: None,
        };
        let repo = InMemoryConversationRepository::with_responses(
            crate::domain::RepoConfig::default(),
            InMemoryConversationResponses {
                list_mcp_sessions_by_cursor: BTreeMap::from([
                    (
                        String::new(),
                        Ok(Page {
                            items: Vec::new(),
                            next_cursor: Some("page-2".to_string()),
                        }),
                    ),
                    (
                        "page-2".to_string(),
                        Ok(Page {
                            items: Vec::new(),
                            next_cursor: None,
                        }),
                    ),
                ]),
                ..Default::default()
            },
        );

        let page_one = repo
            .list_mcp_sessions(filter(0), PageRequest::default())
            .await
            .expect("page 1");
        let cursor = page_one.next_cursor.expect("page 1 mints a cursor");

        let resumed = PageRequest {
            cursor: Some(cursor.clone()),
            ..PageRequest::default()
        };
        assert!(
            repo.list_mcp_sessions(filter(0), resumed.clone())
                .await
                .is_ok(),
            "the minting filter redeems its own token"
        );

        let error = repo
            .list_mcp_sessions(filter(1), resumed)
            .await
            .expect_err("a moved filter must refuse the token");
        assert!(
            matches!(error, RepoError::InvalidCursor(ref message)
                if message == "cursor does not match current list_sessions filter"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn analytics_contract_defaults_and_calls_work_through_trait_object() {
        let fake = Arc::new(InMemoryConversationRepository::default());
        let repo: Arc<dyn ConversationRepository> = fake.clone();
        let preview_query = TablePreviewQuery {
            table: "events".to_string(),
            limit: 0,
        };

        let snapshot = repo
            .analytics_series(AnalyticsRange::ThirtyDays)
            .await
            .expect("default analytics series");
        assert_eq!(snapshot.window.range, AnalyticsRange::ThirtyDays);
        assert_eq!(snapshot.window.window_seconds, 2_592_000);
        assert_eq!(snapshot.window.bucket_seconds, 86_400);
        assert!(snapshot.tokens.is_empty());
        assert!(repo
            .list_web_searches(0)
            .await
            .expect("default web searches")
            .is_empty());
        assert_eq!(
            repo.latest_ingest_heartbeat()
                .await
                .expect("default heartbeat"),
            Default::default()
        );
        assert_eq!(
            repo.list_table_summaries()
                .await
                .expect("default table summaries"),
            Default::default()
        );
        let preview = repo
            .preview_table(preview_query.clone())
            .await
            .expect("default table preview");
        assert_eq!(preview.table, "events");
        assert_eq!(preview.limit, 1);
        assert!(preview.schema.is_empty());
        assert!(preview.rows.is_empty());
        let health = repo
            .read_store_health()
            .await
            .expect("default store health");
        assert!(matches!(health.ping, StoreProbe::Failed { .. }));
        // The additive core-index probe (issue #598) defaults to the
        // not-configured `Failed` variant on the in-memory stub.
        assert!(matches!(health.core_index, StoreProbe::Failed { .. }));
        assert_eq!(
            repo.read_store_diagnostics()
                .await
                .expect("default store diagnostics"),
            Default::default()
        );

        let calls = fake.calls();
        assert_eq!(calls.analytics_series, vec![AnalyticsRange::ThirtyDays]);
        assert_eq!(calls.list_web_searches, vec![0]);
        assert_eq!(calls.latest_ingest_heartbeat, 1);
        assert_eq!(calls.list_table_summaries, 1);
        assert_eq!(calls.preview_table, vec![preview_query]);
        assert_eq!(calls.read_store_health, 1);
        assert_eq!(calls.read_store_diagnostics, 1);
    }

    #[tokio::test]
    async fn analytics_contract_returns_configured_successes_and_accumulates_calls() {
        let analytics = AnalyticsSnapshot {
            window: AnalyticsWindow {
                range: AnalyticsRange::OneHour,
                window_seconds: 3_600,
                bucket_seconds: 300,
                from_unix: 1,
                to_unix: 2,
            },
            tokens: Vec::new(),
            turns: Vec::new(),
            concurrent_sessions: Vec::new(),
        };
        let web_search = WebSearchEvent {
            event_time: "2026-01-01 00:00:00".to_string(),
            harness: "codex".to_string(),
            source_name: "codex-jsonl".to_string(),
            session_id: "session".to_string(),
            model: "gpt-5".to_string(),
            action: "search".to_string(),
            search_query: "rust".to_string(),
            result_url: String::new(),
            source_ref: "source".to_string(),
        };
        let heartbeat = IngestHeartbeatRead {
            table_present: true,
            latest: None,
        };
        let tables = TableSummaries {
            tables: Vec::new(),
            row_counts_error: Some("partial".to_string()),
        };
        let preview = TablePreview {
            table: "events".to_string(),
            limit: 7,
            schema: Vec::new(),
            rows: Vec::new(),
        };
        let health = StoreHealth {
            ping: StoreProbe::Available(1.5),
            version: StoreProbe::Available("25.1".to_string()),
            database_exists: StoreProbe::Available(true),
            connections: StoreProbe::Available(StoreConnectionMetrics::default()),
            publication: StoreProbe::Available(PublicationDiagnostics::default()),
            core_index: StoreProbe::Available(CoreIndexHealth {
                core_indexes_ready: true,
                open_v2_ready: false,
                open_v2_provenance: None,
                backfill_cursor_age_ms: Some(1_500),
                audit_outcome: None,
            }),
            storage: StoreProbe::Failed {
                message: "storage probe not gathered".to_string(),
            },
        };
        let diagnostics = StoreDiagnostics {
            healthy: true,
            database: "moraine".to_string(),
            ..StoreDiagnostics::default()
        };
        let responses = InMemoryConversationResponses {
            analytics_series: Some(Ok(analytics.clone())),
            list_web_searches: Some(Ok(vec![web_search.clone()])),
            latest_ingest_heartbeat: Some(Ok(heartbeat.clone())),
            list_table_summaries: Some(Ok(tables.clone())),
            preview_table: Some(Ok(preview.clone())),
            read_store_health: Some(Ok(health.clone())),
            read_store_diagnostics: Some(Ok(diagnostics.clone())),
            ..InMemoryConversationResponses::default()
        };
        let fake = Arc::new(InMemoryConversationRepository::with_responses(
            Default::default(),
            responses,
        ));
        let repo: Arc<dyn ConversationRepository> = fake.clone();
        let preview_query = TablePreviewQuery {
            table: "events".to_string(),
            limit: 7,
        };

        for _ in 0..2 {
            assert_eq!(
                repo.analytics_series(AnalyticsRange::OneHour)
                    .await
                    .expect("configured analytics"),
                analytics
            );
            assert_eq!(
                repo.list_web_searches(7)
                    .await
                    .expect("configured web searches"),
                vec![web_search.clone()]
            );
            assert_eq!(
                repo.latest_ingest_heartbeat()
                    .await
                    .expect("configured heartbeat"),
                heartbeat
            );
            assert_eq!(
                repo.list_table_summaries()
                    .await
                    .expect("configured table summaries"),
                tables
            );
            assert_eq!(
                repo.preview_table(preview_query.clone())
                    .await
                    .expect("configured preview"),
                preview
            );
            assert_eq!(
                repo.read_store_health().await.expect("configured health"),
                health
            );
            assert_eq!(
                repo.read_store_diagnostics()
                    .await
                    .expect("configured diagnostics"),
                diagnostics
            );
        }

        let calls = fake.calls();
        assert_eq!(
            calls.analytics_series,
            vec![AnalyticsRange::OneHour, AnalyticsRange::OneHour]
        );
        assert_eq!(calls.list_web_searches, vec![7, 7]);
        assert_eq!(calls.latest_ingest_heartbeat, 2);
        assert_eq!(calls.list_table_summaries, 2);
        assert_eq!(
            calls.preview_table,
            vec![preview_query.clone(), preview_query]
        );
        assert_eq!(calls.read_store_health, 2);
        assert_eq!(calls.read_store_diagnostics, 2);
    }

    #[tokio::test]
    async fn analytics_contract_returns_each_configured_error_through_trait_object() {
        let responses = InMemoryConversationResponses {
            analytics_series: Some(Err(RepoError::backend("analytics"))),
            list_web_searches: Some(Err(RepoError::backend("web"))),
            latest_ingest_heartbeat: Some(Err(RepoError::backend("heartbeat"))),
            list_table_summaries: Some(Err(RepoError::backend("tables"))),
            preview_table: Some(Err(RepoError::backend("preview"))),
            read_store_health: Some(Err(RepoError::backend("health"))),
            read_store_diagnostics: Some(Err(RepoError::backend("diagnostics"))),
            ..InMemoryConversationResponses::default()
        };
        let repo: Arc<dyn ConversationRepository> = Arc::new(
            InMemoryConversationRepository::with_responses(Default::default(), responses),
        );

        assert!(matches!(
            repo.analytics_series(AnalyticsRange::default()).await,
            Err(RepoError::Backend(message)) if message == "analytics"
        ));
        assert!(matches!(
            repo.list_web_searches(100).await,
            Err(RepoError::Backend(message)) if message == "web"
        ));
        assert!(matches!(
            repo.latest_ingest_heartbeat().await,
            Err(RepoError::Backend(message)) if message == "heartbeat"
        ));
        assert!(matches!(
            repo.list_table_summaries().await,
            Err(RepoError::Backend(message)) if message == "tables"
        ));
        assert!(matches!(
            repo.preview_table(TablePreviewQuery {
                table: "events".to_string(),
                limit: 25,
            })
            .await,
            Err(RepoError::Backend(message)) if message == "preview"
        ));
        assert!(matches!(
            repo.read_store_health().await,
            Err(RepoError::Backend(message)) if message == "health"
        ));
        assert!(matches!(
            repo.read_store_diagnostics().await,
            Err(RepoError::Backend(message)) if message == "diagnostics"
        ));
    }
}
