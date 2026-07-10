use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;

use crate::domain::{
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationSearchQuery,
    ConversationSearchResults, ConversationSearchStats, ConversationSummary, FileAttentionQuery,
    FileAttentionTouch, McpEventOpen, McpEventType, McpSessionListFilter, McpSessionListItem,
    McpSessionOpen, McpTurnOpen, OpenContext, OpenEventRequest, Page, PageRequest, RepoConfig,
    SearchEventsQuery, SearchEventsResult, SearchEventsStats, SearchMcpEventsQuery,
    SearchMcpEventsResult, SearchMcpEventsStats, SessionEventsQuery, SessionMetadata,
    SessionMetadataSearchQuery, SessionMetadataSearchResults, SessionMetadataSearchStats,
    TraceEvent, Turn, TurnListFilter, TurnSummary,
};
use crate::error::RepoResult;
use crate::repo::ConversationRepository;

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
    pub get_mcp_session: Option<RepoResult<Option<McpSessionOpen>>>,
    pub list_mcp_sessions: Option<RepoResult<Page<McpSessionListItem>>>,
    pub list_turns: Option<RepoResult<Page<TurnSummary>>>,
    pub get_turn: Option<RepoResult<Option<Turn>>>,
    pub get_mcp_turn: Option<RepoResult<Option<McpTurnOpen>>>,
    pub open_event: Option<RepoResult<OpenContext>>,
    pub get_mcp_event: Option<RepoResult<Option<McpEventOpen>>>,
    pub list_session_events: Option<RepoResult<Page<TraceEvent>>>,
    pub search_events: Option<RepoResult<SearchEventsResult>>,
    pub search_mcp_events: Option<RepoResult<SearchMcpEventsResult>>,
    pub search_conversations: Option<RepoResult<ConversationSearchResults>>,
    pub search_session_metadata: Option<RepoResult<SessionMetadataSearchResults>>,
    pub file_attention: Option<RepoResult<Vec<FileAttentionTouch>>>,
    pub prewarm_mcp_search_state: Option<RepoResult<()>>,
    pub cancel_query: Option<RepoResult<()>>,
}

/// Arguments observed by an [`InMemoryConversationRepository`].
#[derive(Debug, Clone, Default)]
pub struct InMemoryConversationCalls {
    pub list_conversations: Vec<(ConversationListFilter, PageRequest)>,
    pub get_conversation: Vec<(String, ConversationDetailOptions)>,
    pub get_session_metadata: Vec<String>,
    pub get_mcp_session: Vec<String>,
    pub list_mcp_sessions: Vec<(McpSessionListFilter, PageRequest)>,
    pub list_turns: Vec<(String, TurnListFilter, PageRequest)>,
    pub get_turn: Vec<(String, u32)>,
    pub get_mcp_turn: Vec<(String, u32)>,
    pub open_event: Vec<OpenEventRequest>,
    pub get_mcp_event: Vec<String>,
    pub list_session_events: Vec<(SessionEventsQuery, PageRequest)>,
    pub search_events: Vec<SearchEventsQuery>,
    pub search_mcp_events: Vec<SearchMcpEventsQuery>,
    pub search_conversations: Vec<ConversationSearchQuery>,
    pub search_session_metadata: Vec<SessionMetadataSearchQuery>,
    pub file_attention: Vec<FileAttentionQuery>,
    pub prewarm_mcp_search_state: usize,
    pub cancel_query: Vec<String>,
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
            query_id: "in-memory".to_string(),
            query: query.query.clone(),
            terms: Vec::new(),
            event_types: query
                .event_types
                .clone()
                .unwrap_or_else(McpEventType::default_search_types),
            truncated: false,
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

    fn empty_session_metadata_search(
        &self,
        query: &SessionMetadataSearchQuery,
    ) -> SessionMetadataSearchResults {
        let requested_limit = query.limit.unwrap_or(self.config.max_results).max(1);
        SessionMetadataSearchResults {
            query_id: "in-memory".to_string(),
            query: query.query.clone(),
            terms: Vec::new(),
            stats: SessionMetadataSearchStats {
                requested_limit,
                effective_limit: requested_limit.min(self.config.max_results),
                limit_capped: requested_limit > self.config.max_results,
                result_count: 0,
                took_ms: 0,
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

    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>> {
        self.record(|calls| calls.get_mcp_session.push(session_id.to_string()));
        response_or!(self, get_mcp_session, None)
    }

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        self.record(|calls| calls.list_mcp_sessions.push((filter, page)));
        response_or!(
            self,
            list_mcp_sessions,
            Page {
                items: Vec::new(),
                next_cursor: None,
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

    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.record(|calls| calls.get_mcp_turn.push((session_id.to_string(), turn_seq)));
        response_or!(self, get_mcp_turn, None)
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

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        self.record(|calls| calls.get_mcp_event.push(event_uid.to_string()));
        response_or!(self, get_mcp_event, None)
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

    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        self.record(|calls| calls.search_session_metadata.push(query.clone()));
        response_or!(
            self,
            search_session_metadata,
            self.empty_session_metadata_search(&query)
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
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
