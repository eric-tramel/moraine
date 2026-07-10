use async_trait::async_trait;

use crate::domain::{
    AnalyticsRange, AnalyticsSnapshot, IngestHeartbeatRead, SessionAnalytics,
    SessionAnalyticsQuery, StoreDiagnostics, StoreHealth, TablePreview, TablePreviewQuery,
    TableSummaries, WebSearchEvent,
};
use crate::domain::{
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationSearchQuery,
    ConversationSearchResults, FileAttentionQuery, FileAttentionTouch, McpEventOpen,
    McpSessionListFilter, McpSessionListItem, McpSessionOpen, McpTurnOpen, OpenContext,
    OpenEventRequest, Page, PageRequest, RepoConfig, SearchEventsQuery, SearchEventsResult,
    SearchMcpEventsQuery, SearchMcpEventsResult, SessionEventsQuery, SessionMetadata,
    SessionMetadataSearchQuery, SessionMetadataSearchResults, TraceEvent, Turn, TurnListFilter,
    TurnSummary,
};
use crate::error::RepoResult;

#[async_trait]
pub trait ConversationRepository: Send + Sync {
    fn config(&self) -> &RepoConfig;

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()>;
    async fn list_session_analytics(
        &self,
        query: SessionAnalyticsQuery,
    ) -> RepoResult<Vec<SessionAnalytics>>;

    async fn analytics_series(&self, range: AnalyticsRange) -> RepoResult<AnalyticsSnapshot>;

    async fn list_web_searches(&self, limit: u16) -> RepoResult<Vec<WebSearchEvent>>;

    async fn latest_ingest_heartbeat(&self) -> RepoResult<IngestHeartbeatRead>;

    async fn list_table_summaries(&self) -> RepoResult<TableSummaries>;

    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview>;

    async fn read_store_health(&self) -> RepoResult<StoreHealth>;

    async fn read_store_diagnostics(&self) -> RepoResult<StoreDiagnostics>;

    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<crate::domain::ConversationSummary>>;

    async fn get_conversation(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>>;

    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>>;

    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>>;

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>>;

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>>;

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>>;

    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>>;

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext>;

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>>;

    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>>;

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult>;

    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult>;

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults>;

    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults>;

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>>;

    async fn cancel_query(&self, query_id: &str) -> RepoResult<()>;
}
