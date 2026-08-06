use std::future::Future;

use async_trait::async_trait;
use moraine_conversations::*;

/// Repository test adapter that gives each repository operation an explicit,
/// independent internal owner. Tests that exercise ownership failures can use
/// `unowned()` to bypass the adapter deliberately.
#[derive(Clone)]
pub(crate) struct OwnedRepository {
    inner: ClickHouseConversationRepository,
}

impl OwnedRepository {
    pub(crate) fn new(inner: ClickHouseConversationRepository) -> Self {
        Self { inner }
    }

    pub(crate) fn runtime(&self) -> QueryRuntime {
        self.inner.query_runtime()
    }

    pub(crate) fn unowned(&self) -> &ClickHouseConversationRepository {
        &self.inner
    }

    async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let owner = QueryOwner::new(&self.runtime(), QueryWorkload::Internal)
            .expect("repository test owner");
        owner.scope(future).await
    }
}

#[async_trait]
impl ConversationRepository for OwnedRepository {
    fn config(&self) -> &RepoConfig {
        self.inner.config()
    }
    fn query_runtime(&self) -> Option<QueryRuntime> {
        Some(self.runtime())
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        self.run(self.inner.prewarm_mcp_search_state()).await
    }
    async fn list_session_analytics(
        &self,
        query: SessionAnalyticsQuery,
    ) -> RepoResult<Vec<SessionAnalytics>> {
        self.run(self.inner.list_session_analytics(query)).await
    }
    async fn analytics_series(&self, range: AnalyticsRange) -> RepoResult<AnalyticsSnapshot> {
        self.run(self.inner.analytics_series(range)).await
    }
    async fn list_web_searches(&self, limit: u16) -> RepoResult<Vec<WebSearchEvent>> {
        self.run(self.inner.list_web_searches(limit)).await
    }
    async fn latest_ingest_heartbeat(&self) -> RepoResult<IngestHeartbeatRead> {
        self.run(self.inner.latest_ingest_heartbeat()).await
    }
    async fn ingest_status(&self, history_limit: u16) -> RepoResult<IngestStatusRead> {
        self.run(self.inner.ingest_status(history_limit)).await
    }
    async fn list_table_summaries(&self) -> RepoResult<TableSummaries> {
        self.run(self.inner.list_table_summaries()).await
    }
    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview> {
        self.run(self.inner.preview_table(query)).await
    }
    async fn read_store_health(&self) -> RepoResult<StoreHealth> {
        self.run(self.inner.read_store_health()).await
    }
    async fn read_store_diagnostics(&self) -> RepoResult<StoreDiagnostics> {
        self.run(self.inner.read_store_diagnostics()).await
    }
    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<ConversationSummary>> {
        self.run(self.inner.list_conversations(filter, page)).await
    }
    async fn get_conversation(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        self.run(self.inner.get_conversation(session_id, opts))
            .await
    }
    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>> {
        self.run(self.inner.get_session_metadata(session_id)).await
    }
    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>> {
        self.run(self.inner.get_mcp_session(session_id)).await
    }
    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        self.run(self.inner.list_mcp_sessions(filter, page)).await
    }
    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        self.run(self.inner.list_turns(session_id, filter, page))
            .await
    }
    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        self.run(self.inner.get_turn(session_id, turn_seq)).await
    }
    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.run(self.inner.get_mcp_turn(session_id, turn_seq))
            .await
    }
    async fn get_mcp_turn_summary(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.run(self.inner.get_mcp_turn_summary(session_id, turn_seq))
            .await
    }
    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        self.run(self.inner.open_event(req)).await
    }
    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        self.run(self.inner.get_mcp_event(event_uid)).await
    }
    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        self.run(self.inner.list_session_events(query, page)).await
    }
    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        self.run(self.inner.search_events(query)).await
    }
    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        self.run(self.inner.search_mcp_events(query)).await
    }
    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        self.run(self.inner.search_conversations(query)).await
    }
    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        self.run(self.inner.search_session_metadata(query)).await
    }
    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        self.run(self.inner.file_attention(query)).await
    }
}
