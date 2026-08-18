use super::*;
use crate::domain::{
    AnalyticsRange, AnalyticsSnapshot, IngestHeartbeatRead, IngestStatusRead, SessionAnalytics,
    SessionAnalyticsQuery, StoreDiagnostics, StoreHealth, TablePreview, TablePreviewQuery,
    TableSummaries, WebSearchEvent,
};
use crate::domain::{
    CanonicalContinuation, CanonicalReadOutcome, CanonicalSessionPage, CanonicalTurnPage,
};

#[async_trait]
impl ConversationRepository for ClickHouseConversationRepository {
    fn config(&self) -> &RepoConfig {
        ClickHouseConversationRepository::config(self)
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.prewarm_mcp_search_state_impl()
        })
        .await
    }
    async fn list_session_analytics(
        &self,
        query: SessionAnalyticsQuery,
    ) -> RepoResult<Vec<SessionAnalytics>> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.list_session_analytics_impl(query.clone())
        })
        .await
    }

    async fn analytics_series(&self, range: AnalyticsRange) -> RepoResult<AnalyticsSnapshot> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.analytics_series_impl(range)
        })
        .await
    }

    async fn list_web_searches(&self, limit: u16) -> RepoResult<Vec<WebSearchEvent>> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.list_web_searches_impl(limit)
        })
        .await
    }

    async fn latest_ingest_heartbeat(&self) -> RepoResult<IngestHeartbeatRead> {
        self.latest_ingest_heartbeat_impl().await
    }
    async fn ingest_status(&self, history_limit: u16) -> RepoResult<IngestStatusRead> {
        self.ingest_status_impl(history_limit).await
    }

    async fn list_table_summaries(&self) -> RepoResult<TableSummaries> {
        self.list_table_summaries_impl().await
    }

    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview> {
        self.preview_table_impl(query).await
    }

    async fn read_store_health(&self) -> RepoResult<StoreHealth> {
        self.read_store_health_impl().await
    }

    async fn read_store_diagnostics(&self) -> RepoResult<StoreDiagnostics> {
        self.read_store_diagnostics_impl().await
    }

    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<ConversationSummary>> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.list_conversations_impl(filter.clone(), page.clone())
        })
        .await
    }

    async fn get_conversation(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.get_conversation_impl(session_id, opts.clone()),
        )
        .await
    }

    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.get_session_metadata_impl(session_id),
        )
        .await
    }

    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.get_mcp_session_impl(session_id),
        )
        .await
    }

    async fn canonical_open_session_page(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.canonical_open_session_page_impl(session_id, limit, after.clone()),
        )
        .await
    }

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.list_mcp_sessions_impl(filter.clone(), page.clone())
        })
        .await
    }

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.list_turns_impl(session_id, filter.clone(), page.clone()),
        )
        .await
    }

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.get_turn_impl(session_id, turn_seq),
        )
        .await
    }

    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.get_mcp_turn_impl(session_id, turn_seq, true),
        )
        .await
    }

    async fn get_mcp_turn_summary(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.get_mcp_turn_impl(session_id, turn_seq, false),
        )
        .await
    }

    async fn canonical_open_turn_page(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalTurnPage>>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::session(session_id),
            || self.canonical_open_turn_page_impl(session_id, turn_seq, limit, after.clone()),
        )
        .await
    }

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        let scope = PublicationReadScope::event(&req.event_uid);
        self.run_publication_consistent_scoped(PublicationReadClass::Strict, scope, || {
            self.open_event_impl(req.clone())
        })
        .await
    }

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::Strict,
            PublicationReadScope::event(event_uid),
            || self.get_mcp_event_impl(event_uid),
        )
        .await
    }

    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        let scope = PublicationReadScope::session(&query.session_id);
        self.run_publication_consistent_scoped(PublicationReadClass::Strict, scope, || {
            self.list_session_events_impl(query.clone(), page.clone())
        })
        .await
    }

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.search_events_impl(query.clone())
        })
        .await
    }

    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.search_mcp_events_impl(query.clone())
        })
        .await
    }

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.search_conversations_impl(query.clone())
        })
        .await
    }

    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            self.search_session_metadata_impl(query.clone())
        })
        .await
    }

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        self.run_publication_consistent(PublicationReadClass::Strict, || {
            self.file_attention_impl(query.clone())
        })
        .await
    }
}
