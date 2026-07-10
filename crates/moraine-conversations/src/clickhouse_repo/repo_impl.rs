use super::*;

#[async_trait]
impl ConversationRepository for ClickHouseConversationRepository {
    fn config(&self) -> &RepoConfig {
        ClickHouseConversationRepository::config(self)
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        ClickHouseConversationRepository::prewarm_mcp_search_state(self).await
    }

    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<ConversationSummary>> {
        self.list_conversations_impl(filter, page).await
    }

    async fn get_conversation(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        self.get_conversation_impl(session_id, opts).await
    }

    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>> {
        self.get_session_metadata_impl(session_id).await
    }

    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>> {
        self.get_mcp_session_impl(session_id).await
    }

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        self.list_mcp_sessions_impl(filter, page).await
    }

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        self.list_turns_impl(session_id, filter, page).await
    }

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        self.get_turn_impl(session_id, turn_seq).await
    }

    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.get_mcp_turn_impl(session_id, turn_seq).await
    }

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        self.open_event_impl(req).await
    }

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        self.get_mcp_event_impl(event_uid).await
    }

    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        self.list_session_events_impl(query, page).await
    }

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        self.search_events_impl(query).await
    }

    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        self.search_mcp_events_impl(query).await
    }

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        self.search_conversations_impl(query).await
    }

    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        ClickHouseConversationRepository::search_session_metadata(self, query).await
    }

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        self.file_attention_impl(query).await
    }

    async fn cancel_query(&self, query_id: &str) -> RepoResult<()> {
        ClickHouseConversationRepository::cancel_query(self, query_id).await
    }
}
