use async_trait::async_trait;

use crate::domain::{
    AnalyticsRange, AnalyticsSnapshot, IngestHeartbeatRead, SessionAnalytics,
    SessionAnalyticsQuery, StoreDiagnostics, StoreHealth, TablePreview, TablePreviewQuery,
    TableSummaries, WebSearchEvent,
};
use crate::domain::{
    CanonicalContinuation, CanonicalReadOutcome, CanonicalSessionPage, CanonicalTurnPage,
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationSearchQuery,
    ConversationSearchResults, FileAttentionQuery, FileAttentionTouch, McpEventOpen,
    McpSessionListFilter, McpSessionListItem, McpSessionOpen, McpTurnOpen, OpenContext,
    OpenEventRequest, Page, PageRequest, RepoConfig, SearchEventsQuery, SearchEventsResult,
    SearchMcpEventsQuery, SearchMcpEventsResult, SessionEventsQuery, SessionMetadata,
    SessionMetadataSearchQuery, SessionMetadataSearchResults, TraceEvent, Turn, TurnListFilter,
    TurnSummary,
};
use crate::error::{RepoError, RepoResult};

#[async_trait]
pub trait ConversationRepository: Send + Sync {
    fn config(&self) -> &RepoConfig;

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()>;

    /// The projector-backed dashboard read. It has had no interactive caller
    /// since issue-599 WI-04 moved the monitor session feed onto
    /// [`Self::list_mcp_sessions`]; only live-fixture and repository tests
    /// still reach it.
    ///
    /// Its implementation walks `v_session_summary` / `v_conversation_trace`
    /// and returns full transcripts, which is exactly the corpus-shaped read
    /// #599 removed from the interactive path. Deleting it belongs to the
    /// projector-retirement PR, which also retires the views it reads and the
    /// `mode_subquery_for_sessions` helper it shares with `list_conversations`
    /// and `get_conversation`.
    #[deprecated(
        note = "no interactive caller since issue-599 WI-04; removal belongs to the projector-retirement PR"
    )]
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

    /// Session DISCOVERY, and the single shared operation MCP `list_sessions`
    /// and the monitor session feed both page through (issue-599 WI-04 moved
    /// the feed here from [`Self::list_session_analytics`]). The `mcp_` prefix
    /// is historical; renaming it is recorded as a post-epic cleanup.
    ///
    /// Whether more results exist is `Page::next_cursor.is_some()` — there is
    /// deliberately no `total`, because a corpus-wide count is exactly the
    /// corpus-sized work this operation exists to remove. An empty `items`
    /// carrying a `next_cursor` is a legal "keep going" signal, not "no
    /// results".
    ///
    /// The cursor is a VALUE anchor over `(updated_at, session_id)`, not a
    /// snapshot: concurrent appends may move a session ahead of an existing
    /// cursor, and it will not be seen again until the caller restarts from
    /// page 1.
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

    async fn get_mcp_turn_summary(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.get_mcp_turn(session_id, turn_seq).await
    }

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

    // --- issue-598 v2 canonical `open` reader (WI-06) ----------------------
    //
    // Page-in / page-out entry points the tool-facing `open_v2` module (WI-07)
    // consumes. They are NOT yet on the `open` dispatch path — WI-08 performs
    // the one-way flip and, for the backend router, the delegation to the inner
    // repository. The default here fails typed so a backend that has not
    // implemented the reader (in-memory, or an unwired router) never silently
    // returns an empty page.

    /// Whether the canonical v2 reader below may serve reads on this
    /// repository.
    ///
    /// This is the SAME `open_v2` readiness authority the MCP `open` cutover
    /// and the issue-599 directory listing path gate on — one authority, one
    /// verdict, so no consumer can read canonical indexes the others refuse.
    /// Every caller of [`Self::canonical_open_session_page`] and its siblings
    /// must consult it first: those readers do not gate themselves, and on a
    /// backend whose indexes are unpublished they would answer from an
    /// incomplete store.
    ///
    /// The default is `false`, matching the typed failure the default readers
    /// below return: a repository with no v2 reader is never ready.
    async fn canonical_reader_ready(&self) -> bool {
        false
    }

    /// One keyset page of an `open(session)` traversal from live canonical rows.
    async fn canonical_open_session_page(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>> {
        let _ = (session_id, limit, after);
        Err(RepoError::backend(
            "canonical v2 open reader is not available on this repository",
        ))
    }

    /// One keyset page of an `open(turn)` traversal from live canonical rows.
    async fn canonical_open_turn_page(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        include_events: bool,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalTurnPage>>> {
        let _ = (session_id, turn_seq, limit, include_events, after);
        Err(RepoError::backend(
            "canonical v2 open reader is not available on this repository",
        ))
    }

    /// `open(event)` reconstructed from the locator seek plus the shared
    /// session reader.
    async fn canonical_open_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        let _ = event_uid;
        Err(RepoError::backend(
            "canonical v2 open reader is not available on this repository",
        ))
    }
}
