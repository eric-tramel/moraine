use std::time::{SystemTime, UNIX_EPOCH};

use moraine_clickhouse::{STATE_KEY_CORE_INDEXES, STATE_KEY_OPEN_V2};

use super::*;
use crate::domain::{
    AnalyticsRange, AnalyticsSnapshot, CoreIndexHealth, IngestHeartbeatRead, SessionAnalytics,
    SessionAnalyticsQuery, StoreDiagnostics, StoreHealth, StoreProbe, TablePreview,
    TablePreviewQuery, TableSummaries, WebSearchEvent,
};

#[async_trait]
impl ConversationRepository for ClickHouseConversationRepository {
    fn config(&self) -> &RepoConfig {
        ClickHouseConversationRepository::config(self)
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        self.run_publication_consistent(PublicationReadClass::MovingFeed, || {
            ClickHouseConversationRepository::prewarm_mcp_search_state(self)
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

    async fn list_table_summaries(&self) -> RepoResult<TableSummaries> {
        self.list_table_summaries_impl().await
    }

    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview> {
        self.preview_table_impl(query).await
    }

    async fn read_store_health(&self) -> RepoResult<StoreHealth> {
        let mut health = self.read_store_health_impl().await?;
        // Additive canonical read-index probe (issue #598 WI-05). Best-effort:
        // a failure to read the readiness state must not fail the whole health
        // report, so it degrades to the `Failed` probe variant rather than
        // propagating.
        health.core_index = self.read_core_index_health().await;
        Ok(health)
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

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        // MovingFeed, not AnchoredSession: that class exists for a single
        // anchored session and a K-session page has no single scope.
        //
        // No deadline narrowing here. Callers scope the envelope around this
        // whole call (`list_sessions_v1` narrows to `LIST_SESSIONS_DEADLINE_MS`),
        // which already puts every statement of the page — snapshot capture and
        // revalidation, the directory pass, and the batched hydration — under
        // one wall with the server-side `max_execution_time` tracking it.
        // A second narrowing inside the repository would be that same bound
        // applied twice.
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

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        let scope = PublicationReadScope::event(&req.event_uid);
        self.run_publication_consistent_scoped_with_result_scope(
            PublicationReadClass::Strict,
            scope,
            |result: &OpenContext| {
                if result.found {
                    Some(PublicationReadScope::session(&result.session_id))
                } else {
                    None
                }
            },
            || self.open_event_impl(req.clone()),
        )
        .await
    }

    async fn canonical_reader_ready(&self) -> bool {
        // Same latch the directory listing path gates on, so `open` and `list`
        // cannot disagree about whether this backend's canonical indexes are
        // published.
        self.canonical_list_path_ready().await
    }

    async fn canonical_open_session_page(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>> {
        // The issue-598 v2 reader runs as an `AnchoredSession` read so an
        // insert-only append fence overlapping the target session does not
        // bounce it (design §5.1, D9).
        self.run_publication_consistent_scoped(
            PublicationReadClass::AnchoredSession,
            PublicationReadScope::session(session_id),
            || self.canonical_open_session_page_impl(session_id, limit, after.clone()),
        )
        .await
    }

    async fn canonical_open_turn_page(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        include_events: bool,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalTurnPage>>> {
        self.run_publication_consistent_scoped(
            PublicationReadClass::AnchoredSession,
            PublicationReadScope::session(session_id),
            || {
                self.canonical_open_turn_page_impl(
                    session_id,
                    turn_seq,
                    limit,
                    include_events,
                    after.clone(),
                )
            },
        )
        .await
    }

    async fn canonical_open_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        // Event opens widen their scope from the event to its resolved session
        // once the locator has identified it (mirrors `get_mcp_event`).
        self.run_publication_consistent_scoped_with_result_scope(
            PublicationReadClass::AnchoredSession,
            PublicationReadScope::event(event_uid),
            |result: &Option<McpEventOpen>| {
                result
                    .as_ref()
                    .map(|event| PublicationReadScope::session(&event.event.session_id))
            },
            || self.canonical_open_event_impl(event_uid),
        )
        .await
    }

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        self.run_publication_consistent_scoped_with_result_scope(
            PublicationReadClass::Strict,
            PublicationReadScope::event(event_uid),
            |result: &Option<McpEventOpen>| {
                result
                    .as_ref()
                    .map(|event| PublicationReadScope::session(&event.event.session_id))
            },
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
        // Query ids and deadlines are owned by the transport envelope; the
        // legacy per-tool id rescope is gone. The caller's cancellation token
        // survives only as the reported `query_id` in the result — explicit
        // cancellation now targets the envelope's request id via
        // `cancel_query`, and abandoned statements are killed by the
        // transport's drop guards.
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

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        // Delegate to the inherent method so both entry points share the
        // publication scope AND the envelope deadline narrowing to the
        // tool's execution budget.
        ClickHouseConversationRepository::file_attention(self, query).await
    }

    async fn cancel_query(&self, query_id: &str) -> RepoResult<()> {
        ClickHouseConversationRepository::cancel_query(self, query_id).await
    }
}

impl ClickHouseConversationRepository {
    /// Best-effort canonical read-index (issue #598) readiness probe for
    /// [`read_store_health`]. Runs under the caller's active query envelope —
    /// the monitor `/api/v1/health` and `/api/v1/status` handlers scope one
    /// Interactive envelope per request, exactly like the sibling
    /// `database_exists`/`connections` probes. Any backend error (an absent
    /// envelope, or a store that cannot report readiness) maps to the
    /// not-configured `Failed` variant so store health still renders. A
    /// reachable database that has not applied migration 036 is not an error:
    /// the WI-03 accessors return `None` for the absent state table, yielding an
    /// `Available` probe with every readiness flag false.
    async fn read_core_index_health(&self) -> StoreProbe<CoreIndexHealth> {
        match self.gather_core_index_health().await {
            Ok(health) => StoreProbe::Available(health),
            Err(error) => StoreProbe::Failed {
                message: error.to_string(),
            },
        }
    }

    /// Read the three `mcp_read_index_state` rows the monitor surfaces and fold
    /// them into a [`CoreIndexHealth`], mirroring the CLI `build_core_index_report`
    /// (issue #598 WI-05) minus the config-derived reader-resolution fields.
    async fn gather_core_index_health(&self) -> RepoResult<CoreIndexHealth> {
        let core_indexes =
            self.map_backend(self.ch.read_index_state(STATE_KEY_CORE_INDEXES).await)?;
        let open_v2 = self.map_backend(self.ch.read_index_state(STATE_KEY_OPEN_V2).await)?;
        let audit_outcome = self.map_backend(self.ch.core_index_audit_outcome().await)?;

        let core_indexes_ready = core_indexes.as_ref().is_some_and(|row| row.ready == 1);
        let open_v2_ready = open_v2.as_ref().is_some_and(|row| row.ready == 1);
        // Provenance is only meaningful once the flag is set and a value was
        // recorded (matches the CLI report's guard).
        let open_v2_provenance = open_v2
            .as_ref()
            .and_then(|row| (open_v2_ready && !row.cursor.is_empty()).then(|| row.cursor.clone()));
        let backfill_cursor_age_ms = core_indexes
            .as_ref()
            .and_then(|row| snowflake_age_ms(row.generation));

        Ok(CoreIndexHealth {
            core_indexes_ready,
            open_v2_ready,
            open_v2_provenance,
            backfill_cursor_age_ms,
            audit_outcome,
        })
    }
}

/// Milliseconds since a ClickHouse `generateSnowflakeID()` value was written.
/// The top 41 bits are milliseconds since the Unix epoch, so `generation >> 22`
/// is the write time. Seed rows carry `generation = 0`; any value too small to
/// be a real snowflake is treated as "not yet swept" (`None`). This is the CLI
/// `snowflake_age_seconds` (issue #598 WI-05) at millisecond resolution — the
/// health probe reports `backfill_cursor_age_ms`.
fn snowflake_age_ms(generation: u64) -> Option<i64> {
    let write_ms = (generation >> 22) as i64;
    if write_ms == 0 {
        return None;
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    Some(now_ms - write_ms)
}
