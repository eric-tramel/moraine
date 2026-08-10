use async_trait::async_trait;

use crate::domain::{
    AnalyticsRange, AnalyticsSnapshot, IngestHeartbeatRead, IngestStatusRead, SessionAnalytics,
    SessionAnalyticsQuery, StoreDiagnostics, StoreHealth, TablePreview, TablePreviewQuery,
    TableSummaries, WebSearchEvent,
};
use crate::domain::{
    CanonicalContinuation, CanonicalReadAnchor, CanonicalReadOutcome, CanonicalSessionPage,
    CanonicalSessionSignals, CanonicalTurnPage, Conversation, ConversationDetailOptions,
    ConversationListFilter, ConversationSearchQuery, ConversationSearchResults, FileAttentionQuery,
    FileAttentionTouch, McpEventOpen, McpSessionListFilter, McpSessionListItem, McpSessionOpen,
    McpTurnCompact, McpTurnOpen, McpTurnRef, OpenContext, OpenEventRequest, Page, PageRequest,
    RepoConfig, SearchEventsQuery, SearchEventsResult, SearchMcpEventsQuery, SearchMcpEventsResult,
    SessionEventsQuery, SessionMetadata, SessionMetadataSearchQuery, SessionMetadataSearchResults,
    TraceEvent, Turn, TurnListFilter, TurnSummary,
};
use crate::error::{RepoError, RepoResult};

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
    async fn ingest_status(&self, _history_limit: u16) -> RepoResult<IngestStatusRead> {
        Ok(IngestStatusRead {
            heartbeat: self.latest_ingest_heartbeat().await?,
            history: Vec::new(),
        })
    }

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

    async fn canonical_open_session_page(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>> {
        let Some(mut session) = self.get_mcp_session(session_id).await? else {
            return Ok(None);
        };
        let signals = session
            .snapshot
            .as_ref()
            .map(|snapshot| CanonicalSessionSignals {
                unique_versions: snapshot.generation,
                fingerprint_a: u64::from(snapshot.slot),
                fingerprint_b: 0,
            });
        if after.is_some() && signals.is_none() {
            return Err(RepoError::invalid_cursor(
                "pagination snapshot is unavailable; reopen the target",
            ));
        }
        if after
            .as_ref()
            .is_some_and(|cursor| signals.as_ref() != Some(&cursor.signals))
        {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let after_turn_seq = after.as_ref().map_or(0, |cursor| cursor.after_turn_seq);
        let start = session
            .turns
            .iter()
            .position(|turn| turn.metadata.turn_seq > after_turn_seq)
            .unwrap_or(session.turns.len());
        let turn_ref = |turn: &McpTurnCompact| McpTurnRef {
            session_id: turn.metadata.session_id.clone(),
            turn_seq: turn.metadata.turn_seq,
            turn_id: turn.metadata.turn_id.clone(),
            started_at: turn.metadata.started_at.clone(),
            ended_at: turn.metadata.ended_at.clone(),
        };
        let first_turn = session.turns.first().map(turn_ref);
        let last_turn = session.turns.last().map(turn_ref);
        let end = start
            .saturating_add(usize::from(limit))
            .min(session.turns.len());
        let has_more = end < session.turns.len();
        session.turns = session.turns[start..end].to_vec();
        if has_more && signals.is_none() {
            return Err(RepoError::invalid_cursor(
                "pagination snapshot is unavailable; reopen the target",
            ));
        }
        let continuation = if has_more {
            session.turns.last().map(|turn| CanonicalContinuation {
                signals: signals.clone().expect("checked above"),
                after: CanonicalReadAnchor {
                    sort_time_ms: turn.metadata.ended_at_unix_ms,
                    source_file: String::new(),
                    source_generation: 0,
                    source_offset: 0,
                    source_line_no: 0,
                    emission_index: 0,
                    event_uid: turn
                        .terminal_event_uid
                        .clone()
                        .unwrap_or_else(|| turn.metadata.turn_id.clone()),
                    event_version: 0,
                    event_order: u64::from(turn.metadata.turn_seq),
                    prefix_user_message_count: u64::from(turn.metadata.turn_seq),
                    event_ordinal: 0,
                },
                after_turn_seq: turn.metadata.turn_seq,
                session_carry: None,
            })
        } else {
            None
        };
        Ok(Some(CanonicalReadOutcome::Page(CanonicalSessionPage {
            session,
            first_turn_seq: first_turn.as_ref().map(|turn| turn.turn_seq),
            last_turn_seq: last_turn.as_ref().map(|turn| turn.turn_seq),
            continuation,
        })))
    }

    async fn canonical_open_turn_page(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalTurnPage>>> {
        let Some(mut turn) = self.get_mcp_turn(session_id, turn_seq).await? else {
            return Ok(None);
        };
        let signals = turn
            .snapshot
            .as_ref()
            .map(|snapshot| CanonicalSessionSignals {
                unique_versions: snapshot.generation,
                fingerprint_a: u64::from(snapshot.slot),
                fingerprint_b: 0,
            });
        if after.is_some() && signals.is_none() {
            return Err(RepoError::invalid_cursor(
                "pagination snapshot is unavailable; reopen the target",
            ));
        }
        if after.as_ref().is_some_and(|cursor| {
            signals.as_ref() != Some(&cursor.signals) || cursor.after_turn_seq != turn_seq
        }) {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let start = after
            .as_ref()
            .and_then(|cursor| usize::try_from(cursor.after.event_ordinal).ok())
            .unwrap_or(0);
        let end = start
            .saturating_add(usize::from(limit))
            .min(turn.events.len());
        let has_more = end < turn.events.len();
        turn.events = turn.events[start..end].to_vec();
        if has_more && signals.is_none() {
            return Err(RepoError::invalid_cursor(
                "pagination snapshot is unavailable; reopen the target",
            ));
        }
        let continuation = if has_more {
            turn.events.last().map(|event| CanonicalContinuation {
                signals: signals.clone().expect("checked above"),
                after: CanonicalReadAnchor {
                    sort_time_ms: event.event_unix_ms,
                    source_file: String::new(),
                    source_generation: 0,
                    source_offset: 0,
                    source_line_no: 0,
                    emission_index: 0,
                    event_uid: event.event_uid.clone(),
                    event_version: 0,
                    event_order: event.event_order,
                    prefix_user_message_count: 0,
                    event_ordinal: end as u32,
                },
                after_turn_seq: turn_seq,
                session_carry: None,
            })
        } else {
            None
        };
        Ok(Some(CanonicalReadOutcome::Page(CanonicalTurnPage {
            turn,
            continuation,
        })))
    }
}
