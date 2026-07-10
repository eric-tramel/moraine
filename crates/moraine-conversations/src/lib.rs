mod clickhouse_repo;
mod cursor;
mod domain;
mod error;
mod in_memory_repo;
mod repo;

pub use clickhouse_repo::ClickHouseConversationRepository;
pub use domain::{
    is_user_facing_content_event, Conversation, ConversationDetailOptions, ConversationListFilter,
    ConversationListSort, ConversationMode, ConversationSearchHit, ConversationSearchQuery,
    ConversationSearchResults, ConversationSearchStats, ConversationSummary, FileAttentionQuery,
    FileAttentionTouch, McpEventOpen, McpEventRef, McpEventSummary, McpEventType,
    McpSessionListFilter, McpSessionListItem, McpSessionOpen, McpTurnCompact, McpTurnOpen,
    McpTurnRef, OpenContext, OpenEvent, OpenEventRequest, Page, PageRequest, RepoConfig,
    SearchEventHit, SearchEventKind, SearchEventsQuery, SearchEventsResult, SearchEventsStats,
    SearchMcpEventHit, SearchMcpEventsQuery, SearchMcpEventsResult, SearchMcpEventsStats,
    SearchStrategyHint, SessionEventsDirection, SessionEventsQuery, SessionMetadata,
    SessionMetadataSearchHit, SessionMetadataSearchQuery, SessionMetadataSearchResults,
    SessionMetadataSearchStats, SessionOriginScope, TraceEvent, Turn, TurnListFilter, TurnSummary,
};
pub use domain::{
    AnalyticsConcurrencyPoint, AnalyticsRange, AnalyticsSnapshot, AnalyticsTokenPoint,
    AnalyticsTurnPoint, AnalyticsWindow, IngestHeartbeat, IngestHeartbeatRead, SessionAnalytics,
    SessionAnalyticsQuery, SessionLookback, SessionStep, SessionTurn, StoreConnectionMetrics,
    StoreDiagnostics, StoreHealth, StoreProbe, TableColumn, TablePreview, TablePreviewQuery,
    TableSummaries, TableSummary, ToolResult, WebSearchEvent,
};
pub use error::{RepoError, RepoResult};
pub use in_memory_repo::{
    InMemoryConversationCalls, InMemoryConversationRepository, InMemoryConversationResponses,
};
pub use repo::ConversationRepository;

/// Build the production ClickHouse repository behind its backend-neutral read
/// trait. Consumers provide configuration, but never construct or retain the
/// storage client themselves.
pub fn build_clickhouse_repository(
    clickhouse: moraine_config::ClickHouseConfig,
    config: RepoConfig,
) -> anyhow::Result<std::sync::Arc<dyn ConversationRepository>> {
    let client = moraine_clickhouse::ClickHouseClient::new(clickhouse)?;
    Ok(std::sync::Arc::new(ClickHouseConversationRepository::new(
        client, config,
    )))
}

#[cfg(test)]
mod construction_tests {
    use super::{build_clickhouse_repository, RepoConfig};

    #[test]
    fn clickhouse_factory_returns_configured_trait_object() {
        let config = RepoConfig {
            max_results: 73,
            ..RepoConfig::default()
        };
        let repository =
            build_clickhouse_repository(moraine_config::ClickHouseConfig::default(), config)
                .expect("valid ClickHouse configuration");

        assert_eq!(repository.config().max_results, 73);
    }
}
