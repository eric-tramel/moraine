mod backend_router;
mod clickhouse_repo;
mod cursor;
mod domain;
mod error;
mod in_memory_repo;
mod repo;

pub use backend_router::{BackendRepository, BackendRepositoryRouter};
pub use clickhouse_repo::{
    with_repository_query_deadline, with_repository_query_id, ClickHouseConversationRepository,
};
pub use domain::{
    is_user_facing_content_event, Conversation, ConversationDetailOptions, ConversationListFilter,
    ConversationListSort, ConversationMode, ConversationSearchHit, ConversationSearchQuery,
    ConversationSearchResults, ConversationSearchStats, ConversationSummary, FileAttentionQuery,
    FileAttentionTouch, McpEventOpen, McpEventRef, McpEventSummary, McpEventType, McpOpenSnapshot,
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
    AnalyticsTurnPoint, AnalyticsWindow, IngestHeartbeat, IngestHeartbeatRead,
    PublicationDiagnostics, SessionAnalytics, SessionAnalyticsQuery, SessionLookback, SessionStep,
    SessionTurn, StoreConnectionMetrics, StoreDiagnostics, StoreHealth, StoreProbe, TableColumn,
    TablePreview, TablePreviewQuery, TableSummaries, TableSummary, ToolResult, WebSearchEvent,
};
pub use error::{RepoError, RepoResult};
pub use in_memory_repo::{
    InMemoryConversationCalls, InMemoryConversationRepository, InMemoryConversationResponses,
};
pub use repo::ConversationRepository;

/// Build the production ClickHouse repository behind its backend-neutral read
/// trait. The compatibility factory assigns the shared conversation-reader
/// identity; composition roots that own a distinct role should use
/// [`build_clickhouse_repository_with_user_agent`].
pub fn build_clickhouse_repository(
    clickhouse: moraine_config::ClickHouseConfig,
    config: RepoConfig,
) -> anyhow::Result<std::sync::Arc<dyn ConversationRepository>> {
    let user_agent = format!(
        "moraine-conversations/{} (pid={})",
        env!("CARGO_PKG_VERSION"),
        std::process::id()
    );
    build_clickhouse_repository_with_user_agent(clickhouse, config, user_agent)
}

/// Build the production ClickHouse repository with an HTTP User-Agent chosen
/// by the owning composition root.
pub fn build_clickhouse_repository_with_user_agent(
    clickhouse: moraine_config::ClickHouseConfig,
    config: RepoConfig,
    user_agent: impl AsRef<str>,
) -> anyhow::Result<std::sync::Arc<dyn ConversationRepository>> {
    let client = moraine_clickhouse::ClickHouseClient::new_with_user_agent(clickhouse, user_agent)?;
    Ok(std::sync::Arc::new(ClickHouseConversationRepository::new(
        client, config,
    )))
}

#[cfg(test)]
mod construction_tests {
    use super::{
        build_clickhouse_repository, build_clickhouse_repository_with_user_agent, RepoConfig,
    };

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

    #[test]
    fn attributed_clickhouse_factory_returns_configured_trait_object() {
        let config = RepoConfig {
            max_results: 41,
            ..RepoConfig::default()
        };
        let repository = build_clickhouse_repository_with_user_agent(
            moraine_config::ClickHouseConfig::default(),
            config,
            "moraine-backend/0.6.4 (pid=4242)",
        )
        .expect("valid attributed ClickHouse configuration");

        assert_eq!(repository.config().max_results, 41);
    }
}
