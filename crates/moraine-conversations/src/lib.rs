mod backend_router;
mod clickhouse_repo;
mod cursor;
mod domain;
mod error;
mod in_memory_repo;
mod repo;
mod session_label;

pub use backend_router::{BackendRepository, BackendRepositoryRouter};
pub use clickhouse_repo::ClickHouseConversationRepository;
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
    AnalyticsTurnPoint, AnalyticsWindow, IngestAlert, IngestAlertCode, IngestCondition,
    IngestConditionState, IngestConditionType, IngestCoverageBasis, IngestEta, IngestHeartbeat,
    IngestHeartbeatRead, IngestHistoryPoint, IngestProgressSnapshot, IngestRate,
    IngestSourceProgress, IngestStatus, IngestStatusRead, SessionAnalytics, SessionAnalyticsQuery,
    SessionLookback, SessionStep, SessionTurn, StoreConnectionMetrics, StoreDiagnostics,
    StoreHealth, StoreProbe, TableColumn, TablePreview, TablePreviewQuery, TableSummaries,
    TableSummary, ToolResult, WebSearchEvent,
};
pub use error::{RepoError, RepoResult};
pub use in_memory_repo::{
    InMemoryConversationCalls, InMemoryConversationRepository, InMemoryConversationResponses,
};
pub use moraine_clickhouse::{
    QueryCause, QueryOwner, QueryRuntime, QueryWorkload, QUERY_CLEANUP_GRACE,
};
pub use repo::ConversationRepository;

/// Build the production ClickHouse repository behind its backend-neutral read
/// trait. The compatibility factory assigns the shared conversation-reader
/// identity; composition roots that own a distinct role should use
/// [`build_clickhouse_repository_with_user_agent`].
pub fn build_clickhouse_repository(
    clickhouse: moraine_config::ClickHouseConfig,
    config: RepoConfig,
    query_runtime: QueryRuntime,
) -> anyhow::Result<std::sync::Arc<dyn ConversationRepository>> {
    let user_agent = format!(
        "moraine-conversations/{} (pid={})",
        moraine_config::BUILD_VERSION,
        std::process::id()
    );
    build_clickhouse_repository_with_user_agent(clickhouse, config, query_runtime, user_agent)
}

/// Build the production ClickHouse repository with an HTTP User-Agent chosen
/// by the owning composition root.
pub fn build_clickhouse_repository_with_user_agent(
    clickhouse: moraine_config::ClickHouseConfig,
    config: RepoConfig,
    query_runtime: QueryRuntime,
    user_agent: impl AsRef<str>,
) -> anyhow::Result<std::sync::Arc<dyn ConversationRepository>> {
    let client = moraine_clickhouse::ClickHouseClient::new_with_runtime_and_user_agent(
        clickhouse,
        query_runtime,
        user_agent,
    )?;
    Ok(std::sync::Arc::new(ClickHouseConversationRepository::new(
        client, config,
    )))
}

#[cfg(test)]
mod construction_tests {
    use super::{
        build_clickhouse_repository, build_clickhouse_repository_with_user_agent, QueryRuntime,
        RepoConfig,
    };

    #[tokio::test]
    async fn clickhouse_factory_returns_configured_trait_object() {
        let config = RepoConfig {
            max_results: 73,
            ..RepoConfig::default()
        };
        let runtime = QueryRuntime::new();
        let repository = build_clickhouse_repository(
            moraine_config::ClickHouseConfig::default(),
            config,
            runtime.clone(),
        )
        .expect("valid ClickHouse configuration");

        assert_eq!(repository.config().max_results, 73);
        let owner = super::QueryOwner::new(&runtime, super::QueryWorkload::Internal)
            .expect("factory runtime owner");
        assert_eq!(runtime.active_owner_count(), 1);
        owner.scope(async {}).await;
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
            QueryRuntime::new(),
            "moraine-backend/0.6.4 (pid=4242)",
        )
        .expect("valid attributed ClickHouse configuration");

        assert_eq!(repository.config().max_results, 41);
    }
}
