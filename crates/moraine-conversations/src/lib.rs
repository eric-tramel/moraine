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
pub use error::{RepoError, RepoResult};
pub use in_memory_repo::{
    InMemoryConversationCalls, InMemoryConversationRepository, InMemoryConversationResponses,
};
pub use repo::ConversationRepository;
