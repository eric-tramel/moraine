use async_trait::async_trait;

use crate::domain::{
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationSearchQuery,
    ConversationSearchResults, OpenContext, OpenEventRequest, Page, PageRequest, SearchEventsQuery,
    SearchEventsResult, Turn, TurnListFilter, TurnSummary,
};
use crate::error::RepoResult;

#[async_trait]
pub trait ConversationRepository: Send + Sync {
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

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>>;

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>>;

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext>;

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult>;

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults>;
}
