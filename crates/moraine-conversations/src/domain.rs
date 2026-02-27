use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationMode {
    WebSearch,
    McpInternal,
    ToolCalling,
    Chat,
}

impl ConversationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WebSearch => "web_search",
            Self::McpInternal => "mcp_internal",
            Self::ToolCalling => "tool_calling",
            Self::Chat => "chat",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConversationListFilter {
    #[serde(default)]
    pub from_unix_ms: Option<i64>,
    #[serde(default)]
    pub to_unix_ms: Option<i64>,
    #[serde(default)]
    pub mode: Option<ConversationMode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnListFilter {
    #[serde(default)]
    pub from_turn_seq: Option<u32>,
    #[serde(default)]
    pub to_turn_seq: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRequest {
    #[serde(default = "default_page_limit")]
    pub limit: u16,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            limit: default_page_limit(),
            cursor: None,
        }
    }
}

impl PageRequest {
    pub fn normalized_limit(&self, max_limit: u16) -> u16 {
        self.limit.max(1).min(max_limit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSummary {
    pub session_id: String,
    pub first_event_time: String,
    pub first_event_unix_ms: i64,
    pub last_event_time: String,
    pub last_event_unix_ms: i64,
    pub total_turns: u32,
    pub total_events: u64,
    pub user_messages: u64,
    pub assistant_messages: u64,
    pub tool_calls: u64,
    pub tool_results: u64,
    pub mode: ConversationMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub summary: ConversationSummary,
    pub turns: Vec<TurnSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConversationDetailOptions {
    #[serde(default)]
    pub include_turns: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnSummary {
    pub session_id: String,
    pub turn_seq: u32,
    pub turn_id: String,
    pub started_at: String,
    pub started_at_unix_ms: i64,
    pub ended_at: String,
    pub ended_at_unix_ms: i64,
    pub total_events: u64,
    pub user_messages: u64,
    pub assistant_messages: u64,
    pub tool_calls: u64,
    pub tool_results: u64,
    pub reasoning_items: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub summary: TurnSummary,
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub session_id: String,
    pub event_uid: String,
    pub event_order: u64,
    pub turn_seq: u32,
    pub event_time: String,
    pub actor_role: String,
    pub event_class: String,
    pub payload_type: String,
    pub call_id: String,
    pub name: String,
    pub phase: String,
    pub item_id: String,
    pub source_ref: String,
    pub text_content: String,
    pub payload_json: String,
    pub token_usage_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenEvent {
    pub is_target: bool,
    pub session_id: String,
    pub event_uid: String,
    pub event_order: u64,
    pub turn_seq: u32,
    pub event_time: String,
    pub actor_role: String,
    pub event_class: String,
    pub payload_type: String,
    pub call_id: String,
    pub name: String,
    pub phase: String,
    pub item_id: String,
    pub source_ref: String,
    pub text_content: String,
    pub payload_json: String,
    pub token_usage_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenContext {
    pub found: bool,
    pub event_uid: String,
    pub session_id: String,
    pub target_event_order: u64,
    pub turn_seq: u32,
    pub before: u16,
    pub after: u16,
    pub events: Vec<OpenEvent>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchEventsQuery {
    pub query: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub limit: Option<u16>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub min_score: Option<f64>,
    #[serde(default)]
    pub min_should_match: Option<u16>,
    #[serde(default)]
    pub include_tool_events: Option<bool>,
    #[serde(default)]
    pub event_kinds: Option<Vec<SearchEventKind>>,
    #[serde(default)]
    pub exclude_codex_mcp: Option<bool>,
    #[serde(default)]
    pub disable_cache: Option<bool>,
    #[serde(default)]
    pub search_strategy: Option<SearchEventsStrategy>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchEventsStrategy {
    #[default]
    Optimized,
    OracleExact,
}

impl SearchEventsStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Optimized => "optimized",
            Self::OracleExact => "oracle_exact",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchEventKind {
    Message,
    Reasoning,
    ToolCall,
    ToolResult,
}

impl SearchEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventsStats {
    pub docs: u64,
    pub avgdl: f64,
    pub took_ms: u32,
    pub result_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventHit {
    pub rank: usize,
    pub event_uid: String,
    pub session_id: String,
    pub source_name: String,
    pub provider: String,
    pub score: f64,
    pub matched_terms: u64,
    pub doc_len: u32,
    pub event_class: String,
    pub payload_type: String,
    pub actor_role: String,
    pub name: String,
    pub phase: String,
    pub source_ref: String,
    pub text_preview: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventsResult {
    pub query_id: String,
    pub query: String,
    pub terms: Vec<String>,
    pub stats: SearchEventsStats,
    pub hits: Vec<SearchEventHit>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConversationSearchQuery {
    pub query: String,
    #[serde(default)]
    pub limit: Option<u16>,
    #[serde(default)]
    pub min_score: Option<f64>,
    #[serde(default)]
    pub min_should_match: Option<u16>,
    #[serde(default)]
    pub from_unix_ms: Option<i64>,
    #[serde(default)]
    pub to_unix_ms: Option<i64>,
    #[serde(default)]
    pub mode: Option<ConversationMode>,
    #[serde(default)]
    pub include_tool_events: Option<bool>,
    #[serde(default)]
    pub exclude_codex_mcp: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSearchStats {
    pub docs: u64,
    pub avgdl: f64,
    pub took_ms: u32,
    pub result_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSearchHit {
    pub rank: usize,
    pub session_id: String,
    pub score: f64,
    pub matched_terms: u16,
    pub event_count_considered: u32,
    pub best_event_uid: Option<String>,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSearchResults {
    pub query_id: String,
    pub query: String,
    pub terms: Vec<String>,
    pub stats: ConversationSearchStats,
    pub hits: Vec<ConversationSearchHit>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenEventRequest {
    pub event_uid: String,
    #[serde(default)]
    pub before: Option<u16>,
    #[serde(default)]
    pub after: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub max_results: u16,
    pub preview_chars: u16,
    pub default_context_before: u16,
    pub default_context_after: u16,
    pub default_include_tool_events: bool,
    pub default_exclude_codex_mcp: bool,
    pub async_log_writes: bool,
    pub bm25_k1: f64,
    pub bm25_b: f64,
    pub bm25_default_min_score: f64,
    pub bm25_default_min_should_match: u16,
    pub bm25_max_query_terms: usize,
}

impl Default for RepoConfig {
    fn default() -> Self {
        Self {
            max_results: 25,
            preview_chars: 220,
            default_context_before: 6,
            default_context_after: 6,
            default_include_tool_events: false,
            default_exclude_codex_mcp: true,
            async_log_writes: true,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            bm25_default_min_score: 0.0,
            bm25_default_min_should_match: 1,
            bm25_max_query_terms: 16,
        }
    }
}

fn default_page_limit() -> u16 {
    50
}
