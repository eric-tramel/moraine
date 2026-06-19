use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConversationListSort {
    Asc,
    #[default]
    Desc,
}

impl ConversationListSort {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
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
    #[serde(default)]
    pub sort: ConversationListSort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSessionListFilter {
    pub start_unix_ms: i64,
    pub end_unix_ms: i64,
    #[serde(default)]
    pub mode: Option<ConversationMode>,
    #[serde(default)]
    pub sort: ConversationListSort,
}

/// A single file-touch query for `file_attention`: every captured tool call
/// whose input path ends with `rel`, scoped and filtered per the request.
#[derive(Debug, Clone)]
pub struct FileAttentionQuery {
    /// Stable identifier assigned to the ClickHouse query so MCP deadlines can
    /// cancel the backend scan if the client-side timeout fires.
    pub query_id: String,
    /// Repo-relative tail to suffix-match against captured file paths. The tail
    /// is what unifies the same logical file across worktree roots.
    pub rel: String,
    /// Whether a structured matched path should be stripped by `rel` to report
    /// a worktree root. Disabled for arbitrary suffixes that could otherwise
    /// mislabel a source/package directory as a repository root.
    pub derive_worktree_roots: bool,
    /// When true the server's configured origin scope (`--project-only`) is
    /// applied on top of the tail match; when false (`scope:"all"`) it is
    /// dropped so touches in sibling and agent-isolation worktrees surface too.
    pub apply_project_scope: bool,
    pub start_unix_ms: Option<i64>,
    pub end_unix_ms: Option<i64>,
    /// Restrict to one tool name (case-insensitive); `None` matches all tools.
    pub tool: Option<String>,
    /// Drop common pure-read touches.
    pub mutations_only: bool,
    /// Hard cap on matched rows pulled from ClickHouse. Summary, root, and
    /// per-session rollups are computed over this scanned set; the caller flags
    /// the result truncated when the cap is hit.
    pub max_rows: usize,
    /// Server-side ClickHouse execution cap for this scan.
    pub max_execution_time_secs: u64,
}

/// One captured tool call that touched the queried file. Deserialized from a
/// `tool_io` ⋈ `events` row; aggregation into summaries, roots, and per-session
/// rollups happens in the MCP layer.
#[derive(Debug, Clone, Deserialize)]
pub struct FileAttentionTouch {
    pub session_id: String,
    pub event_uid: String,
    #[serde(default)]
    pub harness: String,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub tool_phase: String,
    /// `path_suffix` (a structured path key ends with the tail; high
    /// confidence) or `shell_path` (the tail appeared as a path-like token
    /// inside a shell `command` / `cmd`; lower confidence, no single resolvable
    /// path).
    #[serde(default)]
    pub match_kind: String,
    /// Best-effort absolute path that matched — the structured path for
    /// `path_suffix` matches, empty for substring matches.
    #[serde(default)]
    pub matched_path: String,
    /// Worktree root: the matched path with the repo-relative tail stripped.
    /// Empty when no clean absolute path was available (substring matches, or a
    /// path stored relative to its repo root).
    #[serde(default)]
    pub worktree_root: String,
    /// Session working directory recorded on the underlying event, if any.
    #[serde(default)]
    pub cwd: String,
    /// Event timestamp in unix milliseconds, using the same trace timestamp
    /// source as `open(event)`. `None` when the touch has no joinable trace row.
    #[serde(default)]
    pub event_unix_ms: Option<i64>,
    /// Transcript order from `v_conversation_trace`, used to break same-ms ties.
    #[serde(default)]
    pub event_order: u64,
    /// Parent turn sequence accepted by `open(turn:...)`.
    #[serde(default)]
    pub turn_seq: Option<u32>,
    #[serde(default)]
    pub input_preview: String,
    #[serde(default)]
    pub output_preview: String,
}

/// Restricts MCP retrieval to sessions whose origin working directory falls
/// under one of `roots`.
///
/// A session's origin is the first non-empty `cwd` / `workspacePath` value
/// found in its events' `payload_json` (in event order). A session matches
/// when its origin equals a root exactly or lives underneath it
/// (`startsWith(origin, root + "/")`). Sessions that never recorded a
/// working directory have no origin and never match.
///
/// When set on [`RepoConfig`], every MCP retrieval path enforces the scope:
/// `search_mcp_events`, `list_mcp_sessions`, `get_session_metadata`, and
/// `get_mcp_session` / `get_mcp_turn` / `get_mcp_event` (out-of-scope IDs
/// behave as not found). Non-MCP repository methods are not scoped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOriginScope {
    /// Absolute directory roots, without trailing slashes.
    pub roots: Vec<String>,
}

impl SessionOriginScope {
    /// Build a scope from raw root paths, trimming trailing slashes and
    /// dropping empty/relative entries. Returns `None` when nothing usable
    /// remains, so callers cannot accidentally construct an empty scope that
    /// matches nothing.
    pub fn from_roots<I, S>(roots: I) -> Option<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized: Vec<String> = Vec::new();
        for root in roots {
            let root = root.as_ref().trim();
            if !root.starts_with('/') {
                continue;
            }
            let trimmed = root.trim_end_matches('/');
            // "/" trims to empty; scoping the whole filesystem is meaningless
            // (it would only exclude origin-less sessions), so skip it.
            if trimmed.is_empty() {
                continue;
            }
            if !normalized.iter().any(|existing| existing == trimmed) {
                normalized.push(trimmed.to_string());
            }
        }
        if normalized.is_empty() {
            None
        } else {
            Some(Self { roots: normalized })
        }
    }
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
    #[serde(default)]
    pub session_slug: Option<String>,
    #[serde(default)]
    pub session_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadata {
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
    pub first_event_uid: String,
    pub last_event_uid: String,
    pub last_actor_role: String,
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
pub struct McpEventRef {
    pub session_id: String,
    pub event_uid: String,
    pub event_order: u64,
    pub turn_seq: u32,
    pub event_time: String,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTurnRef {
    pub session_id: String,
    pub turn_seq: u32,
    pub turn_id: String,
    pub started_at: String,
    pub ended_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpEventSummary {
    pub session_id: String,
    pub event_uid: String,
    pub event_order: u64,
    pub turn_seq: u32,
    pub event_time: String,
    pub actor_role: String,
    pub event_class: String,
    pub payload_type: String,
    pub event_type: String,
    pub call_id: String,
    pub name: String,
    pub phase: String,
    pub text_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTurnCompact {
    pub metadata: TurnSummary,
    pub user_input_summary: Option<String>,
    pub final_response_summary: Option<String>,
    pub user_input_event: Option<McpEventRef>,
    pub final_response_event: Option<McpEventRef>,
    pub tools_called: Vec<String>,
    pub normalized_event_types: Vec<String>,
    pub completed: bool,
    pub terminal_event_uid: Option<String>,
    pub first_event: Option<McpEventRef>,
    pub last_event: Option<McpEventRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSessionOpen {
    pub metadata: SessionMetadata,
    pub title: Option<String>,
    pub source: Option<String>,
    pub harness: Option<String>,
    pub inference_provider: Option<String>,
    pub session_slug: Option<String>,
    pub session_summary: Option<String>,
    pub turns: Vec<McpTurnCompact>,
    pub completed: bool,
    pub terminal_event_uid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSessionListItem {
    pub session_id: String,
    pub first_event_time: String,
    pub first_event_unix_ms: i64,
    pub last_event_time: String,
    pub last_event_unix_ms: i64,
    pub total_turns: u32,
    pub total_events: u64,
    pub mode: ConversationMode,
    pub completed: bool,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub session_slug: Option<String>,
    #[serde(default)]
    pub session_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTurnOpen {
    pub metadata: TurnSummary,
    pub events: Vec<McpEventSummary>,
    pub user_input_summary: Option<String>,
    pub final_response_summary: Option<String>,
    pub tools_called: Vec<String>,
    pub normalized_event_types: Vec<String>,
    pub completed: bool,
    pub terminal_event_uid: Option<String>,
    pub previous_turn: Option<McpTurnRef>,
    pub next_turn: Option<McpTurnRef>,
    pub first_event: Option<McpEventRef>,
    pub last_event: Option<McpEventRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpEventOpen {
    pub event: TraceEvent,
    pub event_type: String,
    pub event_ordinal: u32,
    pub turn_completed: bool,
    pub turn_terminal_event_uid: Option<String>,
    pub parent_session: SessionMetadata,
    pub parent_session_source: Option<String>,
    pub parent_turn: TurnSummary,
    pub previous_event: Option<McpEventRef>,
    pub next_event: Option<McpEventRef>,
    pub previous_turn: Option<McpTurnRef>,
    pub next_turn: Option<McpTurnRef>,
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
    pub endpoint_kind: String,
    pub token_usage_buckets: BTreeMap<String, u64>,
    pub token_usage_native_units: BTreeMap<String, f64>,
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
    pub endpoint_kind: String,
    pub token_usage_buckets: BTreeMap<String, u64>,
    pub token_usage_native_units: BTreeMap<String, f64>,
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
    pub session_ids: Option<Vec<String>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpEventType {
    UserInput,
    AssistantResponse,
    Reasoning,
    ToolCall,
    ToolResponse,
    Compaction,
    System,
    Runtime,
    Unknown,
}

impl McpEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserInput => "user_input",
            Self::AssistantResponse => "assistant_response",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool_call",
            Self::ToolResponse => "tool_response",
            Self::Compaction => "compaction",
            Self::System => "system",
            Self::Runtime => "runtime",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_normalized(value: &str) -> Self {
        match value {
            "user_input" => Self::UserInput,
            "assistant_response" => Self::AssistantResponse,
            "reasoning" => Self::Reasoning,
            "tool_call" => Self::ToolCall,
            "tool_response" => Self::ToolResponse,
            "compaction" => Self::Compaction,
            "system" => Self::System,
            "runtime" => Self::Runtime,
            _ => Self::Unknown,
        }
    }

    pub fn search_order(self) -> u8 {
        match self {
            Self::UserInput => 0,
            Self::AssistantResponse => 1,
            Self::Reasoning => 2,
            Self::ToolCall => 3,
            Self::ToolResponse => 4,
            Self::Compaction => 5,
            Self::System => 6,
            Self::Runtime => 7,
            Self::Unknown => 8,
        }
    }

    pub fn default_search_types() -> Vec<Self> {
        vec![Self::UserInput, Self::AssistantResponse, Self::ToolResponse]
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEventsDirection {
    #[default]
    Forward,
    Reverse,
}

impl SessionEventsDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Reverse => "reverse",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEventsQuery {
    pub session_id: String,
    #[serde(default)]
    pub direction: SessionEventsDirection,
    #[serde(default)]
    pub event_kinds: Option<Vec<SearchEventKind>>,
}

/// Search/list payloads should only expose richer content for user-facing events.
pub fn is_user_facing_content_event(event_class: &str, actor_role: &str) -> bool {
    !actor_role.eq_ignore_ascii_case("system")
        && matches!(event_class, "message" | "reasoning" | "event_msg")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventsStats {
    pub docs: u64,
    pub avgdl: f64,
    pub took_ms: u32,
    pub result_count: usize,
    pub requested_limit: u16,
    pub effective_limit: u16,
    pub limit_capped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventHit {
    pub rank: usize,
    pub event_uid: String,
    pub session_id: String,
    #[serde(default)]
    pub event_time: Option<String>,
    pub first_event_time: String,
    pub last_event_time: String,
    pub source_name: String,
    pub harness: String,
    pub inference_provider: String,
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
    pub text_content: Option<String>,
    pub payload_json: Option<String>,
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
pub struct SearchMcpEventsQuery {
    pub query: String,
    #[serde(default)]
    pub n_hits: Option<u16>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub turn_seq: Option<u32>,
    #[serde(default)]
    pub event_types: Option<Vec<McpEventType>>,
    #[serde(default)]
    pub min_score: Option<f64>,
    #[serde(default)]
    pub min_should_match: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMcpEventsStats {
    pub docs: u64,
    pub avgdl: f64,
    pub took_ms: u32,
    pub result_count: usize,
    pub requested_n_hits: u16,
    pub effective_n_hits: u16,
    pub limit_capped: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMcpEventHit {
    pub rank: usize,
    pub event_uid: String,
    pub session_id: String,
    pub event_type: McpEventType,
    pub event_time: String,
    pub event_unix_ms: i64,
    pub turn_seq: u32,
    pub turn_ordinal: u32,
    pub event_order: u64,
    pub event_ordinal: u32,
    pub turn_event_count: u64,
    pub session_started_at: Option<String>,
    pub session_updated_at: Option<String>,
    pub session_title: Option<String>,
    pub session_slug: Option<String>,
    pub session_summary: Option<String>,
    pub source_name: Option<String>,
    pub harness: Option<String>,
    pub inference_provider: Option<String>,
    pub event_class: String,
    pub payload_type: String,
    pub actor_role: String,
    pub tool_name: Option<String>,
    pub tool_phase: Option<String>,
    pub call_id: Option<String>,
    pub item_id: Option<String>,
    pub model: Option<String>,
    pub endpoint_kind: Option<String>,
    pub source_ref: Option<String>,
    pub snippet: String,
    pub snippet_truncated: bool,
    pub text_content: Option<String>,
    pub payload_json: Option<String>,
    pub score: f64,
    pub raw_score: f64,
    pub matched_terms: u64,
    pub doc_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMcpEventsResult {
    pub query_id: String,
    pub query: String,
    pub terms: Vec<String>,
    pub event_types: Vec<McpEventType>,
    pub truncated: bool,
    pub stats: SearchMcpEventsStats,
    pub hits: Vec<SearchMcpEventHit>,
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
    pub requested_limit: u16,
    pub effective_limit: u16,
    pub limit_capped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSearchHit {
    pub rank: usize,
    pub session_id: String,
    pub first_event_time: Option<String>,
    pub first_event_unix_ms: Option<i64>,
    pub last_event_time: Option<String>,
    pub last_event_unix_ms: Option<i64>,
    pub harness: Option<String>,
    pub inference_provider: Option<String>,
    pub session_slug: Option<String>,
    pub session_summary: Option<String>,
    pub score: f64,
    pub matched_terms: u16,
    pub event_count_considered: u32,
    pub best_event_uid: Option<String>,
    pub snippet: Option<String>,
    pub text_preview: Option<String>,
    pub text_content: Option<String>,
    pub payload_json: Option<String>,
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
pub struct SessionMetadataSearchQuery {
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
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadataSearchStats {
    pub requested_limit: u16,
    pub effective_limit: u16,
    pub limit_capped: bool,
    pub result_count: usize,
    pub took_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadataSearchHit {
    pub rank: usize,
    pub session_id: String,
    pub first_event_time: Option<String>,
    pub first_event_unix_ms: Option<i64>,
    pub last_event_time: Option<String>,
    pub last_event_unix_ms: Option<i64>,
    pub total_turns: Option<u32>,
    pub total_events: Option<u64>,
    pub user_messages: Option<u64>,
    pub assistant_messages: Option<u64>,
    pub tool_calls: Option<u64>,
    pub tool_results: Option<u64>,
    pub mode: Option<ConversationMode>,
    pub harness: Option<String>,
    pub inference_provider: Option<String>,
    pub session_slug: Option<String>,
    pub session_summary: Option<String>,
    pub meta_event_uid: Option<String>,
    pub score: f64,
    pub matched_terms: u16,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadataSearchResults {
    pub query_id: String,
    pub query: String,
    pub terms: Vec<String>,
    pub stats: SessionMetadataSearchStats,
    pub hits: Vec<SessionMetadataSearchHit>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenEventRequest {
    pub event_uid: String,
    #[serde(default)]
    pub before: Option<u16>,
    #[serde(default)]
    pub after: Option<u16>,
    #[serde(default)]
    pub include_system_events: Option<bool>,
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
    /// When set, MCP retrieval only sees sessions originating under these
    /// roots. See [`SessionOriginScope`].
    #[serde(default)]
    pub session_scope: Option<SessionOriginScope>,
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
            session_scope: None,
        }
    }
}

fn default_page_limit() -> u16 {
    50
}

#[cfg(test)]
mod tests {
    use super::SessionOriginScope;

    #[test]
    fn from_roots_normalizes_and_dedupes() {
        let scope =
            SessionOriginScope::from_roots(["/work/project/", "/work/project", "  /work/other  "])
                .expect("scope from valid roots");
        assert_eq!(scope.roots, vec!["/work/project", "/work/other"]);
    }

    #[test]
    fn from_roots_rejects_relative_root_and_bare_slash() {
        assert!(SessionOriginScope::from_roots(["relative/path", "", "/"]).is_none());
    }
}
