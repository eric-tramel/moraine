use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub source_name: Option<String>,
}

/// A single file-touch query for `file_attention`: every captured tool call
/// whose input path ends with `rel`, scoped and filtered per the request.
#[derive(Debug, Clone)]
pub struct FileAttentionQuery {
    /// Opaque cancellation token assigned by the caller so timed-out requests
    /// can cancel in-flight backend work.
    pub cancellation_token: String,
    /// Project-relative tail to suffix-match against captured file paths. The tail
    /// is what unifies the same logical file across worktree roots.
    pub rel: String,
    /// Canonical request-project identity used by both exact and fallback
    /// lookup. Git projects digest the common directory so linked worktrees
    /// agree; non-Git projects digest the exact canonical launch directory.
    /// `None` keeps project-scoped queries closed; only an explicit unscoped
    /// query may omit this boundary.
    pub normalized_project_id: Option<String>,
    /// Canonical registered roots for the request project. These safely
    /// admit rows written with the pre-digest project identity during the
    /// transition without widening to another repository sharing the backend.
    pub normalized_project_roots: Vec<String>,
    /// Whether the request path was proven to be one project-relative file
    /// and may therefore use structured legacy path/cwd evidence to recover a
    /// missing normalized root.
    pub derive_legacy_roots: bool,
    /// When true normalized request-project identity is enforced in both query
    /// paths. The repository's configured origin scope (`--project-only`) is an
    /// independent hard floor. When false (`scope:"all"`), only request-project
    /// narrowing is dropped.
    pub apply_project_scope: bool,
    pub start_unix_ms: Option<i64>,
    pub end_unix_ms: Option<i64>,
    /// Restrict to one normalized harness name; `None` matches every harness.
    pub harness: Option<String>,
    /// Restrict to one configured ingest source name; `None` matches every source.
    pub source_name: Option<String>,
    /// Restrict to one tool name (case-insensitive); `None` matches all tools.
    pub tool: Option<String>,
    /// Drop common pure-read touches.
    pub mutations_only: bool,
    /// Hard cap on matched rows returned by the backend. Summary, root, and
    /// per-session rollups are computed over this scanned set; the caller flags
    /// the result truncated when the cap is hit.
    pub max_rows: usize,
    /// Execution budget available to the backend for this scan.
    pub execution_budget_secs: u64,
}

/// One captured tool call that touched the queried file. Deserialized from a
/// canonical `events` row; aggregation into summaries, roots, and per-session
/// rollups happens in the MCP layer.
#[derive(Debug, Clone, Deserialize)]
pub struct FileAttentionTouch {
    pub session_id: String,
    pub event_uid: String,
    #[serde(default)]
    pub tool_call_id: String,
    #[serde(default)]
    pub harness: String,
    #[serde(default)]
    pub source_name: String,
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
    pub event_unix_ms: i64,
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
pub struct McpOpenSnapshot {
    pub slot: u8,
    pub generation: u64,
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
    #[serde(default)]
    pub snapshot: Option<McpOpenSnapshot>,
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
    pub harness: Option<String>,
    #[serde(default)]
    pub session_slug: Option<String>,
    #[serde(default)]
    pub session_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTurnOpen {
    pub metadata: TurnSummary,
    pub events: Vec<McpEventSummary>,
    #[serde(default)]
    pub parent_session_source: Option<String>,
    pub user_input_summary: Option<String>,
    pub final_response_summary: Option<String>,
    #[serde(default)]
    pub user_input_event: Option<McpEventRef>,
    #[serde(default)]
    pub final_response_event: Option<McpEventRef>,
    pub tools_called: Vec<String>,
    pub normalized_event_types: Vec<String>,
    pub completed: bool,
    pub terminal_event_uid: Option<String>,
    pub previous_turn: Option<McpTurnRef>,
    pub next_turn: Option<McpTurnRef>,
    pub first_event: Option<McpEventRef>,
    pub last_event: Option<McpEventRef>,
    #[serde(default)]
    pub snapshot: Option<McpOpenSnapshot>,
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
    pub event_unix_ms: i64,
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
    #[serde(default, rename = "disable_cache")]
    pub bypass_cache: Option<bool>,
    /// Preferred tradeoff for this search. Backends may treat this as a hint.
    #[serde(default, rename = "search_strategy")]
    pub strategy_hint: Option<SearchStrategyHint>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchStrategyHint {
    #[default]
    #[serde(rename = "optimized")]
    PreferPerformance,
    #[serde(rename = "oracle_exact")]
    Exact,
}

impl SearchStrategyHint {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreferPerformance => "optimized",
            Self::Exact => "oracle_exact",
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
    #[serde(skip)]
    pub cancellation_token: Option<String>,
    #[serde(default)]
    pub n_hits: Option<u16>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub turn_seq: Option<u32>,
    #[serde(default)]
    pub event_types: Option<Vec<McpEventType>>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub source_name: Option<String>,
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
    #[serde(default)]
    pub turn_completed: bool,
    #[serde(default)]
    pub turn_terminal_event_uid: Option<String>,
    pub session_started_at_unix_ms: Option<i64>,
    pub session_updated_at_unix_ms: Option<i64>,
    pub session_title: Option<String>,
    pub session_slug: Option<String>,
    pub session_summary: Option<String>,
    #[serde(default)]
    pub session_completed: bool,
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
    /// Whether a requested session/turn scope exists and is visible to this
    /// repository. Unscoped searches always report `true`.
    #[serde(default = "default_true")]
    pub scope_exists: bool,
    pub truncated: bool,
    pub stats: SearchMcpEventsStats,
    pub hits: Vec<SearchMcpEventHit>,
}

const fn default_true() -> bool {
    true
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnalyticsRange {
    #[serde(rename = "15m")]
    FifteenMinutes,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "6h")]
    SixHours,
    #[default]
    #[serde(rename = "24h")]
    TwentyFourHours,
    #[serde(rename = "7d")]
    SevenDays,
    #[serde(rename = "30d")]
    ThirtyDays,
}

impl AnalyticsRange {
    pub(crate) const ALL: [Self; 6] = [
        Self::FifteenMinutes,
        Self::OneHour,
        Self::SixHours,
        Self::TwentyFourHours,
        Self::SevenDays,
        Self::ThirtyDays,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::SixHours => "6h",
            Self::TwentyFourHours => "24h",
            Self::SevenDays => "7d",
            Self::ThirtyDays => "30d",
        }
    }

    pub(crate) const fn window_seconds(self) -> u32 {
        match self {
            Self::FifteenMinutes => 15 * 60,
            Self::OneHour => 60 * 60,
            Self::SixHours => 6 * 60 * 60,
            Self::TwentyFourHours => 24 * 60 * 60,
            Self::SevenDays => 7 * 24 * 60 * 60,
            Self::ThirtyDays => 30 * 24 * 60 * 60,
        }
    }

    pub(crate) const fn bucket_seconds(self) -> u32 {
        match self {
            Self::FifteenMinutes => 60,
            Self::OneHour => 5 * 60,
            Self::SixHours => 15 * 60,
            Self::TwentyFourHours => 60 * 60,
            Self::SevenDays => 6 * 60 * 60,
            Self::ThirtyDays => 24 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsWindow {
    pub range: AnalyticsRange,
    pub window_seconds: u32,
    pub bucket_seconds: u32,
    pub from_unix: u64,
    pub to_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsTokenPoint {
    pub bucket_unix: u64,
    pub model: String,
    pub endpoint_kind: String,
    pub bucket: String,
    pub tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsTurnPoint {
    pub bucket_unix: u64,
    pub model: String,
    pub turns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsConcurrencyPoint {
    pub bucket_unix: u64,
    pub concurrent_sessions: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsSnapshot {
    pub window: AnalyticsWindow,
    pub tokens: Vec<AnalyticsTokenPoint>,
    pub turns: Vec<AnalyticsTurnPoint>,
    pub concurrent_sessions: Vec<AnalyticsConcurrencyPoint>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionLookback {
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "24h")]
    TwentyFourHours,
    #[serde(rename = "7d")]
    SevenDays,
    #[default]
    #[serde(rename = "30d")]
    ThirtyDays,
    #[serde(rename = "90d")]
    NinetyDays,
    #[serde(rename = "all")]
    All,
}

impl SessionLookback {
    pub(crate) const fn window_seconds(self) -> Option<u32> {
        match self {
            Self::OneHour => Some(60 * 60),
            Self::SixHours => Some(6 * 60 * 60),
            Self::TwentyFourHours => Some(24 * 60 * 60),
            Self::SevenDays => Some(7 * 24 * 60 * 60),
            Self::ThirtyDays => Some(30 * 24 * 60 * 60),
            Self::NinetyDays => Some(90 * 24 * 60 * 60),
            Self::All => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAnalyticsQuery {
    #[serde(default)]
    pub lookback: SessionLookback,
    #[serde(default = "default_session_analytics_limit")]
    pub limit: u16,
}

impl Default for SessionAnalyticsQuery {
    fn default() -> Self {
        Self {
            lookback: SessionLookback::default(),
            limit: default_session_analytics_limit(),
        }
    }
}

impl SessionAnalyticsQuery {
    pub(crate) fn normalized_limit(&self) -> u16 {
        self.limit.clamp(1, 200)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub event_unix_ms: i64,
    pub text: String,
    pub latency_ms: u32,
    pub is_error: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionStep {
    User {
        event_unix_ms: i64,
        text: String,
    },
    Assistant {
        event_unix_ms: i64,
        text: String,
        endpoint_kind: String,
        latency_ms: Option<u32>,
        token_usage_buckets: BTreeMap<String, u64>,
        token_usage_native_units: BTreeMap<String, f64>,
    },
    Thinking {
        event_unix_ms: i64,
        text: String,
    },
    ToolCall {
        event_unix_ms: i64,
        tool_name: String,
        call_id: String,
        arguments: Value,
        latency_ms: Option<u32>,
        is_error: bool,
        result: Option<ToolResult>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurn {
    pub summary: TurnSummary,
    pub model: String,
    pub token_usage_buckets: BTreeMap<String, u64>,
    pub steps: Vec<SessionStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionAnalytics {
    pub summary: ConversationSummary,
    pub harness: String,
    pub source_name: String,
    pub models: Vec<String>,
    pub trace_id: String,
    pub first_user_text: String,
    pub turns: Vec<SessionTurn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebSearchEvent {
    pub event_time: String,
    pub harness: String,
    pub source_name: String,
    pub session_id: String,
    pub model: String,
    pub action: String,
    pub search_query: String,
    pub result_url: String,
    pub source_ref: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestHeartbeatRead {
    pub table_present: bool,
    pub latest: Option<IngestHeartbeat>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestHeartbeat {
    pub ts: String,
    pub ts_unix_ms: i64,
    pub host: String,
    pub service_version: String,
    pub queue_depth: u64,
    pub files_active: u32,
    pub files_watched: u32,
    pub rows_raw_written: u64,
    pub rows_events_written: u64,
    pub rows_errors_written: u64,
    pub flush_latency_ms: u32,
    pub append_to_visible_p50_ms: u32,
    pub append_to_visible_p95_ms: u32,
    pub last_error: String,
    #[serde(default)]
    pub watcher_backend: Option<String>,
    #[serde(default)]
    pub watcher_error_count: Option<u64>,
    #[serde(default)]
    pub watcher_reset_count: Option<u64>,
    #[serde(default)]
    pub watcher_last_reset_unix_ms: Option<u64>,
    #[serde(default)]
    pub backend_sinks: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<IngestProgressSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestProgressSnapshot {
    pub schema_version: u16,
    pub instance_id: String,
    pub run_started_unix_ms: u64,
    pub snapshot_unix_ms: u64,
    pub discovery_complete: bool,
    pub queue_capacity: u64,
    pub sink_pending_rows: u64,
    pub sink_pending_bytes: u64,
    pub sink_retrying: bool,
    pub oldest_pending_unix_ms: u64,
    pub last_durable_progress_unix_ms: u64,
    pub files_total: u64,
    pub files_completed: u64,
    pub bytes_total: u64,
    pub bytes_completed: u64,
    #[serde(default)]
    pub sources: Vec<IngestSourceProgress>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestSourceProgress {
    pub source_name: String,
    pub format: String,
    pub coverage_basis: IngestCoverageBasis,
    pub files_total: u64,
    pub files_completed: u64,
    pub bytes_total: u64,
    pub bytes_completed: u64,
    pub coverage_degraded: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestCoverageBasis {
    Bytes,
    Files,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestConditionType {
    Health,
    Coverage,
    Freshness,
    Readiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestConditionState {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestCondition {
    pub condition_type: IngestConditionType,
    pub state: IngestConditionState,
    pub reason: String,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestAlertCode {
    HeartbeatStale,
    ProgressStalled,
    QueueSaturated,
    SinkRetrying,
    CoverageDegraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestAlert {
    pub code: IngestAlertCode,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestRate {
    pub bytes_per_second: f64,
    pub sample_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestEta {
    pub scope: String,
    pub low_seconds: u64,
    pub high_seconds: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct IngestStatusRead {
    pub heartbeat: IngestHeartbeatRead,
    pub history: Vec<IngestHeartbeat>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestHistoryPoint {
    pub ts_unix_ms: i64,
    pub queue_depth: u64,
    pub files_active: u32,
    pub queue_capacity: u64,
    pub sink_pending_rows: u64,
    pub sink_retrying: bool,
    pub discovery_complete: bool,
    pub files_total: u64,
    pub files_completed: u64,
    pub bytes_total: u64,
    pub bytes_completed: u64,
}

impl IngestHistoryPoint {
    fn from_heartbeat(heartbeat: &IngestHeartbeat) -> Self {
        let (
            queue_capacity,
            sink_pending_rows,
            sink_retrying,
            discovery_complete,
            files_total,
            files_completed,
            bytes_total,
            bytes_completed,
        ) = match heartbeat.progress.as_ref() {
            Some(progress) => (
                progress.queue_capacity,
                progress.sink_pending_rows,
                progress.sink_retrying,
                progress.discovery_complete,
                progress.files_total,
                progress.files_completed,
                progress.bytes_total,
                progress.bytes_completed,
            ),
            None => (0, 0, false, false, 0, 0, 0, 0),
        };
        Self {
            ts_unix_ms: heartbeat.ts_unix_ms,
            queue_depth: heartbeat.queue_depth,
            files_active: heartbeat.files_active,
            queue_capacity,
            sink_pending_rows,
            sink_retrying,
            discovery_complete,
            files_total,
            files_completed,
            bytes_total,
            bytes_completed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestStatus {
    pub observed_at_unix_ms: i64,
    pub heartbeat: IngestHeartbeatRead,
    pub conditions: Vec<IngestCondition>,
    pub alerts: Vec<IngestAlert>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<IngestRate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta: Option<IngestEta>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<IngestHistoryPoint>,
}

impl IngestStatusRead {
    pub fn derive(self, now_unix_ms: i64) -> IngestStatus {
        let IngestStatusRead { heartbeat, history } = self;
        let latest = heartbeat.latest.as_ref();
        let health = heartbeat_health_condition(latest, now_unix_ms);
        let coverage = coverage_condition(latest, now_unix_ms);
        let freshness = freshness_condition(latest, &health, now_unix_ms);
        let readiness = readiness_condition(&health, &coverage, &freshness, now_unix_ms);
        let (rate, eta) = ingest_rate_and_eta(latest, &history);
        let alerts = ingest_alerts(
            latest,
            &history,
            &health,
            &coverage,
            &freshness,
            now_unix_ms,
        );
        let history_start_unix_ms =
            latest.map(|heartbeat| heartbeat.ts_unix_ms.saturating_sub(300_000));
        let history_end_unix_ms = latest.map(|heartbeat| heartbeat.ts_unix_ms);
        let mut history = history
            .iter()
            .filter(|heartbeat| {
                history_start_unix_ms
                    .zip(history_end_unix_ms)
                    .is_some_and(|(start, end)| (start..=end).contains(&heartbeat.ts_unix_ms))
            })
            .map(IngestHistoryPoint::from_heartbeat)
            .collect::<Vec<_>>();
        history.sort_by_key(|point| point.ts_unix_ms);
        if history.len() > 120 {
            let excess = history.len() - 120;
            drop(history.drain(..excess));
        }

        IngestStatus {
            observed_at_unix_ms: now_unix_ms,
            heartbeat,
            conditions: vec![health, coverage, freshness, readiness],
            alerts,
            rate,
            eta,
            history,
        }
    }
}

fn heartbeat_health_condition(
    latest: Option<&IngestHeartbeat>,
    now_unix_ms: i64,
) -> IngestCondition {
    let (state, reason) = match latest {
        None => (IngestConditionState::Unknown, "heartbeat_missing"),
        Some(latest) if latest.ts_unix_ms > now_unix_ms.saturating_add(5_000) => {
            (IngestConditionState::Unknown, "heartbeat_clock_skew")
        }
        Some(latest) if now_unix_ms.saturating_sub(latest.ts_unix_ms) > 30_000 => {
            (IngestConditionState::False, "heartbeat_stale")
        }
        Some(_) => (IngestConditionState::True, "heartbeat_recent"),
    };
    ingest_condition(IngestConditionType::Health, state, reason, now_unix_ms)
}

fn coverage_condition(latest: Option<&IngestHeartbeat>, now_unix_ms: i64) -> IngestCondition {
    let Some(progress) = latest.and_then(|heartbeat| heartbeat.progress.as_ref()) else {
        return ingest_condition(
            IngestConditionType::Coverage,
            IngestConditionState::Unknown,
            "progress_unavailable",
            now_unix_ms,
        );
    };
    if progress
        .sources
        .iter()
        .any(|source| source.coverage_degraded)
    {
        return ingest_condition(
            IngestConditionType::Coverage,
            IngestConditionState::False,
            "coverage_degraded",
            now_unix_ms,
        );
    }
    if !progress.discovery_complete {
        return ingest_condition(
            IngestConditionType::Coverage,
            IngestConditionState::False,
            "discovery_incomplete",
            now_unix_ms,
        );
    }
    if progress.files_completed < progress.files_total
        || progress.bytes_completed < progress.bytes_total
    {
        return ingest_condition(
            IngestConditionType::Coverage,
            IngestConditionState::False,
            "backfill_partial",
            now_unix_ms,
        );
    }
    ingest_condition(
        IngestConditionType::Coverage,
        IngestConditionState::True,
        "backfill_complete",
        now_unix_ms,
    )
}

fn freshness_condition(
    latest: Option<&IngestHeartbeat>,
    health: &IngestCondition,
    now_unix_ms: i64,
) -> IngestCondition {
    if health.state != IngestConditionState::True {
        return ingest_condition(
            IngestConditionType::Freshness,
            IngestConditionState::Unknown,
            health.reason.as_str(),
            now_unix_ms,
        );
    }
    let Some(latest) = latest else {
        unreachable!("healthy status requires a heartbeat")
    };
    let Some(progress) = latest.progress.as_ref() else {
        return ingest_condition(
            IngestConditionType::Freshness,
            IngestConditionState::Unknown,
            "progress_unavailable",
            now_unix_ms,
        );
    };
    let has_pressure = latest.queue_depth > 0
        || latest.files_active > 0
        || progress.sink_pending_rows > 0
        || progress.sink_retrying;
    let last_progress_unix_ms = progress
        .last_durable_progress_unix_ms
        .max(progress.run_started_unix_ms);
    if snapshot_work_remaining(progress)
        && has_pressure
        && now_unix_ms.saturating_sub(last_progress_unix_ms as i64) >= 60_000
    {
        return ingest_condition(
            IngestConditionType::Freshness,
            IngestConditionState::False,
            "progress_stalled",
            now_unix_ms,
        );
    }
    ingest_condition(
        IngestConditionType::Freshness,
        IngestConditionState::True,
        if has_pressure {
            "progress_recent"
        } else {
            "idle"
        },
        now_unix_ms,
    )
}

fn snapshot_work_remaining(progress: &IngestProgressSnapshot) -> bool {
    progress.files_completed < progress.files_total
        || progress.bytes_completed < progress.bytes_total
}

fn readiness_condition(
    health: &IngestCondition,
    coverage: &IngestCondition,
    freshness: &IngestCondition,
    now_unix_ms: i64,
) -> IngestCondition {
    let conditions = [health, coverage, freshness];
    if conditions
        .iter()
        .any(|condition| condition.state == IngestConditionState::False)
    {
        return ingest_condition(
            IngestConditionType::Readiness,
            IngestConditionState::False,
            "retrieval_may_be_incomplete",
            now_unix_ms,
        );
    }
    if conditions
        .iter()
        .any(|condition| condition.state == IngestConditionState::Unknown)
    {
        return ingest_condition(
            IngestConditionType::Readiness,
            IngestConditionState::Unknown,
            "readiness_unknown",
            now_unix_ms,
        );
    }
    ingest_condition(
        IngestConditionType::Readiness,
        IngestConditionState::True,
        "ready",
        now_unix_ms,
    )
}

fn ingest_condition(
    condition_type: IngestConditionType,
    state: IngestConditionState,
    reason: &str,
    observed_at_unix_ms: i64,
) -> IngestCondition {
    IngestCondition {
        condition_type,
        state,
        reason: reason.to_string(),
        observed_at_unix_ms,
    }
}

fn ingest_rate_and_eta(
    latest: Option<&IngestHeartbeat>,
    history: &[IngestHeartbeat],
) -> (Option<IngestRate>, Option<IngestEta>) {
    let Some(latest) = latest else {
        return (None, None);
    };
    let Some(latest_progress) = latest.progress.as_ref() else {
        return (None, None);
    };
    if !latest_progress.discovery_complete
        || latest_progress.sink_retrying
        || latest_progress.bytes_completed >= latest_progress.bytes_total
    {
        return (None, None);
    }

    let window_start = latest.ts_unix_ms.saturating_sub(300_000);
    let mut samples = history
        .iter()
        .filter(|heartbeat| {
            (window_start..=latest.ts_unix_ms).contains(&heartbeat.ts_unix_ms)
                && heartbeat
                    .progress
                    .as_ref()
                    .is_some_and(|progress| progress.instance_id == latest_progress.instance_id)
        })
        .collect::<Vec<_>>();
    samples.sort_by_key(|heartbeat| heartbeat.ts_unix_ms);
    if samples.len() < 6
        || samples.last().map(|sample| sample.ts_unix_ms) != Some(latest.ts_unix_ms)
        || samples.iter().any(|sample| {
            let progress = sample.progress.as_ref().expect("filtered progress");
            !progress.discovery_complete || progress.sink_retrying
        })
        || samples.iter().any(|sample| {
            !same_snapshot_target(
                sample.progress.as_ref().expect("filtered progress"),
                latest_progress,
            )
        })
        || samples.windows(2).any(|pair| {
            !snapshot_progress_is_monotonic(
                pair[0].progress.as_ref().expect("filtered progress"),
                pair[1].progress.as_ref().expect("filtered progress"),
            )
        })
    {
        return (None, None);
    }

    let first = samples[0];
    let latest_sample = *samples.last().expect("sample count checked");
    if latest_sample.ts_unix_ms.saturating_sub(first.ts_unix_ms) < 30_000 {
        return (None, None);
    }
    let short_start = latest_sample.ts_unix_ms.saturating_sub(60_000);
    let short_first = samples
        .iter()
        .copied()
        .find(|heartbeat| heartbeat.ts_unix_ms >= short_start)
        .expect("latest sample is in short window");
    let Some(long_rate) = sample_byte_rate(first, latest_sample) else {
        return (None, None);
    };
    let Some(short_rate) = sample_byte_rate(short_first, latest_sample) else {
        return (None, None);
    };
    let ratio = short_rate / long_rate;
    if !(0.5..=2.0).contains(&ratio) {
        return (None, None);
    }
    let sample_seconds = (latest_sample.ts_unix_ms.saturating_sub(first.ts_unix_ms) as u64) / 1_000;
    let remaining = latest_progress
        .bytes_total
        .saturating_sub(latest_progress.bytes_completed) as f64;
    let low_seconds = (remaining / short_rate.max(long_rate)).ceil() as u64;
    let high_seconds = (remaining / short_rate.min(long_rate)).ceil() as u64;
    (
        Some(IngestRate {
            bytes_per_second: long_rate,
            sample_seconds,
        }),
        Some(IngestEta {
            scope: "file_backfill".to_string(),
            low_seconds,
            high_seconds,
        }),
    )
}

fn same_snapshot_target(left: &IngestProgressSnapshot, right: &IngestProgressSnapshot) -> bool {
    left.instance_id == right.instance_id
        && left.snapshot_unix_ms == right.snapshot_unix_ms
        && left.files_total == right.files_total
        && left.bytes_total == right.bytes_total
        && left.sources.len() == right.sources.len()
        && left
            .sources
            .iter()
            .zip(&right.sources)
            .all(|(left, right)| {
                left.source_name == right.source_name
                    && left.format == right.format
                    && left.coverage_basis == right.coverage_basis
                    && left.files_total == right.files_total
                    && left.bytes_total == right.bytes_total
            })
}

fn snapshot_progress_is_monotonic(
    previous: &IngestProgressSnapshot,
    current: &IngestProgressSnapshot,
) -> bool {
    same_snapshot_target(previous, current)
        && previous.files_completed <= current.files_completed
        && previous.bytes_completed <= current.bytes_completed
        && previous
            .sources
            .iter()
            .zip(&current.sources)
            .all(|(previous, current)| {
                previous.files_completed <= current.files_completed
                    && previous.bytes_completed <= current.bytes_completed
            })
}

fn sample_byte_rate(first: &IngestHeartbeat, last: &IngestHeartbeat) -> Option<f64> {
    let first_progress = first.progress.as_ref()?;
    let last_progress = last.progress.as_ref()?;
    let elapsed_ms = last.ts_unix_ms.checked_sub(first.ts_unix_ms)?;
    let completed = last_progress
        .bytes_completed
        .checked_sub(first_progress.bytes_completed)?;
    if elapsed_ms <= 0 || completed == 0 {
        return None;
    }
    Some(completed as f64 / (elapsed_ms as f64 / 1_000.0))
}

fn ingest_alerts(
    latest: Option<&IngestHeartbeat>,
    history: &[IngestHeartbeat],
    health: &IngestCondition,
    coverage: &IngestCondition,
    freshness: &IngestCondition,
    now_unix_ms: i64,
) -> Vec<IngestAlert> {
    let mut alerts = Vec::new();
    if health.state == IngestConditionState::False {
        alerts.push(ingest_alert(IngestAlertCode::HeartbeatStale, now_unix_ms));
    }
    if freshness.reason == "progress_stalled" {
        alerts.push(ingest_alert(IngestAlertCode::ProgressStalled, now_unix_ms));
    }
    if coverage.reason == "coverage_degraded" {
        alerts.push(ingest_alert(IngestAlertCode::CoverageDegraded, now_unix_ms));
    }
    if health.state == IngestConditionState::True {
        let Some(latest) = latest else {
            unreachable!("healthy status requires a heartbeat")
        };
        let Some(instance_id) = latest
            .progress
            .as_ref()
            .map(|progress| progress.instance_id.as_str())
        else {
            return alerts;
        };
        let window_start = latest.ts_unix_ms.saturating_sub(300_000);
        let mut same_instance = history
            .iter()
            .filter(|heartbeat| {
                (window_start..=latest.ts_unix_ms).contains(&heartbeat.ts_unix_ms)
                    && heartbeat
                        .progress
                        .as_ref()
                        .is_some_and(|progress| progress.instance_id == instance_id)
            })
            .collect::<Vec<_>>();
        same_instance.sort_by_key(|heartbeat| heartbeat.ts_unix_ms);
        if same_instance.last().map(|heartbeat| heartbeat.ts_unix_ms) != Some(latest.ts_unix_ms) {
            return alerts;
        }
        if same_instance.len() >= 3
            && same_instance.iter().rev().take(3).all(|heartbeat| {
                heartbeat.progress.as_ref().is_some_and(|progress| {
                    progress.queue_capacity > 0 && heartbeat.queue_depth >= progress.queue_capacity
                })
            })
        {
            alerts.push(ingest_alert(IngestAlertCode::QueueSaturated, now_unix_ms));
        }
        if same_instance.len() >= 2
            && same_instance.iter().rev().take(2).all(|heartbeat| {
                heartbeat
                    .progress
                    .as_ref()
                    .is_some_and(|progress| progress.sink_retrying)
            })
        {
            alerts.push(ingest_alert(IngestAlertCode::SinkRetrying, now_unix_ms));
        }
    }
    alerts
}

fn ingest_alert(code: IngestAlertCode, observed_at_unix_ms: i64) -> IngestAlert {
    IngestAlert {
        code,
        observed_at_unix_ms,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableSummaries {
    pub tables: Vec<TableSummary>,
    pub row_counts_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSummary {
    pub name: String,
    pub engine: String,
    pub is_temporary: bool,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePreviewQuery {
    pub table: String,
    #[serde(default = "default_table_preview_limit")]
    pub limit: u16,
}

impl TablePreviewQuery {
    pub(crate) fn normalized_limit(&self) -> u16 {
        self.limit.clamp(1, 500)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableColumn {
    pub name: String,
    pub type_name: String,
    pub default_expression: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TablePreview {
    pub table: String,
    pub limit: u16,
    pub schema: Vec<TableColumn>,
    pub rows: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreProbe<T> {
    Available(T),
    Failed { message: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreConnectionMetrics {
    pub total: u64,
    pub tcp: u64,
    pub http: u64,
    pub mysql: u64,
    pub postgres: u64,
    pub interserver: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreHealth {
    /// Successful ping latency in milliseconds.
    pub ping: StoreProbe<f64>,
    pub version: StoreProbe<String>,
    pub database_exists: StoreProbe<bool>,
    pub connections: StoreProbe<StoreConnectionMetrics>,
}

impl Default for StoreHealth {
    fn default() -> Self {
        Self {
            ping: StoreProbe::Failed {
                message: "ping probe not configured".to_string(),
            },
            version: StoreProbe::Failed {
                message: "version probe not configured".to_string(),
            },
            database_exists: StoreProbe::Failed {
                message: "database-existence probe not configured".to_string(),
            },
            connections: StoreProbe::Failed {
                message: "connection-metrics probe not configured".to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreDiagnostics {
    pub healthy: bool,
    pub version: Option<String>,
    pub database: String,
    pub database_exists: bool,
    pub applied_schema_versions: Vec<String>,
    pub pending_schema_versions: Vec<String>,
    pub missing_tables: Vec<String>,
    pub errors: Vec<String>,
}

fn default_session_analytics_limit() -> u16 {
    50
}

fn default_table_preview_limit() -> u16 {
    25
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
    use super::{
        AnalyticsRange, IngestAlertCode, IngestConditionState, IngestCoverageBasis, IngestEta,
        IngestHeartbeat, IngestHeartbeatRead, IngestProgressSnapshot, IngestSourceProgress,
        IngestStatusRead, SearchEventKind, SearchEventsQuery, SearchStrategyHint,
        SessionAnalyticsQuery, SessionLookback, SessionOriginScope, SessionStep, TablePreviewQuery,
    };

    #[test]
    fn search_events_query_preserves_wire_contract() {
        const WIRE_JSON: &str = r#"{"query":"needle","source":"mcp","limit":7,"session_id":"session-a","session_ids":["session-a","session-b"],"min_score":0.25,"min_should_match":2,"include_tool_events":true,"event_kinds":["message","tool_call"],"exclude_codex_mcp":false,"disable_cache":true,"search_strategy":"optimized"}"#;

        let query: SearchEventsQuery =
            serde_json::from_str(WIRE_JSON).expect("deserialize existing MCP query contract");

        assert_eq!(query.bypass_cache, Some(true));
        assert_eq!(
            query.strategy_hint,
            Some(SearchStrategyHint::PreferPerformance)
        );
        assert_eq!(
            query.event_kinds.as_deref(),
            Some(&[SearchEventKind::Message, SearchEventKind::ToolCall][..])
        );
        assert_eq!(
            serde_json::to_string(&query).expect("serialize MCP query contract"),
            WIRE_JSON
        );
    }

    #[test]
    fn search_strategy_hint_preserves_wire_values() {
        for (hint, wire_value) in [
            (SearchStrategyHint::PreferPerformance, r#""optimized""#),
            (SearchStrategyHint::Exact, r#""oracle_exact""#),
        ] {
            assert_eq!(
                serde_json::to_string(&hint).expect("serialize strategy hint"),
                wire_value
            );
            assert_eq!(
                serde_json::from_str::<SearchStrategyHint>(wire_value)
                    .expect("deserialize strategy hint"),
                hint
            );
        }
    }

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
    #[test]
    fn analytics_ranges_have_exact_wire_and_window_mappings() {
        let expected = [
            (AnalyticsRange::FifteenMinutes, "15m", 900, 60),
            (AnalyticsRange::OneHour, "1h", 3_600, 300),
            (AnalyticsRange::SixHours, "6h", 21_600, 900),
            (AnalyticsRange::TwentyFourHours, "24h", 86_400, 3_600),
            (AnalyticsRange::SevenDays, "7d", 604_800, 21_600),
            (AnalyticsRange::ThirtyDays, "30d", 2_592_000, 86_400),
        ];

        assert_eq!(
            AnalyticsRange::ALL.map(AnalyticsRange::as_str),
            ["15m", "1h", "6h", "24h", "7d", "30d"]
        );
        for (range, wire, window_seconds, bucket_seconds) in expected {
            assert_eq!(range.as_str(), wire);
            assert_eq!(range.window_seconds(), window_seconds);
            assert_eq!(range.bucket_seconds(), bucket_seconds);
            assert_eq!(
                serde_json::to_string(&range).expect("serialize analytics range"),
                format!(r#""{wire}""#)
            );
            assert_eq!(
                serde_json::from_str::<AnalyticsRange>(&format!(r#""{wire}""#))
                    .expect("deserialize analytics range"),
                range
            );
        }
        assert_eq!(AnalyticsRange::default(), AnalyticsRange::TwentyFourHours);
    }

    #[test]
    fn session_lookbacks_have_exact_windows_and_default() {
        for (lookback, wire, window_seconds) in [
            (SessionLookback::OneHour, "1h", Some(3_600)),
            (SessionLookback::SixHours, "6h", Some(21_600)),
            (SessionLookback::TwentyFourHours, "24h", Some(86_400)),
            (SessionLookback::SevenDays, "7d", Some(604_800)),
            (SessionLookback::ThirtyDays, "30d", Some(2_592_000)),
            (SessionLookback::NinetyDays, "90d", Some(7_776_000)),
            (SessionLookback::All, "all", None),
        ] {
            assert_eq!(lookback.window_seconds(), window_seconds);
            assert_eq!(
                serde_json::to_string(&lookback).expect("serialize session lookback"),
                format!(r#""{wire}""#)
            );
        }
        assert_eq!(SessionLookback::default(), SessionLookback::ThirtyDays);
    }

    #[test]
    fn analytics_query_and_table_preview_limits_are_normalized() {
        let mut sessions = SessionAnalyticsQuery::default();
        assert_eq!(sessions.limit, 50);
        assert_eq!(sessions.normalized_limit(), 50);
        sessions.limit = 0;
        assert_eq!(sessions.normalized_limit(), 1);
        sessions.limit = u16::MAX;
        assert_eq!(sessions.normalized_limit(), 200);

        let mut preview = TablePreviewQuery {
            table: "events".to_string(),
            limit: 0,
        };
        assert_eq!(preview.normalized_limit(), 1);
        preview.limit = u16::MAX;
        assert_eq!(preview.normalized_limit(), 500);
    }

    #[test]
    fn session_steps_use_a_stable_kind_tag() {
        let value = serde_json::to_value(SessionStep::User {
            event_unix_ms: 123,
            text: "hello".to_string(),
        })
        .expect("serialize typed session step");

        assert_eq!(
            value,
            serde_json::json!({
                "kind": "user",
                "event_unix_ms": 123,
                "text": "hello",
            })
        );
    }
    fn heartbeat_with_progress(
        ts_unix_ms: i64,
        bytes_completed: u64,
        bytes_total: u64,
    ) -> IngestHeartbeat {
        IngestHeartbeat {
            ts: String::new(),
            ts_unix_ms,
            host: "host-a".to_string(),
            service_version: "test".to_string(),
            queue_depth: u64::from(bytes_completed < bytes_total),
            files_active: u32::from(bytes_completed < bytes_total),
            files_watched: 1,
            rows_raw_written: 0,
            rows_events_written: 0,
            rows_errors_written: 0,
            flush_latency_ms: 0,
            append_to_visible_p50_ms: 0,
            append_to_visible_p95_ms: 0,
            last_error: String::new(),
            watcher_backend: None,
            watcher_error_count: None,
            watcher_reset_count: None,
            watcher_last_reset_unix_ms: None,
            backend_sinks: None,
            progress: Some(IngestProgressSnapshot {
                schema_version: 1,
                instance_id: "run-a".to_string(),
                run_started_unix_ms: 1_000_000,
                snapshot_unix_ms: 1_000_000,
                discovery_complete: true,
                queue_capacity: 1_024,
                sink_pending_rows: 0,
                sink_pending_bytes: 0,
                sink_retrying: false,
                oldest_pending_unix_ms: 0,
                last_durable_progress_unix_ms: ts_unix_ms as u64,
                files_total: 1,
                files_completed: u64::from(bytes_completed >= bytes_total),
                bytes_total,
                bytes_completed,
                sources: vec![IngestSourceProgress {
                    source_name: "codex".to_string(),
                    format: "jsonl".to_string(),
                    coverage_basis: IngestCoverageBasis::Bytes,
                    files_total: 1,
                    files_completed: u64::from(bytes_completed >= bytes_total),
                    bytes_total,
                    bytes_completed,
                    coverage_degraded: false,
                }],
            }),
        }
    }

    #[test]
    fn status_separates_health_from_partial_coverage() {
        let latest = heartbeat_with_progress(1_060_000, 500, 1_000);
        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest.clone()),
            },
            history: vec![latest],
        }
        .derive(1_061_000);

        assert_eq!(
            status.conditions[0].state,
            IngestConditionState::True,
            "recent heartbeat remains healthy"
        );
        assert_eq!(status.conditions[1].reason, "backfill_partial");
        assert_eq!(status.conditions[3].reason, "retrieval_may_be_incomplete");
    }

    #[test]
    fn completed_file_based_coverage_is_ready_without_alert() {
        let mut latest = heartbeat_with_progress(1_060_000, 0, 0);
        let progress = latest.progress.as_mut().expect("progress snapshot");
        let source = progress.sources.first_mut().expect("source progress");
        source.source_name = "cursor-sqlite".to_string();
        source.format = "cursor_sqlite".to_string();
        source.coverage_basis = IngestCoverageBasis::Files;

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest.clone()),
            },
            history: vec![latest],
        }
        .derive(1_061_000);

        assert_eq!(status.conditions[1].state, IngestConditionState::True);
        assert_eq!(status.conditions[1].reason, "backfill_complete");
        assert_eq!(status.conditions[3].state, IngestConditionState::True);
        assert_eq!(status.conditions[3].reason, "ready");
        assert!(status.alerts.is_empty());
    }

    #[test]
    fn stable_durable_history_produces_bounded_eta() {
        let history = (0..=6)
            .map(|sample| {
                heartbeat_with_progress(
                    1_000_000 + sample * 10_000,
                    100 + sample as u64 * 100,
                    1_000,
                )
            })
            .collect::<Vec<_>>();
        let latest = history.last().cloned().expect("latest sample");
        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest),
            },
            history,
        }
        .derive(1_061_000);

        assert_eq!(
            status.rate.as_ref().map(|rate| rate.bytes_per_second),
            Some(10.0)
        );
        assert_eq!(
            status.eta,
            Some(IngestEta {
                scope: "file_backfill".to_string(),
                low_seconds: 30,
                high_seconds: 30,
            })
        );
    }

    #[test]
    fn eta_requires_six_samples_inside_the_five_minute_window() {
        let history = [
            (600_000, 100),
            (620_000, 200),
            (640_000, 300),
            (680_000, 400),
            (950_000, 500),
            (1_000_000, 600),
        ]
        .into_iter()
        .map(|(ts, completed)| heartbeat_with_progress(ts, completed, 1_000))
        .collect::<Vec<_>>();
        let latest = history.last().cloned().expect("latest sample");

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest),
            },
            history,
        }
        .derive(1_001_000);

        assert!(status.rate.is_none());
        assert!(status.eta.is_none());
        assert_eq!(status.history.len(), 2);
    }

    #[test]
    fn eta_rejects_adjacent_completion_and_target_regressions() {
        let mutations: [fn(&mut IngestHeartbeat); 2] = [
            |sample: &mut IngestHeartbeat| {
                let progress = sample.progress.as_mut().expect("progress");
                progress.bytes_completed = 150;
                progress.sources[0].bytes_completed = 150;
            },
            |sample: &mut IngestHeartbeat| {
                let progress = sample.progress.as_mut().expect("progress");
                progress.bytes_total = 900;
                progress.sources[0].bytes_total = 900;
            },
        ];
        for mutate in mutations {
            let mut history = (0..=6)
                .map(|sample| {
                    heartbeat_with_progress(
                        1_000_000 + sample * 10_000,
                        100 + sample as u64 * 100,
                        1_000,
                    )
                })
                .collect::<Vec<_>>();
            mutate(&mut history[3]);
            let latest = history.last().cloned().expect("latest sample");

            let status = IngestStatusRead {
                heartbeat: IngestHeartbeatRead {
                    table_present: true,
                    latest: Some(latest),
                },
                history,
            }
            .derive(1_061_000);

            assert!(status.rate.is_none());
            assert!(status.eta.is_none());
        }
    }

    #[test]
    fn completed_snapshot_with_live_work_is_not_stalled() {
        let mut latest = heartbeat_with_progress(1_060_000, 1_000, 1_000);
        latest.queue_depth = 1;
        latest.files_active = 1;
        latest
            .progress
            .as_mut()
            .expect("progress")
            .last_durable_progress_unix_ms = 1_000_000;

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest.clone()),
            },
            history: vec![latest],
        }
        .derive(1_061_000);

        assert_eq!(status.conditions[2].reason, "progress_recent");
        assert!(!status
            .alerts
            .iter()
            .any(|alert| alert.code == IngestAlertCode::ProgressStalled));
    }

    #[test]
    fn current_heartbeat_emits_sustained_live_pressure_alerts() {
        let history = (0..3)
            .map(|sample| {
                let mut heartbeat =
                    heartbeat_with_progress(1_000_000 + sample * 10_000, 100, 1_000);
                heartbeat.queue_depth = 1_024;
                heartbeat.progress.as_mut().expect("progress").sink_retrying = true;
                heartbeat
            })
            .collect::<Vec<_>>();
        let latest = history.last().cloned().expect("latest sample");

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest),
            },
            history,
        }
        .derive(1_021_000);

        assert!(status
            .alerts
            .iter()
            .any(|alert| alert.code == IngestAlertCode::QueueSaturated));
        assert!(status
            .alerts
            .iter()
            .any(|alert| alert.code == IngestAlertCode::SinkRetrying));
    }

    #[test]
    fn live_pressure_alerts_observe_hold_counts_and_recovery() {
        let mut history = (0..2)
            .map(|sample| {
                let mut heartbeat =
                    heartbeat_with_progress(1_000_000 + sample * 10_000, 100, 1_000);
                heartbeat.queue_depth = 1_024;
                heartbeat.progress.as_mut().expect("progress").sink_retrying = true;
                heartbeat
            })
            .collect::<Vec<_>>();
        let held_latest = history.last().cloned().expect("held latest");
        let held = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(held_latest),
            },
            history: history.clone(),
        }
        .derive(1_011_000);
        assert!(!held
            .alerts
            .iter()
            .any(|alert| alert.code == IngestAlertCode::QueueSaturated));
        assert!(held
            .alerts
            .iter()
            .any(|alert| alert.code == IngestAlertCode::SinkRetrying));

        let recovered = heartbeat_with_progress(1_020_000, 200, 1_000);
        history.push(recovered.clone());
        let recovered = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(recovered),
            },
            history,
        }
        .derive(1_021_000);
        assert!(!recovered.alerts.iter().any(|alert| {
            matches!(
                alert.code,
                IngestAlertCode::QueueSaturated | IngestAlertCode::SinkRetrying
            )
        }));
    }
    #[test]
    fn stale_heartbeat_suppresses_live_pressure_alerts() {
        let history = (0..3)
            .map(|sample| {
                let mut heartbeat =
                    heartbeat_with_progress(1_000_000 + sample * 10_000, 100, 1_000);
                heartbeat.queue_depth = 1_024;
                heartbeat.progress.as_mut().expect("progress").sink_retrying = true;
                heartbeat
            })
            .collect::<Vec<_>>();
        let latest = history.last().cloned().expect("latest sample");

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest),
            },
            history,
        }
        .derive(1_051_000);

        assert!(status
            .alerts
            .iter()
            .any(|alert| alert.code == IngestAlertCode::HeartbeatStale));
        assert!(!status.alerts.iter().any(|alert| {
            matches!(
                alert.code,
                IngestAlertCode::QueueSaturated | IngestAlertCode::SinkRetrying
            )
        }));
    }

    #[test]
    fn derived_history_is_a_narrow_serializable_projection() {
        let mut latest = heartbeat_with_progress(1_060_000, 500, 1_000);
        latest.last_error = "/private/source/path".to_string();
        latest.backend_sinks = Some(serde_json::json!({"credential": "secret"}));
        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest.clone()),
            },
            history: vec![latest],
        }
        .derive(1_061_000);

        assert_eq!(
            serde_json::to_value(&status.history[0]).expect("serialize history point"),
            serde_json::json!({
                "ts_unix_ms": 1_060_000,

                "queue_depth": 1,
                "files_active": 1,
                "queue_capacity": 1_024,
                "sink_pending_rows": 0,
                "sink_retrying": false,
                "discovery_complete": true,
                "files_total": 1,
                "files_completed": 0,
                "bytes_total": 1_000,
                "bytes_completed": 500,
            })
        );
    }

    #[test]
    fn derived_history_preserves_missing_progress_as_a_gap() {
        let mut gap = heartbeat_with_progress(1_050_000, 400, 1_000);
        gap.queue_depth = 7;
        gap.files_active = 2;
        gap.progress = None;
        let latest = heartbeat_with_progress(1_060_000, 500, 1_000);

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest.clone()),
            },
            history: vec![gap, latest],
        }
        .derive(1_061_000);

        assert_eq!(status.history.len(), 2);
        let gap = &status.history[0];
        assert_eq!(gap.ts_unix_ms, 1_050_000);
        assert_eq!(gap.queue_depth, 7);
        assert_eq!(gap.files_active, 2);
        assert_eq!(gap.queue_capacity, 0);
        assert!(!gap.discovery_complete);
        assert_eq!(gap.files_total, 0);
        assert_eq!(gap.bytes_total, 0);
    }
    #[test]
    fn derived_history_retains_at_most_120_points() {
        let history = (0..130)
            .map(|sample| {
                heartbeat_with_progress(1_000_000 + sample * 1_000, 100 + sample as u64, 1_000)
            })
            .collect::<Vec<_>>();
        let latest = history.last().cloned().expect("latest sample");

        let status = IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest: Some(latest),
            },
            history,
        }
        .derive(1_130_000);

        assert_eq!(status.history.len(), 120);
        assert_eq!(status.history[0].ts_unix_ms, 1_010_000);
        assert_eq!(status.history[119].ts_unix_ms, 1_129_000);
    }
}
