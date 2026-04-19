use anyhow::{anyhow, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_conversations::{
    is_user_facing_content_event, ClickHouseConversationRepository, ConversationDetailOptions,
    ConversationListFilter, ConversationListSort, ConversationMode, ConversationRepository,
    ConversationSearchQuery, ConversationSearchResults, OpenEventRequest, PageRequest, RepoConfig,
    RepoError, SearchEventKind, SearchEventsQuery, SearchEventsResult, SessionEventsDirection,
    SessionEventsQuery, TurnListFilter,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, warn};

const TOOL_LIMIT_MIN: u16 = 1;

const CONVERSATION_MODE_CLASSIFICATION_SEMANTICS: &str =
    "Sessions are classified into exactly one mode by first match on any event in the session: web_search > mcp_internal > tool_calling > chat.";

const SEARCH_CONVERSATIONS_MODE_DOC: &str =
    "Optional `mode` filters by that computed session mode. Mode meanings: web_search=any web search activity (`web_search_call`, `search_results_received`, or `tool_use` with WebSearch/WebFetch); mcp_internal=any Codex MCP internal search/open activity (`source_name='codex-mcp'` or tool_name `search`/`open`) when web_search does not match; tool_calling=any tool activity (`tool_call`, `tool_result`, or `tool_use`) when neither higher mode matches; chat=none of the above.";

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Verbosity {
    #[default]
    Prose,
    Full,
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize)]
struct SearchArgs {
    query: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    min_should_match: Option<u16>,
    #[serde(default)]
    include_tool_events: Option<bool>,
    #[serde(default, alias = "event_kinds", alias = "kind", alias = "kinds")]
    event_kind: Option<SearchEventKindsArg>,
    #[serde(default)]
    exclude_codex_mcp: Option<bool>,
    #[serde(default)]
    include_payload_json: Option<bool>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SearchEventKindsArg {
    One(SearchEventKind),
    Many(Vec<SearchEventKind>),
}

impl SearchEventKindsArg {
    fn into_vec(self) -> Vec<SearchEventKind> {
        match self {
            Self::One(kind) => vec![kind],
            Self::Many(kinds) => kinds,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SearchConversationsArgs {
    query: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    min_score: Option<f64>,
    #[serde(default)]
    min_should_match: Option<u16>,
    #[serde(default)]
    from_unix_ms: Option<i64>,
    #[serde(default)]
    to_unix_ms: Option<i64>,
    #[serde(default)]
    mode: Option<ConversationMode>,
    #[serde(default)]
    include_tool_events: Option<bool>,
    #[serde(default)]
    exclude_codex_mcp: Option<bool>,
    #[serde(default)]
    include_payload_json: Option<bool>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Default, Deserialize)]
struct ListSessionsArgs {
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    from_unix_ms: Option<i64>,
    #[serde(default)]
    to_unix_ms: Option<i64>,
    #[serde(default)]
    mode: Option<ConversationMode>,
    #[serde(default)]
    sort: Option<ConversationListSort>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Deserialize)]
struct GetSessionArgs {
    session_id: String,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Deserialize)]
struct OpenArgs {
    #[serde(default)]
    event_uid: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    scope: Option<OpenScope>,
    #[serde(default)]
    include_payload: Option<OpenPayloadArg>,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    before: Option<u16>,
    #[serde(default)]
    after: Option<u16>,
    #[serde(default)]
    include_system_events: Option<bool>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum OpenScope {
    #[default]
    All,
    Messages,
    Events,
    Turns,
}

impl OpenScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Messages => "messages",
            Self::Events => "events",
            Self::Turns => "turns",
        }
    }

    fn include_events(self) -> bool {
        matches!(self, Self::All | Self::Messages | Self::Events)
    }

    fn include_turns(self) -> bool {
        matches!(self, Self::All | Self::Turns)
    }

    fn messages_only(self) -> bool {
        matches!(self, Self::Messages)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OpenPayloadField {
    Text,
    PayloadJson,
}

impl OpenPayloadField {
    fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::PayloadJson => "payload_json",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum OpenPayloadArg {
    One(OpenPayloadField),
    Many(Vec<OpenPayloadField>),
}

impl OpenPayloadArg {
    fn into_vec(self) -> Vec<OpenPayloadField> {
        match self {
            Self::One(field) => vec![field],
            Self::Many(fields) => fields,
        }
    }
}

#[derive(Debug, Deserialize)]
struct GetSessionEventsArgs {
    session_id: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    direction: Option<SessionEventsDirection>,
    #[serde(default, alias = "event_kinds", alias = "kind", alias = "kinds")]
    event_kind: Option<SearchEventKindsArg>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Default, Deserialize)]
struct SearchProsePayload {
    #[serde(default)]
    query_id: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    stats: SearchProseStats,
    #[serde(default)]
    hits: Vec<SearchProseHit>,
}

#[derive(Debug, Default, Deserialize)]
struct SearchProseStats {
    #[serde(default)]
    took_ms: u64,
    #[serde(default)]
    result_count: u64,
    #[serde(default)]
    requested_limit: Option<u16>,
    #[serde(default)]
    effective_limit: Option<u16>,
    #[serde(default)]
    limit_capped: bool,
}

#[derive(Debug, Default, Deserialize)]
struct SearchProseHit {
    #[serde(default)]
    rank: u64,
    #[serde(default)]
    event_uid: String,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    first_event_time: String,
    #[serde(default)]
    last_event_time: String,
    #[serde(default)]
    score: f64,
    #[serde(default)]
    event_class: String,
    #[serde(default)]
    payload_type: String,
    #[serde(default)]
    actor_role: String,
    #[serde(default)]
    text_preview: String,
}

#[derive(Debug, Default, Deserialize)]
struct OpenProsePayload {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    event_uid: String,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    turn_seq: u32,
    #[serde(default)]
    target_event_order: u64,
    #[serde(default)]
    before: u16,
    #[serde(default)]
    after: u16,
    #[serde(default)]
    events: Vec<OpenProseEvent>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenProseEvent {
    #[serde(default)]
    is_target: bool,
    #[serde(default)]
    event_order: u64,
    #[serde(default)]
    actor_role: String,
    #[serde(default)]
    event_class: String,
    #[serde(default)]
    payload_type: String,
    #[serde(default)]
    text_content: String,
}

#[derive(Debug, Default, Deserialize)]
struct OpenSessionProsePayload {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    include_system_events: bool,
    #[serde(default)]
    include_payload: Vec<String>,
    #[serde(default)]
    limit: u16,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    next_cursor: Option<String>,
    #[serde(default)]
    summary: Option<OpenSessionProseSummary>,
    #[serde(default)]
    turns: Vec<OpenSessionProseTurn>,
    #[serde(default)]
    events: Vec<OpenSessionProseEvent>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenSessionProseSummary {
    #[serde(default)]
    start_time: String,
    #[serde(default)]
    start_unix_ms: i64,
    #[serde(default)]
    end_time: String,
    #[serde(default)]
    end_unix_ms: i64,
    #[serde(default)]
    event_count: u64,
    #[serde(default)]
    turn_count: u32,
}

#[derive(Debug, Default, Deserialize)]
struct OpenSessionProseTurn {
    #[serde(default)]
    turn_seq: u32,
    #[serde(default)]
    started_at: String,
    #[serde(default)]
    ended_at: String,
    #[serde(default)]
    event_count: u64,
}

#[derive(Debug, Default, Deserialize)]
struct OpenSessionProseEvent {
    #[serde(default)]
    event_order: u64,
    #[serde(default)]
    actor_role: String,
    #[serde(default)]
    event_class: String,
    #[serde(default)]
    payload_type: String,
    #[serde(default)]
    text_content: String,
    #[serde(default)]
    payload_json: String,
}

#[derive(Debug, Default, Deserialize)]
struct ConversationSearchProsePayload {
    #[serde(default)]
    query_id: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    stats: ConversationSearchProseStats,
    #[serde(default)]
    hits: Vec<ConversationSearchProseHit>,
}

#[derive(Debug, Default, Deserialize)]
struct ConversationSearchProseStats {
    #[serde(default)]
    took_ms: u64,
    #[serde(default)]
    result_count: u64,
    #[serde(default)]
    requested_limit: Option<u16>,
    #[serde(default)]
    effective_limit: Option<u16>,
    #[serde(default)]
    limit_capped: bool,
}

#[derive(Debug, Default, Deserialize)]
struct ConversationSearchProseHit {
    #[serde(default)]
    rank: u64,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    first_event_time: Option<String>,
    #[serde(default)]
    first_event_unix_ms: Option<i64>,
    #[serde(default)]
    last_event_time: Option<String>,
    #[serde(default)]
    last_event_unix_ms: Option<i64>,
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    inference_provider: Option<String>,
    #[serde(default)]
    session_slug: Option<String>,
    #[serde(default)]
    session_summary: Option<String>,
    #[serde(default)]
    score: f64,
    #[serde(default)]
    matched_terms: u16,
    #[serde(default)]
    event_count_considered: u32,
    #[serde(default)]
    best_event_uid: Option<String>,
    #[serde(default)]
    snippet: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SessionListProsePayload {
    #[serde(default)]
    sessions: Vec<SessionListProseSession>,
    #[serde(default)]
    next_cursor: Option<String>,
    #[serde(default)]
    sort: String,
}

#[derive(Debug, Default, Deserialize)]
struct SessionEventsProsePayload {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    direction: String,
    #[serde(default)]
    events: Vec<SessionEventsProseEvent>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SessionEventsProseEvent {
    #[serde(default)]
    event_uid: String,
    #[serde(default)]
    event_order: u64,
    #[serde(default)]
    turn_seq: u32,
    #[serde(default)]
    event_time: String,
    #[serde(default)]
    actor_role: String,
    #[serde(default)]
    event_class: String,
    #[serde(default)]
    payload_type: String,
    #[serde(default)]
    source_ref: String,
    #[serde(default)]
    text_content: String,
}

#[derive(Debug, Default, Deserialize)]
struct SessionListProseSession {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    start_time: String,
    #[serde(default)]
    start_unix_ms: i64,
    #[serde(default)]
    end_time: String,
    #[serde(default)]
    end_unix_ms: i64,
    #[serde(default)]
    event_count: u64,
    #[serde(default)]
    mode: String,
}

#[derive(Debug, Default, Deserialize)]
struct GetSessionProsePayload {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    session: Option<GetSessionProseSession>,
    #[serde(default)]
    error: Option<GetSessionProseError>,
}

#[derive(Debug, Default, Deserialize)]
struct GetSessionProseSession {
    #[serde(default)]
    first_event_time: String,
    #[serde(default)]
    first_event_unix_ms: i64,
    #[serde(default)]
    last_event_time: String,
    #[serde(default)]
    last_event_unix_ms: i64,
    #[serde(default)]
    total_events: u64,
    #[serde(default)]
    total_turns: u32,
    #[serde(default)]
    user_messages: u64,
    #[serde(default)]
    assistant_messages: u64,
    #[serde(default)]
    tool_calls: u64,
    #[serde(default)]
    tool_results: u64,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    first_event_uid: String,
    #[serde(default)]
    last_event_uid: String,
    #[serde(default)]
    last_actor_role: String,
}

#[derive(Debug, Default, Deserialize)]
struct GetSessionProseError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Clone)]
struct AppState {
    cfg: AppConfig,
    repo: ClickHouseConversationRepository,
    prewarm_started: Arc<AtomicBool>,
}

impl AppState {
    async fn handle_request(&self, req: RpcRequest) -> Option<Value> {
        let id = req.id.clone();

        match req.method.as_str() {
            "initialize" => {
                if self
                    .prewarm_started
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    if let Err(err) = self.repo.prewarm_mcp_search_state_quick().await {
                        warn!("mcp quick prewarm failed: {}", err);
                    } else {
                        debug!("mcp quick prewarm completed");
                    }

                    let repo = self.repo.clone();
                    tokio::spawn(async move {
                        if let Err(err) = repo.prewarm_mcp_search_state().await {
                            warn!("mcp prewarm failed: {}", err);
                        } else {
                            debug!("mcp prewarm completed");
                        }
                    });
                }

                let result = json!({
                    "protocolVersion": self.cfg.mcp.protocol_version,
                    "capabilities": {
                        "tools": {
                            "listChanged": false
                        }
                    },
                    "serverInfo": {
                        "name": "codex-mcp",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                });

                id.map(|msg_id| rpc_ok(msg_id, result))
            }
            "ping" => id.map(|msg_id| rpc_ok(msg_id, json!({}))),
            "notifications/initialized" | "initialized" => None,
            "tools/list" => id.map(|msg_id| rpc_ok(msg_id, self.tools_list_result())),
            "tools/call" => {
                let msg_id = id?;

                let parsed: Result<ToolCallParams> =
                    serde_json::from_value(req.params).context("invalid tools/call params payload");

                match parsed {
                    Ok(params) => {
                        let tool_result = match self.call_tool(params).await {
                            Ok(value) => value,
                            Err(err) => tool_error_result(err.to_string()),
                        };
                        Some(rpc_ok(msg_id, tool_result))
                    }
                    Err(err) => Some(rpc_err(msg_id, -32602, &format!("invalid params: {err}"))),
                }
            }
            _ => id.map(|msg_id| {
                rpc_err(msg_id, -32601, &format!("method not found: {}", req.method))
            }),
        }
    }

    fn tools_list_result(&self) -> Value {
        let (limit_min, limit_max) = tool_limit_bounds(self.cfg.mcp.max_results);
        json!({
            "tools": [
                {
                    "name": "search",
                    "description": "BM25 lexical search over Moraine indexed conversation events. Bag-of-words ranking: no phrase matching, no stemming. Word order does not matter.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "limit": { "type": "integer", "minimum": limit_min, "maximum": limit_max },
                            "session_id": { "type": "string" },
                            "min_score": { "type": "number" },
                            "min_should_match": { "type": "integer", "minimum": 1, "description": "Minimum number of query terms that must match. Values exceeding the number of query terms are clamped." },
                            "include_tool_events": { "type": "boolean" },
                            "event_kind": {
                                "oneOf": [
                                    {
                                        "type": "string",
                                        "enum": ["message", "reasoning", "tool_call", "tool_result"]
                                    },
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "string",
                                            "enum": ["message", "reasoning", "tool_call", "tool_result"]
                                        }
                                    }
                                ]
                            },
                            "exclude_codex_mcp": { "type": "boolean" },
                            "include_payload_json": {
                                "type": "boolean",
                                "default": false,
                                "description": "Include truncated payload_json for user-facing message events."
                            },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["query"]
                    }
                },
                {
                    "name": "open",
                    "description": "Open by `event_uid` with surrounding context, or open a session transcript by `session_id`. Callers must supply exactly one of `event_uid` or `session_id`.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "event_uid": { "type": "string" },
                            "session_id": { "type": "string" },
                            "scope": {
                                "type": "string",
                                "enum": ["all", "messages", "events", "turns"],
                                "default": "all"
                            },
                            "include_payload": {
                                "oneOf": [
                                    {
                                        "type": "string",
                                        "enum": ["text", "payload_json"]
                                    },
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "string",
                                            "enum": ["text", "payload_json"]
                                        }
                                    }
                                ]
                            },
                            "limit": { "type": "integer", "minimum": limit_min, "maximum": limit_max },
                            "cursor": { "type": "string" },
                            "before": { "type": "integer", "minimum": 0 },
                            "after": { "type": "integer", "minimum": 0 },
                            "include_system_events": { "type": "boolean", "default": false },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        }
                    }
                },
                {
                    "name": "search_conversations",
                    "description": format!(
                        "BM25 lexical search across whole conversations. {CONVERSATION_MODE_CLASSIFICATION_SEMANTICS} {SEARCH_CONVERSATIONS_MODE_DOC}"
                    ),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "limit": { "type": "integer", "minimum": limit_min, "maximum": limit_max },
                            "min_score": { "type": "number" },
                            "min_should_match": { "type": "integer", "minimum": 1, "description": "Minimum number of query terms that must match. Values exceeding the number of query terms are clamped." },
                            "from_unix_ms": { "type": "integer" },
                            "to_unix_ms": { "type": "integer" },
                            "mode": {
                                "type": "string",
                                "enum": ["web_search", "mcp_internal", "tool_calling", "chat"],
                                "description": SEARCH_CONVERSATIONS_MODE_DOC
                            },
                            "include_tool_events": { "type": "boolean" },
                            "exclude_codex_mcp": { "type": "boolean" },
                            "include_payload_json": {
                                "type": "boolean",
                                "default": false,
                                "description": "Include truncated payload_json for the best event per hit when user-facing."
                            },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["query"]
                    }
                },
                {
                    "name": "list_sessions",
                    "description": "List session metadata in a time window without requiring a search query.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "limit": { "type": "integer", "minimum": 1, "maximum": self.cfg.mcp.max_results },
                            "cursor": { "type": "string" },
                            "from_unix_ms": { "type": "integer" },
                            "to_unix_ms": { "type": "integer" },
                            "mode": {
                                "type": "string",
                                "enum": ["web_search", "mcp_internal", "tool_calling", "chat"],
                                "description": SEARCH_CONVERSATIONS_MODE_DOC
                            },
                            "sort": {
                                "type": "string",
                                "enum": ["asc", "desc"],
                                "default": "desc",
                                "description": "Sort by session end time then session_id. Use `desc` for newest-first or `asc` for oldest-first. Cursor tokens are deterministic for a fixed filter + sort."
                            },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        }
                    }
                },
                {
                    "name": "get_session",
                    "description": "Fetch stable metadata for one summarized session by session_id without loading full event history. Returns found=false when the session is absent from session summary metadata.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": { "type": "string" },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["session_id"]
                    }
                },
                {
                    "name": "get_session_events",
                    "description": "Fetch an ordered timeline of events for one session with deterministic pagination. Results follow `direction` (`forward` = chronological, `reverse` = newest-first).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": { "type": "string" },
                            "limit": { "type": "integer", "minimum": limit_min, "maximum": limit_max },
                            "cursor": { "type": "string" },
                            "direction": {
                                "type": "string",
                                "enum": ["forward", "reverse"],
                                "default": "forward"
                            },
                            "event_kind": {
                                "oneOf": [
                                    {
                                        "type": "string",
                                        "enum": ["message", "reasoning", "tool_call", "tool_result"]
                                    },
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "string",
                                            "enum": ["message", "reasoning", "tool_call", "tool_result"]
                                        }
                                    }
                                ]
                            },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["session_id"]
                    }
                }
            ]
        })
    }

    async fn call_tool(&self, params: ToolCallParams) -> Result<Value> {
        match params.name.as_str() {
            "search" => {
                let mut args: SearchArgs = serde_json::from_value(params.arguments)
                    .context("search expects a JSON object with at least {\"query\": ...}")?;
                args.limit = validate_tool_limit("search", args.limit, self.cfg.mcp.max_results)?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.search(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_search_prose(&payload)?)),
                }
            }
            "open" => {
                let mut args: OpenArgs = serde_json::from_value(params.arguments)
                    .context("open expects one of {\"event_uid\": ...} or {\"session_id\": ...}")?;
                args.limit = validate_tool_limit("open", args.limit, self.cfg.mcp.max_results)?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.open(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_open_prose(&payload)?)),
                }
            }
            "search_conversations" => {
                let mut args: SearchConversationsArgs = serde_json::from_value(params.arguments)
                    .context(
                        "search_conversations expects a JSON object with at least {\"query\": ...}",
                    )?;
                args.limit = validate_tool_limit(
                    "search_conversations",
                    args.limit,
                    self.cfg.mcp.max_results,
                )?;
                let verbosity = args.verbosity.unwrap_or_default();
                let mode = args.mode;
                let payload = self.search_conversations(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_conversation_search_prose(
                        &payload, mode,
                    )?)),
                }
            }
            "list_sessions" => {
                let mut args: ListSessionsArgs = if params.arguments.is_null() {
                    ListSessionsArgs::default()
                } else {
                    serde_json::from_value(params.arguments)
                        .context("list_sessions expects a JSON object with optional filters")?
                };
                args.limit =
                    validate_tool_limit("list_sessions", args.limit, self.cfg.mcp.max_results)?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.list_sessions(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_session_list_prose(&payload)?)),
                }
            }
            "get_session" => {
                let args: GetSessionArgs = serde_json::from_value(params.arguments)
                    .context("get_session expects {\"session_id\": ...}")?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.get_session(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_get_session_prose(&payload)?)),
                }
            }
            "get_session_events" => {
                let mut args: GetSessionEventsArgs = serde_json::from_value(params.arguments)
                    .context("get_session_events expects {\"session_id\": ...}")?;
                args.limit = validate_tool_limit(
                    "get_session_events",
                    args.limit,
                    self.cfg.mcp.max_results,
                )?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.get_session_events(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_session_events_prose(&payload)?)),
                }
            }
            other => Err(anyhow!("unknown tool: {other}")),
        }
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        let include_payload_json = args.include_payload_json.unwrap_or(false);
        let mut result = self
            .repo
            .search_events(SearchEventsQuery {
                query: args.query,
                source: Some("moraine-mcp".to_string()),
                limit: args.limit,
                session_id: args.session_id,
                min_score: args.min_score,
                min_should_match: args.min_should_match,
                include_tool_events: args.include_tool_events,
                event_kinds: args.event_kind.map(SearchEventKindsArg::into_vec),
                exclude_codex_mcp: args.exclude_codex_mcp,
                disable_cache: None,
                search_strategy: None,
            })
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        apply_search_content_policy(&mut result, include_payload_json);
        serde_json::to_value(result).context("failed to encode search result payload")
    }

    async fn open(&self, args: OpenArgs) -> Result<Value> {
        let event_uid = args
            .event_uid
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let session_id = args
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);

        match (event_uid, session_id) {
            (Some(_), Some(_)) => Err(anyhow!(
                "open expects exactly one of event_uid or session_id"
            )),
            (None, None) => Err(anyhow!("open expects one of event_uid or session_id")),
            (Some(event_uid), None) => self.open_by_event_uid(event_uid, args).await,
            (None, Some(session_id)) => self.open_by_session_id(session_id, args).await,
        }
    }

    async fn open_by_event_uid(&self, event_uid: String, args: OpenArgs) -> Result<Value> {
        let result = self
            .repo
            .open_event(OpenEventRequest {
                event_uid,
                before: args.before,
                after: args.after,
                include_system_events: args.include_system_events,
            })
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        if !result.found {
            return Ok(json!({
                "found": false,
                "event_uid": result.event_uid,
                "events": [],
            }));
        }

        serde_json::to_value(result).context("failed to encode open result payload")
    }

    async fn open_by_session_id(&self, session_id: String, args: OpenArgs) -> Result<Value> {
        let scope = args.scope.unwrap_or_default();
        let include_system_events = args.include_system_events.unwrap_or(false);
        let include_payload_fields = args
            .include_payload
            .map(OpenPayloadArg::into_vec)
            .unwrap_or_default();
        let include_text = include_payload_fields.contains(&OpenPayloadField::Text);
        let include_payload_json = include_payload_fields.contains(&OpenPayloadField::PayloadJson);
        let limit = args.limit.unwrap_or(self.cfg.mcp.max_results);
        let cursor = args.cursor;

        let Some(conversation) = self
            .repo
            .get_conversation(
                &session_id,
                ConversationDetailOptions {
                    include_turns: false,
                },
            )
            .await
            .map_err(|err| anyhow!(err.to_string()))?
        else {
            return Ok(json!({
                "open_mode": "session",
                "found": false,
                "session_id": session_id,
                "scope": scope.as_str(),
                "events": [],
                "turns": [],
                "next_cursor": Value::Null,
            }));
        };

        let turn_page = self
            .repo
            .list_turns(
                &session_id,
                TurnListFilter::default(),
                PageRequest {
                    limit,
                    cursor: cursor.clone(),
                },
            )
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        let turns = turn_page
            .items
            .iter()
            .map(|turn| {
                json!({
                    "turn_seq": turn.turn_seq,
                    "started_at": turn.started_at,
                    "started_at_unix_ms": turn.started_at_unix_ms,
                    "ended_at": turn.ended_at,
                    "ended_at_unix_ms": turn.ended_at_unix_ms,
                    "event_count": turn.total_events,
                    "user_messages": turn.user_messages,
                    "assistant_messages": turn.assistant_messages,
                    "tool_calls": turn.tool_calls,
                    "tool_results": turn.tool_results,
                })
            })
            .collect::<Vec<_>>();

        let events = if scope.include_events() {
            let mut session_events = Vec::new();
            for turn in &turn_page.items {
                let Some(turn_detail) = self
                    .repo
                    .get_turn(&session_id, turn.turn_seq)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?
                else {
                    continue;
                };

                for event in turn_detail.events {
                    if !include_system_events
                        && is_low_information_system_event(&event.actor_role, &event.payload_type)
                    {
                        continue;
                    }
                    if scope.messages_only() && !event.event_class.eq_ignore_ascii_case("message") {
                        continue;
                    }

                    let mut event_payload = json!({
                        "event_uid": event.event_uid,
                        "event_order": event.event_order,
                        "turn_seq": event.turn_seq,
                        "event_time": event.event_time,
                        "actor_role": event.actor_role,
                        "event_class": event.event_class,
                        "payload_type": event.payload_type,
                    });
                    if include_text {
                        event_payload["text_content"] = Value::String(event.text_content);
                    }
                    if include_payload_json {
                        event_payload["payload_json"] = Value::String(event.payload_json);
                    }
                    session_events.push(event_payload);
                }
            }
            session_events
        } else {
            Vec::new()
        };

        let include_payload = include_payload_fields
            .into_iter()
            .map(OpenPayloadField::as_str)
            .collect::<Vec<_>>();

        Ok(json!({
            "open_mode": "session",
            "found": true,
            "session_id": session_id,
            "scope": scope.as_str(),
            "include_system_events": include_system_events,
            "include_payload": include_payload,
            "limit": limit,
            "cursor": cursor,
            "next_cursor": turn_page.next_cursor,
            "summary": {
                "start_time": conversation.summary.first_event_time,
                "start_unix_ms": conversation.summary.first_event_unix_ms,
                "end_time": conversation.summary.last_event_time,
                "end_unix_ms": conversation.summary.last_event_unix_ms,
                "event_count": conversation.summary.total_events,
                "turn_count": conversation.summary.total_turns,
            },
            "turns": if scope.include_turns() { turns } else { Vec::new() },
            "events": events,
        }))
    }

    async fn search_conversations(&self, args: SearchConversationsArgs) -> Result<Value> {
        let include_payload_json = args.include_payload_json.unwrap_or(false);
        let mut result = self
            .repo
            .search_conversations(ConversationSearchQuery {
                query: args.query,
                limit: args.limit,
                min_score: args.min_score,
                min_should_match: args.min_should_match,
                from_unix_ms: args.from_unix_ms,
                to_unix_ms: args.to_unix_ms,
                mode: args.mode,
                include_tool_events: args.include_tool_events,
                exclude_codex_mcp: args.exclude_codex_mcp,
            })
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        apply_conversation_search_content_policy(&mut result, include_payload_json);
        serde_json::to_value(result).context("failed to encode search_conversations result payload")
    }

    async fn list_sessions(&self, args: ListSessionsArgs) -> Result<Value> {
        let ListSessionsArgs {
            limit,
            cursor,
            from_unix_ms,
            to_unix_ms,
            mode,
            sort,
            verbosity: _,
        } = args;
        let sort = sort.unwrap_or_default();

        let page = self
            .repo
            .list_conversations(
                ConversationListFilter {
                    from_unix_ms,
                    to_unix_ms,
                    mode,
                    sort,
                },
                PageRequest {
                    limit: limit.unwrap_or(self.cfg.mcp.max_results),
                    cursor,
                },
            )
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        let sessions = page
            .items
            .into_iter()
            .map(|summary| {
                json!({
                    "session_id": summary.session_id,
                    "start_time": summary.first_event_time,
                    "start_unix_ms": summary.first_event_unix_ms,
                    "end_time": summary.last_event_time,
                    "end_unix_ms": summary.last_event_unix_ms,
                    "event_count": summary.total_events,
                    "turn_count": summary.total_turns,
                    "user_messages": summary.user_messages,
                    "assistant_messages": summary.assistant_messages,
                    "tool_calls": summary.tool_calls,
                    "tool_results": summary.tool_results,
                    "mode": summary.mode.as_str(),
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "from_unix_ms": from_unix_ms,
            "to_unix_ms": to_unix_ms,
            "mode": mode.map(ConversationMode::as_str),
            "sort": sort.as_str(),
            "sessions": sessions,
            "next_cursor": page.next_cursor,
        }))
    }

    fn build_session_events_payload(
        session_id: String,
        direction: SessionEventsDirection,
        event_kinds: Option<Vec<SearchEventKind>>,
        events: Vec<Value>,
        next_cursor: Option<String>,
    ) -> Value {
        let event_kinds = event_kinds.map(|kinds| {
            kinds
                .into_iter()
                .map(SearchEventKind::as_str)
                .collect::<Vec<_>>()
        });

        json!({
            "session_id": session_id,
            "direction": direction.as_str(),
            "event_kinds": event_kinds,
            "events": events,
            "next_cursor": next_cursor,
        })
    }

    fn build_get_session_error_payload(
        session_id: String,
        code: &'static str,
        message: impl Into<String>,
    ) -> Value {
        json!({
            "found": false,
            "session_id": session_id,
            "error": {
                "code": code,
                "message": message.into(),
            }
        })
    }

    async fn get_session_events(&self, args: GetSessionEventsArgs) -> Result<Value> {
        let GetSessionEventsArgs {
            session_id,
            limit,
            cursor,
            direction,
            event_kind,
            verbosity: _,
        } = args;

        let direction = direction.unwrap_or_default();
        let event_kinds = event_kind.map(SearchEventKindsArg::into_vec);
        let page = self
            .repo
            .list_session_events(
                SessionEventsQuery {
                    session_id: session_id.clone(),
                    direction,
                    event_kinds: event_kinds.clone(),
                },
                PageRequest {
                    limit: limit.unwrap_or(self.cfg.mcp.max_results),
                    cursor,
                },
            )
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        let events = page
            .items
            .into_iter()
            .map(|event| {
                json!({
                    "event_uid": event.event_uid,
                    "event_order": event.event_order,
                    "turn_seq": event.turn_seq,
                    "event_time": event.event_time,
                    "actor_role": event.actor_role,
                    "event_class": event.event_class,
                    "payload_type": event.payload_type,
                    "call_id": event.call_id,
                    "name": event.name,
                    "phase": event.phase,
                    "item_id": event.item_id,
                    "source_ref": event.source_ref,
                    "text_content": event.text_content,
                    "payload_json": event.payload_json,
                    "token_usage_json": event.token_usage_json,
                })
            })
            .collect::<Vec<_>>();

        Ok(Self::build_session_events_payload(
            session_id,
            direction,
            event_kinds,
            events,
            page.next_cursor,
        ))
    }

    fn build_get_session_payload(
        session_id: String,
        result: Result<Option<moraine_conversations::SessionMetadata>, RepoError>,
    ) -> Result<Value> {
        match result {
            Ok(Some(session)) => Ok(json!({
                "found": true,
                "session_id": session.session_id,
                "session": {
                    "session_id": session.session_id,
                    "first_event_time": session.first_event_time,
                    "first_event_unix_ms": session.first_event_unix_ms,
                    "last_event_time": session.last_event_time,
                    "last_event_unix_ms": session.last_event_unix_ms,
                    "total_events": session.total_events,
                    "total_turns": session.total_turns,
                    "user_messages": session.user_messages,
                    "assistant_messages": session.assistant_messages,
                    "tool_calls": session.tool_calls,
                    "tool_results": session.tool_results,
                    "mode": session.mode.as_str(),
                    "first_event_uid": session.first_event_uid,
                    "last_event_uid": session.last_event_uid,
                    "last_actor_role": session.last_actor_role,
                },
            })),
            Ok(None) => Ok(Self::build_get_session_error_payload(
                session_id,
                "not_found",
                "session_id was not found",
            )),
            Err(RepoError::InvalidArgument(message)) => Ok(Self::build_get_session_error_payload(
                session_id,
                "invalid_argument",
                message,
            )),
            Err(err) => Err(anyhow!(err.to_string())),
        }
    }

    async fn get_session(&self, args: GetSessionArgs) -> Result<Value> {
        let session_id = args.session_id;
        let result = self.repo.get_session_metadata(&session_id).await;
        Self::build_get_session_payload(session_id, result)
    }
}

fn tool_limit_bounds(max_results: u16) -> (u16, u16) {
    (TOOL_LIMIT_MIN, max_results.max(TOOL_LIMIT_MIN))
}

fn validate_tool_limit(
    tool_name: &str,
    limit: Option<u16>,
    max_results: u16,
) -> Result<Option<u16>> {
    let (min, max) = tool_limit_bounds(max_results);
    match limit {
        Some(value) if !(min..=max).contains(&value) => Err(anyhow!(
            "{tool_name} limit must be between {min} and {max} (received {value})"
        )),
        _ => Ok(limit),
    }
}

fn apply_search_content_policy(result: &mut SearchEventsResult, include_payload_json: bool) {
    for hit in &mut result.hits {
        if !is_user_facing_content_event(&hit.event_class, &hit.actor_role) {
            hit.text_content = None;
            hit.payload_json = None;
            continue;
        }

        if !include_payload_json {
            hit.payload_json = None;
        }
    }
}

fn apply_conversation_search_content_policy(
    result: &mut ConversationSearchResults,
    include_payload_json: bool,
) {
    for hit in &mut result.hits {
        if !include_payload_json {
            hit.payload_json = None;
        }
    }
}

fn rpc_ok(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn rpc_err(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn tool_ok_full(payload: Value) -> Value {
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string());
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": payload,
        "isError": false
    })
}

fn tool_ok_prose(text: String) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "isError": false
    })
}

fn tool_error_result(message: String) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    })
}

fn format_search_prose(payload: &Value) -> Result<String> {
    let parsed: SearchProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse search payload")?;

    let mut out = String::new();
    out.push_str(&format!("Search: \"{}\"\n", parsed.query));
    out.push_str(&format!("Query ID: {}\n", parsed.query_id));
    out.push_str(&format!(
        "Hits: {} ({} ms)\n",
        parsed.stats.result_count, parsed.stats.took_ms
    ));
    if let Some(limit_summary) = format_limit_summary(
        parsed.stats.requested_limit,
        parsed.stats.effective_limit,
        parsed.stats.limit_capped,
    ) {
        out.push_str(&format!("Limit: {limit_summary}\n"));
    }

    if parsed.hits.is_empty() {
        out.push_str("\nNo hits.");
        return Ok(out);
    }

    for hit in &parsed.hits {
        let kind = display_kind(&hit.event_class, &hit.payload_type);
        let recency = if hit.last_event_time.is_empty() {
            String::new()
        } else {
            format!(" last_event_time={}", hit.last_event_time)
        };
        out.push_str(&format!(
            "\n{}) session={} score={:.4} kind={} role={}{}\n",
            hit.rank, hit.session_id, hit.score, kind, hit.actor_role, recency
        ));
        if !hit.first_event_time.is_empty() && !hit.last_event_time.is_empty() {
            out.push_str(&format!(
                "   session_window: {} -> {}\n",
                hit.first_event_time, hit.last_event_time
            ));
        }

        let snippet = compact_text_line(&hit.text_preview, 220);
        if !snippet.is_empty() {
            out.push_str(&format!("   snippet: {}\n", snippet));
        }

        out.push_str(&format!("   event_uid: {}\n", hit.event_uid));
        out.push_str(&format!("   next: open(event_uid=\"{}\")\n", hit.event_uid));
    }

    Ok(out.trim_end().to_string())
}

fn format_open_prose(payload: &Value) -> Result<String> {
    if payload.get("open_mode").and_then(Value::as_str) == Some("session") {
        return format_open_session_prose(payload);
    }

    let mut parsed: OpenProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse open payload")?;

    let mut out = String::new();
    out.push_str(&format!("Open event: {}\n", parsed.event_uid));

    if !parsed.found {
        out.push_str("Not found.");
        return Ok(out);
    }

    out.push_str(&format!("Session: {}\n", parsed.session_id));
    out.push_str(&format!("Turn: {}\n", parsed.turn_seq));
    out.push_str(&format!(
        "Context window: before={} after={}\n",
        parsed.before, parsed.after
    ));

    parsed.events.sort_by_key(|e| e.event_order);

    let mut before_events = Vec::new();
    let mut target_events = Vec::new();
    let mut after_events = Vec::new();

    for event in parsed.events {
        if event.is_target || event.event_order == parsed.target_event_order {
            target_events.push(event);
        } else if event.event_order < parsed.target_event_order {
            before_events.push(event);
        } else {
            after_events.push(event);
        }
    }

    out.push_str("\nBefore:\n");
    if before_events.is_empty() {
        out.push_str("- (none)\n");
    } else {
        for event in &before_events {
            append_open_event_line(&mut out, event);
        }
    }

    out.push_str("\nTarget:\n");
    if target_events.is_empty() {
        out.push_str("- (none)\n");
    } else {
        for event in &target_events {
            append_open_event_line(&mut out, event);
        }
    }

    out.push_str("\nAfter:\n");
    if after_events.is_empty() {
        out.push_str("- (none)");
    } else {
        for event in &after_events {
            append_open_event_line(&mut out, event);
        }
    }

    Ok(out.trim_end().to_string())
}

fn format_open_session_prose(payload: &Value) -> Result<String> {
    let parsed: OpenSessionProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse open session payload")?;

    let mut out = String::new();
    out.push_str(&format!("Open session: {}\n", parsed.session_id));

    if !parsed.found {
        out.push_str("Not found.");
        return Ok(out);
    }

    let scope = if parsed.scope.is_empty() {
        OpenScope::All.as_str()
    } else {
        parsed.scope.as_str()
    };
    out.push_str(&format!("Scope: {}\n", scope));
    out.push_str(&format!(
        "System events: {}\n",
        if parsed.include_system_events {
            "included"
        } else {
            "filtered"
        }
    ));
    out.push_str(&format!("Turn page limit: {}\n", parsed.limit));

    if !parsed.include_payload.is_empty() {
        out.push_str(&format!(
            "Payload fields: {}\n",
            parsed.include_payload.join(", ")
        ));
    }

    if let Some(summary) = parsed.summary.as_ref() {
        out.push_str(&format!(
            "Session window: {} -> {}\n",
            summary.start_time, summary.end_time
        ));
        out.push_str(&format!(
            "Session totals: turns={} events={}\n",
            summary.turn_count, summary.event_count
        ));
        out.push_str(&format!(
            "Session unix_ms: {} -> {}\n",
            summary.start_unix_ms, summary.end_unix_ms
        ));
    }

    if let Some(cursor) = parsed.cursor.as_deref() {
        out.push_str(&format!("Cursor: {}\n", cursor));
    }
    if let Some(next_cursor) = parsed.next_cursor.as_deref() {
        out.push_str(&format!("Next cursor: {}\n", next_cursor));
    }

    if !parsed.turns.is_empty() {
        out.push_str("\nTurns:\n");
        for turn in &parsed.turns {
            out.push_str(&format!(
                "- turn={} events={} {} -> {}\n",
                turn.turn_seq, turn.event_count, turn.started_at, turn.ended_at
            ));
        }
    }

    if !parsed.events.is_empty() {
        out.push_str("\nEvents:\n");
        for event in &parsed.events {
            out.push_str(&format!(
                "- [{}] {} {}\n",
                event.event_order,
                event.actor_role,
                display_kind(&event.event_class, &event.payload_type)
            ));
            let text = compact_text_line(&event.text_content, 220);
            if !text.is_empty() {
                out.push_str(&format!("  {}\n", text));
            }
            let payload_json = compact_text_line(&event.payload_json, 220);
            if !payload_json.is_empty() {
                out.push_str(&format!("  payload_json: {}\n", payload_json));
            }
        }
    } else if parsed.turns.is_empty() {
        out.push_str("\nNo transcript data in this page.");
    }

    Ok(out.trim_end().to_string())
}

fn mode_meaning(mode: ConversationMode) -> &'static str {
    match mode {
        ConversationMode::WebSearch => {
            "any web search activity (`web_search_call`, `search_results_received`, or `tool_use` with WebSearch/WebFetch)"
        }
        ConversationMode::McpInternal => {
            "any Codex MCP internal search/open activity (`source_name='codex-mcp'` or tool_name `search`/`open`) when web_search does not match"
        }
        ConversationMode::ToolCalling => {
            "any tool activity (`tool_call`, `tool_result`, or `tool_use`) when neither higher mode matches"
        }
        ConversationMode::Chat => {
            "no detected web-search, mcp-internal, or tool-calling activity"
        }
    }
}

fn format_conversation_search_prose(
    payload: &Value,
    mode: Option<ConversationMode>,
) -> Result<String> {
    let parsed: ConversationSearchProsePayload = serde_json::from_value(payload.clone())
        .context("failed to parse search_conversations payload")?;

    let mut out = String::new();
    out.push_str(&format!("Conversation Search: \"{}\"\n", parsed.query));
    out.push_str(&format!("Query ID: {}\n", parsed.query_id));
    out.push_str(&format!(
        "Hits: {} ({} ms)\n",
        parsed.stats.result_count, parsed.stats.took_ms
    ));
    if let Some(limit_summary) = format_limit_summary(
        parsed.stats.requested_limit,
        parsed.stats.effective_limit,
        parsed.stats.limit_capped,
    ) {
        out.push_str(&format!("Limit: {limit_summary}\n"));
    }

    if let Some(mode) = mode {
        out.push_str(&format!("Mode filter: {}\n", mode.as_str()));
        out.push_str(&format!(
            "Mode semantics: {}\n",
            CONVERSATION_MODE_CLASSIFICATION_SEMANTICS
        ));
        out.push_str(&format!("Mode meaning: {}\n", mode_meaning(mode)));
    }

    if parsed.hits.is_empty() {
        out.push_str("\nNo hits.");
        return Ok(out);
    }

    for hit in &parsed.hits {
        out.push_str(&format!(
            "\n{}) session={} score={:.4} matched_terms={} events={}\n",
            hit.rank, hit.session_id, hit.score, hit.matched_terms, hit.event_count_considered
        ));
        if let Some(harness) = hit.harness.as_deref() {
            out.push_str(&format!("   harness: {}\n", harness));
        }
        if let Some(inference_provider) = hit.inference_provider.as_deref() {
            out.push_str(&format!("   inference_provider: {}\n", inference_provider));
        }
        if let (Some(first), Some(last)) = (
            hit.first_event_time.as_deref(),
            hit.last_event_time.as_deref(),
        ) {
            out.push_str(&format!("   first_last: {} -> {}\n", first, last));
        } else if let (Some(first_ms), Some(last_ms)) =
            (hit.first_event_unix_ms, hit.last_event_unix_ms)
        {
            out.push_str(&format!(
                "   first_last_unix_ms: {} -> {}\n",
                first_ms, last_ms
            ));
        }
        if let Some(session_slug) = hit.session_slug.as_deref() {
            out.push_str(&format!("   session_slug: {}\n", session_slug));
        }
        if let Some(session_summary) = hit.session_summary.as_deref() {
            let compact = compact_text_line(session_summary, 220);
            if !compact.is_empty() {
                out.push_str(&format!("   session_summary: {}\n", compact));
            }
        }

        if let Some(best_event_uid) = hit.best_event_uid.as_deref() {
            out.push_str(&format!("   best_event_uid: {}\n", best_event_uid));
            out.push_str(&format!(
                "   next: open(event_uid=\"{}\")\n",
                best_event_uid
            ));
        }

        if let Some(snippet) = hit.snippet.as_deref() {
            let compact = compact_text_line(snippet, 220);
            if !compact.is_empty() {
                out.push_str(&format!("   snippet: {}\n", compact));
            }
        }
    }

    Ok(out.trim_end().to_string())
}

fn format_session_list_prose(payload: &Value) -> Result<String> {
    let parsed: SessionListProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse list_sessions payload")?;

    let mut out = String::new();
    out.push_str("Session List\n");
    out.push_str(&format!("Sessions: {}\n", parsed.sessions.len()));
    let sort = if parsed.sort.is_empty() {
        "desc"
    } else {
        parsed.sort.as_str()
    };
    out.push_str(&format!("Sort: {}\n", sort));

    if parsed.sessions.is_empty() {
        out.push_str("\nNo sessions.");
        return Ok(out);
    }

    for (idx, session) in parsed.sessions.iter().enumerate() {
        let mode = if session.mode.is_empty() {
            "chat"
        } else {
            session.mode.as_str()
        };

        out.push_str(&format!(
            "\n{}) session={} mode={} events={}\n",
            idx + 1,
            session.session_id,
            mode,
            session.event_count
        ));
        out.push_str(&format!(
            "   start: {} (unix_ms={})\n",
            session.start_time, session.start_unix_ms
        ));
        out.push_str(&format!(
            "   end: {} (unix_ms={})\n",
            session.end_time, session.end_unix_ms
        ));
    }

    if let Some(cursor) = parsed.next_cursor.as_deref() {
        out.push_str(&format!("\nnext_cursor: {}", cursor));
    }

    Ok(out.trim_end().to_string())
}

fn format_session_events_prose(payload: &Value) -> Result<String> {
    let parsed: SessionEventsProsePayload = serde_json::from_value(payload.clone())
        .context("failed to parse get_session_events payload")?;

    let mut out = String::new();
    out.push_str(&format!("Session events: {}\n", parsed.session_id));
    out.push_str(&format!("Direction: {}\n", parsed.direction));
    out.push_str(&format!("Events: {}\n", parsed.events.len()));

    if parsed.events.is_empty() {
        out.push_str("\nNo events.");
        return Ok(out);
    }

    for (idx, event) in parsed.events.iter().enumerate() {
        let kind = display_kind(&event.event_class, &event.payload_type);
        out.push_str(&format!(
            "\n{}) [{}] {} {} turn={} uid={}\n",
            idx + 1,
            event.event_order,
            event.actor_role,
            kind,
            event.turn_seq,
            event.event_uid
        ));
        if !event.event_time.is_empty() {
            out.push_str(&format!("   time: {}\n", event.event_time));
        }
        if !event.source_ref.is_empty() {
            out.push_str(&format!("   source_ref: {}\n", event.source_ref));
        }
        let snippet = compact_text_line(&event.text_content, 220);
        if !snippet.is_empty() {
            out.push_str(&format!("   text: {}\n", snippet));
        }
    }

    if let Some(cursor) = parsed.next_cursor.as_deref() {
        out.push_str(&format!("\nnext_cursor: {}", cursor));
    }

    Ok(out.trim_end().to_string())
}

fn format_get_session_prose(payload: &Value) -> Result<String> {
    let parsed: GetSessionProsePayload =
        serde_json::from_value(payload.clone()).context("failed to parse get_session payload")?;

    let mut out = String::new();
    out.push_str(&format!("Session: {}\n", parsed.session_id));

    if !parsed.found {
        if let Some(err) = parsed.error {
            out.push_str(&format!("Not found ({})", err.code));
            if !err.message.is_empty() {
                out.push_str(&format!(": {}", err.message));
            }
        } else {
            out.push_str("Not found.");
        }
        return Ok(out);
    }

    let Some(session) = parsed.session else {
        out.push_str("No session metadata available.");
        return Ok(out);
    };

    let mode = if session.mode.is_empty() {
        "chat"
    } else {
        session.mode.as_str()
    };

    out.push_str(&format!("Mode: {}\n", mode));
    out.push_str(&format!(
        "First event: {} (unix_ms={})\n",
        session.first_event_time, session.first_event_unix_ms
    ));
    out.push_str(&format!(
        "Last event: {} (unix_ms={})\n",
        session.last_event_time, session.last_event_unix_ms
    ));
    out.push_str(&format!(
        "Counts: events={} turns={} user={} assistant={} tool_calls={} tool_results={}\n",
        session.total_events,
        session.total_turns,
        session.user_messages,
        session.assistant_messages,
        session.tool_calls,
        session.tool_results
    ));
    out.push_str(&format!(
        "Boundary event_uids: first={} last={}\n",
        session.first_event_uid, session.last_event_uid
    ));
    if !session.last_actor_role.is_empty() {
        out.push_str(&format!("Last actor role: {}", session.last_actor_role));
    }

    Ok(out.trim_end().to_string())
}

fn append_open_event_line(out: &mut String, event: &OpenProseEvent) {
    let kind = display_kind(&event.event_class, &event.payload_type);
    out.push_str(&format!(
        "- [{}] {} {}\n",
        event.event_order, event.actor_role, kind
    ));

    let text = compact_text_line(&event.text_content, 220);
    if !text.is_empty() {
        out.push_str(&format!("  {}\n", text));
    }
}

fn format_limit_summary(
    requested_limit: Option<u16>,
    effective_limit: Option<u16>,
    limit_capped: bool,
) -> Option<String> {
    let effective = effective_limit?;
    match requested_limit {
        Some(requested) if limit_capped => Some(format!(
            "effective={} (capped at max_results={}; requested={})",
            effective, effective, requested
        )),
        Some(requested) => Some(format!("effective={} (requested={})", effective, requested)),
        None => Some(format!("effective={effective}")),
    }
}

fn display_kind(event_class: &str, payload_type: &str) -> String {
    if payload_type.is_empty() || payload_type == event_class || payload_type == "unknown" {
        if event_class.is_empty() {
            "event".to_string()
        } else {
            event_class.to_string()
        }
    } else if event_class.is_empty() {
        payload_type.to_string()
    } else {
        format!("{} ({})", event_class, payload_type)
    }
}

fn compact_text_line(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        return compact;
    }

    let mut trimmed: String = compact.chars().take(max_chars.saturating_sub(3)).collect();
    trimmed.push_str("...");
    trimmed
}

fn is_low_information_system_event(actor_role: &str, payload_type: &str) -> bool {
    actor_role.eq_ignore_ascii_case("system")
        && matches!(
            payload_type.to_ascii_lowercase().as_str(),
            "progress" | "file_history_snapshot" | "system"
        )
}

pub async fn run_stdio(cfg: AppConfig) -> Result<()> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;

    let repo_cfg = RepoConfig {
        max_results: cfg.mcp.max_results,
        preview_chars: cfg.mcp.preview_chars,
        default_context_before: cfg.mcp.default_context_before,
        default_context_after: cfg.mcp.default_context_after,
        default_include_tool_events: cfg.mcp.default_include_tool_events,
        default_exclude_codex_mcp: cfg.mcp.default_exclude_codex_mcp,
        async_log_writes: cfg.mcp.async_log_writes,
        bm25_k1: cfg.bm25.k1,
        bm25_b: cfg.bm25.b,
        bm25_default_min_score: cfg.bm25.default_min_score,
        bm25_default_min_should_match: cfg.bm25.default_min_should_match,
        bm25_max_query_terms: cfg.bm25.max_query_terms,
    };

    let repo = ClickHouseConversationRepository::new(ch, repo_cfg);
    let state = Arc::new(AppState {
        cfg,
        repo,
        prewarm_started: Arc::new(AtomicBool::new(false)),
    });

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        debug!("incoming rpc line: {}", line);

        let parsed = serde_json::from_str::<RpcRequest>(line);
        let req = match parsed {
            Ok(req) => req,
            Err(err) => {
                warn!("failed to parse rpc request: {}", err);
                continue;
            }
        };

        if let Some(resp) = state.handle_request(req).await {
            let payload = serde_json::to_vec(&resp)?;
            stdout.write_all(&payload).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_kind_compacts_payload_type_when_redundant() {
        assert_eq!(display_kind("message", "message"), "message");
        assert_eq!(display_kind("", "unknown"), "event");
    }

    #[test]
    fn compact_text_line_truncates() {
        let text = "one two three four five";
        let compact = compact_text_line(text, 10);
        assert!(compact.ends_with("..."));
    }

    #[test]
    fn format_conversation_search_handles_empty_hits() {
        let payload = json!({
            "query_id": "q1",
            "query": "hello world",
            "stats": {
                "took_ms": 2,
                "result_count": 0
            },
            "hits": []
        });

        let text = format_conversation_search_prose(&payload, None).expect("format");
        assert!(text.contains("Conversation Search"));
        assert!(text.contains("No hits"));
    }

    #[test]
    fn search_args_accept_single_event_kind_and_alias() {
        let args: SearchArgs = serde_json::from_value(json!({
            "query": "error",
            "kind": "reasoning"
        }))
        .expect("parse search args");

        let parsed = args.event_kind.expect("event kind should parse").into_vec();
        assert_eq!(parsed, vec![SearchEventKind::Reasoning]);
    }

    #[test]
    fn search_args_accept_event_kind_list() {
        let args: SearchArgs = serde_json::from_value(json!({
            "query": "error",
            "event_kind": ["message", "tool_result"]
        }))
        .expect("parse search args");

        let parsed = args.event_kind.expect("event kind should parse").into_vec();
        assert_eq!(
            parsed,
            vec![SearchEventKind::Message, SearchEventKind::ToolResult]
        );
    }

    #[test]
    fn get_session_events_args_accept_single_event_kind_alias() {
        let args: GetSessionEventsArgs = serde_json::from_value(json!({
            "session_id": "sess-1",
            "kind": "tool_call"
        }))
        .expect("parse get_session_events args");

        let parsed = args.event_kind.expect("event kind should parse").into_vec();
        assert_eq!(parsed, vec![SearchEventKind::ToolCall]);
    }

    #[test]
    fn search_args_accept_include_payload_json_flag() {
        let args: SearchArgs = serde_json::from_value(json!({
            "query": "error",
            "include_payload_json": true
        }))
        .expect("parse search args");

        assert_eq!(args.include_payload_json, Some(true));
    }

    #[test]
    fn apply_search_content_policy_redacts_non_user_facing_events() {
        let mut result = SearchEventsResult {
            query_id: "q1".to_string(),
            query: "query".to_string(),
            terms: vec!["query".to_string()],
            stats: moraine_conversations::SearchEventsStats {
                docs: 1,
                avgdl: 1.0,
                took_ms: 1,
                result_count: 3,
                requested_limit: 3,
                effective_limit: 3,
                limit_capped: false,
            },
            hits: vec![
                moraine_conversations::SearchEventHit {
                    rank: 1,
                    event_uid: "evt-1".to_string(),
                    session_id: "sess-1".to_string(),
                    first_event_time: String::new(),
                    last_event_time: String::new(),
                    source_name: "src".to_string(),
                    harness: "harness".to_string(),
                    inference_provider: "inference-provider".to_string(),
                    score: 1.0,
                    matched_terms: 1,
                    doc_len: 1,
                    event_class: "message".to_string(),
                    payload_type: "text".to_string(),
                    actor_role: "assistant".to_string(),
                    name: String::new(),
                    phase: String::new(),
                    source_ref: String::new(),
                    text_preview: "preview".to_string(),
                    text_content: Some("full text".to_string()),
                    payload_json: Some("{\"x\":1}".to_string()),
                },
                moraine_conversations::SearchEventHit {
                    rank: 2,
                    event_uid: "evt-2".to_string(),
                    session_id: "sess-1".to_string(),
                    first_event_time: String::new(),
                    last_event_time: String::new(),
                    source_name: "src".to_string(),
                    harness: "harness".to_string(),
                    inference_provider: "inference-provider".to_string(),
                    score: 0.9,
                    matched_terms: 1,
                    doc_len: 1,
                    event_class: "tool_result".to_string(),
                    payload_type: "json".to_string(),
                    actor_role: "tool".to_string(),
                    name: "search".to_string(),
                    phase: String::new(),
                    source_ref: String::new(),
                    text_preview: "preview".to_string(),
                    text_content: Some("tool text".to_string()),
                    payload_json: Some("{\"tool\":true}".to_string()),
                },
                moraine_conversations::SearchEventHit {
                    rank: 3,
                    event_uid: "evt-3".to_string(),
                    session_id: "sess-1".to_string(),
                    first_event_time: String::new(),
                    last_event_time: String::new(),
                    source_name: "src".to_string(),
                    harness: "harness".to_string(),
                    inference_provider: "inference-provider".to_string(),
                    score: 0.8,
                    matched_terms: 1,
                    doc_len: 1,
                    event_class: "message".to_string(),
                    payload_type: "text".to_string(),
                    actor_role: "system".to_string(),
                    name: String::new(),
                    phase: String::new(),
                    source_ref: String::new(),
                    text_preview: "preview".to_string(),
                    text_content: Some("system text".to_string()),
                    payload_json: Some("{\"system\":true}".to_string()),
                },
            ],
        };

        apply_search_content_policy(&mut result, false);

        assert_eq!(result.hits[0].text_content.as_deref(), Some("full text"));
        assert!(result.hits[0].payload_json.is_none());
        assert!(result.hits[1].text_content.is_none());
        assert!(result.hits[1].payload_json.is_none());
        assert!(result.hits[2].text_content.is_none());
        assert!(result.hits[2].payload_json.is_none());

        let payload = serde_json::to_value(&result).expect("serialize search payload");
        assert!(payload["hits"][0]["payload_json"].is_null());
        assert!(payload["hits"][1]["text_content"].is_null());
        assert!(payload["hits"][2]["text_content"].is_null());
    }

    #[test]
    fn apply_conversation_search_content_policy_requires_payload_opt_in() {
        let mut result = ConversationSearchResults {
            query_id: "q1".to_string(),
            query: "query".to_string(),
            terms: vec!["query".to_string()],
            stats: moraine_conversations::ConversationSearchStats {
                docs: 1,
                avgdl: 1.0,
                took_ms: 1,
                result_count: 1,
                requested_limit: 1,
                effective_limit: 1,
                limit_capped: false,
            },
            hits: vec![moraine_conversations::ConversationSearchHit {
                rank: 1,
                session_id: "sess-1".to_string(),
                first_event_time: None,
                first_event_unix_ms: None,
                last_event_time: None,
                last_event_unix_ms: None,
                harness: None,
                inference_provider: None,
                session_slug: None,
                session_summary: None,
                score: 1.0,
                matched_terms: 1,
                event_count_considered: 1,
                best_event_uid: Some("evt-1".to_string()),
                snippet: Some("preview".to_string()),
                text_preview: Some("preview".to_string()),
                text_content: Some("full text".to_string()),
                payload_json: Some("{\"x\":1}".to_string()),
            }],
        };

        apply_conversation_search_content_policy(&mut result, false);
        assert!(result.hits[0].payload_json.is_none());
        let payload = serde_json::to_value(&result).expect("serialize conversation payload");
        assert!(payload["hits"][0]["payload_json"].is_null());

        result.hits[0].payload_json = Some("{\"x\":1}".to_string());
        apply_conversation_search_content_policy(&mut result, true);
        assert_eq!(result.hits[0].payload_json.as_deref(), Some("{\"x\":1}"));
        let payload =
            serde_json::to_value(&result).expect("serialize opted-in conversation payload");
        assert_eq!(payload["hits"][0]["payload_json"], json!("{\"x\":1}"));
    }

    #[test]
    fn build_session_events_payload_uses_plural_event_kinds() {
        let payload = AppState::build_session_events_payload(
            "sess-1".to_string(),
            SessionEventsDirection::Reverse,
            Some(vec![SearchEventKind::Message, SearchEventKind::ToolCall]),
            Vec::new(),
            Some("cursor-next".to_string()),
        );

        assert_eq!(payload["session_id"], json!("sess-1"));
        assert_eq!(payload["direction"], json!("reverse"));
        assert_eq!(payload["event_kinds"], json!(["message", "tool_call"]));
        assert_eq!(payload["next_cursor"], json!("cursor-next"));
    }

    #[test]
    fn tool_limit_bounds_use_shared_min_and_effective_max() {
        assert_eq!(tool_limit_bounds(25), (1, 25));
        assert_eq!(tool_limit_bounds(0), (1, 1));
    }

    #[test]
    fn validate_tool_limit_enforces_bounds() {
        assert_eq!(
            validate_tool_limit("search", None, 25).expect("missing limit accepted"),
            None
        );
        assert_eq!(
            validate_tool_limit("search", Some(25), 25).expect("max bound accepted"),
            Some(25)
        );

        let zero_err = validate_tool_limit("search", Some(0), 25).expect_err("zero must fail");
        assert_eq!(
            zero_err.to_string(),
            "search limit must be between 1 and 25 (received 0)"
        );

        let high_err = validate_tool_limit("search", Some(26), 25).expect_err("above max fails");
        assert_eq!(
            high_err.to_string(),
            "search limit must be between 1 and 25 (received 26)"
        );
    }

    #[test]
    fn format_search_prose_reports_capped_limit_metadata() {
        let payload = json!({
            "query_id": "q1",
            "query": "big iron",
            "stats": {
                "took_ms": 7,
                "result_count": 25,
                "requested_limit": 100,
                "effective_limit": 25,
                "limit_capped": true
            },
            "hits": []
        });

        let text = format_search_prose(&payload).expect("format");
        assert!(text.contains("Limit: effective=25 (capped at max_results=25; requested=100)"));
    }

    #[test]
    fn format_conversation_search_reports_effective_limit_when_uncapped() {
        let payload = json!({
            "query_id": "q1",
            "query": "hello world",
            "stats": {
                "took_ms": 2,
                "result_count": 0,
                "requested_limit": 10,
                "effective_limit": 10,
                "limit_capped": false
            },
            "hits": []
        });

        let text = format_conversation_search_prose(&payload, None).expect("format");
        assert!(text.contains("Limit: effective=10 (requested=10)"));
    }

    #[test]
    fn format_conversation_search_includes_mode_semantics_when_mode_filter_is_set() {
        let payload = json!({
            "query_id": "q1",
            "query": "hello world",
            "stats": {
                "took_ms": 2,
                "result_count": 0
            },
            "hits": []
        });

        let text = format_conversation_search_prose(&payload, Some(ConversationMode::ToolCalling))
            .expect("format");
        assert!(text.contains("Mode filter: tool_calling"));
        assert!(text.contains("Mode semantics: Sessions are classified into exactly one mode"));
        assert!(text.contains("Mode meaning: any tool activity"));
    }

    #[test]
    fn search_conversations_mode_doc_describes_precedence_and_mode_meanings() {
        assert!(CONVERSATION_MODE_CLASSIFICATION_SEMANTICS
            .contains("web_search > mcp_internal > tool_calling > chat"));
        assert!(SEARCH_CONVERSATIONS_MODE_DOC.contains("web_search=any web search activity"));
        assert!(SEARCH_CONVERSATIONS_MODE_DOC
            .contains("mcp_internal=any Codex MCP internal search/open activity"));
        assert!(SEARCH_CONVERSATIONS_MODE_DOC.contains("tool_calling=any tool activity"));
        assert!(SEARCH_CONVERSATIONS_MODE_DOC.contains("chat=none of the above"));
    }

    #[test]
    fn format_conversation_search_includes_session_metadata() {
        let payload = json!({
            "query_id": "q1",
            "query": "hello world",
            "stats": {
                "took_ms": 2,
                "result_count": 1
            },
            "hits": [
                {
                    "rank": 1,
                    "session_id": "sess_c",
                    "first_event_time": "2026-01-03 10:00:00",
                    "first_event_unix_ms": 1767434400000_i64,
                    "last_event_time": "2026-01-03 10:10:00",
                    "last_event_unix_ms": 1767435000000_i64,
                    "harness": "codex",
                    "session_slug": "project-c",
                    "session_summary": "Session C summary",
                    "score": 12.5,
                    "matched_terms": 2,
                    "event_count_considered": 3,
                    "best_event_uid": "evt-c-42",
                    "snippet": "best match from session c"
                }
            ]
        });

        let text = format_conversation_search_prose(&payload, None).expect("format");
        assert!(text.contains("harness: codex"));
        assert!(text.contains("first_last: 2026-01-03 10:00:00 -> 2026-01-03 10:10:00"));
        assert!(text.contains("session_slug: project-c"));
        assert!(text.contains("session_summary: Session C summary"));
    }

    #[test]
    fn format_search_prose_includes_session_recency() {
        let payload = json!({
            "query_id": "q2",
            "query": "design decision",
            "stats": {
                "took_ms": 3,
                "result_count": 1
            },
            "hits": [
                {
                    "rank": 1,
                    "event_uid": "evt-1",
                    "session_id": "sess-a",
                    "first_event_time": "2026-01-01 00:00:00",
                    "last_event_time": "2026-01-02 00:00:00",
                    "score": 4.2,
                    "event_class": "message",
                    "payload_type": "text",
                    "actor_role": "assistant",
                    "text_preview": "decision details"
                }
            ]
        });

        let text = format_search_prose(&payload).expect("format");
        assert!(text.contains("last_event_time=2026-01-02 00:00:00"));
        assert!(text.contains("session_window: 2026-01-01 00:00:00 -> 2026-01-02 00:00:00"));
    }

    #[test]
    fn format_session_list_handles_empty_result() {
        let payload = json!({
            "sessions": [],
            "sort": "desc",
            "next_cursor": null
        });

        let text = format_session_list_prose(&payload).expect("format");
        assert!(text.contains("Session List"));
        assert!(text.contains("Sort: desc"));
        assert!(text.contains("No sessions"));
    }

    #[test]
    fn format_session_list_includes_next_cursor_and_times() {
        let payload = json!({
            "sessions": [
                {
                    "session_id": "sess-1",
                    "start_time": "2026-01-02 12:00:00",
                    "start_unix_ms": 1767355200000_i64,
                    "end_time": "2026-01-02 12:05:00",
                    "end_unix_ms": 1767355500000_i64,
                    "event_count": 22_u64,
                    "mode": "web_search"
                }
            ],
            "sort": "asc",
            "next_cursor": "cursor-token"
        });

        let text = format_session_list_prose(&payload).expect("format");
        assert!(text.contains("session=sess-1"));
        assert!(text.contains("Sort: asc"));
        assert!(text.contains("mode=web_search"));
        assert!(text.contains("next_cursor: cursor-token"));
    }

    #[test]
    fn format_session_events_includes_cursor_and_event_details() {
        let payload = json!({
            "session_id": "sess-1",
            "direction": "reverse",
            "events": [
                {
                    "event_uid": "evt-3",
                    "event_order": 3_u64,
                    "turn_seq": 2_u32,
                    "event_time": "2026-01-02 12:05:00",
                    "actor_role": "assistant",
                    "event_class": "message",
                    "payload_type": "text",
                    "source_ref": "/tmp/sess-1.jsonl:1:3",
                    "text_content": "assistant answer"
                }
            ],
            "next_cursor": "cursor-next"
        });

        let text = format_session_events_prose(&payload).expect("format");
        assert!(text.contains("Session events: sess-1"));
        assert!(text.contains("Direction: reverse"));
        assert!(text.contains("[3] assistant message (text)"));
        assert!(text.contains("uid=evt-3"));
        assert!(text.contains("next_cursor: cursor-next"));
    }

    #[test]
    fn format_get_session_includes_summary_fields() {
        let payload = json!({
            "found": true,
            "session_id": "sess-1",
            "session": {
                "session_id": "sess-1",
                "first_event_time": "2026-01-02 12:00:00",
                "first_event_unix_ms": 1767355200000_i64,
                "last_event_time": "2026-01-02 12:05:00",
                "last_event_unix_ms": 1767355500000_i64,
                "total_events": 22_u64,
                "total_turns": 3_u32,
                "user_messages": 5_u64,
                "assistant_messages": 5_u64,
                "tool_calls": 2_u64,
                "tool_results": 2_u64,
                "mode": "web_search",
                "first_event_uid": "evt-0001",
                "last_event_uid": "evt-0022",
                "last_actor_role": "assistant"
            }
        });

        let text = format_get_session_prose(&payload).expect("format");
        assert!(text.contains("Session: sess-1"));
        assert!(text.contains("Mode: web_search"));
        assert!(text
            .contains("Counts: events=22 turns=3 user=5 assistant=5 tool_calls=2 tool_results=2"));
        assert!(text.contains("Boundary event_uids: first=evt-0001 last=evt-0022"));
        assert!(text.contains("Last actor role: assistant"));
    }

    #[test]
    fn format_get_session_handles_not_found_payload() {
        let payload = json!({
            "found": false,
            "session_id": "sess-missing",
            "error": {
                "code": "not_found",
                "message": "session_id was not found"
            }
        });

        let text = format_get_session_prose(&payload).expect("format");
        assert!(text.contains("Session: sess-missing"));
        assert!(text.contains("Not found (not_found): session_id was not found"));
    }

    #[test]
    fn build_get_session_payload_returns_structured_invalid_argument() {
        let payload = AppState::build_get_session_payload(
            "sess bad".to_string(),
            Err(RepoError::invalid_argument(
                "session_id contains unsupported characters",
            )),
        )
        .expect("payload");

        assert_eq!(payload["found"], json!(false));
        assert_eq!(payload["session_id"], json!("sess bad"));
        assert_eq!(payload["error"]["code"], json!("invalid_argument"));
        assert_eq!(
            payload["error"]["message"],
            json!("session_id contains unsupported characters")
        );
    }

    #[test]
    fn format_get_session_handles_invalid_argument_payload() {
        let payload = json!({
            "found": false,
            "session_id": "sess bad",
            "error": {
                "code": "invalid_argument",
                "message": "session_id contains unsupported characters"
            }
        });

        let text = format_get_session_prose(&payload).expect("format");
        assert!(text.contains("Session: sess bad"));
        assert!(text
            .contains("Not found (invalid_argument): session_id contains unsupported characters"));
    }

    #[test]
    fn open_args_accept_session_scope_payload_and_paging() {
        let args: OpenArgs = serde_json::from_value(json!({
            "session_id": "sess-42",
            "scope": "messages",
            "include_payload": ["text", "payload_json"],
            "limit": 5,
            "cursor": "c1"
        }))
        .expect("parse open args");

        assert_eq!(args.session_id.as_deref(), Some("sess-42"));
        assert!(args.event_uid.is_none());
        assert!(matches!(args.scope, Some(OpenScope::Messages)));
        assert_eq!(args.limit, Some(5));
        assert_eq!(args.cursor.as_deref(), Some("c1"));
        let include_payload = args
            .include_payload
            .expect("include payload should parse")
            .into_vec();
        assert_eq!(
            include_payload,
            vec![OpenPayloadField::Text, OpenPayloadField::PayloadJson]
        );
    }

    #[test]
    fn format_open_prose_formats_session_transcript_payload() {
        let payload = json!({
            "open_mode": "session",
            "found": true,
            "session_id": "sess-a",
            "scope": "messages",
            "include_system_events": false,
            "include_payload": ["text"],
            "limit": 10_u16,
            "cursor": null,
            "next_cursor": "cursor-next",
            "summary": {
                "start_time": "2026-01-01 00:00:00",
                "start_unix_ms": 1767225600000_i64,
                "end_time": "2026-01-01 00:10:00",
                "end_unix_ms": 1767226200000_i64,
                "event_count": 2_u64,
                "turn_count": 1_u32
            },
            "events": [
                {
                    "event_order": 1_u64,
                    "actor_role": "user",
                    "event_class": "message",
                    "payload_type": "text",
                    "text_content": "hello world"
                }
            ]
        });

        let text = format_open_prose(&payload).expect("format");
        assert!(text.contains("Open session: sess-a"));
        assert!(text.contains("Scope: messages"));
        assert!(text.contains("Next cursor: cursor-next"));
        assert!(text.contains("hello world"));
    }
}
