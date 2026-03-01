use anyhow::{anyhow, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_conversations::{
    ClickHouseConversationRepository, ConversationListFilter, ConversationMode,
    ConversationRepository, ConversationSearchQuery, OpenEventRequest, PageRequest, RepoConfig,
    SearchEventsQuery,
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
    #[serde(default)]
    exclude_codex_mcp: Option<bool>,
    #[serde(default)]
    verbosity: Option<Verbosity>,
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
    verbosity: Option<Verbosity>,
}

#[derive(Debug, Deserialize)]
struct OpenArgs {
    event_uid: String,
    #[serde(default)]
    before: Option<u16>,
    #[serde(default)]
    after: Option<u16>,
    #[serde(default)]
    include_system_events: Option<bool>,
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
    provider: Option<String>,
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
    harness_type: String,
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
                    "description": "BM25 lexical search over Moraine indexed conversation events.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "limit": { "type": "integer", "minimum": limit_min, "maximum": limit_max },
                            "session_id": { "type": "string" },
                            "min_score": { "type": "number" },
                            "min_should_match": { "type": "integer", "minimum": 1 },
                            "include_tool_events": { "type": "boolean" },
                            "exclude_codex_mcp": { "type": "boolean" },
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
                    "description": "Open one event by uid with surrounding conversation context.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "event_uid": { "type": "string" },
                            "before": { "type": "integer", "minimum": 0 },
                            "after": { "type": "integer", "minimum": 0 },
                            "include_system_events": { "type": "boolean", "default": false },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        },
                        "required": ["event_uid"]
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
                            "min_should_match": { "type": "integer", "minimum": 1 },
                            "from_unix_ms": { "type": "integer" },
                            "to_unix_ms": { "type": "integer" },
                            "mode": {
                                "type": "string",
                                "enum": ["web_search", "mcp_internal", "tool_calling", "chat"],
                                "description": SEARCH_CONVERSATIONS_MODE_DOC
                            },
                            "include_tool_events": { "type": "boolean" },
                            "exclude_codex_mcp": { "type": "boolean" },
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
                                "enum": ["web_search", "mcp_internal", "tool_calling", "chat"]
                            },
                            "verbosity": {
                                "type": "string",
                                "enum": ["prose", "full"],
                                "default": "prose"
                            }
                        }
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
                let args: OpenArgs = serde_json::from_value(params.arguments)
                    .context("open expects {\"event_uid\": ...}")?;
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
                let args: ListSessionsArgs = if params.arguments.is_null() {
                    ListSessionsArgs::default()
                } else {
                    serde_json::from_value(params.arguments)
                        .context("list_sessions expects a JSON object with optional filters")?
                };
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.list_sessions(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => Ok(tool_ok_prose(format_session_list_prose(&payload)?)),
                }
            }
            other => Err(anyhow!("unknown tool: {other}")),
        }
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        let result = self
            .repo
            .search_events(SearchEventsQuery {
                query: args.query,
                source: Some("moraine-mcp".to_string()),
                limit: args.limit,
                session_id: args.session_id,
                min_score: args.min_score,
                min_should_match: args.min_should_match,
                include_tool_events: args.include_tool_events,
                exclude_codex_mcp: args.exclude_codex_mcp,
                disable_cache: None,
                search_strategy: None,
            })
            .await
            .map_err(|err| anyhow!(err.to_string()))?;

        serde_json::to_value(result).context("failed to encode search result payload")
    }

    async fn open(&self, args: OpenArgs) -> Result<Value> {
        let result = self
            .repo
            .open_event(OpenEventRequest {
                event_uid: args.event_uid,
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

    async fn search_conversations(&self, args: SearchConversationsArgs) -> Result<Value> {
        let result = self
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

        serde_json::to_value(result).context("failed to encode search_conversations result payload")
    }

    async fn list_sessions(&self, args: ListSessionsArgs) -> Result<Value> {
        let ListSessionsArgs {
            limit,
            cursor,
            from_unix_ms,
            to_unix_ms,
            mode,
            verbosity: _,
        } = args;

        let page = self
            .repo
            .list_conversations(
                ConversationListFilter {
                    from_unix_ms,
                    to_unix_ms,
                    mode,
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
                    "harness_type": summary.mode.as_str(),
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "from_unix_ms": from_unix_ms,
            "to_unix_ms": to_unix_ms,
            "mode": mode.map(ConversationMode::as_str),
            "sessions": sessions,
            "next_cursor": page.next_cursor,
        }))
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
        if let Some(provider) = hit.provider.as_deref() {
            out.push_str(&format!("   provider: {}\n", provider));
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

    if parsed.sessions.is_empty() {
        out.push_str("\nNo sessions.");
        return Ok(out);
    }

    for (idx, session) in parsed.sessions.iter().enumerate() {
        let harness = if session.harness_type.is_empty() {
            "chat"
        } else {
            session.harness_type.as_str()
        };

        out.push_str(&format!(
            "\n{}) session={} harness={} events={}\n",
            idx + 1,
            session.session_id,
            harness,
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
                    "provider": "codex",
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
        assert!(text.contains("provider: codex"));
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
            "next_cursor": null
        });

        let text = format_session_list_prose(&payload).expect("format");
        assert!(text.contains("Session List"));
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
                    "harness_type": "web_search"
                }
            ],
            "next_cursor": "cursor-token"
        });

        let text = format_session_list_prose(&payload).expect("format");
        assert!(text.contains("session=sess-1"));
        assert!(text.contains("harness=web_search"));
        assert!(text.contains("next_cursor: cursor-token"));
    }
}
