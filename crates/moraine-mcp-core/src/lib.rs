use anyhow::{anyhow, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_conversations::{
    ClickHouseConversationRepository, ConversationMode, ConversationRepository,
    ConversationSearchQuery, OpenEventRequest, RepoConfig, SearchEventKind, SearchEventsQuery,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, warn};

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
}

#[derive(Debug, Default, Deserialize)]
struct ConversationSearchProseHit {
    #[serde(default)]
    rank: u64,
    #[serde(default)]
    session_id: String,
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
        json!({
            "tools": [
                {
                    "name": "search",
                    "description": "BM25 lexical search over Moraine indexed conversation events.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "limit": { "type": "integer", "minimum": 1, "maximum": self.cfg.mcp.max_results },
                            "session_id": { "type": "string" },
                            "min_score": { "type": "number" },
                            "min_should_match": { "type": "integer", "minimum": 1 },
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
                    "description": "BM25 lexical search across whole conversations.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "limit": { "type": "integer", "minimum": 1, "maximum": self.cfg.mcp.max_results },
                            "min_score": { "type": "number" },
                            "min_should_match": { "type": "integer", "minimum": 1 },
                            "from_unix_ms": { "type": "integer" },
                            "to_unix_ms": { "type": "integer" },
                            "mode": {
                                "type": "string",
                                "enum": ["web_search", "mcp_internal", "tool_calling", "chat"]
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
                }
            ]
        })
    }

    async fn call_tool(&self, params: ToolCallParams) -> Result<Value> {
        match params.name.as_str() {
            "search" => {
                let args: SearchArgs = serde_json::from_value(params.arguments)
                    .context("search expects a JSON object with at least {\"query\": ...}")?;
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
                let args: SearchConversationsArgs = serde_json::from_value(params.arguments)
                    .context(
                        "search_conversations expects a JSON object with at least {\"query\": ...}",
                    )?;
                let verbosity = args.verbosity.unwrap_or_default();
                let payload = self.search_conversations(args).await?;
                match verbosity {
                    Verbosity::Full => Ok(tool_ok_full(payload)),
                    Verbosity::Prose => {
                        Ok(tool_ok_prose(format_conversation_search_prose(&payload)?))
                    }
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
                event_kinds: args.event_kind.map(SearchEventKindsArg::into_vec),
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

    if parsed.hits.is_empty() {
        out.push_str("\nNo hits.");
        return Ok(out);
    }

    for hit in &parsed.hits {
        let kind = display_kind(&hit.event_class, &hit.payload_type);
        out.push_str(&format!(
            "\n{}) session={} score={:.4} kind={} role={}\n",
            hit.rank, hit.session_id, hit.score, kind, hit.actor_role
        ));

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

fn format_conversation_search_prose(payload: &Value) -> Result<String> {
    let parsed: ConversationSearchProsePayload = serde_json::from_value(payload.clone())
        .context("failed to parse search_conversations payload")?;

    let mut out = String::new();
    out.push_str(&format!("Conversation Search: \"{}\"\n", parsed.query));
    out.push_str(&format!("Query ID: {}\n", parsed.query_id));
    out.push_str(&format!(
        "Hits: {} ({} ms)\n",
        parsed.stats.result_count, parsed.stats.took_ms
    ));

    if parsed.hits.is_empty() {
        out.push_str("\nNo hits.");
        return Ok(out);
    }

    for hit in &parsed.hits {
        out.push_str(&format!(
            "\n{}) session={} score={:.4} matched_terms={} events={}\n",
            hit.rank, hit.session_id, hit.score, hit.matched_terms, hit.event_count_considered
        ));

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

        let text = format_conversation_search_prose(&payload).expect("format");
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
}
