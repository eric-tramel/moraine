#![recursion_limit = "256"]

pub mod contract;
mod list_sessions_v1;
mod open_v1;
mod search_sessions_v1;

use anyhow::{anyhow, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, warn};

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
                        "name": "moraine-mcp",
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
                    "name": contract::SEARCH_SESSIONS_TOOL,
                    "description": "Search Moraine session history and return compact event-ranked handles. Use open with the returned event_id, turn_id, or session_id to expand results.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Natural-language search query."
                            },
                            "within_id": {
                                "type": ["string", "null"],
                                "description": "Optional session:... or turn:... ID to constrain search scope."
                            },
                            "event_types": {
                                "type": ["array", "null"],
                                "items": {
                                    "type": "string",
                                    "enum": [
                                        "user_input",
                                        "assistant_response",
                                        "reasoning",
                                        "tool_call",
                                        "tool_response",
                                        "compaction",
                                        "system",
                                        "runtime"
                                    ]
                                },
                                "description": "Optional normalized event type filter. Defaults to user_input, assistant_response, and tool_response."
                            },
                            "n_hits": {
                                "type": ["integer", "null"],
                                "minimum": contract::SEARCH_SESSIONS_MIN_N_HITS,
                                "maximum": contract::SEARCH_SESSIONS_MAX_N_HITS,
                                "default": contract::SEARCH_SESSIONS_DEFAULT_N_HITS
                            }
                        },
                        "required": ["query"]
                    },
                    "outputSchema": {
                        "type": "object",
                        "required": ["schema_version", "tool", "request", "data", "warnings", "performance"],
                        "properties": {
                            "schema_version": { "type": "string" },
                            "tool": { "const": contract::SEARCH_SESSIONS_TOOL },
                            "request": { "type": "object" },
                            "data": {
                                "type": "object",
                                "required": ["result_count", "limit", "truncated", "results"]
                            },
                            "warnings": { "type": "array" },
                            "performance": { "type": "object" }
                        }
                    },
                    "annotations": {
                        "readOnlyHint": true
                    }
                },
                {
                    "name": contract::OPEN_TOOL,
                    "description": "Open a Moraine MCP ID returned by search_sessions, list_sessions, or open. Accepts session, turn, and event IDs.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "id": {
                                "type": "string",
                                "description": "A session:..., turn:..., or event:... Moraine MCP ID."
                            }
                        },
                        "required": ["id"]
                    },
                    "outputSchema": {
                        "type": "object",
                        "required": ["schema_version", "tool", "request", "data", "warnings", "performance"],
                        "properties": {
                            "schema_version": { "type": "string" },
                            "tool": { "const": contract::OPEN_TOOL },
                            "request": { "type": "object" },
                            "data": {
                                "type": "object",
                                "required": ["kind"]
                            },
                            "warnings": { "type": "array" },
                            "performance": { "type": "object" }
                        }
                    },
                    "annotations": {
                        "readOnlyHint": true
                    }
                },
                {
                    "name": contract::LIST_SESSIONS_TOOL,
                    "description": "List Moraine sessions that overlap a start/end datetime range. Returns typed session IDs, compact metadata, pagination, and open handles. Use this for metadata browsing by time; use search_sessions for content search and open to inspect a selected session.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "start_datetime": {
                                "type": "string",
                                "description": "Inclusive lower bound for session activity. Must be RFC 3339 / ISO 8601 with an explicit timezone, for example 2026-04-30T09:00:00-04:00 or 2026-04-30T13:00:00Z."
                            },
                            "end_datetime": {
                                "type": "string",
                                "description": "Exclusive upper bound for session activity. Must be RFC 3339 / ISO 8601 with an explicit timezone and later than start_datetime."
                            },
                            "limit": {
                                "type": ["integer", "null"],
                                "minimum": contract::LIST_SESSIONS_MIN_LIMIT,
                                "maximum": self.cfg.mcp.max_results.max(contract::LIST_SESSIONS_MIN_LIMIT),
                                "default": contract::LIST_SESSIONS_DEFAULT_LIMIT.min(self.cfg.mcp.max_results.max(contract::LIST_SESSIONS_MIN_LIMIT))
                            },
                            "cursor": {
                                "type": ["string", "null"],
                                "description": "Opaque cursor from a previous list_sessions response with the same filter and sort values."
                            },
                            "mode": {
                                "type": ["string", "null"],
                                "enum": ["web_search", "mcp_internal", "tool_calling", "chat", null],
                                "description": "Optional session mode filter."
                            },
                            "sort": {
                                "type": ["string", "null"],
                                "enum": ["desc", "asc", null],
                                "default": "desc",
                                "description": "Sort order by session updated_at and session ID."
                            }
                        },
                        "required": ["start_datetime", "end_datetime"]
                    },
                    "outputSchema": {
                        "type": "object",
                        "required": ["schema_version", "tool", "request", "data", "warnings", "performance"],
                        "properties": {
                            "schema_version": { "type": "string" },
                            "tool": { "const": contract::LIST_SESSIONS_TOOL },
                            "request": { "type": "object" },
                            "data": {
                                "type": "object",
                                "required": ["result_count", "limit", "truncated", "sessions", "next_cursor"]
                            },
                            "warnings": { "type": "array" },
                            "performance": { "type": "object" }
                        }
                    },
                    "annotations": {
                        "readOnlyHint": true
                    }
                }
            ]
        })
    }

    async fn call_tool(&self, params: ToolCallParams) -> Result<Value> {
        match params.name.as_str() {
            contract::SEARCH_SESSIONS_TOOL => self.search_sessions_v1(params.arguments).await,
            contract::LIST_SESSIONS_TOOL => self.list_sessions_v1(params.arguments).await,
            contract::OPEN_TOOL => self.open_v1(params.arguments).await,
            other => Err(anyhow!("unknown tool: {other}")),
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

fn tool_ok_hybrid(text: String, payload: Value) -> Value {
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

    fn test_state() -> AppState {
        let cfg = AppConfig::default();
        let ch = ClickHouseClient::new(cfg.clickhouse.clone()).expect("clickhouse client");
        let repo = ClickHouseConversationRepository::new(ch, RepoConfig::default());
        AppState {
            cfg,
            repo,
            prewarm_started: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn tools_list_publishes_mcp_search_surface_with_output_schemas() {
        let state = test_state();
        let payload = state.tools_list_result();
        let tools = payload["tools"].as_array().expect("tools array");
        let names = tools
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["search_sessions", "open", "list_sessions"]);

        for tool_name in ["search_sessions", "open", "list_sessions"] {
            let tool = tools
                .iter()
                .find(|tool| tool["name"].as_str() == Some(tool_name))
                .expect("tool exists");
            assert!(
                tool.get("outputSchema").is_some(),
                "{tool_name} has outputSchema"
            );
            assert_eq!(tool["annotations"]["readOnlyHint"], json!(true));
        }

        let open = tools
            .iter()
            .find(|tool| tool["name"].as_str() == Some("open"))
            .expect("open exists");
        assert_eq!(
            open["inputSchema"]["properties"]
                .as_object()
                .map(|props| props.len()),
            Some(1)
        );
        assert!(open["inputSchema"]["properties"].get("id").is_some());

        let search = tools
            .iter()
            .find(|tool| tool["name"].as_str() == Some("search_sessions"))
            .expect("search_sessions exists");
        assert_eq!(
            search["inputSchema"]["properties"]["event_types"]["description"],
            json!("Optional normalized event type filter. Defaults to user_input, assistant_response, and tool_response.")
        );
        assert_eq!(
            search["inputSchema"]["properties"]["n_hits"]["default"],
            json!(contract::SEARCH_SESSIONS_DEFAULT_N_HITS)
        );
        assert!(
            search["inputSchema"]["properties"]
                .get("evidence_policy")
                .is_none(),
            "search_sessions should not advertise legacy evidence policy controls"
        );

        let list_sessions = tools
            .iter()
            .find(|tool| tool["name"].as_str() == Some("list_sessions"))
            .expect("list_sessions exists");
        assert_eq!(
            list_sessions["inputSchema"]["required"],
            json!(["start_datetime", "end_datetime"])
        );
        assert_eq!(
            list_sessions["inputSchema"]["additionalProperties"],
            json!(false)
        );
        assert_eq!(
            list_sessions["inputSchema"]["properties"]["sort"]["default"],
            json!("desc")
        );
        assert_eq!(
            list_sessions["outputSchema"]["properties"]["data"]["required"],
            json!([
                "result_count",
                "limit",
                "truncated",
                "sessions",
                "next_cursor"
            ])
        );
    }
}
