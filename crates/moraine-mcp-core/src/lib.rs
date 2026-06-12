#![recursion_limit = "256"]

pub mod contract;
mod list_sessions_v1;
mod open_v1;
mod search_sessions_v1;

use anyhow::{anyhow, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
pub use moraine_conversations::SessionOriginScope;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig};
use serde::Deserialize;
use serde_json::{json, Value};
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
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
                if self.cfg.mcp.prewarm_on_initialize
                    && self
                        .prewarm_started
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    let repo = self.repo.clone();
                    tokio::spawn(async move {
                        if let Err(err) = repo.prewarm_mcp_search_state().await {
                            warn!("mcp prewarm failed: {}", err);
                        } else {
                            debug!("mcp prewarm completed");
                        }
                    });
                }

                let mut result = json!({
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
                if let Some(scope) = self.repo.config().session_scope.as_ref() {
                    result["instructions"] = json!(format!(
                        "Retrieval is scoped to sessions that originated under: {}. \
                         Sessions from other directories (or with no recorded working \
                         directory) are not visible.",
                        scope.roots.join(", ")
                    ));
                }

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
                                "anyOf": [
                                    {
                                        "type": "string",
                                        "enum": ["web_search", "mcp_internal", "tool_calling", "chat"]
                                    },
                                    { "type": "null" }
                                ],
                                "description": "Optional session mode filter."
                            },
                            "sort": {
                                "anyOf": [
                                    {
                                        "type": "string",
                                        "enum": ["desc", "asc"]
                                    },
                                    { "type": "null" }
                                ],
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

impl AppState {
    /// Build the shared MCP application state: one ClickHouse client (and its
    /// reqwest connection pool) and one conversation repository (and its
    /// caches). A single instance is shared across every connection a central
    /// server handles, so the heavy state is allocated once per host rather
    /// than once per agent session.
    ///
    /// `session_scope` restricts every retrieval tool to sessions originating
    /// under the given roots (`--project-only`). Scoped states are only ever
    /// built for embedded per-session servers — the central server is shared
    /// across projects and must stay unscoped.
    pub fn build(
        cfg: AppConfig,
        session_scope: Option<SessionOriginScope>,
    ) -> Result<Arc<AppState>> {
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
            session_scope,
        };

        let repo = ClickHouseConversationRepository::new(ch, repo_cfg);
        Ok(Arc::new(AppState {
            cfg,
            repo,
            prewarm_started: Arc::new(AtomicBool::new(false)),
        }))
    }
}

/// Drive a single newline-delimited JSON-RPC connection to completion.
///
/// This is the one source of truth for the MCP wire framing: one JSON object
/// per line in, one JSON object + `\n` per response out, blank lines skipped.
/// `run_stdio` drives it over stdin/stdout; the central server drives one of
/// these per accepted socket connection, all sharing a single `Arc<AppState>`.
async fn serve_connection<R, W>(state: Arc<AppState>, reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = reader.lines();

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
            writer.write_all(&payload).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }
    }

    Ok(())
}

/// Run an embedded MCP server over stdin/stdout. This is the pre-central
/// behavior: one full server (ClickHouse client + caches) per process, owned
/// by the agent session that spawned it. Used directly when the central server
/// is disabled, and as the transparent fallback when it is unreachable.
///
/// `session_scope` carries the `--project-only` restriction; it is `None` for
/// ordinary unscoped serving.
pub async fn run_stdio(cfg: AppConfig, session_scope: Option<SessionOriginScope>) -> Result<()> {
    let state = AppState::build(cfg, session_scope)?;
    serve_connection(
        state,
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

/// Run the shared central MCP server, listening on a Unix domain socket.
///
/// Builds the heavy `AppState` exactly once, then accepts connections and
/// serves each on its own task sharing that single state. One bad client never
/// takes down the server; transient `accept` errors back off and retry. A
/// SIGTERM/SIGINT handler unlinks the socket so a clean `moraine down` leaves
/// no stale socket behind.
#[cfg(unix)]
pub async fn run_socket(cfg: AppConfig, socket_path: PathBuf) -> Result<()> {
    use tokio::net::{UnixListener, UnixStream};

    // The central server is shared by sessions from every project on the
    // host, so it always runs unscoped; `--project-only` sessions run
    // embedded instead (see `apps/moraine-mcp`).
    let state = AppState::build(cfg, None)?;

    if let Some(parent) = socket_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        use std::os::unix::fs::DirBuilderExt;
        // 0o700 on directories we create, so the socket is never reachable
        // through a fresh directory even before its own permissions are set.
        // An existing directory's mode is left alone (it may be shared, e.g.
        // /tmp for an absolute central_socket_path override).
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
            .with_context(|| format!("failed to create socket directory {}", parent.display()))?;
    }

    // Stale-socket dance: if a live server already owns the path, this start is
    // a no-op (idempotent `moraine up`). A dead socket file needs no removal —
    // the rename below atomically replaces it.
    if socket_path.exists() && UnixStream::connect(&socket_path).await.is_ok() {
        warn!(
            "central MCP server already listening at {}; nothing to do",
            socket_path.display()
        );
        return Ok(());
    }

    // Bind on a private temp path, restrict it, then atomically rename into
    // place. The socket is therefore never connectable at the public path with
    // umask-derived (possibly world-connectable) permissions — there is no
    // bind-then-chmod window. Unix sockets are bound to the inode, so the
    // listener keeps accepting through the renamed path.
    let tmp_path = socket_path.with_extension(format!("{}.tmp", std::process::id()));
    let _ = std::fs::remove_file(&tmp_path);
    let listener = UnixListener::bind(&tmp_path)
        .with_context(|| format!("failed to bind MCP socket {}", tmp_path.display()))?;

    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))
            .inspect_err(|_| {
                let _ = std::fs::remove_file(&tmp_path);
            })
            .with_context(|| {
                format!(
                    "failed to set 0o600 permissions on MCP socket {}",
                    tmp_path.display()
                )
            })?;
    }

    std::fs::rename(&tmp_path, &socket_path)
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp_path);
        })
        .with_context(|| {
            format!(
                "failed to move MCP socket into place at {}",
                socket_path.display()
            )
        })?;

    spawn_socket_cleanup_on_signal(socket_path.clone());

    debug!("central MCP server listening on {}", socket_path.display());

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let conn_state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = serve_socket_connection(conn_state, stream).await {
                        debug!("mcp connection ended: {}", err);
                    }
                });
            }
            Err(err) => {
                // e.g. EMFILE under fd pressure: log and keep serving rather
                // than tearing down every other session's connection.
                warn!("mcp accept error: {}", err);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(unix)]
async fn serve_socket_connection(
    state: Arc<AppState>,
    stream: tokio::net::UnixStream,
) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    serve_connection(state, BufReader::new(read_half), write_half).await
}

#[cfg(unix)]
fn spawn_socket_cleanup_on_signal(socket_path: PathBuf) {
    use tokio::signal::unix::{signal, SignalKind};

    tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) {
            Ok(sig) => sig,
            Err(err) => {
                warn!("failed to install SIGTERM handler: {}", err);
                return;
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(sig) => sig,
            Err(err) => {
                warn!("failed to install SIGINT handler: {}", err);
                return;
            }
        };

        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }

        let _ = std::fs::remove_file(&socket_path);
        std::process::exit(0);
    });
}

/// Run a near-zero-cost stdio<->socket proxy for a single agent session.
///
/// No ClickHouse client, no caches, no multi-threaded runtime: just two byte
/// pumps. Because it never parses the JSON, it cannot perturb the framing
/// (id presence, compact spacing, etc.) — it forwards bytes verbatim.
///
/// Half-close semantics matter: when the agent closes stdin we must *not* abort
/// the downstream copy, or a large in-flight `tools/call` response would be
/// truncated. So the downstream pump (server -> stdout) is authoritative for
/// exit; on stdin EOF we shut down the socket write half (signalling EOF to the
/// server) and keep draining downstream until the server closes its side.
#[cfg(unix)]
pub async fn run_proxy(stream: tokio::net::UnixStream) -> Result<()> {
    proxy_streams(tokio::io::stdin(), tokio::io::stdout(), stream).await
}

/// Core of [`run_proxy`], generic over the client side so it can be exercised
/// with in-memory streams in tests. `client_in`/`client_out` are stdin/stdout
/// in production; `stream` is the connection to the central server.
#[cfg(unix)]
async fn proxy_streams<I, O>(
    mut client_in: I,
    mut client_out: O,
    stream: tokio::net::UnixStream,
) -> Result<()>
where
    I: AsyncRead + Unpin,
    O: AsyncWrite + Unpin,
{
    let (mut sock_read, mut sock_write) = stream.into_split();

    let upstream = async {
        let _ = tokio::io::copy(&mut client_in, &mut sock_write).await;
        // Signal EOF to the server so it can finish any in-flight response and
        // close its write half. Ignore errors (server may already be gone).
        let _ = sock_write.shutdown().await;
    };

    let downstream = async {
        let _ = tokio::io::copy(&mut sock_read, &mut client_out).await;
        let _ = client_out.flush().await;
    };

    tokio::pin!(upstream);
    tokio::pin!(downstream);

    tokio::select! {
        // Server closed its write half (normal end, or crash/EOF): we are done.
        _ = &mut downstream => {}
        // Agent finished sending and we half-closed upstream: keep draining the
        // server's response stream to completion before exiting.
        _ = &mut upstream => {
            downstream.await;
        }
    }

    Ok(())
}

/// Entry point used by `moraine run mcp` (the command agents register).
///
/// When the central server is enabled (the default), connect to its socket and
/// proxy; if the socket is missing, refused, or slow to answer, fall back to an
/// embedded stdio server so correctness never depends on the daemon being up.
/// When central mode is disabled, run embedded directly (pre-central behavior).
pub async fn run_mcp_entry(cfg: AppConfig) -> Result<()> {
    #[cfg(unix)]
    if cfg.mcp.use_central_server {
        use tokio::net::UnixStream;

        let socket_path = PathBuf::from(&cfg.mcp.central_socket_path);
        let dur = std::time::Duration::from_millis(cfg.mcp.central_connect_timeout_ms);
        match tokio::time::timeout(dur, UnixStream::connect(&socket_path)).await {
            Ok(Ok(stream)) => {
                debug!(
                    "proxying stdio to central MCP server at {}",
                    socket_path.display()
                );
                return run_proxy(stream).await;
            }
            Ok(Err(err)) => warn!(
                "central MCP server unreachable at {} ({}); using embedded server",
                socket_path.display(),
                err
            ),
            Err(_) => warn!(
                "central MCP server connect timed out at {} after {}ms; using embedded server",
                socket_path.display(),
                cfg.mcp.central_connect_timeout_ms
            ),
        }
    }

    run_stdio(cfg, None).await
}

/// Resolve the `--project-only` scope from the directory this process was
/// launched in.
///
/// Harnesses record the working directory their own process reported, which
/// can be a logical path (the shell's `$PWD`, possibly through symlinks) or a
/// fully resolved one. To match either spelling, the scope contains the
/// launch directory as the OS reports it, its canonicalized form, and `$PWD`
/// when it names the same directory.
pub fn project_scope_from_launch_dir() -> Result<SessionOriginScope> {
    let cwd = std::env::current_dir()
        .context("--project-only requires a readable current working directory")?;
    let canonical_cwd = cwd.canonicalize().ok();

    let mut roots: Vec<String> = vec![cwd.to_string_lossy().into_owned()];
    if let Some(canonical) = &canonical_cwd {
        roots.push(canonical.to_string_lossy().into_owned());
    }
    if let Ok(pwd) = std::env::var("PWD") {
        let same_dir = match (
            std::path::Path::new(&pwd).canonicalize().ok(),
            &canonical_cwd,
        ) {
            (Some(pwd_canonical), Some(canonical)) => &pwd_canonical == canonical,
            _ => false,
        };
        if same_dir {
            roots.push(pwd);
        }
    }

    SessionOriginScope::from_roots(roots).ok_or_else(|| {
        anyhow!(
            "--project-only could not resolve an absolute project root from the launch directory"
        )
    })
}

/// Non-Unix platforms do not support the Unix-socket central server. The
/// `moraine run mcp` entry point still works (embedded only) via the
/// `run_mcp_entry` fallback above.
#[cfg(not(unix))]
pub async fn run_socket(_cfg: AppConfig, _socket_path: std::path::PathBuf) -> Result<()> {
    anyhow::bail!("the central MCP socket server is only supported on Unix platforms")
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

    #[tokio::test]
    async fn initialize_advertises_project_scope_in_instructions() {
        let scope = SessionOriginScope::from_roots(["/work/project"]).expect("valid scope");
        let state = AppState::build(AppConfig::default(), Some(scope)).expect("build state");
        let response = state
            .handle_request(RpcRequest {
                id: Some(json!(1)),
                method: "initialize".to_string(),
                params: json!({}),
            })
            .await
            .expect("initialize response");

        let instructions = response["result"]["instructions"]
            .as_str()
            .expect("scoped server advertises instructions");
        assert!(instructions.contains("/work/project"));

        // Unscoped servers must not grow an instructions field.
        let unscoped = AppState::build(AppConfig::default(), None).expect("build state");
        let response = unscoped
            .handle_request(RpcRequest {
                id: Some(json!(1)),
                method: "initialize".to_string(),
                params: json!({}),
            })
            .await
            .expect("initialize response");
        assert!(response["result"].get("instructions").is_none());
    }

    #[tokio::test]
    async fn initialize_does_not_prewarm_by_default() {
        let state = test_state();
        let response = state
            .handle_request(RpcRequest {
                id: Some(json!(1)),
                method: "initialize".to_string(),
                params: json!({}),
            })
            .await
            .expect("initialize response");

        assert_eq!(
            response["result"]["serverInfo"]["name"],
            json!("moraine-mcp")
        );
        assert!(!state.prewarm_started.load(Ordering::Acquire));
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
            list_sessions["inputSchema"]["properties"]["mode"]["anyOf"],
            json!([
                {
                    "type": "string",
                    "enum": ["web_search", "mcp_internal", "tool_calling", "chat"]
                },
                { "type": "null" }
            ])
        );
        assert_eq!(
            list_sessions["inputSchema"]["properties"]["sort"]["anyOf"],
            json!([
                {
                    "type": "string",
                    "enum": ["desc", "asc"]
                },
                { "type": "null" }
            ])
        );
        for field in ["mode", "sort"] {
            let variants = list_sessions["inputSchema"]["properties"][field]["anyOf"]
                .as_array()
                .expect("nullable enum anyOf");
            for variant in variants {
                if variant["type"] == json!("string") {
                    assert!(
                        !variant["enum"]
                            .as_array()
                            .expect("string enum")
                            .iter()
                            .any(Value::is_null),
                        "{field} string enum branch must not include null"
                    );
                }
            }
        }
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

    #[test]
    fn app_state_build_succeeds_without_live_clickhouse() {
        // Building state only constructs the HTTP client and caches; it must
        // not require ClickHouse to be reachable.
        let state = AppState::build(AppConfig::default(), None).expect("build state");
        assert!(!state.prewarm_started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn serve_connection_frames_one_response_per_request() {
        let state = AppState::build(AppConfig::default(), None).expect("build state");

        // Two requests separated by a blank line (which must be skipped).
        let input = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n",
            "\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n",
        )
        .as_bytes()
        .to_vec();

        let mut out: Vec<u8> = Vec::new();
        serve_connection(state, BufReader::new(&input[..]), &mut out)
            .await
            .expect("serve connection");

        let text = String::from_utf8(out).expect("utf8");
        let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2, "exactly one response per request: {text}");

        let first: Value = serde_json::from_str(lines[0]).expect("json line 1");
        assert_eq!(first["id"], json!(1));
        assert_eq!(first["result"]["serverInfo"]["name"], json!("moraine-mcp"));

        let second: Value = serde_json::from_str(lines[1]).expect("json line 2");
        assert_eq!(second["id"], json!(2));
        assert_eq!(second["result"], json!({}));
    }

    #[cfg(unix)]
    fn unique_socket_path(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::AtomicU32;
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        // Keep the path short to stay well under the ~104-byte sun_path limit.
        std::path::PathBuf::from(format!(
            "/tmp/moraine-{tag}-{}-{nanos}-{n}.sock",
            std::process::id()
        ))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_socket_serves_initialize_over_unix_socket() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        use tokio::net::UnixStream;

        let sock = unique_socket_path("serve");
        let _ = std::fs::remove_file(&sock);

        let server_sock = sock.clone();
        let server = tokio::spawn(async move {
            let _ = run_socket(AppConfig::default(), server_sock).await;
        });

        // Connect with a few retries while the listener comes up.
        let mut stream = None;
        for _ in 0..50 {
            match UnixStream::connect(&sock).await {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
        let mut stream = stream.expect("connect to central socket");

        // The bind-restrict-rename dance must leave the public path user-only.
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&sock)
                .expect("socket metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "socket must be user-only (0o600)");
        }

        stream
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"initialize\",\"params\":{}}\n")
            .await
            .expect("write request");

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read response");

        let resp: Value = serde_json::from_str(line.trim()).expect("json response");
        assert_eq!(resp["id"], json!(7));
        assert_eq!(resp["result"]["serverInfo"]["name"], json!("moraine-mcp"));

        server.abort();
        let _ = std::fs::remove_file(&sock);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn proxy_streams_drains_downstream_after_client_eof() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        use tokio::net::UnixStream;

        let (client_side, server_side) = UnixStream::pair().expect("socket pair");

        // Fake server: read the (small) request, then write a LARGE response and
        // close its write half. If the proxy aborted downstream on client stdin
        // EOF, this response would be truncated.
        const BIG: usize = 1_000_000;
        let server = tokio::spawn(async move {
            let (read_half, mut write_half) = server_side.into_split();
            let mut reader = BufReader::new(read_half);
            let mut req = String::new();
            reader
                .read_line(&mut req)
                .await
                .expect("server read request");
            write_half
                .write_all(&vec![b'x'; BIG])
                .await
                .expect("server write big");
            write_half.write_all(b"\n").await.expect("server write nl");
            write_half.shutdown().await.expect("server shutdown");
        });

        // Client stdin is a tiny request that hits EOF immediately.
        let client_in = std::io::Cursor::new(b"{\"id\":1}\n".to_vec());
        let mut client_out: Vec<u8> = Vec::new();

        proxy_streams(client_in, &mut client_out, client_side)
            .await
            .expect("proxy");
        server.await.expect("server task");

        assert_eq!(
            client_out.len(),
            BIG + 1,
            "downstream response must not be truncated when stdin closes first"
        );
    }
}
