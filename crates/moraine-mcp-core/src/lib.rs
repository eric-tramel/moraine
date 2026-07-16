#![recursion_limit = "256"]

pub mod contract;
mod file_attention_v1;
mod list_sessions_v1;
mod open_v1;
mod private_proxy;
mod search_sessions_v1;

use anyhow::{anyhow, Context, Result};
use moraine_config::{AppConfig, KNOWN_INGEST_HARNESSES};
use moraine_conversations::{BackendRepositoryRouter, RepoError};
pub use moraine_conversations::{ConversationRepository, SessionOriginScope};
pub use private_proxy::private_route_deadline;
#[cfg(unix)]
pub use private_proxy::{negotiate_private_route, PrivateProxyConnection, PrivateRouteNegotiation};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::future::{pending, Future};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex as StdMutex,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestLimitSource {
    Automatic,
    Config,
}

impl RequestLimitSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::Config => "config",
        }
    }
}

const AUTOMATIC_MAX_PARALLEL_REQUESTS: usize = 8;
const MAX_QUEUED_REQUESTS: usize = 16;
const REQUEST_DEADLINE: std::time::Duration = std::time::Duration::from_secs(4);

fn effective_max_parallel_requests(configured: Option<u16>) -> (usize, RequestLimitSource) {
    let (requested, source) = match configured {
        Some(configured) => (usize::from(configured), RequestLimitSource::Config),
        None => (
            AUTOMATIC_MAX_PARALLEL_REQUESTS,
            RequestLimitSource::Automatic,
        ),
    };
    (requested.clamp(1, Semaphore::MAX_PERMITS), source)
}

fn build_request_admission(cfg: &AppConfig) -> Arc<RequestAdmission> {
    let (max_parallel_requests, source) =
        effective_max_parallel_requests(cfg.mcp.max_parallel_requests);
    info!(
        max_parallel_requests,
        max_queued_requests = MAX_QUEUED_REQUESTS,
        request_deadline_ms = REQUEST_DEADLINE.as_millis(),
        source = source.as_str(),
        "configured bounded MCP retrieval admission"
    );
    Arc::new(RequestAdmission::new(
        max_parallel_requests,
        MAX_QUEUED_REQUESTS,
        REQUEST_DEADLINE,
    ))
}

#[derive(Debug)]
struct RequestAdmission {
    execution: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    waiters: StdMutex<VecDeque<u64>>,
    next_ticket: AtomicU64,
    changes: watch::Sender<u64>,
    closed: AtomicBool,
    cleanup_count: AtomicUsize,
    max_executing: usize,
    max_queued: usize,
    deadline: std::time::Duration,
}

impl RequestAdmission {
    fn new(max_executing: usize, max_queued: usize, deadline: std::time::Duration) -> Self {
        let max_executing = max_executing.clamp(1, Semaphore::MAX_PERMITS);
        let max_queued = max_queued.min(Semaphore::MAX_PERMITS - max_executing);
        let (changes, _) = watch::channel(0);
        Self {
            execution: Arc::new(Semaphore::new(max_executing)),
            slots: Arc::new(Semaphore::new(max_executing + max_queued)),
            waiters: StdMutex::new(VecDeque::new()),
            next_ticket: AtomicU64::new(0),
            changes,
            closed: AtomicBool::new(false),
            cleanup_count: AtomicUsize::new(0),
            max_executing,
            max_queued,
            deadline,
        }
    }

    fn try_register(self: &Arc<Self>) -> Result<AdmissionTicket, TryAdmissionError> {
        let slot = match self.slots.clone().try_acquire_owned() {
            Ok(slot) => slot,
            Err(TryAcquireError::NoPermits) => return Err(TryAdmissionError::Full),
            Err(TryAcquireError::Closed) => return Err(TryAdmissionError::Closed),
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(TryAdmissionError::Closed);
        }

        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.waiters
            .lock()
            .expect("request admission queue poisoned")
            .push_back(ticket);
        self.signal_change();
        Ok(AdmissionTicket {
            admission: self.clone(),
            ticket,
            slot: Some(slot),
            queued: true,
        })
    }

    fn is_front(&self, ticket: u64) -> bool {
        self.waiters
            .lock()
            .expect("request admission queue poisoned")
            .front()
            .is_some_and(|front| *front == ticket)
    }

    fn remove_waiter(&self, ticket: u64) {
        let mut waiters = self
            .waiters
            .lock()
            .expect("request admission queue poisoned");
        if let Some(index) = waiters.iter().position(|queued| *queued == ticket) {
            waiters.remove(index);
        }
        drop(waiters);
        self.signal_change();
    }

    fn signal_change(&self) {
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.execution.close();
        self.slots.close();
        self.signal_change();
    }

    fn start_cleanup(self: &Arc<Self>) -> RequestCleanupGuard {
        self.cleanup_count.fetch_add(1, Ordering::AcqRel);
        self.signal_change();
        RequestCleanupGuard {
            admission: self.clone(),
        }
    }

    async fn wait_for_cleanup_until(&self, deadline: tokio::time::Instant) {
        let mut changes = self.changes.subscribe();
        while self.cleanup_count.load(Ordering::Acquire) != 0 {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                changed = changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum TryAdmissionError {
    Full,
    Closed,
}

struct AdmissionTicket {
    admission: Arc<RequestAdmission>,
    ticket: u64,
    slot: Option<OwnedSemaphorePermit>,
    queued: bool,
}

impl AdmissionTicket {
    async fn acquire(mut self) -> Option<RequestPermit> {
        let mut changes = self.admission.changes.subscribe();
        loop {
            if self.admission.closed.load(Ordering::Acquire) {
                return None;
            }
            if self.admission.is_front(self.ticket) {
                match self.admission.execution.clone().try_acquire_owned() {
                    Ok(execution) => {
                        self.admission.remove_waiter(self.ticket);
                        self.queued = false;
                        return Some(RequestPermit {
                            admission: self.admission.clone(),
                            execution: Some(execution),
                            _slot: self
                                .slot
                                .take()
                                .expect("registered admission ticket owns a slot"),
                        });
                    }
                    Err(TryAcquireError::NoPermits) => {}
                    Err(TryAcquireError::Closed) => return None,
                }
            }
            if changes.changed().await.is_err() {
                return None;
            }
        }
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        if self.queued {
            self.admission.remove_waiter(self.ticket);
        }
    }
}

struct RequestPermit {
    admission: Arc<RequestAdmission>,
    execution: Option<OwnedSemaphorePermit>,
    _slot: OwnedSemaphorePermit,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.execution.take();
        self.admission.signal_change();
    }
}

struct RequestCleanupGuard {
    admission: Arc<RequestAdmission>,
}

impl Drop for RequestCleanupGuard {
    fn drop(&mut self) {
        self.admission.cleanup_count.fetch_sub(1, Ordering::AcqRel);
        self.admission.signal_change();
    }
}
const QUERY_CANCELLATION_DEADLINE: std::time::Duration = std::time::Duration::from_millis(1_000);
const SERVICE_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_millis(1_250);

type QueryCancellationSlot = Arc<StdMutex<Option<String>>>;

tokio::task_local! {
    static REQUEST_QUERY_CANCELLATION: QueryCancellationSlot;
    static REQUEST_ACCEPTED_AT: std::time::Instant;
}

pub(crate) fn request_performance() -> contract::PerformanceBuilder {
    let started_at = REQUEST_ACCEPTED_AT
        .try_with(|started_at| *started_at)
        .unwrap_or_else(|_| std::time::Instant::now());
    contract::Performance::builder_from(started_at)
}

pub(crate) fn request_started_at() -> std::time::Instant {
    REQUEST_ACCEPTED_AT
        .try_with(|started_at| *started_at)
        .unwrap_or_else(|_| std::time::Instant::now())
}

pub(crate) fn backend_query_id(kind: &str) -> String {
    static QUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let sequence = QUERY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("moraine-{kind}-{}-{nanos}-{sequence}", std::process::id())
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
struct CancelledParams {
    #[serde(rename = "requestId")]
    request_id: Value,
}

#[derive(Clone)]
struct AppState {
    cfg: Arc<AppConfig>,
    repo: Arc<dyn ConversationRepository>,
    launch_dir: Option<PathBuf>,
    prewarm_started: Arc<AtomicBool>,
    request_admission: Arc<RequestAdmission>,
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
                            Err(err) => unstructured_tool_error_result(err.to_string()),
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
        let harness_filter_description = format!(
            "Optional exact, case-sensitive normalized harness filter. Supported values: {}.",
            KNOWN_INGEST_HARNESSES.join(", ")
        );
        let mut source_values = self
            .cfg
            .ingest
            .sources
            .iter()
            .map(|source| source.name.as_str())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        source_values.sort_unstable();
        source_values.dedup();
        let source_filter_description = if source_values.is_empty() {
            "Optional exact, case-sensitive ingest source filter. No ingest sources are configured for this server.".to_string()
        } else {
            format!(
                "Optional exact, case-sensitive ingest source filter. Configured values for this server: {}. Use source to distinguish sources such as pi and omp that share a harness.",
                source_values.join(", ")
            )
        };
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
                                "description": "Keyword (BM25) search query. Matching is bag-of-words: quotes and punctuation are ignored, and a quoted phrase is matched as independent terms subject to the configured minimum-match threshold, not as an exact phrase."
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
                            "harness": {
                                "type": ["string", "null"],
                                "description": harness_filter_description.as_str()
                            },
                            "source": {
                                "type": ["string", "null"],
                                "description": source_filter_description.as_str()
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
                            "harness": {
                                "type": ["string", "null"],
                                "description": harness_filter_description.as_str()
                            },
                            "source": {
                                "type": ["string", "null"],
                                "description": source_filter_description.as_str()
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
                },
                {
                    "name": contract::FILE_ATTENTION_TOOL,
                    "description": "Show every session that touched a file, across every worktree, drillable through open. Given a path, returns the agent-attention history of that file — edits, reads, and aborted attempts in the main checkout, sibling worktrees, and agent-isolation worktrees — as time-ordered typed session/event IDs. Unlike git blame, this includes work that never landed in git. Matching is by the repo-relative path tail; pass a path specific enough (not a bare basename) and check the surfaced roots for over-match.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "File to trace. Absolute paths are reduced to a repo-relative tail (by walking up to .moraine.toml/.git); a repo-relative path is used as the tail directly and gives the best cross-worktree coverage."
                            },
                            "scope": {
                                "anyOf": [
                                    { "type": "string", "enum": ["project", "all"] },
                                    { "type": "null" }
                                ],
                                "default": "project",
                                "description": "project (default) restricts results to the normalized launch-project identity even without --project-only; all is the deliberate widening path on an unscoped server. A configured --project-only origin boundary remains enforced."
                            },
                            "granularity": {
                                "anyOf": [
                                    { "type": "string", "enum": ["sessions", "events"] },
                                    { "type": "null" }
                                ],
                                "default": "sessions",
                                "description": "sessions (default) returns one rollup per session; events returns the flat touch-by-touch timeline."
                            },
                            "start_datetime": {
                                "type": ["string", "null"],
                                "description": "Optional inclusive lower bound (RFC 3339 with explicit timezone) on touch time."
                            },
                            "end_datetime": {
                                "type": ["string", "null"],
                                "description": "Optional exclusive upper bound (RFC 3339 with explicit timezone) on touch time; must be later than start_datetime."
                            },
                            "tool": {
                                "type": ["string", "null"],
                                "description": "Optional case-insensitive tool-name filter (e.g. Edit, Write, Read, Bash)."
                            },
                            "harness": {
                                "type": ["string", "null"],
                                "description": harness_filter_description.as_str()
                            },
                            "source": {
                                "type": ["string", "null"],
                                "description": source_filter_description.as_str()
                            },
                            "mutations_only": {
                                "type": ["boolean", "null"],
                                "default": false,
                                "description": "Exclude pure reads (tools named Read) when true."
                            },
                            "limit": {
                                "type": ["integer", "null"],
                                "minimum": contract::FILE_ATTENTION_MIN_LIMIT,
                                "maximum": self.cfg.mcp.max_results.max(contract::FILE_ATTENTION_MIN_LIMIT),
                                "default": contract::FILE_ATTENTION_DEFAULT_LIMIT.min(self.cfg.mcp.max_results.max(contract::FILE_ATTENTION_MIN_LIMIT)),
                                "description": "Maximum sessions (or events) returned in the body. Summary and roots are computed over all matched touches regardless."
                            }
                        },
                        "required": ["path"]
                    },
                    "outputSchema": {
                        "type": "object",
                        "required": ["schema_version", "tool", "request", "data", "warnings", "performance"],
                        "properties": {
                            "schema_version": { "type": "string" },
                            "tool": { "const": contract::FILE_ATTENTION_TOOL },
                            "request": { "type": "object" },
                            "data": {
                                "type": "object",
                                "required": ["tail", "scope", "granularity", "summary", "roots", "result_count", "truncated"]
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
            contract::FILE_ATTENTION_TOOL => self.file_attention_v1(params.arguments).await,
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

pub(crate) fn tool_success_result(text: String, payload: Value) -> Value {
    structured_tool_result(text, payload, false)
}

pub(crate) fn handled_tool_error_result(text: String, payload: Value) -> Value {
    structured_tool_result(text, payload, true)
}

fn structured_tool_result(text: String, payload: Value, is_error: bool) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": payload,
        "isError": is_error
    })
}

fn unstructured_tool_error_result(message: String) -> Value {
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

/// Map a repository error onto the MCP tool error contract. Shared by every
/// retrieval tool handler so the RepoError → ToolErrorCode mapping stays in one
/// place.
pub(crate) fn repo_error_to_contract_error(error: RepoError) -> contract::ContractError {
    match error {
        RepoError::InvalidArgument(message) | RepoError::InvalidCursor(message) => {
            contract::ContractError::new(contract::ToolErrorCode::InvalidRequest, message)
        }
        RepoError::Backend(message) | RepoError::Internal(message) => {
            contract::ContractError::new(contract::ToolErrorCode::InternalError, message)
        }
    }
}

/// Wrap a typed-ID encoding failure on repository data (not user input) as an
/// internal error. Shared by every retrieval tool handler.
pub(crate) fn internal_id_error(error: contract::ContractError) -> contract::ContractError {
    contract::ContractError::new(
        contract::ToolErrorCode::InternalError,
        format!("repository returned an invalid MCP identifier component: {error}"),
    )
}

impl AppState {
    fn with_repository(
        cfg: Arc<AppConfig>,
        repo: Arc<dyn ConversationRepository>,
        prewarm_started: Arc<AtomicBool>,
        launch_dir: Option<PathBuf>,
        request_admission: Arc<RequestAdmission>,
    ) -> Arc<AppState> {
        Arc::new(AppState {
            cfg,
            repo,
            launch_dir,
            prewarm_started,
            request_admission,
        })
    }

    fn embedded(cfg: AppConfig, repo: Arc<dyn ConversationRepository>) -> Arc<AppState> {
        let request_admission = build_request_admission(&cfg);
        Self::with_repository(
            cfg.into(),
            repo,
            Arc::new(AtomicBool::new(false)),
            std::env::current_dir().ok(),
            request_admission,
        )
    }
}

pub(crate) struct QueryCancellationGuard {
    query_id: String,
    slot: Option<QueryCancellationSlot>,
}

impl QueryCancellationGuard {
    pub(crate) fn new(query_id: String) -> Self {
        let slot = REQUEST_QUERY_CANCELLATION
            .try_with(|slot| slot.clone())
            .ok();
        if let Some(slot) = &slot {
            *slot.lock().expect("query cancellation slot poisoned") = Some(query_id.clone());
        }
        Self { query_id, slot }
    }

    pub(crate) fn disarm(&mut self) {
        let Some(slot) = &self.slot else {
            return;
        };
        let mut registered = slot.lock().expect("query cancellation slot poisoned");
        if registered.as_ref() == Some(&self.query_id) {
            *registered = None;
        }
    }
}

/// Drive a single newline-delimited JSON-RPC connection to completion.
///
/// Slow repository requests execute concurrently and may complete out of order.
/// Response encoding and writes stay on this connection task so JSON frames can
/// never interleave. Admission is shared across socket connections; requests
/// wait in a finite FIFO queue when execution permits are busy, share one wall
/// deadline from frame acceptance, and are rejected immediately when it is full.
async fn serve_connection_with_first_line<R, W>(
    state: Arc<AppState>,
    reader: R,
    writer: W,
    first_line: Option<Vec<u8>>,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_connection_with_shutdown(state, reader, writer, first_line, pending()).await
}

async fn serve_connection_with_shutdown<R, W, S>(
    state: Arc<AppState>,
    reader: R,
    writer: W,
    first_line: Option<Vec<u8>>,
    shutdown: S,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    S: Future<Output = ()>,
{
    serve_connection_with_lifecycle(state, reader, writer, first_line, shutdown, pending()).await
}

async fn wait_for_connection_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow_and_update() {
            return;
        }
    }
}

async fn serve_connection_with_lifecycle<R, W, S, D>(
    state: Arc<AppState>,
    reader: R,
    mut writer: W,
    first_line: Option<Vec<u8>>,
    shutdown: S,
    peer_disconnected: D,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    S: Future<Output = ()>,
    D: Future<Output = ()>,
{
    let mut requests = JoinSet::new();
    let mut active = HashMap::new();
    let (request_shutdown_tx, request_shutdown_rx) = watch::channel(false);
    let mut shutdown_requested = false;
    let mut connection_error = None;
    tokio::pin!(shutdown);
    tokio::pin!(peer_disconnected);

    if let Some(first_line) = first_line {
        let first_line =
            std::str::from_utf8(&first_line).context("incoming RPC line is not valid UTF-8")?;
        if let Some(response) = dispatch_rpc_line(
            &state,
            first_line,
            &mut requests,
            &mut active,
            request_shutdown_rx.clone(),
        )
        .await?
        {
            tokio::select! {
                result = write_rpc_response(&mut writer, &response) => result?,
                _ = &mut shutdown => shutdown_requested = true,
            }
        }
    }

    let mut lines = reader.lines();
    while !shutdown_requested && connection_error.is_none() {
        tokio::select! {
            _ = &mut shutdown => {
                shutdown_requested = true;
            }
            completed = requests.join_next(), if !requests.is_empty() => {
                if let Some(response) = finish_request(&state, completed, &mut active).await {
                    tokio::select! {
                        result = write_rpc_response(&mut writer, &response) => {
                            if let Err(error) = result {
                                connection_error = Some(error);
                            }
                        }
                        _ = &mut shutdown => shutdown_requested = true,
                    }
                }
            }
            next_line = lines.next_line() => {
                let line = match next_line {
                    Ok(Some(line)) => line,
                    Ok(None) => break,
                    Err(error) => {
                        connection_error = Some(error.into());
                        continue;
                    }
                };
                match dispatch_rpc_line(
                    &state,
                    &line,
                    &mut requests,
                    &mut active,
                    request_shutdown_rx.clone(),
                ).await {
                    Ok(Some(response)) => {
                        tokio::select! {
                            result = write_rpc_response(&mut writer, &response) => {
                                if let Err(error) = result {
                                    connection_error = Some(error);
                                }
                            }
                            _ = &mut shutdown => shutdown_requested = true,
                        }
                    }
                    Ok(None) => {}
                    Err(error) => connection_error = Some(error),
                }
            }
        }
    }

    // EOF only closes the request half of a stream. A client may continue
    // reading responses after it has sent every frame, so admitted work keeps
    // its normal lifecycle and every completed response is serialized before
    // this connection closes. Socket transports separately report a full peer
    // hangup, while service shutdown and write failures move directly to
    // cancellation below.
    if !shutdown_requested && connection_error.is_none() {
        while !requests.is_empty() {
            let completed = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut peer_disconnected => break,
                completed = requests.join_next() => completed,
            };
            if let Some(response) = finish_request(&state, completed, &mut active).await {
                tokio::select! {
                    result = write_rpc_response(&mut writer, &response) => {
                        if let Err(error) = result {
                            connection_error = Some(error);
                            break;
                        }
                    }
                    _ = &mut shutdown => break,
                }
            }
        }
    }

    let _ = request_shutdown_tx.send(true);

    for request in active.values_mut() {
        if let Some(cancellation) = request.cancellation.take() {
            let _ = cancellation.send(());
        }
    }

    // Request tasks own backend cancellation and must be given time to finish
    // it before the connection or service reports itself drained.
    let cancellation_deadline = tokio::time::Instant::now() + QUERY_CANCELLATION_DEADLINE;
    while !requests.is_empty() {
        match tokio::time::timeout_at(cancellation_deadline, requests.join_next()).await {
            Ok(completed) => {
                let _ = finish_request(&state, completed, &mut active).await;
            }
            Err(_) => break,
        }
    }
    requests.abort_all();
    while requests.join_next().await.is_some() {}
    state
        .request_admission
        .wait_for_cleanup_until(cancellation_deadline)
        .await;

    if let Some(error) = connection_error {
        Err(error)
    } else {
        Ok(())
    }
}

async fn serve_connection<R, W>(state: Arc<AppState>, reader: R, writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_connection_with_first_line(state, reader, writer, None).await
}

struct ActiveRequest {
    id: Value,
    task_id: tokio::task::Id,
    query_cancellation: QueryCancellationSlot,
    cancellation: Option<oneshot::Sender<()>>,
}

type RequestResult = (String, Option<Value>);

async fn dispatch_rpc_line(
    state: &Arc<AppState>,
    line: &str,
    requests: &mut JoinSet<RequestResult>,
    active: &mut HashMap<String, ActiveRequest>,
    mut connection_shutdown: watch::Receiver<bool>,
) -> Result<Option<Value>> {
    let accepted_at = tokio::time::Instant::now();
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }

    debug!("incoming rpc line: {}", line);
    let req = match serde_json::from_str::<RpcRequest>(line) {
        Ok(req) => req,
        Err(err) => {
            warn!("failed to parse rpc request: {}", err);
            return Ok(None);
        }
    };

    if req.method == "notifications/cancelled" {
        if let Ok(params) = serde_json::from_value::<CancelledParams>(req.params) {
            if let Some(request) = active.get_mut(&request_key(&params.request_id)?) {
                if let Some(cancellation) = request.cancellation.take() {
                    let _ = cancellation.send(());
                }
            }
        }
        return Ok(None);
    }

    let Some(admitted_tool) = admitted_tool_call(&req, state.cfg.mcp.max_results) else {
        return Ok(state.handle_request(req).await);
    };

    let id = req
        .id
        .clone()
        .expect("admitted repository requests always carry an id");
    let key = request_key(&id)?;
    if active.contains_key(&key) {
        return Ok(Some(rpc_err(
            id,
            -32600,
            "duplicate request id is already in flight",
        )));
    }

    let admission_ticket = match state.request_admission.try_register() {
        Ok(ticket) => ticket,
        Err(TryAdmissionError::Full) => {
            return Ok(Some(admission_error_response(
                id,
                &admitted_tool,
                accepted_at,
                state.request_admission.deadline,
                AdmissionErrorKind::QueueFull {
                    max_executing: state.request_admission.max_executing,
                    max_queued: state.request_admission.max_queued,
                },
            )));
        }
        Err(TryAdmissionError::Closed) => return Ok(None),
    };

    let (cancel_tx, mut cancel_rx) = oneshot::channel();
    let request_query_id = backend_query_id("request");
    let query_cancellation = Arc::new(StdMutex::new(None));
    let task_query_cancellation = query_cancellation.clone();
    let request_state = state.clone();
    let task_key = key.clone();
    let task_id = id.clone();
    let deadline_duration = request_state.request_admission.deadline;
    let deadline = accepted_at + deadline_duration;
    let task = requests.spawn(async move {
        enum AdmissionOutcome {
            Admitted(RequestPermit),
            Cancelled,
            DeadlineExceeded,
        }

        let admission = tokio::select! {
            biased;
            _ = &mut cancel_rx => AdmissionOutcome::Cancelled,
            _ = wait_for_connection_shutdown(&mut connection_shutdown) => AdmissionOutcome::Cancelled,
            _ = tokio::time::sleep_until(deadline) => AdmissionOutcome::DeadlineExceeded,
            permit = admission_ticket.acquire() => match permit {
                Some(permit) => AdmissionOutcome::Admitted(permit),
                None => AdmissionOutcome::Cancelled,
            },
        };
        let permit = match admission {
            AdmissionOutcome::Admitted(permit) => permit,
            AdmissionOutcome::Cancelled => return (task_key, None),
            AdmissionOutcome::DeadlineExceeded => {
                return (
                    task_key,
                    Some(admission_error_response(
                        task_id,
                        &admitted_tool,
                        accepted_at,
                        deadline_duration,
                        AdmissionErrorKind::DeadlineExceeded,
                    )),
                );
            }
        };
        let query_cancellation = task_query_cancellation;
        *query_cancellation
            .lock()
            .expect("query cancellation slot poisoned") = Some(request_query_id.clone());

        enum Outcome {
            Completed(Option<Value>),
            Cancelled,
            DeadlineExceeded,
        }
        let outcome = {
            let request = REQUEST_ACCEPTED_AT.scope(
                accepted_at.into_std(),
                REQUEST_QUERY_CANCELLATION.scope(
                    query_cancellation.clone(),
                    moraine_conversations::with_repository_query_deadline(
                        request_query_id,
                        deadline,
                        request_state.handle_request(req),
                    ),
                ),
            );
            tokio::pin!(request);
            tokio::select! {
                biased;
                _ = &mut cancel_rx => Outcome::Cancelled,
                _ = wait_for_connection_shutdown(&mut connection_shutdown) => Outcome::Cancelled,
                _ = tokio::time::sleep_until(deadline) => Outcome::DeadlineExceeded,
                response = &mut request => Outcome::Completed(response),
            }
        };
        let response = match outcome {
            Outcome::Completed(response) => response,
            Outcome::Cancelled => {
                cancel_registered_query(&request_state, &query_cancellation).await;
                None
            }
            Outcome::DeadlineExceeded => {
                spawn_deadline_cancellation(
                    request_state,
                    query_cancellation,
                    permit,
                );
                return (
                    task_key,
                    Some(admission_error_response(
                        task_id,
                        &admitted_tool,
                        accepted_at,
                        deadline_duration,
                        AdmissionErrorKind::DeadlineExceeded,
                    )),
                );
            }
        };
        (task_key, response)
    });
    active.insert(
        key,
        ActiveRequest {
            id,
            task_id: task.id(),
            query_cancellation,
            cancellation: Some(cancel_tx),
        },
    );
    Ok(None)
}

#[derive(Clone, Debug)]
struct AdmittedToolCall {
    name: String,
    request: Value,
}

enum AdmissionErrorKind {
    QueueFull {
        max_executing: usize,
        max_queued: usize,
    },
    DeadlineExceeded,
}

fn admission_error_response(
    id: Value,
    tool: &AdmittedToolCall,
    accepted_at: tokio::time::Instant,
    deadline: std::time::Duration,
    kind: AdmissionErrorKind,
) -> Value {
    let deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX);
    let error = match kind {
        AdmissionErrorKind::QueueFull {
            max_executing,
            max_queued,
        } => contract::ContractError::new(
            contract::ToolErrorCode::DeadlineExceeded,
            "MCP retrieval queue is full; retry later",
        )
        .with_details(json!({
            "reason": "queue_full",
            "max_executing": max_executing,
            "max_queued": max_queued,
        })),
        AdmissionErrorKind::DeadlineExceeded => contract::ContractError::new(
            contract::ToolErrorCode::DeadlineExceeded,
            "MCP retrieval request exceeded its end-to-end deadline",
        )
        .with_details(json!({
            "reason": "request_deadline",
            "deadline_ms": deadline_ms,
        })),
    };
    let payload = serde_json::to_value(contract::ToolErrorEnvelope::error(
        tool.name.clone(),
        tool.request.clone(),
        error,
        contract::Performance::from_elapsed(accepted_at.elapsed()),
    ))
    .expect("MCP admission error envelope is serializable");
    rpc_ok(
        id,
        handled_tool_error_result(
            format!(
                "{} failed: {}",
                tool.name,
                payload["error"]["message"]
                    .as_str()
                    .unwrap_or("request failed")
            ),
            payload,
        ),
    )
}

async fn cancel_registered_query(state: &AppState, slot: &QueryCancellationSlot) {
    let query_id = slot
        .lock()
        .expect("query cancellation slot poisoned")
        .take();
    let Some(query_id) = query_id else {
        return;
    };
    cancel_query_with_deadline(state, &query_id).await;
}

fn spawn_deadline_cancellation(
    state: Arc<AppState>,
    slot: QueryCancellationSlot,
    permit: RequestPermit,
) {
    let cleanup = state.request_admission.start_cleanup();
    tokio::spawn(async move {
        let _cleanup = cleanup;
        cancel_registered_query(&state, &slot).await;
        drop(permit);
    });
}

pub(crate) async fn cancel_query_with_deadline(state: &AppState, query_id: &str) {
    match tokio::time::timeout(
        QUERY_CANCELLATION_DEADLINE,
        state.repo.cancel_query(query_id),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(
            query_id,
            error = %error,
            "failed to cancel abandoned MCP ClickHouse query"
        ),
        Err(_) => warn!(
            query_id,
            "timed out cancelling abandoned MCP ClickHouse query"
        ),
    }
}

fn admitted_tool_call(req: &RpcRequest, max_results: u16) -> Option<AdmittedToolCall> {
    if req.id.is_none() || req.method != "tools/call" {
        return None;
    }
    let Ok(params) = serde_json::from_value::<ToolCallParams>(req.params.clone()) else {
        return None;
    };

    let valid = match params.name.as_str() {
        contract::SEARCH_SESSIONS_TOOL => {
            serde_json::from_value::<contract::SearchSessionsArgs>(params.arguments.clone())
                .is_ok_and(|args| args.validate().is_ok())
        }
        contract::LIST_SESSIONS_TOOL => {
            serde_json::from_value::<contract::ListSessionsArgs>(params.arguments.clone())
                .is_ok_and(|args| args.validate(max_results).is_ok())
        }
        contract::OPEN_TOOL => {
            serde_json::from_value::<contract::OpenV1Args>(params.arguments.clone())
                .is_ok_and(|args| args.validate().is_ok())
        }
        contract::FILE_ATTENTION_TOOL => {
            serde_json::from_value::<contract::FileAttentionArgs>(params.arguments.clone())
                .is_ok_and(|args| args.validate(max_results).is_ok())
        }
        _ => false,
    };
    valid.then_some(AdmittedToolCall {
        name: params.name,
        request: params.arguments,
    })
}

fn request_key(id: &Value) -> Result<String> {
    serde_json::to_string(id).context("failed to encode JSON-RPC request id")
}

async fn finish_request(
    state: &AppState,
    completed: Option<Result<RequestResult, tokio::task::JoinError>>,
    active: &mut HashMap<String, ActiveRequest>,
) -> Option<Value> {
    match completed {
        Some(Ok((key, response))) => {
            active.remove(&key);
            response
        }
        Some(Err(error)) => {
            let failed = active
                .iter()
                .find_map(|(key, request)| (request.task_id == error.id()).then(|| key.clone()))
                .and_then(|key| active.remove(&key));
            debug!("mcp request task ended unexpectedly: {error}");
            if let Some(request) = failed {
                cancel_registered_query(state, &request.query_cancellation).await;
                Some(rpc_err(request.id, -32603, "request failed"))
            } else {
                None
            }
        }
        None => None,
    }
}

async fn write_rpc_response<W>(writer: &mut W, response: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(response)?;
    writer.write_all(&payload).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

/// Run an embedded MCP server over stdin/stdout using a repository selected and
/// constructed by the owning application.
///
/// Repository construction, project routing, schema policy, and scoped
/// fallback all live outside mcp-core. The injected repository remains fixed
/// for the lifetime of this stdio connection.
pub async fn run_stdio_with_repository(
    cfg: AppConfig,
    repository: Arc<dyn ConversationRepository>,
) -> Result<()> {
    let state = AppState::embedded(cfg, repository);
    serve_connection(
        state,
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

#[cfg(unix)]
#[derive(Clone)]
struct BackendPrewarmGates {
    by_backend: Arc<std::collections::HashMap<String, Arc<AtomicBool>>>,
}

#[cfg(unix)]
impl BackendPrewarmGates {
    fn new(cfg: &AppConfig) -> Self {
        let by_backend = cfg
            .backends
            .keys()
            .map(|name| (name.clone(), Arc::new(AtomicBool::new(false))))
            .collect();
        Self {
            by_backend: Arc::new(by_backend),
        }
    }

    fn for_backend(&self, backend_name: &str) -> Result<Arc<AtomicBool>> {
        self.by_backend
            .get(backend_name)
            .cloned()
            .ok_or_else(|| anyhow!("selected backend '{backend_name}' has no MCP prewarm gate"))
    }
}

#[cfg(unix)]
#[derive(Clone)]
struct SocketState {
    cfg: Arc<AppConfig>,
    router: Arc<BackendRepositoryRouter>,
    prewarm_gates: BackendPrewarmGates,
    request_admission: Arc<RequestAdmission>,
    shutdown: watch::Receiver<bool>,
}

/// Run the shared MCP server on a Unix domain socket using the daemon-owned
/// backend repository router.
///
/// The caller owns process supervision and supplies `shutdown`; this core
/// never installs signal handlers or terminates the process. Each accepted
/// connection negotiates and pins one repository handle, while connections to
/// the same backend share both its repository and its MCP prewarm gate. When
/// shutdown resolves, accepting stops, connection and request cancellation is
/// drained within a fixed bound, and the public socket path is unlinked.
#[cfg(unix)]
pub async fn run_socket_with_router<S>(
    cfg: Arc<AppConfig>,
    router: Arc<BackendRepositoryRouter>,
    socket_path: PathBuf,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send,
{
    use tokio::net::{UnixListener, UnixStream};
    use tokio::task::JoinSet;

    let (connection_shutdown_tx, connection_shutdown_rx) = watch::channel(false);
    let request_admission = build_request_admission(&cfg);
    let state = SocketState {
        prewarm_gates: BackendPrewarmGates::new(&cfg),
        request_admission,
        shutdown: connection_shutdown_rx,
        cfg,
        router,
    };

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

    // A live listener is a startup failure for the unified backend: returning
    // success here would leave its HTTP sibling running without the MCP half.
    // Remove a dead socket only if the path still names the inode that failed
    // the connection probe; a concurrently published replacement makes this
    // start fail rather than being unlinked.
    match std::fs::symlink_metadata(&socket_path) {
        Ok(stale) => {
            use std::os::unix::fs::MetadataExt;
            let stale_identity = (stale.dev(), stale.ino());
            if UnixStream::connect(&socket_path).await.is_ok() {
                return Err(anyhow!(
                    "failed to bind MCP socket {}: another backend is already listening",
                    socket_path.display()
                ));
            }
            match std::fs::symlink_metadata(&socket_path) {
                Ok(current) if (current.dev(), current.ino()) == stale_identity => {
                    std::fs::remove_file(&socket_path).with_context(|| {
                        format!(
                            "failed to remove stale MCP socket {}",
                            socket_path.display()
                        )
                    })?;
                }
                Ok(_) => {
                    return Err(anyhow!(
                        "failed to bind MCP socket {}: socket path changed during startup",
                        socket_path.display()
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to inspect MCP socket {}", socket_path.display())
                    });
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to inspect MCP socket {}", socket_path.display())
            });
        }
    }

    // Bind on a private temp path, restrict it, then atomically rename into
    // place. The socket is therefore never connectable at the public path with
    // umask-derived (possibly world-connectable) permissions.
    let tmp_path = socket_path.with_extension(format!("{}.tmp", std::process::id()));
    let _ = std::fs::remove_file(&tmp_path);
    let listener = UnixListener::bind(&tmp_path)
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp_path);
        })
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

    // Capture the inode before publication. Cleanup only unlinks this inode,
    // so a concurrently started backend that later wins the public path can
    // never have its socket removed by this server's shutdown guard.
    let socket_identity = {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&tmp_path)
            .inspect_err(|_| {
                let _ = std::fs::remove_file(&tmp_path);
            })
            .with_context(|| {
                format!(
                    "failed to inspect MCP socket before publishing {}",
                    tmp_path.display()
                )
            })?;
        (metadata.dev(), metadata.ino())
    };

    rename_socket_noreplace(&tmp_path, &socket_path)
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp_path);
        })
        .with_context(|| {
            format!(
                "failed to publish MCP socket at {} without replacing another backend",
                socket_path.display()
            )
        })?;
    let _socket_cleanup = SocketCleanup::new(socket_path.clone(), socket_identity);

    debug!("central MCP server listening on {}", socket_path.display());

    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let conn_state = state.clone();
                        connections.spawn(async move {
                            serve_socket_connection(conn_state, stream).await
                        });
                    }
                    Err(err) => {
                        // e.g. EMFILE under fd pressure: log and keep serving
                        // rather than tearing down every existing session.
                        warn!("mcp accept error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
            Some(completed) = connections.join_next(), if !connections.is_empty() => {
                match completed {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => debug!("mcp connection ended: {}", err),
                    Err(err) => debug!("mcp connection task ended unexpectedly: {}", err),
                }
            }
        }
    }

    state.request_admission.close();
    let _ = connection_shutdown_tx.send(true);
    let drain_deadline = tokio::time::Instant::now() + SERVICE_DRAIN_GRACE;
    while !connections.is_empty() {
        match tokio::time::timeout_at(drain_deadline, connections.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => break,
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    state
        .request_admission
        .wait_for_cleanup_until(drain_deadline)
        .await;
    Ok(())
}

#[cfg(unix)]
struct SocketCleanup {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl SocketCleanup {
    fn new(path: PathBuf, (device, inode): (u64, u64)) -> Self {
        Self {
            path,
            device,
            inode,
        }
    }
}

#[cfg(unix)]
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;

        let Ok(metadata) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.dev() == self.device && metadata.ino() == self.inode {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
fn socket_path_cstring(path: &std::path::Path) -> std::io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;

    std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Unix socket path contains a NUL byte",
        )
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_socket_noreplace(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    let from = socket_path_cstring(from)?;
    let to = socket_path_cstring(to)?;
    // SAFETY: both C strings are alive for the call, contain no interior NUL,
    // and the directory descriptors are the platform's current-directory
    // sentinel.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn rename_socket_noreplace(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    let from = socket_path_cstring(from)?;
    let to = socket_path_cstring(to)?;
    // SAFETY: both C strings are alive for the call and contain no interior
    // NUL. RENAME_EXCL makes publication fail if `to` already exists.
    let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn rename_socket_noreplace(_from: &std::path::Path, _to: &std::path::Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace Unix socket publication is unsupported on this platform",
    ))
}

#[cfg(unix)]
fn socket_peer_disconnected(fd: std::os::fd::RawFd) -> bool {
    // A zero-length send emits no protocol bytes, but still checks whether the
    // peer retains its read half. Unlike POLLHUP, this distinguishes a client
    // write-half shutdown from a full disconnect on supported Unix sockets.
    // SAFETY: a null buffer is valid for a zero-length send, and the owning
    // socket halves outlive this monitor.
    let sent = unsafe {
        libc::send(
            fd,
            std::ptr::null(),
            0,
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        )
    };
    if sent == 0 {
        return false;
    }

    let errno = std::io::Error::last_os_error().raw_os_error();
    !matches!(
        errno,
        Some(code)
            if code == libc::EINTR || code == libc::EAGAIN || code == libc::EWOULDBLOCK
    )
}

#[cfg(unix)]
async fn wait_for_socket_disconnect(fd: std::os::fd::RawFd) {
    while !socket_peer_disconnected(fd) {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
async fn serve_socket_connection(state: SocketState, stream: tokio::net::UnixStream) -> Result<()> {
    use private_proxy::ServerFirstLine;
    use std::os::fd::AsRawFd;

    let mut shutdown = state.shutdown.clone();
    let peer_fd = stream.as_raw_fd();

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let Some(first_line) = private_proxy::read_server_first_line(&mut reader).await? else {
        return Ok(());
    };

    let (backend, replay_first_line, negotiated, launch_dir) =
        match private_proxy::classify_server_first_line(&first_line) {
            ServerFirstLine::Route { cwd } => {
                match state.router.repository_for_project_dir(Some(&cwd)).await {
                    Ok(backend) => (
                        backend,
                        None,
                        true,
                        (!cwd.is_empty()).then(|| PathBuf::from(cwd)),
                    ),
                    Err(error) => {
                        let message = format!("{error:#}");
                        private_proxy::write_route_error(&mut write_half, &message).await?;
                        return Ok(());
                    }
                }
            }
            ServerFirstLine::Incompatible => {
                private_proxy::write_incompatible_error(&mut write_half).await?;
                return Ok(());
            }
            ServerFirstLine::Raw => {
                let backend = state.router.default_repository().await?;
                (
                    backend,
                    Some(first_line),
                    false,
                    std::env::current_dir().ok(),
                )
            }
        };

    let prewarm_started = match state.prewarm_gates.for_backend(backend.backend_name()) {
        Ok(gate) => gate,
        Err(error) if negotiated => {
            private_proxy::write_route_error(&mut write_half, &error.to_string()).await?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let app_state = AppState::with_repository(
        state.cfg,
        backend.repository().clone(),
        prewarm_started,
        launch_dir,
        state.request_admission,
    );
    if negotiated {
        private_proxy::write_ack(&mut write_half).await?;
    }
    serve_connection_with_lifecycle(
        app_state,
        reader,
        write_half,
        replay_first_line,
        async move {
            if !*shutdown.borrow() {
                let _ = shutdown.changed().await;
            }
        },
        wait_for_socket_disconnect(peer_fd),
    )
    .await
}

/// Run the stdio byte pumps after a private route negotiation was accepted.
///
/// The accepted connection retains the buffered daemon reader used for the
/// private ACK. No agent stdin is consumed before this function starts.
#[cfg(unix)]
pub async fn run_proxy(connection: PrivateProxyConnection) -> Result<()> {
    let (sock_read, sock_write) = connection.into_parts();
    proxy_streams_with_halves(
        tokio::io::stdin(),
        tokio::io::stdout(),
        sock_read,
        sock_write,
    )
    .await
}

#[cfg(unix)]
async fn proxy_streams_with_halves<I, O, R, W>(
    mut client_in: I,
    mut client_out: O,
    mut sock_read: R,
    mut sock_write: W,
) -> Result<()>
where
    I: AsyncRead + Unpin,
    O: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
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

#[cfg(all(test, unix))]
async fn proxy_streams<I, O>(
    client_in: I,
    client_out: O,
    stream: tokio::net::UnixStream,
) -> Result<()>
where
    I: AsyncRead + Unpin,
    O: AsyncWrite + Unpin,
{
    let (read_half, write_half) = stream.into_split();
    proxy_streams_with_halves(client_in, client_out, BufReader::new(read_half), write_half).await
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

/// Non-Unix platforms expose the same injected API so composition roots can
/// compile portably, but attempting to start the socket listener fails.
#[cfg(not(unix))]
pub async fn run_socket_with_router<S>(
    _cfg: Arc<AppConfig>,
    _router: Arc<BackendRepositoryRouter>,
    _socket_path: PathBuf,
    _shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send,
{
    anyhow::bail!("the central MCP socket server is only supported on Unix platforms")
}

#[cfg(test)]
mod concurrency_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_conversations::{
        ConversationMode, InMemoryConversationRepository, InMemoryConversationResponses,
        McpSessionOpen, RepoConfig, RepoError, SessionMetadata,
    };

    fn repository_with_scope(
        session_scope: Option<SessionOriginScope>,
    ) -> Arc<dyn ConversationRepository> {
        Arc::new(InMemoryConversationRepository::new(RepoConfig {
            session_scope,
            ..RepoConfig::default()
        }))
    }

    fn test_state() -> Arc<AppState> {
        AppState::embedded(AppConfig::default(), repository_with_scope(None))
    }

    #[test]
    fn effective_parallel_request_limit_prefers_config_and_defaults_to_eight() {
        assert_eq!(
            effective_max_parallel_requests(Some(12)),
            (12, RequestLimitSource::Config)
        );
        assert_eq!(
            effective_max_parallel_requests(None),
            (8, RequestLimitSource::Automatic)
        );
    }

    #[test]
    fn embedded_state_uses_configured_execution_and_bounded_queue_limits() {
        let mut cfg = AppConfig::default();
        cfg.mcp.max_parallel_requests = Some(3);
        let state = AppState::embedded(cfg, repository_with_scope(None));
        assert_eq!(state.request_admission.execution.available_permits(), 3);
        assert_eq!(state.request_admission.slots.available_permits(), 19);
        assert_eq!(state.request_admission.max_queued, 16);
        assert_eq!(state.request_admission.deadline, REQUEST_DEADLINE);
    }

    #[test]
    fn embedded_state_defaults_to_eight_executing_and_sixteen_queued() {
        let state = test_state();
        assert_eq!(state.request_admission.execution.available_permits(), 8);
        assert_eq!(state.request_admission.slots.available_permits(), 24);
    }

    fn successful_test_state() -> Arc<AppState> {
        let session = McpSessionOpen {
            metadata: SessionMetadata {
                session_id: "session-success".to_string(),
                first_event_time: "2026-07-11 12:00:00.000".to_string(),
                first_event_unix_ms: 1_783_771_200_000,
                last_event_time: "2026-07-11 12:00:00.000".to_string(),
                last_event_unix_ms: 1_783_771_200_000,
                total_turns: 0,
                total_events: 0,
                user_messages: 0,
                assistant_messages: 0,
                tool_calls: 0,
                tool_results: 0,
                mode: ConversationMode::Chat,
                first_event_uid: "event-first".to_string(),
                last_event_uid: "event-last".to_string(),
                last_actor_role: "assistant".to_string(),
            },
            title: Some("Successful retrieval".to_string()),
            source: Some("test".to_string()),
            harness: None,
            inference_provider: None,
            session_slug: None,
            session_summary: None,
            turns: Vec::new(),
            completed: false,
            terminal_event_uid: None,
        };
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_session: Some(Ok(Some(session))),
                ..InMemoryConversationResponses::default()
            },
        ));
        AppState::embedded(AppConfig::default(), repository)
    }

    fn repository_error_test_state() -> Arc<AppState> {
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            InMemoryConversationResponses {
                get_mcp_session: Some(Err(RepoError::internal("open failed"))),
                list_mcp_sessions: Some(Err(RepoError::backend("list failed"))),
                search_mcp_events: Some(Err(RepoError::internal("search failed"))),
                file_attention: Some(Err(RepoError::backend("attention failed"))),
                ..InMemoryConversationResponses::default()
            },
        ));
        AppState::embedded(AppConfig::default(), repository)
    }

    async fn call_tool_rpc(state: &AppState, id: u64, tool: &str, arguments: Value) -> Value {
        state
            .handle_request(RpcRequest {
                id: Some(json!(id)),
                method: "tools/call".to_string(),
                params: json!({
                    "name": tool,
                    "arguments": arguments,
                }),
            })
            .await
            .expect("tools/call response")
    }

    fn assert_successful_tool_exchange(response: &Value, tool: &str) {
        assert_eq!(response["jsonrpc"], "2.0");
        assert!(
            response.get("error").is_none(),
            "handled tool success must use a JSON-RPC result: {response}"
        );
        assert_eq!(response["result"]["isError"], false);
        assert_ne!(
            response["result"]["structuredContent"]["schema_version"],
            contract::ERROR_SCHEMA_VERSION
        );
        assert_eq!(response["result"]["structuredContent"]["tool"], tool);
    }

    fn assert_handled_tool_error_exchange(response: &Value, tool: &str, code: &str) {
        assert_eq!(response["jsonrpc"], "2.0");
        assert!(
            response.get("error").is_none(),
            "handled tool failure must remain a successful JSON-RPC exchange: {response}"
        );
        assert_eq!(response["result"]["isError"], true);
        assert_eq!(
            response["result"]["structuredContent"]["schema_version"],
            contract::ERROR_SCHEMA_VERSION
        );
        assert_eq!(response["result"]["structuredContent"]["tool"], tool);
        assert_eq!(
            response["result"]["structuredContent"]["error"]["code"],
            code
        );
    }

    #[tokio::test]
    async fn initialize_advertises_project_scope_in_instructions() {
        let scope = SessionOriginScope::from_roots(["/work/project"]).expect("valid scope");
        let state = AppState::embedded(AppConfig::default(), repository_with_scope(Some(scope)));
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
        let unscoped = AppState::embedded(AppConfig::default(), repository_with_scope(None));
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

        assert_eq!(
            names,
            ["search_sessions", "open", "list_sessions", "file_attention"]
        );

        for tool_name in ["search_sessions", "open", "list_sessions", "file_attention"] {
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
            search["inputSchema"]["properties"]["query"]["description"],
            json!("Keyword (BM25) search query. Matching is bag-of-words: quotes and punctuation are ignored, and a quoted phrase is matched as independent terms subject to the configured minimum-match threshold, not as an exact phrase.")
        );
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

        let harness_description = json!(
            "Optional exact, case-sensitive normalized harness filter. Supported values: codex, claude-code, cursor, hermes, kimi-cli, opencode, pi-coding-agent."
        );
        let source_description = json!(
            "Optional exact, case-sensitive ingest source filter. Configured values for this server: claude, codex, cursor, cursor-sqlite, hermes, kimi-cli, omp, opencode, pi. Use source to distinguish sources such as pi and omp that share a harness."
        );
        for tool_name in ["search_sessions", "list_sessions", "file_attention"] {
            let tool = tools
                .iter()
                .find(|tool| tool["name"].as_str() == Some(tool_name))
                .expect("retrieval tool exists");
            assert_eq!(
                tool["inputSchema"]["properties"]["harness"]["description"],
                harness_description
            );
            assert_eq!(
                tool["inputSchema"]["properties"]["source"]["description"],
                source_description
            );
        }

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
    fn tools_list_source_values_follow_server_config() {
        let mut cfg = AppConfig::default();
        cfg.ingest.sources.truncate(1);
        cfg.ingest.sources[0].name = "custom-source".to_string();
        let state = AppState::embedded(cfg, repository_with_scope(None));
        let payload = state.tools_list_result();
        let search = payload["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .find(|tool| tool["name"].as_str() == Some("search_sessions"))
            .expect("search_sessions exists");

        assert_eq!(
            search["inputSchema"]["properties"]["source"]["description"],
            json!(
                "Optional exact, case-sensitive ingest source filter. Configured values for this server: custom-source. Use source to distinguish sources such as pi and omp that share a harness."
            )
        );
    }

    #[tokio::test]
    async fn every_retrieval_tool_marks_success_results_as_non_errors() {
        let state = successful_test_state();
        let open_id = contract::McpSessionId::from_raw_session_id("session-success")
            .expect("valid session id")
            .to_string();
        let cases = [
            (
                contract::SEARCH_SESSIONS_TOOL,
                json!({ "query": "nothing" }),
            ),
            (
                contract::LIST_SESSIONS_TOOL,
                json!({
                    "start_datetime": "2026-07-11T00:00:00Z",
                    "end_datetime": "2026-07-12T00:00:00Z"
                }),
            ),
            (contract::OPEN_TOOL, json!({ "id": open_id })),
            (
                contract::FILE_ATTENTION_TOOL,
                json!({ "path": "crates/moraine-mcp-core/src/lib.rs" }),
            ),
        ];

        for (index, (tool, arguments)) in cases.into_iter().enumerate() {
            let response = call_tool_rpc(&state, index as u64 + 1, tool, arguments).await;
            assert_successful_tool_exchange(&response, tool);
        }
    }

    #[tokio::test]
    async fn every_retrieval_tool_marks_invalid_requests_as_handled_errors() {
        let state = test_state();
        let cases = [
            (
                contract::SEARCH_SESSIONS_TOOL,
                json!({ "n_hits": 1 }),
                "invalid_request",
            ),
            (
                contract::LIST_SESSIONS_TOOL,
                json!({
                    "start_datetime": "2026-07-12T00:00:00Z",
                    "end_datetime": "2026-07-11T00:00:00Z"
                }),
                "invalid_request",
            ),
            (
                contract::OPEN_TOOL,
                json!({ "id": "session:not-valid-base64" }),
                "invalid_id",
            ),
            (contract::FILE_ATTENTION_TOOL, json!({}), "invalid_request"),
        ];

        for (index, (tool, arguments, code)) in cases.into_iter().enumerate() {
            let response = call_tool_rpc(&state, index as u64 + 1, tool, arguments).await;
            assert_handled_tool_error_exchange(&response, tool, code);
        }
    }

    #[tokio::test]
    async fn scoped_search_and_open_not_found_are_handled_tool_errors() {
        let state = test_state();
        let missing_id = contract::McpSessionId::from_raw_session_id("missing-session")
            .expect("valid session id")
            .to_string();

        let open = call_tool_rpc(&state, 1, contract::OPEN_TOOL, json!({ "id": missing_id })).await;
        assert_handled_tool_error_exchange(&open, contract::OPEN_TOOL, "not_found");

        let search = call_tool_rpc(
            &state,
            2,
            contract::SEARCH_SESSIONS_TOOL,
            json!({ "query": "nothing", "within_id": missing_id }),
        )
        .await;
        assert_handled_tool_error_exchange(&search, contract::SEARCH_SESSIONS_TOOL, "not_found");
    }

    #[tokio::test]
    async fn every_retrieval_tool_marks_repository_failures_as_handled_errors() {
        let state = repository_error_test_state();
        let open_id = contract::McpSessionId::from_raw_session_id("session-error")
            .expect("valid session id")
            .to_string();
        let cases = [
            (
                contract::SEARCH_SESSIONS_TOOL,
                json!({ "query": "nothing" }),
            ),
            (
                contract::LIST_SESSIONS_TOOL,
                json!({
                    "start_datetime": "2026-07-11T00:00:00Z",
                    "end_datetime": "2026-07-12T00:00:00Z"
                }),
            ),
            (contract::OPEN_TOOL, json!({ "id": open_id })),
            (
                contract::FILE_ATTENTION_TOOL,
                json!({ "path": "crates/moraine-mcp-core/src/lib.rs" }),
            ),
        ];

        for (index, (tool, arguments)) in cases.into_iter().enumerate() {
            let response = call_tool_rpc(&state, index as u64 + 1, tool, arguments).await;
            assert_handled_tool_error_exchange(&response, tool, "internal_error");
        }
    }

    #[tokio::test]
    async fn serve_connection_frames_one_response_per_request() {
        let state = test_state();

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
    fn default_test_router() -> (Arc<AppConfig>, Arc<BackendRepositoryRouter>) {
        let cfg = Arc::new(AppConfig::default());
        let repository: Arc<dyn ConversationRepository> =
            Arc::new(InMemoryConversationRepository::default());
        let router = BackendRepositoryRouter::from_preloaded_for_testing(
            cfg.clone(),
            [("default".to_string(), repository)],
        )
        .expect("preloaded default router");
        (cfg, Arc::new(router))
    }

    #[cfg(unix)]
    fn routed_test_router(
        prewarm_on_initialize: bool,
    ) -> (
        Arc<AppConfig>,
        Arc<BackendRepositoryRouter>,
        Arc<InMemoryConversationRepository>,
        Arc<InMemoryConversationRepository>,
    ) {
        let mut cfg = AppConfig::default();
        cfg.mcp.prewarm_on_initialize = prewarm_on_initialize;
        cfg.backends.insert(
            "team-ch".to_string(),
            moraine_config::ClickHouseConfig {
                url: "http://team.invalid".to_string(),
                database: "moraine_team".to_string(),
                ..Default::default()
            },
        );
        cfg.routes.push(moraine_config::RouteConfig {
            dir: "/work/team/**".to_string(),
            backend: "team-ch".to_string(),
            mode: moraine_config::ROUTE_MODE_MIRROR.to_string(),
        });
        let cfg = Arc::new(cfg);

        let default = Arc::new(InMemoryConversationRepository::default());
        let named_scope =
            SessionOriginScope::from_roots(["/named-repository"]).expect("named scope");
        let named = Arc::new(InMemoryConversationRepository::new(RepoConfig {
            session_scope: Some(named_scope),
            ..RepoConfig::default()
        }));
        let router = BackendRepositoryRouter::from_preloaded_for_testing(
            cfg.clone(),
            [
                (
                    "default".to_string(),
                    default.clone() as Arc<dyn ConversationRepository>,
                ),
                (
                    "team-ch".to_string(),
                    named.clone() as Arc<dyn ConversationRepository>,
                ),
            ],
        )
        .expect("preloaded routed router");
        (cfg, Arc::new(router), default, named)
    }

    #[cfg(unix)]
    async fn spawn_test_socket(
        tag: &str,
        cfg: Arc<AppConfig>,
        router: Arc<BackendRepositoryRouter>,
    ) -> (
        PathBuf,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let sock = unique_socket_path(tag);
        let _ = std::fs::remove_file(&sock);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_sock = sock.clone();
        let server = tokio::spawn(async move {
            run_socket_with_router(cfg, router, server_sock, async {
                let _ = shutdown_rx.await;
            })
            .await
        });
        (sock, shutdown_tx, server)
    }

    #[cfg(unix)]
    async fn connect_to_test_socket(sock: &std::path::Path) -> tokio::net::UnixStream {
        for _ in 0..50 {
            match tokio::net::UnixStream::connect(sock).await {
                Ok(stream) => return stream,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
        panic!("failed to connect to test socket {}", sock.display());
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn socket_publication_never_replaces_an_existing_path() {
        let from = unique_socket_path("publish-from");
        let to = unique_socket_path("publish-to");
        std::fs::write(&from, b"new").expect("write source");
        std::fs::write(&to, b"existing").expect("write destination");

        let error = rename_socket_noreplace(&from, &to)
            .expect_err("atomic publication must not replace a destination");
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(
            std::fs::read(&from).expect("source retained"),
            b"new".to_vec()
        );
        assert_eq!(
            std::fs::read(&to).expect("destination retained"),
            b"existing".to_vec()
        );

        let _ = std::fs::remove_file(from);
        let _ = std::fs::remove_file(to);
    }

    #[cfg(all(
        unix,
        not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "ios"
        ))
    ))]
    #[test]
    fn socket_publication_fails_when_atomic_noreplace_is_unsupported() {
        let from = unique_socket_path("unsupported-from");
        let to = unique_socket_path("unsupported-to");
        std::fs::write(&from, b"new").expect("write source");
        let _ = std::fs::remove_file(&to);

        let error = rename_socket_noreplace(&from, &to)
            .expect_err("publication must fail without an atomic no-replace primitive");
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "atomic no-replace Unix socket publication is unsupported on this platform"
        );
        assert_eq!(
            std::fs::read(&from).expect("source retained"),
            b"new".to_vec()
        );
        assert!(!to.exists(), "destination must not be published");

        let _ = std::fs::remove_file(from);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn live_socket_collision_is_an_error_and_preserves_owner() {
        use tokio::net::{UnixListener, UnixStream};

        let sock = unique_socket_path("live");
        let _ = std::fs::remove_file(&sock);
        let owner = UnixListener::bind(&sock).expect("bind existing owner");
        let (cfg, router) = default_test_router();
        let error = run_socket_with_router(cfg, router, sock.clone(), pending())
            .await
            .expect_err("second backend must fail");
        assert!(
            error
                .to_string()
                .contains("another backend is already listening"),
            "unexpected error: {error:#}"
        );
        assert!(sock.exists(), "losing backend must preserve owner's socket");
        UnixStream::connect(&sock)
            .await
            .expect("owner remains connectable");

        drop(owner);
        let _ = std::fs::remove_file(sock);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn router_socket_replays_raw_client_and_cleans_up() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (cfg, router) = default_test_router();
        let (sock, shutdown_tx, server) = spawn_test_socket("serve", cfg, router).await;
        let mut stream = connect_to_test_socket(&sock).await;

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
            .write_all(
                concat!(
                    "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"initialize\",\"params\":{}}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"ping\"}\n",
                )
                .as_bytes(),
            )
            .await
            .expect("write pipelined raw requests");
        stream.shutdown().await.expect("half-close raw client");

        let mut output = String::new();
        stream
            .read_to_string(&mut output)
            .await
            .expect("read raw responses");
        let responses = output.lines().collect::<Vec<_>>();
        assert_eq!(
            responses.len(),
            2,
            "first line must be replayed exactly once and buffered input retained: {output}"
        );
        let initialize: Value = serde_json::from_str(responses[0]).expect("initialize response");
        assert_eq!(initialize["id"], json!(7));
        assert_eq!(
            initialize["result"]["serverInfo"]["name"],
            json!("moraine-mcp")
        );
        let ping: Value = serde_json::from_str(responses[1]).expect("ping response");
        assert_eq!(ping["id"], json!(8));
        assert_eq!(ping["result"], json!({}));

        shutdown_tx.send(()).expect("request server shutdown");
        server
            .await
            .expect("server task")
            .expect("clean server shutdown");
        assert!(
            !sock.exists(),
            "socket path must be removed before shutdown completes"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn router_socket_preserves_oversized_legacy_first_request() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

        let (cfg, router) = default_test_router();
        let (sock, shutdown_tx, server) = spawn_test_socket("oversized-legacy", cfg, router).await;
        let mut stream = connect_to_test_socket(&sock).await;
        let mut request = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "initialize",
            "params": {
                "padding": "x".repeat(private_proxy::PRIVATE_ROUTE_MAX_LINE_BYTES + 1)
            },
        }))
        .expect("oversized raw request JSON");
        request.push(b'\n');
        assert!(request.len() > private_proxy::PRIVATE_ROUTE_MAX_LINE_BYTES);
        stream
            .write_all(&request)
            .await
            .expect("write oversized legacy request");

        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .expect("read oversized legacy response");
        let response: Value = serde_json::from_str(response.trim()).expect("response JSON");
        assert_eq!(response["id"], json!(9));
        assert_eq!(
            response["result"]["serverInfo"]["name"],
            json!("moraine-mcp")
        );

        shutdown_tx.send(()).expect("request server shutdown");
        server
            .await
            .expect("server task")
            .expect("clean server shutdown");
    }

    #[cfg(unix)]
    async fn accepted_test_connection(sock: &std::path::Path, cwd: &str) -> PrivateProxyConnection {
        let stream = connect_to_test_socket(sock).await;
        match negotiate_private_route(stream, cwd, std::time::Duration::from_secs(3)).await {
            PrivateRouteNegotiation::Accepted(connection) => connection,
            PrivateRouteNegotiation::Incompatible { reason } => {
                panic!("test daemon must be compatible: {reason}")
            }
            PrivateRouteNegotiation::Rejected { message } => {
                panic!("test route must be accepted: {message}")
            }
        }
    }

    #[cfg(unix)]
    async fn proxy_test_requests(connection: PrivateProxyConnection, requests: Vec<u8>) -> Vec<u8> {
        let (sock_read, sock_write) = connection.into_parts();
        let mut output = Vec::new();
        proxy_streams_with_halves(
            std::io::Cursor::new(requests),
            &mut output,
            sock_read,
            sock_write,
        )
        .await
        .expect("proxy test requests");
        output
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn negotiated_socket_selects_named_and_default_without_exposing_ack() {
        let (cfg, router, _default, _named) = routed_test_router(false);
        let (sock, shutdown_tx, server) = spawn_test_socket("route-select", cfg, router).await;

        let named = accepted_test_connection(&sock, "/work/team/project").await;
        let named_output = proxy_test_requests(
            named,
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n".to_vec(),
        )
        .await;
        let named_lines = String::from_utf8(named_output).expect("named output UTF-8");
        let named_responses = named_lines.lines().collect::<Vec<_>>();
        assert_eq!(
            named_responses.len(),
            1,
            "private ACK must not reach agent stdout: {named_lines}"
        );
        let named_response: Value =
            serde_json::from_str(named_responses[0]).expect("named initialize");
        assert_eq!(named_response["id"], json!(1));
        assert!(named_response["result"]["instructions"]
            .as_str()
            .expect("named repository marker")
            .contains("/named-repository"));

        let default = accepted_test_connection(&sock, "").await;
        let default_output = proxy_test_requests(
            default,
            b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"initialize\",\"params\":{}}\n".to_vec(),
        )
        .await;
        let default_response: Value =
            serde_json::from_slice(&default_output).expect("default initialize");
        assert_eq!(default_response["id"], json!(2));
        assert!(default_response["result"].get("instructions").is_none());

        shutdown_tx.send(()).expect("shutdown route server");
        server
            .await
            .expect("route server task")
            .expect("route server shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn negotiated_socket_keeps_client_launch_project_for_file_attention() {
        let root = std::env::temp_dir().join(format!(
            "moraine-mcp-route-project-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join(".git")).expect("create test git directory");
        std::fs::write(root.join(".moraine.toml"), "backend = \"default\"\n")
            .expect("write repository backend marker");
        let expected_project_id =
            moraine_config::project_id_for_repo_root(&root).expect("canonical project id");

        let cfg = Arc::new(AppConfig::default());
        let repository = Arc::new(InMemoryConversationRepository::default());
        let router = BackendRepositoryRouter::from_preloaded_for_testing(
            cfg.clone(),
            [(
                "default".to_string(),
                repository.clone() as Arc<dyn ConversationRepository>,
            )],
        )
        .expect("preloaded default router");
        let (sock, shutdown_tx, server) =
            spawn_test_socket("route-project", cfg, Arc::new(router)).await;

        let root_text = root.to_string_lossy().to_string();
        let connection = accepted_test_connection(&sock, &root_text).await;
        let output = proxy_test_requests(
            connection,
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"file_attention\",\"arguments\":{\"path\":\"src/lib.rs\",\"scope\":\"project\"}}}\n".to_vec(),
        )
        .await;
        let response: Value = serde_json::from_slice(&output).expect("file_attention response");
        assert_eq!(response["id"], json!(1));
        assert!(
            response["result"]["isError"] == json!(false),
            "file_attention should succeed: {response}"
        );
        assert!(
            response["result"]["structuredContent"]["warnings"]
                .as_array()
                .expect("file_attention warnings")
                .iter()
                .any(|warning| warning.as_str().is_some_and(
                    |warning| warning.contains("pruned before durable project mapping")
                )),
            "project scope must disclose the one-time unmappable-history exclusion: {response}"
        );

        let calls = repository.calls();
        assert_eq!(calls.file_attention.len(), 1);
        let query = &calls.file_attention[0];
        assert_eq!(query.rel, "src/lib.rs");
        assert_eq!(
            query.normalized_project_id.as_deref(),
            Some(expected_project_id.as_str())
        );
        assert!(
            query
                .normalized_project_roots
                .iter()
                .any(|root| root == &root_text),
            "launch root must be present in registered project roots"
        );
        assert!(query.apply_project_scope);

        shutdown_tx.send(()).expect("shutdown route server");
        server
            .await
            .expect("route server task")
            .expect("route server shutdown");
        std::fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connection_stays_pinned_after_private_route_ack() {
        let (cfg, router, default, named) = routed_test_router(false);
        let (sock, shutdown_tx, server) = spawn_test_socket("route-pin", cfg, router).await;

        let connection = accepted_test_connection(&sock, "/work/team/project").await;
        let requests = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":\"moraine-route-v1\",\"method\":\"moraine/private/route\",\"params\":{\"version\":1,\"cwd\":\"\"}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"initialize\",\"params\":{}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"search_sessions\",\"arguments\":{\"query\":\"pin-check\"}}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"list_sessions\",\"arguments\":{\"start_datetime\":\"2026-01-01T00:00:00Z\",\"end_datetime\":\"2026-01-02T00:00:00Z\"}}}\n",
        )
        .as_bytes()
        .to_vec();
        let output = proxy_test_requests(connection, requests).await;
        let text = String::from_utf8(output).expect("pinned output UTF-8");
        let responses = text.lines().collect::<Vec<_>>();
        assert_eq!(
            responses.len(),
            4,
            "one response per public request: {text}"
        );
        let reselection: Value =
            serde_json::from_str(responses[0]).expect("midstream route response");
        assert_eq!(reselection["error"]["code"], json!(-32601));
        let initialize: Value =
            serde_json::from_str(responses[1]).expect("pinned initialize response");
        assert!(initialize["result"]["instructions"]
            .as_str()
            .expect("named repository remains selected")
            .contains("/named-repository"));

        let named_calls = named.calls();
        assert_eq!(named_calls.search_mcp_events.len(), 1);
        assert_eq!(named_calls.list_mcp_sessions.len(), 1);
        let default_calls = default.calls();
        assert!(default_calls.search_mcp_events.is_empty());
        assert!(default_calls.list_mcp_sessions.is_empty());

        shutdown_tx.send(()).expect("shutdown pin server");
        server
            .await
            .expect("pin server task")
            .expect("pin server shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prewarm_gate_is_shared_per_backend_and_isolated_between_backends() {
        let (cfg, router, default, named) = routed_test_router(true);
        let (sock, shutdown_tx, server) = spawn_test_socket("prewarm", cfg, router).await;
        let initialize =
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n";

        for _ in 0..2 {
            let connection = accepted_test_connection(&sock, "/work/team/project").await;
            let _ = proxy_test_requests(connection, initialize.to_vec()).await;
        }
        let default_connection = accepted_test_connection(&sock, "").await;
        let _ = proxy_test_requests(default_connection, initialize.to_vec()).await;

        for _ in 0..50 {
            if named.calls().prewarm_mcp_search_state == 1
                && default.calls().prewarm_mcp_search_state == 1
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(named.calls().prewarm_mcp_search_state, 1);
        assert_eq!(default.calls().prewarm_mcp_search_state, 1);

        shutdown_tx.send(()).expect("shutdown prewarm server");
        server
            .await
            .expect("prewarm server task")
            .expect("prewarm server shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unsupported_private_version_gets_structured_incompatible_error() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

        let (cfg, router) = default_test_router();
        let (sock, shutdown_tx, server) = spawn_test_socket("incompatible", cfg, router).await;
        let mut stream = connect_to_test_socket(&sock).await;
        stream
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":\"moraine-route-v1\",\"method\":\"moraine/private/route\",\"params\":{\"version\":2,\"cwd\":\"/work\"}}\n",
            )
            .await
            .expect("write unsupported hello");
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .expect("read incompatible response");
        let response: Value = serde_json::from_str(line.trim()).expect("incompatible JSON");
        assert_eq!(response["id"], json!("moraine-route-v1"));
        assert_eq!(response["error"]["code"], json!(-32001));
        assert_eq!(response["error"]["data"]["kind"], json!("incompatible"));
        assert_eq!(response["error"]["data"]["version"], json!(1));

        shutdown_tx.send(()).expect("shutdown incompatible server");
        server
            .await
            .expect("incompatible server task")
            .expect("incompatible server shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn named_repository_build_failure_is_typed_route_rejection() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.timeout_seconds = 0.1;
        cfg.backends
            .get_mut("default")
            .expect("default backend")
            .timeout_seconds = 0.1;
        cfg.backends.insert(
            "team-ch".to_string(),
            moraine_config::ClickHouseConfig {
                url: "http://127.0.0.1:1".to_string(),
                database: "moraine_team".to_string(),
                timeout_seconds: 0.1,
                ..Default::default()
            },
        );
        cfg.routes.push(moraine_config::RouteConfig {
            dir: "/work/team/**".to_string(),
            backend: "team-ch".to_string(),
            mode: moraine_config::ROUTE_MODE_MIRROR.to_string(),
        });
        let cfg = Arc::new(cfg);
        let default: Arc<dyn ConversationRepository> =
            Arc::new(InMemoryConversationRepository::default());
        let router = BackendRepositoryRouter::from_preloaded_for_testing(
            cfg.clone(),
            [("default".to_string(), default)],
        )
        .expect("router with lazy failing named backend");
        let (sock, shutdown_tx, server) =
            spawn_test_socket("route-reject", cfg, Arc::new(router)).await;

        let stream = connect_to_test_socket(&sock).await;
        let outcome = negotiate_private_route(
            stream,
            "/work/team/project",
            std::time::Duration::from_secs(3),
        )
        .await;
        match outcome {
            PrivateRouteNegotiation::Rejected { message } => {
                assert!(
                    message.contains("team-ch"),
                    "backend named in error: {message}"
                );
                assert!(
                    message.contains("schema handshake failed"),
                    "handshake named in error: {message}"
                );
            }
            PrivateRouteNegotiation::Incompatible { reason } => {
                panic!("route failures must not become fallback-compatible: {reason}")
            }
            PrivateRouteNegotiation::Accepted(_) => {
                panic!("unreachable named backend must not be accepted")
            }
        }

        shutdown_tx.send(()).expect("shutdown rejection server");
        server
            .await
            .expect("rejection server task")
            .expect("rejection server shutdown");
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
