use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Extension, Path, Query, State},
    http::{header, HeaderValue, Request, StatusCode, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
#[cfg(test)]
use moraine_config::AppConfig;
use moraine_conversations::{
    AnalyticsRange, BackendRepository, BackendRepositoryRouter, IngestHeartbeat,
    IngestHeartbeatRead, RepoError, SessionAnalytics, SessionAnalyticsQuery, SessionLookback,
    SessionStep, SessionTurn, StoreConnectionMetrics, StoreHealth, StoreProbe, TablePreviewQuery,
    TableSummaries,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;

struct AppState {
    backend_router: Arc<BackendRepositoryRouter>,
    static_dir: PathBuf,
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

#[derive(Deserialize)]
struct AnalyticsQuery {
    range: Option<String>,
}

#[derive(Deserialize)]
struct SessionsQuery {
    limit: Option<u32>,
    since: Option<String>,
}

#[derive(Serialize)]
struct MonitorTableSummary {
    name: String,
    engine: String,
    is_temporary: u8,
    rows: u64,
}

/// Run the monitor HTTP server using the daemon-owned backend router.
///
/// The supplied shutdown future stops the listener gracefully.
pub async fn run_server_with_router<S>(
    backend_router: Arc<BackendRepositoryRouter>,
    host: String,
    port: u16,
    static_dir: PathBuf,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    let static_dir_display = static_dir.display().to_string();
    let app = router_with_backend_router(backend_router, static_dir)?;
    let bind = format!("{host}:{port}")
        .parse::<SocketAddr>()
        .map_err(|err| anyhow!("invalid bind address: {err}"))?;

    let listener = tokio::net::TcpListener::bind(bind).await.map_err(|error| {
        if error.kind() == ErrorKind::AddrInUse {
            anyhow!(
                "failed to bind {bind}: address already in use. another backend or legacy monitor may already be running; stop it or choose a free --port"
            )
        } else {
            anyhow!("failed to bind {bind}: {error}")
        }
    })?;

    println!("moraine-monitor running at http://{bind}");
    println!("serving UI from {static_dir_display}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// Build the complete monitor router around the daemon-owned backend router.
pub fn router_with_backend_router(
    backend_router: Arc<BackendRepositoryRouter>,
    static_dir: PathBuf,
) -> Result<Router> {
    validate_static_dir(&static_dir)?;
    let state = Arc::new(AppState {
        backend_router,
        static_dir,
    });
    Ok(monitor_router(state))
}

fn monitor_router(state: Arc<AppState>) -> Router {
    let data_routes = dashboard_routes().route_layer(middleware::from_fn_with_state(
        state.backend_router.clone(),
        select_backend_repository,
    ));
    // Capabilities is daemon/default-global metadata, not a project-routed data
    // endpoint. It intentionally remains outside project selection middleware.
    let versioned_routes = data_routes
        .clone()
        .route("/capabilities", get(api_capabilities));

    Router::new()
        .nest("/api/v1", versioned_routes)
        // One-release compatibility surface. These are direct aliases so
        // status codes, query handling, response payloads, and backend
        // selection remain identical.
        .nest("/api", data_routes)
        .fallback(get(static_fallback))
        .with_state(state)
}

fn dashboard_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(api_health))
        .route("/status", get(api_status))
        .route("/analytics", get(api_analytics))
        .route("/tables", get(api_tables))
        .route("/web-searches", get(api_web_searches))
        .route("/tables/:table", get(api_table_rows))
        .route("/sessions", get(api_sessions))
}

/// Optional project context for repository-backed data endpoints. The value is
/// resolved only through configured cwd routes/repo references; it never names
/// a backend endpoint or credentials. Capabilities and static routes ignore it.
const PROJECT_DIR_HEADER: &str = "x-moraine-project-dir";

fn project_dir_header(
    headers: &axum::http::HeaderMap,
) -> std::result::Result<Option<&str>, &'static str> {
    let mut values = headers.get_all(PROJECT_DIR_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err("X-Moraine-Project-Dir must be provided exactly once");
    }
    let value = value
        .to_str()
        .map_err(|_| "X-Moraine-Project-Dir must be valid UTF-8")?
        .trim();
    if value.is_empty() {
        return Err("X-Moraine-Project-Dir must not be empty");
    }
    if !FsPath::new(value).is_absolute() {
        return Err("X-Moraine-Project-Dir must be an absolute path");
    }
    Ok(Some(value))
}

async fn select_backend_repository(
    State(backend_router): State<Arc<BackendRepositoryRouter>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let selected = match project_dir_header(request.headers()) {
        Ok(None) => backend_router.default_repository().await,
        Ok(Some(project_dir)) => {
            backend_router
                .repository_for_project_dir(Some(project_dir))
                .await
        }
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": error}),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let backend = match selected {
        Ok(backend) => backend,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("backend selection failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };
    request.extensions_mut().insert(backend);
    next.run(request).await
}

const MONITOR_DIST_ENV_KEYS: &[&str] = &["MORAINE_MONITOR_DIST", "MORAINE_MONITOR_STATIC_DIR"];

fn monitor_dist_candidate(root: &FsPath) -> PathBuf {
    root.join("web").join("monitor").join("dist")
}

fn find_monitor_dir(root: &FsPath) -> Option<PathBuf> {
    let candidate = monitor_dist_candidate(root);
    candidate.exists().then_some(candidate)
}

fn source_tree_static_dir() -> PathBuf {
    let manifest_dir = FsPath::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(FsPath::parent)
        .expect("workspace root")
        .join("web")
        .join("monitor")
        .join("dist")
}

fn env_override_static_dir_with_keys(keys: &[&str]) -> Option<PathBuf> {
    keys.iter().find_map(|key| {
        let value = std::env::var(key).ok()?;
        let value = value.trim();
        if value.is_empty() {
            return None;
        }
        let configured = PathBuf::from(value);
        configured.exists().then_some(configured)
    })
}

/// Resolve the monitor distribution directory.
///
/// An explicit CLI override wins, followed by the established environment
/// variables, an installed bundle beside the current executable, and finally
/// the source-tree `web/monitor/dist` path. Availability and `index.html` are
/// validated when the router is built.
pub fn resolve_static_dir(override_path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = override_path {
        return path;
    }
    if let Some(configured) = env_override_static_dir_with_keys(MONITOR_DIST_ENV_KEYS) {
        return configured;
    }
    if let Ok(exe) = std::env::current_exe() {
        let exe = exe.canonicalize().unwrap_or(exe);
        if let Some(bundle_root) = exe.parent().and_then(FsPath::parent) {
            if let Some(found) = find_monitor_dir(bundle_root) {
                return found;
            }
        }
    }
    source_tree_static_dir()
}

fn validate_static_dir(static_dir: &FsPath) -> Result<()> {
    let metadata = std::fs::metadata(static_dir).map_err(|error| {
        anyhow!(
            "monitor static directory `{}` is unavailable: {error}. if running from source, build UI assets with `(cd web/monitor && bun install --frozen-lockfile && bun run build)`; otherwise ensure packaged `web/monitor/dist` assets are installed or pass `--static-dir <path>`",
            static_dir.display()
        )
    })?;

    if !metadata.is_dir() {
        return Err(anyhow!(
            "monitor static directory `{}` is not a directory; pass `--static-dir <path>` pointing to a built monitor dist directory",
            static_dir.display()
        ));
    }

    let index_path = static_dir.join("index.html");
    if !index_path.is_file() {
        return Err(anyhow!(
            "monitor static directory `{}` does not contain `index.html`; build monitor assets or pass `--static-dir <path>`",
            static_dir.display()
        ));
    }

    Ok(())
}

fn json_response<T: Serialize>(payload: T, status: StatusCode) -> Response {
    let mut response = Json(payload).into_response();
    *response.status_mut() = status;
    response
}
/// Daemon-wide capabilities intentionally report the owned default backend's
/// schema level. Project routing selects externally managed data stores, not a
/// different daemon protocol or feature set, so this endpoint ignores
/// `X-Moraine-Project-Dir` and remains outside routing middleware.
async fn api_capabilities(State(state): State<Arc<AppState>>) -> Response {
    let schema_migration_level = match state.backend_router.default_repository().await {
        Ok(default_backend) => default_backend
            .repository()
            .read_store_diagnostics()
            .await
            .ok()
            .and_then(|diagnostics| diagnostics.applied_schema_versions.into_iter().max()),
        Err(_) => None,
    };

    json_response(
        json!({
            "ok": true,
            "server_version": env!("CARGO_PKG_VERSION"),
            "schema_migration_level": schema_migration_level,
            "features": {
                "analytics": true,
                "sessions": true,
                "table_inspection": true,
                "web_searches": true,
            },
        }),
        StatusCode::OK,
    )
}

async fn api_health(Extension(backend): Extension<Arc<BackendRepository>>) -> Response {
    let (health, heartbeat) = tokio::join!(
        backend.repository().read_store_health(),
        backend.repository().latest_ingest_heartbeat()
    );
    let health = match health {
        Ok(health) => health,
        Err(error) => {
            let message = error.to_string();
            return json_response(
                json!({
                    "ok": false,
                    "url": backend.clickhouse_url(),
                    "database": backend.clickhouse_database(),
                    "error": message,
                    "connections": {"total": Value::Null, "error": message},
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };
    let connections = connection_payload(&health.connections);

    let ping_ms = match &health.ping {
        StoreProbe::Available(value) => *value,
        StoreProbe::Failed { message } => {
            return health_failure_response(&backend, message, connections);
        }
    };
    let version = match &health.version {
        StoreProbe::Available(value) => value,
        StoreProbe::Failed { message } => {
            return health_failure_response(&backend, message, connections);
        }
    };
    let heartbeat = heartbeat.map(monitor_heartbeat_status).unwrap_or_default();

    json_response(
        json!({
            "ok": true,
            "url": backend.clickhouse_url(),
            "database": backend.clickhouse_database(),
            "version": version,
            "ping_ms": ping_ms,
            "connections": connections,
            "ingestor": health_heartbeat_payload(&heartbeat),
        }),
        StatusCode::OK,
    )
}

fn health_failure_response(
    backend: &BackendRepository,
    message: &str,
    connections: Value,
) -> Response {
    json_response(
        json!({
            "ok": false,
            "url": backend.clickhouse_url(),
            "database": backend.clickhouse_database(),
            "error": message,
            "connections": connections,
        }),
        StatusCode::SERVICE_UNAVAILABLE,
    )
}

async fn api_status(Extension(backend): Extension<Arc<BackendRepository>>) -> Response {
    let health = backend
        .repository()
        .read_store_health()
        .await
        .unwrap_or_else(|error| unavailable_store_health(error.to_string()));
    let database_exists = probe_bool(&health.database_exists).unwrap_or(false);

    let (tables, heartbeat) = if database_exists {
        let (tables, heartbeat) = tokio::join!(
            backend.repository().list_table_summaries(),
            backend.repository().latest_ingest_heartbeat()
        );
        let tables = match tables {
            Ok(tables) => monitor_table_summaries(tables),
            Err(error) => {
                return json_response(
                    json!({"ok": false, "error": error.to_string()}),
                    StatusCode::SERVICE_UNAVAILABLE,
                );
            }
        };
        let heartbeat = heartbeat.map(monitor_heartbeat_status).unwrap_or_default();
        (tables, heartbeat)
    } else {
        (Vec::new(), MonitorHeartbeatStatus::default())
    };

    let estimated_total_rows = tables.iter().map(|table| table.rows).sum::<u64>();
    let clickhouse = status_clickhouse_payload(&backend, &health, database_exists);

    json_response(
        json!({
            "ok": true,
            "clickhouse": clickhouse,
            "database": {
                "exists": database_exists,
                "table_count": tables.len(),
                "estimated_total_rows": estimated_total_rows,
                "tables": tables,
            },
            "ingestor": heartbeat_payload(&heartbeat),
        }),
        StatusCode::OK,
    )
}

fn unavailable_store_health(message: String) -> StoreHealth {
    StoreHealth {
        ping: StoreProbe::Failed {
            message: message.clone(),
        },
        version: StoreProbe::Failed {
            message: message.clone(),
        },
        database_exists: StoreProbe::Failed {
            message: message.clone(),
        },
        connections: StoreProbe::Failed { message },
    }
}

fn probe_bool(probe: &StoreProbe<bool>) -> Option<bool> {
    match probe {
        StoreProbe::Available(value) => Some(*value),
        StoreProbe::Failed { .. } => None,
    }
}

fn status_clickhouse_payload(
    backend: &BackendRepository,
    health: &StoreHealth,
    database_exists: bool,
) -> Value {
    if !database_exists {
        return json!({
            "url": backend.clickhouse_url(),
            "database": backend.clickhouse_database(),
            "healthy": false,
            "version": Value::Null,
            "ping_ms": Value::Null,
            "error": "database not found",
            "connections": {"total": Value::Null, "error": "database not found"},
        });
    }

    let (version, ping_ms, healthy, error) = match &health.version {
        StoreProbe::Failed { message } => (Value::Null, Value::Null, false, json!(message)),
        StoreProbe::Available(version) => match &health.ping {
            StoreProbe::Available(ping_ms) => (json!(version), json!(ping_ms), true, Value::Null),
            StoreProbe::Failed { message } => (json!(version), Value::Null, false, json!(message)),
        },
    };

    json!({
        "url": backend.clickhouse_url(),
        "database": backend.clickhouse_database(),
        "healthy": healthy,
        "version": version,
        "ping_ms": ping_ms,
        "error": error,
        "connections": connection_payload(&health.connections),
    })
}

fn connection_payload(probe: &StoreProbe<StoreConnectionMetrics>) -> Value {
    match probe {
        StoreProbe::Available(metrics) => json!(metrics),
        StoreProbe::Failed { message } => {
            json!({"total": Value::Null, "error": message})
        }
    }
}

async fn api_tables(Extension(backend): Extension<Arc<BackendRepository>>) -> Response {
    match backend.repository().list_table_summaries().await {
        Ok(tables) => json_response(
            json!({"ok": true, "tables": monitor_table_summaries(tables)}),
            StatusCode::OK,
        ),
        Err(error) => json_response(
            json!({"ok": false, "error": error.to_string()}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

fn monitor_table_summaries(summaries: TableSummaries) -> Vec<MonitorTableSummary> {
    summaries
        .tables
        .into_iter()
        .map(|table| MonitorTableSummary {
            name: table.name,
            engine: table.engine,
            is_temporary: u8::from(table.is_temporary),
            rows: table.rows,
        })
        .collect()
}

async fn api_web_searches(
    Query(params): Query<LimitQuery>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    let limit = params.limit.unwrap_or(100).clamp(1, 1000) as u16;
    let rows = match backend.repository().list_web_searches(limit).await {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("web search query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    json_response(
        json!({
            "ok": true,
            "table": "web_searches",
            "limit": limit,
            "schema": [
                {"name": "event_time", "type": "String", "default_expression": ""},
                {"name": "harness", "type": "String", "default_expression": ""},
                {"name": "source_name", "type": "String", "default_expression": ""},
                {"name": "session_id", "type": "String", "default_expression": ""},
                {"name": "model", "type": "String", "default_expression": ""},
                {"name": "action", "type": "String", "default_expression": ""},
                {"name": "search_query", "type": "String", "default_expression": ""},
                {"name": "result_url", "type": "String", "default_expression": ""},
                {"name": "source_ref", "type": "String", "default_expression": ""}
            ],
            "rows": rows,
        }),
        StatusCode::OK,
    )
}

async fn api_analytics(
    Query(params): Query<AnalyticsQuery>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    let range = resolve_analytics_range(params.range.as_deref());
    let snapshot = match backend.repository().analytics_series(range).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    json_response(
        json!({
            "ok": true,
            "range": {
                "key": snapshot.window.range.as_str(),
                "label": format!("Last {}", snapshot.window.range.as_str()),
                "window_seconds": snapshot.window.window_seconds,
                "bucket_seconds": snapshot.window.bucket_seconds,
                "from_unix": snapshot.window.from_unix,
                "to_unix": snapshot.window.to_unix,
            },
            "series": {
                "tokens": snapshot.tokens,
                "turns": snapshot.turns,
                "concurrent_sessions": snapshot.concurrent_sessions,
            }
        }),
        StatusCode::OK,
    )
}

fn resolve_analytics_range(value: Option<&str>) -> AnalyticsRange {
    match value.unwrap_or("24h") {
        "15m" => AnalyticsRange::FifteenMinutes,
        "1h" => AnalyticsRange::OneHour,
        "6h" => AnalyticsRange::SixHours,
        "24h" => AnalyticsRange::TwentyFourHours,
        "7d" => AnalyticsRange::SevenDays,
        "30d" => AnalyticsRange::ThirtyDays,
        _ => AnalyticsRange::TwentyFourHours,
    }
}

async fn api_sessions(
    Query(params): Query<SessionsQuery>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    let query = SessionAnalyticsQuery {
        lookback: resolve_session_lookback(params.since.as_deref()),
        limit: params.limit.unwrap_or(50).clamp(1, 200) as u16,
    };
    let sessions = match backend.repository().list_session_analytics(query).await {
        Ok(sessions) => sessions,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("sessions query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };
    let now_ms = unix_now_ms();
    let sessions = sessions
        .into_iter()
        .map(|session| monitor_session_json(session, now_ms))
        .collect::<Vec<_>>();

    json_response(json!({"ok": true, "sessions": sessions}), StatusCode::OK)
}

fn resolve_session_lookback(value: Option<&str>) -> SessionLookback {
    match value.unwrap_or("30d") {
        "1h" => SessionLookback::OneHour,
        "6h" => SessionLookback::SixHours,
        "24h" => SessionLookback::TwentyFourHours,
        "7d" => SessionLookback::SevenDays,
        "30d" => SessionLookback::ThirtyDays,
        "90d" => SessionLookback::NinetyDays,
        "all" => SessionLookback::All,
        _ => SessionLookback::ThirtyDays,
    }
}

fn monitor_session_json(session: SessionAnalytics, now_ms: i64) -> Value {
    let mut total_tokens = 0_u64;
    let mut total_tool_calls = 0_u64;
    let turns = session
        .turns
        .into_iter()
        .enumerate()
        .map(|(idx, turn)| {
            let (value, tokens, tool_calls) = monitor_turn_json(idx, turn);
            total_tokens = total_tokens.saturating_add(tokens);
            total_tool_calls = total_tool_calls.saturating_add(tool_calls);
            value
        })
        .collect::<Vec<_>>();
    let duration_ms = session
        .summary
        .last_event_unix_ms
        .saturating_sub(session.summary.first_event_unix_ms)
        .max(0);
    let status = if now_ms.saturating_sub(session.summary.last_event_unix_ms) < 60_000 {
        "active"
    } else {
        "completed"
    };

    json!({
        "id": session.summary.session_id,
        "title": derive_title(&session.first_user_text),
        "harness": harness_descriptor(&session.harness, &session.source_name),
        "startedAt": session.summary.first_event_unix_ms,
        "endedAt": session.summary.last_event_unix_ms,
        "durationMs": duration_ms,
        "status": status,
        "models": session.models,
        "turns": turns,
        "totalTokens": total_tokens,
        "totalToolCalls": total_tool_calls,
        "tags": Vec::<String>::new(),
        "traceId": session.trace_id,
    })
}

fn monitor_turn_json(idx: usize, turn: SessionTurn) -> (Value, u64, u64) {
    let prompt_tokens = sum_buckets(
        &turn.token_usage_buckets,
        &[
            "input_text",
            "input_cache_read",
            "input_cache_write",
            "input_image",
            "input_audio",
            "embedding_input_text",
            "embedding_input_image",
        ],
    );
    let total_tokens = turn
        .token_usage_buckets
        .values()
        .copied()
        .fold(0_u64, u64::saturating_add);
    let completion_tokens = total_tokens.saturating_sub(prompt_tokens);
    let tool_calls = turn.summary.tool_calls;
    let steps = turn
        .steps
        .into_iter()
        .map(monitor_step_json)
        .collect::<Vec<_>>();
    let duration_ms = turn
        .summary
        .ended_at_unix_ms
        .saturating_sub(turn.summary.started_at_unix_ms)
        .max(0);

    (
        json!({
            "idx": idx,
            "model": turn.model,
            "startedAt": turn.summary.started_at_unix_ms,
            "endedAt": turn.summary.ended_at_unix_ms,
            "durationMs": duration_ms,
            "promptTokens": prompt_tokens,
            "completionTokens": completion_tokens,
            "totalTokens": total_tokens,
            "toolCalls": tool_calls,
            "steps": steps,
        }),
        total_tokens,
        tool_calls,
    )
}

fn monitor_step_json(step: SessionStep) -> Value {
    match step {
        SessionStep::User {
            event_unix_ms,
            text,
        } => json!({"kind": "user", "at": event_unix_ms, "text": text}),
        SessionStep::Assistant {
            event_unix_ms,
            text,
            endpoint_kind,
            latency_ms,
            token_usage_buckets,
            token_usage_native_units,
        } => {
            let tokens = sum_buckets(
                &token_usage_buckets,
                &[
                    "output_text",
                    "output_image",
                    "output_audio",
                    "reasoning",
                    "server_tool_use",
                ],
            );
            let mut value = json!({
                "kind": "assistant",
                "at": event_unix_ms,
                "text": text,
                "tokens": tokens,
                "endpointKind": endpoint_kind,
                "nativeTokenUnits": token_usage_native_units,
            });
            if let Some(latency_ms) = latency_ms {
                value["durationMs"] = json!(latency_ms);
            }
            value
        }
        SessionStep::Thinking {
            event_unix_ms,
            text,
        } => json!({"kind": "thinking", "at": event_unix_ms, "text": text}),
        SessionStep::ToolCall {
            event_unix_ms,
            tool_name,
            call_id,
            arguments,
            latency_ms: call_latency_ms,
            is_error: call_is_error,
            result,
        } => {
            let (latency_ms, result_text, result_at, status) = match result {
                Some(result) => (
                    result.latency_ms,
                    result.text,
                    result.event_unix_ms,
                    if call_is_error || result.is_error {
                        "error"
                    } else {
                        "ok"
                    },
                ),
                None => (
                    call_latency_ms.unwrap_or_default(),
                    String::new(),
                    event_unix_ms,
                    if call_is_error { "error" } else { "ok" },
                ),
            };
            json!({
                "kind": "tool_call",
                "at": event_unix_ms,
                "tool": tool_name,
                "args": arguments,
                "latencyMs": latency_ms,
                "result": result_text,
                "resultAt": result_at,
                "status": status,
                "callId": call_id,
            })
        }
    }
}

fn sum_buckets(buckets: &std::collections::BTreeMap<String, u64>, names: &[&str]) -> u64 {
    names
        .iter()
        .filter_map(|name| buckets.get(*name))
        .copied()
        .fold(0_u64, u64::saturating_add)
}

fn derive_title(first_user_text: &str) -> String {
    let trimmed = first_user_text.trim();
    if trimmed.is_empty() {
        return "(untitled session)".to_string();
    }

    let first_line = trimmed.lines().next().unwrap_or(trimmed).trim();
    if first_line.chars().count() <= 120 {
        first_line.to_string()
    } else {
        format!(
            "{}\u{2026}",
            first_line.chars().take(120).collect::<String>()
        )
    }
}

fn harness_descriptor(harness_id: &str, source_name: &str) -> Value {
    let id = if harness_id.trim().is_empty() {
        source_name.trim()
    } else {
        harness_id.trim()
    };
    let id = if id.is_empty() { "unknown" } else { id };
    let short = id
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .take(2)
        .filter_map(|part| part.chars().next())
        .collect::<String>()
        .to_uppercase();
    let short = if short.is_empty() {
        id.chars().take(2).collect::<String>().to_uppercase()
    } else {
        short
    };

    json!({
        "id": id,
        "label": id,
        "short": short,
        "hue": hue_for_label(id),
    })
}

fn hue_for_label(label: &str) -> u32 {
    match label {
        "claude-code" => 25,
        "codex" => 150,
        "hermes" => 265,
        "cursor" => 200,
        "aider" => 340,
        "continue" => 100,
        "cli" => 60,
        _ => {
            label.bytes().fold(0_u32, |hash, byte| {
                hash.wrapping_mul(31).wrapping_add(u32::from(byte))
            }) % 360
        }
    }
}

#[derive(Default)]
struct MonitorHeartbeatStatus {
    latest: Option<IngestHeartbeat>,
    age_seconds: Option<u64>,
}

fn monitor_heartbeat_status(read: IngestHeartbeatRead) -> MonitorHeartbeatStatus {
    let age_seconds = read.latest.as_ref().and_then(|latest| {
        (latest.ts_unix_ms >= 0)
            .then(|| unix_now_ms().saturating_sub(latest.ts_unix_ms).max(0) as u64 / 1_000)
    });
    MonitorHeartbeatStatus {
        latest: read.latest,
        age_seconds,
    }
}

fn heartbeat_payload(status: &MonitorHeartbeatStatus) -> Value {
    let latest = status
        .latest
        .as_ref()
        .map(|latest| {
            let mut payload = json!({
                "ts": latest.ts,
                "ts_unix_ms": latest.ts_unix_ms,
                "host": latest.host,
                "service_version": latest.service_version,
                "queue_depth": latest.queue_depth,
                "files_active": latest.files_active,
                "files_watched": latest.files_watched,
                "rows_raw_written": latest.rows_raw_written,
                "rows_events_written": latest.rows_events_written,
                "rows_errors_written": latest.rows_errors_written,
                "flush_latency_ms": latest.flush_latency_ms,
                "append_to_visible_p50_ms": latest.append_to_visible_p50_ms,
                "append_to_visible_p95_ms": latest.append_to_visible_p95_ms,
                "last_error": latest.last_error,
            });
            if let Some(backend_sinks) = &latest.backend_sinks {
                payload["backend_sinks"] = backend_sinks.clone();
            }
            payload
        })
        .unwrap_or(Value::Null);

    json!({
        "present": status.latest.is_some(),
        "alive": status.age_seconds.map(|age| age <= 30).unwrap_or(false),
        "latest": latest,
        "age_seconds": status.age_seconds,
    })
}

fn health_heartbeat_payload(status: &MonitorHeartbeatStatus) -> Value {
    let latest = status.latest.as_ref().map_or(Value::Null, |latest| {
        json!({
            "backend_sinks": latest.backend_sinks.clone().unwrap_or_else(|| json!({})),
        })
    });

    json!({
        "present": status.latest.is_some(),
        "alive": status.age_seconds.map(|age| age <= 30).unwrap_or(false),
        "latest": latest,
        "age_seconds": status.age_seconds,
    })
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

async fn api_table_rows(
    Path(table): Path<String>,
    Query(params): Query<LimitQuery>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    let limit = params.limit.unwrap_or(25).clamp(1, 500) as u16;
    match backend
        .repository()
        .preview_table(TablePreviewQuery {
            table: table.clone(),
            limit,
        })
        .await
    {
        Ok(preview) => {
            let schema = preview
                .schema
                .into_iter()
                .map(|column| {
                    json!({
                        "name": column.name,
                        "type": column.type_name,
                        "default_expression": column.default_expression,
                    })
                })
                .collect::<Vec<_>>();
            json_response(
                json!({
                    "ok": true,
                    "table": preview.table,
                    "limit": preview.limit,
                    "schema": schema,
                    "rows": preview.rows,
                }),
                StatusCode::OK,
            )
        }
        Err(RepoError::InvalidArgument(_)) => json_response(
            json!({"ok": false, "error": "invalid table name"}),
            StatusCode::BAD_REQUEST,
        ),
        Err(error) => json_response(
            json!({
                "ok": false,
                "error": format!("unable to read table {table}: {error}"),
            }),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

async fn static_fallback(State(state): State<Arc<AppState>>, uri: Uri) -> Response {
    let requested = uri.path();
    if requested.contains("..") {
        return json_response(
            json!({"ok": false, "error": "forbidden"}),
            StatusCode::FORBIDDEN,
        );
    }

    let file_path = if requested == "/" || requested.is_empty() {
        state.static_dir.join("index.html")
    } else {
        let mut target = state.static_dir.join(requested.trim_start_matches('/'));
        if target.is_dir() {
            target.push("index.html");
        }
        target
    };

    let canonical_root = match fs::canonicalize(&state.static_dir).await {
        Ok(path) => path,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("static directory unavailable: {error}")}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    let canonical_file = match fs::canonicalize(&file_path).await {
        Ok(path) => path,
        Err(_) => {
            return json_response(
                json!({"ok": false, "error": "not found"}),
                StatusCode::NOT_FOUND,
            );
        }
    };
    if !canonical_file.starts_with(&canonical_root) {
        return json_response(
            json!({"ok": false, "error": "forbidden"}),
            StatusCode::FORBIDDEN,
        );
    }

    let bytes = match fs::read(&canonical_file).await {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("failed to read file: {error}")}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    let content_type = mime_guess::from_path(&canonical_file)
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use moraine_config::{ClickHouseConfig, RouteConfig, DEFAULT_BACKEND_NAME, ROUTE_MODE_MIRROR};
    use moraine_conversations::{
        AnalyticsConcurrencyPoint, AnalyticsSnapshot, AnalyticsTokenPoint, AnalyticsTurnPoint,
        AnalyticsWindow, ConversationMode, ConversationRepository, ConversationSummary,
        InMemoryConversationRepository, InMemoryConversationResponses, IngestHeartbeat, RepoConfig,
        SessionStep, StoreDiagnostics, TableColumn, TablePreview, TableSummary, ToolResult,
        TurnSummary, WebSearchEvent,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use tower::ServiceExt;

    fn temp_path(suffix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "moraine-monitor-core-{suffix}-{}-{stamp}",
            std::process::id()
        ))
    }

    async fn fake_backend(
        responses: InMemoryConversationResponses,
    ) -> (Arc<BackendRepository>, Arc<InMemoryConversationRepository>) {
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            responses,
        ));
        let injected: Arc<dyn ConversationRepository> = repository.clone();
        let router = BackendRepositoryRouter::from_preloaded_for_testing(
            Arc::new(AppConfig::default()),
            [(DEFAULT_BACKEND_NAME.to_string(), injected)],
        )
        .expect("preloaded default router");
        let backend = router
            .default_repository()
            .await
            .expect("preloaded default backend");
        (backend, repository)
    }

    fn fake_state(
        responses: InMemoryConversationResponses,
    ) -> (Arc<AppState>, Arc<InMemoryConversationRepository>) {
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            responses,
        ));
        let injected: Arc<dyn ConversationRepository> = repository.clone();
        let backend_router = Arc::new(
            BackendRepositoryRouter::from_preloaded_for_testing(
                Arc::new(AppConfig::default()),
                [(DEFAULT_BACKEND_NAME.to_string(), injected)],
            )
            .expect("preloaded default router"),
        );
        (
            Arc::new(AppState {
                backend_router,
                static_dir: PathBuf::new(),
            }),
            repository,
        )
    }

    fn routing_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.clickhouse.url = "http://default.example:8123".to_string();
        config.clickhouse.database = "moraine_default".to_string();
        config
            .backends
            .insert(DEFAULT_BACKEND_NAME.to_string(), config.clickhouse.clone());
        config.backends.insert(
            "team-ch".to_string(),
            ClickHouseConfig {
                url: "http://team.example:8123".to_string(),
                database: "moraine_team".to_string(),
                ..ClickHouseConfig::default()
            },
        );
        config.routes = vec![
            RouteConfig {
                dir: "/work/team/**".to_string(),
                backend: "team-ch".to_string(),
                mode: ROUTE_MODE_MIRROR.to_string(),
            },
            RouteConfig {
                dir: "/work/ghost/**".to_string(),
                backend: "not-configured".to_string(),
                mode: ROUTE_MODE_MIRROR.to_string(),
            },
        ];
        config
    }

    fn preloaded_backend_router(
        config: AppConfig,
        default_repository: Arc<InMemoryConversationRepository>,
        named_repository: Arc<InMemoryConversationRepository>,
    ) -> Arc<BackendRepositoryRouter> {
        let default_repository: Arc<dyn ConversationRepository> = default_repository;
        let named_repository: Arc<dyn ConversationRepository> = named_repository;
        Arc::new(
            BackendRepositoryRouter::from_preloaded_for_testing(
                Arc::new(config),
                [
                    (DEFAULT_BACKEND_NAME.to_string(), default_repository),
                    ("team-ch".to_string(), named_repository),
                ],
            )
            .expect("preloaded routing backend"),
        )
    }

    fn static_root(suffix: &str, index: &[u8]) -> PathBuf {
        let root = temp_path(suffix);
        fs::create_dir_all(&root).expect("create static root");
        fs::write(root.join("index.html"), index).expect("write index");
        root
    }

    async fn get_with_project_dir(
        app: &Router,
        uri: &str,
        project_dir: Option<HeaderValue>,
    ) -> Response {
        let mut request = Request::builder().uri(uri);
        if let Some(project_dir) = project_dir {
            request = request.header(PROJECT_DIR_HEADER, project_dir);
        }
        app.clone()
            .oneshot(request.body(Body::empty()).expect("request"))
            .await
            .expect("response")
    }

    async fn response_json(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        serde_json::from_slice(&body).expect("response json")
    }

    async fn router_json(app: &Router, uri: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("API request"),
            )
            .await
            .expect("API response");
        let status = response.status();
        (status, response_json(response).await)
    }

    fn sample_health() -> StoreHealth {
        StoreHealth {
            ping: StoreProbe::Available(3.5),
            version: StoreProbe::Available("25.1.1".to_string()),
            database_exists: StoreProbe::Available(true),
            connections: StoreProbe::Available(StoreConnectionMetrics {
                total: 15,
                tcp: 2,
                http: 3,
                mysql: 4,
                postgres: 5,
                interserver: 1,
            }),
        }
    }

    fn sample_heartbeat() -> IngestHeartbeatRead {
        IngestHeartbeatRead {
            table_present: true,
            latest: Some(IngestHeartbeat {
                ts: "2026-07-10 00:00:00.000".to_string(),
                ts_unix_ms: unix_now_ms(),
                host: "host-a".to_string(),
                service_version: "0.6.4".to_string(),
                queue_depth: 1,
                files_active: 2,
                files_watched: 3,
                rows_raw_written: 4,
                rows_events_written: 5,
                rows_errors_written: 0,
                flush_latency_ms: 6,
                append_to_visible_p50_ms: 7,
                append_to_visible_p95_ms: 8,
                last_error: String::new(),
                watcher_backend: Some("fsevents".to_string()),
                watcher_error_count: Some(0),
                watcher_reset_count: Some(0),
                watcher_last_reset_unix_ms: None,
                backend_sinks: Some(json!({"team-ch": "healthy"})),
            }),
        }
    }

    fn sample_session() -> SessionAnalytics {
        let assistant_buckets =
            BTreeMap::from([("output_text".to_string(), 4), ("reasoning".to_string(), 2)]);
        SessionAnalytics {
            summary: ConversationSummary {
                session_id: "session-1".to_string(),
                first_event_time: "2026-02-16T12:00:00.000Z".to_string(),
                first_event_unix_ms: 1_771_243_200_000,
                last_event_time: "2026-02-16T12:00:03.900Z".to_string(),
                last_event_unix_ms: 1_771_243_203_900,
                total_turns: 1,
                total_events: 7,
                user_messages: 1,
                assistant_messages: 1,
                tool_calls: 1,
                tool_results: 1,
                mode: ConversationMode::ToolCalling,
                session_slug: None,
                session_summary: None,
            },
            harness: "codex".to_string(),
            source_name: "ci-codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            trace_id: "trace-1".to_string(),
            first_user_text: "Inspect the repository".to_string(),
            turns: vec![SessionTurn {
                summary: TurnSummary {
                    session_id: "session-1".to_string(),
                    turn_seq: 1,
                    turn_id: "turn-1".to_string(),
                    started_at: "2026-02-16T12:00:00.000Z".to_string(),
                    started_at_unix_ms: 1_771_243_200_000,
                    ended_at: "2026-02-16T12:00:03.900Z".to_string(),
                    ended_at_unix_ms: 1_771_243_203_900,
                    total_events: 7,
                    user_messages: 1,
                    assistant_messages: 1,
                    tool_calls: 1,
                    tool_results: 1,
                    reasoning_items: 1,
                },
                model: "gpt-5.3-codex".to_string(),
                token_usage_buckets: BTreeMap::from([
                    ("input_text".to_string(), 10),
                    ("output_text".to_string(), 4),
                    ("reasoning".to_string(), 2),
                ]),
                steps: vec![
                    SessionStep::User {
                        event_unix_ms: 1_771_243_200_000,
                        text: "Inspect the repository".to_string(),
                    },
                    SessionStep::Assistant {
                        event_unix_ms: 1_771_243_201_000,
                        text: "I will inspect it".to_string(),
                        endpoint_kind: "responses".to_string(),
                        latency_ms: Some(900),
                        token_usage_buckets: assistant_buckets,
                        token_usage_native_units: BTreeMap::new(),
                    },
                    SessionStep::ToolCall {
                        event_unix_ms: 1_771_243_202_000,
                        tool_name: "Read".to_string(),
                        call_id: "call-1".to_string(),
                        arguments: json!({"path": "Cargo.toml"}),
                        latency_ms: Some(250),
                        is_error: false,
                        result: Some(ToolResult {
                            event_unix_ms: 1_771_243_203_000,
                            text: "workspace".to_string(),
                            latency_ms: 1_000,
                            is_error: false,
                        }),
                    },
                ],
            }],
        }
    }

    fn successful_responses() -> InMemoryConversationResponses {
        InMemoryConversationResponses {
            list_session_analytics: Some(Ok(vec![sample_session()])),
            analytics_series: Some(Ok(AnalyticsSnapshot {
                window: AnalyticsWindow {
                    range: AnalyticsRange::SevenDays,
                    window_seconds: 604_800,
                    bucket_seconds: 21_600,
                    from_unix: 100,
                    to_unix: 200,
                },
                tokens: vec![AnalyticsTokenPoint {
                    bucket_unix: 100,
                    model: "gpt-5.3-codex".to_string(),
                    endpoint_kind: "responses".to_string(),
                    bucket: "output_text".to_string(),
                    tokens: 4,
                }],
                turns: vec![AnalyticsTurnPoint {
                    bucket_unix: 100,
                    model: "gpt-5.3-codex".to_string(),
                    turns: 1,
                }],
                concurrent_sessions: vec![AnalyticsConcurrencyPoint {
                    bucket_unix: 100,
                    concurrent_sessions: 1,
                }],
            })),
            list_web_searches: Some(Ok(vec![WebSearchEvent {
                event_time: "2026-02-16T12:00:00.000Z".to_string(),
                harness: "codex".to_string(),
                source_name: "ci-codex".to_string(),
                session_id: "session-1".to_string(),
                model: "gpt-5.3-codex".to_string(),
                action: "search".to_string(),
                search_query: "moraine".to_string(),
                result_url: String::new(),
                source_ref: "fixture".to_string(),
            }])),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            list_table_summaries: Some(Ok(TableSummaries {
                tables: vec![TableSummary {
                    name: "events".to_string(),
                    engine: "ReplacingMergeTree".to_string(),
                    is_temporary: false,
                    rows: 7,
                }],
                row_counts_error: None,
            })),
            preview_table: Some(Ok(TablePreview {
                table: "events".to_string(),
                limit: 500,
                schema: vec![TableColumn {
                    name: "session_id".to_string(),
                    type_name: "String".to_string(),
                    default_expression: String::new(),
                }],
                rows: vec![json!({"session_id": "session-1"})],
            })),
            read_store_health: Some(Ok(sample_health())),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn capabilities_report_runtime_schema_and_feature_facts() {
        let (state, repository) = fake_state(InMemoryConversationResponses {
            read_store_diagnostics: Some(Ok(StoreDiagnostics {
                applied_schema_versions: vec![
                    "003".to_string(),
                    "025".to_string(),
                    "017".to_string(),
                ],
                ..Default::default()
            })),
            ..Default::default()
        });

        let response = api_capabilities(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            json!({
                "ok": true,
                "server_version": env!("CARGO_PKG_VERSION"),
                "schema_migration_level": "025",
                "features": {
                    "analytics": true,
                    "sessions": true,
                    "table_inspection": true,
                    "web_searches": true,
                },
            })
        );
        assert_eq!(repository.calls().read_store_diagnostics, 1);
    }

    #[tokio::test]
    async fn capabilities_keep_schema_level_null_when_diagnostics_are_unavailable() {
        for response in [
            Ok(StoreDiagnostics::default()),
            Err(RepoError::backend("migration ledger unavailable")),
        ] {
            let (state, repository) = fake_state(InMemoryConversationResponses {
                read_store_diagnostics: Some(response),
                ..Default::default()
            });

            let response = api_capabilities(State(state)).await;
            assert_eq!(response.status(), StatusCode::OK);
            let payload = response_json(response).await;
            assert_eq!(payload["ok"], json!(true));
            assert_eq!(payload["schema_migration_level"], Value::Null);
            assert_eq!(repository.calls().read_store_diagnostics, 1);
        }
    }

    #[tokio::test]
    async fn versioned_route_errors_keep_existing_status_and_envelope() {
        let (state, _) = fake_state(InMemoryConversationResponses {
            analytics_series: Some(Err(RepoError::backend("analytics unavailable"))),
            ..Default::default()
        });
        let app = monitor_router(state);

        let canonical = router_json(&app, "/api/v1/analytics?range=24h").await;
        let legacy = router_json(&app, "/api/analytics?range=24h").await;
        assert_eq!(canonical, legacy);
        assert_eq!(canonical.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(canonical.1["ok"], json!(false));
        assert_eq!(
            canonical.1["error"],
            json!("analytics query failed: backend error: analytics unavailable")
        );

        let malformed = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/sessions?limit=not-a-number")
                    .body(Body::empty())
                    .expect("malformed query request"),
            )
            .await
            .expect("malformed query response");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn handlers_delegate_to_shared_repository_and_preserve_json_contracts() {
        let (backend, repository) = fake_backend(successful_responses()).await;

        let response = api_health(Extension(backend.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let health = response_json(response).await;
        assert_eq!(health["ok"], json!(true));
        assert_eq!(health["version"], json!("25.1.1"));
        assert_eq!(health["connections"]["total"], json!(15));
        assert_eq!(
            health["ingestor"]["latest"],
            json!({"backend_sinks": {"team-ch": "healthy"}})
        );

        let response = api_status(Extension(backend.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let status = response_json(response).await;
        assert_eq!(status["database"]["exists"], json!(true));
        assert_eq!(status["database"]["table_count"], json!(1));
        assert_eq!(status["database"]["estimated_total_rows"], json!(7));
        assert_eq!(status["ingestor"]["latest"]["host"], json!("host-a"));
        let status_latest = status["ingestor"]["latest"]
            .as_object()
            .expect("status latest");
        assert!(!status_latest.contains_key("watcher_backend"));
        assert!(!status_latest.contains_key("watcher_error_count"));
        assert!(!status_latest.contains_key("watcher_reset_count"));
        assert!(!status_latest.contains_key("watcher_last_reset_unix_ms"));

        let response = api_tables(Extension(backend.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let tables = response_json(response).await;
        assert_eq!(tables["tables"][0]["is_temporary"], json!(0));

        let response = api_web_searches(
            Query(LimitQuery { limit: Some(2_500) }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let web_searches = response_json(response).await;
        assert_eq!(web_searches["limit"], json!(1_000));
        assert_eq!(web_searches["schema"].as_array().unwrap().len(), 9);
        assert_eq!(web_searches["rows"][0]["search_query"], json!("moraine"));

        let response = api_analytics(
            Query(AnalyticsQuery {
                range: Some("7d".to_string()),
            }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let analytics = response_json(response).await;
        assert_eq!(analytics["range"]["key"], json!("7d"));
        assert_eq!(analytics["range"]["label"], json!("Last 7d"));
        assert_eq!(analytics["series"]["tokens"][0]["tokens"], json!(4));

        let response = api_sessions(
            Query(SessionsQuery {
                limit: Some(0),
                since: Some("not-a-window".to_string()),
            }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let sessions = response_json(response).await;
        let session = &sessions["sessions"][0];
        assert_eq!(session["id"], json!("session-1"));
        assert_eq!(session["endedAt"], json!(1_771_243_203_900_i64));
        assert_eq!(session["turns"][0]["idx"], json!(0));
        assert_eq!(session["turns"][0]["promptTokens"], json!(10));
        assert_eq!(session["turns"][0]["completionTokens"], json!(6));
        assert_eq!(session["turns"][0]["steps"][1]["durationMs"], json!(900));
        assert_eq!(session["turns"][0]["steps"][2]["latencyMs"], json!(1_000));
        assert!(
            session.get("eventCount").is_none(),
            "session response shape must remain unchanged"
        );

        let response = api_table_rows(
            Path("events".to_string()),
            Query(LimitQuery { limit: Some(999) }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let preview = response_json(response).await;
        assert_eq!(preview["limit"], json!(500));
        assert_eq!(preview["schema"][0]["type"], json!("String"));

        let calls = repository.calls();
        assert_eq!(calls.read_store_health, 2);
        assert_eq!(calls.read_store_diagnostics, 0);
        assert_eq!(calls.latest_ingest_heartbeat, 2);
        assert_eq!(calls.list_table_summaries, 2);
        assert_eq!(calls.list_web_searches, vec![1_000]);
        assert_eq!(calls.analytics_series, vec![AnalyticsRange::SevenDays]);
        assert_eq!(
            calls.list_session_analytics,
            vec![SessionAnalyticsQuery {
                lookback: SessionLookback::ThirtyDays,
                limit: 1,
            }]
        );
        assert_eq!(
            calls.preview_table,
            vec![TablePreviewQuery {
                table: "events".to_string(),
                limit: 500,
            }]
        );
    }

    #[tokio::test]
    async fn repository_failures_keep_existing_http_status_envelopes() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            list_session_analytics: Some(Err(RepoError::backend("sessions unavailable"))),
            analytics_series: Some(Err(RepoError::backend("analytics unavailable"))),
            list_web_searches: Some(Err(RepoError::backend("web unavailable"))),
            list_table_summaries: Some(Err(RepoError::backend("tables unavailable"))),
            preview_table: Some(Err(RepoError::invalid_argument("unsafe table"))),
            read_store_health: Some(Ok(StoreHealth {
                ping: StoreProbe::Failed {
                    message: "ping unavailable".to_string(),
                },
                ..sample_health()
            })),
            ..Default::default()
        })
        .await;

        let health = api_health(Extension(backend.clone())).await;
        assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(health).await["error"],
            json!("ping unavailable")
        );

        let analytics = api_analytics(
            Query(AnalyticsQuery { range: None }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(analytics.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response_json(analytics).await["ok"], json!(false));

        let sessions = api_sessions(
            Query(SessionsQuery {
                limit: None,
                since: None,
            }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(sessions.status(), StatusCode::SERVICE_UNAVAILABLE);

        let web = api_web_searches(
            Query(LimitQuery { limit: None }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(web.status(), StatusCode::SERVICE_UNAVAILABLE);

        let tables = api_tables(Extension(backend.clone())).await;
        assert_eq!(tables.status(), StatusCode::SERVICE_UNAVAILABLE);

        let preview = api_table_rows(
            Path("events;drop".to_string()),
            Query(LimitQuery { limit: None }),
            Extension(backend),
        )
        .await;
        assert_eq!(preview.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(preview).await["error"],
            json!("invalid table name")
        );
    }

    #[test]
    fn default_and_invalid_ranges_keep_legacy_fallbacks() {
        assert_eq!(
            resolve_analytics_range(None),
            AnalyticsRange::TwentyFourHours
        );
        assert_eq!(
            resolve_analytics_range(Some("invalid")),
            AnalyticsRange::TwentyFourHours
        );
        assert_eq!(resolve_session_lookback(None), SessionLookback::ThirtyDays);
        assert_eq!(
            resolve_session_lookback(Some("invalid")),
            SessionLookback::ThirtyDays
        );
        assert_eq!(resolve_session_lookback(Some("all")), SessionLookback::All);
    }

    #[test]
    fn unmatched_tool_call_preserves_call_latency_and_error() {
        let step = monitor_step_json(SessionStep::ToolCall {
            event_unix_ms: 1_000,
            tool_name: "Read".to_string(),
            call_id: "call-unmatched".to_string(),
            arguments: json!({"path": "Cargo.toml"}),
            latency_ms: Some(321),
            is_error: true,
            result: None,
        });

        assert_eq!(step["latencyMs"], json!(321));
        assert_eq!(step["status"], json!("error"));
        assert_eq!(step["result"], json!(""));
        assert_eq!(step["resultAt"], json!(1_000));
    }

    #[tokio::test]
    async fn api_health_redacts_full_heartbeat_internals() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(sample_health())),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            ..Default::default()
        })
        .await;
        let payload = response_json(api_health(Extension(backend)).await).await;
        let latest = payload["ingestor"]["latest"].as_object().expect("latest");

        assert_eq!(latest.len(), 1);
        assert_eq!(latest["backend_sinks"]["team-ch"], json!("healthy"));
        assert!(!latest.contains_key("host"));
        assert!(!latest.contains_key("last_error"));
    }

    #[tokio::test]
    async fn pre017_heartbeat_keeps_legacy_health_and_status_shapes() {
        let mut heartbeat = sample_heartbeat();
        let latest = heartbeat.latest.as_mut().expect("latest heartbeat");
        latest.backend_sinks = None;
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(sample_health())),
            latest_ingest_heartbeat: Some(Ok(heartbeat)),
            ..Default::default()
        })
        .await;

        let health = response_json(api_health(Extension(backend.clone())).await).await;
        assert_eq!(health["ingestor"]["latest"]["backend_sinks"], json!({}));

        let status = response_json(api_status(Extension(backend)).await).await;
        let latest = status["ingestor"]["latest"].as_object().expect("latest");
        assert!(!latest.contains_key("backend_sinks"));
        assert!(!latest.contains_key("watcher_backend"));
    }

    #[tokio::test]
    async fn versioned_routes_alias_legacy_payloads_and_preserve_static_assets() {
        const INDEX_BYTES: &[u8] = b"<!doctype html><title>shared-backend</title>\n";
        let root = temp_path("versioned-router");
        fs::create_dir_all(&root).expect("create static root");
        fs::write(root.join("index.html"), INDEX_BYTES).expect("write index");

        let mut responses = successful_responses();
        responses.latest_ingest_heartbeat = Some(Ok(IngestHeartbeatRead {
            table_present: true,
            latest: None,
        }));
        responses.read_store_diagnostics = Some(Ok(StoreDiagnostics {
            applied_schema_versions: vec!["001".to_string(), "025".to_string()],
            ..Default::default()
        }));
        let repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            responses,
        ));
        let injected: Arc<dyn ConversationRepository> = repository.clone();
        let backend_router = Arc::new(
            BackendRepositoryRouter::from_preloaded_for_testing(
                Arc::new(AppConfig::default()),
                [(DEFAULT_BACKEND_NAME.to_string(), injected)],
            )
            .expect("preloaded default router"),
        );
        let app = router_with_backend_router(backend_router, root.clone())
            .expect("build injected router");

        let static_response = get_with_project_dir(
            &app,
            "/",
            Some(HeaderValue::from_static("malformed-relative-path")),
        )
        .await;
        assert_eq!(static_response.status(), StatusCode::OK);
        assert_eq!(
            static_response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/html"))
        );
        let static_body = to_bytes(static_response.into_body(), usize::MAX)
            .await
            .expect("static body");
        assert_eq!(&static_body[..], INDEX_BYTES);

        let route_matrix = [
            ("/api/v1/health", "/api/health"),
            ("/api/v1/status", "/api/status"),
            ("/api/v1/analytics?range=7d", "/api/analytics?range=7d"),
            ("/api/v1/tables", "/api/tables"),
            (
                "/api/v1/web-searches?limit=1000",
                "/api/web-searches?limit=1000",
            ),
            (
                "/api/v1/tables/events?limit=500",
                "/api/tables/events?limit=500",
            ),
            (
                "/api/v1/sessions?since=30d&limit=1",
                "/api/sessions?since=30d&limit=1",
            ),
        ];
        for (canonical_path, legacy_path) in route_matrix {
            let canonical = router_json(&app, canonical_path).await;
            let legacy = router_json(&app, legacy_path).await;
            assert_eq!(
                canonical, legacy,
                "{legacy_path} must directly alias {canonical_path}"
            );
            assert_eq!(canonical.0, StatusCode::OK);
        }

        let (status, capabilities) = router_json(&app, "/api/v1/capabilities").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(capabilities["server_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(capabilities["schema_migration_level"], json!("025"));
        assert_eq!(
            capabilities["features"],
            json!({
                "analytics": true,
                "sessions": true,
                "table_inspection": true,
                "web_searches": true,
            })
        );

        let (status, missing) = router_json(&app, "/api/v1/not-a-route").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing, json!({"ok": false, "error": "not found"}));
        let calls = repository.calls();
        assert_eq!(calls.read_store_health, 4);
        assert_eq!(calls.read_store_diagnostics, 1);
        assert_eq!(calls.latest_ingest_heartbeat, 4);
        assert_eq!(calls.list_table_summaries, 4);
        assert_eq!(calls.list_web_searches, vec![1_000, 1_000]);
        assert_eq!(
            calls.analytics_series,
            vec![AnalyticsRange::SevenDays, AnalyticsRange::SevenDays]
        );
        assert_eq!(
            calls.list_session_analytics,
            vec![
                SessionAnalyticsQuery {
                    lookback: SessionLookback::ThirtyDays,
                    limit: 1,
                },
                SessionAnalyticsQuery {
                    lookback: SessionLookback::ThirtyDays,
                    limit: 1,
                },
            ]
        );
        assert_eq!(
            calls.preview_table,
            vec![
                TablePreviewQuery {
                    table: "events".to_string(),
                    limit: 500,
                },
                TablePreviewQuery {
                    table: "events".to_string(),
                    limit: 500,
                },
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn data_routes_select_default_named_unknown_and_reuse_repositories() {
        let root = static_root("routing-selection", b"<!doctype html>");
        let default_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            successful_responses(),
        ));
        let named_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            successful_responses(),
        ));
        let backend_router = preloaded_backend_router(
            routing_config(),
            default_repository.clone(),
            named_repository.clone(),
        );
        let app =
            router_with_backend_router(backend_router, root.clone()).expect("routing test app");

        let default =
            response_json(get_with_project_dir(&app, "/api/v1/tables", None).await).await;
        assert_eq!(default_repository.calls().list_table_summaries, 1);
        assert_eq!(named_repository.calls().list_table_summaries, 0);

        let named = response_json(
            get_with_project_dir(
                &app,
                "/api/v1/tables",
                Some(HeaderValue::from_static("  /work/team/project  ")),
            )
            .await,
        )
        .await;
        let unknown = response_json(
            get_with_project_dir(
                &app,
                "/api/tables",
                Some(HeaderValue::from_static("/work/ghost/project")),
            )
            .await,
        )
        .await;
        let named_again = response_json(
            get_with_project_dir(
                &app,
                "/api/tables",
                Some(HeaderValue::from_static("/work/team/other")),
            )
            .await,
        )
        .await;

        assert_eq!(default, named);
        assert_eq!(default, unknown);
        assert_eq!(default, named_again);
        assert_eq!(default_repository.calls().list_table_summaries, 2);
        assert_eq!(named_repository.calls().list_table_summaries, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn capabilities_ignore_project_selector_and_use_default_schema() {
        let root = static_root("routing-capabilities", b"<!doctype html>");
        let mut default_responses = successful_responses();
        default_responses.read_store_diagnostics = Some(Ok(StoreDiagnostics {
            applied_schema_versions: vec!["025".to_string()],
            ..Default::default()
        }));
        let mut named_responses = successful_responses();
        named_responses.read_store_diagnostics = Some(Ok(StoreDiagnostics {
            applied_schema_versions: vec!["999".to_string()],
            ..Default::default()
        }));
        let default_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            default_responses,
        ));
        let named_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            named_responses,
        ));
        let backend_router = preloaded_backend_router(
            routing_config(),
            default_repository.clone(),
            named_repository.clone(),
        );
        let app = router_with_backend_router(backend_router, root.clone())
            .expect("capabilities routing test app");

        for header in [
            None,
            Some(HeaderValue::from_static("/work/team/project")),
            Some(HeaderValue::from_static("malformed-relative-path")),
        ] {
            let response =
                get_with_project_dir(&app, "/api/v1/capabilities", header).await;
            assert_eq!(response.status(), StatusCode::OK);
            let payload = response_json(response).await;
            assert_eq!(payload["schema_migration_level"], json!("025"));
        }
        assert_eq!(default_repository.calls().read_store_diagnostics, 3);
        assert_eq!(named_repository.calls().read_store_diagnostics, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn health_and_status_report_selected_backend_metadata() {
        let root = static_root("routing-metadata", b"<!doctype html>");
        let default_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            successful_responses(),
        ));
        let named_repository = Arc::new(InMemoryConversationRepository::with_responses(
            RepoConfig::default(),
            successful_responses(),
        ));
        let backend_router =
            preloaded_backend_router(routing_config(), default_repository, named_repository);
        let app =
            router_with_backend_router(backend_router, root.clone()).expect("metadata test app");

        let default_health =
            response_json(get_with_project_dir(&app, "/api/health", None).await).await;
        assert_eq!(default_health["url"], json!("http://default.example:8123"));
        assert_eq!(default_health["database"], json!("moraine_default"));

        let named_header = HeaderValue::from_static("/work/team/project");
        let named_health = response_json(
            get_with_project_dir(&app, "/api/health", Some(named_header.clone())).await,
        )
        .await;
        assert_eq!(named_health["url"], json!("http://team.example:8123"));
        assert_eq!(named_health["database"], json!("moraine_team"));

        let named_status =
            response_json(get_with_project_dir(&app, "/api/status", Some(named_header)).await)
                .await;
        assert_eq!(
            named_status["clickhouse"]["url"],
            json!("http://team.example:8123")
        );
        assert_eq!(
            named_status["clickhouse"]["database"],
            json!("moraine_team")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn project_dir_header_validation_rejects_bad_data_requests() {
        let root = static_root("routing-validation", b"<!doctype html>");
        let default_repository =
            Arc::new(InMemoryConversationRepository::new(RepoConfig::default()));
        let named_repository = Arc::new(InMemoryConversationRepository::new(RepoConfig::default()));
        let backend_router = preloaded_backend_router(
            routing_config(),
            default_repository.clone(),
            named_repository.clone(),
        );
        let app =
            router_with_backend_router(backend_router, root.clone()).expect("validation test app");

        let mut repeated = Request::builder()
            .uri("/api/health")
            .body(Body::empty())
            .expect("repeated header request");
        repeated.headers_mut().append(
            PROJECT_DIR_HEADER,
            HeaderValue::from_static("/work/team/one"),
        );
        repeated.headers_mut().append(
            PROJECT_DIR_HEADER,
            HeaderValue::from_static("/work/team/two"),
        );
        let requests = vec![
            repeated,
            Request::builder()
                .uri("/api/health")
                .header(PROJECT_DIR_HEADER, HeaderValue::from_static("   "))
                .body(Body::empty())
                .expect("empty header request"),
            Request::builder()
                .uri("/api/health")
                .header(
                    PROJECT_DIR_HEADER,
                    HeaderValue::from_static("relative/project"),
                )
                .body(Body::empty())
                .expect("relative header request"),
            Request::builder()
                .uri("/api/health")
                .header(
                    PROJECT_DIR_HEADER,
                    HeaderValue::from_bytes(&[0xff]).expect("opaque header"),
                )
                .body(Body::empty())
                .expect("non-UTF-8 header request"),
        ];

        for request in requests {
            let response = app.clone().oneshot(request).await.expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let payload = response_json(response).await;
            assert_eq!(payload["ok"], json!(false));
            assert!(payload["error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()));
        }
        assert_eq!(default_repository.calls().read_store_health, 0);
        assert_eq!(named_repository.calls().read_store_health, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn named_backend_construction_errors_return_service_unavailable() {
        let root = static_root("routing-construction-error", b"<!doctype html>");
        let mut config = routing_config();
        config
            .backends
            .get_mut("team-ch")
            .expect("named backend")
            .url = "://invalid".to_string();
        let backend_router = Arc::new(
            BackendRepositoryRouter::new(
                Arc::new(config),
                RepoConfig::default(),
                "moraine-monitor-core/test",
            )
            .expect("lazy backend router"),
        );
        let app = router_with_backend_router(backend_router, root.clone())
            .expect("construction error test app");

        let response = get_with_project_dir(
            &app,
            "/api/health",
            Some(HeaderValue::from_static("/work/team/project")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let payload = response_json(response).await;
        assert_eq!(payload["ok"], json!(false));
        assert!(payload["error"]
            .as_str()
            .is_some_and(|error| error.contains("team-ch")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_static_dir_override_is_authoritative() {
        let path = temp_path("explicit-static");
        assert_eq!(resolve_static_dir(Some(path.clone())), path);
    }

    #[test]
    fn validate_static_dir_accepts_built_directory() {
        let root = temp_path("static-valid");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("index.html"), "<!doctype html>").expect("write index");

        validate_static_dir(&root).expect("valid static dir");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validate_static_dir_rejects_missing_directory() {
        let missing = temp_path("static-missing");
        let error = validate_static_dir(&missing).expect_err("missing static dir should fail");
        assert!(error.to_string().contains("is unavailable"));
    }

    #[test]
    fn validate_static_dir_rejects_non_directory() {
        let root = temp_path("static-file");
        fs::create_dir_all(&root).expect("create root");
        let path = root.join("dist");
        fs::write(&path, "not a dir").expect("write file");

        let error = validate_static_dir(&path).expect_err("file should fail");
        assert!(error.to_string().contains("is not a directory"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validate_static_dir_requires_index_html() {
        let root = temp_path("static-no-index");
        fs::create_dir_all(&root).expect("create root");

        let error = validate_static_dir(&root).expect_err("missing index should fail");
        assert!(error.to_string().contains("does not contain `index.html`"));

        let _ = fs::remove_dir_all(root);
    }
}
