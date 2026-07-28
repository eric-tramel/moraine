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
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
#[cfg(test)]
use moraine_config::AppConfig;
use moraine_config::{QueryBudgetsConfig, ValidatedQueryBudgets};
use moraine_conversations::{
    budget_telemetry, record_budget_rejection, record_budget_request, session_display_label,
    AnalyticsRange, BackendRepository, BackendRepositoryRouter, CanonicalContinuation,
    CanonicalReadOutcome, ConversationListSort, ConversationMode, CoreIndexHealth, IngestHeartbeat,
    IngestHeartbeatRead, McpSessionListFilter, McpSessionListItem, McpSessionOpen, McpTurnCompact,
    PageRequest, PublicationDiagnostics, QueryClass, QueryEnvelope, RepoError, SessionLookback,
    SessionSearchQuery, StorageReport, StoreConnectionMetrics, StoreHealth, StoreProbe,
    TablePreviewQuery, TableSummaries,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::Semaphore;
use tracing::warn;

/// Concurrent repository-backed dashboard reads admitted at once. Overflow is
/// rejected with 429 `resource_exhausted` instead of queueing unboundedly.
/// `/health` is exempt: supervisors and `moraine status` poll it during
/// incidents, which is exactly when heavy dashboard reads hold the permits.
const MONITOR_READ_PERMITS: usize = 4;

/// Per-request protections for repository-backed monitor endpoints (issue
/// #600 W8): every data request runs inside an Interactive query envelope
/// built from the operator's validated `[query_budgets]`, and heavy reads
/// pass through a small non-queueing admission semaphore.
struct MonitorReadLimits {
    budgets: ValidatedQueryBudgets,
    read_permits: Semaphore,
}

impl MonitorReadLimits {
    fn new(budgets: ValidatedQueryBudgets) -> Self {
        Self {
            budgets,
            read_permits: Semaphore::new(MONITOR_READ_PERMITS),
        }
    }
}

/// Bundled-default budgets for entry points that run without an
/// operator-loaded config (the live-test listener path).
fn default_query_budgets() -> ValidatedQueryBudgets {
    ValidatedQueryBudgets::from_config(&QueryBudgetsConfig::default())
        .expect("bundled default query budgets are valid")
}

struct AppState {
    backend_router: Arc<BackendRepositoryRouter>,
    static_dir: PathBuf,
    read_limits: Arc<MonitorReadLimits>,
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
    cursor: Option<String>,
    harness: Option<String>,
    source: Option<String>,
    mode: Option<String>,
    sort: Option<String>,
}

#[derive(Deserialize)]
struct SessionPageQuery {
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct SessionSearchParams {
    q: Option<String>,
    limit: Option<u32>,
    harness: Option<String>,
    source: Option<String>,
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
    query_budgets: ValidatedQueryBudgets,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    let static_dir_display = static_dir.display().to_string();
    let app = router_with_backend_router(backend_router, static_dir, query_budgets)?;
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

    serve_on_listener(listener, app, bind, static_dir_display, shutdown).await
}

/// Run the monitor HTTP server on an already-bound listener.
///
/// Ownership of the listener transfers to the server and is released after
/// shutdown completes or startup fails. This entry point serves callers that
/// attach without an operator-loaded config (live tests), so its query
/// envelopes use the bundled default `[query_budgets]`; the daemon path
/// threads the operator's budgets through [`run_server_with_router`].
pub async fn run_server_with_listener<S>(
    backend_router: Arc<BackendRepositoryRouter>,
    listener: tokio::net::TcpListener,
    static_dir: PathBuf,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    let static_dir_display = static_dir.display().to_string();
    let app = router_with_backend_router(backend_router, static_dir, default_query_budgets())?;
    let bind = listener
        .local_addr()
        .map_err(|error| anyhow!("failed to read monitor listener address: {error}"))?;

    serve_on_listener(listener, app, bind, static_dir_display, shutdown).await
}

async fn serve_on_listener<S>(
    listener: tokio::net::TcpListener,
    app: Router,
    bind: SocketAddr,
    static_dir_display: String,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    println!("moraine-monitor running at http://{bind}");
    println!("serving UI from {static_dir_display}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// Build the complete monitor router around the daemon-owned backend router.
fn router_with_backend_router(
    backend_router: Arc<BackendRepositoryRouter>,
    static_dir: PathBuf,
    query_budgets: ValidatedQueryBudgets,
) -> Result<Router> {
    validate_static_dir(&static_dir)?;
    let state = Arc::new(AppState {
        backend_router,
        static_dir,
        read_limits: Arc::new(MonitorReadLimits::new(query_budgets)),
    });
    Ok(monitor_router(state))
}

fn monitor_router(state: Arc<AppState>) -> Router {
    // Layer order (outermost first): backend selection -> query envelope ->
    // per-route admission -> handler. The envelope wraps the entire handler
    // future, so an abandoned request (client disconnect) drops the handler
    // inside the scope and the transport's drop guards cancel its statements.
    let data_routes = dashboard_routes(state.read_limits.clone())
        .route_layer(middleware::from_fn_with_state(
            state.read_limits.clone(),
            monitor_query_envelope,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.backend_router.clone(),
            select_backend_repository,
        ));
    // Capabilities is daemon/default-global metadata, not a project-routed data
    // endpoint. It intentionally remains outside project selection middleware.
    let versioned_routes = data_routes.route("/capabilities", get(api_capabilities));

    // The `/api/*` legacy aliases are gone: their documented one-release
    // compatibility window opened with `/api/v1` in v0.7.0 and closed with
    // v0.7.1. An unversioned path now falls through to static handling and
    // returns the JSON 404, which is what an unknown API path has always done.
    Router::new()
        .nest("/api/v1", versioned_routes)
        .fallback(get(static_fallback))
        .with_state(state)
}

fn dashboard_routes(read_limits: Arc<MonitorReadLimits>) -> Router<Arc<AppState>> {
    // `/health` bypasses the read semaphore (see MONITOR_READ_PERMITS) but
    // still runs inside the per-request query envelope added above.
    let admitted = Router::new()
        .route("/status", get(api_status))
        .route("/analytics", get(api_analytics))
        .route("/tables", get(api_tables))
        .route("/web-searches", get(api_web_searches))
        .route("/tables/:table", get(api_table_rows))
        .route("/sessions", get(api_sessions))
        // `/sessions/search` is a fixed path, never a session named "search",
        // and route ORDER has nothing to do with it. The three `/sessions`
        // routes here have one, two and three path segments respectively, so no
        // request path can match two of them and there is no precedence
        // question to get wrong. (`/sessions/search/page` reaches the page
        // handler with `id = "search"`, which is correct: a session really
        // called `search` is still readable.)
        //
        // Precedence would start to matter only if a two-segment parameterised
        // sibling were added — `/sessions/:id` — and it would still be safe,
        // because axum routes through matchit, whose radix trie prefers a
        // static segment over a parameter regardless of insertion order.
        // `matchit_prefers_a_static_segment_over_a_parameter_at_any_registration_order`
        // pins that property against a router built for the purpose: insurance
        // for that future, not the mechanism keeping this table unambiguous
        // today.
        .route("/sessions/search", get(api_session_search))
        .route("/sessions/:id/page", get(api_session_page))
        .route_layer(middleware::from_fn_with_state(
            read_limits,
            monitor_read_admission,
        ));
    Router::new()
        .route("/health", get(api_health))
        .merge(admitted)
}

/// Establish the mandatory Interactive query envelope (issue #600, amendment
/// A6) for one repository-backed monitor request. Per request, never per
/// client or per service: MCP and monitor share one router/repository, so
/// budgets and cancellation must be scoped to this HTTP request alone.
async fn monitor_query_envelope(
    State(limits): State<Arc<MonitorReadLimits>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let envelope = QueryEnvelope::new_with_admin_budget(
        "monitor",
        QueryClass::Interactive,
        &limits.budgets.interactive,
        &limits.budgets.administrative,
    );
    let response = Arc::clone(&envelope).scope(next.run(request)).await;
    // Fold this request's envelope accounting into the process-wide budget
    // sink (issue #600 W11) — these are the monitor's own counters in the
    // `query_budgets` health block. Handlers finish their repository reads
    // before building the response, so the stats are final here.
    record_budget_request(&envelope.stats());
    response
}

/// Bounded admission for repository-heavy dashboard reads: overflow is
/// rejected immediately with 429 `resource_exhausted` rather than queued,
/// so a burst of dashboard traffic cannot pile unbounded work behind the
/// interactive deadline.
async fn monitor_read_admission(
    State(limits): State<Arc<MonitorReadLimits>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let _permit = match limits.read_permits.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            record_budget_rejection();
            return json_response(
                json!({
                    "ok": false,
                    "error": format!(
                        "monitor is serving its maximum of {MONITOR_READ_PERMITS} concurrent reads; retry shortly"
                    ),
                    "code": "resource_exhausted",
                }),
                StatusCode::TOO_MANY_REQUESTS,
            );
        }
    };
    next.run(request).await
}

/// HTTP status plus the additive machine-readable `code` for a repository
/// failure (amendment A11): envelope deadline -> 504 `deadline_exceeded`,
/// budget/cap/allowance exhaustion -> 429 `resource_exhausted`, everything
/// else keeps the pre-envelope 503 with no code. Scope/auth and not-found
/// outcomes never reach this mapping: they travel as `Ok(None)`/empty
/// results, not as `RepoError`.
///
/// The two repository failures caused by the REQUEST rather than by the
/// backend are 400s, because a client must be able to tell "fix your request"
/// apart from "the store is down" — the second is worth retrying and the first
/// never is.
///
/// * A rejected continuation token: paging must distinguish "your cursor is
///   stale, restart the feed" from a transient outage, or a caller that retries
///   pages a silent gap instead of restarting (issue-599 §1.2).
/// * An invalid argument: the repository validates inputs the route cannot
///   (`search_sessions` rejects a query with no searchable terms, and the
///   tokenizer's rules are the repository's to own — a route that re-derived
///   them in its own language would be a second copy that rots). Reported as
///   503 this rendered a typo as an outage, permanently and unrecoverably, for
///   any query the tokenizer cannot split.
fn repo_error_status(error: &RepoError) -> (StatusCode, Option<&'static str>) {
    match error {
        RepoError::DeadlineExceeded { .. } => {
            (StatusCode::GATEWAY_TIMEOUT, Some("deadline_exceeded"))
        }
        RepoError::ResourceExhausted { .. } => {
            (StatusCode::TOO_MANY_REQUESTS, Some("resource_exhausted"))
        }
        RepoError::InvalidCursor(_) => (StatusCode::BAD_REQUEST, Some("invalid_cursor")),
        RepoError::InvalidArgument(_) => (StatusCode::BAD_REQUEST, Some("invalid_argument")),
        _ => (StatusCode::SERVICE_UNAVAILABLE, None),
    }
}

/// `{ok:false,error}` failure response with the additive `code` field when
/// the failure has a budget classification. Existing dashboard fields are
/// untouched (the contract change is additive).
fn repo_error_response(message: String, error: &RepoError) -> Response {
    let (status, code) = repo_error_status(error);
    let mut payload = json!({"ok": false, "error": message});
    if let Some(code) = code {
        payload["code"] = json!(code);
    }
    json_response(payload, status)
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
        Err(_) => {
            warn!("backend project route selection failed; selected backend unavailable or schema-incompatible");
            return json_response(
                json!({
                    "ok": false,
                    "error": "selected backend is unavailable or schema-incompatible"
                }),
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
        // Capabilities sits outside the data-route middleware, so this
        // diagnostics read establishes its own Interactive envelope.
        Ok(default_backend) => {
            let limits = &state.read_limits;
            QueryEnvelope::new_with_admin_budget(
                "monitor",
                QueryClass::Interactive,
                &limits.budgets.interactive,
                &limits.budgets.administrative,
            )
            .scope(async move {
                default_backend
                    .repository()
                    .read_store_diagnostics()
                    .await
                    .ok()
                    .and_then(|diagnostics| diagnostics.applied_schema_versions.into_iter().max())
            })
            .await
        }
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

/// Process-wide query-budget telemetry block (issue #600 W11), a sibling of
/// `publication` in health/status payloads. Counters cover both request
/// boundaries the daemon hosts in this process (MCP tools/call and monitor
/// HTTP requests) plus `unenveloped_statements`, which stays 0 now that the
/// transport fails closed (W12) — a nonzero value would mean a permissive
/// branch regressed into the transport.
/// Additive: existing fields and failure shapes are untouched.
fn query_budgets_payload() -> Value {
    let totals = budget_telemetry();
    json!({
        "requests": totals.requests,
        "statements": totals.statements,
        "deadline_exceeded": totals.deadline_exceeded,
        "resource_exhausted": totals.resource_exhausted,
        "unenveloped_statements": totals.unenveloped_statements,
    })
}

async fn api_health(Extension(backend): Extension<Arc<BackendRepository>>) -> Response {
    let (health, heartbeat) = tokio::join!(
        backend.repository().read_store_health(),
        backend.repository().latest_ingest_heartbeat()
    );
    let health = match health {
        Ok(health) => health,
        Err(error) => {
            let (status, code) = repo_error_status(&error);
            let message = error.to_string();
            let mut payload = json!({
                "ok": false,
                "url": backend.clickhouse_url(),
                "database": backend.clickhouse_database(),
                "error": message,
                "connections": {"total": Value::Null, "error": message},
                "publication": {
                    "available": false,
                    "healthy": false,
                    "error": "publication readiness unavailable while store health is unavailable",
                },
                "core_index": {
                    "available": false,
                    "error": "canonical read-index readiness unavailable while store health is unavailable",
                },
                "storage": {
                    "available": false,
                    "error": "storage report unavailable while store health is unavailable",
                },
                "query_budgets": query_budgets_payload(),
            });
            if let Some(code) = code {
                payload["code"] = json!(code);
            }
            return json_response(payload, status);
        }
    };
    let connections = connection_payload(&health.connections);
    let publication = publication_payload(&health.publication);
    let core_index = core_index_payload(&health.core_index);
    let storage = storage_payload(&health.storage);

    let ping_ms = match &health.ping {
        StoreProbe::Available(value) => *value,
        StoreProbe::Failed { message } => {
            return health_failure_response(
                &backend,
                message,
                connections,
                publication,
                core_index,
                storage,
            );
        }
    };
    let version = match &health.version {
        StoreProbe::Available(value) => value,
        StoreProbe::Failed { message } => {
            return health_failure_response(
                &backend,
                message,
                connections,
                publication,
                core_index,
                storage,
            );
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
            "publication": publication,
            "core_index": core_index,
            "storage": storage,
            "query_budgets": query_budgets_payload(),
            "ingestor": health_heartbeat_payload(&heartbeat),
        }),
        StatusCode::OK,
    )
}

fn health_failure_response(
    backend: &BackendRepository,
    message: &str,
    connections: Value,
    publication: Value,
    core_index: Value,
    storage: Value,
) -> Response {
    json_response(
        json!({
            "ok": false,
            "url": backend.clickhouse_url(),
            "database": backend.clickhouse_database(),
            "error": message,
            "connections": connections,
            "publication": publication,
            "core_index": core_index,
            "storage": storage,
            "query_budgets": query_budgets_payload(),
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
                return repo_error_response(error.to_string(), &error);
            }
        };
        let heartbeat = heartbeat.map(monitor_heartbeat_status).unwrap_or_default();
        (tables, heartbeat)
    } else {
        (Vec::new(), MonitorHeartbeatStatus::default())
    };

    let estimated_total_rows = tables.iter().map(|table| table.rows).sum::<u64>();
    let clickhouse = status_clickhouse_payload(&backend, &health, database_exists);
    let publication = publication_payload(&health.publication);
    let core_index = core_index_payload(&health.core_index);

    json_response(
        json!({
            "ok": true,
            "clickhouse": clickhouse,
            "publication": publication,
            "core_index": core_index,
            // The vocabulary the session feed's `harness` filter accepts.
            // Served rather than inferred: `harness` narrows a keyset-paged
            // feed server-side, so a client deriving the menu from the sessions
            // it happens to have loaded would offer options that change as it
            // pages, and could never select a harness absent from page 1.
            "known_harnesses": moraine_config::KNOWN_INGEST_HARNESSES,
            "query_budgets": query_budgets_payload(),
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
        publication: StoreProbe::Failed {
            message: "publication readiness unavailable while store health is unavailable"
                .to_string(),
        },
        core_index: StoreProbe::Failed {
            message: "canonical read-index readiness unavailable while store health is unavailable"
                .to_string(),
        },
        storage: StoreProbe::Failed {
            message: "storage report unavailable while store health is unavailable".to_string(),
        },
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

fn publication_payload(probe: &StoreProbe<PublicationDiagnostics>) -> Value {
    match probe {
        StoreProbe::Available(diagnostics) => json!({
            "available": true,
            "healthy": diagnostics.is_healthy(),
            "ambiguous_hostless_rows": diagnostics.ambiguous_hostless_rows,
            "replaying_generations": diagnostics.replaying_generations,
            "blocked_generations": diagnostics.blocked_generations,
            "append_preparations": diagnostics.append_preparations,
            "blocked_append_preparations": diagnostics.blocked_append_preparations,
            "mirror_catchup_pending": diagnostics.mirror_catchup_pending,
            "writer_conflicts": diagnostics.writer_conflicts,
            "issues": diagnostics.issues,
        }),
        StoreProbe::Failed { message } => json!({
            "available": false,
            "healthy": false,
            "error": message,
        }),
    }
}

/// The `/api/v1/health` `storage` block (issue #603 WI-02).
///
/// Per-bucket bytes rather than per-table: a health response must not grow
/// with the table count, and the bucket is the unit the ownership model is
/// expressed in. `policy` carries the effective `[retention]` with its
/// provenance, and `destructive_policies` is a non-empty list exactly when
/// configuration authorizes deleting user history — a client should never have
/// to re-derive that from horizons.
///
/// No byte figure here is a "reclaimable" estimate: this block reports what
/// is on disk now. Estimates live behind `moraine db reclaim plan`, where they
/// carry their qualifier.
fn storage_payload(probe: &StoreProbe<StorageReport>) -> Value {
    match probe {
        StoreProbe::Available(report) => json!({
            "available": true,
            "buckets": report
                .buckets
                .iter()
                .map(|bucket| json!({
                    "class": bucket.class.as_str(),
                    "label": bucket.label,
                    "tables": bucket.tables,
                    "rows": bucket.rows,
                    "compressed_bytes": bucket.compressed_bytes,
                }))
                .collect::<Vec<_>>(),
            "total_compressed_bytes": report.total_compressed_bytes(),
            "disk": report.disk.map(|disk| json!({
                "free_bytes": disk.free_bytes,
                "total_bytes": disk.total_bytes,
                "used_bytes": disk.used_bytes(),
            })),
            "policy": report
                .policy
                .iter()
                .map(|entry| json!({
                    "class": entry.class.as_str(),
                    "horizon_seconds": entry.horizon_seconds,
                    "source": entry.source,
                    "config_key": entry.config_key,
                    "destructive": entry.is_destructive(),
                    // `config_key` with no qualification reads as an
                    // invitation to set it. Bucket 4's key is inert, and this
                    // is the API's only chance to say so.
                    "note": entry.note,
                }))
                .collect::<Vec<_>>(),
            "destructive_policies": report
                .destructive_policies()
                .iter()
                .map(|entry| entry.class.as_str())
                .collect::<Vec<_>>(),
            "unclassified_tables": report.unclassified_tables(),
            "notes": report.notes,
        }),
        StoreProbe::Failed { message } => json!({
            "available": false,
            "error": message,
        }),
    }
}

/// Serialize the additive canonical read-index (issue #598) readiness probe for
/// the `/api/v1/health` and `/api/v1/status` payloads. Additive and fail-soft:
/// a store that cannot report readiness yields `{"available": false, ...}` and
/// never fails the surrounding response. Field names mirror the CLI
/// `CoreIndexReport` (issue #598 WI-05) so operators read the same shape across
/// HTTP and CLI.
fn core_index_payload(probe: &StoreProbe<CoreIndexHealth>) -> Value {
    match probe {
        StoreProbe::Available(health) => json!({
            "available": true,
            "core_indexes_ready": health.core_indexes_ready,
            "open_v2_ready": health.open_v2_ready,
            "open_v2_provenance": health.open_v2_provenance,
            "backfill_cursor_age_ms": health.backfill_cursor_age_ms,
            "audit": health.audit_outcome,
        }),
        StoreProbe::Failed { message } => json!({
            "available": false,
            "error": message,
        }),
    }
}

async fn api_tables(Extension(backend): Extension<Arc<BackendRepository>>) -> Response {
    match backend.repository().list_table_summaries().await {
        Ok(tables) => json_response(
            json!({
                "ok": true,
                "read_model": "audit",
                "tables": monitor_table_summaries(tables),
            }),
            StatusCode::OK,
        ),
        Err(error) => repo_error_response(error.to_string(), &error),
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
            return repo_error_response(format!("web search query failed: {error}"), &error);
        }
    };

    json_response(
        json!({
            "ok": true,
            "read_model": "live",
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
            return repo_error_response(format!("analytics query failed: {error}"), &error);
        }
    };

    json_response(
        json!({
            "ok": true,
            "read_model": "live",
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

/// A session with no event newer than this reads as `completed` even without a
/// terminal event, so a feed poll can distinguish live work from a session that
/// simply stopped without one.
const SESSION_ACTIVE_WINDOW_MS: i64 = 60_000;

/// The session feed: SUMMARIES ONLY, one keyset page at a time.
///
/// It serves from [`moraine_conversations::ConversationRepository::list_mcp_sessions`]
/// — the same
/// operation MCP `list_sessions` pages through (issue-599 §1.1) — so the two
/// surfaces cannot drift, and no transcript content is read to render a card.
/// Turns arrive only when a client opens a session, through
/// [`api_session_page`].
///
/// `has_more` is `next_cursor.is_some()` and nothing else. There is
/// deliberately no total: a corpus-wide count is exactly the corpus-sized work
/// this route exists to stop doing. An empty `sessions` with `has_more: true`
/// is a legal "keep going" signal, not "no results".
async fn api_sessions(
    Query(params): Query<SessionsQuery>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    let repository = backend.repository();
    let now_ms = unix_now_ms();
    let lookback = resolve_session_lookback(params.since.as_deref());

    let sort = match params.sort.as_deref() {
        None | Some("desc") => ConversationListSort::Desc,
        Some("asc") => ConversationListSort::Asc,
        Some(other) => return bad_request(format!("sort must be desc or asc, got {other:?}")),
    };
    // An unrecognized mode is rejected rather than ignored: silently dropping
    // it would answer a narrow question with a wide result set.
    let mode = match params
        .mode
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
    {
        None => None,
        Some(value) => match ConversationMode::parse(value) {
            Some(mode) => Some(mode),
            None => return bad_request(format!("unknown mode {value:?}")),
        },
    };
    let token = match optional_cursor(params.cursor.as_deref()) {
        Ok(token) => token,
        Err(message) => return invalid_cursor_response(message),
    };
    // A continuation REPLAYS the window its feed was opened under. The
    // repository binds a cursor to the filter that minted it, so a window
    // re-derived from the current clock would present a different filter and
    // the token this handler issued one request ago would be refused.
    let (window, cursor) = match token.as_deref() {
        None => (resolve_session_window(lookback, now_ms), None),
        Some(token) => match decode_sessions_cursor(token, lookback) {
            Ok(cursor) => (cursor.window, Some(cursor.inner)),
            Err(message) => return invalid_cursor_response(message),
        },
    };

    // Two clamps, both real: the route's documented 1..=200 bound, then the
    // backend's own `max_results`. Reporting the second is what keeps `limit`
    // honest — the repository would otherwise serve fewer rows than the
    // response claims to have asked for.
    let requested = params.limit.unwrap_or(50).clamp(1, 200) as u16;
    let limit = requested.min(repository.config().max_results.max(1));

    let filter = McpSessionListFilter {
        start_unix_ms: window.start_unix_ms,
        end_unix_ms: window.end_unix_ms,
        mode,
        sort,
        // A cleared dashboard filter arrives as an empty value; treat it as
        // absent rather than as a filter that matches nothing.
        harness: optional_filter_value(params.harness.as_deref()),
        source_name: optional_filter_value(params.source.as_deref()),
    };
    let page = match repository
        .list_mcp_sessions(filter, PageRequest { limit, cursor })
        .await
    {
        Ok(page) => page,
        Err(error) => {
            return repo_error_response(format!("sessions query failed: {error}"), &error);
        }
    };

    let sessions = page
        .items
        .iter()
        .map(|session| monitor_session_json(session, now_ms))
        .collect::<Vec<_>>();

    let next_cursor = match page.next_cursor {
        None => None,
        Some(inner) => match encode_sessions_cursor(lookback, window, inner) {
            Ok(token) => Some(token),
            Err(message) => {
                return json_response(
                    json!({"ok": false, "error": message}),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        },
    };

    let has_more = next_cursor.is_some();
    json_response(
        json!({
            "ok": true,
            "read_model": "live",
            "sessions": sessions,
            "limit": limit,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "window": {"start": window.start_unix_ms, "end": window.end_unix_ms},
        }),
        StatusCode::OK,
    )
}

/// Route bound on ranked sessions per search. Well below `/api/v1/sessions`'
/// `200`, deliberately: a BM25 ranking's value is concentrated in its head, and
/// every ranked session costs a hydration slot in the same bounded budget the
/// feed spends. The backend's `max_results` clamps this further.
const SESSION_SEARCH_MAX_LIMIT: u32 = 50;

/// Default ranked sessions per search, matching the repository's own default.
const SESSION_SEARCH_DEFAULT_LIMIT: u32 = 10;

/// Whole-corpus session search (issue-599 WI-09).
///
/// Ranked by content over the entire corpus this backend may serve — not over
/// the page the client happens to have loaded, which is what the interim
/// client-side filter could do and said so. Results are the SAME summary
/// objects `/api/v1/sessions` returns, built by
/// [`monitor_session_json`], so a result opens through
/// `/api/v1/sessions/:id/page` exactly like a listed session.
///
/// There is deliberately no `cursor`. A keyset over a relevance ranking is not
/// the keyset the feed uses — scores are not a monotone anchor and a
/// re-ranked corpus would silently skip or repeat — so this route bounds
/// instead of paging, and says so with `truncated`.
///
/// Project scope is enforced by the repository operation, against each
/// session's hydrated `origin_cwd`; the ranking's own directory predicate is a
/// recall filter. This handler composes no second repository read, so there is
/// no path by which it can assemble an out-of-scope session.
async fn api_session_search(
    Query(params): Query<SessionSearchParams>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    let repository = backend.repository();
    let query = params.q.as_deref().map(str::trim).unwrap_or_default();
    if query.is_empty() {
        // Not an empty result: an absent or blank `q` is a client bug, and
        // answering it with "no sessions match" would read as an empty corpus.
        return bad_request("q must be a non-empty search query".to_string());
    }

    // Two clamps, as on the feed: the route's documented bound, then the
    // backend's own `max_results`. The response reports the second.
    let requested = params
        .limit
        .unwrap_or(SESSION_SEARCH_DEFAULT_LIMIT)
        .clamp(1, SESSION_SEARCH_MAX_LIMIT) as u16;
    let limit = requested.min(repository.config().max_results.max(1));

    let results = match repository
        .search_session_summaries(SessionSearchQuery {
            query: query.to_string(),
            // The #600 envelope this route runs inside. Threading its request id
            // is what makes the repository's `query_id` — and therefore the
            // statements it issues — traceable back to one HTTP request, and it
            // is absent exactly when the route is not enveloped.
            cancellation_token: QueryEnvelope::current()
                .ok()
                .map(|envelope| envelope.request_id().to_string()),
            limit: Some(limit),
            // A cleared dashboard filter arrives as an empty value; treat it as
            // absent rather than as a filter that matches nothing.
            harness: optional_filter_value(params.harness.as_deref()),
            source_name: optional_filter_value(params.source.as_deref()),
        })
        .await
    {
        Ok(results) => results,
        Err(error) => {
            return repo_error_response(format!("session search failed: {error}"), &error);
        }
    };

    let now_ms = unix_now_ms();
    let sessions = results
        .sessions
        .iter()
        .map(|session| monitor_session_json(session, now_ms))
        .collect::<Vec<_>>();

    json_response(
        json!({
            "ok": true,
            "read_model": "live",
            "query": results.query,
            "terms": results.terms,
            "sessions": sessions,
            "limit": limit,
            "result_count": sessions.len(),
            // Four distinct facts about the bound, never collapsed. Ranking is
            // over EVENTS and answers in SESSIONS, and the exact re-check runs
            // after both, so "there is more" and "this is short" have different
            // causes with different remedies. None of them is an error and the
            // sessions returned are a true ranking prefix in every case.
            //
            //   truncated      more ranked SESSIONS existed than `limit`
            //                  returned. Raising `limit` returns more.
            //   hits_truncated the ranking filled its event-hit budget, so
            //                  matching events existed that it never examined.
            //                  Raising `limit` widens that window but promises
            //                  no additional sessions.
            //   incomplete     the ranking's bounded candidate window was
            //                  exhausted before the answer could be filled
            //                  (issue #597 §1.6). Raising `limit` cannot help.
            //   dropped        ranked sessions were removed by the exact
            //                  post-ranking re-check (scope, harness/source,
            //                  tombstone) and nothing refilled them, so this
            //                  answer is a strict subset of what was ranked.
            "truncated": results.truncated,
            "hits_truncated": results.hits_truncated,
            "incomplete": results.incomplete,
            "dropped": results.dropped,
        }),
        StatusCode::OK,
    )
}

fn bad_request(message: String) -> Response {
    json_response(
        json!({"ok": false, "error": message}),
        StatusCode::BAD_REQUEST,
    )
}

/// A refused continuation token, from this handler's own envelope or from the
/// repository, carries the same `400` + `invalid_cursor` pair. The caller
/// recovers by restarting the feed, and reporting it as an error rather than a
/// short page is what stops a stale cursor from presenting as a gap.
fn invalid_cursor_response(message: String) -> Response {
    json_response(
        json!({"ok": false, "error": message, "code": "invalid_cursor"}),
        StatusCode::BAD_REQUEST,
    )
}

/// Ceiling on a monitor continuation token, enforced when minting and again
/// when decoding. Tokens are this server's own mints, so an oversized one is
/// fabricated, and parsing it is unbounded work on unvalidated input. Minting
/// stays under the cap by dropping optional carried state (see
/// [`encode_session_page_cursor`]), which keeps token size O(1) in transcript
/// and session-header size.
const MONITOR_CURSOR_MAX_CHARS: usize = 4096;

/// The mint-side half of [`MONITOR_CURSOR_MAX_CHARS`]. Every token this service
/// hands back passes through here, so the cap cannot be asymmetric: a token a
/// client receives is always one [`decode_monitor_cursor`] accepts. Failing the
/// mint is the only correct outcome — returning the token anyway hands out a
/// continuation that cannot be redeemed, and dropping it silently would present
/// a partial feed as complete.
fn checked_monitor_cursor(token: String) -> Result<String, String> {
    if token.len() > MONITOR_CURSOR_MAX_CHARS {
        return Err(format!(
            "minted cursor exceeds the {MONITOR_CURSOR_MAX_CHARS} character cursor limit"
        ));
    }
    Ok(token)
}

fn decode_monitor_cursor<T: DeserializeOwned>(token: &str) -> Result<T, String> {
    if token.len() > MONITOR_CURSOR_MAX_CHARS {
        return Err(format!(
            "cursor must be at most {MONITOR_CURSOR_MAX_CHARS} characters"
        ));
    }
    let raw = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|error| format!("invalid base64 cursor: {error}"))?;
    serde_json::from_slice(&raw).map_err(|error| format!("invalid cursor payload: {error}"))
}

/// A present-but-blank filter value is a cleared filter, not a filter for the
/// empty string.
fn optional_filter_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// A present `cursor` must carry a token. An empty one is a client bug that
/// would otherwise silently restart the feed from page 1.
fn optional_cursor(value: Option<&str>) -> Result<Option<String>, String> {
    match value {
        None => Ok(None),
        Some(token) if token.trim().is_empty() => {
            Err("cursor must be a non-empty string when provided".to_string())
        }
        Some(token) => Ok(Some(token.to_string())),
    }
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

/// The inverse of [`resolve_session_lookback`]: the `since` value a resolved
/// lookback is named by on the wire.
fn session_lookback_key(lookback: SessionLookback) -> &'static str {
    match lookback {
        SessionLookback::OneHour => "1h",
        SessionLookback::SixHours => "6h",
        SessionLookback::TwentyFourHours => "24h",
        SessionLookback::SevenDays => "7d",
        SessionLookback::ThirtyDays => "30d",
        SessionLookback::NinetyDays => "90d",
        SessionLookback::All => "all",
    }
}

/// The `[start, end)` millisecond window one session-feed page was computed
/// under, and the part of the repository filter this route derives rather than
/// receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct SessionWindow {
    start_unix_ms: i64,
    end_unix_ms: i64,
}

/// `since` is a lookback from now; `all` drops the lower bound. The upper bound
/// is exclusive and the repository requires `start < end`, so a zero-width
/// window at the epoch is not representable.
fn resolve_session_window(lookback: SessionLookback, now_ms: i64) -> SessionWindow {
    SessionWindow {
        start_unix_ms: match lookback.window_seconds() {
            Some(seconds) => now_ms.saturating_sub(i64::from(seconds) * 1_000).max(0),
            None => 0,
        },
        end_unix_ms: now_ms.saturating_add(1),
    }
}

/// Wire version of the monitor's session-feed continuation token. A bump
/// invalidates outstanding tokens rather than reinterpreting them.
const SESSIONS_CURSOR_VERSION: u8 = 1;

/// The monitor's envelope around a repository `list_sessions` token.
///
/// The repository binds its token to the filter that minted it, and this route
/// derives the filter's time window from the wall clock. The window therefore
/// travels inside the token and is replayed verbatim on every continuation, so
/// page 2 presents exactly the filter page 1 was computed under. Without it a
/// freshly minted cursor could not be redeemed at all: the clock moves between
/// requests, and the repository refuses a cursor whose filter moved.
///
/// The remaining filter dimensions (`mode`, `harness`, `source`, `sort`) are
/// client-supplied and are NOT pinned here — the repository already refuses a
/// token presented under different ones, with a message naming the mismatch.
#[derive(Serialize, Deserialize)]
struct SessionsCursor {
    version: u8,
    /// The lookback the window was resolved from. A continuation that asks for
    /// a different one is refused rather than silently served the pinned
    /// window, which would make the `since` control look inert.
    since: SessionLookback,
    #[serde(flatten)]
    window: SessionWindow,
    /// The repository's own token, carried through untouched.
    inner: String,
}

fn encode_sessions_cursor(
    lookback: SessionLookback,
    window: SessionWindow,
    inner: String,
) -> Result<String, String> {
    let cursor = SessionsCursor {
        version: SESSIONS_CURSOR_VERSION,
        since: lookback,
        window,
        inner,
    };
    // Nothing in this envelope is droppable — the repository token is the
    // continuation — so an oversized mint is a hard failure rather than a
    // shrink. It is unreachable for a well-formed repository token; the check
    // exists so a repository that grew its own token cannot silently start
    // issuing unredeemable monitor cursors.
    serde_json::to_vec(&cursor)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|error| format!("failed to encode sessions cursor: {error}"))
        .and_then(checked_monitor_cursor)
}

fn decode_sessions_cursor(
    token: &str,
    lookback: SessionLookback,
) -> Result<SessionsCursor, String> {
    let cursor: SessionsCursor = decode_monitor_cursor(token)?;
    if cursor.version != SESSIONS_CURSOR_VERSION {
        return Err(format!(
            "unsupported sessions cursor version {}",
            cursor.version
        ));
    }
    if cursor.since != lookback {
        return Err(format!(
            "cursor was minted for since={}; restart the feed to change the window",
            session_lookback_key(cursor.since)
        ));
    }
    // The window is the one part of the repository filter this route takes
    // from the token rather than from the request, so it is unvalidated client
    // input on the way into `McpSessionListFilter`. Two checks, both required.
    //
    // The range must be a valid half-open interval: the repository rejects
    // `start >= end` as an invalid argument, which is a 503 here, not the 400
    // a refused token is documented to produce.
    if cursor.window.start_unix_ms < 0 || cursor.window.end_unix_ms <= cursor.window.start_unix_ms {
        return Err("cursor window is not a valid time range".to_string());
    }
    // And the range must be the one `since` names. `resolve_session_window`
    // derives the lower bound entirely from the lookback and the upper bound,
    // so re-deriving it from the token's own `end` reproduces any window this
    // route ever minted and no other. Without this, tampering the lower bound
    // widens the query past what `since` permits — the `since` check above
    // would still pass, because it only compares the label.
    if cursor.window
        != resolve_session_window(cursor.since, cursor.window.end_unix_ms.saturating_sub(1))
    {
        return Err(format!(
            "cursor window does not match since={}",
            session_lookback_key(cursor.since)
        ));
    }
    Ok(cursor)
}

/// One session SUMMARY. No `turns` key, and nothing under it: the feed carries
/// navigation scalars and labels only, so its response size stays flat as
/// transcripts grow (issue-599 §5.3).
///
/// `displayLabel` comes from the shared ladder MCP renders, so the same session
/// reads identically on both surfaces.
fn monitor_session_json(session: &McpSessionListItem, now_ms: i64) -> Value {
    // A recorded terminal event is authoritative; otherwise recency stands in
    // for it. Same rule as before the cutover, so status keeps its meaning.
    let status = if session.completed
        || now_ms.saturating_sub(session.last_event_unix_ms) >= SESSION_ACTIVE_WINDOW_MS
    {
        "completed"
    } else {
        "active"
    };

    json!({
        "id": session.session_id,
        "title": session.title.as_deref(),
        "displayLabel": session_display_label(session),
        "harness": session.harness.as_deref(),
        "source": session.source.as_deref(),
        "inferenceProvider": session.inference_provider.as_deref(),
        "mode": session.mode.as_str(),
        "startedAt": session.first_event_unix_ms,
        "endedAt": session.last_event_unix_ms,
        "status": status,
        "turnCount": session.total_turns,
        "eventCount": session.total_events,
        "toolCallCount": session.tool_calls,
        "sessionSlug": session.session_slug.as_deref(),
        "sessionSummary": session.session_summary.as_deref(),
    })
}

/// The lazy transcript load (issue-599 WI-05): one bounded page of a session's
/// turns, read through [`moraine_conversations::ConversationRepository::canonical_open_session_page`]
/// — the same canonical `open(session)` reader MCP's `open` tool uses. This
/// handler performs no other repository read.
///
/// `Ok(None)` covers both "no such session" and "outside this backend's
/// configured scope" and answers `404` for either, so the route cannot be used
/// to probe for sessions the caller may not read.
///
/// The canonical reader is gated on `open_v2` readiness — the same authority
/// MCP `open` and the directory listing path consult — because it reads
/// `mcp_session_directory` / `mcp_event_navigation` directly and a backend
/// whose backfill or overlap audit has not published them would answer from an
/// incomplete store. A not-ready backend gets `503 canonical_reader_unavailable`
/// rather than a plausible-looking partial transcript.
async fn api_session_page(
    Path(session_id): Path<String>,
    Query(params): Query<SessionPageQuery>,
    Extension(backend): Extension<Arc<BackendRepository>>,
) -> Response {
    if !backend.repository().canonical_reader_ready().await {
        return json_response(
            json!({
                "ok": false,
                "error": "session transcripts are unavailable until this backend publishes its canonical read indexes",
                "code": "canonical_reader_unavailable",
            }),
            StatusCode::SERVICE_UNAVAILABLE,
        );
    }

    let after = match params.cursor.as_deref() {
        None => None,
        Some(token) => match decode_session_page_cursor(token, &session_id) {
            Ok(continuation) => Some(continuation),
            Err(message) => return invalid_cursor_response(message),
        },
    };
    let limit = params.limit.unwrap_or(50).clamp(1, 200) as u16;

    let outcome = backend
        .repository()
        .canonical_open_session_page(&session_id, limit, after)
        .await;
    let page = match outcome {
        Ok(Some(CanonicalReadOutcome::Page(page))) => page,
        Ok(Some(CanonicalReadOutcome::Reopen)) => {
            // The pinned view no longer describes this session (a replay
            // flipped its generations). Reopening from page 1 is the caller's
            // recovery; it is not a failure of this request.
            return json_response(
                json!({"ok": true, "read_model": "live", "reopen": true}),
                StatusCode::OK,
            );
        }
        Ok(None) => {
            return json_response(
                json!({"ok": false, "error": "session not found"}),
                StatusCode::NOT_FOUND,
            );
        }
        Err(error) => {
            return repo_error_response(format!("session page query failed: {error}"), &error);
        }
    };

    let next_cursor = match page.continuation.as_ref() {
        None => None,
        Some(continuation) => match encode_session_page_cursor(&session_id, continuation) {
            Ok(token) => Some(token),
            Err(message) => {
                return json_response(
                    json!({"ok": false, "error": message}),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        },
    };

    json_response(
        json!({
            "ok": true,
            "read_model": "live",
            "limit": limit,
            "session": monitor_session_page_json(&page.session),
            "has_more": next_cursor.is_some(),
            "next_cursor": next_cursor,
        }),
        StatusCode::OK,
    )
}

/// Wire version of the monitor's session-page continuation token. The token is
/// this route's own envelope around the repository continuation — the monitor
/// neither accepts nor mints MCP `open` cursors, and a version bump invalidates
/// outstanding tokens rather than reinterpreting them.
const SESSION_PAGE_CURSOR_VERSION: u8 = 1;

#[derive(Serialize, Deserialize)]
struct SessionPageCursor {
    version: u8,
    session_id: String,
    continuation: CanonicalContinuation,
}

/// Mint a continuation token for `session_id`, bounded by
/// [`MONITOR_CURSOR_MAX_CHARS`].
///
/// `CanonicalContinuation::session_carry` is a JSON-encoded session header, so
/// carrying it verbatim would size the token by header content rather than by
/// the anchor. It is dropped whenever the encoded token would exceed the cap;
/// the reader then recomputes the header on the next page (design §6
/// carry-drop, the same trade the MCP `open` cursor makes). Everything that
/// remains is fixed-shape, so token size is O(1) in transcript and header size.
fn encode_session_page_cursor(
    session_id: &str,
    continuation: &CanonicalContinuation,
) -> Result<String, String> {
    let mut cursor = SessionPageCursor {
        version: SESSION_PAGE_CURSOR_VERSION,
        session_id: session_id.to_string(),
        continuation: continuation.clone(),
    };
    let token = encode_session_page_cursor_token(&cursor)?;
    if token.len() <= MONITOR_CURSOR_MAX_CHARS || cursor.continuation.session_carry.is_none() {
        return checked_monitor_cursor(token);
    }
    cursor.continuation.session_carry = None;
    // Re-checked after the drop: the carry is the only oversized part, but
    // "dropped the carry" is not the same claim as "now fits", and a token that
    // still exceeds the cap must not be handed back.
    encode_session_page_cursor_token(&cursor).and_then(checked_monitor_cursor)
}

fn encode_session_page_cursor_token(cursor: &SessionPageCursor) -> Result<String, String> {
    serde_json::to_vec(cursor)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|error| format!("failed to encode session page cursor: {error}"))
}

/// Decode a continuation token for `session_id`.
///
/// The token carries the session it was minted for and is refused for any
/// other: the anchor it holds is meaningless outside its own traversal, so
/// honoring it on another path would serve an arbitrary slice of that session
/// rather than its first page.
fn decode_session_page_cursor(
    token: &str,
    session_id: &str,
) -> Result<CanonicalContinuation, String> {
    let token = optional_cursor(Some(token))?.unwrap_or_default();
    let cursor: SessionPageCursor = decode_monitor_cursor(&token)?;
    if cursor.version != SESSION_PAGE_CURSOR_VERSION {
        return Err(format!(
            "unsupported session page cursor version {}",
            cursor.version
        ));
    }
    if cursor.session_id != session_id {
        return Err("cursor was minted for a different session".to_string());
    }
    Ok(cursor.continuation)
}

/// The opened session header plus this page's turns. Turn bodies are the
/// summaries and references the canonical reader returns.
fn monitor_session_page_json(session: &McpSessionOpen) -> Value {
    json!({
        "id": session.metadata.session_id,
        "title": session.title.as_deref(),
        "harness": session.harness.as_deref(),
        "source": session.source.as_deref(),
        "inferenceProvider": session.inference_provider.as_deref(),
        "mode": session.metadata.mode.as_str(),
        "startedAt": session.metadata.first_event_unix_ms,
        "endedAt": session.metadata.last_event_unix_ms,
        "completed": session.completed,
        "turnCount": session.metadata.total_turns,
        "eventCount": session.metadata.total_events,
        "sessionSlug": session.session_slug.as_deref(),
        "sessionSummary": session.session_summary.as_deref(),
        "turns": session
            .turns
            .iter()
            .map(monitor_session_turn_json)
            .collect::<Vec<_>>(),
    })
}

fn monitor_session_turn_json(turn: &McpTurnCompact) -> Value {
    json!({
        "turnSeq": turn.metadata.turn_seq,
        "turnId": turn.metadata.turn_id,
        "startedAt": turn.metadata.started_at_unix_ms,
        "endedAt": turn.metadata.ended_at_unix_ms,
        "eventCount": turn.metadata.total_events,
        "userMessages": turn.metadata.user_messages,
        "assistantMessages": turn.metadata.assistant_messages,
        "toolCalls": turn.metadata.tool_calls,
        "toolResults": turn.metadata.tool_results,
        "reasoningItems": turn.metadata.reasoning_items,
        "userInput": turn.user_input_summary.as_deref(),
        "finalResponse": turn.final_response_summary.as_deref(),
        "toolsCalled": turn.tools_called,
        "completed": turn.completed,
    })
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
                    "read_model": "audit",
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
        Err(error) => repo_error_response(format!("unable to read table {table}: {error}"), &error),
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
        AnalyticsWindow, CanonicalReadAnchor, CanonicalSessionPage, CanonicalSessionSignals,
        ConversationMode, ConversationRepository, InMemoryConversationRepository,
        InMemoryConversationResponses, IngestHeartbeat, McpTurnCompact, Page, RepoConfig,
        SessionMetadata, SessionSearchResults, StoreDiagnostics, TableColumn, TablePreview,
        TableSummary, TurnSummary, WebSearchEvent,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Duration;
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
                read_limits: Arc::new(MonitorReadLimits::new(default_query_budgets())),
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
            publication: StoreProbe::Available(PublicationDiagnostics::default()),
            core_index: StoreProbe::Available(CoreIndexHealth {
                core_indexes_ready: true,
                open_v2_ready: true,
                open_v2_provenance: Some("auto-local".to_string()),
                backfill_cursor_age_ms: Some(4_200),
                audit_outcome: None,
            }),
            storage: StoreProbe::Available(sample_storage_report()),
        }
    }

    /// A minimal but non-degenerate storage report: one bucket-1 table, one
    /// bucket-3 table, real disk numbers, and the default (non-destructive)
    /// policy. Enough for the `/api/v1/health` shape assertions.
    fn sample_storage_report() -> StorageReport {
        let tables = vec![
            moraine_conversations::StorageTableReport {
                name: "events".to_string(),
                class: Some(moraine_conversations::TableClass::CanonicalHistory),
                rows: 1_990_776,
                compressed_bytes: 4_787_723_965,
                uncompressed_bytes: 11_420_351_515,
                active_parts: 24,
                oldest_retained: Some("2026-02-20T14:16:45Z".to_string()),
            },
            moraine_conversations::StorageTableReport {
                name: "mcp_open_turns".to_string(),
                class: Some(moraine_conversations::TableClass::Derived),
                rows: 234_694,
                compressed_bytes: 14_356_000_000,
                uncompressed_bytes: 40_000_000_000,
                active_parts: 303,
                oldest_retained: None,
            },
        ];
        StorageReport {
            buckets: moraine_conversations::fold_buckets(&tables),
            tables,
            disk: Some(moraine_conversations::StorageDiskReport {
                free_bytes: 11_780_276_224,
                total_bytes: 994_662_584_320,
            }),
            policy: moraine_conversations::retention_policy_entries(
                &moraine_config::RetentionConfig::default(),
            ),
            notes: Vec::new(),
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

    fn sample_session() -> McpSessionListItem {
        McpSessionListItem {
            session_id: "session-1".to_string(),
            first_event_time: "2026-02-16T12:00:00.000Z".to_string(),
            first_event_unix_ms: 1_771_243_200_000,
            last_event_time: "2026-02-16T12:00:03.900Z".to_string(),
            last_event_unix_ms: 1_771_243_203_900,
            total_turns: 1,
            total_events: 7,
            mode: ConversationMode::ToolCalling,
            completed: false,
            title: Some("Inspect the repository".to_string()),
            source: Some("ci-codex".to_string()),
            harness: Some("codex".to_string()),
            inference_provider: Some("openai".to_string()),
            session_slug: Some("inspect-repo".to_string()),
            session_summary: Some("Repository inspection".to_string()),
            tool_calls: 1,
        }
    }

    /// A page of `count` sessions descending by `last_event_unix_ms`, ids
    /// `feed-00..`, so a paging test can assert exactly which slice it got.
    fn session_feed(start_index: usize, count: usize) -> Vec<McpSessionListItem> {
        (0..count)
            .map(|offset| {
                let index = start_index + offset;
                McpSessionListItem {
                    session_id: format!("feed-{index:02}"),
                    last_event_unix_ms: 1_771_243_203_900 - (index as i64) * 1_000,
                    ..sample_session()
                }
            })
            .collect()
    }

    fn session_ids(payload: &Value) -> Vec<String> {
        payload["sessions"]
            .as_array()
            .expect("sessions array")
            .iter()
            .map(|session| session["id"].as_str().expect("session id").to_string())
            .collect()
    }

    fn sessions_query() -> SessionsQuery {
        SessionsQuery {
            limit: None,
            since: None,
            cursor: None,
            harness: None,
            source: None,
            mode: None,
            sort: None,
        }
    }

    fn successful_responses() -> InMemoryConversationResponses {
        InMemoryConversationResponses {
            list_mcp_sessions: Some(Ok(Page {
                items: vec![sample_session()],
                next_cursor: None,
            })),
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
        assert_eq!(health["publication"]["available"], json!(true));
        assert_eq!(health["publication"]["healthy"], json!(true));
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
        assert_eq!(status["publication"]["healthy"], json!(true));
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
        assert_eq!(tables["read_model"], json!("audit"));
        assert_eq!(tables["tables"][0]["is_temporary"], json!(0));

        let response = api_web_searches(
            Query(LimitQuery { limit: Some(2_500) }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let web_searches = response_json(response).await;
        assert_eq!(web_searches["read_model"], json!("live"));
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
        assert_eq!(analytics["read_model"], json!("live"));
        assert_eq!(analytics["range"]["key"], json!("7d"));
        assert_eq!(analytics["range"]["label"], json!("Last 7d"));
        assert_eq!(analytics["series"]["tokens"][0]["tokens"], json!(4));

        let response = api_sessions(
            Query(SessionsQuery {
                limit: Some(0),
                since: Some("not-a-window".to_string()),
                ..sessions_query()
            }),
            Extension(backend.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let sessions = response_json(response).await;
        assert_eq!(sessions["read_model"], json!("live"));
        assert_eq!(sessions["limit"], json!(1));
        assert_eq!(sessions["has_more"], json!(false));
        assert_eq!(sessions["next_cursor"], Value::Null);
        let session = &sessions["sessions"][0];
        assert_eq!(session["id"], json!("session-1"));
        assert_eq!(session["endedAt"], json!(1_771_243_203_900_i64));
        assert_eq!(session["eventCount"], json!(7));
        assert_eq!(session["turnCount"], json!(1));
        assert_eq!(session["toolCallCount"], json!(1));
        assert_eq!(session["harness"], json!("codex"));
        assert_eq!(session["source"], json!("ci-codex"));
        assert_eq!(session["inferenceProvider"], json!("openai"));
        assert_eq!(session["mode"], json!("tool_calling"));
        assert_eq!(session["displayLabel"], json!("Inspect the repository"));

        let response = api_table_rows(
            Path("events".to_string()),
            Query(LimitQuery { limit: Some(999) }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let preview = response_json(response).await;
        assert_eq!(preview["read_model"], json!("audit"));
        assert_eq!(preview["limit"], json!(500));
        assert_eq!(preview["schema"][0]["type"], json!("String"));

        let calls = repository.calls();
        assert_eq!(calls.read_store_health, 2);
        assert_eq!(calls.read_store_diagnostics, 0);
        assert_eq!(calls.latest_ingest_heartbeat, 2);
        assert_eq!(calls.list_table_summaries, 2);
        assert_eq!(calls.list_web_searches, vec![1_000]);
        assert_eq!(calls.analytics_series, vec![AnalyticsRange::SevenDays]);
        // The feed reads the shared discovery operation, never the projector.
        assert!(calls.list_session_analytics.is_empty());
        let (filter, page) = calls
            .list_mcp_sessions
            .first()
            .expect("session feed reads list_mcp_sessions");
        assert_eq!(page.limit, 1);
        assert_eq!(page.cursor, None);
        assert_eq!(filter.sort, ConversationListSort::Desc);
        assert_eq!(filter.mode, None);
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
            list_mcp_sessions: Some(Err(RepoError::backend("sessions unavailable"))),
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

        let sessions = api_sessions(Query(sessions_query()), Extension(backend.clone())).await;
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

    // --- issue-599 WI-04: `/api/v1/sessions` is summaries + a cursor --------

    /// Every string anywhere in `value`, so an assertion about transcript
    /// content cannot be satisfied by checking only the keys it expects.
    fn all_strings(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(text) => out.push(text.clone()),
            Value::Array(items) => items.iter().for_each(|item| all_strings(item, out)),
            Value::Object(fields) => fields.values().for_each(|item| all_strings(item, out)),
            _ => {}
        }
    }

    fn all_keys(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::Array(items) => items.iter().for_each(|item| all_keys(item, out)),
            Value::Object(fields) => {
                for (key, item) in fields {
                    out.push(key.clone());
                    all_keys(item, out);
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn session_feed_carries_summaries_and_no_transcript_content() {
        let (backend, _) = fake_backend(successful_responses()).await;

        let response = api_sessions(Query(sessions_query()), Extension(backend)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;

        let mut keys = Vec::new();
        all_keys(&payload, &mut keys);
        for forbidden in ["turns", "steps", "text", "events", "payload_json", "models"] {
            assert!(
                !keys.iter().any(|key| key == forbidden),
                "session feed must not carry {forbidden:?}: {payload}"
            );
        }

        let session = &payload["sessions"][0];
        let mut fields = session
            .as_object()
            .expect("session object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        fields.sort();
        assert_eq!(
            fields,
            vec![
                "displayLabel",
                "endedAt",
                "eventCount",
                "harness",
                "id",
                "inferenceProvider",
                "mode",
                "sessionSlug",
                "sessionSummary",
                "source",
                "startedAt",
                "status",
                "title",
                "toolCallCount",
                "turnCount",
            ]
        );
    }

    #[tokio::test]
    async fn session_summary_never_carries_the_body_of_a_message() {
        // A session whose only content-bearing fields are its title and summary
        // still cannot leak an assistant reply, because the feed reads no
        // message text at all.
        let mut session = sample_session();
        session.title = Some("Inspect the repository".to_string());
        session.session_summary = Some("Repository inspection".to_string());
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions: Some(Ok(Page {
                items: vec![session],
                next_cursor: None,
            })),
            ..Default::default()
        })
        .await;

        let payload =
            response_json(api_sessions(Query(sessions_query()), Extension(backend)).await).await;
        let mut strings = Vec::new();
        all_strings(&payload, &mut strings);
        assert!(
            strings.iter().all(|text| text.len() <= 200),
            "no feed string may be transcript-sized: {strings:?}"
        );
    }

    #[tokio::test]
    async fn has_more_is_true_exactly_when_the_repository_returns_a_cursor() {
        for next_cursor in [None, Some("cursor-page-2".to_string())] {
            let (backend, _) = fake_backend(InMemoryConversationResponses {
                list_mcp_sessions: Some(Ok(Page {
                    items: vec![sample_session()],
                    next_cursor: next_cursor.clone(),
                })),
                ..Default::default()
            })
            .await;

            let payload =
                response_json(api_sessions(Query(sessions_query()), Extension(backend)).await)
                    .await;
            assert_eq!(
                payload["has_more"],
                json!(next_cursor.is_some()),
                "has_more must track next_cursor, got {payload}"
            );
            // The wire token is this route's envelope, not the repository's
            // token; it must carry that token through unchanged.
            assert_eq!(
                payload["next_cursor"]
                    .as_str()
                    .map(
                        |token| decode_sessions_cursor(token, SessionLookback::ThirtyDays)
                            .expect("minted cursor decodes")
                            .inner
                    ),
                next_cursor,
            );
        }
    }

    #[tokio::test]
    async fn an_empty_page_with_a_cursor_still_reports_more() {
        // The repository's legal "keep going" signal: the candidate budget ran
        // out before anything survived. Rendering that as "no sessions" would
        // present a subset as complete.
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions: Some(Ok(Page {
                items: Vec::new(),
                next_cursor: Some("keep-going".to_string()),
            })),
            ..Default::default()
        })
        .await;

        let payload =
            response_json(api_sessions(Query(sessions_query()), Extension(backend)).await).await;
        assert_eq!(payload["sessions"], json!([]));
        assert_eq!(payload["has_more"], json!(true));
        assert_eq!(
            decode_sessions_cursor(
                payload["next_cursor"].as_str().expect("keep-going cursor"),
                SessionLookback::ThirtyDays,
            )
            .expect("minted cursor decodes")
            .inner,
            "keep-going",
        );
    }

    #[tokio::test]
    async fn session_cursor_round_trips_to_page_two_with_no_overlap_and_no_gap() {
        // A static six-session corpus split into two pages of three. Page 2 is
        // reachable only by presenting page 1's cursor, so a handler that
        // dropped it would be served page 1 again and overlap.
        //
        // The fake binds each token it mints to that request's filter, exactly
        // as the repository does, so this also fails when page 2 presents a
        // window the handler re-derived from the clock instead of replaying the
        // one page 1 was computed under.
        let page_one = session_feed(0, 3);
        let page_two = session_feed(3, 3);
        let corpus: Vec<String> = page_one
            .iter()
            .chain(page_two.iter())
            .map(|session| session.session_id.clone())
            .collect();
        let (backend, repository) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions_by_cursor: BTreeMap::from([
                (
                    String::new(),
                    Ok(Page {
                        items: page_one,
                        next_cursor: Some("page-2".to_string()),
                    }),
                ),
                (
                    "page-2".to_string(),
                    Ok(Page {
                        items: page_two,
                        next_cursor: None,
                    }),
                ),
            ]),
            ..Default::default()
        })
        .await;

        let first = response_json(
            api_sessions(
                Query(SessionsQuery {
                    limit: Some(3),
                    ..sessions_query()
                }),
                Extension(backend.clone()),
            )
            .await,
        )
        .await;
        assert_eq!(first["has_more"], json!(true));
        let cursor = first["next_cursor"]
            .as_str()
            .expect("page 1 mints a cursor")
            .to_string();

        // The feed window is derived from the wall clock. Let the clock move,
        // which is the production condition: page 2 arrives at a later instant
        // than the page-1 mint.
        tokio::time::sleep(Duration::from_millis(2)).await;

        let second_response = api_sessions(
            Query(SessionsQuery {
                limit: Some(3),
                cursor: Some(cursor.clone()),
                ..sessions_query()
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(
            second_response.status(),
            StatusCode::OK,
            "a freshly minted cursor must be redeemable"
        );
        let second = response_json(second_response).await;
        assert_eq!(second["has_more"], json!(false));
        assert_eq!(
            second["window"], first["window"],
            "a continuation reports the window it replayed"
        );

        let first_ids = session_ids(&first);
        let second_ids = session_ids(&second);
        assert!(
            first_ids.iter().all(|id| !second_ids.contains(id)),
            "pages must not overlap: {first_ids:?} then {second_ids:?}"
        );
        let traversed: Vec<String> = first_ids.into_iter().chain(second_ids).collect();
        assert_eq!(
            traversed, corpus,
            "traversal must cover the corpus in order"
        );

        // The repository's own token reached it verbatim; a dropped or
        // rewritten cursor is what produces the gap this test exists to catch.
        let calls = repository.calls();
        assert_eq!(calls.list_mcp_sessions.len(), 2);
        assert_eq!(calls.list_mcp_sessions[0].1.cursor, None);
        assert_eq!(
            calls.list_mcp_sessions[1].1.cursor.as_deref(),
            Some("page-2")
        );
        // Page 2 presented the identical window, which is what makes the token
        // redeemable at all.
        assert_eq!(
            (
                calls.list_mcp_sessions[1].0.start_unix_ms,
                calls.list_mcp_sessions[1].0.end_unix_ms
            ),
            (
                calls.list_mcp_sessions[0].0.start_unix_ms,
                calls.list_mcp_sessions[0].0.end_unix_ms
            ),
        );
    }

    #[tokio::test]
    async fn a_continuation_replays_the_pinned_window_rather_than_the_current_clock() {
        // Clock-independent statement of the same contract: the window the
        // repository sees on a continuation comes from the token, so a page
        // minted at any instant resolves to the same filter when it is redeemed.
        let pinned = SessionWindow {
            start_unix_ms: 1_768_651_203_900,
            end_unix_ms: 1_771_243_203_901,
        };
        let token =
            encode_sessions_cursor(SessionLookback::ThirtyDays, pinned, "page-2".to_string())
                .expect("cursor encodes");
        let (backend, repository) = fake_backend(successful_responses()).await;

        let response = api_sessions(
            Query(SessionsQuery {
                cursor: Some(token),
                ..sessions_query()
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["window"]["start"], json!(pinned.start_unix_ms));
        assert_eq!(payload["window"]["end"], json!(pinned.end_unix_ms));

        let calls = repository.calls();
        let (filter, page) = calls.list_mcp_sessions.first().expect("one page read");
        assert_eq!(filter.start_unix_ms, pinned.start_unix_ms);
        assert_eq!(filter.end_unix_ms, pinned.end_unix_ms);
        assert_eq!(page.cursor.as_deref(), Some("page-2"));
    }

    #[tokio::test]
    async fn a_continuation_may_not_silently_change_the_window_it_pinned() {
        // Serving the pinned window under a different `since` would make the
        // control look inert; the caller is told to restart the feed instead.
        let token = encode_sessions_cursor(
            SessionLookback::ThirtyDays,
            resolve_session_window(SessionLookback::ThirtyDays, unix_now_ms()),
            "page-2".to_string(),
        )
        .expect("cursor encodes");
        let (backend, repository) = fake_backend(successful_responses()).await;

        let response = api_sessions(
            Query(SessionsQuery {
                since: Some("6h".to_string()),
                cursor: Some(token),
                ..sessions_query()
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert_eq!(payload["code"], json!("invalid_cursor"));
        assert!(payload["error"]
            .as_str()
            .is_some_and(|message| message.contains("since=30d")));
        assert!(
            repository.calls().list_mcp_sessions.is_empty(),
            "a refused cursor must not reach the repository"
        );
    }

    #[tokio::test]
    async fn a_continuation_whose_window_was_tampered_with_is_refused_as_an_invalid_cursor() {
        // The window is the one filter dimension this route takes from the
        // token instead of the request, so it is client input on the way into
        // `McpSessionListFilter`. A widened lower bound queries outside the
        // `since` the token still names, and a reversed range reaches the
        // repository as an invalid argument — which is a 503, not the 400 a
        // refused token is documented to produce.
        let honest = resolve_session_window(SessionLookback::ThirtyDays, unix_now_ms());
        let tampered = [
            SessionWindow {
                start_unix_ms: 0,
                ..honest
            },
            SessionWindow {
                start_unix_ms: honest.end_unix_ms,
                end_unix_ms: honest.start_unix_ms,
            },
            SessionWindow {
                start_unix_ms: -1,
                ..honest
            },
        ];

        for window in tampered {
            let token =
                encode_sessions_cursor(SessionLookback::ThirtyDays, window, "page-2".to_string())
                    .expect("cursor encodes");
            let (backend, repository) = fake_backend(successful_responses()).await;

            let response = api_sessions(
                Query(SessionsQuery {
                    cursor: Some(token),
                    ..sessions_query()
                }),
                Extension(backend),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "tampered window {window:?} must be a client error"
            );
            assert_eq!(
                response_json(response).await["code"],
                json!("invalid_cursor"),
                "tampered window {window:?} must be classified as a refused cursor"
            );
            assert!(
                repository.calls().list_mcp_sessions.is_empty(),
                "a tampered window must not reach the repository filter"
            );
        }
    }

    #[tokio::test]
    async fn a_sessions_cursor_this_service_would_refuse_is_never_minted() {
        // Mint and decode share one cap. A token longer than the decoder
        // accepts would be a continuation the very next request rejects,
        // stranding the caller mid-feed with no way forward.
        let window = resolve_session_window(SessionLookback::ThirtyDays, unix_now_ms());

        for inner_len in [
            0,
            16,
            MONITOR_CURSOR_MAX_CHARS / 2,
            MONITOR_CURSOR_MAX_CHARS,
            MONITOR_CURSOR_MAX_CHARS * 2,
        ] {
            let inner = "r".repeat(inner_len);
            let Ok(token) =
                encode_sessions_cursor(SessionLookback::ThirtyDays, window, inner.clone())
            else {
                continue;
            };
            assert_eq!(
                decode_sessions_cursor(&token, SessionLookback::ThirtyDays)
                    .unwrap_or_else(|error| panic!(
                        "a minted token must decode, inner {inner_len} chars: {error}"
                    ))
                    .inner,
                inner,
            );
        }

        // Not vacuous: the largest case above is genuinely refused at mint
        // rather than quietly fitting.
        let message = encode_sessions_cursor(
            SessionLookback::ThirtyDays,
            window,
            "r".repeat(MONITOR_CURSOR_MAX_CHARS * 2),
        )
        .expect_err("an oversized mint must fail");
        assert!(message.contains("cursor limit"), "got {message}");
    }

    #[tokio::test]
    async fn session_deadline_surfaces_as_a_budget_classified_error() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions: Some(Err(RepoError::deadline_exceeded(
                "query budget deadline expired (budget 15.000s)",
            ))),
            ..Default::default()
        })
        .await;

        let response = api_sessions(Query(sessions_query()), Extension(backend)).await;
        // 504 is this daemon's deadline status (issue #600 amendment A11); the
        // machine-readable code is what a client branches on.
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let payload = response_json(response).await;
        assert_eq!(payload["ok"], json!(false));
        assert_eq!(payload["code"], json!("deadline_exceeded"));

        let (backend, _) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions: Some(Err(RepoError::resource_exhausted(
                "read_rows allowance exhausted (budget 500000000)",
            ))),
            ..Default::default()
        })
        .await;
        let response = api_sessions(Query(sessions_query()), Extension(backend)).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response_json(response).await["code"],
            json!("resource_exhausted")
        );
    }

    #[tokio::test]
    async fn a_refused_cursor_is_a_recoverable_client_error_not_a_backend_failure() {
        // `list_mcp_sessions` refuses a token minted by the other read path. A
        // 503 would read as transient and invite a retry that pages a silent
        // gap; the caller must be told to restart the feed instead.
        let (backend, repository) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions: Some(Err(RepoError::invalid_cursor(
                "cursor was minted by a different list_sessions read path",
            ))),
            ..Default::default()
        })
        .await;

        // A well-formed monitor envelope, so the refusal under test is the
        // repository's and not this route's own decode.
        let token = encode_sessions_cursor(
            SessionLookback::ThirtyDays,
            resolve_session_window(SessionLookback::ThirtyDays, unix_now_ms()),
            "stale-repository-token".to_string(),
        )
        .expect("cursor encodes");
        let response = api_sessions(
            Query(SessionsQuery {
                cursor: Some(token),
                ..sessions_query()
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_json(response).await;
        assert_eq!(payload["ok"], json!(false));
        assert_eq!(payload["code"], json!("invalid_cursor"));
        assert_eq!(
            repository.calls().list_mcp_sessions.len(),
            1,
            "the repository is what refused this token"
        );
    }

    #[tokio::test]
    async fn a_malformed_or_oversized_cursor_is_refused_before_the_repository() {
        let (backend, repository) = fake_backend(successful_responses()).await;

        for token in [
            "not base64!!".to_string(),
            URL_SAFE_NO_PAD.encode(b"not json"),
            "A".repeat(MONITOR_CURSOR_MAX_CHARS + 1),
        ] {
            let response = api_sessions(
                Query(SessionsQuery {
                    cursor: Some(token),
                    ..sessions_query()
                }),
                Extension(backend.clone()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                response_json(response).await["code"],
                json!("invalid_cursor")
            );
        }
        assert!(repository.calls().list_mcp_sessions.is_empty());
    }

    #[tokio::test]
    async fn session_query_parameters_reach_the_shared_repository_filter() {
        let (backend, repository) = fake_backend(successful_responses()).await;

        let response = api_sessions(
            Query(SessionsQuery {
                limit: Some(200),
                since: Some("all".to_string()),
                cursor: None,
                harness: Some("  codex  ".to_string()),
                source: Some(String::new()),
                mode: Some("mcp_internal".to_string()),
                sort: Some("asc".to_string()),
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        // `all` drops the lower bound; the upper bound is exclusive of now.
        assert_eq!(payload["window"]["start"], json!(0));
        assert!(payload["window"]["end"].as_i64().expect("window end") > 0);
        // 200 is inside the route's clamp but above the backend's max_results,
        // so the reported limit is the one actually served.
        assert_eq!(payload["limit"], json!(RepoConfig::default().max_results));

        let calls = repository.calls();
        let (filter, page) = calls.list_mcp_sessions.first().expect("one page read");
        assert_eq!(page.limit, RepoConfig::default().max_results);
        assert_eq!(filter.start_unix_ms, 0);
        assert_eq!(filter.harness.as_deref(), Some("codex"));
        assert_eq!(filter.source_name, None, "a cleared filter is absent");
        assert_eq!(filter.mode, Some(ConversationMode::McpInternal));
        assert_eq!(filter.sort, ConversationListSort::Asc);
    }

    #[tokio::test]
    async fn since_window_bounds_the_filter_the_repository_receives() {
        let (backend, repository) = fake_backend(successful_responses()).await;

        let response = api_sessions(
            Query(SessionsQuery {
                since: Some("6h".to_string()),
                ..sessions_query()
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        let start = payload["window"]["start"].as_i64().expect("window start");
        let end = payload["window"]["end"].as_i64().expect("window end");
        assert_eq!(end - start, 6 * 60 * 60 * 1_000 + 1);

        let calls = repository.calls();
        let (filter, _) = calls.list_mcp_sessions.first().expect("one page read");
        assert_eq!(filter.start_unix_ms, start);
        assert_eq!(filter.end_unix_ms, end);
    }

    #[tokio::test]
    async fn unusable_session_filters_are_rejected_rather_than_widened() {
        let (backend, repository) = fake_backend(successful_responses()).await;

        for query in [
            SessionsQuery {
                mode: Some("not-a-mode".to_string()),
                ..sessions_query()
            },
            SessionsQuery {
                sort: Some("sideways".to_string()),
                ..sessions_query()
            },
            SessionsQuery {
                cursor: Some("   ".to_string()),
                ..sessions_query()
            },
        ] {
            let response = api_sessions(Query(query), Extension(backend.clone())).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(response_json(response).await["ok"], json!(false));
        }
        assert!(
            repository.calls().list_mcp_sessions.is_empty(),
            "a rejected request must not reach the repository"
        );
    }

    // --- issue-599 WI-05: `/api/v1/sessions/:id/page` lazy transcript ------

    fn sample_session_metadata() -> SessionMetadata {
        SessionMetadata {
            session_id: "session-1".to_string(),
            first_event_time: "2026-02-16T12:00:00.000Z".to_string(),
            first_event_unix_ms: 1_771_243_200_000,
            last_event_time: "2026-02-16T12:00:03.900Z".to_string(),
            last_event_unix_ms: 1_771_243_203_900,
            total_turns: 4,
            total_events: 12,
            user_messages: 4,
            assistant_messages: 4,
            tool_calls: 2,
            tool_results: 2,
            mode: ConversationMode::ToolCalling,
            first_event_uid: "uid-first".to_string(),
            last_event_uid: "uid-last".to_string(),
            last_actor_role: "assistant".to_string(),
        }
    }

    fn sample_turn(turn_seq: u32) -> McpTurnCompact {
        McpTurnCompact {
            metadata: TurnSummary {
                session_id: "session-1".to_string(),
                turn_seq,
                turn_id: format!("turn-{turn_seq}"),
                started_at: "2026-02-16T12:00:00.000Z".to_string(),
                started_at_unix_ms: 1_771_243_200_000,
                ended_at: "2026-02-16T12:00:03.900Z".to_string(),
                ended_at_unix_ms: 1_771_243_203_900,
                total_events: 3,
                user_messages: 1,
                assistant_messages: 1,
                tool_calls: 1,
                tool_results: 0,
                reasoning_items: 0,
            },
            user_input_summary: Some(format!("prompt {turn_seq}")),
            final_response_summary: Some(format!("reply {turn_seq}")),
            user_input_event: None,
            final_response_event: None,
            tools_called: vec!["Read".to_string()],
            normalized_event_types: vec!["message".to_string()],
            completed: true,
            terminal_event_uid: None,
            first_event: None,
            last_event: None,
        }
    }

    fn sample_continuation(after_turn_seq: u32) -> CanonicalContinuation {
        CanonicalContinuation {
            signals: CanonicalSessionSignals {
                pinned_revision: 7,
                heads_fingerprint: "fingerprint".to_string(),
                observed_sum: 12,
                min_bound_ms: 1_771_243_200_000,
                max_bound_ms: 1_771_243_203_900,
            },
            after: CanonicalReadAnchor {
                sort_time_ms: 1_771_243_203_900,
                source_host: "host-a".to_string(),
                source_file: "session.jsonl".to_string(),
                source_generation: 1,
                source_offset: 42,
                source_line_no: 9,
                event_uid: format!("uid-{after_turn_seq}"),
                event_order: u64::from(after_turn_seq) * 3,
                turn_seq: after_turn_seq,
                prefix_user_message_count: u64::from(after_turn_seq),
                event_ordinal: 3,
            },
            after_turn_seq,
            session_carry: None,
        }
    }

    fn sample_session_page(
        turns: Vec<McpTurnCompact>,
        continuation: Option<CanonicalContinuation>,
    ) -> CanonicalSessionPage {
        CanonicalSessionPage {
            session: McpSessionOpen {
                metadata: sample_session_metadata(),
                title: Some("Inspect the repository".to_string()),
                source: Some("ci-codex".to_string()),
                harness: Some("codex".to_string()),
                inference_provider: Some("openai".to_string()),
                session_slug: Some("inspect-repo".to_string()),
                session_summary: Some("Repository inspection".to_string()),
                turns,
                completed: false,
                terminal_event_uid: None,
                snapshot: None,
            },
            continuation,
        }
    }

    #[tokio::test]
    async fn session_page_lazily_loads_turns_through_the_canonical_open_reader() {
        let (backend, repository) = fake_backend(InMemoryConversationResponses {
            canonical_open_session_page_after_turn: BTreeMap::from([
                (
                    0,
                    Ok(Some(CanonicalReadOutcome::Page(sample_session_page(
                        vec![sample_turn(1), sample_turn(2)],
                        Some(sample_continuation(2)),
                    )))),
                ),
                (
                    2,
                    Ok(Some(CanonicalReadOutcome::Page(sample_session_page(
                        vec![sample_turn(3), sample_turn(4)],
                        None,
                    )))),
                ),
            ]),
            ..Default::default()
        })
        .await;

        let first = response_json(
            api_session_page(
                Path("session-1".to_string()),
                Query(SessionPageQuery {
                    limit: Some(2),
                    cursor: None,
                }),
                Extension(backend.clone()),
            )
            .await,
        )
        .await;
        assert_eq!(first["ok"], json!(true));
        assert_eq!(first["read_model"], json!("live"));
        assert_eq!(first["session"]["id"], json!("session-1"));
        assert_eq!(first["session"]["turnCount"], json!(4));
        let first_turns = first["session"]["turns"].as_array().expect("page 1 turns");
        assert_eq!(first_turns.len(), 2, "the page is bounded by limit");
        assert_eq!(first_turns[0]["turnSeq"], json!(1));
        assert_eq!(first_turns[0]["userInput"], json!("prompt 1"));
        assert_eq!(first["has_more"], json!(true));
        let cursor = first["next_cursor"]
            .as_str()
            .expect("a non-terminal page mints a cursor")
            .to_string();

        let second = response_json(
            api_session_page(
                Path("session-1".to_string()),
                Query(SessionPageQuery {
                    limit: Some(2),
                    cursor: Some(cursor),
                }),
                Extension(backend),
            )
            .await,
        )
        .await;
        let second_turns = second["session"]["turns"].as_array().expect("page 2 turns");
        assert_eq!(second_turns[0]["turnSeq"], json!(3));
        assert_eq!(second_turns[1]["turnSeq"], json!(4));
        assert_eq!(second["has_more"], json!(false));
        assert_eq!(second["next_cursor"], Value::Null);

        let calls = repository.calls();
        assert_eq!(calls.canonical_open_session_page.len(), 2);
        assert_eq!(calls.canonical_open_session_page[0].0, "session-1");
        assert_eq!(calls.canonical_open_session_page[0].1, 2);
        assert_eq!(calls.canonical_open_session_page[0].2, None);
        // Page 2 handed the repository back its own continuation, unmodified.
        assert_eq!(
            calls.canonical_open_session_page[1].2.as_ref(),
            Some(&sample_continuation(2))
        );
        // The canonical reader, and nothing else.
        assert!(calls.get_mcp_session.is_empty());
        assert!(calls.get_conversation.is_empty());
        assert!(calls.list_turns.is_empty());
        assert!(calls.list_session_events.is_empty());
        assert!(calls.list_mcp_sessions.is_empty());
        assert!(calls.list_session_analytics.is_empty());
    }

    #[tokio::test]
    async fn session_page_reopen_is_surfaced_as_a_reopen_signal() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            canonical_open_session_page: Some(Ok(Some(CanonicalReadOutcome::Reopen))),
            ..Default::default()
        })
        .await;

        let response = api_session_page(
            Path("session-1".to_string()),
            Query(SessionPageQuery {
                limit: None,
                cursor: None,
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["ok"], json!(true));
        assert_eq!(payload["reopen"], json!(true));
        assert!(payload.get("session").is_none());
    }

    #[tokio::test]
    async fn session_page_cursor_is_refused_for_another_session() {
        let (backend, repository) = fake_backend(InMemoryConversationResponses {
            canonical_open_session_page_after_turn: BTreeMap::from([(
                0,
                Ok(Some(CanonicalReadOutcome::Page(sample_session_page(
                    vec![sample_turn(1)],
                    Some(sample_continuation(1)),
                )))),
            )]),
            ..Default::default()
        })
        .await;

        let first = response_json(
            api_session_page(
                Path("session-1".to_string()),
                Query(SessionPageQuery {
                    limit: Some(1),
                    cursor: None,
                }),
                Extension(backend.clone()),
            )
            .await,
        )
        .await;
        let cursor = first["next_cursor"].as_str().expect("cursor").to_string();

        let before = repository.calls().canonical_open_session_page.len();
        for (session_id, token) in [
            ("session-2", cursor),
            ("session-1", "not-base64!!".to_string()),
            ("session-1", String::new()),
            ("session-1", "A".repeat(MONITOR_CURSOR_MAX_CHARS + 1)),
        ] {
            let response = api_session_page(
                Path(session_id.to_string()),
                Query(SessionPageQuery {
                    limit: None,
                    cursor: Some(token),
                }),
                Extension(backend.clone()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let payload = response_json(response).await;
            assert_eq!(payload["ok"], json!(false));
            assert_eq!(payload["code"], json!("invalid_cursor"));
        }
        assert_eq!(
            repository.calls().canonical_open_session_page.len(),
            before,
            "a refused cursor must not reach the reader"
        );
    }

    #[tokio::test]
    async fn session_page_cursor_size_is_bounded_by_the_anchor_not_the_session_header() {
        // `session_carry` is a JSON session header, so a token that embedded it
        // verbatim would grow with header content. Two continuations whose only
        // difference is carry size must mint tokens of the same bounded size.
        let mut carried = sample_continuation(2);
        carried.session_carry = Some("x".repeat(MONITOR_CURSOR_MAX_CHARS * 4));
        let bare = sample_continuation(2);

        let carried_token =
            encode_session_page_cursor("session-1", &carried).expect("carried cursor encodes");
        let bare_token =
            encode_session_page_cursor("session-1", &bare).expect("bare cursor encodes");
        assert!(
            carried_token.len() <= MONITOR_CURSOR_MAX_CHARS,
            "token must stay under the cap, got {}",
            carried_token.len()
        );
        assert_eq!(
            carried_token, bare_token,
            "an oversized carry is dropped, not encoded"
        );
        assert_eq!(
            decode_session_page_cursor(&carried_token, "session-1").expect("token decodes"),
            bare,
        );

        // A carry that fits is kept, so the next page still skips the
        // session-wide header pass.
        let mut small = sample_continuation(2);
        small.session_carry = Some("x".repeat(64));
        let small_token =
            encode_session_page_cursor("session-1", &small).expect("small cursor encodes");
        assert_eq!(
            decode_session_page_cursor(&small_token, "session-1").expect("token decodes"),
            small,
        );
    }

    #[tokio::test]
    async fn a_session_page_cursor_that_still_exceeds_the_cap_after_the_carry_drop_is_not_minted() {
        // Dropping the carry is a shrink, not a proof. An anchor that is itself
        // over the cap must fail the mint rather than be handed back as a
        // continuation the next request refuses.
        let mut oversized_anchor = sample_continuation(2);
        oversized_anchor.after.source_file = "f".repeat(MONITOR_CURSOR_MAX_CHARS * 2);
        oversized_anchor.session_carry = Some("x".repeat(MONITOR_CURSOR_MAX_CHARS));
        let message = encode_session_page_cursor("session-1", &oversized_anchor)
            .expect_err("an anchor over the cap must fail the mint");
        assert!(message.contains("cursor limit"), "got {message}");

        // Same verdict with no carry to drop, which is the branch that returns
        // the first encoding directly.
        let mut carryless = sample_continuation(2);
        carryless.after.source_file = "f".repeat(MONITOR_CURSOR_MAX_CHARS * 2);
        assert!(encode_session_page_cursor("session-1", &carryless).is_err());

        // The ordinary oversized-carry case still mints, and what mints decodes.
        let mut carried = sample_continuation(2);
        carried.session_carry = Some("x".repeat(MONITOR_CURSOR_MAX_CHARS * 4));
        let token = encode_session_page_cursor("session-1", &carried)
            .expect("dropping the carry keeps the mint");
        decode_session_page_cursor(&token, "session-1").expect("a minted token must decode");
    }

    #[tokio::test]
    async fn session_page_is_refused_until_the_canonical_reader_is_ready() {
        // The canonical reader does not gate itself. Every other consumer
        // checks `open_v2` readiness first; a route that skipped it would serve
        // transcripts off indexes whose backfill or overlap audit has not
        // published — precisely the backends where reading them is wrong.
        let (backend, repository) = fake_backend(InMemoryConversationResponses {
            canonical_reader_ready: Some(false),
            canonical_open_session_page: Some(Ok(Some(CanonicalReadOutcome::Page(
                sample_session_page(vec![sample_turn(1)], None),
            )))),
            ..Default::default()
        })
        .await;

        let response = api_session_page(
            Path("session-1".to_string()),
            Query(SessionPageQuery {
                limit: None,
                cursor: None,
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let payload = response_json(response).await;
        assert_eq!(payload["ok"], json!(false));
        assert_eq!(payload["code"], json!("canonical_reader_unavailable"));
        assert!(
            repository.calls().canonical_open_session_page.is_empty(),
            "a not-ready backend must not be read through the v2 reader"
        );
    }

    #[tokio::test]
    async fn session_page_answers_404_for_an_unknown_or_out_of_scope_session() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            canonical_open_session_page: Some(Ok(None)),
            ..Default::default()
        })
        .await;

        let response = api_session_page(
            Path("nope".to_string()),
            Query(SessionPageQuery {
                limit: None,
                cursor: None,
            }),
            Extension(backend),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(response).await,
            json!({"ok": false, "error": "session not found"})
        );
    }

    #[tokio::test]
    async fn session_page_route_is_reachable_and_versioned_only() {
        let (state, _) = fake_state(InMemoryConversationResponses {
            canonical_open_session_page: Some(Ok(Some(CanonicalReadOutcome::Page(
                sample_session_page(vec![sample_turn(1)], None),
            )))),
            ..Default::default()
        });
        let app = monitor_router(state);

        let (status, payload) = router_json(&app, "/api/v1/sessions/session-1/page?limit=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(payload["session"]["turns"][0]["turnSeq"], json!(1));
    }

    // -----------------------------------------------------------------------
    // issue-599 WI-09 — whole-corpus session search.
    // -----------------------------------------------------------------------

    fn search_params(q: &str) -> SessionSearchParams {
        SessionSearchParams {
            q: Some(q.to_string()),
            limit: None,
            harness: None,
            source: None,
        }
    }

    fn search_results(sessions: Vec<McpSessionListItem>) -> SessionSearchResults {
        SessionSearchResults {
            query_id: "monitor-search".to_string(),
            query: "repository".to_string(),
            terms: vec!["repository".to_string()],
            sessions,
            truncated: false,
            hits_truncated: false,
            incomplete: false,
            dropped: false,
        }
    }

    /// **Scope: the SERIALIZER, not the store.** Both routes are handed the
    /// identical `McpSessionListItem` here, so what this proves is that
    /// `api_session_search` and `api_sessions` render one summary the same way
    /// — same keys, same values, no search-shaped near-miss that would need its
    /// own reader.
    ///
    /// It deliberately cannot observe the two routes being handed DIFFERENT
    /// items for one session; only real hydration can produce that, and the
    /// repository integration test
    /// `both_discovery_surfaces_describe_one_session_identically` is what
    /// covers it. `status_is_derived_from_ended_at_so_a_divergent_timestamp_is
    /// _a_cross_surface_contradiction` below is the other half: why a
    /// divergence there would not stay cosmetic.
    #[tokio::test]
    async fn search_results_are_rendered_by_the_same_summary_serializer_as_the_feed() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            search_session_summaries: Some(Ok(search_results(vec![sample_session()]))),
            list_mcp_sessions: Some(Ok(Page {
                items: vec![sample_session()],
                next_cursor: None,
            })),
            ..Default::default()
        })
        .await;
        let (feed_backend, _) = fake_backend(InMemoryConversationResponses {
            list_mcp_sessions: Some(Ok(Page {
                items: vec![sample_session()],
                next_cursor: None,
            })),
            ..Default::default()
        })
        .await;

        let searched = response_json(
            api_session_search(Query(search_params("repository")), Extension(backend)).await,
        )
        .await;
        let listed =
            response_json(api_sessions(Query(sessions_query()), Extension(feed_backend)).await)
                .await;

        assert_eq!(searched["sessions"][0], listed["sessions"][0]);
        assert_eq!(searched["read_model"], json!("live"));
        assert_eq!(searched["result_count"], json!(1));
        assert_eq!(searched["query"], json!("repository"));
    }

    /// Why the two discovery surfaces must agree about `last_event_unix_ms` and
    /// not merely "about roughly when the session ended".
    ///
    /// `monitor_session_json` derives `status` from `endedAt` against a 60 s
    /// activity window, so two renderings of one session that differ by that
    /// window differ by a WORD: `active` versus `completed`. Two relations in
    /// the store can answer "when did this session last have an event" — the
    /// directory's `max(max_observed_event_time)` and navigation's
    /// `argMax(display_time)`, the former never below the latter — and for a
    /// while the feed reported one and the ranked search the other. This is the
    /// consequence that made that unacceptable, which is why both surfaces now
    /// report the directory value (`SessionKeyset`).
    ///
    /// MUTATION: make `status` a constant; this fails.
    ///
    /// Widening `SESSION_ACTIVE_WINDOW_MS` does NOT fail it, and an earlier
    /// revision of this comment offered that as a second recipe without running
    /// it. The test builds its gap FROM the constant
    /// (`last_event_unix_ms = now_ms - SESSION_ACTIVE_WINDOW_MS`), so widening
    /// the window widens the gap in lockstep — there is no fixed "gap used
    /// here". Verified at 600_000 and 86_400_000; both pass.
    #[test]
    fn status_is_derived_from_ended_at_so_a_divergent_timestamp_is_a_cross_surface_contradiction() {
        let now_ms = 1_767_262_260_000_i64;
        let mut session = sample_session();
        session.completed = false;

        // The hydrated aggregate.
        session.last_event_unix_ms = now_ms - SESSION_ACTIVE_WINDOW_MS;
        let hydrated = monitor_session_json(&session, now_ms);
        // The directory aggregate, which is >= the hydrated one, never below.
        session.last_event_unix_ms = now_ms;
        let keyset = monitor_session_json(&session, now_ms);

        assert_eq!(hydrated["status"], json!("completed"));
        assert_eq!(keyset["status"], json!("active"));
        assert_ne!(
            hydrated["status"], keyset["status"],
            "a timestamp difference of one activity window is a status contradiction, \
             not a rounding difference"
        );
    }

    /// The ranking pass reads message text to score and preview it. A search
    /// response must stay the same flat navigation-scalar shape the feed proved
    /// under 50x fatter transcripts (issue-599 §5.3).
    ///
    /// MUTATION: emit any hit-derived content on the response (a `snippet`, a
    /// `highlight`, a `preview`); this fails on the key scan.
    #[tokio::test]
    async fn search_response_carries_summaries_and_no_transcript_content() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            search_session_summaries: Some(Ok(search_results(vec![sample_session()]))),
            ..Default::default()
        })
        .await;

        let payload = response_json(
            api_session_search(Query(search_params("repository")), Extension(backend)).await,
        )
        .await;

        let mut keys = Vec::new();
        all_keys(&payload, &mut keys);
        for forbidden in [
            "turns",
            "steps",
            "text",
            "events",
            "payload_json",
            "snippet",
            "text_content",
            "text_preview",
        ] {
            assert!(
                !keys.iter().any(|key| key == forbidden),
                "search response must not carry {forbidden:?}: {payload}"
            );
        }

        let mut fields = payload["sessions"][0]
            .as_object()
            .expect("session object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        fields.sort();
        assert_eq!(
            fields,
            vec![
                "displayLabel",
                "endedAt",
                "eventCount",
                "harness",
                "id",
                "inferenceProvider",
                "mode",
                "sessionSlug",
                "sessionSummary",
                "source",
                "startedAt",
                "status",
                "title",
                "toolCallCount",
                "turnCount",
            ]
        );
    }

    /// The four bounded-answer signals stay distinct on the wire. Collapsing
    /// any pair tells a reader to apply the wrong remedy: "raise `limit`" when
    /// the ranking ran out of candidate budget, or "this is everything" when
    /// the exact re-check removed half the answer.
    ///
    /// The matrix is exhaustive over all sixteen combinations, so a field wired
    /// to its neighbour's value cannot survive.
    #[tokio::test]
    async fn search_reports_every_bounded_answer_signal_separately() {
        for bits in 0u8..16 {
            let (truncated, hits_truncated, incomplete, dropped) =
                (bits & 1 != 0, bits & 2 != 0, bits & 4 != 0, bits & 8 != 0);
            let (backend, _) = fake_backend(InMemoryConversationResponses {
                search_session_summaries: Some(Ok(SessionSearchResults {
                    truncated,
                    hits_truncated,
                    incomplete,
                    dropped,
                    ..search_results(vec![sample_session()])
                })),
                ..Default::default()
            })
            .await;
            let payload = response_json(
                api_session_search(Query(search_params("repository")), Extension(backend)).await,
            )
            .await;
            assert_eq!(payload["truncated"], json!(truncated), "bits={bits}");
            assert_eq!(
                payload["hits_truncated"],
                json!(hits_truncated),
                "bits={bits}"
            );
            assert_eq!(payload["incomplete"], json!(incomplete), "bits={bits}");
            assert_eq!(payload["dropped"], json!(dropped), "bits={bits}");
        }
    }

    /// A blank or absent query is a client bug. Answering it with an empty
    /// result set would render as "nothing in the corpus matches", which is a
    /// different and false statement.
    #[tokio::test]
    async fn a_blank_search_query_is_refused_rather_than_answered_with_no_results() {
        for q in [None, Some(String::new()), Some("   ".to_string())] {
            let (backend, repository) = fake_backend(InMemoryConversationResponses {
                search_session_summaries: Some(Ok(search_results(vec![sample_session()]))),
                ..Default::default()
            })
            .await;
            let response = api_session_search(
                Query(SessionSearchParams {
                    q: q.clone(),
                    limit: None,
                    harness: None,
                    source: None,
                }),
                Extension(backend),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "q={q:?}");
            assert!(
                repository.calls().search_session_summaries.is_empty(),
                "q={q:?}: a blank query must not reach the repository",
            );
        }
    }

    /// Search narrowing must reach the repository, and a cleared dashboard
    /// filter must arrive as "no filter" rather than as a filter for the empty
    /// string. Both clamps on `limit` are real and the second one is the
    /// backend's own `max_results`.
    #[tokio::test]
    async fn search_parameters_reach_the_shared_repository_query() {
        let (backend, repository) = fake_backend(InMemoryConversationResponses {
            search_session_summaries: Some(Ok(search_results(Vec::new()))),
            ..Default::default()
        })
        .await;

        let payload = response_json(
            api_session_search(
                Query(SessionSearchParams {
                    q: Some("  repository inspection  ".to_string()),
                    limit: Some(9_999),
                    harness: Some(" codex ".to_string()),
                    source: Some("   ".to_string()),
                }),
                Extension(backend),
            )
            .await,
        )
        .await;

        let calls = repository.calls().search_session_summaries;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].query, "repository inspection");
        assert_eq!(calls[0].harness.as_deref(), Some("codex"));
        assert_eq!(calls[0].source_name, None);
        // `9_999` clamps to the route's own bound, then to `RepoConfig`'s
        // `max_results` (25). The response reports the effective value.
        assert_eq!(calls[0].limit, Some(25));
        assert_eq!(payload["limit"], json!(25));
        assert_eq!(payload["sessions"], json!([]));
        assert_eq!(payload["result_count"], json!(0));
    }

    /// Search is one repository read, and a failure of it keeps the same
    /// budget-classified envelope every other monitor read uses. A deadline
    /// answered as an empty result set would read as "nothing matches".
    ///
    /// **The client-error arm is the load-bearing one.** The repository owns
    /// the tokenizer, so it — not the route — decides that `?q=x` or a
    /// Cyrillic query carries no searchable term, and it says so with
    /// `InvalidArgument`. Classified as 503 that rendered as "Search
    /// unavailable", i.e. an outage banner for a typo, permanently, with a
    /// retry that could never succeed.
    ///
    /// MUTATION: delete the `RepoError::InvalidArgument` arm from
    /// `repo_error_status`; the `invalid_argument` row falls through to
    /// `503`/no-code and this fails.
    #[tokio::test]
    async fn a_failed_search_is_classified_by_whether_the_client_or_the_store_is_at_fault() {
        let cases = [
            (
                RepoError::deadline_exceeded("search deadline exceeded"),
                StatusCode::GATEWAY_TIMEOUT,
                Some("deadline_exceeded"),
            ),
            (
                RepoError::resource_exhausted("statement cap"),
                StatusCode::TOO_MANY_REQUESTS,
                Some("resource_exhausted"),
            ),
            (
                RepoError::invalid_argument(
                    "query has no searchable terms (tokens shorter than 2 characters are excluded)",
                ),
                StatusCode::BAD_REQUEST,
                Some("invalid_argument"),
            ),
            (
                RepoError::backend("clickhouse is unreachable"),
                StatusCode::SERVICE_UNAVAILABLE,
                None,
            ),
        ];

        for (error, expected_status, expected_code) in cases {
            let label = format!("{error}");
            let (backend, _) = fake_backend(InMemoryConversationResponses {
                search_session_summaries: Some(Err(error)),
                ..Default::default()
            })
            .await;

            let response =
                api_session_search(Query(search_params("repository")), Extension(backend)).await;
            assert_eq!(response.status(), expected_status, "{label}");
            let payload = response_json(response).await;
            assert_eq!(payload["ok"], json!(false), "{label}");
            match expected_code {
                Some(code) => assert_eq!(payload["code"], json!(code), "{label}"),
                None => assert!(payload.get("code").is_none(), "{label}: {payload}"),
            }
        }
    }

    /// The route runs inside a #600 interactive envelope — asserted through the
    /// REAL router, not a synthetic probe route bolted onto the middleware.
    ///
    /// The handler threads `QueryEnvelope::current()`'s request id into the
    /// repository query, so the recorded call proves an envelope was in scope
    /// when the read was issued. A route registered outside the envelope layer
    /// records `None` here.
    ///
    /// MUTATION: register `/sessions/search` on `versioned_routes` in
    /// `monitor_router` instead of inside `dashboard_routes` — i.e. beside
    /// `/capabilities`, which is the one route deliberately outside the
    /// envelope `route_layer` — or pass `cancellation_token: None` from the
    /// handler; either fails.
    ///
    /// Moving the route WITHIN `dashboard_routes` does not: `route_layer` is
    /// applied to that whole router in `monitor_router`, above both the
    /// `admitted` group and the bare `/health` group, so no reordering of
    /// `.route(...)` lines there can leave a route unenveloped. A previous
    /// version of this comment named that reordering, and it survives.
    #[tokio::test]
    async fn the_session_search_route_runs_inside_an_interactive_monitor_envelope() {
        let (state, repository) = fake_state(InMemoryConversationResponses {
            search_session_summaries: Some(Ok(search_results(vec![sample_session()]))),
            ..Default::default()
        });
        let app = monitor_router(state);

        let (status, _) = router_json(&app, "/api/v1/sessions/search?q=repository").await;
        assert_eq!(status, StatusCode::OK);
        let (second_status, _) = router_json(&app, "/api/v1/sessions/search?q=repository").await;
        assert_eq!(second_status, StatusCode::OK);

        let calls = repository.calls().search_session_summaries;
        assert_eq!(calls.len(), 2);
        let ids = calls
            .iter()
            .map(|call| {
                call.cancellation_token
                    .clone()
                    .expect("the handler must see a #600 envelope")
            })
            .collect::<Vec<_>>();
        for id in &ids {
            assert!(
                id.starts_with("moraine-monitor-"),
                "monitor request ids must carry the monitor kind: {id}"
            );
        }
        // Per request, never per client.
        assert_ne!(ids[0], ids[1]);
    }

    /// `search` is a fixed path segment, not a session id — and a session that
    /// really is called `search` is still readable.
    ///
    /// Both halves are asserted by the RESPONSE BODY, not by "did not 404".
    /// The two handlers answer different shapes, so a 404-only assertion would
    /// pass even if `/sessions/search/page` had been answered by the search
    /// handler, or vice versa.
    #[tokio::test]
    async fn search_is_a_fixed_path_and_does_not_shadow_the_session_page_route() {
        let (state, _) = fake_state(InMemoryConversationResponses {
            search_session_summaries: Some(Ok(search_results(vec![sample_session()]))),
            canonical_open_session_page: Some(Ok(Some(CanonicalReadOutcome::Page(
                sample_session_page(vec![sample_turn(1)], None),
            )))),
            ..Default::default()
        });
        let app = monitor_router(state);

        let (status, payload) = router_json(&app, "/api/v1/sessions/search?q=repository").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(session_ids(&payload), vec!["session-1"]);
        // The search envelope, not a session page.
        assert_eq!(payload["terms"], json!(["repository"]));
        assert!(payload.get("session").is_none(), "{payload}");

        // …and the parameterized sibling still resolves, for the session id
        // `search`, with the PAGE envelope.
        let (page_status, page_payload) =
            router_json(&app, "/api/v1/sessions/search/page?limit=1").await;
        assert_eq!(page_status, StatusCode::OK);
        assert!(
            page_payload["session"].is_object(),
            "`/sessions/search/page` must reach the session page handler: {page_payload}"
        );
        assert!(page_payload.get("terms").is_none(), "{page_payload}");
    }

    /// **Insurance, not the current mechanism.** Today's `/sessions` routes are
    /// unambiguous because their segment counts differ (1, 2 and 3), so nothing
    /// in `dashboard_routes` depends on static-over-parameter precedence — the
    /// registration comment there says so, and this test is deliberately built
    /// against a SYNTHETIC router the real table does not have.
    ///
    /// It exists for the day a two-segment `/sessions/:id` is added, which is
    /// when `/sessions/search` would start to need the rule. Two earlier
    /// versions of that comment each named a mechanism that was not the
    /// operative one — first REGISTRATION ORDER, which axum does not use at
    /// all, then matchit precedence, which the real table never reaches — so
    /// the property is pinned here rather than asserted in prose, and labelled
    /// for what it is.
    ///
    /// This registers the PARAMETER FIRST and asserts the static segment still
    /// wins, because insertion order is the thing a contributor would otherwise
    /// assume matters.
    #[tokio::test]
    async fn matchit_prefers_a_static_segment_over_a_parameter_at_any_registration_order() {
        async fn param() -> &'static str {
            "param"
        }
        async fn statik() -> &'static str {
            "static"
        }

        for register_static_first in [false, true] {
            let router: Router<()> = if register_static_first {
                Router::new()
                    .route("/sessions/search", get(statik))
                    .route("/sessions/:id", get(param))
            } else {
                Router::new()
                    .route("/sessions/:id", get(param))
                    .route("/sessions/search", get(statik))
            };
            let response = router
                .oneshot(
                    Request::builder()
                        .uri("/sessions/search")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            let body = axum::body::to_bytes(response.into_body(), 64)
                .await
                .expect("body");
            assert_eq!(
                std::str::from_utf8(&body).expect("utf8"),
                "static",
                "register_static_first={register_static_first}"
            );
        }
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
    async fn health_and_status_expose_publication_progress_and_fail_closed_diagnostics() {
        let diagnostics = PublicationDiagnostics {
            ambiguous_hostless_rows: 7,
            replaying_generations: 2,
            blocked_generations: 1,
            append_preparations: 3,
            blocked_append_preparations: 2,
            mirror_catchup_pending: 4,
            writer_conflicts: 1,
            issues: vec!["host identity cannot be proven".to_string()],
        };
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(StoreHealth {
                publication: StoreProbe::Available(diagnostics),
                ..sample_health()
            })),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            list_table_summaries: Some(Ok(TableSummaries::default())),
            ..Default::default()
        })
        .await;

        let health = response_json(api_health(Extension(backend.clone())).await).await;
        assert_eq!(health["ok"], json!(true));
        assert_eq!(health["publication"]["healthy"], json!(false));
        assert_eq!(health["publication"]["ambiguous_hostless_rows"], json!(7));
        assert_eq!(health["publication"]["blocked_generations"], json!(1));
        assert_eq!(health["publication"]["writer_conflicts"], json!(1));
        assert_eq!(health["publication"]["replaying_generations"], json!(2));
        assert_eq!(health["publication"]["append_preparations"], json!(3));
        assert_eq!(
            health["publication"]["blocked_append_preparations"],
            json!(2)
        );
        assert_eq!(health["publication"]["mirror_catchup_pending"], json!(4));

        let status = response_json(api_status(Extension(backend)).await).await;
        assert_eq!(status["publication"], health["publication"]);
    }

    #[tokio::test]
    async fn health_and_status_expose_core_index_readiness() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(StoreHealth {
                core_index: StoreProbe::Available(CoreIndexHealth {
                    core_indexes_ready: true,
                    open_v2_ready: true,
                    open_v2_provenance: Some("operator-promote".to_string()),
                    backfill_cursor_age_ms: Some(9_000),
                    audit_outcome: None,
                }),
                ..sample_health()
            })),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            list_table_summaries: Some(Ok(TableSummaries::default())),
            ..Default::default()
        })
        .await;

        let health = response_json(api_health(Extension(backend.clone())).await).await;
        assert_eq!(health["ok"], json!(true));
        assert_eq!(health["core_index"]["available"], json!(true));
        assert_eq!(health["core_index"]["core_indexes_ready"], json!(true));
        assert_eq!(health["core_index"]["open_v2_ready"], json!(true));
        assert_eq!(
            health["core_index"]["open_v2_provenance"],
            json!("operator-promote")
        );
        assert_eq!(health["core_index"]["backfill_cursor_age_ms"], json!(9_000));

        // The additive block is present on `/status` with the same shape.
        let status = response_json(api_status(Extension(backend)).await).await;
        assert_eq!(status["core_index"], health["core_index"]);
    }

    /// Issue #603 WI-02. The `storage` block is additive, per-bucket (so the
    /// response cannot grow with the table count), and reports a stock
    /// configuration as authorizing no deletion.
    #[tokio::test]
    async fn health_exposes_the_storage_block_with_no_destructive_policy_by_default() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(sample_health())),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            list_table_summaries: Some(Ok(TableSummaries::default())),
            ..Default::default()
        })
        .await;

        let health = response_json(api_health(Extension(backend)).await).await;
        assert_eq!(health["ok"], json!(true));
        let storage = &health["storage"];
        assert_eq!(storage["available"], json!(true));

        // Per-bucket, never per-table: every class is present, even at zero.
        let buckets = storage["buckets"].as_array().expect("buckets array");
        assert_eq!(buckets.len(), 5);
        let canonical = buckets
            .iter()
            .find(|bucket| bucket["class"] == json!("canonical_history"))
            .expect("canonical bucket");
        assert_eq!(canonical["tables"], json!(1));
        assert_eq!(canonical["rows"], json!(1_990_776));
        assert_eq!(canonical["compressed_bytes"], json!(4_787_723_965_u64));
        let telemetry = buckets
            .iter()
            .find(|bucket| bucket["class"] == json!("telemetry"))
            .expect("telemetry bucket present even at zero");
        assert_eq!(telemetry["tables"], json!(0));

        assert_eq!(
            storage["total_compressed_bytes"],
            json!(4_787_723_965_u64 + 14_356_000_000_u64)
        );
        assert_eq!(storage["disk"]["free_bytes"], json!(11_780_276_224_u64));
        assert_eq!(storage["disk"]["used_bytes"], json!(982_882_308_096_u64));

        // Default configuration authorizes deleting nothing, and the block
        // says so directly rather than making a client re-derive it.
        assert_eq!(storage["destructive_policies"], json!([]));
        let policy = storage["policy"].as_array().expect("policy array");
        let canonical_policy = policy
            .iter()
            .find(|entry| entry["class"] == json!("canonical_history"))
            .expect("canonical policy");
        assert_eq!(canonical_policy["horizon_seconds"], Value::Null);
        assert_eq!(canonical_policy["source"], json!("default"));
        assert_eq!(canonical_policy["destructive"], json!(false));
        assert_eq!(
            canonical_policy["config_key"],
            json!("retention.canonical_history_horizon_days")
        );
        assert_eq!(canonical_policy["note"], Value::Null);

        // The telemetry horizon reports a `config_key` that cannot move it, so
        // the API must carry the caveat with the key. Without it, a client that
        // renders `config_key` invites an operator to set a value the server
        // will refuse — or, before the refusal existed, to set one the server
        // accepted and never applied.
        let telemetry_policy = policy
            .iter()
            .find(|entry| entry["class"] == json!("telemetry"))
            .expect("telemetry policy");
        assert_eq!(telemetry_policy["source"], json!("default"));
        assert_eq!(telemetry_policy["horizon_seconds"], json!(2_592_000.0));
        assert_eq!(
            telemetry_policy["note"],
            json!(moraine_conversations::TELEMETRY_HORIZON_NOT_CONFIGURABLE_NOTE)
        );
        assert_eq!(storage["unclassified_tables"], json!([]));
    }

    /// The probe degrades independently: an unavailable storage report must not
    /// take the rest of health down with it.
    #[tokio::test]
    async fn health_reports_an_unavailable_storage_probe_without_failing() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(StoreHealth {
                storage: StoreProbe::Failed {
                    message: "system.parts unreadable".to_string(),
                },
                ..sample_health()
            })),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            list_table_summaries: Some(Ok(TableSummaries::default())),
            ..Default::default()
        })
        .await;

        let health = response_json(api_health(Extension(backend)).await).await;
        assert_eq!(health["ok"], json!(true), "storage is not a health gate");
        assert_eq!(health["storage"]["available"], json!(false));
        assert_eq!(health["storage"]["error"], json!("system.parts unreadable"));
        assert_eq!(health["core_index"]["available"], json!(true));
    }

    #[tokio::test]
    async fn status_serves_the_harness_filter_vocabulary() {
        // `harness` narrows the session feed server-side, so the dashboard's
        // menu cannot be derived from the page it happens to have loaded — a
        // harness with no session on page 1 would be unselectable.
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(sample_health())),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            list_table_summaries: Some(Ok(TableSummaries::default())),
            ..Default::default()
        })
        .await;

        let status = response_json(api_status(Extension(backend)).await).await;
        assert_eq!(
            status["known_harnesses"],
            json!(moraine_config::KNOWN_INGEST_HARNESSES),
        );
    }

    #[tokio::test]
    async fn core_index_probe_failure_is_diagnostic_without_hiding_store_liveness() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(StoreHealth {
                core_index: StoreProbe::Failed {
                    message: "core read indexes unavailable".to_string(),
                },
                ..sample_health()
            })),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            ..Default::default()
        })
        .await;

        let response = api_health(Extension(backend)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let health = response_json(response).await;
        assert_eq!(health["ok"], json!(true));
        assert_eq!(health["core_index"]["available"], json!(false));
        assert_eq!(
            health["core_index"]["error"],
            json!("core read indexes unavailable")
        );
    }

    #[tokio::test]
    async fn publication_probe_failure_is_diagnostic_without_hiding_store_liveness() {
        let (backend, _) = fake_backend(InMemoryConversationResponses {
            read_store_health: Some(Ok(StoreHealth {
                publication: StoreProbe::Failed {
                    message: "publication control schema unavailable".to_string(),
                },
                ..sample_health()
            })),
            latest_ingest_heartbeat: Some(Ok(sample_heartbeat())),
            ..Default::default()
        })
        .await;

        let response = api_health(Extension(backend)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let health = response_json(response).await;
        assert_eq!(health["ok"], json!(true));
        assert_eq!(health["publication"]["available"], json!(false));
        assert_eq!(health["publication"]["healthy"], json!(false));
        assert_eq!(
            health["publication"]["error"],
            json!("publication control schema unavailable")
        );
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
    async fn versioned_routes_serve_the_api_and_legacy_aliases_are_gone() {
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
        let app = router_with_backend_router(backend_router, root.clone(), default_query_budgets())
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

        // The one-release `/api/*` compatibility window opened with `/api/v1`
        // in v0.7.0 and closed with v0.7.1. Every legacy path is now an unknown
        // path, which falls through to static handling and 404s.
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
            (
                "/api/v1/sessions/search?q=repository",
                "/api/sessions/search?q=repository",
            ),
        ];
        for (canonical_path, legacy_path) in route_matrix {
            let (canonical_status, _) = router_json(&app, canonical_path).await;
            assert_eq!(canonical_status, StatusCode::OK, "{canonical_path}");

            let (legacy_status, legacy) = router_json(&app, legacy_path).await;
            assert_eq!(
                legacy_status,
                StatusCode::NOT_FOUND,
                "{legacy_path} must no longer be served"
            );
            assert_eq!(legacy, json!({"ok": false, "error": "not found"}));
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
        // Each canonical route was served exactly once; the dead alias reached
        // no handler at all.
        let calls = repository.calls();
        assert_eq!(calls.read_store_health, 2);
        assert_eq!(calls.read_store_diagnostics, 1);
        assert_eq!(calls.latest_ingest_heartbeat, 2);
        assert_eq!(calls.list_table_summaries, 2);
        assert_eq!(calls.list_web_searches, vec![1_000]);
        assert_eq!(calls.analytics_series, vec![AnalyticsRange::SevenDays]);
        assert_eq!(calls.list_mcp_sessions.len(), 1);
        assert_eq!(
            calls.preview_table,
            vec![TablePreviewQuery {
                table: "events".to_string(),
                limit: 500,
            }]
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
        let app = router_with_backend_router(backend_router, root.clone(), default_query_budgets())
            .expect("routing test app");

        let default = response_json(get_with_project_dir(&app, "/api/v1/tables", None).await).await;
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
                "/api/v1/tables",
                Some(HeaderValue::from_static("/work/ghost/project")),
            )
            .await,
        )
        .await;
        let named_again = response_json(
            get_with_project_dir(
                &app,
                "/api/v1/tables",
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
        let app = router_with_backend_router(backend_router, root.clone(), default_query_budgets())
            .expect("capabilities routing test app");

        for header in [
            None,
            Some(HeaderValue::from_static("/work/team/project")),
            Some(HeaderValue::from_static("malformed-relative-path")),
        ] {
            let response = get_with_project_dir(&app, "/api/v1/capabilities", header).await;
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
        let mut config = routing_config();
        config
            .backends
            .get_mut("team-ch")
            .expect("named backend")
            .url = "http://user:secret@team.example:8123/path?token=secret#fragment".to_string();
        let backend_router = preloaded_backend_router(config, default_repository, named_repository);
        let app = router_with_backend_router(backend_router, root.clone(), default_query_budgets())
            .expect("metadata test app");

        let default_health =
            response_json(get_with_project_dir(&app, "/api/v1/health", None).await).await;
        assert_eq!(default_health["url"], json!("http://default.example:8123"));
        assert_eq!(default_health["database"], json!("moraine_default"));

        let named_header = HeaderValue::from_static("/work/team/project");
        let named_health = response_json(
            get_with_project_dir(&app, "/api/v1/health", Some(named_header.clone())).await,
        )
        .await;
        assert_eq!(named_health["url"], json!("http://team.example:8123"));
        assert_eq!(named_health["database"], json!("moraine_team"));

        let named_status =
            response_json(get_with_project_dir(&app, "/api/v1/status", Some(named_header)).await)
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
        let app = router_with_backend_router(backend_router, root.clone(), default_query_budgets())
            .expect("validation test app");

        let mut repeated = Request::builder()
            .uri("/api/v1/health")
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
                .uri("/api/v1/health")
                .header(PROJECT_DIR_HEADER, HeaderValue::from_static("   "))
                .body(Body::empty())
                .expect("empty header request"),
            Request::builder()
                .uri("/api/v1/health")
                .header(
                    PROJECT_DIR_HEADER,
                    HeaderValue::from_static("relative/project"),
                )
                .body(Body::empty())
                .expect("relative header request"),
            Request::builder()
                .uri("/api/v1/health")
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
        let app = router_with_backend_router(backend_router, root.clone(), default_query_budgets())
            .expect("construction error test app");

        let response = get_with_project_dir(
            &app,
            "/api/v1/health",
            Some(HeaderValue::from_static("/work/team/project")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let payload = response_json(response).await;
        assert_eq!(payload["ok"], json!(false));
        assert_eq!(
            payload["error"],
            json!("selected backend is unavailable or schema-incompatible")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn host_port_startup_validates_static_dir_before_binding() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind occupied address");
        let address = occupied.local_addr().expect("occupied address");
        let missing_static_dir = temp_path("host-port-validation-order");
        let (state, _) = fake_state(InMemoryConversationResponses::default());

        let error = run_server_with_router(
            state.backend_router.clone(),
            address.ip().to_string(),
            address.port(),
            missing_static_dir,
            default_query_budgets(),
            std::future::pending(),
        )
        .await
        .expect_err("missing static directory should fail before bind");
        assert!(error.to_string().contains("is unavailable"));
        assert!(!error.to_string().contains("address already in use"));
    }

    #[tokio::test]
    async fn supplied_listener_is_owned_until_shutdown_then_released() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let static_dir = static_root("listener-ownership", b"<!doctype html>");
        let (state, _) = fake_state(InMemoryConversationResponses::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_static_dir = static_dir.clone();
        let server = tokio::spawn(run_server_with_listener(
            state.backend_router.clone(),
            listener,
            server_static_dir,
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        let bind_error = tokio::net::TcpListener::bind(address)
            .await
            .expect_err("server must retain exclusive ownership of listener address");
        assert_eq!(bind_error.kind(), ErrorKind::AddrInUse);

        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect to supplied listener");
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write monitor request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read monitor response");
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));

        shutdown_tx.send(()).expect("signal shutdown");
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .expect("server shutdown timed out")
            .expect("server task panicked")
            .expect("server shutdown failed");

        let rebound = tokio::net::TcpListener::bind(address)
            .await
            .expect("listener address should be reusable after shutdown");
        drop(rebound);
        let _ = fs::remove_dir_all(static_dir);
    }

    #[tokio::test]
    async fn supplied_listener_is_released_when_startup_fails() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let missing_static_dir = temp_path("listener-startup-failure");
        let (state, _) = fake_state(InMemoryConversationResponses::default());

        let error = run_server_with_listener(
            state.backend_router.clone(),
            listener,
            missing_static_dir,
            std::future::pending(),
        )
        .await
        .expect_err("missing static directory should fail startup");
        assert!(error.to_string().contains("is unavailable"));

        let rebound = tokio::net::TcpListener::bind(address)
            .await
            .expect("listener address should be reusable after startup failure");
        drop(rebound);
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

    #[tokio::test]
    async fn data_requests_run_inside_an_interactive_monitor_envelope() {
        let limits = Arc::new(MonitorReadLimits::new(default_query_budgets()));
        let app: Router = Router::new()
            .route(
                "/probe",
                get(|| async {
                    let envelope = QueryEnvelope::current().expect("handler must see an envelope");
                    Json(json!({
                        "request_id": envelope.request_id(),
                        "interactive": envelope.class() == QueryClass::Interactive,
                    }))
                }),
            )
            .route_layer(middleware::from_fn_with_state(
                limits,
                monitor_query_envelope,
            ));

        let (status, first) = router_json(&app, "/probe").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["interactive"], json!(true));
        let first_id = first["request_id"].as_str().expect("request id");
        assert!(
            first_id.starts_with("moraine-monitor-"),
            "monitor request ids must carry the monitor kind: {first_id}"
        );

        // Per request, never per client: a second request gets a new envelope.
        let (_, second) = router_json(&app, "/probe").await;
        assert_ne!(second["request_id"], first["request_id"]);
    }

    #[tokio::test]
    async fn health_and_status_expose_query_budget_totals() {
        let (state, _) = fake_state(successful_responses());
        let app = monitor_router(state);

        let before = budget_telemetry();
        let (code, health) = router_json(&app, "/api/v1/health").await;
        assert_eq!(code, StatusCode::OK);
        for field in [
            "requests",
            "statements",
            "deadline_exceeded",
            "resource_exhausted",
            "unenveloped_statements",
        ] {
            assert!(
                health["query_budgets"][field].is_u64(),
                "query_budgets.{field} must be a counter: {health}"
            );
        }

        let (code, status) = router_json(&app, "/api/v1/status").await;
        assert_eq!(code, StatusCode::OK);
        assert!(
            status["query_budgets"]["requests"].is_u64(),
            "status must carry the same query_budgets block: {status}"
        );

        // Both requests ran inside the monitor envelope middleware, so the
        // process totals grew by at least two (lower bound only: the sink is
        // shared with concurrently running tests in this binary).
        assert!(
            budget_telemetry().requests >= before.requests + 2,
            "monitor requests must fold into the process budget telemetry"
        );
    }

    #[tokio::test]
    async fn read_admission_overflow_returns_429_and_health_stays_exempt() {
        let (state, _) = fake_state(successful_responses());
        let app = monitor_router(state.clone());

        let held = state
            .read_limits
            .read_permits
            .acquire_many(MONITOR_READ_PERMITS as u32)
            .await
            .expect("hold every read permit");

        let before = budget_telemetry();
        let (status, payload) = router_json(&app, "/api/v1/tables").await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(payload["ok"], json!(false));
        assert_eq!(payload["code"], json!("resource_exhausted"));
        assert!(payload["error"]
            .as_str()
            .is_some_and(|message| message.contains("concurrent reads")));
        // The rejection is budget exhaustion and must reach the process-wide
        // telemetry the health endpoint reports (lower bound: the sink is
        // shared with concurrently running tests).
        assert!(
            budget_telemetry().resource_exhausted > before.resource_exhausted,
            "admission overflow must count as resource_exhausted"
        );

        // Health is deliberately outside the semaphore so supervisors can
        // still probe liveness while the dashboard has the permits busy.
        let (status, payload) = router_json(&app, "/api/v1/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(payload["ok"], json!(true));

        drop(held);
        let (status, _) = router_json(&app, "/api/v1/tables").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn budget_errors_map_to_504_and_429_with_additive_codes() {
        let (state, _) = fake_state(InMemoryConversationResponses {
            list_table_summaries: Some(Err(RepoError::deadline_exceeded(
                "query budget deadline expired (budget 15.000s)",
            ))),
            analytics_series: Some(Err(RepoError::resource_exhausted(
                "read_rows allowance exhausted (budget 500000000)",
            ))),
            list_web_searches: Some(Err(RepoError::backend("web unavailable"))),
            read_store_health: Some(Err(RepoError::deadline_exceeded(
                "query budget deadline expired (budget 15.000s)",
            ))),
            ..Default::default()
        });
        let app = monitor_router(state);

        let (status, payload) = router_json(&app, "/api/v1/tables").await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(payload["ok"], json!(false));
        assert_eq!(payload["code"], json!("deadline_exceeded"));
        assert!(payload["error"]
            .as_str()
            .is_some_and(|message| message.contains("deadline exceeded")));

        let (status, payload) = router_json(&app, "/api/v1/analytics").await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(payload["code"], json!("resource_exhausted"));

        // Health keeps its existing failure payload fields and adds the code.
        let (status, payload) = router_json(&app, "/api/v1/health").await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(payload["code"], json!("deadline_exceeded"));
        assert_eq!(payload["publication"]["available"], json!(false));
        // Budget telemetry stays visible on the failure shape too: exhaustion
        // is exactly when the operator needs the counters.
        assert!(
            payload["query_budgets"]["deadline_exceeded"].is_u64(),
            "failure payload must keep the query_budgets block: {payload}"
        );

        // Non-budget failures keep the pre-envelope contract: 503, no code.
        let (status, payload) = router_json(&app, "/api/v1/web-searches").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(payload.get("code").is_none());
    }

    /// Mock ClickHouse endpoint: KILL statements are recorded and succeed,
    /// every other statement signals arrival and then hangs forever.
    #[derive(Clone)]
    struct MockClickHouse {
        kills: Arc<std::sync::Mutex<Vec<String>>>,
        started_tx: tokio::sync::mpsc::UnboundedSender<()>,
    }

    async fn mock_clickhouse_statement(
        State(mock): State<MockClickHouse>,
        Query(params): Query<std::collections::HashMap<String, String>>,
    ) -> Response {
        let sql = params.get("query").cloned().unwrap_or_default();
        if sql.trim_start().to_uppercase().starts_with("KILL QUERY") {
            mock.kills.lock().expect("kill log").push(sql);
            return Response::new(Body::from(""));
        }
        let _ = mock.started_tx.send(());
        std::future::pending::<Response>().await
    }

    #[tokio::test]
    async fn dropped_monitor_request_kills_the_inflight_statement() {
        use moraine_conversations::build_clickhouse_repository;

        let kills = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let mock = MockClickHouse {
            kills: kills.clone(),
            started_tx,
        };
        let mock_app: Router = Router::new()
            .fallback(axum::routing::any(mock_clickhouse_statement))
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock clickhouse");
        let mock_addr = listener.local_addr().expect("mock clickhouse address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, mock_app).await;
        });

        let clickhouse = ClickHouseConfig {
            url: format!("http://{mock_addr}"),
            database: "moraine".to_string(),
            ..ClickHouseConfig::default()
        };
        let repository = build_clickhouse_repository(clickhouse, RepoConfig::default())
            .expect("build repository against mock clickhouse");
        let backend_router = Arc::new(
            BackendRepositoryRouter::from_preloaded_for_testing(
                Arc::new(AppConfig::default()),
                [(DEFAULT_BACKEND_NAME.to_string(), repository)],
            )
            .expect("preloaded mock-backed router"),
        );
        let state = Arc::new(AppState {
            backend_router,
            static_dir: PathBuf::new(),
            read_limits: Arc::new(MonitorReadLimits::new(default_query_budgets())),
        });
        let app = monitor_router(state);

        // Drive a repository-backed endpoint until its first ClickHouse
        // statement is in flight on the mock, then drop the request future —
        // exactly what axum does when the HTTP client disconnects.
        let mut request = Box::pin(
            app.clone().oneshot(
                Request::builder()
                    .uri("/api/v1/tables")
                    .body(Body::empty())
                    .expect("tables request"),
            ),
        );
        tokio::select! {
            response = &mut request => {
                panic!("mock ClickHouse must hold the statement open, got {response:?}");
            }
            started = started_rx.recv() => {
                started.expect("statement start signal");
            }
        }
        drop(request);

        // The transport's drop guards must issue a bounded KILL for the
        // abandoned statement, carrying the monitor envelope's id prefix.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let recorded = kills.lock().expect("kill log").clone();
            if recorded
                .iter()
                .any(|sql| sql.contains("KILL QUERY") && sql.contains("moraine-monitor-"))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no KILL for the dropped monitor request; observed: {recorded:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}
