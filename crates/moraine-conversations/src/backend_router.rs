use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{anyhow, Context as _, Result};
use moraine_clickhouse::{
    enforce_remote_schema_policy, ClickHouseClient, QueryOwner, QueryRuntime, QueryWorkload,
};
use moraine_config::{AppConfig, ClickHouseConfig, DEFAULT_BACKEND_NAME};
use tokio::sync::{watch, Mutex};
use tracing::warn;
use url::Url;

use crate::{
    build_clickhouse_repository_with_user_agent, ClickHouseConversationRepository,
    ConversationRepository, RepoConfig,
};

fn clickhouse_display_url(raw_url: &str) -> Arc<str> {
    let Ok(url) = Url::parse(raw_url) else {
        return Arc::from("<invalid ClickHouse URL>");
    };
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return Arc::from("<invalid ClickHouse URL>");
    }
    Arc::from(url.origin().ascii_serialization())
}

/// An immutable repository handle together with the configured backend identity
/// that produced it.
///
/// Handles are cached by [`BackendRepositoryRouter`]. Cloning the outer `Arc`
/// preserves both the repository instance and its metadata as one unit.
pub struct BackendRepository {
    backend_name: Arc<str>,
    repository: Arc<dyn ConversationRepository>,
    clickhouse_url: Arc<str>,
    clickhouse_database: Arc<str>,
}

impl BackendRepository {
    fn new(
        backend_name: Arc<str>,
        repository: Arc<dyn ConversationRepository>,
        clickhouse: &ClickHouseConfig,
    ) -> Self {
        Self {
            backend_name,
            repository,
            clickhouse_url: clickhouse_display_url(&clickhouse.url),
            clickhouse_database: Arc::from(clickhouse.database.as_str()),
        }
    }

    pub fn backend_name(&self) -> &str {
        &self.backend_name
    }

    pub fn repository(&self) -> &Arc<dyn ConversationRepository> {
        &self.repository
    }

    pub fn clickhouse_url(&self) -> &str {
        &self.clickhouse_url
    }

    pub fn clickhouse_database(&self) -> &str {
        &self.clickhouse_database
    }
}

type SharedInitialization = std::result::Result<Arc<BackendRepository>, Arc<str>>;

struct BuildAttempt {
    result: watch::Sender<Option<SharedInitialization>>,
    lifecycle: StdMutex<BuildAttemptLifecycle>,
}

struct BuildAttemptLifecycle {
    accepting_waiters: bool,
    waiter_count: usize,
    build_abort: Option<tokio::task::AbortHandle>,
}

impl BuildAttempt {
    fn new() -> Self {
        let (result, _) = watch::channel(None);
        Self {
            result,
            lifecycle: StdMutex::new(BuildAttemptLifecycle {
                accepting_waiters: true,
                waiter_count: 0,
                build_abort: None,
            }),
        }
    }

    fn try_waiter(self: &Arc<Self>) -> Option<BuildWaiter> {
        let mut lifecycle = self.lifecycle.lock().expect("build attempt lock poisoned");
        if !lifecycle.accepting_waiters {
            return None;
        }
        lifecycle.waiter_count += 1;
        Some(BuildWaiter {
            attempt: self.clone(),
        })
    }

    fn set_build_abort(&self, build_abort: tokio::task::AbortHandle) {
        let mut lifecycle = self.lifecycle.lock().expect("build attempt lock poisoned");
        debug_assert!(lifecycle.build_abort.is_none());
        if lifecycle.accepting_waiters {
            lifecycle.build_abort = Some(build_abort);
        } else {
            build_abort.abort();
        }
    }

    fn release_waiter(&self) {
        let build_abort = {
            let mut lifecycle = self.lifecycle.lock().expect("build attempt lock poisoned");
            debug_assert!(lifecycle.waiter_count > 0);
            lifecycle.waiter_count -= 1;
            if lifecycle.waiter_count == 0 {
                lifecycle.accepting_waiters = false;
                lifecycle.build_abort.take()
            } else {
                None
            }
        };
        if let Some(build_abort) = build_abort {
            build_abort.abort();
        }
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.lifecycle
            .lock()
            .expect("build attempt lock poisoned")
            .waiter_count
    }
}

struct BuildWaiter {
    attempt: Arc<BuildAttempt>,
}

impl BuildWaiter {
    async fn wait(&self) -> Result<Arc<BackendRepository>> {
        let mut result = self.attempt.result.subscribe();
        loop {
            if let Some(result) = result.borrow_and_update().clone() {
                return result.map_err(|message| anyhow!(message.to_string()));
            }
            result
                .changed()
                .await
                .map_err(|_| anyhow!("backend repository initialization ended without a result"))?;
        }
    }
}

impl Drop for BuildWaiter {
    fn drop(&mut self) {
        self.attempt.release_waiter();
    }
}

enum SlotState {
    Empty,
    Building(Arc<BuildAttempt>),
    Ready(Arc<BackendRepository>),
}

struct BackendSlot {
    backend_name: Arc<str>,
    clickhouse: ClickHouseConfig,
    checked: bool,
    query_runtime: QueryRuntime,
    state: Mutex<SlotState>,
}

impl BackendSlot {
    fn new(
        backend_name: String,
        clickhouse: ClickHouseConfig,
        checked: bool,
        query_runtime: QueryRuntime,
        preloaded: Option<Arc<dyn ConversationRepository>>,
    ) -> Self {
        let backend_name: Arc<str> = Arc::from(backend_name);
        let state = match preloaded {
            Some(repository) => SlotState::Ready(Arc::new(BackendRepository::new(
                backend_name.clone(),
                repository,
                &clickhouse,
            ))),
            None => SlotState::Empty,
        };
        Self {
            backend_name,
            clickhouse,
            checked,
            query_runtime,
            state: Mutex::new(state),
        }
    }

    async fn repository(
        self: &Arc<Self>,
        repo_config: RepoConfig,
        user_agent: Arc<str>,
    ) -> Result<Arc<BackendRepository>> {
        let waiter = {
            let mut state = self.state.lock().await;
            match &*state {
                SlotState::Ready(repository) => return Ok(repository.clone()),
                SlotState::Building(attempt) => match attempt.try_waiter() {
                    Some(waiter) => waiter,
                    None => self.start_build(&mut state, repo_config, user_agent),
                },
                SlotState::Empty => self.start_build(&mut state, repo_config, user_agent),
            }
        };

        waiter.wait().await
    }

    fn start_build(
        self: &Arc<Self>,
        state: &mut SlotState,
        repo_config: RepoConfig,
        user_agent: Arc<str>,
    ) -> BuildWaiter {
        let attempt = Arc::new(BuildAttempt::new());
        let waiter = attempt
            .try_waiter()
            .expect("a new build attempt accepts its initiating waiter");
        *state = SlotState::Building(attempt.clone());

        let build_slot = self.clone();
        let build = tokio::spawn(async move { build_slot.build(repo_config, &user_agent).await });
        attempt.set_build_abort(build.abort_handle());

        let slot = self.clone();
        let published_attempt = attempt.clone();
        tokio::spawn(async move {
            let result = match build.await {
                Ok(result) => result.map_err(|error| Arc::<str>::from(error.to_string())),
                Err(error) => Err(Arc::<str>::from(format!(
                    "backend repository initialization task failed: {error}"
                ))),
            };

            // Publish the slot state before waking waiters. Otherwise the last
            // waiter could return and retire this attempt while it still
            // appears to be building, allowing a racing caller to duplicate a
            // successful initialization.
            let mut state = slot.state.lock().await;
            let is_current_attempt = matches!(
                &*state,
                SlotState::Building(current) if Arc::ptr_eq(current, &published_attempt)
            );
            if is_current_attempt {
                *state = match &result {
                    Ok(repository) => SlotState::Ready(repository.clone()),
                    Err(_) => SlotState::Empty,
                };
            }
            drop(state);

            published_attempt.result.send_replace(Some(result));
        });

        waiter
    }

    async fn build(
        &self,
        repo_config: RepoConfig,
        user_agent: &str,
    ) -> Result<Arc<BackendRepository>> {
        let repository = if self.checked {
            build_checked_clickhouse_repository_with_user_agent(
                &self.backend_name,
                self.clickhouse.clone(),
                repo_config,
                self.query_runtime.clone(),
                user_agent,
            )
            .await?
        } else {
            build_clickhouse_repository_with_user_agent(
                self.clickhouse.clone(),
                repo_config,
                self.query_runtime.clone(),
                user_agent,
            )?
        };

        Ok(Arc::new(BackendRepository::new(
            self.backend_name.clone(),
            repository,
            &self.clickhouse,
        )))
    }
}

/// Lazy repository factory and cwd router shared by daemon read surfaces.
///
/// One slot is predeclared for every normalized backend. The default slot is
/// constructed without a remote schema handshake because Moraine owns and
/// migrates it. Every named slot is checked exactly once per successful
/// initialization. Concurrent callers share an in-flight result while at least
/// one caller is still waiting. An abandoned attempt is cancelled once its last
/// waiter disappears. Failures are not cached, so a later call retries.
pub struct BackendRepositoryRouter {
    config: Arc<AppConfig>,
    repo_config: RepoConfig,
    user_agent: Arc<str>,
    query_runtime: QueryRuntime,
    slots: BTreeMap<String, Arc<BackendSlot>>,
}

impl BackendRepositoryRouter {
    pub fn new(
        config: Arc<AppConfig>,
        repo_config: RepoConfig,
        query_runtime: QueryRuntime,
        user_agent: impl Into<Arc<str>>,
    ) -> Result<Self> {
        Self::new_inner(
            config,
            repo_config,
            user_agent.into(),
            query_runtime,
            BTreeMap::new(),
        )
    }

    /// Construct a router with already-built repositories for consumer tests.
    /// Production composition roots must use [`Self::new`].
    #[doc(hidden)]
    pub fn from_preloaded_for_testing(
        config: Arc<AppConfig>,
        query_runtime: QueryRuntime,
        repositories: impl IntoIterator<Item = (String, Arc<dyn ConversationRepository>)>,
    ) -> Result<Self> {
        Self::new_inner(
            config,
            RepoConfig::default(),
            Arc::from("moraine-conversations-test"),
            query_runtime,
            repositories.into_iter().collect(),
        )
    }

    fn new_inner(
        config: Arc<AppConfig>,
        repo_config: RepoConfig,
        user_agent: Arc<str>,
        query_runtime: QueryRuntime,
        mut preloaded: BTreeMap<String, Arc<dyn ConversationRepository>>,
    ) -> Result<Self> {
        if !config.backends.contains_key(DEFAULT_BACKEND_NAME) {
            return Err(anyhow!(
                "normalized app config is missing the '{}' backend",
                DEFAULT_BACKEND_NAME
            ));
        }

        let mut slots = BTreeMap::new();
        for (backend_name, clickhouse) in &config.backends {
            let repository = preloaded.remove(backend_name);
            slots.insert(
                backend_name.clone(),
                Arc::new(BackendSlot::new(
                    backend_name.clone(),
                    clickhouse.clone(),
                    backend_name != DEFAULT_BACKEND_NAME,
                    query_runtime.clone(),
                    repository,
                )),
            );
        }
        if let Some(unknown) = preloaded.keys().next() {
            return Err(anyhow!(
                "preloaded repository names unconfigured backend '{unknown}'"
            ));
        }

        Ok(Self {
            config,
            repo_config,
            user_agent,
            query_runtime,
            slots,
        })
    }

    pub fn query_runtime(&self) -> QueryRuntime {
        self.query_runtime.clone()
    }

    pub async fn default_repository(&self) -> Result<Arc<BackendRepository>> {
        self.repository_for_backend(DEFAULT_BACKEND_NAME).await
    }

    /// Select a repository for a process/project cwd.
    ///
    /// Ordered home-config routes win over the nearest repo `.moraine.toml`
    /// reference. A route that names `default`, an unknown repo reference, no
    /// route, a missing cwd, or a blank cwd all select the default repository.
    /// A matching home route owns the decision even when its backend name is
    /// unknown; it never falls through to the repo file.
    pub async fn repository_for_project_dir(
        &self,
        project_dir: Option<&str>,
    ) -> Result<Arc<BackendRepository>> {
        self.repository_for_project_dir_with_lookup(project_dir, |dir| {
            moraine_config::find_repo_backend_ref(dir)
        })
        .await
    }

    async fn repository_for_project_dir_with_lookup<F>(
        &self,
        project_dir: Option<&str>,
        repo_lookup: F,
    ) -> Result<Arc<BackendRepository>>
    where
        F: FnOnce(String) -> Option<String> + Send + 'static,
    {
        // The normal single-backend install does no filesystem walk.
        if self.slots.len() == 1 {
            return self.default_repository().await;
        }

        let Some(project_dir) = project_dir.map(str::trim).filter(|dir| !dir.is_empty()) else {
            return self.default_repository().await;
        };

        // A matching home route decides the name even when validation below
        // fails. First match wins; do not consult a repo file for that cwd.
        let backend_name = if let Some(route) = self.config.route_for_dir(project_dir) {
            Some(route.backend.clone())
        } else {
            let project_dir = project_dir.to_string();
            tokio::task::spawn_blocking(move || repo_lookup(project_dir))
                .await
                .context("repo backend reference lookup task failed")?
        };

        let Some(backend_name) = backend_name else {
            return self.default_repository().await;
        };
        if backend_name == DEFAULT_BACKEND_NAME {
            return self.default_repository().await;
        }
        if self.slots.contains_key(&backend_name) {
            return self.repository_for_backend(&backend_name).await;
        }

        warn!(
            backend = %backend_name,
            cwd = project_dir,
            "route names a backend with no [backends.{}] entry in the home config; serving against the default backend",
            backend_name
        );
        self.default_repository().await
    }

    async fn repository_for_backend(&self, backend_name: &str) -> Result<Arc<BackendRepository>> {
        let slot = self
            .slots
            .get(backend_name)
            .ok_or_else(|| anyhow!("backend '{backend_name}' is not configured"))?;
        slot.repository(self.repo_config.clone(), self.user_agent.clone())
            .await
    }
}

/// Build a named ClickHouse repository after enforcing the read-only remote
/// schema policy.
///
/// The same attributed client performs the skew probe and is then moved into
/// the repository. Named backends are never migrated here.
async fn build_checked_clickhouse_repository_with_user_agent(
    backend_name: &str,
    clickhouse: ClickHouseConfig,
    config: RepoConfig,
    query_runtime: QueryRuntime,
    user_agent: impl AsRef<str>,
) -> Result<Arc<dyn ConversationRepository>> {
    let allow_newer_server = clickhouse.allow_newer_server;
    let client = ClickHouseClient::new_with_runtime_and_user_agent(
        clickhouse,
        query_runtime.clone(),
        user_agent,
    )
    .with_context(|| format!("backend '{backend_name}': failed to construct ClickHouse client"))?;
    let owner = QueryOwner::new(&query_runtime, QueryWorkload::Internal)
        .with_context(|| format!("backend '{backend_name}': failed to own schema handshake"))?;
    let skew = owner.scope(client.schema_skew()).await.with_context(|| {
        format!("backend '{backend_name}': schema handshake failed (is the server reachable?)")
    })?;
    enforce_remote_schema_policy(backend_name, &skew, allow_newer_server)?;

    Ok(Arc::new(ClickHouseConversationRepository::new(
        client, config,
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use axum::{
        extract::{Query, State},
        http::{HeaderMap, Method, StatusCode},
        routing::get,
        Router,
    };
    use moraine_clickhouse::bundled_migrations;
    use moraine_config::{RouteConfig, ROUTE_MODE_MIRROR};
    use serde_json::json;
    use tokio::sync::{Barrier, Notify};

    use super::*;
    use crate::InMemoryConversationRepository;

    #[derive(Clone)]
    struct CapturedRequest {
        method: Method,
        query: String,
        query_id: Option<String>,
        user_agent: Option<String>,
    }

    struct FirstRequestBlock {
        reached: Arc<Barrier>,
        release: Arc<Notify>,
    }

    struct SchemaServerState {
        ledger_exists: bool,
        versions: Vec<String>,
        fail_first: AtomicBool,
        requests: AtomicUsize,
        cleanup_polls: AtomicUsize,
        captured: StdMutex<Vec<CapturedRequest>>,
        first_request_block: Option<FirstRequestBlock>,
    }

    async fn spawn_schema_server(
        ledger_exists: bool,
        versions: Vec<String>,
        fail_first: bool,
        first_request_block: Option<FirstRequestBlock>,
    ) -> (String, Arc<SchemaServerState>) {
        async fn handler(
            State(state): State<Arc<SchemaServerState>>,
            method: Method,
            Query(params): Query<HashMap<String, String>>,
            headers: HeaderMap,
        ) -> (StatusCode, String) {
            let query = params.get("query").cloned().unwrap_or_default();
            if query.contains("FROM system.workloads") {
                return (StatusCode::OK, "0".to_string());
            }
            let request_index = state.requests.fetch_add(1, Ordering::SeqCst);
            state
                .captured
                .lock()
                .expect("captured request lock")
                .push(CapturedRequest {
                    method,
                    query: query.clone(),
                    query_id: params.get("query_id").cloned(),
                    user_agent: headers
                        .get("user-agent")
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned),
                });

            if request_index == 0 {
                if let Some(block) = &state.first_request_block {
                    block.reached.wait().await;
                    block.release.notified().await;
                }
            }
            if state.fail_first.swap(false, Ordering::SeqCst) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "temporary schema probe failure".to_string(),
                );
            }

            if query.starts_with("KILL QUERY") {
                return (StatusCode::OK, String::new());
            }
            if query.contains("FROM system.processes") {
                let body = if state.cleanup_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    query
                        .split('\'')
                        .nth(1)
                        .map(|query_id| json!({"query_id": query_id}).to_string())
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                return (StatusCode::OK, body);
            }
            if query.contains("FROM system.tables") {
                return (
                    StatusCode::OK,
                    json!({"data": [{"exists": u8::from(state.ledger_exists)}]}).to_string(),
                );
            }
            if query.contains("schema_migrations GROUP BY version") {
                let rows = state
                    .versions
                    .iter()
                    .map(|version| json!({"version": version}))
                    .collect::<Vec<_>>();
                return (StatusCode::OK, json!({"data": rows}).to_string());
            }

            (
                StatusCode::BAD_REQUEST,
                format!("unexpected query: {query}"),
            )
        }

        let state = Arc::new(SchemaServerState {
            ledger_exists,
            versions,
            fail_first: AtomicBool::new(fail_first),
            requests: AtomicUsize::new(0),
            cleanup_polls: AtomicUsize::new(0),
            captured: StdMutex::new(Vec::new()),
            first_request_block,
        });
        let app = Router::new()
            .route("/", get(handler).post(handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind schema server");
        let address = listener.local_addr().expect("schema server address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{address}"), state)
    }

    fn bundled_versions() -> Vec<String> {
        bundled_migrations()
            .into_iter()
            .map(|migration| migration.version.to_string())
            .collect()
    }

    fn data_request_count(state: &SchemaServerState) -> usize {
        state
            .captured
            .lock()
            .expect("captured request lock")
            .iter()
            .filter(|request| {
                !request.query.starts_with("KILL QUERY")
                    && !request.query.contains("FROM system.processes")
            })
            .count()
    }

    fn clickhouse_config(url: String, allow_newer_server: bool) -> ClickHouseConfig {
        ClickHouseConfig {
            url,
            database: "moraine_team".to_string(),
            timeout_seconds: 2.0,
            allow_newer_server,
            ..ClickHouseConfig::default()
        }
    }

    fn routed_config(url: String, allow_newer_server: bool) -> Arc<AppConfig> {
        let mut config = AppConfig::default();
        config.backends.insert(
            "team-ch".to_string(),
            clickhouse_config(url, allow_newer_server),
        );
        config.routes.push(RouteConfig {
            dir: "/work/team/**".to_string(),
            backend: "team-ch".to_string(),
            mode: ROUTE_MODE_MIRROR.to_string(),
        });
        Arc::new(config)
    }

    fn preloaded_router(config: Arc<AppConfig>) -> BackendRepositoryRouter {
        let default: Arc<dyn ConversationRepository> =
            Arc::new(InMemoryConversationRepository::new(RepoConfig::default()));
        let named: Arc<dyn ConversationRepository> =
            Arc::new(InMemoryConversationRepository::new(RepoConfig::default()));
        BackendRepositoryRouter::from_preloaded_for_testing(
            config,
            QueryRuntime::new(),
            [
                (DEFAULT_BACKEND_NAME.to_string(), default),
                ("team-ch".to_string(), named),
            ],
        )
        .expect("preloaded router")
    }

    #[tokio::test]
    async fn checked_factory_accepts_clean_schema_and_only_reads() {
        let (url, state) = spawn_schema_server(true, bundled_versions(), false, None).await;
        let repository = build_checked_clickhouse_repository_with_user_agent(
            "team-ch",
            clickhouse_config(url, false),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .await
        .expect("clean named backend");

        assert_eq!(
            repository.config().max_results,
            RepoConfig::default().max_results
        );
        let captured = state.captured.lock().expect("captured request lock");
        assert_eq!(captured.len(), 2);
        assert!(captured
            .iter()
            .all(|request| request.query.trim_start().starts_with("SELECT")));
        assert!(captured
            .iter()
            .all(|request| request.method == Method::POST));
        assert!(captured
            .iter()
            .all(|request| { request.user_agent.as_deref() == Some("moraine-backend/test") }));
        let query_ids = captured
            .iter()
            .map(|request| request.query_id.as_deref().expect("owned schema query"))
            .collect::<Vec<_>>();
        let logical_id = query_ids[0]
            .rsplit_once('-')
            .expect("schema child sequence")
            .0;
        assert!(logical_id.starts_with("moraine-internal-"));
        assert!(query_ids
            .iter()
            .all(|query_id| query_id.starts_with(logical_id)));
        assert_ne!(query_ids[0], query_ids[1]);
    }

    #[tokio::test]
    async fn checked_factory_rejects_older_schema() {
        let mut versions = bundled_versions();
        let missing = versions.pop().expect("at least one bundled migration");
        let (url, _) = spawn_schema_server(true, versions, false, None).await;
        let error = build_checked_clickhouse_repository_with_user_agent(
            "team-ch",
            clickhouse_config(url, true),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .await
        .err()
        .expect("older backend must be rejected");

        let message = format!("{error:#}");
        assert!(message.contains("team-ch"));
        assert!(message.contains(&missing));
        assert!(message.contains("never migrates"));
    }

    #[tokio::test]
    async fn checked_factory_rejects_newer_schema_without_opt_in() {
        let mut versions = bundled_versions();
        versions.push("999".to_string());
        let (url, _) = spawn_schema_server(true, versions, false, None).await;
        let error = build_checked_clickhouse_repository_with_user_agent(
            "team-ch",
            clickhouse_config(url, false),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .await
        .err()
        .expect("newer backend must require opt-in");

        let message = format!("{error:#}");
        assert!(message.contains("999"));
        assert!(message.contains("allow_newer_server"));
    }

    #[tokio::test]
    async fn checked_factory_allows_newer_schema_with_opt_in() {
        let mut versions = bundled_versions();
        versions.push("999".to_string());
        let (url, _) = spawn_schema_server(true, versions, false, None).await;
        build_checked_clickhouse_repository_with_user_agent(
            "team-ch",
            clickhouse_config(url, true),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .await
        .expect("allow_newer_server accepts a server-ahead ledger");
    }

    #[tokio::test]
    async fn checked_factory_does_not_create_a_missing_ledger() {
        let (url, state) = spawn_schema_server(false, Vec::new(), false, None).await;
        let error = build_checked_clickhouse_repository_with_user_agent(
            "team-ch",
            clickhouse_config(url, false),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .await
        .err()
        .expect("missing ledger means server is behind");
        assert!(error.to_string().contains("behind"));

        let captured = state.captured.lock().expect("captured request lock");
        assert_eq!(captured.len(), 1);
        assert!(captured[0].query.starts_with("SELECT"));
        assert!(!captured[0].query.contains("CREATE"));
        assert!(!captured[0].query.contains("INSERT"));
    }

    #[tokio::test]
    async fn default_slot_is_lazy_and_unchecked() {
        let mut config = AppConfig::default();
        config.backends.get_mut(DEFAULT_BACKEND_NAME).unwrap().url =
            "http://127.0.0.1:1".to_string();
        let router = BackendRepositoryRouter::new(
            Arc::new(config),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .expect("router");

        let first = router
            .default_repository()
            .await
            .expect("default construction does not probe schema");
        let second = router.default_repository().await.expect("cached default");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.backend_name(), DEFAULT_BACKEND_NAME);
        assert_eq!(first.clickhouse_url(), "http://127.0.0.1:1");
        assert_eq!(
            first.clickhouse_database(),
            ClickHouseConfig::default().database
        );
        assert!(Arc::ptr_eq(first.repository(), second.repository()));
        let runtime = router.query_runtime();
        let owner =
            QueryOwner::new(&runtime, QueryWorkload::Internal).expect("router runtime owner");
        assert_eq!(runtime.active_owner_count(), 1);
        owner.scope(async {}).await;
        assert_eq!(runtime.active_owner_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn named_slot_shares_failure_then_retries_and_caches_success() {
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Notify::new());
        let (url, state) = spawn_schema_server(
            true,
            bundled_versions(),
            true,
            Some(FirstRequestBlock {
                reached: reached.clone(),
                release: release.clone(),
            }),
        )
        .await;
        let router = Arc::new(
            BackendRepositoryRouter::new(
                routed_config(url, false),
                RepoConfig::default(),
                QueryRuntime::new(),
                "moraine-backend/test",
            )
            .expect("router"),
        );

        let start = Arc::new(Barrier::new(9));
        let mut callers = Vec::new();
        for _ in 0..8 {
            let router = router.clone();
            let start = start.clone();
            callers.push(tokio::spawn(async move {
                start.wait().await;
                router
                    .repository_for_project_dir(Some("/work/team/project"))
                    .await
            }));
        }
        start.wait().await;
        reached.wait().await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        release.notify_one();

        for caller in callers {
            let error = caller
                .await
                .expect("caller task")
                .err()
                .expect("shared attempt must fail");
            let message = error.to_string();
            assert!(message.contains("schema handshake failed"));
            assert!(!message.contains("temporary schema probe failure"));
        }
        assert_eq!(data_request_count(&state), 1);

        let recovered = router
            .repository_for_project_dir(Some("/work/team/project"))
            .await
            .expect("later call retries failed slot");
        let cached = router
            .repository_for_project_dir(Some("/work/team/project"))
            .await
            .expect("successful slot is cached");
        assert!(Arc::ptr_eq(&recovered, &cached));
        assert_eq!(data_request_count(&state), 3);
    }

    async fn wait_for_owner_count(runtime: &QueryRuntime, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while runtime.active_owner_count() != expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("query owner count did not converge");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn named_slot_cancels_initialization_when_its_only_waiter_is_dropped() {
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Notify::new());
        let (url, _state) = spawn_schema_server(
            true,
            bundled_versions(),
            false,
            Some(FirstRequestBlock {
                reached: reached.clone(),
                release: release.clone(),
            }),
        )
        .await;
        let query_runtime = QueryRuntime::new();
        let router = Arc::new(
            BackendRepositoryRouter::new(
                routed_config(url, false),
                RepoConfig::default(),
                query_runtime.clone(),
                "moraine-backend/test",
            )
            .expect("router"),
        );

        let caller = {
            let router = router.clone();
            tokio::spawn(async move {
                router
                    .repository_for_project_dir(Some("/work/team/project"))
                    .await
            })
        };
        reached.wait().await;
        assert_eq!(query_runtime.active_owner_count(), 1);

        caller.abort();
        match caller.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("caller must be aborted"),
        }
        wait_for_owner_count(&query_runtime, 0).await;

        // Let the test server retire a handler if its HTTP stack retained the
        // disconnected request. The abandoned build itself is already gone.
        release.notify_waiters();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn named_slot_keeps_shared_initialization_for_a_remaining_waiter() {
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Notify::new());
        let (url, state) = spawn_schema_server(
            true,
            bundled_versions(),
            false,
            Some(FirstRequestBlock {
                reached: reached.clone(),
                release: release.clone(),
            }),
        )
        .await;
        let query_runtime = QueryRuntime::new();
        let router = Arc::new(
            BackendRepositoryRouter::new(
                routed_config(url, false),
                RepoConfig::default(),
                query_runtime.clone(),
                "moraine-backend/test",
            )
            .expect("router"),
        );

        let first = {
            let router = router.clone();
            tokio::spawn(async move {
                router
                    .repository_for_project_dir(Some("/work/team/project"))
                    .await
            })
        };
        reached.wait().await;
        let second = {
            let router = router.clone();
            tokio::spawn(async move {
                router
                    .repository_for_project_dir(Some("/work/team/project"))
                    .await
            })
        };

        let slot = router.slots.get("team-ch").expect("named slot").clone();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let waiter_count = {
                    let state = slot.state.lock().await;
                    match &*state {
                        SlotState::Building(attempt) => attempt.waiter_count(),
                        _ => 0,
                    }
                };
                if waiter_count == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second waiter must join shared initialization");

        first.abort();
        match first.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("first caller must be aborted"),
        }
        assert_eq!(query_runtime.active_owner_count(), 1);

        release.notify_one();
        let repository = second
            .await
            .expect("second caller task")
            .expect("remaining waiter keeps initialization alive");
        assert_eq!(repository.backend_name(), "team-ch");
        wait_for_owner_count(&query_runtime, 0).await;
        assert_eq!(data_request_count(&state), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_build_attempt_releases_waiters_and_retries() {
        let mut config = (*routed_config("http://127.0.0.1:1".to_string(), false)).clone();
        config
            .backends
            .get_mut("team-ch")
            .expect("named backend")
            .timeout_seconds = f64::INFINITY;
        let router = BackendRepositoryRouter::new(
            Arc::new(config),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .expect("router");

        for _ in 0..2 {
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                router.repository_for_project_dir(Some("/work/team/project")),
            )
            .await
            .expect("failed build attempt must publish instead of hanging");
            let error = match result {
                Ok(_) => panic!("invalid timeout must fail construction"),
                Err(error) => error,
            };
            assert!(error
                .to_string()
                .contains("failed to construct ClickHouse client"));
        }
    }

    fn panicking_lookup(dir: String) -> Option<String> {
        panic!("repo lookup must not be consulted (cwd: {dir})")
    }

    #[tokio::test]
    async fn route_skips_lookup_when_only_default_backend() {
        let config = Arc::new(AppConfig::default());
        let default: Arc<dyn ConversationRepository> =
            Arc::new(InMemoryConversationRepository::new(RepoConfig::default()));
        let router = BackendRepositoryRouter::from_preloaded_for_testing(
            config,
            QueryRuntime::new(),
            [(DEFAULT_BACKEND_NAME.to_string(), default)],
        )
        .expect("default-only router");

        let selected = router
            .repository_for_project_dir_with_lookup(Some("/work/team/x"), panicking_lookup)
            .await
            .expect("default selection");
        assert_eq!(selected.backend_name(), DEFAULT_BACKEND_NAME);
    }

    #[tokio::test]
    async fn route_prefers_home_route_over_repo_file() {
        let router = preloaded_router(routed_config("http://team.invalid".to_string(), false));
        let selected = router
            .repository_for_project_dir_with_lookup(Some("/work/team/x"), panicking_lookup)
            .await
            .expect("home route selection");
        assert_eq!(selected.backend_name(), "team-ch");
    }

    #[tokio::test]
    async fn route_falls_back_to_repo_reference() {
        let router = preloaded_router(routed_config("http://team.invalid".to_string(), false));
        let selected = router
            .repository_for_project_dir_with_lookup(Some("/elsewhere/proj"), |dir| {
                assert_eq!(dir, "/elsewhere/proj");
                Some("team-ch".to_string())
            })
            .await
            .expect("repo route selection");
        assert_eq!(selected.backend_name(), "team-ch");
    }

    #[tokio::test]
    async fn route_unknown_repo_name_uses_default() {
        let router = preloaded_router(routed_config("http://team.invalid".to_string(), false));
        let selected = router
            .repository_for_project_dir_with_lookup(Some("/elsewhere/proj"), |_| {
                Some("ghost".to_string())
            })
            .await
            .expect("unknown repo route falls back");
        assert_eq!(selected.backend_name(), DEFAULT_BACKEND_NAME);
    }

    #[tokio::test]
    async fn route_treats_default_reference_as_no_route() {
        let mut config = (*routed_config("http://team.invalid".to_string(), false)).clone();
        config.routes.insert(
            0,
            RouteConfig {
                dir: "/work/local/**".to_string(),
                backend: DEFAULT_BACKEND_NAME.to_string(),
                mode: ROUTE_MODE_MIRROR.to_string(),
            },
        );
        let router = preloaded_router(Arc::new(config));

        let home_selected = router
            .repository_for_project_dir_with_lookup(Some("/work/local/x"), panicking_lookup)
            .await
            .expect("explicit default home route");
        assert_eq!(home_selected.backend_name(), DEFAULT_BACKEND_NAME);
        let repo_selected = router
            .repository_for_project_dir_with_lookup(Some("/elsewhere/proj"), |_| {
                Some(DEFAULT_BACKEND_NAME.to_string())
            })
            .await
            .expect("explicit default repo route");
        assert_eq!(repo_selected.backend_name(), DEFAULT_BACKEND_NAME);
    }

    #[tokio::test]
    async fn route_unknown_home_name_does_not_fall_through() {
        let mut config = (*routed_config("http://team.invalid".to_string(), false)).clone();
        config.routes[0].backend = "ghost".to_string();
        let router = preloaded_router(Arc::new(config));
        let selected = router
            .repository_for_project_dir_with_lookup(Some("/work/team/x"), panicking_lookup)
            .await
            .expect("unknown home route falls back");
        assert_eq!(selected.backend_name(), DEFAULT_BACKEND_NAME);
    }

    #[tokio::test]
    async fn route_ignores_missing_or_blank_cwd() {
        let router = preloaded_router(routed_config("http://team.invalid".to_string(), false));
        for cwd in [None, Some(""), Some("   ")] {
            let selected = router
                .repository_for_project_dir_with_lookup(cwd, panicking_lookup)
                .await
                .expect("blank cwd uses default");
            assert_eq!(selected.backend_name(), DEFAULT_BACKEND_NAME);
        }
    }

    #[tokio::test]
    async fn route_lookup_runs_on_blocking_pool() {
        let router = preloaded_router(routed_config("http://team.invalid".to_string(), false));
        let async_thread = std::thread::current().id();
        let selected = router
            .repository_for_project_dir_with_lookup(Some("/elsewhere/proj"), move |_| {
                assert_ne!(std::thread::current().id(), async_thread);
                Some("team-ch".to_string())
            })
            .await
            .expect("blocking lookup route");
        assert_eq!(selected.backend_name(), "team-ch");
    }

    #[tokio::test]
    async fn route_metadata_redacts_credentials_and_query_parameters() {
        let router = preloaded_router(routed_config(
            "https://user:secret@team.invalid:8443\\signed\\secret?token=secret#fragment"
                .to_string(),
            false,
        ));
        let selected = router
            .repository_for_project_dir(Some("/work/team/project"))
            .await
            .expect("preloaded named route");
        assert_eq!(selected.backend_name(), "team-ch");
        assert_eq!(selected.clickhouse_url(), "https://team.invalid:8443");
        assert_eq!(selected.clickhouse_database(), "moraine_team");
    }

    #[tokio::test]
    async fn later_success_is_reused_without_an_expiry() {
        let (url, state) = spawn_schema_server(true, bundled_versions(), false, None).await;
        let router = BackendRepositoryRouter::new(
            routed_config(url, false),
            RepoConfig::default(),
            QueryRuntime::new(),
            "moraine-backend/test",
        )
        .expect("router");
        let first = router
            .repository_for_project_dir(Some("/work/team/project"))
            .await
            .expect("first named repository");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = router
            .repository_for_project_dir(Some("/work/team/project"))
            .await
            .expect("cached named repository");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(first.repository(), second.repository()));
        assert_eq!(state.requests.load(Ordering::SeqCst), 2);
    }
}
