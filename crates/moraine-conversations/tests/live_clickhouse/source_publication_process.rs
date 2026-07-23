use super::OwnedDatabaseName;
use anyhow::{anyhow, bail, Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures_util::stream;
use moraine_clickhouse::ClickHouseClient;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use uuid::Uuid;

const INGEST_BINARY_ENV: &str = "MORAINE_LIVE_TEST_INGEST_BIN";
const INGEST_BINARY_ROOT: &str = "/opt/moraine/bin";
const PROCESS_SOURCE: &str = "source-publication-process";
const PROCESS_SESSION: &str = "source-publication-process-session";
const FIXTURE_LINES: u64 = 4;
const FIXTURE_EVENTS: u64 = 5;
const PROCESS_WAIT: Duration = Duration::from_secs(120);
const LEGACY_SESSION_PUBLICATION_COLUMNS: &str = "session_id, slot, generation, source_revision, \
    dirty_revision, first_event_time, last_event_time, total_turns, total_events, user_messages, \
    assistant_messages, tool_calls, tool_results, mode, first_event_uid, last_event_uid, \
    last_actor_role, title, source, harness, inference_provider, session_slug, session_summary, \
    completed, terminal_event_uid, origin_cwd";

#[derive(Clone, Debug, Eq, PartialEq)]
enum FaultTarget {
    ReplayingCheckpoint {
        generation: u32,
        last_line: u64,
    },
    GenerationStage {
        generation: u32,
        stage: GenerationStage,
    },
    SourceHead {
        generation: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GenerationStage {
    RawEvents,
    CanonicalAndSearch,
    EventLinks,
    ToolIo,
    IngestErrors,
    McpOpenEvents,
    McpOpenTurns,
    McpOpenHeader,
    McpOpenReadiness,
    FinalCheckpoint,
    SourceReadiness,
}

impl GenerationStage {
    const PRE_HEAD_MATRIX: [Self; 11] = [
        Self::RawEvents,
        Self::CanonicalAndSearch,
        Self::EventLinks,
        Self::ToolIo,
        Self::IngestErrors,
        Self::McpOpenEvents,
        Self::McpOpenTurns,
        Self::McpOpenHeader,
        Self::McpOpenReadiness,
        Self::FinalCheckpoint,
        Self::SourceReadiness,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::RawEvents => "raw-events",
            Self::CanonicalAndSearch => "canonical-events-and-search-mvs",
            Self::EventLinks => "event-links",
            Self::ToolIo => "tool-io",
            Self::IngestErrors => "ingest-errors",
            Self::McpOpenEvents => "mcp-open-events",
            Self::McpOpenTurns => "mcp-open-turns",
            Self::McpOpenHeader => "mcp-open-header",
            Self::McpOpenReadiness => "mcp-open-readiness",
            Self::FinalCheckpoint => "final-checkpoint",
            Self::SourceReadiness => "source-readiness",
        }
    }

    fn table(self) -> &'static str {
        match self {
            Self::RawEvents => "raw_events",
            Self::CanonicalAndSearch => "events",
            Self::EventLinks => "event_links",
            Self::ToolIo => "tool_io",
            Self::IngestErrors => "ingest_errors",
            Self::McpOpenEvents => "mcp_open_events",
            Self::McpOpenTurns => "mcp_open_turns",
            Self::McpOpenHeader => "mcp_open_publication_headers",
            Self::McpOpenReadiness => "mcp_open_generation_readiness",
            Self::FinalCheckpoint => "ingest_checkpoint_transitions",
            Self::SourceReadiness => "source_generation_publication_readiness",
        }
    }
}

impl FaultTarget {
    fn label(&self) -> String {
        match self {
            Self::ReplayingCheckpoint {
                generation,
                last_line,
            } => format!("replaying-checkpoint-g{generation}-line-{last_line}"),
            Self::GenerationStage { generation, stage } => {
                format!("{}-g{generation}", stage.label())
            }
            Self::SourceHead { generation } => format!("source-head-g{generation}"),
        }
    }

    fn matches(&self, query: &str, body: &[u8], source_file: &str) -> bool {
        let table = match self {
            Self::ReplayingCheckpoint { .. } => "ingest_checkpoint_transitions",
            Self::GenerationStage { stage, .. } => stage.table(),
            Self::SourceHead { .. } => "published_source_generations",
        };
        if !query_inserts_table(query, table) {
            return false;
        }

        if let Self::GenerationStage { generation, stage } = self {
            return match stage {
                GenerationStage::EventLinks | GenerationStage::ToolIo => {
                    json_each_rows(body).any(|row| {
                        row.get("source_name").and_then(Value::as_str) == Some(PROCESS_SOURCE)
                            && row.get("session_id").and_then(Value::as_str)
                                == Some(PROCESS_SESSION)
                    })
                }
                GenerationStage::McpOpenEvents | GenerationStage::McpOpenTurns => {
                    query.contains(PROCESS_SESSION)
                        && query.contains(source_file)
                        && query.contains(&format!("source_generation = {generation}"))
                }
                GenerationStage::McpOpenHeader | GenerationStage::McpOpenReadiness => {
                    query.contains(&candidate_publication_id(source_file, *generation))
                }
                GenerationStage::FinalCheckpoint => json_each_rows(body).any(|row| {
                    row_matches_owned_generation(&row, source_file, *generation)
                        && row.get("last_line").and_then(Value::as_u64) == Some(FIXTURE_LINES)
                        && row.get("lifecycle").and_then(Value::as_str) == Some("active")
                        && row.get("final_scan_complete").and_then(Value::as_u64) == Some(1)
                        && row.get("compatibility_prepared").and_then(Value::as_u64) == Some(1)
                }),
                GenerationStage::SourceReadiness => json_each_rows(body).any(|row| {
                    row_matches_owned_generation(&row, source_file, *generation)
                        && row.get("complete").and_then(Value::as_u64) == Some(1)
                        && row.get("compatibility_prepared").and_then(Value::as_u64) == Some(1)
                }),
                GenerationStage::RawEvents
                | GenerationStage::CanonicalAndSearch
                | GenerationStage::IngestErrors => json_each_rows(body)
                    .any(|row| row_matches_owned_generation(&row, source_file, *generation)),
            };
        }

        json_each_rows(body).any(|row| {
            row_matches_owned_generation(
                &row,
                source_file,
                match self {
                    Self::ReplayingCheckpoint { generation, .. }
                    | Self::SourceHead { generation } => *generation,
                    Self::GenerationStage { .. } => unreachable!("handled above"),
                },
            ) && match self {
                Self::ReplayingCheckpoint { last_line, .. } => {
                    row.get("last_line").and_then(Value::as_u64) == Some(*last_line)
                        && row.get("lifecycle").and_then(Value::as_str) == Some("replaying")
                }
                Self::SourceHead { .. } => true,
                Self::GenerationStage { .. } => unreachable!("handled above"),
            }
        })
    }
}

fn query_inserts_table(query: &str, table: &str) -> bool {
    let query = query.trim_start();
    query.starts_with("INSERT INTO")
        && (query.contains(&format!("`{table}`"))
            || query.contains(&format!(".{table}\n"))
            || query.contains(&format!(".{table} "))
            || query.contains(&format!(".{table}(")))
}

fn row_matches_owned_generation(row: &Value, source_file: &str, generation: u32) -> bool {
    row.get("source_name").and_then(Value::as_str) == Some(PROCESS_SOURCE)
        && row.get("source_file").and_then(Value::as_str) == Some(source_file)
        && row.get("source_generation").and_then(Value::as_u64) == Some(u64::from(generation))
}

fn json_each_rows(body: &[u8]) -> impl Iterator<Item = Value> + '_ {
    body.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_slice(line).ok())
}

#[derive(Default)]
struct FaultState {
    armed: Option<FaultTarget>,
    triggered: Option<FaultTarget>,
    blocked: bool,
    client_observed_loss: bool,
}

#[derive(Clone)]
struct FaultController {
    state: Arc<Mutex<FaultState>>,
    notify: Arc<Notify>,
    source_file: Arc<str>,
}

impl FaultController {
    fn new(source_file: &str) -> Self {
        Self {
            state: Arc::new(Mutex::new(FaultState::default())),
            notify: Arc::new(Notify::new()),
            source_file: Arc::from(source_file),
        }
    }

    fn arm(&self, target: FaultTarget) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fault-controller mutex poisoned"))?;
        if let Some(armed) = state.armed.as_ref() {
            bail!(
                "cannot arm {} while {} remains armed",
                target.label(),
                armed.label()
            );
        }
        if state.blocked {
            bail!("cannot arm a fault while the proxy remains blocked");
        }
        state.triggered = None;
        state.client_observed_loss = false;
        state.armed = Some(target);
        Ok(())
    }

    fn commit_if_matches(&self, query: &str, body: &[u8]) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(target) = state.armed.as_ref() else {
            return false;
        };
        if !target.matches(query, body, &self.source_file) {
            return false;
        }
        let target = state.armed.take().expect("armed target disappeared");
        state.triggered = Some(target);
        state.blocked = true;
        state.client_observed_loss = false;
        drop(state);
        self.notify.notify_one();
        true
    }

    fn reject_if_blocked(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return true;
        };
        if !state.blocked {
            return false;
        }
        state.client_observed_loss = true;
        drop(state);
        self.notify.notify_waiters();
        true
    }

    fn release_after_sigkill(&self, target: &FaultTarget) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fault-controller mutex poisoned"))?;
        if state.armed.is_some()
            || state.triggered.as_ref() != Some(target)
            || !state.blocked
            || !state.client_observed_loss
        {
            bail!(
                "cannot release proxy after {} without its committed, client-observed fault",
                target.label()
            );
        }
        state.blocked = false;
        Ok(())
    }

    fn mark_client_observed_loss(&self, target: &FaultTarget) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("fault-controller mutex poisoned"))?;
        if state.triggered.as_ref() != Some(target) || !state.blocked {
            bail!(
                "cannot record client observation before committed fault {}",
                target.label()
            );
        }
        state.client_observed_loss = true;
        Ok(())
    }

    async fn wait_for(&self, target: &FaultTarget, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| anyhow!("fault-controller mutex poisoned"))?;
                if state.triggered.as_ref() == Some(target) {
                    return Ok(());
                }
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .with_context(|| {
                    format!(
                        "timed out waiting for committed response drop at {}",
                        target.label()
                    )
                })?;
        }
    }
}

#[derive(Clone)]
struct ProxyState {
    upstream: Arc<str>,
    http: reqwest::Client,
    faults: FaultController,
}

struct CommitDropProxy {
    url: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}

impl CommitDropProxy {
    async fn start(upstream: &str, faults: FaultController) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind committed-response-drop proxy")?;
        let address = listener
            .local_addr()
            .context("failed to read committed-response-drop proxy address")?;
        let state = ProxyState {
            upstream: Arc::from(upstream.trim_end_matches('/')),
            http: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(15))
                .build()
                .context("failed to build committed-response-drop proxy client")?,
            faults,
        };
        let app = Router::new()
            .route("/", post(proxy_request))
            .with_state(state);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let server = axum::serve(listener, app).with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            });
            server
                .await
                .context("committed-response-drop proxy server failed")
        });
        Ok(Self {
            url: format!("http://{address}"),
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .take()
            .expect("proxy task is present")
            .await
            .context("committed-response-drop proxy task failed to join")??;
        Ok(())
    }
}

impl Drop for CommitDropProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn proxy_request(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if state.faults.reject_if_blocked() {
        return response_with_body(
            StatusCode::SERVICE_UNAVAILABLE,
            Body::from("committed-response-drop proxy is latched until SIGKILL"),
        );
    }
    let upstream_url = format!("{}{}", state.upstream, uri);
    let mut request = state
        .http
        .post(upstream_url)
        .header(CONTENT_LENGTH, body.len().to_string())
        .body(body.clone());
    for (name, value) in &headers {
        if matches!(
            name.as_str(),
            "host" | "content-length" | "connection" | "transfer-encoding"
        ) {
            continue;
        }
        request = request.header(name, value);
    }

    let upstream = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return response_with_body(
                StatusCode::BAD_GATEWAY,
                Body::from(format!("upstream ClickHouse request failed: {error}")),
            );
        }
    };
    let status = upstream.status();
    let upstream_body = match upstream.bytes().await {
        Ok(body) => body,
        Err(error) => {
            return response_with_body(
                StatusCode::BAD_GATEWAY,
                Body::from(format!("upstream ClickHouse response failed: {error}")),
            );
        }
    };

    let query = clickhouse_query(&uri, &body);
    if status.is_success() && state.faults.commit_if_matches(&query, &body) {
        // The upstream response has been read completely, so a synchronous
        // ClickHouse insert is committed. Erroring the downstream body makes
        // the production client observe transport loss instead of an ack.
        let dropped = stream::once(async {
            Err::<Bytes, std::io::Error>(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "live test dropped committed ClickHouse response",
            ))
        });
        return response_with_body(status, Body::from_stream(dropped));
    }

    response_with_body(status, Body::from(upstream_body))
}

fn response_with_body(status: StatusCode, body: Body) -> Response {
    Response::builder()
        .status(status)
        .body(body)
        .expect("static proxy response is valid")
}

fn clickhouse_query(uri: &Uri, body: &[u8]) -> String {
    let url_query = uri.query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find_map(|(key, value)| (key == "query").then(|| value.into_owned()))
    });
    url_query
        .filter(|query| !query.is_empty())
        .unwrap_or_else(|| String::from_utf8_lossy(body).into_owned())
}

struct OwnedTempTree {
    path: PathBuf,
}

impl OwnedTempTree {
    fn create() -> Result<Self> {
        let path = env::temp_dir().join(format!(
            "moraine-source-publication-process-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir(&path).with_context(|| {
            format!(
                "failed to create owned process-test root {}",
                path.display()
            )
        })?;
        Ok(Self { path })
    }
}

impl Drop for OwnedTempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct IngestProcess {
    child: Option<Child>,
    log_lines: Receiver<String>,
    log_tasks: Vec<thread::JoinHandle<()>>,
}

impl IngestProcess {
    fn spawn(binary: &Path, config: &Path) -> Result<Self> {
        let mut child = Command::new(binary)
            .arg("--config")
            .arg(config)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn production ingest binary {}",
                    binary.display()
                )
            })?;
        let stderr = child
            .stderr
            .take()
            .context("production ingest child stderr was not piped")?;
        let stdout = child
            .stdout
            .take()
            .context("production ingest child stdout was not piped")?;
        let (log_tx, log_lines) = mpsc::channel();
        let stdout_tx = log_tx.clone();
        let stdout_task = thread::spawn(move || {
            forward_process_logs(stdout, "stdout", stdout_tx);
        });
        let stderr_task = thread::spawn(move || {
            forward_process_logs(stderr, "stderr", log_tx);
        });
        Ok(Self {
            child: Some(child),
            log_lines,
            log_tasks: vec![stdout_task, stderr_task],
        })
    }

    fn ensure_running(&mut self) -> Result<()> {
        let child = self
            .child
            .as_mut()
            .context("production ingest child is absent")?;
        if let Some(status) = child
            .try_wait()
            .context("failed to inspect production ingest child")?
        {
            bail!("production ingest child exited unexpectedly: {status}");
        }
        Ok(())
    }

    fn sigkill(&mut self) -> Result<()> {
        let mut child = self
            .child
            .take()
            .context("production ingest child is absent")?;
        child
            .kill()
            .context("failed to SIGKILL production ingest child")?;
        let status = child
            .wait()
            .context("failed waiting for SIGKILLed production ingest child")?;
        self.join_logs()?;
        #[cfg(unix)]
        if status.signal() != Some(9) {
            bail!("production ingest child did not exit via SIGKILL: {status}");
        }
        Ok(())
    }

    fn drain_logs(&mut self) {
        while self.log_lines.try_recv().is_ok() {}
    }

    async fn wait_for_fault_log(
        &mut self,
        faults: &FaultController,
        target: &FaultTarget,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut recent = Vec::new();
        loop {
            self.ensure_running()?;
            loop {
                match self.log_lines.try_recv() {
                    Ok(line) => {
                        let observed = line
                            .contains(&format!("failed processing {PROCESS_SOURCE}:"))
                            || line.contains("flush failed; pausing sink intake");
                        if observed {
                            faults.mark_client_observed_loss(target)?;
                            return Ok(());
                        }
                        recent.push(line);
                        if recent.len() > 8 {
                            recent.remove(0);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        bail!(
                            "production ingest log streams closed before response-loss evidence at {}: {recent:?}",
                            target.label()
                        );
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "timed out waiting for production ingest to log response loss at {}: {recent:?}",
                    target.label()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn join_logs(&mut self) -> Result<()> {
        for task in self.log_tasks.drain(..) {
            task.join()
                .map_err(|_| anyhow!("production ingest log reader panicked"))?;
        }
        Ok(())
    }
}

impl Drop for IngestProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        for task in self.log_tasks.drain(..) {
            let _ = task.join();
        }
    }
}

fn forward_process_logs(
    stream: impl std::io::Read,
    stream_name: &str,
    sender: mpsc::Sender<String>,
) {
    for line in BufReader::new(stream).lines() {
        let Ok(line) = line else {
            break;
        };
        eprintln!("[production ingest {stream_name}] {line}");
        if sender.send(line).is_err() {
            break;
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct HeadRow {
    source_generation: u32,
    publication_revision: u64,
    operation_id: String,
}

#[derive(Debug, Deserialize)]
struct HistoryRow {
    history_count: u64,
    revision_count: u64,
    operation_count: u64,
    generation_count: u64,
    max_revision: u64,
    max_generation: u32,
}

#[derive(Debug, Deserialize)]
struct EventRow {
    source_generation: u32,
    text_content: String,
}

#[derive(Debug, Deserialize)]
struct LegacyPointerRow {
    slot: u8,
    generation: u64,
    source_revision: u64,
}

#[derive(Debug, Deserialize)]
struct SourceRevisionRow {
    source_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RequiredSourceHead {
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    publication_revision: u64,
}

#[derive(Debug, Deserialize)]
struct McpReadinessRow {
    candidate_publication_id: String,
    operation_id: String,
    affected_session_count: u64,
    prepared_session_count: u64,
    tombstone_count: u64,
    ready: u8,
    block_reason: String,
}

#[derive(Debug, Deserialize)]
struct CandidateHeaderRow {
    slot: u8,
    generation: u64,
    source_revision: u64,
    total_turns: u64,
    total_events: u64,
    tombstone: u8,
    required_source_heads: Vec<RequiredSourceHead>,
    required_heads_fingerprint: String,
    operation_id: String,
}

#[derive(Debug, Deserialize)]
struct CandidateChildCounts {
    event_count: u64,
    turn_count: u64,
}

#[derive(Debug, Deserialize)]
struct TransitionRow {
    inode: u64,
    source_generation: u32,
    last_offset: u64,
    last_line: u64,
    checkpoint_revision: u64,
    operation_id: String,
    lifecycle: String,
    scan_inode: u64,
    scan_boundary: u64,
    final_scan_complete: u8,
    block_reason: String,
    compatibility_prepared: u8,
    backend_caught_up: u8,
}

#[derive(Debug, Deserialize)]
struct ReadinessRow {
    readiness_revision: u64,
    checkpoint_revision: u64,
    operation_id: String,
    complete: u8,
    block_reason: String,
    compatibility_prepared: u8,
    backend_caught_up: u8,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    value: u64,
}

#[derive(Debug, Deserialize)]
struct CanonicalSearchCounts {
    canonical_events: u64,
    search_documents: u64,
    search_postings: u64,
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn candidate_publication_id(source_file: &str, generation: u32) -> String {
    let mut hasher = Sha256::new();
    for value in ["", PROCESS_SOURCE, source_file] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(generation.to_le_bytes());
    format!("source-publication-{:x}", hasher.finalize())
}

async fn scalar_count(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    query: &str,
    label: &str,
) -> Result<u64> {
    clickhouse
        .query_rows::<CountRow>(query, Some(database.as_str()))
        .await
        .with_context(|| format!("failed to read {label}"))?
        .into_iter()
        .next()
        .map(|row| row.value)
        .with_context(|| format!("{label} returned no row"))
}

async fn mcp_child_generation(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    table: &str,
) -> Result<u64> {
    let query = format!(
        "SELECT toUInt64(if(count() = 0, 0, max(generation))) AS value \
         FROM `{}`.`{table}` WHERE session_id = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
    );
    scalar_count(
        clickhouse,
        database,
        &query,
        &format!("maximum {table} generation"),
    )
    .await
}

async fn stage_baseline(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    stage: GenerationStage,
) -> Result<u64> {
    match stage {
        GenerationStage::McpOpenEvents => {
            mcp_child_generation(clickhouse, database, "mcp_open_events").await
        }
        GenerationStage::McpOpenTurns => {
            mcp_child_generation(clickhouse, database, "mcp_open_turns").await
        }
        _ => Ok(0),
    }
}

async fn assert_stage_commit(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    generation: u32,
    stage: GenerationStage,
    baseline: u64,
) -> Result<()> {
    let owned_filter = format!(
        "source_host = '' AND source_name = {} AND source_file = {} AND source_generation = {}",
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
        generation,
    );
    match stage {
        GenerationStage::RawEvents | GenerationStage::IngestErrors => {
            let table = stage.table();
            let query = format!(
                "SELECT toUInt64(count()) AS value FROM `{}`.`{table}` \
                 WHERE {owned_filter} FORMAT JSONEachRow",
                database.as_str(),
            );
            if scalar_count(clickhouse, database, &query, stage.label()).await? == 0 {
                bail!(
                    "committed response loss at {} left no durable generation {generation} row",
                    stage.label()
                );
            }
        }
        GenerationStage::CanonicalAndSearch => {
            let query = format!(
                "SELECT \
                   toUInt64((SELECT count() FROM `{0}`.`events` FINAL WHERE {1})) AS canonical_events, \
                   toUInt64((SELECT count() FROM `{0}`.`search_documents` FINAL WHERE {1})) AS search_documents, \
                   toUInt64((SELECT count() FROM `{0}`.`search_postings` FINAL WHERE {1})) AS search_postings \
                 FORMAT JSONEachRow",
                database.as_str(),
                owned_filter,
            );
            let counts = clickhouse
                .query_rows::<CanonicalSearchCounts>(&query, Some(database.as_str()))
                .await
                .context("failed to inspect canonical/search MV durability")?
                .into_iter()
                .next()
                .context("canonical/search MV durability query returned no row")?;
            if counts.canonical_events == 0
                || counts.search_documents < counts.canonical_events
                || counts.search_postings == 0
            {
                bail!(
                    "events response loss did not durably commit canonical plus synchronous search projections: {counts:?}"
                );
            }
        }
        GenerationStage::EventLinks | GenerationStage::ToolIo => {
            let table = stage.table();
            let query = format!(
                "SELECT toUInt64(count()) AS value \
                 FROM (SELECT * FROM `{0}`.`{table}` FINAL) AS derived \
                 ALL INNER JOIN (SELECT * FROM `{0}`.`events` FINAL) AS owner \
                   ON derived.source_host = owner.source_host \
                  AND derived.event_uid = owner.event_uid \
                  AND derived.source_event_version = owner.event_version \
                 WHERE owner.source_host = '' AND owner.source_name = {1} \
                   AND owner.source_file = {2} AND owner.source_generation = {3} \
                 FORMAT JSONEachRow",
                database.as_str(),
                sql_string(PROCESS_SOURCE),
                sql_string(source_file),
                generation,
            );
            if scalar_count(clickhouse, database, &query, stage.label()).await? == 0 {
                bail!(
                    "committed response loss at {} left no exactly owner-bound generation {generation} row",
                    stage.label()
                );
            }
        }
        GenerationStage::McpOpenEvents | GenerationStage::McpOpenTurns => {
            let observed = mcp_child_generation(clickhouse, database, stage.table()).await?;
            if observed <= baseline {
                bail!(
                    "committed response loss at {} did not advance its physical child generation: before={baseline}, after={observed}",
                    stage.label()
                );
            }
        }
        GenerationStage::McpOpenHeader | GenerationStage::McpOpenReadiness => {
            let candidate = candidate_publication_id(source_file, generation);
            let query = format!(
                "SELECT toUInt64(count()) AS value FROM `{}`.`{}` FINAL \
                 WHERE candidate_publication_id = {} FORMAT JSONEachRow",
                database.as_str(),
                stage.table(),
                sql_string(&candidate),
            );
            if scalar_count(clickhouse, database, &query, stage.label()).await? != 1 {
                bail!(
                    "committed response loss at {} did not leave exactly one durable candidate {candidate}",
                    stage.label()
                );
            }
        }
        GenerationStage::FinalCheckpoint => {
            let transition = current_transition(clickhouse, database, source_file)
                .await?
                .context("final checkpoint response loss left no current transition")?;
            if transition.source_generation != generation
                || transition.last_line != FIXTURE_LINES
                || transition.lifecycle != "active"
                || transition.checkpoint_revision == 0
                || transition.operation_id.is_empty()
                || transition.final_scan_complete != 1
                || !transition.block_reason.is_empty()
                || transition.compatibility_prepared != 1
                || transition.backend_caught_up != 1
            {
                bail!("unexpected committed final-checkpoint boundary: {transition:?}");
            }
        }
        GenerationStage::SourceReadiness => {
            let readiness = current_readiness(clickhouse, database, source_file, generation)
                .await?
                .context("source readiness response loss left no current readiness row")?;
            if readiness.readiness_revision == 0
                || readiness.checkpoint_revision == 0
                || readiness.operation_id.is_empty()
                || readiness.complete != 1
                || !readiness.block_reason.is_empty()
                || readiness.compatibility_prepared != 1
                || readiness.backend_caught_up != 1
            {
                bail!("unexpected committed source-readiness boundary: {readiness:?}");
            }
        }
    }
    Ok(())
}

async fn current_head(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
) -> Result<Option<HeadRow>> {
    let query = format!(
        "SELECT toUInt32(source_generation) AS source_generation, \
                toUInt64(publication_revision) AS publication_revision, operation_id \
         FROM `{}`.`v_current_published_source_generations` \
         WHERE source_host = '' AND source_name = {} AND source_file = {} \
         LIMIT 1 FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
    );
    Ok(clickhouse
        .query_rows::<HeadRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process source head")?
        .into_iter()
        .next())
}

async fn head_history(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
) -> Result<HistoryRow> {
    let query = format!(
        "SELECT toUInt64(count()) AS history_count, \
                toUInt64(uniqExact(publication_revision)) AS revision_count, \
                toUInt64(uniqExact(operation_id)) AS operation_count, \
                toUInt64(uniqExact(source_generation)) AS generation_count, \
                toUInt64(max(publication_revision)) AS max_revision, \
                toUInt32(max(source_generation)) AS max_generation \
         FROM `{}`.`v_published_source_generation_history` \
         WHERE source_host = '' AND source_name = {} AND source_file = {} \
         FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
    );
    clickhouse
        .query_rows::<HistoryRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process source-head history")?
        .into_iter()
        .next()
        .context("production-process source-head history returned no row")
}

fn history_matches_head(history: &HistoryRow, head: &HeadRow, expected_count: u64) -> bool {
    history.history_count == expected_count
        && history.revision_count == expected_count
        && history.operation_count == expected_count
        && history.generation_count == expected_count
        && history.max_revision == head.publication_revision
        && history.max_generation == head.source_generation
}

async fn live_events(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
) -> Result<Vec<EventRow>> {
    let query = format!(
        "SELECT toUInt32(source_generation) AS source_generation, text_content \
         FROM `{}`.`v_live_events` \
         WHERE source_host = '' AND source_name = {} AND source_file = {} \
           AND session_id = {} AND notEmpty(text_content) \
         ORDER BY source_line_no, event_uid FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
        sql_string(PROCESS_SESSION),
    );
    clickhouse
        .query_rows(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process live events")
}

async fn legacy_events(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<Vec<EventRow>> {
    let query = format!(
        "SELECT toUInt32(0) AS source_generation, projected.text_content AS text_content \
         FROM ( \
           SELECT * FROM `{0}`.`mcp_open_events` FINAL \
           WHERE session_id = {1} \
         ) AS projected \
         INNER JOIN ( \
           SELECT session_id, slot, generation \
           FROM `{0}`.`mcp_open_sessions` FINAL \
           WHERE session_id = {1} \
         ) AS current \
           ON projected.session_id = current.session_id \
          AND projected.slot = current.slot \
          AND projected.generation = current.generation \
          AND projected.candidate_generation = current.generation \
         WHERE projected.session_id = {1} AND notEmpty(projected.text_content) \
         ORDER BY projected.event_order, projected.event_uid FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
    );
    clickhouse
        .query_rows(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process legacy projection")
}

async fn legacy_session_row(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<Value> {
    let query = format!(
        "SELECT * FROM `{}`.`mcp_open_sessions` FINAL \
         WHERE session_id = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
    );
    let rows = clickhouse
        .query_rows::<Value>(&query, Some(database.as_str()))
        .await
        .context("failed to read exact production-process legacy session tuple")?;
    if rows.len() != 1 {
        bail!(
            "expected one production-process legacy session tuple, observed {}",
            rows.len()
        );
    }
    Ok(rows.into_iter().next().expect("length checked"))
}

async fn legacy_pointer(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<LegacyPointerRow> {
    let query = format!(
        "SELECT toUInt8(slot) AS slot, toUInt64(generation) AS generation, \
                toUInt64(source_revision) AS source_revision \
         FROM `{}`.`mcp_open_sessions` FINAL \
         WHERE session_id = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
    );
    let rows = clickhouse
        .query_rows::<LegacyPointerRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process legacy pointer")?;
    if rows.len() != 1 {
        bail!(
            "expected one production-process legacy pointer, observed {}",
            rows.len()
        );
    }
    Ok(rows.into_iter().next().expect("length checked"))
}

async fn assert_legacy_pointer_authorized(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    source_generation: u32,
) -> Result<()> {
    let pointer = legacy_pointer(clickhouse, database).await?;
    let expected =
        canonical_source_revision(clickhouse, database, source_file, source_generation).await?;
    if pointer.slot > 1
        || pointer.generation == 0
        || pointer.source_revision == 0
        || pointer.source_revision != expected
    {
        bail!(
            "legacy pointer is not authorized by canonical generation {source_generation}: pointer={pointer:?}, expected_source_revision={expected}"
        );
    }
    Ok(())
}

async fn canonical_source_revision(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    source_generation: u32,
) -> Result<u64> {
    let query = format!(
        "SELECT toUInt64(if(count() = 0, 0, \
                    cityHash64(arraySort(groupArray(tuple(event_uid, event_version)))))) \
                    AS source_revision \
         FROM `{}`.`events` FINAL \
         WHERE session_id = {} AND source_host = '' AND source_name = {} \
           AND source_file = {} AND source_generation = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
        source_generation,
    );
    let expected = clickhouse
        .query_rows::<SourceRevisionRow>(&query, Some(database.as_str()))
        .await
        .context("failed to recompute production-process legacy source revision")?
        .into_iter()
        .next()
        .context("source-revision recomputation returned no row")?;
    Ok(expected.source_revision)
}

async fn legacy_session_publication_tuple(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<Value> {
    let query = format!(
        "SELECT {LEGACY_SESSION_PUBLICATION_COLUMNS} \
         FROM `{}`.`mcp_open_sessions` FINAL \
         WHERE session_id = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
    );
    one_json_row(
        clickhouse,
        database,
        &query,
        "active production-process legacy session publication tuple",
    )
    .await
}

async fn candidate_session_publication_tuple(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    candidate_publication_id: &str,
) -> Result<Value> {
    let query = format!(
        "SELECT {LEGACY_SESSION_PUBLICATION_COLUMNS} \
         FROM `{}`.`mcp_open_publication_headers` FINAL \
         WHERE session_id = {} AND candidate_publication_id = {} AND tombstone = 0 \
         FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
        sql_string(candidate_publication_id),
    );
    one_json_row(
        clickhouse,
        database,
        &query,
        "prepared production-process legacy session candidate tuple",
    )
    .await
}

async fn one_json_row(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    query: &str,
    label: &str,
) -> Result<Value> {
    let rows = clickhouse
        .query_rows::<Value>(query, Some(database.as_str()))
        .await
        .with_context(|| format!("failed to read {label}"))?;
    if rows.len() != 1 {
        bail!("expected one {label}, observed {}", rows.len());
    }
    Ok(rows.into_iter().next().expect("length checked"))
}

async fn current_transition(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
) -> Result<Option<TransitionRow>> {
    let query = format!(
        "SELECT toUInt64(inode) AS inode, \
                toUInt32(source_generation) AS source_generation, \
                toUInt64(last_offset) AS last_offset, \
                toUInt64(last_line) AS last_line, \
                toUInt64(checkpoint_revision) AS checkpoint_revision, operation_id, \
                lifecycle, toUInt64(scan_inode) AS scan_inode, \
                toUInt64(scan_boundary) AS scan_boundary, final_scan_complete, \
                block_reason, compatibility_prepared, backend_caught_up \
         FROM `{}`.`v_current_ingest_checkpoint_transitions` \
         WHERE host = '' AND source_name = {} AND source_file = {} \
         LIMIT 1 FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
    );
    Ok(clickhouse
        .query_rows::<TransitionRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process causal checkpoint")?
        .into_iter()
        .next())
}

async fn current_readiness(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    generation: u32,
) -> Result<Option<ReadinessRow>> {
    let query = format!(
        "SELECT toUInt64(readiness_revision) AS readiness_revision, \
                toUInt64(checkpoint_revision) AS checkpoint_revision, operation_id, \
                complete, block_reason, compatibility_prepared, backend_caught_up \
         FROM `{}`.`v_current_source_generation_publication_readiness` \
         WHERE source_host = '' AND source_name = {} AND source_file = {} \
           AND source_generation = {} LIMIT 1 FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
        generation,
    );
    Ok(clickhouse
        .query_rows::<ReadinessRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process publication readiness")?
        .into_iter()
        .next())
}

async fn current_mcp_readiness(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    generation: u32,
) -> Result<McpReadinessRow> {
    let query = format!(
        "SELECT candidate_publication_id, operation_id, \
                toUInt64(affected_session_count) AS affected_session_count, \
                toUInt64(prepared_session_count) AS prepared_session_count, \
                toUInt64(tombstone_count) AS tombstone_count, ready, block_reason \
         FROM `{}`.`v_current_mcp_open_generation_readiness` \
         WHERE source_host = '' AND source_name = {} AND source_file = {} \
           AND source_generation = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SOURCE),
        sql_string(source_file),
        generation,
    );
    let rows = clickhouse
        .query_rows::<McpReadinessRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process MCP generation readiness")?;
    if rows.len() != 1 {
        bail!(
            "expected one production-process MCP generation readiness row, observed {}",
            rows.len()
        );
    }
    Ok(rows.into_iter().next().expect("length checked"))
}

async fn candidate_header(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    candidate_publication_id: &str,
) -> Result<CandidateHeaderRow> {
    let query = format!(
        "SELECT toUInt8(slot) AS slot, toUInt64(generation) AS generation, \
                toUInt64(source_revision) AS source_revision, \
                toUInt64(total_turns) AS total_turns, \
                toUInt64(total_events) AS total_events, tombstone, required_source_heads, \
                required_heads_fingerprint, operation_id \
         FROM `{}`.`mcp_open_publication_headers` FINAL \
         WHERE session_id = {} AND candidate_publication_id = {} FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
        sql_string(candidate_publication_id),
    );
    let rows = clickhouse
        .query_rows::<CandidateHeaderRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read production-process candidate header")?;
    if rows.len() != 1 {
        bail!(
            "expected one production-process candidate header, observed {}",
            rows.len()
        );
    }
    Ok(rows.into_iter().next().expect("length checked"))
}

async fn candidate_child_counts(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    header: &CandidateHeaderRow,
) -> Result<CandidateChildCounts> {
    let query = format!(
        "SELECT \
           toUInt64((SELECT count() FROM `{0}`.`mcp_open_events` FINAL \
             WHERE session_id = {1} AND slot = {2} AND generation = {3} \
               AND candidate_generation = {3})) AS event_count, \
           toUInt64((SELECT count() FROM `{0}`.`mcp_open_turns` FINAL \
             WHERE session_id = {1} AND slot = {2} AND generation = {3} \
               AND candidate_generation = {3})) AS turn_count \
         FORMAT JSONEachRow",
        database.as_str(),
        sql_string(PROCESS_SESSION),
        header.slot,
        header.generation,
    );
    clickhouse
        .query_rows::<CandidateChildCounts>(&query, Some(database.as_str()))
        .await
        .context("failed to count production-process candidate children")?
        .into_iter()
        .next()
        .context("candidate child count query returned no row")
}

fn expected_texts(generation: u32) -> BTreeSet<String> {
    (1..=FIXTURE_LINES)
        .map(|line| format!("process generation {generation} line {line}"))
        .collect()
}

fn rows_match_generation(rows: &[EventRow], generation: u32, check_source: bool) -> bool {
    if rows.len() != FIXTURE_LINES as usize {
        return false;
    }
    let texts = rows
        .iter()
        .map(|row| row.text_content.clone())
        .collect::<BTreeSet<_>>();
    texts == expected_texts(generation)
        && (!check_source || rows.iter().all(|row| row.source_generation == generation))
}

async fn assert_live_generation(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    generation: u32,
) -> Result<()> {
    let rows = live_events(clickhouse, database, source_file).await?;
    if !rows_match_generation(&rows, generation, true) {
        bail!("live model is not complete generation {generation}: {rows:?}");
    }
    Ok(())
}

async fn assert_legacy_generation(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    generation: u32,
) -> Result<()> {
    let rows = legacy_events(clickhouse, database).await?;
    if !rows_match_generation(&rows, generation, false) {
        bail!("legacy projection is not complete generation {generation}: {rows:?}");
    }
    Ok(())
}

async fn wait_for_complete_generation(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    process: &mut IngestProcess,
    generation: u32,
    history_count: u64,
) -> Result<HeadRow> {
    let started = Instant::now();
    let mut last_observation = String::new();
    while started.elapsed() < PROCESS_WAIT {
        process.ensure_running()?;
        let observation = generation_observation(clickhouse, database, source_file).await;
        match observation {
            Ok((Some(head), history, live, legacy))
                if head.source_generation == generation
                    && history_matches_head(&history, &head, history_count)
                    && rows_match_generation(&live, generation, true)
                    && rows_match_generation(&legacy, generation, false) =>
            {
                return Ok(head);
            }
            Ok((head, history, live, legacy)) => {
                last_observation =
                    format!("head={head:?}, history={history:?}, live={live:?}, legacy={legacy:?}");
            }
            Err(error) => last_observation = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("timed out waiting for complete generation {generation}: {last_observation}")
}

async fn generation_observation(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
) -> Result<(Option<HeadRow>, HistoryRow, Vec<EventRow>, Vec<EventRow>)> {
    let head = current_head(clickhouse, database, source_file).await?;
    let history = head_history(clickhouse, database, source_file).await?;
    let live = live_events(clickhouse, database, source_file).await?;
    let legacy = legacy_events(clickhouse, database).await?;
    Ok((head, history, live, legacy))
}

fn write_generation(path: &Path, generation: u32) -> Result<()> {
    let mut fixture = String::new();
    for line in 1..=FIXTURE_LINES {
        let uuid = format!("process-g{generation}-line-{line}");
        let parent_uuid = (line > 1).then(|| format!("process-g{generation}-line-{}", line - 1));
        let timestamp = if line == FIXTURE_LINES {
            // A recoverable timestamp error guarantees that the production
            // sink exercises its ingest_errors boundary without preventing
            // this otherwise complete generation from publishing.
            "not-a-timestamp".to_string()
        } else {
            format!("2026-07-20T12:00:{line:02}.000Z")
        };
        let row = match line {
            2 => json!({
                "type": "assistant",
                "timestamp": timestamp,
                "uuid": uuid,
                "parentUuid": parent_uuid,
                "sessionId": PROCESS_SESSION,
                "cwd": "/repo",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": format!("process-tool-g{generation}"),
                            "name": "Read",
                            "input": {"path": format!("generation-{generation}.txt")},
                        },
                        {
                            "type": "text",
                            "text": format!("process generation {generation} line {line}"),
                        },
                    ],
                },
            }),
            3 => json!({
                "type": "user",
                "timestamp": timestamp,
                "uuid": uuid,
                "parentUuid": parent_uuid,
                "parentToolUseID": format!("process-tool-g{generation}"),
                "toolUseID": format!("process-tool-g{generation}"),
                "sourceToolAssistantUUID": format!("process-g{generation}-line-2"),
                "sourceToolUseID": format!("process-tool-g{generation}"),
                "sessionId": PROCESS_SESSION,
                "cwd": "/repo",
                "message": {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": format!("process-tool-g{generation}"),
                        "content": [{
                            "type": "text",
                            "text": format!("process generation {generation} line {line}"),
                        }],
                        "is_error": false,
                    }],
                },
            }),
            _ => json!({
                "type": "user",
                "timestamp": timestamp,
                "uuid": uuid,
                "parentUuid": parent_uuid,
                "sessionId": PROCESS_SESSION,
                "cwd": "/repo",
                "message": {
                    "role": "user",
                    "content": format!("process generation {generation} line {line}"),
                },
            }),
        };
        fixture.push_str(&row.to_string());
        fixture.push('\n');
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open generation fixture {}", path.display()))?;
    file.write_all(fixture.as_bytes())
        .with_context(|| format!("failed to write generation fixture {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync generation fixture {}", path.display()))
}

fn replace_generation(root: &Path, source_file: &Path, generation: u32) -> Result<()> {
    // Keep the staging file invisible to the recursive JSONL watcher. The
    // rename itself remains same-filesystem and therefore atomic.
    let replacement = root.join(format!(".replacement-g{generation}.tmp"));
    write_generation(&replacement, generation)?;
    fs::rename(&replacement, source_file).with_context(|| {
        format!(
            "failed to atomically replace {} with generation {generation}",
            source_file.display()
        )
    })
}

fn toml_string(value: &str) -> String {
    serde_json::to_string(value).expect("JSON strings are valid TOML basic strings")
}

fn write_ingest_config(
    path: &Path,
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    proxy_url: &str,
    root: &Path,
    source_file: &Path,
) -> Result<()> {
    let clickhouse_config = clickhouse.config();
    let state_dir = root.join("state");
    let text = format!(
        "[clickhouse]\n\
         url = {}\n\
         database = {}\n\
         username = {}\n\
         password = {}\n\
         timeout_seconds = 5.0\n\
         request_compression = \"none\"\n\
         async_insert = true\n\
         wait_for_async_insert = true\n\
         allow_newer_server = false\n\n\
         [ingest]\n\
         exclude_project_dirs = []\n\
         batch_size = 1\n\
         max_batch_bytes = 8388608\n\
         flush_interval_seconds = 30.0\n\
         state_dir = {}\n\
         backfill_on_start = false\n\
         max_file_workers = 1\n\
         max_inflight_batches = 1\n\
         debounce_ms = 10\n\
         reconcile_interval_seconds = 3600.0\n\
         heartbeat_interval_seconds = 3600.0\n\
         ack_observation = false\n\n\
         [[ingest.sources]]\n\
         name = {}\n\
         harness = \"claude-code\"\n\
         enabled = true\n\
         glob = {}\n\
         watch_root = {}\n\
         format = \"jsonl\"\n",
        toml_string(proxy_url),
        toml_string(database.as_str()),
        toml_string(&clickhouse_config.username),
        toml_string(&clickhouse_config.password),
        toml_string(&state_dir.to_string_lossy()),
        toml_string(PROCESS_SOURCE),
        toml_string(&source_file.to_string_lossy()),
        toml_string(&root.to_string_lossy()),
    );
    fs::write(path, text).with_context(|| {
        format!(
            "failed to write production ingest config {}",
            path.display()
        )
    })
}

fn validated_ingest_binary() -> Result<PathBuf> {
    let configured = env::var(INGEST_BINARY_ENV)
        .with_context(|| format!("{INGEST_BINARY_ENV} is required for process fault injection"))?;
    let binary = fs::canonicalize(&configured)
        .with_context(|| format!("failed to resolve production ingest binary {configured}"))?;
    let root = fs::canonicalize(INGEST_BINARY_ROOT)
        .with_context(|| format!("failed to resolve owned binary root {INGEST_BINARY_ROOT}"))?;
    if binary.parent() != Some(root.as_path())
        || binary.file_name().and_then(|name| name.to_str()) != Some("moraine-ingest")
    {
        bail!("{INGEST_BINARY_ENV} must resolve to {INGEST_BINARY_ROOT}/moraine-ingest");
    }
    let metadata = fs::metadata(&binary).with_context(|| {
        format!(
            "failed to inspect production ingest binary {}",
            binary.display()
        )
    })?;
    if !metadata.is_file() {
        bail!(
            "production ingest binary is not a regular file: {}",
            binary.display()
        );
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o111 == 0 {
        bail!(
            "production ingest binary is not executable: {}",
            binary.display()
        );
    }
    Ok(binary)
}

async fn assert_replaying_boundary(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    generation: u32,
    last_line: u64,
) -> Result<()> {
    let transition = current_transition(clickhouse, database, source_file)
        .await?
        .context("committed replay checkpoint is missing after SIGKILL")?;
    let expected_backend_caught_up = u8::from(last_line == 0);
    if transition.source_generation != generation
        || transition.last_line != last_line
        || transition.lifecycle != "replaying"
        || transition.checkpoint_revision == 0
        || transition.operation_id.is_empty()
        || transition.final_scan_complete != 0
        || !transition.block_reason.is_empty()
        || transition.compatibility_prepared != 0
        // The begin-replay barrier inherits the proven prior generation's
        // no-mirror catch-up bit. Once a replay batch advances the cursor,
        // its non-final checkpoint clears that bit until final publication.
        || transition.backend_caught_up != expected_backend_caught_up
    {
        bail!("unexpected committed replay boundary: {transition:?}");
    }
    Ok(())
}

async fn assert_final_boundary(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    source_file: &str,
    head: &HeadRow,
) -> Result<String> {
    let transition = current_transition(clickhouse, database, source_file)
        .await?
        .context("final checkpoint is missing after committed head response loss")?;
    if transition.source_generation != head.source_generation
        || transition.inode != transition.scan_inode
        || transition.last_offset != transition.scan_boundary
        || transition.last_line != FIXTURE_LINES
        || transition.checkpoint_revision == 0
        || transition.operation_id != head.operation_id
        || transition.lifecycle != "active"
        || transition.final_scan_complete != 1
        || !transition.block_reason.is_empty()
        || transition.compatibility_prepared != 1
        || transition.backend_caught_up != 1
    {
        bail!("unexpected final checkpoint after committed head response loss: {transition:?}");
    }

    let readiness = current_readiness(clickhouse, database, source_file, head.source_generation)
        .await?
        .context("final publication readiness is missing after committed head response loss")?;
    if readiness.readiness_revision == 0
        || readiness.checkpoint_revision != transition.checkpoint_revision
        || readiness.operation_id != transition.operation_id
        || readiness.complete != 1
        || !readiness.block_reason.is_empty()
        || readiness.compatibility_prepared != 1
        || readiness.backend_caught_up != 1
    {
        bail!("unexpected final readiness after committed head response loss: {readiness:?}");
    }

    let mcp_readiness =
        current_mcp_readiness(clickhouse, database, source_file, head.source_generation).await?;
    let required_head = RequiredSourceHead {
        source_host: String::new(),
        source_name: PROCESS_SOURCE.to_string(),
        source_file: source_file.to_string(),
        source_generation: head.source_generation,
        publication_revision: head.publication_revision,
    };
    if mcp_readiness.candidate_publication_id.is_empty() {
        bail!("MCP readiness has an empty candidate publication id: {mcp_readiness:?}");
    }
    if mcp_readiness.operation_id.is_empty() {
        bail!("MCP readiness has an empty preparation operation id: {mcp_readiness:?}");
    }
    if mcp_readiness.affected_session_count != 1
        || mcp_readiness.prepared_session_count != 1
        || mcp_readiness.tombstone_count != 0
    {
        bail!("MCP readiness candidate cardinality is incomplete: {mcp_readiness:?}");
    }
    if mcp_readiness.ready != 1 || !mcp_readiness.block_reason.is_empty() {
        bail!("MCP readiness authorization is incomplete: {mcp_readiness:?}");
    }

    let candidate = candidate_header(
        clickhouse,
        database,
        &mcp_readiness.candidate_publication_id,
    )
    .await?;
    let expected_source_revision =
        canonical_source_revision(clickhouse, database, source_file, head.source_generation)
            .await?;
    if candidate.slot > 1
        || candidate.generation == 0
        || candidate.source_revision != expected_source_revision
        || candidate.total_events != FIXTURE_EVENTS
        || candidate.total_turns == 0
        || candidate.tombstone != 0
        || candidate.required_source_heads != vec![required_head]
        || candidate.required_heads_fingerprint.is_empty()
        || candidate.operation_id != mcp_readiness.operation_id
    {
        bail!(
            "candidate header is not authorized by the committed causal chain: candidate={candidate:?}, expected_source_revision={expected_source_revision}"
        );
    }
    let children = candidate_child_counts(clickhouse, database, &candidate).await?;
    if children.event_count != candidate.total_events
        || children.turn_count != candidate.total_turns
    {
        bail!(
            "candidate children are incomplete at their pinned generation: header={candidate:?}, children={children:?}"
        );
    }
    Ok(mcp_readiness.candidate_publication_id)
}

#[allow(clippy::too_many_arguments)]
async fn exercise_pre_head_stage_fault(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    binary: &Path,
    config_path: &Path,
    root: &Path,
    source_file: &Path,
    source_file_text: &str,
    faults: &FaultController,
    process: &mut IngestProcess,
    head: &HeadRow,
    history_count: &mut u64,
    generation: u32,
    stage: GenerationStage,
) -> Result<HeadRow> {
    let baseline = stage_baseline(clickhouse, database, stage).await?;
    let legacy_before_fault = legacy_session_row(clickhouse, database).await?;
    let target = FaultTarget::GenerationStage { generation, stage };
    process.drain_logs();
    faults.arm(target.clone())?;
    replace_generation(root, source_file, generation)?;
    faults.wait_for(&target, PROCESS_WAIT).await?;
    process
        .wait_for_fault_log(faults, &target, PROCESS_WAIT)
        .await?;
    process.sigkill()?;
    faults.release_after_sigkill(&target)?;

    assert_stage_commit(
        clickhouse,
        database,
        source_file_text,
        generation,
        stage,
        baseline,
    )
    .await?;
    let old_head = current_head(clickhouse, database, source_file_text)
        .await?
        .with_context(|| format!("old head disappeared after {} SIGKILL", stage.label()))?;
    if &old_head != head {
        bail!(
            "{} SIGKILL changed the source head: before={head:?}, after={old_head:?}",
            stage.label()
        );
    }
    let history = head_history(clickhouse, database, source_file_text).await?;
    if !history_matches_head(&history, &old_head, *history_count) {
        bail!(
            "{} SIGKILL changed head history: {history:?}",
            stage.label()
        );
    }
    assert_live_generation(
        clickhouse,
        database,
        source_file_text,
        head.source_generation,
    )
    .await?;
    assert_legacy_generation(clickhouse, database, head.source_generation).await?;
    assert_legacy_pointer_authorized(
        clickhouse,
        database,
        source_file_text,
        head.source_generation,
    )
    .await?;
    let legacy_after_fault = legacy_session_row(clickhouse, database).await?;
    if legacy_after_fault != legacy_before_fault {
        bail!(
            "{} SIGKILL changed the exact legacy session tuple: before={legacy_before_fault}, after={legacy_after_fault}",
            stage.label()
        );
    }

    *process = IngestProcess::spawn(binary, config_path)?;
    *history_count += 1;
    let repaired = wait_for_complete_generation(
        clickhouse,
        database,
        source_file_text,
        process,
        generation,
        *history_count,
    )
    .await?;
    if repaired.publication_revision != head.publication_revision + 1 {
        bail!(
            "{} repair did not switch publication exactly once: before={head:?}, after={repaired:?}",
            stage.label()
        );
    }
    assert_legacy_pointer_authorized(clickhouse, database, source_file_text, generation).await?;
    Ok(repaired)
}

pub(super) async fn run(clickhouse: &ClickHouseClient, database: &OwnedDatabaseName) -> Result<()> {
    let binary = validated_ingest_binary()?;
    let owned = OwnedTempTree::create()?;
    let source_file = owned.path.join("process-source.jsonl");
    let state_dir = owned.path.join("state");
    fs::create_dir(&state_dir).context("failed to create production ingest state directory")?;
    write_generation(&source_file, 1)?;
    let source_file_text = source_file.to_string_lossy().to_string();

    let faults = FaultController::new(&source_file_text);
    let proxy = CommitDropProxy::start(&clickhouse.config().url, faults.clone()).await?;
    let config_path = owned.path.join("moraine.toml");
    write_ingest_config(
        &config_path,
        clickhouse,
        database,
        &proxy.url,
        &owned.path,
        &source_file,
    )?;

    let mut process = IngestProcess::spawn(&binary, &config_path)?;
    let mut history_count = 1_u64;
    let mut head = wait_for_complete_generation(
        clickhouse,
        database,
        &source_file_text,
        &mut process,
        1,
        history_count,
    )
    .await?;
    assert_legacy_pointer_authorized(clickhouse, database, &source_file_text, 1).await?;

    for (offset, last_line) in (0..=FIXTURE_LINES).enumerate() {
        let generation = u32::try_from(offset)
            .context("process-test generation conversion overflow")?
            .checked_add(2)
            .context("process-test generation overflow")?;
        let target = FaultTarget::ReplayingCheckpoint {
            generation,
            last_line,
        };
        let legacy_before_fault = legacy_session_row(clickhouse, database).await?;
        process.drain_logs();
        faults.arm(target.clone())?;
        replace_generation(&owned.path, &source_file, generation)?;
        faults.wait_for(&target, PROCESS_WAIT).await?;
        process
            .wait_for_fault_log(&faults, &target, PROCESS_WAIT)
            .await?;
        process.sigkill()?;
        faults.release_after_sigkill(&target)?;

        assert_replaying_boundary(
            clickhouse,
            database,
            &source_file_text,
            generation,
            last_line,
        )
        .await?;
        let old_head = current_head(clickhouse, database, &source_file_text)
            .await?
            .context("old head disappeared after replay checkpoint SIGKILL")?;
        if old_head != head {
            bail!(
                "replay checkpoint SIGKILL changed the source head: before={head:?}, after={old_head:?}"
            );
        }
        let history = head_history(clickhouse, database, &source_file_text).await?;
        if !history_matches_head(&history, &old_head, history_count) {
            bail!("replay checkpoint SIGKILL changed head history: {history:?}");
        }
        assert_live_generation(clickhouse, database, &source_file_text, generation - 1).await?;
        assert_legacy_generation(clickhouse, database, generation - 1).await?;
        assert_legacy_pointer_authorized(clickhouse, database, &source_file_text, generation - 1)
            .await?;
        let legacy_after_fault = legacy_session_row(clickhouse, database).await?;
        if legacy_after_fault != legacy_before_fault {
            bail!(
                "replay checkpoint SIGKILL changed the exact legacy session tuple: before={legacy_before_fault}, after={legacy_after_fault}"
            );
        }

        process = IngestProcess::spawn(&binary, &config_path)?;
        history_count += 1;
        let repaired = wait_for_complete_generation(
            clickhouse,
            database,
            &source_file_text,
            &mut process,
            generation,
            history_count,
        )
        .await?;
        if repaired.publication_revision != head.publication_revision + 1 {
            bail!(
                "replay repair did not switch publication exactly once: before={head:?}, after={repaired:?}"
            );
        }
        assert_legacy_pointer_authorized(clickhouse, database, &source_file_text, generation)
            .await?;
        head = repaired;
    }

    // Every target below is a genuinely separate production ClickHouse
    // request boundary. The canonical events insert is intentionally one
    // target because its search-document and search-posting materialized
    // views commit synchronously with that request; the durability assertion
    // above proves all three physical relations together.
    for stage in GenerationStage::PRE_HEAD_MATRIX {
        let generation = head
            .source_generation
            .checked_add(1)
            .context("process-test stage generation overflow")?;
        head = exercise_pre_head_stage_fault(
            clickhouse,
            database,
            &binary,
            &config_path,
            &owned.path,
            &source_file,
            &source_file_text,
            &faults,
            &mut process,
            &head,
            &mut history_count,
            generation,
            stage,
        )
        .await?;
    }

    let head_generation = head.source_generation;
    let generation = head_generation
        .checked_add(1)
        .context("process-test head generation overflow")?;
    let target = FaultTarget::SourceHead { generation };
    let legacy_before_fault = legacy_session_row(clickhouse, database).await?;
    process.drain_logs();
    faults.arm(target.clone())?;
    replace_generation(&owned.path, &source_file, generation)?;
    faults.wait_for(&target, PROCESS_WAIT).await?;
    process
        .wait_for_fault_log(&faults, &target, PROCESS_WAIT)
        .await?;
    process.sigkill()?;
    faults.release_after_sigkill(&target)?;

    history_count += 1;
    let committed_head = current_head(clickhouse, database, &source_file_text)
        .await?
        .context("committed source head is missing after response loss")?;
    if committed_head.source_generation != generation
        || committed_head.publication_revision != head.publication_revision + 1
    {
        bail!(
            "committed head response loss produced an unexpected head: before={head:?}, after={committed_head:?}"
        );
    }
    let committed_history = head_history(clickhouse, database, &source_file_text).await?;
    if !history_matches_head(&committed_history, &committed_head, history_count) {
        bail!("committed head response loss changed logical history: {committed_history:?}");
    }
    let candidate_publication_id =
        assert_final_boundary(clickhouse, database, &source_file_text, &committed_head).await?;
    assert_live_generation(clickhouse, database, &source_file_text, generation).await?;
    assert_legacy_generation(clickhouse, database, head_generation).await?;
    assert_legacy_pointer_authorized(clickhouse, database, &source_file_text, head_generation)
        .await?;
    let legacy_after_fault = legacy_session_row(clickhouse, database).await?;
    if legacy_after_fault != legacy_before_fault {
        bail!(
            "committed head response loss changed the exact legacy session tuple before restart: before={legacy_before_fault}, after={legacy_after_fault}"
        );
    }
    let legacy_publication_before_repair =
        legacy_session_publication_tuple(clickhouse, database).await?;
    let prepared_candidate =
        candidate_session_publication_tuple(clickhouse, database, &candidate_publication_id)
            .await?;
    if legacy_publication_before_repair == prepared_candidate {
        bail!(
            "committed head response loss activated its prepared legacy candidate before restart"
        );
    }

    process = IngestProcess::spawn(&binary, &config_path)?;
    let repaired_head = wait_for_complete_generation(
        clickhouse,
        database,
        &source_file_text,
        &mut process,
        generation,
        history_count,
    )
    .await?;
    if repaired_head != committed_head {
        bail!(
            "restart changed the committed response-loss head: committed={committed_head:?}, repaired={repaired_head:?}"
        );
    }
    let legacy_publication_after_repair =
        legacy_session_publication_tuple(clickhouse, database).await?;
    if legacy_publication_after_repair != prepared_candidate {
        bail!(
            "startup repair did not activate the exact prepared legacy candidate: candidate={prepared_candidate}, active={legacy_publication_after_repair}"
        );
    }
    assert_legacy_pointer_authorized(clickhouse, database, &source_file_text, generation).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let stable_history = head_history(clickhouse, database, &source_file_text).await?;
    if !history_matches_head(&stable_history, &repaired_head, history_count) {
        bail!("startup repair duplicated the committed source head: {stable_history:?}");
    }

    process.sigkill()?;
    proxy.shutdown().await?;
    eprintln!(
        "source-publication process fault evidence: replay_boundaries={} pre_head_stages={} committed_head_generation={} logical_heads={}",
        FIXTURE_LINES + 1,
        GenerationStage::PRE_HEAD_MATRIX.len(),
        generation,
        history_count,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clickhouse_query_accepts_url_and_body_transports() {
        let url: Uri = "/?query=SELECT%201".parse().expect("URL query URI parses");
        assert_eq!(clickhouse_query(&url, b"ignored"), "SELECT 1");

        let body: Uri = "/?database=moraine".parse().expect("body query URI parses");
        assert_eq!(
            clickhouse_query(&body, b"INSERT INTO moraine.events SELECT 1"),
            "INSERT INTO moraine.events SELECT 1"
        );
    }

    #[test]
    fn fault_targets_match_only_the_owned_generation_and_boundary() {
        let source_file = "/tmp/process-source.jsonl";
        let checkpoint = serde_json::to_vec(&json!({
            "source_name": PROCESS_SOURCE,
            "source_file": source_file,
            "source_generation": 4,
            "last_line": 2,
            "lifecycle": "replaying",
        }))
        .expect("fixture serializes");
        assert!(FaultTarget::ReplayingCheckpoint {
            generation: 4,
            last_line: 2,
        }
        .matches(
            "INSERT INTO `db`.`ingest_checkpoint_transitions` FORMAT JSONEachRow",
            &checkpoint,
            source_file,
        ));
        assert!(!FaultTarget::ReplayingCheckpoint {
            generation: 4,
            last_line: 3,
        }
        .matches(
            "INSERT INTO `db`.`ingest_checkpoint_transitions` FORMAT JSONEachRow",
            &checkpoint,
            source_file,
        ));

        let head = serde_json::to_vec(&json!({
            "source_name": PROCESS_SOURCE,
            "source_file": source_file,
            "source_generation": 7,
        }))
        .expect("fixture serializes");
        assert!(FaultTarget::SourceHead { generation: 7 }.matches(
            "INSERT INTO `db`.`published_source_generations` FORMAT JSONEachRow",
            &head,
            source_file,
        ));
        assert!(!FaultTarget::SourceHead { generation: 6 }.matches(
            "INSERT INTO `db`.`published_source_generations` FORMAT JSONEachRow",
            &head,
            source_file,
        ));

        for (stage, table) in [
            (GenerationStage::RawEvents, "raw_events"),
            (GenerationStage::CanonicalAndSearch, "events"),
            (GenerationStage::IngestErrors, "ingest_errors"),
        ] {
            let target = FaultTarget::GenerationStage {
                generation: 7,
                stage,
            };
            assert!(target.matches(
                &format!("INSERT INTO `db`.`{table}` FORMAT JSONEachRow"),
                &head,
                source_file,
            ));
            assert!(!target.matches(
                &format!("INSERT INTO `db`.`{table}` FORMAT JSONEachRow"),
                &checkpoint,
                source_file,
            ));
        }

        let derived = serde_json::to_vec(&json!({
            "source_name": PROCESS_SOURCE,
            "session_id": PROCESS_SESSION,
        }))
        .expect("fixture serializes");
        for (stage, table) in [
            (GenerationStage::EventLinks, "event_links"),
            (GenerationStage::ToolIo, "tool_io"),
        ] {
            assert!(FaultTarget::GenerationStage {
                generation: 7,
                stage,
            }
            .matches(
                &format!("INSERT INTO `db`.`{table}` FORMAT JSONEachRow"),
                &derived,
                source_file,
            ));
        }

        for (stage, table) in [
            (GenerationStage::McpOpenEvents, "mcp_open_events"),
            (GenerationStage::McpOpenTurns, "mcp_open_turns"),
        ] {
            let query = format!(
                "INSERT INTO `db`.{table}\nSELECT * FROM `db`.events AS e WHERE e.session_id = '{PROCESS_SESSION}' AND e.source_file = '{source_file}' AND e.source_generation = 7"
            );
            assert!(FaultTarget::GenerationStage {
                generation: 7,
                stage,
            }
            .matches(&query, &[], source_file));
        }

        let candidate = candidate_publication_id(source_file, 7);
        for (stage, table) in [
            (
                GenerationStage::McpOpenHeader,
                "mcp_open_publication_headers",
            ),
            (
                GenerationStage::McpOpenReadiness,
                "mcp_open_generation_readiness",
            ),
        ] {
            let query = format!("INSERT INTO `db`.{table}\nVALUES ('{candidate}')");
            assert!(FaultTarget::GenerationStage {
                generation: 7,
                stage,
            }
            .matches(&query, &[], source_file));
        }

        let final_checkpoint = serde_json::to_vec(&json!({
            "source_name": PROCESS_SOURCE,
            "source_file": source_file,
            "source_generation": 7,
            "last_line": FIXTURE_LINES,
            "lifecycle": "active",
            "final_scan_complete": 1,
            "compatibility_prepared": 1,
        }))
        .expect("fixture serializes");
        assert!(FaultTarget::GenerationStage {
            generation: 7,
            stage: GenerationStage::FinalCheckpoint,
        }
        .matches(
            "INSERT INTO `db`.`ingest_checkpoint_transitions` FORMAT JSONEachRow",
            &final_checkpoint,
            source_file,
        ));

        let readiness = serde_json::to_vec(&json!({
            "source_name": PROCESS_SOURCE,
            "source_file": source_file,
            "source_generation": 7,
            "complete": 1,
            "compatibility_prepared": 1,
        }))
        .expect("fixture serializes");
        assert!(FaultTarget::GenerationStage {
            generation: 7,
            stage: GenerationStage::SourceReadiness,
        }
        .matches(
            "INSERT INTO `db`.`source_generation_publication_readiness` FORMAT JSONEachRow",
            &readiness,
            source_file,
        ));
    }
}
