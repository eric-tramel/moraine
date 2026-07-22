use anyhow::{anyhow, bail, Context, Result};
use flate2::{write::GzEncoder, Compression};
use moraine_config::{ClickHouseConfig, ClickHouseRequestCompression};
use reqwest::{
    header::{HeaderValue, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, USER_AGENT},
    Client, RequestBuilder, StatusCode, Url,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::io::Write;

pub mod envelope;
mod mcp_open_projection;
pub use envelope::{
    batch_statement_cap, envelope_error_kind, kill_query_prefix, unenveloped_statement_count,
    AllowanceResource, EnvelopeError, EnvelopeStatsSnapshot, QueryClass, QueryEnvelope,
};
pub use mcp_open_projection::{
    McpOpenGenerationReadiness, McpOpenHostRevision, McpOpenPublicationRequest, McpOpenSourceHead,
};
pub mod mcp_tool_names;

use envelope::{StatementDropGuard, MIN_SERVER_EXECUTION_SECONDS};
use std::sync::Arc;

const MAX_INSERT_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const MAX_QUERY_URL_BYTES: usize = 2 * 1024;
const DEFAULT_MAX_QUERY_SIZE_BYTES: usize = 256 * 1024;
use std::collections::{BTreeSet, HashSet};
use std::time::Duration;
const DEFAULT_USER_AGENT_ROLE: &str = "moraine-clickhouse";
const MIN_MIGRATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

fn migration_request_timeout(configured_seconds: f64) -> Duration {
    Duration::from_secs_f64(configured_seconds.max(MIN_MIGRATION_REQUEST_TIMEOUT.as_secs_f64()))
}

/// The bundled-default `[query_budgets.migration]` budget, for migration
/// callers that do not thread an operator-loaded `ValidatedQueryBudgets`.
fn default_migration_budget() -> moraine_config::ValidatedQueryBudget {
    static MIGRATION: std::sync::OnceLock<moraine_config::ValidatedQueryBudget> =
        std::sync::OnceLock::new();
    *MIGRATION.get_or_init(|| {
        moraine_config::ValidatedQueryBudgets::from_config(
            &moraine_config::QueryBudgetsConfig::default(),
        )
        .expect("bundled default query budgets are valid")
        .migration
    })
}

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    http: Client,
}

pub struct ClickHouseByteStream {
    response: reqwest::Response,
    /// Envelope accounting/cancellation for the streaming statement; the
    /// drop guard stays armed until the stream is read to completion, so an
    /// abandoned stream KILLs its server query.
    ticket: Option<StatementTicket>,
}

impl std::fmt::Debug for ClickHouseByteStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseByteStream")
            .field("response", &self.response)
            .finish_non_exhaustive()
    }
}

impl ClickHouseByteStream {
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        match self.response.chunk().await {
            Ok(Some(bytes)) => Ok(Some(bytes.to_vec())),
            Ok(None) => {
                // Stream fully consumed: the server statement is complete.
                if let Some(ticket) = self.ticket.as_mut() {
                    ticket.disarm();
                }
                Ok(None)
            }
            // Transport failure mid-stream: the server query may still be
            // running, so the ticket stays armed and dropping this stream
            // issues the bounded KILL.
            Err(error) => {
                Err(anyhow::Error::new(error).context("failed to read clickhouse response chunk"))
            }
        }
    }
}

/// How the transport envelopes one statement (amendment A3): buffered reads
/// set `wait_end_of_query=1` and decrement the cumulative allowance from the
/// trustworthy end-of-query summary; inserts skip the read-ceiling settings
/// (writes) but still count against the statement cap; streams enforce the
/// per-statement read ceiling but skip the cumulative decrement (their
/// headers flush before execution finishes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementProfile {
    BufferedRead,
    Insert,
    Stream,
}

struct ClickHouseRequestOptions<'a> {
    database: Option<&'a str>,
    async_insert: bool,
    default_format: Option<&'a str>,
    params: &'a [(&'a str, &'a str)],
    request_timeout: Option<Duration>,
    statement: StatementProfile,
}

/// Settings the active envelope owns per statement. Caller-supplied values
/// for these keys are dropped (the envelope wins for budget parameters);
/// every other caller parameter — readonly, format toggles, merge settings —
/// passes through untouched. `max_execution_time` is the one negotiated key:
/// the envelope keeps a caller value that is tighter than the remaining
/// request deadline (min-merge), preserving per-statement caps like
/// file-attention's.
const ENVELOPE_OWNED_PARAMS: [&str; 9] = [
    "query_id",
    "max_execution_time",
    "timeout_overflow_mode",
    "max_memory_usage",
    "max_bytes_before_external_group_by",
    "max_bytes_before_external_sort",
    "max_rows_to_read",
    "max_bytes_to_read",
    "wait_end_of_query",
];

/// Per-statement envelope bookkeeping returned by `request_builder`
/// alongside the HTTP request: allowance accounting on success, telemetry
/// classification on failure, and the cancel-on-drop guard (amendment A4).
struct StatementTicket {
    envelope: Arc<QueryEnvelope>,
    guard: Option<StatementDropGuard>,
    decrement_from_summary: bool,
}

impl StatementTicket {
    fn on_success(&mut self, response: &reqwest::Response) {
        if self.decrement_from_summary {
            if let Some((rows, bytes)) = parse_clickhouse_summary(response.headers()) {
                self.envelope.consume(rows, bytes);
            }
        }
        self.disarm();
    }

    /// A `ClickHouseHttpError` in the chain means the server answered — the
    /// statement finished server-side (rejected, killed, or failed), so the
    /// guard disarms. A pure transport failure (client timeout, connection
    /// reset) leaves it armed: the server query may still be running, and
    /// dropping the ticket issues the bounded KILL.
    fn on_error(&mut self, error: &anyhow::Error) {
        self.envelope
            .note_server_error_kind(clickhouse_error_kind(error));
        if error
            .chain()
            .any(|cause| cause.downcast_ref::<ClickHouseHttpError>().is_some())
        {
            self.disarm();
        }
    }

    fn disarm(&mut self) {
        if let Some(guard) = self.guard.as_mut() {
            guard.disarm();
        }
    }
}

/// Parse `read_rows`/`read_bytes` from an `X-ClickHouse-Summary` header
/// (JSON with string-encoded numbers).
fn parse_clickhouse_summary(headers: &reqwest::header::HeaderMap) -> Option<(u64, u64)> {
    let raw = headers.get("x-clickhouse-summary")?.to_str().ok()?;
    let value: Value = serde_json::from_str(raw).ok()?;
    let field = |name: &str| -> u64 {
        value
            .get(name)
            .and_then(|entry| {
                entry
                    .as_str()
                    .and_then(|text| text.parse::<u64>().ok())
                    .or_else(|| entry.as_u64())
            })
            .unwrap_or(0)
    };
    Some((field("read_rows"), field("read_bytes")))
}

#[derive(Deserialize)]
struct ClickHouseEnvelope<T> {
    data: Vec<T>,
    /// Set when the server hit an exception after response headers were
    /// already flushed (e.g. the query was killed or timed out mid-stream);
    /// `data` is truncated and must not be treated as a result.
    exception: Option<String>,
}

/// Budget-relevant classification of a ClickHouse server error, derived from
/// the server exception code so callers can map failures to their own
/// contracts without string matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClickHouseErrorKind {
    /// 159 TIMEOUT_EXCEEDED, 160 TOO_SLOW, 209 SOCKET_TIMEOUT.
    DeadlineExceeded,
    /// 158 TOO_MANY_ROWS, 202 TOO_MANY_SIMULTANEOUS_QUERIES,
    /// 241 MEMORY_LIMIT_EXCEEDED, 307 TOO_MANY_BYTES,
    /// 396 TOO_MANY_ROWS_OR_BYTES.
    ResourceExhausted,
    /// 394 QUERY_WAS_CANCELLED.
    QueryKilled,
    /// Any other server error, including responses without a parsable code.
    Other,
}

impl ClickHouseErrorKind {
    pub fn from_code(code: i32) -> Self {
        match code {
            159 | 160 | 209 => Self::DeadlineExceeded,
            158 | 202 | 241 | 307 | 396 => Self::ResourceExhausted,
            394 => Self::QueryKilled,
            _ => Self::Other,
        }
    }
}

/// Typed ClickHouse failure: a non-success HTTP response, or a server
/// exception embedded in a 200-OK body after headers were already flushed
/// (a killed or expired mid-stream query).
///
/// Always attached as the root of the returned `anyhow::Error`, so existing
/// `.context()` callers keep composing; recover it with
/// [`clickhouse_error_kind`] or `error.downcast_ref::<ClickHouseHttpError>()`.
#[derive(Debug)]
pub struct ClickHouseHttpError {
    status: StatusCode,
    code: Option<i32>,
    body: String,
}

impl ClickHouseHttpError {
    fn from_error_response(status: StatusCode, header_code: Option<i32>, body: String) -> Self {
        let code = header_code.or_else(|| extract_clickhouse_exception_code(&body));
        Self { status, code, body }
    }

    fn from_in_body_exception(body: String) -> Self {
        let code = extract_clickhouse_exception_code(&body);
        Self {
            status: StatusCode::OK,
            code,
            body,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Server exception code (`Code: NNN`) parsed from the response headers
    /// or body, when present.
    pub fn code(&self) -> Option<i32> {
        self.code
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    pub fn kind(&self) -> ClickHouseErrorKind {
        self.code
            .map_or(ClickHouseErrorKind::Other, ClickHouseErrorKind::from_code)
    }
}

impl std::fmt::Display for ClickHouseHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.status.is_success() {
            write!(
                f,
                "clickhouse reported an exception after the response started: {}",
                self.body
            )
        } else {
            // Keep the historical message; callers classify failures from it.
            write!(f, "clickhouse returned {}: {}", self.status, self.body)
        }
    }
}

impl std::error::Error for ClickHouseHttpError {}

/// Budget-relevant kind of the ClickHouse error in `error`'s chain, if any.
pub fn clickhouse_error_kind(error: &anyhow::Error) -> Option<ClickHouseErrorKind> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ClickHouseHttpError>())
        .map(ClickHouseHttpError::kind)
}

/// FORMAT JSON envelope drift: a completed 200-OK body that is not a JSON
/// envelope and carries no ClickHouse exception marker. The only failure
/// class `query_rows_with_params` may transparently retry as JSONEachRow.
#[derive(Debug)]
struct JsonEnvelopeParseError {
    body: String,
    source: serde_json::Error,
}

impl std::fmt::Display for JsonEnvelopeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid clickhouse JSON response: {}", self.body)
    }
}

impl std::error::Error for JsonEnvelopeParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: &'static str,
    pub name: &'static str,
    pub sql: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationProgress {
    Plan {
        applied: usize,
        pending: usize,
    },
    Started {
        index: usize,
        total: usize,
        version: &'static str,
        name: &'static str,
    },
    Applied {
        index: usize,
        total: usize,
        version: &'static str,
        name: &'static str,
    },
}

/// Result of comparing the server's `schema_migrations` ledger against this
/// build's `bundled_migrations()`. Both lists are sorted ascending.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SchemaSkew {
    /// Bundled versions the server has not applied (server is behind).
    pub missing_on_server: Vec<String>,
    /// Server-applied versions this build does not bundle (server is ahead).
    pub unknown_on_server: Vec<String>,
}

impl SchemaSkew {
    pub fn is_clean(&self) -> bool {
        self.missing_on_server.is_empty() && self.unknown_on_server.is_empty()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DoctorReport {
    pub clickhouse_healthy: bool,
    pub clickhouse_version: Option<String>,
    pub database: String,
    pub database_exists: bool,
    pub applied_migrations: Vec<String>,
    pub pending_migrations: Vec<String>,
    pub missing_tables: Vec<String>,
    pub publication: Option<PublicationDiagnostics>,
    pub errors: Vec<String>,
}

/// Aggregate readiness facts for atomic source-generation publication.
///
/// Replaying generations, append preparations, and mirror catch-up are normal
/// transient states. Ambiguous legacy ownership, blocked generations, and
/// writer conflicts require operator attention and make publication degraded.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PublicationDiagnostics {
    pub ambiguous_hostless_rows: u64,
    pub replaying_generations: u64,
    pub blocked_generations: u64,
    pub append_preparations: u64,
    pub blocked_append_preparations: u64,
    pub mirror_catchup_pending: u64,
    pub writer_conflicts: u64,
    #[serde(default)]
    pub issues: Vec<String>,
}

impl PublicationDiagnostics {
    /// Whether current reads are protected without a known fail-closed
    /// publication condition.
    pub fn is_healthy(&self) -> bool {
        self.ambiguous_hostless_rows == 0
            && self.blocked_generations == 0
            && self.blocked_append_preparations == 0
            && self.writer_conflicts == 0
    }
}

impl ClickHouseClient {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let user_agent = format!(
            "{DEFAULT_USER_AGENT_ROLE}/{} (pid={})",
            env!("CARGO_PKG_VERSION"),
            std::process::id()
        );
        Self::new_with_user_agent(cfg, user_agent)
    }

    /// Construct a client whose every HTTP request carries the supplied
    /// prevalidated-at-construction User-Agent.
    pub fn new_with_user_agent(cfg: ClickHouseConfig, user_agent: impl AsRef<str>) -> Result<Self> {
        let timeout = Duration::from_secs_f64(cfg.timeout_seconds.max(1.0));
        let user_agent = HeaderValue::try_from(user_agent.as_ref())
            .context("invalid ClickHouse HTTP User-Agent")?;
        let mut default_headers = reqwest::header::HeaderMap::with_capacity(1);
        default_headers.insert(USER_AGENT, user_agent);
        let http = Client::builder()
            .timeout(timeout)
            .default_headers(default_headers)
            .build()
            .context("failed to construct reqwest client")?;

        Ok(Self { cfg, http })
    }

    pub fn config(&self) -> &ClickHouseConfig {
        &self.cfg
    }

    fn base_url(&self) -> Result<Url> {
        Url::parse(&self.cfg.url).context("invalid ClickHouse URL")
    }

    async fn request_builder(
        &self,
        query: &str,
        mut body: Vec<u8>,
        options: ClickHouseRequestOptions<'_>,
    ) -> Result<(RequestBuilder, Option<StatementTicket>)> {
        // ClickHouse accepts a complete SQL statement in the POST body. Keep
        // short queries in the request target for compatibility with insert
        // payloads, but move generated SQL out of the URL before percent
        // encoding can exceed the HTTP client's URI limit.
        let query_in_body = body.is_empty() && query.len() > MAX_QUERY_URL_BYTES;
        if query_in_body {
            body.extend_from_slice(query.as_bytes());
        }
        let complete_sql_in_body = query_in_body || (query.is_empty() && !body.is_empty());
        if complete_sql_in_body && body.len() > MAX_INSERT_PAYLOAD_BYTES {
            bail!(
                "ClickHouse SQL statement is {} bytes; maximum is {} bytes",
                body.len(),
                MAX_INSERT_PAYLOAD_BYTES
            );
        }

        // Envelope enforcement (issue #600): with an active envelope every
        // statement is admitted against the shared deadline / statement cap /
        // read allowance, carries a child query id preserving the KILL prefix
        // contract, and gets budget-derived server settings. Without one the
        // statement executes exactly as before the envelope existed, plus a
        // process counter — the fail-closed flip is a later work item
        // (amendment A2).
        let mut envelope_params: Vec<(&'static str, String)> = Vec::new();
        let mut envelope_request_timeout = None;
        let ticket = match QueryEnvelope::current() {
            Err(_) => {
                envelope::record_unenveloped_statement();
                None
            }
            Ok(active) => {
                let admission = active.admit_statement().map_err(anyhow::Error::new)?;
                active.stamp_cancel_target(self);

                // Min-merge a caller-supplied max_execution_time: the tighter
                // of the caller's per-statement cap and the remaining request
                // deadline wins, floored so integer-flooring servers cannot
                // read a fractional value as unlimited.
                let caller_execution_limit = options
                    .params
                    .iter()
                    .find_map(|(name, value)| (*name == "max_execution_time").then_some(*value))
                    .and_then(|value| value.parse::<f64>().ok())
                    .filter(|value| value.is_finite() && *value > 0.0);
                let remaining_seconds = admission.remaining.as_secs_f64();
                let effective_execution_seconds = caller_execution_limit
                    .map_or(remaining_seconds, |limit| limit.min(remaining_seconds))
                    .max(MIN_SERVER_EXECUTION_SECONDS);

                envelope_params.push(("query_id", admission.query_id.clone()));
                envelope_params.push((
                    "max_execution_time",
                    format!("{effective_execution_seconds:.3}"),
                ));
                envelope_params.push(("timeout_overflow_mode", "throw".to_string()));
                envelope_params.push(("max_memory_usage", active.memory_bytes().to_string()));
                envelope_params.push((
                    "max_bytes_before_external_group_by",
                    active.spill_bytes().to_string(),
                ));
                envelope_params.push((
                    "max_bytes_before_external_sort",
                    active.spill_bytes().to_string(),
                ));
                if options.statement != StatementProfile::Insert {
                    // Read ceilings come from the REMAINING request allowance
                    // (amendment A3); inserts are writes and skip them.
                    envelope_params
                        .push(("max_rows_to_read", admission.rows_remaining.to_string()));
                    envelope_params
                        .push(("max_bytes_to_read", admission.bytes_remaining.to_string()));
                }
                if options.statement == StatementProfile::BufferedRead {
                    // Makes X-ClickHouse-Summary reflect completed execution,
                    // so the cumulative decrement is sound (amendment A3).
                    envelope_params.push(("wait_end_of_query", "1".to_string()));
                }

                // Give the server's structured TIMEOUT_EXCEEDED time to win
                // over a client abort. Migration statements keep their
                // caller-supplied (operator-honoring) client bound.
                if options.request_timeout.is_none() && active.class() != QueryClass::Migration {
                    envelope_request_timeout = Some(admission.remaining + Duration::from_secs(2));
                }

                let guard = active.arms_cancel_guards().then(|| {
                    StatementDropGuard::new(
                        admission.query_id.clone(),
                        self.clone(),
                        *active.admin_budget(),
                    )
                });
                Some(StatementTicket {
                    decrement_from_summary: options.statement == StatementProfile::BufferedRead,
                    guard,
                    envelope: active,
                })
            }
        };

        let mut url = self.base_url()?;
        {
            let mut qp = url.query_pairs_mut();
            // An empty query means the complete SQL statement is carried in
            // the POST body. This is required for generated statements that
            // can exceed practical HTTP request-target limits.
            if !query.is_empty() && !query_in_body {
                qp.append_pair("query", query);
            }
            if complete_sql_in_body && body.len() > DEFAULT_MAX_QUERY_SIZE_BYTES {
                let max_query_size = body.len().next_power_of_two();
                qp.append_pair("max_query_size", &max_query_size.to_string());
            }
            if let Some(database) = options.database {
                qp.append_pair("database", database);
            }
            if let Some(default_format) = options.default_format {
                qp.append_pair("default_format", default_format);
            }
            if options.async_insert && self.cfg.async_insert {
                qp.append_pair("async_insert", "1");
                if self.cfg.wait_for_async_insert {
                    qp.append_pair("wait_for_async_insert", "1");
                }
            }
            for (key, value) in options.params {
                if ticket.is_some() && ENVELOPE_OWNED_PARAMS.contains(key) {
                    continue;
                }
                qp.append_pair(key, value);
            }
            for (key, value) in &envelope_params {
                qp.append_pair(key, value);
            }
        }

        // ClickHouse HTTP treats GET as readonly, so use POST for both reads and writes.
        let (body, content_encoding) = match (self.cfg.request_compression, body.is_empty()) {
            (_, true) | (ClickHouseRequestCompression::None, false) => (body, None),
            (ClickHouseRequestCompression::Gzip, false) => {
                let compressed = tokio::task::spawn_blocking(move || {
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder
                        .write_all(&body)
                        .context("failed to gzip ClickHouse request body")?;
                    encoder
                        .finish()
                        .context("failed to finish gzip ClickHouse request body")
                })
                .await
                .context("ClickHouse request compression task failed")??;
                (compressed, Some("gzip"))
            }
        };
        let payload_len = body.len();
        let mut req = self
            .http
            .post(url)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            // Some ClickHouse builds require an explicit Content-Length on POST.
            .header(CONTENT_LENGTH, payload_len)
            .body(body);

        if let Some(content_encoding) = content_encoding {
            req = req.header(CONTENT_ENCODING, content_encoding);
        }

        if let Some(timeout) = options.request_timeout.or(envelope_request_timeout) {
            req = req.timeout(timeout);
        }

        if !self.cfg.username.is_empty() {
            req = req.basic_auth(self.cfg.username.clone(), Some(self.cfg.password.clone()));
        }

        Ok((req, ticket))
    }

    async fn send_checked_response(&self, req: RequestBuilder) -> Result<reqwest::Response> {
        let response = req.send().await.context("clickhouse request failed")?;
        let status = response.status();
        if !status.is_success() {
            let header_code = response
                .headers()
                .get("x-clickhouse-exception-code")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<i32>().ok());
            let text = response.text().await.with_context(|| {
                format!(
                    "failed to read clickhouse response body (status {})",
                    status
                )
            })?;
            return Err(anyhow::Error::new(
                ClickHouseHttpError::from_error_response(status, header_code, text),
            ));
        }

        Ok(response)
    }

    pub async fn request_text(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
    ) -> Result<String> {
        self.request_text_with_params(query, body, database, async_insert, default_format, &[])
            .await
    }

    pub async fn request_text_with_params(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
        params: &[(&str, &str)],
    ) -> Result<String> {
        self.request_text_with_params_and_timeout(
            query,
            body,
            database,
            async_insert,
            default_format,
            params,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_text_with_params_and_timeout(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
        params: &[(&str, &str)],
        request_timeout: Option<Duration>,
    ) -> Result<String> {
        self.request_text_inner(
            query,
            body,
            database,
            async_insert,
            default_format,
            params,
            request_timeout,
            StatementProfile::BufferedRead,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_text_inner(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
        params: &[(&str, &str)],
        request_timeout: Option<Duration>,
        statement: StatementProfile,
    ) -> Result<String> {
        let (req, mut ticket) = self
            .request_builder(
                query,
                body.unwrap_or_default(),
                ClickHouseRequestOptions {
                    database,
                    async_insert,
                    default_format,
                    params,
                    request_timeout,
                    statement,
                },
            )
            .await?;
        let response = match self.send_checked_response(req).await {
            Ok(response) => {
                if let Some(ticket) = ticket.as_mut() {
                    ticket.on_success(&response);
                }
                response
            }
            Err(error) => {
                if let Some(ticket) = ticket.as_mut() {
                    ticket.on_error(&error);
                }
                return Err(error);
            }
        };
        let status = response.status();
        let text = response.text().await.with_context(|| {
            format!(
                "failed to read clickhouse response body (status {})",
                status
            )
        })?;

        Ok(text)
    }

    pub async fn request_stream_with_params(
        &self,
        query: &str,
        database: Option<&str>,
        default_format: Option<&str>,
        params: &[(&str, &str)],
        request_timeout: Option<Duration>,
    ) -> Result<ClickHouseByteStream> {
        let (req, mut ticket) = self
            .request_builder(
                query,
                Vec::new(),
                ClickHouseRequestOptions {
                    database,
                    async_insert: false,
                    default_format,
                    params,
                    request_timeout,
                    statement: StatementProfile::Stream,
                },
            )
            .await?;
        let response = match self.send_checked_response(req).await {
            Ok(response) => response,
            Err(error) => {
                if let Some(ticket) = ticket.as_mut() {
                    ticket.on_error(&error);
                }
                return Err(error);
            }
        };

        Ok(ClickHouseByteStream { response, ticket })
    }

    pub async fn ping(&self) -> Result<()> {
        let response = self
            .request_text("SELECT 1", None, Some("system"), false, None)
            .await?;
        if response.trim() == "1" {
            Ok(())
        } else {
            Err(anyhow!("unexpected ping response: {}", response.trim()))
        }
    }

    pub async fn version(&self) -> Result<String> {
        let rows: Vec<Value> = self
            .query_json_data("SELECT version() AS version", Some("system"))
            .await?;
        let version = rows
            .first()
            .and_then(|row| row.get("version"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing version in payload"))?;

        Ok(version.to_string())
    }

    pub async fn query_json_each_row<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        self.query_json_each_row_with_params(query, database, &[])
            .await
    }

    pub async fn query_json_each_row_with_params<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
        params: &[(&str, &str)],
    ) -> Result<Vec<T>> {
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text_with_params(query, None, database, false, None, params)
            .await?;
        match serde_json::Deserializer::from_str(&raw)
            .into_iter::<T>()
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(rows) => Ok(rows),
            // A killed/expired streaming query appends exception text after
            // 200-OK headers; surface the server error, not a parse failure.
            Err(_) if body_contains_clickhouse_exception(&raw) => Err(anyhow::Error::new(
                ClickHouseHttpError::from_in_body_exception(raw),
            )),
            Err(source) => {
                Err(anyhow::Error::new(source).context("failed to parse JSONEachRow response"))
            }
        }
    }

    pub async fn query_json_data<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        self.query_json_data_with_params(query, database, &[]).await
    }

    pub async fn query_json_data_with_params<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
        params: &[(&str, &str)],
    ) -> Result<Vec<T>> {
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text_with_params(query, None, database, false, Some("JSON"), params)
            .await?;
        let envelope: ClickHouseEnvelope<T> = match serde_json::from_str(&raw) {
            Ok(envelope) => envelope,
            Err(source) => {
                // A kill/timeout can truncate the envelope after 200-OK
                // headers were flushed; report those as server errors so the
                // JSONEachRow fallback never re-runs already-rejected work.
                let error = if body_contains_clickhouse_exception(&raw) {
                    anyhow::Error::new(ClickHouseHttpError::from_in_body_exception(raw))
                } else {
                    anyhow::Error::new(JsonEnvelopeParseError { body: raw, source })
                };
                return Err(error);
            }
        };
        if let Some(exception) = envelope.exception {
            return Err(anyhow::Error::new(
                ClickHouseHttpError::from_in_body_exception(exception),
            ));
        }
        Ok(envelope.data)
    }

    pub async fn query_rows<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        self.query_rows_with_params(query, database, &[]).await
    }

    pub async fn query_rows_with_params<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
        params: &[(&str, &str)],
    ) -> Result<Vec<T>> {
        if has_explicit_json_each_row_format(query) {
            return self
                .query_json_each_row_with_params(query, database, params)
                .await;
        }

        match self
            .query_json_data_with_params(query, database, params)
            .await
        {
            Ok(rows) => Ok(rows),
            // FORMAT JSON envelope drift is the only failure where silently
            // re-running the query is safe. Typed deadline/resource/kill
            // errors and HTTP failures propagate: the server already refused
            // or killed the work once.
            Err(err) if json_envelope_drift_retry_allowed(&err) => {
                self.query_json_each_row_with_params(query, database, params)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        self.insert_json_rows_with_mode(table, rows, true).await
    }

    /// Insert rows synchronously even when the client is configured for
    /// ClickHouse async inserts. Projection maintenance uses this boundary so
    /// canonical events are visible before it rebuilds their session.
    pub async fn insert_json_rows_sync(&self, table: &str, rows: &[Value]) -> Result<()> {
        self.insert_json_rows_with_mode(table, rows, false).await
    }

    async fn insert_json_rows_with_mode(
        &self,
        table: &str,
        rows: &[Value],
        async_insert: bool,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_identifier(table)
        );
        let mut payload = Vec::<u8>::new();
        for row in rows {
            let line = serde_json::to_vec(row).context("failed to encode JSON row")?;
            if !payload.is_empty()
                && payload.len().saturating_add(line.len()).saturating_add(1)
                    > MAX_INSERT_PAYLOAD_BYTES
            {
                self.insert_request_text(&query, std::mem::take(&mut payload), async_insert)
                    .await?;
            }
            payload.extend_from_slice(&line);
            payload.push(b'\n');
        }

        if !payload.is_empty() {
            self.insert_request_text(&query, payload, async_insert)
                .await?;
        }
        Ok(())
    }

    /// Insert-profile transport call for write statements issued as text
    /// (INSERT ... SELECT, INSERT ... VALUES, lightweight DELETE): an active
    /// envelope still allocates a child query id, deadline, and memory
    /// settings and counts the statement against the cap, but skips the
    /// read-ceiling settings — these are writes (amendment A3).
    pub(crate) async fn mutation_request_text_with_params_and_timeout(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        params: &[(&str, &str)],
        request_timeout: Option<Duration>,
    ) -> Result<String> {
        self.request_text_inner(
            query,
            body,
            database,
            false,
            None,
            params,
            request_timeout,
            StatementProfile::Insert,
        )
        .await
    }

    /// Insert-profile transport call: an active envelope still allocates a
    /// child query id, deadline, and memory settings and counts the
    /// statement against the cap, but skips the read-ceiling settings —
    /// inserts are writes (amendment A3).
    async fn insert_request_text(
        &self,
        query: &str,
        payload: Vec<u8>,
        async_insert: bool,
    ) -> Result<String> {
        self.request_text_inner(
            query,
            Some(payload),
            None,
            async_insert,
            None,
            &[],
            None,
            StatementProfile::Insert,
        )
        .await
    }

    pub async fn run_migrations(&self) -> Result<Vec<String>> {
        self.run_migrations_with_progress(|_| {}).await
    }

    /// Migration-class envelope for schema/read-model work driven by this
    /// client (the migration runner itself, and the `migrate`/`up` command
    /// path around `backfill_mcp_open_read_model*` / projection reclaim).
    /// The absolute deadline honors `max(configured migration budget,
    /// operator client timeout, 300s)` per amendment A5, so an envelope can
    /// never be tighter than the client bound migrations honored before
    /// envelopes existed.
    pub fn migration_envelope(
        &self,
        budget: &moraine_config::ValidatedQueryBudget,
    ) -> Arc<QueryEnvelope> {
        QueryEnvelope::new_migration(
            "migration",
            budget,
            migration_request_timeout(self.cfg.timeout_seconds),
        )
    }

    pub async fn run_migrations_with_progress<F>(&self, on_progress: F) -> Result<Vec<String>>
    where
        F: FnMut(MigrationProgress),
    {
        self.run_migrations_with_progress_and_budget(&default_migration_budget(), on_progress)
            .await
    }

    /// Run pending migrations with every statement scoped inside its own
    /// Migration-class envelope built from `migration_budget`.
    ///
    /// Envelope granularity is deliberately per statement, not per run: the
    /// pre-envelope behavior gave every statement the full
    /// `migration_request_timeout` client bound, and one envelope spanning
    /// the run (or even one multi-statement migration) would cap the SUM of
    /// statement times on upgrade — a regression amendment A5 forbids. The
    /// per-statement envelopes deliberately shadow any envelope the caller
    /// scoped for the same reason. Migration statements get no drop-guard
    /// KILLs by class; the operator-honoring client timeout stays as the
    /// reqwest bound.
    pub async fn run_migrations_with_progress_and_budget<F>(
        &self,
        migration_budget: &moraine_config::ValidatedQueryBudget,
        mut on_progress: F,
    ) -> Result<Vec<String>>
    where
        F: FnMut(MigrationProgress),
    {
        validate_identifier(&self.cfg.database)?;

        // Backfills may legitimately outlive the ordinary interactive-query
        // deadline.  Keep any larger operator-configured timeout, but prevent
        // a client-side deadline from abandoning a still-running migration
        // and overlapping it with a retry.
        let migration_request_timeout = migration_request_timeout(self.cfg.timeout_seconds);

        // Plan preflight (database/ledger DDL + applied-version read) under
        // one Migration envelope; each pending statement below gets its own.
        let (bundled_count, pending) = self
            .migration_envelope(migration_budget)
            .scope(async {
                self.request_text(
                    &format!(
                        "CREATE DATABASE IF NOT EXISTS {}",
                        escape_identifier(&self.cfg.database)
                    ),
                    None,
                    None,
                    false,
                    None,
                )
                .await?;

                self.ensure_migration_ledger().await?;
                let applied = self.applied_migration_versions().await?;
                let bundled = bundled_migrations();
                let bundled_count = bundled.len();
                let pending = bundled
                    .into_iter()
                    .filter(|migration| !applied.contains(migration.version))
                    .collect::<Vec<_>>();
                Ok::<_, anyhow::Error>((bundled_count, pending))
            })
            .await?;
        let total = pending.len();
        on_progress(MigrationProgress::Plan {
            applied: bundled_count.saturating_sub(total),
            pending: total,
        });

        let mut executed = Vec::with_capacity(total);
        for (offset, migration) in pending.into_iter().enumerate() {
            let index = offset + 1;
            on_progress(MigrationProgress::Started {
                index,
                total,
                version: migration.version,
                name: migration.name,
            });

            let sql = materialize_migration_sql(migration.sql, &self.cfg.database)?;
            for statement in split_sql_statements(&sql) {
                self.migration_envelope(migration_budget)
                    .scope(self.request_text_with_params_and_timeout(
                        &statement,
                        None,
                        Some(&self.cfg.database),
                        false,
                        None,
                        &[],
                        Some(migration_request_timeout),
                    ))
                    .await
                    .with_context(|| {
                        format!(
                            "failed migration {} statement: {}",
                            migration.name,
                            truncate_for_error(&statement)
                        )
                    })?;
            }

            let log_stmt = format!(
                "INSERT INTO {}.schema_migrations (version, name) VALUES ({}, {})",
                escape_identifier(&self.cfg.database),
                escape_literal(migration.version),
                escape_literal(migration.name)
            );
            self.migration_envelope(migration_budget)
                .scope(self.request_text_with_params_and_timeout(
                    &log_stmt,
                    None,
                    Some(&self.cfg.database),
                    false,
                    None,
                    &[],
                    Some(migration_request_timeout),
                ))
                .await
                .with_context(|| format!("failed to record migration {}", migration.name))?;

            executed.push(migration.version.to_string());
            on_progress(MigrationProgress::Applied {
                index,
                total,
                version: migration.version,
                name: migration.name,
            });
        }

        Ok(executed)
    }

    pub async fn pending_migration_versions(&self) -> Result<Vec<String>> {
        self.ensure_migration_ledger().await?;
        let applied = self.applied_migration_versions().await?;
        Ok(bundled_migrations()
            .into_iter()
            .filter(|m| !applied.contains(m.version))
            .map(|m| m.version.to_string())
            .collect())
    }

    /// Probe schema skew between the server's migration ledger and this
    /// build's bundled migrations. Strictly read-only: unlike
    /// `pending_migration_versions`, it never creates the ledger table, so it
    /// is safe to run against backends moraine does not own. A missing ledger
    /// (or missing database) reports every bundled version as missing.
    pub async fn schema_skew(&self) -> Result<SchemaSkew> {
        let bundled: Vec<&str> = bundled_migrations().iter().map(|m| m.version).collect();
        let applied: Vec<String> = if self.migration_ledger_exists().await? {
            self.applied_migration_versions()
                .await?
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };
        Ok(compute_schema_skew(&bundled, &applied))
    }

    pub async fn doctor_report(&self) -> Result<DoctorReport> {
        let mut report = DoctorReport {
            clickhouse_healthy: false,
            clickhouse_version: None,
            database: self.cfg.database.clone(),
            database_exists: false,
            applied_migrations: Vec::new(),
            pending_migrations: Vec::new(),
            missing_tables: Vec::new(),
            publication: None,
            errors: Vec::new(),
        };

        match self.ping().await {
            Ok(()) => {
                report.clickhouse_healthy = true;
            }
            Err(err) => {
                report.errors.push(format!("ping failed: {err}"));
                return Ok(report);
            }
        }

        match self.version().await {
            Ok(version) => report.clickhouse_version = Some(version),
            Err(err) => report.errors.push(format!("version query failed: {err}")),
        }

        #[derive(Deserialize)]
        struct ExistsRow {
            exists: u8,
        }

        let exists_query = format!(
            "SELECT toUInt8(count() > 0) AS exists FROM system.databases WHERE name = {}",
            escape_literal(&self.cfg.database)
        );

        match self
            .query_json_data::<ExistsRow>(&exists_query, Some("system"))
            .await
        {
            Ok(rows) => {
                report.database_exists = rows.first().map(|r| r.exists == 1).unwrap_or(false)
            }
            Err(err) => {
                report
                    .errors
                    .push(format!("database existence query failed: {err}"));
                return Ok(report);
            }
        }

        if !report.database_exists {
            report
                .errors
                .push(format!("database '{}' does not exist", self.cfg.database));
            return Ok(report);
        }

        match self.applied_migration_versions().await {
            Ok(applied) => {
                let mut versions: Vec<String> = applied.into_iter().collect();
                versions.sort();
                report.applied_migrations = versions;
            }
            Err(err) => report
                .errors
                .push(format!("failed to read migration ledger: {err}")),
        }

        let pending = bundled_migrations()
            .into_iter()
            .filter(|m| !report.applied_migrations.iter().any(|v| v == m.version))
            .map(|m| m.version.to_string())
            .collect::<Vec<_>>();
        report.pending_migrations = pending;

        #[derive(Deserialize)]
        struct TableRow {
            name: String,
        }

        let table_query = format!(
            "SELECT name FROM system.tables WHERE database = {}",
            escape_literal(&self.cfg.database)
        );

        let required = [
            "raw_events",
            "events",
            "event_links",
            "tool_io",
            "ingest_errors",
            "ingest_checkpoints",
            "ingest_heartbeats",
            "search_documents",
            "search_postings",
            "search_conversation_terms",
            "search_term_stats",
            "search_corpus_stats",
            "search_query_log",
            "search_hit_log",
            "search_interaction_log",
            "mcp_open_sessions",
            "mcp_open_turns",
            "mcp_open_events",
            "mcp_open_dirty_sessions",
            "mcp_open_projection_state",
            "mcp_open_publication_headers",
            "mcp_open_generation_readiness",
            "mcp_open_backfill_plans",
            "published_source_generations",
            "ingest_checkpoint_transitions",
            "source_generation_publication_readiness",
            "ingest_append_control",
            "publication_diagnostic_events",
            "v_published_source_generation_history",
            "v_current_published_source_generations",
            "v_current_ingest_checkpoint_transitions",
            "v_current_source_generation_publication_readiness",
            "v_current_ingest_append_control",
            "v_live_events",
            "v_live_event_links",
            "v_live_tool_io",
            "v_live_search_documents",
            "v_live_search_postings",
            "v_mcp_open_publication_headers",
            "v_current_mcp_open_generation_readiness",
            "v_publication_diagnostics",
            "schema_migrations",
        ];

        let existing_tables = match self
            .query_json_data::<TableRow>(&table_query, Some("system"))
            .await
        {
            Ok(rows) => {
                let existing = rows.into_iter().map(|r| r.name).collect::<HashSet<_>>();
                report.missing_tables = required
                    .iter()
                    .filter(|name| !existing.contains(**name))
                    .map(|name| (*name).to_string())
                    .collect();
                Some(existing)
            }
            Err(err) => {
                report.errors.push(format!("table listing failed: {err}"));
                None
            }
        };

        if existing_tables
            .as_ref()
            .is_some_and(|tables| tables.contains("v_publication_diagnostics"))
        {
            match self.publication_diagnostics().await {
                Ok(diagnostics) => report.publication = Some(diagnostics),
                Err(err) => report
                    .errors
                    .push(format!("publication diagnostics failed: {err}")),
            }
        }

        Ok(report)
    }

    /// Read the single-row publication readiness aggregate installed by the
    /// atomic source-publication schema migration.
    pub async fn publication_diagnostics(&self) -> Result<PublicationDiagnostics> {
        let query = format!(
            "SELECT ambiguous_hostless_rows, replaying_generations, blocked_generations, \
             append_preparations, blocked_append_preparations, mirror_catchup_pending, \
             writer_conflicts, issues \
             FROM {}.v_publication_diagnostics FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database)
        );
        let rows: Vec<PublicationDiagnostics> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        match rows.as_slice() {
            [diagnostics] => Ok(diagnostics.clone()),
            _ => Err(anyhow!(
                "publication diagnostics view returned {} rows; expected exactly one",
                rows.len()
            )),
        }
    }

    async fn ensure_migration_ledger(&self) -> Result<()> {
        self.request_text(
            &format!(
                "CREATE TABLE IF NOT EXISTS {}.schema_migrations (\
                 version String, \
                 name String, \
                 applied_at DateTime64(3) DEFAULT now64(3)\
                 ) ENGINE = ReplacingMergeTree(applied_at) \
                 ORDER BY (version)",
                escape_identifier(&self.cfg.database)
            ),
            None,
            Some(&self.cfg.database),
            false,
            None,
        )
        .await?;

        Ok(())
    }

    async fn migration_ledger_exists(&self) -> Result<bool> {
        #[derive(Deserialize)]
        struct ExistsRow {
            exists: u8,
        }

        let query = format!(
            "SELECT toUInt8(count() > 0) AS exists FROM system.tables \
             WHERE database = {} AND name = 'schema_migrations'",
            escape_literal(&self.cfg.database)
        );

        let rows: Vec<ExistsRow> = self.query_json_data(&query, Some("system")).await?;
        Ok(rows.first().map(|r| r.exists == 1).unwrap_or(false))
    }

    async fn applied_migration_versions(&self) -> Result<HashSet<String>> {
        #[derive(Deserialize)]
        struct Row {
            version: String,
        }

        let query = format!(
            "SELECT version FROM {}.schema_migrations GROUP BY version",
            escape_identifier(&self.cfg.database)
        );

        let rows: Vec<Row> = self
            .query_json_data(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().map(|row| row.version).collect())
    }
}

pub fn bundled_migrations() -> Vec<Migration> {
    vec![
        Migration {
            version: "001",
            name: "001_schema.sql",
            sql: include_str!("../../../sql/001_schema.sql"),
        },
        Migration {
            version: "002",
            name: "002_views.sql",
            sql: include_str!("../../../sql/002_views.sql"),
        },
        Migration {
            version: "003",
            name: "003_ingest_heartbeats.sql",
            sql: include_str!("../../../sql/003_ingest_heartbeats.sql"),
        },
        Migration {
            version: "004",
            name: "004_search_index.sql",
            sql: include_str!("../../../sql/004_search_index.sql"),
        },
        Migration {
            version: "005",
            name: "005_watcher_heartbeat_metrics.sql",
            sql: include_str!("../../../sql/005_watcher_heartbeat_metrics.sql"),
        },
        Migration {
            version: "006",
            name: "006_search_stats_authoritative_views.sql",
            sql: include_str!("../../../sql/006_search_stats_authoritative_views.sql"),
        },
        Migration {
            version: "007",
            name: "007_event_links_external_id.sql",
            sql: include_str!("../../../sql/007_event_links_external_id.sql"),
        },
        Migration {
            version: "008",
            name: "008_categorical_domain_contracts.sql",
            sql: include_str!("../../../sql/008_categorical_domain_contracts.sql"),
        },
        Migration {
            version: "009",
            name: "009_search_documents_codex_flag.sql",
            sql: include_str!("../../../sql/009_search_documents_codex_flag.sql"),
        },
        Migration {
            version: "010",
            name: "010_search_conversation_terms.sql",
            sql: include_str!("../../../sql/010_search_conversation_terms.sql"),
        },
        Migration {
            version: "011",
            name: "011_rename_provider_to_harness.sql",
            sql: include_str!("../../../sql/011_rename_provider_to_harness.sql"),
        },
        Migration {
            version: "012",
            name: "012_add_inference_provider_and_rename_claude.sql",
            sql: include_str!("../../../sql/012_add_inference_provider_and_rename_claude.sql"),
        },
        Migration {
            version: "013",
            name: "013_canonical_reasoning_metadata.sql",
            sql: include_str!("../../../sql/013_canonical_reasoning_metadata.sql"),
        },
        Migration {
            version: "014",
            name: "014_harmonized_token_accounting.sql",
            sql: include_str!("../../../sql/014_harmonized_token_accounting.sql"),
        },
        Migration {
            version: "015",
            name: "015_sqlite_checkpoint_cursor.sql",
            sql: include_str!("../../../sql/015_sqlite_checkpoint_cursor.sql"),
        },
        Migration {
            version: "016",
            name: "016_add_event_cwd.sql",
            sql: include_str!("../../../sql/016_add_event_cwd.sql"),
        },
        Migration {
            version: "017",
            name: "017_heartbeat_backend_sinks.sql",
            sql: include_str!("../../../sql/017_heartbeat_backend_sinks.sql"),
        },
        Migration {
            version: "018",
            name: "018_checkpoint_host.sql",
            sql: include_str!("../../../sql/018_checkpoint_host.sql"),
        },
        Migration {
            version: "019",
            name: "019_dedup_conversation_trace_final.sql",
            sql: include_str!("../../../sql/019_dedup_conversation_trace_final.sql"),
        },
        Migration {
            version: "020",
            name: "020_purge_empty_session_claude_code.sql",
            sql: include_str!("../../../sql/020_purge_empty_session_claude_code.sql"),
        },
        Migration {
            version: "021",
            name: "021_file_attention_normalization.sql",
            sql: include_str!("../../../sql/021_file_attention_normalization.sql"),
        },
        Migration {
            version: "022",
            name: "022_heartbeat_redaction_counts.sql",
            sql: include_str!("../../../sql/022_heartbeat_redaction_counts.sql"),
        },
        Migration {
            version: "023",
            name: "023_search_documents_event_uid_bloom.sql",
            sql: include_str!("../../../sql/023_search_documents_event_uid_bloom.sql"),
        },
        Migration {
            version: "024",
            name: "024_add_event_author.sql",
            sql: include_str!("../../../sql/024_add_event_author.sql"),
        },
        Migration {
            version: "025",
            name: "025_kimi_subagent_parent_links.sql",
            sql: include_str!("../../../sql/025_kimi_subagent_parent_links.sql"),
        },
        Migration {
            version: "026",
            name: "026_file_attention_project_roots.sql",
            sql: include_str!("../../../sql/026_file_attention_project_roots.sql"),
        },
        Migration {
            version: "027",
            name: "027_mcp_open_read_model.sql",
            sql: include_str!("../../../sql/027_mcp_open_read_model.sql"),
        },
        Migration {
            version: "028",
            name: "028_refresh_mcp_open_source_metadata.sql",
            sql: include_str!("../../../sql/028_refresh_mcp_open_source_metadata.sql"),
        },
        Migration {
            version: "029",
            name: "029_reset_mcp_open_projection.sql",
            sql: include_str!("../../../sql/029_reset_mcp_open_projection.sql"),
        },
        Migration {
            version: "030",
            name: "030_refresh_omp_session_metadata.sql",
            sql: include_str!("../../../sql/030_refresh_omp_session_metadata.sql"),
        },
        Migration {
            version: "031",
            name: "031_atomic_source_publication_control.sql",
            sql: include_str!("../../../sql/031_atomic_source_publication_control.sql"),
        },
        Migration {
            version: "032",
            name: "032_source_host_live_read_model.sql",
            sql: include_str!("../../../sql/032_source_host_live_read_model.sql"),
        },
        Migration {
            version: "033",
            name: "033_mcp_atomic_publication_bridge.sql",
            sql: include_str!("../../../sql/033_mcp_atomic_publication_bridge.sql"),
        },
        Migration {
            version: "034",
            name: "034_batched_mcp_open_backfill.sql",
            sql: include_str!("../../../sql/034_batched_mcp_open_backfill.sql"),
        },
        Migration {
            version: "035",
            name: "035_mcp_list_metadata_projection.sql",
            sql: include_str!("../../../sql/035_mcp_list_metadata_projection.sql"),
        },
    ]
}

/// Pure comparison of two migration-version lists; the basis of
/// `ClickHouseClient::schema_skew`. Output vectors are sorted and deduplicated.
pub fn compute_schema_skew<B: AsRef<str>, S: AsRef<str>>(
    bundled_versions: &[B],
    server_versions: &[S],
) -> SchemaSkew {
    let bundled: BTreeSet<&str> = bundled_versions.iter().map(AsRef::as_ref).collect();
    let server: BTreeSet<&str> = server_versions.iter().map(AsRef::as_ref).collect();

    SchemaSkew {
        missing_on_server: bundled
            .difference(&server)
            .map(|v| (*v).to_string())
            .collect(),
        unknown_on_server: server
            .difference(&bundled)
            .map(|v| (*v).to_string())
            .collect(),
    }
}

/// Skew policy for non-default backends, which moraine NEVER migrates (the
/// default backend keeps using `run_migrations` and must not go through this).
/// Server behind => hard error; server ahead => hard error unless the
/// backend's `allow_newer_server` is set. Exists to make skew loud, not to
/// manage it.
pub fn enforce_remote_schema_policy(
    backend_name: &str,
    skew: &SchemaSkew,
    allow_newer_server: bool,
) -> Result<()> {
    if !skew.missing_on_server.is_empty() {
        bail!(
            "backend '{}': server schema is behind this moraine build (missing migrations: {}); \
             moraine never migrates non-default backends — apply these migrations on the server first",
            backend_name,
            skew.missing_on_server.join(", ")
        );
    }

    if !skew.unknown_on_server.is_empty() && !allow_newer_server {
        bail!(
            "backend '{}': server schema is ahead of this moraine build (unknown migrations: {}); \
             upgrade moraine, or set `allow_newer_server = true` on this backend to accept it",
            backend_name,
            skew.unknown_on_server.join(", ")
        );
    }

    Ok(())
}

pub fn is_oversized_json_each_row_insert_error(error: &anyhow::Error) -> bool {
    let message = error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_lowercase();

    let has_code_117 = message.contains("code: 117") || message.contains("code 117");
    let has_large_json_object = message.contains("size of json object")
        && message.contains("extremely large")
        && message.contains("expected not greater than");

    has_code_117 && has_large_json_object
}

fn truncate_for_error(statement: &str) -> String {
    const LIMIT: usize = 240;
    let compact = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= LIMIT {
        compact
    } else {
        let mut boundary = LIMIT;
        while !compact.is_char_boundary(boundary) {
            boundary -= 1;
        }
        format!("{}...", &compact[..boundary])
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        bail!("identifier must not be empty");
    }

    let ok = identifier
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !ok {
        bail!("identifier contains unsupported characters: {identifier}");
    }

    Ok(())
}

fn materialize_migration_sql(sql: &str, database: &str) -> Result<String> {
    validate_identifier(database)?;

    let mut text = sql.to_string();
    text = text.replace(
        "CREATE DATABASE IF NOT EXISTS moraine;",
        &format!("CREATE DATABASE IF NOT EXISTS {database};"),
    );
    text = text.replace("moraine.", &format!("{database}."));
    Ok(text)
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut prev = '\0';

    for line in sql.lines() {
        if line.trim_start().starts_with("--") {
            continue;
        }

        let chars: Vec<char> = line.chars().collect();
        let mut idx = 0;
        while idx < chars.len() {
            let ch = chars[idx];
            if ch == '\'' {
                if in_single_quote && idx + 1 < chars.len() && chars[idx + 1] == '\'' {
                    current.push(ch);
                    current.push(chars[idx + 1]);
                    prev = chars[idx + 1];
                    idx += 2;
                    continue;
                }
                if prev != '\\' {
                    in_single_quote = !in_single_quote;
                }
            }

            if ch == ';' && !in_single_quote {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
                prev = '\0';
                idx += 1;
                continue;
            }

            current.push(ch);
            prev = ch;
            idx += 1;
        }

        current.push('\n');
    }

    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }

    statements
}

fn escape_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn escape_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn has_explicit_json_each_row_format(query: &str) -> bool {
    let compact = query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    compact.contains(" format jsoneachrow")
}

/// Extract the `Code: NNN` server exception code ClickHouse embeds in error
/// text, including bodies that failed after 200-OK headers were flushed.
fn extract_clickhouse_exception_code(text: &str) -> Option<i32> {
    const MARKER: &str = "Code: ";
    let mut remaining = text;
    while let Some(idx) = remaining.find(MARKER) {
        let rest = &remaining[idx + MARKER.len()..];
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits > 0 {
            if let Ok(code) = rest[..digits].parse::<i32>() {
                return Some(code);
            }
        }
        remaining = rest;
    }
    None
}

/// Whether a 200-OK body carries a ClickHouse exception marker (the server
/// hit an error after flushing response headers). Requires both the `DB::`
/// exception prefix and a parsable code so ordinary row data mentioning
/// "Code:" does not classify as an exception.
fn body_contains_clickhouse_exception(body: &str) -> bool {
    body.contains("DB::") && extract_clickhouse_exception_code(body).is_some()
}

/// Amendment A8 (issue #600): `query_rows_with_params` may re-issue a query
/// as JSONEachRow only for genuine envelope drift — a completed 200-OK body
/// that failed JSON-envelope parsing and carries no ClickHouse exception
/// marker. Kill-truncated bodies and typed server errors are never silently
/// re-executed.
fn json_envelope_drift_retry_allowed(error: &anyhow::Error) -> bool {
    clickhouse_error_kind(error).is_none()
        && error
            .chain()
            .any(|cause| cause.downcast_ref::<JsonEnvelopeParseError>().is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Bytes,
        extract::{DefaultBodyLimit, Query, State},
        http::{HeaderMap, StatusCode},
        routing::{get, post},
        Router,
    };
    use flate2::read::GzDecoder;
    use moraine_config::ClickHouseConfig;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::Read;
    use std::sync::{Arc, Mutex};

    fn test_clickhouse_config(url: String) -> ClickHouseConfig {
        ClickHouseConfig {
            url,
            database: "moraine".to_string(),
            username: "default".to_string(),
            password: String::new(),
            timeout_seconds: 5.0,
            request_compression: ClickHouseRequestCompression::None,
            async_insert: true,
            wait_for_async_insert: true,
            allow_newer_server: false,
        }
    }

    async fn spawn_mock_server() -> String {
        async fn handler(
            Query(params): Query<HashMap<String, String>>,
            headers: HeaderMap,
        ) -> (StatusCode, String) {
            if headers.get("content-length").is_none() {
                return (
                    StatusCode::LENGTH_REQUIRED,
                    "missing content-length".to_string(),
                );
            }

            let query = params.get("query").cloned().unwrap_or_default();
            if query.contains("FAIL") {
                return (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_string());
            }

            if params
                .get("default_format")
                .is_some_and(|fmt| fmt == "JSON")
            {
                return (StatusCode::OK, "not-json".to_string());
            }

            (StatusCode::OK, "{\"value\":7}\n".to_string())
        }

        let app = Router::new().route("/", get(handler).post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    #[derive(Clone)]
    struct MigrationMockState {
        applied: Arc<Vec<String>>,
        queries: Arc<Mutex<Vec<String>>>,
        params: Arc<Mutex<Vec<HashMap<String, String>>>>,
        fail_ledger_insert: bool,
    }

    impl MigrationMockState {
        fn new(applied: Vec<String>, queries: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                applied: Arc::new(applied),
                queries,
                params: Arc::new(Mutex::new(Vec::new())),
                fail_ledger_insert: false,
            }
        }
    }

    async fn spawn_migration_mock_server(state: MigrationMockState) -> String {
        async fn handler(
            State(state): State<MigrationMockState>,
            Query(params): Query<HashMap<String, String>>,
        ) -> (StatusCode, String) {
            let query = params.get("query").cloned().unwrap_or_default();
            state
                .params
                .lock()
                .expect("migration params mutex poisoned")
                .push(params.clone());
            state
                .queries
                .lock()
                .expect("migration query mutex poisoned")
                .push(query.clone());

            if query.starts_with("SELECT version FROM") {
                let data = state
                    .applied
                    .iter()
                    .map(|version| json!({ "version": version }))
                    .collect::<Vec<_>>();
                return (StatusCode::OK, json!({ "data": data }).to_string());
            }
            if state.fail_ledger_insert
                && query.starts_with("INSERT INTO")
                && query.contains("schema_migrations")
            {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ledger insert failed".to_string(),
                );
            }
            (StatusCode::OK, String::new())
        }

        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind migration mock listener");
        let addr = listener.local_addr().expect("migration mock listener addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_insert_capture_server(lengths: Arc<Mutex<Vec<usize>>>) -> String {
        async fn handler(State(lengths): State<Arc<Mutex<Vec<usize>>>>, body: Bytes) -> StatusCode {
            lengths
                .lock()
                .expect("length capture mutex poisoned")
                .push(body.len());
            StatusCode::OK
        }

        let app = Router::new()
            .route("/", post(handler))
            .layer(DefaultBodyLimit::max(
                MAX_INSERT_PAYLOAD_BYTES.saturating_mul(2),
            ))
            .with_state(lengths);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind insert capture listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    #[derive(Clone)]
    struct RequestCaptureState {
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
    }

    struct CapturedRequest {
        params: HashMap<String, String>,
        headers: HeaderMap,
        body: Vec<u8>,
    }

    async fn spawn_request_capture_server(state: RequestCaptureState) -> String {
        async fn handler(
            State(state): State<RequestCaptureState>,
            Query(params): Query<HashMap<String, String>>,
            headers: HeaderMap,
            body: Bytes,
        ) -> (StatusCode, &'static str) {
            state
                .requests
                .lock()
                .expect("request capture mutex poisoned")
                .push(CapturedRequest {
                    params,
                    headers,
                    body: body.to_vec(),
                });
            (StatusCode::OK, "ok")
        }

        let app = Router::new()
            .route("/", post(handler))
            .layer(DefaultBodyLimit::max(
                MAX_INSERT_PAYLOAD_BYTES.saturating_mul(2),
            ))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind request capture listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    async fn spawn_user_agent_capture_server(
        user_agents: Arc<Mutex<Vec<Option<String>>>>,
    ) -> String {
        async fn handler(
            State(user_agents): State<Arc<Mutex<Vec<Option<String>>>>>,
            headers: HeaderMap,
        ) -> (StatusCode, &'static str) {
            user_agents
                .lock()
                .expect("user-agent capture mutex poisoned")
                .push(
                    headers
                        .get("user-agent")
                        .and_then(|value| value.to_str().ok())
                        .map(ToString::to_string),
                );
            (StatusCode::OK, "1")
        }

        let app = Router::new()
            .route("/", post(handler))
            .with_state(user_agents);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind user-agent capture listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    #[derive(Clone)]
    struct StreamCaptureState {
        params: Arc<Mutex<Vec<HashMap<String, String>>>>,
        content_lengths: Arc<Mutex<Vec<Option<String>>>>,
    }

    async fn spawn_stream_capture_server(state: StreamCaptureState) -> String {
        async fn handler(
            State(state): State<StreamCaptureState>,
            Query(params): Query<HashMap<String, String>>,
            headers: HeaderMap,
        ) -> (StatusCode, &'static str) {
            state
                .params
                .lock()
                .expect("stream params mutex poisoned")
                .push(params);
            state
                .content_lengths
                .lock()
                .expect("stream headers mutex poisoned")
                .push(
                    headers
                        .get("content-length")
                        .and_then(|value| value.to_str().ok())
                        .map(ToString::to_string),
                );

            (StatusCode::OK, "{\"value\":1}\n{\"value\":2}\n")
        }

        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stream capture listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    /// Serves the ClickHouse failure shapes the typed error layer must
    /// classify: non-200 exceptions (body and header coded), 200-OK bodies
    /// truncated by a mid-stream kill, envelope `exception` fields, and
    /// genuine FORMAT JSON envelope drift. Records every received query.
    async fn spawn_typed_error_server(queries: Arc<Mutex<Vec<String>>>) -> String {
        use axum::response::IntoResponse;

        async fn handler(
            State(queries): State<Arc<Mutex<Vec<String>>>>,
            Query(params): Query<HashMap<String, String>>,
        ) -> axum::response::Response {
            let query = params.get("query").cloned().unwrap_or_default();
            queries
                .lock()
                .expect("typed error queries mutex poisoned")
                .push(query.clone());

            if query.contains("MEMORY") {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Code: 241. DB::Exception: Memory limit (for query) exceeded: \
                     would use 1.10 GiB, maximum: 1.00 GiB. (MEMORY_LIMIT_EXCEEDED)"
                        .to_string(),
                )
                    .into_response();
            }
            if query.contains("HEADER_CODE") {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [("x-clickhouse-exception-code", "159")],
                    "timeout without inline code text".to_string(),
                )
                    .into_response();
            }
            if query.contains("KILLED_ENVELOPE") {
                // Headers already flushed when the KILL landed: truncated
                // JSON envelope with the exception text appended.
                return (
                    StatusCode::OK,
                    "{\"meta\":[{\"name\":\"value\"}],\"data\":[{\"value\":1}\
                     Code: 394. DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED)"
                        .to_string(),
                )
                    .into_response();
            }
            if query.contains("EXCEPTION_FIELD") {
                return (
                    StatusCode::OK,
                    "{\"data\":[],\"exception\":\"Code: 159. DB::Exception: \
                     Timeout exceeded: elapsed 15.1 seconds. (TIMEOUT_EXCEEDED)\"}"
                        .to_string(),
                )
                    .into_response();
            }
            if query.contains("KILLED_ROWS") {
                return (
                    StatusCode::OK,
                    "{\"value\":1}\nCode: 394. DB::Exception: Query was cancelled. \
                     (QUERY_WAS_CANCELLED)\n"
                        .to_string(),
                )
                    .into_response();
            }
            // Default: envelope drift — rows come back as JSONEachRow even
            // when the request asked for the FORMAT JSON envelope.
            (StatusCode::OK, "{\"value\":7}\n".to_string()).into_response()
        }

        let app = Router::new().route("/", post(handler)).with_state(queries);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind typed error listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    #[derive(Clone)]
    struct SkewMockState {
        ledger_exists: bool,
        versions: Vec<String>,
        queries: Arc<Mutex<Vec<String>>>,
    }

    async fn spawn_skew_mock_server(state: SkewMockState) -> String {
        async fn handler(
            State(state): State<SkewMockState>,
            Query(params): Query<HashMap<String, String>>,
        ) -> (StatusCode, String) {
            let query = params.get("query").cloned().unwrap_or_default();
            state
                .queries
                .lock()
                .expect("query capture mutex poisoned")
                .push(query.clone());

            if query.contains("system.tables") {
                let exists = u8::from(state.ledger_exists);
                return (
                    StatusCode::OK,
                    format!("{{\"data\":[{{\"exists\":{exists}}}]}}"),
                );
            }

            if query.contains("schema_migrations") {
                let rows: Vec<Value> = state
                    .versions
                    .iter()
                    .map(|v| json!({ "version": v }))
                    .collect();
                let body =
                    serde_json::to_string(&json!({ "data": rows })).expect("encode mock rows");
                return (StatusCode::OK, body);
            }

            (StatusCode::OK, "{\"data\":[]}".to_string())
        }

        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind skew mock listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    fn spawn_truncated_body_server() -> String {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind raw listener");
        let addr = listener.local_addr().expect("raw listener addr");

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request);

                let response = concat!(
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: text/plain; charset=utf-8\r\n",
                    "Content-Length: 20\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                    "short",
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        format!("http://{}", addr)
    }

    #[test]
    fn sql_split_handles_multiple_statements() {
        let sql = "CREATE TABLE a (x String);\nINSERT INTO a VALUES ('a;b');\n";
        let out = split_sql_statements(sql);
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("CREATE TABLE"));
        assert!(out[1].contains("'a;b'"));
    }

    #[test]
    fn sql_split_handles_sql_standard_escaped_quotes() {
        let sql = "INSERT INTO a VALUES ('it''s;fine');\nSELECT 1;\n";
        let out = split_sql_statements(sql);
        assert_eq!(out.len(), 2);
        assert!(out[0].contains("'it''s;fine'"));
    }

    #[test]
    fn sql_split_handles_escaped_quote_after_backslash() {
        let sql = "INSERT INTO a VALUES ('path\\'';still-string');\nSELECT 1;\n";
        let out = split_sql_statements(sql);
        assert_eq!(
            out,
            vec![
                "INSERT INTO a VALUES ('path\\'';still-string')".to_string(),
                "SELECT 1".to_string()
            ]
        );
    }

    #[test]
    fn sql_materialization_rewrites_database() {
        let sql = "CREATE DATABASE IF NOT EXISTS moraine;\nCREATE TABLE moraine.events (x UInt8);";
        let out = materialize_migration_sql(sql, "custom_db").expect("should rewrite");
        assert!(out.contains("CREATE DATABASE IF NOT EXISTS custom_db;"));
        assert!(out.contains("custom_db.events"));
    }

    #[test]
    fn identifier_validation_rejects_invalid() {
        assert!(validate_identifier("moraine_01").is_ok());
        assert!(validate_identifier("moraine-db").is_err());
    }

    #[test]
    fn format_detection_handles_case_and_whitespace() {
        assert!(has_explicit_json_each_row_format(
            "SELECT 1\nFORMAT JSONEachRow"
        ));
        assert!(has_explicit_json_each_row_format(
            "SELECT 1 format jsoneachrow"
        ));
        assert!(!has_explicit_json_each_row_format("SELECT 1"));
        assert!(!has_explicit_json_each_row_format("SELECT 1 FORMAT JSON"));
    }

    #[test]
    fn classifier_matches_clickhouse_oversized_json_each_row_error() {
        let error = anyhow!(
            "clickhouse returned 400 Bad Request: Code: 117. DB::Exception: \
             Size of JSON object at position 104890103 is extremely large. \
             Expected not greater than 10485760 bytes, but current is 104890103 bytes per row. \
             While executing ParallelParsingBlockInputFormat."
        );

        assert!(is_oversized_json_each_row_insert_error(&error));
    }

    #[test]
    fn classifier_rejects_other_code_117_errors() {
        let error = anyhow!(
            "clickhouse returned 400 Bad Request: Code: 117. DB::Exception: \
             Unknown field found while parsing JSONEachRow: unexpected_column"
        );

        assert!(!is_oversized_json_each_row_insert_error(&error));
    }

    #[test]
    fn classifier_requires_clickhouse_code_117() {
        let error = anyhow!(
            "clickhouse returned 400 Bad Request: Size of JSON object at position 42 \
             is extremely large. Expected not greater than 10485760 bytes."
        );

        assert!(!is_oversized_json_each_row_insert_error(&error));
    }

    fn is_migration_filename(name: &str) -> bool {
        // Matches ^\d{3}_.+\.sql$
        let Some(stem) = name.strip_suffix(".sql") else {
            return false;
        };
        if stem.len() < 5 {
            return false;
        }
        let (prefix, rest) = stem.split_at(3);
        prefix.chars().all(|c| c.is_ascii_digit()) && rest.starts_with('_') && rest.len() > 1
    }

    #[test]
    fn bundled_migrations_matches_sql_directory() {
        use std::path::PathBuf;

        let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("sql");

        let mut discovered: Vec<String> = std::fs::read_dir(&sql_dir)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", sql_dir.display()))
            .filter_map(|entry| {
                let entry = entry.ok()?;
                if !entry.file_type().ok()?.is_file() {
                    return None;
                }
                let name = entry.file_name().to_str()?.to_string();
                is_migration_filename(&name).then_some(name)
            })
            .collect();
        discovered.sort();

        assert!(
            !discovered.is_empty(),
            "no migration files found under {}",
            sql_dir.display()
        );

        let migrations = bundled_migrations();
        let bundled_names: Vec<String> = migrations.iter().map(|m| m.name.to_string()).collect();

        assert_eq!(
            bundled_names, discovered,
            "bundled_migrations() is out of sync with sql/*.sql — \
             new migration files must be registered with a matching include_str! entry"
        );

        // bundled_migrations() must be sorted ascending by version.
        let versions: Vec<&str> = migrations.iter().map(|m| m.version).collect();
        let mut sorted = versions.clone();
        sorted.sort();
        assert_eq!(
            versions, sorted,
            "bundled_migrations() must be ordered ascending by version"
        );

        // Each entry's version must match its filename's numeric prefix.
        for m in &migrations {
            assert!(
                m.name.starts_with(&format!("{}_", m.version)),
                "migration name {} does not begin with {}_ prefix",
                m.name,
                m.version
            );
            assert!(
                !m.sql.is_empty(),
                "migration {} has empty bundled sql — include_str! target may be missing",
                m.name
            );
        }
    }

    #[test]
    fn migration_020_purges_every_session_keyed_table() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|m| m.version == "020")
            .expect("migration 020 must be registered");

        // Materialize against a non-default database to also prove the
        // `moraine.` prefix is rewritten everywhere (no bare table names leak).
        let materialized =
            materialize_migration_sql(migration.sql, "other_db").expect("materialize 020");
        let statements = split_sql_statements(&materialized);

        // Every table that can hold empty-session_id claude-code junk must be
        // purged; a dropped table here would leave lingering junk (#386).
        let harness_scoped = [
            "events",
            "raw_events",
            "event_links",
            "tool_io",
            "search_documents",
            "search_postings",
            "search_hit_log",
        ];
        // No harness column on this aggregate — scoped on session_id alone.
        let session_only = ["search_conversation_terms"];

        assert_eq!(
            statements.len(),
            harness_scoped.len() + session_only.len(),
            "unexpected statement count in 020: {statements:#?}"
        );

        for table in harness_scoped {
            let expected =
                format!("ALTER TABLE other_db.{table} DELETE WHERE session_id = '' AND harness = 'claude-code'");
            assert!(
                statements.iter().any(|s| s.contains(&expected)),
                "020 missing harness-scoped purge for `{table}`"
            );
        }
        for table in session_only {
            let expected = format!("ALTER TABLE other_db.{table} DELETE WHERE session_id = ''");
            assert!(
                statements
                    .iter()
                    .any(|s| s.contains(&expected) && !s.contains("harness")),
                "020 missing session-only purge for `{table}`"
            );
        }

        // Every statement must complete synchronously so the migration is only
        // recorded once the junk is actually gone.
        for statement in &statements {
            assert!(
                statement.contains("mutations_sync = 1"),
                "020 statement must run with mutations_sync = 1: {statement}"
            );
            assert!(
                !statement.contains("moraine."),
                "020 statement must not reference a bare `moraine.` after rewrite: {statement}"
            );
        }
    }

    #[test]
    fn mcp_open_migrations_exclude_blank_session_ids() {
        for version in ["027", "029", "030", "033"] {
            let migration = bundled_migrations()
                .into_iter()
                .find(|migration| migration.version == version)
                .unwrap_or_else(|| panic!("migration {version} must be registered"));

            assert!(
                migration.sql.contains("WHERE notEmpty(session_id)"),
                "migration {version} must not enqueue blank session IDs"
            );
        }
    }

    #[test]
    fn migration_030_refreshes_only_omp_session_heads() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "030")
            .expect("migration 030 must be registered");

        assert!(migration.sql.contains("FROM moraine.events FINAL"));
        assert!(migration.sql.contains("source_name = 'omp'"));
    }

    #[test]
    fn migration_031_preserves_publication_history_and_causal_checkpoint_tuples() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "031")
            .expect("migration 031 must be registered");
        let sql = migration.sql;

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS moraine.published_source_generations"));
        assert!(sql.contains("ReplacingMergeTree(publication_revision)"));
        assert!(sql.contains("ORDER BY (source_host, source_name, source_file, source_generation)"));
        assert!(sql.contains("tuple(publication_revision, publisher_id, operation_id)"));
        assert!(sql.contains("tuple(control_revision, publisher_id, batch_id, state)"));
        assert!(
            sql.contains("CREATE VIEW IF NOT EXISTS moraine.v_published_source_generation_history")
        );
        assert!(sql.contains("FROM moraine.v_published_source_generation_history\n)"));
        assert!(sql.contains(") NOT IN\n("));
        assert!(sql.contains("base_revision + toUInt64(row_number()"));
        assert!(sql.contains("cursor_json String DEFAULT ''"));
        assert!(sql.contains("source_fingerprint UInt64 DEFAULT 0"));
        assert!(sql.contains("schema_fingerprint UInt64 DEFAULT 0"));
        assert!(sql.contains("argMax(\n      tuple("));
        assert!(!sql.contains("DELETE WHERE"));
        assert!(!sql.contains("TRUNCATE TABLE"));
    }

    #[test]
    fn migration_032_authorizes_search_before_statistics() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "032")
            .expect("migration 032 must be registered");
        let sql = migration.sql;

        for table in [
            "raw_events",
            "events",
            "event_links",
            "tool_io",
            "ingest_errors",
            "search_documents",
            "search_postings",
        ] {
            assert!(
                sql.contains(&format!("ALTER TABLE moraine.{table}")),
                "032 must migrate {table}"
            );
        }
        assert!(sql.contains("CREATE VIEW moraine.v_live_events"));
        assert!(sql.contains("CREATE VIEW moraine.v_live_search_documents"));
        for table in ["event_links", "tool_io"] {
            let column = format!(
                "ALTER TABLE moraine.{table}\n  ADD COLUMN IF NOT EXISTS source_host String AFTER ingested_at,\n  ADD COLUMN IF NOT EXISTS source_event_version UInt64 DEFAULT 0 AFTER event_version"
            );
            assert!(
                sql.contains(&column),
                "032 must add a fail-closed causal event version to {table}"
            );
        }
        let link_backfill = sql
            .split_once("INSERT INTO moraine.event_links")
            .and_then(|(_, tail)| {
                tail.split_once("INSERT INTO moraine.tool_io")
                    .map(|(backfill, _)| backfill)
            })
            .expect("032 must backfill causal link versions before tool versions");
        let tool_backfill = sql
            .split_once("INSERT INTO moraine.tool_io")
            .and_then(|(_, tail)| {
                tail.split_once("-- The sole current-generation authorization relation")
                    .map(|(backfill, _)| backfill)
            })
            .expect("032 must backfill causal tool versions before live views");
        for (relation, backfill) in [("event_links", link_backfill), ("tool_io", tool_backfill)] {
            assert!(backfill.contains("source_event_version = 0"));
            assert!(backfill.contains("event_version + toUInt64(1) AS event_version"));
            assert!(backfill.contains("e.event_version AS source_event_version"));
            assert!(backfill.contains("ASOF INNER JOIN"));
            assert!(backfill.contains("event_version >= e.event_version"));
            assert!(
                backfill.contains("WHERE event_version > 0"),
                "032 must not bind reserved zero-version owners for {relation}"
            );
            assert!(
                backfill.contains("FROM moraine.events FINAL"),
                "032 must bind legacy {relation} rows to canonical FINAL events"
            );
        }
        let live_links_view = sql
            .split_once("CREATE VIEW moraine.v_live_event_links AS")
            .and_then(|(_, tail)| {
                tail.split_once("CREATE VIEW moraine.v_live_tool_io AS")
                    .map(|(view, _)| view)
            })
            .expect("032 must define live event links");
        let live_tools_view = sql
            .split_once("CREATE VIEW moraine.v_live_tool_io AS")
            .and_then(|(_, tail)| {
                tail.split_once("CREATE MATERIALIZED VIEW moraine.mv_search_documents_from_events")
                    .map(|(view, _)| view)
            })
            .expect("032 must define live tool IO");
        assert!(live_links_view.contains("l.source_event_version = e.event_version"));
        assert!(live_tools_view.contains("t.source_event_version = e.event_version"));
        assert!(live_links_view.contains("l.source_event_version != 0"));
        assert!(live_tools_view.contains("t.source_event_version != 0"));
        assert!(
            !sql.contains("ANY INNER JOIN"),
            "032 authorization joins must preserve every matching left-side row"
        );
        let live_documents_projection = sql
            .split_once("CREATE VIEW moraine.v_live_search_documents AS")
            .and_then(|(_, tail)| tail.split_once(") AS d\nALL INNER JOIN"))
            .map(|(projection, _)| projection)
            .expect("032 must define the live search-document projection");
        let projected_columns = live_documents_projection
            .lines()
            .map(str::trim)
            .map(|line| line.trim_end_matches(','))
            .collect::<HashSet<_>>();
        for column in [
            "doc_version",
            "ingested_at",
            "event_uid",
            "compacted_parent_uid",
            "session_id",
            "session_date",
            "source_host",
            "source_name",
            "harness",
            "inference_provider",
            "endpoint_kind",
            "source_file",
            "source_generation",
            "source_line_no",
            "source_offset",
            "source_ref",
            "record_ts",
            "event_class",
            "payload_type",
            "actor_role",
            "name",
            "phase",
            "text_content",
            "payload_json",
            "token_usage_json",
            "token_usage_buckets",
            "token_usage_native_units",
            "doc_len",
            "has_codex_mcp",
        ] {
            assert!(
                projected_columns.contains(column),
                "032 live search documents omitted `{column}`: {live_documents_projection}"
            );
        }
        assert!(
            !live_documents_projection.lines().any(|line| {
                let line = line.trim();
                line == "SELECT *" || line.starts_with("SELECT *,")
            }),
            "032 must not silently omit MATERIALIZED search columns"
        );
        let document_mv = sql
            .split_once("CREATE MATERIALIZED VIEW moraine.mv_search_documents_from_events")
            .and_then(|(_, tail)| {
                tail.split_once("CREATE MATERIALIZED VIEW moraine.mv_search_postings")
                    .map(|(mv, _)| mv)
            })
            .expect("032 must materialize search documents before backfilling tombstones");
        assert!(document_mv.contains("FROM moraine.events"));
        assert!(
            !document_mv.contains("WHERE"),
            "032 must materialize one document version for every future event revision"
        );
        let stranded_posting_repair = sql
            .split_once("-- An interrupted older copy of this migration")
            .and_then(|(_, tail)| {
                tail.split_once("-- Reconcile every current event version")
                    .map(|(repair, _)| repair)
            })
            .expect("032 must repair documents stranded by an interrupted older attempt");
        assert!(stranded_posting_repair.contains("INSERT INTO moraine.search_postings"));
        assert!(stranded_posting_repair.contains("FROM moraine.search_documents FINAL"));
        assert!(stranded_posting_repair.contains("FROM moraine.search_postings"));
        assert!(stranded_posting_repair.contains("LEFT ANTI JOIN"));
        assert!(stranded_posting_repair.contains("WHERE missing.doc_len > 0"));
        let anti_join = stranded_posting_repair
            .find("LEFT ANTI JOIN")
            .expect("032 stranded-posting repair must anti-join covered documents");
        let tokenization = stranded_posting_repair
            .find("arrayJoin(extractAll")
            .expect("032 stranded-posting repair must tokenize missing documents");
        assert!(
            anti_join < tokenization,
            "032 must eliminate covered documents before regex tokenization"
        );
        assert!(!stranded_posting_repair.contains("FROM moraine.search_postings FINAL"));
        assert!(stranded_posting_repair.contains("join_algorithm = 'grace_hash'"));
        assert!(stranded_posting_repair.contains("max_bytes_in_join = 268435456"));
        for identity in [
            "missing.source_host = p.source_host",
            "missing.source_name = p.source_name",
            "missing.session_id = p.session_id",
            "missing.source_ref = p.source_ref",
            "missing.event_uid = p.doc_id",
            "missing.doc_version = p.post_version",
        ] {
            assert!(
                stranded_posting_repair.contains(identity),
                "032 stranded-posting repair must match `{identity}`"
            );
        }
        assert!(!stranded_posting_repair.contains("missing.source_file = p.source_file"));
        assert!(
            !stranded_posting_repair.contains("missing.source_generation = p.source_generation")
        );

        let tombstone_backfill = sql
            .split_once("-- Reconcile every current event version")
            .and_then(|(_, tail)| {
                tail.split_once("-- Existing posting rows already carry")
                    .map(|(backfill, _)| backfill)
            })
            .expect("032 must reconcile missing latest-event document versions");
        assert!(tombstone_backfill.contains("FROM moraine.events FINAL"));
        assert!(tombstone_backfill.contains("e.event_version AS doc_version"));
        assert!(tombstone_backfill.contains("source_host"));
        assert!(tombstone_backfill.contains("LEFT ANTI JOIN"));
        assert!(tombstone_backfill.contains("FROM moraine.search_documents"));
        for identity in [
            "e.source_host = d.source_host",
            "e.event_uid = d.event_uid",
            "e.event_version = d.doc_version",
        ] {
            assert!(
                tombstone_backfill.contains(identity),
                "032 document reconciliation must match `{identity}`"
            );
        }
        assert_eq!(
            sql.matches("ALL INNER JOIN").count(),
            6,
            "032 authorization joins must preserve every matching event, dependency, document, posting, and dirty session"
        );
        assert!(!sql.contains("ANY INNER JOIN"));
        assert!(!sql.contains("\nINNER JOIN"));
        let dirty_sessions_mv = sql
            .split_once("CREATE MATERIALIZED VIEW moraine.mv_mcp_open_dirty_sessions_from_events")
            .map(|(_, tail)| tail)
            .expect("032 must replace the MCP dirty-session materialized view");
        assert!(dirty_sessions_mv.contains("FROM moraine.events AS e"));
        assert!(dirty_sessions_mv
            .contains("ALL INNER JOIN moraine.v_current_published_source_generations AS h"));
        for identity in [
            "e.source_host = h.source_host",
            "e.source_name = h.source_name",
            "e.source_file = h.source_file",
            "e.source_generation = h.source_generation",
        ] {
            assert!(
                dirty_sessions_mv.contains(identity),
                "032 must not dirty MCP sessions for an unpublished replay: missing `{identity}`"
            );
        }
        assert!(dirty_sessions_mv.contains("GROUP BY e.session_id"));
        let live_documents_view = sql
            .split_once("CREATE VIEW moraine.v_live_search_documents AS")
            .and_then(|(_, tail)| {
                tail.split_once("CREATE VIEW moraine.v_live_search_postings AS")
                    .map(|(view, _)| view)
            })
            .expect("032 must define a bounded live search-document view");
        assert!(live_documents_view.contains("FROM moraine.search_documents FINAL"));
        assert!(live_documents_view
            .contains("ALL INNER JOIN moraine.v_current_published_source_generations AS h"));
        for identity in [
            "d.source_host = h.source_host",
            "d.source_name = h.source_name",
            "d.source_file = h.source_file",
            "d.source_generation = h.source_generation",
        ] {
            assert!(
                live_documents_view.contains(identity),
                "032 live search documents must authorize exact event identity `{identity}`"
            );
        }
        assert!(live_documents_view.contains("WHERE d.doc_len > 0"));
        assert!(sql.contains("CREATE VIEW moraine.v_live_search_postings"));
        let postings_mv = sql
            .find("CREATE MATERIALIZED VIEW moraine.mv_search_postings")
            .expect("032 must recreate the postings materialized view");
        let conversation_terms_mv = sql
            .find("CREATE MATERIALIZED VIEW moraine.mv_search_conversation_terms")
            .expect("032 must recreate the conversation-terms materialized view");
        let document_reconciliation = sql
            .find("INSERT INTO moraine.search_documents")
            .expect("032 must reconcile missing search documents");
        assert!(
            postings_mv < conversation_terms_mv && conversation_terms_mv < document_reconciliation,
            "032 must install the full search MV chain before repairing or reconciling documents"
        );
        let live_postings_view = sql
            .split_once("CREATE VIEW moraine.v_live_search_postings AS")
            .and_then(|(_, tail)| {
                tail.split_once("CREATE VIEW moraine.search_term_stats AS")
                    .map(|(view, _)| view)
            })
            .expect("032 must define a bounded live search-postings view");
        assert!(live_postings_view.contains("FROM moraine.v_live_search_documents"));
        for identity in [
            "p.source_host = d.source_host",
            "p.source_name = d.source_name",
            "p.session_id = d.session_id",
            "p.source_ref = d.source_ref",
            "p.doc_id = d.event_uid",
            "p.post_version = d.doc_version",
        ] {
            assert!(
                live_postings_view.contains(identity),
                "032 live search postings must authorize `{identity}`"
            );
        }
        assert!(!live_postings_view.contains("p.source_file = d.source_file"));
        assert!(!live_postings_view.contains("p.source_generation = d.source_generation"));
        for canonical_projection in [
            "d.source_host AS source_host",
            "d.source_name AS source_name",
            "d.source_file AS source_file",
            "d.source_generation AS source_generation",
        ] {
            assert!(
                live_postings_view.contains(canonical_projection),
                "032 must project canonical posting identity `{canonical_projection}`"
            );
        }
        assert!(sql.contains("FROM moraine.v_live_search_postings\nGROUP BY term"));
        assert!(sql.contains("FROM moraine.v_live_search_documents;"));
        assert_eq!(
            sql.matches("INSERT INTO moraine.search_postings").count(),
            1,
            "032 may repair only zero-posting documents, not rebuild the historical corpus"
        );
    }

    #[test]
    fn migration_033_keeps_candidate_headers_and_children_independent() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "033")
            .expect("migration 033 must be registered");
        let sql = migration.sql;

        assert!(sql.contains("ADD COLUMN IF NOT EXISTS source_host String AFTER event_uid"));
        assert!(sql.contains("ADD COLUMN IF NOT EXISTS candidate_generation UInt64"));
        assert!(
            sql.contains("MODIFY ORDER BY (event_uid, slot, source_host, candidate_generation)")
        );
        assert!(sql.contains("MODIFY ORDER BY (session_id, slot, turn_seq, candidate_generation)"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS moraine.mcp_open_publication_headers"));
        assert!(sql.contains("ORDER BY (session_id, candidate_publication_id)"));
        assert!(sql.contains("required_source_heads Array(Tuple("));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS moraine.mcp_open_generation_readiness"));
        assert!(sql.contains("VALUES ('global', 0, generateSnowflakeID(), '')"));
        assert!(sql.contains("blocked_append_preparations"));
    }

    #[test]
    fn migration_033_diagnostics_only_count_current_checkpoint_generations() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "033")
            .expect("migration 033 must be registered");
        let diagnostics = migration
            .sql
            .split_once("CREATE VIEW moraine.v_publication_diagnostics AS")
            .map(|(_, diagnostics)| diagnostics)
            .expect("033 must install publication diagnostics");

        assert!(diagnostics
            .contains("FROM moraine.v_current_ingest_checkpoint_transitions AS checkpoint"));
        assert!(diagnostics.contains(
            "LEFT JOIN moraine.v_current_source_generation_publication_readiness AS readiness"
        ));
        for identity in [
            "readiness.source_host = checkpoint.host",
            "readiness.source_name = checkpoint.source_name",
            "readiness.source_file = checkpoint.source_file",
            "readiness.source_generation = checkpoint.source_generation",
        ] {
            assert!(
                diagnostics.contains(identity),
                "033 diagnostics must authorize readiness with `{identity}`"
            );
        }
        assert!(!diagnostics.contains("FROM moraine.ingest_checkpoint_transitions FINAL"));
        assert!(!diagnostics.contains("FROM moraine.source_generation_publication_readiness FINAL"));
    }

    #[test]
    fn migration_034_resets_only_the_incomplete_derived_mcp_model() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "034")
            .expect("migration 034 must be registered");
        let sql = migration.sql;

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS moraine.mcp_open_backfill_plans"));
        assert!(sql.contains("candidate_generation UInt64"));
        assert!(sql.contains("phase UInt8"));
        for derived in [
            "mcp_open_events",
            "mcp_open_turns",
            "mcp_open_sessions",
            "mcp_open_publication_headers",
            "mcp_open_generation_readiness",
            "mcp_open_backfill_plans",
        ] {
            assert!(
                sql.contains(&format!("TRUNCATE TABLE moraine.{derived};")),
                "034 must reset derived relation {derived}"
            );
        }
        for canonical in [
            "events",
            "raw_events",
            "search_documents",
            "search_postings",
        ] {
            assert!(
                !sql.contains(&format!("TRUNCATE TABLE moraine.{canonical};")),
                "034 must preserve canonical relation {canonical}"
            );
        }
        assert!(sql.contains("VALUES ('global', 0, generateSnowflakeID(), '')"));
        assert!(
            sql.find("VALUES ('global', 0, generateSnowflakeID(), '')")
                < sql.find("TRUNCATE TABLE moraine.mcp_open_events;"),
            "034 must fence readers before discarding derived rows"
        );
    }

    #[test]
    fn migration_035_adds_list_metadata_and_rebuilds_only_the_derived_mcp_model() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "035")
            .expect("migration 035 must be registered");
        let sql = migration.sql;

        assert!(sql.contains("ADD COLUMN IF NOT EXISTS list_title String"));
        assert!(sql.contains("ADD COLUMN IF NOT EXISTS list_session_summary String"));
        for derived in [
            "mcp_open_events",
            "mcp_open_turns",
            "mcp_open_sessions",
            "mcp_open_publication_headers",
            "mcp_open_generation_readiness",
            "mcp_open_backfill_plans",
        ] {
            assert!(
                sql.contains(&format!("TRUNCATE TABLE moraine.{derived};")),
                "035 must reset derived relation {derived}"
            );
        }
        for canonical in [
            "events",
            "raw_events",
            "search_documents",
            "search_postings",
        ] {
            assert!(
                !sql.contains(&format!("TRUNCATE TABLE moraine.{canonical};")),
                "035 must preserve canonical relation {canonical}"
            );
        }
        assert!(
            sql.find("VALUES ('global', 0, generateSnowflakeID(), '')")
                < sql.find("TRUNCATE TABLE moraine.mcp_open_events;"),
            "035 must fence readers before discarding derived rows"
        );
    }

    #[test]
    fn migration_021_adds_file_attention_columns_without_reordering_tables() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|m| m.version == "021")
            .expect("migration 021 must be registered");

        let materialized =
            materialize_migration_sql(migration.sql, "other_db").expect("materialize 021");
        let statements = split_sql_statements(&materialized);

        assert_eq!(
            statements.len(),
            6,
            "021 should only add three columns to events and tool_io"
        );

        for table in ["events", "tool_io"] {
            for column in ["project_id", "repo_rel_path", "worktree_root"] {
                let expected =
                    format!("ALTER TABLE other_db.{table}\n  ADD COLUMN IF NOT EXISTS {column}");
                assert!(
                    statements
                        .iter()
                        .any(|statement| statement.contains(&expected)),
                    "021 missing {column} on {table}: {statements:#?}"
                );
            }
        }

        assert!(
            statements
                .iter()
                .all(|statement| !statement.contains("ORDER BY")),
            "021 must not rewrite ReplacingMergeTree sort keys"
        );
        assert!(
            statements
                .iter()
                .all(|statement| !statement.contains("Nullable")),
            "021 should use non-null defaults for lookup fields"
        );
    }

    #[test]
    fn migration_filename_matcher_rejects_non_conforming_names() {
        assert!(is_migration_filename("001_schema.sql"));
        assert!(is_migration_filename("012_add_inference_provider.sql"));
        assert!(!is_migration_filename("001_schema.txt"));
        assert!(!is_migration_filename("schema.sql"));
        assert!(!is_migration_filename("01_schema.sql"));
        assert!(!is_migration_filename("0001_schema.sql"));
        assert!(!is_migration_filename("001schema.sql"));
        assert!(!is_migration_filename("001_.sql"));
        assert!(!is_migration_filename("README.md"));
    }

    #[test]
    fn schema_skew_clean_when_versions_match() {
        let skew = compute_schema_skew(&["001", "002"], &["002".to_string(), "001".to_string()]);
        assert!(skew.is_clean());
        assert_eq!(skew, SchemaSkew::default());
    }

    #[test]
    fn schema_skew_reports_server_behind() {
        let skew = compute_schema_skew(&["001", "002", "003"], &["001".to_string()]);
        assert_eq!(skew.missing_on_server, vec!["002", "003"]);
        assert!(skew.unknown_on_server.is_empty());
        assert!(!skew.is_clean());
    }

    #[test]
    fn schema_skew_reports_server_ahead() {
        let skew = compute_schema_skew(&["001"], &["001".to_string(), "017".to_string()]);
        assert!(skew.missing_on_server.is_empty());
        assert_eq!(skew.unknown_on_server, vec!["017"]);
    }

    #[test]
    fn schema_skew_reports_divergence_in_both_directions() {
        let skew = compute_schema_skew(
            &["001", "002"],
            &["001".to_string(), "099".to_string(), "099".to_string()],
        );
        assert_eq!(skew.missing_on_server, vec!["002"]);
        // Output is deduplicated and sorted.
        assert_eq!(skew.unknown_on_server, vec!["099"]);
    }

    #[test]
    fn schema_skew_with_empty_server_ledger_reports_everything_missing() {
        let bundled: Vec<&str> = bundled_migrations().iter().map(|m| m.version).collect();
        let skew = compute_schema_skew(&bundled, &Vec::<String>::new());
        assert_eq!(skew.missing_on_server.len(), bundled.len());
        assert!(skew.unknown_on_server.is_empty());
    }

    #[test]
    fn remote_schema_policy_accepts_clean_skew() {
        let skew = SchemaSkew::default();
        assert!(enforce_remote_schema_policy("team-ch", &skew, false).is_ok());
    }

    #[test]
    fn remote_schema_policy_rejects_server_behind() {
        let skew = SchemaSkew {
            missing_on_server: vec!["015".to_string(), "016".to_string()],
            unknown_on_server: Vec::new(),
        };
        let err = enforce_remote_schema_policy("team-ch", &skew, false)
            .expect_err("server behind must be a hard error");
        let msg = err.to_string();
        assert!(msg.contains("'team-ch'"));
        assert!(msg.contains("015, 016"));
        assert!(msg.contains("never migrates"));
    }

    #[test]
    fn remote_schema_policy_rejects_server_behind_even_with_allow_newer() {
        let skew = SchemaSkew {
            missing_on_server: vec!["016".to_string()],
            unknown_on_server: vec!["017".to_string()],
        };
        let err = enforce_remote_schema_policy("team-ch", &skew, true)
            .expect_err("allow_newer_server must not excuse a server that is behind");
        assert!(err.to_string().contains("016"));
    }

    #[test]
    fn remote_schema_policy_rejects_server_ahead_by_default() {
        let skew = SchemaSkew {
            missing_on_server: Vec::new(),
            unknown_on_server: vec!["017".to_string()],
        };
        let err = enforce_remote_schema_policy("team-ch", &skew, false)
            .expect_err("server ahead must be a hard error without opt-in");
        let msg = err.to_string();
        assert!(msg.contains("'team-ch'"));
        assert!(msg.contains("017"));
        assert!(msg.contains("allow_newer_server"));
    }

    #[test]
    fn remote_schema_policy_allows_server_ahead_when_opted_in() {
        let skew = SchemaSkew {
            missing_on_server: Vec::new(),
            unknown_on_server: vec!["017".to_string()],
        };
        assert!(enforce_remote_schema_policy("team-ch", &skew, true).is_ok());
    }

    #[test]
    fn truncate_for_error_handles_multibyte_utf8_boundaries() {
        let statement = format!("{}é{}", "a".repeat(239), "b".repeat(10));
        let truncated = truncate_for_error(&statement);
        assert_eq!(truncated, format!("{}...", "a".repeat(239)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supplied_user_agent_is_reused_for_every_request() {
        let user_agents = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_user_agent_capture_server(user_agents.clone()).await;
        let identity = "moraine-backend/0.6.4 (pid=4242)";
        let client =
            ClickHouseClient::new_with_user_agent(test_clickhouse_config(base_url), identity)
                .expect("new attributed client");

        client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect("first attributed request");
        client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect("second attributed request");

        assert_eq!(
            user_agents
                .lock()
                .expect("user-agent capture mutex poisoned")
                .as_slice(),
            &[Some(identity.to_string()), Some(identity.to_string())]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compatibility_constructor_sends_default_process_identity() {
        let user_agents = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_user_agent_capture_server(user_agents.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url))
            .expect("compatibility constructor");

        client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect("request from compatibility client");

        let expected = format!(
            "{DEFAULT_USER_AGENT_ROLE}/{} (pid={})",
            env!("CARGO_PKG_VERSION"),
            std::process::id()
        );
        assert_eq!(
            user_agents
                .lock()
                .expect("user-agent capture mutex poisoned")
                .as_slice(),
            &[Some(expected)]
        );
    }

    #[test]
    fn invalid_user_agent_is_rejected_during_construction() {
        let result = ClickHouseClient::new_with_user_agent(
            test_clickhouse_config("http://127.0.0.1:8123".to_string()),
            "moraine-backend/0.6.4\ninjected: true",
        );

        assert!(
            result.is_err(),
            "invalid identity must fail before a request can be built"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gzip_request_compression_encodes_body_and_preserves_metadata() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let mut config = test_clickhouse_config(base_url);
        config.database = "moraine_team".to_string();
        config.username = "svc-moraine".to_string();
        config.password = "test-password".to_string();
        config.request_compression = ClickHouseRequestCompression::Gzip;
        let client = ClickHouseClient::new(config).expect("new client");
        let payload = br#"{"payload":"synthetic trace payload"}
"#
        .to_vec();

        client
            .request_text_with_params(
                "INSERT INTO tool_io FORMAT JSONEachRow",
                Some(payload.clone()),
                Some("moraine_team"),
                true,
                Some("JSONEachRow"),
                &[("query_id", "gzip-test")],
            )
            .await
            .expect("gzip request");

        let requests = requests.lock().expect("request capture mutex poisoned");
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(
            request
                .headers
                .get("content-encoding")
                .and_then(|value| value.to_str().ok()),
            Some("gzip")
        );
        assert_eq!(
            request
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(
            request
                .headers
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            Some(request.body.len().to_string().as_str())
        );
        assert!(request.headers.get("authorization").is_some());
        assert_eq!(
            request.params.get("query").map(String::as_str),
            Some("INSERT INTO tool_io FORMAT JSONEachRow")
        );
        assert_eq!(
            request.params.get("database").map(String::as_str),
            Some("moraine_team")
        );
        assert_eq!(
            request.params.get("default_format").map(String::as_str),
            Some("JSONEachRow")
        );
        assert_eq!(
            request.params.get("async_insert").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            request
                .params
                .get("wait_for_async_insert")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            request.params.get("query_id").map(String::as_str),
            Some("gzip-test")
        );

        let mut decoded = Vec::new();
        GzDecoder::new(request.body.as_slice())
            .read_to_end(&mut decoded)
            .expect("decode gzip body");
        assert_eq!(decoded, payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn default_request_compression_preserves_plain_body() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let payload = b"plain request body".to_vec();

        client
            .request_text("SELECT 1", Some(payload.clone()), None, false, None)
            .await
            .expect("plain request");

        let requests = requests.lock().expect("request capture mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].headers.get("content-encoding").is_none());
        assert_eq!(requests[0].body, payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_sql_statement_is_automatically_carried_in_request_body() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let statement = format!("INSERT INTO plans VALUES ('{}')", "x".repeat(512 * 1024));

        client
            .request_text(&statement, None, Some("moraine"), false, None)
            .await
            .expect("body-carried SQL request");

        let requests = requests.lock().expect("request capture mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].params.contains_key("query"));
        assert_eq!(requests[0].body, statement.as_bytes());
        assert_eq!(
            requests[0].params.get("max_query_size").map(String::as_str),
            Some("1048576")
        );
        assert_eq!(
            requests[0].params.get("database").map(String::as_str),
            Some("moraine")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn body_carried_sql_statement_respects_payload_limit() {
        let client =
            ClickHouseClient::new(test_clickhouse_config("http://127.0.0.1:1".to_string()))
                .expect("new client");
        let statement = "x".repeat(MAX_INSERT_PAYLOAD_BYTES + 1);

        let error = client
            .request_text(&statement, None, Some("moraine"), false, None)
            .await
            .expect_err("oversized body-carried SQL must be rejected before send");

        assert!(error.to_string().contains("maximum is 8388608 bytes"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gzip_request_compression_leaves_empty_body_unencoded() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let mut config = test_clickhouse_config(base_url);
        config.request_compression = ClickHouseRequestCompression::Gzip;
        let client = ClickHouseClient::new(config).expect("new client");

        client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect("empty request");

        let requests = requests.lock().expect("request capture mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].headers.get("content-encoding").is_none());
        assert!(requests[0].body.is_empty());
        assert_eq!(
            requests[0]
                .headers
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn query_rows_falls_back_to_json_each_row() {
        #[derive(Deserialize)]
        struct Row {
            value: u8,
        }

        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let rows: Vec<Row> = client
            .query_rows("SELECT 7 AS value", None)
            .await
            .expect("fallback query_rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 7);
    }

    #[test]
    fn error_kind_classifies_budget_relevant_codes() {
        for code in [159, 160, 209] {
            assert_eq!(
                ClickHouseErrorKind::from_code(code),
                ClickHouseErrorKind::DeadlineExceeded,
                "code {code}"
            );
        }
        for code in [158, 202, 241, 307, 396] {
            assert_eq!(
                ClickHouseErrorKind::from_code(code),
                ClickHouseErrorKind::ResourceExhausted,
                "code {code}"
            );
        }
        assert_eq!(
            ClickHouseErrorKind::from_code(394),
            ClickHouseErrorKind::QueryKilled
        );
        assert_eq!(
            ClickHouseErrorKind::from_code(117),
            ClickHouseErrorKind::Other
        );
    }

    #[test]
    fn exception_code_extraction_handles_representative_bodies() {
        assert_eq!(
            extract_clickhouse_exception_code(
                "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use \
                 1.10 GiB, maximum: 1.00 GiB. (MEMORY_LIMIT_EXCEEDED) (version 24.8.4.13)"
            ),
            Some(241)
        );
        // The first parsable code wins even after a non-numeric near-marker.
        assert_eq!(
            extract_clickhouse_exception_code(
                "prefix Code: notanumber then Code: 394. DB::Exception: Query was cancelled"
            ),
            Some(394)
        );
        assert_eq!(extract_clickhouse_exception_code("no code marker"), None);

        assert!(body_contains_clickhouse_exception(
            "{\"data\":[{\"value\":1}\nCode: 159. DB::Exception: Timeout exceeded"
        ));
        // Row data mentioning "Code:" without a DB:: exception is not a marker.
        assert!(!body_contains_clickhouse_exception(
            "{\"note\":\"see Code: 500 in the transcript\"}"
        ));
        assert!(!body_contains_clickhouse_exception("not-json"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_success_response_carries_typed_code_and_is_not_retried() {
        #[derive(Debug, Deserialize)]
        struct Row {
            #[allow(dead_code)]
            value: u8,
        }

        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_typed_error_server(queries.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .query_rows::<Row>("SELECT MEMORY", None)
            .await
            .expect_err("memory-limit failure propagates");

        let typed = err
            .downcast_ref::<ClickHouseHttpError>()
            .expect("typed clickhouse error");
        assert_eq!(typed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(typed.code(), Some(241));
        assert_eq!(typed.kind(), ClickHouseErrorKind::ResourceExhausted);
        assert_eq!(
            clickhouse_error_kind(&err),
            Some(ClickHouseErrorKind::ResourceExhausted)
        );
        let message = format!("{err:#}");
        assert!(message.contains("clickhouse returned 500"));
        assert!(message.contains("Code: 241"));

        let queries = queries.lock().expect("typed error queries mutex poisoned");
        assert_eq!(queries.len(), 1, "typed errors must not trigger the retry");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exception_code_header_classifies_body_without_inline_code() {
        #[derive(Debug, Deserialize)]
        struct Row {
            #[allow(dead_code)]
            value: u8,
        }

        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_typed_error_server(queries.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .query_rows::<Row>("SELECT HEADER_CODE", None)
            .await
            .expect_err("header-coded failure propagates");

        assert_eq!(
            clickhouse_error_kind(&err),
            Some(ClickHouseErrorKind::DeadlineExceeded)
        );
        let queries = queries.lock().expect("typed error queries mutex poisoned");
        assert_eq!(queries.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kill_truncated_envelope_body_is_typed_and_not_retried() {
        #[derive(Debug, Deserialize)]
        struct Row {
            #[allow(dead_code)]
            value: u8,
        }

        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_typed_error_server(queries.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .query_rows::<Row>("SELECT KILLED_ENVELOPE", None)
            .await
            .expect_err("kill-truncated body propagates");

        assert_eq!(
            clickhouse_error_kind(&err),
            Some(ClickHouseErrorKind::QueryKilled)
        );
        let queries = queries.lock().expect("typed error queries mutex poisoned");
        assert_eq!(
            queries.len(),
            1,
            "a kill-truncated 200-OK body must never re-execute the query"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn envelope_exception_field_is_typed_and_not_retried() {
        #[derive(Debug, Deserialize)]
        struct Row {
            #[allow(dead_code)]
            value: u8,
        }

        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_typed_error_server(queries.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .query_rows::<Row>("SELECT EXCEPTION_FIELD", None)
            .await
            .expect_err("envelope exception field propagates");

        assert_eq!(
            clickhouse_error_kind(&err),
            Some(ClickHouseErrorKind::DeadlineExceeded)
        );
        let queries = queries.lock().expect("typed error queries mutex poisoned");
        assert_eq!(queries.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn killed_json_each_row_body_is_typed_query_killed() {
        #[derive(Debug, Deserialize)]
        struct Row {
            #[allow(dead_code)]
            value: u8,
        }

        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_typed_error_server(queries.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .query_rows::<Row>("SELECT KILLED_ROWS FORMAT JSONEachRow", None)
            .await
            .expect_err("killed streaming body propagates");

        assert_eq!(
            clickhouse_error_kind(&err),
            Some(ClickHouseErrorKind::QueryKilled)
        );
        let queries = queries.lock().expect("typed error queries mutex poisoned");
        assert_eq!(queries.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn format_drift_without_exception_marker_still_falls_back() {
        #[derive(Deserialize)]
        struct Row {
            value: u8,
        }

        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_typed_error_server(queries.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let rows: Vec<Row> = client
            .query_rows("SELECT 7 AS value", None)
            .await
            .expect("drift fallback succeeds");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 7);

        let queries = queries.lock().expect("typed error queries mutex poisoned");
        assert_eq!(
            queries.len(),
            2,
            "genuine envelope drift re-issues the query as JSONEachRow"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_json_rows_chunks_large_payloads() {
        let lengths = Arc::new(Mutex::new(Vec::<usize>::new()));
        let base_url = spawn_insert_capture_server(lengths.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let large_value = "x".repeat((MAX_INSERT_PAYLOAD_BYTES / 2).saturating_add(1024));

        client
            .insert_json_rows(
                "raw_events",
                &[
                    json!({ "payload": large_value }),
                    json!({ "payload": large_value }),
                ],
            )
            .await
            .expect("chunked insert should succeed");

        let lengths = lengths.lock().expect("length capture mutex poisoned");
        assert_eq!(lengths.len(), 2, "rows should be split into two inserts");
        assert!(
            lengths.iter().all(|len| *len < MAX_INSERT_PAYLOAD_BYTES),
            "each captured payload should stay below the byte cap: {lengths:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_stream_with_params_sends_read_settings_and_streams_chunks() {
        let params = Arc::new(Mutex::new(Vec::<HashMap<String, String>>::new()));
        let content_lengths = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
        let base_url = spawn_stream_capture_server(StreamCaptureState {
            params: params.clone(),
            content_lengths: content_lengths.clone(),
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let mut stream = client
            .request_stream_with_params(
                "SELECT value FROM events FORMAT JSONEachRow",
                Some("moraine"),
                None,
                &[
                    ("query_id", "qid-test"),
                    ("readonly", "1"),
                    ("max_execution_time", "600"),
                ],
                Some(Duration::from_secs(630)),
            )
            .await
            .expect("stream request");

        let mut body = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            body.extend_from_slice(&chunk);
        }

        assert_eq!(
            String::from_utf8(body).expect("utf8"),
            "{\"value\":1}\n{\"value\":2}\n"
        );

        let params = params.lock().expect("stream params mutex poisoned");
        assert_eq!(params.len(), 1);
        assert_eq!(
            params[0].get("query").map(String::as_str),
            Some("SELECT value FROM events FORMAT JSONEachRow")
        );
        assert_eq!(
            params[0].get("database").map(String::as_str),
            Some("moraine")
        );
        assert_eq!(
            params[0].get("query_id").map(String::as_str),
            Some("qid-test")
        );
        assert_eq!(params[0].get("readonly").map(String::as_str), Some("1"));
        assert_eq!(
            params[0].get("max_execution_time").map(String::as_str),
            Some("600")
        );

        let content_lengths = content_lengths
            .lock()
            .expect("stream headers mutex poisoned");
        assert_eq!(content_lengths.as_slice(), &[Some("0".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_stream_with_params_includes_status_and_body_on_http_failure() {
        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .request_stream_with_params("SELECT FAIL", None, None, &[], None)
            .await
            .expect_err("expected HTTP failure");

        let msg = err.to_string();
        assert!(msg.contains("clickhouse returned"));
        assert!(msg.contains("500"));
        assert!(msg.contains("boom"));
    }

    #[test]
    fn migration_timeout_outlives_interactive_default_and_preserves_larger_override() {
        assert_eq!(migration_request_timeout(30.0), Duration::from_secs(300));
        assert_eq!(migration_request_timeout(900.0), Duration::from_secs(900));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_progress_reports_current_schema_without_work() {
        let applied = bundled_migrations()
            .into_iter()
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>();
        let base_url = spawn_migration_mock_server(MigrationMockState::new(
            applied,
            Arc::new(Mutex::new(Vec::new())),
        ))
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let mut events = Vec::new();

        let executed = client
            .run_migrations_with_progress(|event| events.push(event))
            .await
            .expect("current migrations");

        assert!(executed.is_empty());
        assert_eq!(
            events,
            vec![MigrationProgress::Plan {
                applied: bundled_migrations().len(),
                pending: 0,
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_progress_applies_latest_after_ledger_write() {
        let bundled = bundled_migrations();
        let latest = bundled.last().expect("latest migration").clone();
        let applied = bundled[..bundled.len() - 1]
            .iter()
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url =
            spawn_migration_mock_server(MigrationMockState::new(applied, queries.clone())).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let mut events = Vec::new();

        let executed = client
            .run_migrations_with_progress(|event| events.push(event))
            .await
            .expect("apply latest migration");

        assert_eq!(executed, vec![latest.version.to_string()]);
        assert_eq!(
            events,
            vec![
                MigrationProgress::Plan {
                    applied: bundled.len() - 1,
                    pending: 1,
                },
                MigrationProgress::Started {
                    index: 1,
                    total: 1,
                    version: latest.version,
                    name: latest.name,
                },
                MigrationProgress::Applied {
                    index: 1,
                    total: 1,
                    version: latest.version,
                    name: latest.name,
                },
            ]
        );
        let queries = queries.lock().expect("migration query mutex poisoned");
        let ledger_index = queries
            .iter()
            .position(|query| {
                query.starts_with("INSERT INTO") && query.contains("schema_migrations")
            })
            .expect("ledger insert query");
        assert_eq!(ledger_index, queries.len() - 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_progress_does_not_apply_when_ledger_write_fails() {
        let bundled = bundled_migrations();
        let latest = bundled.last().expect("latest migration").clone();
        let applied = bundled[..bundled.len() - 1]
            .iter()
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>();
        let base_url = spawn_migration_mock_server(MigrationMockState {
            fail_ledger_insert: true,
            ..MigrationMockState::new(applied, Arc::new(Mutex::new(Vec::new())))
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let mut events = Vec::new();

        let error = client
            .run_migrations_with_progress(|event| events.push(event))
            .await
            .expect_err("ledger insert must fail");

        assert!(error.to_string().contains("failed to record migration"));
        assert_eq!(
            events.last(),
            Some(&MigrationProgress::Started {
                index: 1,
                total: 1,
                version: latest.version,
                name: latest.name,
            })
        );
        assert!(events
            .iter()
            .all(|event| !matches!(event, MigrationProgress::Applied { .. })));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_statements_carry_envelope_deadline_honoring_operator_timeout() {
        let bundled = bundled_migrations();
        let applied = bundled[..bundled.len() - 1]
            .iter()
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>();
        let state = MigrationMockState::new(applied, Arc::new(Mutex::new(Vec::new())));
        let params = state.params.clone();
        let base_url = spawn_migration_mock_server(state).await;

        // Operator client timeout (900s) exceeds the default migration budget
        // deadline (600s): the per-statement Migration envelope must honor the
        // larger operator bound (amendment A5) — never lower than pre-envelope
        // behavior.
        let mut cfg = test_clickhouse_config(base_url);
        cfg.timeout_seconds = 900.0;
        let client = ClickHouseClient::new(cfg).expect("new client");

        client
            .run_migrations_with_progress(|_| {})
            .await
            .expect("apply latest migration");

        let params = params.lock().expect("migration params mutex poisoned");
        assert!(!params.is_empty(), "mock captured no requests");
        for request in params.iter() {
            let query_id = request
                .get("query_id")
                .expect("every migration statement carries an envelope query id");
            assert!(
                query_id.starts_with("moraine-migration-"),
                "unexpected query id {query_id}"
            );
            let max_execution_time = request
                .get("max_execution_time")
                .expect("every migration statement carries a server deadline")
                .parse::<f64>()
                .expect("parseable max_execution_time");
            assert!(
                max_execution_time > 600.0 && max_execution_time <= 900.0,
                "server deadline must honor the 900s operator bound, got {max_execution_time}"
            );
        }
        // Per-statement envelopes: no two statements share a request id, so
        // one upgrade's total is never capped by a single absolute deadline.
        let mut ids = params
            .iter()
            .filter_map(|request| request.get("query_id").cloned())
            .collect::<Vec<_>>();
        let total = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), total, "migration statement ids must be unique");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_skew_probe_compares_remote_ledger_without_writing() {
        let queries = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut versions: Vec<String> = bundled_migrations()
            .iter()
            .map(|m| m.version.to_string())
            .collect();
        versions.pop(); // server is behind by the newest bundled migration
        versions.push("999".to_string()); // and ahead by one unknown version

        let base_url = spawn_skew_mock_server(SkewMockState {
            ledger_exists: true,
            versions,
            queries: queries.clone(),
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let skew = client.schema_skew().await.expect("skew probe");
        let newest = bundled_migrations()
            .last()
            .expect("bundled migrations non-empty")
            .version;
        assert_eq!(skew.missing_on_server, vec![newest.to_string()]);
        assert_eq!(skew.unknown_on_server, vec!["999".to_string()]);

        // The probe must be read-only: no CREATE/INSERT may reach the server.
        let queries = queries.lock().expect("query capture mutex poisoned");
        assert!(
            queries
                .iter()
                .all(|q| !q.contains("CREATE") && !q.contains("INSERT")),
            "schema_skew issued a write statement: {queries:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_skew_probe_treats_missing_ledger_as_all_missing() {
        let base_url = spawn_skew_mock_server(SkewMockState {
            ledger_exists: false,
            versions: vec!["001".to_string()], // must never be consulted
            queries: Arc::new(Mutex::new(Vec::new())),
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let skew = client.schema_skew().await.expect("skew probe");
        assert_eq!(skew.missing_on_server.len(), bundled_migrations().len());
        assert!(skew.unknown_on_server.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_text_includes_status_and_body_on_http_failure() {
        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .request_text("SELECT FAIL", None, None, false, None)
            .await
            .expect_err("expected HTTP failure");

        let msg = err.to_string();
        assert!(msg.contains("clickhouse returned"));
        assert!(msg.contains("500"));
        assert!(msg.contains("boom"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_text_propagates_response_body_read_errors() {
        let base_url = spawn_truncated_body_server();
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect_err("expected response body read failure");

        let msg = err.to_string();
        assert!(msg.contains("failed to read clickhouse response body"));
    }

    // ------------------------------------------------------------------
    // Query envelope transport enforcement (issue #600, W4/W5)
    // ------------------------------------------------------------------

    #[derive(Clone, Default)]
    struct EnvelopeCaptureState {
        requests: Arc<Mutex<Vec<EnvelopeCapturedRequest>>>,
    }

    #[derive(Debug, Clone)]
    struct EnvelopeCapturedRequest {
        query: String,
        pairs: Vec<(String, String)>,
    }

    impl EnvelopeCapturedRequest {
        fn pair(&self, key: &str) -> Option<&str> {
            self.pairs
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.as_str())
        }

        fn pair_count(&self, key: &str) -> usize {
            self.pairs.iter().filter(|(name, _)| name == key).count()
        }
    }

    impl EnvelopeCaptureState {
        fn snapshot(&self) -> Vec<EnvelopeCapturedRequest> {
            self.requests
                .lock()
                .expect("envelope capture mutex poisoned")
                .clone()
        }

        async fn wait_for<F>(&self, deadline: Duration, predicate: F) -> bool
        where
            F: Fn(&[EnvelopeCapturedRequest]) -> bool,
        {
            let started = std::time::Instant::now();
            loop {
                if predicate(&self.snapshot()) {
                    return true;
                }
                if started.elapsed() > deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// Mock ClickHouse that records the exact query-string pairs of every
    /// request, answers with a trustworthy-looking summary header
    /// (read_rows=100, read_bytes=2048), and hangs on queries containing
    /// SLOW so their futures can be dropped mid-flight.
    async fn spawn_envelope_capture_server(state: EnvelopeCaptureState) -> String {
        use axum::extract::RawQuery;
        use axum::response::IntoResponse;

        async fn handler(
            State(state): State<EnvelopeCaptureState>,
            RawQuery(raw_query): RawQuery,
            body: Bytes,
        ) -> axum::response::Response {
            let pairs: Vec<(String, String)> =
                url::form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes())
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect();
            let query = pairs
                .iter()
                .find(|(key, _)| key == "query")
                .map(|(_, value)| value.clone())
                .unwrap_or_else(|| String::from_utf8_lossy(&body).into_owned());
            state
                .requests
                .lock()
                .expect("envelope capture mutex poisoned")
                .push(EnvelopeCapturedRequest {
                    query: query.clone(),
                    pairs,
                });

            if query.contains("SLOW") {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }

            (
                StatusCode::OK,
                [(
                    "x-clickhouse-summary",
                    "{\"read_rows\":\"100\",\"read_bytes\":\"2048\",\"written_rows\":\"0\"}",
                )],
                "ok\n",
            )
                .into_response()
        }

        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind envelope capture listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enveloped_statement_carries_budget_settings_and_child_query_id() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let request_id = query_envelope.request_id().to_string();

        Arc::clone(&query_envelope)
            .scope(async {
                client
                    .request_text("SELECT 1", None, None, false, None)
                    .await
            })
            .await
            .expect("enveloped statement");

        let requests = state.snapshot();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(
            request.pair("query_id"),
            Some(format!("{request_id}-0").as_str())
        );
        let max_execution_time: f64 = request
            .pair("max_execution_time")
            .expect("max_execution_time present")
            .parse()
            .expect("parsable max_execution_time");
        assert!(
            max_execution_time > 0.0 && max_execution_time <= 30.0,
            "max_execution_time out of range: {max_execution_time}"
        );
        assert_eq!(request.pair("timeout_overflow_mode"), Some("throw"));
        assert_eq!(
            request.pair("max_memory_usage"),
            Some((64 * 1024 * 1024).to_string().as_str())
        );
        assert_eq!(
            request.pair("max_bytes_before_external_group_by"),
            Some((8 * 1024 * 1024).to_string().as_str())
        );
        assert_eq!(
            request.pair("max_bytes_before_external_sort"),
            Some((8 * 1024 * 1024).to_string().as_str())
        );
        assert_eq!(request.pair("max_rows_to_read"), Some("1000"));
        assert_eq!(request.pair("max_bytes_to_read"), Some("1000000"));
        assert_eq!(request.pair("wait_end_of_query"), Some("1"));

        // The trustworthy end-of-query summary decremented the allowance.
        let stats = query_envelope.stats();
        assert_eq!(stats.statements, 1);
        assert_eq!(stats.rows_consumed, 100);
        assert_eq!(stats.bytes_consumed, 2_048);
        assert_eq!(stats.rows_remaining, 900);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn envelope_wins_budget_params_and_min_merges_caller_execution_time() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let request_id = query_envelope.request_id().to_string();

        query_envelope
            .scope(async {
                client
                    .request_text_with_params(
                        "SELECT 1",
                        None,
                        None,
                        false,
                        None,
                        &[
                            ("query_id", "caller-id"),
                            ("max_execution_time", "2.0"),
                            ("max_rows_to_read", "999999999"),
                            ("readonly", "1"),
                        ],
                    )
                    .await
            })
            .await
            .expect("enveloped statement");

        let requests = state.snapshot();
        let request = &requests[0];
        // Envelope wins budget params: exactly one instance of each, and the
        // caller's query id / rows ceiling are gone.
        assert_eq!(request.pair_count("query_id"), 1);
        assert_eq!(
            request.pair("query_id"),
            Some(format!("{request_id}-0").as_str())
        );
        assert_eq!(request.pair_count("max_rows_to_read"), 1);
        assert_eq!(request.pair("max_rows_to_read"), Some("1000"));
        // A caller execution cap tighter than the remaining deadline is kept.
        assert_eq!(request.pair_count("max_execution_time"), 1);
        assert_eq!(request.pair("max_execution_time"), Some("2.000"));
        // Non-budget caller params pass through untouched.
        assert_eq!(request.pair("readonly"), Some("1"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enveloped_insert_skips_read_ceilings_but_counts_against_cap() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Background, &budget);

        Arc::clone(&query_envelope)
            .scope(async {
                client
                    .insert_json_rows("events", &[json!({"value": 1})])
                    .await
            })
            .await
            .expect("enveloped insert");

        let requests = state.snapshot();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert!(request.query.starts_with("INSERT INTO"));
        assert!(request.pair("query_id").is_some());
        assert!(request.pair("max_execution_time").is_some());
        assert!(request.pair("max_memory_usage").is_some());
        // Writes skip the read-ceiling settings and the summary wait.
        assert_eq!(request.pair("max_rows_to_read"), None);
        assert_eq!(request.pair("max_bytes_to_read"), None);
        assert_eq!(request.pair("wait_end_of_query"), None);
        // But the statement still consumed a cap slot.
        assert_eq!(query_envelope.stats().statements, 1);
        // And the write summary did not drain the read allowance.
        assert_eq!(query_envelope.stats().rows_consumed, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn summary_decrements_shrink_later_statement_read_ceilings() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);

        Arc::clone(&query_envelope)
            .scope(async {
                for _ in 0..3 {
                    client
                        .request_text("SELECT 1", None, None, false, None)
                        .await
                        .expect("enveloped statement");
                }
            })
            .await;

        let requests = state.snapshot();
        let ceilings: Vec<&str> = requests
            .iter()
            .map(|request| request.pair("max_rows_to_read").expect("rows ceiling"))
            .collect();
        // Each statement's ceiling is the REMAINING allowance after the
        // previous statement's 100-row summary decrement.
        assert_eq!(ceilings, vec!["1000", "900", "800"]);
        assert_eq!(query_envelope.stats().rows_remaining, 700);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admission_failures_are_typed_and_fail_fast_without_reaching_server() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        // Statement cap.
        let budget = envelope::test_budget(30.0, 1, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let error = Arc::clone(&query_envelope)
            .scope(async {
                client
                    .request_text("SELECT 1", None, None, false, None)
                    .await
                    .expect("first statement fits the cap");
                client
                    .request_text("SELECT 1", None, None, false, None)
                    .await
                    .expect_err("second statement exceeds the cap")
            })
            .await;
        assert_eq!(
            envelope_error_kind(&error),
            Some(ClickHouseErrorKind::ResourceExhausted)
        );
        assert_eq!(state.snapshot().len(), 1, "capped statement reached server");

        // Expired deadline: refused client-side, nothing new reaches the server.
        let expired_budget = envelope::test_budget(0.005, 8, 1_000, 1_000_000);
        let expired_envelope =
            QueryEnvelope::new("request", QueryClass::Interactive, &expired_budget);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let error = expired_envelope
            .scope(async {
                client
                    .request_text("SELECT 1", None, None, false, None)
                    .await
                    .expect_err("expired envelope must refuse admission")
            })
            .await;
        assert_eq!(
            envelope_error_kind(&error),
            Some(ClickHouseErrorKind::DeadlineExceeded)
        );
        assert_eq!(
            state.snapshot().len(),
            1,
            "expired statement reached server"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unenveloped_statement_executes_as_today_and_increments_counter() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let before = unenveloped_statement_count();
        client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect("unenveloped statement still executes pre-flip");
        assert!(unenveloped_statement_count() > before);

        let requests = state.snapshot();
        let request = &requests[0];
        assert_eq!(request.pair("query_id"), None);
        assert_eq!(request.pair("max_execution_time"), None);
        assert_eq!(request.pair("wait_end_of_query"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_statement_future_kills_its_child_query() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let child_id = format!("{}-0", query_envelope.request_id());

        query_envelope
            .scope(async {
                // The tokio timeout drops the in-flight statement future —
                // the #576 orphan pattern. The scope itself completes
                // normally afterwards.
                let _ = tokio::time::timeout(
                    Duration::from_millis(200),
                    client.request_text("SELECT SLOW", None, None, false, None),
                )
                .await;
            })
            .await;

        let expected = format!("KILL QUERY WHERE query_id = '{child_id}' SYNC");
        assert!(
            state
                .wait_for(Duration::from_secs(3), |requests| {
                    requests.iter().any(|request| request.query == expected)
                })
                .await,
            "no child KILL observed; captured: {:?}",
            state.snapshot()
        );

        // The KILL itself runs enveloped: administrative kind, own child
        // query id, and a finite deadline.
        let requests = state.snapshot();
        let kill = requests
            .iter()
            .find(|request| request.query == expected)
            .expect("kill captured");
        let kill_query_id = kill.pair("query_id").expect("kill query_id");
        assert!(
            kill_query_id.starts_with("moraine-kill-"),
            "kill query id: {kill_query_id}"
        );
        let kill_deadline: f64 = kill
            .pair("max_execution_time")
            .expect("kill max_execution_time")
            .parse()
            .expect("parsable kill deadline");
        assert!(kill_deadline > 0.0 && kill_deadline <= 5.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_scope_issues_prefix_kill_for_the_whole_request() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
        let request_id = query_envelope.request_id().to_string();

        let task = tokio::spawn({
            let client = client.clone();
            query_envelope.scope(async move {
                let _ = client
                    .request_text("SELECT SLOW", None, None, false, None)
                    .await;
            })
        });

        assert!(
            state
                .wait_for(Duration::from_secs(3), |requests| {
                    requests
                        .iter()
                        .any(|request| request.query.contains("SLOW"))
                })
                .await,
            "slow statement never reached the server"
        );
        task.abort();

        // Aborting the scope drops both guards: the statement guard KILLs
        // the child, and the request guard KILLs the whole id prefix.
        let prefix_clause = format!("startsWith(query_id, '{request_id}-')");
        assert!(
            state
                .wait_for(Duration::from_secs(3), |requests| {
                    requests.iter().any(|request| {
                        request.query.starts_with("KILL QUERY WHERE query_id = ")
                            && request.query.contains(&prefix_clause)
                    })
                })
                .await,
            "no prefix KILL observed; captured: {:?}",
            state.snapshot()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_class_statements_get_no_drop_guard_kill() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("migrate", QueryClass::Migration, &budget);

        query_envelope
            .scope(async {
                let _ = tokio::time::timeout(
                    Duration::from_millis(200),
                    client.request_text("SELECT SLOW", None, None, false, None),
                )
                .await;
            })
            .await;

        // Give any (incorrect) kill task ample time to land.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(
            state
                .snapshot()
                .iter()
                .all(|request| !request.query.starts_with("KILL QUERY")),
            "migration statement must rely on server max_execution_time only; \
             captured: {:?}",
            state.snapshot()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enveloped_stream_enforces_ceilings_without_summary_wait() {
        let state = EnvelopeCaptureState::default();
        let base_url = spawn_envelope_capture_server(state.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let budget = envelope::test_budget(30.0, 8, 1_000, 1_000_000);
        let query_envelope = QueryEnvelope::new("export", QueryClass::Background, &budget);

        Arc::clone(&query_envelope)
            .scope(async {
                let mut stream = client
                    .request_stream_with_params("SELECT 1", None, None, &[], None)
                    .await
                    .expect("enveloped stream");
                while stream.next_chunk().await.expect("stream chunk").is_some() {}
            })
            .await;

        let requests = state.snapshot();
        let request = &requests[0];
        // The per-statement ceiling is enforced server-side...
        assert_eq!(request.pair("max_rows_to_read"), Some("1000"));
        // ...but streams skip the end-of-query wait and cumulative decrement.
        assert_eq!(request.pair("wait_end_of_query"), None);
        assert_eq!(query_envelope.stats().rows_consumed, 0);

        // A fully consumed stream disarms its guard: no KILL follows.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            state
                .snapshot()
                .iter()
                .all(|request| !request.query.starts_with("KILL QUERY")),
            "completed stream must not be killed"
        );
    }
}
