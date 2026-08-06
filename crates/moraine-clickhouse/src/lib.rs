#[cfg(test)]
use anyhow::anyhow;
use anyhow::{bail, Context, Result};
use flate2::{write::GzEncoder, Compression};
use moraine_config::{ClickHouseConfig, ClickHouseRequestCompression};
use reqwest::{
    header::{HeaderValue, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, USER_AGENT},
    Client, RequestBuilder, StatusCode, Url,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

pub mod mcp_tool_names;
pub mod owner;
use owner::{error_for_cause, extract_exception_code, AdminBackend, StatementTicket};
pub use owner::{
    ClickHouseError, ClickHouseErrorCategory, OwnerGuard, QueryCause, QueryOwner, QueryRuntime,
    QueryWorkload, QUERY_CLEANUP_GRACE,
};

const MAX_INSERT_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_USER_AGENT_ROLE: &str = "moraine-clickhouse";
const RESERVED_PARAMS: &[&str] = &[
    "query_id",
    "replace_running_query",
    "max_execution_time",
    "max_execution_time_leaf",
    "timeout_overflow_mode",
    "timeout_before_checking_execution_speed",
];

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    inner: Arc<ClientInner>,
}

struct ClientInner {
    http: Client,
    admin: Arc<AdminBackend>,
    runtime: QueryRuntime,
}

pub struct ClickHouseByteStream {
    response: reqwest::Response,
    ticket: Option<StatementTicket>,
    owner: Arc<QueryOwner>,
    buffered: Vec<u8>,
    reached_eof: bool,
}

impl std::fmt::Debug for ClickHouseByteStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseByteStream")
            .field("response", &self.response)
            .field("owner", &self.owner.logical_id())
            .finish_non_exhaustive()
    }
}

impl ClickHouseByteStream {
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if self.reached_eof {
            if self.buffered.is_empty() {
                if let Some(mut ticket) = self.ticket.take() {
                    ticket.succeed();
                }
                return Ok(None);
            }
            let tail = std::mem::take(&mut self.buffered);
            return Ok(Some(tail));
        }

        loop {
            let chunk = read_response_chunk(&self.owner, &mut self.response).await;
            match chunk {
                Ok(Some(bytes)) => self.buffered.extend_from_slice(&bytes),
                Ok(None) => self.reached_eof = true,
                Err(error) => {
                    if let Some(mut ticket) = self.ticket.take() {
                        ticket.fail(cause_for_error(&error));
                    }
                    return Err(error.into());
                }
            }

            if let Some((code, start)) = find_exception_tail(&self.buffered) {
                let detail = String::from_utf8_lossy(&self.buffered[start..]).into_owned();
                self.buffered.truncate(start);
                let error = classify_exception(
                    &self.owner,
                    code,
                    "ClickHouse stream ended with an exception",
                    Some(&detail),
                );
                if let Some(mut ticket) = self.ticket.take() {
                    ticket.fail(cause_for_error(&error));
                }
                return Err(error.into());
            }

            if self.reached_eof {
                if self.buffered.is_empty() {
                    if let Some(mut ticket) = self.ticket.take() {
                        ticket.succeed();
                    }
                    return Ok(None);
                }
                let tail = std::mem::take(&mut self.buffered);
                return Ok(Some(tail));
            }

            // Retain the incomplete final line so an appended ClickHouse
            // exception is classified before any part of that line is emitted.
            if let Some(boundary) = self.buffered.iter().rposition(|byte| *byte == b'\n') {
                let trailing = self.buffered.split_off(boundary + 1);
                let complete = std::mem::replace(&mut self.buffered, trailing);
                return Ok(Some(complete));
            }
        }
    }
}

struct ClickHouseRequestOptions<'a> {
    database: Option<&'a str>,
    async_insert: bool,
    default_format: Option<&'a str>,
    params: &'a [(&'a str, &'a str)],
}

struct PreparedRequest {
    builder: RequestBuilder,
    ticket: StatementTicket,
    owner: Arc<QueryOwner>,
}

#[derive(Deserialize)]
struct ClickHouseEnvelope<T> {
    data: Vec<T>,
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
    pub errors: Vec<String>,
}

impl ClickHouseClient {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        Self::new_with_runtime(cfg, QueryRuntime::new())
    }

    pub fn new_with_runtime(cfg: ClickHouseConfig, runtime: QueryRuntime) -> Result<Self> {
        let user_agent = format!(
            "{DEFAULT_USER_AGENT_ROLE}/{} (pid={})",
            moraine_config::BUILD_VERSION,
            std::process::id()
        );
        Self::new_with_runtime_and_user_agent(cfg, runtime, user_agent)
    }

    /// Construct an isolated compatibility client with a custom User-Agent.
    pub fn new_with_user_agent(cfg: ClickHouseConfig, user_agent: impl AsRef<str>) -> Result<Self> {
        Self::new_with_runtime_and_user_agent(cfg, QueryRuntime::new(), user_agent)
    }

    pub fn new_with_runtime_and_user_agent(
        cfg: ClickHouseConfig,
        runtime: QueryRuntime,
        user_agent: impl AsRef<str>,
    ) -> Result<Self> {
        validate_connect_timeout(cfg.timeout_seconds)?;
        let url = validate_base_url(&cfg.url)?;
        let user_agent = HeaderValue::try_from(user_agent.as_ref())
            .context("invalid ClickHouse HTTP User-Agent")?;
        let mut default_headers = reqwest::header::HeaderMap::with_capacity(1);
        default_headers.insert(USER_AGENT, user_agent);
        let http = Client::builder()
            .connect_timeout(Duration::from_secs_f64(cfg.timeout_seconds))
            .default_headers(default_headers.clone())
            .build()
            .context("failed to construct reqwest data client")?;
        let admin = AdminBackend::new(
            url,
            cfg.username.clone(),
            cfg.password.clone(),
            default_headers,
        )?;

        Ok(Self {
            cfg,
            inner: Arc::new(ClientInner {
                http,
                admin,
                runtime,
            }),
        })
    }

    pub fn config(&self) -> &ClickHouseConfig {
        &self.cfg
    }

    pub fn runtime(&self) -> QueryRuntime {
        self.inner.runtime.clone()
    }

    fn base_url(&self) -> Result<Url> {
        validate_base_url(&self.cfg.url).map_err(Into::into)
    }

    async fn request_builder(
        &self,
        query: &str,
        body: Vec<u8>,
        options: ClickHouseRequestOptions<'_>,
    ) -> Result<PreparedRequest> {
        // Fail closed before compression, connection establishment, or bytes.
        let owner = QueryOwner::current().ok_or_else(|| {
            ClickHouseError::ownership("ClickHouse egress requires an explicit QueryOwner")
        })?;
        validate_request_params(options.params)?;
        let mut url = self.base_url()?;

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

        // The absolute deadline is checked again after potentially expensive
        // compression. Expired operations register no child and send zero bytes.
        let remaining = owner.remaining()?;
        let ticket = owner.register_statement(self.inner.admin.clone())?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", query);
            qp.append_pair("query_id", ticket.query_id());
            qp.append_pair("replace_running_query", "0");
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
                qp.append_pair(key, value);
            }
            if let Some(remaining) = remaining {
                qp.append_pair("max_execution_time", &format_clickhouse_deadline(remaining));
                qp.append_pair("timeout_before_checking_execution_speed", "0");
                qp.append_pair("timeout_overflow_mode", "throw");
            }
        }

        let payload_len = body.len();
        let mut builder = self
            .inner
            .http
            .post(url)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(CONTENT_LENGTH, payload_len)
            .body(body);
        if let Some(content_encoding) = content_encoding {
            builder = builder.header(CONTENT_ENCODING, content_encoding);
        }
        if let Some(remaining) = remaining {
            builder = builder.timeout(remaining);
        }
        if !self.cfg.username.is_empty() {
            builder =
                builder.basic_auth(self.cfg.username.clone(), Some(self.cfg.password.clone()));
        }
        Ok(PreparedRequest {
            builder,
            ticket,
            owner,
        })
    }

    async fn send_checked_response(
        &self,
        mut prepared: PreparedRequest,
    ) -> Result<(reqwest::Response, StatementTicket, Arc<QueryOwner>)> {
        prepared.ticket.mark_attempted();
        let response = send_request(&prepared.owner, prepared.builder).await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                prepared.ticket.fail(cause_for_error(&error));
                return Err(error.into());
            }
        };
        prepared.ticket.mark_headers_received();
        let status = response.status();
        if !status.is_success() {
            let header_code = response
                .headers()
                .get("x-clickhouse-exception-code")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u32>().ok());
            let body = match read_response_text(&prepared.owner, response).await {
                Ok(body) => body,
                Err(error) => {
                    prepared.ticket.fail(cause_for_error(&error));
                    return Err(error.into());
                }
            };
            let code = header_code.or_else(|| extract_exception_code(&body));
            let error = classify_response(&prepared.owner, status, code, Some(&body));
            prepared.ticket.fail(cause_for_error(&error));
            return Err(error.into());
        }
        Ok((response, prepared.ticket, prepared.owner))
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
        self.request_text_with_options(query, body, database, async_insert, default_format, params)
            .await
    }

    async fn request_text_with_options(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
        params: &[(&str, &str)],
    ) -> Result<String> {
        let prepared = self
            .request_builder(
                query,
                body.unwrap_or_default(),
                ClickHouseRequestOptions {
                    database,
                    async_insert,
                    default_format,
                    params,
                },
            )
            .await?;
        let (response, mut ticket, owner) = self.send_checked_response(prepared).await?;
        let bytes = match read_response_bytes(&owner, response).await {
            Ok(bytes) => bytes,
            Err(error) => {
                ticket.fail(cause_for_error(&error));
                return Err(error.into());
            }
        };
        if let Some((code, start)) = find_exception_tail(&bytes) {
            let detail = String::from_utf8_lossy(&bytes[start..]);
            let error = classify_exception(
                &owner,
                code,
                "ClickHouse response ended with an exception",
                Some(&detail),
            );
            ticket.fail(cause_for_error(&error));
            return Err(error.into());
        }
        let text = match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                ticket.fail(QueryCause::Backend);
                return Err(
                    ClickHouseError::backend("ClickHouse returned a non-UTF-8 response").into(),
                );
            }
        };
        ticket.succeed();
        Ok(text)
    }

    /// The compatibility timeout argument must be `None`: operation deadlines
    /// are absolute and belong to `QueryOwner::with_deadline`.
    pub async fn request_stream_with_params(
        &self,
        query: &str,
        database: Option<&str>,
        default_format: Option<&str>,
        params: &[(&str, &str)],
        request_timeout: Option<Duration>,
    ) -> Result<ClickHouseByteStream> {
        if request_timeout.is_some() {
            return Err(ClickHouseError::ownership(
                "elapsed request timeout is reserved; use QueryOwner::with_deadline",
            )
            .into());
        }
        let prepared = self
            .request_builder(
                query,
                Vec::new(),
                ClickHouseRequestOptions {
                    database,
                    async_insert: false,
                    default_format,
                    params,
                },
            )
            .await?;
        let (response, ticket, owner) = self.send_checked_response(prepared).await?;
        Ok(ClickHouseByteStream {
            response,
            ticket: Some(ticket),
            owner,
            buffered: Vec::new(),
            reached_eof: false,
        })
    }

    pub async fn ping(&self) -> Result<()> {
        let response = self
            .request_text("SELECT 1", None, Some("system"), false, None)
            .await?;
        if response.trim() == "1" {
            Ok(())
        } else {
            Err(ClickHouseError::backend("unexpected ClickHouse ping response").into())
        }
    }

    pub async fn version(&self) -> Result<String> {
        let rows: Vec<Value> = self
            .query_json_data("SELECT version() AS version", Some("system"))
            .await?;
        rows.first()
            .and_then(|row| row.get("version"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .ok_or_else(|| ClickHouseError::backend("missing version in ClickHouse payload").into())
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
        serde_json::Deserializer::from_str(&raw)
            .into_iter::<T>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| {
                ClickHouseError::backend("failed to parse ClickHouse JSONEachRow response").into()
            })
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
        serde_json::from_str::<ClickHouseEnvelope<T>>(&raw)
            .map(|envelope| envelope.data)
            .map_err(|_| ClickHouseError::backend("invalid ClickHouse JSON data envelope").into())
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
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text_with_params(query, None, database, false, Some("JSON"), params)
            .await?;
        let value: Value = serde_json::from_str(&raw)
            .map_err(|_| ClickHouseError::backend("invalid ClickHouse JSON response"))?;
        if value
            .as_object()
            .is_some_and(|object| !object.contains_key("data"))
        {
            // The only compatibility replay: the first HTTP exchange and body
            // completed normally and the JSON envelope structurally lacks data.
            return self
                .query_json_each_row_with_params(query, database, params)
                .await;
        }
        serde_json::from_value::<ClickHouseEnvelope<T>>(value)
            .map(|envelope| envelope.data)
            .map_err(|_| ClickHouseError::backend("invalid ClickHouse JSON data envelope").into())
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
                self.request_text(
                    &query,
                    Some(std::mem::take(&mut payload)),
                    None,
                    async_insert,
                    None,
                )
                .await?;
            }
            payload.extend_from_slice(&line);
            payload.push(b'\n');
        }

        if !payload.is_empty() {
            self.request_text(&query, Some(payload), None, async_insert, None)
                .await?;
        }
        Ok(())
    }

    pub async fn run_migrations(&self) -> Result<Vec<String>> {
        self.run_migrations_with_progress(|_| {}).await
    }

    pub async fn run_migrations_with_progress<F>(&self, mut on_progress: F) -> Result<Vec<String>>
    where
        F: FnMut(MigrationProgress),
    {
        validate_identifier(&self.cfg.database)?;

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
                self.request_text_with_options(
                    &statement,
                    None,
                    Some(&self.cfg.database),
                    false,
                    None,
                    &[],
                )
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
            self.request_text_with_options(
                &log_stmt,
                None,
                Some(&self.cfg.database),
                false,
                None,
                &[],
            )
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
            "ingest_errors",
            "ingest_checkpoints",
            "ingest_heartbeats",
            "mcp_event_locator",
            "mcp_event_navigation",
            "search_postings",
            "search_term_stats",
            "search_corpus_stats",
            "search_query_log",
            "search_hit_log",
            "search_interaction_log",
            "file_attention_project_roots",
            "schema_migrations",
        ];

        match self
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
            }
            Err(err) => report.errors.push(format!("table listing failed: {err}")),
        }

        Ok(report)
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

fn validate_connect_timeout(seconds: f64) -> Result<()> {
    if !seconds.is_finite() || seconds <= 0.0 {
        bail!("clickhouse.timeout_seconds must be finite and greater than zero");
    }
    Ok(())
}

fn is_reserved_param(key: &str) -> bool {
    RESERVED_PARAMS
        .iter()
        .any(|reserved| key.eq_ignore_ascii_case(reserved))
}

fn validate_base_url(raw: &str) -> std::result::Result<Url, ClickHouseError> {
    let url = Url::parse(raw).map_err(|_| ClickHouseError::ownership("invalid ClickHouse URL"))?;
    if let Some((key, _)) = url.query_pairs().find(|(key, _)| is_reserved_param(key)) {
        return Err(ClickHouseError::ownership(format!(
            "ClickHouse URL contains reserved parameter `{key}`"
        )));
    }
    Ok(url)
}

fn validate_request_params(params: &[(&str, &str)]) -> std::result::Result<(), ClickHouseError> {
    if let Some((key, _)) = params.iter().find(|(key, _)| is_reserved_param(key)) {
        return Err(ClickHouseError::ownership(format!(
            "ClickHouse request contains reserved parameter `{key}`"
        )));
    }
    Ok(())
}

fn format_clickhouse_deadline(remaining: Duration) -> String {
    // Ceiling to a microsecond ensures the server-side representation never
    // shortens the caller's absolute deadline.
    let micros = remaining.as_nanos().saturating_add(999) / 1_000;
    format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

async fn send_request(
    owner: &Arc<QueryOwner>,
    builder: RequestBuilder,
) -> std::result::Result<reqwest::Response, ClickHouseError> {
    let token = owner.cancellation_token();
    if let Some(deadline) = owner.deadline() {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                owner.cancel(QueryCause::Deadline);
                Err(ClickHouseError::deadline("ClickHouse query deadline exceeded"))
            }
            _ = token.cancelled() => Err(error_for_cause(owner.cause().unwrap_or(QueryCause::Abandoned), "ClickHouse query cancelled")),
            result = builder.send() => result.map_err(|error| ClickHouseError::transport("ClickHouse request failed", error)),
        }
    } else {
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(error_for_cause(owner.cause().unwrap_or(QueryCause::Abandoned), "ClickHouse query cancelled")),
            result = builder.send() => result.map_err(|error| ClickHouseError::transport("ClickHouse request failed", error)),
        }
    }
}

async fn read_response_bytes(
    owner: &Arc<QueryOwner>,
    response: reqwest::Response,
) -> std::result::Result<Vec<u8>, ClickHouseError> {
    let token = owner.cancellation_token();
    if let Some(deadline) = owner.deadline() {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                owner.cancel(QueryCause::Deadline);
                Err(ClickHouseError::deadline("ClickHouse query deadline exceeded"))
            }
            _ = token.cancelled() => Err(error_for_cause(owner.cause().unwrap_or(QueryCause::Abandoned), "ClickHouse query cancelled")),
            result = response.bytes() => result
                .map(|bytes| bytes.to_vec())
                .map_err(|error| ClickHouseError::transport("failed to read ClickHouse response body", error)),
        }
    } else {
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(error_for_cause(owner.cause().unwrap_or(QueryCause::Abandoned), "ClickHouse query cancelled")),
            result = response.bytes() => result
                .map(|bytes| bytes.to_vec())
                .map_err(|error| ClickHouseError::transport("failed to read ClickHouse response body", error)),
        }
    }
}

async fn read_response_text(
    owner: &Arc<QueryOwner>,
    response: reqwest::Response,
) -> std::result::Result<String, ClickHouseError> {
    let bytes = read_response_bytes(owner, response).await?;
    String::from_utf8(bytes)
        .map_err(|_| ClickHouseError::backend("ClickHouse returned a non-UTF-8 error response"))
}

async fn read_response_chunk(
    owner: &Arc<QueryOwner>,
    response: &mut reqwest::Response,
) -> std::result::Result<Option<Vec<u8>>, ClickHouseError> {
    let token = owner.cancellation_token();
    if let Some(deadline) = owner.deadline() {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                owner.cancel(QueryCause::Deadline);
                Err(ClickHouseError::deadline("ClickHouse query deadline exceeded"))
            }
            _ = token.cancelled() => Err(error_for_cause(owner.cause().unwrap_or(QueryCause::Abandoned), "ClickHouse query cancelled")),
            result = response.chunk() => result
                .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
                .map_err(|error| ClickHouseError::transport("failed to read ClickHouse response chunk", error)),
        }
    } else {
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(error_for_cause(owner.cause().unwrap_or(QueryCause::Abandoned), "ClickHouse query cancelled")),
            result = response.chunk() => result
                .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
                .map_err(|error| ClickHouseError::transport("failed to read ClickHouse response chunk", error)),
        }
    }
}

fn find_exception_tail(body: &[u8]) -> Option<(u32, usize)> {
    let text = std::str::from_utf8(body).ok()?;
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with("Code:") && trimmed.contains("DB::Exception") {
            return extract_exception_code(trimmed)
                .map(|code| (code, offset + line.len() - trimmed.len()));
        }
        offset += line.len();
    }
    None
}

fn classify_response(
    owner: &Arc<QueryOwner>,
    status: StatusCode,
    code: Option<u32>,
    detail: Option<&str>,
) -> ClickHouseError {
    let category = classify_code(owner, code);
    ClickHouseError::response(
        category,
        "ClickHouse request was rejected",
        status,
        code,
        detail,
    )
}

fn classify_exception(
    owner: &Arc<QueryOwner>,
    code: u32,
    context: &str,
    detail: Option<&str>,
) -> ClickHouseError {
    ClickHouseError::exception(classify_code(owner, Some(code)), context, code, detail)
}

fn classify_code(owner: &Arc<QueryOwner>, code: Option<u32>) -> ClickHouseErrorCategory {
    match code {
        Some(159 | 160 | 209) => ClickHouseErrorCategory::DeadlineExceeded,
        Some(158 | 202 | 241 | 307 | 396) => ClickHouseErrorCategory::ResourceExhausted,
        Some(394) => match owner.cause() {
            Some(QueryCause::Deadline) => ClickHouseErrorCategory::DeadlineExceeded,
            Some(
                QueryCause::Explicit
                | QueryCause::Disconnect
                | QueryCause::Shutdown
                | QueryCause::Abandoned,
            ) => ClickHouseErrorCategory::Cancelled,
            _ => ClickHouseErrorCategory::Backend,
        },
        _ => ClickHouseErrorCategory::Backend,
    }
}

fn cause_for_error(error: &ClickHouseError) -> QueryCause {
    match error.category() {
        ClickHouseErrorCategory::Cancelled => QueryCause::Explicit,
        ClickHouseErrorCategory::DeadlineExceeded => QueryCause::Deadline,
        ClickHouseErrorCategory::ResourceExhausted => QueryCause::ResourceExhausted,
        ClickHouseErrorCategory::Backend | ClickHouseErrorCategory::OwnershipViolation => {
            QueryCause::Backend
        }
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
            name: "031_events_content_authority.sql",
            sql: include_str!("../../../sql/031_events_content_authority.sql"),
        },
        Migration {
            version: "032",
            name: "032_drop_frozen_tool_io.sql",
            sql: include_str!("../../../sql/032_drop_frozen_tool_io.sql"),
        },
        Migration {
            version: "033",
            name: "033_replay_stable_events.sql",
            sql: include_str!("../../../sql/033_replay_stable_events.sql"),
        },
        Migration {
            version: "034",
            name: "034_ingest_progress.sql",
            sql: include_str!("../../../sql/034_ingest_progress.sql"),
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

    async fn owned<T>(
        client: &ClickHouseClient,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        let owner = QueryOwner::new(&client.runtime(), QueryWorkload::Internal)
            .expect("create test query owner");
        owner.scope(future).await
    }

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
                return (StatusCode::OK, "{\"value\":7}".to_string());
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
        fail_ledger_insert: bool,
    }

    async fn spawn_migration_mock_server(state: MigrationMockState) -> String {
        async fn handler(
            State(state): State<MigrationMockState>,
            Query(params): Query<HashMap<String, String>>,
        ) -> (StatusCode, String) {
            let query = params.get("query").cloned().unwrap_or_default();
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
    fn migration_031_freezes_and_replays_the_legacy_tool_source_safely() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "031")
            .expect("migration 031 must be registered");
        let sql = materialize_migration_sql(migration.sql, "other_db")
            .expect("materialize migration 031");
        let statements = split_sql_statements(&sql);
        let position = |needle: &str| {
            statements
                .iter()
                .position(|statement| statement.contains(needle))
                .unwrap_or_else(|| panic!("migration 031 must contain {needle}"))
        };

        let frozen = position(
            "CREATE TABLE IF NOT EXISTS other_db.tool_io_events_content_authority_031_frozen",
        );
        let staging = position(
            "CREATE TABLE IF NOT EXISTS other_db.tool_io\nAS other_db.tool_io_events_content_authority_031_frozen",
        );
        let exchange = position(
            "EXCHANGE TABLES other_db.tool_io\nAND other_db.tool_io_events_content_authority_031_frozen",
        );
        let canonical_tool_projection = [
            "event_uid",
            "tool_call_id",
            "parent_tool_call_id",
            "tool_name",
            "tool_phase",
            "tool_error",
            "input_json",
            "output_json",
            "output_text",
            "input_bytes",
            "output_bytes",
            "input_preview",
            "output_preview",
            "io_hash",
            "project_id",
            "repo_rel_path",
            "worktree_root",
            "source_ref",
            "event_version",
        ]
        .join(",\n      ");
        let frozen_fold = format!(
            "SELECT\n      {canonical_tool_projection}\n    \
             FROM other_db.tool_io_events_content_authority_031_frozen FINAL"
        );
        let live_fold =
            format!("SELECT\n      {canonical_tool_projection}\n    FROM other_db.tool_io FINAL");
        let fold = position(&frozen_fold);
        assert!(
            sql.contains(&live_fold),
            "both tool sources must use the same name-based projection"
        );
        assert!(!sql
            .contains("SELECT * FROM other_db.tool_io_events_content_authority_031_frozen FINAL"));
        assert!(!sql.contains("SELECT * FROM other_db.tool_io FINAL"));
        let drop_staging = position("DROP TABLE IF EXISTS other_db.tool_io");
        let truncate = position(
            "TRUNCATE TABLE IF EXISTS other_db.tool_io_events_content_authority_031_frozen",
        );

        assert!(
            frozen < staging
                && staging < exchange
                && exchange < fold
                && fold < drop_staging
                && drop_staging < truncate
        );
        assert!(sql.contains("UNION ALL\n    SELECT\n"));
        assert!(sql.contains("WHERE NOT JSONHas(e.payload_json, 'moraine_tool_io')"));
        assert!(sql.contains(r#"concat('{"source_payload":', toJSONString(e.payload_json), '}')"#));
        assert!(!sql.contains("toJSONString(tuple(e.payload_json AS source_payload))"));
        assert!(!sql.contains("RENAME TABLE IF EXISTS"));
        assert!(!sql
            .contains("DROP TABLE IF EXISTS other_db.tool_io_events_content_authority_031_frozen"));
        assert!(sql.contains("DROP TABLE IF EXISTS other_db.mcp_session_directory"));
        assert!(!sql.contains("CREATE TABLE IF NOT EXISTS other_db.mcp_session_directory"));
        assert!(!sql.contains("INSERT INTO other_db.mcp_session_directory"));
        assert!(!sql
            .contains("CREATE MATERIALIZED VIEW IF NOT EXISTS other_db.mv_mcp_session_directory"));
        assert!(!sql.contains("AggregatingMergeTree"));
    }

    #[test]
    fn migration_033_uses_restart_safe_atomic_cutovers() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "033")
            .expect("migration 033 must be registered");
        let sql = materialize_migration_sql(migration.sql, "other_db")
            .expect("materialize migration 033");
        let statements = split_sql_statements(&sql);
        let link_exchange = statements
            .iter()
            .position(|statement| {
                statement.contains(
                    "EXCHANGE TABLES\n  other_db.event_links AND other_db.event_links_replay_stable_033",
                )
            })
            .expect("link cutover must use EXCHANGE");
        let event_exchange = statements
            .iter()
            .position(|statement| {
                statement.contains(
                    "EXCHANGE TABLES\n  other_db.events AND other_db.events_replay_stable_033",
                )
            })
            .expect("event cutover must use EXCHANGE");
        let event_optimize = statements
            .iter()
            .position(|statement| {
                statement.starts_with("OPTIMIZE TABLE other_db.events_replay_stable_033 FINAL")
            })
            .expect("event replacement must converge before cutover");
        let link_optimize = statements
            .iter()
            .position(|statement| {
                statement.starts_with("OPTIMIZE TABLE other_db.event_links_replay_stable_033 FINAL")
            })
            .expect("link replacement must converge before cutover");
        let uid_lookup_drop = statements
            .iter()
            .position(|statement| statement == "DROP TABLE other_db.event_uid_lookup_033")
            .expect("UID lookup must be released before cutover");
        let final_link_drop = statements
            .iter()
            .rposition(|statement| {
                statement == "DROP TABLE IF EXISTS other_db.event_links_replay_stable_033"
            })
            .expect("old link table must be dropped after cutover");
        let final_event_drop = statements
            .iter()
            .rposition(|statement| {
                statement == "DROP TABLE IF EXISTS other_db.events_replay_stable_033"
            })
            .expect("old event table must be dropped after cutover");
        let uid_base_rewrites = statements
            .iter()
            .filter(|statement| statement.starts_with("INSERT INTO other_db.event_uid_base_033"))
            .collect::<Vec<_>>();
        let uid_map = statements
            .iter()
            .find(|statement| statement.starts_with("INSERT INTO other_db.event_uid_map_033"))
            .expect("UID map build must be registered");
        let uid_lookup = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE other_db.event_uid_lookup_033"))
            .expect("bounded UID lookup must be registered");
        let event_source = statements
            .iter()
            .find(|statement| {
                statement.starts_with("CREATE VIEW other_db.events_replay_source_033")
            })
            .expect("event rewrite source must be registered");
        let event_rewrites = statements
            .iter()
            .filter(|statement| {
                statement.starts_with("INSERT INTO other_db.events_replay_stable_033")
            })
            .collect::<Vec<_>>();
        let link_rewrite = statements
            .iter()
            .find(|statement| {
                statement.starts_with("INSERT INTO other_db.event_links_replay_stable_033")
            })
            .expect("link rewrite must be registered");
        let locator_rebuild = statements
            .iter()
            .find(|statement| statement.starts_with("INSERT INTO other_db.mcp_event_locator"))
            .expect("locator rebuild must be registered");
        let navigation_rebuild = statements
            .iter()
            .find(|statement| statement.starts_with("INSERT INTO other_db.mcp_event_navigation"))
            .expect("navigation rebuild must be registered");
        let search_rewrites = statements
            .iter()
            .filter(|statement| statement.starts_with("INSERT INTO other_db.search_postings"))
            .collect::<Vec<_>>();

        assert!(event_optimize < link_exchange);
        assert!(link_optimize < link_exchange);
        assert!(uid_lookup_drop < link_exchange);
        assert!(link_exchange < event_exchange);
        assert!(event_exchange < final_link_drop);
        assert!(event_exchange < final_event_drop);
        assert_eq!(
            statements
                .iter()
                .filter(|statement| statement.starts_with("EXCHANGE TABLES"))
                .count(),
            2
        );
        assert!(!sql.contains("RENAME TABLE"));
        assert!(!sql.contains("_frozen"));
        assert!(!uid_map.contains("GROUP BY"));
        assert!(uid_lookup.contains("ENGINE = Join(ANY, LEFT, old_event_uid)"));
        assert_eq!(uid_base_rewrites.len(), 8);
        assert!(sql.contains("FROM other_db.events FINAL"));
        for (bucket, rewrite) in uid_base_rewrites.iter().enumerate() {
            assert!(rewrite.contains(&format!("WHERE source_bucket = {bucket}")));
            assert!(rewrite.contains("max_memory_usage = 1073741824"));
        }
        assert!(event_source.contains("FROM other_db.events AS e FINAL"));
        assert!(event_source
            .contains("joinGet('other_db.event_uid_lookup_033', 'new_event_uid', e.event_uid)"));
        assert!(link_rewrite
            .contains("joinGet('other_db.event_uid_lookup_033', 'new_event_uid', l.event_uid)"));
        assert_eq!(event_rewrites.len(), 8);
        assert!(!locator_rebuild.contains("events FINAL"));
        assert!(!navigation_rebuild.contains("events FINAL"));
        assert_eq!(search_rewrites.len(), 16);
        for (bucket, rewrite) in search_rewrites.iter().enumerate() {
            assert!(rewrite.contains(&format!(
                "AND intDiv(cityHash64(d.session_id) % 64, 4) = {bucket}"
            )));
            assert!(rewrite.contains("max_bytes_before_external_group_by = 67108864"));
            assert!(rewrite.contains("max_memory_usage = 1073741824"));
        }
        for (bucket, rewrite) in event_rewrites.iter().enumerate() {
            assert!(rewrite.contains(&format!(
                "WHERE intDiv(cityHash64(session_id) % 64, 8) = {bucket}"
            )));
            for setting in [
                "max_block_size = 1024",
                "preferred_max_column_in_block_size_bytes = 33554432",
                "min_insert_block_size_rows = 0",
                "min_insert_block_size_bytes = 0",
                "max_insert_threads = 1",
                "max_threads = 1",
                "max_memory_usage = 1073741824",
            ] {
                assert!(rewrite.contains(setting));
            }
        }
        for setting in [
            "max_block_size = 1024",
            "preferred_max_column_in_block_size_bytes = 33554432",
            "max_threads = 1",
        ] {
            assert!(
                statements
                    .iter()
                    .filter(|statement| statement.contains(setting))
                    .count()
                    >= 6,
                "every bulk migration insert must bound payload-heavy reads with {setting}"
            );
        }
        assert!(
            statements
                .iter()
                .filter(|statement| statement.contains("max_memory_usage = 1073741824"))
                .count()
                >= 6,
            "every bulk migration insert must have a fixed memory budget"
        );
    }

    #[test]
    fn migration_031_uses_the_same_narrow_path_projection_for_live_and_backfill() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "031")
            .expect("migration 031 must be registered");
        let sql = materialize_migration_sql(migration.sql, "other_db")
            .expect("materialize migration 031");
        let statements = split_sql_statements(&sql);
        let live = statements
            .iter()
            .find(|statement| {
                statement.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS other_db.mv_mcp_event_locator_from_events")
            })
            .expect("live locator projection");
        let backfill = statements
            .iter()
            .find(|statement| statement.starts_with("INSERT INTO other_db.mcp_event_locator"))
            .expect("locator backfill");
        fn path_expression(statement: &str) -> &str {
            let start = statement
                .find("arrayFilter(path ->")
                .expect("path expression start");
            let remainder = &statement[start..];
            let end = remainder.find("\n  toUInt8(").expect("path expression end");
            let expression = remainder[..end].trim_end_matches(',');
            expression
                .strip_suffix(" AS path_tokens")
                .unwrap_or(expression)
        }

        let live_path_expression = path_expression(live);
        let backfill_path_expression = path_expression(backfill);
        assert_eq!(live_path_expression, backfill_path_expression);
        assert!(live_path_expression.contains("file_path|notebook_path|path|target_file"));
        assert!(live_path_expression.contains("JSONExtractString(tool_input, 'command')"));
        assert!(live_path_expression.contains("JSONExtractString(tool_input, 'cmd')"));
        assert_eq!(live_path_expression.matches("extractAll(").count(), 2);
        assert!(!live_path_expression.contains("[A-Za-z0-9_./-]+"));
        assert!(!live_path_expression.contains("\n      '\"((?:[^\""));
    }

    #[test]
    fn migration_032_only_drops_the_empty_frozen_tool_source() {
        let migration = bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "032")
            .expect("migration 032 must be registered");
        let sql = materialize_migration_sql(migration.sql, "other_db")
            .expect("materialize migration 032");

        assert_eq!(
            split_sql_statements(&sql),
            vec![
                "DROP TABLE IF EXISTS other_db.tool_io_events_content_authority_031_frozen"
                    .to_string()
            ]
        );
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
        for version in ["027", "029", "030"] {
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

        owned(
            &client,
            client.request_text("SELECT 1", None, None, false, None),
        )
        .await
        .expect("first attributed request");
        owned(
            &client,
            client.request_text("SELECT 1", None, None, false, None),
        )
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

        owned(
            &client,
            client.request_text("SELECT 1", None, None, false, None),
        )
        .await
        .expect("request from compatibility client");

        let expected = format!(
            "{DEFAULT_USER_AGENT_ROLE}/{} (pid={})",
            moraine_config::BUILD_VERSION,
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

        owned(
            &client,
            client.request_text_with_params(
                "INSERT INTO tool_io FORMAT JSONEachRow",
                Some(payload.clone()),
                Some("moraine_team"),
                true,
                Some("JSONEachRow"),
                &[("readonly", "1")],
            ),
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
        let query_id = request.params.get("query_id").expect("transport query id");
        assert!(query_id.starts_with("moraine-internal-"));
        assert_eq!(
            request.params.get("readonly").map(String::as_str),
            Some("1")
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

        owned(
            &client,
            client.request_text("SELECT 1", Some(payload.clone()), None, false, None),
        )
        .await
        .expect("plain request");

        let requests = requests.lock().expect("request capture mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].headers.get("content-encoding").is_none());
        assert_eq!(requests[0].body, payload);
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

        owned(
            &client,
            client.request_text("SELECT 1", None, None, false, None),
        )
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

        let rows: Vec<Row> = owned(&client, client.query_rows("SELECT 7 AS value", None))
            .await
            .expect("fallback query_rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 7);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_json_rows_chunks_large_payloads() {
        let lengths = Arc::new(Mutex::new(Vec::<usize>::new()));
        let base_url = spawn_insert_capture_server(lengths.clone()).await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let large_value = "x".repeat((MAX_INSERT_PAYLOAD_BYTES / 2).saturating_add(1024));

        owned(
            &client,
            client.insert_json_rows(
                "raw_events",
                &[
                    json!({ "payload": large_value }),
                    json!({ "payload": large_value }),
                ],
            ),
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

        let mut stream = owned(
            &client,
            client.request_stream_with_params(
                "SELECT value FROM events FORMAT JSONEachRow",
                Some("moraine"),
                None,
                &[("readonly", "1")],
                None,
            ),
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
        assert!(params[0]
            .get("query_id")
            .is_some_and(|id| id.starts_with("moraine-internal-")));
        assert_eq!(params[0].get("readonly").map(String::as_str), Some("1"));
        assert!(!params[0].contains_key("max_execution_time"));

        let content_lengths = content_lengths
            .lock()
            .expect("stream headers mutex poisoned");
        assert_eq!(content_lengths.as_slice(), &[Some("0".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_stream_with_params_includes_status_and_body_on_http_failure() {
        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = owned(
            &client,
            client.request_stream_with_params("SELECT FAIL", None, None, &[], None),
        )
        .await
        .expect_err("expected HTTP failure");

        let typed = err
            .downcast_ref::<ClickHouseError>()
            .expect("typed ClickHouse error");
        assert_eq!(typed.category(), ClickHouseErrorCategory::Backend);
        assert_eq!(
            typed.status(),
            Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert!(!err.to_string().contains("SELECT FAIL"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_progress_reports_current_schema_without_work() {
        let applied = bundled_migrations()
            .into_iter()
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>();
        let base_url = spawn_migration_mock_server(MigrationMockState {
            applied: Arc::new(applied),
            queries: Arc::new(Mutex::new(Vec::new())),
            fail_ledger_insert: false,
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let mut events = Vec::new();

        let executed = owned(
            &client,
            client.run_migrations_with_progress(|event| events.push(event)),
        )
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
    async fn migration_progress_applies_post_v071_migrations() {
        let bundled = bundled_migrations();
        let pending = bundled
            .iter()
            .filter(|migration| matches!(migration.version, "031" | "032" | "033" | "034"))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            pending
                .iter()
                .map(|migration| migration.version)
                .collect::<Vec<_>>(),
            vec!["031", "032", "033", "034"]
        );
        let applied = bundled
            .iter()
            .filter(|migration| !matches!(migration.version, "031" | "032" | "033" | "034"))
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>();
        assert_eq!(applied.last().map(String::as_str), Some("030"));
        let queries = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_migration_mock_server(MigrationMockState {
            applied: Arc::new(applied),
            queries: queries.clone(),
            fail_ledger_insert: false,
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let mut events = Vec::new();

        let executed = owned(
            &client,
            client.run_migrations_with_progress(|event| events.push(event)),
        )
        .await
        .expect("apply post-v0.7.1 migrations");

        assert_eq!(executed, vec!["031", "032", "033", "034"]);
        let mut expected_events = vec![MigrationProgress::Plan {
            applied: bundled.len() - pending.len(),
            pending: pending.len(),
        }];
        for (index, migration) in pending.iter().enumerate() {
            expected_events.push(MigrationProgress::Started {
                index: index + 1,
                total: pending.len(),
                version: migration.version,
                name: migration.name,
            });
            expected_events.push(MigrationProgress::Applied {
                index: index + 1,
                total: pending.len(),
                version: migration.version,
                name: migration.name,
            });
        }
        assert_eq!(events, expected_events);
        let queries = queries.lock().expect("migration query mutex poisoned");
        let ledger_indices = queries
            .iter()
            .enumerate()
            .filter_map(|(index, query)| {
                (query.starts_with("INSERT INTO") && query.contains("schema_migrations"))
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(ledger_indices.len(), pending.len());
        assert_eq!(ledger_indices.last().copied(), Some(queries.len() - 1));
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
            applied: Arc::new(applied),
            queries: Arc::new(Mutex::new(Vec::new())),
            fail_ledger_insert: true,
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");
        let mut events = Vec::new();

        let error = owned(
            &client,
            client.run_migrations_with_progress(|event| events.push(event)),
        )
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

        let skew = owned(&client, client.schema_skew())
            .await
            .expect("skew probe");
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

        let skew = owned(&client, client.schema_skew())
            .await
            .expect("skew probe");
        assert_eq!(skew.missing_on_server.len(), bundled_migrations().len());
        assert!(skew.unknown_on_server.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_text_includes_status_and_body_on_http_failure() {
        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = owned(
            &client,
            client.request_text("SELECT FAIL", None, None, false, None),
        )
        .await
        .expect_err("expected HTTP failure");

        let typed = err
            .downcast_ref::<ClickHouseError>()
            .expect("typed ClickHouse error");
        assert_eq!(typed.category(), ClickHouseErrorCategory::Backend);
        assert_eq!(
            typed.status(),
            Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert!(!err.to_string().contains("SELECT FAIL"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_text_propagates_response_body_read_errors() {
        let base_url = spawn_truncated_body_server();
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = owned(
            &client,
            client.request_text("SELECT 1", None, None, false, None),
        )
        .await
        .expect_err("expected response body read failure");

        let msg = err.to_string();
        assert!(msg.contains("failed to read ClickHouse response body"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unowned_egress_is_typed_and_sends_zero_bytes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind raw byte counter");
        let config = test_clickhouse_config(format!("http://{}", listener.local_addr().unwrap()));
        let client = ClickHouseClient::new(config).expect("client");

        let error = client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect_err("unowned buffered request must fail");
        assert_eq!(
            error
                .downcast_ref::<ClickHouseError>()
                .map(ClickHouseError::category),
            Some(ClickHouseErrorCategory::OwnershipViolation)
        );
        let error = client
            .request_stream_with_params("SELECT 1", None, None, &[], None)
            .await
            .expect_err("unowned stream request must fail");
        assert_eq!(
            error
                .downcast_ref::<ClickHouseError>()
                .map(ClickHouseError::category),
            Some(ClickHouseErrorCategory::OwnershipViolation)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "missing ownership must be rejected before TCP connect or bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn base_url_and_request_params_reserve_owner_identity_and_deadline_keys() {
        for key in RESERVED_PARAMS {
            let encoded_key = key.to_ascii_uppercase();
            let config = test_clickhouse_config(format!(
                "http://127.0.0.1:8123/?{}=x",
                url::form_urlencoded::byte_serialize(encoded_key.as_bytes()).collect::<String>()
            ));
            let error = ClickHouseClient::new(config)
                .err()
                .expect("reserved URL key must fail");
            assert!(error.to_string().contains("reserved parameter"));
        }

        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("client");
        for key in RESERVED_PARAMS {
            let owned_key = key.to_ascii_uppercase();
            let owner = QueryOwner::new(&client.runtime(), QueryWorkload::Internal).unwrap();
            let error = owner
                .scope(client.request_text_with_params(
                    "SELECT 1",
                    None,
                    None,
                    false,
                    None,
                    &[(owned_key.as_str(), "1")],
                ))
                .await
                .expect_err("reserved request key must fail");
            assert_eq!(
                error
                    .downcast_ref::<ClickHouseError>()
                    .map(ClickHouseError::category),
                Some(ClickHouseErrorCategory::OwnershipViolation)
            );
        }
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn owner_and_child_ids_are_classed_unique_and_runtime_isolated() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let runtime = QueryRuntime::new();
        let client = ClickHouseClient::new_with_runtime(
            test_clickhouse_config(base_url.clone()),
            runtime.clone(),
        )
        .unwrap();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Mcp).unwrap();
        let logical = owner.logical_id().to_string();
        owner
            .scope(async {
                let (left, right) = tokio::join!(
                    client.request_text("SELECT 1", None, None, false, None),
                    client.request_text("SELECT 2", None, None, false, None),
                );
                left.unwrap();
                right.unwrap();
            })
            .await;
        assert_eq!(runtime.active_owner_count(), 0);

        {
            let captured = requests.lock().unwrap();
            let mut ids = captured
                .iter()
                .map(|request| request.params["query_id"].clone())
                .collect::<Vec<_>>();
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), 2);
            assert!(ids.iter().all(|id| id.starts_with(&format!("{logical}-"))));
        }

        let other_runtime = QueryRuntime::new();
        let other = QueryOwner::new(&other_runtime, QueryWorkload::Mcp).unwrap();
        assert_ne!(logical, other.logical_id());
        other.scope(async {}).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn returned_stream_keeps_ticket_until_eof_and_normal_eof_never_kills() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let runtime = QueryRuntime::new();
        let client =
            ClickHouseClient::new_with_runtime(test_clickhouse_config(base_url), runtime.clone())
                .unwrap();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Export).unwrap();
        let mut stream = owner
            .scope(client.request_stream_with_params("SELECT 1", None, None, &[], None))
            .await
            .unwrap();
        assert_eq!(
            runtime.active_owner_count(),
            1,
            "stream ticket must outlive scope"
        );
        while stream.next_chunk().await.unwrap().is_some() {}
        assert_eq!(runtime.active_owner_count(), 0);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(!captured[0].params["query"].starts_with("KILL QUERY"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_stream_uses_exact_child_kill_on_administrative_path() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let base_url = spawn_request_capture_server(RequestCaptureState {
            requests: requests.clone(),
        })
        .await;
        let runtime = QueryRuntime::new();
        let client =
            ClickHouseClient::new_with_runtime(test_clickhouse_config(base_url), runtime.clone())
                .unwrap();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Export).unwrap();
        let stream = owner
            .scope(client.request_stream_with_params("SELECT 1", None, None, &[], None))
            .await
            .unwrap();
        let child_id = requests.lock().unwrap()[0].params["query_id"].clone();
        drop(stream);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|request| request.params["query"].starts_with("KILL QUERY"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("exact KILL should be attempted");
        let captured = requests.lock().unwrap();
        let kill = captured
            .iter()
            .find(|request| request.params["query"].starts_with("KILL QUERY"))
            .unwrap();
        assert!(kill.params["query"].contains(&format!("query_id IN ('{child_id}')")));
        assert!(!kill.params["query"].contains("LIKE"));
        assert!(kill.params["query_id"].starts_with("moraine-administrative-"));
        assert_eq!(kill.params["replace_running_query"], "0");
    }

    #[test]
    fn deadline_encoding_ceil_never_shortens() {
        assert_eq!(
            format_clickhouse_deadline(Duration::from_nanos(1)),
            "0.000001"
        );
        assert_eq!(
            format_clickhouse_deadline(Duration::from_micros(1)),
            "0.000001"
        );
        assert_eq!(
            format_clickhouse_deadline(Duration::from_nanos(1_000_001)),
            "0.001001"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_absolute_deadline_sends_zero_bytes_and_is_typed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = ClickHouseClient::new(test_clickhouse_config(format!(
            "http://{}",
            listener.local_addr().unwrap()
        )))
        .unwrap();
        let owner = QueryOwner::with_deadline(
            &client.runtime(),
            QueryWorkload::Internal,
            tokio::time::Instant::now(),
        )
        .unwrap();
        let error = owner
            .scope(client.request_text("SELECT 1", None, None, false, None))
            .await
            .expect_err("expired deadline");
        assert_eq!(
            error
                .downcast_ref::<ClickHouseError>()
                .map(ClickHouseError::category),
            Some(ClickHouseErrorCategory::DeadlineExceeded)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deadline_injection_uses_one_absolute_deadline_and_connect_timeout_only() {
        async fn delayed(Query(params): Query<HashMap<String, String>>) -> (StatusCode, String) {
            tokio::time::sleep(Duration::from_millis(75)).await;
            (StatusCode::OK, params["query_id"].clone())
        }
        let app = Router::new().route("/", post(delayed));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let mut config = test_clickhouse_config(format!("http://{addr}"));
        config.timeout_seconds = 0.001;
        let client = ClickHouseClient::new(config).unwrap();
        let owner = QueryOwner::with_deadline(
            &client.runtime(),
            QueryWorkload::Internal,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let result = owner
            .scope(client.request_text("SELECT 1", None, None, false, None))
            .await;
        assert!(
            result.is_ok(),
            "tiny connect timeout must not become total timeout: {result:?}"
        );
    }

    #[test]
    fn exception_codes_map_to_exact_categories() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let domain = QueryRuntime::new();
            let owner = QueryOwner::new(&domain, QueryWorkload::Internal).unwrap();
            for code in [159, 160, 209] {
                assert_eq!(
                    classify_code(&owner, Some(code)),
                    ClickHouseErrorCategory::DeadlineExceeded
                );
            }
            for code in [158, 202, 241, 307, 396] {
                assert_eq!(
                    classify_code(&owner, Some(code)),
                    ClickHouseErrorCategory::ResourceExhausted
                );
            }
            assert_eq!(
                classify_code(&owner, Some(394)),
                ClickHouseErrorCategory::Backend
            );
            assert_eq!(
                classify_code(&owner, Some(999)),
                ClickHouseErrorCategory::Backend
            );
            owner.scope(async {}).await;
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_json_envelope_is_not_replayed() {
        async fn malformed(
            State(requests): State<Arc<Mutex<Vec<()>>>>,
        ) -> (StatusCode, &'static str) {
            requests.lock().unwrap().push(());
            (StatusCode::OK, "not-json")
        }
        let counts = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/", post(malformed))
            .with_state(counts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let client =
            ClickHouseClient::new(test_clickhouse_config(format!("http://{addr}"))).unwrap();
        let result: Result<Vec<Value>> = owned(&client, client.query_rows("SELECT 1", None)).await;
        assert!(result.is_err());
        assert_eq!(
            counts.lock().unwrap().len(),
            1,
            "unsafe fallback replayed request"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_status_with_late_exception_is_typed_without_retry() {
        async fn late_exception() -> (StatusCode, &'static str) {
            (
                StatusCode::OK,
                "{\"value\":1}\nCode: 241. DB::Exception: memory limit exceeded\n",
            )
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().route("/", post(late_exception))).await;
        });
        let client =
            ClickHouseClient::new(test_clickhouse_config(format!("http://{addr}"))).unwrap();
        let error = owned(
            &client,
            client.request_text("SELECT 1", None, None, false, None),
        )
        .await
        .expect_err("late exception must fail");
        let typed = error.downcast_ref::<ClickHouseError>().unwrap();
        assert_eq!(typed.category(), ClickHouseErrorCategory::ResourceExhausted);
        assert_eq!(typed.exception_code(), Some(241));
        assert_eq!(
            typed.exception_detail(),
            Some("Code: 241. DB::Exception: memory limit exceeded")
        );
        assert!(error.to_string().contains("memory limit exceeded"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn typed_code_117_retains_bounded_detail_for_oversized_row_classification() {
        async fn oversized() -> (StatusCode, &'static str) {
            (
                StatusCode::BAD_REQUEST,
                "Code: 117. DB::Exception: Size of JSON object at position 42 is extremely large. Expected not greater than 10485760 bytes.\nstack trace omitted",
            )
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().route("/", post(oversized))).await;
        });
        let client =
            ClickHouseClient::new(test_clickhouse_config(format!("http://{addr}"))).unwrap();
        let error = owned(
            &client,
            client.request_text("INSERT INTO events", None, None, false, None),
        )
        .await
        .expect_err("oversized response must fail");
        let typed = error.downcast_ref::<ClickHouseError>().unwrap();
        assert_eq!(typed.exception_code(), Some(117));
        assert!(typed
            .exception_detail()
            .unwrap()
            .contains("extremely large"));
        assert!(!typed.exception_detail().unwrap().contains("stack trace"));
        assert!(is_oversized_json_each_row_insert_error(&error));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn explicit_cancel_interrupts_response_body_and_cleans_exact_child() {
        use axum::body::Body;
        use axum::response::Response;
        use futures_util::{stream, StreamExt};
        use std::convert::Infallible;

        #[derive(Clone)]
        struct StateData {
            started: Arc<tokio::sync::Notify>,
            requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
            process_polls: Arc<std::sync::atomic::AtomicUsize>,
        }
        async fn handler(
            State(state): State<StateData>,
            Query(params): Query<HashMap<String, String>>,
        ) -> Response<Body> {
            let query = params.get("query").cloned().unwrap_or_default();
            state.requests.lock().unwrap().push(params);
            if query.starts_with("KILL QUERY") {
                return Response::new(Body::from(""));
            }
            if query.contains("system.processes") {
                let poll = state
                    .process_polls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let id = query
                    .split("IN ('")
                    .nth(1)
                    .and_then(|tail| tail.split("')").next())
                    .unwrap();
                return Response::new(Body::from(if poll == 0 {
                    format!(r#"{{"query_id":"{id}"}}"#)
                } else {
                    String::new()
                }));
            }
            state.started.notify_one();
            let body =
                stream::once(async { Ok::<Bytes, Infallible>(Bytes::from_static(b"partial")) })
                    .chain(stream::pending());
            Response::new(Body::from_stream(body))
        }

        let state = StateData {
            started: Arc::new(tokio::sync::Notify::new()),
            requests: Arc::new(Mutex::new(Vec::new())),
            process_polls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", post(handler))
            .with_state(state.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let runtime = QueryRuntime::new();
        let client = ClickHouseClient::new_with_runtime(
            test_clickhouse_config(format!("http://{addr}")),
            runtime.clone(),
        )
        .unwrap();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Mcp).unwrap();
        let request_owner = owner.clone();
        let task = tokio::spawn(async move {
            request_owner
                .scope(client.request_text("SELECT 1", None, None, false, None))
                .await
        });
        state.started.notified().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        owner.cancel(QueryCause::Explicit);
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancel must promptly interrupt body")
            .unwrap()
            .expect_err("cancelled request");
        assert_eq!(
            error
                .downcast_ref::<ClickHouseError>()
                .map(ClickHouseError::category),
            Some(ClickHouseErrorCategory::Cancelled)
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime.active_owner_count() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cleanup should become terminal");
        let requests = state.requests.lock().unwrap();
        let child = requests[0]["query_id"].clone();
        assert!(requests.iter().any(|request| {
            request["query"].starts_with("KILL QUERY")
                && request["query"].contains(&format!("'{child}'"))
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_public_workload_uses_the_fixed_id_vocabulary() {
        let runtime = QueryRuntime::new();
        for workload in [
            QueryWorkload::Mcp,
            QueryWorkload::Monitor,
            QueryWorkload::Internal,
            QueryWorkload::Export,
            QueryWorkload::Migration,
            QueryWorkload::Background,
            QueryWorkload::Administrative,
        ] {
            let owner = QueryOwner::new(&runtime, workload).unwrap();
            let id = owner.logical_id();
            assert!(id.starts_with(&format!("moraine-{}-", workload.as_str())));
            let uuid = id.rsplit('-').next().unwrap();
            assert_eq!(uuid.len(), 32);
            assert!(uuid.bytes().all(|byte| byte.is_ascii_hexdigit()));
            owner.scope(async {}).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn administrative_owner_cancels_through_non_recursive_internal_admin_path() {
        #[derive(Clone)]
        struct LateVisibilityState {
            requests: Arc<Mutex<Vec<CapturedRequest>>>,
            polls: Arc<std::sync::atomic::AtomicUsize>,
        }
        async fn handler(
            State(state): State<LateVisibilityState>,
            Query(params): Query<HashMap<String, String>>,
            headers: HeaderMap,
            body: Bytes,
        ) -> (StatusCode, String) {
            let query = params.get("query").cloned().unwrap_or_default();
            state.requests.lock().unwrap().push(CapturedRequest {
                params,
                headers,
                body: body.to_vec(),
            });
            if query.contains("system.processes") {
                let poll = state
                    .polls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // First absent poll races visibility despite response headers;
                // then the child appears, and finally disappears after KILL.
                let id = query
                    .split("IN ('")
                    .nth(1)
                    .and_then(|tail| tail.split("')").next())
                    .unwrap();
                return (
                    StatusCode::OK,
                    if poll == 1 {
                        format!(r#"{{"query_id":"{id}"}}"#)
                    } else {
                        String::new()
                    },
                );
            }
            (StatusCode::OK, "ok".to_string())
        }

        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = LateVisibilityState {
            requests: requests.clone(),
            polls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/", post(handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let runtime = QueryRuntime::new();
        let client =
            ClickHouseClient::new_with_runtime(test_clickhouse_config(base_url), runtime.clone())
                .unwrap();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Administrative).unwrap();
        let mut stream = owner
            .scope(client.request_stream_with_params(
                "SELECT version()",
                Some("system"),
                None,
                &[],
                None,
            ))
            .await
            .unwrap();
        let child_id = requests.lock().unwrap()[0].params["query_id"].clone();
        assert!(child_id.starts_with("moraine-administrative-"));

        // Read one chunk but abandon before EOF so the normal Administrative
        // statement remains armed and exercises cleanup after headers.
        assert!(stream.next_chunk().await.unwrap().is_some());
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime.active_owner_count() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("late-visible administrative owner cleanup should terminate");

        let captured = requests.lock().unwrap();
        let kills = captured
            .iter()
            .filter(|request| request.params["query"].starts_with("KILL QUERY"))
            .collect::<Vec<_>>();
        let polls = captured
            .iter()
            .filter(|request| request.params["query"].contains("system.processes"))
            .count();
        assert_eq!(
            polls, 3,
            "absent, late-visible, then absent must all be observed"
        );
        assert_eq!(kills.len(), polls);
        assert!(
            kills.iter().all(|kill| {
                kill.params["query"].contains(&format!("query_id IN ('{child_id}')"))
                    && kill.params["query_id"].starts_with("moraine-administrative-")
                    && kill.params["query_id"] != child_id
            }),
            "internal KILL must not recursively own or target itself"
        );
        assert!(
            captured
                .iter()
                .filter(|request| {
                    request.params["query"].starts_with("KILL QUERY")
                        || request.params["query"].contains("system.processes")
                })
                .all(|request| request.headers.get("content-length").unwrap() == "0"),
            "empty administrative POSTs must carry an explicit zero content length"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn still_visible_cleanup_is_bounded_even_when_admin_requests_succeed() {
        #[derive(Clone)]
        struct VisibleState {
            kills: Arc<std::sync::atomic::AtomicUsize>,
        }
        async fn handler(
            State(state): State<VisibleState>,
            Query(params): Query<HashMap<String, String>>,
        ) -> (StatusCode, String) {
            let query = params.get("query").cloned().unwrap_or_default();
            if query.starts_with("KILL QUERY") {
                state
                    .kills
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (StatusCode::OK, String::new());
            }
            if query.contains("system.processes") {
                let id = query
                    .split("IN ('")
                    .nth(1)
                    .and_then(|tail| tail.split("')").next())
                    .unwrap();
                return (StatusCode::OK, format!(r#"{{"query_id":"{id}"}}"#));
            }
            (StatusCode::OK, "ok".to_string())
        }

        let state = VisibleState {
            kills: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/", post(handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let runtime = QueryRuntime::new();
        let client =
            ClickHouseClient::new_with_runtime(test_clickhouse_config(base_url), runtime.clone())
                .unwrap();
        let owner = QueryOwner::new(&runtime, QueryWorkload::Administrative).unwrap();
        let mut stream = owner
            .scope(client.request_stream_with_params("SELECT 1", None, None, &[], None))
            .await
            .unwrap();
        assert!(stream.next_chunk().await.unwrap().is_some());
        let started = tokio::time::Instant::now();
        drop(stream);
        while runtime.active_owner_count() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let elapsed = tokio::time::Instant::now() - started;
        assert!(elapsed >= QUERY_CLEANUP_GRACE);
        assert!(elapsed <= QUERY_CLEANUP_GRACE + Duration::from_millis(20));
        assert!(state.kills.load(std::sync::atomic::Ordering::SeqCst) > 1);
    }
}
