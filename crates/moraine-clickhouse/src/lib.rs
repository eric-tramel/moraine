use anyhow::{anyhow, bail, Context, Result};
use flate2::{write::GzEncoder, Compression};
use moraine_config::{ClickHouseConfig, ClickHouseRequestCompression};
use reqwest::{
    header::{HeaderValue, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, USER_AGENT},
    Client, RequestBuilder, Url,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::io::Write;

mod mcp_open_projection;

const MAX_INSERT_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
use std::collections::{BTreeSet, HashSet};
use std::time::Duration;
const DEFAULT_USER_AGENT_ROLE: &str = "moraine-clickhouse";

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    http: Client,
}

#[derive(Debug)]
pub struct ClickHouseByteStream {
    response: reqwest::Response,
}

impl ClickHouseByteStream {
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        let chunk = self
            .response
            .chunk()
            .await
            .context("failed to read clickhouse response chunk")?;
        Ok(chunk.map(|bytes| bytes.to_vec()))
    }
}

struct ClickHouseRequestOptions<'a> {
    database: Option<&'a str>,
    async_insert: bool,
    default_format: Option<&'a str>,
    params: &'a [(&'a str, &'a str)],
    request_timeout: Option<Duration>,
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
        body: Vec<u8>,
        options: ClickHouseRequestOptions<'_>,
    ) -> Result<RequestBuilder> {
        let mut url = self.base_url()?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", query);
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

        if let Some(timeout) = options.request_timeout {
            req = req.timeout(timeout);
        }

        if !self.cfg.username.is_empty() {
            req = req.basic_auth(self.cfg.username.clone(), Some(self.cfg.password.clone()));
        }

        Ok(req)
    }

    async fn send_checked_response(&self, req: RequestBuilder) -> Result<reqwest::Response> {
        let response = req.send().await.context("clickhouse request failed")?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.with_context(|| {
                format!(
                    "failed to read clickhouse response body (status {})",
                    status
                )
            })?;
            return Err(anyhow!("clickhouse returned {}: {}", status, text));
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
        let req = self
            .request_builder(
                query,
                body.unwrap_or_default(),
                ClickHouseRequestOptions {
                    database,
                    async_insert,
                    default_format,
                    params,
                    request_timeout: None,
                },
            )
            .await?;
        let response = self.send_checked_response(req).await?;
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
        let req = self
            .request_builder(
                query,
                Vec::new(),
                ClickHouseRequestOptions {
                    database,
                    async_insert: false,
                    default_format,
                    params,
                    request_timeout,
                },
            )
            .await?;
        let response = self.send_checked_response(req).await?;

        Ok(ClickHouseByteStream { response })
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
        serde_json::Deserializer::from_str(&raw)
            .into_iter::<T>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to parse JSONEachRow response")
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
        let envelope: ClickHouseEnvelope<T> = serde_json::from_str(&raw)
            .with_context(|| format!("invalid clickhouse JSON response: {}", raw))?;
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
            Err(_) => {
                self.query_json_each_row_with_params(query, database, params)
                    .await
            }
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
                self.request_text(&statement, None, Some(&self.cfg.database), false, None)
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
            self.request_text(&log_stmt, None, Some(&self.cfg.database), false, None)
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
        let base_url = spawn_migration_mock_server(MigrationMockState {
            applied: Arc::new(applied),
            queries: queries.clone(),
            fail_ledger_insert: false,
        })
        .await;
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
            applied: Arc::new(applied),
            queries: Arc::new(Mutex::new(Vec::new())),
            fail_ledger_insert: true,
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
}
