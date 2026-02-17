use anyhow::{anyhow, bail, Context, Result};
use moraine_config::ClickHouseConfig;
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Client, Url,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    http: Client,
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
        let timeout = Duration::from_secs_f64(cfg.timeout_seconds.max(1.0));
        let http = Client::builder()
            .timeout(timeout)
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

    pub async fn request_text(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
    ) -> Result<String> {
        let mut url = self.base_url()?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", query);
            if let Some(database) = database {
                qp.append_pair("database", database);
            }
            if let Some(default_format) = default_format {
                qp.append_pair("default_format", default_format);
            }
            if async_insert && self.cfg.async_insert {
                qp.append_pair("async_insert", "1");
                if self.cfg.wait_for_async_insert {
                    qp.append_pair("wait_for_async_insert", "1");
                }
            }
        }

        // ClickHouse HTTP treats GET as readonly, so use POST for both reads and writes.
        let payload = body.unwrap_or_default();
        let payload_len = payload.len();

        let mut req = self
            .http
            .post(url)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            // Some ClickHouse builds require an explicit Content-Length on POST.
            .header(CONTENT_LENGTH, payload_len)
            .body(payload);

        if !self.cfg.username.is_empty() {
            req = req.basic_auth(self.cfg.username.clone(), Some(self.cfg.password.clone()));
        }

        let response = req.send().await.context("clickhouse request failed")?;
        let status = response.status();
        let text = response.text().await.with_context(|| {
            format!(
                "failed to read clickhouse response body (status {})",
                status
            )
        })?;

        if !status.is_success() {
            return Err(anyhow!("clickhouse returned {}: {}", status, text));
        }

        Ok(text)
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
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text(query, None, database, false, None)
            .await?;
        let mut rows = Vec::new();

        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row = serde_json::from_str::<T>(line)
                .with_context(|| format!("failed to parse JSONEachRow line: {}", line))?;
            rows.push(row);
        }

        Ok(rows)
    }

    pub async fn query_json_data<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text(query, None, database, false, Some("JSON"))
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
        match self.query_json_data(query, database).await {
            Ok(rows) => Ok(rows),
            Err(_) => self.query_json_each_row(query, database).await,
        }
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::<u8>::new();
        for row in rows {
            let line = serde_json::to_vec(row).context("failed to encode JSON row")?;
            payload.extend_from_slice(&line);
            payload.push(b'\n');
        }

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_identifier(table)
        );
        self.request_text(&query, Some(payload), None, true, None)
            .await?;
        Ok(())
    }

    pub async fn run_migrations(&self) -> Result<Vec<String>> {
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

        let mut executed = Vec::new();
        for migration in bundled_migrations() {
            if applied.contains(migration.version) {
                continue;
            }

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
            "search_term_stats",
            "search_corpus_stats",
            "search_query_log",
            "search_hit_log",
            "search_interaction_log",
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
    ]
}

fn truncate_for_error(statement: &str) -> String {
    const LIMIT: usize = 240;
    let compact = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= LIMIT {
        compact
    } else {
        format!("{}...", &compact[..LIMIT])
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::Query,
        http::{HeaderMap, StatusCode},
        routing::get,
        Router,
    };
    use moraine_config::ClickHouseConfig;
    use serde::Deserialize;
    use std::collections::HashMap;

    fn test_clickhouse_config(url: String) -> ClickHouseConfig {
        ClickHouseConfig {
            url,
            database: "moraine".to_string(),
            username: "default".to_string(),
            password: String::new(),
            timeout_seconds: 5.0,
            async_insert: true,
            wait_for_async_insert: true,
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
