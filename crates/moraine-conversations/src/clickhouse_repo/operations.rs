use super::*;
use crate::domain::{
    IngestHeartbeat, IngestHeartbeatRead, StoreConnectionMetrics, StoreDiagnostics, StoreHealth,
    StoreProbe, TableColumn, TablePreview, TablePreviewQuery, TableSummaries, TableSummary,
};

#[derive(Deserialize)]
struct HeartbeatColumnRow {
    name: String,
}

#[derive(Deserialize)]
struct HeartbeatRow {
    ts: String,
    ts_unix_ms: i64,
    host: String,
    service_version: String,
    queue_depth: u64,
    files_active: u32,
    files_watched: u32,
    rows_raw_written: u64,
    rows_events_written: u64,
    rows_errors_written: u64,
    flush_latency_ms: u32,
    append_to_visible_p50_ms: u32,
    append_to_visible_p95_ms: u32,
    last_error: String,
    watcher_backend: Option<String>,
    watcher_error_count: Option<u64>,
    watcher_reset_count: Option<u64>,
    watcher_last_reset_unix_ms: Option<u64>,
    backend_sinks: Option<String>,
}

#[derive(Deserialize)]
struct TableMetadataRow {
    name: String,
    engine: String,
    is_temporary: u8,
}

#[derive(Deserialize)]
struct TablePartRowsRow {
    table: String,
    rows: u64,
}

#[derive(Deserialize)]
struct TableSchemaRow {
    name: String,
    type_name: String,
    default_expression: String,
}

#[derive(Deserialize)]
struct DatabaseExistsRow {
    exists: u8,
}

#[derive(Deserialize)]
struct ConnectionMetricRow {
    metric: String,
    value: u64,
}

impl ClickHouseConversationRepository {
    pub(super) async fn latest_ingest_heartbeat_impl(&self) -> RepoResult<IngestHeartbeatRead> {
        let database_sql = sql_quote(&self.ch.config().database);
        let table_sql = sql_quote("ingest_heartbeats");
        let columns_query = format!(
            "SELECT name\n\
             FROM system.columns\n\
             WHERE database = {database_sql} AND table = {table_sql}\n\
             ORDER BY position ASC\n\
             FORMAT JSONEachRow"
        );
        let columns: Vec<HeartbeatColumnRow> =
            self.map_backend(self.query_rows(&columns_query, Some("system")).await)?;

        if columns.is_empty() {
            return Ok(IngestHeartbeatRead {
                table_present: false,
                latest: None,
            });
        }

        let optional_projection = |name: &str, nullable_type: &str| {
            if columns.iter().any(|column| column.name == name) {
                sql_identifier(name)
            } else {
                format!(
                    "CAST(NULL, {}) AS {}",
                    sql_quote(nullable_type),
                    sql_identifier(name)
                )
            }
        };
        let watcher_backend = optional_projection("watcher_backend", "Nullable(String)");
        let watcher_error_count = optional_projection("watcher_error_count", "Nullable(UInt64)");
        let watcher_reset_count = optional_projection("watcher_reset_count", "Nullable(UInt64)");
        let watcher_last_reset_unix_ms =
            optional_projection("watcher_last_reset_unix_ms", "Nullable(UInt64)");
        let backend_sinks = optional_projection("backend_sinks", "Nullable(String)");
        let heartbeats = self.table_ref("ingest_heartbeats");
        let heartbeat_query = format!(
            "SELECT\n\
               `ts`,\n\
               toUnixTimestamp64Milli(`ts`) AS `ts_unix_ms`,\n\
               `host`,\n\
               `service_version`,\n\
               `queue_depth`,\n\
               `files_active`,\n\
               `files_watched`,\n\
               `rows_raw_written`,\n\
               `rows_events_written`,\n\
               `rows_errors_written`,\n\
               `flush_latency_ms`,\n\
               `append_to_visible_p50_ms`,\n\
               `append_to_visible_p95_ms`,\n\
               `last_error`,\n\
               {watcher_backend},\n\
               {watcher_error_count},\n\
               {watcher_reset_count},\n\
               {watcher_last_reset_unix_ms},\n\
               {backend_sinks}\n\
             FROM {heartbeats}\n\
             ORDER BY `ts` DESC, `host` DESC\n\
             LIMIT 1\n\
             FORMAT JSONEachRow"
        );
        let rows: Vec<HeartbeatRow> =
            self.map_backend(self.query_rows(&heartbeat_query, None).await)?;
        let latest = rows.into_iter().next().map(|row| IngestHeartbeat {
            ts: row.ts,
            ts_unix_ms: row.ts_unix_ms,
            host: row.host,
            service_version: row.service_version,
            queue_depth: row.queue_depth,
            files_active: row.files_active,
            files_watched: row.files_watched,
            rows_raw_written: row.rows_raw_written,
            rows_events_written: row.rows_events_written,
            rows_errors_written: row.rows_errors_written,
            flush_latency_ms: row.flush_latency_ms,
            append_to_visible_p50_ms: row.append_to_visible_p50_ms,
            append_to_visible_p95_ms: row.append_to_visible_p95_ms,
            last_error: row.last_error,
            watcher_backend: row.watcher_backend,
            watcher_error_count: row.watcher_error_count,
            watcher_reset_count: row.watcher_reset_count,
            watcher_last_reset_unix_ms: row.watcher_last_reset_unix_ms,
            backend_sinks: decode_backend_sinks(row.backend_sinks),
        });

        Ok(IngestHeartbeatRead {
            table_present: true,
            latest,
        })
    }

    pub(super) async fn list_table_summaries_impl(&self) -> RepoResult<TableSummaries> {
        let database_sql = sql_quote(&self.ch.config().database);
        let tables_query = format!(
            "SELECT name, engine, toUInt8(is_temporary) AS is_temporary\n\
             FROM system.tables\n\
             WHERE database = {database_sql}\n\
             ORDER BY name ASC\n\
             FORMAT JSONEachRow"
        );
        let tables: Vec<TableMetadataRow> =
            self.map_backend(self.query_rows(&tables_query, Some("system")).await)?;

        let parts_query = format!(
            "SELECT table, toUInt64(sum(rows)) AS rows\n\
             FROM system.parts\n\
             WHERE database = {database_sql} AND active\n\
             GROUP BY table\n\
             ORDER BY table ASC\n\
             FORMAT JSONEachRow"
        );
        let (row_counts, row_counts_error) = match self
            .ch
            .query_rows::<TablePartRowsRow>(&parts_query, Some("system"))
            .await
        {
            Ok(parts) => (
                parts
                    .into_iter()
                    .map(|part| (part.table, part.rows))
                    .collect::<BTreeMap<_, _>>(),
                None,
            ),
            Err(error) => (BTreeMap::new(), Some(error.to_string())),
        };

        let tables = tables
            .into_iter()
            .map(|table| {
                let rows = row_counts.get(&table.name).copied().unwrap_or(0);
                TableSummary {
                    name: table.name,
                    engine: table.engine,
                    is_temporary: table.is_temporary != 0,
                    rows,
                }
            })
            .collect();

        Ok(TableSummaries {
            tables,
            row_counts_error,
        })
    }

    pub(super) async fn preview_table_impl(
        &self,
        query: TablePreviewQuery,
    ) -> RepoResult<TablePreview> {
        if !is_strict_ascii_identifier(&query.table) {
            return Err(RepoError::invalid_argument(
                "table must match [A-Za-z_][A-Za-z0-9_]*",
            ));
        }

        let limit = query.normalized_limit();
        let database_sql = sql_quote(&self.ch.config().database);
        let table_sql = sql_quote(&query.table);
        let schema_query = format!(
            "SELECT name, type AS type_name, default_expression\n\
             FROM system.columns\n\
             WHERE database = {database_sql} AND table = {table_sql}\n\
             ORDER BY position ASC\n\
             FORMAT JSONEachRow"
        );
        let schema_rows: Vec<TableSchemaRow> =
            self.map_backend(self.query_rows(&schema_query, Some("system")).await)?;

        let order_by = schema_rows
            .iter()
            .map(|column| sql_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let table_ref = self.table_ref(&query.table);
        let rows_query = if order_by.is_empty() {
            format!("SELECT * FROM {table_ref} LIMIT {limit} FORMAT JSONEachRow")
        } else {
            format!(
                "SELECT * FROM {table_ref} ORDER BY {order_by} LIMIT {limit} FORMAT JSONEachRow"
            )
        };
        let rows: Vec<Value> = self.map_backend(self.query_rows(&rows_query, None).await)?;
        let schema = schema_rows
            .into_iter()
            .map(|column| TableColumn {
                name: column.name,
                type_name: column.type_name,
                default_expression: column.default_expression,
            })
            .collect();

        Ok(TablePreview {
            table: query.table,
            limit,
            schema,
            rows,
        })
    }

    pub(super) async fn read_store_health_impl(&self) -> RepoResult<StoreHealth> {
        let ping_started = Instant::now();
        let ping_result = self.ch.ping().await;
        let ping_elapsed_ms = ping_started.elapsed().as_secs_f64() * 1_000.0;
        let ping = match ping_result {
            Ok(()) => StoreProbe::Available(ping_elapsed_ms),
            Err(error) => StoreProbe::Failed {
                message: error.to_string(),
            },
        };

        let version = match self.ch.version().await {
            Ok(version) => StoreProbe::Available(version),
            Err(error) => StoreProbe::Failed {
                message: error.to_string(),
            },
        };

        let database_query = format!(
            "SELECT toUInt8(count() > 0) AS exists\n\
             FROM system.databases\n\
             WHERE name = {}\n\
             FORMAT JSONEachRow",
            sql_quote(&self.ch.config().database)
        );
        let database_exists = match self
            .ch
            .query_rows::<DatabaseExistsRow>(&database_query, Some("system"))
            .await
        {
            Ok(rows) => {
                StoreProbe::Available(rows.first().map(|row| row.exists != 0).unwrap_or(false))
            }
            Err(error) => StoreProbe::Failed {
                message: error.to_string(),
            },
        };

        let connections_query = "SELECT metric, toUInt64(value) AS value\n\
                                 FROM system.metrics\n\
                                 WHERE metric IN ('TCPConnection', 'HTTPConnection', 'MySQLConnection', 'PostgreSQLConnection', 'InterserverConnection')\n\
                                 ORDER BY metric ASC\n\
                                 FORMAT JSONEachRow";
        let connections = match self
            .ch
            .query_rows::<ConnectionMetricRow>(connections_query, Some("system"))
            .await
        {
            Ok(rows) => {
                let mut metrics = StoreConnectionMetrics::default();
                for row in rows {
                    match row.metric.as_str() {
                        "TCPConnection" => metrics.tcp = metrics.tcp.saturating_add(row.value),
                        "HTTPConnection" => metrics.http = metrics.http.saturating_add(row.value),
                        "MySQLConnection" => {
                            metrics.mysql = metrics.mysql.saturating_add(row.value)
                        }
                        "PostgreSQLConnection" => {
                            metrics.postgres = metrics.postgres.saturating_add(row.value)
                        }
                        "InterserverConnection" => {
                            metrics.interserver = metrics.interserver.saturating_add(row.value)
                        }
                        _ => {}
                    }
                }
                metrics.total = metrics
                    .tcp
                    .saturating_add(metrics.http)
                    .saturating_add(metrics.mysql)
                    .saturating_add(metrics.postgres)
                    .saturating_add(metrics.interserver);
                StoreProbe::Available(metrics)
            }
            Err(error) => StoreProbe::Failed {
                message: error.to_string(),
            },
        };

        let publication = match self.ch.publication_diagnostics().await {
            Ok(diagnostics) => StoreProbe::Available(diagnostics),
            Err(error) => StoreProbe::Failed {
                message: error.to_string(),
            },
        };

        Ok(StoreHealth {
            ping,
            version,
            database_exists,
            connections,
            publication,
        })
    }

    pub(super) async fn read_store_diagnostics_impl(&self) -> RepoResult<StoreDiagnostics> {
        let report = self.map_backend(self.ch.doctor_report().await)?;
        Ok(StoreDiagnostics {
            healthy: report.clickhouse_healthy,
            version: report.clickhouse_version,
            database: report.database,
            database_exists: report.database_exists,
            applied_schema_versions: report.applied_migrations,
            pending_schema_versions: report.pending_migrations,
            missing_tables: report.missing_tables,
            publication: report.publication,
            errors: report.errors,
        })
    }
}

fn is_strict_ascii_identifier(value: &str) -> bool {
    let Some((first, rest)) = value.as_bytes().split_first() else {
        return false;
    };
    (first.is_ascii_alphabetic() || *first == b'_')
        && rest
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

fn decode_backend_sinks(raw: Option<String>) -> Option<Value> {
    raw.map(|raw| {
        if raw.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(&raw).unwrap_or(Value::String(raw))
        }
    })
}
