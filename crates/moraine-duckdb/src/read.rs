use std::sync::atomic::Ordering;
use std::sync::Arc;

use duckdb::{params, types::Value as DuckValue, Connection, InterruptHandle};
use moraine_conversations::{TableColumn, TablePreview, TableSummaries, TableSummary};
use serde_json::Value;

use crate::{DuckDbError, DuckDbResult, DuckDbStore};

struct ReaderCancellation {
    handle: Arc<InterruptHandle>,
    armed: bool,
}

impl ReaderCancellation {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReaderCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.handle.interrupt();
        }
    }
}

fn json_value(value: DuckValue) -> Value {
    match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(value) => Value::Bool(value),
        DuckValue::TinyInt(value) => Value::from(value),
        DuckValue::SmallInt(value) => Value::from(value),
        DuckValue::Int(value) => Value::from(value),
        DuckValue::BigInt(value) => Value::from(value),
        DuckValue::HugeInt(value) => Value::String(value.to_string()),
        DuckValue::UHugeInt(value) => Value::String(value.to_string()),
        DuckValue::UTinyInt(value) => Value::from(value),
        DuckValue::USmallInt(value) => Value::from(value),
        DuckValue::UInt(value) => Value::from(value),
        DuckValue::UBigInt(value) => Value::from(value),
        DuckValue::Float(value) => Value::from(value),
        DuckValue::Double(value) => Value::from(value),
        DuckValue::Text(value) | DuckValue::Enum(value) => Value::String(value),
        DuckValue::Blob(value) | DuckValue::Geometry(value) => {
            Value::String(format!("<{} bytes>", value.len()))
        }
        DuckValue::List(values) | DuckValue::Array(values) => {
            Value::Array(values.into_iter().map(json_value).collect())
        }
        DuckValue::Struct(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_value(value.clone())))
                .collect(),
        ),
        DuckValue::Union(value) => json_value(*value),
        other => Value::String(format!("{other:?}")),
    }
}

impl DuckDbStore {
    async fn run_reader<T, F>(&self, operation: F) -> DuckDbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> DuckDbResult<T> + Send + 'static,
    {
        if self.is_shutting_down() {
            return Err(DuckDbError::ShuttingDown);
        }
        let permit = self
            .reader_admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| DuckDbError::BlockingJob(error.to_string()))?;
        if self.is_shutting_down() {
            return Err(DuckDbError::ShuttingDown);
        }
        let connection = self
            .readers
            .lock()
            .map_err(|_| DuckDbError::BlockingJob("reader pool lock poisoned".to_string()))?
            .pop()
            .ok_or_else(|| DuckDbError::BlockingJob("reader pool exhausted".to_string()))?;
        let mut cancellation = ReaderCancellation {
            handle: connection.interrupt_handle(),
            armed: true,
        };
        let readers = self.readers.clone();
        let shutting_down = self.shutting_down.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let result = if shutting_down.load(Ordering::Acquire) {
                Err(DuckDbError::ShuttingDown)
            } else {
                operation(&connection)
            };
            readers
                .lock()
                .map_err(|_| DuckDbError::BlockingJob("reader pool lock poisoned".to_string()))?
                .push(connection);
            result
        })
        .await
        .map_err(|error| DuckDbError::BlockingJob(error.to_string()))?;
        cancellation.disarm();
        result
    }

    pub async fn current_event_count(self: Arc<Self>) -> DuckDbResult<u64> {
        self.run_reader(|connection| {
            connection
                .query_row(
                    "SELECT count(*) FROM (SELECT row_number() OVER (PARTITION BY e.source_name, e.source_file, e.source_generation, e.event_uid ORDER BY e.event_version DESC) AS version_rank FROM events e JOIN publication_heads h USING (source_name, source_file) WHERE e.source_generation = h.source_generation) current_events WHERE version_rank = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
        })
        .await
    }

    pub async fn posting_count(self: Arc<Self>) -> DuckDbResult<u64> {
        self.run_reader(|connection| {
            connection
                .query_row("SELECT count(*) FROM search_postings", [], |row| row.get(0))
                .map_err(Into::into)
        })
        .await
    }

    pub async fn stored_event_count(self: Arc<Self>) -> DuckDbResult<u64> {
        self.run_reader(|connection| {
            connection
                .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
                .map_err(Into::into)
        })
        .await
    }

    pub async fn current_search_event_ids(
        self: Arc<Self>,
        term: impl Into<String>,
        limit: u16,
    ) -> DuckDbResult<Vec<String>> {
        let term = term.into().to_ascii_lowercase();
        self.run_reader(move |connection| {
            let mut statement = connection.prepare(
                "SELECT p.event_uid FROM search_postings p JOIN publication_heads h ON h.source_name = p.source_name AND h.source_file = p.source_file AND h.source_generation = p.source_generation JOIN (SELECT source_name, source_file, source_generation, event_uid, max(event_version) AS event_version FROM events GROUP BY source_name, source_file, source_generation, event_uid) latest ON latest.source_name = p.source_name AND latest.source_file = p.source_file AND latest.source_generation = p.source_generation AND latest.event_uid = p.event_uid AND latest.event_version = p.event_version WHERE p.term = ? GROUP BY p.event_uid ORDER BY sum(p.term_frequency) DESC, p.event_uid ASC LIMIT ?",
            )?;
            let rows = statement.query_map(params![term, limit], |row| row.get(0))?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }

    pub async fn publication_head(
        self: Arc<Self>,
        source_name: impl Into<String>,
        source_file: impl Into<String>,
    ) -> DuckDbResult<Option<(u64, u64, u64)>> {
        let source_name = source_name.into();
        let source_file = source_file.into();
        self.run_reader(move |connection| {
            let mut statement = connection.prepare(
                "SELECT source_generation, publication_revision, cache_epoch FROM publication_heads WHERE source_name = ? AND source_file = ?",
            )?;
            let mut rows = statement.query(params![source_name, source_file])?;
            match rows.next()? {
                Some(row) => Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?))),
                None => Ok(None),
            }
        })
        .await
    }

    pub(crate) async fn current_rows(
        &self,
        session_id: Option<String>,
        event_uid: Option<String>,
    ) -> DuckDbResult<Vec<Value>> {
        self.run_reader(move |connection| {
            let session_id = session_id.unwrap_or_default();
            let event_uid = event_uid.unwrap_or_default();
            let mut statement = connection.prepare(
                "SELECT payload_json FROM (SELECT e.*, row_number() OVER (PARTITION BY e.source_name, e.source_file, e.source_generation, e.event_uid ORDER BY e.event_version DESC) AS version_rank FROM events e JOIN publication_heads h USING (source_name, source_file) WHERE e.source_generation = h.source_generation) current_events WHERE version_rank = 1 AND (? = '' OR session_id = ?) AND (? = '' OR event_uid = ?) ORDER BY event_time ASC, event_order ASC, event_uid ASC",
            )?;
            let rows = statement.query_map(
                params![session_id, session_id, event_uid, event_uid],
                |row| row.get::<_, String>(0),
            )?;
            rows.map(|row| {
                let json = row?;
                serde_json::from_str(&json).map_err(|error| {
                    duckdb::Error::FromSqlConversionFailure(
                        0,
                        duckdb::types::Type::Text,
                        Box::new(error),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
        })
        .await
    }

    pub(crate) async fn table_summaries(&self) -> DuckDbResult<TableSummaries> {
        const TABLES: [&str; 10] = [
            "events",
            "raw_events",
            "event_links",
            "ingest_errors",
            "ingest_checkpoints",
            "ingest_heartbeats",
            "ingest_progress",
            "publication_heads",
            "search_postings",
            "moraine_schema_migrations",
        ];
        self.run_reader(|connection| {
            let mut tables = Vec::with_capacity(TABLES.len());
            for name in TABLES {
                let sql = format!("SELECT count(*) FROM {name}");
                let rows = connection.query_row(&sql, [], |row| row.get(0))?;
                tables.push(TableSummary {
                    name: name.to_string(),
                    engine: "DuckDB".to_string(),
                    is_temporary: false,
                    rows,
                });
            }
            Ok(TableSummaries {
                tables,
                row_counts_error: None,
            })
        })
        .await
    }

    pub(crate) async fn preview_table_rows(
        &self,
        table: &str,
        limit: u16,
    ) -> DuckDbResult<TablePreview> {
        const TABLES: [&str; 10] = [
            "events",
            "raw_events",
            "event_links",
            "ingest_errors",
            "ingest_checkpoints",
            "ingest_heartbeats",
            "ingest_progress",
            "publication_heads",
            "search_postings",
            "moraine_schema_migrations",
        ];
        if !TABLES.contains(&table) {
            return Err(DuckDbError::BlockingJob(format!(
                "table `{table}` is not available for DuckDB preview"
            )));
        }
        let table = table.to_string();
        self.run_reader(move |connection| {
            let mut schema_statement = connection.prepare(
                "SELECT column_name, data_type, coalesce(column_default, '') FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
            )?;
            let schema = schema_statement
                .query_map(params![&table], |row| {
                    Ok(TableColumn {
                        name: row.get(0)?,
                        type_name: row.get(1)?,
                        default_expression: row.get(2)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let sql = format!("SELECT * FROM {table} LIMIT ?");
            let mut statement = connection.prepare(&sql)?;
            let column_names = schema
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let rows = statement
                .query_map(params![limit], |row| {
                    let mut object = serde_json::Map::with_capacity(column_names.len());
                    for (index, name) in column_names.iter().enumerate() {
                        object.insert(name.clone(), json_value(row.get::<_, DuckValue>(index)?));
                    }
                    Ok(Value::Object(object))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(TablePreview {
                table,
                limit,
                schema,
                rows,
            })
        })
        .await
    }

    pub(crate) async fn ingest_heartbeat_values(&self, limit: u16) -> DuckDbResult<Vec<Value>> {
        self.run_reader(move |connection| {
            let mut statement = connection.prepare(
                "SELECT row_json FROM ingest_heartbeats ORDER BY observed_at DESC LIMIT ?",
            )?;
            statement
                .query_map(params![limit], |row| row.get::<_, String>(0))?
                .map(|row| {
                    let json = row?;
                    serde_json::from_str(&json).map_err(|error| {
                        duckdb::Error::FromSqlConversionFailure(
                            0,
                            duckdb::types::Type::Text,
                            Box::new(error),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::DuckDbStore;
    use crate::DuckDbStoreOptions;

    #[tokio::test]
    async fn dropped_read_interrupts_and_quarantines_its_connection_until_join() {
        let root = tempfile::tempdir().expect("temporary runtime root");
        let mut options = DuckDbStoreOptions::for_runtime_root(root.path());
        options.read_connections = 1;
        let store = DuckDbStore::open(options).await.expect("open store");
        let reader = store.clone();
        let task = tokio::spawn(async move {
            reader
                .run_reader(|connection| {
                    connection
                        .query_row(
                            "SELECT sum(value) FROM range(0, 1000000000) values(value)",
                            [],
                            |row| row.get::<_, u64>(0),
                        )
                        .map_err(Into::into)
                })
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        task.abort();
        let _ = task.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.reader_count() != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("interrupted blocking read joined and returned its lease");
        assert_eq!(
            store
                .clone()
                .current_event_count()
                .await
                .expect("reader remains usable"),
            0
        );
        store.close().await.expect("close store");
    }
}
