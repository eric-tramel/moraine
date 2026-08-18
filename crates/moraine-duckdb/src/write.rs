use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use duckdb::{params, Transaction};
use moraine_ingest_core::model::Checkpoint;
use moraine_ingest_core::store::{
    DurableIngestAck, IngestCommitKind, IngestStore, IngestStoreError, IngestStoreErrorKind,
    IngestWrite, PreparedIngestBatch, PublicationTransition,
};
use serde_json::Value;

use crate::security::secure_database_file;
use crate::{DuckDbError, DuckDbStore};

fn store_error(error: impl std::fmt::Display) -> IngestStoreError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    let kind = if lower.contains("out of memory")
        || lower.contains("no space")
        || lower.contains("maximum temp")
    {
        IngestStoreErrorKind::ResourceExhausted
    } else if lower.contains("lock") || lower.contains("conflict") || lower.contains("busy") {
        IngestStoreErrorKind::Busy
    } else if lower.contains("interrupt") {
        IngestStoreErrorKind::Cancelled
    } else if lower.contains("corrupt") {
        IngestStoreErrorKind::Corruption
    } else {
        IngestStoreErrorKind::Permanent
    };
    IngestStoreError::new(kind, message)
}

fn text(row: &Value, key: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn number(row: &Value, key: &str) -> u64 {
    row.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn event_order(row: &Value) -> u64 {
    number(row, "event_order").max(
        number(row, "source_line_no")
            .saturating_mul(1024)
            .saturating_add(number(row, "emission_index")),
    )
}

fn token_frequencies(value: &str) -> BTreeMap<String, u32> {
    let mut terms = BTreeMap::new();
    for token in value
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| (2..=64).contains(&token.len()))
    {
        *terms.entry(token.to_ascii_lowercase()).or_insert(0) += 1;
    }
    terms
}

fn append_json_rows(
    transaction: &Transaction<'_>,
    table: &str,
    operation_id: &str,
    rows: &[Value],
) -> Result<(), duckdb::Error> {
    debug_assert!(matches!(table, "event_links" | "ingest_errors"));
    let mut appender = transaction.appender(table)?;
    for row in rows {
        appender.append_row(params![operation_id, row.to_string()])?;
    }
    appender.flush()
}

fn append_batch(
    transaction: &Transaction<'_>,
    operation_id: &str,
    batch: &PreparedIngestBatch,
) -> Result<(), IngestStoreError> {
    for table in ["raw_events", "event_links", "ingest_errors"] {
        transaction
            .execute(
                &format!("DELETE FROM {table} WHERE operation_id = ?"),
                params![operation_id],
            )
            .map_err(store_error)?;
    }
    let mut postings = Vec::new();
    let mut event_appender = transaction.appender("event_staging").map_err(store_error)?;
    for row in &batch.event_rows {
        let event_uid = text(row, "event_uid");
        let event_version = number(row, "event_version");
        let source_name = text(row, "source_name");
        let source_file = text(row, "source_file");
        let source_generation = number(row, "source_generation");
        // `events` is the sole canonical content authority. Persist the full
        // normalized/redacted row; derived tables retain identifiers only.
        let payload_json = row.to_string();
        event_appender
            .append_row(params![
                operation_id,
                event_uid,
                event_version,
                source_name,
                source_file,
                source_generation,
                text(row, "session_id"),
                text(row, "turn_id"),
                text(row, "event_kind"),
                text(row, "event_ts"),
                event_order(row),
                payload_json,
            ])
            .map_err(store_error)?;
        let searchable = format!(
            "{} {} {}",
            text(row, "text_content"),
            text(row, "text_preview"),
            row.get("payload_json")
                .and_then(Value::as_str)
                .unwrap_or_default()
        );
        let frequencies = token_frequencies(&searchable);
        let document_length = frequencies.values().copied().map(u64::from).sum::<u64>();
        for (term, frequency) in frequencies {
            postings.push((
                source_name.clone(),
                source_file.clone(),
                source_generation,
                event_uid.clone(),
                event_version,
                term,
                frequency,
                document_length,
            ));
        }
    }
    event_appender.flush().map_err(store_error)?;
    drop(event_appender);
    transaction
        .execute(
            "INSERT INTO events SELECT event_uid, event_version, source_name, source_file, source_generation, session_id, turn_id, event_type, event_time, event_order, payload_json FROM event_staging WHERE operation_id = ? ON CONFLICT DO NOTHING",
            params![operation_id],
        )
        .map_err(store_error)?;
    transaction
        .execute(
            "DELETE FROM event_staging WHERE operation_id = ?",
            params![operation_id],
        )
        .map_err(store_error)?;

    for posting in postings {
        transaction
            .execute(
                "INSERT OR REPLACE INTO search_postings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    posting.0, posting.1, posting.2, posting.3, posting.4, posting.5, posting.6,
                    posting.7,
                ],
            )
            .map_err(store_error)?;
    }

    append_json_rows(transaction, "event_links", operation_id, &batch.link_rows)
        .map_err(store_error)?;
    append_json_rows(
        transaction,
        "ingest_errors",
        operation_id,
        &batch.error_rows,
    )
    .map_err(store_error)?;
    let mut raw_appender = transaction.appender("raw_events").map_err(store_error)?;
    for row in &batch.raw_rows {
        raw_appender
            .append_row(params![
                operation_id,
                text(row, "source_name"),
                text(row, "source_file"),
                number(row, "source_generation"),
                row.to_string(),
            ])
            .map_err(store_error)?;
    }
    raw_appender.flush().map_err(store_error)?;
    Ok(())
}

fn persist_checkpoint(
    transaction: &Transaction<'_>,
    checkpoint: &Checkpoint,
) -> Result<(), IngestStoreError> {
    let checkpoint_json = serde_json::to_string(checkpoint).map_err(store_error)?;
    transaction
        .execute(
            "INSERT INTO ingest_checkpoints (source_name, source_file, source_generation, source_offset, checkpoint_revision, operation_id, checkpoint_json, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp) ON CONFLICT (source_name, source_file) DO UPDATE SET source_generation = excluded.source_generation, source_offset = excluded.source_offset, checkpoint_revision = excluded.checkpoint_revision, operation_id = excluded.operation_id, checkpoint_json = excluded.checkpoint_json, updated_at = excluded.updated_at WHERE excluded.checkpoint_revision >= ingest_checkpoints.checkpoint_revision",
            params![
                checkpoint.source_name,
                checkpoint.source_file,
                checkpoint.source_generation,
                checkpoint.last_offset,
                checkpoint.checkpoint_revision,
                checkpoint.operation_id,
                checkpoint_json,
            ],
        )
        .map_err(store_error)?;
    Ok(())
}

fn ack(
    kind: IngestCommitKind,
    operation_id: String,
    checkpoint_revision: u64,
    publication_revision: u64,
) -> DurableIngestAck {
    DurableIngestAck {
        kind,
        operation_id,
        checkpoint_revision,
        publication_revision,
    }
}

fn apply_transition(
    transaction: &Transaction<'_>,
    transition: &PublicationTransition,
    publish: bool,
) -> Result<(), IngestStoreError> {
    persist_checkpoint(transaction, &transition.checkpoint)?;
    if publish {
        transaction
            .execute(
                "INSERT INTO publication_heads (source_name, source_file, source_generation, publication_revision, cache_epoch, published_at) VALUES (?, ?, ?, ?, ?, current_timestamp) ON CONFLICT (source_name, source_file) DO UPDATE SET source_generation = excluded.source_generation, publication_revision = excluded.publication_revision, cache_epoch = excluded.cache_epoch, published_at = excluded.published_at WHERE excluded.publication_revision >= publication_heads.publication_revision",
                params![
                    transition.source_name,
                    transition.source_file,
                    transition.source_generation,
                    transition.publication_revision,
                    transition.cache_epoch,
                ],
            )
            .map_err(store_error)?;
    }
    Ok(())
}

impl DuckDbStore {
    fn write_blocking(&self, command: IngestWrite) -> Result<DurableIngestAck, IngestStoreError> {
        if self.is_shutting_down() {
            return Err(IngestStoreError::new(
                IngestStoreErrorKind::Shutdown,
                "DuckDB store is shutting down",
            ));
        }
        let mut guard = self.writer.lock().map_err(|_| {
            IngestStoreError::new(IngestStoreErrorKind::Permanent, "writer lock poisoned")
        })?;
        let connection = guard.as_mut().ok_or_else(|| {
            IngestStoreError::new(IngestStoreErrorKind::Shutdown, "writer is closed")
        })?;
        let transaction = connection.transaction().map_err(store_error)?;
        let result = match command {
            IngestWrite::Append(batch) => {
                let checkpoint = batch.checkpoint.clone();
                let operation_id = checkpoint
                    .as_ref()
                    .map(|item| item.operation_id.clone())
                    .filter(|item| !item.is_empty())
                    .unwrap_or_else(|| "append".to_string());
                append_batch(&transaction, &operation_id, &batch)?;
                if let Some(checkpoint) = &checkpoint {
                    persist_checkpoint(&transaction, checkpoint)?;
                    if checkpoint.status == "active" || checkpoint.status == "ready" {
                        transaction
                            .execute(
                                "INSERT INTO publication_heads (source_name, source_file, source_generation, publication_revision, cache_epoch, published_at) VALUES (?, ?, ?, ?, ?, current_timestamp) ON CONFLICT (source_name, source_file) DO UPDATE SET source_generation = excluded.source_generation, publication_revision = excluded.publication_revision, cache_epoch = excluded.cache_epoch, published_at = excluded.published_at WHERE excluded.publication_revision >= publication_heads.publication_revision",
                                params![
                                    checkpoint.source_name,
                                    checkpoint.source_file,
                                    checkpoint.source_generation,
                                    checkpoint.checkpoint_revision,
                                    checkpoint.cache_epoch,
                                ],
                            )
                            .map_err(store_error)?;
                    }
                }
                ack(
                    IngestCommitKind::Append,
                    operation_id,
                    checkpoint
                        .as_ref()
                        .map_or(0, |item| item.checkpoint_revision),
                    0,
                )
            }
            IngestWrite::BeginReplay(transition) => {
                apply_transition(&transaction, &transition, false)?;
                ack(
                    IngestCommitKind::ReplayPrepared,
                    transition.operation_id,
                    transition.checkpoint_revision,
                    transition.publication_revision,
                )
            }
            IngestWrite::PublishReplay { transition, batch } => {
                append_batch(&transaction, &transition.operation_id, &batch)?;
                apply_transition(&transaction, &transition, true)?;
                ack(
                    IngestCommitKind::ReplayPublished,
                    transition.operation_id,
                    transition.checkpoint_revision,
                    transition.publication_revision,
                )
            }
            IngestWrite::BlockReplay {
                mut transition,
                reason,
            } => {
                transition.checkpoint.block_reason = reason;
                apply_transition(&transaction, &transition, false)?;
                ack(
                    IngestCommitKind::ReplayBlocked,
                    transition.operation_id,
                    transition.checkpoint_revision,
                    transition.publication_revision,
                )
            }
            IngestWrite::Heartbeat(mut row) => {
                let now = DateTime::<Utc>::from(std::time::SystemTime::now());
                if let Some(object) = row.as_object_mut() {
                    object.insert("ts".to_string(), Value::String(now.to_rfc3339()));
                    object.insert(
                        "ts_unix_ms".to_string(),
                        Value::from(now.timestamp_millis()),
                    );
                    if let Some(progress) = object
                        .remove("progress_json")
                        .and_then(|value| value.as_str().map(str::to_string))
                        .and_then(|json| serde_json::from_str(&json).ok())
                    {
                        object.insert("progress".to_string(), progress);
                    }
                }
                transaction
                    .execute(
                        "INSERT INTO ingest_heartbeats (row_json) VALUES (?)",
                        params![row.to_string()],
                    )
                    .map_err(store_error)?;
                ack(IngestCommitKind::Heartbeat, String::new(), 0, 0)
            }
            IngestWrite::Progress(row) => {
                transaction
                    .execute(
                        "INSERT INTO ingest_progress (row_json) VALUES (?)",
                        params![row.to_string()],
                    )
                    .map_err(store_error)?;
                ack(IngestCommitKind::Progress, String::new(), 0, 0)
            }
        };
        transaction.commit().map_err(store_error)?;
        // DuckDB may recreate its WAL using the process umask when a new write
        // transaction begins. Re-apply the private-file contract after every
        // durable commit rather than only when the store opens or closes.
        secure_database_file(&self.database_path).map_err(store_error)?;
        Ok(result)
    }
}

#[async_trait]
impl IngestStore for DuckDbStore {
    async fn validate_capabilities(self: Arc<Self>) -> Result<(), IngestStoreError> {
        (self.schema_version().map_err(store_error)? == 1)
            .then_some(())
            .ok_or_else(|| {
                IngestStoreError::new(
                    IngestStoreErrorKind::SchemaIncompatible,
                    "unsupported DuckDB schema",
                )
            })
    }

    async fn load_checkpoints(self: Arc<Self>) -> Result<Vec<Checkpoint>, IngestStoreError> {
        tokio::task::spawn_blocking(move || {
            let guard = self.writer.lock().map_err(|_| {
                IngestStoreError::new(IngestStoreErrorKind::Permanent, "writer lock poisoned")
            })?;
            let connection = guard.as_ref().ok_or_else(|| {
                IngestStoreError::new(IngestStoreErrorKind::Shutdown, "writer is closed")
            })?;
            let mut statement = connection
                .prepare("SELECT checkpoint_json FROM ingest_checkpoints ORDER BY source_name, source_file")
                .map_err(store_error)?;
            let rows = statement
                .query_map([], |row| {
                    let json = row.get::<_, String>(0)?;
                    serde_json::from_str(&json).map_err(|error| {
                        duckdb::Error::FromSqlConversionFailure(
                            0,
                            duckdb::types::Type::Text,
                            Box::new(error),
                        )
                    })
                })
                .map_err(store_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(store_error)
        })
        .await
        .map_err(store_error)?
    }

    async fn write(
        self: Arc<Self>,
        command: IngestWrite,
    ) -> Result<DurableIngestAck, IngestStoreError> {
        tokio::task::spawn_blocking(move || self.write_blocking(command))
            .await
            .map_err(store_error)?
    }

    async fn shutdown(self: Arc<Self>) -> Result<(), IngestStoreError> {
        self.close().await.map_err(store_error)
    }
}

impl From<DuckDbError> for IngestStoreError {
    fn from(error: DuckDbError) -> Self {
        store_error(error)
    }
}
