use crate::checkpoint::checkpoint_key;
use crate::dispatch::source_inode_for_file;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use crate::{Metrics, SinkMessage, WorkItem};
use anyhow::{Context, Result};
use moraine_config::{AppConfig, SOURCE_FORMAT_OPENCODE_SQLITE};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

use super::{
    hash_str, open_read_only, stat_fingerprint, truncate_chars_local, StatFingerprint,
    SyntheticRecord, CURSOR_STATE_VERSION, ERROR_KIND_OPEN, ERROR_KIND_SCAN, ERROR_KIND_SCHEMA,
    SCAN_PAGE_SIZE,
};

/// Per-table keyset cursor over `(time_updated, id)`. OpenCode bumps
/// `time_updated` on every write, so a row re-emits once its timestamp moves
/// past `watermark_ms`; a row mutated without advancing `time_updated` is not
/// re-scanned. The single case a poll can miss is a same-millisecond insert
/// whose `id` orders before `tie_id` — OpenCode ids are monotonic, so that
/// does not arise in practice.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct OpenCodeTableCursor {
    #[serde(default)]
    watermark_ms: i64,
    #[serde(default)]
    tie_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenCodeState {
    version: u32,
    format: String,
    #[serde(default)]
    stat: StatFingerprint,
    #[serde(default)]
    tables: BTreeMap<String, OpenCodeTableCursor>,
    #[serde(default)]
    last_error: String,
}

impl OpenCodeState {
    fn parse(cursor_json: &str) -> Self {
        if cursor_json.trim().is_empty() {
            return Self::fresh();
        }
        match serde_json::from_str::<OpenCodeState>(cursor_json) {
            Ok(state)
                if state.version == CURSOR_STATE_VERSION
                    && state.format == SOURCE_FORMAT_OPENCODE_SQLITE =>
            {
                state
            }
            Ok(_) | Err(_) => Self::fresh(),
        }
    }

    fn fresh() -> Self {
        Self {
            version: CURSOR_STATE_VERSION,
            format: SOURCE_FORMAT_OPENCODE_SQLITE.to_string(),
            ..Default::default()
        }
    }

    fn serialize(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn has_table_cursors(&self) -> bool {
        OPENCODE_TABLE_SPECS
            .iter()
            .all(|spec| self.tables.contains_key(spec.table))
    }
}

/// Builds one synthetic record from a result row, returning the keyset cursor
/// components `(id, time_updated)` alongside it. The shared scanner owns the
/// pagination loop; each table only supplies its row→record mapping.
type OpenCodeRowBuilder =
    fn(&OpenCodeTableSpec, &rusqlite::Row<'_>) -> Result<(String, i64, Map<String, Value>)>;

#[derive(Clone, Copy)]
struct OpenCodeTableSpec {
    table: &'static str,
    record_type: &'static str,
    required_columns: &'static [&'static str],
    select_sql: &'static str,
    extra_column: OpenCodeExtraColumn,
    build_row: OpenCodeRowBuilder,
}

#[derive(Clone, Copy)]
enum OpenCodeExtraColumn {
    None,
    PartMessageId,
    SessionMessageType,
}

const SESSION_COLUMNS: &[&str] = &[
    "id",
    "project_id",
    "parent_id",
    "slug",
    "directory",
    "title",
    "version",
    "share_url",
    "summary_additions",
    "summary_deletions",
    "summary_files",
    "summary_diffs",
    "time_created",
    "time_updated",
    "workspace_id",
    "path",
    "agent",
    "model",
    "cost",
    "tokens_input",
    "tokens_output",
    "tokens_reasoning",
    "tokens_cache_read",
    "tokens_cache_write",
    "metadata",
];
const MESSAGE_COLUMNS: &[&str] = &["id", "session_id", "time_created", "time_updated", "data"];
const PART_COLUMNS: &[&str] = &[
    "id",
    "message_id",
    "session_id",
    "time_created",
    "time_updated",
    "data",
];
const SESSION_MESSAGE_COLUMNS: &[&str] = &[
    "id",
    "session_id",
    "type",
    "time_created",
    "time_updated",
    "data",
    "seq",
];

const SESSION_SELECT_SQL: &str = "\
SELECT id, project_id, parent_id, slug, directory, title, version, share_url, \
summary_additions, summary_deletions, summary_files, summary_diffs, \
time_created, time_updated, workspace_id, path, agent, model, cost, \
tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, \
tokens_cache_write, metadata FROM session \
WHERE time_updated > ?1 OR (time_updated = ?1 AND id > ?2) \
ORDER BY time_updated, id LIMIT ?3";

const MESSAGE_SELECT_SQL: &str = "\
SELECT id, session_id, time_created, time_updated, data \
FROM message \
WHERE time_updated > ?1 OR (time_updated = ?1 AND id > ?2) \
ORDER BY time_updated, id LIMIT ?3";

const PART_SELECT_SQL: &str = "\
SELECT id, session_id, time_created, time_updated, data, message_id \
FROM part \
WHERE time_updated > ?1 OR (time_updated = ?1 AND id > ?2) \
ORDER BY time_updated, id LIMIT ?3";

const SESSION_MESSAGE_SELECT_SQL: &str = "\
SELECT id, session_id, time_created, time_updated, data, type \
FROM session_message \
WHERE time_updated > ?1 OR (time_updated = ?1 AND id > ?2) \
ORDER BY time_updated, id LIMIT ?3";

const OPENCODE_TABLE_SPECS: &[OpenCodeTableSpec] = &[
    OpenCodeTableSpec {
        table: "session",
        record_type: "opencode_session",
        required_columns: SESSION_COLUMNS,
        select_sql: SESSION_SELECT_SQL,
        extra_column: OpenCodeExtraColumn::None,
        build_row: build_session_row,
    },
    OpenCodeTableSpec {
        table: "message",
        record_type: "opencode_message",
        required_columns: MESSAGE_COLUMNS,
        select_sql: MESSAGE_SELECT_SQL,
        extra_column: OpenCodeExtraColumn::None,
        build_row: build_json_table_row,
    },
    OpenCodeTableSpec {
        table: "part",
        record_type: "opencode_part",
        required_columns: PART_COLUMNS,
        select_sql: PART_SELECT_SQL,
        extra_column: OpenCodeExtraColumn::PartMessageId,
        build_row: build_json_table_row,
    },
    OpenCodeTableSpec {
        table: "session_message",
        record_type: "opencode_session_message",
        required_columns: SESSION_MESSAGE_COLUMNS,
        select_sql: SESSION_MESSAGE_SELECT_SQL,
        extra_column: OpenCodeExtraColumn::SessionMessageType,
        build_row: build_json_table_row,
    },
];

enum OpenCodeScanOutcome {
    Scanned {
        records: Vec<SyntheticRecord>,
        new_state: OpenCodeState,
        schema_fingerprint: u64,
        relevant_rows: u64,
    },
    Failed {
        error_kind: &'static str,
        error_text: String,
    },
}

pub(crate) async fn process_opencode_sqlite_db(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let source_file = work.path.clone();

    let Some(current_stat) = stat_fingerprint(&source_file) else {
        debug!("opencode_sqlite db missing, skipping: {}", source_file);
        return Ok(());
    };

    let meta = match std::fs::metadata(&source_file) {
        Ok(meta) => meta,
        Err(exc) => {
            debug!("metadata missing for {}: {}", source_file, exc);
            return Ok(());
        }
    };
    let inode = source_inode_for_file(&source_file, &meta);

    let cp_key = checkpoint_key(&work.source_name, &source_file);
    let committed = { checkpoints.read().await.get(&cp_key).cloned() };

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.clone(),
        source_inode: inode,
        source_generation: 1,
        status: "active".to_string(),
        ..Default::default()
    });

    let mut state = OpenCodeState::parse(&checkpoint.cursor_json);

    // A replaced database file is a new generation: logical identities (and so
    // event UIDs) restart, and the old table watermarks no longer apply.
    if checkpoint.source_inode != inode {
        checkpoint.source_inode = inode;
        checkpoint.source_generation = checkpoint.source_generation.saturating_add(1).max(1);
        state = OpenCodeState::fresh();
    }

    // Cheap no-change short-circuit: nothing touched the database or its WAL
    // sidecars since the last poll. `has_table_cursors` also forces one real
    // scan after an upgrade that left a checkpoint without per-table cursors.
    if state.stat == current_stat && state.last_error.is_empty() && state.has_table_cursors() {
        return Ok(());
    }

    let scan_db_path = source_file.clone();
    let scan_state = state.clone();
    let outcome =
        tokio::task::spawn_blocking(move || scan_opencode_database(&scan_db_path, &scan_state))
            .await
            .context("opencode_sqlite scan task panicked")?;

    match outcome {
        OpenCodeScanOutcome::Scanned {
            records,
            mut new_state,
            schema_fingerprint,
            relevant_rows,
        } => {
            new_state.stat = current_stat;
            new_state.last_error = String::new();

            let mut batch = RowBatch::default();
            for synthetic in &records {
                let raw_json =
                    serde_json::to_string(&synthetic.record).unwrap_or_else(|_| "{}".to_string());
                match normalize_record(
                    &synthetic.record,
                    &work.source_name,
                    &work.harness,
                    &source_file,
                    inode,
                    checkpoint.source_generation,
                    synthetic.source_line_no,
                    synthetic.source_offset,
                    "",
                    "",
                    "",
                ) {
                    Ok(normalized) => {
                        batch.extend_normalized(normalized);
                        batch.lines_processed = batch.lines_processed.saturating_add(1);
                    }
                    Err(exc) => {
                        batch.push_error_row(json!({
                            "source_name": work.source_name,
                            "harness": work.harness,
                            "source_file": source_file,
                            "source_inode": inode,
                            "source_generation": checkpoint.source_generation,
                            "source_line_no": synthetic.source_line_no,
                            "source_offset": synthetic.source_offset,
                            "error_kind": "normalize_error",
                            "error_text": exc.to_string(),
                            "raw_fragment": truncate_chars_local(&raw_json, 20_000),
                        }));
                    }
                }

                if batch.exceeds_limits(config.ingest.batch_size, config.ingest.max_batch_bytes) {
                    let chunk = batch.drain_to_chunk();
                    sink_tx
                        .send(SinkMessage::Batch(chunk))
                        .await
                        .context("sink channel closed while sending opencode_sqlite chunk")?;
                }
            }

            let emitted = records.len();
            batch.checkpoint = Some(Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: checkpoint.last_offset.saturating_add(1),
                last_line_no: relevant_rows,
                status: "active".to_string(),
                cursor_json: new_state.serialize(),
                source_fingerprint: inode,
                schema_fingerprint,
            });

            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending final opencode_sqlite batch")?;

            if emitted > 0 {
                debug!(
                    "{}:{} opencode_sqlite emitted {} changed records ({} relevant rows)",
                    work.source_name, source_file, emitted, relevant_rows
                );
            }
            let _ = metrics;
            Ok(())
        }
        OpenCodeScanOutcome::Failed {
            error_kind,
            error_text,
        } => {
            // Emit each failure mode once per state change, not once per
            // reconcile tick: the marker is durable and reconcile re-polls every
            // tick, so re-sending it would append an identical error forever.
            if state.last_error == error_kind {
                return Ok(());
            }

            let mut batch = RowBatch::default();
            warn!(
                "opencode_sqlite poll failed for {}: {} ({})",
                source_file, error_kind, error_text
            );
            batch.push_error_row(json!({
                "source_name": work.source_name,
                "harness": work.harness,
                "source_file": source_file,
                "source_inode": inode,
                "source_generation": checkpoint.source_generation,
                "source_line_no": 0u64,
                "source_offset": 0u64,
                "error_kind": error_kind,
                "error_text": error_text,
                "raw_fragment": "",
            }));

            let mut error_state = state.clone();
            error_state.last_error = error_kind.to_string();
            batch.checkpoint = Some(Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: checkpoint.last_offset,
                last_line_no: checkpoint.last_line_no,
                status: "active".to_string(),
                cursor_json: error_state.serialize(),
                source_fingerprint: inode,
                schema_fingerprint: checkpoint.schema_fingerprint,
            });

            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending opencode_sqlite error batch")?;
            Ok(())
        }
    }
}

fn scan_opencode_database(db_path: &str, prior: &OpenCodeState) -> OpenCodeScanOutcome {
    let connection = match open_read_only(db_path) {
        Ok(connection) => connection,
        Err(exc) => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_OPEN,
                error_text: format!("{exc:#}"),
            }
        }
    };

    let schema_fingerprint = match validate_opencode_schema(&connection) {
        Ok(fingerprint) => fingerprint,
        Err(text) => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_SCHEMA,
                error_text: text,
            }
        }
    };

    match scan_opencode_rows(&connection, prior) {
        Ok((records, new_state, relevant_rows)) => OpenCodeScanOutcome::Scanned {
            records,
            new_state,
            schema_fingerprint,
            relevant_rows,
        },
        Err(exc) => OpenCodeScanOutcome::Failed {
            error_kind: ERROR_KIND_SCAN,
            error_text: format!("{exc:#}"),
        },
    }
}

fn validate_opencode_schema(connection: &Connection) -> std::result::Result<u64, String> {
    let mut schema_material = String::new();
    for spec in OPENCODE_TABLE_SPECS {
        let schema_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                rusqlite::params![spec.table],
                |row| row.get(0),
            )
            .map_err(|exc| match exc {
                rusqlite::Error::QueryReturnedNoRows => {
                    format!("required table {} is missing", spec.table)
                }
                other => other.to_string(),
            })?;
        schema_material.push_str(&schema_sql.unwrap_or_default());
        schema_material.push('\n');

        let names = table_columns(connection, spec.table).map_err(|exc| exc.to_string())?;
        for column in spec.required_columns {
            if !names.iter().any(|name| name == column) {
                return Err(format!(
                    "table {} is missing required column {column}",
                    spec.table
                ));
            }
        }
    }
    Ok(hash_str(&schema_material))
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("failed to inspect {table} columns"))?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(names)
}

fn scan_opencode_rows(
    connection: &Connection,
    prior: &OpenCodeState,
) -> Result<(Vec<SyntheticRecord>, OpenCodeState, u64)> {
    let mut records = Vec::new();
    let mut table_cursors = prior.tables.clone();
    let mut relevant_rows = 0u64;

    for spec in OPENCODE_TABLE_SPECS {
        relevant_rows += scan_opencode_table(connection, spec, &mut table_cursors, &mut records)?;
    }

    let mut new_state = OpenCodeState::fresh();
    new_state.tables = table_cursors;
    Ok((records, new_state, relevant_rows))
}

/// Pages an allowlisted table with the `(time_updated, id)` keyset cursor and
/// appends a synthetic record per row. `SCAN_PAGE_SIZE` bounds each read
/// transaction; the cursor itself is just one `(watermark_ms, tie_id)` pair, so
/// it never grows with table size.
fn scan_opencode_table(
    connection: &Connection,
    spec: &OpenCodeTableSpec,
    table_cursors: &mut BTreeMap<String, OpenCodeTableCursor>,
    records: &mut Vec<SyntheticRecord>,
) -> Result<u64> {
    let mut cursor = table_cursors.get(spec.table).cloned().unwrap_or_default();
    let mut seen = 0u64;

    loop {
        let mut page = Vec::<(String, i64, Map<String, Value>)>::new();
        {
            let mut stmt = connection.prepare_cached(spec.select_sql)?;
            let mut rows = stmt.query(rusqlite::params![
                cursor.watermark_ms,
                cursor.tie_id,
                SCAN_PAGE_SIZE as i64
            ])?;
            while let Some(row) = rows.next()? {
                page.push((spec.build_row)(spec, row)?);
            }
        }

        if page.is_empty() {
            break;
        }

        let more = page.len() == SCAN_PAGE_SIZE;
        for (id, time_updated, record) in page {
            seen += 1;
            push_opencode_record(spec.table, &record, records);
            cursor.watermark_ms = time_updated;
            cursor.tie_id = id;
        }
        table_cursors.insert(spec.table.to_string(), cursor.clone());

        if !more {
            break;
        }
    }

    // Record a cursor even for an empty table so `has_table_cursors` can engage
    // the no-change short-circuit on the next poll.
    table_cursors
        .entry(spec.table.to_string())
        .or_insert(cursor);
    Ok(seen)
}

fn build_session_row(
    spec: &OpenCodeTableSpec,
    row: &rusqlite::Row<'_>,
) -> Result<(String, i64, Map<String, Value>)> {
    let id = row.get::<_, String>(0)?;
    let time_updated = row.get::<_, i64>(13)?;
    let mut record = Map::new();
    record.insert("type".to_string(), json!(spec.record_type));
    record.insert("id".to_string(), json!(id.clone()));
    record.insert("project_id".to_string(), json!(row.get::<_, String>(1)?));
    copy_optional_string_column(row, 2, "parent_id", &mut record)?;
    copy_optional_string_column(row, 3, "slug", &mut record)?;
    record.insert("directory".to_string(), json!(row.get::<_, String>(4)?));
    record.insert("title".to_string(), json!(row.get::<_, String>(5)?));
    record.insert("version".to_string(), json!(row.get::<_, String>(6)?));
    copy_optional_string_column(row, 7, "share_url", &mut record)?;
    record.insert(
        "summary_additions".to_string(),
        json!(row.get::<_, i64>(8).unwrap_or(0)),
    );
    record.insert(
        "summary_deletions".to_string(),
        json!(row.get::<_, i64>(9).unwrap_or(0)),
    );
    record.insert(
        "summary_files".to_string(),
        json!(row.get::<_, i64>(10).unwrap_or(0)),
    );
    copy_json_text_column(row, 11, "summary_diffs", &mut record)?;
    record.insert("time_created".to_string(), json!(row.get::<_, i64>(12)?));
    record.insert("time_updated".to_string(), json!(time_updated));
    copy_optional_string_column(row, 14, "workspace_id", &mut record)?;
    copy_optional_string_column(row, 15, "path", &mut record)?;
    copy_optional_string_column(row, 16, "agent", &mut record)?;
    copy_json_text_column(row, 17, "model", &mut record)?;
    record.insert(
        "cost".to_string(),
        json!(row.get::<_, f64>(18).unwrap_or(0.0)),
    );
    record.insert(
        "tokens_input".to_string(),
        json!(row.get::<_, i64>(19).unwrap_or(0)),
    );
    record.insert(
        "tokens_output".to_string(),
        json!(row.get::<_, i64>(20).unwrap_or(0)),
    );
    record.insert(
        "tokens_reasoning".to_string(),
        json!(row.get::<_, i64>(21).unwrap_or(0)),
    );
    record.insert(
        "tokens_cache_read".to_string(),
        json!(row.get::<_, i64>(22).unwrap_or(0)),
    );
    record.insert(
        "tokens_cache_write".to_string(),
        json!(row.get::<_, i64>(23).unwrap_or(0)),
    );
    copy_json_text_column(row, 24, "metadata", &mut record)?;
    Ok((id, time_updated, record))
}

fn build_json_table_row(
    spec: &OpenCodeTableSpec,
    row: &rusqlite::Row<'_>,
) -> Result<(String, i64, Map<String, Value>)> {
    let id = row.get::<_, String>(0)?;
    let time_updated = row.get::<_, i64>(3)?;
    let mut record = Map::new();
    record.insert("type".to_string(), json!(spec.record_type));
    record.insert("id".to_string(), json!(id.clone()));
    record.insert("session_id".to_string(), json!(row.get::<_, String>(1)?));
    record.insert("time_created".to_string(), json!(row.get::<_, i64>(2)?));
    record.insert("time_updated".to_string(), json!(time_updated));
    copy_json_text_column(row, 4, "data", &mut record)?;
    match spec.extra_column {
        OpenCodeExtraColumn::None => {}
        OpenCodeExtraColumn::PartMessageId => {
            record.insert("message_id".to_string(), json!(row.get::<_, String>(5)?));
        }
        OpenCodeExtraColumn::SessionMessageType => {
            record.insert("message_type".to_string(), json!(row.get::<_, String>(5)?));
        }
    }
    Ok((id, time_updated, record))
}

fn push_opencode_record(
    table: &str,
    record: &Map<String, Value>,
    records: &mut Vec<SyntheticRecord>,
) {
    let id = record.get("id").and_then(Value::as_str).unwrap_or("");
    if id.is_empty() {
        return;
    }
    let value = Value::Object(record.clone());
    let record_kind = record
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("opencode_sqlite");
    let source_line_no = hash_str(&format!("{table}:{id}"));
    let source_offset = hash_str(&format!(
        "{SOURCE_FORMAT_OPENCODE_SQLITE}:{table}:{id}:{record_kind}"
    ));
    records.push(SyntheticRecord {
        record: value,
        source_line_no,
        source_offset,
    });
}

fn copy_optional_string_column(
    row: &rusqlite::Row<'_>,
    idx: usize,
    key: &str,
    record: &mut Map<String, Value>,
) -> Result<()> {
    let value: Option<String> = row.get(idx)?;
    if let Some(value) = value {
        if !value.is_empty() {
            record.insert(key.to_string(), json!(value));
        }
    }
    Ok(())
}

fn copy_json_text_column(
    row: &rusqlite::Row<'_>,
    idx: usize,
    key: &str,
    record: &mut Map<String, Value>,
) -> Result<()> {
    let value: Option<String> = row.get(idx)?;
    if let Some(value) = value {
        if value.trim().is_empty() {
            return Ok(());
        }
        match serde_json::from_str::<Value>(&value) {
            Ok(parsed) => {
                record.insert(key.to_string(), parsed);
            }
            Err(_) => {
                record.insert(key.to_string(), json!(value));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
