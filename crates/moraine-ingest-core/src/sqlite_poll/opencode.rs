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
    ERROR_KIND_TOO_LARGE, SCAN_PAGE_MAX_BYTES, SCAN_PAGE_SIZE,
};

const MAX_OPENCODE_RELEVANT_ROWS: u64 = 10_000;
const MAX_OPENCODE_SCAN_BYTES: u64 = 128 * 1024 * 1024;
const OPENCODE_LONG_BINARY_STRING_CHARS: usize = 65_536;
const OPENCODE_DUPLICATE_SESSION_MESSAGE_TYPES: &[&str] = &["user", "assistant"];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenCodeSessionContext {
    #[serde(default)]
    directory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenCodeMessageContext {
    #[serde(default)]
    role: String,
    #[serde(default)]
    agent: String,
    #[serde(default)]
    model_id: String,
    #[serde(default)]
    provider_id: String,
    #[serde(default)]
    directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenCodeState {
    version: u32,
    format: String,
    #[serde(default)]
    stat: StatFingerprint,
    #[serde(default)]
    event_scan_complete: bool,
    #[serde(default)]
    aggregate_sequences: BTreeMap<String, i64>,
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
}

const EVENT_COLUMNS: &[&str] = &["id", "aggregate_id", "seq", "type", "data"];
const EVENT_SEQUENCE_COLUMNS: &[&str] = &["aggregate_id", "seq"];

#[derive(Debug)]
struct OpenCodeEventRow {
    id: String,
    aggregate_id: String,
    seq: i64,
    event_type: String,
    data: Value,
    data_bytes: usize,
}

#[derive(Debug)]
struct OpenCodeAggregateSequence {
    aggregate_id: String,
    seq: i64,
}

#[derive(Default)]
struct OpenCodeAccumulated {
    sessions: BTreeMap<String, Map<String, Value>>,
    messages: BTreeMap<String, Map<String, Value>>,
    parts: BTreeMap<String, Map<String, Value>>,
    session_messages: BTreeMap<String, Map<String, Value>>,
}

#[derive(Debug)]
enum OpenCodeScanError {
    Scan(anyhow::Error),
    TooLarge(String),
}

impl From<anyhow::Error> for OpenCodeScanError {
    fn from(error: anyhow::Error) -> Self {
        Self::Scan(error)
    }
}

impl From<rusqlite::Error> for OpenCodeScanError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Scan(error.into())
    }
}

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
    // event UIDs) restart, and the old event cursor no longer applies.
    if checkpoint.source_inode != inode {
        checkpoint.source_inode = inode;
        checkpoint.source_generation = checkpoint.source_generation.saturating_add(1).max(1);
        state = OpenCodeState::fresh();
    }

    // Cheap no-change short-circuit: nothing touched the database or its WAL
    // sidecars since the last poll. `event_scan_complete` also forces one real
    // scan after upgrading from the earlier projection cursor state.
    if state.stat == current_stat && state.last_error.is_empty() && state.event_scan_complete {
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

    match opencode_relevant_stats(&connection, prior) {
        Ok(stats) if stats.rows > MAX_OPENCODE_RELEVANT_ROWS => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_TOO_LARGE,
                error_text: format!(
                    "{} OpenCode events exceed the {MAX_OPENCODE_RELEVANT_ROWS} event scan ceiling",
                    stats.rows
                ),
            };
        }
        Ok(stats) if stats.max_row_bytes > SCAN_PAGE_MAX_BYTES as u64 => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_TOO_LARGE,
                error_text: format!(
                    "largest OpenCode row is {} bytes, exceeding the {} byte row ceiling",
                    stats.max_row_bytes, SCAN_PAGE_MAX_BYTES
                ),
            };
        }
        Ok(stats) if stats.total_bytes > MAX_OPENCODE_SCAN_BYTES => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_TOO_LARGE,
                error_text: format!(
                    "{} OpenCode source bytes exceed the {} byte scan ceiling",
                    stats.total_bytes, MAX_OPENCODE_SCAN_BYTES
                ),
            };
        }
        Ok(_) => {}
        Err(exc) => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("{exc:#}"),
            };
        }
    }

    match scan_opencode_rows(&connection, prior) {
        Ok((records, new_state, relevant_rows)) => OpenCodeScanOutcome::Scanned {
            records,
            new_state,
            schema_fingerprint,
            relevant_rows,
        },
        Err(OpenCodeScanError::Scan(exc)) => OpenCodeScanOutcome::Failed {
            error_kind: ERROR_KIND_SCAN,
            error_text: format!("{exc:#}"),
        },
        Err(OpenCodeScanError::TooLarge(error_text)) => OpenCodeScanOutcome::Failed {
            error_kind: ERROR_KIND_TOO_LARGE,
            error_text,
        },
    }
}

fn validate_opencode_schema(connection: &Connection) -> std::result::Result<u64, String> {
    let mut schema_material = String::new();
    for (table, required_columns) in [
        ("event", EVENT_COLUMNS),
        ("event_sequence", EVENT_SEQUENCE_COLUMNS),
    ] {
        let schema_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                rusqlite::params![table],
                |row| row.get(0),
            )
            .map_err(|exc| match exc {
                rusqlite::Error::QueryReturnedNoRows => {
                    format!("required table {table} is missing")
                }
                other => other.to_string(),
            })?;
        schema_material.push_str(&schema_sql.unwrap_or_default());
        schema_material.push('\n');

        let names = table_columns(connection, table).map_err(|exc| exc.to_string())?;
        for column in required_columns {
            if !names.iter().any(|name| name == column) {
                return Err(format!("table {table} is missing required column {column}"));
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

#[derive(Default)]
struct OpenCodeRelevantStats {
    rows: u64,
    total_bytes: u64,
    max_row_bytes: u64,
}

fn opencode_relevant_stats(
    connection: &Connection,
    prior: &OpenCodeState,
) -> Result<OpenCodeRelevantStats> {
    let mut stats = OpenCodeRelevantStats::default();
    for aggregate in opencode_aggregate_sequences(connection)? {
        let prior_seq = opencode_scan_from_seq(prior, &aggregate);
        let (rows, total_bytes, max_row_bytes): (i64, i64, i64) = connection
            .query_row(
                "SELECT count(*), coalesce(sum(length(coalesce(data, ''))), 0), \
                 coalesce(max(length(coalesce(data, ''))), 0) \
                 FROM event WHERE aggregate_id = ?1 AND seq > ?2 AND seq <= ?3",
                rusqlite::params![&aggregate.aggregate_id, prior_seq, aggregate.seq],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .with_context(|| {
                format!(
                    "failed measuring OpenCode events for {}",
                    aggregate.aggregate_id
                )
            })?;
        stats.rows = stats.rows.saturating_add(rows.max(0) as u64);
        stats.total_bytes = stats.total_bytes.saturating_add(total_bytes.max(0) as u64);
        stats.max_row_bytes = stats.max_row_bytes.max(max_row_bytes.max(0) as u64);
    }
    Ok(stats)
}

fn scan_opencode_rows(
    connection: &Connection,
    prior: &OpenCodeState,
) -> std::result::Result<(Vec<SyntheticRecord>, OpenCodeState, u64), OpenCodeScanError> {
    let mut records = Vec::new();
    let mut accumulated = OpenCodeAccumulated::default();
    let mut aggregate_sequences = BTreeMap::new();
    let mut session_contexts = BTreeMap::new();
    let mut message_contexts = BTreeMap::new();
    let mut relevant_events = 0u64;
    let mut context_events = 0u64;
    let mut context_bytes = 0u64;

    for aggregate in opencode_aggregate_sequences(connection)? {
        let scan_from_seq = opencode_scan_from_seq(prior, &aggregate);
        if aggregate.seq <= scan_from_seq {
            aggregate_sequences.insert(aggregate.aggregate_id.clone(), scan_from_seq);
            continue;
        }

        let mut last_seq = -1;
        let mut observed_any = false;
        loop {
            let mut page_rows = 0usize;
            let mut page_bytes = 0usize;
            let mut page_capped = false;
            {
                let mut stmt = connection.prepare_cached(
                    "SELECT id, aggregate_id, seq, type, data FROM event \
                     WHERE aggregate_id = ?1 AND seq > ?2 AND seq <= ?3 \
                     ORDER BY seq LIMIT ?4",
                )?;
                let mut rows = stmt.query(rusqlite::params![
                    &aggregate.aggregate_id,
                    last_seq,
                    aggregate.seq,
                    SCAN_PAGE_SIZE as i64
                ])?;
                while let Some(row) = rows.next()? {
                    let event = build_event_row(row)?;
                    if event.data_bytes > SCAN_PAGE_MAX_BYTES {
                        return Err(OpenCodeScanError::TooLarge(format!(
                            "largest OpenCode event is {} bytes, exceeding the {} byte row ceiling",
                            event.data_bytes, SCAN_PAGE_MAX_BYTES
                        )));
                    }
                    page_rows += 1;
                    page_bytes = page_bytes.saturating_add(event.data_bytes);
                    context_events = context_events.saturating_add(1);
                    context_bytes = context_bytes.saturating_add(event.data_bytes as u64);
                    if context_events > MAX_OPENCODE_RELEVANT_ROWS {
                        return Err(OpenCodeScanError::TooLarge(format!(
                            "{context_events} OpenCode context events exceed the {MAX_OPENCODE_RELEVANT_ROWS} event scan ceiling"
                        )));
                    }
                    if context_bytes > MAX_OPENCODE_SCAN_BYTES {
                        return Err(OpenCodeScanError::TooLarge(format!(
                            "{context_bytes} OpenCode context bytes exceed the {MAX_OPENCODE_SCAN_BYTES} byte scan ceiling"
                        )));
                    }

                    update_opencode_context(&event, &mut session_contexts, &mut message_contexts);
                    observed_any = true;
                    if event.seq > scan_from_seq {
                        relevant_events += 1;
                        if relevant_events > MAX_OPENCODE_RELEVANT_ROWS {
                            return Err(OpenCodeScanError::TooLarge(format!(
                                "{relevant_events} OpenCode events exceed the {MAX_OPENCODE_RELEVANT_ROWS} event scan ceiling"
                            )));
                        }
                        apply_opencode_event(&event, &mut accumulated);
                    }
                    last_seq = event.seq;
                    if page_bytes >= SCAN_PAGE_MAX_BYTES || page_rows >= SCAN_PAGE_SIZE {
                        page_capped = true;
                        break;
                    }
                }
            }

            if page_rows == 0 || !page_capped {
                break;
            }
        }
        if observed_any {
            aggregate_sequences.insert(aggregate.aggregate_id.clone(), last_seq);
        } else if scan_from_seq >= 0 {
            aggregate_sequences.insert(aggregate.aggregate_id.clone(), scan_from_seq);
        }
    }

    for (_, record) in accumulated.sessions {
        push_opencode_record("session", &record, &mut records);
    }
    for (_, mut record) in accumulated.messages {
        enrich_message_record(&mut record, &session_contexts);
        push_opencode_record("message", &record, &mut records);
    }
    for (_, mut record) in accumulated.parts {
        enrich_part_record(&mut record, &session_contexts, &message_contexts);
        push_opencode_record("part", &record, &mut records);
    }
    for (_, mut record) in accumulated.session_messages {
        enrich_session_message_record(&mut record, &session_contexts);
        if opencode_record_is_relevant(&record) {
            push_opencode_record("session_message", &record, &mut records);
        }
    }

    let mut new_state = OpenCodeState::fresh();
    new_state.event_scan_complete = true;
    new_state.aggregate_sequences = aggregate_sequences;
    Ok((records, new_state, relevant_events))
}

fn opencode_scan_from_seq(prior: &OpenCodeState, aggregate: &OpenCodeAggregateSequence) -> i64 {
    let prior_seq = prior
        .aggregate_sequences
        .get(&aggregate.aggregate_id)
        .copied()
        .unwrap_or(-1);
    if aggregate.seq < prior_seq {
        -1
    } else {
        prior_seq
    }
}

fn opencode_aggregate_sequences(connection: &Connection) -> Result<Vec<OpenCodeAggregateSequence>> {
    let mut stmt = connection
        .prepare_cached("SELECT aggregate_id, seq FROM event_sequence ORDER BY aggregate_id")?;
    let aggregates = stmt
        .query_map([], |row| {
            Ok(OpenCodeAggregateSequence {
                aggregate_id: row.get(0)?,
                seq: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(aggregates)
}

fn build_event_row(row: &rusqlite::Row<'_>) -> Result<OpenCodeEventRow> {
    let data_text = row.get::<_, String>(4)?;
    let data = serde_json::from_str::<Value>(&data_text).unwrap_or_else(|_| json!(data_text));
    Ok(OpenCodeEventRow {
        id: row.get(0)?,
        aggregate_id: row.get(1)?,
        seq: row.get(2)?,
        event_type: row.get(3)?,
        data,
        data_bytes: data_text.len(),
    })
}

fn update_opencode_context(
    event: &OpenCodeEventRow,
    session_contexts: &mut BTreeMap<String, OpenCodeSessionContext>,
    message_contexts: &mut BTreeMap<String, OpenCodeMessageContext>,
) {
    match event.event_type.as_str() {
        "session.created.1" | "session.updated.1" => {
            let info = event.data.get("info").unwrap_or(&event.data);
            if let Some((id, record)) = build_session_event_record(info) {
                session_contexts.insert(id, session_context_from_record(&record));
            }
        }
        "message.updated.1" => {
            let info = event.data.get("info").unwrap_or(&event.data);
            if let Some((id, record)) = build_message_event_record(info) {
                message_contexts.insert(id, message_context_from_record(&record));
            }
        }
        _ => {}
    }
}

fn apply_opencode_event(event: &OpenCodeEventRow, accumulated: &mut OpenCodeAccumulated) {
    match event.event_type.as_str() {
        "session.created.1" | "session.updated.1" => {
            let info = event.data.get("info").unwrap_or(&event.data);
            if let Some((id, record)) = build_session_event_record(info) {
                accumulated.sessions.insert(id, record);
            }
        }
        "message.updated.1" => {
            let info = event.data.get("info").unwrap_or(&event.data);
            if let Some((id, record)) = build_message_event_record(info) {
                accumulated.messages.insert(id, record);
            }
        }
        "message.part.updated.1" => {
            let part = event.data.get("part").unwrap_or(&event.data);
            let event_time = event.data.get("time").and_then(Value::as_i64).unwrap_or(0);
            if let Some((id, record)) = build_part_event_record(part, event_time) {
                accumulated.parts.insert(id, record);
            }
        }
        "session.next.model.switched.1" => {
            if let Some((id, record)) = build_model_switch_event_record(event) {
                accumulated.session_messages.insert(id, record);
            }
        }
        _ => {}
    }
}

fn build_session_event_record(info: &Value) -> Option<(String, Map<String, Value>)> {
    let id = first_json_text([info.get("id"), info.get("sessionID")]);
    if id.is_empty() {
        return None;
    }
    let mut record = Map::new();
    record.insert("type".to_string(), json!("opencode_session"));
    record.insert("id".to_string(), json!(id.clone()));
    insert_text(
        &mut record,
        "project_id",
        first_json_text([info.get("projectID"), info.get("project_id")]),
    );
    insert_text(
        &mut record,
        "parent_id",
        first_json_text([info.get("parentID"), info.get("parent_id")]),
    );
    insert_text(&mut record, "slug", first_json_text([info.get("slug")]));
    insert_text(
        &mut record,
        "directory",
        first_json_text([info.get("directory")]),
    );
    insert_text(&mut record, "title", first_json_text([info.get("title")]));
    insert_text(
        &mut record,
        "version",
        first_json_text([info.get("version")]),
    );
    insert_text(
        &mut record,
        "share_url",
        first_json_text([info.get("shareURL"), info.get("share_url")]),
    );
    insert_text(
        &mut record,
        "workspace_id",
        first_json_text([info.get("workspaceID"), info.get("workspace_id")]),
    );
    insert_text(&mut record, "path", first_json_text([info.get("path")]));
    insert_text(&mut record, "agent", first_json_text([info.get("agent")]));
    copy_json_value(info, "model", "model", &mut record);
    copy_json_value(info, "metadata", "metadata", &mut record);

    if let Some(summary) = info.get("summary") {
        insert_i64(
            &mut record,
            "summary_additions",
            summary
                .get("additions")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        );
        insert_i64(
            &mut record,
            "summary_deletions",
            summary
                .get("deletions")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        );
        insert_i64(
            &mut record,
            "summary_files",
            summary.get("files").and_then(Value::as_i64).unwrap_or(0),
        );
        if let Some(diffs) = summary.get("diffs") {
            record.insert("summary_diffs".to_string(), diffs.clone());
        }
    }

    insert_i64(
        &mut record,
        "time_created",
        json_i64_path(info, &["/time/created"]).unwrap_or(0),
    );
    insert_i64(
        &mut record,
        "time_updated",
        json_i64_path(info, &["/time/updated", "/time/completed"])
            .or_else(|| json_i64_path(info, &["/time/created"]))
            .unwrap_or(0),
    );
    insert_f64(
        &mut record,
        "cost",
        info.get("cost").and_then(Value::as_f64).unwrap_or(0.0),
    );
    insert_i64(
        &mut record,
        "tokens_input",
        json_i64_path(info, &["/tokens/input"]).unwrap_or(0),
    );
    insert_i64(
        &mut record,
        "tokens_output",
        json_i64_path(info, &["/tokens/output"]).unwrap_or(0),
    );
    insert_i64(
        &mut record,
        "tokens_reasoning",
        json_i64_path(info, &["/tokens/reasoning"]).unwrap_or(0),
    );
    insert_i64(
        &mut record,
        "tokens_cache_read",
        json_i64_path(info, &["/tokens/cache/read"]).unwrap_or(0),
    );
    insert_i64(
        &mut record,
        "tokens_cache_write",
        json_i64_path(info, &["/tokens/cache/write"]).unwrap_or(0),
    );
    Some((id, record))
}

fn build_message_event_record(info: &Value) -> Option<(String, Map<String, Value>)> {
    let id = first_json_text([info.get("id"), info.get("messageID")]);
    let session_id = first_json_text([info.get("sessionID"), info.get("session_id")]);
    if id.is_empty() || session_id.is_empty() {
        return None;
    }
    let mut record = Map::new();
    record.insert("type".to_string(), json!("opencode_message"));
    record.insert("id".to_string(), json!(id.clone()));
    record.insert("session_id".to_string(), json!(session_id));
    insert_i64(
        &mut record,
        "time_created",
        json_i64_path(info, &["/time/created"]).unwrap_or(0),
    );
    insert_i64(
        &mut record,
        "time_updated",
        json_i64_path(info, &["/time/updated", "/time/completed"])
            .or_else(|| json_i64_path(info, &["/time/created"]))
            .unwrap_or(0),
    );
    record.insert("data".to_string(), info.clone());
    copy_message_context_from_value(info, &mut record);
    Some((id, record))
}

fn build_part_event_record(part: &Value, event_time: i64) -> Option<(String, Map<String, Value>)> {
    let id = first_json_text([part.get("id")]);
    let session_id = first_json_text([part.get("sessionID"), part.get("session_id")]);
    let message_id = first_json_text([part.get("messageID"), part.get("message_id")]);
    if id.is_empty() || session_id.is_empty() || message_id.is_empty() {
        return None;
    }
    let created = json_i64_path(part, &["/time/start", "/time/created", "/state/time/start"])
        .unwrap_or(event_time);
    let updated = json_i64_path(
        part,
        &[
            "/time/end",
            "/time/updated",
            "/state/time/end",
            "/state/time/updated",
        ],
    )
    .unwrap_or(event_time.max(created));
    let mut record = Map::new();
    record.insert("type".to_string(), json!("opencode_part"));
    record.insert("id".to_string(), json!(id.clone()));
    record.insert("session_id".to_string(), json!(session_id));
    record.insert("message_id".to_string(), json!(message_id));
    record.insert("time_created".to_string(), json!(created));
    record.insert("time_updated".to_string(), json!(updated));
    record.insert("data".to_string(), part.clone());
    Some((id, record))
}

fn build_model_switch_event_record(
    event: &OpenCodeEventRow,
) -> Option<(String, Map<String, Value>)> {
    let id = first_json_text([
        event.data.get("messageID"),
        Some(&Value::String(event.id.clone())),
    ]);
    let session_id = first_json_text([
        event.data.get("sessionID"),
        Some(&Value::String(event.aggregate_id.clone())),
    ]);
    if id.is_empty() || session_id.is_empty() {
        return None;
    }
    let created = event
        .data
        .get("timestamp")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let data = json!({
        "time": {"created": created},
        "model": event.data.get("model").cloned().unwrap_or(Value::Null)
    });
    Some((
        id.clone(),
        build_session_message_record(id, session_id, "model-switched", created, data),
    ))
}

fn build_session_message_record(
    id: String,
    session_id: String,
    message_type: &str,
    created: i64,
    data: Value,
) -> Map<String, Value> {
    let mut record = Map::new();
    record.insert("type".to_string(), json!("opencode_session_message"));
    record.insert("id".to_string(), json!(id));
    record.insert("session_id".to_string(), json!(session_id));
    record.insert("message_type".to_string(), json!(message_type));
    record.insert("time_created".to_string(), json!(created));
    record.insert("time_updated".to_string(), json!(created));
    record.insert("data".to_string(), data);
    record
}

fn first_json_text<const N: usize>(values: [Option<&Value>; N]) -> String {
    values
        .into_iter()
        .find_map(|value| match value {
            Some(Value::String(text)) if !text.is_empty() => Some(text.clone()),
            Some(Value::Number(number)) => Some(number.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn json_i64_path(value: &Value, pointers: &[&str]) -> Option<i64> {
    pointers.iter().find_map(|pointer| {
        value.pointer(pointer).and_then(Value::as_i64).or_else(|| {
            value
                .pointer(pointer)
                .and_then(Value::as_u64)
                .map(|v| v as i64)
        })
    })
}

fn insert_text(record: &mut Map<String, Value>, key: &str, value: String) {
    if !value.is_empty() {
        record.insert(key.to_string(), json!(value));
    }
}

fn insert_i64(record: &mut Map<String, Value>, key: &str, value: i64) {
    record.insert(key.to_string(), json!(value));
}

fn insert_f64(record: &mut Map<String, Value>, key: &str, value: f64) {
    record.insert(key.to_string(), json!(value));
}

fn copy_json_value(
    source: &Value,
    source_key: &str,
    dest_key: &str,
    record: &mut Map<String, Value>,
) {
    if let Some(value) = source.get(source_key) {
        if !value.is_null() {
            record.insert(dest_key.to_string(), value.clone());
        }
    }
}

fn opencode_record_is_relevant(record: &Map<String, Value>) -> bool {
    if record.get("type").and_then(Value::as_str) != Some("opencode_session_message") {
        return true;
    }
    let message_type = record
        .get("message_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    !OPENCODE_DUPLICATE_SESSION_MESSAGE_TYPES.contains(&message_type)
}

fn copy_message_context_from_value(parsed: &Value, record: &mut Map<String, Value>) {
    copy_message_context_value(parsed, "role", "message_role", record);
    copy_message_context_value(parsed, "agent", "message_agent", record);
    for pointer in ["/modelID", "/model/id", "/model/modelID", "/model/modelId"] {
        copy_message_context_pointer(parsed, pointer, "message_model_id", record);
    }
    for pointer in [
        "/providerID",
        "/model/providerID",
        "/model/providerId",
        "/model/provider",
    ] {
        copy_message_context_pointer(parsed, pointer, "message_provider_id", record);
    }
    if let Some(path) = parsed.get("path") {
        if let Some(cwd) = path.get("cwd").and_then(Value::as_str) {
            if !cwd.is_empty() {
                record.insert("directory".to_string(), json!(cwd));
            }
        }
    }
}

fn session_context_from_record(record: &Map<String, Value>) -> OpenCodeSessionContext {
    OpenCodeSessionContext {
        directory: record
            .get("directory")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        model: record.get("model").cloned(),
    }
}

fn message_context_from_record(record: &Map<String, Value>) -> OpenCodeMessageContext {
    OpenCodeMessageContext {
        role: record
            .get("message_role")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        agent: record
            .get("message_agent")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        model_id: record
            .get("message_model_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        provider_id: record
            .get("message_provider_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        directory: record
            .get("directory")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    }
}

fn enrich_message_record(
    record: &mut Map<String, Value>,
    session_contexts: &BTreeMap<String, OpenCodeSessionContext>,
) {
    let session_id = record
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Some(context) = session_contexts.get(session_id) {
        if !record.contains_key("directory") && !context.directory.is_empty() {
            record.insert("directory".to_string(), json!(context.directory));
        }
        if !record.contains_key("model") {
            if let Some(model) = &context.model {
                record.insert("model".to_string(), model.clone());
            }
        }
    }
}

fn enrich_part_record(
    record: &mut Map<String, Value>,
    session_contexts: &BTreeMap<String, OpenCodeSessionContext>,
    message_contexts: &BTreeMap<String, OpenCodeMessageContext>,
) {
    let session_id = record
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Some(context) = session_contexts.get(session_id) {
        if !record.contains_key("directory") && !context.directory.is_empty() {
            record.insert("directory".to_string(), json!(context.directory));
        }
        if !record.contains_key("model") {
            if let Some(model) = &context.model {
                record.insert("model".to_string(), model.clone());
            }
        }
    }

    let message_id = record
        .get("message_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Some(context) = message_contexts.get(message_id) {
        if !context.role.is_empty() {
            record.insert("message_role".to_string(), json!(context.role));
        }
        if !context.agent.is_empty() {
            record.insert("message_agent".to_string(), json!(context.agent));
        }
        if !context.model_id.is_empty() {
            record.insert("message_model_id".to_string(), json!(context.model_id));
        }
        if !context.provider_id.is_empty() {
            record.insert(
                "message_provider_id".to_string(),
                json!(context.provider_id),
            );
        }
        if !context.directory.is_empty() {
            record.insert("directory".to_string(), json!(context.directory));
        }
    }
}

fn enrich_session_message_record(
    record: &mut Map<String, Value>,
    session_contexts: &BTreeMap<String, OpenCodeSessionContext>,
) {
    let session_id = record
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Some(context) = session_contexts.get(session_id) {
        if !record.contains_key("directory") && !context.directory.is_empty() {
            record.insert("directory".to_string(), json!(context.directory));
        }
        if !record.contains_key("model") {
            if let Some(model) = &context.model {
                record.insert("model".to_string(), model.clone());
            }
        }
    }
}

fn copy_message_context_value(
    parsed: &Value,
    source: &str,
    dest: &str,
    record: &mut Map<String, Value>,
) {
    if record.contains_key(dest) {
        return;
    }
    if let Some(text) = parsed.get(source).and_then(Value::as_str) {
        if !text.is_empty() {
            record.insert(dest.to_string(), json!(text));
        }
    }
}

fn copy_message_context_pointer(
    parsed: &Value,
    pointer: &str,
    dest: &str,
    record: &mut Map<String, Value>,
) {
    if record.contains_key(dest) {
        return;
    }
    if let Some(text) = parsed.pointer(pointer).and_then(Value::as_str) {
        if !text.is_empty() {
            record.insert(dest.to_string(), json!(text));
        }
    }
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
    let mut value = Value::Object(record.clone());
    elide_binary_like_strings(&mut value);
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

fn elide_binary_like_strings(value: &mut Value) {
    match value {
        Value::String(text)
            if text.chars().count() > OPENCODE_LONG_BINARY_STRING_CHARS
                && looks_binary_like(text) =>
        {
            let total = text.chars().count();
            let prefix: String = text.chars().take(256).collect();
            *value = Value::String(format!("{prefix}... <moraine: elided {total} chars>"));
        }
        Value::Array(items) => {
            for item in items {
                elide_binary_like_strings(item);
            }
        }
        Value::Object(map) => {
            for (_, item) in map.iter_mut() {
                elide_binary_like_strings(item);
            }
        }
        _ => {}
    }
}

fn looks_binary_like(text: &str) -> bool {
    if text.starts_with("data:image/") || text.starts_with("data:application/octet-stream") {
        return true;
    }

    let mut sampled = 0usize;
    let mut allowed = 0usize;
    let mut whitespace = 0usize;
    for ch in text.chars().take(4096) {
        sampled += 1;
        if ch.is_ascii_whitespace() {
            whitespace += 1;
        }
        if ch.is_ascii_alphanumeric() || matches!(ch, '+' | '/' | '=' | '_' | '-' | '.') {
            allowed += 1;
        }
    }
    sampled > 0 && whitespace == 0 && allowed * 100 >= sampled * 95
}

#[cfg(test)]
mod tests;
