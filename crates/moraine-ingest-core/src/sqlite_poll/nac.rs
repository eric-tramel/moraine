use crate::checkpoint::checkpoint_key;
use crate::dispatch::source_inode_for_file;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use crate::sources::nac::canonical_mcp_tool_name;
use crate::{Metrics, SinkMessage, WorkItem};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use moraine_config::{AppConfig, SOURCE_FORMAT_NAC_SQLITE};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

use super::{
    hash_str, open_read_only, stat_fingerprint, truncate_chars_local, StatFingerprint,
    SyntheticRecord, VolatilePollMap, ERROR_KIND_OPEN, ERROR_KIND_SCAN, ERROR_KIND_SCHEMA,
    ERROR_KIND_TOO_LARGE, SCAN_PAGE_MAX_BYTES, SCAN_PAGE_SIZE,
};

const NAC_CURSOR_VERSION: u32 = 1;
const MAX_NAC_SESSIONS: u64 = 10_000;
const MAX_NAC_EPISODES: u64 = 100_000;
const MAX_NAC_SCAN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_NAC_SYNTHETIC_RECORDS: usize = 200_000;
const ERROR_KIND_NORMALIZED_ROW_TOO_LARGE: &str = "nac_normalized_row_too_large";
const MAX_NAC_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;
const MAX_NAC_TEXT_CHARS: usize = 200_000;
const ERROR_KIND_MIXED_SNAPSHOT: &str = "sqlite_mixed_snapshot";

const REQUIRED_SESSION_COLUMNS: &[&str] = &[
    "session_id",
    "cwd",
    "model",
    "base_url",
    "messages_json",
    "created_at",
    "updated_at",
];
const REQUIRED_EPISODE_COLUMNS: &[&str] = &[
    "id",
    "thread_name",
    "session_id",
    "action",
    "content",
    "created_at",
];

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct NacSessionCursor {
    metadata_hash: String,
    created_at: String,
    #[serde(default)]
    part_hashes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NacState {
    version: u32,
    format: String,
    #[serde(default)]
    stat: StatFingerprint,
    #[serde(default)]
    schema_fingerprint: u64,
    #[serde(default)]
    sessions: BTreeMap<String, NacSessionCursor>,
    #[serde(default)]
    episode_high_water: i64,
    #[serde(default)]
    worker_threads: BTreeSet<String>,
    #[serde(default)]
    project_exclusions_hash: u64,
    #[serde(default)]
    last_error: String,
}

impl Default for NacState {
    fn default() -> Self {
        Self {
            version: NAC_CURSOR_VERSION,
            format: SOURCE_FORMAT_NAC_SQLITE.to_string(),
            stat: StatFingerprint::default(),
            schema_fingerprint: 0,
            sessions: BTreeMap::new(),
            episode_high_water: 0,
            worker_threads: BTreeSet::new(),
            project_exclusions_hash: 0,
            last_error: String::new(),
        }
    }
}

impl NacState {
    fn parse(raw: &str) -> Self {
        match serde_json::from_str::<Self>(raw) {
            Ok(state)
                if state.version == NAC_CURSOR_VERSION
                    && state.format == SOURCE_FORMAT_NAC_SQLITE =>
            {
                state
            }
            _ => Self::default(),
        }
    }

    fn serialize(&self) -> Result<String> {
        let raw = serde_json::to_string(self).context("failed to serialize NAC cursor")?;
        if raw.len() > MAX_NAC_CHECKPOINT_BYTES {
            anyhow::bail!(
                "NAC cursor is {} bytes, exceeding the {} byte checkpoint ceiling",
                raw.len(),
                MAX_NAC_CHECKPOINT_BYTES
            );
        }
        Ok(raw)
    }
}

#[derive(Debug, Clone)]
struct NacSessionRow {
    raw_session_id: String,
    cwd: String,
    cwd_scope: CwdScope,
    model: String,
    base_url: String,
    backend: String,
    reasoning_effort: String,
    sandbox: Value,
    messages: Value,
    last_response_duration_ms: Option<u64>,
    previous_response_duration_ms: Option<u64>,
    response_durations: Value,
    token_usages: Value,
    created_at: String,
    updated_at: String,
    estimated_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CwdScope {
    Local,
    Remote,
    Unknown,
}

impl CwdScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
enum NacScanOutcome {
    Scanned {
        records: Vec<SyntheticRecord>,
        state: NacState,
        schema_fingerprint: u64,
        relevant_rows: u64,
    },
    Failed {
        error_kind: &'static str,
        error_text: String,
    },
}

pub(crate) async fn process_nac_sqlite_db(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: &VolatilePollMap,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let source_file = work.path.clone();
    let Some(current_stat) = stat_fingerprint(&source_file) else {
        debug!("nac_sqlite db missing, skipping: {}", source_file);
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
    let had_committed = committed.is_some();
    let checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.clone(),
        source_inode: inode,
        source_generation: 1,
        status: "active".to_string(),
        ..Default::default()
    });
    let committed_state = NacState::parse(&checkpoint.cursor_json);
    let generation_changed = checkpoint.source_inode != inode;
    let current_exclusions_hash = super::project_exclusions_hash(config);
    let exclusions_changed = committed_state.project_exclusions_hash != current_exclusions_hash;
    let source_generation = if generation_changed {
        checkpoint.source_generation.saturating_add(1).max(1)
    } else {
        checkpoint.source_generation
    };
    let mut scan_state = if generation_changed || exclusions_changed {
        NacState::default()
    } else {
        committed_state.clone()
    };
    scan_state.project_exclusions_hash = current_exclusions_hash;
    if !generation_changed
        && !exclusions_changed
        && scan_state.stat == current_stat
        && scan_state.last_error.is_empty()
        && scan_state.schema_fingerprint != 0
    {
        return Ok(());
    }
    if !exclusions_changed && poll_state.should_skip_poll(&cp_key, source_generation, &current_stat)
    {
        return Ok(());
    }

    let scan_path = source_file.clone();
    let scan_source_name = work.source_name.clone();
    let prior_scan_state = scan_state.clone();
    let mut outcome = tokio::task::spawn_blocking(move || {
        scan_nac_database(
            &scan_path,
            &scan_source_name,
            source_generation,
            inode,
            current_stat,
            &prior_scan_state,
        )
    })
    .await
    .context("nac_sqlite scan task panicked")?;

    let oversized_row = match &mut outcome {
        NacScanOutcome::Scanned { records, .. } => {
            link_tool_responses(records, &source_file, source_generation);
            records.iter().find_map(|synthetic| {
                if crate::dispatch::record_project_dir_is_excluded(
                    config,
                    &work.harness,
                    &synthetic.record,
                    &synthetic.project_dir,
                ) {
                    return None;
                }
                normalize_record(
                    &synthetic.record,
                    &work.source_name,
                    &work.harness,
                    &source_file,
                    inode,
                    source_generation,
                    synthetic.source_line_no,
                    synthetic.source_offset,
                    "",
                    "",
                    "",
                )
                .ok()
                .and_then(|normalized| {
                    crate::dispatch::largest_serialized_normalized_row(&normalized)
                })
                .filter(|row_size| {
                    row_size.bytes > crate::dispatch::CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT
                })
            })
        }
        NacScanOutcome::Failed { .. } => None,
    };
    if let Some(row_size) = oversized_row {
        outcome = NacScanOutcome::Failed {
            error_kind: ERROR_KIND_NORMALIZED_ROW_TOO_LARGE,
            error_text: format!(
                "{} row serializes to {} bytes, exceeding the {} byte ClickHouse JSON object limit; scan rejected before insert",
                row_size.table,
                row_size.bytes,
                crate::dispatch::CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT
            ),
        };
    }

    match outcome {
        NacScanOutcome::Scanned {
            records,
            state: mut new_state,
            schema_fingerprint,
            relevant_rows,
        } => {
            new_state.project_exclusions_hash = current_exclusions_hash;
            let cursor_json = new_state.serialize()?;
            let prior_state_covered = {
                let mut prior = scan_state.clone();
                prior.stat = current_stat;
                prior.schema_fingerprint = schema_fingerprint;
                prior
            };
            let scan_is_noop = had_committed
                && !generation_changed
                && !exclusions_changed
                && records.is_empty()
                && checkpoint.status == "active"
                && new_state == prior_state_covered
                && schema_fingerprint == checkpoint.schema_fingerprint;
            if scan_is_noop {
                poll_state.record_noop_scan(&cp_key, source_generation, current_stat);
                return Ok(());
            }

            let mut batch = RowBatch::default();
            for synthetic in &records {
                if crate::dispatch::record_project_dir_is_excluded(
                    config,
                    &work.harness,
                    &synthetic.record,
                    &synthetic.project_dir,
                ) {
                    continue;
                }
                let raw_json =
                    serde_json::to_string(&synthetic.record).unwrap_or_else(|_| "{}".to_string());
                match normalize_record(
                    &synthetic.record,
                    &work.source_name,
                    &work.harness,
                    &source_file,
                    inode,
                    source_generation,
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
                    Err(exc) => batch.push_error_row(json!({
                        "source_name": work.source_name,
                        "harness": work.harness,
                        "source_file": source_file,
                        "source_inode": inode,
                        "source_generation": source_generation,
                        "source_line_no": synthetic.source_line_no,
                        "source_offset": synthetic.source_offset,
                        "error_kind": "normalize_error",
                        "error_text": exc.to_string(),
                        "raw_fragment": truncate_chars_local(&raw_json, 20_000),
                    })),
                }
                if batch.exceeds_limits(config.ingest.batch_size, config.ingest.max_batch_bytes) {
                    let chunk = batch.drain_to_chunk();
                    sink_tx
                        .send(SinkMessage::Batch(chunk))
                        .await
                        .context("sink channel closed while sending nac_sqlite chunk")?;
                }
            }

            let emitted = records.len();
            batch.checkpoint = Some(Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation,
                last_offset: checkpoint.last_offset.saturating_add(1),
                last_line_no: relevant_rows,
                status: "active".to_string(),
                cursor_json,
                source_fingerprint: inode,
                schema_fingerprint,
            });
            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending final nac_sqlite batch")?;
            poll_state.clear(&cp_key);
            if emitted > 0 {
                debug!(
                    "{}:{} nac_sqlite emitted {} changed records ({} relevant rows)",
                    work.source_name, source_file, emitted, relevant_rows
                );
            }
            let _ = metrics;
            Ok(())
        }
        NacScanOutcome::Failed {
            error_kind,
            error_text,
        } => {
            poll_state.record_failed_scan(&cp_key);
            if committed_state.last_error == error_kind {
                return Ok(());
            }
            warn!(
                "nac_sqlite poll failed for {}: {} ({})",
                source_file, error_kind, error_text
            );
            let mut batch = RowBatch::default();
            batch.push_error_row(json!({
                "source_name": work.source_name,
                "harness": work.harness,
                "source_file": source_file,
                "source_inode": inode,
                "source_generation": source_generation,
                "source_line_no": 0u64,
                "source_offset": 0u64,
                "error_kind": error_kind,
                "error_text": error_text,
                "raw_fragment": "",
            }));
            let mut failed_state = committed_state;
            failed_state.last_error = error_kind.to_string();
            batch.checkpoint = Some(Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: checkpoint.source_inode,
                source_generation: checkpoint.source_generation,
                last_offset: checkpoint.last_offset,
                last_line_no: checkpoint.last_line_no,
                status: "active".to_string(),
                cursor_json: failed_state.serialize()?,
                source_fingerprint: checkpoint.source_fingerprint,
                schema_fingerprint: checkpoint.schema_fingerprint,
            });
            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending nac_sqlite error batch")?;
            Ok(())
        }
    }
}

fn scan_nac_database(
    db_path: &str,
    source_name: &str,
    source_generation: u32,
    expected_inode: u64,
    pre_scan_stat: StatFingerprint,
    prior: &NacState,
) -> NacScanOutcome {
    let connection = match open_read_only(db_path) {
        Ok(connection) => connection,
        Err(exc) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_OPEN,
                error_text: format!("{exc:#}"),
            }
        }
    };
    let opened_inode = match std::fs::metadata(db_path) {
        Ok(metadata) => source_inode_for_file(db_path, &metadata),
        Err(exc) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_MIXED_SNAPSHOT,
                error_text: format!("NAC database identity unavailable after opening: {exc}"),
            }
        }
    };
    if opened_inode != expected_inode {
        return NacScanOutcome::Failed {
            error_kind: ERROR_KIND_MIXED_SNAPSHOT,
            error_text:
                "NAC database was replaced before the scan opened; retrying with a new generation"
                    .to_string(),
        };
    }
    // Opening a WAL database can create or touch its reader-owned `-shm`
    // sidecar even through a read-only connection. Treat that stable opened
    // state as the scan baseline; comparing against the caller's pre-open
    // fingerprint would misclassify every scan as concurrent writer churn on
    // platforms where SQLite removes the sidecars after the last close.
    let opened_stat = stat_fingerprint(db_path).unwrap_or(pre_scan_stat);
    let schema = match inspect_schema(&connection) {
        Ok(schema) => schema,
        Err(error_text) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCHEMA,
                error_text,
            }
        }
    };
    let data_version_before = match data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read NAC pre-scan data_version: {exc:#}"),
            }
        }
    };
    let result = scan_nac_rows(
        &connection,
        db_path,
        source_name,
        source_generation,
        &schema,
        prior,
    );
    let (records, mut state, relevant_rows) = match result {
        Ok(value) => value,
        Err(NacScanError::Scan(exc)) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("{exc:#}"),
            }
        }
        Err(NacScanError::TooLarge(error_text)) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_TOO_LARGE,
                error_text,
            }
        }
    };
    let data_version_after = match data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read NAC post-scan data_version: {exc:#}"),
            }
        }
    };
    if data_version_before != data_version_after || stat_fingerprint(db_path) != Some(opened_stat) {
        return NacScanOutcome::Failed {
            error_kind: ERROR_KIND_MIXED_SNAPSHOT,
            error_text:
                "NAC database changed during the paged scan; retrying without advancing the cursor"
                    .to_string(),
        };
    }
    state.stat = pre_scan_stat;
    state.schema_fingerprint = schema.fingerprint;
    state.last_error.clear();
    if let Err(exc) = state.serialize() {
        return NacScanOutcome::Failed {
            error_kind: ERROR_KIND_TOO_LARGE,
            error_text: format!("NAC cursor failed size/serialization checks: {exc:#}"),
        };
    }
    NacScanOutcome::Scanned {
        records,
        state,
        schema_fingerprint: schema.fingerprint,
        relevant_rows,
    }
}

#[derive(Debug)]
struct NacSchema {
    session_columns: BTreeSet<String>,
    fingerprint: u64,
}

fn inspect_schema(connection: &Connection) -> std::result::Result<NacSchema, String> {
    let mut material = String::new();
    let mut session_columns = BTreeSet::new();
    for (table, required) in [
        ("sessions", REQUIRED_SESSION_COLUMNS),
        ("episodes", REQUIRED_EPISODE_COLUMNS),
    ] {
        let sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .map_err(|exc| match exc {
                rusqlite::Error::QueryReturnedNoRows => {
                    format!("required table {table} is missing")
                }
                other => other.to_string(),
            })?;
        material.push_str(&sql.unwrap_or_default());
        material.push('\n');
        let columns = table_columns(connection, table).map_err(|exc| exc.to_string())?;
        for column in required {
            if !columns.contains(*column) {
                return Err(format!("table {table} is missing required column {column}"));
            }
        }
        if table == "sessions" {
            session_columns = columns;
        }
    }
    Ok(NacSchema {
        session_columns,
        fingerprint: hash_str(&material),
    })
}

fn table_columns(connection: &Connection, table: &str) -> Result<BTreeSet<String>> {
    let mut stmt = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("failed to inspect {table} columns"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    Ok(columns)
}

fn data_version(connection: &Connection) -> Result<i64> {
    connection
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .context("failed to query PRAGMA data_version")
}

#[derive(Debug)]
enum NacScanError {
    Scan(anyhow::Error),
    TooLarge(String),
}

impl From<anyhow::Error> for NacScanError {
    fn from(value: anyhow::Error) -> Self {
        Self::Scan(value)
    }
}

impl From<rusqlite::Error> for NacScanError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Scan(value.into())
    }
}

fn scan_nac_rows(
    connection: &Connection,
    db_path: &str,
    source_name: &str,
    source_generation: u32,
    schema: &NacSchema,
    prior: &NacState,
) -> std::result::Result<(Vec<SyntheticRecord>, NacState, u64), NacScanError> {
    let canonical_db = std::fs::canonicalize(db_path)
        .unwrap_or_else(|_| std::path::PathBuf::from(db_path))
        .to_string_lossy()
        .to_string();
    let namespace = namespace_prefix(source_name, &canonical_db, source_generation);
    let projection = session_projection(&schema.session_columns);
    let mut records = Vec::new();
    let mut next_state = NacState {
        episode_high_water: prior.episode_high_water,
        worker_threads: prior.worker_threads.clone(),
        project_exclusions_hash: prior.project_exclusions_hash,
        ..NacState::default()
    };
    let mut contexts = BTreeMap::<String, NacSessionRow>::new();
    let mut last_session_id = String::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;

    loop {
        let sql = format!(
            "SELECT {projection} FROM sessions WHERE session_id > ?1 ORDER BY session_id LIMIT ?2"
        );
        let mut stmt = connection.prepare_cached(&sql)?;
        let mut rows = stmt.query(params![&last_session_id, SCAN_PAGE_SIZE as i64])?;
        let mut page_rows = 0usize;
        while let Some(row) = rows.next()? {
            let session = read_session_row(row, &schema.session_columns)?;
            page_rows += 1;
            total_rows = total_rows.saturating_add(1);
            if total_rows > MAX_NAC_SESSIONS {
                return Err(NacScanError::TooLarge(format!(
                    "{total_rows} NAC sessions exceed the {MAX_NAC_SESSIONS} session scan ceiling"
                )));
            }
            let row_bytes = estimated_session_bytes(&session);
            if row_bytes > SCAN_PAGE_MAX_BYTES {
                return Err(NacScanError::TooLarge(format!(
                    "NAC session {} is {row_bytes} bytes, exceeding the {SCAN_PAGE_MAX_BYTES} byte row ceiling",
                    session.raw_session_id
                )));
            }
            total_bytes = total_bytes.saturating_add(row_bytes as u64);
            if total_bytes > MAX_NAC_SCAN_BYTES {
                return Err(NacScanError::TooLarge(format!(
                    "NAC scan bytes exceed the {MAX_NAC_SCAN_BYTES} byte ceiling"
                )));
            }
            let normalized_session_id = format!("{namespace}:{}", session.raw_session_id);
            let (mut session_records, cursor) = synthesize_session(
                &session,
                &normalized_session_id,
                prior.sessions.get(&session.raw_session_id),
                MAX_NAC_SYNTHETIC_RECORDS.saturating_sub(records.len()),
            )?;
            records.append(&mut session_records);
            next_state
                .sessions
                .insert(session.raw_session_id.clone(), cursor);
            last_session_id = session.raw_session_id.clone();
            contexts.insert(session.raw_session_id.clone(), session);
            enforce_record_limit(records.len())?;
        }
        if page_rows < SCAN_PAGE_SIZE {
            break;
        }
    }

    next_state.worker_threads.retain(|key| {
        key.split_once('\n')
            .map(|(session_id, _)| contexts.contains_key(session_id))
            .unwrap_or(false)
    });
    let max_episode_id: i64 =
        connection.query_row("SELECT coalesce(max(id), 0) FROM episodes", [], |row| {
            row.get(0)
        })?;
    let scan_from = if max_episode_id < prior.episode_high_water {
        next_state.worker_threads.clear();
        0
    } else {
        prior.episode_high_water
    };
    let mut last_episode_id = scan_from;
    loop {
        let mut stmt = connection.prepare_cached(
            "SELECT id, thread_name, session_id, action, content, created_at \
             FROM episodes WHERE id > ?1 ORDER BY id LIMIT ?2",
        )?;
        let mut rows = stmt.query(params![last_episode_id, SCAN_PAGE_SIZE as i64])?;
        let mut page_rows = 0usize;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let thread_name: String = row.get(1)?;
            let raw_session_id: String = row.get(2)?;
            let created_at_raw: String = row.get(5)?;
            page_rows += 1;
            total_rows = total_rows.saturating_add(1);
            if total_rows > MAX_NAC_SESSIONS.saturating_add(MAX_NAC_EPISODES) {
                return Err(NacScanError::TooLarge(format!(
                    "NAC session and episode rows exceed the {} row ceiling",
                    MAX_NAC_SESSIONS + MAX_NAC_EPISODES
                )));
            }
            let mut row_bytes = thread_name.len() + raw_session_id.len() + created_at_raw.len();
            if row_bytes > SCAN_PAGE_MAX_BYTES {
                return Err(NacScanError::TooLarge(format!(
                    "NAC episode {id} is {row_bytes} bytes, exceeding the {SCAN_PAGE_MAX_BYTES} byte row ceiling"
                )));
            }
            total_bytes = total_bytes.saturating_add(row_bytes as u64);
            if total_bytes > MAX_NAC_SCAN_BYTES {
                return Err(NacScanError::TooLarge(format!(
                    "NAC scan bytes exceed the {MAX_NAC_SCAN_BYTES} byte ceiling"
                )));
            }
            let parent = contexts.get(&raw_session_id).ok_or_else(|| {
                NacScanError::Scan(anyhow::anyhow!(
                    "episode {id} references missing session {raw_session_id}"
                ))
            })?;
            let parent_id = format!("{namespace}:{raw_session_id}");
            let worker_id = format!(
                "{parent_id}:nac-worker:{}",
                short_sha256(thread_name.as_bytes())
            );
            let thread_key = format!("{raw_session_id}\n{thread_name}");
            if next_state.worker_threads.insert(thread_key) {
                records.push(worker_session_meta_record(
                    parent,
                    &raw_session_id,
                    &parent_id,
                    &worker_id,
                    &thread_name,
                    id as u64,
                    &created_at_raw,
                )?);
            }
            let action: String = row.get(3)?;
            let content: String = row.get(4)?;
            row_bytes = row_bytes.saturating_add(action.len() + content.len());
            if row_bytes > SCAN_PAGE_MAX_BYTES {
                return Err(NacScanError::TooLarge(format!(
                    "NAC episode {id} is {row_bytes} bytes, exceeding the {SCAN_PAGE_MAX_BYTES} byte row ceiling"
                )));
            }
            total_bytes = total_bytes.saturating_add((action.len() + content.len()) as u64);
            if total_bytes > MAX_NAC_SCAN_BYTES {
                return Err(NacScanError::TooLarge(format!(
                    "NAC scan bytes exceed the {MAX_NAC_SCAN_BYTES} byte ceiling"
                )));
            }
            let timestamp = normalize_nac_timestamp(&created_at_raw, false)?;
            records.push(worker_event_record(
                parent,
                &raw_session_id,
                &worker_id,
                &thread_name,
                id as u64,
                0,
                "action",
                &action,
                &timestamp,
            ));
            records.push(worker_event_record(
                parent,
                &raw_session_id,
                &worker_id,
                &thread_name,
                id as u64,
                1,
                "response",
                &content,
                &timestamp,
            ));
            last_episode_id = id;
            next_state.episode_high_water = id;
            enforce_record_limit(records.len())?;
        }
        if page_rows < SCAN_PAGE_SIZE {
            break;
        }
    }

    Ok((records, next_state, total_rows))
}

fn session_projection(columns: &BTreeSet<String>) -> String {
    let optional = |name: &str, fallback: &str| {
        if columns.contains(name) {
            name.to_string()
        } else {
            format!("{fallback} AS {name}")
        }
    };
    let remote = if columns.contains("host_id") {
        "CASE WHEN host_id IS NOT NULL AND host_id <> '' THEN 1 ELSE 0 END AS is_remote".to_string()
    } else {
        "NULL AS is_remote".to_string()
    };
    [
        "session_id".to_string(),
        "cwd".to_string(),
        "model".to_string(),
        "base_url".to_string(),
        optional("backend", "''"),
        optional("reasoning_effort", "''"),
        optional("sandbox_json", "NULL"),
        "messages_json".to_string(),
        optional("last_response_duration_ms", "NULL"),
        optional("previous_response_duration_ms", "NULL"),
        optional("response_durations_json", "NULL"),
        optional("token_usages_json", "NULL"),
        "created_at".to_string(),
        "updated_at".to_string(),
        remote,
    ]
    .join(", ")
}

fn read_session_row(
    row: &rusqlite::Row<'_>,
    columns: &BTreeSet<String>,
) -> std::result::Result<NacSessionRow, NacScanError> {
    let raw_session_id: String = row.get(0)?;
    let cwd: String = row.get(1)?;
    let model: String = row.get(2)?;
    let base_url: String = row.get(3)?;
    let backend = row.get::<_, Option<String>>(4)?.unwrap_or_default();
    let reasoning_effort = row.get::<_, Option<String>>(5)?.unwrap_or_default();
    let sandbox_raw: Option<String> = row.get(6)?;
    let messages_raw: String = row.get(7)?;
    let last_response_duration_ms = row.get::<_, Option<i64>>(8)?.map(|v| v.max(0) as u64);
    let previous_response_duration_ms = row.get::<_, Option<i64>>(9)?.map(|v| v.max(0) as u64);
    let durations_raw: Option<String> = row.get(10)?;
    let usages_raw: Option<String> = row.get(11)?;
    let created_at: String = row.get(12)?;
    let updated_at: String = row.get(13)?;
    let remote: Option<i64> = row.get(14)?;
    let estimated_bytes = raw_session_id.len()
        + cwd.len()
        + model.len()
        + base_url.len()
        + backend.len()
        + reasoning_effort.len()
        + sandbox_raw.as_ref().map_or(0, String::len)
        + messages_raw.len()
        + durations_raw.as_ref().map_or(0, String::len)
        + usages_raw.as_ref().map_or(0, String::len)
        + created_at.len()
        + updated_at.len()
        + 3 * std::mem::size_of::<i64>();
    let parse_json = |label: &str, raw: Option<&str>, default: Value| {
        let Some(raw) = raw.filter(|raw| !raw.trim().is_empty()) else {
            return Ok(default);
        };
        serde_json::from_str(raw).with_context(|| format!("invalid NAC {label} JSON"))
    };
    let messages: Value = parse_json("messages", Some(&messages_raw), Value::Array(Vec::new()))?;
    if !messages.is_array() {
        return Err(NacScanError::Scan(anyhow::anyhow!(
            "NAC messages_json must contain an array"
        )));
    }
    Ok(NacSessionRow {
        raw_session_id,
        cwd,
        cwd_scope: if !columns.contains("host_id") {
            CwdScope::Unknown
        } else if remote == Some(1) {
            CwdScope::Remote
        } else {
            CwdScope::Local
        },
        model,
        base_url,
        backend,
        reasoning_effort,
        sandbox: parse_json("sandbox", sandbox_raw.as_deref(), Value::Null)?,
        messages,
        last_response_duration_ms,
        previous_response_duration_ms,
        response_durations: parse_json(
            "response durations",
            durations_raw.as_deref(),
            Value::Array(Vec::new()),
        )?,
        token_usages: parse_json(
            "token usages",
            usages_raw.as_deref(),
            Value::Array(Vec::new()),
        )?,
        created_at,
        updated_at,
        estimated_bytes,
    })
}

fn estimated_session_bytes(session: &NacSessionRow) -> usize {
    session.estimated_bytes
}

fn synthesize_session(
    session: &NacSessionRow,
    normalized_session_id: &str,
    prior: Option<&NacSessionCursor>,
    record_budget: usize,
) -> std::result::Result<(Vec<SyntheticRecord>, NacSessionCursor), NacScanError> {
    let created_at = normalize_nac_timestamp(&session.created_at, true)?;
    let updated_at = normalize_nac_timestamp(&session.updated_at, true)?;
    let metadata = session_metadata_value(session, normalized_session_id, &created_at, &updated_at);
    let metadata_hash = value_hash(&metadata);
    let mut records = Vec::new();
    let created_changed = prior
        .map(|cursor| cursor.created_at != created_at)
        .unwrap_or(true);
    if prior.map(|cursor| cursor.metadata_hash.as_str()) != Some(metadata_hash.as_str()) {
        if records.len() >= record_budget {
            return Err(NacScanError::TooLarge(format!(
                "NAC synthetic records exceed the {MAX_NAC_SYNTHETIC_RECORDS} record ceiling"
            )));
        }
        records.push(SyntheticRecord {
            record: metadata,
            project_dir: project_dir_for(session),
            source_line_no: 0,
            source_offset: 0,
        });
    }

    let logical_parts = logical_message_parts(
        session,
        normalized_session_id,
        &created_at,
        record_budget.saturating_sub(records.len()),
    )?;
    let mut part_hashes = BTreeMap::new();
    for (logical_id, record, line, offset) in logical_parts {
        let hash = value_hash(&record);
        let changed = created_changed
            || prior
                .and_then(|cursor| cursor.part_hashes.get(&logical_id))
                .map(String::as_str)
                != Some(hash.as_str());
        part_hashes.insert(logical_id, hash);
        if changed {
            records.push(SyntheticRecord {
                record,
                project_dir: project_dir_for(session),
                source_line_no: line,
                source_offset: offset,
            });
        }
    }
    Ok((
        records,
        NacSessionCursor {
            metadata_hash,
            created_at,
            part_hashes,
        },
    ))
}

fn session_metadata_value(
    session: &NacSessionRow,
    normalized_session_id: &str,
    created_at: &str,
    updated_at: &str,
) -> Value {
    json!({
        "type": "session_meta",
        "logical_id": format!("session:{}:meta", session.raw_session_id),
        "session_id": normalized_session_id,
        "raw_session_id": session.raw_session_id,
        "timestamp": created_at,
        "created_at": created_at,
        "updated_at": updated_at,
        "cwd": truncate_chars_local(&session.cwd, MAX_NAC_TEXT_CHARS),
        "cwd_scope": session.cwd_scope.as_str(),
        "model": session.model,
        "base_url": session.base_url,
        "backend": session.backend,
        "reasoning_effort": session.reasoning_effort,
        "sandbox": session.sandbox,
        "last_response_duration_ms": session.last_response_duration_ms,
        "previous_response_duration_ms": session.previous_response_duration_ms,
        "response_durations_ms": session.response_durations,
        "token_usages": session.token_usages,
        "message_count": session.messages.as_array().map_or(0, Vec::len),
    })
}

#[derive(Clone)]
struct ToolRequestContext {
    tool_name: String,
    raw_name: String,
    logical_id: String,
    source_line_no: u64,
    source_offset: u64,
}

fn push_logical_part(
    parts: &mut Vec<(String, Value, u64, u64)>,
    part: (String, Value, u64, u64),
    record_budget: usize,
) -> std::result::Result<(), NacScanError> {
    if parts.len() >= record_budget {
        return Err(NacScanError::TooLarge(format!(
            "NAC synthetic records exceed the {MAX_NAC_SYNTHETIC_RECORDS} record ceiling"
        )));
    }
    parts.push(part);
    Ok(())
}

fn logical_message_parts(
    session: &NacSessionRow,
    normalized_session_id: &str,
    timestamp: &str,
    record_budget: usize,
) -> std::result::Result<Vec<(String, Value, u64, u64)>, NacScanError> {
    let messages = session
        .messages
        .as_array()
        .expect("validated message array");
    let terminals = completed_terminal_indices(messages);
    let usages = session.token_usages.as_array().cloned().unwrap_or_default();
    let durations = session
        .response_durations
        .as_array()
        .cloned()
        .unwrap_or_default();
    let usage_aligned = usages.is_empty() || usages.len() == terminals.len();
    let durations_aligned = durations.is_empty() || durations.len() == terminals.len();
    let align_metrics = usage_aligned && durations_aligned;
    let terminal_slots = terminals
        .iter()
        .enumerate()
        .map(|(slot, index)| (*index, slot))
        .collect::<BTreeMap<_, _>>();
    let mut parts = Vec::new();
    let mut turn_index = 0u32;
    let mut preceding_tools = BTreeMap::<String, ToolRequestContext>::new();

    for (message_index, message) in messages.iter().enumerate() {
        let object = message.as_object().ok_or_else(|| {
            NacScanError::Scan(anyhow::anyhow!(
                "NAC message {message_index} must be an object"
            ))
        })?;
        let role = value_string(object.get("role"));
        if role == "user" {
            turn_index = turn_index.saturating_add(1);
        }
        let offset = message_index as u64 + 1;
        let mut base = Map::new();
        base.insert("session_id".to_string(), json!(normalized_session_id));
        base.insert("raw_session_id".to_string(), json!(session.raw_session_id));
        base.insert("timestamp".to_string(), json!(timestamp));
        base.insert(
            "cwd".to_string(),
            json!(truncate_chars_local(&session.cwd, MAX_NAC_TEXT_CHARS)),
        );
        base.insert("cwd_scope".to_string(), json!(session.cwd_scope.as_str()));
        base.insert("model".to_string(), json!(session.model));
        base.insert("base_url".to_string(), json!(session.base_url));
        base.insert("turn_index".to_string(), json!(turn_index));

        if role == "assistant" {
            if let Some(reasoning) = optional_string(object.get("reasoning_text")) {
                let logical_id = format!(
                    "session:{}:message:{message_index}:reasoning",
                    session.raw_session_id
                );
                let mut record = base.clone();
                record.insert("type".to_string(), json!("message"));
                record.insert("logical_id".to_string(), json!(logical_id));
                record.insert("role".to_string(), json!("assistant"));
                record.insert("reasoning".to_string(), json!(true));
                record.insert(
                    "content".to_string(),
                    json!(truncate_chars_local(&reasoning, MAX_NAC_TEXT_CHARS)),
                );
                if let Some(details) = object.get("reasoning_details") {
                    record.insert("reasoning_details".to_string(), bounded_json(details));
                }
                push_logical_part(
                    &mut parts,
                    (logical_id, Value::Object(record), 0, offset),
                    record_budget,
                )?;
            }
        }

        if role != "tool" {
            if let Some(content) = optional_string(object.get("content")) {
                let logical_id = format!(
                    "session:{}:message:{message_index}:content",
                    session.raw_session_id
                );
                let mut record = base.clone();
                record.insert("type".to_string(), json!("message"));
                record.insert("logical_id".to_string(), json!(logical_id));
                record.insert("role".to_string(), json!(role));
                record.insert(
                    "content".to_string(),
                    json!(truncate_chars_local(&content, MAX_NAC_TEXT_CHARS)),
                );
                if align_metrics {
                    if let Some(slot) = terminal_slots.get(&message_index) {
                        if let Some(usage) = usages.get(*slot) {
                            copy_usage_fields(&mut record, usage);
                        }
                        if let Some(duration) = durations.get(*slot).and_then(Value::as_u64) {
                            record.insert("latency_ms".to_string(), json!(duration));
                        }
                    }
                }
                push_logical_part(
                    &mut parts,
                    (logical_id, Value::Object(record), 1, offset),
                    record_budget,
                )?;
            }
        }

        if role == "assistant" {
            if let Some(tool_calls) = object.get("tool_calls").and_then(Value::as_array) {
                for (tool_index, tool_call) in tool_calls.iter().enumerate() {
                    let call_id = value_string(tool_call.get("id"));
                    if call_id.is_empty() {
                        return Err(NacScanError::Scan(anyhow::anyhow!(
                            "NAC assistant message {message_index} tool call {tool_index} has no id"
                        )));
                    }
                    let raw_name = value_string(tool_call.pointer("/function/name"));
                    let tool_name = canonical_mcp_tool_name(&raw_name);
                    let input = parse_tool_arguments(tool_call.pointer("/function/arguments"))?;
                    let logical_id = format!(
                        "session:{}:message:{message_index}:tool:{call_id}",
                        session.raw_session_id
                    );
                    let mut record = base.clone();
                    record.insert("type".to_string(), json!("tool_request"));
                    record.insert("logical_id".to_string(), json!(logical_id));
                    record.insert("tool_call_id".to_string(), json!(call_id));
                    record.insert("tool_name".to_string(), json!(tool_name));
                    record.insert("raw_tool_name".to_string(), json!(raw_name));
                    record.insert("input".to_string(), input);
                    let source_line_no = hash_str(&call_id).wrapping_shl(2) | 2;
                    preceding_tools.insert(
                        call_id,
                        ToolRequestContext {
                            tool_name,
                            raw_name,
                            logical_id: logical_id.clone(),
                            source_line_no,
                            source_offset: offset,
                        },
                    );
                    push_logical_part(
                        &mut parts,
                        (logical_id, Value::Object(record), source_line_no, offset),
                        record_budget,
                    )?;
                }
            }
        } else if role == "tool" {
            let call_id = value_string(object.get("tool_call_id"));
            let request = preceding_tools.get(&call_id).cloned();
            let tool_name = request
                .as_ref()
                .map(|request| request.tool_name.clone())
                .unwrap_or_default();
            let raw_name = request
                .as_ref()
                .map(|request| request.raw_name.clone())
                .unwrap_or_default();
            let logical_id = format!(
                "session:{}:message:{message_index}:tool-result:{call_id}",
                session.raw_session_id
            );
            let mut record = base;
            record.insert("type".to_string(), json!("tool_response"));
            record.insert("logical_id".to_string(), json!(logical_id));
            record.insert("tool_call_id".to_string(), json!(call_id));
            record.insert("tool_name".to_string(), json!(tool_name));
            record.insert("raw_tool_name".to_string(), json!(raw_name));
            if let Some(request) = request {
                record.insert("request_logical_id".to_string(), json!(request.logical_id));
                record.insert(
                    "request_source_line_no".to_string(),
                    json!(request.source_line_no),
                );
                record.insert(
                    "request_source_offset".to_string(),
                    json!(request.source_offset),
                );
            }
            let content = object.get("content").cloned().unwrap_or(Value::Null);
            record.insert("output".to_string(), content.clone());
            record.insert(
                "output_text".to_string(),
                json!(value_string(Some(&content))),
            );
            record.insert(
                "is_error".to_string(),
                json!(object
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)),
            );
            push_logical_part(
                &mut parts,
                (logical_id, Value::Object(record), 3, offset),
                record_budget,
            )?;
        }
    }
    Ok(parts)
}

fn completed_terminal_indices(messages: &[Value]) -> Vec<usize> {
    let mut terminals = Vec::new();
    let mut run_open = false;
    for (index, message) in messages.iter().enumerate() {
        let role = value_string(message.get("role"));
        if role == "user" {
            run_open = true;
            continue;
        }
        if run_open && role == "assistant" {
            let tool_calls_empty = message
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty);
            if tool_calls_empty {
                terminals.push(index);
                run_open = false;
            }
        }
    }
    terminals
}

fn copy_usage_fields(record: &mut Map<String, Value>, usage: &Value) {
    let Some(object) = usage.as_object() else {
        return;
    };
    for (source, target) in [
        ("input_tokens", "input_tokens"),
        ("output_tokens", "output_tokens"),
        ("cache_read_tokens", "cache_read_tokens"),
        ("cache_write_tokens", "cache_write_tokens"),
        ("reasoning_tokens", "reasoning_tokens"),
    ] {
        if let Some(value) = object.get(source).and_then(Value::as_u64) {
            record.insert(target.to_string(), json!(value));
        }
    }
}

fn worker_session_meta_record(
    parent: &NacSessionRow,
    raw_session_id: &str,
    parent_id: &str,
    worker_id: &str,
    thread_name: &str,
    episode_id: u64,
    created_at_raw: &str,
) -> std::result::Result<SyntheticRecord, NacScanError> {
    let timestamp = normalize_nac_timestamp(created_at_raw, false)?;
    Ok(SyntheticRecord {
        record: json!({
            "type": "worker_session_meta",
            "logical_id": format!("worker:{raw_session_id}:{thread_name}:meta"),
            "session_id": worker_id,
            "parent_session_id": parent_id,
            "raw_session_id": raw_session_id,
            "thread_name": truncate_chars_local(thread_name, 1_000),
            "timestamp": timestamp,
            "cwd": truncate_chars_local(&parent.cwd, MAX_NAC_TEXT_CHARS),
            "cwd_scope": parent.cwd_scope.as_str(),
            "model": parent.model,
            "base_url": parent.base_url,
        }),
        project_dir: project_dir_for(parent),
        source_line_no: 0,
        source_offset: episode_id.saturating_sub(1),
    })
}

#[allow(clippy::too_many_arguments)]
fn worker_event_record(
    parent: &NacSessionRow,
    raw_session_id: &str,
    worker_id: &str,
    thread_name: &str,
    episode_id: u64,
    line: u64,
    action: &str,
    content: &str,
    timestamp: &str,
) -> SyntheticRecord {
    SyntheticRecord {
        record: json!({
            "type": "worker_event",
            "logical_id": format!("episode:{episode_id}:{action}"),
            "session_id": worker_id,
            "raw_session_id": raw_session_id,
            "thread_name": truncate_chars_local(thread_name, 1_000),
            "episode_id": episode_id,
            "action": action,
            "content": truncate_chars_local(content, MAX_NAC_TEXT_CHARS),
            "timestamp": timestamp,
            "cwd": truncate_chars_local(&parent.cwd, MAX_NAC_TEXT_CHARS),
            "cwd_scope": parent.cwd_scope.as_str(),
            "model": parent.model,
            "base_url": parent.base_url,
            "turn_index": episode_id.min(u32::MAX as u64) as u32,
        }),
        project_dir: project_dir_for(parent),
        source_line_no: line,
        source_offset: episode_id,
    }
}

fn link_tool_responses(records: &mut [SyntheticRecord], source_file: &str, generation: u32) {
    let mut requests = BTreeMap::<(String, String), String>::new();
    for synthetic in records.iter() {
        if synthetic.record.get("type").and_then(Value::as_str) != Some("tool_request") {
            continue;
        }
        let session_id = value_string(synthetic.record.get("session_id"));
        let call_id = value_string(synthetic.record.get("tool_call_id"));
        let logical_id = value_string(synthetic.record.get("logical_id"));
        let uid = crate::sources::shared::event_uid(
            source_file,
            generation,
            synthetic.source_line_no,
            synthetic.source_offset,
            &logical_id,
            "tool_request",
        );
        requests.insert((session_id, call_id), uid);
    }
    for synthetic in records.iter_mut() {
        if synthetic.record.get("type").and_then(Value::as_str) != Some("tool_response") {
            continue;
        }
        let session_id = value_string(synthetic.record.get("session_id"));
        let call_id = value_string(synthetic.record.get("tool_call_id"));
        let direct_uid = synthetic
            .record
            .get("request_logical_id")
            .and_then(Value::as_str)
            .map(|logical_id| {
                crate::sources::shared::event_uid(
                    source_file,
                    generation,
                    synthetic
                        .record
                        .get("request_source_line_no")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    synthetic
                        .record
                        .get("request_source_offset")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    logical_id,
                    "tool_request",
                )
            });
        if let Some(uid) = direct_uid.or_else(|| requests.get(&(session_id, call_id)).cloned()) {
            if let Some(object) = synthetic.record.as_object_mut() {
                object.insert("request_event_uid".to_string(), json!(uid));
            }
        }
    }
}

fn normalize_nac_timestamp(raw: &str, nanos: bool) -> std::result::Result<String, NacScanError> {
    let parsed = if nanos {
        NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S%.f")
    } else {
        NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S%.f"))
    }
    .map_err(|exc| {
        NacScanError::Scan(anyhow::anyhow!(
            "invalid required NAC timestamp `{raw}`: {exc}"
        ))
    })?;
    Ok(crate::sources::shared::format_record_ts(
        &DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc),
    ))
}

fn namespace_prefix(source_name: &str, canonical_db: &str, generation: u32) -> String {
    let material = format!("{source_name}\n{canonical_db}\n{generation}");
    format!("nac:{}", short_sha256(material.as_bytes()))
}

fn short_sha256(material: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(material);
    format!("{:x}", hasher.finalize())[..16].to_string()
}

fn value_hash(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value).unwrap_or_default());
    format!("{:x}", hasher.finalize())
}

fn bounded_json(value: &Value) -> Value {
    let raw = serde_json::to_string(value).unwrap_or_default();
    if raw.chars().count() <= MAX_NAC_TEXT_CHARS {
        value.clone()
    } else {
        json!({"truncated": true})
    }
}

fn parse_tool_arguments(value: Option<&Value>) -> std::result::Result<Value, NacScanError> {
    match value {
        None | Some(Value::Null) => Ok(Value::Object(Map::new())),
        Some(Value::String(raw)) if raw.trim().is_empty() => Ok(Value::Object(Map::new())),
        Some(Value::String(raw)) => serde_json::from_str(raw).map_err(|exc| {
            NacScanError::Scan(anyhow::anyhow!("invalid NAC tool arguments JSON: {exc}"))
        }),
        Some(value) => Ok(value.clone()),
    }
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(value) => Some(value.to_string()),
    }
}

fn value_string(value: Option<&Value>) -> String {
    optional_string(value).unwrap_or_default()
}

fn project_dir_for(session: &NacSessionRow) -> String {
    if session.cwd_scope == CwdScope::Local {
        session.cwd.clone()
    } else {
        String::new()
    }
}

fn enforce_record_limit(records: usize) -> std::result::Result<(), NacScanError> {
    if records > MAX_NAC_SYNTHETIC_RECORDS {
        Err(NacScanError::TooLarge(format!(
            "{records} NAC logical records exceed the {MAX_NAC_SYNTHETIC_RECORDS} record ceiling"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn normalizes_both_nac_timestamp_precisions() {
        assert_eq!(
            normalize_nac_timestamp("2026-07-18 12:15:51.775682000", true).unwrap(),
            "2026-07-18T12:15:51.775682Z"
        );
        assert_eq!(
            normalize_nac_timestamp("2026-07-18 12:16:58", false).unwrap(),
            "2026-07-18T12:16:58.000000Z"
        );
    }

    #[test]
    fn session_projection_never_selects_credentials_or_host_values() {
        let columns = [
            "session_id",
            "cwd",
            "model",
            "base_url",
            "messages_json",
            "created_at",
            "updated_at",
            "host_id",
            "api_key_env",
            "extra_headers_json",
            "store_path",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let projection = session_projection(&columns);
        assert!(!projection.contains("api_key_env"));
        assert!(!projection.contains("extra_headers_json"));
        assert!(!projection.contains("store_path"));
        assert!(projection.contains("host_id IS NOT NULL"));
        assert!(!projection.split(',').any(|field| field.trim() == "host_id"));
        assert!(projection.contains("messages_json"));
        assert!(!projection.contains("THEN '[]'"));
        assert!(projection.contains("response_durations_json"));
        assert!(!projection.contains("response_durations_ms_json"));
    }

    static FIXTURE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn fixture_db() -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "moraine-nac-fixture-{}-{nonce}-{sequence}.db",
            std::process::id()
        ));
        let connection = Connection::open(&path).expect("create NAC fixture database");
        connection
            .execute_batch(include_str!("../../../../fixtures/nac/store.sql"))
            .expect("load NAC fixture schema and rows");
        drop(connection);
        path
    }

    fn scan_fixture(path: &Path, prior: &NacState) -> (Vec<SyntheticRecord>, NacState, u64) {
        let stat = stat_fingerprint(path.to_str().expect("UTF-8 fixture path"))
            .expect("fixture stat fingerprint");
        let metadata = std::fs::metadata(path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 fixture path"), &metadata);
        match scan_nac_database(
            path.to_str().expect("UTF-8 fixture path"),
            "nac-fixture",
            1,
            inode,
            stat,
            prior,
        ) {
            NacScanOutcome::Scanned {
                records,
                state,
                relevant_rows,
                ..
            } => (records, state, relevant_rows),
            NacScanOutcome::Failed {
                error_kind,
                error_text,
            } => panic!("fixture scan failed: {error_kind}: {error_text}"),
        }
    }

    fn session_with_messages(messages: Value) -> NacSessionRow {
        NacSessionRow {
            raw_session_id: "stable-session".to_string(),
            cwd: "/workspace".to_string(),
            cwd_scope: CwdScope::Local,
            model: "model".to_string(),
            base_url: "https://example.invalid/v1".to_string(),
            backend: "together-chat".to_string(),
            reasoning_effort: String::new(),
            sandbox: Value::Null,
            messages,
            last_response_duration_ms: None,
            previous_response_duration_ms: None,
            response_durations: json!([]),
            token_usages: json!([]),
            created_at: "2026-07-18 12:00:00.000000000".to_string(),
            updated_at: "2026-07-18 12:00:00.000000000".to_string(),
            estimated_bytes: 0,
        }
    }

    #[test]
    fn logical_coordinates_do_not_shift_when_optional_parts_appear() {
        let without_reasoning = session_with_messages(json!([{
            "role": "assistant",
            "content": "answer",
            "tool_calls": [{
                "id": "call-stable",
                "type": "function",
                "function": {"name": "third_party_tool", "arguments": "{}"}
            }]
        }]));
        let with_reasoning = session_with_messages(json!([{
            "role": "assistant",
            "reasoning_text": "newly persisted reasoning",
            "content": "answer",
            "tool_calls": [{
                "id": "call-stable",
                "type": "function",
                "function": {"name": "third_party_tool", "arguments": "{}"}
            }]
        }]));
        let before = logical_message_parts(
            &without_reasoning,
            "nac:stable-session",
            "2026-07-18T12:00:00.000000000Z",
            10,
        )
        .expect("synthesize original parts")
        .into_iter()
        .map(|(id, _, line, offset)| (id, (line, offset)))
        .collect::<BTreeMap<_, _>>();
        let after = logical_message_parts(
            &with_reasoning,
            "nac:stable-session",
            "2026-07-18T12:00:00.000000000Z",
            10,
        )
        .expect("synthesize updated parts")
        .into_iter()
        .map(|(id, _, line, offset)| (id, (line, offset)))
        .collect::<BTreeMap<_, _>>();
        for (logical_id, coordinates) in before {
            assert_eq!(after.get(&logical_id), Some(&coordinates));
        }
    }

    #[test]
    fn logical_part_budget_fails_before_building_an_unbounded_vector() {
        let session = session_with_messages(json!([{
            "role": "assistant",
            "reasoning_text": "reasoning",
            "content": "answer"
        }]));
        assert!(matches!(
            logical_message_parts(
                &session,
                "nac:stable-session",
                "2026-07-18T12:00:00.000000000Z",
                1,
            ),
            Err(NacScanError::TooLarge(_))
        ));
    }

    #[test]
    fn tool_response_fallback_is_scoped_to_its_session() {
        let mut records = vec![
            SyntheticRecord {
                record: json!({
                    "type": "tool_request",
                    "session_id": "session-a",
                    "logical_id": "request-a",
                    "tool_call_id": "shared-call"
                }),
                project_dir: String::new(),
                source_line_no: 2,
                source_offset: 1,
            },
            SyntheticRecord {
                record: json!({
                    "type": "tool_request",
                    "session_id": "session-b",
                    "logical_id": "request-b",
                    "tool_call_id": "shared-call"
                }),
                project_dir: String::new(),
                source_line_no: 6,
                source_offset: 1,
            },
            SyntheticRecord {
                record: json!({
                    "type": "tool_response",
                    "session_id": "session-b",
                    "logical_id": "response-b",
                    "tool_call_id": "shared-call"
                }),
                project_dir: String::new(),
                source_line_no: 3,
                source_offset: 2,
            },
        ];
        link_tool_responses(&mut records, "/tmp/store.db", 1);
        let expected = crate::sources::shared::event_uid(
            "/tmp/store.db",
            1,
            6,
            1,
            "request-b",
            "tool_request",
        );
        assert_eq!(records[2].record["request_event_uid"], expected);
    }

    #[tokio::test]
    async fn exclusion_changes_replay_unchanged_nac_rows() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-fixture".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel(4);
        let mut excluded = AppConfig::default();
        excluded.ingest.exclude_project_dirs = vec!["/workspace/local/**".to_string()];

        process_nac_sqlite_db(
            &excluded,
            &work,
            checkpoints.clone(),
            &poll_state,
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("scan with local project excluded");
        let SinkMessage::Batch(first_batch) = sink_rx.recv().await.expect("first NAC batch");
        assert_eq!(first_batch.raw_rows.len(), 6);
        let checkpoint = first_batch.checkpoint.expect("first checkpoint");
        checkpoints
            .write()
            .await
            .insert(checkpoint_key("nac-fixture", &source_file), checkpoint);

        let included = AppConfig::default();
        process_nac_sqlite_db(
            &included,
            &work,
            checkpoints,
            &poll_state,
            sink_tx,
            &metrics,
        )
        .await
        .expect("replay after exclusion changes");
        let SinkMessage::Batch(replayed_batch) = sink_rx.recv().await.expect("replayed NAC batch");
        assert_eq!(
            replayed_batch.raw_rows.len(),
            19,
            "unchanged local records must be reconsidered when exclusions change"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    #[test]
    fn scanner_rejects_a_database_replaced_before_open() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let stat = stat_fingerprint(&source_file).expect("fixture stat");
        let metadata = std::fs::metadata(&path).expect("fixture metadata");
        let inode = source_inode_for_file(&source_file, &metadata);
        assert!(matches!(
            scan_nac_database(
                &source_file,
                "nac-fixture",
                1,
                inode ^ 1,
                stat,
                &NacState::default(),
            ),
            NacScanOutcome::Failed {
                error_kind: ERROR_KIND_MIXED_SNAPSHOT,
                ..
            }
        ));
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    #[tokio::test]
    async fn failed_replacement_does_not_checkpoint_a_phantom_generation() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-fixture".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
        };
        let config = AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel(4);
        process_nac_sqlite_db(
            &config,
            &work,
            checkpoints.clone(),
            &poll_state,
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("initial valid scan");
        let SinkMessage::Batch(initial_batch) = sink_rx.recv().await.expect("initial NAC batch");
        let checkpoint = initial_batch.checkpoint.expect("initial checkpoint");
        let committed_inode = checkpoint.source_inode;
        let committed_generation = checkpoint.source_generation;
        checkpoints
            .write()
            .await
            .insert(checkpoint_key("nac-fixture", &source_file), checkpoint);

        std::fs::remove_file(&path).expect("remove fixture database");
        std::fs::write(&path, b"not a sqlite database").expect("write invalid replacement");
        process_nac_sqlite_db(&config, &work, checkpoints, &poll_state, sink_tx, &metrics)
            .await
            .expect("failed replacement is reported as a batch");
        let SinkMessage::Batch(failed_batch) = sink_rx.recv().await.expect("failed NAC batch");
        assert_eq!(failed_batch.error_rows.len(), 1);
        assert_eq!(
            failed_batch.error_rows[0]["source_generation"],
            committed_generation.saturating_add(1)
        );
        let retained = failed_batch
            .checkpoint
            .expect("failure checkpoint retains committed identity");
        assert_eq!(retained.source_inode, committed_inode);
        assert_eq!(retained.source_generation, committed_generation);

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    #[tokio::test]
    async fn oversized_normalized_rows_fail_before_any_payload_is_sent() {
        let path = fixture_db();
        let oversized_arguments = serde_json::to_string(&json!({
            "blob": "x".repeat(11 * 1024 * 1024)
        }))
        .expect("serialize oversized arguments");
        let messages = json!([{
            "role": "assistant",
            "content": null,
            "tool_calls": [{
                "id": "oversized-call",
                "type": "function",
                "function": {
                    "name": "third_party_tool",
                    "arguments": oversized_arguments
                }
            }]
        }]);
        let connection = Connection::open(&path).expect("open fixture for oversized update");
        connection
            .execute(
                "UPDATE sessions SET messages_json = ?1 WHERE session_id = 'session-local'",
                [serde_json::to_string(&messages).expect("serialize oversized messages")],
            )
            .expect("store oversized messages");
        drop(connection);

        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-fixture".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file,
        };
        let (sink_tx, mut sink_rx) = mpsc::channel(2);
        process_nac_sqlite_db(
            &AppConfig::default(),
            &work,
            Arc::new(RwLock::new(HashMap::new())),
            &VolatilePollMap::new(),
            sink_tx,
            &Arc::new(Metrics::default()),
        )
        .await
        .expect("oversized scan reports an ingest error");
        let SinkMessage::Batch(batch) = sink_rx.recv().await.expect("oversized failure batch");
        assert!(batch.raw_rows.is_empty());
        assert!(batch.event_rows.is_empty());
        assert!(batch.tool_rows.is_empty());
        assert_eq!(batch.error_rows.len(), 1);
        assert_eq!(
            batch.error_rows[0]["error_kind"],
            ERROR_KIND_NORMALIZED_ROW_TOO_LARGE
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    #[test]
    fn fixture_snapshot_preserves_sessions_tools_workers_and_incremental_identity() {
        let path = fixture_db();
        let _writer = Connection::open(&path).expect("keep fixture WAL sidecars stable");
        let _: i64 = _writer
            .query_row("SELECT count(*) FROM sessions", [], |row| row.get(0))
            .expect("activate fixture WAL");
        let source_file = path.to_string_lossy().to_string();
        let (mut first, first_state, relevant_rows) = scan_fixture(&path, &NacState::default());
        assert_eq!(relevant_rows, 5);
        assert_eq!(first.len(), 19);
        assert_eq!(
            first
                .iter()
                .filter(|record| record.record["type"] == "tool_response")
                .count(),
            1,
            "tool results must not also be emitted as ordinary messages"
        );
        assert_eq!(
            first
                .iter()
                .filter(|record| record.record["type"] == "worker_session_meta")
                .count(),
            3
        );
        assert!(first.iter().any(|record| {
            record.record["type"] == "tool_request"
                && record.record["tool_name"] == "search"
                && record.record["raw_tool_name"] == "mcp__moraine__search"
        }));
        let remote_records = first
            .iter()
            .filter(|record| record.record["raw_session_id"] == "session-remote")
            .collect::<Vec<_>>();
        assert_eq!(remote_records.len(), 6);
        assert!(remote_records
            .iter()
            .all(|record| record.record["cwd_scope"] == "remote" && record.project_dir.is_empty()));
        let raw_snapshot =
            serde_json::to_string(&first.iter().map(|row| &row.record).collect::<Vec<_>>())
                .expect("serialize synthesized records");
        assert!(!raw_snapshot.contains("NAC_FIXTURE_SECRET_ENV"));
        assert!(!raw_snapshot.contains("fixture-secret"));
        assert!(raw_snapshot.contains("remote question"));
        assert!(raw_snapshot.contains("remote answer"));
        assert!(raw_snapshot.contains("remote private action"));
        assert!(raw_snapshot.contains("remote private response"));
        let local_metadata = first
            .iter()
            .find(|record| {
                record.record["type"] == "session_meta"
                    && record.record["raw_session_id"] == "session-local"
            })
            .expect("local session metadata");
        assert_eq!(
            local_metadata.record["timestamp"], "2026-07-18T12:15:51.775682Z",
            "session metadata keeps a stable creation event timestamp"
        );
        let completed_response = first
            .iter()
            .find(|record| record.record["type"] == "message" && record.record["content"] == "done")
            .expect("completed assistant response");
        assert_eq!(completed_response.record["latency_ms"], 1200);

        link_tool_responses(&mut first, &source_file, 1);
        let response = first
            .iter()
            .find(|record| record.record["type"] == "tool_response")
            .expect("tool response");
        assert!(response
            .record
            .get("request_event_uid")
            .and_then(Value::as_str)
            .is_some_and(|uid| !uid.is_empty()));

        let mut event_uids = BTreeSet::new();
        let mut tool_rows = Vec::new();
        let mut link_rows = Vec::new();
        for synthetic in &first {
            let normalized = normalize_record(
                &synthetic.record,
                "nac-fixture",
                "nac",
                &source_file,
                1,
                1,
                synthetic.source_line_no,
                synthetic.source_offset,
                "",
                "",
                "",
            )
            .expect("normalize NAC fixture record");
            for event in normalized.event_rows {
                let uid = event["event_uid"]
                    .as_str()
                    .expect("normalized event uid")
                    .to_string();
                assert!(event_uids.insert(uid), "event UIDs must be unique");
            }
            tool_rows.extend(normalized.tool_rows);
            link_rows.extend(normalized.link_rows);
        }
        assert_eq!(event_uids.len(), first.len());
        assert_eq!(tool_rows.len(), 2);
        assert_eq!(
            tool_rows
                .iter()
                .filter(|row| row["tool_name"] == "search")
                .count(),
            2
        );
        assert_eq!(
            link_rows
                .iter()
                .filter(|row| row["link_type"] == "subagent_parent")
                .count(),
            3
        );

        let (unchanged, _, _) = scan_fixture(&path, &first_state);
        assert!(unchanged.is_empty(), "unchanged snapshots must be no-ops");

        let connection = Connection::open(&path).expect("open fixture for append");
        let messages_raw: String = connection
            .query_row(
                "SELECT messages_json FROM sessions WHERE session_id = 'session-local'",
                [],
                |row| row.get(0),
            )
            .expect("read fixture messages");
        let mut messages: Value =
            serde_json::from_str(&messages_raw).expect("parse fixture messages");
        let messages = messages.as_array_mut().expect("message array");
        messages.push(json!({"role": "user", "content": "follow up"}));
        messages.push(json!({"role": "assistant", "content": "follow-up answer"}));
        connection
            .execute(
                "UPDATE sessions
                 SET messages_json = ?1,
                     response_durations_json = ?2,
                     token_usages_json = ?3,
                     updated_at = ?4
                 WHERE session_id = 'session-local'",
                params![
                    serde_json::to_string(messages).expect("serialize appended messages"),
                    "[1200,800]",
                    "[{\"input_tokens\":12,\"output_tokens\":4,\"cache_read_tokens\":2},{\"input_tokens\":8,\"output_tokens\":3}]",
                    "2026-07-18 12:17:00.000000000"
                ],
            )
            .expect("append session messages");
        connection
            .execute(
                "INSERT INTO episodes (thread_name, session_id, action, content, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "worker-a",
                    "session-local",
                    "inspect follow-up",
                    "follow-up complete",
                    "2026-07-18 12:17:01"
                ],
            )
            .expect("append worker episode");
        drop(connection);

        let (incremental, second_state, _) = scan_fixture(&path, &first_state);
        assert_eq!(incremental.len(), 5);
        assert_eq!(
            incremental
                .iter()
                .filter(|record| record.record["type"] == "worker_session_meta")
                .count(),
            0,
            "existing worker sessions must not be duplicated"
        );
        assert_eq!(
            incremental
                .iter()
                .filter(|record| record.record["type"] == "message")
                .count(),
            2,
            "{:?}",
            incremental
                .iter()
                .map(|record| (&record.record["logical_id"], &record.record["type"]))
                .collect::<Vec<_>>()
        );
        let (second_noop, _, _) = scan_fixture(&path, &second_state);
        assert!(second_noop.is_empty());

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }
}
