//! SQLite polling engine for database-backed ingest sources (issue #361).
//!
//! The first consumer is the `cursor_sqlite` format, which polls Cursor's
//! `state.vscdb` key/value stores. Cursor's `cursorDiskKV` table has no
//! timestamp or rowid watermark (`key TEXT UNIQUE ON CONFLICT REPLACE`), so
//! change detection is hash-based: the checkpoint's `cursor_json` payload
//! carries one content hash per relevant key, and a poll emits synthetic
//! records only for keys that are new or whose hash changed. Deleted keys are
//! pruned after every full prefix scan.
//!
//! Synthetic records reuse the existing `cursor` harness normalization path
//! (`sources/cursor.rs`) with stable logical identity: `source_line_no` /
//! `source_offset` are hashes of the kv key, and the per-event UID material is
//! `cursor_sqlite:<table>:<pk>` rather than the mutable payload, so a bubble
//! that mutates in place (tool status `pending` → `completed`, streaming
//! text) re-emits the *same* event UIDs with a newer `event_version` and the
//! `ReplacingMergeTree` collapses them.
//!
//! Live application databases are opened read-only with a short busy timeout;
//! Moraine never checkpoints another application's WAL. Failures (busy DB,
//! schema drift, oversized key space) surface as `ingest_errors` rows —
//! rate-limited via the cursor state so a persistent failure does not flood
//! the table on every reconcile tick — and leave the data cursor untouched so
//! the next poll retries.

use crate::checkpoint::checkpoint_key;
use crate::dispatch::source_inode_for_file;
use crate::model::{Checkpoint, RowBatch};
use crate::normalize::normalize_record;
use crate::sources::shared::format_record_ts;
use crate::{Metrics, SinkMessage, WorkItem};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use moraine_config::{AppConfig, SOURCE_FORMAT_CURSOR_SQLITE};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

/// Key prefixes in `cursorDiskKV` that carry transcript data. Everything else
/// (`agentKv:blob:*` provider-wire blobs, `checkpointId:*` file snapshots,
/// `ofsContent:*`/`composer.content.*` raw file text, and the entire
/// `ItemTable` — which holds live auth tokens) is deliberately out of scope
/// for v1; see issue #361 and the field census in PR discussion.
const RELEVANT_PREFIXES: &[&str] = &["bubbleId:", "composerData:"];

/// Issue #361: bail out rather than persist an oversized checkpoint payload.
const MAX_RELEVANT_KEYS: usize = 10_000;

/// Strings longer than this inside tool params/results are elided before the
/// synthetic record is built. Cursor stores base64 screenshots (~1.2 MB each)
/// inside `toolFormerData.result`; real textual outputs observed in the field
/// stay well under this bound.
const LONG_STRING_ELIDE_CHARS: usize = 65_536;

/// Rows fetched per statement so a poll never holds one long read transaction.
const SCAN_PAGE_SIZE: usize = 512;

const CURSOR_STATE_VERSION: u32 = 1;

const ERROR_KIND_OPEN: &str = "sqlite_open_error";
const ERROR_KIND_SCHEMA: &str = "sqlite_schema_mismatch";
const ERROR_KIND_TOO_LARGE: &str = "sqlite_cursor_too_large";
const ERROR_KIND_SCAN: &str = "sqlite_scan_error";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct StatFingerprint {
    db_len: u64,
    db_mtime_ns: u64,
    wal_len: u64,
    wal_mtime_ns: u64,
    shm_len: u64,
    shm_mtime_ns: u64,
}

/// Persisted poll cursor (the checkpoint's `cursor_json` payload).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CursorState {
    version: u32,
    format: String,
    #[serde(default)]
    stat: StatFingerprint,
    /// kv key → content-hash (hex u64). `BTreeMap` keeps serialization stable.
    #[serde(default)]
    kv_hashes: BTreeMap<String, String>,
    /// Last error kind reported for this database; used to emit each failure
    /// mode once instead of once per reconcile tick.
    #[serde(default)]
    last_error: String,
}

impl CursorState {
    fn parse(cursor_json: &str) -> Self {
        if cursor_json.trim().is_empty() {
            return Self::fresh();
        }
        match serde_json::from_str::<CursorState>(cursor_json) {
            Ok(state)
                if state.version == CURSOR_STATE_VERSION
                    && state.format == SOURCE_FORMAT_CURSOR_SQLITE =>
            {
                state
            }
            Ok(_) | Err(_) => Self::fresh(),
        }
    }

    fn fresh() -> Self {
        Self {
            version: CURSOR_STATE_VERSION,
            format: SOURCE_FORMAT_CURSOR_SQLITE.to_string(),
            ..Default::default()
        }
    }

    fn serialize(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// One synthetic record ready for `normalize_record`, with the stable
/// source coordinates required by issue #361 decision 7.
#[derive(Debug, Clone)]
pub struct SyntheticRecord {
    pub record: Value,
    pub source_line_no: u64,
    pub source_offset: u64,
}

enum ScanOutcome {
    Scanned {
        records: Vec<SyntheticRecord>,
        new_state: CursorState,
        schema_fingerprint: u64,
        relevant_keys: u64,
    },
    Failed {
        error_kind: &'static str,
        error_text: String,
    },
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    u64::from_be_bytes(
        digest[0..8]
            .try_into()
            .expect("sha256 digest shorter than 8 bytes"),
    )
}

fn hash_str(text: &str) -> u64 {
    hash_bytes(text.as_bytes())
}

fn stat_fingerprint(db_path: &str) -> Option<StatFingerprint> {
    fn len_and_mtime(path: &str) -> (u64, u64) {
        match std::fs::metadata(path) {
            Ok(meta) => {
                // Nanosecond precision: a watcher event for a same-size write
                // within one timestamp granule must not be short-circuited
                // away, or the change is missed until the next write.
                let mtime_ns = meta
                    .modified()
                    .ok()
                    .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                (meta.len(), mtime_ns)
            }
            Err(_) => (0, 0),
        }
    }

    if !std::path::Path::new(db_path).exists() {
        return None;
    }
    let (db_len, db_mtime_ns) = len_and_mtime(db_path);
    let (wal_len, wal_mtime_ns) = len_and_mtime(&format!("{db_path}-wal"));
    let (shm_len, shm_mtime_ns) = len_and_mtime(&format!("{db_path}-shm"));
    Some(StatFingerprint {
        db_len,
        db_mtime_ns,
        wal_len,
        wal_mtime_ns,
        shm_len,
        shm_mtime_ns,
    })
}

/// Exclusive upper bound for a prefix range scan over the `key` index:
/// the prefix with its final byte incremented (`"bubbleId:"` → `"bubbleId;"`).
fn prefix_range_end(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    if let Some(last) = bytes.last_mut() {
        *last += 1;
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

pub(crate) async fn process_cursor_sqlite_db(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let source_file = work.path.clone();

    let Some(current_stat) = stat_fingerprint(&source_file) else {
        debug!("cursor_sqlite db missing, skipping: {}", source_file);
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

    let mut state = CursorState::parse(&checkpoint.cursor_json);

    // A replaced database file is a new generation: every logical identity
    // (and therefore every event UID) starts over, and the hash cursor is
    // meaningless for the new file's contents.
    if checkpoint.source_inode != inode {
        checkpoint.source_inode = inode;
        checkpoint.source_generation = checkpoint.source_generation.saturating_add(1).max(1);
        state = CursorState::fresh();
    }

    // Cheap no-change short-circuit: nothing touched the database or its WAL
    // sidecars since the last successful poll.
    if state.stat == current_stat && state.last_error.is_empty() {
        return Ok(());
    }

    let scan_db_path = source_file.clone();
    let scan_state = state.clone();
    let outcome = tokio::task::spawn_blocking(move || scan_database(&scan_db_path, &scan_state))
        .await
        .context("cursor_sqlite scan task panicked")?;

    match outcome {
        ScanOutcome::Scanned {
            records,
            mut new_state,
            schema_fingerprint,
            relevant_keys,
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
                        .context("sink channel closed while sending cursor_sqlite chunk")?;
                }
            }

            let emitted = records.len();
            let final_checkpoint = Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                // Monotone poll sequence: `merge_checkpoint` resolves
                // same-generation conflicts by `last_offset >=`, so the
                // cursor payload must ride a strictly increasing value.
                last_offset: checkpoint.last_offset.saturating_add(1),
                last_line_no: relevant_keys,
                status: "active".to_string(),
                cursor_json: new_state.serialize(),
                source_fingerprint: inode,
                schema_fingerprint,
            };

            batch.checkpoint = Some(final_checkpoint);
            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending final cursor_sqlite batch")?;

            if emitted > 0 {
                debug!(
                    "{}:{} cursor_sqlite emitted {} changed records ({} relevant keys)",
                    work.source_name, source_file, emitted, relevant_keys
                );
            }
            let _ = metrics;
            Ok(())
        }
        ScanOutcome::Failed {
            error_kind,
            error_text,
        } => {
            let mut batch = RowBatch::default();
            // Emit each failure mode once per state change, not once per
            // reconcile tick — ingest_errors is append-only.
            if state.last_error != error_kind {
                warn!(
                    "cursor_sqlite poll failed for {}: {} ({})",
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
            }

            // Preserve the data cursor (kv hashes and stat fingerprint stay
            // as-is so the next poll retries); only the error marker moves.
            let mut error_state = state.clone();
            error_state.last_error = error_kind.to_string();
            batch.checkpoint = Some(Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: checkpoint.last_offset.saturating_add(1),
                last_line_no: checkpoint.last_line_no,
                status: "active".to_string(),
                cursor_json: error_state.serialize(),
                source_fingerprint: inode,
                schema_fingerprint: checkpoint.schema_fingerprint,
            });

            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending cursor_sqlite error batch")?;
            Ok(())
        }
    }
}

/// Blocking phase: open the database read-only, validate schema, scan the
/// relevant key ranges, and synthesize records for new/changed keys.
fn scan_database(db_path: &str, prior: &CursorState) -> ScanOutcome {
    let connection = match open_read_only(db_path) {
        Ok(connection) => connection,
        Err(exc) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_OPEN,
                error_text: exc.to_string(),
            }
        }
    };

    let schema_fingerprint = match validate_schema(&connection) {
        Ok(fingerprint) => fingerprint,
        Err(text) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCHEMA,
                error_text: text,
            }
        }
    };

    match count_relevant_keys(&connection) {
        Ok(count) if count > MAX_RELEVANT_KEYS => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_TOO_LARGE,
                error_text: format!(
                    "{count} relevant keys exceed the {MAX_RELEVANT_KEYS} checkpoint ceiling; \
                     use Cursor agent transcripts (JSONL) for this history"
                ),
            };
        }
        Ok(_) => {}
        Err(exc) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: exc.to_string(),
            };
        }
    }

    let mut new_state = CursorState {
        version: CURSOR_STATE_VERSION,
        format: SOURCE_FORMAT_CURSOR_SQLITE.to_string(),
        stat: StatFingerprint::default(),
        kv_hashes: BTreeMap::new(),
        last_error: String::new(),
    };
    let mut changed: Vec<(String, Vec<u8>)> = Vec::new();
    let mut relevant_keys = 0u64;

    for prefix in RELEVANT_PREFIXES {
        let scan = scan_prefix(
            &connection,
            prefix,
            &prior.kv_hashes,
            &mut new_state.kv_hashes,
            &mut changed,
        );
        match scan {
            Ok(seen) => relevant_keys += seen,
            Err(exc) => {
                return ScanOutcome::Failed {
                    error_kind: ERROR_KIND_SCAN,
                    error_text: exc.to_string(),
                };
            }
        }
    }

    let mut records = Vec::<SyntheticRecord>::new();
    for (key, value) in &changed {
        if let Some(record) = synthesize_cursor_sqlite_record(key, value) {
            records.push(record);
        }
    }

    // Composer (session_meta) records first, then bubbles in timestamp order:
    // downstream ordering is timestamp-driven, but deterministic emission
    // keeps raw-row insertion order reproducible for fixtures and debugging.
    records.sort_by(|a, b| {
        let rank = |r: &SyntheticRecord| {
            let kind = r
                .record
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let session = r
                .record
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let ts = r
                .record
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            (
                if kind == "cursor_composer" { 0u8 } else { 1u8 },
                session,
                ts,
                r.source_offset,
            )
        };
        rank(a).cmp(&rank(b))
    });

    ScanOutcome::Scanned {
        records,
        new_state,
        schema_fingerprint,
        relevant_keys,
    }
}

fn open_read_only(db_path: &str) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open {db_path} read-only"))?;
    connection
        .busy_timeout(std::time::Duration::from_millis(500))
        .context("failed to set busy_timeout")?;
    // Defense in depth on a live application database: never write, never
    // checkpoint the WAL.
    connection
        .pragma_update(None, "query_only", "ON")
        .context("failed to set query_only")?;
    Ok(connection)
}

fn validate_schema(connection: &Connection) -> std::result::Result<u64, String> {
    let schema_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'cursorDiskKV'",
            [],
            |row| row.get(0),
        )
        .map_err(|exc| match exc {
            rusqlite::Error::QueryReturnedNoRows => {
                "required table cursorDiskKV is missing".to_string()
            }
            other => other.to_string(),
        })?;

    let schema_sql = schema_sql.unwrap_or_default();
    let mut has_key = false;
    let mut has_value = false;
    connection
        .prepare("PRAGMA table_info(cursorDiskKV)")
        .and_then(|mut stmt| {
            let names = stmt
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for name in names {
                match name.as_str() {
                    "key" => has_key = true,
                    "value" => has_value = true,
                    _ => {}
                }
            }
            Ok(())
        })
        .map_err(|exc| exc.to_string())?;

    if !has_key || !has_value {
        return Err(format!(
            "cursorDiskKV is missing required columns (key: {has_key}, value: {has_value})"
        ));
    }
    Ok(hash_str(&schema_sql))
}

fn count_relevant_keys(connection: &Connection) -> Result<usize> {
    let mut total = 0usize;
    for prefix in RELEVANT_PREFIXES {
        let count: i64 = connection
            .query_row(
                "SELECT count(*) FROM cursorDiskKV WHERE key >= ?1 AND key < ?2",
                rusqlite::params![prefix, prefix_range_end(prefix)],
                |row| row.get(0),
            )
            .with_context(|| format!("failed counting keys for prefix {prefix}"))?;
        total = total.saturating_add(count.max(0) as usize);
    }
    Ok(total)
}

/// Pages through one key prefix, recording content hashes for every key and
/// collecting the value bytes for keys that are new or changed. Paging keeps
/// each implicit read transaction short (issue #361 decision 4).
fn scan_prefix(
    connection: &Connection,
    prefix: &str,
    prior_hashes: &BTreeMap<String, String>,
    new_hashes: &mut BTreeMap<String, String>,
    changed: &mut Vec<(String, Vec<u8>)>,
) -> Result<u64> {
    let range_end = prefix_range_end(prefix);
    let mut last_key = prefix.to_string();
    let mut seen = 0u64;

    loop {
        let mut stmt = connection
            .prepare_cached(
                "SELECT key, value FROM cursorDiskKV \
                 WHERE key > ?1 AND key < ?2 ORDER BY key LIMIT ?3",
            )
            .context("failed to prepare prefix scan")?;
        let page: Vec<(String, Option<Vec<u8>>)> = stmt
            .query_map(
                rusqlite::params![last_key, range_end, SCAN_PAGE_SIZE as i64],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<Vec<u8>>>(1)?)),
            )
            .context("prefix scan query failed")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("prefix scan row failed")?;

        let page_len = page.len();
        for (key, value) in page {
            last_key = key.clone();
            seen += 1;
            let bytes = value.unwrap_or_default();
            let hash = format!("{:016x}", hash_bytes(&bytes));
            let unchanged = prior_hashes.get(&key) == Some(&hash);
            new_hashes.insert(key.clone(), hash);
            if !unchanged && !bytes.is_empty() {
                changed.push((key, bytes));
            }
        }

        if page_len < SCAN_PAGE_SIZE {
            break;
        }
    }

    Ok(seen)
}

/// Builds the synthetic record for one changed kv row, or `None` when the row
/// carries nothing worth normalizing (NULL/empty values, non-JSON payloads,
/// ghost composers, unknown key families).
pub fn synthesize_cursor_sqlite_record(key: &str, value: &[u8]) -> Option<SyntheticRecord> {
    let text = std::str::from_utf8(value).ok()?;
    let parsed: Value = serde_json::from_str(text).ok()?;
    if !parsed.is_object() {
        return None;
    }

    if let Some(composer_id) = key.strip_prefix("composerData:") {
        return synthesize_composer_record(composer_id, &parsed);
    }
    if let Some(rest) = key.strip_prefix("bubbleId:") {
        let (composer_id, bubble_id) = rest.split_once(':')?;
        return synthesize_bubble_record(composer_id, bubble_id, &parsed);
    }
    None
}

fn stable_coordinates(table: &str, pk: &str, record_kind: &str) -> (u64, u64) {
    let line_no = hash_str(&format!("{table}:{pk}"));
    let offset = hash_str(&format!(
        "{}:{table}:{pk}:{record_kind}",
        SOURCE_FORMAT_CURSOR_SQLITE
    ));
    (line_no, offset)
}

fn epoch_ms_to_record_ts(epoch_ms: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_millis(epoch_ms).map(|dt| format_record_ts(&dt))
}

fn synthesize_composer_record(composer_id: &str, data: &Value) -> Option<SyntheticRecord> {
    let headers = data
        .get("fullConversationHeadersOnly")
        .and_then(Value::as_array)
        .map(|headers| headers.len())
        .unwrap_or(0);
    let name = data.get("name").and_then(Value::as_str).unwrap_or("");

    // Cursor auto-creates a composerData shell per window; a record with no
    // conversation headers and no name is UI state, not a session. The hash
    // cursor re-evaluates it on every change, so a shell that later becomes a
    // real session is picked up then.
    if headers == 0 && name.is_empty() {
        return None;
    }

    let created_at_ms = data.get("createdAt").and_then(Value::as_i64).unwrap_or(0);
    // Always stamp the *creation* time: `event_ts` participates in the
    // events table sort key, so a re-emitted composer must keep a stable
    // timestamp for ReplacingMergeTree to collapse versions.
    let timestamp = epoch_ms_to_record_ts(created_at_ms).unwrap_or_default();

    let mut record = Map::new();
    record.insert("type".to_string(), json!("cursor_composer"));
    record.insert("sessionId".to_string(), json!(composer_id));
    record.insert("timestamp".to_string(), json!(timestamp));
    record.insert("messageCount".to_string(), json!(headers));

    copy_fields(
        data,
        &mut record,
        &[
            "name",
            "subtitle",
            "unifiedMode",
            "forceMode",
            "agentBackend",
            "status",
            "createdAt",
            "lastUpdatedAt",
            "totalLinesAdded",
            "totalLinesRemoved",
            "contextUsagePercent",
        ],
    );
    if !name.is_empty() {
        // `title` is what MCP session-info extraction looks for first.
        record.insert("title".to_string(), json!(name));
    }
    if let Some(workspace) = data.get("workspaceIdentifier") {
        if let Some(fs_path) = workspace.pointer("/uri/fsPath").and_then(Value::as_str) {
            record.insert("workspacePath".to_string(), json!(fs_path));
        }
        if let Some(id) = workspace.get("id").and_then(Value::as_str) {
            record.insert("workspaceId".to_string(), json!(id));
        }
    }
    if let Some(breakdown) = data.get("promptTokenBreakdown") {
        if let Some(used) = breakdown.get("totalUsedTokens").and_then(Value::as_i64) {
            record.insert("promptTokensUsed".to_string(), json!(used));
        }
        if let Some(max) = breakdown.get("maxTokens").and_then(Value::as_i64) {
            record.insert("promptTokensMax".to_string(), json!(max));
        }
    }

    let (line_no, offset) = stable_coordinates("composerData", composer_id, "cursor_composer");
    Some(SyntheticRecord {
        record: Value::Object(record),
        source_line_no: line_no,
        source_offset: offset,
    })
}

fn synthesize_bubble_record(
    composer_id: &str,
    bubble_id: &str,
    data: &Value,
) -> Option<SyntheticRecord> {
    let bubble_type = data.get("type").and_then(Value::as_i64).unwrap_or(0);
    if bubble_type != 1 && bubble_type != 2 {
        return None;
    }

    let mut record = Map::new();
    record.insert("type".to_string(), json!("cursor_bubble"));
    record.insert("sessionId".to_string(), json!(composer_id));
    record.insert("bubbleId".to_string(), json!(bubble_id));
    record.insert("bubbleType".to_string(), json!(bubble_type));
    if let Some(created_at) = data.get("createdAt").and_then(Value::as_str) {
        record.insert("timestamp".to_string(), json!(created_at));
    }

    let mut text = data
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if text.trim().is_empty() && bubble_type == 1 {
        if let Some(rich_text) = data.get("richText").and_then(Value::as_str) {
            text = flatten_rich_text(rich_text);
        }
    }
    if !text.trim().is_empty() {
        record.insert("text".to_string(), json!(text));
    }

    copy_fields(
        data,
        &mut record,
        &[
            "requestId",
            "capabilityType",
            "thinkingDurationMs",
            "turnDurationMs",
        ],
    );

    if let Some(thinking_text) = data.pointer("/thinking/text").and_then(Value::as_str) {
        if !thinking_text.trim().is_empty() {
            record.insert("thinking".to_string(), json!({ "text": thinking_text }));
        }
    }

    if let Some(tool_data) = data.get("toolFormerData").and_then(Value::as_object) {
        record.insert(
            "toolFormerData".to_string(),
            sanitize_tool_former_data(tool_data),
        );
    }

    // A bubble with no text, no thinking, and no tool call (placeholder rows
    // observed while Cursor streams) normalizes to nothing useful — but it
    // will re-emit under the same logical UID once content lands, so skip it.
    if !record.contains_key("text")
        && !record.contains_key("thinking")
        && !record.contains_key("toolFormerData")
    {
        return None;
    }

    let pk = format!("{composer_id}:{bubble_id}");
    let (line_no, offset) = stable_coordinates("bubbleId", &pk, "cursor_bubble");
    Some(SyntheticRecord {
        record: Value::Object(record),
        source_line_no: line_no,
        source_offset: offset,
    })
}

fn copy_fields(source: &Value, target: &mut Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            if !value.is_null() {
                target.insert((*field).to_string(), value.clone());
            }
        }
    }
}

/// Flattens a ProseMirror-style `richText` document to its text nodes.
fn flatten_rich_text(rich_text: &str) -> String {
    fn collect(node: &Value, out: &mut Vec<String>) {
        match node {
            Value::Object(map) => {
                if map.get("type").and_then(Value::as_str) == Some("text") {
                    if let Some(text) = map.get("text").and_then(Value::as_str) {
                        out.push(text.to_string());
                    }
                }
                if let Some(children) = map.get("content") {
                    collect(children, out);
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect(item, out);
                }
            }
            _ => {}
        }
    }

    let Ok(doc) = serde_json::from_str::<Value>(rich_text) else {
        return String::new();
    };
    let mut parts = Vec::new();
    collect(&doc, &mut parts);
    parts.join("\n")
}

/// Sanitizes `toolFormerData` for the synthetic record:
///   * drops `toolCallBinary` (a protobuf duplicate of params+result — it
///     doubles screenshot bubbles to ~2.4 MB each);
///   * parses the JSON-string `params` / `rawArgs` / `result` / `error`
///     fields into structured values;
///   * elides any string longer than [`LONG_STRING_ELIDE_CHARS`] (base64
///     screenshots and similar payloads).
fn sanitize_tool_former_data(tool_data: &Map<String, Value>) -> Value {
    let mut sanitized = Map::new();
    for (field, value) in tool_data {
        if field == "toolCallBinary" {
            continue;
        }
        let mut next = if matches!(field.as_str(), "params" | "rawArgs" | "result" | "error") {
            parse_embedded_json(value)
        } else {
            value.clone()
        };
        elide_long_strings(&mut next);
        sanitized.insert(field.clone(), next);
    }
    Value::Object(sanitized)
}

/// Cursor stores tool params/results as JSON *strings* (sometimes doubly
/// encoded for MCP tools). Decode one level when possible so payloads stay
/// structured; leave non-JSON strings untouched.
fn parse_embedded_json(value: &Value) -> Value {
    match value {
        Value::String(text) => match serde_json::from_str::<Value>(text) {
            Ok(parsed) if parsed.is_object() || parsed.is_array() => parsed,
            _ => value.clone(),
        },
        _ => value.clone(),
    }
}

fn elide_long_strings(value: &mut Value) {
    match value {
        Value::String(text) => {
            if text.chars().count() > LONG_STRING_ELIDE_CHARS {
                let prefix: String = text.chars().take(256).collect();
                *value = Value::String(format!(
                    "{prefix}… <moraine: elided {} chars>",
                    text.chars().count()
                ));
            }
        }
        Value::Array(items) => {
            for item in items {
                elide_long_strings(item);
            }
        }
        Value::Object(map) => {
            for (_, item) in map.iter_mut() {
                elide_long_strings(item);
            }
        }
        _ => {}
    }
}

fn truncate_chars_local(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RowBatch;
    use serde_json::json;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::time::timeout;

    const COMPOSER_ID: &str = "11111111-2222-4333-8444-555555555555";
    const USER_BUBBLE_ID: &str = "aaaaaaaa-1111-4111-8111-111111111111";
    const THINKING_BUBBLE_ID: &str = "bbbbbbbb-2222-4222-8222-222222222222";
    const TOOL_BUBBLE_ID: &str = "cccccccc-3333-4333-8333-333333333333";

    fn unique_db_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("moraine-sqlite-poll-{name}-{suffix}.vscdb"))
    }

    fn create_kv_db(path: &PathBuf) -> Connection {
        let connection = Connection::open(path).expect("create fixture db");
        connection
            .execute_batch(
                "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);
                 CREATE TABLE ItemTable (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
            )
            .expect("create tables");
        connection
    }

    fn put(connection: &Connection, key: &str, value: &Value) {
        connection
            .execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, serde_json::to_vec(value).expect("serialize value")],
            )
            .expect("insert kv row");
    }

    fn composer_value(name: &str, header_count: usize) -> Value {
        json!({
            "_v": 16,
            "composerId": COMPOSER_ID,
            "name": name,
            "subtitle": "Edited ideas.py",
            "unifiedMode": "agent",
            "agentBackend": "cursor-agent",
            "status": "completed",
            "createdAt": 1778205877751i64,
            "lastUpdatedAt": 1778205947428i64,
            "workspaceIdentifier": {
                "id": "ws-1",
                "uri": {"fsPath": "/Users/demo/project"}
            },
            "totalLinesAdded": 37,
            "totalLinesRemoved": 0,
            "promptTokenBreakdown": {"totalUsedTokens": 21121, "maxTokens": 272000},
            "fullConversationHeadersOnly": (0..header_count)
                .map(|idx| json!({"bubbleId": format!("bubble-{idx}"), "type": 1}))
                .collect::<Vec<_>>(),
        })
    }

    fn user_bubble_value() -> Value {
        json!({
            "_v": 3,
            "type": 1,
            "bubbleId": USER_BUBBLE_ID,
            "createdAt": "2026-05-08T02:04:37.835Z",
            "requestId": "badfdd27-0a9a-497a-b959-79a5caac5fe0",
            "text": "I'm thinking of some cooking ideas.",
            "richText": "{\"type\":\"doc\",\"content\":[]}",
        })
    }

    fn thinking_bubble_value() -> Value {
        json!({
            "_v": 3,
            "type": 2,
            "bubbleId": THINKING_BUBBLE_ID,
            "createdAt": "2026-05-08T02:04:39.829Z",
            "capabilityType": 30,
            "thinking": {"text": "**Considering recipe suggestions**", "signature": ""},
            "thinkingDurationMs": 2292,
        })
    }

    fn tool_bubble_value(status: &str, with_result: bool) -> Value {
        let mut tool = json!({
            "tool": 38,
            "name": "edit_file_v2",
            "toolCallId": "call_bPJLcsry\nctc_0cc5dfa4",
            "status": status,
            "params": "{\"relativeWorkspacePath\":\"/Users/demo/project/ideas.py\"}",
            "toolCallBinary": "AAAA-binary-duplicate-AAAA",
        });
        if with_result {
            tool.as_object_mut().expect("tool object").insert(
                "result".to_string(),
                json!("{\"afterContentId\":\"composer.content.abc\"}"),
            );
        }
        json!({
            "_v": 3,
            "type": 2,
            "bubbleId": TOOL_BUBBLE_ID,
            "createdAt": "2026-05-08T02:05:34.020Z",
            "capabilityType": 15,
            "toolFormerData": tool,
        })
    }

    fn seed_fixture_db(path: &PathBuf) -> Connection {
        let connection = create_kv_db(path);
        put(
            &connection,
            &format!("composerData:{COMPOSER_ID}"),
            &composer_value("Cooking ideas inspiration", 3),
        );
        put(
            &connection,
            &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
            &user_bubble_value(),
        );
        put(
            &connection,
            &format!("bubbleId:{COMPOSER_ID}:{THINKING_BUBBLE_ID}"),
            &thinking_bubble_value(),
        );
        put(
            &connection,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("pending", false),
        );
        connection
    }

    fn sqlite_work(path: &PathBuf) -> WorkItem {
        WorkItem {
            source_name: "cursor-sqlite-test".to_string(),
            harness: "cursor".to_string(),
            format: SOURCE_FORMAT_CURSOR_SQLITE.to_string(),
            path: path.to_string_lossy().to_string(),
        }
    }

    async fn drain_batches(rx: &mut mpsc::Receiver<SinkMessage>) -> Vec<RowBatch> {
        let mut out = Vec::new();
        while let Ok(Some(SinkMessage::Batch(batch))) =
            timeout(Duration::from_millis(50), rx.recv()).await
        {
            out.push(batch);
        }
        out
    }

    async fn run_poll(
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
    ) -> Vec<RowBatch> {
        let config = moraine_config::AppConfig::default();
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);

        process_cursor_sqlite_db(&config, work, checkpoints.clone(), sink_tx, &metrics)
            .await
            .expect("cursor_sqlite poll should succeed");
        let batches = drain_batches(&mut sink_rx).await;

        // Apply the final checkpoint exactly like the sink would after a
        // successful flush.
        if let Some(cp) = batches.last().and_then(|batch| batch.checkpoint.clone()) {
            let key = checkpoint_key(&cp.source_name, &cp.source_file);
            checkpoints.write().await.insert(key, cp);
        }
        batches
    }

    fn all_event_rows(batches: &[RowBatch]) -> Vec<Value> {
        batches
            .iter()
            .flat_map(|batch| batch.event_rows.iter().cloned())
            .collect()
    }

    fn event_uid_by_kind(rows: &[Value], event_kind: &str) -> Vec<String> {
        rows.iter()
            .filter(|row| row.get("event_kind").and_then(Value::as_str) == Some(event_kind))
            .map(|row| {
                row.get("event_uid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    fn cleanup(path: &PathBuf) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", path.to_string_lossy(), suffix));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_poll_emits_composer_and_bubble_events() {
        let path = unique_db_path("first-poll");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let batches = run_poll(&work, &checkpoints).await;
        let rows = all_event_rows(&batches);

        assert_eq!(event_uid_by_kind(&rows, "session_meta").len(), 1);
        assert_eq!(event_uid_by_kind(&rows, "message").len(), 1, "user text");
        assert_eq!(event_uid_by_kind(&rows, "reasoning").len(), 1);
        assert_eq!(
            event_uid_by_kind(&rows, "tool_call").len(),
            1,
            "pending tool emits the call side only"
        );
        assert!(
            event_uid_by_kind(&rows, "tool_result").is_empty(),
            "pending tool must not emit a result yet"
        );

        // Every event belongs to the composer session.
        for row in &rows {
            assert_eq!(
                row.get("session_id").and_then(Value::as_str),
                Some(COMPOSER_ID)
            );
        }

        // The session_meta payload carries the composer name as title.
        let meta = rows
            .iter()
            .find(|row| row.get("event_kind").and_then(Value::as_str) == Some("session_meta"))
            .expect("session_meta event");
        let payload: Value = serde_json::from_str(
            meta.get("payload_json")
                .and_then(Value::as_str)
                .unwrap_or("{}"),
        )
        .expect("session_meta payload parses");
        assert_eq!(
            payload.get("title").and_then(Value::as_str),
            Some("Cooking ideas inspiration")
        );

        // toolCallBinary is stripped from every payload.
        for row in &rows {
            let payload = row
                .get("payload_json")
                .and_then(Value::as_str)
                .unwrap_or_default();
            assert!(
                !payload.contains("toolCallBinary"),
                "payload must not carry toolCallBinary: {payload}"
            );
        }

        let checkpoint = batches
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("final checkpoint");
        assert_eq!(checkpoint.last_offset, 1, "first poll sequence");
        assert_eq!(checkpoint.last_line_no, 4, "relevant keys observed");
        assert!(checkpoint.cursor_json.contains("kv_hashes"));
        assert_ne!(checkpoint.schema_fingerprint, 0);

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unchanged_db_is_a_noop_on_the_next_poll() {
        let path = unique_db_path("noop");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let first = run_poll(&work, &checkpoints).await;
        assert!(!first.is_empty());

        let second = run_poll(&work, &checkpoints).await;
        assert!(
            second.is_empty(),
            "unchanged database must produce zero batches; got {}",
            second.len()
        );

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mutated_tool_bubble_reemits_the_same_event_uid() {
        let path = unique_db_path("mutate");
        let db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let first = run_poll(&work, &checkpoints).await;
        let first_rows = all_event_rows(&first);
        let first_tool_uids = event_uid_by_kind(&first_rows, "tool_call");
        assert_eq!(first_tool_uids.len(), 1);

        // The tool call completes in place — same key, new value.
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("completed", true),
        );

        let second = run_poll(&work, &checkpoints).await;
        let second_rows = all_event_rows(&second);

        let second_tool_uids = event_uid_by_kind(&second_rows, "tool_call");
        assert_eq!(
            first_tool_uids, second_tool_uids,
            "logical identity must survive payload mutation"
        );
        assert_eq!(
            event_uid_by_kind(&second_rows, "tool_result").len(),
            1,
            "completed tool emits its result side"
        );
        assert!(
            event_uid_by_kind(&second_rows, "session_meta").is_empty()
                && event_uid_by_kind(&second_rows, "message").is_empty(),
            "unchanged keys must not re-emit"
        );

        let tool_rows: Vec<&Value> = second
            .iter()
            .flat_map(|batch| batch.tool_rows.iter())
            .collect();
        assert_eq!(tool_rows.len(), 2, "request + response tool_io rows");
        for row in tool_rows {
            assert_eq!(
                row.get("tool_call_id").and_then(Value::as_str),
                Some("call_bPJLcsry"),
                "newline-joined toolCallId is split to its first line"
            );
            assert_eq!(
                row.get("tool_name").and_then(Value::as_str),
                Some("edit_file_v2")
            );
        }

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn null_binary_and_ghost_values_are_tolerated_without_errors() {
        let path = unique_db_path("tolerance");
        let db = create_kv_db(&path);
        // Ghost composer (no headers, no name) — UI shell, not a session.
        put(
            &db,
            "composerData:99999999-9999-4999-8999-999999999999",
            &json!({"composerId": "99999999-9999-4999-8999-999999999999",
                     "createdAt": 1778205877751i64,
                     "fullConversationHeadersOnly": []}),
        );
        // NULL value (observed on composerData:empty-state-draft).
        db.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES ('composerData:empty-state-draft', NULL)",
            [],
        )
        .expect("insert null row");
        // Binary garbage under a relevant prefix.
        db.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![
                format!("bubbleId:{COMPOSER_ID}:binary"),
                vec![0xffu8, 0x00, 0x9c, 0x01]
            ],
        )
        .expect("insert binary row");

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let batches = run_poll(&work, &checkpoints).await;

        let rows = all_event_rows(&batches);
        assert!(rows.is_empty(), "nothing normalizable: {rows:?}");
        let error_rows: usize = batches.iter().map(|batch| batch.error_rows.len()).sum();
        assert_eq!(error_rows, 0, "tolerated values must not emit error rows");

        // The checkpoint still advances so the keys are not re-scanned.
        let checkpoint = batches
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("checkpoint");
        assert_eq!(checkpoint.last_line_no, 3);

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_mismatch_emits_one_error_and_preserves_cursor() {
        let path = unique_db_path("schema-mismatch");
        let db = Connection::open(&path).expect("create db");
        db.execute_batch("CREATE TABLE unrelated (id INTEGER PRIMARY KEY);")
            .expect("create unrelated table");
        drop(db);

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let first = run_poll(&work, &checkpoints).await;
        let first_errors: Vec<Value> = first
            .iter()
            .flat_map(|batch| batch.error_rows.iter().cloned())
            .collect();
        assert_eq!(first_errors.len(), 1);
        assert_eq!(
            first_errors[0].get("error_kind").and_then(Value::as_str),
            Some(ERROR_KIND_SCHEMA)
        );

        let second = run_poll(&work, &checkpoints).await;
        let second_errors: usize = second.iter().map(|batch| batch.error_rows.len()).sum();
        assert_eq!(
            second_errors, 0,
            "persistent schema mismatch is reported once, not per poll"
        );

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_key_space_is_skipped_with_an_error() {
        let path = unique_db_path("too-large");
        let db = create_kv_db(&path);
        {
            let tx_value = json!({"type": 1, "text": "x"});
            let blob = serde_json::to_vec(&tx_value).expect("serialize");
            let mut stmt = db
                .prepare("INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)")
                .expect("prepare");
            db.execute_batch("BEGIN").expect("begin");
            for idx in 0..(MAX_RELEVANT_KEYS + 1) {
                stmt.execute(rusqlite::params![
                    format!("bubbleId:{COMPOSER_ID}:k{idx:05}"),
                    blob
                ])
                .expect("insert");
            }
            db.execute_batch("COMMIT").expect("commit");
        }

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let batches = run_poll(&work, &checkpoints).await;

        let errors: Vec<Value> = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter().cloned())
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].get("error_kind").and_then(Value::as_str),
            Some(ERROR_KIND_TOO_LARGE)
        );
        assert!(all_event_rows(&batches).is_empty());

        cleanup(&path);
    }

    #[test]
    fn synthesize_skips_unknown_families_and_flattens_rich_text() {
        assert!(synthesize_cursor_sqlite_record("agentKv:blob:abc", b"{}").is_none());
        assert!(synthesize_cursor_sqlite_record("checkpointId:a:b", b"{}").is_none());

        let bubble = json!({
            "type": 1,
            "bubbleId": "b",
            "createdAt": "2026-05-08T02:04:37.835Z",
            "text": "",
            "richText": "{\"type\":\"doc\",\"content\":[{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"hello from rich text\"}]}]}",
        });
        let record = synthesize_cursor_sqlite_record(
            "bubbleId:c:b",
            serde_json::to_string(&bubble)
                .expect("serialize")
                .as_bytes(),
        )
        .expect("user bubble synthesizes");
        assert_eq!(
            record.record.get("text").and_then(Value::as_str),
            Some("hello from rich text")
        );
    }

    #[test]
    fn long_tool_strings_are_elided() {
        let huge = "A".repeat(LONG_STRING_ELIDE_CHARS + 10);
        let bubble = json!({
            "type": 2,
            "bubbleId": "b",
            "createdAt": "2026-05-08T02:05:34.020Z",
            "capabilityType": 15,
            "toolFormerData": {
                "name": "mcp-browser-take_screenshot",
                "toolCallId": "call_x",
                "status": "completed",
                "result": serde_json::to_string(&json!({"content": [{"type": "image", "data": huge}]}))
                    .expect("serialize result"),
            }
        });
        let record = synthesize_cursor_sqlite_record(
            "bubbleId:c:b",
            serde_json::to_string(&bubble)
                .expect("serialize")
                .as_bytes(),
        )
        .expect("tool bubble synthesizes");
        let serialized = serde_json::to_string(&record.record).expect("serialize record");
        assert!(
            serialized.len() < 10_000,
            "screenshot payload must be elided, got {} bytes",
            serialized.len()
        );
        assert!(serialized.contains("elided"));
    }
}
