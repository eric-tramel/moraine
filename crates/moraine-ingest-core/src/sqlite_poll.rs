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
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

mod nac;
pub(crate) use nac::process_nac_sqlite_db;

mod opencode;
pub(crate) use opencode::process_opencode_sqlite_db;

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

/// Byte budget for one page's raw values. A page of screenshot-bearing tool
/// bubbles (~2.4 MB each with the `toolCallBinary` duplicate) would otherwise
/// buffer over a gigabyte at `SCAN_PAGE_SIZE` rows; the cap ends the page
/// early so the scan's working set stays bounded regardless of value sizes.
pub(crate) const SCAN_PAGE_MAX_BYTES: usize = 32 * 1024 * 1024;

const CURSOR_STATE_VERSION: u32 = 1;

const ERROR_KIND_OPEN: &str = "sqlite_open_error";
const ERROR_KIND_SCHEMA: &str = "sqlite_schema_mismatch";
const ERROR_KIND_TOO_LARGE: &str = "sqlite_cursor_too_large";
const ERROR_KIND_SCAN: &str = "sqlite_scan_error";
const ERROR_KIND_MIXED_SNAPSHOT: &str = "sqlite_mixed_snapshot";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
struct StatFingerprint {
    db_len: u64,
    db_mtime_ns: u64,
    wal_len: u64,
    wal_mtime_ns: u64,
    shm_len: u64,
    shm_mtime_ns: u64,
}

/// Persisted poll cursor (the checkpoint's `cursor_json` payload).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct CursorState {
    version: u32,
    format: String,
    #[serde(default)]
    stat: StatFingerprint,
    /// kv key → content-hash (hex u64). `BTreeMap` keeps serialization stable.
    #[serde(default)]
    kv_hashes: BTreeMap<String, String>,
    /// Hash of normalized project exclusion globs used for this scan.
    #[serde(default)]
    project_exclusions_hash: u64,
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

/// After this many consecutive no-op scans, a database is considered
/// stat-noisy and rescans are throttled to `NOOP_RESCAN_MIN_INTERVAL`.
/// Below the threshold every stat change scans immediately, so ordinary
/// write→poll flows (and tests) never wait on the throttle.
const NOOP_SCAN_BACKOFF_THRESHOLD: u32 = 3;

/// Minimum interval between full scans of a stat-noisy database. This bounds
/// pickup latency: after an idle stretch the first relevant write can wait up
/// to this long, but the scan it triggers resets the streak, so an actively
/// streaming session tails in real time again.
const NOOP_RESCAN_MIN_INTERVAL: Duration = Duration::from_secs(15);

/// Volatile per-database poll state (issue #443). Cursor touches its DB, WAL,
/// and SHM sidecars continuously (heartbeats, `ItemTable` writes) without
/// changing any transcript-relevant key. Persisting a durable checkpoint — or
/// even re-hashing every relevant value — on each touch pins ingest and
/// ClickHouse CPU at idle, so scans that change nothing durable record the
/// stat fingerprint they covered here instead of in `ingest_checkpoints`.
/// Losing this map on restart costs one redundant no-op scan per database.
/// Entries for databases that vanish from disk are never evicted; growth is
/// bounded by the number of distinct watched database paths.
struct VolatilePollEntry {
    source_generation: u32,
    stat: StatFingerprint,
    consecutive_noop_scans: u32,
    last_scan_at: Instant,
}

/// One map per ingest pipeline, created in `run_ingestor` and threaded
/// alongside the committed-checkpoint map — never process-global, so a
/// pipeline always starts from durable state only.
#[derive(Clone, Default)]
pub(crate) struct VolatilePollMap {
    entries: Arc<Mutex<HashMap<String, VolatilePollEntry>>>,
}

impl VolatilePollMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, VolatilePollEntry>> {
        self.entries
            .lock()
            .expect("volatile poll state mutex poisoned")
    }

    /// True when a poll should be skipped without scanning: either the last
    /// no-op scan already covered the current stat fingerprint (the durable
    /// checkpoint is intentionally stale after a no-op scan), or the database
    /// is stat-noisy and still inside its rescan backoff window.
    fn should_skip_poll(
        &self,
        cp_key: &str,
        source_generation: u32,
        current_stat: &StatFingerprint,
    ) -> bool {
        let map = self.lock();
        let Some(entry) = map.get(cp_key) else {
            return false;
        };
        if entry.source_generation != source_generation {
            return false;
        }
        if entry.stat == *current_stat {
            return true;
        }
        entry.consecutive_noop_scans >= NOOP_SCAN_BACKOFF_THRESHOLD
            && entry.last_scan_at.elapsed() < NOOP_RESCAN_MIN_INTERVAL
    }

    /// Record a scan that changed nothing durable: remember the stat
    /// fingerprint it covered and extend the no-op streak.
    fn record_noop_scan(&self, cp_key: &str, source_generation: u32, stat: StatFingerprint) {
        let mut map = self.lock();
        let entry = map
            .entry(cp_key.to_string())
            .and_modify(|entry| {
                if entry.source_generation != source_generation {
                    entry.consecutive_noop_scans = 0;
                }
            })
            .or_insert(VolatilePollEntry {
                source_generation,
                stat,
                consecutive_noop_scans: 0,
                last_scan_at: Instant::now(),
            });
        entry.source_generation = source_generation;
        entry.stat = stat;
        entry.consecutive_noop_scans = entry.consecutive_noop_scans.saturating_add(1);
        entry.last_scan_at = Instant::now();
    }

    /// Forget volatile state after a scan that persisted a durable *data*
    /// checkpoint: that checkpoint now carries the authoritative cursor, and
    /// crash recovery must never be suppressed by stale volatile state.
    /// Error-marker checkpoints deliberately do not clear — the failure path
    /// keeps the entry and refreshes its clock via `record_failed_scan`.
    fn clear(&self, cp_key: &str) {
        self.lock().remove(cp_key);
    }

    /// A failed scan refreshes the backoff clock of an existing no-op streak
    /// so a persistently failing stat-noisy database retries on the throttled
    /// cadence instead of every reconcile tick. Without a streak this is a
    /// no-op and the failure retries exactly as before.
    fn record_failed_scan(&self, cp_key: &str) {
        if let Some(entry) = self.lock().get_mut(cp_key) {
            entry.last_scan_at = Instant::now();
        }
    }
}

/// One synthetic record ready for `normalize_record`, with the stable
/// source coordinates required by issue #361 decision 7.
#[derive(Debug, Clone)]
pub struct SyntheticRecord {
    pub record: Value,
    /// Session-sticky directory used only for project exclusion decisions.
    pub project_dir: String,
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

fn project_exclusions_hash(config: &AppConfig) -> u64 {
    if config.ingest.exclude_project_dirs.is_empty() {
        return 0;
    }
    hash_str(
        &serde_json::to_string(&config.ingest.exclude_project_dirs)
            .expect("serializing string exclusion globs cannot fail"),
    )
}

fn sqlite_policy_fingerprint(format: &str, exclusions_hash: u64) -> String {
    format!("sqlite-publication-v1:{format}:{exclusions_hash:016x}")
}

fn sqlite_data_version(connection: &Connection) -> Result<i64> {
    connection
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .context("failed to query PRAGMA data_version")
}

fn database_scan_still_valid(source_file: &str, scan_inode: u64) -> Result<()> {
    let metadata = std::fs::metadata(source_file)
        .with_context(|| format!("database disappeared while scanning {source_file}"))?;
    let final_inode = source_inode_for_file(source_file, &metadata);
    anyhow::ensure!(
        final_inode == scan_inode,
        "database inode changed while scanning {source_file}: {scan_inode} -> {final_inode}"
    );
    Ok(())
}

async fn begin_database_replay(
    sink_tx: &mpsc::Sender<SinkMessage>,
    checkpoint: &Checkpoint,
    scan_boundary: u64,
    policy_fingerprint: &str,
) -> Result<()> {
    let transition = crate::CheckpointTransition::begin_replay(
        checkpoint,
        checkpoint.source_inode,
        scan_boundary,
        policy_fingerprint,
    );
    crate::publication::send_begin_replay(sink_tx, transition).await?;
    Ok(())
}

async fn finalize_database_replay(
    sink_tx: &mpsc::Sender<SinkMessage>,
    checkpoint: &Checkpoint,
    scan_boundary: u64,
    policy_fingerprint: &str,
) -> Result<()> {
    let transition = crate::CheckpointTransition::finalize_replay(
        checkpoint,
        checkpoint.source_inode,
        scan_boundary,
        policy_fingerprint,
    );
    match crate::publication::send_finalize_replay(sink_tx, transition).await? {
        crate::FinalizeReplayOutcome::Published(_) => {}
        crate::FinalizeReplayOutcome::StagedForMirror => {
            tracing::debug!(
                source = %checkpoint.source_name,
                path = %checkpoint.source_file,
                "replacement finalization staged until mirror catch-up barrier"
            );
        }
    }
    Ok(())
}

async fn block_database_replay(
    sink_tx: &mpsc::Sender<SinkMessage>,
    checkpoint: &Checkpoint,
    reason: impl Into<String>,
) -> Result<()> {
    let transition = crate::CheckpointTransition::blocked(checkpoint, reason.into());
    crate::publication::send_block_replay(sink_tx, transition).await?;
    Ok(())
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
    poll_state: &VolatilePollMap,
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
    let had_committed = committed.is_some();

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.clone(),
        source_inode: inode,
        source_generation: 1,
        status: "active".to_string(),
        ..Default::default()
    });

    let mut state = CursorState::parse(&checkpoint.cursor_json);
    let current_exclusions_hash = project_exclusions_hash(config);
    let policy_fingerprint =
        sqlite_policy_fingerprint(SOURCE_FORMAT_CURSOR_SQLITE, current_exclusions_hash);

    // A replaced database file is a new generation: every logical identity
    // (and therefore every event UID) starts over, and the hash cursor is
    // meaningless for the new file's contents. A changed exclusion set also
    // replays the database so rows skipped under the prior policy can return.
    let generation_changed = had_committed && checkpoint.source_inode != inode;
    let exclusions_changed =
        had_committed && state.project_exclusions_hash != current_exclusions_hash;
    let retry_blocked_replay = checkpoint.status == "replaying"
        || (checkpoint.status == "error" && !checkpoint.block_reason.is_empty());
    let starts_replacement = generation_changed || exclusions_changed;
    if starts_replacement {
        checkpoint.source_inode = inode;
        checkpoint.source_generation =
            crate::publication::checked_next_generation(checkpoint.source_generation)
                .context("source generation exhausted while replacing cursor_sqlite database")?;
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
    }
    let replacement_replay = starts_replacement || retry_blocked_replay;
    if replacement_replay {
        state = CursorState::fresh();
    }
    state.project_exclusions_hash = current_exclusions_hash;
    checkpoint.policy_fingerprint = policy_fingerprint.clone();
    checkpoint.status = if replacement_replay {
        "replaying".to_string()
    } else {
        "active".to_string()
    };
    checkpoint.block_reason.clear();
    let scan_boundary = checkpoint
        .last_offset
        .checked_add(1)
        .context("cursor_sqlite poll sequence exhausted")?;
    if replacement_replay {
        begin_database_replay(&sink_tx, &checkpoint, scan_boundary, &policy_fingerprint).await?;
    }

    // Cheap no-change short-circuit: nothing touched the database or its WAL
    // sidecars since the last successful poll.
    if state.stat == current_stat && state.last_error.is_empty() {
        return Ok(());
    }

    // Volatile short-circuit + rescan backoff (issue #443): no-op scans leave
    // the durable checkpoint untouched, so their coverage lives here instead.
    if poll_state.should_skip_poll(&cp_key, checkpoint.source_generation, &current_stat) {
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

            // A no-op scan: only the stat fingerprint moved — no record was
            // emitted and nothing the durable checkpoint carries changed.
            // Persisting a checkpoint here would append an
            // `ingest_checkpoints` row per WAL touch forever (issue #443);
            // record the covered stat in volatile state instead and send
            // nothing. The comparison is structural (stat normalized away)
            // so any future `CursorState` field is durable by default.
            let prior_state_covered = {
                let mut prior = state.clone();
                prior.stat = new_state.stat;
                prior
            };
            let scan_is_noop = had_committed
                && !starts_replacement
                && !retry_blocked_replay
                && records.is_empty()
                && checkpoint.status == "active"
                && new_state == prior_state_covered
                && schema_fingerprint == checkpoint.schema_fingerprint
                && relevant_keys == checkpoint.last_line_no;
            if scan_is_noop {
                poll_state.record_noop_scan(&cp_key, checkpoint.source_generation, new_state.stat);
                return Ok(());
            }

            if let Err(exc) = database_scan_still_valid(&source_file, inode) {
                if replacement_replay {
                    let mut blocked = checkpoint.clone();
                    blocked.status = "error".to_string();
                    blocked.block_reason = exc.to_string();
                    block_database_replay(&sink_tx, &blocked, exc.to_string()).await?;
                }
                return Err(exc);
            }

            let mut batch = RowBatch::default();
            let mut replay_block_reason = None::<String>;
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
                // No cwd hint: kv rows interleave many composers, so a linear
                // hint chain would bleed one session's workspace path onto
                // another's bubbles. Composer records carry `workspacePath`
                // themselves, and the scan stamps it onto changed bubbles
                // (`stamp_bubble_workspace`) so every row is self-describing.
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
                        if replacement_replay && replay_block_reason.is_none() {
                            replay_block_reason = Some(format!(
                                "cursor_sqlite row {} failed normalization: {exc}",
                                synthetic.source_line_no
                            ));
                        }
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
                last_offset: scan_boundary,
                last_line_no: relevant_keys,
                status: if replacement_replay {
                    "replaying".to_string()
                } else {
                    "active".to_string()
                },
                cursor_json: new_state.serialize(),
                source_fingerprint: inode,
                schema_fingerprint,
                policy_fingerprint: policy_fingerprint.clone(),
                scan_inode: inode,
                scan_boundary,
                final_scan_complete: !replacement_replay,
                compatibility_prepared: !replacement_replay,
                backend_caught_up: !replacement_replay,
                ..checkpoint.clone()
            };

            batch.checkpoint = Some(final_checkpoint.clone());
            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending final cursor_sqlite batch")?;
            if replacement_replay {
                if let Some(reason) = replay_block_reason {
                    let blocked_checkpoint = Checkpoint {
                        status: "error".to_string(),
                        final_scan_complete: false,
                        compatibility_prepared: false,
                        backend_caught_up: false,
                        block_reason: reason.clone(),
                        ..final_checkpoint
                    };
                    block_database_replay(&sink_tx, &blocked_checkpoint, reason).await?;
                    poll_state.clear(&cp_key);
                    return Ok(());
                }
                let active_checkpoint = Checkpoint {
                    status: "active".to_string(),
                    final_scan_complete: true,
                    compatibility_prepared: true,
                    backend_caught_up: true,
                    ..final_checkpoint
                };
                finalize_database_replay(
                    &sink_tx,
                    &active_checkpoint,
                    scan_boundary,
                    &policy_fingerprint,
                )
                .await?;
            }
            poll_state.clear(&cp_key);

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
            poll_state.record_failed_scan(&cp_key);

            // A repeat of the failure already marked in the committed
            // checkpoint sends nothing: the marker is durable, and reconcile
            // re-polls every tick — re-sending an identical checkpoint would
            // grow ingest_checkpoints (and re-serialize the whole kv-hash
            // map) forever for a permanently failing database.
            if state.last_error == error_kind {
                return Ok(());
            }

            // Emit each failure mode once per state change, not once per
            // reconcile tick — ingest_errors is append-only.
            let mut batch = RowBatch::default();
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
                "error_text": error_text.clone(),
                "raw_fragment": "",
            }));

            // Preserve the data cursor (kv hashes and stat fingerprint stay
            // as-is so the next poll retries); only the error marker moves.
            // last_offset deliberately does NOT advance: a successful poll's
            // checkpoint may still be pending in the sink's flush window, and
            // merge_checkpoint replaces pending entries on last_offset >= —
            // an error marker must never outrank (and discard) a fresh data
            // cursor.
            let mut error_state = state.clone();
            error_state.last_error = error_kind.to_string();
            let error_checkpoint = Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: checkpoint.last_offset,
                last_line_no: checkpoint.last_line_no,
                status: if replacement_replay {
                    "error".to_string()
                } else {
                    "active".to_string()
                },
                cursor_json: error_state.serialize(),
                source_fingerprint: inode,
                schema_fingerprint: checkpoint.schema_fingerprint,
                policy_fingerprint: policy_fingerprint.clone(),
                scan_inode: inode,
                scan_boundary,
                block_reason: if replacement_replay {
                    error_text.clone()
                } else {
                    String::new()
                },
                ..checkpoint.clone()
            };
            if !replacement_replay {
                batch.checkpoint = Some(error_checkpoint.clone());
            }

            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending cursor_sqlite error batch")?;
            if replacement_replay {
                block_database_replay(&sink_tx, &error_checkpoint, error_text).await?;
            }
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
                error_text: format!("{exc:#}"),
            }
        }
    };
    // Opening a WAL database can create or touch its reader-owned `-shm`
    // sidecar. Use the post-open state as the stable scan baseline.
    let opened_stat = stat_fingerprint(db_path).unwrap_or_default();
    let data_version_before = match sqlite_data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read pre-scan data_version: {exc:#}"),
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
                error_text: format!("{exc:#}"),
            };
        }
    }

    let mut new_state = CursorState {
        version: CURSOR_STATE_VERSION,
        format: SOURCE_FORMAT_CURSOR_SQLITE.to_string(),
        stat: StatFingerprint::default(),
        project_exclusions_hash: prior.project_exclusions_hash,
        kv_hashes: BTreeMap::new(),
        last_error: String::new(),
    };
    let mut records = Vec::<SyntheticRecord>::new();
    let mut relevant_keys = 0u64;
    let mut workspace_cache = HashMap::<String, Option<String>>::new();

    for prefix in RELEVANT_PREFIXES {
        let scan = scan_prefix(
            &connection,
            prefix,
            &prior.kv_hashes,
            &mut new_state.kv_hashes,
            &mut records,
            &mut workspace_cache,
        );
        match scan {
            Ok(seen) => relevant_keys += seen,
            Err(exc) => {
                return ScanOutcome::Failed {
                    error_kind: ERROR_KIND_SCAN,
                    error_text: format!("{exc:#}"),
                };
            }
        }
        // Re-check the ceiling against what the scan actually saw: the count
        // ran in its own read transaction, so keys written in between could
        // otherwise grow cursor_json past the checkpoint payload budget.
        if new_state.kv_hashes.len() > MAX_RELEVANT_KEYS {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_TOO_LARGE,
                error_text: format!(
                    "{} relevant keys exceed the {MAX_RELEVANT_KEYS} checkpoint ceiling; \
                     use Cursor agent transcripts (JSONL) for this history",
                    new_state.kv_hashes.len()
                ),
            };
        }
    }

    let data_version_after = match sqlite_data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read post-scan data_version: {exc:#}"),
            }
        }
    };
    if data_version_before != data_version_after || stat_fingerprint(db_path) != Some(opened_stat) {
        return ScanOutcome::Failed {
            error_kind: ERROR_KIND_MIXED_SNAPSHOT,
            error_text:
                "Cursor database changed during the paged scan; retrying without advancing the cursor"
                    .to_string(),
        };
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

/// Opens the database read-only, retrying with `immutable=1` when the plain
/// open is refused.
///
/// A cleanly closed WAL-mode database on read-only media cannot be opened by
/// a plain read-only connection: SQLite must materialize the WAL shared-memory
/// index and the filesystem refuses the `-shm` create (observed with Cursor
/// `state.vscdb` files under the sandbox's read-only bind mount). The
/// `immutable=1` retry is safe precisely because the sidecar files are absent:
/// no writer is active and every page lives in the main file. Databases with
/// live sidecars never reach the fallback — the existing `-shm` is readable
/// and the plain open succeeds.
fn open_read_only(db_path: &str) -> Result<Connection> {
    match open_and_probe(db_path, false) {
        Ok(connection) => Ok(connection),
        Err(exc) if blocked_by_readonly_media(&exc) => open_and_probe(db_path, true)
            .with_context(|| format!("immutable fallback failed for {db_path}")),
        Err(exc) => Err(exc),
    }
}

fn open_and_probe(db_path: &str, immutable: bool) -> Result<Connection> {
    let connection = if immutable {
        Connection::open_with_flags(
            sqlite_immutable_uri(db_path),
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )
    } else {
        Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
    }
    .with_context(|| format!("failed to open {db_path} read-only"))?;
    connection
        .busy_timeout(std::time::Duration::from_millis(500))
        .context("failed to set busy_timeout")?;
    // Defense in depth on a live application database: never write, never
    // checkpoint the WAL.
    connection
        .pragma_update(None, "query_only", "ON")
        .context("failed to set query_only")?;
    // SQLite opens lazily; force the first page read here so open-class
    // failures are reported as open errors instead of leaking out of the
    // first schema query as a bogus schema mismatch.
    connection
        .query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
        .with_context(|| format!("failed to read {db_path}"))?;
    Ok(connection)
}

/// The sidecar-create failure surfaces as `SQLITE_CANTOPEN` on Linux and as
/// `SQLITE_READONLY` (extended: `SQLITE_READONLY_DIRECTORY`) on macOS; both
/// mean "the filesystem refused the WAL sidecars", so both retry immutable.
fn blocked_by_readonly_media(exc: &anyhow::Error) -> bool {
    exc.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(failure, _))
                if matches!(
                    failure.code,
                    rusqlite::ErrorCode::CannotOpen | rusqlite::ErrorCode::ReadOnly
                )
        )
    })
}

/// SQLite URI filenames percent-decode `%XX` and treat `?`/`#` as delimiters;
/// escape those so arbitrary paths round-trip.
fn sqlite_immutable_uri(db_path: &str) -> String {
    let mut encoded = String::with_capacity(db_path.len());
    for ch in db_path.chars() {
        match ch {
            '%' => encoded.push_str("%25"),
            '#' => encoded.push_str("%23"),
            '?' => encoded.push_str("%3F"),
            other => encoded.push(other),
        }
    }
    format!("file:{encoded}?immutable=1")
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
                // Strictly greater, matching scan_prefix's seed of the bare
                // prefix — a key exactly equal to the prefix is never scanned
                // and must not count toward the ceiling.
                "SELECT count(*) FROM cursorDiskKV WHERE key > ?1 AND key < ?2",
                rusqlite::params![prefix, prefix_range_end(prefix)],
                |row| row.get(0),
            )
            .with_context(|| format!("failed counting keys for prefix {prefix}"))?;
        total = total.saturating_add(count.max(0) as usize);
    }
    Ok(total)
}

/// Pages through one key prefix, recording content hashes for every key and
/// synthesizing records for keys that are new or changed. Paging keeps each
/// implicit read transaction short (issue #361 decision 4), and synthesizing
/// page-by-page keeps raw value bytes from accumulating: sanitization
/// (toolCallBinary drop, long-string elision) shrinks the retained record by
/// orders of magnitude for media-heavy bubbles, so the scan's peak raw-byte
/// buffer is one page, additionally capped by `SCAN_PAGE_MAX_BYTES`.
fn scan_prefix(
    connection: &Connection,
    prefix: &str,
    prior_hashes: &BTreeMap<String, String>,
    new_hashes: &mut BTreeMap<String, String>,
    records: &mut Vec<SyntheticRecord>,
    workspace_cache: &mut HashMap<String, Option<String>>,
) -> Result<u64> {
    let range_end = prefix_range_end(prefix);
    let mut last_key = prefix.to_string();
    let mut seen = 0u64;

    loop {
        let mut page: Vec<(String, Option<Vec<u8>>)> = Vec::new();
        let mut page_bytes = 0usize;
        {
            let mut stmt = connection
                .prepare_cached(
                    "SELECT key, value FROM cursorDiskKV \
                     WHERE key > ?1 AND key < ?2 ORDER BY key LIMIT ?3",
                )
                .context("failed to prepare prefix scan")?;
            let mut rows = stmt
                .query(rusqlite::params![
                    last_key,
                    range_end,
                    SCAN_PAGE_SIZE as i64
                ])
                .context("prefix scan query failed")?;
            while let Some(row) = rows.next().context("prefix scan row failed")? {
                let key = row.get::<_, String>(0).context("prefix scan key failed")?;
                // Cursor writes JSON documents as TEXT even though the
                // column is declared BLOB; accept any storage class instead
                // of trusting the declared affinity.
                let value = match row.get_ref(1).context("prefix scan value failed")? {
                    ValueRef::Null => None,
                    ValueRef::Text(text) => Some(text.to_vec()),
                    ValueRef::Blob(blob) => Some(blob.to_vec()),
                    ValueRef::Integer(int) => Some(int.to_string().into_bytes()),
                    ValueRef::Real(real) => Some(real.to_string().into_bytes()),
                };
                page_bytes += value.as_ref().map_or(0, Vec::len);
                page.push((key, value));
                if page_bytes >= SCAN_PAGE_MAX_BYTES {
                    break;
                }
            }
        }

        // A row-capped page may have more rows behind it; so may a
        // byte-capped one.
        let more = page.len() == SCAN_PAGE_SIZE || page_bytes >= SCAN_PAGE_MAX_BYTES;

        for (key, value) in page {
            seen += 1;
            let bytes = value.unwrap_or_default();
            let hash = format!("{:016x}", hash_bytes(&bytes));
            let unchanged = prior_hashes.get(&key) == Some(&hash);
            if !unchanged && !bytes.is_empty() {
                if let Some(mut record) = synthesize_cursor_sqlite_record(&key, &bytes) {
                    stamp_bubble_workspace(connection, workspace_cache, &mut record);
                    records.push(record);
                }
            }
            new_hashes.insert(key.clone(), hash);
            last_key = key;
        }

        if !more {
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

    // Always stamp the *creation* time: `event_ts` participates in the
    // events table sort key, so a re-emitted composer must keep a stable
    // timestamp for ReplacingMergeTree to collapse versions. A composer
    // without a positive createdAt is deferred entirely — a placeholder
    // timestamp would strand a permanent epoch-dated row in the sort key
    // when the real value appears on a later re-emission.
    let created_at_ms = data
        .get("createdAt")
        .and_then(Value::as_i64)
        .filter(|ms| *ms > 0)?;
    let timestamp = epoch_ms_to_record_ts(created_at_ms)?;

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
        project_dir: String::new(),
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

    // Same stability rule as composers: a bubble without a parseable
    // createdAt is deferred until Cursor writes one (it does at creation).
    // A fallback timestamp would spam timestamp_parse_error rows on every
    // re-emission of a mutating bubble and strand a permanent epoch-dated
    // duplicate once the real value appears — event_ts is in the sort key.
    // Validation uses the same parser the normalizer applies downstream.
    let created_at = data
        .get("createdAt")
        .and_then(Value::as_str)
        .filter(|raw| crate::sources::shared::parse_record_ts(raw).is_some())?;

    let mut record = Map::new();
    record.insert("type".to_string(), json!("cursor_bubble"));
    record.insert("sessionId".to_string(), json!(composer_id));
    record.insert("bubbleId".to_string(), json!(bubble_id));
    record.insert("bubbleType".to_string(), json!(bubble_type));
    record.insert("timestamp".to_string(), json!(created_at));

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
        project_dir: String::new(),
        source_line_no: line_no,
        source_offset: offset,
    })
}

/// Bubble rows carry no working directory of their own, and the route
/// resolver's sticky session pin lives only in process memory: after an
/// ingest restart, a poll delta containing only mutated bubbles (parent
/// composer blob unchanged, so no composer record re-emits) would resolve
/// to no backend and silently miss the mirror while the hash cursor still
/// advances. Stamping the parent composer's `workspacePath` onto each
/// changed bubble makes bubble rows self-describing for route resolution,
/// like claude_code records. One point query per distinct composer per
/// scan, cached; unchanged bubbles never trigger it.
fn stamp_bubble_workspace(
    connection: &Connection,
    cache: &mut HashMap<String, Option<String>>,
    synthetic: &mut SyntheticRecord,
) {
    let Some(record) = synthetic.record.as_object_mut() else {
        return;
    };
    if record.get("type").and_then(Value::as_str) != Some("cursor_bubble") {
        return;
    }
    let Some(composer_id) = record.get("sessionId").and_then(Value::as_str) else {
        return;
    };
    let composer_id = composer_id.to_string();
    let workspace = cache
        .entry(composer_id.clone())
        .or_insert_with(|| lookup_composer_workspace(connection, &composer_id))
        .clone();
    if let Some(path) = workspace {
        record.insert("workspacePath".to_string(), json!(path));
    }
}

/// Best-effort read of `workspaceIdentifier.uri.fsPath` from a bubble's
/// parent `composerData:` blob. Works even for composers the synthesizer
/// defers (no positive `createdAt` yet): Cursor writes the workspace
/// identifier at creation. Any failure resolves to `None`.
fn lookup_composer_workspace(connection: &Connection, composer_id: &str) -> Option<String> {
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT value FROM cursorDiskKV WHERE key = ?1",
            rusqlite::params![format!("composerData:{composer_id}")],
            |row| match row.get_ref(0)? {
                ValueRef::Text(text) => Ok(text.to_vec()),
                ValueRef::Blob(blob) => Ok(blob.to_vec()),
                _ => Ok(Vec::new()),
            },
        )
        .ok()?;
    let parsed: Value = serde_json::from_slice(&bytes).ok()?;
    parsed
        .pointer("/workspaceIdentifier/uri/fsPath")
        .and_then(Value::as_str)
        .map(str::to_string)
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
        Value::String(text) if text.chars().count() > LONG_STRING_ELIDE_CHARS => {
            let prefix: String = text.chars().take(256).collect();
            *value = Value::String(format!(
                "{prefix}… <moraine: elided {} chars>",
                text.chars().count()
            ));
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
    use moraine_config::SourceFormat;
    use serde_json::json;
    use std::path::{Path, PathBuf};
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
        // Real Cursor writes JSON as TEXT despite the BLOB-declared column;
        // fixtures must match or the scan's storage-class handling goes
        // untested (this exact mismatch shipped once).
        connection
            .execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, serde_json::to_string(value).expect("serialize value")],
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

    fn sqlite_work(path: &Path) -> WorkItem {
        WorkItem {
            source_name: "cursor-sqlite-test".to_string(),
            harness: "cursor".to_string(),
            format: SourceFormat::CursorSqlite,
            source_glob: String::new(),
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
        run_poll_with_state(work, checkpoints, &VolatilePollMap::new()).await
    }

    async fn run_poll_with_state(
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        poll_state: &VolatilePollMap,
    ) -> Vec<RowBatch> {
        let config = moraine_config::AppConfig::default();
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        let process = process_cursor_sqlite_db(
            &config,
            work,
            checkpoints.clone(),
            poll_state,
            sink_tx,
            &metrics,
        );
        tokio::pin!(process);
        let mut batches = Vec::new();
        let mut finalized = None;
        loop {
            tokio::select! {
                result = &mut process => {
                    result.expect("cursor_sqlite poll should succeed");
                    break;
                }
                message = sink_rx.recv() => match message.expect("cursor test sink remains open") {
                    SinkMessage::Batch(batch) => batches.push(batch),
                    SinkMessage::BeginReplay { transition, ack }
                    | SinkMessage::BlockReplay { transition, ack }
                    | SinkMessage::MirrorCaughtUp { transition, ack } => {
                        let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                            checkpoint_revision: 1,
                            operation_id: transition.checkpoint.operation_id,
                        }));
                    }
                    SinkMessage::FinalizeReplay { transition, ack } => {
                        finalized = Some(transition.checkpoint);
                        let _ = ack.send(Ok(
                            crate::publication::FinalizeReplayOutcome::Published(
                                crate::publication::PublicationAck {
                                    checkpoint_revision: 2,
                                    publication_revision: 1,
                                    already_published: false,
                                },
                            ),
                        ));
                    }
                }
            }
        }
        while let Ok(message) = sink_rx.try_recv() {
            if let SinkMessage::Batch(batch) = message {
                batches.push(batch);
            }
        }

        // Apply the final checkpoint exactly like the sink would after a
        // successful flush.
        if let Some(cp) =
            finalized.or_else(|| batches.last().and_then(|batch| batch.checkpoint.clone()))
        {
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

    fn cleanup(path: &Path) {
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
    async fn cursor_sqlite_replays_rows_when_exclusions_change() {
        let path = unique_db_path("exclusion-replay");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let poll_state = VolatilePollMap::new();

        let mut excluded_config = moraine_config::AppConfig::default();
        excluded_config.ingest.exclude_project_dirs = vec!["/Users/demo/project/**".to_string()];
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        process_cursor_sqlite_db(
            &excluded_config,
            &work,
            checkpoints.clone(),
            &poll_state,
            sink_tx,
            &metrics,
        )
        .await
        .expect("excluded Cursor poll");
        let excluded_batches = drain_batches(&mut sink_rx).await;
        assert!(
            all_event_rows(&excluded_batches).is_empty(),
            "excluded session rows must not reach the sink"
        );
        let checkpoint = excluded_batches
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("excluded poll must persist its cursor");
        checkpoints.write().await.insert(
            checkpoint_key(&checkpoint.source_name, &checkpoint.source_file),
            checkpoint,
        );

        let included_config = moraine_config::AppConfig::default();
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        let process = process_cursor_sqlite_db(
            &included_config,
            &work,
            checkpoints.clone(),
            &poll_state,
            sink_tx,
            &metrics,
        );
        tokio::pin!(process);
        let mut replayed = Vec::new();
        let mut final_checkpoint = None;
        loop {
            tokio::select! {
                result = &mut process => {
                    result.expect("Cursor replay after exclusion removal");
                    break;
                }
                message = sink_rx.recv() => match message.expect("cursor replay sink remains open") {
                    SinkMessage::Batch(batch) => replayed.push(batch),
                    SinkMessage::BeginReplay { transition, ack } => {
                        assert_eq!(transition.checkpoint.source_generation, 2);
                        let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                            checkpoint_revision: 1,
                            operation_id: transition.checkpoint.operation_id,
                        }));
                    }
                    SinkMessage::FinalizeReplay { transition, ack } => {
                        final_checkpoint = Some(transition.checkpoint);
                        let _ = ack.send(Ok(
                            crate::publication::FinalizeReplayOutcome::Published(
                                crate::publication::PublicationAck {
                                    checkpoint_revision: 2,
                                    publication_revision: 2,
                                    already_published: false,
                                },
                            ),
                        ));
                    }
                    SinkMessage::BlockReplay { .. } | SinkMessage::MirrorCaughtUp { .. } => {
                        panic!("successful exclusion replay must not block")
                    }
                }
            }
        }
        while let Ok(SinkMessage::Batch(batch)) = sink_rx.try_recv() {
            replayed.push(batch);
        }
        let final_checkpoint = final_checkpoint.expect("final replay transition");
        assert_eq!(final_checkpoint.status, "active");
        checkpoints.write().await.insert(
            checkpoint_key(&final_checkpoint.source_name, &final_checkpoint.source_file),
            final_checkpoint,
        );
        assert!(
            !all_event_rows(&replayed).is_empty(),
            "removing exclusions must replay previously skipped rows"
        );

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cursor_sqlite_can_queue_oversized_replay_row_before_final_checkpoint() {
        let path = unique_db_path("sink-limit-envelope");
        let db = create_kv_db(&path);
        put(
            &db,
            &format!("composerData:{COMPOSER_ID}"),
            &composer_value("Oversized replay", 1),
        );
        let payload_bytes = crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES + 1024 * 1024;
        assert!(payload_bytes < SCAN_PAGE_MAX_BYTES);
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
            &json!({
                "_v": 3,
                "type": 1,
                "bubbleId": USER_BUBBLE_ID,
                "createdAt": "2026-05-08T02:04:37.835Z",
                "text": "x".repeat(payload_bytes),
            }),
        );

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let poll_state = VolatilePollMap::new();

        let mut excluded = moraine_config::AppConfig::default();
        excluded.ingest.exclude_project_dirs = vec!["/Users/demo/project/**".to_string()];
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        process_cursor_sqlite_db(
            &excluded,
            &work,
            checkpoints.clone(),
            &poll_state,
            sink_tx,
            &metrics,
        )
        .await
        .expect("excluded initial Cursor poll");
        let excluded_batches = drain_batches(&mut sink_rx).await;
        let initial = excluded_batches
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("excluded poll checkpoint");
        checkpoints.write().await.insert(
            checkpoint_key(&initial.source_name, &initial.source_file),
            initial,
        );

        let included = moraine_config::AppConfig::default();
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        let process = process_cursor_sqlite_db(
            &included,
            &work,
            checkpoints,
            &poll_state,
            sink_tx,
            &metrics,
        );
        tokio::pin!(process);
        let mut replay_batches = Vec::new();
        loop {
            tokio::select! {
                result = &mut process => {
                    result.expect("Cursor oversized replacement replay");
                    break;
                }
                message = sink_rx.recv() => match message.expect("Cursor replay sink remains open") {
                    SinkMessage::Batch(batch) => replay_batches.push(batch),
                    SinkMessage::BeginReplay { transition, ack } => {
                        let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                            checkpoint_revision: 1,
                            operation_id: transition.checkpoint.operation_id,
                        }));
                    }
                    SinkMessage::FinalizeReplay { transition: _, ack } => {
                        let _ = ack.send(Ok(
                            crate::publication::FinalizeReplayOutcome::Published(
                                crate::publication::PublicationAck {
                                    checkpoint_revision: 2,
                                    publication_revision: 2,
                                    already_published: false,
                                },
                            ),
                        ));
                    }
                    SinkMessage::BlockReplay { .. } | SinkMessage::MirrorCaughtUp { .. } => {
                        panic!("valid Cursor replay should reach finalization")
                    }
                }
            }
        }
        while let Ok(SinkMessage::Batch(batch)) = sink_rx.try_recv() {
            replay_batches.push(batch);
        }

        let oversized_index = replay_batches
            .iter()
            .position(|batch| {
                batch.raw_rows.iter().any(|row| {
                    serde_json::to_vec(row).is_ok_and(|encoded| {
                        encoded.len() > crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
                    })
                })
            })
            .expect("Cursor scanner emits a sink-oversized row under its page cap");
        let checkpoint_index = replay_batches
            .iter()
            .position(|batch| batch.checkpoint.is_some())
            .expect("Cursor replay queues a final checkpoint");
        assert!(oversized_index < checkpoint_index);
        assert!(replay_batches[oversized_index].checkpoint.is_none());

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cursor_reference_metadata_can_exceed_sink_limit_while_raw_row_stays_bounded() {
        let path = unique_db_path("reference-expansion-envelope");
        let db = create_kv_db(&path);
        put(
            &db,
            &format!("composerData:{COMPOSER_ID}"),
            &composer_value("Reference expansion replay", 1),
        );

        // Cursor stores one compact nested path once, while event_links
        // expands that prefix into every reference's field_path. This keeps
        // the source row comfortably below 10 MiB while making the derived
        // link cross ClickHouse's per-object limit.
        let nested_key = format!("nested_{}", "x".repeat(249));
        let references = (0..33_000)
            .map(|_| json!({"path": "p"}))
            .collect::<Vec<_>>();
        let mut params = Map::new();
        params.insert(nested_key, Value::Array(references));
        let mut bubble = tool_bubble_value("pending", false);
        *bubble
            .pointer_mut("/toolFormerData/params")
            .expect("tool params") = Value::Object(params);
        let source_bytes = serde_json::to_vec(&bubble).unwrap().len();
        assert!(source_bytes < crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES);
        assert!(source_bytes < SCAN_PAGE_MAX_BYTES);
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &bubble,
        );

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let poll_state = VolatilePollMap::new();

        let mut excluded = moraine_config::AppConfig::default();
        excluded.ingest.exclude_project_dirs = vec!["/Users/demo/project/**".to_string()];
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        process_cursor_sqlite_db(
            &excluded,
            &work,
            checkpoints.clone(),
            &poll_state,
            sink_tx,
            &metrics,
        )
        .await
        .expect("excluded initial Cursor reference poll");
        let excluded_batches = drain_batches(&mut sink_rx).await;
        let initial = excluded_batches
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("excluded reference checkpoint");
        checkpoints.write().await.insert(
            checkpoint_key(&initial.source_name, &initial.source_file),
            initial,
        );

        let included = moraine_config::AppConfig::default();
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        let process = process_cursor_sqlite_db(
            &included,
            &work,
            checkpoints,
            &poll_state,
            sink_tx,
            &metrics,
        );
        tokio::pin!(process);
        let mut replay_batches = Vec::new();
        loop {
            tokio::select! {
                result = &mut process => {
                    result.expect("Cursor reference replacement replay");
                    break;
                }
                message = sink_rx.recv() => match message.expect("Cursor reference sink remains open") {
                    SinkMessage::Batch(batch) => replay_batches.push(batch),
                    SinkMessage::BeginReplay { transition, ack } => {
                        let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                            checkpoint_revision: 1,
                            operation_id: transition.checkpoint.operation_id,
                        }));
                    }
                    SinkMessage::FinalizeReplay { transition: _, ack } => {
                        let _ = ack.send(Ok(
                            crate::publication::FinalizeReplayOutcome::Published(
                                crate::publication::PublicationAck {
                                    checkpoint_revision: 2,
                                    publication_revision: 2,
                                    already_published: false,
                                },
                            ),
                        ));
                    }
                    SinkMessage::BlockReplay { .. } | SinkMessage::MirrorCaughtUp { .. } => {
                        panic!("valid Cursor reference replay should reach finalization")
                    }
                }
            }
        }
        while let Ok(SinkMessage::Batch(batch)) = sink_rx.try_recv() {
            replay_batches.push(batch);
        }

        let (link_batch_index, oversized_link) = replay_batches
            .iter()
            .enumerate()
            .find_map(|(index, batch)| {
                batch
                    .link_rows
                    .iter()
                    .find(|row| {
                        serde_json::to_vec(row).is_ok_and(|encoded| {
                            encoded.len() > crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
                        })
                    })
                    .map(|row| (index, row))
            })
            .expect("Cursor reference metadata expands beyond the sink limit");
        let owner_uid = oversized_link
            .get("event_uid")
            .and_then(Value::as_str)
            .expect("link owner UID");
        let link_batch = &replay_batches[link_batch_index];
        assert!(link_batch.checkpoint.is_none());
        assert!(link_batch
            .event_rows
            .iter()
            .any(|row| { row.get("event_uid").and_then(Value::as_str) == Some(owner_uid) }));
        assert!(replay_batches
            .iter()
            .flat_map(|batch| &batch.raw_rows)
            .all(|row| {
                serde_json::to_vec(row).is_ok_and(|encoded| {
                    encoded.len() < crate::sink::CLICKHOUSE_JSON_EACH_ROW_OBJECT_MAX_BYTES
                })
            }));

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
    async fn irrelevant_write_scans_but_persists_no_checkpoint() {
        let path = unique_db_path("noop-checkpoint");
        let db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();

        let first = run_poll_with_state(&work, &checkpoints, &poll_state).await;
        assert!(!first.is_empty());
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let baseline = checkpoints
            .read()
            .await
            .get(&cp_key)
            .cloned()
            .expect("committed checkpoint after first poll");

        // Cursor constantly rewrites non-transcript keys (issue #443): the
        // stat fingerprint moves but no relevant key changes.
        put(&db, "agentKv:blob:0000", &json!({"opaque": "blob"}));

        let second = run_poll_with_state(&work, &checkpoints, &poll_state).await;
        assert!(
            second.is_empty(),
            "a no-op scan must send nothing durable; got {} batches",
            second.len()
        );

        // The same stat fingerprint is now covered by volatile state, so a
        // re-poll without further writes also sends nothing.
        let third = run_poll_with_state(&work, &checkpoints, &poll_state).await;
        assert!(
            third.is_empty(),
            "volatile stat coverage must short-circuit"
        );

        // A relevant write below the backoff threshold is picked up
        // immediately and persists a durable checkpoint again.
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("completed", true),
        );
        let fourth = run_poll_with_state(&work, &checkpoints, &poll_state).await;
        let rows = all_event_rows(&fourth);
        assert_eq!(event_uid_by_kind(&rows, "tool_result").len(), 1);
        let cp = fourth
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("relevant change persists a checkpoint");
        assert_eq!(
            cp.last_offset,
            baseline.last_offset + 1,
            "poll sequence advances once for the relevant change, not per WAL touch"
        );

        cleanup(&path);
    }

    #[test]
    fn noop_backoff_state_machine() {
        let map = VolatilePollMap::new();
        let key = "cursor-sqlite-test:noop-backoff-state-machine";
        let stat = |db_len: u64| StatFingerprint {
            db_len,
            ..Default::default()
        };

        assert!(
            !map.should_skip_poll(key, 1, &stat(1)),
            "no volatile state yet"
        );
        map.record_failed_scan(key);
        assert!(
            !map.should_skip_poll(key, 1, &stat(1)),
            "a failed scan without a streak leaves retries unthrottled"
        );

        map.record_noop_scan(key, 1, stat(1));
        assert!(
            map.should_skip_poll(key, 1, &stat(1)),
            "covered stat skips without a scan"
        );
        assert!(
            !map.should_skip_poll(key, 1, &stat(2)),
            "below the streak threshold a fresh stat scans immediately"
        );

        map.record_noop_scan(key, 1, stat(2));
        map.record_noop_scan(key, 1, stat(3));
        assert!(
            map.should_skip_poll(key, 1, &stat(4)),
            "at the streak threshold fresh stats are throttled"
        );
        assert!(
            !map.should_skip_poll(key, 2, &stat(4)),
            "a new generation ignores stale volatile state"
        );

        // A failed scan on an established streak refreshes the backoff clock
        // (streak and coverage untouched), keeping a persistently failing
        // stat-noisy database on the throttled cadence.
        map.record_failed_scan(key);
        assert!(
            map.should_skip_poll(key, 1, &stat(5)),
            "failed scan keeps the throttle window open"
        );
        assert!(
            map.should_skip_poll(key, 1, &stat(3)),
            "failed scan does not invalidate the covered stat"
        );

        map.clear(key);
        assert!(
            !map.should_skip_poll(key, 1, &stat(4)),
            "a durable checkpoint write clears the throttle"
        );
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
    async fn changed_bubbles_are_stamped_with_the_composer_workspace() {
        let path = unique_db_path("bubble-workspace");
        let db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let _ = run_poll(&work, &checkpoints).await;

        // Bubble-only delta, as after an ingest restart: the parent composer
        // blob is unchanged, so no composer record re-emits to re-pin the
        // session's route. The bubble row itself must carry the cwd.
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("completed", true),
        );
        let second = run_poll(&work, &checkpoints).await;

        let raw_rows: Vec<Value> = second
            .iter()
            .flat_map(|batch| batch.raw_rows.iter().cloned())
            .collect();
        assert_eq!(raw_rows.len(), 1, "only the mutated bubble re-emits");
        assert_eq!(
            raw_rows[0].get("cwd").and_then(Value::as_str),
            Some("/Users/demo/project"),
            "bubble rows must be self-describing for route resolution"
        );

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
    async fn blob_stored_json_values_normalize() {
        let path = unique_db_path("blob-values");
        let db = create_kv_db(&path);
        // Cursor writes TEXT today, but both storage classes must normalize
        // identically — the column is declared BLOB and writers can change.
        for (key, value) in [
            (
                format!("composerData:{COMPOSER_ID}"),
                composer_value("Blob storage", 1),
            ),
            (
                format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
                user_bubble_value(),
            ),
        ] {
            db.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, serde_json::to_vec(&value).expect("serialize value")],
            )
            .expect("insert blob kv row");
        }

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let batches = run_poll(&work, &checkpoints).await;

        let rows = all_event_rows(&batches);
        for expected in ["session_meta", "message"] {
            assert!(
                rows.iter()
                    .any(|row| row.get("event_kind").and_then(Value::as_str) == Some(expected)),
                "blob-stored rows must emit {expected}: {rows:?}"
            );
        }

        cleanup(&path);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn wal_db_in_read_only_directory_opens_via_immutable_fallback() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "moraine-sqlite-poll-rodir-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let path = dir.join("state.vscdb");
        {
            let connection = Connection::open(&path).expect("create fixture db");
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .expect("enable WAL");
            connection
                .execute_batch(
                    "CREATE TABLE cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
                )
                .expect("create tables");
            put(
                &connection,
                &format!("composerData:{COMPOSER_ID}"),
                &composer_value("Read-only media", 1),
            );
            put(
                &connection,
                &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
                &user_bubble_value(),
            );
        }
        // A clean close checkpoints the WAL and removes the sidecars — the
        // exact shape that breaks plain read-only opens on read-only media.
        assert!(
            !dir.join("state.vscdb-wal").exists(),
            "clean close should remove the WAL sidecar"
        );

        let writable = std::fs::metadata(&dir).expect("stat dir").permissions();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
            .expect("make dir read-only");

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let batches = run_poll(&work, &checkpoints).await;

        std::fs::set_permissions(&dir, writable).expect("restore dir permissions");

        let error_rows: Vec<Value> = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter().cloned())
            .collect();
        assert!(
            error_rows.is_empty(),
            "read-only directory must not emit error rows: {error_rows:?}"
        );
        assert!(
            !all_event_rows(&batches).is_empty(),
            "expected events from the read-only db"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn sqlite_immutable_uri_escapes_delimiters() {
        assert_eq!(
            sqlite_immutable_uri("/tmp/cache 100%?x#y.vscdb"),
            "file:/tmp/cache 100%25%3Fx%23y.vscdb?immutable=1"
        );
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

        let first_checkpoint = first
            .last()
            .and_then(|batch| batch.checkpoint.clone())
            .expect("first failure persists the error marker");
        assert_eq!(
            first_checkpoint.last_offset, 0,
            "error checkpoints must not advance last_offset past a pending success checkpoint"
        );

        let second = run_poll(&work, &checkpoints).await;
        let second_errors: usize = second.iter().map(|batch| batch.error_rows.len()).sum();
        assert_eq!(
            second_errors, 0,
            "persistent schema mismatch is reported once, not per poll"
        );
        assert!(
            second.iter().all(|batch| batch.checkpoint.is_none()),
            "a repeated failure must not re-send checkpoints every reconcile tick"
        );

        cleanup(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rows_without_created_at_are_deferred_without_events_or_errors() {
        let path = unique_db_path("no-created-at");
        let db = create_kv_db(&path);
        let mut composer = composer_value("Cooking ideas inspiration", 1);
        composer
            .as_object_mut()
            .expect("composer object")
            .remove("createdAt");
        put(&db, &format!("composerData:{COMPOSER_ID}"), &composer);
        let mut bubble = user_bubble_value();
        bubble
            .as_object_mut()
            .expect("bubble object")
            .remove("createdAt");
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
            &bubble,
        );

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let batches = run_poll(&work, &checkpoints).await;

        // event_ts is in the events sort key: emitting placeholder timestamps
        // would strand permanent epoch-dated duplicates once createdAt
        // appears on a later re-emission. Defer instead.
        assert!(
            all_event_rows(&batches).is_empty(),
            "rows without createdAt must not emit events"
        );
        let error_rows: usize = batches.iter().map(|batch| batch.error_rows.len()).sum();
        assert_eq!(error_rows, 0, "deferral must not emit error rows");

        // The mutation that adds createdAt re-emits with the real timestamp.
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
            &user_bubble_value(),
        );
        let batches = run_poll(&work, &checkpoints).await;
        let rows = all_event_rows(&batches);
        assert_eq!(rows.len(), 1, "bubble emits once createdAt appears");
        assert_eq!(
            rows[0].get("event_ts").and_then(Value::as_str),
            Some("2026-05-08 02:04:37.835"),
            "the real creation time is stamped, not a placeholder"
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
