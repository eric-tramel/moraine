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
use url::Origin;

use super::{
    drive_sweep_slice, hash_str, open_read_only, record_scan_failure, record_scan_ledger,
    record_sweep_slice_committed, sqlite_data_version, stat_fingerprint,
    take_payload_nullable_string, take_payload_required_string, take_payload_text,
    truncate_chars_local, ScanBudget, ScanLedger, StatFingerprint, SweepItem, SweepPlan,
    SweepState, SyntheticRecord, VolatilePollMap, ERROR_KIND_MIXED_SNAPSHOT, ERROR_KIND_OPEN,
    ERROR_KIND_ROW_TOO_LARGE, ERROR_KIND_SCAN, ERROR_KIND_SCHEMA, SCAN_PAGE_MAX_BYTES,
    SCAN_PAGE_SIZE,
};

const NAC_CURSOR_VERSION: u32 = 1;
/// Per-poll cap on synthetic records built. A *degradation* bound, not a
/// failure (issue #601 §2.3): reaching it stops further reads for this poll
/// with `coverage_degraded`, and per-session overflow truncates the part list.
const MAX_NAC_SYNTHETIC_RECORDS: usize = 200_000;
const ERROR_KIND_NORMALIZED_ROW_TOO_LARGE: &str = "nac_normalized_row_too_large";
/// One episode referencing a session that exists nowhere in `sessions`.
/// Reachable in production — the live schema FKs episodes to `threads`, not
/// `sessions` (§1.3) — and passed exactly once, because `episode_high_water`
/// advances over it (§3.2).
const ERROR_KIND_ORPHAN_EPISODE: &str = "nac_orphan_episode";
/// Ceiling on the serialized cursor payload. Enforced by **eviction** of the
/// oldest session cursors (issue #601 §2.3), never by failing the scan; an
/// evicted session re-emits on a later poll, which is safe because NAC
/// records are content-addressed at stable logical coordinates (§6).
const MAX_NAC_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;
/// `NacSessionCursor.metadata_hash` marker for a session row too large to
/// process (> `SCAN_PAGE_MAX_BYTES`). Structural state: it suppresses the
/// per-poll re-emission of the row error while the row stays oversized, and
/// any real hash differs from it, so a shrunk row re-emits normally.
const OVERSIZED_SESSION_MARKER: &str = "oversized-row";
const MAX_NAC_TEXT_CHARS: usize = 200_000;

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
    /// True while some censused session has never been read in this
    /// generation (a budget remainder or an eviction), a fast-path candidate
    /// was budget-skipped, or the episode watermark trails `max(episodes.id)`
    /// — §2.3's persisted resume marker. The cheap stat short-circuit must not
    /// fire while this is set, or a quiet store's cold-ingest remainder is
    /// unreachable forever. Skipped while false so `cursor_json` stays
    /// byte-identical for fully-covered stores (§2.6).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pending_coverage: bool,
    /// §3.2's fast-path watermark, the `(updated_at, session_id)` keyset
    /// position: sessions at or before it in the census are not re-read by the
    /// fast path — a skip justified by the sweep's coverage guarantee (§0),
    /// never by `updated_at` itself, which is ordering-only (no trigger
    /// maintains it — §1.3). Empty means no watermark: every known session is
    /// a candidate, which is both the fresh-generation state and the upgrade
    /// path from a pre-watermark cursor (one exhaustive, hash-suppressed
    /// re-read establishes it). Deterministic structural state (§2.6): it
    /// advances only to positions a committed scan covered.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    updated_at_high_water: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    updated_at_high_water_session: String,
    /// §2.2's durable reconciliation-sweep cursor over `session_id` order.
    /// The sweep is what detects a session mutated without an `updated_at`
    /// bump (G5c) — the coverage the fast path's skip borrows against.
    /// Episodes need no sweep: their id watermark is exact and append-only.
    #[serde(default, skip_serializing_if = "SweepState::is_default")]
    sweep: SweepState,
    /// The stat fingerprint as of the moment the current (or last) sweep
    /// cycle **started** — the content state that cycle is walking. A due
    /// reconcile poll owes a new cycle while this trails the current stat:
    /// content changed after the last cycle began, and the fast path may have
    /// carried some of it unverified (an emitting poll defers its slice, so
    /// without this debt marker the deferral would never land on a store that
    /// then goes quiet — §0's coverage guarantee closed from the emitting
    /// side). Structural, deterministic state in the same class as `stat`
    /// above (§2.6): it advances only when a committed slice starts a cycle.
    #[serde(default, skip_serializing_if = "StatFingerprint::is_default")]
    sweep_baseline: StatFingerprint,
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
            pending_coverage: false,
            updated_at_high_water: String::new(),
            updated_at_high_water_session: String::new(),
            sweep: SweepState::default(),
            sweep_baseline: StatFingerprint::default(),
        }
    }
}

/// §3.2's format guard, half one: the fixed-width `YYYY-MM-DD?HH:MM:SS` shape
/// a NAC `updated_at` must have for lexicographic comparison to be
/// chronologically meaningful, returning its separator byte. Mixed
/// *precisions* are safe (the fraction is a suffix on the fixed-width prefix);
/// mixed *separators* are not (`'T'` 0x54 > `' '` 0x20 dominates every digit
/// after position 10), and a malformed value is comparable to nothing.
fn nac_timestamp_shape(value: &str) -> Option<u8> {
    let bytes = value.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let separator = bytes[10];
    if separator != b' ' && separator != b'T' {
        return None;
    }
    let digits = [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18]
        .iter()
        .all(|&index| bytes[index].is_ascii_digit());
    let punctuation =
        bytes[4] == b'-' && bytes[7] == b'-' && bytes[13] == b':' && bytes[16] == b':';
    (digits && punctuation).then_some(separator)
}

/// §3.2's format guard, half two: two `updated_at` values may be ordered only
/// when both are well-formed **and share one separator**. Everything else is
/// incomparable, and the fast path must never skip on an incomparable value —
/// it reads the row instead (fail open). "Skip the fast path and let the sweep
/// carry it" is the plan's wording; reading the row now is strictly safer than
/// deferring it to the sweep interval, and no silent cross-format comparison
/// ever happens either way.
fn nac_updated_at_comparable(a: &str, b: &str) -> bool {
    matches!(
        (nac_timestamp_shape(a), nac_timestamp_shape(b)),
        (Some(left), Some(right)) if left == right
    )
}

/// Strict keyset order: is `(updated_at, session_id)` after the watermark?
/// Callers must have established comparability first. The session-id tiebreak
/// is what splits a same-timestamp write burst across bound polls — §3.2's
/// inclusive-bound race on a second-precision store
/// (`a_same_timestamp_nac_write_burst_resumes_through_the_session_id_tiebreak`).
fn nac_keyset_after(
    updated_at: &str,
    session_id: &str,
    high_water: &str,
    high_water_session: &str,
) -> bool {
    updated_at > high_water || (updated_at == high_water && session_id > high_water_session)
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
        serde_json::to_string(self).context("failed to serialize NAC cursor")
    }

    /// Evict the oldest session cursors (by `created_at`, then id) until the
    /// serialized payload fits `max_bytes`, returning how many were dropped
    /// (issue #601 §2.3). Eviction replaces the old serialize-time failure:
    /// the ceiling degrades — an evicted session is re-detected and re-emitted
    /// by a later poll — instead of latching the whole database. Removal is in
    /// batches of one-eighth of the map per round, so a pathological
    /// many-small-entries payload does not re-serialize per entry.
    fn evict_to_fit(&mut self, max_bytes: usize) -> u64 {
        let mut evicted = 0u64;
        loop {
            let raw_len = serde_json::to_string(self)
                .map(|raw| raw.len())
                .unwrap_or(0);
            if raw_len <= max_bytes || self.sessions.is_empty() {
                return evicted;
            }
            let mut by_age: Vec<(String, String)> = self
                .sessions
                .iter()
                .map(|(id, cursor)| (cursor.created_at.clone(), id.clone()))
                .collect();
            by_age.sort();
            let batch = (self.sessions.len().div_ceil(8)).max(1);
            for (_, id) in by_age.into_iter().take(batch) {
                self.sessions.remove(&id);
                evicted += 1;
            }
        }
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
        state: Box<NacState>,
        schema_fingerprint: u64,
        relevant_rows: u64,
        /// Per-row skips (§2.3): an un-processable single row — oversized, or
        /// an orphan episode — is reported once and advanced past, never
        /// allowed to fail the scan.
        row_errors: Vec<NacRowError>,
        /// True when this scan drove a sweep slice whose advance is carried in
        /// `state.sweep`; the caller commits the interval clock only after the
        /// checkpoint persisted (mirroring the Cursor adapter's ordering).
        swept: bool,
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
    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.clone(),
        source_inode: inode,
        source_generation: 1,
        status: "active".to_string(),
        ..Default::default()
    });
    let committed_state = NacState::parse(&checkpoint.cursor_json);
    let current_exclusions_hash = super::project_exclusions_hash(config);
    let policy_fingerprint =
        super::sqlite_policy_fingerprint(SOURCE_FORMAT_NAC_SQLITE, current_exclusions_hash);
    let generation_changed = had_committed && checkpoint.source_inode != inode;
    let exclusions_changed =
        had_committed && committed_state.project_exclusions_hash != current_exclusions_hash;
    // The `replaying` disjunct resumes a replay a crash interrupted between
    // `BeginReplay` and `FinalizeReplay`; the `error` disjunct cannot cover it,
    // because a crash never wrote a block reason. Width note at the Cursor
    // site; `a_crash_interrupted_nac_replay_resumes_from_its_replaying_status`
    // fails if it goes.
    let retry_blocked_replay = checkpoint.status == "replaying"
        || (checkpoint.status == "error" && !checkpoint.block_reason.is_empty());
    // A NAC database stuck in `retry_blocked_replay` previously re-scanned the
    // whole store and re-sent `BeginReplay` on every tick with zero throttle,
    // because the volatile check was skipped during replay. Throttle *before*
    // the durable barrier so the barrier always has a scan behind it
    // (issue #601 §2.1(2), §2.5) — and before the episode preflight below, so
    // a durably blocked store does not pay a read per tick either (the
    // OpenCode rewind-preflight rule). This gate now sits *above* the
    // generation bump, so — exactly as at the OpenCode site — both bypass
    // conjuncts are load-bearing: drop either and a store whose file was
    // replaced, or whose exclusion set changed, is ignored for up to
    // `FAILURE_BACKOFF_MAX`.
    // `a_replaced_nac_database_bypasses_the_blocked_replay_throttle` and
    // `an_exclusion_change_bypasses_the_nac_blocked_replay_throttle` fail, one
    // per conjunct.
    if retry_blocked_replay
        && !generation_changed
        && !exclusions_changed
        && !poll_state.failure_retry_due(&cp_key, checkpoint.source_generation)
    {
        return Ok(());
    }

    // §3.2: an episode rewind (`max(episodes.id)` behind the committed
    // watermark) is a truncation of history and must route through #602's
    // replacement path — generation bump, `BeginReplay` barrier, `sessions`
    // reset — never the old in-place `worker_threads.clear()` that exposed
    // partially replayed worker history beside live data in one generation
    // (G8c, `nac_episode_rewind_routes_through_generation_replay`). Detected
    // by preflight, like OpenCode's sequence rewind, so the bump-then-barrier
    // ordering is preserved; the race where the rewind lands between this
    // read and the scan is closed by the scan failing (see `scan_nac_rows`)
    // and this preflight catching it on the retry.
    let episode_rewound = if had_committed
        && current_stat != committed_state.stat
        && committed_state.episode_high_water > 0
    {
        let scan_db_path = source_file.clone();
        let high_water = committed_state.episode_high_water;
        tokio::task::spawn_blocking(move || nac_episodes_rewound(&scan_db_path, high_water))
            .await
            .context("nac_sqlite episode preflight panicked")?
            .unwrap_or(false)
    } else {
        false
    };

    let starts_replacement = generation_changed || exclusions_changed || episode_rewound;
    if starts_replacement {
        checkpoint.source_generation =
            crate::publication::checked_next_generation(checkpoint.source_generation)
                .context("source generation exhausted while replacing nac_sqlite database")?;
        checkpoint.source_inode = inode;
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
    }
    let replacement_replay = starts_replacement || retry_blocked_replay;
    let source_generation = checkpoint.source_generation;
    let mut scan_state = if replacement_replay {
        NacState::default()
    } else {
        committed_state.clone()
    };
    scan_state.project_exclusions_hash = current_exclusions_hash;
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
        .context("nac_sqlite poll sequence exhausted")?;
    if replacement_replay {
        super::begin_database_replay(&sink_tx, &checkpoint, scan_boundary, &policy_fingerprint)
            .await?;
    }

    // Sweep eligibility, conditions 1, 2 and 4 (issue #601 §2.2/§2.4): only a
    // `Reconcile`-triggered poll of a database that is not replaying and whose
    // per-database interval clock has expired requests a slice. Condition 3
    // (a quiet, cheap fast path) is decided inside the scan, where this poll's
    // ledger exists — same split as the Cursor adapter.
    let sweep_requested = work.trigger == crate::WorkTrigger::Reconcile
        && !replacement_replay
        && poll_state.sweep_slice_due(
            &cp_key,
            source_generation,
            std::time::Duration::from_secs(config.ingest.sqlite.sweep_slice_min_interval_seconds),
        );
    // §2.5's `|| !failure_retry_due` disjunct is absent and **must stay absent
    // while the contention clock lives in `failure_retry_due`** — WI-04 must
    // not add it. It is no longer outcome-redundant with `should_skip_poll`'s
    // failure arm below: §3.2's contention exemption keeps that clock out of
    // `should_skip_poll` on purpose, so after a mixed-snapshot rejection
    // `failure_retry_due` is false for up to 60 s while `should_skip_poll`
    // stays false. The disjunct would skip ordinary polls of an actively
    // written NAC store for that whole window — the §6 prompt-visibility
    // regression the exemption exists to prevent. A pre-`should_skip_poll`
    // throttle for the sweep slice must read the fault ladder alone.
    // `an_ordinary_poll_of_a_contended_nac_store_is_not_throttled` fails if it
    // is added. See `plans/601-delta-sqlite.md` §7 WI-10.
    // The `!pending_coverage` conjunct is §2.3's "continue next poll": while
    // a cold-ingest remainder exists an unchanged stat must not end the poll
    // (a quiet store's stat never moves again). Terminates because resumed
    // scans read never-read sessions first, so the debt strictly shrinks
    // (`a_degraded_nac_cold_ingest_completes_without_new_writes`).
    //
    // The sweep-debt conjunct: a durably covered stat proves nothing changed
    // since the last *persisting* scan, but that scan's fast path may have
    // carried sessions unverified — an `updated_at`-silent mutation in the
    // same write burst as an emitting change, or a cycle still mid-keyspace.
    // A due reconcile poll therefore proceeds while the cycle is in progress
    // (`cursor` non-empty) or a new cycle is owed (`sweep_baseline` trails
    // the current stat: content changed after the last cycle started).
    // Terminates: each due slice advances the cursor, the wrap empties it,
    // the baseline catches up to the stat, and this conjunct then stands
    // aside (`a_quiet_nac_store_finishes_its_sweep_cycle_and_then_goes_idle`
    // bounds both sides).
    // Width note (§7.2 F3): `!starts_replacement` and `!retry_blocked_replay`
    // are equivalent mutants here — a replay's `scan_state` is
    // `NacState::default()`, whose default stat fingerprint can never equal a
    // real file's, so the stat conjunct already fails for them. They stay as
    // documentation of intent, not as guarded width.
    // `schema_fingerprint != 0` is not equivalent and is pinned by
    // `a_legacy_cursor_without_a_schema_fingerprint_is_rescanned_once`.
    if !starts_replacement
        && !retry_blocked_replay
        && scan_state.stat == current_stat
        && scan_state.schema_fingerprint != 0
        && scan_state.last_error.is_empty()
        && !scan_state.pending_coverage
        && (!sweep_requested
            || (scan_state.sweep.cursor.is_empty() && scan_state.sweep_baseline == current_stat))
    {
        return Ok(());
    }
    if !replacement_replay && poll_state.should_skip_poll(&cp_key, source_generation, &current_stat)
    {
        // A due sweep may override a *stat-covered or noop-throttled* skip —
        // that cover is volatile bookkeeping over the fast path, and the fast
        // path is exactly what an `updated_at`-silent mutation evades (§0): a
        // mutation noop-covered by a watcher poll would otherwise never be
        // swept on a store that then goes quiet. It must never override a
        // failure or contention backoff, which is what `failure_retry_due`
        // preserves (`a_due_sweep_does_not_override_a_failure_backoff`; the
        // override's lower bound is G5c, whose watcher poll noop-covers the
        // mutation first).
        if !(sweep_requested && poll_state.failure_retry_due(&cp_key, source_generation)) {
            return Ok(());
        }
    }

    let scan_path = source_file.clone();
    let scan_source_name = work.source_name.clone();
    let prior_scan_state = scan_state.clone();
    // The fast-path work budget (issue #601 §2.1), from `[ingest.sqlite]`.
    // Exceeding it degrades coverage newest-first; it never fails the scan.
    // A replacement replay is unbudgeted: its finalize publishes the
    // generation whole, so degrading it would publish a hole through #602
    // (`a_nac_replacement_replay_reads_past_the_fast_path_budget`).
    let budget = if replacement_replay {
        ScanBudget::unbounded()
    } else {
        ScanBudget::fast_path(&config.ingest.sqlite)
    };
    let sweep_plan = sweep_requested.then(|| SweepPlan::from_config(config));
    let (mut outcome, ledger) = tokio::task::spawn_blocking(move || {
        let mut ledger = ScanLedger::default();
        let outcome = scan_nac_database(
            &scan_path,
            &scan_source_name,
            source_generation,
            inode,
            current_stat,
            &prior_scan_state,
            &budget,
            sweep_plan.as_ref(),
            MAX_NAC_CHECKPOINT_BYTES,
            &mut ledger,
        );
        (outcome, ledger)
    })
    .await
    .context("nac_sqlite scan task panicked")?;
    record_scan_ledger(metrics, &ledger);

    // Normalize once (§3.2's double-normalize cleanup): the oversized
    // pre-check used to normalize every record and throw the result away, and
    // the emit loop then normalized everything again. One pass now feeds both;
    // its peak memory is the poll's normalized output, which the fast-path
    // budget bounds on ordinary polls (a replacement replay already holds the
    // full store in `records`, so the replay's peak moves by a constant
    // factor, not a class).
    let (prepared, oversized_row) = match &mut outcome {
        NacScanOutcome::Scanned { records, .. } => {
            link_tool_responses(records, &source_file, source_generation);
            let mut prepared = Vec::with_capacity(records.len());
            let mut oversized = None;
            for synthetic in records.iter() {
                if crate::dispatch::record_project_dir_is_excluded(
                    config,
                    &work.harness,
                    &synthetic.record,
                    &synthetic.project_dir,
                ) {
                    prepared.push(NacPreparedRecord::Excluded);
                    continue;
                }
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
                        if oversized.is_none() {
                            oversized =
                                crate::dispatch::largest_serialized_normalized_row(&normalized)
                                    .filter(|row_size| {
                                        row_size.bytes
                                            > crate::dispatch::CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT
                                    });
                        }
                        prepared.push(NacPreparedRecord::Normalized(Box::new(normalized)));
                    }
                    Err(exc) => prepared.push(NacPreparedRecord::Error(exc.to_string())),
                }
            }
            (prepared, oversized)
        }
        NacScanOutcome::Failed { .. } => (Vec::new(), None),
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
            row_errors,
            swept,
        } => {
            new_state.project_exclusions_hash = current_exclusions_hash;
            let cursor_json = (*new_state).serialize()?;
            let prior_state_covered = {
                let mut prior = scan_state.clone();
                prior.stat = current_stat;
                prior.schema_fingerprint = schema_fingerprint;
                prior
            };
            let scan_is_noop = had_committed
                && !starts_replacement
                && !retry_blocked_replay
                && records.is_empty()
                && checkpoint.status == "active"
                && *new_state == prior_state_covered
                && schema_fingerprint == checkpoint.schema_fingerprint;
            if scan_is_noop {
                poll_state.record_noop_scan(&cp_key, source_generation, current_stat);
                return Ok(());
            }

            if let Err(exc) = super::database_scan_still_valid(&source_file, inode) {
                if replacement_replay {
                    let mut blocked = checkpoint.clone();
                    blocked.status = "error".to_string();
                    blocked.block_reason = exc.to_string();
                    super::block_database_replay(&sink_tx, &blocked, exc.to_string()).await?;
                }
                return Err(exc);
            }

            let mut batch = RowBatch::default();
            // Per-row skips (§2.3): one `ingest_errors` row each. Oversized
            // rows repeat-suppress via their cursor marker; orphan episodes
            // are one-shot because the watermark advances past them.
            for row_error in &row_errors {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": source_generation,
                    "source_line_no": row_error.source_line_no,
                    "source_offset": 0u64,
                    "error_kind": row_error.error_kind,
                    "error_text": row_error.error_text,
                    "raw_fragment": "",
                }));
            }
            let mut replay_block_reason = None::<String>;
            for (synthetic, prepared) in records.iter().zip(prepared) {
                match prepared {
                    NacPreparedRecord::Excluded => continue,
                    NacPreparedRecord::Normalized(normalized) => {
                        batch.extend_normalized(*normalized);
                        batch.lines_processed = batch.lines_processed.saturating_add(1);
                    }
                    NacPreparedRecord::Error(error_text) => {
                        if replacement_replay && replay_block_reason.is_none() {
                            replay_block_reason = Some(format!(
                                "nac_sqlite row {} failed normalization: {error_text}",
                                synthetic.source_line_no
                            ));
                        }
                        let raw_json = serde_json::to_string(&synthetic.record)
                            .unwrap_or_else(|_| "{}".to_string());
                        batch.push_error_row(json!({
                            "source_name": work.source_name,
                            "harness": work.harness,
                            "source_file": source_file,
                            "source_inode": inode,
                            "source_generation": source_generation,
                            "source_line_no": synthetic.source_line_no,
                            "source_offset": synthetic.source_offset,
                            "error_kind": "normalize_error",
                            "error_text": error_text,
                            "raw_fragment": truncate_chars_local(&raw_json, 20_000),
                        }));
                    }
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
            let final_checkpoint = Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation,
                last_offset: scan_boundary,
                last_line_no: relevant_rows,
                status: if replacement_replay {
                    "replaying".to_string()
                } else {
                    "active".to_string()
                },
                cursor_json,
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
                .context("sink channel closed while sending final nac_sqlite batch")?;
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
                    super::block_database_replay(&sink_tx, &blocked_checkpoint, reason).await?;
                    poll_state.record_blocked_replay(&cp_key, source_generation);
                    return Ok(());
                }
                let active_checkpoint = Checkpoint {
                    status: "active".to_string(),
                    final_scan_complete: true,
                    compatibility_prepared: true,
                    backend_caught_up: true,
                    ..final_checkpoint
                };
                super::finalize_database_replay(
                    &sink_tx,
                    &active_checkpoint,
                    scan_boundary,
                    &policy_fingerprint,
                )
                .await?;
            }
            poll_state.clear(&cp_key);
            if swept {
                // Commit the slice only now — after its checkpoint persisted
                // and after `clear` wiped the entry — so the interval clock
                // survives the wipe and a slice whose scan failed or lost the
                // mixed-snapshot bracket is neither counted nor throttling
                // the retry. Same ordering as the Cursor adapter.
                poll_state.record_sweep_slice(&cp_key, source_generation);
                record_sweep_slice_committed(metrics);
                debug!(
                    "{}:{} nac_sqlite sweep slice committed (cursor {:?}, cycles {})",
                    work.source_name,
                    source_file,
                    new_state.sweep.cursor,
                    new_state.sweep.completed_cycles,
                );
            }
            if emitted > 0 {
                debug!(
                    "{}:{} nac_sqlite emitted {} changed records ({} relevant rows, \
                     {} payload rows, {} payload bytes)",
                    work.source_name,
                    source_file,
                    emitted,
                    relevant_rows,
                    ledger.payload_rows,
                    ledger.payload_bytes
                );
            }
            Ok(())
        }
        NacScanOutcome::Failed {
            error_kind,
            error_text,
        } => {
            record_scan_failure(metrics);
            poll_state.record_scan_failure_outcome(&cp_key, source_generation, error_kind);
            if !replacement_replay && committed_state.last_error == error_kind {
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
                "error_text": error_text.clone(),
                "raw_fragment": "",
            }));
            let mut failed_state = committed_state;
            failed_state.last_error = error_kind.to_string();
            let error_checkpoint = Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation,
                last_offset: checkpoint.last_offset,
                last_line_no: checkpoint.last_line_no,
                status: if replacement_replay {
                    "error".to_string()
                } else {
                    "active".to_string()
                },
                cursor_json: failed_state.serialize()?,
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
                .context("sink channel closed while sending nac_sqlite error batch")?;
            if replacement_replay {
                super::block_database_replay(&sink_tx, &error_checkpoint, error_text).await?;
            }
            Ok(())
        }
    }
}

/// The caller owns `ledger` so that every early return — including each
/// failure arm — still reports the bytes this scan had already paid for.
#[allow(clippy::too_many_arguments)]
fn scan_nac_database(
    db_path: &str,
    source_name: &str,
    source_generation: u32,
    expected_inode: u64,
    pre_scan_stat: StatFingerprint,
    prior: &NacState,
    budget: &ScanBudget,
    sweep: Option<&SweepPlan>,
    checkpoint_ceiling_bytes: usize,
    ledger: &mut ScanLedger,
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
    let schema = match inspect_schema(&connection, ledger) {
        Ok(schema) => schema,
        Err(error_text) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCHEMA,
                error_text,
            }
        }
    };
    let data_version_before = match sqlite_data_version(&connection) {
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
        budget,
        sweep,
        ledger,
    );
    let scan = match result {
        Ok(value) => value,
        Err(NacScanError::Scan(exc)) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("{exc:#}"),
            }
        }
    };
    let NacRowsScan {
        records,
        mut state,
        relevant_rows,
        row_errors,
        swept,
    } = scan;
    let data_version_after = match sqlite_data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return NacScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read NAC post-scan data_version: {exc:#}"),
            }
        }
    };
    if super::snapshot_is_mixed(
        db_path,
        data_version_before,
        data_version_after,
        opened_stat,
    ) {
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
    // A slice that started a fresh cycle stamps the content state that cycle
    // is walking; writes that land during the cycle move the stat past the
    // baseline, so the wrap leaves a follow-up cycle owed — self-correcting.
    // The cycle-start conjunct is load-bearing: a mid-cycle re-stamp would
    // advance the baseline past a silent write behind the sweep cursor and
    // erase that owed cycle
    // (`a_silent_write_behind_the_sweep_cursor_owes_a_follow_up_cycle`).
    if swept && prior.sweep.cursor.is_empty() {
        state.sweep_baseline = pre_scan_stat;
    }
    // The checkpoint-state ceiling degrades by evicting the oldest session
    // cursors (issue #601 §2.3); it never fails the scan. Evicted sessions are
    // re-emitted by a later poll — safe under §6's content-addressed identity.
    // An eviction is a coverage debt by construction (the evicted session is
    // censused but no longer carried), so the resume marker must reflect it.
    let evicted = state.evict_to_fit(checkpoint_ceiling_bytes);
    ledger.mark_evicted(evicted);
    if evicted > 0 {
        state.pending_coverage = true;
    }
    NacScanOutcome::Scanned {
        records,
        state: Box::new(state),
        schema_fingerprint: schema.fingerprint,
        relevant_rows,
        row_errors,
        swept,
    }
}

#[derive(Debug)]
struct NacSchema {
    session_columns: BTreeSet<String>,
    fingerprint: u64,
}

fn inspect_schema(
    connection: &Connection,
    ledger: &mut ScanLedger,
) -> std::result::Result<NacSchema, String> {
    let mut material = String::new();
    let mut session_columns = BTreeSet::new();
    for (table, required) in [
        ("sessions", REQUIRED_SESSION_COLUMNS),
        ("episodes", REQUIRED_EPISODE_COLUMNS),
    ] {
        let sql =
            super::schema_sql_for_table(connection, table, ledger).map_err(|exc| match exc {
                rusqlite::Error::QueryReturnedNoRows => {
                    format!("required table {table} is missing")
                }
                other => other.to_string(),
            })?;
        material.push_str(&sql.unwrap_or_default());
        material.push('\n');
        let columns: BTreeSet<String> = super::table_column_names(connection, table, ledger)
            .map_err(|exc| format!("{exc:#}"))?
            .into_iter()
            .collect();
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

/// One skipped row, destined for a single `ingest_errors` row.
#[derive(Debug, Clone)]
struct NacRowError {
    source_line_no: u64,
    error_kind: &'static str,
    error_text: String,
}

/// One record's single normalization outcome (§3.2's double-normalize
/// cleanup), shared by the oversized pre-check and the emit loop. Boxed so the
/// vector's spine stays lean.
enum NacPreparedRecord {
    Excluded,
    Normalized(Box<crate::model::NormalizedRecord>),
    Error(String),
}

/// The §3.2 episode-rewind preflight: `max(episodes.id)` behind the committed
/// watermark means episode rows vanished — which splits two ways on the id
/// ceiling below. `episodes.id` is `AUTOINCREMENT`, so while `sqlite_sequence`
/// still covers the watermark, ids never recycle: nothing can ever be written
/// at the vanished coordinates, the disappearance is a **deletion**, and
/// deletion is archival — the scan emits nothing and the watermark stands
/// (the e2e's "nac deleted remote metadata remains archived"). Only a ceiling
/// *behind* the watermark — a recreated table, a reset sequence, a schema
/// without `AUTOINCREMENT` — makes those coordinates re-allocatable, and that
/// is the true rewind that must route through #602's generation replacement.
/// Fixed-width scalar aggregates: charged on neither ledger axis (see the
/// `ScanLedger` rules), which is why no ledger threads through here.
fn nac_episodes_rewound(db_path: &str, episode_high_water: i64) -> Result<bool> {
    let connection = open_read_only(db_path)?;
    let max_episode_id: i64 =
        connection.query_row("SELECT coalesce(max(id), 0) FROM episodes", [], |row| {
            row.get(0)
        })?;
    if max_episode_id >= episode_high_water {
        return Ok(false);
    }
    Ok(nac_episode_id_ceiling(&connection)? < episode_high_water)
}

/// The largest `episodes.id` SQLite has ever allocated: `sqlite_sequence.seq`
/// for the `AUTOINCREMENT` table. Returns 0 when the sequence row — or the
/// whole `sqlite_sequence` table — is absent: absent means ids are reusable,
/// so no vanished tail is ever provably a deletion and the rewind arm stays
/// fail-closed (`the_episode_id_ceiling_is_zero_without_autoincrement`).
fn nac_episode_id_ceiling(connection: &Connection) -> rusqlite::Result<i64> {
    let sequence_table_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence')",
        [],
        |row| row.get(0),
    )?;
    if !sequence_table_exists {
        return Ok(0);
    }
    connection.query_row(
        "SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name = 'episodes'), 0)",
        [],
        |row| row.get(0),
    )
}

#[derive(Debug)]
enum NacScanError {
    Scan(anyhow::Error),
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

/// What one `scan_nac_rows` call produced.
struct NacRowsScan {
    records: Vec<SyntheticRecord>,
    state: NacState,
    relevant_rows: u64,
    row_errors: Vec<NacRowError>,
    swept: bool,
}

#[allow(clippy::too_many_arguments)]
fn scan_nac_rows(
    connection: &Connection,
    db_path: &str,
    source_name: &str,
    source_generation: u32,
    schema: &NacSchema,
    prior: &NacState,
    budget: &ScanBudget,
    sweep: Option<&SweepPlan>,
    ledger: &mut ScanLedger,
) -> std::result::Result<NacRowsScan, NacScanError> {
    let canonical_db = std::fs::canonicalize(db_path)
        .unwrap_or_else(|_| std::path::PathBuf::from(db_path))
        .to_string_lossy()
        .to_string();
    let namespace = namespace_prefix(source_name, &canonical_db, source_generation);
    let projection = session_projection(&schema.session_columns);
    let mut records = Vec::new();
    let mut row_errors = Vec::new();
    let mut next_state = NacState {
        episode_high_water: prior.episode_high_water,
        worker_threads: prior.worker_threads.clone(),
        project_exclusions_hash: prior.project_exclusions_hash,
        updated_at_high_water: prior.updated_at_high_water.clone(),
        updated_at_high_water_session: prior.updated_at_high_water_session.clone(),
        // Fast-path activity never resets the sweep cursor (§2.2); a slice
        // below overwrites this from the driver's report when one runs.
        sweep: prior.sweep.clone(),
        sweep_baseline: prior.sweep_baseline,
        ..NacState::default()
    };
    let mut contexts = BTreeMap::<String, NacSessionRow>::new();

    // Census (issue #601 §3.2): the narrow, `idx_sessions_updated_at`-cheap
    // read that identifies every session without materializing payloads. It
    // is the exact deletion detector and the newest-first ordering source;
    // nothing is ever *skipped* on its say-so alone.
    let census = {
        let mut census = Vec::<(String, String)>::new();
        let mut last_session_id = String::new();
        loop {
            let mut stmt = connection.prepare_cached(
                "SELECT session_id, updated_at FROM sessions \
                 WHERE session_id > ?1 ORDER BY session_id LIMIT ?2",
            )?;
            let mut rows = stmt.query(params![&last_session_id, SCAN_PAGE_SIZE as i64])?;
            let mut page_rows = 0usize;
            while let Some(row) = rows.next()? {
                let session_id: String = row.get(0)?;
                let updated_at: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
                ledger.charge_census_row(session_id.len() + updated_at.len());
                page_rows += 1;
                last_session_id = session_id.clone();
                census.push((session_id, updated_at));
            }
            if page_rows < SCAN_PAGE_SIZE {
                break;
            }
        }
        census
    };
    let census_ids: BTreeSet<String> = census.iter().map(|(id, _)| id.clone()).collect();
    let relevant_sessions = census.len() as u64;

    // The §3.2 keyset fast path. Each census row is classified against the
    // `(updated_at, session_id)` watermark:
    //
    // - **never read** (absent from the prior cursor set) → candidate, first
    //   class — cold debt, newest-first within the class (D6). This disjunct
    //   is what admits a new session whose `updated_at` sits at or below the
    //   watermark (clock skew, an imported store) — nothing else does
    //   (`a_new_session_backdated_below_the_watermark_is_read_first_class`);
    // - **incomparable** (format guard: malformed, or a different separator
    //   from the watermark) → candidate — the fast path never *skips* on a
    //   value it cannot order (fail open);
    // - **after the watermark** in strict keyset order → candidate — the
    //   changed tail, read in ascending `(updated_at, session_id)` order
    //   because the watermark is a durable resume position (D8a's rule);
    // - otherwise → carried: the prior cursor rides forward unread. The skip
    //   is justified by the sweep's coverage guarantee (§0), never by
    //   `updated_at` itself.
    let watermark_active = !prior.updated_at_high_water.is_empty();
    let mut candidates = Vec::new();
    for (session_id, updated_at) in &census {
        let known = prior.sessions.contains_key(session_id);
        let candidate = !known
            || !watermark_active
            || !nac_updated_at_comparable(updated_at, &prior.updated_at_high_water)
            || nac_keyset_after(
                updated_at,
                session_id,
                &prior.updated_at_high_water,
                &prior.updated_at_high_water_session,
            );
        if candidate {
            candidates.push((session_id.clone(), updated_at.clone(), known));
        } else if let Some(cursor) = prior.sessions.get(session_id) {
            next_state
                .sessions
                .insert(session_id.clone(), cursor.clone());
        }
    }
    // Never-read sessions first, newest-first within that class (§2.3, D6:
    // never-read-first is what makes a budget-degraded cold ingest converge,
    // and a genuinely new session has no prior cursor so it is in this class
    // already). The known tail follows in ascending keyset order so the
    // watermark below resumes exactly where a bound poll stopped.
    candidates.sort_by(|a, b| {
        a.2.cmp(&b.2).then_with(|| {
            if a.2 {
                a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0))
            } else {
                b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0))
            }
        })
    });
    let mut point_read = connection.prepare_cached(&format!(
        "SELECT {projection} FROM sessions WHERE session_id = ?1"
    ))?;
    let mut fast_path_bound = false;
    for idx in 0..candidates.len() {
        let over_budget = budget.is_exhausted_by(ledger.payload_rows, ledger.payload_bytes);
        let over_records = records.len() >= MAX_NAC_SYNTHETIC_RECORDS;
        if over_budget || over_records {
            // Commit what was read (§2.1/§2.3): every skipped session keeps
            // its prior cursor — "unread this poll", never "deleted" — and a
            // skipped new session stays absent so a later poll detects it.
            // The watermark did not advance past any skipped tail candidate
            // (the tail is read in ascending keyset order), so the skip is
            // held open by `pending_coverage` below.
            let remaining = &candidates[idx..];
            ledger.mark_degraded(remaining.len() as u64, 0);
            fast_path_bound = true;
            for (session_id, _, _) in remaining {
                if let Some(cursor) = prior.sessions.get(session_id) {
                    next_state
                        .sessions
                        .insert(session_id.clone(), cursor.clone());
                }
            }
            break;
        }
        let session_id = &candidates[idx].0;
        let session = {
            let mut rows = point_read.query(params![session_id])?;
            let Some(row) = rows.next()? else {
                // Vanished between census and read: a mid-scan commit the
                // data_version bracket rejects.
                continue;
            };
            match read_session_row(row, &schema.session_columns, ledger)? {
                NacSessionRead::Row(session) => *session,
                NacSessionRead::Oversized {
                    estimated_bytes, ..
                } => {
                    // An un-processable single row (§2.3): report it once,
                    // mark it in the cursor so the report does not repeat
                    // every poll, and keep scanning. A shrunk row hashes
                    // differently from the marker and re-emits normally.
                    let marker = NacSessionCursor {
                        metadata_hash: format!("{OVERSIZED_SESSION_MARKER}:{estimated_bytes}"),
                        created_at: String::new(),
                        part_hashes: BTreeMap::new(),
                    };
                    if prior.sessions.get(session_id) != Some(&marker) {
                        row_errors.push(NacRowError {
                            source_line_no: 0,
                            error_kind: ERROR_KIND_ROW_TOO_LARGE,
                            error_text: format!(
                                "NAC session {session_id} is {estimated_bytes} bytes, exceeding \
                                 the {SCAN_PAGE_MAX_BYTES} byte row ceiling; row skipped"
                            ),
                        });
                    }
                    next_state.sessions.insert(session_id.clone(), marker);
                    // The row was fully processed for this poll's purposes, so
                    // the tail watermark advances past it like any other item.
                    if candidates[idx].2 {
                        advance_updated_at_watermark(
                            &mut next_state,
                            &candidates[idx].1,
                            &candidates[idx].0,
                        );
                    }
                    continue;
                }
            }
        };
        let normalized_session_id = format!("{namespace}:{}", session.raw_session_id);
        let (mut session_records, cursor, parts_truncated) = synthesize_session(
            &session,
            &normalized_session_id,
            prior.sessions.get(&session.raw_session_id),
            MAX_NAC_SYNTHETIC_RECORDS.saturating_sub(records.len()),
        )?;
        if parts_truncated {
            ledger.mark_degraded(0, 0);
        }
        records.append(&mut session_records);
        next_state
            .sessions
            .insert(session.raw_session_id.clone(), cursor);
        contexts.insert(session.raw_session_id.clone(), session);
        // Per-item resume advance, tail (known) candidates only: the tail is
        // read in ascending keyset order, so committing the watermark at the
        // last fully processed position is what lets a budget-bound poll
        // resume instead of re-reading the same prefix forever. Never from
        // the never-read class — it is processed out of keyset order, and an
        // advance from there could leap past unread tail candidates.
        if candidates[idx].2 {
            advance_updated_at_watermark(&mut next_state, &candidates[idx].1, &candidates[idx].0);
        }
    }
    drop(point_read);

    // Prune worker threads against the census keyset — the complete session
    // id set — never against the per-poll `contexts` map, which a budget-
    // bounded read leaves partial (issue #601 §3.2: the prune bug that
    // resurrected pruned worker rows becomes a deletion bug under budgets).
    next_state.worker_threads.retain(|key| {
        key.split_once('\n')
            .map(|(session_id, _)| census_ids.contains(session_id))
            .unwrap_or(false)
    });

    let max_episode_id: i64 =
        connection.query_row("SELECT coalesce(max(id), 0) FROM episodes", [], |row| {
            row.get(0)
        })?;
    if max_episode_id < prior.episode_high_water {
        // §3.2: a vanished episode tail splits on the AUTOINCREMENT id
        // ceiling, exactly as at the preflight (`nac_episodes_rewound`).
        // While `sqlite_sequence` still covers the watermark, ids never
        // recycle: the vanished coordinates can never be rewritten, so this
        // is a **deletion** — archival, not a rewind. The watermark stands
        // (new episodes are allocated above the ceiling, so `id > watermark`
        // still admits every future insert), the loop below reads nothing
        // behind it, and the archived rows stay untouched — deletion emits
        // nothing (`a_session_deleted_from_the_store_emits_nothing_new`,
        // e2e "nac deleted remote metadata remains archived"). It skips no
        // live work: an insert after the deletion sits above the watermark
        // and is read normally
        // (`a_new_session_beside_a_deletion_still_emits`).
        //
        // A ceiling behind the watermark means those ids are re-allocatable
        // — a true rewind (truncation of history) that must route through
        // #602's generation replacement (G8c), which the preflight in
        // `process_nac_sqlite_db` detects and starts. Reaching this arm with
        // a rewound ceiling means the rewind landed *between* that preflight
        // and this scan; failing the scan leaves the committed stat stale,
        // so the next poll's preflight sees it and replaces properly. The
        // old in-place `worker_threads.clear()` reset re-emitted worker
        // history into the live generation with no barrier — exactly what
        // #602 forbids — and never reset `sessions` at all.
        if nac_episode_id_ceiling(connection)? < prior.episode_high_water {
            return Err(NacScanError::Scan(anyhow::anyhow!(
                "NAC episode history rewound mid-poll (max id {max_episode_id} and id ceiling \
                 behind watermark {}); failing this scan so the next poll routes the rewind \
                 through a generation replacement",
                prior.episode_high_water
            )));
        }
    }
    let scan_from = prior.episode_high_water;
    let mut last_episode_id = scan_from;
    let mut episodes_processed = 0u64;
    let episode_bytes = "length(CAST(COALESCE(thread_name, '') AS BLOB)) + \
        length(CAST(COALESCE(session_id, '') AS BLOB)) + \
        length(CAST(COALESCE(action, '') AS BLOB)) + \
        length(CAST(COALESCE(content, '') AS BLOB)) + \
        length(CAST(COALESCE(created_at, '') AS BLOB))";
    // Plain columns plus one `estimated_bytes` expression — the same
    // projection cleanup as `session_projection`: the loop below reads the
    // size before taking any column, so an oversized row's payload is never
    // handed to Rust.
    let episode_sql = format!(
        "SELECT id, thread_name, session_id, action, content, created_at, \
         ({episode_bytes}) AS estimated_bytes \
         FROM episodes WHERE id > ?1 ORDER BY id LIMIT ?2"
    );
    'episodes: loop {
        let mut stmt = connection.prepare_cached(&episode_sql)?;
        let mut rows = stmt.query(params![last_episode_id, SCAN_PAGE_SIZE as i64])?;
        let mut page_rows = 0usize;
        while let Some(row) = rows.next()? {
            // The episode watermark is an exact, durable resume position, so
            // bounded progress here is oldest-first *of the new tail* with
            // `episode_high_water` as the committed cursor — not newest-first,
            // which would tear a hole in the watermark (§2.3's newest-first
            // rule exists for orderings without a resume position). At least
            // one episode is processed per poll, so a backlog can never stall.
            let over_budget = budget.is_exhausted_by(ledger.payload_rows, ledger.payload_bytes);
            let over_records = records.len() >= MAX_NAC_SYNTHETIC_RECORDS;
            if episodes_processed > 0 && (over_budget || over_records) {
                // The row `rows.next()` just handed over was materialized —
                // the projection includes `content`, so SQLite decoded it
                // before the budget was consulted — and the ledger's headline
                // rule charges at the point bytes leave SQLite (the oversize
                // arm below makes the same call). One row per bound poll, on
                // the rows axis only: its columns are never taken, so no
                // bytes are charged. It still counts inside `remaining` — the
                // watermark did not advance past it, so it is coverage debt
                // and the next poll materializes it again. Cost and debt are
                // different axes; one row on both is the honest ledger.
                ledger.charge_payload_row();
                let remaining =
                    u64::try_from(max_episode_id.saturating_sub(last_episode_id)).unwrap_or(0);
                ledger.mark_degraded(remaining, 0);
                break 'episodes;
            }
            // This projection includes `content`, so the whole episode row is
            // charged on the payload axis where SQLite hands it over. The row
            // charge lands **before** the oversize check: SQLite had already
            // decoded every column to evaluate `estimated_bytes`, so a skip
            // arm reporting zero rows would contradict the ledger's headline
            // guarantee. Bytes are still not charged from `estimated_bytes` —
            // the size is read before any payload column is taken, so an
            // oversized row hands Rust nothing beyond its id, and the ledger
            // reports what was materialized.
            let id: i64 = row.get(0)?;
            ledger.charge_payload_row();
            page_rows += 1;
            let row_bytes = checked_row_bytes(row.get(6)?, "NAC episode", id)?;
            if row_bytes > SCAN_PAGE_MAX_BYTES {
                // Un-processable single row (§2.3): one error row, skip it,
                // advance the watermark past it. The watermark makes the
                // report one-shot — this episode is never read again.
                row_errors.push(NacRowError {
                    source_line_no: id as u64,
                    error_kind: ERROR_KIND_ROW_TOO_LARGE,
                    error_text: format!(
                        "NAC episode {id} is {row_bytes} bytes, exceeding the \
                         {SCAN_PAGE_MAX_BYTES} byte row ceiling; row skipped"
                    ),
                });
                last_episode_id = id;
                next_state.episode_high_water = id;
                episodes_processed += 1;
                continue;
            }
            // Every one of these is NOT NULL in the live `episodes` schema and
            // load-bearing downstream (thread/session identity, the action tag
            // the record type is derived from, the payload, the timestamp), so
            // a NULL fails the scan rather than materializing as `""`.
            let thread_name = take_payload_required_string(ledger, row, 1)?;
            let raw_session_id = take_payload_required_string(ledger, row, 2)?;
            let action = take_payload_required_string(ledger, row, 3)?;
            let content = take_payload_required_string(ledger, row, 4)?;
            let created_at_raw = take_payload_required_string(ledger, row, 5)?;
            // Resolve the parent on demand (issue #601 §3.2): a budget-
            // bounded session read makes "not in `contexts`" ordinary, and a
            // point lookup is ~free (§1.3). Only a session absent from the
            // *database* is an orphan — reported once and advanced past,
            // because the live schema's FK points at `threads`, not
            // `sessions`, so this is reachable in production and must never
            // latch the adapter.
            if !contexts.contains_key(&raw_session_id) {
                let mut lookup = connection.prepare_cached(&format!(
                    "SELECT {projection} FROM sessions WHERE session_id = ?1"
                ))?;
                let mut lookup_rows = lookup.query(params![&raw_session_id])?;
                if let Some(session_row) = lookup_rows.next()? {
                    match read_session_row(session_row, &schema.session_columns, ledger)? {
                        NacSessionRead::Row(session) => {
                            contexts.insert(raw_session_id.clone(), *session);
                        }
                        NacSessionRead::Oversized { .. } => {}
                    }
                }
            }
            let Some(parent) = contexts.get(&raw_session_id) else {
                row_errors.push(NacRowError {
                    source_line_no: id as u64,
                    error_kind: ERROR_KIND_ORPHAN_EPISODE,
                    error_text: format!(
                        "episode {id} references missing session {raw_session_id}; \
                         episode skipped"
                    ),
                });
                last_episode_id = id;
                next_state.episode_high_water = id;
                episodes_processed += 1;
                continue;
            };
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
            episodes_processed += 1;
        }
        if page_rows < SCAN_PAGE_SIZE {
            break;
        }
    }

    // Optional sweep slice (§2.2). Conditions 1 (reconcile trigger), 2
    // (interval clock) and 4 (not replaying) were decided by the caller —
    // `sweep` exists only when they held. Condition 3 is decided here, where
    // the fast path's cost is known, with D7's waiver: a degraded poll is
    // sweep-eligible despite its budget consumption, an emitting poll defers
    // its slice to the next quiet reconcile tick. The slice runs inside the
    // data_version bracket, so a mixed snapshot discards its advance with the
    // rest of the scan. The slice is the mechanism behind G5c: it re-reads
    // sessions in `session_id` order and re-emits any whose content disagrees
    // with the carried cursor — the mutation `updated_at` never announced.
    let mut swept = false;
    if let Some(sweep_plan) = sweep {
        let eligible = records.is_empty()
            && (ledger.coverage_degraded
                || !budget.is_half_consumed_by(ledger.payload_rows, ledger.payload_bytes));
        if eligible {
            let mut slice = ScanLedger::default();
            let driven = drive_sweep_slice(
                &prior.sweep,
                &sweep_plan.budget,
                sweep_plan.max_millis,
                sweep_plan.now_unix_ms,
                &mut slice,
                |after, slice| {
                    next_nac_sweep_item(
                        connection,
                        after,
                        &namespace,
                        &projection,
                        &schema.session_columns,
                        &mut next_state.sessions,
                        &mut records,
                        &mut row_errors,
                        slice,
                    )
                },
            );
            // The slice's reads are paid whether or not it completed; fold
            // them before inspecting the outcome so a failure arm still
            // reports them.
            ledger.absorb_sweep_slice(&slice);
            let report = driven?;
            next_state.sweep = report.state;
            swept = true;
        }
    }

    // Full fast-path coverage advances the watermark to the census maximum —
    // over well-formed values sharing the maximum's separator only, so a
    // mixed-format store keeps its minority-format rows as perpetual
    // candidates rather than silently ordering across formats. A bound poll
    // never jumps: its watermark stays at the last processed tail position.
    if !fast_path_bound {
        // The lexicographic maximum among well-formed values is by definition
        // the maximum of its own comparability class (every same-separator
        // value sorts at or below it), so it is the highest position the
        // watermark can honestly claim.
        let jump = census
            .iter()
            .filter(|(_, updated_at)| nac_timestamp_shape(updated_at).is_some())
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        if let Some((session_id, updated_at)) = jump {
            next_state.updated_at_high_water = updated_at.clone();
            next_state.updated_at_high_water_session = session_id.clone();
        }
    }

    // §2.3's persisted resume marker: a censused session absent from the
    // carried cursor set has never been read in this generation, a fast-path
    // candidate was budget-skipped (`fast_path_bound` — the tail watermark
    // stopped short), and an episode watermark short of `max(episodes.id)` is
    // an unread episode tail. Any of the three keeps the cheap stat
    // short-circuit open (see the conjunct in `process_nac_sqlite_db`) until
    // a scan covers everything.
    next_state.pending_coverage = next_state.episode_high_water < max_episode_id
        || fast_path_bound
        || census_ids
            .iter()
            .any(|id| !next_state.sessions.contains_key(id));

    ledger.rows_emitted = records.len() as u64;
    let relevant_rows = relevant_sessions.saturating_add(episodes_processed);
    Ok(NacRowsScan {
        records,
        state: next_state,
        relevant_rows,
        row_errors,
        swept,
    })
}

/// Advance the fast-path watermark to `(updated_at, session_id)` if that is a
/// well-formed position strictly after (or seeding) the current one. Called
/// only for fully processed tail candidates, in their ascending keyset order.
fn advance_updated_at_watermark(state: &mut NacState, updated_at: &str, session_id: &str) {
    if nac_timestamp_shape(updated_at).is_none() {
        return;
    }
    if state.updated_at_high_water.is_empty() {
        state.updated_at_high_water = updated_at.to_string();
        state.updated_at_high_water_session = session_id.to_string();
        return;
    }
    if nac_updated_at_comparable(updated_at, &state.updated_at_high_water)
        && nac_keyset_after(
            updated_at,
            session_id,
            &state.updated_at_high_water,
            &state.updated_at_high_water_session,
        )
    {
        state.updated_at_high_water = updated_at.to_string();
        state.updated_at_high_water_session = session_id.to_string();
    }
}

/// The NAC sweep reader (§2.2): the first session strictly after `after` in
/// `session_id` order, read in full, re-synthesized against the carried
/// cursor, and re-emitted where it disagrees — which is exactly how a
/// mutation that never bumped `updated_at` is detected (G5c). Oversized rows
/// take the same §2.3 marker path as the fast path.
#[allow(clippy::too_many_arguments)]
fn next_nac_sweep_item(
    connection: &Connection,
    after: &str,
    namespace: &str,
    projection: &str,
    session_columns: &BTreeSet<String>,
    sessions: &mut BTreeMap<String, NacSessionCursor>,
    records: &mut Vec<SyntheticRecord>,
    row_errors: &mut Vec<NacRowError>,
    slice: &mut ScanLedger,
) -> std::result::Result<Option<SweepItem>, NacScanError> {
    let mut stmt = connection.prepare_cached(&format!(
        "SELECT {projection} FROM sessions WHERE session_id > ?1 ORDER BY session_id LIMIT 1"
    ))?;
    let mut rows = stmt.query(params![after])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let bytes_before = slice.payload_bytes;
    match read_session_row(row, session_columns, slice)? {
        NacSessionRead::Row(session) => {
            let position = session.raw_session_id.clone();
            let normalized_session_id = format!("{namespace}:{position}");
            let (mut session_records, cursor, parts_truncated) = synthesize_session(
                &session,
                &normalized_session_id,
                sessions.get(&position),
                MAX_NAC_SYNTHETIC_RECORDS.saturating_sub(records.len()),
            )?;
            if parts_truncated {
                slice.mark_degraded(0, 0);
            }
            records.append(&mut session_records);
            sessions.insert(position.clone(), cursor);
            Ok(Some(SweepItem {
                position,
                payload_bytes: slice.payload_bytes.saturating_sub(bytes_before),
            }))
        }
        NacSessionRead::Oversized {
            session_id,
            estimated_bytes,
        } => {
            let marker = NacSessionCursor {
                metadata_hash: format!("{OVERSIZED_SESSION_MARKER}:{estimated_bytes}"),
                created_at: String::new(),
                part_hashes: BTreeMap::new(),
            };
            if sessions.get(&session_id) != Some(&marker) {
                row_errors.push(NacRowError {
                    source_line_no: 0,
                    error_kind: ERROR_KIND_ROW_TOO_LARGE,
                    error_text: format!(
                        "NAC session {session_id} is {estimated_bytes} bytes, exceeding \
                         the {SCAN_PAGE_MAX_BYTES} byte row ceiling; row skipped"
                    ),
                });
            }
            sessions.insert(session_id.clone(), marker);
            Ok(Some(SweepItem {
                position: session_id,
                payload_bytes: slice.payload_bytes.saturating_sub(bytes_before),
            }))
        }
    }
}

/// The session projection: plain columns plus **one** `estimated_bytes`
/// expression. The previous shape wrapped every column in a
/// `CASE WHEN (<14-term length expression>) <= … THEN col ELSE NULL END`
/// guard, inlining the same expression 16 times per row — measured 1.65×
/// slower than a plain select, scaling with blob size (§3.2's projection
/// cleanup). The oversize guard did not move, it just changed language:
/// `read_session_row` reads `estimated_bytes` and `session_id` *first* and
/// returns without touching any payload column when the row is oversized, so
/// SQLite never hands an oversized payload to Rust — `sqlite3_column` decodes
/// lazily, per column requested.
fn session_projection(columns: &BTreeSet<String>) -> String {
    let mut byte_columns = vec![
        "session_id",
        "cwd",
        "model",
        "base_url",
        "messages_json",
        "created_at",
        "updated_at",
    ];
    for optional_name in [
        "backend",
        "reasoning_effort",
        "sandbox_json",
        "last_response_duration_ms",
        "previous_response_duration_ms",
        "response_durations_json",
        "token_usages_json",
        "host_id",
    ] {
        if columns.contains(optional_name) {
            byte_columns.push(optional_name);
        }
    }
    let estimated_bytes = byte_columns
        .into_iter()
        .map(|name| format!("length(CAST(COALESCE({name}, '') AS BLOB))"))
        .collect::<Vec<_>>()
        .join(" + ");
    let optional = |name: &str, fallback: &str| {
        if columns.contains(name) {
            name.to_string()
        } else {
            format!("{fallback} AS {name}")
        }
    };
    let remote = if columns.contains("host_id") {
        "CASE WHEN length(CAST(COALESCE(host_id, '') AS BLOB)) > 0 THEN 1 ELSE 0 END AS is_remote"
            .to_string()
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
        format!("({estimated_bytes}) AS estimated_bytes"),
    ]
    .join(", ")
}

/// Reads one session row. The projection includes `messages_json`, so every
/// variable-length column it materializes is charged on the payload axis —
/// deliberately *not* from the projection's own `estimated_bytes` expression,
/// which SQLite computes internally and which therefore reports bytes that
/// were never handed to Rust for an oversized row.
///
/// `estimated_bytes` and `session_id` are read **first**: an oversized row
/// returns before any payload column is requested, so SQLite never decodes
/// its large columns for Rust — the oversize guard the old per-column `CASE`
/// projection implemented in SQL, now implemented at the read site. The id is
/// taken (and charged) even for oversized rows because both the fast path's
/// cursor marker and the sweep's resume position need it.
fn read_session_row(
    row: &rusqlite::Row<'_>,
    columns: &BTreeSet<String>,
    ledger: &mut ScanLedger,
) -> std::result::Result<NacSessionRead, NacScanError> {
    ledger.charge_payload_row();
    let estimated_bytes = checked_row_bytes(row.get(15)?, "NAC session row", 0)?;
    // NOT NULL in the live `sessions` schema; a NULL is schema drift and must
    // fail the scan. `session_id` and `created_at`/`updated_at` below feed
    // logical-ID and timestamp derivation directly (§6), so absorbing a NULL
    // here would mint records keyed on the empty string.
    let raw_session_id = take_payload_required_string(ledger, row, 0)?;
    if estimated_bytes > SCAN_PAGE_MAX_BYTES {
        return Ok(NacSessionRead::Oversized {
            session_id: raw_session_id,
            estimated_bytes,
        });
    }
    let cwd = take_payload_required_string(ledger, row, 1)?;
    let model = take_payload_required_string(ledger, row, 2)?;
    let base_url = sanitize_base_url(&take_payload_required_string(ledger, row, 3)?);
    // `backend` and `reasoning_effort` are the two columns that genuinely
    // tolerate NULL: they are absent on older stores and the empty string is
    // the documented default (this is what the pre-ledger code did too).
    let backend = take_payload_nullable_string(ledger, row, 4)?;
    let reasoning_effort = take_payload_nullable_string(ledger, row, 5)?;
    let sandbox_raw = take_payload_text(ledger, row, 6)?;
    let messages_raw = take_payload_required_string(ledger, row, 7)?;
    let last_response_duration_ms = row.get::<_, Option<i64>>(8)?.map(|v| v.max(0) as u64);
    let previous_response_duration_ms = row.get::<_, Option<i64>>(9)?.map(|v| v.max(0) as u64);
    let durations_raw = take_payload_text(ledger, row, 10)?;
    let usages_raw = take_payload_text(ledger, row, 11)?;
    let created_at = take_payload_required_string(ledger, row, 12)?;
    let updated_at = take_payload_required_string(ledger, row, 13)?;
    let remote: Option<i64> = row.get(14)?;
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
    Ok(NacSessionRead::Row(Box::new(NacSessionRow {
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
    })))
}

/// What one session point read produced: a full row, or the §2.3
/// un-processable-row marker for a row past the byte ceiling. The row is
/// boxed so the marker variant is not forced to carry a `NacSessionRow`'s
/// footprint.
enum NacSessionRead {
    Row(Box<NacSessionRow>),
    Oversized {
        session_id: String,
        estimated_bytes: usize,
    },
}

fn checked_row_bytes(
    raw: i64,
    label: &str,
    row_id: i64,
) -> std::result::Result<usize, NacScanError> {
    usize::try_from(raw).map_err(|_| {
        NacScanError::Scan(anyhow::anyhow!(
            "{label} {row_id} returned an invalid negative byte length"
        ))
    })
}

fn sanitize_base_url(raw: &str) -> String {
    let Ok(parsed) = url::Url::parse(raw) else {
        return String::new();
    };
    match parsed.origin() {
        Origin::Tuple(_, _, _) => parsed.origin().ascii_serialization(),
        Origin::Opaque(_) => String::new(),
    }
}

fn synthesize_session(
    session: &NacSessionRow,
    normalized_session_id: &str,
    prior: Option<&NacSessionCursor>,
    record_budget: usize,
) -> std::result::Result<(Vec<SyntheticRecord>, NacSessionCursor, bool), NacScanError> {
    let created_at = normalize_nac_timestamp(&session.created_at, true)?;
    let updated_at = normalize_nac_timestamp(&session.updated_at, true)?;
    let metadata = session_metadata_value(session, normalized_session_id, &created_at, &updated_at);
    let metadata_hash = value_hash(&metadata);
    let mut records = Vec::new();
    let mut truncated = false;
    let created_changed = prior
        .map(|cursor| cursor.created_at != created_at)
        .unwrap_or(true);
    if prior.map(|cursor| cursor.metadata_hash.as_str()) != Some(metadata_hash.as_str()) {
        if records.len() >= record_budget {
            truncated = true;
        } else {
            records.push(SyntheticRecord {
                record: metadata,
                project_dir: project_dir_for(session),
                source_line_no: 0,
                source_offset: 0,
            });
        }
    }

    let (logical_parts, parts_truncated) = logical_message_parts(
        session,
        normalized_session_id,
        &created_at,
        record_budget.saturating_sub(records.len()),
    )?;
    truncated = truncated || parts_truncated;
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
        truncated,
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

/// Push one part unless the per-session budget is spent; returns `false` when
/// the budget bound. A bound is bounded progress, not a failure (issue #601
/// §2.3): the caller keeps the prefix it built, reports `coverage_degraded`,
/// and the un-built suffix stays undetected until the budget allows it —
/// which is degraded coverage, never an unbounded vector and never a latch.
fn push_logical_part(
    parts: &mut Vec<(String, Value, u64, u64)>,
    part: (String, Value, u64, u64),
    record_budget: usize,
) -> bool {
    if parts.len() >= record_budget {
        return false;
    }
    parts.push(part);
    true
}

/// The built parts of one session — `(logical_id, record, source_line_no,
/// source_offset)` each — plus whether the record budget truncated the list
/// (§2.3: truncation is reported, never silent).
type LogicalParts = (Vec<(String, Value, u64, u64)>, bool);

fn logical_message_parts(
    session: &NacSessionRow,
    normalized_session_id: &str,
    timestamp: &str,
    record_budget: usize,
) -> std::result::Result<LogicalParts, NacScanError> {
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
                if !push_logical_part(
                    &mut parts,
                    (logical_id, Value::Object(record), 0, offset),
                    record_budget,
                ) {
                    return Ok((parts, true));
                }
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
                if !push_logical_part(
                    &mut parts,
                    (logical_id, Value::Object(record), 1, offset),
                    record_budget,
                ) {
                    return Ok((parts, true));
                }
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
                    if !push_logical_part(
                        &mut parts,
                        (logical_id, Value::Object(record), source_line_no, offset),
                        record_budget,
                    ) {
                        return Ok((parts, true));
                    }
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
            if !push_logical_part(
                &mut parts,
                (logical_id, Value::Object(record), 3, offset),
                record_budget,
            ) {
                return Ok((parts, true));
            }
        }
    }
    Ok((parts, false))
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
    // Both separators normalize (§3.2's format guard, the reading half): the
    // guard's fail-open rule *reads* a row whose `updated_at` separator
    // differs from the watermark's instead of ordering against it, and a
    // normalizer that only accepted the space form would turn that fail-open
    // read into a latched scan failure — fail open would fail closed.
    let trimmed = raw.trim();
    let parsed = if nanos {
        NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f"))
    } else {
        NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f"))
            .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S"))
            .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkTrigger;
    use std::path::Path;

    async fn drive_nac_poll(
        config: &AppConfig,
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        poll_state: &VolatilePollMap,
    ) -> (Vec<RowBatch>, Vec<crate::CheckpointTransition>) {
        drive_nac_poll_with_metrics(
            config,
            work,
            checkpoints,
            poll_state,
            &Arc::new(Metrics::default()),
        )
        .await
    }

    /// Shares one `Metrics` across several polls so a test can count the scans
    /// that actually ran rather than the errors that were reported.
    async fn drive_nac_poll_with_metrics(
        config: &AppConfig,
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        poll_state: &VolatilePollMap,
        metrics: &Arc<Metrics>,
    ) -> (Vec<RowBatch>, Vec<crate::CheckpointTransition>) {
        let (sink_tx, mut sink_rx) = mpsc::channel(8);
        let process = process_nac_sqlite_db(
            config,
            work,
            checkpoints.clone(),
            poll_state,
            sink_tx,
            metrics,
        );
        tokio::pin!(process);
        let mut batches = Vec::new();
        let mut transitions = Vec::new();
        let mut committed = None;
        loop {
            tokio::select! {
                result = &mut process => {
                    result.expect("NAC poll should complete");
                    break;
                }
                message = sink_rx.recv() => match message.expect("NAC test sink remains open") {
                    SinkMessage::Batch(batch) => batches.push(batch),
                    SinkMessage::BeginReplay { transition, ack }
                    | SinkMessage::BlockReplay { transition, ack }
                    | SinkMessage::MirrorCaughtUp { transition, ack } => {
                        committed = Some(transition.checkpoint.clone());
                        let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                            checkpoint_revision: 1,
                            operation_id: transition.checkpoint.operation_id.clone(),
                        }));
                        transitions.push(transition);
                    }
                    SinkMessage::FinalizeReplay { transition, ack } => {
                        committed = Some(transition.checkpoint.clone());
                        let _ = ack.send(Ok(
                            crate::publication::FinalizeReplayOutcome::Published(
                                crate::publication::PublicationAck {
                                    checkpoint_revision: 2,
                                    publication_revision: 1,
                                    already_published: false,
                                },
                            ),
                        ));
                        transitions.push(transition);
                    }
                }
            }
        }
        while let Ok(message) = sink_rx.try_recv() {
            if let SinkMessage::Batch(batch) = message {
                batches.push(batch);
            }
        }
        if let Some(checkpoint) =
            committed.or_else(|| batches.last().and_then(|batch| batch.checkpoint.clone()))
        {
            checkpoints.write().await.insert(
                checkpoint_key(&checkpoint.source_name, &checkpoint.source_file),
                checkpoint,
            );
        }
        (batches, transitions)
    }

    /// Issue #601 §2.5. `record_blocked_replay` exists to stop
    /// `clear(); record_failed_scan();` from pinning a blocked replay at the
    /// 15 s floor forever, and it is guarded at its implementation — but NAC's
    /// **call site** was not. `blocked_replay_backs_off_instead_of_resending_the_barrier`
    /// reaches the `Failed` arm (an unreadable file), never
    /// `record_blocked_replay`, so inserting a `poll_state.clear()` before
    /// nac.rs's call left the suite green.
    ///
    /// This drives the other block path: the scan *succeeds*, and a record
    /// fails `normalize_record`, which is deterministic and content-driven and
    /// therefore recurs on every retry — exactly the shape a flat floor turns
    /// into a full re-read of the whole store every 15 s indefinitely.
    ///
    /// Fails for: `clear`ing volatile state before `record_blocked_replay`, or
    /// dropping the call entirely.
    #[tokio::test]
    async fn a_normalization_blocked_nac_replay_climbs_the_failure_ladder() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        // `normalize_record` rejects an unregistered harness outright, so every
        // synthesized record fails and a replacement replay blocks durably.
        let work = WorkItem {
            source_name: "nac-block-ladder".to_string(),
            harness: "not-a-registered-harness".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        let config = AppConfig::default();
        drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert!(checkpoints.read().await.contains_key(&cp_key));

        // Changing the exclusion set starts a replacement replay that cannot
        // finish, because nothing normalizes.
        let mut replaying = AppConfig::default();
        replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
        let block_poll = |config: AppConfig| {
            let work = work.clone();
            let checkpoints = checkpoints.clone();
            let poll_state = poll_state.clone();
            let metrics = metrics.clone();
            async move {
                let (_, transitions) = drive_nac_poll_with_metrics(
                    &config,
                    &work,
                    &checkpoints,
                    &poll_state,
                    &metrics,
                )
                .await;
                transitions
                    .iter()
                    .any(|transition| transition.checkpoint.status == "error")
            }
        };

        assert!(
            block_poll(replaying.clone()).await,
            "the replacement replay must block durably"
        );
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            1,
            "entering the blocked state starts the ladder"
        );

        // Inside the first 15 s window the retry is suppressed entirely.
        assert!(!block_poll(replaying.clone()).await, "throttled");

        // 16 s later the window has expired and the ladder must climb to 2 —
        // a 30 s window, not another 15 s one.
        poll_state.age_for_tests(&cp_key, std::time::Duration::from_secs(16));
        assert!(block_poll(replaying.clone()).await);
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            2,
            "a repeat block must extend the streak, not restart it — a \
             `clear()` before `record_blocked_replay` pins it at 1"
        );

        // This is the probe a reset-to-1 bug fails: pinned at the floor, the
        // retry would be due and would run.
        poll_state.age_for_tests(&cp_key, std::time::Duration::from_secs(16));
        assert!(
            !block_poll(replaying).await,
            "the second window is 30 s; a blocked replay must not re-read the \
             whole store every 15 s forever"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §2.5/§3.2. NAC's `Failed` arm must route through
    /// `record_scan_failure_outcome`, not straight into the fault ladder:
    /// mixed-snapshot means the store was being written while the scan read
    /// it, and a 15 s → 15 min suppression of an active NAC store is the §6
    /// prompt-visibility regression the exemption exists to prevent. The
    /// contention clock still has to move, because it is what throttles a
    /// contended replay's durable barrier.
    ///
    /// Fails for: calling `record_failed_scan` directly from the `Failed` arm,
    /// or dropping the classification entirely.
    #[tokio::test]
    async fn a_contended_nac_scan_moves_the_contention_clock_not_the_fault_ladder() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-contention".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let config = AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        super::super::contention_injection::arm(&source_file, 1);
        drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            metrics
                .sqlite_scan_failures_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the armed scan must actually reach the mixed-snapshot arm"
        );
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            0,
            "contention must not climb NAC's fault ladder"
        );
        assert_eq!(
            poll_state.consecutive_contended_scans(&cp_key),
            1,
            "…but it must leave the clock that throttles the replay barrier"
        );

        super::super::contention_injection::disarm(&source_file);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §3.2/§6 — NAC's half of the call-site guard. The classifier
    /// test above proves the clock *moves*; this proves the clock does not
    /// reach ordinary polls.
    ///
    /// §2.5's `|| !failure_retry_due` disjunct on the cheap short-circuit was
    /// outcome-redundant with `should_skip_poll` until §3.2 put the contention
    /// clock inside `failure_retry_due` and deliberately left it out of
    /// `should_skip_poll`. Adding it now would stop scanning an actively
    /// written NAC store for up to 60 s — the prompt-visibility regression the
    /// exemption exists to prevent. A NAC store is contended precisely while a
    /// worker is writing episodes into it.
    ///
    /// The end-to-end delivery half of this rule is
    /// `an_ordinary_poll_of_a_contended_database_is_not_throttled` on the
    /// Cursor adapter; this one pins the throttle at NAC's own call site.
    ///
    /// Fails for: adding `|| !poll_state.failure_retry_due(..)` to the cheap
    /// short-circuit (the second scan never runs), or moving the contention
    /// clock into `should_skip_poll`.
    #[tokio::test]
    async fn an_ordinary_poll_of_a_contended_nac_store_is_not_throttled() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-contended-ordinary".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let config = AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        // Cold poll commits a checkpoint over the fixture.
        let (cold, _) =
            drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert!(!cold.is_empty(), "the cold poll emits the fixture");

        // A worker is writing episodes, so the next two scans lose the bracket.
        super::super::contention_injection::arm(&source_file, 2);
        append_worker_episode(&path, "contended follow-up one", "2026-07-18 12:18:00");
        drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(poll_state.consecutive_contended_scans(&cp_key), 1);
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            0,
            "contention is not a fault (§3.2)"
        );

        // The very next tick, far inside the contention window the barrier is
        // now serving, must still read the store.
        append_worker_episode(&path, "contended follow-up two", "2026-07-18 12:18:01");
        drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            poll_state.consecutive_contended_scans(&cp_key),
            2,
            "an ordinary poll of a contended NAC store must not be throttled — \
             the second scan has to run at the ordinary poll cadence"
        );
        assert_eq!(
            metrics
                .sqlite_scan_failures_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "…and must reach the scan, not return before it"
        );

        super::super::contention_injection::disarm(&source_file);
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §2.1(2) — NAC's half of the `retry_blocked_replay` narrowing
    /// width. See the Cursor twin
    /// `a_crash_interrupted_replay_resumes_from_its_replaying_status` for the
    /// argument: `replaying` with no error status and no block reason is what a
    /// crash between `BeginReplay` and `FinalizeReplay` leaves, and the `error`
    /// disjunct cannot cover it.
    ///
    /// **[DIVERGENT FIXTURE]** the stat is unchanged since the cold poll, which
    /// is what a crashed replay looks like and what makes the two behaviours
    /// diverge: with the disjunct the cursor is reset and the scan runs,
    /// without it NAC's cheap short-circuit returns on the unchanged stat.
    ///
    /// Fails for: dropping the `checkpoint.status == "replaying"` disjunct.
    #[tokio::test]
    async fn a_crash_interrupted_nac_replay_resumes_from_its_replaying_status() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-replaying-resume".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let config = AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();

        let (cold, _) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(!cold.is_empty(), "the cold poll emits the fixture");

        {
            let mut map = checkpoints.write().await;
            let checkpoint = map
                .get_mut(&cp_key)
                .expect("the cold poll commits a checkpoint");
            checkpoint.status = "replaying".to_string();
            checkpoint.block_reason.clear();
            checkpoint.final_scan_complete = false;
        }

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;

        let after = checkpoints
            .read()
            .await
            .get(&cp_key)
            .cloned()
            .expect("the checkpoint survives the retry");
        assert_eq!(
            after.status, "active",
            "an interrupted replay must resume and finish, not be relabelled"
        );
        assert!(
            after.final_scan_complete,
            "a resumed replay finalizes; without the `replaying` disjunct the \
             poll returns on the unchanged stat and the source is stuck"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Appends one worker episode, exactly as a live NAC writer would, so a
    /// poll sees a changed stat fingerprint rather than the unchanged-file
    /// short-circuit.
    fn append_worker_episode(path: &Path, content: &str, created_at: &str) {
        let connection = Connection::open(path).expect("open NAC fixture for append");
        connection
            .execute(
                "INSERT INTO episodes (thread_name, session_id, action, content, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["worker-a", "session-local", "inspect", content, created_at],
            )
            .expect("append worker episode");
    }

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
        // Both separators must normalize, or the format guard's fail-open
        // read of a `'T'`-separated row latches the scan instead
        // (`a_mixed_updated_at_format_is_read_not_skipped` covers the scan
        // half; this pins the normalizer half).
        assert_eq!(
            normalize_nac_timestamp("2026-07-18T12:15:51.775682000", true).unwrap(),
            "2026-07-18T12:15:51.775682Z"
        );
        assert_eq!(
            normalize_nac_timestamp("2026-07-18T12:16:58", false).unwrap(),
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
            "last_response_duration_ms",
            "previous_response_duration_ms",
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
        assert!(
            projection.contains("CASE WHEN length(CAST(COALESCE(host_id, '') AS BLOB)) > 0 THEN 1")
        );
        assert!(!projection.split(',').any(|field| field.trim() == "host_id"));
        assert!(projection.contains("messages_json"));
        assert!(!projection.contains("'[]' AS messages_json"));
        assert!(projection.contains("response_durations_json"));
        assert!(!projection.contains("response_durations_ms_json"));
        assert!(projection.contains("AS estimated_bytes"));
        // §3.2's projection cleanup: exactly one `estimated_bytes` expression
        // per row — the old shape inlined the same 14-term length sum inside a
        // per-column CASE guard, 16 times per row. The oversize guard moved to
        // `read_session_row` (size and id first, payload columns never taken
        // for an oversized row); it did not disappear.
        assert_eq!(
            projection
                .matches("COALESCE(messages_json, '') AS BLOB")
                .count(),
            1,
            "the byte-sizing expression must appear exactly once, not once per \
             guarded column"
        );
        assert!(projection.contains("COALESCE(last_response_duration_ms, '') AS BLOB"));
        assert!(projection.contains("COALESCE(previous_response_duration_ms, '') AS BLOB"));
        assert!(projection.contains("COALESCE(host_id, '') AS BLOB"));
    }

    /// Issue #601 §3.2/§6. The `ScanLedger` refactor replaced
    /// `row.get::<_, String>(..)` with a helper that did
    /// `row.get::<Option<String>>(..).unwrap_or_default()` on every text column
    /// at once, which silently turned a NULL in a schema-required column from a
    /// failed scan into `""`. §3.2 wants schema drift **surfaced, not
    /// absorbed**: an empty `session_id` flows straight into logical-ID
    /// derivation and an empty `created_at`/`updated_at` into timestamp
    /// derivation (§6 "stable logical IDs"), so the record is not merely
    /// lossy — it is misattributed.
    ///
    /// Drives every column of the session projection that was strict before the
    /// refactor, and both columns that deliberately were not.
    ///
    /// Fails for: reading any of the seven required columns through
    /// `take_payload_nullable_string`, or making `backend`/`reasoning_effort`
    /// strict (older stores genuinely lack them).
    #[test]
    fn a_null_in_a_required_session_column_fails_the_scan() {
        // Positional, matching `session_projection`'s output order.
        let baseline = [
            "'ses-null-probe'",      // 0  session_id      required
            "'/work/moraine'",       // 1  cwd             required
            "'glm-5.2'",             // 2  model           required
            "'https://api.example'", // 3  base_url        required
            "'zai-coding-plan'",     // 4  backend         nullable
            "'high'",                // 5  reasoning_effort nullable
            "NULL",                  // 6  sandbox_json    nullable
            "'[]'",                  // 7  messages_json   required
            "NULL",                  // 8  last_response_duration_ms
            "NULL",                  // 9  previous_response_duration_ms
            "NULL",                  // 10 response_durations_json
            "NULL",                  // 11 token_usages_json
            "'2026-05-08 02:04:37'", // 12 created_at      required
            "'2026-05-08 02:04:38'", // 13 updated_at      required
            "0",                     // 14 is_remote
            "64",                    // 15 estimated_bytes
        ];
        let columns: BTreeSet<String> = ["host_id"].into_iter().map(str::to_string).collect();
        let connection = Connection::open_in_memory().expect("in-memory probe database");
        let read = |values: &[&str]| {
            let sql = format!("SELECT {}", values.join(", "));
            connection.query_row(&sql, [], |row| {
                let mut ledger = ScanLedger::default();
                Ok(read_session_row(row, &columns, &mut ledger).map(|_| ()))
            })
        };

        read(&baseline)
            .expect("probe query")
            .expect("the baseline row must read cleanly");

        for (index, name) in [
            (0, "session_id"),
            (1, "cwd"),
            (2, "model"),
            (3, "base_url"),
            (7, "messages_json"),
            (12, "created_at"),
            (13, "updated_at"),
        ] {
            let mut values = baseline;
            values[index] = "NULL";
            let outcome = read(&values).expect("probe query");
            let error = outcome
                .err()
                .unwrap_or_else(|| panic!("a NULL {name} must fail the scan, not become \"\""));
            let NacScanError::Scan(error) = error;
            assert!(
                format!("{error:#}").contains("Invalid column type"),
                "a NULL {name} must surface as a column-type error; got {error:#}"
            );
        }

        for (index, name) in [(4, "backend"), (5, "reasoning_effort")] {
            let mut values = baseline;
            values[index] = "NULL";
            read(&values).expect("probe query").unwrap_or_else(|_| {
                panic!("{name} is absent on older stores and must tolerate NULL")
            });
        }
    }

    /// Issue #601 §3.2. Same rule for the episode loop, which reads five
    /// NOT NULL columns and is the other half of what the ledger refactor
    /// loosened. Driven through a real scan because the loop is inline in
    /// `scan_nac_database`.
    ///
    /// The fixture's `episodes` table is rebuilt without its NOT NULL
    /// constraints so the NULL can be inserted at all — the column *set* is
    /// unchanged, which is what the adapter's schema check looks at.
    ///
    /// Fails for: reading `thread_name`/`session_id`/`action`/`content`/
    /// `created_at` through `take_payload_nullable_string`.
    #[test]
    fn a_null_in_a_required_episode_column_fails_the_scan() {
        let path = fixture_db();
        {
            let connection = Connection::open(&path).expect("open fixture for schema relaxation");
            connection
                .execute_batch(
                    r#"
                    ALTER TABLE episodes RENAME TO episodes_strict;
                    CREATE TABLE episodes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        thread_name TEXT,
                        session_id TEXT,
                        action TEXT,
                        content TEXT,
                        created_at TEXT
                    );
                    INSERT INTO episodes SELECT * FROM episodes_strict;
                    DROP TABLE episodes_strict;
                    UPDATE episodes SET content = NULL
                      WHERE id = (SELECT MIN(id) FROM episodes);
                    "#,
                )
                .expect("relax episodes schema and null a required column");
        }

        let stat = stat_fingerprint(path.to_str().expect("UTF-8 fixture path"))
            .expect("fixture stat fingerprint");
        let metadata = std::fs::metadata(&path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 fixture path"), &metadata);
        let mut ledger = ScanLedger::default();
        let outcome = scan_nac_database(
            path.to_str().expect("UTF-8 fixture path"),
            "nac-null-episode",
            1,
            inode,
            stat,
            &NacState::default(),
            &default_nac_budget(),
            None,
            MAX_NAC_CHECKPOINT_BYTES,
            &mut ledger,
        );
        match outcome {
            NacScanOutcome::Failed { error_text, .. } => assert!(
                error_text.contains("Invalid column type"),
                "a NULL episode column must surface as a column-type error; got {error_text}"
            ),
            NacScanOutcome::Scanned { .. } => {
                panic!("a NULL in a NOT NULL episode column must fail the scan, not become \"\"")
            }
        }

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    #[test]
    fn base_url_persistence_omits_credentials_paths_and_queries() {
        assert_eq!(
            sanitize_base_url("https://user:secret@proxy.example:8443/v1?sig=private#token"),
            "https://proxy.example:8443"
        );
        assert_eq!(sanitize_base_url("not a URL"), "");
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

    /// The shipped-default fast-path budget: what a production poll runs
    /// with when nothing in `[ingest.sqlite]` is overridden.
    fn default_nac_budget() -> ScanBudget {
        ScanBudget::fast_path(&moraine_config::SqliteIngestConfig::default())
    }

    fn scan_fixture(path: &Path, prior: &NacState) -> (Vec<SyntheticRecord>, NacState, u64) {
        let (records, state, relevant_rows, _) = scan_fixture_with_ledger(path, prior);
        (records, state, relevant_rows)
    }

    fn scan_fixture_with_ledger(
        path: &Path,
        prior: &NacState,
    ) -> (Vec<SyntheticRecord>, NacState, u64, ScanLedger) {
        let stat = stat_fingerprint(path.to_str().expect("UTF-8 fixture path"))
            .expect("fixture stat fingerprint");
        let metadata = std::fs::metadata(path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 fixture path"), &metadata);
        let mut ledger = ScanLedger::default();
        match scan_nac_database(
            path.to_str().expect("UTF-8 fixture path"),
            "nac-fixture",
            1,
            inode,
            stat,
            prior,
            &default_nac_budget(),
            None,
            MAX_NAC_CHECKPOINT_BYTES,
            &mut ledger,
        ) {
            NacScanOutcome::Scanned {
                records,
                state,
                relevant_rows,
                ..
            } => (records, *state, relevant_rows, ledger),
            NacScanOutcome::Failed {
                error_kind,
                error_text,
            } => panic!("fixture scan failed: {error_kind}: {error_text}"),
        }
    }

    /// `scan_fixture_with_ledger`, with a sweep slice requested — what a
    /// reconcile-triggered poll with an expired interval clock runs.
    fn scan_fixture_with_sweep(
        path: &Path,
        prior: &NacState,
        sweep: &SweepPlan,
    ) -> (Vec<SyntheticRecord>, NacState, u64, ScanLedger) {
        let stat = stat_fingerprint(path.to_str().expect("UTF-8 fixture path"))
            .expect("fixture stat fingerprint");
        let metadata = std::fs::metadata(path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 fixture path"), &metadata);
        let mut ledger = ScanLedger::default();
        match scan_nac_database(
            path.to_str().expect("UTF-8 fixture path"),
            "nac-fixture",
            1,
            inode,
            stat,
            prior,
            &default_nac_budget(),
            Some(sweep),
            MAX_NAC_CHECKPOINT_BYTES,
            &mut ledger,
        ) {
            NacScanOutcome::Scanned {
                records,
                state,
                relevant_rows,
                ..
            } => (records, *state, relevant_rows, ledger),
            NacScanOutcome::Failed {
                error_kind,
                error_text,
            } => panic!("sweep fixture scan failed: {error_kind}: {error_text}"),
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
        }
    }

    /// Issue #601 §2.0. The ledger is **caller-owned** exactly so that a scan
    /// which reads and then fails is still charged for what it read — the
    /// stated guarantee being "a scan that reads 48 MB and then loses the
    /// mixed-snapshot race is charged". Nothing asserted it: every NAC ledger
    /// test destructured `Scanned` and panicked otherwise, and the one test
    /// that does reach a failure arm
    /// (`scanner_rejects_a_database_replaced_before_open`) throws the ledger
    /// away with `&mut ScanLedger::default()`.
    ///
    /// **[DIVERGENT FIXTURE]** the failing session sorts *last* by
    /// `session_id`, so the scan has already paid for real rows before it
    /// fails; a fixture whose only session fails would report zero bytes for an
    /// honest reason and prove nothing.
    ///
    /// Fails for: resetting or rebuilding the ledger on any
    /// `scan_nac_database` failure arm.
    #[test]
    fn a_failed_nac_scan_still_reports_the_bytes_it_had_already_read() {
        let path = fixture_db();
        let connection = Connection::open(&path).expect("open fixture");
        // A row that reads fine and then fails to parse: `read_session_row`
        // charges every column it materialized before it validates the JSON.
        connection
            .execute(
                "INSERT INTO sessions (session_id, cwd, model, base_url, backend, \
                 reasoning_effort, sandbox_json, messages_json, response_durations_json, \
                 token_usages_json, created_at, updated_at) \
                 VALUES ('zzz-broken-json', '/tmp', 'm', '', '', '', NULL, ?1, NULL, NULL, \
                 '2026-07-18 12:00:00', '2026-07-18 12:00:00')",
                ["this is not json at all"],
            )
            .expect("insert unparseable session");
        drop(connection);

        let stat = stat_fingerprint(path.to_str().expect("UTF-8 path")).expect("fixture stat");
        let metadata = std::fs::metadata(&path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 path"), &metadata);
        let mut ledger = ScanLedger::default();
        let outcome = scan_nac_database(
            path.to_str().expect("UTF-8 path"),
            "nac-fixture",
            1,
            inode,
            stat,
            &NacState::default(),
            &default_nac_budget(),
            None,
            MAX_NAC_CHECKPOINT_BYTES,
            &mut ledger,
        );
        let NacScanOutcome::Failed { error_kind, .. } = outcome else {
            panic!("an unparseable messages_json must fail the scan");
        };
        assert_eq!(error_kind, ERROR_KIND_SCAN);
        assert!(
            ledger.payload_rows > 1,
            "the sessions read before the failure are still charged; got {}",
            ledger.payload_rows
        );
        assert!(
            ledger.payload_bytes > 0,
            "a failed scan must still report the bytes it paid for"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §2.0 + §2.3, the episode arm. The row charge lands *before*
    /// the oversize check — SQLite fully decoded the row to evaluate
    /// `estimated_bytes` — and the oversized row itself is an un-processable
    /// single row: one `ingest_errors` row, skipped, watermark advanced past
    /// it. This REWRITES the old `TooLarge` latch assertions: the scan used to
    /// fail outright, which stalled `episode_high_water` forever on one bad
    /// row — the latch class §2.3 retires.
    ///
    /// **[DIVERGENT FIXTURE]** the oversized episode is not the first row read,
    /// so the byte axis is non-zero for reasons independent of the row charge,
    /// and the two axes stay distinguishable.
    ///
    /// Fails for: moving the episode `charge_payload_row` back below the
    /// oversize check, restoring the `TooLarge` failure, or a skip that does
    /// not advance the watermark.
    #[test]
    fn an_oversized_episode_still_charges_the_row_it_decoded() {
        let path = fixture_db();
        let connection = Connection::open(&path).expect("open fixture");
        // One byte past the row ceiling, computed the way the scan computes it.
        let oversized = "e".repeat(SCAN_PAGE_MAX_BYTES + 1);
        connection
            .execute(
                "INSERT INTO episodes (thread_name, session_id, action, content, created_at) \
                 VALUES ('zzz-oversized-thread', 'session-local', 'run', ?1, \
                 '2026-07-18 12:30:00')",
                [&oversized],
            )
            .expect("insert oversized episode");
        drop(connection);

        let stat = stat_fingerprint(path.to_str().expect("UTF-8 path")).expect("fixture stat");
        let metadata = std::fs::metadata(&path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 path"), &metadata);
        let mut ledger = ScanLedger::default();
        let outcome = scan_nac_database(
            path.to_str().expect("UTF-8 path"),
            "nac-fixture",
            1,
            inode,
            stat,
            &NacState::default(),
            &default_nac_budget(),
            None,
            MAX_NAC_CHECKPOINT_BYTES,
            &mut ledger,
        );
        let NacScanOutcome::Scanned {
            state, row_errors, ..
        } = outcome
        else {
            panic!("an oversized episode degrades per §2.3; it must not fail the scan");
        };
        assert_eq!(row_errors.len(), 1, "one error row for the skipped episode");
        assert_eq!(
            row_errors[0].error_kind,
            super::super::ERROR_KIND_ROW_TOO_LARGE
        );
        let max_episode_id: i64 = {
            let connection = Connection::open(&path).expect("reopen fixture for counting");
            connection
                .query_row("SELECT max(id) FROM episodes", [], |row| row.get(0))
                .expect("max episode id")
        };
        assert_eq!(
            state.episode_high_water, max_episode_id,
            "the watermark advances past the skipped row, so the report is one-shot"
        );

        // Every session, every earlier episode, **and** the skipped episode.
        let expected_rows = {
            let connection = Connection::open(&path).expect("reopen fixture for counting");
            let sessions: i64 = connection
                .query_row("SELECT count(*) FROM sessions", [], |row| row.get(0))
                .expect("count sessions");
            let episodes: i64 = connection
                .query_row("SELECT count(*) FROM episodes", [], |row| row.get(0))
                .expect("count episodes");
            (sessions + episodes) as u64
        };
        assert_eq!(
            ledger.payload_rows, expected_rows,
            "the skipped episode was decoded in full to evaluate its size, so \
             the skip arm must charge it like any other row"
        );
        assert!(
            ledger.payload_bytes > 0,
            "and the rows read before it are still charged"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §2.0 / WI-01, NAC arm. **[DIVERGENT FIXTURE]** the second
    /// scan carries the first scan's cursor, so it emits nothing while reading
    /// exactly the same bytes — a ledger charged at the emit site reports zero
    /// there, and a ledger charged only on the changed branch reports zero too.
    ///
    /// Fails for: charging from emitted records, charging only changed rows,
    /// or dropping the row/byte charge from `read_session_row` or the episode
    /// loop.
    #[test]
    fn nac_ledger_charges_payload_bytes_at_the_read_site() {
        let path = fixture_db();
        let connection = Connection::open(&path).expect("open fixture for counting");
        let sessions: i64 = connection
            .query_row("SELECT count(*) FROM sessions", [], |row| row.get(0))
            .expect("count sessions");
        let episodes: i64 = connection
            .query_row("SELECT count(*) FROM episodes", [], |row| row.get(0))
            .expect("count episodes");
        let messages_bytes: i64 = connection
            .query_row(
                "SELECT coalesce(sum(length(CAST(messages_json AS BLOB))), 0) FROM sessions",
                [],
                |row| row.get(0),
            )
            .expect("sum messages bytes");
        drop(connection);

        let (records, state, _, ledger) = scan_fixture_with_ledger(&path, &NacState::default());
        assert!(!records.is_empty(), "the cold scan must emit something");
        assert_eq!(
            ledger.payload_rows,
            (sessions + episodes) as u64,
            "every session and episode row is a payload read"
        );
        assert_eq!(ledger.rows_emitted, records.len() as u64);
        assert!(
            ledger.payload_bytes > messages_bytes as u64,
            "payload bytes must cover messages_json plus the other materialized \
             columns; got {} against {messages_bytes} of messages alone",
            ledger.payload_bytes
        );
        // The census axis carries schema validation plus the §3.2 session
        // census: one narrow `(session_id, updated_at)` row per session. The
        // payload read must not be miscounted as census, or every payload
        // budget deflates.
        let census_key_bytes: i64 = {
            let connection = Connection::open(&path).expect("reopen for census bytes");
            connection
                .query_row(
                    "SELECT coalesce(sum(length(CAST(session_id AS BLOB)) +                      length(CAST(coalesce(updated_at, '') AS BLOB))), 0) FROM sessions",
                    [],
                    |row| row.get(0),
                )
                .expect("sum census bytes")
        };
        let schema_census = {
            let connection =
                super::open_read_only(&path.to_string_lossy()).expect("reopen for schema census");
            crate::sqlite_poll::expected_schema_census(&connection, &["sessions", "episodes"])
        };
        assert!(schema_census.census_rows > 0);
        assert_eq!(
            ledger.census_rows,
            schema_census.census_rows + sessions as u64,
            "census rows are schema validation plus one narrow row per session"
        );
        assert_eq!(
            ledger.census_bytes,
            schema_census.census_bytes + census_key_bytes as u64
        );

        // The warm fast path reads **nothing** on an unchanged store — the
        // §3.2 point: every session sits at or before the keyset watermark,
        // so no payload row is materialized at all.
        let (second_records, second_state, _, second) = scan_fixture_with_ledger(&path, &state);
        assert!(
            second_records.is_empty(),
            "the warm scan must emit nothing, or the divergence is not exercised"
        );
        assert_eq!(second.rows_emitted, 0);
        assert_eq!(
            second.payload_rows, 0,
            "the keyset fast path must not re-read unchanged sessions"
        );
        assert_eq!(second.payload_bytes, 0);

        // A sweep slice is now the read-without-emitting case (the read-site
        // charging this test exists for): the slice re-reads every session in
        // `session_id` order, finds every hash unchanged, and emits nothing —
        // an emit-site ledger would report zero where real bytes were paid,
        // and a slice hidden from the payload axes would evade every budget.
        let (third_records, _, _, third) = scan_fixture_with_sweep(
            &path,
            &second_state,
            &SweepPlan::from_config(&AppConfig::default()),
        );
        assert!(third_records.is_empty(), "nothing changed, nothing emits");
        assert_eq!(third.rows_emitted, 0);
        assert_eq!(
            third.payload_rows, sessions as u64,
            "the slice re-read every session; an emit-site ledger reports zero"
        );
        assert_eq!(
            third.sweep_rows, sessions as u64,
            "and those reads are attributed to the sweep axes through the fold"
        );
        assert!(
            third.payload_bytes >= messages_bytes as u64,
            "a scan that emits nothing still pays for every session it read"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
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
        .0
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
        .0
        .into_iter()
        .map(|(id, _, line, offset)| (id, (line, offset)))
        .collect::<BTreeMap<_, _>>();
        for (logical_id, coordinates) in before {
            assert_eq!(after.get(&logical_id), Some(&coordinates));
        }
    }

    /// Issue #601 §2.3. REWRITES `logical_part_budget_fails_before_building_
    /// an_unbounded_vector`, whose `TooLarge` failure latched the whole scan
    /// on one over-budget session. The budget still bounds the vector — that
    /// was the old test's live property and it is kept — but overflow now
    /// truncates: the built prefix is committed and the truncation is
    /// reported, so the session degrades instead of stopping the adapter.
    #[test]
    fn a_logical_part_budget_truncates_the_parts_instead_of_failing() {
        let session = session_with_messages(json!([{
            "role": "assistant",
            "reasoning_text": "reasoning",
            "content": "answer"
        }]));
        let (parts, truncated) = logical_message_parts(
            &session,
            "nac:stable-session",
            "2026-07-18T12:00:00.000000000Z",
            1,
        )
        .expect("an over-budget session truncates, it does not fail");
        assert_eq!(parts.len(), 1, "the budget still bounds the vector");
        assert!(truncated, "and the truncation is reported, not silent");

        let (all_parts, untruncated) = logical_message_parts(
            &session,
            "nac:stable-session",
            "2026-07-18T12:00:00.000000000Z",
            usize::MAX,
        )
        .expect("an unbounded budget builds every part");
        assert!(all_parts.len() > 1);
        assert!(!untruncated);
    }

    /// `scan_fixture_with_ledger`, parameterized: the budget/ceiling tests
    /// inject their `[ingest.sqlite]` values here, exactly as an operator's
    /// config would arrive through `process_nac_sqlite_db`.
    #[allow(clippy::type_complexity)]
    fn scan_fixture_with_budget(
        path: &Path,
        prior: &NacState,
        budget: &ScanBudget,
        checkpoint_ceiling_bytes: usize,
    ) -> (Vec<SyntheticRecord>, NacState, Vec<NacRowError>, ScanLedger) {
        let stat = stat_fingerprint(path.to_str().expect("UTF-8 fixture path"))
            .expect("fixture stat fingerprint");
        let metadata = std::fs::metadata(path).expect("fixture metadata");
        let inode = source_inode_for_file(path.to_str().expect("UTF-8 fixture path"), &metadata);
        let mut ledger = ScanLedger::default();
        match scan_nac_database(
            path.to_str().expect("UTF-8 fixture path"),
            "nac-fixture",
            1,
            inode,
            stat,
            prior,
            budget,
            None,
            checkpoint_ceiling_bytes,
            &mut ledger,
        ) {
            NacScanOutcome::Scanned {
                records,
                state,
                row_errors,
                ..
            } => (records, *state, row_errors, ledger),
            NacScanOutcome::Failed {
                error_kind,
                error_text,
            } => panic!("budgeted fixture scan must degrade, not fail: {error_kind}: {error_text}"),
        }
    }

    fn cleanup_fixture(path: &Path) {
        std::fs::remove_file(path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §2.3 / gate G4 (NAC work budget — replaces the retired
    /// `MAX_NAC_SESSIONS` / `MAX_NAC_SCAN_BYTES` ceilings). **[DIVERGENT
    /// FIXTURE]** (§8 G4): the newest session by `updated_at`
    /// (`session-remote`) sorts *last* in `session_id` order — the census
    /// order a broken recency sort would fall back to — so a 1-row budget
    /// reads it only if newest-first ordering actually works. Episodes are
    /// watermarked past so the session ordering is observed in isolation.
    /// Fails for: a budget that fails the scan instead of degrading (the old
    /// `TooLarge` latch), bounded progress that is not newest-first, and any
    /// error row minted for history size.
    ///
    /// MUTATION (executed 2026-07-31): restore census (`session_id ASC`)
    /// candidate order by deleting the `sort_by` — fails here (the oldest
    /// session is read instead). Reverting degradation to a `TooLarge`
    /// failure fails at the panic in `scan_fixture_with_budget`.
    #[test]
    fn a_nac_store_over_budget_still_ingests_the_newest_session_first() {
        let path = fixture_db();
        let prior = NacState {
            episode_high_water: 3,
            ..NacState::default()
        };
        let budget = ScanBudget {
            max_payload_rows: 1,
            max_payload_bytes: u64::MAX,
        };
        let (records, state, row_errors, ledger) =
            scan_fixture_with_budget(&path, &prior, &budget, MAX_NAC_CHECKPOINT_BYTES);

        assert!(!records.is_empty(), "the newest session must still emit");
        let serialized: Vec<String> = records
            .iter()
            .map(|record| serde_json::to_string(&record.record).expect("serialize record"))
            .collect();
        assert!(
            serialized.iter().any(|raw| raw.contains("session-remote")),
            "bounded progress must reach the newest session first"
        );
        assert!(
            !serialized.iter().any(|raw| raw.contains("session-local")),
            "census-order truncation would emit the oldest session; newest-first must not"
        );
        assert!(row_errors.is_empty(), "history size is never an error row");
        assert!(ledger.coverage_degraded);
        assert_eq!(
            ledger.payload_rows, 1,
            "the read stops exactly at its budget"
        );
        assert_eq!(
            ledger.skipped_rows, 1,
            "the unread session is accounted for"
        );
        assert!(state.sessions.contains_key("session-remote"));
        assert!(
            !state.sessions.contains_key("session-local"),
            "an unread session must stay absent so a later poll detects it"
        );
        assert!(state.pending_coverage, "the remainder is a durable debt");

        cleanup_fixture(&path);
    }

    /// The forward-progress rule of the episode loop, bounded from both
    /// sides: a poll whose session reads already exhausted the budget still
    /// processes **exactly one** episode — zero would let a busy session set
    /// starve the episode watermark forever (the §2.5 latch class), and more
    /// than one would ignore the budget (the runaway side). The ledger's side
    /// of the break is pinned too: the row the break leaves unprocessed was
    /// still materialized (the projection includes `content`), so it is
    /// charged on the rows axis *and* counted in `skipped_rows` — cost and
    /// debt are different axes.
    ///
    /// MUTATION (executed 2026-07-31): drop `episodes_processed > 0 &&` from
    /// the episode-loop budget check — fails (high water stays 0). Delete
    /// the budget break entirely — fails (high water reaches 3). Drop the
    /// break arm's `charge_payload_row` — fails (`payload_rows` reads 3, not
    /// 4). Each RED was confirmed in a filtered run, so suite-wide isolation
    /// is not claimed.
    #[test]
    fn an_exhausted_session_budget_still_advances_the_episode_watermark() {
        let path = fixture_db();
        let budget = ScanBudget {
            max_payload_rows: 1,
            max_payload_bytes: u64::MAX,
        };
        let (records, state, row_errors, ledger) = scan_fixture_with_budget(
            &path,
            &NacState::default(),
            &budget,
            MAX_NAC_CHECKPOINT_BYTES,
        );

        assert_eq!(
            state.episode_high_water, 1,
            "exactly one episode advances per exhausted poll"
        );
        assert!(
            serde_json::to_string(&records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records")
                .contains("worker-a"),
            "the advanced episode's worker records must emit"
        );
        assert!(row_errors.is_empty());
        assert!(ledger.coverage_degraded);
        assert_eq!(
            ledger.skipped_rows, 3,
            "one unread session and two deferred episodes are accounted for"
        );
        assert_eq!(
            ledger.payload_rows, 4,
            "one budgeted session read, the processed episode plus its \
             parent's point lookup, and the episode the break left behind: a \
             materialized row is charged even when the budget defers it, and \
             the deferred row also sits inside `skipped_rows` — cost and \
             debt are different axes"
        );
        assert!(state.pending_coverage, "the episode tail is a durable debt");

        cleanup_fixture(&path);
    }

    /// The **byte** axis of the same budget, at both NAC call sites. The
    /// rows-axis tests above cannot see a call site that stops feeding the
    /// byte argument (`is_exhausted_by(ledger.payload_rows, 0)` survives
    /// them), and the shared-`ScanBudget` boundary test only pins the
    /// predicate's implementation — this pins NAC's use of it. A 1-byte
    /// budget binds after the first session *and* after the first episode:
    /// the same commit/degrade/forward-progress shape as the rows axis.
    ///
    /// MUTATION (executed 2026-07-31): pass `0` for the byte argument at the
    /// session-loop call site — fails (both sessions read). Same at the
    /// episode-loop call site — fails (every episode read).
    #[test]
    fn a_nac_byte_budget_binds_at_both_call_sites() {
        let path = fixture_db();
        let budget = ScanBudget {
            max_payload_rows: u64::MAX,
            max_payload_bytes: 1,
        };
        let (_, state, row_errors, ledger) = scan_fixture_with_budget(
            &path,
            &NacState::default(),
            &budget,
            MAX_NAC_CHECKPOINT_BYTES,
        );

        assert_eq!(
            state.sessions.len(),
            1,
            "the byte budget binds after the first session read"
        );
        assert!(state.sessions.contains_key("session-remote"));
        assert_eq!(
            state.episode_high_water, 1,
            "one episode still advances, then the byte budget binds"
        );
        assert!(ledger.coverage_degraded);
        assert!(row_errors.is_empty());
        assert!(state.pending_coverage);

        cleanup_fixture(&path);
    }

    /// Issue #601 §2.3 / gate G4 (NAC checkpoint-state ceiling): crossing
    /// `MAX_NAC_CHECKPOINT_BYTES` evicts the **oldest** session cursors until
    /// the payload fits — never fails the scan — and reports the eviction as
    /// degraded coverage. Emission happens before eviction, so recent
    /// sessions still emit; the evicted session is re-covered (and re-emits)
    /// on a later poll, which §6's content-addressed identity makes safe.
    ///
    /// MUTATION (executed 2026-07-31): make `evict_to_fit` a no-op returning
    /// 0 — fails (the state exceeds its ceiling and nothing is evicted).
    /// Evict newest-first (`by_age.sort` descending) — fails (the newest
    /// session is dropped instead of the oldest).
    #[test]
    fn a_checkpoint_over_its_ceiling_evicts_the_oldest_sessions_instead_of_failing() {
        let path = fixture_db();
        let (_, full_state, _) = scan_fixture(&path, &NacState::default());
        let full_len = full_state.serialize().expect("serialize full state").len();

        // One byte under the full payload: eviction must fire, and dropping
        // the single oldest session cursor is enough to fit.
        let ceiling = full_len - 1;
        let (records, state, row_errors, ledger) =
            scan_fixture_with_budget(&path, &NacState::default(), &default_nac_budget(), ceiling);

        assert_eq!(ledger.evicted_entries, 1, "one round of the oldest eighth");
        assert!(ledger.coverage_degraded);
        assert!(
            row_errors.is_empty(),
            "a state ceiling is never an error row"
        );
        assert!(
            !state.sessions.contains_key("session-local"),
            "eviction is oldest-first by created_at"
        );
        assert!(
            state.sessions.contains_key("session-remote"),
            "the newest session's cursor survives"
        );
        assert!(
            state.serialize().expect("serialize evicted state").len() <= ceiling,
            "the persisted payload must fit its ceiling"
        );
        assert!(
            serde_json::to_string(&records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records")
                .contains("session-local"),
            "emission precedes eviction: recent work is not withheld"
        );
        assert!(
            state.pending_coverage,
            "an evicted session is a durable coverage debt"
        );

        cleanup_fixture(&path);
    }

    /// §2.3's "continue next poll" for NAC, end to end, with **no further
    /// writes to the store**: a 1-row budget against two sessions and three
    /// episodes converges to full coverage across resumed polls — never-read
    /// sessions first, one episode per poll — then clears the durable marker
    /// and quiesces on the cheap stat short-circuit.
    ///
    /// MUTATION (executed 2026-07-31): drop the `!scan_state.pending_coverage`
    /// conjunct from the short-circuit — fails at the convergence loop (the
    /// unchanged stat ends every later poll). Drop the never-read-first
    /// class from the candidate sort — fails the same way (every poll
    /// re-reads `session-remote`).
    #[tokio::test]
    async fn a_degraded_nac_cold_ingest_completes_without_new_writes() {
        let path = fixture_db();
        {
            // The fixture ships in WAL mode, where the scan's own read-only
            // open can touch `-shm` and keep the stat moving. Pin DELETE
            // journal mode so "no further writes" really means an unchanged
            // stat — otherwise sidecar churn, not the durable resume marker,
            // would keep the polls alive and this test could not fail.
            let connection = Connection::open(&path).expect("open fixture for journal change");
            connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .expect("checkpoint fixture WAL");
            connection
                .query_row("PRAGMA journal_mode = DELETE", [], |_| Ok(()))
                .expect("switch fixture to DELETE journal mode");
        }
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-cold-converges".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let mut config = AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_rows = 1;

        let (first, _) =
            drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        let error_rows: usize = first.iter().map(|batch| batch.error_rows.len()).sum();
        assert_eq!(error_rows, 0, "a work budget is never an error");
        {
            let map = checkpoints.read().await;
            let checkpoint = map.get(&cp_key).expect("cold poll persists");
            assert_eq!(checkpoint.status, "active");
            assert!(
                checkpoint.cursor_json.contains("pending_coverage"),
                "the resume marker must be durable"
            );
        }

        // No touches: convergence must come from the marker alone.
        let mut polls = 1;
        loop {
            drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
            polls += 1;
            let map = checkpoints.read().await;
            let state = NacState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
            if !state.pending_coverage {
                assert_eq!(state.sessions.len(), 2, "every session covered");
                assert_eq!(state.episode_high_water, 3, "every episode covered");
                break;
            }
            assert!(
                polls <= 8,
                "a 1-row budget against 2 sessions and 3 episodes must converge; \
                 still pending after {polls} polls"
            );
        }

        // Quiesce: coverage complete, stat unchanged — the next poll must
        // not scan.
        let rows_before = metrics
            .sqlite_poll_payload_rows_total
            .load(std::sync::atomic::Ordering::Relaxed);
        drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            metrics
                .sqlite_poll_payload_rows_total
                .load(std::sync::atomic::Ordering::Relaxed),
            rows_before,
            "a covered, unchanged store must short-circuit"
        );

        cleanup_fixture(&path);
    }

    /// The NAC twin of `a_replacement_replay_reads_past_the_fast_path_budget`:
    /// a replacement replay ignores the fast-path budget, because its
    /// finalize publishes the generation whole and a degraded replay would
    /// publish a hole through #602.
    ///
    /// MUTATION (executed 2026-07-31): make `process_nac_sqlite_db` pass the
    /// fast-path budget on replays too — this test fails (one session and
    /// one episode covered); RED was confirmed in a filtered run,
    /// so suite-wide isolation is not claimed.
    #[tokio::test]
    async fn a_nac_replacement_replay_reads_past_the_fast_path_budget() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-replay-unbudgeted".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let mut config = AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_rows = 1;

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;

        // A changed exclusion set starts a replacement replay under the same
        // tight budget; the replay must ignore it.
        let mut replaying = config.clone();
        replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
        let (_, transitions) = drive_nac_poll(&replaying, &work, &checkpoints, &poll_state).await;
        assert_eq!(transitions.len(), 2, "begin and final replay barriers");

        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("finalized replay checkpoint");
        let state = NacState::parse(&checkpoint.cursor_json);
        assert_eq!(
            state.sessions.len(),
            2,
            "the replay must cover every session, budget notwithstanding"
        );
        assert_eq!(state.episode_high_water, 3);
        assert!(!state.pending_coverage, "a finalized replay owes nothing");
        assert_eq!(checkpoint.status, "active");

        cleanup_fixture(&path);
    }

    /// Plan §7.2 F2, the widening direction, at the NAC call site: an
    /// `error`-status checkpoint with no block reason is an ordinary
    /// transient failure marker. Widening `retry_blocked_replay` to a bare
    /// `status == "error"` turns the next poll into a blocked-replacement
    /// retry — cursor reset, full re-read, every unchanged row re-emitted
    /// behind a fresh `BeginReplay`. The unchanged fixture emits nothing on
    /// the correct path, so any re-emission or barrier fails this test.
    ///
    /// MUTATION (executed 2026-07-31): drop
    /// `&& !checkpoint.block_reason.is_empty()` from NAC's
    /// `retry_blocked_replay` — this test fails (rows re-emit behind a
    /// barrier); RED was confirmed in a filtered run,
    /// so suite-wide isolation is not claimed.
    #[tokio::test]
    async fn an_error_marker_without_a_block_reason_is_not_retried_as_a_blocked_nac_replay() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-error-marker-width".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        let (first, _) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(!first.last().expect("cold batch").raw_rows.is_empty());

        // Rewrite the committed checkpoint into a transient-error marker:
        // the shape the non-replay failure arm persists.
        {
            let mut map = checkpoints.write().await;
            let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
            checkpoint.status = "error".to_string();
            checkpoint.block_reason.clear();
            let mut state = NacState::parse(&checkpoint.cursor_json);
            state.last_error = ERROR_KIND_SCAN.to_string();
            checkpoint.cursor_json = state.serialize().expect("serialize error marker");
        }

        let (batches, transitions) =
            drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(
            transitions.is_empty(),
            "an ordinary error marker must not raise a replay barrier"
        );
        let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
        assert_eq!(
            raw_rows, 0,
            "re-emission means the poll was retried as a blocked replay"
        );
        let map = checkpoints.read().await;
        assert_eq!(
            map.get(&cp_key).expect("checkpoint").status,
            "active",
            "the transient marker clears once a scan succeeds"
        );

        cleanup_fixture(&path);
    }

    /// Plan §7.2 F1, the `new_state == prior_state_covered` conjunct of NAC's
    /// `scan_is_noop`: a session deletion moves the cursor map and nothing
    /// else — no records, same schema, same stat class — so only the
    /// structural comparison keeps the deletion's checkpoint from being
    /// suppressed, re-discovered on every later poll, and never durably
    /// recorded. The census is the deletion detector (§3.2), so this guard
    /// belongs to the census's owner.
    ///
    /// MUTATION (executed 2026-07-31): drop `new_state == prior_state_covered`
    /// from NAC's `scan_is_noop` — this test fails (the deleted session's
    /// cursor survives forever); RED was confirmed in a filtered run,
    /// so suite-wide isolation is not claimed.
    #[tokio::test]
    async fn a_deleted_nac_session_is_dropped_from_the_cursor_durably() {
        let path = fixture_db();
        {
            let connection = Connection::open(&path).expect("open fixture for insert");
            connection
                .execute(
                    "INSERT INTO sessions (session_id, cwd, model, base_url, messages_json, \
                     created_at, updated_at) VALUES ('zz-observed', '/workspace/zz', 'model-z', \
                     'https://api.example', '[{\"role\":\"user\",\"content\":\"zz\"}]', \
                     '2026-07-19 09:00:00', '2026-07-19 09:00:01')",
                    [],
                )
                .expect("insert observed session");
        }
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-deletion-durable".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        {
            let map = checkpoints.read().await;
            let state = NacState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
            assert!(state.sessions.contains_key("zz-observed"));
        }

        {
            let connection = Connection::open(&path).expect("open fixture for delete");
            connection
                .execute("DELETE FROM sessions WHERE session_id = 'zz-observed'", [])
                .expect("delete observed session");
        }
        let (batches, _) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
        assert_eq!(raw_rows, 0, "a deletion emits nothing — only state moves");
        let map = checkpoints.read().await;
        let state = NacState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
        assert!(
            !state.sessions.contains_key("zz-observed"),
            "the deletion must be recorded durably, not re-discovered forever"
        );

        cleanup_fixture(&path);
    }

    /// §0's both-ways guard, deletion side — the unit twin of the e2e's
    /// "nac deleted remote metadata remains archived": a session present in
    /// the committed cursor but absent from the store — deleted, its episodes
    /// with it, so `max(episodes.id)` regressed below the watermark — is
    /// **archival**. The poll emits nothing, raises no replacement barrier,
    /// and keeps its generation; only the cursor forgets the session.
    /// Deletion is not disappearance-rewind: the AUTOINCREMENT id ceiling
    /// still covers the watermark, so the vanished coordinates can never be
    /// rewritten. And it is not new work: the surviving, unchanged session is
    /// not re-read, let alone re-emitted.
    ///
    /// **[DIVERGENT FIXTURE]** the deletion leaves `sqlite_sequence`
    /// untouched — the only shape deleting rows from a real NAC store can
    /// produce. The pre-fix code routed every max-id regression through
    /// generation replacement, re-emitting the surviving store into a fresh
    /// namespace: one extra `session_meta` per surviving session, which is
    /// exactly the e2e's remote-metadata count going 2 → 3.
    ///
    /// Fails for: dropping the id-ceiling conjunct from the preflight (the
    /// deletion replays and re-emits), dropping it from the scan-side arm
    /// (the scan errors and the cursor never records the deletion), or
    /// re-emitting carried sessions on a deletion poll.
    #[tokio::test]
    async fn a_session_deleted_from_the_store_emits_nothing_new() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-deletion-archival".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let generation_before = checkpoints
            .read()
            .await
            .get(&cp_key)
            .expect("cold checkpoint")
            .source_generation;

        // The deletion: the session row and its episode go together, exactly
        // as the store's ON DELETE CASCADE would do it. max(episodes.id)
        // regresses 3 → 2; sqlite_sequence stays at 3.
        {
            let connection = Connection::open(&path).expect("open fixture for deletion");
            connection
                .execute(
                    "DELETE FROM episodes WHERE session_id = 'session-remote'",
                    [],
                )
                .expect("delete remote episodes");
            connection
                .execute(
                    "DELETE FROM sessions WHERE session_id = 'session-remote'",
                    [],
                )
                .expect("delete remote session");
        }

        let (batches, transitions) =
            drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(
            transitions.is_empty(),
            "a deletion must raise no replacement barrier; got {transitions:?}"
        );
        let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
        assert_eq!(
            raw_rows, 0,
            "a deletion emits nothing new — the archived canonical rows are \
             the record of the deleted session"
        );
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("post-deletion checkpoint");
        assert_eq!(
            checkpoint.source_generation, generation_before,
            "deletion is archival, never a generation replacement"
        );
        let state = NacState::parse(&checkpoint.cursor_json);
        assert!(
            !state.sessions.contains_key("session-remote"),
            "the cursor forgets the deleted session durably"
        );
        assert!(
            state.sessions.contains_key("session-local"),
            "the surviving session's cursor rides forward untouched"
        );
        assert_eq!(
            state.episode_high_water, 3,
            "the watermark stands: AUTOINCREMENT allocates every future \
             episode above it, so nothing behind it is ever coverable again"
        );
        assert!(
            !state
                .worker_threads
                .iter()
                .any(|key| key.starts_with("session-remote\n")),
            "the census prune drops the deleted session's worker identity"
        );

        cleanup_fixture(&path);
    }

    /// §0's both-ways guard, live side: the deletion skip must not widen into
    /// skipping live work. In the same poll as a *visible* max-id regression
    /// (the deleted episode was the store maximum), a genuinely-new session
    /// still emits first-class — and on the next poll the stood watermark
    /// still admits the next AUTOINCREMENT episode, which is allocated above
    /// the deleted tail, never inside it.
    ///
    /// Fails for: a deletion arm that ends the scan early (skipping the
    /// candidate read beside it), a preflight that turns a regression poll
    /// into a wholesale no-op, or a watermark reset that makes the deleted
    /// tail look like unread coverage debt.
    #[tokio::test]
    async fn a_new_session_beside_a_deletion_still_emits() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-deletion-live-side".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;

        // One write burst: the remote session vanishes (max(id) 3 → 2, a
        // visible regression) and a genuinely-new session lands.
        {
            let connection = Connection::open(&path).expect("open fixture for burst");
            connection
                .execute(
                    "DELETE FROM episodes WHERE session_id = 'session-remote'",
                    [],
                )
                .expect("delete remote episodes");
            connection
                .execute(
                    "DELETE FROM sessions WHERE session_id = 'session-remote'",
                    [],
                )
                .expect("delete remote session");
            connection
                .execute(
                    "INSERT INTO sessions (session_id, cwd, model, base_url, messages_json, \
                     created_at, updated_at) VALUES ('zz-fresh', '/workspace/fresh', 'model-f', \
                     'https://api.example', \
                     '[{\"role\":\"user\",\"content\":\"fresh question\"}]', \
                     '2026-07-18 14:00:00', '2026-07-18 14:00:01')",
                    [],
                )
                .expect("insert fresh session");
        }

        let (batches, transitions) =
            drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(transitions.is_empty(), "no barrier: this is not a rewind");
        let raw: Vec<String> = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .filter_map(|row| row.get("raw_json").and_then(Value::as_str))
            .map(str::to_string)
            .collect();
        assert!(
            raw.iter().any(|row| row.contains("fresh question")),
            "the new session must emit first-class beside the deletion; got {raw:?}"
        );
        assert!(
            !raw.iter().any(|row| row.contains("session-remote")),
            "nothing may be emitted for the deleted session; got {raw:?}"
        );
        assert!(
            !raw.iter().any(|row| row.contains("session-local")),
            "the carried, unchanged session must not be re-emitted; got {raw:?}"
        );

        // The stood watermark still admits new episode work: AUTOINCREMENT
        // allocates the next id above the deleted tail (4, not a reused 3).
        append_worker_episode(&path, "fresh worker payload", "2026-07-18 14:00:02");
        {
            let connection = Connection::open(&path).expect("open fixture to re-home episode");
            connection
                .execute(
                    "UPDATE episodes SET session_id = 'zz-fresh', thread_name = 'worker-fresh' \
                     WHERE id = (SELECT max(id) FROM episodes)",
                    [],
                )
                .expect("re-home appended episode");
        }
        let (batches, _) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let raw: Vec<String> = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .filter_map(|row| row.get("raw_json").and_then(Value::as_str))
            .map(str::to_string)
            .collect();
        assert!(
            raw.iter().any(|row| row.contains("fresh worker payload")),
            "an episode appended after the deletion must ingest; got {raw:?}"
        );
        let map = checkpoints.read().await;
        let state = NacState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
        assert_eq!(
            state.episode_high_water, 4,
            "the new episode is allocated above the deleted tail"
        );

        cleanup_fixture(&path);
    }

    /// The e2e deletion scenario's exact shape: deleting a session's episodes
    /// while the session row survives is a **silent noop** — no records, no
    /// checkpoint write, no barrier — and the worker identity stays in the
    /// cursor, so a thread that later resumes does not re-emit its archived
    /// `session_meta` into the same generation.
    ///
    /// Fails for: routing the regression through generation replacement (the
    /// pre-fix defect: the surviving remote session re-emits its metadata
    /// under the new namespace — the e2e's 2 → 3), failing the scan on a
    /// ceiling-covered regression (the noop becomes a durable error
    /// checkpoint), or pruning `worker_threads` on episode disappearance
    /// instead of the session census.
    #[tokio::test]
    async fn an_episode_deletion_with_a_live_session_is_a_silent_noop() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-episode-deletion-noop".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let checkpoint_before = checkpoints
            .read()
            .await
            .get(&cp_key)
            .expect("cold checkpoint")
            .clone();

        {
            let connection = Connection::open(&path).expect("open fixture for episode delete");
            connection
                .execute(
                    "DELETE FROM episodes WHERE session_id = 'session-remote'",
                    [],
                )
                .expect("delete remote episodes only");
        }

        let (batches, transitions) =
            drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(
            transitions.is_empty() && batches.is_empty(),
            "an episode deletion under a live session changes nothing durable; \
             got {} batches, {} transitions",
            batches.len(),
            transitions.len()
        );
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("checkpoint");
        assert_eq!(
            checkpoint.cursor_json, checkpoint_before.cursor_json,
            "the cursor is byte-identical: watermark stood, worker identity \
             retained, session cursors carried"
        );
        let state = NacState::parse(&checkpoint.cursor_json);
        assert!(
            state
                .worker_threads
                .iter()
                .any(|key| key.starts_with("session-remote\n")),
            "the worker identity survives so a resumed thread does not \
             re-emit its archived session_meta"
        );

        cleanup_fixture(&path);
    }

    /// The fail-closed floor of the deletion/rewind split: a store whose
    /// `episodes` table has no AUTOINCREMENT never proves a deletion — ids
    /// are reusable there, so a vanished tail is always a potential rewind
    /// and must keep replacing. The ceiling helper must read 0 — not error —
    /// when the `sqlite_sequence` row, or the whole `sqlite_sequence` table,
    /// is absent.
    ///
    /// Fails for: querying `sqlite_sequence` without the existence guard
    /// (every regression poll on a plain-rowid store errors), or defaulting
    /// a missing sequence to the watermark instead of 0.
    #[test]
    fn the_episode_id_ceiling_is_zero_without_autoincrement() {
        let plain = Connection::open_in_memory().expect("open plain store");
        plain
            .execute_batch(
                "CREATE TABLE episodes (id INTEGER PRIMARY KEY, content TEXT); \
                 INSERT INTO episodes (id, content) VALUES (7, 'reusable');",
            )
            .expect("seed plain store");
        assert_eq!(
            nac_episode_id_ceiling(&plain).expect("ceiling without sqlite_sequence"),
            0,
            "no sqlite_sequence table: ids are reusable, nothing is provable"
        );

        let foreign = Connection::open_in_memory().expect("open foreign-sequence store");
        foreign
            .execute_batch(
                "CREATE TABLE episodes (id INTEGER PRIMARY KEY, content TEXT); \
                 CREATE TABLE other (id INTEGER PRIMARY KEY AUTOINCREMENT); \
                 INSERT INTO other DEFAULT VALUES;",
            )
            .expect("seed foreign-sequence store");
        assert_eq!(
            nac_episode_id_ceiling(&foreign).expect("ceiling without an episodes row"),
            0,
            "a sqlite_sequence row for another table proves nothing about episodes"
        );

        let path = fixture_db();
        let fixture = Connection::open(&path).expect("open fixture");
        assert_eq!(
            nac_episode_id_ceiling(&fixture).expect("fixture ceiling"),
            3,
            "the AUTOINCREMENT fixture's ceiling covers every allocated id"
        );
        drop(fixture);
        cleanup_fixture(&path);
    }

    /// Gate G5c (§8), the reason the NAC sweep exists: a session mutated
    /// **without** touching `updated_at` is invisible to the keyset fast path
    /// — no trigger maintains the column (§1.3), so nothing announces the
    /// write — and only a sweep slice can find it, within the published
    /// interval.
    ///
    /// **[DIVERGENT FIXTURE]** the mutation is written with
    /// `UPDATE sessions SET messages_json = … ` and no `updated_at` change; a
    /// fixture that bumps `updated_at` is caught by the fast path and the
    /// guard cannot fail. The intermediate watcher poll proves the divergence
    /// (the fast path really cannot see it) **and** noop-covers the stat, so
    /// the reconcile poll also exercises the sweep's override of the
    /// stat-covered volatile skip — without the override the mutation is
    /// unreachable forever on a store that then goes quiet.
    ///
    /// Fails for: a fast path that silently re-reads everything (the
    /// watcher-poll assertion), sweep eligibility keyed on anything that never
    /// fires here, dropping the noop-cover override, or a slice that does not
    /// re-verify content against the carried cursor.
    #[tokio::test]
    async fn nac_sweep_detects_a_session_whose_updated_at_did_not_move() {
        let path = fixture_db();
        {
            // DELETE journal mode, so the watcher poll's noop really covers a
            // frozen stat and the reconcile poll genuinely depends on the
            // sweep's volatile-skip override — under WAL, `-shm` churn from
            // the scan's own read-only open keeps the stat moving and the
            // override arm is never reached (the mutation that drops it
            // would survive).
            let connection = Connection::open(&path).expect("open fixture for journal change");
            connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .expect("checkpoint fixture WAL");
            connection
                .query_row("PRAGMA journal_mode = DELETE", [], |_| Ok(()))
                .expect("switch fixture to DELETE journal mode");
        }
        let source_file = path.to_string_lossy().to_string();
        let watcher = WorkItem {
            source_name: "nac-sweep-g5c".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let config = AppConfig::default();

        let (cold, _) =
            drive_nac_poll_with_metrics(&config, &watcher, &checkpoints, &poll_state, &metrics)
                .await;
        assert!(!cold.is_empty(), "the cold poll emits the fixture");

        // The silent mutation: content changes, `updated_at` does not.
        {
            let connection = Connection::open(&path).expect("open fixture for silent mutation");
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1 WHERE session_id = 'session-remote'",
                    params![
                        "[{\"role\":\"user\",\"content\":\"remote question\"},\
                         {\"role\":\"assistant\",\"content\":\"silently rewritten answer\"}]"
                    ],
                )
                .expect("mutate messages without updated_at");
        }

        // The fast path cannot see it: the watcher poll scans (the stat
        // moved) and emits nothing, noop-covering the stat.
        let (blind, _) =
            drive_nac_poll_with_metrics(&config, &watcher, &checkpoints, &poll_state, &metrics)
                .await;
        let blind_rows: usize = blind.iter().map(|batch| batch.raw_rows.len()).sum();
        assert_eq!(
            blind_rows, 0,
            "an updated_at-silent mutation must be invisible to the fast path, \
             or this fixture does not diverge"
        );

        // The reconcile poll's sweep slice finds it — through the noop cover.
        let (swept, _) =
            drive_nac_poll_with_metrics(&config, &reconcile, &checkpoints, &poll_state, &metrics)
                .await;
        let serialized: Vec<String> = swept
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .filter_map(|row| row.get("raw_json").and_then(Value::as_str))
            .map(str::to_string)
            .collect();
        assert!(
            serialized
                .iter()
                .any(|raw| raw.contains("silently rewritten answer")),
            "the sweep slice must detect and re-emit the silent mutation; got {serialized:?}"
        );
        assert!(
            metrics
                .sqlite_sweep_slices_total
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "and the detection must be attributable to a committed sweep slice"
        );

        cleanup_fixture(&path);
    }

    /// The upper bound of the sweep's volatile-skip override: a due slice may
    /// ride over a *noop-covered stat*, never over a **failure backoff** — a
    /// store whose scan is failing is not scanned harder because a sweep is
    /// due. The G5c test above is the override's lower bound.
    ///
    /// Fails for: an override that reads the noop cover only (dropping the
    /// `failure_retry_due` conjunct), or one that bypasses `should_skip_poll`
    /// wholesale.
    #[tokio::test]
    async fn a_due_sweep_does_not_override_a_failure_backoff() {
        let path = std::env::temp_dir().join(format!(
            "moraine-nac-sweep-backoff-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        {
            // `sessions` is missing: every scan fails schema validation.
            let connection = Connection::open(&path).expect("create broken store");
            connection
                .execute_batch("CREATE TABLE episodes (id INTEGER PRIMARY KEY);")
                .expect("create partial schema");
        }
        let source_file = path.to_string_lossy().to_string();
        let reconcile = WorkItem {
            source_name: "nac-sweep-backoff".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Reconcile,
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let mut config = AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;

        drive_nac_poll_with_metrics(&config, &reconcile, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            metrics
                .sqlite_scan_failures_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the first poll must reach the failing scan"
        );

        // Far inside the 15 s failure window, a due sweep must not force a
        // second scan of the failing store.
        drive_nac_poll_with_metrics(&config, &reconcile, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            metrics
                .sqlite_scan_failures_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "a due sweep must never override a failure backoff"
        );

        cleanup_fixture(&path);
    }

    /// The sweep's termination bound on a quiet store, both sides at once
    /// (the mid-cycle short-circuit bypass in `process_nac_sqlite_db`):
    ///
    /// - **starvation side**: with a 1-row slice budget the cycle spans
    ///   several polls, and a durably covered stat must not strand the
    ///   in-progress cycle — every due reconcile poll advances it until the
    ///   wrap;
    /// - **runaway side**: once the cycle wraps and the store stays quiet,
    ///   reconcile polls go fully idle — no scans, no slices, no checkpoint
    ///   churn — even with the interval at zero.
    ///
    /// Fails for: dropping the mid-cycle bypass conjunct (the cycle stalls
    /// mid-keyspace forever), or widening it past `cursor.is_empty()` (the
    /// store sweeps forever and never idles).
    #[tokio::test]
    async fn a_quiet_nac_store_finishes_its_sweep_cycle_and_then_goes_idle() {
        let path = fixture_db();
        {
            // DELETE journal mode, so "quiet" really means an unchanged stat
            // (see `a_degraded_nac_cold_ingest_completes_without_new_writes`).
            let connection = Connection::open(&path).expect("open fixture for journal change");
            connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .expect("checkpoint fixture WAL");
            connection
                .query_row("PRAGMA journal_mode = DELETE", [], |_| Ok(()))
                .expect("switch fixture to DELETE journal mode");
        }
        let source_file = path.to_string_lossy().to_string();
        let reconcile = WorkItem {
            source_name: "nac-sweep-idle".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Reconcile,
        };
        let cp_key = checkpoint_key(&reconcile.source_name, &reconcile.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let mut config = AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;
        config.ingest.sqlite.sweep_slice_max_payload_rows = 1;

        // Cold poll emits and, being reconcile-triggered and quiet-budgeted,
        // may already start the cycle; drive until the cycle completes.
        let mut polls = 0;
        loop {
            drive_nac_poll_with_metrics(&config, &reconcile, &checkpoints, &poll_state, &metrics)
                .await;
            polls += 1;
            let map = checkpoints.read().await;
            let state = NacState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
            if state.sweep.completed_cycles >= 1 && state.sweep.cursor.is_empty() {
                break;
            }
            assert!(
                polls <= 10,
                "a 1-row slice against 2 sessions must complete its cycle; \
                 still mid-cycle after {polls} polls"
            );
        }

        // Idle: the cycle is complete, the stat is covered, the cursor is
        // empty — further reconcile polls must not scan or sweep at all.
        let slices_before = metrics
            .sqlite_sweep_slices_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let rows_before = metrics
            .sqlite_poll_payload_rows_total
            .load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..3 {
            drive_nac_poll_with_metrics(&config, &reconcile, &checkpoints, &poll_state, &metrics)
                .await;
        }
        assert_eq!(
            metrics
                .sqlite_sweep_slices_total
                .load(std::sync::atomic::Ordering::Relaxed),
            slices_before,
            "a completed cycle over a quiet store must not restart"
        );
        assert_eq!(
            metrics
                .sqlite_poll_payload_rows_total
                .load(std::sync::atomic::Ordering::Relaxed),
            rows_before,
            "idle means no reads at all"
        );

        cleanup_fixture(&path);
    }

    /// `sweep_baseline`'s cycle-start conjunct (§2.2/D14): only a slice that
    /// *starts* a cycle stamps the baseline. A mid-cycle slice that
    /// re-stamped it would advance the baseline past a silent write that
    /// landed **behind** the sweep cursor during the cycle, erasing the owed
    /// follow-up cycle — on a store that then goes quiet, the mutation is
    /// never swept (D14's emitting-deferral scenario, a §0 coverage loss).
    ///
    /// The fixture: a 1-row slice budget spans the cycle across polls; after
    /// the first slice commits (cursor at `session-local`), `session-local`
    /// is silently rewritten — behind the cursor, `updated_at` untouched —
    /// and the store goes quiet. The wrapping cycle must leave a follow-up
    /// owed (the baseline, stamped at cycle start, trails the post-write
    /// stat), and that follow-up cycle must re-emit the rewrite. The other
    /// side of the bound — the baseline *does* catch up and the store then
    /// idles — is `a_quiet_nac_store_finishes_its_sweep_cycle_and_then_goes_idle`.
    ///
    /// MUTATION (executed 2026-07-31): drop the
    /// `prior.sweep.cursor.is_empty()` conjunct (`if swept && …` →
    /// `if swept`) — this test fails: the mid-cycle slice re-stamps the
    /// baseline to the post-write stat, the wrapped cycle closes the idle
    /// conjunct, and the rewrite never re-emits.
    #[tokio::test]
    async fn a_silent_write_behind_the_sweep_cursor_owes_a_follow_up_cycle() {
        let path = fixture_db();
        {
            // DELETE journal mode, so "quiet after the write" really means an
            // unchanged stat (see the sibling sweep tests): under WAL, `-shm`
            // churn keeps every poll scanning and the baseline conjunct is
            // never what keeps the follow-up cycle alive.
            let connection = Connection::open(&path).expect("open fixture for journal change");
            connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .expect("checkpoint fixture WAL");
            connection
                .query_row("PRAGMA journal_mode = DELETE", [], |_| Ok(()))
                .expect("switch fixture to DELETE journal mode");
        }
        let source_file = path.to_string_lossy().to_string();
        let reconcile = WorkItem {
            source_name: "nac-sweep-baseline".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Reconcile,
        };
        let cp_key = checkpoint_key(&reconcile.source_name, &reconcile.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let mut config = AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;
        config.ingest.sqlite.sweep_slice_max_payload_rows = 1;

        // Drive to the mid-cycle point: the first 1-row slice has committed
        // and the cursor sits at the first session in `session_id` order.
        let mut polls = 0;
        loop {
            drive_nac_poll_with_metrics(&config, &reconcile, &checkpoints, &poll_state, &metrics)
                .await;
            polls += 1;
            let map = checkpoints.read().await;
            let state = NacState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
            if state.sweep.cursor == "session-local" {
                break;
            }
            assert!(
                polls <= 5,
                "the first 1-row slice must commit within a few polls; \
                 cursor still elsewhere after {polls}"
            );
        }

        // The silent write, behind the cursor: content changes, `updated_at`
        // does not, and the in-progress cycle has already passed this key.
        {
            let connection = Connection::open(&path).expect("open fixture for silent mutation");
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1 WHERE session_id = 'session-local'",
                    params![
                        "[{\"role\":\"user\",\"content\":\"hello\"},\
                         {\"role\":\"assistant\",\"content\":\"rewritten behind the sweep cursor\"}]"
                    ],
                )
                .expect("mutate messages without updated_at");
        }

        // Quiet from here on. The wrapping cycle cannot see the write; the
        // owed follow-up cycle must.
        let mut polls = 0;
        loop {
            let (batches, _) = drive_nac_poll_with_metrics(
                &config,
                &reconcile,
                &checkpoints,
                &poll_state,
                &metrics,
            )
            .await;
            polls += 1;
            let emitted = batches
                .iter()
                .flat_map(|batch| batch.raw_rows.iter())
                .filter_map(|row| row.get("raw_json").and_then(Value::as_str))
                .any(|raw| raw.contains("rewritten behind the sweep cursor"));
            if emitted {
                break;
            }
            assert!(
                polls <= 10,
                "a silent write behind the sweep cursor must be re-swept by \
                 the owed follow-up cycle; still unseen after {polls} polls"
            );
        }

        cleanup_fixture(&path);
    }

    /// Gate G1c (§8): tool request/result pairing under **delta emission**.
    /// `link_tool_responses` recomputes request UIDs from the emitted record
    /// set only (§6), so a delta scan where the request part is unchanged and
    /// only the response re-emits must still link — through the
    /// request-coordinate fields the response carries — and not silently fall
    /// back to nothing.
    ///
    /// **[DIVERGENT FIXTURE]** only the tool *response* content changes (plus
    /// the forward-dated `updated_at` that makes the session a candidate);
    /// the request part's hash is unchanged, so it must **not** re-emit — the
    /// linkage has to survive without the request in the batch.
    ///
    /// Fails for: dropping the `request_logical_id`/coordinate fallback from
    /// `link_tool_responses`, or a delta scan that re-emits everything (the
    /// request-absence assertion).
    #[test]
    fn nac_delta_scan_preserves_tool_request_result_pairing() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let (mut cold, cold_state, _) = scan_fixture(&path, &NacState::default());
        link_tool_responses(&mut cold, &source_file, 1);
        let cold_response_uid = cold
            .iter()
            .find(|record| record.record["type"] == "tool_response")
            .and_then(|record| record.record.get("request_event_uid"))
            .and_then(Value::as_str)
            .expect("the cold scan links its tool response")
            .to_string();

        {
            let connection = Connection::open(&path).expect("open fixture for response mutation");
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1, updated_at = ?2 \
                     WHERE session_id = 'session-local'",
                    params![
                        // Identical to the fixture except the tool response
                        // content, so only that part's hash moves.
                        "[{\"role\":\"system\",\"content\":\"system prompt\"},\
                          {\"role\":\"user\",\"content\":\"hello\"},\
                          {\"role\":\"assistant\",\"content\":null,\"reasoning_text\":\"inspect first\",\"reasoning_details\":{\"kind\":\"analysis\"},\"tool_calls\":[{\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"mcp__moraine__search\",\"arguments\":\"{\\\"query\\\":\\\"nac\\\"}\"}}]},\
                          {\"role\":\"tool\",\"tool_call_id\":\"call-1\",\"content\":\"{\\\"result\\\":\\\"changed\\\"}\"},\
                          {\"role\":\"assistant\",\"content\":\"done\"}]",
                        "2026-07-18 13:20:00.000000000"
                    ],
                )
                .expect("mutate tool response only");
        }

        let (mut delta, _, _) = scan_fixture(&path, &cold_state);
        link_tool_responses(&mut delta, &source_file, 1);
        assert!(
            !delta
                .iter()
                .any(|record| record.record["type"] == "tool_request"),
            "[DIVERGENT FIXTURE] the unchanged request must not re-emit, or \
             the fallback path is never exercised"
        );
        let response = delta
            .iter()
            .find(|record| record.record["type"] == "tool_response")
            .expect("the changed response re-emits");
        assert_eq!(
            response
                .record
                .get("request_event_uid")
                .and_then(Value::as_str),
            Some(cold_response_uid.as_str()),
            "the delta-emitted response must link to the same request UID the \
             cold scan linked"
        );

        cleanup_fixture(&path);
    }

    /// §3.2's format guard, both sides, at the scan:
    ///
    /// - **fail open**: an `updated_at` whose separator differs from the
    ///   watermark's sorts *below* it lexicographically (`'T'`-separated with
    ///   an earlier date, here) — a naive keyset comparison silently skips
    ///   it. The guard makes an incomparable value a candidate: the row is
    ///   read and its change emits on the fast path.
    /// - **the skip stands**: a *comparable* value at or before the watermark
    ///   is genuinely skipped — that is the fast path's entire point, and the
    ///   skip is what the sweep's coverage guarantee (§0) underwrites (G5c).
    ///
    /// Fails for: dropping `nac_updated_at_comparable` from the candidate
    /// predicate (the incomparable row is skipped), or inverting it into
    /// "read everything" (the comparable row's absence assertion).
    #[test]
    fn a_mixed_updated_at_format_is_read_not_skipped() {
        let path = fixture_db();
        let (_, cold_state, _) = scan_fixture(&path, &NacState::default());
        assert!(
            !cold_state.updated_at_high_water.is_empty(),
            "the covering scan establishes the watermark"
        );

        {
            let connection = Connection::open(&path).expect("open fixture for format drift");
            // `'T'`-separated and a day earlier: sorts lexicographically below
            // the space-separated watermark even though it is a fresh write.
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1, updated_at = ?2 \
                     WHERE session_id = 'session-local'",
                    params![
                        "[{\"role\":\"user\",\"content\":\"format drifted write\"}]",
                        "2026-07-17T00:00:00.000000000"
                    ],
                )
                .expect("write format-drifted updated_at");
            // A comparable value at the watermark boundary: content changes,
            // but the fast path may honestly skip it (the sweep owns it).
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1 \
                     WHERE session_id = 'session-remote'",
                    params!["[{\"role\":\"user\",\"content\":\"comparable but stale stamp\"}]"],
                )
                .expect("write comparable below-watermark mutation");
        }

        let (records, _, _) = scan_fixture(&path, &cold_state);
        let serialized: Vec<String> = records
            .iter()
            .map(|record| serde_json::to_string(&record.record).expect("serialize record"))
            .collect();
        assert!(
            serialized
                .iter()
                .any(|raw| raw.contains("format drifted write")),
            "an incomparable updated_at must be read, never ordered against \
             the watermark; got {serialized:?}"
        );
        assert!(
            !serialized
                .iter()
                .any(|raw| raw.contains("comparable but stale stamp")),
            "[DIVERGENT FIXTURE] a comparable value at or before the watermark \
             is the fast path's legitimate skip — if it emits here, the guard \
             was widened into re-reading everything"
        );

        cleanup_fixture(&path);
    }

    /// §2.3's "continue next poll" for the **changed tail**: a 1-row budget
    /// against two bumped sessions converges across resumed polls because the
    /// keyset watermark is a durable resume position — ascending tail order,
    /// watermark committed at the last fully processed position — and
    /// `pending_coverage` keeps the quiet store scanning until covered.
    ///
    /// Fails for: a watermark that jumps to the census maximum on a *bound*
    /// poll (the second session's change is skipped forever), a tail read in
    /// descending order (the same newest session re-reads on every poll and
    /// the older change starves), or `fast_path_bound` not reaching
    /// `pending_coverage`.
    #[test]
    fn a_bound_nac_changed_tail_resumes_from_its_watermark() {
        let path = fixture_db();
        let (_, cold_state, _) = scan_fixture(&path, &NacState::default());

        {
            let connection = Connection::open(&path).expect("open fixture for tail bumps");
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1, updated_at = ?2 \
                     WHERE session_id = 'session-local'",
                    params![
                        "[{\"role\":\"user\",\"content\":\"older tail change\"}]",
                        "2026-07-18 13:10:00.000000000"
                    ],
                )
                .expect("bump session-local");
            connection
                .execute(
                    "UPDATE sessions SET messages_json = ?1, updated_at = ?2 \
                     WHERE session_id = 'session-remote'",
                    params![
                        "[{\"role\":\"user\",\"content\":\"newer tail change\"}]",
                        "2026-07-18 13:20:00.000000000"
                    ],
                )
                .expect("bump session-remote");
        }

        let budget = ScanBudget {
            max_payload_rows: 1,
            max_payload_bytes: u64::MAX,
        };
        let (first_records, first_state, _, first_ledger) =
            scan_fixture_with_budget(&path, &cold_state, &budget, MAX_NAC_CHECKPOINT_BYTES);
        assert_eq!(first_ledger.payload_rows, 1, "one session per bound poll");
        assert!(
            serde_json::to_string(&first_records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records")
                .contains("older tail change"),
            "the tail is read oldest-first — the resume order"
        );
        assert!(
            first_state.pending_coverage,
            "a bound tail is a durable coverage debt"
        );
        assert_eq!(
            first_state.updated_at_high_water, "2026-07-18 13:10:00.000000000",
            "the watermark commits at the last fully processed tail position, \
             never the census maximum, on a bound poll"
        );

        let (second_records, second_state, _, second_ledger) =
            scan_fixture_with_budget(&path, &first_state, &budget, MAX_NAC_CHECKPOINT_BYTES);
        assert_eq!(second_ledger.payload_rows, 1);
        assert!(
            serde_json::to_string(&second_records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records")
                .contains("newer tail change"),
            "the resumed poll picks up exactly where the watermark stopped"
        );
        assert!(
            !second_state.pending_coverage,
            "full coverage clears the debt"
        );
        assert_eq!(
            second_state.updated_at_high_water, "2026-07-18 13:20:00.000000000",
            "the covering poll's watermark reaches the census maximum"
        );

        cleanup_fixture(&path);
    }

    /// `nac_keyset_after`'s session-id tiebreak, at both of its consumers
    /// (§3.2's same-millisecond race: second-precision stores make
    /// same-timestamp write bursts ordinary, and a strict `updated_at >`
    /// alone cannot split a timestamp class): three sessions are rewritten
    /// at ONE shared timestamp, and a 2-row bound poll must (a) advance the
    /// watermark through the tiebreak to the second-processed session — the
    /// `advance_updated_at_watermark` consumer — and (b) let the resumed
    /// poll classify the third session as strictly after `(T, second)` —
    /// the candidate-classification consumer — reading exactly it and
    /// nothing else (the at-watermark pair stays carried, which bounds the
    /// tiebreak from the other side).
    ///
    /// MUTATION (executed 2026-07-31): drop the
    /// `|| (updated_at == high_water && session_id > high_water_session)`
    /// disjunct from `nac_keyset_after` — fails at the bound poll's
    /// watermark (stuck at the first-processed session). Separately, inline
    /// a strict `updated_at`-only comparison at the candidate-classification
    /// call site alone — fails at the resumed poll's emission (the third
    /// session is classified carried; its change never emits) while the
    /// watermark assertions pass, so each consumer is pinned on its own.
    #[test]
    fn a_same_timestamp_nac_write_burst_resumes_through_the_session_id_tiebreak() {
        let path = fixture_db();
        {
            // A third session that sorts below the fixture pair, so the
            // shared-timestamp class spans three ids.
            let connection = Connection::open(&path).expect("open fixture for third session");
            connection
                .execute(
                    "INSERT INTO sessions (session_id, cwd, model, base_url, messages_json, \
                     created_at, updated_at) VALUES ('aa-first', '/workspace/aa', 'model-a', \
                     'https://api.example', '[{\"role\":\"user\",\"content\":\"aa cold\"}]', \
                     '2026-07-18 11:00:00.000000000', '2026-07-18 11:00:01.000000000')",
                    [],
                )
                .expect("insert third session");
        }
        let (_, cold_state, _) = scan_fixture(&path, &NacState::default());
        assert_eq!(
            cold_state.updated_at_high_water_session, "session-remote",
            "the covering scan's watermark sits at the census maximum"
        );

        let stamp = "2026-07-18 14:00:00.000000000";
        {
            let connection = Connection::open(&path).expect("open fixture for write burst");
            for (session_id, content) in [
                ("aa-first", "same stamp change aa"),
                ("session-local", "same stamp change local"),
                ("session-remote", "same stamp change remote"),
            ] {
                connection
                    .execute(
                        "UPDATE sessions SET messages_json = ?1, updated_at = ?2 \
                         WHERE session_id = ?3",
                        params![
                            format!("[{{\"role\":\"user\",\"content\":\"{content}\"}}]"),
                            stamp,
                            session_id
                        ],
                    )
                    .expect("apply same-timestamp write");
            }
        }

        let budget = ScanBudget {
            max_payload_rows: 2,
            max_payload_bytes: u64::MAX,
        };
        let (first_records, first_state, _, _) =
            scan_fixture_with_budget(&path, &cold_state, &budget, MAX_NAC_CHECKPOINT_BYTES);
        let serialized =
            serde_json::to_string(&first_records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records");
        assert!(
            serialized.contains("same stamp change aa")
                && serialized.contains("same stamp change local")
                && !serialized.contains("same stamp change remote"),
            "ascending keyset order inside one timestamp is session_id order"
        );
        assert_eq!(first_state.updated_at_high_water, stamp);
        assert_eq!(
            first_state.updated_at_high_water_session, "session-local",
            "the watermark must advance through the tiebreak to the second \
             processed session of the shared timestamp — a strict `>` on \
             `updated_at` alone strands it at the first"
        );
        assert!(first_state.pending_coverage);

        let (second_records, second_state, _, second_ledger) =
            scan_fixture_with_budget(&path, &first_state, &budget, MAX_NAC_CHECKPOINT_BYTES);
        let serialized =
            serde_json::to_string(&second_records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records");
        assert!(
            serialized.contains("same stamp change remote"),
            "the resumed poll must classify the third same-stamp session as \
             after the watermark through the tiebreak; got {serialized}"
        );
        assert_eq!(
            second_ledger.payload_rows, 1,
            "and exactly it: the at-watermark pair stays carried — the \
             tiebreak admits the strictly-after remainder, not the class"
        );
        assert!(
            !second_state.pending_coverage,
            "full coverage of the burst clears the debt"
        );

        cleanup_fixture(&path);
    }

    /// D6's "a genuinely new session is already in the first class", pinned
    /// at the `!known` disjunct of the candidate predicate: a session
    /// INSERTed with an `updated_at` at or below the watermark (clock skew,
    /// an imported or restored store) has no prior cursor, so nothing else
    /// admits it — the format guard finds it comparable and the keyset says
    /// "before the watermark". Without the disjunct it is neither read nor
    /// carried: every poll re-opens `pending_coverage` over it (standing
    /// rescan churn) while its content waits on sweep latency.
    ///
    /// MUTATION (executed 2026-07-31): drop `!known ||` from the candidate
    /// predicate — this test fails: the backdated session never emits, is
    /// absent from the carried set, and `pending_coverage` latches.
    #[test]
    fn a_new_session_backdated_below_the_watermark_is_read_first_class() {
        let path = fixture_db();
        let (_, cold_state, _) = scan_fixture(&path, &NacState::default());
        assert!(
            !cold_state.updated_at_high_water.is_empty(),
            "the covering scan establishes the watermark"
        );

        {
            let connection = Connection::open(&path).expect("open fixture for backdated insert");
            connection
                .execute(
                    "INSERT INTO sessions (session_id, cwd, model, base_url, messages_json, \
                     created_at, updated_at) VALUES ('zz-backdated', '/workspace/zz', 'model-z', \
                     'https://api.example', \
                     '[{\"role\":\"user\",\"content\":\"backdated new session\"}]', \
                     '2026-07-18 10:00:00.000000000', '2026-07-18 10:00:00.000000000')",
                    [],
                )
                .expect("insert backdated session");
        }

        let (records, state, _, ledger) = scan_fixture_with_ledger(&path, &cold_state);
        let serialized =
            serde_json::to_string(&records.iter().map(|r| &r.record).collect::<Vec<_>>())
                .expect("serialize records");
        assert!(
            serialized.contains("backdated new session"),
            "a never-read session below the watermark is cold debt and must \
             be read first-class, not deferred to sweep latency; got {serialized}"
        );
        assert_eq!(
            ledger.payload_rows, 1,
            "and it is the only read — the known pair stays carried, so the \
             disjunct admits exactly the never-read class"
        );
        assert!(state.sessions.contains_key("zz-backdated"));
        assert!(
            !state.pending_coverage,
            "a covered store owes nothing — the mutated predicate leaves \
             this latched forever"
        );

        cleanup_fixture(&path);
    }

    /// Plan §7.2 F1, NAC's `schema_fingerprint == checkpoint.schema_fingerprint`
    /// conjunct of `scan_is_noop`: a schema change with no row changes emits
    /// nothing, and only the fingerprint comparison keeps its checkpoint from
    /// being suppressed — leaving the drift re-discovered on every poll and
    /// never durably recorded.
    ///
    /// Fails for: dropping the schema conjunct from NAC's `scan_is_noop`.
    #[tokio::test]
    async fn a_nac_schema_change_with_no_row_changes_still_persists_its_checkpoint() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-schema-conjunct".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let fingerprint_before = checkpoints
            .read()
            .await
            .get(&cp_key)
            .expect("cold checkpoint")
            .schema_fingerprint;

        {
            let connection = Connection::open(&path).expect("open fixture for schema change");
            connection
                .execute("ALTER TABLE episodes ADD COLUMN extra text", [])
                .expect("add drifting column");
        }
        let (batches, _) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
        assert_eq!(raw_rows, 0, "a schema change alone emits nothing");
        let fingerprint_after = checkpoints
            .read()
            .await
            .get(&cp_key)
            .expect("post-drift checkpoint")
            .schema_fingerprint;
        assert_ne!(
            fingerprint_before, fingerprint_after,
            "the moved fingerprint must persist durably, not be suppressed as \
             a noop and re-discovered on every later poll"
        );

        cleanup_fixture(&path);
    }

    /// Plan §7.2 F3, the `schema_fingerprint != 0` conjunct of NAC's cheap
    /// short-circuit — the one of NAC's three extra conjuncts that is not an
    /// equivalent mutant. (`!starts_replacement` and `!retry_blocked_replay`
    /// are: a replay's `scan_state` is `NacState::default()`, whose default
    /// stat fingerprint can never equal a real file's, so the stat conjunct
    /// already fails — see the short-circuit's comment.) A cursor persisted
    /// without a fingerprint must be rescanned once even though its stat
    /// matches, or the fingerprint is never established and schema drift is
    /// undetectable forever.
    ///
    /// Fails for: dropping `schema_fingerprint != 0` from the cheap
    /// short-circuit.
    #[tokio::test]
    async fn a_legacy_cursor_without_a_schema_fingerprint_is_rescanned_once() {
        let path = fixture_db();
        {
            // DELETE journal mode, so the covered-stat conjunct genuinely
            // holds and the fingerprint conjunct is the one deciding — under
            // WAL, sidecar churn re-runs the scan for the wrong reason and
            // the dropped conjunct would survive its mutation.
            let connection = Connection::open(&path).expect("open fixture for journal change");
            connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .expect("checkpoint fixture WAL");
            connection
                .query_row("PRAGMA journal_mode = DELETE", [], |_| Ok(()))
                .expect("switch fixture to DELETE journal mode");
        }
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-legacy-fingerprint".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        // A legacy cursor: covered stat, no fingerprint anywhere.
        {
            let mut map = checkpoints.write().await;
            let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
            checkpoint.schema_fingerprint = 0;
            let mut state = NacState::parse(&checkpoint.cursor_json);
            state.schema_fingerprint = 0;
            checkpoint.cursor_json = state.serialize().expect("serialize legacy cursor");
        }

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("post-rescan checkpoint");
        assert_ne!(
            checkpoint.schema_fingerprint, 0,
            "the rescan must establish the fingerprint despite the covered \
             stat, or schema drift is undetectable forever"
        );

        cleanup_fixture(&path);
    }

    /// Gate G8c (§8): an episode **rewind** — `max(episodes.id)` regressing
    /// below the committed watermark *with the AUTOINCREMENT id ceiling
    /// rewound too*, so the vanished coordinates are re-allocatable — is a
    /// truncation of history and must route through #602's replacement path:
    /// generation bump, `BeginReplay` barrier, `sessions` reset and
    /// re-emitted. The pre-WI-07 code did an in-place
    /// `worker_threads.clear()` inside the live generation, exposing
    /// partially replayed worker history beside live data with no barrier,
    /// and never reset `sessions` at all. **This gate fails against that
    /// code** (confirmed: the plan requires the check before the fix).
    ///
    /// **[DIVERGENT FIXTURE]** the truncation resets `sqlite_sequence`
    /// alongside the rows — the shape a recreated table leaves behind.
    /// Deleting rows *without* touching the sequence is a deletion, which is
    /// archival and emits nothing
    /// (`a_session_deleted_from_the_store_emits_nothing_new`); a fixture
    /// that only deletes cannot reach the replacement path at all. This test
    /// is the upper bound of the deletion split: widening "deletion" to
    /// every max-id regression (dropping the ceiling conjunct's rewind arm)
    /// fails here.
    ///
    /// Fails for: restoring the in-place reset, a preflight that cannot see
    /// the rewind, or a deletion split so wide no rewind replaces anymore.
    #[tokio::test]
    async fn nac_episode_rewind_routes_through_generation_replay() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-episode-rewind".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let generation_before = checkpoints
            .read()
            .await
            .get(&cp_key)
            .expect("cold checkpoint")
            .source_generation;

        // Truncate episode history: max(id) regresses below the watermark
        // AND the AUTOINCREMENT sequence rewinds with it, so ids 2 and 3 are
        // re-allocatable — the recreated-table shape, a true rewind rather
        // than a deletion.
        {
            let connection = Connection::open(&path).expect("open fixture for truncation");
            connection
                .execute("DELETE FROM episodes WHERE id >= 2", [])
                .expect("truncate episodes");
            connection
                .execute(
                    "UPDATE sqlite_sequence SET seq = 1 WHERE name = 'episodes'",
                    [],
                )
                .expect("rewind the episode id sequence");
        }

        let (batches, transitions) =
            drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(
            transitions
                .iter()
                .any(|transition| transition.checkpoint.status == "replaying"),
            "the rewind must raise a BeginReplay barrier, not reset in place"
        );
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("replaced checkpoint");
        assert_eq!(
            checkpoint.source_generation,
            generation_before + 1,
            "the rewind must bump the generation exactly once"
        );
        assert_eq!(checkpoint.status, "active", "the replay finalizes");
        let state = NacState::parse(&checkpoint.cursor_json);
        assert_eq!(
            state.sessions.len(),
            2,
            "`sessions` is reset and rebuilt in the new generation — the old \
             in-place path never reset it"
        );
        assert_eq!(state.episode_high_water, 1, "the surviving episode");
        let raw_rows: usize = batches.iter().map(|batch| batch.raw_rows.len()).sum();
        assert!(
            raw_rows > 0,
            "the replacement generation re-emits the store"
        );

        cleanup_fixture(&path);
    }

    /// The NAC halves of the OpenCode blocked-replay throttle-bypass width
    /// (§2.5): WI-07 moved NAC's throttle above the generation bump (the
    /// episode preflight must not run for a throttled store), which makes the
    /// two bypass conjuncts load-bearing exactly as at the OpenCode site — a
    /// replaced database must never wait out a stale backoff.
    ///
    /// Fails for: dropping `!generation_changed` from the throttle gate.
    #[tokio::test]
    async fn a_replaced_nac_database_bypasses_the_blocked_replay_throttle() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-replaced-bypass".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;

        // Durably block the source and arm its failure backoff.
        {
            let mut map = checkpoints.write().await;
            let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
            checkpoint.status = "error".to_string();
            checkpoint.block_reason = "blocked for the bypass test".to_string();
        }
        poll_state.record_blocked_replay(&cp_key, 1);

        // Replace the database file wholesale: new inode, same path.
        let replacement = fixture_db();
        std::fs::rename(&replacement, &path).expect("replace database file");

        let (_, transitions) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        assert!(
            transitions
                .iter()
                .any(|transition| transition.checkpoint.status == "replaying"),
            "a replaced database must bypass the blocked-replay throttle and \
             start its replacement replay immediately"
        );
        let map = checkpoints.read().await;
        assert_eq!(
            map.get(&cp_key).expect("replacement checkpoint").status,
            "active",
            "the replacement finalizes despite the armed backoff"
        );

        cleanup_fixture(&path);
    }

    /// The exclusion half of the same width: see
    /// `a_replaced_nac_database_bypasses_the_blocked_replay_throttle`.
    ///
    /// Fails for: dropping `!exclusions_changed` from the throttle gate.
    #[tokio::test]
    async fn an_exclusion_change_bypasses_the_nac_blocked_replay_throttle() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-exclusion-bypass".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let config = AppConfig::default();

        drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        {
            let mut map = checkpoints.write().await;
            let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
            checkpoint.status = "error".to_string();
            checkpoint.block_reason = "blocked for the bypass test".to_string();
        }
        poll_state.record_blocked_replay(&cp_key, 1);

        let mut changed = AppConfig::default();
        changed.ingest.exclude_project_dirs = vec!["/nowhere/at/all/**".to_string()];
        let (_, transitions) = drive_nac_poll(&changed, &work, &checkpoints, &poll_state).await;
        assert!(
            transitions
                .iter()
                .any(|transition| transition.checkpoint.status == "replaying"),
            "an exclusion change must bypass the blocked-replay throttle"
        );

        cleanup_fixture(&path);
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
            trigger: WorkTrigger::Watcher,
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let mut excluded = AppConfig::default();
        excluded.ingest.exclude_project_dirs = vec!["/workspace/local/**".to_string()];

        let (first, _) = drive_nac_poll(&excluded, &work, &checkpoints, &poll_state).await;
        let first_batch = first.last().expect("first NAC batch");
        assert_eq!(first_batch.raw_rows.len(), 6);

        let included = AppConfig::default();
        let (replayed, transitions) =
            drive_nac_poll(&included, &work, &checkpoints, &poll_state).await;
        assert_eq!(transitions.len(), 2, "begin and final replay barriers");
        let replayed_batch = replayed.last().expect("replayed NAC batch");
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
                &default_nac_budget(),
                None,
                MAX_NAC_CHECKPOINT_BYTES,
                &mut ScanLedger::default(),
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
    async fn failed_replacement_durably_blocks_the_candidate_generation() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-fixture".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let config = AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let (initial, _) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let checkpoint = initial
            .last()
            .and_then(|batch| batch.checkpoint.as_ref())
            .expect("initial checkpoint");
        let committed_generation = checkpoint.source_generation;

        let replacement = path.with_extension("replacement");
        std::fs::write(&replacement, b"not a sqlite database").expect("write invalid replacement");
        std::fs::remove_file(&path).expect("remove fixture database");
        std::fs::rename(&replacement, &path).expect("install invalid replacement");
        let (failed, transitions) = drive_nac_poll(&config, &work, &checkpoints, &poll_state).await;
        let failed_batch = failed.last().expect("failed NAC batch");
        assert_eq!(failed_batch.error_rows.len(), 1);
        assert_eq!(
            failed_batch.error_rows[0]["source_generation"],
            committed_generation.saturating_add(1)
        );
        assert!(
            failed_batch.checkpoint.is_none(),
            "an error batch cannot make a replacement generation active"
        );
        assert_eq!(transitions.len(), 2, "begin then durable block");
        let blocked = transitions.last().expect("blocked transition");
        assert_eq!(
            blocked.checkpoint.lifecycle().unwrap(),
            crate::model::CheckpointLifecycle::Error
        );
        assert_eq!(
            blocked.checkpoint.source_generation,
            committed_generation + 1
        );
        assert!(!blocked.checkpoint.block_reason.is_empty());

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.display())).ok();
        std::fs::remove_file(format!("{}-shm", path.display())).ok();
    }

    /// Issue #601 §2.5, the NAC-specific aggravation: the volatile check was
    /// skipped entirely during replay (`!replacement_replay && should_skip_poll`),
    /// so a store stuck in `retry_blocked_replay` re-scanned the whole database
    /// **and re-sent a durable `BeginReplay` barrier** on every tick, forever.
    ///
    /// The throttle has to sit *before* the barrier: gating only the scan would
    /// leave a barrier with nothing behind it, which is the failure mode
    /// §2.1(2) warns about.
    ///
    /// Denominated on durable transitions emitted and on observed scans — not
    /// on `ingest_errors` rows, which the `last_error` marker already
    /// suppresses.
    #[tokio::test]
    async fn blocked_replay_backs_off_instead_of_resending_the_barrier() {
        let path = fixture_db();
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "nac-fixture".to_string(),
            harness: "nac".to_string(),
            format: moraine_config::SourceFormat::NacSqlite,
            source_glob: source_file.clone(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };
        let config = AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        // The cold poll's ledger must reach the shared counters: `Metrics` is
        // how every observation below is denominated, and a `record_scan_ledger`
        // that folds nothing would make all of them unfailable.
        assert!(
            metrics
                .sqlite_poll_payload_rows_total
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "the NAC scan's ledger must be folded into the shared metrics"
        );

        // Replace the store in place so the next poll starts a replacement
        // replay that cannot succeed, leaving the checkpoint durably blocked.
        let replacement = path.with_extension("replacement");
        std::fs::write(&replacement, b"not a sqlite database").expect("write invalid replacement");
        std::fs::remove_file(&path).expect("remove fixture database");
        std::fs::rename(&replacement, &path).expect("install invalid replacement");
        let (_, blocking) =
            drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(blocking.len(), 2, "begin then durable block");
        let failures_after_block = metrics
            .sqlite_scan_failures_total
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(failures_after_block, 1);

        let cp_key = checkpoint_key(&work.source_name, &work.path);
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            1,
            "the block starts the failure ladder"
        );

        let scan_failures = || {
            metrics
                .sqlite_scan_failures_total
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        // Ten ticks inside the first 15 s window: nothing runs.
        let mut retry_transitions = 0usize;
        for _ in 0..10 {
            let (_, transitions) =
                drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics)
                    .await;
            retry_transitions += transitions.len();
        }
        assert_eq!(
            retry_transitions, 0,
            "a durably blocked replay must not re-send BeginReplay on every tick"
        );
        assert_eq!(
            scan_failures(),
            failures_after_block,
            "and must not re-run the failing scan either"
        );

        // Ten ticks inside one window cannot distinguish a flat 15 s floor from
        // an exponential ladder, which is what let the floor-pinning reset ship.
        // Backdating the clock does: after the first retry the window is 30 s,
        // so a further 16 s must **not** be enough.
        poll_state.age_for_tests(&cp_key, std::time::Duration::from_secs(16));
        let (_, retried) =
            drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(retried.len(), 2, "the due retry re-sends begin then block");
        assert_eq!(scan_failures(), failures_after_block + 1);
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            2,
            "a repeat failure extends the ladder"
        );

        poll_state.age_for_tests(&cp_key, std::time::Duration::from_secs(16));
        let (_, throttled) =
            drive_nac_poll_with_metrics(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert!(
            throttled.is_empty(),
            "the second window is 30 s; 16 s of elapsed time is not yet due"
        );
        assert_eq!(scan_failures(), failures_after_block + 1);

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
            trigger: WorkTrigger::Watcher,
        };
        // **Known hazard, gate G1d (§3.2).** The mixed-snapshot bracket's second
        // disjunct compares full `StatFingerprint`s including `shm_len` /
        // `shm_mtime_ns`, so it can reject a scan when `data_version` is
        // unchanged — no commit happened at all, only sidecar churn. Under a
        // loaded host (running the whole suite in parallel, or alongside the
        // Cursor mixed-snapshot reproducer) this poll therefore sometimes
        // reports `sqlite_mixed_snapshot` instead of the ceiling it is about.
        // That is not noise and it is not this test's bug: it is exactly the
        // rejection G1d exists to characterize.
        //
        // A mixed-snapshot rejection means "retry without advancing the
        // cursor", so the test does what production does — retries — and
        // requires the ceiling to be reported within a bounded number of
        // attempts. Attributing it this way keeps the assertion sharp instead
        // of loosening it to "either error kind is fine".
        let mut rejections = 0usize;
        let batch = loop {
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
            let SinkMessage::Batch(batch) = sink_rx.recv().await.expect("oversized failure batch")
            else {
                panic!("expected oversized failure batch");
            };
            if batch.error_rows.first().map(|row| &row["error_kind"])
                == Some(&Value::from(ERROR_KIND_MIXED_SNAPSHOT))
            {
                rejections += 1;
                assert!(
                    rejections < 20,
                    "20 consecutive mixed-snapshot rejections against a database \
                     nothing is writing is the G1d hazard escalating, not a flake"
                );
                continue;
            }
            break batch;
        };
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
                    // Later than every fixture `updated_at`, matching the
                    // writer's monotonic wall clock: the §3.2 fast path skips
                    // values at or before the keyset watermark, so a backdated
                    // bump is the *sweep's* case (G5c), not this test's.
                    "2026-07-18 13:10:00.000000000"
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
