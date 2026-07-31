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
use crate::{Metrics, SinkMessage, WorkItem, WorkTrigger};
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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

/// Ceiling on the serialized Cursor cursor payload (issue #601 §2.3), the
/// successor to `MAX_RELEVANT_KEYS`' hard latch. Enforced by **eviction** of
/// the oldest kv hashes, never by failing the scan: an evicted key re-detects
/// as never-read and is re-read (and re-emitted) by a later poll, which is
/// safe because Cursor records are content-addressed at stable logical
/// coordinates (§6). The bound matters beyond memory: `cursor_json` is
/// persisted on every persisting poll and hashed into the #602 transition
/// digest (§2.6), so without it the payload grows with the keyspace up to
/// `fast_path_max_census_rows` entries — tens of MB at the 250 k default.
const MAX_CURSOR_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;

const ERROR_KIND_OPEN: &str = "sqlite_open_error";
const ERROR_KIND_SCHEMA: &str = "sqlite_schema_mismatch";
/// Retired from every scan path by issue #601 §2.3 (WI-05 for Cursor and NAC,
/// WI-06 for OpenCode — the last producer): history size is now a degradation
/// (`coverage_degraded`), never a failure. The constant remains, test-only,
/// because `record_scan_failure_outcome`'s routing width is pinned per kind
/// (`each_error_kind_routes_to_exactly_one_backoff_clock`) and because
/// historical `ingest_errors` rows carry it.
#[cfg(test)]
const ERROR_KIND_TOO_LARGE: &str = "sqlite_cursor_too_large";
/// One genuinely un-processable single row (issue #601 §2.3): larger than
/// `SCAN_PAGE_MAX_BYTES`. Reported as one `ingest_errors` row for that row
/// alone; the scan skips it and advances past it — never fails.
pub(crate) const ERROR_KIND_ROW_TOO_LARGE: &str = "sqlite_row_too_large";
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

impl StatFingerprint {
    /// For `skip_serializing_if`: keeps `cursor_json` byte-identical for
    /// states that never set an optional fingerprint field (§2.6).
    pub(crate) fn is_default(&self) -> bool {
        self == &Self::default()
    }
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
    /// Durable reconciliation-sweep cursor (issue #601 §2.2). Additive to the
    /// payload — skipped while default, so `cursor_json` stays byte-identical
    /// for sources that have never swept, and structural-deterministic, so it
    /// rides the #602 transition digest safely (§2.6).
    #[serde(default, skip_serializing_if = "SweepState::is_default")]
    sweep: SweepState,
    /// True while some censused key has never been read in this generation
    /// (a budget/census-cap remainder) — §2.3's persisted resume marker. The
    /// cheap stat short-circuit must not fire while this is set, or a quiet
    /// database's cold-ingest remainder is hidden forever: nothing will move
    /// the stat, so nothing would ever scan again. Structural and
    /// deterministic (a function of the committed census vs the committed
    /// hash map), and skipped while false so `cursor_json` stays
    /// byte-identical for fully-covered sources (§2.6).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pending_coverage: bool,
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

    /// Evict the oldest kv hashes until the serialized payload fits
    /// `max_bytes`, returning how many were dropped (issue #601 §2.3). The
    /// mirror of `NacState::evict_to_fit`: the ceiling degrades — an evicted
    /// key re-detects as never-read, so a later poll re-reads and re-emits it
    /// (safe under §6's content-addressed identity) — instead of latching the
    /// whole database the way `MAX_RELEVANT_KEYS` did.
    ///
    /// "Oldest" is §2.3's adapter recency ordering, `rowid`, as this poll's
    /// census saw it: entries the census did not cover evict first (their age
    /// is unknowable here, and a truncated census's uncovered tail is where
    /// carried-forward entries come from), then ascending `rowid`, then key
    /// for determinism. The persisted state carries no rowid until WI-08's
    /// `KvEntry { hash, rowid }`, which owns replacing this census-scoped
    /// view with a durable one. Removal is in batches of one-eighth of the
    /// map per round, so a pathological many-small-entries payload does not
    /// re-serialize per entry.
    fn evict_to_fit(&mut self, max_bytes: usize, census: &[CensusRow]) -> u64 {
        // As in `NacState::evict_to_fit`: measured through `serde_json`
        // directly, because `self.serialize()` on a `&mut self` receiver
        // resolves to serde's blanket `impl Serialize for &mut T` before the
        // inherent method.
        let raw_len = |state: &Self| {
            serde_json::to_string(state)
                .map(|raw| raw.len())
                .unwrap_or(0)
        };
        let mut evicted = 0u64;
        if raw_len(self) <= max_bytes {
            return evicted;
        }
        let rowid_by_key: HashMap<&str, i64> = census
            .iter()
            .map(|row| (row.key.as_str(), row.rowid))
            .collect();
        loop {
            if raw_len(self) <= max_bytes || self.kv_hashes.is_empty() {
                return evicted;
            }
            // `None` sorts before `Some`, so un-censused entries evict first.
            let mut by_age: Vec<(Option<i64>, String)> = self
                .kv_hashes
                .keys()
                .map(|key| (rowid_by_key.get(key.as_str()).copied(), key.clone()))
                .collect();
            by_age.sort();
            let batch = (self.kv_hashes.len().div_ceil(8)).max(1);
            for (_, key) in by_age.into_iter().take(batch) {
                self.kv_hashes.remove(&key);
                evicted += 1;
            }
        }
    }
}

/// Test-only injection point for the mixed-snapshot bracket.
///
/// The bracket only trips when something commits inside the scan's read
/// window, so the only way to observe that arm without an injection point is
/// to race a concurrent writer and hope — which made the single guard over
/// Cursor's one post-read failure arm nondeterministic, and made the *replay*
/// consequences of a contended scan untestable altogether.
///
/// Armings are keyed by database path and each `take` consumes one, so tests
/// running in parallel threads of the same binary (every path comes from
/// `unique_db_path`/`fixture_db`) cannot contaminate one another, and an
/// arming that is never consumed cannot leak into a later test on a different
/// database.
#[cfg(test)]
pub(crate) mod contention_injection {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    fn armed() -> &'static Mutex<HashMap<String, u32>> {
        static ARMED: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();
        ARMED.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Force the next `times` scans of `db_path` to fail the mixed-snapshot
    /// bracket, exactly as a writer committing mid-scan would.
    pub(crate) fn arm(db_path: &str, times: u32) {
        armed()
            .lock()
            .expect("contention injection mutex poisoned")
            .insert(db_path.to_string(), times);
    }

    /// Cancel any remaining armings for `db_path`.
    pub(crate) fn disarm(db_path: &str) {
        armed()
            .lock()
            .expect("contention injection mutex poisoned")
            .remove(db_path);
    }

    /// Consume one arming for `db_path`, if any.
    pub(crate) fn take(db_path: &str) -> bool {
        let mut map = armed().lock().expect("contention injection mutex poisoned");
        let Some(remaining) = map.get_mut(db_path) else {
            return false;
        };
        *remaining -= 1;
        if *remaining == 0 {
            map.remove(db_path);
        }
        true
    }
}

/// True when a test has armed `db_path` for a forced mixed-snapshot rejection.
/// Compiles to a constant `false` outside the test build.
#[cfg(test)]
fn forced_mixed_snapshot(db_path: &str) -> bool {
    contention_injection::take(db_path)
}

#[cfg(not(test))]
#[inline(always)]
fn forced_mixed_snapshot(_db_path: &str) -> bool {
    false
}

/// True when the scan that just finished cannot be trusted to have read one
/// point-in-time view of the database, so its results are discarded and the
/// cursor stays where it was.
///
/// All three adapters bracket their row scan with this, which is why it is a
/// function and not three copies of the same expression: the sites are
/// load-bearing for §3.2 and silent drift between them has no natural guard.
///
/// **Known hazard (gate G1d, §3.2).** The stat disjunct compares full
/// `StatFingerprint`s, `shm_len`/`shm_mtime_ns` included, so it can reject a
/// scan when `data_version` is unchanged — i.e. when nothing committed at all
/// and only a concurrent *reader* churned the `-shm` sidecar. That is
/// load-sensitive rather than theoretical: under a loaded host
/// `nac::tests::oversized_normalized_rows_fail_before_any_payload_is_sent` has
/// been observed failing with `sqlite_mixed_snapshot` in place of the error
/// kind it asserts. §3.2 requires the guard be preserved exactly, so it is;
/// narrowing it to the sidecars that imply a commit is G1d's job and needs the
/// sandbox (`/proc/self/io`) to be measured honestly.
///
/// **What holds this width, and what does not.** Every *production* rejection
/// arrives through one of the two real disjuncts and none through the hook —
/// but until this predicate had its own test, that was true of no test either.
/// A commit landing between a scan's two `data_version` reads is not
/// deterministically reachable: the scan runs inside a single function call,
/// and `forced_mixed_snapshot` is its only seam — a seam that short-circuits
/// both real disjuncts, so every adapter contention test reached the arm
/// without ever evaluating them. Racing a concurrent writer would reach them
/// but only probabilistically, and a second injection hook that performed a
/// real mid-scan commit would be more production surface than the guard it
/// checks. So the disjuncts are bounded here instead, separately and
/// deterministically, by
/// `the_mixed_snapshot_bracket_fires_on_a_moved_data_version_and_on_a_moved_stat`:
/// dropping either one, or widening the predicate to fire unconditionally,
/// fails it. The three *call sites* stay bounded by the adapters' contention
/// tests, which reach this through the hook.
fn snapshot_is_mixed(
    db_path: &str,
    data_version_before: i64,
    data_version_after: i64,
    opened_stat: StatFingerprint,
) -> bool {
    forced_mixed_snapshot(db_path)
        || data_version_before != data_version_after
        || stat_fingerprint(db_path) != Some(opened_stat)
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
    /// The stat fingerprint a completed no-op scan covered. `None` means no
    /// scan has covered a fingerprint yet — the state a failed scan leaves
    /// behind, and deliberately not a sentinel value that a real fingerprint
    /// could collide with.
    stat: Option<StatFingerprint>,
    consecutive_noop_scans: u32,
    consecutive_failed_scans: u32,
    last_scan_at: Instant,
    /// When the last mixed-snapshot rejection happened, and how many have run
    /// back to back. Kept **separate from `consecutive_failed_scans`** on
    /// purpose: contention is not a fault, so it must not climb the 15 s → 15
    /// min ladder or suppress ordinary scans of an active database. What it
    /// does have to do is throttle the *durable replay barrier*, which is why
    /// it exists at all — see `failure_retry_due`.
    last_contended_at: Option<Instant>,
    consecutive_contended_scans: u32,
    /// When this database last committed a sweep slice (issue #601 §2.2,
    /// eligibility condition 2). Volatile on purpose: losing it on restart
    /// costs at most one early slice per database, while persisting it would
    /// put a wall clock in `cursor_json` (the §2.6 trap).
    last_sweep_at: Option<Instant>,
}

/// Base delay after one failed scan, doubling per consecutive failure.
const FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(15);

/// Ceiling on the failure backoff: a permanently broken database still retries
/// every 15 minutes, so recovery from an environmental fault (a lock, a
/// permission) never needs a restart.
const FAILURE_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);

/// `min(15 s * 2^(n-1), 15 min)` for `n` consecutive failures; zero failures
/// means retry immediately.
fn failure_backoff(consecutive_failed_scans: u32) -> Duration {
    if consecutive_failed_scans == 0 {
        return Duration::ZERO;
    }
    let shift = consecutive_failed_scans.saturating_sub(1).min(31);
    FAILURE_BACKOFF_BASE
        .saturating_mul(1u32 << shift)
        .min(FAILURE_BACKOFF_MAX)
}

/// Base delay before a *contended* replay barrier is re-sent.
const CONTENTION_BACKOFF_BASE: Duration = Duration::from_secs(15);

/// Ceiling on the contention backoff. Far below `FAILURE_BACKOFF_MAX` on
/// purpose: contention is transient and self-clearing, and a replacement
/// database that is merely busy must become visible within a minute of the
/// writer pausing, not within fifteen.
const CONTENTION_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// `min(15 s * 2^(n-1), 60 s)` for `n` consecutive mixed-snapshot rejections.
fn contention_backoff(consecutive_contended_scans: u32) -> Duration {
    if consecutive_contended_scans == 0 {
        return Duration::ZERO;
    }
    let shift = consecutive_contended_scans.saturating_sub(1).min(31);
    CONTENTION_BACKOFF_BASE
        .saturating_mul(1u32 << shift)
        .min(CONTENTION_BACKOFF_MAX)
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

    /// True when a poll should be skipped without scanning: the last no-op
    /// scan already covered the current stat fingerprint (the durable
    /// checkpoint is intentionally stale after a no-op scan), the database is
    /// stat-noisy and still inside its rescan backoff window, or its last scan
    /// failed and the failure backoff has not expired.
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
        if entry.stat == Some(*current_stat) {
            return true;
        }
        // The failure arm is independent of the no-op streak: a database whose
        // first scan fails has no streak to throttle on, which is exactly the
        // case that re-ran a full failed scan on every tick (issue #601 §2.5).
        if entry.last_scan_at.elapsed() < failure_backoff(entry.consecutive_failed_scans) {
            return true;
        }
        entry.consecutive_noop_scans >= NOOP_SCAN_BACKOFF_THRESHOLD
            && entry.last_scan_at.elapsed() < NOOP_RESCAN_MIN_INTERVAL
    }

    /// True when a database whose last scan did not succeed is due for another
    /// attempt. An absent entry (fresh process, or a scan that last succeeded)
    /// is always due, so a restart never inherits a suppression.
    ///
    /// This gates the **durable replay barrier**, which is why it consults the
    /// contention clock as well as the fault ladder. A mixed-snapshot rejection
    /// is exempt from the fault ladder (§3.2: contention is not a fault) but it
    /// must never be exempt from *this*: the `Failed` arm of a replacement
    /// replay emits `BlockReplay` plus an append-only `ingest_errors` row, and
    /// the next tick sees `retry_blocked_replay` with `starts_replacement`
    /// false. With no clock at all that is `BeginReplay` + a full cold scan +
    /// `BlockReplay` + one error row **per poll, forever** while contention
    /// persists — at the 30 s reconcile cadence *and* on every 50 ms-debounced
    /// watcher event. A replacement replay is the longest scan an adapter runs,
    /// so it is the scan most likely to lose the bracket in the first place.
    ///
    /// The contention clock deliberately does *not* appear in
    /// `should_skip_poll`: throttling ordinary scans of a contended database is
    /// exactly the active-session freshness regression the exemption exists to
    /// prevent.
    fn failure_retry_due(&self, cp_key: &str, source_generation: u32) -> bool {
        let map = self.lock();
        let Some(entry) = map.get(cp_key) else {
            return true;
        };
        if entry.source_generation != source_generation {
            return true;
        }
        if entry.last_scan_at.elapsed() < failure_backoff(entry.consecutive_failed_scans) {
            return false;
        }
        match entry.last_contended_at {
            Some(at) => at.elapsed() >= contention_backoff(entry.consecutive_contended_scans),
            None => true,
        }
    }

    /// Record a scan that changed nothing durable: remember the stat
    /// fingerprint it covered and extend the no-op streak. A no-op scan is a
    /// *successful* scan, so it also clears the failure streak while keeping
    /// the no-op streak.
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
                stat: Some(stat),
                consecutive_noop_scans: 0,
                consecutive_failed_scans: 0,
                last_scan_at: Instant::now(),
                last_contended_at: None,
                consecutive_contended_scans: 0,
                last_sweep_at: None,
            });
        entry.source_generation = source_generation;
        entry.stat = Some(stat);
        entry.consecutive_noop_scans = entry.consecutive_noop_scans.saturating_add(1);
        entry.consecutive_failed_scans = 0;
        // A scan that completed cleanly is proof the contention has passed, so
        // the **ladder** resets and not just its clock. Nothing observes the
        // reset until the next rejection — `consecutive_contended_scans` is
        // read only inside `failure_retry_due`'s `Some(last_contended_at)` arm
        // — so a stale streak left here is silent right up to the moment
        // `record_contended_scan` increments from it, and the barrier throttle
        // then resumes near the 60 s ceiling instead of at the 15 s base.
        // `a_clean_scan_resets_the_contention_ladder_not_only_its_clock` fails
        // if this line goes.
        //
        // The `last_contended_at` reset below is, *given* this one, an
        // equivalent mutant and is deliberately not claimed as pinned:
        // `contention_backoff(0)` is `Duration::ZERO`, so a zeroed streak
        // answers `true` through the `Some` arm exactly as `None` does.
        entry.consecutive_contended_scans = 0;
        entry.last_contended_at = None;
        entry.last_scan_at = Instant::now();
    }

    /// Forget volatile state after a scan that persisted a durable *data*
    /// checkpoint: that checkpoint now carries the authoritative cursor, and
    /// crash recovery must never be suppressed by stale volatile state.
    /// Error-marker checkpoints deliberately do not clear — the failure path
    /// keeps the entry and refreshes its clock via `record_failed_scan`.
    ///
    /// **The sweep interval clock survives the wipe.** `last_sweep_at` is not
    /// scan coverage — it is §2.2's minimum-interval clock — and every
    /// persisting scan lands here, so a clock that lived and died with the
    /// entry was erased by any emitting poll between slices: the next quiet
    /// reconcile poll saw no entry, `sweep_slice_due` answered `true`, and a
    /// database with interleaved writes (the typical agent-session shape, and
    /// every multi-poll cold ingest) swept at up to the reconcile cadence —
    /// up to ~10× the configured minimum — instead of the §4 interval. The
    /// skeleton an armed clock leaves behind is inert to every other reader:
    /// `stat` is `None` and the streaks are zero, so `should_skip_poll`
    /// answers `false` and `failure_retry_due` answers `true`
    /// (`failure_backoff(0)` and `contention_backoff(0)` are both
    /// `Duration::ZERO`), exactly as they do for a missing entry. NAC and
    /// OpenCode share this call site and never arm the clock (no sweep until
    /// WI-06/WI-07), so their entries still clear whole.
    /// `a_second_reconcile_poll_inside_the_sweep_interval_attaches_no_slice`
    /// fails on its emitting-poll interleave if the carry goes.
    fn clear(&self, cp_key: &str) {
        let mut map = self.lock();
        let keep_sweep_clock = map
            .get(cp_key)
            .is_some_and(|entry| entry.last_sweep_at.is_some());
        if !keep_sweep_clock {
            map.remove(cp_key);
            return;
        }
        if let Some(entry) = map.get_mut(cp_key) {
            entry.stat = None;
            entry.consecutive_noop_scans = 0;
            entry.consecutive_failed_scans = 0;
            entry.consecutive_contended_scans = 0;
            entry.last_contended_at = None;
        }
    }

    /// Entering a durably blocked replay is a scan failure for backoff
    /// purposes, and the **only** sanctioned way to record it.
    ///
    /// It exists as its own entry point because the obvious-looking
    /// `clear(); record_failed_scan();` pair is a live defect: `clear` deletes
    /// the entry, so `record_failed_scan`'s `or_insert` restarts the streak at
    /// 1 on every retry and pins the path at the 15 s floor forever. The
    /// trigger is a record failing `normalize_record` during a replacement
    /// replay — deterministic and content-driven, so it recurs on every retry —
    /// which means a flat floor re-sends a durable `BeginReplay` barrier and
    /// re-reads the entire database every 15 s indefinitely.
    /// `record_failed_scan` already sets `stat` to `None`, which is the only
    /// suppression the `clear` was there to avoid.
    fn record_blocked_replay(&self, cp_key: &str, source_generation: u32) {
        self.record_failed_scan(cp_key, source_generation);
    }

    /// Record a completed scan that ended in a failure outcome, routing it to
    /// the fault ladder or the contention exemption by `error_kind`.
    ///
    /// **Mixed-snapshot rejections do not escalate.** `ERROR_KIND_MIXED_SNAPSHOT`
    /// means the database was being written while the scan read it; that is a
    /// *contention* signal, not a fault, and it happens precisely when the
    /// source is active — which is when prompt visibility matters most (§6).
    /// Routing it into the 15 s → 15 min ladder would regress active-session
    /// freshness by up to 15 minutes, and the mitigation the spec pairs with
    /// that ("smaller scans make retries rare") is WI-07/WI-08 and does not
    /// exist yet. So a contended scan retries at the ordinary poll cadence and
    /// leaves the fault streak untouched — neither extending it nor clearing a
    /// genuine one underneath it.
    ///
    /// **Exempt from the fault ladder is not exempt from the replay throttle.**
    /// The rejection still routes to `record_contended_scan`, whose clock
    /// `failure_retry_due` reads, because a replacement replay's `Failed` arm
    /// emits a durable `BlockReplay` and an append-only `ingest_errors` row —
    /// recording nothing at all would re-run that on every tick forever. The
    /// two clocks are read in different places on purpose: `should_skip_poll`
    /// (ordinary scans) sees only the fault ladder, `failure_retry_due` (the
    /// replay barrier) sees both.
    ///
    /// Every adapter's `Failed` arm goes through this one entry point, so the
    /// classification cannot drift between them.
    ///
    /// **The exemption's width is the whole guard.** One kind is named out of
    /// five, and naming one more is a one-token edit that no outcome assertion
    /// can see — `sqlite_scan_error` and `sqlite_cursor_too_large` were both
    /// admissible with the suite green. `sqlite_scan_error` is the expensive
    /// one: eleven of the seventeen production failure sites emit it, covering
    /// the paged read, both `data_version` reads and every adapter's row loop.
    /// Route it here and no scan failure ever reaches `record_failed_scan`,
    /// `consecutive_failed_scans` stays 0, `should_skip_poll`'s failure arm
    /// never fires, and the §2.5 defect this work item exists to remove comes
    /// straight back — a full failed scan on every reconcile tick and every
    /// debounced watcher event, forever — while the barrier throttle silently
    /// drops from `FAILURE_BACKOFF_MAX` to `CONTENTION_BACKOFF_MAX`.
    /// `each_error_kind_routes_to_exactly_one_backoff_clock` pins the routing
    /// per kind rather than the outcome, so widening at *any* neighbour fails.
    fn record_scan_failure_outcome(&self, cp_key: &str, source_generation: u32, error_kind: &str) {
        if error_kind == ERROR_KIND_MIXED_SNAPSHOT {
            self.record_contended_scan(cp_key, source_generation);
            return;
        }
        self.record_failed_scan(cp_key, source_generation);
    }

    /// A failed scan starts (or extends) the failure backoff. It **creates**
    /// the entry when absent: the previous `get_mut`-only version did nothing
    /// for a database whose first scan failed, or whose failure followed any
    /// emitting scan (which clears the entry), so the most common failure
    /// shapes had no backoff at all and re-ran a full failed scan on every
    /// reconcile tick and every debounced watcher event, forever.
    ///
    /// The entry records **no covered stat**: a failure covered nothing, and
    /// claiming otherwise would suppress rescans of an unchanged file
    /// permanently rather than for the backoff window. That is also why the
    /// blocked-replay path must **not** `clear` before calling this: `clear`
    /// deletes the entry, so the very next `or_insert` restarts the streak at
    /// 1 and pins the retry at the 15 s floor forever. Setting `stat` to `None`
    /// here already removes the only suppression `clear` was there to avoid.
    fn record_failed_scan(&self, cp_key: &str, source_generation: u32) {
        let mut map = self.lock();
        let entry = map
            .entry(cp_key.to_string())
            .and_modify(|entry| {
                if entry.source_generation != source_generation {
                    entry.consecutive_noop_scans = 0;
                    entry.consecutive_failed_scans = 0;
                    entry.consecutive_contended_scans = 0;
                    entry.last_contended_at = None;
                }
            })
            .or_insert(VolatilePollEntry {
                source_generation,
                stat: None,
                consecutive_noop_scans: 0,
                consecutive_failed_scans: 0,
                last_scan_at: Instant::now(),
                last_contended_at: None,
                consecutive_contended_scans: 0,
                last_sweep_at: None,
            });
        entry.source_generation = source_generation;
        entry.stat = None;
        entry.consecutive_failed_scans = entry.consecutive_failed_scans.saturating_add(1);
        entry.last_scan_at = Instant::now();
    }

    /// Record a scan the mixed-snapshot bracket rejected.
    ///
    /// Contention is **not** a fault (§3.2), so `consecutive_failed_scans` is
    /// untouched and `should_skip_poll` keeps letting ordinary scans through at
    /// the full poll cadence — that exemption is what protects active-session
    /// freshness. What this *does* record is a clock `failure_retry_due` reads,
    /// so a replacement replay that keeps losing the bracket stops re-sending
    /// its durable barrier (and appending an `ingest_errors` row) on every tick.
    ///
    /// Like `record_failed_scan` it clears the covered stat: a rejected scan
    /// covered nothing, and a mixed-snapshot rejection means the database moved
    /// under it, so any fingerprint a previous no-op scan recorded is stale.
    fn record_contended_scan(&self, cp_key: &str, source_generation: u32) {
        let mut map = self.lock();
        let entry = map
            .entry(cp_key.to_string())
            .and_modify(|entry| {
                if entry.source_generation != source_generation {
                    entry.consecutive_noop_scans = 0;
                    entry.consecutive_failed_scans = 0;
                    entry.consecutive_contended_scans = 0;
                }
            })
            .or_insert(VolatilePollEntry {
                source_generation,
                stat: None,
                consecutive_noop_scans: 0,
                consecutive_failed_scans: 0,
                last_scan_at: Instant::now(),
                last_contended_at: None,
                consecutive_contended_scans: 0,
                last_sweep_at: None,
            });
        entry.source_generation = source_generation;
        entry.stat = None;
        entry.consecutive_contended_scans = entry.consecutive_contended_scans.saturating_add(1);
        entry.last_contended_at = Some(Instant::now());
    }

    /// True when `cp_key` is due another sweep slice: no slice has ever been
    /// recorded (fresh process — one early slice per restart is the accepted
    /// price of keeping this clock volatile), the generation changed, or the
    /// configured minimum interval has elapsed since the last committed slice.
    fn sweep_slice_due(
        &self,
        cp_key: &str,
        source_generation: u32,
        min_interval: Duration,
    ) -> bool {
        let map = self.lock();
        let Some(entry) = map.get(cp_key) else {
            return true;
        };
        if entry.source_generation != source_generation {
            return true;
        }
        match entry.last_sweep_at {
            Some(at) => at.elapsed() >= min_interval,
            None => true,
        }
    }

    /// Record a committed sweep slice for `cp_key`, starting the interval
    /// clock. Called **after** the durable-persist path's `clear` — a slice
    /// always persists a checkpoint, and until the first slice arms the clock
    /// `clear` removes the entry whole, so arming before it would be wiped
    /// and every slice would restart the interval empty
    /// (`a_second_reconcile_poll_inside_the_sweep_interval_attaches_no_slice`
    /// fails if this call or its ordering goes). Once armed, the clock also
    /// survives `clear` itself — see `clear` for why that carry is
    /// load-bearing on databases with interleaved writes.
    fn record_sweep_slice(&self, cp_key: &str, source_generation: u32) {
        let mut map = self.lock();
        let entry = map.entry(cp_key.to_string()).or_insert(VolatilePollEntry {
            source_generation,
            stat: None,
            consecutive_noop_scans: 0,
            consecutive_failed_scans: 0,
            last_scan_at: Instant::now(),
            last_contended_at: None,
            consecutive_contended_scans: 0,
            last_sweep_at: None,
        });
        entry.source_generation = source_generation;
        entry.last_sweep_at = Some(Instant::now());
    }

    /// Consecutive failed scans recorded for `cp_key`; zero when no entry
    /// exists. Test-only: it is how a backoff test observes escalation without
    /// sleeping through a 15 s → 30 s → 60 s ladder in real time.
    #[cfg(test)]
    fn consecutive_failed_scans(&self, cp_key: &str) -> u32 {
        self.lock()
            .get(cp_key)
            .map_or(0, |entry| entry.consecutive_failed_scans)
    }

    /// Consecutive mixed-snapshot rejections recorded for `cp_key`. Test-only:
    /// it is how the contention tests assert that the contention clock moved
    /// while the fault ladder did not.
    #[cfg(test)]
    fn consecutive_contended_scans(&self, cp_key: &str) -> u32 {
        self.lock()
            .get(cp_key)
            .map_or(0, |entry| entry.consecutive_contended_scans)
    }

    /// Backdate `cp_key`'s last-scan clock so the next poll sees `by` of
    /// elapsed time. Test-only: without it a backoff test can only drive ticks
    /// inside the first 15 s window, where a flat floor and an exponential
    /// ladder are indistinguishable — which is exactly how the blocked-replay
    /// reset went unnoticed.
    #[cfg(test)]
    fn age_for_tests(&self, cp_key: &str, by: Duration) {
        if let Some(entry) = self.lock().get_mut(cp_key) {
            entry.last_scan_at = entry
                .last_scan_at
                .checked_sub(by)
                .unwrap_or(entry.last_scan_at);
            // Both clocks age together, so a test that walks the fault ladder
            // is never silently held back by a stale contention clock (and
            // vice versa).
            entry.last_contended_at = entry
                .last_contended_at
                .map(|at| at.checked_sub(by).unwrap_or(at));
            // The sweep clock too: an interval test that could not age it
            // would have to really sleep through the configured interval.
            entry.last_sweep_at = entry
                .last_sweep_at
                .map(|at| at.checked_sub(by).unwrap_or(at));
        }
    }
}

/// Per-poll work accounting (issue #601 §2.0). Charged at the exact point
/// bytes leave SQLite, never derived from emitted-record counts.
///
/// **Charging rules.** Every cost gate in this issue is denominated on one of
/// these axes, so what each one means has to be pinned down here rather than
/// argued about per call site:
///
/// - `census_*` is charged by a row-materializing read whose projection
///   **excludes** the adapter's payload column(s) — the cheap change-detection
///   read. An adapter with no such read reports zero, which is the honest
///   answer, not a rounding of the payload read down.
/// - `payload_*` is charged by a row-materializing read whose projection
///   **includes** a payload column. Bytes are every variable-length byte that
///   read handed to Rust, identity columns included.
/// - Bytes charged per row are the length of what SQLite actually handed to
///   Rust, never a SQL-side `length()` expression and never the size of what
///   was emitted: a row that is read and discarded still costs its bytes.
/// - Fixed-width scalars (INTEGER/REAL) are 8 bytes and are charged on neither
///   axis; they cannot move a byte budget.
/// - **Scalar-aggregate statements materialize no row, so they charge no rows
///   on either axis.** Their *bytes* follow what SQLite had to decode: an
///   aggregate over identity or fixed-width columns (`count(*)`, `max(id)`) is
///   charged nothing, but an aggregate over a payload column
///   (`sum(length(data))`) forces SQLite to decode every byte of that column —
///   §1.1 finding 2 measured `length()` on a TEXT-affine column at 48.5 ms /
///   48 MB, so it is emphatically not a cheap probe — and those bytes must be
///   charged on the payload axis. No such read remains: WI-06 removed the last
///   one (OpenCode's per-aggregate `sum(length(data))` preflight, which
///   double-charged a cold scan by ~2×), and with it the ledger's
///   `charge_aggregate_payload_bytes` helper. Any new payload-column aggregate
///   must bring the helper back rather than call itself free.
///
/// Both axes are always recorded: rows cannot catch content growth, bytes
/// cannot catch a full scan of narrow rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScanLedger {
    /// Rows touched by a covering/narrow census.
    pub(crate) census_rows: u64,
    /// Key/metadata bytes materialized by the census.
    pub(crate) census_bytes: u64,
    /// Rows whose payload column was materialized.
    pub(crate) payload_rows: u64,
    /// Payload bytes materialized (the expensive axis).
    pub(crate) payload_bytes: u64,
    /// Synthetic records produced by the scan.
    pub(crate) rows_emitted: u64,
    /// Subset of `payload_rows` attributable to a reconciliation sweep slice.
    /// Charged only through `absorb_sweep_slice`, which is the slice driver's
    /// single commit point — a sweep read is a payload read *and* a sweep
    /// read, never one or the other.
    pub(crate) sweep_rows: u64,
    /// Subset of `payload_bytes` attributable to a sweep slice.
    pub(crate) sweep_bytes: u64,
    /// True when this poll committed less than full coverage of its source:
    /// a work budget bound first, a census was truncated, or checkpoint state
    /// was evicted to fit its ceiling (issue #601 §2.3). Never an error — the
    /// remainder is covered by later polls and by the sweep.
    pub(crate) coverage_degraded: bool,
    /// Rows the scan knew about and deliberately did not read this poll.
    pub(crate) skipped_rows: u64,
    /// Payload bytes known to have been skipped. Zero when the skipped size is
    /// unknowable without decoding (Cursor values: §1.1 finding 2 — `length()`
    /// is not a cheap probe), which under-reports rather than fabricates.
    pub(crate) skipped_bytes: u64,
    /// Checkpoint-state entries evicted to fit a state ceiling (§2.3).
    pub(crate) evicted_entries: u64,
}

impl ScanLedger {
    /// One row materialized by a payload-bearing read. Call once per row,
    /// before its columns are taken, regardless of how many payload columns
    /// that row has.
    pub(crate) fn charge_payload_row(&mut self) {
        self.payload_rows = self.payload_rows.saturating_add(1);
    }

    /// One row materialized by a census read that excluded the payload column.
    pub(crate) fn charge_census_row(&mut self, bytes: usize) {
        self.census_rows = self.census_rows.saturating_add(1);
        self.census_bytes = self.census_bytes.saturating_add(bytes as u64);
    }

    fn charge_payload_bytes(&mut self, bytes: usize) {
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
    }

    /// Record deliberately-skipped coverage (§2.3): a budget bound before the
    /// scan finished, or a census was truncated. Charged where the skip is
    /// decided, with whatever counts are known there.
    pub(crate) fn mark_degraded(&mut self, skipped_rows: u64, skipped_bytes: u64) {
        self.coverage_degraded = true;
        self.skipped_rows = self.skipped_rows.saturating_add(skipped_rows);
        self.skipped_bytes = self.skipped_bytes.saturating_add(skipped_bytes);
    }

    /// Record checkpoint-state eviction (§2.3): entries dropped to fit a state
    /// ceiling. Eviction is degraded coverage — the evicted entries are
    /// re-covered (and re-emitted) by a later poll.
    pub(crate) fn mark_evicted(&mut self, evicted_entries: u64) {
        if evicted_entries == 0 {
            return;
        }
        self.coverage_degraded = true;
        self.evicted_entries = self.evicted_entries.saturating_add(evicted_entries);
    }

    /// Fold one committed sweep slice's ledger into the poll's ledger. This is
    /// the **only** writer of the sweep axes: the slice's reads charge its own
    /// slice-scoped ledger at the read site (through the same sanctioned
    /// helpers as every other read), and this fold states that those payload
    /// rows/bytes were sweep work. Payload axes are folded too — a sweep read
    /// is still a payload read, and hiding it from the payload axes would let
    /// a sweep evade every fast-path budget assertion.
    pub(crate) fn absorb_sweep_slice(&mut self, slice: &ScanLedger) {
        self.payload_rows = self.payload_rows.saturating_add(slice.payload_rows);
        self.payload_bytes = self.payload_bytes.saturating_add(slice.payload_bytes);
        self.census_rows = self.census_rows.saturating_add(slice.census_rows);
        self.census_bytes = self.census_bytes.saturating_add(slice.census_bytes);
        self.sweep_rows = self.sweep_rows.saturating_add(slice.payload_rows);
        self.sweep_bytes = self.sweep_bytes.saturating_add(slice.payload_bytes);
        // `rows_emitted` is deliberately not folded: the slice driver pushes
        // its synthetic records into the scan's shared `records` vec, so the
        // slice-scoped ledger never charges the field, and the scan assigns
        // `ledger.rows_emitted = records.len()` wholesale after the slice —
        // a fold here would be dead on one end and double-counted on the
        // other the moment either of those facts changed.
    }
}

/// One poll's read budget (issue #601 §2.1/§2.2), instantiated per scan from
/// `[ingest.sqlite]`. Exhaustion is never an error: the caller commits what it
/// has and records `coverage_degraded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanBudget {
    pub(crate) max_payload_bytes: u64,
    pub(crate) max_payload_rows: u64,
}

impl ScanBudget {
    pub(crate) fn fast_path(config: &moraine_config::SqliteIngestConfig) -> Self {
        Self {
            max_payload_bytes: config.fast_path_max_payload_bytes,
            max_payload_rows: config.fast_path_max_payload_rows,
        }
    }

    pub(crate) fn sweep_slice(config: &moraine_config::SqliteIngestConfig) -> Self {
        Self {
            max_payload_bytes: config.sweep_slice_max_payload_bytes,
            max_payload_rows: config.sweep_slice_max_payload_rows,
        }
    }

    /// The replacement-replay budget: none. A budget-degraded replay would
    /// finalize an incomplete generation through #602's publication path —
    /// see `CursorScanPlan::unbudgeted` for the argument. Ordinary polls
    /// must never take this.
    pub(crate) fn unbounded() -> Self {
        Self {
            max_payload_bytes: u64::MAX,
            max_payload_rows: u64::MAX,
        }
    }

    /// True when the charged work has reached either axis of this budget.
    /// `>=` on both axes on purpose: a budget of N rows means the N-th row is
    /// the last one read, and a byte budget binds the moment it is met — the
    /// row that crossed it was still committed (§2.1: commit what was read).
    pub(crate) fn is_exhausted_by(&self, payload_rows: u64, payload_bytes: u64) -> bool {
        payload_rows >= self.max_payload_rows || payload_bytes >= self.max_payload_bytes
    }

    /// True when at least half of either axis is consumed — sweep-eligibility
    /// condition 3 (§2.2): a database whose fast path is doing real work does
    /// not also pay sweep cost this poll. `>=`, so the exact midpoint already
    /// binds — one half is the threshold, not the last admissible value.
    pub(crate) fn is_half_consumed_by(&self, payload_rows: u64, payload_bytes: u64) -> bool {
        payload_rows.saturating_mul(2) >= self.max_payload_rows
            || payload_bytes.saturating_mul(2) >= self.max_payload_bytes
    }
}

/// Durable reconciliation-sweep cursor (issue #601 §2.2), persisted inside the
/// adapter's `cursor_json`. Structural, deterministic state only — every field
/// advances exactly when a slice commits work, so it rides the #602 transition
/// digest safely (§2.6). Poll health/telemetry must NOT be added here.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct SweepState {
    /// Resume position in the adapter's sweep ordering (Cursor: kv key).
    /// Empty = start of a cycle.
    #[serde(default)]
    pub(crate) cursor: String,
    /// Unix ms at which the in-progress cycle started.
    #[serde(default)]
    pub(crate) cycle_started_unix_ms: u64,
    /// Unix ms at which the last *complete* cycle finished. 0 = never.
    #[serde(default)]
    pub(crate) last_complete_unix_ms: u64,
    /// Completed cycles since the generation began.
    #[serde(default)]
    pub(crate) completed_cycles: u64,
    /// Payload bytes covered so far by the in-progress cycle.
    #[serde(default)]
    pub(crate) cycle_payload_bytes: u64,
    /// Payload bytes the last completed cycle covered — the denominator of
    /// `projected_full_sweep_seconds` (§2.2's published interval).
    #[serde(default)]
    pub(crate) last_cycle_payload_bytes: u64,
}

impl SweepState {
    /// True for a state no slice has ever advanced; used to keep `cursor_json`
    /// byte-identical for sources that have never swept.
    pub(crate) fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// §2.2's published maximum complete-sweep interval — a projection from
    /// the last completed cycle, `None` until one exists. The *observed*
    /// interval (`last_complete_unix_ms`) is surfaced alongside it by WI-09,
    /// because the projection is a model and the observation is the truth.
    pub(crate) fn projected_full_sweep_seconds(
        &self,
        slice_max_payload_bytes: u64,
        slice_min_interval_seconds: u64,
    ) -> Option<u64> {
        if self.completed_cycles == 0 || slice_max_payload_bytes == 0 {
            return None;
        }
        let slices = self
            .last_cycle_payload_bytes
            .div_ceil(slice_max_payload_bytes)
            // An empty (or sub-slice) cycle still costs one slice.
            .max(1);
        Some(slices.saturating_mul(slice_min_interval_seconds))
    }
}

/// One processed item in an adapter's sweep ordering.
pub(crate) struct SweepItem {
    /// This item's position; becomes the resume cursor once processed.
    pub(crate) position: String,
    /// Payload bytes the item charged, accumulated into the cycle total.
    pub(crate) payload_bytes: u64,
}

/// What one driven slice did, for the caller to commit. Cycle completion is
/// readable off the state itself (`cursor` wrapped to empty, `completed_cycles`
/// advanced), so the report carries no separate flag to drift from it.
pub(crate) struct SweepSliceReport {
    pub(crate) state: SweepState,
}

/// The shared slice driver (issue #601 §2.2, WI-04): owns budget binding,
/// forward progress, cursor advancement and cycle bookkeeping, so every
/// adapter's sweep obeys one set of rules. The adapter supplies `next_item`,
/// which reads (and charges to `slice`) the first item strictly after the
/// given position in its sweep ordering, or `None` at the end of the ordering.
///
/// Rules, each load-bearing:
/// - **Budget binds between items, never before the first** — a slice always
///   makes forward progress, so a single item larger than the whole byte
///   budget is processed and the cursor advances past it (G6c). The same rule
///   makes `max_millis = 0` mean "one item per slice", not "no progress".
/// - **A cycle ends the slice.** On wrap the cursor resets to empty, the cycle
///   stamps advance, and the slice stops — cycle completion is the durable
///   commit point G5b counts.
/// - The driver never touches the cursor except through processed items, so a
///   rejected scan (mixed snapshot) discards the whole advance with the rest
///   of the scan's state.
pub(crate) fn drive_sweep_slice<E>(
    prior: &SweepState,
    budget: &ScanBudget,
    max_millis: u64,
    now_unix_ms: u64,
    slice: &mut ScanLedger,
    mut next_item: impl FnMut(&str, &mut ScanLedger) -> std::result::Result<Option<SweepItem>, E>,
) -> std::result::Result<SweepSliceReport, E> {
    let started = Instant::now();
    let mut state = prior.clone();
    if state.cursor.is_empty() {
        state.cycle_started_unix_ms = now_unix_ms;
        state.cycle_payload_bytes = 0;
    }
    let mut items = 0u64;
    loop {
        if items > 0
            && (budget.is_exhausted_by(slice.payload_rows, slice.payload_bytes)
                || started.elapsed() >= Duration::from_millis(max_millis))
        {
            break;
        }
        match next_item(&state.cursor, slice)? {
            Some(item) => {
                items += 1;
                state.cursor = item.position;
                state.cycle_payload_bytes =
                    state.cycle_payload_bytes.saturating_add(item.payload_bytes);
            }
            None => {
                state.last_cycle_payload_bytes = state.cycle_payload_bytes;
                state.last_complete_unix_ms = now_unix_ms;
                state.completed_cycles = state.completed_cycles.saturating_add(1);
                state.cursor = String::new();
                break;
            }
        }
    }
    Ok(SweepSliceReport { state })
}

/// The only sanctioned way to materialize a variable-length payload column:
/// it charges the ledger at the read site, before the value is visible to the
/// caller. A new `row.get(payload_col)` that bypasses this is a ledger
/// under-report, and every budget denominated on `payload_bytes` becomes a lie
/// — reviewers reject it.
pub(crate) fn take_payload_text(
    ledger: &mut ScanLedger,
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<String>> {
    let value: Option<String> = row.get(index)?;
    ledger.charge_payload_bytes(value.as_ref().map_or(0, String::len));
    Ok(value)
}

/// `take_payload_text` for a column the schema declares NOT NULL.
///
/// A NULL here is **schema drift, and §3.2 wants it surfaced rather than
/// absorbed**: `row.get::<_, String>` raises `InvalidColumnType`, which fails
/// the scan and leaves the checkpoint where it was. That is deliberately the
/// same behaviour these columns had before the ledger existed; the ledger
/// charge is the only thing this wrapper adds.
///
/// Absorbing the NULL instead is not a smaller version of the same thing. An
/// empty id column manufactures a record whose `source_line_no`/`source_offset`
/// — and therefore whose `event_uid` — are hashed from nothing (§6 "stable
/// logical IDs"); an empty timestamp column manufactures an epoch.
///
/// This is the default for a text column. `take_payload_nullable_string` is
/// the per-column exception, and each of its call sites has to name the column
/// it applies to and why that column tolerates NULL.
pub(crate) fn take_payload_required_string(
    ledger: &mut ScanLedger,
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<String> {
    let value: String = row.get(index)?;
    ledger.charge_payload_bytes(value.len());
    Ok(value)
}

/// `take_payload_text` for a column that genuinely tolerates NULL, where the
/// empty string is the documented default rather than a swallowed defect.
/// See `take_payload_required_string` for the rule this is the exception to.
pub(crate) fn take_payload_nullable_string(
    ledger: &mut ScanLedger,
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<String> {
    Ok(take_payload_text(ledger, row, index)?.unwrap_or_default())
}

/// `take_payload_text` for a column whose storage class is not trusted:
/// Cursor writes JSON documents as TEXT into a column declared BLOB, so the
/// value is taken by reference and charged by its materialized byte length.
pub(crate) fn take_payload_blob(
    ledger: &mut ScanLedger,
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<Vec<u8>>> {
    let value = match row.get_ref(index)? {
        ValueRef::Null => None,
        ValueRef::Text(text) => Some(text.to_vec()),
        ValueRef::Blob(blob) => Some(blob.to_vec()),
        ValueRef::Integer(int) => Some(int.to_string().into_bytes()),
        ValueRef::Real(real) => Some(real.to_string().into_bytes()),
    };
    ledger.charge_payload_bytes(value.as_ref().map_or(0, Vec::len));
    Ok(value)
}

/// Fold one poll's ledger into the host-global counters. Called once per poll
/// that actually scanned, on both the success and the failure path, so a scan
/// that read 48 MB and then lost the mixed-snapshot race is still charged.
///
/// **Both axes are folded, not just the payload one.** A counter nothing can
/// read is the unfailable-guard pattern this issue exists to remove, and the
/// OpenCode rewind preflight charges the census axis *only* — folding payload
/// alone would leave that call site unable to move any observable at all.
pub(crate) fn record_scan_ledger(metrics: &Arc<Metrics>, ledger: &ScanLedger) {
    metrics
        .sqlite_poll_payload_bytes_total
        .fetch_add(ledger.payload_bytes, std::sync::atomic::Ordering::Relaxed);
    metrics
        .sqlite_poll_payload_rows_total
        .fetch_add(ledger.payload_rows, std::sync::atomic::Ordering::Relaxed);
    metrics
        .sqlite_poll_census_rows_total
        .fetch_add(ledger.census_rows, std::sync::atomic::Ordering::Relaxed);
    metrics
        .sqlite_poll_census_bytes_total
        .fetch_add(ledger.census_bytes, std::sync::atomic::Ordering::Relaxed);
    metrics
        .sqlite_sweep_rows_total
        .fetch_add(ledger.sweep_rows, std::sync::atomic::Ordering::Relaxed);
    metrics
        .sqlite_sweep_bytes_total
        .fetch_add(ledger.sweep_bytes, std::sync::atomic::Ordering::Relaxed);
    if ledger.coverage_degraded {
        metrics
            .sqlite_coverage_degraded_scans_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// One committed sweep slice. Counted at the commit site — after the slice's
/// checkpoint persisted — never where the slice merely ran, so a slice whose
/// scan lost the mixed-snapshot bracket is not counted as coverage.
pub(crate) fn record_sweep_slice_committed(metrics: &Arc<Metrics>) {
    metrics
        .sqlite_sweep_slices_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Reads one table's `CREATE TABLE` text out of `sqlite_master`, charging it
/// on the census axis.
///
/// Schema validation is a row-materializing read whose projection excludes
/// every payload column, so the ledger's census rule covers it exactly. It ran
/// uncharged on every poll of all three adapters — a TEXT column pulled into
/// Rust that neither axis knew about, which is the kind of invisible cost the
/// ledger exists to expose.
pub(crate) fn schema_sql_for_table(
    connection: &Connection,
    table: &str,
    ledger: &mut ScanLedger,
) -> rusqlite::Result<Option<String>> {
    let sql: Option<String> = connection.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        rusqlite::params![table],
        |row| row.get(0),
    )?;
    ledger.charge_census_row(sql.as_ref().map_or(0, String::len));
    Ok(sql)
}

/// `PRAGMA table_info` column names, charged on the census axis for the same
/// reason as `schema_sql_for_table`: one materialized row per column.
pub(crate) fn table_column_names(
    connection: &Connection,
    table: &str,
    ledger: &mut ScanLedger,
) -> Result<Vec<String>> {
    let mut stmt = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("failed to inspect {table} columns"))?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<String>, _>>()?;
    for name in &names {
        ledger.charge_census_row(name.len());
    }
    Ok(names)
}

/// The census a scan owes for validating `tables`, so every adapter's ledger
/// test can state its census total as "schema validation **plus** this
/// adapter's own census read" rather than as an unexplained constant.
///
/// Deliberately **does not** call `schema_sql_for_table` /
/// `table_column_names`: an expectation computed by the very functions under
/// test moves with them, so dropping a charge would lower both sides equally
/// and every assertion would stay green. This is an independent oracle — one
/// `sqlite_master.sql` row per table, one `PRAGMA table_info` row per column,
/// each charged its own materialized length.
#[cfg(test)]
pub(crate) fn expected_schema_census(connection: &Connection, tables: &[&str]) -> ScanLedger {
    let mut ledger = ScanLedger::default();
    for table in tables {
        let sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                rusqlite::params![table],
                |row| row.get(0),
            )
            .expect("fixture schema row");
        ledger.census_rows += 1;
        ledger.census_bytes += sql.as_ref().map_or(0, String::len) as u64;

        let mut stmt = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .expect("fixture column names");
        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("fixture column names")
            .map(|row| row.expect("fixture column name"))
            .collect();
        ledger.census_rows += names.len() as u64;
        ledger.census_bytes += names.iter().map(|name| name.len() as u64).sum::<u64>();
    }
    ledger
}

/// One completed scan that ended in `Failed`. Denominated on observed scans,
/// which is what the failure-backoff gate asserts on — not on `ingest_errors`
/// rows, which are rate-limited and therefore cannot count scans.
pub(crate) fn record_scan_failure(metrics: &Arc<Metrics>) {
    metrics
        .sqlite_scan_failures_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        /// Boxed so the `Failed` variant is not forced to carry a
        /// `CursorState`'s footprint.
        new_state: Box<CursorState>,
        schema_fingerprint: u64,
        relevant_keys: u64,
        /// True when this scan ran a sweep slice whose advance is carried in
        /// `new_state.sweep`. The caller commits it (metrics + interval
        /// clock) only after the checkpoint persists.
        swept: bool,
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
    // Carries the normalization-rules version for the same reason the JSONL
    // fingerprint does: an adapter change that alters attribution or row shape
    // must replace the source rather than leave both interpretations live.
    let rules = crate::dispatch::SOURCE_NORMALIZATION_RULES_VERSION;
    format!("sqlite-publication-v1:{rules}:{format}:{exclusions_hash:016x}")
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
    // Both disjuncts are load-bearing and they do not cover each other. A
    // process that dies between `BeginReplay` and `FinalizeReplay` leaves
    // `replaying` with no error status and no block reason, so dropping the
    // first disjunct strands that source: the poll becomes ordinary, the
    // cursor is not reset, the barrier is never re-sent, and the status is
    // quietly rewritten to `active`.
    // `a_crash_interrupted_replay_resumes_from_its_replaying_status` fails if
    // it goes. Widening the *second* disjunct to a bare `status == "error"` is
    // green at all three adapters and is recorded, unfixed, as plan §7.2 F2.
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
    // The failure backoff has to gate the *barrier*, not just the scan:
    // `begin_database_replay` is durable, so throttling only the scan behind
    // it would re-send a barrier with nothing behind it on every tick — the
    // failure mode §2.1(2) warns about. A genuine replacement (new inode,
    // changed exclusions) bumped the generation above and is never throttled.
    if retry_blocked_replay
        && !starts_replacement
        && !poll_state.failure_retry_due(&cp_key, checkpoint.source_generation)
    {
        return Ok(());
    }
    if replacement_replay {
        begin_database_replay(&sink_tx, &checkpoint, scan_boundary, &policy_fingerprint).await?;
    }

    // Cheap no-change short-circuit: nothing touched the database or its WAL
    // sidecars since the last successful poll.
    //
    // §2.5 also specifies a `|| !failure_retry_due` disjunct here. **It must
    // not be added while the contention clock lives in `failure_retry_due`** —
    // not by WI-04, not as a tidy-up.
    //
    // It was once outcome-redundant with `should_skip_poll`'s failure arm
    // below. §3.2's contention exemption broke that equivalence on purpose:
    // `failure_retry_due` reads `last_contended_at` as well as the fault
    // ladder, `should_skip_poll` deliberately does not. After a mixed-snapshot
    // rejection the two disagree by design — `failure_retry_due` is false for
    // up to `CONTENTION_BACKOFF_MAX` (60 s) while `should_skip_poll` stays
    // false, so ordinary scans keep running at the full poll cadence.
    //
    // So the disjunct is now outcome-**changing**: it would return early on an
    // ordinary poll of a contended — i.e. actively written — database for up to
    // a minute. That is exactly the active-session freshness regression the
    // exemption exists to prevent, and exactly what `record_contended_scan`'s
    // doc comment promises does not happen. A pre-`should_skip_poll` throttle
    // for WI-04's sweep slice must therefore read the fault ladder alone.
    //
    // `an_ordinary_poll_of_a_contended_database_is_not_throttled` fails if the
    // disjunct is added; `a_contended_replacement_replay_throttles_its_barrier`
    // bounds the other direction (the durable barrier *is* throttled). See the
    // deviation record in `plans/601-delta-sqlite.md` §7 WI-10.
    //
    // The `!state.pending_coverage` conjunct is §2.3's "continue next poll":
    // while a cold-ingest remainder exists, an unchanged stat must not end
    // the poll, because a quiet database's stat never changes again and the
    // remainder would be unreachable forever. The resume terminates — every
    // resumed scan reads never-read keys first, so the debt strictly shrinks
    // and the flag clears durably with the covering scan's checkpoint
    // (`a_degraded_cold_ingest_completes_without_new_writes` fails if this
    // conjunct or the never-read-first ordering goes).
    if state.stat == current_stat && state.last_error.is_empty() && !state.pending_coverage {
        return Ok(());
    }

    // Volatile short-circuit + rescan backoff (issue #443): no-op scans leave
    // the durable checkpoint untouched, so their coverage lives here instead.
    // Skipped during a replay: the barrier has already been sent, and a skip
    // here would leave it with no scan behind it.
    if !replacement_replay
        && poll_state.should_skip_poll(&cp_key, checkpoint.source_generation, &current_stat)
    {
        return Ok(());
    }

    // Sweep eligibility, conditions 1, 2 and 4 (issue #601 §2.2/§2.4): only a
    // `Reconcile`-triggered poll — the provenance WI-03 established, including
    // the owed-tick upgrades `arm_owed_reconcile` performs — of a database
    // that is not replaying and whose per-database interval clock has expired
    // requests a slice. Condition 3 (a quiet, cheap fast path) is decided
    // inside the scan, where this poll's ledger exists.
    let sweep_requested = work.trigger == WorkTrigger::Reconcile
        && !replacement_replay
        && poll_state.sweep_slice_due(
            &cp_key,
            checkpoint.source_generation,
            Duration::from_secs(config.ingest.sqlite.sweep_slice_min_interval_seconds),
        );
    // A replacement replay is unbudgeted (see `CursorScanPlan::unbudgeted`):
    // its finalize publishes the generation whole, so degrading it would
    // publish a hole.
    let plan = if replacement_replay {
        CursorScanPlan::unbudgeted()
    } else {
        CursorScanPlan::from_config(
            config,
            sweep_requested.then(|| SweepPlan::from_config(config)),
        )
    };
    let scan_db_path = source_file.clone();
    let scan_state = state.clone();
    let (outcome, ledger) = tokio::task::spawn_blocking(move || {
        let mut ledger = ScanLedger::default();
        let outcome = scan_database(&scan_db_path, &scan_state, &plan, &mut ledger);
        (outcome, ledger)
    })
    .await
    .context("cursor_sqlite scan task panicked")?;
    record_scan_ledger(metrics, &ledger);

    match outcome {
        ScanOutcome::Scanned {
            records,
            mut new_state,
            schema_fingerprint,
            relevant_keys,
            swept,
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
                && *new_state == prior_state_covered
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
                cursor_json: (*new_state).serialize(),
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
                    poll_state.record_blocked_replay(&cp_key, checkpoint.source_generation);
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
            if swept {
                // Commit the slice only now — after its checkpoint persisted
                // and after `clear` wiped the entry — so the interval clock
                // survives the wipe and a slice whose scan failed or lost the
                // mixed-snapshot bracket is neither counted nor throttling
                // the retry.
                poll_state.record_sweep_slice(&cp_key, checkpoint.source_generation);
                record_sweep_slice_committed(metrics);
                debug!(
                    "{}:{} sweep slice committed (cursor {:?}, cycles {}, projected full sweep {:?}s)",
                    work.source_name,
                    source_file,
                    new_state.sweep.cursor,
                    new_state.sweep.completed_cycles,
                    new_state.sweep.projected_full_sweep_seconds(
                        config.ingest.sqlite.sweep_slice_max_payload_bytes,
                        config.ingest.sqlite.sweep_slice_min_interval_seconds,
                    ),
                );
            }

            if emitted > 0 {
                debug!(
                    "{}:{} cursor_sqlite emitted {} changed records ({} relevant keys, \
                     {} payload rows, {} payload bytes)",
                    work.source_name,
                    source_file,
                    emitted,
                    relevant_keys,
                    ledger.payload_rows,
                    ledger.payload_bytes
                );
            }
            Ok(())
        }
        ScanOutcome::Failed {
            error_kind,
            error_text,
        } => {
            record_scan_failure(metrics);
            poll_state.record_scan_failure_outcome(
                &cp_key,
                checkpoint.source_generation,
                error_kind,
            );

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

/// Per-scan work plan for the Cursor adapter (issue #601 §2.1/§2.2): the
/// fast-path budget, the census cap, and — on sweep-eligible polls only — the
/// sweep slice request. Built once per poll from `[ingest.sqlite]` in
/// `process_cursor_sqlite_db` and passed whole into the blocking scan, so
/// every budget the scan enforces arrived through one auditable argument.
pub(crate) struct CursorScanPlan {
    fast_budget: ScanBudget,
    max_census_rows: u64,
    /// Ceiling on the serialized cursor payload (§2.3), enforced by eviction
    /// after the scan's reads — a *state* bound, never a work budget, which
    /// is why `unbudgeted()` keeps it.
    max_checkpoint_bytes: usize,
    sweep: Option<SweepPlan>,
}

impl CursorScanPlan {
    pub(crate) fn from_config(config: &AppConfig, sweep: Option<SweepPlan>) -> Self {
        Self {
            fast_budget: ScanBudget::fast_path(&config.ingest.sqlite),
            max_census_rows: config.ingest.sqlite.fast_path_max_census_rows,
            max_checkpoint_bytes: MAX_CURSOR_CHECKPOINT_BYTES,
            sweep,
        }
    }

    /// The replacement-replay plan: no budget, no census cap, no sweep.
    ///
    /// A replay's `FinalizeReplay` publishes the new generation over the old
    /// one, so a budget-degraded replay would publish an *incomplete*
    /// generation — a transient data loss #602's old-complete/new-complete
    /// contract exists to forbid. The replay therefore pays the pre-#601
    /// cost, once, per genuine replacement; ordinary polls never take this
    /// plan. `a_replacement_replay_reads_past_the_fast_path_budget` fails if
    /// a budget sneaks back in.
    ///
    /// The checkpoint ceiling **does** apply here: it is a *state* bound
    /// enforced by eviction after the replay's reads and emissions, so it
    /// bounds what is persisted without un-reading anything — the no-hole
    /// argument above is about reads, not state size, and an evicted key's
    /// records were already emitted by this replay.
    pub(crate) fn unbudgeted() -> Self {
        Self {
            fast_budget: ScanBudget::unbounded(),
            max_census_rows: u64::MAX,
            max_checkpoint_bytes: MAX_CURSOR_CHECKPOINT_BYTES,
            sweep: None,
        }
    }
}

/// One requested sweep slice: its budget, wall-clock cap, and the unix-ms
/// stamp its cycle bookkeeping commits (§2.2).
pub(crate) struct SweepPlan {
    budget: ScanBudget,
    max_millis: u64,
    now_unix_ms: u64,
}

impl SweepPlan {
    pub(crate) fn from_config(config: &AppConfig) -> Self {
        Self {
            budget: ScanBudget::sweep_slice(&config.ingest.sqlite),
            max_millis: config.ingest.sqlite.sweep_slice_max_millis,
            now_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }
}

/// Blocking phase: open the database read-only, validate schema, census the
/// relevant keyspace, and materialize candidate payloads newest-first under
/// the fast-path budget.
///
/// The caller owns `ledger` so that every early return — including each
/// failure arm — still reports the bytes this scan had already paid for.
///
/// **Ceiling degradation (issue #601 §2.3).** This scan has no history-size
/// failure mode: `MAX_RELEVANT_KEYS` and its `sqlite_cursor_too_large` error
/// are retired. What bounds a poll is per-poll work — `plan.fast_budget` on
/// the payload axes and `plan.max_census_rows` on the census — and exceeding
/// either commits what was read (newest-first, by `rowid DESC`: §1.1 shows
/// rowid is a sound recency ordering even though it is not a watermark),
/// records `coverage_degraded`, and leaves the rest to later polls and the
/// sweep.
fn scan_database(
    db_path: &str,
    prior: &CursorState,
    plan: &CursorScanPlan,
    ledger: &mut ScanLedger,
) -> ScanOutcome {
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

    let schema_fingerprint = match validate_schema(&connection, ledger) {
        Ok(fingerprint) => fingerprint,
        Err(text) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCHEMA,
                error_text: text,
            }
        }
    };

    // The census: `(rowid, key)` over both relevant ranges, covering-index
    // backed (§1.1: ~0.1 ms / ~19 KB for the whole reference keyspace). It is
    // the exact change detector for inserts and deletes and the recency
    // ordering for the candidate read below.
    let (census, truncation) = match census_relevant_keys(&connection, plan.max_census_rows, ledger)
    {
        Ok(result) => result,
        Err(exc) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("{exc:#}"),
            }
        }
    };
    if truncation.is_some() {
        // Best-effort size of the un-censused remainder, so the degradation
        // is quantified rather than merely flagged. The count is a covering
        // index aggregate (charged on neither axis per the ledger rules).
        match count_relevant_keys(&connection) {
            Ok(total) => {
                let skipped = (total as u64).saturating_sub(census.len() as u64);
                ledger.mark_degraded(skipped, 0);
            }
            Err(exc) => {
                return ScanOutcome::Failed {
                    error_kind: ERROR_KIND_SCAN,
                    error_text: format!("{exc:#}"),
                }
            }
        }
    }
    let relevant_keys = census.len() as u64;

    let mut new_state = CursorState {
        version: CURSOR_STATE_VERSION,
        format: SOURCE_FORMAT_CURSOR_SQLITE.to_string(),
        stat: StatFingerprint::default(),
        project_exclusions_hash: prior.project_exclusions_hash,
        kv_hashes: BTreeMap::new(),
        last_error: String::new(),
        sweep: prior.sweep.clone(),
        pending_coverage: false,
    };
    let mut records = Vec::<SyntheticRecord>::new();
    let mut workspace_cache = HashMap::<String, Option<String>>::new();

    // Candidate read: never-read keys first, then `rowid DESC` within each
    // class. `rowid DESC` is the §2.3 recency heuristic — it chooses ORDER
    // only. Skipping is justified solely by the budget below, and every
    // skipped key stays covered (prior hash carried) or re-detected (new key
    // stays absent from the state) on later polls.
    //
    // Never-read-first is what makes a budget-degraded ingest *converge*: a
    // plain recency order re-reads the same newest slice on every poll, so a
    // database larger than one budget would keep its cold tail forever. With
    // unknown keys ahead of re-verification, each resumed poll retires at
    // least one budget's worth of never-read debt, and recency is undisturbed
    // where it matters — a genuinely new key has no prior hash, so it is in
    // the first class already, newest first.
    let mut candidates = census;
    candidates.sort_by(|a, b| {
        let known_a = prior.kv_hashes.contains_key(&a.key);
        let known_b = prior.kv_hashes.contains_key(&b.key);
        known_a.cmp(&known_b).then_with(|| b.rowid.cmp(&a.rowid))
    });
    for idx in 0..candidates.len() {
        if plan
            .fast_budget
            .is_exhausted_by(ledger.payload_rows, ledger.payload_bytes)
        {
            // Commit what was read (§2.1): the remainder keeps its prior
            // hash — "unread this poll", never "deleted" — and new keys stay
            // absent so the next poll re-detects them, newest-first again.
            // Skipped bytes are unknowable without decoding (§1.1 finding 2),
            // so rows are counted and bytes honestly under-reported as zero.
            let remaining = &candidates[idx..];
            ledger.mark_degraded(remaining.len() as u64, 0);
            for skipped in remaining {
                if let Some(hash) = prior.kv_hashes.get(&skipped.key) {
                    new_state
                        .kv_hashes
                        .insert(skipped.key.clone(), hash.clone());
                }
            }
            break;
        }
        let key = &candidates[idx].key;
        let value = match read_value_for_key(&connection, key, ledger) {
            Ok(Some(value)) => value,
            // The row vanished between census and read: a mid-scan commit the
            // data_version bracket below will reject; nothing to record here.
            Ok(None) => continue,
            Err(exc) => {
                return ScanOutcome::Failed {
                    error_kind: ERROR_KIND_SCAN,
                    error_text: format!("{exc:#}"),
                }
            }
        };
        let bytes = value.unwrap_or_default();
        let hash = format!("{:016x}", hash_bytes(&bytes));
        let unchanged = prior.kv_hashes.get(key) == Some(&hash);
        if !unchanged && !bytes.is_empty() {
            if let Some(mut record) = synthesize_cursor_sqlite_record(key, &bytes) {
                stamp_bubble_workspace(&connection, &mut workspace_cache, &mut record, ledger);
                records.push(record);
            }
        }
        new_state.kv_hashes.insert(key.clone(), hash);
    }

    if let Some(truncation) = &truncation {
        // Deletion pruning is exact only against a complete census. Carry
        // every prior entry beyond the truncation point so an un-censused key
        // reads "unverified this poll", never silently "deleted".
        for (key, hash) in &prior.kv_hashes {
            if !census_covered(truncation, key) {
                new_state
                    .kv_hashes
                    .entry(key.clone())
                    .or_insert_with(|| hash.clone());
            }
        }
    }

    // Optional sweep slice (§2.2). Conditions 1 (reconcile trigger), 2
    // (interval clock) and 4 (not replaying) were decided by the caller —
    // `plan.sweep` exists only when they held. Condition 3 is decided here,
    // where the fast path's cost is known: a poll that emitted anything, was
    // budget-degraded, or consumed half its budget does not also pay sweep
    // cost. The slice runs inside the data_version bracket, so a mixed
    // snapshot discards its advance with the rest of the scan.
    let mut swept = false;
    if let Some(sweep_plan) = &plan.sweep {
        // §2.2 condition 3, with one deliberate widening: a *degraded* poll is
        // sweep-eligible even though it consumed its whole budget. On a source
        // larger than one fast-path budget every poll is degraded and every
        // poll consumes the full budget, so the plan's literal half-budget
        // clause would block the sweep on exactly the sources whose tail only
        // the sweep can cover — §0's coverage guarantee outranks §2.2's
        // politeness. The emission clause is kept: an emitting poll defers its
        // slice to the next quiet reconcile tick.
        let eligible = records.is_empty()
            && (ledger.coverage_degraded
                || !plan
                    .fast_budget
                    .is_half_consumed_by(ledger.payload_rows, ledger.payload_bytes));
        if eligible {
            let mut slice = ScanLedger::default();
            let driven = drive_sweep_slice(
                &prior.sweep,
                &sweep_plan.budget,
                sweep_plan.max_millis,
                sweep_plan.now_unix_ms,
                &mut slice,
                |after, slice| {
                    next_cursor_sweep_item(
                        &connection,
                        after,
                        &mut new_state.kv_hashes,
                        &mut records,
                        &mut workspace_cache,
                        slice,
                    )
                },
            );
            // The slice's reads are paid whether or not it completed; fold
            // them before inspecting the outcome so a failure arm still
            // reports them.
            ledger.absorb_sweep_slice(&slice);
            match driven {
                Ok(report) => {
                    new_state.sweep = report.state;
                    swept = true;
                }
                Err(exc) => {
                    return ScanOutcome::Failed {
                        error_kind: ERROR_KIND_SCAN,
                        error_text: format!("sweep slice failed: {exc:#}"),
                    }
                }
            }
        }
    }

    // The checkpoint-state ceiling (issue #601 §2.3): evict the oldest kv
    // hashes until the persisted payload fits — never fail the scan. This is
    // `MAX_RELEVANT_KEYS`' replacement, and the bound that keeps
    // `cursor_json` (hashed into the #602 transition digest, §2.6) from
    // growing with the keyspace. It runs after the sweep slice so it bounds
    // everything this poll would persist, and before the resume marker below
    // so an evicted *censused* key re-opens `pending_coverage` structurally —
    // censused-but-absent is exactly what the marker scans for. (An evicted
    // key the census did not cover only exists when the census truncated, and
    // `truncation.is_some()` already holds the marker open.)
    let evicted = new_state.evict_to_fit(plan.max_checkpoint_bytes, &candidates);
    ledger.mark_evicted(evicted);

    // §2.3's persisted resume marker, computed after the sweep slice so keys
    // the slice just read count as covered. A censused key absent from the
    // hash map has never been read in this generation; a truncated census may
    // hide more of them. Either keeps the cheap stat short-circuit open until
    // a scan covers everything (see the conjunct in
    // `process_cursor_sqlite_db`), at which point the flag clears with that
    // scan's checkpoint.
    new_state.pending_coverage = truncation.is_some()
        || candidates
            .iter()
            .any(|row| !new_state.kv_hashes.contains_key(&row.key));

    let data_version_after = match sqlite_data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return ScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read post-scan data_version: {exc:#}"),
            }
        }
    };
    if snapshot_is_mixed(
        db_path,
        data_version_before,
        data_version_after,
        opened_stat,
    ) {
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

    ledger.rows_emitted = records.len() as u64;
    ScanOutcome::Scanned {
        records,
        new_state: Box::new(new_state),
        schema_fingerprint,
        relevant_keys,
        swept,
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

fn validate_schema(
    connection: &Connection,
    ledger: &mut ScanLedger,
) -> std::result::Result<u64, String> {
    let schema_sql =
        schema_sql_for_table(connection, "cursorDiskKV", ledger).map_err(|exc| match exc {
            rusqlite::Error::QueryReturnedNoRows => {
                "required table cursorDiskKV is missing".to_string()
            }
            other => other.to_string(),
        })?;

    let schema_sql = schema_sql.unwrap_or_default();
    let mut has_key = false;
    let mut has_value = false;
    for name in
        table_column_names(connection, "cursorDiskKV", ledger).map_err(|exc| format!("{exc:#}"))?
    {
        match name.as_str() {
            "key" => has_key = true,
            "value" => has_value = true,
            _ => {}
        }
    }

    if !has_key || !has_value {
        return Err(format!(
            "cursorDiskKV is missing required columns (key: {has_key}, value: {has_value})"
        ));
    }
    Ok(hash_str(&schema_sql))
}

/// The two census statements, shared with the plan assertion in
/// `cursor_relevant_key_count_is_a_covering_index_scan` so the statements that
/// test certifies are the statements this adapter actually runs. A copy in the
/// test would let the two drift and the certification would mean nothing.
///
/// Strictly greater, matching the census seed of the bare prefix — a key
/// exactly equal to the prefix is never scanned and must not be counted. Both
/// projections touch indexed columns only (`rowid` rides inside the unique
/// `key` index): adding any reference to `value` costs the covering index and
/// turns a 0.1 ms / 19 KB read into a 55 ms / 48 MB one (§1.1).
const CURSOR_RELEVANT_KEY_COUNT_SQL: &str =
    "SELECT count(*) FROM cursorDiskKV WHERE key > ?1 AND key < ?2";

const CURSOR_CENSUS_SQL: &str =
    "SELECT rowid, key FROM cursorDiskKV WHERE key > ?1 AND key < ?2 ORDER BY key LIMIT ?3";

fn count_relevant_keys(connection: &Connection) -> Result<usize> {
    let mut total = 0usize;
    for prefix in RELEVANT_PREFIXES {
        let count: i64 = connection
            .query_row(
                CURSOR_RELEVANT_KEY_COUNT_SQL,
                rusqlite::params![prefix, prefix_range_end(prefix)],
                |row| row.get(0),
            )
            .with_context(|| format!("failed counting keys for prefix {prefix}"))?;
        total = total.saturating_add(count.max(0) as usize);
    }
    Ok(total)
}

/// One censused relevant row: its key and the recency hint the candidate read
/// orders by. `rowid` is a sound positive mutation signal and a usable recency
/// ordering, **not** a watermark (§1.1) — nothing here skips on it.
struct CensusRow {
    rowid: i64,
    key: String,
}

/// Where a capped census stopped, in walk order: the index of the prefix
/// being walked and the last key actually included. Everything at or before
/// this point was censused; everything after was not.
struct CensusTruncation {
    prefix_idx: usize,
    last_key: String,
}

/// True when `key` falls inside the region a truncated census did cover, i.e.
/// its absence from the census genuinely means deletion.
fn census_covered(truncation: &CensusTruncation, key: &str) -> bool {
    let Some(idx) = RELEVANT_PREFIXES
        .iter()
        .position(|prefix| key.starts_with(prefix))
    else {
        // A stale entry from a key family this adapter no longer tracks: a
        // complete census would drop it, so a truncated one does too.
        return true;
    };
    idx < truncation.prefix_idx
        || (idx == truncation.prefix_idx && key <= truncation.last_key.as_str())
}

/// Walks both relevant key ranges over the covering `key` index, charging one
/// census row per key. Stops at `max_census_rows` — the §2.1 guard against
/// pathological keyspaces — returning the truncation point so the caller can
/// degrade coverage instead of failing.
///
/// `RELEVANT_PREFIXES` is walked in declaration order, which is also
/// lexicographic order of the ranges; the sweep ordering
/// (`next_cursor_sweep_item`) depends on the two agreeing.
fn census_relevant_keys(
    connection: &Connection,
    max_census_rows: u64,
    ledger: &mut ScanLedger,
) -> Result<(Vec<CensusRow>, Option<CensusTruncation>)> {
    let mut census = Vec::new();
    for (prefix_idx, prefix) in RELEVANT_PREFIXES.iter().enumerate() {
        let range_end = prefix_range_end(prefix);
        let mut last_key = prefix.to_string();
        loop {
            let mut page_rows = 0usize;
            let mut stmt = connection
                .prepare_cached(CURSOR_CENSUS_SQL)
                .context("failed to prepare census scan")?;
            let mut rows = stmt
                .query(rusqlite::params![
                    last_key,
                    range_end,
                    SCAN_PAGE_SIZE as i64
                ])
                .context("census scan query failed")?;
            while let Some(row) = rows.next().context("census scan row failed")? {
                let rowid: i64 = row.get(0).context("census rowid failed")?;
                let key: String = row.get(1).context("census key failed")?;
                // The key was materialized either way; charge it before the
                // cap decides whether it is included.
                ledger.charge_census_row(key.len());
                page_rows += 1;
                if census.len() as u64 >= max_census_rows {
                    return Ok((
                        census,
                        Some(CensusTruncation {
                            prefix_idx,
                            last_key,
                        }),
                    ));
                }
                last_key = key.clone();
                census.push(CensusRow { rowid, key });
            }
            if page_rows < SCAN_PAGE_SIZE {
                break;
            }
        }
    }
    Ok((census, None))
}

/// The candidate point read: one key's value, charged on the payload axis at
/// the read site. `Ok(None)` means the row vanished between census and read —
/// a mid-scan commit the data_version bracket rejects.
fn read_value_for_key(
    connection: &Connection,
    key: &str,
    ledger: &mut ScanLedger,
) -> Result<Option<Option<Vec<u8>>>> {
    let mut row_ledger = ScanLedger::default();
    let read = {
        let mut stmt = connection
            .prepare_cached("SELECT value FROM cursorDiskKV WHERE key = ?1")
            .context("failed to prepare candidate read")?;
        stmt.query_row(rusqlite::params![key], |row| {
            row_ledger.charge_payload_row();
            take_payload_blob(&mut row_ledger, row, 0)
        })
    };
    // Charged unconditionally: a row that materialized bytes and then failed
    // still cost them.
    ledger.payload_rows = ledger.payload_rows.saturating_add(row_ledger.payload_rows);
    ledger.payload_bytes = ledger
        .payload_bytes
        .saturating_add(row_ledger.payload_bytes);
    match read {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(exc) => Err(exc).context("candidate read failed"),
    }
}

/// The Cursor adapter's sweep reader: the first relevant key strictly after
/// `after` in key order, read in full, verified against this poll's hash view,
/// and re-emitted when it disagrees (§2.2). With today's exhaustive fast path
/// the disagreement arm is reachable only through hashes carried forward by an
/// earlier degraded poll; WI-08's census fast path makes it the sweep's whole
/// job (G5d), and the driver contract here does not change when it does.
fn next_cursor_sweep_item(
    connection: &Connection,
    after: &str,
    hashes: &mut BTreeMap<String, String>,
    records: &mut Vec<SyntheticRecord>,
    workspace_cache: &mut HashMap<String, Option<String>>,
    slice: &mut ScanLedger,
) -> Result<Option<SweepItem>> {
    for prefix in RELEVANT_PREFIXES {
        let range_end = prefix_range_end(prefix);
        if after >= range_end.as_str() {
            continue;
        }
        let start = if after < *prefix { prefix } else { after };
        let read = {
            let mut stmt = connection
                .prepare_cached(
                    "SELECT key, value FROM cursorDiskKV \
                     WHERE key > ?1 AND key < ?2 ORDER BY key LIMIT 1",
                )
                .context("failed to prepare sweep read")?;
            stmt.query_row(rusqlite::params![start, range_end], |row| {
                // This projection includes `value`, so the whole row is a
                // payload read charged to the slice at the read site.
                slice.charge_payload_row();
                let key = take_payload_required_string(slice, row, 0)?;
                let value = take_payload_blob(slice, row, 1)?;
                Ok((key, value))
            })
        };
        match read {
            Ok((key, value)) => {
                let bytes = value.unwrap_or_default();
                let payload_bytes = bytes.len() as u64;
                let hash = format!("{:016x}", hash_bytes(&bytes));
                if hashes.get(&key) != Some(&hash) {
                    if !bytes.is_empty() {
                        if let Some(mut record) = synthesize_cursor_sqlite_record(&key, &bytes) {
                            stamp_bubble_workspace(connection, workspace_cache, &mut record, slice);
                            records.push(record);
                        }
                    }
                    hashes.insert(key.clone(), hash);
                }
                return Ok(Some(SweepItem {
                    position: key,
                    payload_bytes,
                }));
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => continue,
            Err(exc) => return Err(exc).context("sweep read failed"),
        }
    }
    Ok(None)
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
    ledger: &mut ScanLedger,
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
    let workspace = match cache.get(&composer_id) {
        Some(cached) => cached.clone(),
        None => {
            // A cache miss is a real second payload read of the composer blob
            // (up to 2.4 MB on the reference host) and is charged as one.
            let resolved = lookup_composer_workspace(connection, &composer_id, ledger);
            cache.insert(composer_id.clone(), resolved.clone());
            resolved
        }
    };
    if let Some(path) = workspace {
        record.insert("workspacePath".to_string(), json!(path));
    }
}

/// Best-effort read of `workspaceIdentifier.uri.fsPath` from a bubble's
/// parent `composerData:` blob. Works even for composers the synthesizer
/// defers (no positive `createdAt` yet): Cursor writes the workspace
/// identifier at creation. Any failure resolves to `None`.
fn lookup_composer_workspace(
    connection: &Connection,
    composer_id: &str,
    ledger: &mut ScanLedger,
) -> Option<String> {
    // `query_row` holds the closure's borrow for its whole call, so the read
    // is charged into a scratch ledger and folded in unconditionally — a row
    // that materialized bytes and then failed to parse still cost them.
    let mut row_ledger = ScanLedger::default();
    let read = connection.query_row(
        "SELECT value FROM cursorDiskKV WHERE key = ?1",
        rusqlite::params![format!("composerData:{composer_id}")],
        |row| {
            row_ledger.charge_payload_row();
            Ok(take_payload_blob(&mut row_ledger, row, 0)?.unwrap_or_default())
        },
    );
    ledger.payload_rows = ledger.payload_rows.saturating_add(row_ledger.payload_rows);
    ledger.payload_bytes = ledger
        .payload_bytes
        .saturating_add(row_ledger.payload_bytes);
    let bytes: Vec<u8> = read.ok()?;
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
    use crate::WorkTrigger;
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

    /// The shipped-default scan plan, no sweep: what a non-reconcile poll of
    /// a production config runs with.
    fn default_scan_plan() -> CursorScanPlan {
        CursorScanPlan::from_config(&moraine_config::AppConfig::default(), None)
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
            trigger: WorkTrigger::Watcher,
        }
    }

    fn reconcile_work(path: &Path) -> WorkItem {
        WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..sqlite_work(path)
        }
    }

    static TOUCH_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// Move the database's stat fingerprint without touching any relevant key
    /// — the production shape of Cursor's constant `ItemTable` churn — so a
    /// test can drive repeated polls past the cheap stat short-circuit.
    fn touch_irrelevant(path: &Path) {
        let sequence = TOUCH_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let connection = Connection::open(path).expect("open fixture for irrelevant touch");
        connection
            .execute(
                "INSERT INTO ItemTable (key, value) VALUES (?1, ?2)",
                rusqlite::params![format!("touch-{sequence}"), "x"],
            )
            .expect("insert irrelevant row");
    }

    /// `count` relevant keys whose values are deliberately non-JSON, so every
    /// scan of them emits nothing: the quiet-database shape sweep eligibility
    /// condition 3 requires, with real payload bytes for the slices to read.
    /// Keys are zero-padded so key order equals numeric order.
    fn seed_junk_fixture(path: &PathBuf, count: usize, value_bytes: usize) -> Vec<String> {
        let db = create_kv_db(path);
        let mut keys = Vec::new();
        for idx in 0..count {
            let key = format!("bubbleId:{COMPOSER_ID}:k{idx:03}");
            // Exactly `value_bytes` per value, so byte-denominated assertions
            // (cycle totals, projections) stay arithmetic rather than fuzzy.
            let prefix = format!("junk-{idx:03}-");
            let value = format!("{prefix}{}", "j".repeat(value_bytes - prefix.len()));
            db.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, value],
            )
            .expect("insert junk kv row");
            keys.push(key);
        }
        keys
    }

    fn sweep_slices(metrics: &Arc<Metrics>) -> u64 {
        metrics
            .sqlite_sweep_slices_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn sweep_rows(metrics: &Arc<Metrics>) -> u64 {
        metrics
            .sqlite_sweep_rows_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn degraded_scans(metrics: &Arc<Metrics>) -> u64 {
        metrics
            .sqlite_coverage_degraded_scans_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The persisted sweep cursor for `work`'s committed checkpoint.
    async fn persisted_sweep(
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        work: &WorkItem,
    ) -> SweepState {
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("committed checkpoint");
        CursorState::parse(&checkpoint.cursor_json).sweep
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
        run_poll_with_state_and_metrics(
            work,
            checkpoints,
            poll_state,
            &Arc::new(Metrics::default()),
        )
        .await
    }

    /// Shares one `Metrics` across several polls so a test can count the scans
    /// that actually ran (`sqlite_scan_failures_total`) rather than the errors
    /// that were reported, which are deliberately rate-limited.
    async fn run_poll_with_state_and_metrics(
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        poll_state: &VolatilePollMap,
        metrics: &Arc<Metrics>,
    ) -> Vec<RowBatch> {
        run_poll_with_config(
            &moraine_config::AppConfig::default(),
            work,
            checkpoints,
            poll_state,
            metrics,
        )
        .await
    }

    /// The fully-parameterized runner: budget and sweep tests inject their
    /// `[ingest.sqlite]` values here, exactly as an operator's config would.
    async fn run_poll_with_config(
        config: &moraine_config::AppConfig,
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        poll_state: &VolatilePollMap,
        metrics: &Arc<Metrics>,
    ) -> Vec<RowBatch> {
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        let process = process_cursor_sqlite_db(
            config,
            work,
            checkpoints.clone(),
            poll_state,
            sink_tx,
            metrics,
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

    fn payload_rows(metrics: &Arc<Metrics>) -> u64 {
        metrics
            .sqlite_poll_payload_rows_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn payload_bytes(metrics: &Arc<Metrics>) -> u64 {
        metrics
            .sqlite_poll_payload_bytes_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Issue #601 §2.5. The ladder itself — nothing asserted its shape, so
    /// `failure_backoff` could return `Duration::MAX` for every non-zero `n`
    /// with the whole suite still green. That is the latch class inverted: a
    /// permanently broken database would never retry, and §2.5's stated
    /// purpose for the cap ("recovery from an environmental fault never needs
    /// a restart") could not fail.
    ///
    /// Fails for: replacing the body with a constant, dropping the doubling,
    /// dropping `.min(31)` (`1u32 << 32` overflows in debug), or removing the
    /// `FAILURE_BACKOFF_MAX` clamp.
    #[test]
    fn failure_backoff_doubles_from_the_base_and_saturates_at_the_cap() {
        assert_eq!(
            failure_backoff(0),
            Duration::ZERO,
            "no failures means retry immediately"
        );
        assert_eq!(failure_backoff(1), Duration::from_secs(15));
        assert_eq!(failure_backoff(2), Duration::from_secs(30));
        assert_eq!(failure_backoff(3), Duration::from_secs(60));
        assert_eq!(failure_backoff(6), Duration::from_secs(15 * 32));
        // 15 s * 2^6 = 960 s overshoots the 900 s ceiling, so n = 7 is the
        // first genuinely clamped step rather than a coincidence.
        assert!(Duration::from_secs(15 * 64) > FAILURE_BACKOFF_MAX);
        assert_eq!(failure_backoff(7), FAILURE_BACKOFF_MAX);
        assert_eq!(failure_backoff(60), FAILURE_BACKOFF_MAX);
        // The shift clamp: without `.min(31)` this panics on overflow.
        assert_eq!(failure_backoff(u32::MAX), FAILURE_BACKOFF_MAX);
        assert_eq!(
            FAILURE_BACKOFF_MAX,
            Duration::from_secs(15 * 60),
            "a permanently broken database still retries every 15 minutes"
        );
    }

    /// Issue #601 §3.2 — the *contention* ladder's shape. It was mirrored from
    /// the fault ladder above for its lower bound (it throttles) and its upper
    /// bound (it does not latch), but not for its **width**, and only the base
    /// was pinned — incidentally, through
    /// `a_contended_replacement_replay_throttles_its_barrier`'s single
    /// `age_for_tests(.., 16 s)` step. That test never reaches
    /// `consecutive_contended_scans >= 3`, so everything above the first step
    /// was free: `CONTENTION_BACKOFF_MAX` could be set to `FAILURE_BACKOFF_MAX`
    /// — literally the fifteen minutes the constant's own doc says must not
    /// happen — or collapsed onto the base so the ladder cannot grow at all, or
    /// the doubling removed outright, with the whole suite green.
    ///
    /// Fails for: any of those three, moving the base, or dropping `.min(31)`
    /// (`1u32 << 32` overflows in debug).
    #[test]
    fn contention_backoff_doubles_from_the_base_and_saturates_at_a_minute() {
        assert_eq!(
            contention_backoff(0),
            Duration::ZERO,
            "no contention means retry immediately"
        );
        assert_eq!(contention_backoff(1), Duration::from_secs(15));
        assert_eq!(contention_backoff(2), Duration::from_secs(30));
        // 15 s * 2^2 = 60 s meets the ceiling exactly, so n = 4 is the first
        // genuinely clamped step rather than a coincidence.
        assert_eq!(contention_backoff(3), Duration::from_secs(60));
        assert!(Duration::from_secs(15 * 8) > CONTENTION_BACKOFF_MAX);
        assert_eq!(contention_backoff(4), CONTENTION_BACKOFF_MAX);
        assert_eq!(contention_backoff(60), CONTENTION_BACKOFF_MAX);
        // The shift clamp: without `.min(31)` this panics on overflow.
        assert_eq!(contention_backoff(u32::MAX), CONTENTION_BACKOFF_MAX);
        assert_eq!(
            CONTENTION_BACKOFF_MAX,
            Duration::from_secs(60),
            "a replacement database that is merely busy must become visible \
             within a minute of the writer pausing"
        );
        assert!(
            CONTENTION_BACKOFF_MAX < FAILURE_BACKOFF_MAX,
            "…not within fifteen — the two ladders are separate precisely \
             because contention is transient and self-clearing and a fault is \
             not, so collapsing the ceilings together erases the distinction"
        );
    }

    /// Issue #601 §2.5, the escalation the ladder exists for.
    ///
    /// The `clear(); record_failed_scan();` pair the blocked-replay path used
    /// to run is indistinguishable from a single `record_failed_scan` *inside
    /// one backoff window*, which is why a test driving ten retries in fifteen
    /// seconds proved nothing. Backdating the entry's clock makes the
    /// difference observable: after two failures the window is 30 s, so
    /// sixteen seconds of elapsed time must **not** be enough.
    ///
    /// Fails for: resetting the streak on each failure (every window stays
    /// 15 s, so the 16 s probe is due), or dropping the escalation entirely.
    #[test]
    fn repeated_failures_escalate_the_retry_window() {
        let map = VolatilePollMap::new();
        let key = "escalation-key";

        map.record_failed_scan(key, 1);
        assert_eq!(map.consecutive_failed_scans(key), 1);
        assert!(
            !map.failure_retry_due(key, 1),
            "inside the first 15 s window"
        );
        map.age_for_tests(key, Duration::from_secs(16));
        assert!(map.failure_retry_due(key, 1), "15 s window has expired");

        map.record_failed_scan(key, 1);
        assert_eq!(map.consecutive_failed_scans(key), 2);
        map.age_for_tests(key, Duration::from_secs(16));
        assert!(
            !map.failure_retry_due(key, 1),
            "the second window is 30 s, so 16 s is not yet due — this is the \
             assertion a reset-to-1 bug fails"
        );
        map.age_for_tests(key, Duration::from_secs(16));
        assert!(map.failure_retry_due(key, 1), "32 s > 30 s");

        // And `clear` genuinely does restart the ladder: the defect the
        // blocked-replay path shipped, pinned here so a reintroduction is a
        // visible behavioral claim rather than an invisible one.
        map.record_failed_scan(key, 1);
        assert_eq!(map.consecutive_failed_scans(key), 3);
        map.clear(key);
        map.record_failed_scan(key, 1);
        assert_eq!(
            map.consecutive_failed_scans(key),
            1,
            "clear() before record_failed_scan() resets the ladder — which is \
             why `record_blocked_replay` must never do it"
        );
    }

    /// Issue #601 §6 / §3.2. A mixed-snapshot rejection means the database was
    /// being written while the scan read it. That is **contention**, not a
    /// fault: it happens precisely when the source is active, which is when
    /// prompt visibility matters most. Routing it into the 15 s → 15 min fault
    /// ladder would regress active-session freshness by up to fifteen minutes,
    /// and the mitigation the spec pairs with that ("smaller scans make retries
    /// rare") is WI-07/WI-08 and does not exist yet.
    ///
    /// Fails for: routing `sqlite_mixed_snapshot` through the fault ladder, or
    /// making the exemption swallow a genuine fault's streak.
    #[test]
    fn mixed_snapshot_rejection_does_not_escalate_the_failure_backoff() {
        let stat = StatFingerprint::default();
        let map = VolatilePollMap::new();
        let key = "contended-key";

        for _ in 0..5 {
            map.record_scan_failure_outcome(key, 1, ERROR_KIND_MIXED_SNAPSHOT);
        }
        assert_eq!(
            map.consecutive_failed_scans(key),
            0,
            "contention must not build a fault streak"
        );
        assert!(
            !map.should_skip_poll(key, 1, &stat),
            "a contended database retries at the ordinary poll cadence"
        );

        // A genuine fault still escalates, and a contended retry underneath it
        // neither extends nor clears it.
        map.record_scan_failure_outcome(key, 1, ERROR_KIND_SCHEMA);
        map.record_scan_failure_outcome(key, 1, ERROR_KIND_SCHEMA);
        assert_eq!(map.consecutive_failed_scans(key), 2);
        map.record_scan_failure_outcome(key, 1, ERROR_KIND_MIXED_SNAPSHOT);
        assert_eq!(
            map.consecutive_failed_scans(key),
            2,
            "contention leaves a genuine fault ladder exactly where it was"
        );
        assert!(
            map.should_skip_poll(key, 1, &stat),
            "the fault ladder holds"
        );
    }

    /// Issue #601 §3.2 / §2.5 — the classifier's **extent**, per error kind
    /// rather than per outcome.
    ///
    /// `mixed_snapshot_rejection_does_not_escalate_the_failure_backoff` bounds
    /// the predicate from beneath (the named kind is exempt) and at one
    /// neighbour (`sqlite_schema_mismatch` is not). It does not bound the
    /// width, and two of the four neighbours were open: widening to
    /// `|| error_kind == ERROR_KIND_SCAN` and to
    /// `|| error_kind == ERROR_KIND_TOO_LARGE` were both green. The first is
    /// the expensive one — `sqlite_scan_error` is emitted at eleven of the
    /// seventeen production failure sites — and it silently restores the exact
    /// §2.5 defect: no fault streak, so `should_skip_poll` never backs off and
    /// a full failed scan re-runs on every reconcile tick and every debounced
    /// watcher event, while the durable barrier's throttle collapses from
    /// fifteen minutes to sixty seconds.
    ///
    /// Every kind is asserted on both clocks *and* on the gate the clock
    /// serves, so there is no neighbour left to widen into and no direction to
    /// narrow in.
    ///
    /// Fails for: routing any fault kind to the contention clock, routing
    /// `sqlite_mixed_snapshot` to the fault ladder, or collapsing the split.
    #[test]
    fn each_error_kind_routes_to_exactly_one_backoff_clock() {
        let stat = StatFingerprint::default();

        for kind in [
            ERROR_KIND_OPEN,
            ERROR_KIND_SCHEMA,
            ERROR_KIND_TOO_LARGE,
            ERROR_KIND_SCAN,
        ] {
            let map = VolatilePollMap::new();
            let key = "fault-routing";
            map.record_scan_failure_outcome(key, 1, kind);
            assert_eq!(
                map.consecutive_failed_scans(key),
                1,
                "{kind} is a fault and must climb the fault ladder"
            );
            assert_eq!(
                map.consecutive_contended_scans(key),
                0,
                "{kind} is not contention and must not move the contention clock"
            );
            // The consequence, at the gate §2.5 exists to serve: with no fault
            // streak the very next tick re-runs the whole failed scan.
            assert!(
                map.should_skip_poll(key, 1, &stat),
                "{kind} must make the next ordinary poll back off"
            );
        }

        let map = VolatilePollMap::new();
        let key = "contention-routing";
        map.record_scan_failure_outcome(key, 1, ERROR_KIND_MIXED_SNAPSHOT);
        assert_eq!(
            map.consecutive_failed_scans(key),
            0,
            "contention is the one kind exempt from the fault ladder (§3.2)"
        );
        assert_eq!(
            map.consecutive_contended_scans(key),
            1,
            "…and the one kind that moves the contention clock"
        );
        assert!(
            !map.should_skip_poll(key, 1, &stat),
            "a contended database keeps scanning at the ordinary poll cadence"
        );
    }

    /// Issue #601 §3.2 — the mixed-snapshot bracket's **extent**, one disjunct
    /// at a time.
    ///
    /// The bracket is the production trigger for the entire contention
    /// feature, and no test in any of the three adapters reached it through a
    /// real disjunct: every one arms `contention_injection`, and
    /// `forced_mixed_snapshot` short-circuits the rest of the expression. So
    /// all three brackets could be reduced to the bare `#[cfg(test)]` hook —
    /// deleting *both* real disjuncts — with the suite green in each adapter,
    /// and dropping either one alone was green too.
    ///
    /// Why the guard is bounded here rather than through a live scan is
    /// recorded on `snapshot_is_mixed` itself: a commit landing between a
    /// scan's two `data_version` reads is not deterministically reachable, and
    /// racing for it would give a probabilistic test that also cannot say which
    /// disjunct fired.
    ///
    /// Fails for: dropping the `data_version` disjunct, dropping the stat
    /// disjunct, or widening the predicate to fire unconditionally.
    #[test]
    fn the_mixed_snapshot_bracket_fires_on_a_moved_data_version_and_on_a_moved_stat() {
        let path = unique_db_path("mixed-snapshot-extent");
        std::fs::write(&path, b"a file standing in for a database").expect("seed probe file");
        let db_path = path.to_string_lossy().to_string();
        let opened = stat_fingerprint(&db_path).expect("the probe file exists");

        // Upper bound: a quiet database is a clean scan. Widening the predicate
        // to fire unconditionally dies here, and so does any rewrite that
        // rejects a scan nothing touched.
        assert!(
            !snapshot_is_mixed(&db_path, 7, 7, opened),
            "an unchanged data_version over an unchanged file is a clean scan"
        );

        // Lower bound A — a writer committed during the scan. `data_version`
        // moved while the file did not: the WAL case, where the commit lands in
        // the `-wal` and a coarse-resolution stat can still compare equal.
        assert!(
            snapshot_is_mixed(&db_path, 7, 8, opened),
            "a data_version that moved during the scan is a torn read"
        );

        // Lower bound B — the file moved under the scan while `data_version`
        // did not: a checkpoint, a rotation, or a wholesale replacement.
        std::fs::write(&path, b"a file standing in for a database, now longer")
            .expect("grow the probe file");
        let grown = stat_fingerprint(&db_path).expect("the probe file still exists");
        assert_ne!(
            grown, opened,
            "the probe must actually move the fingerprint, or bound B proves nothing"
        );
        assert!(
            snapshot_is_mixed(&db_path, 7, 7, opened),
            "a file that changed under the scan is a torn read even when \
             data_version did not move"
        );

        cleanup(&path);
    }

    /// Issue #601 §2.0. The ledger is **caller-owned** precisely so that a scan
    /// which reads 48 MB and then loses the mixed-snapshot race is still
    /// charged for what it read. Nothing asserted that: every ledger test
    /// destructured `Scanned` and panicked otherwise, so resetting the ledger
    /// on any failure arm left the suite green and the guarantee unverified.
    ///
    /// This drives the one post-read failure arm Cursor has. The contention is
    /// **injected**, not raced for: the bracket sits after the paged read, so
    /// arming it reproduces exactly the state a writer committing mid-scan
    /// produces — a full ledger and a rejected outcome — without the retry loop
    /// the earlier version needed, which could fail for reasons unrelated to
    /// the code it guards.
    ///
    /// Fails for: resetting or rebuilding the ledger on the mixed-snapshot
    /// return.
    #[test]
    fn a_failed_scan_still_reports_the_bytes_it_had_already_read() {
        let path = unique_db_path("failure-arm-ledger");
        let db = create_kv_db(&path);
        db.execute_batch("BEGIN").expect("begin seed");
        for idx in 0..8 {
            put(
                &db,
                &format!("bubbleId:{COMPOSER_ID}:seed{idx:04}"),
                &json!({"_v": 3, "type": 1, "bubbleId": USER_BUBBLE_ID,
                        "createdAt": "2026-05-08T02:04:37.835Z",
                        "text": "y".repeat(96 * 1024)}),
            );
        }
        db.execute_batch("COMMIT").expect("commit seed");
        drop(db);

        let db_path = path.to_string_lossy().to_string();
        contention_injection::arm(&db_path, 1);
        let mut ledger = ScanLedger::default();
        let outcome = scan_database(
            &db_path,
            &CursorState::fresh(),
            &default_scan_plan(),
            &mut ledger,
        );
        assert!(
            matches!(
                outcome,
                ScanOutcome::Failed {
                    error_kind: ERROR_KIND_MIXED_SNAPSHOT,
                    ..
                }
            ),
            "the armed scan must reach the mixed-snapshot arm; without observing \
             the arm this test proves nothing"
        );
        assert!(
            ledger.payload_rows > 0,
            "the rejected scan had already read rows"
        );
        assert!(
            ledger.payload_bytes > 64 * 1024,
            "and it must still be charged for the bytes it read; got {}",
            ledger.payload_bytes
        );

        // Armings are one-shot and path-keyed: the very next scan of the same
        // database succeeds, so nothing leaks into another test.
        let mut clean = ScanLedger::default();
        assert!(matches!(
            scan_database(
                &db_path,
                &CursorState::fresh(),
                &default_scan_plan(),
                &mut clean
            ),
            ScanOutcome::Scanned { .. }
        ));

        cleanup(&path);
    }

    /// Issue #601 §2.5 / §3.2. Mixed-snapshot rejections are exempt from the
    /// **fault ladder**; they must never be exempt from the **replay throttle**.
    ///
    /// A replacement replay is the longest scan an adapter runs — cold, whole
    /// database — so it is the scan most likely to lose the `data_version`/stat
    /// bracket in the first place. Its `Failed` arm emits a durable
    /// `BlockReplay` plus an append-only `ingest_errors` row, and the next tick
    /// sees `retry_blocked_replay` true with `starts_replacement` false (the
    /// blocked checkpoint carries the current inode and exclusions hash). If
    /// the exemption leaves no clock behind, `failure_retry_due` says "due" and
    /// the pre-barrier throttle never fires: `BeginReplay` + full cold scan +
    /// `BlockReplay` + one error row **per poll, forever**, at the reconcile
    /// cadence and on every 50 ms-debounced watcher event.
    ///
    /// Driven through an actual replay rather than `VolatilePollMap` directly,
    /// because the barrier — not the map — is what was unguarded.
    ///
    /// Fails for: `record_scan_failure_outcome` returning without recording
    /// anything for `ERROR_KIND_MIXED_SNAPSHOT`, or `failure_retry_due`
    /// ignoring the contention clock.
    #[tokio::test]
    async fn a_contended_replacement_replay_throttles_its_barrier() {
        let path = unique_db_path("contended-replay-barrier");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let db_path = work.path.clone();
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        // Cold poll commits a checkpoint under the default exclusion policy.
        run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(checkpoints.read().await.contains_key(&cp_key));

        // Changing the exclusion set starts a replacement replay. Every scan
        // from here loses the mixed-snapshot bracket, as a busy database's
        // cold re-read would.
        let mut replaying = moraine_config::AppConfig::default();
        replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
        contention_injection::arm(&db_path, 64);
        let block_poll = |config: moraine_config::AppConfig| {
            let work = work.clone();
            let checkpoints = checkpoints.clone();
            let poll_state = poll_state.clone();
            let metrics = metrics.clone();
            async move { run_replay_poll(&config, &work, &checkpoints, &poll_state, &metrics).await }
        };

        assert!(
            block_poll(replaying.clone()).await,
            "the contended replay blocks durably"
        );
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            0,
            "contention must not climb the fault ladder (§3.2)"
        );
        assert_eq!(
            poll_state.consecutive_contended_scans(&cp_key),
            1,
            "…but it must leave a clock behind"
        );

        // The retry is throttled: no barrier, no scan, no second error row.
        let scans_before = payload_rows(&metrics);
        assert!(
            !block_poll(replaying.clone()).await,
            "a contended blocked replay must not re-send BeginReplay/BlockReplay \
             on the very next tick"
        );
        assert_eq!(
            payload_rows(&metrics),
            scans_before,
            "and must not re-read the whole database either"
        );

        // Ten more ticks change nothing — this is the "per poll, forever" shape.
        for _ in 0..10 {
            assert!(!block_poll(replaying.clone()).await);
        }
        assert_eq!(payload_rows(&metrics), scans_before);

        // Once the contention window expires the retry runs again, so recovery
        // is not sacrificed: the clock throttles, it does not latch.
        poll_state.age_for_tests(&cp_key, Duration::from_secs(16));
        assert!(
            block_poll(replaying).await,
            "an expired contention window must let the replay retry"
        );
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            0,
            "still not a fault"
        );
        assert_eq!(poll_state.consecutive_contended_scans(&cp_key), 2);

        contention_injection::disarm(&db_path);
        cleanup(&path);
    }

    /// Issue #601 §3.2/§6 — the direction
    /// `a_contended_replacement_replay_throttles_its_barrier` does **not**
    /// bound. That test proves a contended *replay barrier* is throttled; this
    /// one proves an ordinary poll of the same database is not, which is the
    /// half the exemption exists for.
    ///
    /// The two clocks are read in different places on purpose:
    /// `failure_retry_due` (the barrier) consults `last_contended_at`,
    /// `should_skip_poll` (ordinary scans) deliberately does not. A database is
    /// contended precisely because someone is writing to it, so throttling its
    /// ordinary polls regresses freshness on the only sessions anyone is
    /// watching. Nothing pinned that asymmetry at a **call site** — the
    /// predicates could be asserted directly, but the short-circuits could not
    /// — which is how the short-circuit comments were able to keep instructing
    /// WI-04 to add §2.5's `|| !failure_retry_due` disjunct after §3.2 made it
    /// outcome-changing. That disjunct puts the barrier's clock in front of
    /// every ordinary poll.
    ///
    /// Fails for: adding `|| !poll_state.failure_retry_due(..)` to the cheap
    /// no-change short-circuit (the second contended scan never runs, and the
    /// writer's next edit stays invisible for up to 60 s), or moving the
    /// contention clock into `should_skip_poll`.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_ordinary_poll_of_a_contended_database_is_not_throttled() {
        let path = unique_db_path("contended-ordinary-poll");
        let db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let db_path = work.path.clone();
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        let cold =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(
            !all_event_rows(&cold).is_empty(),
            "the cold poll emits the fixture"
        );

        // The session is live. The writer commits, and the next two scans lose
        // the mixed-snapshot bracket to it.
        contention_injection::arm(&db_path, 2);
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("running", false),
        );
        run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            poll_state.consecutive_contended_scans(&cp_key),
            1,
            "the first scan is rejected by the bracket"
        );
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            0,
            "contention is not a fault (§3.2)"
        );

        // Immediately — far inside the 15 s contention window the barrier is
        // now serving — the next ordinary poll must still read the database.
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{THINKING_BUBBLE_ID}"),
            &thinking_bubble_value(),
        );
        run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            poll_state.consecutive_contended_scans(&cp_key),
            2,
            "an ordinary poll of a contended database must not be throttled — \
             the second scan has to run at the ordinary poll cadence"
        );
        assert_eq!(
            metrics
                .sqlite_scan_failures_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "…and must reach the scan, not return before it"
        );

        // The writer's edit lands and the contention clears. This poll is still
        // well inside the contention window, and it is the poll that makes an
        // active session visible.
        contention_injection::disarm(&db_path);
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("completed", true),
        );
        let fresh =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            event_uid_by_kind(&all_event_rows(&fresh), "tool_result").len(),
            1,
            "a contended database's next write must be visible at the ordinary \
             poll cadence, not after a 60 s barrier backoff"
        );

        cleanup(&path);
    }

    /// Issue #601 §1.1. `count_relevant_keys` is Cursor's only census-shaped
    /// read today: two range scans over the unique `key` index, measured at
    /// 0.056 ms / 19 KB for 247 keys on the reference host **because the index
    /// covers them**. Nothing pinned that, so the same statement degrading to
    /// a table scan — 48 MB and 55 ms on that host — would be invisible.
    ///
    /// It stays a scalar aggregate, so by the `ScanLedger` charging rules it
    /// materializes no row and is charged on neither axis. This plan assertion
    /// is what keeps that silence honest rather than an unstated cost; see the
    /// matching note on `ledger_charges_payload_bytes_at_the_read_site`.
    ///
    /// Fails for: widening the count's projection or predicate such that
    /// SQLite stops using the covering index.
    #[test]
    fn cursor_relevant_key_count_is_a_covering_index_scan() {
        let path = unique_db_path("census-plan");
        let db = seed_fixture_db(&path);

        for prefix in RELEVANT_PREFIXES {
            for (statement, uses_limit) in [
                (CURSOR_RELEVANT_KEY_COUNT_SQL, false),
                // The row census projects `(rowid, key)`: rowid rides inside
                // the unique key index, so it must stay covering too.
                (CURSOR_CENSUS_SQL, true),
            ] {
                let plan: Vec<String> = {
                    let mut stmt = db
                        .prepare(&format!("EXPLAIN QUERY PLAN {statement}"))
                        .expect("prepare explain");
                    let mut params: Vec<rusqlite::types::Value> = vec![
                        rusqlite::types::Value::from((*prefix).to_string()),
                        rusqlite::types::Value::from(prefix_range_end(prefix)),
                    ];
                    if uses_limit {
                        params.push(rusqlite::types::Value::from(SCAN_PAGE_SIZE as i64));
                    }
                    stmt.query_map(rusqlite::params_from_iter(params), |row| {
                        row.get::<_, String>(3)
                    })
                    .expect("query plan")
                    .map(|row| row.expect("plan detail"))
                    .collect()
                };
                let detail = plan.join("; ");
                assert!(
                    detail.contains("COVERING INDEX"),
                    "the relevant-key census must stay index-covered for {prefix}; \
                     plan was: {detail}"
                );
                assert!(
                    !detail.contains("SCAN cursorDiskKV"),
                    "the census must never degrade to a table scan for {prefix}; \
                     plan was: {detail}"
                );
            }
        }

        drop(db);
        cleanup(&path);
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

    /// Issue #601 §8. `second.is_empty()` alone is satisfied *equally* by the
    /// cheap stat short-circuit and by a complete re-scan that finds nothing —
    /// deleting the `state.stat == current_stat` short-circuit left this test
    /// green, which made it a guard that could not fail for the property it is
    /// named after. `sqlite_poll_payload_rows_total` is the instrument that
    /// distinguishes them: a poll that short-circuited read no rows.
    ///
    /// **[DIVERGENT FIXTURE]** each poll gets a *fresh* `VolatilePollMap`, so
    /// the issue-#443 volatile short-circuit cannot stand in for the durable
    /// one and mask the mutation.
    ///
    /// Fails for: deleting the `state.stat == current_stat` short-circuit.
    #[tokio::test(flavor = "multi_thread")]
    async fn unchanged_db_is_a_noop_on_the_next_poll() {
        let path = unique_db_path("noop");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());

        let first =
            run_poll_with_state_and_metrics(&work, &checkpoints, &VolatilePollMap::new(), &metrics)
                .await;
        assert!(!first.is_empty());
        let read_after_first = payload_rows(&metrics);
        let bytes_after_first = payload_bytes(&metrics);
        assert!(
            read_after_first > 0 && bytes_after_first > 0,
            "the cold poll must actually read rows and bytes, or the comparison \
             below is vacuous"
        );

        let second =
            run_poll_with_state_and_metrics(&work, &checkpoints, &VolatilePollMap::new(), &metrics)
                .await;
        assert!(
            second.is_empty(),
            "unchanged database must produce zero batches; got {}",
            second.len()
        );
        assert_eq!(
            payload_rows(&metrics),
            read_after_first,
            "an unchanged database must not be re-read at all — zero batches is \
             also what a full re-scan finding nothing produces"
        );
        assert_eq!(
            payload_bytes(&metrics),
            bytes_after_first,
            "both axes: rows cannot catch content growth, bytes cannot catch a \
             scan of narrow rows"
        );

        cleanup(&path);
    }

    /// Issue #601 §8 and issue #443. Every "sends nothing" assertion here is
    /// also satisfied by a full re-scan that finds nothing, so each step now
    /// states what it read as well as what it sent — that is the only way the
    /// two short-circuits (durable stat, volatile stat coverage) become
    /// mutation-provable at all.
    ///
    /// Fails for: deleting the `state.stat == current_stat` short-circuit
    /// (the fifth poll re-reads), or deleting the `should_skip_poll` call (the
    /// third poll re-reads).
    #[tokio::test(flavor = "multi_thread")]
    async fn irrelevant_write_scans_but_persists_no_checkpoint() {
        let path = unique_db_path("noop-checkpoint");
        let db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        let first =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(!first.is_empty());
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let baseline = checkpoints
            .read()
            .await
            .get(&cp_key)
            .cloned()
            .expect("committed checkpoint after first poll");
        let read_after_first = payload_rows(&metrics);
        assert!(read_after_first > 0);

        // Cursor constantly rewrites non-transcript keys (issue #443): the
        // stat fingerprint moves but no relevant key changes.
        put(&db, "agentKv:blob:0000", &json!({"opaque": "blob"}));

        let second =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(
            second.is_empty(),
            "a no-op scan must send nothing durable; got {} batches",
            second.len()
        );
        let read_after_second = payload_rows(&metrics);
        assert!(
            read_after_second > read_after_first,
            "the stat moved, so this poll genuinely re-scanned — the test name's \
             'scans but persists no checkpoint' has to be observable"
        );

        // The same stat fingerprint is now covered by volatile state, so a
        // re-poll without further writes also sends nothing *and reads
        // nothing*.
        let third =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(
            third.is_empty(),
            "volatile stat coverage must short-circuit"
        );
        assert_eq!(
            payload_rows(&metrics),
            read_after_second,
            "volatile coverage must skip the scan, not just its output"
        );

        // A relevant write below the backoff threshold is picked up
        // immediately and persists a durable checkpoint again.
        put(
            &db,
            &format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}"),
            &tool_bubble_value("completed", true),
        );
        let fourth =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
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

        // The emitting scan cleared the volatile entry, so only the *durable*
        // stat short-circuit can suppress this last poll. It is the arm the
        // volatile map cannot stand in for.
        let read_after_fourth = payload_rows(&metrics);
        let fifth =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(fifth.is_empty());
        assert_eq!(
            payload_rows(&metrics),
            read_after_fourth,
            "with no volatile entry left, the durable stat short-circuit is the \
             only thing that can stop the re-read"
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
        map.record_failed_scan(key, 1);
        assert!(
            map.should_skip_poll(key, 1, &stat(1)),
            "a first failed scan, with no prior entry, still starts a backoff \
             (issue #601 §2.5 — this assertion is the inverse of the one it \
             replaced, which pinned the unthrottled retry as correct)"
        );

        map.clear(key);
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

        // A failed scan on an established streak keeps the database throttled
        // and drops the covered stat: a failure covered nothing, so claiming
        // coverage would suppress rescans of an unchanged file permanently
        // instead of for the backoff window.
        map.record_failed_scan(key, 1);
        assert!(
            map.should_skip_poll(key, 1, &stat(5)),
            "failed scan keeps the throttle window open"
        );
        // Asserted on the entry directly, because both a retained coverage
        // claim and the failure backoff produce the same `should_skip_poll`
        // answer here — a behavioral assertion could not tell them apart, and
        // the difference is that the stat-covered arm has no time bound at all.
        assert!(
            map.lock()
                .get(key)
                .expect("failed scan creates an entry")
                .stat
                .is_none(),
            "a failed scan covered nothing, so it must drop any covered stat: \
             keeping one would suppress rescans of an unchanged file forever \
             instead of for the backoff window, and an environmental failure \
             (a lock, a permission) would never recover without a restart"
        );
        assert!(
            !map.failure_retry_due(key, 1),
            "a fresh failure is not due for another attempt"
        );
        assert!(
            map.failure_retry_due(key, 2),
            "a new generation is always due"
        );

        map.record_noop_scan(key, 1, stat(6));
        assert!(
            map.failure_retry_due(key, 1),
            "a successful no-op scan clears the failure streak"
        );

        map.clear(key);
        assert!(
            map.failure_retry_due(key, 1),
            "an absent entry is always due, so a restart inherits no suppression"
        );
        assert!(
            !map.should_skip_poll(key, 1, &stat(4)),
            "a durable checkpoint write clears the throttle"
        );
    }

    /// Issue #601 §3.2 — the **width** of `record_noop_scan`'s contention
    /// reset. A clean scan proves the contention passed, so it must clear the
    /// contention *ladder*, not only the clock the ladder is measured from.
    ///
    /// The reset is unobservable at the moment it happens:
    /// `consecutive_contended_scans` is read in exactly one place —
    /// `failure_retry_due`'s `Some(last_contended_at)` arm — and that arm is
    /// unreachable while the clock is `None`. So deleting the reset changes
    /// nothing until contention returns, at which point `record_contended_scan`
    /// increments from a **stale** streak and the durable replay barrier
    /// resumes part-way up the ladder (here: the 60 s ceiling) instead of at
    /// its 15 s base. A replacement replay of a store that was busy an hour ago
    /// and is busy again now would wait a minute per attempt from the first
    /// rejection.
    ///
    /// **[DIVERGENT FIXTURE]** the streak is walked to 3 before the clean scan,
    /// so the base window and the ceiling are distinguishable; from a streak of
    /// 1 a reset and a stale value give the same answer.
    ///
    /// Fails for: dropping `entry.consecutive_contended_scans = 0` from
    /// `record_noop_scan`.
    #[test]
    fn a_clean_scan_resets_the_contention_ladder_not_only_its_clock() {
        let map = VolatilePollMap::new();
        let key = "cursor-sqlite-test:contention-ladder-reset";
        let stat = |db_len: u64| StatFingerprint {
            db_len,
            ..Default::default()
        };

        // Three consecutive rejections: the next barrier window is the 60 s
        // ceiling, which is what makes a stale streak visible.
        map.record_contended_scan(key, 1);
        map.record_contended_scan(key, 1);
        map.record_contended_scan(key, 1);
        assert_eq!(map.consecutive_contended_scans(key), 3);
        map.age_for_tests(key, Duration::from_secs(31));
        assert!(
            !map.failure_retry_due(key, 1),
            "the fixture must sit deep enough in the ladder that the base \
             window and the ceiling differ, or this guard cannot fail"
        );

        map.record_noop_scan(key, 1, stat(1));
        assert_eq!(
            map.consecutive_contended_scans(key),
            0,
            "a scan that completed cleanly resets the ladder, not only its clock"
        );

        // …so the next rejection is a first rejection, and its window is the
        // 15 s base rather than the ceiling the stale streak would have kept.
        map.record_contended_scan(key, 1);
        assert!(
            !map.failure_retry_due(key, 1),
            "a fresh rejection still throttles the durable barrier"
        );
        map.age_for_tests(key, Duration::from_secs(16));
        assert!(
            map.failure_retry_due(key, 1),
            "one rejection buys a 15 s window; resuming from a stale streak \
             would hold the barrier for up to CONTENTION_BACKOFF_MAX instead"
        );
    }

    /// Issue #601 §2.1(2) — the **narrowing** width of `retry_blocked_replay`,
    /// at the Cursor site. Plan §7.2 F2 records the widening (dropping
    /// `&& !block_reason.is_empty()`); this is the other direction, and it is
    /// the worse one.
    ///
    /// `checkpoint.status == "replaying"` is not covered by the `"error"`
    /// disjunct: a process that dies between `BeginReplay` and
    /// `FinalizeReplay` leaves a checkpoint that is `replaying` with **no**
    /// error status and **no** block reason. Delete the disjunct and that
    /// checkpoint is treated as an ordinary poll — `replacement_replay` is
    /// false, the cursor is not reset, no barrier is re-sent, the status is
    /// quietly rewritten to `active`, and the interrupted replay never
    /// completes. Since round 8 this predicate also feeds the blocked-replay
    /// throttle gates, so its width is load-bearing for code this PR adds.
    ///
    /// **[DIVERGENT FIXTURE]** the stat is deliberately left unchanged since
    /// the cold poll, so the cheap short-circuit is armed. That is what a
    /// crashed replay actually looks like — nothing wrote to the database while
    /// the process was down — and it is what makes the two behaviours diverge:
    /// with the disjunct the state is reset and the scan runs, without it the
    /// unchanged stat returns early.
    ///
    /// Fails for: dropping the `checkpoint.status == "replaying"` disjunct from
    /// `retry_blocked_replay`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_crash_interrupted_replay_resumes_from_its_replaying_status() {
        let path = unique_db_path("replaying-status-resume");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();

        run_poll_with_state(&work, &checkpoints, &poll_state).await;
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        // Exactly what a crash between `BeginReplay` and `FinalizeReplay`
        // leaves behind: `replaying`, no error, no block reason.
        {
            let mut map = checkpoints.write().await;
            let checkpoint = map
                .get_mut(&cp_key)
                .expect("first poll commits a checkpoint");
            checkpoint.status = "replaying".to_string();
            checkpoint.block_reason.clear();
            checkpoint.final_scan_complete = false;
        }

        run_poll_with_state(&work, &checkpoints, &poll_state).await;

        let after = checkpoints
            .read()
            .await
            .get(&cp_key)
            .cloned()
            .expect("checkpoint survives the retry");
        assert_eq!(
            after.status, "active",
            "an interrupted replay must resume and finish, not be relabelled"
        );
        assert!(
            after.final_scan_complete,
            "a resumed replay finalizes; without the `replaying` disjunct the \
             poll returns on the unchanged stat and the source is stuck"
        );

        cleanup(&path);
    }

    fn scan_failures(metrics: &Arc<Metrics>) -> u64 {
        metrics
            .sqlite_scan_failures_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Gate G6b, issue #601 §2.5. A database whose **first** scan fails has
    /// **no prior volatile entry** — precisely the case `record_failed_scan`
    /// was a no-op for, so the full failed scan re-ran on every reconcile tick
    /// and every debounced watcher event, forever.
    ///
    /// Denominated on an **observed scan count**, never on absence of
    /// `ingest_errors` rows: those are rate-limited by `state.last_error`, so a
    /// test asserting on them (as
    /// `schema_mismatch_emits_one_error_and_preserves_cursor` does) stays green
    /// no matter how many scans ran.
    ///
    /// Fails for: reverting `record_failed_scan` to its `get_mut`-only form, or
    /// dropping the failure arm from `should_skip_poll`.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_scan_backs_off_instead_of_rescanning_every_tick() {
        let path = unique_db_path("failure-backoff-first-scan");
        let db = Connection::open(&path).expect("create db");
        db.execute_batch("CREATE TABLE unrelated (id INTEGER PRIMARY KEY);")
            .expect("create unrelated table");
        drop(db);

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        for _ in 0..10 {
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        }

        assert!(
            scan_failures(&metrics) >= 1,
            "the first tick must actually attempt the scan"
        );
        assert!(
            scan_failures(&metrics) <= 2,
            "10 ticks against a database whose first scan fails must not run 10 \
             scans; observed {}",
            scan_failures(&metrics)
        );

        cleanup(&path);
    }

    /// The second shape `record_failed_scan` was a no-op for: a successful
    /// emitting scan calls `poll_state.clear`, deleting the entry, so a failure
    /// that follows one starts from no volatile state at all.
    ///
    /// Fails for: reverting `record_failed_scan` to its `get_mut`-only form.
    #[tokio::test(flavor = "multi_thread")]
    async fn failure_after_a_successful_scan_still_backs_off() {
        let path = unique_db_path("failure-backoff-after-success");
        let db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        let first =
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(
            !all_event_rows(&first).is_empty(),
            "the first poll must emit, so the volatile entry is cleared"
        );
        assert_eq!(scan_failures(&metrics), 0);

        // Same inode, so this is a schema failure rather than a replacement.
        db.execute_batch("DROP TABLE cursorDiskKV;")
            .expect("drop the relevant table");
        drop(db);

        for _ in 0..10 {
            run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        }

        assert!(scan_failures(&metrics) >= 1);
        assert!(
            scan_failures(&metrics) <= 2,
            "a failure following an emitting scan must still back off; observed {}",
            scan_failures(&metrics)
        );

        cleanup(&path);
    }

    /// Issue #601 §2.1(2): a durable `BeginReplay` barrier must never be sent
    /// with no scan behind it. `should_skip_poll` runs *after* the barrier, and
    /// a blocked-replay retry reuses its generation, so a volatile entry
    /// covering the current stat could skip the scan while the barrier had
    /// already been persisted — leaving the source stuck in `replaying`
    /// forever, one barrier per tick.
    ///
    /// **[DIVERGENT FIXTURE]** the volatile entry must genuinely cover the
    /// current stat; with an uncovered stat the skip never triggers and the two
    /// behaviors coincide.
    ///
    /// Fails for: dropping the `!replacement_replay` guard on the
    /// `should_skip_poll` call.
    #[tokio::test(flavor = "multi_thread")]
    async fn blocked_replay_scans_behind_its_barrier() {
        let path = unique_db_path("blocked-replay-scan");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();

        run_poll_with_state(&work, &checkpoints, &poll_state).await;
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        // Durably block the source, exactly as a failed replacement would.
        let generation = {
            let mut map = checkpoints.write().await;
            let checkpoint = map
                .get_mut(&cp_key)
                .expect("first poll commits a checkpoint");
            checkpoint.status = "error".to_string();
            checkpoint.block_reason = "seeded blocked replay".to_string();
            checkpoint.final_scan_complete = false;
            checkpoint.source_generation
        };

        // And make the volatile entry claim the current stat as covered, which
        // is the only state in which the post-barrier skip can fire.
        let current_stat = stat_fingerprint(&work.path).expect("fixture stat");
        poll_state.record_noop_scan(&cp_key, generation, current_stat);
        assert!(
            poll_state.should_skip_poll(&cp_key, generation, &current_stat),
            "the fixture must actually reach the skip condition, or this guard \
             cannot fail"
        );

        run_poll_with_state(&work, &checkpoints, &poll_state).await;

        let after = checkpoints
            .read()
            .await
            .get(&cp_key)
            .cloned()
            .expect("checkpoint survives the retry");
        assert_eq!(
            after.status, "active",
            "a blocked replay retry must run its scan and finalize, not send a \
             barrier and skip"
        );
        assert!(after.final_scan_complete);
        assert!(after.block_reason.is_empty());

        cleanup(&path);
    }

    /// Issue #601 §2.5. A replacement replay whose records cannot be normalized
    /// enters a durably blocked state, and the retry that follows must climb
    /// the failure ladder like any other repeat failure.
    ///
    /// It did not: the block arm ran `clear()` and then `record_failed_scan()`,
    /// and `clear` deletes the entry so the `or_insert` beneath it restarted
    /// the streak at 1 every single time. Because the trigger is a record
    /// failing `normalize_record` — deterministic and content-driven, so it
    /// recurs on every retry — the path was pinned at the 15 s floor forever:
    /// a durable `BeginReplay` barrier plus a full re-read of the database,
    /// every fifteen seconds, indefinitely.
    ///
    /// **[DIVERGENT FIXTURE]** an unknown harness makes `normalize_record` fail
    /// for *every* record, which is what reaches the `replay_block_reason` arm
    /// rather than the ordinary `Failed` arm; and the volatile clock is
    /// backdated so the second and third windows are observable without
    /// sleeping through 15 s + 30 s of real time.
    ///
    /// Fails for: restoring the `clear()` before the blocked-replay failure
    /// record (the third probe becomes due and the retry runs).
    #[tokio::test(flavor = "multi_thread")]
    async fn blocked_replay_retries_climb_the_failure_ladder() {
        let path = unique_db_path("blocked-replay-ladder");
        let _db = seed_fixture_db(&path);
        // `normalize_record` rejects an unregistered harness outright, so every
        // synthesized record fails and a replacement replay blocks durably.
        let work = WorkItem {
            harness: "not-a-registered-harness".to_string(),
            ..sqlite_work(&path)
        };
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());

        // Cold poll commits a checkpoint under the default exclusion policy.
        run_poll_with_state_and_metrics(&work, &checkpoints, &poll_state, &metrics).await;
        assert!(checkpoints.read().await.contains_key(&cp_key));

        // Changing the exclusion set starts a replacement replay, which cannot
        // finish because nothing normalizes.
        let mut replaying = moraine_config::AppConfig::default();
        replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
        let block_poll = |config: moraine_config::AppConfig| {
            let work = work.clone();
            let checkpoints = checkpoints.clone();
            let poll_state = poll_state.clone();
            let metrics = metrics.clone();
            async move { run_replay_poll(&config, &work, &checkpoints, &poll_state, &metrics).await }
        };

        let blocked = block_poll(replaying.clone()).await;
        assert!(blocked, "the replacement replay must block durably");
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            1,
            "entering the blocked state starts the ladder"
        );

        // Inside the first 15 s window the retry is suppressed entirely.
        let scans_before = payload_rows(&metrics);
        assert!(!block_poll(replaying.clone()).await);
        assert_eq!(
            payload_rows(&metrics),
            scans_before,
            "throttled, no re-read"
        );
        assert_eq!(poll_state.consecutive_failed_scans(&cp_key), 1);

        // 16 s later the first window has expired: the retry runs and the
        // ladder climbs to 2, which means a 30 s window.
        poll_state.age_for_tests(&cp_key, Duration::from_secs(16));
        assert!(block_poll(replaying.clone()).await);
        assert_eq!(
            poll_state.consecutive_failed_scans(&cp_key),
            2,
            "a repeat block must extend the streak, not restart it"
        );

        // 16 s is no longer enough. This is the probe a reset-to-1 bug fails:
        // pinned at the floor, the retry would be due and would run.
        poll_state.age_for_tests(&cp_key, Duration::from_secs(16));
        let scans_before = payload_rows(&metrics);
        assert!(!block_poll(replaying.clone()).await);
        assert_eq!(
            payload_rows(&metrics),
            scans_before,
            "the second window is 30 s; a blocked replay must not re-read the \
             whole database every 15 s forever"
        );

        // And it does recover once the wider window expires.
        poll_state.age_for_tests(&cp_key, Duration::from_secs(20));
        assert!(block_poll(replaying).await);
        assert_eq!(poll_state.consecutive_failed_scans(&cp_key), 3);

        cleanup(&path);
    }

    /// Runs one poll, acknowledging replay barriers, and reports whether the
    /// poll sent a `BlockReplay` transition (i.e. whether it actually ran).
    async fn run_replay_poll(
        config: &moraine_config::AppConfig,
        work: &WorkItem,
        checkpoints: &Arc<RwLock<HashMap<String, Checkpoint>>>,
        poll_state: &VolatilePollMap,
        metrics: &Arc<Metrics>,
    ) -> bool {
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);
        let process = process_cursor_sqlite_db(
            config,
            work,
            checkpoints.clone(),
            poll_state,
            sink_tx,
            metrics,
        );
        tokio::pin!(process);
        let mut blocked = None;
        loop {
            tokio::select! {
                result = &mut process => {
                    result.expect("cursor_sqlite replay poll should succeed");
                    break;
                }
                message = sink_rx.recv() => match message.expect("replay sink remains open") {
                    SinkMessage::Batch(_) => {}
                    SinkMessage::BlockReplay { transition, ack } => {
                        blocked = Some(transition.checkpoint.clone());
                        let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                            checkpoint_revision: 1,
                            operation_id: transition.checkpoint.operation_id,
                        }));
                    }
                    SinkMessage::BeginReplay { transition, ack }
                    | SinkMessage::MirrorCaughtUp { transition, ack } => {
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
                                    publication_revision: 1,
                                    already_published: false,
                                },
                            ),
                        ));
                    }
                }
            }
        }
        while let Ok(SinkMessage::Batch(_)) = sink_rx.try_recv() {}
        if let Some(cp) = blocked.clone() {
            let key = checkpoint_key(&cp.source_name, &cp.source_file);
            checkpoints.write().await.insert(key, cp);
        }
        blocked.is_some()
    }

    /// Issue #601 §2.0 / WI-01. Every cost budget in this issue is denominated
    /// on `ScanLedger.payload_bytes`, so a ledger that charges anywhere other
    /// than the point SQLite hands the bytes over makes all of them lies.
    ///
    /// **[DIVERGENT FIXTURE]** — every convenient proxy for "bytes read" is
    /// made to disagree with the truth:
    ///
    /// - the composer's bulk sits in a field the synthesizer never copies, so
    ///   emitted bytes are a small fraction of read bytes;
    /// - one relevant key holds non-JSON ballast, so it is read in full and
    ///   emits nothing — read rows and emitted rows genuinely differ;
    /// - the second poll re-reads every byte and emits nothing, so a ledger
    ///   charged at the emit site (or only on the changed branch) reports zero
    ///   where the truth is unchanged.
    ///
    /// Fails for: charging from emitted records, charging only changed rows,
    /// charging from a SQL-side `length()` expression, or dropping either the
    /// row charge or the byte charge.
    #[test]
    fn ledger_charges_payload_bytes_at_the_read_site() {
        let path = unique_db_path("ledger-read-site");
        let db = create_kv_db(&path);

        // 64 KiB the synthesizer reads and then throws away: `copy_fields`
        // only lifts a fixed field list, and this is not on it.
        let composer = json!({
            "composerId": COMPOSER_ID,
            "name": "Ledger read-site fixture",
            "createdAt": 1_780_000_000_000i64,
            "fullConversationHeadersOnly": [{"bubbleId": USER_BUBBLE_ID, "type": 1}],
            "unreadBallast": "z".repeat(64 * 1024),
        });
        let composer_key = format!("composerData:{COMPOSER_ID}");
        put(&db, &composer_key, &composer);

        // A relevant key whose value is not JSON at all: read in full,
        // synthesizes nothing. Emitted rows can never equal read rows here.
        let junk = format!("not-json-{}", "q".repeat(32 * 1024));
        let junk_key = format!("bubbleId:{COMPOSER_ID}:{TOOL_BUBBLE_ID}");
        db.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![junk_key, junk],
        )
        .expect("insert non-JSON kv row");
        drop(db);

        let composer_text = serde_json::to_string(&composer).expect("serialize composer");
        // Keys are census material now (§3.3 Change 1): the change detector
        // walks `(rowid, key)` over the covering index and only the candidate
        // point reads materialize values, so the payload axis carries value
        // bytes alone.
        let expected_bytes = (composer_text.len() + junk.len()) as u64;
        let expected_census_key_bytes = (composer_key.len() + junk_key.len()) as u64;
        let db_path = path.to_string_lossy().to_string();

        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned { new_state, .. } = scan_database(
            &db_path,
            &CursorState::fresh(),
            &default_scan_plan(),
            &mut ledger,
        ) else {
            panic!("cold ledger scan should succeed");
        };

        assert_eq!(
            ledger.payload_bytes, expected_bytes,
            "payload bytes must be the exact value length SQLite materialized"
        );
        assert_eq!(ledger.payload_rows, 2, "both relevant keys were read");
        assert_eq!(
            ledger.rows_emitted, 1,
            "only the composer synthesizes a record; the ballast key emits nothing"
        );
        assert!(
            ledger.payload_bytes > 90 * 1024,
            "the fixture must read far more than it emits, or the two axes are \
             interchangeable and neither is tested"
        );
        // The census axis carries schema validation plus the real key census
        // (§3.3 Change 1): one row per relevant key, charged its key bytes.
        // The two axes must not bleed into each other — a candidate value read
        // miscounted as census would deflate every payload budget.
        let schema_census = {
            let connection = open_read_only(&db_path).expect("reopen for schema census");
            expected_schema_census(&connection, &["cursorDiskKV"])
        };
        assert!(schema_census.census_rows > 0);
        assert_eq!(
            ledger.census_rows,
            schema_census.census_rows + 2,
            "census rows are schema validation plus one row per relevant key"
        );
        assert_eq!(
            ledger.census_bytes,
            schema_census.census_bytes + expected_census_key_bytes,
            "census bytes are the schema text plus the key bytes"
        );

        // Nothing changed, so nothing is emitted — but every byte is still
        // read. This is the assertion an emit-site ledger cannot survive.
        let mut second = ScanLedger::default();
        let ScanOutcome::Scanned { .. } =
            scan_database(&db_path, &new_state, &default_scan_plan(), &mut second)
        else {
            panic!("warm ledger scan should succeed");
        };
        assert_eq!(
            second.rows_emitted, 0,
            "an unchanged database emits nothing on the second poll"
        );
        assert_eq!(
            second.payload_bytes, expected_bytes,
            "an unchanged database is still fully re-read today, and the ledger \
             must say so"
        );
        assert_eq!(second.payload_rows, 2);

        cleanup(&path);
    }

    /// A bubble's workspace stamp issues a *second* payload read of the parent
    /// composer blob. It is easy to miss because it hides behind a cache, and
    /// a ledger that misses it under-reports by a whole composer value
    /// (2.4 MB on the reference host).
    #[test]
    fn ledger_charges_the_composer_workspace_lookup() {
        let path = unique_db_path("ledger-workspace-lookup");
        let db = create_kv_db(&path);
        let composer = json!({
            "composerId": COMPOSER_ID,
            "workspaceIdentifier": {"uri": {"fsPath": "/work/ledger"}},
            "ballast": "w".repeat(16 * 1024),
        });
        let composer_key = format!("composerData:{COMPOSER_ID}");
        put(&db, &composer_key, &composer);
        let bubble_key = format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}");
        put(&db, &bubble_key, &user_bubble_value());
        drop(db);

        let composer_text = serde_json::to_string(&composer).expect("serialize composer");
        let bubble_text = serde_json::to_string(&user_bubble_value()).expect("serialize bubble");
        // The composer value is materialized twice: once by the candidate
        // read, once by the workspace lookup the bubble triggers. Keys are
        // census material and charge no payload bytes.
        let expected_bytes = (composer_text.len() + bubble_text.len() + composer_text.len()) as u64;
        let _ = (&composer_key, &bubble_key);

        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned { records, .. } = scan_database(
            &path.to_string_lossy(),
            &CursorState::fresh(),
            &default_scan_plan(),
            &mut ledger,
        ) else {
            panic!("workspace-lookup scan should succeed");
        };
        assert!(
            records.iter().any(
                |record| record.record.get("workspacePath").and_then(Value::as_str)
                    == Some("/work/ledger")
            ),
            "the fixture must actually take the lookup path"
        );
        assert_eq!(
            ledger.payload_rows, 3,
            "two scanned keys plus the composer re-read the workspace lookup performs"
        );
        assert_eq!(ledger.payload_bytes, expected_bytes);

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

    /// Issue #601 §2.3 / gate G4 (Cursor). This REWRITES the retired latch
    /// test `oversized_key_space_is_skipped_with_an_error`, inverting every
    /// assertion it made:
    ///
    /// - it asserted exactly one `sqlite_cursor_too_large` error row — now
    ///   zero error rows of any kind, because history size is a degradation,
    ///   never a failure;
    /// - it asserted zero events — now the *newest* record must be emitted,
    ///   because bounded progress is newest-first (`rowid DESC`);
    /// - it asserted no durable progress — now a checkpoint must persist, so
    ///   the poll's coverage is committed rather than discarded.
    ///
    /// **[DIVERGENT FIXTURE]** (§8 G4): the newest row by `rowid` carries the
    /// key that sorts LAST, so the old any-order (key-ascending) scan would
    /// reach it dead last — a budget bolted onto key-order truncation emits
    /// the oldest keys and misses the newest record, and this test fails.
    /// Fails for: a budget that fails the scan instead of degrading, bounded
    /// progress that is not newest-first, and restoring the `TooLarge` arm.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cursor_keyspace_over_budget_still_ingests_recent_work() {
        let path = unique_db_path("over-budget-recent");
        let db = create_kv_db(&path);
        for idx in 0..6 {
            // Old history: keys sort first, rowids are lowest.
            let bubble = json!({
                "_v": 3,
                "type": 1,
                "bubbleId": format!("aaaaaaaa-1111-4111-8111-nnnnnnnn{idx:04}"),
                "createdAt": format!("2026-05-01T02:04:{idx:02}.000Z"),
                "text": format!("old bubble {idx}"),
            });
            put(&db, &format!("bubbleId:{COMPOSER_ID}:a{idx:03}"), &bubble);
        }
        // The newest row: inserted last (highest rowid), key sorts last.
        let newest = json!({
            "_v": 3,
            "type": 1,
            "bubbleId": "ffffffff-9999-4999-8999-999999999999",
            "createdAt": "2026-05-08T02:04:37.835Z",
            "text": "the newest bubble",
        });
        put(&db, &format!("bubbleId:{COMPOSER_ID}:zz-newest"), &newest);
        drop(db);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_rows = 2;

        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let batches = run_poll_with_config(
            &config,
            &work,
            &checkpoints,
            &VolatilePollMap::new(),
            &metrics,
        )
        .await;

        let rows = all_event_rows(&batches);
        let texts: Vec<&str> = rows
            .iter()
            .filter_map(|row| row.get("text_content").and_then(Value::as_str))
            .collect();
        assert!(
            texts.contains(&"the newest bubble"),
            "bounded progress must reach the newest record first; got {texts:?}"
        );
        assert_eq!(
            rows.len(),
            2,
            "a 2-row budget commits exactly the two newest records"
        );
        assert!(
            !texts.contains(&"old bubble 0"),
            "key-order truncation would emit the oldest key; newest-first must not"
        );
        let error_rows: usize = batches.iter().map(|batch| batch.error_rows.len()).sum();
        assert_eq!(
            error_rows, 0,
            "crossing the former ceiling is a degradation, never an error"
        );
        assert_eq!(
            degraded_scans(&metrics),
            1,
            "the skipped remainder must be reported as degraded coverage"
        );
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let map = checkpoints.read().await;
        let checkpoint = map
            .get(&cp_key)
            .expect("degraded poll persists a checkpoint");
        assert_eq!(checkpoint.status, "active");

        cleanup(&path);
    }

    /// The runaway side of the same budget (§8 non-negotiable: bounds bound
    /// from both sides). G4 above proves the budget cannot starve the newest
    /// record; this proves it cannot be exceeded: the scan reads exactly its
    /// row budget and accounts for every candidate it skipped.
    #[test]
    fn a_cursor_scan_over_budget_reads_no_more_than_its_budget() {
        let path = unique_db_path("over-budget-upper");
        seed_junk_fixture(&path, 7, 64);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_rows = 2;
        let plan = CursorScanPlan::from_config(&config, None);
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned { relevant_keys, .. } = scan_database(
            &path.to_string_lossy(),
            &CursorState::fresh(),
            &plan,
            &mut ledger,
        ) else {
            panic!("an over-budget scan must degrade, not fail");
        };
        assert_eq!(relevant_keys, 7, "the census still covers every key");
        assert_eq!(
            ledger.payload_rows, 2,
            "the candidate read stops exactly at its row budget"
        );
        assert!(ledger.coverage_degraded);
        assert_eq!(
            ledger.skipped_rows, 5,
            "every unread candidate is accounted for"
        );

        cleanup(&path);
    }

    /// §2.1's census cap: a pathological keyspace truncates the census and
    /// degrades instead of failing, and every prior entry beyond the
    /// truncation point is carried — "un-censused" must never read "deleted".
    /// A complete census keeps exact deletion pruning (asserted here too, so
    /// the carry cannot silently widen into never-pruning).
    #[test]
    fn a_census_past_its_cap_truncates_and_degrades_instead_of_failing() {
        let path = unique_db_path("census-cap");
        let keys = seed_junk_fixture(&path, 6, 64);

        // Full scan first, so the prior state holds all six hashes.
        let full_plan = default_scan_plan();
        let mut full_ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: full_state,
            ..
        } = scan_database(
            &path.to_string_lossy(),
            &CursorState::fresh(),
            &full_plan,
            &mut full_ledger,
        )
        else {
            panic!("cold scan should succeed");
        };
        assert_eq!(full_state.kv_hashes.len(), 6);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.fast_path_max_census_rows = 3;
        let capped_plan = CursorScanPlan::from_config(&config, None);
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state,
            relevant_keys,
            ..
        } = scan_database(
            &path.to_string_lossy(),
            &full_state,
            &capped_plan,
            &mut ledger,
        )
        else {
            panic!("a capped census must degrade, not fail");
        };
        assert_eq!(relevant_keys, 3, "three keys were censused");
        assert!(ledger.coverage_degraded);
        assert_eq!(
            ledger.skipped_rows, 3,
            "the un-censused remainder is counted"
        );
        assert_eq!(
            new_state.kv_hashes.len(),
            6,
            "prior entries beyond the truncation are carried, not dropped"
        );

        // A complete census still prunes deletions exactly.
        let db = Connection::open(&path).expect("reopen fixture");
        db.execute(
            "DELETE FROM cursorDiskKV WHERE key = ?1",
            rusqlite::params![&keys[0]],
        )
        .expect("delete one key");
        drop(db);
        let mut pruned_ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: pruned, ..
        } = scan_database(
            &path.to_string_lossy(),
            &full_state,
            &full_plan,
            &mut pruned_ledger,
        )
        else {
            panic!("post-delete scan should succeed");
        };
        assert!(
            !pruned.kv_hashes.contains_key(&keys[0]),
            "a complete census must still detect the deletion exactly"
        );
        assert_eq!(pruned.kv_hashes.len(), 5);

        cleanup(&path);
    }

    /// Issue #601 §2.3 / gate G4 (Cursor checkpoint-state ceiling): crossing
    /// `max_checkpoint_bytes` evicts the **oldest** kv hashes until the
    /// payload fits — never fails the scan — and reports the eviction as
    /// degraded coverage. This is `MAX_RELEVANT_KEYS`' replacement: the old
    /// bound latched the whole database dead; the ceiling degrades, and the
    /// evicted key re-detects as never-read on a later poll (§6's
    /// content-addressed identity makes the re-emission safe). `cursor_json`
    /// rides the #602 transition digest, so this ceiling is also what keeps
    /// that digest's input bounded.
    ///
    /// MUTATION (executed 2026-07-31): make `evict_to_fit` return 0 without
    /// evicting — fails (the persisted payload exceeds its ceiling and
    /// nothing is evicted). Reverse the age sort (evict newest-first) —
    /// fails (the newest key's hash is dropped instead of the oldest's).
    #[test]
    fn a_cursor_state_over_its_ceiling_evicts_the_oldest_keys_instead_of_failing() {
        let path = unique_db_path("cursor-ceiling-upper");
        let keys = seed_junk_fixture(&path, 6, 64);

        let full_plan = default_scan_plan();
        let mut full_ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: full_state,
            ..
        } = scan_database(
            &path.to_string_lossy(),
            &CursorState::fresh(),
            &full_plan,
            &mut full_ledger,
        )
        else {
            panic!("cold scan should succeed");
        };
        let full_len = (*full_state).serialize().len();

        // One byte under the full payload: eviction must fire, and one round
        // of the one-eighth batch (a single entry at six keys) fits.
        let ceiling = full_len - 1;
        let mut plan = default_scan_plan();
        plan.max_checkpoint_bytes = ceiling;
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned { new_state, .. } =
            scan_database(&path.to_string_lossy(), &full_state, &plan, &mut ledger)
        else {
            panic!("a state ceiling must degrade, not fail");
        };
        assert_eq!(ledger.evicted_entries, 1, "one round of the oldest eighth");
        assert!(ledger.coverage_degraded);
        assert!(
            !new_state.kv_hashes.contains_key(&keys[0]),
            "eviction is oldest-first by census rowid"
        );
        assert!(
            new_state.kv_hashes.contains_key(&keys[5]),
            "the newest key's hash survives"
        );
        assert!(
            (*new_state).serialize().len() <= ceiling,
            "the persisted payload must fit its ceiling"
        );
        assert!(
            new_state.pending_coverage,
            "an evicted key is a durable coverage debt"
        );

        cleanup(&path);
    }

    /// The starvation side of the Cursor state ceiling: a payload **at** the
    /// ceiling evicts nothing — the fit check is `<=`, the map is untouched,
    /// coverage is not degraded, and the serialized payload is byte-identical
    /// to the un-ceilinged scan's (§2.6: `cursor_json` must stay stable for
    /// fully-covered sources). The upper test alone cannot see a ceiling that
    /// over-evicts by one boundary token; this one pins the boundary.
    ///
    /// MUTATION (executed 2026-07-31): `<=` → `<` in `evict_to_fit`'s fit
    /// checks — fails (a payload exactly at its ceiling is evicted).
    #[test]
    fn a_cursor_state_at_its_ceiling_evicts_nothing() {
        let path = unique_db_path("cursor-ceiling-lower");
        seed_junk_fixture(&path, 6, 64);

        let full_plan = default_scan_plan();
        let mut full_ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: full_state,
            ..
        } = scan_database(
            &path.to_string_lossy(),
            &CursorState::fresh(),
            &full_plan,
            &mut full_ledger,
        )
        else {
            panic!("cold scan should succeed");
        };
        let full_len = (*full_state).serialize().len();

        let mut plan = default_scan_plan();
        plan.max_checkpoint_bytes = full_len;
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned { new_state, .. } =
            scan_database(&path.to_string_lossy(), &full_state, &plan, &mut ledger)
        else {
            panic!("an at-ceiling scan should succeed");
        };
        assert_eq!(ledger.evicted_entries, 0, "at the boundary nothing evicts");
        assert!(!ledger.coverage_degraded);
        assert_eq!(new_state.kv_hashes.len(), 6);
        assert!(!new_state.pending_coverage);
        assert_eq!(
            (*new_state).serialize(),
            (*full_state).serialize(),
            "the persisted payload is byte-identical at the boundary"
        );

        cleanup(&path);
    }

    /// Gate G5a (§8): the sweep cursor is durable. Slices advance it
    /// mid-keyspace, the process "restarts" (fresh volatile state, only the
    /// persisted checkpoint survives), and the next slice resumes from the
    /// persisted position — not from the start of the cycle and not from the
    /// newest end. Fails for: a volatile-only sweep cursor, or a fast path
    /// that resets it.
    #[tokio::test(flavor = "multi_thread")]
    async fn sweep_cursor_survives_restart_and_resumes_mid_cycle() {
        let path = unique_db_path("sweep-restart");
        let keys = seed_junk_fixture(&path, 6, 64);
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_max_payload_rows = 2;
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        touch_irrelevant(&path);
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(sweep_slices(&metrics), 2);
        assert_eq!(
            persisted_sweep(&checkpoints, &work).await.cursor,
            keys[3],
            "two 2-row slices leave the persisted cursor mid-keyspace"
        );

        // Restart: only durable state survives. A fresh VolatilePollMap and
        // fresh metrics stand in for the new process.
        let restarted_state = VolatilePollMap::new();
        let restarted_metrics = Arc::new(Metrics::default());
        touch_irrelevant(&path);
        run_poll_with_config(
            &config,
            &work,
            &checkpoints,
            &restarted_state,
            &restarted_metrics,
        )
        .await;
        assert_eq!(sweep_slices(&restarted_metrics), 1);
        let resumed = persisted_sweep(&checkpoints, &work).await;
        assert_eq!(
            resumed.cursor, keys[5],
            "the restarted slice must resume at the persisted position: from \
             {} it covers exactly the fifth and sixth keys — restarting the \
             cycle would leave the cursor at {}, and starting from the newest \
             end would leave it at {}",
            keys[3], keys[1], keys[4],
        );
        assert_eq!(resumed.completed_cycles, 0);

        cleanup(&path);
    }

    /// Gate G5b (§8): a cycle completes in exactly the projected number of
    /// slices, and `projected_full_sweep_seconds` matches the realized cycle.
    /// Five 1,000-byte values under a 2-row / 2,000-byte slice budget need
    /// ceil(5000/2000) = 3 slices; the third slice reads the last key, finds
    /// the ordering exhausted, and wraps. Fails for: a sweep that revisits or
    /// skips regions, or a projection that does not match observation. Every
    /// slice-carrying poll must persist a checkpoint (the durable commit the
    /// F1 `new_state == prior_state_covered` conjunct guards).
    #[tokio::test(flavor = "multi_thread")]
    async fn sweep_completes_a_full_cycle_within_the_projected_interval() {
        let path = unique_db_path("sweep-cycle");
        seed_junk_fixture(&path, 5, 1000);
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_max_payload_rows = 2;
        config.ingest.sqlite.sweep_slice_max_payload_bytes = 2000;
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 7;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        for slice in 0..3 {
            if slice > 0 {
                touch_irrelevant(&path);
                poll_state.age_for_tests(&cp_key, Duration::from_secs(7));
            }
            let batches =
                run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
            assert!(
                batches
                    .last()
                    .and_then(|batch| batch.checkpoint.as_ref())
                    .is_some(),
                "slice {slice} must persist its advance durably"
            );
            assert_eq!(sweep_slices(&metrics), slice + 1);
        }

        let state = persisted_sweep(&checkpoints, &work).await;
        assert_eq!(
            state.completed_cycles, 1,
            "the third slice completes the cycle"
        );
        assert_eq!(state.cursor, "", "a completed cycle wraps to the start");
        assert_eq!(
            state.last_cycle_payload_bytes, 5000,
            "the cycle covered every value byte exactly once"
        );
        assert!(state.last_complete_unix_ms > 0);
        let projected = state
            .projected_full_sweep_seconds(
                config.ingest.sqlite.sweep_slice_max_payload_bytes,
                config.ingest.sqlite.sweep_slice_min_interval_seconds,
            )
            .expect("a completed cycle yields a projection");
        assert_eq!(
            projected,
            3 * config.ingest.sqlite.sweep_slice_min_interval_seconds,
            "the projection must match the realized cycle: 3 slices at the \
             configured interval"
        );

        cleanup(&path);
    }

    /// Gate G6c (§8): a single row larger than the whole slice byte budget is
    /// still processed and the cursor advances past it — otherwise one
    /// oversized row is a permanent sweep stall, the same latch class §2.3
    /// retires. The follow-up slice proves the cursor really moved past it.
    /// Also the runaway side: the slice's reads are bounded by the budget plus
    /// that one first row, and both ledger folds (sweep and payload axes) must
    /// carry them.
    #[test]
    fn sweep_slice_stops_at_its_budget_and_still_advances() {
        let path = unique_db_path("sweep-oversized");
        let db = create_kv_db(&path);
        let big_key = format!("bubbleId:{COMPOSER_ID}:a-big");
        let small_key = format!("bubbleId:{COMPOSER_ID}:z-small");
        db.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![&big_key, format!("junk-big-{}", "b".repeat(10_000))],
        )
        .expect("insert oversized row");
        db.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![&small_key, "junk-small"],
        )
        .expect("insert small row");
        drop(db);
        let db_path = path.to_string_lossy().to_string();

        let mut cold_ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: cold, ..
        } = scan_database(
            &db_path,
            &CursorState::fresh(),
            &default_scan_plan(),
            &mut cold_ledger,
        )
        else {
            panic!("cold scan should succeed");
        };

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_max_payload_bytes = 100;
        let sweep_plan = SweepPlan::from_config(&config);
        let plan = CursorScanPlan::from_config(&config, Some(sweep_plan));
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: after_big,
            swept,
            ..
        } = scan_database(&db_path, &cold, &plan, &mut ledger)
        else {
            panic!("sweep scan should succeed");
        };
        assert!(swept);
        assert_eq!(
            after_big.sweep.cursor, big_key,
            "the oversized row is processed and the cursor advances past it"
        );
        assert_eq!(
            ledger.sweep_rows, 1,
            "the slice stops after the row that exhausted its budget"
        );
        assert!(
            ledger.sweep_bytes > 10_000,
            "the oversized row's bytes are charged to the sweep axis; got {}",
            ledger.sweep_bytes
        );
        assert!(
            ledger.payload_bytes >= ledger.sweep_bytes,
            "sweep reads are payload reads too — hiding them from the payload \
             axes would let a sweep evade every fast-path budget"
        );

        // The next slice starts strictly after the oversized row: it reads
        // exactly the one remaining key (never the oversized row again),
        // discovers the ordering exhausted, and wraps the cycle.
        let plan = CursorScanPlan::from_config(&config, Some(SweepPlan::from_config(&config)));
        let mut second = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: after_small,
            ..
        } = scan_database(&db_path, &after_big, &plan, &mut second)
        else {
            panic!("second sweep scan should succeed");
        };
        assert_eq!(
            second.sweep_rows, 1,
            "the follow-up slice reads only the key beyond the oversized row"
        );
        assert!(
            second.sweep_bytes < 1_000,
            "re-reading the oversized row would show here; got {}",
            second.sweep_bytes
        );
        assert_eq!(after_small.sweep.completed_cycles, 1);
        assert_eq!(after_small.sweep.cursor, "", "the completed cycle wraps");
        let _ = small_key;

        cleanup(&path);
    }

    /// The wall-clock budget's call site, and the starvation side of the
    /// forward-progress rule: `sweep_slice_max_millis = 0` binds before any
    /// second item, yet the slice still processes exactly one. Fails for:
    /// checking the deadline before the first item, or dropping the deadline
    /// term from the driver.
    #[test]
    fn a_zero_millis_sweep_budget_still_makes_forward_progress() {
        let path = unique_db_path("sweep-zero-millis");
        let keys = seed_junk_fixture(&path, 3, 64);
        let db_path = path.to_string_lossy().to_string();

        let mut cold_ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state: cold, ..
        } = scan_database(
            &db_path,
            &CursorState::fresh(),
            &default_scan_plan(),
            &mut cold_ledger,
        )
        else {
            panic!("cold scan should succeed");
        };

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_max_millis = 0;
        let plan = CursorScanPlan::from_config(&config, Some(SweepPlan::from_config(&config)));
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned {
            new_state, swept, ..
        } = scan_database(&db_path, &cold, &plan, &mut ledger)
        else {
            panic!("sweep scan should succeed");
        };
        assert!(swept);
        assert_eq!(
            ledger.sweep_rows, 1,
            "one item per slice at a zero deadline"
        );
        assert_eq!(new_state.sweep.cursor, keys[0]);

        cleanup(&path);
    }

    /// Gate G6a (§8): sweep eligibility is keyed on trigger provenance and on
    /// nothing else. Watcher polls — however many, however quiet the database
    /// — never attach a slice; the first reconcile poll does. Denominated on
    /// the ledger's sweep axes, not on total rows, so a sweep hidden inside a
    /// fast path would still be caught.
    #[tokio::test(flavor = "multi_thread")]
    async fn sweep_does_not_run_on_watcher_triggered_polls() {
        let path = unique_db_path("sweep-watcher");
        seed_junk_fixture(&path, 3, 64);
        let watcher = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;

        let cp_key = checkpoint_key(&watcher.source_name, &watcher.path);
        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        for _ in 0..5 {
            run_poll_with_config(&config, &watcher, &checkpoints, &poll_state, &metrics).await;
            touch_irrelevant(&path);
            // Past the no-op rescan throttle, so every driven poll genuinely
            // scans — a skipped poll proves nothing about sweep eligibility.
            poll_state.age_for_tests(&cp_key, Duration::from_secs(16));
        }
        assert_eq!(
            sweep_rows(&metrics),
            0,
            "watcher-triggered polls must never attach a sweep slice"
        );
        assert_eq!(sweep_slices(&metrics), 0);

        let reconcile = reconcile_work(&path);
        run_poll_with_config(&config, &reconcile, &checkpoints, &poll_state, &metrics).await;
        assert!(
            sweep_rows(&metrics) > 0,
            "the first reconcile-triggered poll of a quiet database sweeps"
        );
        assert_eq!(sweep_slices(&metrics), 1);

        cleanup(&path);
    }

    /// Eligibility condition 2, upper side: the per-database interval clock
    /// throttles slices even though every one of these polls is reconcile-
    /// triggered and the database stays quiet — and the clock **survives an
    /// emitting poll between slices**. Every persisting scan calls `clear`,
    /// so a clock that lives and dies with the volatile entry is wiped by the
    /// first interleaved write, and the next quiet reconcile poll re-sweeps
    /// at the reconcile cadence — up to ~10× the configured minimum on
    /// databases with interleaved writes, the typical agent-session shape.
    /// Noop polls alone cannot see that: they preserve the entry. Fails for:
    /// dropping the interval term, losing the clock to the slice's own
    /// checkpoint persist (the `record_sweep_slice` re-arm ordering), or
    /// losing it to a later emitting poll's persist (`clear`'s clock carry).
    ///
    /// MUTATION (executed 2026-07-31): revert `clear` to a bare
    /// `self.lock().remove(cp_key)` — the post-emission assertion fails
    /// (slice #2 attaches inside the interval) while the pre-emission half
    /// stays green, which is why the emitting interleave exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_reconcile_poll_inside_the_sweep_interval_attaches_no_slice() {
        let path = unique_db_path("sweep-interval-upper");
        seed_junk_fixture(&path, 3, 64);
        let work = reconcile_work(&path);
        let cp_key = checkpoint_key(&work.source_name, &work.path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 3600;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(sweep_slices(&metrics), 1, "the first slice is immediate");
        for _ in 0..3 {
            touch_irrelevant(&path);
            run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        }
        assert_eq!(
            sweep_slices(&metrics),
            1,
            "no further slice inside the configured interval"
        );

        // A real bubble arrives: the watcher poll emits and persists its own
        // checkpoint, which calls `clear`. The interval clock must ride
        // through that wipe, or the next quiet reconcile poll re-sweeps at
        // the reconcile cadence. Aged past the no-op rescan throttle first —
        // three quiet polls made the entry stat-noisy, and a skipped poll
        // would prove nothing about the clock (16 s is far inside the 3600 s
        // sweep interval, so this cannot arm the slice by itself).
        poll_state.age_for_tests(&cp_key, Duration::from_secs(16));
        {
            let db = Connection::open(&path).expect("reopen fixture");
            put(
                &db,
                &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
                &user_bubble_value(),
            );
        }
        let watcher = sqlite_work(&path);
        let batches =
            run_poll_with_config(&config, &watcher, &checkpoints, &poll_state, &metrics).await;
        assert!(
            !all_event_rows(&batches).is_empty(),
            "the bubble must actually emit for the interleave to mean anything"
        );
        for _ in 0..2 {
            touch_irrelevant(&path);
            run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        }
        assert_eq!(
            sweep_slices(&metrics),
            1,
            "an emitting poll between slices must not re-arm the interval clock"
        );

        cleanup(&path);
    }

    /// Eligibility condition 2, lower side: once the interval elapses the
    /// next reconcile poll is armed again — including when the interval was
    /// spent under continuous **emitting** polls, each of whose checkpoint
    /// persists called `clear`. Together with the upper test this bounds the
    /// interval clock from both directions: it must survive every wipe (no
    /// early slice) and it must still expire (no starvation).
    ///
    /// MUTATION (executed 2026-07-31): `sweep_slice_due`'s armed arm
    /// (`Some(at) => at.elapsed() >= min_interval`) → `Some(_) => false` —
    /// the final assertion fails (an armed clock never expires) while the
    /// upper test stays green, which is why the pair exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_expired_sweep_interval_arms_the_next_reconcile_poll() {
        let path = unique_db_path("sweep-interval-lower");
        seed_junk_fixture(&path, 3, 64);
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 3600;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(sweep_slices(&metrics), 1);

        // Continuous emitting polls inside the interval: each edit persists a
        // checkpoint and calls `clear`, and none of that may starve the sweep
        // once the interval has genuinely elapsed.
        let watcher = sqlite_work(&path);
        for idx in 0..2 {
            {
                let db = Connection::open(&path).expect("reopen fixture");
                let mut value = user_bubble_value();
                value["text"] = json!(format!("edit {idx}"));
                put(
                    &db,
                    &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
                    &value,
                );
            }
            let batches =
                run_poll_with_config(&config, &watcher, &checkpoints, &poll_state, &metrics).await;
            assert!(
                !all_event_rows(&batches).is_empty(),
                "each interleaved poll must actually emit"
            );
        }
        assert_eq!(sweep_slices(&metrics), 1, "still inside the interval");

        poll_state.age_for_tests(&cp_key, Duration::from_secs(3600));
        touch_irrelevant(&path);
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            sweep_slices(&metrics),
            2,
            "an expired interval arms the next reconcile poll even under emitting-poll churn"
        );

        cleanup(&path);
    }

    /// Eligibility condition 3 (§2.2): a poll whose fast path emitted records
    /// does not also pay sweep cost — and the very next quiet reconcile poll
    /// does, so the block is provably the emission and not something else.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_poll_that_emits_records_attaches_no_sweep_slice() {
        let path = unique_db_path("sweep-busy");
        let _db = seed_fixture_db(&path);
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let batches =
            run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert!(
            !all_event_rows(&batches).is_empty(),
            "the cold poll must actually emit for this test to mean anything"
        );
        assert_eq!(
            sweep_slices(&metrics),
            0,
            "an emitting fast path blocks the slice"
        );

        touch_irrelevant(&path);
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            sweep_slices(&metrics),
            1,
            "the next quiet reconcile poll sweeps"
        );

        cleanup(&path);
    }

    /// Eligibility condition 4 (§2.2): a replacement replay never attaches a
    /// slice. The fixture's relevant keyspace is empty so conditions 1-3 all
    /// hold and only the replay stands between the poll and a slice — a
    /// non-empty fixture would block on condition 3 (the replay re-emits
    /// everything) and this guard could not fail.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replacement_replay_poll_attaches_no_sweep_slice() {
        let path = unique_db_path("sweep-replay");
        {
            let db = create_kv_db(&path);
            db.execute(
                "INSERT INTO ItemTable (key, value) VALUES ('seed', 'x')",
                [],
            )
            .expect("seed irrelevant row");
        }
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            sweep_slices(&metrics),
            1,
            "an empty keyspace sweeps (and wraps) immediately"
        );

        // A changed exclusion set starts a replacement replay.
        let mut replaying = config.clone();
        replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
        touch_irrelevant(&path);
        run_poll_with_config(&replaying, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            sweep_slices(&metrics),
            1,
            "a replacement replay poll must not attach a slice"
        );

        cleanup(&path);
    }

    /// Plan §7.2 F2, the widening direction, at the Cursor call site: an
    /// `error`-status checkpoint with **no block reason** is an ordinary
    /// transient failure marker, and the poll it precedes is an ordinary poll.
    /// Widening `retry_blocked_replay` to a bare `status == "error"` turns it
    /// into a blocked-replacement retry: the cursor state is reset and every
    /// unchanged row re-emits behind a fresh `BeginReplay`. The unchanged
    /// fixture emits nothing on the correct path, so any re-emission fails
    /// this test.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_error_marker_without_a_block_reason_is_not_retried_as_a_blocked_replay() {
        let path = unique_db_path("error-marker-width");
        let _db = seed_fixture_db(&path);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        let first = run_poll(&work, &checkpoints).await;
        assert!(!all_event_rows(&first).is_empty());

        // Rewrite the committed checkpoint into a transient-error marker: the
        // shape `record_scan_failure_outcome`'s non-replay arm persists.
        {
            let mut map = checkpoints.write().await;
            let checkpoint = map.get_mut(&cp_key).expect("committed checkpoint");
            checkpoint.status = "error".to_string();
            checkpoint.block_reason.clear();
            let mut state = CursorState::parse(&checkpoint.cursor_json);
            state.last_error = ERROR_KIND_SCAN.to_string();
            checkpoint.cursor_json = state.serialize();
        }

        let batches = run_poll(&work, &checkpoints).await;
        assert!(
            all_event_rows(&batches).is_empty(),
            "an ordinary error marker must clear through an ordinary scan; \
             re-emission means the poll was retried as a blocked replay"
        );
        let map = checkpoints.read().await;
        assert_eq!(
            map.get(&cp_key).expect("checkpoint").status,
            "active",
            "the transient marker clears once a scan succeeds"
        );

        cleanup(&path);
    }

    /// §2.2's fairness sentence, literally: "fast-path activity never resets
    /// the sweep cursor." A slice leaves the cursor mid-cycle; a genuinely
    /// *emitting* watcher poll then persists a checkpoint of its own, and the
    /// persisted sweep cursor must ride through it unchanged. This is the only
    /// test that observes `sweep: prior.sweep.clone()` in the fast path's
    /// state constructor — every slice-carrying poll overwrites that field
    /// from the driver's report, so G5a cannot see it.
    ///
    /// MUTATION (executed 2026-07-31): `sweep: prior.sweep.clone()` →
    /// `sweep: SweepState::default()` in `scan_database` — this test fails
    /// (the emitting poll wipes the mid-cycle cursor) while G5a and G5b stay
    /// green, which is why this test exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn fast_path_activity_never_resets_the_sweep_cursor() {
        let path = unique_db_path("sweep-fastpath-preserves");
        let keys = seed_junk_fixture(&path, 6, 64);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_max_payload_rows = 2;
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 3600;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        let reconcile = reconcile_work(&path);
        run_poll_with_config(&config, &reconcile, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(sweep_slices(&metrics), 1);
        assert_eq!(
            persisted_sweep(&checkpoints, &reconcile).await.cursor,
            keys[1],
            "one 2-row slice leaves the cursor mid-cycle"
        );

        // A real bubble arrives: the watcher poll emits and persists its own
        // checkpoint, with no slice attached (interval far in the future).
        {
            let db = Connection::open(&path).expect("reopen fixture");
            put(
                &db,
                &format!("bubbleId:{COMPOSER_ID}:{USER_BUBBLE_ID}"),
                &user_bubble_value(),
            );
        }
        let watcher = sqlite_work(&path);
        let batches =
            run_poll_with_config(&config, &watcher, &checkpoints, &poll_state, &metrics).await;
        assert!(
            !all_event_rows(&batches).is_empty(),
            "the bubble must actually emit for this test to mean anything"
        );
        assert_eq!(sweep_slices(&metrics), 1, "no slice on the emitting poll");
        assert_eq!(
            persisted_sweep(&checkpoints, &reconcile).await.cursor,
            keys[1],
            "fast-path activity must never reset the sweep cursor"
        );

        cleanup(&path);
    }

    /// Eligibility condition 1's width, the direction G6a cannot see: mutating
    /// `trigger == Reconcile` to `trigger != Watcher` keeps
    /// `sweep_does_not_run_on_watcher_triggered_polls` green while making
    /// every startup poll sweep-eligible — the §2.4 inversion D1c refuses at
    /// the dispatcher, and the cost plan §7.2 F4 warns the tee/backfill sites
    /// about. This is F4's closure: eligibility is now denominated on
    /// something a test observes at the poll itself, so a call site
    /// re-introducing its own trigger stamp has a named failure here.
    ///
    /// MUTATION (executed 2026-07-31): `work.trigger == WorkTrigger::Reconcile`
    /// → `work.trigger != WorkTrigger::Watcher` in `process_cursor_sqlite_db`
    /// — this test fails (a startup poll attaches a slice) while G6a stays
    /// green, which is why this test exists; both RED and green were
    /// confirmed in a filtered run of the pair, so suite-wide isolation is
    /// not claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_startup_poll_attaches_no_sweep_slice() {
        let path = unique_db_path("sweep-startup");
        seed_junk_fixture(&path, 3, 64);
        let startup = WorkItem {
            trigger: WorkTrigger::Startup,
            ..sqlite_work(&path)
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &startup, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            sweep_slices(&metrics),
            0,
            "a startup poll must never attach a sweep slice"
        );
        assert_eq!(sweep_rows(&metrics), 0);

        // The identical poll under a reconcile trigger sweeps, so the block
        // above is provably the trigger and nothing else.
        touch_irrelevant(&path);
        let reconcile = reconcile_work(&path);
        run_poll_with_config(&config, &reconcile, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(sweep_slices(&metrics), 1);

        cleanup(&path);
    }

    /// Eligibility condition 3's budget clause, and the width of its ×2:
    /// a poll that consumed at least half its fast-path budget — without
    /// emitting and without degrading — attaches no slice, and the identical
    /// poll under an ample budget does. Three 64-byte values against a
    /// 300-byte budget is 192 bytes: over one half, under the whole, so this
    /// test separates the half-consumed clause from the degraded override
    /// beside it and from exhaustion.
    ///
    /// MUTATION (executed 2026-07-31): drop the `is_half_consumed_by` clause
    /// from the eligibility check in `scan_database` — fails. Drop the
    /// `saturating_mul(2)` (making it a full-budget check) — fails, because
    /// 192 < 300. Each RED was confirmed in a filtered run, so suite-wide
    /// isolation is not claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_poll_that_spent_half_its_budget_attaches_no_sweep_slice() {
        let path = unique_db_path("sweep-half-budget");
        seed_junk_fixture(&path, 3, 64);
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;
        config.ingest.sqlite.fast_path_max_payload_bytes = 300;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            degraded_scans(&metrics),
            0,
            "192 of 300 bytes is not exhaustion"
        );
        assert_eq!(
            sweep_slices(&metrics),
            0,
            "a half-spent fast path must not also pay sweep cost"
        );

        // The same quiet database under the default budget sweeps at once.
        let ample = moraine_config::AppConfig::default();
        let mut ample_sqlite = ample;
        ample_sqlite.ingest.sqlite.sweep_slice_min_interval_seconds = 0;
        touch_irrelevant(&path);
        run_poll_with_config(&ample_sqlite, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(sweep_slices(&metrics), 1);

        cleanup(&path);
    }

    /// The deliberate widening of condition 3 (recorded in the plan's §7.1):
    /// a *degraded* poll is sweep-eligible even though it consumed its whole
    /// budget. On a source larger than one fast-path budget every poll is
    /// degraded and fully spent, so the literal half-budget clause would
    /// block the sweep on exactly the sources whose cold tail only the sweep
    /// can reach — §0's coverage guarantee outranks §2.2's politeness. Here
    /// the slice covers, in one poll, the very keys the fast path's budget
    /// skipped.
    ///
    /// MUTATION (executed 2026-07-31): drop `ledger.coverage_degraded ||`
    /// from the eligibility check in `scan_database` — this test fails (no
    /// slice attaches); RED was confirmed in a filtered run,
    /// so suite-wide isolation is not claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_budget_degraded_poll_still_attaches_its_sweep_slice() {
        let path = unique_db_path("sweep-degraded");
        seed_junk_fixture(&path, 4, 64);
        let work = reconcile_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.sweep_slice_min_interval_seconds = 0;
        config.ingest.sqlite.fast_path_max_payload_rows = 2;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(degraded_scans(&metrics), 1, "two of four keys is degraded");
        assert_eq!(
            sweep_slices(&metrics),
            1,
            "the degraded poll still sweeps — that is the whole point"
        );
        assert!(
            sweep_rows(&metrics) >= 2,
            "the slice reads keys the fast path skipped; got {}",
            sweep_rows(&metrics)
        );
        let state = persisted_sweep(&checkpoints, &work).await;
        assert_eq!(
            state.completed_cycles, 1,
            "four small keys fit one slice, so the cycle completes"
        );

        cleanup(&path);
    }

    /// §2.3's "persist the resume position, continue next poll", end to end,
    /// with **no further writes to the database** — the case the cheap stat
    /// short-circuit would otherwise seal forever. Three properties in one
    /// deliberate sequence:
    ///
    /// 1. **Progress**: each resumed poll reads never-read keys first, so a
    ///    6-key database under a 2-row budget is fully covered in exactly 3
    ///    polls (a recency-only order re-reads the same 2 newest keys and
    ///    never converges).
    /// 2. **Termination**: once coverage completes, `pending_coverage`
    ///    clears durably and the next poll short-circuits — the resume
    ///    cannot become a 30 s busy loop on a quiet database.
    /// 3. **§2.6 serialization**: while pending, the marker rides
    ///    `cursor_json`; after completion (never having swept) the payload
    ///    carries neither `pending_coverage` nor `sweep`, byte-identical to a
    ///    source that never degraded.
    ///
    /// MUTATION (executed 2026-07-31): drop the `!state.pending_coverage`
    /// conjunct from the cheap short-circuit — fails at the poll-2 coverage
    /// assertion (the unchanged stat ends every later poll). Drop the
    /// never-read-first class from the candidate sort (plain `rowid DESC`) —
    /// fails at the same assertion (polls re-read `k5,k4` forever). Each RED
    /// was confirmed in a filtered run, so suite-wide isolation is not
    /// claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_degraded_cold_ingest_completes_without_new_writes() {
        let path = unique_db_path("cold-ingest-converges");
        seed_junk_fixture(&path, 6, 64);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_rows = 2;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        {
            let map = checkpoints.read().await;
            let checkpoint = map.get(&cp_key).expect("cold poll persists");
            assert!(
                checkpoint.cursor_json.contains("pending_coverage"),
                "the resume marker must be durable, not volatile"
            );
            let state = CursorState::parse(&checkpoint.cursor_json);
            assert_eq!(state.kv_hashes.len(), 2, "the cold poll covered its budget");
        }

        // No touches: the stat never moves again. Two more polls must still
        // scan, and must retire never-read debt rather than re-verify.
        for expected_covered in [4usize, 6] {
            run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
            let map = checkpoints.read().await;
            let checkpoint = map.get(&cp_key).expect("resumed poll persists");
            let state = CursorState::parse(&checkpoint.cursor_json);
            assert_eq!(
                state.kv_hashes.len(),
                expected_covered,
                "each resumed poll must retire one budget of never-read keys"
            );
        }
        {
            let map = checkpoints.read().await;
            let checkpoint = map.get(&cp_key).expect("covering poll persists");
            assert!(
                !checkpoint.cursor_json.contains("pending_coverage"),
                "completed coverage must clear the marker durably"
            );
            assert!(
                !checkpoint.cursor_json.contains("\"sweep\""),
                "a never-swept source's cursor_json stays byte-compatible (§2.6)"
            );
        }

        // Quiesce: with coverage complete and the stat unchanged, the next
        // poll must not scan at all.
        let rows_before = payload_rows(&metrics);
        let census_before = metrics
            .sqlite_poll_census_rows_total
            .load(std::sync::atomic::Ordering::Relaxed);
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;
        assert_eq!(
            payload_rows(&metrics),
            rows_before,
            "a covered, unchanged database must short-circuit"
        );
        assert_eq!(
            metrics
                .sqlite_poll_census_rows_total
                .load(std::sync::atomic::Ordering::Relaxed),
            census_before,
            "not even the census may run"
        );

        cleanup(&path);
    }

    /// A replacement replay reads the whole database regardless of the
    /// fast-path budget: its `FinalizeReplay` publishes the new generation
    /// over the old one, so a budget-degraded replay would publish a hole —
    /// the transient data loss #602's old-complete/new-complete contract
    /// forbids. The replay pays the pre-#601 cost, once, per genuine
    /// replacement.
    ///
    /// MUTATION (executed 2026-07-31): make `process_cursor_sqlite_db` use
    /// `CursorScanPlan::from_config` for replays too — this test fails (the
    /// replay covers 2 of 7 keys); RED was confirmed in a filtered run,
    /// so suite-wide isolation is not claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replacement_replay_reads_past_the_fast_path_budget() {
        let path = unique_db_path("replay-unbudgeted");
        seed_junk_fixture(&path, 7, 64);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_rows = 2;

        let poll_state = VolatilePollMap::new();
        let metrics = Arc::new(Metrics::default());
        run_poll_with_config(&config, &work, &checkpoints, &poll_state, &metrics).await;

        // A changed exclusion set starts a replacement replay under the same
        // tight budget. The replay must ignore it.
        let mut replaying = config.clone();
        replaying.ingest.exclude_project_dirs = vec!["/no/such/dir/**".to_string()];
        touch_irrelevant(&path);
        run_poll_with_config(&replaying, &work, &checkpoints, &poll_state, &metrics).await;

        let map = checkpoints.read().await;
        let checkpoint = map.get(&cp_key).expect("finalized replay checkpoint");
        let state = CursorState::parse(&checkpoint.cursor_json);
        assert_eq!(
            state.kv_hashes.len(),
            7,
            "the replay must cover every key, budget notwithstanding"
        );
        assert!(
            !checkpoint.cursor_json.contains("pending_coverage"),
            "a finalized replay owes nothing"
        );
        assert_eq!(checkpoint.status, "active");

        cleanup(&path);
    }

    /// The byte axis of the fast-path budget at its exact boundary: two
    /// 64-byte values against a 128-byte budget stop the scan at precisely
    /// two rows. `is_exhausted_by` is `>=` on purpose — the row that *meets*
    /// the budget is the last row read (§2.1: commit what was read) — and
    /// this fixture sits exactly on the boundary so the one-token narrowing
    /// to `>` reads a third row and fails here.
    ///
    /// MUTATION (executed 2026-07-31): `payload_bytes >= self.max_payload_bytes`
    /// → `>` — this test fails (3 rows read); the row-axis twin `>=` → `>`
    /// fails `a_cursor_scan_over_budget_reads_no_more_than_its_budget` (3
    /// rows against its 2-row budget). Dropping the byte disjunct entirely
    /// also fails here (all 4 rows read).
    #[test]
    fn a_cursor_byte_budget_binds_exactly_at_its_boundary() {
        let path = unique_db_path("byte-budget-boundary");
        seed_junk_fixture(&path, 4, 64);

        let mut config = moraine_config::AppConfig::default();
        config.ingest.sqlite.fast_path_max_payload_bytes = 128;
        let plan = CursorScanPlan::from_config(&config, None);
        let mut ledger = ScanLedger::default();
        let ScanOutcome::Scanned { .. } = scan_database(
            &path.to_string_lossy(),
            &CursorState::fresh(),
            &plan,
            &mut ledger,
        ) else {
            panic!("an over-budget scan must degrade, not fail");
        };
        assert_eq!(
            ledger.payload_rows, 2,
            "the row that meets the byte budget is the last row read"
        );
        assert_eq!(ledger.payload_bytes, 128);
        assert!(ledger.coverage_degraded);
        assert_eq!(ledger.skipped_rows, 2);

        cleanup(&path);
    }

    /// Plan §7.2 F1, the `new_state == prior_state_covered` conjunct of
    /// `scan_is_noop` — the one whose failure mode is a durable-state change
    /// suppressed forever. A value mutation that synthesizes no record (junk
    /// payloads) moves only the hash map: every other conjunct says "no-op",
    /// and only the structural comparison notices. Suppress it and the
    /// mutation is re-discovered on every later poll and never durably
    /// recorded.
    ///
    /// MUTATION (executed 2026-07-31): drop `new_state == prior_state_covered`
    /// from `scan_is_noop` — this test fails (the persisted hash never
    /// moves); RED was confirmed in a filtered run, so suite-wide isolation
    /// is not claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_value_that_emits_no_records_still_persists_its_checkpoint() {
        let path = unique_db_path("noop-width-state");
        let keys = seed_junk_fixture(&path, 2, 64);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        run_poll(&work, &checkpoints).await;
        let hash_before = {
            let map = checkpoints.read().await;
            let state = CursorState::parse(&map.get(&cp_key).expect("cold checkpoint").cursor_json);
            state.kv_hashes.get(&keys[0]).cloned().expect("covered key")
        };

        // A plain UPDATE to another non-JSON payload: no record synthesizes,
        // only the content hash moves.
        {
            let db = Connection::open(&path).expect("reopen fixture");
            db.execute(
                "UPDATE cursorDiskKV SET value = 'junk-mutated' WHERE key = ?1",
                rusqlite::params![&keys[0]],
            )
            .expect("mutate value in place");
        }
        let batches = run_poll(&work, &checkpoints).await;
        assert!(
            all_event_rows(&batches).is_empty(),
            "junk payloads must not synthesize records"
        );
        let map = checkpoints.read().await;
        let state = CursorState::parse(&map.get(&cp_key).expect("checkpoint").cursor_json);
        assert_ne!(
            state.kv_hashes.get(&keys[0]),
            Some(&hash_before),
            "the moved hash must be recorded durably, not re-discovered forever"
        );

        cleanup(&path);
    }

    /// Plan §7.2 F1, the `schema_fingerprint == checkpoint.schema_fingerprint`
    /// conjunct: a schema change with no row changes moves nothing else — no
    /// records, no state, no census count — so only this conjunct keeps the
    /// scan from being classed a no-op, and only its checkpoint records the
    /// new fingerprint durably.
    ///
    /// MUTATION (executed 2026-07-31): drop the conjunct from `scan_is_noop`
    /// — this test fails (the persisted fingerprint never moves); RED was
    /// confirmed in a filtered run, so suite-wide isolation is not claimed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_schema_change_with_no_row_changes_still_persists_its_checkpoint() {
        let path = unique_db_path("noop-width-schema");
        seed_junk_fixture(&path, 1, 64);
        let work = sqlite_work(&path);
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let cp_key = checkpoint_key(&work.source_name, &work.path);

        run_poll(&work, &checkpoints).await;
        let fingerprint_before = {
            let map = checkpoints.read().await;
            map.get(&cp_key)
                .expect("cold checkpoint")
                .schema_fingerprint
        };

        {
            let db = Connection::open(&path).expect("reopen fixture");
            db.execute("ALTER TABLE cursorDiskKV ADD COLUMN moraine_probe TEXT", [])
                .expect("alter schema without touching rows");
        }
        let batches = run_poll(&work, &checkpoints).await;
        assert!(all_event_rows(&batches).is_empty());
        let map = checkpoints.read().await;
        assert_ne!(
            map.get(&cp_key).expect("checkpoint").schema_fingerprint,
            fingerprint_before,
            "the new schema fingerprint must be recorded durably"
        );

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
