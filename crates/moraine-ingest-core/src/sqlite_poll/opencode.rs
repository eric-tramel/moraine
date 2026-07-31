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
    hash_str, open_read_only, record_scan_failure, record_scan_ledger, sqlite_data_version,
    stat_fingerprint, take_payload_required_string, truncate_chars_local, ScanBudget, ScanLedger,
    StatFingerprint, SyntheticRecord, VolatilePollMap, CURSOR_STATE_VERSION,
    ERROR_KIND_MIXED_SNAPSHOT, ERROR_KIND_OPEN, ERROR_KIND_ROW_TOO_LARGE, ERROR_KIND_SCAN,
    ERROR_KIND_SCHEMA, SCAN_PAGE_MAX_BYTES, SCAN_PAGE_SIZE,
};

/// The paged event read, shared with the fixture-plan assertion in
/// `opencode_fixture_uses_the_production_event_index` so the plan that test
/// certifies is the plan this adapter actually runs. A copy in the test would
/// let the two drift and the certification would become meaningless.
const OPENCODE_EVENT_PAGE_SQL: &str = "SELECT id, aggregate_id, seq, type, data FROM event \
     WHERE aggregate_id = ?1 AND seq > ?2 AND seq <= ?3 \
     ORDER BY seq LIMIT ?4";

/// Event types that feed the reconstruction context, split by which map they
/// update. **Single source of truth** (issue #601 §3.1 Change 3):
/// `update_opencode_context` dispatches on these slices and
/// `opencode_context_rebuild_sql` derives its `IN` list from their
/// concatenation, so a new context-bearing event type cannot be added to one
/// and forgotten in the other.
const OPENCODE_SESSION_CONTEXT_EVENT_TYPES: &[&str] = &["session.created.1", "session.updated.1"];
const OPENCODE_MESSAGE_CONTEXT_EVENT_TYPES: &[&str] = &["message.updated.1"];

/// Ceilings on the persisted reconstruction context (issue #601 §3.1
/// Change 4). This is a **size budget, not an absence check**: WI-06 requires
/// persisting the context (the old "cursor stays bounded" absence assertion
/// directly contradicted it), and what stays bounded is the serialized
/// footprint. Enforced by evicting whole aggregates — never by failing the
/// scan — because eviction is cheap to recover from: the next delta poll of an
/// evicted aggregate runs the type-scoped rebuild, measured at 95 rows / 47 KB
/// / ~0.5 ms against the reference host's 639-event aggregate (§1.2).
///
/// Eviction order is ascending `aggregate_id`, and that is a *disclosed
/// compromise*, not an oversight: OpenCode's `seq` is per-aggregate, so no
/// cross-aggregate recency ordering exists in the store to be "least recently
/// used" against. A deterministic order is mandatory (the maps ride
/// `cursor_json`, hashed into the #602 transition digest — §2.6), and the
/// rebuild cost above is what makes the ordering's quality nearly irrelevant.
const MAX_OPENCODE_CONTEXT_ENTRIES: usize = 20_000;
const MAX_OPENCODE_CONTEXT_BYTES: usize = 4 * 1024 * 1024;

const OPENCODE_LONG_BINARY_STRING_CHARS: usize = 65_536;
const OPENCODE_DUPLICATE_SESSION_MESSAGE_TYPES: &[&str] = &["user", "assistant"];

/// The §3.1 Change 3 bounded context rebuild: type-scoped (index-driven, never
/// a full replay) and bounded at the resume watermark — events past it are
/// walked by the page loop itself, so the rebuild reconstructs history only.
fn opencode_context_rebuild_sql() -> String {
    let types = OPENCODE_SESSION_CONTEXT_EVENT_TYPES
        .iter()
        .chain(OPENCODE_MESSAGE_CONTEXT_EVENT_TYPES)
        .map(|event_type| format!("'{event_type}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT id, aggregate_id, seq, type, data FROM event \
         WHERE aggregate_id = ?1 AND seq <= ?2 AND type IN ({types}) \
         ORDER BY seq"
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
struct OpenCodeSessionContext {
    #[serde(default)]
    directory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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

/// `Eq` is deliberately absent: the persisted contexts carry `serde_json`
/// values (`model`), which are `PartialEq` only. `scan_is_noop`'s structural
/// comparison needs `PartialEq` alone.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
struct OpenCodeState {
    version: u32,
    format: String,
    #[serde(default)]
    stat: StatFingerprint,
    #[serde(default)]
    event_scan_complete: bool,
    #[serde(default)]
    aggregate_sequences: BTreeMap<String, i64>,
    /// Persisted reconstruction context (issue #601 §3.1 Change 2): the exact
    /// inputs `enrich_message_record` / `enrich_part_record` /
    /// `enrich_session_message_record` and `push_opencode_record`'s
    /// `project_dir` derivation consume, seeded from the prior poll and
    /// updated in place by delta events — which is what lets the page loop
    /// start at the watermark instead of replaying history for context.
    /// Keyed by session id (OpenCode's aggregate id for session aggregates).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    session_contexts: BTreeMap<String, OpenCodeSessionContext>,
    /// Message contexts, nested by session (aggregate) id then message id, so
    /// the §3.1 Change 4 ceiling can evict whole aggregates and the Change 7
    /// disappearance rule can drop them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    message_contexts: BTreeMap<String, BTreeMap<String, OpenCodeMessageContext>>,
    #[serde(default)]
    project_exclusions_hash: u64,
    #[serde(default)]
    last_error: String,
    /// True while a work budget deliberately left known events unread this
    /// generation (issue #601 §2.3's persisted resume marker). The cheap stat
    /// short-circuit must not fire while set, or a quiet store's remainder is
    /// unreachable forever. A function of committed scan decisions — never a
    /// timestamp — so it rides the #602 digest safely (§2.6), and omitted
    /// while false so `cursor_json` stays byte-identical for covered stores.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pending_coverage: bool,
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

    /// Enforce the §3.1 Change 4 context ceilings by evicting whole aggregates
    /// (session context plus that aggregate's message contexts together, so an
    /// aggregate is either enrichable or cleanly rebuildable — never half of
    /// each). Returns the number of context *entries* dropped. Removal is in
    /// batches of one-eighth of the aggregate set per round so a pathological
    /// many-small-entries payload does not re-serialize per entry. See the
    /// ceiling constants for why the order is ascending `aggregate_id`.
    fn evict_contexts_to_fit(&mut self, max_entries: usize, max_bytes: usize) -> u64 {
        let mut evicted = 0u64;
        loop {
            let entries = self
                .message_contexts
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
                + self.session_contexts.len();
            let bytes = serde_json::to_string(&(&self.session_contexts, &self.message_contexts))
                .map(|raw| raw.len())
                .unwrap_or(0);
            if entries <= max_entries && bytes <= max_bytes {
                return evicted;
            }
            let aggregates: Vec<String> = self
                .session_contexts
                .keys()
                .chain(self.message_contexts.keys())
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            if aggregates.is_empty() {
                return evicted;
            }
            let batch = (aggregates.len().div_ceil(8)).max(1);
            for id in aggregates.into_iter().take(batch) {
                if self.session_contexts.remove(&id).is_some() {
                    evicted += 1;
                }
                if let Some(messages) = self.message_contexts.remove(&id) {
                    evicted += messages.len() as u64;
                }
            }
        }
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

/// One skipped row, destined for a single `ingest_errors` row (§2.3): an
/// un-processable single event — larger than `SCAN_PAGE_MAX_BYTES` — is
/// reported once and advanced past, never allowed to fail the scan. The
/// watermark advancing past it is what makes the report one-shot.
#[derive(Debug, Clone)]
struct OpenCodeRowError {
    source_line_no: u64,
    error_kind: &'static str,
    error_text: String,
}

enum OpenCodeScanOutcome {
    Scanned {
        records: Vec<SyntheticRecord>,
        new_state: Box<OpenCodeState>,
        schema_fingerprint: u64,
        relevant_rows: u64,
        row_errors: Vec<OpenCodeRowError>,
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
    poll_state: &VolatilePollMap,
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
    let had_committed = committed.is_some();

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.clone(),
        source_inode: inode,
        source_generation: 1,
        status: "active".to_string(),
        ..Default::default()
    });

    let mut state = OpenCodeState::parse(&checkpoint.cursor_json);
    let current_exclusions_hash = super::project_exclusions_hash(config);
    let policy_fingerprint =
        super::sqlite_policy_fingerprint(SOURCE_FORMAT_OPENCODE_SQLITE, current_exclusions_hash);

    // Replaced databases restart logical identities. Changed exclusions replay
    // the event history so rows skipped under the prior policy can return.
    let generation_changed = had_committed && checkpoint.source_inode != inode;
    let exclusions_changed =
        had_committed && state.project_exclusions_hash != current_exclusions_hash;
    // The `replaying` disjunct resumes a replay a crash interrupted between
    // `BeginReplay` and `FinalizeReplay`; the `error` disjunct cannot cover it,
    // because a crash never wrote a block reason. Width note at the Cursor
    // site; `a_crash_interrupted_opencode_replay_resumes_from_its_replaying_status`
    // fails if it goes. It also feeds the throttle gate immediately below, so
    // its width is load-bearing for that gate too.
    let retry_blocked_replay = checkpoint.status == "replaying"
        || (checkpoint.status == "error" && !checkpoint.block_reason.is_empty());

    // The failure backoff gates the **whole poll**, not just the barrier and
    // the scan behind it (issue #601 §2.1(2), §2.5). The rewind preflight below
    // opens the database and walks `event_sequence`, and it used to run ahead
    // of this return, so a durably blocked database still paid for a read — and
    // still charged metrics — on every single tick while everything downstream
    // was throttled. `starts_replacement` is not known yet, but its other two
    // disjuncts are, and they are the only ones that may bypass the throttle: a
    // rewind discovered by the preflight cannot be a reason to have run the
    // preflight.
    //
    // **Both bypass conjuncts are genuinely load-bearing here, unlike the
    // `!starts_replacement` conjunct on the Cursor and NAC gates.** There the
    // gate sits *below* the generation bump, so `failure_retry_due` is asked
    // about a generation the volatile entry has never seen and answers `true`
    // whatever the conjunct says — an equivalent mutant. This gate has to sit
    // *above* the bump (the bump depends on `sequence_rewound`, which is what
    // the preflight computes), so `checkpoint.source_generation` is still the
    // old value and `failure_retry_due` genuinely answers `false`. Drop either
    // conjunct and an OpenCode store whose file was replaced, or whose
    // exclusion set changed, is ignored for up to `FAILURE_BACKOFF_MAX`.
    // `a_replaced_opencode_database_bypasses_the_blocked_replay_throttle` and
    // `an_exclusion_change_bypasses_the_opencode_blocked_replay_throttle` fail,
    // one per conjunct.
    if retry_blocked_replay
        && !generation_changed
        && !exclusions_changed
        && !poll_state.failure_retry_due(&cp_key, checkpoint.source_generation)
    {
        return Ok(());
    }

    let sequence_rewound = if had_committed && current_stat != state.stat {
        let scan_db_path = source_file.clone();
        let prior = state.clone();
        let (rewound, preflight_ledger) = tokio::task::spawn_blocking(move || {
            let mut ledger = ScanLedger::default();
            let rewound = opencode_sequences_rewound(&scan_db_path, &prior, &mut ledger);
            (rewound, ledger)
        })
        .await
        .context("opencode_sqlite sequence preflight panicked")?;
        record_scan_ledger(metrics, &preflight_ledger);
        rewound.unwrap_or(false)
    } else {
        false
    };

    let starts_replacement = generation_changed || exclusions_changed || sequence_rewound;
    if starts_replacement {
        checkpoint.source_inode = inode;
        checkpoint.source_generation =
            crate::publication::checked_next_generation(checkpoint.source_generation)
                .context("source generation exhausted while replacing opencode_sqlite database")?;
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
    }
    let replacement_replay = starts_replacement || retry_blocked_replay;
    if replacement_replay {
        state = OpenCodeState::fresh();
    }
    state.project_exclusions_hash = current_exclusions_hash;
    if replacement_replay {
        // BeginReplay is itself durable. Persist the reset cursor with that
        // boundary so a crash before the scan cannot rediscover the same
        // rewind from the previous generation and bump again.
        checkpoint.cursor_json = state.serialize();
    }
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
        .context("opencode_sqlite poll sequence exhausted")?;
    if replacement_replay {
        super::begin_database_replay(&sink_tx, &checkpoint, scan_boundary, &policy_fingerprint)
            .await?;
    }

    // Cheap no-change short-circuit: nothing touched the database or its WAL
    // sidecars since the last poll. `event_scan_complete` also forces one real
    // scan after upgrading from the earlier projection cursor state.
    // §2.5's `|| !failure_retry_due` disjunct is absent and **must stay absent
    // while the contention clock lives in `failure_retry_due`** — WI-04 must
    // not add it. It is no longer outcome-redundant with `should_skip_poll`'s
    // failure arm below: §3.2's contention exemption keeps that clock out of
    // `should_skip_poll` on purpose, so after a mixed-snapshot rejection
    // `failure_retry_due` is false for up to 60 s while `should_skip_poll`
    // stays false. The disjunct would skip ordinary polls of an actively
    // written OpenCode store for that whole window — the §6 prompt-visibility
    // regression the exemption exists to prevent. A pre-`should_skip_poll`
    // throttle for the sweep slice must read the fault ladder alone.
    // `an_ordinary_poll_of_a_contended_opencode_store_is_not_throttled` fails
    // if it is added. See `plans/601-delta-sqlite.md` §7 WI-10.
    //
    // The `!pending_coverage` conjunct is §2.3's "continue next poll": while a
    // budget remainder exists an unchanged stat must not end the poll (a quiet
    // store's stat never moves again). Terminates because each resumed poll
    // retires at least one budget of the remainder and the flag clears with
    // the covering scan's checkpoint
    // (`a_degraded_opencode_cold_ingest_completes_without_new_writes`).
    if state.stat == current_stat
        && state.event_scan_complete
        && state.last_error.is_empty()
        && !state.pending_coverage
    {
        return Ok(());
    }

    // Volatile short-circuit + rescan backoff (issue #443): no-op scans leave
    // the durable checkpoint untouched, so their coverage lives here instead.
    // Skipped during a replay so the barrier always has a scan behind it.
    //
    // Both halves are pinned, one test each, mirroring the Cursor site:
    // `an_opencode_blocked_replay_scans_behind_its_barrier` fails if the
    // `!replacement_replay` guard goes (the durable barrier is sent and the
    // skip then fires behind it, one barrier per tick forever), and
    // `a_failed_opencode_scan_backs_off_instead_of_rescanning_every_tick` fails
    // if the call goes (a database whose scan fails re-runs the whole failed
    // scan on every reconcile tick and every debounced watcher event).
    if !replacement_replay
        && poll_state.should_skip_poll(&cp_key, checkpoint.source_generation, &current_stat)
    {
        return Ok(());
    }

    let scan_db_path = source_file.clone();
    let scan_state = state.clone();
    // The fast-path work budget (issue #601 §2.1), from `[ingest.sqlite]`.
    // Exceeding it commits what was read and degrades coverage; it never fails
    // the scan. A replacement replay is unbudgeted: its finalize publishes the
    // generation whole, so degrading it would publish a hole through #602
    // (`an_opencode_replacement_replay_reads_past_the_fast_path_budget`).
    let budget = if replacement_replay {
        ScanBudget::unbounded()
    } else {
        ScanBudget::fast_path(&config.ingest.sqlite)
    };
    let (outcome, ledger) = tokio::task::spawn_blocking(move || {
        let mut ledger = ScanLedger::default();
        let outcome = scan_opencode_database(&scan_db_path, &scan_state, &budget, &mut ledger);
        (outcome, ledger)
    })
    .await
    .context("opencode_sqlite scan task panicked")?;
    record_scan_ledger(metrics, &ledger);

    match outcome {
        OpenCodeScanOutcome::Scanned {
            records,
            mut new_state,
            schema_fingerprint,
            relevant_rows,
            row_errors,
        } => {
            new_state.stat = current_stat;
            new_state.last_error = String::new();

            // A no-op scan: only the stat fingerprint moved — no record was
            // emitted and nothing the durable checkpoint carries changed.
            // Persisting a checkpoint here would append an
            // `ingest_checkpoints` row per WAL touch forever (issue #443);
            // record the covered stat in volatile state instead and send
            // nothing. The comparison is structural (stat normalized away)
            // so any future `OpenCodeState` field is durable by default.
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
                && schema_fingerprint == checkpoint.schema_fingerprint;
            if scan_is_noop {
                poll_state.record_noop_scan(&cp_key, checkpoint.source_generation, new_state.stat);
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
            // Per-row skips (§2.3): one `ingest_errors` row each, one-shot
            // because the aggregate watermark advanced past the skipped event.
            for row_error in &row_errors {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": row_error.source_line_no,
                    "source_offset": 0u64,
                    "error_kind": row_error.error_kind,
                    "error_text": row_error.error_text,
                    "raw_fragment": "",
                }));
            }
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
                                "opencode_sqlite row {} failed normalization: {exc}",
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
                        .context("sink channel closed while sending opencode_sqlite chunk")?;
                }
            }

            let emitted = records.len();
            let final_checkpoint = Checkpoint {
                source_name: work.source_name.clone(),
                source_file: source_file.clone(),
                source_inode: inode,
                source_generation: checkpoint.source_generation,
                last_offset: scan_boundary,
                last_line_no: relevant_rows,
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
                .context("sink channel closed while sending final opencode_sqlite batch")?;
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
                super::finalize_database_replay(
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
                    "{}:{} opencode_sqlite emitted {} changed records ({} relevant rows, \
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
        OpenCodeScanOutcome::Failed {
            error_kind,
            error_text,
        } => {
            record_scan_failure(metrics);
            poll_state.record_scan_failure_outcome(
                &cp_key,
                checkpoint.source_generation,
                error_kind,
            );

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
                "error_text": error_text.clone(),
                "raw_fragment": "",
            }));

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
                .context("sink channel closed while sending opencode_sqlite error batch")?;
            if replacement_replay {
                super::block_database_replay(&sink_tx, &error_checkpoint, error_text).await?;
            }
            Ok(())
        }
    }
}

/// The caller owns `ledger` so that every early return — including each
/// failure arm — still reports the bytes this scan had already paid for.
///
/// **Ceiling degradation (issue #601 §2.3, G4 row 4).** This scan has no
/// history-size failure mode: `MAX_OPENCODE_RELEVANT_ROWS` /
/// `MAX_OPENCODE_SCAN_BYTES` and their `sqlite_cursor_too_large` arms are
/// retired, along with the per-aggregate `sum(length(data))` preflight that
/// fed them — which also removes the preflight's second full pass over `data`
/// (the ~2× §1.1 finding 2 predicted) and one of the duplicate
/// `event_sequence` reads (§3.1 Change 5): the scan now reads
/// `event_sequence` exactly once, here, and threads it down. What bounds a
/// poll is the per-poll work budget; exceeding it commits what was read and
/// records `coverage_degraded`.
fn scan_opencode_database(
    db_path: &str,
    prior: &OpenCodeState,
    budget: &ScanBudget,
    ledger: &mut ScanLedger,
) -> OpenCodeScanOutcome {
    let connection = match open_read_only(db_path) {
        Ok(connection) => connection,
        Err(exc) => {
            return OpenCodeScanOutcome::Failed {
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
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read OpenCode pre-scan data_version: {exc:#}"),
            }
        }
    };

    let schema_fingerprint = match validate_opencode_schema(&connection, ledger) {
        Ok(fingerprint) => fingerprint,
        Err(text) => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_SCHEMA,
                error_text: text,
            }
        }
    };

    // §3.1 Change 5: the single `event_sequence` read this scan performs.
    let aggregates = match opencode_aggregate_sequences(&connection, ledger) {
        Ok(aggregates) => aggregates,
        Err(exc) => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("{exc:#}"),
            }
        }
    };

    let result = scan_opencode_rows(&connection, prior, &aggregates, budget, ledger);
    let data_version_after = match sqlite_data_version(&connection) {
        Ok(value) => value,
        Err(exc) => {
            return OpenCodeScanOutcome::Failed {
                error_kind: ERROR_KIND_SCAN,
                error_text: format!("failed to read OpenCode post-scan data_version: {exc:#}"),
            }
        }
    };
    if super::snapshot_is_mixed(
        db_path,
        data_version_before,
        data_version_after,
        opened_stat,
    ) {
        return OpenCodeScanOutcome::Failed {
            error_kind: ERROR_KIND_MIXED_SNAPSHOT,
            error_text:
                "OpenCode database changed during the paged scan; retrying without advancing the cursor"
                    .to_string(),
        };
    }

    match result {
        Ok(scan) => OpenCodeScanOutcome::Scanned {
            records: scan.records,
            new_state: scan.new_state,
            schema_fingerprint,
            relevant_rows: scan.relevant_rows,
            row_errors: scan.row_errors,
        },
        Err(OpenCodeScanError::Scan(exc)) => OpenCodeScanOutcome::Failed {
            error_kind: ERROR_KIND_SCAN,
            error_text: format!("{exc:#}"),
        },
    }
}

fn validate_opencode_schema(
    connection: &Connection,
    ledger: &mut ScanLedger,
) -> std::result::Result<u64, String> {
    let mut schema_material = String::new();
    for (table, required_columns) in [
        ("event", EVENT_COLUMNS),
        ("event_sequence", EVENT_SEQUENCE_COLUMNS),
    ] {
        let schema_sql =
            super::schema_sql_for_table(connection, table, ledger).map_err(|exc| match exc {
                rusqlite::Error::QueryReturnedNoRows => {
                    format!("required table {table} is missing")
                }
                other => other.to_string(),
            })?;
        schema_material.push_str(&schema_sql.unwrap_or_default());
        schema_material.push('\n');

        let names = super::table_column_names(connection, table, ledger)
            .map_err(|exc| format!("{exc:#}"))?;
        for column in required_columns {
            if !names.iter().any(|name| name == column) {
                return Err(format!("table {table} is missing required column {column}"));
            }
        }
    }
    Ok(hash_str(&schema_material))
}

/// What one `scan_opencode_rows` call produced.
struct OpenCodeRowsScan {
    records: Vec<SyntheticRecord>,
    new_state: Box<OpenCodeState>,
    relevant_rows: u64,
    row_errors: Vec<OpenCodeRowError>,
}

/// The delta scan (issue #601 §3.1). Each changed aggregate's page loop starts
/// at its persisted watermark (Change 1) — never `-1` — and the persisted
/// contexts supply the history the replay used to rebuild (Change 2), with the
/// type-scoped rebuild as the bounded fallback when an aggregate's context was
/// evicted or predates persistence (Change 3).
///
/// **Ordering under the budget.** Changed *known* aggregates first — their
/// watermark is exact, so this is the §2.3 "newest-first" for OpenCode
/// (`event_sequence.seq > persisted seq` is the one exact recency signal the
/// store has) and what §4's active-session freshness claim rides on — then
/// never-read aggregates (cold debt), ascending `aggregate_id` within each
/// class (no cross-aggregate recency ordering exists; `seq` is per-aggregate).
/// Convergence does not need Cursor/NAC's never-read-first rule here: the
/// per-aggregate watermark is a durable resume position, so the delta class
/// shrinks to nothing and cold debt then takes the whole budget — D8a's
/// argument, per aggregate.
///
/// **Budget binding.** Between events, never before the first this poll (a
/// poll always makes forward progress); the bound event was materialized by
/// SQLite before the check could run, so it is charged one payload row with no
/// bytes — the same honest-ledger rule as NAC's episode loop.
fn scan_opencode_rows(
    connection: &Connection,
    prior: &OpenCodeState,
    aggregates: &[OpenCodeAggregateSequence],
    budget: &ScanBudget,
    ledger: &mut ScanLedger,
) -> std::result::Result<OpenCodeRowsScan, OpenCodeScanError> {
    let mut records = Vec::new();
    let mut accumulated = OpenCodeAccumulated::default();
    let mut aggregate_sequences = BTreeMap::new();
    let mut row_errors = Vec::new();
    let mut relevant_events = 0u64;
    let mut events_processed = 0u64;
    let mut degraded = false;

    // Change 7: a vanished aggregate drops its watermark (absent from the
    // rebuild below) *and* its context entries (filtered here) — it is not a
    // rewind and must never bump the generation (G8b).
    let current_ids: std::collections::BTreeSet<&str> = aggregates
        .iter()
        .map(|aggregate| aggregate.aggregate_id.as_str())
        .collect();
    let mut session_contexts: BTreeMap<String, OpenCodeSessionContext> = prior
        .session_contexts
        .iter()
        .filter(|(id, _)| current_ids.contains(id.as_str()))
        .map(|(id, context)| (id.clone(), context.clone()))
        .collect();
    let mut message_contexts: BTreeMap<String, BTreeMap<String, OpenCodeMessageContext>> = prior
        .message_contexts
        .iter()
        .filter(|(id, _)| current_ids.contains(id.as_str()))
        .map(|(id, contexts)| (id.clone(), contexts.clone()))
        .collect();

    let mut changed: Vec<(&OpenCodeAggregateSequence, i64, bool)> = Vec::new();
    for aggregate in aggregates {
        let scan_from_seq = opencode_scan_from_seq(prior, aggregate);
        if aggregate.seq <= scan_from_seq {
            aggregate_sequences.insert(aggregate.aggregate_id.clone(), scan_from_seq);
            continue;
        }
        let known = prior
            .aggregate_sequences
            .contains_key(&aggregate.aggregate_id);
        changed.push((aggregate, scan_from_seq, known));
    }
    // Delta (known) class before cold (never-read) class; `aggregates` arrives
    // in `aggregate_id` order, and the stable sort keeps it within each class.
    changed.sort_by_key(|(_, _, known)| std::cmp::Reverse(*known));

    for (idx, (aggregate, scan_from_seq, _)) in changed.iter().enumerate() {
        if events_processed > 0 && budget.is_exhausted_by(ledger.payload_rows, ledger.payload_bytes)
        {
            // Commit what was read (§2.1/§2.3): skipped known aggregates keep
            // their watermark ("unread this poll", never "rewound"), skipped
            // never-read aggregates stay absent so a later poll re-detects
            // them. The skip is exact on the rows axis — `seq` bounds it.
            let mut skipped_events = 0u64;
            for (aggregate, scan_from_seq, _) in &changed[idx..] {
                if *scan_from_seq >= 0 {
                    aggregate_sequences.insert(aggregate.aggregate_id.clone(), *scan_from_seq);
                }
                skipped_events = skipped_events
                    .saturating_add(aggregate.seq.saturating_sub((*scan_from_seq).max(-1)) as u64);
            }
            ledger.mark_degraded(skipped_events, 0);
            degraded = true;
            break;
        }

        // Change 3: resuming an aggregate whose session context is missing
        // (evicted, or the cursor predates context persistence) rebuilds it
        // with the type-scoped read — 95 rows / 47 KB / ~0.5 ms on the
        // reference aggregate versus 639 rows / 6.9 MB for the full replay.
        if *scan_from_seq >= 0 && !session_contexts.contains_key(&aggregate.aggregate_id) {
            rebuild_opencode_context(
                connection,
                &aggregate.aggregate_id,
                *scan_from_seq,
                &mut session_contexts,
                &mut message_contexts,
                ledger,
            )?;
        }

        // Change 1: the page loop's lower bound is the persisted watermark.
        let mut last_seq = *scan_from_seq;
        let mut observed_any = false;
        let mut aggregate_bound = false;
        'pages: loop {
            let mut page_rows = 0usize;
            let mut page_bytes = 0usize;
            let mut page_capped = false;
            {
                let mut stmt = connection.prepare_cached(OPENCODE_EVENT_PAGE_SQL)?;
                let mut rows = stmt.query(rusqlite::params![
                    &aggregate.aggregate_id,
                    last_seq,
                    aggregate.seq,
                    SCAN_PAGE_SIZE as i64
                ])?;
                while let Some(row) = rows.next()? {
                    if events_processed > 0
                        && budget.is_exhausted_by(ledger.payload_rows, ledger.payload_bytes)
                    {
                        // The row SQLite just handed over was materialized
                        // before the budget could be consulted: one row on the
                        // rows axis, no bytes (its columns are never taken).
                        // It stays inside the skipped remainder — the
                        // watermark did not advance past it.
                        ledger.charge_payload_row();
                        aggregate_bound = true;
                        break 'pages;
                    }
                    let event = build_event_row(row, ledger)?;
                    if event.data_bytes > SCAN_PAGE_MAX_BYTES {
                        // Un-processable single row (§2.3): one error row,
                        // skip it, advance the watermark past it — one-shot,
                        // never a scan failure and never a stall.
                        row_errors.push(OpenCodeRowError {
                            source_line_no: event.seq.max(0) as u64,
                            error_kind: ERROR_KIND_ROW_TOO_LARGE,
                            error_text: format!(
                                "OpenCode event {} (aggregate {}, seq {}) is {} bytes, \
                                 exceeding the {} byte row ceiling; event skipped",
                                event.id,
                                event.aggregate_id,
                                event.seq,
                                event.data_bytes,
                                SCAN_PAGE_MAX_BYTES
                            ),
                        });
                        last_seq = event.seq;
                        observed_any = true;
                        events_processed += 1;
                        relevant_events += 1;
                        continue;
                    }
                    page_rows += 1;
                    page_bytes = page_bytes.saturating_add(event.data_bytes);
                    update_opencode_context(&event, &mut session_contexts, &mut message_contexts);
                    apply_opencode_event(&event, &mut accumulated);
                    relevant_events += 1;
                    events_processed += 1;
                    observed_any = true;
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
        if aggregate_bound {
            // Mid-aggregate bound: the watermark is the durable resume
            // position (D8a's rule — oldest-first of the new tail, because an
            // exact resume position exists), so the next poll continues from
            // exactly here. Remaining aggregates are handled by the
            // between-aggregate check above on the next iteration.
            if observed_any {
                aggregate_sequences.insert(aggregate.aggregate_id.clone(), last_seq);
            } else if *scan_from_seq >= 0 {
                aggregate_sequences.insert(aggregate.aggregate_id.clone(), *scan_from_seq);
            }
            ledger.mark_degraded(aggregate.seq.saturating_sub(last_seq.max(-1)) as u64, 0);
            degraded = true;
            continue;
        }
        if observed_any {
            aggregate_sequences.insert(aggregate.aggregate_id.clone(), last_seq);
        } else if *scan_from_seq >= 0 {
            aggregate_sequences.insert(aggregate.aggregate_id.clone(), *scan_from_seq);
        }
    }

    for (_, record) in accumulated.sessions {
        push_opencode_record("session", &record, &session_contexts, &mut records);
    }
    for (_, mut record) in accumulated.messages {
        enrich_message_record(&mut record, &session_contexts);
        push_opencode_record("message", &record, &session_contexts, &mut records);
    }
    for (_, mut record) in accumulated.parts {
        enrich_part_record(&mut record, &session_contexts, &message_contexts);
        push_opencode_record("part", &record, &session_contexts, &mut records);
    }
    for (_, mut record) in accumulated.session_messages {
        enrich_session_message_record(&mut record, &session_contexts);
        if opencode_record_is_relevant(&record) {
            push_opencode_record("session_message", &record, &session_contexts, &mut records);
        }
    }

    let mut new_state = OpenCodeState::fresh();
    new_state.project_exclusions_hash = prior.project_exclusions_hash;
    new_state.event_scan_complete = true;
    new_state.aggregate_sequences = aggregate_sequences;
    new_state.session_contexts = session_contexts;
    new_state.message_contexts = message_contexts;
    // §2.3's persisted resume marker: set exactly when this scan deliberately
    // left known events unread. Context eviction below is *not* coverage debt
    // — events are immutable, watermarks are kept, and enrichment rebuilds on
    // demand — so it marks the ledger but not this flag.
    new_state.pending_coverage = degraded;
    // Change 4: the context ceiling, enforced by eviction, never by failing.
    let evicted =
        new_state.evict_contexts_to_fit(MAX_OPENCODE_CONTEXT_ENTRIES, MAX_OPENCODE_CONTEXT_BYTES);
    ledger.mark_evicted(evicted);
    ledger.rows_emitted = records.len() as u64;
    Ok(OpenCodeRowsScan {
        records,
        new_state: Box::new(new_state),
        relevant_rows: relevant_events,
        row_errors,
    })
}

/// §3.1 Change 3: rebuild one aggregate's reconstruction context from its
/// context-bearing events at or below the resume watermark. Type-scoped and
/// index-driven — the whole point is that this is *not* a replay — and charged
/// on the payload axis at the read site like every other `data` read.
fn rebuild_opencode_context(
    connection: &Connection,
    aggregate_id: &str,
    through_seq: i64,
    session_contexts: &mut BTreeMap<String, OpenCodeSessionContext>,
    message_contexts: &mut BTreeMap<String, BTreeMap<String, OpenCodeMessageContext>>,
    ledger: &mut ScanLedger,
) -> std::result::Result<(), OpenCodeScanError> {
    let sql = opencode_context_rebuild_sql();
    let mut stmt = connection.prepare_cached(&sql)?;
    let mut rows = stmt.query(rusqlite::params![aggregate_id, through_seq])?;
    while let Some(row) = rows.next()? {
        let event = build_event_row(row, ledger)?;
        if event.data_bytes > SCAN_PAGE_MAX_BYTES {
            continue;
        }
        update_opencode_context(&event, session_contexts, message_contexts);
    }
    Ok(())
}

fn opencode_sequences_rewound(
    db_path: &str,
    prior: &OpenCodeState,
    ledger: &mut ScanLedger,
) -> Result<bool> {
    if prior.aggregate_sequences.is_empty() {
        return Ok(false);
    }
    let connection = open_read_only(db_path)?;
    // A preflight on its own connection, before the scan's ledger exists; its
    // census is charged into a scratch ledger that is folded in by the caller.
    let current = opencode_aggregate_sequences(&connection, ledger)?
        .into_iter()
        .map(|aggregate| (aggregate.aggregate_id, aggregate.seq))
        .collect::<BTreeMap<_, _>>();
    // §3.1 Change 7: disappearance is not a rewind. An aggregate absent from
    // `event_sequence` (one deleted old session) drops its watermark and
    // context entries in the scan and must not force a whole-database
    // generation bump and full re-ingest (G8b,
    // `opencode_disappearing_aggregate_is_not_a_generation_bump`). Only a
    // genuine `current < prior` regression routes through
    // `begin_database_replay`.
    Ok(prior
        .aggregate_sequences
        .iter()
        .any(|(aggregate_id, prior_seq)| {
            current
                .get(aggregate_id)
                .is_some_and(|current_seq| current_seq < prior_seq)
        }))
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

/// The one narrow read in this adapter: `event_sequence` carries no payload
/// column, so it is charged on the census axis. It runs more than once per
/// poll today (issue #601 §3.1 Change 5) and the ledger now shows that.
fn opencode_aggregate_sequences(
    connection: &Connection,
    ledger: &mut ScanLedger,
) -> Result<Vec<OpenCodeAggregateSequence>> {
    let mut stmt = connection
        .prepare_cached("SELECT aggregate_id, seq FROM event_sequence ORDER BY aggregate_id")?;
    let mut rows = stmt.query([])?;
    let mut aggregates = Vec::new();
    while let Some(row) = rows.next()? {
        let aggregate_id: String = row.get(0)?;
        ledger.charge_census_row(aggregate_id.len());
        aggregates.push(OpenCodeAggregateSequence {
            aggregate_id,
            seq: row.get(1)?,
        });
    }
    Ok(aggregates)
}

fn build_event_row(row: &rusqlite::Row<'_>, ledger: &mut ScanLedger) -> Result<OpenCodeEventRow> {
    // This projection includes `data`, so the whole row is charged on the
    // payload axis at the point SQLite hands it over.
    ledger.charge_payload_row();
    // All four are NOT NULL in OpenCode's `event` table, and `id` /
    // `aggregate_id` are the material behind `source_line_no`/`source_offset`
    // and therefore behind `event_uid` (§6). A NULL is schema drift: fail the
    // scan instead of minting a record keyed on the empty string with
    // `data_bytes = 0`.
    let id = take_payload_required_string(ledger, row, 0)?;
    let aggregate_id = take_payload_required_string(ledger, row, 1)?;
    let event_type = take_payload_required_string(ledger, row, 3)?;
    let data_text = take_payload_required_string(ledger, row, 4)?;
    let data = serde_json::from_str::<Value>(&data_text).unwrap_or_else(|_| json!(data_text));
    Ok(OpenCodeEventRow {
        id,
        aggregate_id,
        seq: row.get(2)?,
        event_type,
        data,
        data_bytes: data_text.len(),
    })
}

fn update_opencode_context(
    event: &OpenCodeEventRow,
    session_contexts: &mut BTreeMap<String, OpenCodeSessionContext>,
    message_contexts: &mut BTreeMap<String, BTreeMap<String, OpenCodeMessageContext>>,
) {
    // Dispatch on the same constants the rebuild SQL is derived from (§3.1
    // Change 3's single source of truth).
    let event_type = event.event_type.as_str();
    if OPENCODE_SESSION_CONTEXT_EVENT_TYPES.contains(&event_type) {
        let info = event.data.get("info").unwrap_or(&event.data);
        if let Some((id, record)) = build_session_event_record(info) {
            let next = session_context_from_record(&record);
            session_contexts
                .entry(id)
                .and_modify(|current| {
                    if !std::path::Path::new(&current.directory).is_absolute()
                        && std::path::Path::new(&next.directory).is_absolute()
                    {
                        current.directory.clone_from(&next.directory);
                    }
                    if next.model.is_some() {
                        current.model.clone_from(&next.model);
                    }
                })
                .or_insert(next);
        }
    } else if OPENCODE_MESSAGE_CONTEXT_EVENT_TYPES.contains(&event_type) {
        let info = event.data.get("info").unwrap_or(&event.data);
        if let Some((id, record)) = build_message_event_record(info) {
            let context = message_context_from_record(&record);
            if let Some(session_id) = record.get("session_id").and_then(Value::as_str) {
                if std::path::Path::new(&context.directory).is_absolute() {
                    let session = session_contexts.entry(session_id.to_string()).or_default();
                    if !std::path::Path::new(&session.directory).is_absolute() {
                        session.directory.clone_from(&context.directory);
                    }
                }
                message_contexts
                    .entry(session_id.to_string())
                    .or_default()
                    .insert(id, context);
            }
        }
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
    message_contexts: &BTreeMap<String, BTreeMap<String, OpenCodeMessageContext>>,
) {
    let session_id = record
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Some(context) = session_contexts.get(&session_id) {
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
    if let Some(context) = message_contexts
        .get(&session_id)
        .and_then(|by_message| by_message.get(message_id))
    {
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
    session_contexts: &BTreeMap<String, OpenCodeSessionContext>,
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
    let session_id = record
        .get("session_id")
        .and_then(Value::as_str)
        .or_else(|| (record_kind == "opencode_session").then_some(id))
        .unwrap_or_default();
    let project_dir = session_contexts
        .get(session_id)
        .map(|context| context.directory.clone())
        .filter(|dir| std::path::Path::new(dir).is_absolute())
        .unwrap_or_default();
    let source_line_no = hash_str(&format!("{table}:{id}"));
    let source_offset = hash_str(&format!(
        "{SOURCE_FORMAT_OPENCODE_SQLITE}:{table}:{id}:{record_kind}"
    ));
    records.push(SyntheticRecord {
        record: value,
        project_dir,
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
