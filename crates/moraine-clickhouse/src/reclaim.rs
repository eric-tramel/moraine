//! Issue #603 WI-04/WI-05 — the storage reclaim ledger, the dry-run planner,
//! and the driver that claims, executes and settles against it.
//!
//! WI-04 built this module's statement surface with **zero production
//! callers** and labelled it as such: the ledger, the planner, the authority
//! types and the emitter had to exist and be trustworthy before the first
//! executor landed, because the first executor is the point at which a bug
//! costs an operator their history. WI-05 supplies the driver and the first
//! two executors, in one change, which is the only order
//! `no_executor_may_be_registered_before_the_ledger_driver_is_wired` permits.
//! WI-07 registers the third executor — the canonical read-index scope
//! (`reclaim_read_index`) — onto the already-wired driver.
//!
//! ## The §3.2 protocol, as implemented
//!
//! 1. **Plan** — [`ClickHouseClient::reclaim_plan`] runs the registered
//!    scope's candidate probe. Writes nothing; this is the entire dry-run
//!    path, and a `run` plans through the same call, so the plan an operator
//!    reads is the plan a run claims.
//! 2. **Claim** — [`ledger_claim_statement`] writes `phase='claimed'` **before
//!    any delete**, from relations the deletes do not touch. Everything after
//!    this point is re-derivable from that one row.
//! 3. **Execute** — advance to `deleting`, then issue the unit's deletes
//!    **children first, parent last** — the inverse of
//!    `reclaim_superseded_mcp_open_snapshots`.
//! 4. **Settle** — `phase='done'`.
//!
//! [`ClickHouseClient::reclaim_redrive`] runs at the head of **every** run,
//! before any candidate is probed, and completes every unit left in `claimed`
//! or `deleting`. Since the janitor calls `reclaim_run` on a fixed tick, the
//! first tick after a restart *is* the startup recovery pass; there is no
//! separate startup path to forget to call.
//!
//! Re-execution is idempotent: every delete is a predicate over the key set
//! the ledger row names, so replaying a completed unit removes zero additional
//! rows. `an_interrupted_claim_is_completed_by_the_next_runs_redrive` measures
//! exactly that, by interrupting a real claim between the child and the parent
//! delete against a stateful server.
//!
//! The `reclaim-restart` **live** gate — SIGKILL of the process between the
//! two deletes, in a sandbox — is not run in this change: Docker is
//! unavailable on the reference host. The in-process test is not a substitute
//! for it and is not described as one; what it does cover is the property the
//! live gate is for, applied to a real statement stream rather than to an
//! enum.
//!
//! **No executor may be registered before the driver is real.** Shipping an
//! executor onto an unimplemented ledger protocol is how a partial delete
//! becomes unrecoverable: the rows would go, nothing durable would record that
//! they were going, and the crash the ledger exists to survive would strand
//! exactly the children it exists to keep reachable. That ordering is not left
//! to a comment — [`LEDGER_DRIVER_WIRED`] and
//! `no_executor_may_be_registered_before_the_ledger_driver_is_wired` enforce
//! it.
//!
//! ## INV-1 / INV-2 (plan §3.1)
//!
//! > **INV-1.** The reclaimer never evaluates whether a generation *should* be
//! > live. It reads a liveness decision already durably recorded by the atomic
//! > publication path and acts only on generations that decision has retired.
//!
//! > **INV-2.** The reclaimer issues **zero writes** to
//! > `published_source_generations`, `ingest_checkpoint_transitions`,
//! > `source_generation_publication_readiness`, `ingest_append_control`, or
//! > `mcp_read_index_state`.
//!
//! INV-2 is what makes "a failed cleanup does not change which generation is
//! live" a structural property rather than a hope. It is proved by
//! [`tests::no_emitted_statement_writes_a_control_relation`] capturing every
//! statement this module can emit across every scope, not by inspection.
//!
//! ## The ledger, and why the delete order is the inverse of the old reclaimer
//!
//! The existing `mcp_open` reclaimer derives its target set *from headers* and
//! deletes the headers first. A crash between statements strands the children
//! **forever**, because the set can never be re-derived; the reference host's
//! 10.9M orphan `mcp_open_events` rows are that bug's output.
//!
//! The ledger makes the set durable independently of the rows, so this driver
//! can delete **children first, parent last**. A crash then leaves an intact,
//! still-authorizable snapshot rather than an unreachable orphan. Reader
//! safety is preserved by the safety horizon and the anti-join — the unit is
//! already non-live before it is claimed — not by delete ordering.
//!
//! ## Bounding, and what a reclaim `DELETE` actually is
//!
//! Every statement runs under a `Background` envelope.
//!
//! **A reclaim delete is a heavyweight mutation, and the code says so because
//! that is what was measured.** An earlier revision of this module claimed
//! "lightweight `DELETE FROM`, never `ALTER … DELETE`, never
//! `mutations_sync = 1`" and rested the `Background` class on it. On the
//! version-matched binary the stack ships (ClickHouse 25.12.5.44) the server
//! defaults are `lightweight_delete_mode = alter_update` and
//! `lightweight_deletes_sync = 2`, and the shipped statement leaves a
//! `system.mutations` row whose `command` is
//! `(UPDATE _row_exists = 0 WHERE …)`. So the server rewrites every one of
//! these deletes into a mutation and blocks the client until it finishes.
//! Neither half of the old claim was true.
//!
//! What that costs, and what it buys:
//!
//! * **It cannot be cancelled.** Moraine's only cancellation primitive is the
//!   `KILL QUERY` a `Background` envelope's drop guard issues, and killing the
//!   initiating query does not cancel a mutation the server has already
//!   registered. `KILL MUTATION` appears nowhere in this tree. Choosing
//!   `Background` over `Migration` therefore does **not** make a runaway
//!   cancellable — that argument was wrong and has been removed.
//! * **It is bounded instead.** Because `lightweight_deletes_sync = 2` blocks
//!   until the mutation is done, at most one reclaim mutation per table is ever
//!   in flight, and control returns to the driver between units. Stoppability
//!   between units is a real property; cancellability mid-unit is not, and this
//!   module no longer claims it.
//!
//! [`RECLAIM_DELETE_SETTINGS`] pins both settings on every delete rather than
//! inheriting them, so the property above is one this module asserts rather
//! than one it borrows from a server default that a future release may change.

use std::fmt;

use anyhow::{Context, Result};
use moraine_config::{ProtectedRetentionBucket, RetentionConfig, RetentionHorizon};
use serde::{Deserialize, Serialize};

use crate::envelope::{QueryClass, QueryEnvelope};
use crate::storage_class::{classify, TableClass};
use crate::storage_report::StorageReport;
use crate::{escape_identifier, escape_literal, ClickHouseClient};

/// The ledger relation installed by migration 038.
pub const RECLAIM_LEDGER_TABLE: &str = "storage_reclaim_ledger";

/// Settings pinned on every reclaim `DELETE`, as `(name, value)` query params.
///
/// Measured on ClickHouse 25.12.5.44, the version-matched binary the stack
/// ships (`system.settings`, read 2026-07-28):
/// `lightweight_delete_mode = alter_update` and `lightweight_deletes_sync = 2`
/// are already the **server defaults**, and the shipped `DELETE FROM` produced
/// a `system.mutations` row with
/// `command = (UPDATE _row_exists = 0 WHERE …)`.
///
/// They are sent explicitly anyway, and the reason is not belt-and-braces. The
/// safety argument in this module's header — one mutation in flight at a time,
/// control back to the driver between units — is *only* true under
/// `lightweight_deletes_sync != 0`. Under `0` the statement returns
/// immediately and a 64-unit run stacks 64 unfinished mutations on a host
/// chosen for reclamation because its disk is nearly full. Inheriting that
/// from a server default means the property silently depends on
/// `config.xml`, a `default` profile, or a ClickHouse release note. Pinning it
/// makes it this module's decision, and
/// `every_reclaim_delete_pins_its_mutation_settings` makes removing it fail.
///
/// `mutations_sync = 0` is *not* set: it governs `ALTER … DELETE`, not
/// lightweight deletes, and setting it would restate a claim about a statement
/// shape this module does not emit.
pub const RECLAIM_DELETE_SETTINGS: &[(&str, &str)] = &[("lightweight_deletes_sync", "2")];

/// Control relations the reclaimer must never write. INV-2, as data.
///
/// `published_source_generations` is publication truth; the next three are
/// monotone revision allocators or the cache fence; `mcp_read_index_state` is
/// the canonical-read-index readiness fence.
pub const CONTROL_RELATIONS: &[&str] = &[
    "published_source_generations",
    "ingest_checkpoint_transitions",
    "source_generation_publication_readiness",
    "ingest_append_control",
    "mcp_read_index_state",
];

/// Maximum `(key)` tuples inlined into one `DELETE … WHERE … IN (…)`.
///
/// Defined *as* the existing `mcp_open` reclaimer's chunk rather than
/// restating the literal, so the two paths cannot claim "one shape" while
/// drifting to two numbers. The definition is the coupling;
/// `the_two_reclaim_paths_share_one_chunk_size` names it so a future edit that
/// re-copies the literal fails a test rather than a code review.
pub const RECLAIM_DELETE_CHUNK: usize = crate::mcp_open_projection::RECLAIM_DELETE_CHUNK;

/// Whether `statement` writes a control relation, and which one (INV-2).
///
/// Matches the **bare** relation name, not just the backtick-escaped form. An
/// earlier revision tested only ``statement.contains("`moraine`.{control}")``,
/// so a plain `INSERT INTO moraine.published_source_generations …` — the exact
/// text a hand-written repair statement or an un-escaped code path would
/// produce — passed the invariant untouched. The escaped form is a subset of
/// the bare one, so this is strictly wider and the bare match is what the
/// invariant is actually about: the reclaimer must not write publication truth
/// however it spells the qualifier.
///
/// `OPTIMIZE … FINAL` counts as a write. It returns no rows and reads as
/// maintenance, but `published_source_generations` is a `ReplacingMergeTree`
/// keyed on `publication_revision`: collapsing it discards every superseded
/// revision, which breaks the as-of read §1 gives as the reason that table is
/// never-delete. Nothing in the tree emits an `OPTIMIZE` today; the detector
/// covers the shape so the first one to appear is a test failure rather than a
/// statement the invariant waves through.
pub fn writes_control_relation(statement: &str) -> Option<&'static str> {
    let writes = statement.starts_with("INSERT INTO")
        || statement.starts_with("DELETE FROM")
        || statement.starts_with("ALTER")
        || statement.starts_with("TRUNCATE")
        || statement.starts_with("DROP")
        || statement.starts_with("OPTIMIZE");
    if !writes {
        return None;
    }
    CONTROL_RELATIONS
        .iter()
        .find(|control| statement.contains(**control))
        .copied()
}

/// Per-unit statement budget: at most one claim write, one phase advance, the
/// deletes for the unit's tables, one settle write, and margin. Derived from
/// named parts rather than a global constant, so no legitimate unit can exceed
/// its own cap.
const UNIT_STMT_CLAIM: u32 = 1;
const UNIT_STMT_ADVANCE: u32 = 1;
const UNIT_STMT_SETTLE: u32 = 1;
/// Largest table count any single scope deletes from (see
/// [`ReclaimScope::tables`]); asserted in the unit tests.
const UNIT_STMT_MAX_TABLES: u32 = 7;
const UNIT_STMT_MARGIN: u32 = 2;
/// Per-unit statement cap fed to [`QueryEnvelope::new_batch`].
pub const UNIT_STATEMENT_CAP: u32 = UNIT_STMT_CLAIM
    + UNIT_STMT_ADVANCE
    + UNIT_STMT_SETTLE
    + UNIT_STMT_MAX_TABLES
    + UNIT_STMT_MARGIN;

/// Statements one planning pass issues: the ledger summary, the two
/// `system.parts`/`system.disks` reads the byte estimate is derived from, and
/// one candidate probe per scope. Derived from named parts (§3.7) rather than
/// left a round number, so registering a scope raises the cap instead of
/// overrunning it. The planner writes nothing.
const PLAN_STMT_LEDGER_SUMMARY: u32 = 1;
const PLAN_STMT_STORAGE_REPORT: u32 = 2;
const PLAN_STMT_PER_SCOPE_PROBE: u32 = 1;
const PLAN_STMT_MARGIN: u32 = 2;
pub const PLAN_STATEMENT_CAP: u32 = PLAN_STMT_LEDGER_SUMMARY
    + PLAN_STMT_STORAGE_REPORT
    + PLAN_STMT_PER_SCOPE_PROBE * ReclaimScope::ALL.len() as u32
    + PLAN_STMT_MARGIN;

/// `reclaim status` is a plan plus its own storage report and ledger read.
pub const STATUS_STATEMENT_CAP: u32 =
    PLAN_STATEMENT_CAP + PLAN_STMT_STORAGE_REPORT + PLAN_STMT_LEDGER_SUMMARY;

/// Statements a `run` issues before its first unit is claimed: the
/// pending-mutation probe, the ledger re-drive read, one candidate probe, and
/// the byte-estimate report. Each unit then runs under **its own**
/// [`UNIT_STATEMENT_CAP`] envelope — §3.7 is explicit that it is one envelope
/// per unit, not per run, so a deadline caps one unit rather than the sum, and
/// a 64-unit sweep cannot exhaust a cap sized for one.
pub const RUN_PREAMBLE_STATEMENT_CAP: u32 =
    1 + 1 + PLAN_STMT_PER_SCOPE_PROBE + PLAN_STMT_STORAGE_REPORT + PLAN_STMT_MARGIN;

/// The envelope a `moraine db reclaim plan` opens around
/// [`ClickHouseClient::reclaim_plan`].
///
/// The cap is **not** a caller's choice. [`ClickHouseClient::reclaim_status`]
/// already scopes its own envelope inside this module; `plan` and `run` made
/// the CLI name a kind, a class and a cap, and a cap named at a call site is a
/// cap that can be named wrong there — swapping `PLAN_STATEMENT_CAP` for
/// [`UNIT_STATEMENT_CAP`] widens a governance bound with no statement, no
/// outcome and no rendered line to show for it, so no behavioural test can see
/// it. Constructing the envelope here removes the choice rather than testing
/// it, which is the same move `background_batch_envelope` makes for the
/// ingest janitor.
pub fn plan_envelope(
    budget: &moraine_config::ValidatedQueryBudget,
    admin_budget: &moraine_config::ValidatedQueryBudget,
) -> std::sync::Arc<QueryEnvelope> {
    QueryEnvelope::new_batch(
        "reclaim-plan",
        QueryClass::Background,
        budget,
        admin_budget,
        PLAN_STATEMENT_CAP,
    )
}

/// The envelope a `moraine db reclaim run` opens around
/// [`ClickHouseClient::reclaim_run`]'s preamble.
///
/// Sized for the preamble only, because each claimed unit opens its own
/// [`UNIT_STATEMENT_CAP`] envelope inside the run — see
/// [`RUN_PREAMBLE_STATEMENT_CAP`]. Same rationale as [`plan_envelope`] for why
/// the cap is not passed in.
pub fn run_preamble_envelope(
    budget: &moraine_config::ValidatedQueryBudget,
    admin_budget: &moraine_config::ValidatedQueryBudget,
) -> std::sync::Arc<QueryEnvelope> {
    QueryEnvelope::new_batch(
        "reclaim-run",
        QueryClass::Background,
        budget,
        admin_budget,
        RUN_PREAMBLE_STATEMENT_CAP,
    )
}

// ---------------------------------------------------------------------------
// Denomination (plan §3.7) — what may be reported, and in what words
// ---------------------------------------------------------------------------

/// Qualifier every "bytes we could reclaim" number must carry. Nothing is
/// partitioned by `source_generation`, so a reclaim is a lightweight `DELETE`
/// masking rows via `_row_exists`; bytes come back only when a background
/// merge gets to the part.
pub const ESTIMATE_QUALIFIER: &str = "estimate";

/// Qualifier every "bytes we did reclaim" number must carry.
pub const MERGE_DEFERRED_QUALIFIER: &str = "merge-deferred";

/// Words a reclaim surface may never use. "frees"/"recovers" promise an
/// immediate on-disk effect that a lightweight `DELETE` does not deliver, and
/// "partition" promises partition-aligned deletion that no table's layout
/// supports.
pub const FORBIDDEN_DENOMINATION_WORDS: &[&str] = &["frees", "recovers", "partition"];

/// The one sentence every surface uses for an estimate.
pub fn estimated_bytes_note() -> String {
    format!(
        "reclaimable bytes are an {ESTIMATE_QUALIFIER}; a lightweight DELETE masks rows and \
         returns bytes only when a background merge rewrites the part"
    )
}

/// The one sentence every surface uses after a run.
///
/// It said "reclaimed row counts are exact". They are not, and nothing in this
/// module ever computed an exact one: `ReclaimOutcome::Settled` sums the
/// **probe's** per-unit estimate for the units the run claimed, a re-driven
/// unit contributes zero because its rows were not re-counted, and a
/// lightweight `DELETE` reports no affected-row count over the HTTP interface
/// anyway. Getting a real one would need a `count()` per unit before each
/// delete — a second full scan of `mcp_open_events` per unit, which is the
/// cost this scope exists to avoid. So the number stays an estimate and the
/// sentence says so.
pub fn reclaimed_bytes_note() -> String {
    format!(
        "reclaimed row counts are an {ESTIMATE_QUALIFIER} taken at claim time, not a count of \
         rows removed; the on-disk delta is {MERGE_DEFERRED_QUALIFIER} and is not a guarantee"
    )
}

// ---------------------------------------------------------------------------
// Scopes and predicates
// ---------------------------------------------------------------------------

/// A reclaimable unit kind. One variant per work item that will register an
/// executor; each names a target table set and, per table, a predicate shape.
///
/// This is an enum rather than a string so that adding a scope without
/// deciding its tables, its predicate shapes, its authority, and whether it is
/// on by default is a compile error in five places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimScope {
    /// WS-3a (WI-05): `(session_id, candidate_generation)` pairs present in
    /// the `mcp_open_*` child tables with no corresponding publication header.
    /// Provably dead: an exact `(slot, generation)` match against the
    /// header-authorized session is required to pin one, on either engine.
    McpOpenOrphan,
    /// WS-3a′ (WI-05b, plan §0 F4): `(session_id, candidate_generation)` pairs
    /// whose header is complete and valid but whose **lineage** the session has
    /// left. `reclaim_superseded_mcp_open_snapshots` joins within one
    /// `required_heads_fingerprint` and excludes everything else by design
    /// (`mcp_open_projection::superseded_snapshot_set_sql`), so a replacement replay's
    /// predecessor is unreachable by it forever. Retirement rests on the
    /// monotone dirty revision the reader compares for equality, never on
    /// generation order — see `reclaim_mcp_open::retired_lineage_candidate_sql`.
    McpOpenRetiredLineage,
    /// WS-3b (WI-07): canonical read-index rows whose
    /// `(source_host, source_name, source_file, source_generation)` is not in
    /// the published heads. All three targets are content-free and rebuildable
    /// via `moraine db core-index rebuild`.
    ReadIndexGeneration,
    /// WS-3c (WI-09): superseded canonical/raw/search rows. Buckets 1 and 2,
    /// so it is unreachable without an explicit `[retention]` key.
    CanonicalGeneration,
}

impl ReclaimScope {
    pub const ALL: [ReclaimScope; 4] = [
        Self::McpOpenOrphan,
        Self::McpOpenRetiredLineage,
        Self::ReadIndexGeneration,
        Self::CanonicalGeneration,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::McpOpenOrphan => "mcp_open_orphan",
            Self::McpOpenRetiredLineage => "mcp_open_retired_lineage",
            Self::ReadIndexGeneration => "read_index_generation",
            Self::CanonicalGeneration => "canonical_generation",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|scope| scope.as_str() == value)
    }

    /// Tables this scope deletes from, **children first, parent last**
    /// (plan §3.2 and §3.5). A crash mid-unit must leave derived data missing
    /// over intact canonical data, never the inverse.
    pub fn tables(self) -> &'static [ReclaimTable] {
        match self {
            // The header is named even though an orphan unit has none: the
            // delete is a predicate over a key set, so it removes zero rows —
            // and naming it means a header written *between* the probe and the
            // delete (a prepare that completed inside the horizon this unit
            // already passed) does not survive its own children. Statement
            // order is what makes that safe rather than merely tidy: children
            // first, header last.
            Self::McpOpenOrphan | Self::McpOpenRetiredLineage => &[
                ReclaimTable::McpOpenEvents,
                ReclaimTable::McpOpenTurns,
                ReclaimTable::McpOpenPublicationHeaders,
            ],
            Self::ReadIndexGeneration => &[
                ReclaimTable::McpEventNavigation,
                ReclaimTable::McpEventLocator,
                ReclaimTable::McpSessionDirectory,
            ],
            Self::CanonicalGeneration => &[
                ReclaimTable::SearchPostings,
                ReclaimTable::SearchDocuments,
                ReclaimTable::ToolIo,
                ReclaimTable::EventLinks,
                ReclaimTable::IngestErrors,
                ReclaimTable::RawEvents,
                ReclaimTable::Events,
            ],
        }
    }

    /// Whether stock configuration may reach this scope at all.
    ///
    /// `CanonicalGeneration` is not merely "off by default": it is
    /// unconstructible as an authorized claim without a `[retention]` key,
    /// because [`ReclaimAuthority::for_scope`] cannot produce a token for it
    /// from a default config.
    pub fn is_default_on(self) -> bool {
        match self {
            Self::McpOpenOrphan | Self::McpOpenRetiredLineage | Self::ReadIndexGeneration => true,
            Self::CanonicalGeneration => false,
        }
    }

    /// The key one unit of this scope is identified by. See
    /// [`ReclaimUnitGrain`]; exhaustive, so a new scope must decide.
    pub fn unit_grain(self) -> ReclaimUnitGrain {
        match self {
            Self::McpOpenOrphan | Self::McpOpenRetiredLineage => {
                ReclaimUnitGrain::SessionCandidateGeneration
            }
            Self::ReadIndexGeneration | Self::CanonicalGeneration => {
                ReclaimUnitGrain::SourceGeneration
            }
        }
    }

    /// Human-readable description used by the CLI refusal.
    pub fn describe(self) -> &'static str {
        match self {
            Self::McpOpenOrphan => {
                "orphan legacy open-projection rows (no publication header; unreadable by design)"
            }
            Self::McpOpenRetiredLineage => {
                "legacy open-projection snapshots in a source lineage the session has left \
                 (a newer lineage is published and live, and no reader can select the old one)"
            }
            Self::ReadIndexGeneration => {
                "canonical read-index rows for superseded source generations (content-free, \
                 rebuildable with `moraine db core-index rebuild`)"
            }
            Self::CanonicalGeneration => {
                "canonical, raw, and search rows for superseded source generations (USER HISTORY)"
            }
        }
    }
}

impl fmt::Display for ReclaimScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A table a reclaim scope may name. Closed enum, because the whole safety
/// argument rests on the per-table predicate shape being decided once, in
/// [`ReclaimTable::predicate`], as an exhaustive match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimTable {
    McpOpenEvents,
    McpOpenTurns,
    McpOpenPublicationHeaders,
    McpSessionDirectory,
    McpEventLocator,
    McpEventNavigation,
    SearchPostings,
    SearchDocuments,
    ToolIo,
    EventLinks,
    IngestErrors,
    RawEvents,
    Events,
}

/// The shape of the predicate a table's delete may carry.
///
/// **Hazard H3, as a type.** `tool_io` and `event_links` have no
/// `source_file`/`source_generation` column at all, so a generation-shaped
/// predicate against them is a compile-time-valid, runtime-wrong statement.
/// Making the shape part of an exhaustive `match` turns "someone writes a
/// generation predicate for `tool_io`" from a code-review catch into a
/// changed match arm that the tests below assert directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimPredicate {
    /// `(source_host, source_name, source_file, source_generation)` is on the
    /// row itself and is exact.
    Generation,
    /// `(session_id, candidate_generation)` is on the row itself. The two
    /// `mcp_open` **child** tables, and only those.
    SessionGeneration,
    /// `(session_id, generation)` is on the row itself.
    ///
    /// **`mcp_open_publication_headers` has no `candidate_generation` column**
    /// — verified against the deployed schema on 2026-07-28: its columns are
    /// `(session_id, candidate_publication_id, slot, generation, …)`
    /// (`sql/033:16-60`), and the existing reclaimer's own header delete says
    /// so (`mcp_open_projection::superseded_snapshot_delete_statements` predicates
    /// `(session_id, generation)` on the header and
    /// `(session_id, candidate_generation)` on the two children). Folding the
    /// header into [`Self::SessionGeneration`] made every legal header delete
    /// unemittable and every *emittable* one name a column the table does not
    /// have — hazard H3, one table over from the pair it was written for.
    SessionHeaderGeneration,
    /// The row carries no generation. Its liveness is a **uid set** captured
    /// into the ledger before the parent delete runs, or the rows become
    /// unreachable the moment their parent goes.
    UidSet,
    /// The row's own `source_file`/`source_generation` are back-filled type
    /// defaults on the overwhelming majority of rows and must never be
    /// predicated on. The only safe predicate joins through the **document**.
    DocumentJoin,
}

impl ReclaimPredicate {
    /// Columns a delete of this shape must reference, all of them.
    ///
    /// **This is the check that catches the failure that actually empties a
    /// table.** Re-deriving the class and checking the token proves the
    /// emitter may name `events`; it says nothing about *which* rows the
    /// statement removes. An executor that produced `WHERE session_id IN ()`,
    /// or fell back to `WHERE 1` on an empty chunk, previously passed the last
    /// independent check before a `DELETE` reached ClickHouse — and the INV-2
    /// corpus itself built every statement with `"1"`, a full-table delete.
    ///
    /// Every name here is verified present on every table of the shape:
    /// `source_host` reaches `events`/`raw_events`/`ingest_errors`/
    /// `search_documents` via sql/032:25-70 and the read indexes via sql/036;
    /// `candidate_generation` reaches `mcp_open_events`/`mcp_open_turns` via
    /// sql/033:8,13 and **not** `mcp_open_publication_headers`, which carries
    /// `generation` — see [`Self::SessionHeaderGeneration`].
    /// `search_postings` has **no** `event_uid` column at all — its link to
    /// the document is `doc_id` — which is why `DocumentJoin` is a separate
    /// shape rather than `UidSet`.
    pub fn required_columns(self) -> &'static [&'static str] {
        match self {
            Self::Generation => &[
                "source_host",
                "source_name",
                "source_file",
                "source_generation",
            ],
            Self::SessionGeneration => &["session_id", "candidate_generation"],
            Self::SessionHeaderGeneration => &["session_id", "generation"],
            Self::UidSet => &["event_uid"],
            Self::DocumentJoin => &["doc_id"],
        }
    }
}

/// Whether `predicate_sql` names `column` as a **whole SQL identifier**.
///
/// A plain `contains` cannot tell `generation` from `candidate_generation`:
/// one is a substring of the other, so a header delete predicated on
/// `(session_id, candidate_generation)` — a column
/// `mcp_open_publication_headers` does not have — satisfied a requirement for
/// `generation`, and the emitter's third check reported the statement bound
/// when it named no column of the table at all. Identifier characters are
/// `[A-Za-z0-9_]`, so a match is real only when neither neighbour is one.
///
/// This is still name presence rather than binding (see
/// [`emit_delete_statement`]); it is presence of the *right* name.
pub fn predicate_names_column(predicate_sql: &str, column: &str) -> bool {
    let is_ident = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let bytes = predicate_sql.as_bytes();
    predicate_sql.match_indices(column).any(|(start, matched)| {
        let end = start + matched.len();
        let before_ok = start == 0 || !is_ident(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_ident(bytes[end]);
        before_ok && after_ok
    })
}

impl ReclaimTable {
    pub fn name(self) -> &'static str {
        match self {
            Self::McpOpenEvents => "mcp_open_events",
            Self::McpOpenTurns => "mcp_open_turns",
            Self::McpOpenPublicationHeaders => "mcp_open_publication_headers",
            Self::McpSessionDirectory => "mcp_session_directory",
            Self::McpEventLocator => "mcp_event_locator",
            Self::McpEventNavigation => "mcp_event_navigation",
            Self::SearchPostings => "search_postings",
            Self::SearchDocuments => "search_documents",
            Self::ToolIo => "tool_io",
            Self::EventLinks => "event_links",
            Self::IngestErrors => "ingest_errors",
            Self::RawEvents => "raw_events",
            Self::Events => "events",
        }
    }

    /// The predicate shape this table's delete must use. Exhaustive by
    /// construction: a new variant without an arm is a compile error.
    pub fn predicate(self) -> ReclaimPredicate {
        match self {
            Self::McpOpenEvents | Self::McpOpenTurns => ReclaimPredicate::SessionGeneration,
            // Not `SessionGeneration`: the header table's column is
            // `generation`. See `ReclaimPredicate::SessionHeaderGeneration`.
            Self::McpOpenPublicationHeaders => ReclaimPredicate::SessionHeaderGeneration,
            Self::McpSessionDirectory | Self::McpEventNavigation => ReclaimPredicate::Generation,
            // `mcp_event_locator` is ORDER BY (event_uid, source_host) and
            // `event_uid` embeds `source_generation`, so a generation
            // predicate is exact — but `session_id` on a locator row is NOT
            // key-determined, so a locator delete must never carry one.
            Self::McpEventLocator => ReclaimPredicate::Generation,
            Self::SearchPostings => ReclaimPredicate::DocumentJoin,
            // `search_documents` is ORDER BY (event_uid, source_host) alone,
            // so one physical row serves BOTH attributions of a
            // double-attributed uid. Generation-keyed, never session-keyed.
            Self::SearchDocuments => ReclaimPredicate::Generation,
            // H3: no generation column exists on either of these.
            Self::ToolIo | Self::EventLinks => ReclaimPredicate::UidSet,
            Self::IngestErrors | Self::RawEvents | Self::Events => ReclaimPredicate::Generation,
        }
    }

    /// The storage class of this table, re-derived from the classification.
    /// Panics only if the classification and this enum disagree, which
    /// `every_reclaim_table_is_classified_and_never_never_delete` makes
    /// impossible to ship.
    pub fn class(self) -> Option<TableClass> {
        classify(self.name())
    }
}

// ---------------------------------------------------------------------------
// §4 S2/S3 — authority
// ---------------------------------------------------------------------------

/// Permission to emit a delete naming a table of a given class.
///
/// Two of the variants are constructible from a default config; two are not
/// constructible **at all** without a [`RetentionHorizon`], which has no
/// `Default`, no `From<()>`, and only one source: an explicit config key.
/// A missing setting therefore cannot type-check into deletion authority.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReclaimAuthority {
    /// Bucket 3. Constructible from `Default`.
    DerivedOnly,
    /// Bucket 4. Constructible from `Default`.
    Telemetry,
    /// Bucket 2. **Not** constructible without config.
    RawAudit(RetentionHorizon),
    /// Bucket 1. **Not** constructible without config.
    CanonicalHistory(RetentionHorizon),
}

impl ReclaimAuthority {
    /// Whether this token authorizes a delete naming a table of `class`.
    ///
    /// **Exactly one class per variant.** §4 S3 requires the *corresponding*
    /// token and S2 makes permission per-bucket: `retention.raw_audit_horizon_days`
    /// is what authorizes `raw_events`/`ingest_errors`, and nothing else is.
    /// An earlier revision let `CanonicalHistory(_)` cover `RawAudit` and
    /// `Derived` on the reasoning that the canonical horizon is the strictest
    /// one — but that makes the emitter's check depend on
    /// [`Self::for_scope`] demanding both keys, which is precisely the
    /// planner-side check S3 says the emitter must not lean on. A scope
    /// spanning two buckets carries two tokens; see
    /// [`Self::for_scope`]'s `CanonicalGeneration` arm.
    ///
    /// `NeverDelete` is authorized by nothing: there is deliberately no
    /// variant that could return `true` for it.
    pub fn authorizes(self, class: TableClass) -> bool {
        match (self, class) {
            (_, TableClass::NeverDelete) => false,
            (Self::DerivedOnly, TableClass::Derived) => true,
            (Self::Telemetry, TableClass::Telemetry) => true,
            (Self::RawAudit(_), TableClass::RawAudit) => true,
            (Self::CanonicalHistory(_), TableClass::CanonicalHistory) => true,
            _ => false,
        }
    }

    /// The authority tokens `retention` produces for `scope`, or the missing
    /// config key when it produces none.
    ///
    /// `CanonicalGeneration` needs both protected horizons because its table
    /// list spans buckets 1 and 2; a config that names only one of them is
    /// refused naming the other, rather than silently reclaiming half a unit.
    pub fn for_scope(
        scope: ReclaimScope,
        retention: &RetentionConfig,
    ) -> Result<Vec<ReclaimAuthority>, MissingAuthority> {
        match scope {
            ReclaimScope::McpOpenOrphan
            | ReclaimScope::McpOpenRetiredLineage
            | ReclaimScope::ReadIndexGeneration => Ok(vec![Self::DerivedOnly]),
            ReclaimScope::CanonicalGeneration => {
                let canonical = RetentionHorizon::from_config(
                    retention,
                    ProtectedRetentionBucket::CanonicalHistory,
                )
                .ok_or(MissingAuthority {
                    scope,
                    config_key: ProtectedRetentionBucket::CanonicalHistory.config_key(),
                })?;
                let raw =
                    RetentionHorizon::from_config(retention, ProtectedRetentionBucket::RawAudit)
                        .ok_or(MissingAuthority {
                            scope,
                            config_key: ProtectedRetentionBucket::RawAudit.config_key(),
                        })?;
                Ok(vec![
                    Self::DerivedOnly,
                    Self::RawAudit(raw),
                    Self::CanonicalHistory(canonical),
                ])
            }
        }
    }
}

/// A scope asked for without the config key that would authorize it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissingAuthority {
    pub scope: ReclaimScope,
    pub config_key: &'static str,
}

impl fmt::Display for MissingAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "scope `{}` deletes user history and is not configured: set `{}` to a horizon of at \
             least 7 days first. Export before pruning with `moraine export events --format jsonl`.",
            self.scope, self.config_key
        )
    }
}

impl std::error::Error for MissingAuthority {}

/// Refusal from the statement emitter (§4 S3, the second independent check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitRefusal {
    /// [`classify`] returned `None`. S1: unknown is not deletable.
    UnclassifiedTable { table: String },
    /// The table's class is not authorized by any supplied token.
    Unauthorized { table: String, class: TableClass },
    /// The predicate does not bind the claimed unit: it fails to name one or
    /// more of the shape's key columns, so the statement's extent is not the
    /// unit's extent. `WHERE 1` is the limiting case.
    UnboundPredicate {
        table: String,
        predicate: ReclaimPredicate,
        missing: Vec<&'static str>,
    },
}

impl fmt::Display for EmitRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnclassifiedTable { table } => write!(
                formatter,
                "refusing to emit a delete naming unclassified table `{table}`: an unknown table \
                 is not deletable"
            ),
            Self::Unauthorized { table, class } => write!(
                formatter,
                "refusing to emit a delete naming `{table}` ({}): no authority token for this \
                 class was supplied",
                class.as_str()
            ),
            Self::UnboundPredicate {
                table,
                predicate,
                missing,
            } => write!(
                formatter,
                "refusing to emit a delete naming `{table}`: its predicate does not name {} — a \
                 {predicate:?} delete must bind every key column of the claimed unit, or its \
                 extent is not the unit's extent",
                missing.join(", ")
            ),
        }
    }
}

impl std::error::Error for EmitRefusal {}

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------

/// Ledger phase. Advances only forward; `settle` writes `Done`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimPhase {
    /// Written **before any delete**, from relations the deletes do not touch.
    Claimed,
    /// Deletes in flight. A crash here leaves a unit whose parent still
    /// exists; the next run re-drives it from the ledger.
    Deleting,
    Done,
    /// Abandoned by an operator or by a bound. Never re-driven.
    Abandoned,
}

impl ReclaimPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Deleting => "deleting",
            Self::Done => "done",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        [Self::Claimed, Self::Deleting, Self::Done, Self::Abandoned]
            .into_iter()
            .find(|phase| phase.as_str() == value)
    }

    /// Phases a run must re-drive to completion before planning new units.
    pub fn needs_redrive(self) -> bool {
        matches!(self, Self::Claimed | Self::Deleting)
    }
}

/// One claimed unit of reclamation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimUnit {
    pub reclaim_id: String,
    pub scope: ReclaimScope,
    pub source_host: String,
    pub source_name: String,
    pub source_file: String,
    pub source_generation: u32,
    /// Empty for generation-scoped units.
    pub session_id: String,
    /// Zero for generation-scoped units.
    pub candidate_generation: u64,
    pub phase: ReclaimPhase,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    /// How long this unit has been unsettled, in seconds — the age of its
    /// **first** claim, not of the last phase write.
    ///
    /// Zero for a freshly planned unit. Carried through
    /// [`ledger_advance_statement`] so `claimed_at` survives a phase advance;
    /// without that, every re-drive reset the age and
    /// [`RECLAIM_UNSETTLED_ABANDON_SECONDS`] could never be reached by the
    /// units that most need it — the ones that fail on every attempt.
    #[serde(default)]
    pub unsettled_seconds: u64,
}

/// The key a scope's unit is identified by, in the ledger and in its delete
/// predicates.
///
/// An exhaustive property of the scope rather than an inference from which
/// candidate columns happen to be non-empty: adding a scope without deciding
/// its grain is a compile error, and the grain drives both the unit's
/// `reclaim_id` (its ledger identity) and the claim-time validation in
/// [`ReclaimCandidateRow::into_unit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimUnitGrain {
    /// One `(session_id, candidate_generation)` pair — the two `mcp_open`
    /// scopes.
    SessionCandidateGeneration,
    /// One `(source_host, source_name, source_file, source_generation)` tuple
    /// — the read-index scope, and WI-09's canonical scope.
    SourceGeneration,
}

/// Row shape every registered candidate probe returns.
///
/// One struct across both grains, with the other grain's key fields defaulted
/// — the transport deserializes one shape, and [`Self::into_unit`] restores
/// fail-loud behaviour by refusing a row that does not carry its scope's own
/// key. Without that check, a probe emitting the wrong column names would
/// deserialize into all-default rows and claim ledger units keyed `('', 0)`
/// whose deletes bind nothing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ReclaimCandidateRow {
    #[serde(default)]
    pub(crate) session_id: String,
    #[serde(default)]
    pub(crate) candidate_generation: u64,
    #[serde(default)]
    pub(crate) source_host: String,
    #[serde(default)]
    pub(crate) source_name: String,
    #[serde(default)]
    pub(crate) source_file: String,
    #[serde(default)]
    pub(crate) source_generation: u32,
    #[serde(default)]
    pub(crate) event_rows: u64,
    #[serde(default)]
    pub(crate) turn_rows: u64,
    #[serde(default)]
    pub(crate) header_rows: u64,
    #[serde(default)]
    pub(crate) navigation_rows: u64,
    #[serde(default)]
    pub(crate) locator_rows: u64,
    #[serde(default)]
    pub(crate) directory_rows: u64,
}

impl ReclaimCandidateRow {
    pub(crate) fn estimated_rows(&self) -> u64 {
        self.event_rows
            .saturating_add(self.turn_rows)
            .saturating_add(self.header_rows)
            .saturating_add(self.navigation_rows)
            .saturating_add(self.locator_rows)
            .saturating_add(self.directory_rows)
    }

    /// The claimable unit this candidate describes, or an error naming the
    /// missing key when the row does not carry its scope's grain.
    ///
    /// The `reclaim_id` format for the session grain is unchanged from WI-05
    /// (`{scope}:{session}:{generation}`), deliberately: it is the ledger's
    /// `(scope, reclaim_id)` key, and reformatting it would stop a re-claim of
    /// an in-flight unit from collapsing onto its existing row.
    pub(crate) fn into_unit(self, scope: ReclaimScope) -> Result<ReclaimUnit> {
        let reclaim_id = match scope.unit_grain() {
            ReclaimUnitGrain::SessionCandidateGeneration => {
                if self.session_id.is_empty() || self.candidate_generation == 0 {
                    anyhow::bail!(
                        "scope `{scope}` is session-grained but its probe returned a candidate \
                         without a `(session_id, candidate_generation)` key: {self:?}"
                    );
                }
                format!(
                    "{}:{}:{}",
                    scope.as_str(),
                    self.session_id,
                    self.candidate_generation
                )
            }
            ReclaimUnitGrain::SourceGeneration => {
                if self.source_file.is_empty() || self.source_generation == 0 {
                    anyhow::bail!(
                        "scope `{scope}` is generation-grained but its probe returned a candidate \
                         without a `(source_host, source_name, source_file, source_generation)` \
                         key: {self:?}"
                    );
                }
                format!(
                    "{}:{}:{}:{}:{}",
                    scope.as_str(),
                    self.source_host,
                    self.source_name,
                    self.source_file,
                    self.source_generation
                )
            }
        };
        let estimated_rows = self.estimated_rows();
        Ok(ReclaimUnit {
            reclaim_id,
            scope,
            source_host: self.source_host,
            source_name: self.source_name,
            source_file: self.source_file,
            source_generation: self.source_generation,
            session_id: self.session_id,
            candidate_generation: self.candidate_generation,
            phase: ReclaimPhase::Claimed,
            estimated_rows,
            estimated_bytes: 0,
            unsettled_seconds: 0,
        })
    }
}

/// Ledger totals by phase.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimLedgerSummary {
    pub claimed: u64,
    pub deleting: u64,
    pub done: u64,
    pub abandoned: u64,
    /// Present when a run could not proceed. Never `None` merely because
    /// nothing needed doing — hazard H9's failure mode is precisely that
    /// "blocked" and "nothing to do" were indistinguishable.
    pub blocked_reason: Option<String>,
}

impl ReclaimLedgerSummary {
    pub fn needs_redrive(&self) -> u64 {
        self.claimed + self.deleting
    }
}

// ---------------------------------------------------------------------------
// Plan / run outcomes
// ---------------------------------------------------------------------------

/// A dry-run estimate for one scope. Writes nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimableEstimate {
    pub scope: ReclaimScope,
    pub units: u64,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    /// Tables the scope would delete from, in emission order.
    pub tables: Vec<String>,
    /// Why the scope reported zero units, when it did.
    pub note: Option<String>,
}

/// The full `moraine db reclaim plan` result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReclaimPlan {
    pub scopes: Vec<ReclaimableEstimate>,
    /// [`estimated_bytes_note`]. Carried in the payload so a JSON consumer
    /// cannot render a byte count without the qualifier.
    pub denomination: String,
    /// Units already in the ledger awaiting re-drive.
    pub pending_redrive: u64,
}

/// The full `moraine db reclaim status` result: WI-02's storage report plus
/// the ledger and the dry-run estimates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReclaimStatusReport {
    /// `false` when ClickHouse could not be reached; every other field is then
    /// its empty value. Mirrors the never-fails shape of the core-index
    /// report: storage state is transient and must not fail a doctor exit.
    pub available: bool,
    pub storage: Option<StorageReport>,
    pub ledger: ReclaimLedgerSummary,
    pub reclaimable: Vec<ReclaimableEstimate>,
    /// Scopes an executor exists for.
    pub registered_executors: Vec<ReclaimScope>,
    pub denomination: String,
    pub error: Option<String>,
}

/// Outcome of `moraine db reclaim run`.
///
/// **Hazard H9, as a type.** The existing projection reclaim gate returns
/// `Ok(default)` with no log and no counter when a mutation is pending, so a
/// permanently stuck mutation disables reclamation forever with zero operator
/// signal. `Blocked` is a distinct variant carrying the pending count, and it
/// is surfaced by `reclaim status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReclaimOutcome {
    /// No executor is registered for the scope. The only outcome this build
    /// can produce for a `run`.
    NoExecutor {
        scope: ReclaimScope,
        message: String,
    },
    /// A prior mutation over the scope's tables is still running.
    Blocked {
        scope: ReclaimScope,
        pending_mutations: u64,
    },
    /// Nothing to reclaim **and** nothing re-driven. Never returned merely
    /// because the candidate probe came back empty: a run that re-drove 64
    /// wedged units did work, and reporting "nothing to reclaim" for it hides
    /// exactly the state an operator needs to see.
    Idle { scope: ReclaimScope },
    /// An automatic run declined to start because the disk is too full for
    /// reclamation to be safe. Distinct from [`Self::Blocked`]: nothing is
    /// wrong with the server, there is simply no headroom for the `_row_exists`
    /// masks this work writes before merges remove anything.
    LowDisk {
        scope: ReclaimScope,
        free_bytes: u64,
        required_bytes: u64,
    },
    /// Units were settled.
    Settled {
        scope: ReclaimScope,
        units: u64,
        /// **Estimated** rows, summed from the probe's per-unit counts, for
        /// the units this run *claimed*. Re-driven units contribute 0 because
        /// their row counts were not re-probed — the rows may already be gone.
        /// See [`reclaimed_bytes_note`]; this is not a count of rows removed
        /// and nothing may denominate it as one.
        estimated_rows: u64,
        /// Units completed by re-drive, included in `units`. Broken out so
        /// `estimated_rows == 0 && units > 0` is legible rather than looking
        /// like a bug.
        redriven: u64,
        /// Units left unsettled for the next tick, and units abandoned.
        failed: u64,
        abandoned: u64,
        /// [`reclaimed_bytes_note`].
        denomination: String,
    },
}

impl ReclaimOutcome {
    pub fn scope(&self) -> ReclaimScope {
        match self {
            Self::NoExecutor { scope, .. }
            | Self::Blocked { scope, .. }
            | Self::LowDisk { scope, .. }
            | Self::Idle { scope }
            | Self::Settled { scope, .. } => *scope,
        }
    }

    /// Whether the outcome represents work actually performed.
    pub fn deleted_anything(&self) -> bool {
        matches!(self, Self::Settled { units, .. } if *units > 0)
    }
}

// ---------------------------------------------------------------------------
// Executor registry
// ---------------------------------------------------------------------------

/// Whether [`ClickHouseClient::reclaim_run`] genuinely claims units into the
/// ledger, re-drives unsettled ones, and settles them (plan §3.2).
///
/// `true` as of WI-05. [`ClickHouseClient::reclaim_redrive`] runs at the head
/// of every run — before any candidate is probed — and completes every unit the
/// ledger left in `claimed` or `deleting`;
/// [`ClickHouseClient::reclaim_execute_unit`] advances to `deleting`, issues the
/// unit's deletes children-first, and settles to `done`. The four statement
/// builders have production callers, and
/// `an_interrupted_claim_is_completed_by_the_next_runs_redrive` interrupts a
/// real claim mid-unit against a stateful server and asserts the next run
/// finishes it.
///
/// Flipping this constant without wiring the driver is not a shortcut past
/// `no_executor_may_be_registered_before_the_ledger_driver_is_wired`; it is a
/// false statement in a safety guard, and it is one line in a diff. That test
/// therefore asserts the *conjunction* — driver wired **and** the wired-driver
/// behaviours covered — rather than the constant alone.
pub const LEDGER_DRIVER_WIRED: bool = true;

/// Units one run may claim, **derived from the measured cost of one unit**
/// rather than chosen as a round number.
///
/// One unit issues three deletes, and `EXPLAIN indexes = 1` against the shipped
/// predicates reports two different shapes. **The shape is what this bound
/// rests on; the absolute counts below are not, and must not be read as
/// stable.** Parts and granules move with every background merge — a re-run
/// hours later gives different numbers for an unchanged schema and an unchanged
/// query — so what follows is a dated sample of a date-independent property.
///
/// | table | pruning shape | sample, reference host 2026-07-28 |
/// |---|---|---|
/// | `mcp_open_events` | **none: `Condition: true`, every active part and every granule read** | 452/452 parts, 9 692/9 692 granules over 9.66 GiB |
/// | `mcp_open_turns` | one partition of 64; parts and granules within it are session-dependent | 1/294 parts and 1 granule for an absent session; 10/294 parts and 2 743/4 421 granules for the host's largest |
/// | `mcp_open_publication_headers` | one partition of 64 | 1/79 parts for an absent session, 2/79 for a present one |
///
/// The date-independent reason is the sort key, not the sample.
/// `mcp_open_turns` and `mcp_open_publication_headers` are `PARTITION BY
/// cityHash64(session_id) % 64` with `session_id` leading the primary key, so a
/// predicate naming one `session_id` reaches exactly one partition — how many
/// parts and granules that partition currently holds depends on the session's
/// size and on merge state, which is why the turns row above spans two orders
/// of magnitude between an absent session and the largest present one.
/// `mcp_open_events` is `PARTITION BY cityHash64(event_uid) % 64 PRIMARY KEY
/// (event_uid, slot)` — **`session_id` appears in neither**, and
/// `candidate_generation` is only the trailing `ORDER BY` column, so the
/// planner reports `Condition: true` and reads every part. No predicate over
/// `(session_id, candidate_generation)` can prune this table; that is a
/// property of the sort key, and no rewrite here fixes it.
///
/// So each unit costs one full scan of the largest `mcp_open` table plus a
/// mutation that writes a `_row_exists` mask into every part it touches — and a
/// mask makes the table *larger* until merges rewrite the parts. At the
/// previous value of 64, two scopes on a 60 s tick issued up to **128 full
/// scans of `mcp_open_events` per minute** on a host selected for reclamation
/// precisely because it is nearly out of disk.
///
/// 8 keeps a full sweep of the reference host's orphan backlog — of order
/// 700 units, 730 on the last read-only measurement of 2026-07-28, and
/// growing with every interrupted prepare — at roughly 90 ticks, an hour and a
/// half of draining. That is the right trade when the alternative is
/// amplifying a disk-full incident. `moraine db reclaim run --confirm` is the
/// path for an operator who wants it faster and is watching.
pub const RECLAIM_MAX_UNITS_PER_RUN: usize = 8;

/// How long a unit may stay unsettled before a re-drive marks it `abandoned`.
///
/// 24 h: long enough that a genuine outage (ClickHouse down, a table locked
/// behind a long merge) resolves and the unit completes normally, short enough
/// that a deterministically-failing unit stops consuming a re-drive slot on
/// every tick within a day.
///
/// This is the only §3.7 bound the shipped ledger schema can express. The
/// plan's `max_rows` / `max_bytes` / `max_parts_touched` need columns
/// `storage_reclaim_ledger` does not have, so they are not implemented and are
/// not claimed; `ReclaimPhase::Abandoned` previously had **no writer outside
/// `#[cfg(test)]`**, which made the phase — and the "never re-driven"
/// guarantee in its doc comment — a description of nothing.
pub const RECLAIM_UNSETTLED_ABANDON_SECONDS: u64 = 24 * 60 * 60;

/// Free disk below which an **automatic** reclaim run declines to start.
///
/// A reclaim delete registers a mutation that adds a `_row_exists` mask before
/// any merge removes anything, so reclamation's first effect on a full disk is
/// to use more of it. Running it unconditionally from a 60 s tick on a host
/// with no headroom is how a disk-full incident becomes a disk-full outage.
///
/// `moraine db reclaim run --confirm` is **not** subject to this: an operator
/// who has looked at the host may need to reclaim precisely because it is
/// full, and refusing them there would leave no path forward.
pub const RECLAIM_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Who asked for this run. The two triggers are not interchangeable, and the
/// difference is a safety boundary rather than telemetry.
///
/// An **operator** run is a person who has looked at the host and typed
/// `moraine db reclaim run --confirm`. They may need to reclaim *because* the
/// disk is full, so a free-space refusal there would leave them no path
/// forward, and they are present to watch what happens.
///
/// A **maintenance** run is a 60-second background tick with nobody watching.
/// It consults free space first, because reclamation's first effect is to
/// write `_row_exists` masks — it makes the disk fuller before any merge makes
/// it emptier — and because on this host the delete it issues to
/// `mcp_open_events` cannot be index-pruned at all
/// (see [`RECLAIM_MAX_UNITS_PER_RUN`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimTrigger {
    /// An explicit, confirmed CLI invocation.
    Operator,
    /// The background maintenance tick.
    Maintenance,
}

impl ReclaimTrigger {
    /// Whether this trigger refuses to start on a nearly-full disk.
    pub fn checks_free_space(self) -> bool {
        matches!(self, Self::Maintenance)
    }
}

/// What one scope's re-drive pass did. Every field is a count of units.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimRedriveReport {
    /// Units completed to `done` by this pass.
    pub redriven: u64,
    /// Units that failed again and remain unsettled for the next tick.
    pub failed: u64,
    /// Units that exceeded [`RECLAIM_UNSETTLED_ABANDON_SECONDS`] and were
    /// moved to `abandoned`.
    pub abandoned: u64,
    /// Rows this build cannot execute — an unknown scope or phase after a
    /// downgrade. Surfaced rather than silently skipped, because a
    /// permanently non-zero count is an operator-visible upgrade problem.
    pub unresumable: u64,
    /// The last per-unit failure, for the log line.
    pub last_error: Option<String>,
}

impl ReclaimRedriveReport {
    /// Whether this pass did anything an operator should hear about.
    pub fn is_quiet(&self) -> bool {
        self.failed == 0 && self.abandoned == 0 && self.unresumable == 0
    }
}

/// A registered scope executor: the probe that finds its units and the
/// predicate set each unit hands the emitter.
///
/// Function pointers rather than a trait object because the registry is a
/// `const`-shaped decision — "which scopes may delete in this build" — and it
/// must stay answerable without constructing anything. `executor_for` returning
/// `Some` is the single fact
/// `no_executor_may_be_registered_before_the_ledger_driver_is_wired` reads.
#[derive(Clone, Copy)]
pub struct RegisteredExecutor {
    pub scope: ReclaimScope,
    /// `(database, horizon_seconds, limit) -> candidate probe SQL`. Writes
    /// nothing; this is the entire dry-run path.
    pub(crate) probe: fn(&str, u64, usize) -> String,
    /// The unit's per-table delete predicates, **children first, parent last**.
    pub(crate) predicates: fn(&ReclaimUnit) -> Vec<(ReclaimTable, String)>,
}

impl fmt::Debug for RegisteredExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredExecutor")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl PartialEq for RegisteredExecutor {
    fn eq(&self, other: &Self) -> bool {
        self.scope == other.scope
    }
}

impl Eq for RegisteredExecutor {}

/// The executor for `scope`, if one has been registered in this build.
pub fn executor_for(scope: ReclaimScope) -> Option<RegisteredExecutor> {
    match scope {
        // WI-05.
        ReclaimScope::McpOpenOrphan => Some(RegisteredExecutor {
            scope,
            probe: |database, horizon, limit| {
                crate::reclaim_mcp_open::orphan_candidate_sql(database, horizon, limit)
            },
            predicates: crate::reclaim_mcp_open::mcp_open_unit_predicates,
        }),
        // WI-05b (plan §0 F4).
        ReclaimScope::McpOpenRetiredLineage => Some(RegisteredExecutor {
            scope,
            probe: |database, horizon, limit| {
                crate::reclaim_mcp_open::retired_lineage_candidate_sql(database, horizon, limit)
            },
            predicates: crate::reclaim_mcp_open::mcp_open_unit_predicates,
        }),
        // WI-07.
        ReclaimScope::ReadIndexGeneration => Some(RegisteredExecutor {
            scope,
            probe: |database, horizon, limit| {
                crate::reclaim_read_index::read_index_candidate_sql(database, horizon, limit)
            },
            predicates: crate::reclaim_read_index::read_index_unit_predicates,
        }),
        // WI-09 registers this.
        ReclaimScope::CanonicalGeneration => None,
    }
}

/// Every scope with a registered executor. Three in this build — the two
/// legacy open-projection scopes and the canonical read-index scope; see
/// [`executor_for`].
pub fn registered_executors() -> Vec<ReclaimScope> {
    ReclaimScope::ALL
        .into_iter()
        .filter(|scope| executor_for(*scope).is_some())
        .collect()
}

/// The refusal a `run` prints for a scope with no executor.
///
/// A named function rather than an inline `format!` so the renderer's
/// denomination scan can read the text this build actually emits, instead of
/// a copy fabricated in a test.
pub fn no_executor_message(scope: ReclaimScope) -> String {
    format!(
        "no executor is registered for scope `{scope}`; {} adds it. Nothing was deleted.",
        pending_work_item(scope)
    )
}

/// The work item that will register `scope`'s executor, for the refusal text.
fn pending_work_item(scope: ReclaimScope) -> &'static str {
    match scope {
        ReclaimScope::McpOpenOrphan => "WI-05",
        ReclaimScope::McpOpenRetiredLineage => "WI-05b",
        ReclaimScope::ReadIndexGeneration => "WI-07",
        ReclaimScope::CanonicalGeneration => "WI-09",
    }
}

// ---------------------------------------------------------------------------
// Statement construction
// ---------------------------------------------------------------------------

/// The single function that constructs a `DELETE` naming a reclaim table
/// (§4 S3, the emitter-side check).
///
/// It performs three checks, and the third is the one that bounds *extent*:
///
/// 1. It re-derives the table's class from [`classify`] — an *independent*
///    source from whatever the planner used to build the unit.
/// 2. It refuses unless handed a token that authorizes that class. The
///    regression: a planner bug that mis-scopes a unit still cannot produce a
///    canonical delete, because the emitter checks again.
/// 3. It refuses a predicate whose text does not **mention** every key column
///    of the table's [`ReclaimPredicate`] shape. Checks 1 and 2 prove the
///    emitter may name `events`; only check 3 constrains *which rows* leave.
///    Without it, an executor falling back to `WHERE 1` on an empty chunk
///    passed the last independent gate before a `DELETE` reached ClickHouse.
///
///    It is [`predicate_names_column`] — **name presence, not binding**.
///    It cannot tell a bound key tuple from a mention: `WHERE (session_id,
///    candidate_generation) IN ()` passes (correctly — it deletes nothing) and
///    so would a predicate that merely spelled the column names. Turning that
///    into a real binding check needs a predicate the emitter *builds* rather
///    than one it inspects, which is WI-09's shape. Until then the honest
///    statement of what check 3 buys is: an executor cannot emit a predicate
///    that names no key column of *this table* at all.
///
///    It matches on identifier boundaries rather than by substring. The
///    substring form could not distinguish `generation` from
///    `candidate_generation`, which is exactly the pair the three `mcp_open`
///    tables disagree on, so a header delete carrying the child predicate
///    passed check 3 while naming no column the header table has.
///
/// Emits `DELETE FROM`, never `ALTER … DELETE`. That is a statement about the
/// **text**, not about the cost: measured on 25.12.5.44, the server rewrites
/// this into a `(UPDATE _row_exists = 0 WHERE …)` mutation under the default
/// `lightweight_delete_mode = alter_update`. See [`RECLAIM_DELETE_SETTINGS`]
/// and this module's header for what that means for bounding.
pub fn emit_delete_statement(
    database: &str,
    table: ReclaimTable,
    predicate_sql: &str,
    authorities: &[ReclaimAuthority],
) -> Result<String, EmitRefusal> {
    let name = table.name();
    let Some(class) = classify(name) else {
        return Err(EmitRefusal::UnclassifiedTable {
            table: name.to_string(),
        });
    };
    if !authorities
        .iter()
        .any(|authority| authority.authorizes(class))
    {
        return Err(EmitRefusal::Unauthorized {
            table: name.to_string(),
            class,
        });
    }
    let predicate = table.predicate();
    let missing: Vec<&'static str> = predicate
        .required_columns()
        .iter()
        .copied()
        .filter(|column| !predicate_names_column(predicate_sql, column))
        .collect();
    if !missing.is_empty() {
        return Err(EmitRefusal::UnboundPredicate {
            table: name.to_string(),
            predicate,
            missing,
        });
    }
    Ok(format!(
        "DELETE FROM {}.{name}\nWHERE {predicate_sql}",
        escape_identifier(database)
    ))
}

/// Ledger claim write. Happens **before any delete** and is derived from
/// relations the deletes do not touch, which is what makes a crash recoverable.
pub fn ledger_claim_statement(database: &str, unit: &ReclaimUnit) -> String {
    // `claimed_at` is when the unit was **first** claimed. A phase advance
    // carries the age forward rather than resetting the clock, so
    // `ledger_redrive_sql`'s `ORDER BY claimed_at` is stable and an
    // always-failing unit actually ages towards `abandoned` instead of being
    // rejuvenated by its own failed re-drive.
    let claimed_at = if unit.unsettled_seconds == 0 {
        "now64(3)".to_string()
    } else {
        format!("now64(3) - toIntervalSecond({})", unit.unsettled_seconds)
    };
    format!(
        "INSERT INTO {}.{RECLAIM_LEDGER_TABLE}\n\
         (reclaim_id, scope, source_host, source_name, source_file, source_generation,\n \
          session_id, candidate_generation, phase, estimated_rows, estimated_bytes,\n \
          claimed_at, ledger_revision)\n\
         VALUES ({}, {}, {}, {}, {}, toUInt32({}), {}, toUInt64({}), {}, toUInt64({}), \
         toUInt64({}), {claimed_at}, generateSnowflakeID())",
        escape_identifier(database),
        escape_literal(&unit.reclaim_id),
        escape_literal(unit.scope.as_str()),
        escape_literal(&unit.source_host),
        escape_literal(&unit.source_name),
        escape_literal(&unit.source_file),
        unit.source_generation,
        escape_literal(&unit.session_id),
        unit.candidate_generation,
        escape_literal(unit.phase.as_str()),
        unit.estimated_rows,
        unit.estimated_bytes,
    )
}

/// Advance a claimed unit to a later phase. A `ReplacingMergeTree` keyed on
/// `(scope, reclaim_id)` collapses to the newest `ledger_revision`, so this is
/// an insert, never an update and never a delete.
pub fn ledger_advance_statement(database: &str, unit: &ReclaimUnit, phase: ReclaimPhase) -> String {
    let advanced = ReclaimUnit {
        phase,
        ..unit.clone()
    };
    ledger_claim_statement(database, &advanced)
}

/// Units of **one scope** awaiting re-drive, oldest first. Read at the head of
/// every run and on startup: a `claimed` or `deleting` unit is completed
/// **before** new units are planned.
///
/// The `scope` predicate is not a tidiness: without it this took the 64 oldest
/// unsettled rows across *all* scopes and filtered in Rust, so 64 wedged rows
/// belonging to one scope starved every other scope's re-drive completely —
/// the page would be full of rows the caller then skipped, and no scope but
/// the wedged one would ever see a unit again. Guarded by
/// `the_redrive_page_is_scoped_so_one_wedged_scope_cannot_starve_another`.
///
/// `claimed_at` is the unit's **first** claim (see
/// [`ledger_advance_statement`]), so this order is stable across re-drives and
/// a poison unit does not shuffle to the back of the queue on every tick.
pub fn ledger_redrive_sql(database: &str, scope: ReclaimScope, limit: usize) -> String {
    format!(
        "SELECT reclaim_id, scope, source_host, source_name, source_file,\n \
          toUInt32(source_generation) AS source_generation, session_id,\n \
          toUInt64(candidate_generation) AS candidate_generation, phase,\n \
          toUInt64(estimated_rows) AS estimated_rows, toUInt64(estimated_bytes) AS estimated_bytes,\n \
          toUInt64(dateDiff('second', claimed_at, now64(3))) AS unsettled_seconds\n\
         FROM {}.{RECLAIM_LEDGER_TABLE} FINAL\n\
         WHERE phase IN ('claimed', 'deleting')\n   \
           AND scope = {}\n\
         ORDER BY claimed_at ASC, reclaim_id ASC\n\
         LIMIT {limit}\n\
         FORMAT JSONEachRow",
        escape_identifier(database),
        escape_literal(scope.as_str()),
    )
}

/// Ledger totals by phase.
pub fn ledger_summary_sql(database: &str) -> String {
    format!(
        "SELECT phase, toUInt64(count()) AS units\n\
         FROM {}.{RECLAIM_LEDGER_TABLE} FINAL\n\
         GROUP BY phase\n\
         FORMAT JSONEachRow",
        escape_identifier(database)
    )
}

/// Pending mutations over a scope's tables (hazard H9). A run that finds any
/// returns [`ReclaimOutcome::Blocked`] carrying the count, never `Ok(default)`.
pub fn pending_mutations_sql(database: &str, scope: ReclaimScope) -> String {
    let tables = scope
        .tables()
        .iter()
        .map(|table| escape_literal(table.name()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT toUInt64(count()) AS pending\n\
         FROM system.mutations\n\
         WHERE database = {}\n   \
           AND table IN ({tables})\n   \
           AND is_done = 0\n\
         FORMAT JSONEachRow",
        escape_literal(database)
    )
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct LedgerPhaseRow {
    phase: String,
    units: u64,
}

#[derive(Debug, Deserialize)]
struct PendingMutationsRow {
    pending: u64,
}

/// One row of [`ledger_redrive_sql`], before its `scope` and `phase` strings
/// are parsed back into the closed enums.
#[derive(Debug, Deserialize)]
struct LedgerUnitRow {
    reclaim_id: String,
    scope: String,
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    session_id: String,
    candidate_generation: u64,
    phase: String,
    estimated_rows: u64,
    estimated_bytes: u64,
    #[serde(default)]
    unsettled_seconds: u64,
}

impl LedgerUnitRow {
    /// `None` for a row whose scope or phase this build does not know.
    ///
    /// A downgrade past a work item leaves ledger rows naming a scope the
    /// running binary cannot execute. Skipping them is the only safe answer:
    /// the alternative is either executing a unit whose predicate shape this
    /// build does not have, or settling one whose deletes never ran.
    fn into_unit(self) -> Option<ReclaimUnit> {
        Some(ReclaimUnit {
            reclaim_id: self.reclaim_id,
            scope: ReclaimScope::parse(&self.scope)?,
            source_host: self.source_host,
            source_name: self.source_name,
            source_file: self.source_file,
            source_generation: self.source_generation,
            session_id: self.session_id,
            candidate_generation: self.candidate_generation,
            phase: ReclaimPhase::parse(&self.phase)?,
            estimated_rows: self.estimated_rows,
            estimated_bytes: self.estimated_bytes,
            unsettled_seconds: self.unsettled_seconds,
        })
    }
}

impl ClickHouseClient {
    /// Ledger totals by phase. Never writes.
    pub async fn reclaim_ledger_summary(&self) -> Result<ReclaimLedgerSummary> {
        let rows: Vec<LedgerPhaseRow> = self
            .query_json_each_row(
                &ledger_summary_sql(&self.cfg.database),
                Some(&self.cfg.database),
            )
            .await
            .context("failed to read the storage reclaim ledger")?;
        let mut summary = ReclaimLedgerSummary::default();
        for row in rows {
            match ReclaimPhase::parse(&row.phase) {
                Some(ReclaimPhase::Claimed) => summary.claimed = row.units,
                Some(ReclaimPhase::Deleting) => summary.deleting = row.units,
                Some(ReclaimPhase::Done) => summary.done = row.units,
                Some(ReclaimPhase::Abandoned) => summary.abandoned = row.units,
                None => {}
            }
        }
        Ok(summary)
    }

    /// Pending mutations over `scope`'s tables.
    pub async fn reclaim_pending_mutations(&self, scope: ReclaimScope) -> Result<u64> {
        let rows: Vec<PendingMutationsRow> = self
            .query_json_each_row(
                &pending_mutations_sql(&self.cfg.database, scope),
                Some(&self.cfg.database),
            )
            .await
            .context("failed to inspect pending reclaim mutations")?;
        Ok(rows.first().map(|row| row.pending).unwrap_or(0))
    }

    /// Candidate units for `scope`, or `None` when no executor is registered.
    /// **Writes nothing** — this is the entire dry-run path, and
    /// [`Self::reclaim_run`] plans through the same call so the plan an
    /// operator reads is the plan a run claims.
    async fn reclaim_candidates(
        &self,
        scope: ReclaimScope,
        retention: &RetentionConfig,
        limit: usize,
    ) -> Result<Option<Vec<ReclaimCandidateRow>>> {
        let Some(executor) = executor_for(scope) else {
            return Ok(None);
        };
        let horizon = retention.derived_horizon_seconds().max(0.0) as u64;
        let sql = (executor.probe)(&self.cfg.database, horizon, limit);
        let rows = self
            .query_json_each_row(&sql, Some(&self.cfg.database))
            .await
            .with_context(|| format!("failed to probe reclaim candidates for scope `{scope}`"))?;
        Ok(Some(rows))
    }

    /// Compressed bytes per row for a table, from `system.parts` — never a
    /// `FINAL` scan of the table itself (hazard H4).
    ///
    /// The estimate is deliberately crude, and §3.7 is why it may be: nothing
    /// is partitioned by the unit key, so no exact "bytes this unit occupies"
    /// exists to compute. Every surface that reports the number carries
    /// [`estimated_bytes_note`].
    fn reclaim_bytes_per_row(report: Option<&StorageReport>, tables: &[ReclaimTable]) -> f64 {
        let Some(report) = report else {
            return 0.0;
        };
        let (rows, bytes) = report
            .tables
            .iter()
            .filter(|table| tables.iter().any(|wanted| wanted.name() == table.name))
            .fold((0_u64, 0_u64), |(rows, bytes), table| {
                (
                    rows.saturating_add(table.rows),
                    bytes.saturating_add(table.compressed_bytes),
                )
            });
        if rows == 0 {
            0.0
        } else {
            bytes as f64 / rows as f64
        }
    }

    /// The dry-run planner. **Writes nothing.**
    pub async fn reclaim_plan(
        &self,
        retention: &RetentionConfig,
        scopes: &[ReclaimScope],
    ) -> Result<ReclaimPlan> {
        let ledger = self.reclaim_ledger_summary().await.unwrap_or_default();
        // One `system.parts`/`system.disks` read for the whole pass, not one
        // per scope: the byte estimate is the same denominator for all of them
        // and `PLAN_STATEMENT_CAP` is derived on that basis.
        let report = self.storage_report(&RetentionConfig::default()).await.ok();
        let mut estimates = Vec::with_capacity(scopes.len());
        for scope in scopes {
            estimates.push(self.plan_scope(*scope, retention, report.as_ref()).await);
        }
        Ok(ReclaimPlan {
            scopes: estimates,
            denomination: estimated_bytes_note(),
            pending_redrive: ledger.needs_redrive(),
        })
    }

    async fn plan_scope(
        &self,
        scope: ReclaimScope,
        retention: &RetentionConfig,
        report: Option<&StorageReport>,
    ) -> ReclaimableEstimate {
        let tables: Vec<String> = scope
            .tables()
            .iter()
            .map(|table| table.name().to_string())
            .collect();
        let empty = |note: Option<String>| ReclaimableEstimate {
            scope,
            units: 0,
            estimated_rows: 0,
            estimated_bytes: 0,
            tables: tables.clone(),
            note,
        };
        if let Err(missing) = ReclaimAuthority::for_scope(scope, retention) {
            return empty(Some(missing.to_string()));
        }
        let candidates = match self
            .reclaim_candidates(scope, retention, RECLAIM_MAX_UNITS_PER_RUN)
            .await
        {
            Ok(Some(candidates)) => candidates,
            Ok(None) => {
                return empty(Some(format!(
                    "no candidate probe is registered for `{scope}` in this build; {} adds it",
                    pending_work_item(scope)
                )))
            }
            Err(error) => return empty(Some(format!("candidate probe failed: {error:#}"))),
        };
        if candidates.is_empty() {
            return empty(None);
        }
        let estimated_rows = candidates.iter().fold(0_u64, |total, row| {
            total.saturating_add(row.estimated_rows())
        });
        let bytes_per_row = Self::reclaim_bytes_per_row(report, scope.tables());
        ReclaimableEstimate {
            scope,
            units: candidates.len() as u64,
            estimated_rows,
            estimated_bytes: (estimated_rows as f64 * bytes_per_row) as u64,
            tables,
            note: (candidates.len() == RECLAIM_MAX_UNITS_PER_RUN).then(|| {
                format!("bounded at {RECLAIM_MAX_UNITS_PER_RUN} units per run; more may remain")
            }),
        }
    }

    /// Complete every unit the ledger left in `claimed` or `deleting`.
    ///
    /// **Runs at the head of every run, before any candidate is probed.** A
    /// process that died between a claim and its deletes, or between two of its
    /// deletes, leaves a durable row naming exactly which key set was in
    /// flight; that row is what makes the set re-derivable when the rows it
    /// names no longer are. The existing reclaimer has no such row, which is
    /// why the reference host carries 11.17M child rows it can never see.
    ///
    /// Re-execution is idempotent: every delete is a predicate over the key
    /// pair the ledger row names, so replaying a completed unit removes zero
    /// additional rows.
    ///
    /// A unit whose scope no longer has an executor is left alone rather than
    /// abandoned: an operator downgrading past a work item must not have their
    /// in-flight units silently marked settled.
    pub async fn reclaim_redrive(
        &self,
        scope: ReclaimScope,
        retention: &RetentionConfig,
        budget: &moraine_config::ValidatedQueryBudget,
        admin_budget: &moraine_config::ValidatedQueryBudget,
    ) -> Result<ReclaimRedriveReport> {
        let rows: Vec<LedgerUnitRow> = self
            .query_json_each_row(
                &ledger_redrive_sql(&self.cfg.database, scope, RECLAIM_MAX_UNITS_PER_RUN),
                Some(&self.cfg.database),
            )
            .await
            .context("failed to read unsettled storage reclaim units")?;
        let mut report = ReclaimRedriveReport::default();
        for row in rows {
            let Some(unit) = row.into_unit() else {
                report.unresumable = report.unresumable.saturating_add(1);
                continue;
            };
            if unit.scope != scope || executor_for(unit.scope).is_none() {
                report.unresumable = report.unresumable.saturating_add(1);
                continue;
            }
            // §3.7's missing bound, in the only denomination this ledger can
            // express without a schema change. A unit that has been unsettled
            // for longer than the abandon horizon has survived hundreds of
            // re-drive attempts; it is not going to succeed on the next one,
            // and until now it took its whole scope down with it forever.
            if unit.unsettled_seconds >= RECLAIM_UNSETTLED_ABANDON_SECONDS {
                let abandoned = ReclaimUnit {
                    phase: ReclaimPhase::Abandoned,
                    ..unit.clone()
                };
                self.reclaim_ledger_write(&ledger_claim_statement(&self.cfg.database, &abandoned))
                    .await
                    .context("failed to abandon a wedged reclaim unit")?;
                report.abandoned = report.abandoned.saturating_add(1);
                continue;
            }
            // **Per-unit isolation.** Previously this `?`-propagated, and
            // because re-drive runs before any candidate is probed, one
            // deterministically-failing unit aborted every subsequent run of
            // its scope on every tick, indefinitely, with no new work ever
            // claimed and one `warn!` per tick as the only signal. A failed
            // unit is left `deleting` — its ledger row is still durable, so
            // the next tick retries it — and the run continues.
            match self
                .reclaim_unit_envelope(budget, admin_budget)
                .scope(self.reclaim_execute_unit(&unit, retention))
                .await
            {
                Ok(()) => report.redriven = report.redriven.saturating_add(1),
                Err(error) => {
                    report.failed = report.failed.saturating_add(1);
                    report.last_error = Some(format!("`{}`: {error:#}", unit.reclaim_id));
                }
            }
        }
        Ok(report)
    }

    /// Advance one claimed unit to `deleting`, issue its deletes **children
    /// first and the parent last**, then settle it to `done`.
    ///
    /// The ordering is the inverse of `reclaim_superseded_mcp_open_snapshots`
    /// and the inversion is the point: with the claim durable, a crash between
    /// two deletes leaves an intact, still-authorizable snapshot that the next
    /// run finishes, rather than children no probe can ever see again. Reader
    /// safety comes from the horizon and the anti-join — the unit was already
    /// unreachable before it was claimed — not from delete order.
    ///
    /// Every statement goes through [`emit_delete_statement`], so §4 S3's
    /// second, independent check runs on the way to ClickHouse even though the
    /// planner already decided the unit's scope.
    /// One `Background` envelope per unit (§3.7).
    ///
    /// `Background` rather than `Migration` for its budget and statement cap,
    /// **not** for cancellability. `arms_cancel_guards()` is true for this
    /// class and its drop guard issues `KILL QUERY`, but a reclaim delete is a
    /// server-side mutation (see [`RECLAIM_DELETE_SETTINGS`]) and killing the
    /// initiating query does not cancel one. Hazard H8's answer here is the
    /// bound, not the kill: one mutation in flight at a time, and the unit
    /// loop re-checks its stop conditions between units.
    fn reclaim_unit_envelope(
        &self,
        budget: &moraine_config::ValidatedQueryBudget,
        admin_budget: &moraine_config::ValidatedQueryBudget,
    ) -> std::sync::Arc<QueryEnvelope> {
        QueryEnvelope::new_batch(
            "reclaim-unit",
            QueryClass::Background,
            budget,
            admin_budget,
            UNIT_STATEMENT_CAP,
        )
    }

    /// `retention` is the **caller's** config, never
    /// `RetentionConfig::default()`.
    ///
    /// Re-deriving authority from a default was harmless for the two bucket-3
    /// scopes — `DerivedOnly` needs no `[retention]` key — and fatal for every
    /// scope that does: `ReclaimAuthority::for_scope(CanonicalGeneration,
    /// &RetentionConfig::default())` always errors, so WI-09's executor would
    /// have been unable to execute a single unit the moment it was
    /// registered, including on re-drive of a unit an operator had properly
    /// authorized. Guarded by
    /// `an_execute_unit_uses_the_callers_retention_not_a_default`.
    async fn reclaim_execute_unit(
        &self,
        unit: &ReclaimUnit,
        retention: &RetentionConfig,
    ) -> Result<()> {
        let executor = executor_for(unit.scope)
            .ok_or_else(|| anyhow::anyhow!("no executor for scope `{}`", unit.scope))?;
        let authorities = ReclaimAuthority::for_scope(unit.scope, retention)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("scope `{}` lost its authority", unit.scope))?;

        self.reclaim_ledger_write(&ledger_advance_statement(
            &self.cfg.database,
            unit,
            ReclaimPhase::Deleting,
        ))
        .await
        .context("failed to advance a reclaim unit to `deleting`")?;

        for (table, predicate) in (executor.predicates)(unit) {
            let statement =
                emit_delete_statement(&self.cfg.database, table, &predicate, &authorities)
                    .map_err(anyhow::Error::new)
                    .with_context(|| format!("refusing to reclaim `{}`", table.name()))?;
            self.reclaim_delete_write(&statement)
                .await
                .with_context(|| format!("failed to reclaim rows from `{}`", table.name()))?;
        }

        self.reclaim_ledger_write(&ledger_advance_statement(
            &self.cfg.database,
            unit,
            ReclaimPhase::Done,
        ))
        .await
        .context("failed to settle a reclaim unit")?;
        Ok(())
    }

    /// Insert-profile transport for one reclaim **ledger** statement.
    ///
    /// Ledger writes are `INSERT`s into a `ReplacingMergeTree`; they carry no
    /// mutation settings because they register no mutation.
    async fn reclaim_ledger_write(&self, statement: &str) -> Result<()> {
        self.reclaim_write(statement, &[]).await
    }

    /// Insert-profile transport for one reclaim **delete**, with
    /// [`RECLAIM_DELETE_SETTINGS`] pinned.
    ///
    /// Separate from [`Self::reclaim_ledger_write`] so the settings cannot be
    /// attached to the wrong statement class and so the difference is visible
    /// at the call site: this is the one that registers a mutation.
    async fn reclaim_delete_write(&self, statement: &str) -> Result<()> {
        self.reclaim_write(statement, RECLAIM_DELETE_SETTINGS).await
    }

    async fn reclaim_write(&self, statement: &str, settings: &[(&str, &str)]) -> Result<()> {
        debug_assert!(
            writes_control_relation(statement).is_none(),
            "INV-2: the reclaimer must never write a control relation"
        );
        self.mutation_request_text_with_params_and_timeout(
            "",
            Some(statement.as_bytes().to_vec()),
            Some(&self.cfg.database),
            settings,
            Some(crate::migration_request_timeout(self.cfg.timeout_seconds)),
        )
        .await?;
        Ok(())
    }

    /// `moraine db reclaim run` — the §3.2 protocol, driven.
    ///
    /// In order, and the order is the safety property:
    ///
    /// 1. **Authority.** `ReclaimAuthority::for_scope` is the S2 enforcement
    ///    point at the command boundary, not a nicety for a better error
    ///    message: deleting that one line lets a run with no `[retention]` key
    ///    proceed. Guarded by
    ///    `reclaim_run_refuses_an_unconfigured_canonical_scope_before_anything_else`.
    /// 2. **Executor registry.** Guarded by
    ///    `reclaim_run_refuses_an_unregistered_scope_without_reaching_clickhouse`.
    /// 3. **Pending mutations** (hazard H9) — a typed `Blocked` carrying the
    ///    count, never `Ok(default)`.
    /// 4. **Free disk**, for an automatic run only (see [`ReclaimTrigger`]).
    /// 5. **Re-drive**, before anything is planned.
    /// 6. **Plan, claim, execute, settle**, one unit at a time.
    ///
    /// A unit is claimed **before** its first delete and from relations the
    /// deletes do not touch. That is what the whole ledger is for, and
    /// `an_interrupted_claim_is_completed_by_the_next_runs_redrive` interrupts
    /// a real claim between the child and parent deletes to prove step 5
    /// finishes it.
    pub async fn reclaim_run(
        &self,
        retention: &RetentionConfig,
        scope: ReclaimScope,
        trigger: ReclaimTrigger,
        budget: &moraine_config::ValidatedQueryBudget,
        admin_budget: &moraine_config::ValidatedQueryBudget,
    ) -> Result<ReclaimOutcome> {
        ReclaimAuthority::for_scope(scope, retention)?;
        if executor_for(scope).is_none() {
            return Ok(ReclaimOutcome::NoExecutor {
                scope,
                message: no_executor_message(scope),
            });
        }
        let pending = self.reclaim_pending_mutations(scope).await?;
        if pending > 0 {
            return Ok(ReclaimOutcome::Blocked {
                scope,
                pending_mutations: pending,
            });
        }

        // One `system.parts`/`system.disks` read, reused for the free-space
        // gate and the byte estimate below.
        let report = self.storage_report(&RetentionConfig::default()).await.ok();
        if trigger.checks_free_space() {
            if let Some(free_bytes) = report
                .as_ref()
                .and_then(|report| report.disk.as_ref())
                .map(|disk| disk.free_bytes)
            {
                if free_bytes < RECLAIM_MIN_FREE_BYTES {
                    return Ok(ReclaimOutcome::LowDisk {
                        scope,
                        free_bytes,
                        required_bytes: RECLAIM_MIN_FREE_BYTES,
                    });
                }
            }
        }

        let redrive = self
            .reclaim_redrive(scope, retention, budget, admin_budget)
            .await?;

        let candidates = self
            .reclaim_candidates(scope, retention, RECLAIM_MAX_UNITS_PER_RUN)
            .await?
            .unwrap_or_default();
        // `Idle` means the whole run was a no-op. A pass that re-drove,
        // abandoned, or failed units did something, and saying "nothing to
        // reclaim" for it is how a wedged scope stays invisible.
        if candidates.is_empty() && redrive == ReclaimRedriveReport::default() {
            return Ok(ReclaimOutcome::Idle { scope });
        }

        let bytes_per_row = Self::reclaim_bytes_per_row(report.as_ref(), scope.tables());
        let mut units = redrive.redriven;
        let mut estimated_rows_total = 0_u64;
        for candidate in candidates {
            // A row that does not carry its scope's unit key is refused here,
            // before anything durable happens — see
            // `ReclaimCandidateRow::into_unit`.
            let mut unit = candidate.into_unit(scope)?;
            let estimated_rows = unit.estimated_rows;
            unit.estimated_bytes = (estimated_rows as f64 * bytes_per_row) as u64;
            // Claim first, then execute, both inside the unit's own
            // envelope. Everything after the claim is re-derivable from the
            // ledger row that statement makes durable.
            self.reclaim_unit_envelope(budget, admin_budget)
                .scope(async {
                    self.reclaim_ledger_write(&ledger_claim_statement(&self.cfg.database, &unit))
                        .await
                        .context("failed to claim a storage reclaim unit")?;
                    self.reclaim_execute_unit(&unit, retention).await
                })
                .await?;
            units = units.saturating_add(1);
            estimated_rows_total = estimated_rows_total.saturating_add(estimated_rows);
        }

        Ok(ReclaimOutcome::Settled {
            scope,
            units,
            estimated_rows: estimated_rows_total,
            redriven: redrive.redriven,
            failed: redrive.failed,
            abandoned: redrive.abandoned,
            denomination: reclaimed_bytes_note(),
        })
    }

    /// `moraine db reclaim status`: WI-02's storage report, the ledger, and
    /// the dry-run estimates, under one `Background` envelope.
    ///
    /// Never fails: an unreachable ClickHouse yields `available: false` with
    /// the error attached, so the surrounding command still renders.
    pub async fn reclaim_status(
        &self,
        retention: &RetentionConfig,
        budget: &moraine_config::ValidatedQueryBudget,
        admin_budget: &moraine_config::ValidatedQueryBudget,
    ) -> ReclaimStatusReport {
        let gathered = QueryEnvelope::new_batch(
            "reclaim-status",
            QueryClass::Background,
            budget,
            admin_budget,
            STATUS_STATEMENT_CAP,
        )
        .scope(async {
            let storage = self.storage_report(retention).await?;
            let ledger = self.reclaim_ledger_summary().await.unwrap_or_default();
            let plan = self.reclaim_plan(retention, &ReclaimScope::ALL).await?;
            anyhow::Ok((storage, ledger, plan))
        })
        .await;

        match gathered {
            Ok((storage, ledger, plan)) => ReclaimStatusReport {
                available: true,
                storage: Some(storage),
                ledger,
                reclaimable: plan.scopes,
                registered_executors: registered_executors(),
                denomination: estimated_bytes_note(),
                error: None,
            },
            Err(error) => ReclaimStatusReport {
                available: false,
                storage: None,
                ledger: ReclaimLedgerSummary::default(),
                reclaimable: Vec::new(),
                registered_executors: registered_executors(),
                denomination: estimated_bytes_note(),
                error: Some(error.to_string()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(scope: ReclaimScope) -> ReclaimUnit {
        ReclaimUnit {
            reclaim_id: "7'2".to_string(),
            scope,
            source_host: "host-a".to_string(),
            source_name: "codex".to_string(),
            source_file: "/tmp/a'b.jsonl".to_string(),
            source_generation: 3,
            session_id: "s-1".to_string(),
            candidate_generation: 99,
            phase: ReclaimPhase::Claimed,
            estimated_rows: 10,
            estimated_bytes: 20,
            unsettled_seconds: 0,
        }
    }

    fn derived_only() -> Vec<ReclaimAuthority> {
        vec![ReclaimAuthority::DerivedOnly]
    }

    fn full_authority() -> Vec<ReclaimAuthority> {
        let retention = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        };
        ReclaimAuthority::for_scope(ReclaimScope::CanonicalGeneration, &retention)
            .expect("configured retention grants authority")
    }

    /// A predicate of the shape `table` requires, naming every key column of
    /// the claimed unit.
    ///
    /// The corpus used to build every statement with `"1"` — a full-table
    /// delete — which is why it could not have caught an executor that emitted
    /// one. The emitter now refuses that, so the corpus has to bind its units
    /// like a real executor would.
    fn sample_predicate(table: ReclaimTable) -> String {
        match table.predicate() {
            ReclaimPredicate::Generation => "(source_host, source_name, source_file, \
                 source_generation) IN (('host-a', 'codex', '/tmp/a.jsonl', 3))"
                .to_string(),
            ReclaimPredicate::SessionGeneration => {
                "(session_id, candidate_generation) IN (('s-1', 99))".to_string()
            }
            ReclaimPredicate::SessionHeaderGeneration => {
                "(session_id, generation) IN (('s-1', 99))".to_string()
            }
            ReclaimPredicate::UidSet => "event_uid IN ('u-1', 'u-2')".to_string(),
            ReclaimPredicate::DocumentJoin => {
                "doc_id IN (SELECT event_uid FROM moraine.search_documents)".to_string()
            }
        }
    }

    /// Every statement this module can emit, across every scope and every
    /// table, with maximal authority. The corpus **G-INV2** asserts over.
    fn every_emittable_statement() -> Vec<String> {
        let mut statements = Vec::new();
        let authorities = full_authority();
        for scope in ReclaimScope::ALL {
            statements.push(ledger_claim_statement("moraine", &unit(scope)));
            for phase in [
                ReclaimPhase::Deleting,
                ReclaimPhase::Done,
                ReclaimPhase::Abandoned,
            ] {
                statements.push(ledger_advance_statement("moraine", &unit(scope), phase));
            }
            statements.push(pending_mutations_sql("moraine", scope));
            for table in scope.tables() {
                statements.push(
                    emit_delete_statement(
                        "moraine",
                        *table,
                        &sample_predicate(*table),
                        &authorities,
                    )
                    .expect("full authority emits every table"),
                );
            }
        }
        statements.push(ledger_summary_sql("moraine"));
        statements.push(ledger_redrive_sql(
            "moraine",
            ReclaimScope::McpOpenOrphan,
            64,
        ));
        statements
    }

    /// **G-INV2.** Fails for: the reclaimer writing a control relation.
    /// Denomination: statement text, all scopes.
    ///
    /// MUTATION (executed 2026-07-27): add
    /// `statements.push(format!("INSERT INTO {}.published_source_generations VALUES (1)", escape_identifier("moraine")))`
    /// to `every_emittable_statement` => FAILS naming
    /// `published_source_generations`. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-27): add the same statement written
    /// **unescaped** — `INSERT INTO moraine.published_source_generations
    /// VALUES (1)` — which the previous
    /// ``statement.contains("`moraine`.{control}")`` form let through
    /// untouched. => FAILS now. **Width: the invariant is about the relation,
    /// not about one spelling of the qualifier.**
    ///
    /// MUTATION (executed 2026-07-27): make `writes_control_relation` return
    /// `None` unconditionally => the negative-corpus assertion below FAILS, so
    /// a detector that detects nothing is not a passing "fix". **Upper
    /// bound.**
    #[test]
    fn no_emitted_statement_writes_a_control_relation() {
        let statements = every_emittable_statement();
        assert!(
            statements.len() >= 20,
            "the INV-2 corpus must actually contain the statements it claims to check: {}",
            statements.len()
        );
        assert!(
            statements
                .iter()
                .any(|statement| statement.contains("DELETE FROM `moraine`.events")),
            "the corpus must include the most dangerous statement the emitter can produce"
        );

        for statement in &statements {
            assert_eq!(
                writes_control_relation(statement),
                None,
                "INV-2 violated by `{statement}`"
            );
        }

        // The detector must actually detect, in both spellings, or the loop
        // above passes because nothing is ever a violation.
        for (statement, expected) in [
            (
                "INSERT INTO moraine.published_source_generations VALUES (1)",
                Some("published_source_generations"),
            ),
            (
                "INSERT INTO `moraine`.published_source_generations VALUES (1)",
                Some("published_source_generations"),
            ),
            (
                "ALTER TABLE moraine.ingest_checkpoint_transitions DELETE WHERE 1",
                Some("ingest_checkpoint_transitions"),
            ),
            (
                "TRUNCATE TABLE moraine.mcp_read_index_state",
                Some("mcp_read_index_state"),
            ),
            (
                "DROP TABLE moraine.ingest_append_control",
                Some("ingest_append_control"),
            ),
            // `OPTIMIZE … FINAL` returns no rows and reads as maintenance, but
            // `published_source_generations` is a ReplacingMergeTree keyed on
            // `publication_revision`: collapsing it destroys every superseded
            // revision and breaks as-of reads exactly as §1's rationale warns.
            //
            // MUTATION (executed 2026-07-27): drop
            // `|| statement.starts_with("OPTIMIZE")` from
            // `writes_control_relation` => FAILS on both rows below.
            (
                "OPTIMIZE TABLE moraine.published_source_generations FINAL",
                Some("published_source_generations"),
            ),
            (
                "OPTIMIZE TABLE `moraine`.ingest_checkpoint_transitions FINAL DEDUPLICATE",
                Some("ingest_checkpoint_transitions"),
            ),
            // Width: `OPTIMIZE` on a derived relation is legal — the shape is
            // not the violation, the relation is. Widening the detector to
            // "any OPTIMIZE" turns this red.
            ("OPTIMIZE TABLE moraine.mcp_open_turns FINAL", None),
            // Reads are not writes: the pending-mutation probe names every
            // scope table and must stay legal.
            (
                "SELECT count() FROM moraine.published_source_generations",
                None,
            ),
            // A generation predicate mentions `source_generation`, which is a
            // prefix of a control relation's name but not the relation.
            (
                "DELETE FROM `moraine`.events\nWHERE source_generation = 3",
                None,
            ),
        ] {
            assert_eq!(
                writes_control_relation(statement),
                expected,
                "detector disagreed on `{statement}`"
            );
        }
    }

    #[test]
    fn control_relations_are_exactly_the_five_inv2_names() {
        assert_eq!(CONTROL_RELATIONS.len(), 5);
        for control in CONTROL_RELATIONS {
            assert_eq!(
                classify(control),
                Some(TableClass::NeverDelete),
                "`{control}` must also be never-delete in the classification"
            );
        }
        // No reclaim table may be a control relation. Belt and braces: this
        // catches a future scope adding `mcp_read_index_state` to its list.
        for scope in ReclaimScope::ALL {
            for table in scope.tables() {
                assert!(
                    !CONTROL_RELATIONS.contains(&table.name()),
                    "scope `{scope}` names control relation `{}`",
                    table.name()
                );
            }
        }
    }

    /// **G-NOAUTH.** Fails for: the emitter producing a canonical delete
    /// without a token.
    /// Denomination: statement text.
    ///
    /// MUTATION (executed 2026-07-27): delete the `authorizes` check from
    /// `emit_delete_statement` (S3's re-derivation) => `events`, `raw_events`,
    /// and `ingest_errors` all emit under `DerivedOnly` and this test FAILS.
    /// That is the point of having two checks: the planner-side check in
    /// `ReclaimAuthority::for_scope` is a different function, so removing
    /// either one alone must still fail a named test.
    #[test]
    fn the_emitter_refuses_protected_tables_without_a_token() {
        for reclaim_table in [
            ReclaimTable::Events,
            ReclaimTable::RawEvents,
            ReclaimTable::IngestErrors,
        ] {
            let refusal = emit_delete_statement(
                "moraine",
                reclaim_table,
                &sample_predicate(reclaim_table),
                &derived_only(),
            )
            .expect_err("a derived-only token must not emit a protected delete");
            // The binding used to be named `table`, which shadowed the loop
            // variable and made the guard compare a `&String` to itself. A
            // tautology inside a safety check is worse than no check: it reads
            // as coverage.
            assert!(
                matches!(
                    refusal,
                    EmitRefusal::Unauthorized { ref table, .. } if table == reclaim_table.name()
                ),
                "{refusal:?}"
            );
            assert!(refusal.to_string().contains(reclaim_table.name()));
        }

        // Bounded in the other direction: a derived table still emits under a
        // derived-only token, or the emitter would refuse everything and the
        // test above would pass vacuously.
        let statement = emit_delete_statement(
            "moraine",
            ReclaimTable::McpOpenEvents,
            &sample_predicate(ReclaimTable::McpOpenEvents),
            &derived_only(),
        )
        .expect("derived tables emit under a derived-only token");
        assert!(statement.starts_with("DELETE FROM `moraine`.mcp_open_events"));

        // And with full authority the protected tables do emit, so the refusal
        // above is about the token and not about the table.
        for reclaim_table in [ReclaimTable::Events, ReclaimTable::RawEvents] {
            emit_delete_statement(
                "moraine",
                reclaim_table,
                &sample_predicate(reclaim_table),
                &full_authority(),
            )
            .expect("configured retention authorizes the protected tables");
        }
    }

    /// **G-AUTHMATRIX.** Fails for: any authority variant authorizing any
    /// class other than its own.
    /// Denomination: the full (variant x class) truth table.
    ///
    /// §4 S3 requires the *corresponding* token; S2 makes permission
    /// per-bucket. `CanonicalHistory(_)` used to also return `true` for
    /// `RawAudit` and `Derived`, which was unreachable only because
    /// `for_scope` demands both keys — i.e. only because of the planner-side
    /// check S3 says the emitter must not depend on.
    /// `no_authority_variant_can_unlock_a_never_delete_table` bounded only the
    /// never-delete row of this table.
    ///
    /// MUTATION (executed 2026-07-27): restore
    /// `(Self::CanonicalHistory(_), TableClass::CanonicalHistory |
    /// TableClass::RawAudit | TableClass::Derived) => true` => FAILS on two
    /// cells. **Width: widening any arm by one class breaks a named cell.**
    ///
    /// MUTATION (executed 2026-07-27): narrow
    /// `(Self::DerivedOnly, TableClass::Derived)` to `=> false` => FAILS on
    /// the `DerivedOnly`/`Derived` cell, so emptying the matrix is not a
    /// passing "fix". **Both directions.**
    #[test]
    fn each_authority_variant_authorizes_exactly_its_own_bucket() {
        let horizon = |bucket| {
            RetentionHorizon::from_config(
                &RetentionConfig {
                    canonical_history_horizon_days: Some(365.0),
                    raw_audit_horizon_days: Some(90.0),
                    ..RetentionConfig::default()
                },
                bucket,
            )
            .expect("configured")
        };
        let matrix = [
            (
                ReclaimAuthority::DerivedOnly,
                TableClass::Derived,
                "bucket 3",
            ),
            (
                ReclaimAuthority::Telemetry,
                TableClass::Telemetry,
                "bucket 4",
            ),
            (
                ReclaimAuthority::RawAudit(horizon(ProtectedRetentionBucket::RawAudit)),
                TableClass::RawAudit,
                "bucket 2",
            ),
            (
                ReclaimAuthority::CanonicalHistory(horizon(
                    ProtectedRetentionBucket::CanonicalHistory,
                )),
                TableClass::CanonicalHistory,
                "bucket 1",
            ),
        ];
        for (authority, own, label) in matrix {
            for class in TableClass::ALL {
                let expected = class == own;
                assert_eq!(
                    authority.authorizes(class),
                    expected,
                    "{authority:?} ({label}) vs {class:?}: a token authorizes its own bucket and \
                     nothing else"
                );
            }
        }
    }

    #[test]
    fn no_authority_variant_can_unlock_a_never_delete_table() {
        let mut authorities = full_authority();
        authorities.push(ReclaimAuthority::Telemetry);
        for authority in authorities {
            assert!(
                !authority.authorizes(TableClass::NeverDelete),
                "{authority:?} must not authorize never-delete"
            );
        }
    }

    /// **G-BOUNDPRED.** Fails for: a delete whose predicate does not bind the
    /// claimed unit.
    /// Denomination: per-table, per-required-column.
    ///
    /// This is the last independent check before a `DELETE` reaches
    /// ClickHouse. Class and token prove the emitter may *name* `events`;
    /// nothing before this constrained *which rows leave*, and the INV-2
    /// corpus itself built every statement with `"1"` — a full-table delete
    /// that `no_emitted_statement_writes_a_control_relation` accepted.
    ///
    /// MUTATION (executed 2026-07-27): delete the `missing` check from
    /// `emit_delete_statement` => FAILS on the `"1"` case for all 13 tables.
    /// **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-27): make `required_columns` return `&[]`
    /// for every shape => FAILS the same way. **Same direction, other end of
    /// the mechanism.**
    ///
    /// MUTATION (executed 2026-07-27): make the check `any` rather than
    /// `all` (accept a predicate naming *one* key column) => FAILS on the
    /// partial-tuple case below, where a generation delete names
    /// `source_generation` but not the file it belongs to and would sweep
    /// every host's generation 3. **Width: dropping one column from the
    /// requirement breaks a named case.**
    ///
    /// The upper bound is `every_emittable_statement`, which builds a
    /// shape-correct predicate for all 13 tables and must keep emitting.
    #[test]
    fn the_emitter_refuses_a_predicate_that_does_not_bind_the_unit() {
        let authorities = full_authority();
        for scope in ReclaimScope::ALL {
            for reclaim_table in scope.tables() {
                let refusal = emit_delete_statement("moraine", *reclaim_table, "1", &authorities)
                    .expect_err("a constant-true predicate must never emit");
                let EmitRefusal::UnboundPredicate {
                    ref table,
                    ref missing,
                    ..
                } = refusal
                else {
                    panic!("{refusal:?}");
                };
                assert_eq!(table, reclaim_table.name());
                assert_eq!(
                    *missing,
                    reclaim_table.predicate().required_columns().to_vec(),
                    "`WHERE 1` names none of `{}`'s key columns",
                    reclaim_table.name()
                );
                assert!(refusal.to_string().contains(reclaim_table.name()));
            }
        }

        // An empty `IN ()` list — the shape a WI-05 executor produces on an
        // empty chunk — still names its columns and is therefore emittable;
        // it deletes nothing, which is correct. The refusal is about a
        // predicate that names no column at all.
        emit_delete_statement(
            "moraine",
            ReclaimTable::McpOpenEvents,
            "(session_id, candidate_generation) IN ()",
            &authorities,
        )
        .expect("an empty key list is bound, just empty");

        // A partial tuple is NOT enough: `source_generation = 3` alone spans
        // every host, name, and file that ever had a generation 3.
        let refusal = emit_delete_statement(
            "moraine",
            ReclaimTable::Events,
            "source_generation = 3",
            &authorities,
        )
        .expect_err("a partial key tuple must not emit");
        assert!(
            matches!(
                refusal,
                EmitRefusal::UnboundPredicate { ref missing, .. }
                    if missing == &vec!["source_host", "source_name", "source_file"]
            ),
            "{refusal:?}"
        );

        // The shapes' column sets are the documented ones. `search_postings`
        // has no `event_uid` column at all, so `DocumentJoin` is keyed on
        // `doc_id`; asserting that here keeps the hazard visible next to the
        // guard that enforces it.
        assert_eq!(
            ReclaimPredicate::DocumentJoin.required_columns(),
            &["doc_id"]
        );
        assert_eq!(ReclaimPredicate::UidSet.required_columns(), &["event_uid"]);
        assert_eq!(
            ReclaimPredicate::SessionGeneration.required_columns(),
            &["session_id", "candidate_generation"]
        );
        assert_eq!(
            ReclaimPredicate::Generation.required_columns(),
            &[
                "source_host",
                "source_name",
                "source_file",
                "source_generation"
            ]
        );
    }

    #[test]
    fn emitted_deletes_are_lightweight_and_unsynchronized() {
        for statement in every_emittable_statement() {
            assert!(
                !statement.contains("ALTER TABLE"),
                "cleanup must never use ALTER … DELETE: {statement}"
            );
            assert!(
                !statement.contains("mutations_sync"),
                "cleanup must never set mutations_sync: {statement}"
            );
            assert!(
                !statement.contains("OPTIMIZE"),
                "production never OPTIMIZEs: {statement}"
            );
        }
    }

    /// **G-NOGEN.** Fails for: `tool_io` / `event_links` given a generation
    /// predicate.
    /// Denomination: match arms.
    ///
    /// The mapping is an exhaustive `match` over a closed enum, so adding a
    /// table without deciding its predicate shape is a compile error. This
    /// test pins the two arms the hazard is about, plus the two that are
    /// dangerous for a different reason.
    ///
    /// MUTATION (executed 2026-07-27): change the `ToolIo | EventLinks` arm to
    /// `ReclaimPredicate::Generation` => this test FAILS on both. Bounds the
    /// direction *a table with no generation column gains a generation
    /// predicate*; the `Generation` assertions below bound the opposite
    /// direction, so flipping everything to `UidSet` does not pass either.
    #[test]
    fn tables_without_a_generation_column_carry_a_uid_set_predicate() {
        assert_eq!(
            ReclaimTable::ToolIo.predicate(),
            ReclaimPredicate::UidSet,
            "tool_io has no source_file/source_generation column"
        );
        assert_eq!(
            ReclaimTable::EventLinks.predicate(),
            ReclaimPredicate::UidSet,
            "event_links has no source_file/source_generation column"
        );
        assert_eq!(
            ReclaimTable::SearchPostings.predicate(),
            ReclaimPredicate::DocumentJoin,
            "83.6% of postings carry back-filled type defaults for source_generation; the only \
             safe predicate joins through the document"
        );
        for table in [
            ReclaimTable::Events,
            ReclaimTable::RawEvents,
            ReclaimTable::IngestErrors,
            ReclaimTable::SearchDocuments,
            ReclaimTable::McpEventLocator,
            ReclaimTable::McpEventNavigation,
            ReclaimTable::McpSessionDirectory,
        ] {
            assert_eq!(
                table.predicate(),
                ReclaimPredicate::Generation,
                "{} is directly generation-keyed",
                table.name()
            );
        }
        for table in [ReclaimTable::McpOpenEvents, ReclaimTable::McpOpenTurns] {
            assert_eq!(table.predicate(), ReclaimPredicate::SessionGeneration);
        }
        // The header table is `(session_id, candidate_publication_id, slot,
        // generation, …)` — it has no `candidate_generation` column at all
        // (sql/033:16-60, and verified against the deployed schema
        // 2026-07-28). Folding it in with its own children made every legal
        // header delete unemittable, which is hazard H3 one table over.
        assert_eq!(
            ReclaimTable::McpOpenPublicationHeaders.predicate(),
            ReclaimPredicate::SessionHeaderGeneration,
            "mcp_open_publication_headers is keyed on `generation`, not `candidate_generation`"
        );
    }

    /// **G-COLBOUND.** Fails for: the emitter's third check accepting a
    /// predicate that names a *different* column whose text happens to contain
    /// the required one.
    /// Denomination: the emitted statement, per table, both directions.
    ///
    /// MUTATION (executed 2026-07-28): replace `predicate_names_column` with
    /// `predicate_sql.contains(column)` => FAILS on the header case: the child
    /// predicate `(session_id, candidate_generation) IN …` satisfies a
    /// requirement for `generation` by substring, and the emitter reports a
    /// header delete as bound while it names no column the header table has.
    /// **Lower bound, and it is the check that was actually broken.**
    ///
    /// MUTATION (executed 2026-07-28): make `predicate_names_column` return
    /// `true` unconditionally => FAILS on the `WHERE 1` case below. **Upper
    /// bound: a check that accepts everything is not a passing "fix".**
    ///
    /// MUTATION (executed 2026-07-28): make `predicate_names_column` return
    /// `false` unconditionally => FAILS on the positive case. **Width.**
    #[test]
    fn the_emitter_tells_generation_from_candidate_generation() {
        let child_predicate = "(session_id, candidate_generation) IN (('s-1', toUInt64(9)))";
        let header_predicate = "(session_id, generation) IN (('s-1', toUInt64(9)))";

        let refusal = emit_delete_statement(
            "moraine",
            ReclaimTable::McpOpenPublicationHeaders,
            child_predicate,
            &derived_only(),
        )
        .expect_err("a header delete carrying the child predicate names no column of the table");
        match refusal {
            EmitRefusal::UnboundPredicate { missing, .. } => {
                assert_eq!(missing, vec!["generation"]);
            }
            other => panic!("expected an unbound-predicate refusal, got {other:?}"),
        }

        emit_delete_statement(
            "moraine",
            ReclaimTable::McpOpenPublicationHeaders,
            header_predicate,
            &derived_only(),
        )
        .expect("the header's own predicate is emittable");

        // …and the converse: `generation` alone does not satisfy the child
        // tables' `candidate_generation`.
        let refusal = emit_delete_statement(
            "moraine",
            ReclaimTable::McpOpenEvents,
            header_predicate,
            &derived_only(),
        )
        .expect_err("a child delete carrying the header predicate is not bound");
        match refusal {
            EmitRefusal::UnboundPredicate { missing, .. } => {
                assert_eq!(missing, vec!["candidate_generation"]);
            }
            other => panic!("expected an unbound-predicate refusal, got {other:?}"),
        }

        assert!(
            emit_delete_statement("moraine", ReclaimTable::McpOpenEvents, "1", &derived_only())
                .is_err(),
            "`WHERE 1` names no key column and must stay refused"
        );
        assert!(predicate_names_column(
            "(session_id, generation) IN (())",
            "generation"
        ));
        assert!(!predicate_names_column(
            "(session_id, candidate_generation) IN (())",
            "generation"
        ));
        assert!(predicate_names_column(
            "(session_id, candidate_generation) IN (())",
            "candidate_generation"
        ));
    }

    #[test]
    fn every_reclaim_table_is_classified_and_never_never_delete() {
        for scope in ReclaimScope::ALL {
            for table in scope.tables() {
                let class = table
                    .class()
                    .unwrap_or_else(|| panic!("`{}` must be classified", table.name()));
                assert_ne!(
                    class,
                    TableClass::NeverDelete,
                    "`{}` must never appear in a reclaim scope",
                    table.name()
                );
            }
        }
    }

    /// **G-DEFAULT** (the planner half). Fails for: default config
    /// authorizing canonical deletion.
    /// Denomination: exact table-name set.
    ///
    /// MUTATION (executed 2026-07-27): make `ReclaimAuthority::for_scope`
    /// return `Ok(vec![Self::DerivedOnly])` for `CanonicalGeneration` — the
    /// planner-side check S3 relies on — => the reachable set gains `events`,
    /// `raw_events`, `ingest_errors`, `search_documents`, `search_postings`,
    /// `tool_io`, `event_links` and this test FAILS on the exact-set
    /// assertion. Bounds the direction *the default reachable set grows*; the
    /// non-empty assertion bounds the opposite direction, so emptying the set
    /// is not a passing "fix".
    ///
    /// A companion mutation keeps the two answers from drifting apart:
    /// flipping only `is_default_on` to `true` for `CanonicalGeneration`
    /// (executed 2026-07-27) leaves THIS test green but FAILS
    /// `default_on_scopes_agree_with_the_authority_check`, so the advertised
    /// default and the enforced default cannot disagree silently.
    #[test]
    fn the_default_reachable_table_set_excludes_all_user_history() {
        let retention = RetentionConfig::default();
        let mut reachable: Vec<&str> = ReclaimScope::ALL
            .into_iter()
            .filter(|scope| ReclaimAuthority::for_scope(*scope, &retention).is_ok())
            .flat_map(|scope| scope.tables().iter().map(|table| table.name()))
            .collect();
        reachable.sort_unstable();
        reachable.dedup();

        assert_eq!(
            reachable,
            vec![
                "mcp_event_locator",
                "mcp_event_navigation",
                "mcp_open_events",
                "mcp_open_publication_headers",
                "mcp_open_turns",
                "mcp_session_directory",
            ],
            "the stock-config reachable table set"
        );
        assert!(!reachable.is_empty());
        for forbidden in ["events", "raw_events", "ingest_errors", "search_documents"] {
            assert!(
                !reachable.contains(&forbidden),
                "`{forbidden}` must be unreachable under stock configuration"
            );
        }
        for name in &reachable {
            assert_eq!(
                classify(name),
                Some(TableClass::Derived),
                "every default-reachable table must be bucket 3: `{name}`"
            );
        }
        // And the scope that would reach user history refuses, naming the key.
        let missing = ReclaimAuthority::for_scope(ReclaimScope::CanonicalGeneration, &retention)
            .expect_err("canonical scope must refuse under stock configuration");
        assert_eq!(
            missing.config_key,
            "retention.canonical_history_horizon_days"
        );
        assert!(missing.to_string().contains("moraine export events"));
    }

    /// **G-TOKENS.** Fails for: a scope handed an authority token it does not
    /// need.
    /// Denomination: the exact token vector, per scope, both directions.
    ///
    /// `the_default_reachable_table_set_excludes_all_user_history` pins the
    /// reachable **tables**; nothing pinned the reachable **tokens**. Adding
    /// `Self::Telemetry` to the `McpOpenOrphan | ReadIndexGeneration` arm was
    /// GREEN across the whole suite (executed 2026-07-27), because no scope's
    /// `tables()` currently names a bucket-4 relation — i.e. the emitter was
    /// safe only by virtue of the planner's table list, which is exactly the
    /// planner-side reasoning §4 S3 says the emitter must not lean on. This is
    /// set equality on the vector itself, in the same denomination as the
    /// `NeverDelete` roster.
    ///
    /// MUTATION (executed 2026-07-27): add `Self::Telemetry` to the default
    /// arm => FAILS here, and nowhere else.
    ///
    /// MUTATION (executed 2026-07-27): drop `Self::DerivedOnly` from
    /// `CanonicalGeneration`'s vector => FAILS here **and** in
    /// `emit_delete_statement`'s refusal tests, because that scope's table
    /// list opens with five derived relations. **Lower bound.**
    #[test]
    fn for_scope_hands_out_exactly_the_tokens_each_scope_needs() {
        use std::collections::BTreeSet;

        let stock = RetentionConfig::default();
        for scope in [
            ReclaimScope::McpOpenOrphan,
            ReclaimScope::ReadIndexGeneration,
        ] {
            assert_eq!(
                ReclaimAuthority::for_scope(scope, &stock).expect("default-on scope"),
                vec![ReclaimAuthority::DerivedOnly],
                "`{scope}` must carry the derived token and nothing else"
            );
        }

        let configured = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        };
        let raw = RetentionHorizon::from_config(&configured, ProtectedRetentionBucket::RawAudit)
            .expect("raw horizon");
        let canonical =
            RetentionHorizon::from_config(&configured, ProtectedRetentionBucket::CanonicalHistory)
                .expect("canonical horizon");
        assert_eq!(
            ReclaimAuthority::for_scope(ReclaimScope::CanonicalGeneration, &configured)
                .expect("fully configured canonical scope"),
            vec![
                ReclaimAuthority::DerivedOnly,
                ReclaimAuthority::RawAudit(raw),
                ReclaimAuthority::CanonicalHistory(canonical),
            ],
            "the canonical scope spans buckets 1, 2 and 3 and must carry exactly those three"
        );

        // Width, stated as the invariant rather than as a list: no scope may
        // hold a token no table of its own needs.
        for scope in ReclaimScope::ALL {
            let Ok(tokens) = ReclaimAuthority::for_scope(scope, &configured) else {
                continue;
            };
            let needed: BTreeSet<TableClass> = scope
                .tables()
                .iter()
                .filter_map(|table| table.class())
                .collect();
            for token in tokens {
                assert!(
                    needed.iter().any(|class| token.authorizes(*class)),
                    "`{scope}` carries `{token:?}`, which authorizes no class in its own table \
                     list {needed:?} — a token nobody needs is a token nobody notices widening"
                );
            }
        }
    }

    #[test]
    fn default_on_scopes_agree_with_the_authority_check() {
        let retention = RetentionConfig::default();
        for scope in ReclaimScope::ALL {
            assert_eq!(
                scope.is_default_on(),
                ReclaimAuthority::for_scope(scope, &retention).is_ok(),
                "`{scope}` disagrees between is_default_on and for_scope"
            );
        }
    }

    #[test]
    fn a_partially_configured_canonical_scope_is_refused_naming_the_missing_key() {
        // Canonical horizon set, raw/audit absent: the scope spans both
        // buckets, so half-authority must refuse rather than reclaim half.
        let retention = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            ..RetentionConfig::default()
        };
        let missing = ReclaimAuthority::for_scope(ReclaimScope::CanonicalGeneration, &retention)
            .expect_err("half-configured retention must refuse");
        assert_eq!(missing.config_key, "retention.raw_audit_horizon_days");
    }

    // ---- ledger ---------------------------------------------------------

    /// **G-LEDGER** (the pure half, and *only* the pure half).
    ///
    /// This asserts enum predicates and SQL substrings. It does **not** prove
    /// a re-drive: [`ClickHouseClient::reclaim_run`] never claims, so there is
    /// no claim for this test to interrupt, and `ledger_redrive_sql` has no
    /// production caller. Read it as "the phase machine and the re-drive query
    /// say the right thing", not as coverage of §3.2's driver, which is
    /// descoped to WI-05 (see the module docs and [`LEDGER_DRIVER_WIRED`]).
    /// The live half — SIGKILL between the child and parent deletes — is the
    /// `reclaim-restart` gate, and it arrives with the first executor.
    #[test]
    fn the_phase_machine_redrives_exactly_the_unsettled_phases() {
        assert!(ReclaimPhase::Claimed.needs_redrive());
        assert!(ReclaimPhase::Deleting.needs_redrive());
        assert!(!ReclaimPhase::Done.needs_redrive());
        assert!(
            !ReclaimPhase::Abandoned.needs_redrive(),
            "an abandoned unit must never be silently resumed"
        );
        for phase in [
            ReclaimPhase::Claimed,
            ReclaimPhase::Deleting,
            ReclaimPhase::Done,
            ReclaimPhase::Abandoned,
        ] {
            assert_eq!(ReclaimPhase::parse(phase.as_str()), Some(phase));
        }
        assert_eq!(ReclaimPhase::parse("deleted"), None);

        let redrive = ledger_redrive_sql("moraine", ReclaimScope::McpOpenOrphan, 64);
        assert!(redrive.contains("phase IN ('claimed', 'deleting')"));
        assert!(
            !redrive.contains("'done'") && !redrive.contains("'abandoned'"),
            "a settled unit must never be re-driven"
        );
        assert!(redrive.contains("ORDER BY claimed_at ASC"));
    }

    #[test]
    fn ledger_writes_are_inserts_that_collapse_by_revision() {
        let claimed = ledger_claim_statement("moraine", &unit(ReclaimScope::McpOpenOrphan));
        assert!(claimed.starts_with("INSERT INTO `moraine`.storage_reclaim_ledger"));
        assert!(claimed.contains("generateSnowflakeID()"));
        assert!(claimed.contains("'claimed'"));
        // Literals are escaped: a source file or reclaim id containing a quote
        // must not terminate the statement.
        assert!(claimed.contains("'/tmp/a\\'b.jsonl'"));
        assert!(claimed.contains("'7\\'2'"));

        let deleting = ledger_advance_statement(
            "moraine",
            &unit(ReclaimScope::McpOpenOrphan),
            ReclaimPhase::Deleting,
        );
        assert!(deleting.starts_with("INSERT INTO `moraine`.storage_reclaim_ledger"));
        assert!(deleting.contains("'deleting'"));
        assert!(
            !deleting.contains("ALTER") && !deleting.contains("DELETE FROM"),
            "advancing a phase must never mutate the ledger in place"
        );
    }

    #[test]
    fn the_pending_mutation_probe_names_only_the_scopes_own_tables() {
        for scope in ReclaimScope::ALL {
            let sql = pending_mutations_sql("moraine", scope);
            assert!(sql.contains("FROM system.mutations"));
            assert!(sql.contains("is_done = 0"));
            for table in scope.tables() {
                assert!(
                    sql.contains(&format!("'{}'", table.name())),
                    "`{scope}` probe must name `{}`",
                    table.name()
                );
            }
        }
    }

    // ---- ordering -------------------------------------------------------

    #[test]
    fn canonical_is_deleted_last_and_children_precede_their_parents() {
        let canonical: Vec<&str> = ReclaimScope::CanonicalGeneration
            .tables()
            .iter()
            .map(|table| table.name())
            .collect();
        assert_eq!(
            canonical,
            vec![
                "search_postings",
                "search_documents",
                "tool_io",
                "event_links",
                "ingest_errors",
                "raw_events",
                "events",
            ],
            "a crash must leave derived data missing over intact canonical data, never the inverse"
        );

        // The orphan scope deletes children before the header, the inverse of
        // the existing reclaimer. That inversion is the stranded-child fix.
        let orphan: Vec<&str> = ReclaimScope::McpOpenOrphan
            .tables()
            .iter()
            .map(|table| table.name())
            .collect();
        assert_eq!(
            orphan,
            vec![
                "mcp_open_events",
                "mcp_open_turns",
                "mcp_open_publication_headers",
            ]
        );
        assert!(
            orphan.iter().position(|name| *name == "mcp_open_events")
                < orphan
                    .iter()
                    .position(|name| *name == "mcp_open_publication_headers"),
            "children first, parent last"
        );
    }

    #[test]
    fn the_unit_statement_cap_covers_the_widest_scope() {
        let widest = ReclaimScope::ALL
            .into_iter()
            .map(|scope| scope.tables().len() as u32)
            .max()
            .expect("scopes exist");
        assert_eq!(
            widest, UNIT_STMT_MAX_TABLES,
            "UNIT_STMT_MAX_TABLES must track the widest scope, or a legitimate unit could exceed \
             its own statement cap"
        );
        assert_eq!(UNIT_STATEMENT_CAP, 1 + 1 + 1 + 7 + 2);
    }

    #[test]
    fn the_two_reclaim_paths_share_one_chunk_size() {
        // The doc claims "the two paths have one shape". It is a definition,
        // not a copy: `RECLAIM_DELETE_CHUNK` here IS the projection
        // reclaimer's constant, so tuning one tunes both.
        assert_eq!(
            RECLAIM_DELETE_CHUNK,
            crate::mcp_open_projection::RECLAIM_DELETE_CHUNK,
            "the #603 driver and the existing mcp_open reclaimer must not drift to two chunk sizes"
        );
        assert_eq!(RECLAIM_DELETE_CHUNK, 1_000);
    }

    // ---- executors ------------------------------------------------------

    /// Exactly the two `mcp_open` scopes and the read-index scope execute in
    /// this build. Names only what it checks: it deliberately does **not**
    /// call `reclaim_run`, so it is not the guard for anything inside that
    /// function — see the `reclaim_run_*` tests below, which do.
    ///
    /// MUTATION (executed 2026-07-31): register `CanonicalGeneration` => FAILS
    /// here on the exact-set assertion. WI-09 has no probe in this build, so a
    /// registration would claim units nothing can describe. **Upper bound.**
    ///
    /// MUTATION (executed 2026-07-31): unregister `ReadIndexGeneration`
    /// (restore its `None` arm) => FAILS here on the exact-set assertion.
    /// **Lower bound: WI-07's executor is a deliberate entry, and losing it in
    /// a merge must not be silent.**
    #[test]
    fn this_build_registers_exactly_the_three_bucket_three_scopes() {
        assert_eq!(
            registered_executors(),
            vec![
                ReclaimScope::McpOpenOrphan,
                ReclaimScope::McpOpenRetiredLineage,
                ReclaimScope::ReadIndexGeneration
            ]
        );
        assert!(
            executor_for(ReclaimScope::CanonicalGeneration).is_none(),
            "`canonical_generation` must not execute before WI-09"
        );
    }

    /// The grain of every scope's unit, pinned per scope — not derived from
    /// the subject. The grain drives the ledger identity and the claim-time
    /// key validation, so a one-token change here re-keys a scope's ledger.
    ///
    /// MUTATION (executed 2026-07-31): map `ReadIndexGeneration` to
    /// `SessionCandidateGeneration` => FAILS here and at
    /// `a_candidate_row_must_carry_its_scopes_unit_key`. **Lower bound.**
    #[test]
    fn each_scopes_unit_grain_is_pinned() {
        for (scope, grain) in [
            (
                ReclaimScope::McpOpenOrphan,
                ReclaimUnitGrain::SessionCandidateGeneration,
            ),
            (
                ReclaimScope::McpOpenRetiredLineage,
                ReclaimUnitGrain::SessionCandidateGeneration,
            ),
            (
                ReclaimScope::ReadIndexGeneration,
                ReclaimUnitGrain::SourceGeneration,
            ),
            (
                ReclaimScope::CanonicalGeneration,
                ReclaimUnitGrain::SourceGeneration,
            ),
        ] {
            assert_eq!(scope.unit_grain(), grain, "`{scope}`");
        }
    }

    /// **G-CANDKEY.** Fails for: a candidate row claiming a unit without its
    /// scope's own key — the all-default row a mis-shaped probe response
    /// deserializes into, whose deletes would bind `('', 0)`.
    /// Denomination: the returned error, and the exact `reclaim_id` of the
    /// accepted rows (the session-grain format is WI-05's, unchanged — it is
    /// the ledger key, and reformatting it would stop a re-claim from
    /// collapsing onto its existing row).
    ///
    /// MUTATION (executed 2026-07-31): delete the empty-key `bail!` from the
    /// `SourceGeneration` arm of `into_unit` => FAILS here on the refusal
    /// assertion. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-31): build the session-grain `reclaim_id`
    /// with the source-grain format => FAILS here on the id pin. **Width: the
    /// grain decides the identity.**
    #[test]
    fn a_candidate_row_must_carry_its_scopes_unit_key() {
        let session_row = ReclaimCandidateRow {
            session_id: "s-1".to_string(),
            candidate_generation: 99,
            event_rows: 2,
            turn_rows: 1,
            header_rows: 1,
            ..empty_candidate_row()
        };
        let unit = session_row
            .clone()
            .into_unit(ReclaimScope::McpOpenOrphan)
            .expect("a session-keyed row claims under a session-grained scope");
        assert_eq!(unit.reclaim_id, "mcp_open_orphan:s-1:99");
        assert_eq!(unit.estimated_rows, 4);

        let generation_row = ReclaimCandidateRow {
            source_host: "h".to_string(),
            source_name: "codex".to_string(),
            source_file: "/a.jsonl".to_string(),
            source_generation: 5,
            navigation_rows: 3,
            locator_rows: 3,
            directory_rows: 1,
            ..empty_candidate_row()
        };
        let unit = generation_row
            .clone()
            .into_unit(ReclaimScope::ReadIndexGeneration)
            .expect("a generation-keyed row claims under a generation-grained scope");
        assert_eq!(unit.reclaim_id, "read_index_generation:h:codex:/a.jsonl:5");
        assert_eq!(unit.estimated_rows, 7);

        // Cross-grain rows are refused, in both directions, naming the key.
        let refused = session_row
            .into_unit(ReclaimScope::ReadIndexGeneration)
            .expect_err("a session-keyed row must not claim a generation-grained unit");
        assert!(
            refused.to_string().contains("source_generation"),
            "{refused}"
        );
        let refused = generation_row
            .into_unit(ReclaimScope::McpOpenOrphan)
            .expect_err("a generation-keyed row must not claim a session-grained unit");
        assert!(
            refused.to_string().contains("candidate_generation"),
            "{refused}"
        );
    }

    fn empty_candidate_row() -> ReclaimCandidateRow {
        ReclaimCandidateRow {
            session_id: String::new(),
            candidate_generation: 0,
            source_host: String::new(),
            source_name: String::new(),
            source_file: String::new(),
            source_generation: 0,
            event_rows: 0,
            turn_rows: 0,
            header_rows: 0,
            navigation_rows: 0,
            locator_rows: 0,
            directory_rows: 0,
        }
    }

    /// **D1's ordering, as a gate rather than a promise.** An executor
    /// registered onto an unimplemented §3.2 driver deletes rows that nothing
    /// durable records: the crash the ledger exists to survive would strand
    /// exactly the children it exists to keep reachable.
    ///
    /// The gate failed in both directions while the driver was unwritten. Now
    /// that it is written, one direction — "an executor with no driver" — is
    /// still the assertion, and the other has to become something a flipped
    /// constant cannot satisfy on its own. So this asserts the *behaviours*
    /// `LEDGER_DRIVER_WIRED` claims, by name, rather than the constant twice:
    /// a build that sets the constant without a driver fails
    /// `an_interrupted_claim_is_completed_by_the_next_runs_redrive` and
    /// `a_settled_run_claims_before_it_deletes_and_deletes_children_first`,
    /// which drive `reclaim_run` against a stateful server.
    ///
    /// MUTATION (executed 2026-07-28): set `LEDGER_DRIVER_WIRED = false` while
    /// leaving both executors registered => FAILS here. **Lower bound, the
    /// original direction.**
    ///
    /// MUTATION (executed 2026-07-28): delete the
    /// `self.reclaim_redrive(scope).await?` line from `reclaim_run` while
    /// leaving the constant `true` => this test still passes, and
    /// `an_interrupted_claim_is_completed_by_the_next_runs_redrive` FAILS.
    /// **Recorded, not repaired: this gate is about registration order; the
    /// driver's own behaviour is guarded where the driver runs.**
    #[test]
    fn no_executor_may_be_registered_before_the_ledger_driver_is_wired() {
        assert!(
            LEDGER_DRIVER_WIRED || registered_executors().is_empty(),
            "an executor is registered but `reclaim_run` still never claims, advances, or settles \
             a ledger unit. Wire the §3.2 driver and set LEDGER_DRIVER_WIRED, or unregister the \
             executor: {:?}",
            registered_executors()
        );
        assert!(
            !registered_executors().is_empty(),
            "WI-05 registers the first executors; an empty registry here means the constant is \
             the only thing that changed"
        );
        for scope in registered_executors() {
            let executor = executor_for(scope).expect("registered");
            assert!(
                !(executor.probe)("moraine", 86_400, 8).is_empty(),
                "`{scope}` is registered with no probe"
            );
            assert!(
                !(executor.predicates)(&unit(scope)).is_empty(),
                "`{scope}` is registered with no delete predicates"
            );
        }
    }

    /// A client pointed at a port nothing listens on. Any test that reaches
    /// the network fails loudly instead of passing on a stale assumption.
    fn offline_client() -> ClickHouseClient {
        ClickHouseClient::new(moraine_config::ClickHouseConfig {
            url: "http://127.0.0.1:1".to_string(),
            database: "moraine".to_string(),
            timeout_seconds: 1.0,
            ..Default::default()
        })
        .expect("client construction performs no I/O")
    }

    /// **G-RUNAUTH.** Fails for: `reclaim_run` proceeding past a scope whose
    /// `[retention]` key is absent.
    /// Denomination: the returned error, and its named config key.
    ///
    /// `ReclaimAuthority::for_scope` inside `reclaim_run` is the S2
    /// enforcement point at the command boundary. Deleting it left the suite
    /// at 177/0, because `this_build_registers_exactly_the_three_bucket_three_scopes`
    /// — named `this_build_registers_no_executor` when that was written —
    /// never called `reclaim_run` at all; it asserted over the registry.
    ///
    /// MUTATION (executed 2026-07-27): delete the
    /// `ReclaimAuthority::for_scope(scope, retention)?;` line from
    /// `reclaim_run` => FAILS here: the call returns `Ok(NoExecutor)` instead
    /// of the missing-key error. **Lower bound.**
    #[tokio::test(flavor = "multi_thread")]
    async fn reclaim_run_refuses_an_unconfigured_canonical_scope_before_anything_else() {
        let client = offline_client();
        let error = client
            .reclaim_run(
                &RetentionConfig::default(),
                ReclaimScope::CanonicalGeneration,
                ReclaimTrigger::Operator,
                &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
                &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
            )
            .await
            .expect_err("an unconfigured bucket-1 scope must not run");
        let rendered = error.to_string();
        assert!(
            rendered.contains("retention.canonical_history_horizon_days"),
            "{rendered}"
        );
        assert!(rendered.contains("moraine export events"), "{rendered}");
    }

    /// **G-RUNEXEC.** Fails for: `reclaim_run` proceeding past the executor
    /// registry.
    /// Denomination: the returned outcome, and the absence of any I/O.
    ///
    /// MUTATION (executed 2026-07-28): delete the
    /// `if executor_for(scope).is_none() { … }` block from `reclaim_run` =>
    /// FAILS here: control reaches `reclaim_pending_mutations`, the offline
    /// client cannot connect, and the call returns `Err` instead of
    /// `NoExecutor`. **Lower bound, and it is the bound that matters: with an
    /// authorized scope, that block is all that stands between a run and the
    /// server.**
    ///
    /// The registered scopes are deliberately **not** in this loop. They reach
    /// the server, by design, and asserting that they refuse locally would be
    /// asserting the opposite of what WI-05 ships; their behaviour is guarded
    /// by the two mock-server tests below.
    #[tokio::test(flavor = "multi_thread")]
    async fn reclaim_run_refuses_an_unregistered_scope_without_reaching_clickhouse() {
        let client = offline_client();
        // Fully authorized, so the authority check cannot be what refuses.
        let retention = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        };
        let unregistered: Vec<ReclaimScope> = ReclaimScope::ALL
            .into_iter()
            .filter(|scope| executor_for(*scope).is_none())
            .collect();
        assert!(
            !unregistered.is_empty(),
            "this test needs at least one unregistered scope to be about anything"
        );
        for scope in unregistered {
            let outcome = client
                .reclaim_run(
                    &retention,
                    scope,
                    ReclaimTrigger::Operator,
                    &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
                    &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("`{scope}` must refuse locally, not by failing to connect: {error:#}")
                });
            match outcome {
                ReclaimOutcome::NoExecutor {
                    scope: refused,
                    ref message,
                } => {
                    assert_eq!(refused, scope);
                    assert!(message.contains("Nothing was deleted"), "{message}");
                    assert!(message.contains(pending_work_item(scope)), "{message}");
                }
                other => panic!("`{scope}` produced {other:?}"),
            }
            assert!(!outcome.deleted_anything());
        }
    }

    // ---- denomination ---------------------------------------------------

    /// **G-DENOM** (the library half; the rendered-string half lives in
    /// `apps/moraine/src/render.rs`). Fails for: a byte number reported
    /// without its qualifier, or a surface promising partition-aligned
    /// deletion.
    /// Denomination: rendered string.
    ///
    /// MUTATION (executed 2026-07-27): change `reclaimed_bytes_note` to
    /// `"freed N bytes"` => this test FAILS on both the `merge-deferred`
    /// assertion and the forbidden-word scan.
    ///
    /// MUTATION (executed 2026-07-28): empty `FORBIDDEN_DENOMINATION_WORDS` to
    /// `&[]` => FAILS here on the pinned list. **This is the vacuity the pin
    /// exists for.** Both denomination guards — this one and
    /// `every_rendered_reclaim_line_carries_its_denomination` two crates away
    /// — build their expectation by iterating the list, so with the list empty
    /// each scan runs zero comparisons and both stay green while every
    /// forbidden promise becomes sayable. A test that derives its expectation
    /// from its subject cannot fail for the subject changing, so the list is
    /// pinned here, once, and the scans below may keep iterating it.
    #[test]
    fn byte_denominations_carry_their_qualifiers_and_promise_nothing() {
        assert_eq!(
            FORBIDDEN_DENOMINATION_WORDS,
            ["frees", "recovers", "partition"],
            "shrinking this list silently disarms every scan that iterates it"
        );

        let estimate = estimated_bytes_note();
        let reclaimed = reclaimed_bytes_note();
        assert!(estimate.contains(ESTIMATE_QUALIFIER));
        assert!(reclaimed.contains(MERGE_DEFERRED_QUALIFIER));
        // The old note said "reclaimed row counts are exact". They never
        // were: `Settled` sums the probe's claim-time estimate, a re-driven
        // unit contributes zero, and a lightweight DELETE reports no
        // affected-row count over HTTP at all.
        assert!(
            reclaimed.contains(ESTIMATE_QUALIFIER),
            "the reclaimed-row number is an estimate and must say so: {reclaimed}"
        );
        assert!(
            !reclaimed.contains("exact"),
            "nothing in this module computes an exact count of rows removed: {reclaimed}"
        );

        for text in [&estimate, &reclaimed] {
            let lowered = text.to_lowercase();
            for forbidden in FORBIDDEN_DENOMINATION_WORDS {
                assert!(
                    !lowered.contains(forbidden),
                    "`{forbidden}` promises something no table's layout supports: {text}"
                );
            }
        }

        // The scope descriptions are operator-facing too.
        for scope in ReclaimScope::ALL {
            let lowered = scope.describe().to_lowercase();
            for forbidden in FORBIDDEN_DENOMINATION_WORDS {
                assert!(
                    !lowered.contains(forbidden),
                    "`{scope}` description uses `{forbidden}`"
                );
            }
        }
    }

    #[test]
    fn scope_names_round_trip() {
        for scope in ReclaimScope::ALL {
            assert_eq!(ReclaimScope::parse(scope.as_str()), Some(scope));
        }
        assert_eq!(ReclaimScope::parse("everything"), None);
        assert_eq!(ReclaimScope::parse(""), None);
    }

    // ---- the §3.2 driver, against a stateful server ----------------------
    //
    // Everything below drives `reclaim_run` end to end against an in-process
    // ClickHouse stand-in that holds real rows and a real ledger and answers
    // the statements the driver actually sends. It is not the `reclaim-restart`
    // live gate — that one SIGKILLs the process between two deletes and needs a
    // sandbox, and **Docker is unavailable on this host**, so it is not run in
    // this PR and is not claimed. What these do provide is the property that
    // gate is for: a claim interrupted *mid-unit* is completed by the next
    // run's re-drive, with the interruption applied to a real statement stream
    // rather than to an enum.

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockLedgerRow {
        scope: String,
        session_id: String,
        candidate_generation: u64,
        phase: String,
    }

    #[derive(Debug)]
    struct ReclaimMockState {
        statements: Vec<String>,
        /// `(session_id, candidate_generation) -> (event_rows, turn_rows, header_rows)`.
        rows: BTreeMap<(String, u64), (u64, u64, u64)>,
        ledger: BTreeMap<String, MockLedgerRow>,
        /// Rows each `DELETE` actually removed, in statement order. A replayed
        /// delete appends a `0`, which is what "replay removes zero additional
        /// rows" is measured from.
        deleted: Vec<(String, u64)>,
        /// The server refuses the first statement containing this marker, once.
        fail_once_on: Option<String>,
        /// The server refuses **every** statement containing this marker. A
        /// poison unit, which is what `reclaim_redrive`'s isolation is for.
        fail_always_on: Option<String>,
        /// `(statement, sorted query params)` for every request, so a test can
        /// assert the settings a statement was sent *with* and not only its
        /// text.
        params: Vec<(String, BTreeMap<String, String>)>,
        /// What `system.disks` reports. Large enough to clear
        /// `RECLAIM_MIN_FREE_BYTES` unless a test lowers it.
        free_bytes: u64,
        /// Ledger rows the re-drive page should report as already aged.
        unsettled_seconds: u64,
        /// Extra raw `JSONEachRow` lines appended to the re-drive page's
        /// response, for rows the typed mock cannot express — a phase or scope
        /// this build cannot parse, which is what an operator downgrading past
        /// a work item leaves behind.
        redrive_extra_rows: Vec<String>,
        /// What the pending-mutation probe reports (hazard H9).
        pending_mutations: u64,
    }

    impl Default for ReclaimMockState {
        fn default() -> Self {
            Self {
                statements: Vec::new(),
                rows: BTreeMap::new(),
                ledger: BTreeMap::new(),
                deleted: Vec::new(),
                fail_once_on: None,
                fail_always_on: None,
                params: Vec::new(),
                free_bytes: 64 * 1024 * 1024 * 1024,
                unsettled_seconds: 0,
                redrive_extra_rows: Vec::new(),
                pending_mutations: 0,
            }
        }
    }

    #[derive(Clone, Default)]
    struct ReclaimMock(Arc<Mutex<ReclaimMockState>>);

    impl ReclaimMock {
        fn with_orphans(pairs: &[(&str, u64, u64, u64)]) -> Self {
            let mock = Self::default();
            {
                let mut state = mock.lock();
                for (session, generation, events, turns) in pairs {
                    state
                        .rows
                        .insert((session.to_string(), *generation), (*events, *turns, 0));
                }
            }
            mock
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, ReclaimMockState> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        /// Query params every request carried, paired with its statement.
        fn params(&self) -> Vec<(String, BTreeMap<String, String>)> {
            self.lock().params.clone()
        }

        fn statements(&self) -> Vec<String> {
            self.lock().statements.clone()
        }

        fn ledger(&self) -> BTreeMap<String, MockLedgerRow> {
            self.lock().ledger.clone()
        }

        fn rows(&self) -> BTreeMap<(String, u64), (u64, u64, u64)> {
            self.lock().rows.clone()
        }

        fn deleted(&self) -> Vec<(String, u64)> {
            self.lock().deleted.clone()
        }

        fn fail_once_on(&self, marker: &str) {
            self.lock().fail_once_on = Some(marker.to_string());
        }

        /// Index of the first recorded statement containing `needle`.
        fn first_index(&self, needle: &str) -> Option<usize> {
            self.statements()
                .iter()
                .position(|statement| statement.contains(needle))
        }

        /// Index of the first ledger **write** carrying `phase`.
        ///
        /// Not `first_index("'claimed'")`: the re-drive *probe* spells
        /// `phase IN ('claimed', 'deleting')`, so a substring search finds a
        /// read at index 1 and every ordering assertion below it becomes
        /// vacuous. It did, on the first run of this test.
        fn first_phase_write(&self, phase: ReclaimPhase) -> Option<usize> {
            let literal = format!("'{}'", phase.as_str());
            self.statements().iter().position(|statement| {
                statement.starts_with("INSERT INTO `moraine`.storage_reclaim_ledger")
                    && statement.contains(&literal)
            })
        }
    }

    /// Every single-quoted literal in `statement`, in order.
    fn quoted_literals(statement: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut chars = statement.char_indices().peekable();
        while let Some((_, ch)) = chars.next() {
            if ch != '\'' {
                continue;
            }
            let mut literal = String::new();
            while let Some((_, ch)) = chars.next() {
                if ch == '\\' {
                    if let Some((_, escaped)) = chars.next() {
                        literal.push(escaped);
                    }
                    continue;
                }
                if ch == '\'' {
                    break;
                }
                literal.push(ch);
            }
            out.push(literal);
        }
        out
    }

    /// The first `toUInt64(<digits>)` argument in `statement`.
    fn first_u64_arg(statement: &str) -> Option<u64> {
        let start = statement.find("toUInt64(")? + "toUInt64(".len();
        let rest = &statement[start..];
        let end = rest.find(')')?;
        rest[..end].trim().parse().ok()
    }

    /// Reads by URL `query` parameter, writes by body — the two transport
    /// profiles the client actually uses (`query_json_each_row` puts the
    /// statement in the query string; `mutation_request_text_with_params_and_timeout`
    /// puts it in the body with an empty `query`).
    async fn reclaim_mock_handler(
        axum::extract::State(mock): axum::extract::State<ReclaimMock>,
        axum::extract::Query(params): axum::extract::Query<BTreeMap<String, String>>,
        body: String,
    ) -> (axum::http::StatusCode, String) {
        let from_url = params.get("query").cloned().unwrap_or_default();
        let statement = if from_url.trim().is_empty() {
            body.trim().to_string()
        } else {
            from_url.trim().to_string()
        };
        {
            let mut state = mock.lock();
            state
                .params
                .push((statement.clone(), params.clone().into_iter().collect()));
            if state
                .fail_always_on
                .as_ref()
                .is_some_and(|marker| statement.contains(marker))
            {
                state.statements.push(statement.clone());
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "Code: 999. DB::Exception: simulated poison unit".to_string(),
                );
            }
            if state
                .fail_once_on
                .as_ref()
                .is_some_and(|marker| statement.contains(marker))
            {
                state.fail_once_on = None;
                state.statements.push(statement.clone());
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "Code: 999. DB::Exception: simulated interruption".to_string(),
                );
            }
            state.statements.push(statement.clone());
        }

        // --- writes ---
        if statement.starts_with("INSERT INTO `moraine`.storage_reclaim_ledger") {
            let literals = quoted_literals(&statement);
            let (Some(reclaim_id), Some(scope), Some(session_id), Some(phase)) = (
                literals.first(),
                literals.get(1),
                literals.get(5),
                literals.get(6),
            ) else {
                return (axum::http::StatusCode::BAD_REQUEST, String::new());
            };
            let candidate_generation = first_u64_arg(&statement).unwrap_or_default();
            mock.lock().ledger.insert(
                reclaim_id.clone(),
                MockLedgerRow {
                    scope: scope.clone(),
                    session_id: session_id.clone(),
                    candidate_generation,
                    phase: phase.clone(),
                },
            );
            return (axum::http::StatusCode::OK, String::new());
        }
        if let Some(table) = statement
            .strip_prefix("DELETE FROM `moraine`.")
            .and_then(|rest| rest.split('\n').next())
        {
            let literals = quoted_literals(&statement);
            let session_id = literals.first().cloned().unwrap_or_default();
            let candidate_generation = first_u64_arg(&statement).unwrap_or_default();
            let mut state = mock.lock();
            let removed = match state.rows.get_mut(&(session_id, candidate_generation)) {
                Some(counts) => {
                    let slot = match table {
                        "mcp_open_events" => &mut counts.0,
                        "mcp_open_turns" => &mut counts.1,
                        "mcp_open_publication_headers" => &mut counts.2,
                        _ => panic!("unexpected reclaim target `{table}`"),
                    };
                    std::mem::take(slot)
                }
                None => 0,
            };
            state.deleted.push((table.to_string(), removed));
            return (axum::http::StatusCode::OK, String::new());
        }

        // --- reads ---
        if statement.contains("FROM system.mutations") {
            let pending = mock.lock().pending_mutations;
            return (
                axum::http::StatusCode::OK,
                format!("{{\"pending\":{pending}}}\n"),
            );
        }
        if statement.contains("FROM system.disks") {
            let free_bytes = mock.lock().free_bytes;
            return (
                axum::http::StatusCode::OK,
                format!(
                    "{{\"free_bytes\":{free_bytes},\"total_bytes\":{}}}\n",
                    free_bytes * 2
                ),
            );
        }
        if statement.contains("FROM system.parts") {
            return (axum::http::StatusCode::OK, String::new());
        }
        if statement.contains("storage_reclaim_ledger FINAL") {
            if statement.contains("GROUP BY phase") {
                let mut counts: BTreeMap<String, u64> = BTreeMap::new();
                for row in mock.ledger().values() {
                    *counts.entry(row.phase.clone()).or_default() += 1;
                }
                let body = counts
                    .into_iter()
                    .map(|(phase, units)| format!("{{\"phase\":\"{phase}\",\"units\":{units}}}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                return (axum::http::StatusCode::OK, format!("{body}\n"));
            }
            let (unsettled_seconds, extra) = {
                let state = mock.lock();
                (state.unsettled_seconds, state.redrive_extra_rows.clone())
            };
            let body = mock
                .ledger()
                .into_iter()
                .filter(|(_, row)| row.phase == "claimed" || row.phase == "deleting")
                .filter(|(_, row)| statement.contains(&format!("scope = '{}'", row.scope)))
                .map(|(reclaim_id, row)| {
                    format!(
                        "{{\"reclaim_id\":\"{reclaim_id}\",\"scope\":\"{}\",\"source_host\":\"\",\
                         \"source_name\":\"\",\"source_file\":\"\",\"source_generation\":0,\
                         \"session_id\":\"{}\",\"candidate_generation\":{},\"phase\":\"{}\",\
                         \"estimated_rows\":0,\"estimated_bytes\":0,\"unsettled_seconds\":{}}}",
                        row.scope,
                        row.session_id,
                        row.candidate_generation,
                        row.phase,
                        unsettled_seconds
                    )
                })
                .chain(extra)
                .collect::<Vec<_>>()
                .join("\n");
            return (axum::http::StatusCode::OK, format!("{body}\n"));
        }
        // The orphan candidate probe: pairs with child rows and no header.
        if statement.contains("mcp_open_publication_headers\n  )") {
            let body = mock
                .rows()
                .into_iter()
                .filter(|(_, (events, turns, headers))| {
                    *headers == 0 && (*events > 0 || *turns > 0)
                })
                .map(|((session_id, generation), (events, turns, _))| {
                    format!(
                        "{{\"session_id\":\"{session_id}\",\"candidate_generation\":{generation},\
                         \"event_rows\":{events},\"turn_rows\":{turns},\"header_rows\":0}}"
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            return (axum::http::StatusCode::OK, format!("{body}\n"));
        }
        (axum::http::StatusCode::OK, String::new())
    }

    async fn spawn_reclaim_mock(mock: ReclaimMock) -> String {
        let app = axum::Router::new()
            .route(
                "/",
                axum::routing::post(reclaim_mock_handler).get(reclaim_mock_handler),
            )
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind reclaim mock listener");
        let addr = listener.local_addr().expect("reclaim mock addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn mock_client(url: String) -> ClickHouseClient {
        ClickHouseClient::new(moraine_config::ClickHouseConfig {
            url,
            database: "moraine".to_string(),
            username: "default".to_string(),
            password: String::new(),
            timeout_seconds: 5.0,
            request_compression: moraine_config::ClickHouseRequestCompression::None,
            async_insert: false,
            wait_for_async_insert: true,
            allow_newer_server: false,
        })
        .expect("mock client")
    }

    async fn run_orphan_reclaim(client: &ClickHouseClient) -> Result<ReclaimOutcome> {
        run_orphan_reclaim_as(client, ReclaimTrigger::Operator).await
    }

    async fn run_orphan_reclaim_as(
        client: &ClickHouseClient,
        trigger: ReclaimTrigger,
    ) -> Result<ReclaimOutcome> {
        crate::envelope::with_test_envelope(client.reclaim_run(
            &RetentionConfig::default(),
            ReclaimScope::McpOpenOrphan,
            trigger,
            &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
            &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
        ))
        .await
    }

    /// **G-RESTART (the in-process half).** Fails for: a run that does not
    /// complete a unit an earlier run left mid-flight.
    /// Denomination: the ledger phase of the interrupted unit, the per-table
    /// row counts it left behind, and the statement order of the recovery run.
    ///
    /// The interruption is applied to a **real claim**: the run below claims
    /// unit one, advances it to `deleting`, issues and completes the
    /// `mcp_open_events` delete, and is then refused by the server on the
    /// `mcp_open_turns` delete — exactly the window hazard H7 is about, and
    /// exactly the window that produced the reference host's 11.17M stranded
    /// child rows. There is no claim to interrupt in a build whose `run` never
    /// claims, which is why the WI-04 phase-machine test could not make this
    /// assertion and said so.
    ///
    /// MUTATION (executed 2026-07-28): delete `let redriven =
    /// self.reclaim_redrive(scope).await?;` from `reclaim_run` => FAILS: the
    /// interrupted unit stays at `deleting` forever and its turn and header
    /// rows are never removed. **Lower bound — this is the whole work item.**
    ///
    /// MUTATION (executed 2026-07-28): move the `reclaim_redrive` call to
    /// *after* the candidate probe and the claim loop => FAILS on the
    /// statement-order assertion. **Width: §3.2 says "before new units are
    /// planned", and a re-drive that runs afterwards re-claims a unit the
    /// ledger already names.**
    ///
    /// MUTATION (executed 2026-07-28): move the claim write to *after*
    /// `reclaim_execute_unit` => FAILS: the interrupted unit has no ledger row
    /// at all after run one, so the re-drive finds nothing and the assertion on
    /// `deleting` fails first. **Width: claim-before-delete is what makes the
    /// set re-derivable.**
    #[tokio::test(flavor = "multi_thread")]
    async fn an_interrupted_claim_is_completed_by_the_next_runs_redrive() {
        let mock = ReclaimMock::with_orphans(&[("s-1", 11, 40, 4), ("s-2", 22, 7, 1)]);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        mock.fail_once_on("DELETE FROM `moraine`.mcp_open_turns");

        let interrupted = run_orphan_reclaim(&client).await;
        assert!(
            interrupted.is_err(),
            "the interrupted run must surface the failure, not report success: {interrupted:?}"
        );

        // The claim is durable and names the unit that was in flight.
        let ledger = mock.ledger();
        assert_eq!(ledger.len(), 1, "only unit one was reached: {ledger:?}");
        let (reclaim_id, row) = ledger.iter().next().expect("one claimed unit");
        assert_eq!(reclaim_id, "mcp_open_orphan:s-1:11");
        assert_eq!(row.phase, "deleting", "a crash mid-unit leaves `deleting`");

        // Children first: the events rows are gone, the turns and the header
        // are not. This is precisely the state the old reclaimer could never
        // recover from, because it derived its set from the header it deleted
        // first.
        let rows = mock.rows();
        assert_eq!(rows[&("s-1".to_string(), 11)], (0, 4, 0));
        assert_eq!(
            rows[&("s-2".to_string(), 22)],
            (7, 1, 0),
            "unit two must not have been touched"
        );

        // Second run: the re-drive finishes unit one before planning anything.
        let statements_before = mock.statements().len();
        let outcome = run_orphan_reclaim(&client)
            .await
            .expect("the recovery run completes");

        let statements = mock.statements();
        let redrive = statements
            .iter()
            .skip(statements_before)
            .position(|statement| statement.contains("phase IN ('claimed', 'deleting')"))
            .expect("the recovery run reads the ledger");
        let probe = statements
            .iter()
            .skip(statements_before)
            .position(|statement| statement.contains("mcp_open_publication_headers\n  )"))
            .expect("the recovery run probes for new candidates");
        assert!(
            redrive < probe,
            "§3.2: unsettled units are re-driven BEFORE new ones are planned \
             (re-drive at {redrive}, probe at {probe})"
        );

        // Nothing is stranded: every pair is empty, and unit one settled.
        for ((session_id, generation), counts) in mock.rows() {
            assert_eq!(
                counts,
                (0, 0, 0),
                "`{session_id}`/{generation} still holds rows after the recovery run"
            );
        }
        let ledger = mock.ledger();
        assert_eq!(ledger.len(), 2, "{ledger:?}");
        for (reclaim_id, row) in &ledger {
            assert_eq!(row.phase, "done", "`{reclaim_id}` did not settle");
        }
        assert!(
            matches!(outcome, ReclaimOutcome::Settled { units: 2, .. }),
            "the re-driven unit and the new one both count: {outcome:?}"
        );

        // Replay is a no-op: the re-driven unit's `mcp_open_events` delete ran
        // a second time and removed nothing, because its predicate is over the
        // key set the ledger names rather than over rows it re-derives.
        let events_deletes: Vec<u64> = mock
            .deleted()
            .into_iter()
            .filter(|(table, _)| table == "mcp_open_events")
            .map(|(_, removed)| removed)
            .collect();
        assert_eq!(
            events_deletes,
            vec![40, 0, 7],
            "the replayed delete must remove zero additional rows"
        );
    }

    /// **G-CLAIMORDER.** Fails for: a unit deleted before it is claimed, or a
    /// header removed before its children.
    /// Denomination: the recorded statement sequence for one unit.
    ///
    /// MUTATION (executed 2026-07-28): reorder `ReclaimScope::tables` for
    /// `McpOpenOrphan` to put `McpOpenPublicationHeaders` first => FAILS on the
    /// children-before-header assertion.
    ///
    /// MUTATION (executed 2026-07-28): delete the
    /// `ledger_advance_statement(.., ReclaimPhase::Deleting)` write => FAILS on
    /// the phase-sequence assertion.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_settled_run_claims_before_it_deletes_and_deletes_children_first() {
        let mock = ReclaimMock::with_orphans(&[("s-1", 11, 3, 2)]);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);

        let outcome = run_orphan_reclaim(&client).await.expect("run");
        assert!(matches!(outcome, ReclaimOutcome::Settled { units: 1, .. }));

        let claim = mock
            .first_phase_write(ReclaimPhase::Claimed)
            .expect("the unit is claimed");
        let deleting = mock
            .first_phase_write(ReclaimPhase::Deleting)
            .expect("the unit advances to deleting");
        let events = mock
            .first_index("DELETE FROM `moraine`.mcp_open_events")
            .expect("the events delete runs");
        let turns = mock
            .first_index("DELETE FROM `moraine`.mcp_open_turns")
            .expect("the turns delete runs");
        let header = mock
            .first_index("DELETE FROM `moraine`.mcp_open_publication_headers")
            .expect("the header delete runs");
        let done = mock
            .first_phase_write(ReclaimPhase::Done)
            .expect("the unit settles");

        assert!(
            claim < deleting && deleting < events,
            "the claim must be durable before the first delete: claim {claim}, deleting \
             {deleting}, events {events}"
        );
        assert!(
            events < turns && turns < header,
            "children first, parent last: events {events}, turns {turns}, header {header}"
        );
        assert!(header < done, "settle last: header {header}, done {done}");

        // And the H9 probe ran before any of it.
        let mutations = mock
            .first_index("FROM system.mutations")
            .expect("the pending-mutation probe runs");
        assert!(mutations < claim, "a blocked scope must not claim");
    }

    /// A run over a scope with nothing to do reports `Idle`, not `Settled{0}`.
    /// Hazard H9's shape one level up: "nothing to reclaim" and "reclaimed
    /// nothing" must not render the same.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_empty_scope_is_idle_rather_than_a_zero_unit_settlement() {
        let mock = ReclaimMock::default();
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        let outcome = run_orphan_reclaim(&client).await.expect("run");
        assert!(
            matches!(outcome, ReclaimOutcome::Idle { .. }),
            "{outcome:?}"
        );
        assert!(mock.ledger().is_empty(), "an idle run claims nothing");
        assert!(
            mock.deleted().is_empty(),
            "an idle run issues no delete: {:?}",
            mock.deleted()
        );
    }

    /// **G-SIGNAL.** Fails for: a pending mutation being reported as "nothing
    /// to do" (hazard H9 — the existing gate returns `Ok(default)` with no log
    /// and no counter).
    /// Denomination: the typed outcome, and the absence of a claim.
    ///
    /// MUTATION (executed 2026-07-28): replace the `Blocked` return with
    /// `Ok(ReclaimOutcome::Idle { scope })` => FAILS here. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): widen the gate to `if pending > 1` =>
    /// FAILS on the `pending = 1` case below, where the run proceeds to claim
    /// and delete over tables that already have a mutation in flight.
    /// **Width: the boundary. The previous revision presented only
    /// `pending = 3`, so every off-by-one on this comparison was untested and
    /// the single-mutation case — the common one, since `H9` is about *a*
    /// stuck mutation — was the one left open.**
    ///
    /// MUTATION (executed 2026-07-28): narrow the gate to `if true` => FAILS
    /// on the `pending = 0` case, which must reach the probe rather than
    /// block. **Upper bound: a gate that blocks unconditionally reclaims
    /// nothing, ever.** (`pending >= 0` is the same defect written more
    /// naturally, but `pending` is a `u64`, so that spelling trips
    /// `unused_comparisons` and is a warning in the diff rather than a silent
    /// one.)
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pending_mutation_blocks_with_a_count_rather_than_reporting_idle() {
        // One is the boundary, and it is the realistic case: hazard H9 is one
        // mutation that never finishes.
        for pending in [1_u64, 3] {
            let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
            mock.lock().pending_mutations = pending;
            let client = mock_client(spawn_reclaim_mock(mock.clone()).await);

            let outcome = run_orphan_reclaim(&client).await.expect("run");
            assert_eq!(
                outcome,
                ReclaimOutcome::Blocked {
                    scope: ReclaimScope::McpOpenOrphan,
                    pending_mutations: pending,
                }
            );
            // A blocked run stops *before* it plans: the pending-mutation
            // probe is the only statement it issues, so there is no probe, no
            // ledger read, no claim, and no delete over tables something else
            // is already mutating.
            let issued = mock.statements();
            assert_eq!(
                issued.len(),
                1,
                "a blocked run issues nothing else: {issued:?}"
            );
            assert!(issued[0].contains("FROM system.mutations"), "{issued:?}");
            assert!(mock.ledger().is_empty() && mock.deleted().is_empty());
        }

        // And zero does not block, or the gate would be a permanent refusal.
        let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
        assert_eq!(mock.lock().pending_mutations, 0);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        let outcome = run_orphan_reclaim(&client).await.expect("run");
        assert!(
            matches!(outcome, ReclaimOutcome::Settled { .. }),
            "no pending mutation must not block: {outcome:?}"
        );
    }

    /// **G-BOUNDED (the envelope half).** Fails for: a run whose statements all
    /// land in one envelope, so a sweep exhausts a cap sized for a single unit.
    /// Denomination: a run of more units than one `UNIT_STATEMENT_CAP` could
    /// hold, completing.
    ///
    /// Plan §3.7 is explicit — *"one envelope per unit, not per run, so a
    /// deadline caps one unit rather than the sum"*. Eight units at seven
    /// statements each is 56, comfortably past the 12-statement per-unit cap
    /// and past the preamble cap too.
    ///
    /// MUTATION (executed 2026-07-28): drop the per-unit
    /// `reclaim_unit_envelope(..).scope(..)` wrapper so the claim and the
    /// deletes run under the caller's envelope => FAILS here with
    /// `StatementCapExceeded`. **Lower bound.**
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sweep_opens_one_envelope_per_unit_rather_than_one_per_run() {
        let pairs: Vec<(String, u64, u64, u64)> = (0..8)
            .map(|index| (format!("s-{index}"), 100 + index as u64, 3, 2))
            .collect();
        let borrowed: Vec<(&str, u64, u64, u64)> = pairs
            .iter()
            .map(|(session, generation, events, turns)| {
                (session.as_str(), *generation, *events, *turns)
            })
            .collect();
        let mock = ReclaimMock::with_orphans(&borrowed);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);

        // The caller's envelope is sized for the preamble only, and it is
        // built by the same constructor `cmd_db_reclaim_run` uses, so this is
        // the production shape rather than a re-derivation of it.
        let budget = crate::envelope::test_budget(
            30.0,
            RUN_PREAMBLE_STATEMENT_CAP,
            1_000_000,
            1_000_000_000,
        );
        let outcome = run_preamble_envelope(&budget, &budget)
            .scope(client.reclaim_run(
                &RetentionConfig::default(),
                ReclaimScope::McpOpenOrphan,
                ReclaimTrigger::Operator,
                &budget,
                &budget,
            ))
            .await
            .expect("a bounded sweep completes");

        assert!(
            matches!(outcome, ReclaimOutcome::Settled { units: 8, .. }),
            "{outcome:?}"
        );
        let deletes = mock.deleted().len();
        assert_eq!(deletes, 8 * 3, "three tables per unit: {deletes}");
        for (_, counts) in mock.rows() {
            assert_eq!(counts, (0, 0, 0));
        }
    }

    /// Statements each envelope admitted, keyed by the envelope's request id
    /// (`moraine-<kind>-<uuid>`, with the `-<sequence>` suffix stripped). Same
    /// decomposition `sink::tests::assert_enveloped` makes, used here to count
    /// per phase rather than to check the prefix.
    fn statements_per_envelope(mock: &ReclaimMock) -> BTreeMap<String, u32> {
        let mut counts: BTreeMap<String, u32> = BTreeMap::new();
        for (statement, params) in mock.params() {
            let query_id = params
                .get("query_id")
                .unwrap_or_else(|| panic!("statement carried no envelope query id: {statement}"));
            let (request, sequence) = query_id
                .rsplit_once('-')
                .expect("child query ids end with a sequence");
            sequence
                .parse::<u32>()
                .expect("child query ids end with a numeric sequence");
            *counts.entry(request.to_string()).or_default() += 1;
        }
        counts
    }

    /// The one envelope in `counts` whose request id names `kind`.
    fn envelope_statements(counts: &BTreeMap<String, u32>, kind: &str) -> u32 {
        let prefix = format!("moraine-{kind}-");
        let matching: Vec<u32> = counts
            .iter()
            .filter(|(request, _)| request.starts_with(&prefix))
            .map(|(_, count)| *count)
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "expected exactly one `{kind}` envelope, saw {counts:?}"
        );
        matching[0]
    }

    /// **G-CAP-PHASES.** Every statement cap is derived from named parts, and
    /// each derivation covers what its phase actually issues — *measured*, by
    /// driving all four phases against the stateful stand-in and counting the
    /// statements each envelope admitted.
    /// Denomination: statements per envelope, grouped by the `query_id` the
    /// server received.
    ///
    /// Fails for: a cap a legitimate phase can exceed — §3.7's rule, and the
    /// reason `PLAN_STATEMENT_CAP = 8` stopped being sufficient the moment the
    /// planner gained a probe per scope.
    ///
    /// The previous revision of this test was named
    /// `..._is_derived_from_what_its_phase_issues` and drove **no phase**: it
    /// read the four cap constants and checked them against the named parts
    /// they are defined from. Constants agreeing with their own definition is
    /// not the claim the name makes, and it is why a `run` that opened a
    /// per-unit-sized envelope around its preamble had nothing to fail.
    ///
    /// Each phase is bounded from **both** sides: an upper bound against its
    /// cap, and a lower bound against the named parts the cap is derived from,
    /// so a phase that stops issuing one of its own statements fails here too.
    /// Measured 2026-07-28: plan 5, run preamble 5, unit 6, status 8, against
    /// caps 9 / 7 / 12 / 12.
    ///
    /// Two of the upper bounds are **projections**, and deliberately so. Only
    /// two scopes have a registered executor, so a plan issues two probes
    /// rather than four; the cap must cover the fully-registered case, so the
    /// unregistered scopes are added back. Likewise the driven unit is a
    /// 3-table scope and the widest is 7, so the measurement is projected onto
    /// the widest scope's table count. A projection is stated where it is used.
    ///
    /// MUTATION (executed 2026-07-28): drop the `PLAN_STMT_PER_SCOPE_PROBE *
    /// ReclaimScope::ALL.len()` term from `PLAN_STATEMENT_CAP` — the historical
    /// regression named above => FAILS on the plan projection (7 > 5) and on
    /// the status projection. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): `UNIT_STMT_MAX_TABLES` 7 → 3, so the cap
    /// covers only the scope actually driven => FAILS on the widest-scope
    /// projection (10 > 8). **Width: the measurement is not the widest case,
    /// and the projection is what closes the gap.**
    ///
    /// MUTATION (executed 2026-07-28): `RUN_PREAMBLE_STATEMENT_CAP` → `1 + 1 +
    /// PLAN_STMT_PER_SCOPE_PROBE` => FAILS with `StatementCapExceeded` inside
    /// the driven run, before any assertion. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): drop `reclaim_status`'s own
    /// `reclaim_ledger_summary()` read, leaving the panel's ledger block
    /// permanently empty => FAILS on the status **lower** bound (7 < 8).
    /// **Width: status is a plan *plus* its own report and ledger read, and
    /// that "plus" is what its cap is named for.**
    ///
    /// MUTATION (executed 2026-07-28): `STATUS_STATEMENT_CAP` →
    /// `PLAN_STATEMENT_CAP` => FAILS **at compile time** on the `const` assert
    /// below. Recorded as a build-time kill rather than a test failure, and
    /// stated as such: the status upper bound carries `PLAN_STMT_MARGIN` of
    /// slack, so it cannot itself catch a cap between 10 and 12, and the const
    /// assert is the tighter of the two.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_statement_cap_covers_what_its_phase_issues() {
        // Scopes with no registered executor issue no probe, so a plan today
        // costs less than a fully-registered one. The cap must cover the
        // fully-registered case.
        let unprobed = ReclaimScope::ALL.len() as u32 - registered_executors().len() as u32;

        // ---- plan -------------------------------------------------------
        let plan_mock = ReclaimMock::with_orphans(&[("s-plan", 100, 3, 2)]);
        let plan_client = mock_client(spawn_reclaim_mock(plan_mock.clone()).await);
        let plan_budget =
            crate::envelope::test_budget(30.0, PLAN_STATEMENT_CAP, 1_000_000, 1_000_000_000);
        plan_envelope(&plan_budget, &plan_budget)
            .scope(plan_client.reclaim_plan(&RetentionConfig::default(), &ReclaimScope::ALL))
            .await
            .expect("a full-scope plan fits its own cap");
        let plan_issued = envelope_statements(&statements_per_envelope(&plan_mock), "reclaim-plan");
        let probing_scopes = registered_executors().len() as u32;
        assert_eq!(
            plan_issued,
            PLAN_STMT_LEDGER_SUMMARY
                + PLAN_STMT_STORAGE_REPORT
                + PLAN_STMT_PER_SCOPE_PROBE * probing_scopes,
            "the plan phase must issue exactly the parts its cap is derived from"
        );
        assert!(
            plan_issued + unprobed <= PLAN_STATEMENT_CAP,
            "a fully-registered plan issues {plan_issued}+{unprobed} statements, cap is \
             {PLAN_STATEMENT_CAP}"
        );

        // ---- run preamble, and the unit envelope nested inside it -------
        let run_mock = ReclaimMock::with_orphans(&[("s-run", 200, 3, 2)]);
        let run_client = mock_client(spawn_reclaim_mock(run_mock.clone()).await);
        let run_budget = crate::envelope::test_budget(
            30.0,
            RUN_PREAMBLE_STATEMENT_CAP,
            1_000_000,
            1_000_000_000,
        );
        let unit_budget =
            crate::envelope::test_budget(30.0, UNIT_STATEMENT_CAP, 1_000_000, 1_000_000_000);
        run_preamble_envelope(&run_budget, &run_budget)
            .scope(run_client.reclaim_run(
                &RetentionConfig::default(),
                ReclaimScope::McpOpenOrphan,
                ReclaimTrigger::Operator,
                &unit_budget,
                &unit_budget,
            ))
            .await
            .expect("a one-unit run fits the preamble and unit caps");
        let run_counts = statements_per_envelope(&run_mock);
        let preamble_issued = envelope_statements(&run_counts, "reclaim-run");
        // The pending-mutation probe, the ledger re-drive read, one candidate
        // probe and the byte-estimate report — the four named parts
        // `RUN_PREAMBLE_STATEMENT_CAP` is built from, all four measured.
        assert_eq!(
            preamble_issued,
            1 + 1 + PLAN_STMT_PER_SCOPE_PROBE + PLAN_STMT_STORAGE_REPORT,
            "the run preamble must issue exactly the parts its cap is derived from"
        );
        assert!(
            preamble_issued <= RUN_PREAMBLE_STATEMENT_CAP,
            "the preamble issued {preamble_issued} statements, cap is \
             {RUN_PREAMBLE_STATEMENT_CAP}"
        );

        // The driven scope deletes from three tables; the widest deletes from
        // more, and the cap has to cover that one.
        let driven_tables = ReclaimScope::McpOpenOrphan.tables().len() as u32;
        let widest_tables = ReclaimScope::ALL
            .into_iter()
            .map(|scope| scope.tables().len() as u32)
            .max()
            .expect("scopes exist");
        let unit_issued = envelope_statements(&run_counts, "reclaim-unit");
        assert_eq!(
            unit_issued,
            UNIT_STMT_CLAIM + UNIT_STMT_ADVANCE + driven_tables + UNIT_STMT_SETTLE,
            "a unit must issue exactly claim + advance + one delete per table + settle"
        );
        assert!(
            unit_issued - driven_tables + widest_tables <= UNIT_STATEMENT_CAP,
            "a unit of the widest scope issues {unit_issued}-{driven_tables}+{widest_tables} \
             statements, cap is {UNIT_STATEMENT_CAP}"
        );

        // ---- status, which scopes its own envelope ----------------------
        let status_mock = ReclaimMock::with_orphans(&[("s-status", 300, 3, 2)]);
        let status_client = mock_client(spawn_reclaim_mock(status_mock.clone()).await);
        let status_budget =
            crate::envelope::test_budget(30.0, STATUS_STATEMENT_CAP, 1_000_000, 1_000_000_000);
        let report = status_client
            .reclaim_status(&RetentionConfig::default(), &status_budget, &status_budget)
            .await;
        assert!(
            report.available,
            "status did not complete: {:?}",
            report.error
        );
        let status_issued =
            envelope_statements(&statements_per_envelope(&status_mock), "reclaim-status");
        // "A plan plus its own report and ledger read" is the cap's whole
        // justification, so it is measured rather than asserted of the
        // constants: drop either and this is the assertion that notices.
        assert_eq!(
            status_issued,
            plan_issued + PLAN_STMT_STORAGE_REPORT + PLAN_STMT_LEDGER_SUMMARY,
            "status must issue a plan plus its own storage report and ledger read"
        );
        assert!(
            status_issued + unprobed <= STATUS_STATEMENT_CAP,
            "a fully-registered status issues {status_issued}+{unprobed} statements, cap is \
             {STATUS_STATEMENT_CAP}"
        );

        // The derivations themselves, which the measurements above bound but
        // do not replace: a cap must be built from named parts, not left a
        // round number.
        assert_eq!(
            PLAN_STATEMENT_CAP,
            PLAN_STMT_LEDGER_SUMMARY
                + PLAN_STMT_STORAGE_REPORT
                + PLAN_STMT_PER_SCOPE_PROBE * ReclaimScope::ALL.len() as u32
                + PLAN_STMT_MARGIN
        );
        const { assert!(STATUS_STATEMENT_CAP > PLAN_STATEMENT_CAP) };
        assert_eq!(
            UNIT_STATEMENT_CAP,
            UNIT_STMT_CLAIM
                + UNIT_STMT_ADVANCE
                + UNIT_STMT_SETTLE
                + UNIT_STMT_MAX_TABLES
                + UNIT_STMT_MARGIN
        );
        assert_eq!(
            UNIT_STMT_MAX_TABLES, widest_tables,
            "the per-unit cap's table term must be the widest scope's table count"
        );
    }

    /// A ledger row naming a scope this build cannot execute is left alone.
    /// Fails for: a downgrade settling units whose deletes never ran, or
    /// executing a unit whose predicate shape the running binary does not have.
    #[test]
    fn a_ledger_row_from_an_unknown_scope_is_not_resumable() {
        let row = LedgerUnitRow {
            reclaim_id: "r".to_string(),
            scope: "search_generation".to_string(),
            source_host: String::new(),
            source_name: String::new(),
            source_file: String::new(),
            source_generation: 0,
            session_id: "s".to_string(),
            candidate_generation: 1,
            phase: "claimed".to_string(),
            estimated_rows: 0,
            estimated_bytes: 0,
            unsettled_seconds: 0,
        };
        assert!(row.into_unit().is_none());
    }

    /// **G-DELETE-SETTINGS.** Fails for: a reclaim delete that inherits its
    /// mutation settings from the server instead of pinning them, or a pinned
    /// setting the transport silently drops.
    /// Denomination: the query params the DELETE request actually carried.
    ///
    /// The module header's bounding argument — one mutation in flight at a
    /// time, control back to the driver between units — is only true while
    /// `lightweight_deletes_sync != 0`. Under `0` the statement returns before
    /// the mutation finishes and a run stacks one unfinished mutation per unit
    /// on a host chosen for reclamation because its disk is nearly full.
    ///
    /// MUTATION (executed 2026-07-28): make `reclaim_delete_write` call
    /// `reclaim_write(statement, &[])` => FAILS here. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): change the pinned value to `"0"` =>
    /// FAILS here. **Width: the value, not merely the key's presence.**
    ///
    /// MUTATION (executed 2026-07-28): add `"lightweight_deletes_sync"` to
    /// `crate::ENVELOPE_OWNED_PARAMS` **and widen its type from `[&str; 9]` to
    /// `[&str; 10]`** => FAILS here, because the transport then drops the
    /// caller's copy and the param never reaches the wire. **Width: a pin that
    /// reads correctly at the call site but does nothing.** The type widening
    /// is not optional and is not cosmetic: the constant is a fixed-length
    /// array, so "add an entry" as written does not compile and reaches no
    /// assertion at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_reclaim_delete_pins_its_mutation_settings() {
        assert_eq!(
            RECLAIM_DELETE_SETTINGS,
            &[("lightweight_deletes_sync", "2")]
        );

        let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        run_orphan_reclaim(&client).await.expect("run completes");

        let sent = mock.params();
        let deletes: Vec<_> = sent
            .iter()
            .filter(|(statement, _)| statement.starts_with("DELETE FROM"))
            .collect();
        assert_eq!(deletes.len(), 3, "one delete per table of the scope");
        for (statement, params) in &deletes {
            for (key, value) in RECLAIM_DELETE_SETTINGS {
                assert_eq!(
                    params.get(*key).map(String::as_str),
                    Some(*value),
                    "`{key}` never reached the wire for: {statement}"
                );
            }
        }
        // The ledger inserts register no mutation and must not claim to.
        for (statement, params) in &sent {
            if statement.starts_with("INSERT INTO") {
                assert!(
                    !params.contains_key("lightweight_deletes_sync"),
                    "a ledger insert must not carry delete settings: {statement}"
                );
            }
        }
    }

    /// **G-REDRIVE-SCOPE.** Fails for: a re-drive page that is not scoped, so
    /// one wedged scope starves every other scope's recovery.
    ///
    /// MUTATION (executed 2026-07-28): drop `AND scope = {}` from
    /// `ledger_redrive_sql` => FAILS here. **Lower bound: with 64 wedged
    /// `mcp_open_orphan` rows the unscoped page is entirely orphan rows, the
    /// caller filters them all out in Rust, and `mcp_open_retired_lineage`
    /// never re-drives a unit again.**
    #[test]
    fn the_redrive_page_is_scoped_so_one_wedged_scope_cannot_starve_another() {
        for scope in ReclaimScope::ALL {
            let sql = ledger_redrive_sql("moraine", scope, 64);
            assert!(
                sql.contains(&format!("AND scope = '{}'", scope.as_str())),
                "the re-drive page must be scoped in SQL, not filtered in Rust: {sql}"
            );
            assert!(
                sql.contains("AS unsettled_seconds"),
                "the abandon bound needs the unit's durable age: {sql}"
            );
        }
        assert_ne!(
            ledger_redrive_sql("moraine", ReclaimScope::McpOpenOrphan, 64),
            ledger_redrive_sql("moraine", ReclaimScope::McpOpenRetiredLineage, 64),
        );
    }

    /// A phase advance preserves the unit's original claim time.
    ///
    /// `claimed_at` is the re-drive page's sort key and the only input the
    /// abandon bound has. Resetting it on every advance meant a unit that
    /// failed on every attempt was rejuvenated by its own failure: it could
    /// never age past [`RECLAIM_UNSETTLED_ABANDON_SECONDS`].
    ///
    /// MUTATION (executed 2026-07-28): make `ledger_claim_statement` always
    /// emit `now64(3)` => FAILS here. **Lower bound.**
    #[test]
    fn a_phase_advance_carries_the_original_claim_time_forward() {
        let fresh = unit(ReclaimScope::McpOpenOrphan);
        assert_eq!(fresh.unsettled_seconds, 0);
        assert!(
            ledger_claim_statement("moraine", &fresh).contains("now64(3), generateSnowflakeID()"),
            "a first claim is claimed now"
        );
        let aged = ReclaimUnit {
            unsettled_seconds: 3_600,
            ..fresh
        };
        let advanced = ledger_advance_statement("moraine", &aged, ReclaimPhase::Deleting);
        assert!(
            advanced.contains("now64(3) - toIntervalSecond(3600)"),
            "a re-drive must not reset the unit's age: {advanced}"
        );
    }

    /// **G-ABANDON.** A unit that has been unsettled past the bound is moved
    /// to `abandoned` and stops consuming a re-drive slot — **and a unit that
    /// has not is re-driven, on both sides of the boundary.**
    /// Denomination: the ledger phase the run wrote, and the deletes it did
    /// **not** issue for that unit.
    ///
    /// `ReclaimPhase::Abandoned` documented itself as "Abandoned by an
    /// operator or by a bound. Never re-driven." and had **no writer outside
    /// `#[cfg(test)]`**, while the three §3.7 bounds that would produce one
    /// (`max_rows`, `max_bytes`, `max_parts_touched`) did not exist. A poison
    /// unit therefore wedged its scope forever with one `warn!` per tick as
    /// the only signal.
    ///
    /// MUTATION (executed 2026-07-28): delete the `unit.unsettled_seconds >=
    /// RECLAIM_UNSETTLED_ABANDON_SECONDS` branch from `reclaim_redrive` =>
    /// FAILS here on the wedged half: the unit is re-driven again instead of
    /// being abandoned. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): compare with `>` against `0` instead,
    /// so every re-driven unit is abandoned on its first retry => FAILS here
    /// on the fresh half. **Upper bound.**
    ///
    /// The previous revision claimed that upper bound was carried by
    /// `an_interrupted_claim_is_completed_by_the_next_runs_redrive`. **It is
    /// not**: with `> 0` in place, the only test in this module that fails is
    /// this one, and `an_interrupted_claim_…` passes (executed 2026-07-28; it
    /// was the sole failure named by the run). The mock reports
    /// `unsettled_seconds` from one field defaulting to `0`, so before the
    /// fresh half below existed, no test presented an aged unit and every
    /// re-driven unit was fresh — nothing could observe the threshold at all.
    /// `Abandoned` is terminal — "never re-driven" — so a build that abandons
    /// every unit on its first retry destroys the recovery path permanently.
    /// The fresh half is that missing bound, and it sits at
    /// `RECLAIM_UNSETTLED_ABANDON_SECONDS - 1` rather than at `0` so it pins
    /// the comparison's *boundary* and not merely its existence.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_unit_unsettled_past_the_bound_is_abandoned_rather_than_redriven() {
        assert_eq!(RECLAIM_UNSETTLED_ABANDON_SECONDS, 86_400);

        /// One run against a ledger holding a single `deleting` unit that has
        /// been unsettled for `age` seconds.
        async fn run_with_age(age: u64) -> (ReclaimOutcome, ReclaimMock) {
            let mock = ReclaimMock::default();
            {
                let mut state = mock.lock();
                state.ledger.insert(
                    "mcp_open_orphan:wedged:1".to_string(),
                    MockLedgerRow {
                        scope: ReclaimScope::McpOpenOrphan.as_str().to_string(),
                        session_id: "wedged".to_string(),
                        candidate_generation: 1,
                        phase: "deleting".to_string(),
                    },
                );
                state.unsettled_seconds = age;
            }
            let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
            let outcome = run_orphan_reclaim(&client).await.expect("run completes");
            (outcome, mock)
        }

        // Past the bound: abandoned, and it issues no delete.
        let (outcome, mock) = run_with_age(RECLAIM_UNSETTLED_ABANDON_SECONDS + 1).await;
        assert!(
            matches!(outcome, ReclaimOutcome::Settled { abandoned: 1, .. }),
            "the wedged unit must be abandoned and reported: {outcome:?}"
        );
        assert_eq!(
            mock.ledger()
                .get("mcp_open_orphan:wedged:1")
                .map(|row| row.phase.as_str()),
            Some("abandoned"),
            "the terminal phase must be durable"
        );
        assert!(
            !mock
                .statements()
                .iter()
                .any(|statement| statement.starts_with("DELETE FROM")),
            "an abandoned unit must not issue deletes"
        );

        // One second short of it: re-driven to `done`, which is the path a
        // transient outage recovers along.
        let (outcome, mock) = run_with_age(RECLAIM_UNSETTLED_ABANDON_SECONDS - 1).await;
        assert!(
            matches!(
                outcome,
                ReclaimOutcome::Settled {
                    abandoned: 0,
                    redriven: 1,
                    ..
                }
            ),
            "a unit inside the bound must be re-driven, not abandoned: {outcome:?}"
        );
        assert_eq!(
            mock.ledger()
                .get("mcp_open_orphan:wedged:1")
                .map(|row| row.phase.as_str()),
            Some("done"),
            "re-drive settles the unit rather than retiring it"
        );
        assert!(
            mock.statements()
                .iter()
                .any(|statement| statement.starts_with("DELETE FROM")),
            "a re-driven unit finishes its deletes: {:?}",
            mock.statements()
        );
    }

    /// **G-POISON.** A deterministically-failing unit does not abort its
    /// scope's run; the run reports it and carries on to new work.
    /// Denomination: the outcome's `failed` count and whether new units were
    /// still claimed.
    ///
    /// `reclaim_redrive` previously `?`-propagated on the first failing unit,
    /// and because re-drive runs **before** any candidate is probed, one such
    /// unit aborted every subsequent run of that scope on every tick,
    /// indefinitely, with no new work ever claimed.
    ///
    /// MUTATION (executed 2026-07-28): restore
    /// `.await?` on `reclaim_execute_unit` inside `reclaim_redrive` => FAILS
    /// here: `reclaim_run` returns `Err` and no candidate is ever claimed.
    /// **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): count a failed unit as `redriven`
    /// instead of `failed` => FAILS on the `failed` assertion. **Width: a
    /// wedged unit must not be reported as progress.**
    #[tokio::test(flavor = "multi_thread")]
    async fn a_poison_unit_does_not_wedge_its_scope() {
        let mock = ReclaimMock::with_orphans(&[("fresh", 20, 2, 1)]);
        {
            let mut state = mock.lock();
            state.ledger.insert(
                "mcp_open_orphan:poison:9".to_string(),
                MockLedgerRow {
                    scope: ReclaimScope::McpOpenOrphan.as_str().to_string(),
                    session_id: "poison".to_string(),
                    candidate_generation: 9,
                    phase: "deleting".to_string(),
                },
            );
            // Every delete naming the poison unit is refused, forever.
            state.fail_always_on = Some("'poison'".to_string());
        }
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        let outcome = run_orphan_reclaim(&client)
            .await
            .expect("a poison unit must not abort the run");

        let ReclaimOutcome::Settled { failed, units, .. } = outcome else {
            panic!("expected a settled run that reports the failure: {outcome:?}");
        };
        assert_eq!(failed, 1, "the poison unit must be counted, not swallowed");
        assert!(
            units > 0,
            "new work must still be claimed while one unit is wedged"
        );
        // The fresh candidate really was collected.
        assert_eq!(
            mock.ledger()
                .get("mcp_open_orphan:fresh:20")
                .map(|row| row.phase.as_str()),
            Some("done"),
        );
    }

    /// A re-drive report separates progress from wedging.
    ///
    /// MUTATION (executed 2026-07-28): make `is_quiet` return `true`
    /// unconditionally => FAILS here.
    #[test]
    fn a_redrive_report_distinguishes_progress_from_wedging() {
        assert!(ReclaimRedriveReport::default().is_quiet());
        for report in [
            ReclaimRedriveReport {
                failed: 1,
                ..Default::default()
            },
            ReclaimRedriveReport {
                abandoned: 1,
                ..Default::default()
            },
            ReclaimRedriveReport {
                unresumable: 1,
                ..Default::default()
            },
        ] {
            assert!(!report.is_quiet(), "{report:?} is not a quiet pass");
        }
        assert!(ReclaimRedriveReport {
            redriven: 5,
            ..Default::default()
        }
        .is_quiet());
    }

    /// **G-LOWDISK.** Only the unattended trigger refuses to start on a
    /// nearly-full disk, when it refuses it deletes nothing, and the two
    /// figures it reports are the two figures it compared.
    /// Denomination: the outcome variant, the statements issued, and the
    /// `free_bytes`/`required_bytes` a real run put on the refusal.
    ///
    /// The reported requirement is derived here from **where the gate flips**,
    /// not read off `RECLAIM_MIN_FREE_BYTES`: the run is replayed at exactly
    /// `required_bytes` free (must proceed) and one byte below it (must still
    /// decline). A figure that is merely a constant this test also knows would
    /// be satisfied by any pair that moved together.
    ///
    /// This half exists because the operator-facing string is assembled two
    /// crates away, and
    /// `render::tests::every_rendered_reclaim_line_carries_its_denomination`
    /// builds `LowDisk` by hand from literals — it proves the line renders both
    /// numbers, never that a run put the right ones there. With
    /// `required_bytes` free, the rendered refusal read "declined: 1073741824
    /// free byte(s), 0 required": an operator told the run needs no headroom,
    /// by the run that just refused to start for want of headroom.
    ///
    /// MUTATION (executed 2026-07-28): make `checks_free_space` return `true`
    /// for `Operator` => FAILS on the operator half, and it would refuse the
    /// one command an operator has on a full disk. **Upper bound.**
    ///
    /// MUTATION (executed 2026-07-28): make it return `false` for
    /// `Maintenance`, or delete the `if trigger.checks_free_space()` block
    /// from `reclaim_run` => FAILS on the maintenance half. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): `required_bytes: 0` on the `LowDisk`
    /// arm of `reclaim_run` => FAILS on the coherence assertion. **The figure
    /// the refusal reports, which nothing observed.**
    ///
    /// MUTATION (executed 2026-07-28): `required_bytes: RECLAIM_MIN_FREE_BYTES
    /// * 2` => FAILS on the boundary replay: a host with that much free is not
    /// declined. **Width: over-reporting as well as under-reporting.**
    #[tokio::test(flavor = "multi_thread")]
    async fn only_the_unattended_trigger_refuses_to_start_on_a_full_disk() {
        assert!(ReclaimTrigger::Maintenance.checks_free_space());
        assert!(!ReclaimTrigger::Operator.checks_free_space());
        assert_eq!(RECLAIM_MIN_FREE_BYTES, 10 * 1024 * 1024 * 1024);

        // A host with a gigabyte free: below the bound.
        let free_on_a_full_host = 1024 * 1024 * 1024;
        let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
        mock.lock().free_bytes = free_on_a_full_host;
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);

        let refused = run_orphan_reclaim_as(&client, ReclaimTrigger::Maintenance)
            .await
            .expect("a low-disk refusal is an outcome, not an error");
        let ReclaimOutcome::LowDisk {
            free_bytes,
            required_bytes,
            ..
        } = refused
        else {
            panic!("the unattended tick must decline: {refused:?}");
        };
        assert_eq!(
            free_bytes, free_on_a_full_host,
            "the refusal must report the disk it actually read"
        );
        assert!(
            free_bytes < required_bytes,
            "a run declining for want of headroom reported needing {required_bytes} byte(s) with \
             {free_bytes} free, which is not a refusal an operator can act on"
        );
        assert!(
            !mock
                .statements()
                .iter()
                .any(|statement| statement.starts_with("DELETE FROM")),
            "a declined run must delete nothing"
        );

        // The same host, the same disk, an operator who asked for it.
        let allowed = run_orphan_reclaim_as(&client, ReclaimTrigger::Operator)
            .await
            .expect("an operator run proceeds");
        assert!(
            matches!(allowed, ReclaimOutcome::Settled { .. }),
            "an operator must be able to reclaim precisely when the disk is full: {allowed:?}"
        );

        // `required_bytes` is the headroom the run says it needs, so it must be
        // the headroom that actually lets it start — replayed on both sides of
        // the figure the refusal itself reported.
        for (free, should_decline) in [(required_bytes - 1, true), (required_bytes, false)] {
            let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
            mock.lock().free_bytes = free;
            let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
            let outcome = run_orphan_reclaim_as(&client, ReclaimTrigger::Maintenance)
                .await
                .expect("the gate returns an outcome");
            let declined = matches!(outcome, ReclaimOutcome::LowDisk { .. });
            assert_eq!(
                declined, should_decline,
                "the refusal reported needing {required_bytes} free byte(s); at {free} free the \
                 unattended tick produced {outcome:?}"
            );
        }
    }

    /// The per-run unit bound is defensible against the measured cost of one
    /// unit.
    ///
    /// One unit issues one **unprunable** delete against `mcp_open_events`
    /// plus two deletes that prune to one partition of 64. Unprunable is the
    /// date-independent part: `session_id` is in neither that table's
    /// `PARTITION BY cityHash64(event_uid) % 64` nor its
    /// `PRIMARY KEY (event_uid, slot)`, so `EXPLAIN indexes = 1` reports
    /// `Condition: true` and reads every active part and every granule, on any
    /// day. The size of that scan is not date-independent and is quoted here
    /// only as a magnitude — of order 10 GiB; see
    /// [`RECLAIM_MAX_UNITS_PER_RUN`] for the dated sample and why the counts
    /// drift. At 64 units and two scopes on a 60 s tick that was up to 128 full
    /// scans of the largest `mcp_open` table per minute.
    ///
    /// MUTATION (executed 2026-07-28): restore `RECLAIM_MAX_UNITS_PER_RUN` to
    /// 64 => FAILS here. **Upper bound.**
    #[test]
    fn the_per_run_unit_bound_is_derived_from_the_measured_unit_cost() {
        // `mcp_open_events` is unprunable for this unit key — `Condition:
        // true`, every part, every granule, on any day — so one unit costs one
        // full scan of it. Every registered scope that **names that table**
        // shares the 60 s tick, so the per-minute ceiling is
        // `mcp_open_scopes * bound` full scans of the largest `mcp_open`
        // table. The multiplier is the scopes whose `tables()` include it —
        // not `registered_executors().len()`, which as of WI-07 counts the
        // read-index scope too, and that scope's deletes never touch a
        // `mcp_open` table: multiplying by it would let shrinking the bound
        // "pay for" a scope that costs a ~0.4 GiB sweep, not a ~10 GiB one.
        // The read-index sweep gets its own ceiling below, in its own
        // denomination.
        //
        // This reads the **constant**. What a run actually sends is a separate
        // fact, and this test cannot see it: `reclaim_candidates` could pass a
        // literal and nothing here would notice. That half is
        // `the_per_run_unit_bound_reaches_every_paged_statement`, which parses
        // the `LIMIT` off the statements a real run issued.
        let mcp_open_scopes = registered_executors()
            .into_iter()
            .filter(|scope| scope.tables().contains(&ReclaimTable::McpOpenEvents))
            .count();
        assert_eq!(
            mcp_open_scopes, 2,
            "the two mcp_open scopes are the ones whose units scan the ~10 GiB table"
        );
        let full_scans_per_tick = mcp_open_scopes * RECLAIM_MAX_UNITS_PER_RUN;
        assert!(
            (1..=16).contains(&full_scans_per_tick),
            "an unattended tick may issue {full_scans_per_tick} unprunable full scans of the \
             largest mcp_open table per minute; that is not a bound"
        );
        // The read-index scope's unit deletes are unprunable too — none of the
        // three tables leads its primary key or partition key with any column
        // of the generation tuple — but the tables total ~0.4 GiB, not
        // ~10 GiB, so the same page bound holds it to a per-tick sweep two
        // orders of magnitude smaller.
        let read_index_scopes = registered_executors()
            .into_iter()
            .filter(|scope| scope.tables().contains(&ReclaimTable::McpEventNavigation))
            .count();
        assert_eq!(read_index_scopes, 1);
        assert!((1..=16).contains(&(read_index_scopes * RECLAIM_MAX_UNITS_PER_RUN)));
    }

    /// The `LIMIT` a statement was sent with, if it carries one.
    ///
    /// Deliberately parsed out of the statement text rather than compared
    /// against `RECLAIM_MAX_UNITS_PER_RUN`: the point is to read the number
    /// that reached the wire, from a test that does not know what the constant
    /// says.
    fn issued_limit(statement: &str) -> Option<usize> {
        let rest = statement.split("\nLIMIT ").nth(1)?;
        rest.split(|character: char| !character.is_ascii_digit())
            .next()?
            .parse()
            .ok()
    }

    /// **G-BOUND (the wire half).** Fails for: a per-run bound that is correct
    /// in the constant and wrong in the call that uses it.
    /// Denomination: the `LIMIT` on every paged statement a real run issued.
    ///
    /// All three of this scope's paged reads — the run's candidate probe, the
    /// re-drive page, and the **planner's** candidate probe — take their bound
    /// as a parameter, and **every existing guard read the constant rather than
    /// the argument.** `(executor.probe)(&db, horizon, 512)` and
    /// `ledger_redrive_sql(&db, scope, 512)` each left
    /// `cargo test --workspace` fully green while undoing the headline V4
    /// mitigation: 64 units × 2 scopes × a 60 s tick is 128 unprunable full
    /// scans of a ~10 GiB table per minute, on a host chosen for reclamation
    /// because it is nearly out of disk.
    ///
    /// *Every* is meant literally, which is why this drives `reclaim_plan` as
    /// well as `reclaim_run`. `plan_scope` reaches `reclaim_candidates` by its
    /// own call, and a version of this test that drove only the run left that
    /// call site free — a literal `512` there changed no test result anywhere
    /// in the workspace. The planner's page is not a lesser surface: an
    /// operator reads its unit counts before confirming, and
    /// `moraine db reclaim plan` is the one command that is *not* gated on
    /// free disk.
    ///
    /// The assertion is deliberately **not** `limit == RECLAIM_MAX_UNITS_PER_RUN`.
    /// That would pass for any call site as long as the constant moved with
    /// it. It re-derives the mitigation instead — scopes × issued bound, per
    /// tick — from the number the server was actually sent.
    ///
    /// MUTATION (executed 2026-07-28): change `reclaim_run`'s
    /// `reclaim_candidates` third argument to a literal `512` => FAILS here on
    /// the run's candidate probe. **Upper bound.**
    ///
    /// MUTATION (executed 2026-07-28): change `plan_scope`'s
    /// `reclaim_candidates` third argument to a literal `512` => FAILS here on
    /// the planner's probe, and passed everything before the `reclaim_plan`
    /// drive was added. **Width: the planner is a paged read too.**
    ///
    /// MUTATION (executed 2026-07-28): change `ledger_redrive_sql`'s third
    /// argument in `reclaim_redrive` to a literal `512` => FAILS here on the
    /// re-drive page. **Width: all three paged reads, not only the probe.**
    ///
    /// MUTATION (executed 2026-07-28): restore `RECLAIM_MAX_UNITS_PER_RUN` to
    /// 64 => FAILS here as well as in
    /// `the_per_run_unit_bound_is_derived_from_the_measured_unit_cost`.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_per_run_unit_bound_reaches_every_paged_statement() {
        let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        let outcome = run_orphan_reclaim(&client).await.expect("run completes");
        assert!(
            matches!(outcome, ReclaimOutcome::Settled { .. }),
            "{outcome:?}"
        );
        // The dry-run planner reaches `reclaim_candidates` through its own call
        // site, so a bound that is right in `reclaim_run` and wrong in
        // `plan_scope` is still a 512-unit page issued against the host.
        crate::envelope::with_test_envelope(
            client.reclaim_plan(&RetentionConfig::default(), &ReclaimScope::ALL),
        )
        .await
        .expect("plan completes");

        let issued = mock.statements();
        let paged: Vec<(&String, usize)> = issued
            .iter()
            .filter_map(|statement| issued_limit(statement).map(|limit| (statement, limit)))
            .collect();
        // Every paged read must be present, or "every paged statement is
        // bounded" is satisfied by a driver that issued none.
        assert!(
            paged
                .iter()
                .any(|(statement, _)| statement.contains("ORDER BY claimed_at ASC")),
            "the run issued no re-drive page: {issued:?}"
        );
        // Both candidate-probe orderings: the session-grained scopes' and the
        // read-index scope's. A probe that matched neither would silently fall
        // out of this count, which is why the expectation is exact.
        let probes = paged
            .iter()
            .filter(|(statement, _)| {
                statement.contains("ORDER BY session_id ASC, candidate_generation ASC")
                    || statement.contains(
                        "ORDER BY source_host ASC, source_name ASC, source_file ASC, \
                         source_generation ASC",
                    )
            })
            .count();
        assert_eq!(
            probes,
            1 + registered_executors().len(),
            "expected the run's candidate probe plus one per registered scope from the plan, \
             or a call site this test cannot see stayed unbounded: {issued:?}"
        );

        // The V4 mitigation, re-derived from the wire: one unit is one
        // unprunable full scan of `mcp_open_events` for every scope that names
        // that table, and each such scope gets its own page on the same 60 s
        // tick. See `the_per_run_unit_bound_is_derived_from_the_measured_unit_cost`
        // for why the multiplier is the mcp_open-naming scopes rather than the
        // whole registry.
        let mcp_open_scopes = registered_executors()
            .into_iter()
            .filter(|scope| scope.tables().contains(&ReclaimTable::McpOpenEvents))
            .count();
        for (statement, limit) in paged {
            let full_scans_per_tick = mcp_open_scopes * limit;
            assert!(
                (1..=16).contains(&full_scans_per_tick),
                "a paged statement reached the server bounded at {limit}, which is \
                 {full_scans_per_tick} unprunable full scans per unattended tick: {statement}"
            );
        }
    }

    /// **G-HORIZON (the wire half).** Fails for: a safety horizon that is
    /// derived correctly from config and then not passed to the probe.
    /// Denomination: the `toIntervalSecond(…)` literal on the statements a
    /// real run and a real plan issued.
    ///
    /// The horizon is the **only** thing standing between the orphan collector
    /// and a prepare in flight: `prepare` writes children first and the header
    /// last, so between those two writes a healthy session is
    /// indistinguishable from an orphan by the anti-join alone. Age is what
    /// tells them apart.
    ///
    /// `reclaim_mcp_open`'s guards all hand a probe a literal and then look for
    /// that literal, so they cannot see this; the round that added them named a
    /// test here as the other half and **that test did not exist**, which left
    /// `let horizon = 0;` in `reclaim_candidates` fully green — a collector
    /// with no horizon at all, deleting the children of every prepare that had
    /// not yet written its header.
    ///
    /// MUTATION (executed 2026-07-28): `let horizon = 0;` in
    /// `reclaim_candidates` => FAILS here. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): `let horizon = 86_400;` — the stock
    /// value, hard-coded => FAILS here, because the configured horizon is not
    /// the stock one. **Width: the *configured* value, not a plausible
    /// constant.**
    #[tokio::test(flavor = "multi_thread")]
    async fn the_configured_horizon_reaches_the_probe_statement() {
        let retention = RetentionConfig {
            derived_horizon_hours: 72.0,
            ..RetentionConfig::default()
        };
        let configured = retention.derived_horizon_seconds().max(0.0) as u64;
        assert_eq!(configured, 259_200, "72 h in seconds");
        let stock = RetentionConfig::default()
            .derived_horizon_seconds()
            .max(0.0) as u64;
        assert_ne!(
            configured, stock,
            "the test horizon must differ from the default, or a hard-coded default passes"
        );

        let mock = ReclaimMock::with_orphans(&[("s-1", 10, 3, 1)]);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        let budget = crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000);
        crate::envelope::with_test_envelope(client.reclaim_run(
            &retention,
            ReclaimScope::McpOpenOrphan,
            ReclaimTrigger::Operator,
            &budget,
            &budget,
        ))
        .await
        .expect("run completes");
        // The dry-run planner reaches the same call, and an operator reads its
        // output before confirming, so it must agree.
        crate::envelope::with_test_envelope(client.reclaim_plan(&retention, &ReclaimScope::ALL))
            .await
            .expect("plan completes");

        let issued = mock.statements();
        let probes: Vec<&String> = issued
            .iter()
            .filter(|statement| statement.contains("toIntervalSecond("))
            .collect();
        assert!(
            probes.len() >= 3,
            "expected the run's probe plus one per registered scope from the plan: {issued:?}"
        );
        for probe in probes {
            assert!(
                probe.contains(&format!("toIntervalSecond({configured})")),
                "the configured horizon must be the one the server was sent: {probe}"
            );
            assert!(
                !probe.contains(&format!("toIntervalSecond({stock})")),
                "a hard-coded default reached the server instead: {probe}"
            );
        }
    }

    /// A re-drive page carrying a row this build cannot resume does not report
    /// the pass as idle.
    /// Fails for: `reclaim_redrive` dropping its `unresumable` increment, which
    /// is the only signal an operator gets that a downgrade has stranded units.
    ///
    /// `ReclaimRedriveReport::unresumable` has no field in
    /// [`ReclaimOutcome::Settled`], so the *whole* observable is that the run
    /// is not `Idle`. That is the observable that matters: without the
    /// increment, a binary downgraded past a work item reports "nothing to
    /// reclaim" on every tick while its predecessor's claimed units sit in the
    /// ledger forever, and `is_quiet` — which the janitor's logging is built
    /// on — returns `true`.
    ///
    /// MUTATION (executed 2026-07-28): delete
    /// `report.unresumable = report.unresumable.saturating_add(1);` from the
    /// `row.into_unit()` arm of `reclaim_redrive` => FAILS here: the run
    /// returns `Idle`. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): delete the same statement from the
    /// `unit.scope != scope || executor_for(..).is_none()` arm => FAILS here on
    /// the unknown-scope half. **Width: both arms.**
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pass_that_only_skipped_an_unresumable_unit_is_not_idle() {
        // A phase this build cannot parse, and a scope it cannot execute:
        // exactly what a downgrade past a work item leaves in the ledger.
        for extra in [
            "{\"reclaim_id\":\"r-quarantined\",\"scope\":\"mcp_open_orphan\",\"source_host\":\"\",\
             \"source_name\":\"\",\"source_file\":\"\",\"source_generation\":0,\
             \"session_id\":\"s\",\"candidate_generation\":1,\"phase\":\"quarantined\",\
             \"estimated_rows\":0,\"estimated_bytes\":0,\"unsettled_seconds\":0}",
            "{\"reclaim_id\":\"r-future\",\"scope\":\"canonical_generation\",\"source_host\":\"\",\
             \"source_name\":\"\",\"source_file\":\"\",\"source_generation\":0,\
             \"session_id\":\"s\",\"candidate_generation\":1,\"phase\":\"claimed\",\
             \"estimated_rows\":0,\"estimated_bytes\":0,\"unsettled_seconds\":0}",
        ] {
            let mock = ReclaimMock::default();
            mock.lock().redrive_extra_rows = vec![extra.to_string()];
            let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
            let outcome = run_orphan_reclaim(&client).await.expect("run completes");

            assert!(
                !matches!(outcome, ReclaimOutcome::Idle { .. }),
                "a pass that stranded a unit must not report `nothing to reclaim`: {outcome:?}"
            );
            assert!(
                mock.deleted().is_empty(),
                "an unresumable unit must not be executed: {:?}",
                mock.deleted()
            );
            assert!(
                mock.ledger().is_empty(),
                "an unresumable unit must not be settled or abandoned: {:?}",
                mock.ledger()
            );
        }
    }

    /// `reclaim_execute_unit` re-derives authority from the **caller's**
    /// config, not from `RetentionConfig::default()`.
    ///
    /// Harmless for the two bucket-3 scopes — `DerivedOnly` needs no
    /// `[retention]` key — and fatal for every scope that does. This test
    /// states the consequence executably: a default config cannot authorize a
    /// bucket-1 scope, so a default-derived token means WI-09's executor could
    /// never execute a unit, including on re-drive of one an operator had
    /// properly authorized.
    ///
    /// MUTATION (executed 2026-07-28): restore
    /// `ReclaimAuthority::for_scope(unit.scope, &RetentionConfig::default())`
    /// and register a `CanonicalGeneration` executor => the run fails with
    /// "lost its authority" despite a configured horizon. Unregistered in this
    /// build, so the guard here is the type: `reclaim_execute_unit` takes the
    /// config as a parameter and cannot reach a default.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_execute_unit_uses_the_callers_retention_not_a_default() {
        assert!(
            ReclaimAuthority::for_scope(
                ReclaimScope::CanonicalGeneration,
                &RetentionConfig::default()
            )
            .is_err(),
            "a default config must not authorize bucket 1; if it does, threading the real \
             config stops mattering and this test stops explaining itself"
        );
        let configured = RetentionConfig {
            canonical_history_horizon_days: Some(30.0),
            raw_audit_horizon_days: Some(30.0),
            ..RetentionConfig::default()
        };
        assert!(
            ReclaimAuthority::for_scope(ReclaimScope::CanonicalGeneration, &configured).is_ok(),
            "the caller's config is the only one that can authorize bucket 1"
        );

        // And the bucket-3 path really does run under the caller's config.
        let mock = ReclaimMock::with_orphans(&[("s-1", 10, 1, 0)]);
        let client = mock_client(spawn_reclaim_mock(mock.clone()).await);
        let outcome = crate::envelope::with_test_envelope(client.reclaim_run(
            &configured,
            ReclaimScope::McpOpenOrphan,
            ReclaimTrigger::Operator,
            &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
            &crate::envelope::test_budget(30.0, 256, 1_000_000, 1_000_000_000),
        ))
        .await
        .expect("a configured run completes");
        assert!(
            matches!(outcome, ReclaimOutcome::Settled { .. }),
            "{outcome:?}"
        );
    }
}
