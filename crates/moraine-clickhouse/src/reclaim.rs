//! Issue #603 WI-04 — the storage reclaim ledger, the dry-run planner, and the
//! statement surface of the claim/execute/settle protocol.
//!
//! **Nothing in this module deletes any user data, and that is the point.**
//! No executor is registered for any scope yet ([`executor_for`] returns
//! `None` for every variant), so [`ClickHouseClient::reclaim_run`] refuses
//! every scope. The ledger, the planner, the authority types, and the
//! statement emitter must all exist and be trustworthy *before* the first
//! executor lands, because the first executor is the point at which a bug
//! costs an operator their history.
//!
//! ## What is NOT in this build: the §3.2 driver (descoped to WI-05)
//!
//! Plan §3.2 specifies a four-step protocol — plan, claim, execute, settle —
//! and states that "any unit in `claimed` or `deleting` is re-driven to
//! completion before new units are planned". **That driver is not implemented
//! here.** [`emit_delete_statement`], [`ledger_claim_statement`],
//! [`ledger_advance_statement`] and [`ledger_redrive_sql`] have zero
//! production callers: they are the protocol's *statement surface*, built and
//! unit-tested first so that the SQL a WI-05 executor will run is reviewable
//! before it runs. [`ClickHouseClient::reclaim_run`] never reads the ledger,
//! never claims, never advances, and never settles.
//!
//! Do not read [`tests::the_phase_machine_redrives_exactly_the_unsettled_phases`]
//! as evidence to the contrary: it asserts enum predicates and SQL substrings.
//! There is no claim for it to interrupt in this build, and the live half —
//! SIGKILL between the child and parent deletes — is the `reclaim-restart`
//! gate, which arrives with WI-05.
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
//! ## Bounding
//!
//! Every statement runs under a `Background` envelope. Never `Migration`:
//! `arms_cancel_guards()` is false for that class, so a runaway cleanup would
//! be uncancellable by Moraine on a host that has already filled its disk
//! twice. Never `ALTER … DELETE`, never `mutations_sync = 1`: cleanup uses
//! lightweight `DELETE FROM` through the insert-profile transport.

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

/// Statement cap for the planner: the ledger re-drive probe, the candidate
/// probe, and the `system.parts` estimate, with margin. The planner writes
/// nothing.
pub const PLAN_STATEMENT_CAP: u32 = 8;

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
pub fn reclaimed_bytes_note() -> String {
    format!(
        "reclaimed row counts are exact; the on-disk delta is {MERGE_DEFERRED_QUALIFIER} and is \
         not a guarantee"
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
    pub const ALL: [ReclaimScope; 3] = [
        Self::McpOpenOrphan,
        Self::ReadIndexGeneration,
        Self::CanonicalGeneration,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::McpOpenOrphan => "mcp_open_orphan",
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
            Self::McpOpenOrphan => &[
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
            Self::McpOpenOrphan | Self::ReadIndexGeneration => true,
            Self::CanonicalGeneration => false,
        }
    }

    /// Human-readable description used by the CLI refusal.
    pub fn describe(self) -> &'static str {
        match self {
            Self::McpOpenOrphan => {
                "orphan legacy open-projection rows (no publication header; unreadable by design)"
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
    /// `(session_id, candidate_generation)` is on the row itself.
    SessionGeneration,
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
    /// sql/033:8,13 and `mcp_open_publication_headers` at creation.
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
            Self::UidSet => &["event_uid"],
            Self::DocumentJoin => &["doc_id"],
        }
    }
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
            Self::McpOpenEvents | Self::McpOpenTurns | Self::McpOpenPublicationHeaders => {
                ReclaimPredicate::SessionGeneration
            }
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
    /// `every_reclaim_table_is_classified` makes impossible to ship.
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
            ReclaimScope::McpOpenOrphan | ReclaimScope::ReadIndexGeneration => {
                Ok(vec![Self::DerivedOnly])
            }
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
    /// Scopes an executor exists for. Empty in this build.
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
    /// Nothing to reclaim.
    Idle { scope: ReclaimScope },
    /// Units were settled.
    Settled {
        scope: ReclaimScope,
        units: u64,
        reclaimed_rows: u64,
        /// [`reclaimed_bytes_note`].
        denomination: String,
    },
}

impl ReclaimOutcome {
    pub fn scope(&self) -> ReclaimScope {
        match self {
            Self::NoExecutor { scope, .. }
            | Self::Blocked { scope, .. }
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
/// `false` in WI-04: the four statement builders exist and are unit-tested,
/// but nothing calls them outside `#[cfg(test)]`. WI-05 wires the driver and
/// flips this in the same PR that registers the first executor — which is the
/// only order that is safe, and which
/// `no_executor_may_be_registered_before_the_ledger_driver_is_wired` enforces.
///
/// Flipping this constant without wiring the driver is not a shortcut past
/// that test; it is a false statement in a safety guard, and it is one line in
/// a diff.
pub const LEDGER_DRIVER_WIRED: bool = false;

/// A registered scope executor.
///
/// Deliberately empty in WI-04. WI-05 registers `McpOpenOrphan`, WI-07
/// registers `ReadIndexGeneration`, WI-09 registers `CanonicalGeneration`.
/// Until then every `run` refuses, and the refusal names the work item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredExecutor {
    pub scope: ReclaimScope,
}

/// The executor for `scope`, if one has been registered in this build.
pub fn executor_for(scope: ReclaimScope) -> Option<RegisteredExecutor> {
    match scope {
        // WI-05 registers this.
        ReclaimScope::McpOpenOrphan => None,
        // WI-07 registers this.
        ReclaimScope::ReadIndexGeneration => None,
        // WI-09 registers this.
        ReclaimScope::CanonicalGeneration => None,
    }
}

/// Every scope with a registered executor. Empty in this build.
pub fn registered_executors() -> Vec<ReclaimScope> {
    ReclaimScope::ALL
        .into_iter()
        .filter(|scope| executor_for(*scope).is_some())
        .collect()
}

/// The work item that will register `scope`'s executor, for the refusal text.
fn pending_work_item(scope: ReclaimScope) -> &'static str {
    match scope {
        ReclaimScope::McpOpenOrphan => "WI-05",
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
///    It is `predicate_sql.contains(column)` — **name presence, not binding**.
///    It cannot tell a bound key tuple from a mention: `WHERE (session_id,
///    candidate_generation) IN ()` passes (correctly — it deletes nothing) and
///    so would a predicate that merely spelled the column names. Turning that
///    into a real binding check needs a predicate the emitter *builds* rather
///    than one it inspects, which is WI-09's shape. Until then the honest
///    statement of what check 3 buys is: an executor cannot emit a predicate
///    that names no key column at all.
///
/// Lightweight `DELETE FROM`, never `ALTER … DELETE`, never
/// `mutations_sync = 1`.
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
        .filter(|column| !predicate_sql.contains(column))
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
    format!(
        "INSERT INTO {}.{RECLAIM_LEDGER_TABLE}\n\
         (reclaim_id, scope, source_host, source_name, source_file, source_generation,\n \
          session_id, candidate_generation, phase, estimated_rows, estimated_bytes,\n \
          claimed_at, ledger_revision)\n\
         VALUES ({}, {}, {}, {}, {}, toUInt32({}), {}, toUInt64({}), {}, toUInt64({}), \
         toUInt64({}), now64(3), generateSnowflakeID())",
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

/// Units awaiting re-drive, oldest first. Read at the head of every run and on
/// startup: a `claimed` or `deleting` unit is completed **before** new units
/// are planned.
pub fn ledger_redrive_sql(database: &str, limit: usize) -> String {
    format!(
        "SELECT reclaim_id, scope, source_host, source_name, source_file,\n \
          toUInt32(source_generation) AS source_generation, session_id,\n \
          toUInt64(candidate_generation) AS candidate_generation, phase,\n \
          toUInt64(estimated_rows) AS estimated_rows, toUInt64(estimated_bytes) AS estimated_bytes\n\
         FROM {}.{RECLAIM_LEDGER_TABLE} FINAL\n\
         WHERE phase IN ('claimed', 'deleting')\n\
         ORDER BY claimed_at ASC, reclaim_id ASC\n\
         LIMIT {limit}\n\
         FORMAT JSONEachRow",
        escape_identifier(database)
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

    /// The dry-run planner. **Writes nothing.**
    ///
    /// In this build every scope reports zero units with a note naming the
    /// work item that will register its probe: the candidate-set probe is
    /// executor-specific (an anti-join against headers for `McpOpenOrphan`, an
    /// anti-join against published heads for the other two) and lives with the
    /// executor it belongs to, so that the probe and the delete it authorizes
    /// are reviewed together rather than one release apart.
    pub async fn reclaim_plan(
        &self,
        retention: &RetentionConfig,
        scopes: &[ReclaimScope],
    ) -> Result<ReclaimPlan> {
        let ledger = self.reclaim_ledger_summary().await.unwrap_or_default();
        let scopes = scopes
            .iter()
            .map(|scope| self.plan_scope(*scope, retention))
            .collect();
        Ok(ReclaimPlan {
            scopes,
            denomination: estimated_bytes_note(),
            pending_redrive: ledger.needs_redrive(),
        })
    }

    fn plan_scope(&self, scope: ReclaimScope, retention: &RetentionConfig) -> ReclaimableEstimate {
        let note = match ReclaimAuthority::for_scope(scope, retention) {
            Err(missing) => Some(missing.to_string()),
            Ok(_) => Some(format!(
                "no candidate probe is registered for `{scope}` in this build; {} adds it",
                pending_work_item(scope)
            )),
        };
        ReclaimableEstimate {
            scope,
            units: 0,
            estimated_rows: 0,
            estimated_bytes: 0,
            tables: scope
                .tables()
                .iter()
                .map(|table| table.name().to_string())
                .collect(),
            note,
        }
    }

    /// `moraine db reclaim run`.
    ///
    /// Refuses every scope in this build: no executor is registered. The
    /// authority check still runs first, so a bucket-1/2 scope is refused for
    /// the *missing config key* rather than for the missing executor — the
    /// refusal an operator will still get once WI-09 lands.
    ///
    /// The `ReclaimAuthority::for_scope` call below is **the S2 enforcement
    /// point at the command boundary**, not a nicety for a better error
    /// message. Once an executor is registered, deleting that one line lets a
    /// run with no `[retention]` key proceed. It is guarded by
    /// `reclaim_run_refuses_an_unconfigured_canonical_scope_before_anything_else`,
    /// and the executor check below it by
    /// `reclaim_run_refuses_a_registered_scope_without_reaching_clickhouse` —
    /// both of which call this function, which
    /// `this_build_registers_no_executor` deliberately does not.
    ///
    /// This build reads no ledger and writes no claim; see the module docs on
    /// the descoped §3.2 driver.
    pub async fn reclaim_run(
        &self,
        retention: &RetentionConfig,
        scope: ReclaimScope,
    ) -> Result<ReclaimOutcome> {
        ReclaimAuthority::for_scope(scope, retention)?;
        if executor_for(scope).is_none() {
            return Ok(ReclaimOutcome::NoExecutor {
                scope,
                message: format!(
                    "no executor is registered for scope `{scope}`; {} adds it. Nothing was \
                     deleted.",
                    pending_work_item(scope)
                ),
            });
        }
        // Unreachable in this build; kept so the H9 signal path is not written
        // for the first time in the same PR as the first executor.
        let pending = self.reclaim_pending_mutations(scope).await?;
        if pending > 0 {
            return Ok(ReclaimOutcome::Blocked {
                scope,
                pending_mutations: pending,
            });
        }
        Ok(ReclaimOutcome::Idle { scope })
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
            PLAN_STATEMENT_CAP,
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
        statements.push(ledger_redrive_sql("moraine", 64));
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
        for table in [
            ReclaimTable::McpOpenEvents,
            ReclaimTable::McpOpenTurns,
            ReclaimTable::McpOpenPublicationHeaders,
        ] {
            assert_eq!(table.predicate(), ReclaimPredicate::SessionGeneration);
        }
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

        let redrive = ledger_redrive_sql("moraine", 64);
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

    /// Names only what it checks: the registry is empty. It deliberately does
    /// **not** call `reclaim_run`, so it is not the guard for anything inside
    /// that function — see the two `reclaim_run_*` tests below, which do.
    #[test]
    fn this_build_registers_no_executor() {
        assert!(registered_executors().is_empty());
        for scope in ReclaimScope::ALL {
            assert!(executor_for(scope).is_none(), "`{scope}` must not execute");
        }
    }

    /// **D1's ordering, as a gate rather than a promise.** An executor
    /// registered onto an unimplemented §3.2 driver deletes rows that nothing
    /// durable records: the crash the ledger exists to survive would strand
    /// exactly the children it exists to keep reachable.
    ///
    /// MUTATION (executed 2026-07-27): make `executor_for` return
    /// `Some(RegisteredExecutor { scope })` for `McpOpenOrphan` while
    /// `LEDGER_DRIVER_WIRED` stays `false` => FAILS here. That is the whole
    /// point: WI-05 must flip the constant in the same PR, which is the PR
    /// that has to make it true.
    #[test]
    fn no_executor_may_be_registered_before_the_ledger_driver_is_wired() {
        assert!(
            LEDGER_DRIVER_WIRED || registered_executors().is_empty(),
            "an executor is registered but `reclaim_run` still never claims, advances, or settles \
             a ledger unit. Wire the §3.2 driver and set LEDGER_DRIVER_WIRED, or unregister the \
             executor: {:?}",
            registered_executors()
        );
        // WI-04 ships neither. Stated as an equality so that flipping the
        // constant alone, without registering anything, is also a failing
        // test rather than a quiet lie.
        assert_eq!(
            (LEDGER_DRIVER_WIRED, registered_executors().len()),
            (false, 0),
            "WI-04 wires no §3.2 driver and registers no executor; WI-05 changes both, in one PR"
        );
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
    /// at 177/0, because `this_build_registers_no_executor` never called
    /// `reclaim_run` at all — it asserted over the registry.
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
    /// MUTATION (executed 2026-07-27): delete the
    /// `if executor_for(scope).is_none() { … }` block from `reclaim_run` =>
    /// FAILS here: control reaches `reclaim_pending_mutations`, the offline
    /// client cannot connect, and the call returns `Err` instead of
    /// `NoExecutor`. **Lower bound, and it is the bound that matters: with an
    /// authorized scope, that block is all that stands between a run and the
    /// server.**
    #[tokio::test(flavor = "multi_thread")]
    async fn reclaim_run_refuses_a_registered_scope_without_reaching_clickhouse() {
        let client = offline_client();
        // Fully authorized, so the authority check cannot be what refuses.
        let retention = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        };
        for scope in ReclaimScope::ALL {
            let outcome = client
                .reclaim_run(&retention, scope)
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
    #[test]
    fn byte_denominations_carry_their_qualifiers_and_promise_nothing() {
        let estimate = estimated_bytes_note();
        let reclaimed = reclaimed_bytes_note();
        assert!(estimate.contains(ESTIMATE_QUALIFIER));
        assert!(reclaimed.contains(MERGE_DEFERRED_QUALIFIER));
        assert!(reclaimed.contains("exact"), "row counts ARE deterministic");

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
}
