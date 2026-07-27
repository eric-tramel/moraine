//! Crash-resumable one-shot backfill for the migration-036 canonical read
//! indexes (issue #598 WI-03).
//!
//! Migration 036 installs `mcp_session_directory`, `mcp_event_locator`, and
//! `mcp_event_navigation` plus the three materialized views that maintain them
//! from every new `moraine.events` insert block. The MVs only cover blocks
//! inserted *after* they exist; this module sweeps the pre-existing corpus and
//! feeds the same three tables so the indexes become complete, then runs an
//! overlap audit and publishes readiness.
//!
//! ## Envelope discipline (BINDING D5)
//!
//! Each page runs under its OWN Migration-class batch [`QueryEnvelope`]
//! (`new_batch` with a derived statement cap), sequenced AFTER and OUTSIDE the
//! existing v1 `mcp_open` backfill envelope in `commands.rs` — never nested
//! inside it. The migration runner's per-statement doctrine (a single envelope
//! spanning a run would cap the SUM of statement times, a regression amendment
//! A5 forbids) applies here too: one envelope per page, so a budget-exceeded
//! page fails typed and retryable while the durable per-page cursor makes rerun
//! resumption page-exact.
//!
//! ## Overlap idempotency
//!
//! The sweep scans `events` WITHOUT `FINAL` and every derivation expression is
//! taken from the shared [`crate::canonical_derivations`] authority using the
//! real `events` column names, so a backfilled row is byte-identical to the row
//! the MV would produce for the same event. Overlap with concurrent live MV
//! writes is therefore idempotent by construction: `ReplacingMergeTree`
//! navigation/locator rows version-collapse on their identical primary keys and
//! `AggregatingMergeTree` directory bounds merge (min/max duplicate-insensitive;
//! `observed_events` is the documented hint-only over-counting counter).
//!
//! ## Readiness + the Local gate (BINDING D3)
//!
//! After the sweep and a passing overlap audit, `core_indexes.ready` is
//! published. `open_v2.ready` — the one-way `open` cutover flag consumers read —
//! is then auto-published ONLY for the default single-owner Local backend
//! (`publication_mode_is_local`). Shared/multi-writer backends leave `open_v2`
//! unpublished; promotion there is the explicit operator command (WI-05).

use serde::{Deserialize, Serialize};

use super::{escape_identifier, escape_literal, migration_request_timeout, ClickHouseClient};
use crate::canonical_derivations::{self as cd, DerivationColumns};
use crate::envelope::{QueryClass, QueryEnvelope};
use anyhow::{Context, Result};
use moraine_config::ValidatedQueryBudget;

/// State-table key for the coverage sweep cursor + coverage-ready flag.
pub const STATE_KEY_CORE_INDEXES: &str = "core_indexes";
/// State-table key for the post-sweep overlap-audit outcome.
pub const STATE_KEY_CORE_AUDIT: &str = "core_audit";
/// State-table key for the one-way `open` v2 cutover flag.
pub const STATE_KEY_OPEN_V2: &str = "open_v2";

/// Provenance recorded in `open_v2.cursor` when readiness is auto-published on
/// the Local backend (contrast the operator `promote` provenance below).
pub const OPEN_V2_PROVENANCE_AUTO_LOCAL: &str = "auto-local";

/// Provenance recorded in `open_v2.cursor` when readiness is published by the
/// explicit operator `moraine db core-index promote` command (BINDING D3): the
/// non-Local path for a Shared/multi-writer backend, or a re-promotion after a
/// rebuild.
pub const OPEN_V2_PROVENANCE_OPERATOR_PROMOTE: &str = "operator-promote";

/// Provenance recorded in `open_v2.cursor` (with `ready = 0`) when a passing
/// publish deliberately withholds `open_v2` because the backend is not the
/// default single-owner Local backend (BINDING D3). The marker distinguishes
/// "promotion is the operator's explicit next step" from a crash between the
/// `core_indexes` and `open_v2` readiness writes, so the idempotent
/// resume-gate re-evaluation completes only genuinely stranded Local
/// publications and never bypasses the Shared promote ceremony.
pub const OPEN_V2_PROVENANCE_WITHHELD_NON_LOCAL: &str = "withheld-non-local";

/// The three migration-036 index tables truncated by a `core-index rebuild`.
const CANONICAL_INDEX_TABLES: [&str; 3] = [
    "mcp_session_directory",
    "mcp_event_locator",
    "mcp_event_navigation",
];

/// Events swept per page. Each page reads at most `~PAGE_SIZE` rows per
/// statement (asserted per-page in the live tests, C2-R1); the durable cursor
/// advances by exactly one page.
pub const BACKFILL_PAGE_SIZE: u64 = 50_000;

// Per-page statement budget (BINDING D5). A page issues, at most:
//   1 boundary probe (locate the page end via keyset OFFSET),
// + 1 final-max probe   (only the last page, to bound its closed range),
// + 1 final-count query  (only the last page, exact remaining-row tally),
// + 3 INSERT .. SELECT   (directory, locator, navigation),
// + 1 cursor persist,
// = 7 statements on the final page, 5 on every other page. The cap adds margin
// for a readiness probe / retry. The arithmetic is asserted in the unit tests.
const PAGE_STMT_BOUNDARY_PROBE: u32 = 1;
const PAGE_STMT_FINAL_MAX_PROBE: u32 = 1;
const PAGE_STMT_FINAL_COUNT: u32 = 1;
const PAGE_STMT_INSERTS: u32 = 3;
const PAGE_STMT_CURSOR_PERSIST: u32 = 1;
const PAGE_STMT_MARGIN: u32 = 1;
/// Derived per-page statement cap fed to [`QueryEnvelope::new_batch`].
pub const PAGE_STATEMENT_CAP: u32 = PAGE_STMT_BOUNDARY_PROBE
    + PAGE_STMT_FINAL_MAX_PROBE
    + PAGE_STMT_FINAL_COUNT
    + PAGE_STMT_INSERTS
    + PAGE_STMT_CURSOR_PERSIST
    + PAGE_STMT_MARGIN;

/// Statement cap for the resume-state read phase: up to three
/// existence-probe + state-read pairs (`core_indexes`, `core_audit`,
/// `open_v2`) plus the stranded-publication completing write, with margin.
const RESUME_STATEMENT_CAP: u32 = 8;
/// Statement cap for the audit phase (sample fetch + coverage/cardinality
/// probes + audit persist).
const AUDIT_STATEMENT_CAP: u32 = 16;
/// Statement cap for the readiness-publication phase: a `core_indexes` state
/// read (existence probe + read = 2), an `open_v2` state read guarding the
/// non-local withhold marker, and the `core_indexes` and `open_v2` writes,
/// with margin.
const PUBLISH_STATEMENT_CAP: u32 = 8;

/// Sessions sampled by the overlap audit (randomly, so repeated audits do not
/// re-check the same fixed alphabetical prefix of the corpus).
const AUDIT_SESSION_SAMPLE: u64 = 128;
/// Events per coverage slice per sampled session. The audit checks TWO slices
/// of each session — its newest and its oldest events — because post-036 the
/// newest rows are exactly what the live MVs wrote: a newest-only sample could
/// pass without ever touching the range the backfill swept. The oldest slice
/// is the backfilled range for any session predating the migration.
const AUDIT_EVENTS_PER_SESSION: u64 = 256;
/// Memory ceiling for the audit anti-join probes (spills instead of blowing the
/// server memory cap; precedent `sql/032`).
const AUDIT_MAX_BYTES_IN_JOIN: u64 = 268_435_456;

/// The durable full-events-PK sweep cursor. Serialized as JSON into
/// `mcp_read_index_state('core_indexes').cursor` after every page and restored
/// on resume.
///
/// The tuple is the physical `events` primary key
/// (`sql/001_schema.sql:125`): `(session_id, event_ts, source_name,
/// source_file, source_generation, source_offset, source_line_no, event_uid)`.
/// `source_host` is deliberately NOT part of the keyset (a documented deviation
/// from the design's 9-tuple): the events `ReplacingMergeTree` treats this exact
/// 8-tuple as the row identity, so `source_host` is functionally determined by
/// it and adding it would only risk defeating the primary-key `KeyCondition`
/// granule pruning the keyset paging relies on. `event_ts` is carried as
/// integer milliseconds so the JSON round-trip is exact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreIndexCursor {
    pub session_id: String,
    pub event_ts_ms: i64,
    pub source_name: String,
    pub source_file: String,
    pub source_generation: u32,
    pub source_offset: u64,
    pub source_line_no: u64,
    pub event_uid: String,
}

impl CoreIndexCursor {
    /// The `events` primary-key column tuple, in keyset order. Reads the bare
    /// primary-key columns (no wrapping functions) so `KeyCondition` can prune
    /// granules on the keyset comparison.
    pub fn pk_columns() -> &'static str {
        "(session_id, event_ts, source_name, source_file, \
         source_generation, source_offset, source_line_no, event_uid)"
    }

    /// This cursor rendered as a comparable literal tuple. `event_ts` is
    /// reconstructed from integer milliseconds and cast back to the column's
    /// `DateTime64(3)` type so the tuple element types line up for comparison.
    fn literal_tuple(&self) -> String {
        format!(
            "({}, CAST(fromUnixTimestamp64Milli(toInt64({})) AS DateTime64(3)), {}, {}, {}, {}, {}, {})",
            escape_literal(&self.session_id),
            self.event_ts_ms,
            escape_literal(&self.source_name),
            escape_literal(&self.source_file),
            self.source_generation,
            self.source_offset,
            self.source_line_no,
            escape_literal(&self.event_uid),
        )
    }
}

/// One page's [`events`] keyset range: `(lower, upper]`, both over
/// [`CoreIndexCursor::pk_columns`]. `lower` is `None` for the first page.
struct PageRange<'a> {
    lower: Option<&'a CoreIndexCursor>,
    upper: &'a CoreIndexCursor,
}

impl PageRange<'_> {
    /// The `WHERE` predicate selecting exactly this page's non-blank-session
    /// events, PK-pruned via the keyset tuple comparison.
    fn predicate(&self) -> String {
        let cols = CoreIndexCursor::pk_columns();
        let mut predicate = format!(
            "notEmpty(session_id) AND {cols} <= {}",
            self.upper.literal_tuple()
        );
        if let Some(lower) = self.lower {
            predicate.push_str(&format!(" AND {cols} > {}", lower.literal_tuple()));
        }
        predicate
    }
}

/// Outcome of an overlap audit; persisted as JSON to
/// `mcp_read_index_state('core_audit').cursor`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CoreIndexAuditOutcome {
    pub passed: bool,
    pub sampled_sessions: u64,
    pub sampled_events: u64,
    pub navigation_missing: u64,
    pub locator_missing: u64,
    pub directory_missing_sessions: u64,
    pub navigation_locator_cardinality_delta: i64,
    /// Unix-milliseconds wall clock the audit completed at.
    pub completed_at_ms: i64,
}

/// Result of a backfill invocation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoreIndexBackfillOutcome {
    /// The sweep was already complete (`core_indexes.ready == 1`) on entry; no
    /// pages were run this invocation.
    pub already_complete: bool,
    /// Pages swept this invocation.
    pub pages: u64,
    /// Events indexed this invocation (exact; full pages contribute
    /// `BACKFILL_PAGE_SIZE`, the final page its counted remainder).
    pub events_indexed: u64,
    /// Overlap-audit outcome (only meaningful when the sweep ran to completion
    /// this invocation).
    pub audit: CoreIndexAuditOutcome,
    /// `core_indexes.ready` was set to 1 (sweep + audit passed).
    pub core_indexes_published: bool,
    /// `open_v2.ready` was set to 1 (the Local-gate auto-publish fired).
    pub open_v2_published: bool,
}

/// Per-page / phase progress for the CLI renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreIndexBackfillProgress {
    /// Sweep is starting (or resuming from a durable cursor).
    Starting { resuming: bool },
    /// A page finished; cumulative counters.
    PageIndexed { pages: u64, events_indexed: u64 },
    /// The overlap audit is running.
    Auditing,
    /// Readiness was published (or withheld). `open_v2` reflects the Local gate.
    Published { core_indexes: bool, open_v2: bool },
    /// Nothing to do: the sweep was already complete on entry.
    AlreadyComplete,
}

/// Result of an operator `open_v2` promotion attempt (WI-05).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OpenV2PromotionOutcome {
    /// The coverage sweep is complete (`core_indexes.ready == 1`).
    pub core_indexes_ready: bool,
    /// The persisted overlap audit passed.
    pub audit_passed: bool,
    /// `open_v2.ready` was already 1 before this call (idempotent no-op).
    pub already_promoted: bool,
    /// A coverage sweep is in flight (or crashed mid-sweep): `core_indexes`
    /// is not ready but its durable page cursor is persisted. Promotion is
    /// refused until the sweep (or a rerun of it) completes.
    pub backfill_in_flight: bool,
    /// This call published `open_v2.ready = 1` with the operator provenance.
    pub promoted: bool,
}

/// A materialized `mcp_read_index_state` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexState {
    pub state_key: String,
    pub ready: u8,
    pub generation: u64,
    pub cursor: String,
}

// --- JSONEachRow row shapes -----------------------------------------------
//
// 64-bit integer columns are quoted as strings by ClickHouse's default
// `output_format_json_quote_64bit_integers`, so they are read as `String` and
// parsed. `UInt32`/`UInt8` render as JSON numbers and deserialize directly.

#[derive(Debug, Deserialize)]
struct PkBoundaryRow {
    session_id: String,
    event_ts_ms: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    // Suffixed aliases: bare `source_offset`/`source_line_no` SELECT aliases
    // would shadow the physical PK columns inside the same statement's
    // WHERE/ORDER BY, silently turning the keyset tuple comparison and the
    // boundary ordering into STRING comparisons (caught live by the
    // canonical-open-bench multi-page backfill).
    source_offset_str: String,
    source_line_no_str: String,
    event_uid: String,
}

impl PkBoundaryRow {
    fn into_cursor(self) -> Result<CoreIndexCursor> {
        Ok(CoreIndexCursor {
            session_id: self.session_id,
            event_ts_ms: self
                .event_ts_ms
                .parse()
                .context("failed to parse core-index cursor event_ts")?,
            source_name: self.source_name,
            source_file: self.source_file,
            source_generation: self.source_generation,
            source_offset: self
                .source_offset_str
                .parse()
                .context("failed to parse core-index cursor source_offset")?,
            source_line_no: self
                .source_line_no_str
                .parse()
                .context("failed to parse core-index cursor source_line_no")?,
            event_uid: self.event_uid,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawStateRow {
    state_key: String,
    ready: u8,
    generation: String,
    #[serde(default)]
    cursor: String,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize)]
struct SessionIdRow {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct AuditCoverageRow {
    #[serde(default)]
    sampled_events: String,
    #[serde(default)]
    navigation_missing: String,
    #[serde(default)]
    locator_missing: String,
}

#[derive(Debug, Deserialize)]
struct CardinalityRow {
    #[serde(default)]
    delta: String,
}

impl ClickHouseClient {
    /// Crash-resumable one-shot backfill of the migration-036 canonical read
    /// indexes, followed by the overlap audit and readiness publication
    /// (issue #598 WI-03).
    ///
    /// `publication_mode_is_local` gates the auto-publish of `open_v2.ready`
    /// (BINDING D3): pass the `PublicationConsistencyMode::Local` predicate
    /// resolved from config. The migrate/up path only ever targets the default
    /// single-owner local backend, so it passes `true`; a hypothetical
    /// shared-target migrate passes `false` and leaves `open_v2` unpublished for
    /// the explicit operator `promote` command.
    ///
    /// Every phase runs inside its own Migration-class envelope built from
    /// `migration_budget` / `admin_budget`; the per-page envelope uses
    /// [`PAGE_STATEMENT_CAP`]. This call MUST be sequenced after and OUTSIDE the
    /// v1 `mcp_open` backfill envelope.
    pub async fn backfill_canonical_read_indexes<F>(
        &self,
        publication_mode_is_local: bool,
        migration_budget: &ValidatedQueryBudget,
        admin_budget: &ValidatedQueryBudget,
        mut on_progress: F,
    ) -> Result<CoreIndexBackfillOutcome>
    where
        F: FnMut(CoreIndexBackfillProgress),
    {
        let mut outcome = CoreIndexBackfillOutcome::default();

        // Resume state: short-circuit when the sweep already completed, but
        // first re-evaluate the publication gate idempotently (a crash between
        // the `core_indexes` and `open_v2` readiness writes must not strand
        // the Local cutover forever behind this short-circuit).
        let action = QueryEnvelope::new_batch(
            "core-index-resume",
            QueryClass::Migration,
            migration_budget,
            admin_budget,
            RESUME_STATEMENT_CAP,
        )
        .scope(self.resume_action(publication_mode_is_local))
        .await?;
        let mut cursor: Option<CoreIndexCursor> = match action {
            ResumeAction::AlreadyComplete {
                open_v2_republished,
            } => {
                outcome.already_complete = true;
                outcome.open_v2_published = open_v2_republished;
                if open_v2_republished {
                    on_progress(CoreIndexBackfillProgress::Published {
                        core_indexes: true,
                        open_v2: true,
                    });
                } else {
                    on_progress(CoreIndexBackfillProgress::AlreadyComplete);
                }
                return Ok(outcome);
            }
            ResumeAction::Sweep { cursor } => cursor,
        };
        on_progress(CoreIndexBackfillProgress::Starting {
            resuming: cursor.is_some(),
        });

        // Keyset sweep: one Migration-class batch envelope per page.
        loop {
            let page = QueryEnvelope::new_batch(
                "core-index-backfill-page",
                QueryClass::Migration,
                migration_budget,
                admin_budget,
                PAGE_STATEMENT_CAP,
            )
            .scope(self.sweep_one_page(cursor.as_ref()))
            .await?;

            let Some(page) = page else {
                break;
            };
            outcome.pages += 1;
            outcome.events_indexed += page.events_indexed;
            cursor = Some(page.new_cursor);
            on_progress(CoreIndexBackfillProgress::PageIndexed {
                pages: outcome.pages,
                events_indexed: outcome.events_indexed,
            });
            if page.final_page {
                break;
            }
        }

        // Overlap audit.
        on_progress(CoreIndexBackfillProgress::Auditing);
        let audit = QueryEnvelope::new_batch(
            "core-index-audit",
            QueryClass::Migration,
            migration_budget,
            admin_budget,
            AUDIT_STATEMENT_CAP,
        )
        .scope(self.run_overlap_audit())
        .await?;
        outcome.audit = audit.clone();

        // Readiness publication (sweep + audit passed) and the Local-gated
        // open_v2 auto-publish.
        let (core_published, open_v2_published) = QueryEnvelope::new_batch(
            "core-index-publish",
            QueryClass::Migration,
            migration_budget,
            admin_budget,
            PUBLISH_STATEMENT_CAP,
        )
        .scope(self.publish_readiness(&audit, publication_mode_is_local))
        .await?;
        outcome.core_indexes_published = core_published;
        outcome.open_v2_published = open_v2_published;
        on_progress(CoreIndexBackfillProgress::Published {
            core_indexes: core_published,
            open_v2: open_v2_published,
        });

        Ok(outcome)
    }

    /// Read the durable resume state and decide this invocation's course.
    ///
    /// On the already-complete path the readiness publication gate is
    /// re-derived from the persisted audit (idempotent re-evaluation, BINDING
    /// D3): `core_indexes.ready` and the Local `open_v2` auto-publish are two
    /// separate statements, so a crash between them leaves the sweep complete
    /// with `open_v2` stranded at 0. That stranded publication is completed
    /// here. The explicit non-local withhold marker
    /// ([`OPEN_V2_PROVENANCE_WITHHELD_NON_LOCAL`]) is respected: for a
    /// Shared-ceremony backend, promotion stays the operator's explicit step.
    ///
    /// Assumes an active query envelope ([`RESUME_STATEMENT_CAP`]).
    async fn resume_action(&self, publication_mode_is_local: bool) -> Result<ResumeAction> {
        let Some(state) = self.read_index_state(STATE_KEY_CORE_INDEXES).await? else {
            return Ok(ResumeAction::Sweep { cursor: None });
        };
        if state.ready != 1 {
            let cursor = if state.cursor.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str(&state.cursor)
                        .context("failed to decode persisted core-index backfill cursor")?,
                )
            };
            return Ok(ResumeAction::Sweep { cursor });
        }

        let audit_passed = self
            .core_index_audit_outcome()
            .await?
            .is_some_and(|audit| audit.passed);
        let plan = readiness_publication_plan(audit_passed, publication_mode_is_local);
        if !plan.publish_open_v2 {
            return Ok(ResumeAction::AlreadyComplete {
                open_v2_republished: false,
            });
        }
        let stranded = match self.read_index_state(STATE_KEY_OPEN_V2).await? {
            None => true,
            Some(row) if row.ready == 1 => false,
            Some(row) => row.cursor != OPEN_V2_PROVENANCE_WITHHELD_NON_LOCAL,
        };
        if !stranded {
            return Ok(ResumeAction::AlreadyComplete {
                open_v2_republished: false,
            });
        }
        self.write_index_state(STATE_KEY_OPEN_V2, true, OPEN_V2_PROVENANCE_AUTO_LOCAL)
            .await?;
        Ok(ResumeAction::AlreadyComplete {
            open_v2_republished: true,
        })
    }

    /// Read the latest `mcp_read_index_state` row for `state_key`, or `None`
    /// when the table is missing or the row is unseeded. Assumes an active
    /// query envelope (accessor for WI-05/WI-08 — the caller scopes it).
    ///
    /// A missing table returns `None` rather than erroring: an operator status
    /// read or a repository readiness probe can run against a database that has
    /// not applied migration 036 yet. The existence pre-check is required
    /// because ClickHouse fails to plan a query whose `FROM` names an absent
    /// table regardless of any guard in the `WHERE`.
    pub async fn read_index_state(&self, state_key: &str) -> Result<Option<ReadIndexState>> {
        if !self.read_index_state_table_exists().await? {
            return Ok(None);
        }
        let query = format!(
            "SELECT state_key, ready, toString(generation) AS generation, cursor\n\
             FROM {db}.mcp_read_index_state FINAL\n\
             WHERE state_key = {key}\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            db = escape_identifier(&self.cfg.database),
            key = escape_literal(state_key),
        );
        let rows: Vec<RawStateRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .with_context(|| format!("failed to read mcp_read_index_state('{state_key}')"))?;
        rows.into_iter()
            .next()
            .map(|row| {
                Ok(ReadIndexState {
                    state_key: row.state_key,
                    ready: row.ready,
                    generation: row
                        .generation
                        .parse()
                        .context("failed to parse mcp_read_index_state generation")?,
                    cursor: row.cursor,
                })
            })
            .transpose()
    }

    async fn read_index_state_table_exists(&self) -> Result<bool> {
        let query = format!(
            "SELECT toString(count()) AS value\n\
             FROM system.tables\n\
             WHERE database = {db_lit} AND name = 'mcp_read_index_state'\n\
             FORMAT JSONEachRow",
            db_lit = escape_literal(&self.cfg.database),
        );
        let rows: Vec<CountRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to probe for the mcp_read_index_state table")?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.value.parse::<u64>().ok())
            .unwrap_or(0)
            >= 1)
    }

    /// Whether the canonical read-index coverage sweep has completed
    /// (`core_indexes.ready == 1`). Assumes an active query envelope.
    pub async fn canonical_read_indexes_ready(&self) -> Result<bool> {
        Ok(self
            .read_index_state(STATE_KEY_CORE_INDEXES)
            .await?
            .is_some_and(|state| state.ready == 1))
    }

    /// Whether the one-way v2 `open` reader has been published
    /// (`open_v2.ready == 1`). Assumes an active query envelope.
    pub async fn open_v2_reader_ready(&self) -> Result<bool> {
        Ok(self
            .read_index_state(STATE_KEY_OPEN_V2)
            .await?
            .is_some_and(|state| state.ready == 1))
    }

    /// The persisted overlap-audit outcome, if any. Assumes an active query
    /// envelope.
    pub async fn core_index_audit_outcome(&self) -> Result<Option<CoreIndexAuditOutcome>> {
        let Some(state) = self.read_index_state(STATE_KEY_CORE_AUDIT).await? else {
            return Ok(None);
        };
        if state.cursor.is_empty() {
            return Ok(None);
        }
        let outcome = serde_json::from_str(&state.cursor)
            .context("failed to decode persisted core-index audit outcome")?;
        Ok(Some(outcome))
    }

    /// Explicit operator promotion of the one-way `open_v2` reader flag (WI-05,
    /// BINDING D3). This is the sanctioned path for a Shared/multi-writer
    /// backend — where the Local auto-gate deliberately withholds `open_v2` —
    /// and for re-promotion after a `rebuild`.
    ///
    /// Promotion is only published when the coverage sweep is complete
    /// (`core_indexes.ready == 1`) AND the persisted overlap audit passed; the
    /// reader must never be switched onto an unaudited index. When those
    /// preconditions are not met the returned outcome reports them and nothing
    /// is written — including while a backfill sweep is in flight
    /// (`core_indexes.ready == 0` with a persisted page cursor).
    /// Already-promoted is an idempotent success. The caller is responsible
    /// for the operator confirmation that every consumer is v2-capable before
    /// invoking this (a Shared promotion switches ALL readers).
    ///
    /// ClickHouse offers no transactions, so the preconditions are re-verified
    /// immediately AFTER the publish write: a concurrent `rebuild` reset zeroes
    /// the readiness rows around this write, and the freshly published row's
    /// newer snowflake generation would otherwise survive that reset and keep
    /// `open_v2 = 1` over truncated, unaudited indexes. When the re-check fails
    /// the published row is revoked and the call errors.
    ///
    /// Assumes an active query envelope (the caller scopes it).
    pub async fn promote_open_v2_reader(&self) -> Result<OpenV2PromotionOutcome> {
        let core_indexes = self.read_index_state(STATE_KEY_CORE_INDEXES).await?;
        let mut outcome = OpenV2PromotionOutcome {
            core_indexes_ready: core_indexes.as_ref().is_some_and(|state| state.ready == 1),
            audit_passed: self
                .core_index_audit_outcome()
                .await?
                .is_some_and(|audit| audit.passed),
            already_promoted: self.open_v2_reader_ready().await?,
            backfill_in_flight: core_indexes
                .as_ref()
                .is_some_and(|state| state.ready == 0 && !state.cursor.is_empty()),
            promoted: false,
        };

        if outcome.already_promoted {
            return Ok(outcome);
        }
        if !outcome.core_indexes_ready || !outcome.audit_passed {
            // Preconditions unmet: leave open_v2 at 0, report why.
            return Ok(outcome);
        }

        self.write_index_state(STATE_KEY_OPEN_V2, true, OPEN_V2_PROVENANCE_OPERATOR_PROMOTE)
            .await?;

        // Post-write re-verification (see the doc comment). Also confirm the
        // published row itself is still visible: a concurrent non-local publish
        // withhold could have overwritten it with the marker row.
        let still_ready = self.canonical_read_indexes_ready().await?;
        let still_audited = self
            .core_index_audit_outcome()
            .await?
            .is_some_and(|audit| audit.passed);
        let still_published = self.open_v2_reader_ready().await?;
        if !still_ready || !still_audited || !still_published {
            self.write_index_state(STATE_KEY_OPEN_V2, false, "").await?;
            anyhow::bail!(
                "open_v2 promotion aborted: core-index readiness changed while promoting \
                 (a `core-index rebuild` reset is likely in flight); the published row was \
                 revoked — re-run `moraine db core-index promote --force` after the rebuild \
                 completes"
            );
        }
        outcome.promoted = true;
        Ok(outcome)
    }

    /// Reset the migration-036 canonical read indexes to a pre-backfill state
    /// (WI-05 `core-index rebuild`): reset the `open_v2`, `core_audit`, and
    /// `core_indexes` readiness rows to `ready = 0` with an empty cursor, then
    /// truncate the three index tables. A subsequent
    /// [`Self::backfill_canonical_read_indexes`] then re-sweeps from scratch.
    ///
    /// Every statement here is a separate HTTP request with no transaction, so
    /// crash safety comes from the write ordering: readiness is revoked BEFORE
    /// any destructive statement, `open_v2` (the consumer cutover flag) first.
    /// A crash at any boundary leaves the state reporting not-ready over
    /// intact-or-empty tables — never ready-but-empty — and the next backfill
    /// re-sweeps (replays are idempotent by this module's overlap argument).
    /// The RMT(generation) engine keeps the newest write, so the zeros win
    /// until the rerun republishes.
    ///
    /// Truncation uses `IF EXISTS` so an operator can safely run it against a
    /// database that has not applied migration 036 yet. Assumes an active query
    /// envelope.
    pub async fn reset_canonical_read_indexes(&self) -> Result<()> {
        for state_key in [
            STATE_KEY_OPEN_V2,
            STATE_KEY_CORE_AUDIT,
            STATE_KEY_CORE_INDEXES,
        ] {
            self.write_index_state(state_key, false, "").await?;
        }
        for table in CANONICAL_INDEX_TABLES {
            self.truncate_index_table(table).await?;
        }
        Ok(())
    }

    async fn truncate_index_table(&self, table: &str) -> Result<()> {
        let statement = format!(
            "TRUNCATE TABLE IF EXISTS {db}.{table}",
            db = escape_identifier(&self.cfg.database),
            table = escape_identifier(table),
        );
        self.mutation_request_text_with_params_and_timeout(
            &statement,
            None,
            Some(&self.cfg.database),
            &[],
            Some(migration_request_timeout(self.cfg.timeout_seconds)),
        )
        .await
        .with_context(|| format!("failed to truncate {table} during core-index rebuild"))?;
        Ok(())
    }

    /// Sweep one page under the active per-page envelope. Returns `None` when no
    /// events remain past `cursor` (sweep complete).
    async fn sweep_one_page(&self, cursor: Option<&CoreIndexCursor>) -> Result<Option<SweptPage>> {
        // Locate the page end: the BACKFILL_PAGE_SIZE-th event past the cursor.
        let boundary = self.probe_page_boundary(cursor).await?;
        let (page_end, final_page) = match boundary {
            Some(page_end) => (page_end, false),
            None => {
                // Fewer than a full page remains: the final page ends at the
                // corpus max past the cursor.
                match self.probe_final_max(cursor).await? {
                    Some(max) => (max, true),
                    None => return Ok(None),
                }
            }
        };

        let range = PageRange {
            lower: cursor,
            upper: &page_end,
        };
        let predicate = range.predicate();

        self.execute_index_insert(self.directory_insert_sql(&predicate))
            .await?;
        self.execute_index_insert(self.locator_insert_sql(&predicate))
            .await?;
        self.execute_index_insert(self.navigation_insert_sql(&predicate))
            .await?;

        let events_indexed = if final_page {
            self.count_range(&predicate).await?
        } else {
            BACKFILL_PAGE_SIZE
        };

        self.persist_core_index_cursor(&page_end).await?;

        Ok(Some(SweptPage {
            new_cursor: page_end,
            events_indexed,
            final_page,
        }))
    }

    /// The tuple of the page-boundary event (`OFFSET PAGE_SIZE-1`), or `None`
    /// when fewer than a full page of events remain past `cursor`.
    async fn probe_page_boundary(
        &self,
        cursor: Option<&CoreIndexCursor>,
    ) -> Result<Option<CoreIndexCursor>> {
        let query = format!(
            "{select}\n{where_clause}\nORDER BY {order}\nLIMIT 1 OFFSET {offset}\nFORMAT JSONEachRow",
            select = self.pk_boundary_select(),
            where_clause = self.pk_lower_where(cursor),
            order = PK_ORDER_ASC,
            offset = BACKFILL_PAGE_SIZE - 1,
        );
        let rows: Vec<PkBoundaryRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to probe core-index backfill page boundary")?;
        rows.into_iter()
            .next()
            .map(PkBoundaryRow::into_cursor)
            .transpose()
    }

    /// The tuple of the last event past `cursor` (corpus max), or `None` when
    /// none remain.
    async fn probe_final_max(
        &self,
        cursor: Option<&CoreIndexCursor>,
    ) -> Result<Option<CoreIndexCursor>> {
        let query = format!(
            "{select}\n{where_clause}\nORDER BY {order}\nLIMIT 1\nFORMAT JSONEachRow",
            select = self.pk_boundary_select(),
            where_clause = self.pk_lower_where(cursor),
            order = PK_ORDER_DESC,
        );
        let rows: Vec<PkBoundaryRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to probe core-index backfill final page")?;
        rows.into_iter()
            .next()
            .map(PkBoundaryRow::into_cursor)
            .transpose()
    }

    /// Exact count of events in a page range predicate.
    async fn count_range(&self, predicate: &str) -> Result<u64> {
        let query = format!(
            "SELECT toString(count()) AS value\nFROM {db}.events\nWHERE {predicate}\nFORMAT JSONEachRow",
            db = escape_identifier(&self.cfg.database),
        );
        let rows: Vec<CountRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to count core-index backfill page rows")?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.value.parse().ok())
            .unwrap_or(0))
    }

    fn pk_boundary_select(&self) -> String {
        format!(
            "SELECT\n\
             session_id,\n\
             toString(toUnixTimestamp64Milli(event_ts)) AS event_ts_ms,\n\
             source_name,\n\
             source_file,\n\
             source_generation,\n\
             toString(source_offset) AS source_offset_str,\n\
             toString(source_line_no) AS source_line_no_str,\n\
             event_uid\n\
             FROM {db}.events",
            db = escape_identifier(&self.cfg.database),
        )
    }

    /// The `WHERE` clause for the boundary probes: non-blank sessions past the
    /// optional keyset lower bound.
    fn pk_lower_where(&self, cursor: Option<&CoreIndexCursor>) -> String {
        match cursor {
            Some(cursor) => format!(
                "WHERE notEmpty(session_id) AND {cols} > {lit}",
                cols = CoreIndexCursor::pk_columns(),
                lit = cursor.literal_tuple(),
            ),
            None => "WHERE notEmpty(session_id)".to_string(),
        }
    }

    fn directory_insert_sql(&self, predicate: &str) -> String {
        let cols = DerivationColumns::EVENTS;
        format!(
            "INSERT INTO {db}.mcp_session_directory\n\
             (session_id, source_host, source_name, source_file, source_generation, harness,\n\
              mode_hint, min_observed_event_time, max_observed_event_time, observed_events, origin_cwd_state)\n\
             SELECT\n\
             session_id, source_host, source_name, source_file, source_generation, harness,\n\
             max({mode_rank}) AS mode_hint,\n\
             min({display}) AS min_observed_event_time,\n\
             max({display}) AS max_observed_event_time,\n\
             count() AS observed_events,\n\
             argMinIfState(cwd, tuple(event_ts, event_uid), cwd != '') AS origin_cwd_state\n\
             FROM {db}.events\n\
             WHERE {predicate}\n\
             GROUP BY session_id, source_host, source_name, source_file, source_generation, harness",
            db = escape_identifier(&self.cfg.database),
            mode_rank = cd::mode_rank_expr(cols),
            display = cd::DISPLAY_TIME_EXPR,
        )
    }

    fn locator_insert_sql(&self, predicate: &str) -> String {
        format!(
            "INSERT INTO {db}.mcp_event_locator\n\
             (event_uid, source_host, event_version, session_id, source_name, source_file,\n\
              source_generation, source_offset, source_line_no, sort_time)\n\
             SELECT\n\
             event_uid, source_host, event_version, session_id, source_name, source_file,\n\
             source_generation, source_offset, source_line_no,\n\
             {sort_time} AS sort_time\n\
             FROM {db}.events\n\
             WHERE {predicate}",
            db = escape_identifier(&self.cfg.database),
            sort_time = cd::SORT_TIME_EXPR,
        )
    }

    fn navigation_insert_sql(&self, predicate: &str) -> String {
        let cols = DerivationColumns::EVENTS;
        format!(
            "INSERT INTO {db}.mcp_event_navigation\n\
             (session_id, sort_time, source_host, source_file, source_generation, source_offset,\n\
              source_line_no, event_uid, event_version, source_name, event_ts, display_time,\n\
              event_kind, actor_kind, payload_type, turn_index, tool_call_id, tool_name, tool_phase,\n\
              op_status, item_id, harness, inference_provider, cwd, is_user_message, is_metadata_bearing)\n\
             SELECT\n\
             session_id, {sort_time} AS sort_time, source_host, source_file, source_generation, source_offset,\n\
             source_line_no, event_uid, event_version, source_name, event_ts, {display} AS display_time,\n\
             event_kind, actor_kind, payload_type, turn_index, tool_call_id, tool_name, tool_phase,\n\
             op_status, item_id, harness, inference_provider, cwd,\n\
             toUInt8({is_user_message}) AS is_user_message,\n\
             toUInt8({is_metadata_bearing}) AS is_metadata_bearing\n\
             FROM {db}.events\n\
             WHERE {predicate}",
            db = escape_identifier(&self.cfg.database),
            sort_time = cd::SORT_TIME_EXPR,
            display = cd::DISPLAY_TIME_EXPR,
            is_user_message = cd::user_message_count_predicate(cols),
            is_metadata_bearing = cd::is_metadata_bearing_predicate(cols),
        )
    }

    async fn execute_index_insert(&self, statement: String) -> Result<()> {
        // Carry the SQL in the POST body so a wide INSERT .. SELECT is bounded
        // by the payload limit, not URI parsing limits (v1 backfill idiom).
        self.mutation_request_text_with_params_and_timeout(
            "",
            Some(statement.into_bytes()),
            Some(&self.cfg.database),
            &[],
            Some(migration_request_timeout(self.cfg.timeout_seconds)),
        )
        .await
        .context("failed to insert a core-index backfill page")?;
        Ok(())
    }

    async fn persist_core_index_cursor(&self, cursor: &CoreIndexCursor) -> Result<()> {
        let payload =
            serde_json::to_string(cursor).context("failed to encode core-index backfill cursor")?;
        self.write_index_state(STATE_KEY_CORE_INDEXES, false, &payload)
            .await
    }

    /// Insert a fresh `mcp_read_index_state` row (RMT(generation) with a
    /// snowflake version so the newest write wins). Assumes an active envelope.
    async fn write_index_state(&self, state_key: &str, ready: bool, cursor: &str) -> Result<()> {
        let statement = format!(
            "INSERT INTO {db}.mcp_read_index_state\n\
             (state_key, ready, generation, cursor)\n\
             VALUES ({key}, {ready}, generateSnowflakeID(), {cursor})",
            db = escape_identifier(&self.cfg.database),
            key = escape_literal(state_key),
            ready = u8::from(ready),
            cursor = escape_literal(cursor),
        );
        self.mutation_request_text_with_params_and_timeout(
            &statement,
            None,
            Some(&self.cfg.database),
            &[],
            None,
        )
        .await
        .with_context(|| format!("failed to write mcp_read_index_state('{state_key}')"))?;
        Ok(())
    }

    /// Bounded overlap audit: samples each sampled session's newest AND oldest
    /// events (the oldest slice covers the backfilled range, which post-036 MV
    /// writes never touch) and confirms the locator, navigation, and directory
    /// indexes cover them, plus per-session navigation-vs-locator cardinality
    /// agreement. Sessions with fewer than two slices of events contribute the
    /// same rows to both slices; the missing counters gate on zero-vs-nonzero,
    /// so the double count is harmless.
    async fn run_overlap_audit(&self) -> Result<CoreIndexAuditOutcome> {
        let mut outcome = CoreIndexAuditOutcome::default();

        let sessions = self.audit_sample_sessions().await?;
        outcome.sampled_sessions = sessions.len() as u64;
        if sessions.is_empty() {
            // Empty corpus: coverage is trivially complete.
            outcome.passed = true;
            outcome.completed_at_ms = now_unix_millis();
            self.persist_audit_outcome(&outcome).await?;
            return Ok(outcome);
        }

        let session_list = sessions
            .iter()
            .map(|id| escape_literal(id))
            .collect::<Vec<_>>()
            .join(", ");

        let newest = self
            .audit_coverage(&session_list, AuditSlice::Newest)
            .await?;
        let oldest = self
            .audit_coverage(&session_list, AuditSlice::Oldest)
            .await?;
        outcome.sampled_events = newest.0 + oldest.0;
        outcome.navigation_missing = newest.1 + oldest.1;
        outcome.locator_missing = newest.2 + oldest.2;
        outcome.directory_missing_sessions = self
            .audit_directory_missing(&sessions, &session_list)
            .await?;
        outcome.navigation_locator_cardinality_delta =
            self.audit_cardinality_delta(&session_list).await?;

        outcome.passed = outcome.navigation_missing == 0
            && outcome.locator_missing == 0
            && outcome.directory_missing_sessions == 0
            && outcome.navigation_locator_cardinality_delta == 0;
        outcome.completed_at_ms = now_unix_millis();
        self.persist_audit_outcome(&outcome).await?;
        Ok(outcome)
    }

    async fn audit_sample_sessions(&self) -> Result<Vec<String>> {
        let query = audit_sample_sessions_sql(&escape_identifier(&self.cfg.database));
        let rows: Vec<SessionIdRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to sample sessions for the core-index overlap audit")?;
        Ok(rows.into_iter().map(|row| row.session_id).collect())
    }

    /// Returns `(sampled_events, navigation_missing, locator_missing)` over one
    /// coverage slice (newest or oldest events) of the sampled sessions.
    async fn audit_coverage(
        &self,
        session_list: &str,
        slice: AuditSlice,
    ) -> Result<(u64, u64, u64)> {
        let query = audit_coverage_sql(&escape_identifier(&self.cfg.database), session_list, slice);
        let rows: Vec<AuditCoverageRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to probe core-index coverage")?;
        let row = rows.into_iter().next().unwrap_or(AuditCoverageRow {
            sampled_events: String::new(),
            navigation_missing: String::new(),
            locator_missing: String::new(),
        });
        Ok((
            row.sampled_events.parse().unwrap_or(0),
            row.navigation_missing.parse().unwrap_or(0),
            row.locator_missing.parse().unwrap_or(0),
        ))
    }

    /// Count of sampled sessions with no directory row.
    async fn audit_directory_missing(
        &self,
        sessions: &[String],
        session_list: &str,
    ) -> Result<u64> {
        let db = escape_identifier(&self.cfg.database);
        let query = format!(
            "SELECT toString(count()) AS value\n\
             FROM (\n\
               SELECT DISTINCT session_id FROM {db}.mcp_session_directory\n\
               WHERE session_id IN ({session_list})\n\
             )\n\
             FORMAT JSONEachRow",
        );
        let rows: Vec<CountRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to probe core-index directory coverage")?;
        let covered: u64 = rows
            .into_iter()
            .next()
            .and_then(|row| row.value.parse().ok())
            .unwrap_or(0);
        Ok((sessions.len() as u64).saturating_sub(covered))
    }

    /// Summed absolute per-session delta in DISTINCT `event_uid` between
    /// navigation and locator over the sampled sessions (0 means agreement).
    ///
    /// Counting ROWS here was wrong in two independent ways, and the two
    /// together made the gate unreachable on any real corpus:
    ///
    /// * `mcp_event_navigation` keeps one row per generation (its sort key
    ///   carries `source_generation`) while `mcp_event_locator` keeps one per
    ///   uid, so a replayed source made the row counts differ permanently even
    ///   though both indexes agreed on which events exist.
    /// * `event_uid` is content-addressed and excludes `session_id` (#608), so
    ///   an ingest double-attribution puts one uid under two sessions.
    ///   Navigation keeps both rows; the locator collapses them. On the
    ///   reference host that is 19,846 uids, i.e. a permanent delta of 19,846
    ///   against a gate that requires exactly 0.
    ///
    /// Distinct uids is the invariant the reader actually depends on: the two
    /// indexes must agree on WHICH events exist, not on how many physical rows
    /// encode them. A genuinely missing index row still moves this number.
    async fn audit_cardinality_delta(&self, session_list: &str) -> Result<i64> {
        let query = Self::audit_cardinality_delta_sql(&self.cfg.database, session_list);
        let rows: Vec<CardinalityRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await
            .context("failed to probe navigation/locator cardinality agreement")?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.delta.parse().ok())
            .unwrap_or(0))
    }

    pub(crate) fn audit_cardinality_delta_sql(database: &str, session_list: &str) -> String {
        let db = escape_identifier(database);
        let query = format!(
            "SELECT toString(sum(abs(nav_count - loc_count))) AS delta\n\
             FROM (\n\
               SELECT session_id, uniqExact(event_uid) AS nav_count\n\
               FROM {db}.mcp_event_navigation FINAL\n\
               WHERE session_id IN ({session_list})\n\
               GROUP BY session_id\n\
             ) AS n\n\
             FULL OUTER JOIN (\n\
               SELECT session_id, uniqExact(event_uid) AS loc_count\n\
               FROM {db}.mcp_event_locator FINAL\n\
               WHERE session_id IN ({session_list})\n\
               GROUP BY session_id\n\
             ) AS l USING (session_id)\n\
             SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32,\n\
                      max_bytes_in_join = {join_bytes}\n\
             FORMAT JSONEachRow",
            join_bytes = AUDIT_MAX_BYTES_IN_JOIN,
        );
        query
    }

    async fn persist_audit_outcome(&self, outcome: &CoreIndexAuditOutcome) -> Result<()> {
        let payload =
            serde_json::to_string(outcome).context("failed to encode core-index audit outcome")?;
        self.write_index_state(STATE_KEY_CORE_AUDIT, outcome.passed, &payload)
            .await
    }

    /// Publish `core_indexes.ready=1` when the audit passed, then the
    /// Local-gated `open_v2.ready=1`. Returns `(core_published, open_v2_published)`.
    async fn publish_readiness(
        &self,
        audit: &CoreIndexAuditOutcome,
        publication_mode_is_local: bool,
    ) -> Result<(bool, bool)> {
        let plan = readiness_publication_plan(audit.passed, publication_mode_is_local);
        if !plan.publish_core_indexes {
            // Leave every readiness flag at 0: the reader never silently falls
            // back to an unaudited index.
            return Ok((false, false));
        }

        // Preserve the durable sweep cursor while flipping the ready flag.
        let cursor = self
            .read_index_state(STATE_KEY_CORE_INDEXES)
            .await?
            .map(|state| state.cursor)
            .unwrap_or_default();
        self.write_index_state(STATE_KEY_CORE_INDEXES, true, &cursor)
            .await?;

        // BINDING D3: only the default single-owner Local backend auto-flips the
        // one-way open_v2 consumer flag. Shared backends require the explicit
        // operator promote command (WI-05).
        if plan.publish_open_v2 {
            self.write_index_state(STATE_KEY_OPEN_V2, true, OPEN_V2_PROVENANCE_AUTO_LOCAL)
                .await?;
        } else if !self.open_v2_reader_ready().await? {
            // Non-local withhold marker: distinguishes "promotion is the
            // operator's explicit step" from a crash-stranded Local
            // auto-publish, so the resume-gate re-evaluation never completes
            // a publication the Shared ceremony deliberately withheld. Never
            // written over a published row (a concurrent promote wins).
            self.write_index_state(
                STATE_KEY_OPEN_V2,
                false,
                OPEN_V2_PROVENANCE_WITHHELD_NON_LOCAL,
            )
            .await?;
        }
        Ok((plan.publish_core_indexes, plan.publish_open_v2))
    }
}

/// The readiness-publication decision (BINDING D3), factored out so the Local
/// gate is unit-testable without a live backend.
///
/// `core_indexes` publishes iff the overlap audit passed; `open_v2` additionally
/// requires the default single-owner Local backend. A failed audit publishes
/// nothing (no silent fallback to an unaudited index).
struct ReadinessPublicationPlan {
    publish_core_indexes: bool,
    publish_open_v2: bool,
}

fn readiness_publication_plan(
    audit_passed: bool,
    publication_mode_is_local: bool,
) -> ReadinessPublicationPlan {
    ReadinessPublicationPlan {
        publish_core_indexes: audit_passed,
        publish_open_v2: audit_passed && publication_mode_is_local,
    }
}

/// Which end of a sampled session the coverage probe reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditSlice {
    /// The most-recent events: post-036 these are the live-MV-written rows.
    Newest,
    /// The oldest events: the backfilled range for sessions predating 036.
    Oldest,
}

impl AuditSlice {
    fn order_direction(self) -> &'static str {
        match self {
            Self::Newest => "DESC",
            Self::Oldest => "ASC",
        }
    }
}

/// The overlap-audit session sample query. Random sampling: a deterministic
/// (e.g. alphabetical) prefix would re-audit the same fixed corpus subset on
/// every run and could structurally miss defects confined to the rest.
fn audit_sample_sessions_sql(db: &str) -> String {
    format!(
        "SELECT session_id\n\
         FROM (SELECT DISTINCT session_id FROM {db}.events WHERE notEmpty(session_id)\n\
               ORDER BY rand() LIMIT {sample})\n\
         FORMAT JSONEachRow",
        sample = AUDIT_SESSION_SAMPLE,
    )
}

/// The overlap-audit coverage probe for one slice of the sampled sessions.
///
/// `loc_present` is scoped by UID, not by session, and that distinction is
/// load-bearing. `mcp_event_locator` is keyed `(event_uid, source_host)` — one
/// row per uid — while `event_uid` is content-addressed and excludes
/// `session_id` (#608), so an ingest double-attribution puts one uid under two
/// sessions. The locator keeps a single row carrying ONE of those sessions.
/// Filtering the locator by the sampled session list therefore reports the
/// other session's event as missing from an index that is not supposed to
/// carry it per session, and no corpus containing a double-attributed uid can
/// ever pass. Asking "is this uid in the locator at all" is the question the
/// reader's uid seek actually depends on; a genuinely absent uid still fails.
fn audit_coverage_sql(db: &str, session_list: &str, slice: AuditSlice) -> String {
    let dir = slice.order_direction();
    format!(
        "SELECT\n\
         toString(count()) AS sampled_events,\n\
         toString(countIf(NOT nav_present)) AS navigation_missing,\n\
         toString(countIf(NOT loc_present)) AS locator_missing\n\
         FROM (\n\
           SELECT event_uid, source_host,\n\
             (event_uid, source_host) IN (\n\
               SELECT event_uid, source_host FROM {db}.mcp_event_navigation\n\
               WHERE session_id IN ({session_list})) AS nav_present,\n\
             event_uid IN (\n\
               SELECT event_uid FROM {db}.mcp_event_locator\n\
               WHERE event_uid IN (\n\
                 SELECT event_uid FROM {db}.mcp_event_navigation\n\
                 WHERE session_id IN ({session_list}))) AS loc_present\n\
           FROM {db}.events\n\
           WHERE notEmpty(session_id) AND session_id IN ({session_list})\n\
           ORDER BY session_id, event_ts {dir}, source_offset {dir}, source_line_no {dir}, event_uid {dir}\n\
           LIMIT {per_session} BY session_id\n\
         )\n\
         SETTINGS max_bytes_in_join = {join_bytes}\n\
         FORMAT JSONEachRow",
        per_session = AUDIT_EVENTS_PER_SESSION,
        join_bytes = AUDIT_MAX_BYTES_IN_JOIN,
    )
}

/// Decision for a backfill invocation after reading the durable resume state.
enum ResumeAction {
    /// Sweep already complete. `open_v2_republished` reports whether this
    /// invocation completed a stranded Local auto-publish (a previous run
    /// crashed between the `core_indexes` and `open_v2` readiness writes).
    AlreadyComplete { open_v2_republished: bool },
    /// Sweep from `cursor` (`None` sweeps from the corpus start).
    Sweep { cursor: Option<CoreIndexCursor> },
}

/// One swept page's result.
struct SweptPage {
    new_cursor: CoreIndexCursor,
    events_indexed: u64,
    final_page: bool,
}

/// Ascending keyset order over the events primary key.
const PK_ORDER_ASC: &str = "session_id ASC, event_ts ASC, source_name ASC, source_file ASC, \
     source_generation ASC, source_offset ASC, source_line_no ASC, event_uid ASC";
/// Descending keyset order (final-page corpus-max probe).
const PK_ORDER_DESC: &str = "session_id DESC, event_ts DESC, source_name DESC, source_file DESC, \
     source_generation DESC, source_offset DESC, source_line_no DESC, event_uid DESC";

fn now_unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {

    /// The overlap audit must compare DISTINCT uids, never row counts.
    ///
    /// MUTATION: change either `uniqExact(event_uid)` back to `count()`; this
    /// fails. Row counts made the gate unreachable on any real corpus — a
    /// replayed generation gives navigation more rows than the locator, and an
    /// ingest double-attribution (one uid under two sessions, 19,846 of them on
    /// the reference host) gives a permanent nonzero delta against a gate that
    /// requires exactly 0. Distinct uids is what the reader depends on: the two
    /// indexes must agree on WHICH events exist.
    #[test]
    fn overlap_audit_compares_distinct_uids_not_row_counts() {
        let sql = ClickHouseClient::audit_cardinality_delta_sql("moraine", "'sess-a'");

        assert_eq!(
            sql.matches("uniqExact(event_uid)").count(),
            2,
            "both sides of the audit must count distinct uids:\n{sql}"
        );
        assert!(
            !sql.contains("count() AS nav_count") && !sql.contains("count() AS loc_count"),
            "a row-count side makes the gate unreachable on a replayed or \
             double-attributed corpus:\n{sql}"
        );
        assert!(
            sql.contains("mcp_event_navigation") && sql.contains("mcp_event_locator"),
            "the audit must still compare the two indexes:\n{sql}"
        );
    }

    use super::*;

    #[test]
    fn page_statement_cap_covers_the_worst_case_page() {
        // The final page issues the most statements: boundary probe + final-max
        // probe + final count + 3 INSERTs + cursor persist = 7.
        let worst_case_final_page = PAGE_STMT_BOUNDARY_PROBE
            + PAGE_STMT_FINAL_MAX_PROBE
            + PAGE_STMT_FINAL_COUNT
            + PAGE_STMT_INSERTS
            + PAGE_STMT_CURSOR_PERSIST;
        assert_eq!(worst_case_final_page, 7);
        assert!(PAGE_STATEMENT_CAP > worst_case_final_page);
        assert_eq!(PAGE_STATEMENT_CAP, 8);
    }

    #[test]
    fn cursor_json_round_trips_exactly() {
        let cursor = CoreIndexCursor {
            session_id: "sess-1".to_string(),
            event_ts_ms: 1_726_000_123_456,
            source_name: "claude".to_string(),
            source_file: "/home/u/.claude/projects/x/session.jsonl".to_string(),
            source_generation: 3,
            source_offset: 9_876_543_210,
            source_line_no: 4242,
            event_uid: "evt-abc".to_string(),
        };
        let encoded = serde_json::to_string(&cursor).expect("encode");
        let decoded: CoreIndexCursor = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(cursor, decoded);
    }

    #[test]
    fn cursor_literal_tuple_reconstructs_the_datetime_and_escapes_strings() {
        let cursor = CoreIndexCursor {
            session_id: "s'1".to_string(),
            event_ts_ms: 1_726_000_123_456,
            source_name: "claude".to_string(),
            source_file: "f".to_string(),
            source_generation: 2,
            source_offset: 10,
            source_line_no: 20,
            event_uid: "u".to_string(),
        };
        let literal = cursor.literal_tuple();
        // event_ts rebuilt from ms and cast back to the column type.
        assert!(literal
            .contains("CAST(fromUnixTimestamp64Milli(toInt64(1726000123456)) AS DateTime64(3))"));
        // String elements are escaped (single quote backslash-escaped).
        assert!(literal.contains("'s\\'1'"));
        // Integer PK columns render bare.
        assert!(literal.contains(", 2, 10, 20, "));
    }

    #[test]
    fn page_range_predicate_is_two_sided_after_the_first_page() {
        let upper = CoreIndexCursor {
            session_id: "z".to_string(),
            event_ts_ms: 2,
            source_name: "n".to_string(),
            source_file: "f".to_string(),
            source_generation: 0,
            source_offset: 0,
            source_line_no: 0,
            event_uid: "e2".to_string(),
        };
        let lower = CoreIndexCursor {
            session_id: "a".to_string(),
            event_ts_ms: 1,
            source_name: "n".to_string(),
            source_file: "f".to_string(),
            source_generation: 0,
            source_offset: 0,
            source_line_no: 0,
            event_uid: "e1".to_string(),
        };

        let first_page = PageRange {
            lower: None,
            upper: &upper,
        }
        .predicate();
        assert!(first_page.contains("notEmpty(session_id)"));
        assert!(first_page.contains("<="));
        assert!(!first_page.contains(" > "));

        let later_page = PageRange {
            lower: Some(&lower),
            upper: &upper,
        }
        .predicate();
        assert!(later_page.contains("<="));
        assert!(later_page.contains(" > "));
        // Both bounds compare the same primary-key column tuple.
        assert_eq!(later_page.matches(CoreIndexCursor::pk_columns()).count(), 2);
    }

    #[test]
    fn insert_sql_uses_shared_derivations_and_real_event_columns() {
        // Build a client-independent view of the SQL via the fragment authority
        // (the INSERT builders are methods on the client, but the derivation
        // expressions they embed are what must not drift).
        let cols = DerivationColumns::EVENTS;
        let nav_flag = cd::user_message_count_predicate(cols);
        assert_eq!(nav_flag, "actor_kind = 'user' AND event_kind = 'message'");
        assert!(cd::SORT_TIME_EXPR.contains("toDateTime64('1970-01-01 00:00:00', 3)"));
        assert!(cd::DISPLAY_TIME_EXPR.contains("ingested_at"));
        // The navigation is_metadata_bearing flag uses the real event_kind
        // column, not the projector alias.
        assert!(cd::is_metadata_bearing_predicate(cols).starts_with("event_kind = 'session_meta'"));
    }

    #[test]
    fn local_gate_publishes_open_v2_only_on_the_local_backend() {
        // Audit passed on the Local backend: both flags publish.
        let local = readiness_publication_plan(true, true);
        assert!(local.publish_core_indexes);
        assert!(local.publish_open_v2);

        // Audit passed on a Shared/multi-writer backend: core indexes publish
        // but open_v2 stays 0 (operator promote required, WI-05).
        let shared = readiness_publication_plan(true, false);
        assert!(shared.publish_core_indexes);
        assert!(!shared.publish_open_v2);

        // Failed audit publishes nothing, regardless of backend.
        for is_local in [true, false] {
            let failed = readiness_publication_plan(false, is_local);
            assert!(!failed.publish_core_indexes);
            assert!(!failed.publish_open_v2);
        }
    }

    #[test]
    fn audit_outcome_json_round_trips() {
        let outcome = CoreIndexAuditOutcome {
            passed: true,
            sampled_sessions: 12,
            sampled_events: 3072,
            navigation_missing: 0,
            locator_missing: 0,
            directory_missing_sessions: 0,
            navigation_locator_cardinality_delta: 0,
            completed_at_ms: 1_726_000_000_000,
        };
        let encoded = serde_json::to_string(&outcome).expect("encode");
        let decoded: CoreIndexAuditOutcome = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(outcome, decoded);
    }

    #[test]
    fn audit_samples_randomly_and_covers_both_session_ends() {
        // Random session sampling: a deterministic alphabetical prefix would
        // re-audit the same fixed subset on every run.
        assert!(audit_sample_sessions_sql("moraine").contains("ORDER BY rand()"));

        // Post-036 the newest rows per session are exactly what the live MVs
        // wrote; only the oldest slice exercises the backfilled range. Both
        // slices must be probed.
        let newest = audit_coverage_sql("moraine", "'s1'", AuditSlice::Newest);
        assert!(newest.contains("event_ts DESC"));
        assert!(newest.contains("event_uid DESC"));
        let oldest = audit_coverage_sql("moraine", "'s1'", AuditSlice::Oldest);
        assert!(oldest.contains("event_ts ASC"));
        assert!(oldest.contains("event_uid ASC"));
    }

    // --- scripted-transport tests (state-machine crash ordering) ------------

    use crate::envelope::{test_budget, with_test_envelope};
    use axum::extract::{Query as AxumQuery, State as AxumState};
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Scripted `mcp_read_index_state` transport double: serves state reads
    /// from an in-memory row map, applies state INSERTs back onto that map,
    /// and records every mutation statement in arrival order (the crash-safety
    /// contract under test is exactly that ordering).
    #[derive(Clone, Default)]
    struct StateMock {
        rows: Arc<Mutex<HashMap<String, (u8, String)>>>,
        statements: Arc<Mutex<Vec<String>>>,
        /// Simulate a concurrent `rebuild` reset racing a promote: as soon as
        /// the `open_v2` publish INSERT lands, `core_indexes` reads flip to
        /// `ready = 0`.
        zero_core_after_open_v2_publish: bool,
    }

    impl StateMock {
        fn set_row(&self, state_key: &str, ready: u8, cursor: &str) {
            self.rows
                .lock()
                .expect("row mutex")
                .insert(state_key.to_string(), (ready, cursor.to_string()));
        }

        fn statements(&self) -> Vec<String> {
            self.statements.lock().expect("statement mutex").clone()
        }
    }

    /// `('key', ready, generateSnowflakeID(), 'cursor')` from a state INSERT.
    fn parse_state_write(statement: &str) -> Option<(String, u8, String)> {
        let values = statement.split("VALUES (").nth(1)?;
        let mut parts = values.splitn(4, ", ");
        let key = parts.next()?.trim_matches('\'').to_string();
        let ready = parts.next()?.parse().ok()?;
        let _generation = parts.next()?;
        let cursor = parts
            .next()?
            .trim_end_matches(')')
            .trim_matches('\'')
            .to_string();
        Some((key, ready, cursor))
    }

    async fn state_mock_handler(
        AxumState(mock): AxumState<StateMock>,
        AxumQuery(params): AxumQuery<HashMap<String, String>>,
    ) -> (StatusCode, String) {
        let query = params.get("query").cloned().unwrap_or_default();

        if query.starts_with("INSERT INTO") || query.starts_with("TRUNCATE") {
            mock.statements
                .lock()
                .expect("statement mutex")
                .push(query.clone());
            if let Some((key, ready, cursor)) = parse_state_write(&query) {
                let publish_landed = key == STATE_KEY_OPEN_V2 && ready == 1;
                mock.rows
                    .lock()
                    .expect("row mutex")
                    .insert(key, (ready, cursor));
                if publish_landed && mock.zero_core_after_open_v2_publish {
                    mock.rows
                        .lock()
                        .expect("row mutex")
                        .insert(STATE_KEY_CORE_INDEXES.to_string(), (0, String::new()));
                }
            }
            return (StatusCode::OK, String::new());
        }
        if query.contains("FROM system.tables") {
            return (StatusCode::OK, "{\"value\":\"1\"}\n".to_string());
        }
        if query.contains("mcp_read_index_state") {
            let Some(key) = query
                .split("state_key = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
            else {
                return (StatusCode::OK, String::new());
            };
            let row = mock.rows.lock().expect("row mutex").get(key).cloned();
            return match row {
                Some((ready, cursor)) => (
                    StatusCode::OK,
                    format!(
                        "{}\n",
                        serde_json::json!({
                            "state_key": key,
                            "ready": ready,
                            "generation": "123",
                            "cursor": cursor,
                        })
                    ),
                ),
                None => (StatusCode::OK, String::new()),
            };
        }
        (StatusCode::OK, String::new())
    }

    async fn spawn_state_mock(mock: StateMock) -> String {
        let app = Router::new()
            .route("/", post(state_mock_handler).get(state_mock_handler))
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind state mock listener");
        let addr = listener.local_addr().expect("state mock listener addr");
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
            async_insert: true,
            wait_for_async_insert: true,
            allow_newer_server: false,
        })
        .expect("mock client")
    }

    fn passed_audit_json() -> String {
        serde_json::to_string(&CoreIndexAuditOutcome {
            passed: true,
            ..CoreIndexAuditOutcome::default()
        })
        .expect("encode audit")
    }

    #[tokio::test]
    async fn reset_revokes_readiness_before_truncating() {
        let mock = StateMock::default();
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        with_test_envelope(client.reset_canonical_read_indexes())
            .await
            .expect("reset");

        // Crash ordering: all three ready=0 rows land BEFORE the first
        // destructive statement, and open_v2 (the consumer flag) lands first.
        let statements = mock.statements();
        assert_eq!(statements.len(), 6, "statements: {statements:#?}");
        assert!(statements[0].contains("'open_v2', 0"));
        assert!(statements[1].contains("'core_audit', 0"));
        assert!(statements[2].contains("'core_indexes', 0"));
        for statement in &statements[3..] {
            assert!(
                statement.starts_with("TRUNCATE TABLE"),
                "expected truncate after the readiness zeros: {statement}"
            );
        }
    }

    #[tokio::test]
    async fn backfill_completes_a_stranded_local_open_v2_publication() {
        // A previous run crashed between the core_indexes publish and the
        // Local open_v2 auto-publish: sweep complete, audit passed, open_v2
        // stranded at 0 with no withhold marker.
        let mock = StateMock::default();
        mock.set_row(STATE_KEY_CORE_INDEXES, 1, "{}");
        mock.set_row(STATE_KEY_CORE_AUDIT, 1, &passed_audit_json());
        mock.set_row(STATE_KEY_OPEN_V2, 0, "");
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        let budget = test_budget(30.0, 64, 1_000_000, 1_000_000_000);
        let mut events = Vec::new();
        let outcome = client
            .backfill_canonical_read_indexes(true, &budget, &budget, |event| events.push(event))
            .await
            .expect("backfill");

        assert!(outcome.already_complete);
        assert!(outcome.open_v2_published, "stranded publication completed");
        let statements = mock.statements();
        assert_eq!(statements.len(), 1, "statements: {statements:#?}");
        assert!(statements[0].contains("'open_v2', 1"));
        assert!(statements[0].contains(OPEN_V2_PROVENANCE_AUTO_LOCAL));
        assert!(events.contains(&CoreIndexBackfillProgress::Published {
            core_indexes: true,
            open_v2: true,
        }));
    }

    #[tokio::test]
    async fn backfill_already_complete_respects_the_non_local_withhold_marker() {
        // The open_v2 row carries the deliberate Shared-ceremony withhold
        // marker: the resume gate must NOT complete the publication even when
        // invoked with the Local publication mode.
        let mock = StateMock::default();
        mock.set_row(STATE_KEY_CORE_INDEXES, 1, "{}");
        mock.set_row(STATE_KEY_CORE_AUDIT, 1, &passed_audit_json());
        mock.set_row(STATE_KEY_OPEN_V2, 0, OPEN_V2_PROVENANCE_WITHHELD_NON_LOCAL);
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        let budget = test_budget(30.0, 64, 1_000_000, 1_000_000_000);
        let outcome = client
            .backfill_canonical_read_indexes(true, &budget, &budget, |_| {})
            .await
            .expect("backfill");

        assert!(outcome.already_complete);
        assert!(!outcome.open_v2_published);
        assert!(mock.statements().is_empty(), "no writes on withheld state");
    }

    #[tokio::test]
    async fn backfill_already_complete_is_a_noop_when_open_v2_is_published() {
        let mock = StateMock::default();
        mock.set_row(STATE_KEY_CORE_INDEXES, 1, "{}");
        mock.set_row(STATE_KEY_CORE_AUDIT, 1, &passed_audit_json());
        mock.set_row(STATE_KEY_OPEN_V2, 1, OPEN_V2_PROVENANCE_AUTO_LOCAL);
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        let budget = test_budget(30.0, 64, 1_000_000, 1_000_000_000);
        let outcome = client
            .backfill_canonical_read_indexes(true, &budget, &budget, |_| {})
            .await
            .expect("backfill");

        assert!(outcome.already_complete);
        assert!(!outcome.open_v2_published);
        assert!(mock.statements().is_empty());
    }

    #[tokio::test]
    async fn promote_refuses_while_a_backfill_sweep_is_in_flight() {
        // ready = 0 with a persisted page cursor: a sweep is mid-flight (or
        // crashed mid-flight); promotion must refuse and say so.
        let mock = StateMock::default();
        mock.set_row(STATE_KEY_CORE_INDEXES, 0, "{\"session_id\":\"s\"}");
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        let outcome = with_test_envelope(client.promote_open_v2_reader())
            .await
            .expect("promote");

        assert!(!outcome.promoted);
        assert!(!outcome.core_indexes_ready);
        assert!(outcome.backfill_in_flight);
        assert!(mock.statements().is_empty(), "refusal writes nothing");
    }

    #[tokio::test]
    async fn promote_revokes_its_publish_when_a_reset_races_it() {
        // Preconditions read clean, but a concurrent rebuild reset zeroes
        // core_indexes around the publish write. The post-write re-check must
        // revoke the freshly published row (whose newer snowflake would
        // otherwise survive the reset) and fail loudly.
        let mock = StateMock {
            zero_core_after_open_v2_publish: true,
            ..StateMock::default()
        };
        mock.set_row(STATE_KEY_CORE_INDEXES, 1, "{}");
        mock.set_row(STATE_KEY_CORE_AUDIT, 1, &passed_audit_json());
        mock.set_row(STATE_KEY_OPEN_V2, 0, "");
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        let error = with_test_envelope(client.promote_open_v2_reader())
            .await
            .expect_err("racing reset must abort the promotion");
        assert!(
            error.to_string().contains("promotion aborted"),
            "unexpected error: {error:#}"
        );

        let statements = mock.statements();
        assert_eq!(statements.len(), 2, "statements: {statements:#?}");
        assert!(statements[0].contains("'open_v2', 1"));
        assert!(statements[0].contains(OPEN_V2_PROVENANCE_OPERATOR_PROMOTE));
        assert!(statements[1].contains("'open_v2', 0"));
    }

    #[tokio::test]
    async fn promote_publishes_when_preconditions_hold_through_the_write() {
        let mock = StateMock::default();
        mock.set_row(STATE_KEY_CORE_INDEXES, 1, "{}");
        mock.set_row(STATE_KEY_CORE_AUDIT, 1, &passed_audit_json());
        mock.set_row(STATE_KEY_OPEN_V2, 0, "");
        let client = mock_client(spawn_state_mock(mock.clone()).await);

        let outcome = with_test_envelope(client.promote_open_v2_reader())
            .await
            .expect("promote");

        assert!(outcome.promoted);
        assert!(!outcome.already_promoted);
        assert!(!outcome.backfill_in_flight);
        let statements = mock.statements();
        assert_eq!(statements.len(), 1, "statements: {statements:#?}");
        assert!(statements[0].contains("'open_v2', 1"));
        assert!(statements[0].contains(OPEN_V2_PROVENANCE_OPERATOR_PROMOTE));
    }
}
