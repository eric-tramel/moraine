//! Issue #603 WI-01 — the ownership model of [`plans/603-reclamation.md` §1]
//! encoded as code rather than documented in a table.
//!
//! This module is the **safety foundation** of #603: it decides what may ever
//! be deleted automatically. Nothing here deletes anything; everything that
//! does must route its table names through [`classify`] first.
//!
//! ## Why a total function with no default arm (§4 S1)
//!
//! The failure mode this exists to prevent is: a future migration adds a
//! table, nobody classifies it, a scope glob sweeps it up, and the operator
//! discovers it when the rows are gone. So:
//!
//! * [`classify`] returns `Option<TableClass>`. `None` means *unknown*, and
//!   unknown is a hard error in the planner — never a fallthrough to
//!   "probably derived".
//! * [`CLASSIFIED_TABLES`] and [`REQUIRED_SCHEMA_OBJECTS`] are asserted
//!   **mutually exhaustive** by [`classification_gaps`], which the unit test
//!   `classification_and_required_schema_objects_are_mutually_exhaustive`
//!   drives. Adding a table to the schema without classifying it fails that
//!   test; classifying a table that the schema handshake does not require
//!   fails it too.
//!
//! ## Why the classes are not the four spec buckets verbatim
//!
//! The spec names four buckets; this enum has five variants. The extra one is
//! [`TableClass::NeverDelete`], which carries the *Auto = never* rows of
//! buckets 2 and 3 — publication truth, revision allocators, the cache fence,
//! the readiness fence. Those are not "raw audit data an operator may
//! configure a retention for"; they are control state whose deletion breaks
//! ingest or silently changes query results, so they get a class no authority
//! token can unlock rather than a bucket with a configurable horizon.

use std::collections::BTreeSet;

use crate::REQUIRED_SCHEMA_OBJECTS;

/// Storage ownership class of one physical table (issue #603 §1).
///
/// The class is the *authority* axis, not the *scope* axis: it says what kind
/// of permission a delete naming this table needs, not whether any scope
/// currently targets it. A table can be [`TableClass::Derived`] and still be
/// unreachable by default because no default scope names it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TableClass {
    /// Bucket 1. Retained by default; deletion requires explicit user config.
    CanonicalHistory,
    /// Bucket 2 with a configurable retention. Never silently shortened.
    RawAudit,
    /// Bucket 3. Rebuildable derived data, eligible for automatic reclamation
    /// once the replacement/cutover it derives from is verified.
    Derived,
    /// Bucket 4. Operational telemetry, bounded by safe default TTLs.
    Telemetry,
    /// No code path may ever delete it. Publication truth, revision
    /// allocators, control fences, and the migration ledger.
    NeverDelete,
}

impl TableClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CanonicalHistory => "canonical_history",
            Self::RawAudit => "raw_audit",
            Self::Derived => "derived",
            Self::Telemetry => "telemetry",
            Self::NeverDelete => "never_delete",
        }
    }

    /// Human-facing bucket label used by the storage report and CLI.
    pub fn label(self) -> &'static str {
        match self {
            Self::CanonicalHistory => "canonical user history",
            Self::RawAudit => "raw / audit source data",
            Self::Derived => "rebuildable derived data",
            Self::Telemetry => "operational telemetry",
            Self::NeverDelete => "control state (never deleted)",
        }
    }

    /// Whether a `DELETE`/`TRUNCATE` naming a table of this class requires an
    /// explicit [`crate::reclaim::ReclaimAuthority`] token (§4 S3), and
    /// whether a bundled migration may contain one at all (§4 S4).
    ///
    /// `Derived` and `Telemetry` are the two classes stock configuration may
    /// reclaim; the other three are protected.
    pub fn is_protected(self) -> bool {
        matches!(
            self,
            Self::CanonicalHistory | Self::RawAudit | Self::NeverDelete
        )
    }

    /// Every class, for exhaustive folds. Adding a variant without adding it
    /// here fails `every_class_is_enumerated`.
    pub const ALL: [TableClass; 5] = [
        Self::CanonicalHistory,
        Self::RawAudit,
        Self::Derived,
        Self::Telemetry,
        Self::NeverDelete,
    ];
}

/// One row of the §1 ownership table.
#[derive(Debug, Clone, Copy)]
pub struct ClassifiedTable {
    /// Unqualified table name inside the Moraine database, or the
    /// `system.`-qualified name of a ClickHouse system log.
    pub name: &'static str,
    pub class: TableClass,
    /// Why this class and not a looser one. Read this before reclassifying:
    /// several of these entries encode a failure that has already happened.
    pub rationale: &'static str,
}

/// ClickHouse's own system logs. Not Moraine tables, not part of the schema
/// handshake, bounded by the `<ttl>` entries in `config/clickhouse.xml`
/// (issue #603 WI-08).
///
/// Verified empirically on the reference host (ClickHouse 25.12.5.44,
/// 2026-07-27): all three carry **no** TTL, because `config/clickhouse.xml` is
/// rendered as the whole server config and launched with `--config-file`, so
/// the packaged `config.xml` that ships `event_date + INTERVAL 30 DAY DELETE`
/// is never loaded. `system.tables.create_table_query` contains no `TTL`
/// clause for any of the three — `metric_log`'s only match on the substring is
/// a *column* named `…OrTTLMicroseconds`.
///
/// This closes plan OQ-3, which required the answer before WI-08 could be
/// sized. §8 of the plan records it as RESOLVED with this evidence; the two
/// must not drift, because OQ-3 open would mean WI-08 — which ships here — was
/// sized against an unverified premise.
pub const CLICKHOUSE_SYSTEM_LOGS: &[&str] = &[
    "system.query_log",
    "system.metric_log",
    "system.asynchronous_metric_log",
];

/// Physical tables that exist in the Moraine database but are deliberately
/// **not** in [`REQUIRED_SCHEMA_OBJECTS`].
///
/// `file_attention_project_roots` is read at `file_attention.rs` yet was never
/// registered in the schema handshake. That is a pre-existing gap (#603 §1
/// bucket 3 note) and #603 deliberately does not fix it — a schema-handshake
/// change is a separate, separately revertible edit. It is classified here
/// anyway, because an unclassified table is exactly what this module exists to
/// make impossible.
///
/// This constant suppresses a *finding*, so it needs a width bound of its own.
/// For a round it did not have one: the doc claimed "the exhaustiveness test
/// names it explicitly so the exemption cannot silently grow", and the only
/// naming was a failure-message string. MUTATION (executed 2026-07-28): adding
/// `"events"` here left the whole crate GREEN, and so did adding
/// `"search_documents"` — an operator could have un-registered canonical
/// history from the schema handshake and no test would have said so. Its two
/// sibling constants were both already bounded ([`CLICKHOUSE_SYSTEM_LOGS`] by a
/// length assertion, [`SCHEMA_VIEW_OBJECTS`] by `stale_view_declarations`);
/// this was the one that was not.
/// `the_unregistered_table_exemption_is_exactly_one_table` is the bound.
pub const UNREGISTERED_PHYSICAL_TABLES: &[&str] = &["file_attention_project_roots"];

/// [`REQUIRED_SCHEMA_OBJECTS`] entries that are views or materialized views
/// rather than physical tables, and therefore hold no bytes and need no class.
///
/// Named explicitly rather than sniffed from a `v_`/`mv_` prefix: migration
/// 032 installs `search_term_stats` and `search_corpus_stats` as plain
/// `CREATE VIEW`s under names that carry no prefix, so a prefix heuristic
/// would classify two views as unclassified tables and a future *table* named
/// `v_something` as a view.
pub const SCHEMA_VIEW_OBJECTS: &[&str] = &[
    "search_term_stats",
    "search_corpus_stats",
    "mv_mcp_session_directory_from_events",
    "mv_mcp_event_locator_from_events",
    "mv_mcp_event_navigation_from_events",
    "v_published_source_generation_history",
    "v_current_published_source_generations",
    "v_current_ingest_checkpoint_transitions",
    "v_current_source_generation_publication_readiness",
    "v_current_ingest_append_control",
    "v_live_events",
    "v_live_event_links",
    "v_live_tool_io",
    "v_live_search_documents",
    "v_live_search_postings",
    "v_mcp_open_publication_headers",
    "v_current_mcp_open_generation_readiness",
    "v_publication_diagnostics",
];

/// The §1 ownership model. Every physical table in the Moraine database plus
/// the three ClickHouse system logs.
///
/// Verified against the reference host on 2026-07-27: `system.tables` reports
/// exactly 32 non-view relations in `moraine`, and this list carries those 32
/// plus `storage_reclaim_ledger` (added by migration 038) and the 3 system
/// logs.
pub const CLASSIFIED_TABLES: &[ClassifiedTable] = &[
    // ---- Bucket 1 — canonical user history -------------------------------
    ClassifiedTable {
        name: "events",
        class: TableClass::CanonicalHistory,
        rationale: "The canonical record. `ingested_at` is deliberately excluded from the sort \
                    key so FINAL dedups across monthly partitions (sql/019), which means a \
                    replayed generation lands in the current month while its predecessor stays \
                    in an old month interleaved with unrelated live sources: no partition-level \
                    operation is ever safe here.",
    },
    // ---- Bucket 2 — raw / audit source data ------------------------------
    ClassifiedTable {
        name: "raw_events",
        class: TableClass::RawAudit,
        rationale: "`raw_json` is the complete untruncated source record and has zero runtime \
                    readers, which makes it the largest pure-audit candidate — and is exactly \
                    why it stays opt-in: bucket 2 is never silently shortened.",
    },
    ClassifiedTable {
        name: "ingest_errors",
        class: TableClass::RawAudit,
        rationale: "Untruncated 20 000-char `raw_fragment` of what failed to parse. No readers, \
                    but it is the only evidence an operator has after a bad ingest.",
    },
    // ---- Bucket 2/3 — control state that may never be deleted ------------
    ClassifiedTable {
        name: "published_source_generations",
        class: TableClass::NeverDelete,
        rationale: "Publication truth. As-of reads reconstruct heads under \
                    `publication_revision <= R`; a request pinned at r_old <= R < r_new loses \
                    the source entirely from an ALL INNER JOIN, so deleting a superseded row is \
                    a silent mid-request disappearance, not a stale answer.",
    },
    ClassifiedTable {
        name: "ingest_checkpoint_transitions",
        class: TableClass::NeverDelete,
        rationale: "Resume cursor AND a monotone allocator source: the next checkpoint revision \
                    is `max(checkpoint_revision) + 1` over the whole history for the host. The \
                    reference host's maximum is 1 784 588 514 188 (a wall-clock-ms seed), so \
                    deleting the row that holds the maximum restarts allocation near 1 and \
                    collides with thousands of live rows. `transition_revision` additionally \
                    looks rows up by `operation_id` across all history for retry idempotency, \
                    so even a time-based TTL breaks retry safety.",
    },
    ClassifiedTable {
        name: "source_generation_publication_readiness",
        class: TableClass::NeverDelete,
        rationale: "Same monotone-allocator hazard as `ingest_checkpoint_transitions`: the next \
                    readiness revision is derived from the maximum over all history.",
    },
    ClassifiedTable {
        name: "ingest_append_control",
        class: TableClass::NeverDelete,
        rationale: "One row per host; the cache fence. Startup hard-refuses to run without it, \
                    so deleting it does not degrade ingest, it stops it.",
    },
    ClassifiedTable {
        name: "publication_diagnostic_events",
        class: TableClass::NeverDelete,
        rationale: "Bounded by distinct (host, name, file, kind) and self-collapsing, so there \
                    is nothing to gain by deleting it and a diagnostic history to lose.",
    },
    ClassifiedTable {
        name: "ingest_checkpoints",
        class: TableClass::NeverDelete,
        rationale: "Looks like retired telemetry; is not. Still written by the sink and still \
                    read as legacy resume state.",
    },
    ClassifiedTable {
        name: "schema_migrations",
        class: TableClass::NeverDelete,
        rationale: "The migration ledger, created by the runner rather than by `sql/`. Deleting \
                    a row re-applies a migration.",
    },
    ClassifiedTable {
        name: "mcp_read_index_state",
        class: TableClass::NeverDelete,
        rationale: "Three control rows; the canonical-read-index readiness fence. Reset only \
                    via the explicit `core-index rebuild` path, never by reclamation.",
    },
    ClassifiedTable {
        name: "storage_reclaim_ledger",
        class: TableClass::NeverDelete,
        rationale: "The #603 reclaim ledger itself (migration 038). Restart safety is exactly \
                    the property that a claimed unit outlives the crash that interrupted it, so \
                    a reclaimer that could delete its own ledger would reintroduce the stranded- \
                    child bug it exists to fix. Settling advances `phase`, never deletes.",
    },
    ClassifiedTable {
        name: "search_conversation_terms",
        class: TableClass::NeverDelete,
        rationale: "Hazard H5. A SummingMergeTree accumulator with NO tombstone path: a delete \
                    never decrements, so any reclaim naming it leaves the corpus permanently \
                    overstated, and §1 marks it `never (pending OQ-1)` because OQ-1 records that \
                    removal `is not reversible without a corpus-wide re-tokenize`. Whether it is \
                    dropped or rebuilt is OQ-1 and blocks WI-09; until that is answered the safe \
                    class is the one no authority token can unlock, not the one the stock \
                    `DerivedOnly` token authorizes. Migration 010 truncates it on install and is \
                    allowlisted for exactly that.",
    },
    // ---- Bucket 3 — rebuildable derived data -----------------------------
    ClassifiedTable {
        name: "mcp_open_events",
        class: TableClass::Derived,
        rationale: "Legacy open projection. `candidate_generation` is in the sort key so \
                    ReplacingMergeTree can never collapse two candidate generations — the \
                    uncollapsible-generation mechanism behind the PR-604 disk-fill incident.",
    },
    ClassifiedTable {
        name: "mcp_open_turns",
        class: TableClass::Derived,
        rationale: "Same uncollapsible-generation property; the single largest table on the \
                    reference host, almost entirely `event_summaries_json`.",
    },
    ClassifiedTable {
        name: "mcp_open_publication_headers",
        class: TableClass::Derived,
        rationale: "One durable row per candidate publication per session; the authorization \
                    barrier a reader pins. Never collapses.",
    },
    ClassifiedTable {
        name: "mcp_open_sessions",
        class: TableClass::Derived,
        rationale: "Published per-session pointer for the v1 open engine. Collapses cleanly.",
    },
    ClassifiedTable {
        name: "mcp_open_generation_readiness",
        class: TableClass::Derived,
        rationale: "Grows as candidate-publications x sources.",
    },
    ClassifiedTable {
        name: "mcp_open_dirty_sessions",
        class: TableClass::Derived,
        rationale: "Projection debt queue; post-merge bounded by session count.",
    },
    ClassifiedTable {
        name: "mcp_open_backfill_plans",
        class: TableClass::Derived,
        rationale: "One row per session mid-backfill. A live plan row is also the exclusion \
                    that keeps orphan reclaim off a session currently being prepared.",
    },
    ClassifiedTable {
        name: "mcp_open_projection_state",
        class: TableClass::Derived,
        rationale: "Single 'global' row; the v1 projection readiness flag.",
    },
    ClassifiedTable {
        name: "mcp_session_directory",
        class: TableClass::Derived,
        rationale: "Canonical read index. `source_generation` is in the key, so one aggregate \
                    row set survives per generation forever. Content-free by design and fully \
                    rebuildable via `moraine db core-index rebuild`.",
    },
    ClassifiedTable {
        name: "mcp_event_locator",
        class: TableClass::Derived,
        rationale: "Canonical read index. `event_uid` embeds `source_generation`, so a \
                    superseded generation's uids are disjoint from the live one's and a \
                    generation predicate is exact. `session_id` on a locator row is NOT \
                    key-determined, so a locator delete must never be predicated on it.",
    },
    ClassifiedTable {
        name: "mcp_event_navigation",
        class: TableClass::Derived,
        rationale: "Canonical read index, explicitly content-free. `source_generation` is in \
                    the key.",
    },
    ClassifiedTable {
        name: "search_documents",
        class: TableClass::Derived,
        rationale: "Rebuildable from `events`, but ORDER BY (event_uid, source_host) alone \
                    means ONE physical row serves BOTH attributions of a double-attributed uid \
                    (19 846 of them on the reference host). Deleting 'the document for session \
                    X' deletes it for live session Y and silently changes BM25 df and scores, \
                    so no delete naming this table may be predicated on `session_id`.",
    },
    ClassifiedTable {
        name: "search_postings",
        class: TableClass::Derived,
        rationale: "83.6% of rows on the reference host carry `source_file=''` and \
                    `source_generation=0` — back-filled type defaults from migration 032, not \
                    real provenance. Predicating a posting delete on the posting's own \
                    generation deletes the entire live corpus; the only safe predicate joins \
                    through the document, mirroring `v_live_search_postings`.",
    },
    ClassifiedTable {
        name: "tool_io",
        class: TableClass::Derived,
        rationale: "Denormalized duplicate of tool payloads already in `events`. Has NO \
                    `source_file`/`source_generation` column at all, so any generation-shaped \
                    predicate is a compile-time-valid, runtime-wrong statement — its liveness \
                    is a uid set joined through `events`.",
    },
    ClassifiedTable {
        name: "event_links",
        class: TableClass::Derived,
        rationale: "Derived edge set with no readers, and the same missing-generation-column \
                    hazard as `tool_io`.",
    },
    ClassifiedTable {
        name: "file_attention_project_roots",
        class: TableClass::Derived,
        rationale: "Bounded by distinct (project_id, worktree_root) and rebuilt by its \
                    materialized views. Not registered in REQUIRED_SCHEMA_OBJECTS — see \
                    UNREGISTERED_PHYSICAL_TABLES.",
    },
    // ---- Bucket 4 — operational telemetry --------------------------------
    ClassifiedTable {
        name: "ingest_heartbeats",
        class: TableClass::Telemetry,
        rationale: "Only the latest row is ever read, but one row is written every 5 s per \
                    host. Time-partitioned, so a TTL genuinely drops whole parts.",
    },
    ClassifiedTable {
        name: "search_query_log",
        class: TableClass::Telemetry,
        rationale: "Single reader is a rolling 7-day hot-query window, so the default 30-day \
                    horizon carries 4x headroom over the only horizon any code depends on.",
    },
    ClassifiedTable {
        name: "search_hit_log",
        class: TableClass::Telemetry,
        rationale: "No readers. Up to `result_limit` rows per query, so the highest-volume \
                    telemetry table by construction.",
    },
    ClassifiedTable {
        name: "search_interaction_log",
        class: TableClass::Telemetry,
        rationale: "No writer and no reader; migration 020 already says so, and the reference \
                    host carries zero active parts. Dropping the table is plan OQ-2; a TTL is \
                    free in the meantime.",
    },
    // ---- ClickHouse system logs ------------------------------------------
    ClassifiedTable {
        name: "system.query_log",
        class: TableClass::Telemetry,
        rationale: "3.51 GiB / 14.0M rows on the reference host spanning five months, with no \
                    TTL because the packaged `config.xml` is never loaded.",
    },
    ClassifiedTable {
        name: "system.metric_log",
        class: TableClass::Telemetry,
        rationale: "One row per second forever at the configured 1000 ms collect interval.",
    },
    ClassifiedTable {
        name: "system.asynchronous_metric_log",
        class: TableClass::Telemetry,
        rationale: "542M rows on the reference host; cheap per row, unbounded in aggregate.",
    },
];

/// The ownership class of `table`, or `None` when the table is unknown to the
/// classification (§4 S1).
///
/// `None` is a hard error for every caller that is about to delete something.
/// It is deliberately not "probably derived": the whole point of this function
/// is that a table nobody thought about is a table nobody may delete.
///
/// Accepts either an unqualified Moraine table name (`events`) or a
/// `system.`-qualified system log.
pub fn classify(table: &str) -> Option<TableClass> {
    CLASSIFIED_TABLES
        .iter()
        .find(|entry| entry.name == table)
        .map(|entry| entry.class)
}

/// The full classification entry, including the rationale, for `table`.
pub fn classification(table: &str) -> Option<&'static ClassifiedTable> {
    CLASSIFIED_TABLES.iter().find(|entry| entry.name == table)
}

/// Every Moraine (non-`system.`) table of `class`, sorted.
pub fn tables_of_class(class: TableClass) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = CLASSIFIED_TABLES
        .iter()
        .filter(|entry| entry.class == class && !entry.name.starts_with("system."))
        .map(|entry| entry.name)
        .collect();
    names.sort_unstable();
    names
}

/// Both directions of the §4 S1 exhaustiveness invariant, as name sets.
///
/// Returned rather than asserted so the unit test can name each side in its
/// failure message, and so a future caller (a doctor probe, say) can surface a
/// drift without panicking.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClassificationGaps {
    /// Schema objects the handshake requires that are neither classified nor
    /// declared a view. **A new table that nobody classified lands here.**
    pub unclassified_schema_objects: Vec<String>,
    /// Classified Moraine tables that the schema handshake does not require
    /// and that are not on the documented unregistered allowlist.
    pub classified_but_unregistered: Vec<String>,
    /// Names declared to be views that the schema handshake does not require.
    pub stale_view_declarations: Vec<String>,
    /// **The third side.** `CREATE TABLE`s in [`crate::bundled_migrations`]
    /// naming something neither classified nor declared a view.
    ///
    /// The other two fields compare two hand-maintained Rust lists to each
    /// other, so a migration that installs a table nobody ever registered
    /// leaves both of them empty — which is precisely how
    /// `file_attention_project_roots` (migration 026) came to need
    /// [`UNREGISTERED_PHYSICAL_TABLES`]. This field reads `sql/`.
    pub unclassified_migration_tables: Vec<String>,
}

impl ClassificationGaps {
    /// Whether all four vectors are empty.
    ///
    /// Every caller today is a test, and a summary that omitted a field would
    /// be green in all of them, because each also asserts its fields
    /// individually. `each_classification_gap_vector_names_a_planted_gap`
    /// therefore asserts this method **per field**: a gap planted in any one of
    /// the four has to make it `false`. Without that, hard-wiring it to `true`
    /// is green, and the first non-test adopter inherits a summary that always
    /// says "no gaps".
    pub fn is_empty(&self) -> bool {
        self.unclassified_schema_objects.is_empty()
            && self.classified_but_unregistered.is_empty()
            && self.stale_view_declarations.is_empty()
            && self.unclassified_migration_tables.is_empty()
    }
}

/// Compute [`ClassificationGaps`] between [`CLASSIFIED_TABLES`],
/// [`SCHEMA_VIEW_OBJECTS`], [`REQUIRED_SCHEMA_OBJECTS`], and the `CREATE TABLE`
/// statements of every bundled migration.
pub fn classification_gaps() -> ClassificationGaps {
    let created: Vec<String> = crate::bundled_migrations()
        .iter()
        .flat_map(|migration| migration_created_tables(migration.sql))
        .collect();
    classification_gaps_between(
        &REQUIRED_SCHEMA_OBJECTS.iter().copied().collect(),
        &SCHEMA_VIEW_OBJECTS.iter().copied().collect(),
        &UNREGISTERED_PHYSICAL_TABLES.iter().copied().collect(),
        &CLASSIFIED_TABLES
            .iter()
            .map(|entry| entry.name)
            .filter(|name| !name.starts_with("system."))
            .collect(),
        &created,
    )
}

/// [`classification_gaps`] over supplied sets rather than over the shipped
/// constants.
///
/// Split out **only** so the non-vacuity companions can plant a gap in each
/// input and watch the corresponding field name it. Three of the four fields
/// are pure functions of `&'static` constants, so a test built from those
/// constants alone can assert nothing beyond "the shipped tree is clean" — and
/// a field narrowed to `Vec::new()` satisfies that assertion just as well as a
/// working one. `each_classification_gap_vector_names_a_planted_gap` is the
/// bound; `the_migration_side_of_exhaustiveness_actually_parses_create_table`
/// is the same claim for the fourth, which reads `sql/`.
fn classification_gaps_between(
    required: &BTreeSet<&str>,
    views: &BTreeSet<&str>,
    unregistered: &BTreeSet<&str>,
    classified: &BTreeSet<&str>,
    created: &[String],
) -> ClassificationGaps {
    ClassificationGaps {
        unclassified_schema_objects: required
            .iter()
            .filter(|name| !classified.contains(*name) && !views.contains(*name))
            .map(|name| (*name).to_string())
            .collect(),
        classified_but_unregistered: classified
            .iter()
            .filter(|name| !required.contains(*name) && !unregistered.contains(*name))
            .map(|name| (*name).to_string())
            .collect(),
        stale_view_declarations: views
            .iter()
            .filter(|name| !required.contains(*name))
            .map(|name| (*name).to_string())
            .collect(),
        unclassified_migration_tables: {
            // No `!views.contains(…)` companion here, deliberately. There used
            // to be one and it was unreachable: `created_table` returns `None`
            // for `CREATE VIEW`/`CREATE MATERIALIZED VIEW`, and no bundled
            // migration creates a physical table under a name in
            // `SCHEMA_VIEW_OBJECTS`, so no view name could reach the filter.
            //
            // MUTATION (executed 2026-07-27): add the filter back => the whole
            // crate stays GREEN, which is the measurement that it was dead
            // code rather than a guard.
            //
            // Removing it is also the safer reading: a migration that installs
            // a real table named `search_term_stats` *is* a classification gap
            // and should be reported as one.
            let mut created: Vec<String> = created
                .iter()
                .filter(|name| !classified.contains(name.as_str()))
                .cloned()
                .collect();
            created.sort();
            created.dedup();
            created
        },
    }
}

// ---------------------------------------------------------------------------
// §4 S4 — the repo-wide migration invariant
// ---------------------------------------------------------------------------

/// One migration's licence to perform named *shapes* of removal against a
/// named set of tables.
///
/// Keyed on **(version, table, shape)**. Each looser key was fail-open one
/// dimension at a time, and each was found by appending one statement to a
/// migration that already had an entry:
///
/// * Keyed on `version` alone, appending
///   `TRUNCATE TABLE moraine.search_documents;` to `sql/020` was invisible —
///   exempting a version exempted every table it did not yet name.
/// * Keyed on `(version, table)`, appending `TRUNCATE TABLE moraine.events;` to
///   `sql/012` was invisible, and `DROP TABLE moraine.events;` appended to
///   `sql/020` passed the gate. 012's own reason says its statements are
///   "guarded by a WHERE that skips already-correct rows … none removes a row";
///   that entry was nonetheless authorizing a truncate of bucket 1.
///
/// **A finding whose shape is `None` is never exempt.** An exemption is a
/// licence for a statement somebody read and described; a form nobody has named
/// is the case the gate exists for, and it stays a finding inside an exempted
/// migration exactly as it would anywhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationRemovalExemption {
    pub version: &'static str,
    /// Unqualified table names this entry covers.
    pub tables: &'static [&'static str],
    /// Statement shapes this entry covers.
    ///
    /// The licence is the **cross product** of `tables` and `shapes`, and
    /// `every_allowlist_entry_is_load_bearing_and_still_exempts_a_live_migration`
    /// requires every pair in it to be witnessed by a real statement of the
    /// migration. An entry whose cross product would over-grant has to be split
    /// into two entries, which is why `032` appears twice.
    pub shapes: &'static [DeleteShape],
    pub reason: &'static str,
}

/// Migrations allowed to contain a statement of a named shape against a named
/// table, each with the reason.
///
/// Growing this list is a deliberate act that shows up in review as an edit to
/// this constant — which is the whole point of paying the one-line cost here
/// rather than loosening a table's class, or widening
/// [`BENIGN_ALTER_OPERATIONS`], to make a migration compile clean.
///
/// Entries are in version order, and a version may hold more than one when a
/// single entry's cross product would over-grant.
///
/// Every entry is historical and one-shot except the standing
/// [`DeleteShape::MaterializedViewInto`] writers, which are standing by
/// construction.
///
/// `012` and `013` are here because [`benign_shape`] is an allowlist: they were
/// invisible for as long as the gate enumerated destructive shapes, since no
/// round of that enumeration ever listed `ALTER … UPDATE`. Between them they
/// rewrite five columns across twenty-one statements on `moraine.events` and
/// `moraine.raw_events`. `009`, `014`, `031`, `032` and `036` are here for the
/// same reason one round later: `MATERIALIZE COLUMN` and `INSERT` were on the
/// benign list, so a migration that rewrites a column of canonical history —
/// 014 materializes three on `moraine.events` — or appends to a never-delete
/// control table said nothing.
///
/// `004`, `011`, `012`, `014` and `032` gained a second kind of entry in round
/// 6, when the write-head check moved from `is_protected()` to
/// [`relation_is_guarded`] and started consulting
/// [`MIGRATION_PRESERVED_DERIVED_TABLES`]. That surfaced eleven statements —
/// nine `CREATE MATERIALIZED VIEW … TO` and two `INSERT INTO` — admitted until
/// then because `search_documents`/`search_postings` are `Derived`. They are
/// the views that *populate* the corpus and the backfills that reseed it, so
/// every one is legitimate; the reason they now have to be named is that the
/// same head aimed at the same table with a `SELECT * REPLACE (…)` **empties**
/// it (executed in `clickhouse local`: 23 bytes of `text_content` to 0 through
/// `FINAL`, still 0 after `OPTIMIZE … FINAL`), and nothing distinguished the
/// two.
///
/// Every one of these is legitimate, and "a bundled migration rewrites a column
/// on canonical history" — or "writes into the BM25 corpus" — is exactly the
/// sentence this gate exists to make somebody type.
pub const MIGRATION_DELETE_ALLOWLIST: &[MigrationRemovalExemption] = &[
    MigrationRemovalExemption {
        version: "004",
        tables: &["search_documents", "search_postings"],
        shapes: &[DeleteShape::MaterializedViewInto],
        reason: "installs the two standing writers that build the search corpus in the first \
                 place: `mv_search_documents_from_events` from `moraine.events`, and \
                 `mv_search_postings` tokenizing the documents. Both are the corpus's source, \
                 not a supersede of it",
    },
    MigrationRemovalExemption {
        version: "009",
        tables: &["search_documents"],
        shapes: &[DeleteShape::AlterRewriteColumn],
        reason: "adds the MATERIALIZED column `has_codex_mcp` and materializes it across every \
                 existing part. The rewrite is the point of the migration — without it the flag \
                 is false for all history — and it touches only the column it just added",
    },
    MigrationRemovalExemption {
        version: "010",
        tables: &["search_conversation_terms"],
        shapes: &[
            DeleteShape::Truncate,
            DeleteShape::InsertInto,
            DeleteShape::MaterializedViewInto,
        ],
        reason: "installs `search_conversation_terms`, TRUNCATEs it once so the accumulator \
                 starts from a known-empty state, backfills it from `search_documents`, then \
                 installs the materialized view that keeps it current. The table is never-delete \
                 (H5: no tombstone path) and these are the only bundled statements that may \
                 empty or supersede it",
    },
    MigrationRemovalExemption {
        version: "011",
        tables: &["search_documents", "search_postings"],
        shapes: &[DeleteShape::MaterializedViewInto],
        reason: "the #300 provider-to-harness rename redefines both 004 writers so their \
                 projections carry the renamed column. Same two standing views, recreated, \
                 selecting the same rows",
    },
    MigrationRemovalExemption {
        version: "012",
        tables: &["search_documents", "search_postings"],
        shapes: &[DeleteShape::MaterializedViewInto],
        reason: "adds `inference_provider` to both standing writers alongside 012's `ALTER … \
                 UPDATE` backfills. Split from 012's other entry so the licence is not the cross \
                 product: 012 may rewrite eight tables' columns and may recreate these two \
                 views, and neither permission implies the other",
    },
    MigrationRemovalExemption {
        version: "012",
        tables: &[
            "raw_events",
            "events",
            "event_links",
            "tool_io",
            "ingest_errors",
            "search_documents",
            "search_postings",
            "search_hit_log",
        ],
        shapes: &[DeleteShape::AlterUpdate],
        reason: "issue #300 harness rename: eight `ALTER … UPDATE` backfills that set \
                 `inference_provider` from `harness`, then eight that rewrite the legacy \
                 `claude` harness label to `claude-code`. Every one is guarded by a WHERE that \
                 skips already-correct rows, so re-running is a no-op; none removes a row",
    },
    MigrationRemovalExemption {
        version: "013",
        tables: &[
            "events",
            "search_documents",
            "search_postings",
            "search_hit_log",
        ],
        shapes: &[DeleteShape::AlterUpdate],
        reason: "canonicalizes the legacy `thinking` payload label to `reasoning` across the \
                 canonical table and the projections that copy `payload_type` out of it. \
                 Idempotent by the same already-correct WHERE guard as 012; it overwrites two \
                 metadata columns and a flag, never `payload_json`",
    },
    MigrationRemovalExemption {
        version: "014",
        tables: &["search_documents"],
        shapes: &[DeleteShape::MaterializedViewInto],
        reason: "harmonized token accounting redefines `mv_search_documents_from_events` to \
                 project the three new token columns. `search_postings` is not touched, and \
                 listing it here would grant a permission 014 does not use",
    },
    MigrationRemovalExemption {
        version: "014",
        tables: &["events", "search_documents"],
        shapes: &[DeleteShape::AlterRewriteColumn],
        reason: "harmonized token accounting materializes `endpoint_kind`, \
                 `token_usage_buckets` and `token_usage_native_units` across every part of \
                 canonical history, then across the search projection. Each is a MATERIALIZED \
                 column 014 itself adds, so the rewrite fills columns that were empty rather \
                 than replacing recorded values — but it is a rewrite of `moraine.events`, and \
                 it must be read as one",
    },
    MigrationRemovalExemption {
        version: "020",
        tables: &[
            "events",
            "raw_events",
            "event_links",
            "tool_io",
            "search_documents",
            "search_postings",
            "search_conversation_terms",
            "search_hit_log",
        ],
        shapes: &[DeleteShape::AlterDelete],
        reason: "issue #386 one-shot purge of empty-session_id claude-code rows; the only \
                 bundled migration that has ever legitimately removed canonical or raw rows",
    },
    MigrationRemovalExemption {
        version: "031",
        tables: &[
            "ingest_append_control",
            "ingest_checkpoint_transitions",
            "published_source_generations",
            "source_generation_publication_readiness",
        ],
        shapes: &[DeleteShape::InsertInto],
        reason: "seeds the four never-delete control relations of atomic source publication. \
                 Each is a ReplacingMergeTree keyed by a monotone revision, so the seed row is \
                 an append that supersedes nothing: the migration runs once, before any \
                 revision has been allocated",
    },
    MigrationRemovalExemption {
        version: "032",
        tables: &["search_documents", "search_postings"],
        shapes: &[DeleteShape::AlterRewriteColumn],
        reason: "re-declares `source_name` as `LowCardinality(String)` on both search \
                 projections, which rewrites every part. The corpus is rebuildable by a full \
                 re-tokenize and the column's values are preserved by the conversion, but the \
                 statement does rewrite the whole table",
    },
    MigrationRemovalExemption {
        version: "032",
        tables: &["search_conversation_terms"],
        shapes: &[DeleteShape::MaterializedViewInto],
        reason: "recreates `mv_search_conversation_terms` after the source-host read-model \
                 change; the standing writer into the never-delete accumulator is 010's, \
                 redefined, not a new one. Split from 032's other entry so the licence is not \
                 the cross product: 032 may rewrite a column of the search projections and may \
                 install this view, and neither permission implies the other",
    },
    MigrationRemovalExemption {
        version: "032",
        tables: &["search_documents", "search_postings"],
        shapes: &[DeleteShape::MaterializedViewInto, DeleteShape::InsertInto],
        reason: "the source-host live read model recreates both standing writers and backfills \
                 the rows that predate `source_host`. The two `INSERT INTO`s are the only \
                 bundled statements that append to the corpus directly; both are additive \
                 reseeds, neither carries a `REPLACE` that would supersede a live document",
    },
    MigrationRemovalExemption {
        version: "036",
        tables: &["mcp_read_index_state"],
        shapes: &[DeleteShape::InsertInto],
        reason: "stamps the three canonical read indexes as built. `mcp_read_index_state` is \
                 never-delete because it is the fence a reader consults before trusting an \
                 index, and these three appends are what set the fence in the first place",
    },
];

/// Derived tables that no bundled migration may remove rows from, with the
/// reason each one is more expensive to lose than its `Derived` class suggests.
///
/// This is the coverage the per-migration 034/035 loops used to provide for
/// `search_documents`/`search_postings`. Those loops were substring matches on
/// one statement form pinned to two versions; this is the same claim made
/// shape-aware and repo-wide, so it also covers migration 040.
pub const MIGRATION_PRESERVED_DERIVED_TABLES: &[(&str, &str)] = &[
    (
        "search_documents",
        "§2: `text_content` is the only remaining copy that could prove the ranking path wrong \
         while #597's C1 is open, and the corpus is rebuildable only by a full re-tokenize",
    ),
    (
        "search_postings",
        "the BM25 posting list; a migration that empties it silently changes every score until a \
         corpus-wide rebuild runs",
    ),
];

/// A statement in a bundled migration that is not on the benign allowlist and
/// names a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationDeleteFinding {
    pub version: String,
    pub table: String,
    pub class: TableClass,
    /// What the statement appears to do, when [`delete_shape`] recognizes it.
    /// `None` means "not benign, and not a shape anyone has named" — the case
    /// the gate exists for, and never a reason to suppress the finding.
    pub shape: Option<DeleteShape>,
    /// The offending statement, trimmed and collapsed to one line.
    pub statement: String,
}

/// Statement shapes that destroy rows.
///
/// **This enum classifies findings; it does not decide them.** The gate is
/// [`benign_shape`], and a statement it does not recognize is a finding whether
/// or not anything here matches — see [`migration_row_removals`].
///
/// It was the gate for four rounds, and each round's adversarial sweep found
/// shapes the previous round's list could not see: the `DROP` family, then
/// `MODIFY TTL` / `CLEAR COLUMN` / `MOVE … TO TABLE` / `REPLACE TABLE` /
/// `RENAME`, then `ALTER … UPDATE` / `REPLACE PARTITION` / `OPTIMIZE … FINAL` /
/// `DETACH PARTITION`. Enumerating destructive forms is an unbounded
/// adversarial search against a SQL dialect that keeps growing. Enumerating
/// benign ones is bounded by what migrations in this repository actually do —
/// 294 statements, [`BENIGN_STATEMENT_HEADS`] plus
/// [`BENIGN_ALTER_OPERATIONS`] — so that is what the gate enumerates now.
///
/// The fifth round added [`DeleteShape::AlterRewriteColumn`],
/// [`DeleteShape::InsertInto`] and [`DeleteShape::MaterializedViewInto`]. Those
/// are **not** a fifth attempt at completing the destructive enumeration: each
/// names a form that was on the *benign* list and had to come off it, and the
/// label exists so the exemption key — which is
/// `(version, table, shape)` — can license the bundled migrations that
/// legitimately use them.
///
/// What survives here is the reporting: "`ALTER … UPDATE` on `events`" is a
/// far better failure message than "unrecognized statement on `events`", and
/// the arms are still pinned in both directions by
/// `every_row_removing_shape_is_recognized_including_the_ones_the_old_guard_missed`,
/// which asserts the shape and not only the finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteShape {
    /// `TRUNCATE [TABLE|DATABASE] [IF EXISTS] <name>`.
    Truncate,
    /// `DELETE FROM <name> …` (lightweight delete).
    DeleteFrom,
    /// `ALTER TABLE <name> … DELETE …` (mutation).
    AlterDelete,
    /// `ALTER TABLE <name> UPDATE <col> = <expr> WHERE …` (mutation).
    ///
    /// The standard ClickHouse overwrite. `UPDATE payload_json = '' WHERE 1`
    /// keeps every row and destroys 3.52 GiB of compressed column content on
    /// the reference host — [`DeleteShape::AlterClear`] with an arbitrary
    /// expression instead of the column default. Twenty-one of these ship in
    /// `sql/012` and `sql/013`; both are on [`MIGRATION_DELETE_ALLOWLIST`].
    AlterUpdate,
    /// `INSERT INTO <name> …` where `<name>` is protected.
    ///
    /// See [`PROTECTED_WRITE_HEADS`]: on a `ReplacingMergeTree` an insert that
    /// repeats the sort key with a higher version supersedes the row that was
    /// there, and `moraine.events` is one.
    InsertInto,
    /// `CREATE MATERIALIZED VIEW <v> TO <name> …` where `<name>` is protected.
    ///
    /// [`DeleteShape::InsertInto`] installed as standing state: every future
    /// insert into the MV's source writes into `<name>` too.
    MaterializedViewInto,
    /// `ALTER TABLE <name> … MATERIALIZE COLUMN <col>` / `… MODIFY COLUMN <col>
    /// <type> …`.
    ///
    /// Both rewrite the stored bytes of a column that already has values.
    ///
    /// `MATERIALIZE COLUMN` recomputes the column from its current
    /// `DEFAULT`/`MATERIALIZED` expression across all parts. **This repository
    /// documents the hazard itself**: `sql/037_search_ranking_metadata.sql`
    /// records that #603 must materialize `text_digest` and `payload_phase`
    /// *before* dropping either source column, "or every historical digest
    /// silently becomes the digest of an empty string". `events` ships
    /// `inference_provider … DEFAULT ''` and `author … DEFAULT ''` today, so
    /// the default a materialize would recompute against exists.
    ///
    /// A `MODIFY COLUMN` that re-declares the type rewrites every part:
    /// `MODIFY COLUMN ingested_at DateTime` on the `DateTime64(3)` column that
    /// feeds `PARTITION BY toYYYYMM(ingested_at)` drops the millisecond
    /// component from all canonical history. The gate cannot tell a widening
    /// from a narrowing without the schema, so it reports both — see
    /// [`MODIFY_COLUMN_METADATA_KEYWORDS`] for the metadata forms that stay
    /// benign.
    AlterRewriteColumn,
    /// `ALTER TABLE <name> … MODIFY TTL <expr> …`, or a `MODIFY COLUMN <col>
    /// TTL <expr>` that puts a TTL on one column.
    ///
    /// **A TTL expression with no action clause defaults to `DELETE`**, and
    /// `materialize_ttl_after_modify` defaults to 1, so the ALTER mutates
    /// existing parts immediately: every row past the horizon is gone when the
    /// statement returns. `MODIFY TTL … GROUP BY … SET …` collapses rows
    /// instead of removing them, which is destructive in the same way.
    ///
    /// The `TO DISK` / `TO VOLUME` / `RECOMPRESS` forms move or re-encode
    /// bytes without removing a row and are nonetheless reported. That is a
    /// deliberate over-approximation, not an oversight: there is no storage
    /// policy and no tiering TTL anywhere in this tree, so a carve-out would
    /// be untested surface guarding a statement nobody writes, while *any*
    /// TTL policy landing on canonical history is a review event.
    /// `REMOVE TTL` is not a shape — it deletes the policy, not the rows.
    AlterModifyTtl,
    /// `ALTER TABLE <name> … DROP PARTITION|PART|COLUMN|DETACHED …`.
    AlterDrop,
    /// `ALTER TABLE <name> … DETACH PARTITION|PART …`.
    ///
    /// Step **one** of the two-step `DETACH` + `DROP DETACHED` delete. The
    /// parts move to `detached/` and the rows stop being readable under a
    /// protected name, which is the same claim
    /// [`DeleteShape::RenameRelation`] is here for. The `DROP` list has
    /// covered step two (`" DROP DETACHED"`) since the round that added it.
    AlterDetach,
    /// `ALTER TABLE <name> … REPLACE PARTITION <p> FROM <other>`.
    ///
    /// There is no `REPLACE PART`: `EXPLAIN AST` on ClickHouse 25.12.5.44
    /// rejects it, so the `PARTITION` spelling is the whole shape.
    ///
    /// Atomically discards the **destination's** partition and replaces it with
    /// the source's. `events` spans six active partitions on the reference
    /// host, so one statement discards a month. Invisible to
    /// [`DeleteShape::ReplaceRelation`], whose test is a `starts_with` prefix
    /// and cannot see an infix `REPLACE PARTITION`.
    AlterReplacePartition,
    /// `ALTER TABLE <name> … CLEAR COLUMN <col> [IN PARTITION …]`.
    ///
    /// Rewrites every part with the column's default in place of its values.
    /// On `events` that is the 3.48 GiB `payload_json` column: the rows
    /// survive and their content does not, which no `DROP`-shaped predicate
    /// sees. `CLEAR INDEX` / `CLEAR PROJECTION` discard derived structures and
    /// are not shapes.
    AlterClear,
    /// `ALTER TABLE <name> … MOVE PARTITION … TO TABLE <other>` /
    /// `… MOVE PART … TO SHARD <path>`.
    ///
    /// Detaches the parts from the source and attaches them elsewhere, so the
    /// source loses the rows. The `TO DISK` / `TO VOLUME` forms move bytes
    /// between storage tiers and remove nothing, so the **destination** — not
    /// the `MOVE` keyword — is the extent, and the test is written as the
    /// negation of the tiering destinations rather than as a list of the
    /// removing ones. `EXPLAIN AST` on ClickHouse 25.12.5.44 rejects
    /// `MOVE PART … TO TABLE` and `MOVE PARTITION … TO SHARD`, so the two
    /// valid removing spellings are the two above; enumerating them positively
    /// would have to be revisited the next time ClickHouse adds a destination.
    AlterMove,
    /// `DROP TABLE [IF EXISTS] <name>` / `DROP DATABASE [IF EXISTS] <name>`.
    DropRelation,
    /// `[CREATE OR] REPLACE TABLE <name> …`.
    ///
    /// Atomically swaps a populated table for a freshly created empty one.
    /// Reported by no `DROP`- or `TRUNCATE`-shaped predicate, and
    /// [`created_table`] recognizes it too so a table installed this way still
    /// has to be classified.
    ReplaceRelation,
    /// `RENAME TABLE|DATABASE <a> TO <b>` / `EXCHANGE TABLES <a> AND <b>`.
    ///
    /// Neither removes a row by itself, but both make the rows that were
    /// reachable under a protected name unreachable under it — `EXCHANGE
    /// TABLES moraine.events AND moraine.events_empty` is `TRUNCATE` with
    /// extra steps and a rollback. **Every** name the statement mentions is
    /// reported, because the displaced relation may be either operand.
    RenameRelation,
    /// `OPTIMIZE TABLE <name> FINAL [DEDUPLICATE [BY …]]`.
    ///
    /// `events` is a `ReplacingMergeTree`, so `FINAL` applies the collapse
    /// immediately instead of whenever a merge happens to run, and
    /// `DEDUPLICATE BY` collapses on an arbitrary column subset — a hand-picked
    /// subset can fold together rows that are not duplicates at all.
    /// `reclaim.rs`'s emitter has treated `OPTIMIZE … FINAL` as a write since
    /// the round that added it; this is the same argument applied to the
    /// migration path, where a hand-written upgrade statement is likelier to
    /// appear.
    OptimizeFinal,
}

/// Statement heads that cannot remove or overwrite a row of any table they
/// name, and the migration or corpus statement that witnesses each one.
///
/// **This is the gate.** A statement whose head is not here and whose ALTER
/// clauses are not in [`BENIGN_ALTER_OPERATIONS`] is a finding for every
/// protected table it names, whether or not [`delete_shape`] recognizes it.
///
/// Two of these heads are conditional on the class of the relation they write
/// into rather than on their form alone — see [`PROTECTED_WRITE_HEADS`]. They
/// are listed here anyway, because the head is what
/// `every_benign_entry_is_witnessed_by_a_real_statement` iterates and the
/// condition is what [`benign_shape`] applies.
///
/// Entries are matched against the uppercased, whitespace-collapsed statement
/// with `starts_with`, so `CREATE TABLE` does **not** admit
/// `CREATE OR REPLACE TABLE` — a different head, and a destructive one.
///
/// Every entry is exercised by a bundled migration or by a named row of the
/// negative corpus; `every_benign_entry_is_witnessed_by_a_real_statement`
/// fails for one that is not, which is what stops this list from becoming the
/// same unbounded enumeration in the other direction. That test also separates
/// **tree-witnessed** from **corpus-witnessed** entries, because a row the same
/// author added alongside an entry is not evidence that this repository writes
/// the form.
///
/// **Adding a head here means adding a rule to [`BENIGN_HEAD_TARGET_RULES`]**,
/// which is where the question "can a statement with this head displace a
/// relation that already exists?" has to be answered — in code, with the gated
/// answer executed against a probe. `CREATE OR REPLACE VIEW ` shipped for three
/// rounds answering it in a comment, and the comment was wrong.
const BENIGN_STATEMENT_HEADS: &[&str] = &[
    // Reads nothing away. sql/ has no bare SELECT; the corpus does.
    "SELECT ",
    // Appends. 25 statements across sql/, both the VALUES and the `… SELECT`
    // forms. Conditional on the target's class — see [`PROTECTED_WRITE_HEADS`].
    "INSERT INTO ",
    "CREATE DATABASE ",
    "CREATE TABLE ",
    // Views hold no rows. `CREATE MATERIALIZED VIEW` must precede nothing here;
    // it is listed separately because `CREATE VIEW` is not its prefix, and it
    // is conditional on its `TO` target's class for the same reason `INSERT`
    // is: an MV with a `TO` clause is a standing writer.
    "CREATE VIEW ",
    "CREATE MATERIALIZED VIEW ",
    // The atomic spelling of the drop-and-recreate that nine bundled
    // migrations already perform on views (002, 004, 006, 011, 012, 014, 019,
    // 032, 033). **Conditional on the class of the name it replaces** — see
    // [`PROTECTED_REPLACE_HEADS`], and do not restore the sentence this
    // comment used to carry ("a view holds no rows, so this one can destroy
    // nothing"), which was measurably false.
    "CREATE OR REPLACE VIEW ",
    // Safe unconditionally, and unlike the head above that is a property of
    // the server rather than of this list: `DROP VIEW` on a table throws
    // `Code: 80 … is not a View` (executed, 25.12.5.44).
    "DROP VIEW ",
];

/// `ALTER TABLE` operation clauses that cannot remove or overwrite a row.
///
/// An operation is the clause's first token plus the token after it, so
/// `MODIFY ORDER BY` appears as `MODIFY ORDER` and
/// `ADD COLUMN IF NOT EXISTS x` still reduces to `ADD COLUMN`. `ALTER` accepts
/// comma-separated operations; [`alter_clauses`] splits them and **every**
/// clause must reduce to an entry here, because one destructive operation is
/// enough to make the statement a finding no matter what it is bundled with.
///
/// Three operations are **conditional** and therefore deliberately absent —
/// listing them would admit their destructive spellings. See
/// [`clause_is_benign`]:
///
/// * `MOVE PARTITION` / `MOVE PART` — benign only with a `TO DISK` /
///   `TO VOLUME` destination.
/// * `MODIFY COLUMN` — benign only as a metadata change; see
///   [`modify_column_is_metadata_only`].
/// * `MODIFY QUERY` — benign only on a declared [`SCHEMA_VIEW_OBJECTS`] name.
///
/// The three derived-structure families are here in full — `INDEX`,
/// `PROJECTION` and `STATISTICS` each get `ADD`/`DROP`/`CLEAR`/`MATERIALIZE`.
/// A half-family is its own defect: for a round this list carried
/// `DROP PROJECTION` and `CLEAR PROJECTION` without `ADD PROJECTION`, so a
/// migration could delete a projection but not create one.
const BENIGN_ALTER_OPERATIONS: &[&str] = &[
    "ADD COLUMN",
    "ADD CONSTRAINT",
    "ADD INDEX",
    "ADD PROJECTION",
    "ADD STATISTICS",
    "CLEAR INDEX",
    "CLEAR PROJECTION",
    "CLEAR STATISTICS",
    "COMMENT COLUMN",
    "DROP CONSTRAINT",
    "DROP INDEX",
    "DROP PROJECTION",
    "DROP STATISTICS",
    "MATERIALIZE INDEX",
    "MATERIALIZE PROJECTION",
    "MATERIALIZE STATISTICS",
    "MATERIALIZE TTL",
    // The table's own comment. `COMMENT COLUMN` was already here; this is the
    // relation-level spelling of the same metadata edit.
    "MODIFY COMMENT",
    // `MODIFY ORDER BY` — the sort key, not the rows.
    "MODIFY ORDER",
    "MODIFY SETTING",
    // Deletes the TTL policy, not the rows it would have removed.
    "REMOVE TTL",
    "RENAME COLUMN",
    // Reverts a table setting to its default. Changes no row.
    "RESET SETTING",
];

/// Tokens that may follow the column name in a benign `MODIFY COLUMN` clause.
///
/// A `MODIFY COLUMN` clause is
/// `MODIFY COLUMN [IF EXISTS] <name> [<type>] [<property> …]`. When the token
/// after the name is one of these the clause edits metadata; when it is
/// anything else it is a **type declaration**, and re-declaring a type rewrites
/// every part.
///
/// The full set ClickHouse 25.12.5.44 accepts in that position is
/// `COLLATE, DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL, AUTO_INCREMENT, COMMENT,
/// CODEC, STATISTICS, TTL, PRIMARY KEY, SETTINGS, REMOVE, MODIFY` — read off
/// the parser's own error message, then each candidate re-checked with
/// `EXPLAIN AST` (2026-07-27). This list is a deliberate **subset**:
///
/// * `TTL` is absent, but the absence guards nothing on its own and the doc
///   must not pretend otherwise: `EXPLAIN AST` rejects `MODIFY COLUMN <c> TTL
///   …` without a preceding type, so in valid ClickHouse the type is always the
///   first tail token and this list is never consulted for a column TTL. **The
///   type check is what catches it.** (MUTATION: adding `"TTL"` here left the
///   whole crate GREEN when measured on 2026-07-27, because nothing bounded
///   this list at all; re-measured 2026-07-28 it is **RED** in
///   `a_modify_column_property_the_parser_accepts_is_reported_not_admitted`,
///   which is the bound that constant was missing.) The entry stays off the
///   list as defence in depth against a dialect that later relaxes the
///   requirement, and [`modify_column_sets_ttl`] — which *is*
///   load-bearing, for the label — is what reports it as
///   [`DeleteShape::AlterModifyTtl`] rather than
///   [`DeleteShape::AlterRewriteColumn`].
///
///   Why a column TTL is a removal at all: it replaces expired values with the
///   column default and, once every value in a part has expired, removes the
///   column from that part on disk — [`DeleteShape::AlterClear`] on a rolling
///   horizon. The reference host reports `materialize_ttl_after_modify = 1` and
///   `merge_with_ttl_timeout = 14400`, so it lands within four hours unaided,
///   and `MATERIALIZE TTL` forces it immediately.
/// * `ALIAS` and `EPHEMERAL` are absent because they convert a **stored**
///   column into one that is not stored. That is `DROP COLUMN` under another
///   name: `MODIFY COLUMN payload_json ALIAS ''` discards 3.48 GiB of canonical
///   payload.
/// * `PRIMARY KEY` is absent because changing the primary key rewrites the
///   parts.
/// * `COLLATE`, `SETTINGS` and `AFTER` are absent because `EXPLAIN AST`
///   rejects each of them in this position without a preceding type — at which
///   point the type is the first token and this list is never consulted.
///   Listing them would be dead surface. (Re-measured 2026-07-28:
///   `MODIFY COLUMN c COLLATE 'en'`, `… SETTINGS max_compress_block_size = 1`
///   and `… AFTER other` are all syntax errors.) `FIRST` is present because it
///   is the one position modifier the parser accepts without a type, and column
///   order is schema metadata rather than column content.
/// * `STATISTICS(<type>)` and `AUTO_INCREMENT` are absent, and this doc used to
///   claim they were rejected in this position too. **They are not.**
///   Re-measured 2026-07-28: `ALTER TABLE t MODIFY COLUMN c
///   STATISTICS(tdigest)` and `ALTER TABLE t MODIFY COLUMN c AUTO_INCREMENT`
///   are both VALID without a preceding type. (Bare `STATISTICS tdigest`, with
///   no parentheses, is not.) So for these two the surface is live and their
///   absence is a deliberate **over-approximation**: a statistics declaration
///   and an auto-increment flag rewrite no column bytes, and the gate reports
///   them anyway, which costs one allowlist line if a migration ever writes
///   one. That is the fail-closed direction, and it is recorded here rather
///   than mislabelled as dead surface. `a_modify_column_property_the_parser_accepts_is_reported_not_admitted`
///   pins both as findings, so adding either to this list turns a green test
///   red — the non-vacuity bound this constant did not have.
const MODIFY_COLUMN_METADATA_KEYWORDS: &[&str] = &[
    "CODEC",
    "COMMENT",
    "DEFAULT",
    // Column order in the schema, not column content.
    "FIRST",
    "MATERIALIZED",
    // `MODIFY SETTING` / `RESET SETTING`, the per-column spellings.
    "MODIFY",
    "REMOVE",
    "RESET",
];

/// Split an `ALTER TABLE` body into its comma-separated operation clauses.
///
/// This is the fix for the round-4 root cause. The previous implementation was
/// not a clause parser at all: it scanned the whole body for any token in a
/// verb list, which failed in **both** directions.
///
/// * Fail-closed. `MODIFY COLUMN author String DEFAULT '' COMMENT 'who'`
///   reduced to two operations — the real `MODIFY COLUMN` and a phantom
///   `COMMENT 'WHO'` manufactured from the inline column comment — and every
///   operation had to be benign, so ordinary DDL was rejected.
/// * Fail-open. The `MOVE` tiering test searched the whole statement, so
///   `ADD COLUMN c String DEFAULT 'x TO DISK y', MOVE PARTITION '202601' TO
///   TABLE moraine.mcp_open_turns` was admitted on a substring inside an
///   unrelated literal.
///
/// Splitting on top-level commas and judging each clause on its own text
/// closes both. The input is [`mask_quoted`], so a comma inside a literal
/// cannot split a clause and a keyword inside one cannot be read.
///
/// **A mis-split fails closed.** `UPDATE a = 1, b = 2 WHERE 1` is one
/// ClickHouse operation whose arguments contain a top-level comma, so it splits
/// into `UPDATE a = 1` and `b = 2 WHERE 1`; the first is not benign and the
/// second reduces to `B =`, which is not benign either.
/// `a_mis_split_alter_clause_fails_closed` pins that direction.
///
/// **An unbalanced body yields no clause at all**, which [`alter_is_benign`]
/// treats as a finding. A well-formed statement's parentheses balance outside
/// its literals, so this costs nothing and closes the round-6 vector where an
/// unrecognized `(` — a heredoc's, before [`scan_statement`] learned the
/// syntax — held the depth above zero and stopped the top-level comma from
/// splitting off a `DROP COLUMN`. The lexer fix and this one are independent:
/// either alone kills that statement.
fn alter_clauses(masked_upper: &str) -> Vec<String> {
    let tokens: Vec<&str> = masked_upper.split_whitespace().collect();
    // `ALTER TABLE <name> [ON CLUSTER <cluster>]`. Skipping the name is what
    // stops a relation whose name is an operation keyword from being read as
    // one; skipping `ON CLUSTER <cluster>` is what stops the distributed
    // spelling from presenting `ON` as its first clause head.
    let mut start = 3;
    if tokens.get(3) == Some(&"ON") && tokens.get(4) == Some(&"CLUSTER") {
        start = 6;
    }
    if tokens.len() <= start {
        return Vec::new();
    }

    let body = tokens[start..].join(" ");
    let mut fragments = Vec::new();
    let mut depth = 0usize;
    let mut unbalanced = false;
    let mut current = String::new();
    for ch in body.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                if depth == 0 {
                    unbalanced = true;
                }
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                if !current.trim().is_empty() {
                    fragments.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        fragments.push(current.trim().to_string());
    }
    if unbalanced || depth != 0 {
        return Vec::new();
    }
    merge_clause_continuations(fragments)
}

/// Re-join the fragments of the operations whose *own arguments* are a
/// comma-separated list.
///
/// Three ClickHouse operations take a comma-separated tail, and splitting on
/// every top-level comma made all three unrepresentable — round 6's false
/// findings, each `EXPLAIN AST`-valid on 25.12.5.44:
///
/// * `MODIFY SETTING index_granularity = 8192, min_bytes_for_wide_part = 0` —
///   **the ordinary spelling**; `MODIFY SETTING` takes a list.
/// * `RESET SETTING a, b`, and the query-level `… SETTINGS alter_sync = 2,
///   mutations_sync = 1` tail any clause may carry.
/// * `MODIFY QUERY SELECT 1 AS a, 2 AS b` — so `MODIFY QUERY` support was
///   single-column-only.
///
/// Each reported shape `None`, and a `None` shape is categorically unexemptable
/// (see [`MIGRATION_DELETE_ALLOWLIST`]), so a migration that legitimately
/// needed a two-setting `MODIFY SETTING` could not be allowlisted at all. The
/// parser was the only thing that could fix it.
///
/// **Merging cannot launder a command**, for two independent reasons.
/// Syntactically: ClickHouse does not accept an alter command after a settings
/// list or a `MODIFY QUERY` — `MODIFY SETTING a = 8192, DROP COLUMN c`,
/// `RESET SETTING a, DROP COLUMN c`, `MODIFY QUERY SELECT 1, DROP COLUMN c`
/// and the comma-free `MODIFY SETTING a = 2 DROP COLUMN c` are all syntax
/// errors (executed, 25.12.5.44). And structurally: a settings continuation
/// must be exactly `<ident> = <value>` or a bare `<ident>`, so `DROP COLUMN c`
/// is not one and stays its own clause, where it is a finding.
fn merge_clause_continuations(fragments: Vec<String>) -> Vec<String> {
    let mut clauses: Vec<String> = Vec::new();
    for fragment in fragments {
        let continues = clauses
            .last()
            .is_some_and(|open| clause_takes_comma_tail(open, &fragment));
        match clauses.last_mut() {
            Some(open) if continues => {
                open.push_str(", ");
                open.push_str(&fragment);
            }
            _ => clauses.push(fragment),
        }
    }
    clauses
}

/// Whether `fragment` continues the argument list of the clause `open`.
fn clause_takes_comma_tail(open: &str, fragment: &str) -> bool {
    match clause_operation(open).as_str() {
        // The tail is a whole `SELECT`, whose projection list, `GROUP BY` and
        // window definitions are full of top-level commas, so nothing short of
        // a SQL parser could find its end. Swallowing to the end of the
        // statement is sound *because* `MODIFY QUERY` is benign only on a
        // declared [`SCHEMA_VIEW_OBJECTS`] name: every clause of an `ALTER`
        // applies to the one relation the `ALTER` names, and on any other name
        // the whole statement is already a finding.
        "MODIFY QUERY" => true,
        "MODIFY SETTING" => is_setting_assignment(fragment),
        "RESET SETTING" => is_setting_name(fragment),
        // The query-level `SETTINGS` tail, which any clause may carry.
        _ => {
            open.split_whitespace().any(|token| token == "SETTINGS")
                && is_setting_assignment(fragment)
        }
    }
}

/// `<ident> = <value>`, and nothing longer.
///
/// Three tokens exactly. A settings value is a scalar — a number, or a literal
/// that [`mask_quoted`] has already collapsed to one token — so this covers
/// every settings list in the tree while refusing to absorb `DROP COLUMN
/// payload_json` (whose second token is `COLUMN`, not `=`) or the compound
/// `y = 2 DROP COLUMN payload_json` (six tokens). A settings value written as
/// an arithmetic expression would be a finding rather than a bypass; §7.4
/// records it.
fn is_setting_assignment(fragment: &str) -> bool {
    let tokens: Vec<&str> = fragment.split_whitespace().collect();
    tokens.len() == 3 && tokens[1] == "=" && is_bare_identifier(tokens[0])
}

/// A single bare identifier, which is all `RESET SETTING`'s list holds.
fn is_setting_name(fragment: &str) -> bool {
    let mut tokens = fragment.split_whitespace();
    tokens.next().is_some_and(is_bare_identifier) && tokens.next().is_none()
}

fn is_bare_identifier(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// Reduce one clause to `"<FIRST> <SECOND>"`, or `"<FIRST>"` at end of clause.
///
/// There is no verb allowlist any more. Under the old whole-body token scan an
/// unlisted verb was a **silent pass**, so the list had to be exhaustive
/// against a dialect that keeps growing; now the clause head is reduced
/// unconditionally and an unrecognized head simply fails
/// [`BENIGN_ALTER_OPERATIONS`] lookup, which is a finding. Completing the
/// enumeration stopped being load-bearing, so the enumeration is gone.
fn clause_operation(clause: &str) -> String {
    let mut tokens = clause.split_whitespace();
    let first = tokens.next().unwrap_or_default();
    match tokens.next() {
        Some(second) => format!("{first} {second}"),
        None => first.to_string(),
    }
}

/// Everything after the column name in a `MODIFY COLUMN <name> …` clause: the
/// optional type followed by the optional properties.
///
/// `MODIFY COLUMN [IF EXISTS] <name>` is the prefix; the caller judges the
/// tail. Reading the type positionally rather than by pattern is what lets
/// `MODIFY COLUMN ttl_seconds UInt32` and `MODIFY COLUMN c UInt32 TTL x`
/// disagree: only the second has a bare `TTL` token *after* the name.
fn column_clause_tail(clause: &str) -> Vec<&str> {
    let mut tokens = clause.split_whitespace().skip(2).peekable();
    if tokens.peek() == Some(&"IF") {
        tokens.next();
        if tokens.peek() == Some(&"EXISTS") {
            tokens.next();
        }
    }
    if tokens.next().is_none() {
        // No column name: the clause did not parse.
        return Vec::new();
    }
    tokens.collect()
}

/// Whether a `MODIFY COLUMN` clause edits metadata rather than stored bytes.
///
/// A bare `MODIFY COLUMN <name>` with no tail is not valid ClickHouse; it
/// reports `false`, because a clause the parser could not finish reading is the
/// case that must fail loudly.
fn modify_column_is_metadata_only(clause: &str) -> bool {
    match column_clause_tail(clause).first() {
        None => false,
        Some(first) => MODIFY_COLUMN_METADATA_KEYWORDS
            .iter()
            .any(|keyword| *first == *keyword || first.starts_with(&format!("{keyword}("))),
    }
}

/// Whether a `MODIFY COLUMN` clause attaches a TTL to the column.
///
/// A bare `TTL` token in the tail, so a column *named* `ttl` cannot trip it and
/// a literal cannot smuggle one in — the clause is masked.
fn modify_column_sets_ttl(clause: &str) -> bool {
    column_clause_tail(clause).contains(&"TTL")
}

/// Whether one masked `ALTER` clause cannot remove or overwrite a row.
///
/// `relation` is the table the `ALTER` names, needed only by `MODIFY QUERY`.
fn clause_is_benign(clause: &str, relation: Option<&str>) -> bool {
    match clause_operation(clause).as_str() {
        // Tiering moves bytes between storage tiers and keeps every row in the
        // table it names. `MOVE … TO TABLE` and `MOVE … TO SHARD` do not. The
        // destination is read from **this clause**, not from the statement.
        "MOVE PARTITION" | "MOVE PART" => {
            clause.contains(" TO DISK") || clause.contains(" TO VOLUME")
        }
        "MODIFY COLUMN" => modify_column_is_metadata_only(clause),
        // Redefining a materialized view's SELECT changes what it writes from
        // now on and rewrites nothing that exists. Scoped to declared views: on
        // a `MergeTree` name it is a statement ClickHouse rejects, and an
        // undeclared name is unknown, which S1 treats as protected.
        "MODIFY QUERY" => relation.is_some_and(|name| SCHEMA_VIEW_OBJECTS.contains(&name)),
        operation => BENIGN_ALTER_OPERATIONS.contains(&operation),
    }
}

/// Whether every clause of one `ALTER TABLE` statement is benign.
///
/// An `ALTER` that yields no clause at all is **not** benign: the parser
/// failing to reach an operation is exactly the case that must fail loudly.
fn alter_is_benign(masked_upper: &str, relation: Option<&str>) -> bool {
    let clauses = alter_clauses(masked_upper);
    if clauses.is_empty() {
        return false;
    }
    clauses
        .iter()
        .all(|clause| clause_is_benign(clause, relation))
}

/// Statement heads that write rows into a relation they do not otherwise
/// touch, paired with the shape a write into a **protected** relation reports.
///
/// Appending is benign for the rows already in a table — except on a
/// `ReplacingMergeTree`, where repeating the sort key with a higher version
/// supersedes them. `moraine.events` is
/// `ReplacingMergeTree(event_version)` with `payload_json` outside the sort
/// key, so
/// `INSERT INTO moraine.events SELECT * REPLACE ('' AS payload_json,
/// event_version + 1 AS event_version) FROM moraine.events FINAL` blanks every
/// payload in canonical history: unreachable immediately through `FINAL` and
/// `v_live_events`, physically gone at the next merge. A
/// `CREATE MATERIALIZED VIEW … TO moraine.events` installs the same overwrite
/// as standing state.
///
/// The class is what separates the hazard from the ordinary case, and the gate
/// already knows it: superseding a `Derived` row costs a rebuild, superseding a
/// canonical or never-delete one costs the record. So these two heads are
/// benign for an unprotected target and a finding for a protected one — see
/// [`benign_shape`].
const PROTECTED_WRITE_HEADS: &[(&str, DeleteShape)] = &[
    ("INSERT INTO ", DeleteShape::InsertInto),
    (
        "CREATE MATERIALIZED VIEW ",
        DeleteShape::MaterializedViewInto,
    ),
];

/// Why a statement with a given benign head, aimed at a relation that already
/// exists, cannot destroy it.
///
/// Round 6's `CREATE OR REPLACE VIEW` hole was not a missing check so much as a
/// question nobody had been made to answer. `INSERT INTO ` and
/// `CREATE MATERIALIZED VIEW ` carried a target-class condition;
/// `CREATE OR REPLACE VIEW ` carried a *sentence* asserting it needed none, and
/// the sentence was wrong. Making the answer a value means the next head cannot
/// ship without one, and the `ClassGated` arm is executed rather than asserted:
/// see `every_benign_head_answers_the_target_class_question`.
///
/// It is `#[cfg(test)]` — a review obligation, not a runtime one — but it lives
/// here rather than in the test module on purpose, so that the author adding a
/// head to [`BENIGN_STATEMENT_HEADS`] reads it in the same screen.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadTargetRule {
    /// [`benign_shape`] resolves the relation and consults
    /// [`relation_is_guarded`]. The probe is a statement with this head aimed
    /// at `moraine.events`, which the gate must reject.
    ClassGated(&'static str),
    /// The head cannot name an existing relation as something it overwrites,
    /// with the reason. A reason is not a proof, so each one names a mechanism
    /// that was checked against the server rather than assumed.
    CannotDisplace(&'static str),
}

/// One rule per [`BENIGN_STATEMENT_HEADS`] entry, and the test requires the two
/// lists to correspond exactly — same entries, same order.
#[cfg(test)]
const BENIGN_HEAD_TARGET_RULES: &[(&str, HeadTargetRule)] = &[
    (
        "SELECT ",
        HeadTargetRule::CannotDisplace(
            "a read. It writes nowhere, which is the one case in this \
                                       table that needs no server behaviour to be true.",
        ),
    ),
    (
        "INSERT INTO ",
        HeadTargetRule::ClassGated("INSERT INTO moraine.events SELECT * FROM moraine.raw_events"),
    ),
    (
        "CREATE DATABASE ",
        HeadTargetRule::CannotDisplace(
            "names a database and has no `OR REPLACE` spelling, so on an existing name it is \
             either `IF NOT EXISTS` (a no-op) or an error. It cannot swap out a populated one.",
        ),
    ),
    (
        "CREATE TABLE ",
        HeadTargetRule::CannotDisplace(
            "errors on an existing name unless it carries `IF NOT EXISTS`, in which case it does \
             nothing. `CREATE OR REPLACE TABLE` is a different head and is not on the benign \
             list — the `starts_with` match is what keeps them apart.",
        ),
    ),
    (
        "CREATE VIEW ",
        HeadTargetRule::CannotDisplace(
            "same as `CREATE TABLE`: no `OR REPLACE`, so an existing name is an error or a \
             no-op. The replacing spelling is the entry below, and it is gated.",
        ),
    ),
    (
        "CREATE MATERIALIZED VIEW ",
        HeadTargetRule::ClassGated(
            "CREATE MATERIALIZED VIEW moraine.mv_x TO moraine.events AS SELECT * FROM \
             moraine.raw_events",
        ),
    ),
    (
        "CREATE OR REPLACE VIEW ",
        HeadTargetRule::ClassGated("CREATE OR REPLACE VIEW moraine.events AS SELECT 1"),
    ),
    (
        "DROP VIEW ",
        HeadTargetRule::CannotDisplace(
            "the server refuses it: `DROP VIEW` on a `MergeTree` throws `Code: 80 … is not a \
             View` (executed on 25.12.5.44). This is the one entry whose safety is a property \
             of ClickHouse rather than of the statement, which is exactly the claim \
             `CREATE OR REPLACE VIEW` was wrongly assumed to share.",
        ),
    ),
];

/// Statement heads that **replace** the relation they name, paired with the
/// shape a replacement of a guarded relation reports.
///
/// `CREATE OR REPLACE VIEW` shipped on [`BENIGN_STATEMENT_HEADS`] with no
/// target-class check for three rounds, under the claim that "a view holds no
/// rows, so unlike `CREATE OR REPLACE TABLE` … this one can destroy nothing".
/// **Executed on ClickHouse 25.12.5.44:** a `MergeTree` table with three rows,
/// then `CREATE OR REPLACE VIEW probe.t AS SELECT 99 AS a;` — no error,
/// `count()` goes 3 → 1 and `system.tables.engine` goes `MergeTree` → `View`.
/// Repeated against a `ReplacingMergeTree(event_version)` shaped like
/// `moraine.events`: 2 rows → 1, engine → `View`. ClickHouse's
/// `CREATE OR REPLACE` builds a replacement and `EXCHANGE`s it in; it does not
/// check that what stood there was a view. So
/// `CREATE OR REPLACE VIEW moraine.events AS SELECT 1` is `TRUNCATE` plus
/// `DROP` in one statement, and it was admitted.
///
/// Gated exactly as [`PROTECTED_WRITE_HEADS`] is, on
/// [`relation_is_guarded`] — and `every_benign_head_answers_the_target_class_question`
/// is what makes shipping a fourth head in this family without an answer
/// impossible rather than merely discouraged.
const PROTECTED_REPLACE_HEADS: &[(&str, DeleteShape)] =
    &[("CREATE OR REPLACE VIEW ", DeleteShape::ReplaceRelation)];

/// Whether a bundled migration may not write into or replace `table`.
///
/// Three sources, because the class alone is not the whole answer:
///
/// * an unknown name is guarded (S1: unknown is not deletable);
/// * a [`TableClass::is_protected`] class is guarded;
/// * a [`MIGRATION_PRESERVED_DERIVED_TABLES`] name is guarded **even though it
///   is `Derived`**, which is the entire reason that constant exists. Round 6
///   found the gap by aiming the `ReplacingMergeTree` supersede at the search
///   corpus instead of at `events`: executed in `clickhouse local`,
///   `INSERT INTO moraine.search_documents SELECT * REPLACE ('' AS
///   text_content, doc_version + 1 AS doc_version) FROM
///   moraine.search_documents FINAL` takes `sum(length(text_content))` from 23
///   to **0** through `FINAL`, and it is still 0 after `OPTIMIZE … FINAL`. The
///   `CREATE MATERIALIZED VIEW … TO` twin installs it as standing state.
///   `no_bundled_migration_empties_the_search_corpus` could not see either,
///   because it filters [`migration_row_removals`] and the write heads had
///   already admitted them.
///
/// A declared [`SCHEMA_VIEW_OBJECTS`] name is **not** guarded: replacing a
/// declared view is the drop-and-recreate nine bundled migrations perform, and
/// an *undeclared* view name is unknown, so it is. That is the condition that
/// makes `CREATE OR REPLACE VIEW` safe — the name has to be one somebody
/// registered as a view — rather than the shape of the head.
fn relation_is_guarded(table: &str) -> bool {
    if SCHEMA_VIEW_OBJECTS.contains(&table) {
        return false;
    }
    classify(table).is_none_or(|class| class.is_protected())
        || MIGRATION_PRESERVED_DERIVED_TABLES
            .iter()
            .any(|(name, _)| *name == table)
}

/// The relation a [`PROTECTED_REPLACE_HEADS`] statement replaces, or the one a
/// `CREATE OR REPLACE TABLE` / `REPLACE TABLE` installs over.
///
/// Unlike [`named_relations`]' generic parse this one steps *over*
/// [`NEVER_A_RELATION`] rather than bailing on it, because in
/// `CREATE OR REPLACE VIEW moraine.events AS …` the object keyword stands
/// between the head and the name that is actually at risk.
fn replace_target(normalized: &str) -> Option<String> {
    let mut words = normalized.split_whitespace();
    words.next()?; // CREATE | REPLACE
    let mut candidate = words.next()?;
    while is_relation_noise(candidate)
        || NEVER_A_RELATION.contains(&candidate.to_ascii_uppercase().as_str())
    {
        candidate = words.next()?;
    }
    unqualified_name(candidate)
}

/// The relation a [`PROTECTED_WRITE_HEADS`] statement writes into.
///
/// `INSERT INTO <t>` names it directly. `CREATE MATERIALIZED VIEW <v> TO <t>
/// AS …` names it after `TO`; an MV with no `TO` owns its storage and writes
/// into nothing that existed, so it reports `None`.
fn write_target(normalized: &str, head: &str) -> Option<String> {
    if head == "INSERT INTO " {
        return named_relations(normalized, None).into_iter().next();
    }
    let tokens: Vec<&str> = normalized.split_whitespace().collect();
    let mut idx = 0;
    while idx < tokens.len() {
        let upper = tokens[idx].to_ascii_uppercase();
        if upper == "AS" {
            return None;
        }
        if upper == "TO" {
            return tokens.get(idx + 1).and_then(|name| unqualified_name(name));
        }
        idx += 1;
    }
    None
}

/// Whether one already-normalized statement is on the benign allowlist.
///
/// Returns the matched head — `"ALTER TABLE"` for the clause-parsed case — so a
/// test can assert *which* entry admitted a statement rather than only that
/// something did.
fn benign_shape(statement: &str) -> Option<&'static str> {
    let masked = mask_quoted(&statement.to_ascii_uppercase());
    if masked.starts_with("ALTER TABLE ") {
        let relation = named_relations(statement, None).into_iter().next();
        return alter_is_benign(&masked, relation.as_deref()).then_some("ALTER TABLE");
    }
    let head = BENIGN_STATEMENT_HEADS
        .iter()
        .find(|head| masked.starts_with(**head))
        .copied()?;
    if PROTECTED_WRITE_HEADS
        .iter()
        .any(|(write_head, _)| *write_head == head)
    {
        // An MV with no `TO` owns its storage and writes into nothing that
        // existed, so an absent target is an append nobody needs to authorize.
        //
        // An `INSERT` is the opposite: it always writes somewhere, so a target
        // this parser could not resolve is a target it could not check. The
        // spellings that reach here are `INSERT INTO FUNCTION remote(...)` and
        // `INSERT INTO TABLE FUNCTION ...`, where the name-skip lands on the
        // `NEVER_A_RELATION` keyword `FUNCTION` and `named_relations` comes
        // back empty. `remote('127.0.0.1', 'moraine', 'events')` reaches the
        // same rows the class check exists to protect: executed on 25.12.5.44,
        // an `INSERT INTO FUNCTION remote(...) SELECT * REPLACE ('' AS
        // payload_json, event_version + 1 AS event_version) FROM moraine.events
        // FINAL` took `sum(length(payload_json))` from 87 to 0 through `FINAL`,
        // and it stayed 0 after `OPTIMIZE ... FINAL`.
        //
        // MUTATION: restore the bare `return Some(head)` and
        // `an_insert_whose_target_this_parser_cannot_resolve_is_a_finding` fails.
        if let Some(target) = write_target(statement, head) {
            return (!relation_is_guarded(&target)).then_some(head);
        }
        return (head != "INSERT INTO ").then_some(head);
    }
    if PROTECTED_REPLACE_HEADS
        .iter()
        .any(|(replace_head, _)| *replace_head == head)
    {
        // A head that replaces the relation it names has to resolve that name:
        // unlike the write heads there is no benign "no target" reading, so an
        // unparseable one is a finding.
        let target = replace_target(statement)?;
        return (!relation_is_guarded(&target)).then_some(head);
    }
    Some(head)
}

/// `ALTER … DROP <what>` clauses that destroy data, as opposed to the ones
/// that only change metadata.
///
/// `DROP CONSTRAINT` (sql/025:2), `DROP INDEX`, and `DROP PROJECTION` remove no
/// rows and must not be findings, or the guard would forbid an ordinary schema
/// edit and be turned off.
///
/// **The width bound is `the_destructive_drop_clause_list_is_exactly_the_four_that_remove_storage`,
/// not the negative corpus.** This doc used to claim that each of those three
/// clauses "is a named negative-corpus row on `moraine.events`, so widening
/// this list by one clause turns a green test red". That mechanism cannot
/// work — all three are [`BENIGN_ALTER_OPERATIONS`] entries, so
/// [`benign_shape`] admits the statement and [`delete_shape`] is never
/// consulted, and adding any of them was measured **GREEN** while the corpus
/// was the only bound. Since the inversion this list only *labels*, so the
/// bound had to move to a test that calls [`alter_clause_shape`] directly.
///
/// MUTATION (executed 2026-07-28, against the tree that ships this doc):
/// adding `"DROP INDEX"`, `"DROP CONSTRAINT"` or `"DROP PROJECTION"` here is
/// **RED** in every case, and the single failure is
/// `the_destructive_drop_clause_list_is_exactly_the_four_that_remove_storage`.
const DESTRUCTIVE_ALTER_DROP_CLAUSES: &[&str] = &[
    "DROP PARTITION",
    "DROP PART ",
    "DROP COLUMN",
    "DROP DETACHED",
];

/// Name what one already-comment-stripped, whitespace-normalized statement
/// appears to do, or `None` when no named shape fits.
///
/// **`None` is not a pass.** [`migration_row_removals`] has already decided the
/// statement is not benign by the time it calls this; the answer only picks the
/// label in the finding. That is the whole point of the inversion: a
/// destructive form nobody has named yet reports `None` and is still reported.
///
/// `DROP VIEW` is deliberately not a shape: a view holds no rows, and the tree
/// drops and recreates views constantly (sql/002, 004, 006, 011, 012, 014,
/// 019, 032, 033 — nine of the thirty-nine). A materialized view dropped with
/// `DROP TABLE`
/// rather than `DROP VIEW` *is* reported — [`SCHEMA_VIEW_OBJECTS`] is the
/// declared exemption, and an undeclared name resolves to `None` from
/// [`classify`], which S1 treats as protected.
fn delete_shape(statement: &str) -> Option<DeleteShape> {
    let masked = mask_quoted(&statement.to_ascii_uppercase());
    if masked.starts_with("TRUNCATE ") {
        return Some(DeleteShape::Truncate);
    }
    if masked.starts_with("DELETE FROM") {
        return Some(DeleteShape::DeleteFrom);
    }
    if masked.starts_with("DROP TABLE") || masked.starts_with("DROP DATABASE") {
        return Some(DeleteShape::DropRelation);
    }
    if masked.starts_with("CREATE OR REPLACE TABLE") || masked.starts_with("REPLACE TABLE") {
        return Some(DeleteShape::ReplaceRelation);
    }
    if masked.starts_with("RENAME TABLE")
        || masked.starts_with("RENAME DATABASE")
        || masked.starts_with("EXCHANGE TABLES")
    {
        return Some(DeleteShape::RenameRelation);
    }
    if masked.starts_with("OPTIMIZE ")
        && (masked.contains(" FINAL") || masked.contains(" DEDUPLICATE"))
    {
        return Some(DeleteShape::OptimizeFinal);
    }
    for (head, shape) in PROTECTED_WRITE_HEADS {
        if masked.starts_with(head) && write_target(statement, head).is_some() {
            return Some(*shape);
        }
    }
    for (head, shape) in PROTECTED_REPLACE_HEADS {
        if masked.starts_with(head) && replace_target(statement).is_some() {
            return Some(*shape);
        }
    }
    if masked.starts_with("ALTER TABLE") {
        // Clause by clause, in the same priority order the whole-statement
        // `contains` scan used — but a keyword inside a literal can no longer
        // pick the label, and a keyword in one clause can no longer be read as
        // if it belonged to another.
        // The label of the most destructive clause wins, so the priority is
        // the order of this list rather than the order the clauses appear in:
        // `ADD COLUMN x String, DROP COLUMN payload_json` reports `AlterDrop`.
        let shapes: Vec<DeleteShape> = alter_clauses(&masked)
            .iter()
            .filter_map(|clause| alter_clause_shape(clause))
            .collect();
        return ALTER_SHAPE_PRIORITY
            .iter()
            .find(|shape| shapes.contains(shape))
            .copied();
    }
    None
}

/// Most destructive first. A clause list is reduced to the first of these it
/// contains, which keeps the label stable regardless of clause order.
const ALTER_SHAPE_PRIORITY: &[DeleteShape] = &[
    DeleteShape::AlterDelete,
    DeleteShape::AlterUpdate,
    DeleteShape::AlterModifyTtl,
    DeleteShape::AlterDrop,
    DeleteShape::AlterDetach,
    DeleteShape::AlterClear,
    DeleteShape::AlterReplacePartition,
    DeleteShape::AlterMove,
    DeleteShape::AlterRewriteColumn,
];

/// What one masked `ALTER` clause appears to do, or `None` for a clause that
/// removes nothing — including the benign ones, which never reach here.
fn alter_clause_shape(clause: &str) -> Option<DeleteShape> {
    if clause.starts_with("DELETE") {
        return Some(DeleteShape::AlterDelete);
    }
    if clause.starts_with("UPDATE") {
        return Some(DeleteShape::AlterUpdate);
    }
    if clause.starts_with("MODIFY TTL")
        || (clause.starts_with("MODIFY COLUMN") && modify_column_sets_ttl(clause))
    {
        return Some(DeleteShape::AlterModifyTtl);
    }
    if DESTRUCTIVE_ALTER_DROP_CLAUSES
        .iter()
        .any(|prefix| clause.starts_with(prefix))
    {
        return Some(DeleteShape::AlterDrop);
    }
    if clause.starts_with("DETACH PARTITION") || clause.starts_with("DETACH PART ") {
        return Some(DeleteShape::AlterDetach);
    }
    if clause.starts_with("CLEAR COLUMN") {
        return Some(DeleteShape::AlterClear);
    }
    if clause.starts_with("REPLACE PARTITION") {
        return Some(DeleteShape::AlterReplacePartition);
    }
    if (clause.starts_with("MOVE PARTITION") || clause.starts_with("MOVE PART "))
        && !(clause.contains(" TO DISK") || clause.contains(" TO VOLUME"))
    {
        return Some(DeleteShape::AlterMove);
    }
    if clause.starts_with("MATERIALIZE COLUMN")
        || (clause.starts_with("MODIFY COLUMN") && !modify_column_is_metadata_only(clause))
    {
        return Some(DeleteShape::AlterRewriteColumn);
    }
    None
}

/// What one character of a statement is, to this module's lexer.
///
/// The distinction exists because the two consumers want opposite things from
/// a comment: [`normalize_statement`] deletes it, [`mask_quoted`] must not read
/// it as code. Both get the answer from the same walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CharKind {
    /// Ordinary SQL, and the opening delimiter of a literal or a heredoc.
    Code,
    /// Inside a string literal, a quoted identifier or a heredoc body, up to
    /// and including the closing delimiter.
    Quoted,
    /// Inside a `--`, `/* … */` or `#` comment, delimiters included.
    Comment,
}

/// Walk one statement, reporting what each character is.
///
/// One state machine serves [`normalize_statement`] and [`mask_quoted`], which
/// is the point: a comment stripper and a literal masker that disagree about
/// where a literal or a comment ends are two parsers, and the gap between them
/// is a laundering channel. Round 6 found that gap twice, in the two syntaxes
/// this function did not know:
///
/// * **`#` line comments.** `SELECT 1 # trailing` is valid ClickHouse
///   (executed, 25.12.5.44). The stripper knew `--` and `/* … */` only, so a
///   `'`, `"` or backtick parked in a `#` comment opened a literal ClickHouse
///   never sees; [`mask_quoted`] then masked the rest of the statement,
///   clause-separating comma included, and
///   `ADD COLUMN c String # a " b⏎, DROP COLUMN payload_json` reduced to one
///   benign `ADD COLUMN`. Executed in `clickhouse local`, that statement takes
///   the column list from `['uid','payload_json']` to `['uid','c']`.
/// * **`$$…$$` / `$tag$…$tag$` heredocs.** `SELECT $tag$a'b$tag$` returns
///   `a'b` (executed). An unrecognized heredoc body is read as code, which is
///   fail-closed for a comma but fail-**open** for a parenthesis:
///   `COMMENT $$($$, DROP COLUMN payload_json` drove [`alter_clauses`]' paren
///   depth to 1 so the top-level comma never split. (The depth guard now also
///   fails closed on an unbalanced body, so that vector is dead twice over.)
///
/// `'…'` is a string literal, `"…"` and `` `…` `` are quoted identifiers.
/// `\'` and `''` escape a quote inside a single-quoted literal, matching
/// [`crate::split_sql_statements`].
///
/// A `#` starts a comment wherever it appears outside a literal. ClickHouse is
/// narrower — it wants `# ` or `#!`, and rejects `SELECT 1#2` as an
/// unrecognized token (executed) — so treating every bare `#` as a comment
/// discards a tail of a statement the server would refuse to run anyway.
fn scan_statement(statement: &str, mut visit: impl FnMut(char, CharKind)) {
    let chars: Vec<char> = statement.chars().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        // ---- comments ----
        if ch == '-' && chars.get(idx + 1) == Some(&'-') {
            while idx < chars.len() && chars[idx] != '\n' {
                visit(chars[idx], CharKind::Comment);
                idx += 1;
            }
            continue;
        }
        if ch == '#' {
            while idx < chars.len() && chars[idx] != '\n' {
                visit(chars[idx], CharKind::Comment);
                idx += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(idx + 1) == Some(&'*') {
            visit('/', CharKind::Comment);
            visit('*', CharKind::Comment);
            idx += 2;
            while idx < chars.len() && !(chars[idx] == '*' && chars.get(idx + 1) == Some(&'/')) {
                visit(chars[idx], CharKind::Comment);
                idx += 1;
            }
            if idx < chars.len() {
                visit('*', CharKind::Comment);
                visit('/', CharKind::Comment);
                idx += 2;
            }
            continue;
        }
        // ---- heredocs ----
        if ch == '$' {
            if let Some(tag_len) = heredoc_tag_len(&chars, idx) {
                // `$tag$` opens; everything through the matching `$tag$`
                // closes. The opening delimiter reports Code so the masked
                // text still shows a heredoc where one stood.
                for offset in 0..tag_len {
                    visit(chars[idx + offset], CharKind::Code);
                }
                let open = &chars[idx..idx + tag_len];
                let mut cursor = idx + tag_len;
                while cursor < chars.len() {
                    if chars[cursor] == '$' && chars[cursor..].starts_with(open) {
                        for offset in 0..tag_len {
                            visit(chars[cursor + offset], CharKind::Quoted);
                        }
                        cursor += tag_len;
                        break;
                    }
                    visit(chars[cursor], CharKind::Quoted);
                    cursor += 1;
                }
                idx = cursor;
                continue;
            }
        }
        // ---- literals and quoted identifiers ----
        if ch == '\'' || ch == '"' || ch == '`' {
            visit(ch, CharKind::Code);
            idx += 1;
            while idx < chars.len() {
                let inner = chars[idx];
                visit(inner, CharKind::Quoted);
                if inner == '\\' && idx + 1 < chars.len() {
                    visit(chars[idx + 1], CharKind::Quoted);
                    idx += 2;
                    continue;
                }
                if inner == ch {
                    if ch == '\'' && chars.get(idx + 1) == Some(&'\'') {
                        visit('\'', CharKind::Quoted);
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            continue;
        }
        visit(ch, CharKind::Code);
        idx += 1;
    }
}

/// Length of the `$tag$` delimiter opening at `start`, or `None` when the `$`
/// is not a heredoc open.
///
/// The tag charset is the one ClickHouse 25.12.5.44 accepts, read off by
/// probing each candidate character with `EXPLAIN AST` (2026-07-27):
/// `$a$`, `$A9$`, `$_$`, `$a-b$`, `$a.b$`, `$a+b$` and the empty `$$` are
/// valid; `$a#b$`, `$a"b$`, `$a(b$` and `$a b$` are not. A `$` whose tag would
/// contain anything else is not an open — which is why `SELECT 5 $ 5` is a
/// syntax error rather than an unterminated heredoc.
///
/// The delimiter must also *close*: a lone `$$` at the end of a statement
/// opens nothing, because masking to end-of-statement on an unmatched
/// delimiter would let one stray character blank a clause.
fn heredoc_tag_len(chars: &[char], start: usize) -> Option<usize> {
    let is_tag_char =
        |ch: char| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.' || ch == '+';
    let mut cursor = start + 1;
    while cursor < chars.len() && is_tag_char(chars[cursor]) {
        cursor += 1;
    }
    if chars.get(cursor) != Some(&'$') {
        return None;
    }
    let tag_len = cursor - start + 1;
    let open = &chars[start..start + tag_len];
    let closes = chars
        .get(start + tag_len..)?
        .windows(tag_len)
        .any(|window| window == open);
    closes.then_some(tag_len)
}

/// Strip `--`, `#` and `/* … */` comments, then collapse all whitespace runs
/// to single spaces.
///
/// **Comments are stripped only outside a string literal, and a literal is
/// only a literal outside a comment.** Both halves have been a live defect:
///
/// * The implementation before round 5 cut every line at its first `--`
///   unconditionally, which made `ALTER TABLE moraine.events MODIFY COLUMN c
///   COMMENT 'a -- b', DROP COLUMN payload_json` normalize to a statement
///   ending mid-literal — the `DROP COLUMN` vanished.
/// * The implementation before round 6 did not know `#` at all, so a quote
///   parked inside a `#` comment opened a literal that ClickHouse never sees.
///   See [`scan_statement`].
///
/// `comments_are_stripped_only_outside_a_string_literal` and
/// `the_lexer_knows_every_clickhouse_comment_and_quote_syntax` are the guards.
fn normalize_statement(statement: &str) -> String {
    let mut out = String::with_capacity(statement.len());
    scan_statement(statement, |ch, kind| {
        // A comment contributes whitespace, exactly as it does to the server.
        out.push(if kind == CharKind::Comment { ' ' } else { ch });
    });
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Replace the *contents* of every string literal, quoted identifier and
/// heredoc with `_`, keeping the delimiters, the token count and the
/// whitespace.
///
/// Every substring and comma test the `ALTER` parser makes runs against the
/// masked text, so a literal cannot smuggle a keyword or a clause separator
/// past it. `ALTER TABLE moraine.events ADD COLUMN c String DEFAULT
/// 'x TO DISK y', MOVE PARTITION '202601' TO TABLE moraine.mcp_open_turns` was
/// **admitted** before this existed: the `MOVE` clause's tiering test searched
/// the whole statement, and the laundering substring sat in an unrelated
/// literal.
///
/// Comments become spaces rather than `_`, which is what the server does with
/// them. That choice is about the false-positive rate and **not** about safety,
/// and the doc must not claim otherwise: MUTATION (executed 2026-07-28)
/// emitting `_` for comment characters leaves the crate GREEN, because a
/// comment ends at its newline and the `_` run it leaves behind becomes a
/// clause head that is on no benign list. The failure direction is
/// over-reporting. In the pipeline the question does not arise at all —
/// [`normalize_statement`] has already deleted every comment before
/// [`benign_shape`] and [`delete_shape`] mask anything.
fn mask_quoted(statement: &str) -> String {
    let mut out = String::with_capacity(statement.len());
    scan_statement(statement, |ch, kind| {
        // The opening delimiter reports `Code` and is kept, so a masked
        // literal is still visibly a literal; everything up to and including
        // the closing delimiter becomes `_`.
        out.push(match kind {
            CharKind::Code => ch,
            CharKind::Quoted => '_',
            CharKind::Comment => ' ',
        });
    });
    out
}

/// Strip the database qualifier, backticks/quotes, and any trailing
/// punctuation from one identifier token.
fn unqualified_name(candidate: &str) -> Option<String> {
    // `CREATE TABLE moraine.tight(a String)` puts the column list flush
    // against the name, so cut at the paren before splitting the qualifier.
    let head = candidate.split('(').next().unwrap_or(candidate);
    let unqualified = head.rsplit('.').next().unwrap_or(head);
    let cleaned = unqualified.trim_matches(|c| c == '`' || c == '"' || c == ';' || c == ',');
    (!cleaned.is_empty()).then(|| cleaned.to_string())
}

/// Tokens that stand between a statement keyword and the relation name.
///
/// `FROM` and `INTO` are here so that one leading-keyword skip serves every
/// statement head — `DELETE FROM t`, `INSERT INTO t`, `ALTER TABLE t`,
/// `OPTIMIZE TABLE t` and `TRUNCATE DATABASE d` all reduce the same way. That
/// is what lets [`named_relations`] work for a statement whose shape nobody has
/// named, which is the case the gate is now built around.
fn is_relation_noise(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "TABLE"
            | "TABLES"
            | "DATABASE"
            | "IF"
            | "NOT"
            | "EXISTS"
            | "OR"
            | "REPLACE"
            | "FROM"
            | "INTO"
    )
}

/// Object keywords that the leading-keyword skip can land on, and that no
/// relation in this database is named.
///
/// A finding filed against one of these is a **phantom**: it reads like a real
/// table and is not one. `CREATE OR REPLACE VIEW moraine.v_live_events AS …`
/// used to be reported against a relation literally named `VIEW`, because
/// `is_relation_noise` eats `OR` and `REPLACE` and then takes `VIEW` as the
/// name — which classifies as unknown, and unknown is protected.
///
/// Landing here yields an empty parse, which
/// [`migration_row_removals`] reports as [`UNPARSED_RELATION`]: still a
/// finding, still `NeverDelete`, but the message no longer names a table that
/// does not exist. Fail-closed is preserved; only the label changes.
const NEVER_A_RELATION: &[&str] = &[
    "VIEW",
    "MATERIALIZED",
    "DICTIONARY",
    "FUNCTION",
    "TEMPORARY",
    "LIVE",
    "WINDOW",
];

/// Every table a non-benign statement names, unqualified.
///
/// Handles `moraine.t`, `` `moraine`.t ``, `` `moraine`.`t` ``, a bare `t`,
/// and the `IF EXISTS` variants of `TRUNCATE`/`DROP`.
///
/// `shape` is `Option` because the caller reaches here for **any** statement
/// the benign allowlist rejected, named shape or not. Only
/// [`DeleteShape::RenameRelation`] changes the parse: it names two relations
/// and the displaced one may be either operand — `RENAME TABLE moraine.events
/// TO moraine.attic` empties the name `events`, and `RENAME TABLE
/// moraine.scratch TO moraine.events` displaces whatever `events` was. Every
/// other statement, including the unnamed ones, yields exactly one name, which
/// `every_shape_but_rename_names_exactly_one_relation` pins.
///
/// An empty result is a **parse failure**, not an absence, and
/// [`migration_row_removals`] reports it as [`UNPARSED_RELATION`] rather than
/// dropping the statement.
fn named_relations(statement: &str, shape: Option<DeleteShape>) -> Vec<String> {
    let normalized = normalize_statement(statement);
    let mut words = normalized.split_whitespace();
    // Skip the statement keyword; `is_relation_noise` eats the rest of the
    // prefix, so one is enough for every spelling.
    if words.next().is_none() {
        return Vec::new();
    }

    // A materialized view's extent is its `TO` target, not the token after the
    // statement keyword — which is `MATERIALIZED`, a phantom.
    if shape == Some(DeleteShape::MaterializedViewInto) {
        return write_target(statement, "CREATE MATERIALIZED VIEW ")
            .into_iter()
            .collect();
    }

    // `CREATE OR REPLACE VIEW moraine.events` puts `VIEW` — a
    // [`NEVER_A_RELATION`] keyword — where the generic parse looks, so the
    // generic parse would report `<unparsed>` for the one statement whose
    // whole hazard is the name it displaces. The same walk serves
    // `CREATE OR REPLACE TABLE` and `REPLACE TABLE`.
    if shape == Some(DeleteShape::ReplaceRelation) {
        return replace_target(&normalized).into_iter().collect();
    }

    if shape == Some(DeleteShape::RenameRelation) {
        let mut names = Vec::new();
        for word in words {
            let upper = word.to_ascii_uppercase();
            // `ON CLUSTER '<name>'` names a cluster, not a relation.
            if upper == "ON" {
                break;
            }
            if upper == "TO" || upper == "AND" || is_relation_noise(word) {
                continue;
            }
            if let Some(name) = unqualified_name(word) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        return names;
    }

    let mut candidate = match words.next() {
        Some(word) => word,
        None => return Vec::new(),
    };
    while is_relation_noise(candidate) {
        candidate = match words.next() {
            Some(word) => word,
            None => return Vec::new(),
        };
    }
    if NEVER_A_RELATION.contains(&candidate.to_ascii_uppercase().as_str()) {
        return Vec::new();
    }
    unqualified_name(candidate).into_iter().collect()
}

/// The table a `CREATE TABLE` statement installs, unqualified, or `None` for
/// every other statement — including `CREATE MATERIALIZED VIEW` and
/// `CREATE VIEW`, which install no physical relation and hold no bytes.
///
/// `CREATE OR REPLACE TABLE` and the bare `REPLACE TABLE` install a physical
/// relation too. Before they were recognized here, installing a table that way
/// was a **double** miss: `classification_gaps`' migration side did not see the
/// new unclassified table, and [`delete_shape`] did not see that the statement
/// had emptied whatever stood in its place.
fn created_table(statement: &str) -> Option<String> {
    let normalized = normalize_statement(statement);
    let upper = normalized.to_ascii_uppercase();
    if !(upper.starts_with("CREATE TABLE")
        || upper.starts_with("CREATE OR REPLACE TABLE")
        || upper.starts_with("REPLACE TABLE"))
    {
        return None;
    }
    let mut words = normalized.split_whitespace();
    words.next()?; // CREATE | REPLACE
    let mut candidate = words.next()?;
    while is_relation_noise(candidate) {
        candidate = words.next()?;
    }
    unqualified_name(candidate)
}

/// Every physical table one migration's SQL creates, in statement order.
pub fn migration_created_tables(sql: &str) -> Vec<String> {
    crate::split_sql_statements(sql)
        .iter()
        .filter_map(|statement| created_table(statement))
        .collect()
}

/// Scan one migration's SQL for statements that are not on the benign
/// allowlist and name a protected ([`TableClass::is_protected`]) table.
///
/// The pre-#603 guards this replaces were substring matches over exactly one
/// statement form, pinned by a hard-coded `.find(|m| m.version == "034")`
/// lookup, so `ALTER TABLE moraine.events DELETE WHERE …`,
/// `DELETE FROM moraine.events`, and `TRUNCATE TABLE IF EXISTS moraine.events`
/// all passed them untouched — and a newly added migration was covered by no
/// guard of the family at all. This function is per-statement, fail-closed,
/// and runs over **every** bundled migration.
///
/// An unclassified table is reported as a finding too: `classify` returning
/// `None` is the S1 hard error, and a migration that deletes from a table
/// nobody classified is precisely the regression S1 exists to catch.
pub fn migration_delete_findings(version: &str, sql: &str) -> Vec<MigrationDeleteFinding> {
    migration_row_removals(version, sql)
        .into_iter()
        .filter(|finding| finding.class.is_protected())
        .collect()
}

/// Stand-in table name for a non-benign statement whose relation could not be
/// parsed at all.
///
/// It classifies as `NeverDelete` like any other unknown name, so an
/// unparseable statement fails the gate instead of falling out of it. Nothing
/// in the tree produces one; the alternative is a `Vec::new()` that reads
/// exactly like "this statement is fine".
pub const UNPARSED_RELATION: &str = "<unparsed>";

/// Every statement in one migration that is not on the benign allowlist,
/// **regardless of the class of the table it names**, with the allowlist
/// applied.
///
/// The gate is [`benign_shape`] and it is fail-closed: a statement is a finding
/// unless its head, or every one of its `ALTER` operations, is a named benign
/// form. [`delete_shape`] then labels the finding and is free to answer `None`.
///
/// [`migration_delete_findings`] is this filtered to the protected classes. A
/// caller that must guard one specific derived relation — the search corpus,
/// say — uses this, because "`Derived` is deletable" is a statement about the
/// reclaimer's authority model, not a licence for a bundled migration to empty
/// the BM25 corpus on upgrade.
pub fn migration_row_removals(version: &str, sql: &str) -> Vec<MigrationDeleteFinding> {
    let views: BTreeSet<&str> = SCHEMA_VIEW_OBJECTS.iter().copied().collect();
    // (version, table, shape). A `None` shape matches no entry, so a statement
    // nobody has named is a finding even inside an exempted migration.
    let is_exempt = |table: &str, shape: Option<DeleteShape>| {
        let Some(shape) = shape else {
            return false;
        };
        MIGRATION_DELETE_ALLOWLIST.iter().any(|exemption| {
            exemption.version == version
                && exemption.tables.contains(&table)
                && exemption.shapes.contains(&shape)
        })
    };
    crate::split_sql_statements(sql)
        .into_iter()
        .flat_map(|statement| {
            let normalized = normalize_statement(&statement);
            // A statement that is only a comment survives `split_sql_statements`
            // and normalizes to nothing. It names no relation and destroys none.
            if normalized.is_empty() {
                return Vec::new();
            }
            if benign_shape(&normalized).is_some() {
                return Vec::new();
            }
            let shape = delete_shape(&normalized);
            let mut relations = named_relations(&normalized, shape);
            if relations.is_empty() {
                relations.push(UNPARSED_RELATION.to_string());
            }
            relations
                .into_iter()
                .filter(|table| !is_exempt(table.as_str(), shape))
                // A view holds no rows. `DROP TABLE moraine.search_term_stats`
                // (sql/004:213, sql/006:4) names an object migration 032
                // replaced with a `CREATE VIEW`, so the drop destroys nothing.
                // Scoped to `DropRelation`: `TRUNCATE TABLE
                // moraine.search_term_stats` would be a statement ClickHouse
                // rejects against a view and accepts against a table someone
                // reinstated under that name, so it stays a finding —
                // `truncating_a_declared_view_name_is_still_a_finding` is what
                // stops this exemption from being widened to every shape.
                .filter(|table| {
                    !(shape == Some(DeleteShape::DropRelation) && views.contains(table.as_str()))
                })
                .map(|table| {
                    // Unknown => treated as protected (S1: unknown is not
                    // deletable).
                    let class = classify(&table).unwrap_or(TableClass::NeverDelete);
                    MigrationDeleteFinding {
                        version: version.to_string(),
                        table,
                        class,
                        shape,
                        statement: normalized.clone(),
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundled_migrations;

    #[test]
    fn every_class_is_enumerated() {
        // Adding a TableClass variant without adding it to ALL breaks every
        // per-bucket fold silently; this is the one place that notices.
        assert_eq!(TableClass::ALL.len(), 5);
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for class in TableClass::ALL {
            assert!(seen.insert(class.as_str()), "duplicate class {class:?}");
            assert!(!class.label().is_empty());
        }
    }

    /// **G-CLASSIFY.** Fails for: a schema table exists with no bucket.
    /// Denomination: set equality, both directions.
    ///
    /// MUTATION (executed 2026-07-27): add `"reclaim_scratch"` to
    /// `REQUIRED_SCHEMA_OBJECTS` and nothing else =>
    /// `unclassified_schema_objects` is `["reclaim_scratch"]` and this test
    /// fails. The guard bounds the direction *schema grows without
    /// classification*; `classified_but_unregistered` bounds the opposite
    /// direction (classification names a table the schema does not require),
    /// which the second mutation below exercises.
    ///
    /// MUTATION (executed 2026-07-27): remove `"tool_io"` from
    /// `REQUIRED_SCHEMA_OBJECTS` => `classified_but_unregistered` is
    /// `["tool_io"]` and this test fails.
    ///
    /// MUTATION (executed 2026-07-27): append
    /// `CREATE TABLE moraine.reclaim_scratch (a String) ENGINE = MergeTree
    /// ORDER BY a;` to `sql/003_ingest_heartbeats.sql`, classifying and
    /// registering it nowhere => `unclassified_migration_tables` is
    /// `["reclaim_scratch"]` and this test fails. Before the third side
    /// existed that mutation left the suite at 177/0, because the other two
    /// fields compare two Rust lists that both still agreed with each other.
    #[test]
    fn classification_and_required_schema_objects_are_mutually_exhaustive() {
        let gaps = classification_gaps();
        assert!(
            gaps.unclassified_schema_objects.is_empty(),
            "schema objects with no storage class (add them to CLASSIFIED_TABLES, or to \
             SCHEMA_VIEW_OBJECTS if they are views): {:?}",
            gaps.unclassified_schema_objects
        );
        assert!(
            gaps.classified_but_unregistered.is_empty(),
            "classified tables missing from REQUIRED_SCHEMA_OBJECTS (register them, or add them \
             to UNREGISTERED_PHYSICAL_TABLES with a reason): {:?}",
            gaps.classified_but_unregistered
        );
        assert!(
            gaps.stale_view_declarations.is_empty(),
            "SCHEMA_VIEW_OBJECTS names objects the schema handshake no longer requires: {:?}",
            gaps.stale_view_declarations
        );
        assert!(
            gaps.unclassified_migration_tables.is_empty(),
            "a bundled migration CREATEs a table with no storage class (classify it in \
             CLASSIFIED_TABLES): {:?}",
            gaps.unclassified_migration_tables
        );
        assert!(gaps.is_empty());
    }

    /// The third side, bounded in the direction that proves it reads `sql/` at
    /// all rather than vacuously returning an empty vector.
    #[test]
    fn the_migration_side_of_exhaustiveness_actually_parses_create_table() {
        // Every physical table the schema installs is created by some bundled
        // migration, except `schema_migrations`, which the runner creates.
        let created: BTreeSet<String> = crate::bundled_migrations()
            .iter()
            .flat_map(|migration| migration_created_tables(migration.sql))
            .collect();
        assert!(created.contains("events"), "{created:?}");
        assert!(created.contains("storage_reclaim_ledger"));
        assert!(created.contains("file_attention_project_roots"));
        assert!(
            !created.contains("schema_migrations"),
            "the migration ledger is created by the runner, not by sql/"
        );
        let classified: BTreeSet<&str> = CLASSIFIED_TABLES
            .iter()
            .map(|entry| entry.name)
            .filter(|name| !name.starts_with("system."))
            .collect();
        assert_eq!(
            created.len(),
            classified.len() - 1,
            "every classified Moraine table except `schema_migrations` is created by a bundled \
             migration: created={created:?}"
        );

        // A materialized view is not a physical table and must not be counted:
        // the tree creates dozens, and counting them would force every MV into
        // CLASSIFIED_TABLES.
        assert_eq!(
            created_table("CREATE MATERIALIZED VIEW moraine.mv_x TO moraine.events AS SELECT 1"),
            None
        );
        assert_eq!(created_table("CREATE VIEW moraine.v_x AS SELECT 1"), None);
        assert_eq!(
            created_table("CREATE TABLE IF NOT EXISTS `moraine`.`brand_new` (a String)"),
            Some("brand_new".to_string())
        );
        assert_eq!(
            created_table("CREATE TABLE moraine.tight(a String)"),
            Some("tight".to_string())
        );
        // `CREATE OR REPLACE TABLE` installs a physical relation too. Missing
        // it was a double miss: the new table escaped classification here AND
        // the statement's emptying of whatever it replaced escaped
        // `delete_shape`.
        //
        // MUTATION (executed 2026-07-27): remove the `CREATE OR REPLACE
        // TABLE`/`REPLACE TABLE` prefixes from `created_table` => FAILS on
        // these two rows.
        assert_eq!(
            created_table("CREATE OR REPLACE TABLE moraine.brand_new (a String)"),
            Some("brand_new".to_string())
        );
        assert_eq!(
            created_table("REPLACE TABLE `moraine`.`brand_new` (a String)"),
            Some("brand_new".to_string())
        );
    }

    #[test]
    fn classification_covers_the_hosts_thirty_two_physical_tables() {
        // Verified against the reference host 2026-07-27: `system.tables`
        // reports 32 non-view relations in `moraine`. This build adds
        // `storage_reclaim_ledger` (migration 038), so the classified Moraine
        // table count is 33 and the total including system logs is 36.
        let moraine_tables = CLASSIFIED_TABLES
            .iter()
            .filter(|entry| !entry.name.starts_with("system."))
            .count();
        assert_eq!(moraine_tables, 33, "classified Moraine tables");
        assert_eq!(CLICKHOUSE_SYSTEM_LOGS.len(), 3);
        assert_eq!(CLASSIFIED_TABLES.len(), moraine_tables + 3);
    }

    #[test]
    fn every_classification_carries_a_rationale_and_no_duplicates() {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for entry in CLASSIFIED_TABLES {
            assert!(
                seen.insert(entry.name),
                "duplicate classification for `{}`",
                entry.name
            );
            assert!(
                entry.rationale.len() > 40,
                "`{}` needs a rationale explaining why this class and not a looser one",
                entry.name
            );
        }
        for log in CLICKHOUSE_SYSTEM_LOGS {
            assert_eq!(
                classify(log),
                Some(TableClass::Telemetry),
                "system log `{log}` must be classified telemetry"
            );
        }
    }

    #[test]
    fn unknown_tables_are_unclassified_rather_than_derived() {
        assert_eq!(classify("not_a_moraine_table"), None);
        assert_eq!(classify(""), None);
        // Near-misses must not classify: a prefix/suffix match would make a
        // future `events_v2` inherit `events`' class by accident.
        assert_eq!(classify("events_v2"), None);
        assert_eq!(classify("moraine.events"), None);
    }

    #[test]
    fn protected_classes_are_exactly_the_three_the_emitter_gates_on() {
        assert!(TableClass::CanonicalHistory.is_protected());
        assert!(TableClass::RawAudit.is_protected());
        assert!(TableClass::NeverDelete.is_protected());
        assert!(!TableClass::Derived.is_protected());
        assert!(!TableClass::Telemetry.is_protected());
    }

    /// **G-NEVERDELETE.** Fails for: a never-delete table losing its class,
    /// and for a new never-delete table arriving with no coverage.
    /// Denomination: exact name set, both directions.
    ///
    /// This is set equality rather than a `for` loop over a chosen few.
    /// `is_protected()` is the sole gate in BOTH `emit_delete_statement` and
    /// `migration_delete_findings`, so a never-delete table quietly reclassed
    /// `Telemetry` becomes reachable by a telemetry token and eligible for a
    /// future TTL — and the previous loop covered 7 of the 9 entries, leaving
    /// `ingest_checkpoints` (whose own rationale says "Looks like retired
    /// telemetry; is not") and `publication_diagnostic_events` uncovered.
    ///
    /// MUTATION (executed 2026-07-27), each run separately: flip
    /// `ingest_checkpoints` to `Telemetry`; flip `publication_diagnostic_events`
    /// to `Derived`; flip `search_conversation_terms` to `Derived` => this test
    /// FAILS on the set equality in each case. All three left the suite at
    /// 177/0 before this guard existed.
    ///
    /// MUTATION (executed 2026-07-27): promote a table INTO the class without
    /// listing it — flip `ingest_heartbeats` from `Telemetry` to
    /// `NeverDelete` => this test FAILS, forcing the roster to be updated
    /// deliberately. That is the *width* bound: the guard is a set, so it
    /// cannot be satisfied by a subset in either direction, and a new
    /// never-delete table cannot arrive uncovered.
    #[test]
    fn the_never_delete_roster_is_exactly_these_tables() {
        assert_eq!(
            tables_of_class(TableClass::NeverDelete),
            vec![
                "ingest_append_control",
                "ingest_checkpoint_transitions",
                "ingest_checkpoints",
                "mcp_read_index_state",
                "publication_diagnostic_events",
                "published_source_generations",
                "schema_migrations",
                "search_conversation_terms",
                "source_generation_publication_readiness",
                "storage_reclaim_ledger",
            ],
            "every never-delete table must be listed here, and every table listed here must still \
             be never-delete: `is_protected()` is the only gate the emitter and the migration \
             invariant consult"
        );
        for control in tables_of_class(TableClass::NeverDelete) {
            assert_eq!(classify(control), Some(TableClass::NeverDelete));
            assert!(
                TableClass::NeverDelete.is_protected(),
                "`{control}` is only unreachable because never-delete is protected"
            );
        }

        // The allocator rationale must stay attached to the table it explains:
        // a TTL on checkpoint history regresses max(checkpoint_revision) from
        // a wall-clock-ms seed to a colliding small value.
        let checkpoint = classification("ingest_checkpoint_transitions").expect("classified");
        assert!(checkpoint.rationale.contains("1 784 588 514 188"));
        assert!(checkpoint.rationale.contains("monotone allocator"));
        // H5, as the reason `search_conversation_terms` is not merely derived.
        let terms = classification("search_conversation_terms").expect("classified");
        assert!(
            terms.rationale.to_lowercase().contains("no tombstone path"),
            "{terms:?}"
        );
        assert!(terms.rationale.contains("OQ-1"));
    }

    // ---- §4 S4 migration invariant ------------------------------------

    /// **G-MIGRATION.** Fails for: a bundled migration removing rows from a
    /// canonical, raw-audit, or never-delete table.
    /// Denomination: per-statement parse over every bundled migration.
    ///
    /// Every mutation below is appended to `sql/003_ingest_heartbeats.sql`,
    /// which carries no statement-count or content assertion of its own, so
    /// the only test that reacts is the invariant under examination.
    ///
    /// MUTATION (executed 2026-07-27), all three run separately against an
    /// isolated copy of the tree, all three pass the *old* substring guards:
    ///   1. append `ALTER TABLE moraine.events DELETE WHERE 1;` => FAILS
    ///      here, finding `{table: "events", class: CanonicalHistory}`.
    ///   2. append `DELETE FROM moraine.events;` => FAILS here.
    ///   3. append `TRUNCATE TABLE IF EXISTS moraine.events;` => FAILS here.
    ///
    /// Four further mutations, all appended to
    /// `sql/003_ingest_heartbeats.sql` and all run separately, cover the
    /// `DROP` family. Every one of them left the suite at 177/0 before the
    /// `DropRelation`/`AlterDrop` shapes existed, and
    /// `DROP TABLE moraine.events` removes strictly more canonical history
    /// than the `TRUNCATE` the shape list did catch:
    ///   4. `DROP TABLE IF EXISTS moraine.events;` => FAILS here.
    ///   5. `ALTER TABLE moraine.events DROP PARTITION '202601';` => FAILS.
    ///   6. `ALTER TABLE moraine.events DROP COLUMN payload_json;` => FAILS.
    ///   7. `DROP TABLE moraine.published_source_generations;` => FAILS with
    ///      class `NeverDelete`.
    ///
    /// MUTATION (executed 2026-07-27): remove the `DROP TABLE`/`DROP DATABASE`
    /// arm from `delete_shape` and append mutation 4 => the shape test
    /// `every_row_removing_shape_is_recognized_including_the_ones_the_old_guard_missed`
    /// FAILS, so the new arm is load-bearing rather than decorative. Since the
    /// inversion, that mutation no longer changes *this* test's answer: the
    /// finding survives with a `None` shape, because the gate is
    /// [`benign_shape`] and `DROP TABLE` is not on it.
    ///
    /// The four mutations the inversion added, each appended to
    /// `sql/003_ingest_heartbeats.sql`, each run separately (executed
    /// 2026-07-27), each GREEN here before the inversion and RED after:
    ///   8.  `ALTER TABLE moraine.events UPDATE payload_json = '' WHERE 1;`
    ///   9.  `ALTER TABLE moraine.events REPLACE PARTITION '202601' FROM
    ///       moraine.scratch;`
    ///   10. `OPTIMIZE TABLE moraine.events FINAL DEDUPLICATE BY event_uid;`
    ///   11. `ALTER TABLE moraine.events DETACH PARTITION '202601';`
    ///
    /// Two controls bound the opposite direction — the gate is not simply
    /// "any statement fails". Appending `TRUNCATE TABLE moraine.mcp_open_turns;`
    /// (Derived) leaves this test GREEN, and so does appending
    /// `DROP VIEW IF EXISTS moraine.v_live_events;` +
    /// `DROP TABLE IF EXISTS moraine.search_term_stats;` +
    /// `ALTER TABLE moraine.event_links DROP CONSTRAINT IF EXISTS c_x;`
    /// (both executed 2026-07-27). The standing upper bound is the tree itself:
    /// the gate runs over all 294 statements of the 39 bundled migrations and
    /// is green, including the 64 `ADD COLUMN`s, the 41 `DROP VIEW`s, the 32
    /// `CREATE TABLE`s and the 25 `INSERT`s.
    ///
    /// Round 5 added five more end-to-end mutations of the same kind, each a
    /// statement the *inverted* gate admitted and each `EXPLAIN AST`-valid on
    /// ClickHouse 25.12.5.44 (executed 2026-07-27, each RED here now):
    ///   12. `ALTER TABLE moraine.events MODIFY COLUMN payload_json String TTL
    ///       ingested_at + INTERVAL 1 DAY;`
    ///   13. `ALTER TABLE moraine.events MATERIALIZE COLUMN payload_json;`
    ///   14. `INSERT INTO moraine.events SELECT * REPLACE ('' AS payload_json,
    ///       event_version + 1 AS event_version) FROM moraine.events FINAL;`
    ///   15. `CREATE MATERIALIZED VIEW moraine.mv_z TO moraine.events AS SELECT
    ///       * FROM moraine.raw_events;`
    ///   16. `ALTER TABLE moraine.events MODIFY COLUMN c COMMENT 'a -- b',
    ///       DROP COLUMN payload_json;` — the comment-laundered drop, which the
    ///       old normalizer truncated to a benign `MODIFY COLUMN`.
    ///
    /// And three more controls, each of which the *round-4* gate rejected and
    /// this one admits: `ALTER TABLE moraine.events ADD PROJECTION p_x (SELECT
    /// a, b ORDER BY ts);`, `CREATE OR REPLACE VIEW moraine.v_z AS SELECT 1;`,
    /// and `ALTER TABLE moraine.events ADD COLUMN note String DEFAULT 'x'
    /// COMMENT 'drop this';`.
    #[test]
    fn no_bundled_migration_removes_protected_rows() {
        let mut findings = Vec::new();
        for migration in bundled_migrations() {
            findings.extend(migration_delete_findings(migration.version, migration.sql));
        }
        assert!(
            findings.is_empty(),
            "bundled migrations must not contain a statement outside the benign allowlist that \
             names a canonical, raw-audit, or never-delete table. Findings: {findings:#?}. If the \
             statement is genuinely benign, add its shape to BENIGN_STATEMENT_HEADS or \
             BENIGN_ALTER_OPERATIONS *and* a corpus row that witnesses it. If the migration \
             legitimately must destroy those rows, add the (version, table) pair to \
             MIGRATION_DELETE_ALLOWLIST with a reason."
        );
    }

    /// Every `(relation, shape)` an unexempted migration would be reported
    /// for, in statement order. The allowlist's own denominator.
    ///
    /// It mirrors [`migration_row_removals`] with only the allowlist removed —
    /// including the `DropRelation`-on-a-declared-view filter. Dropping that
    /// filter here made the denominator wider than the numerator it is compared
    /// against, which stayed invisible for as long as no migration that drops a
    /// declared view name (`004`, `006`) also had an allowlist entry.
    fn unexempted_relations(sql: &str) -> Vec<(String, Option<DeleteShape>)> {
        let views: BTreeSet<&str> = SCHEMA_VIEW_OBJECTS.iter().copied().collect();
        crate::split_sql_statements(sql)
            .into_iter()
            .flat_map(|statement| {
                let normalized = normalize_statement(&statement);
                if normalized.is_empty() || benign_shape(&normalized).is_some() {
                    return Vec::new();
                }
                let shape = delete_shape(&normalized);
                let mut relations = named_relations(&normalized, shape);
                if relations.is_empty() {
                    relations.push(UNPARSED_RELATION.to_string());
                }
                relations
                    .into_iter()
                    .filter(|relation| {
                        !(shape == Some(DeleteShape::DropRelation)
                            && views.contains(relation.as_str()))
                    })
                    .map(|relation| (relation, shape))
                    .collect()
            })
            .collect()
    }

    /// **G-ALLOWLIST.** Every exemption names a migration that still exists,
    /// and every `(version, table, shape)` triple in its cross product really
    /// would be a finding without it. An entry that exempts nothing is an entry
    /// nobody would notice going stale, and a cross product wider than the
    /// migration is a licence nobody asked for.
    ///
    /// MUTATION (executed 2026-07-27): drop `"012"` and `"013"` from the
    /// allowlist => FAILS in `no_bundled_migration_removes_protected_rows`
    /// with twelve findings on `events`/`raw_events`, all `AlterUpdate`. That
    /// is the inversion earning its keep: those twenty-one statements were
    /// invisible to four consecutive rounds of destructive-shape enumeration.
    ///
    /// MUTATION (executed 2026-07-27): merge 032's two entries into one whose
    /// cross product is `{search_documents, search_postings,
    /// search_conversation_terms} × {AlterRewriteColumn,
    /// MaterializedViewInto}` => FAILS here on the four unwitnessed pairs. That
    /// is what stops an entry from being widened by adding a table to the list
    /// that already carries the shape somebody wanted.
    ///
    /// The key's own width lives in `the_allowlist_key_is_version_table_and_shape`.
    #[test]
    fn every_allowlist_entry_is_load_bearing_and_still_exempts_a_live_migration() {
        let versions: Vec<&str> = MIGRATION_DELETE_ALLOWLIST
            .iter()
            .map(|exemption| exemption.version)
            .collect();
        assert_eq!(
            versions,
            vec![
                "004", "009", "010", "011", "012", "012", "013", "014", "014", "020", "031", "032",
                "032", "032", "036"
            ]
        );

        for exemption in MIGRATION_DELETE_ALLOWLIST {
            let version = exemption.version;
            assert!(exemption.reason.len() > 40, "`{version}` needs a reason");
            assert!(!exemption.tables.is_empty(), "`{version}` exempts no table");
            assert!(!exemption.shapes.is_empty(), "`{version}` exempts no shape");
            let migration = bundled_migrations()
                .into_iter()
                .find(|migration| migration.version == version)
                .unwrap_or_else(|| {
                    panic!("the allowlist must not outlive the migration `{version}` it exempts")
                });
            // Unexempted, every pair in the cross product really is reported:
            // this is what proves the allowlist is load-bearing rather than
            // decorative, and that the entry is not wider than its migration.
            let unexempted = unexempted_relations(migration.sql);
            for table in exemption.tables {
                for shape in exemption.shapes {
                    assert!(
                        unexempted
                            .iter()
                            .any(|(name, seen)| name == table && *seen == Some(*shape)),
                        "allowlist entry `{version}`/`{table}`/{shape:?} exempts nothing; delete \
                         it, or split the entry so its cross product stops over-granting. \
                         {version} names: {unexempted:?}"
                    );
                }
            }
        }

        // And nothing is exempted that no entry for that version names. The
        // check aggregates across entries because a version may hold more than
        // one, each carrying a different shape.
        for migration in bundled_migrations() {
            let entries: Vec<&MigrationRemovalExemption> = MIGRATION_DELETE_ALLOWLIST
                .iter()
                .filter(|exemption| exemption.version == migration.version)
                .collect();
            if entries.is_empty() {
                continue;
            }
            let leaked: Vec<(String, Option<DeleteShape>)> = unexempted_relations(migration.sql)
                .into_iter()
                .filter(|(table, shape)| {
                    !entries.iter().any(|exemption| {
                        exemption.tables.contains(&table.as_str())
                            && shape.is_some_and(|shape| exemption.shapes.contains(&shape))
                    })
                })
                .collect();
            assert!(
                leaked.is_empty(),
                "`{}` destroys rows in {leaked:?}, which its exemptions do not name — add them \
                 with a reason or fix the migration",
                migration.version
            );
        }

        // The historical purges are still the ones being exempted.
        let entry = |version: &str, shape: DeleteShape| {
            MIGRATION_DELETE_ALLOWLIST
                .iter()
                .find(|exemption| exemption.version == version && exemption.shapes.contains(&shape))
                .unwrap_or_else(|| panic!("no `{version}` entry carries {shape:?}"))
        };
        assert!(entry("010", DeleteShape::Truncate)
            .tables
            .contains(&"search_conversation_terms"));
        assert!(entry("020", DeleteShape::AlterDelete)
            .tables
            .contains(&"events"));
        assert!(entry("020", DeleteShape::AlterDelete)
            .tables
            .contains(&"raw_events"));
        // …the two the inversion surfaced overwrite canonical history…
        assert!(entry("012", DeleteShape::AlterUpdate)
            .tables
            .contains(&"events"));
        assert!(entry("013", DeleteShape::AlterUpdate)
            .tables
            .contains(&"events"));
        // …and the one this round surfaced rewrites three of its columns.
        assert!(entry("014", DeleteShape::AlterRewriteColumn)
            .tables
            .contains(&"events"));
    }

    /// **G-ALLOWLIST, key width.** All three dimensions of the exemption key
    /// are load-bearing, each pinned by a pair of statements that differ in
    /// exactly that dimension.
    ///
    /// Every looser key passed the whole suite while being fail-open, which is
    /// how the same defect shipped three rounds running:
    ///
    /// MUTATION (executed 2026-07-27), each run separately against an isolated
    /// copy:
    ///   * drop the `exemption.version == version` conjunct => FAILS on the
    ///     version pair.
    ///   * drop the `exemption.tables.contains(&table)` conjunct => FAILS on
    ///     the table pair. Before this test existed, replacing the per-table
    ///     filter with a version-only equivalent left the suite GREEN at 195/0.
    ///   * drop the `exemption.shapes.contains(&shape)` conjunct => FAILS on
    ///     the shape pair.
    ///   * make a `None` shape exempt (`let Some(shape) = shape else { return
    ///     true }`) => FAILS on the unnamed-shape row.
    #[test]
    fn the_allowlist_key_is_version_table_and_shape() {
        let reported = |version: &str, sql: &str| migration_row_removals(version, sql).len();

        // Version: `012` may rewrite `events` with `ALTER … UPDATE`; `011` may
        // not, and nothing else about the statement changes.
        let update = "ALTER TABLE moraine.events UPDATE payload_json = '' WHERE 1";
        assert_eq!(reported("012", update), 0, "012 exempts this exactly");
        assert_eq!(reported("011", update), 1, "the version is part of the key");

        // Table: `020` may `ALTER … DELETE` from `events`, and from seven
        // others — but not from `ingest_errors`, which it does not name.
        let delete = |table: &str| format!("ALTER TABLE moraine.{table} DELETE WHERE 1");
        assert_eq!(reported("020", &delete("events")), 0);
        assert_eq!(
            reported("020", &delete("ingest_errors")),
            1,
            "the table is part of the key"
        );

        // Shape: 012's licence is `AlterUpdate` on `events`. Its own reason
        // says "none removes a row"; a TRUNCATE of the same table under the
        // same version must not ride on it.
        assert_eq!(
            reported("012", "TRUNCATE TABLE moraine.events"),
            1,
            "the shape is part of the key"
        );
        assert_eq!(
            reported("020", "DROP TABLE moraine.events"),
            1,
            "020's licence is AlterDelete, not DropRelation"
        );

        // Unnamed shape: an exempted migration is not a place where a
        // statement nobody has named becomes acceptable.
        assert_eq!(
            delete_shape("ALTER TABLE moraine.events APPLY DELETED MASK"),
            None
        );
        assert_eq!(
            reported("012", "ALTER TABLE moraine.events APPLY DELETED MASK"),
            1,
            "a `None` shape matches no entry"
        );
    }

    /// **G-BENIGN, non-vacuity.** Every entry of the benign allowlist is
    /// reached by a real statement — a bundled migration's, or a named row of
    /// the negative corpus in
    /// `every_row_removing_shape_is_recognized_including_the_ones_the_old_guard_missed`.
    ///
    /// The inversion's whole claim is that the *benign* list is bounded by what
    /// this repository actually does, unlike the destructive list. That claim
    /// is only true if nobody may add a speculative entry: an unwitnessed
    /// benign form is an unbounded enumeration wearing the other hat, and it is
    /// a hole in the gate rather than a false positive.
    ///
    /// MUTATION (executed 2026-07-28): add `"ATTACH PARTITION"` to
    /// `BENIGN_ALTER_OPERATIONS` => FAILS here, unwitnessed. MUTATION: add
    /// `"OPTIMIZE "` to `BENIGN_STATEMENT_HEADS` => FAILS here **and** in the
    /// `OPTIMIZE … FINAL` positive rows.
    ///
    /// **Tree-witnessed and corpus-witnessed are not the same claim**, and for
    /// a round this test conflated them. It reads `BENIGN_CORPUS`, so an entry
    /// whose only witness is a row the same author added alongside it satisfies
    /// it — the list is then bounded by what somebody wrote in one commit
    /// rather than by "what this repository actually does". That is how
    /// `CREATE OR REPLACE VIEW` shipped: grepping `sql/` finds it in **zero**
    /// bundled migrations. [`CORPUS_WITNESSED_BENIGN_ENTRIES`] is the declared
    /// set, and this test pins it in both directions, so an entry moving from
    /// tree-witnessed to corpus-witnessed — or a new speculative one — is an
    /// edit somebody has to make on purpose.
    #[test]
    fn every_benign_entry_is_witnessed_by_a_real_statement() {
        let tree: Vec<String> = bundled_migrations()
            .iter()
            .flat_map(|migration| crate::split_sql_statements(migration.sql))
            .map(|statement| normalize_statement(&statement))
            .filter(|statement| !statement.is_empty())
            .collect();
        let corpus: Vec<String> = BENIGN_CORPUS
            .iter()
            .map(|sql| normalize_statement(sql))
            .collect();
        let statements: Vec<String> = tree.iter().chain(corpus.iter()).cloned().collect();

        // Every operation of every `ALTER TABLE` statement, in clause order.
        let alter_operations = |source: &[String]| -> BTreeSet<String> {
            source
                .iter()
                .filter(|statement| statement.to_ascii_uppercase().starts_with("ALTER TABLE "))
                .flat_map(|statement| {
                    alter_clauses(&mask_quoted(&statement.to_ascii_uppercase()))
                        .iter()
                        .map(|clause| clause_operation(clause))
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        let reached_in_tree = alter_operations(&tree);
        let reached = alter_operations(&statements);

        let declared: BTreeSet<&str> = CORPUS_WITNESSED_BENIGN_ENTRIES
            .iter()
            .map(|(entry, _)| *entry)
            .collect();
        let mut corpus_only: BTreeSet<&str> = BTreeSet::new();

        for head in BENIGN_STATEMENT_HEADS {
            assert!(
                statements
                    .iter()
                    .any(|statement| benign_shape(statement) == Some(*head)),
                "benign head `{head}` is not reached by any bundled migration or corpus row; a \
                 benign form nobody writes is a hole in the gate, not a convenience"
            );
            if !tree
                .iter()
                .any(|statement| benign_shape(statement) == Some(*head))
            {
                corpus_only.insert(head);
            }
        }

        for operation in BENIGN_ALTER_OPERATIONS {
            assert!(
                reached.contains(*operation),
                "benign ALTER operation `{operation}` is not reached by any bundled migration or \
                 corpus row: {reached:?}"
            );
            if !reached_in_tree.contains(*operation) {
                corpus_only.insert(operation);
            }
        }
        // The four conditional operations, which are not in the constant
        // because listing them would admit their destructive spellings.
        for operation in [
            "MOVE PARTITION",
            "MOVE PART",
            "MODIFY COLUMN",
            "MODIFY QUERY",
        ] {
            assert!(
                reached.contains(operation),
                "conditional ALTER operation `{operation}` is not reached: {reached:?}"
            );
        }

        assert_eq!(
            corpus_only, declared,
            "the set of benign entries whose only witness is a `BENIGN_CORPUS` row has changed. \
             An entry that no bundled migration reaches is bounded by what one author wrote in \
             one commit, not by what this repository does; declare it in \
             CORPUS_WITNESSED_BENIGN_ENTRIES with the reason it is worth carrying, or delete it."
        );
        for (entry, reason) in CORPUS_WITNESSED_BENIGN_ENTRIES {
            assert!(reason.len() > 40, "`{entry}` needs a reason");
        }
    }

    /// Benign entries that **no bundled migration reaches**, with the reason
    /// each is worth carrying anyway.
    ///
    /// Every one of these is a form this repository has not written yet, so
    /// each is a promise about a statement somebody might write rather than a
    /// description of one that exists. That is a weaker footing than the rest
    /// of the list stands on, and it is where the round-6 `CREATE OR REPLACE
    /// VIEW` hole lived — grepping `sql/` finds that head in zero migrations,
    /// and sixteen of the twenty-three `BENIGN_ALTER_OPERATIONS` entries are
    /// in the same position.
    ///
    /// The rule that follows from it: **a corpus-witnessed head that can write
    /// into or replace an existing relation must carry a target-class
    /// condition** — `every_benign_head_answers_the_target_class_question`
    /// enforces it for every head, witnessed or not.
    const CORPUS_WITNESSED_BENIGN_ENTRIES: &[(&str, &str)] = &[
        (
            "SELECT ",
            "no bundled migration is a bare SELECT; the entry exists so a future data-driven \
             migration reads as benign rather than as an unnamed shape",
        ),
        (
            "CREATE OR REPLACE VIEW ",
            "zero bundled migrations use it — the nine that drop and recreate a view spell it as \
             `DROP VIEW` then `CREATE VIEW`. Carried because it is the atomic spelling of that \
             pair, and gated on the class of the name it replaces because ClickHouse does not \
             check that the name was a view (executed: 3 rows to 1, engine MergeTree to View)",
        ),
        (
            "COMMENT COLUMN",
            "the tree documents its columns in the `CREATE TABLE` rather than with this clause. \
             Column metadata, no row",
        ),
        (
            "MODIFY COMMENT",
            "the relation-level spelling of `COMMENT COLUMN`, and unwitnessed for the same \
             reason. Table metadata, no row",
        ),
        (
            "ADD PROJECTION",
            "no bundled migration uses projections. The family is listed whole because a round \
             shipped `DROP`/`CLEAR PROJECTION` without `ADD PROJECTION`, and a gate that permits \
             deleting a derived structure but not creating one gets turned off",
        ),
        (
            "CLEAR PROJECTION",
            "the maintenance half of the PROJECTION family",
        ),
        (
            "DROP PROJECTION",
            "the removal half of the PROJECTION family, and one of the three clauses \
             `the_destructive_drop_clause_list_is_exactly_the_four_that_remove_storage` bounds \
             against",
        ),
        (
            "MATERIALIZE PROJECTION",
            "the backfill half of the PROJECTION family",
        ),
        (
            "ADD STATISTICS",
            "no bundled migration declares column statistics. Same whole-family argument as \
             PROJECTION",
        ),
        (
            "CLEAR STATISTICS",
            "the maintenance half of the STATISTICS family",
        ),
        (
            "DROP STATISTICS",
            "the removal half of the STATISTICS family",
        ),
        (
            "MATERIALIZE STATISTICS",
            "the backfill half of the STATISTICS family",
        ),
        (
            "CLEAR INDEX",
            "the tree adds and materializes a secondary index but never clears one; the INDEX \
             family is listed whole for the same reason as the other two",
        ),
        (
            "DROP INDEX",
            "the removal half of the INDEX family, and one of the three clauses \
             `the_destructive_drop_clause_list_is_exactly_the_four_that_remove_storage` bounds \
             against",
        ),
        (
            "MATERIALIZE TTL",
            "no bundled TTL exists to materialize before sql/039, and 039 anchors its TTLs on a \
             column it materializes instead. Carried because forcing an already-declared TTL to \
             apply removes nothing the declaration had not already licensed — §7.4 records the \
             residual",
        ),
        (
            "MODIFY SETTING",
            "no bundled migration changes a table setting after creation. A setting, not a row; \
             its comma-separated argument list is what `merge_clause_continuations` exists for",
        ),
        (
            "RESET SETTING",
            "reverts a table setting to its default; the inverse of `MODIFY SETTING`, listed so \
             the pair is whole",
        ),
        (
            "REMOVE TTL",
            "deletes a TTL policy rather than the rows it would have removed — the inverse of \
             the shape this gate reports most often, and unwritten for the same reason \
             `MATERIALIZE TTL` is",
        ),
    ];

    /// **G-HEADCLASS.** Every benign head answers, in code, whether a statement
    /// with that head aimed at an existing relation can displace it.
    ///
    /// This is the rule that would have stopped round 6's blocking finding
    /// before it shipped. `CREATE OR REPLACE VIEW ` went onto
    /// [`BENIGN_STATEMENT_HEADS`] with a comment asserting it needed no
    /// target-class check — "a view holds no rows, so unlike `CREATE OR REPLACE
    /// TABLE` … this one can destroy nothing" — while `INSERT INTO ` and
    /// `CREATE MATERIALIZED VIEW ` two lines above it both carried one. The
    /// asymmetry was invisible because nothing required the question to be
    /// answered at all.
    ///
    /// [`HeadTargetRule::ClassGated`] is **executed**: the probe is run through
    /// [`benign_shape`] and must be rejected. A rule that claims a gate the
    /// code does not have fails here.
    ///
    /// MUTATION (executed 2026-07-28): change `CREATE OR REPLACE VIEW `'s rule
    /// to `CannotDisplace` => FAILS here (the head is in
    /// [`PROTECTED_REPLACE_HEADS`], so the arms disagree). Delete the
    /// [`PROTECTED_REPLACE_HEADS`] branch from [`benign_shape`] but keep the
    /// rule => FAILS here on the probe, which is admitted.
    /// An `INSERT` whose target this parser cannot resolve is a finding, because
    /// a target it could not name is a target it could not class-check.
    ///
    /// `INSERT INTO FUNCTION remote(...)` and `INSERT INTO TABLE FUNCTION ...`
    /// land the name-skip on the [`NEVER_A_RELATION`] keyword `FUNCTION`, so
    /// [`named_relations`] returns empty and [`write_target`] returns `None`.
    /// The `None` arm exists for a materialized view with no `TO`, which owns
    /// its storage and displaces nothing — but an `INSERT` always writes
    /// somewhere, and `remote('127.0.0.1', 'moraine', 'events')` reaches the
    /// rows [`PROTECTED_WRITE_HEADS`] exists to protect. Executed on
    /// 25.12.5.44: `INSERT INTO FUNCTION remote(...) SELECT * REPLACE ('' AS
    /// payload_json, event_version + 1 AS event_version) FROM moraine.events
    /// FINAL` took `sum(length(payload_json))` from 87 to 0 through `FINAL`,
    /// and it was still 0 after `OPTIMIZE ... FINAL`.
    ///
    /// MUTATION (executed 2026-07-28): restore the bare `return Some(head)` in
    /// [`benign_shape`]'s [`PROTECTED_WRITE_HEADS`] arm => FAILS here. The MV
    /// half of the same assertion keeps that arm from being narrowed instead.
    #[test]
    fn an_insert_whose_target_this_parser_cannot_resolve_is_a_finding() {
        for statement in [
            "INSERT INTO FUNCTION remote('127.0.0.1:9000', 'moraine', 'events') SELECT 1",
            "INSERT INTO TABLE FUNCTION remote('127.0.0.1:9000', 'moraine', 'events') SELECT 1",
            "INSERT INTO FUNCTION clusterAllReplicas('c', 'moraine', 'events') SELECT 1",
        ] {
            assert!(
                benign_shape(&normalize_statement(statement)).is_none(),
                "an INSERT whose target this parser cannot resolve must not be admitted: \
                 {statement}"
            );
        }

        // The narrowing direction: a materialized view with no `TO` genuinely
        // has no target, owns its own storage, and must stay benign.
        let mv = "CREATE MATERIALIZED VIEW moraine.mv_own ENGINE = MergeTree ORDER BY a \
                  AS SELECT a FROM moraine.events";
        assert!(
            benign_shape(&normalize_statement(mv)).is_some(),
            "a materialized view with no TO displaces nothing and must stay benign"
        );
    }

    #[test]
    fn every_benign_head_answers_the_target_class_question() {
        let ruled: Vec<&str> = BENIGN_HEAD_TARGET_RULES
            .iter()
            .map(|(head, _)| *head)
            .collect();
        assert_eq!(
            ruled,
            BENIGN_STATEMENT_HEADS.to_vec(),
            "every benign head needs exactly one target-class rule, in the same order, so adding \
             a head forces the question to be answered"
        );

        for (head, rule) in BENIGN_HEAD_TARGET_RULES {
            let gated = PROTECTED_WRITE_HEADS
                .iter()
                .any(|(write_head, _)| write_head == head)
                || PROTECTED_REPLACE_HEADS
                    .iter()
                    .any(|(replace_head, _)| replace_head == head);
            match rule {
                HeadTargetRule::ClassGated(probe) => {
                    assert!(
                        gated,
                        "`{head}` claims a target-class gate but is in neither \
                         PROTECTED_WRITE_HEADS nor PROTECTED_REPLACE_HEADS"
                    );
                    let normalized = normalize_statement(probe);
                    assert_eq!(
                        benign_shape(&normalized),
                        None,
                        "`{head}`'s probe `{probe}` aims at bucket-1 `moraine.events` and must be \
                         a finding"
                    );
                    let findings = migration_delete_findings("999", probe);
                    assert_eq!(findings.len(), 1, "`{probe}`");
                    assert_eq!(findings[0].table, "events");
                }
                HeadTargetRule::CannotDisplace(reason) => {
                    assert!(
                        !gated,
                        "`{head}` is target-class gated but its rule says it cannot displace \
                         anything; the two must not disagree"
                    );
                    assert!(reason.len() > 40, "`{head}` needs a reason");
                }
            }
        }
    }

    /// **G-SEARCHCORPUS.** Fails for: a bundled migration emptying
    /// `search_documents` or `search_postings`.
    /// Denomination: per-statement parse over every bundled migration.
    ///
    /// This is the coverage the deleted 034/035 loops used to carry. Those
    /// loops asserted over `["events", "raw_events", "search_documents",
    /// "search_postings"]`; the repo-wide S4 invariant that replaced them
    /// reports only `is_protected()` tables, and both search tables are
    /// `Derived`, so for one round they were covered by no test in any
    /// migration.
    ///
    /// MUTATION (executed 2026-07-27): append
    /// `TRUNCATE TABLE moraine.search_documents;` to
    /// `sql/003_ingest_heartbeats.sql` => FAILS here (and leaves
    /// `no_bundled_migration_removes_protected_rows` green, which is exactly
    /// the gap). Same for `search_postings`, and for
    /// `ALTER TABLE moraine.search_documents DELETE WHERE 1;`.
    ///
    /// Bounded in the other direction by the tree itself: migrations 034 and
    /// 035 truncate six `mcp_open_*` relations on every run and this test is
    /// green, so it is not "no migration may truncate anything derived".
    #[test]
    fn no_bundled_migration_empties_the_search_corpus() {
        let guarded: BTreeSet<&str> = MIGRATION_PRESERVED_DERIVED_TABLES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        for (name, reason) in MIGRATION_PRESERVED_DERIVED_TABLES {
            assert_eq!(
                classify(name),
                Some(TableClass::Derived),
                "`{name}` is guarded here precisely because its class does not guard it"
            );
            assert!(reason.len() > 40, "`{name}` needs a reason");
        }

        let findings: Vec<MigrationDeleteFinding> = bundled_migrations()
            .iter()
            .flat_map(|migration| migration_row_removals(migration.version, migration.sql))
            .filter(|finding| guarded.contains(finding.table.as_str()))
            .collect();
        assert!(
            findings.is_empty(),
            "a bundled migration removes rows from the search corpus. Findings: {findings:#?}"
        );

        // And the scanner really does see derived removals, or the assertion
        // above would pass because `migration_row_removals` reports nothing.
        let derived_removals: Vec<MigrationDeleteFinding> = bundled_migrations()
            .iter()
            .flat_map(|migration| migration_row_removals(migration.version, migration.sql))
            .filter(|finding| finding.class == TableClass::Derived)
            .collect();
        assert!(
            derived_removals
                .iter()
                .any(|finding| finding.table == "mcp_open_events"),
            "034/035 truncate the mcp_open_* family; a scanner that cannot see that cannot see a \
             search-corpus truncate either: {derived_removals:#?}"
        );
    }

    /// **G-SEARCHCORPUS, supersede side.** The corpus is guarded against a
    /// statement that *writes into* it, not only one that removes from it.
    ///
    /// Round 6's finding, and the reason it survived round 5: `benign_shape`
    /// admitted an `INSERT INTO` / `CREATE MATERIALIZED VIEW … TO` whose target
    /// was not `is_protected()`, and both search tables are `Derived`. But
    /// [`MIGRATION_PRESERVED_DERIVED_TABLES`] exists *precisely because*
    /// `Derived` does not guard the corpus, and
    /// `no_bundled_migration_empties_the_search_corpus` filters
    /// [`migration_row_removals`], which never saw these statements at all.
    ///
    /// Both tables are `ReplacingMergeTree` on the reference host — the
    /// identical hazard [`PROTECTED_WRITE_HEADS`] spells out for `events`.
    /// **Executed in `clickhouse local` 25.12.5.44** against a
    /// `ReplacingMergeTree(doc_version) ORDER BY (event_uid, source_host)`
    /// shaped like `sql/004`'s: `INSERT INTO moraine.search_documents SELECT *
    /// REPLACE ('' AS text_content, doc_version + 1 AS doc_version) FROM
    /// moraine.search_documents FINAL` takes `sum(length(text_content))` from
    /// **23 to 0** through `FINAL`, and it is still 0 after `OPTIMIZE …
    /// FINAL` — the rows survive and their content does not.
    ///
    /// MUTATION (executed 2026-07-28): narrow [`relation_is_guarded`] back to
    /// `classify(table).is_none_or(|class| class.is_protected())` => FAILS here
    /// on all four rows.
    #[test]
    fn a_write_into_the_search_corpus_is_a_finding_even_though_it_is_derived() {
        for (sql, table, shape) in [
            (
                "INSERT INTO moraine.search_documents SELECT * REPLACE ('' AS text_content, \
                 doc_version + 1 AS doc_version) FROM moraine.search_documents FINAL",
                "search_documents",
                DeleteShape::InsertInto,
            ),
            (
                "INSERT INTO moraine.search_postings SELECT * REPLACE (0 AS tf, post_version + 1 \
                 AS post_version) FROM moraine.search_postings FINAL",
                "search_postings",
                DeleteShape::InsertInto,
            ),
            (
                "CREATE MATERIALIZED VIEW moraine.mv_blank TO moraine.search_documents AS SELECT \
                 * REPLACE ('' AS text_content) FROM moraine.events",
                "search_documents",
                DeleteShape::MaterializedViewInto,
            ),
            (
                "CREATE MATERIALIZED VIEW moraine.mv_blank TO moraine.search_postings AS SELECT * \
                 FROM moraine.search_documents",
                "search_postings",
                DeleteShape::MaterializedViewInto,
            ),
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_none(),
                "`{sql}` must not be admitted by the benign allowlist"
            );
            // `Derived`, so `migration_delete_findings` filters it out on
            // purpose; the corpus floor reads `migration_row_removals`.
            assert!(migration_delete_findings("999", sql).is_empty());
            let findings = migration_row_removals("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}`");
            assert_eq!(findings[0].table, table);
            assert_eq!(findings[0].shape, Some(shape));
            assert_eq!(findings[0].class, TableClass::Derived);
        }

        // Width: a `Derived` target that is *not* on the preserved list stays
        // benign, so this is not "no migration may write into anything
        // derived". `mcp_open_turns` is the corpus's control.
        assert!(benign_shape(&normalize_statement(
            "INSERT INTO moraine.mcp_open_turns SELECT * FROM moraine.events"
        ))
        .is_some());
    }

    /// **G-REPLACE.** `CREATE OR REPLACE VIEW` replaces whatever stands under
    /// the name, and ClickHouse does not check that it was a view.
    ///
    /// The head shipped on [`BENIGN_STATEMENT_HEADS`] with no target-class
    /// check under the claim that "a view holds no rows, so … this one can
    /// destroy nothing". **Executed, ClickHouse 25.12.5.44:** a `MergeTree`
    /// with three rows, then `CREATE OR REPLACE VIEW probe.t AS SELECT 99 AS
    /// a;` — no error, `count()` 3 → 1, `system.tables.engine` `MergeTree` →
    /// `View`. Repeated on a `ReplacingMergeTree(event_version)` shaped like
    /// `moraine.events`: 2 rows → 1, engine → `View`.
    ///
    /// The contrast is `DROP VIEW`, which really is safe on a table and is safe
    /// because of the *server*: `DROP VIEW probe.t2` on a `MergeTree` throws
    /// `Code: 80 … is not a View` (executed). That is why one of the two heads
    /// needs a condition and the other does not.
    ///
    /// MUTATION (executed 2026-07-28): delete the [`PROTECTED_REPLACE_HEADS`]
    /// branch from [`benign_shape`] => FAILS here on all four protected rows.
    #[test]
    fn replacing_a_relation_with_a_view_is_gated_on_the_targets_class() {
        for (sql, table) in [
            (
                "CREATE OR REPLACE VIEW moraine.events AS SELECT 1",
                "events",
            ),
            (
                "CREATE OR REPLACE VIEW moraine.raw_events AS SELECT 1",
                "raw_events",
            ),
            (
                "CREATE OR REPLACE VIEW `moraine`.`ingest_append_control` AS SELECT 1",
                "ingest_append_control",
            ),
            // Unknown is protected (S1), so an undeclared view name is a
            // finding too — which is what makes the declared-view carve-out
            // below a decision somebody registered rather than a prefix guess.
            (
                "CREATE OR REPLACE VIEW moraine.v_not_declared AS SELECT 1",
                "v_not_declared",
            ),
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_none(),
                "`{sql}` must not be admitted by the benign allowlist"
            );
            let findings = migration_delete_findings("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}`");
            assert_eq!(
                findings[0].table, table,
                "the finding names the relation the statement displaces, not the `VIEW` keyword"
            );
            assert_eq!(findings[0].shape, Some(DeleteShape::ReplaceRelation));
        }

        // `Derived`-but-preserved is a finding as well: W3 and W1 share the
        // gate, so the corpus is protected from the replace head too.
        let findings = migration_row_removals(
            "999",
            "CREATE OR REPLACE VIEW moraine.search_documents AS SELECT 1",
        );
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].table, "search_documents");

        // Width, both directions: a declared view name is the drop-and-recreate
        // nine bundled migrations already perform, and an ordinary `Derived`
        // table is not preserved.
        for sql in [
            "CREATE OR REPLACE VIEW moraine.v_live_events AS SELECT * FROM moraine.events",
            "CREATE OR REPLACE VIEW moraine.mcp_open_turns AS SELECT 1",
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_some(),
                "`{sql}` must stay admitted"
            );
        }
    }

    /// The negative corpus: statements that name `moraine.events` — bucket 1,
    /// the most protected class there is — and must be admitted by
    /// [`benign_shape`] anyway.
    ///
    /// **Every row names `moraine.events` on purpose.** A row naming a
    /// `Derived` table would be vacuous: `migration_delete_findings` filters on
    /// `is_protected()` before anything else, so such a row is empty under
    /// *every* possible benign list, including an empty one.
    ///
    /// It is a constant rather than a literal inside the test because
    /// `every_benign_entry_is_witnessed_by_a_real_statement` reads it too: the
    /// benign list may contain nothing this corpus and the bundled migrations
    /// do not between them reach.
    const BENIGN_CORPUS: &[&str] = &[
        "ALTER TABLE moraine.events ADD COLUMN IF NOT EXISTS x String",
        "ALTER TABLE moraine.events ADD CONSTRAINT c_x CHECK ts > 0",
        "ALTER TABLE moraine.events ADD INDEX IF NOT EXISTS idx_x ts TYPE minmax GRANULARITY 4",
        "ALTER TABLE moraine.events COMMENT COLUMN payload_json 'the payload'",
        "ALTER TABLE moraine.events MATERIALIZE INDEX idx_x",
        "ALTER TABLE moraine.events MATERIALIZE TTL",
        "ALTER TABLE moraine.events RENAME COLUMN IF EXISTS a TO b",
        // The three derived-structure families, in full. A migration that may
        // drop a projection but not create one is a gate nobody can work with;
        // `every_benign_entry_is_witnessed_by_a_real_statement` requires each
        // of these rows before the operation may be listed.
        "ALTER TABLE moraine.events ADD PROJECTION p_x (SELECT * ORDER BY ts)",
        "ALTER TABLE moraine.events MATERIALIZE PROJECTION p_x",
        "ALTER TABLE moraine.events ADD STATISTICS ts TYPE tdigest",
        "ALTER TABLE moraine.events DROP STATISTICS ts",
        "ALTER TABLE moraine.events CLEAR STATISTICS ts",
        "ALTER TABLE moraine.events MATERIALIZE STATISTICS ts",
        // The relation-level spelling of `COMMENT COLUMN`, and the inverse of
        // `MODIFY SETTING`.
        "ALTER TABLE moraine.events MODIFY COMMENT 'the canonical record'",
        "ALTER TABLE moraine.events RESET SETTING index_granularity",
        "SELECT deleteme FROM moraine.events",
        "CREATE DATABASE IF NOT EXISTS moraine",
        "CREATE TABLE IF NOT EXISTS moraine.events (a String) ENGINE = MergeTree ORDER BY a",
        "CREATE VIEW IF NOT EXISTS moraine.v_events AS SELECT * FROM moraine.events",
        // Non-vacuous through the *head*, not the name: without the
        // `CREATE OR REPLACE VIEW ` entry the leading-keyword skip lands on
        // `VIEW`, [`NEVER_A_RELATION`] empties the parse, and the row is a
        // finding on `<unparsed>`.
        "CREATE OR REPLACE VIEW moraine.v_live_events AS SELECT * FROM moraine.events",
        // An MV writing into a `Derived` target. The class is the whole
        // difference: the same statement aimed at `moraine.events` is a
        // finding, two rows down in the positive corpus.
        "CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_events TO moraine.mcp_open_turns AS \
         SELECT * FROM moraine.raw_events",
        // An MV with no `TO` owns its storage and supersedes nothing.
        "CREATE MATERIALIZED VIEW moraine.mv_own ENGINE = MergeTree ORDER BY a AS SELECT a FROM \
         moraine.events",
        // Redefining a declared materialized view's SELECT. Scoped to
        // SCHEMA_VIEW_OBJECTS: the same clause on `moraine.events` is a
        // finding, and non-vacuous here because an undeclared MV name is
        // unknown, which S1 treats as protected.
        "ALTER TABLE moraine.mv_mcp_session_directory_from_events MODIFY QUERY SELECT 1",
        // TTL width: `MODIFY TTL` and nothing broader. Widening the benign
        // `MODIFY ORDER` entry to a bare `MODIFY` admits `MODIFY TTL`, and the
        // TTL rows of the positive corpus go green-to-red.
        "ALTER TABLE moraine.events REMOVE TTL",
        "ALTER TABLE moraine.events MODIFY ORDER BY (event_uid, ts)",
        "ALTER TABLE moraine.events MODIFY SETTING index_granularity = 8192",
        // The comma-tailed operations. Every one is `EXPLAIN AST`-valid on
        // 25.12.5.44 and every one was a **false finding** until round 6:
        // `alter_clauses` split on their argument lists' own commas, they
        // reported shape `None`, and a `None` shape is categorically
        // unexemptable — so a migration needing a two-setting `MODIFY SETTING`,
        // the ordinary spelling, could not be allowlisted at all. See
        // [`merge_clause_continuations`].
        "ALTER TABLE moraine.events MODIFY SETTING index_granularity = 8192, \
         min_bytes_for_wide_part = 0",
        "ALTER TABLE moraine.events RESET SETTING index_granularity, min_bytes_for_wide_part",
        "ALTER TABLE moraine.events ADD COLUMN a2 String SETTINGS alter_sync = 2, \
         mutations_sync = 1",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json COMMENT 'x' SETTINGS \
         max_compress_block_size = 1, min_compress_block_size = 2",
        "ALTER TABLE moraine.mv_mcp_session_directory_from_events MODIFY QUERY SELECT 1 AS a, 2 \
         AS b",
        // MODIFY COLUMN width: the metadata forms, and only those. Each names
        // a different [`MODIFY_COLUMN_METADATA_KEYWORDS`] entry; removing one
        // turns its row red, and removing the positional check turns the
        // type-declaring rows of the positive corpus green.
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json COMMENT 'the payload'",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json CODEC(ZSTD(3))",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json REMOVE TTL",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json DEFAULT ''",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json MATERIALIZED toString(1)",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json FIRST",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json MODIFY SETTING \
         max_compress_block_size = 1",
        "ALTER TABLE moraine.events MODIFY COLUMN payload_json RESET SETTING \
         max_compress_block_size",
        "ALTER TABLE moraine.events MODIFY COLUMN IF EXISTS payload_json COMMENT 'x'",
        // The phantom-operation case. Under the whole-body verb scan this
        // reduced to `ADD COLUMN` **and** a manufactured `COMMENT 'DROP`, and
        // was rejected. One clause, one operation.
        "ALTER TABLE moraine.events ADD COLUMN note String DEFAULT 'x' COMMENT 'drop this'",
        // A comma inside a literal is content, not a clause separator. Without
        // [`mask_quoted`] this splits into `ADD COLUMN x String COMMENT 'a` and
        // `DROP COLUMN payload_json'`, and the second half is a false finding
        // on bucket 1.
        "ALTER TABLE moraine.events ADD COLUMN x String COMMENT 'a, DROP COLUMN payload_json'",
        // CLEAR width: only columns hold row content. Adding `"CLEAR COLUMN"`
        // to BENIGN_ALTER_OPERATIONS turns the positive corpus's two
        // `CLEAR COLUMN` rows red; these two must stay benign regardless.
        "ALTER TABLE moraine.events CLEAR INDEX idx_x IN PARTITION '202601'",
        "ALTER TABLE moraine.events CLEAR PROJECTION p_x",
        // MOVE width: the destination is the extent. Tiering keeps every row
        // in the table it names; `TO TABLE` does not.
        "ALTER TABLE moraine.events MOVE PARTITION '202601' TO DISK 'cold'",
        "ALTER TABLE moraine.events MOVE PART '202601_1_1_0' TO VOLUME 'slow'",
        // DROP width: sql/025:2's constraint drop, plus the two other
        // metadata-only DROP clauses. Removing any of `DROP CONSTRAINT`,
        // `DROP INDEX` or `DROP PROJECTION` from BENIGN_ALTER_OPERATIONS turns
        // one of these red. The constraint row named `moraine.event_links` for
        // a round, which bounded nothing: running the SAME narrowing with that
        // row restored to `event_links` is GREEN across the whole suite
        // (executed 2026-07-27), because `event_links` is `Derived` and the
        // class filter discards it before the statement is inspected.
        "ALTER TABLE moraine.events DROP CONSTRAINT IF EXISTS events_ts_domain",
        "ALTER TABLE moraine.events DROP INDEX idx_x",
        "ALTER TABLE moraine.events DROP PROJECTION p_x",
        // Views hold no rows; the tree drops them on nearly every run.
        "DROP VIEW IF EXISTS moraine.v_live_events",
        "DROP VIEW IF EXISTS moraine.mv_search_postings",
    ];

    /// **G-SHAPE, positive side.** Every statement the benign allowlist must
    /// reject, checked for the finding *and* for the label it is reported
    /// under.
    ///
    /// The shape assertion is not decoration. Since the inversion,
    /// [`delete_shape`] no longer decides anything: deleting one of its arms
    /// leaves the finding intact and only degrades the message, so a corpus
    /// that asserted findings alone would pin nothing about it. Asserting the
    /// shape restores a lower, an upper and a width bound on every arm.
    ///
    /// The `DROP` rows are the S4 hole one reviewer found by bundling each of
    /// them into a migration and watching the suite stay at 177/0. The
    /// `MODIFY TTL`, `CLEAR COLUMN`, `MOVE … TO TABLE`, `REPLACE TABLE`,
    /// `RENAME`/`EXCHANGE` and `TRUNCATE DATABASE` rows are the next reviewer
    /// doing the same thing to the extended list. The `ALTER … UPDATE`,
    /// `REPLACE PARTITION`, `OPTIMIZE … FINAL` and `DETACH PARTITION` rows are
    /// the reviewer after that — the fourth consecutive round to find shapes
    /// the enumeration could not see, which is why the gate is an allowlist
    /// now and this corpus only labels what the allowlist already caught.
    ///
    /// MUTATION (executed 2026-07-27), each run separately against an isolated
    /// copy of the tree, each by deleting the named arm from `delete_shape`:
    ///   * `" MODIFY TTL"` => FAILS on the four TTL rows below.
    ///   * `" CLEAR COLUMN"` => FAILS on the two `CLEAR COLUMN` rows.
    ///   * the `MOVE … TO TABLE` arm => FAILS on the two `MOVE` rows.
    ///   * the `REPLACE TABLE` arm => FAILS on the two `REPLACE TABLE` rows.
    ///   * the `RENAME`/`EXCHANGE` arm => FAILS on the rename rows.
    ///   * narrowing `TRUNCATE ` to `TRUNCATE TABLE` => FAILS on
    ///     `TRUNCATE DATABASE moraine`, which every other `TRUNCATE` row in
    ///     this corpus carries the `TABLE` keyword for and so could not bound.
    ///   * `" DROP PART "` => FAILS on the `DROP PART '202601_1_1_0'` row.
    ///     `" DROP PARTITION"` does not cover it: `DROP PART 'p'` does not
    ///     contain that substring.
    ///   * `" UPDATE "` => FAILS on the two `ALTER … UPDATE` rows.
    ///   * `" DETACH PARTITION"` / `" DETACH PART "` => FAILS on the two
    ///     `DETACH` rows, each of which bounds one of the pair.
    ///   * `" REPLACE PARTITION"` / `" REPLACE PART "` => same, on the two
    ///     `REPLACE PARTITION`/`REPLACE PART` rows.
    ///   * the `OPTIMIZE` arm => FAILS on the three `OPTIMIZE` rows.
    ///
    /// Width, in the narrowing direction:
    ///   * `" DELETE "` -> `" DELETE WHERE "` => FAILS on
    ///     `DELETE IN PARTITION '202601' WHERE 1`, real ClickHouse syntax that
    ///     no other `ALTER … DELETE` row in this corpus spells.
    ///   * dropping `|| upper.starts_with("RENAME DATABASE")` => FAILS on the
    ///     `RENAME DATABASE moraine TO moraine_old` row, which reports one
    ///     relation instead of two once the generic parse takes over.
    ///   * dropping the `OPTIMIZE … DEDUPLICATE` disjunct => FAILS on the
    ///     `DEDUPLICATE BY` row, the only one without `FINAL`.
    ///
    /// The `MODIFY TTL` arm additionally has an end-to-end demonstration,
    /// because this corpus is *its own* guard and could be deleted alongside
    /// it. Appending `sql/039`'s three TTL statements retargeted at
    /// `moraine.events` / `moraine.raw_events` /
    /// `moraine.ingest_checkpoint_transitions` to
    /// `sql/003_ingest_heartbeats.sql` and running
    /// `no_bundled_migration_removes_protected_rows` **alone**:
    ///   * with the arm present => FAILS, three findings (executed 2026-07-27).
    ///   * with the arm deleted, **before the inversion** => PASSES: the S4
    ///     invariant reported zero findings for a migration that puts a
    ///     `DELETE`-defaulting TTL on canonical history. That is the state this
    ///     branch shipped in for a round.
    ///   * with the arm deleted, **after the inversion** => still FAILS, three
    ///     findings, now labelled `None` instead of `AlterModifyTtl`. That
    ///     difference is the inversion.
    #[test]
    fn every_row_removing_shape_is_recognized_including_the_ones_the_old_guard_missed() {
        for (sql, table, shape) in [
            (
                "ALTER TABLE moraine.events DELETE WHERE 1",
                "events",
                DeleteShape::AlterDelete,
            ),
            // Width for `" DELETE "`: narrowing it to `" DELETE WHERE "` — the
            // only form every other row here spells — misses this one.
            (
                "ALTER TABLE moraine.events DELETE IN PARTITION '202601' WHERE 1",
                "events",
                DeleteShape::AlterDelete,
            ),
            (
                "DELETE FROM moraine.events WHERE 1",
                "events",
                DeleteShape::DeleteFrom,
            ),
            (
                "TRUNCATE TABLE IF EXISTS moraine.events",
                "events",
                DeleteShape::Truncate,
            ),
            (
                "TRUNCATE TABLE moraine.events;",
                "events",
                DeleteShape::Truncate,
            ),
            (
                "truncate table `moraine`.`events`",
                "events",
                DeleteShape::Truncate,
            ),
            (
                "DELETE FROM `moraine`.raw_events WHERE 1",
                "raw_events",
                DeleteShape::DeleteFrom,
            ),
            (
                "ALTER TABLE other_db.ingest_checkpoint_transitions DELETE WHERE 1",
                "ingest_checkpoint_transitions",
                DeleteShape::AlterDelete,
            ),
            // `TRUNCATE DATABASE` takes every table with it and names none of
            // them. Only the bare `TRUNCATE ` arm sees it.
            (
                "TRUNCATE DATABASE moraine",
                "moraine",
                DeleteShape::Truncate,
            ),
            // The DROP family.
            (
                "DROP TABLE IF EXISTS moraine.events",
                "events",
                DeleteShape::DropRelation,
            ),
            (
                "DROP TABLE moraine.events",
                "events",
                DeleteShape::DropRelation,
            ),
            (
                "drop table `moraine`.`events`;",
                "events",
                DeleteShape::DropRelation,
            ),
            (
                "DROP TABLE moraine.published_source_generations",
                "published_source_generations",
                DeleteShape::DropRelation,
            ),
            (
                "ALTER TABLE moraine.events DROP PARTITION '202601'",
                "events",
                DeleteShape::AlterDrop,
            ),
            (
                "ALTER TABLE moraine.events DROP PART '202601_1_1_0'",
                "events",
                DeleteShape::AlterDrop,
            ),
            (
                "ALTER TABLE moraine.events DROP COLUMN payload_json",
                "events",
                DeleteShape::AlterDrop,
            ),
            (
                "ALTER TABLE moraine.raw_events DROP DETACHED PARTITION '202601'",
                "raw_events",
                DeleteShape::AlterDrop,
            ),
            (
                "DROP DATABASE IF EXISTS moraine",
                "moraine",
                DeleteShape::DropRelation,
            ),
            // DETACH is step ONE of the two-step delete whose step two
            // (`DROP DETACHED`) the row above covers.
            (
                "ALTER TABLE moraine.events DETACH PARTITION '202601'",
                "events",
                DeleteShape::AlterDetach,
            ),
            (
                "ALTER TABLE moraine.raw_events DETACH PART '202601_1_1_0'",
                "raw_events",
                DeleteShape::AlterDetach,
            ),
            // The TTL family. A TTL expression with no action clause defaults
            // to DELETE, so the second row is a canonical-history deletion
            // written without the word.
            (
                "ALTER TABLE moraine.events MODIFY TTL retention_anchor + INTERVAL 30 DAY DELETE",
                "events",
                DeleteShape::AlterModifyTtl,
            ),
            (
                "ALTER TABLE moraine.events MODIFY TTL ts + INTERVAL 30 DAY",
                "events",
                DeleteShape::AlterModifyTtl,
            ),
            (
                "ALTER TABLE moraine.raw_events MODIFY TTL ingested_at + INTERVAL 7 DAY DELETE",
                "raw_events",
                DeleteShape::AlterModifyTtl,
            ),
            (
                "ALTER TABLE moraine.ingest_checkpoint_transitions MODIFY TTL created_at + \
                 INTERVAL 30 DAY",
                "ingest_checkpoint_transitions",
                DeleteShape::AlterModifyTtl,
            ),
            // CLEAR COLUMN keeps the rows and destroys their content.
            (
                "ALTER TABLE moraine.events CLEAR COLUMN payload_json",
                "events",
                DeleteShape::AlterClear,
            ),
            (
                "ALTER TABLE moraine.events CLEAR COLUMN payload_json IN PARTITION '202601'",
                "events",
                DeleteShape::AlterClear,
            ),
            // UPDATE does the same thing with an arbitrary expression, and is
            // the form four consecutive rounds of shape enumeration missed.
            (
                "ALTER TABLE moraine.events UPDATE payload_json = '' WHERE 1",
                "events",
                DeleteShape::AlterUpdate,
            ),
            (
                "ALTER TABLE moraine.raw_events UPDATE raw_json = '' WHERE ts < now()",
                "raw_events",
                DeleteShape::AlterUpdate,
            ),
            // MOVE … TO TABLE removes the parts from the source.
            (
                "ALTER TABLE moraine.events MOVE PARTITION '202601' TO TABLE moraine.trash",
                "events",
                DeleteShape::AlterMove,
            ),
            (
                "ALTER TABLE moraine.events MOVE PART '202601_1_1_0' TO SHARD \
                 '/clickhouse/tables/x'",
                "events",
                DeleteShape::AlterMove,
            ),
            // The laundering destination, in the *same* clause this time. This
            // is the row that makes [`mask_quoted`] load-bearing: without it
            // the tiering test finds ` TO DISK` inside the partition literal
            // and admits a move off canonical history.
            (
                "ALTER TABLE moraine.events MOVE PARTITION 'x TO DISK y' TO TABLE \
                 moraine.mcp_open_turns",
                "events",
                DeleteShape::AlterMove,
            ),
            // REPLACE PARTITION discards the DESTINATION's partition, and the
            // destination is the table the statement names first.
            (
                "ALTER TABLE moraine.events REPLACE PARTITION '202601' FROM moraine.scratch",
                "events",
                DeleteShape::AlterReplacePartition,
            ),
            // CREATE OR REPLACE swaps a populated table for an empty one.
            (
                "CREATE OR REPLACE TABLE moraine.events (a String) ENGINE = MergeTree ORDER BY a",
                "events",
                DeleteShape::ReplaceRelation,
            ),
            (
                "REPLACE TABLE moraine.events (a String) ENGINE = MergeTree ORDER BY a",
                "events",
                DeleteShape::ReplaceRelation,
            ),
            // `events` is a ReplacingMergeTree: FINAL applies the collapse now
            // rather than at some future merge, and DEDUPLICATE BY collapses on
            // an arbitrary column subset. `reclaim.rs` has treated
            // `OPTIMIZE … FINAL` as a write since the round that added it.
            (
                "OPTIMIZE TABLE moraine.events FINAL",
                "events",
                DeleteShape::OptimizeFinal,
            ),
            (
                "OPTIMIZE TABLE moraine.events FINAL DEDUPLICATE BY event_uid",
                "events",
                DeleteShape::OptimizeFinal,
            ),
            (
                "OPTIMIZE TABLE moraine.events DEDUPLICATE",
                "events",
                DeleteShape::OptimizeFinal,
            ),
            // A1: the accumulator is never-delete, so a migration truncating
            // it outside the 010 exemption is a finding.
            (
                "TRUNCATE TABLE moraine.search_conversation_terms",
                "search_conversation_terms",
                DeleteShape::Truncate,
            ),
            // A column TTL replaces expired values with the column default and
            // removes the column from a part once every value in it has
            // expired: `CLEAR COLUMN` on a rolling horizon, and
            // `MATERIALIZE TTL` — itself benign — forces it immediately.
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json String TTL ingested_at + \
                 INTERVAL 1 DAY",
                "events",
                DeleteShape::AlterModifyTtl,
            ),
            // …the zero horizon, which expires every value at once…
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json String TTL ingested_at + \
                 INTERVAL 0 SECOND",
                "events",
                DeleteShape::AlterModifyTtl,
            ),
            // …and the `ON CLUSTER` spelling, which the clause parser has to
            // skip past before it can see any clause at all. (The type is not
            // optional here: `EXPLAIN AST` rejects `MODIFY COLUMN <c> TTL …`
            // without one, which is why every row of this family carries it.)
            (
                "ALTER TABLE moraine.events ON CLUSTER 'c' MODIFY COLUMN payload_json String TTL \
                 ingested_at + INTERVAL 1 DAY",
                "events",
                DeleteShape::AlterModifyTtl,
            ),
            // Converting a stored column to one that is not stored is
            // `DROP COLUMN` under another name.
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json ALIAS other_col",
                "events",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json EPHEMERAL ''",
                "events",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json PRIMARY KEY",
                "events",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.raw_events MODIFY COLUMN raw_json String TTL ingested_at + \
                 INTERVAL 1 DAY",
                "raw_events",
                DeleteShape::AlterModifyTtl,
            ),
            // MATERIALIZE COLUMN recomputes a column from its DEFAULT across
            // every part. sql/037 documents this exact hazard against
            // `text_digest`; `events` ships two `DEFAULT ''` columns today.
            (
                "ALTER TABLE moraine.events MATERIALIZE COLUMN payload_type",
                "events",
                DeleteShape::AlterRewriteColumn,
            ),
            // A type re-declaration rewrites every part. This one narrows the
            // column that `PARTITION BY toYYYYMM(ingested_at)` is built on.
            (
                "ALTER TABLE moraine.events MODIFY COLUMN ingested_at DateTime",
                "events",
                DeleteShape::AlterRewriteColumn,
            ),
            // The no-op re-declaration is reported too. The gate cannot see the
            // current type, so it cannot tell this from the row above; the cost
            // is one allowlist line for sql/032, which really does convert
            // `source_name` to `LowCardinality(String)` across every part.
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json String",
                "events",
                DeleteShape::AlterRewriteColumn,
            ),
            // `events` is `ReplacingMergeTree(event_version)` with
            // `payload_json` outside the sort key, so an insert that repeats
            // the key with a higher version blanks canonical history:
            // unreachable through FINAL immediately, gone at the next merge.
            (
                "INSERT INTO moraine.events SELECT * REPLACE ('' AS payload_json, event_version + \
                 1 AS event_version) FROM moraine.events FINAL",
                "events",
                DeleteShape::InsertInto,
            ),
            (
                "INSERT INTO moraine.events SELECT * FROM moraine.raw_events",
                "events",
                DeleteShape::InsertInto,
            ),
            // Clause priority, both halves. A benign clause must not mask a
            // destructive one's label, and between two destructive clauses the
            // more destructive wins regardless of the order they are written
            // in — which is what [`ALTER_SHAPE_PRIORITY`] is for.
            (
                "ALTER TABLE moraine.events ADD COLUMN x String, DROP COLUMN payload_json",
                "events",
                DeleteShape::AlterDrop,
            ),
            (
                "ALTER TABLE moraine.events UPDATE payload_json = '' WHERE 1, DELETE WHERE 1",
                "events",
                DeleteShape::AlterDelete,
            ),
            // The same overwrite installed as standing state, and the finding
            // is filed against the `TO` target rather than the phantom
            // `MATERIALIZED` the leading-keyword skip would have produced.
            (
                "CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_events TO moraine.events AS \
                 SELECT * FROM moraine.raw_events",
                "events",
                DeleteShape::MaterializedViewInto,
            ),
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_none(),
                "`{sql}` must not be admitted by the benign allowlist"
            );
            let findings = migration_delete_findings("999", sql);
            assert_eq!(
                findings.len(),
                1,
                "`{sql}` must be recognized as a protected removal"
            );
            assert_eq!(findings[0].table, table);
            assert_eq!(
                findings[0].shape,
                Some(shape),
                "`{sql}` must be reported as {shape:?}"
            );
        }

        // RENAME/EXCHANGE name two relations and the displaced one may be
        // either operand, so both are reported and the finding count is not 1.
        for (sql, expected) in [
            (
                "RENAME TABLE moraine.events TO moraine.attic_events",
                vec!["events", "attic_events"],
            ),
            (
                "RENAME TABLE moraine.mcp_open_turns TO moraine.events",
                vec!["events"],
            ),
            // Width for the `ON CLUSTER` break: without it, `'c'` is parsed as
            // a third relation, unclassified, and reported.
            (
                "RENAME TABLE moraine.events TO moraine.attic_events ON CLUSTER 'c'",
                vec!["events", "attic_events"],
            ),
            // Width for the `RENAME DATABASE` disjunct: dropping it leaves the
            // statement a finding (the head is not benign) but the generic
            // one-relation parse reports `moraine` alone.
            (
                "RENAME DATABASE moraine TO moraine_old",
                vec!["moraine", "moraine_old"],
            ),
            (
                "EXCHANGE TABLES moraine.events AND moraine.mcp_open_turns",
                vec!["events"],
            ),
        ] {
            let reported: Vec<String> = migration_delete_findings("999", sql)
                .into_iter()
                .map(|finding| finding.table)
                .collect();
            assert_eq!(
                reported, expected,
                "`{sql}` must report every protected relation it displaces"
            );
        }

        // Bounded in the other direction: the benign corpus must be admitted,
        // or the gate would forbid an ordinary schema edit and be turned off.
        for sql in BENIGN_CORPUS {
            let normalized = normalize_statement(sql);
            assert!(
                benign_shape(&normalized).is_some(),
                "`{sql}` must be admitted by the benign allowlist"
            );
            assert!(
                migration_delete_findings("999", sql).is_empty(),
                "`{sql}` must not be reported as a protected removal"
            );
        }

        // The declared-view `DROP TABLE`s are the one benign case the head list
        // cannot express, because it is the *name* and not the shape that makes
        // them safe. sql/004:213 and sql/006:4 — objects migration 032 replaced
        // with plain `CREATE VIEW`s, declared in SCHEMA_VIEW_OBJECTS.
        for sql in [
            "DROP TABLE IF EXISTS moraine.search_term_stats",
            "DROP TABLE IF EXISTS moraine.search_corpus_stats",
        ] {
            assert!(benign_shape(&normalize_statement(sql)).is_none());
            assert!(
                migration_delete_findings("999", sql).is_empty(),
                "`{sql}` names a declared view and must not be a finding"
            );
        }

        // A tiering move is not a removal shape *either*, and the label layer
        // has to say so independently of the gate. Without this, widening
        // `AlterMove` to every `MOVE` — dropping the destination test — is
        // invisible: the two rows above are already benign, so nothing asks
        // `delete_shape` about them.
        for sql in [
            "ALTER TABLE moraine.events MOVE PARTITION '202601' TO DISK 'cold'",
            "ALTER TABLE moraine.events MOVE PART '202601_1_1_0' TO VOLUME 'slow'",
        ] {
            assert_eq!(
                delete_shape(&normalize_statement(sql)),
                None,
                "`{sql}` moves bytes between tiers and removes nothing"
            );
        }

        // Width: an undeclared name dropped with DROP TABLE is still a
        // finding, because S1 says unknown is not deletable. Removing the
        // SCHEMA_VIEW_OBJECTS exemption test above without removing this one
        // would be a contradiction, which is the point.
        let findings = migration_delete_findings("999", "DROP TABLE moraine.mv_not_declared");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].class, TableClass::NeverDelete);
    }

    /// **G-BENIGN, fail-closed.** The gate's whole promise: a statement nobody
    /// anticipated is a finding, not a pass.
    ///
    /// None of these has a [`DeleteShape`]. Before the inversion every one of
    /// them was silently benign; each was found by an adversarial sweep in the
    /// round after the one that "completed" the destructive list.
    ///
    /// MUTATION (executed 2026-07-27): add `"ALTER TABLE"` unconditionally to
    /// the benign heads (i.e. make `alter_is_benign` return `true`) => FAILS
    /// here on the first four rows. MUTATION: make `alter_is_benign` return
    /// `true` for an empty operation list => FAILS on the truncated-`ALTER`
    /// row, which is the parse-failure case.
    #[test]
    fn a_statement_nobody_anticipated_is_a_finding_rather_than_a_pass() {
        for sql in [
            // Real ClickHouse, destructive, and named by no DeleteShape arm.
            "ALTER TABLE moraine.events APPLY DELETED MASK",
            "ALTER TABLE moraine.events FREEZE PARTITION '202601'",
            "ALTER TABLE moraine.events ATTACH PARTITION '202601' FROM moraine.scratch",
            // A benign operation bundled with a destructive one. `ALTER`
            // accepts comma-separated operations; only checking the first would
            // pass this.
            "ALTER TABLE moraine.events ADD COLUMN x String, DROP COLUMN payload_json",
            // The laundering case. The `MOVE` clause's tiering test used to
            // search the whole statement, so the substring in the unrelated
            // literal admitted a `MOVE … TO TABLE` off canonical history.
            "ALTER TABLE moraine.events ADD COLUMN c String DEFAULT 'x TO DISK y', MOVE \
             PARTITION '202601' TO TABLE moraine.mcp_open_turns",
            // `MODIFY QUERY` is scoped to declared views; `moraine.events` is
            // not one.
            "ALTER TABLE moraine.events MODIFY QUERY SELECT 1",
            // An `ALTER` whose operation clause did not parse. This is the row
            // that pins `alter_is_benign`'s empty-clause guard: without it,
            // "no operation found" reads as "no destructive operation found".
            "ALTER TABLE moraine.events",
            // A `MODIFY COLUMN` whose tail did not parse — `EXPLAIN AST`
            // rejects this, which is exactly why the gate must not read it as
            // "no property, therefore no dangerous property". It pins
            // `modify_column_is_metadata_only`'s empty-tail arm.
            "ALTER TABLE moraine.events MODIFY COLUMN payload_json",
        ] {
            let normalized = normalize_statement(sql);
            assert!(
                benign_shape(&normalized).is_none(),
                "`{sql}` must not be admitted by the benign allowlist"
            );
            let findings = migration_delete_findings("999", sql);
            assert!(
                findings.iter().any(|finding| finding.table == "events"),
                "`{sql}` must be a finding on `events`: {findings:#?}"
            );
        }

        // Not ClickHouse at all — the case where the parser is what failed. The
        // relation name is junk, so the finding is on an unclassified name;
        // unknown is not deletable, so it is a finding all the same.
        let findings = migration_delete_findings("999", "SOME NEW STATEMENT KIND moraine.events");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].class, TableClass::NeverDelete);

        // A statement whose relation cannot be parsed at all is reported under
        // UNPARSED_RELATION rather than dropped.
        let findings = migration_delete_findings("999", "ALTER TABLE");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].table, UNPARSED_RELATION);
        assert_eq!(findings[0].class, TableClass::NeverDelete);
    }

    /// **G-SHAPE, class-filter side.** The one row whose purpose is the class
    /// filter rather than the shape, asserted in *both* halves so it is not
    /// vacuous.
    ///
    /// `TRUNCATE TABLE moraine.mcp_open_turns` sat in the negative corpus
    /// above for a round, where it bounded nothing: `mcp_open_turns` is
    /// `Derived`, so `migration_delete_findings` discards it on class before
    /// `delete_shape` is consulted, which makes
    /// `migration_delete_findings(..).is_empty()` true for that row under
    /// *every* possible shape list — including an empty one. Asserting that
    /// the shape IS seen and that the class is what suppresses it makes the
    /// same statement carry the claim it was always meant to.
    ///
    /// MUTATION (executed 2026-07-27): delete the `Truncate` arm from
    /// `delete_shape` => FAILS here on the `row_removals` half.
    /// **Non-vacuity.** (The arm has plenty of other coverage — that run also
    /// took down seven further tests, including migrations 034/035 and the 010
    /// allowlist entry — so this row is about the *negative* claim, not about
    /// whether `TRUNCATE` is recognized at all.)
    ///
    /// MUTATION (executed 2026-07-27): make `TableClass::Derived` protected =>
    /// FAILS here on the `delete_findings` half. **Upper bound: 034/035 reset
    /// six `mcp_open_*` relations on every run and must stay legal.**
    #[test]
    fn a_derived_truncate_is_seen_as_a_removal_and_suppressed_only_by_its_class() {
        let sql = "TRUNCATE TABLE moraine.mcp_open_turns";
        let removals = migration_row_removals("999", sql);
        assert_eq!(removals.len(), 1, "the shape must be recognized: {sql}");
        assert_eq!(removals[0].table, "mcp_open_turns");
        assert_eq!(removals[0].class, TableClass::Derived);
        assert!(
            migration_delete_findings("999", sql).is_empty(),
            "and suppressed by `is_protected()`, not by the parser"
        );
    }

    /// **G-SHAPE, exemption width.** The `SCHEMA_VIEW_OBJECTS` exemption is
    /// scoped to `DropRelation` and must stay scoped to it.
    ///
    /// MUTATION (executed 2026-07-27): widen the exemption in
    /// `migration_row_removals` from
    /// `shape == DeleteShape::DropRelation && views.contains(…)` to
    /// `views.contains(…)` => FAILS here. Nothing else in the suite noticed,
    /// because the only declared-view rows in the corpus were `DROP TABLE`s.
    #[test]
    fn truncating_a_declared_view_name_is_still_a_finding() {
        let findings = migration_delete_findings("999", "TRUNCATE TABLE moraine.search_term_stats");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].table, "search_term_stats");
        // Unclassified: S1 says unknown is not deletable.
        assert_eq!(findings[0].class, TableClass::NeverDelete);
        // And the exemption really is live for the shape it is scoped to.
        assert!(
            migration_delete_findings("999", "DROP TABLE moraine.search_term_stats").is_empty()
        );
    }

    /// **G-CLAUSE.** An `ALTER` clause is judged on its own text, in both
    /// directions.
    ///
    /// This is the round-4 root cause. `alter_operations` was not a clause
    /// parser: it matched any bare token from a verb list anywhere in the
    /// body, which produced phantom operations out of column comments *and*
    /// let a `MOVE` clause launder its destination through a substring in an
    /// unrelated literal.
    ///
    /// MUTATION (executed 2026-07-27), each run separately against an isolated
    /// copy:
    ///   * make `clause_is_benign`'s `MOVE` arm search the whole statement
    ///     instead of the clause => FAILS on the laundering row.
    ///   * delete the `mask_quoted` call in `benign_shape` => FAILS on the
    ///     laundering row.
    ///   * replace `alter_clauses` with the old whole-body verb scan => FAILS
    ///     on the phantom rows.
    ///   * drop the `ON CLUSTER` skip in `alter_clauses` => FAILS on the
    ///     `ON CLUSTER` row, whose first clause head becomes `ON`.
    ///   * drop the paren-depth tracking => FAILS on the `ADD PROJECTION` row,
    ///     whose `(SELECT a, b …)` splits into two clauses.
    #[test]
    fn an_alter_clause_is_judged_on_its_own_text() {
        // Admitted: one clause, one operation. Each of these was a finding
        // under the whole-body verb scan, filed against a phantom operation
        // manufactured from an inline column property.
        for sql in [
            "ALTER TABLE moraine.events MODIFY COLUMN payload_json COMMENT 'the payload'",
            "ALTER TABLE moraine.events ADD COLUMN note String DEFAULT 'x' COMMENT 'drop this'",
            "ALTER TABLE moraine.events ADD PROJECTION p_x (SELECT a, b ORDER BY ts)",
            "ALTER TABLE moraine.events ON CLUSTER 'c' ADD COLUMN x String",
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_some(),
                "`{sql}` is one benign clause"
            );
        }

        // Rejected: the destination of a `MOVE` clause is read from that
        // clause. The single-clause control is the width bound — it was
        // already red, so only the *scope* of the test was defeated.
        for sql in [
            "ALTER TABLE moraine.events ADD COLUMN c String DEFAULT 'x TO DISK y', MOVE \
             PARTITION '202601' TO TABLE moraine.mcp_open_turns",
            "ALTER TABLE moraine.events MOVE PARTITION '202601' TO TABLE moraine.mcp_open_turns",
        ] {
            let findings = migration_delete_findings("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}` moves a partition off `events`");
            assert_eq!(findings[0].table, "events");
            assert_eq!(findings[0].shape, Some(DeleteShape::AlterMove));
        }

        // And the tiering forms stay benign, clause-locally.
        assert!(benign_shape(&normalize_statement(
            "ALTER TABLE moraine.events ADD COLUMN c String, MOVE PARTITION '202601' TO DISK \
             'cold'"
        ))
        .is_some());
    }

    /// **G-CLAUSE, fail direction.** A clause split that does not match
    /// ClickHouse's own grammar fails **closed**.
    ///
    /// `UPDATE a = 1, b = 2 WHERE 1` is one ClickHouse operation whose
    /// arguments contain a top-level comma, so [`alter_clauses`] splits it in
    /// two. Neither half is benign, which is the direction a parser
    /// disagreement has to fail in. Same for `MODIFY TTL e1, e2`.
    ///
    /// MUTATION (executed 2026-07-28): make `clause_operation` return the
    /// first token only, so `B =` reduces to `B` => still RED here, but
    /// `alter_is_benign`'s `all()` is what carries it; make `alter_is_benign`
    /// use `any()` instead of `all()` => FAILS here.
    ///
    /// Rows three and four are the **width bound on
    /// [`merge_clause_continuations`]**: a settings continuation is
    /// `<ident> = <value>` or a single bare `<ident>`, and nothing longer, so a
    /// destructive clause parked after a settings list is not absorbed into it.
    /// (ClickHouse rejects both statements outright — executed, 25.12.5.44 —
    /// but the parser must not depend on the server to refuse them.)
    ///
    /// MUTATION (executed 2026-07-28): relax [`is_setting_assignment`] from
    /// `tokens.len() == 3` to `tokens.len() >= 3` => FAILS on row three. Drop
    /// [`is_setting_name`]'s `tokens.next().is_none()` conjunct => FAILS on row
    /// four, where `DROP COLUMN payload_json` would otherwise be absorbed into
    /// a benign `RESET SETTING` and **admitted**. That second one was GREEN
    /// when the continuation merge first landed.
    #[test]
    fn a_mis_split_alter_clause_fails_closed() {
        for sql in [
            "ALTER TABLE moraine.events UPDATE a = 1, b = 2 WHERE 1",
            "ALTER TABLE moraine.events MODIFY TTL ts + INTERVAL 1 DAY, x + INTERVAL 2 DAY",
            "ALTER TABLE moraine.events MODIFY SETTING index_granularity = 8192, x = 1 DROP \
             COLUMN payload_json",
            "ALTER TABLE moraine.events RESET SETTING index_granularity, DROP COLUMN payload_json",
        ] {
            let normalized = normalize_statement(sql);
            assert!(
                benign_shape(&normalized).is_none(),
                "`{sql}` must not be admitted"
            );
        }
        // The first two still carry a usable label.
        assert_eq!(
            delete_shape(&normalize_statement(
                "ALTER TABLE moraine.events UPDATE a = 1, b = 2 WHERE 1"
            )),
            Some(DeleteShape::AlterUpdate)
        );
        assert_eq!(
            delete_shape(&normalize_statement(
                "ALTER TABLE moraine.events MODIFY TTL ts + INTERVAL 1 DAY, x + INTERVAL 2 DAY"
            )),
            Some(DeleteShape::AlterModifyTtl)
        );
    }

    /// **G-DROPWIDTH.** [`DESTRUCTIVE_ALTER_DROP_CLAUSES`] is exactly the four
    /// `DROP` clauses that remove storage, bounded by calling
    /// [`alter_clause_shape`] rather than by the corpus.
    ///
    /// The constant's doc claimed for a round that each of `DROP INDEX`,
    /// `DROP CONSTRAINT` and `DROP PROJECTION` "is a named negative-corpus row
    /// on `moraine.events`, so widening this list by one clause turns a green
    /// test red". That mechanism cannot work — all three are
    /// [`BENIGN_ALTER_OPERATIONS`] entries, so [`benign_shape`] admits the
    /// statement and [`delete_shape`] is never reached, and adding any one of
    /// them was measured **GREEN** while the corpus was the only bound. Since
    /// the inversion this list only labels, which is why this test exists and
    /// calls [`alter_clause_shape`] directly.
    ///
    /// MUTATION (executed 2026-07-28), against this test: add `"DROP INDEX"`,
    /// `"DROP CONSTRAINT"` or `"DROP PROJECTION"` => FAILS on the matching
    /// non-removal row. Remove `"DROP PART "` => FAILS on the `DROP PART` row,
    /// which `"DROP PARTITION"` does not cover.
    #[test]
    fn the_destructive_drop_clause_list_is_exactly_the_four_that_remove_storage() {
        for clause in [
            "DROP PARTITION '202601'",
            "DROP PART '202601_1_1_0'",
            "DROP COLUMN PAYLOAD_JSON",
            "DROP DETACHED PARTITION '202601'",
        ] {
            assert_eq!(
                alter_clause_shape(clause),
                Some(DeleteShape::AlterDrop),
                "`{clause}` removes rows or a column's storage"
            );
        }
        // The metadata-only DROPs remove nothing, and must not be labelled — a
        // gate that calls an ordinary schema edit destructive gets turned off.
        for clause in [
            "DROP INDEX IDX_X",
            "DROP CONSTRAINT IF EXISTS EVENTS_TS_DOMAIN",
            "DROP PROJECTION P_X",
            "DROP STATISTICS TS",
        ] {
            assert_eq!(
                alter_clause_shape(clause),
                None,
                "`{clause}` changes metadata and must not be reported as a removal"
            );
        }
    }

    /// **G-UNREGISTERED.** The one exemption from the schema handshake stays
    /// one, and stays that one.
    ///
    /// [`UNREGISTERED_PHYSICAL_TABLES`] suppresses a finding, and it was the
    /// only constant of its family with no width bound: its doc claimed "the
    /// exhaustiveness test names it explicitly so the exemption cannot silently
    /// grow", and the only naming was a failure-message string. MUTATION
    /// (executed 2026-07-28): adding `"events"` left the whole crate GREEN, and
    /// so did adding `"search_documents"`. Its two siblings were both already
    /// bounded — [`CLICKHOUSE_SYSTEM_LOGS`] by a length assertion in
    /// `classification_covers_the_hosts_thirty_two_physical_tables`,
    /// [`SCHEMA_VIEW_OBJECTS`] by `stale_view_declarations`.
    #[test]
    fn the_unregistered_table_exemption_is_exactly_one_table() {
        assert_eq!(
            UNREGISTERED_PHYSICAL_TABLES,
            ["file_attention_project_roots"],
            "this list excuses a physical table from the schema handshake. Growing it is a \
             deliberate act; growing it by a table that holds user history is the regression S1 \
             exists to prevent"
        );
        // And the exemption is load-bearing rather than stale: the table really
        // is classified, and really is absent from the handshake.
        assert_eq!(
            classify("file_attention_project_roots"),
            Some(TableClass::Derived)
        );
        assert!(!crate::REQUIRED_SCHEMA_OBJECTS.contains(&"file_attention_project_roots"));
    }

    /// **G-MODIFYCOLUMN, non-vacuity and width.**
    /// [`MODIFY_COLUMN_METADATA_KEYWORDS`] had no bound in either direction:
    /// MUTATION (executed 2026-07-28) adding `"ZAPNONSENSE"`, `"STATISTICS"` or
    /// `"AUTO_INCREMENT"` was GREEN across the crate. Its two sibling lists are
    /// bounded by `every_benign_entry_is_witnessed_by_a_real_statement`, which
    /// does not iterate this third one.
    ///
    /// The second half re-measures the doc's own claim. It asserted that
    /// `COLLATE`, `STATISTICS`, `SETTINGS` and `AUTO_INCREMENT` are all
    /// rejected by `EXPLAIN AST` in this position, so listing them would be
    /// dead surface. **Re-measured on 25.12.5.44, 2026-07-28:**
    /// `STATISTICS(tdigest)` and `AUTO_INCREMENT` are **VALID** there without a
    /// preceding type; only `COLLATE`, `SETTINGS` and `AFTER` are rejected. So
    /// for two of the four the surface is live, their absence is an
    /// over-approximation rather than a no-op, and these rows are what record
    /// it as a decision.
    #[test]
    fn a_modify_column_property_the_parser_accepts_is_reported_not_admitted() {
        // Non-vacuity: every listed keyword is reached by a corpus row.
        for keyword in MODIFY_COLUMN_METADATA_KEYWORDS {
            let reached = BENIGN_CORPUS.iter().any(|sql| {
                let masked = mask_quoted(&normalize_statement(sql).to_ascii_uppercase());
                alter_clauses(&masked).iter().any(|clause| {
                    clause_operation(clause) == "MODIFY COLUMN"
                        && column_clause_tail(clause).first().is_some_and(|first| {
                            *first == *keyword || first.starts_with(&format!("{keyword}("))
                        })
                })
            });
            assert!(
                reached,
                "`MODIFY COLUMN … {keyword}` is not witnessed by any BENIGN_CORPUS row; an \
                 unwitnessed metadata keyword is a hole in the gate, not a convenience"
            );
        }

        // Width: the properties the parser accepts in that position and this
        // list deliberately excludes.
        for (sql, shape) in [
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json STATISTICS(tdigest)",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json AUTO_INCREMENT",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json PRIMARY KEY",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json ALIAS other_col",
                DeleteShape::AlterRewriteColumn,
            ),
            (
                "ALTER TABLE moraine.events MODIFY COLUMN payload_json EPHEMERAL ''",
                DeleteShape::AlterRewriteColumn,
            ),
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_none(),
                "`{sql}` must not be admitted"
            );
            let findings = migration_delete_findings("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}`");
            assert_eq!(findings[0].shape, Some(shape), "`{sql}`");
        }
    }

    /// **G-LEXER.** Every comment and quote syntax ClickHouse 25.12.5.44
    /// accepts, and the two the lexer did not know until round 6.
    ///
    /// Both were full bypasses, and both worked by opening a span the server
    /// never opens so that [`mask_quoted`] would swallow a clause separator:
    ///
    /// * **`#` line comments.** `SELECT 1 # trailing` is valid (executed). The
    ///   stripper knew `--` and `/* … */` only, so a `"` parked in a `#`
    ///   comment opened a quoted identifier, and everything after it — comma
    ///   included — was masked. `ALTER TABLE moraine.events ADD COLUMN c
    ///   String # a " b⏎, DROP COLUMN payload_json` reduced to one benign
    ///   `ADD COLUMN`. `EXPLAIN AST` reports both `ADD_COLUMN` and
    ///   `DROP_COLUMN payload_json`; executed in `clickhouse local` it takes
    ///   the column list from `['uid','payload_json']` to `['uid','c']`.
    /// * **`$$…$$` / `$tag$…$tag$` heredocs.** `SELECT $tag$a'b$tag$` returns
    ///   `a'b` (executed). One of these is a *distinct* mechanism from the
    ///   quote-opening one: `COMMENT $$($$, DROP COLUMN payload_json` needs no
    ///   quote at all — the unmasked `(` drove [`alter_clauses`]' paren depth
    ///   to 1 so the top-level comma never split.
    ///
    /// MUTATION (executed 2026-07-28), each against an isolated copy:
    ///   * delete the `#` arm from [`scan_statement`] => FAILS on the `#` rows.
    ///   * delete the heredoc arm => FAILS on the `$tag$` quote-opening row.
    ///     The `$$($$` row stays RED without it, because the unbalanced-paren
    ///     guard catches that one independently — which is the point of having
    ///     both.
    ///   * delete the `unbalanced || depth != 0` guard from [`alter_clauses`]
    ///     => FAILS on the unbalanced row below. With the heredoc arm *also*
    ///     deleted it additionally fails on `$$($$`.
    #[test]
    fn the_lexer_knows_every_clickhouse_comment_and_quote_syntax() {
        // Each of these is `EXPLAIN AST`-valid on 25.12.5.44 and carries a
        // `DROP COLUMN` the gate has to see.
        for sql in [
            // `#` comment holding an unbalanced double quote, mid-statement.
            "ALTER TABLE moraine.events ADD COLUMN c String # a \" b\n, DROP COLUMN payload_json",
            // …a single quote, and a backtick.
            "ALTER TABLE moraine.events ADD COLUMN c String # it's\n, DROP COLUMN payload_json",
            "ALTER TABLE moraine.events ADD COLUMN c String # a ` b\n, DROP COLUMN payload_json",
            // The `#!` spelling, which ClickHouse also accepts.
            "ALTER TABLE moraine.events ADD COLUMN c String #!x '\n, DROP COLUMN payload_json",
            // Heredoc holding a quote…
            "ALTER TABLE moraine.events ADD COLUMN c String COMMENT $tag$a'b$tag$, DROP COLUMN \
             payload_json",
            // …and the paren-depth mechanism, which needs no quote.
            "ALTER TABLE moraine.events ADD COLUMN c String COMMENT $$($$, DROP COLUMN \
             payload_json",
        ] {
            let normalized = normalize_statement(sql);
            assert!(
                benign_shape(&normalized).is_none(),
                "`{sql}` must not be admitted; it drops a column of canonical history"
            );
            let findings = migration_delete_findings("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}`");
            assert_eq!(findings[0].table, "events");
            assert_eq!(findings[0].shape, Some(DeleteShape::AlterDrop), "`{sql}`");
        }

        // An `ALTER` body whose parentheses do not balance did not parse, and a
        // parser that could not finish reading must fail loudly rather than
        // report the prefix it managed.
        let unbalanced =
            "ALTER TABLE moraine.events ADD COLUMN c String COMMENT 'x'), DROP COLUMN payload_json";
        assert!(alter_clauses(&mask_quoted(&unbalanced.to_ascii_uppercase())).is_empty());
        assert!(benign_shape(&normalize_statement(unbalanced)).is_none());
        assert_eq!(migration_delete_findings("999", unbalanced).len(), 1);

        // Bounded in the benign direction: the same syntaxes carrying nothing
        // destructive stay admitted, or the gate would reject ordinary DDL.
        for sql in [
            "ALTER TABLE moraine.events ADD COLUMN c String # a trailing note\n",
            "ALTER TABLE moraine.events ADD COLUMN c String COMMENT $$a, b$$",
            "ALTER TABLE moraine.events ADD COLUMN c String COMMENT $x$ DROP COLUMN payload_json \
             $x$",
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_some(),
                "`{sql}` must stay admitted"
            );
        }

        // And the lexer's own answers, so a rewrite that keeps the tests above
        // green by accident still has to reproduce these.
        assert_eq!(
            normalize_statement("SELECT 1 # a ' \" `\nFROM t"),
            "SELECT 1 FROM t"
        );
        assert_eq!(
            normalize_statement("SELECT '#notacomment'"),
            "SELECT '#notacomment'"
        );
        // The opening `$$` survives so a masked heredoc is still visibly one;
        // the body and the closing delimiter become `_`.
        assert_eq!(mask_quoted("SELECT $$a,b$$"), "SELECT $$_____");
        // An unterminated `$$` is not a heredoc open: masking to the end of the
        // statement on one stray delimiter would blank a clause.
        assert_eq!(mask_quoted("SELECT $$a,b"), "SELECT $$a,b");
        // [`mask_quoted`] must reach the same answer on unnormalized input,
        // because it is the function the `#` bypass actually ran through: the
        // quote inside the comment opened an identifier that ClickHouse never
        // opens, and everything after it — the comma included — was masked.
        assert_eq!(
            alter_clauses(&mask_quoted(
                &"ALTER TABLE moraine.events ADD COLUMN c String # a \" b\n, DROP COLUMN \
                  payload_json"
                    .to_ascii_uppercase()
            ))
            .len(),
            2,
            "a quote inside a `#` comment must not open a literal that swallows the next clause"
        );
    }

    /// **G-NORMALIZE.** A comment is a comment only outside a string literal.
    ///
    /// The previous normalizer cut every line at its first `--`
    /// unconditionally. Combined with clause-head judgement that is a
    /// laundering channel: the tail of the statement — including a whole
    /// destructive clause — disappears, and what is left ends mid-literal and
    /// reads as a benign metadata edit.
    ///
    /// MUTATION (executed 2026-07-27): restore the unconditional
    /// `line.find("--")` cut => FAILS here on the laundering row, which becomes
    /// `ALTER TABLE moraine.events MODIFY COLUMN c COMMENT 'a` — one benign
    /// `MODIFY COLUMN` clause — while the statement ClickHouse executes drops
    /// `payload_json`.
    ///
    /// MUTATION: delete the `/* … */` arm => FAILS on the block-comment row.
    #[test]
    fn comments_are_stripped_only_outside_a_string_literal() {
        let laundered = "ALTER TABLE moraine.events MODIFY COLUMN c COMMENT 'a -- b', DROP \
                         COLUMN payload_json";
        let normalized = normalize_statement(laundered);
        assert!(
            normalized.ends_with("DROP COLUMN payload_json"),
            "the literal must not swallow the rest of the statement: {normalized}"
        );
        let findings = migration_delete_findings("999", laundered);
        assert_eq!(findings.len(), 1, "{findings:#?}");
        assert_eq!(findings[0].table, "events");
        assert_eq!(findings[0].shape, Some(DeleteShape::AlterDrop));

        // A real trailing comment is still stripped…
        assert_eq!(
            normalize_statement("ALTER TABLE moraine.events ADD COLUMN x String -- why not"),
            "ALTER TABLE moraine.events ADD COLUMN x String"
        );
        // …in the block spelling too, mid-statement.
        assert_eq!(
            normalize_statement("ALTER TABLE moraine.events /* note */ ADD COLUMN x String"),
            "ALTER TABLE moraine.events ADD COLUMN x String"
        );
        // …and a block comment inside a literal is content, not a comment.
        assert_eq!(
            normalize_statement("SELECT '/* kept */' FROM moraine.events"),
            "SELECT '/* kept */' FROM moraine.events"
        );
    }

    /// **G-WRITE.** The class of the target is the whole difference between an
    /// ordinary append and an overwrite of history.
    ///
    /// Both heads are benign in form; `benign_shape` consults [`classify`] for
    /// them, and nothing else in the gate does. That makes the pair below the
    /// only place the conditional is visible.
    ///
    /// MUTATION (executed 2026-07-27): drop the class test from `benign_shape`
    /// so both heads are unconditionally benign => FAILS here and on the four
    /// write rows of the positive corpus. MUTATION: invert it (protected
    /// admitted, unprotected reported) => FAILS here on the derived rows and in
    /// `no_bundled_migration_removes_protected_rows` with eight findings.
    #[test]
    fn a_write_into_a_protected_relation_is_a_finding_and_into_a_derived_one_is_not() {
        for (sql, table, shape) in [
            (
                "INSERT INTO moraine.published_source_generations SELECT * FROM moraine.scratch",
                "published_source_generations",
                DeleteShape::InsertInto,
            ),
            (
                "CREATE MATERIALIZED VIEW moraine.mv_x TO moraine.raw_events AS SELECT * FROM \
                 moraine.events",
                "raw_events",
                DeleteShape::MaterializedViewInto,
            ),
        ] {
            let findings = migration_delete_findings("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}`: {findings:#?}");
            assert_eq!(findings[0].table, table);
            assert_eq!(findings[0].shape, Some(shape));
        }

        // Same two statements, `Derived` targets: admitted. 034/035 rebuild
        // the projections by insert on every run, and 027/036 install standing
        // views into them, so this direction is the whole tree.
        for sql in [
            "INSERT INTO moraine.mcp_open_turns SELECT * FROM moraine.scratch",
            "CREATE MATERIALIZED VIEW moraine.mv_x TO moraine.mcp_open_turns AS SELECT * FROM \
             moraine.events",
        ] {
            assert!(
                benign_shape(&normalize_statement(sql)).is_some(),
                "`{sql}` writes into a rebuildable relation"
            );
            assert!(migration_row_removals("999", sql).is_empty(), "`{sql}`");
        }

        // An unclassified target is unknown, and unknown is protected.
        let findings = migration_delete_findings("999", "INSERT INTO moraine.brand_new SELECT 1");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].class, TableClass::NeverDelete);
    }

    /// **G-RELATION.** A finding is never filed against a SQL keyword.
    ///
    /// `CREATE OR REPLACE VIEW moraine.v_live_events AS …` used to be reported
    /// against a relation literally named `VIEW`: `is_relation_noise` eats `OR`
    /// and `REPLACE`, the skip lands on `VIEW`, and an unknown name classifies
    /// as `NeverDelete`. A phantom that reads like a real table is worse than
    /// no name at all, because the reader goes looking for it.
    ///
    /// Fail-closed is preserved — every row below is still a finding, on
    /// [`UNPARSED_RELATION`].
    ///
    /// MUTATION (executed 2026-07-27): delete the [`NEVER_A_RELATION`] check
    /// => FAILS here with `VIEW`, `MATERIALIZED` and `DICTIONARY` as table
    /// names.
    #[test]
    fn a_finding_is_never_filed_against_a_sql_keyword() {
        for sql in [
            // Not a benign head (`CREATE OR REPLACE TABLE` is the destructive
            // sibling), so the parse really runs.
            "CREATE OR REPLACE DICTIONARY moraine.d_x (a String) PRIMARY KEY a",
            "DROP DICTIONARY IF EXISTS moraine.d_x",
            "ATTACH MATERIALIZED VIEW moraine.mv_x",
        ] {
            let findings = migration_delete_findings("999", sql);
            assert_eq!(findings.len(), 1, "`{sql}`: {findings:#?}");
            assert_eq!(
                findings[0].table, UNPARSED_RELATION,
                "`{sql}` must not name a keyword"
            );
            assert_eq!(findings[0].class, TableClass::NeverDelete);
        }

        // And no keyword this guard covers is the name of a relation anybody
        // classified, which is what makes the guard safe to apply.
        for keyword in NEVER_A_RELATION {
            assert_eq!(
                classify(&keyword.to_ascii_lowercase()),
                None,
                "`{keyword}` is both a keyword and a classified table"
            );
        }
    }

    /// Exactly one relation per statement, except the two-operand ones. A
    /// statement whose extraction silently returns nothing reports only
    /// `UNPARSED_RELATION`, which loses the table name from the message.
    ///
    /// The unnamed-shape rows are the ones that matter now: since the
    /// inversion, [`named_relations`] runs for statements [`delete_shape`] has
    /// never heard of, and it has to find the table anyway.
    #[test]
    fn every_shape_but_rename_names_exactly_one_relation() {
        for (sql, expected, expect_shape) in [
            ("TRUNCATE TABLE moraine.events", "events", true),
            ("TRUNCATE DATABASE moraine", "moraine", true),
            ("DELETE FROM moraine.events WHERE 1", "events", true),
            ("ALTER TABLE moraine.events DELETE WHERE 1", "events", true),
            (
                "ALTER TABLE moraine.events MODIFY TTL ts + INTERVAL 1 DAY",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events DROP PARTITION '202601'",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events CLEAR COLUMN payload_json",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events UPDATE payload_json = '' WHERE 1",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events DETACH PARTITION '202601'",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events REPLACE PARTITION '202601' FROM moraine.scratch",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events MOVE PARTITION '202601' TO TABLE moraine.trash",
                "events",
                true,
            ),
            (
                "ALTER TABLE moraine.events MOVE PART '202601_1_1_0' TO SHARD '/ch/x'",
                "events",
                true,
            ),
            ("DROP TABLE moraine.events", "events", true),
            (
                "CREATE OR REPLACE TABLE moraine.events (a String)",
                "events",
                true,
            ),
            ("REPLACE TABLE moraine.events (a String)", "events", true),
            ("OPTIMIZE TABLE moraine.events FINAL", "events", true),
            // No named shape; the generic parse still has to find `events`.
            (
                "ALTER TABLE moraine.events FREEZE PARTITION '202601'",
                "events",
                false,
            ),
            (
                "ALTER TABLE moraine.events APPLY DELETED MASK",
                "events",
                false,
            ),
            // Not ClickHouse at all. The parse takes the second token, so the
            // "relation" is junk — which classifies as `NeverDelete` and is
            // therefore still a finding. Fail-closed costs a wrong name in the
            // message; fail-open would cost the finding.
            ("SOME NEW STATEMENT KIND moraine.events", "NEW", false),
        ] {
            let normalized = normalize_statement(sql);
            let shape = delete_shape(&normalized);
            assert_eq!(shape.is_some(), expect_shape, "`{sql}` shape: {shape:?}");
            assert_eq!(
                named_relations(&normalized, shape),
                vec![expected.to_string()],
                "`{sql}` ({shape:?}) must name exactly `{expected}`"
            );
        }
        assert_eq!(
            named_relations(
                &normalize_statement("EXCHANGE TABLES moraine.events AND moraine.mcp_open_turns"),
                Some(DeleteShape::RenameRelation)
            ),
            vec!["events".to_string(), "mcp_open_turns".to_string()]
        );
    }

    #[test]
    fn an_unclassified_table_in_a_migration_delete_is_a_finding() {
        // S1: unknown is not deletable. A migration that truncates a table
        // nobody classified must fail, not pass by defaulting to derived.
        let findings = migration_delete_findings("999", "TRUNCATE TABLE moraine.brand_new_table");
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].class, TableClass::NeverDelete);
    }

    /// **G-CLASSIFY, non-vacuity.** Each of the four gap vectors names a gap
    /// when one is planted in its input.
    ///
    /// `classification_and_required_schema_objects_are_mutually_exhaustive`
    /// asserts all four vectors are empty over the shipped constants. A field
    /// narrowed to `Vec::new()` satisfies that just as well as a working one —
    /// and the narrowing silently disarms the *positive* mutation each side
    /// exists to catch, so the two tests together are the pair.
    ///
    /// MUTATION (executed 2026-07-27), each run separately: replace the body of
    /// `unclassified_schema_objects`, then `classified_but_unregistered`, then
    /// `stale_view_declarations`, then `unclassified_migration_tables` with
    /// `Vec::new()` => in **all four** cases this is the ONLY test in the
    /// crate that fails. That is the measurement: it is the sole guard, so
    /// before it existed each narrowing was green, and each narrowing disarms
    /// the mutation the exhaustiveness test's own docstring relies on.
    ///
    /// `the_migration_side_of_exhaustiveness_actually_parses_create_table`
    /// bounds a different thing — that `migration_created_tables` really reads
    /// `sql/` — and does not survive this narrowing on its own.
    #[test]
    fn each_classification_gap_vector_names_a_planted_gap() {
        let required: BTreeSet<&str> = ["events", "search_term_stats"].into_iter().collect();
        let views: BTreeSet<&str> = ["search_term_stats"].into_iter().collect();
        let unregistered: BTreeSet<&str> = BTreeSet::new();
        let classified: BTreeSet<&str> = ["events"].into_iter().collect();

        // Baseline: these inputs are mutually exhaustive, so nothing is a gap.
        let clean = classification_gaps_between(&required, &views, &unregistered, &classified, &[]);
        assert!(clean.is_empty(), "{clean:?}");

        // 1. The schema requires an object nobody classified and nobody
        //    declared a view.
        let planted: BTreeSet<&str> = ["events", "search_term_stats", "reclaim_scratch"]
            .into_iter()
            .collect();
        let gaps = classification_gaps_between(&planted, &views, &unregistered, &classified, &[]);
        assert_eq!(gaps.unclassified_schema_objects, vec!["reclaim_scratch"]);
        assert!(gaps.classified_but_unregistered.is_empty());
        assert!(!gaps.is_empty(), "the summary must see field 1");

        // 2. A classified table the handshake does not require and the
        //    unregistered allowlist does not excuse.
        let planted: BTreeSet<&str> = ["events", "reclaim_scratch"].into_iter().collect();
        let gaps = classification_gaps_between(&required, &views, &unregistered, &planted, &[]);
        assert_eq!(gaps.classified_but_unregistered, vec!["reclaim_scratch"]);
        assert!(!gaps.is_empty(), "the summary must see field 2");
        // …and the allowlist is what excuses it, or the field would be noise.
        let excused: BTreeSet<&str> = ["reclaim_scratch"].into_iter().collect();
        let gaps = classification_gaps_between(&required, &views, &excused, &planted, &[]);
        assert!(gaps.classified_but_unregistered.is_empty(), "{gaps:?}");

        // 3. A view declaration the handshake no longer requires.
        let planted: BTreeSet<&str> = ["search_term_stats", "v_retired"].into_iter().collect();
        let gaps =
            classification_gaps_between(&required, &planted, &unregistered, &classified, &[]);
        assert_eq!(gaps.stale_view_declarations, vec!["v_retired"]);
        assert!(!gaps.is_empty(), "the summary must see field 3");

        // 4. The migration side, for symmetry with the other three.
        let gaps = classification_gaps_between(
            &required,
            &views,
            &unregistered,
            &classified,
            &["events".to_string(), "reclaim_scratch".to_string()],
        );
        assert_eq!(gaps.unclassified_migration_tables, vec!["reclaim_scratch"]);
        assert!(!gaps.is_empty(), "the summary must see field 4");
    }
}
