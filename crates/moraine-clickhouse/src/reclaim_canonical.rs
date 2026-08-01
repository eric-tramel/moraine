//! Issue #603 WI-09 — the `canonical_generation` candidate probe and the
//! delete predicates it authorizes
//! ([`crate::reclaim::ReclaimScope::CanonicalGeneration`]).
//!
//! Targets superseded source generations in the bucket-1/2 tables — `events`,
//! `raw_events`, `ingest_errors` — and their derived satellites
//! `search_documents`, `search_postings`, `tool_io` and `event_links`. This is
//! the one scope that deletes **user history**, so it is reachable only
//! through `moraine db reclaim run --scope canonical_generation --confirm`
//! with **both** `retention.canonical_history_horizon_days` and
//! `retention.raw_audit_horizon_days` set: `ReclaimAuthority::for_scope`
//! refuses to mint its tokens from a config missing either key, and the
//! janitor's scope roster (`STORAGE_RECLAIM_MAINTENANCE_SCOPES`) excludes the
//! scope entirely — plan §3.7: WS-3c runs only from the explicit CLI.
//!
//! ## Retirement, and which horizon gates a unit
//!
//! A generation is a candidate under exactly the WI-07 notion — R1 it was
//! published; R2 a **different** generation is its file's current published
//! head (head identity, never generation order — a rollback that re-publishes
//! an older generation is honoured in both directions); R3 the superseding
//! head predates the horizon; R4 at least one directly generation-keyed table
//! holds rows for it — with one difference: **the horizon is the stricter
//! (larger) of the two protected horizons**, `max(canonical_history,
//! raw_audit)`, computed by `reclaim::probe_horizon_seconds`. Per bucket:
//!
//! * **bucket 1 (`events`)** — gated by
//!   `retention.canonical_history_horizon_days`;
//! * **bucket 2 (`raw_events`, `ingest_errors`)** — gated by
//!   `retention.raw_audit_horizon_days`;
//! * **the unit** — one generation is one unit and one decision, so it becomes
//!   claimable only when **both** horizons have passed. Splitting a unit by
//!   bucket would leave a half-reclaimed generation re-emitted on every probe
//!   between the two horizons, and a ledger row whose deletes are not the
//!   unit's extent.
//! * **bucket-3 satellites** (`search_documents`, `search_postings`,
//!   `tool_io`, `event_links`) — governed by the unit's protected horizon,
//!   never by the shorter `derived_horizon_hours`: a satellite that outlived
//!   its canonical parent would be exactly the orphan shape this scope's
//!   delete ordering exists to prevent, and both protected keys carry a 7-day
//!   validation floor that dominates the 24-hour derived default.
//!
//! ## The reader-visibility argument, and what `FINAL` does and does not hide
//!
//! `v_live_events` (sql/032) is the sole authorization relation: `events
//! FINAL` joined to the current published heads on the full generation tuple.
//! A superseded generation's rows are **full physical rows, not collapsed
//! ones** — `event_uid` embeds `(source_file, source_generation, line,
//! offset, fingerprint)` (`sources/shared.rs`), so a replayed generation's
//! uids are disjoint from its predecessor's and the `ReplacingMergeTree`
//! collapse never merges the two. What `FINAL` does collapse is same-uid
//! re-ingests *within* a generation, across monthly partitions, because
//! `ingested_at` is deliberately excluded from the sort key (sql/019). Both
//! properties are proved in the `clickhouse local` fixture: the pinned-reader
//! census (the `v_live_events` join, verbatim) is identical before, mid-unit
//! and after the deletes, while the physical census shows the superseded
//! generation's full rows leaving.
//!
//! ## Per-table predicate analysis
//!
//! Delete order is [`crate::reclaim::ReclaimScope::tables`]'s and is the
//! safety property: satellites and derived rows first, `events` last, so a
//! crash leaves derived data missing over intact canonical data (rebuildable),
//! never orphaned satellites of deleted canonical rows.
//!
//! * `events`, `raw_events`, `ingest_errors`, `search_documents` — all four
//!   carry the full generation tuple as plain columns (sql/001 + sql/032),
//!   so the tuple predicate's extent is exact. `search_documents` is
//!   `ORDER BY (event_uid, source_host)` and one physical row serves **both**
//!   attributions of a double-attributed uid (hazard H2, #608) — which is why
//!   its delete is generation-keyed and no predicate in this module names
//!   `session_id`: both attributions share the row's one `(file, generation)`
//!   and retire together with it.
//! * `search_postings` — hazard H1: 83.6% of the reference host's posting
//!   rows carry `source_file = ''` / `source_generation = 0`, back-filled
//!   type defaults from the sql/032 ALTER, so the posting's own generation
//!   columns must never be predicated on. The predicate joins through the
//!   **document** (`doc_id` → `search_documents.event_uid`), mirroring
//!   `v_live_search_postings`, and additionally binds the posting's own
//!   `source_host` **and `source_name`** — because the uid excludes *both*
//!   discriminators. The host: two shared-backend hosts ingesting the same
//!   file produce identical uids (that collision is the whole reason sql/032
//!   added the column), and without the host bind a `doc_id`-only delete for
//!   host A would take host B's postings with it. The name: the uid material
//!   is `file|generation|line|offset|fingerprint|suffix`
//!   (`sources/shared.rs`) — it excludes `source_name` — so one physical
//!   line reachable through two overlapping `[sources]` definitions on the
//!   same host carries **one uid under two source names**, and the retired
//!   name's `(file, generation)` can be superseded while the live name's
//!   identical pair remains the head. A name-blind delete for the retired
//!   name takes the live name's postings with it — plan §7.2's G-DUALUID
//!   shape, executed as the dual-uid `clickhouse local` fixture. A pre-032
//!   posting's `source_host` is a back-filled `''`, which fails the equality
//!   against a real host: the miss is the safe direction (the H1 rows are
//!   *unreachable*, never *swept*), and is recorded as residue below.
//! * `tool_io`, `event_links` — hazard H3: no generation column exists on
//!   either ([`crate::reclaim::ReclaimPredicate::UidSet`] is the type-level
//!   statement of that). Their liveness is `(source_host, event_uid,
//!   source_event_version = events.event_version)` (sql/032), so the
//!   predicate is a uid set **re-derived from the parent `events` rows** by
//!   subquery at execution time, bound to `(source_host, source_name,
//!   event_uid)` — host and name for the same two uid-exclusion reasons as
//!   postings (both tables carry `source_name` from sql/001).
//!
//! ## Why the uid set is re-derived rather than captured into the ledger
//!
//! The `ReclaimPredicate::UidSet` doc once specified a ledger capture, and
//! the plan (§7.4 residual) records that `storage_reclaim_ledger` (sql/038)
//! has no column able to hold a uid set. This module resolves that residual
//! the other way: the predicate is **re-derivable for as long as it is
//! needed**, and the delete ordering is what makes that provable.
//!
//! Within one unit, every predicate that reads another table by subquery
//! reads only tables deleted **strictly later** in the unit's own order
//! (`search_postings` reads `search_documents`, deleted next; `tool_io` and
//! `event_links` read `events`, deleted last). Deletes are issued
//! sequentially and each blocks until its mutation completes
//! ([`crate::reclaim::RECLAIM_DELETE_SETTINGS`] pins
//! `lightweight_deletes_sync = 2`), so at any crash point the applied
//! statements are a strict prefix of the unit's order. On re-drive the driver
//! replays the whole unit: every prefix statement re-executes over
//! already-deleted rows and removes zero more (an emptied subquery source can
//! only *shrink* an already-satisfied extent), and every suffix statement
//! still finds its subquery sources intact — they sit later in the order than
//! the crash point. A ledger capture would additionally have to chunk
//! arbitrarily large uid sets across statements
//! (`UNIT_STMT_MAX_TABLES` assumes one statement per table) and hold
//! multi-hundred-kilobyte rows the ledger schema was never sized for;
//! re-derivation needs neither, at the cost of the ordering invariant —
//! which `every_canonical_subquery_source_is_deleted_later_in_the_unit`
//! pins structurally.
//!
//! A canonical unit interrupted mid-flight is re-driven **only by the next
//! operator run**: the janitor's roster excludes this scope, so unattended
//! machinery never finishes a canonical delete either. The half-done state it
//! waits in is the safe one — satellites missing, canonical rows intact — and
//! `reclaim status` reports the unsettled unit.
//!
//! ## OQ-1 — `search_conversation_terms` is dead, and is decommissioned
//!
//! Reclaiming postings while a `SummingMergeTree` accumulator kept counting
//! them was hazard H5: deletes never decrement it, so the corpus statistics
//! it claimed to hold would be permanently overstated. OQ-1's answer,
//! shipped here: the table is **dead** — zero readers across the repository,
//! the monitor UI, and the Python bindings (checked 2026-07-31; only schema
//! registration and migration text name it) — and already double-counts every
//! re-ingest, so it holds no statistic worth preserving. Migration 040 drops
//! its feeding materialized view (`mv_search_conversation_terms`), ending the
//! write amplification sql/037 already called dead and freezing the table as
//! a historical artifact; the frozen table itself stays classified
//! `NeverDelete` — unreachable by every authority token — until WI-10's
//! batched schema cleanup drops it alongside `search_interaction_log` (OQ-2).
//! `oq1_search_conversation_terms_is_decommissioned_not_reclaimed` asserts
//! all three halves.
//!
//! ## Recorded residue
//!
//! * **Pre-032 rows are unreachable, not reclaimed.** Rows whose
//!   `source_host` is the back-filled `''` (events/documents/postings written
//!   before migration 032) never match a unit's tuple — publication truth
//!   carries real host names — and pre-032 postings additionally fail the
//!   host bind. The `source_name` bind has the same safe direction: a
//!   satellite row whose `source_name` is `''` (or otherwise diverged from
//!   its parent's) is *stranded*, never *swept*. Bounded: every post-032 row
//!   carries a real host, and while `search_postings` gained its
//!   `source_name` column by back-fill (`ALTER TABLE ... ADD COLUMN ...
//!   DEFAULT ''`, sql/004) — so installs predating it hold `''`-named rows —
//!   those rows are exactly the stranded-not-swept case above, measured at 0
//!   on the reference host, and the residue can only shrink.
//! * **Cross-generation uid collisions are excluded by the emitter, not by
//!   the predicate.** The uid-set and document-join deletes distinguish
//!   host and name but not generation — `tool_io`/`event_links` have no
//!   generation column to bind (H3), and the postings join reaches only
//!   `search_documents.event_uid`. A uid present in **two generations** of
//!   one `(host, name, file)` — one retired, one live — would therefore lose
//!   its live satellite rows to the retired unit's deletes. No predicate
//!   this module can write closes that; what closes it is the emitter:
//!   `event_uid` embeds `source_generation` in its hash material
//!   (`sources/shared.rs`), so a replayed generation's uids are disjoint
//!   from its predecessor's **by construction**, and uid disjointness across
//!   generations is an *emitter* property, not a predicate property.
//!   Executed 2026-07-31 as the X2 `clickhouse local` shape (hand-forged
//!   same-uid rows across two generations) to record what a violated
//!   emitter invariant would cost; defence in depth residue, not a guarded
//!   claim.
//! * **Never-published generations are not collected** — same R1 argument,
//!   same bound, and the same deliberate reason as WI-07: a generation with
//!   no history row is indistinguishable from a publication in flight.
//! * **A rollback past the horizon is unrecoverable here.** Unlike WI-07's
//!   read indexes (rebuildable from `events`), a canonical generation deleted
//!   by this scope is gone from Moraine; re-publishing it requires the source
//!   file to still exist on disk. That is why both keys are explicit, both
//!   floors are 7 days, and the refusal names
//!   `moraine export events --format jsonl` as the pre-destructive valve.
//!
//! ## Cost shape (hazard H4)
//!
//! The probe reads no payload column and uses no `FINAL`. The retirement set
//! is computed on the publication views; the per-table rollups are `GROUP BY`
//! over the generation tuple **filtered to the retired set first**, over the
//! four generation-keyed tables only — `search_postings`, `tool_io` and
//! `event_links` are deliberately absent from the rollup (their probe-time
//! join costs a second scan of the largest tables for an estimate R4 does not
//! need; a generation with zero rows in all four rolled-up tables has nothing
//! reachable in the three satellites either, because their predicates derive
//! from those tables' rows). Unit row estimates therefore undercount the
//! satellite mass; every surface already labels them estimates. The deletes
//! are unprunable full-part scans on every table whose sort key does not lead
//! with the tuple — which is all of them — bounded by the shared
//! [`crate::reclaim::RECLAIM_MAX_UNITS_PER_RUN`] page and the one-mutation-
//! at-a-time transport.

use crate::escape_identifier;
use crate::reclaim::{ReclaimTable, ReclaimUnit};

/// Candidate superseded canonical generations (plan §3.5, WI-09): one unit
/// per retired `(source_host, source_name, source_file, source_generation)`.
///
/// `horizon_seconds` is the **stricter of the two protected horizons** — see
/// the module docs and `reclaim::probe_horizon_seconds`, which is the one
/// place that rule is computed.
pub(crate) fn canonical_candidate_sql(
    database: &str,
    horizon_seconds: u64,
    limit: usize,
) -> String {
    let database = escape_identifier(database);
    format!(
        "WITH\n  \
         heads AS (\n    \
           SELECT source_host, source_name, source_file, source_generation, published_at\n    \
           FROM {database}.v_current_published_source_generations\n  ),\n  \
         retired AS (\n    \
           SELECT h.source_host AS source_host, h.source_name AS source_name,\n           \
             h.source_file AS source_file, h.source_generation AS source_generation\n    \
           FROM {database}.v_published_source_generation_history AS h\n    \
           INNER JOIN heads AS head\n      \
             ON head.source_host = h.source_host\n     \
             AND head.source_name = h.source_name\n     \
             AND head.source_file = h.source_file\n    \
           WHERE h.source_generation != head.source_generation\n      \
             AND head.published_at < now64(3) - toIntervalSecond({horizon_seconds})\n  ),\n  \
         cg_rollup AS (\n    {}\n  )\n\
         SELECT r.source_host AS source_host, r.source_name AS source_name,\n       \
           r.source_file AS source_file, toUInt32(r.source_generation) AS source_generation,\n       \
           toUInt64(sum(cg_rollup.event_rows)) AS event_rows,\n       \
           toUInt64(sum(cg_rollup.raw_rows)) AS raw_rows,\n       \
           toUInt64(sum(cg_rollup.error_rows)) AS error_rows,\n       \
           toUInt64(sum(cg_rollup.document_rows)) AS document_rows\n\
         FROM retired AS r\n\
         INNER JOIN cg_rollup ON cg_rollup.source_host = r.source_host\n  \
           AND cg_rollup.source_name = r.source_name\n  \
           AND cg_rollup.source_file = r.source_file\n  \
           AND cg_rollup.source_generation = r.source_generation\n\
         GROUP BY r.source_host, r.source_name, r.source_file, r.source_generation\n\
         ORDER BY source_host ASC, source_name ASC, source_file ASC, source_generation ASC\n\
         LIMIT {limit}\n\
         FORMAT JSONEachRow",
        canonical_rollup_sql(&database),
    )
}

/// The four generation-keyed tables, aggregated to the generation tuple,
/// filtered to the retired set **before** grouping, as one relation.
///
/// The `INNER JOIN` between `retired` and this rollup is R4, exactly as in
/// `reclaim_read_index`: a retired generation no rolled-up table holds rows
/// for is not a unit, or it would claim an empty ledger row on every run
/// forever. No `FINAL` (hazard H4) and no payload column: a physical row
/// count is the right estimate denomination, and superseded rows are full
/// physical rows either way (module docs).
fn canonical_rollup_sql(database: &str) -> String {
    let branch = |table: &str, events: &str, raw: &str, errors: &str, documents: &str| {
        format!(
            "SELECT source_host, source_name, source_file, source_generation,\n         \
               toUInt64({events}) AS event_rows, toUInt64({raw}) AS raw_rows,\n         \
               toUInt64({errors}) AS error_rows, toUInt64({documents}) AS document_rows\n    \
             FROM {database}.{table}\n    \
             WHERE (source_host, source_name, source_file, source_generation) IN (\n      \
               SELECT source_host, source_name, source_file, source_generation FROM retired\n    )\n    \
             GROUP BY source_host, source_name, source_file, source_generation"
        )
    };
    [
        branch("events", "count()", "0", "0", "0"),
        branch("raw_events", "0", "count()", "0", "0"),
        branch("ingest_errors", "0", "0", "count()", "0"),
        branch("search_documents", "0", "0", "0", "count()"),
    ]
    .join("\n    UNION ALL\n    ")
}

/// The delete predicates for one canonical unit, in
/// [`crate::reclaim::ReclaimScope::tables`] order — satellites first,
/// `events` last.
///
/// Three shapes, each argued in the module docs: the generation tuple for the
/// four tables that carry it; the document join (host- and name-bound) for
/// `search_postings` (hazard H1 + the dual-uid shape); the re-derived
/// `(source_host, source_name, event_uid)` set for `tool_io`/`event_links`
/// (hazard H3). No predicate names `session_id` anywhere (hazard H2), and
/// every subquery reads only tables deleted later in this same list — the
/// restart-safety invariant
/// `every_canonical_subquery_source_is_deleted_later_in_the_unit` pins.
pub(crate) fn canonical_unit_predicates(
    database: &str,
    unit: &ReclaimUnit,
) -> Vec<(ReclaimTable, String)> {
    let database = escape_identifier(database);
    let host = crate::escape_literal(&unit.source_host);
    let name = crate::escape_literal(&unit.source_name);
    let file = crate::escape_literal(&unit.source_file);
    let generation = unit.source_generation;
    let tuple = format!(
        "(source_host, source_name, source_file, source_generation) IN \
         (({host}, {name}, {file}, toUInt32({generation})))"
    );
    let uid_set = format!(
        "(source_host, source_name, event_uid) IN (SELECT source_host, source_name, event_uid \
         FROM {database}.events WHERE {tuple})"
    );
    unit.scope
        .tables()
        .iter()
        .map(|table| {
            let predicate = match table {
                ReclaimTable::SearchPostings => format!(
                    "source_host = {host} AND source_name = {name} AND doc_id IN \
                     (SELECT event_uid FROM {database}.search_documents WHERE {tuple})"
                ),
                ReclaimTable::ToolIo | ReclaimTable::EventLinks => uid_set.clone(),
                _ => tuple.clone(),
            };
            (*table, predicate)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reclaim::{
        emit_delete_statement, executor_for, probe_horizon_seconds, ReclaimAuthority, ReclaimPhase,
        ReclaimScope, ReclaimUnitGrain,
    };
    use crate::storage_class::{classify, TableClass};
    use moraine_config::RetentionConfig;

    fn unit() -> ReclaimUnit {
        ReclaimUnit {
            reclaim_id: "r-1".to_string(),
            scope: ReclaimScope::CanonicalGeneration,
            source_host: "h'1".to_string(),
            source_name: "codex".to_string(),
            source_file: "/a'b.jsonl".to_string(),
            source_generation: 7,
            session_id: String::new(),
            candidate_generation: 0,
            phase: ReclaimPhase::Claimed,
            estimated_rows: 10,
            estimated_bytes: 20,
            unsettled_seconds: 0,
        }
    }

    fn full_authority() -> Vec<ReclaimAuthority> {
        ReclaimAuthority::for_scope(
            ReclaimScope::CanonicalGeneration,
            &RetentionConfig {
                canonical_history_horizon_days: Some(365.0),
                raw_audit_horizon_days: Some(90.0),
                ..RetentionConfig::default()
            },
        )
        .expect("configured retention grants authority")
    }

    /// The horizon and page the fixture's generated files are built at. The
    /// horizon is the 7-day protected floor — the smallest value either
    /// `[retention]` key may legally hold.
    const FIXTURE_HORIZON_SECONDS: u64 = 604_800;
    const FIXTURE_PROBE_LIMIT: usize = 512;

    /// The `(source_host, source_name, source_file, source_generation)` units
    /// the base canonical `clickhouse local` fixture drives its deletes from:
    /// every unit the shipped probe returns against
    /// `testdata/reclaim_probe/fixture_canonical.sql`.
    ///
    /// Hand-maintained, and therefore checked against the probe at fixture
    /// run time by the `throwIf` block [`fixture_delete_script`] opens with —
    /// the `reclaim_read_index::FIXTURE_UNITS` defence, for the same reason:
    /// a delete script generated from a stale list reports "intact" for
    /// exactly the rows a real run would destroy.
    const FIXTURE_UNITS: &[(&str, &str, &str, u32)] = &[("h", "codex", "/live.jsonl", 5)];

    /// The dual-uid fixture's unit set
    /// (`testdata/reclaim_probe/fixture_canonical_dualuid.sql`): one uid
    /// under **two source names** on one host, the `codex` attribution
    /// retired, the `codexalt` attribution live — plan §7.2's G-DUALUID
    /// shape as a `clickhouse local` stand-in. Same hand-maintained/`throwIf`
    /// contract as [`FIXTURE_UNITS`].
    const DUALUID_FIXTURE_UNITS: &[(&str, &str, &str, u32)] = &[("h", "codex", "/dup.jsonl", 5)];

    /// The statements that make a generated delete script abort unless the
    /// shipped probe returns exactly `units`.
    fn fixture_unit_agreement_sql(units: &[(&str, &str, &str, u32)]) -> String {
        let probe =
            canonical_candidate_sql("moraine", FIXTURE_HORIZON_SECONDS, FIXTURE_PROBE_LIMIT);
        let body = probe
            .trim_end()
            .trim_end_matches("FORMAT JSONEachRow")
            .trim_end()
            .to_string();
        let mut keys: Vec<String> = units
            .iter()
            .map(|(host, name, file, generation)| {
                crate::escape_literal(&format!("{host}/{name}/{file}/{generation}"))
            })
            .collect();
        keys.sort();
        format!(
            "-- Unit-set agreement, checked BEFORE any delete runs. The unit list in\n\
             -- `reclaim_canonical.rs` is hand-maintained; the probe is the truth. If they\n\
             -- disagree this file aborts, having deleted nothing.\n\
             SELECT throwIf(\n  \
               arraySort(groupArray(concat(source_host, '/', source_name, '/', source_file, '/', \
             toString(source_generation))))\n    \
                 != [{}],\n  \
               'the fixture unit list no longer matches this build''s canonical_generation probe; \
             the DELETE statements below are stale and the census that follows them is meaningless'\n\
             )\nFROM (\n{body}\n)\nFORMAT Null;\n\n",
            keys.join(", "),
        )
    }

    /// The pinned-reader census: what a reader authorized through the
    /// current published heads sees, per relation, using the shipped
    /// `v_live_*` join shapes verbatim (sql/032) — `events FINAL` joined to
    /// the heads; documents behind `doc_len > 0`; postings behind the
    /// document join; `tool_io`/`event_links` behind the live-events
    /// version join. Interleaved between the satellite and canonical deletes
    /// of one unit: identical counts at 'before', 'mid-unit' and 'after' are
    /// the proof that a pinned reader cannot observe the reclaim at all —
    /// including the per-term `df` line, which is the statistic a wrong
    /// postings delete moves first.
    fn reader_visible_census_sql(label: &str) -> String {
        format!(
            "SELECT '{label}' AS at, src, toUInt64(count()) AS reader_visible_rows\n\
             FROM (\n  \
               SELECT 'events' AS src\n  \
               FROM (SELECT * FROM moraine.events FINAL) AS e\n  \
               ALL INNER JOIN moraine.v_current_published_source_generations AS h\n    \
                 ON e.source_host = h.source_host AND e.source_name = h.source_name\n   \
                 AND e.source_file = h.source_file AND e.source_generation = h.source_generation\n  \
               UNION ALL\n  \
               SELECT 'documents' AS src\n  \
               FROM (SELECT * FROM moraine.search_documents FINAL) AS d\n  \
               ALL INNER JOIN moraine.v_current_published_source_generations AS h\n    \
                 ON d.source_host = h.source_host AND d.source_name = h.source_name\n   \
                 AND d.source_file = h.source_file AND d.source_generation = h.source_generation\n  \
               WHERE d.doc_len > 0\n  \
               UNION ALL\n  \
               SELECT 'tool_io' AS src\n  \
               FROM (SELECT * FROM moraine.tool_io FINAL) AS t\n  \
               ALL INNER JOIN (\n    \
                 SELECT e.source_host AS source_host, e.event_uid AS event_uid, \
             e.event_version AS event_version\n    \
                 FROM (SELECT * FROM moraine.events FINAL) AS e\n    \
                 ALL INNER JOIN moraine.v_current_published_source_generations AS h\n      \
                   ON e.source_host = h.source_host AND e.source_name = h.source_name\n     \
                   AND e.source_file = h.source_file AND e.source_generation = h.source_generation\n  \
               ) AS le ON t.source_host = le.source_host AND t.event_uid = le.event_uid\n   \
                 AND t.source_event_version != 0 AND t.source_event_version = le.event_version\n  \
               UNION ALL\n  \
               SELECT 'event_links' AS src\n  \
               FROM (SELECT * FROM moraine.event_links FINAL) AS l\n  \
               ALL INNER JOIN (\n    \
                 SELECT e.source_host AS source_host, e.event_uid AS event_uid, \
             e.event_version AS event_version\n    \
                 FROM (SELECT * FROM moraine.events FINAL) AS e\n    \
                 ALL INNER JOIN moraine.v_current_published_source_generations AS h\n      \
                   ON e.source_host = h.source_host AND e.source_name = h.source_name\n     \
                   AND e.source_file = h.source_file AND e.source_generation = h.source_generation\n  \
               ) AS le ON l.source_host = le.source_host AND l.event_uid = le.event_uid\n   \
                 AND l.source_event_version != 0 AND l.source_event_version = le.event_version\n\
             )\n\
             GROUP BY src\nORDER BY src ASC\nFORMAT TSVWithNames;\n\n\
             SELECT '{label}' AS at, p.term AS term, toUInt64(count()) AS live_df\n\
             FROM (SELECT * FROM moraine.search_postings FINAL) AS p\n\
             ALL INNER JOIN (\n  \
               SELECT d.source_host AS source_host, d.source_name AS source_name,\n         \
                 d.session_id AS session_id, d.source_ref AS source_ref,\n         \
                 d.event_uid AS event_uid, d.doc_version AS doc_version\n  \
               FROM (SELECT * FROM moraine.search_documents FINAL) AS d\n  \
               ALL INNER JOIN moraine.v_current_published_source_generations AS h\n    \
                 ON d.source_host = h.source_host AND d.source_name = h.source_name\n   \
                 AND d.source_file = h.source_file AND d.source_generation = h.source_generation\n  \
               WHERE d.doc_len > 0\n\
             ) AS d\n  \
               ON p.source_host = d.source_host AND p.source_name = d.source_name\n \
               AND p.session_id = d.session_id AND p.source_ref = d.source_ref\n \
               AND p.doc_id = d.event_uid AND p.post_version = d.doc_version\n\
             GROUP BY term\nORDER BY term ASC\nFORMAT TSVWithNames;\n\n"
        )
    }

    /// Physical survivor census, per generation tag. Every tag carries the
    /// row's own `source_name`, which is what attributes the dual-uid
    /// fixture's two same-uid rows to their two source names; every fixture
    /// uid is additionally prefixed with a generation marker (`g5u…`,
    /// `dU…`), so `tool_io`/`event_links` — which carry no generation column
    /// (hazard H3) — stay attributable after their parent `events` rows are
    /// gone.
    fn physical_census_sql(label: &str) -> String {
        format!(
            "SELECT '{label}' AS at, src, gen_tag, toUInt64(count()) AS physical_rows\n\
             FROM (\n  \
               SELECT 'events' AS src, concat(source_file, '#', toString(source_generation), \
             '@', source_name) AS gen_tag FROM moraine.events\n  \
               UNION ALL SELECT 'raw_events', concat(source_file, '#', \
             toString(source_generation), '@', source_name) FROM moraine.raw_events\n  \
               UNION ALL SELECT 'ingest_errors', concat(source_file, '#', \
             toString(source_generation), '@', source_name) FROM moraine.ingest_errors\n  \
               UNION ALL SELECT 'search_documents', concat(source_file, '#', \
             toString(source_generation), '@', source_name) FROM moraine.search_documents\n  \
               UNION ALL SELECT 'search_postings', concat('uid:', substring(doc_id, 1, 2), \
             '@', source_name) FROM moraine.search_postings\n  \
               UNION ALL SELECT 'tool_io', concat('uid:', substring(event_uid, 1, 2), \
             '@', source_name) FROM moraine.tool_io\n  \
               UNION ALL SELECT 'event_links', concat('uid:', substring(event_uid, 1, 2), \
             '@', source_name) FROM moraine.event_links\n\
             )\n\
             GROUP BY src, gen_tag\nORDER BY src ASC, gen_tag ASC\nFORMAT TSVWithNames;\n\n"
        )
    }

    /// This build's delete statements for the given fixture units, emitted
    /// through [`emit_delete_statement`] under the full three-token authority
    /// — verbatim what a configured run issues.
    fn fixture_unit_statements(units: &[(&str, &str, &str, u32)]) -> Vec<(ReclaimTable, String)> {
        let authorities = full_authority();
        units
            .iter()
            .flat_map(|(host, name, file, generation)| {
                let unit = ReclaimUnit {
                    reclaim_id: String::new(),
                    scope: ReclaimScope::CanonicalGeneration,
                    source_host: (*host).to_string(),
                    source_name: (*name).to_string(),
                    source_file: (*file).to_string(),
                    source_generation: *generation,
                    session_id: String::new(),
                    candidate_generation: 0,
                    phase: ReclaimPhase::Claimed,
                    estimated_rows: 0,
                    estimated_bytes: 0,
                    unsettled_seconds: 0,
                };
                canonical_unit_predicates("moraine", &unit)
                    .into_iter()
                    .map(|(table, predicate)| {
                        let statement =
                            emit_delete_statement("moraine", table, &predicate, &authorities)
                                .expect("every fixture unit must be emittable");
                        (table, statement)
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The number of leading deletes that are satellite/derived deletes —
    /// everything before the first bucket-1/2 table. The interrupted script
    /// stops exactly here, which is the crash window the task's re-drive
    /// property is about.
    fn satellite_statement_count() -> usize {
        ReclaimScope::CanonicalGeneration
            .tables()
            .iter()
            .take_while(|table| classify(table.name()) == Some(TableClass::Derived))
            .count()
    }

    /// The full delete script: agreement check, reader census 'before', the
    /// unit's seven deletes canonical-last with the reader census re-run
    /// between the last satellite delete and the first canonical delete,
    /// reader census 'after', physical survivor census.
    ///
    /// Doubles as the **re-drive** script: run over a database where
    /// `canonical_interrupt.sql` already applied the satellite prefix, every
    /// prefix delete re-executes over already-deleted rows and removes zero
    /// more, and the canonical suffix completes the unit — the §3.2 replay,
    /// executed by hand.
    fn fixture_delete_script() -> String {
        delete_script_for(
            "-- GENERATED by `reclaim_canonical::tests::fixture_delete_script`; do not\n\
             -- hand-edit. Regenerate with MORAINE_UPDATE_RECLAIM_GOLDEN=1.\n\
             --\n\
             -- Run after `fixture_canonical.sql` against the same `--path` — either directly\n\
             -- (the uninterrupted unit) or after `canonical_interrupt.sql` (the re-drive of a\n\
             -- unit that crashed between the satellite and canonical deletes; the satellite\n\
             -- deletes below then re-execute over already-deleted rows and remove zero more).\n\
             -- Every DELETE is `emit_delete_statement(canonical_unit_predicates(unit))`\n\
             -- verbatim for every unit the shipped probe returns against that fixture,\n\
             -- children first, `events` LAST. The pinned-reader census re-runs BETWEEN the\n\
             -- satellite and canonical deletes: identical counts at 'before', 'mid-unit' and\n\
             -- 'after' are the proof that a reader authorized through the published heads\n\
             -- cannot observe the reclaim at any point.\n\n",
            FIXTURE_UNITS,
        )
    }

    /// The dual-uid delete script: the same census interleaving as
    /// [`fixture_delete_script`], over the fixture whose one uid lives under
    /// two source names. The reader censuses staying identical is plan
    /// §7.2's G-DUALUID claim executed in `clickhouse local`.
    fn fixture_dualuid_delete_script() -> String {
        delete_script_for(
            "-- GENERATED by `reclaim_canonical::tests::fixture_dualuid_delete_script`; do\n\
             -- not hand-edit. Regenerate with MORAINE_UPDATE_RECLAIM_GOLDEN=1.\n\
             --\n\
             -- Run after `fixture_canonical_dualuid.sql` against the same `--path`. The\n\
             -- fixture holds ONE uid under TWO source names on one host — the uid material\n\
             -- excludes `source_name` (`sources/shared.rs`), so overlapping source\n\
             -- definitions share uids — with the `codex` attribution retired and the\n\
             -- `codexalt` attribution live. Every DELETE is\n\
             -- `emit_delete_statement(canonical_unit_predicates(unit))` verbatim for the one\n\
             -- unit the shipped probe returns, children first, `events` LAST. Identical\n\
             -- pinned-reader censuses at 'before', 'mid-unit' and 'after' — documents,\n\
             -- tool_io, event_links and the per-term df — are plan §7.2's G-DUALUID claim\n\
             -- executed in `clickhouse local`: the live name's rows all survive the retired\n\
             -- name's unit.\n\n",
            DUALUID_FIXTURE_UNITS,
        )
    }

    fn delete_script_for(header: &str, units: &[(&str, &str, &str, u32)]) -> String {
        let mut out = String::from(header);
        out.push_str(&fixture_unit_agreement_sql(units));
        out.push_str(&reader_visible_census_sql("before"));
        let statements = fixture_unit_statements(units);
        let satellites = satellite_statement_count();
        for (index, (table, statement)) in statements.iter().enumerate() {
            out.push_str(&format!("-- delete {}: {}\n", index + 1, table.name()));
            out.push_str(statement);
            out.push_str(";\n\n");
            if index + 1 == satellites {
                out.push_str(&reader_visible_census_sql("mid-unit"));
            }
        }
        out.push_str(&reader_visible_census_sql("after"));
        out.push_str(&physical_census_sql("after"));
        out
    }

    /// The interrupted prefix: the satellite deletes only, then the censuses
    /// a crashed-mid-unit database must show — pinned reader unchanged,
    /// canonical rows still intact (the safe half-state), satellites gone.
    fn fixture_interrupt_script() -> String {
        let mut out = String::from(
            "-- GENERATED by `reclaim_canonical::tests::fixture_interrupt_script`; do not\n\
             -- hand-edit. Regenerate with MORAINE_UPDATE_RECLAIM_GOLDEN=1.\n\
             --\n\
             -- The crash window: run after `fixture_canonical.sql` against the same `--path`,\n\
             -- INSTEAD of `canonical_delete.sql`. It applies only the satellite deletes and\n\
             -- stops — the process death between the satellite and canonical deletes. The\n\
             -- censuses then show the safe half-state the ledger re-drives from: the pinned\n\
             -- reader unchanged, every bucket-1/2 row intact, satellites gone. Follow with\n\
             -- `canonical_delete.sql` as the re-drive; its satellite deletes remove zero\n\
             -- additional rows and the unit completes canonical-last.\n\n",
        );
        out.push_str(&fixture_unit_agreement_sql(FIXTURE_UNITS));
        out.push_str(&reader_visible_census_sql("before"));
        let statements = fixture_unit_statements(FIXTURE_UNITS);
        for (index, (table, statement)) in statements
            .iter()
            .take(satellite_statement_count())
            .enumerate()
        {
            out.push_str(&format!("-- delete {}: {}\n", index + 1, table.name()));
            out.push_str(statement);
            out.push_str(";\n\n");
        }
        out.push_str(&reader_visible_census_sql("interrupted"));
        out.push_str(&physical_census_sql("interrupted"));
        out
    }

    /// **G-CG-PROBE.** Fails for: a probe that decides retirement from the
    /// data instead of consuming publication truth (INV-1), that drops a
    /// conjunct of R1–R3, that reasons about generation order, or that scans
    /// a table with `FINAL` (hazard H4).
    /// Denomination: statement text, one assertion per conjunct.
    ///
    /// MUTATION (executed 2026-07-31): replace the `FROM
    /// {database}.v_published_source_generation_history` source with
    /// `{database}.events` — deriving the candidate set from the data being
    /// deleted, the INV-1 violation => FAILS on the R1 assertion. **Lower
    /// bound.**
    ///
    /// MUTATION (executed 2026-07-31): drop `h.source_generation !=
    /// head.source_generation` => FAILS on the R2 assertion. **Lower bound:
    /// without it every published generation, including every live head, is
    /// a unit.**
    ///
    /// MUTATION (executed 2026-07-31): drop the `head.published_at < …`
    /// horizon clause => FAILS on the R3 assertion. **Lower bound on the
    /// horizon.**
    ///
    /// MUTATION (executed 2026-07-31): append ` FINAL` to the `events`
    /// rollup branch => FAILS on the H4 assertion. **Width: the probe's
    /// cost, not only its extent.**
    ///
    /// MUTATION (executed 2026-07-31): make `canonical_candidate_sql` return
    /// `String::new()` => FAILS on the R1 assertion, so an empty probe is
    /// not a passing "fix". **Upper bound.**
    #[test]
    fn the_canonical_probe_consumes_publication_truth_and_never_scans_final() {
        let sql = canonical_candidate_sql("moraine", 604_800, 512);
        // R1 — the candidate set is publication history, never the data.
        assert!(
            sql.contains("FROM `moraine`.v_published_source_generation_history AS h"),
            "R1: candidates come from publication truth, not from the tables the scope \
             deletes: {sql}"
        );
        // R2 — retired means a different generation is the current head.
        assert!(
            sql.contains("INNER JOIN heads AS head"),
            "R2: a file with no published head has no supersession decision — the join must \
             be inner, fail-closed: {sql}"
        );
        assert!(
            sql.contains("FROM `moraine`.v_current_published_source_generations"),
            "R2 reads the current heads, it does not recompute them: {sql}"
        );
        assert!(
            sql.contains("WHERE h.source_generation != head.source_generation"),
            "R2: {sql}"
        );
        // No generation-order reasoning anywhere: supersession is head
        // identity, so a rollback that re-publishes an older generation is
        // honoured in both directions.
        assert!(
            !sql.contains("source_generation <") && !sql.contains("max(source_generation)"),
            "retirement must never rest on generation order: {sql}"
        );
        // R3 — the horizon anchors on the superseding head's publication time.
        assert!(
            sql.contains("AND head.published_at < now64(3) - toIntervalSecond(604800)"),
            "R3: the freshly-superseded generation is exactly the one a just-pinned reader \
             (or an operator about to roll back) can still need: {sql}"
        );
        // H4 — no FINAL on any table this scope touches, and no payload
        // column anywhere in the probe.
        for table in ReclaimScope::CanonicalGeneration.tables() {
            assert!(
                !sql.contains(&format!("{} FINAL", table.name())),
                "H4: `{}` must not be scanned with FINAL: {sql}",
                table.name()
            );
        }
        for payload in ["payload_json", "text_content", "raw_json", "output_json"] {
            assert!(
                !sql.contains(payload),
                "H4: the probe must not read payload column `{payload}`: {sql}"
            );
        }
        // The rollups are filtered to the retired set before grouping, so the
        // probe's extent is the candidate mass, not the table.
        assert_eq!(
            sql.matches(
                "WHERE (source_host, source_name, source_file, source_generation) IN (\n      \
                 SELECT source_host, source_name, source_file, source_generation FROM retired\n    )"
            )
            .count(),
            4,
            "each of the four rollup branches must pre-filter to the retired set: {sql}"
        );
        assert!(sql.contains("LIMIT 512"), "{sql}");
        assert!(
            sql.contains(
                "ORDER BY source_host ASC, source_name ASC, source_file ASC, source_generation ASC"
            ),
            "claim order must be total so a re-drive resumes the same order: {sql}"
        );
    }

    /// **G-CG-EMPTY (R4).** Fails for: a probe that emits a retired
    /// generation no generation-keyed table holds rows for — an empty unit
    /// re-claimed on every run forever.
    /// Denomination: statement text — the rollup join is `INNER`, and the
    /// rollup names exactly the four generation-keyed tables (the three
    /// satellites are deliberately absent; their reachable rows derive from
    /// the rolled-up tables' rows, so they cannot make a unit non-empty on
    /// their own).
    ///
    /// MUTATION (executed 2026-07-31): change `INNER JOIN cg_rollup` to
    /// `LEFT JOIN cg_rollup` => FAILS here, and against the fixture the
    /// probe additionally returns `/empty.jsonl` gen 1 — retired in history,
    /// zero rows anywhere. **Lower bound.**
    #[test]
    fn a_retired_generation_with_no_canonical_rows_is_not_a_unit() {
        let sql = canonical_candidate_sql("moraine", 604_800, 512);
        assert!(
            sql.contains("INNER JOIN cg_rollup ON cg_rollup.source_host = r.source_host"),
            "R4: a unit must own at least one canonical row, or it settles nothing and never \
             converges: {sql}"
        );
        assert!(!sql.contains("LEFT JOIN cg_rollup"), "{sql}");
        for rolled in [
            "moraine`.events",
            "raw_events",
            "ingest_errors",
            "search_documents",
        ] {
            assert!(
                sql.contains(rolled),
                "the rollup must cover `{rolled}`: {sql}"
            );
        }
        for absent in ["search_postings", "tool_io", "event_links"] {
            assert!(
                !sql.contains(absent),
                "the probe must not scan `{absent}` — its reachable rows derive from the \
                 rolled-up tables (module docs, cost shape): {sql}"
            );
        }
    }

    /// The config→statement half of the horizon path for this scope. The
    /// wiring from `probe_horizon_seconds` into `(executor.probe)` is gated
    /// by `reclaim::tests::the_canonical_probe_carries_the_stricter_protected_horizon`,
    /// which reads the statement a run actually issued.
    ///
    /// MUTATION (executed 2026-07-31): hard-code `604800` for
    /// `{horizon_seconds}` in `canonical_candidate_sql` => FAILS here on the
    /// non-floor case. **Width: the parameter travels.**
    #[test]
    fn the_canonical_probe_horizon_is_the_callers_not_a_constant() {
        assert!(
            canonical_candidate_sql("moraine", 604_800, 64).contains("toIntervalSecond(604800)")
        );
        assert!(
            canonical_candidate_sql("moraine", 7_776_000, 64).contains("toIntervalSecond(7776000)")
        );
    }

    /// **G-CG-EXTENT.** Fails for: **any** change to a canonical delete
    /// predicate's text, in either direction.
    /// Denomination: the exact predicate string, per table, in the exact
    /// canonical-last order.
    ///
    /// The equality pin is the `reclaim_read_index` G-RI-EXTENT shape,
    /// carried over with its rationale: every other guard is a `contains`,
    /// and `contains` cannot see a predicate that *widens* — the widened
    /// text still contains the narrow text, and the widening is what empties
    /// a table. Here the pin also freezes the hazard answers as text: the
    /// postings predicate joins through the document and binds the posting's
    /// own `source_host` **and `source_name`** (H1 + the shared-backend host
    /// collision + the same-host cross-name uid collision — the G-DUALUID
    /// shape), and the `tool_io`/`event_links` predicates derive their
    /// `(source_host, source_name, event_uid)` set from `events` (H3, with
    /// the same two binds). For all three satellite binds the emitter is
    /// **not** an independent gate: the generation tuple inside each
    /// subquery already names `source_host`/`source_name`, so the name-
    /// presence check accepts a predicate that lost an outer bind — this pin
    /// and the fixture are the gates.
    ///
    /// MUTATION (executed 2026-07-31): predicate the postings delete on the
    /// posting's **own** generation columns —
    /// `(source_file, source_generation) IN ((file, gen))` — hazard H1's
    /// shape => FAILS here, and the emitter refuses it too
    /// (`UnboundPredicate` naming `doc_id`): two independent gates. Executed
    /// against the fixture as well: with the H1 anti-join spelling
    /// (`(source_file, source_generation) NOT IN (SELECT … FROM heads)`) the
    /// pre-032-shaped posting under the LIVE `/pre032.jsonl` document is
    /// deleted and the live `df` census loses `preterm` — the corpus-loss
    /// shape H1 names. **Lower bound, and the hazard.**
    ///
    /// MUTATION (executed 2026-07-31): drop `source_host = {host} AND ` from
    /// the postings predicate => FAILS here, with only the golden (which
    /// embeds this predicate's text) following — `doc_id` is still named, so
    /// the emitter accepts it and this pin is the one independent gate.
    /// Executed against the
    /// fixture: host `h2`'s postings for the shared uids of its still-live
    /// `/live.jsonl` generation 5 are deleted with host `h`'s — the
    /// cross-host loss the host bind exists for. **Width.**
    ///
    /// MUTATION (executed 2026-07-31): drop `source_name = {name} AND ` from
    /// the postings predicate => FAILS here (the emitter accepts — the
    /// subquery's tuple still names the column — so this pin is the one
    /// independent gate). Executed against the dual-uid fixture: the retired
    /// `codex` unit's delete takes the live `codexalt` posting for the
    /// shared uid with it and the live per-term df census loses `delta` —
    /// the G-DUALUID loss the name bind exists for. **Width.**
    ///
    /// MUTATION (executed 2026-07-31): narrow the `tool_io` arm alone to a
    /// name-less `(source_host, event_uid)` set => FAILS here on the
    /// `tool_io` row. Executed against the dual-uid fixture: the live
    /// `codexalt` tool_io row for the shared uid is deleted and the
    /// pinned-reader tool_io census drops 1 → 0. **Width.**
    ///
    /// MUTATION (executed 2026-07-31): the same name-less narrowing for
    /// `event_links` alone => FAILS here on the `event_links` row. Executed
    /// against the dual-uid fixture: pinned-reader event_links census
    /// 1 → 0. **Width.**
    ///
    /// MUTATION (executed 2026-07-31): replace the `tool_io` uid-set
    /// subquery source `{database}.events` with a generation tuple predicate
    /// on `tool_io`'s own columns => the emitter refuses
    /// (`UnboundPredicate`: a `UidSet` delete must name `event_uid`), which
    /// is hazard H3 enforced by the shape — and this pin FAILS first.
    /// **Width: the type carries the hazard.**
    ///
    /// MUTATION (executed 2026-07-31): append ` OR source_file = {file}` to
    /// the events predicate => FAILS here. Executed against the fixture: the
    /// widened predicate takes `/live.jsonl`'s live generation 6 to zero
    /// events rows — the loss shape this pin exists to stop. **Upper bound —
    /// the direction nothing else covers.**
    ///
    /// MUTATION (executed 2026-07-31): swap the `events` entry before
    /// `raw_events` in `ReclaimScope::tables` => FAILS here on order (and
    /// `the_canonical_unit_deletes_canonical_last` fails with it). **Order
    /// is part of the pin.**
    #[test]
    fn the_canonical_delete_predicates_are_pinned_exactly_in_both_directions() {
        let predicates = canonical_unit_predicates("moraine", &unit());
        let rendered: Vec<(&'static str, String)> = predicates
            .iter()
            .map(|(table, predicate)| (table.name(), predicate.clone()))
            .collect();
        let tuple = "(source_host, source_name, source_file, source_generation) IN \
                     (('h\\'1', 'codex', '/a\\'b.jsonl', toUInt32(7)))"
            .to_string();
        let uid_set = format!(
            "(source_host, source_name, event_uid) IN (SELECT source_host, source_name, \
             event_uid FROM `moraine`.events WHERE {tuple})"
        );
        assert_eq!(
            rendered,
            vec![
                (
                    "search_postings",
                    format!(
                        "source_host = 'h\\'1' AND source_name = 'codex' AND doc_id IN \
                         (SELECT event_uid FROM `moraine`.search_documents WHERE {tuple})"
                    ),
                ),
                ("search_documents", tuple.clone()),
                ("tool_io", uid_set.clone()),
                ("event_links", uid_set),
                ("ingest_errors", tuple.clone()),
                ("raw_events", tuple.clone()),
                ("events", tuple),
            ],
            "a canonical delete predicate (or the unit's delete order) changed. These are the \
             statements that can destroy user history; re-derive the extent and the ordering \
             argument and update this pin deliberately, never to make a build green."
        );
        for (table, predicate) in &predicates {
            let statement = emit_delete_statement("moraine", *table, predicate, &full_authority())
                .unwrap_or_else(|refusal| {
                    panic!("`{}` must be emittable: {refusal}", table.name())
                });
            assert!(
                statement.starts_with("DELETE FROM `moraine`."),
                "{statement}"
            );
            assert!(
                !predicate.contains("session_id"),
                "H2: one `search_documents` row serves both attributions of a \
                 double-attributed uid, so `session_id` must appear in no canonical \
                 predicate: {predicate}"
            );
        }
    }

    /// The restart-safety invariant, pinned structurally: within one unit,
    /// every predicate that reads another reclaim table by subquery reads
    /// only tables deleted **strictly later** in the unit's own order. That
    /// is the whole re-derivability argument (module docs): a crash leaves a
    /// strict prefix applied, so on replay every already-applied delete
    /// removes zero more rows and every not-yet-applied delete still finds
    /// its subquery sources intact.
    ///
    /// MUTATION (executed 2026-07-31): swap `SearchPostings` after
    /// `SearchDocuments` in `ReclaimScope::tables`'s `CanonicalGeneration`
    /// arm => FAILS here: the postings predicate would re-derive from a
    /// document set its own unit had already deleted, and a crash between
    /// the two would strand the postings forever — the WI-05 stranded-child
    /// bug, one table over. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-31): move `Events` to the front of the
    /// same arm => FAILS here for both uid-set tables, and
    /// `the_canonical_unit_deletes_canonical_last` fails with it. **Width.**
    #[test]
    fn every_canonical_subquery_source_is_deleted_later_in_the_unit() {
        let predicates = canonical_unit_predicates("moraine", &unit());
        let order: Vec<&'static str> = predicates.iter().map(|(table, _)| table.name()).collect();
        let mut subquery_edges = 0;
        for (position, (table, predicate)) in predicates.iter().enumerate() {
            for (source_position, source) in order.iter().enumerate() {
                if source == &table.name() {
                    continue;
                }
                if predicate.contains(&format!("`moraine`.{source}")) {
                    subquery_edges += 1;
                    assert!(
                        source_position > position,
                        "`{}` re-derives its extent from `{source}`, which its own unit \
                         deletes at position {source_position} <= {position}: a crash between \
                         the two leaves rows no replay can ever name again",
                        table.name()
                    );
                }
            }
        }
        assert_eq!(
            subquery_edges, 3,
            "the postings→documents and tool_io/event_links→events edges are the invariant's \
             subject matter; if an edge was added or removed, re-derive the ordering argument"
        );
    }

    /// Canonical-last, as classes rather than names: every `Derived` table
    /// precedes every bucket-1/2 table, and `events` — the only bucket-1
    /// table — is last of all.
    ///
    /// MUTATION (executed 2026-07-31): move `RawEvents` after `Events` in
    /// `ReclaimScope::tables` => FAILS here on the last-position assertion.
    /// **Lower bound.**
    #[test]
    fn the_canonical_unit_deletes_canonical_last() {
        let tables = ReclaimScope::CanonicalGeneration.tables();
        let classes: Vec<TableClass> = tables
            .iter()
            .map(|table| classify(table.name()).expect("every reclaim table is classified"))
            .collect();
        let first_protected = classes
            .iter()
            .position(|class| *class != TableClass::Derived)
            .expect("the scope names bucket-1/2 tables");
        assert!(
            classes[..first_protected]
                .iter()
                .all(|class| *class == TableClass::Derived),
            "every satellite precedes every protected table: {classes:?}"
        );
        assert!(
            classes[first_protected..]
                .iter()
                .all(|class| *class != TableClass::Derived),
            "no satellite may follow a protected delete: {classes:?}"
        );
        assert_eq!(
            tables.last().map(|table| table.name()),
            Some("events"),
            "bucket 1 is the parent of everything and goes last"
        );
        assert_eq!(classes.last(), Some(&TableClass::CanonicalHistory));
    }

    /// The registered executor runs **this** probe and **these** predicates,
    /// at source-generation grain.
    ///
    /// MUTATION (executed 2026-07-31): point `CanonicalGeneration`'s `probe`
    /// at `reclaim_read_index::read_index_candidate_sql` => FAILS on the
    /// probe equality. **Lower bound.**
    #[test]
    fn the_canonical_executor_runs_its_own_probe_at_source_generation_grain() {
        let executor = executor_for(ReclaimScope::CanonicalGeneration)
            .expect("WI-09 registers the canonical executor");
        assert_eq!(
            (executor.probe)("moraine", 604_800, 8),
            canonical_candidate_sql("moraine", 604_800, 8)
        );
        assert_eq!(
            (executor.predicates)("moraine", &unit()),
            canonical_unit_predicates("moraine", &unit())
        );
        assert_eq!(
            ReclaimScope::CanonicalGeneration.unit_grain(),
            ReclaimUnitGrain::SourceGeneration
        );
    }

    /// The per-bucket horizon rule, pinned at its one computation site:
    /// `probe_horizon_seconds` for this scope is the **stricter (larger)**
    /// of the two configured protected horizons, and a config missing either
    /// key yields no horizon at all (the same `MissingAuthority` the run and
    /// the planner refuse with).
    ///
    /// MUTATION (executed 2026-07-31): take the canonical horizon alone
    /// (drop the `max`) => FAILS on the raw-stricter case: a unit would be
    /// claimed — and `raw_events` audit rows deleted — while
    /// `retention.raw_audit_horizon_days` still protected them. **Lower
    /// bound, and the rule.**
    ///
    /// MUTATION (executed 2026-07-31): fall back to
    /// `derived_horizon_seconds` when the keys are missing => FAILS on the
    /// missing-key case. **Upper bound: absence stays a refusal, never a
    /// shorter horizon.**
    #[test]
    fn the_canonical_horizon_is_the_stricter_of_the_two_protected_horizons() {
        let canonical_stricter = RetentionConfig {
            canonical_history_horizon_days: Some(365.0),
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        };
        assert_eq!(
            probe_horizon_seconds(ReclaimScope::CanonicalGeneration, &canonical_stricter)
                .expect("both keys present"),
            365 * 86_400
        );
        let raw_stricter = RetentionConfig {
            canonical_history_horizon_days: Some(7.0),
            raw_audit_horizon_days: Some(90.0),
            ..RetentionConfig::default()
        };
        assert_eq!(
            probe_horizon_seconds(ReclaimScope::CanonicalGeneration, &raw_stricter)
                .expect("both keys present"),
            90 * 86_400
        );
        for missing in [
            RetentionConfig::default(),
            RetentionConfig {
                canonical_history_horizon_days: Some(365.0),
                ..RetentionConfig::default()
            },
            RetentionConfig {
                raw_audit_horizon_days: Some(90.0),
                ..RetentionConfig::default()
            },
        ] {
            assert!(
                probe_horizon_seconds(ReclaimScope::CanonicalGeneration, &missing).is_err(),
                "a config missing a protected key must yield no horizon, not a default"
            );
        }
        // The bucket-3 scope keeps the derived horizon.
        assert_eq!(
            probe_horizon_seconds(
                ReclaimScope::ReadIndexGeneration,
                &RetentionConfig::default()
            )
            .expect("bucket 3"),
            RetentionConfig::default()
                .derived_horizon_seconds()
                .max(0.0) as u64
        );
    }

    /// **OQ-1, asserted as the resolution it records** (hazard H5). Three
    /// halves: migration 040 drops the accumulator's feeding MV and touches
    /// nothing else destructive; the frozen table stays `NeverDelete` —
    /// unreachable by every authority token, including this scope's full
    /// set; and no reclaim scope names it.
    ///
    /// MUTATION (executed 2026-07-31): reclassify
    /// `search_conversation_terms` as `Derived` => FAILS here on the
    /// authority half (the stock `DerivedOnly` token would unlock it), and
    /// `storage_class::tests::the_never_delete_roster_is_exactly_these_tables` fails with
    /// it. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-31): drop the `DROP VIEW` statement from
    /// migration 040 => FAILS here on the migration half: the decommission
    /// this module's docs and `docs/configuration.md` record would be a
    /// sentence about nothing. **Upper bound.**
    #[test]
    fn oq1_search_conversation_terms_is_decommissioned_not_reclaimed() {
        // The frozen table is authorized by nothing.
        assert_eq!(
            classify("search_conversation_terms"),
            Some(TableClass::NeverDelete)
        );
        for authority in full_authority() {
            assert!(
                !authority.authorizes(TableClass::NeverDelete),
                "no token may unlock the frozen accumulator"
            );
        }
        // No scope names it.
        for scope in ReclaimScope::ALL {
            assert!(
                scope
                    .tables()
                    .iter()
                    .all(|table| table.name() != "search_conversation_terms"),
                "`{scope}` must not name the accumulator"
            );
        }
        // Migration 040 drops exactly the MV: one DROP VIEW, no other
        // statement shape, and the table itself is not dropped (that is
        // WI-10's batched cleanup).
        let migration = crate::bundled_migrations()
            .into_iter()
            .find(|migration| migration.version == "040")
            .expect("migration 040 decommissions the conversation-terms MV");
        assert!(
            migration
                .sql
                .contains("DROP VIEW IF EXISTS moraine.mv_search_conversation_terms"),
            "040 must drop the feeding MV: {}",
            migration.sql
        );
        assert!(
            !migration.sql.contains("DROP TABLE"),
            "the frozen table's drop is WI-10's, not 040's: {}",
            migration.sql
        );
    }

    /// The `clickhouse local` fixture's files hold **this build's**
    /// statements, verbatim — a text comparison, not an execution, exactly
    /// as the `reclaim_read_index` golden and for the same reason: the
    /// fixture is executed by hand (`testdata/reclaim_probe/
    /// fixture_canonical_ddl.sql` carries the recipe), and a hand-run
    /// fixture that silently measures the previous build is worse than no
    /// fixture. The `reclaim-generation` live gate (plan §7.2) remains
    /// unwritten in this branch.
    ///
    /// MUTATION (executed 2026-07-31): add ` AND 1` to the probe's
    /// retirement `WHERE` => FAILS here on `canonical.sql`.
    ///
    /// MUTATION (executed 2026-07-31): remove the entry from
    /// `FIXTURE_UNITS` => FAILS here on `canonical_interrupt.sql`, the first
    /// of the two delete scripts compared — both generated scripts embed the
    /// expected unit array in a `throwIf`, so `canonical_delete.sql` is
    /// equally stale behind it.
    ///
    /// Regenerate after an intentional edit with
    /// `MORAINE_UPDATE_RECLAIM_GOLDEN=1 cargo test -p moraine-clickhouse
    /// the_canonical_fixture_files_are_this_builds_statements_verbatim`,
    /// then re-run the fixture and update its expected-output header comment.
    #[test]
    fn the_canonical_fixture_files_are_this_builds_statements_verbatim() {
        let cases = [
            (
                include_str!("testdata/reclaim_probe/expected/canonical.sql"),
                canonical_candidate_sql("moraine", FIXTURE_HORIZON_SECONDS, FIXTURE_PROBE_LIMIT)
                    .replace("FORMAT JSONEachRow", "FORMAT TSVWithNames"),
                "canonical.sql",
            ),
            (
                include_str!("testdata/reclaim_probe/expected/canonical_interrupt.sql"),
                fixture_interrupt_script(),
                "canonical_interrupt.sql",
            ),
            (
                include_str!("testdata/reclaim_probe/expected/canonical_delete.sql"),
                fixture_delete_script(),
                "canonical_delete.sql",
            ),
            (
                include_str!("testdata/reclaim_probe/expected/canonical_dualuid_delete.sql"),
                fixture_dualuid_delete_script(),
                "canonical_dualuid_delete.sql",
            ),
        ];
        let update = std::env::var_os("MORAINE_UPDATE_RECLAIM_GOLDEN").is_some();
        for (golden, expected, name) in cases {
            if update {
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/testdata/reclaim_probe/expected")
                    .join(name);
                std::fs::write(&path, format!("{}\n", expected.trim_end()))
                    .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
                continue;
            }
            assert_eq!(
                golden.trim_end(),
                expected.trim_end(),
                "`testdata/reclaim_probe/expected/{name}` is stale; regenerate it with \
                 MORAINE_UPDATE_RECLAIM_GOLDEN=1 before trusting the fixture's output"
            );
        }
        assert!(
            !update,
            "golden files regenerated; re-run without the env var"
        );
    }
}
