//! Issue #603 WI-07 — the canonical read-index candidate probe and the delete
//! predicates it authorizes ([`crate::reclaim::ReclaimScope::ReadIndexGeneration`]).
//!
//! Targets the three migration-036 read indexes — `mcp_event_navigation`,
//! `mcp_event_locator`, `mcp_session_directory` — and reclaims rows belonging
//! to **retired** source generations: generations publication truth has
//! affirmatively superseded. All three tables are content-free by design
//! (`sql/036`) and fully rebuildable with `moraine db core-index rebuild`,
//! which is the documented recovery path if a reclaim here ever proves wrong.
//!
//! The probe and the deletes live in one module on the `reclaim_mcp_open`
//! precedent: a scope's candidate set, its bounding, and the exact predicate
//! each of its tables receives are one reviewable unit.
//!
//! ## What "retired" means here, and why it is publication-aware
//!
//! A generation is a candidate when **all four** hold:
//!
//! * **R1 — it was published.** Its `(source_host, source_name, source_file,
//!   source_generation)` has a row in `v_published_source_generation_history`.
//!   Publication history is append-only (`sql/031` states the no-deletes
//!   invariant), so this is a durable decision, read, never recomputed
//!   (INV-1).
//! * **R2 — it is no longer the head.** Its file's row in
//!   `v_current_published_source_generations` names a **different**
//!   generation. R1 ∧ R2 is exactly "a newer publication decision retired
//!   it": `v_current` is the argMax over the same relation R1 reads, so a
//!   history row that is not current is one a later head `INSERT` displaced.
//!   Generation *order* appears nowhere — a rollback that re-publishes an
//!   older generation makes that generation current again and its successor
//!   retired, and this predicate follows publication truth in both
//!   directions.
//! * **R3 — horizon.** The current head was published longer ago than the
//!   bucket-3 safety horizon (`head.published_at < now() - horizon`). This is
//!   what protects the freshly-superseded generation: a request that captured
//!   its publication snapshot just *before* the new head landed can still be
//!   executing with the old generation pinned, and an operator noticing a bad
//!   publication needs time to roll back (plan OQ-6). Chained supersessions
//!   anchor on the *newest* head's `published_at`, which can only hold a unit
//!   back longer — conservative, and it converges within one horizon.
//! * **R4 — it has rows to reclaim.** The unit is emitted only when at least
//!   one of the three tables holds rows at its key. A retired generation the
//!   indexes no longer mention deletes nothing, and emitting it would claim
//!   an empty unit on every tick forever.
//!
//! ## The reader-visibility argument
//!
//! Every reader of the three tables — the v2 `open` reader
//! (`canonical_open.rs`), the #599 discovery page (`canonical_list.rs`,
//! `list.rs`), canonical search (`search_canonical.rs`), and the monitor's
//! session page — filters every row through the pinned published head set:
//! either an `ALL INNER JOIN` against `published_generations_subquery()` or a
//! tuple-`IN` of the same subquery, which reconstructs one head per file from
//! `v_published_source_generation_history` under
//! `publication_revision <= R`. `R` is captured from **current** state at
//! request start (`consistency.rs`, `capture_publication_snapshot`) and
//! revalidated at request end; it is never client-supplied. So:
//!
//! * a generation retired at revision `r_new` is selectable only by a request
//!   whose pin predates `r_new` — a window of one request lifetime
//!   (capture → revalidate, seconds), which the horizon exceeds by orders of
//!   magnitude;
//! * a generation that was **never** published is selectable at *no*
//!   revision, and is also never a candidate here (R1) — see the residue note
//!   below;
//! * **a reader cannot straddle a partially-reclaimed generation.** The three
//!   deletes of one unit are not atomic, but every row they remove belongs to
//!   a generation no reachable pin selects, so a request arriving between the
//!   navigation delete and the locator delete sees exactly what a request
//!   after the last delete sees: the live head's rows, all of them, and
//!   nothing else. Cross-table consistency is a property of the head join,
//!   not of delete ordering. The read-index fixture proves the safe case by
//!   re-running the head-joined census between two deletes of one unit.
//!
//! ## Per-table key analysis (why one predicate shape is safe on all three)
//!
//! The delete predicate is the full generation tuple
//! `(source_host, source_name, source_file, source_generation)`, and all
//! three tables carry all four as plain columns (`sql/036`). Their engines
//! differ, so each table gets its own argument:
//!
//! * `mcp_event_navigation` — `ReplacingMergeTree(event_version)`,
//!   session-hash partitioned, `source_generation` **in the sort key**: two
//!   generations never collapse into one row, so the tuple's extent is exact.
//! * `mcp_session_directory` — `AggregatingMergeTree` with
//!   `source_generation` **in the sort key**: one aggregate row set per
//!   generation, same argument.
//! * `mcp_event_locator` — `ReplacingMergeTree(event_version)`
//!   `ORDER BY (event_uid, source_host)`: the generation is **not** in the
//!   sort key, but `event_uid` is
//!   `SHA256(source_file | source_generation | line | offset | fingerprint |
//!   suffix)` (`sources/shared.rs`), so `(source_file, source_generation)` is
//!   uid-determined and the tuple's extent is exact anyway. What is **not**
//!   uid-determined is `session_id` — the uid excludes it (#608), one
//!   physical locator row serves both attributions of a double-attributed
//!   uid, and a locator delete must therefore never be predicated on
//!   `session_id` (hazard H2). The predicate here names no `session_id`, and
//!   `the_read_index_delete_predicates_are_pinned_exactly_in_both_directions`
//!   pins that.
//!
//! Delete order is [`crate::reclaim::ReclaimScope::tables`]'s: navigation,
//! locator, directory — bulk first, per-session discovery scalars last. There
//! is no parent/child relation among the three (deletes are
//! children-of-nothing); reader atomicity comes from the pin argument above,
//! not from ordering.
//!
//! ## The rollback caveat, stated rather than implied
//!
//! Unlike the `mcp_open` retirement scope, whose P4 (the monotone dirty
//! revision) makes a retired snapshot unselectable **forever**, a retired
//! read-index generation can become selectable again: an operator rollback
//! re-publishes the old generation and the head join follows it. The horizon
//! is the rollback window this scope honours. A rollback executed *within*
//! the horizon finds every row intact (the unit was never claimed); a
//! rollback *past* the horizon re-selects a generation whose index rows are
//! gone and must be followed by `moraine db core-index rebuild` — the
//! recovery path `docs/operations/canonical-read-indexes.md` documents. The
//! canonical rows themselves (`events`) are bucket 1 and untouched by this
//! scope, which is what makes the rebuild possible.
//!
//! ## Recorded residue: never-published generations are not collected
//!
//! Measured read-only on the reference host 2026-07-31: the three tables
//! carry 76 generation tuples that are not current heads — 152 676
//! `mcp_event_navigation` rows, 152 670 `mcp_event_locator` rows, 76
//! `mcp_session_directory` row groups (≈7.5% of each event-grained table,
//! ≈32 MiB compressed) — and **zero** of them appear in publication history.
//! 69 sit below their file's current head (superseded before the #602
//! publication tables existed, so no durable decision names them); 7 belong
//! to files with no published head at all. R1 excludes every one of them,
//! deliberately: a generation with no history row is indistinguishable, from
//! publication truth alone, from one whose head `INSERT` is about to land —
//! the read-index rows are written by MVs on the `events` insert, **before**
//! the head is published, so "in the indexes but not in history" is exactly
//! what a publication in flight looks like. Collecting the pre-#602 residue
//! needs an orphan-shaped scope with its own safety argument (the analog of
//! `McpOpenOrphan`), not a widened retirement predicate; until then the
//! residue is bounded (it can never grow — every post-#602 generation is
//! published) and one `core-index rebuild` removes it entirely.
//!
//! ## Cost shape (hazard H4)
//!
//! No statement here reads a payload column and nothing uses `FINAL`. The
//! retirement set is computed on the publication views (5 446 history rows on
//! the reference host), and the per-table rollups are `GROUP BY` over the
//! generation tuple **filtered to the retired set first**, so their extent is
//! the candidate mass, not the table.
//!
//! The unit **deletes** are unprunable, and that is a property of the sort
//! keys: none of the three tables leads its primary key or partition key with
//! any column of the generation tuple, so each delete reads every part of its
//! table. What bounds it is the tables' size — ~0.4 GiB across all three on
//! the reference host, against ~10 GiB for one `mcp_open_events` scan — and
//! the shared [`crate::reclaim::RECLAIM_MAX_UNITS_PER_RUN`] page;
//! `reclaim::tests::the_per_run_unit_bound_is_derived_from_the_measured_unit_cost`
//! keeps the two magnitudes in their own denominations.

use crate::escape_identifier;
use crate::reclaim::{ReclaimTable, ReclaimUnit};

/// Candidate retired read-index units (plan §3.4, WI-07): one unit per
/// retired `(source_host, source_name, source_file, source_generation)`.
///
/// See the module docs for the four conjuncts R1–R4 and the mutation ledger
/// on `the_read_index_probe_consumes_publication_truth_and_never_scans_final`.
pub(crate) fn read_index_candidate_sql(
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
         ri_rollup AS (\n    {}\n  )\n\
         SELECT r.source_host AS source_host, r.source_name AS source_name,\n       \
           r.source_file AS source_file, toUInt32(r.source_generation) AS source_generation,\n       \
           toUInt64(sum(ri_rollup.navigation_rows)) AS navigation_rows,\n       \
           toUInt64(sum(ri_rollup.locator_rows)) AS locator_rows,\n       \
           toUInt64(sum(ri_rollup.directory_rows)) AS directory_rows\n\
         FROM retired AS r\n\
         INNER JOIN ri_rollup ON ri_rollup.source_host = r.source_host\n  \
           AND ri_rollup.source_name = r.source_name\n  \
           AND ri_rollup.source_file = r.source_file\n  \
           AND ri_rollup.source_generation = r.source_generation\n\
         GROUP BY r.source_host, r.source_name, r.source_file, r.source_generation\n\
         ORDER BY source_host ASC, source_name ASC, source_file ASC, source_generation ASC\n\
         LIMIT {limit}\n\
         FORMAT JSONEachRow",
        read_index_rollup_sql(&database),
    )
}

/// The three read-index tables, aggregated to the generation tuple, filtered
/// to the retired set **before** grouping, as one relation.
///
/// The `INNER JOIN` between `retired` and this rollup is R4: a retired
/// generation with no rows in any table is not a unit. That is deliberately
/// the inverse of `retired_lineage_candidate_sql`'s `LEFT JOIN` — there a
/// childless unit still owns a header row that must go; here there is no
/// parent row anywhere, so an empty unit would delete nothing and be
/// re-claimed on every tick forever.
///
/// No `FINAL` (hazard H4): a physical row count is the right estimate
/// denomination, and none of these columns is a payload. Rows already masked
/// by a settled unit's lightweight delete are excluded from `count()` by the
/// server, which is what makes the probe converge to empty after a sweep.
fn read_index_rollup_sql(database: &str) -> String {
    let branch = |table: &str, navigation: &str, locator: &str, directory: &str| {
        format!(
            "SELECT source_host, source_name, source_file, source_generation,\n         \
               toUInt64({navigation}) AS navigation_rows, toUInt64({locator}) AS locator_rows,\n         \
               toUInt64({directory}) AS directory_rows\n    \
             FROM {database}.{table}\n    \
             WHERE (source_host, source_name, source_file, source_generation) IN (\n      \
               SELECT source_host, source_name, source_file, source_generation FROM retired\n    )\n    \
             GROUP BY source_host, source_name, source_file, source_generation"
        )
    };
    [
        branch("mcp_event_navigation", "count()", "0", "0"),
        branch("mcp_event_locator", "0", "count()", "0"),
        branch("mcp_session_directory", "0", "0", "count()"),
    ]
    .join("\n    UNION ALL\n    ")
}

/// The delete predicates for one read-index unit, in
/// [`crate::reclaim::ReclaimScope::tables`] order (navigation, locator,
/// directory).
///
/// One predicate text for all three tables, and that is a checked fact, not
/// an assumption: all three carry the full generation tuple as plain columns,
/// and the module docs give the per-table argument for why the tuple's extent
/// is exact on each engine. No predicate names `session_id` — on
/// `mcp_event_locator` it is not key-determined (hazard H2) and a session
/// predicate there deletes rows that also serve another session.
pub(crate) fn read_index_unit_predicates(unit: &ReclaimUnit) -> Vec<(ReclaimTable, String)> {
    let host = crate::escape_literal(&unit.source_host);
    let name = crate::escape_literal(&unit.source_name);
    let file = crate::escape_literal(&unit.source_file);
    let generation = unit.source_generation;
    unit.scope
        .tables()
        .iter()
        .map(|table| {
            (
                *table,
                format!(
                    "(source_host, source_name, source_file, source_generation) IN \
                     (({host}, {name}, {file}, toUInt32({generation})))"
                ),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reclaim::{
        emit_delete_statement, executor_for, ReclaimAuthority, ReclaimPhase, ReclaimScope,
        ReclaimUnitGrain,
    };

    fn unit() -> ReclaimUnit {
        ReclaimUnit {
            reclaim_id: "r-1".to_string(),
            scope: ReclaimScope::ReadIndexGeneration,
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

    /// The horizon and page the fixture's generated files are built at. One
    /// pair, mirroring `reclaim_mcp_open`'s, so the probe golden and the
    /// agreement check inside the delete golden measure one probe.
    const FIXTURE_HORIZON_SECONDS: u64 = 86_400;
    const FIXTURE_PROBE_LIMIT: usize = 512;

    /// The `(source_host, source_name, source_file, source_generation)` units
    /// the read-index `clickhouse local` fixture drives its deletes from:
    /// every unit the shipped probe returns against
    /// `testdata/reclaim_probe/fixture_read_index.sql`.
    ///
    /// Hand-maintained, and therefore checked against the probe at fixture run
    /// time by the `throwIf` block [`fixture_delete_script`] opens with — the
    /// same defence `reclaim_mcp_open::FIXTURE_UNITS` carries, for the same
    /// reason: a delete script generated from a stale list reports "intact"
    /// for exactly the rows a real run would destroy.
    const FIXTURE_UNITS: &[(&str, &str, &str, u32)] = &[("h", "codex", "/live.jsonl", 5)];

    /// The statements that make `read_index_delete.sql` abort unless the
    /// shipped probe returns exactly [`FIXTURE_UNITS`].
    fn fixture_unit_agreement_sql() -> String {
        let probe =
            read_index_candidate_sql("moraine", FIXTURE_HORIZON_SECONDS, FIXTURE_PROBE_LIMIT);
        let body = probe
            .trim_end()
            .trim_end_matches("FORMAT JSONEachRow")
            .trim_end()
            .to_string();
        let mut keys: Vec<String> = FIXTURE_UNITS
            .iter()
            .map(|(host, name, file, generation)| {
                crate::escape_literal(&format!("{host}/{name}/{file}/{generation}"))
            })
            .collect();
        keys.sort();
        format!(
            "-- Unit-set agreement, checked BEFORE any delete runs. `FIXTURE_UNITS` in\n\
             -- `reclaim_read_index.rs` is hand-maintained; the probe is the truth. If they\n\
             -- disagree this file aborts, having deleted nothing.\n\
             SELECT throwIf(\n  \
               arraySort(groupArray(concat(source_host, '/', source_name, '/', source_file, '/', \
             toString(source_generation))))\n    \
                 != [{}],\n  \
               'FIXTURE_UNITS no longer matches this build''s read_index_generation probe; the \
             DELETE statements below are stale and the census that follows them is meaningless'\n\
             )\nFROM (\n{body}\n)\nFORMAT Null;\n\n",
            keys.join(", "),
        )
    }

    /// The head-joined census a v2 reader's row set reduces to: per table, the
    /// rows whose generation tuple is a current published head. Interleaved
    /// between two deletes of one unit in [`fixture_delete_script`], it is
    /// what proves a reader cannot straddle a partially-reclaimed generation —
    /// the counts are identical before, between, and after the unit's deletes.
    fn reader_visible_census_sql(label: &str) -> String {
        format!(
            "SELECT '{label}' AS at, src, toUInt64(count()) AS reader_visible_rows\n\
             FROM (\n  \
               SELECT 'navigation' AS src, source_host, source_name, source_file, source_generation \
             FROM moraine.mcp_event_navigation\n  \
               UNION ALL SELECT 'locator', source_host, source_name, source_file, source_generation \
             FROM moraine.mcp_event_locator\n  \
               UNION ALL SELECT 'directory', source_host, source_name, source_file, source_generation \
             FROM moraine.mcp_session_directory\n\
             )\n\
             WHERE (source_host, source_name, source_file, source_generation) IN (\n  \
               SELECT source_host, source_name, source_file, source_generation \
             FROM moraine.v_current_published_source_generations\n)\n\
             GROUP BY src\nORDER BY src ASC\nFORMAT TSVWithNames;\n\n"
        )
    }

    /// The fixture's delete script: **this build's** delete statements for
    /// every unit the shipped probe returns, with the reader-visible census
    /// re-run between the first and second delete of the unit (the straddle
    /// proof), then the physical survivor census the fixture header records.
    fn fixture_delete_script() -> String {
        let mut out = String::from(
            "-- GENERATED by `reclaim_read_index::tests::fixture_delete_script`; do not\n\
             -- hand-edit. Regenerate with MORAINE_UPDATE_RECLAIM_GOLDEN=1.\n\
             --\n\
             -- Run after `fixture_read_index.sql` against the same `--path`. Every DELETE is\n\
             -- `emit_delete_statement(read_index_unit_predicates(unit))` verbatim for every\n\
             -- unit the shipped probe returns against that fixture. The reader-visible\n\
             -- census re-runs BETWEEN two deletes of the unit: identical counts at 'before',\n\
             -- 'mid-unit' and 'after' are the proof that a reader pinned to the published\n\
             -- heads cannot observe a partially-reclaimed generation.\n\n",
        );
        out.push_str(&fixture_unit_agreement_sql());
        out.push_str(&reader_visible_census_sql("before"));
        for (host, name, file, generation) in FIXTURE_UNITS {
            let unit = ReclaimUnit {
                reclaim_id: String::new(),
                scope: ReclaimScope::ReadIndexGeneration,
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
            out.push_str(&format!(
                "-- unit: read_index_generation {host}/{name}/{file}/{generation}\n"
            ));
            for (index, (table, predicate)) in
                read_index_unit_predicates(&unit).into_iter().enumerate()
            {
                let statement = emit_delete_statement(
                    "moraine",
                    table,
                    &predicate,
                    &[ReclaimAuthority::DerivedOnly],
                )
                .expect("every fixture unit must be emittable");
                out.push_str(&statement);
                out.push_str(";\n");
                if index == 0 {
                    out.push('\n');
                    out.push_str(&reader_visible_census_sql("mid-unit"));
                }
            }
            out.push('\n');
        }
        out.push_str(&reader_visible_census_sql("after"));
        out.push_str(
            "-- Physical survivor census, per generation tuple. The retired generation\n\
             -- (/live.jsonl gen 5) must be gone from all three tables; every other\n\
             -- generation — the live head 6, both /fresh.jsonl generations (horizon), the\n\
             -- never-published /unpub.jsonl gen 2 and /nohead.jsonl gen 1 (R1) — intact.\n\
             SELECT source_file, toUInt32(source_generation) AS generation,\n       \
               toUInt64(countIf(src = 'navigation')) AS navigation,\n       \
               toUInt64(countIf(src = 'locator')) AS locator,\n       \
               toUInt64(countIf(src = 'directory')) AS directory\n\
             FROM (\n  \
               SELECT source_file, source_generation, 'navigation' AS src \
             FROM moraine.mcp_event_navigation\n  \
               UNION ALL SELECT source_file, source_generation, 'locator' AS src \
             FROM moraine.mcp_event_locator\n  \
               UNION ALL SELECT source_file, source_generation, 'directory' AS src \
             FROM moraine.mcp_session_directory\n\
             )\n\
             GROUP BY source_file, source_generation\n\
             ORDER BY source_file ASC, generation ASC\n\
             FORMAT TSVWithNames\n",
        );
        out
    }

    /// **G-RI-PROBE.** Fails for: a probe that decides retirement from the
    /// data instead of consuming publication truth, that drops a conjunct of
    /// R1–R3, or that scans an index table with `FINAL` (hazard H4).
    /// Denomination: statement text, one assertion per conjunct.
    ///
    /// MUTATION (executed 2026-07-31): replace the `FROM
    /// {database}.v_published_source_generation_history` source with
    /// `{database}.mcp_event_navigation` — deriving the candidate set from the
    /// data it deletes, the shape plan §0 F2 indicts => FAILS on the R1
    /// assertion. **Lower bound, and the one the epic's history names.**
    ///
    /// MUTATION (executed 2026-07-31): drop `h.source_generation !=
    /// head.source_generation` => FAILS on the R2 assertion. **Lower bound:
    /// without it every published generation, including every live head, is a
    /// unit.**
    ///
    /// MUTATION (executed 2026-07-31): change `INNER JOIN heads` to `LEFT
    /// JOIN heads` => FAILS on the join assertion. **Width — and text-only,
    /// stated as such:** in production the two views derive from the one
    /// `published_source_generations` relation, so a file with a history row
    /// always has a current head and the two join kinds cannot differ. The
    /// INNER form is pinned because the fixture stands the views in with two
    /// independent tables, and because a future view change must not be able
    /// to turn NULL-joined rows into candidates silently.
    ///
    /// MUTATION (executed 2026-07-31): drop the `head.published_at < …`
    /// horizon clause => FAILS on the R3 assertion, and against the fixture
    /// the probe additionally returns `/fresh.jsonl` gen 2 — the
    /// freshly-superseded generation a just-pinned reader may still select.
    /// **Lower bound on the horizon.**
    ///
    /// MUTATION (executed 2026-07-31): append ` FINAL` to the
    /// `mcp_event_navigation` rollup branch => FAILS on the H4 assertion.
    /// **Width: the probe's cost, not only its extent.**
    ///
    /// MUTATION (executed 2026-07-31): make `read_index_candidate_sql` return
    /// `String::new()` => FAILS on the R1 assertion, so an empty probe is not
    /// a passing "fix". **Upper bound.**
    #[test]
    fn the_read_index_probe_consumes_publication_truth_and_never_scans_final() {
        let sql = read_index_candidate_sql("moraine", 86_400, 512);
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
        // identity, never `<` on generations.
        assert!(
            !sql.contains("source_generation <") && !sql.contains("max(source_generation)"),
            "retirement must never rest on generation order: {sql}"
        );
        // R3 — the horizon anchors on the superseding head's publication time.
        assert!(
            sql.contains("AND head.published_at < now64(3) - toIntervalSecond(86400)"),
            "R3: the freshly-superseded generation is exactly the one a just-pinned reader \
             can still select: {sql}"
        );
        // H4 — no FINAL on any of the three tables.
        for table in [
            "mcp_event_navigation",
            "mcp_event_locator",
            "mcp_session_directory",
        ] {
            assert!(
                !sql.contains(&format!("{table} FINAL")),
                "H4: `{table}` must not be scanned with FINAL: {sql}"
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
            3,
            "each of the three rollup branches must pre-filter to the retired set: {sql}"
        );
        assert!(sql.contains("LIMIT 512"), "{sql}");
        assert!(
            sql.contains(
                "ORDER BY source_host ASC, source_name ASC, source_file ASC, source_generation ASC"
            ),
            "claim order must be total so a re-drive resumes the same order: {sql}"
        );
    }

    /// **G-RI-EMPTY (R4).** Fails for: a probe that emits a retired generation
    /// the indexes hold no rows for — an empty unit that deletes nothing and
    /// is re-claimed on every tick forever.
    /// Denomination: statement text — the rollup join is `INNER`, the
    /// deliberate inverse of the retired-lineage probe's `LEFT JOIN child`.
    ///
    /// MUTATION (executed 2026-07-31): change `INNER JOIN ri_rollup` to
    /// `LEFT JOIN ri_rollup` => FAILS here, and against the fixture the probe
    /// additionally returns `/empty.jsonl` gen 1 — retired in history, zero
    /// rows anywhere. **Lower bound.**
    #[test]
    fn a_retired_generation_with_no_index_rows_is_not_a_unit() {
        let sql = read_index_candidate_sql("moraine", 86_400, 512);
        assert!(
            sql.contains("INNER JOIN ri_rollup ON ri_rollup.source_host = r.source_host"),
            "R4: a unit must own at least one row, or it settles nothing and never \
             converges: {sql}"
        );
        assert!(
            !sql.contains("LEFT JOIN ri_rollup"),
            "the retired-lineage probe's LEFT JOIN is for a scope whose unit owns a parent \
             row; this scope's units own nothing but what the rollup counts: {sql}"
        );
    }

    /// The config→statement half of the horizon path for this scope, the
    /// mirror of `reclaim_mcp_open`'s. The wiring from
    /// `RetentionConfig::derived_horizon_seconds` into `(executor.probe)` is
    /// gated by `reclaim::tests::the_configured_horizon_reaches_the_probe_statement`,
    /// which reads the statement a run actually issued.
    ///
    /// MUTATION (executed 2026-07-31): hard-code `86400` for
    /// `{horizon_seconds}` in `read_index_candidate_sql` => FAILS here on the
    /// non-stock case. **Width: the parameter travels.**
    #[test]
    fn the_read_index_probe_horizon_is_the_callers_not_a_constant() {
        assert!(read_index_candidate_sql("moraine", 86_400, 64).contains("toIntervalSecond(86400)"));
        assert!(
            read_index_candidate_sql("moraine", 259_200, 64).contains("toIntervalSecond(259200)")
        );
    }

    /// **G-RI-EXTENT.** Fails for: **any** change to a read-index delete
    /// predicate's text, in either direction.
    /// Denomination: the exact predicate string, per table.
    ///
    /// The equality pin is the `reclaim_mcp_open` G-EXTENT shape, carried over
    /// with its rationale: every other guard is a `contains`, and `contains`
    /// cannot see a predicate that *widens* — the widened text still contains
    /// the narrow text, and the widening is what empties a table.
    ///
    /// MUTATION (executed 2026-07-31): append ` OR source_file = {file}` to
    /// the predicate => FAILS here on all three tables. **Upper bound — the
    /// direction nothing else covers.** (Executed against the fixture as
    /// well: the widened predicate takes `/live.jsonl`'s live generation 6 to
    /// zero rows in all three tables — the exact shape of loss this pin
    /// exists to stop.)
    ///
    /// MUTATION (executed 2026-07-31): drop `toUInt32(…)`, leaving a bare
    /// integer => FAILS here (and the emitter still accepts it, so this pin
    /// is the only gate). **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-31): drop `source_name` from the tuple =>
    /// FAILS here **and** the emitter refuses the statement
    /// (`UnboundPredicate` naming `source_name`), which is the two-gate
    /// redundancy `emit_delete_statement`'s third check exists to provide.
    /// **Width.**
    ///
    /// MUTATION (executed 2026-07-31): predicate the locator delete on
    /// `(session_id, source_generation)` — hazard H2's shape => FAILS here
    /// and at the emitter (`UnboundPredicate`: a `Generation` delete must
    /// name all four key columns; `session_id` satisfies none of them).
    #[test]
    fn the_read_index_delete_predicates_are_pinned_exactly_in_both_directions() {
        let predicates = read_index_unit_predicates(&unit());
        let rendered: Vec<(&'static str, String)> = predicates
            .iter()
            .map(|(table, predicate)| (table.name(), predicate.clone()))
            .collect();
        let expected = "(source_host, source_name, source_file, source_generation) IN \
                        (('h\\'1', 'codex', '/a\\'b.jsonl', toUInt32(7)))"
            .to_string();
        assert_eq!(
            rendered,
            vec![
                ("mcp_event_navigation", expected.clone()),
                ("mcp_event_locator", expected.clone()),
                ("mcp_session_directory", expected),
            ],
            "a read-index delete predicate changed. This is the statement that can destroy \
             live index rows; re-derive its extent and update this pin deliberately, never to \
             make a build green."
        );
        for (table, predicate) in &predicates {
            let statement = emit_delete_statement(
                "moraine",
                *table,
                predicate,
                &[ReclaimAuthority::DerivedOnly],
            )
            .unwrap_or_else(|refusal| panic!("`{}` must be emittable: {refusal}", table.name()));
            assert!(
                statement.starts_with("DELETE FROM `moraine`."),
                "{statement}"
            );
            assert!(
                !predicate.contains("session_id"),
                "H2: `session_id` is not key-determined on the locator and must appear in no \
                 read-index predicate: {predicate}"
            );
        }
    }

    /// The registered executor runs **this** probe and **these** predicates,
    /// and its unit grain is the source generation.
    ///
    /// MUTATION (executed 2026-07-31): point `ReadIndexGeneration`'s `probe`
    /// at `reclaim_mcp_open::orphan_candidate_sql` => FAILS on the probe
    /// equality. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-31): map `ReadIndexGeneration` to
    /// `ReclaimUnitGrain::SessionCandidateGeneration` in
    /// `ReclaimScope::unit_grain` => FAILS on the grain assertion here, and
    /// `reclaim::tests::a_candidate_row_must_carry_its_scopes_unit_key`
    /// FAILS on the validation side: the probe's rows carry no session key,
    /// so every candidate would be refused at claim time. **Width: the grain
    /// decides both the ledger identity and the validation.**
    #[test]
    fn the_read_index_executor_runs_its_own_probe_at_source_generation_grain() {
        let executor = executor_for(ReclaimScope::ReadIndexGeneration)
            .expect("WI-07 registers the read-index executor");
        assert_eq!(
            (executor.probe)("moraine", 86_400, 8),
            read_index_candidate_sql("moraine", 86_400, 8)
        );
        assert_eq!(
            (executor.predicates)(&unit()),
            read_index_unit_predicates(&unit())
        );
        assert_eq!(
            ReclaimScope::ReadIndexGeneration.unit_grain(),
            ReclaimUnitGrain::SourceGeneration,
            "a read-index unit is one source generation; a session-grained identity would \
             collapse every unit into ledger row ':0'"
        );
    }

    /// The `clickhouse local` fixture's files hold **this build's**
    /// statements, verbatim — a text comparison, not an execution, exactly as
    /// `reclaim_mcp_open::tests::the_fixture_files_are_this_builds_statements_verbatim`
    /// and for the same reason: the fixture is executed by hand
    /// (`testdata/reclaim_probe/fixture_read_index.sql` carries the recipe),
    /// and a hand-run fixture that silently measures the previous build is
    /// worse than no fixture. The `reclaim-generation` live gate remains
    /// **unwritten** in this branch.
    ///
    /// MUTATION (executed 2026-07-31): add ` AND 1` to the probe's retirement
    /// `WHERE` => FAILS here on `read_index.sql`.
    ///
    /// MUTATION (executed 2026-07-31): remove the entry from `FIXTURE_UNITS`
    /// => FAILS here on `read_index_delete.sql`, because the generated file
    /// embeds the probe text and the expected unit array in a `throwIf`.
    ///
    /// Regenerate after an intentional edit with
    /// `MORAINE_UPDATE_RECLAIM_GOLDEN=1 cargo test -p moraine-clickhouse
    /// the_read_index_fixture_files_are_this_builds_statements_verbatim`, then
    /// re-run the fixture and update its expected-output header comment.
    #[test]
    fn the_read_index_fixture_files_are_this_builds_statements_verbatim() {
        let cases = [
            (
                include_str!("testdata/reclaim_probe/expected/read_index.sql"),
                read_index_candidate_sql("moraine", FIXTURE_HORIZON_SECONDS, FIXTURE_PROBE_LIMIT),
                "read_index.sql",
            ),
            (
                include_str!("testdata/reclaim_probe/expected/read_index_delete.sql"),
                fixture_delete_script(),
                "read_index_delete.sql",
            ),
        ];
        let update = std::env::var_os("MORAINE_UPDATE_RECLAIM_GOLDEN").is_some();
        for (golden, probe, name) in cases {
            let expected = probe.replace("FORMAT JSONEachRow", "FORMAT TSVWithNames");
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
