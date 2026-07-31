//! Issue #603 WI-05 and WI-05b — the two `mcp_open_*` candidate probes and
//! the delete predicates they authorize.
//!
//! The probe and the delete live in one module on purpose. The existing
//! reclaimer's failure mode is not that its delete was wrong; it is that its
//! delete order made its *probe* unable to see what the delete had left
//! behind, and the two were read a screen apart. Here a scope's candidate set,
//! its bounding, and the exact predicate each of its tables receives are one
//! reviewable unit.
//!
//! Two scopes, and they are disjoint by construction:
//!
//! * [`ReclaimScope::McpOpenOrphan`] — child rows with **no header at all**.
//!   Nothing can authorize them, on either engine.
//! * [`ReclaimScope::McpOpenRetiredLineage`] — snapshots with a complete and
//!   valid header in a lineage the session has since left. This is plan §0 F4:
//!   the existing reclaimer joins within one `required_heads_fingerprint` and
//!   deliberately excludes everything else, so a replacement replay's
//!   predecessor is unreachable forever.
//!
//! Neither probe is a `FINAL` scan of a child table (hazard H4). Both aggregate
//! the children by their sort-key prefix and anti-join a table three orders of
//! magnitude smaller.
//!
//! ## Why "no header" is safe, in one paragraph
//!
//! `get_mcp_event` collects candidate rows by `event_uid`, then for each one
//! loads the session's *header* and requires `header.slot == row.slot &&
//! header.generation == row.generation` — the candidate walk in
//! `moraine_conversations`'s `clickhouse_repo::open::ClickHouseConversationRepository::get_mcp_event_impl`.
//! Named rather than cited by line: that region already differs by a line
//! between this branch and `main`. `load_projected_session` selects from
//! `mcp_open_publication_headers`, never from the child tables — its `sessions`
//! binding is that relation, and neither `mcp_open_events` nor `mcp_open_turns`
//! appears anywhere in the query it builds. A `(session, candidate
//! generation)` with no header row therefore matches no reader's pin, at any
//! captured publication revision, on either engine. That is the whole safety
//! argument for [`ReclaimScope::McpOpenOrphan`], and it does not depend on the
//! #598 cutover.
//!
//! ## Why a retired lineage is safe, in rather more than one paragraph
//!
//! See [`retired_lineage_candidate_sql`]. The short version: the reclaimer
//! never decides that a lineage *should* be retired. It reads three decisions
//! somebody else already recorded — the published head set, the session
//! pointer, and the monotone dirty revision the reader itself compares for
//! equality — and collects only what all three have already left behind.

use crate::escape_identifier;
use crate::reclaim::{ReclaimTable, ReclaimUnit};

/// `BackfillPhase::Complete`, as the literal the probe compares against.
///
/// Defined *as* the projection's own value rather than restating `4`, on the
/// `RECLAIM_DELETE_CHUNK` precedent: the definition is the coupling, so the two
/// cannot drift and no test has to notice that they have. A renumbering still
/// has to be looked at, because
/// `the_orphan_probe_protects_an_in_flight_prepare_by_pair_not_by_session`
/// asserts the emitted statement text and would fail on the new number.
pub(crate) const BACKFILL_PHASE_COMPLETE: u8 =
    crate::mcp_open_projection::BACKFILL_PHASE_COMPLETE_VALUE;

/// Row shape both probes return.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub(crate) struct McpOpenCandidateRow {
    pub(crate) session_id: String,
    pub(crate) candidate_generation: u64,
    pub(crate) event_rows: u64,
    pub(crate) turn_rows: u64,
    pub(crate) header_rows: u64,
}

impl McpOpenCandidateRow {
    pub(crate) fn estimated_rows(&self) -> u64 {
        self.event_rows
            .saturating_add(self.turn_rows)
            .saturating_add(self.header_rows)
    }
}

/// The two child tables, aggregated to `(session_id, candidate_generation)`
/// with their newest write, as one relation.
///
/// `mcp_open_events` is `ORDER BY (event_uid, slot, source_host,
/// candidate_generation)` and `mcp_open_turns` is `ORDER BY (session_id, slot,
/// turn_seq, candidate_generation)`, so neither aggregation is key-aligned —
/// but both are plain `GROUP BY`s over two columns with no `FINAL`, no
/// `payload_json`, and no `text_content`, which is what hazard H4 is about.
/// The two statements that OOM'd at the 4 GiB per-query ceiling were a
/// `search_documents FINAL` reading `payload_json` and an `mcp_open_events
/// FINAL` grouped by uid; this reads two `UInt64`-ish columns and a timestamp.
///
/// `projected_at` is never written explicitly by any insert — every
/// `INSERT INTO … mcp_open_events`/`mcp_open_turns` column list in the tree
/// omits it (`mcp_open_projection::single_projected_events_sql` and
/// `::projected_events_insert_sql`, and the turns pair), so it
/// takes its `DEFAULT now64(3)` and is exactly "when was this row written".
/// That is what makes it a usable horizon clock; an `event_time`-derived
/// column would not be.
fn child_rollup_sql(database: &str) -> String {
    format!(
        "SELECT session_id, candidate_generation, toUInt64(count()) AS event_rows,\n         \
           toUInt64(0) AS turn_rows, max(projected_at) AS newest\n    \
         FROM {database}.mcp_open_events\n    \
         GROUP BY session_id, candidate_generation\n    \
         UNION ALL\n    \
         SELECT session_id, candidate_generation, toUInt64(0) AS event_rows,\n         \
           toUInt64(count()) AS turn_rows, max(projected_at) AS newest\n    \
         FROM {database}.mcp_open_turns\n    \
         GROUP BY session_id, candidate_generation"
    )
}

/// Every `(session_id, generation)` a header exists for, **without `FINAL`**.
///
/// Deliberately non-`FINAL`, and the direction matters. This set is used only
/// to *exclude* candidates, so reading a stale duplicate can only shrink the
/// orphan set, never grow it — fail-closed. And it cannot lose a pair:
/// `mcp_open_publication_headers` is `ReplacingMergeTree(header_revision)
/// ORDER BY (session_id, candidate_publication_id)`, a merge collapses rows
/// that agree on that key, and `(session_id, candidate_publication_id) →
/// generation` is functional. Measured read-only on the reference host
/// 2026-07-28: 0 keys carry more than one generation, and raw, `FINAL`,
/// `uniqExact((session_id, generation))` and
/// `uniqExact((session_id, candidate_publication_id))` all agree — 4 835 rows
/// in that sample.
///
/// **Every header-row count in this module is that one sample, quoted as
/// "~4 800", and is approximate by the hour**: publication writes headers
/// continuously, so three docstrings written on one afternoon otherwise cite
/// three different exact numbers and each reads like an independent
/// measurement. What is load-bearing is the *agreement* between the four
/// counts, which is scale-free.
///
/// Tombstoned headers are **included**. A tombstone is still a header, so its
/// generation is not an orphan; and the v1 reader selects the newest
/// authorized header and returns `None` when it is a tombstone
/// (`load_projected_session`'s `if row.tombstone != 0 { return Ok(None) }` arm,
/// taken after the authorized header row is selected), which makes a removed
/// session read as absent. Orphan collection must not touch it.
fn header_pairs_sql(database: &str) -> String {
    format!("SELECT session_id, generation FROM {database}.mcp_open_publication_headers")
}

/// Candidate orphan units: `(session_id, candidate_generation)` present in a
/// child table with no header row (plan §3.3, WI-05).
///
/// Three independent bounds, and each one is a measurement rather than a
/// precaution:
///
/// 1. **No header.** The anti-join above. 735 pairs on the reference host
///    (2026-07-28), 11 174 380 `mcp_open_events` rows and 112 976
///    `mcp_open_turns` rows across 72 sessions.
/// 2. **Safety horizon.** `prepare` writes children first and the header last
///    (`sql/027:1-6`), so a pair being prepared right now looks exactly like an
///    orphan. `max(projected_at) < now() - horizon` excludes it. At the stock
///    24 h horizon this holds back 35 of the 729 event-side pairs.
/// 3. **In-flight backfill plan.** `mcp_open_backfill_plans` is one row per
///    session naming the candidate generation being prepared; `phase = 4` is
///    `Complete`. An incomplete plan protects its pair.
///
/// **Bound 3 is pair-scoped, not session-scoped, and that is a correction to
/// plan §3.3.** The plan says "exclude any session named by a live
/// `mcp_open_backfill_plans` row" and records "0 of the 695 orphan pairs are
/// covered by a plan, so this exclusion costs nothing today". Measured again
/// on 2026-07-28: *every* session has a plan row (4 082 of them, 4 081 at
/// `Complete`), so the session-scoped reading of "covered by a plan" excludes
/// **735 of 735** pairs and the work item collects nothing. Restricting it to
/// incomplete plans is necessary but not sufficient: one session held an
/// incomplete plan at that moment and it owned **568 of the 735 pairs** — a
/// session-scoped exclusion would forfeit 77% of the target set to a plan row
/// whose own generation had not yet written a single child row, and would
/// forfeit it *permanently* if that plan ever wedged. The pair-scoped form
/// protects exactly the prepare in flight, which is the race the exclusion was
/// written for.
///
/// `ORDER BY` is total over the returned key, so the claim order a run takes is
/// the claim order the re-drive after a crash resumes.
pub(crate) fn orphan_candidate_sql(database: &str, horizon_seconds: u64, limit: usize) -> String {
    let database = escape_identifier(database);
    format!(
        "SELECT child.session_id AS session_id,\n       \
           toUInt64(child.candidate_generation) AS candidate_generation,\n       \
           toUInt64(sum(child.event_rows)) AS event_rows,\n       \
           toUInt64(sum(child.turn_rows)) AS turn_rows,\n       \
           toUInt64(0) AS header_rows\n\
         FROM (\n    {}\n  ) AS child\n\
         WHERE (child.session_id, child.candidate_generation) NOT IN (\n    {}\n  )\n  \
           AND (child.session_id, child.candidate_generation) NOT IN (\n    \
             SELECT session_id, candidate_generation\n    \
             FROM {database}.mcp_open_backfill_plans FINAL\n    \
             WHERE phase != {BACKFILL_PHASE_COMPLETE}\n  )\n\
         GROUP BY child.session_id, child.candidate_generation\n\
         HAVING max(child.newest) < now64(3) - toIntervalSecond({horizon_seconds})\n\
         ORDER BY session_id ASC, candidate_generation ASC\n\
         LIMIT {limit}\n\
         FORMAT JSONEachRow",
        child_rollup_sql(&database),
        header_pairs_sql(&database),
    )
}

/// Candidate retired-lineage units: complete, valid headers in a lineage the
/// session has left, plus their children (plan §0 F4).
///
/// # The predicate, and why each conjunct is there
///
/// For a session `S`, let `pointer(S)` be `mcp_open_sessions FINAL`'s row —
/// the per-session publication pointer — and `dirty(S)` be
/// `mcp_open_dirty_sessions FINAL`'s revision. A header `h` of `S` is
/// retirable when **all five** hold:
///
/// * **P1 — a newer lineage is _published_.** `live` is the header at
///   `pointer(S).generation`, it is not a tombstone, it declares a non-empty
///   required head set, and **every** head it requires is in
///   `v_current_published_source_generations`. Publication truth, read, never
///   decided (INV-1).
/// * **P2 — …and _live_.** `live.dirty_revision = dirty(S)`. This is the
///   reader's own authorization clause (the `s.dirty_revision = (SELECT …
///   max(dirty.dirty_revision) …)` conjunct `load_projected_session` appends to
///   every header read): a header
///   whose dirty revision is not the session's current one is selected by no
///   request. Requiring it of `live` is what makes "live" mean *a reader would
///   return this row*, not merely *a pointer names it*.
/// * **P3 — `h` is a different lineage.** `h.required_heads_fingerprint !=
///   live.required_heads_fingerprint`. This scope is disjoint from
///   `reclaim_superseded_mcp_open_snapshots`, which handles supersession
///   **within** a fingerprint. Nothing is claimed twice and neither path has to
///   reason about the other.
/// * **P4 — `h` is permanently unselectable.** `h.dirty_revision < dirty(S)`.
///   **This is the conjunct that replaces generation order, and it is the whole
///   rollback-safety argument.** `dirty_revision` is a snowflake from
///   `mv_mcp_open_dirty_sessions_from_events` (`sql/027:135-146`) into a
///   `ReplacingMergeTree(dirty_revision)` keyed on `session_id`, so `dirty(S)`
///   is monotone. The reader compares it for **equality**, and that comparison
///   is evaluated against current state in every request — it is not part of
///   the as-of publication snapshot. So a header below the current dirty
///   revision is unselectable by every request at every captured publication
///   revision, now and forever, whatever happens to publication truth
///   afterwards. A rollback re-publishing the old source generation does not
///   move `dirty(S)` back, because a rollback inserts no events.
/// * **P5 — horizon.** `live.prepared_at` is older than the bucket-3 safety
///   horizon, and the children of `h` were last written before it too.
/// * **P6 — the unit key is entirely retirable.** The two conjuncts above are
///   properties of one header *row*; the unit this probe emits is a
///   `(session_id, generation)` **key**, and [`mcp_open_unit_predicates`]
///   deletes every row at that key. Those are not the same extent, because
///   `mcp_open_publication_headers` is `ORDER BY (session_id,
///   candidate_publication_id)` — `generation` is not its primary key, and
///   nothing in the schema stops two headers of one session sharing a
///   generation. P6 closes the gap in two parts: the key may not be the
///   generation the session pointer names (`h.generation !=
///   l.live_generation`), and **every** header row at the key must satisfy
///   P3+P4 (`countIf(…) = count()`). Without it the probe emitted a key whose
///   rows it had not all examined; see
///   `a_generation_the_live_pointer_names_is_never_a_retirement_unit`.
///
/// `h.header_revision < live.header_revision` is asserted as well. It is
/// implied by P4 in every case the host exhibits, and it is stated anyway
/// because it is the ordering the reader's `ORDER BY header_revision DESC LIMIT
/// 1` actually uses; a future header written with a *lower* revision than the
/// pointer's would otherwise be retirable while still outranking nothing.
///
/// # Why P6 is not redundant with the projector's own invariant
///
/// Both header inserts write `header_revision = {generation}`
/// (`mcp_open_projection::insert_mcp_open_tombstone` and
/// `::projected_publication_header_sql`) and `generation` comes from
/// `generateSnowflakeID()`, so on projector-written data `(session_id,
/// generation)` *is* unique — measured read-only on the reference host
/// 2026-07-28 over its ~4 800 header rows (see [`header_pairs_sql`] for the
/// sample and why the count is quoted approximately): the row count, the
/// distinct `(session_id, generation)` count and the distinct `(session_id,
/// candidate_publication_id)` count are all equal, and 0 rows have
/// `header_revision != generation`. That invariant is what made the pre-P6
/// probe safe, and **nothing stated it**. It is not a property of this
/// predicate, it is a property of a writer three modules away that no test
/// here reads; a header written by any future path that does not tie
/// `header_revision` to `generation` re-opens it. Measured against a
/// `clickhouse local` fixture with two headers at one generation, the pre-P6
/// probe emitted the **live** generation as a unit and the three shipped
/// deletes left `headers 0, turns 0, events 0` with `mcp_open_sessions` still
/// pointing at it — the live snapshot destroyed and the pointer dangling. P6
/// makes the unit key self-describing so the predicate no longer borrows its
/// safety from a writer's coincidence.
///
/// # What this does not rest on
///
/// It does not rest on `h.generation < live.generation`. Plan §0 F4 is explicit
/// that generation order alone is what the existing cross-lineage exclusion
/// protects against, and the measurement says why. Re-derived read-only on the
/// reference host 2026-07-28, driving the shipped `live` CTE and dropping only
/// P4: **135** headers are admitted, of which **43 are still
/// reader-selectable** — their `dirty_revision` equals `dirty(S)`, so
/// `load_projected_session` can still return them. P4 removes exactly those 43
/// and leaves 92 (135 − 43 = 92).
///
/// An earlier revision of this comment claimed 494 admitted and 162
/// reader-selectable "leaving 92", which never closed arithmetically
/// (494 − 162 = 332) and did not reproduce under any formulation: the
/// *unconstrained* naive predicate — different fingerprint, lower generation,
/// no P1/P2/P5 on `live` — selects 378 today, of which 43 are
/// reader-selectable. 43 is the figure that is stable across every
/// formulation tried, and it is the one the conclusion actually needs.
///
/// # Tombstoned headers: eligible here, never on the `live` side
///
/// Plan §0 F4 asks for a deliberate decision, and this is it, in both
/// directions.
///
/// *Never `live`.* `publish_legacy_candidate_heads_sql`
/// filters `tombstone = 0`, so a tombstone can
/// never become the session pointer; P1 excludes it for free, and the
/// `tombstone = 0` clause on `live` states it rather than relying on that.
///
/// *Eligible as `h`.* A tombstone header is load-bearing: `load_projected_session`
/// applies **no** tombstone filter, takes the newest authorized header, and
/// returns `Ok(None)` when that header is a tombstone
/// (`ORDER BY header_revision DESC LIMIT 1` under the authorization clause,
/// then the `row.tombstone != 0` arm). Deleting the tombstone a reader is selecting
/// would resurrect a removed session — which is why leaving them out looks
/// safe. But P4 means a retirable `h` is one **no** reader can select, and P1
/// means the row a reader *does* select is a different, non-tombstone header
/// that is currently published. A tombstone reaching this predicate is
/// therefore one the suppression path can no longer reach. Excluding it instead
/// would carry one permanently unreachable row per retired
/// `(session, candidate publication)` forever, which is the same
/// unbounded-in-replays growth this scope exists to bound, at one row per unit.
/// The host has one tombstone today and it is not retirable.
pub(crate) fn retired_lineage_candidate_sql(
    database: &str,
    horizon_seconds: u64,
    limit: usize,
) -> String {
    let database = escape_identifier(database);
    format!(
        "WITH\n  \
         headers AS (\n    \
           SELECT session_id, generation, header_revision, tombstone,\n           \
             required_heads_fingerprint, dirty_revision, prepared_at, required_source_heads\n    \
           FROM {database}.mcp_open_publication_headers FINAL\n  ),\n  \
         required AS (\n    \
           SELECT session_id, generation, head.1 AS source_host, head.2 AS source_name,\n           \
             head.3 AS source_file, head.4 AS source_generation\n    \
           FROM headers ARRAY JOIN required_source_heads AS head\n  ),\n  \
         unpublished AS (\n    \
           SELECT DISTINCT session_id, generation\n    \
           FROM required\n    \
           WHERE (source_host, source_name, source_file, source_generation) NOT IN (\n      \
             SELECT source_host, source_name, source_file, source_generation\n      \
             FROM {database}.v_current_published_source_generations\n    )\n  ),\n  \
         live AS (\n    \
           SELECT h.session_id AS session_id, h.header_revision AS live_header_revision,\n           \
             h.generation AS live_generation,\n           \
             h.required_heads_fingerprint AS live_fingerprint, h.prepared_at AS live_prepared_at,\n           \
             d.dirty_revision AS current_dirty\n    \
           FROM headers AS h\n    \
           INNER JOIN (\n      \
             SELECT session_id, generation FROM {database}.mcp_open_sessions FINAL\n    \
           ) AS p ON p.session_id = h.session_id AND p.generation = h.generation\n    \
           INNER JOIN (\n      \
             SELECT session_id, dirty_revision FROM {database}.mcp_open_dirty_sessions FINAL\n    \
           ) AS d ON d.session_id = h.session_id\n    \
           WHERE h.tombstone = 0\n      \
             AND length(h.required_source_heads) > 0\n      \
             AND h.dirty_revision = d.dirty_revision\n      \
             AND (h.session_id, h.generation) NOT IN (SELECT session_id, generation FROM unpublished)\n      \
             AND h.prepared_at < now64(3) - toIntervalSecond({horizon_seconds})\n  ),\n  \
         retired AS (\n    \
           SELECT h.session_id AS session_id, h.generation AS generation\n    \
           FROM headers AS h\n    \
           INNER JOIN live AS l ON l.session_id = h.session_id\n    \
           WHERE h.generation != l.live_generation\n    \
           GROUP BY h.session_id, h.generation\n    \
           HAVING countIf(h.header_revision < l.live_header_revision\n             \
             AND h.dirty_revision < l.current_dirty\n             \
             AND h.required_heads_fingerprint != l.live_fingerprint) = count()\n  ),\n  \
         child AS (\n    {}\n  )\n\
         SELECT r.session_id AS session_id,\n       \
           toUInt64(r.generation) AS candidate_generation,\n       \
           toUInt64(sum(ifNull(child.event_rows, 0))) AS event_rows,\n       \
           toUInt64(sum(ifNull(child.turn_rows, 0))) AS turn_rows,\n       \
           toUInt64(1) AS header_rows\n\
         FROM retired AS r\n\
         LEFT JOIN child ON child.session_id = r.session_id\n  \
           AND child.candidate_generation = r.generation\n\
         GROUP BY r.session_id, r.generation\n\
         HAVING max(ifNull(child.newest, toDateTime64(0, 3))) < now64(3) - toIntervalSecond({horizon_seconds})\n\
         ORDER BY session_id ASC, candidate_generation ASC\n\
         LIMIT {limit}\n\
         FORMAT JSONEachRow",
        child_rollup_sql(&database),
    )
}

/// The delete predicates for one `mcp_open` unit, **children first, parent
/// last** — the inverse of `reclaim_superseded_mcp_open_snapshots`.
///
/// One unit is one `(session_id, candidate_generation)`, so each statement's
/// extent is one key pair and `RECLAIM_DELETE_CHUNK` never binds: no unit can
/// emit more than one statement per table, which is the assumption
/// `UNIT_STMT_MAX_TABLES` rests on (plan §7.4's last residual).
///
/// The header predicate names `generation`, the two children name
/// `candidate_generation`. They are different columns on different tables and
/// the emitter's third check now tells them apart.
pub(crate) fn mcp_open_unit_predicates(unit: &ReclaimUnit) -> Vec<(ReclaimTable, String)> {
    let session = crate::escape_literal(&unit.session_id);
    let generation = unit.candidate_generation;
    unit.scope
        .tables()
        .iter()
        .map(|table| {
            let predicate = match table {
                ReclaimTable::McpOpenPublicationHeaders => {
                    format!("(session_id, generation) IN (({session}, toUInt64({generation})))")
                }
                _ => format!(
                    "(session_id, candidate_generation) IN (({session}, toUInt64({generation})))"
                ),
            };
            (*table, predicate)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reclaim::{emit_delete_statement, ReclaimAuthority, ReclaimPhase, ReclaimScope};

    fn unit(scope: ReclaimScope) -> ReclaimUnit {
        ReclaimUnit {
            reclaim_id: "r-1".to_string(),
            scope,
            source_host: String::new(),
            source_name: String::new(),
            source_file: String::new(),
            source_generation: 0,
            session_id: "s'1".to_string(),
            candidate_generation: 7_487_744_971_366_645_760,
            phase: ReclaimPhase::Claimed,
            estimated_rows: 10,
            estimated_bytes: 20,
            unsettled_seconds: 0,
        }
    }

    /// The horizon and page the fixture's three generated files are built at.
    /// One pair, so `orphan.sql`, `retired.sql` and the agreement check inside
    /// `delete.sql` cannot be measuring three different probes.
    const FIXTURE_HORIZON_SECONDS: u64 = 86_400;
    const FIXTURE_PROBE_LIMIT: usize = 512;

    /// The `(session_id, candidate_generation)` units the `clickhouse local`
    /// fixture drives its deletes from: every unit the two shipped probes
    /// return against `testdata/reclaim_probe/fixture.sql`.
    ///
    /// Hand-maintained, and therefore **checked against the probes at fixture
    /// run time** by [`fixture_unit_agreement_sql`] rather than trusted. See
    /// that function for why a hand-maintained list here is a hole and not
    /// merely a chore.
    const FIXTURE_UNITS: &[(ReclaimScope, &str, u64)] = &[
        (ReclaimScope::McpOpenOrphan, "o-collect", 100),
        (ReclaimScope::McpOpenOrphan, "o-plandone", 105),
        (ReclaimScope::McpOpenOrphan, "o-planned", 104),
        (ReclaimScope::McpOpenOrphan, "o-turnsonly", 107),
        (ReclaimScope::McpOpenRetiredLineage, "L-RETIRE", 50),
        (ReclaimScope::McpOpenRetiredLineage, "L-RETIRE", 53),
    ];

    /// The fixture's delete script: **this build's** delete statements for
    /// every unit the fixture's probes return, followed by the survivor census
    /// the fixture asserts against.
    ///
    /// This exists because the delete is the only statement in issue #603 that
    /// can destroy live data, and until now nothing executed one. The probes
    /// had a semantic gate (`clickhouse local`) and three text gates; the
    /// deletes had text gates only, and text gates are asymmetric — they kill
    /// a predicate that *narrows* (drop a key column and the emitter refuses)
    /// but not one that *widens*. Appending ` OR session_id = {session}` to
    /// the child arm of [`mcp_open_unit_predicates`] left the entire workspace
    /// green while taking `L-RETIRE` from four generations to zero rows.
    ///
    /// Generating the script from the shipped predicates rather than writing
    /// it out means the fixture cannot drift from the code, and the golden
    /// comparison in
    /// `the_fixture_files_are_this_builds_statements_verbatim` turns any edit
    /// to a delete predicate — in either direction — into a failing named
    /// test.
    ///
    /// The unit *list* those predicates are instantiated over is a separate
    /// question, and [`fixture_unit_agreement_sql`] is its answer.
    /// The statements that make `delete.sql` abort unless the shipped probes
    /// return exactly [`FIXTURE_UNITS`].
    ///
    /// **This is what makes the fixture non-blind in the *add* direction.**
    /// Every `DELETE` in `delete.sql` is generated by instantiating the shipped
    /// predicates over a hand-maintained list, so a probe that starts returning
    /// a unit the list omits produces a file that simply never deletes it — and
    /// the survivor census then prints that session as intact, for a build that
    /// destroys it. That is not hypothetical: under the pre-P6 probe the orphan
    /// scope returned `A1B 60`, `delete.sql` had no statement for it, and the
    /// census reported A1B "fully intact" while the shipped code would have
    /// emptied the live snapshot the session pointer names.
    ///
    /// Running the probes themselves is the only way to close that, and it
    /// needs a server, which is why this lives in the fixture rather than in
    /// `cargo test`. Both sides are sorted so the check is about the *set* of
    /// units, not about the probes' `ORDER BY` surviving a subquery.
    fn fixture_unit_agreement_sql() -> String {
        let mut out = String::from(
            "-- Unit-set agreement, checked BEFORE any delete runs. `FIXTURE_UNITS` in\n\
             -- `reclaim_mcp_open.rs` is hand-maintained; the probes are the truth. If they\n\
             -- disagree this file aborts, because a file whose DELETEs are generated from a\n\
             -- stale list reports \"intact\" for exactly the rows a real run would destroy.\n\n",
        );
        for (scope, probe) in [
            (
                ReclaimScope::McpOpenOrphan,
                orphan_candidate_sql("moraine", FIXTURE_HORIZON_SECONDS, FIXTURE_PROBE_LIMIT),
            ),
            (
                ReclaimScope::McpOpenRetiredLineage,
                retired_lineage_candidate_sql(
                    "moraine",
                    FIXTURE_HORIZON_SECONDS,
                    FIXTURE_PROBE_LIMIT,
                ),
            ),
        ] {
            let body = probe
                .trim_end()
                .trim_end_matches("FORMAT JSONEachRow")
                .trim_end()
                .to_string();
            let mut keys: Vec<String> = FIXTURE_UNITS
                .iter()
                .filter(|(unit_scope, _, _)| *unit_scope == scope)
                .map(|(_, session_id, candidate_generation)| {
                    crate::escape_literal(&format!("{session_id}/{candidate_generation}"))
                })
                .collect();
            keys.sort();
            out.push_str(&format!(
                "-- {scope}\n\
                 SELECT throwIf(\n  \
                   arraySort(groupArray(concat(session_id, '/', toString(candidate_generation))))\n    \
                     != [{}],\n  \
                   'FIXTURE_UNITS no longer matches this build''s {scope} probe; the DELETE \
                 statements below are stale and the census that follows them is meaningless'\n\
                 )\nFROM (\n{body}\n)\nFORMAT Null;\n\n",
                keys.join(", "),
            ));
        }
        out
    }

    fn fixture_delete_script() -> String {
        let mut out = String::from(
            "-- GENERATED by `reclaim_mcp_open::tests::fixture_delete_script`; do not hand-edit.\n\
             -- Regenerate with MORAINE_UPDATE_RECLAIM_GOLDEN=1.\n\
             --\n\
             -- Run after `fixture.sql` against the same `--path`. Every statement below is\n\
             -- `emit_delete_statement(mcp_open_unit_predicates(unit))` verbatim, for every unit\n\
             -- the two shipped probes return against that fixture.\n\n",
        );
        out.push_str(&fixture_unit_agreement_sql());
        for (scope, session_id, candidate_generation) in FIXTURE_UNITS {
            let unit = ReclaimUnit {
                reclaim_id: String::new(),
                scope: *scope,
                source_host: String::new(),
                source_name: String::new(),
                source_file: String::new(),
                source_generation: 0,
                session_id: (*session_id).to_string(),
                candidate_generation: *candidate_generation,
                phase: ReclaimPhase::Claimed,
                estimated_rows: 0,
                estimated_bytes: 0,
                unsettled_seconds: 0,
            };
            out.push_str(&format!(
                "-- unit: {scope} {session_id}/{candidate_generation}\n"
            ));
            for (table, predicate) in mcp_open_unit_predicates(&unit) {
                let statement = emit_delete_statement(
                    "moraine",
                    table,
                    &predicate,
                    &[ReclaimAuthority::DerivedOnly],
                )
                .expect("every fixture unit must be emittable");
                out.push_str(&statement);
                out.push_str(";\n");
            }
            out.push('\n');
        }
        out.push_str(
            "-- Survivor census. The two rows that matter are `A1B` — two headers at one\n\
             -- generation, the live one — and `L-RETIRE` generation 60, the live pointer\n\
             -- target. Both must be untouched; see the expected block in fixture.sql.\n\
             SELECT session_id,\n       \
               toUInt64(countIf(src = 'header')) AS headers,\n       \
               toUInt64(countIf(src = 'turn')) AS turns,\n       \
               toUInt64(countIf(src = 'event')) AS events\n\
             FROM (\n  \
               SELECT session_id, generation AS gen, 'header' AS src \
             FROM moraine.mcp_open_publication_headers\n  \
               UNION ALL SELECT session_id, candidate_generation AS gen, 'turn' AS src \
             FROM moraine.mcp_open_turns\n  \
               UNION ALL SELECT session_id, candidate_generation AS gen, 'event' AS src \
             FROM moraine.mcp_open_events\n\
             )\n\
             GROUP BY session_id\n\
             ORDER BY session_id ASC\n\
             FORMAT TSVWithNames\n",
        );
        out
    }

    /// **G-ORPHAN-PROBE.** Fails for: an orphan probe that is not an anti-join
    /// against headers, or that scans a child table with `FINAL`.
    /// Denomination: statement text.
    ///
    /// MUTATION (executed 2026-07-28): make [`header_pairs_sql`] return a
    /// relation that matches nothing — `SELECT '' AS session_id, toUInt64(0) AS
    /// generation WHERE 0` — so the anti-join excludes no candidate => FAILS on
    /// the header-exclusion assertion. (Deleting the clause outright is not the
    /// mutation, because it leaves an unused `format!` argument and does not
    /// compile; this is the same defect with the same effect on the target
    /// set.) **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): change `child_rollup_sql`'s first
    /// `FROM {database}.mcp_open_events` to `… mcp_open_events FINAL` => FAILS
    /// on the H4 assertion. **Width: the probe's cost, not only its extent.**
    ///
    /// MUTATION (executed 2026-07-28): make `orphan_candidate_sql` return
    /// `String::new()` => FAILS on the header-exclusion assertion, so an empty
    /// probe is not a passing "fix". **Upper bound.**
    #[test]
    fn the_orphan_probe_is_a_header_anti_join_with_no_final_child_scan() {
        let sql = orphan_candidate_sql("moraine", 86_400, 512);
        assert!(
            sql.contains(
                "WHERE (child.session_id, child.candidate_generation) NOT IN (\n    SELECT \
                 session_id, generation FROM `moraine`.mcp_open_publication_headers\n  )"
            ),
            "{sql}"
        );
        assert!(
            !sql.contains("mcp_open_events FINAL") && !sql.contains("mcp_open_turns FINAL"),
            "H4: a child-table FINAL scan is what OOM'd the host's 4 GiB ceiling: {sql}"
        );
        assert!(
            sql.contains("toIntervalSecond(86400)"),
            "the horizon must reach the statement: {sql}"
        );
        assert!(sql.contains("LIMIT 512"), "{sql}");
        assert!(
            sql.contains("ORDER BY session_id ASC, candidate_generation ASC"),
            "claim order must be total so a re-drive resumes the same order: {sql}"
        );
    }

    /// **G-ORPHAN-PREPARE.** Fails for: an orphan probe that can claim a
    /// `(session, generation)` whose prepare is in flight.
    /// Denomination: statement text, both exclusions present.
    ///
    /// MUTATION (executed 2026-07-28): drop the `HAVING max(child.newest) < …`
    /// clause => FAILS on the horizon assertion. **Lower bound.**
    ///
    /// MUTATION (executed 2026-07-28): widen the plan exclusion from the pair
    /// to the session — delete `AND child.candidate_generation =
    /// p.candidate_generation` in spirit by removing `candidate_generation`
    /// from the `NOT IN` tuple => FAILS on the pair-scope assertion. **Upper
    /// bound, and it is the bound that matters: the session-scoped form is the
    /// one plan §3.3 asks for, and on 2026-07-28 it excluded 735 of 735
    /// pairs.**
    ///
    /// MUTATION (executed 2026-07-28): change `phase != 4` to `phase != 0` =>
    /// FAILS on the completed-phase assertion. **Width: which plans protect.**
    #[test]
    fn the_orphan_probe_protects_an_in_flight_prepare_by_pair_not_by_session() {
        let sql = orphan_candidate_sql("moraine", 3_600, 64);
        assert!(
            sql.contains("HAVING max(child.newest) < now64(3) - toIntervalSecond(3600)"),
            "the header-last prepare window needs a horizon: {sql}"
        );
        assert!(
            sql.contains(
                "AND (child.session_id, child.candidate_generation) NOT IN (\n    SELECT \
                 session_id, candidate_generation\n    FROM \
                 `moraine`.mcp_open_backfill_plans FINAL\n    WHERE phase != 4\n  )"
            ),
            "the plan exclusion must be pair-scoped and restricted to incomplete plans: {sql}"
        );
    }

    /// The plan exclusion's `phase != N` literal is the projection's own
    /// `BackfillPhase::Complete`, not a restatement of `4`.
    ///
    /// `mcp_open_projection.rs` names this test as the coupling that keeps the
    /// two from drifting. It did not exist — the citation was written for a
    /// test that was never added, so the constant's only guard was that some
    /// *other* test happened to assert `phase != 4` as a literal, which is
    /// exactly the restatement the constant exists to avoid.
    ///
    /// MUTATION (executed 2026-07-28): define `BACKFILL_PHASE_COMPLETE` as a
    /// bare `4` instead of deriving it from
    /// `mcp_open_projection::BACKFILL_PHASE_COMPLETE_VALUE` => still passes
    /// (the values agree today), so this test is a *drift* guard, not a value
    /// guard. **Renumbering `BackfillPhase::Complete` to 5 => FAILS here and
    /// at `the_orphan_probe_protects_an_in_flight_prepare_by_pair_not_by_session`,
    /// which is the pair the citation promised.**
    #[test]
    fn the_backfill_exclusion_uses_the_projections_own_complete_phase() {
        assert_eq!(
            BACKFILL_PHASE_COMPLETE,
            crate::mcp_open_projection::BACKFILL_PHASE_COMPLETE_VALUE,
            "the probe's literal must be the projection's variant, not a copy of its number"
        );
        let sql = orphan_candidate_sql("moraine", 3_600, 64);
        assert!(
            sql.contains(&format!("WHERE phase != {BACKFILL_PHASE_COMPLETE}")),
            "the derived constant must be what reaches the statement: {sql}"
        );
    }

    /// The config→seconds half of the horizon path: the stock
    /// `RetentionConfig` denominates 24 h as 86 400 s, and a non-default
    /// horizon travels into the probe text rather than being ignored.
    ///
    /// This is deliberately only *half* the gate. The other half — that
    /// `ClickHouseClient::reclaim_candidates` passes
    /// `retention.derived_horizon_seconds()` into `(executor.probe)` rather
    /// than a constant — cannot be seen from here, because every assertion in
    /// this module hands the probe a literal and then looks for that literal.
    /// That is what let replacing the call with `0` stay green. The wiring
    /// itself is gated by
    /// `reclaim::tests::the_configured_horizon_reaches_the_probe_statement`,
    /// which reads the statement a run actually issued.
    ///
    /// MUTATION (executed 2026-07-28): change `derived_horizon_seconds` to
    /// return hours rather than seconds => FAILS here on the stock case
    /// (86 400 vs 24). **Width: the unit.**
    #[test]
    fn the_configured_derived_horizon_is_denominated_in_seconds() {
        let stock = moraine_config::RetentionConfig::default();
        let seconds = stock.derived_horizon_seconds().max(0.0) as u64;
        assert_eq!(
            seconds, 86_400,
            "stock horizon is 24 h expressed in seconds"
        );
        for probe in [
            orphan_candidate_sql("moraine", seconds, 64),
            retired_lineage_candidate_sql("moraine", seconds, 64),
        ] {
            assert!(
                probe.contains(&format!("toIntervalSecond({seconds})")),
                "the configured horizon must be the one the probe compares against: {probe}"
            );
        }
        // And a non-stock horizon must travel too, or the assertion above
        // would pass for a probe that hard-coded the default.
        let custom = moraine_config::RetentionConfig {
            derived_horizon_hours: 72.0,
            ..stock
        };
        let custom_seconds = custom.derived_horizon_seconds().max(0.0) as u64;
        assert_eq!(custom_seconds, 259_200);
        assert!(orphan_candidate_sql("moraine", custom_seconds, 64)
            .contains("toIntervalSecond(259200)"));
    }

    /// **G-LINEAGE-PRED.** Fails for: a retirement predicate that rests on
    /// generation order, or that omits either half of "published and live".
    /// Denomination: statement text, one assertion per conjunct.
    ///
    /// MUTATION (executed 2026-07-28): replace `h.dirty_revision <
    /// l.current_dirty` with `h.generation < l.live_header_revision` — i.e.
    /// retire on ordering alone — => FAILS on the P4 assertion. **Lower bound,
    /// and the one plan §0 F4 names: run against the `clickhouse local`
    /// fixture that substitution admits `L-RETIRE/51`, and re-derived
    /// read-only on the reference host 2026-07-28 the ordering-only predicate
    /// admits 43 headers a reader can still select** (135 admitted without P4,
    /// 43 of them reader-selectable, leaving 92 — see
    /// [`retired_lineage_candidate_sql`] for why the previously documented
    /// "494 / 162" did not reproduce and never closed arithmetically).
    ///
    /// MUTATION (executed 2026-07-28): drop the `unpublished` `NOT IN` clause
    /// from `live` => FAILS on the P1 assertion. **Width: "published".**
    ///
    /// MUTATION (executed 2026-07-28): drop `AND h.dirty_revision =
    /// d.dirty_revision` from `live` => FAILS on the P2 assertion. **Width:
    /// "and live".**
    ///
    /// MUTATION (executed 2026-07-28): drop the fingerprint inequality =>
    /// FAILS on the P3 assertion, which is what keeps this scope disjoint from
    /// `reclaim_superseded_mcp_open_snapshots`. **Width: disjointness.**
    #[test]
    fn the_retirement_predicate_never_rests_on_generation_order() {
        let sql = retired_lineage_candidate_sql("moraine", 86_400, 128);
        // P1 — published.
        assert!(
            sql.contains(
                "AND (h.session_id, h.generation) NOT IN (SELECT session_id, generation FROM \
                 unpublished)"
            ),
            "P1: the live lineage must require only currently published heads: {sql}"
        );
        assert!(
            sql.contains("FROM `moraine`.v_current_published_source_generations"),
            "P1 reads publication truth, it does not recompute it: {sql}"
        );
        // P2 — and live.
        assert!(
            sql.contains("AND h.dirty_revision = d.dirty_revision"),
            "P2: `live` must be the row a reader would actually return: {sql}"
        );
        assert!(
            sql.contains("FROM `moraine`.mcp_open_sessions FINAL"),
            "P2: `live` must be the published session pointer: {sql}"
        );
        // P3 — a different lineage, so this scope stays disjoint from the
        // within-lineage reclaimer.
        assert!(
            sql.contains("AND h.required_heads_fingerprint != l.live_fingerprint"),
            "P3: {sql}"
        );
        // P4 — the retired header is permanently unselectable.
        assert!(
            sql.contains("AND h.dirty_revision < l.current_dirty"),
            "P4: retirement must rest on the monotone dirty revision, not on \
             generation order: {sql}"
        );
        // NOT an assertion that generation order is absent. It cannot be:
        // both header inserts write `header_revision = {generation}`
        // (`insert_mcp_open_tombstone`, `projected_publication_header_sql`),
        // so the retained
        // `h.header_revision < l.live_header_revision` **is** generation order
        // under another name on every row the projector writes — measured
        // read-only on the reference host 2026-07-28, 0 of its ~4 800 header
        // rows have `header_revision != generation`. The previous revision asserted
        // `!sql.contains("h.generation < ")` under the banner "generation
        // order must not appear at all", which could not fail for the hazard
        // it named: the hazard is *resting on* generation order, and the
        // predicate rests on it either way. What P4 actually buys is that
        // retirement additionally requires the monotone dirty revision to have
        // moved, which ordering alone does not imply — so the honest assertion
        // is that P4 is present (above) and that nothing has re-introduced a
        // bare generation comparison as a *substitute* for it.
        assert!(
            sql.contains("AND h.dirty_revision < l.current_dirty")
                && !sql.contains("h.generation < l.live_generation"),
            "P4 must be the retirement conjunct, not a generation comparison standing in for \
             it: {sql}"
        );
        // P5 — horizon, on both the live header and the retired children.
        assert!(
            sql.contains("AND h.prepared_at < now64(3) - toIntervalSecond(86400)"),
            "P5: the newer lineage must have been live longer than the horizon: {sql}"
        );
        assert!(
            sql.contains(
                "HAVING max(ifNull(child.newest, toDateTime64(0, 3))) < now64(3) - \
                 toIntervalSecond(86400)"
            ),
            "P5: a re-prepare under an old lineage must not be collected: {sql}"
        );
    }

    /// **G-UNITKEY (P6).** Fails for: a retirement probe that can emit a
    /// `(session_id, generation)` unit whose rows it has not all examined —
    /// the defect that let one stale header authorize the deletion of a live
    /// snapshot.
    /// Denomination: statement text, one assertion per half of P6.
    ///
    /// `mcp_open_publication_headers` is `ORDER BY (session_id,
    /// candidate_publication_id)`. The unit key is `(session_id, generation)`
    /// and [`mcp_open_unit_predicates`] deletes **every** row at that key, so
    /// a probe that qualifies one row and emits the key has an extent wider
    /// than its evidence.
    ///
    /// **The two halves are mutually redundant on the fixture, and the
    /// mutation ledger says so rather than implying otherwise.** Measured
    /// 2026-07-28 by regenerating the golden probe from a mutated build and
    /// running it against `testdata/reclaim_probe/fixture.sql`:
    ///
    /// | mutation | fixture result |
    /// |---|---|
    /// | drop P6a (`WHERE h.generation != l.live_generation`) | `A1B` **still excluded** — P6b covers it |
    /// | replace P6b's `= count()` with `> 0` | `A1B` **still excluded** — P6a covers it |
    /// | drop **both** | `A1B 60` **re-admitted**, and the three shipped deletes then leave `headers 0, turns 0, events 0` with `mcp_open_sessions` still naming generation 60 |
    ///
    /// That is by construction, not luck: the header that became `live` is
    /// itself a row of the group at the pointer generation, and
    /// `h.header_revision < l.live_header_revision` is false when compared
    /// against itself, so P6b alone always fails the key. P6a states the same
    /// exclusion directly instead of deriving it, because "the live
    /// generation is never a unit" is the property a reader needs to be able
    /// to check without reconstructing that argument.
    ///
    /// So each half has a **named-test** lower bound (below) and the pair has
    /// the semantic one. Neither half alone has a semantic lower bound on this
    /// fixture, and claiming one would have been false.
    ///
    /// MUTATION (executed 2026-07-28): drop `WHERE h.generation !=
    /// l.live_generation` => FAILS on the live-generation assertion here and
    /// on the golden in `the_fixture_files_are_this_builds_statements_verbatim`.
    /// **Lower bound, text.**
    ///
    /// MUTATION (executed 2026-07-28): replace `countIf(…) = count()` with
    /// `countIf(…) > 0` — one retirable row authorizing the whole key =>
    /// FAILS on the all-rows assertion. **Width: evidence per key, not per
    /// row.**
    ///
    /// MUTATION (executed 2026-07-28): drop **both** => FAILS on both
    /// assertions, and re-admits `A1B/60` against the fixture. **Lower bound,
    /// semantic — this is the pre-fix predicate, and it destroys a live
    /// snapshot.**
    ///
    /// MUTATION (executed 2026-07-28): drop the `GROUP BY h.session_id,
    /// h.generation` and revert to the row-wise `WHERE` => does not compile
    /// (`countIf` outside an aggregate context); the equivalent defect is the
    /// `> 0` mutation above.
    #[test]
    fn a_generation_the_live_pointer_names_is_never_a_retirement_unit() {
        let sql = retired_lineage_candidate_sql("moraine", 86_400, 128);
        assert!(
            sql.contains("h.generation AS live_generation"),
            "P6 needs the pointer's generation on the `live` side: {sql}"
        );
        assert!(
            sql.contains("WHERE h.generation != l.live_generation"),
            "P6a: the generation the session pointer names must never be a unit key: {sql}"
        );
        assert!(
            sql.contains("GROUP BY h.session_id, h.generation")
                && sql.contains("HAVING countIf(h.header_revision < l.live_header_revision")
                && sql
                    .contains("AND h.required_heads_fingerprint != l.live_fingerprint) = count()"),
            "P6b: every header row at the unit key must satisfy the retirement conjuncts, \
             because the delete removes every row at that key: {sql}"
        );
    }

    /// A header with no children still retires. Fails for: an `INNER JOIN` to
    /// the child rollup, which would leave every childless retired header —
    /// every tombstone, exactly the rows §0 F4 asks about — permanently
    /// uncollectable while claiming to bound growth.
    ///
    /// MUTATION (executed 2026-07-28): change `LEFT JOIN child` to
    /// `INNER JOIN child` => FAILS here. **Lower bound.**
    #[test]
    fn a_retired_header_with_no_child_rows_is_still_a_unit() {
        let sql = retired_lineage_candidate_sql("moraine", 86_400, 128);
        assert!(
            sql.contains("LEFT JOIN child ON child.session_id = r.session_id"),
            "{sql}"
        );
        assert!(
            sql.contains("toUInt64(sum(ifNull(child.event_rows, 0))) AS event_rows"),
            "a childless unit must count zero rows, not vanish: {sql}"
        );
        assert!(
            sql.contains("max(ifNull(child.newest, toDateTime64(0, 3)))"),
            "a childless unit must pass the child horizon, not fail it on NULL: {sql}"
        );
    }

    /// **G-HEADERCOL.** Fails for: a header delete predicated on
    /// `candidate_generation`, a column `mcp_open_publication_headers` does not
    /// have.
    /// Denomination: the emitted statement text, per table.
    ///
    /// MUTATION (executed 2026-07-28): map `McpOpenPublicationHeaders` back to
    /// `ReclaimPredicate::SessionGeneration` => FAILS here: the emitter refuses
    /// the header statement for a missing `candidate_generation`. **Lower
    /// bound.**
    ///
    /// MUTATION (executed 2026-07-28): give the header the child predicate in
    /// `mcp_open_unit_predicates` while leaving the shape correct => FAILS on
    /// the emitter refusal, because `predicate_names_column` matches on
    /// identifier boundaries. Under the previous `contains` form this mutation
    /// was **GREEN**, and the statement it produced named no column of the
    /// table it deleted from. **Width: the substring/identifier distinction is
    /// load-bearing, not cosmetic.**
    #[test]
    fn the_header_delete_names_generation_and_the_children_name_candidate_generation() {
        for scope in [
            ReclaimScope::McpOpenOrphan,
            ReclaimScope::McpOpenRetiredLineage,
        ] {
            let predicates = mcp_open_unit_predicates(&unit(scope));
            assert_eq!(
                predicates.len(),
                scope.tables().len(),
                "every table of `{scope}` needs a predicate"
            );
            let last = predicates.last().expect("non-empty");
            assert_eq!(
                last.0,
                ReclaimTable::McpOpenPublicationHeaders,
                "`{scope}` must delete the header last"
            );
            for (table, predicate) in &predicates {
                let statement = emit_delete_statement(
                    "moraine",
                    *table,
                    predicate,
                    &[ReclaimAuthority::DerivedOnly],
                )
                .unwrap_or_else(|refusal| {
                    panic!("`{}` must be emittable: {refusal}", table.name())
                });
                assert!(
                    statement.starts_with("DELETE FROM `moraine`."),
                    "{statement}"
                );
                assert!(
                    statement.contains("toUInt64(7487744971366645760)"),
                    "the unit's key must reach the statement: {statement}"
                );
                assert!(
                    statement.contains("'s\\'1'"),
                    "a session id containing a quote must be escaped: {statement}"
                );
            }
            let (_, header_predicate) = predicates
                .iter()
                .find(|(table, _)| *table == ReclaimTable::McpOpenPublicationHeaders)
                .expect("header predicate");
            assert!(
                header_predicate.contains("(session_id, generation) IN")
                    && !header_predicate.contains("candidate_generation"),
                "the header table has no `candidate_generation` column: {header_predicate}"
            );
            for (table, predicate) in &predicates {
                if *table == ReclaimTable::McpOpenPublicationHeaders {
                    continue;
                }
                assert!(
                    predicate.contains("(session_id, candidate_generation) IN"),
                    "`{}` is keyed on `candidate_generation`: {predicate}",
                    table.name()
                );
            }
        }
    }

    /// **G-EXTENT.** Fails for: **any** change to a delete predicate's text,
    /// in either direction.
    /// Denomination: the exact predicate string, per table.
    ///
    /// Every other guard on these two strings is a `contains`, and `contains`
    /// is one-sided: it kills a predicate that *narrows* — drop a key column
    /// and `emit_delete_statement`'s third check refuses the statement — but
    /// it cannot see a predicate that *widens*, because the widened text still
    /// contains the narrow text. Appending ` OR session_id = {session}` to the
    /// child arm of [`mcp_open_unit_predicates`] left `cargo test --workspace`
    /// entirely green while taking the fixture's `L-RETIRE` from four
    /// generations — including the live pointer's — to zero rows. Three things
    /// independently failed to see it: the emitter's third check is documented
    /// name-presence only, the in-process mock parses just the first quoted
    /// literal and first `toUInt64(…)` so the extra disjunct never reached its
    /// row bookkeeping, and the `clickhouse local` fixture executed no DELETE
    /// at all.
    ///
    /// An equality pin is the right shape precisely because these two strings
    /// are the only thing standing between a planner bug and unrecoverable
    /// data loss: there is no such thing as an incidental edit to them, so
    /// "this test failed" is always the correct outcome of changing one.
    ///
    /// MUTATION (executed 2026-07-28): append ` OR session_id = {session}` to
    /// the child arm => FAILS here on both child tables. **Upper bound — the
    /// direction nothing else covered.**
    ///
    /// MUTATION (executed 2026-07-28): drop `toUInt64(…)` from the child arm,
    /// leaving a bare integer => FAILS here. **Lower bound (and the emitter
    /// still accepts it, so this is the only gate).**
    ///
    /// MUTATION (executed 2026-07-28): swap the header arm's `generation` for
    /// `candidate_generation` => FAILS here *and* at
    /// `the_header_delete_names_generation_and_the_children_name_candidate_generation`.
    #[test]
    fn the_delete_predicates_are_pinned_exactly_in_both_directions() {
        for scope in [
            ReclaimScope::McpOpenOrphan,
            ReclaimScope::McpOpenRetiredLineage,
        ] {
            let predicates = mcp_open_unit_predicates(&unit(scope));
            let rendered: Vec<(&'static str, String)> = predicates
                .iter()
                .map(|(table, predicate)| (table.name(), predicate.clone()))
                .collect();
            assert_eq!(
                rendered,
                vec![
                    (
                        "mcp_open_events",
                        "(session_id, candidate_generation) IN (('s\\'1', \
                         toUInt64(7487744971366645760)))"
                            .to_string()
                    ),
                    (
                        "mcp_open_turns",
                        "(session_id, candidate_generation) IN (('s\\'1', \
                         toUInt64(7487744971366645760)))"
                            .to_string()
                    ),
                    (
                        "mcp_open_publication_headers",
                        "(session_id, generation) IN (('s\\'1', toUInt64(7487744971366645760)))"
                            .to_string()
                    ),
                ],
                "`{scope}`: a delete predicate changed. This is the statement that can destroy \
                 live data; re-derive its extent and update this pin deliberately, never to make \
                 a build green."
            );
        }
    }

    /// Every registered executor's probe names the scope's own tables and
    /// nothing else. Fails for: a scope wired to another scope's probe — the
    /// two `mcp_open` executors differ only in one function pointer, and a
    /// copy-paste that pointed both at the orphan probe would claim retired
    /// lineages under the orphan predicate and delete nothing, silently.
    ///
    /// MUTATION (executed 2026-07-28): point `McpOpenRetiredLineage`'s `probe`
    /// at `orphan_candidate_sql` => FAILS here. **Lower bound.**
    #[test]
    fn each_registered_executor_runs_its_own_probe() {
        let orphan = crate::reclaim::executor_for(ReclaimScope::McpOpenOrphan)
            .expect("WI-05 registers the orphan executor");
        let retired = crate::reclaim::executor_for(ReclaimScope::McpOpenRetiredLineage)
            .expect("WI-05b registers the retired-lineage executor");
        let orphan_sql = (orphan.probe)("moraine", 86_400, 8);
        let retired_sql = (retired.probe)("moraine", 86_400, 8);
        assert_eq!(orphan_sql, orphan_candidate_sql("moraine", 86_400, 8));
        assert_eq!(
            retired_sql,
            retired_lineage_candidate_sql("moraine", 86_400, 8)
        );
        assert_ne!(orphan_sql, retired_sql);
        assert!(
            !orphan_sql.contains("required_heads_fingerprint"),
            "the orphan probe must not depend on lineage at all: {orphan_sql}"
        );
        assert!(
            retired_sql.contains("required_heads_fingerprint"),
            "the retirement probe is about lineage: {retired_sql}"
        );
        for scope in [
            ReclaimScope::ReadIndexGeneration,
            ReclaimScope::CanonicalGeneration,
        ] {
            assert!(
                crate::reclaim::executor_for(scope).is_none(),
                "`{scope}` has no probe in this build and must not be registered"
            );
        }
    }

    /// The `clickhouse local` fixture's files hold **this build's statements,
    /// verbatim** — a text comparison, not an execution.
    ///
    /// **Named for what it does.** It was
    /// `the_clickhouse_local_fixture_runs_this_builds_probes`, and it runs
    /// nothing: it compares generated strings against checked-in goldens.
    /// Nothing in `cargo test` starts a ClickHouse, so no automated gate in
    /// this repository evaluates a #603 probe or a #603 delete against data.
    /// The fixture is executed by hand (`testdata/reclaim_probe/fixture.sql`
    /// carries the recipe) and this test's only job is to keep the files it
    /// executes from drifting behind the code — which is a real job, because a
    /// hand-run fixture that silently measures the previous release is worse
    /// than no fixture.
    ///
    /// The semantic gates that *would* run automatically — plan §7.2's
    /// `reclaim-orphan` and `reclaim-generation` live gates — are **unwritten**
    /// in this branch, not merely unrun: `grep` finds no such test anywhere in
    /// the tree, and Docker is unavailable on the reference host, so a sandbox
    /// could not have run them either. Saying "not run" would imply something
    /// exists to run.
    ///
    /// Following the `testdata/projector_golden` precedent. The Rust tests
    /// above assert each conjunct's text; the fixture is what asserts what the
    /// conjuncts *do*, and it can only do that if it is running this build's
    /// SQL.
    ///
    /// MUTATION (executed 2026-07-28): add ` AND 1` to the orphan probe's
    /// `WHERE` => FAILS here.
    ///
    /// MUTATION (executed 2026-07-28): remove one entry from `FIXTURE_UNITS`
    /// => FAILS here, because `delete.sql` now embeds the probes' own text and
    /// the expected unit array in a `throwIf`. **Width: the generated file
    /// covers the unit list as well as the predicates.**
    ///
    /// Regenerate after an intentional probe edit with
    /// `MORAINE_UPDATE_RECLAIM_GOLDEN=1 cargo test -p moraine-clickhouse
    /// the_fixture_files_are_this_builds_statements_verbatim`, then re-run the
    /// fixture and update its expected-output header comment. The previous
    /// revision told the reader to regenerate and shipped no way to do it, so
    /// the golden was maintained by hand — which is the same staleness the
    /// test exists to catch, moved one step back.
    #[test]
    fn the_fixture_files_are_this_builds_statements_verbatim() {
        let cases = [
            (
                include_str!("testdata/reclaim_probe/expected/orphan.sql"),
                orphan_candidate_sql("moraine", FIXTURE_HORIZON_SECONDS, FIXTURE_PROBE_LIMIT),
                "orphan.sql",
            ),
            (
                include_str!("testdata/reclaim_probe/expected/retired.sql"),
                retired_lineage_candidate_sql(
                    "moraine",
                    FIXTURE_HORIZON_SECONDS,
                    FIXTURE_PROBE_LIMIT,
                ),
                "retired.sql",
            ),
            (
                include_str!("testdata/reclaim_probe/expected/delete.sql"),
                fixture_delete_script(),
                "delete.sql",
            ),
        ];
        let update = std::env::var_os("MORAINE_UPDATE_RECLAIM_GOLDEN").is_some();
        for (golden, probe, name) in cases {
            // The fixture reads TSV so the expected rows are diffable; that is
            // the only edit the golden carries.
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
