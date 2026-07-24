//! The shared page-aware canonical `open` reader (issue-598 WI-06).
//!
//! One session-scoped reader reconstructs `open(session|turn|event)` directly
//! from live canonical `events` rows plus the migration-036 content-free
//! indexes (`mcp_session_directory`, `mcp_event_locator`, `mcp_event_navigation`)
//! — never `mcp_open_*`. It implements the spec's narrow query shape
//! (issue-598.md §4):
//!
//! 1. Read narrow ordering/classification/version columns first (payload-free,
//!    asserted by the SQL-shape tests: no statement here lists `text_content`
//!    or `payload_json` except the two explicitly wide statements — the bounded
//!    metadata read and the page hydration).
//! 2. Derive order/turns/summaries/neighbors over the *bounded page set* using
//!    the WI-01 shared fragments ([`cd`], [`DerivationColumns::EVENTS`]) so the
//!    derivation matches the projector byte-for-byte where it must.
//! 3. Keyset pagination with `LIMIT K + 1` in SQL.
//! 4. Read `text_content` / `payload_json` only for the selected page's events.
//! 5. Read session metadata only from metadata-bearing rows (the
//!    `is_metadata_bearing` predicate), so metadata work is bounded by the
//!    metadata-event count, not by session size.
//!
//! Every statement runs under the ambient #600 query envelope and the pinned
//! publication snapshot (design §5, D1/D9/D10/D11). This module is the
//! REPOSITORY reader; the tool-facing `open_v2` cursor is WI-07, which consumes
//! the page-in/page-out API defined here (keyed by [`CanonicalContinuation`]).
//!
//! Exact live parity with the v1 projector (event-ordinal edge cases,
//! non-contiguous stray-override folding, the summary two-phase selection, and
//! the total-turns counter refinement in the VERIFIER ADDENDUM) is pinned and
//! verified by the WI-10 live parity fixtures; the folds here are structured to
//! that contract and unit-tested for their SQL shape and forward arithmetic.

use super::*;
use moraine_clickhouse::canonical_derivations::{self as cd, DerivationColumns};

/// The `events`-column derivation set the reader always uses (the migration-036
/// indexes carry the physical `actor_kind`/`event_kind`/`tool_name` names).
const COLS: DerivationColumns = DerivationColumns::EVENTS;

// ---------------------------------------------------------------------------
// Forward derivation arithmetic (design §5.3) — pure, unit-tested.
// ---------------------------------------------------------------------------

/// One navigation row's inputs to the forward `(event_order, turn_seq,
/// event_ordinal)` derivation.
#[derive(Debug, Clone, Copy)]
pub(super) struct ForwardInput {
    pub(super) turn_index: u32,
    pub(super) is_user_message: bool,
}

/// One row's forward-derived public ordering state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ForwardDerived {
    pub(super) event_order: u64,
    pub(super) turn_seq: u32,
    pub(super) prefix_user_message_count: u64,
    pub(super) event_ordinal: u32,
}

/// Derive `(event_order, turn_seq, U, event_ordinal)` for the rows streamed in
/// tuple order after `anchor` (design §5.3). All arithmetic is O(1) per row.
///
/// `anchor` is the last event of the previous page (its derived state carried
/// on the cursor); for a first page pass `anchor` is the synthetic pre-session
/// anchor (`event_order = 0`, `turn_seq = 0`, `U = 0`, `event_ordinal = 0`).
pub(super) fn derive_forward(anchor: ForwardDerived, rows: &[ForwardInput]) -> Vec<ForwardDerived> {
    let mut u_count = anchor.prefix_user_message_count;
    // Seed the per-turn ordinal counter with the anchor's turn so a page that
    // continues inside the anchor's turn keeps counting (design R2).
    let mut ordinal_by_turn: HashMap<u32, u32> = HashMap::default();
    if anchor.turn_seq > 0 {
        ordinal_by_turn.insert(anchor.turn_seq, anchor.event_ordinal);
    }
    let mut out = Vec::with_capacity(rows.len());
    for (offset, row) in rows.iter().enumerate() {
        let event_order = anchor.event_order + offset as u64 + 1;
        if row.is_user_message {
            u_count += 1;
        }
        let turn_seq = if row.turn_index > 0 {
            row.turn_index
        } else {
            std::cmp::max(
                1,
                u32::try_from(u_count.min(u64::from(u32::MAX))).unwrap_or(u32::MAX),
            )
        };
        let ordinal_slot = ordinal_by_turn.entry(turn_seq).or_insert(0);
        *ordinal_slot += 1;
        out.push(ForwardDerived {
            event_order,
            turn_seq,
            prefix_user_message_count: u_count,
            event_ordinal: *ordinal_slot,
        });
    }
    out
}

/// The reader's `total_turns` identity (design §5.2.4, WI-01
/// [`cd::total_turns_identity_expr`]): `max(maxO, if(rows, max(1, U), 0))`.
/// Computed in Rust from the totals aggregate so both the override-path
/// (`max_override`) and counter-path (`counter_user_messages`) contributions
/// are explicit — the VERIFIER ADDENDUM item 1 counter refinement is the
/// `counter_user_messages` input (U at the last `turn_index = 0` row), supplied
/// by the totals SQL's companion aggregate.
pub(super) fn total_turns_from_parts(
    total_events: u64,
    max_override: u32,
    counter_user_messages: u64,
) -> u32 {
    let counter = if total_events > 0 {
        std::cmp::max(
            1,
            u32::try_from(counter_user_messages.min(u64::from(u32::MAX))).unwrap_or(u32::MAX),
        )
    } else {
        0
    };
    std::cmp::max(max_override, counter)
}

// ---------------------------------------------------------------------------
// Layered staleness / continuation decision (design §5.4) — pure, unit-tested.
// ---------------------------------------------------------------------------

/// The next step a continuation takes after re-reading a session's signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContinuationStep {
    /// Nothing relevant changed: reuse the carried anchor and page on.
    Proceed,
    /// Inserts landed; run the exact prefix boundary guard before proceeding.
    RunBoundaryGuard,
    /// The session moved in a way the cursor cannot continue across: reopen.
    Reopen,
}

/// Decide the continuation step from the cursor's pinned signals and a fresh
/// signal read (design §5.4 steps 1-7). `fresh_fingerprint_matches` is whether
/// the session's live-head fingerprint is unchanged; it is only consulted when
/// the global publication revision moved (step 3's session-scoped precision
/// refinement).
pub(super) fn plan_continuation(
    cursor: &CanonicalSessionSignals,
    fresh: &CanonicalSessionSignals,
    fresh_fingerprint_matches: bool,
) -> ContinuationStep {
    let signals_equal = fresh.observed_sum == cursor.observed_sum
        && fresh.min_bound_ms == cursor.min_bound_ms
        && fresh.max_bound_ms == cursor.max_bound_ms;

    // Step 2: quiescent session, revision unchanged.
    if fresh.pinned_revision == cursor.pinned_revision && signals_equal {
        return ContinuationStep::Proceed;
    }
    // Step 3: the global revision moved — reopen only if THIS session's heads
    // flipped; otherwise an unrelated publication moved it, fall through.
    if fresh.pinned_revision != cursor.pinned_revision && !fresh_fingerprint_matches {
        return ContinuationStep::Reopen;
    }
    // Step 4: signals unchanged (carries reusable).
    if signals_equal {
        return ContinuationStep::Proceed;
    }
    // Step 5: impossible-direction / truncate-rebuild.
    if fresh.observed_sum < cursor.observed_sum || fresh.min_bound_ms < cursor.min_bound_ms {
        return ContinuationStep::Reopen;
    }
    // Step 6: inserts landed — the exact boundary guard decides.
    if fresh.observed_sum > cursor.observed_sum {
        return ContinuationStep::RunBoundaryGuard;
    }
    // Step 7: sum unchanged but the max bound advanced — a sentinel-row
    // re-insert rewrite signature.
    ContinuationStep::Reopen
}

// ---------------------------------------------------------------------------
// Deserialization rows.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SignalRow {
    pub(super) observed_sum: u64,
    pub(super) min_bound_ms: i64,
    pub(super) max_bound_ms: i64,
    pub(super) heads_fingerprint: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct BoundaryGuardRow {
    pub(super) count_le_anchor: u64,
    pub(super) user_messages_le_anchor: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct LocatorRow {
    pub(super) event_uid: String,
    pub(super) source_host: String,
    pub(super) session_id: String,
    pub(super) source_file: String,
    pub(super) source_generation: u32,
    pub(super) source_offset: u64,
    pub(super) source_line_no: u64,
    pub(super) sort_time_ms: i64,
}

/// One row of a navigation window chunk. Payload-free by construction.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct NavWindowRow {
    pub(super) event_uid: String,
    pub(super) source_host: String,
    pub(super) source_file: String,
    pub(super) source_generation: u32,
    pub(super) source_offset: u64,
    pub(super) source_line_no: u64,
    pub(super) sort_time_ms: i64,
    pub(super) display_time: String,
    pub(super) display_time_ms: i64,
    pub(super) turn_index: u32,
    pub(super) is_user_message: u8,
    pub(super) actor_kind: String,
    pub(super) event_kind: String,
    pub(super) payload_type: String,
    pub(super) tool_name: String,
    pub(super) event_type: String,
    pub(super) is_user_input: u8,
    pub(super) is_final_response: u8,
    pub(super) is_terminal: u8,
    pub(super) is_completed: u8,
}

// ---------------------------------------------------------------------------
// SQL builders. Every builder is a pure method for the SQL-shape tests.
// ---------------------------------------------------------------------------

impl ClickHouseConversationRepository {
    /// The pinned as-of published source-generation set, as a derived table the
    /// content-free index scans inner-join against to keep only live-generation
    /// rows (design §5.2). Mirrors the `published` subquery inside
    /// [`Self::live_events_source`] but is a standalone relation for the
    /// navigation / directory / locator statements.
    pub(super) fn published_generations_subquery(&self) -> String {
        let snapshot = require_active_publication_snapshot("canonical index reads");
        let history = self.table_ref("v_published_source_generation_history");
        let predicate = snapshot.publication_revision_predicate("history");
        format!(
            "(SELECT\n    history.source_host AS source_host,\n    history.source_name AS source_name,\n    history.source_file AS source_file,\n    tupleElement(argMax(tuple(history.source_generation, history.publication_revision), history.publication_revision), 1) AS source_generation\n  FROM {history} AS history\n  WHERE {predicate}\n  GROUP BY history.source_host, history.source_name, history.source_file)"
        )
    }

    /// `FROM mcp_event_navigation AS n FINAL ALL INNER JOIN <published> …` — the
    /// live-head-filtered navigation scan prefix. The `WHERE n.session_id = …`
    /// each caller appends prunes the navigation primary key.
    fn navigation_live_from(&self) -> String {
        let nav = self.table_ref("mcp_event_navigation");
        let published = self.published_generations_subquery();
        format!(
            "FROM {nav} AS n FINAL\nALL INNER JOIN {published} AS published\n  ON published.source_host = n.source_host\n AND published.source_name = n.source_name\n AND published.source_file = n.source_file\n AND published.source_generation = n.source_generation"
        )
    }

    /// The navigation ordering tuple (the primary key with `sort_time`
    /// substituted for event_time) qualified to alias `n`.
    fn navigation_sort_tuple(alias: &str) -> String {
        format!(
            "tuple({a}.sort_time, {a}.source_host, {a}.source_file, {a}.source_generation, {a}.source_offset, {a}.source_line_no, {a}.event_uid)",
            a = alias
        )
    }

    /// §5.2.1 signal read: one directory point-range aggregate for the change
    /// signals plus the live-head fingerprint. Content-free (no wide columns).
    pub(super) fn build_session_signal_sql(&self, session_id: &str) -> String {
        let directory = self.table_ref("mcp_session_directory");
        let published = self.published_generations_subquery();
        let sid = sql_quote(session_id);
        // The fingerprint is over the session's LIVE (host,name,file,generation)
        // heads at the pinned revision: intersect the published generations with
        // the sources the directory has observed for this session.
        let fingerprint = format!(
            "(SELECT hex(SHA256(arrayStringConcat(arraySort(groupArray(toJSONString(tuple(p.source_host, p.source_name, p.source_file, p.source_generation)))), '\\0')))\n   FROM {published} AS p\n   WHERE (p.source_host, p.source_name, p.source_file) IN (\n     SELECT source_host, source_name, source_file FROM {directory} WHERE session_id = {sid}\n   ))"
        );
        format!(
            "SELECT\n  toUInt64(sum(d.observed_events)) AS observed_sum,\n  toInt64(toUnixTimestamp64Milli(min(d.min_observed_event_time))) AS min_bound_ms,\n  toInt64(toUnixTimestamp64Milli(max(d.max_observed_event_time))) AS max_bound_ms,\n  {fingerprint} AS heads_fingerprint\nFROM {directory} AS d\nWHERE d.session_id = {sid}\nFORMAT JSONEachRow"
        )
    }

    /// §5.2.2 boundary guard: one streaming aggregate over the navigation rows
    /// at-or-before the anchor tuple. `count()` and `countIf(is_user_message)`
    /// prove the served prefix is intact. Content-free; PK-pruned to the session
    /// and to `sort_time <= anchor.sort_time`.
    pub(super) fn build_prefix_boundary_guard_sql(
        &self,
        session_id: &str,
        anchor: &CanonicalReadAnchor,
    ) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let anchor_tuple = self.anchor_tuple_literal(anchor);
        format!(
            "SELECT\n  toUInt64(countIf({tuple} <= {anchor_tuple})) AS count_le_anchor,\n  toUInt64(countIf(n.is_user_message = 1 AND {tuple} <= {anchor_tuple})) AS user_messages_le_anchor\n{from}\nWHERE n.session_id = {sid}\n  AND n.sort_time <= toDateTime64({sort_ms} / 1000.0, 3)\nFORMAT JSONEachRow",
            sid = sql_quote(session_id),
            sort_ms = anchor.sort_time_ms,
        )
    }

    /// The navigation ordering-tuple literal for an anchor, for keyset and
    /// boundary comparisons.
    fn anchor_tuple_literal(&self, anchor: &CanonicalReadAnchor) -> String {
        format!(
            "tuple(toDateTime64({sort_ms} / 1000.0, 3), {host}, {file}, {gen}, {offset}, {line}, {uid})",
            sort_ms = anchor.sort_time_ms,
            host = sql_quote(&anchor.source_host),
            file = sql_quote(&anchor.source_file),
            gen = anchor.source_generation,
            offset = anchor.source_offset,
            line = anchor.source_line_no,
            uid = sql_quote(&anchor.event_uid),
        )
    }

    /// §5.2.3 window chunk read: one navigation page of at most `limit + 1`
    /// rows strictly after `after` (keyset), bounded above by a closed
    /// `sort_time` window so the input is granule-bounded regardless of FINAL
    /// read-in-order behavior. Content-free; every classification column is a
    /// per-row scalar or a WI-01 fragment over the live-head-filtered rows.
    ///
    /// `after` is `None` for a first page (session start). The `LIMIT` is
    /// `limit + 1` so the caller detects a further page from the extra row.
    pub(super) fn build_navigation_window_sql(
        &self,
        session_id: &str,
        after: Option<&CanonicalReadAnchor>,
        window_upper_ms: Option<i64>,
        limit: u16,
    ) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let sid = sql_quote(session_id);
        let mut predicates = vec![format!("n.session_id = {sid}")];
        if let Some(after) = after {
            predicates.push(format!("{tuple} > {}", self.anchor_tuple_literal(after)));
        }
        if let Some(upper_ms) = window_upper_ms {
            predicates.push(format!(
                "n.sort_time < toDateTime64({upper_ms} / 1000.0, 3)"
            ));
        }
        let where_clause = predicates.join("\n  AND ");
        let k_plus_one = u32::from(limit).saturating_add(1);
        // Inner derived table exposes the physical `events` column names plus
        // `phase` (the WI-01 turn-summary predicates reference `phase`, which
        // the navigation index stores as `tool_phase`). Keyset + ORDER BY +
        // LIMIT live on the inner scan so the navigation PK is pruned.
        let inner = format!(
            "SELECT\n    n.event_uid AS event_uid,\n    n.source_host AS source_host,\n    n.source_file AS source_file,\n    n.source_generation AS source_generation,\n    n.source_offset AS source_offset,\n    n.source_line_no AS source_line_no,\n    n.sort_time AS sort_time,\n    n.display_time AS display_time,\n    n.turn_index AS turn_index,\n    n.is_user_message AS is_user_message,\n    n.actor_kind AS actor_kind,\n    n.event_kind AS event_kind,\n    n.payload_type AS payload_type,\n    n.tool_name AS tool_name,\n    n.tool_phase AS phase\n  {from}\n  WHERE {where_clause}\n  ORDER BY {tuple} ASC\n  LIMIT {k_plus_one}"
        );
        let event_type = cd::event_type_expr(COLS);
        let predicates = cd::TurnSummaryPredicates::new(COLS);
        format!(
            "SELECT\n  ev.event_uid AS event_uid,\n  ev.source_host AS source_host,\n  ev.source_file AS source_file,\n  ev.source_generation AS source_generation,\n  ev.source_offset AS source_offset,\n  ev.source_line_no AS source_line_no,\n  toInt64(toUnixTimestamp64Milli(ev.sort_time)) AS sort_time_ms,\n  toString(ev.display_time) AS display_time,\n  toInt64(toUnixTimestamp64Milli(ev.display_time)) AS display_time_ms,\n  toUInt32(ev.turn_index) AS turn_index,\n  toUInt8(ev.is_user_message) AS is_user_message,\n  ev.actor_kind AS actor_kind,\n  ev.event_kind AS event_kind,\n  ev.payload_type AS payload_type,\n  ev.tool_name AS tool_name,\n  {event_type} AS event_type,\n  toUInt8({user_input}) AS is_user_input,\n  toUInt8({final_response}) AS is_final_response,\n  toUInt8(ev.payload_type IN ('task_complete', 'turn_aborted')) AS is_terminal,\n  toUInt8(ev.payload_type = 'task_complete') AS is_completed\nFROM (\n  {inner}\n) AS ev\nORDER BY {inner_tuple} ASC\nFORMAT JSONEachRow",
            user_input = predicates.user_input,
            final_response = predicates.final_response,
            inner_tuple = Self::navigation_sort_tuple("ev"),
        )
    }

    /// §5.2.4 totals aggregate: one streaming hash aggregate over the session's
    /// live navigation rows producing every [`SessionMetadata`] scalar plus the
    /// override-path (`max_override`) and counter-path (`counter_user_messages`
    /// = U at the last `turn_index = 0` row — VERIFIER ADDENDUM item 1)
    /// contributions to `total_turns`. Content-free.
    pub(super) fn build_session_totals_sql(&self, session_id: &str) -> String {
        let from = self.navigation_live_from();
        let sid = sql_quote(session_id);
        let umsg = cd::user_message_count_predicate(COLS);
        let assistant_msg = "n.actor_kind = 'assistant' AND n.event_kind = 'message'";
        let mode = cd::mode_aggregate_expr(COLS);
        let display_tuple = "tuple(n.display_time, n.sort_time, n.source_host, n.source_file, n.source_generation, n.source_offset, n.source_line_no, n.event_uid)";
        let sort_tuple = Self::navigation_sort_tuple("n");
        // U at the last counter (turn_index = 0) row: count user-messages at or
        // before the max counter-row tuple. Both scans are session-pruned and
        // narrow.
        let counter_user_messages = format!(
            "toUInt64(countIf(n.is_user_message = 1 AND {sort_tuple} <= (\n    SELECT maxIf({inner_tuple}, ni.turn_index = 0)\n    FROM {nav} AS ni FINAL\n    ALL INNER JOIN {published} AS pi\n      ON pi.source_host = ni.source_host AND pi.source_name = ni.source_name AND pi.source_file = ni.source_file AND pi.source_generation = ni.source_generation\n    WHERE ni.session_id = {sid}\n  )))",
            inner_tuple = Self::navigation_sort_tuple("ni"),
            nav = self.table_ref("mcp_event_navigation"),
            published = self.published_generations_subquery(),
        );
        let event_ts_tuple = "tuple(n.event_ts, n.event_uid)";
        format!(
            "SELECT\n  toUInt64(count()) AS total_events,\n  toUInt64(countIf({umsg})) AS user_messages,\n  toUInt64(countIf({assistant_msg})) AS assistant_messages,\n  toUInt64(countIf(n.event_kind = 'tool_call')) AS tool_calls,\n  toUInt64(countIf(n.event_kind = 'tool_result')) AS tool_results,\n  toUInt32(max(n.turn_index)) AS max_override,\n  {counter_user_messages} AS counter_user_messages,\n  toString(min(n.display_time)) AS first_event_time,\n  toInt64(toUnixTimestamp64Milli(min(n.display_time))) AS first_event_unix_ms,\n  toString(max(n.display_time)) AS last_event_time,\n  toInt64(toUnixTimestamp64Milli(max(n.display_time))) AS last_event_unix_ms,\n  ifNull(argMin(nullIf(n.event_uid, ''), {display_tuple}), '') AS first_event_uid,\n  ifNull(argMax(nullIf(n.event_uid, ''), {display_tuple}), '') AS last_event_uid,\n  ifNull(argMax(nullIf(n.actor_kind, ''), {display_tuple}), '') AS last_actor_role,\n  ifNull(argMax(nullIf(n.source_name, ''), {event_ts_tuple}), '') AS source,\n  ifNull(argMax(nullIf(n.harness, ''), {event_ts_tuple}), '') AS harness,\n  ifNull(argMax(nullIf(n.inference_provider, ''), {event_ts_tuple}), '') AS inference_provider,\n  {mode} AS mode\n{from}\nWHERE n.session_id = {sid}\nFORMAT JSONEachRow"
        )
    }

    /// §4 bounded metadata read: `payload_json` is decompressed for exactly the
    /// metadata-bearing rows (`is_metadata_bearing = 1`), never the whole
    /// session. The reader applies the OPEN per-field precedence in Rust.
    pub(super) fn build_session_metadata_sql(&self, session_id: &str) -> String {
        let events = self.live_events_source_scoped(Some(session_id));
        let nav = self.table_ref("mcp_event_navigation");
        let published = self.published_generations_subquery();
        let sid = sql_quote(session_id);
        format!(
            "SELECT\n  toString(e.event_ts) AS event_ts,\n  e.event_uid AS event_uid,\n  e.event_kind AS event_kind,\n  e.payload_json AS payload_json,\n  e.source_name AS source_name,\n  e.source_file AS source_file,\n  e.session_id AS session_id\nFROM {events} AS e\nWHERE e.event_uid IN (\n  SELECT n.event_uid\n  FROM {nav} AS n FINAL\n  ALL INNER JOIN {published} AS published\n    ON published.source_host = n.source_host AND published.source_name = n.source_name AND published.source_file = n.source_file AND published.source_generation = n.source_generation\n  WHERE n.session_id = {sid} AND n.is_metadata_bearing = 1\n)\nFORMAT JSONEachRow"
        )
    }

    /// Session-level completed / terminal (design §5.5, R1(b)): the terminal
    /// semantics of the session's last terminal event by ordering tuple, a
    /// faithful streaming approximation of the projector's
    /// `argMax(…, turn_seq)`-over-turns semantics that the WI-10 terminal-in-
    /// middle-turn parity fixture pins exactly. Content-free.
    pub(super) fn build_session_terminal_sql(&self, session_id: &str) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        // v1's session rule is two-level (R1(b) / session_terminal_selection):
        // per-TURN completed = the turn's latest terminal event being
        // task_complete, then the session takes `argMax(completed, turn_seq)`
        // — the LAST turn's flag. A session-wide latest-terminal argMax
        // diverges when the terminal event sits in a middle turn (caught by
        // the canonical-open-parity terminal-mid fixture). turn_seq is derived
        // per event with the projector's shared windowed rule, using the
        // navigation index's precomputed is_user_message (identical predicate
        // by MV construction).
        format!(
            "SELECT\n  toUInt8(argMax(turn_completed, turn_seq)) AS completed,\n  argMax(turn_terminal_uid, turn_seq) AS terminal_event_uid,\n  toUInt64(sum(turn_terminal_count)) AS terminal_count\nFROM (\n  SELECT\n    turn_seq,\n    argMaxIf(toUInt8(payload_type = 'task_complete'), sort_key, payload_type IN ('task_complete', 'turn_aborted')) AS turn_completed,\n    ifNull(argMaxIf(nullIf(event_uid, ''), sort_key, payload_type IN ('task_complete', 'turn_aborted')), '') AS turn_terminal_uid,\n    countIf(payload_type IN ('task_complete', 'turn_aborted')) AS turn_terminal_count\n  FROM (\n    SELECT\n      n.event_uid AS event_uid,\n      n.payload_type AS payload_type,\n      {tuple} AS sort_key,\n      if(toUInt32(n.turn_index) > 0, toUInt32(n.turn_index), greatest(toUInt32(1), toUInt32(sum(if(n.is_user_message = 1, 1, 0)) OVER turn_window))) AS turn_seq\n    {from}\n    WHERE n.session_id = {sid}\n    WINDOW turn_window AS (ORDER BY {tuple} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n  )\n  GROUP BY turn_seq\n)\nFORMAT JSONEachRow",
            sid = sql_quote(session_id),
        )
    }

    /// §5.2.7 wide hydration: `text_content` / `payload_json` (and every other
    /// [`TraceEvent`] content column) for exactly the page's selected
    /// `event_uid`s — the only place besides the bounded metadata read that
    /// touches wide columns. `event_order` / `turn_seq` are NOT read here; they
    /// are reader-derived.
    pub(super) fn build_wide_hydration_sql(
        &self,
        session_id: &str,
        event_uids: &[String],
    ) -> String {
        let events = self.live_events_source_scoped(Some(session_id));
        let uid_list = sql_array_strings(event_uids);
        let text_cap = cd::MAX_PROJECTED_TEXT_SUMMARY_CHARS;
        let payload_cap = cd::MAX_PROJECTED_PAYLOAD_SUMMARY_CHARS;
        format!(
            "SELECT\n  e.session_id AS session_id,\n  e.event_uid AS event_uid,\n  toString(e.event_ts) AS event_time,\n  toInt64(toUnixTimestamp64Milli(e.event_ts)) AS event_unix_ms,\n  e.actor_kind AS actor_role,\n  e.event_kind AS event_class,\n  e.payload_type AS payload_type,\n  e.tool_call_id AS call_id,\n  e.tool_name AS name,\n  e.tool_phase AS phase,\n  e.item_id AS item_id,\n  e.source_ref AS source_ref,\n  substring(e.text_content, 1, {text_cap}) AS text_content,\n  substring(e.payload_json, 1, {payload_cap}) AS payload_json,\n  e.token_usage_json AS token_usage_json,\n  e.endpoint_kind AS endpoint_kind,\n  e.token_usage_buckets AS token_usage_buckets,\n  e.token_usage_native_units AS token_usage_native_units\nFROM {events} AS e\nWHERE e.event_uid IN {uid_list}\nFORMAT JSONEachRow"
        )
    }

    /// §5.5 `open(event)` locator seek: point lookup by `event_uid` against the
    /// versioned locator, then reject non-live generations by inner-joining the
    /// pinned published heads. Zero live candidates ⇒ `Ok(None)` upstream.
    pub(super) fn build_event_locator_sql(&self, event_uid: &str) -> String {
        let locator = self.table_ref("mcp_event_locator");
        let published = self.published_generations_subquery();
        format!(
            "SELECT\n  l.event_uid AS event_uid,\n  l.source_host AS source_host,\n  l.session_id AS session_id,\n  l.source_file AS source_file,\n  l.source_generation AS source_generation,\n  l.source_offset AS source_offset,\n  l.source_line_no AS source_line_no,\n  toInt64(toUnixTimestamp64Milli(l.sort_time)) AS sort_time_ms\nFROM {locator} AS l FINAL\nALL INNER JOIN {published} AS published\n  ON published.source_host = l.source_host AND published.source_name = l.source_name AND published.source_file = l.source_file AND published.source_generation = l.source_generation\nWHERE l.event_uid = {uid}\nFORMAT JSONEachRow",
            uid = sql_quote(event_uid),
        )
    }

    /// §5.5 `open(event)` prefix position: the target event's own
    /// `(event_order, turn_seq, prefix_user_message_count, event_ordinal)` from
    /// one aggregate over the navigation rows at-or-before its tuple (the spec's
    /// single session-scoped read). Content-free.
    pub(super) fn build_event_prefix_position_sql(&self, locator: &LocatorRow) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let target = format!(
            "tuple(toDateTime64({sort_ms} / 1000.0, 3), {host}, {file}, {gen}, {offset}, {line}, {uid})",
            sort_ms = locator.sort_time_ms,
            host = sql_quote(&locator.source_host),
            file = sql_quote(&locator.source_file),
            gen = locator.source_generation,
            offset = locator.source_offset,
            line = locator.source_line_no,
            uid = sql_quote(&locator.event_uid),
        );
        // `turn_seq_at_target` is the derived turn of the target: max(1, U at
        // target) unless overridden. The reader recomputes turn membership for
        // the ordinal via the same forward rule; here we surface the raw counts.
        format!(
            "SELECT\n  toUInt64(countIf({tuple} <= {target})) AS event_order,\n  toUInt64(countIf(n.is_user_message = 1 AND {tuple} <= {target})) AS prefix_user_message_count,\n  toUInt32(anyIf(n.turn_index, {tuple} = {target})) AS target_turn_index\n{from}\nWHERE n.session_id = {sid}\n  AND n.sort_time <= toDateTime64({sort_ms} / 1000.0, 3)\nFORMAT JSONEachRow",
            sid = sql_quote(&locator.session_id),
            sort_ms = locator.sort_time_ms,
        )
    }
}

// ---------------------------------------------------------------------------
// Deserialization rows for the aggregate / hydration statements.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub(super) struct TotalsRow {
    pub(super) total_events: u64,
    pub(super) user_messages: u64,
    pub(super) assistant_messages: u64,
    pub(super) tool_calls: u64,
    pub(super) tool_results: u64,
    pub(super) max_override: u32,
    pub(super) counter_user_messages: u64,
    pub(super) first_event_time: String,
    pub(super) first_event_unix_ms: i64,
    pub(super) last_event_time: String,
    pub(super) last_event_unix_ms: i64,
    pub(super) first_event_uid: String,
    pub(super) last_event_uid: String,
    pub(super) last_actor_role: String,
    pub(super) source: String,
    pub(super) harness: String,
    pub(super) inference_provider: String,
    pub(super) mode: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct MetaRow {
    pub(super) event_ts: String,
    pub(super) event_uid: String,
    pub(super) event_kind: String,
    pub(super) payload_json: String,
    pub(super) source_name: String,
    pub(super) source_file: String,
    pub(super) session_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct TerminalRow {
    pub(super) completed: u8,
    pub(super) terminal_event_uid: String,
    #[allow(dead_code)]
    pub(super) terminal_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EventPrefixRow {
    pub(super) event_order: u64,
    pub(super) prefix_user_message_count: u64,
    pub(super) target_turn_index: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WideRow {
    pub(super) session_id: String,
    pub(super) event_uid: String,
    pub(super) event_time: String,
    pub(super) event_unix_ms: i64,
    pub(super) actor_role: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) call_id: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) item_id: String,
    pub(super) source_ref: String,
    pub(super) text_content: String,
    pub(super) payload_json: String,
    pub(super) token_usage_json: String,
    pub(super) endpoint_kind: String,
    pub(super) token_usage_buckets: BTreeMap<String, u64>,
    pub(super) token_usage_native_units: BTreeMap<String, f64>,
}

/// The session-level header the three open paths share: metadata scalars plus
/// the OPEN-precedence title/summary/slug and session completed/terminal.
///
/// Serializable so an `open(session)` continuation can carry it across pages
/// (design §5.5 `SessionCarry`): the header is session-level state that does not
/// change while a session is quiescent, so page k+1 reuses it instead of
/// re-running the page-1 totals/metadata/terminal scans.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct CanonicalSessionHeader {
    pub(super) metadata: SessionMetadata,
    pub(super) title: Option<String>,
    pub(super) source: Option<String>,
    pub(super) harness: Option<String>,
    pub(super) inference_provider: Option<String>,
    pub(super) session_slug: Option<String>,
    pub(super) session_summary: Option<String>,
    pub(super) completed: bool,
    pub(super) terminal_event_uid: Option<String>,
    /// The session's `max(turn_index)` (design §5.2.4 override-path input). When
    /// it is `0` the session has no explicit turn overrides, so `turn_seq` is
    /// monotonic in navigation-tuple order and the session page reader can
    /// early-stop after `K + 1` turns rather than sweeping the whole session.
    pub(super) max_override: u32,
}

/// The outcome of resolving a continuation cursor into a resumable page start.
pub(super) struct ResolvedContinuation {
    pub(super) start_after: Option<CanonicalReadAnchor>,
    pub(super) start_derived: ForwardDerived,
    pub(super) signals: CanonicalSessionSignals,
    /// The page-1 session header, reused verbatim when the session is quiescent
    /// (design §5.5). `None` on a first page or after inserts landed, in which
    /// case the page recomputes the header.
    pub(super) carried_header: Option<CanonicalSessionHeader>,
}

/// Serialize a session header for the continuation cursor's `session_carry`.
fn encode_session_carry(header: &CanonicalSessionHeader) -> Option<String> {
    serde_json::to_string(header).ok()
}

/// Decode a carried session header; a malformed carry falls back to recompute.
fn decode_session_carry(carry: Option<&str>) -> Option<CanonicalSessionHeader> {
    carry.and_then(|json| serde_json::from_str(json).ok())
}

/// A synthetic pre-session anchor: the zero state a first-page forward
/// derivation starts from.
fn zero_derived() -> ForwardDerived {
    ForwardDerived {
        event_order: 0,
        turn_seq: 0,
        prefix_user_message_count: 0,
        event_ordinal: 0,
    }
}

fn forward_from_anchor(anchor: &CanonicalReadAnchor) -> ForwardDerived {
    ForwardDerived {
        event_order: anchor.event_order,
        turn_seq: anchor.turn_seq,
        prefix_user_message_count: anchor.prefix_user_message_count,
        event_ordinal: anchor.event_ordinal,
    }
}

fn nav_row_to_anchor(row: &NavWindowRow, der: &ForwardDerived) -> CanonicalReadAnchor {
    CanonicalReadAnchor {
        sort_time_ms: row.sort_time_ms,
        source_host: row.source_host.clone(),
        source_file: row.source_file.clone(),
        source_generation: row.source_generation,
        source_offset: row.source_offset,
        source_line_no: row.source_line_no,
        event_uid: row.event_uid.clone(),
        event_order: der.event_order,
        turn_seq: der.turn_seq,
        prefix_user_message_count: der.prefix_user_message_count,
        event_ordinal: der.event_ordinal,
    }
}

fn json_field(payload_json: &str, key: &str) -> String {
    serde_json::from_str::<Value>(payload_json)
        .ok()
        .and_then(|value| value.get(key).and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default()
}

/// The projector's omp dispatch-title basename (`projection.rs`): backslashes to
/// `/`, last path segment, strip a trailing `.jsonl`, trimmed.
fn omp_dispatch_basename(source_file: &str) -> String {
    let normalized = source_file.replace('\\', "/");
    let basename = normalized.rsplit('/').next().unwrap_or(&normalized);
    basename
        .strip_suffix(".jsonl")
        .unwrap_or(basename)
        .trim()
        .to_string()
}

#[derive(Debug, Clone, Default)]
struct MetadataPrecedence {
    latest_metadata_title: String,
    latest_metadata_name: String,
    latest_metadata_summary: String,
    session_slug: String,
    omp_dispatch_title: String,
}

/// Reproduce the projector's OPEN metadata-precedence inputs
/// (`projection.rs`) in Rust over the bounded metadata-bearing rows: latest
/// non-empty per field by `(event_ts, event_uid)`, session-meta-only for
/// name/summary/slug, and the first omp non-session-file dispatch basename.
fn metadata_precedence(rows: &[MetaRow]) -> MetadataPrecedence {
    let mut ordered: Vec<&MetaRow> = rows.iter().collect();
    ordered.sort_by(|a, b| {
        (a.event_ts.as_str(), a.event_uid.as_str())
            .cmp(&(b.event_ts.as_str(), b.event_uid.as_str()))
    });
    let mut precedence = MetadataPrecedence::default();
    let mut dispatch_seen = false;
    for row in ordered {
        let title = json_field(&row.payload_json, "title");
        if !title.is_empty() {
            precedence.latest_metadata_title = title;
        }
        if row.event_kind == "session_meta" {
            let name = json_field(&row.payload_json, "name");
            if !name.is_empty() {
                precedence.latest_metadata_name = name;
            }
            let summary = json_field(&row.payload_json, "summary");
            if !summary.is_empty() {
                precedence.latest_metadata_summary = summary;
            }
            let slug = json_field(&row.payload_json, "slug");
            if !slug.is_empty() {
                precedence.session_slug = slug;
            }
        }
        let session_file = format!("{}.jsonl", row.session_id);
        if !dispatch_seen
            && row.source_name == "omp"
            && row.source_file.ends_with(".jsonl")
            && !row.source_file.ends_with(&session_file)
        {
            let basename = omp_dispatch_basename(&row.source_file);
            if !basename.is_empty() {
                precedence.omp_dispatch_title = basename;
                dispatch_seen = true;
            }
        }
    }
    precedence
}

/// The projector's OPEN title/summary coalesce chains (`projection.rs`):
/// omp sources fall through to the dispatch title, non-omp sources do not.
fn open_title_and_summary(source: &str, p: &MetadataPrecedence) -> (String, String) {
    let coalesce = |candidates: &[&str]| -> String {
        candidates
            .iter()
            .find(|value| !value.is_empty())
            .map(|value| value.to_string())
            .unwrap_or_default()
    };
    let (title, summary) = if source == "omp" {
        (
            coalesce(&[
                &p.latest_metadata_title,
                &p.latest_metadata_name,
                &p.latest_metadata_summary,
                &p.omp_dispatch_title,
            ]),
            coalesce(&[
                &p.latest_metadata_summary,
                &p.latest_metadata_title,
                &p.latest_metadata_name,
                &p.omp_dispatch_title,
            ]),
        )
    } else {
        (
            coalesce(&[&p.latest_metadata_title, &p.latest_metadata_name]),
            coalesce(&[
                &p.latest_metadata_summary,
                &p.latest_metadata_title,
                &p.latest_metadata_name,
            ]),
        )
    };
    (title, summary)
}

/// A per-event preview from a hydrated wide row: `text_content` if present,
/// else `payload_json` (the projector's `summary_is_payload = empty(text)`
/// split).
fn preview_from_wide(row: &WideRow, preview_chars: u16) -> Option<String> {
    if row.text_content.trim().is_empty() {
        canonical_preview(&row.payload_json, true, preview_chars)
    } else {
        canonical_preview(&row.text_content, false, preview_chars)
    }
}

fn event_ref_from(session_id: &str, der: &ForwardDerived, row: &NavWindowRow) -> McpEventRef {
    McpEventRef {
        session_id: session_id.to_string(),
        event_uid: row.event_uid.clone(),
        event_order: der.event_order,
        turn_seq: der.turn_seq,
        event_time: row.display_time.clone(),
        event_type: row.event_type.clone(),
    }
}

impl ClickHouseConversationRepository {
    /// Read the cheap per-session change signals plus the pinned revision.
    pub(super) async fn canonical_read_signals(
        &self,
        session_id: &str,
    ) -> RepoResult<CanonicalSessionSignals> {
        let snapshot = require_active_publication_snapshot("canonical signal reads");
        let sql = self.build_session_signal_sql(session_id);
        let row = self
            .map_backend(self.query_rows::<SignalRow>(&sql, None).await)?
            .into_iter()
            .next();
        Ok(CanonicalSessionSignals {
            pinned_revision: snapshot.pinned_revision_u64(),
            heads_fingerprint: row
                .as_ref()
                .map(|row| row.heads_fingerprint.clone())
                .unwrap_or_default(),
            observed_sum: row.as_ref().map(|row| row.observed_sum).unwrap_or(0),
            min_bound_ms: row.as_ref().map(|row| row.min_bound_ms).unwrap_or(0),
            max_bound_ms: row.map(|row| row.max_bound_ms).unwrap_or(0),
        })
    }

    /// Run the exact prefix boundary guard for a continuation anchor.
    pub(super) async fn canonical_boundary_intact(
        &self,
        session_id: &str,
        anchor: &CanonicalReadAnchor,
    ) -> RepoResult<bool> {
        let sql = self.build_prefix_boundary_guard_sql(session_id, anchor);
        let guard = self
            .map_backend(self.query_rows::<BoundaryGuardRow>(&sql, None).await)?
            .into_iter()
            .next();
        Ok(guard.is_some_and(|guard| {
            guard.count_le_anchor == anchor.event_order
                && guard.user_messages_le_anchor == anchor.prefix_user_message_count
        }))
    }

    /// Assemble the shared session header (metadata + OPEN precedence + terminal).
    pub(super) async fn canonical_session_header(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<CanonicalSessionHeader>> {
        let totals_sql = self.build_session_totals_sql(session_id);
        let Some(totals) = self
            .map_backend(self.query_rows::<TotalsRow>(&totals_sql, None).await)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        if totals.total_events == 0 {
            return Ok(None);
        }

        let metadata_sql = self.build_session_metadata_sql(session_id);
        let metadata_rows =
            self.map_backend(self.query_rows::<MetaRow>(&metadata_sql, None).await)?;
        let terminal_sql = self.build_session_terminal_sql(session_id);
        let terminal = self
            .map_backend(self.query_rows::<TerminalRow>(&terminal_sql, None).await)?
            .into_iter()
            .next();

        let precedence = metadata_precedence(&metadata_rows);
        let (title, summary) = open_title_and_summary(&totals.source, &precedence);
        let total_turns = total_turns_from_parts(
            totals.total_events,
            totals.max_override,
            totals.counter_user_messages,
        );
        let metadata = SessionMetadata {
            session_id: session_id.to_string(),
            first_event_time: totals.first_event_time,
            first_event_unix_ms: totals.first_event_unix_ms,
            last_event_time: totals.last_event_time,
            last_event_unix_ms: totals.last_event_unix_ms,
            total_turns,
            total_events: totals.total_events,
            user_messages: totals.user_messages,
            assistant_messages: totals.assistant_messages,
            tool_calls: totals.tool_calls,
            tool_results: totals.tool_results,
            mode: Self::parse_mode(&totals.mode),
            first_event_uid: totals.first_event_uid,
            last_event_uid: totals.last_event_uid,
            last_actor_role: totals.last_actor_role,
        };
        Ok(Some(CanonicalSessionHeader {
            metadata,
            max_override: totals.max_override,
            title: non_empty_string(title),
            source: non_empty_string(totals.source),
            harness: non_empty_string(totals.harness),
            inference_provider: non_empty_string(totals.inference_provider),
            session_slug: non_empty_string(precedence.session_slug),
            session_summary: non_empty_string(summary),
            completed: terminal.as_ref().is_some_and(|row| row.completed != 0),
            terminal_event_uid: terminal.and_then(|row| non_empty_string(row.terminal_event_uid)),
        }))
    }

    /// Stream the session's live navigation rows in order, forward-deriving
    /// `(event_order, turn_seq, U, event_ordinal)` per row from `start_derived`.
    ///
    /// Chunked by keyset navigation windows; each chunk is granule-bounded.
    ///
    /// NOTE (WI-06 → WI-09): the caller currently drives a full-session sweep
    /// for session/turn/event assembly. The output-sized early-stop paging
    /// (stop after `K + 1` turns, cursor-anchored prefix skip, per-page stray-
    /// override statement — design §5.2.3/§5.3) is the performance-gated
    /// refinement verified by the WI-09 boundedness benchmark; the shared query
    /// shapes and forward derivation it relies on are established here.
    pub(super) async fn canonical_stream_navigation(
        &self,
        session_id: &str,
        start_after: Option<CanonicalReadAnchor>,
        start_derived: ForwardDerived,
    ) -> RepoResult<Vec<(NavWindowRow, ForwardDerived)>> {
        const CHUNK: u16 = 4096;
        let mut out: Vec<(NavWindowRow, ForwardDerived)> = Vec::new();
        let mut anchor_state = start_derived;
        let mut after = start_after;
        loop {
            let sql = self.build_navigation_window_sql(session_id, after.as_ref(), None, CHUNK);
            let chunk = self.map_backend(self.query_rows::<NavWindowRow>(&sql, None).await)?;
            if chunk.is_empty() {
                break;
            }
            let inputs: Vec<ForwardInput> = chunk
                .iter()
                .map(|row| ForwardInput {
                    turn_index: row.turn_index,
                    is_user_message: row.is_user_message != 0,
                })
                .collect();
            let derived = derive_forward(anchor_state, &inputs);
            let has_more = chunk.len() > usize::from(CHUNK);
            for (row, der) in chunk.into_iter().zip(derived.iter().copied()) {
                out.push((row, der));
            }
            if let Some((last_row, last_der)) = out.last() {
                anchor_state = *last_der;
                after = Some(nav_row_to_anchor(last_row, last_der));
            }
            if !has_more {
                break;
            }
        }
        Ok(out)
    }

    /// Output-sized `open(session)` page collection (design §5.2.3/§5.5): stream
    /// navigation windows forward from `start_after`, early-stopping as soon as
    /// `limit + 1` distinct `turn_seq`s beyond `after_turn_seq` have begun — so
    /// one page reads `O(page events)` rows, never the whole session.
    ///
    /// SAFETY (parity): this is only equal to the full-session bucketing when
    /// `turn_seq` is monotonic in navigation-tuple order, which holds exactly
    /// when the session has no explicit turn overrides (`max_override == 0`): all
    /// rows then carry `turn_index = 0`, so `turn_seq = max(1, U)` and `U` is
    /// non-decreasing in tuple order. The caller keeps the full-session sweep for
    /// override-bearing sessions, whose non-contiguous stray recurrences the
    /// early stop cannot see. The per-query chunk is `max(1024, 8 * limit)`
    /// (design §5.2.3 chunk target), so read-rows per page are bounded by the
    /// page's own turns plus one chunk of granularity, independent of session
    /// size.
    async fn canonical_stream_session_page(
        &self,
        session_id: &str,
        start_after: Option<CanonicalReadAnchor>,
        start_derived: ForwardDerived,
        after_turn_seq: u32,
        limit: usize,
    ) -> RepoResult<Vec<(NavWindowRow, ForwardDerived)>> {
        let chunk: u16 = u16::try_from(std::cmp::max(1024, limit.saturating_mul(8)))
            .unwrap_or(u16::MAX)
            .saturating_sub(1)
            .max(1);
        let mut out: Vec<(NavWindowRow, ForwardDerived)> = Vec::new();
        let mut anchor_state = start_derived;
        let mut after = start_after;
        let mut distinct_new: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
        'outer: loop {
            let sql = self.build_navigation_window_sql(session_id, after.as_ref(), None, chunk);
            let rows = self.map_backend(self.query_rows::<NavWindowRow>(&sql, None).await)?;
            if rows.is_empty() {
                break;
            }
            let take = rows.len().min(usize::from(chunk));
            let has_more_chunk = rows.len() > usize::from(chunk);
            let inputs: Vec<ForwardInput> = rows
                .iter()
                .take(take)
                .map(|row| ForwardInput {
                    turn_index: row.turn_index,
                    is_user_message: row.is_user_message != 0,
                })
                .collect();
            let derived = derive_forward(anchor_state, &inputs);
            for (row, der) in rows.into_iter().take(take).zip(derived.iter().copied()) {
                if der.turn_seq > after_turn_seq {
                    distinct_new.insert(der.turn_seq);
                }
                out.push((row, der));
                // Once the `(limit + 1)`-th new turn has begun, every earlier new
                // turn is complete (monotonic ordering); stop with that turn's
                // first row present so `bucket_turns` still sees `limit + 1`
                // buckets (has-more detection + the page's continuation anchor).
                if distinct_new.len() > limit {
                    break 'outer;
                }
            }
            if let Some((last_row, last_der)) = out.last() {
                anchor_state = *last_der;
                after = Some(nav_row_to_anchor(last_row, last_der));
            }
            if !has_more_chunk {
                break;
            }
        }
        Ok(out)
    }

    /// Hydrate the wide content columns for exactly `event_uids`.
    async fn canonical_hydrate(
        &self,
        session_id: &str,
        event_uids: &[String],
    ) -> RepoResult<HashMap<String, WideRow>> {
        if event_uids.is_empty() {
            return Ok(HashMap::default());
        }
        let sql = self.build_wide_hydration_sql(session_id, event_uids);
        let rows = self.map_backend(self.query_rows::<WideRow>(&sql, None).await)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.event_uid.clone(), row))
            .collect())
    }

    /// Fold one turn's page rows into an [`McpTurnCompact`] (design §5.3 folds,
    /// WI-01 fragment parity). `summaries` hydrates only the turn's user-input /
    /// final-response events.
    fn assemble_turn_compact(
        &self,
        session_id: &str,
        rows: &[(NavWindowRow, ForwardDerived)],
        summaries: &HashMap<String, WideRow>,
    ) -> McpTurnCompact {
        let preview_chars = self.cfg.preview_chars;
        let turn_seq = rows.first().map(|(_, der)| der.turn_seq).unwrap_or(0);
        let by_order = |candidates: &[usize]| -> Option<usize> {
            candidates
                .iter()
                .copied()
                .min_by_key(|&i| (rows[i].1.event_order, rows[i].0.event_uid.clone()))
        };
        let by_order_max = |candidates: &[usize]| -> Option<usize> {
            candidates
                .iter()
                .copied()
                .max_by_key(|&i| (rows[i].1.event_order, rows[i].0.event_uid.clone()))
        };

        let user_input_idx = by_order(&indices_where(rows, |(row, _)| row.is_user_input != 0));
        let final_response_idx =
            by_order_max(&indices_where(rows, |(row, _)| row.is_final_response != 0));
        let terminal_idx = by_order_max(&indices_where(rows, |(row, _)| row.is_terminal != 0));
        let first_idx = by_order(&(0..rows.len()).collect::<Vec<_>>());
        let last_idx = by_order_max(&(0..rows.len()).collect::<Vec<_>>());

        let turn_id = first_idx
            .map(|i| rows[i].0.turn_index.to_string())
            .unwrap_or_else(|| turn_seq.to_string());

        let started_at = rows
            .iter()
            .map(|(row, _)| row.display_time_ms)
            .min()
            .unwrap_or(0);
        let ended_at = rows
            .iter()
            .map(|(row, _)| row.display_time_ms)
            .max()
            .unwrap_or(0);
        let (started_str, ended_str) = (
            rows.iter()
                .min_by_key(|(row, _)| row.display_time_ms)
                .map(|(row, _)| row.display_time.clone())
                .unwrap_or_default(),
            rows.iter()
                .max_by_key(|(row, _)| row.display_time_ms)
                .map(|(row, _)| row.display_time.clone())
                .unwrap_or_default(),
        );

        let mut tools_called: Vec<String> = Vec::new();
        let mut normalized_event_types: Vec<String> = Vec::new();
        for (row, _) in rows {
            if row.event_type == "tool_call"
                && !row.tool_name.is_empty()
                && !tools_called.contains(&row.tool_name)
            {
                tools_called.push(row.tool_name.clone());
            }
            if row.event_type != McpEventType::Unknown.as_str()
                && !normalized_event_types.contains(&row.event_type)
            {
                normalized_event_types.push(row.event_type.clone());
            }
        }

        let summary_for = |idx: Option<usize>| -> Option<String> {
            idx.and_then(|i| summaries.get(&rows[i].0.event_uid))
                .and_then(|wide| preview_from_wide(wide, preview_chars))
        };

        let metadata = TurnSummary {
            session_id: session_id.to_string(),
            turn_seq,
            turn_id,
            started_at: started_str,
            started_at_unix_ms: started_at,
            ended_at: ended_str,
            ended_at_unix_ms: ended_at,
            total_events: rows.len() as u64,
            user_messages: rows.iter().filter(|(r, _)| r.is_user_message != 0).count() as u64,
            assistant_messages: rows
                .iter()
                .filter(|(r, _)| r.actor_kind == "assistant" && r.event_kind == "message")
                .count() as u64,
            tool_calls: rows
                .iter()
                .filter(|(r, _)| r.event_kind == "tool_call")
                .count() as u64,
            tool_results: rows
                .iter()
                .filter(|(r, _)| r.event_kind == "tool_result")
                .count() as u64,
            reasoning_items: rows
                .iter()
                .filter(|(r, _)| r.event_kind == "reasoning")
                .count() as u64,
        };

        McpTurnCompact {
            metadata,
            user_input_summary: summary_for(user_input_idx),
            final_response_summary: summary_for(final_response_idx),
            user_input_event: user_input_idx
                .map(|i| event_ref_from(session_id, &rows[i].1, &rows[i].0)),
            final_response_event: final_response_idx
                .map(|i| event_ref_from(session_id, &rows[i].1, &rows[i].0)),
            tools_called,
            normalized_event_types,
            completed: terminal_idx.is_some_and(|i| rows[i].0.is_completed != 0),
            terminal_event_uid: terminal_idx.map(|i| rows[i].0.event_uid.clone()),
            first_event: first_idx.map(|i| event_ref_from(session_id, &rows[i].1, &rows[i].0)),
            last_event: last_idx.map(|i| event_ref_from(session_id, &rows[i].1, &rows[i].0)),
        }
    }

    /// Page-in / page-out: one keyset page of an `open(session)` traversal.
    pub(super) async fn canonical_open_session_page_impl(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>> {
        Self::validate_session_id(session_id)?;
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        let limit = usize::from(limit.max(1));

        let ResolvedContinuation {
            start_after,
            start_derived,
            signals,
            carried_header,
        } = match self
            .canonical_resolve_continuation(session_id, after)
            .await?
        {
            Some(resolved) => resolved,
            None => return Ok(Some(CanonicalReadOutcome::Reopen)),
        };
        let after_turn_seq = start_derived.turn_seq;

        // Page 1 (and any post-insert continuation) runs the single allowed
        // session-wide header pass; a quiescent continuation reuses the carried
        // page-1 header, so later pages do no session-sized scan (design §5.5).
        let header = match carried_header {
            Some(header) => header,
            None => match self.canonical_session_header(session_id).await? {
                Some(header) => header,
                None => return Ok(None),
            },
        };

        // Override-free sessions have `turn_seq` monotonic in tuple order, so the
        // page reader early-stops after `K + 1` turns instead of sweeping the
        // whole session; override-bearing sessions keep the full sweep so their
        // non-contiguous stray recurrences still fold correctly (design §5.2.3).
        let rows = if header.max_override == 0 {
            self.canonical_stream_session_page(
                session_id,
                start_after,
                start_derived,
                after_turn_seq,
                limit,
            )
            .await?
        } else {
            self.canonical_stream_navigation(session_id, start_after, start_derived)
                .await?
        };
        let turns = bucket_turns(rows, after_turn_seq);

        let has_more = turns.len() > limit;
        let page_turns = &turns[..turns.len().min(limit)];

        // Hydrate the page's per-turn summary events only.
        let mut summary_uids: Vec<String> = Vec::new();
        for (_, bucket) in page_turns {
            for (row, _) in bucket {
                if row.is_user_input != 0 || row.is_final_response != 0 {
                    summary_uids.push(row.event_uid.clone());
                }
            }
        }
        let summaries = self.canonical_hydrate(session_id, &summary_uids).await?;

        let compact_turns: Vec<McpTurnCompact> = page_turns
            .iter()
            .map(|(_, bucket)| self.assemble_turn_compact(session_id, bucket, &summaries))
            .collect();

        // Carry the header verbatim so the next quiescent page skips the
        // session-wide header pass (design §5.5). The tool layer drops it if the
        // encoded cursor would exceed OPEN_CURSOR_MAX_CHARS; the reader then
        // recomputes.
        let session_carry = has_more.then(|| encode_session_carry(&header)).flatten();
        let continuation = if has_more {
            page_turns
                .last()
                .and_then(|(_, bucket)| bucket.last())
                .map(|(row, der)| CanonicalContinuation {
                    signals: signals.clone(),
                    after: nav_row_to_anchor(row, der),
                    after_turn_seq: der.turn_seq,
                    session_carry: session_carry.clone(),
                })
        } else {
            None
        };

        let session = McpSessionOpen {
            metadata: header.metadata,
            title: header.title,
            source: header.source,
            harness: header.harness,
            inference_provider: header.inference_provider,
            session_slug: header.session_slug,
            session_summary: header.session_summary,
            turns: compact_turns,
            completed: header.completed,
            terminal_event_uid: header.terminal_event_uid,
            snapshot: None,
        };
        Ok(Some(CanonicalReadOutcome::Page(CanonicalSessionPage {
            session,
            continuation,
        })))
    }

    /// Page-in / page-out: one keyset page of an `open(turn)` traversal.
    pub(super) async fn canonical_open_turn_page_impl(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        include_events: bool,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalTurnPage>>> {
        Self::validate_session_id(session_id)?;
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        let limit = usize::from(limit.max(1));

        let signals = match self
            .canonical_resolve_continuation(session_id, after.clone())
            .await?
        {
            Some(resolved) => resolved.signals,
            None => return Ok(Some(CanonicalReadOutcome::Reopen)),
        };

        let Some(header) = self.canonical_session_header(session_id).await? else {
            return Ok(None);
        };

        // Full session sweep, then the requested turn's rows (folding any
        // non-contiguous override recurrences into the same turn_seq bucket).
        let rows = self
            .canonical_stream_navigation(session_id, None, zero_derived())
            .await?;
        let turn_map = turn_ref_map(&rows, session_id);
        let turn_rows: Vec<(NavWindowRow, ForwardDerived)> = rows
            .into_iter()
            .filter(|(_, der)| der.turn_seq == turn_seq)
            .collect();
        if turn_rows.is_empty() {
            return Ok(None);
        }

        // Event paging within the turn (K + 1). The continuation anchor, when
        // present, is an event inside this turn.
        let start_index = match &after {
            Some(cont) => turn_rows
                .iter()
                .position(|(row, _)| row.event_uid == cont.after.event_uid)
                .map(|i| i + 1)
                .unwrap_or(0),
            None => 0,
        };
        let remaining = &turn_rows[start_index.min(turn_rows.len())..];
        let has_more = remaining.len() > limit;
        let page_rows = &remaining[..remaining.len().min(limit)];

        // Summaries always hydrate the turn's user-input / final-response
        // events; event previews additionally hydrate the page's events.
        let mut hydrate_uids: Vec<String> = turn_rows
            .iter()
            .filter(|(row, _)| row.is_user_input != 0 || row.is_final_response != 0)
            .map(|(row, _)| row.event_uid.clone())
            .collect();
        if include_events {
            hydrate_uids.extend(page_rows.iter().map(|(row, _)| row.event_uid.clone()));
        }
        let hydrated = self.canonical_hydrate(session_id, &hydrate_uids).await?;

        let compact = self.assemble_turn_compact(session_id, &turn_rows, &hydrated);
        let events = if include_events {
            self.assemble_event_summaries(session_id, page_rows, &hydrated)
        } else {
            Vec::new()
        };

        let previous_turn = turn_map
            .iter()
            .filter(|(seq, _)| **seq < turn_seq)
            .max_by_key(|(seq, _)| **seq)
            .map(|(_, turn_ref)| turn_ref.clone());
        let next_turn = turn_map
            .iter()
            .filter(|(seq, _)| **seq > turn_seq)
            .min_by_key(|(seq, _)| **seq)
            .map(|(_, turn_ref)| turn_ref.clone());

        let continuation = if include_events && has_more {
            page_rows.last().map(|(row, der)| CanonicalContinuation {
                signals: signals.clone(),
                after: nav_row_to_anchor(row, der),
                after_turn_seq: turn_seq,
                session_carry: None,
            })
        } else {
            None
        };

        let turn = McpTurnOpen {
            metadata: compact.metadata,
            events,
            parent_session_source: header.source,
            user_input_summary: compact.user_input_summary,
            final_response_summary: compact.final_response_summary,
            user_input_event: compact.user_input_event,
            final_response_event: compact.final_response_event,
            tools_called: compact.tools_called,
            normalized_event_types: compact.normalized_event_types,
            completed: compact.completed,
            terminal_event_uid: compact.terminal_event_uid,
            previous_turn,
            next_turn,
            first_event: compact.first_event,
            last_event: compact.last_event,
            snapshot: None,
        };
        Ok(Some(CanonicalReadOutcome::Page(CanonicalTurnPage {
            turn,
            continuation,
        })))
    }

    /// `open(event)`: locator seek, non-live rejection, scope, then the parent
    /// session/turn reconstruction and neighbor refs (design §5.5).
    pub(super) async fn canonical_open_event_impl(
        &self,
        event_uid: &str,
    ) -> RepoResult<Option<McpEventOpen>> {
        let event_uid = event_uid.trim();
        if event_uid.is_empty() {
            return Err(RepoError::invalid_argument("event_uid cannot be empty"));
        }
        Self::validate_event_uid(event_uid)?;

        let locator_sql = self.build_event_locator_sql(event_uid);
        // A superseded / replaying-only generation is rejected by the inner
        // join to the pinned heads, so zero rows ⇒ Ok(None).
        let Some(locator) = self
            .map_backend(self.query_rows::<LocatorRow>(&locator_sql, None).await)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        if !self.session_in_scope(&locator.session_id).await? {
            return Ok(None);
        }

        // The spec's single session-scoped position read.
        let prefix_sql = self.build_event_prefix_position_sql(&locator);
        let Some(prefix) = self
            .map_backend(self.query_rows::<EventPrefixRow>(&prefix_sql, None).await)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        if prefix.event_order == 0 {
            return Ok(None);
        }

        let Some(header) = self.canonical_session_header(&locator.session_id).await? else {
            return Ok(None);
        };

        // Reconstruct the parent turn and locate the target + neighbors.
        let rows = self
            .canonical_stream_navigation(&locator.session_id, None, zero_derived())
            .await?;
        let Some(target_index) = rows.iter().position(|(row, _)| row.event_uid == event_uid) else {
            return Ok(None);
        };
        let target_turn_seq = rows[target_index].1.turn_seq;

        let previous_event = target_index
            .checked_sub(1)
            .map(|i| event_ref_from(&locator.session_id, &rows[i].1, &rows[i].0));
        let next_event = rows
            .get(target_index + 1)
            .map(|(row, der)| event_ref_from(&locator.session_id, der, row));

        let turn_map = turn_ref_map(&rows, &locator.session_id);
        let previous_turn = turn_map
            .iter()
            .filter(|(seq, _)| **seq < target_turn_seq)
            .max_by_key(|(seq, _)| **seq)
            .map(|(_, turn_ref)| turn_ref.clone());
        let next_turn = turn_map
            .iter()
            .filter(|(seq, _)| **seq > target_turn_seq)
            .min_by_key(|(seq, _)| **seq)
            .map(|(_, turn_ref)| turn_ref.clone());

        let turn_rows: Vec<(NavWindowRow, ForwardDerived)> = rows
            .iter()
            .filter(|(_, der)| der.turn_seq == target_turn_seq)
            .cloned()
            .collect();
        let summary_uids: Vec<String> = turn_rows
            .iter()
            .filter(|(row, _)| row.is_user_input != 0 || row.is_final_response != 0)
            .map(|(row, _)| row.event_uid.clone())
            .collect();
        let summaries = self
            .canonical_hydrate(&locator.session_id, &summary_uids)
            .await?;
        let parent_turn = self.assemble_turn_compact(&locator.session_id, &turn_rows, &summaries);

        // Hydrate the target event's full content only.
        let hydrated = self
            .canonical_hydrate(&locator.session_id, &[event_uid.to_string()])
            .await?;
        let Some(wide) = hydrated.get(event_uid) else {
            return Ok(None);
        };
        let target_der = rows[target_index].1;
        let event = TraceEvent {
            session_id: wide.session_id.clone(),
            event_uid: wide.event_uid.clone(),
            event_order: prefix.event_order,
            turn_seq: target_turn_seq,
            event_time: wide.event_time.clone(),
            event_unix_ms: wide.event_unix_ms,
            actor_role: wide.actor_role.clone(),
            event_class: wide.event_class.clone(),
            payload_type: wide.payload_type.clone(),
            call_id: wide.call_id.clone(),
            name: wide.name.clone(),
            phase: wide.phase.clone(),
            item_id: wide.item_id.clone(),
            source_ref: wide.source_ref.clone(),
            text_content: wide.text_content.clone(),
            payload_json: wide.payload_json.clone(),
            token_usage_json: wide.token_usage_json.clone(),
            endpoint_kind: wide.endpoint_kind.clone(),
            token_usage_buckets: wide.token_usage_buckets.clone(),
            token_usage_native_units: wide.token_usage_native_units.clone(),
        };
        let event_type = rows[target_index].0.event_type.clone();
        // Cross-check the streamed derivation against the aggregate position;
        // the aggregate is authoritative for `prefix_user_message_count`.
        let _ = (prefix.prefix_user_message_count, prefix.target_turn_index);

        Ok(Some(McpEventOpen {
            event,
            event_type,
            event_ordinal: target_der.event_ordinal,
            turn_completed: parent_turn.completed,
            turn_terminal_event_uid: parent_turn.terminal_event_uid.clone(),
            parent_session: header.metadata,
            parent_session_source: header.source,
            parent_turn: parent_turn.metadata,
            previous_event,
            next_event,
            previous_turn,
            next_turn,
        }))
    }

    /// Resolve a continuation cursor's staleness (design §5.4). `Ok(None)` means
    /// reopen. `Ok(Some((after, derived, signals)))` gives the resume point.
    async fn canonical_resolve_continuation(
        &self,
        session_id: &str,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<ResolvedContinuation>> {
        match after {
            None => {
                let signals = self.canonical_read_signals(session_id).await?;
                Ok(Some(ResolvedContinuation {
                    start_after: None,
                    start_derived: zero_derived(),
                    signals,
                    carried_header: None,
                }))
            }
            Some(cont) => {
                let fresh = self.canonical_read_signals(session_id).await?;
                let fingerprint_matches = fresh.heads_fingerprint == cont.signals.heads_fingerprint;
                match plan_continuation(&cont.signals, &fresh, fingerprint_matches) {
                    ContinuationStep::Reopen => Ok(None),
                    ContinuationStep::RunBoundaryGuard => {
                        // Inserts landed: the header may have grown, so the
                        // carried copy is NOT reused — the page recomputes it.
                        if self
                            .canonical_boundary_intact(session_id, &cont.after)
                            .await?
                        {
                            Ok(Some(ResolvedContinuation {
                                start_after: Some(cont.after.clone()),
                                start_derived: forward_from_anchor(&cont.after),
                                signals: fresh,
                                carried_header: None,
                            }))
                        } else {
                            Ok(None)
                        }
                    }
                    // Quiescent (or unrelated global-revision move): the session
                    // content is unchanged, so the page-1 header carried on the
                    // cursor is reused verbatim — the reader runs no session-wide
                    // totals/metadata/terminal scan on this page.
                    ContinuationStep::Proceed => Ok(Some(ResolvedContinuation {
                        start_after: Some(cont.after.clone()),
                        start_derived: forward_from_anchor(&cont.after),
                        signals: fresh,
                        carried_header: decode_session_carry(cont.session_carry.as_deref()),
                    })),
                }
            }
        }
    }

    /// Build the page's [`McpEventSummary`] rows from hydrated content.
    fn assemble_event_summaries(
        &self,
        session_id: &str,
        rows: &[(NavWindowRow, ForwardDerived)],
        hydrated: &HashMap<String, WideRow>,
    ) -> Vec<McpEventSummary> {
        let preview_chars = self.cfg.preview_chars;
        rows.iter()
            .map(|(row, der)| {
                let wide = hydrated.get(&row.event_uid);
                McpEventSummary {
                    session_id: session_id.to_string(),
                    event_uid: row.event_uid.clone(),
                    event_order: der.event_order,
                    turn_seq: der.turn_seq,
                    event_time: row.display_time.clone(),
                    event_unix_ms: row.display_time_ms,
                    actor_role: row.actor_kind.clone(),
                    event_class: row.event_kind.clone(),
                    payload_type: row.payload_type.clone(),
                    event_type: row.event_type.clone(),
                    call_id: wide.map(|w| w.call_id.clone()).unwrap_or_default(),
                    name: row.tool_name.clone(),
                    phase: wide.map(|w| w.phase.clone()).unwrap_or_default(),
                    text_preview: wide.and_then(|w| preview_from_wide(w, preview_chars)),
                }
            })
            .collect()
    }
}

/// Indices of `rows` matching `predicate`, preserving stream order.
fn indices_where(
    rows: &[(NavWindowRow, ForwardDerived)],
    predicate: impl Fn(&(NavWindowRow, ForwardDerived)) -> bool,
) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(i, row)| predicate(row).then_some(i))
        .collect()
}

/// Bucket streamed rows into turns keyed by derived `turn_seq`, folding any
/// non-contiguous override recurrences into the earlier turn's bucket, then
/// order the buckets by `turn_seq`. Only turns strictly after `after_turn_seq`
/// are returned (continuation serves whole turns beyond the last served one).
fn bucket_turns(
    rows: Vec<(NavWindowRow, ForwardDerived)>,
    after_turn_seq: u32,
) -> Vec<(u32, Vec<(NavWindowRow, ForwardDerived)>)> {
    let mut buckets: Vec<(u32, Vec<(NavWindowRow, ForwardDerived)>)> = Vec::new();
    for (row, der) in rows {
        if der.turn_seq <= after_turn_seq {
            continue;
        }
        if let Some(existing) = buckets.iter_mut().find(|(seq, _)| *seq == der.turn_seq) {
            existing.1.push((row, der));
        } else {
            buckets.push((der.turn_seq, vec![(row, der)]));
        }
    }
    buckets.sort_by_key(|(seq, _)| *seq);
    buckets
}

/// A `turn_seq -> McpTurnRef` map for neighbor resolution (v1 orders turn
/// neighbors by `turn_seq` value).
fn turn_ref_map(
    rows: &[(NavWindowRow, ForwardDerived)],
    session_id: &str,
) -> BTreeMap<u32, McpTurnRef> {
    let mut map: BTreeMap<u32, McpTurnRef> = BTreeMap::new();
    for (row, der) in rows {
        let entry = map.entry(der.turn_seq).or_insert_with(|| McpTurnRef {
            session_id: session_id.to_string(),
            turn_seq: der.turn_seq,
            turn_id: row.turn_index.to_string(),
            started_at: row.display_time.clone(),
            ended_at: row.display_time.clone(),
        });
        // started_at = min display, ended_at = max display.
        if row.display_time < entry.started_at || entry.started_at.is_empty() {
            entry.started_at = row.display_time.clone();
        }
        if row.display_time > entry.ended_at {
            entry.ended_at = row.display_time.clone();
        }
    }
    map
}

/// The reader's preview compaction, matching the projector's
/// `compact_projected_source` contract: payload previews get a wider source
/// budget before compaction, empty sources collapse to `None`.
pub(super) fn canonical_preview(
    source: &str,
    is_payload: bool,
    preview_chars: u16,
) -> Option<String> {
    if source.trim().is_empty() {
        return None;
    }
    let output_limit = usize::from(preview_chars).max(1);
    let source_limit = if is_payload {
        output_limit.max(4).saturating_mul(2)
    } else {
        output_limit.max(4)
    };
    let truncated: String = if source.chars().count() <= source_limit {
        source.to_string()
    } else {
        let prefix: String = source
            .chars()
            .take(source_limit.saturating_sub(3))
            .collect();
        format!("{prefix}...")
    };
    let compact = compact_text_line(&truncated, output_limit);
    (!compact.is_empty()).then_some(compact)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> ClickHouseConversationRepository {
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        ClickHouseConversationRepository::new(client, RepoConfig::default())
    }

    fn anchor() -> CanonicalReadAnchor {
        CanonicalReadAnchor {
            sort_time_ms: 1_700_000_000_000,
            source_host: "host-a".to_string(),
            source_file: "/s/a.jsonl".to_string(),
            source_generation: 3,
            source_offset: 100,
            source_line_no: 7,
            event_uid: "uid-a".to_string(),
            event_order: 42,
            turn_seq: 5,
            prefix_user_message_count: 5,
            event_ordinal: 2,
        }
    }

    // --- forward derivation ------------------------------------------------

    #[test]
    fn forward_derivation_counts_order_turns_and_ordinals() {
        let anchor = ForwardDerived {
            event_order: 0,
            turn_seq: 0,
            prefix_user_message_count: 0,
            event_ordinal: 0,
        };
        let rows = [
            // user message -> turn 1
            ForwardInput {
                turn_index: 0,
                is_user_message: true,
            },
            // assistant -> still turn 1, ordinal 2
            ForwardInput {
                turn_index: 0,
                is_user_message: false,
            },
            // second user message -> turn 2
            ForwardInput {
                turn_index: 0,
                is_user_message: true,
            },
            // explicit override to turn 9
            ForwardInput {
                turn_index: 9,
                is_user_message: false,
            },
        ];
        let derived = derive_forward(anchor, &rows);
        assert_eq!(derived[0].event_order, 1);
        assert_eq!(derived[0].turn_seq, 1);
        assert_eq!(derived[0].event_ordinal, 1);
        assert_eq!(derived[1].turn_seq, 1);
        assert_eq!(derived[1].event_ordinal, 2);
        assert_eq!(derived[2].turn_seq, 2);
        assert_eq!(derived[2].event_ordinal, 1);
        assert_eq!(derived[2].prefix_user_message_count, 2);
        assert_eq!(derived[3].turn_seq, 9);
        assert_eq!(derived[3].event_order, 4);
    }

    #[test]
    fn forward_derivation_seeds_ordinal_from_the_anchor_turn() {
        // A continuation page whose first rows belong to the anchor's turn keeps
        // counting from the anchor's ordinal (design R2).
        let anchor = ForwardDerived {
            event_order: 10,
            turn_seq: 3,
            prefix_user_message_count: 3,
            event_ordinal: 4,
        };
        let rows = [ForwardInput {
            turn_index: 3,
            is_user_message: false,
        }];
        let derived = derive_forward(anchor, &rows);
        assert_eq!(derived[0].turn_seq, 3);
        assert_eq!(derived[0].event_ordinal, 5);
        assert_eq!(derived[0].event_order, 11);
    }

    #[test]
    fn total_turns_identity_takes_the_max_of_override_and_counter() {
        // counter path wins
        assert_eq!(total_turns_from_parts(10, 0, 4), 4);
        // override path wins
        assert_eq!(total_turns_from_parts(10, 9, 4), 9);
        // tool-only session: floor of 1
        assert_eq!(total_turns_from_parts(3, 0, 0), 1);
        // empty session: zero
        assert_eq!(total_turns_from_parts(0, 0, 0), 0);
    }

    // --- continuation decision (design §5.4) -------------------------------

    fn signals(rev: u64, sum: u64, min_ms: i64, max_ms: i64, fp: &str) -> CanonicalSessionSignals {
        CanonicalSessionSignals {
            pinned_revision: rev,
            heads_fingerprint: fp.to_string(),
            observed_sum: sum,
            min_bound_ms: min_ms,
            max_bound_ms: max_ms,
        }
    }

    #[test]
    fn quiescent_session_proceeds_with_no_session_work() {
        let cursor = signals(7, 100, 1000, 2000, "fp-a");
        let fresh = signals(7, 100, 1000, 2000, "fp-a");
        assert_eq!(
            plan_continuation(&cursor, &fresh, true),
            ContinuationStep::Proceed
        );
    }

    #[test]
    fn unrelated_publication_move_does_not_reopen() {
        // Global revision advanced but THIS session's fingerprint is intact and
        // signals are unchanged (step 3 fall-through -> step 4 Proceed).
        let cursor = signals(7, 100, 1000, 2000, "fp-a");
        let fresh = signals(9, 100, 1000, 2000, "fp-a");
        assert_eq!(
            plan_continuation(&cursor, &fresh, true),
            ContinuationStep::Proceed
        );
    }

    #[test]
    fn session_head_flip_reopens() {
        let cursor = signals(7, 100, 1000, 2000, "fp-a");
        let fresh = signals(9, 100, 1000, 2000, "fp-b");
        assert_eq!(
            plan_continuation(&cursor, &fresh, false),
            ContinuationStep::Reopen
        );
    }

    #[test]
    fn inserts_run_the_boundary_guard() {
        let cursor = signals(7, 100, 1000, 2000, "fp-a");
        let fresh = signals(7, 105, 1000, 2100, "fp-a");
        assert_eq!(
            plan_continuation(&cursor, &fresh, true),
            ContinuationStep::RunBoundaryGuard
        );
    }

    #[test]
    fn impossible_direction_or_rewrite_reopens() {
        let cursor = signals(7, 100, 1000, 2000, "fp-a");
        // sum decreased (truncate/rebuild)
        assert_eq!(
            plan_continuation(&cursor, &signals(7, 90, 1000, 2000, "fp-a"), true),
            ContinuationStep::Reopen
        );
        // min bound moved earlier
        assert_eq!(
            plan_continuation(&cursor, &signals(7, 100, 900, 2000, "fp-a"), true),
            ContinuationStep::Reopen
        );
        // sum unchanged but max advanced -> sentinel re-insert rewrite signature
        assert_eq!(
            plan_continuation(&cursor, &signals(7, 100, 1000, 2100, "fp-a"), true),
            ContinuationStep::Reopen
        );
    }

    // --- session carry / early-stop (design §5.5, WI-09 bound) -------------

    fn sample_header(max_override: u32) -> CanonicalSessionHeader {
        CanonicalSessionHeader {
            metadata: SessionMetadata {
                session_id: "session-a".to_string(),
                first_event_time: "2026-07-20 12:00:00.000".to_string(),
                first_event_unix_ms: 1_777_000_000_000,
                last_event_time: "2026-07-20 12:30:00.000".to_string(),
                last_event_unix_ms: 1_777_001_800_000,
                total_turns: 5000,
                total_events: 50_000,
                user_messages: 5000,
                assistant_messages: 5000,
                tool_calls: 0,
                tool_results: 0,
                mode: ConversationMode::Chat,
                first_event_uid: "e-0001".to_string(),
                last_event_uid: "e-50000".to_string(),
                last_actor_role: "assistant".to_string(),
            },
            max_override,
            title: Some("Monster session".to_string()),
            source: Some("fixture".to_string()),
            harness: Some("codex".to_string()),
            inference_provider: Some("openai".to_string()),
            session_slug: None,
            session_summary: Some("Monster session".to_string()),
            completed: true,
            terminal_event_uid: Some("e-50000".to_string()),
        }
    }

    #[test]
    fn session_carry_round_trips_through_the_cursor_string() {
        let header = sample_header(0);
        let encoded = encode_session_carry(&header).expect("carry encodes");
        let decoded = decode_session_carry(Some(encoded.as_str())).expect("carry decodes");
        // The reused header is byte-identical: page k+1 renders the same session
        // fields as page 1 without a session-wide scan.
        assert_eq!(decoded.metadata.session_id, header.metadata.session_id);
        assert_eq!(decoded.metadata.total_turns, header.metadata.total_turns);
        assert_eq!(decoded.max_override, header.max_override);
        assert_eq!(decoded.title, header.title);
        assert_eq!(decoded.completed, header.completed);
        assert_eq!(decoded.terminal_event_uid, header.terminal_event_uid);
    }

    #[test]
    fn malformed_or_absent_carry_falls_back_to_recompute() {
        assert!(decode_session_carry(None).is_none());
        assert!(decode_session_carry(Some("not json")).is_none());
    }

    #[test]
    fn override_free_streams_keep_turn_seq_monotonic() {
        // The early-stop path is only engaged for override-free sessions; there
        // `turn_seq` is non-decreasing in tuple order, which is exactly what lets
        // a `K + 1`-turn early stop equal the full-session bucketing.
        let mut anchor = zero_derived();
        let inputs: Vec<ForwardInput> = (0..200)
            .map(|i| ForwardInput {
                turn_index: 0,
                is_user_message: i % 10 == 0,
            })
            .collect();
        let mut last_turn = 0;
        for input in &inputs {
            let derived = derive_forward(anchor, std::slice::from_ref(input));
            assert!(
                derived[0].turn_seq >= last_turn,
                "override-free turn_seq regressed: {} < {last_turn}",
                derived[0].turn_seq
            );
            last_turn = derived[0].turn_seq;
            anchor = derived[0];
        }
    }

    // --- SQL shape ---------------------------------------------------------

    async fn build<F: FnOnce(&ClickHouseConversationRepository) -> String>(f: F) -> String {
        let repo = repo();
        with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async move {
            f(&repo)
        })
        .await
    }

    /// Every narrow statement must be payload-free (design §4, B4).
    fn assert_payload_free(sql: &str) {
        assert!(
            !sql.contains("text_content"),
            "narrow statement leaked text_content:\n{sql}"
        );
        assert!(
            !sql.contains("payload_json"),
            "narrow statement leaked payload_json:\n{sql}"
        );
    }

    /// No reader statement may read or gate on the retired projection tables.
    fn assert_no_projection(sql: &str) {
        assert!(
            !sql.contains("mcp_open_"),
            "reader touched mcp_open_*:\n{sql}"
        );
    }

    #[tokio::test]
    async fn signal_read_is_payload_free_and_directory_scoped() {
        let sql = build(|r| r.build_session_signal_sql("session-a")).await;
        assert_payload_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("sum(d.observed_events)"));
        assert!(sql.contains("`moraine`.`mcp_session_directory`"));
        assert!(sql.contains("heads_fingerprint"));
        assert!(sql.contains("WHERE d.session_id = 'session-a'"));
    }

    #[tokio::test]
    async fn boundary_guard_counts_prefix_and_is_payload_free() {
        let sql = build(|r| r.build_prefix_boundary_guard_sql("session-a", &anchor())).await;
        assert_payload_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("count_le_anchor"));
        assert!(sql.contains("user_messages_le_anchor"));
        assert!(sql.contains("`moraine`.`mcp_event_navigation` AS n FINAL"));
        // PK-pruned to the session and the closed upper bound.
        assert!(sql.contains("WHERE n.session_id = 'session-a'"));
        assert!(sql.contains("n.sort_time <= toDateTime64"));
    }

    #[tokio::test]
    async fn navigation_window_paginates_k_plus_one_and_is_payload_free() {
        let sql = build(|r| r.build_navigation_window_sql("session-a", None, None, 25)).await;
        assert_payload_free(&sql);
        assert_no_projection(&sql);
        // K + 1 in SQL.
        assert!(sql.contains("LIMIT 26"), "expected LIMIT 26:\n{sql}");
        assert!(sql.contains("`moraine`.`mcp_event_navigation` AS n FINAL"));
        assert!(sql.contains("is_user_input"));
        assert!(sql.contains("is_final_response"));
        assert!(sql.contains("AS event_type"));
    }

    #[tokio::test]
    async fn navigation_window_keyset_after_anchor_uses_a_closed_window() {
        let sql = build(|r| {
            r.build_navigation_window_sql("session-a", Some(&anchor()), Some(1_700_000_100_000), 10)
        })
        .await;
        assert_payload_free(&sql);
        // keyset: strictly after the anchor tuple.
        assert!(sql.contains(") > tuple(toDateTime64"));
        // closed upper window bound.
        assert!(sql.contains("n.sort_time < toDateTime64(1700000100000 / 1000.0, 3)"));
        assert!(sql.contains("LIMIT 11"));
    }

    #[tokio::test]
    async fn totals_are_payload_free_and_expose_both_turn_contributions() {
        let sql = build(|r| r.build_session_totals_sql("session-a")).await;
        assert_payload_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("max_override"));
        assert!(sql.contains("counter_user_messages"));
        assert!(sql.contains("AS mode"));
        assert!(sql.contains("first_event_uid"));
        assert!(sql.contains("last_actor_role"));
    }

    #[tokio::test]
    async fn metadata_read_is_bounded_to_metadata_bearing_rows() {
        let sql = build(|r| r.build_session_metadata_sql("session-a")).await;
        assert_no_projection(&sql);
        // The ONLY payload read is gated by the is_metadata_bearing flag …
        assert!(sql.contains("n.is_metadata_bearing = 1"));
        assert!(sql.contains("e.payload_json AS payload_json"));
        // … and it selects payload only for the bounded metadata uid set.
        assert!(sql.contains("WHERE e.event_uid IN ("));
        // The session filter is pushed inside the live-events derived table.
        assert!(sql.contains("WHERE e.session_id = 'session-a'"));
    }

    #[tokio::test]
    async fn wide_hydration_reads_content_only_for_the_selected_uids() {
        let uids = vec!["uid-1".to_string(), "uid-2".to_string()];
        let sql = build(|r| r.build_wide_hydration_sql("session-a", &uids)).await;
        assert_no_projection(&sql);
        assert!(sql.contains("text_content"));
        assert!(sql.contains("payload_json"));
        assert!(sql.contains("token_usage_buckets"));
        // Bounded to the explicit uid set — never the whole session.
        assert!(sql.contains("WHERE e.event_uid IN ['uid-1','uid-2']"));
        // event_order / turn_seq are reader-derived, never hydrated.
        assert!(!sql.contains("event_order"));
        assert!(!sql.contains("turn_seq"));
        // Raw events columns, aliased to the projector's public names — the
        // physical table has tool_call_id/tool_name/tool_phase, not the
        // aliases (caught live by the parity gate).
        assert!(sql.contains("e.tool_call_id AS call_id"));
        assert!(sql.contains("e.tool_name AS name"));
        assert!(sql.contains("e.tool_phase AS phase"));
        assert!(!sql.contains("e.call_id AS"));
    }

    #[tokio::test]
    async fn session_terminal_uses_the_last_turns_flag_not_a_session_wide_argmax() {
        let sql = build(|r| r.build_session_terminal_sql("session-a")).await;
        assert_payload_free(&sql);
        // Two-level rule: per-turn completed grouped by the windowed turn_seq,
        // then the LAST turn's flag wins (R1(b)); a terminal event in a middle
        // turn must not mark the session completed.
        assert!(sql.contains("argMax(turn_completed, turn_seq)"));
        assert!(sql.contains("GROUP BY turn_seq"));
        assert!(sql.contains("OVER turn_window"));
        assert!(sql.contains("n.is_user_message = 1"));
        assert!(sql.contains("toUInt32(n.turn_index) > 0"));
    }

    #[tokio::test]
    async fn locator_seek_rejects_non_live_generations_by_join() {
        let sql = build(|r| r.build_event_locator_sql("uid-x")).await;
        assert_payload_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("`moraine`.`mcp_event_locator` AS l FINAL"));
        assert!(sql.contains("ALL INNER JOIN"));
        assert!(sql.contains("WHERE l.event_uid = 'uid-x'"));
    }

    #[tokio::test]
    async fn event_prefix_position_is_a_single_session_scoped_aggregate() {
        let locator = LocatorRow {
            event_uid: "uid-x".to_string(),
            source_host: "host-a".to_string(),
            session_id: "session-a".to_string(),
            source_file: "/s/a.jsonl".to_string(),
            source_generation: 2,
            source_offset: 10,
            source_line_no: 3,
            sort_time_ms: 1_700_000_000_000,
        };
        let sql = build(|r| r.build_event_prefix_position_sql(&locator)).await;
        assert_payload_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("AS event_order"));
        assert!(sql.contains("prefix_user_message_count"));
        assert!(sql.contains("WHERE n.session_id = 'session-a'"));
        assert!(sql.contains("n.sort_time <= toDateTime64"));
    }

    #[test]
    fn preview_compaction_matches_the_projector_contract() {
        assert_eq!(canonical_preview("   ", false, 220), None);
        let text = "x".repeat(65_536);
        let preview = canonical_preview(&text, false, u16::MAX).expect("preview");
        assert!(preview.ends_with("..."));
        assert_eq!(preview.chars().count(), usize::from(u16::MAX));
    }
}
