//! Shared canonical derivation SQL fragments.
//!
//! Every interactive read of a Moraine session — the legacy `mcp_open_*`
//! projector, the migration-036 materialized views, and the issue-598 v2
//! canonical reader — must agree on how a session's public ordering, turns,
//! summaries, mode, and session metadata are derived from canonical `events`
//! rows. This module is the single authority for those derivation expressions
//! so the three consumers cannot drift.
//!
//! ## Column naming
//!
//! The projector aliases the physical `events` columns inside its `canonical`
//! CTE (`event_kind AS event_class`, `actor_kind AS actor_role`,
//! `tool_name AS name`, `tool_call_id AS call_id`), so its derivation SQL reads
//! the aliased names. Migration 036 MV bodies and the v2 reader read the
//! physical `events` columns directly (see `sql/001_schema.sql:42-43`). Every
//! fragment is therefore a pure function of a [`DerivationColumns`] set: pass
//! [`DerivationColumns::PROJECTOR`] to reproduce the projector's byte-identical
//! SQL, or [`DerivationColumns::EVENTS`] for the raw `events` column names.
//!
//! ## Whitespace
//!
//! The projector builds its SQL with `\n\` string continuations, which Rust
//! strips to a left-aligned, newline-delimited statement. Fragments here return
//! left-aligned, `\n`-joined text so they slot into the projector's generated
//! SQL with zero byte diff (pinned by the byte-identity snapshot test in
//! `mcp_open_projection`). ClickHouse ignores this whitespace, so MV/reader
//! consumers may embed the same fragments wherever convenient.

use crate::mcp_tool_names;

/// SQL-side cap on a projected/hydrated `text_content` summary
/// (`projection.rs` historical value). Shared so the v2 reader's wide-column
/// hydration matches the projector's preview contract.
pub const MAX_PROJECTED_TEXT_SUMMARY_CHARS: usize = 65_536;

/// SQL-side cap on a projected/hydrated `payload_json` summary.
pub const MAX_PROJECTED_PAYLOAD_SUMMARY_CHARS: usize = 131_071;

/// The `events`-derived column names a derivation fragment reads.
///
/// See the module docs: the projector reads aliased names, the 036 MVs and v2
/// reader read the physical `events` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivationColumns {
    /// Actor kind column (`actor_role` aliased / `actor_kind` physical).
    pub actor: &'static str,
    /// Event kind column (`event_class` aliased / `event_kind` physical).
    pub event_kind: &'static str,
    /// Tool name column (`name` aliased / `tool_name` physical).
    pub tool_name: &'static str,
    /// Tool call id column (`call_id` aliased / `tool_call_id` physical).
    pub tool_call_id: &'static str,
}

impl DerivationColumns {
    /// The projector's aliased column names. Fragments built with this set are
    /// byte-identical to the current `mcp_open_*` projection SQL.
    pub const PROJECTOR: Self = Self {
        actor: "actor_role",
        event_kind: "event_class",
        tool_name: "name",
        tool_call_id: "call_id",
    };

    /// The physical `events` table column names (`sql/001_schema.sql:42-43`),
    /// for migration 036 MV bodies and the v2 canonical reader.
    pub const EVENTS: Self = Self {
        actor: "actor_kind",
        event_kind: "event_kind",
        tool_name: "tool_name",
        tool_call_id: "tool_call_id",
    };
}

// --- Ordering / time keys -------------------------------------------------

/// v1 display/aggregate time expression: CH-parsed `record_ts`, else the
/// non-deterministic `ingested_at` insert-clock fallback. Authoritative for the
/// projector's `event_time`, for response display, and for directory window
/// bounds. NOT stable across re-inserts of a malformed-`record_ts` row.
pub const DISPLAY_TIME_EXPR: &str =
    "ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)";

/// Deterministic v2 navigation order key: CH-parsed `record_ts`, else the epoch
/// sentinel. A pure function of the record bytes (never `ingested_at`), so a
/// re-insert or re-normalization of the same `event_uid` yields an identical
/// key and ReplacingMergeTree collapses it — no ghost rows. Diverges from
/// [`DISPLAY_TIME_EXPR`] only for rows whose `record_ts` fails BestEffort parse
/// (they sort at the session start instead of their v1 insert-arrival slot).
/// Not consumed by the projector; the 036 navigation index and v2 reader use it.
pub const SORT_TIME_EXPR: &str =
    "ifNull(parseDateTime64BestEffortOrNull(record_ts), toDateTime64('1970-01-01 00:00:00', 3))";

/// The event-order tie-break columns that follow the time key in the
/// authoritative session ordering tuple (projector window key).
pub const ORDER_KEY_TAIL: &str =
    "source_host, source_file, source_generation, source_offset, source_line_no, event_uid";

/// The v1 event-order window `ORDER BY` key: display time then tie-breaks.
/// This is the projector's `canonical_window`/`canonical_rows` order.
pub fn event_order_window_key() -> String {
    format!("{DISPLAY_TIME_EXPR}, {ORDER_KEY_TAIL}")
}

/// The deterministic v2 navigation `ORDER BY` key: [`SORT_TIME_EXPR`] then
/// tie-breaks. Used by the 036 navigation index PK and the v2 reader; not the
/// projector.
pub fn sort_time_window_key() -> String {
    format!("{SORT_TIME_EXPR}, {ORDER_KEY_TAIL}")
}

// --- User-message predicate variants (R1(c)) ------------------------------

/// The case-sensitive user-message counting predicate
/// (`<actor> = 'user' AND <event_kind> = 'message'`).
///
/// This is the projector's `turn_seq` counter predicate and its per-turn /
/// per-session `user_messages` count predicate. Built with
/// [`DerivationColumns::EVENTS`] it is the 036 navigation index's
/// `is_user_message` flag (per the design's VERIFIER ADDENDUM item 3, the MV
/// uses the real `actor_kind`/`event_kind` columns).
pub fn user_message_count_predicate(cols: DerivationColumns) -> String {
    format!(
        "{} = 'user' AND {} = 'message'",
        cols.actor, cols.event_kind
    )
}

/// The projector's windowed `turn_seq`: explicit `turn_index` override, else
/// the running count of user-message rows at-or-before this row (floored at 1),
/// evaluated over the named running-count window.
pub fn turn_seq_windowed_expr(cols: DerivationColumns, window: &str) -> String {
    format!(
        "if(toUInt32(turn_index) > 0, toUInt32(turn_index), greatest(toUInt32(1), toUInt32(sum(if({predicate}, 1, 0)) OVER {window})))",
        predicate = user_message_count_predicate(cols),
    )
}

/// The lowerUTF8 turn-summary message-class predicates (R1(c) variant 2).
///
/// These select the user-input and final-response events whose summaries a turn
/// exposes. Note the payload-type list here **omits** the `'event_msg'` sentinel
/// that [`event_type_expr`] includes — a deliberate v1 divergence preserved for
/// parity.
pub struct TurnSummaryPredicates {
    /// The message-class predicate (`message` local in the projector).
    pub message: String,
    /// User-input predicate: a user actor emitting a message-class event.
    pub user_input: String,
    /// Assistant predicate: an assistant actor emitting a message-class event.
    pub assistant: String,
    /// Final-response predicate: an assistant message that is not commentary.
    pub final_response: String,
}

impl TurnSummaryPredicates {
    /// Build the turn-summary predicates for the given column set.
    pub fn new(cols: DerivationColumns) -> Self {
        let ek = cols.event_kind;
        let actor = cols.actor;
        let message = format!(
            "({ek} = 'message' OR ({ek} = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))"
        );
        let user_input = format!("(lowerUTF8({actor}) = 'user' AND {message})");
        let assistant = format!("(lowerUTF8({actor}) = 'assistant' AND {message})");
        let final_response = format!("({assistant} AND lowerUTF8(phase) != 'commentary')");
        Self {
            message,
            user_input,
            assistant,
            final_response,
        }
    }
}

// --- Event classification (event_type) ------------------------------------

/// The public `event_type` classification `multiIf` (R1(c) variant 3).
///
/// Its user/assistant message arms include the `'event_msg'` payload sentinel in
/// the IN-list — the deliberate divergence from [`TurnSummaryPredicates`], kept
/// so both match v1 exactly.
pub fn event_type_expr(cols: DerivationColumns) -> String {
    let actor = cols.actor;
    let ek = cols.event_kind;
    let message = format!(
        "({ek} = 'message' OR ({ek} = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg')))"
    );
    format!(
        "multiIf(\
lowerUTF8({actor}) = 'user' AND {message}, 'user_input',\
lowerUTF8({actor}) = 'assistant' AND {message}, 'assistant_response',\
lowerUTF8({actor}) != 'system' AND ({ek} = 'reasoning' OR payload_type IN ('agent_reasoning', 'reasoning', 'thinking')), 'reasoning',\
lowerUTF8({actor}) != 'system' AND ({ek} = 'tool_call' OR payload_type IN ('tool_use', 'function_call', 'custom_tool_call', 'web_search_call')), 'tool_call',\
lowerUTF8({actor}) != 'system' AND ({ek} = 'tool_result' OR payload_type IN ('tool_result', 'function_call_output', 'custom_tool_call_output', 'search_results_received')), 'tool_response',\
{ek} IN ('compacted_raw', 'summary') OR payload_type IN ('compacted', 'summary'), 'compaction',\
{ek} = 'queue_operation' OR payload_type IN ('task_started', 'task_complete', 'turn_aborted', 'item_completed', 'queue-operation'), 'runtime',\
lowerUTF8({actor}) = 'system' OR {ek} IN ('system', 'progress', 'file_history_snapshot') OR payload_type IN ('system', 'progress', 'file-history-snapshot', 'file_history_snapshot'), 'system',\
'unknown')"
    )
}

// --- Conversation mode ----------------------------------------------------

fn mode_web_search_predicate(cols: DerivationColumns) -> String {
    format!(
        "payload_type = 'web_search_call' OR payload_type = 'search_results_received' OR (payload_type = 'tool_use' AND {tn} IN ('WebSearch', 'WebFetch'))",
        tn = cols.tool_name,
    )
}

fn mode_mcp_internal_predicate(cols: DerivationColumns) -> String {
    format!(
        "source_name = 'codex-mcp' OR {}",
        mcp_tool_names::sql_predicate(cols.tool_name),
    )
}

fn mode_tool_calling_predicate(cols: DerivationColumns) -> String {
    format!(
        "{ek} IN ('tool_call', 'tool_result') OR payload_type = 'tool_use'",
        ek = cols.event_kind,
    )
}

/// The session-mode aggregate `multiIf` (presence-ranked over a session's
/// rows). Reproduces the projector header's `mode` expression and the
/// `helpers::mode_aggregate_sql` list/search expression from one authority.
pub fn mode_aggregate_expr(cols: DerivationColumns) -> String {
    format!(
        "multiIf(\n\
countIf({web}) > 0, 'web_search',\n\
countIf({mcp}) > 0, 'mcp_internal',\n\
countIf({tool}) > 0, 'tool_calling', 'chat')",
        web = mode_web_search_predicate(cols),
        mcp = mode_mcp_internal_predicate(cols),
        tool = mode_tool_calling_predicate(cols),
    )
}

/// The per-event mode rank (3=web_search, 2=mcp_internal, 1=tool_calling,
/// 0=chat) from the same per-event tests as [`mode_aggregate_expr`]. Consumed
/// by the 036 `mcp_session_directory` MV as `mode_hint`; `max(rank)` over a
/// session equals the presence-ranked aggregate outcome. Not used by the
/// projector.
pub fn mode_rank_expr(cols: DerivationColumns) -> String {
    format!(
        "multiIf({web}, 3, {mcp}, 2, {tool}, 1, 0)",
        web = mode_web_search_predicate(cols),
        mcp = mode_mcp_internal_predicate(cols),
        tool = mode_tool_calling_predicate(cols),
    )
}

// --- Turn aggregate folds & neighbors -------------------------------------

/// The shared turn-row aggregate fold columns, from `turn_seq` through
/// `event_summaries_json`, that both the single-session and batch projector
/// turn INSERTs group by `(session_id, turn_seq)`.
///
/// argMin/argMaxIf folds key on `tuple(event_order, event_uid)`; summary
/// selection uses the [`TurnSummaryPredicates`]; per-class counts use the
/// case-sensitive [`user_message_count_predicate`] and raw event-kind tests.
/// `turn_id` uses the v1 `anyIf(turn_id, turn_id != '')` rule — see
/// [`deterministic_turn_id_expr`] for the v2 reader's deterministic variant.
pub fn turn_fold_columns(cols: DerivationColumns, predicates: &TurnSummaryPredicates) -> String {
    let ek = cols.event_kind;
    let umsg = user_message_count_predicate(cols);
    let assistant_msg = format!("{} = 'assistant' AND {ek} = 'message'", cols.actor);
    let user_message = &predicates.user_input;
    let final_response_message = &predicates.final_response;
    format!(
        "turn_seq, anyIf(turn_id, turn_id != '') AS turn_id,\n\
min(event_time) AS started_at, max(event_time) AS ended_at, count() AS total_events,\n\
countIf({umsg}) AS user_messages,\n\
countIf({assistant_msg}) AS assistant_messages,\n\
countIf({ek} = 'tool_call') AS tool_calls, countIf({ek} = 'tool_result') AS tool_results,\n\
countIf({ek} = 'reasoning') AS reasoning_items,\n\
argMinIf(summary_source, tuple(event_order, event_uid), {user_message}) AS user_input_summary_source,\n\
argMaxIf(summary_source, tuple(event_order, event_uid), {final_response_message}) AS final_response_summary_source,\n\
argMinIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {user_message}) AS user_input_summary_is_payload,\n\
argMaxIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {final_response_message}) AS final_response_summary_is_payload,\n\
argMinIf(event_uid, tuple(event_order, event_uid), {user_message}) AS user_input_event_uid,\n\
argMinIf(event_order, tuple(event_order, event_uid), {user_message}) AS user_input_event_order,\n\
argMinIf(event_time, tuple(event_order, event_uid), {user_message}) AS user_input_event_time,\n\
argMinIf(event_type, tuple(event_order, event_uid), {user_message}) AS user_input_event_type,\n\
argMaxIf(event_uid, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_uid,\n\
argMaxIf(event_order, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_order,\n\
argMaxIf(event_time, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_time,\n\
argMaxIf(event_type, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_type,\n\
arrayDistinct(arrayMap(x -> x.2, arraySort(groupArrayIf(tuple(event_order, tool_label), event_type = 'tool_call' AND tool_label != '')))) AS tools_called,\n\
arrayDistinct(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_type))))) AS normalized_event_types,\n\
argMaxIf(toUInt8(payload_type = 'task_complete'), tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS completed,\n\
argMaxIf(event_uid, tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS terminal_event_uid,\n\
argMin(event_uid, tuple(event_order, event_uid)) AS first_event_uid,\n\
argMin(event_order, tuple(event_order, event_uid)) AS first_event_order,\n\
argMin(event_time, tuple(event_order, event_uid)) AS first_event_time,\n\
argMin(event_type, tuple(event_order, event_uid)) AS first_event_type,\n\
argMax(event_uid, tuple(event_order, event_uid)) AS last_event_uid,\n\
argMax(event_order, tuple(event_order, event_uid)) AS last_event_order,\n\
argMax(event_time, tuple(event_order, event_uid)) AS last_event_time,\n\
argMax(event_type, tuple(event_order, event_uid)) AS last_event_type,\n\
toJSONString(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_summary))))) AS event_summaries_json"
    )
}

/// The `turn_neighbors` CTE: prev/next turn refs ordered by `turn_seq` **value**
/// (R1's turn-neighbor semantics), shared by both projector turn INSERTs. Column
/// independent (operates on already-derived turn rows).
pub fn turn_neighbors_cte() -> String {
    "turn_neighbors AS (\n\
SELECT *,\n\
lagInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS previous_turn_seq,\n\
lagInFrame(turn_id, 1, '') OVER turn_window AS previous_turn_id,\n\
lagInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_started_at,\n\
lagInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_ended_at,\n\
leadInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS next_turn_seq,\n\
leadInFrame(turn_id, 1, '') OVER turn_window AS next_turn_id,\n\
leadInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_started_at,\n\
leadInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_ended_at\n\
FROM turn_rows\n\
WINDOW turn_window AS (PARTITION BY session_id ORDER BY turn_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n\
)"
    .to_string()
}

// --- Session-header rollup ------------------------------------------------

/// The enumerated metadata-bearing predicate (design §4): a row carries session
/// metadata iff it is a `session_meta` event or an omp title/title_change
/// payload. Consumed by the 036 navigation MV's `is_metadata_bearing` flag and
/// the v2 reader's bounded metadata fetch. The projector's `latest_metadata_*`
/// argMaxIf conditions embed the same logic (see [`session_header_rollup_columns`]).
pub fn is_metadata_bearing_predicate(cols: DerivationColumns) -> String {
    format!(
        "{ek} = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))",
        ek = cols.event_kind,
    )
}

/// The projector-formatted metadata-bearing predicate: identical logic to
/// [`is_metadata_bearing_predicate`] but with the newline the projector's
/// generated SQL places before the `OR` arm, so the header rollup stays
/// byte-identical.
fn metadata_bearing_predicate_projector_form(cols: DerivationColumns) -> String {
    is_metadata_bearing_predicate(cols).replacen(
        " OR (source_name = 'omp'",
        "\nOR (source_name = 'omp'",
        1,
    )
}

/// The session-header scalar rollup columns, from `min(event_time) AS
/// first_event_time` through `... AS origin_cwd`, in the exact order the
/// projector's `mcp_open_publication_headers` header CTE selects them.
///
/// Composes: session counts, the [`mode_aggregate_expr`], the first/last/
/// last_actor precedence key `tuple(event_time, event_order, event_uid)`
/// (R1(a): equivalent to the projector's third key because `event_order` is
/// monotone in the ordering tuple), and the OPEN+LIST metadata-precedence
/// columns (latest_metadata_*, latest_session_meta_*, omp dispatch title,
/// source/harness/inference, session_slug, origin_cwd argMinIf).
pub fn session_header_rollup_columns(cols: DerivationColumns) -> String {
    let ek = cols.event_kind;
    let actor = cols.actor;
    let umsg = user_message_count_predicate(cols);
    let assistant_msg = format!("{actor} = 'assistant' AND {ek} = 'message'");
    let mode = mode_aggregate_expr(cols);
    let metadata_bearing = metadata_bearing_predicate_projector_form(cols);
    format!(
        "min(event_time) AS first_event_time,\n\
max(event_time) AS last_event_time, toUInt32(max(turn_seq)) AS total_turns, count() AS total_events,\n\
countIf({umsg}) AS user_messages,\n\
countIf({assistant_msg}) AS assistant_messages,\n\
countIf({ek} = 'tool_call') AS tool_calls, countIf({ek} = 'tool_result') AS tool_results,\n\
{mode} AS mode,\n\
argMin(event_uid, tuple(event_time, event_order, event_uid)) AS first_event_uid,\n\
argMax(event_uid, tuple(event_time, event_order, event_uid)) AS last_event_uid,\n\
argMax({actor}, tuple(event_time, event_order, event_uid)) AS last_actor_role,\n\
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''),\n\
tuple(event_ts, event_uid), {metadata_bearing}), '') AS latest_metadata_title,\n\
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''),\n\
tuple(event_ts, event_uid), {ek} = 'session_meta'), '') AS latest_metadata_name,\n\
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'summary'), ''),\n\
tuple(event_ts, event_uid), {ek} = 'session_meta'), '') AS latest_metadata_summary,\n\
ifNull(argMaxIf(coalesce(nullIf(JSONExtractString(payload_json, 'title'), ''), nullIf(JSONExtractString(payload_json, 'name'), ''), nullIf(JSONExtractString(payload_json, 'summary'), '')),\n\
tuple(event_ts, event_uid), {ek} = 'session_meta'), '') AS latest_session_meta_title,\n\
ifNull(argMaxIf(coalesce(nullIf(JSONExtractString(payload_json, 'summary'), ''), nullIf(JSONExtractString(payload_json, 'title'), ''), nullIf(JSONExtractString(payload_json, 'name'), '')),\n\
tuple(event_ts, event_uid), {ek} = 'session_meta'), '') AS latest_session_meta_summary,\n\
ifNull(argMinIf(nullIf(trimBoth(replaceRegexpOne(arrayElement(splitByChar('/', replaceAll(source_file, '\\\\', '/')), -1), '[.]jsonl$', '')), ''),\n\
tuple(event_ts, event_uid), source_name = 'omp' AND notEmpty(session_id)\n\
AND endsWith(source_file, '.jsonl')\n\
AND NOT endsWith(source_file, concat(session_id, '.jsonl'))), '') AS omp_dispatch_title,\n\
ifNull(argMax(nullIf(source_name, ''), tuple(event_ts, event_uid)), '') AS source,\n\
ifNull(argMax(nullIf(harness, ''), tuple(event_ts, event_uid)), '') AS harness,\n\
ifNull(argMax(nullIf(inference_provider, ''), tuple(event_ts, event_uid)), '') AS inference_provider,\n\
ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid), {ek} = 'session_meta'), '') AS session_slug,\n\
ifNull(argMinIf(cwd, tuple(event_ts, event_uid), cwd != ''), '') AS origin_cwd"
    )
}

/// The session completed/terminal selection columns (R1(b)):
/// `argMax(completed, turn_seq)` and `argMax(terminal_event_uid, turn_seq)` —
/// the terminal semantics of the max-`turn_seq` turn. Consumers MUST restrict to
/// real turns (`turn_seq > 0`) before aggregating; the projector applies that
/// filter in its `terminal` CTE's `WHERE`, and the v2 reader applies it over its
/// derived turn rows.
pub fn session_terminal_selection_columns() -> &'static str {
    "argMax(completed, turn_seq) AS completed, argMax(terminal_event_uid, turn_seq) AS terminal_event_uid"
}

// --- v2-reader-only derivations (not consumed by the projector) ----------

/// The v2 reader's deterministic `turn_id` rule (R1(d)): a turn's id is the
/// `toString(turn_index)` of the member row selected by the minimum
/// `(event_order, event_uid)`. Deterministic even when a turn mixes
/// `turn_index` values, unlike v1's `anyIf(turn_id, turn_id != '')`
/// (nondeterministic within the turn's value set). Not used by the projector.
pub fn deterministic_turn_id_expr() -> String {
    "argMin(toString(turn_index), tuple(event_order, event_uid))".to_string()
}

/// The v2 reader's `total_turns` identity: `max(maxO, if(any rows, max(1, U), 0))`,
/// where `maxO = max(turn_index)` (the override-path contribution) and
/// `U = countIf(is_user_message)` (the counter-path contribution).
///
/// Proof: counter-path `turn_seq` is nondecreasing with maximum `max(1, U)`;
/// override-path maximum is `maxO`; the total is the max of both — matching the
/// projector's `toUInt32(max(turn_seq))`. VERIFIER ADDENDUM item 1: the identity
/// holds only when `max(1, U_total)` is attained by a counter-path row (or
/// `maxO >= U`); the reader computes the counter contribution as
/// `max(1, U at the last turn_index=0 row)` via one extra tiny aggregate so a
/// trailing overridden user message cannot inflate the count. Documented here as
/// the shared authority; the projector uses `toUInt32(max(turn_seq))` directly.
pub fn total_turns_identity_expr(max_override_expr: &str, user_message_count_expr: &str) -> String {
    format!(
        "greatest({max_override_expr}, if(count() > 0, greatest(toUInt32(1), {user_message_count_expr}), toUInt32(0)))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projector_and_events_column_sets_differ_only_in_aliases() {
        assert_eq!(DerivationColumns::PROJECTOR.actor, "actor_role");
        assert_eq!(DerivationColumns::PROJECTOR.event_kind, "event_class");
        assert_eq!(DerivationColumns::PROJECTOR.tool_name, "name");
        assert_eq!(DerivationColumns::EVENTS.actor, "actor_kind");
        assert_eq!(DerivationColumns::EVENTS.event_kind, "event_kind");
        assert_eq!(DerivationColumns::EVENTS.tool_name, "tool_name");
        assert_eq!(DerivationColumns::EVENTS.tool_call_id, "tool_call_id");
    }

    #[test]
    fn display_and_sort_time_keys_differ_only_in_the_fallback() {
        assert!(DISPLAY_TIME_EXPR.contains("ingested_at"));
        assert!(!SORT_TIME_EXPR.contains("ingested_at"));
        assert!(SORT_TIME_EXPR.contains("toDateTime64('1970-01-01 00:00:00', 3)"));
        // Both share the same tie-break tail.
        assert!(event_order_window_key().ends_with(ORDER_KEY_TAIL));
        assert!(sort_time_window_key().ends_with(ORDER_KEY_TAIL));
        assert_ne!(event_order_window_key(), sort_time_window_key());
    }

    #[test]
    fn user_message_predicate_uses_the_requested_columns() {
        assert_eq!(
            user_message_count_predicate(DerivationColumns::PROJECTOR),
            "actor_role = 'user' AND event_class = 'message'"
        );
        // VERIFIER ADDENDUM item 3: the 036 navigation is_user_message flag uses
        // the real events columns.
        assert_eq!(
            user_message_count_predicate(DerivationColumns::EVENTS),
            "actor_kind = 'user' AND event_kind = 'message'"
        );
    }

    #[test]
    fn turn_seq_expr_counts_user_messages_over_the_named_window() {
        let expr = turn_seq_windowed_expr(DerivationColumns::PROJECTOR, "canonical_rows");
        assert!(expr.contains("if(toUInt32(turn_index) > 0, toUInt32(turn_index)"));
        assert!(expr.contains(
            "sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER canonical_rows"
        ));
    }

    #[test]
    fn event_type_message_arm_includes_the_event_msg_sentinel() {
        let expr = event_type_expr(DerivationColumns::PROJECTOR);
        assert!(expr.contains(
            "payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg')"
        ));
        assert!(expr.contains("'unknown')"));
        // Single-line: event_type is built with `\` continuations in v1.
        assert!(!expr.contains('\n'));
    }

    #[test]
    fn turn_summary_predicates_omit_the_event_msg_sentinel() {
        let p = TurnSummaryPredicates::new(DerivationColumns::PROJECTOR);
        assert!(p
            .message
            .contains("payload_type IN ('user_message', 'agent_message', 'message', 'text')"));
        assert!(!p.message.contains("'text', 'event_msg'"));
        assert_eq!(
            p.user_input,
            "(lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text'))))"
        );
        assert!(p
            .final_response
            .contains("lowerUTF8(phase) != 'commentary'"));
    }

    #[test]
    fn mode_aggregate_and_rank_share_the_same_per_event_tests() {
        let agg = mode_aggregate_expr(DerivationColumns::PROJECTOR);
        assert!(agg.contains("countIf(payload_type = 'web_search_call'"));
        assert!(agg.contains("name IN ('WebSearch', 'WebFetch')"));
        assert!(agg.contains("'tool_calling', 'chat')"));

        let rank = mode_rank_expr(DerivationColumns::EVENTS);
        assert!(rank.starts_with("multiIf("));
        assert!(rank.contains("tool_name IN ('WebSearch', 'WebFetch')"));
        assert!(rank.contains("event_kind IN ('tool_call', 'tool_result')"));
        assert!(rank.ends_with(", 1, 0)"));
        // The rank has no countIf wrappers — it is a per-row test.
        assert!(!rank.contains("countIf"));
    }

    #[test]
    fn metadata_bearing_predicate_covers_session_meta_and_omp_arms() {
        let clean = is_metadata_bearing_predicate(DerivationColumns::EVENTS);
        assert_eq!(
            clean,
            "event_kind = 'session_meta' OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))"
        );
        // The projector form is identical logic with one newline before OR.
        let proj = metadata_bearing_predicate_projector_form(DerivationColumns::PROJECTOR);
        assert!(proj.contains("event_class = 'session_meta'\nOR (source_name = 'omp'"));
        assert_eq!(
            proj.replace('\n', " "),
            is_metadata_bearing_predicate(DerivationColumns::PROJECTOR)
        );
    }

    #[test]
    fn deterministic_turn_id_and_total_turns_identity_are_documented() {
        assert_eq!(
            deterministic_turn_id_expr(),
            "argMin(toString(turn_index), tuple(event_order, event_uid))"
        );
        let identity = total_turns_identity_expr("toUInt32(max(turn_index))", "toUInt32(U)");
        assert!(identity.contains("greatest(toUInt32(max(turn_index))"));
        assert!(identity.contains("greatest(toUInt32(1), toUInt32(U))"));
    }
}
