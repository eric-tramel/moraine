//! Page-first session listing SQL shared by `list_conversations` (monitor) and
//! `list_sessions` (MCP).
//!
//! Listing used to read `v_session_summary`, which aggregates
//! `v_conversation_trace`: a `row_number()` plus running `sum()` window
//! `PARTITION BY session_id` over all of `events FINAL`, ordered by columns
//! that include `payload_json`. Outer predicates cannot be pushed below a
//! window, so every call sorted the whole corpus in memory.
//!
//! Listing is now two phases:
//!
//! 1. **Page selection** ([`SessionPageSelect`]): a narrow `GROUP BY session_id`
//!    directly over `events FINAL` that reads only `session_id`, `record_ts`
//!    and `ingested_at` (plus a few narrow columns when a mode/harness/source
//!    filter must be resolved from event rows). No `payload_json`, no windows.
//!    Time bounds, the keyset cursor, ordering and `LIMIT` apply here.
//! 2. **Page summary** ([`session_summary_sql`]): the `v_session_summary`
//!    aggregates computed only for the selected ids. The turn-sequence window
//!    is inlined with its `session_id IN (...)` restriction on the innermost
//!    `events FINAL` read, so the window only ever sees page sessions.
//!
//! Remaining cost: phase 1 filters on time, which is not in the `events`
//! ORDER BY, so it still scans the narrow columns of the whole table. Its
//! memory is bounded by `GROUP BY session_id` (one small state per session)
//! rather than by event count, and `optimize_aggregation_in_order` streams the
//! aggregation in key order. A `mode` filter additionally reads four narrow
//! columns because mode is an aggregate over event rows; there is no cheaper
//! source for it without a new index table.

use super::*;

/// Per-query settings for phase 1.
///
/// `optimize_aggregation_in_order = 1` does engage (`EXPLAIN PIPELINE` shows
/// `AggregatingInOrderTransform`/`FinishAggregatingInOrderTransform` because
/// `events` is sorted by `session_id`), but on a 220k-event / 501-session
/// corpus it was slower and heavier than hash aggregation (about 60 ms / 41 MiB
/// versus 34 ms / 22 MiB): the grouped state is one small row per session
/// either way, and in-order reading makes `FINAL` read more rows.
/// `do_not_merge_across_partitions_select_final` stays pinned to 0 by
/// `REPOSITORY_READ_SETTINGS` (legacy cross-partition duplicates, #454) and
/// measured no gain here. Left empty so phase 1 uses the server defaults.
pub(super) const SESSION_PAGE_SELECT_SETTINGS: [(&str, &str); 0] = [];

/// Per-query settings for phase-2 page summaries. Each wide-column read pulls
/// whole granules (up to ~10 MiB for `payload_json`) per reader thread, so peak
/// memory scales with the thread count; four threads keep a 200-session page
/// under the 256 MiB interactive cap on payload-heavy corpora.
pub(super) const SESSION_PAGE_SUMMARY_SETTINGS: [(&str, &str); 1] = [("max_threads", "4")];

/// Same expression as `v_conversation_trace.event_time`.
const EVENT_TIME_SQL: &str = "ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)";

pub(super) struct SessionPageSelect<'a> {
    pub(super) events_ref: String,
    /// `last_event_time >= x` (unix ms).
    pub(super) last_event_from_ms: Option<i64>,
    /// `last_event_time < x` (unix ms).
    pub(super) last_event_to_ms: Option<i64>,
    /// `last_event_time >= now() - INTERVAL n SECOND`.
    pub(super) last_event_within_secs: Option<u64>,
    /// `first_event_time < x` (unix ms).
    pub(super) first_event_before_ms: Option<i64>,
    pub(super) cursor: Option<(i64, &'a str)>,
    pub(super) sort: ConversationListSort,
    pub(super) mode: Option<ConversationMode>,
    pub(super) harness: Option<&'a str>,
    pub(super) source_name: Option<&'a str>,
    pub(super) scope_clause: Option<String>,
    pub(super) limit_plus: usize,
}

impl SessionPageSelect<'_> {
    pub(super) fn to_sql(&self) -> String {
        let dir = match self.sort {
            ConversationListSort::Desc => "DESC",
            ConversationListSort::Asc => "ASC",
        };
        let mut columns = vec!["session_id", "record_ts", "ingested_at"];
        let mut extra_aggregates = Vec::new();
        let mut having = Vec::new();

        if self.mode.is_some() {
            columns.extend(["event_kind", "payload_type", "tool_name", "source_name"]);
            extra_aggregates.push(format!(
                ",\n  {} AS session_mode",
                ClickHouseConversationRepository::mode_aggregate_sql()
            ));
        }
        if self.harness.is_some() || self.source_name.is_some() {
            columns.extend(["event_ts", "event_uid"]);
        }
        if self.harness.is_some() {
            columns.push("harness");
            extra_aggregates.push(
                ",\n  ifNull(argMax(nullIf(harness, ''), tuple(event_ts, event_uid)), '') AS latest_harness"
                    .to_string(),
            );
        }
        if self.source_name.is_some() && !columns.contains(&"source_name") {
            columns.push("source_name");
        }
        if self.source_name.is_some() {
            extra_aggregates.push(
                ",\n  ifNull(argMax(nullIf(source_name, ''), tuple(event_ts, event_uid)), '') AS latest_source_name"
                    .to_string(),
            );
        }
        let columns = columns.join(", ");
        let extra_aggregates = extra_aggregates.concat();

        if let Some(ms) = self.last_event_from_ms {
            having.push(format!("toUnixTimestamp64Milli(last_event_time) >= {ms}"));
        }
        if let Some(ms) = self.last_event_to_ms {
            having.push(format!("toUnixTimestamp64Milli(last_event_time) < {ms}"));
        }
        if let Some(seconds) = self.last_event_within_secs {
            having.push(format!(
                "last_event_time >= now() - INTERVAL {seconds} SECOND"
            ));
        }
        if let Some(ms) = self.first_event_before_ms {
            having.push(format!("toUnixTimestamp64Milli(first_event_time) < {ms}"));
        }
        if let Some(mode) = self.mode {
            having.push(format!("session_mode = {}", sql_quote(mode.as_str())));
        }
        if let Some(harness) = self.harness {
            having.push(format!("latest_harness = {}", sql_quote(harness)));
        }
        if let Some(source_name) = self.source_name {
            having.push(format!("latest_source_name = {}", sql_quote(source_name)));
        }
        if let Some((last_ms, session_id)) = self.cursor {
            let cmp = match self.sort {
                ConversationListSort::Desc => "<",
                ConversationListSort::Asc => ">",
            };
            having.push(format!(
                "(toUnixTimestamp64Milli(last_event_time) {cmp} {last_ms} OR (toUnixTimestamp64Milli(last_event_time) = {last_ms} AND session_id {cmp} {}))",
                sql_quote(session_id)
            ));
        }
        let having_sql = if having.is_empty() {
            String::new()
        } else {
            format!("HAVING {}\n", having.join("\n  AND "))
        };

        // A blank session_id is never a real session (e.g. the orphan
        // Workflow-journal events ingested before #386's exclusion). Drop them
        // so they never consume a LIMIT slot or anchor the keyset cursor.
        // `session_id` leads the events ORDER BY, so the WHERE is applied
        // before FINAL merges.
        let mut where_clauses = vec!["notEmpty(trimBoth(session_id))".to_string()];
        if let Some(scope) = &self.scope_clause {
            where_clauses.push(scope.clone());
        }
        let where_sql = where_clauses.join("\n  AND ");

        format!(
            "SELECT
  session_id,
  min({event_time}) AS first_event_time,
  max({event_time}) AS last_event_time{extra_aggregates}
FROM (
  SELECT {columns}
  FROM {events_ref} FINAL
  WHERE {where_sql}
)
GROUP BY session_id
{having_sql}ORDER BY last_event_time {dir}, session_id {dir}
LIMIT {limit_plus}
FORMAT JSONEachRow",
            event_time = EVENT_TIME_SQL,
            events_ref = self.events_ref,
            limit_plus = self.limit_plus,
        )
    }
}

/// Optional wide-column extras for [`session_trace_sql`].
#[derive(Default)]
pub(super) struct TraceExtras<'a> {
    /// Extra `(expression, alias)` columns read by the narrow `events FINAL`
    /// scan and carried through the trace.
    pub(super) narrow_columns: &'a [(&'a str, &'a str)],
    /// Include `event_order` (a `row_number()` over the trace ordering).
    pub(super) with_event_order: bool,
    /// `tool_args_json`: the `input` of `tool_call` rows, read through
    /// [`payload_rows_sql`] so `payload_json` is only read for those rows.
    pub(super) with_tool_args: bool,
}

/// Wide columns (`select_list`, typically `payload_json`) of the canonical
/// `events` rows selected by `keys_sql`, a query returning
/// `(session_id, event_uid, event_version)`.
///
/// `events FINAL` merges every part's wide-column granules in memory, and
/// `payload_json` dominates the table (single rows reach several MiB), so a
/// `FINAL` read that touches it can exceed the interactive memory cap long
/// before it finishes. Instead the winning versions are chosen by a narrow
/// `FINAL` (`keys_sql`) and the wide read is a plain primary-key read of just
/// those rows. It can return a row more than once only if identical versions
/// were inserted twice; callers either aggregate idempotently or `ANY JOIN`.
fn payload_rows_by_keys_sql(
    events_ref: &str,
    session_ids_sql: &str,
    select_list: &str,
    keys_sql: &str,
) -> String {
    format!(
        "SELECT {select_list}
FROM {events_ref}
WHERE session_id IN ({session_ids_sql})
  AND (session_id, event_uid, event_version) IN (
    {keys_sql}
  )"
    )
}

/// [`payload_rows_by_keys_sql`] for rows whose narrow-column predicate
/// `pred_sql` holds. `pred_sql` must be a deterministic property of the event
/// (e.g. its kind or source).
pub(super) fn payload_rows_sql(
    events_ref: &str,
    session_ids_sql: &str,
    pred_sql: &str,
    select_list: &str,
) -> String {
    let keys = format!(
        "SELECT session_id, event_uid, event_version
    FROM {events_ref} FINAL
    WHERE session_id IN ({session_ids_sql})
      AND ({pred_sql})"
    );
    payload_rows_by_keys_sql(events_ref, session_ids_sql, select_list, &keys)
}

/// `moraine_emission_index` for the rows that need it as a sort tie-breaker.
///
/// The view orders by `(event_time, source_file, source_generation,
/// source_offset, source_line_no, moraine_emission_index, event_uid)`. The
/// emission index can only change the order among rows that share the first
/// five keys, so it is read from `payload_json` only for those (a few percent
/// of rows); every other row sorts identically with an index of zero.
///
/// When the caller does not expose `event_order` (`needs_event_order = false`),
/// only `turn_seq` depends on the order, and only for a tie group that holds
/// both a turn-less row (`turn_index = 0`, which takes the running user-message
/// count) and a user message (which advances that count). Every other group
/// yields the same `turn_seq` values in any order, so its payload is not read.
fn tie_emission_sql(events_ref: &str, session_ids_sql: &str, needs_event_order: bool) -> String {
    let order_sensitive = if needs_event_order {
        ""
    } else {
        "
        AND countIf(turn_index = 0) > 0
        AND countIf(actor_kind = 'user' AND event_kind = 'message') > 0"
    };
    let tied_keys = format!(
        "SELECT session_id, k.1 AS event_uid, k.2 AS event_version
    FROM (
      SELECT session_id, arrayJoin(groupArray((event_uid, event_version))) AS k
      FROM {events_ref} FINAL
      WHERE session_id IN ({session_ids_sql})
      GROUP BY session_id, {EVENT_TIME_SQL}, source_file, source_generation, source_offset, source_line_no
      HAVING count() > 1{order_sensitive}
    )"
    );
    payload_rows_by_keys_sql(
        events_ref,
        session_ids_sql,
        "session_id, event_uid, toUInt32(JSONExtractUInt(payload_json, 'moraine_emission_index')) AS emission_index",
        &tied_keys,
    )
}

/// Inlined `v_conversation_trace` for `session_ids_sql` only.
///
/// Yields `session_id`, `event_class`, `actor_role`, `turn_id`, `event_time`,
/// `turn_seq` (plus `event_order`, `tool_args_json` and `extras.narrow_columns`
/// when requested). The `session_id IN (...)` restriction sits on the innermost
/// `events FINAL` reads, so the `row_number()` and running `sum()` windows only
/// ever see those sessions, and no read of `payload_json` is proportional to
/// the page (see [`tie_emission_sql`]). Ordering matches the view exactly.
pub(super) fn session_trace_sql(
    events_ref: &str,
    session_ids_sql: &str,
    extras: &TraceExtras<'_>,
) -> String {
    let order_by = "event_time, source_file, source_generation, source_offset, source_line_no, emission_index, event_uid";
    let event_order = if extras.with_event_order {
        format!(
            "\n    row_number() OVER (PARTITION BY session_id ORDER BY {order_by}) AS event_order,"
        )
    } else {
        String::new()
    };
    let (tool_args_select, tool_args_join) = if extras.with_tool_args {
        (
            ",\n    ifNull(ta.tool_args_json, '') AS tool_args_json",
            format!(
                "\n  ANY LEFT JOIN (
    {tool_args}
  ) AS ta ON ta.session_id = t.session_id AND ta.event_uid = t.event_uid",
                tool_args = payload_rows_sql(
                    events_ref,
                    session_ids_sql,
                    "event_kind = 'tool_call'",
                    "session_id, event_uid, substringUTF8(JSONExtractRaw(payload_json, 'input'), 1, 2000) AS tool_args_json",
                )
            ),
        )
    } else {
        ("", String::new())
    };
    let narrow_select: String = extras
        .narrow_columns
        .iter()
        .map(|(expr, alias)| format!(",\n        {expr} AS {alias}"))
        .collect();
    let narrow_names: String = extras
        .narrow_columns
        .iter()
        .map(|(_, alias)| format!(",\n    {alias}"))
        .collect();
    format!(
        "SELECT
    session_id,
    event_class,
    actor_role,
    turn_id,
    event_time,{event_order}
    if(
      toUInt32OrZero(turn_id) > 0,
      toUInt32OrZero(turn_id),
      greatest(
        toUInt32(1),
        toUInt32(
          sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER (
            PARTITION BY session_id
            ORDER BY {order_by}
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )
        )
      )
    ) AS turn_seq{narrow_names}{tool_args_select_outer}
  FROM (
    SELECT
      t.session_id AS session_id,
      t.event_uid AS event_uid,
      t.event_kind AS event_class,
      t.actor_kind AS actor_role,
      toString(t.turn_index) AS turn_id,
      {EVENT_TIME_SQL_T} AS event_time,
      t.source_file AS source_file,
      t.source_generation AS source_generation,
      t.source_offset AS source_offset,
      t.source_line_no AS source_line_no,
      ifNull(em.emission_index, 0) AS emission_index{narrow_names_t}{tool_args_select}
    FROM (
      SELECT session_id, event_uid, event_kind, actor_kind, turn_index, record_ts, ingested_at,
        source_file, source_generation, source_offset, source_line_no{narrow_select}
      FROM {events_ref} FINAL
      WHERE session_id IN ({session_ids_sql})
    ) AS t
    ANY LEFT JOIN (
      {tie_emission}
    ) AS em ON em.session_id = t.session_id AND em.event_uid = t.event_uid{tool_args_join}
  )",
        EVENT_TIME_SQL_T = "ifNull(parseDateTime64BestEffortOrNull(t.record_ts), t.ingested_at)",
        tool_args_select_outer = if extras.with_tool_args {
            ",\n    tool_args_json"
        } else {
            ""
        },
        narrow_names_t = narrow_names.replace("\n    ", "\n      "),
        tie_emission = tie_emission_sql(events_ref, session_ids_sql, extras.with_event_order),
    )
}

/// `v_session_summary` aggregates (session_id, first/last_event_time,
/// total_turns, total_events, user/assistant messages, tool calls/results) for
/// `session_ids_sql` only.
pub(super) fn session_summary_sql(events_ref: &str, session_ids_sql: &str) -> String {
    let trace = session_trace_sql(events_ref, session_ids_sql, &TraceExtras::default());
    format!(
        "SELECT
  session_id,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  max(turn_seq) AS total_turns,
  count() AS total_events,
  countIf(event_class = 'tool_call') AS tool_calls,
  countIf(event_class = 'tool_result') AS tool_results,
  countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,
  countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages
FROM (
  {trace}
)
GROUP BY session_id"
    )
}

/// `v_turn_summary` aggregates for `session_ids_sql` only.
pub(super) fn turn_summary_sql(events_ref: &str, session_ids_sql: &str) -> String {
    let trace = session_trace_sql(events_ref, session_ids_sql, &TraceExtras::default());
    format!(
        "SELECT
  session_id,
  turn_seq,
  anyIf(turn_id, turn_id != '') AS turn_id,
  min(event_time) AS started_at,
  max(event_time) AS ended_at,
  count() AS total_events,
  countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,
  countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,
  countIf(event_class = 'tool_call') AS tool_calls,
  countIf(event_class = 'tool_result') AS tool_results,
  countIf(event_class = 'reasoning') AS reasoning_items
FROM (
  {trace}
)
GROUP BY session_id, turn_seq"
    )
}

pub(super) fn sql_string_list(ids: &[String]) -> String {
    ids.iter()
        .map(|id| sql_quote(id))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENTS: &str = "`moraine`.`events`";

    fn select(sort: ConversationListSort) -> SessionPageSelect<'static> {
        SessionPageSelect {
            events_ref: EVENTS.to_string(),
            last_event_from_ms: Some(10),
            last_event_to_ms: None,
            last_event_within_secs: None,
            first_event_before_ms: Some(20),
            cursor: Some((15, "sess_b")),
            sort,
            mode: None,
            harness: None,
            source_name: None,
            scope_clause: None,
            limit_plus: 26,
        }
    }

    fn assert_no_view_chain(sql: &str) {
        assert!(!sql.contains("v_session_summary"), "{sql}");
        assert!(!sql.contains("v_conversation_trace"), "{sql}");
        assert!(!sql.contains("v_turn_summary"), "{sql}");
        assert!(!sql.contains("v_all_events"), "{sql}");
    }

    #[test]
    fn page_select_reads_only_narrow_columns_without_windows() {
        let sql = select(ConversationListSort::Desc).to_sql();
        assert_no_view_chain(&sql);
        assert!(sql.contains(
            "SELECT session_id, record_ts, ingested_at\n  FROM `moraine`.`events` FINAL"
        ));
        assert!(!sql.contains("payload_json"));
        assert!(!sql.contains("OVER ("));
        assert!(sql.contains("WHERE notEmpty(trimBoth(session_id))"));
        assert!(sql.contains("toUnixTimestamp64Milli(last_event_time) >= 10"));
        assert!(sql.contains("toUnixTimestamp64Milli(first_event_time) < 20"));
        assert!(
            sql.contains("toUnixTimestamp64Milli(last_event_time) = 15 AND session_id < 'sess_b'")
        );
        assert!(sql.contains("ORDER BY last_event_time DESC, session_id DESC\nLIMIT 26"));
    }

    #[test]
    fn page_select_event_time_matches_the_view_expression() {
        let sql = select(ConversationListSort::Asc).to_sql();
        assert!(sql.contains(
            "min(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS first_event_time"
        ));
        assert!(sql.contains(
            "max(ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)) AS last_event_time"
        ));
        assert!(sql.contains("session_id > 'sess_b'"));
        assert!(sql.contains("ORDER BY last_event_time ASC, session_id ASC"));
    }

    #[test]
    fn page_select_resolves_aggregate_filters_from_narrow_columns() {
        let mut spec = select(ConversationListSort::Desc);
        spec.mode = Some(ConversationMode::ToolCalling);
        spec.harness = Some("codex");
        spec.source_name = Some("host-codex");
        spec.scope_clause = Some("session_id IN (SELECT 'scoped')".to_string());
        let sql = spec.to_sql();
        assert_no_view_chain(&sql);
        assert!(!sql.contains("payload_json"), "{sql}");
        assert!(sql.contains("session_mode = 'tool_calling'"));
        assert!(sql.contains("latest_harness = 'codex'"));
        assert!(sql.contains("latest_source_name = 'host-codex'"));
        // Scope applies before FINAL merges, not after aggregation.
        let (inner, _) = sql.rsplit_once("\n)\nGROUP BY session_id").unwrap();
        assert!(inner.contains("session_id IN (SELECT 'scoped')"));
    }

    #[test]
    fn phase_one_leaves_aggregation_settings_to_the_server() {
        assert!(SESSION_PAGE_SELECT_SETTINGS.is_empty());
        assert!(SESSION_PAGE_SUMMARY_SETTINGS.contains(&("max_threads", "4")));
    }

    #[test]
    fn trace_restricts_sessions_before_any_window_and_reads_no_payload_through_final() {
        let sql = session_trace_sql(EVENTS, "'a', 'b'", &TraceExtras::default());
        assert_no_view_chain(&sql);
        let first_window = sql.find("OVER (").expect("trace has windows");
        let restriction = sql
            .find("FROM `moraine`.`events` FINAL\n      WHERE session_id IN ('a', 'b')")
            .expect("innermost FINAL read is restricted to the page");
        // The windows sit in the outer select over the restricted scan below.
        assert!(restriction > first_window);
        assert!(!sql[restriction..first_window.max(restriction)].contains("OVER ("));
        // Every events read in the trace is restricted to the page ids.
        assert_eq!(
            sql.matches("FROM `moraine`.`events`").count(),
            sql.matches("WHERE session_id IN ('a', 'b')").count()
        );
        // The narrow scan never touches payload_json; only the keyed tie lookup does.
        let narrow_scan = &sql[..sql.find("ANY LEFT JOIN").unwrap()];
        assert!(!narrow_scan.contains("payload_json"));
        assert!(sql.contains("AND (session_id, event_uid, event_version) IN ("));
        assert!(sql.contains(
            "event_time, source_file, source_generation, source_offset, source_line_no, emission_index, event_uid"
        ));
        assert!(sql.contains("HAVING count() > 1"));
        // Without event_order only order-sensitive turn_seq ties need payload.
        assert!(sql.contains("AND countIf(turn_index = 0) > 0"));
    }

    #[test]
    fn trace_optional_columns_are_opt_in() {
        let plain = session_trace_sql(EVENTS, "'a'", &TraceExtras::default());
        assert!(!plain.contains("event_order"));
        assert!(!plain.contains("tool_args_json"));
        let rich = session_trace_sql(
            EVENTS,
            "'a'",
            &TraceExtras {
                narrow_columns: &[("tool_name", "name")],
                with_event_order: true,
                with_tool_args: true,
            },
        );
        assert!(rich.contains("row_number() OVER (PARTITION BY session_id ORDER BY"));
        assert!(rich.contains("tool_name AS name"));
        assert!(rich.contains("event_kind = 'tool_call'"));
        assert!(rich.contains("tool_args_json"));
        // event_order exposes the full ordering, so every tie group is resolved.
        assert!(rich.contains("HAVING count() > 1\n    )"));
        assert!(!rich.contains("countIf(turn_index = 0)"));
    }

    #[test]
    fn summaries_group_the_restricted_trace() {
        for sql in [
            session_summary_sql(EVENTS, "'a'"),
            turn_summary_sql(EVENTS, "'a'"),
        ] {
            assert_no_view_chain(&sql);
            assert!(sql.contains("FROM (\n  SELECT"));
            assert!(sql.contains("session_id IN ('a')"));
        }
        assert!(session_summary_sql(EVENTS, "'a'").contains("max(turn_seq) AS total_turns"));
        assert!(turn_summary_sql(EVENTS, "'a'").contains("GROUP BY session_id, turn_seq"));
    }

    #[test]
    fn payload_rows_are_keyed_by_a_narrow_final_not_read_through_final() {
        let sql = payload_rows_sql(
            EVENTS,
            "'a'",
            "event_kind = 'session_meta'",
            "session_id, payload_json",
        );
        let (outer, keys) = sql
            .split_once("(session_id, event_uid, event_version) IN (")
            .expect("version-keyed lookup");
        assert!(outer.contains("FROM `moraine`.`events`\nWHERE session_id IN ('a')"));
        assert!(!outer.contains("FINAL"));
        assert!(keys.contains("FROM `moraine`.`events` FINAL"));
        assert!(!keys.contains("payload_json"));
        assert!(keys.contains("event_kind = 'session_meta'"));
    }

    #[test]
    fn sql_string_list_quotes_and_escapes() {
        assert_eq!(
            sql_string_list(&["a".to_string(), "it's".to_string()]),
            "'a', 'it''s'"
        );
    }
}
