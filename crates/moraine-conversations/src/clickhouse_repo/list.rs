use super::*;

impl ClickHouseConversationRepository {
    pub(super) async fn list_conversations_impl(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<ConversationSummary>> {
        Self::validate_time_bounds(filter.from_unix_ms, filter.to_unix_ms)?;

        let limit = page.normalized_limit(self.cfg.max_results);
        let filter_sig = Self::conversation_filter_sig(&filter);
        let sort = filter.sort;

        let cursor = if let Some(token) = page.cursor.as_deref() {
            let cursor: ConversationCursor = decode_cursor(token)?;
            if cursor.filter_sig != filter_sig {
                return Err(RepoError::invalid_cursor(
                    "cursor does not match current conversation filter",
                ));
            }
            if cursor.sort != sort {
                return Err(RepoError::invalid_cursor(
                    "cursor sort does not match requested sort order",
                ));
            }
            Some(cursor)
        } else {
            None
        };

        let session_summary = self.table_ref("v_session_summary");
        let events_source = canonical_events_source(&self.table_ref("events"));
        let mode_subquery = self.mode_subquery();

        let mut where_clauses = vec!["1 = 1".to_string()];

        if let Some(from_unix_ms) = filter.from_unix_ms {
            where_clauses.push(format!(
                "toUnixTimestamp64Milli(s.last_event_time) >= {from_unix_ms}"
            ));
        }
        if let Some(to_unix_ms) = filter.to_unix_ms {
            where_clauses.push(format!(
                "toUnixTimestamp64Milli(s.last_event_time) < {to_unix_ms}"
            ));
        }
        if let Some(mode_clause) = Self::mode_filter_clause(filter.mode) {
            where_clauses.push(mode_clause);
        }

        if let Some(cursor) = &cursor {
            let (time_cmp, session_cmp) = match sort {
                ConversationListSort::Desc => ("<", "<"),
                ConversationListSort::Asc => (">", ">"),
            };
            where_clauses.push(format!(
                "(toUnixTimestamp64Milli(s.last_event_time) {time_cmp} {} OR (toUnixTimestamp64Milli(s.last_event_time) = {} AND s.session_id {session_cmp} {}))",
                cursor.last_event_unix_ms,
                cursor.last_event_unix_ms,
                sql_quote(&cursor.session_id)
            ));
        }

        let where_sql = where_clauses.join("\n  AND ");
        let order_dir = match sort {
            ConversationListSort::Desc => "DESC",
            ConversationListSort::Asc => "ASC",
        };

        let query = format!(
            "SELECT
  s.session_id AS session_id,
  toString(s.first_event_time) AS first_event_time,
  toInt64(toUnixTimestamp64Milli(s.first_event_time)) AS first_event_unix_ms,
  toString(s.last_event_time) AS last_event_time,
  toInt64(toUnixTimestamp64Milli(s.last_event_time)) AS last_event_unix_ms,
  toUInt32(s.total_turns) AS total_turns,
  toUInt64(s.total_events) AS total_events,
  toUInt64(s.user_messages) AS user_messages,
  toUInt64(s.assistant_messages) AS assistant_messages,
  toUInt64(s.tool_calls) AS tool_calls,
  toUInt64(s.tool_results) AS tool_results,
  ifNull(m.mode, 'chat') AS mode,
  ifNull(meta.session_slug, '') AS session_slug,
  ifNull(meta.session_summary, '') AS session_summary
FROM {session_summary} AS s
LEFT JOIN ({mode_subquery}) AS m ON m.session_id = s.session_id
LEFT JOIN (
  SELECT
    session_id,
    ifNull(argMax(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid)), '') AS session_slug,
    ifNull(
      argMax(
        coalesce(
          nullIf(JSONExtractString(payload_json, 'summary'), ''),
          nullIf(JSONExtractString(payload_json, 'title'), ''),
          nullIf(JSONExtractString(payload_json, 'name'), '')
        ),
        tuple(event_ts, event_uid)
      ),
      ''
    ) AS session_summary
  FROM {events_source}
  WHERE event_kind = 'session_meta'
  GROUP BY session_id
) AS meta ON meta.session_id = s.session_id
WHERE {where_sql}
ORDER BY s.last_event_time {order_dir}, s.session_id {order_dir}
LIMIT {limit_plus}
FORMAT JSONEachRow",
            session_summary = session_summary,
            events_source = events_source,
            mode_subquery = mode_subquery,
            where_sql = where_sql,
            order_dir = order_dir,
            limit_plus = (limit as usize) + 1,
        );

        let rows: Vec<ConversationSummaryRow> =
            self.map_backend(self.query_rows(&query, None).await)?;

        let mut items: Vec<ConversationSummary> = rows
            .iter()
            .take(limit as usize)
            .cloned()
            .map(Self::map_conversation_row)
            .collect();

        let next_cursor = if rows.len() > limit as usize {
            if let Some(last) = items.last() {
                Some(encode_cursor(&ConversationCursor {
                    last_event_unix_ms: last.last_event_unix_ms,
                    session_id: last.session_id.clone(),
                    filter_sig,
                    sort,
                })?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Page {
            items: std::mem::take(&mut items),
            next_cursor,
        })
    }

    pub(super) async fn list_mcp_sessions_impl(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        Self::validate_required_time_bounds(filter.start_unix_ms, filter.end_unix_ms)?;

        let limit = page.normalized_limit(self.cfg.max_results);
        let filter_sig = self.mcp_session_list_filter_sig(&filter);
        let sort = filter.sort;

        let cursor = if let Some(token) = page.cursor.as_deref() {
            let cursor: McpSessionListCursor = decode_cursor(token)?;
            if cursor.filter_sig != filter_sig {
                return Err(RepoError::invalid_cursor(
                    "cursor does not match current list_sessions filter",
                ));
            }
            if cursor.sort != sort {
                return Err(RepoError::invalid_cursor(
                    "cursor sort does not match requested sort order",
                ));
            }
            Some(cursor)
        } else {
            None
        };

        let session_summary = self.table_ref("v_session_summary");
        let events_source = canonical_events_source(&self.table_ref("events"));

        let mut where_clauses = vec![
            // A blank session_id is never a real session (e.g. the orphan
            // Workflow-journal events ingested before #386's exclusion). Drop
            // them here so they never consume a LIMIT slot or anchor the keyset
            // cursor. `notEmpty(trimBoth(...))` mirrors the MCP contract's
            // `trim().is_empty()` rejection so the repo filter and the mcp-core
            // skip agree on what counts as blank.
            "notEmpty(trimBoth(s.session_id))".to_string(),
            format!(
                "toUnixTimestamp64Milli(s.last_event_time) >= {}",
                filter.start_unix_ms
            ),
            format!(
                "toUnixTimestamp64Milli(s.first_event_time) < {}",
                filter.end_unix_ms
            ),
        ];
        if let Some(scope_clause) = self.session_scope_clause("s.session_id") {
            where_clauses.push(scope_clause);
        }

        if let Some(cursor) = &cursor {
            let (time_cmp, session_cmp) = match sort {
                ConversationListSort::Desc => ("<", "<"),
                ConversationListSort::Asc => (">", ">"),
            };
            where_clauses.push(format!(
                "(toUnixTimestamp64Milli(s.last_event_time) {time_cmp} {} OR (toUnixTimestamp64Milli(s.last_event_time) = {} AND s.session_id {session_cmp} {}))",
                cursor.last_event_unix_ms,
                cursor.last_event_unix_ms,
                sql_quote(&cursor.session_id)
            ));
        }

        let where_sql = where_clauses.join("\n  AND ");
        let order_dir = match sort {
            ConversationListSort::Desc => "DESC",
            ConversationListSort::Asc => "ASC",
        };
        let limit_plus = (limit as usize) + 1;
        let window_limit_sql = if filter.mode.is_none()
            && filter.harness.is_none()
            && filter.source_name.is_none()
        {
            format!(
                    "  ORDER BY s.last_event_time {order_dir}, s.session_id {order_dir}\n  LIMIT {limit_plus}\n"
                )
        } else {
            String::new()
        };
        let mut event_filter_clauses = Vec::new();
        if let Some(mode) = filter.mode {
            event_filter_clauses.push(format!(
                "ifNull(r.mode, 'chat') = {}",
                sql_quote(mode.as_str())
            ));
        }
        if let Some(harness) = filter.harness.as_deref() {
            event_filter_clauses.push(format!("r.latest_harness = {}", sql_quote(harness)));
        }
        if let Some(source_name) = filter.source_name.as_deref() {
            event_filter_clauses.push(format!("r.latest_source_name = {}", sql_quote(source_name)));
        }
        let event_filter_sql = if event_filter_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}\n", event_filter_clauses.join("\n    AND "))
        };
        let mode_aggregate = Self::mode_aggregate_sql();

        // Resolve the time/scope window first, applying the keyset LIMIT before
        // event aggregation whenever no mode predicate depends on that
        // aggregation. Then compute every event-backed field in one grouped pass
        // over only the candidate sessions. The previous query independently
        // aggregated mode, completion state, and metadata across the entire
        // events table before applying the window and LIMIT.
        let query = format!(
            "WITH
window_sessions AS (
  SELECT
    s.session_id AS session_id,
    toString(s.first_event_time) AS first_event_time,
    toInt64(toUnixTimestamp64Milli(s.first_event_time)) AS first_event_unix_ms,
    toString(s.last_event_time) AS last_event_time,
    toInt64(toUnixTimestamp64Milli(s.last_event_time)) AS last_event_unix_ms,
    toUInt32(s.total_turns) AS total_turns,
    toUInt64(s.total_events) AS total_events
  FROM {session_summary} AS s
  WHERE {where_sql}
{window_limit_sql}),
event_rollups AS (
  SELECT
    session_id,
    {mode_aggregate} AS mode,
    maxIf(toUInt32(turn_index), payload_type IN ('task_complete', 'turn_aborted')) AS latest_terminal_turn_seq,
    ifNull(
      argMaxIf(payload_type, tuple(event_ts, event_uid), payload_type IN ('task_complete', 'turn_aborted')),
      ''
    ) AS latest_terminal_payload_type,
    ifNull(
      argMaxIf(
        coalesce(
          nullIf(JSONExtractString(payload_json, 'title'), ''),
          nullIf(JSONExtractString(payload_json, 'name'), ''),
          nullIf(JSONExtractString(payload_json, 'summary'), '')
        ),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
      ),
      ''
    ) AS title,
    ifNull(argMax(nullIf(e.harness, ''), tuple(event_ts, event_uid)), '') AS latest_harness,
    ifNull(argMax(nullIf(e.source_name, ''), tuple(event_ts, event_uid)), '') AS latest_source_name,
    ifNull(
      argMaxIf(
        nullIf(JSONExtractString(payload_json, 'slug'), ''),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
      ),
      ''
    ) AS session_slug,
    ifNull(
      argMaxIf(
        coalesce(
          nullIf(JSONExtractString(payload_json, 'summary'), ''),
          nullIf(JSONExtractString(payload_json, 'title'), ''),
          nullIf(JSONExtractString(payload_json, 'name'), '')
        ),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
      ),
      ''
    ) AS session_summary
  FROM {events_source} AS e
  WHERE session_id IN (SELECT session_id FROM window_sessions)
  GROUP BY session_id
),
candidate_sessions AS (
  SELECT
    w.session_id AS session_id,
    w.first_event_time AS first_event_time,
    w.last_event_time AS last_event_time,
    w.first_event_unix_ms AS first_event_unix_ms,
    w.last_event_unix_ms AS last_event_unix_ms,
    w.total_turns AS total_turns,
    w.total_events AS total_events,
    ifNull(r.mode, 'chat') AS mode,
    toUInt8(
      ifNull(r.latest_terminal_turn_seq, toUInt32(0)) = w.total_turns
      AND ifNull(r.latest_terminal_payload_type, '') = 'task_complete'
    ) AS completed,
    ifNull(r.title, '') AS title,
    ifNull(r.latest_source_name, '') AS source,
    ifNull(r.latest_harness, '') AS harness,
    ifNull(r.session_slug, '') AS session_slug,
    ifNull(r.session_summary, '') AS session_summary
  FROM window_sessions AS w
  LEFT JOIN event_rollups AS r ON r.session_id = w.session_id
  {event_filter_sql}ORDER BY w.last_event_unix_ms {order_dir}, w.session_id {order_dir}
  LIMIT {limit_plus}
)
SELECT
  session_id,
  first_event_time,
  first_event_unix_ms,
  last_event_time,
  last_event_unix_ms,
  total_turns,
  total_events,
  mode,
  completed,
  title,
  source,
  harness,
  session_slug,
  session_summary
FROM candidate_sessions
ORDER BY last_event_unix_ms {order_dir}, session_id {order_dir}
FORMAT JSONEachRow",
            session_summary = session_summary,
            events_source = events_source,
            where_sql = where_sql,
            mode_aggregate = mode_aggregate,
            window_limit_sql = window_limit_sql,
            event_filter_sql = event_filter_sql,
            order_dir = order_dir,
            limit_plus = limit_plus,
        );

        let rows: Vec<McpSessionListRow> = self.map_backend(self.query_rows(&query, None).await)?;

        let mut items: Vec<McpSessionListItem> = rows
            .iter()
            .take(limit as usize)
            .cloned()
            .map(Self::map_mcp_session_list_row)
            .collect();

        let next_cursor = if rows.len() > limit as usize {
            if let Some(last) = items.last() {
                Some(encode_cursor(&McpSessionListCursor {
                    last_event_unix_ms: last.last_event_unix_ms,
                    session_id: last.session_id.clone(),
                    filter_sig,
                    sort,
                })?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Page {
            items: std::mem::take(&mut items),
            next_cursor,
        })
    }

    pub(super) async fn list_turns_impl(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        Self::validate_session_id(session_id)?;

        let limit = page.normalized_limit(self.cfg.max_results);
        let filter_sig = Self::turn_filter_sig(session_id, &filter);

        let cursor = if let Some(token) = page.cursor.as_deref() {
            let cursor: TurnCursor = decode_cursor(token)?;
            if cursor.filter_sig != filter_sig {
                return Err(RepoError::invalid_cursor(
                    "cursor does not match current turn filter",
                ));
            }
            Some(cursor)
        } else {
            None
        };

        let turn_summary = self.table_ref("v_turn_summary");
        let mut where_clauses = vec![format!("session_id = {}", sql_quote(session_id))];

        if let Some(from_turn_seq) = filter.from_turn_seq {
            where_clauses.push(format!("turn_seq >= {from_turn_seq}"));
        }
        if let Some(to_turn_seq) = filter.to_turn_seq {
            where_clauses.push(format!("turn_seq <= {to_turn_seq}"));
        }

        if let Some(cursor) = &cursor {
            if cursor.session_id != session_id {
                return Err(RepoError::invalid_cursor(
                    "cursor session_id does not match requested session_id",
                ));
            }
            where_clauses.push(format!("turn_seq > {}", cursor.last_turn_seq));
        }

        let where_sql = where_clauses.join("\n  AND ");
        let query = format!(
            "SELECT
  session_id,
  toUInt32(turn_seq) AS turn_seq,
  ifNull(turn_id, '') AS turn_id,
  toString(ts.started_at) AS started_at,
  toInt64(toUnixTimestamp64Milli(ts.started_at)) AS started_at_unix_ms,
  toString(ts.ended_at) AS ended_at,
  toInt64(toUnixTimestamp64Milli(ts.ended_at)) AS ended_at_unix_ms,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  toUInt64(reasoning_items) AS reasoning_items
FROM {turn_summary} AS ts
WHERE {where_sql}
ORDER BY turn_seq ASC
LIMIT {limit_plus}
FORMAT JSONEachRow",
            turn_summary = turn_summary,
            where_sql = where_sql,
            limit_plus = (limit as usize) + 1,
        );

        let rows: Vec<TurnSummaryRow> = self.map_backend(self.query_rows(&query, None).await)?;
        let items: Vec<TurnSummary> = rows
            .iter()
            .take(limit as usize)
            .cloned()
            .map(Self::map_turn_row)
            .collect();

        let next_cursor = if rows.len() > limit as usize {
            if let Some(last) = items.last() {
                Some(encode_cursor(&TurnCursor {
                    last_turn_seq: last.turn_seq,
                    session_id: session_id.to_string(),
                    filter_sig,
                })?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Page { items, next_cursor })
    }

    pub(super) async fn list_session_events_impl(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        let session_id = query.session_id.trim();
        if session_id.is_empty() {
            return Err(RepoError::invalid_argument("session_id cannot be empty"));
        }
        Self::validate_session_id(session_id)?;

        let direction = query.direction;
        let event_kinds = Self::normalize_event_kinds(query.event_kinds)?;
        let filter_sig =
            Self::session_events_filter_sig(session_id, direction, event_kinds.as_deref());
        let limit = page.normalized_limit(self.cfg.max_results);

        let cursor = if let Some(token) = page.cursor.as_deref() {
            let cursor: SessionEventCursor = decode_cursor(token)?;
            if cursor.session_id != session_id {
                return Err(RepoError::invalid_cursor(
                    "cursor session_id does not match requested session_id",
                ));
            }
            if cursor.direction != direction {
                return Err(RepoError::invalid_cursor(
                    "cursor direction does not match requested direction",
                ));
            }
            if cursor.filter_sig != filter_sig {
                return Err(RepoError::invalid_cursor(
                    "cursor does not match current session event filter",
                ));
            }
            Some(cursor)
        } else {
            None
        };

        let trace_table = self.table_ref("v_conversation_trace");
        let mut where_clauses = vec![format!("session_id = {}", sql_quote(session_id))];
        if let Some(event_kinds) = event_kinds.as_deref() {
            where_clauses.push(Self::event_kind_filter_clause(
                "event_class",
                "payload_type",
                event_kinds,
            ));
        }
        if let Some(cursor) = &cursor {
            let cursor_clause = match direction {
                SessionEventsDirection::Forward => format!(
                    "(event_order > {} OR (event_order = {} AND event_uid > {}))",
                    cursor.last_event_order,
                    cursor.last_event_order,
                    sql_quote(&cursor.last_event_uid)
                ),
                SessionEventsDirection::Reverse => format!(
                    "(event_order < {} OR (event_order = {} AND event_uid < {}))",
                    cursor.last_event_order,
                    cursor.last_event_order,
                    sql_quote(&cursor.last_event_uid)
                ),
            };
            where_clauses.push(cursor_clause);
        }
        let where_sql = where_clauses.join("\n  AND ");
        let order_by_sql = match direction {
            SessionEventsDirection::Forward => "event_order ASC, event_uid ASC",
            SessionEventsDirection::Reverse => "event_order DESC, event_uid DESC",
        };

        let query = format!(
            "SELECT
  session_id,
  event_uid,
  toUInt64(event_order) AS event_order,
  toUInt32(turn_seq) AS turn_seq,
  toString(tr.event_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms,
  actor_role,
  event_class,
  payload_type,
  call_id,
  name,
  phase,
  item_id,
  source_ref,
  text_content,
  payload_json,
  token_usage_json,
  endpoint_kind,
  token_usage_buckets,
  token_usage_native_units
FROM {trace_table} AS tr
WHERE {where_sql}
ORDER BY {order_by_sql}
LIMIT {limit_plus}
FORMAT JSONEachRow",
            trace_table = trace_table,
            where_sql = where_sql,
            order_by_sql = order_by_sql,
            limit_plus = (limit as usize) + 1,
        );

        let mut rows: Vec<TraceEventRow> = self.map_backend(self.query_rows(&query, None).await)?;
        let has_more = rows.len() > limit as usize;
        if has_more {
            rows.truncate(limit as usize);
        }

        let next_cursor = if has_more {
            if let Some(last) = rows.last() {
                Some(encode_cursor(&SessionEventCursor {
                    last_event_order: last.event_order,
                    last_event_uid: last.event_uid.clone(),
                    session_id: session_id.to_string(),
                    direction,
                    filter_sig,
                })?)
            } else {
                None
            }
        } else {
            None
        };

        let items = rows.into_iter().map(Self::map_trace_event).collect();
        Ok(Page { items, next_cursor })
    }
}
