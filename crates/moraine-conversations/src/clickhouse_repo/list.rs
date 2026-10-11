use super::*;

/// Sessions per phase-2 query. Wide-column reads cost whole granules, so peak
/// ClickHouse memory is bounded by chunking the page rather than by its size.
const SESSION_LIST_CHUNK: usize = 10;

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

        let events_ref = self.table_ref("events");
        let order_dir = match sort {
            ConversationListSort::Desc => "DESC",
            ConversationListSort::Asc => "ASC",
        };

        // Phase 1: pick the page of session ids from narrow columns only.
        let select_sql = SessionPageSelect {
            events_ref: events_ref.clone(),
            last_event_from_ms: filter.from_unix_ms,
            last_event_to_ms: filter.to_unix_ms,
            last_event_within_secs: None,
            first_event_before_ms: None,
            cursor: cursor
                .as_ref()
                .map(|c| (c.last_event_unix_ms, c.session_id.as_str())),
            sort,
            mode: filter.mode,
            harness: None,
            source_name: None,
            scope_clause: None,
            limit_plus: (limit as usize) + 1,
        }
        .to_sql();
        let selected: Vec<SessionIdRow> = self.map_backend(
            self.query_rows_with_params(&select_sql, None, &SESSION_PAGE_SELECT_SETTINGS)
                .await,
        )?;
        let has_more = selected.len() > limit as usize;
        let page_ids: Vec<String> = selected
            .into_iter()
            .take(limit as usize)
            .map(|row| row.session_id)
            .collect();
        if page_ids.is_empty() {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        // Phase 2: summarize only the selected sessions. Every subquery and
        // join side below is restricted to `page_ids` before aggregation.
        let build_query = |ids_sql: &str| {
            let summary = session_summary_sql(&events_ref, ids_sql);
            let mode_subquery = self.mode_subquery_for_sessions(Some(ids_sql));
            format!(
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
FROM ({summary}) AS s
ANY LEFT JOIN ({mode_subquery}) AS m ON m.session_id = s.session_id
ANY LEFT JOIN (
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
  FROM ({meta_rows})
  GROUP BY session_id
) AS meta ON meta.session_id = s.session_id
ORDER BY s.last_event_time {order_dir}, s.session_id {order_dir}
FORMAT JSONEachRow",
            summary = summary,
            mode_subquery = mode_subquery,
            meta_rows = payload_rows_sql(
                &events_ref,
                ids_sql,
                "event_kind = 'session_meta'",
                "session_id, event_ts, event_uid, payload_json",
            ),
            order_dir = order_dir,
        )
        };

        let rows: Vec<ConversationSummaryRow> = self
            .query_session_page_chunks(&page_ids, SESSION_LIST_CHUNK, build_query)
            .await?;

        let mut items: Vec<ConversationSummary> =
            rows.into_iter().map(Self::map_conversation_row).collect();

        let next_cursor = if has_more {
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

        let events_ref = self.table_ref("events");
        let order_dir = match sort {
            ConversationListSort::Desc => "DESC",
            ConversationListSort::Asc => "ASC",
        };

        // Phase 1: pick the page of session ids. Time window, scope, keyset
        // cursor, and the mode/harness/source filters all resolve here from
        // narrow columns (no payload_json, no window functions). The mode
        // filter is an aggregate over event rows, so it reads four extra
        // narrow columns for every candidate session in the time window; that
        // is the one remaining corpus-proportional cost, and it is still a
        // plain GROUP BY, not a sort.
        let select_sql = SessionPageSelect {
            events_ref: events_ref.clone(),
            last_event_from_ms: Some(filter.start_unix_ms),
            last_event_to_ms: None,
            last_event_within_secs: None,
            first_event_before_ms: Some(filter.end_unix_ms),
            cursor: cursor
                .as_ref()
                .map(|c| (c.last_event_unix_ms, c.session_id.as_str())),
            sort,
            mode: filter.mode,
            harness: filter.harness.as_deref(),
            source_name: filter.source_name.as_deref(),
            scope_clause: self.session_scope_clause("session_id"),
            limit_plus: (limit as usize) + 1,
        }
        .to_sql();
        let selected: Vec<SessionIdRow> = self.map_backend(
            self.query_rows_with_params(&select_sql, None, &SESSION_PAGE_SELECT_SETTINGS)
                .await,
        )?;
        let has_more = selected.len() > limit as usize;
        let page_ids: Vec<String> = selected
            .into_iter()
            .take(limit as usize)
            .map(|row| row.session_id)
            .collect();
        if page_ids.is_empty() {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        // Phase 2: every event-backed field is computed in one grouped pass
        // over only the selected sessions.
        let mode_aggregate = Self::mode_aggregate_sql();
        let build_query = |ids_sql: &str| {
            let summary = session_summary_sql(&events_ref, ids_sql);
            // Title/slug/summary fields live in `payload_json`; read it only for
            // the session_meta and OMP rows that carry them (see payload_rows_sql).
            let metadata_rows = payload_rows_sql(
            &events_ref,
            ids_sql,
            "event_kind = 'session_meta' OR source_name = 'omp'",
            "session_id, event_ts, event_uid, event_kind, source_name, source_file, payload_json",
        );

            format!(
            "WITH
page_sessions AS (
  SELECT
    s.session_id AS session_id,
    toString(s.first_event_time) AS first_event_time,
    toInt64(toUnixTimestamp64Milli(s.first_event_time)) AS first_event_unix_ms,
    toString(s.last_event_time) AS last_event_time,
    toInt64(toUnixTimestamp64Milli(s.last_event_time)) AS last_event_unix_ms,
    toUInt32(s.total_turns) AS total_turns,
    toUInt64(s.total_events) AS total_events
  FROM ({summary}) AS s
),
event_rollups AS (
  SELECT
    session_id,
    {mode_aggregate} AS mode,
    maxIf(toUInt32(turn_index), payload_type IN ('task_complete', 'turn_aborted')) AS latest_terminal_turn_seq,
    ifNull(
      argMaxIf(payload_type, tuple(event_ts, event_uid), payload_type IN ('task_complete', 'turn_aborted')),
      ''
    ) AS latest_terminal_payload_type,
    ifNull(argMax(nullIf(e.harness, ''), tuple(event_ts, event_uid)), '') AS latest_harness,
    ifNull(argMax(nullIf(e.source_name, ''), tuple(event_ts, event_uid)), '') AS latest_source_name,
    ifNull(
      argMinIf(
        nullIf(trimBoth(substringUTF8(e.text_preview, 1, 320)), ''),
        tuple(
          e.event_ts,
          e.source_name,
          e.source_file,
          e.source_generation,
          e.source_offset,
          e.source_line_no,
          e.event_uid
        ),
        e.harness = 'codex'
          AND e.actor_kind = 'user'
          AND e.event_kind = 'event_msg'
          AND e.payload_type = 'user_message'
      ),
      ''
    ) AS codex_user_message_preview
  FROM (
    SELECT session_id, event_ts, event_uid, event_kind, actor_kind, payload_type, tool_name,
      source_name, source_file, source_generation, source_offset, source_line_no,
      harness, turn_index, text_preview
    FROM {events_ref} FINAL
    WHERE session_id IN ({ids_sql})
  ) AS e
  GROUP BY session_id
),
metadata_rollups AS (
  SELECT
    session_id,
    ifNull(
      argMaxIf(
        coalesce(
          nullIf(trimBoth(JSONExtractString(payload_json, 'title')), ''),
          nullIf(trimBoth(JSONExtractString(payload_json, 'name')), '')
        ),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
          AND (
            notEmpty(trimBoth(JSONExtractString(payload_json, 'title')))
            OR notEmpty(trimBoth(JSONExtractString(payload_json, 'name')))
          )
      ),
      ''
    ) AS latest_explicit_title,
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
    ) AS latest_session_meta_title,
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
    ) AS latest_session_meta_summary,
    ifNull(
      argMaxIf(
        nullIf(JSONExtractString(payload_json, 'title'), ''),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
          OR (e.source_name = 'omp' AND JSONExtractString(e.payload_json, 'type') IN ('title', 'title_change'))
      ),
      ''
    ) AS latest_metadata_title,
    ifNull(
      argMaxIf(
        nullIf(JSONExtractString(payload_json, 'name'), ''),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
      ),
      ''
    ) AS latest_metadata_name,
    ifNull(
      argMaxIf(
        nullIf(JSONExtractString(payload_json, 'summary'), ''),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
      ),
      ''
    ) AS latest_metadata_summary,
    ifNull(
      argMinIf(
        nullIf(
          trimBoth(
            replaceRegexpOne(
              arrayElement(splitByChar('/', replaceAll(e.source_file, '\\\\', '/')), -1),
              '[.]jsonl$',
              ''
            )
          ),
          ''
        ),
        tuple(event_ts, event_uid),
        e.source_name = 'omp'
          AND notEmpty(e.session_id)
          AND endsWith(e.source_file, '.jsonl')
          AND NOT endsWith(e.source_file, concat(e.session_id, '.jsonl'))
      ),
      ''
    ) AS omp_dispatch_title,
    ifNull(
      argMaxIf(
        nullIf(JSONExtractString(payload_json, 'slug'), ''),
        tuple(event_ts, event_uid),
        event_kind = 'session_meta'
      ),
      ''
    ) AS session_slug
  FROM (
    {metadata_rows}
  ) AS e
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
    if(
      ifNull(r.latest_source_name, '') = 'omp',
      coalesce(
        nullIf(x.latest_metadata_title, ''),
        nullIf(x.latest_metadata_name, ''),
        nullIf(x.latest_metadata_summary, ''),
        nullIf(x.omp_dispatch_title, ''),
        ''
      ),
      ifNull(x.latest_session_meta_title, '')
    ) AS title,
    ifNull(r.latest_source_name, '') AS source,
    ifNull(r.latest_harness, '') AS harness,
    ifNull(x.session_slug, '') AS session_slug,
    if(
      ifNull(r.latest_source_name, '') = 'omp',
      coalesce(
        nullIf(x.latest_metadata_summary, ''),
        nullIf(x.latest_metadata_title, ''),
        nullIf(x.latest_metadata_name, ''),
        nullIf(x.omp_dispatch_title, ''),
        ''
      ),
      ifNull(x.latest_session_meta_summary, '')
    ) AS session_summary,
    ifNull(x.latest_explicit_title, '') AS explicit_title,
    if(
      ifNull(r.latest_harness, '') = 'codex',
      ifNull(r.codex_user_message_preview, ''),
      ''
    ) AS codex_user_message_preview
  FROM page_sessions AS w
  ANY LEFT JOIN event_rollups AS r ON r.session_id = w.session_id
  ANY LEFT JOIN metadata_rollups AS x ON x.session_id = w.session_id
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
  session_summary,
  explicit_title,
  codex_user_message_preview
FROM candidate_sessions
ORDER BY last_event_unix_ms {order_dir}, session_id {order_dir}
FORMAT JSONEachRow",
            summary = summary,
            events_ref = events_ref,
            ids_sql = ids_sql,
            metadata_rows = metadata_rows,
            mode_aggregate = mode_aggregate,
            order_dir = order_dir,
        )
        };

        let rows: Vec<McpSessionListRow> = self
            .query_session_page_chunks(&page_ids, SESSION_LIST_CHUNK, build_query)
            .await?;

        let mut items: Vec<McpSessionListItem> = rows
            .into_iter()
            .map(Self::map_mcp_session_list_row)
            .collect();

        let next_cursor = if has_more {
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
