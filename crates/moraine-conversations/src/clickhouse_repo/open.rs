use super::*;

impl ClickHouseConversationRepository {
    pub(super) async fn load_turns_for_session(
        &self,
        session_id: &str,
    ) -> RepoResult<Vec<TurnSummary>> {
        let turn_summary = self.table_ref("v_turn_summary");
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
WHERE session_id = {}
ORDER BY turn_seq ASC
FORMAT JSONEachRow",
            sql_quote(session_id),
        );

        let rows: Vec<TurnSummaryRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().map(Self::map_turn_row).collect())
    }

    pub(super) async fn load_conversation_summary(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<ConversationSummary>> {
        let session_summary = self.table_ref("v_session_summary");
        let mode_subquery = self.mode_subquery();
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
  ifNull(m.mode, 'chat') AS mode
FROM {session_summary} AS s
LEFT JOIN ({mode_subquery}) AS m ON m.session_id = s.session_id
WHERE s.session_id = {}
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(session_id),
        );

        let rows: Vec<ConversationSummaryRow> =
            self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().map(Self::map_conversation_row))
    }

    pub(super) async fn load_session_metadata(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<SessionMetadata>> {
        let session_summary = self.table_ref("v_session_summary");
        let mode_subquery = self.mode_subquery();
        let trace_table = self.table_ref("v_conversation_trace");
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
  ifNull(e.first_event_uid, '') AS first_event_uid,
  ifNull(e.last_event_uid, '') AS last_event_uid,
  ifNull(e.last_actor_role, '') AS last_actor_role
FROM {session_summary} AS s
LEFT JOIN ({mode_subquery}) AS m ON m.session_id = s.session_id
LEFT JOIN (
  SELECT
    session_id,
    argMin(event_uid, tuple(event_time, event_order, event_uid)) AS first_event_uid,
    argMax(event_uid, tuple(event_time, event_order, event_uid)) AS last_event_uid,
    argMax(actor_role, tuple(event_time, event_order, event_uid)) AS last_actor_role
  FROM {trace_table}
  WHERE session_id = {session_id_sql}
  GROUP BY session_id
) AS e ON e.session_id = s.session_id
WHERE s.session_id = {session_id_sql}
LIMIT 1
FORMAT JSONEachRow",
            session_summary = session_summary,
            mode_subquery = mode_subquery,
            trace_table = trace_table,
            session_id_sql = sql_quote(session_id),
        );

        let rows: Vec<SessionMetadataRow> =
            self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().map(Self::map_session_metadata_row))
    }

    pub(super) async fn load_mcp_session_info(
        &self,
        session_id: &str,
    ) -> RepoResult<McpSessionInfoRow> {
        let events_source = canonical_events_source(&self.table_ref("events"));
        let query = format!(
            "SELECT
  ifNull(
    argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'),
    ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'), '')
  ) AS title,
  ifNull(
    argMaxIf(nullIf(JSONExtractString(payload_json, 'source'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'),
    ifNull(argMax(nullIf(source_name, ''), tuple(event_ts, event_uid)), '')
  ) AS source,
  ifNull(argMax(nullIf(harness, ''), tuple(event_ts, event_uid)), '') AS harness,
  ifNull(argMax(nullIf(inference_provider, ''), tuple(event_ts, event_uid)), '') AS inference_provider,
  ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'), '') AS session_slug,
  ifNull(
    argMaxIf(nullIf(JSONExtractString(payload_json, 'summary'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'),
    ifNull(
      argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'),
      ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''), tuple(event_ts, event_uid), event_kind = 'session_meta'), '')
    )
  ) AS session_summary
FROM {events_source}
WHERE session_id = {}
GROUP BY session_id
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(session_id),
        );

        let rows: Vec<McpSessionInfoRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().unwrap_or_default())
    }

    pub(super) async fn load_turn_summary(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<TurnSummary>> {
        let turn_summary = self.table_ref("v_turn_summary");
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
WHERE session_id = {} AND turn_seq = {}
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(session_id),
            turn_seq,
        );

        let rows: Vec<TurnSummaryRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().map(Self::map_turn_row))
    }

    pub(super) async fn load_event_by_uid(
        &self,
        event_uid: &str,
    ) -> RepoResult<Option<TraceEvent>> {
        let trace_table = self.table_ref("v_conversation_trace");
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
WHERE event_uid = {}
ORDER BY event_order DESC
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(event_uid),
        );

        let rows: Vec<TraceEventRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().map(Self::map_trace_event))
    }

    pub(super) async fn load_event_session_id(
        &self,
        event_uid: &str,
    ) -> RepoResult<Option<String>> {
        let documents_table = self.table_ref("search_documents");
        let docs_query = format!(
            "SELECT
  argMax(session_id, doc_version) AS session_id
FROM {documents_table}
WHERE event_uid = {}
GROUP BY event_uid
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(event_uid),
        );
        let rows: Vec<EventSessionRow> =
            self.map_backend(self.query_rows(&docs_query, None).await)?;
        if let Some(row) = rows.into_iter().next() {
            if !row.session_id.is_empty() {
                return Ok(Some(row.session_id));
            }
        }

        let trace_table = self.table_ref("v_conversation_trace");
        let trace_query = format!(
            "SELECT session_id
FROM {trace_table}
WHERE event_uid = {}
ORDER BY event_time DESC, event_order DESC, session_id ASC
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(event_uid),
        );
        let rows: Vec<EventSessionRow> =
            self.map_backend(self.query_rows(&trace_query, None).await)?;
        Ok(rows
            .into_iter()
            .next()
            .map(|row| row.session_id)
            .filter(|session_id| !session_id.is_empty()))
    }

    pub(super) async fn load_mcp_session_events(
        &self,
        session_id: &str,
        turn_seq: Option<u32>,
        target_full_event_uid: Option<&str>,
    ) -> RepoResult<Vec<TraceEvent>> {
        let trace_table = self.table_ref("v_conversation_trace");
        let text_limit = usize::from(self.cfg.preview_chars).max(4);
        let payload_limit = text_limit.saturating_mul(2);
        let text_preview_expr = truncated_utf8_sql("text_content", text_limit);
        let payload_preview_expr = truncated_utf8_sql("payload_json", payload_limit);
        let (text_expr, payload_expr) = if let Some(event_uid) = target_full_event_uid {
            let event_uid_sql = sql_quote(event_uid);
            (
                format!("if(event_uid = {event_uid_sql}, text_content, {text_preview_expr})"),
                format!("if(event_uid = {event_uid_sql}, payload_json, {payload_preview_expr})"),
            )
        } else {
            (text_preview_expr, payload_preview_expr)
        };
        let turn_filter = turn_seq
            .map(|turn_seq| format!(" AND turn_seq = {turn_seq}"))
            .unwrap_or_default();
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
  {text_expr} AS text_content,
  {payload_expr} AS payload_json,
  token_usage_json,
  endpoint_kind,
  token_usage_buckets,
  token_usage_native_units
FROM {trace_table} AS tr
WHERE session_id = {}{turn_filter}
ORDER BY event_order ASC, event_uid ASC
FORMAT JSONEachRow",
            sql_quote(session_id),
        );

        let rows: Vec<TraceEventRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().map(Self::map_trace_event).collect())
    }

    pub(super) async fn load_events_for_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Vec<TraceEvent>> {
        let trace_table = self.table_ref("v_conversation_trace");
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
WHERE session_id = {} AND turn_seq = {}
ORDER BY event_order ASC
FORMAT JSONEachRow",
            sql_quote(session_id),
            turn_seq,
        );

        let rows: Vec<TraceEventRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().map(Self::map_trace_event).collect())
    }

    pub(super) fn mcp_event_ref(event: &TraceEvent) -> McpEventRef {
        McpEventRef {
            session_id: event.session_id.clone(),
            event_uid: event.event_uid.clone(),
            event_order: event.event_order,
            turn_seq: event.turn_seq,
            event_time: event.event_time.clone(),
            event_type: Self::normalized_event_type(
                &event.event_class,
                &event.payload_type,
                &event.actor_role,
            ),
        }
    }

    pub(super) fn mcp_turn_ref_from_summary(summary: &TurnSummary) -> McpTurnRef {
        McpTurnRef {
            session_id: summary.session_id.clone(),
            turn_seq: summary.turn_seq,
            turn_id: summary.turn_id.clone(),
            started_at: summary.started_at.clone(),
            ended_at: summary.ended_at.clone(),
        }
    }

    pub(super) fn mcp_event_summary(&self, event: &TraceEvent) -> McpEventSummary {
        McpEventSummary {
            session_id: event.session_id.clone(),
            event_uid: event.event_uid.clone(),
            event_order: event.event_order,
            turn_seq: event.turn_seq,
            event_time: event.event_time.clone(),
            event_unix_ms: event.event_unix_ms,
            actor_role: event.actor_role.clone(),
            event_class: event.event_class.clone(),
            payload_type: event.payload_type.clone(),
            event_type: Self::normalized_event_type(
                &event.event_class,
                &event.payload_type,
                &event.actor_role,
            ),
            call_id: event.call_id.clone(),
            name: event.name.clone(),
            phase: event.phase.clone(),
            text_preview: Self::compact_event_text(event, self.cfg.preview_chars),
        }
    }

    pub(super) fn compact_event_text(event: &TraceEvent, preview_chars: u16) -> Option<String> {
        let source = if event.text_content.trim().is_empty() {
            event.payload_json.as_str()
        } else {
            event.text_content.as_str()
        };
        let compact = compact_text_line(source, usize::from(preview_chars).max(1));
        (!compact.is_empty()).then_some(compact)
    }

    pub(super) fn mcp_turn_compact(
        &self,
        summary: TurnSummary,
        events: &[TraceEvent],
    ) -> McpTurnCompact {
        let user_input_event = events.iter().find(|event| {
            event.actor_role.eq_ignore_ascii_case("user")
                && Self::matches_event_kind(
                    &event.event_class,
                    &event.payload_type,
                    SearchEventKind::Message,
                )
        });
        let user_input_summary = user_input_event
            .and_then(|event| Self::compact_event_text(event, self.cfg.preview_chars));

        let final_response_event = events.iter().rev().find(|event| {
            event.actor_role.eq_ignore_ascii_case("assistant")
                && Self::matches_event_kind(
                    &event.event_class,
                    &event.payload_type,
                    SearchEventKind::Message,
                )
        });
        let final_response_summary = final_response_event
            .and_then(|event| Self::compact_event_text(event, self.cfg.preview_chars));

        let mut tools_called = Vec::<String>::new();
        let mut normalized_event_types = Vec::<String>::new();
        for event in events {
            let event_type = Self::normalized_event_type(
                &event.event_class,
                &event.payload_type,
                &event.actor_role,
            );
            push_first_seen(&mut normalized_event_types, event_type.clone());
            if event_type == McpEventType::ToolCall.as_str() {
                let tool_name = if event.name.trim().is_empty() {
                    event.call_id.trim()
                } else {
                    event.name.trim()
                };
                if !tool_name.is_empty() {
                    push_first_seen(&mut tools_called, tool_name.to_string());
                }
            }
        }

        let terminal_event = events.iter().rev().find_map(|event| {
            Self::turn_terminal_completed(event).map(|completed| (event, completed))
        });
        let completed = terminal_event
            .map(|(_, completed)| completed)
            .unwrap_or(false);
        let terminal_event_uid = terminal_event.map(|(event, _)| event.event_uid.clone());

        McpTurnCompact {
            metadata: summary,
            user_input_summary,
            final_response_summary,
            user_input_event: user_input_event.map(Self::mcp_event_ref),
            final_response_event: final_response_event.map(Self::mcp_event_ref),
            tools_called,
            normalized_event_types,
            completed,
            terminal_event_uid,
            first_event: events.first().map(Self::mcp_event_ref),
            last_event: events.last().map(Self::mcp_event_ref),
        }
    }

    pub(super) fn mcp_turn_open(
        &self,
        summary: TurnSummary,
        events: Vec<TraceEvent>,
        previous_turn: Option<McpTurnRef>,
        next_turn: Option<McpTurnRef>,
    ) -> McpTurnOpen {
        let compact = self.mcp_turn_compact(summary, &events);
        let events = events
            .iter()
            .map(|event| self.mcp_event_summary(event))
            .collect();

        McpTurnOpen {
            metadata: compact.metadata,
            events,
            user_input_summary: compact.user_input_summary,
            final_response_summary: compact.final_response_summary,
            tools_called: compact.tools_called,
            normalized_event_types: compact.normalized_event_types,
            completed: compact.completed,
            terminal_event_uid: compact.terminal_event_uid,
            previous_turn,
            next_turn,
            first_event: compact.first_event,
            last_event: compact.last_event,
        }
    }

    pub(super) fn normalized_event_type(
        event_class: &str,
        payload_type: &str,
        actor_role: &str,
    ) -> String {
        Self::mcp_event_type_for(event_class, payload_type, actor_role)
            .as_str()
            .to_string()
    }

    pub(super) fn turn_terminal_completed(event: &TraceEvent) -> Option<bool> {
        match event.payload_type.as_str() {
            "task_complete" => Some(true),
            "turn_aborted" => Some(false),
            _ => None,
        }
    }

    pub(super) async fn get_conversation_impl(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        Self::validate_session_id(session_id)?;

        let Some(summary) = self.load_conversation_summary(session_id).await? else {
            return Ok(None);
        };

        let turns = if opts.include_turns {
            self.load_turns_for_session(session_id).await?
        } else {
            Vec::new()
        };

        Ok(Some(Conversation { summary, turns }))
    }

    pub(super) async fn get_session_metadata_impl(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<SessionMetadata>> {
        Self::validate_session_id(session_id)?;
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        self.load_session_metadata(session_id).await
    }

    pub(super) async fn get_mcp_session_impl(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<McpSessionOpen>> {
        Self::validate_session_id(session_id)?;
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }

        let Some(metadata) = self.load_session_metadata(session_id).await? else {
            return Ok(None);
        };
        let events = self.load_mcp_session_events(session_id, None, None).await?;
        let session_info = self.load_mcp_session_info(session_id).await?;
        let turn_summaries = self.load_turns_for_session(session_id).await?;
        let mut events_by_turn = BTreeMap::<u32, Vec<TraceEvent>>::new();
        for event in events {
            events_by_turn
                .entry(event.turn_seq)
                .or_default()
                .push(event);
        }

        let turns = turn_summaries
            .into_iter()
            .map(|summary| {
                let events = events_by_turn
                    .get(&summary.turn_seq)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                self.mcp_turn_compact(summary, events)
            })
            .collect::<Vec<_>>();

        let completed = turns.last().map(|turn| turn.completed).unwrap_or(false);
        let terminal_event_uid = turns
            .last()
            .and_then(|turn| turn.terminal_event_uid.clone());
        Ok(Some(McpSessionOpen {
            metadata,
            title: non_empty_string(session_info.title),
            source: non_empty_string(session_info.source),
            harness: non_empty_string(session_info.harness),
            inference_provider: non_empty_string(session_info.inference_provider),
            session_slug: non_empty_string(session_info.session_slug),
            session_summary: non_empty_string(session_info.session_summary),
            completed,
            terminal_event_uid,
            turns,
        }))
    }

    pub(super) async fn get_turn_impl(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<Turn>> {
        Self::validate_session_id(session_id)?;

        let Some(summary) = self.load_turn_summary(session_id, turn_seq).await? else {
            return Ok(None);
        };
        let events = self.load_events_for_turn(session_id, turn_seq).await?;

        Ok(Some(Turn { summary, events }))
    }

    pub(super) async fn get_mcp_turn_impl(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        Self::validate_session_id(session_id)?;
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }

        let turn_summaries = self.load_turns_for_session(session_id).await?;
        let Some(summary) = turn_summaries
            .iter()
            .find(|summary| summary.turn_seq == turn_seq)
            .cloned()
        else {
            return Ok(None);
        };
        let previous_turn = turn_summaries
            .iter()
            .rev()
            .find(|summary| summary.turn_seq < turn_seq)
            .map(Self::mcp_turn_ref_from_summary);
        let next_turn = turn_summaries
            .iter()
            .find(|summary| summary.turn_seq > turn_seq)
            .map(Self::mcp_turn_ref_from_summary);
        let events = self
            .load_mcp_session_events(session_id, Some(turn_seq), None)
            .await?;

        Ok(Some(self.mcp_turn_open(
            summary,
            events,
            previous_turn,
            next_turn,
        )))
    }

    pub(super) async fn open_event_impl(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        let event_uid = req.event_uid.trim();
        if event_uid.is_empty() {
            return Err(RepoError::invalid_argument("event_uid cannot be empty"));
        }
        Self::validate_event_uid(event_uid)?;

        let before = req.before.unwrap_or(self.cfg.default_context_before);
        let after = req.after.unwrap_or(self.cfg.default_context_after);
        let include_system_events = req.include_system_events.unwrap_or(false);
        let context_filter = Self::open_context_filter_clause(include_system_events);
        let trace_table = self.table_ref("v_conversation_trace");

        let target_query = format!(
            "SELECT session_id, event_order, turn_seq FROM {trace_table} WHERE event_uid = {} ORDER BY event_order DESC LIMIT 1 FORMAT JSONEachRow",
            sql_quote(event_uid)
        );

        let targets: Vec<OpenTargetRow> =
            self.map_backend(self.query_rows(&target_query, None).await)?;
        let Some(target) = targets.first() else {
            return Ok(OpenContext {
                found: false,
                event_uid: event_uid.to_string(),
                session_id: String::new(),
                target_event_order: 0,
                turn_seq: 0,
                before,
                after,
                events: Vec::new(),
            });
        };

        let target_row_query = format!(
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
WHERE session_id = {} AND event_order = {} AND event_uid = {}
ORDER BY event_order ASC
FORMAT JSONEachRow",
            sql_quote(&target.session_id),
            target.event_order,
            sql_quote(event_uid),
        );

        let mut before_rows: Vec<TraceEventRow> = if before == 0 {
            Vec::new()
        } else {
            let before_query = format!(
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
WHERE session_id = {} AND event_order < {}{}
ORDER BY event_order DESC
LIMIT {}
FORMAT JSONEachRow",
                sql_quote(&target.session_id),
                target.event_order,
                context_filter,
                before,
            );

            self.map_backend(self.query_rows(&before_query, None).await)?
        };
        if !include_system_events {
            before_rows.retain(|row| {
                !Self::is_low_information_system_event(&row.actor_role, &row.payload_type)
            });
        }
        before_rows.reverse();

        let target_rows: Vec<TraceEventRow> =
            self.map_backend(self.query_rows(&target_row_query, None).await)?;

        let mut after_rows: Vec<TraceEventRow> = if after == 0 {
            Vec::new()
        } else {
            let after_query = format!(
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
WHERE session_id = {} AND event_order > {}{}
ORDER BY event_order ASC
LIMIT {}
FORMAT JSONEachRow",
                sql_quote(&target.session_id),
                target.event_order,
                context_filter,
                after,
            );

            self.map_backend(self.query_rows(&after_query, None).await)?
        };
        if !include_system_events {
            after_rows.retain(|row| {
                !Self::is_low_information_system_event(&row.actor_role, &row.payload_type)
            });
        }

        let mut rows = Vec::with_capacity(before_rows.len() + target_rows.len() + after_rows.len());
        rows.extend(before_rows);
        rows.extend(target_rows);
        rows.extend(after_rows);

        let events: Vec<OpenEvent> = rows
            .into_iter()
            .map(|row| OpenEvent {
                is_target: row.event_uid == event_uid,
                session_id: row.session_id,
                event_uid: row.event_uid,
                event_order: row.event_order,
                turn_seq: row.turn_seq,
                event_time: row.event_time,
                actor_role: row.actor_role,
                event_class: row.event_class,
                payload_type: row.payload_type,
                call_id: row.call_id,
                name: row.name,
                phase: row.phase,
                item_id: row.item_id,
                source_ref: row.source_ref,
                text_content: row.text_content,
                payload_json: row.payload_json,
                token_usage_json: row.token_usage_json,
                endpoint_kind: row.endpoint_kind,
                token_usage_buckets: row.token_usage_buckets,
                token_usage_native_units: row.token_usage_native_units,
            })
            .collect();

        Ok(OpenContext {
            found: true,
            event_uid: event_uid.to_string(),
            session_id: target.session_id.clone(),
            target_event_order: target.event_order,
            turn_seq: target.turn_seq,
            before,
            after,
            events,
        })
    }

    pub(super) async fn get_mcp_event_impl(
        &self,
        event_uid: &str,
    ) -> RepoResult<Option<McpEventOpen>> {
        let event_uid = event_uid.trim();
        if event_uid.is_empty() {
            return Err(RepoError::invalid_argument("event_uid cannot be empty"));
        }
        Self::validate_event_uid(event_uid)?;

        let session_id = if let Some(session_id) = self.load_event_session_id(event_uid).await? {
            session_id
        } else if let Some(event) = self.load_event_by_uid(event_uid).await? {
            event.session_id
        } else {
            return Ok(None);
        };
        // Out-of-scope events answer exactly like missing ones, so an event
        // ID cannot be used to probe sessions outside the project scope.
        if !self.session_in_scope(&session_id).await? {
            return Ok(None);
        }

        let events = self
            .load_mcp_session_events(&session_id, None, Some(event_uid))
            .await?;
        let Some(target_index) = events.iter().position(|event| event.event_uid == event_uid)
        else {
            return Ok(None);
        };
        let event = events[target_index].clone();
        let Some(parent_session) = self.load_session_metadata(&session_id).await? else {
            return Ok(None);
        };
        let parent_session_info = self.load_mcp_session_info(&session_id).await?;
        let turn_summaries = self.load_turns_for_session(&session_id).await?;
        let Some(parent_turn) = turn_summaries
            .iter()
            .find(|summary| summary.turn_seq == event.turn_seq)
            .cloned()
        else {
            return Ok(None);
        };
        let previous_turn = turn_summaries
            .iter()
            .rev()
            .find(|summary| summary.turn_seq < event.turn_seq)
            .map(Self::mcp_turn_ref_from_summary);
        let next_turn = turn_summaries
            .iter()
            .find(|summary| summary.turn_seq > event.turn_seq)
            .map(Self::mcp_turn_ref_from_summary);
        let previous_event = target_index
            .checked_sub(1)
            .map(|index| Self::mcp_event_ref(&events[index]));
        let next_event = events.get(target_index + 1).map(Self::mcp_event_ref);
        let turn_events = events
            .iter()
            .filter(|candidate| candidate.turn_seq == event.turn_seq)
            .cloned()
            .collect::<Vec<_>>();
        let event_ordinal = turn_events
            .iter()
            .position(|turn_event| turn_event.event_uid == event_uid)
            .map(|index| (index + 1) as u32)
            .unwrap_or(1);
        let turn_compact = self.mcp_turn_compact(parent_turn.clone(), &turn_events);

        Ok(Some(McpEventOpen {
            event_type: Self::normalized_event_type(
                &event.event_class,
                &event.payload_type,
                &event.actor_role,
            ),
            event,
            event_ordinal,
            turn_completed: turn_compact.completed,
            turn_terminal_event_uid: turn_compact.terminal_event_uid,
            parent_session,
            parent_session_source: non_empty_string(parent_session_info.source),
            parent_turn,
            previous_event,
            next_event,
            previous_turn,
            next_turn,
        }))
    }
}
