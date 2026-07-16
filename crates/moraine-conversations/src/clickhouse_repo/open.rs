use super::*;
use crate::domain::McpOpenSnapshot;

const MAX_MCP_OPEN_SNAPSHOT_ATTEMPTS: usize = 4;

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
        let trace_table = self.table_ref("v_conversation_trace");
        let session_id_sql = sql_quote(session_id);
        let single_session_sql = format!("SELECT {session_id_sql}");
        let mode_subquery = self.mode_subquery_for_sessions(Some(&single_session_sql));
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
            session_id_sql = session_id_sql,
        );

        let rows: Vec<SessionMetadataRow> =
            self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().map(Self::map_session_metadata_row))
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
        self.ensure_mcp_open_read_model_ready().await?;
        for _ in 0..MAX_MCP_OPEN_SNAPSHOT_ATTEMPTS {
            let Some(session) = self.load_projected_session(session_id).await? else {
                return Ok(None);
            };
            if !self.projected_session_in_scope(&session) {
                return Ok(None);
            }

            let load_started = Instant::now();
            let turns = self
                .load_projected_turns(&session, None, false)
                .await?
                .into_iter()
                .map(|turn| turn.compact)
                .collect();
            if !self.projected_snapshot_still_current(&session).await? {
                continue;
            }
            tracing::debug!(
                elapsed_ms = load_started.elapsed().as_millis() as u64,
                "mcp_open_session_load"
            );
            return Ok(Some(McpSessionOpen {
                metadata: session.metadata,
                title: non_empty_string(session.row.title),
                source: non_empty_string(session.row.source),
                harness: non_empty_string(session.row.harness),
                inference_provider: non_empty_string(session.row.inference_provider),
                session_slug: non_empty_string(session.row.session_slug),
                session_summary: non_empty_string(session.row.session_summary),
                turns,
                completed: session.row.completed != 0,
                terminal_event_uid: non_empty_string(session.row.terminal_event_uid),
                snapshot: Some(McpOpenSnapshot {
                    slot: session.row.slot,
                    generation: session.row.generation,
                }),
            }));
        }
        Err(RepoError::backend(
            "MCP open session snapshot changed repeatedly",
        ))
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
        include_events: bool,
    ) -> RepoResult<Option<McpTurnOpen>> {
        Self::validate_session_id(session_id)?;
        self.ensure_mcp_open_read_model_ready().await?;
        for _ in 0..MAX_MCP_OPEN_SNAPSHOT_ATTEMPTS {
            let Some(session) = self.load_projected_session(session_id).await? else {
                return Ok(None);
            };
            if !self.projected_session_in_scope(&session) {
                return Ok(None);
            }

            let load_started = Instant::now();
            let turn = self
                .load_projected_turns(&session, Some(turn_seq), include_events)
                .await?
                .into_iter()
                .next();
            if !self.projected_snapshot_still_current(&session).await? {
                continue;
            }
            let Some(turn) = turn else {
                return Ok(None);
            };
            tracing::debug!(
                elapsed_ms = load_started.elapsed().as_millis() as u64,
                "mcp_open_turn_load"
            );
            return Ok(Some(McpTurnOpen {
                metadata: turn.compact.metadata,
                events: turn.events,
                parent_session_source: non_empty_string(session.row.source),
                user_input_summary: turn.compact.user_input_summary,
                final_response_summary: turn.compact.final_response_summary,
                user_input_event: turn.compact.user_input_event,
                final_response_event: turn.compact.final_response_event,
                tools_called: turn.compact.tools_called,
                normalized_event_types: turn.compact.normalized_event_types,
                completed: turn.compact.completed,
                terminal_event_uid: turn.compact.terminal_event_uid,
                previous_turn: turn.previous_turn,
                next_turn: turn.next_turn,
                first_event: turn.compact.first_event,
                last_event: turn.compact.last_event,
                snapshot: Some(McpOpenSnapshot {
                    slot: session.row.slot,
                    generation: session.row.generation,
                }),
            }));
        }
        Err(RepoError::backend(
            "MCP open turn snapshot changed repeatedly",
        ))
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
        self.ensure_mcp_open_read_model_ready().await?;

        for _ in 0..MAX_MCP_OPEN_SNAPSHOT_ATTEMPTS {
            let candidates = self.load_projected_event_candidates(event_uid).await?;
            let stale_candidates_observed = !candidates.is_empty();
            let mut pinned = None;
            for lookup in candidates {
                let Some(session) = self.load_projected_session(&lookup.session_id).await? else {
                    continue;
                };
                if session.row.slot == lookup.slot && session.row.generation == lookup.generation {
                    pinned = Some((lookup, session));
                    break;
                }
            }
            let Some((lookup, session)) = pinned else {
                if stale_candidates_observed {
                    continue;
                }
                return Ok(None);
            };
            // Authorize the narrow UID ownership row before reading event content.
            if !self.projected_session_in_scope(&session) {
                return Ok(None);
            }

            let load_started = Instant::now();
            let row = self.load_projected_event(&lookup).await?;
            let parent_turn = match &row {
                Some(row) => self
                    .load_projected_turns(&session, Some(row.turn_seq), false)
                    .await?
                    .into_iter()
                    .next(),
                None => None,
            };
            let mut neighbors = match &row {
                Some(row) => {
                    let neighbor_uids =
                        [row.previous_event_uid.clone(), row.next_event_uid.clone()]
                            .into_iter()
                            .filter(|event_uid| !event_uid.is_empty())
                            .collect::<Vec<_>>();
                    self.load_projected_event_refs(neighbor_uids, lookup.slot, lookup.generation)
                        .await?
                }
                None => HashMap::new(),
            };
            if !self.projected_snapshot_still_current(&session).await? {
                continue;
            }
            let (Some(row), Some(parent_turn)) = (row, parent_turn) else {
                return Ok(None);
            };
            tracing::debug!(
                elapsed_ms = load_started.elapsed().as_millis() as u64,
                "mcp_open_event_load"
            );

            let previous_event = neighbors.remove(&row.previous_event_uid);
            let next_event = neighbors.remove(&row.next_event_uid);
            let event_type = row.event_type;
            let event_ordinal = row.event_ordinal;
            let event = TraceEvent {
                session_id: row.session_id,
                event_uid: row.event_uid,
                event_order: row.event_order,
                turn_seq: row.turn_seq,
                event_time: row.event_time,
                event_unix_ms: row.event_unix_ms,
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
            };
            return Ok(Some(McpEventOpen {
                event,
                event_type,
                event_ordinal,
                turn_completed: parent_turn.compact.completed,
                turn_terminal_event_uid: parent_turn.compact.terminal_event_uid,
                parent_session: session.metadata,
                parent_session_source: non_empty_string(session.row.source),
                parent_turn: parent_turn.compact.metadata,
                previous_event,
                next_event,
                previous_turn: parent_turn.previous_turn,
                next_turn: parent_turn.next_turn,
            }));
        }
        Err(RepoError::backend(
            "MCP open event snapshot changed repeatedly",
        ))
    }
}
