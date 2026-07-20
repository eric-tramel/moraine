use super::*;

pub(super) struct ProjectedSession {
    pub(super) row: McpOpenSessionRow,
    pub(super) metadata: SessionMetadata,
}

impl ProjectedSession {
    pub(super) fn same_snapshot(&self, other: &Self) -> bool {
        self.row.candidate_publication_id == other.row.candidate_publication_id
            && self.row.slot == other.row.slot
            && self.row.generation == other.row.generation
            && self.row.source_revision == other.row.source_revision
            && self.row.dirty_revision == other.row.dirty_revision
            && self.row.required_heads_fingerprint == other.row.required_heads_fingerprint
    }
}

pub(super) struct ProjectedTurn {
    pub(super) compact: McpTurnCompact,
    pub(super) events: Vec<McpEventSummary>,
    pub(super) previous_turn: Option<McpTurnRef>,
    pub(super) next_turn: Option<McpTurnRef>,
}

impl ClickHouseConversationRepository {
    pub(super) async fn ensure_mcp_open_read_model_ready(&self) -> RepoResult<()> {
        let ready = self.map_backend(self.ch.mcp_open_read_model_ready().await)?;
        if ready {
            Ok(())
        } else {
            Err(RepoError::backend(
                "MCP open read model is not ready; run `moraine db migrate`",
            ))
        }
    }

    pub(super) async fn projected_snapshot_still_current(
        &self,
        pinned: &ProjectedSession,
    ) -> RepoResult<bool> {
        Ok(self
            .load_projected_session(&pinned.row.session_id)
            .await?
            .is_some_and(|current| pinned.same_snapshot(&current)))
    }

    pub(super) async fn load_projected_session(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<ProjectedSession>> {
        let snapshot = active_publication_snapshot();
        let (sessions, snapshot_cte, authorization, revision_columns, ordering) = if let Some(
            snapshot,
        ) =
            snapshot.as_ref()
        {
            let history = self.table_ref("v_published_source_generation_history");
            let captured_heads = snapshot.captured_source_heads_sql(&history);
            let live_events = self.live_events_source();
            (
                    self.table_ref("mcp_open_publication_headers"),
                    format!("WITH {captured_heads} AS captured_heads\n"),
                    format!(
                        "\n  AND length(s.required_source_heads) > 0\n  AND arrayAll(required_head -> has(captured_heads, required_head), s.required_source_heads)\n  AND s.dirty_revision = (\n    SELECT if(count() = 0, toUInt64(0), toUInt64(max(dirty.dirty_revision)))\n    FROM {} AS dirty FINAL WHERE dirty.session_id = {}\n  )\n  AND s.source_revision = (\n    SELECT if(count() = 0, toUInt64(0), toUInt64(cityHash64(arraySort(groupArray(tuple(e.event_uid, e.event_version))))))\n    FROM {live_events} AS e WHERE e.session_id = {}\n  )",
                        self.table_ref("mcp_open_dirty_sessions"),
                        sql_quote(session_id),
                        sql_quote(session_id),
                    ),
                    "candidate_publication_id, toUInt64(source_revision) AS source_revision, toUInt64(dirty_revision) AS dirty_revision, toUInt8(tombstone) AS tombstone, required_heads_fingerprint,",
                    "ORDER BY header_revision DESC",
                )
        } else {
            (
                    self.table_ref("mcp_open_sessions"),
                    String::new(),
                    String::new(),
                    "'' AS candidate_publication_id, toUInt64(source_revision) AS source_revision, toUInt64(dirty_revision) AS dirty_revision, toUInt8(0) AS tombstone, '' AS required_heads_fingerprint,",
                    "",
                )
        };
        let query = format!(
            "{snapshot_cte}SELECT
  session_id,
  {revision_columns}
  toUInt8(slot) AS slot,
  toUInt64(generation) AS generation,
  toString(s.first_event_time) AS first_event_time,
  toInt64(toUnixTimestamp64Milli(s.first_event_time)) AS first_event_unix_ms,
  toString(s.last_event_time) AS last_event_time,
  toInt64(toUnixTimestamp64Milli(s.last_event_time)) AS last_event_unix_ms,
  toUInt32(total_turns) AS total_turns,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  mode,
  first_event_uid,
  last_event_uid,
  last_actor_role,
  title,
  source,
  harness,
  inference_provider,
  session_slug,
  session_summary,
  toUInt8(completed) AS completed,
  terminal_event_uid,
  origin_cwd
FROM {sessions} AS s FINAL
WHERE s.session_id = {}{authorization}
{ordering}
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(session_id),
        );
        let rows: Vec<McpOpenSessionRow> = self.map_backend(self.query_rows(&query, None).await)?;
        let Some(row) = rows.into_iter().next() else {
            if let Some(snapshot) = snapshot.as_ref() {
                if self
                    .canonical_session_exists_in_snapshot(session_id, snapshot)
                    .await?
                {
                    return Err(RepoError::ReadModelChanged);
                }
            }
            return Ok(None);
        };
        if row.tombstone != 0 {
            return Ok(None);
        }
        Ok(Some({
            let metadata = SessionMetadata {
                session_id: row.session_id.clone(),
                first_event_time: row.first_event_time.clone(),
                first_event_unix_ms: row.first_event_unix_ms,
                last_event_time: row.last_event_time.clone(),
                last_event_unix_ms: row.last_event_unix_ms,
                total_turns: row.total_turns,
                total_events: row.total_events,
                user_messages: row.user_messages,
                assistant_messages: row.assistant_messages,
                tool_calls: row.tool_calls,
                tool_results: row.tool_results,
                mode: Self::parse_mode(&row.mode),
                first_event_uid: row.first_event_uid.clone(),
                last_event_uid: row.last_event_uid.clone(),
                last_actor_role: row.last_actor_role.clone(),
            };
            ProjectedSession { row, metadata }
        }))
    }

    pub(super) async fn canonical_session_exists_in_snapshot(
        &self,
        session_id: &str,
        _snapshot: &PublicationSnapshot,
    ) -> RepoResult<bool> {
        #[derive(Deserialize)]
        struct ExistsRow {
            exists: u8,
        }
        let query = format!(
            "SELECT toUInt8(count() > 0) AS exists\nFROM {} AS e\nWHERE e.session_id = {}\nFORMAT JSONEachRow",
            self.live_events_source(),
            sql_quote(session_id),
        );
        let rows: Vec<ExistsRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.first().is_some_and(|row| row.exists != 0))
    }

    pub(super) async fn canonical_event_exists_in_snapshot(
        &self,
        event_uid: &str,
    ) -> RepoResult<bool> {
        #[derive(Deserialize)]
        struct ExistsRow {
            exists: u8,
        }
        let Some(_snapshot) = active_publication_snapshot() else {
            return Ok(false);
        };
        let query = format!(
            "SELECT toUInt8(count() > 0) AS exists\nFROM {} AS e\nWHERE e.event_uid = {}\nFORMAT JSONEachRow",
            self.live_events_source(),
            sql_quote(event_uid),
        );
        let rows: Vec<ExistsRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.first().is_some_and(|row| row.exists != 0))
    }

    pub(super) fn projected_session_in_scope(&self, session: &ProjectedSession) -> bool {
        let Some(scope) = self.cfg.session_scope.as_ref() else {
            return true;
        };
        let origin = session.row.origin_cwd.trim_end_matches('/');
        !origin.is_empty()
            && scope.roots.iter().any(|root| {
                origin == root
                    || origin
                        .strip_prefix(root)
                        .is_some_and(|tail| tail.starts_with('/'))
            })
    }

    pub(super) async fn load_projected_turns(
        &self,
        session: &ProjectedSession,
        turn_seq: Option<u32>,
        include_events: bool,
    ) -> RepoResult<Vec<ProjectedTurn>> {
        let turns = self.table_ref("mcp_open_turns");
        let event_json = if include_events {
            "event_summaries_json"
        } else {
            "'[]'"
        };
        let turn_filter = turn_seq
            .map(|turn_seq| format!(" AND turn_seq = {turn_seq}"))
            .unwrap_or_default();
        let query = format!(
            "SELECT
  session_id,
  toUInt32(turn_seq) AS turn_seq,
  turn_id,
  toString(t.started_at) AS started_at,
  toInt64(toUnixTimestamp64Milli(t.started_at)) AS started_at_unix_ms,
  toString(t.ended_at) AS ended_at,
  toInt64(toUnixTimestamp64Milli(t.ended_at)) AS ended_at_unix_ms,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  toUInt64(reasoning_items) AS reasoning_items,
  user_input_summary_source,
  final_response_summary_source,
  toUInt8(user_input_summary_is_payload) AS user_input_summary_is_payload,
  toUInt8(final_response_summary_is_payload) AS final_response_summary_is_payload,
  user_input_event_uid,
  toUInt64(user_input_event_order) AS user_input_event_order,
  toString(user_input_event_time) AS user_input_event_time,
  user_input_event_type,
  final_response_event_uid,
  toUInt64(final_response_event_order) AS final_response_event_order,
  toString(final_response_event_time) AS final_response_event_time,
  final_response_event_type,
  tools_called,
  normalized_event_types,
  toUInt8(completed) AS completed,
  terminal_event_uid,
  first_event_uid,
  toUInt64(first_event_order) AS first_event_order,
  toString(first_event_time) AS first_event_time,
  first_event_type,
  last_event_uid,
  toUInt64(last_event_order) AS last_event_order,
  toString(last_event_time) AS last_event_time,
  last_event_type,
  toUInt32(previous_turn_seq) AS previous_turn_seq,
  previous_turn_id,
  toString(previous_turn_started_at) AS previous_turn_started_at,
  toString(previous_turn_ended_at) AS previous_turn_ended_at,
  toUInt32(next_turn_seq) AS next_turn_seq,
  next_turn_id,
  toString(next_turn_started_at) AS next_turn_started_at,
  toString(next_turn_ended_at) AS next_turn_ended_at,
  {event_json} AS event_summaries_json
FROM {turns} AS t FINAL
WHERE t.session_id = {session_id} AND t.slot = {slot} AND t.generation = {generation} AND t.turn_seq > 0{turn_filter}
ORDER BY t.turn_seq ASC
FORMAT JSONEachRow",
            session_id = sql_quote(&session.row.session_id),
            slot = session.row.slot,
            generation = session.row.generation,
        );
        let rows: Vec<McpOpenTurnRow> = self.map_backend(self.query_rows(&query, None).await)?;
        rows.into_iter()
            .map(|row| self.map_projected_turn(row))
            .collect()
    }

    pub(super) async fn load_projected_event_candidates(
        &self,
        event_uid: &str,
    ) -> RepoResult<Vec<McpOpenEventLookupRow>> {
        let events = self.table_ref("mcp_open_events");
        let query = format!(
            "SELECT
  event_uid,
  session_id,
  toUInt8(slot) AS slot,
  toUInt64(generation) AS generation
FROM {events} FINAL
WHERE event_uid = {}
ORDER BY generation DESC
LIMIT 64
FORMAT JSONEachRow",
            sql_quote(event_uid),
        );
        self.map_backend(self.query_rows(&query, None).await)
    }

    pub(super) async fn load_projected_event(
        &self,
        lookup: &McpOpenEventLookupRow,
    ) -> RepoResult<Option<McpOpenEventRow>> {
        let events = self.table_ref("mcp_open_events");
        let query = format!(
            "SELECT
  session_id,
  event_uid,
  toUInt64(event_order) AS event_order,
  toUInt32(turn_seq) AS turn_seq,
  toString(e.event_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(e.event_time)) AS event_unix_ms,
  actor_role,
  event_class,
  payload_type,
  event_type,
  toUInt32(event_ordinal) AS event_ordinal,
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
  token_usage_native_units,
  previous_event_uid,
  next_event_uid
FROM {events} AS e FINAL
WHERE e.event_uid = {event_uid} AND e.slot = {slot} AND e.generation = {generation}
LIMIT 1
FORMAT JSONEachRow",
            event_uid = sql_quote(&lookup.event_uid),
            slot = lookup.slot,
            generation = lookup.generation,
        );
        let rows: Vec<McpOpenEventRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next())
    }

    pub(super) async fn load_projected_event_refs(
        &self,
        event_uids: Vec<String>,
        slot: u8,
        generation: u64,
    ) -> RepoResult<HashMap<String, McpEventRef>> {
        if event_uids.is_empty() {
            return Ok(HashMap::new());
        }
        #[derive(Deserialize)]
        struct RefRow {
            session_id: String,
            event_uid: String,
            event_order: u64,
            turn_seq: u32,
            event_time: String,
            event_type: String,
        }
        let events = self.table_ref("mcp_open_events");
        let query = format!(
            "SELECT
  session_id,
  event_uid,
  toUInt64(event_order) AS event_order,
  toUInt32(turn_seq) AS turn_seq,
  toString(event_time) AS event_time,
  event_type
FROM {events} FINAL
WHERE event_uid IN {} AND slot = {} AND generation = {}
FORMAT JSONEachRow",
            sql_array_strings(&event_uids),
            slot,
            generation,
        );
        let rows: Vec<RefRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let event_uid = row.event_uid.clone();
                (
                    event_uid,
                    McpEventRef {
                        session_id: row.session_id,
                        event_uid: row.event_uid,
                        event_order: row.event_order,
                        turn_seq: row.turn_seq,
                        event_time: row.event_time,
                        event_type: row.event_type,
                    },
                )
            })
            .collect())
    }

    fn map_projected_turn(&self, row: McpOpenTurnRow) -> RepoResult<ProjectedTurn> {
        let metadata = TurnSummary {
            session_id: row.session_id.clone(),
            turn_seq: row.turn_seq,
            turn_id: row.turn_id.clone(),
            started_at: row.started_at.clone(),
            started_at_unix_ms: row.started_at_unix_ms,
            ended_at: row.ended_at.clone(),
            ended_at_unix_ms: row.ended_at_unix_ms,
            total_events: row.total_events,
            user_messages: row.user_messages,
            assistant_messages: row.assistant_messages,
            tool_calls: row.tool_calls,
            tool_results: row.tool_results,
            reasoning_items: row.reasoning_items,
        };
        let user_input_event = projected_event_ref(
            &row.session_id,
            row.turn_seq,
            &row.user_input_event_uid,
            row.user_input_event_order,
            &row.user_input_event_time,
            &row.user_input_event_type,
        );
        let final_response_event = projected_event_ref(
            &row.session_id,
            row.turn_seq,
            &row.final_response_event_uid,
            row.final_response_event_order,
            &row.final_response_event_time,
            &row.final_response_event_type,
        );
        let first_event = projected_event_ref(
            &row.session_id,
            row.turn_seq,
            &row.first_event_uid,
            row.first_event_order,
            &row.first_event_time,
            &row.first_event_type,
        );
        let last_event = projected_event_ref(
            &row.session_id,
            row.turn_seq,
            &row.last_event_uid,
            row.last_event_order,
            &row.last_event_time,
            &row.last_event_type,
        );
        let events: Vec<ProjectedEventSummaryRow> = serde_json::from_str(&row.event_summaries_json)
            .map_err(|error| {
                RepoError::internal(format!("invalid projected event summaries: {error}"))
            })?;
        let event_session_id = row.session_id.clone();
        let event_turn_seq = row.turn_seq;
        let events = events
            .into_iter()
            .map(|event| McpEventSummary {
                session_id: event_session_id.clone(),
                event_uid: event.event_uid,
                event_order: event.event_order,
                turn_seq: event_turn_seq,
                event_time: event.event_time,
                event_unix_ms: event.event_unix_ms,
                actor_role: event.actor_role,
                event_class: event.event_class,
                payload_type: event.payload_type,
                event_type: event.event_type,
                call_id: event.call_id,
                name: event.name,
                phase: event.phase,
                text_preview: compact_projected_source(
                    &event.summary_source,
                    event.summary_is_payload != 0,
                    self.cfg.preview_chars,
                ),
            })
            .collect();
        let previous_turn = projected_turn_ref(
            &row.session_id,
            row.previous_turn_seq,
            &row.previous_turn_id,
            &row.previous_turn_started_at,
            &row.previous_turn_ended_at,
        );
        let next_turn = projected_turn_ref(
            &row.session_id,
            row.next_turn_seq,
            &row.next_turn_id,
            &row.next_turn_started_at,
            &row.next_turn_ended_at,
        );
        let mut normalized_event_types = row.normalized_event_types;
        normalized_event_types
            .retain(|event_type| event_type.as_str() != McpEventType::Unknown.as_str());
        Ok(ProjectedTurn {
            compact: McpTurnCompact {
                metadata,
                user_input_summary: compact_projected_source(
                    &row.user_input_summary_source,
                    row.user_input_summary_is_payload != 0,
                    self.cfg.preview_chars,
                ),
                final_response_summary: compact_projected_source(
                    &row.final_response_summary_source,
                    row.final_response_summary_is_payload != 0,
                    self.cfg.preview_chars,
                ),
                user_input_event,
                final_response_event,
                tools_called: row.tools_called,
                normalized_event_types,
                completed: row.completed != 0,
                terminal_event_uid: non_empty_string(row.terminal_event_uid),
                first_event,
                last_event,
            },
            events,
            previous_turn,
            next_turn,
        })
    }
}

fn projected_event_ref(
    session_id: &str,
    turn_seq: u32,
    event_uid: &str,
    event_order: u64,
    event_time: &str,
    event_type: &str,
) -> Option<McpEventRef> {
    (!event_uid.is_empty()).then(|| McpEventRef {
        session_id: session_id.to_string(),
        event_uid: event_uid.to_string(),
        event_order,
        turn_seq,
        event_time: event_time.to_string(),
        event_type: event_type.to_string(),
    })
}

fn projected_turn_ref(
    session_id: &str,
    turn_seq: u32,
    turn_id: &str,
    started_at: &str,
    ended_at: &str,
) -> Option<McpTurnRef> {
    (turn_seq > 0).then(|| McpTurnRef {
        session_id: session_id.to_string(),
        turn_seq,
        turn_id: turn_id.to_string(),
        started_at: started_at.to_string(),
        ended_at: ended_at.to_string(),
    })
}

fn compact_projected_source(source: &str, is_payload: bool, preview_chars: u16) -> Option<String> {
    if source.trim().is_empty() {
        return None;
    }
    let output_limit = usize::from(preview_chars).max(1);
    let source_limit = if is_payload {
        output_limit.max(4).saturating_mul(2)
    } else {
        output_limit.max(4)
    };
    let truncated = truncate_with_ellipsis(source, source_limit);
    let compact = compact_text_line(&truncated, output_limit);
    (!compact.is_empty()).then_some(compact)
}

fn truncate_with_ellipsis(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let prefix = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    format!("{prefix}...")
}

#[cfg(test)]
mod tests {
    use super::compact_projected_source;

    #[test]
    fn projected_summary_sentinel_preserves_max_preview_ellipsis() {
        let text = "x".repeat(65_536);
        let text_preview = compact_projected_source(&text, false, u16::MAX).expect("text preview");
        assert_eq!(text_preview.chars().count(), usize::from(u16::MAX));
        assert!(text_preview.ends_with("..."));

        let payload = "y".repeat(131_071);
        let payload_preview =
            compact_projected_source(&payload, true, u16::MAX).expect("payload preview");
        assert_eq!(payload_preview.chars().count(), usize::from(u16::MAX));
        assert!(payload_preview.ends_with("..."));
    }
}
