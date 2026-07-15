use super::{escape_identifier, escape_literal, ClickHouseClient};
use anyhow::{bail, Context, Result};
use futures_util::{stream, TryStreamExt};
use serde::Deserialize;
use std::collections::BTreeSet;

const BACKFILL_PAGE_SIZE: usize = 64;
const REFRESH_CONCURRENCY: usize = 4;
const MAX_UNSTABLE_REFRESH_ATTEMPTS: usize = 8;
const MAX_PROJECTED_TEXT_SUMMARY_CHARS: usize = 65_536;
const MAX_PROJECTED_PAYLOAD_SUMMARY_CHARS: usize = 131_071;

#[derive(Debug, Deserialize)]
struct SessionHeadRow {
    slot: u8,
    generation: u64,
    source_revision: u64,
    dirty_revision: u64,
}

#[derive(Debug, Deserialize)]
struct SourceRevisionRow {
    source_revision: u64,
}

#[derive(Debug, Deserialize)]
struct DirtyRevisionRow {
    dirty_revision: u64,
}

#[derive(Debug, Deserialize)]
struct SessionIdRow {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct ProjectionStateRow {
    ready: u8,
    #[serde(default)]
    backfill_cursor: String,
}

impl ClickHouseClient {
    /// Rebuild complete canonical snapshots for the affected sessions and
    /// publish each session head only after its inactive children are durable.
    pub async fn refresh_mcp_open_read_model<I, S>(&self, session_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let session_ids = session_ids
            .into_iter()
            .map(|session_id| session_id.as_ref().trim().to_string())
            .filter(|session_id| !session_id.is_empty())
            .collect::<BTreeSet<_>>();

        stream::iter(session_ids.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(REFRESH_CONCURRENCY, |session_id| async move {
                self.refresh_mcp_open_session(&session_id).await
            })
            .await
    }

    /// Resume the historical projection and reconcile sessions dirtied while
    /// the DDL/backfill was running. Safe to call after every migrate command.
    pub async fn backfill_mcp_open_read_model(&self) -> Result<()> {
        self.backfill_mcp_open_read_model_with_progress(|_| {})
            .await
    }

    /// Backfill the read model and report the cumulative number of refreshed
    /// sessions after each bounded page.
    pub async fn backfill_mcp_open_read_model_with_progress<F>(
        &self,
        mut on_progress: F,
    ) -> Result<()>
    where
        F: FnMut(usize),
    {
        let mut refreshed_sessions = 0;
        let state = self.mcp_open_projection_state().await?;
        let historical_complete = state.as_ref().is_some_and(|state| state.ready == 1);
        let mut cursor = state.map_or_else(String::new, |state| state.backfill_cursor);

        if !historical_complete {
            loop {
                let query = format!(
                    "SELECT session_id\n\
                     FROM (\n\
                       SELECT DISTINCT session_id\n\
                       FROM {}.events FINAL\n\
                       WHERE session_id > {}\n\
                     )\n\
                     ORDER BY session_id ASC\n\
                     LIMIT {}\n\
                     FORMAT JSONEachRow",
                    escape_identifier(&self.cfg.database),
                    escape_literal(&cursor),
                    BACKFILL_PAGE_SIZE,
                );
                let rows: Vec<SessionIdRow> = self
                    .query_json_each_row(&query, Some(&self.cfg.database))
                    .await
                    .context("failed to page MCP open backfill sessions")?;
                if rows.is_empty() {
                    break;
                }
                self.refresh_mcp_open_read_model(rows.iter().map(|row| row.session_id.as_str()))
                    .await?;
                cursor = rows.last().expect("non-empty page").session_id.clone();
                self.set_mcp_open_projection_state(false, &cursor).await?;
                refreshed_sessions += rows.len();
                on_progress(refreshed_sessions);
            }
        }

        loop {
            let query = format!(
                "SELECT d.session_id AS session_id\n\
                 FROM (\n\
                   SELECT session_id, dirty_revision\n\
                   FROM {}.mcp_open_dirty_sessions FINAL\n\
                 ) AS d\n\
                 LEFT JOIN (\n\
                   SELECT session_id, dirty_revision\n\
                   FROM {}.mcp_open_sessions FINAL\n\
                 ) AS s ON s.session_id = d.session_id\n\
                 WHERE d.dirty_revision > ifNull(s.dirty_revision, 0)\n\
                 ORDER BY d.session_id ASC\n\
                 LIMIT {}\n\
                 FORMAT JSONEachRow",
                escape_identifier(&self.cfg.database),
                escape_identifier(&self.cfg.database),
                BACKFILL_PAGE_SIZE,
            );
            let rows: Vec<SessionIdRow> = self
                .query_json_each_row(&query, Some(&self.cfg.database))
                .await
                .context("failed to read dirty MCP open sessions")?;
            if rows.is_empty() {
                break;
            }
            self.refresh_mcp_open_read_model(rows.iter().map(|row| row.session_id.as_str()))
                .await?;
            refreshed_sessions += rows.len();
            on_progress(refreshed_sessions);
        }

        self.set_mcp_open_projection_state(true, "").await?;
        Ok(())
    }

    pub async fn mcp_open_read_model_ready(&self) -> Result<bool> {
        Ok(self
            .mcp_open_projection_state()
            .await?
            .is_some_and(|state| state.ready == 1))
    }

    async fn mcp_open_projection_state(&self) -> Result<Option<ProjectionStateRow>> {
        let query = format!(
            "SELECT ready, backfill_cursor\n\
             FROM {}.mcp_open_projection_state FINAL\n\
             WHERE state_key = 'global'\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
        );
        let rows: Vec<ProjectionStateRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn set_mcp_open_projection_state(&self, ready: bool, cursor: &str) -> Result<()> {
        let statement = format!(
            "INSERT INTO {}.mcp_open_projection_state\n\
             (state_key, ready, generation, backfill_cursor)\n\
             VALUES ('global', {}, generateSnowflakeID(), {})",
            escape_identifier(&self.cfg.database),
            u8::from(ready),
            escape_literal(cursor),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn refresh_mcp_open_session(&self, session_id: &str) -> Result<()> {
        for _ in 0..MAX_UNSTABLE_REFRESH_ATTEMPTS {
            let source_revision = self.session_source_revision(session_id).await?;
            if source_revision == 0 {
                return Ok(());
            }
            let dirty_revision = self.session_dirty_revision(session_id).await?;
            let head = self.mcp_open_session_head(session_id).await?;
            if head.as_ref().is_some_and(|head| {
                head.source_revision == source_revision && head.dirty_revision == dirty_revision
            }) {
                return Ok(());
            }

            let slot = head
                .as_ref()
                .map_or(0, |head| 1_u8.saturating_sub(head.slot));
            let generation = self.next_projection_generation().await?;

            self.insert_projected_events(session_id, slot, generation, source_revision)
                .await?;
            self.insert_projected_turns(session_id, slot, generation, source_revision)
                .await?;

            if self.session_source_revision(session_id).await? != source_revision
                || self.session_dirty_revision(session_id).await? != dirty_revision
            {
                continue;
            }
            if self
                .insert_projected_session_head(
                    session_id,
                    slot,
                    generation,
                    source_revision,
                    dirty_revision,
                )
                .await?
            {
                return Ok(());
            }
        }

        bail!(
            "MCP open projection source kept changing for session after {} attempts",
            MAX_UNSTABLE_REFRESH_ATTEMPTS
        )
    }

    async fn mcp_open_session_head(&self, session_id: &str) -> Result<Option<SessionHeadRow>> {
        let query = format!(
            "SELECT slot, generation, source_revision, dirty_revision\n\
             FROM {}.mcp_open_sessions FINAL\n\
             WHERE session_id = {}\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
        );
        let rows: Vec<SessionHeadRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn next_projection_generation(&self) -> Result<u64> {
        let rows: Vec<SourceRevisionRow> = self
            .query_json_each_row(
                "SELECT toUInt64(generateSnowflakeID()) AS source_revision FORMAT JSONEachRow",
                Some(&self.cfg.database),
            )
            .await?;
        rows.first()
            .map(|row| row.source_revision)
            .context("ClickHouse did not allocate an MCP open projection generation")
    }

    async fn session_source_revision(&self, session_id: &str) -> Result<u64> {
        let query = format!(
            "SELECT\n\
               if(count() = 0, toUInt64(0), toUInt64(cityHash64(arraySort(groupArray(tuple(event_uid, event_version)))))) AS source_revision\n\
             FROM (\n\
               SELECT event_uid, event_version\n\
               FROM {}.events FINAL\n\
               WHERE session_id = {}\n\
               ORDER BY event_uid ASC\n\
             )\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
        );
        let rows: Vec<SourceRevisionRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.first().map_or(0, |row| row.source_revision))
    }

    async fn session_dirty_revision(&self, session_id: &str) -> Result<u64> {
        let query = format!(
            "SELECT dirty_revision\n\
             FROM {}.mcp_open_dirty_sessions FINAL\n\
             WHERE session_id = {}\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
        );
        let rows: Vec<DirtyRevisionRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.first().map_or(0, |row| row.dirty_revision))
    }

    async fn insert_projected_events(
        &self,
        session_id: &str,
        slot: u8,
        generation: u64,
        source_revision: u64,
    ) -> Result<()> {
        let database = escape_identifier(&self.cfg.database);
        let ctes = projection_ctes(&database, session_id, source_revision);
        let statement = format!(
            "INSERT INTO {database}.mcp_open_events\n\
             (event_uid, slot, generation, session_id, event_order, turn_seq,\n\
              event_time, actor_role, event_class, payload_type, event_type, event_ordinal,\n\
              call_id, name, phase, item_id, source_ref, text_content, payload_json,\n\
              token_usage_json, endpoint_kind, token_usage_buckets, token_usage_native_units,\n\
              previous_event_uid, next_event_uid)\n\
             {ctes}\n\
             SELECT\n\
               event_uid, {slot}, {generation}, session_id, event_order, turn_seq,\n\
               event_time, actor_role, event_class, payload_type, event_type, event_ordinal,\n\
               call_id, name, phase, item_id, source_ref, text_content, payload_json,\n\
               token_usage_json, endpoint_kind, token_usage_buckets, token_usage_native_units,\n\
               previous_event_uid, next_event_uid\n\
             FROM enriched",
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn insert_projected_turns(
        &self,
        session_id: &str,
        slot: u8,
        generation: u64,
        source_revision: u64,
    ) -> Result<()> {
        let database = escape_identifier(&self.cfg.database);
        let ctes = projection_ctes(&database, session_id, source_revision);
        let message = "(event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))";
        let user_message = format!("(lowerUTF8(actor_role) = 'user' AND {message})");
        let assistant_message = format!("(lowerUTF8(actor_role) = 'assistant' AND {message})");
        let statement = format!(
            "INSERT INTO {database}.mcp_open_turns\n\
             (session_id, slot, generation, turn_seq, turn_id, started_at, ended_at,\n\
              total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,\n\
              user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,\n\
              user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,\n\
              final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,\n\
              tools_called, normalized_event_types, completed, terminal_event_uid,\n\
              first_event_uid, first_event_order, first_event_time, first_event_type,\n\
              last_event_uid, last_event_order, last_event_time, last_event_type,\n\
              previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,\n\
              next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json)\n\
             {ctes},\n\
             turn_rows AS (\n\
               SELECT\n\
                 session_id, turn_seq, anyIf(turn_id, turn_id != '') AS turn_id,\n\
                 min(event_time) AS started_at, max(event_time) AS ended_at, count() AS total_events,\n\
                 countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,\n\
                 countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,\n\
                 countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,\n\
                 countIf(event_class = 'reasoning') AS reasoning_items,\n\
                 argMinIf(summary_source, tuple(event_order, event_uid), {user_message}) AS user_input_summary_source,\n\
                 argMaxIf(summary_source, tuple(event_order, event_uid), {assistant_message}) AS final_response_summary_source,\n\
                 argMinIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {user_message}) AS user_input_summary_is_payload,\n\
                 argMaxIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {assistant_message}) AS final_response_summary_is_payload,\n\
                 argMinIf(event_uid, tuple(event_order, event_uid), {user_message}) AS user_input_event_uid,\n\
                 argMinIf(event_order, tuple(event_order, event_uid), {user_message}) AS user_input_event_order,\n\
                 argMinIf(event_time, tuple(event_order, event_uid), {user_message}) AS user_input_event_time,\n\
                 argMinIf(event_type, tuple(event_order, event_uid), {user_message}) AS user_input_event_type,\n\
                 argMaxIf(event_uid, tuple(event_order, event_uid), {assistant_message}) AS final_response_event_uid,\n\
                 argMaxIf(event_order, tuple(event_order, event_uid), {assistant_message}) AS final_response_event_order,\n\
                 argMaxIf(event_time, tuple(event_order, event_uid), {assistant_message}) AS final_response_event_time,\n\
                 argMaxIf(event_type, tuple(event_order, event_uid), {assistant_message}) AS final_response_event_type,\n\
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
                 toJSONString(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_summary))))) AS event_summaries_json\n\
               FROM summarized\n\
               GROUP BY session_id, turn_seq\n\
             ),\n\
             turn_neighbors AS (\n\
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
             )\n\
             SELECT\n\
               session_id, {slot}, {generation}, turn_seq, turn_id, started_at, ended_at,\n\
               total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,\n\
               user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,\n\
               user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,\n\
               final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,\n\
               tools_called, normalized_event_types, completed, terminal_event_uid,\n\
               first_event_uid, first_event_order, first_event_time, first_event_type,\n\
               last_event_uid, last_event_order, last_event_time, last_event_type,\n\
               previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,\n\
               next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json\n\
             FROM turn_neighbors",
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn insert_projected_session_head(
        &self,
        session_id: &str,
        slot: u8,
        generation: u64,
        expected_source_revision: u64,
        dirty_revision: u64,
    ) -> Result<bool> {
        let database = escape_identifier(&self.cfg.database);
        let ctes = projection_ctes(&database, session_id, expected_source_revision);
        let statement = format!(
            "INSERT INTO {database}.mcp_open_sessions\n\
             (session_id, slot, generation, source_revision, dirty_revision, first_event_time,\n\
              last_event_time, total_turns, total_events, user_messages, assistant_messages,\n\
              tool_calls, tool_results, mode, first_event_uid, last_event_uid, last_actor_role,\n\
              title, source, harness, inference_provider, session_slug, session_summary,\n\
              completed, terminal_event_uid, origin_cwd)\n\
             {ctes},\n\
             header AS (\n\
               SELECT\n\
                 session_id, max(source_revision) AS source_revision, min(event_time) AS first_event_time,\n\
                 max(event_time) AS last_event_time, toUInt32(max(turn_seq)) AS total_turns, count() AS total_events,\n\
                 countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,\n\
                 countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,\n\
                 countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,\n\
                 multiIf(\n\
                   countIf(payload_type = 'web_search_call' OR payload_type = 'search_results_received' OR (payload_type = 'tool_use' AND name IN ('WebSearch', 'WebFetch'))) > 0, 'web_search',\n\
                   countIf(source_name = 'codex-mcp' OR lowerUTF8(name) IN ('search', 'open', 'list_sessions', 'file_attention')) > 0, 'mcp_internal',\n\
                   countIf(event_class IN ('tool_call', 'tool_result') OR payload_type = 'tool_use') > 0, 'tool_calling', 'chat') AS mode,\n\
                 argMin(event_uid, tuple(event_time, event_order, event_uid)) AS first_event_uid,\n\
                 argMax(event_uid, tuple(event_time, event_order, event_uid)) AS last_event_uid,\n\
                 argMax(actor_role, tuple(event_time, event_order, event_uid)) AS last_actor_role,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'),\n\
                   ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'), '')) AS title,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'source'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'),\n\
                   ifNull(argMax(nullIf(source_name, ''), tuple(event_ts, event_uid)), '')) AS source,\n\
                 ifNull(argMax(nullIf(harness, ''), tuple(event_ts, event_uid)), '') AS harness,\n\
                 ifNull(argMax(nullIf(inference_provider, ''), tuple(event_ts, event_uid)), '') AS inference_provider,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS session_slug,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'summary'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'),\n\
                   ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'),\n\
                     ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'), ''))) AS session_summary,\n\
                 ifNull(argMinIf(cwd, tuple(event_ts, event_uid), cwd != ''), '') AS origin_cwd\n\
               FROM enriched\n\
               GROUP BY session_id\n\
               HAVING source_revision = {expected_source_revision}\n\
             ),\n\
             current_dirty AS (\n\
               SELECT if(count() = 0, toUInt64(0), toUInt64(max(dirty_revision))) AS dirty_revision\n\
               FROM {database}.mcp_open_dirty_sessions FINAL\n\
               WHERE session_id = {session_sql}\n\
             ),\n\
             terminal AS (\n\
               SELECT\n\
                 session_id, argMax(completed, turn_seq) AS completed, argMax(terminal_event_uid, turn_seq) AS terminal_event_uid\n\
               FROM {database}.mcp_open_turns FINAL\n\
               WHERE session_id = {session_sql} AND slot = {slot} AND generation = {generation} AND turn_seq > 0\n\
               GROUP BY session_id\n\
             )\n\
             SELECT\n\
               h.session_id, {slot}, {generation}, h.source_revision, {dirty_revision},\n\
               h.first_event_time, h.last_event_time, h.total_turns, h.total_events,\n\
               h.user_messages, h.assistant_messages, h.tool_calls, h.tool_results, h.mode,\n\
               h.first_event_uid, h.last_event_uid, h.last_actor_role, h.title, h.source,\n\
               h.harness, h.inference_provider, h.session_slug, h.session_summary,\n\
               ifNull(t.completed, 0), ifNull(t.terminal_event_uid, ''), h.origin_cwd\n\
             FROM header AS h\n\
             CROSS JOIN current_dirty AS d\n\
             LEFT JOIN terminal AS t ON t.session_id = h.session_id\n\
             WHERE d.dirty_revision = {dirty_revision}",
            session_sql = escape_literal(session_id),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        let head = self.mcp_open_session_head(session_id).await?;
        Ok(head.is_some_and(|head| {
            head.generation == generation
                && head.slot == slot
                && head.source_revision == expected_source_revision
                && head.dirty_revision == dirty_revision
        }))
    }
}

fn projection_ctes(database: &str, session_id: &str, source_revision: u64) -> String {
    let session_id = escape_literal(session_id);
    let event_type = event_type_sql();
    format!(
        "WITH\n\
         canonical AS (\n\
           SELECT\n\
             ingested_at, event_uid, session_id, source_name, harness, inference_provider, source_file,\n\
             source_generation, source_line_no, source_offset, source_ref, record_ts, event_ts,\n\
             event_kind AS event_class, actor_kind AS actor_role, payload_type, turn_index, toString(turn_index) AS turn_id, item_id,\n\
             tool_call_id AS call_id, tool_name AS name, if(tool_phase != '', tool_phase, op_status) AS phase,\n\
             text_content, payload_json, token_usage_json, endpoint_kind, token_usage_buckets,\n\
             token_usage_native_units, cwd, event_version\n\
           FROM {database}.events FINAL\n\
           WHERE session_id = {session_id}\n\
         ),\n\
         canonical_revision AS (\n\
           SELECT if(count() = 0, toUInt64(0), toUInt64(cityHash64(arraySort(groupArray(tuple(event_uid, event_version)))))) AS source_revision\n\
           FROM canonical\n\
         ),\n\
         ordered AS (\n\
           SELECT canonical.*,\n\
             ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS event_time,\n\
             row_number() OVER canonical_window AS event_order,\n\
             if(toUInt32(turn_index) > 0, toUInt32(turn_index), greatest(toUInt32(1), toUInt32(sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER canonical_rows))) AS turn_seq,\n\
             revision.source_revision AS source_revision\n\
           FROM canonical\n\
           CROSS JOIN canonical_revision AS revision\n\
           WHERE revision.source_revision = {source_revision}\n\
           WINDOW\n\
             canonical_window AS (PARTITION BY session_id ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_file, source_generation, source_offset, source_line_no, event_uid),\n\
             canonical_rows AS (PARTITION BY session_id ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_file, source_generation, source_offset, source_line_no, event_uid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
         ),\n\
         typed AS (\n\
           SELECT *, {event_type} AS event_type,\n\
             empty(trimBoth(text_content)) AS summary_is_payload,\n\
             if(summary_is_payload,\n\
               leftUTF8(payload_json, {payload_summary_chars}),\n\
               leftUTF8(text_content, {text_summary_chars})) AS summary_source,\n\
             if(notEmpty(trimBoth(name)), trimBoth(name), trimBoth(call_id)) AS tool_label\n\
           FROM ordered\n\
         ),\n\
         enriched AS (\n\
           SELECT *,\n\
             toUInt32(row_number() OVER (PARTITION BY session_id, turn_seq ORDER BY event_order, event_uid)) AS event_ordinal,\n\
             lagInFrame(event_uid, 1, '') OVER event_window AS previous_event_uid,\n\
             leadInFrame(event_uid, 1, '') OVER event_window AS next_event_uid\n\
           FROM typed\n\
           WINDOW event_window AS (PARTITION BY session_id ORDER BY event_order, event_uid ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n\
         ),\n\
         summarized AS (\n\
           SELECT *, CAST(tuple(\n\
             event_uid, event_order, toString(event_time),\n\
             toInt64(toUnixTimestamp64Milli(event_time)), actor_role, event_class, payload_type,\n\
             event_type, call_id, name, phase, summary_source, toUInt8(summary_is_payload)\n\
           ), 'Tuple(event_uid String, event_order UInt64, event_time String, event_unix_ms Int64, actor_role String, event_class String, payload_type String, event_type String, call_id String, name String, phase String, summary_source String, summary_is_payload UInt8)') AS event_summary\n\
           FROM enriched\n\
         )",
        payload_summary_chars = MAX_PROJECTED_PAYLOAD_SUMMARY_CHARS,
        text_summary_chars = MAX_PROJECTED_TEXT_SUMMARY_CHARS,
    )
}

fn event_type_sql() -> &'static str {
    "multiIf(\
      lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg'))), 'user_input',\
      lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg'))), 'assistant_response',\
      lowerUTF8(actor_role) != 'system' AND (event_class = 'reasoning' OR payload_type IN ('agent_reasoning', 'reasoning', 'thinking')), 'reasoning',\
      lowerUTF8(actor_role) != 'system' AND (event_class = 'tool_call' OR payload_type IN ('tool_use', 'function_call', 'custom_tool_call', 'web_search_call')), 'tool_call',\
      lowerUTF8(actor_role) != 'system' AND (event_class = 'tool_result' OR payload_type IN ('tool_result', 'function_call_output', 'custom_tool_call_output', 'search_results_received')), 'tool_response',\
      event_class IN ('compacted_raw', 'summary') OR payload_type IN ('compacted', 'summary'), 'compaction',\
      event_class = 'queue_operation' OR payload_type IN ('task_started', 'task_complete', 'turn_aborted', 'item_completed', 'queue-operation'), 'runtime',\
      lowerUTF8(actor_role) = 'system' OR event_class IN ('system', 'progress', 'file_history_snapshot') OR payload_type IN ('system', 'progress', 'file-history-snapshot', 'file_history_snapshot'), 'system',\
      'unknown')"
}
