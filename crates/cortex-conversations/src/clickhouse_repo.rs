use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::Result as AnyResult;
use async_trait::async_trait;
use cortex_clickhouse::ClickHouseClient;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::cursor::{decode_cursor, encode_cursor, ConversationCursor, TurnCursor};
use crate::domain::{
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationMode,
    ConversationSearchHit, ConversationSearchQuery, ConversationSearchResults,
    ConversationSearchStats, ConversationSummary, OpenContext, OpenEvent, OpenEventRequest, Page,
    PageRequest, RepoConfig, SearchEventHit, SearchEventsQuery, SearchEventsResult,
    SearchEventsStats, TraceEvent, Turn, TurnListFilter, TurnSummary,
};
use crate::error::{RepoError, RepoResult};
use crate::repo::ConversationRepository;

#[derive(Clone)]
pub struct ClickHouseConversationRepository {
    ch: ClickHouseClient,
    cfg: RepoConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct ConversationSummaryRow {
    session_id: String,
    first_event_time: String,
    first_event_unix_ms: i64,
    last_event_time: String,
    last_event_unix_ms: i64,
    total_turns: u32,
    total_events: u64,
    user_messages: u64,
    assistant_messages: u64,
    tool_calls: u64,
    tool_results: u64,
    mode: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TurnSummaryRow {
    session_id: String,
    turn_seq: u32,
    turn_id: String,
    started_at: String,
    started_at_unix_ms: i64,
    ended_at: String,
    ended_at_unix_ms: i64,
    total_events: u64,
    user_messages: u64,
    assistant_messages: u64,
    tool_calls: u64,
    tool_results: u64,
    reasoning_items: u64,
}

#[derive(Debug, Deserialize)]
struct TraceEventRow {
    session_id: String,
    event_uid: String,
    event_order: u64,
    turn_seq: u32,
    event_time: String,
    actor_role: String,
    event_class: String,
    payload_type: String,
    call_id: String,
    name: String,
    phase: String,
    item_id: String,
    source_ref: String,
    text_content: String,
    payload_json: String,
    token_usage_json: String,
}

#[derive(Debug, Deserialize)]
struct OpenTargetRow {
    session_id: String,
    event_order: u64,
    turn_seq: u32,
}

#[derive(Debug, Deserialize)]
struct SearchRow {
    event_uid: String,
    session_id: String,
    source_name: String,
    provider: String,
    event_class: String,
    payload_type: String,
    actor_role: String,
    name: String,
    phase: String,
    source_ref: String,
    doc_len: u32,
    text_preview: String,
    score: f64,
    matched_terms: u64,
}

#[derive(Debug, Deserialize)]
struct CorpusStatsRow {
    docs: u64,
    total_doc_len: u64,
}

#[derive(Debug, Deserialize)]
struct DfRow {
    term: String,
    df: u64,
}

#[derive(Debug, Deserialize)]
struct ConversationSearchRow {
    session_id: String,
    score: f64,
    matched_terms: u16,
    event_count_considered: u32,
    best_event_uid: String,
    snippet: String,
}

impl ClickHouseConversationRepository {
    pub fn new(ch: ClickHouseClient, cfg: RepoConfig) -> Self {
        Self { ch, cfg }
    }

    pub fn config(&self) -> &RepoConfig {
        &self.cfg
    }

    fn table_ref(&self, table: &str) -> String {
        format!(
            "{}.{}",
            sql_identifier(&self.ch.config().database),
            sql_identifier(table)
        )
    }

    fn map_backend<T>(&self, result: AnyResult<T>) -> RepoResult<T> {
        result.map_err(|err| RepoError::backend(err.to_string()))
    }

    fn mode_subquery(&self) -> String {
        let events_table = self.table_ref("events");
        format!(
            "SELECT
  session_id,
  multiIf(
    countIf(
      payload_type = 'web_search_call'
      OR payload_type = 'search_results_received'
      OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch'))
    ) > 0,
    'web_search',
    countIf(source_name = 'codex-mcp' OR lowerUTF8(tool_name) IN ('search', 'open')) > 0,
    'mcp_internal',
    countIf(event_kind IN ('tool_call', 'tool_result') OR payload_type = 'tool_use') > 0,
    'tool_calling',
    'chat'
  ) AS mode
FROM {events_table}
GROUP BY session_id"
        )
    }

    fn parse_mode(raw: &str) -> ConversationMode {
        match raw {
            "web_search" => ConversationMode::WebSearch,
            "mcp_internal" => ConversationMode::McpInternal,
            "tool_calling" => ConversationMode::ToolCalling,
            _ => ConversationMode::Chat,
        }
    }

    fn conversation_filter_sig(filter: &ConversationListFilter) -> String {
        format!(
            "from={:?};to={:?};mode={}",
            filter.from_unix_ms,
            filter.to_unix_ms,
            filter
                .mode
                .map(ConversationMode::as_str)
                .unwrap_or("__none__")
        )
    }

    fn turn_filter_sig(session_id: &str, filter: &TurnListFilter) -> String {
        format!(
            "session={};from={:?};to={:?}",
            session_id, filter.from_turn_seq, filter.to_turn_seq
        )
    }

    fn validate_time_bounds(from_unix_ms: Option<i64>, to_unix_ms: Option<i64>) -> RepoResult<()> {
        if let (Some(from), Some(to)) = (from_unix_ms, to_unix_ms) {
            if from >= to {
                return Err(RepoError::invalid_argument(
                    "from_unix_ms must be strictly less than to_unix_ms",
                ));
            }
        }
        Ok(())
    }

    fn validate_session_id(session_id: &str) -> RepoResult<()> {
        if !is_safe_filter_value(session_id) {
            return Err(RepoError::invalid_argument(
                "session_id contains unsupported characters",
            ));
        }
        Ok(())
    }

    fn validate_event_uid(event_uid: &str) -> RepoResult<()> {
        if !is_safe_filter_value(event_uid) {
            return Err(RepoError::invalid_argument(
                "event_uid contains unsupported characters",
            ));
        }
        Ok(())
    }

    fn map_conversation_row(row: ConversationSummaryRow) -> ConversationSummary {
        ConversationSummary {
            session_id: row.session_id,
            first_event_time: row.first_event_time,
            first_event_unix_ms: row.first_event_unix_ms,
            last_event_time: row.last_event_time,
            last_event_unix_ms: row.last_event_unix_ms,
            total_turns: row.total_turns,
            total_events: row.total_events,
            user_messages: row.user_messages,
            assistant_messages: row.assistant_messages,
            tool_calls: row.tool_calls,
            tool_results: row.tool_results,
            mode: Self::parse_mode(&row.mode),
        }
    }

    fn map_turn_row(row: TurnSummaryRow) -> TurnSummary {
        TurnSummary {
            session_id: row.session_id,
            turn_seq: row.turn_seq,
            turn_id: row.turn_id,
            started_at: row.started_at,
            started_at_unix_ms: row.started_at_unix_ms,
            ended_at: row.ended_at,
            ended_at_unix_ms: row.ended_at_unix_ms,
            total_events: row.total_events,
            user_messages: row.user_messages,
            assistant_messages: row.assistant_messages,
            tool_calls: row.tool_calls,
            tool_results: row.tool_results,
            reasoning_items: row.reasoning_items,
        }
    }

    fn map_trace_event(row: TraceEventRow) -> TraceEvent {
        TraceEvent {
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
        }
    }

    fn mode_filter_clause(mode: Option<ConversationMode>) -> Option<String> {
        mode.map(|m| format!("ifNull(m.mode, 'chat') = {}", sql_quote(m.as_str())))
    }

    async fn load_turns_for_session(&self, session_id: &str) -> RepoResult<Vec<TurnSummary>> {
        let turn_summary = self.table_ref("v_turn_summary");
        let query = format!(
            "SELECT
  session_id,
  toUInt32(turn_seq) AS turn_seq,
  ifNull(turn_id, '') AS turn_id,
  toString(started_at) AS started_at,
  toInt64(toUnixTimestamp64Milli(started_at)) AS started_at_unix_ms,
  toString(ended_at) AS ended_at,
  toInt64(toUnixTimestamp64Milli(ended_at)) AS ended_at_unix_ms,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  toUInt64(reasoning_items) AS reasoning_items
FROM {turn_summary}
WHERE session_id = {}
ORDER BY turn_seq ASC
FORMAT JSONEachRow",
            sql_quote(session_id),
        );

        let rows: Vec<TurnSummaryRow> = self.map_backend(self.ch.query_rows(&query, None).await)?;
        Ok(rows.into_iter().map(Self::map_turn_row).collect())
    }

    async fn load_conversation_summary(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<ConversationSummary>> {
        let session_summary = self.table_ref("v_session_summary");
        let mode_subquery = self.mode_subquery();
        let query = format!(
            "SELECT
  s.session_id,
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
            self.map_backend(self.ch.query_rows(&query, None).await)?;
        Ok(rows.into_iter().next().map(Self::map_conversation_row))
    }

    async fn corpus_stats(&self) -> RepoResult<(u64, u64)> {
        let from_stats_query = format!(
            "SELECT toUInt64(ifNull(sum(docs), 0)) AS docs, toUInt64(ifNull(sum(total_doc_len), 0)) AS total_doc_len FROM {} FORMAT JSONEachRow",
            self.table_ref("search_corpus_stats")
        );

        let from_stats: Vec<CorpusStatsRow> =
            self.map_backend(self.ch.query_rows(&from_stats_query, None).await)?;

        if let Some(row) = from_stats.first() {
            if row.docs > 0 {
                return Ok((row.docs, row.total_doc_len));
            }
        }

        let fallback_query = format!(
            "SELECT toUInt64(count()) AS docs, toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len FROM {} FINAL WHERE doc_len > 0 FORMAT JSONEachRow",
            self.table_ref("search_documents")
        );
        let fallback: Vec<CorpusStatsRow> =
            self.map_backend(self.ch.query_rows(&fallback_query, None).await)?;
        if let Some(row) = fallback.first() {
            Ok((row.docs, row.total_doc_len))
        } else {
            Ok((0, 0))
        }
    }

    async fn df_map(&self, terms: &[String]) -> RepoResult<HashMap<String, u64>> {
        let terms_array = sql_array_strings(terms);
        let term_stats_table = self.table_ref("search_term_stats");
        let postings_table = self.table_ref("search_postings");
        let primary_query = format!(
            "SELECT term, toUInt64(sum(docs)) AS df FROM {term_stats_table} WHERE term IN {terms_array} GROUP BY term FORMAT JSONEachRow",
        );

        let mut map = HashMap::<String, u64>::new();

        let primary_rows: Vec<DfRow> =
            self.map_backend(self.ch.query_rows(&primary_query, None).await)?;
        for row in primary_rows {
            map.insert(row.term, row.df);
        }

        if map.len() == terms.len() {
            return Ok(map);
        }

        let fallback_query = format!(
            "SELECT term, count() AS df FROM {postings_table} FINAL WHERE term IN {terms_array} GROUP BY term FORMAT JSONEachRow",
        );
        let fallback_rows: Vec<DfRow> =
            self.map_backend(self.ch.query_rows(&fallback_query, None).await)?;
        for row in fallback_rows {
            map.insert(row.term, row.df);
        }

        Ok(map)
    }

    fn build_search_events_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }

        let postings_table = self.table_ref("search_postings");
        let documents_table = self.table_ref("search_documents");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);

        let mut where_clauses = vec![format!("p.term IN {}", terms_array_sql)];

        if let Some(sid) = session_id {
            where_clauses.push(format!("p.session_id = {}", sql_quote(sid)));
        }

        if include_tool_events {
            where_clauses.push("p.payload_type != 'token_count'".to_string());
        } else {
            where_clauses
                .push("p.event_class IN ('message', 'reasoning', 'event_msg')".to_string());
            where_clauses.push(
                "p.payload_type NOT IN ('token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed')"
                    .to_string(),
            );
        }

        if exclude_codex_mcp {
            where_clauses
                .push("positionCaseInsensitiveUTF8(d.payload_json, 'codex-mcp') = 0".to_string());
            where_clauses.push("lowerUTF8(d.name) NOT IN ('search', 'open')".to_string());
        }

        let where_sql = where_clauses.join("\n  AND ");
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  greatest({avgdl:.6}, 1.0) AS avgdl,
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  p.doc_id AS event_uid,
  any(p.session_id) AS session_id,
  any(p.source_name) AS source_name,
  any(p.provider) AS provider,
  any(p.event_class) AS event_class,
  any(p.payload_type) AS payload_type,
  any(p.actor_role) AS actor_role,
  any(p.name) AS name,
  any(p.phase) AS phase,
  any(p.source_ref) AS source_ref,
  any(p.doc_len) AS doc_len,
  leftUTF8(any(d.text_content), {preview}) AS text_preview,
  sum(
    transform(p.term, q_terms, q_idf, 0.0)
    *
    (
      (toFloat64(p.tf) * (k1 + 1.0))
      /
      (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl)))
    )
  ) AS score,
  uniqExact(p.term) AS matched_terms
FROM {postings_table} AS p
ANY INNER JOIN {documents_table} AS d ON d.event_uid = p.doc_id
WHERE {where_sql}
GROUP BY p.doc_id
HAVING matched_terms >= {min_should_match} AND score >= {min_score:.6}
ORDER BY score DESC
LIMIT {limit}
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            postings_table = postings_table,
            documents_table = documents_table,
        ))
    }

    fn build_search_conversations_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<ConversationMode>,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }

        let postings_table = self.table_ref("search_postings");
        let documents_table = self.table_ref("search_documents");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);

        let mut event_where = vec![format!("p.term IN {}", terms_array_sql)];
        if let Some(from_unix_ms) = from_unix_ms {
            event_where.push(format!(
                "toUnixTimestamp64Milli(d.ingested_at) >= {from_unix_ms}"
            ));
        }
        if let Some(to_unix_ms) = to_unix_ms {
            event_where.push(format!(
                "toUnixTimestamp64Milli(d.ingested_at) < {to_unix_ms}"
            ));
        }

        if include_tool_events {
            event_where.push("p.payload_type != 'token_count'".to_string());
        } else {
            event_where.push("p.event_class IN ('message', 'reasoning', 'event_msg')".to_string());
            event_where.push(
                "p.payload_type NOT IN ('token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed')"
                    .to_string(),
            );
        }

        if exclude_codex_mcp {
            event_where
                .push("positionCaseInsensitiveUTF8(d.payload_json, 'codex-mcp') = 0".to_string());
            event_where.push("lowerUTF8(d.name) NOT IN ('search', 'open')".to_string());
        }

        let event_where_sql = event_where.join("\n      AND ");
        let mode_subquery = self.mode_subquery();
        let mode_filter_sql = Self::mode_filter_clause(mode)
            .map(|clause| format!("AND {clause}"))
            .unwrap_or_default();

        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  greatest({avgdl:.6}, 1.0) AS avgdl,
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  c.session_id,
  c.score,
  toUInt16(c.matched_terms) AS matched_terms,
  toUInt32(c.event_count_considered) AS event_count_considered,
  c.best_event_uid,
  c.snippet
FROM (
  SELECT
    e.session_id AS session_id,
    sum(e.event_score) AS score,
    length(arrayDistinct(arrayFlatten(groupArray(e.matched_terms_arr)))) AS matched_terms,
    count() AS event_count_considered,
    argMax(e.event_uid, e.event_score) AS best_event_uid,
    argMax(e.text_preview, e.event_score) AS snippet
  FROM (
    SELECT
      p.doc_id AS event_uid,
      any(p.session_id) AS session_id,
      groupUniqArray(p.term) AS matched_terms_arr,
      leftUTF8(any(d.text_content), {preview}) AS text_preview,
      sum(
        transform(p.term, q_terms, q_idf, 0.0)
        *
        (
          (toFloat64(p.tf) * (k1 + 1.0))
          /
          (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl)))
        )
      ) AS event_score
    FROM {postings_table} AS p
    ANY INNER JOIN {documents_table} AS d ON d.event_uid = p.doc_id
    WHERE {event_where_sql}
    GROUP BY p.doc_id
  ) AS e
  GROUP BY e.session_id
) AS c
ANY LEFT JOIN ({mode_subquery}) AS m ON m.session_id = c.session_id
WHERE c.matched_terms >= {min_should_match}
  AND c.score >= {min_score:.6}
  {mode_filter_sql}
ORDER BY c.score DESC, c.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            postings_table = postings_table,
            documents_table = documents_table,
            event_where_sql = event_where_sql,
            mode_subquery = mode_subquery,
            mode_filter_sql = mode_filter_sql,
            min_should_match = min_should_match,
            min_score = min_score,
            limit = limit,
        ))
    }

    async fn log_search_events(
        &self,
        query_id: &str,
        raw_query: &str,
        session_hint: &str,
        terms: &[String],
        limit: u16,
        min_should_match: u16,
        min_score: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        took_ms: u32,
        rows: &[SearchRow],
        docs: u64,
        avgdl: f64,
    ) {
        let metadata_json = match serde_json::to_string(&json!({
            "docs": docs,
            "avgdl": avgdl,
            "k1": self.cfg.bm25_k1,
            "b": self.cfg.bm25_b
        })) {
            Ok(value) => value,
            Err(err) => {
                warn!("failed to encode search metadata: {}", err);
                "{}".to_string()
            }
        };

        let query_row = json!({
            "query_id": query_id,
            "source": "cortex-conversations",
            "session_hint": session_hint,
            "raw_query": raw_query,
            "normalized_terms": terms,
            "term_count": terms.len() as u16,
            "result_limit": limit,
            "min_should_match": min_should_match,
            "min_score": min_score,
            "include_tool_events": if include_tool_events { 1 } else { 0 },
            "exclude_codex_mcp": if exclude_codex_mcp { 1 } else { 0 },
            "response_ms": took_ms,
            "result_count": rows.len() as u16,
            "metadata_json": metadata_json,
        });

        let hit_rows: Vec<Value> = rows
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                json!({
                    "query_id": query_id,
                    "rank": (idx + 1) as u16,
                    "event_uid": row.event_uid,
                    "session_id": row.session_id,
                    "source_name": row.source_name,
                    "provider": row.provider,
                    "score": row.score,
                    "matched_terms": row.matched_terms as u16,
                    "doc_len": row.doc_len,
                    "event_class": row.event_class,
                    "payload_type": row.payload_type,
                    "actor_role": row.actor_role,
                    "name": row.name,
                    "source_ref": row.source_ref,
                })
            })
            .collect();

        let ch = self.ch.clone();
        if self.cfg.async_log_writes {
            tokio::spawn(async move {
                if let Err(err) = ch.insert_json_rows("search_query_log", &[query_row]).await {
                    warn!("failed to write search_query_log: {}", err);
                }
                if !hit_rows.is_empty() {
                    if let Err(err) = ch.insert_json_rows("search_hit_log", &hit_rows).await {
                        warn!("failed to write search_hit_log: {}", err);
                    }
                }
            });
        } else {
            if let Err(err) = self
                .ch
                .insert_json_rows("search_query_log", &[query_row])
                .await
            {
                warn!("failed to write search_query_log: {}", err);
            }
            if !hit_rows.is_empty() {
                if let Err(err) = self.ch.insert_json_rows("search_hit_log", &hit_rows).await {
                    warn!("failed to write search_hit_log: {}", err);
                }
            }
        }
    }
}

#[async_trait]
impl ConversationRepository for ClickHouseConversationRepository {
    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<ConversationSummary>> {
        Self::validate_time_bounds(filter.from_unix_ms, filter.to_unix_ms)?;

        let limit = page.normalized_limit(self.cfg.max_results);
        let filter_sig = Self::conversation_filter_sig(&filter);

        let cursor = if let Some(token) = page.cursor.as_deref() {
            let cursor: ConversationCursor = decode_cursor(token)?;
            if cursor.filter_sig != filter_sig {
                return Err(RepoError::invalid_cursor(
                    "cursor does not match current conversation filter",
                ));
            }
            Some(cursor)
        } else {
            None
        };

        let session_summary = self.table_ref("v_session_summary");
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
            where_clauses.push(format!(
                "(toUnixTimestamp64Milli(s.last_event_time) < {} OR (toUnixTimestamp64Milli(s.last_event_time) = {} AND s.session_id < {}))",
                cursor.last_event_unix_ms,
                cursor.last_event_unix_ms,
                sql_quote(&cursor.session_id)
            ));
        }

        let where_sql = where_clauses.join("\n  AND ");

        let query = format!(
            "SELECT
  s.session_id,
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
WHERE {where_sql}
ORDER BY s.last_event_time DESC, s.session_id DESC
LIMIT {limit_plus}
FORMAT JSONEachRow",
            session_summary = session_summary,
            mode_subquery = mode_subquery,
            where_sql = where_sql,
            limit_plus = (limit as usize) + 1,
        );

        let rows: Vec<ConversationSummaryRow> =
            self.map_backend(self.ch.query_rows(&query, None).await)?;

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

    async fn get_conversation(
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

    async fn list_turns(
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
  toString(started_at) AS started_at,
  toInt64(toUnixTimestamp64Milli(started_at)) AS started_at_unix_ms,
  toString(ended_at) AS ended_at,
  toInt64(toUnixTimestamp64Milli(ended_at)) AS ended_at_unix_ms,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  toUInt64(reasoning_items) AS reasoning_items
FROM {turn_summary}
WHERE {where_sql}
ORDER BY turn_seq ASC
LIMIT {limit_plus}
FORMAT JSONEachRow",
            turn_summary = turn_summary,
            where_sql = where_sql,
            limit_plus = (limit as usize) + 1,
        );

        let rows: Vec<TurnSummaryRow> = self.map_backend(self.ch.query_rows(&query, None).await)?;
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

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        Self::validate_session_id(session_id)?;

        let turn_summary = self.table_ref("v_turn_summary");
        let summary_query = format!(
            "SELECT
  session_id,
  toUInt32(turn_seq) AS turn_seq,
  ifNull(turn_id, '') AS turn_id,
  toString(started_at) AS started_at,
  toInt64(toUnixTimestamp64Milli(started_at)) AS started_at_unix_ms,
  toString(ended_at) AS ended_at,
  toInt64(toUnixTimestamp64Milli(ended_at)) AS ended_at_unix_ms,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  toUInt64(reasoning_items) AS reasoning_items
FROM {turn_summary}
WHERE session_id = {} AND turn_seq = {}
LIMIT 1
FORMAT JSONEachRow",
            sql_quote(session_id),
            turn_seq,
        );

        let rows: Vec<TurnSummaryRow> =
            self.map_backend(self.ch.query_rows(&summary_query, None).await)?;
        let Some(summary_row) = rows.into_iter().next() else {
            return Ok(None);
        };

        let trace_table = self.table_ref("v_conversation_trace");
        let events_query = format!(
            "SELECT
  session_id,
  event_uid,
  toUInt64(event_order) AS event_order,
  toUInt32(turn_seq) AS turn_seq,
  toString(event_time) AS event_time,
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
  token_usage_json
FROM {trace_table}
WHERE session_id = {} AND turn_seq = {}
ORDER BY event_order ASC
FORMAT JSONEachRow",
            sql_quote(session_id),
            turn_seq,
        );

        let event_rows: Vec<TraceEventRow> =
            self.map_backend(self.ch.query_rows(&events_query, None).await)?;
        let events = event_rows.into_iter().map(Self::map_trace_event).collect();

        Ok(Some(Turn {
            summary: Self::map_turn_row(summary_row),
            events,
        }))
    }

    async fn open_event(&self, req: OpenEventRequest) -> RepoResult<OpenContext> {
        let event_uid = req.event_uid.trim();
        if event_uid.is_empty() {
            return Err(RepoError::invalid_argument("event_uid cannot be empty"));
        }
        Self::validate_event_uid(event_uid)?;

        let before = req.before.unwrap_or(self.cfg.default_context_before);
        let after = req.after.unwrap_or(self.cfg.default_context_after);
        let trace_table = self.table_ref("v_conversation_trace");

        let target_query = format!(
            "SELECT session_id, event_order, turn_seq FROM {trace_table} WHERE event_uid = {} ORDER BY event_order DESC LIMIT 1 FORMAT JSONEachRow",
            sql_quote(event_uid)
        );

        let targets: Vec<OpenTargetRow> =
            self.map_backend(self.ch.query_rows(&target_query, None).await)?;
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

        let lower = target.event_order.saturating_sub(before as u64).max(1);
        let upper = target.event_order + after as u64;

        let context_query = format!(
            "SELECT
  session_id,
  event_uid,
  toUInt64(event_order) AS event_order,
  toUInt32(turn_seq) AS turn_seq,
  toString(event_time) AS event_time,
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
  token_usage_json
FROM {trace_table}
WHERE session_id = {} AND event_order BETWEEN {} AND {}
ORDER BY event_order ASC
FORMAT JSONEachRow",
            sql_quote(&target.session_id),
            lower,
            upper
        );

        let mut rows: Vec<TraceEventRow> =
            self.map_backend(self.ch.query_rows(&context_query, None).await)?;
        rows.sort_by_key(|row| row.event_order);

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

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        let query_text = query.query.trim();
        if query_text.is_empty() {
            return Err(RepoError::invalid_argument("query cannot be empty"));
        }

        let query_id = Uuid::new_v4().to_string();
        let started = Instant::now();

        let terms_with_qf = tokenize_query(query_text, self.cfg.bm25_max_query_terms);
        if terms_with_qf.is_empty() {
            return Err(RepoError::invalid_argument("query has no searchable terms"));
        }
        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();

        let limit = query
            .limit
            .unwrap_or(self.cfg.max_results)
            .max(1)
            .min(self.cfg.max_results);

        let min_should_match = query
            .min_should_match
            .unwrap_or(self.cfg.bm25_default_min_should_match)
            .max(1)
            .min(terms.len() as u16);

        let min_score = query.min_score.unwrap_or(self.cfg.bm25_default_min_score);
        let include_tool_events = query
            .include_tool_events
            .unwrap_or(self.cfg.default_include_tool_events);
        let exclude_codex_mcp = query
            .exclude_codex_mcp
            .unwrap_or(self.cfg.default_exclude_codex_mcp);

        if let Some(session_id) = query.session_id.as_deref() {
            Self::validate_session_id(session_id)?;
        }

        let (docs, total_doc_len) = self.corpus_stats().await?;
        if docs == 0 {
            return Ok(SearchEventsResult {
                query_id,
                query: query_text.to_string(),
                terms,
                stats: SearchEventsStats {
                    docs: 0,
                    avgdl: 0.0,
                    took_ms: started.elapsed().as_millis() as u32,
                    result_count: 0,
                },
                hits: Vec::new(),
            });
        }

        let avgdl = (total_doc_len as f64 / docs as f64).max(1.0);
        let df_map = self.df_map(&terms).await?;

        let mut idf_by_term = HashMap::<String, f64>::new();
        for term in &terms {
            let df = *df_map.get(term).unwrap_or(&0);
            let idf = if df == 0 {
                (1.0 + ((docs as f64 + 0.5) / 0.5)).ln()
            } else {
                let n = docs.max(df) as f64;
                (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln()
            };
            idf_by_term.insert(term.clone(), idf.max(0.0));
        }

        let sql = self.build_search_events_sql(
            &terms,
            &idf_by_term,
            avgdl,
            include_tool_events,
            exclude_codex_mcp,
            query.session_id.as_deref(),
            min_should_match,
            min_score,
            limit,
        )?;

        let mut rows: Vec<SearchRow> = self.map_backend(self.ch.query_rows(&sql, None).await)?;
        rows.sort_by(|a, b| b.score.total_cmp(&a.score));

        let took_ms = started.elapsed().as_millis() as u32;

        let hits: Vec<SearchEventHit> = rows
            .iter()
            .enumerate()
            .map(|(idx, row)| SearchEventHit {
                rank: idx + 1,
                event_uid: row.event_uid.clone(),
                session_id: row.session_id.clone(),
                source_name: row.source_name.clone(),
                provider: row.provider.clone(),
                score: row.score,
                matched_terms: row.matched_terms,
                doc_len: row.doc_len,
                event_class: row.event_class.clone(),
                payload_type: row.payload_type.clone(),
                actor_role: row.actor_role.clone(),
                name: row.name.clone(),
                phase: row.phase.clone(),
                source_ref: row.source_ref.clone(),
                text_preview: row.text_preview.clone(),
            })
            .collect();

        self.log_search_events(
            &query_id,
            query_text,
            query.session_id.as_deref().unwrap_or(""),
            &terms,
            limit,
            min_should_match,
            min_score,
            include_tool_events,
            exclude_codex_mcp,
            took_ms,
            &rows,
            docs,
            avgdl,
        )
        .await;

        Ok(SearchEventsResult {
            query_id,
            query: query_text.to_string(),
            terms,
            stats: SearchEventsStats {
                docs,
                avgdl,
                took_ms,
                result_count: hits.len(),
            },
            hits,
        })
    }

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        let query_text = query.query.trim();
        if query_text.is_empty() {
            return Err(RepoError::invalid_argument("query cannot be empty"));
        }

        Self::validate_time_bounds(query.from_unix_ms, query.to_unix_ms)?;

        let query_id = Uuid::new_v4().to_string();
        let started = Instant::now();

        let terms_with_qf = tokenize_query(query_text, self.cfg.bm25_max_query_terms);
        if terms_with_qf.is_empty() {
            return Err(RepoError::invalid_argument("query has no searchable terms"));
        }
        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();

        let limit = query
            .limit
            .unwrap_or(self.cfg.max_results)
            .max(1)
            .min(self.cfg.max_results);

        let min_should_match = query
            .min_should_match
            .unwrap_or(self.cfg.bm25_default_min_should_match)
            .max(1)
            .min(terms.len() as u16);

        let min_score = query.min_score.unwrap_or(self.cfg.bm25_default_min_score);
        let include_tool_events = query
            .include_tool_events
            .unwrap_or(self.cfg.default_include_tool_events);
        let exclude_codex_mcp = query
            .exclude_codex_mcp
            .unwrap_or(self.cfg.default_exclude_codex_mcp);

        let (docs, total_doc_len) = self.corpus_stats().await?;
        if docs == 0 {
            return Ok(ConversationSearchResults {
                query_id,
                query: query_text.to_string(),
                terms,
                stats: ConversationSearchStats {
                    docs: 0,
                    avgdl: 0.0,
                    took_ms: started.elapsed().as_millis() as u32,
                    result_count: 0,
                },
                hits: Vec::new(),
            });
        }

        let avgdl = (total_doc_len as f64 / docs as f64).max(1.0);
        let df_map = self.df_map(&terms).await?;

        let mut idf_by_term = HashMap::<String, f64>::new();
        for term in &terms {
            let df = *df_map.get(term).unwrap_or(&0);
            let idf = if df == 0 {
                (1.0 + ((docs as f64 + 0.5) / 0.5)).ln()
            } else {
                let n = docs.max(df) as f64;
                (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln()
            };
            idf_by_term.insert(term.clone(), idf.max(0.0));
        }

        let sql = self.build_search_conversations_sql(
            &terms,
            &idf_by_term,
            avgdl,
            include_tool_events,
            exclude_codex_mcp,
            min_should_match,
            min_score,
            limit,
            query.from_unix_ms,
            query.to_unix_ms,
            query.mode,
        )?;

        let rows: Vec<ConversationSearchRow> =
            self.map_backend(self.ch.query_rows(&sql, None).await)?;

        let hits = rows
            .into_iter()
            .enumerate()
            .map(|(idx, row)| ConversationSearchHit {
                rank: idx + 1,
                session_id: row.session_id,
                score: row.score,
                matched_terms: row.matched_terms,
                event_count_considered: row.event_count_considered,
                best_event_uid: if row.best_event_uid.is_empty() {
                    None
                } else {
                    Some(row.best_event_uid)
                },
                snippet: if row.snippet.is_empty() {
                    None
                } else {
                    Some(row.snippet)
                },
            })
            .collect::<Vec<_>>();

        Ok(ConversationSearchResults {
            query_id,
            query: query_text.to_string(),
            terms,
            stats: ConversationSearchStats {
                docs,
                avgdl,
                took_ms: started.elapsed().as_millis() as u32,
                result_count: hits.len(),
            },
            hits,
        })
    }
}

fn token_re() -> &'static Regex {
    static TOKEN_RE: OnceLock<Regex> = OnceLock::new();
    TOKEN_RE.get_or_init(|| Regex::new(r"[A-Za-z0-9_]+").expect("valid token regex"))
}

fn safe_value_re() -> &'static Regex {
    static SAFE_RE: OnceLock<Regex> = OnceLock::new();
    SAFE_RE
        .get_or_init(|| Regex::new(r"^[A-Za-z0-9._:@/-]{1,256}$").expect("valid safe-value regex"))
}

fn tokenize_query(text: &str, max_terms: usize) -> Vec<(String, u32)> {
    let mut order = Vec::<String>::new();
    let mut tf = HashMap::<String, u32>::new();

    for mat in token_re().find_iter(text) {
        let token = mat.as_str().to_ascii_lowercase();
        if token.len() < 2 || token.len() > 64 {
            continue;
        }

        if !tf.contains_key(&token) {
            order.push(token.clone());
        }
        let entry = tf.entry(token).or_insert(0);
        *entry += 1;

        if order.len() >= max_terms {
            break;
        }
    }

    order
        .into_iter()
        .map(|token| {
            let count = *tf.get(&token).unwrap_or(&1);
            (token, count)
        })
        .collect()
}

fn is_safe_filter_value(value: &str) -> bool {
    safe_value_re().is_match(value)
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn sql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn sql_array_strings(items: &[String]) -> String {
    let parts = items.iter().map(|item| sql_quote(item)).collect::<Vec<_>>();
    format!("[{}]", parts.join(","))
}

fn sql_array_f64(items: &[f64]) -> String {
    let parts = items
        .iter()
        .map(|v| format!("{:.12}", v))
        .collect::<Vec<_>>();
    format!("[{}]", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_query_enforces_limits_and_counts() {
        let terms = tokenize_query("Hello hello world tool_use", 3);
        assert_eq!(terms.len(), 3);
        assert_eq!(terms[0], ("hello".to_string(), 2));
        assert_eq!(terms[1].0, "world");
    }

    #[test]
    fn safe_filter_value_validation() {
        assert!(is_safe_filter_value("session_123"));
        assert!(is_safe_filter_value("a/b.c:d@e-1"));
        assert!(!is_safe_filter_value("drop table;"));
    }

    #[test]
    fn sql_array_builders_escape_values() {
        let values = vec!["a".to_string(), "b'c".to_string()];
        let out = sql_array_strings(&values);
        assert!(out.contains("'a'"));
        assert!(out.contains("'b''c'"));
    }
}
