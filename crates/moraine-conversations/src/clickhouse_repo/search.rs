use super::*;

pub(super) const CONVERSATION_CANDIDATE_MIN: usize = 512;
pub(super) const CONVERSATION_CANDIDATE_MULTIPLIER: usize = 80;
pub(super) const CONVERSATION_CANDIDATE_MAX: usize = 20_000;
pub(super) const CONVERSATION_RECENT_WINDOW_MS: i64 = 45_000;
pub(super) const CONVERSATION_RECENT_CANDIDATE_LIMIT: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub(super) struct SearchScoreAccum<'a> {
    pub(super) row: &'a CachedPostingRow,
    pub(super) score: f64,
    pub(super) matched_mask: u64,
}

impl ClickHouseConversationRepository {
    pub async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        let query_text = query.query.trim();
        if query_text.is_empty() {
            return Err(RepoError::invalid_argument("query cannot be empty"));
        }

        Self::validate_time_bounds(query.from_unix_ms, query.to_unix_ms)?;
        if let Some(session_id) = query.session_id.as_deref() {
            Self::validate_session_id(session_id)?;
        }

        let query_id = Uuid::new_v4().to_string();
        let started = Instant::now();

        let terms_with_qf = tokenize_query(query_text, self.cfg.bm25_max_query_terms);
        if terms_with_qf.is_empty() {
            return Err(RepoError::invalid_argument(
                "query has no searchable terms (tokens shorter than 2 characters are excluded)",
            ));
        }
        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();

        let requested_limit = query.limit.unwrap_or(self.cfg.max_results).max(1);
        let limit = requested_limit.min(self.cfg.max_results);
        let limit_capped = requested_limit > limit;

        let min_should_match = query
            .min_should_match
            .unwrap_or(self.cfg.bm25_default_min_should_match)
            .max(1)
            .min(terms.len() as u16);
        let min_score = query.min_score.unwrap_or(0.0);

        let sql = self.build_search_session_metadata_sql(
            &terms,
            min_should_match,
            min_score,
            limit,
            query.from_unix_ms,
            query.to_unix_ms,
            query.mode,
            query.session_id.as_deref(),
        )?;

        let rows: Vec<SessionMetadataSearchRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let hits = rows
            .into_iter()
            .enumerate()
            .map(|(idx, row)| self.map_session_metadata_search_row(idx + 1, row, &terms))
            .collect::<Vec<_>>();
        let took_ms = started.elapsed().as_millis() as u32;

        Ok(SessionMetadataSearchResults {
            query_id,
            query: query_text.to_string(),
            terms,
            stats: SessionMetadataSearchStats {
                requested_limit,
                effective_limit: limit,
                limit_capped,
                result_count: hits.len(),
                took_ms,
            },
            hits,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_session_metadata_sql(
        &self,
        terms: &[String],
        min_should_match: u16,
        min_score: f64,
        limit: u16,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<ConversationMode>,
        session_id: Option<&str>,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build session metadata search query with empty terms",
            ));
        }

        let events_source = canonical_events_source(&self.table_ref("events"));
        let session_summary_table = self.table_ref("v_session_summary");
        let mode_subquery = self.mode_subquery();
        let terms_array_sql = sql_array_strings(terms);

        let mut where_clauses = vec![
            format!("meta.matched_terms >= {min_should_match}"),
            format!("meta.score >= {min_score:.6}"),
        ];
        if let Some(from_unix_ms) = from_unix_ms {
            where_clauses.push(format!(
                "toUnixTimestamp64Milli(s.last_event_time) >= {from_unix_ms}"
            ));
        }
        if let Some(to_unix_ms) = to_unix_ms {
            where_clauses.push(format!(
                "toUnixTimestamp64Milli(s.last_event_time) < {to_unix_ms}"
            ));
        }
        if let Some(mode_clause) = Self::mode_filter_clause(mode) {
            where_clauses.push(mode_clause);
        }
        if let Some(session_id) = session_id {
            where_clauses.push(format!("meta.session_id = {}", sql_quote(session_id)));
        }
        let where_sql = where_clauses.join("\n  AND ");

        Ok(format!(
            "WITH
  {terms_array_sql} AS q_terms
SELECT
  meta.session_id AS session_id,
  if(s.session_id = '', '', toString(s.first_event_time)) AS first_event_time,
  if(
    s.session_id = '',
    toInt64(0),
    toInt64(toUnixTimestamp64Milli(s.first_event_time))
  ) AS first_event_unix_ms,
  if(s.session_id = '', '', toString(s.last_event_time)) AS last_event_time,
  if(
    s.session_id = '',
    toInt64(0),
    toInt64(toUnixTimestamp64Milli(s.last_event_time))
  ) AS last_event_unix_ms,
  if(s.session_id = '', toUInt32(0), toUInt32(s.total_turns)) AS total_turns,
  if(s.session_id = '', toUInt64(0), toUInt64(s.total_events)) AS total_events,
  if(s.session_id = '', toUInt64(0), toUInt64(s.user_messages)) AS user_messages,
  if(s.session_id = '', toUInt64(0), toUInt64(s.assistant_messages)) AS assistant_messages,
  if(s.session_id = '', toUInt64(0), toUInt64(s.tool_calls)) AS tool_calls,
  if(s.session_id = '', toUInt64(0), toUInt64(s.tool_results)) AS tool_results,
  ifNull(m.mode, 'chat') AS mode,
  meta.harness AS harness,
  meta.inference_provider AS inference_provider,
  meta.session_slug AS session_slug,
  meta.session_summary AS session_summary,
  meta.meta_event_uid AS meta_event_uid,
  meta.score AS score,
  meta.matched_terms AS matched_terms,
  leftUTF8(meta.metadata_text, {metadata_limit}) AS metadata_text
FROM (
  SELECT
    searchable.session_id AS session_id,
    searchable.meta_event_uid AS meta_event_uid,
    searchable.harness AS harness,
    searchable.inference_provider AS inference_provider,
    searchable.session_slug AS session_slug,
    searchable.session_summary AS session_summary,
    searchable.metadata_text AS metadata_text,
    toUInt16(arraySum(arrayMap(
      term -> if(positionCaseInsensitiveUTF8(searchable.search_text, term) > 0, 1, 0),
      q_terms
    ))) AS matched_terms,
    arraySum(arrayMap(
      term ->
        if(positionCaseInsensitiveUTF8(searchable.session_summary, term) > 0, 2.0, 0.0)
        + if(positionCaseInsensitiveUTF8(searchable.session_slug, term) > 0, 1.5, 0.0)
        + if(positionCaseInsensitiveUTF8(searchable.metadata_text, term) > 0, 1.0, 0.0),
      q_terms
    )) AS score
  FROM (
    SELECT
      base.session_id AS session_id,
      base.meta_event_uid AS meta_event_uid,
      base.harness AS harness,
      base.inference_provider AS inference_provider,
      base.session_slug AS session_slug,
      base.session_summary AS session_summary,
      base.metadata_text AS metadata_text,
      concat(base.session_summary, '\n', base.session_slug, '\n', base.metadata_text) AS search_text
    FROM (
      SELECT
        e.session_id AS session_id,
        argMax(e.event_uid, tuple(e.event_ts, e.event_uid)) AS meta_event_uid,
        argMax(e.harness, tuple(e.event_ts, e.event_uid)) AS harness,
        argMax(e.inference_provider, tuple(e.event_ts, e.event_uid)) AS inference_provider,
        ifNull(argMax(nullIf(JSONExtractString(e.payload_json, 'slug'), ''), tuple(e.event_ts, e.event_uid)), '') AS session_slug,
        ifNull(
          argMax(
            coalesce(
              nullIf(JSONExtractString(e.payload_json, 'summary'), ''),
              nullIf(JSONExtractString(e.payload_json, 'title'), ''),
              nullIf(JSONExtractString(e.payload_json, 'name'), '')
            ),
            tuple(e.event_ts, e.event_uid)
          ),
          ''
        ) AS session_summary,
        argMax(e.payload_json, tuple(e.event_ts, e.event_uid)) AS metadata_text
      FROM {events_source} AS e
      WHERE e.event_kind = 'session_meta'
      GROUP BY e.session_id
    ) AS base
  ) AS searchable
) AS meta
LEFT JOIN {session_summary_table} AS s ON s.session_id = meta.session_id
ANY LEFT JOIN ({mode_subquery}) AS m ON m.session_id = meta.session_id
WHERE {where_sql}
ORDER BY meta.score DESC, meta.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            terms_array_sql = terms_array_sql,
            events_source = events_source,
            session_summary_table = session_summary_table,
            mode_subquery = mode_subquery,
            where_sql = where_sql,
            limit = limit,
            metadata_limit = usize::from(self.cfg.preview_chars).saturating_mul(8),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_events_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        avgdl: f64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        use_document_codex_flag: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
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
        let documents_join_sql = if use_document_codex_flag {
            format!(
                "(SELECT
  t.event_uid AS event_uid,
  any(t.session_id) AS session_id,
  any(t.record_ts) AS event_time,
  any(t.source_name) AS source_name,
  any(t.harness) AS harness,
  any(t.inference_provider) AS inference_provider,
  any(t.event_class) AS event_class,
  any(t.payload_type) AS payload_type,
  any(t.actor_role) AS actor_role,
  any(t.name) AS name,
  any(t.phase) AS phase,
  any(t.source_ref) AS source_ref,
  any(t.doc_len) AS doc_len,
  any(t.text_content) AS text_content,
  any(t.payload_json) AS payload_json,
  toUInt8(any(t.has_codex_mcp)) AS has_codex_mcp
FROM {documents_table} AS t
GROUP BY t.event_uid)"
            )
        } else {
            format!(
                "(SELECT
  t.event_uid AS event_uid,
  any(t.session_id) AS session_id,
  any(t.record_ts) AS event_time,
  any(t.source_name) AS source_name,
  any(t.harness) AS harness,
  any(t.inference_provider) AS inference_provider,
  any(t.event_class) AS event_class,
  any(t.payload_type) AS payload_type,
  any(t.actor_role) AS actor_role,
  any(t.name) AS name,
  any(t.phase) AS phase,
  any(t.source_ref) AS source_ref,
  any(t.doc_len) AS doc_len,
  any(t.text_content) AS text_content,
  any(t.payload_json) AS payload_json,
  toUInt8(0) AS has_codex_mcp
FROM {documents_table} AS t
GROUP BY t.event_uid)"
            )
        };

        let mut where_clauses = vec![format!("p.term IN {}", terms_array_sql)];

        if let Some(sid) = session_id {
            where_clauses.push(format!("d.session_id = {}", sql_quote(sid)));
        }
        if let Some(session_ids) = session_ids {
            if !session_ids.is_empty() {
                where_clauses.push(format!(
                    "d.session_id IN {}",
                    sql_array_strings(session_ids)
                ));
            }
        }

        if let Some(event_kinds) = event_kinds {
            where_clauses.push(Self::event_kind_filter_clause(
                "d.event_class",
                "d.payload_type",
                event_kinds,
            ));
        } else if include_tool_events {
            where_clauses.push("d.payload_type != 'token_count'".to_string());
        } else {
            where_clauses
                .push("d.event_class IN ('message', 'reasoning', 'event_msg')".to_string());
            where_clauses.push(
                "d.payload_type NOT IN ('token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed')"
                    .to_string(),
            );
        }

        if exclude_codex_mcp {
            if use_document_codex_flag {
                where_clauses.push("toUInt8(d.has_codex_mcp) = 0".to_string());
            } else {
                where_clauses.push(
                    "positionCaseInsensitiveUTF8(d.payload_json, 'codex-mcp') = 0".to_string(),
                );
            }
            where_clauses.push(format!(
                "lowerUTF8(d.name) NOT IN ({MCP_INTERNAL_TOOL_NAMES_SQL})"
            ));
        }

        let where_sql = where_clauses.join("\n  AND ");
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  greatest({avgdl:.6}, 1.0) AS avgdl,
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  p.doc_id AS event_uid,
  any(d.session_id) AS session_id,
  any(d.event_time) AS event_time,
  any(d.source_name) AS source_name,
  any(d.harness) AS harness,
  any(d.inference_provider) AS inference_provider,
  any(d.event_class) AS event_class,
  any(d.payload_type) AS payload_type,
  any(d.actor_role) AS actor_role,
  any(d.name) AS name,
  any(d.phase) AS phase,
  any(d.source_ref) AS source_ref,
  any(d.doc_len) AS doc_len,
  leftUTF8(any(d.text_content), {preview}) AS text_preview,
  leftUTF8(any(d.text_content), {text_content_limit}) AS text_content,
  leftUTF8(any(d.payload_json), {payload_json_limit}) AS payload_json,
  sum(
    transform(toString(p.term), q_terms, q_idf, 0.0)
    *
    (
      (toFloat64(p.tf) * (k1 + 1.0))
      /
      (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl)))
    )
  ) AS score,
  uniqExact(p.term) AS matched_terms
FROM {postings_table} AS p
INNER JOIN {documents_join_sql} AS d ON d.event_uid = p.doc_id
WHERE {where_sql}
GROUP BY p.doc_id
HAVING matched_terms >= {min_should_match} AND score >= {min_score:.6}
ORDER BY score DESC, event_uid ASC
LIMIT {limit}
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            text_content_limit = text_content_limit,
            payload_json_limit = payload_json_limit,
            postings_table = postings_table,
            documents_join_sql = documents_join_sql,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_mcp_events_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        avgdl: f64,
        event_types: &[McpEventType],
        session_id: Option<&str>,
        turn_seq: Option<u32>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }
        if event_types.is_empty() {
            return Err(RepoError::invalid_argument(
                "event_types filter cannot be an empty list",
            ));
        }

        let postings_table = self.table_ref("search_postings");
        let documents_table = self.table_ref("search_documents");
        let trace_table = self.table_ref("v_conversation_trace");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let documents_join_sql = format!(
            "(SELECT
  t.event_uid AS event_uid,
  any(t.session_id) AS session_id,
  any(t.source_name) AS source_name,
  any(t.harness) AS harness,
  any(t.inference_provider) AS inference_provider,
  any(t.endpoint_kind) AS endpoint_kind,
  any(t.event_class) AS event_class,
  any(t.payload_type) AS payload_type,
  any(t.actor_role) AS actor_role,
  any(t.name) AS name,
  any(t.phase) AS phase,
  any(t.source_ref) AS source_ref,
  any(t.doc_len) AS doc_len,
  any(t.text_content) AS text_content,
  any(t.payload_json) AS payload_json
FROM {documents_table} AS t
GROUP BY t.event_uid)"
        );

        let mut where_clauses = vec![format!("p.term IN {}", terms_array_sql)];
        if let Some(turn_seq) = turn_seq {
            let Some(session_id) = session_id else {
                return Err(RepoError::invalid_argument(
                    "turn-scoped search requires session_id",
                ));
            };
            where_clauses.push(format!(
                "tr.session_id = {} AND tr.turn_seq = {}",
                sql_quote(session_id),
                turn_seq
            ));
        } else if let Some(session_id) = session_id {
            where_clauses.push(format!("d.session_id = {}", sql_quote(session_id)));
        }
        where_clauses.push(Self::mcp_event_type_filter_clause(
            "d.event_class",
            "d.payload_type",
            "d.actor_role",
            event_types,
        ));
        if let Some(scope_clause) = self.session_scope_clause("d.session_id") {
            where_clauses.push(scope_clause);
        }

        let where_sql = where_clauses.join("\n  AND ");
        let mcp_event_type_expr =
            Self::mcp_event_type_sql_expr("d.event_class", "d.payload_type", "d.actor_role");
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  greatest({avgdl:.6}, 1.0) AS avgdl,
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  p.doc_id AS event_uid,
  any(d.session_id) AS session_id,
  any(d.source_name) AS source_name,
  any(d.harness) AS harness,
  any(d.inference_provider) AS inference_provider,
  any(d.endpoint_kind) AS endpoint_kind,
  any(d.event_class) AS event_class,
  any(d.payload_type) AS payload_type,
  any(d.actor_role) AS actor_role,
  any(d.name) AS name,
  any(d.phase) AS phase,
  any(d.source_ref) AS source_ref,
  any(d.doc_len) AS doc_len,
  leftUTF8(any(d.text_content), {preview}) AS text_preview,
  leftUTF8(any(d.text_content), {text_content_limit}) AS text_content,
  leftUTF8(any(d.payload_json), {payload_json_limit}) AS payload_json,
  any({mcp_event_type_expr}) AS mcp_event_type,
  sum(
    transform(toString(p.term), q_terms, q_idf, 0.0)
    *
    (
      (toFloat64(p.tf) * (k1 + 1.0))
      /
      (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl)))
    )
  ) AS raw_score,
  uniqExact(p.term) AS matched_terms,
  toString(any(tr.event_time)) AS event_time,
  toInt64(toUnixTimestamp64Milli(any(tr.event_time))) AS event_unix_ms,
  toUInt64(any(tr.event_order)) AS event_order,
  toUInt32(any(tr.turn_seq)) AS turn_seq
FROM {postings_table} AS p
INNER JOIN {documents_join_sql} AS d ON d.event_uid = p.doc_id
ANY INNER JOIN {trace_table} AS tr ON tr.event_uid = p.doc_id
WHERE {where_sql}
GROUP BY p.doc_id
HAVING matched_terms >= {min_should_match} AND raw_score >= {min_score:.6}
ORDER BY raw_score DESC, event_unix_ms DESC, event_uid ASC
LIMIT {limit}
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            text_content_limit = text_content_limit,
            payload_json_limit = payload_json_limit,
            postings_table = postings_table,
            documents_join_sql = documents_join_sql,
            trace_table = trace_table,
        ))
    }

    pub(super) fn build_search_events_hydrate_sql(
        &self,
        event_uids: &[String],
        use_document_codex_flag: bool,
    ) -> RepoResult<String> {
        if event_uids.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot hydrate search rows for empty event_uids",
            ));
        }
        let documents_table = self.table_ref("search_documents");
        let event_uids_array = sql_array_strings(event_uids);
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);
        // Truncate the fat columns inside the aggregation (issue #443): the
        // GROUP BY state then holds at most `*_limit` characters per uid
        // instead of full multi-MB payload blobs. The codex fallback still
        // scans every full payload value, but as a boolean aggregate rather
        // than a held string.
        let codex_inner_expr = if use_document_codex_flag {
            "toUInt8(any(t.has_codex_mcp))"
        } else {
            "toUInt8(max(toUInt8(positionCaseInsensitiveUTF8(t.payload_json, 'codex-mcp') > 0)))"
        };
        let documents_source_sql = format!(
            "(SELECT
  t.event_uid AS event_uid,
  any(t.session_id) AS session_id,
  any(t.record_ts) AS event_time,
  any(t.source_name) AS source_name,
  any(t.harness) AS harness,
  any(t.inference_provider) AS inference_provider,
  any(t.event_class) AS event_class,
  any(t.payload_type) AS payload_type,
  any(t.actor_role) AS actor_role,
  any(t.name) AS name,
  any(t.phase) AS phase,
  any(t.source_ref) AS source_ref,
  any(t.doc_len) AS doc_len,
  any(leftUTF8(t.text_content, {text_content_limit})) AS text_content,
  any(leftUTF8(t.payload_json, {payload_json_limit})) AS payload_json,
  {codex_inner_expr} AS has_codex_mcp
FROM {documents_table} AS t
WHERE t.event_uid IN {event_uids_array}
GROUP BY t.event_uid)"
        );

        Ok(format!(
            "SELECT
  d.event_uid AS event_uid,
  d.session_id AS session_id,
  d.event_time AS event_time,
  d.source_name AS source_name,
  d.harness AS harness,
  d.inference_provider AS inference_provider,
  d.event_class AS event_class,
  d.payload_type AS payload_type,
  d.actor_role AS actor_role,
  d.name AS name,
  d.phase AS phase,
  d.source_ref AS source_ref,
  d.doc_len AS doc_len,
  leftUTF8(d.text_content, {preview}) AS text_preview,
  d.text_content AS text_content,
  d.payload_json AS payload_json,
  d.has_codex_mcp AS has_codex_mcp
FROM {documents_source_sql} AS d
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            documents_source_sql = documents_source_sql,
        ))
    }

    pub(super) async fn search_documents_has_codex_flag(&self) -> RepoResult<bool> {
        let now = Instant::now();
        {
            let cache = self.stats_cache.read().await;
            if let Some((value, fetched_at)) = cache.has_codex_flag_column {
                if now.duration_since(fetched_at) <= SEARCH_SCHEMA_CACHE_TTL {
                    return Ok(value);
                }
            }
        }

        let query = format!(
            "SELECT
  toUInt8(count() > 0) AS exists
FROM system.columns
WHERE database = {}
  AND table = 'search_documents'
  AND name = 'has_codex_mcp'
FORMAT JSONEachRow",
            sql_quote(&self.ch.config().database)
        );
        let rows: Vec<ColumnExistsRow> = self.map_backend(self.query_rows(&query, None).await)?;
        let exists = rows.first().map(|row| row.exists != 0).unwrap_or(false);

        let mut cache = self.stats_cache.write().await;
        cache.has_codex_flag_column = Some((exists, now));
        Ok(exists)
    }

    pub(super) fn passes_search_doc_filters(
        row: &SearchDocExtraCacheEntry,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
    ) -> bool {
        if let Some(sid) = session_id {
            if row.session_id != sid {
                return false;
            }
        }
        if let Some(session_ids) = session_ids {
            if !session_ids.iter().any(|sid| sid == &row.session_id) {
                return false;
            }
        }

        if let Some(event_kinds) = event_kinds {
            if !Self::matches_requested_event_kinds(
                &row.event_class,
                &row.payload_type,
                event_kinds,
            ) {
                return false;
            }
        } else if include_tool_events {
            if row.payload_type == "token_count" {
                return false;
            }
        } else {
            if row.event_class != "message"
                && row.event_class != "reasoning"
                && row.event_class != "event_msg"
            {
                return false;
            }
            if row.payload_type == "token_count"
                || row.payload_type == "task_started"
                || row.payload_type == "task_complete"
                || row.payload_type == "turn_aborted"
                || row.payload_type == "item_completed"
            {
                return false;
            }
        }

        if exclude_codex_mcp {
            if row.has_codex_mcp != 0 {
                return false;
            }
            if Self::is_mcp_internal_tool_name(&row.name) {
                return false;
            }
        }

        true
    }

    pub(super) fn bm25_term_score(tf: u16, doc_len: u32, avgdl: f64, k1: f64, b: f64) -> f64 {
        let tf = tf as f64;
        let norm = tf + k1 * (1.0 - b + b * (doc_len as f64 / avgdl.max(1.0)));
        if norm <= 0.0 {
            0.0
        } else {
            tf * (k1 + 1.0) / norm
        }
    }

    pub(super) fn bm25_idf(docs: u64, df: u64) -> f64 {
        let idf = if df == 0 {
            (1.0 + ((docs as f64 + 0.5) / 0.5)).ln()
        } else {
            let n = docs.max(df) as f64;
            (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln()
        };
        idf.max(0.0)
    }

    pub(super) fn has_broad_fast_path_term(
        terms: &[String],
        df_by_term: &HashMap<String, u64>,
        docs: u64,
    ) -> bool {
        terms.iter().any(|term| {
            Self::term_df_too_broad_for_fast_path(*df_by_term.get(term).unwrap_or(&0), docs)
        })
    }

    pub(super) fn term_df_too_broad_for_fast_path(df: u64, docs: u64) -> bool {
        if df > TERM_POSTINGS_FAST_PATH_MAX_ROWS_PER_TERM {
            return true;
        }

        docs >= TERM_POSTINGS_FAST_PATH_RATIO_MIN_DOCS
            && df.saturating_mul(TERM_POSTINGS_FAST_PATH_MAX_DOC_RATIO_DENOMINATOR)
                >= docs.saturating_mul(TERM_POSTINGS_FAST_PATH_MAX_DOC_RATIO_NUMERATOR)
    }

    pub(super) async fn load_term_postings_for_terms(
        &self,
        terms: &[String],
    ) -> RepoResult<HashMap<String, Arc<[CachedPostingRow]>>> {
        let now = Instant::now();
        let mut by_term = HashMap::<String, Arc<[CachedPostingRow]>>::new();
        let mut missing_terms = Vec::<String>::new();

        {
            let cache = self.term_postings_cache.read().await;
            for term in terms {
                if let Some(entry) = cache.get(term) {
                    if now.duration_since(entry.fetched_at) <= TERM_POSTINGS_CACHE_TTL {
                        by_term.insert(term.clone(), Arc::clone(&entry.rows));
                        continue;
                    }
                }
                missing_terms.push(term.clone());
            }
        }

        if !missing_terms.is_empty() {
            let postings_table = self.table_ref("search_postings");
            let terms_array = sql_array_strings(&missing_terms);
            let query = format!(
                "SELECT
  term,
  doc_id AS event_uid,
  doc_len,
  tf
FROM {postings_table}
WHERE term IN {terms_array}
FORMAT JSONEachRow",
            );

            let fetched_rows: Vec<FetchedPostingRow> =
                self.map_backend(self.query_rows(&query, None).await)?;
            let mut grouped = HashMap::<String, Vec<CachedPostingRow>>::new();
            for row in fetched_rows {
                grouped.entry(row.term).or_default().push(CachedPostingRow {
                    event_uid: row.event_uid,
                    doc_len: row.doc_len,
                    tf: row.tf,
                });
            }

            let mut cache = self.term_postings_cache.write().await;

            for term in missing_terms {
                let rows_vec = grouped.remove(&term).unwrap_or_default();
                let rows: Arc<[CachedPostingRow]> = Arc::from(rows_vec.into_boxed_slice());
                by_term.insert(term.clone(), Arc::clone(&rows));
                if rows.len() <= TERM_POSTINGS_CACHE_MAX_ROWS_PER_TERM {
                    cache.insert(
                        term,
                        TermPostingsCacheEntry {
                            rows,
                            fetched_at: now,
                        },
                    );
                }
            }

            let mut cached_rows_total = cache.values().map(|entry| entry.rows.len()).sum::<usize>();
            while cache.len() > TERM_POSTINGS_CACHE_MAX_ENTRIES
                || cached_rows_total > TERM_POSTINGS_CACHE_MAX_ROWS_TOTAL
            {
                if let Some(oldest_key) = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.fetched_at)
                    .map(|(k, _)| k.clone())
                {
                    if let Some(evicted) = cache.remove(&oldest_key) {
                        cached_rows_total = cached_rows_total.saturating_sub(evicted.rows.len());
                    }
                } else {
                    break;
                }
            }
        }

        for term in terms {
            by_term
                .entry(term.clone())
                .or_insert_with(|| Arc::from(Vec::<CachedPostingRow>::new().into_boxed_slice()));
        }
        Ok(by_term)
    }

    pub(super) async fn load_search_doc_extras(
        &self,
        event_uids: &[String],
        use_document_codex_flag: bool,
    ) -> RepoResult<HashMap<String, SearchDocExtraCacheEntry>> {
        let now = Instant::now();
        let mut by_uid = HashMap::<String, SearchDocExtraCacheEntry>::new();
        let mut missing_uids = Vec::<String>::new();

        {
            let cache = self.search_doc_extra_cache.read().await;
            for uid in event_uids {
                if let Some(entry) = cache.get(uid) {
                    if now.duration_since(entry.fetched_at) <= SEARCH_DOC_EXTRA_CACHE_TTL {
                        by_uid.insert(uid.clone(), entry.clone());
                        continue;
                    }
                }
                missing_uids.push(uid.clone());
            }
        }

        if !missing_uids.is_empty() {
            let query =
                self.build_search_events_hydrate_sql(&missing_uids, use_document_codex_flag)?;
            let fetched_rows: Vec<SearchDocExtraRow> =
                self.map_backend(self.query_rows(&query, None).await)?;

            let mut cache = self.search_doc_extra_cache.write().await;

            for row in fetched_rows {
                let entry = SearchDocExtraCacheEntry {
                    session_id: row.session_id,
                    event_time: row.event_time,
                    source_name: row.source_name,
                    harness: row.harness,
                    inference_provider: row.inference_provider,
                    event_class: row.event_class,
                    payload_type: row.payload_type,
                    actor_role: row.actor_role,
                    name: row.name,
                    phase: row.phase,
                    source_ref: row.source_ref,
                    doc_len: row.doc_len,
                    text_preview: row.text_preview,
                    text_content: row.text_content,
                    payload_json: row.payload_json,
                    has_codex_mcp: row.has_codex_mcp,
                    fetched_at: now,
                };
                by_uid.insert(row.event_uid.clone(), entry.clone());
                cache.insert(row.event_uid, entry);
            }

            while cache.len() > SEARCH_DOC_EXTRA_CACHE_MAX_ENTRIES {
                if let Some(oldest_key) = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.fetched_at)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&oldest_key);
                } else {
                    break;
                }
            }
        }

        Ok(by_uid)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_events_rows(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchRow>> {
        let (rows, _candidate_count) = self
            .search_events_rows_fast_pass(
                terms,
                docs,
                avgdl,
                include_tool_events,
                event_kinds,
                exclude_codex_mcp,
                session_id,
                session_ids,
                min_should_match,
                min_score,
                limit,
                usize::MAX,
            )
            .await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        self.search_events_rows_exact_sql(
            terms,
            docs,
            avgdl,
            include_tool_events,
            event_kinds,
            exclude_codex_mcp,
            session_id,
            session_ids,
            min_should_match,
            min_score,
            limit,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_events_rows_exact_sql(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchRow>> {
        let df_map = self.df_map(terms).await?;
        let mut idf_by_term = HashMap::<String, f64>::new();
        for term in terms {
            let df = *df_map.get(term).unwrap_or(&0);
            idf_by_term.insert(term.clone(), Self::bm25_idf(docs, df));
        }

        let use_document_codex_flag = self.search_documents_has_codex_flag().await?;
        let fallback_sql = self.build_search_events_sql(
            terms,
            &idf_by_term,
            avgdl,
            include_tool_events,
            event_kinds,
            exclude_codex_mcp,
            use_document_codex_flag,
            session_id,
            session_ids,
            min_should_match,
            min_score,
            limit,
        )?;

        let mut fallback_rows: Vec<SearchRow> =
            self.map_backend(self.query_rows(&fallback_sql, None).await)?;
        fallback_rows.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.event_uid.cmp(&b.event_uid))
        });
        Ok(fallback_rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_events_rows_by_strategy(
        &self,
        strategy_hint: SearchStrategyHint,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchRow>> {
        match strategy_hint {
            SearchStrategyHint::PreferPerformance => {
                self.search_events_rows(
                    terms,
                    docs,
                    avgdl,
                    include_tool_events,
                    event_kinds,
                    exclude_codex_mcp,
                    session_id,
                    session_ids,
                    min_should_match,
                    min_score,
                    limit,
                )
                .await
            }
            SearchStrategyHint::Exact => {
                self.search_events_rows_exact_sql(
                    terms,
                    docs,
                    avgdl,
                    include_tool_events,
                    event_kinds,
                    exclude_codex_mcp,
                    session_id,
                    session_ids,
                    min_should_match,
                    min_score,
                    limit,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_mcp_event_rows(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        event_types: &[McpEventType],
        session_id: Option<&str>,
        turn_seq: Option<u32>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchMcpEventRow>> {
        // The in-memory postings fast pass filters per-session only after
        // hydration and knows nothing about origin scoping, so a scoped
        // repository always takes the SQL path, where the scope is a WHERE
        // clause.
        if turn_seq.is_none() && self.cfg.session_scope.is_none() {
            let fast_rows = self
                .search_mcp_event_rows_fast_pass(
                    terms,
                    docs,
                    avgdl,
                    event_types,
                    session_id,
                    min_should_match,
                    min_score,
                    limit,
                )
                .await?;
            if !fast_rows.is_empty() {
                return Ok(fast_rows);
            }
        }

        let df_map = self.df_map(terms).await?;
        let mut idf_by_term = HashMap::<String, f64>::new();
        for term in terms {
            let df = *df_map.get(term).unwrap_or(&0);
            idf_by_term.insert(term.clone(), Self::bm25_idf(docs, df));
        }

        let sql = self.build_search_mcp_events_sql(
            terms,
            &idf_by_term,
            avgdl,
            event_types,
            session_id,
            turn_seq,
            min_should_match,
            min_score,
            limit,
        )?;
        let mut rows: Vec<SearchMcpEventRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        Self::sort_search_mcp_event_rows(&mut rows);
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_mcp_event_rows_fast_pass(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        event_types: &[McpEventType],
        session_id: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchMcpEventRow>> {
        #[derive(Clone, Copy)]
        struct CandidateRef<'a> {
            row: &'a CachedPostingRow,
            score: f64,
            matched_terms: u64,
        }

        let df_map = self.df_map(terms).await?;
        if Self::has_broad_fast_path_term(terms, &df_map, docs) {
            return Ok(Vec::new());
        }

        let postings_by_term = self.load_term_postings_for_terms(terms).await?;
        let use_document_codex_flag = self.search_documents_has_codex_flag().await?;
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);
        let mut idf_by_term = HashMap::<&str, f64>::new();
        for term in terms {
            let df = *df_map.get(term).unwrap_or(&0);
            idf_by_term.insert(term.as_str(), Self::bm25_idf(docs, df));
        }

        let mut accum_by_uid = HashMap::<&str, SearchScoreAccum<'_>>::new();
        for (idx, term) in terms.iter().enumerate() {
            if idx >= 64 {
                break;
            }
            let idf = *idf_by_term.get(term.as_str()).unwrap_or(&0.0);
            if idf <= 0.0 {
                continue;
            }

            if let Some(rows) = postings_by_term.get(term) {
                for row in rows.iter() {
                    let entry = accum_by_uid
                        .entry(row.event_uid.as_str())
                        .or_insert_with(|| SearchScoreAccum {
                            row,
                            score: 0.0,
                            matched_mask: 0,
                        });

                    entry.score += idf * Self::bm25_term_score(row.tf, row.doc_len, avgdl, k1, b);
                    entry.matched_mask |= 1u64 << idx;
                }
            }
        }

        let mut candidates = Vec::<CandidateRef<'_>>::new();
        for acc in accum_by_uid.values() {
            let matched_terms = acc.matched_mask.count_ones() as u64;
            if matched_terms < min_should_match as u64 || acc.score < min_score {
                continue;
            }
            candidates.push(CandidateRef {
                row: acc.row,
                score: acc.score,
                matched_terms,
            });
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        candidates.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.row.event_uid.cmp(&b.row.event_uid))
        });

        let mut rows = Vec::<SearchMcpEventRow>::new();
        let target_rows = (limit as usize)
            .saturating_mul(8)
            .max((limit as usize).saturating_add(32))
            .min(256);
        let hydrate_chunk_size = target_rows.max(128);
        let mut offset = 0usize;
        while offset < candidates.len() && rows.len() < target_rows {
            let end = (offset + hydrate_chunk_size).min(candidates.len());
            let event_uids: Vec<String> = candidates[offset..end]
                .iter()
                .map(|candidate| candidate.row.event_uid.clone())
                .collect();
            let doc_extras = self
                .load_search_doc_extras(&event_uids, use_document_codex_flag)
                .await?;

            for candidate in &candidates[offset..end] {
                let Some(extra) = doc_extras.get(candidate.row.event_uid.as_str()) else {
                    continue;
                };
                if session_id.is_some_and(|session_id| extra.session_id != session_id) {
                    continue;
                }
                let event_type = Self::mcp_event_type_for(
                    &extra.event_class,
                    &extra.payload_type,
                    &extra.actor_role,
                );
                if !event_types.contains(&event_type) {
                    continue;
                }

                rows.push(SearchMcpEventRow {
                    event_uid: candidate.row.event_uid.clone(),
                    session_id: extra.session_id.clone(),
                    source_name: extra.source_name.clone(),
                    harness: extra.harness.clone(),
                    inference_provider: extra.inference_provider.clone(),
                    endpoint_kind: String::new(),
                    event_class: extra.event_class.clone(),
                    payload_type: extra.payload_type.clone(),
                    actor_role: extra.actor_role.clone(),
                    name: extra.name.clone(),
                    phase: extra.phase.clone(),
                    source_ref: extra.source_ref.clone(),
                    doc_len: extra.doc_len,
                    text_preview: extra.text_preview.clone(),
                    text_content: extra.text_content.clone(),
                    payload_json: extra.payload_json.clone(),
                    mcp_event_type: event_type.as_str().to_string(),
                    raw_score: candidate.score,
                    matched_terms: candidate.matched_terms,
                    event_time: String::new(),
                    event_unix_ms: 0,
                    event_order: 0,
                    turn_seq: 0,
                });

                if rows.len() >= target_rows {
                    break;
                }
            }
            offset = end;
        }

        let event_enrichment_by_uid = self.load_mcp_event_enrichment(&rows).await?;
        for row in rows.iter_mut() {
            if let Some(enrichment) = event_enrichment_by_uid.get(row.event_uid.as_str()) {
                row.event_time = enrichment.event_time.clone();
                row.event_unix_ms = enrichment.event_unix_ms;
                row.event_order = enrichment.event_order;
                row.turn_seq = enrichment.turn_seq;
            }
        }
        Self::sort_search_mcp_event_rows(&mut rows);
        rows.truncate(limit as usize);
        Ok(rows)
    }

    pub(super) fn sort_search_mcp_event_rows(rows: &mut [SearchMcpEventRow]) {
        rows.sort_by(|a, b| {
            b.raw_score
                .total_cmp(&a.raw_score)
                .then_with(|| b.event_unix_ms.cmp(&a.event_unix_ms))
                .then_with(|| a.event_uid.cmp(&b.event_uid))
        });
    }

    pub(super) fn dedupe_fetch_limit(limit: u16) -> u16 {
        limit.saturating_mul(3).max(limit)
    }

    pub(super) fn is_message_search_row(row: &SearchRow) -> bool {
        row.event_class == "message" && row.payload_type == "message"
    }

    pub(super) fn is_event_msg_search_row(row: &SearchRow) -> bool {
        row.event_class == "event_msg"
            && (row.payload_type == "agent_message"
                || row.payload_type == "user_message"
                || row.payload_type == "event_msg")
    }

    pub(super) fn is_reasoning_search_row(row: &SearchRow) -> bool {
        row.event_class == "reasoning"
    }

    pub(super) fn is_event_msg_reasoning_search_row(row: &SearchRow) -> bool {
        row.event_class == "event_msg" && row.payload_type == "agent_reasoning"
    }

    pub(super) fn compact_preview_for_dedup(text: &str) -> String {
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    pub(super) fn search_rows_are_mirrors(a: &SearchRow, b: &SearchRow) -> bool {
        let is_message_pair = (Self::is_message_search_row(a) && Self::is_event_msg_search_row(b))
            || (Self::is_event_msg_search_row(a) && Self::is_message_search_row(b));
        let is_reasoning_pair = (Self::is_reasoning_search_row(a)
            && Self::is_event_msg_reasoning_search_row(b))
            || (Self::is_event_msg_reasoning_search_row(a) && Self::is_reasoning_search_row(b));
        let same_kind_pair = is_message_pair || is_reasoning_pair;
        if !same_kind_pair {
            return false;
        }

        if a.session_id != b.session_id
            || a.actor_role != b.actor_role
            || a.matched_terms != b.matched_terms
        {
            return false;
        }

        if (a.score - b.score).abs() > 1e-9 {
            return false;
        }

        Self::compact_preview_for_dedup(&a.text_preview)
            == Self::compact_preview_for_dedup(&b.text_preview)
    }

    pub(super) fn search_row_kind_priority(row: &SearchRow) -> u8 {
        if Self::is_message_search_row(row) || Self::is_reasoning_search_row(row) {
            0
        } else if Self::is_event_msg_search_row(row) || Self::is_event_msg_reasoning_search_row(row)
        {
            1
        } else {
            2
        }
    }

    pub(super) fn should_replace_mirror(existing: &SearchRow, candidate: &SearchRow) -> bool {
        let existing_priority = Self::search_row_kind_priority(existing);
        let candidate_priority = Self::search_row_kind_priority(candidate);
        candidate_priority < existing_priority
            || (candidate_priority == existing_priority && candidate.event_uid < existing.event_uid)
    }

    pub(super) fn dedupe_search_rows(rows: Vec<SearchRow>, limit: u16) -> Vec<SearchRow> {
        let target = limit as usize;
        let mut deduped = Vec::<SearchRow>::with_capacity(rows.len().min(target));

        for row in rows {
            if let Some(existing_idx) = deduped
                .iter()
                .position(|existing| Self::search_rows_are_mirrors(existing, &row))
            {
                if Self::should_replace_mirror(&deduped[existing_idx], &row) {
                    deduped[existing_idx] = row;
                }
                continue;
            }

            deduped.push(row);
            if deduped.len() >= target {
                break;
            }
        }

        deduped
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_events_rows_fast_pass(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
        candidate_limit: usize,
    ) -> RepoResult<(Vec<SearchRow>, usize)> {
        #[derive(Clone, Copy)]
        struct CandidateRef<'a> {
            row: &'a CachedPostingRow,
            score: f64,
            matched_terms: u64,
        }

        let df_map = self.df_map(terms).await?;
        if Self::has_broad_fast_path_term(terms, &df_map, docs) {
            return Ok((Vec::new(), 0));
        }

        let postings_by_term = self.load_term_postings_for_terms(terms).await?;
        let use_document_codex_flag = self.search_documents_has_codex_flag().await?;
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);
        let mut idf_by_term = HashMap::<&str, f64>::new();
        for term in terms {
            let df = *df_map.get(term).unwrap_or(&0);
            idf_by_term.insert(term.as_str(), Self::bm25_idf(docs, df));
        }

        let mut accum_by_uid = HashMap::<&str, SearchScoreAccum<'_>>::new();
        for (idx, term) in terms.iter().enumerate() {
            if idx >= 64 {
                break;
            }
            let idf = *idf_by_term.get(term.as_str()).unwrap_or(&0.0);
            if idf <= 0.0 {
                continue;
            }

            if let Some(rows) = postings_by_term.get(term) {
                for row in rows.iter() {
                    let entry = accum_by_uid
                        .entry(row.event_uid.as_str())
                        .or_insert_with(|| SearchScoreAccum {
                            row,
                            score: 0.0,
                            matched_mask: 0,
                        });

                    entry.score += idf * Self::bm25_term_score(row.tf, row.doc_len, avgdl, k1, b);
                    entry.matched_mask |= 1u64 << idx;
                }
            }
        }

        let mut fast_candidates = Vec::<CandidateRef<'_>>::new();
        for acc in accum_by_uid.values() {
            let matched_terms = acc.matched_mask.count_ones() as u64;
            if matched_terms < min_should_match as u64 || acc.score < min_score {
                continue;
            }
            fast_candidates.push(CandidateRef {
                row: acc.row,
                score: acc.score,
                matched_terms,
            });
        }

        if fast_candidates.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let candidate_count = fast_candidates.len();
        if candidate_limit < fast_candidates.len() {
            fast_candidates.select_nth_unstable_by(candidate_limit, |a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.row.event_uid.cmp(&b.row.event_uid))
            });
            fast_candidates.truncate(candidate_limit);
        }
        fast_candidates.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.row.event_uid.cmp(&b.row.event_uid))
        });

        let mut fast_rows = Vec::<SearchRow>::new();
        let hydrate_chunk_size = (limit as usize).saturating_mul(8).max(128);
        let mut offset = 0usize;
        while offset < fast_candidates.len() && fast_rows.len() < limit as usize {
            let end = (offset + hydrate_chunk_size).min(fast_candidates.len());
            let event_uids: Vec<String> = fast_candidates[offset..end]
                .iter()
                .map(|row| row.row.event_uid.clone())
                .collect();
            let doc_extras = self
                .load_search_doc_extras(&event_uids, use_document_codex_flag)
                .await?;

            for row in &fast_candidates[offset..end] {
                let Some(extra) = doc_extras.get(row.row.event_uid.as_str()) else {
                    continue;
                };
                if !Self::passes_search_doc_filters(
                    extra,
                    include_tool_events,
                    event_kinds,
                    exclude_codex_mcp,
                    session_id,
                    session_ids,
                ) {
                    continue;
                }

                fast_rows.push(SearchRow {
                    event_uid: row.row.event_uid.clone(),
                    session_id: extra.session_id.clone(),
                    event_time: extra.event_time.clone(),
                    source_name: extra.source_name.clone(),
                    harness: extra.harness.clone(),
                    inference_provider: extra.inference_provider.clone(),
                    event_class: extra.event_class.clone(),
                    payload_type: extra.payload_type.clone(),
                    actor_role: extra.actor_role.clone(),
                    name: extra.name.clone(),
                    phase: extra.phase.clone(),
                    source_ref: extra.source_ref.clone(),
                    doc_len: extra.doc_len,
                    text_preview: extra.text_preview.clone(),
                    text_content: extra.text_content.clone(),
                    payload_json: extra.payload_json.clone(),
                    score: row.score,
                    matched_terms: row.matched_terms,
                });

                if fast_rows.len() >= limit as usize {
                    break;
                }
            }
            offset = end;
        }

        Ok((fast_rows, candidate_count))
    }

    pub(super) fn conversation_candidate_limit(limit: u16) -> usize {
        (limit as usize)
            .saturating_mul(CONVERSATION_CANDIDATE_MULTIPLIER)
            .clamp(CONVERSATION_CANDIDATE_MIN, CONVERSATION_CANDIDATE_MAX)
    }

    pub(super) fn now_unix_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_conversation_postings_filter_sql(
        &self,
        terms: &[String],
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        recent_from_unix_ms: Option<i64>,
        candidate_session_ids: Option<&[String]>,
    ) -> (String, String, String) {
        let terms_array_sql = sql_array_strings(terms);
        let mut postings_filters = vec![format!("p.term IN {}", terms_array_sql)];
        let mut document_filters = Vec::new();

        if let Some(from_unix_ms) = from_unix_ms {
            document_filters.push(format!(
                "toUnixTimestamp64Milli(d.ingested_at) >= {from_unix_ms}"
            ));
        }
        if let Some(to_unix_ms) = to_unix_ms {
            document_filters.push(format!(
                "toUnixTimestamp64Milli(d.ingested_at) < {to_unix_ms}"
            ));
        }
        if let Some(recent_from_unix_ms) = recent_from_unix_ms {
            document_filters.push(format!(
                "toUnixTimestamp64Milli(d.ingested_at) >= {recent_from_unix_ms}"
            ));
        }

        if include_tool_events {
            postings_filters.push("p.payload_type != 'token_count'".to_string());
        } else {
            postings_filters
                .push("p.event_class IN ('message', 'reasoning', 'event_msg')".to_string());
            postings_filters.push(
                "p.payload_type NOT IN ('token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed')"
                    .to_string(),
            );
        }

        if exclude_codex_mcp {
            postings_filters.push("p.source_name != 'codex-mcp'".to_string());
            postings_filters.push(format!(
                "lowerUTF8(p.name) NOT IN ({MCP_INTERNAL_TOOL_NAMES_SQL})"
            ));
        }

        if let Some(candidate_session_ids) = candidate_session_ids {
            if !candidate_session_ids.is_empty() {
                postings_filters.push(format!(
                    "p.session_id IN {}",
                    sql_array_strings(candidate_session_ids)
                ));
            }
        }

        let prewhere_sql = postings_filters.join("\n      AND ");
        let where_sql = if document_filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", document_filters.join("\n      AND "))
        };
        let docs_join_sql = if document_filters.is_empty() {
            String::new()
        } else {
            let documents_table = self.table_ref("search_documents");
            format!("ANY INNER JOIN {documents_table} AS d ON d.event_uid = p.doc_id")
        };
        (docs_join_sql, prewhere_sql, where_sql)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_conversation_candidates_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        min_should_match: u16,
        limit: usize,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<ConversationMode>,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build candidate query with empty terms",
            ));
        }

        let postings_table = self.table_ref("search_postings");
        let conversation_terms_table = self.table_ref("search_conversation_terms");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let (docs_join_sql, prewhere_sql, where_sql) = self.build_conversation_postings_filter_sql(
            terms,
            include_tool_events,
            exclude_codex_mcp,
            from_unix_ms,
            to_unix_ms,
            None,
            None,
        );

        let (mode_join_sql, mode_filter_sql) = if let Some(selected_mode) = mode {
            let mode_subquery = self.mode_subquery();
            let mode_filter_sql = Self::mode_filter_clause(Some(selected_mode))
                .map(|clause| format!("AND {clause}"))
                .unwrap_or_default();
            (
                format!("ANY LEFT JOIN ({mode_subquery}) AS m ON m.session_id = c.session_id"),
                mode_filter_sql,
            )
        } else {
            (String::new(), String::new())
        };

        Ok(format!(
            "WITH
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  c.session_id AS session_id,
  c.score AS score,
  toUInt16(c.matched_terms) AS matched_terms
FROM (
  SELECT
    ct.session_id,
    sum(transform(ct.term, q_terms, q_idf, 0.0) * log1p(toFloat64(ct.tf_sum))) AS score,
    toUInt16(countDistinct(ct.term)) AS matched_terms
  FROM {conversation_terms_table} AS ct
  ANY INNER JOIN (
    SELECT DISTINCT p.session_id
    FROM {postings_table} AS p
    {docs_join_sql}
    PREWHERE {prewhere_sql}
    {where_sql}
  ) AS eligible ON eligible.session_id = ct.session_id
  WHERE ct.term IN {terms_array_sql}
  GROUP BY ct.session_id
) AS c
{mode_join_sql}
WHERE c.matched_terms >= {min_should_match}
  {mode_filter_sql}
ORDER BY c.score DESC, c.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            conversation_terms_table = conversation_terms_table,
            postings_table = postings_table,
            docs_join_sql = docs_join_sql,
            prewhere_sql = prewhere_sql,
            where_sql = where_sql,
            mode_join_sql = mode_join_sql,
            mode_filter_sql = mode_filter_sql,
            min_should_match = min_should_match,
            limit = limit,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_conversation_recent_candidates_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        min_should_match: u16,
        limit: usize,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<ConversationMode>,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build recent candidate query with empty terms",
            ));
        }

        let postings_table = self.table_ref("search_postings");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let now_unix_ms = Self::now_unix_ms();
        let recent_floor = now_unix_ms.saturating_sub(CONVERSATION_RECENT_WINDOW_MS);
        let recent_from_unix_ms = match from_unix_ms {
            Some(from) => from.max(recent_floor),
            None => recent_floor,
        };
        let (docs_join_sql, prewhere_sql, where_sql) = self.build_conversation_postings_filter_sql(
            terms,
            include_tool_events,
            exclude_codex_mcp,
            from_unix_ms,
            to_unix_ms,
            Some(recent_from_unix_ms),
            None,
        );

        let (mode_join_sql, mode_filter_sql) = if let Some(selected_mode) = mode {
            let mode_subquery = self.mode_subquery();
            let mode_filter_sql = Self::mode_filter_clause(Some(selected_mode))
                .map(|clause| format!("AND {clause}"))
                .unwrap_or_default();
            (
                format!("ANY LEFT JOIN ({mode_subquery}) AS m ON m.session_id = c.session_id"),
                mode_filter_sql,
            )
        } else {
            (String::new(), String::new())
        };

        Ok(format!(
            "WITH
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf
SELECT
  c.session_id AS session_id,
  c.score AS score,
  toUInt16(c.matched_terms) AS matched_terms
FROM (
  SELECT
    p.session_id AS session_id,
    sum(transform(toString(p.term), q_terms, q_idf, 0.0) * log1p(toFloat64(p.tf))) AS score,
    toUInt16(countDistinct(p.term)) AS matched_terms
  FROM {postings_table} AS p
  {docs_join_sql}
  PREWHERE {prewhere_sql}
  {where_sql}
  GROUP BY p.session_id
) AS c
{mode_join_sql}
WHERE c.matched_terms >= {min_should_match}
  {mode_filter_sql}
ORDER BY c.score DESC, c.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            postings_table = postings_table,
            docs_join_sql = docs_join_sql,
            prewhere_sql = prewhere_sql,
            where_sql = where_sql,
            mode_join_sql = mode_join_sql,
            mode_filter_sql = mode_filter_sql,
            min_should_match = min_should_match,
            limit = limit,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fetch_conversation_candidates(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        min_should_match: u16,
        limit: u16,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        mode: Option<ConversationMode>,
    ) -> RepoResult<ConversationCandidateSet> {
        let candidate_limit = Self::conversation_candidate_limit(limit);
        let persistent_sql = self.build_search_conversation_candidates_sql(
            terms,
            idf_by_term,
            include_tool_events,
            exclude_codex_mcp,
            min_should_match,
            candidate_limit,
            from_unix_ms,
            to_unix_ms,
            mode,
        )?;
        let mut persistent_rows: Vec<ConversationCandidateRow> =
            self.map_backend(self.query_rows(&persistent_sql, None).await)?;
        let truncated = persistent_rows.len() >= candidate_limit;
        if truncated {
            return Ok(ConversationCandidateSet {
                rows: persistent_rows,
                truncated: true,
            });
        }

        let recent_sql = self.build_search_conversation_recent_candidates_sql(
            terms,
            idf_by_term,
            include_tool_events,
            exclude_codex_mcp,
            min_should_match,
            CONVERSATION_RECENT_CANDIDATE_LIMIT,
            from_unix_ms,
            to_unix_ms,
            mode,
        )?;
        let recent_rows: Vec<ConversationCandidateRow> =
            self.map_backend(self.query_rows(&recent_sql, None).await)?;

        let mut by_session = HashMap::<String, (f64, u16)>::new();
        for row in persistent_rows.drain(..) {
            by_session.insert(row.session_id, (row.score, row.matched_terms));
        }
        for row in recent_rows {
            let entry = by_session
                .entry(row.session_id)
                .or_insert((row.score, row.matched_terms));
            if row.score > entry.0 {
                entry.0 = row.score;
            }
            if row.matched_terms > entry.1 {
                entry.1 = row.matched_terms;
            }
        }

        let mut rows = by_session
            .into_iter()
            .map(
                |(session_id, (score, matched_terms))| ConversationCandidateRow {
                    session_id,
                    score,
                    matched_terms,
                },
            )
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.session_id.cmp(&b.session_id))
        });
        let max_rows = candidate_limit.saturating_add(CONVERSATION_RECENT_CANDIDATE_LIMIT);
        if rows.len() > max_rows {
            rows.truncate(max_rows);
        }

        Ok(ConversationCandidateSet {
            rows,
            truncated: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_conversations_sql(
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
        candidate_session_ids: Option<&[String]>,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }

        let postings_table = self.table_ref("search_postings");
        let session_summary_table = self.table_ref("v_session_summary");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let (docs_join_sql, prewhere_sql, where_sql) = self.build_conversation_postings_filter_sql(
            terms,
            include_tool_events,
            exclude_codex_mcp,
            from_unix_ms,
            to_unix_ms,
            None,
            candidate_session_ids,
        );
        let (mode_join_sql, mode_filter_sql) = if let Some(selected_mode) = mode {
            let mode_subquery = self.mode_subquery();
            let mode_filter_sql = Self::mode_filter_clause(Some(selected_mode))
                .map(|clause| format!("AND {clause}"))
                .unwrap_or_default();
            (
                format!("ANY LEFT JOIN ({mode_subquery}) AS m ON m.session_id = c.session_id"),
                mode_filter_sql,
            )
        } else {
            (String::new(), String::new())
        };

        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);
        let use_term_bitmask = terms.len() <= 63;
        let term_bits_with_sql = if use_term_bitmask {
            ",\n  arrayMap(idx -> toUInt64(bitShiftLeft(toUInt64(1), idx - 1)), arrayEnumerate(q_terms)) AS q_bits"
                .to_string()
        } else {
            String::new()
        };
        let outer_matched_terms_sql = if use_term_bitmask {
            "bitCount(groupBitOr(e.term_mask))".to_string()
        } else {
            "length(arrayDistinct(arrayFlatten(groupArray(e.matched_terms_arr))))".to_string()
        };
        let inner_matched_terms_sql = if use_term_bitmask {
            "groupBitOr(transform(toString(p.term), q_terms, q_bits, toUInt64(0))) AS term_mask,"
                .to_string()
        } else {
            "groupUniqArray(toString(p.term)) AS matched_terms_arr,".to_string()
        };

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  greatest({avgdl:.6}, 1.0) AS avgdl,
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf{term_bits_with_sql}
SELECT
  c.session_id AS session_id,
  if(s.session_id = '', '', toString(s.first_event_time)) AS first_event_time,
  if(
    s.session_id = '',
    toInt64(0),
    toInt64(toUnixTimestamp64Milli(s.first_event_time))
  ) AS first_event_unix_ms,
  if(s.session_id = '', '', toString(s.last_event_time)) AS last_event_time,
  if(
    s.session_id = '',
    toInt64(0),
    toInt64(toUnixTimestamp64Milli(s.last_event_time))
  ) AS last_event_unix_ms,
  c.harness AS harness,
  c.inference_provider AS inference_provider,
  c.score AS score,
  toUInt16(c.matched_terms) AS matched_terms,
  toUInt32(c.event_count_considered) AS event_count_considered,
  c.best_event_uid AS best_event_uid
FROM (
  SELECT
    e.session_id AS session_id,
    sum(e.event_score) AS score,
    {outer_matched_terms_sql} AS matched_terms,
    count() AS event_count_considered,
    argMax(e.harness, e.event_score) AS harness,
    argMax(e.inference_provider, e.event_score) AS inference_provider,
    argMax(e.event_uid, e.event_score) AS best_event_uid
  FROM (
    SELECT
      p.doc_id AS event_uid,
      any(p.session_id) AS session_id,
      any(p.harness) AS harness,
      any(p.inference_provider) AS inference_provider,
      {inner_matched_terms_sql}
      sum(
        transform(toString(p.term), q_terms, q_idf, 0.0)
        *
        (
          (toFloat64(p.tf) * (k1 + 1.0))
          /
          (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl)))
        )
      ) AS event_score
    FROM {postings_table} AS p
    {docs_join_sql}
    PREWHERE {prewhere_sql}
    {where_sql}
    GROUP BY p.doc_id
  ) AS e
  GROUP BY e.session_id
) AS c
ANY LEFT JOIN {session_summary_table} AS s ON s.session_id = c.session_id
{mode_join_sql}
WHERE c.matched_terms >= {min_should_match}
  AND c.score >= {min_score:.6}
  {mode_filter_sql}
ORDER BY c.score DESC, c.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            postings_table = postings_table,
            docs_join_sql = docs_join_sql,
            prewhere_sql = prewhere_sql,
            where_sql = where_sql,
            outer_matched_terms_sql = outer_matched_terms_sql,
            inner_matched_terms_sql = inner_matched_terms_sql,
            term_bits_with_sql = term_bits_with_sql,
            session_summary_table = session_summary_table,
            mode_join_sql = mode_join_sql,
            mode_filter_sql = mode_filter_sql,
            min_should_match = min_should_match,
            min_score = min_score,
            limit = limit,
        ))
    }

    pub(super) async fn fetch_conversation_snippets(
        &self,
        event_uids: &[String],
    ) -> RepoResult<HashMap<String, ConversationSnippetContent>> {
        if event_uids.is_empty() {
            return Ok(HashMap::new());
        }

        let documents_table = self.table_ref("search_documents");
        let event_uids_sql = sql_array_strings(event_uids);
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);
        // Truncate inside the aggregation (issue #443) so the GROUP BY state
        // holds bounded strings, not full payload blobs.
        let sql = format!(
            "SELECT
  event_uid,
  leftUTF8(text_content_raw, {preview}) AS snippet,
  text_content_raw AS text_content,
  payload_json_raw AS payload_json,
  event_class_raw AS event_class,
  actor_role_raw AS actor_role
FROM (
  SELECT
    event_uid,
    any(leftUTF8(text_content, {text_content_limit})) AS text_content_raw,
    any(leftUTF8(payload_json, {payload_json_limit})) AS payload_json_raw,
    any(event_class) AS event_class_raw,
    any(actor_role) AS actor_role_raw
  FROM {documents_table}
  WHERE event_uid IN {event_uids_sql}
  GROUP BY event_uid
)
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            text_content_limit = text_content_limit,
            payload_json_limit = payload_json_limit,
            documents_table = documents_table,
            event_uids_sql = event_uids_sql,
        );
        let rows: Vec<ConversationSnippetRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let mut by_uid = HashMap::new();
        for row in rows {
            let is_user_facing = is_user_facing_content_event(&row.event_class, &row.actor_role);
            by_uid.insert(
                row.event_uid,
                ConversationSnippetContent {
                    snippet: row.snippet,
                    text_content: is_user_facing
                        .then_some(row.text_content)
                        .filter(|value| !value.is_empty()),
                    payload_json: is_user_facing
                        .then_some(row.payload_json)
                        .filter(|value| !value.is_empty()),
                },
            );
        }
        Ok(by_uid)
    }

    pub(super) async fn load_session_time_bounds(
        &self,
        session_ids: &[String],
    ) -> RepoResult<HashMap<String, SessionTimeBounds>> {
        let mut unique_session_ids = session_ids.to_vec();
        unique_session_ids.sort_unstable();
        unique_session_ids.dedup();
        if unique_session_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let session_summary_table = self.table_ref("v_session_summary");
        let session_ids_sql = sql_array_strings(&unique_session_ids);
        let sql = format!(
            "SELECT
  session_id,
  toString(ss.first_event_time) AS first_event_time,
  toString(ss.last_event_time) AS last_event_time,
  toInt64(toUnixTimestamp64Milli(ss.first_event_time)) AS first_event_unix_ms,
  toInt64(toUnixTimestamp64Milli(ss.last_event_time)) AS last_event_unix_ms
FROM {session_summary_table} AS ss
WHERE session_id IN {session_ids_sql}
FORMAT JSONEachRow",
            session_summary_table = session_summary_table,
            session_ids_sql = session_ids_sql,
        );

        let rows: Vec<SessionTimeBoundsRow> =
            match self.map_backend(self.query_rows(&sql, None).await) {
                Ok(rows) => rows,
                Err(err) => {
                    warn!("failed to load session time bounds: {}", err);
                    return Ok(HashMap::new());
                }
            };
        let mut bounds_by_session = HashMap::new();
        for row in rows {
            bounds_by_session.insert(
                row.session_id,
                SessionTimeBounds {
                    first_event_time: row.first_event_time,
                    last_event_time: row.last_event_time,
                    first_event_unix_ms: row.first_event_unix_ms,
                    last_event_unix_ms: row.last_event_unix_ms,
                },
            );
        }
        Ok(bounds_by_session)
    }

    pub(super) async fn map_search_rows_to_hits(
        &self,
        rows: Vec<SearchRow>,
    ) -> RepoResult<Vec<SearchEventHit>> {
        let session_ids = rows
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>();
        let session_time_bounds = self.load_session_time_bounds(&session_ids).await?;

        Ok(rows
            .into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let session_id = row.session_id;
                let (first_event_time, last_event_time) = session_time_bounds
                    .get(session_id.as_str())
                    .map(|bounds| {
                        (
                            bounds.first_event_time.clone(),
                            bounds.last_event_time.clone(),
                        )
                    })
                    .unwrap_or_default();

                SearchEventHit {
                    rank: idx + 1,
                    event_uid: row.event_uid,
                    session_id,
                    event_time: (!row.event_time.is_empty()).then_some(row.event_time),
                    first_event_time,
                    last_event_time,
                    source_name: row.source_name,
                    harness: row.harness,
                    inference_provider: row.inference_provider,
                    score: row.score,
                    matched_terms: row.matched_terms,
                    doc_len: row.doc_len,
                    event_class: row.event_class,
                    payload_type: row.payload_type,
                    actor_role: row.actor_role,
                    name: row.name,
                    phase: row.phase,
                    source_ref: row.source_ref,
                    text_preview: row.text_preview,
                    text_content: (!row.text_content.is_empty()).then_some(row.text_content),
                    payload_json: (!row.payload_json.is_empty()).then_some(row.payload_json),
                }
            })
            .collect())
    }

    pub(super) async fn load_mcp_event_enrichment(
        &self,
        rows: &[SearchMcpEventRow],
    ) -> RepoResult<HashMap<String, SearchMcpEventEnrichmentRow>> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }

        let mut event_uids = rows
            .iter()
            .map(|row| row.event_uid.clone())
            .collect::<Vec<_>>();
        event_uids.sort_unstable();
        event_uids.dedup();

        let mut session_ids = rows
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>();
        session_ids.sort_unstable();
        session_ids.dedup();

        let trace_table = self.table_ref("v_conversation_trace");
        let events_source = canonical_events_source(&self.table_ref("events"));
        let event_uids_sql = sql_array_strings(&event_uids);
        let session_ids_sql = sql_array_strings(&session_ids);
        let sql = format!(
            "SELECT
  tr.event_uid AS event_uid,
  toString(tr.event_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms,
  toUInt64(tr.event_order) AS event_order,
  toUInt32(tr.turn_seq) AS turn_seq,
  toUInt32(tr.event_ordinal) AS event_ordinal,
  toUInt64(tr.turn_event_count) AS turn_event_count,
  tr.call_id AS call_id,
  tr.item_id AS item_id,
  ifNull(e.model, '') AS model
FROM (
  SELECT
    event_uid,
    session_id,
    event_time,
    event_order,
    turn_seq,
    call_id,
    item_id,
    row_number() OVER (
      PARTITION BY session_id, turn_seq
      ORDER BY event_order ASC, event_uid ASC
    ) AS event_ordinal,
    count() OVER (PARTITION BY session_id, turn_seq) AS turn_event_count
  FROM {trace_table}
  WHERE session_id IN {session_ids_sql}
) AS tr
ANY LEFT JOIN (
  SELECT event_uid, model
  FROM {events_source}
  WHERE event_uid IN {event_uids_sql}
) AS e ON e.event_uid = tr.event_uid
WHERE tr.event_uid IN {event_uids_sql}
FORMAT JSONEachRow",
            trace_table = trace_table,
            events_source = events_source,
            session_ids_sql = session_ids_sql,
            event_uids_sql = event_uids_sql,
        );

        let rows: Vec<SearchMcpEventEnrichmentRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let mut by_uid = HashMap::new();
        for row in rows {
            by_uid.insert(row.event_uid.clone(), row);
        }
        Ok(by_uid)
    }

    pub(super) async fn map_search_mcp_rows_to_hits(
        &self,
        rows: Vec<SearchMcpEventRow>,
    ) -> RepoResult<Vec<SearchMcpEventHit>> {
        let session_ids = rows
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>();
        let session_time_bounds = self.load_session_time_bounds(&session_ids).await?;
        let session_metadata_by_session_id = self
            .fetch_conversation_session_metadata(&session_ids)
            .await?;
        let event_enrichment_by_uid = self.load_mcp_event_enrichment(&rows).await?;
        let max_raw_score = rows.iter().map(|row| row.raw_score).fold(0.0_f64, f64::max);

        Ok(rows
            .into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let session_id = row.session_id;
                let enrichment = event_enrichment_by_uid.get(row.event_uid.as_str());
                let event_type = if row.mcp_event_type.is_empty() {
                    Self::mcp_event_type_for(&row.event_class, &row.payload_type, &row.actor_role)
                } else {
                    McpEventType::from_normalized(&row.mcp_event_type)
                };
                let session_metadata = session_metadata_by_session_id.get(&session_id);
                let session_slug = session_metadata.and_then(|meta| {
                    (!meta.session_slug.is_empty()).then(|| meta.session_slug.clone())
                });
                let session_summary = session_metadata.and_then(|meta| {
                    (!meta.session_summary.is_empty()).then(|| meta.session_summary.clone())
                });
                let session_title = session_summary.clone().or_else(|| session_slug.clone());
                let (session_started_at_unix_ms, session_updated_at_unix_ms) = session_time_bounds
                    .get(session_id.as_str())
                    .map(|bounds| {
                        (
                            Some(bounds.first_event_unix_ms),
                            Some(bounds.last_event_unix_ms),
                        )
                    })
                    .unwrap_or_default();
                let text_content_len = row.text_content.chars().count();
                let snippet_len = row.text_preview.chars().count();
                let snippet = if row.text_preview.is_empty() {
                    row.text_content.clone()
                } else {
                    row.text_preview.clone()
                };
                let score = if max_raw_score > 0.0 {
                    (row.raw_score / max_raw_score).clamp(0.0, 1.0)
                } else {
                    0.0
                };

                SearchMcpEventHit {
                    rank: idx + 1,
                    event_uid: row.event_uid,
                    session_id,
                    event_type,
                    event_time: enrichment
                        .map(|value| value.event_time.clone())
                        .unwrap_or(row.event_time),
                    event_unix_ms: enrichment
                        .map(|value| value.event_unix_ms)
                        .unwrap_or(row.event_unix_ms),
                    turn_seq: enrichment
                        .map(|value| value.turn_seq)
                        .unwrap_or(row.turn_seq),
                    turn_ordinal: enrichment
                        .map(|value| value.turn_seq)
                        .unwrap_or(row.turn_seq),
                    event_order: enrichment
                        .map(|value| value.event_order)
                        .unwrap_or(row.event_order),
                    event_ordinal: enrichment
                        .map(|value| value.event_ordinal)
                        .unwrap_or_default(),
                    turn_event_count: enrichment
                        .map(|value| value.turn_event_count)
                        .unwrap_or_default(),
                    session_started_at_unix_ms,
                    session_updated_at_unix_ms,
                    session_title,
                    session_slug,
                    session_summary,
                    source_name: non_empty_string(row.source_name),
                    harness: non_empty_string(row.harness),
                    inference_provider: non_empty_string(row.inference_provider),
                    event_class: row.event_class,
                    payload_type: row.payload_type,
                    actor_role: row.actor_role,
                    tool_name: non_empty_string(row.name),
                    tool_phase: non_empty_string(row.phase),
                    call_id: enrichment.and_then(|value| non_empty_string(value.call_id.clone())),
                    item_id: enrichment.and_then(|value| non_empty_string(value.item_id.clone())),
                    model: enrichment.and_then(|value| non_empty_string(value.model.clone())),
                    endpoint_kind: non_empty_string(row.endpoint_kind),
                    source_ref: non_empty_string(row.source_ref),
                    snippet,
                    snippet_truncated: text_content_len > snippet_len && snippet_len > 0,
                    text_content: non_empty_string(row.text_content),
                    payload_json: non_empty_string(row.payload_json),
                    score,
                    raw_score: row.raw_score,
                    matched_terms: row.matched_terms,
                    doc_len: row.doc_len,
                }
            })
            .collect())
    }

    pub(super) async fn fetch_conversation_session_metadata(
        &self,
        session_ids: &[String],
    ) -> RepoResult<HashMap<String, ConversationSessionMetadataRow>> {
        if session_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let events_source = canonical_events_source(&self.table_ref("events"));
        let session_ids_sql = sql_array_strings(session_ids);
        let sql = format!(
            "SELECT
  session_id,
  argMax(harness, event_ts) AS harness,
  argMax(inference_provider, event_ts) AS inference_provider,
  ifNull(argMax(nullIf(JSONExtractString(payload_json, 'slug'), ''), event_ts), '') AS session_slug,
  ifNull(
    argMax(
      coalesce(
        nullIf(JSONExtractString(payload_json, 'summary'), ''),
        nullIf(JSONExtractString(payload_json, 'title'), ''),
        nullIf(JSONExtractString(payload_json, 'name'), '')
      ),
      event_ts
    ),
    ''
  ) AS session_summary
FROM {events_source}
WHERE event_kind = 'session_meta'
  AND session_id IN {session_ids_sql}
GROUP BY session_id
FORMAT JSONEachRow",
            events_source = events_source,
            session_ids_sql = session_ids_sql,
        );

        let rows: Vec<ConversationSessionMetadataRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let mut by_session = HashMap::new();
        for row in rows {
            by_session.insert(row.session_id.clone(), row);
        }
        Ok(by_session)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn log_search_events(
        &self,
        query_id: &str,
        source: &str,
        raw_query: &str,
        session_hint: &str,
        terms: &[String],
        limit: u16,
        min_should_match: u16,
        min_score: f64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        took_ms: u32,
        hits: &[SearchEventHit],
        docs: u64,
        avgdl: f64,
    ) {
        let event_kinds = event_kinds
            .map(|kinds| kinds.iter().map(|kind| kind.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        let metadata_json = match serde_json::to_string(&json!({
            "docs": docs,
            "avgdl": avgdl,
            "k1": self.cfg.bm25_k1,
            "b": self.cfg.bm25_b,
            "event_kinds": event_kinds
        })) {
            Ok(value) => value,
            Err(err) => {
                warn!("failed to encode search metadata: {}", err);
                "{}".to_string()
            }
        };

        let query_row = json!({
            "query_id": query_id,
            "source": source,
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
            "result_count": hits.len() as u16,
            "metadata_json": metadata_json,
        });

        let hit_rows: Vec<Value> = hits
            .iter()
            .map(|hit| {
                json!({
                    "query_id": query_id,
                    "rank": hit.rank as u16,
                    "event_uid": hit.event_uid,
                    "session_id": hit.session_id,
                    "source_name": hit.source_name,
                    "harness": hit.harness,
                    "inference_provider": hit.inference_provider,
                    "score": hit.score,
                    "matched_terms": hit.matched_terms as u16,
                    "doc_len": hit.doc_len,
                    "event_class": hit.event_class,
                    "payload_type": hit.payload_type,
                    "actor_role": hit.actor_role,
                    "name": hit.name,
                    "source_ref": hit.source_ref,
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

    pub(super) async fn search_events_impl(
        &self,
        query: SearchEventsQuery,
    ) -> RepoResult<SearchEventsResult> {
        let query_text = query.query.trim();
        if query_text.is_empty() {
            return Err(RepoError::invalid_argument("query cannot be empty"));
        }
        let source = query
            .source
            .as_deref()
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .unwrap_or("moraine-conversations");

        let query_id = if source == BENCHMARK_REPLAY_SOURCE {
            "benchmark-replay".to_string()
        } else {
            Uuid::new_v4().to_string()
        };
        let started = Instant::now();

        let terms_with_qf = tokenize_query(query_text, self.cfg.bm25_max_query_terms);
        if terms_with_qf.is_empty() {
            return Err(RepoError::invalid_argument(
                "query has no searchable terms (tokens shorter than 2 characters are excluded)",
            ));
        }
        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();

        let requested_limit = query.limit.unwrap_or(self.cfg.max_results).max(1);
        let limit = requested_limit.min(self.cfg.max_results);
        let limit_capped = requested_limit > limit;

        let min_should_match = query
            .min_should_match
            .unwrap_or(self.cfg.bm25_default_min_should_match)
            .max(1)
            .min(terms.len() as u16);

        let min_score = query.min_score.unwrap_or(self.cfg.bm25_default_min_score);
        let include_tool_events = query
            .include_tool_events
            .unwrap_or(self.cfg.default_include_tool_events);
        let event_kinds = Self::normalize_event_kinds(query.event_kinds)?;
        let exclude_codex_mcp = query
            .exclude_codex_mcp
            .unwrap_or(self.cfg.default_exclude_codex_mcp);
        let bypass_cache = query.bypass_cache.unwrap_or(false);
        let effective_strategy_hint = query.strategy_hint.unwrap_or_default();

        let session_id = query.session_id.clone();
        if let Some(session_id) = session_id.as_deref() {
            Self::validate_session_id(session_id)?;
        }
        let mut session_ids = query
            .session_ids
            .unwrap_or_default()
            .into_iter()
            .map(|session_id| session_id.trim().to_string())
            .filter(|session_id| !session_id.is_empty())
            .collect::<Vec<_>>();
        session_ids.sort_unstable();
        session_ids.dedup();
        for session_id in &session_ids {
            Self::validate_session_id(session_id)?;
        }
        let session_id = session_id.as_deref();
        let session_ids = (!session_ids.is_empty()).then_some(session_ids);
        let session_ids_ref = session_ids.as_deref();
        let session_hint = session_id
            .map(ToOwned::to_owned)
            .or_else(|| session_ids_ref.map(|ids| ids.join(",")))
            .unwrap_or_default();

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
                    requested_limit,
                    effective_limit: limit,
                    limit_capped,
                },
                hits: Vec::new(),
            });
        }

        let avgdl = (total_doc_len as f64 / docs as f64).max(1.0);
        let fetch_limit = Self::dedupe_fetch_limit(limit);

        let hits = if bypass_cache {
            let rows = self
                .search_events_rows_by_strategy(
                    effective_strategy_hint,
                    &terms,
                    docs,
                    avgdl,
                    include_tool_events,
                    event_kinds.as_deref(),
                    exclude_codex_mcp,
                    session_id,
                    session_ids_ref,
                    min_should_match,
                    min_score,
                    fetch_limit,
                )
                .await?;
            let rows = Self::dedupe_search_rows(rows, limit);
            self.map_search_rows_to_hits(rows).await?
        } else {
            let cache_key = Self::search_events_cache_key(
                &terms,
                effective_strategy_hint,
                include_tool_events,
                event_kinds.as_deref(),
                exclude_codex_mcp,
                session_id,
                session_ids_ref,
                min_should_match,
                min_score,
                limit,
            );

            if let Some(cached_hits) = self.search_events_cache_get(&cache_key).await {
                cached_hits
            } else {
                let fresh_rows = self
                    .search_events_rows_by_strategy(
                        effective_strategy_hint,
                        &terms,
                        docs,
                        avgdl,
                        include_tool_events,
                        event_kinds.as_deref(),
                        exclude_codex_mcp,
                        session_id,
                        session_ids_ref,
                        min_should_match,
                        min_score,
                        fetch_limit,
                    )
                    .await?;
                let fresh_rows = Self::dedupe_search_rows(fresh_rows, limit);
                let fresh_hits = self.map_search_rows_to_hits(fresh_rows).await?;
                self.search_events_cache_put(cache_key, &fresh_hits).await;
                fresh_hits
            }
        };

        let took_ms = started.elapsed().as_millis() as u32;

        if source != BENCHMARK_REPLAY_SOURCE {
            self.log_search_events(
                &query_id,
                source,
                query_text,
                &session_hint,
                &terms,
                limit,
                min_should_match,
                min_score,
                include_tool_events,
                event_kinds.as_deref(),
                exclude_codex_mcp,
                took_ms,
                &hits,
                docs,
                avgdl,
            )
            .await;
        }

        Ok(SearchEventsResult {
            query_id,
            query: query_text.to_string(),
            terms,
            stats: SearchEventsStats {
                docs,
                avgdl,
                took_ms,
                result_count: hits.len(),
                requested_limit,
                effective_limit: limit,
                limit_capped,
            },
            hits,
        })
    }

    pub(super) async fn search_mcp_events_impl(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        let query_text = query.query.trim();
        if query_text.is_empty() {
            return Err(RepoError::invalid_argument("query cannot be empty"));
        }

        if let Some(session_id) = query.session_id.as_deref() {
            Self::validate_session_id(session_id)?;
        }
        if let Some(turn_seq) = query.turn_seq {
            if turn_seq == 0 {
                return Err(RepoError::invalid_argument(
                    "turn_seq must be greater than zero",
                ));
            }
            if query.session_id.is_none() {
                return Err(RepoError::invalid_argument(
                    "turn-scoped search requires session_id",
                ));
            }
        }

        let query_id = Uuid::new_v4().to_string();
        let started = Instant::now();

        let terms_with_qf = tokenize_query(query_text, self.cfg.bm25_max_query_terms);
        if terms_with_qf.is_empty() {
            return Err(RepoError::invalid_argument(
                "query has no searchable terms (tokens shorter than 2 characters are excluded)",
            ));
        }
        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();
        let event_types = Self::normalize_mcp_event_types(query.event_types)?;

        let requested_n_hits = query.n_hits.unwrap_or(10).max(1);
        let effective_n_hits = requested_n_hits.min(self.cfg.max_results);
        let limit_capped = requested_n_hits > effective_n_hits;
        let fetch_limit = effective_n_hits.saturating_add(1);

        let min_should_match = query
            .min_should_match
            .unwrap_or(self.cfg.bm25_default_min_should_match)
            .max(1)
            .min(terms.len() as u16);
        let min_score = query.min_score.unwrap_or(self.cfg.bm25_default_min_score);

        let (docs, total_doc_len) = self.corpus_stats().await?;
        if docs == 0 {
            return Ok(SearchMcpEventsResult {
                query_id,
                query: query_text.to_string(),
                terms,
                event_types,
                truncated: false,
                stats: SearchMcpEventsStats {
                    docs: 0,
                    avgdl: 0.0,
                    took_ms: started.elapsed().as_millis() as u32,
                    result_count: 0,
                    requested_n_hits,
                    effective_n_hits,
                    limit_capped,
                    truncated: false,
                },
                hits: Vec::new(),
            });
        }

        let avgdl = (total_doc_len as f64 / docs as f64).max(1.0);
        let cache_key = Self::search_mcp_events_cache_key(
            &terms,
            &event_types,
            query.session_id.as_deref(),
            query.turn_seq,
            min_should_match,
            min_score,
            effective_n_hits,
        );
        let cached_result = self.search_mcp_events_cache_get(&cache_key).await;
        let cache_hit = cached_result.is_some();
        tracing::info!(cache_hit, "mcp_search_cache");

        let (hits, truncated) = if let Some(cached_result) = cached_result {
            cached_result
        } else {
            let mut rows = self
                .search_mcp_event_rows(
                    &terms,
                    docs,
                    avgdl,
                    &event_types,
                    query.session_id.as_deref(),
                    query.turn_seq,
                    min_should_match,
                    min_score,
                    fetch_limit,
                )
                .await?;
            let truncated = rows.len() > effective_n_hits as usize;
            if truncated {
                rows.truncate(effective_n_hits as usize);
            }
            let hits = self.map_search_mcp_rows_to_hits(rows).await?;
            self.search_mcp_events_cache_put(cache_key, &hits, truncated)
                .await;
            (hits, truncated)
        };
        let took_ms = started.elapsed().as_millis() as u32;

        Ok(SearchMcpEventsResult {
            query_id,
            query: query_text.to_string(),
            terms,
            event_types,
            truncated,
            stats: SearchMcpEventsStats {
                docs,
                avgdl,
                took_ms,
                result_count: hits.len(),
                requested_n_hits,
                effective_n_hits,
                limit_capped,
                truncated,
            },
            hits,
        })
    }

    pub(super) async fn search_conversations_impl(
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
            return Err(RepoError::invalid_argument(
                "query has no searchable terms (tokens shorter than 2 characters are excluded)",
            ));
        }
        let terms: Vec<String> = terms_with_qf.iter().map(|(term, _)| term.clone()).collect();

        let requested_limit = query.limit.unwrap_or(self.cfg.max_results).max(1);
        let limit = requested_limit.min(self.cfg.max_results);
        let limit_capped = requested_limit > limit;

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
                    requested_limit,
                    effective_limit: limit,
                    limit_capped,
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

        let candidate_set = match self
            .fetch_conversation_candidates(
                &terms,
                &idf_by_term,
                include_tool_events,
                exclude_codex_mcp,
                min_should_match,
                limit,
                query.from_unix_ms,
                query.to_unix_ms,
                query.mode,
            )
            .await
        {
            Ok(set) => set,
            Err(err) => {
                warn!("search_conversations candidate stage failed; falling back to exact path: {err}");
                ConversationCandidateSet::default()
            }
        };
        let candidate_limit = Self::conversation_candidate_limit(limit);
        let candidate_session_ids = if candidate_set.truncated
            || candidate_set.rows.is_empty()
            || candidate_set.rows.len() >= candidate_limit
        {
            None
        } else {
            Some(
                candidate_set
                    .rows
                    .into_iter()
                    .map(|row| row.session_id)
                    .collect::<Vec<_>>(),
            )
        };

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
            candidate_session_ids.as_deref(),
        )?;

        let rows: Vec<ConversationSearchRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let best_event_uids = rows
            .iter()
            .filter_map(|row| {
                if row.best_event_uid.is_empty() {
                    None
                } else {
                    Some(row.best_event_uid.clone())
                }
            })
            .collect::<Vec<_>>();
        let snippet_by_event_uid = self.fetch_conversation_snippets(&best_event_uids).await?;
        let session_ids = rows
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>();
        let session_metadata_by_session_id = self
            .fetch_conversation_session_metadata(&session_ids)
            .await?;

        let hits = rows
            .into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let ConversationSearchRow {
                    session_id,
                    first_event_time,
                    first_event_unix_ms,
                    last_event_time,
                    last_event_unix_ms,
                    harness: row_harness,
                    inference_provider: row_inference_provider,
                    score,
                    matched_terms,
                    event_count_considered,
                    best_event_uid: row_best_event_uid,
                    snippet: row_snippet,
                } = row;
                let session_metadata = session_metadata_by_session_id.get(&session_id);

                let best_event_uid = if row_best_event_uid.is_empty() {
                    None
                } else {
                    Some(row_best_event_uid)
                };
                let snippet_content = best_event_uid
                    .as_ref()
                    .and_then(|event_uid| snippet_by_event_uid.get(event_uid).cloned());
                let snippet = snippet_content
                    .as_ref()
                    .map(|content| content.snippet.clone())
                    .or((!row_snippet.is_empty()).then_some(row_snippet));
                let text_preview = snippet.clone();
                let text_content = snippet_content
                    .as_ref()
                    .and_then(|content| content.text_content.clone());
                let payload_json = snippet_content
                    .as_ref()
                    .and_then(|content| content.payload_json.clone());
                let has_first_event_time = !first_event_time.is_empty();
                let has_last_event_time = !last_event_time.is_empty();
                let harness = session_metadata
                    .and_then(|meta| (!meta.harness.is_empty()).then(|| meta.harness.clone()))
                    .or((!row_harness.is_empty()).then_some(row_harness));
                let inference_provider = session_metadata
                    .and_then(|meta| {
                        (!meta.inference_provider.is_empty())
                            .then(|| meta.inference_provider.clone())
                    })
                    .or((!row_inference_provider.is_empty()).then_some(row_inference_provider));
                let session_slug = session_metadata.and_then(|meta| {
                    (!meta.session_slug.is_empty()).then(|| meta.session_slug.clone())
                });
                let session_summary = session_metadata.and_then(|meta| {
                    (!meta.session_summary.is_empty()).then(|| meta.session_summary.clone())
                });
                ConversationSearchHit {
                    rank: idx + 1,
                    session_id,
                    first_event_time: has_first_event_time.then_some(first_event_time),
                    first_event_unix_ms: has_first_event_time.then_some(first_event_unix_ms),
                    last_event_time: has_last_event_time.then_some(last_event_time),
                    last_event_unix_ms: has_last_event_time.then_some(last_event_unix_ms),
                    harness,
                    inference_provider,
                    session_slug,
                    session_summary,
                    score,
                    matched_terms,
                    event_count_considered,
                    best_event_uid,
                    snippet,
                    text_preview,
                    text_content,
                    payload_json,
                }
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
                requested_limit,
                effective_limit: limit,
                limit_capped,
            },
            hits,
        })
    }
}
