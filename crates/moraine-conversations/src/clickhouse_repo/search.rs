use super::canonical_list::{SessionMetaBatchRow, SessionTotalsBatchRow};
use super::canonical_open::{list_title_and_summary, metadata_precedence, MetaRow};
use super::consistency::EventTsBounds;
use super::search_canonical::{
    mcp_candidate_fetch_size, ScopeExistsRow, SearchCandidateDerivationRow, SearchCandidateRow,
    SearchDedupKeyRow, SearchEventsCandidateRow, SearchRankingParams, SearchTurnAggregateRow,
    SearchWideRow, TurnEventUidRow, MAX_TURN_SCOPE_UIDS,
};
use super::*;

pub(super) const CONVERSATION_CANDIDATE_MIN: usize = 512;
pub(super) const CONVERSATION_CANDIDATE_MULTIPLIER: usize = 80;
pub(super) const CONVERSATION_CANDIDATE_MAX: usize = 20_000;
pub(super) const CONVERSATION_RECENT_WINDOW_MS: i64 = 45_000;
pub(super) const CONVERSATION_RECENT_CANDIDATE_LIMIT: usize = 1024;
pub(super) const CODEX_FINAL_ANSWER_MIRROR_MAX_TIMESTAMP_DELTA_MS: u64 = 10;

/// Which sessions a conversation-search postings statement may read (issue
/// #597 §1.5/F5).
///
/// The retired shape was `Option<&[String]>`, and `None` meant "emit no session
/// predicate at all" — a whole-corpus postings scan. THREE distinct conditions
/// produced it: a candidate-stage error (swallowed by a `warn!`), zero
/// candidates, and candidate saturation. Two of those are ordinary outcomes, so
/// the unbounded branch was the common case, not the exception.
///
/// This enum has no variant that means "no predicate on a scoring statement".
/// [`Self::Discovery`] is only constructible by the candidate stage, which has
/// no session set by construction and is bounded by its own `LIMIT`; the
/// scoring stage can only pass [`Self::Sessions`], and
/// `search_conversations_impl` returns zero results before building SQL when
/// that list is empty.
#[derive(Debug, Clone, Copy)]
pub(super) enum ConversationSessionFilter<'a> {
    /// Candidate DISCOVERY. Bounded by `LIMIT`, not by a session predicate.
    Discovery,
    /// Candidate SCORING, restricted to the sessions the discovery stage found.
    Sessions(&'a [String]),
}

/// One bounded ranking pass's result, shared by the v1 (projected-header) and
/// v2 (canonical-index) engines.
pub(super) struct McpSearchPage {
    pub(super) rows: Vec<SearchMcpEventRow>,
    pub(super) docs: u64,
    pub(super) total_doc_len: u64,
    pub(super) scope_exists: bool,
    /// See [`crate::domain::SearchMcpEventsResult::incomplete_due_to_candidate_budget`].
    pub(super) incomplete_due_to_candidate_budget: bool,
}

/// The two knobs an INTERNAL consumer of the event ranking may set and the
/// public `search_mcp_events` tool may not.
///
/// It is a struct rather than two positional arguments because both are
/// easy-to-transpose small scalars whose defaults are correct for the tool path
/// and wrong for the internal one.
#[derive(Debug, Clone, Copy)]
pub(super) struct McpEventRankingOptions {
    /// Ceiling on ranked HITS.
    ///
    /// `max_results` bounds *rows a caller may receive*, and a consumer that
    /// folds many hits into one result row is not bounded by the same number.
    /// Session discovery by content ranks EVENTS and answers in SESSIONS —
    /// several hits routinely land in one session — so it must be able to ask
    /// for more hits than the sessions it returns. Clamping its request to
    /// `max_results` made the over-fetch inert in every shipped configuration,
    /// because `max_results` is also the session limit.
    ///
    /// It is still bounded, and bounded from BOTH sides rather than by
    /// `limit x factor` alone: see [`super::list::session_search_hit_budget`],
    /// which caps the fan-in so the derived candidate window cannot reach
    /// [`super::search_canonical::MCP_SEARCH_CANDIDATE_MAX`], and floors it
    /// above `limit` so the fan-in cannot collapse to 1:1 at the largest page.
    /// `effective_n_hits` stays part of the result cache key, so two callers
    /// with different ceilings cannot serve each other's windows.
    pub(super) hit_cap: u16,
    /// The issue-598 `open_v2` readiness verdict, when the caller has already
    /// resolved it.
    ///
    /// `None` means "probe it" and is right for a request whose only readiness
    /// branch is the ranking engine. `Some` is required of any caller that
    /// branches on readiness AGAIN after ranking: a second probe is a second
    /// point read, and — because the latch flips when a backfill publishes —
    /// two probes in one request can disagree, ranking over the `mcp_open_*`
    /// projection and then hydrating from the canonical navigation index.
    pub(super) canonical_ready: Option<bool>,
}

impl McpEventRankingOptions {
    /// The public tool's settings: hits bounded by the rows a caller may
    /// receive, readiness resolved by the ranking itself.
    pub(super) fn for_tool_caller(cfg: &RepoConfig) -> Self {
        Self {
            hit_cap: cfg.max_results,
            canonical_ready: None,
        }
    }
}

impl ClickHouseConversationRepository {
    /// Session headers that are authorized by the operation's captured source
    /// heads and by the current canonical session contents.
    pub(super) fn mcp_search_sessions_source(&self) -> String {
        self.mcp_search_sessions_source_for(None)
    }

    fn mcp_search_sessions_source_for(&self, session_ids_source: Option<&str>) -> String {
        let snapshot = require_active_publication_snapshot("MCP search session reads");

        let headers = self.table_ref("mcp_open_publication_headers");
        let history = self.table_ref("v_published_source_generation_history");
        let dirty_sessions = self.table_ref("mcp_open_dirty_sessions");
        let captured_heads = snapshot.captured_source_heads_sql(&history);
        let live_events = self.live_events_source();
        let candidate_filter = |alias: &str| {
            session_ids_source
                .map(|source| {
                    format!("\n      AND {alias}.session_id IN (SELECT session_id FROM {source})")
                })
                .unwrap_or_default()
        };
        let header_candidate_filter = candidate_filter("h");
        let source_candidate_filter = candidate_filter("e");
        let dirty_candidate_filter = candidate_filter("dirty");
        format!(
            "(WITH
  {captured_heads} AS captured_heads,
  head_authorized_headers AS (
    SELECT h.*
    FROM {headers} AS h FINAL
    WHERE h.tombstone = 0
      AND length(h.required_source_heads) > 0
      AND arrayAll(
        required_head -> has(captured_heads, required_head),
        h.required_source_heads
      ){header_candidate_filter}
  ),
  current_sources AS (
    SELECT
      e.session_id AS session_id,
      toUInt64(cityHash64(arraySort(groupArray(tuple(e.event_uid, e.event_version))))) AS source_revision
    FROM {live_events} AS e
    WHERE notEmpty(e.session_id){source_candidate_filter}
      AND e.session_id IN (SELECT session_id FROM head_authorized_headers)
    GROUP BY e.session_id
  ),
  current_dirty AS (
    SELECT
      dirty.session_id AS session_id,
      toUInt64(max(dirty.dirty_revision)) AS dirty_revision
    FROM {dirty_sessions} AS dirty FINAL
    WHERE notEmpty(dirty.session_id){dirty_candidate_filter}
      AND dirty.session_id IN (SELECT session_id FROM head_authorized_headers)
    GROUP BY dirty.session_id
  )
SELECT
  h.session_id AS session_id,
  toUInt8(h.slot) AS slot,
  toUInt64(h.generation) AS generation,
  toUInt64(h.source_revision) AS source_revision,
  toUInt64(h.dirty_revision) AS dirty_revision,
  h.first_event_time AS first_event_time,
  h.last_event_time AS last_event_time,
  toUInt32(h.total_turns) AS total_turns,
  toUInt64(h.total_events) AS total_events,
  toUInt64(h.user_messages) AS user_messages,
  toUInt64(h.assistant_messages) AS assistant_messages,
  toUInt64(h.tool_calls) AS tool_calls,
  toUInt64(h.tool_results) AS tool_results,
  h.title AS title,
  h.session_slug AS session_slug,
  h.session_summary AS session_summary,
  toUInt8(h.completed) AS completed,
  h.origin_cwd AS origin_cwd
FROM head_authorized_headers AS h
ALL INNER JOIN current_sources AS source
  ON source.session_id = h.session_id
ANY LEFT JOIN current_dirty AS dirty
  ON dirty.session_id = h.session_id
WHERE h.source_revision = source.source_revision
  AND h.dirty_revision = ifNull(dirty.dirty_revision, toUInt64(0))
ORDER BY h.session_id ASC, h.header_revision DESC
LIMIT 1 BY h.session_id)"
        )
    }

    /// The retired exact oracle (issue #597 §1.5/F1, F3).
    ///
    /// This is the unbounded shape the issue exists to remove: an unfiltered
    /// `GROUP BY t.source_host, t.event_uid` over `v_live_search_documents`
    /// holding `any(t.text_content)` and `any(t.payload_json)` in aggregate
    /// state — `O(D × doc_bytes)` — reached from three live triggers (an empty
    /// fast pass, any broad term, and the public `oracle_exact` strategy).
    ///
    /// It is retained ONLY under `cfg(test)` and has no live caller: it is the
    /// independent reference the relevance oracle compares bounded ranking
    /// against, which is worth more than deleting it (the deleted
    /// `rank_cached_postings` tests compared a test-only scorer against a
    /// reference re-implemented inline in the same test, so they could stay
    /// green through any regression in the shipped path). Anything that reaches
    /// it from a non-test path is a bug;
    /// `search_events_ranks_once_from_postings_and_reads_no_content` is what
    /// proves the interactive path does not.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(super) fn build_search_events_exact_oracle_sql(
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

        let postings_table = self.table_ref("v_live_search_postings");
        let documents_table = self.table_ref("v_live_search_documents");
        let terms_array_sql = sql_array_strings(terms);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let documents_join_sql = if use_document_codex_flag {
            format!(
                "(SELECT
  t.source_host AS source_host,
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
GROUP BY t.source_host, t.event_uid)"
            )
        } else {
            format!(
                "(SELECT
  t.source_host AS source_host,
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
GROUP BY t.source_host, t.event_uid)"
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
                "NOT {}",
                moraine_clickhouse::mcp_tool_names::sql_predicate("d.name")
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
  p.source_host AS source_host,
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
ALL INNER JOIN {documents_join_sql} AS d
  ON d.source_host = p.source_host
 AND d.event_uid = p.doc_id
WHERE {where_sql}
GROUP BY p.doc_id, p.source_host
HAVING matched_terms >= {min_should_match} AND score >= {min_score:.6}
ORDER BY score DESC, event_uid ASC, source_host ASC
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
        event_types: &[McpEventType],
        session_id: Option<&str>,
        turn_seq: Option<u32>,
        harness: Option<&str>,
        source_name: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        corpus_stats: Option<(u64, u64)>,
        limit: u32,
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

        let postings_table = self.table_ref("v_live_search_postings");
        let corpus_table = self.table_ref("search_corpus_stats");
        let projection_state_table = self.table_ref("mcp_open_projection_state");
        let sessions_source = self.mcp_search_sessions_source();
        let sessions_table = "authorized_sessions";
        let turns_table = self.table_ref("mcp_open_turns");
        let events_table = self.table_ref("mcp_open_events");
        let dirty_sessions_table = self.table_ref("mcp_open_dirty_sessions");
        let live_events_source = self.live_events_source();
        let terms_array_sql = sql_array_strings(terms);
        let corpus_stats_sql = match corpus_stats {
            Some((docs, total_doc_len)) => {
                format!("tuple(toUInt64({docs}), toUInt64({total_doc_len}))")
            }
            None => format!(
                "(\n    SELECT tuple(toUInt64(docs), toUInt64(total_doc_len))\n    FROM {corpus_table}\n  )"
            ),
        };

        let mut posting_clauses = Vec::new();
        if let Some(session_id) = session_id {
            posting_clauses.push(format!("p.session_id = {}", sql_quote(session_id)));
        }
        let mut event_clauses = Vec::new();
        if let Some(turn_seq) = turn_seq {
            let Some(session_id) = session_id else {
                return Err(RepoError::invalid_argument(
                    "turn-scoped search requires session_id",
                ));
            };
            event_clauses.push(format!(
                "e.session_id = {} AND e.turn_seq = {}",
                sql_quote(session_id),
                turn_seq
            ));
        }
        if let Some(harness) = harness {
            posting_clauses.push(format!("p.harness = {}", sql_quote(harness)));
        }
        if let Some(source_name) = source_name {
            posting_clauses.push(format!("p.source_name = {}", sql_quote(source_name)));
        }
        posting_clauses.push("p.source_name != 'codex-mcp'".to_string());
        posting_clauses.push(format!(
            "NOT {}",
            moraine_clickhouse::mcp_tool_names::sql_predicate("p.name")
        ));
        posting_clauses.push(Self::mcp_event_type_filter_clause(
            "p.event_class",
            "p.payload_type",
            "p.actor_role",
            event_types,
        ));

        let projected_origin_clause = |alias: &str| {
            self.cfg.session_scope.as_ref().map(|scope| {
                let roots = scope
                    .roots
                    .iter()
                    .map(|root| {
                        format!(
                        "{alias}.origin_cwd = {root} OR startsWith({alias}.origin_cwd, {prefix})",
                        root = sql_quote(root),
                        prefix = sql_quote(&format!("{root}/")),
                    )
                    })
                    .collect::<Vec<_>>()
                    .join(" OR ");
                format!("({roots})")
            })
        };
        let posting_origin_clause = projected_origin_clause("s");
        if let Some(scope_clause) = posting_origin_clause.as_ref() {
            posting_clauses.push(scope_clause.clone());
        }
        let posting_where_sql = posting_clauses.join("\n      AND ");
        event_clauses.push("projection_ready = 1".to_string());
        event_clauses.push("projection_clean = 1".to_string());
        let event_where_sql = event_clauses.join("\n      AND ");
        let scope_origin_filter = projected_origin_clause("scope_s")
            .as_deref()
            .map(|clause| format!(" AND {clause}"))
            .unwrap_or_default();
        let scope_state_sql = match (session_id, turn_seq) {
            (Some(session_id), Some(turn_seq)) => format!(
                "SELECT toUInt8(count() > 0) AS scope_exists
FROM {sessions_table} AS scope_s
ALL INNER JOIN {turns_table} AS scope_t FINAL
  ON scope_t.session_id = scope_s.session_id
  AND scope_t.slot = scope_s.slot
  AND scope_t.generation = scope_s.generation
WHERE scope_s.session_id = {session_id}
  AND scope_t.turn_seq = {turn_seq}{scope_origin_filter}",
                session_id = sql_quote(session_id),
            ),
            (Some(session_id), None) => format!(
                "SELECT toUInt8(count() > 0) AS scope_exists
FROM {sessions_table} AS scope_s
WHERE scope_s.session_id = {session_id}{scope_origin_filter}",
                session_id = sql_quote(session_id),
            ),
            (None, Some(_)) => {
                return Err(RepoError::invalid_argument(
                    "turn-scoped search requires session_id",
                ));
            }
            (None, None) => "SELECT toUInt8(1) AS scope_exists".to_string(),
        };
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);

        Ok(format!(
            "WITH
  authorized_sessions AS {sessions_source},
  live_session_ids AS (
    SELECT e.session_id AS session_id
    FROM {live_events_source} AS e
    WHERE notEmpty(e.session_id)
    GROUP BY e.session_id
  ),
  {k1:.6} AS k1,
  {b:.6} AS b,
  {terms_array_sql} AS q_terms,
  {corpus_stats_sql} AS corpus_stats,
  tupleElement(corpus_stats, 1) AS corpus_docs,
  tupleElement(corpus_stats, 2) AS corpus_total_doc_len,
  greatest(
    if(corpus_docs = 0, 1.0, toFloat64(corpus_total_doc_len) / toFloat64(corpus_docs)),
    1.0
  ) AS avgdl,
  (
    SELECT toUInt8(if(count() = 0, 0, max(ready)))
    FROM {projection_state_table} FINAL
    WHERE state_key = 'global'
  ) AS projection_ready,
  (
    SELECT tuple(
      toUInt8(countIf(dirty.dirty_revision > ifNull(published.dirty_revision, 0)) = 0),
      toUInt64(ifNull(max(dirty.dirty_revision), 0))
    )
    FROM (
      SELECT session_id, dirty_revision
      FROM {dirty_sessions_table} FINAL
      WHERE notEmpty(session_id)
        AND session_id IN (SELECT session_id FROM live_session_ids)
    ) AS dirty
    ANY LEFT JOIN (
      SELECT session_id, dirty_revision
      FROM {sessions_table}
    ) AS published ON published.session_id = dirty.session_id
  ) AS projection_status,
  tupleElement(projection_status, 1) AS projection_clean,
  tupleElement(projection_status, 2) AS projection_revision,
  ({scope_state_sql}) AS scope_exists,
  term_postings AS (
    SELECT
      p.*,
      toUInt64(count() OVER (PARTITION BY p.term)) AS df
    FROM {postings_table} AS p FINAL
    WHERE p.term IN q_terms
  ),
  ranked AS (
    SELECT
      p.source_host AS source_host,
      p.doc_id AS event_uid,
      any(s.session_id) AS session_id,
      toUInt8(any(s.slot)) AS slot,
      toUInt64(any(s.generation)) AS generation,
      sum(
        log(1.0 + ((greatest(toFloat64(corpus_docs), toFloat64(p.df))
          - toFloat64(p.df) + 0.5) / (toFloat64(p.df) + 0.5)))
        * ((toFloat64(p.tf) * (k1 + 1.0))
          / (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl))))
      ) AS raw_score,
      toUInt64(count()) AS matched_terms,
      toInt64(toUnixTimestamp64Milli(any(e.event_time))) AS event_unix_ms
    FROM term_postings AS p
    ALL INNER JOIN {sessions_table} AS s ON s.session_id = p.session_id
    ALL INNER JOIN {events_table} AS e FINAL
      ON e.source_host = p.source_host
      AND e.event_uid = p.doc_id
      AND e.session_id = s.session_id
      AND e.slot = s.slot
      AND e.generation = s.generation
    WHERE {posting_where_sql}
      AND {event_where_sql}
    GROUP BY p.doc_id, p.source_host
    HAVING matched_terms >= {min_should_match} AND raw_score >= {min_score:.6}
    ORDER BY raw_score DESC, event_unix_ms DESC, event_uid ASC, source_host ASC
    LIMIT {limit}
  )
SELECT *
FROM (
SELECT
  toUInt8(0) AS row_kind,
  ranked.event_uid AS event_uid,
  ranked.source_host AS source_host,
  ranked.session_id AS session_id,
  ranked.slot AS slot,
  ranked.generation AS generation,
  ranked.raw_score AS raw_score,
  ranked.matched_terms AS matched_terms,
  ranked.event_unix_ms AS event_unix_ms,
  corpus_docs AS docs,
  corpus_total_doc_len AS total_doc_len,
  scope_exists AS scope_exists,
  projection_ready AS projection_ready,
  projection_clean AS projection_clean,
  projection_revision AS projection_revision
FROM ranked
UNION ALL
SELECT
  toUInt8(1) AS row_kind,
  '' AS event_uid,
  '' AS source_host,
  '' AS session_id,
  toUInt8(0) AS slot,
  toUInt64(0) AS generation,
  toFloat64(0) AS raw_score,
  toUInt64(0) AS matched_terms,
  toInt64(0) AS event_unix_ms,
  corpus_docs AS docs,
  corpus_total_doc_len AS total_doc_len,
  scope_exists AS scope_exists,
  projection_ready AS projection_ready,
  projection_clean AS projection_clean,
  projection_revision AS projection_revision
)
ORDER BY row_kind ASC, raw_score DESC, event_unix_ms DESC, event_uid ASC, source_host ASC
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864
FORMAT JSONEachRow",
            postings_table = postings_table,
            projection_state_table = projection_state_table,
            sessions_source = sessions_source,
            sessions_table = sessions_table,
            live_events_source = live_events_source,
            events_table = events_table,
            dirty_sessions_table = dirty_sessions_table,
        ))
    }

    pub(super) fn build_search_mcp_event_details_sql(
        &self,
        candidates: &[SearchMcpCandidateRow],
    ) -> RepoResult<String> {
        if candidates.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot hydrate MCP search rows for empty event_uids",
            ));
        }

        let documents_table = self.table_ref("v_live_search_documents");
        let mut candidate_session_ids = candidates
            .iter()
            .map(|candidate| candidate.session_id.clone())
            .collect::<Vec<_>>();
        candidate_session_ids.sort_unstable();
        candidate_session_ids.dedup();
        let candidate_session_ids_sql = sql_array_strings(&candidate_session_ids);
        let sessions_source = self.mcp_search_sessions_source_for(Some("candidate_session_ids"));
        let sessions_table = "authorized_sessions";
        let turns_table = self.table_ref("mcp_open_turns");
        let projected_events_table = self.table_ref("mcp_open_events");
        let events_table = self.table_ref("v_live_events");
        let event_uids = candidates
            .iter()
            .map(|candidate| candidate.event_uid.clone())
            .collect::<Vec<_>>();
        let event_uids_sql = sql_array_strings(&event_uids);
        let candidate_heads_sql = candidates
            .iter()
            .map(|candidate| {
                format!(
                    "({}, {}, {}, toUInt8({}), toUInt64({}))",
                    sql_quote(&candidate.source_host),
                    sql_quote(&candidate.event_uid),
                    sql_quote(&candidate.session_id),
                    candidate.slot,
                    candidate.generation,
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let preview = self.cfg.preview_chars;
        let text_content_limit = usize::from(preview).saturating_mul(4);
        let payload_json_limit = usize::from(preview).saturating_mul(8);

        Ok(format!(
            "WITH
  candidate_session_ids AS (
    SELECT arrayJoin({candidate_session_ids_sql}) AS session_id
  ),
  authorized_sessions AS {sessions_source},
  {event_uids_sql} AS event_uids,
  candidate_heads AS (
    SELECT
      tupleElement(candidate, 1) AS source_host,
      tupleElement(candidate, 2) AS event_uid,
      tupleElement(candidate, 3) AS session_id,
      toUInt8(tupleElement(candidate, 4)) AS slot,
      toUInt64(tupleElement(candidate, 5)) AS generation
    FROM (SELECT arrayJoin([{candidate_heads_sql}]) AS candidate)
  ),
  documents AS (
    SELECT
      document.source_host AS source_host,
      document.event_uid AS event_uid,
      argMax(document.session_id, document.doc_version) AS session_id,
      argMax(document.source_name, document.doc_version) AS source_name,
      argMax(document.harness, document.doc_version) AS harness,
      argMax(document.inference_provider, document.doc_version) AS inference_provider,
      argMax(document.event_class, document.doc_version) AS event_class,
      argMax(document.payload_type, document.doc_version) AS payload_type,
      argMax(document.actor_role, document.doc_version) AS actor_role,
      argMax(document.name, document.doc_version) AS name,
      argMax(document.phase, document.doc_version) AS phase,
      argMax(JSONExtractString(document.payload_json, 'phase'), document.doc_version) AS payload_phase,
      argMax(document.source_ref, document.doc_version) AS source_ref,
      toUInt32(argMax(document.doc_len, document.doc_version)) AS doc_len,
      argMax(leftUTF8(document.text_content, {preview}), document.doc_version) AS text_preview,
      argMax(leftUTF8(document.text_content, {text_content_limit}), document.doc_version) AS text_content,
      argMax(leftUTF8(document.payload_json, {payload_json_limit}), document.doc_version) AS payload_json
    FROM {documents_table} AS document
    WHERE (document.source_host, document.event_uid) IN (
      SELECT source_host, event_uid FROM candidate_heads
    )
    GROUP BY document.source_host, document.event_uid
  ),
  models AS (
    SELECT source_event.source_host AS source_host,
      source_event.event_uid AS event_uid,
      argMax(source_event.model, source_event.event_version) AS model
    FROM {events_table} AS source_event
    WHERE (source_event.source_host, source_event.event_uid) IN (
      SELECT source_host, event_uid FROM candidate_heads
    )
    GROUP BY source_event.source_host, source_event.event_uid
  )
SELECT
  documents.event_uid AS event_uid,
  documents.source_host AS source_host,
  documents.session_id AS session_id,
  documents.source_name AS source_name,
  documents.harness AS harness,
  documents.inference_provider AS inference_provider,
  projected_events.endpoint_kind AS endpoint_kind,
  documents.event_class AS event_class,
  documents.payload_type AS payload_type,
  documents.actor_role AS actor_role,
  documents.name AS name,
  documents.phase AS phase,
  documents.payload_phase AS payload_phase,
  documents.source_ref AS source_ref,
  documents.doc_len AS doc_len,
  documents.text_preview AS text_preview,
  documents.text_content AS text_content,
  hex(SHA256(projected_events.text_content)) AS text_content_digest,
  documents.payload_json AS payload_json,
  projected_events.event_type AS mcp_event_type,
  toFloat64(0) AS raw_score,
  toUInt64(0) AS matched_terms,
  toString(projected_events.event_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(projected_events.event_time)) AS event_unix_ms,
  toUInt64(projected_events.event_order) AS event_order,
  toUInt32(projected_events.turn_seq) AS turn_seq,
  toUInt32(projected_events.event_ordinal) AS event_ordinal,
  toUInt64(ifNull(turns.total_events, 0)) AS turn_event_count,
  toUInt8(ifNull(turns.completed, 0)) AS turn_completed,
  ifNull(turns.terminal_event_uid, '') AS turn_terminal_event_uid,
  projected_events.call_id AS call_id,
  projected_events.item_id AS item_id,
  ifNull(models.model, '') AS model,
  toInt64(toUnixTimestamp64Milli(sessions.first_event_time)) AS session_started_at_unix_ms,
  toInt64(toUnixTimestamp64Milli(sessions.last_event_time)) AS session_updated_at_unix_ms,
  sessions.title AS session_title,
  sessions.session_slug AS session_slug,
  sessions.session_summary AS session_summary,
  toUInt8(sessions.completed) AS session_completed
FROM documents
ALL INNER JOIN candidate_heads AS candidate
  ON candidate.source_host = documents.source_host
  AND candidate.event_uid = documents.event_uid
ALL INNER JOIN {sessions_table} AS sessions
  ON sessions.session_id = candidate.session_id
  AND sessions.slot = candidate.slot
  AND sessions.generation = candidate.generation
ALL INNER JOIN {projected_events_table} AS projected_events FINAL
  ON projected_events.source_host = candidate.source_host
  AND projected_events.event_uid = candidate.event_uid
  AND projected_events.session_id = candidate.session_id
  AND projected_events.slot = candidate.slot
  AND projected_events.generation = candidate.generation
ANY LEFT JOIN {turns_table} AS turns FINAL
  ON turns.session_id = sessions.session_id
  AND turns.slot = sessions.slot
  AND turns.generation = sessions.generation
  AND turns.turn_seq = projected_events.turn_seq
ANY LEFT JOIN models
  ON models.source_host = documents.source_host
  AND models.event_uid = documents.event_uid
ORDER BY indexOf(event_uids, documents.event_uid) ASC
FORMAT JSONEachRow",
        ))
    }

    /// Bounded winner hydration for `search_events`: the document row for
    /// exactly the requested identities, with the wide columns truncated inside
    /// the aggregate.
    ///
    /// Issue #597/F8 removed the `use_document_codex_flag` branch. When the
    /// probe reported the column absent this statement fell back to
    /// `positionCaseInsensitiveUTF8(t.payload_json, 'codex-mcp')`, which scans
    /// every full payload value in the requested set. `has_codex_mcp` is a
    /// MATERIALIZED column installed by migration 009 and listed in
    /// `REQUIRED_SCHEMA_OBJECTS`, so a backend that lacks it has not been
    /// migrated and must fail loudly rather than silently switch to a
    /// content scan.
    pub(super) fn build_search_events_hydrate_sql(
        &self,
        document_identities: &[SearchDocumentIdentity],
    ) -> RepoResult<String> {
        if document_identities.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot hydrate search rows for empty document identities",
            ));
        }
        let documents_table = self.table_ref("v_live_search_documents");
        let event_uids = document_identities
            .iter()
            .map(|identity| identity.event_uid.clone())
            .collect::<Vec<_>>();
        let event_uids_array = sql_array_strings(&event_uids);
        let identities_sql = document_identities
            .iter()
            .map(|identity| {
                format!(
                    "({}, {})",
                    sql_quote(&identity.source_host),
                    sql_quote(&identity.event_uid)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);
        // Truncate the fat columns inside the aggregation (issue #443): the
        // GROUP BY state then holds at most `*_limit` characters per uid
        // instead of full multi-MB payload blobs.
        let codex_inner_expr = "toUInt8(any(t.has_codex_mcp))";
        let documents_source_sql = format!(
            "(SELECT
  t.source_host AS source_host,
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
ALL INNER JOIN requested_documents AS requested
  ON requested.source_host = t.source_host
 AND requested.event_uid = t.event_uid
WHERE t.event_uid IN {event_uids_array}
GROUP BY t.source_host, t.event_uid)"
        );

        Ok(format!(
            "WITH requested_documents AS (
  SELECT
    tupleElement(identity, 1) AS source_host,
    tupleElement(identity, 2) AS event_uid
  FROM (SELECT arrayJoin([{identities_sql}]) AS identity)
)
SELECT
  d.source_host AS source_host,
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

    pub(super) fn bm25_idf(docs: u64, df: u64) -> f64 {
        let idf = if df == 0 {
            (1.0 + ((docs as f64 + 0.5) / 0.5)).ln()
        } else {
            let n = docs.max(df) as f64;
            (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln()
        };
        idf.max(0.0)
    }

    pub(super) async fn load_search_doc_extras(
        &self,
        document_identities: &[SearchDocumentIdentity],
    ) -> RepoResult<HashMap<SearchDocumentIdentity, SearchDocExtraCacheEntry>> {
        let now = Instant::now();
        let mut by_identity = HashMap::<SearchDocumentIdentity, SearchDocExtraCacheEntry>::new();
        let mut missing_identities = Vec::<SearchDocumentIdentity>::new();

        {
            let cache = self.search_doc_extra_cache.read().await;
            for identity in document_identities {
                let cache_key = publication_cache_key(&format!(
                    "document:{}:{}:{}",
                    identity.source_host.len(),
                    identity.source_host,
                    identity.event_uid
                ));
                if let Some(entry) = cache_key
                    .as_deref()
                    .and_then(|cache_key| cache.get(cache_key))
                {
                    if now.duration_since(entry.fetched_at) <= SEARCH_DOC_EXTRA_CACHE_TTL {
                        by_identity.insert(identity.clone(), entry.clone());
                        continue;
                    }
                }
                missing_identities.push(identity.clone());
            }
        }

        if !missing_identities.is_empty() {
            let query = self.build_search_events_hydrate_sql(&missing_identities)?;
            let fetched_rows: Vec<SearchDocExtraRow> =
                self.map_backend(self.query_rows(&query, None).await)?;

            let mut cache = self.search_doc_extra_cache.write().await;

            for row in fetched_rows {
                let identity = SearchDocumentIdentity::new(&row.source_host, &row.event_uid);
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
                by_identity.insert(identity.clone(), entry.clone());
                if let Some(cache_key) = publication_cache_key(&format!(
                    "document:{}:{}:{}",
                    identity.source_host.len(),
                    identity.source_host,
                    identity.event_uid
                )) {
                    cache.insert(cache_key, entry);
                }
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

        Ok(by_identity)
    }

    /// `search_events`' bounded ranking + hydration path (issue #597 WI-06).
    ///
    /// Two statements, both bounded: one SQL ranking pass over the term-pruned,
    /// locator-authorized postings ([`Self::build_search_events_ranking_sql`]),
    /// then one hydration read for the bounded winner identities. What this
    /// replaces:
    ///
    /// * the in-process scorer, fed by an unbounded `SELECT … FROM
    ///   v_live_search_postings WHERE term IN (…)` with no `LIMIT` that
    ///   materialized every posting for every query term into process memory
    ///   (F4), plus its 15 s posting cache;
    /// * the broad-term bail (F2), which returned zero rows for exactly the
    ///   queries most in need of a bound and handed them to —
    /// * the exact fallback (F1): an unfiltered `GROUP BY` over
    ///   `v_live_search_documents` holding `any(text_content)` /
    ///   `any(payload_json)` in aggregate state, triggered whenever the fast
    ///   pass returned empty for ANY reason, including "there is genuinely no
    ///   match". An empty result is now an empty result.
    ///
    /// Filters are applied twice on purpose. In ranking they are fixed-width
    /// predicates on posting columns (which `mv_search_postings` copies
    /// verbatim from `search_documents`, so they are exact for every dimension
    /// except the codex payload flag). After hydration
    /// [`Self::passes_search_doc_filters`] re-checks all of them against the
    /// document row, which is what keeps `exclude_codex_mcp` exact: postings
    /// carry no `has_codex_mcp`, so ranking can apply only the fixed-width
    /// `source_name` / internal-tool-name recall half.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_events_rows(
        &self,
        terms: &[String],
        docs: u64,
        total_doc_len: u64,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchRow>> {
        // `limit` is already `dedupe_fetch_limit(user_limit)` = 3x, and the
        // ranking window over-fetches that by another 3x. The two multipliers
        // cover different losses and both are needed: the outer one absorbs the
        // #539 mirror collapse in `dedupe_search_rows`, the inner one absorbs
        // the drops that happen BEFORE dedup — a ranked posting with no live
        // document row, and the exact `has_codex_mcp` / event-kind re-check
        // that ranking can only approximate. The product is still capped by
        // `MCP_SEARCH_CANDIDATE_MAX`, so the window is bounded regardless.
        let candidate_fetch_size = mcp_candidate_fetch_size(limit);
        let ranking_sql = self.build_search_events_ranking_sql(
            terms,
            include_tool_events,
            event_kinds,
            exclude_codex_mcp,
            session_id,
            session_ids,
            min_should_match,
            min_score,
            (docs, total_doc_len),
            candidate_fetch_size,
        )?;
        let candidates: Vec<SearchEventsCandidateRow> =
            self.map_backend(self.query_rows(&ranking_sql, None).await)?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let document_identities = candidates
            .iter()
            .map(|candidate| {
                SearchDocumentIdentity::new(
                    candidate.source_host.clone(),
                    candidate.event_uid.clone(),
                )
            })
            .collect::<Vec<_>>();
        let doc_extras = self.load_search_doc_extras(&document_identities).await?;

        let mut rows = Vec::<SearchRow>::with_capacity(candidates.len().min(usize::from(limit)));
        for candidate in candidates {
            let identity = SearchDocumentIdentity::new(
                candidate.source_host.clone(),
                candidate.event_uid.clone(),
            );
            // No document row for a ranked posting means the posting's document
            // revision is gone: a provable staleness drop, never a fallback.
            let Some(extra) = doc_extras.get(&identity) else {
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
            rows.push(SearchRow {
                source_host: candidate.source_host,
                event_uid: candidate.event_uid,
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
                score: candidate.score,
                matched_terms: candidate.matched_terms,
            });
            if rows.len() >= usize::from(limit) {
                break;
            }
        }
        Ok(rows)
    }

    /// ONE bounded over-fetch pass per request (issue #597 §1.6).
    ///
    /// This replaces the retired offset-refill loop, which re-executed the
    /// entire ranking statement — including every corpus-sized preamble
    /// relation — with increasing offsets up to 16 times, each followed by a
    /// detail statement, and then failed with
    /// `backend("MCP search duplicate scan budget exhausted")`. A single ranked
    /// window is over-fetched by [`mcp_candidate_fetch_size`]; when dedup and
    /// validation eat enough of a SATURATED window that fewer than
    /// `unique_fetch_limit` unique hits survive, the valid hits are returned
    /// with [`McpSearchPage::incomplete_due_to_candidate_budget`] rather than an
    /// error. `resource_exhausted` stays reserved for a #600 envelope
    /// violation.
    ///
    /// The engine is chosen by the issue-598 `open_v2` readiness latch — the
    /// same authority the `open` cutover and the issue-599 listing path use.
    /// While the canonical read indexes are not published the legacy
    /// projected-header engine still serves; it is deleted wholesale in the
    /// projector-retirement PR.
    ///
    /// `canonical_ready` is that latch's verdict, PASSED IN rather than probed
    /// here: a request whose later stages also branch on readiness must reach
    /// them with the same verdict this one used, or a backfill publishing
    /// mid-request produces one answer ranked over the `mcp_open_*` projection
    /// and hydrated from the canonical navigation index.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_mcp_event_page(
        &self,
        terms: &[String],
        event_types: &[McpEventType],
        session_id: Option<&str>,
        turn_seq: Option<u32>,
        harness: Option<&str>,
        source_name: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        unique_fetch_limit: u16,
        canonical_ready: bool,
    ) -> RepoResult<McpSearchPage> {
        if canonical_ready {
            return self
                .search_mcp_event_page_v2(
                    terms,
                    event_types,
                    session_id,
                    turn_seq,
                    harness,
                    source_name,
                    min_should_match,
                    min_score,
                    unique_fetch_limit,
                )
                .await;
        }
        self.search_mcp_event_page_v1(
            terms,
            event_types,
            session_id,
            turn_seq,
            harness,
            source_name,
            min_should_match,
            min_score,
            unique_fetch_limit,
        )
        .await
    }

    /// Legacy engine: ranks and hydrates through the `mcp_open_*` projection.
    /// Unchanged apart from losing the refill loop — one ranking statement, one
    /// detail statement, and the same global projector gates it has always
    /// enforced. Retired with the projector.
    #[allow(clippy::too_many_arguments)]
    async fn search_mcp_event_page_v1(
        &self,
        terms: &[String],
        event_types: &[McpEventType],
        session_id: Option<&str>,
        turn_seq: Option<u32>,
        harness: Option<&str>,
        source_name: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        unique_fetch_limit: u16,
    ) -> RepoResult<McpSearchPage> {
        let candidate_fetch_size = mcp_candidate_fetch_size(unique_fetch_limit);
        // v1 inlines `search_corpus_stats` into its own ranking statement when
        // the cache is cold, and that is the SAME statement every other path
        // reads its corpus scalars from — one population, one cache slot.
        let corpus_stats = self.cached_corpus_stats().await;
        let scanned_corpus_stats = corpus_stats.is_none();

        let sql = self.build_search_mcp_events_sql(
            terms,
            event_types,
            session_id,
            turn_seq,
            harness,
            source_name,
            min_should_match,
            min_score,
            corpus_stats,
            candidate_fetch_size,
        )?;
        let candidate_rows: Vec<SearchMcpCandidateRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let metadata = candidate_rows
            .iter()
            .find(|row| row.row_kind == 1)
            .ok_or_else(|| RepoError::backend("MCP search candidate query omitted metadata"))?;
        let docs = metadata.docs;
        let total_doc_len = metadata.total_doc_len;
        let scope_exists = metadata.scope_exists != 0;
        if scanned_corpus_stats {
            // What v1 just scanned is `search_corpus_stats`, inlined into its
            // own ranking statement — the document-authorized population, which
            // is the one its inline `df` pairs with.
            self.cache_corpus_stats(docs, total_doc_len, Instant::now())
                .await;
        }
        if metadata.projection_ready == 0 {
            return Err(RepoError::backend(
                "MCP search read model is not ready; run `moraine db migrate`",
            ));
        }
        if metadata.projection_clean == 0 {
            return Err(RepoError::ReadModelChanged);
        }

        let candidates = candidate_rows
            .into_iter()
            .filter(|row| row.row_kind == 0)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(McpSearchPage {
                rows: Vec::new(),
                docs,
                total_doc_len,
                scope_exists,
                incomplete_due_to_candidate_budget: false,
            });
        }
        let saturated = candidates.len() as u32 == candidate_fetch_size;

        let detail_sql = self.build_search_mcp_event_details_sql(&candidates)?;
        let detail_rows: Vec<SearchMcpEventRow> =
            self.map_backend(self.query_rows(&detail_sql, None).await)?;
        let mut details_by_identity = detail_rows
            .into_iter()
            .map(|row| ((row.source_host.clone(), row.event_uid.clone()), row))
            .collect::<HashMap<_, _>>();

        let mut rows = Vec::<SearchMcpEventRow>::with_capacity(candidates.len());
        for candidate in candidates {
            let identity = (candidate.source_host, candidate.event_uid);
            let Some(mut detail) = details_by_identity.remove(&identity) else {
                return Err(RepoError::ReadModelChanged);
            };
            detail.raw_score = candidate.raw_score;
            detail.matched_terms = candidate.matched_terms;
            // Ranking is defined by the candidate snapshot. Preserve its
            // timestamp if the projection publishes between the two reads.
            detail.event_unix_ms = candidate.event_unix_ms;
            rows.push(detail);
        }
        Self::sort_search_mcp_event_rows(&mut rows);
        let rows = Self::dedupe_mcp_search_rows(rows, unique_fetch_limit);

        Ok(McpSearchPage {
            incomplete_due_to_candidate_budget: Self::candidate_budget_incomplete(
                saturated,
                rows.len(),
                unique_fetch_limit,
            ),
            rows,
            docs,
            total_doc_len,
            scope_exists,
        })
    }

    /// The bounded canonical engine (issue #597 §1–§2). Statement budget for a
    /// request: 1 ranking + 1 candidate derivation + 1 dedup keys + 4 winner
    /// hydration, plus 1 for turn or session scope existence and 1 for a cold
    /// corpus-stats refresh — at most 9, against the retired loop's 32 and the
    /// Interactive `statement_cap` of 256.
    ///
    /// Reads no `mcp_open_*` relation and enforces no global projector gate, so
    /// an actively-ingesting session A can no longer disable search for session
    /// B. Candidate validity is proven per row instead — twice: by the locator
    /// version join during ranking, and by the candidate's presence at the same
    /// `event_version` in live navigation during derivation.
    #[allow(clippy::too_many_arguments)]
    async fn search_mcp_event_page_v2(
        &self,
        terms: &[String],
        event_types: &[McpEventType],
        session_id: Option<&str>,
        turn_seq: Option<u32>,
        harness: Option<&str>,
        source_name: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        unique_fetch_limit: u16,
    ) -> RepoResult<McpSearchPage> {
        let (docs, total_doc_len) = self.corpus_stats().await?;
        let empty_page = |scope_exists: bool| McpSearchPage {
            rows: Vec::new(),
            docs,
            total_doc_len,
            scope_exists,
            incomplete_due_to_candidate_budget: false,
        };

        // Phase 0 — scope existence, and the turn's live uid set.
        let mut turn_event_uids: Option<Vec<String>> = None;
        let scope_exists = match (session_id, turn_seq) {
            (Some(session_id), Some(turn_seq)) => {
                let sql = self.build_search_turn_event_uids_sql(session_id, turn_seq);
                let rows: Vec<TurnEventUidRow> =
                    self.map_backend(self.query_rows(&sql, None).await)?;
                if rows.is_empty() {
                    // The turn does not exist. `search_sessions_v1` turns a
                    // false `scope_exists` into `not_found`; a turn that exists
                    // but matches nothing returns true with zero hits.
                    return Ok(empty_page(false));
                }
                // Above the cap the uid literal set is dropped and the turn is
                // re-checked exactly against the derived `turn_seq` after
                // candidate derivation. Correct either way; the fallback only
                // spends candidate budget on out-of-turn events.
                if rows.len() <= MAX_TURN_SCOPE_UIDS {
                    turn_event_uids = Some(
                        rows.into_iter()
                            .map(|row| row.event_uid)
                            .collect::<Vec<_>>(),
                    );
                }
                true
            }
            (Some(session_id), None) => {
                let sql = self.build_search_scope_exists_sql(session_id);
                let rows: Vec<ScopeExistsRow> =
                    self.map_backend(self.query_rows(&sql, None).await)?;
                let exists = rows.first().is_some_and(|row| row.scope_exists != 0);
                if !exists {
                    return Ok(empty_page(false));
                }
                true
            }
            (None, Some(_)) => {
                return Err(RepoError::invalid_argument(
                    "turn-scoped search requires session_id",
                ));
            }
            (None, None) => true,
        };

        // Phase 1 — the single bounded ranking pass.
        let candidate_fetch_size = mcp_candidate_fetch_size(unique_fetch_limit);
        let ranking_sql = self.build_search_ranking_sql(&SearchRankingParams {
            terms,
            event_types,
            session_id,
            turn_event_uids: turn_event_uids.as_deref(),
            harness,
            source_name,
            min_should_match,
            min_score,
            corpus_stats: (docs, total_doc_len),
            limit: candidate_fetch_size,
        })?;
        let candidates: Vec<SearchCandidateRow> =
            self.map_backend(self.query_rows(&ranking_sql, None).await)?;
        if candidates.is_empty() {
            return Ok(empty_page(scope_exists));
        }
        let saturated = candidates.len() as u32 == candidate_fetch_size;

        let mut candidate_session_ids = candidates
            .iter()
            .map(|candidate| candidate.session_id.clone())
            .collect::<Vec<_>>();
        candidate_session_ids.sort_unstable();
        candidate_session_ids.dedup();
        // Every read after ranking is keyed by the SESSION-QUALIFIED identity.
        // `event_uid` is content-addressed over the source coordinates and
        // deliberately excludes `session_id` (#608), so one uid legitimately
        // exists under two sessions; a `(source_host, event_uid)` key silently
        // collapses those two candidates into one and hydrates the survivor
        // against whichever session's row happened to arrive last.
        let candidate_identities = candidates
            .iter()
            .map(|candidate| (candidate.session_id.clone(), candidate.event_uid.clone()))
            .collect::<Vec<_>>();

        // Phase 2 — content-free derivation over the candidate sessions only.
        let derivation_sql = self
            .build_search_candidate_derivation_sql(&candidate_session_ids, &candidate_identities)?;
        let derivations: Vec<SearchCandidateDerivationRow> =
            self.map_backend(self.query_rows(&derivation_sql, None).await)?;
        let mut derivation_by_identity = derivations
            .into_iter()
            .map(|row| {
                (
                    (
                        row.source_host.clone(),
                        row.session_id.clone(),
                        row.event_uid.clone(),
                    ),
                    row,
                )
            })
            .collect::<HashMap<_, _>>();

        // Phase 3 — the two fixed-width dedup inputs. `search_documents` is
        // `ORDER BY (event_uid)`, so a document is per-uid by construction and
        // its digest/phase are the same values for every session the uid is
        // attributed to. The map is therefore keyed per document and READ, not
        // consumed — `remove` would make the lookup order-dependent if a uid
        // ever reached this point more than once. Ranking is document-grained
        // (see `bounded_ranking_ctes`), so today it does not; keeping the read
        // non-consuming means that stays a property of ranking rather than a
        // silent dependency of this code.
        let dedup_sql = self.build_search_dedup_keys_sql(&candidates)?;
        let dedup_rows: Vec<SearchDedupKeyRow> =
            self.map_backend(self.query_rows(&dedup_sql, None).await)?;
        let dedup_by_document = dedup_rows
            .into_iter()
            .map(|row| ((row.source_host.clone(), row.event_uid.clone()), row))
            .collect::<HashMap<_, _>>();

        // Phase 4 — assemble, drop the provably-stale, dedup, trim.
        let scope = self.cfg.session_scope.clone();
        let mut rows = Vec::<SearchMcpEventRow>::with_capacity(candidates.len());
        for candidate in candidates {
            let identity = (
                candidate.source_host.clone(),
                candidate.session_id.clone(),
                candidate.event_uid.clone(),
            );
            // No live navigation row for this SESSION at the candidate's
            // version: the locator authorized it, navigation does not carry it
            // at that version, so it is provably stale. A silent drop, never an
            // error.
            let Some(derived) = derivation_by_identity.remove(&identity) else {
                continue;
            };
            // The second, independent version check. The locator and the
            // navigation index are maintained by two different materialized
            // views from the same `events` insert block; a candidate the
            // locator authorized at `post_version` that navigation carries at a
            // different `event_version` is a proven mid-flight row.
            if derived.event_version != candidate.post_version {
                continue;
            }
            // Project scope is re-checked EXACTLY against the navigation
            // `argMinIf(cwd, …)`; the directory recall filter in ranking is not
            // scope enforcement.
            if let Some(scope) = scope.as_ref() {
                let cwd = derived.origin_cwd.as_str();
                let in_scope = scope
                    .roots
                    .iter()
                    .any(|root| cwd == root.as_str() || cwd.starts_with(&format!("{root}/")));
                if !in_scope {
                    continue;
                }
            }
            // Turn scope above the uid cap: the exact re-check.
            if let Some(turn_seq) = turn_seq {
                if turn_event_uids.is_none() && derived.turn_seq != turn_seq {
                    continue;
                }
            }
            // The dedup-key read is a THIRD version check, against
            // `search_documents` at the candidate's exact `post_version`. Its
            // absence is not "no digest available": it means the document
            // revision that produced this posting is gone, so the posting is
            // stale — drop it.
            //
            // This is also what keeps `mcp_search_rows_are_equivalent` honest.
            // With dedup running BEFORE hydration, `text_content` is empty on
            // every row, so its empty-digest fallback (`a.text_content ==
            // b.text_content`) would report every digest-less pair as identical
            // content and collapse unrelated events. There is no digest-less
            // row to reach it with.
            let Some(dedup) = dedup_by_document
                .get(&(candidate.source_host.clone(), candidate.event_uid.clone()))
            else {
                continue;
            };
            rows.push(Self::canonical_candidate_row(
                candidate,
                derived,
                dedup.clone(),
            ));
        }
        if rows.is_empty() {
            return Ok(McpSearchPage {
                rows,
                docs,
                total_doc_len,
                scope_exists,
                incomplete_due_to_candidate_budget: saturated,
            });
        }
        // NOT `sort_search_mcp_event_rows`: that orders by `event_unix_ms`
        // (display time), which is the v1 ranking key but not the v2 one.
        Self::sort_canonical_search_rows(&mut rows);
        let mut rows = Self::dedupe_mcp_search_rows(rows, unique_fetch_limit);
        let incomplete_due_to_candidate_budget =
            Self::candidate_budget_incomplete(saturated, rows.len(), unique_fetch_limit);

        // Phase 5 — winner-only hydration.
        self.hydrate_canonical_search_winners(&mut rows).await?;

        Ok(McpSearchPage {
            rows,
            docs,
            total_doc_len,
            scope_exists,
            incomplete_due_to_candidate_budget,
        })
    }

    /// `saturated && short` — the exact §1.6 predicate, evaluated once after
    /// dedup. A window that was NOT saturated returned the whole ranking, so a
    /// short result is simply the complete answer and must never set the
    /// marker.
    pub(super) fn candidate_budget_incomplete(
        saturated: bool,
        unique_hits: usize,
        unique_fetch_limit: u16,
    ) -> bool {
        saturated && unique_hits < usize::from(unique_fetch_limit)
    }

    /// The v2 ranking order, restated in Rust over the same keys the ranking
    /// statement's `ORDER BY` uses, so the documented order holds even if the
    /// rows arrive re-ordered.
    ///
    /// The second key is the locator's `sort_time`, NOT the reported
    /// `event_unix_ms` (`display_time`). The two differ for an event whose
    /// `record_ts` does not parse, and the difference is only observable on an
    /// exact `raw_score` tie — where it decides which hit wins.
    pub(super) fn sort_canonical_search_rows(rows: &mut [SearchMcpEventRow]) {
        rows.sort_by(|a, b| {
            b.raw_score
                .total_cmp(&a.raw_score)
                .then_with(|| b.ranking_sort_time_ms.cmp(&a.ranking_sort_time_ms))
                .then_with(|| a.event_uid.cmp(&b.event_uid))
                .then_with(|| a.source_host.cmp(&b.source_host))
        });
    }

    /// Fold one ranked candidate plus its content-free derivation and
    /// fixed-width dedup keys into the row shape the response mapper consumes.
    /// Wide content is filled in later, for winners only.
    fn canonical_candidate_row(
        candidate: SearchCandidateRow,
        derived: SearchCandidateDerivationRow,
        dedup: SearchDedupKeyRow,
    ) -> SearchMcpEventRow {
        let SearchDedupKeyRow {
            text_content_digest,
            payload_phase,
            ..
        } = dedup;
        SearchMcpEventRow {
            event_uid: candidate.event_uid,
            source_host: candidate.source_host,
            session_id: candidate.session_id,
            source_name: candidate.source_name,
            harness: candidate.harness,
            inference_provider: String::new(),
            endpoint_kind: String::new(),
            event_class: candidate.event_class,
            payload_type: candidate.payload_type,
            actor_role: candidate.actor_role,
            name: candidate.name,
            phase: candidate.phase,
            payload_phase,
            // The locator's fixed coordinates, re-rendered in the v1 display
            // form. The dedup rule reads the parsed halves from
            // `SearchMcpEventRow::source_ref`, so keeping one representation
            // keeps `mcp_search_rows_are_codex_final_answer_mirrors` untouched.
            source_ref: format!(
                "{}:{}:{}",
                candidate.source_file, candidate.source_generation, candidate.source_line_no
            ),
            doc_len: candidate.doc_len,
            text_preview: String::new(),
            text_content: String::new(),
            text_content_digest,
            payload_json: String::new(),
            mcp_event_type: String::new(),
            raw_score: candidate.raw_score,
            matched_terms: candidate.matched_terms,
            event_time: derived.display_time,
            event_unix_ms: derived.display_time_ms,
            event_order: derived.event_order,
            turn_seq: derived.turn_seq,
            event_ordinal: derived.event_ordinal,
            turn_event_count: 0,
            turn_completed: 0,
            turn_terminal_event_uid: String::new(),
            call_id: String::new(),
            item_id: String::new(),
            model: String::new(),
            session_started_at_unix_ms: 0,
            session_updated_at_unix_ms: 0,
            session_title: String::new(),
            session_slug: String::new(),
            session_summary: String::new(),
            session_completed: 0,
            // Carried only so hydration can bound the `events` scan by the
            // winners' own `event_ts`; never reported.
            hydration_event_ts_ms: derived.event_ts_ms,
            ranking_sort_time_ms: candidate.sort_time_ms,
        }
    }

    /// Phase 5 (issue #597 §2.5): four batched statements decorate the ≤K
    /// winners. Never a per-winner loop.
    async fn hydrate_canonical_search_winners(
        &self,
        rows: &mut [SearchMcpEventRow],
    ) -> RepoResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut session_ids = rows
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>();
        session_ids.sort_unstable();
        session_ids.dedup();
        // Session-qualified, for the same reason the candidate derivation is:
        // `moraine.events` is `ORDER BY (session_id, …)` and carries one uid
        // under two sessions whenever ingest double-attributes a physical line
        // (#608). A uid-only hydration key hydrates a winner from the other
        // session's row.
        let winner_identities = rows
            .iter()
            .map(|row| (row.session_id.clone(), row.event_uid.clone()))
            .collect::<Vec<_>>();

        let totals_sql = self.build_session_totals_batch_sql(&session_ids);
        let totals: Vec<SessionTotalsBatchRow> =
            self.map_backend(self.query_rows(&totals_sql, None).await)?;
        let totals_by_session = totals
            .into_iter()
            .map(|row| (row.session_id.clone(), row))
            .collect::<HashMap<_, _>>();

        let metadata_sql = self.build_session_metadata_batch_sql(&session_ids);
        let metadata: Vec<SessionMetaBatchRow> =
            self.map_backend(self.query_rows(&metadata_sql, None).await)?;
        let mut metadata_by_session: HashMap<String, Vec<MetaRow>> = HashMap::default();
        for row in metadata {
            metadata_by_session
                .entry(row.session_id)
                .or_default()
                .push(MetaRow {
                    event_ts: row.event_ts,
                    event_uid: row.event_uid,
                    event_kind: row.event_kind,
                    payload_json: row.payload_json,
                });
        }

        // One statement yields BOTH grains. Per-turn rows decorate the hit;
        // the session-level `completed` is `argMax(turn_completed, turn_seq)`
        // over the same rows — v1's two-level rule (`build_session_terminal_sql`
        // and the projector agree on it), which is NOT the hit's own turn.
        // Reporting the hit's turn flag as `session_completed` would call a
        // session complete because the matched turn happened to end in
        // `task_complete`.
        let turns_sql = self.build_search_turn_aggregates_sql(&session_ids);
        let turns: Vec<SearchTurnAggregateRow> =
            self.map_backend(self.query_rows(&turns_sql, None).await)?;
        let mut session_completed = HashMap::<String, (u32, u8)>::new();
        for turn in &turns {
            let entry = session_completed
                .entry(turn.session_id.clone())
                .or_insert((turn.turn_seq, turn.turn_completed));
            if turn.turn_seq >= entry.0 {
                *entry = (turn.turn_seq, turn.turn_completed);
            }
        }
        let turns_by_key = turns
            .into_iter()
            .map(|row| ((row.session_id.clone(), row.turn_seq), row))
            .collect::<HashMap<_, _>>();

        // The wide read's `event_ts` bound is computed from the winners' own
        // canonical timestamps, so it prunes without ever excluding a winner.
        let bounds = Self::search_hydration_bounds(rows);
        let wide_sql =
            self.build_search_wide_hydration_sql(&session_ids, &winner_identities, bounds)?;
        let wide: Vec<SearchWideRow> = self.map_backend(self.query_rows(&wide_sql, None).await)?;
        let mut wide_by_identity = wide
            .into_iter()
            .map(|row| {
                (
                    (
                        row.source_host.clone(),
                        row.session_id.clone(),
                        row.event_uid.clone(),
                    ),
                    row,
                )
            })
            .collect::<HashMap<_, _>>();

        for row in rows.iter_mut() {
            if let Some(totals) = totals_by_session.get(&row.session_id) {
                row.inference_provider = totals.inference_provider.clone();
                row.session_started_at_unix_ms = totals.first_event_unix_ms;
                row.session_updated_at_unix_ms = totals.last_event_unix_ms;
                if row.source_name.is_empty() {
                    row.source_name = totals.source.clone();
                }
                if row.harness.is_empty() {
                    row.harness = totals.harness.clone();
                }
                let mut precedence = metadata_precedence(
                    metadata_by_session
                        .get(&row.session_id)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                );
                precedence.omp_dispatch_title = totals.omp_dispatch_title.clone();
                let (title, summary) = list_title_and_summary(&totals.source, &precedence);
                row.session_title = title;
                row.session_summary = summary;
                row.session_slug = precedence.session_slug.clone();
            }
            if let Some(turn) = turns_by_key.get(&(row.session_id.clone(), row.turn_seq)) {
                row.turn_event_count = turn.turn_event_count;
                row.turn_completed = turn.turn_completed;
                row.turn_terminal_event_uid = turn.turn_terminal_event_uid.clone();
            }
            if let Some((_, completed)) = session_completed.get(&row.session_id) {
                row.session_completed = *completed;
            }
            if let Some(wide) = wide_by_identity.remove(&(
                row.source_host.clone(),
                row.session_id.clone(),
                row.event_uid.clone(),
            )) {
                row.endpoint_kind = wide.endpoint_kind;
                row.call_id = wide.call_id;
                row.item_id = wide.item_id;
                row.model = wide.model;
                row.source_ref = wide.source_ref;
                row.text_preview = wide.text_preview;
                row.text_content = wide.text_content;
                row.payload_json = wide.payload_json;
                if row.inference_provider.is_empty() {
                    row.inference_provider = wide.inference_provider;
                }
            }
        }
        Ok(())
    }

    /// The closed `event_ts` window of the winner rows, plus the epoch branch
    /// when any winner carries the malformed-`record_ts` sentinel.
    fn search_hydration_bounds(rows: &[SearchMcpEventRow]) -> Option<EventTsBounds> {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        let mut include_epoch = false;
        for row in rows {
            let ts = row.hydration_event_ts_ms;
            if ts == 0 {
                include_epoch = true;
                continue;
            }
            min = min.min(ts);
            max = max.max(ts);
        }
        let range = (min <= max).then_some((min, max));
        (range.is_some() || include_epoch).then_some(EventTsBounds {
            range,
            include_epoch,
        })
    }

    pub(super) fn sort_search_mcp_event_rows(rows: &mut [SearchMcpEventRow]) {
        rows.sort_by(|a, b| {
            b.raw_score
                .total_cmp(&a.raw_score)
                .then_with(|| b.event_unix_ms.cmp(&a.event_unix_ms))
                .then_with(|| a.event_uid.cmp(&b.event_uid))
                .then_with(|| a.source_host.cmp(&b.source_host))
        });
    }

    pub(super) fn mcp_search_rows_are_equivalent(
        a: &SearchMcpEventRow,
        b: &SearchMcpEventRow,
    ) -> bool {
        let a_event_type = if a.mcp_event_type.is_empty() {
            Self::mcp_event_type_for(&a.event_class, &a.payload_type, &a.actor_role)
        } else {
            McpEventType::from_normalized(&a.mcp_event_type)
        };
        let b_event_type = if b.mcp_event_type.is_empty() {
            Self::mcp_event_type_for(&b.event_class, &b.payload_type, &b.actor_role)
        } else {
            McpEventType::from_normalized(&b.mcp_event_type)
        };

        let same_content = if a.text_content_digest.is_empty() && b.text_content_digest.is_empty() {
            a.text_content == b.text_content
        } else {
            a.text_content_digest == b.text_content_digest
        };
        let same_logical_coordinates = a.source_host == b.source_host
            && a.session_id == b.session_id
            && a.turn_seq == b.turn_seq
            && a_event_type == b_event_type
            && same_content;

        same_logical_coordinates
            && (a.event_unix_ms == b.event_unix_ms
                || Self::mcp_search_rows_are_codex_final_answer_mirrors(a, b))
    }

    fn mcp_search_rows_are_codex_final_answer_mirrors(
        a: &SearchMcpEventRow,
        b: &SearchMcpEventRow,
    ) -> bool {
        let is_known_representation_pair = matches!(
            (
                (a.event_class.as_str(), a.payload_type.as_str()),
                (b.event_class.as_str(), b.payload_type.as_str()),
            ),
            (("message", "message"), ("event_msg", "agent_message"))
                | (("event_msg", "agent_message"), ("message", "message"))
        );
        let is_final_answer = |row: &SearchMcpEventRow| {
            row.phase == "final_answer" || row.payload_phase == "final_answer"
        };
        let Some((a_source_file, a_generation, a_line)) = Self::parse_mcp_source_ref(&a.source_ref)
        else {
            return false;
        };
        let Some((b_source_file, b_generation, b_line)) = Self::parse_mcp_source_ref(&b.source_ref)
        else {
            return false;
        };

        a.harness == "codex"
            && b.harness == "codex"
            && !a.source_name.is_empty()
            && a.source_name == b.source_name
            && is_known_representation_pair
            && is_final_answer(a)
            && is_final_answer(b)
            && a_source_file == b_source_file
            && a_generation == b_generation
            && a_line.abs_diff(b_line) == 1
            && a.event_order.abs_diff(b.event_order) == 1
            && a.event_unix_ms.abs_diff(b.event_unix_ms)
                <= CODEX_FINAL_ANSWER_MIRROR_MAX_TIMESTAMP_DELTA_MS
    }

    fn parse_mcp_source_ref(source_ref: &str) -> Option<(&str, u64, u64)> {
        let (source_and_generation, source_line) = source_ref.rsplit_once(':')?;
        let (source_file, source_generation) = source_and_generation.rsplit_once(':')?;
        if source_file.is_empty() {
            return None;
        }
        Some((
            source_file,
            source_generation.parse().ok()?,
            source_line.parse().ok()?,
        ))
    }

    pub(super) fn dedupe_mcp_search_rows(
        rows: Vec<SearchMcpEventRow>,
        limit: u16,
    ) -> Vec<SearchMcpEventRow> {
        let target = limit as usize;
        let mut deduped = Vec::<SearchMcpEventRow>::with_capacity(rows.len().min(target));

        for row in rows {
            if deduped
                .iter()
                .any(|existing| Self::mcp_search_rows_are_equivalent(existing, &row))
            {
                continue;
            }

            deduped.push(row);
            if deduped.len() >= target {
                break;
            }
        }

        deduped
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
            || a.source_host != b.source_host
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

    /// The shared bounded ranking relation, with the extra physical posting
    /// column conversation search projects.
    ///
    /// **Issue #597 B6 — one BM25 document population.** Conversation search
    /// used to score `v_live_search_postings` (authorized only through
    /// `search_documents`) while taking its `df` from
    /// [`Self::bounded_term_df_map`], which counts the locator-authorized
    /// `term_postings`. Those are two different document populations in one
    /// score: a posting whose `search_documents` revision is current but whose
    /// live `event_version` has moved on contributed `tf` to the numerator and
    /// nothing to `df`. The population is now stated once and used everywhere:
    ///
    /// > the live document population is `search_documents` rows that are
    /// > published-generation-authorized AND carry the live canonical
    /// > `event_version` — i.e. the `live_locator` join.
    ///
    /// `df` (here and in ranking), the scored postings and the session
    /// aggregation all describe exactly that relation. `docs`/`avgdl`
    /// ([`Self::corpus_stats`]) deliberately do NOT carry the per-event locator
    /// join — see `CorpusStatsCacheEntry` for why (design §2.6/OQ-2), and for
    /// the `greatest(corpus_docs, df)` guard that absorbs the MV-lag window in
    /// which a term's `df` can exceed `docs`.
    fn conversation_ranking_ctes(&self, terms_array_sql: &str) -> String {
        self.bounded_ranking_ctes(terms_array_sql, &["inference_provider"])
    }

    /// Filters for the conversation-search statements, expressed against the
    /// bounded `term_postings` relation (issue #597 B6): ONE document
    /// population feeds `df`, the scored postings and the session aggregation.
    ///
    /// `p.term IN …` is deliberately absent — the shared ranking CTE owns term
    /// membership, and duplicating it here is how the two relations start to
    /// drift.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_conversation_postings_filter_sql(
        &self,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
        recent_from_unix_ms: Option<i64>,
        session_filter: ConversationSessionFilter<'_>,
    ) -> (String, String) {
        let mut postings_filters: Vec<String> = Vec::new();
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
                "NOT {}",
                moraine_clickhouse::mcp_tool_names::sql_predicate("p.name")
            ));
        }

        match session_filter {
            ConversationSessionFilter::Discovery => {}
            ConversationSessionFilter::Sessions(session_ids) => {
                postings_filters.push(format!(
                    "p.session_id IN {}",
                    sql_array_strings(session_ids)
                ));
            }
        }

        // The `ingested_at` window is the one predicate `search_postings` cannot
        // answer. The join does NOT widen the population: postings are derived
        // from `search_documents`, so every row of `term_postings` has exactly
        // one document row, and the join is a lookup rather than a filter on
        // membership.
        let docs_join_sql = if document_filters.is_empty() {
            String::new()
        } else {
            let documents_table = self.table_ref("v_live_search_documents");
            format!(
                "ANY INNER JOIN {documents_table} AS d\n  ON d.source_host = p.source_host\n AND d.event_uid = p.event_uid"
            )
        };
        postings_filters.extend(document_filters);
        if postings_filters.is_empty() {
            postings_filters.push("1".to_string());
        }
        let filter_sql = format!("WHERE {}", postings_filters.join("\n      AND "));
        (docs_join_sql, filter_sql)
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

        let terms_array_sql = sql_array_strings(terms);
        let ranking_ctes = self.conversation_ranking_ctes(&terms_array_sql);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let (docs_join_sql, filter_sql) = self.build_conversation_postings_filter_sql(
            include_tool_events,
            exclude_codex_mcp,
            from_unix_ms,
            to_unix_ms,
            None,
            ConversationSessionFilter::Discovery,
        );

        let (mode_join_sql, mode_filter_sql) = if let Some(selected_mode) = mode {
            // Bounded to the sessions this query already selected. An
            // unpredicated mode aggregate is a whole-corpus
            // `events FINAL … GROUP BY session_id` riding along with a bounded
            // ranking pass (issue #597 B5).
            let mode_subquery =
                self.mode_subquery_for_sessions(Some("SELECT session_id FROM eligible_sessions"));
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
{ranking_ctes},
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf,
  eligible_sessions AS (
    SELECT DISTINCT p.session_id
    FROM term_postings AS p
    {docs_join_sql}
    {filter_sql}
  ),
  session_terms AS (
    SELECT
      p.session_id AS session_id,
      toString(p.term) AS term,
      sum(p.tf) AS tf_sum
    FROM term_postings AS p
    ALL INNER JOIN eligible_sessions AS eligible
      ON eligible.session_id = p.session_id
    GROUP BY p.session_id, p.term
  )
SELECT
  c.session_id AS session_id,
  c.score AS score,
  toUInt16(c.matched_terms) AS matched_terms
FROM (
  SELECT
    terms.session_id,
    sum(transform(terms.term, q_terms, q_idf, 0.0) * log1p(toFloat64(terms.tf_sum))) AS score,
    toUInt16(countDistinct(terms.term)) AS matched_terms
  FROM session_terms AS terms
  GROUP BY terms.session_id
) AS c
{mode_join_sql}
WHERE c.matched_terms >= {min_should_match}
  {mode_filter_sql}
ORDER BY c.score DESC, c.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            docs_join_sql = docs_join_sql,
            filter_sql = filter_sql,
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

        let terms_array_sql = sql_array_strings(terms);
        let ranking_ctes = self.conversation_ranking_ctes(&terms_array_sql);
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
        let (docs_join_sql, filter_sql) = self.build_conversation_postings_filter_sql(
            include_tool_events,
            exclude_codex_mcp,
            from_unix_ms,
            to_unix_ms,
            Some(recent_from_unix_ms),
            ConversationSessionFilter::Discovery,
        );

        let (mode_join_sql, mode_filter_sql) = if let Some(selected_mode) = mode {
            let mode_subquery =
                self.mode_subquery_for_sessions(Some("SELECT session_id FROM eligible_sessions"));
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
{ranking_ctes},
  {terms_array_sql} AS q_terms,
  {idf_array_sql} AS q_idf,
  eligible_sessions AS (
    SELECT DISTINCT p.session_id
    FROM term_postings AS p
    {docs_join_sql}
    {filter_sql}
  )
SELECT
  c.session_id AS session_id,
  c.score AS score,
  toUInt16(c.matched_terms) AS matched_terms
FROM (
  SELECT
    p.session_id AS session_id,
    sum(transform(toString(p.term), q_terms, q_idf, 0.0) * log1p(toFloat64(p.tf))) AS score,
    toUInt16(countDistinct(p.term)) AS matched_terms
  FROM term_postings AS p
  {docs_join_sql}
  {filter_sql}
  GROUP BY p.session_id
) AS c
{mode_join_sql}
WHERE c.matched_terms >= {min_should_match}
  {mode_filter_sql}
ORDER BY c.score DESC, c.session_id ASC
LIMIT {limit}
FORMAT JSONEachRow",
            docs_join_sql = docs_join_sql,
            filter_sql = filter_sql,
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
            // The persistent window saturated: its bounded prefix IS the
            // session set. Merging the recent window on top would not make it
            // more complete, and #597/F5 no longer has an unbounded branch to
            // fall back to.
            return Ok(ConversationCandidateSet {
                rows: persistent_rows,
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

        Ok(ConversationCandidateSet { rows })
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
        candidate_session_ids: &[String],
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }

        let session_summary_table = self.table_ref("v_session_summary");
        let terms_array_sql = sql_array_strings(terms);
        let ranking_ctes = self.conversation_ranking_ctes(&terms_array_sql);
        let idf_vals: Vec<f64> = terms
            .iter()
            .map(|t| *idf_by_term.get(t).unwrap_or(&0.0))
            .collect();
        let idf_array_sql = sql_array_f64(&idf_vals);
        let (docs_join_sql, filter_sql) = self.build_conversation_postings_filter_sql(
            include_tool_events,
            exclude_codex_mcp,
            from_unix_ms,
            to_unix_ms,
            None,
            ConversationSessionFilter::Sessions(candidate_session_ids),
        );
        let (mode_join_sql, mode_filter_sql) = if let Some(selected_mode) = mode {
            let candidate_sessions_sql = format!(
                "SELECT arrayJoin({}) AS session_id",
                sql_array_strings(candidate_session_ids)
            );
            let mode_subquery = self.mode_subquery_for_sessions(Some(&candidate_sessions_sql));
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
{ranking_ctes},
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
  tupleElement(c.best_event_identity, 1) AS best_source_host,
  tupleElement(c.best_event_identity, 2) AS best_event_uid
FROM (
  SELECT
    e.session_id AS session_id,
    sum(e.event_score) AS score,
    {outer_matched_terms_sql} AS matched_terms,
    count() AS event_count_considered,
    argMax(e.harness, e.event_score) AS harness,
    argMax(e.inference_provider, e.event_score) AS inference_provider,
    argMax(
      tuple(e.source_host, e.event_uid),
      tuple(e.event_score, e.event_uid, e.source_host)
    ) AS best_event_identity
  FROM (
    SELECT
      p.source_host AS source_host,
      p.event_uid AS event_uid,
      p.session_id AS session_id,
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
    FROM term_postings AS p
    {docs_join_sql}
    {filter_sql}
    GROUP BY p.event_uid, p.source_host, p.session_id
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
            docs_join_sql = docs_join_sql,
            filter_sql = filter_sql,
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
        document_identities: &[SearchDocumentIdentity],
    ) -> RepoResult<HashMap<SearchDocumentIdentity, ConversationSnippetContent>> {
        if document_identities.is_empty() {
            return Ok(HashMap::new());
        }

        let documents_table = self.table_ref("v_live_search_documents");
        let event_uids = document_identities
            .iter()
            .map(|identity| identity.event_uid.clone())
            .collect::<Vec<_>>();
        let event_uids_sql = sql_array_strings(&event_uids);
        let identities_sql = document_identities
            .iter()
            .map(|identity| {
                format!(
                    "({}, {})",
                    sql_quote(&identity.source_host),
                    sql_quote(&identity.event_uid)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);
        // Truncate inside the aggregation (issue #443) so the GROUP BY state
        // holds bounded strings, not full payload blobs.
        let sql = format!(
            "WITH requested_documents AS (
  SELECT
    tupleElement(identity, 1) AS source_host,
    tupleElement(identity, 2) AS event_uid
  FROM (SELECT arrayJoin([{identities_sql}]) AS identity)
)
SELECT
  source_host,
  event_uid,
  leftUTF8(text_content_raw, {preview}) AS snippet,
  text_content_raw AS text_content,
  payload_json_raw AS payload_json,
  event_class_raw AS event_class,
  actor_role_raw AS actor_role
FROM (
  SELECT
    document.source_host AS source_host,
    document.event_uid AS event_uid,
    any(leftUTF8(document.text_content, {text_content_limit})) AS text_content_raw,
    any(leftUTF8(document.payload_json, {payload_json_limit})) AS payload_json_raw,
    any(document.event_class) AS event_class_raw,
    any(document.actor_role) AS actor_role_raw
  FROM {documents_table} AS document
  ALL INNER JOIN requested_documents AS requested
    ON requested.source_host = document.source_host
   AND requested.event_uid = document.event_uid
  WHERE document.event_uid IN {event_uids_sql}
  GROUP BY document.source_host, document.event_uid
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
        let mut by_identity = HashMap::new();
        for row in rows {
            let is_user_facing = is_user_facing_content_event(&row.event_class, &row.actor_role);
            by_identity.insert(
                SearchDocumentIdentity::new(row.source_host, row.event_uid),
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
        Ok(by_identity)
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

    pub(super) fn map_search_mcp_rows_to_hits(
        rows: Vec<SearchMcpEventRow>,
    ) -> Vec<SearchMcpEventHit> {
        let max_raw_score = rows.iter().map(|row| row.raw_score).fold(0.0_f64, f64::max);

        rows.into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let session_id = row.session_id;
                let event_type = if row.mcp_event_type.is_empty() {
                    Self::mcp_event_type_for(&row.event_class, &row.payload_type, &row.actor_role)
                } else {
                    McpEventType::from_normalized(&row.mcp_event_type)
                };
                let session_started_at_unix_ms =
                    (row.session_started_at_unix_ms != 0).then_some(row.session_started_at_unix_ms);
                let session_updated_at_unix_ms =
                    (row.session_updated_at_unix_ms != 0).then_some(row.session_updated_at_unix_ms);
                let session_title = non_empty_string(row.session_title);
                let session_slug = non_empty_string(row.session_slug);
                let session_summary = non_empty_string(row.session_summary);
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
                    event_time: row.event_time,
                    event_unix_ms: row.event_unix_ms,
                    turn_seq: row.turn_seq,
                    turn_ordinal: row.turn_seq,
                    event_order: row.event_order,
                    event_ordinal: row.event_ordinal,
                    turn_event_count: row.turn_event_count,
                    turn_completed: row.turn_completed != 0,
                    turn_terminal_event_uid: non_empty_string(row.turn_terminal_event_uid),
                    session_started_at_unix_ms,
                    session_updated_at_unix_ms,
                    session_title,
                    session_slug,
                    session_summary,
                    session_completed: row.session_completed != 0,
                    source_name: non_empty_string(row.source_name),
                    harness: non_empty_string(row.harness),
                    inference_provider: non_empty_string(row.inference_provider),
                    event_class: row.event_class,
                    payload_type: row.payload_type,
                    actor_role: row.actor_role,
                    tool_name: non_empty_string(row.name),
                    tool_phase: non_empty_string(row.phase),
                    call_id: non_empty_string(row.call_id),
                    item_id: non_empty_string(row.item_id),
                    model: non_empty_string(row.model),
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
            .collect()
    }

    /// The winner-session `session_meta` fold.
    ///
    /// Bounded INSIDE the live-events derived table: `events` leads its primary
    /// key with `session_id`, and the same predicate one level up sits above
    /// the publication join, where it prunes nothing (issue-598 C2-R0) — i.e. a
    /// corpus-wide scan of every `session_meta` event wearing a bounded-looking
    /// `WHERE` (issue #597 B5). The outer predicate is kept as the exact
    /// filter; the inner one is what makes this a point range.
    pub(super) fn build_conversation_session_metadata_sql(&self, session_ids: &[String]) -> String {
        let events_source = self.live_events_source_sessions_bounded(session_ids, None);
        let session_ids_sql = sql_array_strings(session_ids);
        format!(
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
        )
    }

    pub(super) async fn fetch_conversation_session_metadata(
        &self,
        session_ids: &[String],
    ) -> RepoResult<HashMap<String, ConversationSessionMetadataRow>> {
        if session_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let sql = self.build_conversation_session_metadata_sql(session_ids);

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

        defer_publication_effect(PublicationEffect::SearchTelemetry {
            query_row,
            hit_rows,
        })
        .await;
    }

    pub(super) async fn write_search_log_rows(&self, query_row: Value, hit_rows: Vec<Value>) {
        async fn insert_rows(ch: &ClickHouseClient, query_row: Value, hit_rows: Vec<Value>) {
            if let Err(err) = ch.insert_json_rows("search_query_log", &[query_row]).await {
                warn!("failed to write search_query_log: {}", err);
            }
            if !hit_rows.is_empty() {
                if let Err(err) = ch.insert_json_rows("search_hit_log", &hit_rows).await {
                    warn!("failed to write search_hit_log: {}", err);
                }
            }
        }

        if self.cfg.async_log_writes {
            let ch = self.ch.clone();
            let admin_budget = administrative_query_budget();
            tokio::spawn(async move {
                // Task-locals do not cross tokio::spawn: the request envelope
                // (if any) is not active in here, so the telemetry inserts get
                // their own Administrative-class envelope (amendments A6/A10).
                QueryEnvelope::new("telemetry", QueryClass::Administrative, &admin_budget)
                    .scope(async move { insert_rows(&ch, query_row, hit_rows).await })
                    .await;
            });
        } else if QueryEnvelope::current().is_ok() {
            // Ride the active request envelope: the inserts count against the
            // request's statement cap like any other statement it issues.
            insert_rows(&self.ch, query_row, hit_rows).await;
        } else {
            let admin_budget = administrative_query_budget();
            QueryEnvelope::new("telemetry", QueryClass::Administrative, &admin_budget)
                .scope(insert_rows(&self.ch, query_row, hit_rows))
                .await;
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
        // Issue #597 §1.5/F3: `oracle_exact` was the caller-facing door into the
        // unbounded exact aggregation. It is refused rather than silently
        // downgraded, so a caller that depended on exact-scan semantics learns
        // that it no longer exists instead of quietly getting different results.
        let effective_strategy_hint = query.strategy_hint.unwrap_or_default();
        if effective_strategy_hint == SearchStrategyHint::Exact {
            return Err(RepoError::invalid_argument(
                "oracle_exact was retired in #597; interactive search has no exact-scan path",
            ));
        }

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

        let publication_cache_available = publication_cache_key("").is_some();
        let hits = if bypass_cache || !publication_cache_available {
            let rows = self
                .search_events_rows(
                    &terms,
                    docs,
                    total_doc_len,
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
            let cache_key = publication_cache_key(&Self::search_events_cache_key(
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
            ))
            .expect("publication cache availability was checked above");

            if let Some(cached_hits) = self.search_events_cache_get(&cache_key).await {
                cached_hits
            } else {
                let fresh_rows = self
                    .search_events_rows(
                        &terms,
                        docs,
                        total_doc_len,
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

    /// Event-grained MCP search as CALLERS see it: `n_hits` is clamped to the
    /// caller-facing `[mcp] max_results`, which is the number of RESULT ROWS a
    /// tool response may contain. Readiness is resolved inside, because this
    /// request has no earlier stage that already needed the verdict.
    pub(super) async fn search_mcp_events_impl(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        self.search_mcp_events_ranked(query, McpEventRankingOptions::for_tool_caller(&self.cfg))
            .await
    }

    /// The ranking with the knobs an INTERNAL consumer may set.
    ///
    /// See [`McpEventRankingOptions`] for why each exists. Everything else —
    /// validation, tokenization, the result cache, the truncation report — is
    /// the tool path's, unchanged, so an internal consumer cannot drift into a
    /// second ranking with its own rules.
    pub(super) async fn search_mcp_events_ranked(
        &self,
        query: SearchMcpEventsQuery,
        options: McpEventRankingOptions,
    ) -> RepoResult<SearchMcpEventsResult> {
        let hit_cap = options.hit_cap.max(1);
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

        let query_id = query
            .cancellation_token
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
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
        let effective_n_hits = requested_n_hits.min(hit_cap);
        let limit_capped = requested_n_hits > effective_n_hits;
        let unique_fetch_limit = effective_n_hits.saturating_add(1);

        let min_should_match = query
            .min_should_match
            .unwrap_or(self.cfg.bm25_default_min_should_match)
            .max(1)
            .min(terms.len() as u16);
        let min_score = query.min_score.unwrap_or(self.cfg.bm25_default_min_score);
        // #570 freshness (issue #597 §2.7): a request carrying `session_id`
        // neither READS nor WRITES the result cache. A session-scoped search is
        // by construction a self-review of a session that may be writing right
        // now, and the publication token that namespaces every cache does not
        // move on an append-only tick, so a 15 s entry can pin a pre-append
        // empty answer well past the p95 <= 2 s freshness budget. The previous
        // guard here reasoned only about `scope_exists`/`docs`, which is
        // exactly what let that through. Unscoped traffic — which dominates —
        // keeps its cache-hit rate untouched.
        let cacheable = query.session_id.is_none();
        let cache_key = cacheable
            .then(|| {
                publication_cache_key(&Self::search_mcp_events_cache_key(
                    &terms,
                    &event_types,
                    query.session_id.as_deref(),
                    query.harness.as_deref(),
                    query.source_name.as_deref(),
                    query.turn_seq,
                    min_should_match,
                    min_score,
                    effective_n_hits,
                ))
            })
            .flatten();
        let cached_result = match cache_key.as_deref() {
            Some(cache_key) => self.search_mcp_events_cache_get(cache_key).await,
            None => None,
        };
        let cache_hit = cached_result.is_some();
        tracing::info!(cache_hit, cacheable, "mcp_search_cache");

        let (hits, truncated, docs, avgdl, scope_exists, incomplete) =
            if let Some(cached) = cached_result {
                (
                    cached.hits,
                    cached.truncated,
                    cached.docs,
                    cached.avgdl,
                    true,
                    false,
                )
            } else {
                // Resolved HERE and not one frame down, so that a caller which
                // already needed the verdict for a later stage pays for one
                // point read instead of two — and, more importantly, so one
                // request cannot straddle a mid-request readiness flip by
                // ranking on one read model and hydrating from the other. A
                // cache hit skips this entirely, which is why it is not hoisted
                // above the lookup.
                let canonical_ready = match options.canonical_ready {
                    Some(ready) => ready,
                    None => self.canonical_list_path_ready().await,
                };
                let page = self
                    .search_mcp_event_page(
                        &terms,
                        &event_types,
                        query.session_id.as_deref(),
                        query.turn_seq,
                        query.harness.as_deref(),
                        query.source_name.as_deref(),
                        min_should_match,
                        min_score,
                        unique_fetch_limit,
                        canonical_ready,
                    )
                    .await?;
                let McpSearchPage {
                    mut rows,
                    docs,
                    total_doc_len,
                    scope_exists,
                    incomplete_due_to_candidate_budget,
                } = page;
                let avgdl = if docs == 0 {
                    0.0
                } else {
                    (total_doc_len as f64 / docs as f64).max(1.0)
                };
                let truncated = rows.len() > effective_n_hits as usize;
                if truncated {
                    rows.truncate(effective_n_hits as usize);
                }
                let hits = Self::map_search_mcp_rows_to_hits(rows);
                // Projection publication and first ingest can make a negative or
                // empty answer become positive immediately. Preserve that
                // visibility by caching only stable, published-corpus results —
                // and never cache a result the candidate budget cut short.
                if scope_exists && docs > 0 && !incomplete_due_to_candidate_budget {
                    if let Some(cache_key) = cache_key {
                        self.search_mcp_events_cache_put(cache_key, &hits, truncated, docs, avgdl)
                            .await;
                    }
                }
                (
                    hits,
                    truncated,
                    docs,
                    avgdl,
                    scope_exists,
                    incomplete_due_to_candidate_budget,
                )
            };
        let took_ms = started.elapsed().as_millis() as u32;

        Ok(SearchMcpEventsResult {
            query_id,
            query: query_text.to_string(),
            terms,
            event_types,
            scope_exists,
            truncated,
            incomplete_due_to_candidate_budget: incomplete,
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
        // ONE df formula ships (issue #597 §2.6). The retired `df_map` used
        // `uniqExact(tuple(source_host, doc_id))` over `v_live_search_postings`
        // while ranking used `count()` over the locator-authorized relation;
        // the two agreed only while that join stayed 1:1. Both are now
        // `count()` over the same bounded ranking CTE.
        let df_map = self.bounded_term_df_map(&terms).await?;

        let mut idf_by_term = HashMap::<String, f64>::new();
        for term in &terms {
            let df = *df_map.get(term).unwrap_or(&0);
            idf_by_term.insert(term.clone(), Self::bm25_idf(docs, df));
        }

        // Issue #597 §1.5/F5. A candidate-stage error propagates instead of
        // being downgraded to an unrestricted scan; zero candidates is zero
        // results; and a SATURATED candidate window keeps its bounded prefix as
        // the session predicate rather than dropping the predicate entirely.
        // Saturation is the case that mattered: it fired exactly when the
        // corpus was large enough for the unbounded query to hurt.
        let candidate_set = self
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
            .await?;
        let candidate_session_ids = candidate_set
            .rows
            .into_iter()
            .map(|row| row.session_id)
            .collect::<Vec<_>>();
        if candidate_session_ids.is_empty() {
            return Ok(ConversationSearchResults {
                query_id,
                query: query_text.to_string(),
                terms,
                stats: ConversationSearchStats {
                    docs,
                    avgdl,
                    took_ms: started.elapsed().as_millis() as u32,
                    result_count: 0,
                    requested_limit,
                    effective_limit: limit,
                    limit_capped,
                },
                hits: Vec::new(),
            });
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
            &candidate_session_ids,
        )?;

        let rows: Vec<ConversationSearchRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        let best_event_identities = rows
            .iter()
            .filter_map(|row| {
                if row.best_event_uid.is_empty() {
                    None
                } else {
                    Some(SearchDocumentIdentity::new(
                        row.best_source_host.clone(),
                        row.best_event_uid.clone(),
                    ))
                }
            })
            .collect::<Vec<_>>();
        let snippet_by_identity = self
            .fetch_conversation_snippets(&best_event_identities)
            .await?;
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
                    best_source_host,
                    best_event_uid: row_best_event_uid,
                    snippet: row_snippet,
                } = row;
                let session_metadata = session_metadata_by_session_id.get(&session_id);

                let best_event_uid = if row_best_event_uid.is_empty() {
                    None
                } else {
                    Some(row_best_event_uid)
                };
                let snippet_content = best_event_uid.as_ref().and_then(|event_uid| {
                    snippet_by_identity
                        .get(&SearchDocumentIdentity::new(best_source_host, event_uid))
                        .cloned()
                });
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
