//! Bounded canonical search (issue #597).
//!
//! Every statement the v2 MCP search path issues lives here. The contract the
//! whole issue turns on:
//!
//! 1. **One bounded ranking pass.** `search_postings` is scanned by
//!    `term IN q_terms` (its primary key) and authorized per-row by joining
//!    migration-036's `mcp_event_locator` on
//!    `(doc_id = event_uid, source_host, post_version = event_version)` under
//!    #602's pinned published generations — *before* `df` or BM25 are computed.
//!    No `search_documents`, no `mcp_open_*`, no `events` scan participates in
//!    candidate selection.
//!
//!    Ranking is DOCUMENT-grained: every relation it touches
//!    (`search_documents`, `search_postings`, `mcp_event_locator`) is keyed on
//!    `(event_uid, source_host)`, and `event_uid` is content-addressed over the
//!    physical line. See [`ClickHouseConversationRepository::bounded_ranking_ctes`].
//! 2. **Winner-only hydration.** Everything after ranking is keyed by the
//!    bounded candidate set (≤ [`MCP_SEARCH_CANDIDATE_MAX`] rows) or by the
//!    winner sessions, never by the corpus.
//! 3. **No projector.** Nothing here reads `mcp_open_projection_state`,
//!    `mcp_open_dirty_sessions`, `mcp_open_publication_headers`,
//!    `mcp_open_sessions`, `mcp_open_turns`, or `mcp_open_events`, so activity
//!    in session A can never disable search in session B. The shape tests
//!    assert the absence mechanically (`assert_no_projection`).
//!
//! Every builder is a pure method so the SQL-shape tests can assert without a
//! backend — but a shape test cannot catch an outer query referencing
//! `alias.column` that an inner derived table fails to project (ClickHouse
//! resolves that only at execution time, and this repository has shipped that
//! defect twice). The live execution gates in
//! `tests/live_clickhouse/bounded_search.rs` are the other half.

use super::consistency::EventTsBounds;
use super::*;

/// Candidates one ranking pass over-fetches for a page of `k` unique hits.
///
/// `3×` mirrors [`ClickHouseConversationRepository::dedupe_fetch_limit`], the
/// sibling event-search policy, and covers the #565 codex mirror collapse
/// (2→1), locator version-drift drops, and exact scope re-check drops in a
/// single pass. There is deliberately no second pass: the retired refill loop
/// re-executed every corpus-sized preamble up to 16 times per request.
pub(super) const MCP_SEARCH_CANDIDATE_MULTIPLIER: u32 = 3;
/// Hard ceiling on one ranking pass's candidate window.
///
/// Two callers set `n_hits`, and each derives a bounded budget, so
/// `C ∈ [2, 228]` in practice and this constant guards against a RAISED cap
/// rather than being a number requests land on:
///
/// * the MCP event search tool, whose contract validates `n_hits ∈ 1..=50` and
///   which additionally clamps to `[mcp] max_results`. Worst case
///   `C = 3 × (50 + 1) = 153`;
/// * session discovery by content, whose INTERNAL hit budget is deliberately
///   NOT clamped to `max_results` — a consumer that folds many hits into one
///   result row is not bounded by the rows a caller may receive. It is bounded
///   instead by [`super::list::session_search_hit_budget`], which caps the
///   budget at 75 over its whole reachable `limit` domain (`1..=50`, clamped in
///   the repository, not by a route). Worst case `C = 3 × (75 + 1) = 228`, at
///   the largest page a caller may ask for; the shipped default (`limit = 25`)
///   derives 153.
///
/// The second caller is why "further capped by `max_results`" no longer holds
/// as the reason. Do not restore that wording: out-fetching `max_results` is
/// the point of that path, and the second bullet is the bound that replaced it.
/// Without an explicit budget ceiling there, the shipped default shape
/// (`max_results = 25`, a client asking for 25) derives `C = 256` and pins this
/// constant on every interactive search.
pub(super) const MCP_SEARCH_CANDIDATE_MAX: u32 = 256;
/// Turn-scoped ranking inlines the turn's live event uids as a literal `IN`
/// set. Above this many events in one turn the request falls back to
/// session-scoped recall plus the exact turn re-check, which is correct but
/// spends candidate budget on out-of-turn events.
pub(super) const MAX_TURN_SCOPE_UIDS: usize = 4096;

/// The bounded candidate window for a page of `unique_fetch_limit` unique hits.
pub(super) fn mcp_candidate_fetch_size(unique_fetch_limit: u16) -> u32 {
    let floor = u32::from(unique_fetch_limit).max(1);
    floor
        .saturating_mul(MCP_SEARCH_CANDIDATE_MULTIPLIER)
        .min(MCP_SEARCH_CANDIDATE_MAX)
        .max(floor)
}

/// Everything the ranking statement needs that is not a repository config
/// value. Grouped so the builder keeps one argument and the call site reads as
/// a filter list.
pub(super) struct SearchRankingParams<'a> {
    pub(super) terms: &'a [String],
    pub(super) event_types: &'a [McpEventType],
    pub(super) session_id: Option<&'a str>,
    /// The turn's live event uids, derived by
    /// [`ClickHouseConversationRepository::build_search_turn_event_uids_sql`].
    /// `None` when the request is not turn-scoped or the uid set exceeded
    /// [`MAX_TURN_SCOPE_UIDS`] (in which case the turn is re-checked exactly
    /// after candidate derivation).
    pub(super) turn_event_uids: Option<&'a [String]>,
    pub(super) harness: Option<&'a str>,
    pub(super) source_name: Option<&'a str>,
    pub(super) min_should_match: u16,
    pub(super) min_score: f64,
    /// `(docs, total_doc_len)` — always passed in, never inlined as a view
    /// read, so the ranking statement cannot grow a corpus aggregate.
    pub(super) corpus_stats: (u64, u64),
    pub(super) limit: u32,
}

impl ClickHouseConversationRepository {
    // -----------------------------------------------------------------------
    // Phase 0 — scope existence and turn-scope uid derivation.
    // -----------------------------------------------------------------------

    /// Session-scope existence as a `mcp_session_directory` point read.
    ///
    /// The directory's primary key leads with `session_id` (`sql/036:49`) and
    /// the published-generation filter is a tuple-`IN`, never a join, so this
    /// is a PK-pruned point range. It replaces the v1 `scope_state_sql`
    /// subquery over the projected publication headers (which dragged the O(E)
    /// `current_sources` cityHash behind it) and, for turn scope, the
    /// `mcp_open_turns FINAL` join.
    ///
    /// No `FINAL`: `mcp_session_directory` is an `AggregatingMergeTree` and
    /// existence does not depend on merge state.
    ///
    /// **The configured project scope is part of existence, not a result
    /// filter.** v1's `scope_state_sql` filtered its `authorized_sessions` read
    /// by `origin_cwd` (`search.rs`, `projected_origin_clause("scope_s")`); a
    /// scoped caller asking about a session outside `cfg.session_scope` got
    /// `scope_exists = 0` and therefore `not_found`, which is the same answer a
    /// session id that does not exist at all produces. Dropping the predicate
    /// turns that into `scope_exists = 1` plus zero hits — a different wire
    /// answer, and one that discloses the existence of a session the caller is
    /// not scoped to see.
    pub(super) fn build_search_scope_exists_sql(&self, session_id: &str) -> String {
        let directory = self.table_ref("mcp_session_directory");
        let published = self.published_generations_subquery();
        let sid = sql_quote(session_id);
        let Some(roots) = self.scope_root_predicate("scoped.origin_cwd") else {
            return format!(
                "SELECT toUInt8(count() > 0) AS scope_exists\nFROM {directory} AS d\nWHERE d.session_id = {sid}\n  AND (d.source_host, d.source_name, d.source_file, d.source_generation) IN {published}\nFORMAT JSONEachRow",
            );
        };
        // `origin_cwd` is `argMinIfMerge(origin_cwd_state)` — an aggregate over
        // the session's directory rows — so the grouping relation must project
        // it and the root predicate belongs one level up (the same shape the
        // recall subquery uses, and the same execution-time failure mode if it
        // is collapsed).
        format!(
            "SELECT toUInt8(count() > 0) AS scope_exists\nFROM (\n  SELECT\n    d.session_id AS session_id,\n    argMinIfMerge(d.origin_cwd_state) AS origin_cwd\n  FROM {directory} AS d\n  WHERE d.session_id = {sid}\n    AND (d.source_host, d.source_name, d.source_file, d.source_generation) IN {published}\n  GROUP BY d.session_id\n) AS scoped\nWHERE ({roots})\nFORMAT JSONEachRow",
        )
    }

    /// The live event uids of one derived turn (spec §2: "first derive the
    /// allowed live event UIDs from the one requested session, then constrain
    /// postings to that set").
    ///
    /// `search_postings` carries no `turn_seq` and cannot: `turn_seq` is a
    /// *derived running* quantity (`greatest(1, running user-message count)`
    /// unless overridden by `turn_index`), not a per-event scalar. This applies
    /// the identical windowed rule as
    /// [`Self::build_session_terminal_sql`] over the session's navigation rows,
    /// so turn membership cannot drift from `open`.
    ///
    /// An empty result means the turn does not exist (`scope_exists = false`),
    /// which `search_sessions_v1` turns into `not_found`. Content-free.
    ///
    /// The configured project scope gates this statement too, and for the same
    /// reason it gates [`Self::build_search_scope_exists_sql`]: a turn-scoped
    /// request is the OTHER door into `scope_exists`, and v1 applied
    /// `projected_origin_clause` to its turn branch as well. The gate here is
    /// the EXACT origin `cwd` — `argMinIf(n.cwd, …)` over the same session's
    /// navigation rows, which is the identical authority the Phase 4 per-hit
    /// re-check uses — so turn existence can never disagree with hit
    /// visibility. It costs no extra statement and no extra table: the session
    /// is already the one being scanned.
    pub(super) fn build_search_turn_event_uids_sql(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let sid = sql_quote(session_id);
        let (scope_with_sql, scope_where_sql) = match self.scope_root_predicate("session_origin_cwd")
        {
            Some(roots) => (
                format!(
                    "WITH (\n  SELECT ifNull(argMinIf(n.cwd, tuple(n.event_ts, n.event_uid), n.cwd != ''), '')\n  {from}\n  WHERE n.session_id = {sid}\n) AS session_origin_cwd\n"
                ),
                format!("\n  AND ({roots})"),
            ),
            None => (String::new(), String::new()),
        };
        format!(
            "{scope_with_sql}SELECT event_uid\nFROM (\n  SELECT\n    n.event_uid AS event_uid,\n    if(toUInt32(n.turn_index) > 0, toUInt32(n.turn_index), greatest(toUInt32(1), toUInt32(sum(if(n.is_user_message = 1, 1, 0)) OVER turn_window))) AS turn_seq\n  {from}\n  WHERE n.session_id = {sid}\n  WINDOW turn_window AS (ORDER BY {tuple} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n)\nWHERE turn_seq = {turn_seq}{scope_where_sql}\nLIMIT {cap}\nFORMAT JSONEachRow",
            cap = MAX_TURN_SCOPE_UIDS + 1,
        )
    }

    // -----------------------------------------------------------------------
    // Phase 1 — ranking.
    // -----------------------------------------------------------------------

    /// The two CTEs every bounded ranking statement is built on — `live_locator`
    /// and `term_postings` — emitted as a `WITH` fragment so the MCP and event
    /// search projections share ONE ranking relation and ONE `df` formula.
    ///
    /// `extra_posting_columns` are additional physical `search_postings` columns
    /// a caller's projection needs. They are deliberately opt-in: every column
    /// listed here is decompressed for the whole term scan, so the MCP path
    /// (which takes its wide fields from bounded winner hydration) passes none.
    ///
    /// Nothing about the caller's filters enters this fragment. `df` is
    /// `count() OVER (PARTITION BY p.term)` over the version- and
    /// generation-authorized postings, so it is corpus-wide by construction; a
    /// user predicate pushed in here — into `term_postings` OR into
    /// `live_locator`, which feeds it — would silently move every BM25 score.
    ///
    /// # Ranking is DOCUMENT-grained, by design (issue #597 C1/D1, #608)
    ///
    /// `event_uid` is content-addressed over
    /// `source_file|source_generation|source_line_no|source_offset|
    /// record_fingerprint` and deliberately EXCLUDES `session_id`
    /// (`moraine-ingest-core`, `sources/shared.rs`). A physical line that
    /// ingest attributed to two sessions is therefore ONE uid under two session
    /// ids — 19,846 of them on the reference host (#608) — and, decisively, it
    /// is ONE DOCUMENT: one file, one generation, one line, one byte range, one
    /// fingerprint. BM25 scores documents; `df` and `docs` are document counts.
    ///
    /// The whole read model says the same thing physically. `search_documents`
    /// is `ReplacingMergeTree(doc_version) ORDER BY (event_uid, source_host)`
    /// (`sql/004`, key widened by `sql/032`); `search_postings` is
    /// `ReplacingMergeTree(post_version) ORDER BY (term, doc_id, source_host)`;
    /// `mcp_event_locator` is `ReplacingMergeTree(event_version) ORDER BY
    /// (event_uid, source_host)`. None of the three has `session_id` in its
    /// sort key, so none of them can DURABLY hold two attributions of one uid:
    /// `ReplacingMergeTree` deduplicates on the sort key at storage time,
    /// during background merges, not only when a query says `FINAL`.
    /// `mv_search_postings` does group by `session_id` (`sql/032`) — but it is
    /// fed by `search_documents`, which is already document-grained, and the
    /// rows it writes share a sort key, so a merge destroys one. Two rows are a
    /// transient artifact of unmerged parts, never a state a query may be built
    /// on: a statement whose result cardinality depends on merge scheduling
    /// returns non-repeatable search results.
    ///
    /// `FINAL` is therefore correct here, and the version join closes even the
    /// transient window: `live_locator` reads `mcp_event_locator FINAL`, one row
    /// per `(event_uid, source_host)` carrying `max(event_version)`, and the
    /// `ALL INNER JOIN … AND l.event_version = p.post_version` drops every
    /// posting revision that is not the live one. `event_version` is wall-clock
    /// millis at emit time and a double attribution is ingested seconds apart,
    /// so the losing attribution is dropped by the join whether or not a merge
    /// has run.
    ///
    /// Consequence, stated rather than hidden: a search scoped to the LOSING
    /// session of a double-attributed uid does not return that uid. That is the
    /// shipping behaviour on a real server, it is ~1% of the reference corpus,
    /// and it is an INGEST defect owned by #608 — one of the two session ids is
    /// simply wrong (the rollout filename names the right one in 100% of the
    /// 19,846 observed cases). The read model must not mirror the corruption:
    /// making ranking attribution-grained would fork `df`/`docs` into two
    /// corpora over one physical line, and would let search resolve a uid to a
    /// session that the `open_v2` exact-event seek — which reads the same
    /// `mcp_event_locator` row — resolves differently, so a user following the
    /// losing hit would land somewhere else.
    ///
    /// Widening `search_postings`' own `ORDER BY` is not the fix either, and
    /// not only because it is expensive: `search_documents` and
    /// `mcp_event_locator` share the same uid-grained key, so all three would
    /// have to be widened and "the live revision of a document" redefined
    /// per-attribution — i.e. one physical line declared to be two documents,
    /// contradicting the content-addressed uid. See the ledger entry in
    /// `plans/597-open-defects.md` (D2).
    ///
    /// # `session_id` is the POSTING's own physical column, never the locator's
    ///
    /// Design §1.2, and v1 parity (v1 joined the projected event on
    /// `e.session_id = p.session_id`). The locator supplies version authority
    /// and the fixed source coordinates only — identical for both attributions,
    /// because they are exactly the fields the uid is addressed over — and it
    /// projects no session at all, so there is none to take by accident. The
    /// session a hit reports is the one the surviving posting carries, which is
    /// the same revision the locator authorized.
    pub(super) fn bounded_ranking_ctes(
        &self,
        terms_array_sql: &str,
        extra_posting_columns: &[&str],
    ) -> String {
        let postings = self.table_ref("search_postings");
        let locator = self.table_ref("mcp_event_locator");
        let published = self.published_generations_subquery();
        let extras = extra_posting_columns
            .iter()
            .map(|column| format!("      p.{column} AS {column},\n"))
            .collect::<String>();
        format!(
            "  live_locator AS (
    SELECT
      l.event_uid AS event_uid,
      l.source_host AS source_host,
      l.event_version AS event_version,
      l.source_file AS source_file,
      l.source_generation AS source_generation,
      l.source_line_no AS source_line_no,
      l.sort_time AS sort_time
    FROM {locator} AS l FINAL
    WHERE l.event_uid IN (
      SELECT pruned.doc_id
      FROM {postings} AS pruned
      WHERE pruned.term IN {terms_array_sql}
    )
      AND (l.source_host, l.source_name, l.source_file, l.source_generation) IN {published}
  ),
  term_postings AS (
    SELECT
      p.term AS term,
      p.doc_id AS event_uid,
      p.source_host AS source_host,
      p.tf AS tf,
      p.doc_len AS doc_len,
      p.harness AS harness,
      p.source_name AS source_name,
      p.event_class AS event_class,
      p.payload_type AS payload_type,
      p.actor_role AS actor_role,
      p.name AS name,
      p.phase AS phase,
{extras}      p.session_id AS session_id,
      l.event_version AS event_version,
      l.source_file AS source_file,
      l.source_generation AS source_generation,
      l.source_line_no AS source_line_no,
      l.sort_time AS sort_time,
      toUInt64(count() OVER (PARTITION BY p.term)) AS df
    FROM {postings} AS p FINAL
    ALL INNER JOIN live_locator AS l
      ON l.event_uid = p.doc_id
     AND l.source_host = p.source_host
     AND l.event_version = p.post_version
    WHERE p.term IN {terms_array_sql}
  )"
        )
    }

    /// Per-term `df` for the callers that cannot inline the ranking window —
    /// today only conversation search, whose scoring statement aggregates by
    /// session and takes `idf` as an array parameter.
    ///
    /// `count()` grouped by term over `term_postings` is BY CONSTRUCTION the
    /// same value as ranking's `count() OVER (PARTITION BY p.term)` on the same
    /// relation: same rows, same partition, same counting function. That
    /// identity is the point. The retired `df_map` used a DIFFERENT formula
    /// (`uniqExact(tuple(source_host, doc_id))`) over a DIFFERENT relation
    /// (`v_live_search_postings`, authorized only through `search_documents`),
    /// so the two could diverge silently — and a locator join changes exactly
    /// the join cardinality that made them agree.
    ///
    /// `count()` is an exact DOCUMENT count here, not an approximation of one:
    /// `FROM search_postings FINAL` plus the `live_locator` equi-join leaves at
    /// most one `term_postings` row per `(term, event_uid, source_host)` (see
    /// [`Self::bounded_ranking_ctes`]), so `count()` and
    /// `uniqExact(tuple(event_uid, source_host))` are provably equal on this
    /// relation — and `count()` is an O(1) per-partition accumulator instead of
    /// an exact hash set sized to the term's `df`, on the interactive path,
    /// under this statement's own 64 MiB external-group-by threshold. That
    /// equality is an INVARIANT of the fragment, pinned by
    /// `df_counts_documents_because_final_and_the_version_join_make_it_exact`.
    pub(super) fn build_term_df_sql(&self, terms: &[String]) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot read document frequency for empty terms",
            ));
        }
        let terms_array_sql = sql_array_strings(terms);
        let ranking_ctes = self.bounded_ranking_ctes(&terms_array_sql, &[]);
        Ok(format!(
            "WITH
{ranking_ctes}
SELECT
  toString(p.term) AS term,
  toUInt64(count()) AS df
FROM term_postings AS p
GROUP BY p.term
FORMAT JSONEachRow"
        ))
    }

    /// [`Self::build_term_df_sql`], executed, with every requested term present
    /// (a term no live document contains has `df = 0`, not a missing key).
    pub(super) async fn bounded_term_df_map(
        &self,
        terms: &[String],
    ) -> RepoResult<HashMap<String, u64>> {
        let sql = self.build_term_df_sql(terms)?;
        let rows: Vec<TermDfRow> = self.map_backend(self.query_rows(&sql, None).await)?;
        let mut map = rows
            .into_iter()
            .map(|row| (row.term, row.df))
            .collect::<HashMap<_, _>>();
        for term in terms {
            map.entry(term.clone()).or_insert(0);
        }
        Ok(map)
    }

    /// The BM25 term contribution, shared by both ranking projections so one
    /// scorer ships. `df` is the CTE's corpus-wide window value; `corpus_docs`,
    /// `avgdl`, `k1` and `b` are `WITH` scalars the caller declares.
    fn bm25_sum_expression() -> &'static str {
        "sum(
    log(1.0 + ((greatest(toFloat64(corpus_docs), toFloat64(p.df))
      - toFloat64(p.df) + 0.5) / (toFloat64(p.df) + 0.5)))
    * ((toFloat64(p.tf) * (k1 + 1.0))
      / (toFloat64(p.tf) + k1 * (1.0 - b + b * (toFloat64(p.doc_len) / avgdl))))
  )"
    }

    /// The bounded ranking pass for `search_events` (WI-06).
    ///
    /// Same relation, same `df`, same BM25 expression as the MCP path — the
    /// deleted alternative was an in-process scorer fed by an unbounded
    /// `SELECT … FROM v_live_search_postings WHERE term IN (…)` with no `LIMIT`,
    /// which bailed out on any broad term into a full `search_documents`
    /// aggregation carrying `any(text_content)` / `any(payload_json)`.
    ///
    /// Content is NOT read here. `exclude_codex_mcp` is applied as a
    /// fixed-width RECALL predicate (`p.source_name != 'codex-mcp'` plus the
    /// internal-tool-name rule); the exact `has_codex_mcp` payload flag is
    /// re-checked in Rust against the bounded winner hydration, so the deleted
    /// `positionCaseInsensitiveUTF8(payload_json, 'codex-mcp')` corpus scan has
    /// no live caller.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_search_events_ranking_sql(
        &self,
        terms: &[String],
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        corpus_stats: (u64, u64),
        limit: u32,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }
        let terms_array_sql = sql_array_strings(terms);
        let ranking_ctes = self.bounded_ranking_ctes(&terms_array_sql, &[]);
        let (docs, total_doc_len) = corpus_stats;
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);

        let mut where_clauses = Vec::<String>::new();
        if let Some(session_id) = session_id {
            where_clauses.push(format!("p.session_id = {}", sql_quote(session_id)));
        }
        if let Some(session_ids) = session_ids.filter(|ids| !ids.is_empty()) {
            where_clauses.push(format!(
                "p.session_id IN {}",
                sql_array_strings(session_ids)
            ));
        }
        if let Some(event_kinds) = event_kinds {
            where_clauses.push(Self::event_kind_filter_clause(
                "p.event_class",
                "p.payload_type",
                event_kinds,
            ));
        } else if include_tool_events {
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
            where_clauses.push("p.source_name != 'codex-mcp'".to_string());
            where_clauses.push(format!(
                "NOT {}",
                moraine_clickhouse::mcp_tool_names::sql_predicate("p.name")
            ));
        }
        if where_clauses.is_empty() {
            where_clauses.push("1".to_string());
        }
        let where_sql = where_clauses.join("\n  AND ");
        let bm25 = Self::bm25_sum_expression();

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  toUInt64({docs}) AS corpus_docs,
  toUInt64({total_doc_len}) AS corpus_total_doc_len,
  greatest(
    if(corpus_docs = 0, 1.0, toFloat64(corpus_total_doc_len) / toFloat64(corpus_docs)),
    1.0
  ) AS avgdl,
{ranking_ctes}
SELECT
  p.event_uid AS event_uid,
  p.source_host AS source_host,
  {bm25} AS score,
  toUInt64(count()) AS matched_terms
FROM term_postings AS p
WHERE {where_sql}
GROUP BY p.event_uid, p.source_host
HAVING matched_terms >= {min_should_match} AND score >= {min_score:.6}
ORDER BY score DESC, event_uid ASC, source_host ASC
LIMIT {limit}
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864
FORMAT JSONEachRow"
        ))
    }

    /// The one bounded ranking pass (§1.1/§1.2).
    ///
    /// Load-bearing properties, each pinned by a named shape test:
    ///
    /// * **`p.term IN q_terms` is the only predicate on the postings scan.**
    ///   `search_postings` is `ORDER BY (term, doc_id, source_host)`
    ///   `PARTITION BY cityHash64(term) % 32`, so this prunes by primary key.
    ///   That is the `O(P_q)` term.
    /// * **`df` is computed after the locator join and before every user
    ///   filter, and must stay corpus-wide.** No session / turn / harness /
    ///   source / kind / scope predicate may move into `term_postings`: doing
    ///   so silently moves every BM25 score. It is a hard constraint, not a
    ///   preference (`ranking_does_not_push_user_filters_into_the_df_cte`).
    /// * **The locator scan is pruned by the query's own posting doc ids.**
    ///   `mcp_event_locator` is one row per EVENT; an unpredicated scan of it
    ///   would trade one O(E) canonical scan for another O(E) index scan and
    ///   fail the flatness gate at the ranking phase. Its primary key leads
    ///   with `event_uid`, so the `IN` over the term-pruned posting doc ids is
    ///   the prunable form.
    /// * **Published generations are a tuple-`IN`, never `ALL INNER JOIN`,** on
    ///   that scan: a join defeats `KeyCondition` pruning (#599's finding,
    ///   `canonical_list.rs`).
    /// * **`FINAL` on `search_postings` is retained** (RMT(`post_version`)); the
    ///   locator join then drops any surviving non-current version, because
    ///   `l.event_version` is the live max maintained directly from `events`.
    ///   Together those two make `term_postings` at most one row per
    ///   `(term, event_uid, source_host)`, which is what makes `df`'s `count()`
    ///   an exact document count (C1/D1; see [`Self::bounded_ranking_ctes`]).
    ///
    /// Why the locator join and not a `search_documents` version join: a
    /// document row whose `doc_version` no longer matches the *live event*
    /// version — an MV gap, an interrupted backfill, a partially applied insert
    /// block — is invisible to `search_documents FINAL`, which is
    /// self-consistent and will serve the stale pair as current indefinitely.
    /// Only a relation maintained directly from `events` can detect it.
    pub(super) fn build_search_ranking_sql(
        &self,
        params: &SearchRankingParams<'_>,
    ) -> RepoResult<String> {
        if params.terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }
        if params.event_types.is_empty() {
            return Err(RepoError::invalid_argument(
                "event_types filter cannot be an empty list",
            ));
        }

        let terms_array_sql = sql_array_strings(params.terms);
        let ranking_ctes = self.bounded_ranking_ctes(&terms_array_sql, &[]);

        let (docs, total_doc_len) = params.corpus_stats;
        let k1 = self.cfg.bm25_k1.max(0.01);
        let b = self.cfg.bm25_b.clamp(0.0, 1.0);

        // Every user filter lives here — BELOW the `df` window, over a relation
        // that is already version- and generation-authorized.
        let mut where_clauses = Vec::<String>::new();
        if let Some(session_id) = params.session_id {
            where_clauses.push(format!("p.session_id = {}", sql_quote(session_id)));
        }
        if let Some(uids) = params.turn_event_uids {
            where_clauses.push(format!("p.event_uid IN {}", sql_array_strings(uids)));
        }
        if let Some(harness) = params.harness {
            where_clauses.push(format!("p.harness = {}", sql_quote(harness)));
        }
        if let Some(source_name) = params.source_name {
            where_clauses.push(format!("p.source_name = {}", sql_quote(source_name)));
        }
        where_clauses.push("p.source_name != 'codex-mcp'".to_string());
        where_clauses.push(format!(
            "NOT {}",
            moraine_clickhouse::mcp_tool_names::sql_predicate("p.name")
        ));
        where_clauses.push(Self::mcp_event_type_filter_clause(
            "p.event_class",
            "p.payload_type",
            "p.actor_role",
            params.event_types,
        ));
        // Project scope RECALL only. The exact re-check runs in Rust against
        // the navigation `argMinIf(cwd, …)` the candidate-derivation statement
        // projects — scope decides what a caller may see, so it does not rest
        // on a recall filter (#599's verdict, inherited).
        if let Some(scope_sql) = self.search_scope_recall_subquery() {
            where_clauses.push(format!("p.session_id IN {scope_sql}"));
        }
        let where_sql = where_clauses.join("\n  AND ");

        Ok(format!(
            "WITH
  {k1:.6} AS k1,
  {b:.6} AS b,
  toUInt64({docs}) AS corpus_docs,
  toUInt64({total_doc_len}) AS corpus_total_doc_len,
  greatest(
    if(corpus_docs = 0, 1.0, toFloat64(corpus_total_doc_len) / toFloat64(corpus_docs)),
    1.0
  ) AS avgdl,
{ranking_ctes}
SELECT
  p.event_uid AS event_uid,
  p.source_host AS source_host,
  p.session_id AS session_id,
  toUInt64(any(p.event_version)) AS post_version,
  any(p.source_file) AS source_file,
  toUInt32(any(p.source_generation)) AS source_generation,
  toUInt64(any(p.source_line_no)) AS source_line_no,
  toInt64(toUnixTimestamp64Milli(any(p.sort_time))) AS sort_time_ms,
  any(p.harness) AS harness,
  any(p.source_name) AS source_name,
  any(p.event_class) AS event_class,
  any(p.payload_type) AS payload_type,
  any(p.actor_role) AS actor_role,
  any(p.name) AS name,
  any(p.phase) AS phase,
  toUInt32(any(p.doc_len)) AS doc_len,
  {bm25} AS raw_score,
  toUInt64(count()) AS matched_terms
FROM term_postings AS p
WHERE {where_sql}
GROUP BY p.event_uid, p.source_host, p.session_id
HAVING matched_terms >= {min_should_match} AND raw_score >= {min_score:.6}
ORDER BY raw_score DESC, sort_time_ms DESC, event_uid ASC, source_host ASC
LIMIT {limit}
SETTINGS max_bytes_before_external_group_by = 67108864,
  max_bytes_before_external_sort = 67108864
FORMAT JSONEachRow",
            bm25 = Self::bm25_sum_expression(),
            min_should_match = params.min_should_match,
            min_score = params.min_score,
            limit = params.limit,
        ))
    }

    /// The configured project scope as an `OR`ed root predicate over `column`,
    /// or `None` when no scope is configured. One authority, so a scope check
    /// cannot be spelled two different ways in two statements.
    pub(super) fn scope_root_predicate(&self, column: &str) -> Option<String> {
        let scope = self.cfg.session_scope.as_ref()?;
        Some(
            scope
                .roots
                .iter()
                .map(|root| {
                    format!(
                        "{column} = {root} OR startsWith({column}, {prefix})",
                        root = sql_quote(root),
                        prefix = sql_quote(&format!("{root}/")),
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR "),
        )
    }

    /// The configured project scope as a `mcp_session_directory` recall
    /// subquery — the identical relation and predicate shape as
    /// [`Self::build_session_directory_page_sql`], and O(S) over fixed-width
    /// scalars. `None` when no scope is configured, so an unscoped request
    /// emits no extra relation at all.
    ///
    /// `origin_cwd` is `argMinIfMerge(d.origin_cwd_state)` — an aggregate over
    /// the session's directory rows — so it MUST be projected by the grouping
    /// relation and filtered one level up. Referencing the alias from this
    /// statement's own `HAVING` while projecting only `session_id` compiles as
    /// a string and fails at execution with `Unknown identifier: origin_cwd`;
    /// ClickHouse resolves that only when the statement runs, which is why the
    /// nested form is pinned by
    /// `scope_recall_projects_the_origin_cwd_it_filters` and by a live gate.
    /// The outer projection is one column so the relation stays usable as an
    /// `IN` set.
    pub(super) fn search_scope_recall_subquery(&self) -> Option<String> {
        let directory = self.table_ref("mcp_session_directory");
        let published = self.published_generations_subquery();
        let roots = self.scope_root_predicate("scoped.origin_cwd")?;
        Some(format!(
            "(\n    SELECT scoped.session_id\n    FROM (\n      SELECT\n        d.session_id AS session_id,\n        argMinIfMerge(d.origin_cwd_state) AS origin_cwd\n      FROM {directory} AS d\n      WHERE notEmpty(trimBoth(d.session_id))\n        AND (d.source_host, d.source_name, d.source_file, d.source_generation) IN {published}\n      GROUP BY d.session_id\n    ) AS scoped\n    WHERE ({roots})\n  )"
        ))
    }

    // -----------------------------------------------------------------------
    // Phase 2 — content-free candidate derivation.
    // -----------------------------------------------------------------------

    /// Derive `event_order` / `turn_seq` / `event_ordinal` / display time for
    /// the bounded candidate set, plus each candidate session's exact
    /// `origin_cwd`, from `mcp_event_navigation` only (§2.2).
    ///
    /// Cost is `O(Σ events of candidate sessions)` — flat in unrelated corpus
    /// size, which is what the hydration exit gate asks for. Content-free: no
    /// `text_content`, no `payload_json`.
    ///
    /// The row's presence at the candidate's `event_version` is a **second,
    /// independent version check**: a candidate the locator authorized but
    /// navigation does not carry at the same version yields no row and is
    /// dropped.
    ///
    /// `origin_cwd` is computed in its own session-grain derived table and
    /// `ANY LEFT JOIN`ed on, because `argMinIf` is an aggregate over the whole
    /// session while the outer rows are per event.
    ///
    /// The final filter is a `(session_id, event_uid)` tuple set, not a bare
    /// uid list: `event_uid` excludes `session_id` from its content address
    /// (#608), so one uid can legitimately exist under two sessions, and a
    /// uid-only filter returns BOTH sessions' rows for it. Deriving
    /// `turn_seq` / `event_order` / `event_ordinal` from the wrong session's
    /// ordering is exactly the mis-hydration this qualification prevents.
    pub(super) fn build_search_candidate_derivation_sql(
        &self,
        session_ids: &[String],
        candidates: &[(String, String)],
    ) -> RepoResult<String> {
        if session_ids.is_empty() || candidates.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot derive canonical search candidates for an empty candidate set",
            ));
        }
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let ids = sql_array_strings(session_ids);
        let uids = Self::sql_session_event_tuples(candidates);
        let event_ts_tuple = "tuple(n.event_ts, n.event_uid)";
        Ok(format!(
            "WITH
  session_cwd AS (
    SELECT
      n.session_id AS session_id,
      ifNull(argMinIf(n.cwd, {event_ts_tuple}, n.cwd != ''), '') AS origin_cwd
    {from}
    WHERE n.session_id IN {ids}
    GROUP BY n.session_id
  ),
  turned AS (
    SELECT
      n.session_id AS session_id,
      n.event_uid AS event_uid,
      n.source_host AS source_host,
      n.event_version AS event_version,
      n.display_time AS display_time,
      n.event_ts AS event_ts,
      {tuple} AS sort_key,
      row_number() OVER order_window AS event_order,
      if(toUInt32(n.turn_index) > 0, toUInt32(n.turn_index), greatest(toUInt32(1), toUInt32(sum(if(n.is_user_message = 1, 1, 0)) OVER order_window))) AS turn_seq
    {from}
    WHERE n.session_id IN {ids}
    WINDOW order_window AS (PARTITION BY n.session_id ORDER BY {tuple} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ),
  ordinaled AS (
    SELECT
      turned.session_id AS session_id,
      turned.event_uid AS event_uid,
      turned.source_host AS source_host,
      turned.event_version AS event_version,
      turned.display_time AS display_time,
      turned.event_ts AS event_ts,
      turned.event_order AS event_order,
      turned.turn_seq AS turn_seq,
      row_number() OVER (PARTITION BY turned.session_id, turned.turn_seq ORDER BY turned.sort_key ASC) AS event_ordinal
    FROM turned
  )
SELECT
  ordinaled.session_id AS session_id,
  ordinaled.event_uid AS event_uid,
  ordinaled.source_host AS source_host,
  toUInt64(ordinaled.event_version) AS event_version,
  toString(ordinaled.display_time) AS display_time,
  toInt64(toUnixTimestamp64Milli(ordinaled.display_time)) AS display_time_ms,
  toInt64(toUnixTimestamp64Milli(ordinaled.event_ts)) AS event_ts_ms,
  toUInt64(ordinaled.event_order) AS event_order,
  toUInt32(ordinaled.turn_seq) AS turn_seq,
  toUInt32(ordinaled.event_ordinal) AS event_ordinal,
  ifNull(session_cwd.origin_cwd, '') AS origin_cwd
FROM ordinaled
ANY LEFT JOIN session_cwd
  ON session_cwd.session_id = ordinaled.session_id
WHERE (ordinaled.session_id, ordinaled.event_uid) IN ({uids})
FORMAT JSONEachRow"
        ))
    }

    // -----------------------------------------------------------------------
    // Phase 3 — dedup keys.
    // -----------------------------------------------------------------------

    /// The two content-DERIVED dedup inputs (#539/#565) as fixed-width columns
    /// for the bounded candidate set (§2.3).
    ///
    /// `text_digest` and `payload_phase` are migration-037 MATERIALIZED columns
    /// on `search_documents`. Because ClickHouse is columnar, this statement
    /// reads only those columns plus the identity — no wide column appears in
    /// the projection list, which is the property the read-bytes gate asserts.
    /// (On parts written before 037, ClickHouse recomputes the MATERIALIZED
    /// value from the source column at read time; #603's `MATERIALIZE COLUMN`
    /// is what turns the decoupling into a byte saving.)
    ///
    /// The version triple is exact: the candidate's `post_version` IS the live
    /// `event_version` the locator authorized, so a stale document revision
    /// cannot contribute a digest. A candidate with no matching document row
    /// (an MV gap) simply carries no digest, and
    /// [`ClickHouseConversationRepository::mcp_search_rows_are_equivalent`]
    /// falls back to comparing `text_content` for it — the same behaviour it
    /// has always had for an empty digest.
    ///
    /// No `FINAL`: the version is pinned by the `IN` set, and RMT duplicates at
    /// the same `doc_version` are byte-identical.
    pub(super) fn build_search_dedup_keys_sql(
        &self,
        candidates: &[SearchCandidateRow],
    ) -> RepoResult<String> {
        if candidates.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot read dedup keys for an empty candidate set",
            ));
        }
        let documents = self.table_ref("search_documents");
        let triples = candidates
            .iter()
            .map(|candidate| {
                format!(
                    "({}, {}, toUInt64({}))",
                    sql_quote(&candidate.source_host),
                    sql_quote(&candidate.event_uid),
                    candidate.post_version,
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!(
            "SELECT\n  d.source_host AS source_host,\n  d.event_uid AS event_uid,\n  any(d.text_digest) AS text_content_digest,\n  any(d.payload_phase) AS payload_phase\nFROM {documents} AS d\nWHERE (d.source_host, d.event_uid, d.doc_version) IN ({triples})\nGROUP BY d.source_host, d.event_uid\nFORMAT JSONEachRow"
        ))
    }

    // -----------------------------------------------------------------------
    // Phase 5 — winner hydration.
    // -----------------------------------------------------------------------

    /// Per-turn scalars for the winner sessions (§2.5). The K-session,
    /// per-TURN form of [`Self::build_session_terminal_batch_sql`], which
    /// collapses to one session-level row and therefore cannot serve a hit's
    /// `turn_event_count` / `turn_completed` / `turn_terminal_event_uid`.
    ///
    /// The two-level terminal rule is preserved exactly: a turn's `completed`
    /// is its latest terminal event being `task_complete`. Content-free.
    pub(super) fn build_search_turn_aggregates_sql(&self, session_ids: &[String]) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let ids = sql_array_strings(session_ids);
        format!(
            "SELECT\n  session_id,\n  toUInt32(turn_seq) AS turn_seq,\n  toUInt64(count()) AS turn_event_count,\n  toUInt8(argMaxIf(toUInt8(payload_type = 'task_complete'), sort_key, payload_type IN ('task_complete', 'turn_aborted'))) AS turn_completed,\n  ifNull(argMaxIf(nullIf(event_uid, ''), sort_key, payload_type IN ('task_complete', 'turn_aborted')), '') AS turn_terminal_event_uid\nFROM (\n  SELECT\n    n.session_id AS session_id,\n    n.event_uid AS event_uid,\n    n.payload_type AS payload_type,\n    {tuple} AS sort_key,\n    if(toUInt32(n.turn_index) > 0, toUInt32(n.turn_index), greatest(toUInt32(1), toUInt32(sum(if(n.is_user_message = 1, 1, 0)) OVER turn_window))) AS turn_seq\n  {from}\n  WHERE n.session_id IN {ids}\n  WINDOW turn_window AS (PARTITION BY n.session_id ORDER BY {tuple} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n)\nGROUP BY session_id, turn_seq\nFORMAT JSONEachRow"
        )
    }

    /// The K-uid wide read (§2.5): `text_content` / `payload_json` truncated to
    /// the configured preview budget, plus `model`, `endpoint_kind`, `call_id`,
    /// `item_id` and `source_ref` for exactly the winners.
    ///
    /// This is the statement that retires the v1 `models` CTE, which scanned
    /// `v_live_events` with a uid-only predicate against a primary key
    /// (`session_id, event_ts, …`) that a uid cannot prune. Here `model` rides
    /// the same session- and `event_ts`-bounded scan as the content columns.
    ///
    /// The `(session_id, event_uid)` `IN` set is the exact filter;
    /// [`EventTsBounds`] exists only for granule pruning and is emitted INSIDE
    /// the derived table, above which an identical predicate would prune
    /// nothing. The filter is session-qualified for the same reason the
    /// candidate derivation is: `moraine.events` is `ORDER BY (session_id, …)`
    /// and genuinely carries one uid under two sessions when ingest
    /// double-attributes a physical line (#608), so a uid-only filter hydrates
    /// a winner from whichever session's row arrives last.
    pub(super) fn build_search_wide_hydration_sql(
        &self,
        session_ids: &[String],
        winners: &[(String, String)],
        bounds: Option<EventTsBounds>,
    ) -> RepoResult<String> {
        if session_ids.is_empty() || winners.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot hydrate canonical search winners for an empty set",
            ));
        }
        let events = self.live_events_source_sessions_bounded(session_ids, bounds);
        let uid_list = Self::sql_session_event_tuples(winners);
        let text_content_limit = usize::from(self.cfg.preview_chars).saturating_mul(4);
        let payload_json_limit = usize::from(self.cfg.preview_chars).saturating_mul(8);
        // Exactly the columns the merge consumes, and no others: every column
        // named here is decompressed for the winners, and the read-bytes gate
        // is denominated on that list. `event_class` / `payload_type` /
        // `actor_role` / `phase` deliberately do NOT appear — they come from
        // the ranked posting, so the value dedup compared is the value the hit
        // reports.
        Ok(format!(
            "SELECT\n  e.session_id AS session_id,\n  e.event_uid AS event_uid,\n  e.source_host AS source_host,\n  e.inference_provider AS inference_provider,\n  e.endpoint_kind AS endpoint_kind,\n  e.tool_call_id AS call_id,\n  e.item_id AS item_id,\n  e.model AS model,\n  e.source_ref AS source_ref,\n  leftUTF8(e.text_content, {preview}) AS text_preview,\n  leftUTF8(e.text_content, {text_content_limit}) AS text_content,\n  leftUTF8(e.payload_json, {payload_json_limit}) AS payload_json\nFROM {events} AS e\nWHERE (e.session_id, e.event_uid) IN ({uid_list})\nFORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
        ))
    }

    /// `('sess','uid'),…` — the session-qualified identity set every
    /// canonical search read is filtered by. See
    /// [`Self::build_search_candidate_derivation_sql`] for why a bare uid list
    /// is not a valid identity.
    fn sql_session_event_tuples(pairs: &[(String, String)]) -> String {
        pairs
            .iter()
            .map(|(session_id, event_uid)| {
                format!("({}, {})", sql_quote(session_id), sql_quote(event_uid))
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

// ---------------------------------------------------------------------------
// Deserialization rows.
// ---------------------------------------------------------------------------

/// One ranked candidate. Carries the locator's fixed source coordinates, which
/// replace `parse_mcp_source_ref(&row.source_ref)` as the input to the #565
/// codex-mirror rule: the locator's values are authoritative and version
/// matched, while `source_ref` is a formatted string that has to be re-parsed.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchCandidateRow {
    pub(super) event_uid: String,
    pub(super) source_host: String,
    pub(super) session_id: String,
    /// The live canonical `event_version` the locator authorized. Replaces v1's
    /// `(slot, generation)` projector publication identifiers, which have no v2
    /// analogue.
    pub(super) post_version: u64,
    pub(super) source_file: String,
    pub(super) source_generation: u32,
    pub(super) source_line_no: u64,
    pub(super) sort_time_ms: i64,
    pub(super) harness: String,
    pub(super) source_name: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) actor_role: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) doc_len: u32,
    pub(super) raw_score: f64,
    pub(super) matched_terms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchCandidateDerivationRow {
    pub(super) session_id: String,
    pub(super) event_uid: String,
    pub(super) source_host: String,
    pub(super) event_version: u64,
    pub(super) display_time: String,
    pub(super) display_time_ms: i64,
    pub(super) event_ts_ms: i64,
    pub(super) event_order: u64,
    pub(super) turn_seq: u32,
    pub(super) event_ordinal: u32,
    pub(super) origin_cwd: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchDedupKeyRow {
    pub(super) source_host: String,
    pub(super) event_uid: String,
    pub(super) text_content_digest: String,
    pub(super) payload_phase: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchTurnAggregateRow {
    pub(super) session_id: String,
    pub(super) turn_seq: u32,
    pub(super) turn_event_count: u64,
    pub(super) turn_completed: u8,
    pub(super) turn_terminal_event_uid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchWideRow {
    pub(super) session_id: String,
    pub(super) event_uid: String,
    pub(super) source_host: String,
    pub(super) inference_provider: String,
    pub(super) endpoint_kind: String,
    pub(super) call_id: String,
    pub(super) item_id: String,
    pub(super) model: String,
    pub(super) source_ref: String,
    pub(super) text_preview: String,
    pub(super) text_content: String,
    pub(super) payload_json: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct TermDfRow {
    pub(super) term: String,
    pub(super) df: u64,
}

/// One ranked `search_events` candidate — the bounded replacement for the
/// in-process scorer's `CachedPostingRow` accumulation. Content-free; the
/// winners' text arrives from the bounded hydration read.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchEventsCandidateRow {
    pub(super) event_uid: String,
    pub(super) source_host: String,
    pub(super) score: f64,
    pub(super) matched_terms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ScopeExistsRow {
    pub(super) scope_exists: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct TurnEventUidRow {
    pub(super) event_uid: String,
}

// ---------------------------------------------------------------------------
// SQL-shape gates.
//
// Every assertion below names the single production edit that breaks it. A
// shape test that no edit can fail is decoration — this epic has shipped six of
// those — so each one was proven by reintroducing the defect and confirming a
// NAMED test failed.
//
// Shape tests are NOT sufficient on their own: ClickHouse resolves an outer
// reference to `alias.column` that the inner derived table fails to project
// only at execution time, and this repository has shipped that defect twice.
// The live half lives in `tests/live_clickhouse/bounded_search.rs`.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse_repo::consistency::{
        with_test_publication_snapshot, TestPublicationSnapshot,
    };
    use crate::clickhouse_repo::sql::canonical_assertions::{
        assert_content_free, assert_no_legacy_view_chain, assert_no_projection,
    };
    use moraine_clickhouse::ClickHouseClient;

    fn repo() -> ClickHouseConversationRepository {
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        ClickHouseConversationRepository::new(client, RepoConfig::default())
    }

    fn scoped_repo(roots: &[&str]) -> ClickHouseConversationRepository {
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        ClickHouseConversationRepository::new(
            client,
            RepoConfig {
                session_scope: SessionOriginScope::from_roots(roots.iter().copied()),
                ..RepoConfig::default()
            },
        )
    }

    async fn build<F: FnOnce(&ClickHouseConversationRepository) -> String>(
        repo: ClickHouseConversationRepository,
        f: F,
    ) -> String {
        with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async move {
            f(&repo)
        })
        .await
    }

    fn terms() -> Vec<String> {
        vec!["alpha".to_string(), "beta".to_string()]
    }

    fn ranking_params<'a>(
        terms: &'a [String],
        types: &'a [McpEventType],
    ) -> SearchRankingParams<'a> {
        SearchRankingParams {
            terms,
            event_types: types,
            session_id: None,
            turn_event_uids: None,
            harness: None,
            source_name: None,
            min_should_match: 1,
            min_score: 0.0,
            corpus_stats: (100, 5_000),
            limit: 9,
        }
    }

    fn candidate(event_uid: &str, session_id: &str) -> SearchCandidateRow {
        SearchCandidateRow {
            event_uid: event_uid.to_string(),
            source_host: "host-a".to_string(),
            session_id: session_id.to_string(),
            post_version: 7,
            source_file: "/tmp/a.jsonl".to_string(),
            source_generation: 1,
            source_line_no: 4,
            sort_time_ms: 1_767_434_520_000,
            harness: "codex".to_string(),
            source_name: "codex".to_string(),
            event_class: "message".to_string(),
            payload_type: "message".to_string(),
            actor_role: "assistant".to_string(),
            name: String::new(),
            phase: String::new(),
            doc_len: 19,
            raw_score: 12.5,
            matched_terms: 2,
        }
    }

    /// Every v2 search statement, in one place, against the issue-598 exit gate.
    ///
    /// MUTATION: point any one builder back at `mcp_open_events` /
    /// `mcp_open_turns` / `mcp_open_publication_headers` and this fails naming
    /// that builder. The gate is deliberately unconditional HERE (these are the
    /// v2 builders, which exist only for the canonical path) — the live gate is
    /// the one that must be conditioned on the readiness latch, because the v1
    /// engine legitimately still reads the projection while `open_v2.ready = 0`.
    #[tokio::test]
    async fn every_v2_search_builder_is_free_of_the_projection() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let candidates = [candidate("evt-a", "sess-a")];
        let sessions = vec!["sess-a".to_string()];
        let uids = vec![("sess-a".to_string(), "evt-a".to_string())];

        let statements: Vec<(&str, String)> =
            with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async {
                let repo = repo();
                vec![
                    ("scope_exists", repo.build_search_scope_exists_sql("sess-a")),
                    (
                        "turn_event_uids",
                        repo.build_search_turn_event_uids_sql("sess-a", 2),
                    ),
                    (
                        "ranking",
                        repo.build_search_ranking_sql(&ranking_params(&terms, &types))
                            .expect("ranking sql"),
                    ),
                    (
                        "events_ranking",
                        repo.build_search_events_ranking_sql(
                            &terms,
                            true,
                            None,
                            true,
                            None,
                            None,
                            1,
                            0.0,
                            (100, 5_000),
                            9,
                        )
                        .expect("events ranking sql"),
                    ),
                    ("term_df", repo.build_term_df_sql(&terms).expect("df sql")),
                    (
                        "candidate_derivation",
                        repo.build_search_candidate_derivation_sql(&sessions, &uids)
                            .expect("derivation sql"),
                    ),
                    (
                        "dedup_keys",
                        repo.build_search_dedup_keys_sql(&candidates)
                            .expect("dedup sql"),
                    ),
                    (
                        "turn_aggregates",
                        repo.build_search_turn_aggregates_sql(&sessions),
                    ),
                    (
                        "wide_hydration",
                        repo.build_search_wide_hydration_sql(&sessions, &uids, None)
                            .expect("wide sql"),
                    ),
                ]
            })
            .await;

        assert_eq!(statements.len(), 9, "every v2 builder must be covered here");
        for (name, sql) in &statements {
            assert_no_projection(name, sql);
            assert_no_legacy_view_chain(name, sql);
            // Only the winner-hydration read is allowed to name a wide column.
            if *name != "wide_hydration" {
                assert_content_free(name, sql);
            }
        }
    }

    /// The candidate-selection statements touch `moraine.events` not once, let
    /// alone twice.
    ///
    /// v1's ranking statement scanned canonical `events` TWICE before it read a
    /// single posting: `mcp_search_sessions_source`'s `current_sources`
    /// `cityHash64(groupArray(tuple(event_uid, event_version)))` over every live
    /// event of every authorized session, and a second, independent
    /// `live_session_ids` `GROUP BY session_id` feeding the dirty-session gate.
    /// Both executed on every request, before term pruning, and both were O(E)
    /// in TOTAL corpus size — the shape the issue's "hydration remains flat as
    /// unrelated corpus size grows" exit gate is denominated on.
    ///
    /// v2 selects candidates from `search_postings` ⋈ `mcp_event_locator`
    /// only. `events` is read exactly once per request, by Phase 5, over the
    /// winners' own sessions and `event_ts` range.
    ///
    /// MUTATION: add any `live_events_source()`-derived relation to a
    /// candidate-selection builder; this fails naming it.
    #[tokio::test]
    async fn no_v2_candidate_statement_scans_canonical_events() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let candidates = [candidate("evt-a", "sess-a")];
        let sessions = vec!["sess-a".to_string()];
        let uids = vec![("sess-a".to_string(), "evt-a".to_string())];

        let statements: Vec<(&str, String)> =
            with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async {
                let repo = scoped_repo(&["/repo"]);
                vec![
                    ("scope_exists", repo.build_search_scope_exists_sql("sess-a")),
                    (
                        "turn_event_uids",
                        repo.build_search_turn_event_uids_sql("sess-a", 2),
                    ),
                    (
                        "ranking",
                        repo.build_search_ranking_sql(&ranking_params(&terms, &types))
                            .expect("ranking sql"),
                    ),
                    (
                        "events_ranking",
                        repo.build_search_events_ranking_sql(
                            &terms,
                            true,
                            None,
                            true,
                            None,
                            None,
                            1,
                            0.0,
                            (100, 5_000),
                            9,
                        )
                        .expect("events ranking sql"),
                    ),
                    ("term_df", repo.build_term_df_sql(&terms).expect("df sql")),
                    (
                        "candidate_derivation",
                        repo.build_search_candidate_derivation_sql(&sessions, &uids)
                            .expect("derivation sql"),
                    ),
                    (
                        "dedup_keys",
                        repo.build_search_dedup_keys_sql(&candidates)
                            .expect("dedup sql"),
                    ),
                    (
                        "turn_aggregates",
                        repo.build_search_turn_aggregates_sql(&sessions),
                    ),
                ]
            })
            .await;

        assert_eq!(
            statements.len(),
            8,
            "every candidate-selection builder must be covered here"
        );
        for (name, sql) in &statements {
            for forbidden in ["`moraine`.`events`", "v_live_events", "live_events"] {
                assert!(
                    !sql.contains(forbidden),
                    "{name} must not scan canonical events, found `{forbidden}`:\n{sql}"
                );
            }
        }

        // The negative half: Phase 5 DOES read `events`, exactly once, and it
        // is the only statement that may. Without this the assertion above is
        // satisfiable by a build that never hydrates anything.
        let wide = build(repo(), move |repo| {
            repo.build_search_wide_hydration_sql(
                &["sess-a".to_string()],
                &[("sess-a".to_string(), "evt-a".to_string())],
                None,
            )
            .expect("wide sql")
        })
        .await;
        assert_eq!(
            wide.matches("`moraine`.`events`").count(),
            1,
            "winner hydration is the one and only canonical events read:\n{wide}"
        );
    }

    /// The v1 engine still reads the projection, and MUST keep doing so while
    /// `open_v2.ready = 0`.
    ///
    /// Without this negative case the projection gate above is unfalsifiable in
    /// one direction: someone "fixing" a live query-log gate that fires on an
    /// un-promoted box would delete the v1 reads and silently break the
    /// fallback engine. This pins that the fallback is still the fallback.
    #[tokio::test]
    async fn the_v1_engine_still_reads_the_projection() {
        let sql = build(repo(), |repo| {
            repo.build_search_mcp_events_sql(
                &terms(),
                &[McpEventType::AssistantResponse],
                None,
                None,
                None,
                None,
                1,
                0.0,
                Some((100, 5_000)),
                9,
            )
            .expect("v1 ranking sql")
        })
        .await;
        assert!(
            sql.contains("mcp_open_events") && sql.contains("mcp_open_projection_state"),
            "the v1 engine is the projected-header path; if this ever stops \
             being true the fallback is dead and the latch branch is a lie"
        );
    }

    /// §1.1: `df` is corpus-wide because it is computed inside `term_postings`,
    /// which carries no predicate but term membership. Pushing a user filter in
    /// there moves every BM25 score silently — no test of the RESULT would
    /// catch it, because the result would be self-consistently wrong.
    ///
    /// MUTATION: move any `where_clauses.push(...)` from
    /// `build_search_ranking_sql` into `bounded_ranking_ctes`; this fails.
    ///
    /// The gate covers the WHOLE fragment, `live_locator` included, not just
    /// `term_postings`'s own `WHERE`: `live_locator` FEEDS `term_postings`, so
    /// a predicate there prunes the df relation exactly as effectively as one a
    /// line lower, while reading as harmless "authorization".
    /// The ranking CTE reads postings with `FINAL` and projects their columns
    /// plainly — it must not aggregate.
    ///
    /// MUTATION: wrap the extras projection in `argMax(p.{col}, p.post_version)`,
    /// or turn the postings read into a `GROUP BY`; this fails.
    ///
    /// Round 3 of this issue did exactly that, on the false premise that
    /// `FINAL` was discarding a second session attribution at read time.
    /// `ReplacingMergeTree` collapses on the SORT KEY at STORAGE time and
    /// `search_postings` is `ORDER BY (term, doc_id, source_host)`, so the
    /// table cannot durably hold two attributions and the aggregation bought
    /// nothing — while replacing a term-key-pruned merging read with a
    /// `GROUP BY` plus ~12 per-row accumulators on the interactive path. No
    /// guard could reach that path, which is why this one exists.
    #[tokio::test]
    async fn ranking_reads_postings_without_aggregating_them() {
        // Built with a NON-EMPTY extras list, because that is the only shape
        // the regression can appear in: `extra_posting_columns` is `&[]` on
        // every path except conversation search (`search.rs`, which passes
        // `["inference_provider"]`). A guard built on the empty-extras shape
        // passes no matter what the projection does — verified by mutation.
        let ctes = build(repo(), |repo| {
            repo.bounded_ranking_ctes("['alpha','beta']", &["inference_provider"])
        })
        .await;

        assert!(
            ctes.contains("p.inference_provider AS inference_provider"),
            "fixture must exercise the extras projection, or this guard is \
             vacuous:\n{ctes}"
        );
        assert!(
            ctes.contains("FROM `moraine`.`search_postings` AS p FINAL"),
            "ranking must read postings with FINAL, not re-collapse them:\n{ctes}"
        );
        assert!(
            !ctes.contains("argMax(p."),
            "ranking must project posting columns plainly; an argMax means the \
             read became an aggregation:\n{ctes}"
        );
        assert!(
            !ctes.contains("GROUP BY p.term"),
            "ranking must not GROUP BY the postings key — that replaces a \
             key-pruned merging read with a full aggregation:\n{ctes}"
        );
    }

    #[tokio::test]
    async fn ranking_never_pushes_a_user_filter_into_the_df_cte() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let turn_uids = ["evt-turn-a".to_string()];
        let sql = build(scoped_repo(&["/repo"]), move |repo| {
            let mut params = ranking_params(&terms, &types);
            params.session_id = Some("sess-a");
            params.harness = Some("codex");
            params.source_name = Some("codex");
            params.turn_event_uids = Some(&turn_uids);
            repo.build_search_ranking_sql(&params).expect("ranking sql")
        })
        .await;

        let (ctes, projection) = sql
            .split_once("\nSELECT\n  p.event_uid AS event_uid,")
            .expect("ranking statement has CTEs and a projection");
        assert!(ctes.contains("toUInt64(count() OVER (PARTITION BY p.term)) AS df"));
        assert_eq!(
            ctes.matches(" OVER (").count(),
            1,
            "the df window is the only window in the ranking CTEs:\n{ctes}"
        );
        // The postings scan carries term membership and NOTHING else: the
        // clause runs to the CTE's closing paren with no conjunct after it.
        // This is the exact form, not a containment check — a containment check
        // would pass with a predicate appended below the term clause.
        let term_postings_where = ctes
            .rsplit_once("    WHERE p.term IN ['alpha','beta']")
            .expect("term_postings filters on term membership")
            .1;
        assert_eq!(
            term_postings_where.trim(),
            ")",
            "no user filter may live inside the df CTEs, found `{term_postings_where}`"
        );
        // …and every one of those filters really is present, one level down.
        // Every request-shaped value, anywhere in the fragment - `live_locator`
        // included. Each of these is a value the caller supplied, so its
        // presence above the window means `df` is no longer corpus-wide.
        for leaked in [
            "sess-a",
            "codex",
            "evt-turn-a",
            "/repo",
            "argMinIfMerge",
            "assistant",
            "mcp_session_directory",
        ] {
            assert!(
                !ctes.contains(leaked),
                "`{leaked}` is a user filter and must not appear anywhere in \
                 the df fragment - `live_locator` feeds `term_postings`, so a \
                 predicate there prunes the df relation just the same:\n{ctes}"
            );
        }
        for filter in [
            "p.session_id = 'sess-a'",
            "p.harness = 'codex'",
            "p.source_name = 'codex'",
            "p.source_name != 'codex-mcp'",
            "p.event_uid IN ['evt-turn-a']",
            "argMinIfMerge(d.origin_cwd_state)",
        ] {
            assert!(
                projection.contains(filter),
                "ranking WHERE lost `{filter}`:\n{projection}"
            );
        }
    }

    /// §1.1 / correction C2: `mcp_event_locator` holds one row per EVENT. An
    /// unpredicated scan of it trades one O(E) canonical scan for another O(E)
    /// index scan and fails the flatness gate at the ranking phase. Its primary
    /// key leads with `event_uid`, so the prunable form is an `IN` over the
    /// query's own term-pruned posting doc ids.
    ///
    /// MUTATION: drop the `WHERE l.event_uid IN (SELECT pruned.doc_id …)`
    /// predicate; this fails.
    #[tokio::test]
    async fn the_locator_scan_is_pruned_by_the_query_own_posting_doc_ids() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let sql = build(repo(), move |repo| {
            repo.build_search_ranking_sql(&ranking_params(&terms, &types))
                .expect("ranking sql")
        })
        .await;

        let locator = sql
            .split_once("FROM `moraine`.`mcp_event_locator` AS l FINAL")
            .expect("ranking joins the locator")
            .1;
        assert!(
            locator.starts_with("\n    WHERE l.event_uid IN (\n      SELECT pruned.doc_id"),
            "the locator scan must be pruned by the query's posting doc ids:\n{locator}"
        );
        // Published generations are a tuple-IN, never a join: #599 established
        // that a join blocks KeyCondition pruning.
        assert!(locator.contains(
            "AND (l.source_host, l.source_name, l.source_file, l.source_generation) IN (SELECT"
        ));
        assert!(
            !locator[..locator.find("  ),").unwrap_or(locator.len())].contains("ALL INNER JOIN"),
            "published generations must not be joined onto the locator scan:\n{locator}"
        );
        // The version join is what a `search_documents`-only version check
        // cannot do: it is maintained directly from `events`.
        assert!(sql.contains("AND l.event_version = p.post_version"));
    }

    /// §1.4: `origin_cwd` is `argMinIfMerge(origin_cwd_state)` — an AGGREGATE
    /// over the session's directory rows — so the relation that groups must
    /// also project it, and the filter belongs one level up.
    ///
    /// This is the "outer query references `alias.column` the inner derived
    /// table does not project" class that ClickHouse only rejects at execution
    /// time, and that this repository has shipped twice. A `HAVING (origin_cwd
    /// = …)` beside `SELECT d.session_id` alone builds a perfectly plausible
    /// string and fails with `Unknown identifier: origin_cwd` against a server.
    ///
    /// MUTATION: collapse the nested form back to `SELECT d.session_id … GROUP
    /// BY d.session_id HAVING (origin_cwd = …)`; this fails.
    #[tokio::test]
    async fn scope_recall_projects_the_origin_cwd_it_filters() {
        let sql = build(scoped_repo(&["/repo"]), |repo| {
            repo.search_scope_recall_subquery()
                .expect("scoped repo emits a recall subquery")
        })
        .await;

        let (inner, outer) = sql
            .split_once(") AS scoped")
            .expect("the recall subquery groups in a named derived table");
        assert!(
            inner.contains("argMinIfMerge(d.origin_cwd_state) AS origin_cwd"),
            "the grouping relation must PROJECT the value the filter reads:\n{inner}"
        );
        assert!(
            outer.contains("scoped.origin_cwd = '/repo'")
                && outer.contains("startsWith(scoped.origin_cwd, '/repo/')"),
            "the root predicate must read the projected alias:\n{outer}"
        );
        assert!(
            !inner.contains("HAVING"),
            "an aggregate alias cannot be filtered in the HAVING of the \
             statement that defines it:\n{inner}"
        );
        // Exactly one projected column, so the relation stays usable as an IN set.
        assert!(sql
            .trim_start()
            .starts_with("(\n    SELECT scoped.session_id\n"));
    }

    /// B1 / #608. Every canonical read after ranking is filtered by the
    /// SESSION-QUALIFIED identity, never by a bare uid list.
    ///
    /// `event_uid` is content-addressed over
    /// `source_file|source_generation|source_line_no|source_offset|
    /// record_fingerprint` and deliberately excludes `session_id`, so one uid
    /// legitimately exists under two sessions (19,846 of them on the reference
    /// host). `mcp_event_navigation` and `moraine.events` both lead their
    /// primary key with `session_id` and therefore CARRY both rows; a uid-only
    /// filter returns both and leaves the reader to guess.
    ///
    /// MUTATION: emit `WHERE ordinaled.event_uid IN [...]` /
    /// `WHERE e.event_uid IN [...]`; this fails.
    #[tokio::test]
    async fn canonical_search_reads_are_session_qualified() {
        let sessions = vec!["sess-a".to_string(), "sess-c".to_string()];
        // ONE uid, TWO sessions — the shape the qualification exists for.
        let winners = vec![
            ("sess-c".to_string(), "evt-shared".to_string()),
            ("sess-a".to_string(), "evt-shared".to_string()),
        ];
        let (derivation, wide) =
            with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async {
                let repo = repo();
                (
                    repo.build_search_candidate_derivation_sql(&sessions, &winners)
                        .expect("derivation sql"),
                    repo.build_search_wide_hydration_sql(&sessions, &winners, None)
                        .expect("wide sql"),
                )
            })
            .await;

        assert!(
            derivation.contains(
                "WHERE (ordinaled.session_id, ordinaled.event_uid) IN (('sess-c', 'evt-shared'),('sess-a', 'evt-shared'))"
            ),
            "the derivation must be filtered by the session-qualified identity:\n{derivation}"
        );
        assert!(
            wide.contains(
                "WHERE (e.session_id, e.event_uid) IN (('sess-c', 'evt-shared'),('sess-a', 'evt-shared'))"
            ),
            "the winner hydration must be filtered by the session-qualified identity:\n{wide}"
        );
        for (name, sql) in [("derivation", &derivation), ("wide", &wide)] {
            assert!(
                !sql.contains("event_uid IN ["),
                "{name} must not fall back to a bare uid list:\n{sql}"
            );
        }
    }

    /// B1 / #608, the RANKING half. A hit's `session_id` is the posting's own
    /// physical column, never the locator's.
    ///
    /// `mcp_event_locator` is `ReplacingMergeTree(event_version) ORDER BY
    /// (event_uid, source_host)` — `session_id` is not in its sort key. A uid
    /// that ingest attributed to two sessions therefore collapses to ONE
    /// arbitrary locator row, so reading `session_id` off the locator (a) puts
    /// one of the two attributions under the other's session and (b) makes
    /// `p.session_id = '<requested>'` in a session-scoped search filter on a
    /// session the caller never named. `search_postings.session_id` is a
    /// per-posting physical column (`sql/004_search_index.sql`), carries both
    /// attributions, and is what v1 joined on.
    ///
    /// The locator projects no session at all, so there is nothing to take by
    /// accident; the second assertion is what keeps that true.
    ///
    /// MUTATION: project `l.session_id AS session_id` in `live_locator` and use
    /// it in `term_postings`; this fails on both halves.
    #[tokio::test]
    async fn ranking_session_id_is_the_postings_own_column_not_the_locators() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let sql = build(repo(), move |repo| {
            repo.build_search_ranking_sql(&ranking_params(&terms, &types))
                .expect("ranking sql")
        })
        .await;

        let (locator, term_postings) = sql
            .split_once("  live_locator AS (")
            .expect("the ranking statement carries the shared CTEs")
            .1
            .split_once("  term_postings AS (")
            .expect("the ranking statement defines term_postings");
        assert!(
            !locator.contains("session_id"),
            "the locator must not project a session — its ReplacingMergeTree \
             collapses a double-attributed uid to one arbitrary session:\n{locator}"
        );
        assert!(
            term_postings.contains("p.session_id AS session_id"),
            "the ranked identity's session must come from the posting's own \
             physical column:\n{term_postings}"
        );
        assert!(
            !term_postings.contains("l.session_id"),
            "…and never from the locator:\n{term_postings}"
        );
        // The ranked identity is projected session-qualified — the same triple
        // every post-ranking read is keyed on — rather than reduced to an
        // `any(p.session_id)`. Under `FINAL` this cannot change the ranking's
        // cardinality (one posting row per `(term, event_uid, source_host)`
        // survives, so the session is functionally determined); what it pins is
        // that the session travels as a GROUP KEY and never as an aggregate
        // over rows that could disagree.
        assert!(
            sql.contains("GROUP BY p.event_uid, p.source_host, p.session_id"),
            "the ranked identity is (event_uid, source_host, session_id):\n{sql}"
        );
        assert!(
            !sql.contains("any(p.session_id)"),
            "the hit's session must be a group key, never an aggregate:\n{sql}"
        );
    }

    /// C1 / D1. `df`'s `count()` is an EXACT document count, and it is exact
    /// only because of two other clauses in the same CTE. This pins all three
    /// together so a future edit cannot silently turn `count()` back into a row
    /// count.
    ///
    /// The chain: `FROM search_postings FINAL` leaves one row per
    /// `(term, doc_id, source_host)` — the table is
    /// `ReplacingMergeTree(post_version) ORDER BY (term, doc_id, source_host)`
    /// (`sql/004`, widened by `sql/032`) — and the three-way equi-join to
    /// `live_locator` (one row per `(event_uid, source_host)`, on
    /// `event_version = post_version`) cannot multiply that. So `term_postings`
    /// holds at most one row per `(term, event_uid, source_host)`, and
    /// `count()` equals `uniqExact(tuple(event_uid, source_host))` on it.
    ///
    /// Removing `FINAL`, weakening the join to a non-equality, or adding any
    /// relation that can multiply rows per document breaks that equality
    /// SILENTLY: `df` inflates, every IDF shifts, and no result-shaped test
    /// notices because the result is self-consistently wrong.
    ///
    /// MUTATION (a): drop `FINAL` from the postings scan — fails.
    /// MUTATION (b): drop `AND l.event_version = p.post_version` from the join
    /// — fails.
    /// MUTATION (c): `df` ← anything other than `count() OVER (PARTITION BY
    /// p.term)` — fails.
    #[tokio::test]
    async fn df_counts_documents_because_final_and_the_version_join_make_it_exact() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let sql = build(repo(), move |repo| {
            repo.build_search_ranking_sql(&ranking_params(&terms, &types))
                .expect("ranking sql")
        })
        .await;

        let cte = sql
            .split_once("  term_postings AS (")
            .expect("the ranking statement defines term_postings")
            .1;
        let cte = &cte[..cte.find("\n  )").expect("term_postings is closed")];
        for clause in [
            "      toUInt64(count() OVER (PARTITION BY p.term)) AS df",
            "    FROM `moraine`.`search_postings` AS p FINAL",
            "    ALL INNER JOIN live_locator AS l",
            "      ON l.event_uid = p.doc_id",
            "     AND l.source_host = p.source_host",
            "     AND l.event_version = p.post_version",
        ] {
            assert!(
                cte.contains(clause),
                "`count()` is an exact DOCUMENT count only while `{clause}` is \
                 in the SAME CTE; without it `df` silently becomes a row \
                 count:\n{cte}"
            );
        }
        // The collapse is the table's own replacement, not a hand-rolled one:
        // an in-query `GROUP BY` over the postings scan would trade a
        // term-key-pruned merging read for a hash aggregation on the
        // interactive path, and — since the sort key omits `session_id` — would
        // key the result on a distinction the table cannot durably hold.
        assert!(
            !cte.contains("GROUP BY"),
            "the postings scan must stay a streaming merging read:\n{cte}"
        );
        assert!(
            !cte.contains("argMax("),
            "per-column argMax accumulators are the hand-rolled collapse \
             `FINAL` already does:\n{cte}"
        );
    }

    /// B2 / §1.3. `scope_exists` decides whether the tool answers `not_found`
    /// or "exists, zero hits". v1 filtered its `scope_state_sql` by the
    /// projected `origin_cwd`, so a scoped caller could not tell an
    /// out-of-scope session from a nonexistent one. The v2 point read must
    /// enforce the identical predicate, over the directory's
    /// `argMinIfMerge(origin_cwd_state)` — the same relation and same shape the
    /// ranking recall filter uses.
    ///
    /// MUTATION: return the unscoped `SELECT count() > 0 FROM directory …`
    /// branch unconditionally; this fails.
    #[tokio::test]
    async fn scope_exists_point_read_enforces_the_configured_project_scope() {
        let (unscoped, scoped) =
            with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async {
                (
                    repo().build_search_scope_exists_sql("sess-a"),
                    scoped_repo(&["/repo"]).build_search_scope_exists_sql("sess-a"),
                )
            })
            .await;

        // An unscoped server emits no scope relation at all.
        assert!(!unscoped.contains("origin_cwd"), "{unscoped}");
        assert!(!unscoped.contains("AS scoped"), "{unscoped}");

        assert!(
            scoped.contains("argMinIfMerge(d.origin_cwd_state) AS origin_cwd"),
            "the grouping relation must PROJECT the value the filter reads:\n{scoped}"
        );
        assert!(
            scoped.contains(
                "WHERE (scoped.origin_cwd = '/repo' OR startsWith(scoped.origin_cwd, '/repo/'))"
            ),
            "an out-of-scope session must not be disclosed as existing:\n{scoped}"
        );
        // The point read stays a point read: the session predicate is still on
        // the directory scan, which is where the primary key prunes it.
        assert!(scoped.contains("WHERE d.session_id = 'sess-a'"), "{scoped}");
        // The aggregate alias is filtered one level up, never in the HAVING of
        // the statement that defines it (execution-time failure, see
        // `scope_recall_projects_the_origin_cwd_it_filters`).
        assert!(!scoped.contains("HAVING"), "{scoped}");
    }

    /// B2, turn half. A turn-scoped request is the OTHER door into
    /// `scope_exists`: an empty uid derivation is what `search_sessions_v1`
    /// turns into `not_found`. v1 applied its origin-scope filter to the turn
    /// branch too, so this statement must gate on scope as well — and it gates
    /// on the EXACT navigation `argMinIf(n.cwd, …)`, the same authority the
    /// per-hit Phase 4 re-check uses, so turn existence cannot disagree with
    /// hit visibility.
    ///
    /// MUTATION: drop the `session_origin_cwd` WITH-scalar or its `AND (…)`;
    /// this fails.
    #[tokio::test]
    async fn turn_scope_derivation_enforces_the_configured_project_scope() {
        let (unscoped, scoped) =
            with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async {
                (
                    repo().build_search_turn_event_uids_sql("sess-a", 2),
                    scoped_repo(&["/repo"]).build_search_turn_event_uids_sql("sess-a", 2),
                )
            })
            .await;

        assert!(!unscoped.contains("session_origin_cwd"), "{unscoped}");
        assert!(!unscoped.contains("n.cwd"), "{unscoped}");

        assert!(
            scoped.contains("argMinIf(n.cwd, tuple(n.event_ts, n.event_uid), n.cwd != '')"),
            "the turn gate must read the EXACT origin cwd:\n{scoped}"
        );
        assert!(scoped.contains(") AS session_origin_cwd"), "{scoped}");
        assert!(
            scoped.contains("WHERE turn_seq = 2\n  AND (session_origin_cwd = '/repo' OR startsWith(session_origin_cwd, '/repo/'))"),
            "an out-of-scope session's turn must derive no uids:\n{scoped}"
        );
    }

    /// §2.3: the dedup-key read names ONLY fixed-width columns. This is the
    /// property the read-bytes gate is denominated on — a columnar engine reads
    /// exactly what the projection names, so naming `text_content` here would
    /// blow the budget by orders of magnitude while leaving row counts
    /// identical.
    ///
    /// MUTATION: replace `any(d.text_digest)` with
    /// `hex(SHA256(d.text_content))` (the v1 expression); this fails.
    #[tokio::test]
    async fn dedup_keys_read_fixed_width_columns_at_the_exact_candidate_version() {
        let candidates = [candidate("evt-a", "sess-a"), candidate("evt-b", "sess-b")];
        let sql = build(repo(), move |repo| {
            repo.build_search_dedup_keys_sql(&candidates)
                .expect("dedup sql")
        })
        .await;

        assert!(sql.contains("any(d.text_digest) AS text_content_digest"));
        assert!(sql.contains("any(d.payload_phase) AS payload_phase"));
        assert!(!sql.contains("SHA256"));
        assert!(!sql.contains("JSONExtractString"));
        // The version is pinned per candidate, so a stale document revision
        // cannot contribute a digest.
        assert!(sql.contains("WHERE (d.source_host, d.event_uid, d.doc_version) IN (('host-a', 'evt-a', toUInt64(7)),('host-a', 'evt-b', toUInt64(7)))"));
    }

    /// §2.5: the wide read is the ONLY statement permitted to name a wide
    /// column, it is bounded by the winners' uids, and its `event_ts` window is
    /// emitted INSIDE the live-events derived table.
    ///
    /// The same predicate in the outer `WHERE` sits above the publication join
    /// and prunes nothing (issue-598 C2-R0, "no optimizer trust") — which is a
    /// whole-corpus `events FINAL` scan wearing a bounded-looking predicate.
    ///
    /// MUTATION: pass the bounds to `live_events_source` instead of
    /// `live_events_source_sessions_bounded`, or move the session `IN` to the
    /// outer WHERE; this fails.
    #[tokio::test]
    async fn winner_hydration_is_bounded_inside_the_events_derived_table() {
        let sessions = vec!["sess-a".to_string(), "sess-b".to_string()];
        let uids = vec![("sess-a".to_string(), "evt-a".to_string())];
        let sql = build(repo(), move |repo| {
            repo.build_search_wide_hydration_sql(
                &sessions,
                &uids,
                Some(EventTsBounds {
                    range: Some((1_767_434_520_000, 1_767_434_530_000)),
                    include_epoch: true,
                }),
            )
            .expect("wide sql")
        })
        .await;

        let derived = sql
            .split_once("(SELECT e.*")
            .expect("the wide read uses the live-events derived table")
            .1;
        let derived = &derived[..derived.find(") AS e").expect("terminated derived table")];
        assert!(
            derived.contains("e.session_id IN ['sess-a','sess-b']"),
            "the session IN must be INSIDE the derived table:\n{derived}"
        );
        assert!(
            derived.contains("e.event_ts BETWEEN fromUnixTimestamp64Milli(1767434520000)"),
            "the event_ts window must be INSIDE the derived table:\n{derived}"
        );
        assert!(
            derived.contains("OR e.event_ts = fromUnixTimestamp64Milli(0)"),
            "the epoch-sentinel branch must survive, or a malformed-record_ts \
             winner silently disappears from its own hydration:\n{derived}"
        );
        // The session-qualified identity set is the exact filter; the bound
        // exists only for pruning.
        assert!(sql.contains("WHERE (e.session_id, e.event_uid) IN (('sess-a', 'evt-a'))"));
        // Content is truncated, and only the columns the merge consumes appear.
        assert!(sql.contains("leftUTF8(e.text_content, 880) AS text_content"));
        assert!(sql.contains("leftUTF8(e.payload_json, 1760) AS payload_json"));
        assert!(sql.contains("e.model AS model"));
        for absent in [
            "e.token_usage_json",
            "e.token_usage_buckets",
            "e.session_date",
        ] {
            assert!(
                !sql.contains(absent),
                "the wide read must not grow a column no winner field consumes: {absent}"
            );
        }
    }

    /// §2.6 / risk R10: ONE df formula ships. `build_term_df_sql` must produce
    /// the same value as the ranking window, over the same relation.
    ///
    /// MUTATION: change either side to `uniqExact(tuple(source_host, doc_id))`
    /// (the retired `df_map` formula) and the shared-CTE assertion fails.
    #[tokio::test]
    async fn one_df_formula_ships() {
        let terms = terms();
        let types = [McpEventType::AssistantResponse];
        let (ranking, df) =
            with_test_publication_snapshot(TestPublicationSnapshot::idle_local(11, 1), async {
                let repo = repo();
                (
                    repo.build_search_ranking_sql(&ranking_params(&terms, &types))
                        .expect("ranking sql"),
                    repo.build_term_df_sql(&terms).expect("df sql"),
                )
            })
            .await;

        // Both are built from the identical `live_locator` + `term_postings`
        // fragment, so they cannot drift apart by construction.
        let fragment = df
            .split_once("  live_locator AS (")
            .expect("df statement carries the shared CTEs")
            .1;
        let fragment = &fragment[..fragment
            .find("\nSELECT\n  toString(p.term)")
            .expect("df statement has a projection")];
        assert!(
            ranking.contains(fragment),
            "the df statement and the ranking statement must share ONE ranking \
             relation:\n{fragment}"
        );
        // …and ONE function over it. `count()` is exact here because
        // `term_postings` is at most one row per document per term — see
        // `df_counts_documents_because_final_and_the_version_join_make_it_exact`.
        assert!(df.contains("toUInt64(count()) AS df"));
        assert!(!df.contains("uniqExact"));
        assert!(ranking.contains("toUInt64(count() OVER (PARTITION BY p.term)) AS df"));
    }

    /// §1.6: the marker is `saturated && short`. Each conjunct is load-bearing
    /// and each is proven separately here.
    ///
    /// MUTATION: drop `saturated` -> the third case fails (a short but complete
    /// answer would be reported incomplete). Drop `short` -> the second case
    /// fails (a full page off a saturated window would be reported incomplete).
    #[test]
    fn candidate_budget_marker_requires_both_saturation_and_shortfall() {
        let incomplete = ClickHouseConversationRepository::candidate_budget_incomplete;
        assert!(incomplete(true, 2, 3), "saturated and short");
        assert!(!incomplete(true, 3, 3), "saturated but complete");
        assert!(!incomplete(false, 2, 3), "short but not saturated");
        assert!(!incomplete(false, 0, 3), "empty result is not incomplete");
    }

    /// The over-fetch window: one pass, `3x`, capped, never below the page.
    #[test]
    fn candidate_fetch_size_over_fetches_once_and_stays_bounded() {
        assert_eq!(mcp_candidate_fetch_size(1), 3);
        assert_eq!(mcp_candidate_fetch_size(3), 9);
        assert_eq!(mcp_candidate_fetch_size(26), 78);
        assert_eq!(
            mcp_candidate_fetch_size(u16::MAX),
            u32::from(u16::MAX),
            "the cap must never drop the window below the page it has to fill"
        );
        assert_eq!(mcp_candidate_fetch_size(200), MCP_SEARCH_CANDIDATE_MAX);
    }
}
