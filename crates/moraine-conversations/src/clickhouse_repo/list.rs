use super::canonical_list::{
    candidate_fetch_size, hydration_chunk_size, DirectoryCandidateRow, DirectoryPageParams,
    HydratedSession, SessionKeyset, SessionKeysetBatchRow, SessionMetaBatchRow,
    SessionTerminalBatchRow, SessionTotalsBatchRow,
};
use super::canonical_open::{list_title_and_summary, total_turns_from_parts, MetaRow};
use super::search::McpEventRankingOptions;
use super::*;
use crate::cursor::{
    ACCEPTED_MCP_SESSION_LIST_CURSOR_VERSIONS, MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY,
    MCP_SESSION_LIST_CURSOR_VERSION_HEADERS,
};

/// Sessions returned by a content search when the caller names no `limit`.
const SESSION_SEARCH_DEFAULT_LIMIT: u16 = 10;

/// Ranked sessions one content search may return, clamped HERE rather than
/// left to whichever transport called in.
///
/// `/api/v1/sessions/search` applies the same bound (`SESSION_SEARCH_MAX_LIMIT`
/// in `moraine-monitor-core`) and is today the only caller, so this changes no
/// answer. It is here because every bound below — the hit budget, the fan-in
/// floor, and therefore the ranking's candidate window — is derived from
/// `limit`, and a derivation whose safety depends on a constant in another
/// crate is not a bound, it is a coincidence. With this clamp the whole
/// reachable domain is `1..=SESSION_SEARCH_MAX_LIMIT` and
/// [`session_search_hit_budget`]'s invariants can be (and are) asserted over
/// all of it.
pub(super) const SESSION_SEARCH_MAX_LIMIT: u16 = 50;

/// Ranked EVENT hits requested per requested SESSION, while
/// [`SESSION_SEARCH_HIT_BUDGET_MAX`] allows it. Hits cluster inside a session —
/// a query that matches a session usually matches it several times — so a 1:1
/// request would return a fraction of the sessions asked for.
///
/// This factor is applied to an INTERNAL hit budget that is deliberately not
/// clamped to the caller-facing `[mcp] max_results`. It used to be, and in
/// every shipped configuration `max_results` is also the session limit, so the
/// budget collapsed to exactly `limit` and this constant never did anything.
/// See [`super::search::McpEventRankingOptions::hit_cap`].
const SESSION_SEARCH_HITS_PER_SESSION: u16 = 4;

/// Ceiling on the internal hit budget, and the reason the fan-in factor cannot
/// grow the ranking's cost without bound.
///
/// The hit budget sets `unique_fetch_limit`, which sets the ranking's candidate
/// window through `mcp_candidate_fetch_size` — the most expensive stage of the
/// whole request. Left as `limit x factor`, the shipped default shape
/// (`max_results = 25`, a client asking for 25) produced a budget of 100, a
/// window of `min(3 x 101, 256) = 256`, and therefore pinned the HARD ceiling
/// [`super::search_canonical::MCP_SEARCH_CANDIDATE_MAX`] on every default
/// interactive search — 3.3x the pre-fix cost, on the default path, ungated.
///
/// `50` is not a fresh magic number: it is the largest `n_hits` the MCP event
/// search tool itself accepts (`n_hits must be between 1 and 50`), so an
/// internal consumer may out-fetch `max_results` but never spends more ranking
/// budget than the tool path's own documented maximum.
const SESSION_SEARCH_HIT_BUDGET_MAX: u16 = 50;

/// The internal hit budget for a page of `limit` ranked sessions.
///
/// Three shapes, in `limit` order, and the reason for each:
///
/// * `limit <= 12` — the full `SESSION_SEARCH_HITS_PER_SESSION` fan-in.
/// * `13 <= limit <= 33` — [`SESSION_SEARCH_HIT_BUDGET_MAX`] binds and the
///   effective factor DECAYS as `50 / limit` (4.0 down to 1.52). This is the
///   deliberate degradation: a bounded ranking cost is worth more than a
///   constant fan-in, and it is what keeps the shipped default shape
///   (`limit = 25`) at a candidate window of 153 instead of the hard ceiling.
/// * `limit >= 34` — the decay would take the factor to 1.0, so a FLOOR of
///   `1.5x` takes over. That floor is not cosmetic: the budget must exceed
///   `limit`, or `ranked.hits` bounds the distinct sessions at `limit`,
///   `truncated: ranked_sessions > limit` becomes structurally unsettable, and
///   the fan-in this function exists for collapses to 1:1 at exactly the
///   largest page a caller may ask for. `.max(limit + 1)` states that
///   requirement literally rather than leaving it implied by the arithmetic.
///
/// Over the whole reachable domain `1..=SESSION_SEARCH_MAX_LIMIT` this yields a
/// budget in `4..=75`, always strictly greater than `limit`, and therefore a
/// candidate window `C = mcp_candidate_fetch_size(budget + 1)` in `15..=228` —
/// strictly below [`super::search_canonical::MCP_SEARCH_CANDIDATE_MAX`], which
/// stays a guard against a raised cap rather than a number requests land on.
/// `session_search_hit_budget_holds_its_bounds_across_the_whole_reachable_domain`
/// asserts all of it, per `limit`.
pub(super) fn session_search_hit_budget(limit: u16) -> u16 {
    // 1.5x, rounded UP so an odd `limit` cannot land just under the floor, and
    // never below `limit + 1` (which is what 1.5x rounds to at `limit = 1`).
    let fan_in_floor = (limit.saturating_mul(3).saturating_add(1) / 2).max(limit.saturating_add(1));
    limit
        .saturating_mul(SESSION_SEARCH_HITS_PER_SESSION)
        .min(SESSION_SEARCH_HIT_BUDGET_MAX)
        .max(fan_in_floor)
}

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
        let events_source = self.live_events_source();
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

    /// Session DISCOVERY. MCP `list_sessions` and the monitor session feed both
    /// page through it (issue-599 §1.1), which is why the `mcp_` prefix is
    /// historical and the operation carries no MCP-specific behavior.
    ///
    /// **Cursor contract (issue-599 §1.2).** The token is a VALUE anchor over
    /// `(updated_at, session_id)` — the `updated_at` the page ORDERS by and the
    /// same one it REPORTS, because both are [`SessionKeyset::last_unix_ms`]
    /// (`docs/mcp-search-interface-spec.md`: "The response is always sorted by
    /// the `updated_at` it reports"). It is not a snapshot. A session that receives
    /// events while a caller pages moves its `updated_at` ahead of the anchor
    /// and will not be seen again on a later page; restarting from page 1
    /// observes the new order. Unrelated appends never invalidate the cursor.
    /// A session can repeat across a paging run only if its `updated_at` moves
    /// backwards, which append-only ingest cannot do under a fixed generation
    /// — across a #602 generation replay it can, and that duplicate is
    /// accepted rather than de-duplicated with server-side state.
    pub(super) async fn list_mcp_sessions_impl(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        Self::validate_required_time_bounds(filter.start_unix_ms, filter.end_unix_ms)?;

        let limit = page.normalized_limit(self.cfg.max_results);
        let filter_sig = self.mcp_session_list_filter_sig(&filter);
        let sort = filter.sort;
        // Canonical-first when the issue-598 backfill has published coverage;
        // otherwise the projected-header path still serves the page. The
        // fallback is short-lived by design — it is deleted once the live
        // gates are green (issue-599 open question 4).
        //
        // The token is decoded against the SERVING path's version, so a
        // readiness flip mid-pagination surfaces as a cursor mismatch rather
        // than a silent gap.
        if self.canonical_list_path_ready().await {
            let cursor = Self::decode_session_list_cursor(
                page.cursor.as_deref(),
                &filter_sig,
                sort,
                MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY,
            )?;
            self.list_mcp_sessions_directory(&filter, filter_sig, sort, limit, cursor)
                .await
        } else {
            let cursor = Self::decode_session_list_cursor(
                page.cursor.as_deref(),
                &filter_sig,
                sort,
                MCP_SESSION_LIST_CURSOR_VERSION_HEADERS,
            )?;
            self.list_mcp_sessions_headers(&filter, filter_sig, sort, limit, cursor)
                .await
        }
    }

    /// Decode and validate a `list_sessions` continuation token.
    ///
    /// `serving_version` is the version the path about to use this token mints.
    /// A token minted by the OTHER path anchors on a different value (see
    /// [`McpSessionListCursor`]), so resuming it here would silently skip every
    /// session whose two anchors straddle it. It is rejected as a cursor
    /// mismatch instead, which a caller can recover from by restarting the
    /// feed. The mismatch messages are contract surface asserted by the tool
    /// tests.
    fn decode_session_list_cursor(
        token: Option<&str>,
        filter_sig: &str,
        sort: ConversationListSort,
        serving_version: u8,
    ) -> RepoResult<Option<McpSessionListCursor>> {
        let Some(token) = token else {
            return Ok(None);
        };
        let cursor: McpSessionListCursor = decode_cursor(token)?;
        if !ACCEPTED_MCP_SESSION_LIST_CURSOR_VERSIONS.contains(&cursor.version) {
            return Err(RepoError::invalid_cursor(format!(
                "unsupported list_sessions cursor version {}",
                cursor.version
            )));
        }
        if cursor.version != serving_version {
            return Err(RepoError::invalid_cursor(
                "cursor was minted by a different list_sessions read path",
            ));
        }
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
        Ok(Some(cursor))
    }

    fn mint_session_list_cursor(
        last_event_unix_ms: i64,
        session_id: &str,
        filter_sig: String,
        sort: ConversationListSort,
        version: u8,
    ) -> RepoResult<String> {
        encode_cursor(&McpSessionListCursor {
            version,
            last_event_unix_ms,
            session_id: session_id.to_string(),
            filter_sig,
            sort,
        })
    }

    /// The issue-599 canonical-first discovery page: content-free candidate
    /// selection from `mcp_session_directory` (Phase A), batched hydration of
    /// the bounded candidate chunks (Phase B), then the exact re-filter, title
    /// fold, trim, and cursor mint (Phase C).
    ///
    /// **Phase A runs exactly once.** Its keyset predicate is post-aggregation
    /// and the directory's sort key leads with `session_id`, so re-running it
    /// with an advanced keyset would re-aggregate the whole directory without
    /// pruning anything. The single pass therefore over-fetches the page's
    /// entire hydration budget ([`candidate_fetch_size`]) and only HYDRATION is
    /// chunked. Worst case per page: `1 + MAX_HYDRATION_CHUNKS × 3 = 13`
    /// statements.
    ///
    /// Chunked hydration exists because the directory's recall filters are
    /// inexact: a chunk can be eliminated wholesale by the exact re-filter and
    /// the page must still make progress. When the budget runs out with
    /// candidates remaining, the page returns what survived WITH a continuation
    /// cursor (possibly on an empty `items`) rather than a short page that
    /// reads as "no more results".
    async fn list_mcp_sessions_directory(
        &self,
        filter: &McpSessionListFilter,
        filter_sig: String,
        sort: ConversationListSort,
        limit: u16,
        cursor: Option<McpSessionListCursor>,
    ) -> RepoResult<Page<McpSessionListItem>> {
        let fetch = candidate_fetch_size(limit);
        let sql = {
            let params = DirectoryPageParams {
                start_unix_ms: filter.start_unix_ms,
                end_unix_ms: filter.end_unix_ms,
                mode: filter.mode,
                harness: filter.harness.as_deref(),
                source_name: filter.source_name.as_deref(),
                sort,
                after: cursor
                    .as_ref()
                    .map(|cursor| (cursor.last_event_unix_ms, cursor.session_id.as_str())),
                limit: fetch,
            };
            self.build_session_directory_page_sql(&params)
        };
        let candidates: Vec<DirectoryCandidateRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        // A short candidate page proves the directory holds nothing further
        // under this filter and keyset.
        let directory_exhausted = candidates.len() < fetch as usize;

        let mut survivors: Vec<McpSessionListItem> = Vec::new();
        let mut last_resolved: Option<(i64, String)> = None;
        for batch in candidates.chunks(hydration_chunk_size(limit) as usize) {
            if survivors.len() > usize::from(limit) {
                // The page is full and provably has more; the untouched tail of
                // the over-fetch is resolved by the next request.
                break;
            }
            let session_ids: Vec<String> = batch
                .iter()
                .map(|candidate| candidate.session_id.clone())
                .collect();
            let hydrated = self.hydrate_session_list_chunk(&session_ids).await?;
            survivors.extend(batch.iter().filter_map(|candidate| {
                Self::session_list_item(
                    candidate,
                    &hydrated,
                    filter,
                    self.cfg.session_scope.as_ref(),
                )
            }));
            last_resolved = batch
                .last()
                .map(|candidate| (candidate.cand_last_ms, candidate.session_id.clone()));
        }

        // Phase A ordered CANDIDATES; hydration is unordered, so re-establish
        // the keyset order here. `last_event_unix_ms` IS the directory value
        // Phase A ordered and filtered by (`SessionKeyset`), which is what lets
        // the sort, the cursor and the response all read one field — and what
        // makes the page sorted by the `updated_at` it reports.
        survivors.sort_by(|a, b| {
            let ordering = a
                .last_event_unix_ms
                .cmp(&b.last_event_unix_ms)
                .then_with(|| a.session_id.cmp(&b.session_id));
            match sort {
                ConversationListSort::Desc => ordering.reverse(),
                ConversationListSort::Asc => ordering,
            }
        });
        let has_more_survivors = survivors.len() > usize::from(limit);
        survivors.truncate(usize::from(limit));

        let next_cursor = if has_more_survivors {
            // Anchored on the LAST KEPT item, so the trailing survivors this
            // page dropped are served by the next one.
            survivors
                .last()
                .map(|item| {
                    Self::mint_session_list_cursor(
                        item.last_event_unix_ms,
                        &item.session_id,
                        filter_sig,
                        sort,
                        MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY,
                    )
                })
                .transpose()?
        } else if !directory_exhausted {
            // The hydration budget ran out with directory candidates left, so
            // the page returns short (possibly empty) but WITH a continuation.
            //
            // The anchor is the last RESOLVED CANDIDATE, not the last kept
            // item: every candidate up to it has already been hydrated and
            // judged, so resuming strictly after it loses nothing, and it is
            // what guarantees forward progress. Anchoring on the last kept item
            // instead would make the next request re-examine the same rejects
            // and, when a filter is selective enough to eliminate the whole
            // budget, hand back the identical cursor forever.
            last_resolved
                .as_ref()
                .map(|(last_ms, session_id)| {
                    Self::mint_session_list_cursor(
                        *last_ms,
                        session_id,
                        filter_sig,
                        sort,
                        MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY,
                    )
                })
                .transpose()?
        } else {
            None
        };

        Ok(Page {
            items: survivors,
            next_cursor,
        })
    }

    /// The directory `updated_at` for a bounded id set a NON-paging selector
    /// chose, keyed by session id.
    ///
    /// The time-ordered page gets this for free from Phase A; content ranking
    /// has no Phase A and buys it here, so that both surfaces report the value
    /// the feed also orders and pages by rather than each deriving its own. One
    /// PK-pruned, content-free statement — see
    /// [`Self::build_session_keyset_batch_sql`].
    async fn session_keyset_batch(
        &self,
        session_ids: &[String],
    ) -> RepoResult<HashMap<String, SessionKeysetBatchRow>> {
        if session_ids.is_empty() {
            return Ok(HashMap::default());
        }
        let sql = self.build_session_keyset_batch_sql(session_ids);
        let rows: Vec<SessionKeysetBatchRow> =
            self.map_backend(self.query_rows(&sql, None).await)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.session_id.clone(), row))
            .collect())
    }

    /// Phase B (issue-599 §1.4): three batched statements hydrate the whole
    /// candidate chunk. Never a per-session loop — 3 × K sequential round trips
    /// cannot fit a 3 s tool deadline.
    async fn hydrate_session_list_chunk(
        &self,
        session_ids: &[String],
    ) -> RepoResult<HashMap<String, HydratedSession>> {
        if session_ids.is_empty() {
            return Ok(HashMap::default());
        }
        let totals_sql = self.build_session_totals_batch_sql(session_ids);
        let totals: Vec<SessionTotalsBatchRow> =
            self.map_backend(self.query_rows(&totals_sql, None).await)?;
        let mut hydrated: HashMap<String, HydratedSession> = totals
            .into_iter()
            .map(|totals| {
                (
                    totals.session_id.clone(),
                    HydratedSession {
                        totals,
                        metadata: Vec::new(),
                        completed: false,
                    },
                )
            })
            .collect();
        // Every candidate failed the published-generation join: nothing to
        // hydrate, and the two remaining statements would read nothing.
        if hydrated.is_empty() {
            return Ok(hydrated);
        }

        let metadata_sql = self.build_session_metadata_batch_sql(session_ids);
        let metadata: Vec<SessionMetaBatchRow> =
            self.map_backend(self.query_rows(&metadata_sql, None).await)?;
        for row in metadata {
            if let Some(session) = hydrated.get_mut(&row.session_id) {
                session.metadata.push(MetaRow {
                    event_ts: row.event_ts,
                    event_uid: row.event_uid,
                    event_kind: row.event_kind,
                    payload_json: row.payload_json,
                });
            }
        }

        let terminal_sql = self.build_session_terminal_batch_sql(session_ids);
        let terminal: Vec<SessionTerminalBatchRow> =
            self.map_backend(self.query_rows(&terminal_sql, None).await)?;
        for row in terminal {
            if let Some(session) = hydrated.get_mut(&row.session_id) {
                session.completed = row.completed != 0;
            }
        }

        Ok(hydrated)
    }

    /// Session DISCOVERY BY CONTENT (issue-599 WI-09).
    ///
    /// Candidate selection is issue #597's bounded postings ranking, reached
    /// through the SAME repository entry point MCP `search_sessions` uses, so
    /// there is one ranking, one corpus-stats cache and one candidate-budget
    /// contract rather than a monitor-only copy. Everything after ranking is
    /// [`Self::list_mcp_sessions_impl`]'s hydration and visibility rules, on
    /// whichever read model that operation is currently serving — so the two
    /// discovery surfaces cannot disagree about who may be served, what a
    /// session is called, or which harness it ran under.
    ///
    /// **Readiness is branched on exactly like the sibling list path.** While
    /// the issue-598 canonical read indexes are unpublished, `mcp_event_navigation`
    /// is empty and hydrating from it would answer every query with "nothing in
    /// the corpus matched" — a confident falsehood, served with `ok: true`,
    /// while `/api/v1/sessions` was still serving the same sessions from the
    /// projected headers. The not-ready arm hydrates from
    /// `mcp_open_publication_headers` instead, which is what
    /// [`Self::list_mcp_sessions_headers`] reads, so session summaries stay
    /// available throughout the cutover (docs/monitor-http-api.md).
    ///
    /// **Readiness is resolved ONCE, at the top, and threaded into both
    /// stages.** Ranking branches on the latch to pick its engine and hydration
    /// branches on it to pick its read model. Probing twice costs a second
    /// `mcp_read_index_state` point read on every pre-cutover search, and — the
    /// reason that matters — the latch flips when a backfill publishes, so two
    /// probes in one request can disagree and produce an answer ranked over the
    /// `mcp_open_*` projection and then hydrated from the canonical navigation
    /// index. `list_mcp_sessions_impl` branches once and cannot straddle the
    /// flip; this must not be weaker.
    ///
    /// Statement budget: 1 readiness point read (skipped once the latch is
    /// positive for the process), ranking at most 9 (issue #597 §1), then the
    /// directory keyset batch + hydration — 4 (directory) or 1 (headers) — so
    /// at most 14, bounded well inside the Interactive `statement_cap` even at
    /// `SESSION_SEARCH_MAX_LIMIT`.
    ///
    /// Ranking hydrates message text to build snippets. **None of it survives
    /// this function** — the only thing carried out of `ranked` is the ordered
    /// list of session ids.
    pub(super) async fn search_session_summaries_impl(
        &self,
        query: SessionSearchQuery,
    ) -> RepoResult<SessionSearchResults> {
        let canonical_ready = self.canonical_list_path_ready().await;
        let max_results = self.cfg.max_results.max(1);
        // Two clamps: the operation's own documented bound, then the backend's
        // `max_results`. The first is what makes `limit` — and therefore every
        // budget derived from it below — bounded by this crate rather than by
        // whichever transport happened to call in.
        let limit = query
            .limit
            .unwrap_or(SESSION_SEARCH_DEFAULT_LIMIT)
            .clamp(1, SESSION_SEARCH_MAX_LIMIT)
            .min(max_results);
        // Several ranked hits routinely land in one session, so asking the
        // ranking for exactly `limit` hits answers with far fewer than `limit`
        // sessions. The over-fetch is an INTERNAL budget and is passed as the
        // ranking's own ceiling: clamping it to the caller-facing `max_results`
        // (as this did) makes it inert, because `max_results` is also what
        // `limit` is clamped to, so the two collapse to the same number in
        // every shipped configuration.
        let hit_budget = session_search_hit_budget(limit);

        let ranked = self
            .search_mcp_events_ranked(
                SearchMcpEventsQuery {
                    query: query.query.clone(),
                    cancellation_token: query.cancellation_token.clone(),
                    n_hits: Some(hit_budget),
                    // Whole-corpus by construction: a session or turn scope here
                    // would make this a within-session search, which is `open`'s
                    // job, not discovery's.
                    session_id: None,
                    turn_seq: None,
                    event_types: None,
                    harness: query.harness.clone(),
                    source_name: query.source_name.clone(),
                    min_score: None,
                    min_should_match: None,
                },
                McpEventRankingOptions {
                    hit_cap: hit_budget,
                    canonical_ready: Some(canonical_ready),
                },
            )
            .await?;

        // Distinct sessions in RANK order: a session's best hit is its rank.
        // Ranking is EVENT-grained and a matching session is normally matched
        // several times, so without this a session is rendered once per hit and
        // `result_count` counts hits while claiming to count sessions.
        let mut ranked_session_ids: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for hit in &ranked.hits {
            if hit.session_id.trim().is_empty() {
                continue;
            }
            if seen.insert(hit.session_id.as_str()) {
                ranked_session_ids.push(hit.session_id.clone());
            }
        }
        let ranked_sessions = ranked_session_ids.len();
        ranked_session_ids.truncate(usize::from(limit));
        let considered = ranked_session_ids.len();

        // ONE hydration round, not a chunk loop: `hydration_chunk_size(limit)`
        // is at least `limit + 1` and the id list is at most `limit`, so the
        // LIST path's chunking would always produce exactly one batch here.
        // (LIST needs the loop because its Phase A over-fetches a whole
        // multi-chunk candidate budget; a ranking hands back at most `limit`.)
        //
        // Ranked order is the response order in BOTH arms, so each folds over
        // the ranked id sequence and not over the (unordered) hydration map.
        //
        // The SAME verdict the ranking used, never a fresh probe.
        let sessions = if canonical_ready {
            // The directory value FIRST, for the same reason the time-ordered
            // page derives it first: it is this operation's `updated_at`, and
            // an id with no row here has no live published generation — the
            // canonical replacement for the retired `tombstone` column, and
            // exactly what would keep it out of Phase A on the feed. Dropping
            // it here is what makes the two surfaces agree about visibility as
            // well as about the value, and hydration is narrowed to what
            // survives so no statement pays for a session that cannot be
            // served.
            let keysets = self.session_keyset_batch(&ranked_session_ids).await?;
            let live_ids: Vec<String> = ranked_session_ids
                .iter()
                .filter(|session_id| keysets.contains_key(session_id.as_str()))
                .cloned()
                .collect();
            let hydrated = self.hydrate_session_list_chunk(&live_ids).await?;
            ranked_session_ids
                .iter()
                .filter_map(|session_id| {
                    Self::hydrated_session_summary(
                        session_id,
                        keysets.get(session_id.as_str())?.keyset(),
                        &hydrated,
                        self.cfg.session_scope.as_ref(),
                        query.harness.as_deref(),
                        query.source_name.as_deref(),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            let mut headers = self
                .hydrate_session_headers(
                    &ranked_session_ids,
                    query.harness.as_deref(),
                    query.source_name.as_deref(),
                )
                .await?;
            ranked_session_ids
                .iter()
                .filter_map(|session_id| headers.remove(session_id.as_str()))
                .collect::<Vec<_>>()
        };

        let bounds = SessionSearchBounds::derive(
            ranked_sessions,
            limit,
            considered,
            sessions.len(),
            ranked.truncated,
        );

        Ok(SessionSearchResults {
            query_id: ranked.query_id,
            query: ranked.query,
            terms: ranked.terms,
            sessions,
            truncated: bounds.truncated,
            hits_truncated: bounds.hits_truncated,
            incomplete: ranked.incomplete_due_to_candidate_budget,
            dropped: bounds.dropped,
        })
    }

    /// Hydrate a bounded set of session ids from the PROJECTED HEADERS, keyed
    /// by session id.
    ///
    /// The not-ready counterpart to [`Self::hydrate_session_list_chunk`]. One
    /// statement, and its visibility predicates come from
    /// [`Self::header_visibility_clauses`] — the same builder
    /// [`Self::list_mcp_sessions_headers`] uses — so a session the feed would
    /// serve and a session a search may disclose are decided by one rule set.
    ///
    /// An id with no row in the answer was dropped (tombstoned, out of scope,
    /// wrong harness/source, unpublished); the caller reports that rather than
    /// silently returning a shorter page.
    async fn hydrate_session_headers(
        &self,
        session_ids: &[String],
        harness: Option<&str>,
        source_name: Option<&str>,
    ) -> RepoResult<HashMap<String, McpSessionListItem>> {
        if session_ids.is_empty() {
            return Ok(HashMap::default());
        }
        let mut clauses = self.header_visibility_clauses(harness, source_name);
        clauses.push(format!(
            "s.session_id IN {}",
            sql_array_strings(session_ids)
        ));
        let sql = self.build_session_headers_sql(
            &clauses.join("\n  AND "),
            "s.session_id ASC",
            session_ids.len(),
        );
        let rows: Vec<McpSessionListRow> = self.map_backend(self.query_rows(&sql, None).await)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.session_id.clone(), Self::map_mcp_session_list_row(row)))
            .collect())
    }

    /// One hydrated session folded into the summary both discovery paths serve,
    /// or dropped.
    ///
    /// **This is the single authority for who may be served at all, and for
    /// what a session's facets are, on the canonical read model.** The blank id
    /// rule, the tombstone rule, the exact project-scope re-check and the exact
    /// harness/source equality live here and nowhere else, so time-ordered LIST
    /// and content-ranked SEARCH cannot disagree about a session's visibility
    /// or about whether it belongs to a requested harness. A second copy would
    /// be a second place for it to rot — and a scope check that rots discloses
    /// sessions.
    ///
    /// `harness` / `source_name` are re-checked EXACTLY because neither
    /// candidate pass can decide them. LIST's directory predicate is
    /// `has(groupUniqArray(harness), …)` over the session's whole history;
    /// SEARCH's ranking predicate is `p.harness = …` on a single POSTING. The
    /// summary's own `harness` is `argMax(…)` — the session's LATEST value — so
    /// both passes admit a session whose current harness differs from the one
    /// asked for, and rendering it would show a card whose `harness` field
    /// contradicts the filter that produced it.
    ///
    /// **`updated_at` is the caller's [`SessionKeyset`], on BOTH paths, and it
    /// is a required argument for exactly that reason.** The time-ordered page
    /// can only ORDER and keyset by the directory aggregate — it is the only
    /// value its next `HAVING` can compare against — so that is what it
    /// reports, and a ranked search buys the same value for its winners
    /// ([`ClickHouseConversationRepository::build_session_keyset_batch_sql`])
    /// rather than deriving a second one from hydration. Two quantities
    /// answering "when did this session last have an event" is what made the
    /// page unsorted by the field it returns AND made one session render two
    /// ways: `endedAt` drives the monitor's `active`/`completed` derivation, so
    /// a 60 s difference is a contradiction in words, not in rounding
    /// (issue-599 B1).
    ///
    /// `first_event_*` stays hydrated: nothing orders or pages by it, and the
    /// exact `FINAL`-deduped lower bound is the better answer for `started_at`.
    fn hydrated_session_summary(
        session_id: &str,
        keyset: SessionKeyset<'_>,
        hydrated: &HashMap<String, HydratedSession>,
        session_scope: Option<&SessionOriginScope>,
        harness: Option<&str>,
        source_name: Option<&str>,
    ) -> Option<McpSessionListItem> {
        // Belt-and-braces with the SQL guard: a blank session_id never appears,
        // never consumes a LIMIT slot, and never anchors a cursor. mcp-core
        // applies the same `trim().is_empty()` rejection and the two halves
        // must agree, or the contiguous-`rank` and `result_count` invariants
        // break.
        if session_id.trim().is_empty() {
            return None;
        }
        // A candidate with no live navigation rows under the pinned
        // publication has no totals row at all — the canonical replacement for
        // the retired `tombstone` column (issue-599 §2.7).
        let session = hydrated.get(session_id)?;
        let totals = &session.totals;
        if totals.total_events == 0 {
            return None;
        }
        // Project scope is re-checked EXACTLY here, against the hydrated
        // `origin_cwd`. For LIST the candidate pass is the directory's
        // `argMinIfMerge(origin_cwd_state)`, merged over live-generation rows
        // rather than the `FINAL`-deduped navigation rows — a genuine recall
        // filter. For SEARCH the ranking's directory subquery is recall too,
        // and issue #597's own Phase 4 already re-checks the same value; this
        // is then redundant with it and kept anyway, so that ONE function
        // decides disclosure for both surfaces instead of each surface owning
        // a copy. Scope decides what a caller may see, so a false positive is
        // not acceptable and it does not rest on recall.
        if let Some(scope) = session_scope {
            let cwd = totals.origin_cwd.as_str();
            let in_scope = scope
                .roots
                .iter()
                .any(|root| cwd == root.as_str() || cwd.starts_with(&format!("{root}/")));
            if !in_scope {
                return None;
            }
        }
        // Exact, case-sensitive equality against the hydrated aggregates. Both
        // candidate passes are recall-only here (see the function doc).
        if harness.is_some_and(|harness| totals.harness != harness) {
            return None;
        }
        if source_name.is_some_and(|source_name| totals.source != source_name) {
            return None;
        }

        let precedence = session.precedence();
        let (title, session_summary) = list_title_and_summary(&totals.source, &precedence);
        Some(McpSessionListItem {
            session_id: session_id.to_string(),
            first_event_time: totals.first_event_time.clone(),
            first_event_unix_ms: totals.first_event_unix_ms,
            last_event_time: keyset.last_time.to_string(),
            last_event_unix_ms: keyset.last_unix_ms,
            total_turns: total_turns_from_parts(
                totals.total_events,
                totals.max_override,
                totals.counter_user_messages,
            ),
            total_events: totals.total_events,
            mode: Self::parse_mode(&totals.mode),
            completed: session.completed,
            title: non_empty_string(title),
            source: non_empty_string(totals.source.clone()),
            harness: non_empty_string(totals.harness.clone()),
            inference_provider: non_empty_string(totals.inference_provider.clone()),
            session_slug: non_empty_string(precedence.session_slug),
            session_summary: non_empty_string(session_summary),
            tool_calls: totals.tool_calls,
        })
    }

    /// Phase C (issue-599 §1.5): re-apply the exact filters to one candidate's
    /// hydrated values and fold it into a list item, or drop it. The candidate
    /// carries the [`SessionKeyset`] the item reports, so ordering, cursor
    /// minting and rendering all read one number and cannot drift apart.
    fn session_list_item(
        candidate: &DirectoryCandidateRow,
        hydrated: &HashMap<String, HydratedSession>,
        filter: &McpSessionListFilter,
        session_scope: Option<&SessionOriginScope>,
    ) -> Option<McpSessionListItem> {
        let session_id = candidate.session_id.as_str();
        let item = Self::hydrated_session_summary(
            session_id,
            candidate.keyset(),
            hydrated,
            session_scope,
            filter.harness.as_deref(),
            filter.source_name.as_deref(),
        )?;
        let totals = &hydrated.get(session_id)?.totals;
        // Both passes are published-filtered (Phase A carries the same
        // `(host, name, file, generation) IN published` tuple-IN), so this is
        // not a generation-visibility re-check. It refines the window on the
        // EXACT hydrated bounds, which can sit inside the directory's
        // aggregate when a re-inserted event lowered a display time.
        if totals.last_event_unix_ms < filter.start_unix_ms
            || totals.first_event_unix_ms >= filter.end_unix_ms
        {
            return None;
        }
        // Exact, case-sensitive equality against the hydrated aggregates: the
        // directory's `mode_hint` predicate is a recall filter only. Harness and
        // source are re-checked in the shared fold above, because SEARCH needs
        // the identical check and a second copy is a second place to rot.
        if filter.mode.is_some_and(|mode| totals.mode != mode.as_str()) {
            return None;
        }
        Some(item)
    }

    /// The projected-header VISIBILITY rules, in one place.
    ///
    /// This is the header read model's counterpart to
    /// [`Self::hydrated_session_summary`]. On the headers these values are
    /// materialized and exact — `origin_cwd`, `harness` and `source` are
    /// columns, not recall hints — so the rules are enforced in SQL rather than
    /// re-checked in Rust, which is what the pre-#599 list path has always
    /// done. Both discovery surfaces build their header predicate from here, so
    /// one rule set decides who may be served on this read model too.
    fn header_visibility_clauses(
        &self,
        harness: Option<&str>,
        source_name: Option<&str>,
    ) -> Vec<String> {
        let mut clauses = vec![
            // A blank session_id is never a real session (e.g. the orphan
            // Workflow-journal events ingested before #386's exclusion). Drop
            // them here so they never consume a LIMIT slot or anchor the keyset
            // cursor. `notEmpty(trimBoth(...))` mirrors the MCP contract's
            // `trim().is_empty()` rejection so the repo filter and the mcp-core
            // skip agree on what counts as blank.
            "s.tombstone = 0".to_string(),
            "notEmpty(trimBoth(s.session_id))".to_string(),
        ];
        if let Some(scope) = self.cfg.session_scope.as_ref() {
            let roots = scope
                .roots
                .iter()
                .map(|root| {
                    format!(
                        "s.origin_cwd = {root} OR startsWith(s.origin_cwd, {prefix})",
                        root = sql_quote(root),
                        prefix = sql_quote(&format!("{root}/")),
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            clauses.push(format!("({roots})"));
        }
        if let Some(harness) = harness {
            clauses.push(format!("s.harness = {}", sql_quote(harness)));
        }
        if let Some(source_name) = source_name {
            clauses.push(format!("s.source = {}", sql_quote(source_name)));
        }
        clauses
    }

    /// The projected-header projection, shared by the time-ordered feed and the
    /// ranked search.
    ///
    /// Callers differ only in `where_sql`, `order_sql` and `limit`. Everything
    /// that decides which header REVISION is current and whether it may be
    /// served at all — publication source-head authorization, dirty-revision
    /// fencing, the `LIMIT 1 BY session_id` collapse — is single-sourced here.
    ///
    /// Listing is a moving-feed read over the already materialized session
    /// headers. Source-head authorization pins each candidate to this
    /// operation's publication snapshot, while the dirty revision prevents a
    /// header from being served after its canonical session changes. This
    /// avoids rebuilding all list metadata from the canonical event corpus on
    /// every page.
    fn build_session_headers_sql(&self, where_sql: &str, order_sql: &str, limit: usize) -> String {
        let snapshot = require_active_publication_snapshot("projected MCP session list reads");
        let headers = self.table_ref("mcp_open_publication_headers");
        let history = self.table_ref("v_published_source_generation_history");
        let dirty_sessions = self.table_ref("mcp_open_dirty_sessions");
        let captured_heads = snapshot.captured_source_heads_sql(&history);

        format!(
            "WITH
  {captured_heads} AS captured_heads,
current_dirty AS (
  SELECT
    session_id,
    toUInt64(dirty_revision) AS dirty_revision
  FROM {dirty_sessions} FINAL
  WHERE notEmpty(session_id)
),
authorized_headers AS (
  SELECT
    h.*
  FROM {headers} AS h FINAL
  ANY LEFT JOIN current_dirty AS d ON d.session_id = h.session_id
  WHERE length(h.required_source_heads) > 0
    AND arrayAll(
      required_head -> has(captured_heads, required_head),
      h.required_source_heads
    )
    AND h.dirty_revision = ifNull(d.dirty_revision, toUInt64(0))
),
current_headers AS (
  SELECT *
  FROM authorized_headers
  ORDER BY session_id ASC, header_revision DESC
  LIMIT 1 BY session_id
)
SELECT
  s.session_id AS session_id,
  toString(s.first_event_time) AS first_event_time,
  toInt64(toUnixTimestamp64Milli(s.first_event_time)) AS first_event_unix_ms,
  toString(s.last_event_time) AS last_event_time,
  toInt64(toUnixTimestamp64Milli(s.last_event_time)) AS last_event_unix_ms,
  toUInt32(s.total_turns) AS total_turns,
  toUInt64(s.total_events) AS total_events,
  s.mode AS mode,
  toUInt8(s.completed) AS completed,
  toUInt64(s.tool_calls) AS tool_calls,
  s.list_title AS title,
  s.source AS source,
  s.harness AS harness,
  s.inference_provider AS inference_provider,
  s.session_slug AS session_slug,
  s.list_session_summary AS session_summary
FROM current_headers AS s
WHERE {where_sql}
ORDER BY {order_sql}
LIMIT {limit}
FORMAT JSONEachRow",
        )
    }

    /// The pre-#599 projected-header discovery page, kept behind the
    /// canonical read-index readiness gate for stores whose issue-598 backfill
    /// has not published coverage yet.
    async fn list_mcp_sessions_headers(
        &self,
        filter: &McpSessionListFilter,
        filter_sig: String,
        sort: ConversationListSort,
        limit: u16,
        cursor: Option<McpSessionListCursor>,
    ) -> RepoResult<Page<McpSessionListItem>> {
        let mut where_clauses = self
            .header_visibility_clauses(filter.harness.as_deref(), filter.source_name.as_deref());
        where_clauses.push(format!(
            "toUnixTimestamp64Milli(s.last_event_time) >= {}",
            filter.start_unix_ms
        ));
        where_clauses.push(format!(
            "toUnixTimestamp64Milli(s.first_event_time) < {}",
            filter.end_unix_ms
        ));
        if let Some(mode) = filter.mode {
            where_clauses.push(format!("s.mode = {}", sql_quote(mode.as_str())));
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

        let query = self.build_session_headers_sql(
            &where_sql,
            &format!("s.last_event_time {order_dir}, s.session_id {order_dir}"),
            limit_plus,
        );

        let rows: Vec<McpSessionListRow> = self.map_backend(self.query_rows(&query, None).await)?;

        let mut items: Vec<McpSessionListItem> = rows
            .iter()
            .take(limit as usize)
            .cloned()
            .map(Self::map_mcp_session_list_row)
            .collect();

        let next_cursor = if rows.len() > limit as usize {
            items
                .last()
                .map(|last| {
                    Self::mint_session_list_cursor(
                        last.last_event_unix_ms,
                        &last.session_id,
                        filter_sig,
                        sort,
                        MCP_SESSION_LIST_CURSOR_VERSION_HEADERS,
                    )
                })
                .transpose()?
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
            if let Some(cursor_token) = cursor.publication_token.as_deref() {
                if cursor_token != active_publication_token() {
                    return Err(RepoError::ReadModelChanged);
                }
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
                    publication_token: Some(active_publication_token()),
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
            if let Some(cursor_token) = cursor.publication_token.as_deref() {
                if cursor_token != active_publication_token() {
                    return Err(RepoError::ReadModelChanged);
                }
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
                    publication_token: Some(active_publication_token()),
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

/// The bounded-answer facts a ranked session page reports, and the ONE place
/// they are derived (issue-599 WI-09).
///
/// They are separate because their causes are separate and so are their
/// remedies. `incomplete` is not here: it is issue #597's candidate-budget
/// verdict, propagated verbatim from the ranking with nothing derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionSearchBounds {
    /// SESSION grain: the ranking yielded more distinct sessions than `limit`.
    truncated: bool,
    /// EVENT grain: the ranking filled its hit budget.
    hits_truncated: bool,
    /// The exact post-ranking re-check shortened the answer, unrefilled.
    ///
    /// Never a project-scope removal: both ranking arms apply scope while they
    /// choose candidates, so an out-of-scope session is not in `considered` to
    /// begin with. See [`crate::domain::SessionSearchResults::dropped`] for why
    /// that distinction is a disclosure property and not a detail.
    dropped: bool,
}

impl SessionSearchBounds {
    /// * `ranked_sessions` — distinct sessions the ranking yielded, BEFORE the
    ///   trim to `limit`.
    /// * `considered` — sessions actually hydrated, i.e.
    ///   `min(ranked_sessions, limit)`.
    /// * `disclosed` — sessions that survived the exact re-check.
    /// * `ranking_hit_truncated` — the ranking's own report, at EVENT grain.
    ///
    /// **`truncated` must not absorb `ranking_hit_truncated`.** Ranking is over
    /// events and hits cluster inside a session, so a term appearing thirty
    /// times in ONE session fills the hit budget while yielding one session.
    /// Reporting that as `truncated` tells the reader "more sessions existed
    /// than `limit` returned, raise `limit`" — advice that returns the same one
    /// session forever, and a statement about the corpus that is false.
    fn derive(
        ranked_sessions: usize,
        limit: u16,
        considered: usize,
        disclosed: usize,
        ranking_hit_truncated: bool,
    ) -> Self {
        Self {
            truncated: ranked_sessions > usize::from(limit),
            hits_truncated: ranking_hit_truncated,
            dropped: disclosed < considered,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn totals(session_id: &str, origin_cwd: &str) -> SessionTotalsBatchRow {
        SessionTotalsBatchRow {
            session_id: session_id.to_string(),
            total_events: 12,
            tool_calls: 2,
            max_override: 0,
            counter_user_messages: 3,
            first_event_time: "2026-01-01 10:00:00".to_string(),
            first_event_unix_ms: 1_767_261_600_000,
            last_event_unix_ms: 1_767_262_200_000,
            origin_cwd: origin_cwd.to_string(),
            source: "codex".to_string(),
            harness: "codex".to_string(),
            inference_provider: "openai".to_string(),
            omp_dispatch_title: String::new(),
            mode: "tool_calling".to_string(),
        }
    }

    fn hydrated(rows: Vec<SessionTotalsBatchRow>) -> HashMap<String, HydratedSession> {
        rows.into_iter()
            .map(|totals| {
                (
                    totals.session_id.clone(),
                    HydratedSession {
                        totals,
                        metadata: Vec::new(),
                        completed: false,
                    },
                )
            })
            .collect()
    }

    fn scope(roots: &[&str]) -> Option<SessionOriginScope> {
        SessionOriginScope::from_roots(roots.iter().copied())
    }

    /// The directory `updated_at` every fixture session carries, and its
    /// display form. The SEARCH arm buys it with
    /// `build_session_keyset_batch_sql`; the LIST arm gets it from Phase A.
    const KEYSET_MS: i64 = 1_767_262_260_000;
    const KEYSET_TIME: &str = "2026-01-01 10:11:00";

    fn keyset() -> SessionKeyset<'static> {
        SessionKeyset {
            last_unix_ms: KEYSET_MS,
            last_time: KEYSET_TIME,
        }
    }

    /// The SEARCH arm of the shared fold: no time window, no mode.
    fn searched(
        session_id: &str,
        hydrated: &HashMap<String, HydratedSession>,
        session_scope: Option<&SessionOriginScope>,
        harness: Option<&str>,
        source_name: Option<&str>,
    ) -> Option<McpSessionListItem> {
        ClickHouseConversationRepository::hydrated_session_summary(
            session_id,
            keyset(),
            hydrated,
            session_scope,
            harness,
            source_name,
        )
    }

    fn candidate(session_id: &str) -> DirectoryCandidateRow {
        DirectoryCandidateRow {
            session_id: session_id.to_string(),
            cand_last_ms: KEYSET_MS,
            cand_last_time: KEYSET_TIME.to_string(),
        }
    }

    fn unfiltered_list_filter() -> McpSessionListFilter {
        McpSessionListFilter {
            start_unix_ms: 0,
            end_unix_ms: i64::MAX,
            mode: None,
            sort: ConversationListSort::Desc,
            harness: None,
            source_name: None,
        }
    }

    /// **The scope guard.** A ranked hit is not permission to disclose the
    /// session it sits in: `--project-only` decides that, and it decides it on
    /// the hydrated `origin_cwd`, exactly.
    ///
    /// The fixture is deliberately mixed rather than all-out-of-scope: an
    /// in-scope session, a same-prefix-different-directory sibling (the classic
    /// `startsWith` false positive), a nested child that MUST be kept, and a
    /// session whose harness recorded no cwd at all. A guard that only dropped
    /// the obviously-foreign row would still pass an all-out-of-scope fixture.
    #[test]
    fn search_never_discloses_a_session_outside_the_configured_scope() {
        let scope = scope(&["/work/moraine"]);
        let rows = vec![
            totals("in-scope-root", "/work/moraine"),
            totals("in-scope-child", "/work/moraine/crates/x"),
            // `/work/moraine-other` starts with `/work/moraine` as a STRING but
            // is a different project.
            totals("prefix-sibling", "/work/moraine-other"),
            totals("foreign", "/elsewhere/repo"),
            // A harness that records no cwd matches no root (issue-599 §4).
            totals("no-cwd", ""),
        ];
        let hydrated = hydrated(rows);

        let disclosed: Vec<String> = [
            "in-scope-root",
            "in-scope-child",
            "prefix-sibling",
            "foreign",
            "no-cwd",
        ]
        .into_iter()
        .filter_map(|session_id| searched(session_id, &hydrated, scope.as_ref(), None, None))
        .map(|item| item.session_id)
        .collect();

        assert_eq!(disclosed, vec!["in-scope-root", "in-scope-child"]);
    }

    /// The unscoped backend is the control arm: without it, a fold that dropped
    /// EVERY session would pass the guard above.
    #[test]
    fn an_unscoped_backend_discloses_every_hydrated_session() {
        let hydrated = hydrated(vec![
            totals("a", "/work/moraine"),
            totals("b", "/elsewhere/repo"),
            totals("c", ""),
        ]);
        let disclosed: Vec<String> = ["a", "b", "c"]
            .into_iter()
            .filter_map(|session_id| searched(session_id, &hydrated, None, None, None))
            .map(|item| item.session_id)
            .collect();
        assert_eq!(disclosed, vec!["a", "b", "c"]);
    }

    /// **The facet guard.** `harness` / `source` reach candidate selection as
    /// recall predicates on relations that are not the summary's own value:
    /// LIST pushes `has(groupUniqArray(harness), …)` over the session's whole
    /// history, SEARCH pushes `p.harness = …` on a single POSTING. The summary
    /// reports `argMax(harness)` — the LATEST value. So both passes admit a
    /// session that later renders under a different harness, and only the exact
    /// re-check can drop it.
    ///
    /// MUTATION: delete either `is_some_and` block in
    /// `hydrated_session_summary`; the narrowed arms below then disclose
    /// `re-harnessed` / `re-sourced` and this fails.
    #[test]
    fn both_discovery_paths_drop_a_session_whose_current_facet_differs() {
        let mut re_harnessed = totals("re-harnessed", "/work/moraine");
        re_harnessed.harness = "omp".to_string();
        let mut re_sourced = totals("re-sourced", "/work/moraine");
        re_sourced.source = "omp".to_string();
        let hydrated = hydrated(vec![
            totals("matches", "/work/moraine"),
            re_harnessed,
            re_sourced,
        ]);
        let ids = ["matches", "re-harnessed", "re-sourced"];

        // Control arm: unnarrowed, every candidate is disclosed, so the arms
        // below can only be shortened by the re-check.
        let unnarrowed: Vec<String> = ids
            .into_iter()
            .filter_map(|session_id| searched(session_id, &hydrated, None, None, None))
            .map(|item| item.session_id)
            .collect();
        assert_eq!(unnarrowed, ids.to_vec());

        let by_harness: Vec<String> = ids
            .into_iter()
            .filter_map(|session_id| searched(session_id, &hydrated, None, Some("codex"), None))
            .map(|item| item.session_id)
            .collect();
        assert_eq!(by_harness, vec!["matches", "re-sourced"]);

        let by_source: Vec<String> = ids
            .into_iter()
            .filter_map(|session_id| searched(session_id, &hydrated, None, None, Some("codex")))
            .map(|item| item.session_id)
            .collect();
        assert_eq!(by_source, vec!["matches", "re-harnessed"]);

        // And the time-ordered path answers identically, because it reaches the
        // same fold rather than owning a copy of the rule.
        let listed: Vec<String> = ids
            .into_iter()
            .filter_map(|session_id| {
                ClickHouseConversationRepository::session_list_item(
                    &candidate(session_id),
                    &hydrated,
                    &McpSessionListFilter {
                        harness: Some("codex".to_string()),
                        ..unfiltered_list_filter()
                    },
                    None,
                )
            })
            .map(|item| item.session_id)
            .collect();
        assert_eq!(listed, by_harness);
    }

    /// Every disclosed summary's own `harness` / `source` equals the value the
    /// request narrowed by. This is the property a reader sees: a card rendered
    /// `source: "omp"` under `?source=pi` is the defect, whatever produced it.
    #[test]
    fn a_narrowed_search_never_renders_a_session_under_another_facet() {
        let mut drifted = totals("drifted", "/work/moraine");
        drifted.harness = "omp".to_string();
        drifted.source = "omp".to_string();
        let hydrated = hydrated(vec![totals("kept", "/work/moraine"), drifted]);

        for (harness, source_name) in [(Some("codex"), None), (None, Some("codex"))] {
            let disclosed = ["kept", "drifted"]
                .into_iter()
                .filter_map(|session_id| {
                    searched(session_id, &hydrated, None, harness, source_name)
                })
                .collect::<Vec<_>>();
            assert!(!disclosed.is_empty(), "fixture must disclose something");
            for item in disclosed {
                if let Some(harness) = harness {
                    assert_eq!(item.harness.as_deref(), Some(harness), "{item:?}");
                }
                if let Some(source_name) = source_name {
                    assert_eq!(item.source.as_deref(), Some(source_name), "{item:?}");
                }
            }
        }
    }

    /// The canonical replacement for the retired `tombstone` column: a session
    /// with no live navigation rows under the pinned publication has no totals
    /// row, or a zero-event one, and is not discoverable by either path.
    #[test]
    fn a_session_with_no_live_rows_is_not_discoverable() {
        let mut zeroed = totals("emptied", "/work/moraine");
        zeroed.total_events = 0;
        let hydrated = hydrated(vec![zeroed]);

        assert!(searched("emptied", &hydrated, None, None, None).is_none());
        assert!(searched("never-hydrated", &hydrated, None, None, None).is_none());
        assert!(searched("   ", &hydrated, None, None, None).is_none());
    }

    /// **The one-`updated_at` guard (issue-599 B1).** Both discovery paths
    /// report the DIRECTORY keyset — the value the time-ordered page also
    /// orders by and mints its cursor from — and neither reports the hydrated
    /// aggregate.
    ///
    /// The fixture's keyset is deliberately 60 s ahead of the hydrated value —
    /// exactly the monitor's activity window — because that is the width at
    /// which a divergence stops being cosmetic: `monitor_session_json` derives
    /// `status` from `last_event_unix_ms`, so two surfaces disagreeing by that
    /// much render one session `active` and `completed` at one instant. With
    /// the two values equal (every ordinary session) neither half of this test
    /// could fail however the code were written.
    ///
    /// MUTATION: report `totals.last_event_unix_ms` instead of
    /// `keyset.last_unix_ms` in `hydrated_session_summary`; the first block
    /// fails.
    #[test]
    fn both_discovery_paths_report_the_directory_keyset_they_page_by() {
        let hydrated = hydrated(vec![totals("sess-a", "/work/moraine")]);
        let filter = unfiltered_list_filter();

        let ranked = searched("sess-a", &hydrated, None, None, None).expect("in-scope session");
        assert_eq!(ranked.last_event_unix_ms, KEYSET_MS);
        assert_eq!(ranked.last_event_time, KEYSET_TIME);
        // The hydrated aggregate is a DIFFERENT number, and is not what either
        // surface reports.
        assert_ne!(ranked.last_event_unix_ms, 1_767_262_200_000);
        // `started_at` still comes from hydration: nothing orders or pages by
        // it, and the exact `FINAL`-deduped bound is the better answer.
        assert_eq!(ranked.first_event_unix_ms, 1_767_261_600_000);

        let listed = ClickHouseConversationRepository::session_list_item(
            &candidate("sess-a"),
            &hydrated,
            &filter,
            None,
        )
        .expect("surviving candidate");
        // The listed item is the SAME object the ranked search returns, field
        // for field. Asserting the whole item, not just the timestamps, is what
        // stops the next field that gets "adjusted for paging" from
        // reintroducing the divergence under a different name.
        assert_eq!(listed, ranked);
    }

    /// **The hit-budget bounds, over the WHOLE reachable domain.**
    ///
    /// `limit` is clamped to `1..=SESSION_SEARCH_MAX_LIMIT` inside
    /// `search_session_summaries_impl`, so this is not a sample of the input
    /// space — it is all of it. Each assertion is one of the properties
    /// `session_search_hit_budget` exists to hold, and the previous shape
    /// (`.max(limit).min(MAX.max(limit))`) violated two of them at every
    /// `limit >= 50`: the budget collapsed to exactly `limit`, which makes the
    /// fan-in 1:1 and `truncated: ranked_sessions > limit` unsettable.
    ///
    /// MUTATION: drop the `.max(fan_in_floor)` and the loop panics at
    /// `limit 34: fan-in 50/34 fell below 1.5x` — the floor assertion, not the
    /// "must exceed" one, and well before `limit = 50`. Drop the
    /// `.min(SESSION_SEARCH_HIT_BUDGET_MAX)` instead and it panics at
    /// `limit 22: window 256 pinned the hard candidate ceiling`, because 22
    /// already derives `min(3 × 89, 256) = 256`.
    ///
    /// Both recipes are stated at the limit where the loop ACTUALLY stops. An
    /// earlier revision named `limit = 50` for both; the loop never gets there,
    /// so a contributor checking the stated limit would look in the wrong place.
    #[test]
    fn session_search_hit_budget_holds_its_bounds_across_the_whole_reachable_domain() {
        use super::super::search_canonical::{mcp_candidate_fetch_size, MCP_SEARCH_CANDIDATE_MAX};

        let mut previous = 0;
        for limit in 1..=SESSION_SEARCH_MAX_LIMIT {
            let budget = session_search_hit_budget(limit);

            // 1. The budget must EXCEED the session limit, or the ranking's own
            //    hit ceiling bounds the distinct sessions at `limit` and
            //    `truncated` can never be set.
            assert!(
                budget > limit,
                "limit {limit}: budget {budget} does not exceed the session limit",
            );
            // 2. The fan-in degrades but never collapses: 4x while the ceiling
            //    allows, and never below 1.5x.
            assert!(
                budget * 2 >= limit * 3,
                "limit {limit}: fan-in {budget}/{limit} fell below 1.5x",
            );
            assert!(
                budget <= limit * SESSION_SEARCH_HITS_PER_SESSION,
                "limit {limit}: budget {budget} exceeds the {SESSION_SEARCH_HITS_PER_SESSION}x \
                 fan-in it is derived from",
            );
            // 3. Monotone in `limit`: asking for more sessions never buys a
            //    smaller ranking.
            assert!(budget >= previous, "limit {limit}: budget went backwards");
            previous = budget;

            // 4. The derived candidate window stays strictly BELOW the hard
            //    ceiling, so that constant remains a guard against a raised cap
            //    rather than the number requests land on.
            let window = mcp_candidate_fetch_size(budget + 1);
            assert!(
                window < MCP_SEARCH_CANDIDATE_MAX,
                "limit {limit}: window {window} pinned the hard candidate ceiling",
            );
        }

        // The three documented shapes, at their boundaries.
        assert_eq!(session_search_hit_budget(1), 4, "full 4x fan-in");
        assert_eq!(session_search_hit_budget(12), 48, "last 4x limit");
        assert_eq!(session_search_hit_budget(13), 50, "the ceiling binds");
        assert_eq!(session_search_hit_budget(25), 50, "the shipped default");
        assert_eq!(
            session_search_hit_budget(33),
            50,
            "last ceiling-bound limit"
        );
        assert_eq!(
            session_search_hit_budget(34),
            51,
            "the 1.5x floor takes over"
        );
        assert_eq!(session_search_hit_budget(50), 75, "the largest page");
        // …and the windows those derive, which is what the cost actually is.
        assert_eq!(
            mcp_candidate_fetch_size(session_search_hit_budget(1) + 1),
            15
        );
        assert_eq!(
            mcp_candidate_fetch_size(session_search_hit_budget(25) + 1),
            153
        );
        assert_eq!(
            mcp_candidate_fetch_size(session_search_hit_budget(50) + 1),
            228
        );
    }

    /// **The bounded-answer derivation.** Each fact is asserted through an
    /// input that sets it and leaves the others clear, so a field wired to a
    /// neighbour, deleted, or inverted has nowhere to hide.
    ///
    /// MUTATION (the shipped defect): restore
    /// `truncated: ranked_sessions > limit || ranking_hit_truncated`; the
    /// `one_session_hit_many_times` case then reports `truncated: true` and
    /// this fails.
    #[test]
    fn the_bounded_answer_facts_are_derived_from_independent_causes() {
        // Everything fitted: two ranked sessions for a requested ten, both
        // disclosed, hit budget not filled.
        let complete = SessionSearchBounds::derive(2, 10, 2, 2, false);
        assert_eq!(
            complete,
            SessionSearchBounds {
                truncated: false,
                hits_truncated: false,
                dropped: false,
            }
        );

        // SESSION grain: eleven ranked sessions, ten returned.
        let more_sessions = SessionSearchBounds::derive(11, 10, 10, 10, false);
        assert!(more_sessions.truncated);
        assert!(!more_sessions.hits_truncated);
        assert!(!more_sessions.dropped);

        // HIT grain, and the case the old `||` got wrong: a term appearing
        // thirty times in ONE session fills the hit budget and yields ONE
        // session. There is nothing more to return at the session grain, and
        // raising `limit` returns the same one session forever.
        let one_session_hit_many_times = SessionSearchBounds::derive(1, 10, 1, 1, true);
        assert!(
            !one_session_hit_many_times.truncated,
            "one ranked session is not 'more sessions existed than limit returned'",
        );
        assert!(one_session_hit_many_times.hits_truncated);
        assert!(!one_session_hit_many_times.dropped);

        // The exact re-check shortened the answer and nothing refilled it.
        let shortened = SessionSearchBounds::derive(4, 10, 4, 1, false);
        assert!(shortened.dropped);
        assert!(!shortened.truncated);
        assert!(!shortened.hits_truncated);

        // A full page that dropped nothing is not "dropped", even though it is
        // exactly `limit` long.
        let exactly_limit = SessionSearchBounds::derive(10, 10, 10, 10, false);
        assert!(!exactly_limit.dropped);
        assert!(!exactly_limit.truncated);

        // And the causes compose rather than mask each other.
        let everything = SessionSearchBounds::derive(11, 10, 10, 3, true);
        assert_eq!(
            everything,
            SessionSearchBounds {
                truncated: true,
                hits_truncated: true,
                dropped: true,
            }
        );
    }
}
