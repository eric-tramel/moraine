use super::canonical_list::{
    candidate_fetch_size, hydration_chunk_size, DirectoryCandidateRow, DirectoryPageParams,
    HydratedSession, SessionMetaBatchRow, SessionTerminalBatchRow, SessionTotalsBatchRow,
};
use super::canonical_open::{list_title_and_summary, total_turns_from_parts, MetaRow};
use super::*;
use crate::cursor::{
    ACCEPTED_MCP_SESSION_LIST_CURSOR_VERSIONS, MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY,
    MCP_SESSION_LIST_CURSOR_VERSION_HEADERS,
};

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

    /// Session DISCOVERY. MCP `list_sessions` pages through it today; the
    /// monitor feed still reads `list_session_analytics` and moves here in
    /// WI-03 (issue-599 §1.1), which is why the `mcp_` prefix is already
    /// historical and the operation carries no MCP-specific behavior.
    ///
    /// **Cursor contract (issue-599 §1.2).** The token is a VALUE anchor over
    /// `(updated_at, session_id)`, not a snapshot. A session that receives
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

        let mut survivors: Vec<SurvivingCandidate> = Vec::new();
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
            survivors.extend(
                batch
                    .iter()
                    .filter_map(|candidate| Self::session_list_item(candidate, &hydrated, filter)),
            );
            last_resolved = batch
                .last()
                .map(|candidate| (candidate.cand_last_ms, candidate.session_id.clone()));
        }

        // Phase A ordered CANDIDATES; hydration is unordered, so re-establish
        // the keyset order here — on the directory value Phase A ordered and
        // filtered by, never on the hydrated timestamp, which the next page's
        // `HAVING` cannot compare against (see `DirectoryCandidateRow`).
        survivors.sort_by(|a, b| {
            let ordering = a
                .keyset_ms
                .cmp(&b.keyset_ms)
                .then_with(|| a.item.session_id.cmp(&b.item.session_id));
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
                .map(|survivor| {
                    Self::mint_session_list_cursor(
                        survivor.keyset_ms,
                        &survivor.item.session_id,
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
            items: survivors
                .into_iter()
                .map(|survivor| survivor.item)
                .collect(),
            next_cursor,
        })
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

    /// Phase C (issue-599 §1.5): re-apply the exact filters to one candidate's
    /// hydrated values and fold it into a list item, or drop it. The candidate's
    /// keyset value travels with the item because ordering and cursor minting
    /// must read it, not the hydrated timestamp.
    fn session_list_item(
        candidate: &DirectoryCandidateRow,
        hydrated: &HashMap<String, HydratedSession>,
        filter: &McpSessionListFilter,
    ) -> Option<SurvivingCandidate> {
        let session_id = candidate.session_id.as_str();
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
        // directory's `mode_hint` / `groupUniqArray` predicates are recall
        // filters only. Project scope is NOT re-applied — Phase A's
        // `argMinIfMerge(origin_cwd_state)` is already the exact rule.
        if filter.mode.is_some_and(|mode| totals.mode != mode.as_str()) {
            return None;
        }
        if filter
            .harness
            .as_deref()
            .is_some_and(|harness| totals.harness != harness)
        {
            return None;
        }
        if filter
            .source_name
            .as_deref()
            .is_some_and(|source_name| totals.source != source_name)
        {
            return None;
        }

        let precedence = session.precedence();
        let (title, session_summary) = list_title_and_summary(&totals.source, &precedence);
        Some(SurvivingCandidate {
            keyset_ms: candidate.cand_last_ms,
            item: McpSessionListItem {
                session_id: session_id.to_string(),
                first_event_time: totals.first_event_time.clone(),
                first_event_unix_ms: totals.first_event_unix_ms,
                // Report the value this page was ORDERED and KEYSET by, not
                // the hydrated exact one: the published contract sorts by the
                // `updated_at` the response reports, and Phase A can only
                // order on the directory aggregate. The two differ only when
                // an event is re-inserted within one live generation with a
                // decreased display time — `SimpleAggregateFunction(max)`
                // cannot retract that, navigation read `FINAL` can — so
                // `cand_last_ms >= totals.last_event_unix_ms`, equal in every
                // ordinary case (issue-599 B1).
                last_event_time: candidate.cand_last_time.clone(),
                last_event_unix_ms: candidate.cand_last_ms,
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
            },
        })
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
        let snapshot = require_active_publication_snapshot("projected MCP session list reads");
        let headers = self.table_ref("mcp_open_publication_headers");
        let history = self.table_ref("v_published_source_generation_history");
        let dirty_sessions = self.table_ref("mcp_open_dirty_sessions");
        let captured_heads = snapshot.captured_source_heads_sql(&history);

        let mut where_clauses = vec![
            // A blank session_id is never a real session (e.g. the orphan
            // Workflow-journal events ingested before #386's exclusion). Drop
            // them here so they never consume a LIMIT slot or anchor the keyset
            // cursor. `notEmpty(trimBoth(...))` mirrors the MCP contract's
            // `trim().is_empty()` rejection so the repo filter and the mcp-core
            // skip agree on what counts as blank.
            "s.tombstone = 0".to_string(),
            "notEmpty(trimBoth(s.session_id))".to_string(),
            format!(
                "toUnixTimestamp64Milli(s.last_event_time) >= {}",
                filter.start_unix_ms
            ),
            format!(
                "toUnixTimestamp64Milli(s.first_event_time) < {}",
                filter.end_unix_ms
            ),
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
            where_clauses.push(format!("({roots})"));
        }
        if let Some(mode) = filter.mode {
            where_clauses.push(format!("s.mode = {}", sql_quote(mode.as_str())));
        }
        if let Some(harness) = filter.harness.as_deref() {
            where_clauses.push(format!("s.harness = {}", sql_quote(harness)));
        }
        if let Some(source_name) = filter.source_name.as_deref() {
            where_clauses.push(format!("s.source = {}", sql_quote(source_name)));
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

        // Listing is a moving-feed read over the already materialized session
        // headers. Source-head authorization pins each candidate to this
        // operation's publication snapshot, while the dirty revision prevents a
        // header from being served after its canonical session changes. This
        // avoids rebuilding all list metadata from the canonical event corpus on
        // every page.
        let query = format!(
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
ORDER BY s.last_event_time {order_dir}, s.session_id {order_dir}
LIMIT {limit_plus}
FORMAT JSONEachRow",
            headers = headers,
            dirty_sessions = dirty_sessions,
            captured_heads = captured_heads,
            where_sql = where_sql,
            order_dir = order_dir,
            limit_plus = limit_plus,
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

/// A Phase-A candidate that survived Phase C, paired with the directory keyset
/// value it was selected by. `keyset_ms` — not `item.last_event_unix_ms` —
/// orders the page and mints the continuation cursor, because it is the only
/// value the next page's Phase A can filter on
/// ([`DirectoryCandidateRow::cand_last_ms`]).
struct SurvivingCandidate {
    keyset_ms: i64,
    item: McpSessionListItem,
}
