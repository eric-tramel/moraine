//! Batched, content-free session DISCOVERY over the migration-036 indexes
//! (issue-599 WI-01).
//!
//! [`canonical_open`](super::canonical_open) is session-scoped: every builder
//! there pins `WHERE n.session_id = '<one literal>'`. Session listing needs the
//! K-session forms of the same statements, so this module supplies them:
//!
//! * **Phase A** — [`ClickHouseConversationRepository::build_session_directory_page_sql`]:
//!   one content-free `GROUP BY session_id` over `mcp_session_directory`,
//!   tuple-`IN`-joined to the #602 published source generations, producing the
//!   candidate keyset page. No wide column, no `events` scan, no session-origin
//!   subquery. **Exactly one such statement runs per page request** — see
//!   [`candidate_fetch_size`].
//! * **Phase B** — the three batched hydration builders
//!   ([`build_session_totals_batch_sql`](ClickHouseConversationRepository::build_session_totals_batch_sql),
//!   [`build_session_metadata_batch_sql`](ClickHouseConversationRepository::build_session_metadata_batch_sql),
//!   [`build_session_terminal_batch_sql`](ClickHouseConversationRepository::build_session_terminal_batch_sql)),
//!   each PK-pruned to the bounded hydration chunk. Only the metadata statement
//!   decompresses `payload_json`, and only for `is_metadata_bearing = 1` rows.
//!   **No statement here reads `text_content`.**
//!
//! Every builder is a pure method so the SQL-shape tests can assert the query
//! contract without a backend. Orchestration (Phase C: exact re-filter, title
//! fold, trim, cursor mint) lives in [`list`](super::list).

use super::canonical_open::{metadata_precedence, MetaRow, MetadataPrecedence};
use super::*;
use moraine_clickhouse::canonical_derivations::{self as cd, DerivationColumns};

/// The migration-036 indexes carry the physical `events` column names.
const COLS: DerivationColumns = DerivationColumns::EVENTS;

/// How many candidate chunks one page request may HYDRATE before it returns
/// what survived with a continuation cursor.
///
/// Only hydration is chunked. Phase A is *not* re-run per chunk: its keyset
/// predicate lives in `HAVING` (post-aggregation) and `mcp_session_directory`
/// leads its sort key with `session_id` (`sql/036:49`), so advancing the keyset
/// prunes nothing at the storage layer and every extra pass would re-aggregate
/// the ENTIRE directory — the corpus-scaling cost issue #599 exists to remove.
/// The single pass instead over-fetches the whole budget up front
/// ([`candidate_fetch_size`]), so the worst case per page is
/// `1 directory + MAX_HYDRATION_CHUNKS × 3 hydration = 13` statements against
/// the Interactive `statement_cap = 256` — and against the naive per-session
/// alternative of `3 × 25 = 75` sequential round trips inside a 3 s deadline.
/// Tunable against the issue-599 §5.3 `filtered` boundedness phase, not a
/// design invariant.
pub(super) const MAX_HYDRATION_CHUNKS: usize = 4;

/// Ceiling on one hydration chunk — but the caller's page size WINS when it is
/// larger. `hydration_chunk_size` floors the chunk at `limit + 1` so a single
/// chunk can always answer "is there more", so a request for a page bigger than
/// this constant is hydrated in `limit + 1` rows, not clamped to 256. The
/// effective bound is therefore `max(256, limit + 1)`, and `limit` is itself
/// bounded by the configured `max_results`.
const MAX_HYDRATION_CHUNK_ROWS: u32 = 256;

/// Ceiling on the single Phase-A over-fetch. Same caveat as
/// [`MAX_HYDRATION_CHUNK_ROWS`]: `candidate_fetch_size` floors the fetch at one
/// chunk, so the effective bound is `max(1024, chunk)`. It bounds the candidate
/// page independently of `limit` only while `limit + 1 <= 1024`; past that the
/// caller's page size governs, which is the intended trade (a caller asking for
/// N sessions must be able to receive N).
const MAX_CANDIDATE_FETCH_ROWS: u32 = 1024;

/// Sessions hydrated per batched round trip:
/// `clamp(2 × (limit + 1), limit + 1, 256)`. Over-fetching absorbs the recall
/// filters' false positives without a second round trip; the floor keeps a
/// `limit + 1` "is there more" probe possible in one chunk.
pub(super) fn hydration_chunk_size(limit: u16) -> u32 {
    let floor = u32::from(limit).saturating_add(1);
    floor
        .saturating_mul(2)
        .min(MAX_HYDRATION_CHUNK_ROWS)
        .max(floor)
}

/// Candidates the SINGLE Phase-A pass fetches: the page's whole hydration
/// budget, capped.
///
/// Fetching the full budget in one statement is what keeps Phase A to one
/// pass (see [`MAX_HYDRATION_CHUNKS`]). The result never exceeds
/// `MAX_HYDRATION_CHUNKS × hydration_chunk_size`, so slicing the candidate
/// vector into hydration chunks is bounded by construction and needs no
/// separate counter.
pub(super) fn candidate_fetch_size(limit: u16) -> u32 {
    let chunk = hydration_chunk_size(limit);
    chunk
        .saturating_mul(MAX_HYDRATION_CHUNKS as u32)
        .min(MAX_CANDIDATE_FETCH_ROWS)
        .max(chunk)
}

/// The `mcp_session_directory.mode_hint` LOWER BOUND a mode filter may push,
/// or `None` when no predicate is safe (issue-599 §2.3).
///
/// `mode_hint` is `max(cd::mode_rank_expr)` — `3 = web_search`,
/// `2 = mcp_internal`, `1 = tool_calling`, `0 = chat`. The internal-tool-name
/// allowlist is FROZEN in the migration-036 MV body (`sql/036:156`), so a
/// session using an MCP tool added after that migration stores a hint BELOW its
/// live rank. Pushing `mode_hint = 2` would silently drop it; pushing nothing
/// keeps it a candidate and the exact `cd::mode_aggregate_expr` re-filter in
/// Phase C decides. `chat` (rank 0) has a vacuous bound.
fn mode_hint_lower_bound(mode: ConversationMode) -> Option<u8> {
    match mode {
        ConversationMode::WebSearch => Some(3),
        ConversationMode::ToolCalling => Some(1),
        ConversationMode::McpInternal | ConversationMode::Chat => None,
    }
}

/// One Phase-A candidate-page request. `after` is the keyset anchor
/// `(cand_last_ms, session_id)` the page resumes strictly after — always the
/// directory value, never a hydrated one (see
/// [`DirectoryCandidateRow::cand_last_ms`]).
pub(super) struct DirectoryPageParams<'a> {
    pub(super) start_unix_ms: i64,
    pub(super) end_unix_ms: i64,
    pub(super) mode: Option<ConversationMode>,
    pub(super) harness: Option<&'a str>,
    pub(super) source_name: Option<&'a str>,
    pub(super) sort: ConversationListSort,
    pub(super) after: Option<(i64, &'a str)>,
    pub(super) limit: u32,
}

impl ClickHouseConversationRepository {
    /// Phase A (issue-599 §1.3): the content-free candidate page.
    ///
    /// Load-bearing properties, each of which a shape test pins:
    ///
    /// * **No `FINAL`.** `mcp_session_directory` is an `AggregatingMergeTree`;
    ///   its `SimpleAggregateFunction` columns re-aggregate with their own
    ///   functions and `origin_cwd_state` merges with `argMinIfMerge`. Same
    ///   pattern as [`Self::build_session_signal_sql`].
    /// * **Published generations as a tuple-`IN`, never `ALL INNER JOIN`** — a
    ///   join defeats KeyCondition pruning. Unlike the session-scoped signal
    ///   read, this statement MUST apply the filter: an unpublished
    ///   (replay-in-progress) generation would otherwise inflate `cand_last_ms`
    ///   and leak an incomplete generation into the page, violating #602's
    ///   "previous complete generation until the atomic switch". It is also the
    ///   canonical replacement for the retired `tombstone` column: a session
    ///   whose only source generation is unpublished has zero live rows and
    ///   drops out of the `GROUP BY` entirely (issue-599 §2.7).
    /// * **`notEmpty(trimBoth(session_id))`.** The 036 MVs guard only
    ///   `notEmpty` (`sql/036:162,181,218`), so whitespace-only ids reach the
    ///   directory. Without the trim they would consume LIMIT slots and anchor
    ///   cursors while mcp-core drops them, breaking the contiguous-`rank`
    ///   invariant.
    /// * **`argMinIfMerge(origin_cwd_state)` is exact**, not a heuristic:
    ///   `argMinIfState(cwd, tuple(event_ts, event_uid), cwd != '')` merged
    ///   across a session's directory rows IS
    ///   `argMin(cwd, (event_ts, event_uid)) WHERE cwd != ''` — the identical
    ///   rule as [`Self::session_origin_scope_subquery`]. Project scope is
    ///   therefore applied here and NOT re-applied after hydration, which
    ///   deletes a corpus-sized `events FINAL` scan from every scoped page.
    /// * `harness` / `source` / `mode_hint` are RECALL filters only; the exact
    ///   values are re-checked in Phase C against the hydrated aggregates.
    /// * **`cand_last_ms` is the operation's single keyset time source.** It is
    ///   the only time value this statement can filter on, so it is also what
    ///   Phase C orders survivors by and mints the cursor from
    ///   ([`DirectoryCandidateRow::cand_last_ms`]).
    pub(super) fn build_session_directory_page_sql(&self, params: &DirectoryPageParams) -> String {
        let directory = self.table_ref("mcp_session_directory");
        let published = self.published_generations_subquery();

        let mut having = vec![
            format!("cand_last_ms >= {}", params.start_unix_ms),
            format!("cand_first_ms < {}", params.end_unix_ms),
        ];
        if let Some((last_ms, session_id)) = params.after {
            let (time_cmp, session_cmp) = match params.sort {
                ConversationListSort::Desc => ("<", "<"),
                ConversationListSort::Asc => (">", ">"),
            };
            having.push(format!(
                "(cand_last_ms {time_cmp} {last_ms} OR (cand_last_ms = {last_ms} AND session_id {session_cmp} {}))",
                sql_quote(session_id)
            ));
        }
        if let Some(rank) = params.mode.and_then(mode_hint_lower_bound) {
            having.push(format!("mode_hint >= {rank}"));
        }
        if let Some(harness) = params.harness {
            having.push(format!("has(harnesses, {})", sql_quote(harness)));
        }
        if let Some(source_name) = params.source_name {
            having.push(format!("has(sources, {})", sql_quote(source_name)));
        }
        if let Some(scope) = self.cfg.session_scope.as_ref() {
            let roots = scope
                .roots
                .iter()
                .map(|root| {
                    format!(
                        "origin_cwd = {root} OR startsWith(origin_cwd, {prefix})",
                        root = sql_quote(root),
                        prefix = sql_quote(&format!("{root}/")),
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            having.push(format!("({roots})"));
        }

        let order_dir = match params.sort {
            ConversationListSort::Desc => "DESC",
            ConversationListSort::Asc => "ASC",
        };
        format!(
            "SELECT\n  d.session_id AS session_id,\n  toInt64(toUnixTimestamp64Milli(max(d.max_observed_event_time))) AS cand_last_ms,\n  toString(max(d.max_observed_event_time)) AS cand_last_time,\n  toInt64(toUnixTimestamp64Milli(min(d.min_observed_event_time))) AS cand_first_ms,\n  toUInt8(max(d.mode_hint)) AS mode_hint,\n  argMinIfMerge(d.origin_cwd_state) AS origin_cwd,\n  groupUniqArray(d.harness) AS harnesses,\n  groupUniqArray(d.source_name) AS sources\nFROM {directory} AS d\nWHERE notEmpty(trimBoth(d.session_id))\n  AND (d.source_host, d.source_name, d.source_file, d.source_generation) IN {published}\nGROUP BY d.session_id\nHAVING {having}\nORDER BY cand_last_ms {order_dir}, session_id {order_dir}\nLIMIT {limit}\nFORMAT JSONEachRow",
            having = having.join("\n   AND "),
            limit = params.limit,
        )
    }

    /// Phase B1 (issue-599 §1.4): the K-session form of
    /// [`Self::build_session_totals_sql`]. Content-free; `ALL INNER JOIN
    /// published` is correct here because the statement consumes whole
    /// session-pruned ranges anyway (see [`Self::navigation_live_from`]).
    ///
    /// The projection list is the single-session builder's minus the three
    /// OPEN-only columns that need the display tuple (`first_event_uid`,
    /// `last_event_uid`, `last_actor_role`), plus `session_id` for the Rust
    /// bucketing. `user_messages` / `assistant_messages` / `tool_results` stay
    /// even though no `McpSessionListItem` field reads them: they are `countIf`s
    /// folded into the same aggregate pass, and they are what
    /// `totals_batch_reproduces_the_single_session_aggregates` compares to prove
    /// the transcription stayed mechanical.
    ///
    /// `counter_user_messages` is the one non-mechanical column. The
    /// single-session builder uses a correlated scalar subquery; emitting K of
    /// those would be a per-session loop in disguise, so this uses one windowed
    /// pass: `running_u` (the prefix user-message count in navigation-tuple
    /// order) sampled at the LAST `turn_index = 0` row is exactly the same
    /// value (design VERIFIER ADDENDUM item 1). With no counter row,
    /// `argMaxIf` yields its `UInt64` default `0` — the same answer the
    /// single-session `countIf(… <= <default tuple>)` produces.
    pub(super) fn build_session_totals_batch_sql(&self, session_ids: &[String]) -> String {
        let from = self.navigation_live_from();
        let ids = sql_array_strings(session_ids);
        let sort_tuple = Self::navigation_sort_tuple("n");
        let umsg = cd::user_message_count_predicate(COLS);
        let assistant_msg = "nav.actor_kind = 'assistant' AND nav.event_kind = 'message'";
        let mode = cd::mode_aggregate_expr(COLS);
        let event_ts_tuple = "tuple(nav.event_ts, nav.event_uid)";
        // The inner scan republishes the physical navigation columns the outer
        // aggregates read, so `cd::*` fragments (which name the raw `events`
        // columns) resolve against the single derived relation unqualified.
        let inner = format!(
            "SELECT\n    n.session_id AS session_id,\n    n.turn_index AS turn_index,\n    n.is_user_message AS is_user_message,\n    n.actor_kind AS actor_kind,\n    n.event_kind AS event_kind,\n    n.payload_type AS payload_type,\n    n.tool_name AS tool_name,\n    n.source_name AS source_name,\n    n.source_file AS source_file,\n    n.harness AS harness,\n    n.inference_provider AS inference_provider,\n    n.cwd AS cwd,\n    n.display_time AS display_time,\n    n.event_ts AS event_ts,\n    n.event_uid AS event_uid,\n    {sort_tuple} AS sort_key,\n    sum(if(n.is_user_message = 1, 1, 0)) OVER counter_window AS running_u\n  {from}\n  WHERE n.session_id IN {ids}\n  WINDOW counter_window AS (PARTITION BY n.session_id ORDER BY {sort_tuple} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
        format!(
            "SELECT\n  nav.session_id AS session_id,\n  toUInt64(count()) AS total_events,\n  toUInt64(countIf({umsg})) AS user_messages,\n  toUInt64(countIf({assistant_msg})) AS assistant_messages,\n  toUInt64(countIf(nav.event_kind = 'tool_call')) AS tool_calls,\n  toUInt64(countIf(nav.event_kind = 'tool_result')) AS tool_results,\n  toUInt32(max(nav.turn_index)) AS max_override,\n  toUInt64(argMaxIf(nav.running_u, nav.sort_key, nav.turn_index = 0)) AS counter_user_messages,\n  toString(min(nav.display_time)) AS first_event_time,\n  toInt64(toUnixTimestamp64Milli(min(nav.display_time))) AS first_event_unix_ms,\n  toInt64(toUnixTimestamp64Milli(max(nav.display_time))) AS last_event_unix_ms,\n  ifNull(argMinIf(nav.cwd, {event_ts_tuple}, nav.cwd != ''), '') AS origin_cwd,\n  ifNull(argMax(nullIf(nav.source_name, ''), {event_ts_tuple}), '') AS source,\n  ifNull(argMax(nullIf(nav.harness, ''), {event_ts_tuple}), '') AS harness,\n  ifNull(argMax(nullIf(nav.inference_provider, ''), {event_ts_tuple}), '') AS inference_provider,\n  ifNull(argMinIf(nullIf(trimBoth(replaceRegexpOne(arrayElement(splitByChar('/', replaceAll(nav.source_file, '\\\\', '/')), -1), '[.]jsonl$', '')), ''), {event_ts_tuple}, nav.source_name = 'omp' AND notEmpty(nav.session_id) AND endsWith(nav.source_file, '.jsonl') AND NOT endsWith(nav.source_file, concat(nav.session_id, '.jsonl'))), '') AS omp_dispatch_title,\n  {mode} AS mode\nFROM (\n  {inner}\n) AS nav\nGROUP BY nav.session_id\nFORMAT JSONEachRow"
        )
    }

    /// Phase B2 (issue-599 §1.4): the K-session form of
    /// [`Self::build_session_metadata_sql`] — the ONLY list statement that
    /// decompresses `payload_json`, and only for `is_metadata_bearing = 1`
    /// rows (typically 0-3 per session).
    ///
    /// `moraine.events` is `ORDER BY (session_id, event_ts, …)`
    /// (`sql/001_schema.sql:125`), so the K-element `IN` on the leading key
    /// column prunes the `FINAL` scan to K key ranges — but only because
    /// [`Self::live_events_source_sessions`] emits it INSIDE the derived table.
    /// The same predicate in this statement's outer `WHERE` sits above the
    /// publication join and prunes nothing (issue-598 C2-R0 "no optimizer
    /// trust"), which is a whole-corpus `events FINAL` scan on the discovery
    /// path. No time bound is applied: the directory's bounds are on
    /// `display_time` while the events primary key is on `event_ts` — different
    /// expressions that must not be conflated.
    pub(super) fn build_session_metadata_batch_sql(&self, session_ids: &[String]) -> String {
        let events = self.live_events_source_sessions(session_ids);
        let nav = self.table_ref("mcp_event_navigation");
        let published = self.published_generations_subquery();
        let ids = sql_array_strings(session_ids);
        format!(
            "SELECT\n  e.session_id AS session_id,\n  toString(e.event_ts) AS event_ts,\n  e.event_uid AS event_uid,\n  e.event_kind AS event_kind,\n  e.payload_json AS payload_json\nFROM {events} AS e\nWHERE e.event_uid IN (\n  SELECT n.event_uid\n  FROM {nav} AS n FINAL\n  ALL INNER JOIN {published} AS published\n    ON published.source_host = n.source_host AND published.source_name = n.source_name AND published.source_file = n.source_file AND published.source_generation = n.source_generation\n  WHERE n.session_id IN {ids} AND n.is_metadata_bearing = 1\n)\nFORMAT JSONEachRow"
        )
    }

    /// Phase B3 (issue-599 §1.4): the K-session form of
    /// [`Self::build_session_terminal_sql`]. Content-free.
    ///
    /// The two-level rule is preserved exactly: per-TURN `completed` is the
    /// turn's latest terminal event being `task_complete`, then the SESSION
    /// takes `argMax(completed, turn_seq)` — the LAST turn's flag. A
    /// session-wide latest-terminal `argMax` diverges when the terminal event
    /// sits in a middle turn (the WI-10 terminal-mid parity fixture). The only
    /// batch change is `PARTITION BY n.session_id` on the turn window plus the
    /// two `session_id` group keys.
    pub(super) fn build_session_terminal_batch_sql(&self, session_ids: &[String]) -> String {
        let from = self.navigation_live_from();
        let tuple = Self::navigation_sort_tuple("n");
        let ids = sql_array_strings(session_ids);
        format!(
            "SELECT\n  session_id,\n  toUInt8(argMax(turn_completed, turn_seq)) AS completed\nFROM (\n  SELECT\n    session_id,\n    turn_seq,\n    argMaxIf(toUInt8(payload_type = 'task_complete'), sort_key, payload_type IN ('task_complete', 'turn_aborted')) AS turn_completed\n  FROM (\n    SELECT\n      n.session_id AS session_id,\n      n.payload_type AS payload_type,\n      {tuple} AS sort_key,\n      if(toUInt32(n.turn_index) > 0, toUInt32(n.turn_index), greatest(toUInt32(1), toUInt32(sum(if(n.is_user_message = 1, 1, 0)) OVER turn_window))) AS turn_seq\n    {from}\n    WHERE n.session_id IN {ids}\n    WINDOW turn_window AS (PARTITION BY n.session_id ORDER BY {tuple} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n  )\n  GROUP BY session_id, turn_seq\n)\nGROUP BY session_id\nFORMAT JSONEachRow"
        )
    }
}

// ---------------------------------------------------------------------------
// Deserialization rows.
// ---------------------------------------------------------------------------

/// One Phase-A candidate. The recall columns (`mode_hint`, `origin_cwd`,
/// `harnesses`, `sources`) are consumed by the statement's own `HAVING` and
/// deliberately not deserialized.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct DirectoryCandidateRow {
    pub(super) session_id: String,
    /// Display form of [`Self::cand_last_ms`], reported as the item's
    /// `last_event_time` so the response's timestamp is the value the page was
    /// ordered and keyset by (issue-599 B1). Reporting the hydrated exact value
    /// instead would leave the page sorted by a field it does not return.
    pub(super) cand_last_time: String,
    /// **The operation's one keyset time source.** Phase A orders by it, its
    /// `HAVING` keyset resumes strictly after it, Phase C sorts survivors by
    /// it, and the continuation cursor is minted from it. Candidate filtering,
    /// result ordering and cursor minting must all read this same value or a
    /// page can skip sessions.
    ///
    /// It is authoritative because it is the ONLY time value Phase A can filter
    /// on: `mcp_session_directory` is the only relation the candidate pass
    /// reads, and `max(max_observed_event_time)` is its only upper time bound.
    ///
    /// The hydrated `SessionTotalsBatchRow::last_event_unix_ms` is a DIFFERENT
    /// number. The directory is an `AggregatingMergeTree` whose
    /// `SimpleAggregateFunction(max)` accumulates every physically inserted
    /// event version and can never retract a superseded one, while
    /// `mcp_event_navigation` is `ReplacingMergeTree(event_version)` read with
    /// `FINAL`, so hydration sees only the winning version. Hence
    /// `cand_last_ms >= last_event_unix_ms`, with equality in the common
    /// never-re-ingested case. Minting the cursor from the hydrated value and
    /// then comparing it against `cand_last_ms` on the next page silently skips
    /// every session whose directory aggregate falls between the two.
    ///
    /// The hydrated value is what the item REPORTS as `last_event_unix_ms`
    /// (response fidelity — it is the same exact aggregate the projected-header
    /// path served) and is never an ordering or cursor input.
    pub(super) cand_last_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SessionTotalsBatchRow {
    pub(super) session_id: String,
    pub(super) total_events: u64,
    pub(super) tool_calls: u64,
    pub(super) max_override: u32,
    pub(super) counter_user_messages: u64,
    pub(super) first_event_time: String,
    pub(super) first_event_unix_ms: i64,
    pub(super) last_event_unix_ms: i64,
    /// The session's origin cwd under the EXACT rule `scope.rs` applies —
    /// `argMin(cwd, (event_ts, event_uid))` over rows with a non-empty cwd,
    /// here over the navigation index read `FINAL`. Phase A's
    /// `argMinIfMerge(origin_cwd_state)` is the same rule merged over the
    /// directory's live-generation rows, but a `SimpleAggregateFunction`-style
    /// merge cannot see a superseded version the way `FINAL` can, so scope is
    /// re-checked against THIS value before a session is served. Scope decides
    /// what a caller is allowed to see; it must not rest on a recall filter.
    pub(super) origin_cwd: String,
    pub(super) source: String,
    pub(super) harness: String,
    pub(super) inference_provider: String,
    pub(super) omp_dispatch_title: String,
    pub(super) mode: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SessionMetaBatchRow {
    pub(super) session_id: String,
    pub(super) event_ts: String,
    pub(super) event_uid: String,
    pub(super) event_kind: String,
    pub(super) payload_json: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SessionTerminalBatchRow {
    pub(super) session_id: String,
    pub(super) completed: u8,
}

/// One candidate session's hydrated exact state (Phase B output, Phase C
/// input).
pub(super) struct HydratedSession {
    pub(super) totals: SessionTotalsBatchRow,
    pub(super) metadata: Vec<MetaRow>,
    pub(super) completed: bool,
}

impl HydratedSession {
    /// The metadata-precedence inputs for this session: the bounded
    /// metadata-bearing fold, seeded with the omp dispatch-title fallback the
    /// fold cannot see (v1 derives it over ALL canonical rows, so the totals
    /// pass supplies it).
    pub(super) fn precedence(&self) -> MetadataPrecedence {
        let mut precedence = metadata_precedence(&self.metadata);
        precedence
            .omp_dispatch_title
            .clone_from(&self.totals.omp_dispatch_title);
        precedence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn params<'a>(mode: Option<ConversationMode>) -> DirectoryPageParams<'a> {
        DirectoryPageParams {
            start_unix_ms: 1_767_261_600_000,
            end_unix_ms: 1_767_500_000_000,
            mode,
            harness: None,
            source_name: None,
            sort: ConversationListSort::Desc,
            after: None,
            limit: 52,
        }
    }

    fn ids() -> Vec<String> {
        vec!["sess-a".to_string(), "sess-b".to_string()]
    }

    /// No discovery statement may read transcript content, and none may open
    /// the `events` relation unpruned.
    ///
    /// The name grep alone cannot see the second failure: the live-events
    /// derived table projects `SELECT e.*`, which republishes every wide column
    /// — including `text_content` — without ever naming one. The guard that
    /// actually bounds it is the leading-primary-key filter INSIDE that derived
    /// table, so assert on the filter, not on the column names.
    fn assert_content_free(sql: &str) {
        assert!(
            !sql.contains("text_content"),
            "discovery statement leaked text_content:\n{sql}"
        );
        assert_events_scan_is_key_pruned(sql);
    }

    /// Every `SELECT e.*` derived table must carry a `session_id` predicate in
    /// its own body (issue-598 C2-R0): an outer `WHERE` sits above the
    /// publication join and prunes nothing, so the statement would scan the
    /// whole `events` table with `FINAL`.
    /// Every `nav.<column>` the OUTER query references must be projected by
    /// the inner derived table it selects from.
    ///
    /// ClickHouse resolves this only at execution time, so a missing
    /// projection passes every text-matching shape assertion and fails in the
    /// functional stack. That has now happened twice — `ev.event_ts` in the
    /// #598 navigation window and `nav.cwd` here — which is why this is a
    /// structural check rather than another literal.
    fn assert_outer_columns_are_projected_by_inner(sql: &str, alias: &str) {
        let Some(inner_start) = sql.find("FROM (") else {
            panic!("no derived table to check in:\n{sql}");
        };
        let (outer, inner) = sql.split_at(inner_start);

        let needle = format!("{alias}.");
        let mut referenced: Vec<String> = Vec::new();
        let mut rest = outer;
        while let Some(at) = rest.find(&needle) {
            let tail = &rest[at + needle.len()..];
            let end = tail
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .unwrap_or(tail.len());
            let column = &tail[..end];
            if !column.is_empty() && !referenced.iter().any(|seen| seen == column) {
                referenced.push(column.to_string());
            }
            rest = &tail[end..];
        }
        assert!(
            !referenced.is_empty(),
            "found no {alias}.<column> references to check in:\n{sql}"
        );
        for column in referenced {
            assert!(
                inner.contains(&format!("AS {column}")),
                "outer references {alias}.{column} but the inner derived table does not project it \
                 — ClickHouse would fail this at execution time:\n{sql}"
            );
        }
    }

    fn assert_events_scan_is_key_pruned(sql: &str) {
        // Only statements that actually open `events` are in scope…
        if !sql.contains("`events`") {
            return;
        }
        // …and for those, an unrecognized derived-table shape is a FAILURE, not
        // a pass. Returning early here would let any future shape through
        // unexamined, which is exactly how the unpruned scan this guard exists
        // for slipped past the `text_content` grep.
        let Some(start) = sql.find("(SELECT e.*") else {
            panic!("events scan is not in the recognized live-events derived-table shape:\n{sql}");
        };
        let body = &sql[start..];
        let end = body
            .find(") AS e")
            .unwrap_or_else(|| panic!("unterminated live-events derived table in:\n{sql}"));
        let body = &body[..end];
        assert!(
            body.contains("\nWHERE e.session_id"),
            "the events FINAL scan is not key-pruned inside its derived table:\n{sql}"
        );
    }

    /// No discovery statement may read or gate on the retired projection.
    fn assert_no_projection(sql: &str) {
        super::super::sql::canonical_assertions::assert_no_projection("discovery statement", sql);
        super::super::sql::canonical_assertions::assert_no_legacy_view_chain(
            "discovery statement",
            sql,
        );
    }

    // --- chunk sizing ------------------------------------------------------

    #[test]
    fn hydration_chunk_over_fetches_within_the_ceiling() {
        assert_eq!(hydration_chunk_size(25), 52);
        assert_eq!(hydration_chunk_size(1), 4);
        // Ceiling holds …
        assert_eq!(hydration_chunk_size(200), 256);
        // … but never below `limit + 1`, so a K+1 probe is always possible.
        assert_eq!(hydration_chunk_size(400), 401);
    }

    #[test]
    fn one_phase_a_pass_fetches_the_whole_hydration_budget() {
        // The invariant the single-pass design rests on: the over-fetch never
        // exceeds what the chunk loop is allowed to hydrate, so a page can
        // never need a second directory aggregation.
        for limit in [1_u16, 2, 25, 50, 200, 400, u16::MAX] {
            let chunk = hydration_chunk_size(limit);
            let fetch = candidate_fetch_size(limit);
            assert!(fetch >= chunk, "limit {limit} cannot fill one chunk");
            assert!(
                fetch <= chunk.saturating_mul(MAX_HYDRATION_CHUNKS as u32),
                "limit {limit} over-fetches beyond the hydration budget"
            );
            assert!(fetch <= MAX_CANDIDATE_FETCH_ROWS.max(chunk));
        }
        assert_eq!(candidate_fetch_size(25), 208);
        assert_eq!(candidate_fetch_size(1), 16);
        // The absolute ceiling clamps before the budget multiple does.
        assert_eq!(candidate_fetch_size(200), 1024);
    }

    // --- Phase A shape -----------------------------------------------------

    #[tokio::test]
    async fn directory_page_is_content_free_and_publication_pinned() {
        let sql = build(repo(), |r| {
            r.build_session_directory_page_sql(&params(None))
        })
        .await;
        assert_content_free(&sql);
        assert_no_projection(&sql);
        assert!(
            !sql.contains("payload_json"),
            "directory read is scalar-only"
        );
        assert!(sql.contains("`moraine`.`mcp_session_directory` AS d"));
        // AggregatingMergeTree re-aggregates through its own functions.
        assert!(
            !sql.contains("FINAL"),
            "directory read must not use FINAL:\n{sql}"
        );
        // Published heads are a tuple-IN, never a join (KeyCondition pruning),
        // and they are the canonical `tombstone` replacement.
        assert!(
            !sql.contains("ALL INNER JOIN"),
            "directory read must not join the published heads:\n{sql}"
        );
        assert!(sql.contains(
            "(d.source_host, d.source_name, d.source_file, d.source_generation) IN (SELECT"
        ));
        // Whitespace-only ids never consume a LIMIT slot or anchor a cursor.
        assert!(sql.contains("WHERE notEmpty(trimBoth(d.session_id))"));
        // The exact origin-cwd rule, merged from the directory state.
        assert!(sql.contains("argMinIfMerge(d.origin_cwd_state) AS origin_cwd"));
        // Overlap, not containment.
        assert!(sql.contains("cand_last_ms >= 1767261600000"));
        assert!(sql.contains("cand_first_ms < 1767500000000"));
        assert!(sql.contains("ORDER BY cand_last_ms DESC, session_id DESC"));
        assert!(sql.contains("LIMIT 52"));
    }

    #[tokio::test]
    async fn directory_page_never_runs_the_corpus_wide_scope_subquery() {
        let sql = build(scoped_repo(&["/work/project"]), |r| {
            r.build_session_directory_page_sql(&params(None))
        })
        .await;
        assert!(sql.contains("origin_cwd = '/work/project'"));
        assert!(sql.contains("startsWith(origin_cwd, '/work/project/')"));
        // The corpus-sized `argMin(cwd, …) GROUP BY session_id` over
        // `events FINAL` is exactly what the directory state replaces.
        assert!(
            !sql.contains("argMin(cwd"),
            "scoped page must not re-run the session-origin subquery:\n{sql}"
        );
    }

    #[tokio::test]
    async fn mode_prefilter_is_a_lower_bound_and_absent_for_mcp_internal() {
        for (mode, rank) in [
            (ConversationMode::WebSearch, 3_u8),
            (ConversationMode::ToolCalling, 1),
        ] {
            let sql = build(repo(), move |r| {
                r.build_session_directory_page_sql(&params(Some(mode)))
            })
            .await;
            assert!(
                sql.contains(&format!("mode_hint >= {rank}")),
                "expected a lower-bound prefilter for {mode:?}:\n{sql}"
            );
            assert!(
                !sql.contains("mode_hint ="),
                "mode_hint must never be precision-filtered (sql/036:156 freezes the allowlist):\n{sql}"
            );
        }
        // rank 2 (frozen allowlist) and rank 0 (vacuous) push nothing.
        for mode in [ConversationMode::McpInternal, ConversationMode::Chat] {
            let sql = build(repo(), move |r| {
                r.build_session_directory_page_sql(&params(Some(mode)))
            })
            .await;
            assert!(
                !sql.contains("mode_hint >="),
                "{mode:?} must not push a mode_hint predicate:\n{sql}"
            );
        }
    }

    #[tokio::test]
    async fn recall_filters_and_keyset_flip_with_sort() {
        let sql = build(repo(), |r| {
            r.build_session_directory_page_sql(&DirectoryPageParams {
                harness: Some("codex"),
                source_name: Some("codex-jsonl"),
                sort: ConversationListSort::Asc,
                after: Some((1_767_348_600_000, "sess_b")),
                ..params(None)
            })
        })
        .await;
        assert!(sql.contains("has(harnesses, 'codex')"));
        assert!(sql.contains("has(sources, 'codex-jsonl')"));
        assert!(sql.contains(
            "(cand_last_ms > 1767348600000 OR (cand_last_ms = 1767348600000 AND session_id > 'sess_b'))"
        ));
        assert!(sql.contains("ORDER BY cand_last_ms ASC, session_id ASC"));
    }

    // --- Phase B shape -----------------------------------------------------

    #[tokio::test]
    async fn totals_batch_groups_by_session_and_is_content_free() {
        let sql = build(repo(), |r| r.build_session_totals_batch_sql(&ids())).await;
        assert_content_free(&sql);
        assert_no_projection(&sql);
        assert_outer_columns_are_projected_by_inner(&sql, "nav");
        assert!(!sql.contains("payload_json"));
        assert!(sql.contains("`moraine`.`mcp_event_navigation` AS n FINAL"));
        assert!(sql.contains("WHERE n.session_id IN ['sess-a','sess-b']"));
        assert!(sql.contains("GROUP BY nav.session_id"));
        assert!(sql.contains("AS inference_provider"));
        assert!(sql.contains("AS tool_calls"));
        // One windowed pass, never K correlated subqueries.
        assert!(sql.contains(
            "WINDOW counter_window AS (PARTITION BY n.session_id ORDER BY tuple(n.sort_time"
        ));
        assert!(sql.contains("argMaxIf(nav.running_u, nav.sort_key, nav.turn_index = 0)"));
        assert!(
            !sql.contains("SELECT maxIf("),
            "the batch must not emit the single-session correlated subquery:\n{sql}"
        );
    }

    #[tokio::test]
    async fn totals_batch_reproduces_the_single_session_aggregates() {
        // Mechanical-transcription guard: for every column both builders
        // project, the aggregate expression must be identical once the table
        // alias is normalized away.
        let batch = build(repo(), |r| r.build_session_totals_batch_sql(&ids())).await;
        let single = build(repo(), |r| r.build_session_totals_sql("sess-a")).await;
        for alias in [
            "total_events",
            "user_messages",
            "assistant_messages",
            "tool_calls",
            "tool_results",
            "max_override",
            "first_event_time",
            "first_event_unix_ms",
            // `origin_cwd` is intentionally absent: it is batch-only, hydrated
            // for the exact project-scope re-check. The single-session builder
            // has no equivalent because `open(session)` is already scoped
            // before it runs.
            // `last_event_time` is deliberately absent: the directory path
            // reports the value it orders by (`cand_last_time`), so hydrating
            // the display string here would transfer bytes that are discarded.
            "last_event_unix_ms",
            "source",
            "harness",
            "inference_provider",
            "omp_dispatch_title",
        ] {
            assert_eq!(
                projection_for(&batch, alias),
                projection_for(&single, alias),
                "batched `{alias}` diverged from the single-session builder"
            );
        }
        // `mode` spans lines; assert both embed the shared authority verbatim.
        let mode = cd::mode_aggregate_expr(COLS);
        assert!(batch.contains(&mode) && single.contains(&mode));
        // `counter_user_messages` is the deliberate divergence (windowed pass
        // vs correlated subquery), asserted separately above.
    }

    /// The single projection line ending in `AS {alias}`, alias-normalized.
    fn projection_for(sql: &str, alias: &str) -> String {
        let needle = format!(" AS {alias},");
        let line = sql
            .lines()
            .find(|line| line.ends_with(&needle) || line.ends_with(&format!(" AS {alias}")))
            .unwrap_or_else(|| panic!("no `AS {alias}` projection in:\n{sql}"));
        line.trim().replace("nav.", "").replace("n.", "")
    }

    #[tokio::test]
    async fn metadata_batch_reads_payload_only_for_metadata_bearing_rows() {
        let sql = build(repo(), |r| r.build_session_metadata_batch_sql(&ids())).await;
        assert_content_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("n.is_metadata_bearing = 1"));
        assert!(sql.contains("e.payload_json AS payload_json"));
        assert!(sql.contains("e.session_id AS session_id"));
        // Leading-primary-key prune, emitted INSIDE the live-events derived
        // table so it survives the publication join (C2-R0). The same predicate
        // in the outer WHERE would prune nothing and scan the whole corpus.
        assert!(sql.contains(
            "AND published.source_generation = e.source_generation\nWHERE e.session_id IN ['sess-a','sess-b'])"
        ));
        // The uid set is the exact filter and stays outside.
        assert!(sql.contains(") AS e\nWHERE e.event_uid IN ("));
        // The directory's display-time bounds must never be applied to the
        // events `event_ts` primary key.
        assert!(!sql.contains("BETWEEN"));
    }

    /// Regression pin for [`assert_content_free`] itself. This is the exact
    /// shape the metadata builder had before the C2-R0 fix: an outer-only
    /// `session_id` filter over an unpruned `SELECT e.*` derived table. The
    /// `text_content` grep passes it, so only the key-prune check can fail it.
    #[tokio::test]
    #[should_panic(expected = "not key-pruned inside its derived table")]
    async fn unpruned_events_scan_fails_the_content_free_guard() {
        let sql = build(repo(), |r| {
            format!(
                "SELECT e.session_id\nFROM {} AS e\nWHERE e.session_id IN ['sess-a']\nFORMAT JSONEachRow",
                r.live_events_source()
            )
        })
        .await;
        assert!(
            !sql.contains("text_content"),
            "the name grep alone must pass this shape"
        );
        assert_content_free(&sql);
    }

    #[tokio::test]
    async fn terminal_batch_keeps_the_two_level_last_turn_rule() {
        let sql = build(repo(), |r| r.build_session_terminal_batch_sql(&ids())).await;
        assert_content_free(&sql);
        assert_no_projection(&sql);
        assert!(sql.contains("argMax(turn_completed, turn_seq)"));
        assert!(sql.contains("GROUP BY session_id, turn_seq"));
        assert!(sql.contains("GROUP BY session_id\nFORMAT"));
        assert!(sql.contains(
            "WINDOW turn_window AS (PARTITION BY n.session_id ORDER BY tuple(n.sort_time"
        ));
        assert!(sql.contains("WHERE n.session_id IN ['sess-a','sess-b']"));
    }

    #[tokio::test]
    async fn terminal_batch_matches_the_single_session_turn_rule() {
        let batch = build(repo(), |r| r.build_session_terminal_batch_sql(&ids())).await;
        let single = build(repo(), |r| r.build_session_terminal_sql("sess-a")).await;
        for fragment in [
            "argMax(turn_completed, turn_seq)",
            "argMaxIf(toUInt8(payload_type = 'task_complete'), sort_key, payload_type IN ('task_complete', 'turn_aborted'))",
            "if(toUInt32(n.turn_index) > 0, toUInt32(n.turn_index), greatest(toUInt32(1), toUInt32(sum(if(n.is_user_message = 1, 1, 0)) OVER turn_window)))",
        ] {
            assert!(batch.contains(fragment), "batch lost `{fragment}`:\n{batch}");
            assert!(single.contains(fragment));
        }
    }
}
