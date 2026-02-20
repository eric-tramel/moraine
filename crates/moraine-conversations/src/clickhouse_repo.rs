use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ahash::AHashMap as HashMap;
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use moraine_clickhouse::ClickHouseClient;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::warn;
use uuid::Uuid;

use crate::cursor::{decode_cursor, encode_cursor, ConversationCursor, TurnCursor};
use crate::domain::{
    Conversation, ConversationDetailOptions, ConversationListFilter, ConversationMode,
    ConversationSearchHit, ConversationSearchQuery, ConversationSearchResults,
    ConversationSearchStats, ConversationSummary, OpenContext, OpenEvent, OpenEventRequest, Page,
    PageRequest, RepoConfig, SearchEventHit, SearchEventsQuery, SearchEventsResult,
    SearchEventsStats, SearchEventsStrategy, TraceEvent, Turn, TurnListFilter, TurnSummary,
};
use crate::error::{RepoError, RepoResult};
use crate::repo::ConversationRepository;

#[derive(Clone)]
pub struct ClickHouseConversationRepository {
    ch: ClickHouseClient,
    cfg: RepoConfig,
    stats_cache: Arc<RwLock<SearchStatsCache>>,
    search_cache: Arc<RwLock<HashMap<String, SearchEventsCacheEntry>>>,
    term_postings_cache: Arc<RwLock<HashMap<String, TermPostingsCacheEntry>>>,
    search_doc_extra_cache: Arc<RwLock<HashMap<String, SearchDocExtraCacheEntry>>>,
}

const BENCHMARK_REPLAY_SOURCE: &str = "benchmark-replay";
const CORPUS_STATS_CACHE_TTL: Duration = Duration::from_secs(30);
const TERM_DF_CACHE_TTL: Duration = Duration::from_secs(300);
const SEARCH_SCHEMA_CACHE_TTL: Duration = Duration::from_secs(60);
const SEARCH_RESULT_CACHE_TTL: Duration = Duration::from_secs(15);
const SEARCH_RESULT_CACHE_MAX_ENTRIES: usize = 256;
const TERM_POSTINGS_CACHE_TTL: Duration = Duration::from_secs(15);
const TERM_POSTINGS_CACHE_MAX_ENTRIES: usize = 2048;
const TERM_POSTINGS_CACHE_MAX_ROWS_PER_TERM: usize = 131_072;
const TERM_POSTINGS_CACHE_MAX_ROWS_TOTAL: usize = 262_144;
const SEARCH_DOC_EXTRA_CACHE_TTL: Duration = Duration::from_secs(15);
const SEARCH_DOC_EXTRA_CACHE_MAX_ENTRIES: usize = 65536;
const CONVERSATION_CANDIDATE_MIN: usize = 512;
const CONVERSATION_CANDIDATE_MULTIPLIER: usize = 80;
const CONVERSATION_CANDIDATE_MAX: usize = 20_000;
const CONVERSATION_RECENT_WINDOW_MS: i64 = 45_000;
const CONVERSATION_RECENT_CANDIDATE_LIMIT: usize = 1024;

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

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
struct CachedPostingRow {
    event_uid: String,
    doc_len: u32,
    tf: u16,
}

#[derive(Debug, Clone, Deserialize)]
struct FetchedPostingRow {
    term: String,
    event_uid: String,
    doc_len: u32,
    tf: u16,
}

#[derive(Debug, Clone, Deserialize)]
struct SearchDocExtraRow {
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
    has_codex_mcp: u8,
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
struct HotQueryRow {
    raw_query: String,
}

#[derive(Debug, Deserialize)]
struct ConversationSearchRow {
    session_id: String,
    score: f64,
    matched_terms: u16,
    event_count_considered: u32,
    best_event_uid: String,
    #[serde(default)]
    snippet: String,
}

#[derive(Debug, Deserialize)]
struct ConversationSnippetRow {
    event_uid: String,
    snippet: String,
}

#[derive(Debug, Deserialize)]
struct ConversationCandidateRow {
    session_id: String,
    score: f64,
    matched_terms: u16,
}

#[derive(Debug, Default)]
struct ConversationCandidateSet {
    rows: Vec<ConversationCandidateRow>,
    truncated: bool,
}

#[derive(Debug, Deserialize)]
struct ColumnExistsRow {
    exists: u8,
}

#[derive(Debug, Clone)]
struct CorpusStatsCacheEntry {
    docs: u64,
    total_doc_len: u64,
    fetched_at: Instant,
}

#[derive(Debug, Clone)]
struct TermDfCacheEntry {
    df: u64,
    fetched_at: Instant,
}

#[derive(Debug, Default)]
struct SearchStatsCache {
    corpus_stats: Option<CorpusStatsCacheEntry>,
    term_df_by_term: HashMap<String, TermDfCacheEntry>,
    has_codex_flag_column: Option<(bool, Instant)>,
}

#[derive(Debug, Clone)]
struct SearchEventsCacheEntry {
    hits: Vec<SearchEventHit>,
    fetched_at: Instant,
}

#[derive(Debug, Clone)]
struct TermPostingsCacheEntry {
    rows: Arc<[CachedPostingRow]>,
    fetched_at: Instant,
}

#[derive(Debug, Clone)]
struct SearchDocExtraCacheEntry {
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
    has_codex_mcp: u8,
    fetched_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct SearchScoreAccum<'a> {
    row: &'a CachedPostingRow,
    score: f64,
    matched_mask: u64,
}

impl ClickHouseConversationRepository {
    pub fn new(ch: ClickHouseClient, cfg: RepoConfig) -> Self {
        Self {
            ch,
            cfg,
            stats_cache: Arc::new(RwLock::new(SearchStatsCache::default())),
            search_cache: Arc::new(RwLock::new(HashMap::new())),
            term_postings_cache: Arc::new(RwLock::new(HashMap::new())),
            search_doc_extra_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &RepoConfig {
        &self.cfg
    }

    async fn run_mcp_search_prewarm_queries(
        &self,
        queries: impl IntoIterator<Item = String>,
        limit: u16,
    ) {
        for query in queries {
            if query.trim().is_empty() {
                continue;
            }
            if let Err(err) = self
                .search_events(SearchEventsQuery {
                    query,
                    source: Some(BENCHMARK_REPLAY_SOURCE.to_string()),
                    limit: Some(limit),
                    session_id: None,
                    min_score: None,
                    min_should_match: None,
                    include_tool_events: None,
                    exclude_codex_mcp: None,
                    disable_cache: Some(false),
                    search_strategy: Some(SearchEventsStrategy::Optimized),
                })
                .await
            {
                warn!("mcp prewarm query failed: {}", err);
            }
        }
    }

    pub async fn prewarm_mcp_search_state_quick(&self) -> RepoResult<()> {
        // Keep synchronous initialize prewarm deterministic and bounded.
        // Variable hot-query prewarm stays in the async background path.
        const PREWARM_QUERY: &str = "the";
        const PREWARM_LIMITS: [u16; 2] = [1, 25];

        for limit in PREWARM_LIMITS {
            if let Err(err) = self
                .search_events(SearchEventsQuery {
                    query: PREWARM_QUERY.to_string(),
                    source: Some(BENCHMARK_REPLAY_SOURCE.to_string()),
                    limit: Some(limit),
                    session_id: None,
                    min_score: None,
                    min_should_match: None,
                    include_tool_events: None,
                    exclude_codex_mcp: None,
                    disable_cache: Some(false),
                    search_strategy: Some(SearchEventsStrategy::Optimized),
                })
                .await
            {
                warn!("mcp quick prewarm query failed: {}", err);
            }
        }

        Ok(())
    }

    pub async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        const PREWARM_QUERY_LIMIT: u16 = 10;
        const PREWARM_HOT_QUERY_COUNT: usize = 6;
        const PREWARM_FALLBACK_QUERIES: [&str; 5] = [
            "the",
            "error",
            "test",
            "file directory path config",
            "function code implementation",
        ];

        let mut queries = self
            .load_hot_queries_for_prewarm(PREWARM_HOT_QUERY_COUNT)
            .await?;
        for fallback in PREWARM_FALLBACK_QUERIES {
            if !queries.iter().any(|existing| existing == fallback) {
                queries.push(fallback.to_string());
            }
        }

        self.run_mcp_search_prewarm_queries(queries, PREWARM_QUERY_LIMIT)
            .await;

        Ok(())
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

    fn search_events_cache_key(
        terms: &[String],
        search_strategy: SearchEventsStrategy,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> String {
        let mut cache_terms = terms.to_vec();
        cache_terms.sort_unstable();
        format!(
            "strategy={};incl_tools={include_tool_events};excl_codex={exclude_codex_mcp};session={};msm={min_should_match};min_score={min_score:.12};limit={limit};terms={}",
            search_strategy.as_str(),
            session_id.unwrap_or(""),
            cache_terms.join(",")
        )
    }

    async fn search_events_cache_get(&self, key: &str) -> Option<Vec<SearchEventHit>> {
        let now = Instant::now();
        {
            let cache = self.search_cache.read().await;
            if let Some(entry) = cache.get(key) {
                if now.duration_since(entry.fetched_at) <= SEARCH_RESULT_CACHE_TTL {
                    return Some(entry.hits.clone());
                }
            } else {
                return None;
            }
        }

        let mut cache = self.search_cache.write().await;
        if let Some(entry) = cache.get(key) {
            if now.duration_since(entry.fetched_at) <= SEARCH_RESULT_CACHE_TTL {
                return Some(entry.hits.clone());
            }
        }
        cache.remove(key);
        None
    }

    async fn search_events_cache_put(&self, key: String, hits: &[SearchEventHit]) {
        let now = Instant::now();
        let mut cache = self.search_cache.write().await;
        cache.retain(|_, entry| now.duration_since(entry.fetched_at) <= SEARCH_RESULT_CACHE_TTL);

        if cache.len() >= SEARCH_RESULT_CACHE_MAX_ENTRIES {
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.fetched_at)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(
            key,
            SearchEventsCacheEntry {
                hits: hits.to_vec(),
                fetched_at: now,
            },
        );
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
        let now = Instant::now();
        {
            let cache = self.stats_cache.read().await;
            if let Some(entry) = cache.corpus_stats.as_ref() {
                if now.duration_since(entry.fetched_at) <= CORPUS_STATS_CACHE_TTL {
                    return Ok((entry.docs, entry.total_doc_len));
                }
            }
        }

        let from_stats_query = format!(
            "SELECT toUInt64(ifNull(sum(docs), 0)) AS docs, toUInt64(ifNull(sum(total_doc_len), 0)) AS total_doc_len FROM {} FORMAT JSONEachRow",
            self.table_ref("search_corpus_stats")
        );

        let from_stats: Vec<CorpusStatsRow> =
            self.map_backend(self.ch.query_rows(&from_stats_query, None).await)?;

        if let Some(row) = from_stats.first() {
            if row.docs > 0 {
                self.cache_corpus_stats(row.docs, row.total_doc_len, now)
                    .await;
                return Ok((row.docs, row.total_doc_len));
            }
        }

        let fallback_query = format!(
            "SELECT toUInt64(count()) AS docs, toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len FROM {} FINAL WHERE doc_len > 0 FORMAT JSONEachRow",
            self.table_ref("search_documents")
        );
        let fallback: Vec<CorpusStatsRow> =
            self.map_backend(self.ch.query_rows(&fallback_query, None).await)?;
        let resolved = if let Some(row) = fallback.first() {
            (row.docs, row.total_doc_len)
        } else {
            (0, 0)
        };

        let mut cache = self.stats_cache.write().await;
        cache.corpus_stats = Some(CorpusStatsCacheEntry {
            docs: resolved.0,
            total_doc_len: resolved.1,
            fetched_at: now,
        });
        Ok(resolved)
    }

    async fn cache_corpus_stats(&self, docs: u64, total_doc_len: u64, fetched_at: Instant) {
        let mut cache = self.stats_cache.write().await;
        cache.corpus_stats = Some(CorpusStatsCacheEntry {
            docs,
            total_doc_len,
            fetched_at,
        });
    }

    async fn cache_term_df_values(
        &self,
        terms: impl IntoIterator<Item = String>,
        map: &HashMap<String, u64>,
        fetched_at: Instant,
    ) {
        let mut cache = self.stats_cache.write().await;
        for term in terms {
            let df = *map.get(&term).unwrap_or(&0);
            cache
                .term_df_by_term
                .insert(term, TermDfCacheEntry { df, fetched_at });
        }
    }

    async fn load_hot_queries_for_prewarm(&self, limit: usize) -> RepoResult<Vec<String>> {
        let query = format!(
            "SELECT raw_query
FROM (
  SELECT
    raw_query,
    count() AS query_count,
    avg(response_ms) AS avg_response_ms
  FROM {}
  WHERE source = 'moraine-mcp'
    AND ts >= now() - INTERVAL 7 DAY
    AND lengthUTF8(trim(BOTH ' ' FROM raw_query)) > 0
  GROUP BY raw_query
  ORDER BY query_count DESC, avg_response_ms DESC
  LIMIT {}
)
FORMAT JSONEachRow",
            self.table_ref("search_query_log"),
            limit
        );
        let rows: Vec<HotQueryRow> = self.map_backend(self.ch.query_rows(&query, None).await)?;
        Ok(rows.into_iter().map(|row| row.raw_query).collect())
    }

    async fn df_map(&self, terms: &[String]) -> RepoResult<HashMap<String, u64>> {
        let now = Instant::now();
        let postings_table = self.table_ref("search_postings");

        let mut map = HashMap::<String, u64>::new();
        let mut missing_terms = Vec::<String>::new();

        {
            let cache = self.stats_cache.read().await;
            for term in terms {
                if let Some(entry) = cache.term_df_by_term.get(term) {
                    if now.duration_since(entry.fetched_at) <= TERM_DF_CACHE_TTL {
                        map.insert(term.clone(), entry.df);
                        continue;
                    }
                }
                missing_terms.push(term.clone());
            }
        }

        if missing_terms.is_empty() {
            return Ok(map);
        }

        let missing_terms_array = sql_array_strings(&missing_terms);
        let df_query = format!(
            "SELECT term, toUInt64(uniqExact(doc_id)) AS df FROM {postings_table} WHERE term IN {missing_terms_array} GROUP BY term FORMAT JSONEachRow",
        );
        let rows: Vec<DfRow> = self.map_backend(self.ch.query_rows(&df_query, None).await)?;
        for row in rows {
            map.insert(row.term, row.df);
        }

        for term in &missing_terms {
            map.entry(term.clone()).or_insert(0);
        }

        self.cache_term_df_values(missing_terms, &map, now).await;
        Ok(map)
    }

    fn build_search_events_sql(
        &self,
        terms: &[String],
        idf_by_term: &HashMap<String, f64>,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        use_document_codex_flag: bool,
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
        let documents_join_sql = if use_document_codex_flag {
            format!(
                "(SELECT
  event_uid,
  any(session_id) AS session_id,
  any(source_name) AS source_name,
  any(provider) AS provider,
  any(event_class) AS event_class,
  any(payload_type) AS payload_type,
  any(actor_role) AS actor_role,
  any(name) AS name,
  any(phase) AS phase,
  any(source_ref) AS source_ref,
  any(doc_len) AS doc_len,
  any(text_content) AS text_content,
  any(payload_json) AS payload_json,
  toUInt8(any(has_codex_mcp)) AS has_codex_mcp
FROM {documents_table}
GROUP BY event_uid)"
            )
        } else {
            format!(
                "(SELECT
  event_uid,
  any(session_id) AS session_id,
  any(source_name) AS source_name,
  any(provider) AS provider,
  any(event_class) AS event_class,
  any(payload_type) AS payload_type,
  any(actor_role) AS actor_role,
  any(name) AS name,
  any(phase) AS phase,
  any(source_ref) AS source_ref,
  any(doc_len) AS doc_len,
  any(text_content) AS text_content,
  any(payload_json) AS payload_json,
  toUInt8(0) AS has_codex_mcp
FROM {documents_table}
GROUP BY event_uid)"
            )
        };

        let mut where_clauses = vec![format!("p.term IN {}", terms_array_sql)];

        if let Some(sid) = session_id {
            where_clauses.push(format!("d.session_id = {}", sql_quote(sid)));
        }

        if include_tool_events {
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
  any(d.session_id) AS session_id,
  any(d.source_name) AS source_name,
  any(d.provider) AS provider,
  any(d.event_class) AS event_class,
  any(d.payload_type) AS payload_type,
  any(d.actor_role) AS actor_role,
  any(d.name) AS name,
  any(d.phase) AS phase,
  any(d.source_ref) AS source_ref,
  any(d.doc_len) AS doc_len,
  leftUTF8(any(d.text_content), {preview}) AS text_preview,
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
            postings_table = postings_table,
            documents_join_sql = documents_join_sql,
        ))
    }

    fn build_search_events_hydrate_sql(
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
        let codex_expr = if use_document_codex_flag {
            "d.has_codex_mcp"
        } else {
            "toUInt8(positionCaseInsensitiveUTF8(d.payload_json, 'codex-mcp') > 0)"
        };
        let documents_source_sql = if use_document_codex_flag {
            format!(
                "(SELECT
  event_uid,
  any(session_id) AS session_id,
  any(source_name) AS source_name,
  any(provider) AS provider,
  any(event_class) AS event_class,
  any(payload_type) AS payload_type,
  any(actor_role) AS actor_role,
  any(name) AS name,
  any(phase) AS phase,
  any(source_ref) AS source_ref,
  any(doc_len) AS doc_len,
  any(text_content) AS text_content,
  any(payload_json) AS payload_json,
  toUInt8(any(has_codex_mcp)) AS has_codex_mcp
FROM {documents_table}
WHERE event_uid IN {event_uids_array}
GROUP BY event_uid)"
            )
        } else {
            format!(
                "(SELECT
  event_uid,
  any(session_id) AS session_id,
  any(source_name) AS source_name,
  any(provider) AS provider,
  any(event_class) AS event_class,
  any(payload_type) AS payload_type,
  any(actor_role) AS actor_role,
  any(name) AS name,
  any(phase) AS phase,
  any(source_ref) AS source_ref,
  any(doc_len) AS doc_len,
  any(text_content) AS text_content,
  any(payload_json) AS payload_json,
  toUInt8(0) AS has_codex_mcp
FROM {documents_table}
WHERE event_uid IN {event_uids_array}
GROUP BY event_uid)"
            )
        };

        Ok(format!(
            "SELECT
  d.event_uid AS event_uid,
  d.session_id AS session_id,
  d.source_name AS source_name,
  d.provider AS provider,
  d.event_class AS event_class,
  d.payload_type AS payload_type,
  d.actor_role AS actor_role,
  d.name AS name,
  d.phase AS phase,
  d.source_ref AS source_ref,
  d.doc_len AS doc_len,
  leftUTF8(d.text_content, {preview}) AS text_preview,
  {codex_expr} AS has_codex_mcp
FROM {documents_source_sql} AS d
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            codex_expr = codex_expr,
            documents_source_sql = documents_source_sql,
        ))
    }

    async fn search_documents_has_codex_flag(&self) -> RepoResult<bool> {
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
        let rows: Vec<ColumnExistsRow> =
            self.map_backend(self.ch.query_rows(&query, None).await)?;
        let exists = rows.first().map(|row| row.exists != 0).unwrap_or(false);

        let mut cache = self.stats_cache.write().await;
        cache.has_codex_flag_column = Some((exists, now));
        Ok(exists)
    }

    fn passes_search_doc_filters(
        row: &SearchDocExtraCacheEntry,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
    ) -> bool {
        if let Some(sid) = session_id {
            if row.session_id != sid {
                return false;
            }
        }

        if include_tool_events {
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
            if row.name.eq_ignore_ascii_case("search") || row.name.eq_ignore_ascii_case("open") {
                return false;
            }
        }

        true
    }

    fn bm25_term_score(tf: u16, doc_len: u32, avgdl: f64, k1: f64, b: f64) -> f64 {
        let tf = tf as f64;
        let norm = tf + k1 * (1.0 - b + b * (doc_len as f64 / avgdl.max(1.0)));
        if norm <= 0.0 {
            0.0
        } else {
            tf * (k1 + 1.0) / norm
        }
    }

    fn bm25_idf(docs: u64, df: u64) -> f64 {
        let idf = if df == 0 {
            (1.0 + ((docs as f64 + 0.5) / 0.5)).ln()
        } else {
            let n = docs.max(df) as f64;
            (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln()
        };
        idf.max(0.0)
    }

    async fn load_term_postings_for_terms(
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
                self.map_backend(self.ch.query_rows(&query, None).await)?;
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

    async fn load_search_doc_extras(
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
                self.map_backend(self.ch.query_rows(&query, None).await)?;

            let mut cache = self.search_doc_extra_cache.write().await;

            for row in fetched_rows {
                let entry = SearchDocExtraCacheEntry {
                    session_id: row.session_id,
                    source_name: row.source_name,
                    provider: row.provider,
                    event_class: row.event_class,
                    payload_type: row.payload_type,
                    actor_role: row.actor_role,
                    name: row.name,
                    phase: row.phase,
                    source_ref: row.source_ref,
                    doc_len: row.doc_len,
                    text_preview: row.text_preview,
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

    async fn search_events_rows(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
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
                exclude_codex_mcp,
                session_id,
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
            exclude_codex_mcp,
            session_id,
            min_should_match,
            min_score,
            limit,
        )
        .await
    }

    async fn search_events_rows_exact_sql(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
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
            exclude_codex_mcp,
            use_document_codex_flag,
            session_id,
            min_should_match,
            min_score,
            limit,
        )?;

        let mut fallback_rows: Vec<SearchRow> =
            self.map_backend(self.ch.query_rows(&fallback_sql, None).await)?;
        fallback_rows.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.event_uid.cmp(&b.event_uid))
        });
        Ok(fallback_rows)
    }

    async fn search_events_rows_by_strategy(
        &self,
        strategy: SearchEventsStrategy,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> RepoResult<Vec<SearchRow>> {
        match strategy {
            SearchEventsStrategy::Optimized => {
                self.search_events_rows(
                    terms,
                    docs,
                    avgdl,
                    include_tool_events,
                    exclude_codex_mcp,
                    session_id,
                    min_should_match,
                    min_score,
                    limit,
                )
                .await
            }
            SearchEventsStrategy::OracleExact => {
                self.search_events_rows_exact_sql(
                    terms,
                    docs,
                    avgdl,
                    include_tool_events,
                    exclude_codex_mcp,
                    session_id,
                    min_should_match,
                    min_score,
                    limit,
                )
                .await
            }
        }
    }

    async fn search_events_rows_fast_pass(
        &self,
        terms: &[String],
        docs: u64,
        avgdl: f64,
        include_tool_events: bool,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
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

        let postings_by_term = self.load_term_postings_for_terms(terms).await?;
        let use_document_codex_flag = self.search_documents_has_codex_flag().await?;
        let df_map = self.df_map(terms).await?;
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
                    exclude_codex_mcp,
                    session_id,
                ) {
                    continue;
                }

                fast_rows.push(SearchRow {
                    event_uid: row.row.event_uid.clone(),
                    session_id: extra.session_id.clone(),
                    source_name: extra.source_name.clone(),
                    provider: extra.provider.clone(),
                    event_class: extra.event_class.clone(),
                    payload_type: extra.payload_type.clone(),
                    actor_role: extra.actor_role.clone(),
                    name: extra.name.clone(),
                    phase: extra.phase.clone(),
                    source_ref: extra.source_ref.clone(),
                    doc_len: extra.doc_len,
                    text_preview: extra.text_preview.clone(),
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

    fn conversation_candidate_limit(limit: u16) -> usize {
        (limit as usize)
            .saturating_mul(CONVERSATION_CANDIDATE_MULTIPLIER)
            .clamp(CONVERSATION_CANDIDATE_MIN, CONVERSATION_CANDIDATE_MAX)
    }

    fn now_unix_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default()
    }

    fn build_conversation_postings_filter_sql(
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
            postings_filters.push("lowerUTF8(p.name) NOT IN ('search', 'open')".to_string());
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

    fn build_search_conversation_candidates_sql(
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
  c.session_id,
  c.score,
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

    fn build_search_conversation_recent_candidates_sql(
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
  c.session_id,
  c.score,
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

    async fn fetch_conversation_candidates(
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
            self.map_backend(self.ch.query_rows(&persistent_sql, None).await)?;
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
            self.map_backend(self.ch.query_rows(&recent_sql, None).await)?;

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
        candidate_session_ids: Option<&[String]>,
    ) -> RepoResult<String> {
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "cannot build search query with empty terms",
            ));
        }

        let postings_table = self.table_ref("search_postings");
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
  c.session_id,
  c.score,
  toUInt16(c.matched_terms) AS matched_terms,
  toUInt32(c.event_count_considered) AS event_count_considered,
  c.best_event_uid
FROM (
  SELECT
    e.session_id AS session_id,
    sum(e.event_score) AS score,
    {outer_matched_terms_sql} AS matched_terms,
    count() AS event_count_considered,
    argMax(e.event_uid, e.event_score) AS best_event_uid
  FROM (
    SELECT
      p.doc_id AS event_uid,
      any(p.session_id) AS session_id,
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
            mode_join_sql = mode_join_sql,
            mode_filter_sql = mode_filter_sql,
            min_should_match = min_should_match,
            min_score = min_score,
            limit = limit,
        ))
    }

    async fn fetch_conversation_snippets(
        &self,
        event_uids: &[String],
    ) -> RepoResult<HashMap<String, String>> {
        if event_uids.is_empty() {
            return Ok(HashMap::new());
        }

        let documents_table = self.table_ref("search_documents");
        let event_uids_sql = sql_array_strings(event_uids);
        let sql = format!(
            "SELECT
  event_uid,
  leftUTF8(any(text_content), {preview}) AS snippet
FROM {documents_table}
WHERE event_uid IN {event_uids_sql}
GROUP BY event_uid
FORMAT JSONEachRow",
            preview = self.cfg.preview_chars,
            documents_table = documents_table,
            event_uids_sql = event_uids_sql,
        );
        let rows: Vec<ConversationSnippetRow> =
            self.map_backend(self.ch.query_rows(&sql, None).await)?;
        let mut by_uid = HashMap::new();
        for row in rows {
            by_uid.insert(row.event_uid, row.snippet);
        }
        Ok(by_uid)
    }

    async fn log_search_events(
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
        exclude_codex_mcp: bool,
        took_ms: u32,
        hits: &[SearchEventHit],
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
                    "provider": hit.provider,
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
        let disable_cache = query.disable_cache.unwrap_or(false);
        let effective_strategy = query.search_strategy.unwrap_or_default();

        if let Some(session_id) = query.session_id.as_deref() {
            Self::validate_session_id(session_id)?;
        }
        let session_id = query.session_id.as_deref();

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
        let rows_to_hits = |rows: Vec<SearchRow>| -> Vec<SearchEventHit> {
            rows.into_iter()
                .enumerate()
                .map(|(idx, row)| SearchEventHit {
                    rank: idx + 1,
                    event_uid: row.event_uid,
                    session_id: row.session_id,
                    source_name: row.source_name,
                    provider: row.provider,
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
                })
                .collect()
        };

        let hits = if disable_cache {
            let rows = self
                .search_events_rows_by_strategy(
                    effective_strategy,
                    &terms,
                    docs,
                    avgdl,
                    include_tool_events,
                    exclude_codex_mcp,
                    session_id,
                    min_should_match,
                    min_score,
                    limit,
                )
                .await?;
            rows_to_hits(rows)
        } else {
            let cache_key = Self::search_events_cache_key(
                &terms,
                effective_strategy,
                include_tool_events,
                exclude_codex_mcp,
                session_id,
                min_should_match,
                min_score,
                limit,
            );

            if let Some(cached_hits) = self.search_events_cache_get(&cache_key).await {
                cached_hits
            } else {
                let fresh_rows = self
                    .search_events_rows_by_strategy(
                        effective_strategy,
                        &terms,
                        docs,
                        avgdl,
                        include_tool_events,
                        exclude_codex_mcp,
                        session_id,
                        min_should_match,
                        min_score,
                        limit,
                    )
                    .await?;
                let fresh_hits = rows_to_hits(fresh_rows);
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
                session_id.unwrap_or(""),
                &terms,
                limit,
                min_should_match,
                min_score,
                include_tool_events,
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
            self.map_backend(self.ch.query_rows(&sql, None).await)?;
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

        let hits = rows
            .into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let best_event_uid = if row.best_event_uid.is_empty() {
                    None
                } else {
                    Some(row.best_event_uid)
                };
                let snippet = best_event_uid
                    .as_ref()
                    .and_then(|event_uid| snippet_by_event_uid.get(event_uid).cloned())
                    .or(if row.snippet.is_empty() {
                        None
                    } else {
                        Some(row.snippet)
                    });
                ConversationSearchHit {
                    rank: idx + 1,
                    session_id: row.session_id,
                    score: row.score,
                    matched_terms: row.matched_terms,
                    event_count_considered: row.event_count_considered,
                    best_event_uid,
                    snippet,
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

    fn sample_search_doc() -> SearchDocExtraCacheEntry {
        SearchDocExtraCacheEntry {
            session_id: "session-1".to_string(),
            source_name: "source".to_string(),
            provider: "provider".to_string(),
            event_class: "message".to_string(),
            payload_type: "message".to_string(),
            actor_role: "assistant".to_string(),
            name: "tool".to_string(),
            phase: "".to_string(),
            source_ref: "source-ref".to_string(),
            doc_len: 42,
            text_preview: "preview".to_string(),
            has_codex_mcp: 0,
            fetched_at: Instant::now(),
        }
    }

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

    #[test]
    fn search_doc_filters_exclude_codex_by_flag() {
        let mut row = sample_search_doc();
        row.has_codex_mcp = 1;
        assert!(
            !ClickHouseConversationRepository::passes_search_doc_filters(&row, false, true, None)
        );
    }

    #[test]
    fn search_doc_filters_exclude_codex_by_tool_name() {
        let mut row = sample_search_doc();
        row.name = "search".to_string();
        assert!(
            !ClickHouseConversationRepository::passes_search_doc_filters(&row, false, true, None)
        );
    }
}
