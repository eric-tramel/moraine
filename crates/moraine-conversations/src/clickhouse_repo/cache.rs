use super::*;

pub(super) const BENCHMARK_REPLAY_SOURCE: &str = "benchmark-replay";
pub(super) const ANALYTICS_CACHE_TTL: Duration = Duration::from_secs(30);
pub(super) const ANALYTICS_RANGE_COUNT: usize = AnalyticsRange::ALL.len();
pub(super) const CORPUS_STATS_CACHE_TTL: Duration = Duration::from_secs(30);
pub(super) const TERM_DF_CACHE_TTL: Duration = Duration::from_secs(300);
pub(super) const SEARCH_SCHEMA_CACHE_TTL: Duration = Duration::from_secs(60);
pub(super) const SEARCH_RESULT_CACHE_TTL: Duration = Duration::from_secs(15);
pub(super) const SEARCH_RESULT_CACHE_MAX_ENTRIES: usize = 256;
pub(super) const TERM_POSTINGS_CACHE_TTL: Duration = Duration::from_secs(15);
pub(super) const TERM_POSTINGS_CACHE_MAX_ENTRIES: usize = 2048;
pub(super) const TERM_POSTINGS_CACHE_MAX_ROWS_PER_TERM: usize = 131_072;
pub(super) const TERM_POSTINGS_CACHE_MAX_ROWS_TOTAL: usize = 262_144;
pub(super) const TERM_POSTINGS_FAST_PATH_MAX_ROWS_PER_TERM: u64 =
    TERM_POSTINGS_CACHE_MAX_ROWS_PER_TERM as u64;
pub(super) const TERM_POSTINGS_FAST_PATH_RATIO_MIN_DOCS: u64 = 10_000;
pub(super) const TERM_POSTINGS_FAST_PATH_MAX_DOC_RATIO_NUMERATOR: u64 = 1;
pub(super) const TERM_POSTINGS_FAST_PATH_MAX_DOC_RATIO_DENOMINATOR: u64 = 4;
// 60s (issue #443): hydrated doc rows are near-immutable — an event_uid's
// content only moves when a mutable source (cursor bubble) re-emits it — and
// agents issue bursts of overlapping searches, so a short TTL re-reads the
// same fat search_documents granules over and over. The cost of staleness is
// a preview up to a minute old, never a wrong hit.
pub(super) const SEARCH_DOC_EXTRA_CACHE_TTL: Duration = Duration::from_secs(60);
pub(super) const SEARCH_DOC_EXTRA_CACHE_MAX_ENTRIES: usize = 65536;

#[derive(Debug, Clone)]
pub(super) struct AnalyticsCacheEntry {
    pub(super) snapshot: AnalyticsSnapshot,
    pub(super) fetched_at: Instant,
}

impl AnalyticsCacheEntry {
    pub(super) fn is_fresh(&self, now: Instant) -> bool {
        now.checked_duration_since(self.fetched_at)
            .unwrap_or_default()
            <= ANALYTICS_CACHE_TTL
    }
}

pub(super) const fn analytics_range_index(range: AnalyticsRange) -> usize {
    match range {
        AnalyticsRange::FifteenMinutes => 0,
        AnalyticsRange::OneHour => 1,
        AnalyticsRange::SixHours => 2,
        AnalyticsRange::TwentyFourHours => 3,
        AnalyticsRange::SevenDays => 4,
        AnalyticsRange::ThirtyDays => 5,
    }
}

#[derive(Debug, Clone)]
pub(super) struct CorpusStatsCacheEntry {
    pub(super) docs: u64,
    pub(super) total_doc_len: u64,
    pub(super) fetched_at: Instant,
}

#[derive(Debug, Clone)]
pub(super) struct TermDfCacheEntry {
    pub(super) df: u64,
    pub(super) fetched_at: Instant,
}

#[derive(Debug, Default)]
pub(super) struct SearchStatsCache {
    pub(super) corpus_stats: Option<CorpusStatsCacheEntry>,
    pub(super) term_df_by_term: HashMap<String, TermDfCacheEntry>,
    pub(super) has_codex_flag_column: Option<(bool, Instant)>,
}

#[derive(Debug, Clone)]
pub(super) struct SearchEventsCacheEntry {
    pub(super) hits: Vec<SearchEventHit>,
    pub(super) fetched_at: Instant,
}

#[derive(Debug, Clone)]
pub(super) struct TermPostingsCacheEntry {
    pub(super) rows: Arc<[CachedPostingRow]>,
    pub(super) fetched_at: Instant,
}

#[derive(Debug, Clone)]
pub(super) struct SearchDocExtraCacheEntry {
    pub(super) session_id: String,
    pub(super) event_time: String,
    pub(super) source_name: String,
    pub(super) harness: String,
    pub(super) inference_provider: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) actor_role: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) source_ref: String,
    pub(super) doc_len: u32,
    pub(super) text_preview: String,
    pub(super) text_content: String,
    pub(super) payload_json: String,
    pub(super) has_codex_mcp: u8,
    pub(super) fetched_at: Instant,
}

impl ClickHouseConversationRepository {
    pub(super) async fn run_mcp_search_prewarm_queries(
        &self,
        queries: impl IntoIterator<Item = String>,
        limit: u16,
    ) {
        for query in queries {
            let query = query.trim();
            if query.is_empty() || !self.is_safe_mcp_prewarm_query(query) {
                continue;
            }
            if let Err(err) = self
                .search_events_impl(SearchEventsQuery {
                    query: query.to_string(),
                    source: Some(BENCHMARK_REPLAY_SOURCE.to_string()),
                    limit: Some(limit),
                    session_id: None,
                    session_ids: None,
                    min_score: None,
                    min_should_match: None,
                    include_tool_events: None,
                    event_kinds: None,
                    exclude_codex_mcp: None,
                    bypass_cache: Some(false),
                    strategy_hint: Some(SearchStrategyHint::PreferPerformance),
                })
                .await
            {
                warn!("mcp prewarm query failed: {}", err);
            }
        }
    }

    pub(super) fn is_safe_mcp_prewarm_query(&self, query: &str) -> bool {
        Self::is_safe_mcp_prewarm_query_with_max_terms(query, self.cfg.bm25_max_query_terms)
    }

    pub(super) fn is_safe_mcp_prewarm_query_with_max_terms(query: &str, max_terms: usize) -> bool {
        tokenize_query(query, max_terms).len() >= 2
    }

    pub async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        const PREWARM_QUERY_LIMIT: u16 = 10;
        const PREWARM_HOT_QUERY_COUNT: usize = 6;
        const PREWARM_FALLBACK_QUERIES: [&str; 5] = [
            "error stack trace",
            "test failure assertion",
            "file directory path config",
            "function code implementation",
            "session search results",
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn search_events_cache_key(
        terms: &[String],
        strategy_hint: SearchStrategyHint,
        include_tool_events: bool,
        event_kinds: Option<&[SearchEventKind]>,
        exclude_codex_mcp: bool,
        session_id: Option<&str>,
        session_ids: Option<&[String]>,
        min_should_match: u16,
        min_score: f64,
        limit: u16,
    ) -> String {
        let mut cache_terms = terms.to_vec();
        cache_terms.sort_unstable();
        let event_kind_sig = event_kinds
            .map(|kinds| {
                kinds
                    .iter()
                    .map(|kind| kind.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let session_ids_sig = session_ids
            .map(|ids| {
                let mut ids = ids.to_vec();
                ids.sort_unstable();
                ids.join(",")
            })
            .unwrap_or_default();
        format!(
            "strategy={};incl_tools={include_tool_events};event_kinds={event_kind_sig};excl_codex={exclude_codex_mcp};session={};sessions={session_ids_sig};msm={min_should_match};min_score={min_score:.12};limit={limit};terms={}",
            strategy_hint.as_str(),
            session_id.unwrap_or(""),
            cache_terms.join(",")
        )
    }

    pub(super) async fn search_events_cache_get(&self, key: &str) -> Option<Vec<SearchEventHit>> {
        let now = Instant::now();
        {
            let cache = self.search_cache.read().await;
            let entry = cache.get(key)?;
            if now.duration_since(entry.fetched_at) <= SEARCH_RESULT_CACHE_TTL {
                return Some(entry.hits.clone());
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

    pub(super) async fn search_events_cache_put(&self, key: String, hits: &[SearchEventHit]) {
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

    pub(super) async fn corpus_stats(&self) -> RepoResult<(u64, u64)> {
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

    pub(super) async fn cache_corpus_stats(
        &self,
        docs: u64,
        total_doc_len: u64,
        fetched_at: Instant,
    ) {
        let mut cache = self.stats_cache.write().await;
        cache.corpus_stats = Some(CorpusStatsCacheEntry {
            docs,
            total_doc_len,
            fetched_at,
        });
    }

    pub(super) async fn cache_term_df_values(
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

    pub(super) async fn load_hot_queries_for_prewarm(
        &self,
        limit: usize,
    ) -> RepoResult<Vec<String>> {
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

    pub(super) async fn df_map(&self, terms: &[String]) -> RepoResult<HashMap<String, u64>> {
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
}
