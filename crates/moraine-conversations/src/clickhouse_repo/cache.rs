use super::*;

pub(super) const BENCHMARK_REPLAY_SOURCE: &str = "benchmark-replay";
pub(super) const ANALYTICS_CACHE_TTL: Duration = Duration::from_secs(30);
pub(super) const ANALYTICS_RANGE_COUNT: usize = AnalyticsRange::ALL.len();
pub(super) const CORPUS_STATS_CACHE_TTL: Duration = Duration::from_secs(30);
pub(super) const SCOPED_SESSION_CACHE_MAX_ENTRIES: usize = 16_384;
pub(super) const SEARCH_RESULT_CACHE_TTL: Duration = Duration::from_secs(15);
pub(super) const SEARCH_RESULT_CACHE_MAX_ENTRIES: usize = 256;
pub(super) const MCP_SEARCH_RESULT_CACHE_TTL: Duration = Duration::from_secs(15);
pub(super) const MCP_SEARCH_RESULT_CACHE_MAX_ENTRIES: usize = 256;
// 60s (issue #443): hydrated doc rows are near-immutable — an event_uid's
// content only moves when a mutable source (cursor bubble) re-emits it — and
// agents issue bursts of overlapping searches, so a short TTL re-reads the
// same fat search_documents granules over and over. The cost of staleness is
// a preview up to a minute old, never a wrong hit.
pub(super) const SEARCH_DOC_EXTRA_CACHE_TTL: Duration = Duration::from_secs(60);
pub(super) const SEARCH_DOC_EXTRA_CACHE_MAX_ENTRIES: usize = 65536;

#[derive(Debug, Clone)]
pub(super) struct AnalyticsCacheEntry {
    pub(super) publication_token: String,
    pub(super) snapshot: AnalyticsSnapshot,
    pub(super) fetched_at: Instant,
}

impl AnalyticsCacheEntry {
    pub(super) fn is_fresh(&self, now: Instant, publication_token: &str) -> bool {
        self.publication_token == publication_token
            && now
                .checked_duration_since(self.fetched_at)
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

/// Which document population a BM25 score is computed over.
///
/// **Issue #597 B6.** `log(1 + (docs - df + 0.5) / (df + 0.5))` is only a
/// meaningful quantity when `df` is a subset count of `docs`. A statement whose
/// `df` is drawn from one relation and whose `docs`/`avgdl` are drawn from
/// another is not "approximately right" — it is two corpora in one number.
///
/// So the population is a property of the STATEMENT, and it travels with the
/// statement. It is deliberately not derived from the `open_v2` readiness
/// latch: `search_events` and `search_conversations` have no v1 engine to fall
/// back to — they were collapsed onto the bounded ranking core outright — so on
/// an unready backend a latch-driven choice hands them `search_corpus_stats`'s
/// population while their `df` still comes from the locator's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DocumentPopulation {
    /// `search_documents` rows that are published-generation-authorized AND
    /// carry the live canonical `event_version` — the `live_locator` join in
    /// [`ClickHouseConversationRepository::bounded_ranking_ctes`]. Every
    /// statement built on `term_postings` scores this population, so every
    /// statement built on `term_postings` takes its `docs`/`avgdl` from here.
    LocatorAuthorized,
    /// `v_live_search_documents` — published-generation-authorized only, with
    /// no per-event version join. The RETIRED v1 MCP ranking statement computes
    /// its own inline `df` over `v_live_search_postings`, which is authorized
    /// the same way, so this is the population that pairs with it.
    DocumentAuthorized,
}

#[derive(Debug, Clone)]
pub(super) struct CorpusStatsCacheEntry {
    pub(super) publication_token: String,
    /// Part of the cache identity, not payload: the two populations produce
    /// different numbers, and serving one path's entry to the other is the same
    /// defect as choosing the wrong statement.
    pub(super) population: DocumentPopulation,
    pub(super) docs: u64,
    pub(super) total_doc_len: u64,
    pub(super) fetched_at: Instant,
}

/// Issue #597 deleted the second `df` formula and the schema probe that once
/// lived here. Ranking's `df` is now `count() OVER (PARTITION BY term)` inside
/// the one bounded ranking CTE, computed over version- and
/// generation-authorized postings; the retired `term_df_by_term` cache fed an
/// in-process scorer with `uniqExact(tuple(source_host, doc_id))`, a DIFFERENT
/// formula that agreed with ranking's only while the live-postings join stayed
/// 1:1. One df formula ships.
#[derive(Debug, Default)]
pub(super) struct SearchStatsCache {
    pub(super) corpus_stats: Option<CorpusStatsCacheEntry>,
}

pub(super) fn scoped_session_cache_contains(
    cache: &HashMap<String, String>,
    session_id: &str,
    publication_token: &str,
) -> bool {
    cache
        .get(session_id)
        .is_some_and(|cached_token| cached_token == publication_token)
}

pub(super) fn insert_scoped_session_cache_entry(
    cache: &mut HashMap<String, String>,
    session_id: String,
    publication_token: String,
    max_entries: usize,
) {
    if max_entries == 0 {
        return;
    }

    while !cache.contains_key(&session_id) && cache.len() >= max_entries {
        // An entry from another revision cannot hit for this operation, so it
        // is the best eviction candidate. Arbitrary same-revision eviction is
        // still correctness-neutral when the cache is full.
        let eviction = cache
            .iter()
            .find(|(_, cached_token)| *cached_token != &publication_token)
            .or_else(|| cache.iter().next())
            .map(|(session_id, _)| session_id.clone());
        let Some(eviction) = eviction else {
            break;
        };
        cache.remove(&eviction);
    }

    cache.insert(session_id, publication_token);
}

#[derive(Debug, Clone)]
pub(super) struct SearchEventsCacheEntry {
    pub(super) hits: Vec<SearchEventHit>,
    pub(super) fetched_at: Instant,
}

#[derive(Debug, Clone)]
pub(super) struct SearchMcpEventsCacheEntry {
    pub(super) hits: Vec<SearchMcpEventHit>,
    pub(super) truncated: bool,
    pub(super) docs: u64,
    pub(super) avgdl: f64,
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn search_mcp_events_cache_key(
        terms: &[String],
        event_types: &[McpEventType],
        session_id: Option<&str>,
        harness: Option<&str>,
        source_name: Option<&str>,
        turn_seq: Option<u32>,
        min_should_match: u16,
        min_score: f64,
        effective_n_hits: u16,
    ) -> String {
        let mut cache_terms = terms.to_vec();
        cache_terms.sort_unstable();
        let event_type_sig = event_types
            .iter()
            .map(|event_type| event_type.as_str())
            .collect::<Vec<_>>();
        serde_json::to_string(&(
            event_type_sig,
            session_id,
            turn_seq,
            harness,
            source_name,
            min_should_match,
            min_score.to_bits(),
            effective_n_hits,
            cache_terms,
        ))
        .expect("MCP search cache key contains only serializable primitives")
    }

    pub(super) async fn search_mcp_events_cache_get(
        &self,
        key: &str,
    ) -> Option<SearchMcpEventsCacheEntry> {
        let now = Instant::now();
        {
            let cache = self.mcp_search_cache.read().await;
            let entry = cache.get(key)?;
            if now.duration_since(entry.fetched_at) <= MCP_SEARCH_RESULT_CACHE_TTL {
                return Some(entry.clone());
            }
        }

        let mut cache = self.mcp_search_cache.write().await;
        if let Some(entry) = cache.get(key) {
            if now.duration_since(entry.fetched_at) <= MCP_SEARCH_RESULT_CACHE_TTL {
                return Some(entry.clone());
            }
        }
        cache.remove(key);
        None
    }

    pub(super) async fn search_mcp_events_cache_put(
        &self,
        key: String,
        hits: &[SearchMcpEventHit],
        truncated: bool,
        docs: u64,
        avgdl: f64,
    ) {
        let now = Instant::now();
        let mut cache = self.mcp_search_cache.write().await;
        cache
            .retain(|_, entry| now.duration_since(entry.fetched_at) <= MCP_SEARCH_RESULT_CACHE_TTL);

        if cache.len() >= MCP_SEARCH_RESULT_CACHE_MAX_ENTRIES {
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.fetched_at)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(
            key,
            SearchMcpEventsCacheEntry {
                hits: hits.to_vec(),
                truncated,
                docs,
                avgdl,
                fetched_at: now,
            },
        );
    }

    pub(super) async fn cached_corpus_stats(
        &self,
        population: DocumentPopulation,
    ) -> Option<(u64, u64)> {
        let publication_token = publication_cache_key("corpus-stats")?;
        let now = Instant::now();
        let cache = self.stats_cache.read().await;
        cache.corpus_stats.as_ref().and_then(|entry| {
            (entry.publication_token == publication_token
                && entry.population == population
                && now.duration_since(entry.fetched_at) <= CORPUS_STATS_CACHE_TTL)
                .then_some((entry.docs, entry.total_doc_len))
        })
    }

    /// `docs` / `avgdl` for the document population the CALLER's `df` is
    /// counted over (issue #597 B6). The caller names it because the caller is
    /// what knows which ranking relation it scored.
    pub(super) async fn corpus_stats(
        &self,
        population: DocumentPopulation,
    ) -> RepoResult<(u64, u64)> {
        let now = Instant::now();
        if let Some(stats) = self.cached_corpus_stats(population).await {
            return Ok(stats);
        }

        let from_stats_query = match population {
            DocumentPopulation::LocatorAuthorized => self.build_live_corpus_stats_sql(),
            DocumentPopulation::DocumentAuthorized => format!(
                "SELECT toUInt64(ifNull(sum(docs), 0)) AS docs, toUInt64(ifNull(sum(total_doc_len), 0)) AS total_doc_len FROM {} FORMAT JSONEachRow",
                self.table_ref("search_corpus_stats")
            ),
        };

        let from_stats: Vec<CorpusStatsRow> =
            self.map_backend(self.query_rows(&from_stats_query, None).await)?;

        // Issue #597 §1.5/F6: `docs = 0` is a legitimate answer and every caller
        // already handles it. The deleted fallback re-read
        // `count(), sum(doc_len) FROM v_live_search_documents WHERE doc_len > 0`
        // — a whole-corpus aggregation — every time the stats view reported an
        // empty corpus, which is precisely the state a cold or freshly migrated
        // backend is in.
        let resolved = from_stats
            .first()
            .map(|row| (row.docs, row.total_doc_len))
            .unwrap_or((0, 0));

        if let Some(publication_token) = publication_cache_key("corpus-stats") {
            let mut cache = self.stats_cache.write().await;
            cache.corpus_stats = Some(CorpusStatsCacheEntry {
                publication_token,
                population,
                docs: resolved.0,
                total_doc_len: resolved.1,
                fetched_at: now,
            });
        }
        Ok(resolved)
    }

    pub(super) async fn cache_corpus_stats(
        &self,
        population: DocumentPopulation,
        docs: u64,
        total_doc_len: u64,
        fetched_at: Instant,
    ) {
        if let Some(publication_token) = publication_cache_key("corpus-stats") {
            let mut cache = self.stats_cache.write().await;
            cache.corpus_stats = Some(CorpusStatsCacheEntry {
                publication_token,
                population,
                docs,
                total_doc_len,
                fetched_at,
            });
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
        let rows: Vec<HotQueryRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows.into_iter().map(|row| row.raw_query).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_session_cache_replaces_revisions_and_remains_bounded() {
        const TEST_LIMIT: usize = 8;
        let mut cache = HashMap::new();

        for revision in 0..64 {
            insert_scoped_session_cache_entry(
                &mut cache,
                "repeat".to_string(),
                format!("revision-{revision}"),
                TEST_LIMIT,
            );
            assert_eq!(cache.len(), 1);
        }
        assert!(scoped_session_cache_contains(
            &cache,
            "repeat",
            "revision-63"
        ));

        for revision in 0..64 {
            insert_scoped_session_cache_entry(
                &mut cache,
                format!("session-{revision}"),
                format!("revision-{revision}"),
                TEST_LIMIT,
            );
            assert!(cache.len() <= TEST_LIMIT);
        }
    }

    #[test]
    fn scoped_session_cache_never_authorizes_a_concurrent_revision() {
        let mut cache = HashMap::new();

        insert_scoped_session_cache_entry(&mut cache, "session".to_string(), "new".to_string(), 1);
        insert_scoped_session_cache_entry(&mut cache, "session".to_string(), "old".to_string(), 1);

        assert!(scoped_session_cache_contains(&cache, "session", "old"));
        assert!(!scoped_session_cache_contains(&cache, "session", "new"));
    }
}
