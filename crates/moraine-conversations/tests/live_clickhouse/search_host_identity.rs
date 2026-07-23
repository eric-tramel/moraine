use super::*;
use moraine_conversations::{SearchEventsQuery, SearchStrategyHint};

const SHARED_UID: &str = "search-host-shared-uid";
const STALE_UID: &str = "search-host-stale-uid";
const ORPHAN_UID: &str = "search-host-orphan-uid";

#[derive(Debug, Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(Debug, Deserialize)]
struct TermStatsRow {
    docs: u64,
}

#[derive(Clone, Debug)]
struct SearchEvidence {
    event_uid: String,
    session_id: String,
    rank: usize,
    score: f64,
    text_content: Option<String>,
}

async fn insert_fixture(clickhouse: &ClickHouseClient, database: &str) -> Result<()> {
    let heads = format!(
        "INSERT INTO `{database}`.`published_source_generations`
         (source_host, source_name, source_file, source_generation,
          publication_revision, publisher_id, operation_id)
         VALUES
         ('host-a', 'codex', '/fixtures/host-a.jsonl', 1, 101, 'test', 'publish-a'),
         ('host-b', 'codex', '/fixtures/host-b.jsonl', 1, 102, 'test', 'publish-b')"
    );
    clickhouse
        .request_text(&heads, None, Some(database), false, None)
        .await
        .context("failed to publish host-qualified search fixture heads")?;

    let events = format!(
        "INSERT INTO `{database}`.`events`
         (ingested_at, source_host, event_uid, session_id, session_date,
          source_name, harness, inference_provider, source_file, source_inode,
          source_generation, source_line_no, source_offset, source_ref,
          record_ts, event_ts, event_kind, actor_kind, payload_type,
          text_content, text_preview, payload_json, event_version)
         VALUES
         (now64(3), 'host-a', '{SHARED_UID}', 'search-host-session-a', '2026-07-20',
          'codex', 'codex', 'openai', '/fixtures/host-a.jsonl', 11, 1, 1, 1,
          '/fixtures/host-a.jsonl:1:1', '2026-07-20T12:00:00Z',
          toDateTime64('2026-07-20 12:00:00', 3), 'message', 'assistant', 'message',
          'commonidentity alphahosttext', 'commonidentity alphahosttext',
          '{{\"text\":\"commonidentity alphahosttext\"}}', 11),
         (now64(3), 'host-b', '{SHARED_UID}', 'search-host-session-b', '2026-07-20',
          'codex', 'codex', 'openai', '/fixtures/host-b.jsonl', 22, 1, 1, 1,
          '/fixtures/host-b.jsonl:1:1', '2026-07-20T12:00:01Z',
          toDateTime64('2026-07-20 12:00:01', 3), 'message', 'assistant', 'message',
          'commonidentity betahosttext', 'commonidentity betahosttext',
          '{{\"text\":\"commonidentity betahosttext\"}}', 21),
         (now64(3), 'host-a', '{STALE_UID}', 'search-host-stale-session', '2026-07-20',
          'codex', 'codex', 'openai', '/fixtures/host-a.jsonl', 11, 1, 2, 2,
          '/fixtures/host-a.jsonl:1:2', '2026-07-20T12:00:02Z',
          toDateTime64('2026-07-20 12:00:02', 3), 'message', 'assistant', 'message',
          'canonicalownertext', 'canonicalownertext',
          '{{\"text\":\"canonicalownertext\"}}', 31)"
    );
    clickhouse
        .request_text(&events, None, Some(database), false, None)
        .await
        .context("failed to insert host-qualified canonical events")?;

    // These rows have a current published generation but no exact canonical
    // event revision. The stale replacement wins search_documents FINAL over
    // the MV-produced owner document; the orphan has no owner at all.
    let unauthorized_documents = format!(
        "INSERT INTO `{database}`.`search_documents`
         (doc_version, ingested_at, event_uid, session_id, session_date,
          source_host, source_name, harness, inference_provider, source_file,
          source_generation, source_line_no, source_offset, source_ref,
          record_ts, event_class, payload_type, actor_role, text_content,
          payload_json)
         VALUES
         (32, now64(3), '{STALE_UID}', 'search-host-stale-session', '2026-07-20',
          'host-a', 'codex', 'codex', 'openai', '/fixtures/host-a.jsonl', 1, 2, 2,
          '/fixtures/host-a.jsonl:1:2', '2026-07-20T12:00:02Z',
          'message', 'message', 'assistant', 'staleghostterm',
          '{{\"text\":\"staleghostterm\"}}'),
         (41, now64(3), '{ORPHAN_UID}', 'search-host-orphan-session', '2026-07-20',
          'host-a', 'codex', 'codex', 'openai', '/fixtures/host-a.jsonl', 1, 3, 3,
          '/fixtures/host-a.jsonl:1:3', '2026-07-20T12:00:03Z',
          'message', 'message', 'assistant', 'orphanghostterm',
          '{{\"text\":\"orphanghostterm\"}}')"
    );
    clickhouse
        .request_text(&unauthorized_documents, None, Some(database), false, None)
        .await
        .context("failed to insert stale/orphan search documents")?;
    Ok(())
}

async fn search(
    repository: &ClickHouseConversationRepository,
    query: &str,
    strategy_hint: SearchStrategyHint,
    limit: u16,
) -> Result<Vec<SearchEvidence>> {
    let result = repository
        .search_events(SearchEventsQuery {
            query: query.to_string(),
            source: Some("search-host-identity-live-test".to_string()),
            limit: Some(limit),
            min_score: Some(0.0),
            min_should_match: Some(1),
            bypass_cache: Some(true),
            strategy_hint: Some(strategy_hint),
            ..SearchEventsQuery::default()
        })
        .await
        .with_context(|| format!("search failed for {query:?} via {strategy_hint:?}"))?;
    Ok(result
        .hits
        .into_iter()
        .map(|hit| SearchEvidence {
            event_uid: hit.event_uid,
            session_id: hit.session_id,
            rank: hit.rank,
            score: hit.score,
            text_content: hit.text_content,
        })
        .collect())
}

fn assert_same_ranking(
    context: &str,
    before: &[SearchEvidence],
    after: &[SearchEvidence],
) -> Result<()> {
    if before.len() != after.len() {
        bail!("{context} changed search cardinality: before={before:?}, after={after:?}");
    }
    for (before, after) in before.iter().zip(after) {
        if before.event_uid != after.event_uid
            || before.session_id != after.session_id
            || before.rank != after.rank
            || before.text_content != after.text_content
            || (before.score - after.score).abs() > 1e-12
        {
            bail!("{context} changed ranking/BM25 evidence: before={before:?}, after={after:?}");
        }
    }
    Ok(())
}

fn assert_expected_score(context: &str, hits: &[SearchEvidence], expected: f64) -> Result<()> {
    for hit in hits {
        if (hit.score - expected).abs() > 1e-10 {
            bail!(
                "{context} used incorrect BM25 document frequency: expected score {expected:.12}, got {hit:?}"
            );
        }
    }
    Ok(())
}

async fn assert_fixture_semantics(
    clickhouse: &ClickHouseClient,
    database: &str,
) -> Result<Vec<SearchEvidence>> {
    let physical_query = format!(
        "SELECT toUInt64(count()) AS count
         FROM `{database}`.`search_documents` FINAL
         WHERE event_uid IN ('{STALE_UID}', '{ORPHAN_UID}')
         FORMAT JSONEachRow"
    );
    let physical: Vec<CountRow> = clickhouse
        .query_rows(&physical_query, Some(database))
        .await
        .context("failed to count physical stale/orphan documents")?;
    if physical.first().map(|row| row.count) != Some(2) {
        bail!("stale/orphan documents were not physically present: {physical_query}");
    }

    let live_query = format!(
        "SELECT toUInt64(count()) AS count
         FROM `{database}`.`v_live_search_documents`
         WHERE event_uid IN ('{STALE_UID}', '{ORPHAN_UID}')
         FORMAT JSONEachRow"
    );
    let live: Vec<CountRow> = clickhouse
        .query_rows(&live_query, Some(database))
        .await
        .context("failed to count live stale/orphan documents")?;
    if live.first().map(|row| row.count) != Some(0) {
        bail!("stale/orphan current-generation documents became live");
    }

    let term_stats_query = format!(
        "SELECT toUInt64(docs) AS docs
         FROM `{database}`.`search_term_stats`
         WHERE term = 'commonidentity'
         FORMAT JSONEachRow"
    );
    let term_stats: Vec<TermStatsRow> = clickhouse
        .query_rows(&term_stats_query, Some(database))
        .await
        .context("failed to inspect host-qualified document frequency")?;
    if term_stats.first().map(|row| row.docs) != Some(2) {
        bail!("commonidentity DF must count both host-qualified documents: {term_stats:?}");
    }

    let repository = ClickHouseConversationRepository::new(
        clickhouse.clone(),
        RepoConfig {
            async_log_writes: false,
            ..RepoConfig::default()
        },
    );
    for unauthorized_term in ["staleghostterm", "orphanghostterm"] {
        let hits = search(
            &repository,
            unauthorized_term,
            SearchStrategyHint::Exact,
            10,
        )
        .await?;
        if !hits.is_empty() {
            bail!("unauthorized term {unauthorized_term:?} returned hits: {hits:?}");
        }
    }

    let common = search(&repository, "commonidentity", SearchStrategyHint::Exact, 10).await?;
    if common.len() != 2
        || common.iter().any(|hit| hit.event_uid != SHARED_UID)
        || common[0].session_id != "search-host-session-a"
        || common[1].session_id != "search-host-session-b"
        || common
            .iter()
            .any(|hit| !hit.score.is_finite() || hit.score <= 0.0)
    {
        bail!("cross-host identical UIDs were mixed or dropped: {common:?}");
    }
    // N=2 and df=2, while tf=1 and dl=avgdl=2 make the BM25 term-frequency
    // factor exactly one. The expected score is therefore ln(1 + 0.5/2.5).
    let expected_common_score = 1.2_f64.ln();
    assert_expected_score(
        "exact commonidentity search",
        &common,
        expected_common_score,
    )?;
    let optimized_common = search(
        &repository,
        "commonidentity",
        SearchStrategyHint::PreferPerformance,
        10,
    )
    .await?;
    assert_same_ranking(
        "optimized versus exact commonidentity search",
        &common,
        &optimized_common,
    )?;
    assert_expected_score(
        "optimized commonidentity search",
        &optimized_common,
        expected_common_score,
    )?;

    let alpha = search(
        &repository,
        "alphahosttext",
        SearchStrategyHint::PreferPerformance,
        10,
    )
    .await?;
    let beta = search(
        &repository,
        "betahosttext",
        SearchStrategyHint::PreferPerformance,
        10,
    )
    .await?;
    if alpha.len() != 1
        || alpha[0].session_id != "search-host-session-a"
        || alpha[0]
            .text_content
            .as_deref()
            .is_none_or(|text| !text.contains("alphahosttext"))
    {
        bail!("host-a text hydrated from the wrong document: {alpha:?}");
    }
    if beta.len() != 1
        || beta[0].session_id != "search-host-session-b"
        || beta[0]
            .text_content
            .as_deref()
            .is_none_or(|text| !text.contains("betahosttext"))
    {
        bail!("host-b text hydrated from the wrong document: {beta:?}");
    }
    // Each host-specific term has df=1 in a two-document corpus, with the
    // same unit term-frequency factor, yielding ln(1 + 1.5/1.5) = ln(2).
    assert_expected_score("host-a unique term", &alpha, 2.0_f64.ln())?;
    assert_expected_score("host-b unique term", &beta, 2.0_f64.ln())?;

    let top_one = search(&repository, "commonidentity", SearchStrategyHint::Exact, 1).await?;
    if top_one.len() != 1 || top_one[0].session_id != "search-host-session-a" {
        bail!("host-qualified top-k tie break was unstable: {top_one:?}");
    }
    Ok(common)
}

#[tokio::test]
#[ignore = "requires wrapper-owned live ClickHouse and destructive opt-in"]
async fn live_search_host_identity_and_exact_document_revision() -> Result<()> {
    with_live_fixture_envelope(live_search_host_identity_and_exact_document_revision_body()).await
}

async fn live_search_host_identity_and_exact_document_revision_body() -> Result<()> {
    let prerequisites = LivePrerequisites::load()?;
    let database = prepare_owned_database_identity(&prerequisites.sandbox_id)?;
    let clickhouse = live_client(&prerequisites, &database)?;
    assert_owned_database_census_empty(&clickhouse, "before mutation").await?;

    let outcome = async {
        clickhouse
            .run_migrations()
            .await
            .context("failed to migrate host-qualified search database")?;
        insert_fixture(&clickhouse, database.as_str()).await?;
        let before = assert_fixture_semantics(&clickhouse, database.as_str()).await?;

        for table in ["events", "search_documents", "search_postings"] {
            let optimize = format!("OPTIMIZE TABLE `{}`.`{table}` FINAL", database.as_str());
            clickhouse
                .request_text(&optimize, None, Some(database.as_str()), false, None)
                .await
                .with_context(|| format!("failed to optimize {table} FINAL"))?;
        }

        let after = assert_fixture_semantics(&clickhouse, database.as_str()).await?;
        assert_same_ranking("OPTIMIZE FINAL", &before, &after)
    }
    .await;

    let cleanup = cleanup_database(&clickhouse, &database).await;
    let census = assert_owned_database_census_empty(&clickhouse, "after cleanup").await;
    finish_with_cleanup(outcome, finish_with_cleanup(cleanup, census))
}
