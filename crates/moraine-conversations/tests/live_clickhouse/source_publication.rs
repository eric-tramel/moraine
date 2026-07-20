use super::OwnedDatabaseName;
use anyhow::{bail, Context, Result};
use moraine_clickhouse::{ClickHouseClient, McpOpenPublicationRequest, McpOpenSourceHead};
use moraine_conversations::{
    ClickHouseConversationRepository, ConversationRepository, PageRequest, RepoConfig, RepoError,
    SearchEventsQuery, SearchStrategyHint, SessionEventsDirection, SessionEventsQuery,
};
use serde::Deserialize;
use serde_json::{json, Value};

const HOST_A: &str = "publication-host-a";
const HOST_B: &str = "publication-host-b";
const SOURCE: &str = "publication-fixture";
const SOURCE_FILE: &str = "/fixtures/publication.jsonl";
const SIDE_SOURCE: &str = "publication-side";
const SIDE_FILE: &str = "/fixtures/side.jsonl";
const CAUSAL_CHECKPOINT_SOURCE: &str = "publication-causal-checkpoint";
const CAUSAL_CHECKPOINT_FILE: &str = "/fixtures/publication-causal-checkpoint.jsonl";
const CROSS_HOST_UID_G1: &str = "cross-host-uid-g1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplayEventUids {
    g1: &'static str,
    g2: &'static str,
}

impl ReplayEventUids {
    const fn at(self, generation: u32) -> &'static str {
        match generation {
            1 => self.g1,
            2 => self.g2,
            _ => panic!("source-publication fixture supports generations 1 and 2"),
        }
    }
}

const STABLE_UIDS: ReplayEventUids = ReplayEventUids {
    g1: "stable-event-g1",
    g2: "stable-event-g2",
};
const CHANGED_UIDS: ReplayEventUids = ReplayEventUids {
    g1: "changed-event-g1",
    g2: "changed-event-g2",
};
const DELETED_UIDS: ReplayEventUids = ReplayEventUids {
    g1: "deleted-event-g1",
    g2: "deleted-event-g2",
};
const EMPTY_UIDS: ReplayEventUids = ReplayEventUids {
    g1: "empty-event-g1",
    g2: "empty-event-g2",
};
const NEW_UIDS: ReplayEventUids = ReplayEventUids {
    g1: "new-event-g1",
    g2: "new-event-g2",
};
const CROSS_SOURCE_SIDE_G1: &str = "cross-source-side-g1";
const INACTIVE_ADVERSARY_UID_G3: &str = "inactive-adversary-g3";
const APPEND_UID_G2: &str = "append-event-g2";
const ACTIVE_DERIVED_TOOL_CALL: &str = "active-version-skewed-tool";
const INACTIVE_DERIVED_TOOL_CALL: &str = "inactive-matching-version-tool";

#[derive(Debug, Deserialize)]
struct CountRow {
    value: u64,
}

#[derive(Debug, Deserialize)]
struct HeadRow {
    source_generation: u32,
    publication_revision: u64,
}

#[derive(Debug, Deserialize)]
struct CheckpointRow {
    inode: u64,
    source_generation: u32,
    last_offset: u64,
    last_line: u64,
    cursor_json: String,
    checkpoint_revision: u64,
    operation_id: String,
    lifecycle: String,
    final_scan_complete: u8,
    compatibility_prepared: u8,
    backend_caught_up: u8,
}

#[derive(Debug, Deserialize)]
struct AppendControlRow {
    control_revision: u64,
    cache_epoch: u64,
    state: String,
    batch_id: String,
}

#[derive(Debug, Deserialize)]
struct ControlStorageRow {
    table: String,
    rows: u64,
    active_parts: u64,
    compressed_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct PostingEvidenceRow {
    term: String,
    doc_id: String,
    post_version: u64,
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    doc_len: u32,
    tf: u16,
}

#[derive(Debug, Deserialize)]
struct DocumentEvidenceRow {
    event_uid: String,
    doc_version: u64,
    doc_len: u32,
    text_content: String,
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
}

#[derive(Debug, Deserialize)]
struct LiveDocumentSummaryRow {
    event_uid: String,
    doc_len: u32,
}

#[derive(Debug, Deserialize)]
struct CorpusEvidenceRow {
    docs: u64,
    total_doc_len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedPublicationModel {
    GenerationOne,
    GenerationTwo,
}

impl ExpectedPublicationModel {
    const fn source_generation(self) -> u32 {
        match self {
            Self::GenerationOne => 1,
            Self::GenerationTwo => 2,
        }
    }

    const fn publication_revision(self) -> u64 {
        match self {
            Self::GenerationOne => 1,
            Self::GenerationTwo => 4,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DurableBoundaryState {
    model: ExpectedPublicationModel,
    legacy_model: ExpectedPublicationModel,
    prepared_header_count: Option<u64>,
    final_checkpoint_durable: bool,
    final_readiness_durable: bool,
}

fn source_head(
    source_host: &str,
    source_name: &str,
    source_file: &str,
    source_generation: u32,
    publication_revision: u64,
    operation_id: &str,
) -> Value {
    json!({
        "source_host": source_host,
        "source_name": source_name,
        "source_file": source_file,
        "source_generation": source_generation,
        "publication_revision": publication_revision,
        "publisher_id": "source-publication-live-test",
        "operation_id": operation_id,
        "published_at": "2026-07-20 12:00:00.000",
    })
}

#[allow(clippy::too_many_arguments)]
fn event(
    source_host: &str,
    source_name: &str,
    source_file: &str,
    source_generation: u32,
    source_offset: u64,
    event_uid: &str,
    session_id: &str,
    text_content: &str,
    event_version: u64,
) -> Value {
    json!({
        "ingested_at": "2026-07-20 12:00:00.000",
        "event_uid": event_uid,
        "session_id": session_id,
        "session_date": "2026-07-20",
        "source_host": source_host,
        "source_name": source_name,
        "harness": "codex",
        "inference_provider": "openai",
        "source_file": source_file,
        "source_inode": if source_generation == 1 { 71 } else { 72 },
        "source_generation": source_generation,
        "source_line_no": source_offset,
        "source_offset": source_offset,
        "source_ref": event_uid,
        "record_ts": format!("2026-07-20T12:00:{:02}.000Z", source_offset % 60),
        "event_ts": format!("2026-07-20 12:00:{:02}.000", source_offset % 60),
        "event_kind": "message",
        "actor_kind": "assistant",
        "payload_type": "text",
        "turn_index": 1,
        "model": "fixture-model",
        "content_types": ["text"],
        "text_content": text_content,
        "text_preview": text_content,
        "payload_json": "{}",
        "token_usage_json": "{}",
        "event_version": event_version,
    })
}

async fn scalar_u64(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    query: &str,
) -> Result<u64> {
    let rows: Vec<CountRow> = clickhouse
        .query_rows(query, Some(database.as_str()))
        .await
        .context("failed to read source-publication scalar")?;
    rows.into_iter()
        .next()
        .map(|row| row.value)
        .context("source-publication scalar query returned no row")
}

async fn assert_derived_visibility_uses_published_uid(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    clickhouse
        .insert_json_rows_sync(
            "tool_io",
            &[
                json!({
                    "source_host": HOST_A,
                    "event_uid": STABLE_UIDS.at(1),
                    "session_id": "stable-session",
                    "harness": "codex",
                    "source_name": SOURCE,
                    "tool_call_id": ACTIVE_DERIVED_TOOL_CALL,
                    "tool_name": "Read",
                    "tool_phase": "request",
                    "input_json": "{\"path\":\"Cargo.toml\"}",
                    "source_ref": STABLE_UIDS.at(1),
                    // Deliberately differs from the published event's version 1.
                    "event_version": 10_001,
                }),
                json!({
                    "source_host": HOST_A,
                    "event_uid": INACTIVE_ADVERSARY_UID_G3,
                    "session_id": "inactive-session",
                    "harness": "codex",
                    "source_name": SOURCE,
                    "tool_call_id": INACTIVE_DERIVED_TOOL_CALL,
                    "tool_name": "Read",
                    "tool_phase": "request",
                    "input_json": "{\"path\":\"Cargo.toml\"}",
                    "source_ref": INACTIVE_ADVERSARY_UID_G3,
                    // Deliberately matches the inactive event's version 1.
                    "event_version": 1,
                }),
            ],
        )
        .await
        .context("failed to seed derived tool visibility fixtures")?;
    clickhouse
        .insert_json_rows_sync(
            "event_links",
            &[
                json!({
                    "source_host": HOST_A,
                    "event_uid": STABLE_UIDS.at(1),
                    "session_id": "stable-session",
                    "harness": "codex",
                    "source_name": SOURCE,
                    "linked_external_id": "active-version-skewed-parent",
                    "link_type": "parent_event",
                    "metadata_json": "{}",
                    // Deliberately differs from the published event's version 1.
                    "event_version": 10_001,
                }),
                json!({
                    "source_host": HOST_A,
                    "event_uid": INACTIVE_ADVERSARY_UID_G3,
                    "session_id": "inactive-session",
                    "harness": "codex",
                    "source_name": SOURCE,
                    "linked_external_id": "inactive-matching-version-parent",
                    "link_type": "parent_event",
                    "metadata_json": "{}",
                    // Deliberately matches the inactive event's version 1.
                    "event_version": 1,
                }),
            ],
        )
        .await
        .context("failed to seed derived link visibility fixtures")?;

    for (relation, predicate, expected, label) in [
        (
            "v_live_tool_io",
            format!("tool_call_id = '{ACTIVE_DERIVED_TOOL_CALL}'"),
            1,
            "version-skewed published tool row",
        ),
        (
            "v_live_event_links",
            "linked_external_id = 'active-version-skewed-parent'".to_string(),
            1,
            "version-skewed published link row",
        ),
        (
            "v_live_tool_io",
            format!("tool_call_id = '{INACTIVE_DERIVED_TOOL_CALL}'"),
            0,
            "matching-version inactive tool row",
        ),
        (
            "v_live_event_links",
            "linked_external_id = 'inactive-matching-version-parent'".to_string(),
            0,
            "matching-version inactive link row",
        ),
    ] {
        let count = scalar_u64(
            clickhouse,
            database,
            &format!(
                "SELECT toUInt64(count()) AS value FROM `{}`.`{relation}` \
                 WHERE {predicate} FORMAT JSONEachRow",
                database.as_str(),
            ),
        )
        .await?;
        if count != expected {
            bail!("{label} visibility was {count}, expected {expected}");
        }
    }

    Ok(())
}

async fn assert_live_search_precondition(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    phase: &str,
    event_uid: &str,
    term: &str,
) -> Result<()> {
    let live_events = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`v_live_events` \
             WHERE event_uid = '{}' FORMAT JSONEachRow",
            database.as_str(),
            event_uid,
        ),
    )
    .await?;
    let live_documents = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`v_live_search_documents` \
             WHERE event_uid = '{}' FORMAT JSONEachRow",
            database.as_str(),
            event_uid,
        ),
    )
    .await?;
    let document_query = format!(
        "SELECT event_uid, toUInt64(doc_version) AS doc_version, \
                toUInt32(doc_len) AS doc_len, text_content, source_host, source_name, \
                source_file, toUInt32(source_generation) AS source_generation \
         FROM `{}`.`v_live_search_documents` WHERE event_uid = '{}' \
         ORDER BY source_host, source_name, source_file, source_generation \
         FORMAT JSONEachRow",
        database.as_str(),
        event_uid,
    );
    let stable_documents: Vec<DocumentEvidenceRow> = clickhouse
        .query_rows(&document_query, Some(database.as_str()))
        .await
        .with_context(|| format!("failed to inspect live search document {event_uid:?}"))?;
    let all_documents_query = format!(
        "SELECT event_uid, toUInt32(doc_len) AS doc_len \
         FROM `{}`.`v_live_search_documents` ORDER BY event_uid FORMAT JSONEachRow",
        database.as_str(),
    );
    let all_live_documents: Vec<LiveDocumentSummaryRow> = clickhouse
        .query_rows(&all_documents_query, Some(database.as_str()))
        .await
        .context("failed to inspect all live search documents")?;
    let posting_query = format!(
        "SELECT term, doc_id, toUInt64(post_version) AS post_version, source_host, \
                source_name, source_file, toUInt32(source_generation) AS source_generation, \
                toUInt32(doc_len) AS doc_len, toUInt16(tf) AS tf \
         FROM `{}`.`v_live_search_postings` \
         WHERE term = '{}' ORDER BY doc_id, source_host FORMAT JSONEachRow",
        database.as_str(),
        term,
    );
    let postings: Vec<PostingEvidenceRow> = clickhouse
        .query_rows(&posting_query, Some(database.as_str()))
        .await
        .with_context(|| format!("failed to inspect live postings for {term:?}"))?;
    let physical_doc_postings_query = format!(
        "SELECT term, doc_id, toUInt64(post_version) AS post_version, source_host, \
                source_name, source_file, toUInt32(source_generation) AS source_generation, \
                toUInt32(doc_len) AS doc_len, toUInt16(tf) AS tf \
         FROM `{}`.`search_postings` WHERE doc_id = '{}' \
         ORDER BY term, source_host, source_generation FORMAT JSONEachRow",
        database.as_str(),
        event_uid,
    );
    let physical_doc_postings: Vec<PostingEvidenceRow> = clickhouse
        .query_rows(&physical_doc_postings_query, Some(database.as_str()))
        .await
        .with_context(|| format!("failed to inspect physical postings for {event_uid:?}"))?;
    let live_doc_postings_query = format!(
        "SELECT term, doc_id, toUInt64(post_version) AS post_version, source_host, \
                source_name, source_file, toUInt32(source_generation) AS source_generation, \
                toUInt32(doc_len) AS doc_len, toUInt16(tf) AS tf \
         FROM `{}`.`v_live_search_postings` WHERE doc_id = '{}' \
         ORDER BY term, source_host, source_generation FORMAT JSONEachRow",
        database.as_str(),
        event_uid,
    );
    let live_doc_postings: Vec<PostingEvidenceRow> = clickhouse
        .query_rows(&live_doc_postings_query, Some(database.as_str()))
        .await
        .with_context(|| format!("failed to inspect live postings for {event_uid:?}"))?;
    let corpus_query = format!(
        "SELECT toUInt64(docs) AS docs, toUInt64(total_doc_len) AS total_doc_len \
         FROM `{}`.`search_corpus_stats` FORMAT JSONEachRow",
        database.as_str(),
    );
    let corpus = clickhouse
        .query_rows::<CorpusEvidenceRow>(&corpus_query, Some(database.as_str()))
        .await
        .context("failed to inspect live search corpus stats")?
        .into_iter()
        .next()
        .context("live search corpus stats returned no row")?;
    let term_df = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(ifNull(max(docs), 0)) AS value \
             FROM `{}`.`search_term_stats` WHERE term = '{}' FORMAT JSONEachRow",
            database.as_str(),
            term,
        ),
    )
    .await?;
    let evidence = json!({
        "kind": "source_publication_search_precondition",
        "phase": phase,
        "event_uid": event_uid,
        "term": term,
        "live_events": live_events,
        "live_documents": live_documents,
        "stable_documents": stable_documents.iter().map(|row| json!({
            "event_uid": row.event_uid,
            "doc_version": row.doc_version,
            "doc_len": row.doc_len,
            "text_content": row.text_content,
            "source_host": row.source_host,
            "source_name": row.source_name,
            "source_file": row.source_file,
            "source_generation": row.source_generation,
        })).collect::<Vec<_>>(),
        "all_live_documents": all_live_documents.iter().map(|row| json!({
            "event_uid": row.event_uid,
            "doc_len": row.doc_len,
        })).collect::<Vec<_>>(),
        "term_postings": postings.iter().map(|row| json!({
            "term": row.term,
            "doc_id": row.doc_id,
            "post_version": row.post_version,
            "source_host": row.source_host,
            "source_name": row.source_name,
            "source_file": row.source_file,
            "source_generation": row.source_generation,
            "doc_len": row.doc_len,
            "tf": row.tf,
        })).collect::<Vec<_>>(),
        "physical_doc_postings": physical_doc_postings.iter().map(|row| json!({
            "term": row.term,
            "doc_id": row.doc_id,
            "post_version": row.post_version,
            "source_host": row.source_host,
            "source_name": row.source_name,
            "source_file": row.source_file,
            "source_generation": row.source_generation,
            "doc_len": row.doc_len,
            "tf": row.tf,
        })).collect::<Vec<_>>(),
        "live_doc_postings": live_doc_postings.iter().map(|row| json!({
            "term": row.term,
            "doc_id": row.doc_id,
            "post_version": row.post_version,
            "source_host": row.source_host,
            "source_name": row.source_name,
            "source_file": row.source_file,
            "source_generation": row.source_generation,
            "doc_len": row.doc_len,
            "tf": row.tf,
        })).collect::<Vec<_>>(),
        "corpus_docs": corpus.docs,
        "corpus_total_doc_len": corpus.total_doc_len,
        "term_df": term_df,
    });
    eprintln!("{evidence}");

    let expected_posting = postings.iter().any(|row| row.doc_id == event_uid);
    let physical_term_posting = physical_doc_postings
        .iter()
        .any(|row| row.term == term && row.doc_id == event_uid);
    let live_term_posting = live_doc_postings
        .iter()
        .any(|row| row.term == term && row.doc_id == event_uid);
    let expected_document = stable_documents.iter().any(|row| {
        row.event_uid == event_uid && row.text_content.split_whitespace().any(|word| word == term)
    });
    if live_events != 1
        || live_documents != 1
        || stable_documents.len() != 1
        || !expected_document
        || all_live_documents.len() as u64 != corpus.docs
        || postings.len() != 1
        || !expected_posting
        || !physical_term_posting
        || !live_term_posting
        || corpus.docs == 0
        || corpus.total_doc_len == 0
        || term_df != 1
    {
        bail!("live search precondition failed before repository search: {evidence}");
    }
    Ok(())
}

async fn current_head(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<HeadRow> {
    let query = format!(
        "SELECT toUInt32(source_generation) AS source_generation, \
                toUInt64(publication_revision) AS publication_revision \
         FROM `{}`.`v_current_published_source_generations` \
         WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
         FORMAT JSONEachRow",
        database.as_str(),
        HOST_A,
        SOURCE,
        SOURCE_FILE,
    );
    clickhouse
        .query_rows::<HeadRow>(&query, Some(database.as_str()))
        .await
        .context("failed to load current publication head")?
        .into_iter()
        .next()
        .context("current publication head is missing")
}

async fn logical_head_history_count(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<u64> {
    scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_published_source_generation_history` \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await
}

async fn assert_live_term_count(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    term: &str,
    expected: u64,
) -> Result<()> {
    let value = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_live_search_postings` \
             WHERE term = '{}' FORMAT JSONEachRow",
            database.as_str(),
            term,
        ),
    )
    .await?;
    if value != expected {
        bail!("live posting count for {term:?}: expected {expected}, got {value}");
    }
    Ok(())
}

async fn assert_search_uids(
    repository: &ClickHouseConversationRepository,
    query: &str,
    expected: &[&str],
) -> Result<()> {
    let result = repository
        .search_events(SearchEventsQuery {
            query: query.to_string(),
            limit: Some(20),
            bypass_cache: Some(true),
            strategy_hint: Some(SearchStrategyHint::Exact),
            ..SearchEventsQuery::default()
        })
        .await
        .with_context(|| format!("repository search failed for {query:?}"))?;
    let mut actual = result
        .hits
        .iter()
        .map(|hit| hit.event_uid.as_str())
        .collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    if actual != expected {
        bail!("repository search for {query:?}: expected {expected:?}, got {actual:?}");
    }
    Ok(())
}

async fn session_event_texts(
    repository: &ClickHouseConversationRepository,
    session_id: &str,
) -> Result<Vec<(String, String)>> {
    let page = repository
        .list_session_events(
            SessionEventsQuery {
                session_id: session_id.to_string(),
                direction: SessionEventsDirection::Forward,
                event_kinds: None,
            },
            PageRequest {
                limit: 100,
                cursor: None,
            },
        )
        .await
        .with_context(|| format!("failed to resolve session handle {session_id}"))?;
    Ok(page
        .items
        .into_iter()
        .map(|event| (event.event_uid, event.text_content))
        .collect())
}

fn reconstruct_clickhouse_client(durable_client: &ClickHouseClient) -> Result<ClickHouseClient> {
    ClickHouseClient::new(durable_client.config().clone())
        .context("failed to reconstruct an independent ClickHouse client")
}

fn reconstruct_reader_runtime(
    durable_client: &ClickHouseClient,
) -> Result<(ClickHouseClient, ClickHouseConversationRepository)> {
    let restarted_client = reconstruct_clickhouse_client(durable_client)?;
    let restarted_repository =
        ClickHouseConversationRepository::new(restarted_client.clone(), RepoConfig::default());
    Ok((restarted_client, restarted_repository))
}

async fn assert_durable_boundary_state(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    boundary: &str,
    state: DurableBoundaryState,
) -> Result<()> {
    let candidate_readiness_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_current_mcp_open_generation_readiness` \
             WHERE candidate_publication_id = 'source-publication-g2' \
               AND source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND source_generation = 2 AND ready = 1 AND block_reason = '' \
               AND affected_session_count = 6 AND prepared_session_count = 6 \
               AND tombstone_count = 1 \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let candidate_header_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_mcp_open_publication_headers` \
             WHERE candidate_publication_id = 'source-publication-g2' \
             FORMAT JSONEachRow",
            database.as_str(),
        ),
    )
    .await?;
    match state.prepared_header_count {
        Some(expected) => {
            if candidate_readiness_count != 1 || candidate_header_count != expected {
                bail!(
                    "{boundary}: reconstructed compatibility preparation was incomplete: readiness={candidate_readiness_count}, headers={candidate_header_count}, expected_headers={expected}"
                );
            }
        }
        None => {
            if candidate_readiness_count != 0 || candidate_header_count != 0 {
                bail!(
                    "{boundary}: reconstructed runtime observed compatibility rows before preparation: readiness={candidate_readiness_count}, headers={candidate_header_count}"
                );
            }
        }
    }

    let physical_event_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`events` FINAL \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND source_generation = 2 FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let physical_document_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`search_documents` FINAL \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND source_generation = 2 FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let empty_tombstone_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`search_documents` FINAL \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND source_generation = 2 AND event_uid = '{}' \
               AND doc_version = 2 AND doc_len = 0 AND text_content = '' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            EMPTY_UIDS.at(2),
        ),
    )
    .await?;
    // Every event revision now has a search-document replacement row. The
    // fourth row is the empty-text tombstone that keeps an older searchable
    // version from resurfacing; v_live_search_documents still filters it.
    if physical_event_count != 4 || physical_document_count != 4 || empty_tombstone_count != 1 {
        bail!(
            "{boundary}: reconstructed physical generation was incomplete: events={physical_event_count}, documents={physical_document_count}, empty_tombstones={empty_tombstone_count}"
        );
    }

    let replaying_checkpoint_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_current_ingest_checkpoint_transitions` \
             WHERE host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND inode = 72 AND source_generation = 2 \
               AND last_offset = 0 AND last_line = 0 \
               AND checkpoint_revision = 9 AND operation_id = 'begin-primary-g2' \
               AND lifecycle = 'replaying' AND final_scan_complete = 0 \
               AND block_reason = '' AND compatibility_prepared = 0 \
               AND backend_caught_up = 0 FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let expected_replaying_count = u64::from(!state.final_checkpoint_durable);
    if replaying_checkpoint_count != expected_replaying_count {
        bail!(
            "{boundary}: reconstructed replaying checkpoint count was {replaying_checkpoint_count}, expected {expected_replaying_count}"
        );
    }

    let final_checkpoint_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_current_ingest_checkpoint_transitions` \
             WHERE host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND inode = 72 AND source_generation = 2 \
               AND last_offset = 8 AND last_line = 8 \
               AND cursor_json = 'final-cursor-g2' AND checkpoint_revision = 10 \
               AND operation_id = 'publish-primary-g2' AND lifecycle = 'active' \
               AND final_scan_complete = 1 AND block_reason = '' \
               AND compatibility_prepared = 1 AND backend_caught_up = 1 \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let expected_checkpoint_count = u64::from(state.final_checkpoint_durable);
    if final_checkpoint_count != expected_checkpoint_count {
        bail!(
            "{boundary}: reconstructed final checkpoint count was {final_checkpoint_count}, expected {expected_checkpoint_count}"
        );
    }

    let final_readiness_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_current_source_generation_publication_readiness` \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
               AND source_generation = 2 AND readiness_revision = 10 \
               AND checkpoint_revision = 10 AND operation_id = 'publish-primary-g2' \
               AND complete = 1 AND block_reason = '' \
               AND compatibility_prepared = 1 AND backend_caught_up = 1 \
               AND manifest_digest = 'fixture-manifest-g2' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let expected_readiness_count = u64::from(state.final_readiness_durable);
    if final_readiness_count != expected_readiness_count {
        bail!(
            "{boundary}: reconstructed final readiness count was {final_readiness_count}, expected {expected_readiness_count}"
        );
    }
    Ok(())
}

async fn assert_legacy_compatibility_pointer(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    boundary: &str,
    expected: ExpectedPublicationModel,
) -> Result<()> {
    let (expected_uid, hidden_uid) = match expected {
        ExpectedPublicationModel::GenerationOne => (CHANGED_UIDS.at(1), CHANGED_UIDS.at(2)),
        ExpectedPublicationModel::GenerationTwo => (CHANGED_UIDS.at(2), CHANGED_UIDS.at(1)),
    };
    let query_count = |event_uid: &str| {
        format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{database}`.`mcp_open_sessions` AS sessions FINAL \
             INNER JOIN `{database}`.`mcp_open_events` AS events FINAL \
               ON events.session_id = sessions.session_id \
              AND events.slot = sessions.slot AND events.generation = sessions.generation \
             WHERE sessions.session_id = 'changed-session' AND events.event_uid = '{event_uid}' \
             FORMAT JSONEachRow",
            database = database.as_str(),
        )
    };
    let expected_count = scalar_u64(clickhouse, database, &query_count(expected_uid)).await?;
    let hidden_count = scalar_u64(clickhouse, database, &query_count(hidden_uid)).await?;
    if expected_count != 1 || hidden_count != 0 {
        bail!(
            "{boundary}: legacy compatibility pointer was mixed: expected={expected_count}, hidden={hidden_count}"
        );
    }
    Ok(())
}

async fn assert_complete_after_reconstruction(
    durable_client: &ClickHouseClient,
    database: &OwnedDatabaseName,
    boundary: &str,
    state: DurableBoundaryState,
) -> Result<()> {
    // This deliberately constructs a new reqwest client and an empty set of
    // repository caches. The original objects remain only as fixture drivers;
    // all boundary observations are made through this reconstructed runtime.
    let (restarted_client, restarted_repository) = reconstruct_reader_runtime(durable_client)?;
    assert_durable_boundary_state(&restarted_client, database, boundary, state).await?;
    assert_legacy_compatibility_pointer(&restarted_client, database, boundary, state.legacy_model)
        .await?;

    let head = current_head(&restarted_client, database).await?;
    if head.source_generation != state.model.source_generation()
        || head.publication_revision != state.model.publication_revision()
    {
        bail!("{boundary}: reconstructed runtime observed an unexpected head: {head:?}");
    }
    let live_event_count = scalar_u64(
        &restarted_client,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`v_live_events` FORMAT JSONEachRow",
            database.as_str(),
        ),
    )
    .await?;
    let expected_live_event_count = match state.model {
        ExpectedPublicationModel::GenerationOne => 7,
        ExpectedPublicationModel::GenerationTwo => 6,
    };
    if live_event_count != expected_live_event_count {
        bail!(
            "{boundary}: reconstructed runtime observed {live_event_count} live events, expected {expected_live_event_count}"
        );
    }

    let (
        expected_changed,
        hidden_changed,
        expected_stable,
        hidden_stable,
        expected_empty,
        hidden_empty,
        expect_new,
        expect_deleted,
    ) = match state.model {
        ExpectedPublicationModel::GenerationOne => (
            CHANGED_UIDS.at(1),
            CHANGED_UIDS.at(2),
            STABLE_UIDS.at(1),
            STABLE_UIDS.at(2),
            EMPTY_UIDS.at(1),
            EMPTY_UIDS.at(2),
            false,
            true,
        ),
        ExpectedPublicationModel::GenerationTwo => (
            CHANGED_UIDS.at(2),
            CHANGED_UIDS.at(1),
            STABLE_UIDS.at(2),
            STABLE_UIDS.at(1),
            EMPTY_UIDS.at(2),
            EMPTY_UIDS.at(1),
            true,
            false,
        ),
    };
    if restarted_repository
        .get_mcp_event(expected_changed)
        .await?
        .is_none()
    {
        bail!("{boundary}: reconstructed runtime omitted the expected changed event");
    }
    if restarted_repository
        .get_mcp_event(hidden_changed)
        .await?
        .is_some()
    {
        bail!("{boundary}: reconstructed runtime mixed old and new changed events");
    }
    if restarted_repository
        .get_mcp_event(expected_stable)
        .await?
        .is_none()
        || restarted_repository
            .get_mcp_event(hidden_stable)
            .await?
            .is_some()
    {
        bail!("{boundary}: reconstructed runtime observed a mixed stable-event state");
    }
    if restarted_repository
        .get_mcp_event(expected_empty)
        .await?
        .is_none()
        || restarted_repository
            .get_mcp_event(hidden_empty)
            .await?
            .is_some()
    {
        bail!("{boundary}: reconstructed runtime observed a mixed empty-event state");
    }
    let new_event = restarted_repository.get_mcp_event(NEW_UIDS.at(2)).await?;
    if new_event.is_some() != expect_new {
        bail!("{boundary}: reconstructed runtime observed a mixed new-event state");
    }
    let deleted_event = restarted_repository
        .get_mcp_event(DELETED_UIDS.at(1))
        .await?;
    if deleted_event.is_some() != expect_deleted {
        bail!("{boundary}: reconstructed runtime observed a mixed deletion state");
    }

    let changed_session = session_event_texts(&restarted_repository, "changed-session").await?;
    let mut actual_session_uids = changed_session
        .iter()
        .map(|(uid, _)| uid.as_str())
        .collect::<Vec<_>>();
    actual_session_uids.sort_unstable();
    let mut expected_session_uids = vec![expected_changed, CROSS_SOURCE_SIDE_G1];
    expected_session_uids.sort_unstable();
    if actual_session_uids != expected_session_uids {
        bail!("{boundary}: reconstructed runtime did not expose one complete cross-source session");
    }

    match state.model {
        ExpectedPublicationModel::GenerationOne => {
            assert_search_uids(&restarted_repository, "beforeterm", &[CHANGED_UIDS.at(1)]).await?;
            assert_search_uids(&restarted_repository, "replacementterm", &[]).await?;
            assert_search_uids(&restarted_repository, "deletedterm", &[DELETED_UIDS.at(1)]).await?;
            assert_search_uids(&restarted_repository, "newgenerationterm", &[]).await?;
            assert_search_uids(&restarted_repository, "oldemptyterm", &[EMPTY_UIDS.at(1)]).await?;
            assert_search_uids(&restarted_repository, "identicalterm", &[STABLE_UIDS.at(1)])
                .await?;
        }
        ExpectedPublicationModel::GenerationTwo => {
            assert_search_uids(&restarted_repository, "beforeterm", &[]).await?;
            assert_search_uids(
                &restarted_repository,
                "replacementterm",
                &[CHANGED_UIDS.at(2)],
            )
            .await?;
            assert_search_uids(&restarted_repository, "deletedterm", &[]).await?;
            assert_search_uids(
                &restarted_repository,
                "newgenerationterm",
                &[NEW_UIDS.at(2)],
            )
            .await?;
            assert_search_uids(&restarted_repository, "oldemptyterm", &[]).await?;
            assert_search_uids(&restarted_repository, "identicalterm", &[STABLE_UIDS.at(2)])
                .await?;
        }
    }
    assert_search_uids(&restarted_repository, "inactiveadversaryterm", &[]).await?;

    eprintln!(
        "{}",
        json!({
            "kind": "source_publication_restart_boundary",
            "boundary": boundary,
            "observed_generation": head.source_generation,
            "observed_publication_revision": head.publication_revision,
            "legacy_generation": state.legacy_model.source_generation(),
            "candidate_prepared": state.prepared_header_count.is_some(),
            "final_checkpoint_durable": state.final_checkpoint_durable,
            "final_readiness_durable": state.final_readiness_durable,
        })
    );
    Ok(())
}

async fn persist_replaying_checkpoint(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    clickhouse
        .insert_json_rows_sync(
            "ingest_checkpoint_transitions",
            &[json!({
                "host": HOST_A,
                "source_name": SOURCE,
                "source_file": SOURCE_FILE,
                "inode": 72,
                "source_generation": 2,
                "last_offset": 0,
                "last_line": 0,
                "cursor_json": "",
                "source_fingerprint": 501,
                "schema_fingerprint": 601,
                "sqlite_mtime_ns": 0,
                "sqlite_size": 0,
                "checkpoint_revision": 9,
                "operation_id": "begin-primary-g2",
                "lifecycle": "replaying",
                "protocol_version": 1,
                "scan_inode": 72,
                "scan_boundary": 8,
                "policy_fingerprint": "fixture-policy",
                "final_scan_complete": 0,
                "block_reason": "",
                "compatibility_prepared": 0,
                "backend_caught_up": 0,
                "append_batch_id": "",
                "cache_epoch": 0,
                "updated_at": "2026-07-20 12:00:10.000",
            })],
        )
        .await
        .context("failed to persist generation-two replaying checkpoint")?;
    let durable = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`ingest_checkpoint_transitions` FINAL \
             WHERE host = '{}' AND operation_id = 'begin-primary-g2' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
        ),
    )
    .await?;
    if durable != 1 {
        bail!("replaying checkpoint insert was not durably observable");
    }
    Ok(())
}

async fn persist_final_checkpoint(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    clickhouse
        .insert_json_rows_sync(
            "ingest_checkpoint_transitions",
            &[json!({
                "host": HOST_A,
                "source_name": SOURCE,
                "source_file": SOURCE_FILE,
                "inode": 72,
                "source_generation": 2,
                "last_offset": 8,
                "last_line": 8,
                "cursor_json": "final-cursor-g2",
                "source_fingerprint": 501,
                "schema_fingerprint": 601,
                "sqlite_mtime_ns": 0,
                "sqlite_size": 0,
                "checkpoint_revision": 10,
                "operation_id": "publish-primary-g2",
                "lifecycle": "active",
                "protocol_version": 1,
                "scan_inode": 72,
                "scan_boundary": 8,
                "policy_fingerprint": "fixture-policy",
                "final_scan_complete": 1,
                "block_reason": "",
                "compatibility_prepared": 1,
                "backend_caught_up": 1,
                "append_batch_id": "",
                "cache_epoch": 0,
                "updated_at": "2026-07-20 12:00:30.000",
            })],
        )
        .await
        .context("failed to persist final generation-two checkpoint")?;
    let durable = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`ingest_checkpoint_transitions` FINAL \
             WHERE host = '{}' AND operation_id = 'publish-primary-g2' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
        ),
    )
    .await?;
    if durable != 1 {
        bail!("final checkpoint insert was not durably observable");
    }
    Ok(())
}

async fn persist_final_readiness(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    clickhouse
        .insert_json_rows_sync(
            "source_generation_publication_readiness",
            &[json!({
                "source_host": HOST_A,
                "source_name": SOURCE,
                "source_file": SOURCE_FILE,
                "source_generation": 2,
                "readiness_revision": 10,
                "checkpoint_revision": 10,
                "operation_id": "publish-primary-g2",
                "complete": 1,
                "block_reason": "",
                "compatibility_prepared": 1,
                "backend_caught_up": 1,
                "manifest_digest": "fixture-manifest-g2",
                "updated_at": "2026-07-20 12:00:31.000",
            })],
        )
        .await
        .context("failed to persist final generation-two readiness")?;
    let durable = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`source_generation_publication_readiness` FINAL \
             WHERE source_host = '{}' AND operation_id = 'publish-primary-g2' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
        ),
    )
    .await?;
    if durable != 1 {
        bail!("final readiness insert was not durably observable");
    }
    Ok(())
}

async fn assert_equal_timestamp_checkpoint_is_causal(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let shared = json!({
        "host": HOST_A,
        "source_name": CAUSAL_CHECKPOINT_SOURCE,
        "source_file": CAUSAL_CHECKPOINT_FILE,
        "source_generation": 2,
        "source_fingerprint": 501,
        "schema_fingerprint": 601,
        "sqlite_mtime_ns": 0,
        "sqlite_size": 0,
        "protocol_version": 1,
        "scan_inode": 72,
        "scan_boundary": 900,
        "policy_fingerprint": "fixture-policy",
        "block_reason": "",
        "append_batch_id": "",
        "cache_epoch": 0,
        "updated_at": "2026-07-20 12:01:00.000",
    });
    let mut replaying = shared.clone();
    let replaying = replaying
        .as_object_mut()
        .expect("checkpoint fixture is an object");
    replaying.extend(
        json!({
            "inode": 72,
            "last_offset": 400,
            "last_line": 4,
            "cursor_json": "replaying-cursor",
            "checkpoint_revision": 11,
            "operation_id": "checkpoint-replaying",
            "lifecycle": "replaying",
            "final_scan_complete": 0,
            "compatibility_prepared": 0,
            "backend_caught_up": 0,
        })
        .as_object()
        .expect("checkpoint fixture fields are an object")
        .clone(),
    );
    let replaying = Value::Object(replaying.clone());

    let mut active = shared;
    let active = active
        .as_object_mut()
        .expect("checkpoint fixture is an object");
    active.extend(
        json!({
            "inode": 72,
            "last_offset": 900,
            "last_line": 9,
            "cursor_json": "active-cursor",
            "checkpoint_revision": 12,
            "operation_id": "checkpoint-active",
            "lifecycle": "active",
            "final_scan_complete": 1,
            "compatibility_prepared": 1,
            "backend_caught_up": 1,
        })
        .as_object()
        .expect("checkpoint fixture fields are an object")
        .clone(),
    );
    let active = Value::Object(active.clone());

    clickhouse
        .insert_json_rows_sync("ingest_checkpoint_transitions", &[replaying, active])
        .await
        .context("failed to seed equal-timestamp checkpoint transitions")?;
    let query = format!(
        "SELECT inode, toUInt32(source_generation) AS source_generation, \
                toUInt64(last_offset) AS last_offset, toUInt64(last_line) AS last_line, \
                cursor_json, toUInt64(checkpoint_revision) AS checkpoint_revision, \
                operation_id, lifecycle, toUInt8(final_scan_complete) AS final_scan_complete, \
                toUInt8(compatibility_prepared) AS compatibility_prepared, \
                toUInt8(backend_caught_up) AS backend_caught_up \
         FROM `{}`.`v_current_ingest_checkpoint_transitions` \
         WHERE host = '{}' AND source_name = '{}' AND source_file = '{}' \
         FORMAT JSONEachRow",
        database.as_str(),
        HOST_A,
        CAUSAL_CHECKPOINT_SOURCE,
        CAUSAL_CHECKPOINT_FILE,
    );
    let row = clickhouse
        .query_rows::<CheckpointRow>(&query, Some(database.as_str()))
        .await
        .context("failed to read causal checkpoint head")?
        .into_iter()
        .next()
        .context("causal checkpoint head is missing")?;
    if row.inode != 72
        || row.source_generation != 2
        || row.last_offset != 900
        || row.last_line != 9
        || row.cursor_json != "active-cursor"
        || row.checkpoint_revision != 12
        || row.operation_id != "checkpoint-active"
        || row.lifecycle != "active"
        || row.final_scan_complete != 1
        || row.compatibility_prepared != 1
        || row.backend_caught_up != 1
    {
        bail!("equal-timestamp checkpoint fields were causally mixed: {row:?}");
    }
    Ok(())
}

fn initial_heads() -> Vec<McpOpenSourceHead> {
    vec![
        McpOpenSourceHead {
            source_host: HOST_A.to_string(),
            source_name: SOURCE.to_string(),
            source_file: SOURCE_FILE.to_string(),
            source_generation: 1,
            publication_revision: 1,
        },
        McpOpenSourceHead {
            source_host: HOST_A.to_string(),
            source_name: SIDE_SOURCE.to_string(),
            source_file: SIDE_FILE.to_string(),
            source_generation: 1,
            publication_revision: 3,
        },
        McpOpenSourceHead {
            source_host: HOST_B.to_string(),
            source_name: SOURCE.to_string(),
            source_file: SOURCE_FILE.to_string(),
            source_generation: 1,
            publication_revision: 2,
        },
    ]
}

fn replacement_heads() -> Vec<McpOpenSourceHead> {
    initial_heads()
        .into_iter()
        .map(|head| {
            if head.source_host == HOST_A
                && head.source_name == SOURCE
                && head.source_file == SOURCE_FILE
            {
                McpOpenSourceHead {
                    source_generation: 2,
                    publication_revision: 4,
                    ..head
                }
            } else {
                head
            }
        })
        .collect()
}

pub(super) async fn run(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    migration_ms: u64,
) -> Result<()> {
    clickhouse
        .insert_json_rows_sync(
            "published_source_generations",
            &[
                source_head(HOST_A, SOURCE, SOURCE_FILE, 1, 1, "publish-primary-g1"),
                source_head(HOST_B, SOURCE, SOURCE_FILE, 1, 2, "publish-host-b-g1"),
                source_head(HOST_A, SIDE_SOURCE, SIDE_FILE, 1, 3, "publish-side-g1"),
            ],
        )
        .await
        .context("failed to seed initial published source heads")?;

    let generation_one = vec![
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            1,
            1,
            STABLE_UIDS.at(1),
            "stable-session",
            "stable commonterm identicalterm",
            1,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            1,
            2,
            CHANGED_UIDS.at(1),
            "changed-session",
            "beforeterm commonterm",
            1,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            1,
            3,
            DELETED_UIDS.at(1),
            "deleted-session",
            "deletedterm commonterm",
            1,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            1,
            4,
            EMPTY_UIDS.at(1),
            "empty-session",
            "oldemptyterm commonterm",
            1,
        ),
        event(
            HOST_A,
            SIDE_SOURCE,
            SIDE_FILE,
            1,
            5,
            CROSS_SOURCE_SIDE_G1,
            "changed-session",
            "sidecarterm",
            1,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            1,
            6,
            CROSS_HOST_UID_G1,
            "cross-host-session",
            "hostalphaterm",
            1,
        ),
        event(
            HOST_B,
            SOURCE,
            SOURCE_FILE,
            1,
            6,
            CROSS_HOST_UID_G1,
            "cross-host-session",
            "hostbetaterm",
            1,
        ),
    ];
    clickhouse
        .insert_json_rows_sync("events", &generation_one)
        .await
        .context("failed to seed generation-one events")?;

    let repository =
        ClickHouseConversationRepository::new(clickhouse.clone(), RepoConfig::default());
    clickhouse
        .backfill_mcp_open_read_model()
        .await
        .context("failed to build initial MCP compatibility projection")?;
    if repository
        .get_mcp_event(DELETED_UIDS.at(1))
        .await?
        .is_none()
    {
        bail!("published generation-one event handle did not resolve");
    }
    let initial_session = session_event_texts(&repository, "changed-session").await?;
    if !initial_session
        .iter()
        .any(|(uid, text)| uid == CHANGED_UIDS.at(1) && text.contains("beforeterm"))
    {
        bail!("stable session handle did not resolve generation-one content");
    }
    assert_search_uids(&repository, "beforeterm", &[CHANGED_UIDS.at(1)]).await?;
    assert_live_search_precondition(
        clickhouse,
        database,
        "generation_one",
        STABLE_UIDS.at(1),
        "identicalterm",
    )
    .await?;
    assert_search_uids(&repository, "identicalterm", &[STABLE_UIDS.at(1)]).await?;
    assert_search_uids(&repository, "replacementterm", &[]).await?;

    // Both hosts still publish generation one here. Exercise the physical
    // replacement key under FINAL before host A advances to generation two;
    // after that cutover, host A's generation-one row is correctly non-live.
    clickhouse
        .request_text(
            &format!("OPTIMIZE TABLE `{}`.`events` FINAL", database.as_str()),
            None,
            Some(database.as_str()),
            false,
            None,
        )
        .await
        .context("failed to merge cross-host UID fixture")?;
    let cross_host_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`v_live_events` \
             WHERE event_uid = '{}' FORMAT JSONEachRow",
            database.as_str(),
            CROSS_HOST_UID_G1,
        ),
    )
    .await?;
    if cross_host_count != 2 {
        bail!("cross-host identical UIDs collapsed after FINAL: {cross_host_count}");
    }

    // The generation-one repository has served its purpose. All subsequent
    // cutover checks use newly constructed clients and repositories so no
    // in-memory cache can bridge a simulated restart boundary.
    drop(repository);
    let replay_writer = reconstruct_clickhouse_client(clickhouse)?;
    persist_replaying_checkpoint(&replay_writer, database).await?;

    let generation_two = vec![
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            2,
            1,
            STABLE_UIDS.at(2),
            "stable-session",
            "stable commonterm identicalterm",
            1,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            2,
            2,
            CHANGED_UIDS.at(2),
            "changed-session",
            "replacementterm commonterm",
            2,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            2,
            4,
            EMPTY_UIDS.at(2),
            "empty-session",
            "oldemptyterm commonterm",
            1,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            2,
            4,
            EMPTY_UIDS.at(2),
            "empty-session",
            "",
            2,
        ),
        event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            2,
            8,
            NEW_UIDS.at(2),
            "new-session",
            "newgenerationterm",
            1,
        ),
    ];
    replay_writer
        .insert_json_rows_sync("events", &generation_two)
        .await
        .context("failed to stage replacement generation")?;
    replay_writer
        .insert_json_rows_sync(
            "events",
            &[event(
                HOST_A,
                SOURCE,
                SOURCE_FILE,
                3,
                9,
                INACTIVE_ADVERSARY_UID_G3,
                "inactive-session",
                &format!("inactiveadversaryterm {}", "commonterm ".repeat(200)),
                1,
            )],
        )
        .await
        .context("failed to seed inactive high-TF adversary")?;
    assert_derived_visibility_uses_published_uid(&replay_writer, database).await?;
    drop(replay_writer);

    assert_live_term_count(clickhouse, database, "replacementterm", 0).await?;
    assert_live_term_count(clickhouse, database, "beforeterm", 1).await?;
    assert_live_term_count(clickhouse, database, "inactiveadversaryterm", 0).await?;
    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "physical_rows_durable",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationOne,
            legacy_model: ExpectedPublicationModel::GenerationOne,
            prepared_header_count: None,
            final_checkpoint_durable: false,
            final_readiness_durable: false,
        },
    )
    .await?;

    let compatibility_writer = reconstruct_clickhouse_client(clickhouse)?;
    let readiness = compatibility_writer
        .prepare_mcp_open_publication(&McpOpenPublicationRequest {
            candidate_publication_id: "source-publication-g2".to_string(),
            operation_id: "prepare-primary-g2".to_string(),
            publisher_id: "source-publication-live-test".to_string(),
            source_host: HOST_A.to_string(),
            source_name: SOURCE.to_string(),
            source_file: SOURCE_FILE.to_string(),
            previous_source_generation: Some(1),
            source_generation: 2,
            required_source_heads: replacement_heads(),
        })
        .await
        .context("failed to prepare replacement MCP compatibility candidates")?;
    drop(compatibility_writer);
    if readiness.affected_session_count != 6
        || readiness.prepared_session_count != 6
        || readiness.tombstone_count != 1
    {
        bail!("incomplete replacement compatibility readiness: {readiness:?}");
    }

    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "compatibility_preparation_durable",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationOne,
            legacy_model: ExpectedPublicationModel::GenerationOne,
            prepared_header_count: Some(6),
            final_checkpoint_durable: false,
            final_readiness_durable: false,
        },
    )
    .await?;

    let checkpoint_writer = reconstruct_clickhouse_client(clickhouse)?;
    persist_final_checkpoint(&checkpoint_writer, database).await?;
    drop(checkpoint_writer);
    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "final_checkpoint_durable",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationOne,
            legacy_model: ExpectedPublicationModel::GenerationOne,
            prepared_header_count: Some(6),
            final_checkpoint_durable: true,
            final_readiness_durable: false,
        },
    )
    .await?;

    let readiness_writer = reconstruct_clickhouse_client(clickhouse)?;
    persist_final_readiness(&readiness_writer, database).await?;
    drop(readiness_writer);
    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "final_readiness_durable",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationOne,
            legacy_model: ExpectedPublicationModel::GenerationOne,
            prepared_header_count: Some(6),
            final_checkpoint_durable: true,
            final_readiness_durable: true,
        },
    )
    .await?;

    let prehead_activation_client = reconstruct_clickhouse_client(clickhouse)?;
    if prehead_activation_client
        .activate_mcp_open_publication("source-publication-g2")
        .await
        .context("failed to test compatibility activation before the source head")?
    {
        bail!("compatibility candidate activated before its required source head was current");
    }
    drop(prehead_activation_client);

    let as_of_before = current_head(clickhouse, database).await?;
    let history_before = logical_head_history_count(clickhouse, database).await?;
    let replacement_head = source_head(HOST_A, SOURCE, SOURCE_FILE, 2, 4, "publish-primary-g2");
    let head_writer = reconstruct_clickhouse_client(clickhouse)?;
    head_writer
        .insert_json_rows_sync(
            "published_source_generations",
            std::slice::from_ref(&replacement_head),
        )
        .await
        .context("failed to publish replacement source head")?;
    drop(head_writer);
    let current = current_head(clickhouse, database).await?;
    if current.source_generation != 2 || current.publication_revision != 4 {
        bail!("replacement did not become the one logical current head: {current:?}");
    }
    let logical_current_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_current_published_source_generations` \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    if logical_current_count != 1 || logical_head_history_count(clickhouse, database).await? != 2 {
        bail!("replacement created an unexpected logical publication head/history count");
    }
    let as_of_generation = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(tupleElement(head, 1)) AS value FROM ( \
               SELECT argMax(tuple(source_generation, publication_revision), publication_revision) AS head \
               FROM `{}`.`v_published_source_generation_history` \
               WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
                 AND publication_revision <= 3 \
             ) FORMAT JSONEachRow",
            database.as_str(), HOST_A, SOURCE, SOURCE_FILE,
        ),
    )
    .await?;
    if as_of_generation != 1 {
        bail!("captured revision 3 did not reconstruct generation-one head");
    }

    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "source_head_durable_before_compatibility_activation",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationTwo,
            legacy_model: ExpectedPublicationModel::GenerationOne,
            prepared_header_count: Some(6),
            final_checkpoint_durable: true,
            final_readiness_durable: true,
        },
    )
    .await?;

    // Model an acknowledged insert whose response was lost. The retry is
    // issued through another client after the post-head reconstruction above.
    let response_loss_retry = reconstruct_clickhouse_client(clickhouse)?;
    response_loss_retry
        .insert_json_rows_sync("published_source_generations", &[replacement_head])
        .await
        .context("failed to retry the committed replacement source head")?;
    drop(response_loss_retry);
    let retried_head = current_head(clickhouse, database).await?;
    let retried_current_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value \
             FROM `{}`.`v_current_published_source_generations` \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' \
             FORMAT JSONEachRow",
            database.as_str(),
            HOST_A,
            SOURCE,
            SOURCE_FILE,
        ),
    )
    .await?;
    let retried_history_count = logical_head_history_count(clickhouse, database).await?;
    if retried_head.source_generation != 2
        || retried_head.publication_revision != 4
        || retried_current_count != 1
        || retried_history_count != 2
    {
        bail!(
            "response-loss head retry changed logical publication state: head={retried_head:?}, current={retried_current_count}, history={retried_history_count}"
        );
    }
    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "source_head_response_loss_retry_durable",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationTwo,
            legacy_model: ExpectedPublicationModel::GenerationOne,
            prepared_header_count: Some(6),
            final_checkpoint_durable: true,
            final_readiness_durable: true,
        },
    )
    .await?;

    // Compatibility activation itself runs through another fresh client,
    // modeling repair after a writer restart in the post-head window.
    let (activation_client, activation_repository) = reconstruct_reader_runtime(clickhouse)?;
    drop(activation_repository);
    if !activation_client
        .activate_mcp_open_publication("source-publication-g2")
        .await
        .context("failed to activate replacement MCP compatibility candidate")?
    {
        bail!("prepared MCP compatibility candidate was not activated");
    }
    drop(activation_client);

    assert_complete_after_reconstruction(
        clickhouse,
        database,
        "compatibility_activation_durable",
        DurableBoundaryState {
            model: ExpectedPublicationModel::GenerationTwo,
            legacy_model: ExpectedPublicationModel::GenerationTwo,
            prepared_header_count: Some(6),
            final_checkpoint_durable: true,
            final_readiness_durable: true,
        },
    )
    .await?;

    let (_post_activation_client, repository) = reconstruct_reader_runtime(clickhouse)?;

    let stable_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`v_live_events` \
             WHERE event_uid = '{}' FORMAT JSONEachRow",
            database.as_str(),
            STABLE_UIDS.at(2),
        ),
    )
    .await?;
    if stable_count != 1 {
        bail!("identical replay changed live stable g2 cardinality: {stable_count}");
    }
    let stale_stable_count = scalar_u64(
        clickhouse,
        database,
        &format!(
            "SELECT toUInt64(count()) AS value FROM `{}`.`v_live_events` \
             WHERE event_uid = '{}' FORMAT JSONEachRow",
            database.as_str(),
            STABLE_UIDS.at(1),
        ),
    )
    .await?;
    if stale_stable_count != 0 {
        bail!("old-generation stable event handle remained live: {stale_stable_count}");
    }
    if repository.get_mcp_event(STABLE_UIDS.at(1)).await?.is_some() {
        bail!("unchanged replay left the generation-one physical event handle resolvable");
    }
    let stable_event = repository
        .get_mcp_event(STABLE_UIDS.at(2))
        .await?
        .context("unchanged replay generation-two event handle is missing")?;
    if stable_event.event.text_content != "stable commonterm identicalterm" {
        bail!("unchanged replay changed stable event content");
    }
    if repository
        .get_mcp_session("stable-session")
        .await?
        .is_none()
    {
        bail!("stable session handle stopped resolving after unchanged replay");
    }
    let stable_session = session_event_texts(&repository, "stable-session").await?;
    if stable_session
        != vec![(
            STABLE_UIDS.at(2).to_string(),
            "stable commonterm identicalterm".to_string(),
        )]
    {
        bail!("unchanged replay did not preserve the stable session's logical shape: {stable_session:?}");
    }
    for hidden in [
        "beforeterm",
        "deletedterm",
        "oldemptyterm",
        "inactiveadversaryterm",
    ] {
        assert_live_term_count(clickhouse, database, hidden, 0).await?;
        assert_search_uids(&repository, hidden, &[]).await?;
    }
    assert_live_term_count(clickhouse, database, "replacementterm", 1).await?;
    assert_search_uids(&repository, "replacementterm", &[CHANGED_UIDS.at(2)]).await?;
    assert_live_search_precondition(
        clickhouse,
        database,
        "generation_two",
        STABLE_UIDS.at(2),
        "identicalterm",
    )
    .await?;
    assert_search_uids(&repository, "identicalterm", &[STABLE_UIDS.at(2)]).await?;
    if repository
        .get_mcp_event(DELETED_UIDS.at(1))
        .await?
        .is_some()
    {
        bail!("old-generation event handle survived replacement tombstone");
    }
    if repository
        .get_mcp_event(CHANGED_UIDS.at(1))
        .await?
        .is_some()
    {
        bail!("replaced generation-one event handle resolved generation-two content");
    }
    let changed = repository
        .get_mcp_event(CHANGED_UIDS.at(2))
        .await?
        .context("current generation-two changed-event handle is missing")?;
    if !changed.event.text_content.contains("replacementterm") {
        bail!("current event handle returned pre-replacement content");
    }
    if repository
        .get_mcp_session("changed-session")
        .await?
        .is_none()
    {
        bail!("stable session handle stopped resolving after replacement");
    }
    let changed_session = session_event_texts(&repository, "changed-session").await?;
    if !changed_session
        .iter()
        .any(|(uid, text)| uid == CHANGED_UIDS.at(2) && text.contains("replacementterm"))
        || !changed_session
            .iter()
            .any(|(uid, _)| uid == CROSS_SOURCE_SIDE_G1)
    {
        bail!("stable cross-source session handle did not resolve the complete new head set");
    }

    assert_equal_timestamp_checkpoint_is_causal(clickhouse, database).await?;

    let head_before_append = current_head(clickhouse, database).await?;
    let history_before_append = logical_head_history_count(clickhouse, database).await?;
    clickhouse
        .insert_json_rows_sync(
            "ingest_append_control",
            &[json!({
                "host": HOST_A,
                "control_revision": 1,
                "cache_epoch": 0,
                "state": "preparing",
                "batch_id": "append-live-fixture",
                "publisher_id": "source-publication-live-test",
                "manifest_json": format!("{{\"event_uids\":[\"{}\"]}}", APPEND_UID_G2),
                "insert_only": 0,
            })],
        )
        .await
        .context("failed to persist append preparation fence")?;
    let fenced = repository
        .list_session_events(
            SessionEventsQuery {
                session_id: "changed-session".to_string(),
                direction: SessionEventsDirection::Forward,
                event_kinds: None,
            },
            PageRequest::default(),
        )
        .await;
    if !matches!(fenced, Err(RepoError::ReadModelChanged)) {
        bail!("strict read escaped append preparation fence: {fenced:?}");
    }
    clickhouse
        .insert_json_rows_sync(
            "events",
            &[event(
                HOST_A,
                SOURCE,
                SOURCE_FILE,
                2,
                10,
                APPEND_UID_G2,
                "changed-session",
                "appendvisibleterm",
                1,
            )],
        )
        .await
        .context("failed to insert ordinary same-generation append")?;
    let committed_append = json!({
        "host": HOST_A,
        "control_revision": 2,
        "cache_epoch": 1,
        "state": "idle",
        "batch_id": "append-live-fixture",
        "publisher_id": "source-publication-live-test",
        "manifest_json": "",
        "insert_only": 0,
    });
    clickhouse
        .insert_json_rows_sync(
            "ingest_append_control",
            &[committed_append.clone(), committed_append],
        )
        .await
        .context("failed to commit response-loss append epoch fixture")?;
    let append_control_query = format!(
        "SELECT toUInt64(control_revision) AS control_revision, \
                toUInt64(cache_epoch) AS cache_epoch, state, batch_id \
         FROM `{}`.`v_current_ingest_append_control` WHERE host = '{}' \
         FORMAT JSONEachRow",
        database.as_str(),
        HOST_A,
    );
    let append_control = clickhouse
        .query_rows::<AppendControlRow>(&append_control_query, Some(database.as_str()))
        .await
        .context("failed to reload committed append control after response-loss retry")?
        .into_iter()
        .next()
        .context("committed append control is missing")?;
    if append_control.control_revision != 2
        || append_control.cache_epoch != 1
        || append_control.state != "idle"
        || append_control.batch_id != "append-live-fixture"
    {
        bail!("response-loss retry changed the logical append commit: {append_control:?}");
    }
    let appended = session_event_texts(&repository, "changed-session").await?;
    if !appended.iter().any(|(uid, _)| uid == APPEND_UID_G2) {
        bail!("ordinary append did not become visible after cache-epoch commit");
    }
    let head_after_append = current_head(clickhouse, database).await?;
    let history_after_append = logical_head_history_count(clickhouse, database).await?;
    if head_after_append.source_generation != head_before_append.source_generation
        || head_after_append.publication_revision != head_before_append.publication_revision
        || history_after_append != history_before_append
    {
        bail!(
            "ordinary append wrote a source head: before={head_before_append:?}/{history_before_append}, after={head_after_append:?}/{history_after_append}"
        );
    }

    let control_storage: Vec<ControlStorageRow> = clickhouse
        .query_rows(
            &format!(
                "SELECT table, toUInt64(sum(rows)) AS rows, \
                        toUInt64(countIf(active)) AS active_parts, \
                        toUInt64(sum(data_compressed_bytes)) AS compressed_bytes \
                 FROM system.parts WHERE database = '{}' AND table IN ( \
                   'published_source_generations', 'ingest_checkpoint_transitions', \
                   'source_generation_publication_readiness', 'ingest_append_control', \
                   'mcp_open_publication_headers', \
                   'mcp_open_generation_readiness' \
                 ) GROUP BY table ORDER BY table FORMAT JSONEachRow",
                database.as_str(),
            ),
            Some("system"),
        )
        .await
        .context("failed to collect source-publication control storage evidence")?;
    let control_tables = control_storage
        .iter()
        .map(|row| row.table.as_str())
        .collect::<Vec<_>>();
    if control_tables
        != [
            "ingest_append_control",
            "ingest_checkpoint_transitions",
            "mcp_open_generation_readiness",
            "mcp_open_publication_headers",
            "published_source_generations",
            "source_generation_publication_readiness",
        ]
    {
        bail!("source-publication control-storage evidence is incomplete: {control_tables:?}");
    }
    eprintln!(
        "{}",
        json!({
            "kind": "source_publication_acceptance_evidence",
            "migration_ms": migration_ms,
            "captured_head": {
                "generation": as_of_before.source_generation,
                "revision": as_of_before.publication_revision,
                "logical_history_rows": history_before,
            },
            "published_head": {
                "generation": current.source_generation,
                "revision": current.publication_revision,
                "logical_current_rows": logical_current_count,
            },
            "ordinary_append": {
                "publication_revision_before": head_before_append.publication_revision,
                "publication_revision_after": head_after_append.publication_revision,
                "logical_head_writes": history_after_append - history_before_append,
                "cache_epoch": append_control.cache_epoch,
                "control_revision": append_control.control_revision,
            },
            "cross_host_live_uid_rows": cross_host_count,
            "control_storage": control_storage.into_iter().map(|row| json!({
                "table": row.table,
                "rows": row.rows,
                "active_parts": row.active_parts,
                "compressed_bytes": row.compressed_bytes,
            })).collect::<Vec<_>>(),
        })
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_fixture_uses_generation_qualified_physical_event_uids() {
        for uids in [
            STABLE_UIDS,
            CHANGED_UIDS,
            DELETED_UIDS,
            EMPTY_UIDS,
            NEW_UIDS,
        ] {
            let generation_one = uids.at(1);
            let generation_two = uids.at(2);
            assert!(generation_one.ends_with("-g1"));
            assert!(generation_two.ends_with("-g2"));
            assert_ne!(generation_one, generation_two);
        }
        for generation_one_only in [CROSS_SOURCE_SIDE_G1, CROSS_HOST_UID_G1] {
            assert!(generation_one_only.ends_with("-g1"));
        }
        assert!(APPEND_UID_G2.ends_with("-g2"));
        assert!(INACTIVE_ADVERSARY_UID_G3.ends_with("-g3"));

        let host_a = event(
            HOST_A,
            SOURCE,
            SOURCE_FILE,
            1,
            6,
            CROSS_HOST_UID_G1,
            "cross-host-session",
            "host a",
            1,
        );
        let host_b = event(
            HOST_B,
            SOURCE,
            SOURCE_FILE,
            1,
            6,
            CROSS_HOST_UID_G1,
            "cross-host-session",
            "host b",
            1,
        );
        assert_eq!(host_a["event_uid"], host_b["event_uid"]);
        assert_ne!(host_a["source_host"], host_b["source_host"]);
    }
}
