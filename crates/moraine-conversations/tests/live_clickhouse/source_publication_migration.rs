use super::OwnedDatabaseName;
use anyhow::{bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Instant;

const DEFAULT_SOURCE: &str = "legacy-default-local";
const DEFAULT_FILE: &str = "/legacy/default-local.jsonl";
const DEFAULT_SESSION: &str = "legacy-default-local-session";
const DEFAULT_EVENT: &str = "legacy-default-local-event";
const STRANDED_EVENT: &str = "legacy-default-local-stranded-event";
const STRANDED_SOURCE_REF: &str = "legacy-default:2:stranded";
const STRANDED_POSTINGS: u64 = 3;
const SHARED_HOST: &str = "legacy-shared-host-a";
const SHARED_SOURCE: &str = "legacy-shared";
const SHARED_FILE: &str = "/legacy/shared.jsonl";
const SHARED_SESSION: &str = "legacy-shared-session";
const SHARED_EVENT: &str = "legacy-shared-event";
const AMBIGUOUS_HOST: &str = "legacy-shared-host-b";
const AMBIGUOUS_SOURCE: &str = "legacy-equal-timestamp";
const AMBIGUOUS_FILE: &str = "/legacy/equal-timestamp.jsonl";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct HeadRow {
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    publication_revision: u64,
    publisher_id: String,
    operation_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct TransitionRow {
    host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    last_offset: u64,
    last_line: u64,
    cursor_json: String,
    checkpoint_revision: u64,
    operation_id: String,
    lifecycle: String,
    final_scan_complete: u8,
    block_reason: String,
    compatibility_prepared: u8,
    backend_caught_up: u8,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ReadinessRow {
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    readiness_revision: u64,
    checkpoint_revision: u64,
    operation_id: String,
    complete: u8,
    block_reason: String,
    compatibility_prepared: u8,
    backend_caught_up: u8,
    manifest_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BackfillSnapshot {
    heads: Vec<HeadRow>,
    transitions: Vec<TransitionRow>,
    readiness: Vec<ReadinessRow>,
    raw_head_rows: u64,
    raw_transition_rows: u64,
    raw_readiness_rows: u64,
    append_control_rows: u64,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    value: u64,
}

#[derive(Debug, Deserialize)]
struct ProjectionStateRow {
    ready: u8,
    generation: u64,
    backfill_cursor: String,
}

#[derive(Debug, Deserialize)]
struct DiagnosticsRow {
    ambiguous_hostless_rows: u64,
    issues: Vec<String>,
}

async fn scalar(clickhouse: &ClickHouseClient, query: &str) -> Result<u64> {
    let rows: Vec<CountRow> = clickhouse
        .query_rows(query, None)
        .await
        .with_context(|| format!("failed legacy-migration scalar query: {query}"))?;
    rows.first()
        .map(|row| row.value)
        .context("legacy-migration scalar query returned no row")
}

async fn bootstrap_schema_through_030(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    let database = database.as_str();
    clickhouse
        .request_text(
            &format!("CREATE DATABASE IF NOT EXISTS `{database}`"),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .context("failed to create legacy-migration fixture database")?;
    clickhouse
        .request_text(
            &format!(
                "CREATE TABLE IF NOT EXISTS `{database}`.schema_migrations (\
                 version String, name String, applied_at DateTime64(3) DEFAULT now64(3)\
                 ) ENGINE = ReplacingMergeTree(applied_at) ORDER BY (version)"
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .context("failed to create legacy-migration fixture ledger")?;
    clickhouse
        .request_text(
            &format!(
                "INSERT INTO `{database}`.schema_migrations (version, name) VALUES \
                 ('031', 'fixture-hold-031'), \
                 ('032', 'fixture-hold-032'), \
                 ('033', 'fixture-hold-033'), \
                 ('034', 'fixture-hold-034')"
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .context("failed to hold publication migrations during legacy fixture setup")?;

    let applied = clickhouse
        .run_migrations()
        .await
        .context("failed to build schema through migration 030")?;
    let expected = (1..=30)
        .map(|version| format!("{version:03}"))
        .collect::<Vec<_>>();
    if applied != expected {
        bail!("legacy fixture expected migrations 001-030 exactly, got {applied:?}");
    }
    Ok(())
}

async fn remove_migration_ledger_rows(
    clickhouse: &ClickHouseClient,
    database: &str,
    versions: &[&str],
) -> Result<()> {
    let versions = versions
        .iter()
        .map(|version| format!("'{version}'"))
        .collect::<Vec<_>>()
        .join(", ");
    clickhouse
        .request_text(
            &format!(
                "ALTER TABLE `{database}`.schema_migrations DELETE \
                 WHERE version IN ({versions}) SETTINGS mutations_sync = 2"
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .with_context(|| format!("failed to expose migrations {versions} in fixture ledger"))?;
    Ok(())
}

async fn set_checkpoint_merges(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
    enabled: bool,
) -> Result<()> {
    let action = if enabled { "START" } else { "STOP" };
    clickhouse
        .request_text(
            &format!(
                "SYSTEM {action} MERGES `{}`.ingest_checkpoints",
                database.as_str()
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .with_context(|| format!("failed to {action} legacy checkpoint merges"))?;
    Ok(())
}

fn checkpoint_rows() -> Vec<Value> {
    vec![
        json!({
            "updated_at": "2026-01-01 00:00:00.100",
            "source_name": DEFAULT_SOURCE,
            "source_file": DEFAULT_FILE,
            "source_inode": 101,
            "source_generation": 1,
            "last_offset": 100,
            "last_line_no": 1,
            "status": "active",
            "cursor_json": "{\"generation\":1}",
            "source_fingerprint": 1001,
            "schema_fingerprint": 2001,
            "host": "",
        }),
        json!({
            "updated_at": "2026-01-01 00:00:00.200",
            "source_name": DEFAULT_SOURCE,
            "source_file": DEFAULT_FILE,
            "source_inode": 102,
            "source_generation": 2,
            "last_offset": 240,
            "last_line_no": 2,
            "status": "active",
            "cursor_json": "{\"generation\":2}",
            "source_fingerprint": 1002,
            "schema_fingerprint": 2002,
            "host": "",
        }),
        json!({
            "updated_at": "2026-01-01 00:00:00.300",
            "source_name": SHARED_SOURCE,
            "source_file": SHARED_FILE,
            "source_inode": 201,
            "source_generation": 1,
            "last_offset": 320,
            "last_line_no": 3,
            "status": "active",
            "cursor_json": "{\"shared\":true}",
            "source_fingerprint": 3001,
            "schema_fingerprint": 4001,
            "host": SHARED_HOST,
        }),
        json!({
            "updated_at": "2026-01-01 00:00:00.400",
            "source_name": AMBIGUOUS_SOURCE,
            "source_file": AMBIGUOUS_FILE,
            "source_inode": 301,
            "source_generation": 1,
            "last_offset": 410,
            "last_line_no": 4,
            "status": "active",
            "cursor_json": "{\"branch\":\"a\"}",
            "source_fingerprint": 5001,
            "schema_fingerprint": 6001,
            "host": AMBIGUOUS_HOST,
        }),
        json!({
            "updated_at": "2026-01-01 00:00:00.400",
            "source_name": AMBIGUOUS_SOURCE,
            "source_file": AMBIGUOUS_FILE,
            "source_inode": 302,
            "source_generation": 1,
            "last_offset": 420,
            "last_line_no": 5,
            "status": "error",
            "cursor_json": "{\"branch\":\"b\"}",
            "source_fingerprint": 5002,
            "schema_fingerprint": 6002,
            "host": AMBIGUOUS_HOST,
        }),
    ]
}

fn event_rows() -> Vec<Value> {
    vec![
        json!({
            "ingested_at": "2026-01-01 00:00:01.000",
            "event_uid": DEFAULT_EVENT,
            "session_id": DEFAULT_SESSION,
            "session_date": "2026-01-01",
            "source_name": DEFAULT_SOURCE,
            "harness": "codex",
            "source_file": DEFAULT_FILE,
            "source_inode": 102,
            "source_generation": 2,
            "source_line_no": 2,
            "source_offset": 240,
            "source_ref": "legacy-default:2",
            "record_ts": "2026-01-01T00:00:01Z",
            "event_ts": "2026-01-01 00:00:01.000",
            "event_kind": "message",
            "actor_kind": "user",
            "payload_type": "message",
            "endpoint_kind": "generation",
            "content_types": ["text"],
            "text_content": "default local legacy event",
            "text_preview": "default local legacy event",
            "payload_json": "{}",
            "token_usage_json": "{}",
            "event_version": 10001,
        }),
        json!({
            "ingested_at": "2026-01-01 00:00:02.000",
            "event_uid": SHARED_EVENT,
            "session_id": SHARED_SESSION,
            "session_date": "2026-01-01",
            "source_name": SHARED_SOURCE,
            "harness": "codex",
            "source_file": SHARED_FILE,
            "source_inode": 201,
            "source_generation": 1,
            "source_line_no": 3,
            "source_offset": 320,
            "source_ref": "legacy-shared:1",
            "record_ts": "2026-01-01T00:00:02Z",
            "event_ts": "2026-01-01 00:00:02.000",
            "event_kind": "message",
            "actor_kind": "user",
            "payload_type": "message",
            "endpoint_kind": "generation",
            "content_types": ["text"],
            "text_content": "hostless event from a legacy shared backend",
            "text_preview": "hostless event from a legacy shared backend",
            "payload_json": "{}",
            "token_usage_json": "{}",
            "event_version": 10002,
        }),
    ]
}

fn stranded_event_row() -> Value {
    json!({
        "ingested_at": "2026-01-01 00:00:01.500",
        "event_uid": STRANDED_EVENT,
        "session_id": DEFAULT_SESSION,
        "session_date": "2026-01-01",
        "source_name": DEFAULT_SOURCE,
        "harness": "codex",
        "source_file": DEFAULT_FILE,
        "source_inode": 102,
        "source_generation": 2,
        "source_line_no": 3,
        "source_offset": 260,
        "source_ref": STRANDED_SOURCE_REF,
        "record_ts": "2026-01-01T00:00:01.500Z",
        "event_ts": "2026-01-01 00:00:01.500",
        "event_kind": "message",
        "actor_kind": "user",
        "payload_type": "message",
        "endpoint_kind": "generation",
        "content_types": ["text"],
        "text_content": "stranded retry repair",
        "text_preview": "stranded retry repair",
        "payload_json": "{}",
        "token_usage_json": "{}",
        "event_version": 10003,
    })
}

async fn seed_pre_031_fixture(
    clickhouse: &ClickHouseClient,
    database: &OwnedDatabaseName,
) -> Result<()> {
    set_checkpoint_merges(clickhouse, database, false).await?;
    let rows = checkpoint_rows();
    let (equal_timestamp_variant_b, preceding_rows) = rows
        .split_last()
        .context("legacy checkpoint fixture must contain its second tied variant")?;
    clickhouse
        .insert_json_rows_sync("ingest_checkpoints", preceding_rows)
        .await
        .context("failed to seed pre-031 legacy checkpoint first parts")?;
    clickhouse
        .insert_json_rows_sync(
            "ingest_checkpoints",
            std::slice::from_ref(equal_timestamp_variant_b),
        )
        .await
        .context("failed to seed distinct equal-timestamp checkpoint part")?;
    clickhouse
        .insert_json_rows_sync("events", &event_rows())
        .await
        .context("failed to seed pre-032 hostless canonical events")?;

    // Reproduce the durable prefix left by the old migration 032 ordering:
    // its document reconciliation succeeded while the postings MV was absent,
    // then the global posting rebuild timed out before producing rows.
    clickhouse
        .request_text(
            &format!(
                "DROP VIEW IF EXISTS `{}`.mv_search_postings",
                database.as_str()
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .context("failed to pause legacy postings MV for interrupted-032 fixture")?;
    clickhouse
        .insert_json_rows_sync("events", &[stranded_event_row()])
        .await
        .context("failed to seed document stranded by interrupted migration 032")?;

    let database = database.as_str();
    clickhouse
        .request_text(
            &format!(
                "INSERT INTO `{database}`.mcp_open_sessions \
                 (session_id, slot, generation, source_revision, dirty_revision, \
                  first_event_time, last_event_time, total_turns, total_events, \
                  user_messages, assistant_messages, tool_calls, tool_results, mode, \
                  first_event_uid, last_event_uid, last_actor_role, title, source, harness, \
                  inference_provider, session_slug, session_summary, completed, \
                  terminal_event_uid, origin_cwd) VALUES \
                 ('{DEFAULT_SESSION}', 0, 7, 7, 7, \
                  toDateTime64('2026-01-01 00:00:01.000', 3), \
                  toDateTime64('2026-01-01 00:00:01.000', 3), \
                  1, 1, 1, 0, 0, 0, 'normal', '{DEFAULT_EVENT}', '{DEFAULT_EVENT}', \
                  'user', 'legacy session-only head', '{DEFAULT_SOURCE}', 'codex', '', \
                  'legacy-session', '', 0, '', '')"
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .context("failed to seed legacy session-only MCP head")?;
    clickhouse
        .request_text(
            &format!(
                "INSERT INTO `{database}`.mcp_open_projection_state \
                 (state_key, ready, generation, backfill_cursor) \
                 SELECT 'global', 1, max(generation) + 1, 'legacy-ready-cursor' \
                 FROM `{database}`.mcp_open_projection_state"
            ),
            None,
            Some("system"),
            false,
            None,
        )
        .await
        .context("failed to seed legacy ready MCP projection control")?;

    let projection = current_projection_state(clickhouse).await?;
    if projection.ready != 1 || projection.backfill_cursor != "legacy-ready-cursor" {
        bail!("legacy projection seed was not current before migration: {projection:?}");
    }
    Ok(())
}

async fn current_projection_state(clickhouse: &ClickHouseClient) -> Result<ProjectionStateRow> {
    let mut rows: Vec<ProjectionStateRow> = clickhouse
        .query_rows(
            "SELECT toUInt8(ready) AS ready, toUInt64(generation) AS generation, \
             backfill_cursor FROM mcp_open_projection_state FINAL \
             WHERE state_key = 'global' FORMAT JSONEachRow",
            None,
        )
        .await
        .context("failed to load MCP projection state")?;
    match rows.len() {
        1 => Ok(rows.remove(0)),
        count => bail!("expected one current MCP projection state, found {count}"),
    }
}

async fn snapshot(clickhouse: &ClickHouseClient) -> Result<BackfillSnapshot> {
    let heads = clickhouse
        .query_rows(
            "SELECT source_host, toString(source_name) AS source_name, source_file, \
                    toUInt32(source_generation) AS source_generation, \
                    toUInt64(publication_revision) AS publication_revision, \
                    publisher_id, operation_id \
             FROM v_current_published_source_generations \
             ORDER BY source_host, source_name, source_file FORMAT JSONEachRow",
            None,
        )
        .await
        .context("failed to snapshot migrated legacy heads")?;
    let transitions = clickhouse
        .query_rows(
            "SELECT host, toString(source_name) AS source_name, source_file, \
                    toUInt32(source_generation) AS source_generation, \
                    toUInt64(last_offset) AS last_offset, toUInt64(last_line) AS last_line, \
                    cursor_json, toUInt64(checkpoint_revision) AS checkpoint_revision, \
                    operation_id, toString(lifecycle) AS lifecycle, \
                    toUInt8(final_scan_complete) AS final_scan_complete, block_reason, \
                    toUInt8(compatibility_prepared) AS compatibility_prepared, \
                    toUInt8(backend_caught_up) AS backend_caught_up \
             FROM ingest_checkpoint_transitions FINAL \
             ORDER BY host, source_name, source_file, source_generation FORMAT JSONEachRow",
            None,
        )
        .await
        .context("failed to snapshot migrated legacy checkpoint transitions")?;
    let readiness = clickhouse
        .query_rows(
            "SELECT source_host, toString(source_name) AS source_name, source_file, \
                    toUInt32(source_generation) AS source_generation, \
                    toUInt64(readiness_revision) AS readiness_revision, \
                    toUInt64(checkpoint_revision) AS checkpoint_revision, operation_id, \
                    toUInt8(complete) AS complete, block_reason, \
                    toUInt8(compatibility_prepared) AS compatibility_prepared, \
                    toUInt8(backend_caught_up) AS backend_caught_up, manifest_digest \
             FROM source_generation_publication_readiness FINAL \
             ORDER BY source_host, source_name, source_file, source_generation \
             FORMAT JSONEachRow",
            None,
        )
        .await
        .context("failed to snapshot migrated legacy readiness")?;
    Ok(BackfillSnapshot {
        heads,
        transitions,
        readiness,
        raw_head_rows: scalar(
            clickhouse,
            "SELECT toUInt64(count()) AS value FROM published_source_generations \
             FORMAT JSONEachRow",
        )
        .await?,
        raw_transition_rows: scalar(
            clickhouse,
            "SELECT toUInt64(count()) AS value FROM ingest_checkpoint_transitions \
             FORMAT JSONEachRow",
        )
        .await?,
        raw_readiness_rows: scalar(
            clickhouse,
            "SELECT toUInt64(count()) AS value \
             FROM source_generation_publication_readiness FORMAT JSONEachRow",
        )
        .await?,
        append_control_rows: scalar(
            clickhouse,
            "SELECT toUInt64(count()) AS value FROM ingest_append_control FORMAT JSONEachRow",
        )
        .await?,
    })
}

fn assert_control_backfill(state: &BackfillSnapshot) -> Result<()> {
    let default_head = state.heads.iter().find(|head| {
        head.source_host.is_empty()
            && head.source_name == DEFAULT_SOURCE
            && head.source_file == DEFAULT_FILE
    });
    if !matches!(default_head, Some(head) if head.source_generation == 2 && head.publisher_id == "migration-031")
    {
        bail!("default-local legacy source did not publish generation 2: {state:?}");
    }
    let shared_head = state.heads.iter().find(|head| {
        head.source_host == SHARED_HOST
            && head.source_name == SHARED_SOURCE
            && head.source_file == SHARED_FILE
    });
    if !matches!(shared_head, Some(head) if head.source_generation == 1 && head.publisher_id == "migration-031")
    {
        bail!("named shared legacy checkpoint did not receive its exact host head: {state:?}");
    }
    if state.heads.iter().any(|head| {
        head.source_host == AMBIGUOUS_HOST
            && head.source_name == AMBIGUOUS_SOURCE
            && head.source_file == AMBIGUOUS_FILE
    }) {
        bail!("equal-timestamp ambiguous generation was published: {state:?}");
    }
    if state.heads.len() != 2
        || state.transitions.len() != 4
        || state.readiness.len() != 2
        || state.raw_head_rows != 2
        || state.raw_transition_rows != 4
        || state.raw_readiness_rows != 2
        || state.append_control_rows != 1
    {
        bail!("unexpected legacy backfill cardinality: {state:?}");
    }

    let ambiguous = state.transitions.iter().find(|transition| {
        transition.host == AMBIGUOUS_HOST
            && transition.source_name == AMBIGUOUS_SOURCE
            && transition.source_file == AMBIGUOUS_FILE
    });
    if !matches!(
        ambiguous,
        Some(transition)
            if transition.lifecycle == "error"
                && transition.block_reason == "legacy_equal_timestamp_ambiguity"
                && transition.final_scan_complete == 0
                && transition.compatibility_prepared == 0
                && transition.backend_caught_up == 0
    ) {
        bail!("equal-timestamp variants were not deterministically blocked: {state:?}");
    }
    if state.readiness.iter().any(|row| {
        row.source_host == AMBIGUOUS_HOST
            && row.source_name == AMBIGUOUS_SOURCE
            && row.source_file == AMBIGUOUS_FILE
    }) {
        bail!("ambiguous legacy generation received readiness: {state:?}");
    }
    if state.readiness.iter().any(|row| {
        row.complete != 1
            || !row.block_reason.is_empty()
            || row.compatibility_prepared != 1
            || row.backend_caught_up != 1
            || row.manifest_digest != "legacy-migration-031"
    }) {
        bail!("unambiguous legacy readiness is incomplete: {state:?}");
    }
    for readiness in &state.readiness {
        let head = state.heads.iter().find(|head| {
            head.source_host == readiness.source_host
                && head.source_name == readiness.source_name
                && head.source_file == readiness.source_file
                && head.source_generation == readiness.source_generation
        });
        let transition = state.transitions.iter().find(|transition| {
            transition.host == readiness.source_host
                && transition.source_name == readiness.source_name
                && transition.source_file == readiness.source_file
                && transition.source_generation == readiness.source_generation
        });
        if !matches!(
            (head, transition),
            (Some(head), Some(transition))
                if readiness.readiness_revision == head.publication_revision
                    && readiness.checkpoint_revision == transition.checkpoint_revision
        ) {
            bail!("legacy readiness is not causally bound to its exact head/checkpoint: {state:?}");
        }
    }
    if state
        .heads
        .iter()
        .map(|head| head.publication_revision)
        .collect::<Vec<_>>()
        != [1, 2]
    {
        bail!("legacy head revision allocation is not deterministic: {state:?}");
    }
    Ok(())
}

async fn assert_post_migration_authorization(clickhouse: &ClickHouseClient) -> Result<()> {
    let default_live = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM v_live_events \
             WHERE event_uid = '{DEFAULT_EVENT}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    let shared_physical_hostless = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM events FINAL \
             WHERE event_uid = '{SHARED_EVENT}' AND source_host = '' FORMAT JSONEachRow"
        ),
    )
    .await?;
    let shared_live = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM v_live_events \
             WHERE event_uid = '{SHARED_EVENT}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    if default_live != 1 || shared_physical_hostless != 1 || shared_live != 0 {
        bail!(
            "host-aware migration authorization mismatch: default_live={default_live} \
             shared_physical_hostless={shared_physical_hostless} shared_live={shared_live}"
        );
    }

    let projection = current_projection_state(clickhouse).await?;
    if projection.ready != 0 || !projection.backfill_cursor.is_empty() {
        bail!("migration did not invalidate the legacy MCP projection: {projection:?}");
    }
    let legacy_headers = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM mcp_open_publication_headers \
             WHERE session_id = '{DEFAULT_SESSION}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    let dirty_default_session = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM mcp_open_dirty_sessions FINAL \
             WHERE session_id = '{DEFAULT_SESSION}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    if legacy_headers != 0 || dirty_default_session != 1 {
        bail!(
            "legacy session-only compatibility head did not fail closed: \
             headers={legacy_headers} dirty={dirty_default_session}"
        );
    }

    let diagnostics: Vec<DiagnosticsRow> = clickhouse
        .query_rows(
            "SELECT toUInt64(ambiguous_hostless_rows) AS ambiguous_hostless_rows, issues \
             FROM v_publication_diagnostics FORMAT JSONEachRow",
            None,
        )
        .await
        .context("failed to load publication diagnostics after legacy migration")?;
    if !matches!(
        diagnostics.as_slice(),
        [row]
            if row.ambiguous_hostless_rows == 1
                && row.issues.iter().any(|issue| issue == "legacy_host_ambiguity")
    ) {
        bail!("legacy ambiguity is absent from publication diagnostics: {diagnostics:?}");
    }
    Ok(())
}

async fn assert_interrupted_032_prefix(clickhouse: &ClickHouseClient) -> Result<()> {
    let documents = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM search_documents FINAL \
             WHERE event_uid = '{STRANDED_EVENT}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    let postings = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM search_postings FINAL \
             WHERE doc_id = '{STRANDED_EVENT}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    if documents != 1 || postings != 0 {
        bail!(
            "interrupted-032 fixture must contain one stranded document and no postings: \
             documents={documents} postings={postings}"
        );
    }
    Ok(())
}

async fn assert_legacy_posting_projection(
    clickhouse: &ClickHouseClient,
    expected_raw_rows: u64,
) -> Result<()> {
    let raw_rows = scalar(
        clickhouse,
        "SELECT toUInt64(count()) AS value FROM search_postings FORMAT JSONEachRow",
    )
    .await?;
    if raw_rows != expected_raw_rows {
        bail!(
            "migration 032 rewrote the historical postings corpus: \
             before={expected_raw_rows} after={raw_rows}"
        );
    }

    let physical_legacy = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM search_postings FINAL \
             WHERE doc_id = '{DEFAULT_EVENT}' AND source_host = '' \
               AND source_file = '' AND source_generation = 0 FORMAT JSONEachRow"
        ),
    )
    .await?;
    let projected_live = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM v_live_search_postings \
             WHERE doc_id = '{DEFAULT_EVENT}' AND source_host = '' \
               AND source_name = '{DEFAULT_SOURCE}' AND source_file = '{DEFAULT_FILE}' \
               AND source_generation = 2 FORMAT JSONEachRow"
        ),
    )
    .await?;
    let shared_live = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM v_live_search_postings \
             WHERE doc_id = '{SHARED_EVENT}' FORMAT JSONEachRow"
        ),
    )
    .await?;
    let repaired_physical = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM search_postings FINAL \
             WHERE doc_id = '{STRANDED_EVENT}' AND source_host = '' \
               AND source_name = '{DEFAULT_SOURCE}' AND source_file = '{DEFAULT_FILE}' \
               AND source_generation = 2 \
               AND term IN ('stranded', 'retry', 'repair') FORMAT JSONEachRow"
        ),
    )
    .await?;
    let repaired_live = scalar(
        clickhouse,
        &format!(
            "SELECT toUInt64(count()) AS value FROM v_live_search_postings \
             WHERE doc_id = '{STRANDED_EVENT}' AND source_ref = '{STRANDED_SOURCE_REF}' \
               AND term IN ('stranded', 'retry', 'repair') FORMAT JSONEachRow"
        ),
    )
    .await?;
    if physical_legacy != 4
        || projected_live != physical_legacy
        || shared_live != 0
        || repaired_physical != STRANDED_POSTINGS
        || repaired_live != STRANDED_POSTINGS
    {
        bail!(
            "legacy posting projection mismatch: physical_legacy={physical_legacy} \
             projected_live={projected_live} shared_live={shared_live} \
             repaired_physical={repaired_physical} repaired_live={repaired_live}"
        );
    }
    Ok(())
}

pub(super) async fn run(clickhouse: &ClickHouseClient, database: &OwnedDatabaseName) -> Result<()> {
    bootstrap_schema_through_030(clickhouse, database).await?;
    seed_pre_031_fixture(clickhouse, database).await?;

    remove_migration_ledger_rows(clickhouse, database.as_str(), &["031"]).await?;
    let migration_031_started = Instant::now();
    let applied_031 = clickhouse
        .run_migrations()
        .await
        .context("failed to apply migration 031 to legacy fixture")?;
    let migration_031_elapsed = migration_031_started.elapsed();
    if applied_031 != ["031"] {
        bail!("legacy fixture expected migration 031 exactly, got {applied_031:?}");
    }

    let first = snapshot(clickhouse).await?;
    assert_control_backfill(&first)?;

    let no_op = clickhouse
        .run_migrations()
        .await
        .context("failed idempotent no-op migration pass")?;
    if !no_op.is_empty() {
        bail!("fully applied migration ledger unexpectedly executed: {no_op:?}");
    }

    remove_migration_ledger_rows(clickhouse, database.as_str(), &["031"]).await?;
    let replay_started = Instant::now();
    let replayed = clickhouse
        .run_migrations()
        .await
        .context("failed forced idempotency replay of migration 031")?;
    let replay_elapsed = replay_started.elapsed();
    if replayed != ["031"] {
        bail!("forced idempotency pass expected migration 031, got {replayed:?}");
    }
    let second = snapshot(clickhouse).await?;
    if second != first {
        bail!(
            "publication head/readiness backfill changed on migration retry: \
             first={first:?} second={second:?}"
        );
    }
    assert_control_backfill(&second)?;

    set_checkpoint_merges(clickhouse, database, true).await?;
    assert_interrupted_032_prefix(clickhouse).await?;
    let pre_032_posting_rows = scalar(
        clickhouse,
        "SELECT toUInt64(count()) AS value FROM search_postings FORMAT JSONEachRow",
    )
    .await?;

    remove_migration_ledger_rows(clickhouse, database.as_str(), &["032"]).await?;
    let migration_032_started = Instant::now();
    let applied_032 = clickhouse
        .run_migrations()
        .await
        .context("failed to apply migration 032 to legacy fixture")?;
    let migration_032_elapsed = migration_032_started.elapsed();
    if applied_032 != ["032"] {
        bail!("legacy fixture expected migration 032 exactly, got {applied_032:?}");
    }
    let post_032_posting_rows = pre_032_posting_rows + STRANDED_POSTINGS;
    assert_legacy_posting_projection(clickhouse, post_032_posting_rows).await?;

    remove_migration_ledger_rows(clickhouse, database.as_str(), &["032"]).await?;
    let replay_032_started = Instant::now();
    let replayed_032 = clickhouse
        .run_migrations()
        .await
        .context("failed forced idempotency replay of migration 032")?;
    let replay_032_elapsed = replay_032_started.elapsed();
    if replayed_032 != ["032"] {
        bail!("forced idempotency pass expected migration 032, got {replayed_032:?}");
    }
    assert_legacy_posting_projection(clickhouse, post_032_posting_rows).await?;

    remove_migration_ledger_rows(clickhouse, database.as_str(), &["033"]).await?;
    let migration_033_started = Instant::now();
    let applied_033 = clickhouse
        .run_migrations()
        .await
        .context("failed to apply migration 033 to legacy fixture")?;
    let migration_033_elapsed = migration_033_started.elapsed();
    if applied_033 != ["033"] {
        bail!("legacy fixture expected migration 033 exactly, got {applied_033:?}");
    }

    remove_migration_ledger_rows(clickhouse, database.as_str(), &["034"]).await?;
    let migration_034_started = Instant::now();
    let applied_034 = clickhouse
        .run_migrations()
        .await
        .context("failed to apply migration 034 to legacy fixture")?;
    let migration_034_elapsed = migration_034_started.elapsed();
    if applied_034 != ["034"] {
        bail!("legacy fixture expected migration 034 exactly, got {applied_034:?}");
    }
    let migration_032_034_elapsed =
        migration_032_elapsed + migration_033_elapsed + migration_034_elapsed;
    let final_state = snapshot(clickhouse).await?;
    if final_state != first {
        bail!(
            "host-aware/read-model migrations changed publication control backfill: \
             before={first:?} after={final_state:?}"
        );
    }
    assert_post_migration_authorization(clickhouse).await?;

    let final_no_op = clickhouse
        .run_migrations()
        .await
        .context("failed final no-op migration pass")?;
    if !final_no_op.is_empty() {
        bail!("fully applied publication migrations reran unexpectedly: {final_no_op:?}");
    }

    let projection = current_projection_state(clickhouse).await?;
    let migration_elapsed = migration_031_elapsed + migration_032_034_elapsed;
    eprintln!(
        "{}",
        json!({
            "kind": "source_publication_legacy_migration_evidence",
            "fixture": {
                "legacy_checkpoint_rows": 5,
                "legacy_event_rows": 3,
                "default_local_sources": 1,
                "named_shared_sources": 2,
                "equal_timestamp_variants": 2,
                "legacy_mcp_session_heads": 1,
                "interrupted_032_stranded_documents": 1,
            },
            "migrations": ["031", "032", "033", "034"],
            "migration_031_us": migration_031_elapsed.as_micros(),
            "migration_031_ms": migration_031_elapsed.as_millis(),
            "migration_032_034_us": migration_032_034_elapsed.as_micros(),
            "migration_032_034_ms": migration_032_034_elapsed.as_millis(),
            "forced_032_idempotency_replay_us": replay_032_elapsed.as_micros(),
            "forced_032_idempotency_replay_ms": replay_032_elapsed.as_millis(),
            "interrupted_032_repaired_postings": STRANDED_POSTINGS,
            "pre_032_physical_postings": pre_032_posting_rows,
            "post_032_physical_postings": post_032_posting_rows,
            "migration_us": migration_elapsed.as_micros(),
            "migration_ms": migration_elapsed.as_millis(),
            "forced_031_idempotency_replay_us": replay_elapsed.as_micros(),
            "forced_031_idempotency_replay_ms": replay_elapsed.as_millis(),
            "published_heads": first.raw_head_rows,
            "checkpoint_transitions": first.raw_transition_rows,
            "publication_readiness": first.raw_readiness_rows,
            "append_control_rows": first.append_control_rows,
            "ambiguous_generations": 1,
            "default_local_live_events": 1,
            "shared_hostless_live_events": 0,
            "mcp_projection_generation": projection.generation,
            "mcp_projection_ready": projection.ready,
        })
    );
    Ok(())
}
