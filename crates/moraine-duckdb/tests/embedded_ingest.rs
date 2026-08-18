use std::path::PathBuf;

use moraine_config::{AppConfig, IngestSource, SourceFormat, StorageEngine};
use moraine_conversations::{
    ConversationListFilter, ConversationRepository, OpenEventRequest, PageRequest, QueryRuntime,
    SearchEventsQuery, SearchMcpEventsQuery, SessionEventsDirection, SessionEventsQuery,
    SessionMetadataSearchQuery, TablePreviewQuery, TurnListFilter,
};
use moraine_duckdb::{run_embedded_ingest, DuckDbStore, DuckDbStoreOptions};

#[tokio::test]
async fn retained_codex_source_ingests_through_the_embedded_pipeline() {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/codex/session.jsonl")
        .canonicalize()
        .expect("canonical Codex fixture");
    let mut config = AppConfig::default();
    config.storage.engine = StorageEngine::Duckdb;
    config.backends.clear();
    config.runtime.root_dir = root.path().to_string_lossy().into_owned();
    config.ingest.state_dir = root.path().join("ingest").to_string_lossy().into_owned();
    config.ingest.sources = vec![IngestSource {
        name: "codex".to_string(),
        harness: "codex".to_string(),
        enabled: true,
        glob: fixture.to_string_lossy().into_owned(),
        watch_root: fixture
            .parent()
            .expect("fixture parent")
            .to_string_lossy()
            .into_owned(),
        format: SourceFormat::Jsonl,
    }];
    config.ingest.reconcile_interval_seconds = 60.0;
    config.ingest.flush_interval_seconds = 0.05;

    let store = DuckDbStore::open(DuckDbStoreOptions::for_runtime_root(root.path()))
        .await
        .expect("open DuckDB store");
    let query_runtime = QueryRuntime::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ingest = tokio::spawn(run_embedded_ingest(
        config,
        query_runtime.clone(),
        store.clone(),
        shutdown_rx,
    ));

    let visible = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if !store
                .clone()
                .current_search_event_ids("inspect", 10)
                .await
                .expect("search current events")
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await;
    shutdown_tx.send(true).expect("request ingest shutdown");
    visible.expect("fixture should become searchable");
    tokio::time::timeout(std::time::Duration::from_secs(5), ingest)
        .await
        .expect("embedded ingest shutdown timeout")
        .expect("embedded ingest task joins")
        .expect("embedded ingest succeeds");
    assert!(
        store
            .clone()
            .current_event_count()
            .await
            .expect("current event count")
            > 0
    );
    let conversations = store
        .list_conversations(ConversationListFilter::default(), PageRequest::default())
        .await
        .expect("list DuckDB conversations");
    assert!(!conversations.items.is_empty());
    let session_id = conversations.items[0].session_id.clone();
    let session = store
        .get_mcp_session(&session_id)
        .await
        .expect("open DuckDB session")
        .expect("fixture session exists");
    assert!(!session.turns.is_empty());
    let results = store
        .search_events(SearchEventsQuery {
            query: "inspect".to_string(),
            ..SearchEventsQuery::default()
        })
        .await
        .expect("search DuckDB events");
    assert!(!results.hits.is_empty());
    let opened = store
        .open_event(OpenEventRequest {
            event_uid: results.hits[0].event_uid.clone(),
            ..OpenEventRequest::default()
        })
        .await
        .expect("open DuckDB event context");
    assert!(opened.found);
    assert!(opened.events.iter().any(|event| event.is_target));
    let turns = store
        .list_turns(
            &session_id,
            TurnListFilter::default(),
            PageRequest::default(),
        )
        .await
        .expect("list DuckDB turns");
    assert!(!turns.items.is_empty());
    assert!(store
        .get_mcp_turn(&session_id, turns.items[0].turn_seq)
        .await
        .expect("open DuckDB turn")
        .is_some());
    let events = store
        .list_session_events(
            SessionEventsQuery {
                session_id: session_id.clone(),
                direction: SessionEventsDirection::Forward,
                event_kinds: None,
            },
            PageRequest::default(),
        )
        .await
        .expect("list DuckDB session events");
    assert!(!events.items.is_empty());
    assert!(store
        .get_mcp_event(&events.items[0].event_uid)
        .await
        .expect("open DuckDB MCP event")
        .is_some());
    let matched_type = match results.hits[0].event_class.as_str() {
        "tool_call" => moraine_conversations::McpEventType::ToolCall,
        "tool_result" => moraine_conversations::McpEventType::ToolResponse,
        "reasoning" => moraine_conversations::McpEventType::Reasoning,
        _ if results.hits[0].actor_role == "user" => moraine_conversations::McpEventType::UserInput,
        _ => moraine_conversations::McpEventType::AssistantResponse,
    };
    let mcp_results = store
        .search_mcp_events(SearchMcpEventsQuery {
            query: "inspect".to_string(),
            event_types: Some(vec![matched_type]),
            ..SearchMcpEventsQuery::default()
        })
        .await
        .expect("search DuckDB MCP events");
    assert!(!mcp_results.hits.is_empty());
    let metadata_results = store
        .search_session_metadata(SessionMetadataSearchQuery {
            query: "codex".to_string(),
            ..SessionMetadataSearchQuery::default()
        })
        .await
        .expect("search DuckDB session metadata");
    assert!(!metadata_results.hits.is_empty());
    let tables = store
        .list_table_summaries()
        .await
        .expect("list DuckDB tables");
    assert!(tables
        .tables
        .iter()
        .any(|table| table.name == "events" && table.rows > 0));
    let preview = store
        .preview_table(TablePreviewQuery {
            table: "events".to_string(),
            limit: 2,
        })
        .await
        .expect("preview DuckDB events");
    assert!(!preview.schema.is_empty());
    assert!(!preview.rows.is_empty());
    let heartbeat = store
        .latest_ingest_heartbeat()
        .await
        .expect("read embedded ingest heartbeat");
    assert!(heartbeat.table_present);
    assert!(heartbeat.latest.is_some());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let wal = root.path().join("duckdb/moraine.duckdb.wal");
        assert_eq!(
            std::fs::metadata(wal)
                .expect("live WAL metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    query_runtime.close_and_drain().await;
    store.close().await.expect("close store");
}
