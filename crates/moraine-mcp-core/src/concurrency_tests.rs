use super::*;
use moraine_conversations::{
    AnalyticsRange, AnalyticsSnapshot, Conversation, ConversationDetailOptions,
    ConversationListFilter, ConversationSearchQuery, ConversationSearchResults, FileAttentionQuery,
    FileAttentionTouch, InMemoryConversationRepository, IngestHeartbeatRead, McpEventOpen,
    McpSessionListFilter, McpSessionListItem, McpSessionOpen, McpTurnOpen, OpenContext,
    OpenEventRequest, Page, PageRequest, RepoConfig, RepoResult, SearchEventsQuery,
    SearchEventsResult, SearchMcpEventsQuery, SearchMcpEventsResult, SessionAnalytics,
    SessionAnalyticsQuery, SessionEventsQuery, SessionMetadata, SessionMetadataSearchQuery,
    SessionMetadataSearchResults, StoreDiagnostics, StoreHealth, TablePreview, TablePreviewQuery,
    TableSummaries, TraceEvent, Turn, TurnListFilter, TurnSummary, WebSearchEvent,
};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, Notify};

const BLOCK_SEARCH: u8 = 1;
const BLOCK_FILE_ATTENTION: u8 = 2;
const DELAY_SEARCH: u8 = 3;
const PANIC_SEARCH: u8 = 4;
const BLOCK_LIST: u8 = 5;
const BLOCK_OPEN: u8 = 6;
const TEST_MAX_PARALLEL_REQUESTS: usize = 2;

struct BlockingRepository {
    inner: InMemoryConversationRepository,
    mode: AtomicU8,
    started: AtomicUsize,
    dropped: AtomicUsize,
    state_changed: Notify,
    release_search: Notify,
    started_queries: Mutex<Vec<String>>,
    block_cancellation: AtomicBool,
    release_cancellation: Notify,
    cancelled_queries: Mutex<Vec<String>>,
}

impl BlockingRepository {
    fn new(mode: u8) -> Self {
        Self {
            inner: InMemoryConversationRepository::default(),
            mode: AtomicU8::new(mode),
            started: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            state_changed: Notify::new(),
            release_search: Notify::new(),
            started_queries: Mutex::new(Vec::new()),
            block_cancellation: AtomicBool::new(false),
            release_cancellation: Notify::new(),
            cancelled_queries: Mutex::new(Vec::new()),
        }
    }

    async fn wait_for_started(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while self.started.load(Ordering::Acquire) < count {
                self.state_changed.notified().await;
            }
        })
        .await
        .expect("blocked repository requests started");
    }

    async fn wait_for_dropped(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while self.dropped.load(Ordering::Acquire) < count {
                self.state_changed.notified().await;
            }
        })
        .await
        .expect("blocked repository requests were cancelled");
    }

    async fn block_forever(&self) -> ! {
        struct DropSignal<'a> {
            dropped: &'a AtomicUsize,
            state_changed: &'a Notify,
        }

        impl Drop for DropSignal<'_> {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::AcqRel);
                self.state_changed.notify_one();
            }
        }

        self.started.fetch_add(1, Ordering::AcqRel);
        self.state_changed.notify_one();
        let _drop_signal = DropSignal {
            dropped: &self.dropped,
            state_changed: &self.state_changed,
        };
        std::future::pending().await
    }

    async fn delay_until_released(&self, query: String) {
        self.started_queries.lock().await.push(query);
        self.started.fetch_add(1, Ordering::AcqRel);
        self.state_changed.notify_one();
        self.release_search.notified().await;
    }

    async fn wait_for_cancelled(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if self.cancelled_queries.lock().await.len() >= count {
                    break;
                }
                self.state_changed.notified().await;
            }
        })
        .await
        .expect("backend queries were explicitly cancelled");
    }
}

#[async_trait::async_trait]
impl ConversationRepository for BlockingRepository {
    fn config(&self) -> &RepoConfig {
        self.inner.config()
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        self.inner.prewarm_mcp_search_state().await
    }

    async fn list_session_analytics(
        &self,
        query: SessionAnalyticsQuery,
    ) -> RepoResult<Vec<SessionAnalytics>> {
        self.inner.list_session_analytics(query).await
    }

    async fn analytics_series(&self, range: AnalyticsRange) -> RepoResult<AnalyticsSnapshot> {
        self.inner.analytics_series(range).await
    }

    async fn list_web_searches(&self, limit: u16) -> RepoResult<Vec<WebSearchEvent>> {
        self.inner.list_web_searches(limit).await
    }

    async fn latest_ingest_heartbeat(&self) -> RepoResult<IngestHeartbeatRead> {
        self.inner.latest_ingest_heartbeat().await
    }

    async fn list_table_summaries(&self) -> RepoResult<TableSummaries> {
        self.inner.list_table_summaries().await
    }

    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview> {
        self.inner.preview_table(query).await
    }

    async fn read_store_health(&self) -> RepoResult<StoreHealth> {
        self.inner.read_store_health().await
    }

    async fn read_store_diagnostics(&self) -> RepoResult<StoreDiagnostics> {
        self.inner.read_store_diagnostics().await
    }

    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<moraine_conversations::ConversationSummary>> {
        self.inner.list_conversations(filter, page).await
    }

    async fn get_conversation(
        &self,
        session_id: &str,
        opts: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        self.inner.get_conversation(session_id, opts).await
    }

    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>> {
        self.inner.get_session_metadata(session_id).await
    }

    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>> {
        if self.mode.load(Ordering::Acquire) == BLOCK_OPEN {
            self.block_forever().await
        }
        self.inner.get_mcp_session(session_id).await
    }

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        if self.mode.load(Ordering::Acquire) == BLOCK_LIST {
            self.block_forever().await
        }
        self.inner.list_mcp_sessions(filter, page).await
    }

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        self.inner.list_turns(session_id, filter, page).await
    }

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        self.inner.get_turn(session_id, turn_seq).await
    }

    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        self.inner.get_mcp_turn(session_id, turn_seq).await
    }

    async fn open_event(&self, request: OpenEventRequest) -> RepoResult<OpenContext> {
        self.inner.open_event(request).await
    }

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        self.inner.get_mcp_event(event_uid).await
    }

    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        self.inner.list_session_events(query, page).await
    }

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        self.inner.search_events(query).await
    }

    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        match self.mode.load(Ordering::Acquire) {
            BLOCK_SEARCH => self.block_forever().await,
            DELAY_SEARCH if query.query.starts_with("slow") => {
                self.delay_until_released(query.query.clone()).await
            }
            PANIC_SEARCH => panic!("simulated repository panic"),
            _ => {}
        }
        self.inner.search_mcp_events(query).await
    }

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        self.inner.search_conversations(query).await
    }

    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        self.inner.search_session_metadata(query).await
    }

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        if self.mode.load(Ordering::Acquire) == BLOCK_FILE_ATTENTION {
            self.block_forever().await
        }
        self.inner.file_attention(query).await
    }

    async fn cancel_query(&self, query_id: &str) -> RepoResult<()> {
        if self.block_cancellation.load(Ordering::Acquire) {
            self.release_cancellation.notified().await;
        }
        self.cancelled_queries
            .lock()
            .await
            .push(query_id.to_string());
        self.state_changed.notify_one();
        Ok(())
    }
}

type ClientReader = BufReader<ReadHalf<tokio::io::DuplexStream>>;
type ClientWriter = WriteHalf<tokio::io::DuplexStream>;

fn test_state(repository: Arc<BlockingRepository>, max_parallel_requests: usize) -> Arc<AppState> {
    test_state_with_admission(repository, max_parallel_requests, MAX_QUEUED_REQUESTS)
}

fn test_state_with_admission(
    repository: Arc<BlockingRepository>,
    max_parallel_requests: usize,
    max_queued_requests: usize,
) -> Arc<AppState> {
    AppState::with_repository(
        Arc::new(AppConfig::default()),
        repository,
        Arc::new(AtomicBool::new(false)),
        std::env::current_dir().ok(),
        Arc::new(RequestAdmission::new(
            max_parallel_requests,
            max_queued_requests,
        )),
    )
}

fn start_connection_with_state(
    state: Arc<AppState>,
) -> (
    ClientReader,
    ClientWriter,
    tokio::task::JoinHandle<Result<()>>,
) {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let task = tokio::spawn(serve_connection(
        state,
        BufReader::new(server_read),
        server_write,
    ));
    (BufReader::new(client_read), client_write, task)
}

fn start_connection(
    repository: Arc<BlockingRepository>,
) -> (
    ClientReader,
    ClientWriter,
    tokio::task::JoinHandle<Result<()>>,
) {
    start_connection_with_capacity(repository, TEST_MAX_PARALLEL_REQUESTS)
}

fn start_connection_with_capacity(
    repository: Arc<BlockingRepository>,
    max_parallel_requests: usize,
) -> (
    ClientReader,
    ClientWriter,
    tokio::task::JoinHandle<Result<()>>,
) {
    let state = test_state(repository, max_parallel_requests);
    start_connection_with_state(state)
}

fn start_connection_with_shutdown(
    repository: Arc<BlockingRepository>,
) -> (
    ClientReader,
    ClientWriter,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let state = test_state(repository, TEST_MAX_PARALLEL_REQUESTS);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(serve_connection_with_shutdown(
        state,
        BufReader::new(server_read),
        server_write,
        None,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    (BufReader::new(client_read), client_write, shutdown_tx, task)
}

fn search_request_with_query(id: Value, query: &str) -> String {
    let mut request = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": "search_sessions",
            "arguments": { "query": query },
        },
    }))
    .expect("serialize search request");
    request.push('\n');
    request
}

fn search_request(id: usize) -> String {
    search_request_with_query(json!(id), "latency")
}
fn tool_request(id: &str, name: &str, arguments: Value) -> String {
    let mut request = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments,
        },
    }))
    .expect("serialize tool request");
    request.push('\n');
    request
}

async fn read_response<R>(reader: &mut R) -> Value
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        reader.read_line(&mut line),
    )
    .await
    .expect("response deadline")
    .expect("read response");
    serde_json::from_str(line.trim()).expect("one complete JSON response frame")
}

#[tokio::test]
async fn full_queue_returns_structured_overload_within_one_hundred_milliseconds() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let state = test_state_with_admission(repository.clone(), 1, 2);
    let admission = state.request_admission.clone();
    let (mut reader, mut writer, server) = start_connection_with_state(state);

    let burst = (1..=4)
        .map(|id| search_request_with_query(json!(id), &format!("slow-{id}")))
        .collect::<String>();
    let started_at = tokio::time::Instant::now();
    writer
        .write_all(burst.as_bytes())
        .await
        .expect("send over-capacity burst");
    let overloaded = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        read_response(&mut reader),
    )
    .await
    .expect("queue-full response stayed within overload budget");

    assert_eq!(overloaded["id"], json!(4));
    assert_eq!(overloaded["result"]["isError"], json!(true));
    assert_eq!(
        overloaded["result"]["structuredContent"]["error"]["code"],
        json!("deadline_exceeded")
    );
    assert_eq!(
        overloaded["result"]["structuredContent"]["error"]["details"]["reason"],
        json!("queue_full")
    );
    assert_eq!(
        overloaded["result"]["structuredContent"]["error"]["details"]["max_executing"],
        json!(1)
    );
    assert_eq!(
        overloaded["result"]["structuredContent"]["error"]["details"]["max_queued"],
        json!(2)
    );
    assert!(started_at.elapsed() < std::time::Duration::from_millis(100));
    repository.wait_for_started(1).await;
    assert_eq!(admission.slots.available_permits(), 0);

    for id in 1..=3 {
        writer
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":{id}}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .expect("cancel admitted request");
    }
    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
    repository.wait_for_cancelled(1).await;
    assert_eq!(repository.started.load(Ordering::Acquire), 1);
    assert_eq!(admission.execution.available_permits(), 1);
    assert_eq!(admission.slots.available_permits(), 3);
}

#[tokio::test]
async fn queued_success_reports_wall_time_from_frame_acceptance() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let state = test_state_with_admission(repository.clone(), 1, 1);
    let (mut reader, mut writer, server) = start_connection_with_state(state);

    writer
        .write_all(search_request_with_query(json!(1), "slow-first").as_bytes())
        .await
        .expect("send executing request");
    repository.wait_for_started(1).await;
    writer
        .write_all(search_request_with_query(json!(2), "queued-success").as_bytes())
        .await
        .expect("send queued request");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    repository.release_search.notify_one();

    let first = read_response(&mut reader).await;
    let second = read_response(&mut reader).await;
    let queued = [&first, &second]
        .into_iter()
        .find(|response| response["id"] == json!(2))
        .expect("queued response");
    let performance = &queued["result"]["structuredContent"]["performance"];
    assert!(
        performance["elapsed_ms"].as_u64().expect("elapsed_ms") >= 750,
        "queue time must be included: {performance}"
    );
    assert_eq!(
        performance.as_object().expect("performance object").len(),
        1,
        "performance must only report elapsed time: {performance}"
    );

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn queued_request_waits_for_execution_capacity_without_expiring() {
    let repository = Arc::new(BlockingRepository::new(0));
    let state = test_state_with_admission(repository.clone(), 1, 1);
    let admission = state.request_admission.clone();
    let held = admission
        .try_register()
        .expect("reserve execution slot")
        .acquire()
        .await
        .expect("acquire execution slot");
    let (mut reader, mut writer, server) = start_connection_with_state(state);

    writer
        .write_all(search_request(1).as_bytes())
        .await
        .expect("send queued request");
    tokio::time::sleep(std::time::Duration::from_millis(4_100)).await;
    assert_eq!(repository.started.load(Ordering::Acquire), 0);
    assert!(repository.cancelled_queries.lock().await.is_empty());
    assert_eq!(admission.slots.available_permits(), 0);

    drop(held);
    let response = read_response(&mut reader).await;
    assert_eq!(response["id"], json!(1));
    assert_eq!(response["result"]["isError"], json!(false));

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
    assert_eq!(admission.execution.available_permits(), 1);
    assert_eq!(admission.slots.available_permits(), 2);
}

#[tokio::test]
async fn running_request_continues_past_four_seconds() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let state = test_state_with_admission(repository.clone(), 1, 1);
    let admission = state.request_admission.clone();
    let (mut reader, mut writer, server) = start_connection_with_state(state);

    writer
        .write_all(search_request_with_query(json!(1), "slow-deadline").as_bytes())
        .await
        .expect("send slow request");
    repository.wait_for_started(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(4_100)).await;
    assert!(repository.cancelled_queries.lock().await.is_empty());
    assert_eq!(admission.execution.available_permits(), 0);
    assert_eq!(admission.slots.available_permits(), 1);

    repository.release_search.notify_one();
    let response = read_response(&mut reader).await;
    assert_eq!(response["id"], json!(1));
    assert_eq!(response["result"]["isError"], json!(false));

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
    assert_eq!(admission.execution.available_permits(), 1);
    assert_eq!(admission.slots.available_permits(), 2);
}

#[tokio::test]
async fn queued_requests_acquire_execution_in_frame_acceptance_order() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let state = test_state_with_admission(repository.clone(), 1, 3);
    let admission = state.request_admission.clone();
    let held = admission
        .try_register()
        .expect("reserve execution slot")
        .acquire()
        .await
        .expect("acquire execution slot");
    let (mut reader, mut writer, server) = start_connection_with_state(state);

    let burst = (1..=3)
        .map(|id| search_request_with_query(json!(id), &format!("slow-{id}")))
        .collect::<String>();
    writer
        .write_all(burst.as_bytes())
        .await
        .expect("send ordered queued burst");
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"barrier\",\"method\":\"ping\"}\n")
        .await
        .expect("send dispatch barrier");
    let barrier = read_response(&mut reader).await;
    assert_eq!(barrier["id"], json!("barrier"));
    assert_eq!(repository.started.load(Ordering::Acquire), 0);

    drop(held);
    for id in 1..=3 {
        repository.wait_for_started(id).await;
        assert_eq!(
            repository.started_queries.lock().await[id - 1],
            format!("slow-{id}")
        );
        repository.release_search.notify_one();
        let response = read_response(&mut reader).await;
        assert_eq!(response["id"], json!(id));
    }

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
    assert_eq!(admission.execution.available_permits(), 1);
    assert_eq!(admission.slots.available_permits(), 4);
}

#[tokio::test]
async fn blocked_burst_queues_retrieval_while_control_requests_stay_responsive() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let (mut reader, mut writer, server) = start_connection(repository.clone());

    let burst = (1..=TEST_MAX_PARALLEL_REQUESTS)
        .map(|id| search_request_with_query(json!(id), "slow"))
        .collect::<String>();
    writer
        .write_all(burst.as_bytes())
        .await
        .expect("send admitted burst");
    repository
        .wait_for_started(TEST_MAX_PARALLEL_REQUESTS)
        .await;

    writer
        .write_all(search_request_with_query(json!(3), "slow").as_bytes())
        .await
        .expect("send queued request");
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"ping\",\"method\":\"ping\",\"params\":{}}\n")
        .await
        .expect("send control request");
    writer
        .write_all(tool_request("invalid-open", "open", json!({})).as_bytes())
        .await
        .expect("send invalid retrieval request");
    writer
        .write_all(tool_request("unknown-tool", "not_a_tool", json!({})).as_bytes())
        .await
        .expect("send unknown tool request");
    writer
        .write_all(search_request_with_query(json!(3), "slow").as_bytes())
        .await
        .expect("send duplicate queued id");

    let responses = [
        read_response(&mut reader).await,
        read_response(&mut reader).await,
        read_response(&mut reader).await,
        read_response(&mut reader).await,
    ];
    let ping = responses
        .iter()
        .find(|response| response["id"] == json!("ping"))
        .expect("ping bypassed saturated retrievals");
    assert_eq!(ping["result"], json!({}));
    let duplicate = responses
        .iter()
        .find(|response| response["id"] == json!(3))
        .expect("queued request id was reserved");
    assert_eq!(duplicate["error"]["code"], json!(-32600));
    let invalid = responses
        .iter()
        .find(|response| response["id"] == json!("invalid-open"))
        .expect("invalid arguments bypassed saturated retrievals");
    assert_eq!(invalid["result"]["isError"], json!(true));
    let unknown = responses
        .iter()
        .find(|response| response["id"] == json!("unknown-tool"))
        .expect("unknown tool bypassed saturated retrievals");
    assert_eq!(unknown["result"]["isError"], json!(true));
    assert_eq!(
        repository.started.load(Ordering::Acquire),
        TEST_MAX_PARALLEL_REQUESTS
    );

    repository.release_search.notify_one();
    repository
        .wait_for_started(TEST_MAX_PARALLEL_REQUESTS + 1)
        .await;
    repository.release_search.notify_waiters();

    let mut completed_ids = Vec::new();
    for _ in 0..=TEST_MAX_PARALLEL_REQUESTS {
        let response = read_response(&mut reader).await;
        assert!(response.get("result").is_some());
        completed_ids.push(response["id"].as_u64().expect("numeric response id"));
    }
    completed_ids.sort_unstable();
    assert_eq!(completed_ids, vec![1, 2, 3]);

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn queued_cancellation_never_reaches_repository_or_backend_cancel() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let (mut reader, mut writer, server) = start_connection_with_capacity(repository.clone(), 1);

    writer
        .write_all(search_request_with_query(json!(1), "slow").as_bytes())
        .await
        .expect("send admitted request");
    repository.wait_for_started(1).await;
    writer
        .write_all(search_request_with_query(json!(2), "slow").as_bytes())
        .await
        .expect("send queued request");
    writer
        .write_all(
            b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":2,\"reason\":\"test\"}}\n",
        )
        .await
        .expect("cancel queued request");
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"barrier\",\"method\":\"ping\",\"params\":{}}\n")
        .await
        .expect("send dispatch barrier");

    let barrier = read_response(&mut reader).await;
    assert_eq!(barrier["id"], json!("barrier"));
    repository.release_search.notify_one();
    let completed = read_response(&mut reader).await;
    assert_eq!(completed["id"], json!(1));
    assert_eq!(repository.started.load(Ordering::Acquire), 1);
    assert!(repository.cancelled_queries.lock().await.is_empty());

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn cancellation_is_registered_before_each_tool_first_repository_read() {
    let repository = Arc::new(BlockingRepository::new(BLOCK_SEARCH));
    let (_reader, mut writer, server) = start_connection(repository.clone());
    let session_id = crate::contract::McpSessionId::from_raw_session_id("blocked-session")
        .expect("valid session id")
        .to_string();
    let requests = [
        (
            BLOCK_SEARCH,
            "scope",
            tool_request(
                "scope",
                "search_sessions",
                json!({ "query": "latency", "within_id": session_id }),
            ),
        ),
        (
            BLOCK_LIST,
            "list",
            tool_request(
                "list",
                "list_sessions",
                json!({
                    "start_datetime": "2026-01-01T00:00:00Z",
                    "end_datetime": "2026-01-02T00:00:00Z",
                    "limit": 1,
                }),
            ),
        ),
        (
            BLOCK_OPEN,
            "open",
            tool_request("open", "open", json!({ "id": session_id })),
        ),
    ];

    for (index, (mode, id, request)) in requests.into_iter().enumerate() {
        repository.mode.store(mode, Ordering::Release);
        writer
            .write_all(request.as_bytes())
            .await
            .expect("send blocked tool request");
        repository.wait_for_started(index + 1).await;
        writer
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":\"{id}\"}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .expect("cancel blocked tool request");
        repository.wait_for_dropped(index + 1).await;
        repository.wait_for_cancelled(index + 1).await;
    }

    let cancelled = repository.cancelled_queries.lock().await.clone();
    assert_eq!(cancelled.len(), 3);
    assert!(cancelled[0].starts_with("moraine-search-sessions-"));
    assert!(cancelled[1..]
        .iter()
        .all(|query_id| query_id.starts_with("moraine-request-")));

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn request_cancellation_propagates_unique_clickhouse_query_ids() {
    let repository = Arc::new(BlockingRepository::new(BLOCK_FILE_ATTENTION));
    let (mut reader, mut writer, server) = start_connection(repository.clone());

    for id in ["file-a", "file-b"] {
        writer
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":\"{id}\",\"method\":\"tools/call\",\"params\":{{\"name\":\"file_attention\",\"arguments\":{{\"path\":\"src/lib.rs\"}}}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .expect("send file request");
    }
    repository.wait_for_started(2).await;

    for id in ["file-a", "file-b"] {
        writer
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":\"{id}\"}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .expect("cancel file request");
    }
    repository.wait_for_dropped(2).await;

    repository.wait_for_cancelled(2).await;
    let cancelled = repository.cancelled_queries.lock().await.clone();
    assert_ne!(cancelled[0], cancelled[1]);
    assert!(cancelled
        .iter()
        .all(|query_id| query_id.starts_with("moraine-file-attention-")));

    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"ping\",\"method\":\"ping\"}\n")
        .await
        .expect("send ping after cancellation");
    let ping = read_response(&mut reader).await;
    assert_eq!(ping["id"], json!("ping"));

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn full_socket_disconnect_cancels_blocked_repository_work() {
    use std::os::fd::AsRawFd;

    let repository = Arc::new(BlockingRepository::new(BLOCK_SEARCH));
    let state = test_state(repository.clone(), 1);
    let (server_stream, client_stream) =
        tokio::net::UnixStream::pair().expect("create socket pair");
    let peer_fd = server_stream.as_raw_fd();
    let (server_read, server_write) = server_stream.into_split();
    let (client_read, mut client_write) = client_stream.into_split();
    let mut client_read = BufReader::new(client_read);
    let server = tokio::spawn(serve_connection_with_lifecycle(
        state,
        BufReader::new(server_read),
        server_write,
        None,
        pending(),
        wait_for_socket_disconnect(peer_fd),
    ));

    let burst = format!(
        "{}{}{}",
        search_request(1),
        search_request(2),
        "{\"jsonrpc\":\"2.0\",\"id\":\"disconnect-barrier\",\"method\":\"ping\"}\n"
    );
    client_write
        .write_all(burst.as_bytes())
        .await
        .expect("send running, queued, and barrier requests");
    repository.wait_for_started(1).await;
    let barrier = read_response(&mut client_read).await;
    assert_eq!(barrier["id"], json!("disconnect-barrier"));
    drop(client_read);
    drop(client_write);

    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("disconnect cleanup deadline")
        .expect("connection task joined")
        .expect("connection succeeded");
    repository.wait_for_dropped(1).await;
    repository.wait_for_cancelled(1).await;
    assert_eq!(repository.started.load(Ordering::Acquire), 1);
    assert_eq!(repository.cancelled_queries.lock().await.len(), 1);
    assert!(repository.cancelled_queries.lock().await[0].starts_with("moraine-search-sessions-"));
}

#[tokio::test]
async fn clean_read_half_close_preserves_slow_admitted_response() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let (mut reader, mut writer, server) = start_connection(repository.clone());

    writer
        .write_all(search_request_with_query(json!("slow-half-close"), "slow").as_bytes())
        .await
        .expect("send slow request");
    repository.wait_for_started(1).await;
    writer.shutdown().await.expect("half-close request stream");
    drop(writer);

    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    repository.release_search.notify_one();
    let response = read_response(&mut reader).await;
    assert_eq!(response["id"], json!("slow-half-close"));
    assert!(response.get("result").is_some());

    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("half-closed connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn failed_request_task_releases_id_permit_and_backend_query() {
    let repository = Arc::new(BlockingRepository::new(PANIC_SEARCH));
    let (mut reader, mut writer, server) = start_connection(repository.clone());

    writer
        .write_all(search_request_with_query(json!("reused"), "panic").as_bytes())
        .await
        .expect("send panicking request");
    let failed = read_response(&mut reader).await;
    assert_eq!(failed["id"], json!("reused"));
    assert_eq!(failed["error"]["code"], json!(-32603));
    repository.wait_for_cancelled(1).await;

    repository.mode.store(0, Ordering::Release);
    writer
        .write_all(search_request_with_query(json!("reused"), "recovered").as_bytes())
        .await
        .expect("reuse request id after task failure");
    let recovered = read_response(&mut reader).await;
    assert_eq!(recovered["id"], json!("reused"));
    assert!(recovered.get("result").is_some());

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn responses_are_framed_and_keep_ids_when_completion_is_out_of_order() {
    let repository = Arc::new(BlockingRepository::new(DELAY_SEARCH));
    let (mut reader, mut writer, server) = start_connection(repository.clone());

    writer
        .write_all(search_request_with_query(json!("slow-id"), "slow").as_bytes())
        .await
        .expect("send slow request");
    repository.wait_for_started(1).await;
    writer
        .write_all(search_request_with_query(json!(42), "fast").as_bytes())
        .await
        .expect("send fast request");

    let fast = read_response(&mut reader).await;
    assert_eq!(fast["id"], json!(42));
    assert!(fast.get("result").is_some());

    repository.release_search.notify_one();
    let slow = read_response(&mut reader).await;
    assert_eq!(slow["id"], json!("slow-id"));
    assert!(slow.get("result").is_some());

    writer.shutdown().await.expect("half-close request stream");
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection drained")
        .expect("connection task joined")
        .expect("connection succeeded");
}

#[tokio::test]
async fn service_shutdown_cancels_in_flight_backend_query() {
    let repository = Arc::new(BlockingRepository::new(BLOCK_FILE_ATTENTION));
    let (_reader, mut writer, shutdown, server) =
        start_connection_with_shutdown(repository.clone());

    for id in ["shutdown-a", "shutdown-b", "shutdown-queued"] {
        writer
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":\"{id}\",\"method\":\"tools/call\",\"params\":{{\"name\":\"file_attention\",\"arguments\":{{\"path\":\"src/lib.rs\"}}}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .expect("send shutdown request");
    }
    repository
        .wait_for_started(TEST_MAX_PARALLEL_REQUESTS)
        .await;
    shutdown.send(()).expect("request service shutdown");

    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .expect("service shutdown deadline")
        .expect("connection task joined")
        .expect("connection succeeded");
    repository
        .wait_for_dropped(TEST_MAX_PARALLEL_REQUESTS)
        .await;
    repository
        .wait_for_cancelled(TEST_MAX_PARALLEL_REQUESTS)
        .await;
    assert_eq!(
        repository.started.load(Ordering::Acquire),
        TEST_MAX_PARALLEL_REQUESTS
    );
    assert!(repository
        .cancelled_queries
        .lock()
        .await
        .iter()
        .all(|query_id| query_id.starts_with("moraine-file-attention-")));
}

#[cfg(unix)]
#[tokio::test]
async fn central_service_shutdown_cancels_process_wide_queued_requests() {
    async fn connect(path: &std::path::Path) -> tokio::net::UnixStream {
        for _ in 0..100 {
            if let Ok(stream) = tokio::net::UnixStream::connect(path).await {
                return stream;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("failed to connect to test socket {}", path.display());
    }

    let repository = Arc::new(BlockingRepository::new(BLOCK_FILE_ATTENTION));
    let mut cfg = AppConfig::default();
    cfg.mcp.max_parallel_requests = Some(1);
    let cfg = Arc::new(cfg);
    let repository_trait: Arc<dyn ConversationRepository> = repository.clone();
    let router = Arc::new(
        BackendRepositoryRouter::from_preloaded_for_testing(
            cfg.clone(),
            [("default".to_string(), repository_trait)],
        )
        .expect("preload blocking repository"),
    );
    let socket_path = PathBuf::from(format!(
        "/tmp/moraine-shutdown-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&socket_path);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_socket_path = socket_path.clone();
    let server = tokio::spawn(async move {
        run_socket_with_router(cfg, router, server_socket_path, async move {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let running_client = connect(&socket_path).await;
    let queued_client = connect(&socket_path).await;
    let (running_read, mut running_writer) = running_client.into_split();
    let _running_reader = BufReader::new(running_read);
    let (queued_read, mut queued_writer) = queued_client.into_split();
    let mut queued_reader = BufReader::new(queued_read);

    running_writer
        .write_all(
            tool_request("running", "file_attention", json!({"path": "src/lib.rs"})).as_bytes(),
        )
        .await
        .expect("send running request");
    repository.wait_for_started(1).await;
    queued_writer
        .write_all(
            tool_request("queued", "file_attention", json!({"path": "src/lib.rs"})).as_bytes(),
        )
        .await
        .expect("send queued request");
    queued_writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"barrier\",\"method\":\"ping\",\"params\":{}}\n")
        .await
        .expect("send queued-connection barrier");
    let barrier = read_response(&mut queued_reader).await;
    assert_eq!(barrier["id"], json!("barrier"));

    shutdown_tx.send(()).expect("request central shutdown");
    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .expect("central shutdown deadline")
        .expect("central server task joined")
        .expect("central server shutdown succeeded");
    repository.wait_for_dropped(1).await;
    repository.wait_for_cancelled(1).await;
    assert_eq!(repository.started.load(Ordering::Acquire), 1);
    assert_eq!(repository.cancelled_queries.lock().await.len(), 1);
    assert!(!socket_path.exists(), "central socket was cleaned up");
}

#[tokio::test]
async fn malformed_transport_still_cancels_in_flight_backend_query() {
    let repository = Arc::new(BlockingRepository::new(BLOCK_FILE_ATTENTION));
    let (_reader, mut writer, server) = start_connection(repository.clone());

    for id in ["malformed-a", "malformed-b", "malformed-queued"] {
        writer
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":\"{id}\",\"method\":\"tools/call\",\"params\":{{\"name\":\"file_attention\",\"arguments\":{{\"path\":\"src/lib.rs\"}}}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .expect("send malformed-transport request");
    }
    repository
        .wait_for_started(TEST_MAX_PARALLEL_REQUESTS)
        .await;
    writer
        .write_all(&[0xff, b'\n'])
        .await
        .expect("send malformed UTF-8 frame");

    let error = tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .expect("transport cleanup deadline")
        .expect("connection task joined")
        .expect_err("invalid UTF-8 must fail the connection");
    assert!(error
        .to_string()
        .contains("stream did not contain valid UTF-8"));
    repository
        .wait_for_dropped(TEST_MAX_PARALLEL_REQUESTS)
        .await;
    repository
        .wait_for_cancelled(TEST_MAX_PARALLEL_REQUESTS)
        .await;
    assert_eq!(
        repository.started.load(Ordering::Acquire),
        TEST_MAX_PARALLEL_REQUESTS
    );
}
