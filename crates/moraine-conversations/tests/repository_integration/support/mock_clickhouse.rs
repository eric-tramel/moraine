use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::ClickHouseConfig;
use moraine_conversations::{ClickHouseConversationRepository, RepoConfig, SessionOriginScope};
use serde_json::json;
use tokio::sync::Notify;

use super::mcp_open_fixtures::{
    event_lookup, event_ref_rows, full_event_row, session_row, turn_rows,
};
use super::responses::{json_each_row, trace_event_row, turn_summary_row};

#[derive(Clone)]
pub(crate) struct ScriptedBarrier {
    pub(crate) reached: Arc<Notify>,
    pub(crate) release: Arc<Notify>,
}

#[derive(Clone)]
pub(crate) struct QueryBarrier {
    pub(crate) required: Vec<&'static str>,
    pub(crate) reached: Arc<Notify>,
    pub(crate) release: Arc<Notify>,
}

#[derive(Clone)]
pub(crate) struct ScriptedResponse {
    pub(crate) required: Vec<&'static str>,
    pub(crate) forbidden: Vec<&'static str>,
    pub(crate) status: StatusCode,
    pub(crate) body: String,
    pub(crate) barrier: Option<ScriptedBarrier>,
}

impl ScriptedResponse {
    pub(crate) fn rows(required: &[&'static str], rows: serde_json::Value) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::OK,
            barrier: None,
            body: json_each_row(rows),
        }
    }

    pub(crate) fn raw(required: &[&'static str], body: impl Into<String>) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::OK,
            barrier: None,
            body: body.into(),
        }
    }

    pub(crate) fn failure(required: &[&'static str], message: &'static str) -> Self {
        Self {
            required: required.to_vec(),
            forbidden: Vec::new(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
            barrier: None,
            body: message.to_string(),
        }
    }

    pub(crate) fn forbidding(mut self, forbidden: &[&'static str]) -> Self {
        self.forbidden = forbidden.to_vec();
        self
    }

    pub(crate) fn blocked(mut self, reached: Arc<Notify>, release: Arc<Notify>) -> Self {
        self.barrier = Some(ScriptedBarrier { reached, release });
        self
    }
}

/// `mcp_candidate_fetch_size(unique_fetch_limit = n_hits + 1 = 3)` — the exact
/// window the bounded ranking pass asks for at the fixtures' `n_hits = 2`. The
/// saturation fixture must return EXACTLY this many rows or the `saturated`
/// half of the marker predicate is not exercised.
pub(crate) const MOCK_SATURATED_CANDIDATE_WINDOW: u32 = 9;

#[derive(Clone, Default)]
pub(crate) struct MockOptions {
    pub(crate) omit_second_snippet_row: bool,
    pub(crate) dirty_projection_on_first_candidate: bool,
    pub(crate) omit_first_mcp_detail_row: bool,
    pub(crate) repeated_corpus_stats_barrier: Option<ScriptedBarrier>,
    /// Return a candidate window of exactly `candidate_fetch_size` rows that
    /// all collapse into one another under the #539/#565 dedup rule — the
    /// input the retired 16-page refill loop turned into
    /// `backend("duplicate scan budget exhausted")`.
    pub(crate) saturate_candidate_window: bool,
    /// Drop `evt-a-11` from the issue-597 dedup-key read, simulating a
    /// `search_documents` revision that no longer exists at the posting's
    /// `post_version` (an MV gap, an interrupted backfill).
    pub(crate) omit_dedup_key_for_second_candidate: bool,
    /// Rank ONE content-addressed `event_uid` attributed to TWO sessions. The
    /// uid is addressed over `source_file|source_generation|source_line_no|
    /// source_offset|record_fingerprint` and deliberately excludes
    /// `session_id` (#608), so this is a real corpus shape, not a synthetic
    /// one — the reference host carries 19,846 of them.
    pub(crate) shared_event_uid_across_sessions: bool,
    /// Rank two DISTINCT events that share session, turn, event type and
    /// timestamp and differ only in content. The #539 content digest is the
    /// only input that keeps them apart.
    pub(crate) two_distinct_events_in_one_turn: bool,
    /// Report a navigation `event_version` for `evt-a-11` that differs from the
    /// version the locator authorized — the second, independent version check.
    pub(crate) stale_navigation_version_for_second_candidate: bool,
    /// Report an out-of-scope navigation `origin_cwd` for `evt-a-11`, so only
    /// the exact Phase 4 re-check can drop it (the directory recall filter
    /// admitted it).
    pub(crate) out_of_scope_cwd_for_second_candidate: bool,
    /// The requested turn's live uid set exceeds `MAX_TURN_SCOPE_UIDS`, so
    /// ranking falls back to session recall and the turn is enforced only by
    /// the exact Phase 4 re-check.
    pub(crate) turn_scope_uid_overflow: bool,
    pub(crate) scripted_responses: Vec<ScriptedResponse>,
    pub(crate) query_barrier: Option<QueryBarrier>,
    /// Verdict for the issue-598 `open_v2` readiness key, which gates the
    /// issue-599 directory session-listing path. `Some` answers the
    /// `mcp_read_index_state` probe out of band, so it neither consumes a
    /// scripted response nor lands in the recorded query log; `None` leaves the
    /// probe visible to the scripts, which is what the store health tests
    /// assert on directly.
    pub(crate) open_v2_reader_ready: Option<bool>,
}

#[derive(Default)]
pub(crate) struct MockState {
    pub(crate) queries: Mutex<Vec<String>>,
    pub(crate) publication_snapshot_queries: Mutex<Vec<String>>,
    /// `mcp_read_index_state` reads answered out of band by
    /// [`MockOptions::open_v2_reader_ready`]. Kept out of `queries` so the
    /// per-test statement budgets stay about the operation under test.
    pub(crate) readiness_probe_queries: Mutex<Vec<String>>,
    pub(crate) query_ids: Mutex<Vec<Option<String>>>,
    pub(crate) request_params: Mutex<Vec<HashMap<String, String>>>,
    pub(crate) options: MockOptions,
    pub(crate) scripted_responses: Mutex<Option<VecDeque<ScriptedResponse>>>,
}

/// Run one repository interaction under a generous Interactive-class
/// envelope (30s deadline, bundled-default caps). Post-flip (issue #600
/// W12) the transport refuses unenveloped statements, so every integration
/// test scopes its repository calls through this helper; tests that prove
/// specific budget behavior build their own tighter envelopes instead.
pub(crate) async fn scoped<F: std::future::Future>(f: F) -> F::Output {
    moraine_conversations::QueryEnvelope::new(
        "test",
        moraine_conversations::QueryClass::Interactive,
        &interactive_test_budget(30.0),
    )
    .scope(f)
    .await
}

/// Interactive-class query budget with the given deadline for
/// envelope-scoped integration tests; every other field keeps the bundled
/// defaults (budgets are constructible only from validated config).
pub(crate) fn interactive_test_budget(
    deadline_seconds: f64,
) -> moraine_config::ValidatedQueryBudget {
    let defaults = moraine_config::QueryBudgetsConfig::default();
    let cfg = moraine_config::QueryBudgetsConfig {
        interactive: moraine_config::QueryBudgetClassConfig {
            deadline_seconds,
            ..defaults.interactive
        },
        ..defaults
    };
    moraine_config::ValidatedQueryBudgets::from_config(&cfg)
        .expect("test budget validates")
        .interactive
}

pub(crate) fn test_clickhouse_config(url: String) -> ClickHouseConfig {
    ClickHouseConfig {
        url,
        database: "moraine".to_string(),
        username: "default".to_string(),
        password: String::new(),
        timeout_seconds: 5.0,
        request_compression: Default::default(),
        async_insert: true,
        wait_for_async_insert: true,
        allow_newer_server: false,
    }
}
/// The subset of `rows` whose `session_id` appears in a batched statement's
/// `session_id IN ['…']` array literal — the mock's stand-in for the
/// primary-key prune the real hydration statements get.
fn rows_for_requested_sessions(query: &str, rows: &serde_json::Value) -> Vec<serde_json::Value> {
    rows.as_array()
        .expect("fixture rows are an array")
        .iter()
        .filter(|row| {
            let session_id = row["session_id"].as_str().unwrap_or_default();
            query.contains(&format!("'{session_id}'"))
        })
        .cloned()
        .collect()
}

/// Every uid the v2 handlers can serve, as `(fixture key, REPORTED event_uid)`.
///
/// The two entries differ only for the shared-uid pair, and that is the whole
/// point: `event_uid` is content-addressed over the source coordinates and
/// deliberately EXCLUDES `session_id` (#608), so ONE uid string legitimately
/// belongs to TWO sessions. The mock keys its per-event values by a fixture
/// key so it can describe that shape; the backend reports the same uid twice.
const V2_FIXTURE_EVENTS: [(&str, &str); 10] = [
    ("evt-c-tool-call", "evt-c-tool-call"),
    ("evt-c-tool", "evt-c-tool"),
    ("evt-c-user", "evt-c-user"),
    ("evt-c-42", "evt-c-42"),
    ("evt-c-twin", "evt-c-twin"),
    ("evt-c-duplicate", "evt-c-duplicate"),
    ("evt-a-11", "evt-a-11"),
    ("evt-b-9", "evt-b-9"),
    ("evt-shared-c", SHARED_EVENT_UID),
    ("evt-shared-a", SHARED_EVENT_UID),
];

/// The single content-addressed uid the `shared_event_uid_across_sessions`
/// fixture attributes to both `sess_c` and `sess_a`.
pub(crate) const SHARED_EVENT_UID: &str = "evt-shared";

// Issue #597 B6 — the two BM25 document populations, deliberately DIVERGENT.
//
// The fixture's story: of the 100 rows in `v_live_search_documents`, 20 are
// MV-lag ghosts whose `doc_version` no longer matches the live canonical
// `event_version`, and they carry 1 800 of the 5 000 tokens. The locator join
// excludes them, so the population `df` is counted over is 80 documents /
// 3 200 tokens.
//
// Both `docs` AND `avgdl` differ (100/50.0 vs 80/40.0). A fixture where the
// two coincide is satisfied by an implementation that picks either one, which
// is exactly the failure mode B6 describes.
/// `v_live_search_documents`, published-generation-authorized only.
pub(crate) const DOCUMENT_AUTHORIZED_DOCS: u64 = 100;
pub(crate) const DOCUMENT_AUTHORIZED_TOTAL_DOC_LEN: u64 = 5_000;
/// The same documents, additionally required to carry the live
/// `event_version` — the `live_locator` join every `term_postings` statement
/// scores through.
pub(crate) const LOCATOR_AUTHORIZED_DOCS: u64 = 80;
pub(crate) const LOCATOR_AUTHORIZED_TOTAL_DOC_LEN: u64 = 3_200;

/// The `(fixture key, reported uid)` pairs a statement asked for, plus the
/// saturation window. A statement that names the reported uid gets every
/// session that carries it — which is exactly what a uid-only filter returns
/// from a real index, and what a session-qualified filter must NOT rely on.
fn v2_requested_events(query: &str) -> Vec<(String, String)> {
    let saturation = (0..MOCK_SATURATED_CANDIDATE_WINDOW)
        .map(|idx| (format!("evt-sat-{idx}"), format!("evt-sat-{idx}")));
    V2_FIXTURE_EVENTS
        .iter()
        .map(|(key, uid)| ((*key).to_string(), (*uid).to_string()))
        .chain(saturation)
        .filter(|(_, uid)| query.contains(&format!("'{uid}'")))
        .collect()
}

/// The one MCP-search event fixture BOTH engines describe.
///
/// v1 serves it out of the projected detail statement; v2 assembles the same
/// hit from the ranking pass, the content-free derivation, the dedup-key read
/// and the winner hydration. Keeping ONE fixture is what makes the `SearchPath`
/// matrix a comparison rather than two unrelated stories.
pub(crate) fn mcp_search_detail_row(event_uid: &str) -> serde_json::Value {
    let (session_id, event_time, event_unix_ms, event_order, turn_seq) = match event_uid {
        "evt-a-11" => (
            "sess_a",
            "2026-01-01 10:02:00",
            1_767_261_720_000_i64,
            11_u64,
            1_u32,
        ),
        "evt-b-9" => (
            "sess_b",
            "2026-01-02 10:02:00",
            1_767_348_120_000_i64,
            9_u64,
            1_u32,
        ),
        "evt-c-tool-call" => (
            "sess_c",
            "2026-01-03 10:00:00",
            1_767_434_400_000_i64,
            39_u64,
            2_u32,
        ),
        "evt-c-tool" => (
            "sess_c",
            "2026-01-03 10:00:30",
            1_767_434_430_000_i64,
            40_u64,
            2_u32,
        ),
        "evt-c-user" => (
            "sess_c",
            "2026-01-03 10:01:00",
            1_767_434_460_000_i64,
            41_u64,
            2_u32,
        ),
        "evt-c-duplicate" => (
            "sess_c",
            "2026-01-03 10:02:00.003",
            1_767_434_520_003_i64,
            43_u64,
            2_u32,
        ),
        // A second, genuinely different assistant response in the SAME
        // session, turn, event type and millisecond as `evt-c-42`. The
        // #539 content digest is the only field that distinguishes
        // them, so a hydration path that stops producing one collapses
        // the pair.
        "evt-c-twin" => (
            "sess_c",
            "2026-01-03 10:02:00",
            1_767_434_520_000_i64,
            44_u64,
            2_u32,
        ),
        // ONE content-addressed uid under TWO sessions (#608). The uid
        // string is per-session here only because the mock keys its
        // fixture by uid; the production shape is one uid string, and
        // the property under test is that every read after ranking is
        // keyed by (source_host, session_id, event_uid).
        "evt-shared-a" => (
            "sess_a",
            "2026-01-01 10:02:00",
            1_767_261_720_000_i64,
            11_u64,
            1_u32,
        ),
        "evt-shared-c" => (
            "sess_c",
            "2026-01-03 10:02:00",
            1_767_434_520_000_i64,
            42_u64,
            2_u32,
        ),
        _ => (
            "sess_c",
            "2026-01-03 10:02:00",
            1_767_434_520_000_i64,
            42_u64,
            2_u32,
        ),
    };
    let is_tool_call = event_uid == "evt-c-tool-call";
    let is_tool_response = event_uid == "evt-c-tool";
    let is_user = event_uid == "evt-c-user";
    let is_duplicate = event_uid == "evt-c-duplicate";
    let is_canonical_response = matches!(
        event_uid,
        "evt-c-42" | "evt-c-twin" | "evt-shared-a" | "evt-shared-c"
    );
    let actor_role = if is_tool_response {
        "tool"
    } else if is_user {
        "user"
    } else {
        "assistant"
    };
    let event_type = if is_tool_call {
        "tool_call"
    } else if is_tool_response {
        "tool_response"
    } else if is_user {
        "user_input"
    } else {
        "assistant_response"
    };
    let text = match event_uid {
        "evt-c-tool-call" => "assistant invoked bash for hello world",
        "evt-c-tool" => "cargo test failure output with stack details",
        "evt-c-user" => "user asked about hello world in a prompt",
        "evt-a-11" => "weaker assistant event in session a with extra context",
        "evt-b-9" => "third assistant event with extra context",
        "evt-c-twin" => "a DIFFERENT assistant event in session c, same turn",
        _ => "best assistant event in session c with extra context",
    };
    json!({
        "event_uid": event_uid,
        "session_id": session_id,
        "source_name": "codex",
        "harness": "codex",
        "inference_provider": "openai",
        "endpoint_kind": "generation",
        "event_class": if is_tool_call { "tool_call" } else if is_tool_response { "tool_result" } else if is_duplicate { "event_msg" } else { "message" },
        "payload_type": if is_tool_call { "tool_use" } else if is_tool_response { "tool_result" } else if is_duplicate { "agent_message" } else if is_canonical_response { "message" } else { "text" },
        "actor_role": actor_role,
        "name": if is_tool_call || is_tool_response { "bash" } else { "" },
        "phase": if is_tool_call || is_tool_response || is_duplicate { "completed" } else if is_canonical_response { "final_answer" } else { "" },
        "payload_phase": if is_duplicate || is_canonical_response { "final_answer" } else { "" },
        "source_ref": format!("/tmp/{session_id}.jsonl:1:{event_order}"),
        "doc_len": 19_u32,
        "text_preview": text,
        "text_content": text,
        "text_content_digest": text,
        "payload_json": if is_duplicate || is_canonical_response { "{\"phase\":\"final_answer\"}" } else { "{}" },
        "mcp_event_type": event_type,
        "raw_score": 0.0,
        "matched_terms": 0_u64,
        "event_time": event_time,
        "event_unix_ms": event_unix_ms,
        "event_order": event_order,
        "turn_seq": turn_seq,
        "event_ordinal": if is_tool_call || is_tool_response { 1_u32 } else if is_user { 2_u32 } else if session_id == "sess_c" { 3_u32 } else { 1_u32 },
        "turn_event_count": if session_id == "sess_c" { 3_u64 } else { 1_u64 },
        "turn_completed": if session_id == "sess_c" { 1_u8 } else { 0_u8 },
        "turn_terminal_event_uid": if session_id == "sess_c" { "evt-c-42" } else { "" },
        "call_id": if is_tool_call || is_tool_response { "call-bash-1" } else { "" },
        "item_id": format!("item-{event_uid}"),
        "model": "gpt-5.3-codex",
        "session_started_at_unix_ms": event_unix_ms - 120_000,
        "session_updated_at_unix_ms": event_unix_ms + 480_000,
        "session_title": if session_id == "sess_c" { "Session C summary" } else { "" },
        "session_slug": "",
        "session_summary": if session_id == "sess_c" { "Session C summary" } else { "" },
        "session_completed": if session_id == "sess_c" { 1_u8 } else { 0_u8 }
    })
}

pub(crate) async fn spawn_mock_server(options: MockOptions) -> (String, Arc<MockState>) {
    async fn handler(
        State(state): State<Arc<MockState>>,
        Query(params): Query<HashMap<String, String>>,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, String) {
        if headers.get("content-length").is_none() {
            return (
                StatusCode::LENGTH_REQUIRED,
                "missing content-length".to_string(),
            );
        }

        let query = params
            .get("query")
            .filter(|query| !query.is_empty())
            .cloned()
            .unwrap_or(body);
        // Cancellation KILLs are recorded (the cancel_query tests assert the
        // prefix contract) but answered out-of-band: drop-guard KILLs are
        // spawned, best-effort, and racy by design (issue #600), so they
        // must never consume a scripted response or trip a query barrier.
        if query.trim_start().starts_with("KILL QUERY") {
            state.queries.lock().expect("query lock").push(query);
            state
                .query_ids
                .lock()
                .expect("query id lock")
                .push(params.get("query_id").cloned());
            state
                .request_params
                .lock()
                .expect("request params lock")
                .push(params.clone());
            return (StatusCode::OK, String::new());
        }
        // Publication capture/revalidation is repository infrastructure, not
        // part of the individual query scripts below. Keep the legacy
        // fixtures focused on the operation under test while returning one
        // combined, stable publication-head and append-fence snapshot.
        if query.contains("moraine:publication_snapshot:") {
            state
                .publication_snapshot_queries
                .lock()
                .expect("publication snapshot query lock")
                .push(query);
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "row_kind": 0_u8,
                        "source_host": "",
                        "publication_revision": 1_u64,
                        "head_count": 0_u64,
                        "head_fingerprint": "",
                        "host": "",
                        "control_revision": 0_u64,
                        "cache_epoch": 0_u64,
                        "state": "",
                        "batch_id": "",
                        "publisher_id": "",
                        "manifest_json": "",
                        "insert_only": 0_u8
                    },
                    {
                        "row_kind": 1_u8,
                        "source_host": "",
                        "publication_revision": 0_u64,
                        "head_count": 0_u64,
                        "head_fingerprint": "",
                        "host": "host-a",
                        "control_revision": 1_u64,
                        "cache_epoch": 1_u64,
                        "state": "idle",
                        "batch_id": "",
                        "publisher_id": "publisher-a",
                        "manifest_json": "",
                        "insert_only": 0_u8
                    }
                ])),
            );
        }
        // The issue-599 readiness gate probes `mcp_read_index_state` once per
        // repository before the first session-list page. It is repository
        // infrastructure like the publication snapshot above, so a mock that
        // declares a verdict answers it out of band and keeps the per-test
        // query scripts focused on the operation under test.
        if let Some(ready) = state.options.open_v2_reader_ready {
            if query.contains("mcp_read_index_state") {
                state
                    .readiness_probe_queries
                    .lock()
                    .expect("readiness probe lock")
                    .push(query.clone());
                if query.contains("FROM system.tables") {
                    return (StatusCode::OK, json_each_row(json!([{ "value": "1" }])));
                }
                return (
                    StatusCode::OK,
                    json_each_row(json!([{
                        "state_key": "open_v2",
                        "ready": u8::from(ready),
                        "generation": "1",
                        "cursor": ""
                    }])),
                );
            }
        }
        state
            .queries
            .lock()
            .expect("query lock")
            .push(query.clone());
        state
            .query_ids
            .lock()
            .expect("query id lock")
            .push(params.get("query_id").cloned());
        state
            .request_params
            .lock()
            .expect("request params lock")
            .push(params.clone());

        if let Some(barrier) = state.options.query_barrier.clone() {
            if barrier
                .required
                .iter()
                .all(|required| query.contains(*required))
            {
                barrier.reached.notify_one();
                barrier.release.notified().await;
            }
        }

        let scripted_response = {
            let mut scripted = state
                .scripted_responses
                .lock()
                .expect("scripted response lock");
            scripted.as_mut().map(VecDeque::pop_front)
        };
        if let Some(scripted_response) = scripted_response {
            let Some(response) = scripted_response else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unexpected extra query: {query}"),
                );
            };
            let missing = response
                .required
                .iter()
                .filter(|required| !query.contains(**required))
                .copied()
                .collect::<Vec<_>>();
            let forbidden = response
                .forbidden
                .iter()
                .filter(|forbidden| query.contains(**forbidden))
                .copied()
                .collect::<Vec<_>>();
            if !missing.is_empty() || !forbidden.is_empty() {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "script mismatch; missing={missing:?}; forbidden={forbidden:?}; query={query}"
                    ),
                );
            }
            if let Some(barrier) = response.barrier {
                barrier.reached.notify_one();
                barrier.release.notified().await;
            }
            return (response.status, response.body);
        }

        if query.contains("mcp_open_projection_state")
            && query.contains("WHERE state_key = 'global'")
            && !query.contains("toUInt8(0) AS row_kind")
        {
            return (StatusCode::OK, json_each_row(json!([{ "ready": 1_u8 }])));
        }

        if query.contains("FROM `moraine`.`mcp_open_publication_headers`")
            && query.contains("FINAL")
            && !query.contains("toUInt8(0) AS row_kind")
            && !query.contains("candidate_heads AS")
            && !query.contains("current_headers AS")
        {
            let session_id = query
                .split("s.session_id = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("");
            let rows = session_row(session_id).into_iter().collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("FROM `moraine`.`mcp_open_turns`")
            && query.contains("FINAL")
            && !query.contains("toUInt8(0) AS row_kind")
        {
            let session_id = query
                .split("session_id = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("");
            let turn_seq = query
                .split(" AND turn_seq = ")
                .nth(1)
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse::<u32>().ok());
            let mut rows = turn_rows(session_id, turn_seq);
            if query.contains("'[]' AS event_summaries_json") {
                for row in &mut rows {
                    row["event_summaries_json"] = json!("[]");
                }
            }
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("FROM `moraine`.`mcp_open_events` FINAL")
            && query.contains("SELECT\n  source_host,\n  event_uid,\n  session_id,")
        {
            let event_uid = query
                .split("WHERE event_uid = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("");
            let rows = event_lookup(event_uid).into_iter().collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("FROM `moraine`.`mcp_open_events`")
            && query.contains("FINAL")
            && query.contains("previous_event_uid")
        {
            let event_uid = query
                .split("event_uid = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("");
            let rows = full_event_row(event_uid).into_iter().collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("FROM `moraine`.`mcp_open_events` FINAL")
            && query.contains("event_order IN")
            && !query.contains("toUInt8(0) AS row_kind")
        {
            return (StatusCode::OK, json_each_row(json!(event_ref_rows())));
        }

        if query.starts_with("INSERT INTO `moraine`.`file_attention_project_roots`") {
            return (StatusCode::OK, String::new());
        }

        // --project-only origin-scope gate: the point lookup issued by
        // `session_in_scope`. Sessions whose ID contains "out-of-scope" are
        // outside the scope; everything else is inside it. Only the
        // standalone gate query starts with this prefix — list/search
        // queries embed the same subquery but match their own branches.
        if query.starts_with("SELECT session_id FROM (")
            && query.contains("argMin(cwd, tuple(event_ts, event_uid))")
        {
            let session_id = query
                .split("session_id = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("");
            if session_id.is_empty() || session_id.contains("out-of-scope") {
                return (StatusCode::OK, json_each_row(json!([])));
            }
            return (
                StatusCode::OK,
                json_each_row(json!([{ "session_id": session_id }])),
            );
        }

        if query.contains("argMax(session_id, doc_version) AS session_id")
            && query.contains("WHERE event_uid = 'evt-out-of-scope'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([{ "session_id": "sess-out-of-scope" }])),
            );
        }

        if query.contains("FROM `moraine`.`v_live_tool_io`")
            && query.contains("repo_rel_path = 'crates/foo.rs'")
            && query.contains("project_id = 'project-a'")
            && !query.contains("JSONExtractString(input_json")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess-normalized",
                        "event_uid": "evt-normalized",
                        "tool_call_id": "call-normalized",
                        "harness": "codex",
                        "tool_name": "edit",
                        "tool_phase": "request",
                        "matched_path": "/worktree-a/crates/foo.rs",
                        "match_kind": "path_suffix",
                        "worktree_root": "/worktree-a",
                        "cwd": "/worktree-a",
                        "event_unix_ms": 1769940100000_i64,
                        "event_order": 20_u64,
                        "turn_seq": 2_u32,
                        "input_preview": "{\"not_the_path\":\"hidden\"}",
                        "output_preview": ""
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_live_tool_io`")
            && query.contains("JSONExtractString(input_json, 'path')")
            && query.contains("crates/foo.rs")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess-normalized",
                        "event_uid": "evt-normalized",
                        "tool_call_id": "call-normalized",
                        "harness": "codex",
                        "tool_name": "edit",
                        "tool_phase": "request",
                        "matched_path": "/legacy-would-have-won/crates/foo.rs",
                        "match_kind": "path_suffix",
                        "worktree_root": "/legacy-would-have-won",
                        "cwd": "/legacy-would-have-won",
                        "event_unix_ms": 1769940100000_i64,
                        "event_order": 20_u64,
                        "turn_seq": 2_u32,
                        "input_preview": "{\"path\":\"/legacy-would-have-won/crates/foo.rs\"}",
                        "output_preview": ""
                    },
                    {
                        "session_id": "sess-legacy",
                        "event_uid": "evt-legacy",
                        "tool_call_id": "call-legacy",
                        "harness": "codex",
                        "tool_name": "read",
                        "tool_phase": "request",
                        "matched_path": "crates/foo.rs",
                        "match_kind": "path_suffix",
                        "worktree_root": "/legacy",
                        "cwd": "/legacy",
                        "event_unix_ms": 1769940000000_i64,
                        "event_order": 10_u64,
                        "turn_seq": 1_u32,
                        "input_preview": "{\"path\":\"crates/foo.rs\"}",
                        "output_preview": ""
                    }
                ])),
            );
        }

        // --- issue-599 canonical-first session discovery -------------------
        // Phase A: the content-free directory candidate page. The same three
        // fixture sessions the projected-header page serves, so the two paths
        // can be asserted against identical expectations.
        if query.contains("FROM `moraine`.`mcp_session_directory` AS d")
            && query.contains("AS cand_last_ms")
        {
            // `cand_last_time` is the display form of `cand_last_ms` — the
            // value the directory path both orders by and reports. It matches
            // the projected header fixture exactly, because the aggregate and
            // the hydrated value agree for every session that was not
            // re-ingested; they diverge only when a re-inserted event lowered
            // a display time within one live generation.
            let candidates = json!([
                {
                    "session_id": "sess_c",
                    "cand_last_ms": 1_767_435_000_000_i64,
                    "cand_last_time": "2026-01-03 10:10:00"
                },
                {
                    "session_id": "sess_b",
                    "cand_last_ms": 1_767_348_600_000_i64,
                    "cand_last_time": "2026-01-02 10:10:00"
                },
                {
                    "session_id": "sess_a",
                    "cand_last_ms": 1_767_262_200_000_i64,
                    "cand_last_time": "2026-01-01 10:10:00"
                }
            ]);
            let rows = candidates
                .as_array()
                .expect("candidate rows")
                .iter()
                .filter(|row| {
                    let session_id = row["session_id"].as_str().unwrap_or_default();
                    // Honor whichever keyset the page carries.
                    if query.contains("session_id < 'sess_b'") {
                        return session_id < "sess_b";
                    }
                    if query.contains("session_id > 'sess_b'") {
                        return session_id > "sess_b";
                    }
                    true
                })
                .cloned()
                .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        // ------------------------------------------------------------------
        // Issue #597 v2: the bounded canonical MCP search engine. Seven shapes,
        // none of which names an `mcp_open_*` relation — that absence is what
        // `search_mcp_events_v2_never_touches_the_projection` asserts against
        // the statements this handler actually recorded.
        //
        // Every shape below reads its values out of `mcp_search_detail_row`,
        // the SAME fixture the v1 detail statement serves, so a `SearchPath`
        // matrix assertion compares two engines describing one corpus.
        // ------------------------------------------------------------------

        // Phase 0: session-scope existence as a directory point read.
        if query.contains("AS scope_exists")
            && query.contains("FROM `moraine`.`mcp_session_directory` AS d")
        {
            // A scoped repository must filter this read by the session's
            // `argMinIfMerge(origin_cwd_state)`. `sess_a` is the fixture's
            // out-of-scope session, so a statement that carries the root
            // predicate answers 0 for it and a statement that dropped the
            // predicate answers 1 — which is exactly the disclosure.
            let scoped = query.contains("argMinIfMerge(d.origin_cwd_state)");
            let in_scope = !scoped || (query.contains("'/repo'") && !query.contains("'sess_a'"));
            let exists = u8::from(!query.contains("'sess_missing'") && in_scope);
            return (
                StatusCode::OK,
                json_each_row(json!([{ "scope_exists": exists }])),
            );
        }

        // Phase 0: the turn's live event uid set.
        if query.contains("WINDOW turn_window AS") && query.contains("\nWHERE turn_seq = ") {
            let session_id = query
                .split("WHERE n.session_id = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or("")
                .to_string();
            let turn_seq = query
                .rsplit_once("\nWHERE turn_seq = ")
                .and_then(|(_, tail)| tail.split(['\n', ' ']).next())
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(0);
            // A scoped statement carries the exact `argMinIf(n.cwd, …)` gate.
            // `sess_a` is the fixture's out-of-scope session.
            if query.contains("AS session_origin_cwd") && session_id == "sess_a" {
                return (StatusCode::OK, json_each_row(json!([])));
            }
            if state.options.turn_scope_uid_overflow {
                // Above `MAX_TURN_SCOPE_UIDS` the ranking pass drops the uid
                // literal set and the turn is re-checked exactly in Phase 4.
                let rows = (0..=4096)
                    .map(|idx| json!({ "event_uid": format!("evt-turn-{idx}") }))
                    .collect::<Vec<_>>();
                return (StatusCode::OK, json_each_row(json!(rows)));
            }
            let rows = [
                "evt-c-tool-call",
                "evt-c-tool",
                "evt-c-user",
                "evt-c-42",
                "evt-c-duplicate",
                "evt-a-11",
                "evt-b-9",
            ]
            .into_iter()
            .filter(|uid| {
                let row = mcp_search_detail_row(uid);
                row["session_id"] == session_id.as_str()
                    && row["turn_seq"].as_u64() == Some(u64::from(turn_seq))
            })
            .map(|uid| json!({ "event_uid": uid }))
            .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        // Phase 1: the one bounded ranking pass.
        if query.contains("term_postings AS (") && query.contains("AS post_version") {
            let candidate_as =
                |fixture_key: &str, reported_uid: &str, raw_score: f64, matched_terms: u64| {
                    let detail = mcp_search_detail_row(fixture_key);
                    let session_id = detail["session_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    json!({
                        "event_uid": reported_uid,
                        "source_host": "",
                        "session_id": session_id,
                        "post_version": 7_u64,
                        "source_file": format!("/tmp/{session_id}.jsonl"),
                        "source_generation": 1_u32,
                        "source_line_no": detail["event_order"],
                        "sort_time_ms": detail["event_unix_ms"],
                        "harness": detail["harness"],
                        "source_name": detail["source_name"],
                        "event_class": detail["event_class"],
                        "payload_type": detail["payload_type"],
                        "actor_role": detail["actor_role"],
                        "name": detail["name"],
                        "phase": detail["phase"],
                        "doc_len": 19_u32,
                        "raw_score": raw_score,
                        "matched_terms": matched_terms
                    })
                };
            let candidate = |event_uid: &str, raw_score: f64, matched_terms: u64| {
                candidate_as(event_uid, event_uid, raw_score, matched_terms)
            };
            let filter_clause = query
                .split_once("\nFROM term_postings AS p\nWHERE ")
                .and_then(|(_, tail)| tail.split_once("\nGROUP BY"))
                .map(|(filter, _)| filter)
                .unwrap_or(query.as_str());
            let includes_user = filter_clause.contains("lowerUTF8(p.actor_role) = 'user'");
            let includes_assistant =
                filter_clause.contains("lowerUTF8(p.actor_role) = 'assistant'");
            let includes_tool_call = filter_clause.contains("p.event_class = 'tool_call'");
            let includes_tool_response = filter_clause.contains("p.event_class = 'tool_result'");
            let rows = if state.options.shared_event_uid_across_sessions {
                // ONE content-addressed uid, TWO sessions (#608). Both rows are
                // real: `event_uid` is addressed over the source coordinates and
                // excludes `session_id`, so a physical line ingest attributed to
                // two sessions produces exactly this.
                vec![
                    candidate_as("evt-shared-c", SHARED_EVENT_UID, 12.5, 2),
                    candidate_as("evt-shared-a", SHARED_EVENT_UID, 7.0, 1),
                ]
            } else if state.options.two_distinct_events_in_one_turn {
                // Same session, same turn, same event type, same timestamp —
                // DIFFERENT content. The #539 digest is the only thing telling
                // them apart, so a hydration path that stops producing one
                // collapses them.
                vec![
                    candidate("evt-c-42", 12.5, 2),
                    candidate("evt-c-twin", 12.5, 2),
                ]
            } else if query.contains("p.session_id = 'sess_c'")
                && !query.contains("p.event_uid IN [")
            {
                // Turn scope above the uid cap: recall is session-only and the
                // turn is re-checked exactly in Phase 4.
                vec![candidate("evt-c-42", 12.5, 2)]
            } else if query.contains("p.session_id = 'sess_a'") {
                vec![candidate("evt-a-11", 7.0, 1)]
            } else if includes_user
                && !includes_assistant
                && !includes_tool_call
                && !includes_tool_response
            {
                vec![candidate("evt-c-user", 11.0, 2)]
            } else if includes_assistant
                && !includes_user
                && !includes_tool_call
                && !includes_tool_response
            {
                vec![candidate("evt-c-42", 12.5, 2)]
            } else if includes_tool_call
                && !includes_user
                && !includes_assistant
                && !includes_tool_response
            {
                vec![candidate("evt-c-tool-call", 13.5, 2)]
            } else if includes_tool_response
                && !includes_user
                && !includes_assistant
                && !includes_tool_call
            {
                vec![candidate("evt-c-tool", 13.0, 2)]
            } else if includes_user
                && includes_tool_call
                && !includes_assistant
                && !includes_tool_response
            {
                vec![
                    candidate("evt-c-tool-call", 13.5, 2),
                    candidate("evt-c-user", 11.0, 2),
                ]
            } else if state.options.saturate_candidate_window {
                (0..MOCK_SATURATED_CANDIDATE_WINDOW)
                    .map(|idx| Box::leak(format!("evt-sat-{idx}").into_boxed_str()) as &str)
                    .map(|uid| candidate(uid, 12.0, 2))
                    .collect()
            } else if query.contains("LIMIT 9") {
                vec![
                    candidate("evt-c-42", 12.5, 2),
                    candidate("evt-c-duplicate", 12.0, 2),
                    candidate("evt-a-11", 7.0, 1),
                    candidate("evt-b-9", 6.0, 1),
                ]
            } else {
                vec![
                    candidate("evt-c-42", 12.5, 2),
                    candidate("evt-a-11", 7.0, 1),
                ]
            };
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        // Phase 2: content-free candidate derivation over the candidate
        // sessions, carrying the second independent version check and the exact
        // project-scope input.
        if query.contains("ordinaled AS (") && query.contains("session_cwd AS (") {
            let derived = |fixture_key: &str, reported_uid: &str| {
                let detail = mcp_search_detail_row(fixture_key);
                let session_id = detail["session_id"].as_str().unwrap_or_default();
                let stale = state.options.stale_navigation_version_for_second_candidate
                    && fixture_key == "evt-a-11";
                let out_of_scope = state.options.out_of_scope_cwd_for_second_candidate
                    && fixture_key == "evt-a-11";
                json!({
                    "session_id": session_id,
                    "event_uid": reported_uid,
                    "source_host": "",
                    // The locator authorized every candidate at version 7; a
                    // navigation row at a different version is a proven
                    // mid-flight write and the candidate must be dropped.
                    "event_version": if stale { 8_u64 } else { 7_u64 },
                    "display_time": detail["event_time"],
                    "display_time_ms": detail["event_unix_ms"],
                    "event_ts_ms": detail["event_unix_ms"],
                    "event_order": detail["event_order"],
                    "turn_seq": detail["turn_seq"],
                    "event_ordinal": detail["event_ordinal"],
                    "origin_cwd": if out_of_scope { "/elsewhere" } else { "/repo" }
                })
            };
            // A uid-only filter cannot distinguish sessions, so it gets EVERY
            // session that carries the uid — which is precisely what makes a
            // uid-only derivation key mis-attribute the winner.
            let rows = v2_requested_events(&query)
                .into_iter()
                .map(|(key, uid)| derived(&key, &uid))
                .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        // Phase 3: the two fixed-width dedup inputs. `search_documents` is
        // `ORDER BY (event_uid)`, so this relation is per DOCUMENT: one row per
        // uid, whatever session the uid is attributed to.
        if query.contains("any(d.text_digest) AS text_content_digest") {
            let mut seen = Vec::<String>::new();
            let rows = v2_requested_events(&query)
                .into_iter()
                .filter(|(key, _)| {
                    !(state.options.omit_dedup_key_for_second_candidate && key == "evt-a-11")
                })
                // ONE row per document: the digest is a property of the
                // content, so a uid attributed to two sessions still has a
                // single document row.
                .filter(|(_, uid)| {
                    let fresh = !seen.contains(uid);
                    if fresh {
                        seen.push(uid.clone());
                    }
                    fresh
                })
                .map(|(key, uid)| {
                    let detail = mcp_search_detail_row(&key);
                    json!({
                        "source_host": "",
                        "event_uid": uid,
                        "text_content_digest": detail["text_content_digest"],
                        "payload_phase": detail["payload_phase"]
                    })
                })
                .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        // Phase 5: per-turn scalars for the winner sessions.
        if query.contains("AS turn_terminal_event_uid")
            && query.contains("GROUP BY session_id, turn_seq")
        {
            let turns = json!([
                {
                    "session_id": "sess_c",
                    "turn_seq": 2_u32,
                    "turn_event_count": 3_u64,
                    "turn_completed": 1_u8,
                    "turn_terminal_event_uid": "evt-c-42"
                },
                {
                    // sess_c's LAST turn, and it is NOT complete. The hit above
                    // sits in turn 2, which IS complete — so the hit's own turn
                    // flag and the session's flag disagree, which is the only
                    // fixture shape that can catch a reader that confuses them.
                    "session_id": "sess_c",
                    "turn_seq": 3_u32,
                    "turn_event_count": 2_u64,
                    "turn_completed": 0_u8,
                    "turn_terminal_event_uid": ""
                },
                {
                    "session_id": "sess_a",
                    "turn_seq": 1_u32,
                    "turn_event_count": 1_u64,
                    "turn_completed": 0_u8,
                    "turn_terminal_event_uid": ""
                },
                {
                    "session_id": "sess_b",
                    "turn_seq": 1_u32,
                    "turn_event_count": 1_u64,
                    "turn_completed": 0_u8,
                    "turn_terminal_event_uid": ""
                }
            ]);
            return (
                StatusCode::OK,
                json_each_row(json!(rows_for_requested_sessions(&query, &turns))),
            );
        }

        // Phase 5: the K-uid wide read — the ONLY v2 statement that names a
        // wide column, and the one that retires the uid-only `models` CTE.
        if query.contains("e.model AS model") && query.contains("AS text_preview") {
            // As with the derivation, a uid-only filter gets every session that
            // carries the uid.
            let rows = v2_requested_events(&query)
                .into_iter()
                .map(|(key, uid)| {
                    let detail = mcp_search_detail_row(&key);
                    json!({
                        "session_id": detail["session_id"],
                        "event_uid": uid,
                        "source_host": "",
                        "inference_provider": detail["inference_provider"],
                        "endpoint_kind": detail["endpoint_kind"],
                        "call_id": detail["call_id"],
                        "item_id": detail["item_id"],
                        "model": detail["model"],
                        "source_ref": detail["source_ref"],
                        "text_preview": detail["text_preview"],
                        "text_content": detail["text_content"],
                        "payload_json": detail["payload_json"]
                    })
                })
                .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        // Phase B1: batched totals.
        if query.contains("AS counter_user_messages") && query.contains("GROUP BY nav.session_id") {
            let totals = json!([
                    {
                        "session_id": "sess_c",
                        "total_events": 30_u64,
                        "tool_calls": 6_u64,
                        "max_override": 0_u32,
                        "counter_user_messages": 3_u64,
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1_767_434_400_000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1_767_435_000_000_i64,
                        "origin_cwd": "/repo",
            "source": "codex",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "omp_dispatch_title": "",
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_b",
                        "total_events": 22_u64,
                        "tool_calls": 4_u64,
                        "max_override": 0_u32,
                        "counter_user_messages": 2_u64,
                        "first_event_time": "2026-01-02 10:00:00",
                        "first_event_unix_ms": 1_767_348_000_000_i64,
                        "last_event_time": "2026-01-02 10:10:00",
                        "last_event_unix_ms": 1_767_348_600_000_i64,
                        "origin_cwd": "/repo",
            "source": "codex",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "omp_dispatch_title": "",
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_a",
                        "total_events": 20_u64,
                        "tool_calls": 2_u64,
                        "max_override": 0_u32,
                        "counter_user_messages": 2_u64,
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1_767_261_600_000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1_767_262_200_000_i64,
                        "origin_cwd": "/repo",
            "source": "codex",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "omp_dispatch_title": "",
                        "mode": "web_search"
                    }
                ]);
            return (
                StatusCode::OK,
                json_each_row(json!(rows_for_requested_sessions(&query, &totals))),
            );
        }

        // Phase B2: batched metadata, bounded to metadata-bearing rows.
        if query.contains("n.is_metadata_bearing = 1")
            && query.contains("e.payload_json AS payload_json")
            && query.contains("WHERE e.session_id IN [")
        {
            let metadata = json!([
                {
                    "session_id": "sess_c",
                    "event_ts": "2026-01-03 10:00:00",
                    "event_uid": "evt-c-meta",
                    "event_kind": "session_meta",
                    "payload_json": "{\"title\":\"Session C title\",\"summary\":\"Session C summary\",\"slug\":\"project-c\"}"
                },
                {
                    "session_id": "sess_b",
                    "event_ts": "2026-01-02 10:00:00",
                    "event_uid": "evt-b-meta",
                    "event_kind": "session_meta",
                    "payload_json": "{\"title\":\"Session B title\",\"summary\":\"Session B summary\",\"slug\":\"project-b\"}"
                }
            ]);
            return (
                StatusCode::OK,
                json_each_row(json!(rows_for_requested_sessions(&query, &metadata))),
            );
        }

        // Phase B3: batched terminal state.
        if query.contains("argMax(turn_completed, turn_seq)")
            && query.contains("GROUP BY session_id, turn_seq")
        {
            let terminal = json!([
                { "session_id": "sess_c", "completed": 1_u8 },
                { "session_id": "sess_b", "completed": 1_u8 },
                { "session_id": "sess_a", "completed": 0_u8 }
            ]);
            return (
                StatusCode::OK,
                json_each_row(json!(rows_for_requested_sessions(&query, &terminal))),
            );
        }

        if query.contains("FROM `moraine`.`mcp_open_publication_headers` AS h FINAL")
            && query.contains("current_headers AS")
            && query.contains("toUInt8(s.completed) AS completed")
        {
            if query.contains("s.session_id < 'sess_b'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                                    {
                                        "session_id": "sess_a",
                                        "first_event_time": "2026-01-01 10:00:00",
                                        "first_event_unix_ms": 1767261600000_i64,
                                        "last_event_time": "2026-01-01 10:10:00",
                                        "last_event_unix_ms": 1767262200000_i64,
                                        "total_turns": 2,
                                        "total_events": 20,
                                        "mode": "web_search",
                                        "completed": 0_u8,
                                        "title": "",
                                        "origin_cwd": "/repo",
                    "source": "codex",
                                        "harness": "codex",
                                        "inference_provider": "openai",
                                        "tool_calls": 2,
                                        "session_slug": "",
                                        "session_summary": ""
                                    }
                                ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                            {
                                "session_id": "sess_c",
                                "first_event_time": "2026-01-03 10:00:00",
                                "first_event_unix_ms": 1767434400000_i64,
                                "last_event_time": "2026-01-03 10:10:00",
                                "last_event_unix_ms": 1767435000000_i64,
                                "total_turns": 3,
                                "total_events": 30,
                                "mode": "web_search",
                                "completed": 1_u8,
                                "title": "Session C title",
                                "origin_cwd": "/repo",
                "source": "codex",
                                "harness": "codex",
                                "inference_provider": "openai",
                                "tool_calls": 6,
                                "originator": "Codex Desktop",
                                "origin_cwd": "/work/acme-secret-merger",
                                "project": "acme-secret-merger",
                                "session_slug": "project-c",
                                "session_summary": "Session C summary"
                            },
                            {
                                "session_id": "sess_b",
                                "first_event_time": "2026-01-02 10:00:00",
                                "first_event_unix_ms": 1767348000000_i64,
                                "last_event_time": "2026-01-02 10:10:00",
                                "last_event_unix_ms": 1767348600000_i64,
                                "total_turns": 2,
                                "total_events": 22,
                                "mode": "web_search",
                                "completed": 1_u8,
                                "title": "Session B title",
                                "origin_cwd": "/repo",
                "source": "codex",
                                "harness": "codex",
                                "inference_provider": "openai",
                                "tool_calls": 4,
                                "session_slug": "project-b",
                                "session_summary": "Session B summary"
                            },
                            {
                                "session_id": "sess_a",
                                "first_event_time": "2026-01-01 10:00:00",
                                "first_event_unix_ms": 1767261600000_i64,
                                "last_event_time": "2026-01-01 10:10:00",
                                "last_event_unix_ms": 1767262200000_i64,
                                "total_turns": 2,
                                "total_events": 20,
                                "mode": "web_search",
                                "completed": 0_u8,
                                "title": "",
                                "origin_cwd": "/repo",
                "source": "codex",
                                "harness": "codex",
                                "inference_provider": "openai",
                                "tool_calls": 2,
                                "session_slug": "",
                                "session_summary": ""
                            }
                        ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("ORDER BY s.last_event_time DESC")
        {
            if query.contains("s.session_id < 'sess_b'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_a",
                            "first_event_time": "2026-01-01 10:00:00",
                            "first_event_unix_ms": 1767261600000_i64,
                            "last_event_time": "2026-01-01 10:10:00",
                            "last_event_unix_ms": 1767262200000_i64,
                            "total_turns": 2,
                            "total_events": 20,
                            "user_messages": 4,
                            "assistant_messages": 4,
                            "tool_calls": 2,
                            "tool_results": 2,
                            "mode": "web_search"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3,
                        "total_events": 30,
                        "user_messages": 6,
                        "assistant_messages": 6,
                        "tool_calls": 3,
                        "tool_results": 3,
                        "mode": "web_search",
                        "session_slug": "project-c",
                        "session_summary": "Session C summary"
                    },
                    {
                        "session_id": "sess_b",
                        "first_event_time": "2026-01-02 10:00:00",
                        "first_event_unix_ms": 1767348000000_i64,
                        "last_event_time": "2026-01-02 10:10:00",
                        "last_event_unix_ms": 1767348600000_i64,
                        "total_turns": 2,
                        "total_events": 22,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "total_turns": 2,
                        "total_events": 20,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("argMin(event_uid, tuple(event_time, event_order, event_uid))")
            && query.contains("argMax(actor_role, tuple(event_time, event_order, event_uid))")
            && query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE s.session_id =")
        {
            if query.contains("WHERE s.session_id = 'sess-missing'") {
                return (StatusCode::OK, json_each_row(json!([])));
            }

            if query.contains("WHERE s.session_id = 'sess-empty'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess-empty",
                            "first_event_time": "2026-01-04 09:00:00",
                            "first_event_unix_ms": 1767517200000_i64,
                            "last_event_time": "2026-01-04 09:01:00",
                            "last_event_unix_ms": 1767517260000_i64,
                            "total_turns": 1_u32,
                            "total_events": 2_u64,
                            "user_messages": 1_u64,
                            "assistant_messages": 1_u64,
                            "tool_calls": 0_u64,
                            "tool_results": 0_u64,
                            "mode": "chat",
                            "first_event_uid": "",
                            "last_event_uid": "",
                            "last_actor_role": ""
                        }
                    ])),
                );
            }

            if query.contains("WHERE s.session_id = 'sess-open'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess-open",
                            "first_event_time": "2026-02-01 10:01:00",
                            "first_event_unix_ms": 1769940060000_i64,
                            "last_event_time": "2026-02-01 10:02:30",
                            "last_event_unix_ms": 1769940150000_i64,
                            "total_turns": 2_u32,
                            "total_events": 8_u64,
                            "user_messages": 2_u64,
                            "assistant_messages": 2_u64,
                            "tool_calls": 1_u64,
                            "tool_results": 1_u64,
                            "mode": "tool_calling",
                            "first_event_uid": "evt-open-1",
                            "last_event_uid": "evt-open-8",
                            "last_actor_role": "system",
                            "title": "Open model session",
                            "source": "codex-source",
                            "harness": "codex",
                            "inference_provider": "openai",
                            "session_slug": "open-model-session",
                            "session_summary": "Session summary from metadata"
                        }
                    ])),
                );
            }

            if query.contains("WHERE s.session_id = 'sess-event'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess-event",
                            "first_event_time": "2026-02-01 10:00:01",
                            "first_event_unix_ms": 1769940001000_i64,
                            "last_event_time": "2026-02-01 10:00:04",
                            "last_event_unix_ms": 1769940004000_i64,
                            "total_turns": 2_u32,
                            "total_events": 4_u64,
                            "user_messages": 1_u64,
                            "assistant_messages": 1_u64,
                            "tool_calls": 0_u64,
                            "tool_results": 0_u64,
                            "mode": "chat",
                            "first_event_uid": "evt-event-1",
                            "last_event_uid": "evt-event-4",
                            "last_actor_role": "assistant"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3_u32,
                        "total_events": 30_u64,
                        "user_messages": 6_u64,
                        "assistant_messages": 6_u64,
                        "tool_calls": 3_u64,
                        "tool_results": 3_u64,
                        "mode": "web_search",
                        "first_event_uid": "evt-c-1",
                        "last_event_uid": "evt-c-42",
                        "last_actor_role": "assistant"
                    }
                ])),
            );
        }

        if query.contains("FROM (SELECT * FROM `moraine`.`events` FINAL)")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("AS title")
            && query.contains("AS session_summary")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "title": "Open model session",
                        "source": "codex-source",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "session_slug": "open-model-session",
                        "session_summary": "Session summary from metadata"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS s")
            && query.contains("ORDER BY s.last_event_time ASC")
        {
            if query.contains("s.session_id > 'sess_b'") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_c",
                            "first_event_time": "2026-01-03 10:00:00",
                            "first_event_unix_ms": 1767434400000_i64,
                            "last_event_time": "2026-01-03 10:10:00",
                            "last_event_unix_ms": 1767435000000_i64,
                            "total_turns": 3,
                            "total_events": 30,
                            "user_messages": 6,
                            "assistant_messages": 6,
                            "tool_calls": 3,
                            "tool_results": 3,
                            "mode": "web_search"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "total_turns": 2,
                        "total_events": 20,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_b",
                        "first_event_time": "2026-01-02 10:00:00",
                        "first_event_unix_ms": 1767348000000_i64,
                        "last_event_time": "2026-01-02 10:10:00",
                        "last_event_unix_ms": 1767348600000_i64,
                        "total_turns": 2,
                        "total_events": 22,
                        "user_messages": 4,
                        "assistant_messages": 4,
                        "tool_calls": 2,
                        "tool_results": 2,
                        "mode": "web_search"
                    },
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "total_turns": 3,
                        "total_events": 30,
                        "user_messages": 6,
                        "assistant_messages": 6,
                        "tool_calls": 3,
                        "tool_results": 3,
                        "mode": "web_search"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_session_summary` AS ss")
            && query.contains("WHERE session_id IN")
            && query.contains("toString(ss.first_event_time) AS first_event_time")
            && query.contains("toString(ss.last_event_time) AS last_event_time")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "last_event_time": "2026-01-03 10:10:00",
                        "first_event_unix_ms": 1_767_434_400_000_i64,
                        "last_event_unix_ms": 1_767_435_000_000_i64
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "last_event_time": "2026-01-01 10:10:00",
                        "first_event_unix_ms": 1_767_261_600_000_i64,
                        "last_event_unix_ms": 1_767_262_200_000_i64
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`search_corpus_stats`")
            && !query.contains("toUInt8(0) AS row_kind")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "docs": DOCUMENT_AUTHORIZED_DOCS,
                        "total_doc_len": DOCUMENT_AUTHORIZED_TOTAL_DOC_LEN
                    }
                ])),
            );
        }

        // Issue #597 B6: the locator-authorized population, and it must NOT
        // agree with `search_corpus_stats` — a fixture where the two coincide
        // proves nothing and passes against an implementation that picks the
        // wrong one. See [`LOCATOR_AUTHORIZED_DOCS`].
        if query.contains("FROM `moraine`.`mcp_event_locator` AS l FINAL")
            && query.contains("AS total_doc_len")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "docs": LOCATOR_AUTHORIZED_DOCS,
                        "total_doc_len": LOCATOR_AUTHORIZED_TOTAL_DOC_LEN
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`search_term_stats`") {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    { "term": "hello", "df": 20_u64 },
                    { "term": "world", "df": 10_u64 }
                ])),
            );
        }

        if query.contains("toUInt8(0) AS row_kind") && query.contains("term_postings AS (") {
            let candidate_query_count = state
                .queries
                .lock()
                .expect("query lock")
                .iter()
                .filter(|candidate_query| {
                    candidate_query.contains("toUInt8(0) AS row_kind")
                        && candidate_query.contains("term_postings AS (")
                })
                .count();
            if candidate_query_count == 2 && query.contains("search_corpus_stats") {
                if let Some(barrier) = state.options.repeated_corpus_stats_barrier.clone() {
                    barrier.reached.notify_one();
                    barrier.release.notified().await;
                }
            }
            let projection_clean = u8::from(
                !state.options.dirty_projection_on_first_candidate || candidate_query_count != 1,
            );
            let candidate = |event_uid: &str,
                             raw_score: f64,
                             matched_terms: u64,
                             event_unix_ms: i64| {
                json!({
                    "row_kind": 0_u8,
                    "event_uid": event_uid,
                    "session_id": if event_uid.starts_with("evt-a") { "sess_a" } else if event_uid.starts_with("evt-b") { "sess_b" } else { "sess_c" },
                    "slot": 0_u8,
                    "generation": 1_u64,
                    "raw_score": raw_score,
                    "matched_terms": matched_terms,
                    "event_unix_ms": event_unix_ms,
                    "docs": 100_u64,
                    "total_doc_len": 5000_u64,
                    "scope_exists": 1_u8,
                    "projection_ready": 1_u8,
                    "projection_clean": projection_clean
                })
            };
            // Scope existence, the v1 way: `scope_state_sql` is inlined into
            // the candidate statement as a scalar. The verdict has to match the
            // v2 point read's, or the `SearchPath` matrix compares two
            // different corpora.
            let scope_session = query
                .split("WHERE scope_s.session_id = '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or_default()
                .to_string();
            let scope_turn = query
                .split("AND scope_t.turn_seq = ")
                .nth(1)
                .and_then(|rest| rest.split(['\n', ' ', ')']).next())
                .and_then(|value| value.parse::<u32>().ok());
            let scope_exists = u8::from(match (scope_session.as_str(), scope_turn) {
                ("", _) => true,
                ("sess_missing", _) => false,
                (session_id, Some(turn_seq)) => [
                    "evt-c-tool-call",
                    "evt-c-tool",
                    "evt-c-user",
                    "evt-c-42",
                    "evt-c-duplicate",
                    "evt-a-11",
                    "evt-b-9",
                ]
                .into_iter()
                .any(|uid| {
                    let row = mcp_search_detail_row(uid);
                    row["session_id"] == session_id
                        && row["turn_seq"].as_u64() == Some(u64::from(turn_seq))
                }),
                _ => true,
            });
            let metadata = json!({
                "row_kind": 1_u8,
                "event_uid": "",
                "session_id": "",
                "slot": 0_u8,
                "generation": 0_u64,
                "raw_score": 0.0,
                "matched_terms": 0_u64,
                "event_unix_ms": 0_i64,
                "docs": 100_u64,
                "total_doc_len": 5000_u64,
                "scope_exists": scope_exists,
                "projection_ready": 1_u8,
                "projection_clean": projection_clean
            });
            let filter_clause = query
                .split_once("WHERE ")
                .and_then(|(_, tail)| tail.split_once("GROUP BY p.doc_id"))
                .map(|(filter, _)| filter)
                .unwrap_or(query.as_str());
            let includes_user = filter_clause.contains("lowerUTF8(p.actor_role) = 'user'");
            let includes_assistant =
                filter_clause.contains("lowerUTF8(p.actor_role) = 'assistant'");
            let includes_tool_call = filter_clause.contains("p.event_class = 'tool_call'");
            let includes_tool_response = filter_clause.contains("p.event_class = 'tool_result'");
            let mut rows = if query.contains("e.session_id = 'sess_c' AND e.turn_seq = 2") {
                vec![candidate("evt-c-tool", 13.0, 2, 1_767_434_430_000)]
            } else if query.contains("p.session_id = 'sess_a'") {
                vec![candidate("evt-a-11", 7.0, 1, 1_767_261_720_000)]
            } else if includes_user
                && !includes_assistant
                && !includes_tool_call
                && !includes_tool_response
            {
                vec![candidate("evt-c-user", 11.0, 2, 1_767_434_460_000)]
            } else if includes_assistant
                && !includes_user
                && !includes_tool_call
                && !includes_tool_response
            {
                vec![candidate("evt-c-42", 12.5, 2, 1_767_434_520_000)]
            } else if includes_tool_call
                && !includes_user
                && !includes_assistant
                && !includes_tool_response
            {
                vec![candidate("evt-c-tool-call", 13.5, 2, 1_767_434_400_000)]
            } else if includes_tool_response
                && !includes_user
                && !includes_assistant
                && !includes_tool_call
            {
                vec![candidate("evt-c-tool", 13.0, 2, 1_767_434_430_000)]
            } else if includes_user
                && includes_tool_call
                && !includes_assistant
                && !includes_tool_response
            {
                vec![
                    candidate("evt-c-tool-call", 13.5, 2, 1_767_434_400_000),
                    candidate("evt-c-user", 11.0, 2, 1_767_434_460_000),
                ]
            } else if state.options.saturate_candidate_window {
                // A SATURATED window (exactly `candidate_fetch_size` rows) whose
                // members all collapse into one another. The retired code looped
                // 16 times over this and then failed with
                // `backend("duplicate scan budget exhausted")`; the bounded pass
                // returns the surviving hit plus the incompleteness marker.
                (0..MOCK_SATURATED_CANDIDATE_WINDOW)
                    .map(|idx| Box::leak(format!("evt-sat-{idx}").into_boxed_str()) as &str)
                    .map(|uid| candidate(uid, 12.0, 2, 1_767_434_520_000))
                    .collect()
            } else if query.contains("LIMIT 9") {
                // n_hits = 2 -> unique_fetch_limit = 3 -> one window of 9.
                // Four rows, one of them the #539 duplicate: dedup leaves three
                // unique hits, which is more than the two requested, so the page
                // reports `truncated` WITHOUT a second ranking pass.
                vec![
                    candidate("evt-c-42", 12.5, 2, 1_767_434_520_000),
                    candidate("evt-c-duplicate", 12.0, 2, 1_767_434_520_003),
                    candidate("evt-a-11", 7.0, 1, 1_767_261_720_000),
                    candidate("evt-b-9", 6.0, 1, 1_767_348_120_000),
                ]
            } else {
                vec![
                    candidate("evt-c-42", 12.5, 2, 1_767_434_520_000),
                    candidate("evt-a-11", 7.0, 1, 1_767_261_720_000),
                ]
            };
            rows.push(metadata);
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("documents AS (")
            && query.contains("AS session_started_at_unix_ms")
            && query.contains("FROM documents")
            && query.contains("mcp_open_events")
        {
            let detail = mcp_search_detail_row;
            // `evt-sat-N` are the saturated-window uids. They take `detail`'s
            // fallback shape, so they share session, turn, event type, digest
            // and timestamp — i.e. they are #539-equivalent and collapse to one.
            let saturation_uids = (0..MOCK_SATURATED_CANDIDATE_WINDOW)
                .map(|idx| format!("evt-sat-{idx}"))
                .collect::<Vec<_>>();
            let event_uids = [
                "evt-c-tool-call",
                "evt-c-tool",
                "evt-c-user",
                "evt-c-42",
                "evt-c-duplicate",
                "evt-a-11",
                "evt-b-9",
            ]
            .into_iter()
            .map(str::to_string)
            .chain(saturation_uids)
            .filter(|event_uid| query.contains(&format!("'{event_uid}'")))
            .skip(usize::from(state.options.omit_first_mcp_detail_row))
            .map(|event_uid| detail(event_uid.as_str()))
            .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(event_uids)));
        }

        if query.contains("AS mcp_event_type")
            && query.contains("AS raw_score")
            && query.contains("FROM `moraine`.`v_live_search_postings` AS p")
        {
            let assistant_row = json!({
                "event_uid": "evt-c-42",
                "session_id": "sess_c",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_c.jsonl:1:42",
                "doc_len": 19_u32,
                "text_preview": "best assistant event in session c",
                "text_content": "best assistant event in session c with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-c\"}",
                "mcp_event_type": "assistant_response",
                "raw_score": 12.5,
                "matched_terms": 2_u64,
                "event_time": "2026-01-03 10:02:00",
                "event_unix_ms": 1767434520000_i64,
                "event_order": 42_u64,
                "turn_seq": 2_u32
            });
            let user_row = json!({
                "event_uid": "evt-c-user",
                "session_id": "sess_c",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "user",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_c.jsonl:1:41",
                "doc_len": 15_u32,
                "text_preview": "user asked about hello world",
                "text_content": "user asked about hello world in a prompt",
                "payload_json": "{\"type\":\"message\",\"role\":\"user\"}",
                "mcp_event_type": "user_input",
                "raw_score": 11.0,
                "matched_terms": 2_u64,
                "event_time": "2026-01-03 10:01:00",
                "event_unix_ms": 1767434460000_i64,
                "event_order": 41_u64,
                "turn_seq": 2_u32
            });
            let tool_row = json!({
                "event_uid": "evt-c-tool",
                "session_id": "sess_c",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "tool_result",
                "payload_type": "tool_result",
                "actor_role": "tool",
                "name": "bash",
                "phase": "completed",
                "source_ref": "/tmp/sess_c.jsonl:1:40",
                "doc_len": 21_u32,
                "text_preview": "cargo test failure output",
                "text_content": "cargo test failure output with stack details",
                "payload_json": "{\"tool\":\"bash\",\"status\":\"failed\"}",
                "mcp_event_type": "tool_response",
                "raw_score": 13.0,
                "matched_terms": 2_u64,
                "event_time": "2026-01-03 10:00:30",
                "event_unix_ms": 1767434430000_i64,
                "event_order": 40_u64,
                "turn_seq": 2_u32
            });
            let session_a_row = json!({
                "event_uid": "evt-a-11",
                "session_id": "sess_a",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_a.jsonl:1:11",
                "doc_len": 13_u32,
                "text_preview": "weaker assistant event in session a",
                "text_content": "weaker assistant event in session a with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-a\"}",
                "mcp_event_type": "assistant_response",
                "raw_score": 7.0,
                "matched_terms": 1_u64,
                "event_time": "2026-01-01 10:02:00",
                "event_unix_ms": 1767261720000_i64,
                "event_order": 11_u64,
                "turn_seq": 1_u32
            });
            let session_b_row = json!({
                "event_uid": "evt-b-9",
                "session_id": "sess_b",
                "source_name": "codex",
                "harness": "codex",
                "inference_provider": "openai",
                "endpoint_kind": "generation",
                "event_class": "message",
                "payload_type": "text",
                "actor_role": "assistant",
                "name": "",
                "phase": "",
                "source_ref": "/tmp/sess_b.jsonl:1:9",
                "doc_len": 9_u32,
                "text_preview": "third assistant event",
                "text_content": "third assistant event with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-b\"}",
                "mcp_event_type": "assistant_response",
                "raw_score": 6.0,
                "matched_terms": 1_u64,
                "event_time": "2026-01-02 10:02:00",
                "event_unix_ms": 1767348120000_i64,
                "event_order": 9_u64,
                "turn_seq": 1_u32
            });

            let filter_clause = query
                .split_once("WHERE ")
                .and_then(|(_, tail)| tail.split_once("GROUP BY p.doc_id"))
                .map(|(filter, _)| filter)
                .unwrap_or(query.as_str());

            if query.contains("tr.session_id = 'sess_c' AND tr.turn_seq = 2") {
                return (StatusCode::OK, json_each_row(json!([tool_row])));
            }
            if query.contains("d.session_id = 'sess_a'") {
                return (StatusCode::OK, json_each_row(json!([session_a_row])));
            }
            if filter_clause.contains("lowerUTF8(d.actor_role) = 'user'")
                && !filter_clause.contains("lowerUTF8(d.actor_role) = 'assistant'")
            {
                return (StatusCode::OK, json_each_row(json!([user_row])));
            }
            if filter_clause.contains("lowerUTF8(d.actor_role) = 'assistant'")
                && !filter_clause.contains("lowerUTF8(d.actor_role) = 'user'")
            {
                return (StatusCode::OK, json_each_row(json!([assistant_row])));
            }
            if query.contains("LIMIT 3") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([assistant_row, session_a_row, session_b_row])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([assistant_row, session_a_row])),
            );
        }

        // Conversation candidate discovery (issue #597 B6: over the bounded,
        // locator-authorized `term_postings`, the same relation `df` is counted
        // over).
        if query.contains("FROM term_postings AS p")
            && query.contains("GROUP BY p.session_id")
            && query.contains("SELECT\n  c.session_id AS session_id")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "score": 8.0,
                        "matched_terms": 2_u16
                    },
                    {
                        "session_id": "sess_a",
                        "score": 5.0,
                        "matched_terms": 1_u16
                    }
                ])),
            );
        }

        if query.contains("GROUP BY e.session_id") && query.contains("FROM term_postings AS p") {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "first_event_time": "2026-01-03 10:00:00",
                        "first_event_unix_ms": 1767434400000_i64,
                        "last_event_time": "2026-01-03 10:10:00",
                        "last_event_unix_ms": 1767435000000_i64,
                        "harness": "codex",
                        "score": 12.5,
                        "matched_terms": 2_u16,
                        "event_count_considered": 3_u32,
                        "best_event_uid": "evt-c-42",
                        "snippet": "best match from session c"
                    },
                    {
                        "session_id": "sess_a",
                        "first_event_time": "2026-01-01 10:00:00",
                        "first_event_unix_ms": 1767261600000_i64,
                        "last_event_time": "2026-01-01 10:10:00",
                        "last_event_unix_ms": 1767262200000_i64,
                        "harness": "codex",
                        "score": 7.0,
                        "matched_terms": 1_u16,
                        "event_count_considered": 2_u32,
                        "best_event_uid": "evt-a-11",
                        "snippet": "weaker match from session a"
                    }
                ])),
            );
        }

        // Issue #597 WI-06: the bounded `search_events` ranking pass. Content
        // free — identity, score and matched-term count only; everything the
        // `SearchRow` reports comes from the winner hydration read below.
        //
        // The branch this replaced matched the RETIRED exact fallback
        // (`GROUP BY p.doc_id` over `v_live_search_documents` carrying
        // `any(text_content)`). That is worth recording: because the mock
        // answers unmatched queries with an empty result, the old `search_events`
        // fast pass saw `df = 0` for every term, ranked nothing, and every
        // `search_events` fixture in this file was in fact exercising the
        // unbounded fallback.
        if query.contains("term_postings AS (")
            && query.contains("GROUP BY p.event_uid, p.source_host")
            && query.contains("ORDER BY score DESC, event_uid ASC, source_host ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "event_uid": "evt-c-42",
                        "source_host": "",
                        "score": 12.5,
                        "matched_terms": 2_u64
                    },
                    {
                        "event_uid": "evt-a-11",
                        "source_host": "",
                        "score": 7.0,
                        "matched_terms": 1_u64
                    }
                ])),
            );
        }

        // Issue #597 WI-06: bounded winner hydration for `search_events`
        // (`build_search_events_hydrate_sql`), keyed by the ranked identities.
        if query.contains("WITH requested_documents AS (") && query.contains("AS has_codex_mcp") {
            let rows = [
                json!({
                    "source_host": "",
                    "event_uid": "evt-c-42",
                    "session_id": "sess_c",
                    "event_time": "2026-01-03 10:02:00",
                    "source_name": "codex",
                    "harness": "codex",
                    "inference_provider": "openai",
                    "event_class": "message",
                    "payload_type": "text",
                    "actor_role": "assistant",
                    "name": "",
                    "phase": "",
                    "source_ref": "/tmp/sess_c.jsonl:1:42",
                    "doc_len": 19_u32,
                    "text_preview": "best event in session c",
                    "text_content": "best event in session c with extra context",
                    "payload_json": "{\"type\":\"message\",\"topic\":\"session-c\"}",
                    "has_codex_mcp": 0_u8
                }),
                json!({
                    "source_host": "",
                    "event_uid": "evt-a-11",
                    "session_id": "sess_a",
                    "event_time": "2026-01-01 10:02:00",
                    "source_name": "codex",
                    "harness": "codex",
                    "inference_provider": "openai",
                    "event_class": "message",
                    "payload_type": "text",
                    "actor_role": "assistant",
                    "name": "",
                    "phase": "",
                    "source_ref": "/tmp/sess_a.jsonl:1:11",
                    "doc_len": 13_u32,
                    "text_preview": "weaker event in session a",
                    "text_content": "weaker event in session a with extra context",
                    "payload_json": "{\"type\":\"message\",\"topic\":\"session-a\"}",
                    "has_codex_mcp": 0_u8
                }),
            ]
            .into_iter()
            .filter(|row| {
                let uid = row["event_uid"].as_str().unwrap_or_default();
                query.contains(&format!("'{uid}'"))
            })
            .collect::<Vec<_>>();
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("WHERE document.event_uid IN")
            && query.contains("GROUP BY document.source_host, document.event_uid")
            && query.contains("AS text_content")
            && query.contains("AS payload_json")
            && query.contains("AS event_class")
            && query.contains("AS actor_role")
        {
            let mut rows = vec![json!({
                "event_uid": "evt-c-42",
                "snippet": "best match from session c",
                "text_content": "best match from session c with extra context",
                "payload_json": "{\"type\":\"message\",\"topic\":\"session-c\"}",
                "event_class": "message",
                "actor_role": "assistant"
            })];
            if !state.options.omit_second_snippet_row {
                rows.push(json!({
                    "event_uid": "evt-a-11",
                    "snippet": "weaker match from session a",
                    "text_content": "weaker match from session a with extra context",
                    "payload_json": "{\"type\":\"message\",\"topic\":\"session-a\"}",
                    "event_class": "message",
                    "actor_role": "assistant"
                }));
            }
            return (StatusCode::OK, json_each_row(json!(rows)));
        }

        if query.contains("WHERE event_kind = 'session_meta'")
            && query.contains("GROUP BY session_id")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "harness": "codex",
                        "session_slug": "project-c",
                        "session_summary": "Session C summary"
                    },
                    {
                        "session_id": "sess_a",
                        "harness": "codex",
                        "session_slug": "",
                        "session_summary": ""
                    }
                ])),
            );
        }

        if query.contains("WHERE e.event_kind = 'session_meta'")
            && query.contains("AS meta_event_uid")
            && query.contains("AS matched_terms")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_meta_summary",
                        "first_event_time": "2026-01-05 10:00:00",
                        "first_event_unix_ms": 1767607200000_i64,
                        "last_event_time": "2026-01-05 10:15:00",
                        "last_event_unix_ms": 1767608100000_i64,
                        "total_turns": 4_u32,
                        "total_events": 18_u64,
                        "user_messages": 5_u64,
                        "assistant_messages": 5_u64,
                        "tool_calls": 1_u64,
                        "tool_results": 1_u64,
                        "mode": "chat",
                        "harness": "codex",
                        "inference_provider": "openai",
                        "session_slug": "rare-summary-session",
                        "session_summary": "Rare summary-only session about metadata discovery.",
                        "meta_event_uid": "meta-rare-1",
                        "score": 5.0,
                        "matched_terms": 2_u16,
                        "metadata_text": "{\"summary\":\"Rare summary-only session about metadata discovery.\"}"
                    }
                ])),
            );
        }

        if query.contains("row_number() OVER")
            && query.contains("PARTITION BY session_id, turn_seq")
            && query.contains("WHERE tr.event_uid IN")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "event_uid": "evt-c-tool",
                        "event_time": "2026-01-03 10:00:30",
                        "event_unix_ms": 1767434430000_i64,
                        "event_order": 40_u64,
                        "turn_seq": 2_u32,
                        "event_ordinal": 1_u32,
                        "turn_event_count": 3_u64,
                        "call_id": "call-bash-1",
                        "item_id": "item-tool",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-c-user",
                        "event_time": "2026-01-03 10:01:00",
                        "event_unix_ms": 1767434460000_i64,
                        "event_order": 41_u64,
                        "turn_seq": 2_u32,
                        "event_ordinal": 2_u32,
                        "turn_event_count": 3_u64,
                        "call_id": "",
                        "item_id": "item-user",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-c-42",
                        "event_time": "2026-01-03 10:02:00",
                        "event_unix_ms": 1767434520000_i64,
                        "event_order": 42_u64,
                        "turn_seq": 2_u32,
                        "event_ordinal": 3_u32,
                        "turn_event_count": 3_u64,
                        "call_id": "",
                        "item_id": "item-assistant",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-a-11",
                        "event_time": "2026-01-01 10:02:00",
                        "event_unix_ms": 1767261720000_i64,
                        "event_order": 11_u64,
                        "turn_seq": 1_u32,
                        "event_ordinal": 1_u32,
                        "turn_event_count": 1_u64,
                        "call_id": "",
                        "item_id": "item-a",
                        "model": "gpt-5.3-codex"
                    },
                    {
                        "event_uid": "evt-b-9",
                        "event_time": "2026-01-02 10:02:00",
                        "event_unix_ms": 1767348120000_i64,
                        "event_order": 9_u64,
                        "turn_seq": 1_u32,
                        "event_ordinal": 1_u32,
                        "turn_event_count": 1_u64,
                        "call_id": "",
                        "item_id": "item-b",
                        "model": "gpt-5.3-codex"
                    }
                ])),
            );
        }

        if query.contains("SELECT session_id, event_order, turn_seq")
            && query.contains("WHERE event_uid = 'evt-open-full'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess-event",
                        "event_order": 2_u64,
                        "turn_seq": 1_u32
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-open'")
            && query.contains("ORDER BY turn_seq ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    turn_summary_row("sess-open", 1, 5, 1, 1, 1, 1, 0),
                    turn_summary_row("sess-open", 2, 3, 1, 1, 0, 0, 0)
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
            && query.contains("WHERE session_id = 'sess-open'")
            && !query.contains("turn_seq =")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-open",
                        "evt-open-1",
                        1,
                        1,
                        "user",
                        "message",
                        "text",
                        "How should repository open models work?",
                        "{\"text\":\"How should repository open models work?\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-2",
                        2,
                        1,
                        "assistant",
                        "tool_call",
                        "function_call",
                        "",
                        "{\"name\":\"search_repo\"}",
                        "search_repo",
                        "call-search"
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-3",
                        3,
                        1,
                        "tool",
                        "tool_result",
                        "function_call_output",
                        "repo results",
                        "{\"result\":\"repo results\"}",
                        "search_repo",
                        "call-search"
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-4",
                        4,
                        1,
                        "assistant",
                        "message",
                        "text",
                        "First answer with repository context.",
                        "{\"text\":\"First answer with repository context.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-5",
                        5,
                        1,
                        "system",
                        "event_msg",
                        "task_complete",
                        "",
                        "{\"status\":\"complete\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-6",
                        6,
                        2,
                        "user",
                        "message",
                        "text",
                        "Confirm the final shape.",
                        "{\"text\":\"Confirm the final shape.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-7",
                        7,
                        2,
                        "assistant",
                        "message",
                        "text",
                        "Final response summary text.",
                        "{\"text\":\"Final response summary text.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-open",
                        "evt-open-8",
                        8,
                        2,
                        "system",
                        "event_msg",
                        "task_complete",
                        "",
                        "{\"status\":\"complete\"}",
                        "",
                        ""
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete'")
            && query.contains("ORDER BY turn_seq ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    turn_summary_row("sess-incomplete", 1, 2, 1, 1, 0, 0, 0),
                    turn_summary_row("sess-incomplete", 2, 3, 1, 0, 1, 1, 0)
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq = 2")
            && query.contains("LIMIT 1")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row(
                    "sess-incomplete",
                    2,
                    3,
                    1,
                    0,
                    1,
                    1,
                    0
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-incomplete'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
            && !query.contains("turn_seq =")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-1",
                        1,
                        1,
                        "user",
                        "message",
                        "text",
                        "Previous turn.",
                        "{\"text\":\"Previous turn.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-2",
                        2,
                        2,
                        "user",
                        "message",
                        "text",
                        "Run the incomplete workflow.",
                        "{\"text\":\"Run the incomplete workflow.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-3",
                        3,
                        2,
                        "assistant",
                        "tool_call",
                        "function_call",
                        "",
                        "{\"name\":\"inspect\"}",
                        "inspect",
                        "call-inspect"
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-4",
                        4,
                        2,
                        "tool",
                        "tool_result",
                        "function_call_output",
                        "inspection output",
                        "{\"ok\":true}",
                        "inspect",
                        "call-inspect"
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq = 2")
            && query.contains("ORDER BY event_order ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-2",
                        2,
                        2,
                        "user",
                        "message",
                        "text",
                        "Run the incomplete workflow.",
                        "{\"text\":\"Run the incomplete workflow.\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-3",
                        3,
                        2,
                        "assistant",
                        "tool_call",
                        "function_call",
                        "",
                        "{\"name\":\"inspect\"}",
                        "inspect",
                        "call-inspect"
                    ),
                    trace_event_row(
                        "sess-incomplete",
                        "evt-inc-4",
                        4,
                        2,
                        "tool",
                        "tool_result",
                        "function_call_output",
                        "inspection output",
                        "{\"ok\":true}",
                        "inspect",
                        "call-inspect"
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq < 2")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row(
                    "sess-incomplete",
                    1,
                    2,
                    1,
                    1,
                    0,
                    0,
                    0
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-incomplete' AND turn_seq > 2")
        {
            return (StatusCode::OK, json_each_row(json!([])));
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query.contains("ORDER BY turn_seq ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    turn_summary_row("sess-event", 1, 3, 1, 1, 0, 0, 0),
                    turn_summary_row("sess-event", 2, 1, 0, 1, 0, 0, 0)
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE event_uid = 'evt-open-full'")
            && query.contains("ORDER BY event_order DESC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row("sess-event", "evt-open-full", 2, 1, "assistant", "message", "text", "This is the full available event content that must not be clipped by the repository open model.", "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}", "", "")
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event' AND event_order = 2")
            && query.contains("event_uid = 'evt-open-full'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row("sess-event", "evt-open-full", 2, 1, "assistant", "message", "text", "This is the full available event content that must not be clipped by the repository open model.", "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}", "", "")
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event' AND turn_seq = 1")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row("sess-event", 1, 3, 1, 1, 0, 0, 0)])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query
                .contains("event_order < 2 OR (event_order = 2 AND event_uid < 'evt-open-full')")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([trace_event_row(
                    "sess-event",
                    "evt-event-1",
                    1,
                    1,
                    "user",
                    "message",
                    "text",
                    "question before full event",
                    "{\"text\":\"question before full event\"}",
                    "",
                    ""
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query
                .contains("event_order > 2 OR (event_order = 2 AND event_uid > 'evt-open-full')")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([trace_event_row(
                    "sess-event",
                    "evt-event-3",
                    3,
                    1,
                    "system",
                    "event_msg",
                    "task_complete",
                    "",
                    "{\"status\":\"complete\"}",
                    "",
                    ""
                )])),
            );
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event' AND turn_seq < 1")
        {
            return (StatusCode::OK, json_each_row(json!([])));
        }

        if query.contains("FROM `moraine`.`v_turn_summary`")
            && query.contains("WHERE session_id = 'sess-event' AND turn_seq > 1")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([turn_summary_row("sess-event", 2, 1, 0, 1, 0, 0, 0)])),
            );
        }

        if query.contains("FROM `moraine`.`v_live_search_documents`")
            && query.contains("WHERE event_uid = 'evt-open-full'")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([{ "session_id": "sess-event" }])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess-event'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
        {
            return (
                StatusCode::OK,
                json_each_row(json!([
                    trace_event_row(
                        "sess-event",
                        "evt-event-1",
                        1,
                        1,
                        "user",
                        "message",
                        "text",
                        "question before full event",
                        "{\"text\":\"question before full event\"}",
                        "",
                        ""
                    ),
                    trace_event_row("sess-event", "evt-open-full", 2, 1, "assistant", "message", "text", "This is the full available event content that must not be clipped by the repository open model.", "{\"text\":\"This is the full payload JSON value that must also remain intact\",\"nested\":{\"answer\":42}}", "", ""),
                    trace_event_row(
                        "sess-event",
                        "evt-event-3",
                        3,
                        1,
                        "system",
                        "event_msg",
                        "task_complete",
                        "",
                        "{\"status\":\"complete\"}",
                        "",
                        ""
                    ),
                    trace_event_row(
                        "sess-event",
                        "evt-event-4",
                        4,
                        2,
                        "assistant",
                        "message",
                        "text",
                        "next turn",
                        "{\"text\":\"next turn\"}",
                        "",
                        ""
                    )
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess_c'")
            && query.contains("ORDER BY event_order ASC, event_uid ASC")
        {
            if query.contains("event_order > 2 OR (event_order = 2 AND event_uid > 'evt-2')") {
                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_c",
                            "event_uid": "evt-3",
                            "event_order": 3_u64,
                            "turn_seq": 2_u32,
                            "event_time": "2026-01-03 10:02:00",
                            "event_unix_ms": 1_767_434_520_000_i64,
                            "actor_role": "assistant",
                            "event_class": "message",
                            "payload_type": "text",
                            "call_id": "",
                            "name": "",
                            "phase": "",
                            "item_id": "itm-3",
                            "source_ref": "/tmp/sess_c.jsonl:1:3",
                            "text_content": "assistant answer",
                            "payload_json": "{\"text\":\"assistant answer\"}",
                            "token_usage_json": "{}"
                        }
                    ])),
                );
            }

            return (
                StatusCode::OK,
                json_each_row(json!([
                    {
                        "session_id": "sess_c",
                        "event_uid": "evt-1",
                        "event_order": 1_u64,
                        "turn_seq": 1_u32,
                        "event_time": "2026-01-03 10:00:00",
                        "event_unix_ms": 1_767_434_400_000_i64,
                        "actor_role": "user",
                        "event_class": "message",
                        "payload_type": "text",
                        "call_id": "",
                        "name": "",
                        "phase": "",
                        "item_id": "itm-1",
                        "source_ref": "/tmp/sess_c.jsonl:1:1",
                        "text_content": "user question",
                        "payload_json": "{\"text\":\"user question\"}",
                        "token_usage_json": "{}"
                    },
                    {
                        "session_id": "sess_c",
                        "event_uid": "evt-2",
                        "event_order": 2_u64,
                        "turn_seq": 1_u32,
                        "event_time": "2026-01-03 10:01:00",
                        "event_unix_ms": 1_767_434_460_000_i64,
                        "actor_role": "assistant",
                        "event_class": "reasoning",
                        "payload_type": "text",
                        "call_id": "",
                        "name": "",
                        "phase": "",
                        "item_id": "itm-2",
                        "source_ref": "/tmp/sess_c.jsonl:1:2",
                        "text_content": "assistant reasoning",
                        "payload_json": "{\"text\":\"assistant reasoning\"}",
                        "token_usage_json": "{}"
                    },
                    {
                        "session_id": "sess_c",
                        "event_uid": "evt-3",
                        "event_order": 3_u64,
                        "turn_seq": 2_u32,
                        "event_time": "2026-01-03 10:02:00",
                        "event_unix_ms": 1_767_434_520_000_i64,
                        "actor_role": "assistant",
                        "event_class": "message",
                        "payload_type": "text",
                        "call_id": "",
                        "name": "",
                        "phase": "",
                        "item_id": "itm-3",
                        "source_ref": "/tmp/sess_c.jsonl:1:3",
                        "text_content": "assistant answer",
                        "payload_json": "{\"text\":\"assistant answer\"}",
                        "token_usage_json": "{}"
                    }
                ])),
            );
        }

        if query.contains("FROM `moraine`.`v_conversation_trace`")
            && query.contains("WHERE session_id = 'sess_c'")
            && query.contains("ORDER BY event_order DESC, event_uid DESC")
        {
            if query.contains("event_class = 'message'") {
                if query.contains("event_order < 3 OR (event_order = 3 AND event_uid < 'evt-3')") {
                    return (
                        StatusCode::OK,
                        json_each_row(json!([
                            {
                                "session_id": "sess_c",
                                "event_uid": "evt-1",
                                "event_order": 1_u64,
                                "turn_seq": 1_u32,
                                "event_time": "2026-01-03 10:00:00",
                                "event_unix_ms": 1_767_434_400_000_i64,
                                "actor_role": "user",
                                "event_class": "message",
                                "payload_type": "text",
                                "call_id": "",
                                "name": "",
                                "phase": "",
                                "item_id": "itm-1",
                                "source_ref": "/tmp/sess_c.jsonl:1:1",
                                "text_content": "user question",
                                "payload_json": "{\"text\":\"user question\"}",
                                "token_usage_json": "{}"
                            }
                        ])),
                    );
                }

                return (
                    StatusCode::OK,
                    json_each_row(json!([
                        {
                            "session_id": "sess_c",
                            "event_uid": "evt-3",
                            "event_order": 3_u64,
                            "turn_seq": 2_u32,
                            "event_time": "2026-01-03 10:02:00",
                            "event_unix_ms": 1_767_434_520_000_i64,
                            "actor_role": "assistant",
                            "event_class": "message",
                            "payload_type": "text",
                            "call_id": "",
                            "name": "",
                            "phase": "",
                            "item_id": "itm-3",
                            "source_ref": "/tmp/sess_c.jsonl:1:3",
                            "text_content": "assistant answer",
                            "payload_json": "{\"text\":\"assistant answer\"}",
                            "token_usage_json": "{}"
                        },
                        {
                            "session_id": "sess_c",
                            "event_uid": "evt-1",
                            "event_order": 1_u64,
                            "turn_seq": 1_u32,
                            "event_time": "2026-01-03 10:00:00",
                            "event_unix_ms": 1_767_434_400_000_i64,
                            "actor_role": "user",
                            "event_class": "message",
                            "payload_type": "text",
                            "call_id": "",
                            "name": "",
                            "phase": "",
                            "item_id": "itm-1",
                            "source_ref": "/tmp/sess_c.jsonl:1:1",
                            "text_content": "user question",
                            "payload_json": "{\"text\":\"user question\"}",
                            "token_usage_json": "{}"
                        }
                    ])),
                );
            }
        }

        (StatusCode::OK, json_each_row(json!([])))
    }

    let scripted_responses = (!options.scripted_responses.is_empty())
        .then(|| options.scripted_responses.iter().cloned().collect());
    let state = Arc::new(MockState {
        queries: Mutex::default(),
        publication_snapshot_queries: Mutex::default(),
        readiness_probe_queries: Mutex::default(),
        query_ids: Mutex::default(),
        request_params: Mutex::default(),
        options,
        scripted_responses: Mutex::new(scripted_responses),
    });
    let app = Router::new()
        .route("/", get(handler).post(handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("listener addr");

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{}", addr), state)
}

pub(crate) async fn build_repo_with_max_results(
    max_results: u16,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        max_results,
        MockOptions {
            // Not-ready store: session listing stays on the projected-header
            // path, which is what the pre-#599 fixtures below describe.
            open_v2_reader_ready: Some(false),
            ..MockOptions::default()
        },
    )
    .await
}

pub(crate) async fn build_repo_with_options(
    max_results: u16,
    options: MockOptions,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    let (base_url, state) = spawn_mock_server(options).await;
    let client =
        ClickHouseClient::new(test_clickhouse_config(base_url)).expect("valid clickhouse client");

    let repo = ClickHouseConversationRepository::new(
        client,
        RepoConfig {
            max_results,
            ..RepoConfig::default()
        },
    );

    (repo, state)
}

pub(crate) async fn build_scripted_repo(
    scripted_responses: Vec<ScriptedResponse>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            scripted_responses,
            ..MockOptions::default()
        },
    )
    .await
}

/// [`build_scripted_repo`] with the issue-598 readiness verdict declared, so
/// the `mcp_read_index_state` probe is answered out of band instead of
/// consuming the script's first response.
///
/// Search fixtures need this: every `search_mcp_events` request asks the latch
/// which engine to run. Without a declared verdict the probe eats a scripted
/// reply and every later assertion is silently off by one. `false` selects the
/// legacy projected-header engine, which is what the pre-#597 scripts describe.
pub(crate) async fn build_scripted_repo_with_readiness(
    scripted_responses: Vec<ScriptedResponse>,
    open_v2_reader_ready: bool,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            scripted_responses,
            open_v2_reader_ready: Some(open_v2_reader_ready),
            ..MockOptions::default()
        },
    )
    .await
}
pub(crate) async fn build_repo() -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_max_results(100).await
}

/// Repository with a `--project-only` session origin scope configured.
pub(crate) async fn build_scoped_repo(
    roots: &[&str],
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_scoped_repo_with_readiness(roots, Some(false)).await
}

/// Repository whose backend reports the issue-598 `open_v2` key ready, so
/// session listing takes the issue-599 directory path.
pub(crate) async fn build_directory_repo() -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            open_v2_reader_ready: Some(true),
            ..MockOptions::default()
        },
    )
    .await
}

/// [`build_directory_repo`] with a `--project-only` session origin scope.
pub(crate) async fn build_scoped_directory_repo(
    roots: &[&str],
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_scoped_repo_with_readiness(roots, Some(true)).await
}

/// [`build_scoped_directory_repo`] with mock behaviour declared, so a test can
/// drive the exact Phase 4 scope re-check with a candidate the directory recall
/// filter admitted.
pub(crate) async fn build_scoped_directory_repo_with_options(
    roots: &[&str],
    options: MockOptions,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    let (base_url, state) = spawn_mock_server(MockOptions {
        open_v2_reader_ready: Some(true),
        ..options
    })
    .await;
    let client =
        ClickHouseClient::new(test_clickhouse_config(base_url)).expect("valid clickhouse client");
    let repo = ClickHouseConversationRepository::new(
        client,
        RepoConfig {
            max_results: 100,
            session_scope: SessionOriginScope::from_roots(roots.iter().copied()),
            ..RepoConfig::default()
        },
    );
    (repo, state)
}

/// [`build_scripted_repo`] against a directory-ready backend.
pub(crate) async fn build_scripted_directory_repo(
    scripted_responses: Vec<ScriptedResponse>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            scripted_responses,
            open_v2_reader_ready: Some(true),
            ..MockOptions::default()
        },
    )
    .await
}

/// [`build_scripted_repo`] against a backend whose `open_v2` reader is not
/// published, so scripts describe the projected-header path.
pub(crate) async fn build_scripted_header_repo(
    scripted_responses: Vec<ScriptedResponse>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    build_repo_with_options(
        100,
        MockOptions {
            scripted_responses,
            open_v2_reader_ready: Some(false),
            ..MockOptions::default()
        },
    )
    .await
}

/// [`build_scripted_directory_repo`] with a configured project scope, so a
/// test can script a candidate that Phase A's recall predicate admitted and
/// assert the exact Phase C re-check still rejects it.
pub(crate) async fn build_scoped_scripted_directory_repo(
    roots: &[&str],
    scripted_responses: Vec<ScriptedResponse>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    let (base_url, state) = spawn_mock_server(MockOptions {
        scripted_responses,
        open_v2_reader_ready: Some(true),
        ..MockOptions::default()
    })
    .await;
    let client =
        ClickHouseClient::new(test_clickhouse_config(base_url)).expect("valid clickhouse client");
    let repo = ClickHouseConversationRepository::new(
        client,
        RepoConfig {
            max_results: 100,
            session_scope: SessionOriginScope::from_roots(roots.iter().copied()),
            ..RepoConfig::default()
        },
    );
    (repo, state)
}

async fn build_scoped_repo_with_readiness(
    roots: &[&str],
    open_v2_reader_ready: Option<bool>,
) -> (ClickHouseConversationRepository, Arc<MockState>) {
    let (base_url, state) = spawn_mock_server(MockOptions {
        open_v2_reader_ready,
        ..MockOptions::default()
    })
    .await;
    let client =
        ClickHouseClient::new(test_clickhouse_config(base_url)).expect("valid clickhouse client");

    let repo = ClickHouseConversationRepository::new(
        client,
        RepoConfig {
            max_results: 100,
            session_scope: SessionOriginScope::from_roots(roots.iter().copied()),
            ..RepoConfig::default()
        },
    );

    (repo, state)
}
