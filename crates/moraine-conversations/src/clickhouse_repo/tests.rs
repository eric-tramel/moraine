use super::file_attention::merge_file_attention_touches;
use super::search::{ConversationSessionFilter, CODEX_FINAL_ANSWER_MIRROR_MAX_TIMESTAMP_DELTA_MS};
use super::*;

fn sample_search_doc() -> SearchDocExtraCacheEntry {
    SearchDocExtraCacheEntry {
        session_id: "session-1".to_string(),
        event_time: "2026-04-27T12:00:00.000Z".to_string(),
        source_name: "source".to_string(),
        harness: "harness".to_string(),
        inference_provider: "inference-provider".to_string(),
        event_class: "message".to_string(),
        payload_type: "message".to_string(),
        actor_role: "assistant".to_string(),
        name: "tool".to_string(),
        phase: "".to_string(),
        source_ref: "source-ref".to_string(),
        doc_len: 42,
        text_preview: "preview".to_string(),
        text_content: "full preview content".to_string(),
        payload_json: "{\"type\":\"message\"}".to_string(),
        has_codex_mcp: 0,
        fetched_at: Instant::now(),
    }
}

#[allow(clippy::too_many_arguments)]
fn sample_search_row(
    event_uid: &str,
    session_id: &str,
    event_class: &str,
    payload_type: &str,
    actor_role: &str,
    text_preview: &str,
    score: f64,
    matched_terms: u64,
) -> SearchRow {
    SearchRow {
        source_host: "host-a".to_string(),
        event_uid: event_uid.to_string(),
        session_id: session_id.to_string(),
        event_time: "2026-04-27T12:00:00.000Z".to_string(),
        source_name: "source".to_string(),
        harness: "harness".to_string(),
        inference_provider: "inference-provider".to_string(),
        event_class: event_class.to_string(),
        payload_type: payload_type.to_string(),
        actor_role: actor_role.to_string(),
        name: String::new(),
        phase: String::new(),
        source_ref: "source-ref".to_string(),
        doc_len: 42,
        text_preview: text_preview.to_string(),
        text_content: text_preview.to_string(),
        payload_json: "{\"type\":\"message\"}".to_string(),
        score,
        matched_terms,
    }
}

fn sample_mcp_search_row(event_uid: &str, raw_score: f64, event_unix_ms: i64) -> SearchMcpEventRow {
    SearchMcpEventRow {
        event_uid: event_uid.to_string(),
        source_host: "host-a".to_string(),
        session_id: "session-1".to_string(),
        source_name: "source".to_string(),
        harness: "harness".to_string(),
        inference_provider: "inference-provider".to_string(),
        endpoint_kind: "generation".to_string(),
        event_class: "message".to_string(),
        payload_type: "message".to_string(),
        actor_role: "assistant".to_string(),
        name: String::new(),
        phase: String::new(),
        payload_phase: String::new(),
        source_ref: "source-ref".to_string(),
        doc_len: 42,
        text_preview: "preview".to_string(),
        text_content: "preview".to_string(),
        text_content_digest: "digest-preview".to_string(),
        payload_json: "{}".to_string(),
        mcp_event_type: "assistant_response".to_string(),
        raw_score,
        matched_terms: 1,
        event_time: String::new(),
        event_unix_ms,
        event_order: 0,
        turn_seq: 0,
        event_ordinal: 0,
        turn_event_count: 0,
        turn_completed: 0,
        turn_terminal_event_uid: String::new(),
        call_id: String::new(),
        item_id: String::new(),
        model: String::new(),
        session_started_at_unix_ms: 0,
        session_updated_at_unix_ms: 0,
        session_title: String::new(),
        session_slug: String::new(),
        session_summary: String::new(),
        session_completed: 0,
        hydration_event_ts_ms: event_unix_ms,
        ranking_sort_time_ms: event_unix_ms,
    }
}

fn sample_codex_final_answer_mirror_rows(
    timestamp_delta_ms: i64,
) -> (SearchMcpEventRow, SearchMcpEventRow) {
    let mut response_item = sample_mcp_search_row("uid-response-item", 18.0, 1_777_000_000_000);
    response_item.harness = "codex".to_string();
    response_item.phase = "final_answer".to_string();
    response_item.source_ref = "/tmp/session:with-colon.jsonl:7:41".to_string();
    response_item.event_order = 41;
    response_item.turn_seq = 2;
    response_item.text_content = "byte-identical final answer".to_string();
    response_item.text_content_digest = "digest-final-answer".to_string();

    let mut event_msg = response_item.clone();
    event_msg.event_uid = "uid-event-msg".to_string();
    event_msg.event_class = "event_msg".to_string();
    event_msg.payload_type = "agent_message".to_string();
    event_msg.phase = "completed".to_string();
    event_msg.payload_phase = "final_answer".to_string();
    event_msg.source_ref = "/tmp/session:with-colon.jsonl:7:42".to_string();
    event_msg.event_order = 42;
    event_msg.event_unix_ms += timestamp_delta_ms;

    (response_item, event_msg)
}

fn sample_file_attention_touch(
    session_id: &str,
    tool_call_id: &str,
    event_uid: &str,
    event_unix_ms: Option<i64>,
    event_order: u64,
) -> FileAttentionTouch {
    FileAttentionTouch {
        session_id: session_id.to_string(),
        event_uid: event_uid.to_string(),
        tool_call_id: tool_call_id.to_string(),
        harness: "codex".to_string(),
        source_name: "codex".to_string(),
        tool_name: "Edit".to_string(),
        tool_phase: "request".to_string(),
        match_kind: "path_suffix".to_string(),
        matched_path: "/repo/src/lib.rs".to_string(),
        worktree_root: "/repo".to_string(),
        cwd: "/repo".to_string(),
        event_unix_ms,
        event_order,
        turn_seq: Some(1),
        input_preview: String::new(),
        output_preview: String::new(),
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
fn repository_reads_leave_thread_scheduling_to_clickhouse() {
    assert!(REPOSITORY_READ_SETTINGS
        .iter()
        .all(|(name, _)| *name != "max_threads"));
    assert!(
        REPOSITORY_READ_SETTINGS.contains(&("do_not_merge_across_partitions_select_final", "0"))
    );
}

#[tokio::test]
async fn mcp_search_sql_excludes_internal_tool_calls() {
    let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
        .expect("build ClickHouse client");
    let repo = ClickHouseConversationRepository::new(client, RepoConfig::default());
    let sql = with_test_publication_snapshot(TestPublicationSnapshot::idle_local(1, 1), async {
        repo.build_search_mcp_events_sql(
            &["needle".to_string()],
            &[McpEventType::ToolCall],
            None,
            None,
            None,
            None,
            1,
            0.0,
            Some((1, 1)),
            20,
        )
        .expect("build MCP event search SQL")
    })
    .await;

    assert!(sql.contains("p.source_name != 'codex-mcp'"));
    assert!(sql.contains("splitByString('__', lowerUTF8(trimBoth(p.name)))"));
    assert!(sql.contains("arrayElement"));
    assert!(sql.contains("= 'moraine'"));
    assert!(sql.contains("FROM `moraine`.`v_live_search_postings` AS p FINAL"));
    assert!(sql.contains("WHERE p.term IN q_terms"));
    assert_eq!(
        sql.matches("FROM `moraine`.`v_live_search_postings` AS p FINAL")
            .count(),
        1
    );
    assert!(!sql.contains("matching_doc_ids AS ("));
    assert!(!sql.contains("projected_candidates AS ("));
    assert!(sql.contains("ALL INNER JOIN `moraine`.`mcp_open_events` AS e FINAL"));
    assert!(sql.contains("ON e.source_host = p.source_host"));
    assert!(sql.contains("AND e.event_uid = p.doc_id"));
    assert!(sql.contains("AND e.session_id = s.session_id"));
    assert!(sql.contains("AND e.slot = s.slot"));
    assert!(sql.contains("AND e.generation = s.generation"));
    assert!(sql.contains("GROUP BY p.doc_id, p.source_host"));
    assert!(!sql.contains("PREWHERE p.term"));
}

#[test]
fn exact_oracle_still_pins_the_full_document_view_contract() {
    // The oracle survives ONLY under cfg(test) (issue #597 §1.5/F1+F3). This
    // test is deliberately kept rather than deleted: it is what makes the
    // oracle's `O(D x doc_bytes)` shape visible, so the contrast with
    // `search_events_ranking_reads_no_content_and_no_documents` is explicit
    // rather than asserted in prose.
    let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
        .expect("build ClickHouse client");
    let repo = ClickHouseConversationRepository::new(client, RepoConfig::default());
    let terms = vec!["needle".to_string()];
    let idf = HashMap::from([("needle".to_string(), 1.0)]);

    let oracle_sql = repo
        .build_search_events_exact_oracle_sql(
            &terms, &idf, 10.0, true, None, true, true, None, None, 1, 0.0, 20,
        )
        .expect("build exact oracle SQL");
    let hydration_sql = repo
        .build_search_events_hydrate_sql(&[SearchDocumentIdentity::new("host-a", "event-a")])
        .expect("build live search hydration SQL");

    for sql in [&oracle_sql, &hydration_sql] {
        assert!(sql.contains("FROM `moraine`.`v_live_search_documents` AS t"));
        for required in [
            "t.source_host",
            "t.event_uid",
            "t.session_id",
            "t.record_ts",
            "t.source_name",
            "t.harness",
            "t.inference_provider",
            "t.event_class",
            "t.payload_type",
            "t.actor_role",
            "t.name",
            "t.phase",
            "t.source_ref",
            "t.doc_len",
            "t.text_content",
            "t.payload_json",
            "t.has_codex_mcp",
        ] {
            assert!(
                sql.contains(required),
                "live search SQL omitted {required}: {sql}"
            );
        }
    }
    assert!(oracle_sql.contains("GROUP BY t.source_host, t.event_uid"));
    assert!(oracle_sql.contains("ON d.source_host = p.source_host"));
    assert!(oracle_sql.contains("GROUP BY p.doc_id, p.source_host"));
    assert!(hydration_sql.contains("requested.source_host = t.source_host"));
    assert!(hydration_sql.contains("GROUP BY t.source_host, t.event_uid"));
}

/// The F8 deletion. When the schema probe reported `has_codex_mcp` absent, the
/// bounded hydration statement fell back to
/// `positionCaseInsensitiveUTF8(t.payload_json, 'codex-mcp')`, decompressing
/// every requested payload to compute one boolean.
///
/// MUTATION: restore the `use_document_codex_flag` branch and emit the
/// `positionCaseInsensitiveUTF8` expression; this test fails.
#[test]
fn hydration_reads_the_codex_flag_column_and_never_scans_payloads_for_it() {
    let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
        .expect("build ClickHouse client");
    let repo = ClickHouseConversationRepository::new(client, RepoConfig::default());
    let sql = repo
        .build_search_events_hydrate_sql(&[SearchDocumentIdentity::new("host-a", "event-a")])
        .expect("build live search hydration SQL");

    assert!(sql.contains("toUInt8(any(t.has_codex_mcp)) AS has_codex_mcp"));
    assert!(
        !sql.contains("positionCaseInsensitiveUTF8"),
        "hydration must not scan payloads for the codex flag: {sql}"
    );
}

#[test]
fn conversation_candidates_aggregate_live_term_frequency_before_scoring() {
    let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
        .expect("build ClickHouse client");
    let repo = ClickHouseConversationRepository::new(client, RepoConfig::default());
    let terms = vec!["needle".to_string()];
    let idf = HashMap::from([("needle".to_string(), 1.0)]);

    let sql = repo
        .build_search_conversation_candidates_sql(
            &terms, &idf, true, false, 1, 20, None, None, None,
        )
        .expect("build conversation candidate SQL");

    assert!(sql.contains("sum(p.tf) AS tf_sum"));
    assert!(sql.contains("GROUP BY p.session_id, p.term"));
    assert!(sql.contains("log1p(toFloat64(terms.tf_sum))"));
    assert!(!sql.contains("log1p(toFloat64(p.tf))"));
}

#[test]
fn conversation_candidate_document_filters_only_select_eligible_sessions() {
    let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
        .expect("build ClickHouse client");
    let repo = ClickHouseConversationRepository::new(client, RepoConfig::default());
    let terms = vec!["needle".to_string()];
    let idf = HashMap::from([("needle".to_string(), 1.0)]);

    let sql = repo
        .build_search_conversation_candidates_sql(
            &terms,
            &idf,
            false,
            true,
            1,
            20,
            Some(1_000),
            Some(2_000),
            None,
        )
        .expect("build filtered conversation candidate SQL");
    let (_, after_eligible) = sql
        .split_once("eligible_sessions AS (\n")
        .expect("eligible-session CTE");
    let (eligible, after_eligible) = after_eligible
        .split_once("  ),\n  session_terms AS (\n")
        .expect("session-term CTE boundary");
    let (session_terms, _) = after_eligible
        .split_once("  )\nSELECT")
        .expect("session-term CTE end");

    assert!(eligible.contains("v_live_search_documents"));
    assert!(eligible.contains("toUnixTimestamp64Milli(d.ingested_at) >= 1000"));
    assert!(eligible.contains("toUnixTimestamp64Milli(d.ingested_at) < 2000"));
    assert!(session_terms.contains("ALL INNER JOIN eligible_sessions AS eligible"));
    assert!(session_terms.contains("WHERE p.term IN q_terms"));
    assert!(!session_terms.contains("v_live_search_documents"));
    assert!(!session_terms.contains("d.ingested_at"));
}

#[test]
fn safe_filter_value_validation() {
    assert!(is_safe_filter_value("session_123"));
    assert!(is_safe_filter_value("a/b.c:d@e-1"));
    assert!(!is_safe_filter_value("drop table;"));
}

/// F5. Three conditions used to collapse to `candidate_session_ids = None`,
/// which emitted NO session predicate — a whole-corpus postings scan. The type
/// now has no variant that means that, but a `Sessions(&[])` that quietly
/// emitted nothing would reopen the hole with the type change still in place
/// (mutation M16 showed exactly that).
///
/// So: `Sessions` ALWAYS emits the predicate. An empty list produces
/// `p.session_id IN []`, which matches nothing — zero candidates means zero
/// results, which is the correct answer and the one the retired code refused to
/// give. `search_conversations_impl` returns early before it ever gets here.
///
/// MUTATION: wrap the push in `if !session_ids.is_empty()`; this fails.
#[test]
fn conversation_scoring_always_emits_a_session_predicate() {
    let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
        .expect("build ClickHouse client");
    let repo = ClickHouseConversationRepository::new(client, RepoConfig::default());
    let terms = vec!["needle".to_string()];

    for (label, sessions) in [
        ("empty", Vec::<String>::new()),
        ("populated", vec!["sess-a".to_string()]),
    ] {
        let (_, filter_sql) = repo.build_conversation_postings_filter_sql(
            &terms,
            true,
            false,
            None,
            None,
            None,
            ConversationSessionFilter::Sessions(&sessions),
        );
        assert!(
            filter_sql.contains("p.session_id IN "),
            "the {label} session set must still restrict the postings scan: {filter_sql}"
        );
    }

    // Discovery is the ONE relation allowed no session predicate, and it is
    // bounded by its own LIMIT instead.
    let (_, discovery_sql) = repo.build_conversation_postings_filter_sql(
        &terms,
        true,
        false,
        None,
        None,
        None,
        ConversationSessionFilter::Discovery,
    );
    assert!(!discovery_sql.contains("p.session_id IN "));
}

#[test]
fn sql_array_builders_escape_values() {
    let values = vec!["a".to_string(), "b'c".to_string()];
    let out = sql_array_strings(&values);
    assert!(out.contains("'a'"));
    assert!(out.contains("'b''c'"));
}

/// F-TIE. The v2 ranking statement orders by the locator's `sort_time`, while
/// the REPORTED `event.timestamp` is navigation's `display_time` — they differ
/// exactly for an event whose `record_ts` does not parse, and only when
/// `raw_score` ties. That is precisely when the tiebreak decides the answer, so
/// the Rust-side order must use the same key the SQL did.
///
/// MUTATION: point `sort_canonical_search_rows` at `event_unix_ms` (i.e. reuse
/// `sort_search_mcp_event_rows`, which is correct for the v1 engine) and this
/// fails: the malformed-`record_ts` row sorts first by display time and last by
/// sort time.
#[test]
fn canonical_search_rows_order_by_sort_time_not_display_time() {
    // Two hits, identical score. `evt-broken` has an unparseable `record_ts`:
    // its `sort_time` is the epoch sentinel (ranked last) while its
    // `display_time` falls back to `ingested_at`, which is LATER than the other
    // row's (which would rank it first).
    let mut good = sample_mcp_search_row("evt-good", 9.0, 1_767_434_520_000);
    good.ranking_sort_time_ms = 1_767_434_520_000;
    let mut broken = sample_mcp_search_row("evt-broken", 9.0, 1_767_434_999_000);
    broken.ranking_sort_time_ms = 0;

    let mut rows = vec![broken.clone(), good.clone()];
    ClickHouseConversationRepository::sort_canonical_search_rows(&mut rows);
    assert_eq!(
        rows.iter()
            .map(|row| row.event_uid.as_str())
            .collect::<Vec<_>>(),
        vec!["evt-good", "evt-broken"],
        "v2 order is by the locator sort_time"
    );

    // The reported timestamp is untouched: it stays `display_time`, byte
    // identical with what `open` reports for the same event.
    assert_eq!(rows[1].event_unix_ms, 1_767_434_999_000);

    // The v1 sort really does disagree — without this the test could pass
    // against an implementation that never distinguished the two keys.
    let mut v1_rows = vec![broken, good];
    ClickHouseConversationRepository::sort_search_mcp_event_rows(&mut v1_rows);
    assert_eq!(
        v1_rows
            .iter()
            .map(|row| row.event_uid.as_str())
            .collect::<Vec<_>>(),
        vec!["evt-broken", "evt-good"],
        "the two sort keys must genuinely disagree on this fixture, or it \
         proves nothing"
    );
}

#[test]
fn sort_mcp_search_rows_uses_timestamp_before_event_uid_tiebreaker() {
    let mut rows = vec![
        sample_mcp_search_row("evt-a", 4.0, 100),
        sample_mcp_search_row("evt-b", 4.0, 300),
        sample_mcp_search_row("evt-c", 5.0, 50),
        sample_mcp_search_row("evt-d", 4.0, 300),
    ];

    ClickHouseConversationRepository::sort_search_mcp_event_rows(&mut rows);

    let ids = rows
        .iter()
        .map(|row| row.event_uid.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["evt-c", "evt-b", "evt-d", "evt-a"]);
}

#[test]
fn file_attention_merge_dedupes_by_tool_row_identity_and_sorts_once() {
    let exact = sample_file_attention_touch("s1", "call-1", "event-a", Some(100), 1);
    let duplicate_fallback = sample_file_attention_touch("s1", "call-1", "event-a", Some(100), 1);
    let newer_fallback = sample_file_attention_touch("s2", "call-2", "event-b", Some(200), 1);
    let untimed = sample_file_attention_touch("s3", "call-3", "event-c", None, 0);

    let merged =
        merge_file_attention_touches(vec![exact, duplicate_fallback, untimed, newer_fallback], 10);
    let ids = merged
        .iter()
        .map(|touch| touch.event_uid.as_str())
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["event-b", "event-a", "event-c"]);
}

#[test]
fn file_attention_merge_preserves_exact_row_when_fallback_duplicates_it() {
    let mut exact = sample_file_attention_touch("s1", "call-1", "event-a", Some(100), 1);
    exact.matched_path = "/normalized/src/lib.rs".to_string();
    let mut fallback = sample_file_attention_touch("s1", "call-1", "event-a", Some(100), 1);
    fallback.matched_path = "/fallback/src/lib.rs".to_string();

    let merged = merge_file_attention_touches(vec![exact, fallback], 10);

    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].matched_path, "/normalized/src/lib.rs");
}

#[test]
fn prewarm_query_filter_rejects_single_term_queries() {
    assert!(!ClickHouseConversationRepository::is_safe_mcp_prewarm_query_with_max_terms("the", 32));
    assert!(
        !ClickHouseConversationRepository::is_safe_mcp_prewarm_query_with_max_terms("error", 32)
    );
    assert!(
        ClickHouseConversationRepository::is_safe_mcp_prewarm_query_with_max_terms(
            "file directory path config",
            32
        )
    );
}

#[test]
fn search_doc_filters_exclude_codex_by_flag() {
    let mut row = sample_search_doc();
    row.has_codex_mcp = 1;
    assert!(
        !ClickHouseConversationRepository::passes_search_doc_filters(
            &row, false, None, true, None, None
        )
    );
}

#[test]
fn search_doc_filters_exclude_bare_and_qwen_qualified_moraine_tools() {
    let mut row = sample_search_doc();
    for leaf in moraine_clickhouse::mcp_tool_names::INTERNAL_TOOL_NAMES {
        for name in [
            (*leaf).to_string(),
            format!("mcp__moraine__{leaf}"),
            format!("MCP__MORAINE__{}", leaf.to_ascii_uppercase()),
        ] {
            row.name = name.clone();
            assert!(
                !ClickHouseConversationRepository::passes_search_doc_filters(
                    &row, false, None, true, None, None
                ),
                "{name} should be treated as an internal MCP tool"
            );
        }
    }

    for name in [
        "mcp__other__search_sessions",
        "mcp__moraine__unrelated",
        "mcp__moraine__open__extra",
    ] {
        row.name = name.to_string();
        assert!(
            ClickHouseConversationRepository::passes_search_doc_filters(
                &row, false, None, true, None, None
            ),
            "{name} must remain an ordinary tool"
        );
    }
}

#[test]
fn mode_sql_uses_shared_structured_mcp_tool_predicate() {
    let sql = ClickHouseConversationRepository::mode_aggregate_sql();
    assert!(sql.contains("splitByString('__', lowerUTF8(trimBoth(tool_name)))"));
    assert!(sql.contains("= 'moraine'"));
    assert!(sql.contains("'search_sessions'"));
    assert!(sql.contains("'mcp_internal'"));
}

#[test]
fn search_doc_filters_event_kinds_override_include_tool_toggle() {
    let mut row = sample_search_doc();
    row.event_class = "tool_result".to_string();
    row.payload_type = "tool_result".to_string();

    assert!(ClickHouseConversationRepository::passes_search_doc_filters(
        &row,
        false,
        Some(&[SearchEventKind::ToolResult]),
        false,
        None,
        None
    ));
    assert!(
        !ClickHouseConversationRepository::passes_search_doc_filters(
            &row,
            true,
            Some(&[SearchEventKind::Message]),
            false,
            None,
            None
        )
    );
}

#[test]
fn search_doc_filters_map_event_msg_reasoning() {
    let mut row = sample_search_doc();
    row.event_class = "event_msg".to_string();
    row.payload_type = "agent_reasoning".to_string();

    assert!(ClickHouseConversationRepository::passes_search_doc_filters(
        &row,
        true,
        Some(&[SearchEventKind::Reasoning]),
        false,
        None,
        None
    ));
    assert!(
        !ClickHouseConversationRepository::passes_search_doc_filters(
            &row,
            true,
            Some(&[SearchEventKind::Message]),
            false,
            None,
            None
        )
    );
}

#[test]
fn normalize_event_kinds_rejects_empty_lists() {
    let result = ClickHouseConversationRepository::normalize_event_kinds(Some(vec![]));
    assert!(result.is_err());
}

#[test]
fn normalize_event_kinds_sorts_and_deduplicates() {
    let normalized = ClickHouseConversationRepository::normalize_event_kinds(Some(vec![
        SearchEventKind::ToolResult,
        SearchEventKind::Message,
        SearchEventKind::ToolResult,
    ]))
    .expect("normalize should succeed")
    .expect("normalized kinds should be present");

    assert_eq!(
        normalized,
        vec![SearchEventKind::Message, SearchEventKind::ToolResult]
    );
}

#[test]
fn dedupe_search_rows_prefers_message_over_event_msg_mirror() {
    let rows = vec![
        sample_search_row(
            "uid-event-msg",
            "sess-a",
            "event_msg",
            "agent_message",
            "assistant",
            "Short answer: no",
            18.26,
            3,
        ),
        sample_search_row(
            "uid-message",
            "sess-a",
            "message",
            "message",
            "assistant",
            "Short  answer:\nno",
            18.26,
            3,
        ),
    ];

    let deduped = ClickHouseConversationRepository::dedupe_search_rows(rows, 5);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].event_uid, "uid-message");
    assert_eq!(deduped[0].event_class, "message");
}

#[test]
fn dedupe_search_rows_never_collapses_mirrors_from_different_hosts() {
    let first = sample_search_row(
        "shared-uid",
        "sess-a",
        "event_msg",
        "agent_message",
        "assistant",
        "same answer",
        18.26,
        3,
    );
    let mut second = sample_search_row(
        "shared-uid",
        "sess-a",
        "message",
        "message",
        "assistant",
        "same answer",
        18.26,
        3,
    );
    second.source_host = "host-b".to_string();

    let deduped = ClickHouseConversationRepository::dedupe_search_rows(vec![first, second], 5);
    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].source_host, "host-a");
    assert_eq!(deduped[1].source_host, "host-b");
}

#[test]
fn dedupe_search_rows_fills_limit_after_collapsing_mirrors() {
    let rows = vec![
        sample_search_row(
            "uid-event-msg",
            "sess-a",
            "event_msg",
            "agent_message",
            "assistant",
            "same answer",
            18.26,
            3,
        ),
        sample_search_row(
            "uid-message",
            "sess-a",
            "message",
            "message",
            "assistant",
            "same answer",
            18.26,
            3,
        ),
        sample_search_row(
            "uid-2",
            "sess-b",
            "message",
            "message",
            "assistant",
            "different answer 2",
            17.00,
            2,
        ),
        sample_search_row(
            "uid-3",
            "sess-c",
            "message",
            "message",
            "assistant",
            "different answer 3",
            16.00,
            2,
        ),
    ];

    let deduped = ClickHouseConversationRepository::dedupe_search_rows(rows, 3);
    assert_eq!(deduped.len(), 3);
    assert_eq!(deduped[0].event_uid, "uid-message");
    assert_eq!(deduped[1].event_uid, "uid-2");
    assert_eq!(deduped[2].event_uid, "uid-3");
}

#[test]
fn dedupe_search_rows_does_not_collapse_same_kind_hits() {
    let rows = vec![
        sample_search_row(
            "uid-1",
            "sess-a",
            "message",
            "message",
            "assistant",
            "same text",
            10.0,
            2,
        ),
        sample_search_row(
            "uid-2",
            "sess-a",
            "message",
            "message",
            "assistant",
            "same text",
            10.0,
            2,
        ),
    ];

    let deduped = ClickHouseConversationRepository::dedupe_search_rows(rows, 5);
    assert_eq!(deduped.len(), 2);
}

#[test]
fn dedupe_search_rows_prefers_reasoning_over_event_msg_reasoning_mirror() {
    let rows = vec![
        sample_search_row(
            "uid-event-msg-reasoning",
            "sess-a",
            "event_msg",
            "agent_reasoning",
            "assistant",
            "Let me think about this",
            12.50,
            2,
        ),
        sample_search_row(
            "uid-reasoning",
            "sess-a",
            "reasoning",
            "reasoning",
            "assistant",
            "Let me think about this",
            12.50,
            2,
        ),
    ];

    let deduped = ClickHouseConversationRepository::dedupe_search_rows(rows, 5);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].event_uid, "uid-reasoning");
    assert_eq!(deduped[0].event_class, "reasoning");
}

#[test]
fn dedupe_search_rows_reasoning_mirrors_do_not_collapse_with_messages() {
    let rows = vec![
        sample_search_row(
            "uid-reasoning",
            "sess-a",
            "reasoning",
            "reasoning",
            "assistant",
            "same text",
            10.0,
            2,
        ),
        sample_search_row(
            "uid-message",
            "sess-a",
            "message",
            "message",
            "assistant",
            "same text",
            10.0,
            2,
        ),
    ];

    let deduped = ClickHouseConversationRepository::dedupe_search_rows(rows, 5);
    assert_eq!(deduped.len(), 2);
}

#[test]
fn dedupe_mcp_search_rows_collapses_equivalent_events_and_fills_limit() {
    let mut duplicate = sample_mcp_search_row("uid-duplicate", 18.0, 1_777_000_000_000);
    duplicate.turn_seq = 2;
    duplicate.text_content = "byte-identical response".to_string();
    let mut canonical = duplicate.clone();
    canonical.event_uid = "uid-canonical".to_string();
    let mut second = sample_mcp_search_row("uid-second", 17.0, 1_777_000_001_000);
    second.turn_seq = 2;
    second.text_content = "distinct response".to_string();
    let mut third = sample_mcp_search_row("uid-third", 16.0, 1_777_000_002_000);
    third.turn_seq = 3;
    third.text_content = "another distinct response".to_string();

    let deduped = ClickHouseConversationRepository::dedupe_mcp_search_rows(
        vec![duplicate, canonical, second, third],
        3,
    );

    assert_eq!(deduped.len(), 3);
    assert_eq!(deduped[0].event_uid, "uid-duplicate");
    assert_eq!(deduped[1].event_uid, "uid-second");
    assert_eq!(deduped[2].event_uid, "uid-third");
}

#[test]
fn dedupe_mcp_search_rows_collapses_codex_final_answer_mirror_with_near_ms_timestamp() {
    let (response_item, event_msg) = sample_codex_final_answer_mirror_rows(3);
    let deduped = ClickHouseConversationRepository::dedupe_mcp_search_rows(
        vec![response_item.clone(), event_msg.clone()],
        2,
    );
    assert_eq!(deduped.len(), 1);

    let reversed =
        ClickHouseConversationRepository::dedupe_mcp_search_rows(vec![event_msg, response_item], 2);
    assert_eq!(reversed.len(), 1);

    let (response_item, event_msg) = sample_codex_final_answer_mirror_rows(
        CODEX_FINAL_ANSWER_MIRROR_MAX_TIMESTAMP_DELTA_MS as i64,
    );
    let at_boundary =
        ClickHouseConversationRepository::dedupe_mcp_search_rows(vec![response_item, event_msg], 2);
    assert_eq!(at_boundary.len(), 1);
}

#[test]
fn dedupe_mcp_search_rows_preserves_non_mirror_codex_final_answers() {
    let (response_item, event_msg) = sample_codex_final_answer_mirror_rows(3);
    let assert_distinct = |candidate: SearchMcpEventRow| {
        let rows = ClickHouseConversationRepository::dedupe_mcp_search_rows(
            vec![response_item.clone(), candidate],
            2,
        );
        assert_eq!(rows.len(), 2);
    };

    let mut same_representation = event_msg.clone();
    same_representation.event_class = "message".to_string();
    same_representation.payload_type = "message".to_string();
    assert_distinct(same_representation);

    let mut different_session = event_msg.clone();
    different_session.session_id = "session-2".to_string();
    assert_distinct(different_session);

    let mut different_host = event_msg.clone();
    different_host.source_host = "host-b".to_string();
    assert_distinct(different_host);

    let mut different_turn = event_msg.clone();
    different_turn.turn_seq += 1;
    assert_distinct(different_turn);

    let mut not_final = event_msg.clone();
    not_final.payload_phase.clear();
    assert_distinct(not_final);

    let mut different_source_name = event_msg.clone();
    different_source_name.source_name = "other-source".to_string();
    assert_distinct(different_source_name);

    let mut different_source_file = event_msg.clone();
    different_source_file.source_ref = "/tmp/other.jsonl:7:42".to_string();
    assert_distinct(different_source_file);

    let mut different_generation = event_msg.clone();
    different_generation.source_ref = "/tmp/session:with-colon.jsonl:8:42".to_string();
    assert_distinct(different_generation);

    let mut non_adjacent_source_line = event_msg.clone();
    non_adjacent_source_line.source_ref = "/tmp/session:with-colon.jsonl:7:43".to_string();
    assert_distinct(non_adjacent_source_line);

    let mut non_adjacent_event_order = event_msg.clone();
    non_adjacent_event_order.event_order += 1;
    assert_distinct(non_adjacent_event_order);

    let mut malformed_source_ref = event_msg.clone();
    malformed_source_ref.source_ref = "malformed".to_string();
    assert_distinct(malformed_source_ref);

    let (_, outside_timestamp_bound) = sample_codex_final_answer_mirror_rows(
        CODEX_FINAL_ANSWER_MIRROR_MAX_TIMESTAMP_DELTA_MS as i64 + 1,
    );
    assert_distinct(outside_timestamp_bound);

    let mut different_type = event_msg.clone();
    different_type.mcp_event_type = "reasoning".to_string();
    assert_distinct(different_type);

    let mut different_content = event_msg;
    different_content.text_content_digest = "other-digest".to_string();
    assert_distinct(different_content);
}

#[test]
fn dedupe_mcp_search_rows_preserves_distinct_same_turn_events() {
    let mut base = sample_mcp_search_row("uid-base", 18.0, 1_777_000_000_000);
    base.turn_seq = 2;
    base.text_content = "same response".to_string();

    let mut different_timestamp = base.clone();
    different_timestamp.event_uid = "uid-timestamp".to_string();
    different_timestamp.event_unix_ms += 1;
    let mut different_type = base.clone();
    different_type.event_uid = "uid-type".to_string();
    different_type.mcp_event_type = "reasoning".to_string();
    let mut different_content = base.clone();
    different_content.event_uid = "uid-content".to_string();
    different_content.text_content = "same response with more".to_string();
    different_content.text_content_digest = "digest-same-response-with-more".to_string();

    let deduped = ClickHouseConversationRepository::dedupe_mcp_search_rows(
        vec![base, different_timestamp, different_type, different_content],
        4,
    );

    assert_eq!(deduped.len(), 4);
}

#[test]
fn dedupe_mcp_search_rows_uses_full_content_digest_beyond_preview() {
    let shared_prefix = "x".repeat(1_000);
    let mut first = sample_mcp_search_row("uid-first", 18.0, 1_777_000_000_000);
    first.turn_seq = 2;
    first.text_content = shared_prefix.clone();
    first.text_content_digest = "digest-full-content-a".to_string();
    let mut second = first.clone();
    second.event_uid = "uid-second".to_string();
    second.text_content_digest = "digest-full-content-b".to_string();

    let deduped = ClickHouseConversationRepository::dedupe_mcp_search_rows(vec![first, second], 2);

    assert_eq!(deduped.len(), 2);
}

#[test]
fn mcp_internal_classifier_covers_every_public_retrieval_tool() {
    for name in [
        "search",
        "search_sessions",
        "open",
        "list_sessions",
        "file_attention",
    ] {
        assert!(
            ClickHouseConversationRepository::is_mcp_internal_tool_name(name),
            "{name} must classify as mcp_internal"
        );
        assert!(
            ClickHouseConversationRepository::mode_aggregate_sql().contains(&format!("'{name}'")),
            "{name} must be present in SQL mode classification"
        );
    }
    assert!(!ClickHouseConversationRepository::is_mcp_internal_tool_name("read_file"));
}

#[test]
fn low_information_system_event_classifier_targets_open_noise() {
    assert!(
        ClickHouseConversationRepository::is_low_information_system_event("system", "progress")
    );
    assert!(
        ClickHouseConversationRepository::is_low_information_system_event(
            "SYSTEM",
            "file_history_snapshot"
        )
    );
    assert!(ClickHouseConversationRepository::is_low_information_system_event("system", "system"));
    assert!(
        !ClickHouseConversationRepository::is_low_information_system_event("assistant", "progress")
    );
    assert!(
        !ClickHouseConversationRepository::is_low_information_system_event("system", "reasoning")
    );
}

#[test]
fn open_context_filter_clause_respects_include_system_events_flag() {
    assert_eq!(
        ClickHouseConversationRepository::open_context_filter_clause(true),
        ""
    );
    let filtered_clause = ClickHouseConversationRepository::open_context_filter_clause(false);
    assert!(filtered_clause.contains("progress"));
    assert!(filtered_clause.contains("file_history_snapshot"));
    assert!(filtered_clause.contains("lowerUTF8(actor_role) = 'system'"));
}

#[test]
fn bm25_idf_treats_identical_uids_on_two_hosts_as_two_documents() {
    let host_qualified = ClickHouseConversationRepository::bm25_idf(2, 2);
    let uid_only_bug = ClickHouseConversationRepository::bm25_idf(2, 1);

    assert!((host_qualified - 1.2_f64.ln()).abs() < 1e-12);
    assert!((uid_only_bug - 2.0_f64.ln()).abs() < 1e-12);
    assert!(host_qualified < uid_only_bug);
}

// --- issue #600: typed budget-error classification at the repo boundary ---

fn interactive_test_budget() -> moraine_config::ValidatedQueryBudget {
    moraine_config::ValidatedQueryBudgets::from_config(
        &moraine_config::QueryBudgetsConfig::default(),
    )
    .expect("default budgets validate")
    .interactive
}

#[test]
fn classify_backend_error_maps_envelope_deadline_to_deadline_exceeded() {
    let error = anyhow::Error::new(EnvelopeError::DeadlineExpired {
        budget: Duration::from_secs(15),
    })
    .context("outer repository context");
    match classify_backend_error(error) {
        RepoError::DeadlineExceeded { budget_note } => {
            assert!(
                budget_note.contains("15.000"),
                "note should carry the budget: {budget_note}"
            );
        }
        other => panic!("expected DeadlineExceeded, got {other:?}"),
    }
}

#[test]
fn classify_backend_error_maps_cap_and_allowance_to_resource_exhausted() {
    let cap = anyhow::Error::new(EnvelopeError::StatementCapExceeded { cap: 4 });
    assert!(matches!(
        classify_backend_error(cap),
        RepoError::ResourceExhausted { .. }
    ));

    let allowance = anyhow::Error::new(EnvelopeError::AllowanceExhausted {
        resource: moraine_clickhouse::AllowanceResource::Rows,
        budget: 100,
    });
    match classify_backend_error(allowance) {
        RepoError::ResourceExhausted { budget_note } => {
            assert!(
                budget_note.contains("read_rows"),
                "note should name the exhausted resource: {budget_note}"
            );
        }
        other => panic!("expected ResourceExhausted, got {other:?}"),
    }
}

#[test]
fn classify_backend_error_keeps_unclassified_failures_as_backend() {
    // A missing envelope is not a budget outcome: pre-flip it cannot happen
    // at classification time (the statement ran unenveloped), and post-flip
    // it is a wiring bug — either way it stays an opaque backend error.
    let missing = anyhow::Error::new(EnvelopeError::Missing);
    assert!(matches!(
        classify_backend_error(missing),
        RepoError::Backend(_)
    ));

    let plain = anyhow::anyhow!("clickhouse returned 500 Internal Server Error: Code: 60");
    match classify_backend_error(plain) {
        RepoError::Backend(message) => assert!(message.contains("Code: 60")),
        other => panic!("expected Backend, got {other:?}"),
    }
}

#[tokio::test]
async fn classify_backend_error_is_stable_inside_an_active_envelope_scope() {
    let budget = interactive_test_budget();
    let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
    envelope
        .scope(async move {
            // Local admission errors keep their own budget text under scope().
            let error = anyhow::Error::new(EnvelopeError::DeadlineExpired {
                budget: Duration::from_secs(15),
            });
            match classify_backend_error(error) {
                RepoError::DeadlineExceeded { budget_note } => {
                    assert!(budget_note.contains("deadline expired"));
                }
                other => panic!("expected DeadlineExceeded, got {other:?}"),
            }
            // Errors without a typed budget root stay opaque Backend errors
            // even while an envelope is active (no false positives).
            let unrelated = anyhow::anyhow!("some transport failure");
            assert!(matches!(
                classify_backend_error(unrelated),
                RepoError::Backend(_)
            ));
        })
        .await;
}
