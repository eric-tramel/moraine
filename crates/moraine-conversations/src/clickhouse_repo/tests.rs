use super::file_attention::merge_file_attention_touches;
use super::search::{RankedPosting, SearchScoreAccum};
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
        source_ref: "source-ref".to_string(),
        doc_len: 42,
        text_preview: "preview".to_string(),
        text_content: "preview".to_string(),
        payload_json: "{}".to_string(),
        mcp_event_type: "assistant_response".to_string(),
        raw_score,
        matched_terms: 1,
        event_time: String::new(),
        event_unix_ms,
        event_order: 0,
        turn_seq: 0,
    }
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
fn evidence_snippet_prefers_matching_metadata_over_nonmatching_summary() {
    let terms = vec!["quartz".to_string()];
    let snippet = evidence_snippet(
        "General session summary.",
        "{\"metadata\":{\"codename\":\"quartz\"}}",
        &terms,
        80,
    );
    assert_eq!(
        snippet.as_deref(),
        Some("{\"metadata\":{\"codename\":\"quartz\"}}")
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
fn broad_fast_path_term_guard_uses_row_cap_and_corpus_ratio() {
    assert!(
        ClickHouseConversationRepository::term_df_too_broad_for_fast_path(
            TERM_POSTINGS_FAST_PATH_MAX_ROWS_PER_TERM + 1,
            1_000_000
        )
    );
    assert!(ClickHouseConversationRepository::term_df_too_broad_for_fast_path(25_000, 100_000));
    assert!(!ClickHouseConversationRepository::term_df_too_broad_for_fast_path(24_999, 100_000));

    let terms = vec!["the".to_string(), "rare".to_string()];
    let mut df_by_term = HashMap::<String, u64>::new();
    df_by_term.insert("the".to_string(), 25_000);
    df_by_term.insert("rare".to_string(), 12);
    assert!(ClickHouseConversationRepository::has_broad_fast_path_term(
        &terms,
        &df_by_term,
        100_000
    ));
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
fn search_doc_filters_exclude_codex_by_tool_name() {
    let mut row = sample_search_doc();
    for name in ["search", "open", "list_sessions", "file_attention"] {
        row.name = name.to_string();
        assert!(
            !ClickHouseConversationRepository::passes_search_doc_filters(
                &row, false, None, true, None, None
            ),
            "{name} should be treated as an internal MCP tool"
        );
    }
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
fn cached_posting_ranker_matches_full_sort_reference() {
    let posting = |event_uid: &str, doc_len: u32, tf: u16| CachedPostingRow {
        event_uid: event_uid.to_string(),
        doc_len,
        tf,
    };
    let terms = ["alpha", "beta", "gamma"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut alpha_rows = vec![
        posting("u1", 100, 2),
        posting("u2", 120, 1),
        posting("tie-a", 100, 1),
    ];
    let mut beta_rows = vec![
        posting("u1", 100, 1),
        posting("u3", 80, 4),
        posting("tie-b", 100, 1),
    ];
    for doc in 0..300 {
        let event_uid = format!("bulk-{doc:03}");
        alpha_rows.push(posting(&event_uid, 100, 1));
        beta_rows.push(posting(&event_uid, 100, 1));
    }
    let postings_by_term = HashMap::<String, Arc<[CachedPostingRow]>>::from([
        (
            "alpha".to_string(),
            Arc::from(alpha_rows.into_boxed_slice()),
        ),
        ("beta".to_string(), Arc::from(beta_rows.into_boxed_slice())),
        (
            "gamma".to_string(),
            Arc::from(
                vec![
                    posting("u2", 120, 3),
                    posting("u3", 80, 1),
                    posting("tie-a", 100, 1),
                    posting("tie-b", 100, 1),
                ]
                .into_boxed_slice(),
            ),
        ),
    ]);
    let df_by_term = postings_by_term
        .iter()
        .map(|(term, rows)| (term.clone(), rows.len() as u64))
        .collect::<HashMap<_, _>>();
    let docs = 100_u64;
    let avgdl = 100.0;
    let k1 = 1.2;
    let b = 0.75;
    let min_should_match = 2;

    let actual = ClickHouseConversationRepository::rank_cached_postings(
        &terms,
        &postings_by_term,
        &df_by_term,
        docs,
        avgdl,
        k1,
        b,
        min_should_match,
        0.0,
    );

    let mut reference_by_uid = HashMap::<&str, SearchScoreAccum<'_>>::new();
    for (idx, term) in terms.iter().enumerate() {
        let idf = ClickHouseConversationRepository::bm25_idf(docs, df_by_term[term]);
        for row in postings_by_term[term].iter() {
            let entry =
                reference_by_uid
                    .entry(row.event_uid.as_str())
                    .or_insert(SearchScoreAccum {
                        row,
                        score: 0.0,
                        matched_mask: 0,
                    });
            entry.score += idf
                * ClickHouseConversationRepository::bm25_term_score(
                    row.tf,
                    row.doc_len,
                    avgdl,
                    k1,
                    b,
                );
            entry.matched_mask |= 1_u64 << idx;
        }
    }
    let mut expected = reference_by_uid
        .into_values()
        .filter_map(|acc| {
            let matched_terms = u64::from(acc.matched_mask.count_ones());
            (matched_terms >= u64::from(min_should_match)).then_some(RankedPosting {
                row: acc.row,
                score: acc.score,
                matched_terms,
            })
        })
        .collect::<Vec<_>>();
    expected.sort_unstable_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.row.event_uid.cmp(&b.row.event_uid))
    });

    assert_eq!(actual.len(), expected.len());
    assert!(actual.len() > 256, "fixture must exercise partial ordering");
    assert_eq!(
        expected[255].score, expected[256].score,
        "fixture must place a score tie across the partial-order boundary"
    );
    for (actual, expected) in actual.iter().zip(&expected).take(256) {
        assert_eq!(actual.row.event_uid, expected.row.event_uid);
        assert_eq!(actual.matched_terms, expected.matched_terms);
        assert!(
            (actual.score - expected.score).abs() < 1e-12,
            "score mismatch for {}: {} != {}",
            actual.row.event_uid,
            actual.score,
            expected.score
        );
    }
}
