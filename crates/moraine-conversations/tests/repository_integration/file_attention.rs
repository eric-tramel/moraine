use super::*;

fn file_attention_query(execution_budget_secs: u64) -> FileAttentionQuery {
    FileAttentionQuery {
        cancellation_token: "test-file-attention-token".to_string(),
        rel: "crates/foo.rs".to_string(),
        normalized_project_id: Some("project-a".to_string()),
        normalized_project_roots: vec!["/worktree-a".to_string()],
        derive_legacy_roots: true,
        apply_project_scope: true,
        start_unix_ms: None,
        end_unix_ms: None,
        harness: None,
        source_name: None,
        tool: None,
        mutations_only: false,
        max_rows: 10,
        execution_budget_secs,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_clamps_its_query_budget_to_the_request_deadline() {
    let (repo, state) = build_repo().await;

    // Interactive envelope with a 2s deadline: tighter than the tool's own
    // 4s execution budget, so the envelope's remaining deadline must win the
    // transport min-merge on every statement.
    let budget = interactive_test_budget(2.0);
    QueryEnvelope::new("request", QueryClass::Interactive, &budget)
        .scope(ConversationRepository::file_attention(
            &repo,
            file_attention_query(4),
        ))
        .await
        .expect("deadline-scoped file attention succeeds");

    let request_params = state.request_params.lock().expect("request params lock");
    let deadline_params = request_params
        .iter()
        .filter(|params| params.contains_key("max_execution_time"))
        .collect::<Vec<_>>();
    assert!(!deadline_params.is_empty());
    for params in deadline_params {
        let remaining = params["max_execution_time"]
            .parse::<f64>()
            .expect("numeric remaining ClickHouse deadline");
        assert!(remaining > 0.0 && remaining <= 2.0);
        assert_eq!(params["timeout_overflow_mode"], "throw");
    }
    drop(request_params);

    let snapshot_queries = state
        .publication_snapshot_queries
        .lock()
        .expect("publication snapshot query lock");
    assert_eq!(snapshot_queries.len(), 2);
    assert!(snapshot_queries[0].contains("moraine:publication_snapshot:capture"));
    assert!(snapshot_queries[1].contains("moraine:publication_snapshot:revalidate"));
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_narrows_the_active_envelope_to_its_execution_budget() {
    let (repo, state) = build_repo().await;

    // Generous 30s envelope, 2s tool budget: scope_narrowed must tighten the
    // envelope deadline for the WHOLE operation, so even statements that
    // carry no caller max_execution_time of their own (the durable
    // project-roots INSERT) get the <=2s server deadline. Ids and budgets
    // stay the parent's — narrowing never resets.
    let budget = interactive_test_budget(30.0);
    let envelope = QueryEnvelope::new("request", QueryClass::Interactive, &budget);
    let request_id = envelope.request_id().to_string();
    envelope
        .scope(ConversationRepository::file_attention(
            &repo,
            file_attention_query(2),
        ))
        .await
        .expect("envelope-scoped file attention succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let roots_write_index = queries
        .iter()
        .position(|query| query.starts_with("INSERT INTO `moraine`.`file_attention_project_roots`"))
        .expect("durable project-roots write should be captured");

    let request_params = state.request_params.lock().expect("request params lock");
    assert_eq!(request_params.len(), queries.len());
    let child_prefix = format!("{request_id}-");
    for (index, params) in request_params.iter().enumerate() {
        let remaining = params["max_execution_time"]
            .parse::<f64>()
            .expect("numeric remaining ClickHouse deadline");
        assert!(
            remaining > 0.0 && remaining <= 2.0,
            "statement {index} escaped the narrowed deadline: {remaining}"
        );
        assert!(
            params["query_id"].starts_with(&child_prefix),
            "statement {index} must carry the envelope's child id, got {}",
            params["query_id"]
        );
    }
    // The roots INSERT proves the narrowing (it has no caller-supplied cap).
    let insert_remaining = request_params[roots_write_index]["max_execution_time"]
        .parse::<f64>()
        .expect("numeric INSERT deadline");
    assert!(insert_remaining <= 2.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_query_delegates_to_the_bounded_transport_kill() {
    let (repo, state) = build_repo().await;

    ConversationRepository::cancel_query(&repo, "moraine-request-77-42-0")
        .await
        .expect("cancel_query succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let kill_index = queries
        .iter()
        .position(|query| query.contains("KILL QUERY"))
        .expect("KILL statement should be captured");
    assert!(
        queries[kill_index].contains(
            "query_id = 'moraine-request-77-42-0' OR startsWith(query_id, 'moraine-request-77-42-0-') SYNC"
        ),
        "prefix contract must be preserved: {}",
        queries[kill_index]
    );

    // The KILL itself runs under an Administrative-class envelope: its own
    // query id and a finite server deadline from the administrative budget.
    let request_params = state.request_params.lock().expect("request params lock");
    let params = &request_params[kill_index];
    assert!(
        params["query_id"].starts_with("moraine-kill-"),
        "KILL must carry its own administrative query id, got {}",
        params["query_id"]
    );
    let kill_deadline = params["max_execution_time"]
        .parse::<f64>()
        .expect("numeric KILL deadline");
    assert!(
        kill_deadline > 0.0 && kill_deadline <= 5.0,
        "KILL deadline must be finite and within the administrative budget: {kill_deadline}"
    );
    assert_eq!(params["timeout_overflow_mode"], "throw");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_query_with_blank_id_issues_no_statement() {
    let (repo, state) = build_repo().await;

    ConversationRepository::cancel_query(&repo, "   ")
        .await
        .expect("blank cancel is a no-op");

    let queries = state.queries.lock().expect("queries lock");
    assert!(
        queries.iter().all(|query| !query.contains("KILL QUERY")),
        "blank id must not reach the server"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_merges_normalized_exact_lookup_with_suffix_fallback() {
    let (repo, state) = build_repo().await;

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-normalized".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: Some("codex".to_string()),
            source_name: Some("codex".to_string()),
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        })
        .await
        .expect("file_attention query succeeds");

    assert_eq!(touches.len(), 2);
    assert_eq!(touches[0].session_id, "sess-normalized");
    assert_eq!(touches[0].event_uid, "evt-normalized");
    assert_eq!(touches[0].tool_call_id, "call-normalized");
    assert_eq!(touches[0].matched_path, "/worktree-a/crates/foo.rs");
    assert_eq!(touches[0].worktree_root, "/worktree-a");
    assert_eq!(touches[0].match_kind, "path_suffix");
    assert_eq!(touches[1].session_id, "sess-legacy");
    assert_eq!(touches[1].event_uid, "evt-legacy");
    assert_eq!(touches[1].match_kind, "path_suffix");

    let queries = state.queries.lock().expect("queries lock").clone();
    let exact_query = queries
        .iter()
        .find(|query| query.contains("repo_rel_path = 'crates/foo.rs'"))
        .expect("normalized exact file_attention query should be captured");
    assert!(exact_query.contains("project_id = 'project-a'"));
    assert!(exact_query.contains("tool_phase = 'request'"));
    assert!(exact_query.contains("harness = 'codex'"));
    assert!(exact_query.contains("e.source_name = 'codex'"));
    assert!(
        !exact_query.contains("JSONExtractString(input_json"),
        "exact lookup should not depend on legacy JSON suffix extraction: {exact_query}"
    );
    assert!(
        exact_query.contains("position(ti.worktree_root, ';') = 0"),
        "normalized roots must still satisfy the one-path invariant: {exact_query}"
    );
    for fragment in ["char(0)", "char(10)", "char(13)", "'`'"] {
        assert!(
            exact_query.contains(fragment),
            "normalized root guard must reject {fragment}: {exact_query}"
        );
    }

    let fallback_query = queries
        .iter()
        .find(|query| query.contains("JSONExtractString(input_json, 'path')"))
        .expect("Tier-0 suffix file_attention query should be captured");
    assert!(fallback_query.contains("crates/foo.rs"));
    assert!(fallback_query.contains("harness = 'codex'"));
    assert!(fallback_query.contains("e.source_name = 'codex'"));
    assert!(
        fallback_query.contains("ti.project_id = 'project-a'")
            && fallback_query.contains(
                "ti.project_id != '' AND NOT startsWith(ti.project_id, 'git:')"
            )
            && fallback_query.contains("has(project_roots, ti.worktree_root)")
            && fallback_query.contains("ti.project_id = '' AND e.project_id = 'project-a'")
            && fallback_query.contains("legacy_candidate_root")
            && fallback_query.contains("verified_project_root")
            && fallback_query.contains(
                "arrayFirst(root -> legacy_candidate_root = root, project_roots)"
            ),
        "project scope must admit current IDs and root-verified pre-digest rows without widening: {fallback_query}"
    );
    assert!(
        fallback_query.contains("AS legacy_scalar_paths")
            && fallback_query.contains("AS legacy_scalar_path")
            && fallback_query.contains("countMatches(ti.input_json")
            && fallback_query.contains("|command|cmd)")
            && fallback_query
                .contains("arrayCount(path -> path != '', legacy_scalar_paths) = 1")
            && fallback_query.contains(
                "= concat(ifNull(e.cwd, ''), '/', 'crates/foo.rs')"
            ),
        "legacy root recovery must require exactly one top-level scalar path and no second structured or shell path evidence: {fallback_query}"
    );
    assert!(
        fallback_query.contains("position(legacy_scalar_path")
            && fallback_query.contains("position(ifNull(e.cwd, ''), ';') = 0"),
        "legacy compound path captures must not become roots: {fallback_query}"
    );
    assert!(
        !fallback_query.contains("substring(matched_path"),
        "an arbitrary suffix prefix must not be reported as a repository root: {fallback_query}"
    );
    for fragment in ["char(0)", "char(10)", "char(13)", "'`'"] {
        assert!(
            fallback_query.contains(fragment),
            "legacy root guard must reject {fragment}: {fallback_query}"
        );
    }

    let snapshot_queries = state
        .publication_snapshot_queries
        .lock()
        .expect("publication snapshot query lock");
    assert_eq!(snapshot_queries.len(), 2);
    assert!(snapshot_queries[0].contains("moraine:publication_snapshot:capture"));
    assert!(snapshot_queries[1].contains("moraine:publication_snapshot:revalidate"));
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_recovers_durable_only_deleted_root() {
    let responses = vec![
        ScriptedResponse::rows(&["repo_rel_path = 'crates/foo.rs'"], json!([])),
        ScriptedResponse::rows(
            &[
                "NOT startsWith(ti.project_id, 'git:')",
                "has(project_roots, ti.worktree_root)",
                "'/worktree-a'",
                "arrayFirst(root -> legacy_candidate_root = root, project_roots)",
                "e.project_id = '' AND verified_project_root != ''",
                "SELECT groupArray(worktree_root) FROM `moraine`.`file_attention_project_roots` FINAL",
                "WHERE project_id = 'git:new-project-id' AND worktree_root != ''",
            ],
            json!([{
                "session_id": "sess-pre-digest",
                "event_uid": "evt-pre-digest",
                "tool_call_id": "call-pre-digest",
                "harness": "codex",
                "tool_name": "read",
                "tool_phase": "request",
                "matched_path": "/worktree-b/crates/foo.rs",
                "match_kind": "path_suffix",
                "worktree_root": "/worktree-b",
                "cwd": "/worktree-b",
                "event_unix_ms": 1769940000000_i64,
                "event_order": 10_u64,
                "turn_seq": 1_u32,
                "input_preview": "{\"path\":\"crates/foo.rs\"}",
                "output_preview": ""
            }]),
        ),
        ScriptedResponse::raw(
            &[
                "INSERT INTO `moraine`.`file_attention_project_roots`",
                "arrayJoin(['/worktree-a'])",
            ],
            "",
        ),
    ];
    let (repo, _state) = build_scripted_repo(responses).await;

    let touches = ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-pre-digest".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("git:new-project-id".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("durably mapped deleted root remains visible during migration");

    assert_eq!(touches.len(), 1);
    assert_eq!(touches[0].session_id, "sess-pre-digest");
    assert_eq!(touches[0].worktree_root, "/worktree-b");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_excludes_unmapped_pruned_legacy_root() {
    let responses = vec![
        ScriptedResponse::rows(&["repo_rel_path = 'crates/foo.rs'"], json!([])),
        ScriptedResponse::rows(
            &[
                "has(project_roots, ti.worktree_root)",
                "SELECT groupArray(worktree_root) FROM `moraine`.`file_attention_project_roots` FINAL",
                "WHERE project_id = 'git:new-project-id' AND worktree_root != ''",
            ],
            json!([]),
        )
        .forbidding(&["'/worktree-b'"]),
        ScriptedResponse::raw(
            &[
                "INSERT INTO `moraine`.`file_attention_project_roots`",
                "arrayJoin(['/worktree-a'])",
            ],
            "",
        ),
    ];
    let (repo, _state) = build_scripted_repo(responses).await;

    let touches = ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-unmapped-pruned".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("git:new-project-id".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("unmappable pruned history must fail closed");

    assert!(touches.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_all_scope_is_the_only_unscoped_widening_path() {
    let (repo, state) = build_repo().await;

    ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-all".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: false,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("all-scope file_attention query succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let exact_query = queries
        .iter()
        .find(|query| query.contains("repo_rel_path = 'crates/foo.rs'"))
        .expect("normalized exact file_attention query should be captured");
    assert!(exact_query.contains("project_id != ''"));
    assert!(!exact_query.contains("project_id = 'project-a'"));

    let fallback_query = queries
        .iter()
        .find(|query| query.contains("JSONExtractString(input_json, 'path')"))
        .expect("Tier-0 suffix file_attention query should be captured");
    assert!(
        !fallback_query.contains("coalesce(nullIf(ti.project_id"),
        "scope=all must not retain the request project predicate: {fallback_query}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_all_scope_keeps_the_configured_server_floor_only() {
    let (repo, state) = build_scoped_repo(&["/work/project"]).await;

    ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-scoped-all".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: false,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("all-scope query on scoped repository succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let attention_queries = queries
        .iter()
        .filter(|query| query.contains("FROM `moraine`.`v_live_tool_io`"))
        .collect::<Vec<_>>();
    assert_eq!(attention_queries.len(), 2);
    for query in attention_queries {
        assert!(query.contains("origin_cwd = '/work/project'"));
        assert!(!query.contains("project_id = 'project-a'"));
        assert!(!query.contains("coalesce(nullIf(ti.project_id"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_all_scope_preserves_legacy_suffix_fallback_on_schema_skew() {
    let responses = vec![
        ScriptedResponse::failure(
            &["repo_rel_path = 'crates/foo.rs'"],
            "Unknown identifier project_id",
        ),
        ScriptedResponse::rows(&["JSONExtractString(input_json, 'path')"], json!([])).forbidding(
            &[
                ", project_id, repo_rel_path, worktree_root",
                "any(project_id) AS project_id",
            ],
        ),
    ];
    let (repo, state) = build_scripted_repo(responses).await;

    let touches = ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-schema-skew-all".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: false,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("legacy all-scope fallback succeeds");

    assert!(touches.is_empty());
    assert_eq!(state.queries.lock().expect("queries lock").len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_stays_closed_on_schema_skew() {
    let responses = vec![ScriptedResponse::failure(
        &["repo_rel_path = 'crates/foo.rs'"],
        "Unknown identifier project_id",
    )];
    let (repo, state) = build_scripted_repo(responses).await;

    let touches = ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-schema-skew-project".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            derive_legacy_roots: true,
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("project-scoped schema-skew query stays closed");

    assert!(touches.is_empty());
    assert_eq!(state.queries.lock().expect("queries lock").len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_without_identity_stays_closed() {
    let (repo, state) = build_repo().await;

    ConversationRepository::file_attention(
        &repo,
        FileAttentionQuery {
            cancellation_token: "test-file-attention-unknown-project".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: None,
            normalized_project_roots: Vec::new(),
            derive_legacy_roots: false,
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            harness: None,
            source_name: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        },
    )
    .await
    .expect("closed project-scope file_attention query succeeds");

    assert!(
        state.queries.lock().expect("queries lock").is_empty(),
        "an unknown request project must stay closed without issuing a backend query"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_query_kills_base_and_fanout_child_query_ids() {
    let (repo, state) = build_repo().await;

    repo.cancel_query("mcp-request-123")
        .await
        .expect("cancel query family");

    let queries = state.queries.lock().expect("queries lock");
    let kill = queries
        .iter()
        .find(|query| query.starts_with("KILL QUERY"))
        .expect("kill query captured");
    assert!(kill.contains("query_id = 'mcp-request-123'"));
    assert!(kill.contains("startsWith(query_id, 'mcp-request-123-')"));
}
