use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_merges_normalized_exact_lookup_with_suffix_fallback() {
    let (repo, state) = build_repo().await;

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-normalized".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
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
    assert!(
        fallback_query.contains("ti.project_id = 'project-a'")
            && fallback_query.contains(
                "ti.project_id != '' AND NOT startsWith(ti.project_id, 'git:')"
            )
            && fallback_query.contains("ti.worktree_root IN ('/worktree-a')")
            && fallback_query.contains("ti.project_id = '' AND (e.project_id = 'project-a'")
            && fallback_query.contains(
                "matched_path = concat(ifNull(e.worktree_root, ''), '/', 'crates/foo.rs')"
            ),
        "project scope must admit current IDs and root-verified pre-digest rows without widening: {fallback_query}"
    );
    assert!(
        fallback_query.contains("position(matched_path, ';') = 0"),
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
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_migrates_registered_pre_digest_sibling_root() {
    let responses = vec![
        ScriptedResponse::rows(&["repo_rel_path = 'crates/foo.rs'"], json!([])),
        ScriptedResponse::raw(
            &[
                "INSERT INTO `moraine`.`file_attention_project_roots`",
                "arrayJoin(['/worktree-a', '/worktree-b', '/worktree-''quoted'])",
            ],
            "",
        ),
        ScriptedResponse::rows(
            &[
                "NOT startsWith(ti.project_id, 'git:')",
                "ti.worktree_root IN ('/worktree-a', '/worktree-b', '/worktree-''quoted')",
                "SELECT worktree_root FROM `moraine`.`file_attention_project_roots` FINAL WHERE project_id = 'git:new-project-id'",
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
    ];
    let (repo, _state) = build_scripted_repo(responses).await;

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-pre-digest".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("git:new-project-id".to_string()),
            normalized_project_roots: vec![
                "/worktree-a".to_string(),
                "/worktree-b".to_string(),
                "/worktree-'quoted".to_string(),
            ],
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        })
        .await
        .expect("registered pre-digest sibling row remains visible during migration");

    assert_eq!(touches.len(), 1);
    assert_eq!(touches[0].session_id, "sess-pre-digest");
    assert_eq!(touches[0].worktree_root, "/worktree-b");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_excludes_unmapped_pruned_legacy_root() {
    let responses = vec![
        ScriptedResponse::rows(&["repo_rel_path = 'crates/foo.rs'"], json!([])),
        ScriptedResponse::raw(
            &[
                "INSERT INTO `moraine`.`file_attention_project_roots`",
                "arrayJoin(['/worktree-a'])",
            ],
            "",
        ),
        ScriptedResponse::rows(
            &[
                "ti.worktree_root IN ('/worktree-a')",
                "SELECT worktree_root FROM `moraine`.`file_attention_project_roots` FINAL WHERE project_id = 'git:new-project-id'",
            ],
            json!([]),
        )
        .forbidding(&["'/worktree-b'"]),
    ];
    let (repo, _state) = build_scripted_repo(responses).await;

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-unmapped-pruned".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("git:new-project-id".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        })
        .await
        .expect("unmappable pruned history must fail closed");

    assert!(touches.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_all_scope_is_the_only_unscoped_widening_path() {
    let (repo, state) = build_repo().await;

    repo.file_attention(FileAttentionQuery {
        cancellation_token: "test-file-attention-all".to_string(),
        rel: "crates/foo.rs".to_string(),
        normalized_project_id: Some("project-a".to_string()),
        normalized_project_roots: vec!["/worktree-a".to_string()],
        apply_project_scope: false,
        start_unix_ms: None,
        end_unix_ms: None,
        tool: None,
        mutations_only: false,
        max_rows: 10,
        execution_budget_secs: 3,
    })
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

    repo.file_attention(FileAttentionQuery {
        cancellation_token: "test-file-attention-scoped-all".to_string(),
        rel: "crates/foo.rs".to_string(),
        normalized_project_id: Some("project-a".to_string()),
        normalized_project_roots: vec!["/worktree-a".to_string()],
        apply_project_scope: false,
        start_unix_ms: None,
        end_unix_ms: None,
        tool: None,
        mutations_only: false,
        max_rows: 10,
        execution_budget_secs: 3,
    })
    .await
    .expect("all-scope query on scoped repository succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let attention_queries = queries
        .iter()
        .filter(|query| query.contains("FROM `moraine`.`tool_io` FINAL"))
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

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-schema-skew-all".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            apply_project_scope: false,
            start_unix_ms: None,
            end_unix_ms: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        })
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

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-schema-skew-project".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            normalized_project_roots: vec!["/worktree-a".to_string()],
            apply_project_scope: true,
            start_unix_ms: None,
            end_unix_ms: None,
            tool: None,
            mutations_only: false,
            max_rows: 10,
            execution_budget_secs: 3,
        })
        .await
        .expect("project-scoped schema-skew query stays closed");

    assert!(touches.is_empty());
    assert_eq!(state.queries.lock().expect("queries lock").len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_without_identity_stays_closed() {
    let (repo, state) = build_repo().await;

    repo.file_attention(FileAttentionQuery {
        cancellation_token: "test-file-attention-unknown-project".to_string(),
        rel: "crates/foo.rs".to_string(),
        normalized_project_id: None,
        normalized_project_roots: Vec::new(),
        apply_project_scope: true,
        start_unix_ms: None,
        end_unix_ms: None,
        tool: None,
        mutations_only: false,
        max_rows: 10,
        execution_budget_secs: 3,
    })
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
