use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_uses_the_owner_absolute_deadline() {
    let (repo, state) = build_repo().await;

    let owner = QueryOwner::with_deadline(
        &repo.runtime(),
        QueryWorkload::Mcp,
        tokio::time::Instant::now() + Duration::from_secs(2),
    )
    .expect("deadline owner");
    owner
        .scope(repo.unowned().file_attention(FileAttentionQuery {
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
        }))
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
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_merges_normalized_exact_lookup_with_suffix_fallback() {
    let (repo, state) = build_repo().await;

    let touches = repo
        .file_attention(FileAttentionQuery {
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
    assert!(
        !exact_query.contains("FINAL"),
        "exact file_attention must resolve versions with argMax, not FINAL: {exact_query}"
    );
    assert!(
        exact_query.contains("argMax(tuple(session_id, project_id, repo_rel_path, worktree_root), (event_version, ingested_at))")
            && exact_query.contains("HAVING argMax(toUInt8(project_id = 'project-a' AND repo_rel_path = 'crates/foo.rs'), (event_version, ingested_at))"),
        "locator must pick the current version, then re-apply the filter to it: {exact_query}"
    );
    assert!(
        exact_query.contains("PREWHERE session_id IN (")
            && exact_query.contains("(session_id, event_uid, event_version) IN (")
            && exact_query.contains("max(event_version) AS win_version")
            && exact_query.contains("argMax(tool_phase, event_version) AS w_tool_phase"),
        "events hydration must use narrow winning keys, then a keyed payload read: {exact_query}"
    );
    assert!(exact_query.contains("project_id = 'project-a'"));
    assert!(exact_query.contains("tool_phase = 'request'"));
    assert!(exact_query.contains("harness = 'codex'"));
    assert!(exact_query.contains("ti.source_name = 'codex'"));
    assert!(
        !exact_query.contains("JSONExtractString(input_json"),
        "exact lookup should not depend on legacy JSON suffix extraction: {exact_query}"
    );
    assert!(
        exact_query.contains("position(ti.worktree_root, ';') = 0"),
        "normalized roots must still satisfy the one-path invariant: {exact_query}"
    );
    assert!(
        !exact_query
            .contains("(session_id, event_uid) IN (SELECT session_id, event_uid FROM matched_ids)"),
        "exact lookup must not use a correlated CTE in a tuple IN predicate: {exact_query}"
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
    assert!(fallback_query.contains("ti.source_name = 'codex'"));
    assert!(
        fallback_query.contains("ti.project_id = 'project-a'")
            && fallback_query.contains(
                "ti.project_id != '' AND NOT startsWith(ti.project_id, 'git:')"
            )
            && fallback_query.contains("has(project_roots, ti.worktree_root)")
            && fallback_query.contains("ti.project_id = '' AND ti.event_project_id = 'project-a'")
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
                "= concat(ifNull(ti.cwd, ''), '/', 'crates/foo.rs')"
            ),
        "legacy root recovery must require exactly one top-level scalar path and no second structured or shell path evidence: {fallback_query}"
    );
    assert!(
        fallback_query.contains("position(legacy_scalar_path")
            && fallback_query.contains("position(ifNull(ti.cwd, ''), ';') = 0"),
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
    let query_ids = state
        .query_ids
        .lock()
        .expect("query ids lock")
        .iter()
        .filter_map(Clone::clone)
        .collect::<Vec<_>>();
    assert_eq!(
        query_ids.len(),
        3,
        "exact, mapping insert, and suffix are owned"
    );
    let logical_prefix = query_ids[0]
        .rsplit_once('-')
        .expect("child query sequence")
        .0;
    assert!(logical_prefix.starts_with("moraine-internal-"));
    assert!(query_ids
        .iter()
        .all(|query_id| query_id.starts_with(logical_prefix)));
    assert_eq!(
        query_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        query_ids.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_recovers_durable_only_deleted_root() {
    let suffix_response = ScriptedResponse::rows(
        &[
            "NOT startsWith(ti.project_id, 'git:')",
            "has(project_roots, ti.worktree_root)",
            "'/worktree-a'",
            "arrayFirst(root -> legacy_candidate_root = root, project_roots)",
            "ti.event_project_id = '' AND verified_project_root != ''",
            "SELECT groupArray(worktree_root) FROM (",
            "FROM `moraine`.`file_attention_project_roots`",
            "WHERE project_id = 'git:new-project-id' AND worktree_root != ''",
            "GROUP BY worktree_root",
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
    );
    let responses = vec![
        ScriptedResponse::rows(&["repo_rel_path = 'crates/foo.rs'"], json!([])),
        ScriptedResponse::raw(
            &[
                "INSERT INTO `moraine`.`file_attention_project_roots`",
                "arrayJoin(['/worktree-a'])",
            ],
            "",
        ),
        suffix_response.clone(),
        suffix_response,
    ];
    let (repo, _state) = build_scripted_repo(responses).await;

    let touches = repo
        .file_attention(FileAttentionQuery {
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
        })
        .await
        .expect("durably mapped deleted root remains visible during migration");

    assert_eq!(touches.len(), 1);
    assert_eq!(touches[0].session_id, "sess-pre-digest");
    assert_eq!(touches[0].worktree_root, "/worktree-b");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_excludes_unmapped_pruned_legacy_root() {
    let suffix_response = ScriptedResponse::rows(
        &[
            "has(project_roots, ti.worktree_root)",
            "SELECT groupArray(worktree_root) FROM (",
            "FROM `moraine`.`file_attention_project_roots`",
            "WHERE project_id = 'git:new-project-id' AND worktree_root != ''",
            "GROUP BY worktree_root",
        ],
        json!([]),
    )
    .forbidding(&["'/worktree-b'"]);
    let responses = vec![
        ScriptedResponse::rows(&["repo_rel_path = 'crates/foo.rs'"], json!([])),
        ScriptedResponse::raw(
            &[
                "INSERT INTO `moraine`.`file_attention_project_roots`",
                "arrayJoin(['/worktree-a'])",
            ],
            "",
        ),
        suffix_response.clone(),
        suffix_response,
    ];
    let (repo, _state) = build_scripted_repo(responses).await;

    let touches = repo
        .file_attention(FileAttentionQuery {
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
        })
        .await
        .expect("unmappable pruned history must fail closed");

    assert!(touches.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_all_scope_is_the_only_unscoped_widening_path() {
    let (repo, state) = build_repo().await;

    repo.file_attention(FileAttentionQuery {
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
    })
    .await
    .expect("all-scope query on scoped repository succeeds");

    let queries = state.queries.lock().expect("queries lock").clone();
    let attention_queries = queries
        .iter()
        .filter(|query| query.contains("moraine_tool_io"))
        .collect::<Vec<_>>();
    assert_eq!(attention_queries.len(), 2);
    for query in attention_queries {
        assert!(query.contains("origin_cwd = '/work/project'"));
        assert!(!query.contains("project_id = 'project-a'"));
        assert!(!query.contains("coalesce(nullIf(ti.project_id"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_all_scope_does_not_replay_after_backend_schema_error() {
    let responses = vec![ScriptedResponse::failure(
        &["repo_rel_path = 'crates/foo.rs'"],
        "Code: 47. DB::Exception: Unknown identifier project_id",
    )];
    let (repo, state) = build_scripted_repo(responses).await;

    let error = repo
        .file_attention(FileAttentionQuery {
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
        })
        .await
        .expect_err("backend schema failure is not replayed");

    assert!(matches!(error, RepoError::Backend(_)));
    assert_script_consumed(&state, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_project_scope_stays_closed_on_schema_skew() {
    let responses = vec![ScriptedResponse::failure(
        &["repo_rel_path = 'crates/foo.rs'"],
        "Code: 47. DB::Exception: Unknown identifier project_id",
    )];
    let (repo, state) = build_scripted_repo(responses).await;

    let touches = repo
        .file_attention(FileAttentionQuery {
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
    })
    .await
    .expect("closed project-scope file_attention query succeeds");

    assert!(
        state.queries.lock().expect("queries lock").is_empty(),
        "an unknown request project must stay closed without issuing a backend query"
    );
}

fn file_attention_shape_query(
    harness: Option<&str>,
    mutations_only: bool,
    apply_project_scope: bool,
) -> FileAttentionQuery {
    FileAttentionQuery {
        rel: "README.md".to_string(),
        normalized_project_id: Some("project-a".to_string()),
        normalized_project_roots: vec!["/worktree-a".to_string()],
        derive_legacy_roots: true,
        apply_project_scope,
        start_unix_ms: Some(1),
        end_unix_ms: Some(2_000_000_000_000),
        harness: harness.map(str::to_string),
        source_name: None,
        tool: Some("edit".to_string()),
        mutations_only,
        max_rows: 10,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_sql_never_uses_final_and_evaluates_each_candidate_set_once() {
    for (harness, mutations_only, apply_project_scope) in [
        (None, false, false),
        (None, true, false),
        (Some("claude-code"), false, false),
        (Some("claude-code"), true, true),
    ] {
        let (repo, state) = build_repo().await;
        repo.file_attention(file_attention_shape_query(
            harness,
            mutations_only,
            apply_project_scope,
        ))
        .await
        .expect("file_attention query succeeds");

        let queries = state.queries.lock().expect("queries lock").clone();
        let attention_queries = queries
            .iter()
            .filter(|query| query.contains("mcp_event_locator"))
            .collect::<Vec<_>>();
        assert_eq!(attention_queries.len(), 2, "exact + suffix: {queries:?}");
        for query in attention_queries {
            assert!(
                !query.contains("FINAL"),
                "file_attention SQL must not use FINAL: {query}"
            );
            // The candidate-session and candidate-key subqueries are rendered
            // identically at every use so ClickHouse builds each IN set once.
            let sessions_sets = query
                .matches("session_id IN (SELECT session_id FROM (SELECT event_uid, cur.1")
                .count();
            assert!(
                sessions_sets >= 2,
                "events and navigation share one session set: {query}"
            );
            assert!(
                query.contains(
                    "(session_id, event_uid) IN (\n  SELECT session_id, event_uid FROM ("
                ) || query.contains(
                    "(session_id, event_uid) IN (\n        SELECT session_id, event_uid FROM ("
                ),
                "candidate key set missing: {query}"
            );
            assert!(
                !query.contains("SELECT * FROM `moraine`.`events`"),
                "payload_json must not be read as SELECT *: {query}"
            );
            if mutations_only {
                assert!(query.contains("NOT IN ('read'"));
                assert!(query.contains("lowerUTF8(w_tool_name) NOT IN"));
            }
            if let Some(harness) = harness {
                assert!(query.contains(&format!("w_harness = '{harness}'")));
            }
        }
    }
}
