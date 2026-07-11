use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn file_attention_merges_normalized_exact_lookup_with_suffix_fallback() {
    let (repo, state) = build_repo().await;

    let touches = repo
        .file_attention(FileAttentionQuery {
            cancellation_token: "test-file-attention-normalized".to_string(),
            rel: "crates/foo.rs".to_string(),
            normalized_project_id: Some("project-a".to_string()),
            derive_worktree_roots: true,
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
    assert_eq!(touches[1].match_kind, "shell_path");

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

    let fallback_query = queries
        .iter()
        .find(|query| query.contains("JSONExtractString(input_json, 'path')"))
        .expect("Tier-0 suffix file_attention query should be captured");
    assert!(fallback_query.contains("crates/foo.rs"));
}
