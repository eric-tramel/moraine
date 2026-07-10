use super::*;

const FILE_ATTENTION_READ_TOOL_NAMES_SQL: &str =
    "'read', 'readfile', 'read_file', 'notebookread', 'notebook_read', 'view', 'cat', 'grep', 'rg', 'glob', 'ls', 'list', 'find'";

impl ClickHouseConversationRepository {
    pub async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        self.file_attention_impl(query).await
    }

    /// Tier-0 file-attention query: every captured tool call whose input path
    /// ends with `query.rel`, across every worktree in the connected backend.
    ///
    /// Worktree unification is by construction: matching the repo-relative
    /// tail collapses the main checkout, sibling worktrees, and
    /// agent-isolation worktrees into one result set. This intentionally scans
    /// `tool_io` because `input_json` is not indexed yet; the trace/events
    /// joins are bounded to exact matched `(session_id, event_uid)` pairs.
    pub(super) async fn file_attention_impl(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        if query.rel.is_empty() {
            return Err(RepoError::invalid_argument(
                "file_attention requires a non-empty path tail",
            ));
        }

        let mut touches = Vec::<FileAttentionTouch>::new();
        if !query.apply_project_scope
            || query
                .normalized_project_id
                .as_deref()
                .is_some_and(|project_id| !project_id.is_empty())
        {
            match self.file_attention_exact_impl(&query).await {
                Ok(rows) => touches.extend(rows),
                Err(RepoError::Backend(message)) if is_file_attention_schema_skew(&message) => {
                    warn!(
                        error = %message,
                        "file_attention normalized lookup unavailable; falling back to Tier-0 suffix scan"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        touches.extend(self.file_attention_suffix_impl(&query).await?);
        Ok(merge_file_attention_touches(
            touches,
            query.max_rows.saturating_add(1),
        ))
    }

    async fn file_attention_exact_impl(
        &self,
        query: &FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        let rel = query.rel.as_str();

        let tool_io = self.table_ref("tool_io");
        let events = self.table_ref("events");
        let trace = self.table_ref("v_conversation_trace");
        let rel_sql = sql_quote(rel);
        let project_predicate = if query.apply_project_scope {
            let project_id = query
                .normalized_project_id
                .as_deref()
                .filter(|project_id| !project_id.is_empty())
                .ok_or_else(|| {
                    RepoError::invalid_argument("file_attention exact lookup requires project_id")
                })?;
            format!("project_id = {}", sql_quote(project_id))
        } else {
            "project_id != ''".to_string()
        };

        let mut inner_clauses = vec![format!(
            "tool_phase = 'request'
      AND NOT (lowerUTF8(tool_name) = 'file_attention' OR endsWith(lowerUTF8(tool_name), '_file_attention'))
      AND {project_predicate}
      AND repo_rel_path = {rel_sql}"
        )];
        if let Some(tool) = query.tool.as_deref() {
            inner_clauses.push(format!("lower(tool_name) = lower({})", sql_quote(tool)));
        }
        if query.mutations_only {
            inner_clauses.push(format!(
                "lowerUTF8(tool_name) NOT IN ({FILE_ATTENTION_READ_TOOL_NAMES_SQL})"
            ));
        }
        if query.apply_project_scope {
            if let Some(scope_clause) = self.session_scope_clause("session_id") {
                inner_clauses.push(scope_clause);
            }
        }
        let match_predicate = inner_clauses.join("\n      AND ");

        let mut outer_clauses: Vec<String> = Vec::new();
        if let Some(start) = query.start_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) >= {start}"));
        }
        if let Some(end) = query.end_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) < {end}"));
        }
        let outer_where = if outer_clauses.is_empty() {
            "1".to_string()
        } else {
            outer_clauses.join("\n    AND ")
        };

        let limit_plus = query.max_rows.saturating_add(1);
        let max_execution_time = query.execution_budget_secs.max(1).to_string();
        let params = [
            ("query_id", query.cancellation_token.as_str()),
            ("max_execution_time", max_execution_time.as_str()),
            ("join_use_nulls", "1"),
        ];

        let sql = format!(
            "WITH matched AS (
    SELECT session_id, event_uid, tool_call_id, harness, tool_name, tool_phase, input_preview, output_preview, repo_rel_path, worktree_root
    FROM {tool_io} FINAL
    WHERE {match_predicate}
  )
  SELECT
    ti.session_id AS session_id,
    ti.event_uid AS event_uid,
    ti.tool_call_id AS tool_call_id,
    ti.harness AS harness,
    ti.tool_name AS tool_name,
    ti.tool_phase AS tool_phase,
    if(ti.worktree_root != '', concat(ti.worktree_root, '/', ti.repo_rel_path), ti.repo_rel_path) AS matched_path,
    'path_suffix' AS match_kind,
    ti.worktree_root AS worktree_root,
    ifNull(e.cwd, '') AS cwd,
    toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms,
    toUInt64(ifNull(tr.event_order, toUInt64(0))) AS event_order,
    tr.turn_seq AS turn_seq,
    ti.input_preview AS input_preview,
    ti.output_preview AS output_preview
  FROM matched AS ti
  ANY LEFT JOIN (
    SELECT session_id, event_uid, event_time, toUInt64(event_order) AS event_order, toUInt32(turn_seq) AS turn_seq
    FROM {trace}
    WHERE (session_id, event_uid) IN (SELECT session_id, event_uid FROM matched)
  ) AS tr ON tr.session_id = ti.session_id AND tr.event_uid = ti.event_uid
  ANY LEFT JOIN (
    SELECT session_id, event_uid, any(cwd) AS cwd
    FROM {events} FINAL
    WHERE (session_id, event_uid) IN (SELECT session_id, event_uid FROM matched)
    GROUP BY session_id, event_uid
  ) AS e ON e.session_id = ti.session_id AND e.event_uid = ti.event_uid
  WHERE {outer_where}
  ORDER BY isNull(event_unix_ms) ASC, event_unix_ms DESC, event_order DESC, ti.event_uid DESC
  LIMIT {limit_plus}
  FORMAT JSONEachRow",
        );

        self.map_backend(self.ch.query_rows_with_params(&sql, None, &params).await)
    }

    async fn file_attention_suffix_impl(
        &self,
        query: &FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        let rel = query.rel.as_str();

        let tool_io = self.table_ref("tool_io");
        let events = self.table_ref("events");
        let trace = self.table_ref("v_conversation_trace");
        let rel_sql = sql_quote(rel);
        let slash_rel_sql = sql_quote(&format!("/{rel}"));
        let rel_regex = regex::escape(rel);
        let slash_rel_regex = regex::escape(&format!("/{rel}"));
        let rel_len = rel.len();

        const PATH_KEYS: [&str; 9] = [
            "file_path",
            "notebook_path",
            "path",
            "target_file",
            "relativeWorkspacePath",
            "relative_workspace_path",
            "filepath",
            "file",
            "filename",
        ];

        let key_match = |col: &str, key: &str| {
            format!(
                "(endsWith(JSONExtractString({col}, '{key}'), {slash_rel_sql}) OR JSONExtractString({col}, '{key}') = {rel_sql})"
            )
        };
        let ends_with_any = PATH_KEYS
            .iter()
            .map(|key| key_match("input_json", key))
            .collect::<Vec<_>>()
            .join("\n        OR ");

        let path_key_regex = PATH_KEYS.join("|");
        let nested_scalar_path_regex = sql_quote(&format!(
            "\"(?:{path_key_regex})\"[[:space:]]*:[[:space:]]*\"((?:[^\"\\\\]|\\\\.)*{slash_rel_regex}|{rel_regex})\""
        ));
        let nested_array_path_regex = sql_quote(&format!(
            "\"(?:{path_key_regex})\"[[:space:]]*:[[:space:]]*\\[[^\\]]*\"((?:[^\"\\\\]|\\\\.)*{slash_rel_regex}|{rel_regex})\""
        ));
        let nested_path_expr = format!(
            "ifNull(nullIf(extract(ti.input_json, {nested_scalar_path_regex}), ''), extract(ti.input_json, {nested_array_path_regex}))"
        );
        let nested_path_match = format!(
            "(extract(input_json, {nested_scalar_path_regex}) != '' OR extract(input_json, {nested_array_path_regex}) != '')"
        );

        let command_expr = "if(JSONExtractString(input_json, 'command') != '', JSONExtractString(input_json, 'command'), JSONExtractString(input_json, 'cmd'))";
        let shell_path_regex = sql_quote(&format!(
            "(^|[[:space:]'\"`=(])(/[^/][^[:space:]'\"`<>|;&),]*{slash_rel_regex}|/{rel_regex}|{rel_regex})([[:space:]'\"`,;|&<>)]|$)"
        ));
        let shell_match = format!(
            "((JSONHas(input_json, 'command') OR JSONHas(input_json, 'cmd')) AND match({command_expr}, {shell_path_regex}))"
        );

        let mut inner_clauses = vec![format!(
            "tool_phase = 'request'\n      AND NOT (lowerUTF8(tool_name) = 'file_attention' OR endsWith(lowerUTF8(tool_name), '_file_attention'))\n      AND (\n        {ends_with_any}\n        OR {nested_path_match}\n        OR {shell_match}\n      )"
        )];
        if let Some(tool) = query.tool.as_deref() {
            inner_clauses.push(format!("lower(tool_name) = lower({})", sql_quote(tool)));
        }
        if query.mutations_only {
            inner_clauses.push(format!(
                "lowerUTF8(tool_name) NOT IN ({FILE_ATTENTION_READ_TOOL_NAMES_SQL})"
            ));
        }
        if query.apply_project_scope {
            if let Some(scope_clause) = self.session_scope_clause("session_id") {
                inner_clauses.push(scope_clause);
            }
        }
        let match_predicate = inner_clauses.join("\n      AND ");

        let matched_path_arms = PATH_KEYS
            .iter()
            .map(|key| {
                format!(
                    "{}, JSONExtractString(ti.input_json, '{key}')",
                    key_match("ti.input_json", key)
                )
            })
            .collect::<Vec<_>>()
            .join(",\n      ");
        let matched_path_expr =
            format!("multiIf(\n      {matched_path_arms},\n      {nested_path_expr})");

        let exact_relative_root_condition = if rel.starts_with('/') {
            "0".to_string()
        } else {
            format!("matched_path = {rel_sql} AND ifNull(e.cwd, '') != ''")
        };

        let worktree_root_expr = if query.derive_worktree_roots {
            format!(
                "multiIf(
      matched_path != ''
      AND length(matched_path) > {rel_len} + 1
      AND substring(matched_path, length(matched_path) - {rel_len}, 1) = '/',
      substring(matched_path, 1, length(matched_path) - {rel_len} - 1),
      {exact_relative_root_condition},
      ifNull(e.cwd, ''),
      ''
    )"
            )
        } else {
            format!("if({exact_relative_root_condition}, ifNull(e.cwd, ''), '')")
        };

        let mut outer_clauses: Vec<String> = Vec::new();
        if let Some(start) = query.start_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) >= {start}"));
        }
        if let Some(end) = query.end_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) < {end}"));
        }
        let outer_where = if outer_clauses.is_empty() {
            "1".to_string()
        } else {
            outer_clauses.join("\n    AND ")
        };

        let limit_plus = query.max_rows.saturating_add(1);
        let max_execution_time = query.execution_budget_secs.max(1).to_string();
        let params = [
            ("query_id", query.cancellation_token.as_str()),
            ("max_execution_time", max_execution_time.as_str()),
            ("join_use_nulls", "1"),
        ];

        let sql = format!(
            "WITH matched AS (
    SELECT session_id, event_uid, tool_call_id, harness, tool_name, tool_phase, input_json, input_preview, output_preview
    FROM {tool_io} FINAL
    WHERE {match_predicate}
  )
  SELECT
    ti.session_id AS session_id,
    ti.event_uid AS event_uid,
    ti.tool_call_id AS tool_call_id,
    ti.harness AS harness,
    ti.tool_name AS tool_name,
    ti.tool_phase AS tool_phase,
    {matched_path_expr} AS matched_path,
    if(matched_path != '', 'path_suffix', 'shell_path') AS match_kind,
    {worktree_root_expr} AS worktree_root,
    ifNull(e.cwd, '') AS cwd,
    toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms,
    toUInt64(ifNull(tr.event_order, toUInt64(0))) AS event_order,
    tr.turn_seq AS turn_seq,
    ti.input_preview AS input_preview,
    ti.output_preview AS output_preview
  FROM matched AS ti
  ANY LEFT JOIN (
    SELECT session_id, event_uid, event_time, toUInt64(event_order) AS event_order, toUInt32(turn_seq) AS turn_seq
    FROM {trace}
    WHERE (session_id, event_uid) IN (SELECT session_id, event_uid FROM matched)
  ) AS tr ON tr.session_id = ti.session_id AND tr.event_uid = ti.event_uid
  ANY LEFT JOIN (
    SELECT session_id, event_uid, any(cwd) AS cwd
    FROM {events} FINAL
    WHERE (session_id, event_uid) IN (SELECT session_id, event_uid FROM matched)
    GROUP BY session_id, event_uid
  ) AS e ON e.session_id = ti.session_id AND e.event_uid = ti.event_uid
  WHERE {outer_where}
  ORDER BY isNull(event_unix_ms) ASC, event_unix_ms DESC, event_order DESC, ti.event_uid DESC
  LIMIT {limit_plus}
  FORMAT JSONEachRow",
        );

        self.map_backend(self.ch.query_rows_with_params(&sql, None, &params).await)
    }

    pub async fn cancel_query(&self, query_id: &str) -> RepoResult<()> {
        let query_id = query_id.trim();
        if query_id.is_empty() {
            return Ok(());
        }
        let sql = format!("KILL QUERY WHERE query_id = {} SYNC", sql_quote(query_id));
        self.map_backend(self.ch.request_text(&sql, None, None, false, None).await)
            .map(|_| ())
    }
}

fn is_file_attention_schema_skew(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("project_id")
        || lower.contains("repo_rel_path")
        || lower.contains("worktree_root")
}

pub(super) fn merge_file_attention_touches(
    touches: Vec<FileAttentionTouch>,
    limit: usize,
) -> Vec<FileAttentionTouch> {
    let mut seen = std::collections::HashSet::<(String, String, String)>::new();
    let mut merged = Vec::with_capacity(touches.len().min(limit));
    for touch in touches {
        let key = (
            touch.session_id.clone(),
            touch.tool_call_id.clone(),
            touch.event_uid.clone(),
        );
        if seen.insert(key) {
            merged.push(touch);
        }
    }

    merged.sort_by(compare_file_attention_touches);
    merged.truncate(limit);
    merged
}

fn compare_file_attention_touches(
    a: &FileAttentionTouch,
    b: &FileAttentionTouch,
) -> std::cmp::Ordering {
    match (a.event_unix_ms, b.event_unix_ms) {
        (Some(a_ms), Some(b_ms)) => b_ms.cmp(&a_ms),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
    .then_with(|| b.event_order.cmp(&a.event_order))
    .then_with(|| b.event_uid.cmp(&a.event_uid))
}
