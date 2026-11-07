use super::*;

const FILE_ATTENTION_READ_TOOL_NAMES_SQL: &str =
    "'read', 'readfile', 'read_file', 'notebookread', 'notebook_read', 'view', 'cat', 'grep', 'rg', 'glob', 'ls', 'list', 'find'";

/// Per-statement settings. `max_threads` bounds the payload read: every reader
/// thread holds one decompressed `payload_json` granule (~16 MiB on 2.5 KiB
/// rows, several times that on tool-heavy stores), so peak memory is roughly
/// `max_threads x granule`. Two threads measured 53 MiB against 110 MiB at the
/// default on the same keyed read; the read touches few granules, so the
/// latency cost is small.
const FILE_ATTENTION_QUERY_SETTINGS: [(&str, &str); 2] =
    [("join_use_nulls", "1"), ("max_threads", "2")];

/// Sort key that picks the current `mcp_event_locator` row per `event_uid`.
///
/// `ReplacingMergeTree(event_version)` keeps the highest `event_version`. Rows
/// with an equal version are re-derivations of the same `events` row and
/// differ at most in `ingested_at`, so `ingested_at` is the deterministic
/// tie-break (latest wins, as the engine would pick the last inserted row).
const LOCATOR_VERSION_KEY: &str = "(event_version, ingested_at)";

/// Current locator rows whose *current* version satisfies `filter`; the
/// equivalent of `mcp_event_locator FINAL WHERE <filter>` without `FINAL`.
///
/// `FINAL` merges every part across the whole unpartitioned table (2.78M rows,
/// ~297 MiB on a 1.6M-event store) even though the filter keeps a few hundred
/// keys. Here the filter first selects candidate `event_uid`s from the plain
/// table (any version may match), then only those keys are aggregated with
/// `argMax(.., version)`, and `HAVING` re-applies the filter to the winning
/// version so keys that matched only through a stale version drop out.
///
/// Yields `event_uid, session_id, project_id, repo_rel_path, worktree_root`.
fn current_locator_sql(locator: &str, filter: &str) -> String {
    format!(
        "SELECT event_uid, cur.1 AS session_id, cur.2 AS project_id, cur.3 AS repo_rel_path, cur.4 AS worktree_root
  FROM (
    SELECT
      event_uid,
      argMax(tuple(session_id, project_id, repo_rel_path, worktree_root), {LOCATOR_VERSION_KEY}) AS cur
    FROM {locator}
    WHERE event_uid IN (SELECT event_uid FROM {locator} WHERE {filter})
    GROUP BY event_uid
    HAVING argMax(toUInt8({filter}), {LOCATOR_VERSION_KEY})
  )"
    )
}

/// `(session ids, (session_id, event_uid) keys)` candidate subqueries for the
/// current locator rows matching `filter`. Every consumer interpolates exactly
/// these two strings, so ClickHouse builds each `IN` set once per statement.
fn locator_candidate_sets(locator: &str, filter: &str) -> (String, String) {
    let current = current_locator_sql(locator, filter);
    (
        format!("SELECT session_id FROM ({current})"),
        format!("SELECT session_id, event_uid FROM ({current})"),
    )
}

/// Predicates on the narrow `events` columns that must hold for the winning
/// version of an event (`w_*` aliases of [`hydrated_tool_events_sql`]). They
/// shrink the key set before any `payload_json` is read.
fn narrow_event_predicates(query: &FileAttentionQuery) -> String {
    let mut clauses = vec![
        "w_tool_phase = 'request'
    AND NOT (lowerUTF8(w_tool_name) = 'file_attention' OR endsWith(lowerUTF8(w_tool_name), '_file_attention'))"
            .to_string(),
    ];
    if let Some(tool) = query.tool.as_deref() {
        clauses.push(format!("lower(w_tool_name) = lower({})", sql_quote(tool)));
    }
    if let Some(harness) = query.harness.as_deref() {
        clauses.push(format!("w_harness = {}", sql_quote(harness)));
    }
    if query.mutations_only {
        clauses.push(format!(
            "lowerUTF8(w_tool_name) NOT IN ({FILE_ATTENTION_READ_TOOL_NAMES_SQL})"
        ));
    }
    clauses.join("\n    AND ")
}

/// Tool-call `events` rows at their winning version, without `FINAL`.
///
/// 1. Winning keys: a narrow-column `argMax` over only the candidate
///    sessions/keys (primary-key prefix `session_id`), keeping keys whose
///    winning version satisfies `narrow_predicates`. Equal versions are
///    re-derivations of one content-addressed row, so `max(event_version)`
///    needs no further tie-break.
/// 2. Wide read: `payload_json` is read only for those exact
///    `(session_id, event_uid, event_version)` keys (`PREWHERE`), and reduced
///    to the `moraine_tool_io` object in the same SELECT so the wide column
///    never survives past the read.
///
/// With `scope = None` (legacy schema without the locator) the aggregation
/// runs over every key of the table, still narrow-column only (it used to be
/// `SELECT * FROM events FINAL`).
fn hydrated_tool_events_sql(
    events_ref: &str,
    scope: Option<(&str, &str)>,
    narrow_predicates: &str,
    with_event_scope: bool,
) -> String {
    let (winner_where, session_prune) = match scope {
        Some((sessions, keys)) => (
            format!("session_id IN ({sessions})\n      AND (session_id, event_uid) IN (\n        {keys}\n      )"),
            format!("session_id IN ({sessions})\n    AND "),
        ),
        None => ("1".to_string(), String::new()),
    };
    let event_scope = if with_event_scope {
        ",
      project_id AS event_project_id,
      worktree_root AS event_worktree_root"
    } else {
        ""
    };
    format!(
        "(SELECT
  session_id, event_uid, tool_call_id, harness, tool_name, tool_phase, source_name, cwd{event_scope_names},
  JSONExtractString(tool_io, 'input_json') AS input_json,
  JSONExtractString(tool_io, 'input_preview') AS input_preview,
  JSONExtractString(tool_io, 'output_preview') AS output_preview,
  JSONExtractString(tool_io, 'project_id') AS project_id,
  JSONExtractString(tool_io, 'repo_rel_path') AS repo_rel_path,
  JSONExtractString(tool_io, 'worktree_root') AS worktree_root
FROM (
  SELECT
      session_id, event_uid, tool_call_id, harness, tool_name, tool_phase, source_name, cwd{event_scope},
      JSONExtractRaw(payload_json, 'moraine_tool_io') AS tool_io
  FROM {events_ref}
  PREWHERE {session_prune}(session_id, event_uid, event_version) IN (
    SELECT session_id, event_uid, win_version AS event_version
    FROM (
      SELECT
        session_id,
        event_uid,
        max(event_version) AS win_version,
        argMax(tool_phase, event_version) AS w_tool_phase,
        argMax(tool_name, event_version) AS w_tool_name,
        argMax(harness, event_version) AS w_harness
      FROM {events_ref}
      WHERE {winner_where}
      GROUP BY session_id, event_uid
    )
    WHERE {narrow_predicates}
  )
)
WHERE tool_io != '')",
        event_scope_names = if with_event_scope {
            ", event_project_id, event_worktree_root"
        } else {
            ""
        },
    )
}

impl ClickHouseConversationRepository {
    fn tool_events_source(&self, query: &FileAttentionQuery, use_candidate_index: bool) -> String {
        let events_ref = self.table_ref("events");
        let narrow = narrow_event_predicates(query);
        if use_candidate_index {
            let locator = self.table_ref("mcp_event_locator");
            let (sessions, keys) =
                locator_candidate_sets(&locator, &suffix_locator_filter(&query.rel));
            hydrated_tool_events_sql(&events_ref, Some((&sessions, &keys)), &narrow, true)
        } else {
            hydrated_tool_events_sql(&events_ref, None, &narrow, false)
        }
    }

    /// Navigation `(event_time, event_order, turn_seq)` for the candidate
    /// events. Ordinals depend on the whole session, so every event of the
    /// candidate sessions is reduced to its highest `event_version` (no
    /// `FINAL`), windowed, and only then restricted to the candidate keys.
    ///
    /// The reduction is `ORDER BY (session_id, event_uid, event_version DESC)
    /// LIMIT 1 BY (session_id, event_uid)` rather than an `argMax` `GROUP BY`:
    /// the order matches the table's sort key, so ClickHouse streams it
    /// (~12 MiB over 250k rows) where a hash aggregation held ~50 MiB of
    /// per-key state, and no `argMax` tuple of wide columns is materialized.
    fn file_attention_navigation_source(&self, sessions: &str, keys: &str) -> String {
        let navigation = self.table_ref("mcp_event_navigation");
        format!(
            "(SELECT
  session_id,
  event_uid,
  display_time AS event_time,
  toUInt64(event_order) AS event_order,
  if(turn_index > 0, turn_index, greatest(toUInt32(1), toUInt32(running_user_count))) AS turn_seq
FROM (
  SELECT
    session_id,
    event_uid,
    display_time,
    turn_index,
    row_number() OVER (
      PARTITION BY session_id
      ORDER BY sort_time, source_file, source_generation, source_offset, source_line_no, emission_index, event_uid
    ) AS event_order,
    countIf(is_user_message != 0) OVER (
      PARTITION BY session_id
      ORDER BY sort_time, source_file, source_generation, source_offset, source_line_no, emission_index, event_uid
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_user_count
  FROM (
    SELECT
      session_id, event_uid, sort_time, source_file, source_generation, source_offset,
      source_line_no, emission_index, display_time, turn_index, is_user_message
    FROM {navigation}
    WHERE session_id IN ({sessions})
    ORDER BY session_id, event_uid, event_version DESC
    LIMIT 1 BY session_id, event_uid
  )
)
WHERE (session_id, event_uid) IN (
  {keys}
))"
        )
    }

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
    /// agent-isolation worktrees into one result set. The exact normalized-path
    /// pass uses the canonical event locator; only its bounded matches are
    /// hydrated from event payloads. The legacy suffix fallback still scans
    /// payload JSON for records without normalized paths.
    pub(super) async fn file_attention_impl(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        if query.rel.is_empty() {
            return Err(RepoError::invalid_argument(
                "file_attention requires a non-empty path tail",
            ));
        }

        if query.apply_project_scope
            && query
                .normalized_project_id
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Ok(Vec::new());
        }
        let limit_plus = query.max_rows.saturating_add(1);
        let mut touches = Vec::<FileAttentionTouch>::new();
        let normalized_schema_available = match self.file_attention_exact_impl(&query).await {
            Ok(rows) => {
                touches.extend(rows);
                true
            }
            Err(RepoError::Backend(message)) if is_file_attention_schema_skew(&message) => {
                if !query.apply_project_scope {
                    return Err(RepoError::backend(message));
                }
                warn!(
                    error = %message,
                    "file_attention normalized lookup unavailable; keeping project-scoped request closed"
                );
                false
            }
            Err(error) => return Err(error),
        };
        let project_root_mapping_available = if query.apply_project_scope
            && normalized_schema_available
        {
            match self.record_file_attention_project_roots(&query).await {
                Ok(()) => true,
                Err(RepoError::Backend(message)) if is_file_attention_schema_skew(&message) => {
                    warn!(
                        error = %message,
                        "file_attention durable project-root mapping unavailable; keeping request closed"
                    );
                    return Err(RepoError::backend(message));
                }
                Err(error) => return Err(error),
            }
        } else {
            false
        };

        if query.apply_project_scope && !normalized_schema_available {
            return Ok(touches);
        }
        if normalized_schema_available {
            touches.extend(
                self.file_attention_suffix_impl(
                    &query,
                    normalized_schema_available,
                    project_root_mapping_available,
                    true,
                )
                .await?,
            );
            return Ok(merge_file_attention_touches(touches, limit_plus));
        }
        touches.extend(
            self.file_attention_suffix_impl(
                &query,
                normalized_schema_available,
                project_root_mapping_available,
                false,
            )
            .await?,
        );
        Ok(merge_file_attention_touches(touches, limit_plus))
    }

    async fn file_attention_exact_impl(
        &self,
        query: &FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        let rel = query.rel.as_str();

        let locator = self.table_ref("mcp_event_locator");
        let events = self.table_ref("events");
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
        let exact_candidate_filter = format!("{project_predicate} AND repo_rel_path = {rel_sql}");
        let (candidate_sessions, candidate_keys) =
            locator_candidate_sets(&locator, &exact_candidate_filter);
        let attention_navigation =
            self.file_attention_navigation_source(&candidate_sessions, &candidate_keys);

        let mut inner_clauses = vec![
            "e.tool_phase = 'request'
      AND NOT (lowerUTF8(e.tool_name) = 'file_attention' OR endsWith(lowerUTF8(e.tool_name), '_file_attention'))"
                .to_string(),
        ];
        if let Some(tool) = query.tool.as_deref() {
            inner_clauses.push(format!("lower(e.tool_name) = lower({})", sql_quote(tool)));
        }
        if let Some(harness) = query.harness.as_deref() {
            inner_clauses.push(format!("e.harness = {}", sql_quote(harness)));
        }
        if query.mutations_only {
            inner_clauses.push(format!(
                "lowerUTF8(e.tool_name) NOT IN ({FILE_ATTENTION_READ_TOOL_NAMES_SQL})"
            ));
        }
        if let Some(scope_clause) = self.session_scope_clause("e.session_id") {
            inner_clauses.push(scope_clause);
        }
        let match_predicate = inner_clauses.join("\n      AND ");

        let mut outer_clauses: Vec<String> = Vec::new();
        if let Some(start) = query.start_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) >= {start}"));
        }
        if let Some(end) = query.end_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) < {end}"));
        }
        if let Some(source_name) = query.source_name.as_deref() {
            outer_clauses.push(format!("ti.source_name = {}", sql_quote(source_name)));
        }
        let outer_where = if outer_clauses.is_empty() {
            "1".to_string()
        } else {
            outer_clauses.join("\n    AND ")
        };

        let limit_plus = query.max_rows.saturating_add(1);
        let params = FILE_ATTENTION_QUERY_SETTINGS;

        let normalized_root_condition = single_path_sql("ti.worktree_root");

        let candidate_events = hydrated_tool_events_sql(
            &events,
            Some((&candidate_sessions, &candidate_keys)),
            &narrow_event_predicates(query),
            false,
        );
        let matched_ids = current_locator_sql(&locator, &exact_candidate_filter);

        let sql = format!(
            "WITH matched_ids AS (
    SELECT session_id, event_uid, repo_rel_path, worktree_root
    FROM ({matched_ids})
  ),
  matched AS (
    SELECT
      e.session_id AS session_id,
      e.event_uid AS event_uid,
      e.tool_call_id AS tool_call_id,
      e.harness AS harness,
      e.source_name AS source_name,
      e.tool_name AS tool_name,
      e.tool_phase AS tool_phase,
      e.input_preview AS input_preview,
      e.output_preview AS output_preview,
      mi.repo_rel_path AS repo_rel_path,
      mi.worktree_root AS worktree_root,
      e.cwd AS cwd
    FROM (
      SELECT session_id, event_uid, tool_call_id, harness, tool_name, tool_phase, source_name, cwd, input_preview, output_preview
      FROM {candidate_events}
    ) AS e
    ANY INNER JOIN matched_ids AS mi
      ON mi.session_id = e.session_id AND mi.event_uid = e.event_uid
    WHERE {match_predicate}
  )
  SELECT
    ti.session_id AS session_id,
    ti.event_uid AS event_uid,
    ti.tool_call_id AS tool_call_id,
    ti.harness AS harness,
    ti.source_name AS source_name,
    ti.tool_name AS tool_name,
    ti.tool_phase AS tool_phase,
    if({normalized_root_condition}, concat(ti.worktree_root, '/', ti.repo_rel_path), ti.repo_rel_path) AS matched_path,
    'path_suffix' AS match_kind,
    if({normalized_root_condition}, ti.worktree_root, '') AS worktree_root,
    ti.cwd AS cwd,
    toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms,
    toUInt64(ifNull(tr.event_order, toUInt64(0))) AS event_order,
    tr.turn_seq AS turn_seq,
    ti.input_preview AS input_preview,
    ti.output_preview AS output_preview
  FROM matched AS ti
  ANY LEFT JOIN (
    SELECT session_id, event_uid, event_time, event_order, turn_seq
    FROM {attention_navigation}
  ) AS tr ON tr.session_id = ti.session_id AND tr.event_uid = ti.event_uid
  WHERE {outer_where}
  ORDER BY isNull(event_unix_ms) ASC, event_unix_ms DESC, event_order DESC, ti.event_uid DESC
  LIMIT {limit_plus}
  FORMAT JSONEachRow",
        );

        self.map_backend(self.query_rows_with_params(&sql, None, &params).await)
    }

    async fn record_file_attention_project_roots(
        &self,
        query: &FileAttentionQuery,
    ) -> RepoResult<()> {
        let Some(project_id) = query
            .normalized_project_id
            .as_deref()
            .filter(|project_id| !project_id.is_empty())
        else {
            return Ok(());
        };
        if query.normalized_project_roots.is_empty() {
            return Ok(());
        }

        let roots = query
            .normalized_project_roots
            .iter()
            .filter(|root| !root.is_empty())
            .map(|root| sql_quote(root))
            .collect::<Vec<_>>()
            .join(", ");
        if roots.is_empty() {
            return Ok(());
        }

        let mappings = self.table_ref("file_attention_project_roots");
        let sql = format!(
            "INSERT INTO {mappings} (project_id, worktree_root, observed_version)
SELECT {}, arrayJoin([{roots}]), toUInt64(toUnixTimestamp64Milli(now64(3)))",
            sql_quote(project_id)
        );
        self.map_backend(self.ch.request_text(&sql, None, None, false, None).await)
            .map(|_| ())
    }

    async fn file_attention_suffix_impl(
        &self,
        query: &FileAttentionQuery,
        normalized_schema_available: bool,
        project_root_mapping_available: bool,
        use_candidate_index: bool,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        let rel = query.rel.as_str();

        let tool_events = self.tool_events_source(query, use_candidate_index);
        let rel_sql = sql_quote(rel);
        let slash_rel_sql = sql_quote(&format!("/{rel}"));
        let attention_navigation = if use_candidate_index {
            let locator = self.table_ref("mcp_event_locator");
            let (sessions, keys) = locator_candidate_sets(&locator, &suffix_locator_filter(rel));
            self.file_attention_navigation_source(&sessions, &keys)
        } else {
            let trace = self.table_ref("v_conversation_trace");
            format!(
                "(SELECT session_id, event_uid, event_time, toUInt64(event_order) AS event_order, toUInt32(turn_seq) AS turn_seq
FROM {trace}
WHERE (session_id, event_uid) IN (SELECT session_id, event_uid FROM matched))"
            )
        };
        let rel_regex = regex::escape(rel);
        let slash_rel_regex = regex::escape(&format!("/{rel}"));

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

        let normalized_root_is_single = single_path_sql("ti.worktree_root");
        let event_root_expression = if normalized_schema_available {
            "ifNull(ti.event_worktree_root, '')"
        } else {
            "ifNull(ti.cwd, '')"
        };
        let event_root_is_single = single_path_sql(event_root_expression);

        let mut inner_clauses = vec![format!(
            "tool_phase = 'request'\n      AND NOT (lowerUTF8(tool_name) = 'file_attention' OR endsWith(lowerUTF8(tool_name), '_file_attention'))\n      AND (\n        {ends_with_any}\n        OR {nested_path_match}\n        OR {shell_match}\n      )"
        )];
        if let Some(tool) = query.tool.as_deref() {
            inner_clauses.push(format!("lower(tool_name) = lower({})", sql_quote(tool)));
        }
        if let Some(harness) = query.harness.as_deref() {
            inner_clauses.push(format!("harness = {}", sql_quote(harness)));
        }
        if query.mutations_only {
            inner_clauses.push(format!(
                "lowerUTF8(tool_name) NOT IN ({FILE_ATTENTION_READ_TOOL_NAMES_SQL})"
            ));
        }
        if let Some(scope_clause) = self.session_scope_clause("session_id") {
            inner_clauses.push(scope_clause);
        }
        let match_predicate = inner_clauses.join("\n      AND ");

        let scalar_path_values = PATH_KEYS
            .iter()
            .map(|key| format!("JSONExtractString(ti.input_json, '{key}')"))
            .collect::<Vec<_>>()
            .join(", ");
        let legacy_scalar_paths_expr = format!("[{scalar_path_values}]");
        let legacy_scalar_path_expr = format!(
            "arrayFirst(path -> endsWith(path, {slash_rel_sql}) OR path = {rel_sql}, legacy_scalar_paths)"
        );
        let legacy_scalar_path_is_single = single_path_sql("legacy_scalar_path");
        let provenance_key_regex = sql_quote(&format!(
            "\"(?:{path_key_regex}|command|cmd)\"[[:space:]]*:"
        ));
        let cwd_is_single = single_path_sql("ifNull(ti.cwd, '')");
        let candidate_root_expr = if query.derive_legacy_roots && !rel.starts_with('/') {
            format!(
                "if(
      countMatches(ti.input_json, {provenance_key_regex}) = 1
      AND arrayCount(path -> path != '', legacy_scalar_paths) = 1
      AND {legacy_scalar_path_is_single}
      AND {cwd_is_single}
      AND (legacy_scalar_path = {rel_sql}
           OR legacy_scalar_path = concat(ifNull(ti.cwd, ''), '/', {rel_sql})),
      ifNull(ti.cwd, ''),
      '')"
            )
        } else {
            "''".to_string()
        };
        let normalized_root_condition = if normalized_schema_available {
            format!(
                "ti.project_id != '' AND ti.repo_rel_path = {rel_sql} AND {normalized_root_is_single}"
            )
        } else {
            "0".to_string()
        };

        let mut outer_clauses: Vec<String> = Vec::new();
        if let Some(start) = query.start_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) >= {start}"));
        }
        if let Some(end) = query.end_unix_ms {
            outer_clauses.push(format!("toUnixTimestamp64Milli(tr.event_time) < {end}"));
        }
        if let Some(source_name) = query.source_name.as_deref() {
            outer_clauses.push(format!("ti.source_name = {}", sql_quote(source_name)));
        }
        let mut project_roots_with = String::new();
        let verified_project_root_expr = if query.apply_project_scope {
            let project_id = query
                .normalized_project_id
                .as_deref()
                .expect("project scope identity checked before querying");
            let project_id_sql = sql_quote(project_id);
            let mut registered_roots = query
                .normalized_project_roots
                .iter()
                .filter(|root| !root.is_empty())
                .cloned()
                .collect::<Vec<_>>();
            registered_roots.sort_by_key(|root| std::cmp::Reverse(root.len()));
            registered_roots.dedup();
            let legacy_roots_sql = registered_roots
                .iter()
                .map(|root| sql_quote(root))
                .collect::<Vec<_>>()
                .join(", ");
            let durable_roots = self.table_ref("file_attention_project_roots");
            let registered_roots_array = if legacy_roots_sql.is_empty() {
                "CAST([], 'Array(String)')".to_string()
            } else {
                format!("[{legacy_roots_sql}]")
            };
            let project_roots_array = if project_root_mapping_available {
                format!(
                    "arrayDistinct(arrayConcat(
      {registered_roots_array},
      (SELECT groupArray(worktree_root) FROM (
         SELECT worktree_root FROM {durable_roots}
         WHERE project_id = {project_id_sql} AND worktree_root != ''
         GROUP BY worktree_root
       ))
    ))"
                )
            } else {
                registered_roots_array
            };
            project_roots_with = format!("{project_roots_array} AS project_roots,\n  ");
            let verified_root =
                "arrayFirst(root -> legacy_candidate_root = root, project_roots)".to_string();
            let root_membership = |expression: &str| format!("has(project_roots, {expression})");
            let legacy_tool_scope = format!(
                "(ti.project_id != '' AND NOT startsWith(ti.project_id, 'git:') AND {normalized_root_is_single} AND {})",
                root_membership("ti.worktree_root")
            );
            let legacy_event_scope = format!(
                "(ti.event_project_id != '' AND NOT startsWith(ti.event_project_id, 'git:') AND {event_root_is_single} AND {})",
                root_membership(event_root_expression)
            );
            outer_clauses.push(format!(
                "(ti.project_id = {project_id_sql}
      OR {legacy_tool_scope}
      OR (ti.project_id = '' AND ti.event_project_id = {project_id_sql})
      OR (ti.project_id = '' AND legacy_candidate_root != ''
          AND ({legacy_event_scope} OR (ti.event_project_id = '' AND verified_project_root != ''))))"
            ));
            Some(verified_root)
        } else {
            None
        };

        let legacy_root_expr = if verified_project_root_expr.is_some() {
            "verified_project_root"
        } else {
            "legacy_candidate_root"
        };
        let worktree_root_expr = format!(
            "multiIf(
      {normalized_root_condition},
      ti.worktree_root,
      legacy_candidate_root != '',
      {legacy_root_expr},
      ''
    )"
        );
        let verified_project_root_column = verified_project_root_expr
            .as_deref()
            .map(|expression| format!(",\n    {expression} AS verified_project_root"))
            .unwrap_or_default();

        let outer_where = if outer_clauses.is_empty() {
            "1".to_string()
        } else {
            outer_clauses.join("\n    AND ")
        };

        let limit_plus = query.max_rows.saturating_add(1);
        let params = FILE_ATTENTION_QUERY_SETTINGS;

        let normalized_tool_columns = if normalized_schema_available {
            ", project_id, repo_rel_path, worktree_root, event_project_id, event_worktree_root"
        } else {
            ""
        };

        let sql = format!(
            "WITH {project_roots_with}matched AS (
    SELECT session_id, event_uid, tool_call_id, harness, tool_name, tool_phase, input_json, input_preview, output_preview, source_name, cwd{normalized_tool_columns}
    FROM {tool_events}
    WHERE {match_predicate}
  )
  SELECT
    ti.session_id AS session_id,
    ti.event_uid AS event_uid,
    ti.tool_call_id AS tool_call_id,
    ti.harness AS harness,
    ti.source_name AS source_name,
    ti.tool_name AS tool_name,
    ti.tool_phase AS tool_phase,
    {legacy_scalar_paths_expr} AS legacy_scalar_paths,
    {legacy_scalar_path_expr} AS legacy_scalar_path,
    if(legacy_scalar_path != '', legacy_scalar_path, {nested_path_expr}) AS matched_path,
    {candidate_root_expr} AS legacy_candidate_root{verified_project_root_column},
    if(matched_path != '', 'path_suffix', 'shell_path') AS match_kind,
    {worktree_root_expr} AS worktree_root,
    ti.cwd AS cwd,
    toInt64(toUnixTimestamp64Milli(tr.event_time)) AS event_unix_ms,
    toUInt64(ifNull(tr.event_order, toUInt64(0))) AS event_order,
    tr.turn_seq AS turn_seq,
    ti.input_preview AS input_preview,
    ti.output_preview AS output_preview
  FROM matched AS ti
  ANY LEFT JOIN (
    SELECT session_id, event_uid, event_time, event_order, turn_seq
    FROM {attention_navigation}
  ) AS tr ON tr.session_id = ti.session_id AND tr.event_uid = ti.event_uid
  WHERE {outer_where}
  ORDER BY isNull(event_unix_ms) ASC, event_unix_ms DESC, event_order DESC, ti.event_uid DESC
  LIMIT {limit_plus}
  FORMAT JSONEachRow",
        );

        self.map_backend(self.query_rows_with_params(&sql, None, &params).await)
    }
}

/// Locator filter for the legacy suffix fallback: any normalized path token
/// equal to, or ending with `/`, the requested path tail.
fn suffix_locator_filter(rel: &str) -> String {
    format!(
        "arrayExists(path -> path = {} OR endsWith(path, {}), path_tokens)",
        sql_quote(rel),
        sql_quote(&format!("/{rel}"))
    )
}

fn single_path_sql(expression: &str) -> String {
    format!(
        "({expression} != '' AND position({expression}, char(0)) = 0 AND position({expression}, char(10)) = 0 AND position({expression}, char(13)) = 0 AND position({expression}, ';') = 0 AND position({expression}, '|') = 0 AND position({expression}, '&') = 0 AND position({expression}, '`') = 0)"
    )
}

fn is_file_attention_schema_skew(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("clickhouse code 47")
        || lower.contains("project_id")
        || lower.contains("repo_rel_path")
        || lower.contains("worktree_root")
        || lower.contains("file_attention_project_roots")
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
