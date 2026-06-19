use super::*;

impl ClickHouseConversationRepository {
    /// Subquery selecting the session IDs whose origin directory falls inside
    /// `scope`. A session's origin is its first (by event time) non-empty
    /// `cwd` — the column the ingest normalizer fills for every event from the
    /// harness's recorded working directory (record-level for claude-code,
    /// session-level for codex/pi, `workspacePath` for cursor). Sessions whose
    /// harness records no working directory have an empty origin and match no
    /// root.
    ///
    /// `session_id` narrows the scan to one session for cheap point lookups.
    pub(super) fn session_origin_scope_subquery(
        &self,
        scope: &SessionOriginScope,
        session_id: Option<&str>,
    ) -> String {
        let events_table = self.table_ref("events");
        let session_filter = session_id
            .map(|session_id| format!("session_id = {}\n      AND ", sql_quote(session_id)))
            .unwrap_or_default();
        let root_clauses: Vec<String> = scope
            .roots
            .iter()
            .map(|root| {
                format!(
                    "origin_cwd = {root_sql} OR startsWith(origin_cwd, {root_prefix_sql})",
                    root_sql = sql_quote(root),
                    root_prefix_sql = sql_quote(&format!("{root}/")),
                )
            })
            .collect();
        format!(
            "(SELECT session_id FROM (
    SELECT
      session_id,
      argMin(cwd, tuple(event_ts, event_uid)) AS origin_cwd
    FROM {events_table}
    WHERE {session_filter}cwd != ''
    GROUP BY session_id
  )
  WHERE {root_clauses})",
            root_clauses = root_clauses.join("\n    OR "),
        )
    }

    /// `IN`-clause restricting `column` to sessions inside the configured
    /// scope, or `None` when retrieval is unscoped.
    pub(super) fn session_scope_clause(&self, column: &str) -> Option<String> {
        let scope = self.cfg.session_scope.as_ref()?;
        Some(format!(
            "{column} IN {}",
            self.session_origin_scope_subquery(scope, None)
        ))
    }

    /// Whether `session_id` is visible under the configured scope. Always
    /// true when unscoped. Positive answers are cached for the lifetime of
    /// the repository (a session's origin never changes); negative answers
    /// are re-checked on every call.
    pub(super) async fn session_in_scope(&self, session_id: &str) -> RepoResult<bool> {
        let Some(scope) = self.cfg.session_scope.as_ref() else {
            return Ok(true);
        };

        if self.scoped_session_cache.read().await.contains(session_id) {
            return Ok(true);
        }

        let query = format!(
            "SELECT session_id FROM {} LIMIT 1 FORMAT JSONEachRow",
            self.session_origin_scope_subquery(scope, Some(session_id))
        );

        #[derive(Deserialize)]
        struct SessionIdRow {
            #[allow(dead_code)]
            session_id: String,
        }

        let rows: Vec<SessionIdRow> = self.map_backend(self.ch.query_rows(&query, None).await)?;
        let in_scope = !rows.is_empty();
        if in_scope {
            self.scoped_session_cache
                .write()
                .await
                .insert(session_id.to_string());
        }
        Ok(in_scope)
    }
}
