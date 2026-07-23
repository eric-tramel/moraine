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
        let events_source = self.live_events_source();
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
    FROM {events_source}
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
    /// true when unscoped. Positive answers are cached under the publication
    /// token that authorized them; negative answers are re-checked on every
    /// call.
    pub(super) async fn session_in_scope(&self, session_id: &str) -> RepoResult<bool> {
        let Some(scope) = self.cfg.session_scope.as_ref() else {
            return Ok(true);
        };

        let publication_token = publication_cache_token();
        if let Some(publication_token) = publication_token.as_deref() {
            let cache = self.scoped_session_cache.read().await;
            if scoped_session_cache_contains(&cache, session_id, publication_token) {
                return Ok(true);
            }
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

        let rows: Vec<SessionIdRow> = self.map_backend(self.query_rows(&query, None).await)?;
        let in_scope = !rows.is_empty();
        if in_scope {
            if let Some(publication_token) = publication_token {
                defer_publication_effect(PublicationEffect::ScopedSessionCacheInsert {
                    session_id: session_id.to_string(),
                    publication_token,
                })
                .await;
            }
        }
        Ok(in_scope)
    }

    pub(super) async fn insert_scoped_session_cache(
        &self,
        session_id: String,
        publication_token: String,
    ) {
        let mut cache = self.scoped_session_cache.write().await;
        insert_scoped_session_cache_entry(
            &mut cache,
            session_id,
            publication_token,
            SCOPED_SESSION_CACHE_MAX_ENTRIES,
        );
    }
}
