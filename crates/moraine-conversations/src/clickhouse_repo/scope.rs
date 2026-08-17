use super::*;

impl ClickHouseConversationRepository {
    /// Subquery selecting `(source_host, session_id)` keys whose origin
    /// directory falls inside `scope`. Host remains part of authorization:
    /// shared backends may legitimately contain the same session ID from
    /// multiple installations.
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
            "(SELECT source_host, session_id FROM (
    SELECT
      source_host,
      session_id,
      argMin(cwd, tuple(event_ts, event_uid)) AS origin_cwd
    FROM {events_source}
    WHERE {session_filter}cwd != ''
    GROUP BY source_host, session_id
  )
  WHERE {root_clauses})",
            root_clauses = root_clauses.join("\n    OR "),
        )
    }

    /// Tuple predicate restricting a host-qualified session key to the
    /// configured project scope, or `None` when retrieval is unscoped.
    pub(super) fn session_scope_clause(
        &self,
        host_column: &str,
        session_column: &str,
    ) -> Option<String> {
        let scope = self.cfg.session_scope.as_ref()?;
        Some(format!(
            "({host_column}, {session_column}) IN {}",
            self.session_origin_scope_subquery(scope, None)
        ))
    }

    /// Predicate restricting a host-collapsed session summary to one
    /// unambiguous in-scope host. A summary row has no `source_host`, so the
    /// authorization subquery must reject duplicate session IDs before the
    /// aggregate is exposed.
    pub(super) fn unqualified_session_scope_clause(&self, session_column: &str) -> Option<String> {
        let scope = self.cfg.session_scope.as_ref()?;
        let events_source = self.live_events_source();
        let root_clauses = scope
            .roots
            .iter()
            .map(|root| {
                format!(
                    "origin_cwd = {} OR startsWith(origin_cwd, {})",
                    sql_quote(root),
                    sql_quote(&format!("{root}/"))
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        Some(format!(
            "{session_column} IN (SELECT session_id FROM (
    SELECT source_host, session_id, argMin(cwd, tuple(event_ts, event_uid)) AS origin_cwd
    FROM {events_source}
    WHERE cwd != ''
    GROUP BY source_host, session_id
  )
  GROUP BY session_id
  HAVING count() = 1 AND countIf({root_clauses}) = 1)"
        ))
    }

    /// Whether a bare public session handle resolves to exactly one source
    /// host and, when configured, that host-local session is in project scope.
    /// Ambiguous cross-host handles fail closed instead of merging histories.
    pub(super) async fn session_in_scope(
        &self,
        source_host: Option<&str>,
        session_id: &str,
    ) -> RepoResult<bool> {
        let publication_token = source_host
            .is_none()
            .then(publication_cache_token)
            .flatten();
        if let Some(publication_token) = publication_token.as_deref() {
            let cache = self.scoped_session_cache.read().await;
            if scoped_session_cache_contains(&cache, session_id, publication_token) {
                return Ok(true);
            }
        }

        let host_filter = source_host
            .map(|host| format!("source_host = {} AND ", sql_quote(host)))
            .unwrap_or_default();
        let events_source = self.live_events_source();
        let (cwd_filter, scope_predicate) = if let Some(scope) = &self.cfg.session_scope {
            let root_clauses = scope
                .roots
                .iter()
                .map(|root| {
                    format!(
                        "origin_cwd = {} OR startsWith(origin_cwd, {})",
                        sql_quote(root),
                        sql_quote(&format!("{root}/"))
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            ("AND cwd != ''", format!(" AND countIf({root_clauses}) = 1"))
        } else {
            ("", String::new())
        };
        let query = format!(
            "SELECT toUInt64(count() = 1{scope_predicate}) AS in_scope
FROM (
  SELECT source_host, argMin(cwd, tuple(event_ts, event_uid)) AS origin_cwd
  FROM {events_source}
  WHERE {host_filter}session_id = {} {cwd_filter}
  GROUP BY source_host
)
FORMAT JSONEachRow",
            sql_quote(session_id)
        );

        #[derive(Deserialize)]
        struct InScopeRow {
            in_scope: u8,
        }

        let rows: Vec<InScopeRow> = self.map_backend(self.query_rows(&query, None).await)?;
        let in_scope = rows.first().is_some_and(|row| row.in_scope == 1);
        if in_scope {
            let Some(publication_token) = publication_token else {
                return Ok(true);
            };
            let Some(PublicationEffect::ScopedSessionCacheInsert {
                session_id,
                publication_token,
            }) = defer_publication_effect(PublicationEffect::ScopedSessionCacheInsert {
                session_id: session_id.to_string(),
                publication_token,
            })
            .await
            else {
                return Ok(true);
            };
            self.insert_scoped_session_cache(session_id, publication_token)
                .await;
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
