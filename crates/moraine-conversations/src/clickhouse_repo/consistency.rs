use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const MAX_PUBLICATION_READ_ATTEMPTS: usize = 4;

/// Selects the publication token shape used by a repository backend.
///
/// The default Moraine store has one serialized publisher and therefore one
/// global publication revision. Named/shared stores can have independent host
/// publishers, so their token is the complete sorted host revision vector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum PublicationConsistencyMode {
    #[default]
    Local,
    Shared,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(super) struct PublicationHostState {
    #[serde(default)]
    pub(super) source_host: String,
    pub(super) publication_revision: u64,
    #[serde(default)]
    pub(super) head_count: u64,
    #[serde(default)]
    pub(super) head_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize)]
struct AppendControlRow {
    host: String,
    control_revision: u64,
    cache_epoch: u64,
    state: String,
    batch_id: String,
    publisher_id: String,
    #[serde(default)]
    manifest_json: String,
    insert_only: u8,
    #[serde(skip)]
    manifest: Option<AppendManifestScope>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
struct PublicationSourceKey {
    source_host: String,
    source_name: String,
    source_file: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
struct AppendManifestScope {
    #[serde(default)]
    source_keys: BTreeSet<PublicationSourceKey>,
    #[serde(default)]
    session_ids: BTreeSet<String>,
    #[serde(default)]
    project_ids: BTreeSet<String>,
    #[serde(default)]
    event_uids: BTreeSet<String>,
    #[serde(default)]
    dependency_uids: BTreeSet<String>,
    #[serde(default)]
    insert_only: bool,
}

impl AppendControlRow {
    fn parse_manifest(&mut self) {
        self.manifest = (!self.manifest_json.is_empty())
            .then(|| serde_json::from_str(&self.manifest_json).ok())
            .flatten();
    }

    fn has_verified_insert_only_manifest(&self) -> bool {
        self.insert_only == 1
            && self.manifest.as_ref().is_some_and(|manifest| {
                manifest.insert_only
                    && !manifest.source_keys.is_empty()
                    && !manifest.session_ids.is_empty()
                    && !manifest.event_uids.is_empty()
                    && manifest
                        .source_keys
                        .iter()
                        .all(|source| source.source_host == self.host)
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PublicationReadScope {
    global: bool,
    source_keys: BTreeSet<PublicationSourceKey>,
    session_ids: BTreeSet<String>,
    project_ids: BTreeSet<String>,
    event_uids: BTreeSet<String>,
}

impl PublicationReadScope {
    pub(super) fn global() -> Self {
        Self {
            global: true,
            source_keys: BTreeSet::new(),
            session_ids: BTreeSet::new(),
            project_ids: BTreeSet::new(),
            event_uids: BTreeSet::new(),
        }
    }

    pub(super) fn session(session_id: &str) -> Self {
        let session_ids = if session_id.is_empty() {
            BTreeSet::new()
        } else {
            BTreeSet::from([session_id.to_string()])
        };
        Self::from_scopes(
            BTreeSet::new(),
            session_ids,
            BTreeSet::new(),
            BTreeSet::new(),
        )
    }

    pub(super) fn event(event_uid: &str) -> Self {
        let event_uids = if event_uid.is_empty() {
            BTreeSet::new()
        } else {
            BTreeSet::from([event_uid.to_string()])
        };
        Self::from_scopes(
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            event_uids,
        )
    }

    pub(super) fn projects(project_ids: impl IntoIterator<Item = String>) -> Self {
        Self::from_scopes(
            BTreeSet::new(),
            BTreeSet::new(),
            project_ids
                .into_iter()
                .filter(|project_id| !project_id.is_empty())
                .collect(),
            BTreeSet::new(),
        )
    }

    fn from_scopes(
        source_keys: BTreeSet<PublicationSourceKey>,
        session_ids: BTreeSet<String>,
        project_ids: BTreeSet<String>,
        event_uids: BTreeSet<String>,
    ) -> Self {
        let global = source_keys.is_empty()
            && session_ids.is_empty()
            && project_ids.is_empty()
            && event_uids.is_empty();
        Self {
            global,
            source_keys,
            session_ids,
            project_ids,
            event_uids,
        }
    }

    fn union(&self, other: &Self) -> Self {
        if self.global || other.global {
            return Self::global();
        }
        Self::from_scopes(
            self.source_keys
                .union(&other.source_keys)
                .cloned()
                .collect(),
            self.session_ids
                .union(&other.session_ids)
                .cloned()
                .collect(),
            self.project_ids
                .union(&other.project_ids)
                .cloned()
                .collect(),
            self.event_uids.union(&other.event_uids).cloned().collect(),
        )
    }

    fn overlaps(&self, manifest: &AppendManifestScope) -> bool {
        self.global
            || !self.source_keys.is_disjoint(&manifest.source_keys)
            || !self.session_ids.is_disjoint(&manifest.session_ids)
            || !self.project_ids.is_disjoint(&manifest.project_ids)
            || !self.event_uids.is_disjoint(&manifest.event_uids)
            || !self.event_uids.is_disjoint(&manifest.dependency_uids)
    }
}

/// Immutable authorization token held for one whole repository operation.
#[derive(Clone, Debug)]
pub(super) struct PublicationSnapshot {
    mode: PublicationConsistencyMode,
    hosts: Arc<[PublicationHostState]>,
    controls: Arc<[AppendControlRow]>,
    token: Arc<str>,
}

impl PublicationSnapshot {
    fn new(
        mode: PublicationConsistencyMode,
        hosts: Vec<PublicationHostState>,
        controls: Vec<AppendControlRow>,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update([match mode {
            PublicationConsistencyMode::Local => 0,
            PublicationConsistencyMode::Shared => 1,
        }]);
        for host in &hosts {
            hasher.update(host.publication_revision.to_be_bytes());
            if mode == PublicationConsistencyMode::Shared {
                update_digest_string(&mut hasher, &host.source_host);
                hasher.update(host.head_count.to_be_bytes());
                update_digest_string(&mut hasher, &host.head_fingerprint);
            }
        }
        for control in &controls {
            update_digest_string(&mut hasher, &control.host);
            hasher.update(control.control_revision.to_be_bytes());
            hasher.update(control.cache_epoch.to_be_bytes());
            update_digest_string(&mut hasher, &control.state);
            update_digest_string(&mut hasher, &control.batch_id);
            update_digest_string(&mut hasher, &control.publisher_id);
            // `manifest_json` is immutable within a control revision. Keep its
            // parsed scope for fencing, but do not re-hash the potentially
            // large payload for every request token.
            hasher.update([control.insert_only]);
        }
        let token: Arc<str> = format!("{:x}", hasher.finalize()).into();
        Self {
            mode,
            hosts: hosts.into(),
            controls: controls.into(),
            token,
        }
    }

    pub(super) fn token(&self) -> &str {
        &self.token
    }

    pub(super) fn cache_allowed(&self) -> bool {
        self.controls.iter().all(|control| control.state == "idle")
    }

    pub(super) fn publication_revision_predicate(&self, alias: &str) -> String {
        match self.mode {
            PublicationConsistencyMode::Local => {
                let revision = self
                    .hosts
                    .first()
                    .map(|host| host.publication_revision)
                    .unwrap_or(0);
                format!("{alias}.publication_revision <= {revision}")
            }
            PublicationConsistencyMode::Shared => {
                if self.hosts.is_empty() {
                    return "0".to_string();
                }
                self.hosts
                    .iter()
                    .map(|host| {
                        format!(
                            "({alias}.source_host = {} AND {alias}.publication_revision <= {})",
                            sql_quote(&host.source_host),
                            host.publication_revision
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" OR ")
            }
        }
    }

    /// Scalar array expression containing the exact source heads reconstructed
    /// at this snapshot. Large head sets remain inside ClickHouse.
    pub(super) fn captured_source_heads_sql(&self, history_ref: &str) -> String {
        let predicate = self.publication_revision_predicate("history");
        format!(
            "(SELECT groupArray(CAST(tuple(captured.source_host, captured.source_name, captured.source_file, captured.source_generation, captured.publication_revision), 'Tuple(source_host String, source_name String, source_file String, source_generation UInt32, publication_revision UInt64)'))\nFROM (\n  SELECT\n    grouped.source_host AS source_host,\n    grouped.source_name AS source_name,\n    grouped.source_file AS source_file,\n    toUInt32(tupleElement(grouped.head, 1)) AS source_generation,\n    toUInt64(tupleElement(grouped.head, 2)) AS publication_revision\n  FROM (\n    SELECT\n      history.source_host AS source_host,\n      history.source_name AS source_name,\n      history.source_file AS source_file,\n      argMax(tuple(history.source_generation, history.publication_revision), history.publication_revision) AS head\n    FROM {history_ref} AS history\n    WHERE {predicate}\n    GROUP BY history.source_host, history.source_name, history.source_file\n  ) AS grouped\n) AS captured)"
        )
    }

    fn read_allowed(&self, class: PublicationReadClass, scope: &PublicationReadScope) -> bool {
        self.controls
            .iter()
            .all(|control| match control.state.as_str() {
                "idle" => true,
                "preparing" | "blocked" if control.has_verified_insert_only_manifest() => {
                    class == PublicationReadClass::MovingFeed
                        || !scope.overlaps(control.manifest.as_ref().expect("verified manifest"))
                }
                _ => false,
            })
    }

    #[cfg(test)]
    fn host_revision_vector(&self) -> Vec<(&str, u64)> {
        self.hosts
            .iter()
            .map(|host| (host.source_host.as_str(), host.publication_revision))
            .collect()
    }
}

// Compare the same causal identity encoded in `token`. The parsed append
// manifest is policy payload owned by its control revision, not an additional
// revision signal, so revalidation never walks or copies its full UID sets.
impl PartialEq for PublicationSnapshot {
    fn eq(&self, other: &Self) -> bool {
        if self.mode != other.mode || self.hosts.len() != other.hosts.len() {
            return false;
        }
        let hosts_match = self
            .hosts
            .iter()
            .zip(other.hosts.iter())
            .all(|(left, right)| match self.mode {
                PublicationConsistencyMode::Local => {
                    left.publication_revision == right.publication_revision
                }
                PublicationConsistencyMode::Shared => left == right,
            });
        hosts_match
            && self.controls.len() == other.controls.len()
            && self
                .controls
                .iter()
                .zip(other.controls.iter())
                .all(|(left, right)| {
                    left.host == right.host
                        && left.control_revision == right.control_revision
                        && left.cache_epoch == right.cache_epoch
                        && left.state == right.state
                        && left.batch_id == right.batch_id
                        && left.publisher_id == right.publisher_id
                        && left.insert_only == right.insert_only
                })
    }
}

impl Eq for PublicationSnapshot {}

fn update_digest_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

tokio::task_local! {
    static ACTIVE_PUBLICATION_SNAPSHOT: PublicationSnapshot;
    static ACTIVE_PUBLICATION_EFFECTS: Arc<Mutex<Vec<PublicationEffect>>>;
}

pub(super) enum PublicationEffect {
    SearchTelemetry {
        query_row: Value,
        hit_rows: Vec<Value>,
    },
    ScopedSessionCacheInsert {
        session_id: String,
        publication_token: String,
    },
    FileAttentionProjectRootsWrite {
        sql: String,
    },
}

async fn take_committable_effects(
    snapshot: &PublicationSnapshot,
    effects: &Mutex<Vec<PublicationEffect>>,
) -> Vec<PublicationEffect> {
    let mut effects = effects.lock().await;
    if snapshot.cache_allowed() {
        std::mem::take(&mut *effects)
    } else {
        effects.clear();
        Vec::new()
    }
}

pub(super) fn active_publication_snapshot() -> Option<PublicationSnapshot> {
    ACTIVE_PUBLICATION_SNAPSHOT.try_with(Clone::clone).ok()
}

pub(super) fn active_publication_token() -> Option<String> {
    active_publication_snapshot().map(|snapshot| snapshot.token().to_string())
}

pub(super) async fn defer_publication_effect(
    effect: PublicationEffect,
) -> Option<PublicationEffect> {
    let effects = ACTIVE_PUBLICATION_EFFECTS.try_with(Clone::clone).ok();
    let Some(effects) = effects else {
        return Some(effect);
    };
    effects.lock().await.push(effect);
    None
}

/// Return the authorization namespace for mutable live-cache entries.
/// `None` means an append is preparing, so callers must bypass both lookup and
/// population. Direct implementation tests that do not install an operation
/// snapshot retain an isolated legacy namespace.
pub(super) fn publication_cache_token() -> Option<String> {
    match active_publication_snapshot() {
        Some(snapshot) if snapshot.cache_allowed() => Some(snapshot.token().to_string()),
        Some(_) => None,
        None => Some("legacy".to_string()),
    }
}

/// Qualify a mutable live-cache key with the operation's authorization token.
pub(super) fn publication_cache_key(key: &str) -> Option<String> {
    publication_cache_token().map(|token| format!("{token}\u{1f}{key}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PublicationReadClass {
    /// Session, turn, event, file-attention, or scope-sensitive operation.
    Strict,
    /// Global list/search/analytics operation. Only a proven insert-only
    /// append is allowed to expose a moving prefix while it is preparing.
    MovingFeed,
}

impl ClickHouseConversationRepository {
    /// Canonical events authorized by the operation's captured heads. Outside
    /// a repository operation (query-builder/unit-test use), fall back to the
    /// current live view.
    pub(super) fn live_events_source(&self) -> String {
        let Some(snapshot) = active_publication_snapshot() else {
            return canonical_events_source(&self.table_ref("v_live_events"));
        };

        let history = self.table_ref("v_published_source_generation_history");
        let revision_predicate = snapshot.publication_revision_predicate("history");
        let events = self.table_ref("events");
        format!(
            "(SELECT e.*\nFROM {events} AS e FINAL\nALL INNER JOIN (\n  SELECT\n    history.source_host AS source_host,\n    history.source_name AS source_name,\n    history.source_file AS source_file,\n    tupleElement(argMax(tuple(history.source_generation, history.publication_revision), history.publication_revision), 1) AS source_generation\n  FROM {history} AS history\n  WHERE {revision_predicate}\n  GROUP BY history.source_host, history.source_name, history.source_file\n) AS published\n  ON published.source_host = e.source_host\n AND published.source_name = e.source_name\n AND published.source_file = e.source_file\n AND published.source_generation = e.source_generation)"
        )
    }

    async fn capture_publication_snapshot(
        &self,
        phase: &'static str,
    ) -> RepoResult<PublicationSnapshot> {
        let heads_marker = match phase {
            "capture" => "/* moraine:publication_snapshot:capture */",
            "revalidate" => "/* moraine:publication_snapshot:revalidate */",
            _ => unreachable!("publication snapshot phase is internal"),
        };
        let heads_query = match self.publication_mode {
            PublicationConsistencyMode::Local => {
                // The local publication actor allocates one globally monotonic
                // revision, so its maximum is the complete request token.
                let history = self.table_ref("published_source_generations");
                format!(
                    "{heads_marker}\nSELECT\n  toUInt64(ifNull(max(publication_revision), 0)) AS publication_revision\nFROM {history}\nFORMAT JSONEachRow"
                )
            }
            PublicationConsistencyMode::Shared => {
                let current_heads = self.table_ref("v_current_published_source_generations");
                // Qualify every input so ClickHouse cannot substitute the aggregate
                // output alias `publication_revision` back into this groupArray.
                let canonical_fingerprint = "hex(SHA256(arrayStringConcat(arraySort(groupArray(toJSONString(tuple(heads.source_host, heads.source_name, heads.source_file, heads.source_generation, heads.publication_revision)))), '\\0')))";
                format!(
                    "{heads_marker}\nSELECT\n  heads.source_host AS source_host,\n  toUInt64(ifNull(max(heads.publication_revision), 0)) AS publication_revision,\n  toUInt64(count()) AS head_count,\n  {canonical_fingerprint} AS head_fingerprint\nFROM {current_heads} AS heads\nGROUP BY heads.source_host\nORDER BY heads.source_host\nFORMAT JSONEachRow"
                )
            }
        };
        let hosts: Vec<PublicationHostState> =
            self.map_backend(self.query_rows(&heads_query, None).await)?;

        if self.publication_mode == PublicationConsistencyMode::Shared
            && hosts.iter().any(|host| host.source_host.is_empty())
        {
            return Err(RepoError::backend(
                "shared publication snapshot contains an unowned hostless source",
            ));
        }

        let fence_marker = match phase {
            "capture" => "/* moraine:append_fence:capture */",
            "revalidate" => "/* moraine:append_fence:revalidate */",
            _ => unreachable!("publication snapshot phase is internal"),
        };
        let controls_query = format!(
            "{fence_marker}\nSELECT\n  host,\n  toUInt64(control_revision) AS control_revision,\n  toUInt64(cache_epoch) AS cache_epoch,\n  state,\n  batch_id,\n  publisher_id,\n  manifest_json,\n  toUInt8(insert_only) AS insert_only\nFROM {}\nORDER BY host\nFORMAT JSONEachRow",
            self.table_ref("v_current_ingest_append_control")
        );
        let mut controls: Vec<AppendControlRow> =
            self.map_backend(self.query_rows(&controls_query, None).await)?;
        for control in &mut controls {
            control.parse_manifest();
        }

        Ok(PublicationSnapshot::new(
            self.publication_mode,
            hosts,
            controls,
        ))
    }

    pub(super) async fn run_publication_consistent<T, F, Fut>(
        &self,
        class: PublicationReadClass,
        operation: F,
    ) -> RepoResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RepoResult<T>>,
    {
        self.run_publication_consistent_scoped(class, PublicationReadScope::global(), operation)
            .await
    }

    pub(super) async fn run_publication_consistent_scoped<T, F, Fut>(
        &self,
        class: PublicationReadClass,
        scope: PublicationReadScope,
        operation: F,
    ) -> RepoResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RepoResult<T>>,
    {
        self.run_publication_consistent_scoped_with_result_scope(class, scope, |_| None, operation)
            .await
    }

    pub(super) async fn run_publication_consistent_scoped_with_result_scope<T, F, Fut, S>(
        &self,
        class: PublicationReadClass,
        scope: PublicationReadScope,
        result_scope: S,
        mut operation: F,
    ) -> RepoResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RepoResult<T>>,
        S: Fn(&T) -> Option<PublicationReadScope>,
    {
        // A repository operation invoked by another operation must inherit the
        // outer authorization token instead of sampling a new model.
        if let Some(snapshot) = active_publication_snapshot() {
            if !snapshot.read_allowed(class, &scope) {
                return Err(RepoError::ReadModelChanged);
            }
            let value = operation().await?;
            let release_scope = result_scope(&value)
                .map(|discovered| scope.union(&discovered))
                .unwrap_or_else(|| scope.clone());
            if !snapshot.read_allowed(class, &release_scope) {
                return Err(RepoError::ReadModelChanged);
            }
            return Ok(value);
        }

        for _ in 0..MAX_PUBLICATION_READ_ATTEMPTS {
            let snapshot = self.capture_publication_snapshot("capture").await?;
            if !snapshot.read_allowed(class, &scope) {
                return Err(RepoError::ReadModelChanged);
            }

            let effects = Arc::new(Mutex::new(Vec::new()));
            let result = ACTIVE_PUBLICATION_SNAPSHOT
                .scope(
                    snapshot.clone(),
                    ACTIVE_PUBLICATION_EFFECTS.scope(effects.clone(), operation()),
                )
                .await;
            let value = match result {
                Ok(value) => value,
                Err(RepoError::ReadModelChanged) => continue,
                Err(error) => return Err(error),
            };
            let release_scope = result_scope(&value)
                .map(|discovered| scope.union(&discovered))
                .unwrap_or_else(|| scope.clone());

            // Some compatibility views are current-head views rather than
            // parameterized as-of relations. Moraine sends every statement
            // and this revalidation to the same ClickHouse endpoint; a head or
            // append-control transition on that endpoint changes this complete
            // token, so mixed attempts and their buffered effects are retried.
            let current = self.capture_publication_snapshot("revalidate").await?;
            if current == snapshot {
                if !snapshot.read_allowed(class, &release_scope) {
                    return Err(RepoError::ReadModelChanged);
                }
                let effects = take_committable_effects(&snapshot, &effects).await;
                for effect in effects {
                    match effect {
                        PublicationEffect::SearchTelemetry {
                            query_row,
                            hit_rows,
                        } => self.write_search_log_rows(query_row, hit_rows).await,
                        PublicationEffect::ScopedSessionCacheInsert {
                            session_id,
                            publication_token,
                        } => {
                            self.insert_scoped_session_cache(session_id, publication_token)
                                .await;
                        }
                        PublicationEffect::FileAttentionProjectRootsWrite { sql } => {
                            self.execute_file_attention_project_roots_write(&sql)
                                .await?;
                        }
                    }
                }
                return Ok(value);
            }
        }

        Err(RepoError::ReadModelChanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host_state(host: &str, revision: u64, fingerprint: &str) -> PublicationHostState {
        PublicationHostState {
            source_host: host.to_string(),
            publication_revision: revision,
            head_count: 1,
            head_fingerprint: fingerprint.to_string(),
        }
    }

    fn control(state: &str, epoch: u64, insert_only: bool) -> AppendControlRow {
        let manifest_json = if state == "idle" {
            String::new()
        } else {
            json!({
                "source_keys": [{
                    "source_host": "host-a",
                    "source_name": "codex",
                    "source_file": "/sessions/a.jsonl"
                }],
                "session_ids": ["session-a"],
                "project_ids": ["project-a"],
                "event_uids": ["event-a"],
                "dependency_uids": ["dependency-a"],
                "insert_only": insert_only
            })
            .to_string()
        };
        let manifest = (!manifest_json.is_empty())
            .then(|| serde_json::from_str(&manifest_json).expect("test append manifest"));
        AppendControlRow {
            host: "host-a".to_string(),
            control_revision: epoch,
            cache_epoch: epoch,
            state: state.to_string(),
            batch_id: "batch-a".to_string(),
            publisher_id: "publisher-a".to_string(),
            manifest_json,
            insert_only: u8::from(insert_only),
            manifest,
        }
    }

    fn source_scope(host: &str, name: &str, file: &str) -> PublicationReadScope {
        PublicationReadScope::from_scopes(
            BTreeSet::from([PublicationSourceKey {
                source_host: host.to_string(),
                source_name: name.to_string(),
                source_file: file.to_string(),
            }]),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
        )
    }

    #[test]
    fn snapshot_token_changes_for_heads_and_cache_epochs() {
        let original = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("idle", 7, false)],
        );
        let replaced = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 5, "heads-b")],
            vec![control("idle", 7, false)],
        );
        let appended = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("idle", 8, false)],
        );

        assert_ne!(original.token(), replaced.token());
        assert_ne!(original.token(), appended.token());
    }

    #[test]
    fn local_snapshot_identity_uses_only_the_global_revision() {
        let local_row: PublicationHostState = serde_json::from_value(json!({
            "publication_revision": 4
        }))
        .expect("deserialize the compact local snapshot row");
        assert_eq!(local_row.source_host, "");
        assert_eq!(local_row.head_count, 0);
        assert_eq!(local_row.head_fingerprint, "");

        let mut different_proof = host_state("unexpected-host", 4, "different-heads");
        different_proof.head_count = 99;
        let first = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![local_row],
            Vec::new(),
        );
        let second = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![different_proof],
            Vec::new(),
        );

        assert_eq!(first.token(), second.token());
        assert_eq!(first, second);

        let shared_first = PublicationSnapshot::new(
            PublicationConsistencyMode::Shared,
            vec![host_state("host-a", 4, "heads-a")],
            Vec::new(),
        );
        let shared_second = PublicationSnapshot::new(
            PublicationConsistencyMode::Shared,
            vec![host_state("host-a", 4, "heads-b")],
            Vec::new(),
        );
        assert_ne!(shared_first.token(), shared_second.token());
        assert_ne!(shared_first, shared_second);
    }

    #[test]
    fn append_snapshot_identity_uses_the_control_revision_not_manifest_bytes() {
        let original_control = control("preparing", 7, true);
        let mut different_manifest = control("preparing", 7, true);
        different_manifest.manifest_json = different_manifest
            .manifest_json
            .replace("session-a", "session-b");
        different_manifest.parse_manifest();

        let original = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![original_control],
        );
        let same_transition = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![different_manifest.clone()],
        );
        assert_eq!(original.token(), same_transition.token());
        assert_eq!(original, same_transition);

        different_manifest.control_revision += 1;
        let next_transition = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![different_manifest],
        );
        assert_ne!(original.token(), next_transition.token());
        assert_ne!(original, next_transition);
    }

    #[tokio::test]
    async fn repeated_publication_revisions_replace_logical_cache_entries() {
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        let repository = ClickHouseConversationRepository::new(client, RepoConfig::default());
        let mut final_token = String::new();

        for epoch in 0..64 {
            let snapshot = PublicationSnapshot::new(
                PublicationConsistencyMode::Local,
                vec![host_state("", 4, "heads-a")],
                vec![control("idle", epoch, false)],
            );
            final_token = snapshot.token().to_string();
            let term_values = HashMap::from([("repeat".to_string(), epoch)]);

            ACTIVE_PUBLICATION_SNAPSHOT
                .scope(snapshot, async {
                    repository
                        .cache_term_df_values(
                            vec!["repeat".to_string()],
                            &term_values,
                            Instant::now(),
                        )
                        .await;
                    repository
                        .insert_scoped_session_cache(
                            "session-a".to_string(),
                            publication_cache_token().expect("idle snapshot cache token"),
                        )
                        .await;
                })
                .await;
        }

        let stats_cache = repository.stats_cache.read().await;
        assert_eq!(stats_cache.term_df_by_term.len(), 1);
        assert_eq!(stats_cache.term_df_by_term["repeat"].df, 63);
        assert_eq!(
            stats_cache.term_df_by_term["repeat"].publication_token,
            final_token
        );
        drop(stats_cache);

        let scope_cache = repository.scoped_session_cache.read().await;
        assert_eq!(scope_cache.len(), 1);
        assert!(scoped_session_cache_contains(
            &scope_cache,
            "session-a",
            &final_token
        ));
    }

    #[test]
    fn insert_only_preparation_fences_only_overlapping_strict_reads() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("preparing", 7, true)],
        );

        assert!(!snapshot.cache_allowed());
        assert!(snapshot.read_allowed(
            PublicationReadClass::MovingFeed,
            &PublicationReadScope::global()
        ));
        assert!(!snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::session("session-a")
        ));
        assert!(snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::session("session-b")
        ));
        assert!(!snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::event("dependency-a")
        ));
        assert!(snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::event("event-b")
        ));
        let resolved_event_scope = PublicationReadScope::event("event-b")
            .union(&PublicationReadScope::session("session-a"));
        assert!(!snapshot.read_allowed(PublicationReadClass::Strict, &resolved_event_scope));
        assert!(!snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::projects(["project-a".to_string()])
        ));
        assert!(!snapshot.read_allowed(
            PublicationReadClass::Strict,
            &source_scope("host-a", "codex", "/sessions/a.jsonl")
        ));
        assert!(snapshot.read_allowed(
            PublicationReadClass::Strict,
            &source_scope("host-a", "codex", "/sessions/b.jsonl")
        ));
    }

    #[test]
    fn unresolved_insert_only_fence_retains_the_same_manifest_scope() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("blocked", 7, true)],
        );

        assert!(snapshot.read_allowed(
            PublicationReadClass::MovingFeed,
            &PublicationReadScope::global()
        ));
        assert!(!snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::session("session-a")
        ));
        assert!(snapshot.read_allowed(
            PublicationReadClass::Strict,
            &PublicationReadScope::session("session-b")
        ));
    }

    #[test]
    fn update_or_unknown_append_control_is_a_backend_wide_fence() {
        for state in ["preparing", "blocked"] {
            let hard_fence = PublicationSnapshot::new(
                PublicationConsistencyMode::Local,
                vec![host_state("", 4, "heads-a")],
                vec![control(state, 7, false)],
            );
            assert!(!hard_fence.read_allowed(
                PublicationReadClass::MovingFeed,
                &PublicationReadScope::global()
            ));
            assert!(!hard_fence.read_allowed(
                PublicationReadClass::Strict,
                &PublicationReadScope::session("session-b")
            ));

            for manifest_json in ["{", r#"{"insert_only":true}"#] {
                let mut unknown_control = control(state, 7, true);
                unknown_control.manifest_json = manifest_json.to_string();
                unknown_control.parse_manifest();
                let unknown = PublicationSnapshot::new(
                    PublicationConsistencyMode::Local,
                    vec![host_state("", 4, "heads-a")],
                    vec![unknown_control],
                );
                assert!(!unknown.read_allowed(
                    PublicationReadClass::MovingFeed,
                    &PublicationReadScope::global()
                ));
                assert!(!unknown.read_allowed(
                    PublicationReadClass::Strict,
                    &PublicationReadScope::session("session-b")
                ));
            }

            let mut unknown_classification = control(state, 7, true);
            unknown_classification.insert_only = 2;
            let unknown = PublicationSnapshot::new(
                PublicationConsistencyMode::Local,
                vec![host_state("", 4, "heads-a")],
                vec![unknown_classification],
            );
            assert!(!unknown.read_allowed(
                PublicationReadClass::MovingFeed,
                &PublicationReadScope::global()
            ));
        }
    }

    #[test]
    fn shared_snapshot_retains_complete_sorted_host_revision_vector() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Shared,
            vec![
                host_state("host-a", 7, "heads-a"),
                host_state("host-b", 3, "heads-b"),
            ],
            Vec::new(),
        );

        assert_eq!(
            snapshot.host_revision_vector(),
            vec![("host-a", 7), ("host-b", 3)]
        );
    }

    #[test]
    fn captured_head_aggregation_qualifies_raw_revision_inputs() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Shared,
            vec![host_state("host-a", 7, "heads-a")],
            Vec::new(),
        );
        let sql = snapshot.captured_source_heads_sql("history");

        assert!(sql.contains(
            "argMax(tuple(history.source_generation, history.publication_revision), history.publication_revision)"
        ));
        assert!(
            sql.contains("GROUP BY history.source_host, history.source_name, history.source_file")
        );
        assert!(!sql.contains("tuple(source_generation, publication_revision)"));
    }

    #[tokio::test]
    async fn captured_live_events_use_portable_final_alias_order() {
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        let repository = ClickHouseConversationRepository::new(client, RepoConfig::default());
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            Vec::new(),
        );

        ACTIVE_PUBLICATION_SNAPSHOT
            .scope(snapshot, async {
                let sql = repository.live_events_source();
                assert!(sql.contains("FROM `moraine`.`events` AS e FINAL"));
                assert!(!sql.contains("FINAL AS e"));
            })
            .await;
    }

    #[tokio::test]
    async fn snapshot_survives_nested_query_id_scope() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("idle", 7, false)],
        );
        let expected = snapshot.token().to_string();

        ACTIVE_PUBLICATION_SNAPSHOT
            .scope(snapshot, async move {
                with_repository_query_id("request-a".to_string(), async move {
                    assert_eq!(
                        active_publication_token().as_deref(),
                        Some(expected.as_str())
                    );
                })
                .await;
            })
            .await;
    }

    #[tokio::test]
    async fn captured_live_events_places_alias_before_final() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("idle", 7, false)],
        );
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        let repository = ClickHouseConversationRepository::new(client, RepoConfig::default());

        ACTIVE_PUBLICATION_SNAPSHOT
            .scope(snapshot, async {
                let sql = repository.live_events_source();
                let unsupported_final_alias = ["FINAL", "AS"].join(" ");
                assert!(sql.contains("AS e FINAL\nALL INNER JOIN"));
                assert!(sql.contains(
                    "argMax(tuple(history.source_generation, history.publication_revision), history.publication_revision)"
                ));
                assert!(sql.contains(
                    "GROUP BY history.source_host, history.source_name, history.source_file"
                ));
                assert!(!sql.contains("tuple(source_generation, publication_revision)"));
                assert!(!sql.contains(&unsupported_final_alias));
            })
            .await;
    }

    #[tokio::test]
    async fn captured_mcp_search_sessions_publish_unqualified_column_names() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("idle", 7, false)],
        );
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        let repository = ClickHouseConversationRepository::new(client, RepoConfig::default());

        ACTIVE_PUBLICATION_SNAPSHOT
            .scope(snapshot, async {
                let sql = repository.mcp_search_sessions_source();

                assert!(sql.contains("h.session_id AS session_id"));
                assert!(sql.contains("toUInt8(h.slot) AS slot"));
                assert!(sql.contains("toUInt64(h.generation) AS generation"));
                assert!(sql.contains("h.first_event_time AS first_event_time"));
                assert!(sql.contains("h.origin_cwd AS origin_cwd"));
                assert!(!sql.contains("SELECT h.*\nFROM head_authorized_headers AS h"));
            })
            .await;
    }

    #[tokio::test]
    async fn attempt_effects_are_buffered_until_the_wrapper_commits_them() {
        let effects = Arc::new(Mutex::new(Vec::new()));
        ACTIVE_PUBLICATION_EFFECTS
            .scope(effects.clone(), async {
                let deferred = defer_publication_effect(PublicationEffect::SearchTelemetry {
                    query_row: json!({"query_id": "query-a"}),
                    hit_rows: vec![json!({"event_uid": "event-a"})],
                })
                .await;
                assert!(deferred.is_none());

                let deferred =
                    defer_publication_effect(PublicationEffect::ScopedSessionCacheInsert {
                        session_id: "session-a".to_string(),
                        publication_token: "snapshot-a".to_string(),
                    })
                    .await;
                assert!(deferred.is_none());

                let deferred =
                    defer_publication_effect(PublicationEffect::FileAttentionProjectRootsWrite {
                        sql: "INSERT INTO roots VALUES ('project-a', '/repo')".to_string(),
                    })
                    .await;
                assert!(deferred.is_none());
            })
            .await;

        assert_eq!(effects.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn accepted_non_idle_snapshot_discards_every_buffered_effect() {
        let snapshot = PublicationSnapshot::new(
            PublicationConsistencyMode::Local,
            vec![host_state("", 4, "heads-a")],
            vec![control("preparing", 7, true)],
        );
        let effects = Mutex::new(vec![
            PublicationEffect::SearchTelemetry {
                query_row: json!({"query_id": "query-a"}),
                hit_rows: vec![json!({"event_uid": "event-a"})],
            },
            PublicationEffect::ScopedSessionCacheInsert {
                session_id: "session-a".to_string(),
                publication_token: "snapshot-a".to_string(),
            },
            PublicationEffect::FileAttentionProjectRootsWrite {
                sql: "INSERT INTO roots VALUES ('project-a', '/repo')".to_string(),
            },
        ]);

        let committable = take_committable_effects(&snapshot, &effects).await;

        assert!(committable.is_empty());
        assert!(effects.lock().await.is_empty());
    }

    #[tokio::test]
    async fn effects_remain_immediate_without_an_operation_snapshot() {
        let effect = defer_publication_effect(PublicationEffect::ScopedSessionCacheInsert {
            session_id: "session-a".to_string(),
            publication_token: "legacy".to_string(),
        })
        .await;
        assert!(matches!(
            effect,
            Some(PublicationEffect::ScopedSessionCacheInsert {
                session_id,
                publication_token,
            }) if session_id == "session-a" && publication_token == "legacy"
        ));

        let effect = defer_publication_effect(PublicationEffect::FileAttentionProjectRootsWrite {
            sql: "INSERT INTO roots VALUES ('project-a', '/repo')".to_string(),
        })
        .await;
        assert!(matches!(
            effect,
            Some(PublicationEffect::FileAttentionProjectRootsWrite { sql })
                if sql.contains("INSERT INTO roots")
        ));
    }

    #[tokio::test]
    async fn discarding_an_attempt_does_not_populate_the_scope_cache() {
        let client = ClickHouseClient::new(moraine_config::ClickHouseConfig::default())
            .expect("build ClickHouse client");
        let repository = ClickHouseConversationRepository::new(client, RepoConfig::default());
        let effects = Arc::new(Mutex::new(Vec::new()));

        ACTIVE_PUBLICATION_EFFECTS
            .scope(effects.clone(), async {
                assert!(
                    defer_publication_effect(PublicationEffect::ScopedSessionCacheInsert {
                        session_id: "session-a".to_string(),
                        publication_token: "snapshot-a".to_string(),
                    })
                    .await
                    .is_none()
                );
            })
            .await;

        // A changed publication token drops this vector rather than flushing
        // it. In particular, retrying must not authorize the next attempt via
        // a positive cache result obtained from the discarded attempt.
        drop(effects);
        assert!(repository.scoped_session_cache.read().await.is_empty());
    }
}
