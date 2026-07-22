use super::{escape_identifier, escape_literal, migration_request_timeout, ClickHouseClient};
use anyhow::{bail, Context, Result};
use futures_util::{stream, TryStreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const BACKFILL_PAGE_SIZE: usize = 64;
const REFRESH_CONCURRENCY: usize = 4;
const MAX_UNSTABLE_REFRESH_ATTEMPTS: usize = 8;
const MAX_PROJECTED_TEXT_SUMMARY_CHARS: usize = 65_536;
const MAX_PROJECTED_PAYLOAD_SUMMARY_CHARS: usize = 131_071;

/// One exact source head required by a prepared MCP compatibility candidate.
///
/// Ordering is part of the durable protocol: callers may supply heads in any
/// order, but the projector always stores and fingerprints the canonical
/// sorted, de-duplicated representation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct McpOpenSourceHead {
    pub source_host: String,
    pub source_name: String,
    pub source_file: String,
    pub source_generation: u32,
    pub publication_revision: u64,
}

/// A bounded replacement-generation compatibility preparation request.
///
/// `captured_host_revisions` is a compact causal token for the complete
/// published backend head set. Per-session preparation expands only the heads
/// that can contribute to that session, plus the target source head so a
/// disappearance tombstone cannot authorize before the source cutover.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct McpOpenHostRevision {
    pub source_host: String,
    pub publication_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOpenPublicationRequest {
    pub candidate_publication_id: String,
    pub operation_id: String,
    pub publisher_id: String,
    pub source_host: String,
    pub source_name: String,
    pub source_file: String,
    pub previous_source_generation: Option<u32>,
    pub source_generation: u32,
    pub publication_revision: u64,
    pub captured_host_revisions: Vec<McpOpenHostRevision>,
}

/// Durable preparation counts consumed by the source-publication actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOpenGenerationReadiness {
    pub candidate_publication_id: String,
    pub affected_session_count: u64,
    pub prepared_session_count: u64,
    pub tombstone_count: u64,
}

#[derive(Debug, Deserialize)]
struct SessionHeadRow {
    slot: u8,
}

#[derive(Debug, Deserialize)]
struct CandidateHeaderRow {
    session_id: String,
    slot: u8,
    generation: u64,
    source_revision: u64,
    dirty_revision: u64,
    tombstone: u8,
    required_heads_fingerprint: String,
}

struct CandidateHeaderInsert<'a> {
    session_id: &'a str,
    candidate_publication_id: &'a str,
    operation_id: &'a str,
    publisher_id: &'a str,
    slot: u8,
    generation: u64,
    source_revision: u64,
    dirty_revision: u64,
    required_heads: &'a [McpOpenSourceHead],
}

struct McpOpenSessionRefreshRequest<'a> {
    session_id: &'a str,
    candidate_publication_id: &'a str,
    operation_id: &'a str,
    publisher_id: &'a str,
    required_heads: &'a [McpOpenSourceHead],
    publish_legacy_head: bool,
    first_generation: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SourceRevisionRow {
    source_revision: u64,
}

#[derive(Debug, Deserialize)]
struct DirtyRevisionRow {
    dirty_revision: u64,
}

#[derive(Debug, Deserialize)]
struct SessionIdRow {
    session_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SourceHeadRow {
    source_host: String,
    source_name: String,
    source_file: String,
    source_generation: u32,
    publication_revision: u64,
}

#[derive(Debug, Deserialize)]
struct RequiredHeadsRow {
    required_source_heads: Vec<SourceHeadRow>,
}

#[derive(Debug, Deserialize)]
struct ReadyRow {
    ready: u8,
}

#[derive(Debug, Deserialize)]
struct ProjectionStateRow {
    ready: u8,
    #[serde(default)]
    backfill_cursor: String,
}

#[derive(Debug, Deserialize)]
struct BackfillFactRow {
    session_id: String,
    slot: u8,
    source_revision: u64,
    dirty_revision: u64,
    required_source_heads: Vec<SourceHeadRow>,
}

#[derive(Debug, Clone, Deserialize)]
struct BackfillPlanRow {
    session_id: String,
    candidate_publication_id: String,
    slot: u8,
    candidate_generation: u64,
    source_revision: u64,
    dirty_revision: u64,
    required_source_heads: Vec<SourceHeadRow>,
    required_heads_fingerprint: String,
}

#[derive(Debug, Deserialize)]
struct BackfillCompleteRow {
    complete: u64,
}

#[derive(Debug, Clone, Copy)]
enum BackfillPhaseGuard {
    LegacyHead,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum BackfillPhase {
    Planned = 0,
    EventsInserted = 1,
    TurnsInserted = 2,
    Complete = 4,
}

impl BackfillPhase {
    const fn value(self) -> u8 {
        self as u8
    }
}

fn projection_session_ids<I, S>(session_ids: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    session_ids
        .into_iter()
        .map(|session_id| session_id.as_ref().to_string())
        .filter(|session_id| !session_id.is_empty())
        .collect()
}

fn canonical_source_heads(heads: &[McpOpenSourceHead]) -> Result<Vec<McpOpenSourceHead>> {
    let mut by_key = BTreeMap::new();
    for head in heads {
        let key = (
            head.source_host.clone(),
            head.source_name.clone(),
            head.source_file.clone(),
        );
        if let Some(previous) = by_key.insert(key.clone(), head.clone()) {
            if previous != *head {
                bail!(
                    "conflicting MCP source heads for ({:?}, {:?}, {:?})",
                    key.0,
                    key.1,
                    key.2
                );
            }
        }
    }
    Ok(by_key.into_values().collect())
}

fn source_heads_fingerprint(heads: &[McpOpenSourceHead]) -> String {
    let mut hasher = Sha256::new();
    for head in heads {
        for value in [
            head.source_host.as_bytes(),
            head.source_name.as_bytes(),
            head.source_file.as_bytes(),
        ] {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value);
        }
        hasher.update(head.source_generation.to_be_bytes());
        hasher.update(head.publication_revision.to_be_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn source_heads_sql(heads: &[McpOpenSourceHead]) -> String {
    let rows = heads
        .iter()
        .map(|head| {
            format!(
                "tuple({}, {}, {}, toUInt32({}), toUInt64({}))",
                escape_literal(&head.source_host),
                escape_literal(&head.source_name),
                escape_literal(&head.source_file),
                head.source_generation,
                head.publication_revision,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{rows}]")
}
fn host_revisions_sql(revisions: &[McpOpenHostRevision]) -> String {
    let rows = revisions
        .iter()
        .map(|revision| {
            format!(
                "tuple({}, toUInt64({}))",
                escape_literal(&revision.source_host),
                revision.publication_revision,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CAST([{rows}], 'Array(Tuple(String, UInt64))')")
}

fn source_head_filter(heads: &[McpOpenSourceHead], alias: &str) -> String {
    if heads.is_empty() {
        return "0".to_string();
    }
    heads
        .iter()
        .map(|head| {
            format!(
                "({alias}.source_host = {} AND {alias}.source_name = {} AND {alias}.source_file = {} AND {alias}.source_generation = {})",
                escape_literal(&head.source_host),
                escape_literal(&head.source_name),
                escape_literal(&head.source_file),
                head.source_generation,
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn source_head_key_eq(left: &McpOpenSourceHead, right: &McpOpenSourceHead) -> bool {
    left.source_host == right.source_host
        && left.source_name == right.source_name
        && left.source_file == right.source_file
}

fn source_head_from_row(row: SourceHeadRow) -> McpOpenSourceHead {
    McpOpenSourceHead {
        source_host: row.source_host,
        source_name: row.source_name,
        source_file: row.source_file,
        source_generation: row.source_generation,
        publication_revision: row.publication_revision,
    }
}

impl ClickHouseClient {
    /// Rebuild complete canonical snapshots for the affected sessions and
    /// publish each session head only after its inactive children are durable.
    pub async fn refresh_mcp_open_read_model<I, S>(&self, session_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let session_ids = projection_session_ids(session_ids);

        stream::iter(session_ids.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(REFRESH_CONCURRENCY, |session_id| async move {
                let required_heads = self.current_session_source_heads(&session_id).await?;
                if required_heads.is_empty() {
                    return Ok(());
                }
                // One Snowflake can identify both this one-session candidate
                // and its first immutable child snapshot.
                let first_generation = self.next_projection_generation().await?;
                let candidate_publication_id =
                    format!("append:{}:{}", session_id, first_generation);
                self.refresh_mcp_open_session(McpOpenSessionRefreshRequest {
                    session_id: &session_id,
                    candidate_publication_id: &candidate_publication_id,
                    operation_id: &candidate_publication_id,
                    publisher_id: "ordinary-append",
                    required_heads: &required_heads,
                    publish_legacy_head: true,
                    first_generation: Some(first_generation),
                })
                .await
                .map(|_| ())
            })
            .await
    }

    /// Prepare one replacement generation's complete old/new affected-session
    /// union without changing the legacy session pointer or source head.
    ///
    /// The returned readiness row is persisted only after every candidate
    /// header (including disappearances) is durable. Callers must switch the
    /// source head after this succeeds, then invoke
    /// [`activate_mcp_open_publication`](Self::activate_mcp_open_publication)
    /// once to reconcile the compatibility pointer.
    pub async fn prepare_mcp_open_publication(
        &self,
        request: &McpOpenPublicationRequest,
    ) -> Result<McpOpenGenerationReadiness> {
        if request.candidate_publication_id.trim().is_empty() {
            bail!("MCP candidate publication id cannot be empty");
        }
        if request.operation_id.trim().is_empty() {
            bail!("MCP publication operation id cannot be empty");
        }

        let target_head = McpOpenSourceHead {
            source_host: request.source_host.clone(),
            source_name: request.source_name.clone(),
            source_file: request.source_file.clone(),
            source_generation: request.source_generation,
            publication_revision: request.publication_revision,
        };
        let affected_sessions = self
            .mcp_open_affected_sessions(request.previous_source_generation, &target_head)
            .await?;
        let affected_session_count = affected_sessions.len() as u64;
        let mut prepared_session_count = 0_u64;
        let mut tombstone_count = 0_u64;

        for session_id in affected_sessions {
            let mut session_heads = self
                .session_source_heads_for_snapshot(&session_id, request, &target_head)
                .await?;
            if !session_heads
                .iter()
                .any(|head| source_head_key_eq(head, &target_head))
            {
                session_heads.push(target_head.clone());
                session_heads = canonical_source_heads(&session_heads)?;
            }

            let has_live_events = self
                .session_source_revision_for_heads(&session_id, &session_heads)
                .await?
                != 0;
            if has_live_events {
                self.refresh_mcp_open_session(McpOpenSessionRefreshRequest {
                    session_id: &session_id,
                    candidate_publication_id: &request.candidate_publication_id,
                    operation_id: &request.operation_id,
                    publisher_id: &request.publisher_id,
                    required_heads: &session_heads,
                    publish_legacy_head: false,
                    first_generation: None,
                })
                .await?;
                prepared_session_count += 1;
            } else {
                tombstone_count += 1;
                prepared_session_count += 1;
                self.insert_mcp_open_tombstone(
                    &session_id,
                    &request.candidate_publication_id,
                    &request.operation_id,
                    &request.publisher_id,
                    &session_heads,
                )
                .await?;
            }
        }

        let readiness = McpOpenGenerationReadiness {
            candidate_publication_id: request.candidate_publication_id.clone(),
            affected_session_count,
            prepared_session_count,
            tombstone_count,
        };
        self.insert_mcp_open_generation_readiness(request, &readiness, true, "")
            .await?;
        Ok(readiness)
    }

    /// Revalidate an already prepared candidate against the now-current source
    /// heads and dirty/source revisions, then update the legacy compatibility
    /// session pointers. Header authorization remains the primary read path.
    pub async fn activate_mcp_open_publication(
        &self,
        candidate_publication_id: &str,
    ) -> Result<bool> {
        let candidates = self
            .mcp_open_candidate_headers(candidate_publication_id)
            .await?;
        if candidates.is_empty() {
            return self
                .mcp_open_candidate_generation_ready(candidate_publication_id)
                .await;
        }
        for candidate in &candidates {
            let heads = self
                .mcp_open_candidate_required_heads(&candidate.session_id, candidate_publication_id)
                .await?;
            if !self.source_heads_are_current(&heads).await?
                || self.session_dirty_revision(&candidate.session_id).await?
                    != candidate.dirty_revision
                || self
                    .session_source_revision_for_heads(&candidate.session_id, &heads)
                    .await?
                    != candidate.source_revision
            {
                return Ok(false);
            }
        }
        self.publish_legacy_candidate_heads(candidate_publication_id)
            .await?;
        Ok(true)
    }

    /// Resume the historical projection and reconcile sessions dirtied while
    /// the DDL/backfill was running. Safe to call after every migrate command.
    pub async fn backfill_mcp_open_read_model(&self) -> Result<()> {
        self.backfill_mcp_open_read_model_with_progress(|_| {})
            .await
    }

    /// Backfill the read model and report the cumulative number of refreshed
    /// sessions after each bounded page.
    pub async fn backfill_mcp_open_read_model_with_progress<F>(
        &self,
        mut on_progress: F,
    ) -> Result<()>
    where
        F: FnMut(usize),
    {
        let mut refreshed_sessions = 0;
        let state = self.mcp_open_projection_state().await?;
        let historical_complete = state.as_ref().is_some_and(|state| state.ready == 1);
        let mut cursor = state.map_or_else(String::new, |state| state.backfill_cursor);

        if !historical_complete {
            loop {
                let query = format!(
                    "SELECT session_id\n\
                     FROM (\n\
                       SELECT DISTINCT session_id\n\
                       FROM {}.v_live_events\n\
                       WHERE session_id > {}\n\
                         AND notEmpty(session_id)\n\
                     )\n\
                     ORDER BY session_id ASC\n\
                     LIMIT {}\n\
                     FORMAT JSONEachRow",
                    escape_identifier(&self.cfg.database),
                    escape_literal(&cursor),
                    BACKFILL_PAGE_SIZE,
                );
                let rows: Vec<SessionIdRow> = self
                    .query_json_each_row(&query, Some(&self.cfg.database))
                    .await
                    .context("failed to page MCP open backfill sessions")?;
                if rows.is_empty() {
                    break;
                }
                self.refresh_mcp_open_read_model_batch(
                    rows.iter().map(|row| row.session_id.as_str()),
                )
                .await?;
                cursor = rows.last().expect("non-empty page").session_id.clone();
                self.set_mcp_open_projection_state(false, &cursor).await?;
                refreshed_sessions += rows.len();
                on_progress(refreshed_sessions);
            }
        }

        loop {
            let query = format!(
                "SELECT d.session_id AS session_id\n\
                 FROM (\n\
                   SELECT session_id, dirty_revision\n\
                   FROM {}.mcp_open_dirty_sessions FINAL\n\
                 ) AS d\n\
                 INNER JOIN (\n\
                   SELECT DISTINCT session_id\n\
                   FROM {}.v_live_events\n\
                   WHERE notEmpty(session_id)\n\
                 ) AS live ON live.session_id = d.session_id\n\
                 LEFT JOIN (\n\
                   SELECT session_id, dirty_revision\n\
                   FROM {}.mcp_open_sessions FINAL\n\
                 ) AS s ON s.session_id = d.session_id\n\
                 WHERE notEmpty(d.session_id)\n\
                   AND d.dirty_revision > ifNull(s.dirty_revision, 0)\n\
                 ORDER BY d.session_id ASC\n\
                 LIMIT {}\n\
                 FORMAT JSONEachRow",
                escape_identifier(&self.cfg.database),
                escape_identifier(&self.cfg.database),
                escape_identifier(&self.cfg.database),
                BACKFILL_PAGE_SIZE,
            );
            let rows: Vec<SessionIdRow> = self
                .query_json_each_row(&query, Some(&self.cfg.database))
                .await
                .context("failed to read dirty MCP open sessions")?;
            if rows.is_empty() {
                break;
            }
            self.refresh_mcp_open_read_model_batch(rows.iter().map(|row| row.session_id.as_str()))
                .await?;
            refreshed_sessions += rows.len();
            on_progress(refreshed_sessions);
        }

        self.set_mcp_open_projection_state(true, "").await?;
        Ok(())
    }

    async fn refresh_mcp_open_read_model_batch<I, S>(&self, session_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let session_ids = projection_session_ids(session_ids);
        if session_ids.is_empty() {
            return Ok(());
        }

        for _attempt in 0..MAX_UNSTABLE_REFRESH_ATTEMPTS {
            self.prepare_mcp_open_backfill_plans(&session_ids).await?;

            self.execute_mcp_open_backfill_insert(batch_projected_events_sql(
                &self.cfg.database,
                &session_ids,
            ))
            .await?;
            self.advance_mcp_open_backfill_phase(
                &session_ids,
                BackfillPhase::Planned,
                BackfillPhase::EventsInserted,
                None,
            )
            .await?;

            self.execute_mcp_open_backfill_insert(batch_projected_turns_sql(
                &self.cfg.database,
                &session_ids,
            ))
            .await?;
            self.advance_mcp_open_backfill_phase(
                &session_ids,
                BackfillPhase::EventsInserted,
                BackfillPhase::TurnsInserted,
                None,
            )
            .await?;

            // A prior attempt may have published the header and legacy head but
            // lost the response before recording completion. Recover that exact
            // durable state before issuing any per-session finalization writes.
            self.advance_mcp_open_backfill_phase(
                &session_ids,
                BackfillPhase::TurnsInserted,
                BackfillPhase::Complete,
                Some(BackfillPhaseGuard::LegacyHead),
            )
            .await?;
            self.finalize_mcp_open_backfill_plans(&session_ids).await?;
            self.advance_mcp_open_backfill_phase(
                &session_ids,
                BackfillPhase::TurnsInserted,
                BackfillPhase::Complete,
                Some(BackfillPhaseGuard::LegacyHead),
            )
            .await?;

            if self.mcp_open_backfill_batch_complete(&session_ids).await? {
                return Ok(());
            }
        }

        bail!(
            "MCP open projection source kept changing for a {}-session batch after {} attempts",
            session_ids.len(),
            MAX_UNSTABLE_REFRESH_ATTEMPTS
        )
    }

    async fn prepare_mcp_open_backfill_plans(&self, session_ids: &BTreeSet<String>) -> Result<()> {
        let facts: Vec<BackfillFactRow> = self
            .mcp_open_backfill_query(&backfill_facts_sql(&self.cfg.database, session_ids))
            .await
            .context("failed to read MCP open batch facts")?;
        if facts.len() != session_ids.len() {
            bail!(
                "MCP open batch facts covered {} of {} requested sessions",
                facts.len(),
                session_ids.len()
            );
        }
        let current_plans: Vec<BackfillPlanRow> = self
            .mcp_open_backfill_query(&backfill_plans_sql(&self.cfg.database, session_ids))
            .await
            .context("failed to read MCP open batch plans")?;
        let current_plans = current_plans
            .into_iter()
            .map(|plan| (plan.session_id.clone(), plan))
            .collect::<BTreeMap<_, _>>();

        let mut replacements = Vec::new();
        for fact in facts {
            let required_heads = canonical_source_heads(
                &fact
                    .required_source_heads
                    .iter()
                    .cloned()
                    .map(source_head_from_row)
                    .collect::<Vec<_>>(),
            )?;
            let fingerprint = source_heads_fingerprint(&required_heads);
            let reusable = current_plans.get(&fact.session_id).is_some_and(|plan| {
                plan.source_revision == fact.source_revision
                    && plan.dirty_revision == fact.dirty_revision
                    && plan.required_heads_fingerprint == fingerprint
            });
            if !reusable {
                replacements.push((fact, required_heads, fingerprint));
            }
        }
        if replacements.is_empty() {
            return Ok(());
        }

        let generation = self.next_projection_generation().await?;
        let values = replacements
            .into_iter()
            .map(|(fact, required_heads, fingerprint)| {
                let candidate_publication_id =
                    format!("backfill:{generation}:{}", fact.session_id);
                format!(
                    "({}, {}, {}, toUInt64({generation}), toUInt64({}), toUInt64({}), {}, {}, toUInt8({}), toUInt64({generation}))",
                    escape_literal(&fact.session_id),
                    escape_literal(&candidate_publication_id),
                    fact.slot,
                    fact.source_revision,
                    fact.dirty_revision,
                    source_heads_sql(&required_heads),
                    escape_literal(&fingerprint),
                    BackfillPhase::Planned.value(),
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        let statement = format!(
            "INSERT INTO {}.mcp_open_backfill_plans\n\
             (session_id, candidate_publication_id, slot, candidate_generation, source_revision,\n\
              dirty_revision, required_source_heads, required_heads_fingerprint, phase, plan_revision)\n\
             VALUES {values}",
            escape_identifier(&self.cfg.database),
        );
        self.execute_mcp_open_backfill_insert(statement).await
    }

    async fn finalize_mcp_open_backfill_plans(&self, session_ids: &BTreeSet<String>) -> Result<()> {
        let plans: Vec<BackfillPlanRow> = self
            .mcp_open_backfill_query(&backfill_phase_plans_sql(
                &self.cfg.database,
                session_ids,
                Some(BackfillPhase::TurnsInserted),
            ))
            .await?;
        stream::iter(plans.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(REFRESH_CONCURRENCY, |plan| async move {
                let required_heads = canonical_source_heads(
                    &plan
                        .required_source_heads
                        .into_iter()
                        .map(source_head_from_row)
                        .collect::<Vec<_>>(),
                )?;
                let candidate = self
                    .insert_projected_publication_header(CandidateHeaderInsert {
                        session_id: &plan.session_id,
                        candidate_publication_id: &plan.candidate_publication_id,
                        operation_id: &plan.candidate_publication_id,
                        publisher_id: "historical-backfill",
                        slot: plan.slot,
                        generation: plan.candidate_generation,
                        source_revision: plan.source_revision,
                        dirty_revision: plan.dirty_revision,
                        required_heads: &required_heads,
                    })
                    .await?;
                if candidate.is_some() {
                    self.publish_legacy_session_candidate_head(
                        &plan.session_id,
                        &plan.candidate_publication_id,
                    )
                    .await?;
                }
                Ok(())
            })
            .await
    }

    async fn advance_mcp_open_backfill_phase(
        &self,
        session_ids: &BTreeSet<String>,
        from_phase: BackfillPhase,
        to_phase: BackfillPhase,
        guard: Option<BackfillPhaseGuard>,
    ) -> Result<()> {
        self.execute_mcp_open_backfill_insert(backfill_phase_advance_sql(
            &self.cfg.database,
            session_ids,
            from_phase,
            to_phase,
            guard,
        ))
        .await
    }

    async fn mcp_open_backfill_batch_complete(
        &self,
        session_ids: &BTreeSet<String>,
    ) -> Result<bool> {
        let rows: Vec<BackfillCompleteRow> = self
            .mcp_open_backfill_query(&backfill_complete_sql(&self.cfg.database, session_ids))
            .await?;
        Ok(rows
            .first()
            .is_some_and(|row| row.complete == session_ids.len() as u64))
    }

    async fn execute_mcp_open_backfill_insert(&self, statement: String) -> Result<()> {
        // Generated plan rows contain complete source-head paths and can make
        // a batch much larger than an HTTP request target. Carry the SQL in
        // the POST body so backfill size is bounded by the payload limit, not
        // by URI parsing limits.
        self.request_text_with_params_and_timeout(
            "",
            Some(statement.into_bytes()),
            Some(&self.cfg.database),
            false,
            None,
            &[],
            Some(migration_request_timeout(self.cfg.timeout_seconds)),
        )
        .await?;
        Ok(())
    }

    async fn mcp_open_backfill_query<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        let raw = self
            .request_text_with_params_and_timeout(
                query,
                None,
                Some(&self.cfg.database),
                false,
                None,
                &[],
                Some(migration_request_timeout(self.cfg.timeout_seconds)),
            )
            .await?;
        serde_json::Deserializer::from_str(&raw)
            .into_iter::<T>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to parse MCP open batch JSONEachRow response")
    }

    pub async fn mcp_open_read_model_ready(&self) -> Result<bool> {
        Ok(self
            .mcp_open_projection_state()
            .await?
            .is_some_and(|state| state.ready == 1))
    }

    async fn mcp_open_projection_state(&self) -> Result<Option<ProjectionStateRow>> {
        let query = format!(
            "SELECT ready, backfill_cursor\n\
             FROM {}.mcp_open_projection_state FINAL\n\
             WHERE state_key = 'global'\n\
               AND (SELECT count() FROM system.tables\n\
                    WHERE database = {}\n\
                      AND name IN ('mcp_open_publication_headers', 'mcp_open_generation_readiness')) = 2\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(&self.cfg.database),
        );
        let rows: Vec<ProjectionStateRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn set_mcp_open_projection_state(&self, ready: bool, cursor: &str) -> Result<()> {
        let statement = format!(
            "INSERT INTO {}.mcp_open_projection_state\n\
             (state_key, ready, generation, backfill_cursor)\n\
             VALUES ('global', {}, generateSnowflakeID(), {})",
            escape_identifier(&self.cfg.database),
            u8::from(ready),
            escape_literal(cursor),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn refresh_mcp_open_session(
        &self,
        request: McpOpenSessionRefreshRequest<'_>,
    ) -> Result<CandidateHeaderRow> {
        let McpOpenSessionRefreshRequest {
            session_id,
            candidate_publication_id,
            operation_id,
            publisher_id,
            required_heads,
            publish_legacy_head,
            first_generation,
        } = request;
        for attempt in 0..MAX_UNSTABLE_REFRESH_ATTEMPTS {
            let source_revision = self
                .session_source_revision_for_heads(session_id, required_heads)
                .await?;
            if source_revision == 0 {
                bail!("cannot prepare a non-tombstone MCP session with no authorized events");
            }
            let dirty_revision = self.session_dirty_revision(session_id).await?;
            // The first ordinary-append attempt embeds the Snowflake allocated
            // by this invocation in its candidate id, so that id cannot exist
            // yet. Stable replacement ids and retry attempts still probe for
            // a durable candidate, preserving response-loss recovery.
            let candidate = if attempt == 0 && first_generation.is_some() {
                None
            } else {
                self.mcp_open_candidate_header(session_id, candidate_publication_id)
                    .await?
            };
            if let Some(candidate) = candidate {
                if candidate.source_revision == source_revision
                    && candidate.dirty_revision == dirty_revision
                    && candidate.required_heads_fingerprint
                        == source_heads_fingerprint(required_heads)
                {
                    if publish_legacy_head {
                        self.publish_legacy_session_candidate_head(
                            session_id,
                            candidate_publication_id,
                        )
                        .await?;
                    }
                    return Ok(candidate);
                }
            }

            let head = self.mcp_open_session_head(session_id).await?;
            let slot = head
                .as_ref()
                .map_or(0, |head| 1_u8.saturating_sub(head.slot));
            let generation = match (attempt, first_generation) {
                (0, Some(generation)) => generation,
                _ => self.next_projection_generation().await?,
            };

            self.insert_projected_events(
                session_id,
                slot,
                generation,
                source_revision,
                required_heads,
            )
            .await?;
            self.insert_projected_turns(
                session_id,
                slot,
                generation,
                source_revision,
                required_heads,
            )
            .await?;

            // The header statement recomputes the canonical source revision
            // and gates the current dirty revision atomically. A miss returns
            // no candidate and retries, so separate revalidation reads here
            // only lengthen the publication window.
            let inserted = self
                .insert_projected_publication_header(CandidateHeaderInsert {
                    session_id,
                    candidate_publication_id,
                    operation_id,
                    publisher_id,
                    slot,
                    generation,
                    source_revision,
                    dirty_revision,
                    required_heads,
                })
                .await?;
            if let Some(candidate) = inserted {
                if publish_legacy_head {
                    self.publish_legacy_session_candidate_head(
                        session_id,
                        candidate_publication_id,
                    )
                    .await?;
                }
                return Ok(candidate);
            }
        }

        bail!(
            "MCP open projection source kept changing for session after {} attempts",
            MAX_UNSTABLE_REFRESH_ATTEMPTS
        )
    }

    async fn mcp_open_session_head(&self, session_id: &str) -> Result<Option<SessionHeadRow>> {
        let query = format!(
            "SELECT slot\n\
             FROM {}.mcp_open_sessions FINAL\n\
             WHERE session_id = {}\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
        );
        let rows: Vec<SessionHeadRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn next_projection_generation(&self) -> Result<u64> {
        let rows: Vec<SourceRevisionRow> = self
            .query_json_each_row(
                "SELECT toUInt64(generateSnowflakeID()) AS source_revision FORMAT JSONEachRow",
                Some(&self.cfg.database),
            )
            .await?;
        rows.first()
            .map(|row| row.source_revision)
            .context("ClickHouse did not allocate an MCP open projection generation")
    }

    async fn current_session_source_heads(
        &self,
        session_id: &str,
    ) -> Result<Vec<McpOpenSourceHead>> {
        let query = current_session_source_heads_sql(&self.cfg.database, session_id);
        let rows: Vec<SourceHeadRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        canonical_source_heads(
            &rows
                .into_iter()
                .map(source_head_from_row)
                .collect::<Vec<_>>(),
        )
    }

    async fn session_source_heads_for_snapshot(
        &self,
        session_id: &str,
        request: &McpOpenPublicationRequest,
        target: &McpOpenSourceHead,
    ) -> Result<Vec<McpOpenSourceHead>> {
        let query = session_source_heads_for_snapshot_sql(
            &self.cfg.database,
            session_id,
            &request.captured_host_revisions,
            target,
        );
        let rows: Vec<SourceHeadRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        canonical_source_heads(
            &rows
                .into_iter()
                .map(source_head_from_row)
                .collect::<Vec<_>>(),
        )
    }

    async fn session_source_revision_for_heads(
        &self,
        session_id: &str,
        required_heads: &[McpOpenSourceHead],
    ) -> Result<u64> {
        let query = session_source_revision_sql(&self.cfg.database, session_id, required_heads);
        let rows: Vec<SourceRevisionRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.first().map_or(0, |row| row.source_revision))
    }

    async fn session_dirty_revision(&self, session_id: &str) -> Result<u64> {
        let query = format!(
            "SELECT dirty_revision\n\
             FROM {}.mcp_open_dirty_sessions FINAL\n\
             WHERE session_id = {}\n\
             LIMIT 1\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
        );
        let rows: Vec<DirtyRevisionRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.first().map_or(0, |row| row.dirty_revision))
    }

    async fn mcp_open_affected_sessions(
        &self,
        previous_generation: Option<u32>,
        target: &McpOpenSourceHead,
    ) -> Result<Vec<String>> {
        let generation_predicate = previous_generation.map_or_else(
            || format!("source_generation = {}", target.source_generation),
            |previous| {
                format!(
                    "source_generation IN ({previous}, {})",
                    target.source_generation
                )
            },
        );
        let query = format!(
            "SELECT DISTINCT session_id\n\
             FROM {}.events FINAL\n\
             WHERE source_host = {} AND source_name = {} AND source_file = {}\n\
               AND {generation_predicate} AND notEmpty(session_id)\n\
             ORDER BY session_id\n\
             FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(&target.source_host),
            escape_literal(&target.source_name),
            escape_literal(&target.source_file),
        );
        let rows: Vec<SessionIdRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().map(|row| row.session_id).collect())
    }

    async fn mcp_open_candidate_header(
        &self,
        session_id: &str,
        candidate_publication_id: &str,
    ) -> Result<Option<CandidateHeaderRow>> {
        let query = format!(
            "SELECT session_id, slot, generation, source_revision, dirty_revision, tombstone, required_heads_fingerprint\n\
             FROM {}.mcp_open_publication_headers FINAL\n\
             WHERE session_id = {} AND candidate_publication_id = {}\n\
             LIMIT 1 FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
            escape_literal(candidate_publication_id),
        );
        let rows: Vec<CandidateHeaderRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn mcp_open_candidate_headers(
        &self,
        candidate_publication_id: &str,
    ) -> Result<Vec<CandidateHeaderRow>> {
        let query = format!(
            "SELECT session_id, slot, generation, source_revision, dirty_revision, tombstone, required_heads_fingerprint\n\
             FROM {}.mcp_open_publication_headers FINAL\n\
             WHERE candidate_publication_id = {}\n\
             ORDER BY session_id FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(candidate_publication_id),
        );
        self.query_json_each_row(&query, Some(&self.cfg.database))
            .await
    }

    async fn mcp_open_candidate_generation_ready(
        &self,
        candidate_publication_id: &str,
    ) -> Result<bool> {
        let query = format!(
            "SELECT toUInt8(countIf(ready = 1) > 0) AS ready\n\
             FROM {}.v_current_mcp_open_generation_readiness\n\
             WHERE candidate_publication_id = {} FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(candidate_publication_id),
        );
        let rows: Vec<ReadyRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.first().is_some_and(|row| row.ready != 0))
    }

    async fn mcp_open_candidate_required_heads(
        &self,
        session_id: &str,
        candidate_publication_id: &str,
    ) -> Result<Vec<McpOpenSourceHead>> {
        let query = format!(
            "SELECT required_source_heads\n\
             FROM {}.mcp_open_publication_headers FINAL\n\
             WHERE session_id = {} AND candidate_publication_id = {}\n\
             LIMIT 1 FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_literal(session_id),
            escape_literal(candidate_publication_id),
        );
        let rows: Vec<RequiredHeadsRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(Vec::new());
        };
        canonical_source_heads(
            &row.required_source_heads
                .into_iter()
                .map(source_head_from_row)
                .collect::<Vec<_>>(),
        )
    }

    async fn source_heads_are_current(&self, required: &[McpOpenSourceHead]) -> Result<bool> {
        if required.is_empty() {
            return Ok(false);
        }
        let key_filter = required
            .iter()
            .map(|head| {
                format!(
                    "(source_host = {} AND source_name = {} AND source_file = {})",
                    escape_literal(&head.source_host),
                    escape_literal(&head.source_name),
                    escape_literal(&head.source_file),
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let query = format!(
            "SELECT source_host, source_name, source_file, source_generation, publication_revision\n\
             FROM {}.v_current_published_source_generations\n\
             WHERE {key_filter}\n\
             ORDER BY source_host, source_name, source_file FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
        );
        let rows: Vec<SourceHeadRow> = self
            .query_json_each_row(&query, Some(&self.cfg.database))
            .await?;
        let current = canonical_source_heads(
            &rows
                .into_iter()
                .map(source_head_from_row)
                .collect::<Vec<_>>(),
        )?;
        Ok(current == canonical_source_heads(required)?)
    }

    async fn insert_mcp_open_tombstone(
        &self,
        session_id: &str,
        candidate_publication_id: &str,
        operation_id: &str,
        publisher_id: &str,
        required_heads: &[McpOpenSourceHead],
    ) -> Result<CandidateHeaderRow> {
        let dirty_revision = self.session_dirty_revision(session_id).await?;
        if let Some(existing) = self
            .mcp_open_candidate_header(session_id, candidate_publication_id)
            .await?
        {
            if existing.tombstone != 0
                && existing.required_heads_fingerprint == source_heads_fingerprint(required_heads)
                && existing.dirty_revision == dirty_revision
            {
                return Ok(existing);
            }
        }
        let active = self.mcp_open_session_head(session_id).await?;
        let slot = active
            .as_ref()
            .map_or(0, |head| 1_u8.saturating_sub(head.slot));
        let generation = self.next_projection_generation().await?;
        let required_heads_fingerprint = source_heads_fingerprint(required_heads);
        let statement = format!(
            "INSERT INTO {database}.mcp_open_publication_headers\n\
             (session_id, candidate_publication_id, slot, generation, source_revision, dirty_revision,\n\
              first_event_time, last_event_time, total_turns, total_events, user_messages, assistant_messages,\n\
              tool_calls, tool_results, mode, first_event_uid, last_event_uid, last_actor_role, title, source,\n\
              harness, inference_provider, session_slug, session_summary, list_title, list_session_summary,\n\
              completed, terminal_event_uid, origin_cwd, tombstone, required_source_heads,\n\
              required_heads_fingerprint, header_revision, publisher_id, operation_id)\n\
             SELECT {session_id}, {candidate_publication_id}, {slot}, {generation}, toUInt64(0),\n\
               toUInt64({dirty_revision}), toDateTime64(0, 3), toDateTime64(0, 3), toUInt32(0),\n\
               toUInt64(0), toUInt64(0), toUInt64(0), toUInt64(0), toUInt64(0), '', '', '', '', '', '',\n\
               '', '', '', '', '', '', toUInt8(0), '', '', toUInt8(1), {required_heads},\n\
               {required_heads_fingerprint}, {generation}, {publisher_id}, {operation_id}",
            database = escape_identifier(&self.cfg.database),
            session_id = escape_literal(session_id),
            candidate_publication_id = escape_literal(candidate_publication_id),
            dirty_revision = dirty_revision,
            required_heads = source_heads_sql(required_heads),
            required_heads_fingerprint = escape_literal(&required_heads_fingerprint),
            publisher_id = escape_literal(publisher_id),
            operation_id = escape_literal(operation_id),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        self.mcp_open_candidate_header(session_id, candidate_publication_id)
            .await?
            .context("MCP tombstone header was not durable after insert")
    }

    async fn insert_mcp_open_generation_readiness(
        &self,
        request: &McpOpenPublicationRequest,
        readiness: &McpOpenGenerationReadiness,
        ready: bool,
        block_reason: &str,
    ) -> Result<()> {
        let readiness_revision = self.next_projection_generation().await?;
        let statement = format!(
            "INSERT INTO {database}.mcp_open_generation_readiness\n\
             (candidate_publication_id, source_host, source_name, source_file, source_generation,\n\
              readiness_revision, operation_id, affected_session_count, prepared_session_count,\n\
              tombstone_count, ready, block_reason)\n\
             VALUES ({candidate_id}, {source_host}, {source_name}, {source_file}, {source_generation},\n\
              {readiness_revision}, {operation_id}, {affected}, {prepared}, {tombstones},\n\
              {ready}, {block_reason})",
            database = escape_identifier(&self.cfg.database),
            candidate_id = escape_literal(&request.candidate_publication_id),
            source_host = escape_literal(&request.source_host),
            source_name = escape_literal(&request.source_name),
            source_file = escape_literal(&request.source_file),
            source_generation = request.source_generation,
            operation_id = escape_literal(&request.operation_id),
            affected = readiness.affected_session_count,
            prepared = readiness.prepared_session_count,
            tombstones = readiness.tombstone_count,
            ready = u8::from(ready),
            block_reason = escape_literal(block_reason),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn publish_legacy_candidate_heads(&self, candidate_publication_id: &str) -> Result<()> {
        let statement =
            publish_legacy_candidate_heads_sql(&self.cfg.database, candidate_publication_id, None);
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn publish_legacy_session_candidate_head(
        &self,
        session_id: &str,
        candidate_publication_id: &str,
    ) -> Result<()> {
        let statement = publish_legacy_candidate_heads_sql(
            &self.cfg.database,
            candidate_publication_id,
            Some(session_id),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn insert_projected_events(
        &self,
        session_id: &str,
        slot: u8,
        generation: u64,
        source_revision: u64,
        required_heads: &[McpOpenSourceHead],
    ) -> Result<()> {
        let database = escape_identifier(&self.cfg.database);
        let ctes = projection_ctes(
            &database,
            session_id,
            source_revision,
            required_heads,
            false,
        );
        let statement = format!(
            "INSERT INTO {database}.mcp_open_events\n\
             (event_uid, source_host, slot, candidate_generation, generation, session_id, event_order, turn_seq,\n\
              event_time, actor_role, event_class, payload_type, event_type, event_ordinal,\n\
              call_id, name, phase, item_id, source_ref, text_content, payload_json,\n\
              token_usage_json, endpoint_kind, token_usage_buckets, token_usage_native_units,\n\
              previous_event_uid, next_event_uid)\n\
             {ctes}\n\
             SELECT\n\
               event_uid, source_host, {slot}, {generation}, {generation}, session_id, event_order, turn_seq,\n\
               event_time, actor_role, event_class, payload_type, event_type, event_ordinal,\n\
               call_id, name, phase, item_id, source_ref, text_content, payload_json,\n\
               token_usage_json, endpoint_kind, token_usage_buckets, token_usage_native_units,\n\
               previous_event_uid, next_event_uid\n\
             FROM enriched",
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn insert_projected_turns(
        &self,
        session_id: &str,
        slot: u8,
        generation: u64,
        source_revision: u64,
        required_heads: &[McpOpenSourceHead],
    ) -> Result<()> {
        let database = escape_identifier(&self.cfg.database);
        let ctes = projection_ctes(
            &database,
            session_id,
            source_revision,
            required_heads,
            false,
        );
        let message = "(event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))";
        let user_message = format!("(lowerUTF8(actor_role) = 'user' AND {message})");
        let assistant_message = format!("(lowerUTF8(actor_role) = 'assistant' AND {message})");
        let final_response_message =
            format!("({assistant_message} AND lowerUTF8(phase) != 'commentary')");
        let statement = format!(
            "INSERT INTO {database}.mcp_open_turns\n\
             (session_id, slot, candidate_generation, generation, turn_seq, turn_id, started_at, ended_at,\n\
              total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,\n\
              user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,\n\
              user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,\n\
              final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,\n\
              tools_called, normalized_event_types, completed, terminal_event_uid,\n\
              first_event_uid, first_event_order, first_event_time, first_event_type,\n\
              last_event_uid, last_event_order, last_event_time, last_event_type,\n\
              previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,\n\
              next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json)\n\
             {ctes},\n\
             turn_rows AS (\n\
               SELECT\n\
                 session_id, turn_seq, anyIf(turn_id, turn_id != '') AS turn_id,\n\
                 min(event_time) AS started_at, max(event_time) AS ended_at, count() AS total_events,\n\
                 countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,\n\
                 countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,\n\
                 countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,\n\
                 countIf(event_class = 'reasoning') AS reasoning_items,\n\
                 argMinIf(summary_source, tuple(event_order, event_uid), {user_message}) AS user_input_summary_source,\n\
                 argMaxIf(summary_source, tuple(event_order, event_uid), {final_response_message}) AS final_response_summary_source,\n\
                 argMinIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {user_message}) AS user_input_summary_is_payload,\n\
                 argMaxIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {final_response_message}) AS final_response_summary_is_payload,\n\
                 argMinIf(event_uid, tuple(event_order, event_uid), {user_message}) AS user_input_event_uid,\n\
                 argMinIf(event_order, tuple(event_order, event_uid), {user_message}) AS user_input_event_order,\n\
                 argMinIf(event_time, tuple(event_order, event_uid), {user_message}) AS user_input_event_time,\n\
                 argMinIf(event_type, tuple(event_order, event_uid), {user_message}) AS user_input_event_type,\n\
                 argMaxIf(event_uid, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_uid,\n\
                 argMaxIf(event_order, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_order,\n\
                 argMaxIf(event_time, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_time,\n\
                 argMaxIf(event_type, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_type,\n\
                 arrayDistinct(arrayMap(x -> x.2, arraySort(groupArrayIf(tuple(event_order, tool_label), event_type = 'tool_call' AND tool_label != '')))) AS tools_called,\n\
                 arrayDistinct(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_type))))) AS normalized_event_types,\n\
                 argMaxIf(toUInt8(payload_type = 'task_complete'), tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS completed,\n\
                 argMaxIf(event_uid, tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS terminal_event_uid,\n\
                 argMin(event_uid, tuple(event_order, event_uid)) AS first_event_uid,\n\
                 argMin(event_order, tuple(event_order, event_uid)) AS first_event_order,\n\
                 argMin(event_time, tuple(event_order, event_uid)) AS first_event_time,\n\
                 argMin(event_type, tuple(event_order, event_uid)) AS first_event_type,\n\
                 argMax(event_uid, tuple(event_order, event_uid)) AS last_event_uid,\n\
                 argMax(event_order, tuple(event_order, event_uid)) AS last_event_order,\n\
                 argMax(event_time, tuple(event_order, event_uid)) AS last_event_time,\n\
                 argMax(event_type, tuple(event_order, event_uid)) AS last_event_type,\n\
                 toJSONString(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_summary))))) AS event_summaries_json\n\
               FROM summarized\n\
               GROUP BY session_id, turn_seq\n\
             ),\n\
             turn_neighbors AS (\n\
               SELECT *,\n\
                 lagInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS previous_turn_seq,\n\
                 lagInFrame(turn_id, 1, '') OVER turn_window AS previous_turn_id,\n\
                 lagInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_started_at,\n\
                 lagInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_ended_at,\n\
                 leadInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS next_turn_seq,\n\
                 leadInFrame(turn_id, 1, '') OVER turn_window AS next_turn_id,\n\
                 leadInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_started_at,\n\
                 leadInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_ended_at\n\
               FROM turn_rows\n\
               WINDOW turn_window AS (PARTITION BY session_id ORDER BY turn_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n\
             )\n\
             SELECT\n\
               session_id, {slot}, {generation}, {generation}, turn_seq, turn_id, started_at, ended_at,\n\
               total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,\n\
               user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,\n\
               user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,\n\
               final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,\n\
               tools_called, normalized_event_types, completed, terminal_event_uid,\n\
               first_event_uid, first_event_order, first_event_time, first_event_type,\n\
               last_event_uid, last_event_order, last_event_time, last_event_type,\n\
               previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,\n\
               next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json\n\
             FROM turn_neighbors",
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        Ok(())
    }

    async fn insert_projected_publication_header(
        &self,
        insert: CandidateHeaderInsert<'_>,
    ) -> Result<Option<CandidateHeaderRow>> {
        let CandidateHeaderInsert {
            session_id,
            candidate_publication_id,
            operation_id,
            publisher_id,
            slot,
            generation,
            source_revision: expected_source_revision,
            dirty_revision,
            required_heads,
        } = insert;
        let database = escape_identifier(&self.cfg.database);
        let ctes = projection_ctes(
            &database,
            session_id,
            expected_source_revision,
            required_heads,
            true,
        );
        let mcp_name_predicate = crate::mcp_tool_names::sql_predicate("name");
        let required_heads_sql = source_heads_sql(required_heads);
        let required_heads_fingerprint = source_heads_fingerprint(required_heads);
        let statement = format!(
            "INSERT INTO {database}.mcp_open_publication_headers\n\
             (session_id, candidate_publication_id, slot, generation, source_revision, dirty_revision, first_event_time,\n\
              last_event_time, total_turns, total_events, user_messages, assistant_messages,\n\
              tool_calls, tool_results, mode, first_event_uid, last_event_uid, last_actor_role,\n\
              title, source, harness, inference_provider, session_slug, session_summary,\n\
              list_title, list_session_summary, completed, terminal_event_uid, origin_cwd,\n\
              tombstone, required_source_heads, required_heads_fingerprint, header_revision,\n\
              publisher_id, operation_id)\n\
             {ctes},\n\
             header AS (\n\
               SELECT\n\
                 session_id, max(projected.source_revision) AS source_revision, min(event_time) AS first_event_time,\n\
                 max(event_time) AS last_event_time, toUInt32(max(turn_seq)) AS total_turns, count() AS total_events,\n\
                 countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,\n\
                 countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,\n\
                 countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,\n\
                 multiIf(\n\
                   countIf(payload_type = 'web_search_call' OR payload_type = 'search_results_received' OR (payload_type = 'tool_use' AND name IN ('WebSearch', 'WebFetch'))) > 0, 'web_search',\n\
                   countIf(source_name = 'codex-mcp' OR {mcp_name_predicate}) > 0, 'mcp_internal',\n\
                   countIf(event_class IN ('tool_call', 'tool_result') OR payload_type = 'tool_use') > 0, 'tool_calling', 'chat') AS mode,\n\
                 argMin(event_uid, tuple(event_time, event_order, event_uid)) AS first_event_uid,\n\
                 argMax(event_uid, tuple(event_time, event_order, event_uid)) AS last_event_uid,\n\
                 argMax(actor_role, tuple(event_time, event_order, event_uid)) AS last_actor_role,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'title'), ''),\n\
                   tuple(event_ts, event_uid), event_class = 'session_meta'\n\
                     OR (source_name = 'omp' AND JSONExtractString(payload_json, 'type') IN ('title', 'title_change'))), '') AS latest_metadata_title,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'name'), ''),\n\
                   tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_metadata_name,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'summary'), ''),\n\
                   tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_metadata_summary,\n\
                 ifNull(argMaxIf(coalesce(nullIf(JSONExtractString(payload_json, 'title'), ''), nullIf(JSONExtractString(payload_json, 'name'), ''), nullIf(JSONExtractString(payload_json, 'summary'), '')),\n\
                   tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_session_meta_title,\n\
                 ifNull(argMaxIf(coalesce(nullIf(JSONExtractString(payload_json, 'summary'), ''), nullIf(JSONExtractString(payload_json, 'title'), ''), nullIf(JSONExtractString(payload_json, 'name'), '')),\n\
                   tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS latest_session_meta_summary,\n\
                 ifNull(argMinIf(nullIf(trimBoth(replaceRegexpOne(arrayElement(splitByChar('/', replaceAll(source_file, '\\\\', '/')), -1), '[.]jsonl$', '')), ''),\n\
                   tuple(event_ts, event_uid), source_name = 'omp' AND notEmpty(session_id)\n\
                     AND endsWith(source_file, '.jsonl')\n\
                     AND NOT endsWith(source_file, concat(session_id, '.jsonl'))), '') AS omp_dispatch_title,\n\
                 ifNull(argMax(nullIf(source_name, ''), tuple(event_ts, event_uid)), '') AS source,\n\
                 ifNull(argMax(nullIf(harness, ''), tuple(event_ts, event_uid)), '') AS harness,\n\
                 ifNull(argMax(nullIf(inference_provider, ''), tuple(event_ts, event_uid)), '') AS inference_provider,\n\
                 ifNull(argMaxIf(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid), event_class = 'session_meta'), '') AS session_slug,\n\
                 ifNull(argMinIf(cwd, tuple(event_ts, event_uid), cwd != ''), '') AS origin_cwd\n\
               FROM enriched AS projected\n\
               GROUP BY session_id\n\
               HAVING source_revision = {expected_source_revision}\n\
             ),\n\
             current_dirty AS (\n\
               SELECT if(count() = 0, toUInt64(0), toUInt64(max(dirty.dirty_revision))) AS dirty_revision\n\
               FROM {database}.mcp_open_dirty_sessions AS dirty FINAL\n\
               WHERE dirty.session_id = {session_sql}\n\
             ),\n\
             terminal AS (\n\
               SELECT\n\
                 session_id, argMax(completed, turn_seq) AS completed, argMax(terminal_event_uid, turn_seq) AS terminal_event_uid\n\
               FROM {database}.mcp_open_turns FINAL\n\
               WHERE session_id = {session_sql} AND slot = {slot} AND generation = {generation} AND turn_seq > 0\n\
               GROUP BY session_id\n\
             )\n\
             SELECT\n\
               h.session_id, {candidate_publication_id}, {slot}, {generation}, h.source_revision, {dirty_revision},\n\
               h.first_event_time, h.last_event_time, h.total_turns, h.total_events,\n\
               h.user_messages, h.assistant_messages, h.tool_calls, h.tool_results, h.mode,\n\
               h.first_event_uid, h.last_event_uid, h.last_actor_role,\n\
               if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.latest_metadata_summary, ''), nullIf(h.omp_dispatch_title, ''), ''), coalesce(nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), '')), h.source,\n\
               h.harness, h.inference_provider, h.session_slug,\n\
               if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_summary, ''), nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.omp_dispatch_title, ''), ''), coalesce(nullIf(h.latest_metadata_summary, ''), nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), '')),\n\
               if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.latest_metadata_summary, ''), nullIf(h.omp_dispatch_title, ''), ''), h.latest_session_meta_title),\n\
               if(h.source = 'omp', coalesce(nullIf(h.latest_metadata_summary, ''), nullIf(h.latest_metadata_title, ''), nullIf(h.latest_metadata_name, ''), nullIf(h.omp_dispatch_title, ''), ''), h.latest_session_meta_summary),\n\
               ifNull(t.completed, 0), ifNull(t.terminal_event_uid, ''), h.origin_cwd,\n\
               toUInt8(0), {required_heads_sql}, {required_heads_fingerprint}, {generation},\n\
               {publisher_id}, {operation_id}\n\
             FROM header AS h\n\
             CROSS JOIN current_dirty AS d\n\
             LEFT JOIN terminal AS t ON t.session_id = h.session_id\n\
             WHERE d.dirty_revision = {dirty_revision}",
            session_sql = escape_literal(session_id),
            candidate_publication_id = escape_literal(candidate_publication_id),
            required_heads_fingerprint = escape_literal(&required_heads_fingerprint),
            publisher_id = escape_literal(publisher_id),
            operation_id = escape_literal(operation_id),
        );
        self.request_text(&statement, None, Some(&self.cfg.database), false, None)
            .await?;
        let candidate = self
            .mcp_open_candidate_header(session_id, candidate_publication_id)
            .await?;
        Ok(candidate.filter(|head| {
            head.generation == generation
                && head.slot == slot
                && head.source_revision == expected_source_revision
                && head.dirty_revision == dirty_revision
                && head.tombstone == 0
                && head.required_heads_fingerprint == required_heads_fingerprint
        }))
    }
}

fn publish_legacy_candidate_heads_sql(
    database: &str,
    candidate_publication_id: &str,
    session_id: Option<&str>,
) -> String {
    let database = escape_identifier(database);
    let session_filter = session_id.map_or_else(String::new, |session_id| {
        format!(" AND session_id = {}", escape_literal(session_id))
    });
    format!(
        "INSERT INTO {database}.mcp_open_sessions\n\
         (session_id, slot, generation, source_revision, dirty_revision, first_event_time,\n\
          last_event_time, total_turns, total_events, user_messages, assistant_messages,\n\
          tool_calls, tool_results, mode, first_event_uid, last_event_uid, last_actor_role,\n\
          title, source, harness, inference_provider, session_slug, session_summary, completed,\n\
          terminal_event_uid, origin_cwd)\n\
         SELECT session_id, slot, generation, source_revision, dirty_revision, first_event_time,\n\
          last_event_time, total_turns, total_events, user_messages, assistant_messages,\n\
          tool_calls, tool_results, mode, first_event_uid, last_event_uid, last_actor_role,\n\
          title, source, harness, inference_provider, session_slug, session_summary, completed,\n\
          terminal_event_uid, origin_cwd\n\
         FROM {database}.mcp_open_publication_headers FINAL\n\
         WHERE candidate_publication_id = {candidate_id}{session_filter} AND tombstone = 0",
        candidate_id = escape_literal(candidate_publication_id),
    )
}

fn session_source_revision_sql(
    database: &str,
    session_id: &str,
    required_heads: &[McpOpenSourceHead],
) -> String {
    format!(
        "SELECT\n\
           if(count() = 0, toUInt64(0), toUInt64(cityHash64(arraySort(groupArray(tuple(event_uid, event_version)))))) AS source_revision\n\
         FROM (\n\
           SELECT event_uid, event_version\n\
           FROM {}.events AS e FINAL\n\
           PREWHERE e.session_id = {}\n\
           WHERE {}\n\
         )\n\
         FORMAT JSONEachRow",
        escape_identifier(database),
        escape_literal(session_id),
        source_head_filter(required_heads, "e"),
    )
}

fn current_session_source_heads_sql(database: &str, session_id: &str) -> String {
    let database = escape_identifier(database);
    format!(
        "WITH current_heads AS (\n\
           SELECT\n\
             heads.source_host AS source_host,\n\
             heads.source_name AS source_name,\n\
             heads.source_file AS source_file,\n\
             tupleElement(argMax(tuple(heads.source_generation, heads.publication_revision), heads.publication_revision), 1) AS source_generation,\n\
             max(heads.publication_revision) AS publication_revision\n\
           FROM {database}.published_source_generations AS heads\n\
           GROUP BY heads.source_host, heads.source_name, heads.source_file\n\
         )\n\
         SELECT DISTINCT\n\
           h.source_host AS source_host,\n\
           h.source_name AS source_name,\n\
           h.source_file AS source_file,\n\
           toUInt32(h.source_generation) AS source_generation,\n\
           toUInt64(h.publication_revision) AS publication_revision\n\
         FROM {database}.events AS e FINAL\n\
         INNER JOIN current_heads AS h\n\
           ON h.source_host = e.source_host AND h.source_name = e.source_name\n\
          AND h.source_file = e.source_file AND h.source_generation = e.source_generation\n\
         PREWHERE e.session_id = {}\n\
         ORDER BY source_host, source_name, source_file\n\
         FORMAT JSONEachRow",
        escape_literal(session_id),
    )
}

fn session_source_heads_for_snapshot_sql(
    database: &str,
    session_id: &str,
    captured_host_revisions: &[McpOpenHostRevision],
    target: &McpOpenSourceHead,
) -> String {
    let database = escape_identifier(database);
    let host_revisions = host_revisions_sql(captured_host_revisions);
    format!(
        "WITH {host_revisions} AS captured_host_revisions,\n\
         captured_heads AS (\n\
           SELECT\n\
             history.source_host AS source_host,\n\
             history.source_name AS source_name,\n\
             history.source_file AS source_file,\n\
             tupleElement(argMax(tuple(history.source_generation, history.publication_revision), history.publication_revision), 1) AS source_generation,\n\
             max(history.publication_revision) AS publication_revision\n\
           FROM {database}.v_published_source_generation_history AS history\n\
           ARRAY JOIN captured_host_revisions AS captured\n\
           WHERE history.source_host = tupleElement(captured, 1)\n\
             AND history.publication_revision <= tupleElement(captured, 2)\n\
           GROUP BY history.source_host, history.source_name, history.source_file\n\
         ),\n\
         desired_heads AS (\n\
           SELECT * FROM captured_heads\n\
           WHERE source_host != {target_host} OR source_name != {target_name} OR source_file != {target_file}\n\
           UNION ALL\n\
           SELECT {target_host}, {target_name}, {target_file}, toUInt32({target_generation}), toUInt64({target_revision})\n\
         )\n\
         SELECT DISTINCT\n\
           e.source_host AS source_host,\n\
           e.source_name AS source_name,\n\
           e.source_file AS source_file,\n\
           toUInt32(e.source_generation) AS source_generation,\n\
           toUInt64(head.publication_revision) AS publication_revision\n\
         FROM {database}.events AS e FINAL\n\
         INNER JOIN desired_heads AS head\n\
           ON e.source_host = head.source_host AND e.source_name = head.source_name\n\
          AND e.source_file = head.source_file AND e.source_generation = head.source_generation\n\
         PREWHERE e.session_id = {session_id}\n\
         ORDER BY source_host, source_name, source_file\n\
         FORMAT JSONEachRow",
        session_id = escape_literal(session_id),
        target_host = escape_literal(&target.source_host),
        target_name = escape_literal(&target.source_name),
        target_file = escape_literal(&target.source_file),
        target_generation = target.source_generation,
        target_revision = target.publication_revision,
    )
}

fn backfill_session_ids_sql(session_ids: &BTreeSet<String>) -> String {
    format!(
        "[{}]",
        session_ids
            .iter()
            .map(|session_id| escape_literal(session_id))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn backfill_facts_sql(database: &str, session_ids: &BTreeSet<String>) -> String {
    let database = escape_identifier(database);
    let session_ids = backfill_session_ids_sql(session_ids);
    format!(
        "WITH current_heads AS (\n\
           SELECT source_host, source_name, source_file, source_generation, publication_revision\n\
           FROM {database}.v_current_published_source_generations\n\
         ),\n\
         requested_events AS (\n\
           SELECT event_uid, event_version, session_id, source_host, source_name, source_file, source_generation\n\
           FROM {database}.events FINAL\n\
           PREWHERE session_id IN {session_ids}\n\
         ),\n\
         active_sessions AS (\n\
           SELECT session_id, slot, toUInt8(1) AS present\n\
           FROM {database}.mcp_open_sessions FINAL\n\
           WHERE session_id IN {session_ids}\n\
         ),\n\
         dirty_sessions AS (\n\
           SELECT session_id, dirty_revision, toUInt8(1) AS present\n\
           FROM {database}.mcp_open_dirty_sessions FINAL\n\
           WHERE session_id IN {session_ids}\n\
         )\n\
         SELECT\n\
           e.session_id AS session_id,\n\
           if(s.present = 1, toUInt8(1 - s.slot), toUInt8(0)) AS slot,\n\
           toUInt64(cityHash64(arraySort(groupArray(tuple(e.event_uid, e.event_version))))) AS source_revision,\n\
           if(d.present = 1, toUInt64(d.dirty_revision), toUInt64(0)) AS dirty_revision,\n\
           CAST(arraySort(groupUniqArray(tuple(\n\
             h.source_host, h.source_name, h.source_file, toUInt32(h.source_generation),\n\
             toUInt64(h.publication_revision)\n\
           ))), 'Array(Tuple(source_host String, source_name String, source_file String, source_generation UInt32, publication_revision UInt64))') AS required_source_heads\n\
         FROM requested_events AS e\n\
         INNER JOIN current_heads AS h\n\
           ON h.source_host = e.source_host AND h.source_name = e.source_name\n\
          AND h.source_file = e.source_file AND h.source_generation = e.source_generation\n\
         LEFT JOIN active_sessions AS s ON s.session_id = e.session_id\n\
         LEFT JOIN dirty_sessions AS d ON d.session_id = e.session_id\n\
         GROUP BY e.session_id, s.present, s.slot, d.present, d.dirty_revision\n\
         ORDER BY e.session_id\n\
         FORMAT JSONEachRow"
    )
}

fn backfill_plans_sql(database: &str, session_ids: &BTreeSet<String>) -> String {
    backfill_phase_plans_sql(database, session_ids, None)
}

fn backfill_phase_plans_sql(
    database: &str,
    session_ids: &BTreeSet<String>,
    phase: Option<BackfillPhase>,
) -> String {
    let phase_filter = phase
        .map(|phase| format!(" AND phase = {}", phase.value()))
        .unwrap_or_default();
    format!(
        "SELECT session_id, candidate_publication_id, slot, candidate_generation, source_revision,\n\
          dirty_revision, required_source_heads, required_heads_fingerprint, phase\n\
         FROM {}.mcp_open_backfill_plans FINAL\n\
         WHERE session_id IN {}{}\n\
         ORDER BY session_id\n\
         FORMAT JSONEachRow",
        escape_identifier(database),
        backfill_session_ids_sql(session_ids),
        phase_filter,
    )
}

fn batch_projection_ctes(
    database: &str,
    session_ids: &BTreeSet<String>,
    phase: BackfillPhase,
) -> String {
    let database = escape_identifier(database);
    let session_ids = backfill_session_ids_sql(session_ids);
    let phase = phase.value();
    let pipeline = projection_pipeline_ctes("toUInt64(planned_source_revision)", "");
    format!(
        "WITH\n\
         plans AS (\n\
           SELECT session_id, candidate_publication_id, slot, candidate_generation, source_revision,\n\
             dirty_revision, required_source_heads, required_heads_fingerprint\n\
           FROM {database}.mcp_open_backfill_plans FINAL\n\
           WHERE session_id IN {session_ids} AND phase = {phase}\n\
         ),\n\
         canonical AS (\n\
           SELECT\n\
             e.ingested_at, e.event_uid, e.session_id, e.source_host, e.source_name, e.harness,\n\
             e.inference_provider, e.source_file, e.source_generation, e.source_line_no,\n\
             e.source_offset, e.source_ref, e.record_ts, e.event_ts,\n\
             e.event_kind AS event_class, e.actor_kind AS actor_role, e.payload_type,\n\
             e.turn_index, toString(e.turn_index) AS turn_id, e.item_id,\n\
             e.tool_call_id AS call_id, e.tool_name AS name,\n\
             if(e.tool_phase != '', e.tool_phase, e.op_status) AS phase,\n\
             e.text_content, e.payload_json, e.token_usage_json, e.endpoint_kind,\n\
             e.token_usage_buckets, e.token_usage_native_units, e.cwd, e.event_version,\n\
             p.candidate_publication_id, p.slot AS candidate_slot,\n\
             p.candidate_generation, p.source_revision AS planned_source_revision,\n\
             p.dirty_revision, p.required_source_heads, p.required_heads_fingerprint\n\
           FROM {database}.events AS e FINAL\n\
           INNER JOIN plans AS p ON p.session_id = e.session_id\n\
           PREWHERE e.session_id IN {session_ids}\n\
           WHERE arrayExists(head ->\n\
             e.source_host = tupleElement(head, 1)\n\
             AND e.source_name = tupleElement(head, 2)\n\
             AND e.source_file = tupleElement(head, 3)\n\
             AND e.source_generation = tupleElement(head, 4),\n\
             p.required_source_heads)\n\
         ),\n\
         {pipeline}"
    )
}

fn projected_events_insert_sql(database: &str, ctes: &str, slot: &str, generation: &str) -> String {
    format!(
        "INSERT INTO {database}.mcp_open_events\n\
         (event_uid, source_host, slot, candidate_generation, generation, session_id, event_order, turn_seq,\n\
          event_time, actor_role, event_class, payload_type, event_type, event_ordinal,\n\
          call_id, name, phase, item_id, source_ref, text_content, payload_json,\n\
          token_usage_json, endpoint_kind, token_usage_buckets, token_usage_native_units,\n\
          previous_event_uid, next_event_uid)\n\
         {ctes}\n\
         SELECT\n\
           event_uid, source_host, {slot}, {generation}, {generation}, session_id, event_order, turn_seq,\n\
           event_time, actor_role, event_class, payload_type, event_type, event_ordinal,\n\
           call_id, name, phase, item_id, source_ref, text_content, payload_json,\n\
           token_usage_json, endpoint_kind, token_usage_buckets, token_usage_native_units,\n\
           previous_event_uid, next_event_uid\n\
         FROM enriched AS projected
         LEFT ANTI JOIN (
           SELECT event_uid, slot, source_host, candidate_generation
           FROM {database}.mcp_open_events
           PREWHERE event_uid IN (SELECT event_uid FROM canonical)
         ) AS existing
           ON existing.event_uid = projected.event_uid
          AND existing.slot = projected.candidate_slot
          AND existing.source_host = projected.source_host
          AND existing.candidate_generation = projected.candidate_generation"
    )
}

fn projected_turns_insert_sql(
    database: &str,
    ctes: &str,
    slot: &str,
    generation: &str,
    session_ids: &BTreeSet<String>,
) -> String {
    let message = "(event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text')))";
    let user_message = format!("(lowerUTF8(actor_role) = 'user' AND {message})");
    let assistant_message = format!("(lowerUTF8(actor_role) = 'assistant' AND {message})");
    let final_response_message =
        format!("({assistant_message} AND lowerUTF8(phase) != 'commentary')");
    format!(
        "INSERT INTO {database}.mcp_open_turns\n\
         (session_id, slot, candidate_generation, generation, turn_seq, turn_id, started_at, ended_at,\n\
          total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,\n\
          user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,\n\
          user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,\n\
          final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,\n\
          tools_called, normalized_event_types, completed, terminal_event_uid,\n\
          first_event_uid, first_event_order, first_event_time, first_event_type,\n\
          last_event_uid, last_event_order, last_event_time, last_event_type,\n\
          previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,\n\
          next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json)\n\
         {ctes},\n\
         turn_rows AS (\n\
           SELECT\n\
             session_id, any(candidate_slot) AS candidate_slot, any(candidate_generation) AS candidate_generation,\n\
             turn_seq, anyIf(turn_id, turn_id != '') AS turn_id,\n\
             min(event_time) AS started_at, max(event_time) AS ended_at, count() AS total_events,\n\
             countIf(actor_role = 'user' AND event_class = 'message') AS user_messages,\n\
             countIf(actor_role = 'assistant' AND event_class = 'message') AS assistant_messages,\n\
             countIf(event_class = 'tool_call') AS tool_calls, countIf(event_class = 'tool_result') AS tool_results,\n\
             countIf(event_class = 'reasoning') AS reasoning_items,\n\
             argMinIf(summary_source, tuple(event_order, event_uid), {user_message}) AS user_input_summary_source,\n\
             argMaxIf(summary_source, tuple(event_order, event_uid), {final_response_message}) AS final_response_summary_source,\n\
             argMinIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {user_message}) AS user_input_summary_is_payload,\n\
             argMaxIf(toUInt8(summary_is_payload), tuple(event_order, event_uid), {final_response_message}) AS final_response_summary_is_payload,\n\
             argMinIf(event_uid, tuple(event_order, event_uid), {user_message}) AS user_input_event_uid,\n\
             argMinIf(event_order, tuple(event_order, event_uid), {user_message}) AS user_input_event_order,\n\
             argMinIf(event_time, tuple(event_order, event_uid), {user_message}) AS user_input_event_time,\n\
             argMinIf(event_type, tuple(event_order, event_uid), {user_message}) AS user_input_event_type,\n\
             argMaxIf(event_uid, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_uid,\n\
             argMaxIf(event_order, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_order,\n\
             argMaxIf(event_time, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_time,\n\
             argMaxIf(event_type, tuple(event_order, event_uid), {final_response_message}) AS final_response_event_type,\n\
             arrayDistinct(arrayMap(x -> x.2, arraySort(groupArrayIf(tuple(event_order, tool_label), event_type = 'tool_call' AND tool_label != '')))) AS tools_called,\n\
             arrayDistinct(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_type))))) AS normalized_event_types,\n\
             argMaxIf(toUInt8(payload_type = 'task_complete'), tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS completed,\n\
             argMaxIf(event_uid, tuple(event_order, event_uid), payload_type IN ('task_complete', 'turn_aborted')) AS terminal_event_uid,\n\
             argMin(event_uid, tuple(event_order, event_uid)) AS first_event_uid,\n\
             argMin(event_order, tuple(event_order, event_uid)) AS first_event_order,\n\
             argMin(event_time, tuple(event_order, event_uid)) AS first_event_time,\n\
             argMin(event_type, tuple(event_order, event_uid)) AS first_event_type,\n\
             argMax(event_uid, tuple(event_order, event_uid)) AS last_event_uid,\n\
             argMax(event_order, tuple(event_order, event_uid)) AS last_event_order,\n\
             argMax(event_time, tuple(event_order, event_uid)) AS last_event_time,\n\
             argMax(event_type, tuple(event_order, event_uid)) AS last_event_type,\n\
             toJSONString(arrayMap(x -> x.2, arraySort(groupArray(tuple(event_order, event_summary))))) AS event_summaries_json\n\
           FROM summarized\n\
           GROUP BY session_id, turn_seq\n\
         ),\n\
         turn_neighbors AS (\n\
           SELECT *,\n\
             lagInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS previous_turn_seq,\n\
             lagInFrame(turn_id, 1, '') OVER turn_window AS previous_turn_id,\n\
             lagInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_started_at,\n\
             lagInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS previous_turn_ended_at,\n\
             leadInFrame(turn_seq, 1, toUInt32(0)) OVER turn_window AS next_turn_seq,\n\
             leadInFrame(turn_id, 1, '') OVER turn_window AS next_turn_id,\n\
             leadInFrame(started_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_started_at,\n\
             leadInFrame(ended_at, 1, toDateTime64(0, 3)) OVER turn_window AS next_turn_ended_at\n\
           FROM turn_rows\n\
           WINDOW turn_window AS (PARTITION BY session_id ORDER BY turn_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n\
         )\n\
         SELECT\n\
           session_id, {slot}, {generation}, {generation}, turn_seq, turn_id, started_at, ended_at,\n\
           total_events, user_messages, assistant_messages, tool_calls, tool_results, reasoning_items,\n\
           user_input_summary_source, final_response_summary_source, user_input_summary_is_payload, final_response_summary_is_payload,\n\
           user_input_event_uid, user_input_event_order, user_input_event_time, user_input_event_type,\n\
           final_response_event_uid, final_response_event_order, final_response_event_time, final_response_event_type,\n\
           tools_called, normalized_event_types, completed, terminal_event_uid,\n\
           first_event_uid, first_event_order, first_event_time, first_event_type,\n\
           last_event_uid, last_event_order, last_event_time, last_event_type,\n\
           previous_turn_seq, previous_turn_id, previous_turn_started_at, previous_turn_ended_at,\n\
           next_turn_seq, next_turn_id, next_turn_started_at, next_turn_ended_at, event_summaries_json\n\
         FROM turn_neighbors AS projected
         LEFT ANTI JOIN (
           SELECT session_id, slot, candidate_generation, turn_seq
           FROM {database}.mcp_open_turns
           PREWHERE session_id IN {}
         ) AS existing
           ON existing.session_id = projected.session_id
          AND existing.slot = projected.candidate_slot
          AND existing.candidate_generation = projected.candidate_generation
          AND existing.turn_seq = projected.turn_seq",
        backfill_session_ids_sql(session_ids),
    )
}

fn batch_projected_events_sql(database: &str, session_ids: &BTreeSet<String>) -> String {
    let ctes = batch_projection_ctes(database, session_ids, BackfillPhase::Planned);
    projected_events_insert_sql(
        &escape_identifier(database),
        &ctes,
        "candidate_slot",
        "candidate_generation",
    )
}

fn batch_projected_turns_sql(database: &str, session_ids: &BTreeSet<String>) -> String {
    let ctes = batch_projection_ctes(database, session_ids, BackfillPhase::EventsInserted);
    projected_turns_insert_sql(
        &escape_identifier(database),
        &ctes,
        "candidate_slot",
        "candidate_generation",
        session_ids,
    )
}

fn backfill_phase_advance_sql(
    database: &str,
    session_ids: &BTreeSet<String>,
    from_phase: BackfillPhase,
    to_phase: BackfillPhase,
    guard: Option<BackfillPhaseGuard>,
) -> String {
    let database = escape_identifier(database);
    let session_ids = backfill_session_ids_sql(session_ids);
    let from_phase = from_phase.value();
    let to_phase = to_phase.value();
    let guard_join = match guard {
        None => String::new(),
        Some(BackfillPhaseGuard::LegacyHead) => format!(
            "INNER JOIN {database}.mcp_open_publication_headers AS h FINAL\n\
               ON h.session_id = p.session_id\n\
              AND h.candidate_publication_id = p.candidate_publication_id\n\
              AND h.slot = p.slot AND h.generation = p.candidate_generation\n\
              AND h.source_revision = p.source_revision AND h.dirty_revision = p.dirty_revision\n\
             INNER JOIN {database}.mcp_open_sessions AS s FINAL\n\
               ON s.session_id = p.session_id AND s.slot = p.slot\n\
              AND s.generation = p.candidate_generation\n\
              AND s.source_revision = p.source_revision AND s.dirty_revision = p.dirty_revision\n\
             "
        ),
    };
    format!(
        "INSERT INTO {database}.mcp_open_backfill_plans\n\
         (session_id, candidate_publication_id, slot, candidate_generation, source_revision,\n\
          dirty_revision, required_source_heads, required_heads_fingerprint, phase, plan_revision)\n\
         SELECT p.session_id, p.candidate_publication_id, p.slot, p.candidate_generation,\n\
           p.source_revision, p.dirty_revision, p.required_source_heads,\n\
           p.required_heads_fingerprint, toUInt8({to_phase}), generateSnowflakeID()\n\
         FROM {database}.mcp_open_backfill_plans AS p FINAL\n\
         {guard_join}\
         WHERE p.session_id IN {session_ids} AND p.phase = {from_phase}"
    )
}

fn backfill_complete_sql(database: &str, session_ids: &BTreeSet<String>) -> String {
    format!(
        "SELECT toUInt64(countIf(phase >= {})) AS complete\n\
         FROM {}.mcp_open_backfill_plans FINAL\n\
         WHERE session_id IN {}\n\
         FORMAT JSONEachRow",
        BackfillPhase::Complete.value(),
        escape_identifier(database),
        backfill_session_ids_sql(session_ids),
    )
}

fn projection_pipeline_ctes(projected_source_revision: &str, source_revision_gate: &str) -> String {
    let event_type = event_type_sql();
    format!(
        "ordered AS (\n\
           SELECT canonical.*,\n\
             ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS event_time,\n\
             row_number() OVER canonical_window AS event_order,\n\
             if(toUInt32(turn_index) > 0, toUInt32(turn_index), greatest(toUInt32(1), toUInt32(sum(if(actor_role = 'user' AND event_class = 'message', 1, 0)) OVER canonical_rows))) AS turn_seq,\n\
             {projected_source_revision} AS source_revision\n\
           FROM canonical\n\
           {source_revision_gate}\n\
           WINDOW\n\
             canonical_window AS (PARTITION BY session_id ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_host, source_file, source_generation, source_offset, source_line_no, event_uid),\n\
             canonical_rows AS (PARTITION BY session_id ORDER BY ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at), source_host, source_file, source_generation, source_offset, source_line_no, event_uid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n\
         ),\n\
         typed AS (\n\
           SELECT *, {event_type} AS event_type,\n\
             empty(trimBoth(text_content)) AS summary_is_payload,\n\
             if(summary_is_payload,\n\
               leftUTF8(payload_json, {payload_summary_chars}),\n\
               leftUTF8(text_content, {text_summary_chars})) AS summary_source,\n\
             if(notEmpty(trimBoth(name)), trimBoth(name), trimBoth(call_id)) AS tool_label\n\
           FROM ordered\n\
         ),\n\
         enriched AS (\n\
           SELECT *,\n\
             toUInt32(row_number() OVER (PARTITION BY session_id, turn_seq ORDER BY event_order, event_uid)) AS event_ordinal,\n\
             lagInFrame(event_uid, 1, '') OVER event_window AS previous_event_uid,\n\
             leadInFrame(event_uid, 1, '') OVER event_window AS next_event_uid\n\
           FROM typed\n\
           WINDOW event_window AS (PARTITION BY session_id ORDER BY event_order, event_uid ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n\
         ),\n\
         summarized AS (\n\
           SELECT *, CAST(tuple(\n\
             event_uid, event_order, toString(event_time),\n\
             toInt64(toUnixTimestamp64Milli(event_time)), actor_role, event_class, payload_type,\n\
             event_type, call_id, name, phase, summary_source, toUInt8(summary_is_payload)\n\
           ), 'Tuple(event_uid String, event_order UInt64, event_time String, event_unix_ms Int64, actor_role String, event_class String, payload_type String, event_type String, call_id String, name String, phase String, summary_source String, summary_is_payload UInt8)') AS event_summary\n\
           FROM enriched\n\
         )",
        payload_summary_chars = MAX_PROJECTED_PAYLOAD_SUMMARY_CHARS,
        text_summary_chars = MAX_PROJECTED_TEXT_SUMMARY_CHARS,
    )
}

fn projection_ctes(
    database: &str,
    session_id: &str,
    source_revision: u64,
    required_heads: &[McpOpenSourceHead],
    revalidate_source_revision: bool,
) -> String {
    let session_id = escape_literal(session_id);
    let (canonical_revision, projected_source_revision, source_revision_gate) =
        if revalidate_source_revision {
            (
                "canonical_revision AS (\n\
                   SELECT if(count() = 0, toUInt64(0), toUInt64(cityHash64(arraySort(groupArray(tuple(event_uid, event_version)))))) AS source_revision\n\
                   FROM canonical\n\
                 ),\n\
                 ",
                "revision.source_revision".to_string(),
                format!(
                    "CROSS JOIN canonical_revision AS revision\n\
                     WHERE revision.source_revision = {source_revision}\n\
                     "
                ),
            )
        } else {
            ("", format!("toUInt64({source_revision})"), String::new())
        };
    let pipeline = projection_pipeline_ctes(&projected_source_revision, &source_revision_gate);
    format!(
        "WITH\n\
         canonical AS (\n\
           SELECT\n\
             ingested_at, event_uid, session_id, source_host, source_name, harness, inference_provider, source_file,\n\
             source_generation, source_line_no, source_offset, source_ref, record_ts, event_ts,\n\
             event_kind AS event_class, actor_kind AS actor_role, payload_type, turn_index, toString(turn_index) AS turn_id, item_id,\n\
             tool_call_id AS call_id, tool_name AS name, if(tool_phase != '', tool_phase, op_status) AS phase,\n\
             text_content, payload_json, token_usage_json, endpoint_kind, token_usage_buckets,\n\
             token_usage_native_units, cwd, event_version\n\
           FROM {database}.events AS e FINAL\n\
           PREWHERE e.session_id = {session_id}\n\
           WHERE {source_head_filter}\n\
         ),\n\
         {canonical_revision}\
         {pipeline}",
        source_head_filter = source_head_filter(required_heads, "e"),
    )
}

fn event_type_sql() -> &'static str {
    "multiIf(\
      lowerUTF8(actor_role) = 'user' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg'))), 'user_input',\
      lowerUTF8(actor_role) = 'assistant' AND (event_class = 'message' OR (event_class = 'event_msg' AND payload_type IN ('user_message', 'agent_message', 'message', 'text', 'event_msg'))), 'assistant_response',\
      lowerUTF8(actor_role) != 'system' AND (event_class = 'reasoning' OR payload_type IN ('agent_reasoning', 'reasoning', 'thinking')), 'reasoning',\
      lowerUTF8(actor_role) != 'system' AND (event_class = 'tool_call' OR payload_type IN ('tool_use', 'function_call', 'custom_tool_call', 'web_search_call')), 'tool_call',\
      lowerUTF8(actor_role) != 'system' AND (event_class = 'tool_result' OR payload_type IN ('tool_result', 'function_call_output', 'custom_tool_call_output', 'search_results_received')), 'tool_response',\
      event_class IN ('compacted_raw', 'summary') OR payload_type IN ('compacted', 'summary'), 'compaction',\
      event_class = 'queue_operation' OR payload_type IN ('task_started', 'task_complete', 'turn_aborted', 'item_completed', 'queue-operation'), 'runtime',\
      lowerUTF8(actor_role) = 'system' OR event_class IN ('system', 'progress', 'file_history_snapshot') OR payload_type IN ('system', 'progress', 'file-history-snapshot', 'file_history_snapshot'), 'system',\
      'unknown')"
}

#[cfg(test)]
mod tests {
    use super::{
        backfill_facts_sql, backfill_phase_advance_sql, batch_projected_events_sql,
        batch_projected_turns_sql, canonical_source_heads, current_session_source_heads_sql,
        projection_ctes, projection_session_ids, publish_legacy_candidate_heads_sql,
        session_source_heads_for_snapshot_sql, session_source_revision_sql, source_head_filter,
        source_heads_fingerprint, BackfillPhase, BackfillPhaseGuard, McpOpenHostRevision,
        McpOpenSourceHead, SourceHeadRow,
    };
    use std::collections::BTreeSet;

    #[test]
    fn projection_session_ids_drop_only_the_empty_storage_sentinel() {
        let ids = projection_session_ids(["", " ", "\u{a0}", "session"]);

        assert_eq!(
            ids,
            BTreeSet::from([" ".to_string(), "\u{a0}".to_string(), "session".to_string(),])
        );
    }

    fn head(host: &str, file: &str, generation: u32, revision: u64) -> McpOpenSourceHead {
        McpOpenSourceHead {
            source_host: host.to_string(),
            source_name: "codex".to_string(),
            source_file: file.to_string(),
            source_generation: generation,
            publication_revision: revision,
        }
    }

    #[test]
    fn cross_source_heads_are_canonical_and_fingerprint_every_host() {
        let host_b = head("host-b", "/sessions/b.jsonl", 7, 11);
        let host_a = head("host-a", "/sessions/a.jsonl", 3, 10);
        let canonical = canonical_source_heads(&[host_b.clone(), host_a.clone(), host_a.clone()])
            .expect("matching duplicates canonicalize");

        assert_eq!(canonical, vec![host_a.clone(), host_b.clone()]);
        assert_ne!(
            source_heads_fingerprint(&canonical),
            source_heads_fingerprint(&[host_a])
        );
        let filter = source_head_filter(&canonical, "e");
        assert!(filter.contains("e.source_host = 'host-a'"));
        assert!(filter.contains("e.source_host = 'host-b'"));
        assert!(filter.contains("e.source_generation = 7"));

        let ctes = projection_ctes("moraine", "shared-session", 42, &canonical, true);
        let unsupported_final_alias = ["FINAL", "AS"].join(" ");
        assert!(ctes.contains("PREWHERE e.session_id = 'shared-session'"));
        assert!(ctes.contains("FROM moraine.events AS e FINAL"));
        assert!(!ctes.contains(&unsupported_final_alias));
        assert!(ctes.contains("e.source_host = 'host-a'"));
        assert!(ctes.contains("e.source_host = 'host-b'"));
        assert!(ctes.contains("source_host, source_file, source_generation"));
        assert!(ctes.contains("canonical_revision AS"));
        assert!(ctes.contains("WHERE revision.source_revision = 42"));

        let child_ctes = projection_ctes("moraine", "shared-session", 42, &canonical, false);
        assert!(!child_ctes.contains("canonical_revision AS"));
        assert!(!child_ctes.contains("CROSS JOIN canonical_revision"));
        assert!(child_ctes.contains("toUInt64(42) AS source_revision"));

        let projection_source = include_str!("mcp_open_projection.rs");
        assert!(projection_source.contains(
            "(event_uid, source_host, slot, candidate_generation, generation, session_id"
        ));
        assert!(projection_source
            .contains("event_uid, source_host, {slot}, {generation}, {generation}, session_id"));
    }

    #[test]
    fn source_revision_scan_prewhere_has_no_redundant_ordering() {
        let required = [head("host-a", "/sessions/a.jsonl", 3, 10)];
        let sql = session_source_revision_sql("moraine", "session-a", &required);

        assert!(sql.contains("FROM `moraine`.events AS e FINAL"));
        assert!(sql.contains("PREWHERE e.session_id = 'session-a'"));
        assert!(!sql.contains("ORDER BY event_uid"));
    }

    #[test]
    fn historical_backfill_batches_children_and_guards_completion() {
        let sessions = BTreeSet::from(["session-a".to_string(), "session-b".to_string()]);
        let facts = backfill_facts_sql("moraine", &sessions);
        assert!(facts.contains("PREWHERE session_id IN ['session-a', 'session-b']"));
        assert!(facts.contains("Array(Tuple(source_host String"));
        assert!(facts.contains("mcp_open_sessions FINAL"));

        let events = batch_projected_events_sql("moraine", &sessions);
        let turns = batch_projected_turns_sql("moraine", &sessions);
        for (sql, table, phase) in [
            (&events, "mcp_open_events", "phase = 0"),
            (&turns, "mcp_open_turns", "phase = 1"),
        ] {
            assert!(sql.starts_with(&format!("INSERT INTO `moraine`.{table}")));
            assert!(sql.contains("FROM `moraine`.mcp_open_backfill_plans FINAL"));
            assert!(sql.contains(phase));
            assert!(sql.contains("INNER JOIN plans AS p ON p.session_id = e.session_id"));
            assert!(sql.contains("PREWHERE e.session_id IN ['session-a', 'session-b']"));
            assert!(sql.contains("p.candidate_generation"));
            assert!(sql.contains("LEFT ANTI JOIN"));
            assert!(sql.contains("existing.candidate_generation = projected.candidate_generation"));
        }
        assert!(events.contains("existing.source_host = projected.source_host"));

        let completion = backfill_phase_advance_sql(
            "moraine",
            &sessions,
            BackfillPhase::TurnsInserted,
            BackfillPhase::Complete,
            Some(BackfillPhaseGuard::LegacyHead),
        );
        assert!(completion.contains("mcp_open_publication_headers AS h FINAL"));
        assert!(completion.contains("mcp_open_sessions AS s FINAL"));
        assert!(completion.contains("h.candidate_publication_id = p.candidate_publication_id"));
        assert!(completion.contains("p.phase = 2"));
    }

    #[test]
    fn production_projection_sql_never_places_final_before_an_alias() {
        let source = include_str!("mcp_open_projection.rs");
        let unsupported_final_alias = ["FINAL", "AS"].join(" ");

        assert!(!source.contains(&unsupported_final_alias));
    }

    #[test]
    fn current_head_aggregation_qualifies_raw_revision_inputs() {
        let sql = current_session_source_heads_sql("moraine", "session-a");

        assert!(sql.contains(
            "argMax(tuple(heads.source_generation, heads.publication_revision), heads.publication_revision)"
        ));
        assert!(sql.contains("max(heads.publication_revision) AS publication_revision"));
        assert!(sql.contains("FROM `moraine`.published_source_generations AS heads"));
        assert!(sql.contains("GROUP BY heads.source_host, heads.source_name, heads.source_file"));
        assert!(sql.contains("h.source_host AS source_host"));
        assert!(sql.contains("h.source_name AS source_name"));
        assert!(sql.contains("h.source_file AS source_file"));
        assert!(sql.contains("PREWHERE e.session_id = 'session-a'"));
        assert!(!sql.contains("tuple(source_generation, publication_revision)"));
        assert!(!sql.contains("max(publication_revision)"));
    }

    #[test]
    fn source_head_queries_emit_deserializable_json_field_names() {
        let desired = head("host-a", "/sessions/a.jsonl", 3, 10);
        let captured = [McpOpenHostRevision {
            source_host: "host-a".to_string(),
            publication_revision: 9,
        }];
        let sql =
            session_source_heads_for_snapshot_sql("moraine", "session-a", &captured, &desired);

        assert!(sql.contains("e.source_host AS source_host"));
        assert!(sql.contains("e.source_name AS source_name"));
        assert!(sql.contains("e.source_file AS source_file"));
        assert!(sql.contains("PREWHERE e.session_id = 'session-a'"));
        assert!(sql.contains("history.publication_revision <= tupleElement(captured, 2)"));
        assert!(sql.contains("FROM `moraine`.v_published_source_generation_history AS history"));
        assert!(sql.contains("toUInt32(3), toUInt64(10)"));

        let row: SourceHeadRow = serde_json::from_str(
            r#"{"source_host":"host-a","source_name":"codex","source_file":"/sessions/a.jsonl","source_generation":3,"publication_revision":10}"#,
        )
        .expect("aliased JSONEachRow fields deserialize into SourceHeadRow");
        assert_eq!(row.source_host, "host-a");
        assert_eq!(row.source_generation, 3);
        assert_eq!(row.publication_revision, 10);
    }

    #[test]
    fn ordinary_legacy_head_publish_includes_session_predicate() {
        let ordinary =
            publish_legacy_candidate_heads_sql("moraine", "append:session-a:42", Some("session-a"));
        assert!(ordinary.contains("candidate_publication_id = 'append:session-a:42'"));
        assert!(ordinary.contains("session_id = 'session-a'"));

        let replacement = publish_legacy_candidate_heads_sql("moraine", "replacement:42", None);
        assert!(replacement.contains("candidate_publication_id = 'replacement:42'"));
        assert!(!replacement.contains("session_id ="));
    }

    #[test]
    fn conflicting_generation_for_one_source_key_is_rejected() {
        let error = canonical_source_heads(&[
            head("host-a", "/sessions/a.jsonl", 1, 4),
            head("host-a", "/sessions/a.jsonl", 2, 5),
        ])
        .expect_err("one source key cannot require two heads");

        assert!(error.to_string().contains("conflicting MCP source heads"));
    }
}
