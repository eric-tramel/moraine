use crate::model::{Checkpoint, CheckpointLifecycle};
use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::{ClickHouseClient, McpOpenHostRevision, McpOpenPublicationRequest};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct SourceKey {
    pub(crate) source_host: String,
    pub(crate) source_name: String,
    pub(crate) source_file: String,
}

impl SourceKey {
    pub(crate) fn local(source_name: impl Into<String>, source_file: impl Into<String>) -> Self {
        Self {
            source_host: String::new(),
            source_name: source_name.into(),
            source_file: source_file.into(),
        }
    }

    pub(crate) fn from_checkpoint(checkpoint: &Checkpoint) -> Self {
        Self::local(
            checkpoint.source_name.clone(),
            checkpoint.source_file.clone(),
        )
    }

    pub(crate) fn canonicalized(mut self, sink_host: &str) -> Self {
        if self.source_host.is_empty() && !sink_host.is_empty() {
            self.source_host = sink_host.to_string();
        }
        self
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CheckpointTransition {
    pub(crate) source: SourceKey,
    pub(crate) checkpoint: Checkpoint,
}

impl CheckpointTransition {
    pub(crate) fn try_from_checkpoint(checkpoint: Checkpoint) -> Result<Self> {
        let lifecycle = checkpoint
            .lifecycle()
            .context("checkpoint has invalid lifecycle")?;
        Ok(Self::from_valid_checkpoint(checkpoint, lifecycle))
    }

    fn from_valid_checkpoint(mut checkpoint: Checkpoint, lifecycle: CheckpointLifecycle) -> Self {
        // Callers commonly build a new cursor with `..committed.clone()`.
        // Never inherit the previous transition's causal identity: derive it
        // deterministically from the complete new state so retries converge
        // while real cursor/lifecycle changes get a fresh revision.
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        let source = SourceKey::from_checkpoint(&checkpoint);
        let mut transition = Self { source, checkpoint };
        transition.ensure_operation_id(lifecycle);
        transition
    }

    pub(crate) fn begin_replay(
        checkpoint: &Checkpoint,
        scan_inode: u64,
        scan_boundary: u64,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        let mut checkpoint = checkpoint.clone();
        checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        checkpoint.scan_inode = scan_inode;
        checkpoint.scan_boundary = scan_boundary;
        checkpoint.policy_fingerprint = policy_fingerprint.into();
        checkpoint.final_scan_complete = false;
        checkpoint.block_reason.clear();
        checkpoint.compatibility_prepared = false;
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        Self::from_valid_checkpoint(checkpoint, CheckpointLifecycle::Replaying)
    }

    pub(crate) fn finalize_replay(
        checkpoint: &Checkpoint,
        scan_inode: u64,
        scan_boundary: u64,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        let mut checkpoint = checkpoint.clone();
        checkpoint.set_lifecycle(CheckpointLifecycle::Active);
        checkpoint.scan_inode = scan_inode;
        checkpoint.scan_boundary = scan_boundary;
        checkpoint.policy_fingerprint = policy_fingerprint.into();
        checkpoint.final_scan_complete = true;
        checkpoint.block_reason.clear();
        checkpoint.compatibility_prepared = false;
        checkpoint.backend_caught_up = false;
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        Self::from_valid_checkpoint(checkpoint, CheckpointLifecycle::Active)
    }

    pub(crate) fn blocked(checkpoint: &Checkpoint, reason: impl Into<String>) -> Self {
        let mut checkpoint = checkpoint.clone();
        checkpoint.set_lifecycle(CheckpointLifecycle::Error);
        checkpoint.block_reason = reason.into();
        checkpoint.final_scan_complete = false;
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        Self::from_valid_checkpoint(checkpoint, CheckpointLifecycle::Error)
    }

    #[cfg(test)]
    pub(crate) fn with_backend_caught_up(mut self) -> Self {
        self.set_backend_caught_up(true);
        self
    }

    pub(crate) fn set_backend_caught_up(&mut self, caught_up: bool) {
        self.checkpoint.backend_caught_up = caught_up;
        self.checkpoint.checkpoint_revision = 0;
        self.checkpoint.operation_id.clear();
        let lifecycle = self
            .checkpoint
            .lifecycle()
            .expect("checkpoint transition lifecycle is validated at construction");
        self.ensure_operation_id(lifecycle);
    }

    fn set_compatibility_prepared(&mut self, prepared: bool) {
        self.checkpoint.compatibility_prepared = prepared;
        self.checkpoint.checkpoint_revision = 0;
        self.checkpoint.operation_id.clear();
        let lifecycle = self
            .checkpoint
            .lifecycle()
            .expect("checkpoint transition lifecycle is validated at construction");
        self.ensure_operation_id(lifecycle);
    }

    pub(crate) fn canonicalize_source(&mut self, sink_host: &str) {
        self.source = self.source.clone().canonicalized(sink_host);
    }

    pub(crate) fn validate_begin_replay(&self) -> Result<()> {
        if self.checkpoint.lifecycle()? != CheckpointLifecycle::Replaying {
            bail!("begin-replay transition must have replaying lifecycle");
        }
        if self.checkpoint.source_generation < 2 {
            bail!("replacement replay requires generation >= 2");
        }
        if self.checkpoint.final_scan_complete {
            bail!("begin-replay transition cannot be final");
        }
        Ok(())
    }

    pub(crate) fn validate_final(&self) -> Result<()> {
        self.validate_final_source()?;
        if !self.checkpoint.compatibility_prepared {
            bail!("compatibility projection is not prepared");
        }
        Ok(())
    }

    fn validate_final_source(&self) -> Result<()> {
        self.validate_staged_final()?;
        if !self.checkpoint.backend_caught_up {
            bail!("backend catch-up barrier is not durable");
        }
        Ok(())
    }

    pub(crate) fn validate_staged_final(&self) -> Result<()> {
        if self.checkpoint.lifecycle()? != CheckpointLifecycle::Active {
            bail!("final replay transition must have active lifecycle");
        }
        if !self.checkpoint.final_scan_complete {
            bail!("final replay transition is missing source-boundary validation");
        }
        if !self.checkpoint.block_reason.is_empty() {
            bail!(
                "blocked replay cannot publish: {}",
                self.checkpoint.block_reason
            );
        }
        if self.checkpoint.scan_inode == 0
            || self.checkpoint.scan_inode != self.checkpoint.source_inode
        {
            bail!("source inode changed during replay validation");
        }
        if self.checkpoint.last_offset < self.checkpoint.scan_boundary {
            bail!("replay stopped before the captured source boundary");
        }
        Ok(())
    }

    fn ensure_operation_id(&mut self, lifecycle: CheckpointLifecycle) {
        if !self.checkpoint.operation_id.is_empty() {
            return;
        }
        let mut hasher = Sha256::new();
        for value in [
            self.source.source_host.as_str(),
            self.source.source_name.as_str(),
            self.source.source_file.as_str(),
            lifecycle.as_str(),
            self.checkpoint.policy_fingerprint.as_str(),
        ] {
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        hasher.update(self.checkpoint.source_generation.to_le_bytes());
        hasher.update(self.checkpoint.source_inode.to_le_bytes());
        hasher.update(self.checkpoint.last_offset.to_le_bytes());
        hasher.update(self.checkpoint.last_line_no.to_le_bytes());
        hasher.update(self.checkpoint.cursor_json.as_bytes());
        hasher.update(self.checkpoint.source_fingerprint.to_le_bytes());
        hasher.update(self.checkpoint.schema_fingerprint.to_le_bytes());
        hasher.update(self.checkpoint.scan_inode.to_le_bytes());
        hasher.update(self.checkpoint.scan_boundary.to_le_bytes());
        hasher.update([self.checkpoint.final_scan_complete as u8]);
        hasher.update(self.checkpoint.block_reason.as_bytes());
        hasher.update([self.checkpoint.compatibility_prepared as u8]);
        hasher.update([self.checkpoint.backend_caught_up as u8]);
        self.checkpoint.operation_id = format!("cp-{:x}", hasher.finalize());
    }

    fn to_row(
        &self,
        host: &str,
        checkpoint_revision: u64,
        lifecycle: CheckpointLifecycle,
    ) -> Value {
        json!({
            "host": host,
            "source_name": self.source.source_name,
            "source_file": self.source.source_file,
            "inode": self.checkpoint.source_inode,
            "source_generation": self.checkpoint.source_generation,
            "last_offset": self.checkpoint.last_offset,
            "last_line": self.checkpoint.last_line_no,
            "cursor_json": self.checkpoint.cursor_json,
            "source_fingerprint": self.checkpoint.source_fingerprint,
            "schema_fingerprint": self.checkpoint.schema_fingerprint,
            "checkpoint_revision": checkpoint_revision,
            "operation_id": self.checkpoint.operation_id,
            "lifecycle": lifecycle.as_str(),
            "scan_inode": self.checkpoint.scan_inode,
            "scan_boundary": self.checkpoint.scan_boundary,
            "policy_fingerprint": self.checkpoint.policy_fingerprint,
            "final_scan_complete": u8::from(self.checkpoint.final_scan_complete),
            "block_reason": self.checkpoint.block_reason,
            "compatibility_prepared": u8::from(self.checkpoint.compatibility_prepared),
            "backend_caught_up": u8::from(self.checkpoint.backend_caught_up),
            "append_batch_id": self.checkpoint.append_batch_id,
            "cache_epoch": self.checkpoint.cache_epoch,
            "updated_at": clickhouse_datetime64_now(),
        })
    }
}

pub(crate) fn checked_next_generation(generation: u32) -> Result<u32> {
    generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("source generation exhausted at {generation}"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplayBarrierAck {
    pub(crate) checkpoint_revision: u64,
    pub(crate) operation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicationAck {
    pub(crate) checkpoint_revision: u64,
    pub(crate) publication_revision: u64,
    pub(crate) already_published: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FinalizeReplayOutcome {
    Published(PublicationAck),
    StagedForMirror,
}

pub(crate) async fn send_begin_replay(
    sink: &mpsc::Sender<crate::SinkMessage>,
    transition: CheckpointTransition,
) -> Result<ReplayBarrierAck> {
    let (ack, receive) = oneshot::channel();
    sink.send(crate::SinkMessage::BeginReplay { transition, ack })
        .await
        .map_err(|_| anyhow!("sink closed before begin-replay barrier"))?;
    receive
        .await
        .map_err(|_| anyhow!("sink closed before begin-replay acknowledgement"))?
        .map_err(anyhow::Error::msg)
}

pub(crate) async fn send_finalize_replay(
    sink: &mpsc::Sender<crate::SinkMessage>,
    transition: CheckpointTransition,
) -> Result<FinalizeReplayOutcome> {
    let (ack, receive) = oneshot::channel();
    sink.send(crate::SinkMessage::FinalizeReplay { transition, ack })
        .await
        .map_err(|_| anyhow!("sink closed before replay-finalization barrier"))?;
    receive
        .await
        .map_err(|_| anyhow!("sink closed before publication acknowledgement"))?
        .map_err(anyhow::Error::msg)
}

pub(crate) async fn send_block_replay(
    sink: &mpsc::Sender<crate::SinkMessage>,
    transition: CheckpointTransition,
) -> Result<ReplayBarrierAck> {
    let (ack, receive) = oneshot::channel();
    sink.send(crate::SinkMessage::BlockReplay { transition, ack })
        .await
        .map_err(|_| anyhow!("sink closed before replay-block barrier"))?;
    receive
        .await
        .map_err(|_| anyhow!("sink closed before replay-block acknowledgement"))?
        .map_err(anyhow::Error::msg)
}

pub(crate) async fn send_mirror_caught_up(
    sink: &mpsc::Sender<crate::SinkMessage>,
    transition: CheckpointTransition,
) -> Result<ReplayBarrierAck> {
    let (ack, receive) = oneshot::channel();
    sink.send(crate::SinkMessage::MirrorCaughtUp { transition, ack })
        .await
        .map_err(|_| anyhow!("sink closed before mirror catch-up barrier"))?;
    receive
        .await
        .map_err(|_| anyhow!("sink closed before mirror catch-up acknowledgement"))?
        .map_err(anyhow::Error::msg)
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RevisionRow {
    #[serde(default)]
    revision: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CurrentHeadRow {
    source_generation: u32,
    publication_revision: u64,
    #[serde(default)]
    publisher_id: String,
    #[serde(default)]
    operation_id: String,
}

fn is_current_initial_head(head: &CurrentHeadRow) -> bool {
    head.source_generation == 1
}

fn is_original_initial_publication(head: &CurrentHeadRow, operation_id: &str) -> bool {
    is_current_initial_head(head) && head.operation_id == operation_id
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TransitionRevisionRow {
    checkpoint_revision: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CheckpointRevisionStateRow {
    operation_revision: u64,
    max_revision: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AppendControlRow {
    control_revision: u64,
    cache_epoch: u64,
    state: String,
    batch_id: String,
    #[serde(default)]
    publisher_id: String,
    #[serde(default)]
    manifest_json: String,
    #[serde(default)]
    insert_only: u8,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CountRow {
    #[serde(default)]
    existing_count: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ExistenceRow {
    #[serde(default)]
    existing: u8,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AppendManifest {
    pub(crate) source_keys: BTreeSet<SourceKey>,
    pub(crate) session_ids: BTreeSet<String>,
    pub(crate) project_ids: BTreeSet<String>,
    pub(crate) event_uids: BTreeSet<String>,
    pub(crate) dependency_uids: BTreeSet<String>,
    pub(crate) expected_counts: BTreeMap<String, usize>,
    pub(crate) target_cursors: BTreeMap<String, (u32, u64, u64)>,
    pub(crate) digest: String,
    /// Classification defaults to false. The writer may set it only after a
    /// preflight proves every replacement key is new.
    pub(crate) insert_only: bool,
    /// A prior process durably fenced this batch but exited before every
    /// source checkpoint reached the fence. New flushes may advance the
    /// missing checkpoints under the retained batch identity.
    #[serde(default)]
    pub(crate) recovery_pending: bool,
    /// Checkpoint revision observed for each source when crash continuation
    /// began. A later proof must advance past this durable causal boundary.
    #[serde(default)]
    pub(crate) recovery_checkpoint_revisions: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AppendCheckpointProof {
    source_host: String,
    source_name: String,
    source_file: String,
    checkpoint_revision: u64,
    source_generation: u32,
    #[serde(default)]
    published_source_generation: u32,
    last_offset: u64,
    last_line: u64,
    lifecycle: String,
    block_reason: String,
    append_batch_id: String,
    cache_epoch: u64,
}

impl AppendManifest {
    pub(crate) fn from_pending(
        host: &str,
        raw_rows: &[Value],
        event_rows: &[Value],
        link_rows: &[Value],
        tool_rows: &[Value],
        error_rows: &[Value],
        checkpoints: impl Iterator<Item = Checkpoint>,
    ) -> Self {
        let checkpoints = checkpoints.collect::<Vec<_>>();
        let mut manifest = Self::default();
        for checkpoint in &checkpoints {
            let key = SourceKey::from_checkpoint(checkpoint).canonicalized(host);
            manifest.source_keys.insert(key.clone());
            manifest.target_cursors.insert(
                source_key_manifest_id(&key),
                (
                    checkpoint.source_generation,
                    checkpoint.last_offset,
                    checkpoint.last_line_no,
                ),
            );
        }
        for row in raw_rows
            .iter()
            .chain(event_rows)
            .chain(link_rows)
            .chain(tool_rows)
            .chain(error_rows)
        {
            if let Some(source) = source_key_from_row(row, host) {
                manifest.source_keys.insert(source);
            }
        }
        for row in raw_rows
            .iter()
            .chain(event_rows)
            .chain(link_rows)
            .chain(tool_rows)
            .chain(error_rows)
        {
            collect_string(row, "session_id", &mut manifest.session_ids);
            collect_string(row, "project_id", &mut manifest.project_ids);
            collect_string(row, "event_uid", &mut manifest.event_uids);
            collect_string(row, "source_event_uid", &mut manifest.dependency_uids);
            collect_string(row, "target_event_uid", &mut manifest.dependency_uids);
        }
        manifest.expected_counts = BTreeMap::from([
            ("raw_events".to_string(), raw_rows.len()),
            ("events".to_string(), event_rows.len()),
            ("event_links".to_string(), link_rows.len()),
            ("tool_io".to_string(), tool_rows.len()),
            ("ingest_errors".to_string(), error_rows.len()),
            ("checkpoints".to_string(), checkpoints.len()),
        ]);
        manifest.digest = manifest.compute_digest();
        manifest
    }

    pub(crate) fn batch_id(&self) -> String {
        // Retried flush stages reuse the manifest retained by `PendingFlush`.
        // Include row identities so consecutive checkpoint-less chunks from
        // the same source cannot reuse an already committed fence.
        let encoded = serde_json::to_vec(&(
            &self.source_keys,
            &self.target_cursors,
            &self.session_ids,
            &self.project_ids,
            &self.event_uids,
            &self.dependency_uids,
            &self.expected_counts,
        ))
        .expect("append target contains only serializable values");
        let mut hasher = Sha256::new();
        hasher.update(encoded);
        format!("append-{:x}", hasher.finalize())
    }

    fn compute_digest(&self) -> String {
        let encoded = serde_json::to_vec(&(
            &self.source_keys,
            &self.session_ids,
            &self.project_ids,
            &self.event_uids,
            &self.dependency_uids,
            &self.expected_counts,
            &self.target_cursors,
            self.insert_only,
        ))
        .expect("append manifest contains only serializable values");
        let mut hasher = Sha256::new();
        hasher.update(encoded);
        format!("{:x}", hasher.finalize())
    }
}

fn source_key_from_row(row: &Value, default_host: &str) -> Option<SourceKey> {
    let source_name = row.get("source_name")?.as_str()?.trim();
    let source_file = row.get("source_file")?.as_str()?.trim();
    if source_name.is_empty() || source_file.is_empty() {
        return None;
    }
    let source_host = row
        .get("source_host")
        .and_then(Value::as_str)
        .filter(|host| !host.is_empty())
        .unwrap_or(default_host);
    Some(SourceKey {
        source_host: source_host.to_string(),
        source_name: source_name.to_string(),
        source_file: source_file.to_string(),
    })
}

fn collect_string(row: &Value, key: &str, values: &mut BTreeSet<String>) {
    if let Some(value) = row
        .get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        values.insert(value.to_string());
    }
}

fn source_key_manifest_id(source: &SourceKey) -> String {
    format!(
        "{}:{}{}:{}{}:{}",
        source.source_host.len(),
        source.source_host,
        source.source_name.len(),
        source.source_name,
        source.source_file.len(),
        source.source_file,
    )
}

fn source_keys_sql_tuple_list(source_keys: &BTreeSet<SourceKey>) -> String {
    source_keys
        .iter()
        .map(|source| {
            format!(
                "('{}', '{}', '{}')",
                escape_string(&source.source_host),
                escape_string(&source.source_name),
                escape_string(&source.source_file),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn append_proof_is_nonterminal(proof: &AppendCheckpointProof) -> bool {
    (proof.lifecycle == CheckpointLifecycle::Active.as_str()
        || proof.lifecycle == CheckpointLifecycle::Replaying.as_str())
        && proof.block_reason.is_empty()
}

fn append_manifest_is_complete(
    manifest: &AppendManifest,
    proofs: &BTreeMap<String, AppendCheckpointProof>,
    batch_id: &str,
    target_epoch: u64,
) -> bool {
    !manifest.source_keys.is_empty()
        && manifest.source_keys.iter().all(|source| {
            let key = source_key_manifest_id(source);
            let Some((target_generation, target_offset, target_line)) =
                manifest.target_cursors.get(&key)
            else {
                return false;
            };
            proofs.get(&key).is_some_and(|proof| {
                proof.source_generation == *target_generation
                    && proof.last_offset >= *target_offset
                    && proof.last_line >= *target_line
                    && append_proof_is_nonterminal(proof)
                    && proof.append_batch_id == batch_id
                    && proof.cache_epoch == target_epoch
            })
        })
}

fn publisher_host_id(publisher_id: &str) -> Option<&str> {
    let (process_prefix, instance_id) = publisher_id.rsplit_once(':')?;
    let (host_id, process_id) = process_prefix.rsplit_once(':')?;
    if host_id.is_empty() || instance_id.is_empty() || process_id.parse::<u32>().is_err() {
        return None;
    }
    Some(host_id)
}

fn same_publication_host(left: &str, right: &str) -> bool {
    publisher_host_id(left)
        .zip(publisher_host_id(right))
        .is_some_and(|(left, right)| left == right)
}

fn append_recovery_proof_authorized(
    manifest: &AppendManifest,
    key: &str,
    proof: &AppendCheckpointProof,
    batch_id: &str,
    target_epoch: u64,
) -> bool {
    (proof.append_batch_id == batch_id && proof.cache_epoch == target_epoch)
        || manifest
            .recovery_checkpoint_revisions
            .get(key)
            .is_some_and(|revision| proof.checkpoint_revision > *revision)
}

fn prepare_append_recovery(
    manifest: &mut AppendManifest,
    proofs: &BTreeMap<String, AppendCheckpointProof>,
) {
    if !manifest.recovery_pending {
        manifest.recovery_checkpoint_revisions = manifest
            .source_keys
            .iter()
            .map(|source| {
                let key = source_key_manifest_id(source);
                let revision = proofs
                    .get(&key)
                    .map_or(0, |proof| proof.checkpoint_revision);
                (key, revision)
            })
            .collect();
    }
    manifest.recovery_pending = true;
}

fn append_manifest_recovery_is_complete(
    manifest: &AppendManifest,
    proofs: &BTreeMap<String, AppendCheckpointProof>,
    batch_id: &str,
    target_epoch: u64,
) -> bool {
    !manifest.source_keys.is_empty()
        && manifest.source_keys.iter().all(|source| {
            let key = source_key_manifest_id(source);
            let Some((target_generation, target_offset, target_line)) =
                manifest.target_cursors.get(&key)
            else {
                return false;
            };
            proofs.get(&key).is_some_and(|proof| {
                let superseded = proof.source_generation > *target_generation
                    && proof.published_source_generation == proof.source_generation
                    && proof.lifecycle == CheckpointLifecycle::Active.as_str()
                    && proof.block_reason.is_empty();
                superseded
                    || (proof.source_generation == *target_generation
                        && proof.last_offset >= *target_offset
                        && proof.last_line >= *target_line
                        && append_proof_is_nonterminal(proof)
                        && append_recovery_proof_authorized(
                            manifest,
                            &key,
                            proof,
                            batch_id,
                            target_epoch,
                        ))
            })
        })
}

fn append_manifest_can_resume(
    manifest: &AppendManifest,
    proofs: &BTreeMap<String, AppendCheckpointProof>,
    batch_id: &str,
    target_epoch: u64,
) -> bool {
    !manifest.source_keys.is_empty()
        && manifest.source_keys.iter().all(|source| {
            let key = source_key_manifest_id(source);
            let Some((target_generation, target_offset, target_line)) =
                manifest.target_cursors.get(&key)
            else {
                return false;
            };
            let Some(proof) = proofs.get(&key) else {
                return true;
            };
            let superseded = proof.source_generation > *target_generation
                && proof.published_source_generation == proof.source_generation
                && proof.lifecycle == CheckpointLifecycle::Active.as_str()
                && proof.block_reason.is_empty();
            if superseded {
                return true;
            }
            if proof.source_generation != *target_generation || !append_proof_is_nonterminal(proof)
            {
                return false;
            }
            let reached_target =
                proof.last_offset >= *target_offset && proof.last_line >= *target_line;
            !reached_target
                || append_recovery_proof_authorized(manifest, &key, proof, batch_id, target_epoch)
        })
}

/// Process-wide ownership proof for one local publication identity. Holding
/// the file descriptor keeps the advisory lock alive until shutdown.
pub(crate) struct PublicationOwnerLock {
    file: File,
}

impl PublicationOwnerLock {
    pub(crate) fn acquire(state_dir: impl AsRef<Path>, publisher_id: &str) -> Result<Self> {
        let state_dir = state_dir.as_ref();
        std::fs::create_dir_all(state_dir).with_context(|| {
            format!(
                "failed to create ingest state directory {}",
                state_dir.display()
            )
        })?;
        let path = state_dir.join("publication-owner.lock");
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        }
        let mut file = options
            .open(&path)
            .with_context(|| format!("failed to open publication lock {}", path.display()))?;
        validate_publication_owner_lock(&file, &path)?;
        #[cfg(unix)]
        {
            // SAFETY: `file` owns a valid descriptor and `flock` has no
            // pointer arguments. The descriptor remains open in Self.
            if unsafe {
                libc::flock(
                    std::os::fd::AsRawFd::as_raw_fd(&file),
                    libc::LOCK_EX | libc::LOCK_NB,
                )
            } != 0
            {
                bail!(
                    "another Moraine ingest process owns publication identity (lock {})",
                    path.display()
                );
            }
        }
        // Revalidate the descriptor after acquiring the lock so an unsafe
        // metadata race cannot reach the truncate/write path.
        validate_publication_owner_lock(&file, &path)?;
        file.set_len(0)
            .context("failed to reset publication owner lock metadata")?;
        file.write_all(publisher_id.as_bytes())
            .context("failed to write publication owner identity")?;
        file.sync_all()
            .context("failed to durably record publication owner identity")?;
        Ok(Self { file })
    }
}

fn validate_publication_owner_lock(file: &File, path: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect publication lock {}", path.display()))?;
    if !metadata.is_file() {
        bail!("publication lock {} is not a regular file", path.display());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let mode = metadata.mode() & 0o7777;
        if mode != 0o600 {
            bail!(
                "publication lock {} has insecure permissions {:o}; expected 600",
                path.display(),
                mode
            );
        }
        // SAFETY: geteuid has no arguments and no failure mode.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid {
            bail!(
                "publication lock {} is owned by uid {}, not current uid {}",
                path.display(),
                metadata.uid(),
                effective_uid
            );
        }
        if metadata.nlink() != 1 {
            bail!(
                "publication lock {} has {} hard links; expected exactly 1",
                path.display(),
                metadata.nlink()
            );
        }
    }

    Ok(())
}

impl Drop for PublicationOwnerLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        // SAFETY: the descriptor remains valid until after Drop returns.
        unsafe {
            libc::flock(std::os::fd::AsRawFd::as_raw_fd(&self.file), libc::LOCK_UN);
        }
    }
}

/// Serialized owner of checkpoint revisions, append fences, source-head
/// activation, and response-loss repair for one backend connection.
pub(crate) struct PublicationActor {
    clickhouse: ClickHouseClient,
    source_host: String,
    publisher_id: String,
    gate: Mutex<()>,
    diagnostic_revision: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourcePublicationKind {
    Initial,
    Replacement,
}

impl SourcePublicationKind {
    fn validate_prepared(self, transition: &CheckpointTransition) -> Result<()> {
        match self {
            Self::Initial => Ok(()),
            Self::Replacement => transition.validate_final(),
        }
    }

    fn persists_readiness_after_direct_repair_activation(self) -> bool {
        matches!(self, Self::Replacement)
    }

    fn messages(self) -> HeadPublicationMessages {
        match self {
            Self::Initial => HeadPublicationMessages {
                repair_not_authorized:
                    "initial MCP compatibility repair did not authorize the published generation",
                insert_context: "failed to publish initial source generation",
                missing_after_insert:
                    "initial source head is not observable after acknowledged insert",
                conflict_detail: "another publisher won initial source-head verification",
                conflict_error:
                    "conflicting publisher changed the initial source head during activation",
                activate_context: "failed to activate initial MCP compatibility publication",
                reprepare_context:
                    "failed to reprepare initial MCP compatibility after concurrent head change",
                reactivate_context: "failed to reactivate initial MCP compatibility publication",
                activation_not_current:
                    "initial MCP compatibility activation did not become current",
            },
            Self::Replacement => HeadPublicationMessages {
                repair_not_authorized:
                    "MCP compatibility repair did not authorize the published generation",
                insert_context: "failed to publish source generation",
                missing_after_insert:
                    "published source head is not observable after acknowledged insert",
                conflict_detail: "another publisher won source-head verification",
                conflict_error: "conflicting publisher changed the source head during activation",
                activate_context: "failed to activate MCP compatibility publication",
                reprepare_context:
                    "failed to reprepare MCP compatibility after concurrent head change",
                reactivate_context: "failed to reactivate MCP compatibility publication",
                activation_not_current:
                    "MCP compatibility activation did not become current after source publication",
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadPublicationAction {
    Repair {
        publication_revision: u64,
        previous_source_generation: Option<u32>,
    },
    Commit {
        publication_revision: u64,
        previous_source_generation: Option<u32>,
    },
}

struct HeadPublicationMessages {
    repair_not_authorized: &'static str,
    insert_context: &'static str,
    missing_after_insert: &'static str,
    conflict_detail: &'static str,
    conflict_error: &'static str,
    activate_context: &'static str,
    reprepare_context: &'static str,
    reactivate_context: &'static str,
    activation_not_current: &'static str,
}

impl PublicationActor {
    pub(crate) fn new(
        clickhouse: ClickHouseClient,
        source_host: String,
        publisher_id: String,
    ) -> Self {
        Self {
            clickhouse,
            source_host,
            publisher_id,
            gate: Mutex::new(()),
            diagnostic_revision: AtomicU64::new(system_time_revision()),
        }
    }

    pub(crate) async fn persist_transition(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<ReplayBarrierAck> {
        let _guard = self.gate.lock().await;
        self.persist_transition_locked(transition).await
    }

    /// Persist a validated final scan on a mirror before its ordered catch-up
    /// barrier. The complete readiness row is intentionally not publishable:
    /// `backend_caught_up = 0` keeps diagnostics and repair fail-closed while
    /// making the outstanding mirror work durable and observable.
    pub(crate) async fn persist_staged_mirror_final(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<ReplayBarrierAck> {
        transition.validate_staged_final()?;
        if transition.checkpoint.backend_caught_up {
            bail!("staged mirror final is already marked backend-caught-up");
        }
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        let checkpoint = self.persist_transition_locked(transition).await?;
        self.record_readiness_locked(transition, true).await?;
        self.commit_recovered_append_for_source_locked(&transition.source)
            .await?;
        Ok(checkpoint)
    }

    /// Persist the ordered mirror catch-up gate. Final scans supersede their
    /// staged `backend_caught_up = 0` readiness immediately; compatibility
    /// publication records the final prepared row in the next actor step.
    pub(crate) async fn persist_mirror_caught_up(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<ReplayBarrierAck> {
        if !transition.checkpoint.backend_caught_up {
            bail!("mirror catch-up transition is not marked backend-caught-up");
        }
        let validated_final = transition.validate_staged_final().is_ok();
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        let checkpoint = self.persist_transition_locked(transition).await?;
        if validated_final {
            self.record_readiness_locked(transition, true).await?;
        }
        self.commit_recovered_append_for_source_locked(&transition.source)
            .await?;
        Ok(checkpoint)
    }

    /// Keep a post-catch-up publication failure visible without rolling back
    /// durable backend readiness or poisoning the active checkpoint used by a
    /// later repair retry.
    pub(crate) async fn record_publication_repair_failure(
        &self,
        transition: &CheckpointTransition,
    ) -> Result<()> {
        let mut diagnostic = transition.clone();
        diagnostic.canonicalize_source(&self.source_host);
        diagnostic.checkpoint.block_reason = "publication_repair_failed".to_string();
        let _guard = self.gate.lock().await;
        self.record_readiness_locked(&diagnostic, true).await
    }

    pub(crate) async fn diagnose_shared_legacy_ambiguity(&self) -> Result<bool> {
        if self.source_host.is_empty() {
            return Ok(false);
        }
        let query = format!(
            "SELECT toUInt64(count()) AS existing_count FROM {}.events FINAL WHERE source_host = ''",
            quote_ident(&self.clickhouse.config().database),
        );
        let rows: Vec<CountRow> = self.clickhouse.query_rows(&query, None).await?;
        let ambiguous = rows.first().is_some_and(|row| row.existing_count > 0);
        self.record_diagnostic(
            &SourceKey {
                source_host: self.source_host.clone(),
                ..SourceKey::default()
            },
            "legacy_host_ambiguity",
            ambiguous,
            if ambiguous {
                "shared backend contains hostless legacy canonical rows"
            } else {
                ""
            },
        )
        .await?;
        Ok(ambiguous)
    }

    async fn append_checkpoint_proofs(
        &self,
        manifest: &AppendManifest,
    ) -> Result<BTreeMap<String, AppendCheckpointProof>> {
        if manifest.source_keys.is_empty() {
            return Ok(BTreeMap::new());
        }
        let query = format!(
            "SELECT checkpoint.host AS source_host, \
                    checkpoint.source_name AS source_name, \
                    checkpoint.source_file AS source_file, \
                    toUInt64(checkpoint.checkpoint_revision) AS checkpoint_revision, \
                    toUInt32(checkpoint.source_generation) AS source_generation, \
                    toUInt32(ifNull(head.source_generation, 0)) AS published_source_generation, \
                    toUInt64(checkpoint.last_offset) AS last_offset, \
                    toUInt64(checkpoint.last_line) AS last_line, \
                    checkpoint.lifecycle AS lifecycle, \
                    checkpoint.block_reason AS block_reason, \
                    checkpoint.append_batch_id AS append_batch_id, \
                    toUInt64(checkpoint.cache_epoch) AS cache_epoch \
             FROM {database}.v_current_ingest_checkpoint_transitions AS checkpoint \
             LEFT JOIN {database}.v_current_published_source_generations AS head \
               ON head.source_host = checkpoint.host \
              AND head.source_name = checkpoint.source_name \
              AND head.source_file = checkpoint.source_file \
             WHERE tuple(checkpoint.host, checkpoint.source_name, checkpoint.source_file) \
                   IN ({source_keys})",
            database = quote_ident(&self.clickhouse.config().database),
            source_keys = source_keys_sql_tuple_list(&manifest.source_keys),
        );
        let rows: Vec<AppendCheckpointProof> = self.clickhouse.query_rows(&query, None).await?;
        Ok(rows
            .into_iter()
            .map(|proof| {
                let key = source_key_manifest_id(&SourceKey {
                    source_host: proof.source_host.clone(),
                    source_name: proof.source_name.clone(),
                    source_file: proof.source_file.clone(),
                });
                (key, proof)
            })
            .collect())
    }

    async fn commit_recovered_append_for_source_locked(&self, source: &SourceKey) -> Result<()> {
        let current = self.current_append_control().await?;
        if current.state != "preparing" || current.publisher_id != self.publisher_id {
            return Ok(());
        }
        let manifest = serde_json::from_str::<AppendManifest>(&current.manifest_json)
            .context("failed to decode append manifest at checkpoint publication boundary")?;
        if !manifest.recovery_pending || !manifest.source_keys.contains(source) {
            return Ok(());
        }
        let advanced_sources = BTreeSet::from([source.clone()]);
        self.commit_append_locked(&current.batch_id, Some(&advanced_sources))
            .await?;
        Ok(())
    }

    async fn reopen_append_for_recovery(
        &self,
        expected: &AppendControlRow,
        manifest: &AppendManifest,
    ) -> Result<()> {
        let _guard = self.gate.lock().await;
        let current = self.current_append_control().await?;
        if current.batch_id != expected.batch_id
            || (current.state != "preparing" && current.state != "blocked")
        {
            bail!(
                "append control changed while recovering batch {}",
                expected.batch_id
            );
        }
        if !same_publication_host(&current.publisher_id, &self.publisher_id) {
            bail!(
                "another publication host owns append batch {}",
                current.batch_id
            );
        }
        let revision = current
            .control_revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("append control revision exhausted during recovery"))?;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_append_control",
                &[json!({
                    "host": self.source_host,
                    "control_revision": revision,
                    "cache_epoch": current.cache_epoch,
                    "state": "preparing",
                    "batch_id": current.batch_id,
                    "publisher_id": self.publisher_id,
                    "manifest_json": serde_json::to_string(manifest)?,
                    "insert_only": 0,
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to reopen interrupted append fence")?;
        for source in &manifest.source_keys {
            self.record_writer_conflict(source, false, "").await?;
        }
        Ok(())
    }

    /// Crash recovery advances an interrupted batch only through its durable
    /// source checkpoints. Recoverable work keeps the original batch and cache
    /// epoch; malformed, foreign-owned, or terminal manifests stay blocked.
    pub(crate) async fn block_orphaned_append_on_startup(&self) -> Result<bool> {
        let current = {
            let _guard = self.gate.lock().await;
            self.current_append_control().await?
        };
        if current.state != "preparing" && current.state != "blocked" {
            return Ok(false);
        }
        let mut manifest = match serde_json::from_str::<AppendManifest>(&current.manifest_json) {
            Ok(manifest) => manifest,
            Err(error) => {
                self.block_orphaned_append_raw(&current).await?;
                tracing::warn!("append manifest is invalid; startup repair blocked it: {error}");
                return Ok(true);
            }
        };
        let Some(target_epoch) = current.cache_epoch.checked_add(1) else {
            self.block_orphaned_append_raw(&current).await?;
            return Ok(true);
        };
        let proofs = self.append_checkpoint_proofs(&manifest).await?;
        if !same_publication_host(&current.publisher_id, &self.publisher_id) {
            for source in &manifest.source_keys {
                self.record_writer_conflict(
                    source,
                    true,
                    "another publication host owns the unresolved append fence",
                )
                .await?;
            }
            return Ok(true);
        }
        let complete = if manifest.recovery_pending {
            append_manifest_recovery_is_complete(
                &manifest,
                &proofs,
                &current.batch_id,
                target_epoch,
            )
        } else {
            append_manifest_is_complete(&manifest, &proofs, &current.batch_id, target_epoch)
        };
        if complete {
            if current.state == "blocked" || current.publisher_id != self.publisher_id {
                prepare_append_recovery(&mut manifest, &proofs);
                self.reopen_append_for_recovery(&current, &manifest).await?;
            }
            self.commit_append(&current.batch_id, None).await?;
            return Ok(false);
        }
        if !append_manifest_can_resume(&manifest, &proofs, &current.batch_id, target_epoch) {
            if current.state == "preparing" {
                self.block_append(&current.batch_id, &manifest).await?;
            }
            return Ok(true);
        }
        prepare_append_recovery(&mut manifest, &proofs);
        self.reopen_append_for_recovery(&current, &manifest).await?;
        tracing::info!(
            batch_id = current.batch_id,
            "reopened interrupted append fence for checkpoint continuation"
        );
        Ok(false)
    }

    async fn block_orphaned_append_raw(&self, current: &AppendControlRow) -> Result<()> {
        let _guard = self.gate.lock().await;
        let latest = self.current_append_control().await?;
        if latest.state != "preparing" || latest.batch_id != current.batch_id {
            return Ok(());
        }
        let revision = latest
            .control_revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("append control revision exhausted during startup repair"))?;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_append_control",
                &[json!({
                    "host": self.source_host,
                    "control_revision": revision,
                    "cache_epoch": latest.cache_epoch,
                    "state": "blocked",
                    "batch_id": latest.batch_id,
                    "publisher_id": self.publisher_id,
                    "manifest_json": latest.manifest_json,
                    "insert_only": latest.insert_only,
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to block orphaned append preparation")
    }

    async fn persist_transition_locked(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<ReplayBarrierAck> {
        transition.canonicalize_source(&self.source_host);
        let lifecycle = transition
            .checkpoint
            .lifecycle()
            .context("checkpoint transition has invalid lifecycle")?;
        let revision_state = self
            .checkpoint_revision_state(&transition.checkpoint.operation_id)
            .await?;
        if revision_state.operation_revision > 0 {
            let revision = revision_state.operation_revision;
            transition.checkpoint.checkpoint_revision = revision;
            return Ok(ReplayBarrierAck {
                checkpoint_revision: revision,
                operation_id: transition.checkpoint.operation_id.clone(),
            });
        }
        let revision = revision_state
            .max_revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("checkpoint revision exhausted"))?;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_checkpoint_transitions",
                &[transition.to_row(&self.source_host, revision, lifecycle)],
            )
            .await
            .context("failed to persist causal ingest checkpoint")?;
        transition.checkpoint.checkpoint_revision = revision;
        if lifecycle == CheckpointLifecycle::Error || !transition.checkpoint.block_reason.is_empty()
        {
            self.record_readiness_locked(transition, false).await?;
        }
        Ok(ReplayBarrierAck {
            checkpoint_revision: revision,
            operation_id: transition.checkpoint.operation_id.clone(),
        })
    }

    /// Commit a new source head or repair the compatibility activation for a
    /// head that was already durable when its acknowledgement was lost.
    ///
    /// Callers retain generation-specific eligibility and stale-head policy.
    /// This primitive owns the shared causal sequence while the actor gate is
    /// held: candidate preparation, checkpoint/readiness persistence, head
    /// insert and verification, activation, and one reprepare/reactivate.
    async fn commit_or_repair_head_locked(
        &self,
        transition: &mut CheckpointTransition,
        action: HeadPublicationAction,
        kind: SourcePublicationKind,
    ) -> Result<PublicationAck> {
        let messages = kind.messages();
        match action {
            HeadPublicationAction::Repair {
                publication_revision,
                previous_source_generation,
            } => {
                if transition.checkpoint.checkpoint_revision == 0 {
                    if let Some(revision) = self
                        .transition_revision(&transition.checkpoint.operation_id)
                        .await?
                    {
                        transition.checkpoint.checkpoint_revision = revision;
                    }
                }
                let candidate_id = candidate_publication_id(transition);
                let mut activated = self
                    .clickhouse
                    .activate_mcp_open_publication(&candidate_id)
                    .await?;
                if !activated {
                    self.prepare_compatibility_locked(
                        transition,
                        previous_source_generation,
                        publication_revision,
                    )
                    .await?;
                    transition.set_compatibility_prepared(true);
                    kind.validate_prepared(transition)?;
                    self.persist_transition_locked(transition).await?;
                    self.record_readiness_locked(transition, true).await?;
                    activated = self
                        .clickhouse
                        .activate_mcp_open_publication(&candidate_id)
                        .await?;
                }
                if !activated {
                    bail!(messages.repair_not_authorized);
                }
                if kind.persists_readiness_after_direct_repair_activation()
                    && !transition.checkpoint.compatibility_prepared
                {
                    // A replacement activation may repair a head committed by
                    // the prior process without entering the reprepare branch.
                    // Persist the final 1/1/1 gate so an older repair-failure
                    // diagnostic cannot remain current after success.
                    transition.set_compatibility_prepared(true);
                    kind.validate_prepared(transition)?;
                    self.persist_transition_locked(transition).await?;
                    self.record_readiness_locked(transition, true).await?;
                }
                Ok(PublicationAck {
                    checkpoint_revision: transition.checkpoint.checkpoint_revision,
                    publication_revision,
                    already_published: true,
                })
            }
            HeadPublicationAction::Commit {
                publication_revision,
                previous_source_generation,
            } => {
                let candidate_id = self
                    .prepare_compatibility_locked(
                        transition,
                        previous_source_generation,
                        publication_revision,
                    )
                    .await?;
                transition.set_compatibility_prepared(true);
                kind.validate_prepared(transition)?;
                let checkpoint = self.persist_transition_locked(transition).await?;
                self.record_readiness_locked(transition, true).await?;
                self.clickhouse
                    .insert_json_rows_sync(
                        "published_source_generations",
                        &[json!({
                            "source_host": transition.source.source_host,
                            "source_name": transition.source.source_name,
                            "source_file": transition.source.source_file,
                            "source_generation": transition.checkpoint.source_generation,
                            "publication_revision": publication_revision,
                            "publisher_id": self.publisher_id,
                            "operation_id": transition.checkpoint.operation_id,
                            "published_at": clickhouse_datetime64_now(),
                        })],
                    )
                    .await
                    .context(messages.insert_context)?;
                let written = self
                    .current_head(&transition.source)
                    .await?
                    .context(messages.missing_after_insert)?;
                if written.source_generation != transition.checkpoint.source_generation
                    || written.publication_revision != publication_revision
                    || written.operation_id != transition.checkpoint.operation_id
                    || written.publisher_id != self.publisher_id
                {
                    self.record_writer_conflict(&transition.source, true, messages.conflict_detail)
                        .await?;
                    bail!(messages.conflict_error);
                }
                self.record_writer_conflict(&transition.source, false, "")
                    .await?;
                let mut activated = self
                    .clickhouse
                    .activate_mcp_open_publication(&candidate_id)
                    .await
                    .context(messages.activate_context)?;
                if !activated {
                    self.prepare_compatibility_locked(
                        transition,
                        previous_source_generation,
                        publication_revision,
                    )
                    .await
                    .context(messages.reprepare_context)?;
                    activated = self
                        .clickhouse
                        .activate_mcp_open_publication(&candidate_id)
                        .await
                        .context(messages.reactivate_context)?;
                }
                if !activated {
                    bail!(messages.activation_not_current);
                }
                Ok(PublicationAck {
                    checkpoint_revision: checkpoint.checkpoint_revision,
                    publication_revision,
                    already_published: false,
                })
            }
        }
    }

    pub(crate) async fn publish_final(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<PublicationAck> {
        transition.validate_final_source()?;
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        let current_head = self.current_head(&transition.source).await?;
        if current_head
            .as_ref()
            .is_some_and(|head| head.source_generation > transition.checkpoint.source_generation)
        {
            bail!(
                "refusing to publish stale generation {} behind current {}",
                transition.checkpoint.source_generation,
                current_head
                    .as_ref()
                    .expect("stale current head exists")
                    .source_generation
            );
        }
        let publication = if let Some(head) = current_head
            .as_ref()
            .filter(|head| head.source_generation == transition.checkpoint.source_generation)
        {
            self.commit_or_repair_head_locked(
                transition,
                HeadPublicationAction::Repair {
                    publication_revision: head.publication_revision,
                    previous_source_generation: transition
                        .checkpoint
                        .source_generation
                        .checked_sub(1),
                },
                SourcePublicationKind::Replacement,
            )
            .await?
        } else {
            let publication_revision = self.next_publication_revision().await?;
            self.commit_or_repair_head_locked(
                transition,
                HeadPublicationAction::Commit {
                    publication_revision,
                    previous_source_generation: self
                        .current_head(&transition.source)
                        .await?
                        .map(|head| head.source_generation),
                },
                SourcePublicationKind::Replacement,
            )
            .await?
        };
        self.commit_recovered_append_for_source_locked(&transition.source)
            .await?;
        Ok(publication)
    }

    pub(crate) async fn publish_initial_if_absent(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<Option<PublicationAck>> {
        if transition.checkpoint.lifecycle()? != CheckpointLifecycle::Active
            || transition.checkpoint.source_generation != 1
            || !transition.checkpoint.block_reason.is_empty()
        {
            return Ok(None);
        }
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        if let Some(head) = self.current_head(&transition.source).await? {
            // A delayed generation-1 repair must not rebuild or activate its
            // compatibility candidate after a replacement generation has
            // already become authoritative.
            if !is_current_initial_head(&head) {
                return Ok(None);
            }
            // A generation-1 source remains published while its inode grows.
            // Later ordinary checkpoints have their own operation identity;
            // persist that causal cursor without reactivating (or rebuilding)
            // the source-wide candidate prepared by the initial publication.
            // Only the original operation can represent a crash after the
            // source head became durable but before compatibility activation.
            if !is_original_initial_publication(&head, &transition.checkpoint.operation_id) {
                let checkpoint = self.persist_transition_locked(transition).await?;
                return Ok(Some(PublicationAck {
                    checkpoint_revision: checkpoint.checkpoint_revision,
                    publication_revision: head.publication_revision,
                    already_published: true,
                }));
            }
            return self
                .commit_or_repair_head_locked(
                    transition,
                    HeadPublicationAction::Repair {
                        publication_revision: head.publication_revision,
                        previous_source_generation: None,
                    },
                    SourcePublicationKind::Initial,
                )
                .await
                .map(Some);
        }
        let publication_revision = self.next_publication_revision().await?;
        self.commit_or_repair_head_locked(
            transition,
            HeadPublicationAction::Commit {
                publication_revision,
                previous_source_generation: None,
            },
            SourcePublicationKind::Initial,
        )
        .await
        .map(Some)
    }

    pub(crate) async fn has_published_generation(
        &self,
        source: &SourceKey,
        generation: u32,
    ) -> Result<bool> {
        let _guard = self.gate.lock().await;
        Ok(self
            .current_head(source)
            .await?
            .is_some_and(|head| head.source_generation == generation))
    }

    /// Idempotent startup/catch-up repair. Only checkpoints carrying every
    /// durable readiness bit are candidates; incomplete generations remain
    /// unpublished. Existing heads are observed rather than consuming a new
    /// publication revision.
    pub(crate) async fn repair_ready_publications(
        &self,
        checkpoints: impl IntoIterator<Item = Checkpoint>,
    ) -> Result<Vec<PublicationAck>> {
        let mut repaired = Vec::new();
        for mut checkpoint in checkpoints {
            if checkpoint.lifecycle()? != CheckpointLifecycle::Active
                || !checkpoint.block_reason.is_empty()
            {
                continue;
            }
            checkpoint.backend_caught_up = true;
            checkpoint.checkpoint_revision = 0;
            checkpoint.operation_id.clear();
            let mut transition = CheckpointTransition::try_from_checkpoint(checkpoint)?;
            transition.checkpoint.backend_caught_up = true;
            if transition.checkpoint.source_generation == 1 {
                if let Some(publication) = self.publish_initial_if_absent(&mut transition).await? {
                    repaired.push(publication);
                }
            } else if transition.checkpoint.final_scan_complete {
                repaired.push(self.publish_final(&mut transition).await?);
            }
        }
        Ok(repaired)
    }

    pub(crate) async fn has_recovering_append(&self) -> Result<bool> {
        let _guard = self.gate.lock().await;
        let current = self.current_append_control().await?;
        if current.state != "preparing" || current.publisher_id != self.publisher_id {
            return Ok(false);
        }
        Ok(
            serde_json::from_str::<AppendManifest>(&current.manifest_json)
                .context("failed to decode current append manifest")?
                .recovery_pending,
        )
    }

    pub(crate) async fn begin_append(
        &self,
        manifest: &AppendManifest,
    ) -> Result<Option<(String, u64)>> {
        if manifest.source_keys.is_empty() {
            return Ok(None);
        }
        let _guard = self.gate.lock().await;
        let batch_id = manifest.batch_id();
        let current = self.current_append_control().await?;
        let current_owner = current.publisher_id == self.publisher_id;
        let same_host_restart = same_publication_host(&current.publisher_id, &self.publisher_id);
        let clear_writer_conflict = !current.publisher_id.is_empty() && !current_owner;
        if current.batch_id == batch_id
            && current.state == "blocked"
            && (current_owner || same_host_restart)
        {
            let revision = current
                .control_revision
                .checked_add(1)
                .ok_or_else(|| anyhow!("append control revision exhausted during recovery"))?;
            self.clickhouse
                .insert_json_rows_sync(
                    "ingest_append_control",
                    &[json!({
                        "host": self.source_host,
                        "control_revision": revision,
                        "cache_epoch": current.cache_epoch,
                        "state": "preparing",
                        "batch_id": batch_id,
                        "publisher_id": self.publisher_id,
                        "manifest_json": current.manifest_json,
                        "insert_only": current.insert_only,
                        "updated_at": clickhouse_datetime64_now(),
                    })],
                )
                .await
                .context("failed to reacquire blocked append fence")?;
            for source in &manifest.source_keys {
                self.record_writer_conflict(source, false, "").await?;
            }
            return Ok(Some((batch_id, current.cache_epoch)));
        }
        if (current.state == "preparing" || current.state == "blocked") && !current_owner {
            for source in &manifest.source_keys {
                self.record_writer_conflict(
                    source,
                    true,
                    "another publisher owns the unresolved append fence",
                )
                .await?;
            }
            bail!(
                "append fence is {} for another publisher's batch {}",
                current.state,
                current.batch_id
            );
        }
        if current.state == "preparing" && current_owner {
            let persisted = serde_json::from_str::<AppendManifest>(&current.manifest_json)
                .context("failed to decode current append manifest")?;
            if persisted.recovery_pending {
                for source in manifest.source_keys.union(&persisted.source_keys) {
                    self.record_writer_conflict(source, false, "").await?;
                }
                return Ok(Some((current.batch_id, current.cache_epoch)));
            }
        }
        if current.batch_id == batch_id {
            if current.state == "idle" {
                return Ok(None);
            }
            return Ok(Some((batch_id, current.cache_epoch)));
        }
        if current.state == "preparing" || current.state == "blocked" {
            bail!(
                "append fence is {} for unresolved batch {}",
                current.state,
                current.batch_id
            );
        }
        let revision = current.control_revision.checked_add(1).ok_or_else(|| {
            anyhow!(
                "append control revision exhausted at {}",
                current.control_revision
            )
        })?;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_append_control",
                &[json!({
                    "host": self.source_host,
                    "control_revision": revision,
                    "cache_epoch": current.cache_epoch,
                    "state": "preparing",
                    "batch_id": batch_id,
                    "publisher_id": self.publisher_id,
                    "manifest_json": serde_json::to_string(manifest)?,
                    "insert_only": u8::from(manifest.insert_only),
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to persist append preparation fence")?;
        let verified = self.current_append_control().await?;
        if verified.batch_id != batch_id || verified.publisher_id != self.publisher_id {
            for source in &manifest.source_keys {
                self.record_writer_conflict(
                    source,
                    true,
                    "another publisher won append-fence verification",
                )
                .await?;
            }
            bail!("conflicting publisher changed append control during preparation");
        }
        if clear_writer_conflict {
            for source in &manifest.source_keys {
                self.record_writer_conflict(source, false, "").await?;
            }
        }
        Ok(Some((batch_id, current.cache_epoch)))
    }

    /// Fail-closed preflight for the narrow moving-feed exception. A batch is
    /// insert-only only when it carries canonical events and none of its event
    /// or session replacement keys exists in the currently published model.
    /// Any query failure propagates and keeps the hard fence in force.
    pub(crate) async fn prove_insert_only(&self, manifest: &mut AppendManifest) -> Result<bool> {
        if manifest.event_uids.is_empty() || manifest.session_ids.is_empty() {
            manifest.insert_only = false;
            return Ok(false);
        }
        let event_uids = sql_string_list(&manifest.event_uids);
        let session_ids = sql_string_list(&manifest.session_ids);
        let query = format!(
            "SELECT toUInt8(1) AS existing FROM {}.v_live_events \
             WHERE event_uid IN ({}) OR session_id IN ({}) LIMIT 1",
            quote_ident(&self.clickhouse.config().database),
            event_uids,
            session_ids,
        );
        let rows: Vec<ExistenceRow> = self.clickhouse.query_rows(&query, None).await?;
        let insert_only = rows.first().is_none_or(|row| row.existing == 0);
        manifest.insert_only = insert_only;
        // The digest covers the classification as well as the affected rows.
        manifest.digest = manifest.compute_digest();
        Ok(insert_only)
    }

    pub(crate) async fn commit_append(
        &self,
        batch_id: &str,
        advanced_sources: Option<&BTreeSet<SourceKey>>,
    ) -> Result<u64> {
        let _guard = self.gate.lock().await;
        self.commit_append_locked(batch_id, advanced_sources).await
    }

    async fn commit_append_locked(
        &self,
        batch_id: &str,
        advanced_sources: Option<&BTreeSet<SourceKey>>,
    ) -> Result<u64> {
        let current = self.current_append_control().await?;
        if current.batch_id == batch_id && current.state == "idle" {
            return Ok(current.cache_epoch);
        }
        if current.batch_id != batch_id || current.state != "preparing" {
            bail!(
                "cannot commit append batch {batch_id}; current control is {} ({})",
                current.batch_id,
                current.state
            );
        }
        if current.publisher_id != self.publisher_id {
            bail!("cannot commit append batch {batch_id} owned by another publisher");
        }
        let manifest = serde_json::from_str::<AppendManifest>(&current.manifest_json)
            .context("failed to decode append manifest before commit")?;
        if manifest.recovery_pending {
            if advanced_sources.is_some_and(|sources| sources.is_disjoint(&manifest.source_keys)) {
                return Ok(current.cache_epoch);
            }
            let target_epoch = current.cache_epoch.checked_add(1).ok_or_else(|| {
                anyhow!("append cache epoch exhausted at {}", current.cache_epoch)
            })?;
            let proofs = self.append_checkpoint_proofs(&manifest).await?;
            if !append_manifest_recovery_is_complete(&manifest, &proofs, batch_id, target_epoch) {
                return Ok(current.cache_epoch);
            }
        }
        let revision = current.control_revision.checked_add(1).ok_or_else(|| {
            anyhow!(
                "append control revision exhausted at {}",
                current.control_revision
            )
        })?;
        let cache_epoch = current
            .cache_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow!("append cache epoch exhausted at {}", current.cache_epoch))?;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_append_control",
                &[json!({
                    "host": self.source_host,
                    "control_revision": revision,
                    "cache_epoch": cache_epoch,
                    "state": "idle",
                    "batch_id": batch_id,
                    "publisher_id": self.publisher_id,
                    "manifest_json": "",
                    "insert_only": 0,
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to atomically commit append cache epoch")?;
        Ok(cache_epoch)
    }

    pub(crate) async fn block_append(
        &self,
        batch_id: &str,
        manifest: &AppendManifest,
    ) -> Result<()> {
        let _guard = self.gate.lock().await;
        let current = self.current_append_control().await?;
        if current.batch_id != batch_id || current.state != "preparing" {
            return Ok(());
        }
        let manifest_json = if current.manifest_json.is_empty() {
            serde_json::to_string(manifest)?
        } else {
            current.manifest_json.clone()
        };
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_append_control",
                &[json!({
                    "host": self.source_host,
                    "control_revision": current.control_revision.checked_add(1)
                        .ok_or_else(|| anyhow!("append control revision exhausted"))?,
                    "cache_epoch": current.cache_epoch,
                    "state": "blocked",
                    "batch_id": batch_id,
                    "publisher_id": self.publisher_id,
                    "manifest_json": manifest_json,
                    "insert_only": current.insert_only,
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to block incomplete append fence")
    }

    async fn transition_revision(&self, operation_id: &str) -> Result<Option<u64>> {
        let query = format!(
            "SELECT toUInt64(max(transitions.checkpoint_revision)) AS checkpoint_revision \
             FROM {}.ingest_checkpoint_transitions AS transitions \
             WHERE transitions.host = '{}' AND transitions.operation_id = '{}'",
            quote_ident(&self.clickhouse.config().database),
            escape_string(&self.source_host),
            escape_string(operation_id),
        );
        let rows: Vec<TransitionRevisionRow> = self.clickhouse.query_rows(&query, None).await?;
        Ok(rows
            .first()
            .map(|row| row.checkpoint_revision)
            .filter(|revision| *revision > 0))
    }

    async fn checkpoint_revision_state(
        &self,
        operation_id: &str,
    ) -> Result<CheckpointRevisionStateRow> {
        let query = format!(
            "SELECT toUInt64(maxIf(transitions.checkpoint_revision, \
                                    transitions.operation_id = '{}')) AS operation_revision, \
                    toUInt64(max(transitions.checkpoint_revision)) AS max_revision \
             FROM {}.ingest_checkpoint_transitions AS transitions \
             WHERE transitions.host = '{}'",
            escape_string(operation_id),
            quote_ident(&self.clickhouse.config().database),
            escape_string(&self.source_host),
        );
        let rows: Vec<CheckpointRevisionStateRow> =
            self.clickhouse.query_rows(&query, None).await?;
        Ok(rows.into_iter().next().unwrap_or_default())
    }

    async fn next_publication_revision(&self) -> Result<u64> {
        let query = format!(
            "SELECT toUInt64(max(heads.publication_revision)) AS revision \
             FROM {}.published_source_generations AS heads \
             WHERE heads.source_host = '{}'",
            quote_ident(&self.clickhouse.config().database),
            escape_string(&self.source_host),
        );
        self.next_revision(&query, "publication").await
    }

    async fn record_readiness_locked(
        &self,
        transition: &CheckpointTransition,
        complete: bool,
    ) -> Result<()> {
        let query = format!(
            "SELECT toUInt64(max(readiness.readiness_revision)) AS revision \
             FROM {}.source_generation_publication_readiness AS readiness \
             WHERE readiness.source_host = '{}'",
            quote_ident(&self.clickhouse.config().database),
            escape_string(&self.source_host),
        );
        let readiness_revision = self.next_revision(&query, "readiness").await?;
        self.clickhouse
            .insert_json_rows_sync(
                "source_generation_publication_readiness",
                &[json!({
                    "source_host": transition.source.source_host,
                    "source_name": transition.source.source_name,
                    "source_file": transition.source.source_file,
                    "source_generation": transition.checkpoint.source_generation,
                    "readiness_revision": readiness_revision,
                    "checkpoint_revision": transition.checkpoint.checkpoint_revision,
                    "operation_id": transition.checkpoint.operation_id,
                    "complete": u8::from(complete),
                    "block_reason": transition.checkpoint.block_reason,
                    "compatibility_prepared": u8::from(transition.checkpoint.compatibility_prepared),
                    "backend_caught_up": u8::from(transition.checkpoint.backend_caught_up),
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to persist source-generation publication readiness")
    }

    async fn prepare_compatibility_locked(
        &self,
        transition: &CheckpointTransition,
        previous_source_generation: Option<u32>,
        publication_revision: u64,
    ) -> Result<String> {
        let captured_host_revisions = self.current_host_revisions().await?;
        let candidate_publication_id = candidate_publication_id(transition);
        let readiness = self
            .clickhouse
            .prepare_mcp_open_publication(&McpOpenPublicationRequest {
                candidate_publication_id: candidate_publication_id.clone(),
                operation_id: transition.checkpoint.operation_id.clone(),
                publisher_id: self.publisher_id.clone(),
                source_host: transition.source.source_host.clone(),
                source_name: transition.source.source_name.clone(),
                source_file: transition.source.source_file.clone(),
                previous_source_generation,
                source_generation: transition.checkpoint.source_generation,
                publication_revision,
                captured_host_revisions,
            })
            .await
            .context("failed to prepare MCP compatibility publication")?;
        if readiness.prepared_session_count != readiness.affected_session_count {
            bail!(
                "MCP compatibility preparation incomplete: prepared {} of {} affected sessions",
                readiness.prepared_session_count,
                readiness.affected_session_count
            );
        }
        Ok(candidate_publication_id)
    }

    async fn current_host_revisions(&self) -> Result<Vec<McpOpenHostRevision>> {
        let query = format!(
            "SELECT source_host, toUInt64(max(publication_revision)) AS publication_revision \
             FROM {}.v_current_published_source_generations \
             GROUP BY source_host ORDER BY source_host",
            quote_ident(&self.clickhouse.config().database),
        );
        self.clickhouse.query_rows(&query, None).await
    }

    async fn next_revision(&self, query: &str, kind: &str) -> Result<u64> {
        let rows: Vec<RevisionRow> = self.clickhouse.query_rows(query, None).await?;
        rows.first()
            .map(|row| row.revision)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("{kind} revision exhausted"))
    }

    async fn current_head(&self, source: &SourceKey) -> Result<Option<CurrentHeadRow>> {
        let source = source.clone().canonicalized(&self.source_host);
        let query = format!(
            "SELECT toUInt32(source_generation) AS source_generation, \
                    toUInt64(publication_revision) AS publication_revision, \
                    publisher_id, operation_id \
             FROM {}.v_current_published_source_generations \
             WHERE source_host = '{}' AND source_name = '{}' AND source_file = '{}' LIMIT 1",
            quote_ident(&self.clickhouse.config().database),
            escape_string(&source.source_host),
            escape_string(&source.source_name),
            escape_string(&source.source_file),
        );
        let mut rows: Vec<CurrentHeadRow> = self.clickhouse.query_rows(&query, None).await?;
        Ok(rows.pop())
    }

    async fn current_append_control(&self) -> Result<AppendControlRow> {
        let query = format!(
            "SELECT toUInt64(control_revision) AS control_revision, \
                    toUInt64(cache_epoch) AS cache_epoch, state, batch_id, publisher_id, \
                    manifest_json, toUInt8(insert_only) AS insert_only \
             FROM {}.v_current_ingest_append_control \
             WHERE host = '{}' LIMIT 1",
            quote_ident(&self.clickhouse.config().database),
            escape_string(&self.source_host),
        );
        let mut rows: Vec<AppendControlRow> = self.clickhouse.query_rows(&query, None).await?;
        Ok(rows.pop().unwrap_or_else(|| AppendControlRow {
            state: "idle".to_string(),
            ..Default::default()
        }))
    }

    async fn record_writer_conflict(
        &self,
        source: &SourceKey,
        active: bool,
        detail: &str,
    ) -> Result<()> {
        self.record_diagnostic(source, "writer_conflict", active, detail)
            .await
    }

    async fn record_diagnostic(
        &self,
        source: &SourceKey,
        diagnostic_kind: &str,
        active: bool,
        detail: &str,
    ) -> Result<()> {
        let revision = self.diagnostic_revision.fetch_add(1, Ordering::Relaxed);
        self.clickhouse
            .insert_json_rows_sync(
                "publication_diagnostic_events",
                &[json!({
                    "source_host": source.source_host,
                    "source_name": source.source_name,
                    "source_file": source.source_file,
                    "diagnostic_kind": diagnostic_kind,
                    "diagnostic_revision": revision,
                    "detail": detail,
                    "active": u8::from(active),
                })],
            )
            .await
            .context("failed to persist publication diagnostic")
    }
}

fn escape_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn system_time_revision() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn clickhouse_datetime64_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn quote_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn sql_string_list(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", escape_string(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn candidate_publication_id(transition: &CheckpointTransition) -> String {
    let mut hasher = Sha256::new();
    for value in [
        transition.source.source_host.as_str(),
        transition.source.source_name.as_str(),
        transition.source.source_file.as_str(),
    ] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(transition.checkpoint.source_generation.to_le_bytes());
    format!("source-publication-{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        http::StatusCode,
        routing::post,
        Router,
    };
    #[cfg(unix)]
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex as StdMutex};

    #[cfg(unix)]
    struct TestPublicationStateDir(PathBuf);

    #[cfg(unix)]
    impl TestPublicationStateDir {
        fn new(label: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "moraine-publication-lock-{label}-{}",
                uuid::Uuid::new_v4()
            )))
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn lock_path(&self) -> PathBuf {
            self.path().join("publication-owner.lock")
        }
    }

    #[cfg(unix)]
    impl Drop for TestPublicationStateDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn checkpoint(generation: u32) -> Checkpoint {
        Checkpoint {
            source_name: "codex".to_string(),
            source_file: "/tmp/session.jsonl".to_string(),
            source_inode: 42,
            source_generation: generation,
            last_offset: 100,
            last_line_no: 4,
            status: CheckpointLifecycle::Active.to_string(),
            ..Checkpoint::default()
        }
    }

    #[derive(Clone, Default)]
    struct HeadPublicationMock {
        queries: Arc<StdMutex<Vec<String>>>,
        inserts: Arc<StdMutex<BTreeMap<String, usize>>>,
        current_head: Arc<StdMutex<Option<Value>>>,
        transition_revisions: Arc<StdMutex<BTreeMap<String, u64>>>,
        mcp_generation_ready: Arc<StdMutex<bool>>,
    }

    impl HeadPublicationMock {
        fn insert_count(&self, table: &str) -> usize {
            self.inserts
                .lock()
                .expect("head mock inserts mutex poisoned")
                .get(table)
                .copied()
                .unwrap_or(0)
        }

        fn query_count(&self, needle: &str) -> usize {
            self.queries
                .lock()
                .expect("head mock queries mutex poisoned")
                .iter()
                .filter(|query| query.contains(needle))
                .count()
        }

        fn set_current_head(&self, transition: &CheckpointTransition, revision: u64) {
            *self
                .current_head
                .lock()
                .expect("head mock current_head mutex poisoned") = Some(json!({
                "source_host": "test-host",
                "source_name": transition.source.source_name,
                "source_file": transition.source.source_file,
                "source_generation": transition.checkpoint.source_generation,
                "publication_revision": revision,
                "publisher_id": "test-publisher",
                "operation_id": transition.checkpoint.operation_id,
            }));
        }

        fn seed_transition_revision(&self, operation_id: &str, revision: u64) {
            self.transition_revisions
                .lock()
                .expect("head mock transition revisions mutex poisoned")
                .insert(operation_id.to_string(), revision);
        }

        fn set_mcp_generation_ready(&self, ready: bool) {
            *self
                .mcp_generation_ready
                .lock()
                .expect("head mock MCP readiness mutex poisoned") = ready;
        }
    }

    fn head_mock_response(params: &BTreeMap<String, String>, rows: &[Value]) -> String {
        if params
            .get("default_format")
            .is_some_and(|format| format == "JSON")
        {
            json!({ "data": rows }).to_string()
        } else {
            rows.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        }
    }

    fn head_mock_insert_table(query: &str) -> Option<&'static str> {
        [
            "ingest_checkpoint_transitions",
            "source_generation_publication_readiness",
            "published_source_generations",
            "publication_diagnostic_events",
            "mcp_open_generation_readiness",
        ]
        .into_iter()
        .find(|table| query.contains(table))
    }

    async fn head_publication_mock_handler(
        State(state): State<HeadPublicationMock>,
        Query(params): Query<BTreeMap<String, String>>,
        body: String,
    ) -> (StatusCode, String) {
        let query = params
            .get("query")
            .map(String::as_str)
            .unwrap_or(body.as_str());
        state
            .queries
            .lock()
            .expect("head mock queries mutex poisoned")
            .push(query.to_string());

        if query.contains("generateSnowflakeID()") {
            return (
                StatusCode::OK,
                head_mock_response(&params, &[json!({ "source_revision": 41u64 })]),
            );
        }
        if query.contains("maxIf(transitions.checkpoint_revision") {
            let revisions = state
                .transition_revisions
                .lock()
                .expect("head mock transition revisions mutex poisoned");
            let max_revision = revisions.values().copied().max().unwrap_or(0);
            let operation_revision = revisions
                .iter()
                .find_map(|(operation_id, revision)| {
                    query
                        .contains(&format!("transitions.operation_id = '{operation_id}'"))
                        .then_some(*revision)
                })
                .unwrap_or(0);
            return (
                StatusCode::OK,
                head_mock_response(
                    &params,
                    &[json!({
                        "operation_revision": operation_revision,
                        "max_revision": max_revision,
                    })],
                ),
            );
        }
        if query.contains("max(transitions.checkpoint_revision)) AS checkpoint_revision") {
            let revisions = state
                .transition_revisions
                .lock()
                .expect("head mock transition revisions mutex poisoned");
            let checkpoint_revision = revisions
                .iter()
                .find_map(|(operation_id, revision)| {
                    query
                        .contains(&format!("transitions.operation_id = '{operation_id}'"))
                        .then_some(*revision)
                })
                .unwrap_or(0);
            return (
                StatusCode::OK,
                head_mock_response(
                    &params,
                    &[json!({ "checkpoint_revision": checkpoint_revision })],
                ),
            );
        }
        if query.contains("v_current_published_source_generations")
            && query.contains("WHERE source_host =")
        {
            let rows = state
                .current_head
                .lock()
                .expect("head mock current_head mutex poisoned")
                .clone()
                .into_iter()
                .collect::<Vec<_>>();
            return (StatusCode::OK, head_mock_response(&params, &rows));
        }
        if query.contains("v_current_published_source_generations")
            && query.contains("ORDER BY source_host")
        {
            let rows = state
                .current_head
                .lock()
                .expect("head mock current_head mutex poisoned")
                .clone()
                .into_iter()
                .collect::<Vec<_>>();
            return (StatusCode::OK, head_mock_response(&params, &rows));
        }
        if query.contains("v_current_mcp_open_generation_readiness") {
            let ready = *state
                .mcp_generation_ready
                .lock()
                .expect("head mock MCP readiness mutex poisoned");
            return (
                StatusCode::OK,
                head_mock_response(&params, &[json!({ "ready": u8::from(ready) })]),
            );
        }
        if query.contains("SELECT") && !query.contains("INSERT INTO") {
            return (StatusCode::OK, head_mock_response(&params, &[]));
        }

        let Some(table) = head_mock_insert_table(query) else {
            return (
                StatusCode::BAD_REQUEST,
                format!("unexpected head publication query: {query}"),
            );
        };
        *state
            .inserts
            .lock()
            .expect("head mock inserts mutex poisoned")
            .entry(table.to_string())
            .or_insert(0) += 1;

        if table == "mcp_open_generation_readiness" {
            state.set_mcp_generation_ready(true);
        }
        for row in body
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        {
            if table == "ingest_checkpoint_transitions" {
                if let (Some(operation_id), Some(revision)) = (
                    row.get("operation_id").and_then(Value::as_str),
                    row.get("checkpoint_revision").and_then(Value::as_u64),
                ) {
                    state.seed_transition_revision(operation_id, revision);
                }
            } else if table == "published_source_generations" {
                *state
                    .current_head
                    .lock()
                    .expect("head mock current_head mutex poisoned") = Some(row);
            }
        }
        (StatusCode::OK, String::new())
    }

    async fn spawn_head_publication_actor() -> (PublicationActor, HeadPublicationMock) {
        let state = HeadPublicationMock::default();
        let app = Router::new()
            .route("/", post(head_publication_mock_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind head publication mock");
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = moraine_config::AppConfig::default();
        config.clickhouse.url = format!("http://{address}");
        config.clickhouse.timeout_seconds = 1.0;
        let clickhouse = ClickHouseClient::new(config.clickhouse).unwrap();
        (
            PublicationActor::new(
                clickhouse,
                "test-host".to_string(),
                "test-publisher".to_string(),
            ),
            state,
        )
    }

    #[derive(Debug, Eq, PartialEq)]
    struct HeadPublicationTrace {
        checkpoint_transitions: usize,
        publication_readiness: usize,
        source_heads: usize,
        writer_diagnostics: usize,
        mcp_readiness: usize,
        mcp_candidate_reads: usize,
        mcp_activation_reads: usize,
        mcp_preparations: usize,
    }

    fn head_publication_trace(state: &HeadPublicationMock) -> HeadPublicationTrace {
        HeadPublicationTrace {
            checkpoint_transitions: state.insert_count("ingest_checkpoint_transitions"),
            publication_readiness: state.insert_count("source_generation_publication_readiness"),
            source_heads: state.insert_count("published_source_generations"),
            writer_diagnostics: state.insert_count("publication_diagnostic_events"),
            mcp_readiness: state.insert_count("mcp_open_generation_readiness"),
            mcp_candidate_reads: state.query_count("mcp_open_publication_headers FINAL"),
            mcp_activation_reads: state.query_count("v_current_mcp_open_generation_readiness"),
            mcp_preparations: state.query_count("SELECT DISTINCT session_id"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn initial_and_replacement_new_heads_share_the_commit_sequence() {
        let (initial_actor, initial_state) = spawn_head_publication_actor().await;
        let mut initial_checkpoint = checkpoint(1);
        initial_checkpoint.backend_caught_up = true;
        let mut initial = CheckpointTransition::try_from_checkpoint(initial_checkpoint).unwrap();
        let initial_ack = initial_actor
            .publish_initial_if_absent(&mut initial)
            .await
            .expect("publish initial source head")
            .expect("eligible initial generation");

        let (replacement_actor, replacement_state) = spawn_head_publication_actor().await;
        let mut replacement =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy");
        replacement.set_backend_caught_up(true);
        let replacement_ack = replacement_actor
            .publish_final(&mut replacement)
            .await
            .expect("publish replacement source head");

        assert_eq!(initial_ack.checkpoint_revision, 1);
        assert_eq!(replacement_ack.checkpoint_revision, 1);
        assert_eq!(initial_ack.publication_revision, 1);
        assert_eq!(replacement_ack.publication_revision, 1);
        assert!(!initial_ack.already_published);
        assert!(!replacement_ack.already_published);
        assert_eq!(
            head_publication_trace(&initial_state),
            head_publication_trace(&replacement_state),
            "both entry points must execute the same durable commit stages"
        );
        assert_eq!(
            replacement_state.query_count("WHERE source_host ="),
            initial_state.query_count("WHERE source_host =") + 1,
            "replacement policy preserves its second current-head read for previous-generation capture"
        );
    }

    async fn repair_after_lost_head_ack(
        generation: u32,
        kind: SourcePublicationKind,
        initially_ready: bool,
    ) -> (PublicationAck, HeadPublicationMock, CheckpointTransition) {
        let (actor, state) = spawn_head_publication_actor().await;
        let mut transition = if kind == SourcePublicationKind::Initial {
            let mut checkpoint = checkpoint(1);
            checkpoint.backend_caught_up = true;
            CheckpointTransition::try_from_checkpoint(checkpoint).unwrap()
        } else {
            let mut transition =
                CheckpointTransition::finalize_replay(&checkpoint(generation), 42, 100, "policy");
            transition.set_backend_caught_up(true);
            transition
        };
        state.set_current_head(&transition, 7);
        state.seed_transition_revision(&transition.checkpoint.operation_id, 5);
        state.set_mcp_generation_ready(initially_ready);

        let ack = if kind == SourcePublicationKind::Initial {
            actor
                .publish_initial_if_absent(&mut transition)
                .await
                .expect("repair initial publication")
                .expect("initial generation remains eligible")
        } else {
            actor
                .publish_final(&mut transition)
                .await
                .expect("repair replacement publication")
        };
        (ack, state, transition)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn initial_and_replacement_response_loss_share_reprepare_repair() {
        let (initial_ack, initial_state, initial) =
            repair_after_lost_head_ack(1, SourcePublicationKind::Initial, false).await;
        let (replacement_ack, replacement_state, replacement) =
            repair_after_lost_head_ack(2, SourcePublicationKind::Replacement, false).await;

        assert!(initial_ack.already_published);
        assert!(replacement_ack.already_published);
        assert_eq!(initial_ack.publication_revision, 7);
        assert_eq!(replacement_ack.publication_revision, 7);
        assert_eq!(initial_ack.checkpoint_revision, 6);
        assert_eq!(replacement_ack.checkpoint_revision, 6);
        assert!(initial.checkpoint.compatibility_prepared);
        assert!(replacement.checkpoint.compatibility_prepared);
        assert_eq!(
            head_publication_trace(&initial_state),
            head_publication_trace(&replacement_state),
            "both response-loss paths must prepare, persist readiness, and reactivate identically"
        );
        assert_eq!(
            initial_state.insert_count("published_source_generations"),
            0
        );
        assert_eq!(
            replacement_state.insert_count("published_source_generations"),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn direct_response_loss_activation_keeps_replacement_readiness_repair() {
        let (initial_ack, initial_state, initial) =
            repair_after_lost_head_ack(1, SourcePublicationKind::Initial, true).await;
        let (replacement_ack, replacement_state, replacement) =
            repair_after_lost_head_ack(2, SourcePublicationKind::Replacement, true).await;

        assert_eq!(initial_ack.checkpoint_revision, 5);
        assert_eq!(replacement_ack.checkpoint_revision, 6);
        assert!(!initial.checkpoint.compatibility_prepared);
        assert!(replacement.checkpoint.compatibility_prepared);
        assert_eq!(
            initial_state.insert_count("ingest_checkpoint_transitions"),
            0
        );
        assert_eq!(
            initial_state.insert_count("source_generation_publication_readiness"),
            0
        );
        assert_eq!(
            replacement_state.insert_count("ingest_checkpoint_transitions"),
            1
        );
        assert_eq!(
            replacement_state.insert_count("source_generation_publication_readiness"),
            1
        );
    }

    #[test]
    fn transition_construction_rejects_an_unknown_checkpoint_lifecycle() {
        let mut checkpoint = checkpoint(2);
        checkpoint.status = "paused".to_string();

        let error = CheckpointTransition::try_from_checkpoint(checkpoint)
            .expect_err("unknown lifecycle must not become an error transition");

        assert!(error.to_string().contains("invalid lifecycle"));
        assert!(format!("{error:#}").contains("paused"));
    }

    #[test]
    fn transition_serialization_has_one_canonical_checkpoint_state() {
        let transition = CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy");
        let encoded = serde_json::to_value(&transition).unwrap();
        let object = encoded.as_object().unwrap();

        assert_eq!(
            object.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["checkpoint".to_string(), "source".to_string(),])
        );
        assert_eq!(encoded["checkpoint"]["status"], json!("active"));
        assert!(encoded.get("lifecycle").is_none());
        assert!(encoded.get("operation_id").is_none());

        let decoded: CheckpointTransition = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, transition);
        assert_eq!(
            decoded.checkpoint.lifecycle().unwrap(),
            CheckpointLifecycle::Active
        );
    }

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_creates_and_reopens_only_secure_files() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;

        let state = TestPublicationStateDir::new("secure");
        let owner = PublicationOwnerLock::acquire(state.path(), "publisher-one")
            .expect("create secure publication lock");
        let metadata = owner.file.metadata().expect("publication lock metadata");
        assert!(metadata.is_file());
        assert_eq!(metadata.mode() & 0o7777, 0o600);
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        assert_eq!(metadata.nlink(), 1);
        // SAFETY: F_GETFD only reads descriptor flags from the valid lock fd.
        let descriptor_flags = unsafe { libc::fcntl(owner.file.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(descriptor_flags, -1);
        assert_ne!(descriptor_flags & libc::FD_CLOEXEC, 0);
        assert_eq!(
            std::fs::read_to_string(state.lock_path()).unwrap(),
            "publisher-one"
        );
        drop(owner);

        let reopened = PublicationOwnerLock::acquire(state.path(), "publisher-two")
            .expect("reopen pre-existing secure publication lock");
        assert_eq!(
            std::fs::read_to_string(state.lock_path()).unwrap(),
            "publisher-two"
        );
        drop(reopened);
    }

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_refuses_symlinks_without_touching_the_target() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let state = TestPublicationStateDir::new("symlink");
        std::fs::create_dir_all(state.path()).unwrap();
        let target = state.path().join("target");
        std::fs::write(&target, "unchanged").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, state.lock_path()).unwrap();

        let error = PublicationOwnerLock::acquire(state.path(), "attacker")
            .err()
            .expect("symlink lock must fail closed");
        assert!(format!("{error:#}").contains("failed to open publication lock"));
        assert_eq!(std::fs::read_to_string(target).unwrap(), "unchanged");
    }

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_refuses_insecure_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let state = TestPublicationStateDir::new("permissions");
        std::fs::create_dir_all(state.path()).unwrap();
        let lock_path = state.lock_path();
        std::fs::write(&lock_path, "unchanged").unwrap();
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o640)).unwrap();

        let error = PublicationOwnerLock::acquire(state.path(), "attacker")
            .err()
            .expect("insecure lock permissions must fail closed");
        assert!(error.to_string().contains("insecure permissions"));
        assert_eq!(std::fs::read_to_string(lock_path).unwrap(), "unchanged");
    }

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_refuses_multiple_hard_links() {
        use std::os::unix::fs::PermissionsExt;

        let state = TestPublicationStateDir::new("hard-link");
        std::fs::create_dir_all(state.path()).unwrap();
        let target = state.path().join("target");
        std::fs::write(&target, "unchanged").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::hard_link(&target, state.lock_path()).unwrap();

        let error = PublicationOwnerLock::acquire(state.path(), "attacker")
            .err()
            .expect("multiply linked lock must fail closed");
        assert!(error.to_string().contains("hard links"));
        assert_eq!(std::fs::read_to_string(target).unwrap(), "unchanged");
    }

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_refuses_non_regular_files() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::PermissionsExt;

        let state = TestPublicationStateDir::new("fifo");
        std::fs::create_dir_all(state.path()).unwrap();
        let lock_path = state.lock_path();
        let encoded = CString::new(lock_path.as_os_str().as_bytes()).unwrap();
        // SAFETY: encoded is a valid, NUL-terminated path and mode is valid.
        assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600)).unwrap();

        let error = PublicationOwnerLock::acquire(state.path(), "attacker")
            .err()
            .expect("non-regular lock must fail closed");
        assert!(error.to_string().contains("not a regular file"));
    }

    #[tokio::test]
    async fn finalize_replay_ack_keeps_staging_typed_and_failures_intact() {
        let (sink, mut messages) = mpsc::channel(1);
        let transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy-fingerprint");
        let staged = tokio::spawn(async move { send_finalize_replay(&sink, transition).await });
        let crate::SinkMessage::FinalizeReplay { ack, .. } =
            messages.recv().await.expect("finalize message")
        else {
            panic!("expected finalize-replay message");
        };
        ack.send(Ok(FinalizeReplayOutcome::StagedForMirror))
            .expect("finalize receiver remains open");
        assert_eq!(
            staged
                .await
                .expect("finalize task should not panic")
                .expect("staging is a successful outcome"),
            FinalizeReplayOutcome::StagedForMirror
        );

        let (sink, mut messages) = mpsc::channel(1);
        let transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy-fingerprint");
        let failed = tokio::spawn(async move { send_finalize_replay(&sink, transition).await });
        let crate::SinkMessage::FinalizeReplay { ack, .. } =
            messages.recv().await.expect("finalize message")
        else {
            panic!("expected finalize-replay message");
        };
        let failure = "publication conflict: expected revision 7, observed revision 8";
        ack.send(Err(failure.to_string()))
            .expect("finalize receiver remains open");
        assert_eq!(
            failed
                .await
                .expect("finalize task should not panic")
                .expect_err("actual publication failure must remain an error")
                .to_string(),
            failure
        );
    }

    #[test]
    fn generation_increment_is_checked() {
        assert_eq!(checked_next_generation(7).unwrap(), 8);
        assert!(checked_next_generation(u32::MAX).is_err());
    }

    #[test]
    fn control_timestamps_use_clickhouse_datetime64_json_format() {
        let timestamp = clickhouse_datetime64_now();
        assert_eq!(timestamp.len(), 23);
        assert_eq!(&timestamp[4..5], "-");
        assert_eq!(&timestamp[7..8], "-");
        assert_eq!(&timestamp[10..11], " ");
        assert_eq!(&timestamp[19..20], ".");
        assert!(!timestamp.contains('T'));
        assert!(!timestamp.contains('+'));
        assert!(!timestamp.ends_with('Z'));
    }

    #[test]
    fn replay_operation_id_is_retry_stable() {
        let left = CheckpointTransition::begin_replay(&checkpoint(2), 42, 100, "policy");
        let right = CheckpointTransition::begin_replay(&checkpoint(2), 42, 100, "policy");
        assert_eq!(left.checkpoint.operation_id, right.checkpoint.operation_id);
        assert_ne!(
            left.checkpoint.operation_id,
            CheckpointTransition::begin_replay(&checkpoint(3), 42, 100, "policy")
                .checkpoint
                .operation_id
        );
    }

    #[test]
    fn checkpoint_operation_identity_tracks_cursor_but_not_append_fence() {
        let mut value = checkpoint(1);
        value.compatibility_prepared = true;
        value.backend_caught_up = true;
        let original = CheckpointTransition::try_from_checkpoint(value.clone())
            .unwrap()
            .checkpoint
            .operation_id;

        value.append_batch_id = "retry-fence".to_string();
        value.cache_epoch = 99;
        assert_eq!(
            CheckpointTransition::try_from_checkpoint(value.clone())
                .unwrap()
                .checkpoint
                .operation_id,
            original,
            "a response-loss retry may establish a new append fence"
        );

        value.source_fingerprint = 7;
        let source_changed = CheckpointTransition::try_from_checkpoint(value.clone())
            .unwrap()
            .checkpoint
            .operation_id;
        assert_ne!(source_changed, original);

        value.schema_fingerprint = 11;
        assert_ne!(
            CheckpointTransition::try_from_checkpoint(value.clone())
                .unwrap()
                .checkpoint
                .operation_id,
            source_changed
        );

        value.last_offset += 1;
        assert_ne!(
            CheckpointTransition::try_from_checkpoint(value)
                .unwrap()
                .checkpoint
                .operation_id,
            original
        );
    }

    #[test]
    fn readiness_transition_identity_round_trips_after_each_mutation() {
        let mut transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy");
        let staged_operation_id = transition.checkpoint.operation_id.clone();

        transition.set_backend_caught_up(true);
        let caught_up_operation_id = transition.checkpoint.operation_id.clone();
        assert_ne!(caught_up_operation_id, staged_operation_id);
        assert_eq!(
            CheckpointTransition::try_from_checkpoint(transition.checkpoint.clone())
                .unwrap()
                .checkpoint
                .operation_id,
            caught_up_operation_id
        );

        transition.set_compatibility_prepared(true);
        let prepared_operation_id = transition.checkpoint.operation_id.clone();
        assert_ne!(prepared_operation_id, caught_up_operation_id);
        let reconstructed =
            CheckpointTransition::try_from_checkpoint(transition.checkpoint.clone()).unwrap();
        assert_eq!(reconstructed.checkpoint.operation_id, prepared_operation_id);
        assert!(reconstructed.checkpoint.backend_caught_up);
        assert!(reconstructed.checkpoint.compatibility_prepared);
    }

    #[test]
    fn initial_compatibility_repair_rejects_a_newer_current_head() {
        let generation_one = CurrentHeadRow {
            source_generation: 1,
            publication_revision: 7,
            operation_id: "initial-operation".to_string(),
            ..CurrentHeadRow::default()
        };
        let generation_two = CurrentHeadRow {
            source_generation: 2,
            publication_revision: 8,
            ..CurrentHeadRow::default()
        };

        assert!(is_current_initial_head(&generation_one));
        assert!(!is_current_initial_head(&generation_two));
        assert!(is_original_initial_publication(
            &generation_one,
            "initial-operation"
        ));
        assert!(!is_original_initial_publication(
            &generation_one,
            "later-append-operation"
        ));
        assert!(!is_original_initial_publication(
            &generation_two,
            "initial-operation"
        ));
    }

    #[test]
    fn final_transition_requires_every_publication_gate() {
        let mut transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy")
                .with_backend_caught_up();
        transition.set_compatibility_prepared(true);
        assert!(transition.validate_final().is_ok());
        transition.set_compatibility_prepared(false);
        assert!(transition
            .validate_final()
            .unwrap_err()
            .to_string()
            .contains("compatibility"));
    }

    #[test]
    fn append_manifest_is_deterministic_and_fail_closed() {
        let rows = vec![json!({"session_id":"s1","event_uid":"e1","project_id":"p1"})];
        let left = AppendManifest::from_pending(
            "",
            &[],
            &rows,
            &[],
            &[],
            &[],
            std::iter::once(checkpoint(1)),
        );
        let right = AppendManifest::from_pending(
            "",
            &[],
            &rows,
            &[],
            &[],
            &[],
            std::iter::once(checkpoint(1)),
        );
        assert_eq!(left.digest, right.digest);
        assert_eq!(left.batch_id(), right.batch_id());
        assert!(!left.insert_only);
        assert_eq!(left.session_ids, BTreeSet::from(["s1".to_string()]));
    }

    #[test]
    fn checkpointless_append_chunks_own_distinct_source_fences() {
        let row = |event_uid: &str| {
            json!({
                "source_host": "",
                "source_name": "opencode",
                "source_file": "/tmp/opencode.db",
                "source_generation": 3,
                "session_id": "session-a",
                "event_uid": event_uid,
            })
        };
        let first = AppendManifest::from_pending(
            "host-a",
            &[],
            &[row("event-a")],
            &[],
            &[],
            &[],
            std::iter::empty(),
        );
        let retry = AppendManifest::from_pending(
            "host-a",
            &[],
            &[row("event-a")],
            &[],
            &[],
            &[],
            std::iter::empty(),
        );
        let next = AppendManifest::from_pending(
            "host-a",
            &[],
            &[row("event-b")],
            &[],
            &[],
            &[],
            std::iter::empty(),
        );
        assert_eq!(
            first.source_keys,
            BTreeSet::from([SourceKey {
                source_host: "host-a".to_string(),
                source_name: "opencode".to_string(),
                source_file: "/tmp/opencode.db".to_string(),
            }])
        );
        assert_eq!(first.batch_id(), retry.batch_id());
        assert_ne!(first.batch_id(), next.batch_id());
    }

    #[test]
    fn startup_append_repair_requires_every_manifest_checkpoint() {
        let first = checkpoint(1);
        let mut second = checkpoint(1);
        second.source_file = "/tmp/second.jsonl".to_string();
        second.last_offset = 200;
        second.last_line_no = 8;
        let manifest = AppendManifest::from_pending(
            "",
            &[],
            &[json!({"session_id":"s1","event_uid":"e1"})],
            &[],
            &[],
            &[],
            [first.clone(), second.clone()].into_iter(),
        );
        let batch_id = manifest.batch_id();
        let proof = |checkpoint: &Checkpoint| AppendCheckpointProof {
            source_host: String::new(),
            source_name: checkpoint.source_name.clone(),
            source_file: checkpoint.source_file.clone(),
            checkpoint_revision: 10,
            source_generation: checkpoint.source_generation,
            published_source_generation: checkpoint.source_generation,
            last_offset: checkpoint.last_offset,
            last_line: checkpoint.last_line_no,
            lifecycle: "active".to_string(),
            block_reason: String::new(),
            append_batch_id: batch_id.clone(),
            cache_epoch: 5,
        };
        let first_key = source_key_manifest_id(&SourceKey::from_checkpoint(&first));
        let second_key = source_key_manifest_id(&SourceKey::from_checkpoint(&second));
        let partial = BTreeMap::from([(first_key.clone(), proof(&first))]);
        assert!(
            !append_manifest_is_complete(&manifest, &partial, &batch_id, 5),
            "a multi-source partial checkpoint must remain blocked"
        );
        let complete = BTreeMap::from([(first_key, proof(&first)), (second_key, proof(&second))]);
        assert!(append_manifest_is_complete(
            &manifest, &complete, &batch_id, 5
        ));
    }

    #[test]
    fn startup_append_repair_rejects_error_or_wrong_epoch() {
        let checkpoint = checkpoint(1);
        let manifest = AppendManifest::from_pending(
            "",
            &[],
            &[json!({"session_id":"s1","event_uid":"e1"})],
            &[],
            &[],
            &[],
            std::iter::once(checkpoint.clone()),
        );
        let batch_id = manifest.batch_id();
        let key = source_key_manifest_id(&SourceKey::from_checkpoint(&checkpoint));
        let mut proof = AppendCheckpointProof {
            source_host: String::new(),
            source_name: checkpoint.source_name.clone(),
            source_file: checkpoint.source_file.clone(),
            checkpoint_revision: 10,
            source_generation: 1,
            published_source_generation: 1,
            last_offset: checkpoint.last_offset,
            last_line: checkpoint.last_line_no,
            lifecycle: "error".to_string(),
            block_reason: "quarantined".to_string(),
            append_batch_id: batch_id.clone(),
            cache_epoch: 3,
        };
        assert!(!append_manifest_is_complete(
            &manifest,
            &BTreeMap::from([(key.clone(), proof.clone())]),
            &batch_id,
            3
        ));
        proof.lifecycle = "active".to_string();
        proof.block_reason.clear();
        proof.cache_epoch = 2;
        assert!(!append_manifest_is_complete(
            &manifest,
            &BTreeMap::from([(key.clone(), proof.clone())]),
            &batch_id,
            3
        ));

        let mut recovering = manifest.clone();
        prepare_append_recovery(
            &mut recovering,
            &BTreeMap::from([(key.clone(), proof.clone())]),
        );
        proof.checkpoint_revision = 11;
        assert!(append_manifest_recovery_is_complete(
            &recovering,
            &BTreeMap::from([(key.clone(), proof.clone())]),
            &batch_id,
            3
        ));
        proof.checkpoint_revision = 10;
        assert!(!append_manifest_recovery_is_complete(
            &recovering,
            &BTreeMap::from([(key.clone(), proof.clone())]),
            &batch_id,
            3
        ));

        proof.checkpoint_revision = 11;
        proof.source_generation = 2;
        proof.published_source_generation = 2;
        assert!(append_manifest_recovery_is_complete(
            &recovering,
            &BTreeMap::from([(key.clone(), proof.clone())]),
            &batch_id,
            3
        ));

        proof.source_generation = 1;
        proof.published_source_generation = 1;
        proof.lifecycle = CheckpointLifecycle::Replaying.as_str().to_string();
        proof.append_batch_id = batch_id.clone();
        proof.cache_epoch = 3;
        assert!(append_manifest_is_complete(
            &manifest,
            &BTreeMap::from([(key.clone(), proof.clone())]),
            &batch_id,
            3
        ));

        proof.append_batch_id = "different-batch".to_string();
        proof.cache_epoch = 2;
        proof.last_offset = checkpoint.last_offset.saturating_sub(1);
        assert!(append_manifest_can_resume(
            &manifest,
            &BTreeMap::from([(key, proof)]),
            &batch_id,
            3
        ));
    }

    #[test]
    fn append_restart_ownership_uses_the_durable_host_identity() {
        let first = "453ceec9-18c2-477b-8b7b-6545a784ac6f:10816:instance-a";
        let restarted = "453ceec9-18c2-477b-8b7b-6545a784ac6f:60138:instance-b";
        let foreign = "d57ab5c0-11b9-4e17-b642-bc2884af85bc:42:instance-c";
        assert!(same_publication_host(first, restarted));
        assert!(!same_publication_host(first, foreign));
        assert!(!same_publication_host(first, "migration-031"));
    }
}
