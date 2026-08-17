use crate::model::Checkpoint;
use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::ClickHouseClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

pub(crate) const PUBLICATION_PROTOCOL_VERSION: u16 = 1;

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointLifecycle {
    Active,
    Replaying,
    Error,
}

impl CheckpointLifecycle {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Replaying => "replaying",
            Self::Error => "error",
        }
    }

    pub(crate) fn parse(value: &str) -> Self {
        match value {
            "active" => Self::Active,
            "replaying" => Self::Replaying,
            _ => Self::Error,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CheckpointTransition {
    pub(crate) source: SourceKey,
    pub(crate) checkpoint: Checkpoint,
    pub(crate) checkpoint_revision: u64,
    pub(crate) operation_id: String,
    pub(crate) lifecycle: CheckpointLifecycle,
    pub(crate) protocol_version: u16,
    pub(crate) scan_inode: u64,
    pub(crate) scan_boundary: u64,
    pub(crate) policy_fingerprint: String,
    pub(crate) final_scan_complete: bool,
    pub(crate) block_reason: String,
    pub(crate) compatibility_prepared: bool,
    pub(crate) backend_caught_up: bool,
    pub(crate) append_batch_id: String,
    pub(crate) cache_epoch: u64,
}

impl CheckpointTransition {
    pub(crate) fn from_checkpoint(mut checkpoint: Checkpoint) -> Self {
        // Callers commonly build a new cursor with `..committed.clone()`.
        // Never inherit the previous transition's causal identity: derive it
        // deterministically from the complete new state so retries converge
        // while real cursor/lifecycle changes get a fresh revision.
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        let lifecycle = CheckpointLifecycle::parse(&checkpoint.status);
        let source = SourceKey::from_checkpoint(&checkpoint);
        let mut transition = Self {
            source,
            checkpoint_revision: 0,
            operation_id: String::new(),
            lifecycle,
            protocol_version: PUBLICATION_PROTOCOL_VERSION,
            scan_inode: checkpoint.scan_inode,
            scan_boundary: checkpoint.scan_boundary,
            policy_fingerprint: checkpoint.policy_fingerprint.clone(),
            final_scan_complete: checkpoint.final_scan_complete,
            block_reason: checkpoint.block_reason.clone(),
            compatibility_prepared: checkpoint.compatibility_prepared,
            backend_caught_up: checkpoint.backend_caught_up,
            append_batch_id: checkpoint.append_batch_id.clone(),
            cache_epoch: checkpoint.cache_epoch,
            checkpoint,
        };
        transition.ensure_operation_id();
        transition
    }

    pub(crate) fn begin_replay(
        checkpoint: &Checkpoint,
        scan_inode: u64,
        scan_boundary: u64,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        let mut checkpoint = checkpoint.clone();
        checkpoint.status = CheckpointLifecycle::Replaying.as_str().to_string();
        checkpoint.scan_inode = scan_inode;
        checkpoint.scan_boundary = scan_boundary;
        checkpoint.policy_fingerprint = policy_fingerprint.into();
        checkpoint.final_scan_complete = false;
        checkpoint.block_reason.clear();
        checkpoint.compatibility_prepared = false;
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        let mut transition = Self::from_checkpoint(checkpoint);
        transition.lifecycle = CheckpointLifecycle::Replaying;
        transition.ensure_operation_id();
        transition
    }

    pub(crate) fn finalize_replay(
        checkpoint: &Checkpoint,
        scan_inode: u64,
        scan_boundary: u64,
        policy_fingerprint: impl Into<String>,
    ) -> Self {
        let mut checkpoint = checkpoint.clone();
        checkpoint.status = CheckpointLifecycle::Active.as_str().to_string();
        checkpoint.scan_inode = scan_inode;
        checkpoint.scan_boundary = scan_boundary;
        checkpoint.policy_fingerprint = policy_fingerprint.into();
        checkpoint.final_scan_complete = true;
        checkpoint.block_reason.clear();
        checkpoint.compatibility_prepared = false;
        checkpoint.backend_caught_up = false;
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        let mut transition = Self::from_checkpoint(checkpoint);
        transition.lifecycle = CheckpointLifecycle::Active;
        transition.ensure_operation_id();
        transition
    }

    pub(crate) fn blocked(checkpoint: &Checkpoint, reason: impl Into<String>) -> Self {
        let mut checkpoint = checkpoint.clone();
        checkpoint.status = CheckpointLifecycle::Error.as_str().to_string();
        checkpoint.block_reason = reason.into();
        checkpoint.final_scan_complete = false;
        checkpoint.checkpoint_revision = 0;
        checkpoint.operation_id.clear();
        let mut transition = Self::from_checkpoint(checkpoint);
        transition.lifecycle = CheckpointLifecycle::Error;
        transition.ensure_operation_id();
        transition
    }

    pub(crate) fn set_backend_caught_up(&mut self, caught_up: bool) {
        self.backend_caught_up = caught_up;
        self.checkpoint.backend_caught_up = caught_up;
        self.checkpoint_revision = 0;
        self.checkpoint.checkpoint_revision = 0;
        self.operation_id.clear();
        self.checkpoint.operation_id.clear();
        self.ensure_operation_id();
    }

    pub(crate) fn canonicalize_source(&mut self, sink_host: &str) {
        self.source = self.source.clone().canonicalized(sink_host);
    }

    pub(crate) fn validate_begin_replay(&self) -> Result<()> {
        if self.lifecycle != CheckpointLifecycle::Replaying {
            bail!("begin-replay transition must have replaying lifecycle");
        }
        if self.checkpoint.source_generation < 2 {
            bail!("replacement replay requires generation >= 2");
        }
        if self.final_scan_complete {
            bail!("begin-replay transition cannot be final");
        }
        Ok(())
    }

    fn validate_final_source(&self) -> Result<()> {
        self.validate_staged_final()?;
        if !self.backend_caught_up {
            bail!("backend catch-up barrier is not durable");
        }
        Ok(())
    }

    pub(crate) fn validate_staged_final(&self) -> Result<()> {
        if self.lifecycle != CheckpointLifecycle::Active {
            bail!("final replay transition must have active lifecycle");
        }
        if !self.final_scan_complete {
            bail!("final replay transition is missing source-boundary validation");
        }
        if !self.block_reason.is_empty() {
            bail!("blocked replay cannot publish: {}", self.block_reason);
        }
        if self.scan_inode == 0 || self.scan_inode != self.checkpoint.source_inode {
            bail!("source inode changed during replay validation");
        }
        if self.checkpoint.last_offset < self.scan_boundary {
            bail!("replay stopped before the captured source boundary");
        }
        Ok(())
    }

    fn ensure_operation_id(&mut self) {
        if !self.operation_id.is_empty() {
            return;
        }
        let mut hasher = Sha256::new();
        let mut hash_string = |value: &str| {
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        };
        for value in [
            self.source.source_host.as_str(),
            self.source.source_name.as_str(),
            self.source.source_file.as_str(),
            self.lifecycle.as_str(),
            self.policy_fingerprint.as_str(),
            self.checkpoint.cursor_json.as_str(),
            self.block_reason.as_str(),
            self.append_batch_id.as_str(),
        ] {
            hash_string(value);
        }
        hasher.update(self.protocol_version.to_le_bytes());
        hasher.update(self.checkpoint.source_generation.to_le_bytes());
        hasher.update(self.checkpoint.source_inode.to_le_bytes());
        hasher.update(self.checkpoint.last_offset.to_le_bytes());
        hasher.update(self.checkpoint.last_line_no.to_le_bytes());
        hasher.update(self.checkpoint.source_fingerprint.to_le_bytes());
        hasher.update(self.checkpoint.schema_fingerprint.to_le_bytes());
        hasher.update(self.scan_inode.to_le_bytes());
        hasher.update(self.scan_boundary.to_le_bytes());
        hasher.update([self.final_scan_complete as u8]);
        hasher.update([self.compatibility_prepared as u8]);
        hasher.update([self.backend_caught_up as u8]);
        hasher.update(self.cache_epoch.to_le_bytes());
        self.operation_id = format!("cp-{:x}", hasher.finalize());
        self.checkpoint.operation_id = self.operation_id.clone();
    }

    fn to_row(&self, host: &str, checkpoint_revision: u64) -> Value {
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
            "operation_id": self.operation_id,
            "lifecycle": self.lifecycle.as_str(),
            "protocol_version": self.protocol_version,
            "scan_inode": self.scan_inode,
            "scan_boundary": self.scan_boundary,
            "policy_fingerprint": self.policy_fingerprint,
            "final_scan_complete": u8::from(self.final_scan_complete),
            "block_reason": self.block_reason,
            "compatibility_prepared": u8::from(self.compatibility_prepared),
            "backend_caught_up": u8::from(self.backend_caught_up),
            "append_batch_id": self.append_batch_id,
            "cache_epoch": self.cache_epoch,
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

pub(crate) async fn send_mirror_caught_up(sink: &mpsc::Sender<crate::SinkMessage>) -> Result<()> {
    let (ack, receive) = oneshot::channel();
    sink.send(crate::SinkMessage::MirrorCaughtUp { ack })
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

#[derive(Clone, Debug, Default, Deserialize)]
struct TransitionRevisionRow {
    checkpoint_revision: u64,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AppendManifest {
    pub(crate) source_keys: BTreeSet<SourceKey>,
    pub(crate) session_ids: BTreeSet<String>,
    pub(crate) project_ids: BTreeSet<String>,
    pub(crate) event_uids: BTreeSet<String>,
    pub(crate) dependency_uids: BTreeSet<String>,
    #[serde(default)]
    pub(crate) row_digests: BTreeSet<String>,
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
        for (table, rows) in [
            ("raw_events", raw_rows),
            ("events", event_rows),
            ("event_links", link_rows),
            ("tool_io", tool_rows),
            ("ingest_errors", error_rows),
        ] {
            for row in rows {
                let encoded = serde_json::to_vec(row)
                    .expect("pending ClickHouse row contains only serializable values");
                let mut hasher = Sha256::new();
                hasher.update((table.len() as u64).to_le_bytes());
                hasher.update(table.as_bytes());
                hasher.update((encoded.len() as u64).to_le_bytes());
                hasher.update(encoded);
                manifest
                    .row_digests
                    .insert(format!("{table}:{:x}", hasher.finalize()));
            }
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
        // Retried flush stages reuse the retained manifest. Include row
        // identities so consecutive checkpoint-less chunks from one source
        // cannot reuse an already committed fence.
        let encoded = serde_json::to_vec(&(
            &self.source_keys,
            &self.target_cursors,
            &self.session_ids,
            &self.project_ids,
            &self.event_uids,
            &self.dependency_uids,
            &self.row_digests,
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
            &self.row_digests,
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
fn append_manifest_is_checkpointless(manifest: &AppendManifest) -> bool {
    manifest.expected_counts.get("checkpoints").copied() == Some(0)
}

fn checkpointless_replay_completed(
    manifest: &AppendManifest,
    advanced_sources: Option<&BTreeSet<SourceKey>>,
) -> bool {
    append_manifest_is_checkpointless(manifest)
        && advanced_sources.is_some_and(|sources| manifest.source_keys.is_subset(sources))
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

fn legacy_migration_head_is_claimable(head: &CurrentHeadRow) -> bool {
    head.publisher_id == "migration-036" && head.operation_id.starts_with("legacy-head:")
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
    if append_manifest_is_checkpointless(manifest) {
        return !manifest.source_keys.is_empty();
    }
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
    path: PathBuf,
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if !file
                .metadata()
                .context("failed to inspect publication owner lock")?
                .file_type()
                .is_file()
            {
                bail!("publication owner lock is not a regular file");
            }
            if file
                .metadata()
                .context("failed to inspect publication owner lock permissions")?
                .mode()
                & 0o077
                != 0
            {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))
                    .context("failed to restrict publication owner lock permissions")?;
            }
        }
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
        file.set_len(0)
            .context("failed to reset publication owner lock metadata")?;
        file.write_all(publisher_id.as_bytes())
            .context("failed to write publication owner identity")?;
        file.sync_all()
            .context("failed to durably record publication owner identity")?;
        Ok(Self { file, path })
    }
}

impl Drop for PublicationOwnerLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        // SAFETY: the descriptor remains valid until after Drop returns.
        unsafe {
            libc::flock(std::os::fd::AsRawFd::as_raw_fd(&self.file), libc::LOCK_UN);
        }
        let _ = &self.path;
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
        if transition.lifecycle == CheckpointLifecycle::Replaying {
            transition.validate_begin_replay()?;
        }
        let _guard = self.gate.lock().await;
        let checkpoint = self.persist_transition_locked(transition).await?;
        if transition.lifecycle == CheckpointLifecycle::Active {
            self.commit_recovered_append_for_source_locked(&transition.source)
                .await?;
        }
        Ok(checkpoint)
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
        if transition.backend_caught_up {
            bail!("staged mirror final is already marked backend-caught-up");
        }
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        let checkpoint = self.persist_transition_locked(transition).await?;
        self.record_readiness_locked(transition, true).await?;
        Ok(checkpoint)
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
            let current = self.current_append_control().await?;
            if current.state == "idle" && current.publisher_id != self.publisher_id {
                let revision = current
                    .control_revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("append control revision exhausted"))?;
                self.clickhouse
                    .insert_json_rows_sync(
                        "ingest_append_control",
                        &[json!({
                            "host": self.source_host,
                            "control_revision": revision,
                            "cache_epoch": current.cache_epoch,
                            "state": "idle",
                            "batch_id": "",
                            "publisher_id": self.publisher_id,
                            "manifest_json": "",
                            "insert_only": 0,
                            "updated_at": clickhouse_datetime64_now(),
                        })],
                    )
                    .await
                    .context("failed to initialize append control ownership")?;
                return Ok(false);
            }
            current
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
        if let Some(revision) = self.transition_revision(&transition.operation_id).await? {
            transition.checkpoint_revision = revision;
            transition.checkpoint.checkpoint_revision = revision;
            return Ok(ReplayBarrierAck {
                checkpoint_revision: revision,
                operation_id: transition.operation_id.clone(),
            });
        }
        let revision = self.next_checkpoint_revision().await?;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_checkpoint_transitions",
                &[transition.to_row(&self.source_host, revision)],
            )
            .await
            .context("failed to persist causal ingest checkpoint")?;
        transition.checkpoint_revision = revision;
        transition.checkpoint.checkpoint_revision = revision;
        if transition.lifecycle == CheckpointLifecycle::Error || !transition.block_reason.is_empty()
        {
            self.record_readiness_locked(transition, false).await?;
        }
        Ok(ReplayBarrierAck {
            checkpoint_revision: revision,
            operation_id: transition.operation_id.clone(),
        })
    }

    async fn persist_compatibility_checkpoint_locked(
        &self,
        transition: &CheckpointTransition,
    ) -> Result<()> {
        let checkpoint = &transition.checkpoint;
        self.clickhouse
            .insert_json_rows_sync(
                "ingest_checkpoints",
                &[json!({
                    "host": self.source_host,
                    "source_name": checkpoint.source_name,
                    "source_file": checkpoint.source_file,
                    "source_inode": checkpoint.source_inode,
                    "source_generation": checkpoint.source_generation,
                    "last_offset": checkpoint.last_offset,
                    "last_line_no": checkpoint.last_line_no,
                    "status": checkpoint.status,
                    "cursor_json": checkpoint.cursor_json,
                    "source_fingerprint": checkpoint.source_fingerprint,
                    "schema_fingerprint": checkpoint.schema_fingerprint,
                })],
            )
            .await
            .context("failed to persist published compatibility checkpoint")
    }

    pub(crate) async fn publish_final(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<PublicationAck> {
        transition.validate_final_source()?;
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        if let Some(head) = self.current_head(&transition.source).await? {
            if head.source_generation == transition.checkpoint.source_generation {
                if same_publication_host(&head.publisher_id, &self.publisher_id) {
                    if transition.checkpoint_revision == 0 {
                        if let Some(revision) =
                            self.transition_revision(&transition.operation_id).await?
                        {
                            transition.checkpoint_revision = revision;
                            transition.checkpoint.checkpoint_revision = revision;
                        }
                    }
                    self.persist_compatibility_checkpoint_locked(transition)
                        .await?;
                    return Ok(PublicationAck {
                        checkpoint_revision: transition.checkpoint_revision,
                        publication_revision: head.publication_revision,
                        already_published: true,
                    });
                }
                if !legacy_migration_head_is_claimable(&head) {
                    self.record_writer_conflict(
                        &transition.source,
                        true,
                        "another durable publication host owns the current source head",
                    )
                    .await?;
                    bail!("refusing to adopt a source head owned by another publication host");
                }
            }
            if head.source_generation > transition.checkpoint.source_generation {
                bail!(
                    "refusing to publish stale generation {} behind current {}",
                    transition.checkpoint.source_generation,
                    head.source_generation
                );
            }
        }

        let publication_revision = self.next_publication_revision().await?;
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
                    "operation_id": transition.operation_id,
                    "published_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to publish source generation")?;
        let written = self
            .current_head(&transition.source)
            .await?
            .context("published source head is not observable after acknowledged insert")?;
        if written.source_generation != transition.checkpoint.source_generation
            || written.publication_revision != publication_revision
            || written.operation_id != transition.operation_id
            || written.publisher_id != self.publisher_id
        {
            self.record_writer_conflict(
                &transition.source,
                true,
                "another publisher won source-head verification",
            )
            .await?;
            bail!("conflicting publisher changed the source head during activation");
        }
        self.record_writer_conflict(&transition.source, false, "")
            .await?;
        self.commit_recovered_append_for_source_locked(&transition.source)
            .await?;
        self.persist_compatibility_checkpoint_locked(transition)
            .await?;
        Ok(PublicationAck {
            checkpoint_revision: checkpoint.checkpoint_revision,
            publication_revision,
            already_published: false,
        })
    }

    pub(crate) async fn publish_initial_if_absent(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<Option<PublicationAck>> {
        if transition.lifecycle != CheckpointLifecycle::Active
            || transition.checkpoint.source_generation != 1
            || !transition.block_reason.is_empty()
        {
            return Ok(None);
        }
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        if let Some(head) = self.current_head(&transition.source).await? {
            if !is_current_initial_head(&head) {
                return Ok(None);
            }
            if same_publication_host(&head.publisher_id, &self.publisher_id) {
                return Ok(Some(PublicationAck {
                    checkpoint_revision: transition.checkpoint_revision,
                    publication_revision: head.publication_revision,
                    already_published: true,
                }));
            }
            if !legacy_migration_head_is_claimable(&head) {
                self.record_writer_conflict(
                    &transition.source,
                    true,
                    "another durable publication host owns the initial source head",
                )
                .await?;
                bail!("refusing to adopt an initial source head owned by another publication host");
            }
        }

        transition.backend_caught_up = true;
        transition.checkpoint.backend_caught_up = true;
        transition.ensure_operation_id();
        let publication_revision = self.next_publication_revision().await?;
        let checkpoint = self.persist_transition_locked(transition).await?;
        self.record_readiness_locked(transition, true).await?;
        self.clickhouse
            .insert_json_rows_sync(
                "published_source_generations",
                &[json!({
                    "source_host": transition.source.source_host,
                    "source_name": transition.source.source_name,
                    "source_file": transition.source.source_file,
                    "source_generation": 1,
                    "publication_revision": publication_revision,
                    "publisher_id": self.publisher_id,
                    "operation_id": transition.operation_id,
                    "published_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to publish initial source generation")?;
        self.commit_recovered_append_for_source_locked(&transition.source)
            .await?;
        Ok(Some(PublicationAck {
            checkpoint_revision: checkpoint.checkpoint_revision,
            publication_revision,
            already_published: false,
        }))
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
            if checkpoint.status != CheckpointLifecycle::Active.as_str()
                || !checkpoint.block_reason.is_empty()
            {
                continue;
            }
            checkpoint.checkpoint_revision = 0;
            checkpoint.operation_id.clear();
            let mut transition = CheckpointTransition::from_checkpoint(checkpoint);
            if transition.checkpoint.source_generation == 1 {
                if let Some(publication) = self.publish_initial_if_absent(&mut transition).await? {
                    repaired.push(publication);
                }
            } else if transition.final_scan_complete && transition.backend_caught_up {
                repaired.push(self.publish_final(&mut transition).await?);
            }
        }
        Ok(repaired)
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
            if append_manifest_is_checkpointless(&persisted) && current.batch_id != batch_id {
                for source in manifest.source_keys.union(&persisted.source_keys) {
                    self.record_writer_conflict(
                        source,
                        true,
                        "checkpointless append recovery requires the original batch identity",
                    )
                    .await?;
                }
                bail!(
                    "checkpointless append recovery expected batch {}, got {}",
                    current.batch_id,
                    batch_id
                );
            }
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
            "SELECT toUInt64(count()) AS existing_count FROM {}.v_live_events \
             WHERE event_uid IN ({}) OR session_id IN ({})",
            quote_ident(&self.clickhouse.config().database),
            event_uids,
            session_ids,
        );
        let rows: Vec<CountRow> = self.clickhouse.query_rows(&query, None).await?;
        let insert_only = rows
            .first()
            .map(|row| row.existing_count == 0)
            .unwrap_or(false);
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
            if !checkpointless_replay_completed(&manifest, advanced_sources) {
                let proofs = self.append_checkpoint_proofs(&manifest).await?;
                if !append_manifest_recovery_is_complete(&manifest, &proofs, batch_id, target_epoch)
                {
                    return Ok(current.cache_epoch);
                }
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

    async fn next_checkpoint_revision(&self) -> Result<u64> {
        let query = format!(
            "SELECT toUInt64(max(transitions.checkpoint_revision)) AS revision \
             FROM {}.ingest_checkpoint_transitions AS transitions \
             WHERE transitions.host = '{}'",
            quote_ident(&self.clickhouse.config().database),
            escape_string(&self.source_host),
        );
        self.next_revision(&query, "checkpoint").await
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
        let manifest_digest = transition_manifest_digest(transition);
        self.clickhouse
            .insert_json_rows_sync(
                "source_generation_publication_readiness",
                &[json!({
                    "source_host": transition.source.source_host,
                    "source_name": transition.source.source_name,
                    "source_file": transition.source.source_file,
                    "source_generation": transition.checkpoint.source_generation,
                    "readiness_revision": readiness_revision,
                    "checkpoint_revision": transition.checkpoint_revision,
                    "operation_id": transition.operation_id,
                    "complete": u8::from(complete),
                    "block_reason": transition.block_reason,
                    "compatibility_prepared": u8::from(transition.compatibility_prepared),
                    "backend_caught_up": u8::from(transition.backend_caught_up),
                    "manifest_digest": manifest_digest,
                    "updated_at": clickhouse_datetime64_now(),
                })],
            )
            .await
            .context("failed to persist source-generation publication readiness")
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

fn transition_manifest_digest(transition: &CheckpointTransition) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transition.operation_id.as_bytes());
    hasher.update(transition.checkpoint_revision.to_le_bytes());
    hasher.update(transition.checkpoint.source_generation.to_le_bytes());
    hasher.update(transition.checkpoint.last_offset.to_le_bytes());
    hasher.update(transition.checkpoint.last_line_no.to_le_bytes());
    hasher.update([transition.compatibility_prepared as u8]);
    hasher.update([transition.backend_caught_up as u8]);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(generation: u32) -> Checkpoint {
        Checkpoint {
            source_name: "codex".to_string(),
            source_file: "/tmp/session.jsonl".to_string(),
            source_inode: 42,
            source_generation: generation,
            last_offset: 100,
            last_line_no: 4,
            status: "active".to_string(),
            ..Checkpoint::default()
        }
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

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_is_exclusive_and_reusable() {
        let state_dir =
            std::env::temp_dir().join(format!("moraine-publication-lock-{}", uuid::Uuid::new_v4()));
        let first = PublicationOwnerLock::acquire(&state_dir, "publisher-a")
            .expect("first publication owner acquires the lock");
        let conflict = match PublicationOwnerLock::acquire(&state_dir, "publisher-b") {
            Ok(_) => panic!("a concurrent publication owner must be rejected"),
            Err(error) => error,
        };
        assert!(conflict
            .to_string()
            .contains("another Moraine ingest process"));
        drop(first);
        PublicationOwnerLock::acquire(&state_dir, "publisher-c")
            .expect("the publication lock is reusable after shutdown");
        std::fs::remove_dir_all(state_dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn publication_owner_lock_refuses_symlinks() {
        use std::os::unix::fs::symlink;

        let state_dir = std::env::temp_dir().join(format!(
            "moraine-publication-lock-symlink-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&state_dir).expect("create test state directory");
        let victim = state_dir.join("victim");
        std::fs::write(&victim, "unchanged").expect("write symlink victim");
        symlink(&victim, state_dir.join("publication-owner.lock"))
            .expect("create publication lock symlink");

        assert!(PublicationOwnerLock::acquire(&state_dir, "publisher-a").is_err());
        assert_eq!(
            std::fs::read_to_string(&victim).expect("read symlink victim"),
            "unchanged"
        );
        std::fs::remove_dir_all(state_dir).ok();
    }

    #[test]
    fn replay_operation_id_is_retry_stable() {
        let left = CheckpointTransition::begin_replay(&checkpoint(2), 42, 100, "policy");
        let right = CheckpointTransition::begin_replay(&checkpoint(2), 42, 100, "policy");
        assert_eq!(left.operation_id, right.operation_id);
        assert_ne!(
            left.operation_id,
            CheckpointTransition::begin_replay(&checkpoint(3), 42, 100, "policy").operation_id
        );
    }

    #[test]
    fn readiness_transition_identity_round_trips_after_backend_catch_up() {
        let mut transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy");
        let staged_operation_id = transition.operation_id.clone();

        transition.set_backend_caught_up(true);
        let caught_up_operation_id = transition.operation_id.clone();
        assert_ne!(caught_up_operation_id, staged_operation_id);
        let reconstructed = CheckpointTransition::from_checkpoint(transition.checkpoint.clone());
        assert_eq!(reconstructed.operation_id, caught_up_operation_id);
        assert!(reconstructed.backend_caught_up);
    }

    #[test]
    fn initial_compatibility_repair_rejects_a_newer_current_head() {
        let generation_one = CurrentHeadRow {
            source_generation: 1,
            publication_revision: 7,
            ..CurrentHeadRow::default()
        };
        let generation_two = CurrentHeadRow {
            source_generation: 2,
            publication_revision: 8,
            ..CurrentHeadRow::default()
        };

        assert!(is_current_initial_head(&generation_one));
        assert!(!is_current_initial_head(&generation_two));
    }

    #[test]
    fn final_transition_requires_durable_backend_catch_up() {
        let mut transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy");
        assert!(transition
            .validate_final_source()
            .unwrap_err()
            .to_string()
            .contains("catch-up"));
        transition.set_backend_caught_up(true);
        assert!(transition.validate_final_source().is_ok());
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
    fn append_manifest_identity_changes_with_row_versions() {
        let generation_one = [json!({
            "source_name": "codex",
            "source_file": "/tmp/session.jsonl",
            "source_generation": 1,
            "session_id": "s1",
            "event_uid": "e1",
            "event_version": 1,
            "text_content": "before",
        })];
        let generation_two = [json!({
            "source_name": "codex",
            "source_file": "/tmp/session.jsonl",
            "source_generation": 1,
            "session_id": "s1",
            "event_uid": "e1",
            "event_version": 2,
            "text_content": "after",
        })];
        let before = AppendManifest::from_pending(
            "",
            &[],
            &generation_one,
            &[],
            &[],
            &[],
            std::iter::empty(),
        );
        let after = AppendManifest::from_pending(
            "",
            &[],
            &generation_two,
            &[],
            &[],
            &[],
            std::iter::empty(),
        );

        assert_ne!(before.row_digests, after.row_digests);
        assert_ne!(before.batch_id(), after.batch_id());
    }

    #[test]
    fn checkpoint_operation_identity_covers_every_durable_causal_field() {
        let baseline = CheckpointTransition::from_checkpoint(checkpoint(1));
        let mut changed_fingerprint = checkpoint(1);
        changed_fingerprint.source_fingerprint = 99;
        let changed_fingerprint = CheckpointTransition::from_checkpoint(changed_fingerprint);
        let mut changed_epoch = checkpoint(1);
        changed_epoch.cache_epoch = 7;
        changed_epoch.append_batch_id = "append-new".to_string();
        let changed_epoch = CheckpointTransition::from_checkpoint(changed_epoch);

        assert_ne!(baseline.operation_id, changed_fingerprint.operation_id);
        assert_ne!(baseline.operation_id, changed_epoch.operation_id);
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
            source_generation: checkpoint.source_generation,
            last_offset: checkpoint.last_offset,
            last_line: checkpoint.last_line_no,
            lifecycle: "active".to_string(),
            block_reason: String::new(),
            append_batch_id: batch_id.clone(),
            cache_epoch: 5,
            ..AppendCheckpointProof::default()
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
    fn checkpointless_append_recovery_requires_replaying_the_complete_source_set() {
        let rows = [json!({
            "source_host": "",
            "source_name": "codex",
            "source_file": "/tmp/session.jsonl",
            "source_generation": 1,
            "source_offset": 100,
            "source_line_no": 4,
            "session_id": "s1",
            "event_uid": "e1",
        })];
        let mut manifest =
            AppendManifest::from_pending("", &[], &rows, &[], &[], &[], std::iter::empty());
        assert!(append_manifest_can_resume(
            &manifest,
            &BTreeMap::new(),
            &manifest.batch_id(),
            5
        ));

        prepare_append_recovery(&mut manifest, &BTreeMap::new());
        assert!(!checkpointless_replay_completed(&manifest, None));
        assert!(!checkpointless_replay_completed(
            &manifest,
            Some(&BTreeSet::new())
        ));
        assert!(checkpointless_replay_completed(
            &manifest,
            Some(&manifest.source_keys)
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
            source_generation: 1,
            last_offset: checkpoint.last_offset,
            last_line: checkpoint.last_line_no,
            lifecycle: "error".to_string(),
            block_reason: "quarantined".to_string(),
            append_batch_id: batch_id.clone(),
            cache_epoch: 3,
            ..AppendCheckpointProof::default()
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
            &BTreeMap::from([(key, proof)]),
            &batch_id,
            3
        ));
    }
}
