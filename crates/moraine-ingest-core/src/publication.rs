use crate::model::Checkpoint;
use anyhow::{anyhow, bail, Context, Result};
use moraine_clickhouse::{ClickHouseClient, McpOpenPublicationRequest, McpOpenSourceHead};
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

    #[cfg(test)]
    pub(crate) fn with_backend_caught_up(mut self) -> Self {
        self.set_backend_caught_up(true);
        self
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

    fn set_compatibility_prepared(&mut self, prepared: bool) {
        self.compatibility_prepared = prepared;
        self.checkpoint.compatibility_prepared = prepared;
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

    pub(crate) fn validate_final(&self) -> Result<()> {
        self.validate_final_source()?;
        if !self.compatibility_prepared {
            bail!("compatibility projection is not prepared");
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
        for value in [
            self.source.source_host.as_str(),
            self.source.source_name.as_str(),
            self.source.source_file.as_str(),
            self.lifecycle.as_str(),
            self.policy_fingerprint.as_str(),
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
        hasher.update(self.scan_inode.to_le_bytes());
        hasher.update(self.scan_boundary.to_le_bytes());
        hasher.update([self.final_scan_complete as u8]);
        hasher.update(self.block_reason.as_bytes());
        hasher.update([self.compatibility_prepared as u8]);
        hasher.update([self.backend_caught_up as u8]);
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
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AppendCheckpointProof {
    source_generation: u32,
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
        // The physical sink drains successful stages between retries. Derive
        // the retry identity from the immutable causal target, while `digest`
        // continues to cover the full first-attempt manifest persisted in the
        // control row.
        let encoded = serde_json::to_vec(&(&self.source_keys, &self.target_cursors))
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
                    && proof.lifecycle == CheckpointLifecycle::Active.as_str()
                    && proof.block_reason.is_empty()
                    && proof.append_batch_id == batch_id
                    && proof.cache_epoch == target_epoch
            })
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
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open publication lock {}", path.display()))?;
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
        if transition.backend_caught_up {
            bail!("staged mirror final is already marked backend-caught-up");
        }
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        let checkpoint = self.persist_transition_locked(transition).await?;
        self.record_readiness_locked(transition, true).await?;
        Ok(checkpoint)
    }

    /// Persist the ordered mirror catch-up gate. Final scans supersede their
    /// staged `backend_caught_up = 0` readiness immediately; compatibility
    /// publication records the final prepared row in the next actor step.
    pub(crate) async fn persist_mirror_caught_up(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<ReplayBarrierAck> {
        if !transition.backend_caught_up {
            bail!("mirror catch-up transition is not marked backend-caught-up");
        }
        let validated_final = transition.validate_staged_final().is_ok();
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        let checkpoint = self.persist_transition_locked(transition).await?;
        if validated_final {
            self.record_readiness_locked(transition, true).await?;
        }
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
        diagnostic.block_reason = "publication_repair_failed".to_string();
        diagnostic.checkpoint.block_reason = diagnostic.block_reason.clone();
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

    /// Crash recovery never infers completion from row arrival. Until an
    /// exact stage verifier proves the persisted manifest, convert an orphaned
    /// preparation to a durable blocked fence while preserving its scope.
    pub(crate) async fn block_orphaned_append_on_startup(&self) -> Result<bool> {
        let current = {
            let _guard = self.gate.lock().await;
            self.current_append_control().await?
        };
        if current.state != "preparing" {
            return Ok(current.state == "blocked");
        }
        let manifest = match serde_json::from_str::<AppendManifest>(&current.manifest_json) {
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
        let mut proofs = BTreeMap::new();
        for source in &manifest.source_keys {
            let query = format!(
                "SELECT toUInt32(source_generation) AS source_generation, \
                        toUInt64(last_offset) AS last_offset, \
                        toUInt64(last_line) AS last_line, lifecycle, block_reason, \
                        append_batch_id, toUInt64(cache_epoch) AS cache_epoch \
                 FROM {}.v_current_ingest_checkpoint_transitions \
                 WHERE host = '{}' AND source_name = '{}' AND source_file = '{}' LIMIT 1",
                quote_ident(&self.clickhouse.config().database),
                escape_string(&source.source_host),
                escape_string(&source.source_name),
                escape_string(&source.source_file),
            );
            let mut rows: Vec<AppendCheckpointProof> =
                self.clickhouse.query_rows(&query, None).await?;
            if let Some(proof) = rows.pop() {
                proofs.insert(source_key_manifest_id(source), proof);
            }
        }
        if append_manifest_is_complete(&manifest, &proofs, &current.batch_id, target_epoch) {
            self.commit_append(&current.batch_id).await?;
            return Ok(false);
        }
        self.block_append(&current.batch_id, &manifest).await?;
        Ok(true)
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
        let revision_state = self
            .checkpoint_revision_state(&transition.operation_id)
            .await?;
        if revision_state.operation_revision > 0 {
            let revision = revision_state.operation_revision;
            transition.checkpoint_revision = revision;
            transition.checkpoint.checkpoint_revision = revision;
            return Ok(ReplayBarrierAck {
                checkpoint_revision: revision,
                operation_id: transition.operation_id.clone(),
            });
        }
        let revision = revision_state
            .max_revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("checkpoint revision exhausted"))?;
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

    pub(crate) async fn publish_final(
        &self,
        transition: &mut CheckpointTransition,
    ) -> Result<PublicationAck> {
        transition.validate_final_source()?;
        let _guard = self.gate.lock().await;
        transition.canonicalize_source(&self.source_host);
        if let Some(head) = self.current_head(&transition.source).await? {
            if head.source_generation == transition.checkpoint.source_generation {
                if transition.checkpoint_revision == 0 {
                    if let Some(revision) =
                        self.transition_revision(&transition.operation_id).await?
                    {
                        transition.checkpoint_revision = revision;
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
                        transition.checkpoint.source_generation.checked_sub(1),
                        head.publication_revision,
                    )
                    .await?;
                    transition.set_compatibility_prepared(true);
                    transition.validate_final()?;
                    self.persist_transition_locked(transition).await?;
                    self.record_readiness_locked(transition, true).await?;
                    activated = self
                        .clickhouse
                        .activate_mcp_open_publication(&candidate_id)
                        .await?;
                }
                if !activated {
                    bail!("MCP compatibility repair did not authorize the published generation");
                }
                if !transition.compatibility_prepared {
                    // Activation may repair a head committed by the prior
                    // process without entering the reprepare branch above.
                    // Persist the final 1/1/1 gate so a prior repair-failure
                    // diagnostic cannot remain current after success.
                    transition.set_compatibility_prepared(true);
                    transition.validate_final()?;
                    self.persist_transition_locked(transition).await?;
                    self.record_readiness_locked(transition, true).await?;
                }
                return Ok(PublicationAck {
                    checkpoint_revision: transition.checkpoint_revision,
                    publication_revision: head.publication_revision,
                    already_published: true,
                });
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
        let previous_generation = self
            .current_head(&transition.source)
            .await?
            .map(|head| head.source_generation);
        let candidate_id = self
            .prepare_compatibility_locked(transition, previous_generation, publication_revision)
            .await?;
        transition.set_compatibility_prepared(true);
        transition.validate_final()?;
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
        let mut activated = self
            .clickhouse
            .activate_mcp_open_publication(&candidate_id)
            .await
            .context("failed to activate MCP compatibility publication")?;
        if !activated {
            self.prepare_compatibility_locked(
                transition,
                previous_generation,
                publication_revision,
            )
            .await
            .context("failed to reprepare MCP compatibility after concurrent head change")?;
            activated = self
                .clickhouse
                .activate_mcp_open_publication(&candidate_id)
                .await
                .context("failed to reactivate MCP compatibility publication")?;
        }
        if !activated {
            bail!("MCP compatibility activation did not become current after source publication");
        }
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
            if !is_original_initial_publication(&head, &transition.operation_id) {
                let checkpoint = self.persist_transition_locked(transition).await?;
                return Ok(Some(PublicationAck {
                    checkpoint_revision: checkpoint.checkpoint_revision,
                    publication_revision: head.publication_revision,
                    already_published: true,
                }));
            }
            if transition.checkpoint_revision == 0 {
                if let Some(revision) = self.transition_revision(&transition.operation_id).await? {
                    transition.checkpoint_revision = revision;
                    transition.checkpoint.checkpoint_revision = revision;
                }
            }
            let candidate_id = candidate_publication_id(transition);
            let mut activated = self
                .clickhouse
                .activate_mcp_open_publication(&candidate_id)
                .await?;
            if !activated {
                self.prepare_compatibility_locked(transition, None, head.publication_revision)
                    .await?;
                transition.set_compatibility_prepared(true);
                self.persist_transition_locked(transition).await?;
                self.record_readiness_locked(transition, true).await?;
                activated = self
                    .clickhouse
                    .activate_mcp_open_publication(&candidate_id)
                    .await?;
            }
            if !activated {
                bail!(
                    "initial MCP compatibility repair did not authorize the published generation"
                );
            }
            return Ok(Some(PublicationAck {
                checkpoint_revision: transition.checkpoint_revision,
                publication_revision: head.publication_revision,
                already_published: true,
            }));
        }
        let publication_revision = self.next_publication_revision().await?;
        let candidate_id = self
            .prepare_compatibility_locked(transition, None, publication_revision)
            .await?;
        transition.set_compatibility_prepared(true);
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
        let written = self
            .current_head(&transition.source)
            .await?
            .context("initial source head is not observable after acknowledged insert")?;
        if written.source_generation != 1
            || written.publication_revision != publication_revision
            || written.operation_id != transition.operation_id
            || written.publisher_id != self.publisher_id
        {
            self.record_writer_conflict(
                &transition.source,
                true,
                "another publisher won initial source-head verification",
            )
            .await?;
            bail!("conflicting publisher changed the initial source head during activation");
        }
        self.record_writer_conflict(&transition.source, false, "")
            .await?;
        let mut activated = self
            .clickhouse
            .activate_mcp_open_publication(&candidate_id)
            .await
            .context("failed to activate initial MCP compatibility publication")?;
        if !activated {
            self.prepare_compatibility_locked(transition, None, publication_revision)
                .await
                .context(
                    "failed to reprepare initial MCP compatibility after concurrent head change",
                )?;
            activated = self
                .clickhouse
                .activate_mcp_open_publication(&candidate_id)
                .await
                .context("failed to reactivate initial MCP compatibility publication")?;
        }
        if !activated {
            bail!("initial MCP compatibility activation did not become current");
        }
        Ok(Some(PublicationAck {
            checkpoint_revision: checkpoint.checkpoint_revision,
            publication_revision,
            already_published: false,
        }))
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
            if checkpoint.status != CheckpointLifecycle::Active.as_str()
                || !checkpoint.block_reason.is_empty()
            {
                continue;
            }
            checkpoint.backend_caught_up = true;
            checkpoint.checkpoint_revision = 0;
            checkpoint.operation_id.clear();
            let mut transition = CheckpointTransition::from_checkpoint(checkpoint);
            transition.backend_caught_up = true;
            transition.checkpoint.backend_caught_up = true;
            if transition.checkpoint.source_generation == 1 {
                if let Some(publication) = self.publish_initial_if_absent(&mut transition).await? {
                    repaired.push(publication);
                }
            } else if transition.final_scan_complete {
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
        let clear_writer_conflict =
            !current.publisher_id.is_empty() && current.publisher_id != self.publisher_id;
        if (current.state == "preparing" || current.state == "blocked") && clear_writer_conflict {
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

    pub(crate) async fn commit_append(&self, batch_id: &str) -> Result<u64> {
        let _guard = self.gate.lock().await;
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

    async fn prepare_compatibility_locked(
        &self,
        transition: &CheckpointTransition,
        previous_source_generation: Option<u32>,
        publication_revision: u64,
    ) -> Result<String> {
        let mut required_source_heads = self.current_source_heads().await?;
        required_source_heads.retain(|head| {
            head.source_host != transition.source.source_host
                || head.source_name != transition.source.source_name
                || head.source_file != transition.source.source_file
        });
        required_source_heads.push(McpOpenSourceHead {
            source_host: transition.source.source_host.clone(),
            source_name: transition.source.source_name.clone(),
            source_file: transition.source.source_file.clone(),
            source_generation: transition.checkpoint.source_generation,
            publication_revision,
        });
        required_source_heads.sort();
        required_source_heads.dedup();
        let candidate_publication_id = candidate_publication_id(transition);
        let readiness = self
            .clickhouse
            .prepare_mcp_open_publication(&McpOpenPublicationRequest {
                candidate_publication_id: candidate_publication_id.clone(),
                operation_id: transition.operation_id.clone(),
                publisher_id: self.publisher_id.clone(),
                source_host: transition.source.source_host.clone(),
                source_name: transition.source.source_name.clone(),
                source_file: transition.source.source_file.clone(),
                previous_source_generation,
                source_generation: transition.checkpoint.source_generation,
                required_source_heads,
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

    async fn current_source_heads(&self) -> Result<Vec<McpOpenSourceHead>> {
        let query = format!(
            "SELECT source_host, source_name, source_file, \
                    toUInt32(source_generation) AS source_generation, \
                    toUInt64(publication_revision) AS publication_revision \
             FROM {}.v_current_published_source_generations \
             ORDER BY source_host, source_name, source_file",
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
    fn checkpoint_operation_identity_tracks_cursor_but_not_append_fence() {
        let mut value = checkpoint(1);
        value.compatibility_prepared = true;
        value.backend_caught_up = true;
        let original = CheckpointTransition::from_checkpoint(value.clone()).operation_id;

        value.append_batch_id = "retry-fence".to_string();
        value.cache_epoch = 99;
        assert_eq!(
            CheckpointTransition::from_checkpoint(value.clone()).operation_id,
            original,
            "a response-loss retry may establish a new append fence"
        );

        value.source_fingerprint = 7;
        let source_changed = CheckpointTransition::from_checkpoint(value.clone()).operation_id;
        assert_ne!(source_changed, original);

        value.schema_fingerprint = 11;
        assert_ne!(
            CheckpointTransition::from_checkpoint(value.clone()).operation_id,
            source_changed
        );

        value.last_offset += 1;
        assert_ne!(
            CheckpointTransition::from_checkpoint(value).operation_id,
            original
        );
    }

    #[test]
    fn readiness_transition_identity_round_trips_after_each_mutation() {
        let mut transition =
            CheckpointTransition::finalize_replay(&checkpoint(2), 42, 100, "policy");
        let staged_operation_id = transition.operation_id.clone();

        transition.set_backend_caught_up(true);
        let caught_up_operation_id = transition.operation_id.clone();
        assert_ne!(caught_up_operation_id, staged_operation_id);
        assert_eq!(
            CheckpointTransition::from_checkpoint(transition.checkpoint.clone()).operation_id,
            caught_up_operation_id
        );

        transition.set_compatibility_prepared(true);
        let prepared_operation_id = transition.operation_id.clone();
        assert_ne!(prepared_operation_id, caught_up_operation_id);
        let reconstructed = CheckpointTransition::from_checkpoint(transition.checkpoint.clone());
        assert_eq!(reconstructed.operation_id, prepared_operation_id);
        assert!(reconstructed.backend_caught_up);
        assert!(reconstructed.compatibility_prepared);
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
            source_generation: 1,
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
            &BTreeMap::from([(key, proof)]),
            &batch_id,
            3
        ));
    }
}
