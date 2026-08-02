use crate::checkpoint::checkpoint_key;
use crate::model::Checkpoint;
use moraine_config::{IngestSource, SourceFormat};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub(crate) struct ProgressState {
    instance_id: String,
    run_started_unix_ms: u64,
    snapshot_unix_ms: u64,
    discovery_complete: bool,
    queue_capacity: u64,
    targets: HashMap<String, ProgressTarget>,
    last_durable_progress_unix_ms: u64,
}

#[derive(Debug)]
struct ProgressTarget {
    source_name: String,
    format: SourceFormat,
    coverage_basis: CoverageBasis,
    source_inode: Option<u64>,
    source_generation: u32,
    kiro_identity: Option<KiroTargetIdentity>,
    target_bytes: Option<u64>,
    completed_bytes: u64,
    completed: bool,
    degradation: TargetDegradation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetDegradation {
    None,
    UntilCompatibleCheckpoint,
    Permanent,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TargetObservation {
    file_size: Option<u64>,
    source_inode: Option<u64>,
    kiro_identity: Option<KiroTargetIdentity>,
}

impl TargetObservation {
    pub(crate) fn known(
        file_size: u64,
        source_inode: u64,
        kiro_identity: Option<KiroTargetIdentity>,
    ) -> Self {
        Self {
            file_size: Some(file_size),
            source_inode: Some(source_inode),
            kiro_identity,
        }
    }

    pub(crate) fn unknown() -> Self {
        Self {
            file_size: None,
            source_inode: None,
            kiro_identity: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct KiroTargetIdentity {
    source_fingerprint: u64,
    transcript_fingerprint: u64,
    sidecar_valid: bool,
    observable: bool,
}

impl KiroTargetIdentity {
    pub(crate) fn new(
        source_fingerprint: u64,
        transcript_fingerprint: u64,
        sidecar_valid: bool,
        observable: bool,
    ) -> Self {
        Self {
            source_fingerprint,
            transcript_fingerprint,
            sidecar_valid,
            observable,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoverageBasis {
    Bytes,
    Files,
}

#[derive(Serialize)]
struct ProgressSnapshot {
    schema_version: u16,
    instance_id: String,
    run_started_unix_ms: u64,
    snapshot_unix_ms: u64,
    discovery_complete: bool,
    queue_capacity: u64,
    sink_pending_rows: u64,
    sink_pending_bytes: u64,
    sink_retrying: bool,
    oldest_pending_unix_ms: u64,
    last_durable_progress_unix_ms: u64,
    files_total: u64,
    files_completed: u64,
    bytes_total: u64,
    bytes_completed: u64,
    sources: Vec<SourceProgressSnapshot>,
}

#[derive(Default, Serialize)]
struct SourceProgressSnapshot {
    source_name: String,
    format: String,
    coverage_basis: Option<CoverageBasis>,
    files_total: u64,
    files_completed: u64,
    bytes_total: u64,
    bytes_completed: u64,
    coverage_degraded: bool,
}

#[derive(Default, Deserialize)]
struct KiroCheckpointIdentity {
    #[serde(default)]
    kiro_sidecar_valid: bool,
    #[serde(default)]
    transcript_fingerprint: u64,
}

#[derive(Deserialize)]
struct SqliteCursorEvidence {
    version: u32,
    format: String,
    #[serde(default)]
    last_error: String,
}

impl ProgressState {
    pub(crate) fn new(queue_capacity: usize) -> Self {
        let now = unix_ms_now();
        Self {
            instance_id: format!("{now}-{}", std::process::id()),
            run_started_unix_ms: now,
            snapshot_unix_ms: 0,
            discovery_complete: false,
            queue_capacity: queue_capacity as u64,
            targets: HashMap::new(),
            last_durable_progress_unix_ms: 0,
        }
    }

    pub(crate) fn register_target(
        &mut self,
        source: &IngestSource,
        path: &str,
        observation: TargetObservation,
        checkpoints: &HashMap<String, Checkpoint>,
    ) {
        let coverage_basis = match source.format {
            SourceFormat::Jsonl | SourceFormat::KiroSession | SourceFormat::SessionJson => {
                CoverageBasis::Bytes
            }
            SourceFormat::CursorSqlite | SourceFormat::NacSqlite | SourceFormat::OpenCodeSqlite => {
                CoverageBasis::Files
            }
            SourceFormat::Infer => return,
        };
        let key = checkpoint_key(&source.name, path);
        let checkpoint = checkpoints.get(&key);
        let source_inode = if source.format == SourceFormat::SessionJson {
            observation.source_inode.map(|_| 0)
        } else {
            observation.source_inode
        };
        let target_bytes = if matches!(coverage_basis, CoverageBasis::Bytes) {
            observation.file_size
        } else {
            None
        };
        let identity_observable = observation.file_size.is_some()
            && source_inode.is_some()
            && observation
                .kiro_identity
                .is_none_or(|identity| identity.observable);
        let source_generation = expected_generation(
            source.format,
            source_inode,
            observation.file_size,
            checkpoint,
        );
        let mut target = ProgressTarget {
            source_name: source.name.clone(),
            format: source.format,
            coverage_basis,
            source_inode,
            source_generation,
            kiro_identity: observation.kiro_identity,
            target_bytes,
            completed_bytes: 0,
            completed: false,
            degradation: if identity_observable {
                TargetDegradation::None
            } else {
                TargetDegradation::Permanent
            },
        };

        if let Some(checkpoint) = checkpoint {
            if checkpoint_is_compatible(&target, checkpoint) {
                match target.coverage_basis {
                    CoverageBasis::Bytes => {
                        let target_bytes = target.target_bytes.unwrap_or(0);
                        target.completed_bytes = checkpoint.last_offset;
                        target.completed = checkpoint.last_offset >= target_bytes;
                    }
                    CoverageBasis::Files => target.completed = true,
                }
            } else if target.degradation != TargetDegradation::Permanent {
                target.degradation = TargetDegradation::UntilCompatibleCheckpoint;
            }
        }

        self.targets.insert(key, target);
    }

    pub(crate) fn finish_discovery(&mut self) {
        self.discovery_complete = true;
        self.snapshot_unix_ms = unix_ms_now();
    }

    pub(crate) fn acknowledge(
        &mut self,
        checkpoints: &HashMap<String, Checkpoint>,
        sqlite_cursor_durable: bool,
    ) {
        let mut advanced = false;
        for checkpoint in checkpoints.values() {
            let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
            let Some(target) = self.targets.get_mut(&key) else {
                continue;
            };
            if !checkpoint_identity_matches(target, checkpoint) {
                invalidate_target(target, TargetDegradation::Permanent);
                continue;
            }
            if !checkpoint_is_successful(target.format, checkpoint, sqlite_cursor_durable) {
                let degradation = if target.degradation == TargetDegradation::Permanent {
                    TargetDegradation::Permanent
                } else {
                    TargetDegradation::UntilCompatibleCheckpoint
                };
                invalidate_target(target, degradation);
                continue;
            }
            if target.degradation == TargetDegradation::UntilCompatibleCheckpoint {
                target.degradation = TargetDegradation::None;
                advanced = true;
            }
            if target.degradation == TargetDegradation::Permanent {
                continue;
            }
            match target.coverage_basis {
                CoverageBasis::Bytes => {
                    let Some(target_bytes) = target.target_bytes else {
                        continue;
                    };
                    let completed = checkpoint.last_offset.min(target_bytes);
                    if completed > target.completed_bytes {
                        target.completed_bytes = completed;
                        advanced = true;
                    }
                    if completed >= target_bytes && !target.completed {
                        target.completed = true;
                        advanced = true;
                    }
                }
                CoverageBasis::Files => {
                    if !target.completed {
                        target.completed = true;
                        advanced = true;
                    }
                }
            }
        }
        if advanced {
            self.last_durable_progress_unix_ms = unix_ms_now();
        }
    }

    pub(crate) fn to_json(
        &self,
        oldest_pending_unix_ms: u64,
        sink_pending_rows: u64,
        sink_pending_bytes: u64,
        sink_retrying: bool,
    ) -> String {
        let mut sources = BTreeMap::<String, SourceProgressSnapshot>::new();
        let mut files_completed = 0_u64;
        let mut bytes_total = 0_u64;
        let mut bytes_completed = 0_u64;
        for target in self.targets.values() {
            let source = sources
                .entry(target.source_name.clone())
                .or_insert_with(|| SourceProgressSnapshot {
                    source_name: target.source_name.clone(),
                    format: target.format.as_str().to_string(),
                    coverage_basis: Some(target.coverage_basis),
                    ..SourceProgressSnapshot::default()
                });
            source.coverage_degraded |= target.degradation != TargetDegradation::None;
            source.files_total = source.files_total.saturating_add(1);
            if target.completed {
                source.files_completed = source.files_completed.saturating_add(1);
                files_completed = files_completed.saturating_add(1);
            }
            if let Some(target_bytes) = target.target_bytes {
                source.bytes_total = source.bytes_total.saturating_add(target_bytes);
                source.bytes_completed = source
                    .bytes_completed
                    .saturating_add(target.completed_bytes);
                bytes_total = bytes_total.saturating_add(target_bytes);
                bytes_completed = bytes_completed.saturating_add(target.completed_bytes);
            }
        }
        serde_json::to_string(&ProgressSnapshot {
            schema_version: 1,
            instance_id: self.instance_id.clone(),
            run_started_unix_ms: self.run_started_unix_ms,
            snapshot_unix_ms: self.snapshot_unix_ms,
            discovery_complete: self.discovery_complete,
            queue_capacity: self.queue_capacity,
            sink_pending_rows,
            sink_pending_bytes,
            sink_retrying,
            oldest_pending_unix_ms,
            last_durable_progress_unix_ms: self.last_durable_progress_unix_ms,
            files_total: self.targets.len() as u64,
            files_completed,
            bytes_total,
            bytes_completed,
            sources: sources.into_values().collect(),
        })
        .unwrap_or_default()
    }
}

fn expected_generation(
    format: SourceFormat,
    source_inode: Option<u64>,
    file_size: Option<u64>,
    checkpoint: Option<&Checkpoint>,
) -> u32 {
    if format == SourceFormat::SessionJson {
        return 1;
    }
    let Some(checkpoint) = checkpoint else {
        return 1;
    };
    if source_inode.is_some_and(|inode| checkpoint.source_inode != inode)
        || file_size.is_some_and(|size| checkpoint.last_offset > size)
    {
        checkpoint.source_generation.saturating_add(1).max(1)
    } else {
        checkpoint.source_generation.max(1)
    }
}

fn checkpoint_is_compatible(target: &ProgressTarget, checkpoint: &Checkpoint) -> bool {
    checkpoint_identity_matches_initial(target, checkpoint)
        && target
            .target_bytes
            .is_none_or(|target_bytes| checkpoint.last_offset <= target_bytes)
        && checkpoint_is_successful(target.format, checkpoint, true)
}

fn checkpoint_identity_matches_initial(target: &ProgressTarget, checkpoint: &Checkpoint) -> bool {
    if target.source_inode.is_none() {
        return false;
    }
    let basic_identity_matches = if target.format == SourceFormat::SessionJson {
        true
    } else {
        Some(checkpoint.source_inode) == target.source_inode
            && checkpoint.source_generation.max(1) == target.source_generation
    };
    basic_identity_matches && kiro_identity_matches(target, checkpoint)
}

fn checkpoint_identity_matches(target: &ProgressTarget, checkpoint: &Checkpoint) -> bool {
    Some(checkpoint.source_inode) == target.source_inode
        && checkpoint.source_generation == target.source_generation
        && kiro_identity_matches(target, checkpoint)
}

fn kiro_identity_matches(target: &ProgressTarget, checkpoint: &Checkpoint) -> bool {
    let Some(expected) = target.kiro_identity else {
        return true;
    };
    if !expected.observable || checkpoint.source_fingerprint != expected.source_fingerprint {
        return false;
    }
    let cursor = if checkpoint.cursor_json.trim().is_empty() {
        KiroCheckpointIdentity::default()
    } else {
        let Ok(cursor) = serde_json::from_str::<KiroCheckpointIdentity>(&checkpoint.cursor_json)
        else {
            return false;
        };
        cursor
    };
    cursor.kiro_sidecar_valid == expected.sidecar_valid
        && cursor.transcript_fingerprint == expected.transcript_fingerprint
}

fn checkpoint_is_successful(
    format: SourceFormat,
    checkpoint: &Checkpoint,
    sqlite_cursor_durable: bool,
) -> bool {
    if checkpoint.status != "active" {
        return false;
    }
    if !matches!(
        format,
        SourceFormat::CursorSqlite | SourceFormat::NacSqlite | SourceFormat::OpenCodeSqlite
    ) {
        return true;
    }
    if !sqlite_cursor_durable {
        return false;
    }
    let raw = checkpoint.cursor_json.trim();
    if raw.is_empty() {
        return false;
    }
    serde_json::from_str::<SqliteCursorEvidence>(raw).is_ok_and(|cursor| {
        cursor.version > 0
            && cursor.format == format.as_str()
            && cursor.last_error.trim().is_empty()
    })
}

fn invalidate_target(target: &mut ProgressTarget, degradation: TargetDegradation) {
    target.completed_bytes = 0;
    target.completed = false;
    target.degradation = degradation;
}

pub(crate) fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn source(format: SourceFormat) -> IngestSource {
        IngestSource {
            name: "test-source".to_string(),
            harness: "test-harness".to_string(),
            enabled: true,
            glob: String::new(),
            watch_root: String::new(),
            format,
        }
    }

    fn snapshot(progress: &ProgressState) -> Value {
        serde_json::from_str(&progress.to_json(0, 0, 0, false)).expect("progress JSON")
    }

    fn checkpoint(
        source: &IngestSource,
        path: &str,
        inode: u64,
        generation: u32,
        offset: u64,
    ) -> Checkpoint {
        Checkpoint {
            source_name: source.name.clone(),
            source_file: path.to_string(),
            source_inode: inode,
            source_generation: generation,
            last_offset: offset,
            status: "active".to_string(),
            ..Checkpoint::default()
        }
    }

    #[test]
    fn later_files_do_not_move_frozen_snapshot() {
        let mut progress = ProgressState::new(1024);
        let source = source(SourceFormat::Jsonl);
        progress.register_target(
            &source,
            "/missing/initial.jsonl",
            TargetObservation::known(0, 1, None),
            &HashMap::new(),
        );
        progress.finish_discovery();
        assert_eq!(progress.targets.len(), 1);
    }

    #[test]
    fn replaced_file_only_completes_after_matching_generation_commits() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::Jsonl);
        let path = "/sessions/replaced.jsonl";
        let old_checkpoint = checkpoint(&source, path, 7, 1, 100);
        let mut checkpoints = HashMap::from([(checkpoint_key(&source.name, path), old_checkpoint)]);
        progress.register_target(
            &source,
            path,
            TargetObservation::known(100, 8, None),
            &checkpoints,
        );
        progress.finish_discovery();
        let before = snapshot(&progress);
        assert_eq!(before["bytes_completed"], 0);
        assert_eq!(before["sources"][0]["coverage_degraded"], true);

        checkpoints.insert(
            checkpoint_key(&source.name, path),
            checkpoint(&source, path, 8, 2, 100),
        );
        progress.acknowledge(&checkpoints, true);
        let after = snapshot(&progress);
        assert_eq!(after["bytes_completed"], 100);
        assert_eq!(after["files_completed"], 1);
        assert_eq!(after["sources"][0]["coverage_degraded"], false);
    }

    #[test]
    fn same_inode_truncation_does_not_inherit_the_old_offset() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::Jsonl);
        let path = "/sessions/truncated.jsonl";
        let mut checkpoints = HashMap::from([(
            checkpoint_key(&source.name, path),
            checkpoint(&source, path, 7, 1, 100),
        )]);
        progress.register_target(
            &source,
            path,
            TargetObservation::known(40, 7, None),
            &checkpoints,
        );
        let before = snapshot(&progress);
        assert_eq!(before["bytes_completed"], 0);
        assert_eq!(before["files_completed"], 0);
        assert_eq!(before["sources"][0]["coverage_degraded"], true);

        checkpoints.insert(
            checkpoint_key(&source.name, path),
            checkpoint(&source, path, 7, 2, 40),
        );
        progress.acknowledge(&checkpoints, true);
        let after = snapshot(&progress);
        assert_eq!(after["bytes_completed"], 40);
        assert_eq!(after["files_completed"], 1);
        assert_eq!(after["sources"][0]["coverage_degraded"], false);
    }

    #[test]
    fn metadata_failure_is_unknown_instead_of_a_zero_byte_completion() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::Jsonl);
        progress.register_target(
            &source,
            "/sessions/unreadable.jsonl",
            TargetObservation::unknown(),
            &HashMap::new(),
        );
        let value = snapshot(&progress);
        assert_eq!(value["files_total"], 1);
        assert_eq!(value["files_completed"], 0);
        assert_eq!(value["bytes_total"], 0);
        assert_eq!(value["sources"][0]["coverage_degraded"], true);
    }

    #[test]
    fn sqlite_file_completion_requires_a_successful_cursor() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::CursorSqlite);
        let path = "/sessions/cursor.db";
        let mut malformed = checkpoint(&source, path, 7, 1, 0);
        malformed.cursor_json = "{not-json".to_string();
        let mut checkpoints = HashMap::from([(checkpoint_key(&source.name, path), malformed)]);
        progress.register_target(
            &source,
            path,
            TargetObservation::known(4096, 7, None),
            &checkpoints,
        );
        assert_eq!(snapshot(&progress)["files_completed"], 0);

        let mut failed = checkpoint(&source, path, 7, 1, 0);
        failed.cursor_json = json!({
            "version": 1,
            "format": "cursor_sqlite",
            "last_error": "database_locked"
        })
        .to_string();
        checkpoints.insert(checkpoint_key(&source.name, path), failed);
        progress.acknowledge(&checkpoints, true);
        assert_eq!(snapshot(&progress)["files_completed"], 0);

        let mut successful = checkpoint(&source, path, 7, 1, 0);
        successful.cursor_json = json!({
            "version": 1,
            "format": "cursor_sqlite",
            "last_error": ""
        })
        .to_string();
        checkpoints.insert(checkpoint_key(&source.name, path), successful);
        progress.acknowledge(&checkpoints, false);
        assert_eq!(snapshot(&progress)["files_completed"], 0);
        progress.acknowledge(&checkpoints, true);
        let completed = snapshot(&progress);
        assert_eq!(completed["files_completed"], 1);
        assert_eq!(completed["sources"][0]["coverage_basis"], "files");
        assert_eq!(completed["sources"][0]["coverage_degraded"], false);
    }

    #[test]
    fn session_json_acknowledges_canonical_generation_one() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::SessionJson);
        let path = "/sessions/session.json";
        let legacy = checkpoint(&source, path, 99, 8, 5);
        let mut checkpoints = HashMap::from([(checkpoint_key(&source.name, path), legacy)]);
        progress.register_target(
            &source,
            path,
            TargetObservation::known(10, 101, None),
            &checkpoints,
        );
        assert_eq!(
            progress
                .targets
                .get(&checkpoint_key(&source.name, path))
                .expect("session target")
                .source_generation,
            1
        );
        assert_eq!(snapshot(&progress)["bytes_completed"], 5);

        checkpoints.insert(
            checkpoint_key(&source.name, path),
            checkpoint(&source, path, 0, 1, 10),
        );
        progress.acknowledge(&checkpoints, true);
        let after = snapshot(&progress);
        assert_eq!(after["bytes_completed"], 10);
        assert_eq!(after["files_completed"], 1);
    }

    #[test]
    fn post_snapshot_identity_mismatch_revokes_completion_and_degrades_coverage() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::Jsonl);
        let path = "/sessions/moved.jsonl";
        let current = checkpoint(&source, path, 7, 1, 100);
        let mut checkpoints = HashMap::from([(checkpoint_key(&source.name, path), current)]);
        progress.register_target(
            &source,
            path,
            TargetObservation::known(100, 7, None),
            &checkpoints,
        );
        assert_eq!(snapshot(&progress)["files_completed"], 1);

        checkpoints.insert(
            checkpoint_key(&source.name, path),
            checkpoint(&source, path, 8, 2, 100),
        );
        progress.acknowledge(&checkpoints, true);
        let after = snapshot(&progress);
        assert_eq!(after["files_completed"], 0);
        assert_eq!(after["bytes_completed"], 0);
        assert_eq!(after["sources"][0]["coverage_degraded"], true);

        checkpoints.insert(
            checkpoint_key(&source.name, path),
            checkpoint(&source, path, 7, 1, 100),
        );
        progress.acknowledge(&checkpoints, true);
        let still_degraded = snapshot(&progress);
        assert_eq!(still_degraded["files_completed"], 0);
        assert_eq!(still_degraded["sources"][0]["coverage_degraded"], true);
    }

    #[test]
    fn kiro_sidecar_ambiguity_clears_only_after_matching_checkpoint() {
        let mut progress = ProgressState::new(8);
        let source = source(SourceFormat::KiroSession);
        let path = "/sessions/kiro.jsonl";
        let mut old = checkpoint(&source, path, 7, 1, 100);
        old.source_fingerprint = 10;
        old.cursor_json = json!({
            "kiro_sidecar_valid": false,
            "transcript_fingerprint": 0
        })
        .to_string();
        let mut checkpoints = HashMap::from([(checkpoint_key(&source.name, path), old)]);
        let identity = KiroTargetIdentity::new(20, 30, true, true);
        progress.register_target(
            &source,
            path,
            TargetObservation::known(100, 7, Some(identity)),
            &checkpoints,
        );
        let before = snapshot(&progress);
        assert_eq!(before["files_completed"], 0);
        assert_eq!(before["sources"][0]["coverage_degraded"], true);

        let mut matching = checkpoint(&source, path, 7, 1, 100);
        matching.source_fingerprint = 20;
        matching.cursor_json = json!({
            "kiro_sidecar_valid": true,
            "transcript_fingerprint": 30
        })
        .to_string();
        checkpoints.insert(checkpoint_key(&source.name, path), matching);
        progress.acknowledge(&checkpoints, true);
        let after = snapshot(&progress);
        assert_eq!(after["files_completed"], 1);
        assert_eq!(after["sources"][0]["coverage_degraded"], false);
    }
}
