use crate::checkpoint::checkpoint_key;
use crate::model::{Checkpoint, CheckpointLifecycle, NormalizedRecord, RowBatch};
use crate::normalize::{normalize_record, normalize_record_with_ts_hint};
use crate::sources::claude_code::cowork_session_path;
use crate::sources::kiro_cli::{load_kiro_session_metadata, KiroSessionMetadata};
use crate::sources::shared::{format_record_ts, infer_vendor_from_base_url, parse_record_ts};
use crate::sqlite_poll::VolatilePollMap;
use crate::{DispatchState, Metrics, SinkMessage, WorkItem, WorkTrigger};
use anyhow::{Context, Result};
use moraine_config::{is_workflow_journal_path, map_tracked_path, AppConfig, SourceFormat};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
#[cfg(not(unix))]
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, OwnedSemaphorePermit, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

#[cfg(not(unix))]
use same_file::Handle;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(not(unix))]
use std::time::UNIX_EPOCH;

/// Session-json sources read whole-file snapshots that are atomically replaced
/// every save, so the inode churns. We pin a stable synthetic identity here so
/// the checkpoint key and `event_uid` derivation stay stable across saves.
const SESSION_JSON_INODE: u64 = 0;
const SESSION_JSON_GENERATION: u32 = 1;
pub(crate) const CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT: usize = 10 * 1024 * 1024;
/// Keep per-line JSONL rows below ClickHouse's hard JSONEachRow object limit
/// after Moraine wraps the source record into raw/event rows. The default
/// ingest batch byte budget is 8 MiB; capping source lines there leaves room
/// for the row envelope and escaped `raw_json` string.
const DEFAULT_JSONL_SOURCE_LINE_BYTE_LIMIT: usize = 8 * 1024 * 1024;
const ERROR_KIND_SOURCE_LINE_TOO_LARGE: &str = "jsonl_source_line_too_large";
const ERROR_KIND_NORMALIZED_ROW_TOO_LARGE: &str = "jsonl_normalized_row_too_large";
const JSONL_PUBLICATION_PROTOCOL_VERSION: &str = "jsonl-publication-v1";

/// Version of the source-adapter normalization rules, i.e. what a given source
/// line is turned into — session attribution, row shape, derived links.
///
/// **Bump this whenever an adapter changes what rows or what attribution a
/// source line produces.** It feeds the policy fingerprints, so a bump is a
/// whole-source replacement replay: every source is re-read under a fresh
/// generation and the atomic publication switches to it, leaving the old
/// interpretation superseded rather than live alongside the new one. Without
/// the bump, a corpus keeps rows that the current code would never write, and
/// the two interpretations disagree forever — precisely the state that
/// followed the sub-agent attribution fix.
///
/// v2: session attribution became deterministic and thread-truthful. A Codex
/// sub-agent rollout embeds the parent thread's `session_meta`, which used to
/// re-attribute the rest of the file to the parent, and attribution differed
/// between a full read and a resumed read.
pub(crate) const SOURCE_NORMALIZATION_RULES_VERSION: &str = "normalization-v2";

/// Fingerprint the policy inputs that can change which logical rows a file
/// produces. A changed value is a whole-source replacement, even when the
/// inode and byte offset are unchanged: rows hidden by an old exclusion or
/// normalized with old adapter rules must not remain live alongside the new
/// interpretation.
fn jsonl_policy_fingerprint(config: &AppConfig, work: &WorkItem) -> String {
    let mut exclusions = config.ingest.exclude_project_dirs.clone();
    exclusions.sort();
    let payload = serde_json::to_vec(&json!({
        "protocol": JSONL_PUBLICATION_PROTOCOL_VERSION,
        "normalization_rules": SOURCE_NORMALIZATION_RULES_VERSION,
        "source_format": work.format.to_string(),
        "harness": work.harness,
        "project_exclusions": exclusions,
    }))
    .expect("JSONL publication policy is serializable");
    let digest = Sha256::digest(payload);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn source_scan_still_valid(source_file: &str, scan_inode: u64, scan_boundary: u64) -> Result<()> {
    let metadata = std::fs::metadata(source_file)
        .with_context(|| format!("source disappeared while scanning {source_file}"))?;
    let final_inode = source_inode_for_file(source_file, &metadata);
    anyhow::ensure!(
        final_inode == scan_inode,
        "source inode changed while scanning {source_file}: {scan_inode} -> {final_inode}"
    );
    anyhow::ensure!(
        metadata.len() >= scan_boundary,
        "source shrank while scanning {source_file}: {} < captured boundary {scan_boundary}",
        metadata.len()
    );
    Ok(())
}

async fn begin_replay_barrier(
    sink_tx: &mpsc::Sender<SinkMessage>,
    checkpoint: &Checkpoint,
    scan_inode: u64,
    scan_boundary: u64,
    policy_fingerprint: &str,
) -> Result<()> {
    let transition = crate::CheckpointTransition::begin_replay(
        checkpoint,
        scan_inode,
        scan_boundary,
        policy_fingerprint,
    );
    crate::publication::send_begin_replay(sink_tx, transition).await?;
    Ok(())
}

async fn finalize_replay_barrier(
    sink_tx: &mpsc::Sender<SinkMessage>,
    checkpoint: &Checkpoint,
    scan_inode: u64,
    scan_boundary: u64,
    policy_fingerprint: &str,
) -> Result<()> {
    let transition = crate::CheckpointTransition::finalize_replay(
        checkpoint,
        scan_inode,
        scan_boundary,
        policy_fingerprint,
    );
    match crate::publication::send_finalize_replay(sink_tx, transition).await? {
        crate::FinalizeReplayOutcome::Published(_) => {}
        crate::FinalizeReplayOutcome::StagedForMirror => {
            debug!(
                source = %checkpoint.source_name,
                path = %checkpoint.source_file,
                "replacement finalization staged until mirror catch-up barrier"
            );
        }
    }
    Ok(())
}

async fn block_replay_barrier(
    sink_tx: &mpsc::Sender<SinkMessage>,
    checkpoint: &Checkpoint,
    reason: impl Into<String>,
) -> Result<()> {
    let transition = crate::CheckpointTransition::blocked(checkpoint, reason.into());
    crate::publication::send_block_replay(sink_tx, transition).await?;
    Ok(())
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct KiroCheckpointCursor {
    #[serde(default)]
    kiro_sidecar_valid: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    record_ts_hint: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    transcript_fingerprint: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn parse_kiro_checkpoint_cursor(cursor_json: &str) -> KiroCheckpointCursor {
    serde_json::from_str(cursor_json).unwrap_or_default()
}

fn encode_kiro_checkpoint_cursor(cursor: &KiroCheckpointCursor) -> String {
    serde_json::to_string(cursor).expect("Kiro checkpoint cursor is serializable")
}

/// A work item is processable only when its path is already the canonical
/// tracked path for its format (sidecar paths are canonicalized at the
/// watcher; anything else here is a stray event for an untracked file).
fn work_path_is_canonical(work: &WorkItem) -> bool {
    map_tracked_path(work.format, &work.source_glob, &work.path).as_deref()
        == Some(work.path.as_str())
}

/// The single gate before a path becomes ingest work: every entry point
/// (backfill, reconcile, and the live watcher via the debounce task) funnels
/// through `enqueue_work`, which calls this. A path is ingestable only when it
/// is the canonical tracked path for its format AND it is not an
/// orchestration-internal trace that merely shares a session source's
/// glob/extension.
///
/// The only excluded class today is Claude Code `Workflow` journals (issue
/// #386): the recursive `~/.claude/projects/**/*.jsonl` glob (and the
/// recursive watcher) pick them up, but they carry no `sessionId` and would
/// normalize to empty-`session_id` junk that breaks `list_sessions`. Filtering
/// here — rather than tightening the glob — also catches live watcher writes,
/// which never consult the glob. The exclusion is scoped to the `claude-code`
/// harness so a same-named file under any other configured source is never
/// silently dropped.
fn work_item_is_ingestable(work: &WorkItem) -> bool {
    if !work_path_is_canonical(work) {
        debug!(
            "dropping non-canonical work item {} (format {})",
            work.path, work.format
        );
        return false;
    }
    if work.source_name == "claude-cowork" && cowork_session_path(&work.path).is_none() {
        debug!("skipping non-transcript Claude Cowork path {}", work.path);
        return false;
    }
    if work.harness == "claude-code" && is_workflow_journal_path(&work.path) {
        debug!(
            "skipping workflow orchestration journal {} (no sessionId; issue #386)",
            work.path
        );
        return false;
    }
    true
}

struct CoworkCompanionRecord {
    record: Value,
    source_file: String,
}

fn load_cowork_companion_record(work: &WorkItem) -> Option<CoworkCompanionRecord> {
    if work.source_name != "claude-cowork" {
        return None;
    }
    let cowork = cowork_session_path(&work.path)?;
    let metadata_path = cowork.metadata_path();
    let metadata = match std::fs::metadata(&metadata_path) {
        Ok(metadata) => metadata,
        Err(exc) => {
            warn!(
                source_file = %work.path,
                metadata_file = %metadata_path.display(),
                "Claude Cowork metadata unavailable: {exc}"
            );
            return None;
        }
    };
    let raw = match std::fs::File::open(&metadata_path)
        .ok()
        .and_then(|file| serde_json::from_reader::<_, Value>(file).ok())
    {
        Some(Value::Object(raw)) => raw,
        Some(_) | None => {
            warn!(
                source_file = %work.path,
                metadata_file = %metadata_path.display(),
                "Claude Cowork metadata is not a valid JSON object"
            );
            return None;
        }
    };

    let mut record = Map::new();
    record.insert(
        "type".to_string(),
        Value::String("cowork-session-meta".to_string()),
    );
    for key in [
        "cliSessionId",
        "createdAt",
        "lastActivityAt",
        "cwd",
        "model",
        "title",
        "isArchived",
        "isStarred",
    ] {
        if let Some(value) = raw.get(key) {
            record.insert(key.to_string(), value.clone());
        }
    }
    record.insert(
        "sessionId".to_string(),
        Value::String(cowork.session_id.to_owned()),
    );

    let record_ts = ["lastActivityAt", "createdAt"]
        .into_iter()
        .filter_map(|key| raw.get(key))
        .find_map(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
                .map(|timestamp| format_record_ts(&timestamp))
        })
        .or_else(|| {
            metadata.modified().ok().map(|modified| {
                let timestamp: chrono::DateTime<chrono::Utc> = modified.into();
                format_record_ts(&timestamp)
            })
        });
    if let Some(record_ts) = record_ts {
        record.insert("timestamp".to_string(), Value::String(record_ts));
    }

    let source_file = metadata_path.to_string_lossy().to_string();
    Some(CoworkCompanionRecord {
        record: Value::Object(record),
        source_file,
    })
}

fn compose_hermes_model(model: &str, base_url: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.contains('/') {
        return trimmed.to_string();
    }
    let vendor = infer_vendor_from_base_url(base_url);
    if vendor.is_empty() {
        trimmed.to_string()
    } else {
        format!("{}/{}", vendor, trimmed)
    }
}

fn jsonl_source_line_byte_limit(config: &AppConfig) -> usize {
    config
        .ingest
        .max_batch_bytes
        .clamp(1, DEFAULT_JSONL_SOURCE_LINE_BYTE_LIMIT)
}

#[allow(clippy::too_many_arguments)]
fn oversized_source_line_error_row(
    work: &WorkItem,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    line_bytes: usize,
    limit_bytes: usize,
) -> Value {
    json!({
        "source_name": work.source_name,
        "harness": work.harness,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "error_kind": ERROR_KIND_SOURCE_LINE_TOO_LARGE,
        "error_text": format!(
            "source line is {line_bytes} bytes, exceeding the {limit_bytes} byte JSONL ingest limit; skipped before normalization"
        ),
        "raw_fragment": json!({
            "action": "skipped",
            "line_bytes": line_bytes,
            "limit_bytes": limit_bytes,
        }).to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SerializedRowSize {
    pub(crate) table: &'static str,
    pub(crate) bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn oversized_normalized_row_error_row(
    work: &WorkItem,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    line_bytes: usize,
    row_size: &SerializedRowSize,
) -> Value {
    json!({
        "source_name": work.source_name,
        "harness": work.harness,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "error_kind": ERROR_KIND_NORMALIZED_ROW_TOO_LARGE,
        "error_text": format!(
            "{} row serializes to {} bytes, exceeding the {} byte ClickHouse JSON object limit; skipped before insert",
            row_size.table,
            row_size.bytes,
            CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT
        ),
        "raw_fragment": json!({
            "action": "skipped",
            "line_bytes": line_bytes,
            "serialized_row_table": row_size.table,
            "serialized_row_bytes": row_size.bytes,
            "limit_bytes": CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT,
        }).to_string(),
    })
}

fn serialized_json_object_bytes(row: &Value) -> usize {
    serde_json::to_vec(row)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

pub(crate) fn largest_serialized_normalized_row(
    normalized: &NormalizedRecord,
) -> Option<SerializedRowSize> {
    let mut largest: Option<SerializedRowSize> = None;

    let mut observe = |table: &'static str, row: &Value| {
        if row.is_null() {
            return;
        }
        let bytes = serialized_json_object_bytes(row);
        if largest.as_ref().is_none_or(|current| bytes > current.bytes) {
            largest = Some(SerializedRowSize { table, bytes });
        }
    };

    observe("raw_events", &normalized.raw_row);
    for row in &normalized.event_rows {
        observe("events", row);
    }
    for row in &normalized.link_rows {
        observe("event_links", row);
    }
    for row in &normalized.tool_rows {
        observe("tool_io", row);
    }
    for row in &normalized.error_rows {
        observe("ingest_errors", row);
    }

    largest
}

enum JsonlLineRead {
    Eof,
    Normal { buf: Vec<u8>, bytes_read: usize },
    Oversized { bytes_read: usize },
}

fn read_bounded_jsonl_line<R: BufRead>(
    reader: &mut R,
    max_bytes: usize,
) -> std::io::Result<JsonlLineRead> {
    let mut buf = Vec::<u8>::new();
    let mut bytes_read = 0usize;
    let mut oversized = false;

    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if bytes_read == 0 {
                return Ok(JsonlLineRead::Eof);
            }
            return if oversized {
                Ok(JsonlLineRead::Oversized { bytes_read })
            } else {
                Ok(JsonlLineRead::Normal { buf, bytes_read })
            };
        }

        let newline_pos = available.iter().position(|byte| *byte == b'\n');
        let take = newline_pos.map_or(available.len(), |pos| pos + 1);
        let crosses_limit = !oversized && bytes_read.saturating_add(take) > max_bytes;

        if crosses_limit {
            oversized = true;
            buf.clear();
        } else if !oversized {
            buf.extend_from_slice(&available[..take]);
        }

        reader.consume(take);
        bytes_read = bytes_read.saturating_add(take);

        if newline_pos.is_some() {
            return if oversized {
                Ok(JsonlLineRead::Oversized { bytes_read })
            } else {
                Ok(JsonlLineRead::Normal { buf, bytes_read })
            };
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_chunk_if_batch_exceeds_limits(
    batch: &mut RowBatch,
    config: &AppConfig,
    sink_tx: &mpsc::Sender<SinkMessage>,
    work: &WorkItem,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    offset: u64,
    line_no: u64,
    lifecycle: CheckpointLifecycle,
    scan_boundary: u64,
    policy_fingerprint: &str,
    context: &'static str,
) -> Result<()> {
    if !batch.exceeds_limits(config.ingest.batch_size, config.ingest.max_batch_bytes) {
        return Ok(());
    }

    let mut chunk = batch.drain_to_chunk();
    chunk.checkpoint = Some(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode,
        source_generation,
        last_offset: offset,
        last_line_no: line_no,
        status: lifecycle.to_string(),
        policy_fingerprint: policy_fingerprint.to_string(),
        scan_inode: source_inode,
        scan_boundary,
        ..Default::default()
    });

    sink_tx
        .send(SinkMessage::Batch(chunk))
        .await
        .with_context(|| format!("sink channel closed while sending {context}"))
}

/// Per-session cursor used to derive model-side latency for Claude Code
/// assistant turns. We stamp `latency_ms` on the first assistant event of a
/// record when the immediately preceding event in the same session was a
/// tool_result — that interval is bounded on both ends by machine events
/// (tool harness → model provider → next assistant block), so it cleanly
/// represents server-side processing with no human-in-the-loop noise.
#[derive(Clone, Copy)]
struct SessionCursor {
    prev_event_ts_ms: i64,
    prev_was_tool_result: bool,
}

fn parse_event_ts_ms(event_ts: &str) -> Option<i64> {
    chrono::NaiveDateTime::parse_from_str(event_ts, "%Y-%m-%d %H:%M:%S%.3f")
        .ok()
        .map(|dt| dt.and_utc().timestamp_millis())
}

fn infer_initial_record_ts_hint(source_file: &str, harness: &str, offset: u64) -> Option<String> {
    let mut file = std::fs::File::open(source_file).ok()?;
    file.seek(SeekFrom::Start(offset)).ok()?;

    let source = crate::sources::registry().get(harness)?;
    let mut reader = BufReader::new(file);
    loop {
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader.read_until(b'\n', &mut buf).ok()?;
        if bytes_read == 0 {
            break;
        }

        let text = String::from_utf8_lossy(&buf);
        if text.trim().is_empty() {
            continue;
        }
        let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let record_ts = source.record_ts(&parsed);
        if parse_record_ts(&record_ts).is_some() {
            return Some(record_ts);
        }
    }

    source_file_modified_ts(source_file)
}

fn infer_previous_record_ts_hint(source_file: &str, harness: &str, offset: u64) -> Option<String> {
    let file = std::fs::File::open(source_file).ok()?;
    let source = crate::sources::registry().get(harness)?;
    let mut reader = BufReader::new(file);
    let mut consumed = 0u64;
    let mut last_record_ts = None;

    while consumed < offset {
        let mut buf = Vec::<u8>::new();
        let bytes_read = reader.read_until(b'\n', &mut buf).ok()?;
        if bytes_read == 0 {
            break;
        }
        consumed = consumed.saturating_add(bytes_read as u64);
        if consumed > offset {
            break;
        }

        let text = String::from_utf8_lossy(&buf);
        if text.trim().is_empty() {
            continue;
        }
        let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let record_ts = source.record_ts(&parsed);
        if parse_record_ts(&record_ts).is_some() {
            last_record_ts = Some(record_ts);
        }
    }

    last_record_ts.or_else(|| source_file_modified_ts(source_file))
}

fn source_file_modified_ts(source_file: &str) -> Option<String> {
    std::fs::metadata(source_file)
        .ok()
        .and_then(|meta| meta.modified().ok())
        .map(|modified| {
            let dt: chrono::DateTime<chrono::Utc> = modified.into();
            format_record_ts(&dt)
        })
}

#[derive(Default)]
struct InitialSourceHints {
    session_id: String,
    cwd: String,
}

/// File-level identity recovered from the bounded file head, used for every
/// pass over the file so a resume starting past the session header and a read
/// starting at byte zero agree on who the file belongs to. The first non-empty
/// id wins, which is the file's own header — a forked or sub-agent transcript
/// that replays its parent's header further down never reaches this scan.
/// Priming cwd from the same head keeps resumed rows from losing it and keeps
/// leading OMP rows (a title record precedes the session header) out of an
/// empty-ID pseudo-session.
fn infer_initial_source_hints(
    source_file: &str,
    source_name: &str,
    harness: &str,
) -> InitialSourceHints {
    const MAX_HEAD_LINES: usize = 25;
    const MAX_HEAD_BYTES: u64 = 512 * 1024;

    let Some(source) = crate::sources::registry().get(harness) else {
        return InitialSourceHints::default();
    };
    let Ok(file) = std::fs::File::open(source_file) else {
        return InitialSourceHints::default();
    };
    let mut reader = BufReader::new(file.take(MAX_HEAD_BYTES));
    let mut hints = InitialSourceHints::default();

    for _ in 0..MAX_HEAD_LINES {
        let mut buf = Vec::<u8>::new();
        let Ok(bytes_read) = reader.read_until(b'\n', &mut buf) else {
            break;
        };
        if bytes_read == 0 {
            break;
        }

        let text = String::from_utf8_lossy(&buf);
        let Ok(record) = serde_json::from_str::<Value>(text.trim()) else {
            continue;
        };
        if hints.session_id.is_empty() {
            let top_type = source.top_type(&record);
            let session_id = source.session_id(
                &record,
                &crate::sources::SourceRecordContext {
                    source_name,
                    source_file,
                    session_hint: "",
                    top_type: &top_type,
                    base_uid: "",
                },
            );
            if !session_id.trim().is_empty() {
                hints.session_id = session_id;
            }
        }
        if hints.cwd.is_empty() {
            let cwd = source.cwd(&record);
            if !cwd.trim().is_empty() {
                hints.cwd = cwd;
            }
        }
        if !hints.session_id.is_empty() && !hints.cwd.is_empty() {
            break;
        }
    }

    hints
}

/// Reads a bounded JSONL prefix to find the session's first non-empty absolute
/// working directory. For Codex this stops at the initial `session_meta`
/// record, avoiding normalization and sink work for excluded trajectories.
fn infer_first_source_cwd(source_file: &str, harness: &str, max_line_bytes: usize) -> String {
    const MAX_CWD_SCAN_LINES: usize = 256;
    const MAX_CWD_SCAN_BYTES: usize = 1024 * 1024;

    let Some(source) = crate::sources::registry().get(harness) else {
        return String::new();
    };
    if !source.jsonl_carries_cwd() {
        return String::new();
    }
    let Ok(file) = std::fs::File::open(source_file) else {
        return String::new();
    };
    let mut reader = BufReader::new(file.take(MAX_CWD_SCAN_BYTES as u64));
    let mut bytes_scanned = 0usize;

    for _ in 0..MAX_CWD_SCAN_LINES {
        let Ok(read) = read_bounded_jsonl_line(&mut reader, max_line_bytes.min(MAX_CWD_SCAN_BYTES))
        else {
            return String::new();
        };
        let (buf, bytes_read) = match read {
            JsonlLineRead::Eof => return String::new(),
            JsonlLineRead::Normal { buf, bytes_read } => (Some(buf), bytes_read),
            JsonlLineRead::Oversized { bytes_read } => (None, bytes_read),
        };
        bytes_scanned = bytes_scanned.saturating_add(bytes_read);
        if bytes_scanned > MAX_CWD_SCAN_BYTES {
            return String::new();
        }
        let Some(buf) = buf else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<Value>(&buf) else {
            continue;
        };
        let cwd = source.cwd(&record);
        if std::path::Path::new(&cwd).is_absolute() {
            return cwd;
        }
    }
    String::new()
}

pub(crate) fn record_project_dir_is_excluded(
    config: &AppConfig,
    harness: &str,
    record: &Value,
    session_cwd: &str,
) -> bool {
    if config.ingest.exclude_project_dirs.is_empty() {
        return false;
    }
    let Some(source) = crate::sources::registry().get(harness) else {
        return false;
    };
    let cwd = if session_cwd.is_empty() {
        source.cwd(record)
    } else {
        session_cwd.to_string()
    };
    std::path::Path::new(&cwd).is_absolute() && config.is_project_dir_excluded(&cwd)
}

/// Post-process event rows from a single Claude Code record:
///   * if the session's prior event was a `tool_result`, stamp `latency_ms`
///     on the first assistant-actor event in this record (= wall-clock time
///     the model provider spent between `tool_result received` and
///     `first block of assistant response produced`);
///   * advance the per-session cursor for the next record.
///
/// No-op for non-claude harnesses or empty row sets. The stamped value is
/// clamped to u32 (>49 days saturates).
fn enrich_claude_model_latency(
    harness: &str,
    event_rows: &mut [Value],
    cursors: &mut HashMap<String, SessionCursor>,
) {
    if harness != "claude-code" || event_rows.is_empty() {
        return;
    }

    let session_id = event_rows[0]
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if session_id.is_empty() {
        return;
    }

    let ts_ms = match event_rows[0]
        .get("event_ts")
        .and_then(|v| v.as_str())
        .and_then(parse_event_ts_ms)
    {
        Some(ms) => ms,
        None => return,
    };

    let any_tool_result = event_rows
        .iter()
        .any(|r| r.get("event_kind").and_then(|v| v.as_str()) == Some("tool_result"));

    if let Some(cursor) = cursors.get(&session_id) {
        if cursor.prev_was_tool_result && ts_ms > cursor.prev_event_ts_ms {
            if let Some(idx) = event_rows
                .iter()
                .position(|r| r.get("actor_kind").and_then(|v| v.as_str()) == Some("assistant"))
            {
                let delta = (ts_ms - cursor.prev_event_ts_ms).max(0) as u64;
                let capped = delta.min(u32::MAX as u64) as u32;
                if let Some(obj) = event_rows[idx].as_object_mut() {
                    obj.insert("latency_ms".to_string(), json!(capped));
                }
            }
        }
    }

    // Only advance the cursor from events that participate in the turn
    // sequence (user/assistant/tool). System/progress rows are out-of-band
    // and must not reset the tool_result → assistant chain.
    let touches_turn = event_rows.iter().any(|r| {
        matches!(
            r.get("actor_kind").and_then(|v| v.as_str()),
            Some("user") | Some("assistant") | Some("tool")
        )
    });
    if touches_turn {
        cursors.insert(
            session_id,
            SessionCursor {
                prev_event_ts_ms: ts_ms,
                prev_was_tool_result: any_tool_result,
            },
        );
    }
}

pub(crate) fn spawn_debounce_task(
    config: AppConfig,
    mut rx: mpsc::UnboundedReceiver<WorkItem>,
    process_tx: mpsc::Sender<WorkItem>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let debounce = Duration::from_millis(config.ingest.debounce_ms.max(5));
        let mut pending = HashMap::<String, (WorkItem, Instant)>::new();
        let mut tick = tokio::time::interval(Duration::from_millis(
            (config.ingest.debounce_ms / 2).max(10),
        ));

        loop {
            tokio::select! {
                maybe_work = rx.recv() => {
                    match maybe_work {
                        Some(mut work) => {
                            let key = work.key();
                            // **This window is watcher-only.** Its receiver is
                            // `watch_path_rx`, and the only sender is
                            // `spawn_watcher_threads`, which funnels every
                            // event through `WorkItem::watcher` — the crate's
                            // single `WorkTrigger::Watcher` literal. Reconcile
                            // ticks and startup backfill bypass the debounce
                            // and call `enqueue_work` directly, so no reconcile
                            // tick can ever be merged away *here*.
                            //
                            // That is what makes `reconcile_owed` have exactly
                            // **one** creator, `enqueue_work`'s incoming-
                            // `Reconcile` path, which is the whole reachability
                            // argument `complete_work` rests on. An earlier
                            // revision also owed from this window; that path
                            // could not fire in production and only its own
                            // synthetic test drove it, so it is gone rather
                            // than left as a second, untestable creator.
                            //
                            // The assertion below is the invariant, not a
                            // comment about it: route a `Reconcile` producer
                            // into this channel and the merge would silently
                            // discard the tick, so such a change has to bring
                            // a ledger entry with it.
                            debug_assert_eq!(
                                work.trigger,
                                WorkTrigger::Watcher,
                                "the debounce window is fed only by the watcher \
                                 threads; a {:?} producer here would have its \
                                 trigger merged away with nothing recording it",
                                work.trigger
                            );
                            // Coalescing keeps the least reconciliation-
                            // eligible trigger (§2.4): merging never *raises*
                            // the cost of an item already queued behind a
                            // debounce window. `key()` excludes the trigger, so
                            // the two items still merge into one poll.
                            //
                            // Under the invariant asserted above this merge is
                            // **inert** — every item in this window carries
                            // `Watcher`, and `Watcher.merge(Watcher)` is
                            // `Watcher` — so no test can distinguish it from
                            // the bare `insert` below and none tries to. It is
                            // kept because the alternative is not "no merge"
                            // but last-writer-wins, which for a future mixed
                            // producer would let the *later* event raise the
                            // cost of a queued poll. Deleting it is safe today
                            // and wrong the moment the assert above starts
                            // earning its keep.
                            if let Some((queued, _)) = pending.get(&key) {
                                work.trigger = work.trigger.merge(queued.trigger);
                            }
                            pending.insert(key, (work, Instant::now()));
                        }
                        None => break,
                    }
                }
                _ = tick.tick() => {
                    if pending.is_empty() {
                        continue;
                    }

                    let now = Instant::now();
                    let ready: Vec<String> = pending
                        .iter()
                        .filter_map(|(key, (_, seen_at))| {
                            if now.duration_since(*seen_at) >= debounce {
                                Some(key.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    for key in ready {
                        if let Some((work, _)) = pending.remove(&key) {
                            // `enqueue_work` is the single ingestability gate;
                            // it early-returns on non-ingestable items, so no
                            // pre-check is needed here.
                            enqueue_work(work, &process_tx, &dispatch, &metrics).await;
                        }
                    }
                }
            }
        }
    })
}

/// Re-arm a merged-away reconcile tick onto a poll that is about to start,
/// and settle the debt in the same step.
///
/// **This is a disclosed deviation from §2.4.** That section says the dirty
/// re-enqueue "preserves the original trigger and must never upgrade it to
/// `Reconcile`"; this function is called from that re-enqueue and does exactly
/// that upgrade. The deviation is deliberate — §2.4's other bullet makes every
/// reconcile tick that lands on a busy key merge away, and on a continuously
/// written database that is *every* tick, so obeying both bullets literally
/// makes the §4 complete-sweep interval unbounded on the source with the most
/// history to sweep. What is preserved is the narrower rule the upgrade
/// actually depends on: no item that has already been handed to a poll is ever
/// upgraded. Both bullets are restated for WI-10 in
/// `plans/601-delta-sqlite.md`; the plan text is the thing that must change,
/// not this comment.
///
/// Draining here is what makes the debt settle: the poll that carries the tick
/// is the poll that pays for it. It is also the **only** remover of a
/// `reconcile_owed` entry, so "does not arm" and "does not drain" have to stay
/// the same decision — see the `Startup` refusal below.
///
/// `Startup` — and **only** `Startup` — is refused. `WorkTrigger`'s own doc says
/// startup is the worst moment to add a broad scan, and `merge` encodes that by
/// keeping `Startup` over `Reconcile`; this function is the one place in the
/// dispatcher that overrides `merge`'s ranking, so it must not override it
/// *here* as well. The debt is left standing rather than spent: a poll that does
/// not carry the tick has not paid for it, and the key's next watcher or
/// reconcile poll will.
///
/// The refusal's **width** matters as much as its existence, and the two are
/// separately guarded. Widening it by one token — refusing everything that is
/// not `Watcher` — still refuses every `Startup` poll, so the refusal's own
/// guard stays green; what it silently drops is the settlement of a standing
/// debt onto a `Reconcile` carrier, which is a reachable steady state (a tick
/// owed in the pending window, its poll completing empty, and the next reconcile
/// firing finding the key idle). The debt would then survive that firing and be
/// spent by an ordinary watcher event instead, so a filesystem event pays a
/// sweep slice nothing bought — the §2.4 inversion — and on a path that never
/// sees another watcher event it never leaves the ledger at all. That direction
/// is pinned by
/// `a_standing_debt_is_settled_by_a_reconcile_poll_not_only_a_watcher_one`.
///
/// Reachability: no production `Startup` item can hold a debt today. The two
/// `tee.rs` startup-poll sites call `process_file` directly and never
/// touch the dispatcher, and `run_ingestor`'s backfill enumerates each path
/// exactly once — so reaching this refusal needs a path that acquired an unpaid
/// tick and then went fully idle inside the startup window, before backfill's
/// round-robin got to it. That race is not structurally impossible (the
/// reconcile task is spawned before the backfill loop and `tokio::time::interval`
/// fires immediately), just vanishingly narrow. WI-04 is what makes the branch
/// worth pinning: it gives the upgrade a cost, and a `Startup` item paying it
/// is the exact thing §2.4 forbids.
fn arm_owed_reconcile(state: &mut DispatchState, key: &str, work: &mut WorkItem) {
    if work.trigger == WorkTrigger::Startup {
        return;
    }
    if state.reconcile_owed.remove(key) {
        work.trigger = WorkTrigger::Reconcile;
    }
}

pub(crate) async fn enqueue_work(
    work: WorkItem,
    process_tx: &mpsc::Sender<WorkItem>,
    dispatch: &Arc<Mutex<DispatchState>>,
    metrics: &Arc<Metrics>,
) {
    if !work_item_is_ingestable(&work) {
        return;
    }

    let mut work = work;
    let key = work.key();
    let incoming_trigger = work.trigger;
    let mut should_send = false;
    {
        let mut state = dispatch.lock().expect("dispatch mutex poisoned");
        // `item_by_key` is what `complete_work`'s dirty re-enqueue replays, so
        // overwriting it wholesale would let a reconcile tick arriving during
        // an inflight watcher poll upgrade that poll's trigger. Merge instead:
        // the trigger can be downgraded to `Watcher`, never upgraded (§2.4).
        if let Some(queued) = state.item_by_key.get(&key) {
            work.trigger = work.trigger.merge(queued.trigger);
        }
        if state.inflight.contains(&key) {
            state.item_by_key.insert(key.clone(), work.clone());
            state.dirty.insert(key.clone());
        } else if state.pending.insert(key.clone()) {
            // This send starts the poll, so an owed tick may ride it.
            arm_owed_reconcile(&mut state, &key, &mut work);
            state.item_by_key.insert(key.clone(), work.clone());
            should_send = true;
        } else {
            state.item_by_key.insert(key.clone(), work.clone());
        }

        // The debt ledger's **only** creator, and — since `complete_work` no
        // longer prunes — the only thing that bounds the map at all.
        //
        // There is no `remove` to pair with it, and the reason is not "the
        // debt is always already drained by here": settling is
        // `arm_owed_reconcile`'s job *alone*, and on the `should_send` paths it
        // has already made the call. For a `Watcher` or `Reconcile` carrier it
        // drained; for a `Startup` carrier it deliberately did **not**, and that
        // entry has to stand. A `remove` here would be redundant in the first
        // case and would destroy a live tick in the second.
        //
        // `carried` is "this call handed the tick to a poll", which is exactly
        // `should_send`. It is not also worth testing `work.trigger ==
        // Reconcile`: for a dispatching call the trigger it sends is the trigger
        // it arrived with, unless `arm_owed_reconcile` raised it to `Reconcile`.
        // `should_send` is only set where `pending.insert` returned true and the
        // key was not `inflight`, so the merge above found no `item_by_key`
        // entry — every insert site (this block and `complete_work`'s dirty
        // re-enqueue) leaves its key in `pending ∪ inflight ∪ dirty` under this
        // same lock, and the sole `remove` runs only when the key is in none of
        // them, so a key absent from `pending` and `inflight` is absent from the
        // map. The `debug_assert` below is where that invariant is stated in
        // executable form; a future change that lets a stale entry survive trips
        // it rather than silently under-owing.
        //
        // What remains is bounded in both directions by mutation:
        //
        //   * `carried = true` (never owe) is an **under-owe**: a tick landing
        //     on a busy key is treated as paid and silently dropped — two ticks
        //     straddling one queued poll become one. Guarded from beneath by
        //     `two_reconcile_ticks_straddling_one_queued_poll_buy_two_polls`.
        //   * `carried = false` (always owe) is an **over-owe**: every
        //     dispatched sweep tick is also recorded, so a single sweep leaves a
        //     standing debt on every idle path it touched. Because nothing
        //     prunes, those debts are permanent and upgrade every later watcher
        //     poll of every path to `Reconcile` — strictly worse than a per-key
        //     latch. Guarded from above by
        //     `a_reconcile_sweep_over_idle_paths_leaves_no_standing_debt`.
        //
        // `incoming_trigger == Reconcile` needs its **width** pinned too, and at
        // both neighbours rather than only at `Reconcile`. Each one-token
        // widening has its own guard, and each guard was checked by running it
        // against its own mutation rather than by reading the suite:
        //
        //   * `!= Watcher` lets a startup backfill enqueue mint a debt that some
        //     later watcher poll then spends — the §2.4 inversion
        //     `arm_owed_reconcile` refuses from the other side. Held by
        //     `a_startup_poll_landing_on_a_queued_poll_owes_nothing`, which
        //     fails under it on its own.
        //   * `!= Startup` lets every watcher event landing on a busy key mint
        //     one, so watcher churn alone manufactures sweep eligibility. Held
        //     by `one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll`,
        //     the *only* test in the crate that fails under it.
        //
        // An earlier revision of this comment named
        // `debounce_coalesces_a_watcher_burst_into_one_watcher_poll` for the
        // second. It cannot bound that width and never could: the debounce
        // coalesces the burst into a single `enqueue_work` call against an idle
        // key, so `pending.insert` returns true, `carried` is true, and this owe
        // branch is unreachable whatever the filter admits. Run alone under
        // `!= Startup` it passes.
        //
        // A debt is deliberately **not** created by observing that a queued
        // item's trigger got downgraded: `item_by_key`'s entry describes a poll
        // that is already pending or inflight, so re-owing on its downgrade is
        // re-owing a tick that was already paid. That is a latch —
        // `complete_work` arms `Reconcile` into `item_by_key`, the next watcher
        // event downgrades it and re-owes, `complete_work` re-arms — and it
        // makes *every* poll of a churning key sweep-eligible at the 50 ms
        // watcher cadence, which inverts §2.4 in the expensive direction on
        // precisely the busiest database.
        //
        // Nothing is lost by dropping that case: a queued `Reconcile` in
        // `item_by_key` was itself an incoming tick once, and at that moment it
        // was either dispatched (paid) or recorded here (owed).
        //
        // **The two failure directions are not symmetric, and this assertion is
        // deliberately the weaker kind.** The `work.trigger == Reconcile`
        // conjunct that used to sit alongside `should_send` was dead under the
        // invariant, so dropping it changed no outcome — but it failed *safely*:
        // if the invariant ever broke it would have over-owed, which is bounded
        // and self-correcting (one extra reconcile-eligible poll, spent on the
        // next completion). Bare `carried = should_send` fails the other way; a
        // break silently under-owes, and a lost tick is exactly the §0/§4
        // coverage failure the whole D1a analysis exists to prevent.
        //
        // It stays a `debug_assert!` even so. A hard `assert!` here panics with
        // the dispatch mutex held, poisoning it and killing every subsequent
        // poll for every source — trading a bounded, one-tick coverage loss for
        // a total ingest outage. The invariant is structural (it is about which
        // insert sites can leave an entry in `item_by_key`), so any change that
        // could break it is a code change, and code changes run the suite in
        // debug where this does fire.
        debug_assert!(
            !should_send
                || incoming_trigger != WorkTrigger::Reconcile
                || work.trigger == WorkTrigger::Reconcile,
            "a dispatching call cannot have merged away the tick it arrived \
             with: `should_send` implies no `item_by_key` entry existed"
        );
        //
        // This `insert` **coalesces** on purpose. `reconcile_owed` is a set, so
        // several ticks landing on a key that stays pending/inflight/dirty
        // throughout collapse to one debt and are settled by one sweep slice. A
        // slice covers everything accumulated since the last one, so the debt
        // is a flag ("this key owes a sweep") rather than an amount of work,
        // and §4's complete-sweep interval is bounded by how often the key
        // polls at all — which coalescing does not change. The declaration-site
        // doc on `DispatchState::reconcile_owed` carries the full argument;
        // `reconcile_ticks_landing_on_one_queued_poll_coalesce` pins it.
        let carried = should_send;
        if incoming_trigger == WorkTrigger::Reconcile && !carried {
            state.reconcile_owed.insert(key.clone());
        }
    }

    if should_send && process_tx.send(work).await.is_ok() {
        metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn complete_work(key: &str, dispatch: &Arc<Mutex<DispatchState>>) -> Option<WorkItem> {
    let mut state = dispatch.lock().expect("dispatch mutex poisoned");
    state.inflight.remove(key);

    if state.dirty.remove(key) {
        if state.pending.insert(key.to_string()) {
            let mut item = state.item_by_key.get(key).cloned()?;
            // The poll that was inflight has finished; this re-enqueue starts
            // a new one, so it is the first moment an owed reconcile tick can
            // be honoured without upgrading work already in flight.
            arm_owed_reconcile(&mut state, key, &mut item);
            state.item_by_key.insert(key.to_string(), item.clone());
            return Some(item);
        }
        return None;
    }

    if !state.pending.contains(key) && !state.inflight.contains(key) && !state.dirty.contains(key) {
        // The key has left the dispatcher entirely, so its queued item goes.
        //
        // `reconcile_owed` deliberately does **not** follow it out, and this is
        // not tidiness deferred: pruning the debt here destroys a live
        // reconcile tick on every firing. The ledger has exactly one production
        // creator — `enqueue_work`'s incoming-`Reconcile` path — and that path
        // splits on the key's state:
        //
        //   * key **inflight** with a `Watcher` or `Reconcile` item: `dirty` is
        //     set, so `complete_work` takes the dirty branch above and
        //     `arm_owed_reconcile` settles the debt onto the re-enqueued poll.
        //     This branch is never reached.
        //   * key **inflight** with a `Startup` item: `dirty` is set and the
        //     dirty branch still runs, but `arm_owed_reconcile` **refuses** to
        //     upgrade `Startup` (D1c) and leaves the debt standing by design.
        //     The re-enqueued poll can then complete with nothing dirty behind
        //     it and arrive *here* holding an unpaid tick, exactly like the
        //     pending case. D1c added this case after the two-case split above
        //     was written; it does not weaken the conclusion, it is a third way
        //     to reach this point with a live debt.
        //   * key **pending**: the item is already queued for a worker and
        //     `dirty` is *not* set, so the debt is recorded against a poll that
        //     still carries `Watcher`. The window is wide — the processor loop
        //     parks on `sem.acquire_owned()` before draining the next item, so
        //     a saturated worker pool holds keys in `pending` indefinitely.
        //     No poll picks that tick up, and when the queued poll finishes
        //     with nothing dirty behind it, control arrives *here* with the
        //     debt unpaid.
        //
        // So every reachable prune deletes a tick a reconcile firing created
        // milliseconds earlier — the §0/§4 coverage guarantee failing on
        // exactly the source with the most history to sweep. A *paid* debt is
        // already gone by the time this runs (`arm_owed_reconcile` drains on
        // use), so there is no reachable state in which a prune here collects
        // anything but a live tick.
        //
        // Retention costs one `String` per *indebted* path, and the bound is
        // worth stating exactly rather than rounding to "the tracked-path set".
        // For a path that stays tracked the entry is transient: the next
        // reconcile firing enumerates it and `arm_owed_reconcile` drains the
        // debt onto that poll. For a path that leaves tracking while holding an
        // unpaid debt — deleted, rotated out of the glob — nothing enumerates
        // it again, so its entry persists for the lifetime of the process and
        // is cleared only by restart. That is the honest bound: one `String`
        // per such path, unbounded in time but not in rate, against a ledger
        // whose sole creator fires once per `reconcile_interval_seconds`.
        //
        // Since this prune went, `enqueue_work`'s settle/owe condition is the
        // only thing bounding the map; it is guarded from both directions by
        // `a_reconcile_sweep_over_idle_paths_leaves_no_standing_debt` (above)
        // and `two_reconcile_ticks_straddling_one_queued_poll_buy_two_polls`
        // (beneath). Removing *this* prune is what
        // `a_tick_landing_on_a_queued_poll_survives_that_polls_completion`
        // pins, in the pending window that no other test reaches.
        state.item_by_key.remove(key);
    }

    None
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_work_item(
    config: AppConfig,
    work: WorkItem,
    permit: OwnedSemaphorePermit,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: VolatilePollMap,
    sink_tx: mpsc::Sender<crate::SinkMessage>,
    process_tx: mpsc::Sender<WorkItem>,
    dispatch: Arc<Mutex<DispatchState>>,
    metrics: Arc<Metrics>,
) {
    let key = work.key();

    if let Err(exc) =
        process_file(&config, &work, checkpoints, &poll_state, sink_tx, &metrics).await
    {
        error!(
            "failed processing {}:{}: {exc}",
            work.source_name, work.path
        );
        *metrics
            .last_error
            .lock()
            .expect("metrics last_error mutex poisoned") = exc.to_string();
    }

    let reschedule = complete_work(&key, &dispatch);

    // Release before the reschedule `send`; holding it across a full
    // `process_tx` would deadlock the processor loop (issue #215).
    drop(permit);

    if let Some(item) = reschedule {
        if process_tx.send(item).await.is_ok() {
            metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) async fn process_file(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    poll_state: &VolatilePollMap,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    match work.format {
        SourceFormat::Infer => {
            anyhow::bail!(
                "source format must be normalized before ingest processor dispatch for {}",
                work.source_name
            );
        }
        SourceFormat::Jsonl | SourceFormat::KiroSession => {}
        SourceFormat::SessionJson => {
            return process_session_json_file(config, work, checkpoints, sink_tx, metrics).await;
        }
        SourceFormat::CursorSqlite => {
            return crate::sqlite_poll::process_cursor_sqlite_db(
                config,
                work,
                checkpoints,
                poll_state,
                sink_tx,
                metrics,
            )
            .await;
        }
        SourceFormat::NacSqlite => {
            return crate::sqlite_poll::process_nac_sqlite_db(
                config,
                work,
                checkpoints,
                poll_state,
                sink_tx,
                metrics,
            )
            .await;
        }
        SourceFormat::OpenCodeSqlite => {
            return crate::sqlite_poll::process_opencode_sqlite_db(
                config,
                work,
                checkpoints,
                poll_state,
                sink_tx,
                metrics,
            )
            .await;
        }
    }

    let source_file = &work.path;

    let meta = match std::fs::metadata(source_file) {
        Ok(meta) => meta,
        Err(exc) => {
            debug!("metadata missing for {}: {}", source_file, exc);
            return Ok(());
        }
    };

    let inode = source_inode_for_file(source_file, &meta);

    // Pin the scan to this boundary. Growth after it is deliberately left for
    // the next ordinary append; reading an unbounded growing file can starve a
    // replacement's publication forever.
    let file_size = meta.len();
    let cp_key = checkpoint_key(&work.source_name, source_file);
    let committed = { checkpoints.read().await.get(&cp_key).cloned() };
    let first_ingest = committed.is_none();
    let policy_fingerprint = jsonl_policy_fingerprint(config, work);

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: 1,
        last_offset: 0,
        last_line_no: 0,
        status: CheckpointLifecycle::Active.to_string(),
        policy_fingerprint: policy_fingerprint.clone(),
        ..Default::default()
    });

    let kiro_metadata = if work.format == SourceFormat::KiroSession {
        Some(load_kiro_session_metadata(source_file))
    } else {
        None
    };
    let kiro_cursor = parse_kiro_checkpoint_cursor(&checkpoint.cursor_json);
    let sidecar_fingerprint = kiro_metadata
        .as_ref()
        .map_or(checkpoint.source_fingerprint, |metadata| {
            metadata.fingerprint()
        });
    let sidecar_changed = kiro_metadata
        .as_ref()
        .is_some_and(|metadata| metadata.fingerprint() != checkpoint.source_fingerprint);
    let kiro_sidecar_valid = kiro_metadata
        .as_ref()
        .is_some_and(|metadata| metadata.record().is_some());
    let transcript_fingerprint = kiro_metadata
        .as_ref()
        .map_or(0, KiroSessionMetadata::transcript_fingerprint);
    let sidecar_requires_transcript_replay = kiro_sidecar_valid
        && !first_ingest
        && checkpoint.last_offset > 0
        && (!kiro_cursor.kiro_sidecar_valid
            || kiro_cursor.transcript_fingerprint != transcript_fingerprint);
    let source_identity_changed = checkpoint.source_inode != inode;
    let source_truncated = file_size < checkpoint.last_offset;
    // A legacy checkpoint has no persisted policy fingerprint. Adopt the
    // current fingerprint on its next successful checkpoint; subsequent
    // changes are explicit replacement replays.
    let policy_changed = !first_ingest
        && !checkpoint.policy_fingerprint.is_empty()
        && checkpoint.policy_fingerprint != policy_fingerprint;
    let starts_replacement = source_identity_changed
        || source_truncated
        || policy_changed
        || sidecar_requires_transcript_replay;
    let checkpoint_lifecycle = checkpoint.lifecycle()?;
    let resume_replay = checkpoint_lifecycle == CheckpointLifecycle::Replaying;
    let retry_blocked_replay =
        checkpoint_lifecycle == CheckpointLifecycle::Error && !checkpoint.block_reason.is_empty();
    if starts_replacement {
        checkpoint.source_generation =
            crate::publication::checked_next_generation(checkpoint.source_generation)
                .context("source generation exhausted while beginning JSONL replacement")?;
        checkpoint.source_inode = inode;
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
        checkpoint.cursor_json.clear();
        checkpoint.policy_fingerprint = policy_fingerprint.clone();
    }
    // A blocked replay checkpoint can carry the terminal cursor of a scan
    // that quarantined one or more rows. Resuming from that cursor would see
    // EOF, forget the quarantine, and publish the incomplete generation. A
    // retry of the same candidate generation must therefore validate the
    // whole captured source again.
    if retry_blocked_replay && !starts_replacement {
        checkpoint.last_offset = 0;
        checkpoint.last_line_no = 0;
        checkpoint.cursor_json.clear();
    }
    let replacement_replay = starts_replacement || resume_replay || retry_blocked_replay;
    let scan_boundary = if resume_replay && checkpoint.scan_boundary > 0 {
        checkpoint.scan_boundary.max(checkpoint.last_offset)
    } else {
        file_size
    };
    if replacement_replay {
        checkpoint.set_lifecycle(CheckpointLifecycle::Replaying);
        checkpoint.scan_inode = inode;
        checkpoint.scan_boundary = scan_boundary;
        checkpoint.final_scan_complete = false;
        checkpoint.block_reason.clear();
        begin_replay_barrier(
            &sink_tx,
            &checkpoint,
            inode,
            scan_boundary,
            &policy_fingerprint,
        )
        .await?;
    }
    let sidecar_needs_processing =
        kiro_metadata.is_some() && (first_ingest || replacement_replay || sidecar_changed);

    if file_size == checkpoint.last_offset && !replacement_replay && !sidecar_needs_processing {
        return Ok(());
    }
    if !config.ingest.exclude_project_dirs.is_empty() {
        let sidecar_cwd = kiro_metadata
            .as_ref()
            .map(KiroSessionMetadata::cwd)
            .unwrap_or_default();
        let cwd = if sidecar_cwd.trim().is_empty() {
            infer_first_source_cwd(
                source_file,
                &work.harness,
                jsonl_source_line_byte_limit(config),
            )
        } else {
            sidecar_cwd.to_string()
        };
        if work.format == SourceFormat::KiroSession && !std::path::Path::new(&cwd).is_absolute() {
            let reason = format!(
                "Kiro project exclusions require a trusted absolute cwd, but none was available for {source_file}"
            );
            warn!(
                source_name = %work.source_name,
                harness = %work.harness,
                source_file,
                "{reason}"
            );
            if replacement_replay {
                block_replay_barrier(&sink_tx, &checkpoint, reason).await?;
            }
            return Ok(());
        }
        if config.is_project_dir_excluded(&cwd) {
            debug!(
                source_name = %work.source_name,
                harness = %work.harness,
                source_file,
                project_dir = %cwd,
                "skipping session from excluded project directory"
            );
            if replacement_replay {
                let mut final_checkpoint = checkpoint.clone();
                final_checkpoint.last_offset = scan_boundary;
                final_checkpoint.last_line_no = 0;
                final_checkpoint.set_lifecycle(CheckpointLifecycle::Active);
                final_checkpoint.final_scan_complete = true;
                final_checkpoint.compatibility_prepared = true;
                final_checkpoint.backend_caught_up = true;
                source_scan_still_valid(source_file, inode, scan_boundary)?;
                finalize_replay_barrier(
                    &sink_tx,
                    &final_checkpoint,
                    inode,
                    scan_boundary,
                    &policy_fingerprint,
                )
                .await?;
            }
            return Ok(());
        }
    }
    let cowork_companion = load_cowork_companion_record(work);
    let batch_lifecycle = if replacement_replay {
        CheckpointLifecycle::Replaying
    } else {
        CheckpointLifecycle::Active
    };

    let mut file = std::fs::File::open(source_file)
        .with_context(|| format!("failed to open {}", source_file))?;
    file.seek(SeekFrom::Start(checkpoint.last_offset))
        .with_context(|| format!("failed to seek {}", source_file))?;

    let remaining = scan_boundary.saturating_sub(checkpoint.last_offset);
    let mut reader = BufReader::new(file.take(remaining));
    let mut offset = checkpoint.last_offset;
    let mut line_no = checkpoint.last_line_no;
    // Priming is unconditional: a resume that primed and a full read that did
    // not would start the same line from different state, and the resolvers
    // that fall back to it would attribute that line two different ways.
    let initial_hints = infer_initial_source_hints(source_file, &work.source_name, &work.harness);
    // Session identity is fixed for the whole file before the first record is
    // normalized and never reassigned from a record, so `session_id` is a pure
    // function of (file, line). A record that names its own session still wins
    // — this is only what an unnamed record falls back to.
    let session_identity = kiro_metadata
        .as_ref()
        .map(KiroSessionMetadata::session_id)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&initial_hints.session_id)
        .to_string();
    let mut model_hint = kiro_metadata
        .as_ref()
        .map(KiroSessionMetadata::model)
        .unwrap_or_default()
        .to_string();
    let mut cwd_hint = kiro_metadata
        .as_ref()
        .map(KiroSessionMetadata::cwd)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&initial_hints.cwd)
        .to_string();
    let sidecar_created_at = kiro_metadata
        .as_ref()
        .map(KiroSessionMetadata::created_at)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    let persisted_kiro_record_ts = (!kiro_cursor.record_ts_hint.is_empty()
        && parse_record_ts(&kiro_cursor.record_ts_hint).is_some())
    .then(|| kiro_cursor.record_ts_hint.clone());
    let mut record_ts_hint = if work.format == SourceFormat::KiroSession {
        if checkpoint.last_offset > 0 {
            persisted_kiro_record_ts
                .or_else(|| {
                    infer_previous_record_ts_hint(
                        source_file,
                        &work.harness,
                        checkpoint.last_offset,
                    )
                })
                .or(sidecar_created_at)
        } else {
            sidecar_created_at
                .or_else(|| infer_initial_record_ts_hint(source_file, &work.harness, 0))
        }
    } else {
        infer_initial_record_ts_hint(source_file, &work.harness, checkpoint.last_offset)
    }
    .unwrap_or_default();
    let mut session_cursors: HashMap<String, SessionCursor> = HashMap::new();

    let mut batch = RowBatch::default();
    let mut replay_block_reason = None::<String>;
    if let Some(companion) = cowork_companion {
        match normalize_record(
            &companion.record,
            &work.source_name,
            &work.harness,
            source_file,
            inode,
            checkpoint.source_generation,
            1,
            0,
            "",
            "",
            "",
        ) {
            Ok(normalized) => batch.extend_normalized(normalized),
            Err(exc) => {
                warn!(
                    source_file,
                    metadata_file = %companion.source_file,
                    "Claude Cowork metadata normalization failed: {exc}"
                );
                if replacement_replay {
                    replay_block_reason = Some(format!(
                        "Claude Cowork companion metadata normalization failed: {exc}"
                    ));
                }
            }
        }
    }
    let source_line_byte_limit = jsonl_source_line_byte_limit(config);

    if sidecar_needs_processing {
        let metadata = kiro_metadata
            .as_ref()
            .expect("sidecar processing requires Kiro metadata state");
        if let Some(error_text) = metadata.error() {
            if replacement_replay {
                replay_block_reason = Some(format!("Kiro sidecar is invalid: {error_text}"));
            }
            batch.push_error_row(json!({
                "source_name": work.source_name,
                "harness": work.harness,
                "source_file": source_file,
                "source_inode": inode,
                "source_generation": checkpoint.source_generation,
                "source_line_no": 0u64,
                "source_offset": 0u64,
                "error_kind": "kiro_session_metadata_error",
                "error_text": error_text,
                "raw_fragment": "",
            }));
        }

        if let Some(record) = metadata.record() {
            match normalize_record_with_ts_hint(
                record,
                &work.source_name,
                &work.harness,
                source_file,
                inode,
                checkpoint.source_generation,
                0,
                0,
                &session_identity,
                &model_hint,
                &cwd_hint,
                &record_ts_hint,
            ) {
                Ok(normalized) => {
                    model_hint = normalized.model_hint.clone();
                    cwd_hint = normalized.cwd_hint.clone();
                    batch.extend_normalized(normalized);
                }
                Err(exc) => {
                    if replacement_replay {
                        replay_block_reason =
                            Some(format!("Kiro sidecar normalization failed: {exc}"));
                    }
                    batch.push_error_row(json!({
                        "source_name": work.source_name,
                        "harness": work.harness,
                        "source_file": source_file,
                        "source_inode": inode,
                        "source_generation": checkpoint.source_generation,
                        "source_line_no": 0u64,
                        "source_offset": 0u64,
                        "error_kind": "normalize_error",
                        "error_text": exc.to_string(),
                        "raw_fragment": truncate(&record.to_string(), 20_000),
                    }));
                }
            }
        }
    }

    loop {
        let start_offset = offset;
        let read = read_bounded_jsonl_line(&mut reader, source_line_byte_limit)
            .with_context(|| format!("failed reading {}", source_file))?;
        let (buf, bytes_read) = match read {
            JsonlLineRead::Eof => break,
            JsonlLineRead::Normal { buf, bytes_read } => (Some(buf), bytes_read),
            JsonlLineRead::Oversized { bytes_read } => (None, bytes_read),
        };

        offset = offset.saturating_add(bytes_read as u64);
        line_no = line_no.saturating_add(1);

        let Some(buf) = buf else {
            if replacement_replay && replay_block_reason.is_none() {
                replay_block_reason = Some(format!(
                    "source line {line_no} exceeded the JSONL ingest limit"
                ));
            }
            warn!(
                source_file,
                source_line_no = line_no,
                source_offset = start_offset,
                line_bytes = bytes_read,
                limit_bytes = source_line_byte_limit,
                "skipping oversized JSONL source line before normalization"
            );
            batch.push_error_row(oversized_source_line_error_row(
                work,
                source_file,
                inode,
                checkpoint.source_generation,
                line_no,
                start_offset,
                bytes_read,
                source_line_byte_limit,
            ));
            batch.lines_processed = batch.lines_processed.saturating_add(1);

            send_chunk_if_batch_exceeds_limits(
                &mut batch,
                config,
                &sink_tx,
                work,
                source_file,
                inode,
                checkpoint.source_generation,
                offset,
                line_no,
                batch_lifecycle,
                scan_boundary,
                &policy_fingerprint,
                "oversized-line chunk",
            )
            .await?;

            continue;
        };

        let mut text = String::from_utf8_lossy(&buf).to_string();
        if text.ends_with('\n') {
            text.pop();
        }

        if text.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str::<Value>(&text) {
            Ok(value) if value.is_object() => value,
            Ok(_) => {
                if replacement_replay && replay_block_reason.is_none() {
                    replay_block_reason =
                        Some(format!("source line {line_no} was not a JSON object"));
                }
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "json_parse_error",
                    "error_text": "Expected JSON object",
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
            Err(exc) => {
                if replacement_replay && replay_block_reason.is_none() {
                    replay_block_reason =
                        Some(format!("source line {line_no} failed JSON parsing: {exc}"));
                }
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "json_parse_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
        };

        let mut normalized = match normalize_record_with_ts_hint(
            &parsed,
            &work.source_name,
            &work.harness,
            source_file,
            inode,
            checkpoint.source_generation,
            line_no,
            start_offset,
            &session_identity,
            &model_hint,
            &cwd_hint,
            &record_ts_hint,
        ) {
            Ok(normalized) => normalized,
            Err(exc) => {
                if replacement_replay && replay_block_reason.is_none() {
                    replay_block_reason =
                        Some(format!("source line {line_no} failed normalization: {exc}"));
                }
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": inode,
                    "source_generation": checkpoint.source_generation,
                    "source_line_no": line_no,
                    "source_offset": start_offset,
                    "error_kind": "normalize_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&text, 20_000),
                }));
                continue;
            }
        };

        if let Some(row_size) = largest_serialized_normalized_row(&normalized) {
            if row_size.bytes > CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT {
                if replacement_replay && replay_block_reason.is_none() {
                    replay_block_reason = Some(format!(
                        "source line {line_no} normalized past the ClickHouse object limit"
                    ));
                }
                warn!(
                    source_file,
                    source_line_no = line_no,
                    source_offset = start_offset,
                    line_bytes = bytes_read,
                    serialized_row_table = row_size.table,
                    serialized_row_bytes = row_size.bytes,
                    limit_bytes = CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT,
                    "skipping JSONL source line whose normalized row is too large for ClickHouse"
                );
                batch.push_error_row(oversized_normalized_row_error_row(
                    work,
                    source_file,
                    inode,
                    checkpoint.source_generation,
                    line_no,
                    start_offset,
                    bytes_read,
                    &row_size,
                ));
                batch.lines_processed = batch.lines_processed.saturating_add(1);

                send_chunk_if_batch_exceeds_limits(
                    &mut batch,
                    config,
                    &sink_tx,
                    work,
                    source_file,
                    inode,
                    checkpoint.source_generation,
                    offset,
                    line_no,
                    batch_lifecycle,
                    scan_boundary,
                    &policy_fingerprint,
                    "oversized-normalized-row chunk",
                )
                .await?;

                continue;
            }
        }

        if let Some(record_ts) = normalized.raw_row.get("record_ts").and_then(Value::as_str) {
            if parse_record_ts(record_ts).is_some() {
                record_ts_hint = record_ts.to_string();
            }
        }

        enrich_claude_model_latency(
            &work.harness,
            &mut normalized.event_rows,
            &mut session_cursors,
        );

        // `session_identity` is deliberately not reassigned here: chaining it
        // would carry a record's resolved session forward, and a resume that
        // began below that record would carry something else.
        model_hint = normalized.model_hint.clone();
        cwd_hint = normalized.cwd_hint.clone();
        // A null `raw_row` means the normalizer deliberately skipped the
        // record (e.g. the Kimi wire metadata header). Advance the line
        // counter and checkpoint, but emit nothing downstream — passing a
        // `Value::Null` through to ClickHouse breaks the whole JSONEachRow
        // batch with "expected '{' before: 'null'".
        batch.extend_normalized(normalized);
        batch.lines_processed = batch.lines_processed.saturating_add(1);

        send_chunk_if_batch_exceeds_limits(
            &mut batch,
            config,
            &sink_tx,
            work,
            source_file,
            inode,
            checkpoint.source_generation,
            offset,
            line_no,
            batch_lifecycle,
            scan_boundary,
            &policy_fingerprint,
            "chunk",
        )
        .await?;
    }

    let kiro_cursor_json = kiro_metadata.as_ref().map(|metadata| {
        encode_kiro_checkpoint_cursor(&KiroCheckpointCursor {
            kiro_sidecar_valid,
            record_ts_hint: record_ts_hint.clone(),
            transcript_fingerprint: metadata.transcript_fingerprint(),
        })
    });
    if let Err(exc) = source_scan_still_valid(source_file, inode, scan_boundary) {
        if replacement_replay {
            let mut blocked = checkpoint.clone();
            blocked.last_offset = offset;
            blocked.last_line_no = line_no;
            blocked.set_lifecycle(CheckpointLifecycle::Error);
            blocked.policy_fingerprint = policy_fingerprint.clone();
            blocked.scan_inode = inode;
            blocked.scan_boundary = scan_boundary;
            blocked.block_reason = exc.to_string();
            block_replay_barrier(&sink_tx, &blocked, exc.to_string()).await?;
        }
        return Err(exc);
    }

    let final_checkpoint = Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: inode,
        source_generation: checkpoint.source_generation,
        last_offset: offset,
        last_line_no: line_no,
        status: CheckpointLifecycle::Active.to_string(),
        cursor_json: kiro_cursor_json.unwrap_or_else(|| checkpoint.cursor_json.clone()),
        source_fingerprint: sidecar_fingerprint,
        policy_fingerprint: policy_fingerprint.clone(),
        scan_inode: inode,
        scan_boundary,
        final_scan_complete: true,
        compatibility_prepared: true,
        backend_caught_up: true,
        ..checkpoint.clone()
    };

    if batch.row_count() > 0
        || replacement_replay
        || sidecar_needs_processing
        || offset != checkpoint.last_offset
    {
        let batch_checkpoint = if replacement_replay {
            Checkpoint {
                status: CheckpointLifecycle::Replaying.to_string(),
                final_scan_complete: false,
                compatibility_prepared: false,
                backend_caught_up: false,
                ..final_checkpoint.clone()
            }
        } else {
            final_checkpoint.clone()
        };
        batch.checkpoint = Some(batch_checkpoint);
        sink_tx
            .send(SinkMessage::Batch(batch))
            .await
            .context("sink channel closed while sending final batch")?;
        if replacement_replay {
            if let Some(reason) = replay_block_reason {
                let blocked_checkpoint = Checkpoint {
                    status: CheckpointLifecycle::Error.to_string(),
                    final_scan_complete: false,
                    compatibility_prepared: false,
                    backend_caught_up: false,
                    block_reason: reason.clone(),
                    ..final_checkpoint
                };
                block_replay_barrier(&sink_tx, &blocked_checkpoint, reason).await?;
                return Ok(());
            }
            finalize_replay_barrier(
                &sink_tx,
                &final_checkpoint,
                inode,
                scan_boundary,
                &policy_fingerprint,
            )
            .await?;
        }
    }

    if metrics.queue_depth.load(Ordering::Relaxed) == 0 {
        debug!(
            "{}:{} caught up at offset {}",
            work.source_name, source_file, offset
        );
    }

    Ok(())
}

/// Process a Hermes live-session file (single JSON document, rewritten in
/// place via atomic rename every save). Each message in `messages[]` is
/// normalized independently, with the checkpoint's `last_line_no` acting as a
/// "last-emitted message index" cursor. We pin a synthetic inode/generation so
/// event_uids remain stable across saves, and rely on the ClickHouse
/// ReplacingMergeTree on `events` to dedupe any re-emits.
async fn process_session_json_file(
    config: &AppConfig,
    work: &WorkItem,
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
    sink_tx: mpsc::Sender<SinkMessage>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let source_file = &work.path;

    let body = match std::fs::read_to_string(source_file) {
        Ok(body) => body,
        Err(exc) => {
            debug!("session_json read skipped {}: {}", source_file, exc);
            return Ok(());
        }
    };
    let file_size = body.len() as u64;

    if body.trim().is_empty() {
        return Ok(());
    }

    let session_doc: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(exc) => {
            // Atomic-rename keeps the on-disk file consistent, so a parse error
            // likely means the writer is still warming up or the file is
            // corrupted. Emit an error row and move on — we'll try again on the
            // next modify event.
            warn!(source_file, "session_json parse failed; skipping: {}", exc);
            let error_row = json!({
                "source_name": work.source_name,
                "harness": work.harness,
                "source_file": source_file,
                "source_inode": SESSION_JSON_INODE,
                "source_generation": SESSION_JSON_GENERATION,
                "source_line_no": 0u64,
                "source_offset": 0u64,
                "error_kind": "json_parse_error",
                "error_text": exc.to_string(),
                "raw_fragment": truncate(&body, 20_000),
            });
            let mut batch = RowBatch::default();
            batch.push_error_row(error_row);
            sink_tx
                .send(SinkMessage::Batch(batch))
                .await
                .context("sink channel closed while sending session_json parse error")?;
            return Ok(());
        }
    };

    let messages = session_doc
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let message_count = messages.len() as u64;

    let cp_key = checkpoint_key(&work.source_name, source_file);
    let committed = { checkpoints.read().await.get(&cp_key).cloned() };

    let mut checkpoint = committed.unwrap_or(Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: SESSION_JSON_INODE,
        source_generation: SESSION_JSON_GENERATION,
        last_offset: 0,
        last_line_no: 0,
        status: CheckpointLifecycle::Active.to_string(),
        ..Default::default()
    });

    // Re-pin the synthetic identity on every run — older checkpoints written
    // before this code path existed may carry real inode/generation values.
    checkpoint.source_inode = SESSION_JSON_INODE;
    checkpoint.source_generation = SESSION_JSON_GENERATION;

    let already_emitted = checkpoint.last_line_no;
    if message_count < already_emitted {
        // Hermes's writer guards against this ("never overwrite a larger
        // session log with fewer messages"), so we treat it as a spurious
        // read. Don't rewind — leave the checkpoint alone.
        debug!(
            source_file,
            current = message_count,
            last_emitted = already_emitted,
            "session_json shrank; ignoring",
        );
        return Ok(());
    }

    // On the very first run for a session file, also emit the session_meta
    // pseudo-record so downstream consumers see harness/model/platform up front.
    let mut synthetic_records: Vec<(u64, Value)> = Vec::new();
    if already_emitted == 0 {
        synthetic_records.push((0, build_session_meta_record(&session_doc)));
    }
    for idx in already_emitted..message_count {
        let msg = &messages[idx as usize];
        synthetic_records.push((
            idx + 1,
            build_session_message_record(&session_doc, msg, idx),
        ));
    }

    if synthetic_records.is_empty() && file_size == checkpoint.last_offset {
        return Ok(());
    }

    let mut batch = RowBatch::default();
    // Only messages after `already_emitted` are synthesized on a resume, so a
    // chained identity would start from a different record than a full pass.
    // Every session-doc record carries its own session, so nothing falls back
    // to this and it stays empty for the whole file.
    let session_identity = String::new();
    let mut model_hint = String::new();
    let mut cwd_hint = String::new();

    for (line_no, record) in synthetic_records {
        let raw_json = serde_json::to_string(&record).unwrap_or_else(|_| "{}".to_string());
        match normalize_record(
            &record,
            &work.source_name,
            &work.harness,
            source_file,
            SESSION_JSON_INODE,
            SESSION_JSON_GENERATION,
            line_no,
            0,
            &session_identity,
            &model_hint,
            &cwd_hint,
        ) {
            Ok(normalized) => {
                model_hint = normalized.model_hint;
                cwd_hint = normalized.cwd_hint;
                // A null `raw_row` is the normalizer's "skip this record"
                // signal (e.g. Kimi wire metadata header). Still count the
                // line for checkpointing, but don't emit a null row — it
                // would poison the JSONEachRow batch at flush time.
                if !normalized.raw_row.is_null() {
                    batch.push_raw_row(normalized.raw_row);
                }
                batch.extend_event_rows(normalized.event_rows);
                batch.extend_link_rows(normalized.link_rows);
                batch.extend_tool_rows(normalized.tool_rows);
                batch.extend_error_rows(normalized.error_rows);
                batch.lines_processed = batch.lines_processed.saturating_add(1);
            }
            Err(exc) => {
                batch.push_error_row(json!({
                    "source_name": work.source_name,
                    "harness": work.harness,
                    "source_file": source_file,
                    "source_inode": SESSION_JSON_INODE,
                    "source_generation": SESSION_JSON_GENERATION,
                    "source_line_no": line_no,
                    "source_offset": 0u64,
                    "error_kind": "normalize_error",
                    "error_text": exc.to_string(),
                    "raw_fragment": truncate(&raw_json, 20_000),
                }));
            }
        }

        if batch.exceeds_limits(config.ingest.batch_size, config.ingest.max_batch_bytes) {
            let chunk = batch.drain_to_chunk();
            sink_tx
                .send(SinkMessage::Batch(chunk))
                .await
                .context("sink channel closed while sending session_json chunk")?;
        }
    }

    let final_checkpoint = Checkpoint {
        source_name: work.source_name.clone(),
        source_file: source_file.to_string(),
        source_inode: SESSION_JSON_INODE,
        source_generation: SESSION_JSON_GENERATION,
        last_offset: file_size,
        last_line_no: message_count,
        status: CheckpointLifecycle::Active.to_string(),
        ..Default::default()
    };

    if batch.row_count() > 0
        || message_count != already_emitted
        || file_size != checkpoint.last_offset
    {
        batch.checkpoint = Some(final_checkpoint);
        sink_tx
            .send(SinkMessage::Batch(batch))
            .await
            .context("sink channel closed while sending final session_json batch")?;
    }

    if metrics.queue_depth.load(Ordering::Relaxed) == 0 {
        debug!(
            "{}:{} session_json caught up at message_count={}",
            work.source_name, source_file, message_count
        );
    }

    Ok(())
}

fn build_session_meta_record(session_doc: &Value) -> Value {
    let session_id = session_doc
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let base_url = session_doc
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let model = compose_hermes_model(
        session_doc
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        &base_url,
    );
    let platform = session_doc
        .get("platform")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let session_start = session_doc
        .get("session_start")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let last_updated = session_doc
        .get("last_updated")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let system_prompt = session_doc
        .get("system_prompt")
        .cloned()
        .unwrap_or(Value::Null);
    let tools = session_doc.get("tools").cloned().unwrap_or(Value::Null);
    let message_count = session_doc
        .get("message_count")
        .cloned()
        .unwrap_or(Value::Null);

    // `timestamp` is expected top-level by normalize_record for event_ts
    // derivation. We prefer the session start; callers can always fall back to
    // `record_ts` on the raw row if needed.
    let timestamp = if !session_start.is_empty() {
        session_start.clone()
    } else {
        last_updated.clone()
    };

    json!({
        "type": "session_meta",
        "timestamp": timestamp,
        "session_id": session_id,
        "model": model,
        "base_url": base_url,
        "platform": platform,
        "session_start": session_start,
        "last_updated": last_updated,
        "system_prompt": system_prompt,
        "tools": tools,
        "message_count": message_count,
    })
}

fn build_session_message_record(session_doc: &Value, message: &Value, message_index: u64) -> Value {
    let session_id = session_doc
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let base_url = session_doc
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let model = compose_hermes_model(
        session_doc
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        &base_url,
    );
    let platform = session_doc
        .get("platform")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let last_updated = session_doc
        .get("last_updated")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let session_start = session_doc
        .get("session_start")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let timestamp = if !last_updated.is_empty() {
        last_updated.clone()
    } else {
        session_start.clone()
    };

    json!({
        "type": "session_message",
        "timestamp": timestamp,
        "session_id": session_id,
        "model": model,
        "base_url": base_url,
        "platform": platform,
        "message_index": message_index,
        "message": message,
    })
}

pub(crate) fn source_inode_for_file(source_file: &str, meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        let _ = source_file;
        meta.ino()
    }

    #[cfg(not(unix))]
    {
        non_unix_source_inode(source_file, meta)
    }
}

#[cfg(not(unix))]
fn non_unix_source_inode(source_file: &str, meta: &std::fs::Metadata) -> u64 {
    if let Ok(handle) = Handle::from_path(source_file) {
        let id = hash_identity(&handle);
        if id != 0 {
            return id;
        }
    }

    // Fallback when a platform file handle identity is unavailable.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    source_file.hash(&mut hasher);
    if let Ok(created_at) = meta.created() {
        if let Ok(since_epoch) = created_at.duration_since(UNIX_EPOCH) {
            since_epoch.as_nanos().hash(&mut hasher);
        }
    }

    let id = hasher.finish();
    if id == 0 {
        1
    } else {
        id
    }
}

#[cfg(not(unix))]
fn hash_identity(value: &impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn truncate(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    input.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        complete_work, compose_hermes_model, enqueue_work, enrich_claude_model_latency,
        jsonl_policy_fingerprint, jsonl_source_line_byte_limit, process_file,
        process_session_json_file, run_work_item, source_inode_for_file, spawn_debounce_task,
        work_item_is_ingestable, work_path_is_canonical, SessionCursor,
        CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT, ERROR_KIND_NORMALIZED_ROW_TOO_LARGE,
        ERROR_KIND_SOURCE_LINE_TOO_LARGE, JSONL_PUBLICATION_PROTOCOL_VERSION,
        SESSION_JSON_GENERATION, SESSION_JSON_INODE, SOURCE_NORMALIZATION_RULES_VERSION,
    };
    use crate::model::{Checkpoint, CheckpointLifecycle};
    use crate::sqlite_poll::VolatilePollMap;
    use crate::{DispatchState, Metrics, SinkMessage, WorkItem, WorkTrigger};
    use moraine_config::SourceFormat;
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::fs;
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::{mpsc, RwLock, Semaphore};
    use tokio::time::timeout;

    fn sample_work(path: &str) -> WorkItem {
        WorkItem {
            source_name: "test-source".to_string(),
            harness: "test-harness".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string(),
            trigger: WorkTrigger::Watcher,
        }
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("moraine-dispatch-{name}-{suffix}.jsonl"))
    }

    #[test]
    fn complete_work_prunes_idle_item() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let work = sample_work("/tmp/idle.jsonl");
        let key = work.key();

        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.inflight.insert(key.clone());
            state.item_by_key.insert(key.clone(), work);
        }

        let reschedule = complete_work(&key, &dispatch);
        assert!(reschedule.is_none());

        let state = dispatch.lock().expect("dispatch mutex poisoned");
        assert!(!state.inflight.contains(&key));
        assert!(!state.pending.contains(&key));
        assert!(!state.dirty.contains(&key));
        assert!(!state.item_by_key.contains_key(&key));
    }

    #[test]
    fn complete_work_reschedules_dirty_item() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let work = sample_work("/tmp/dirty.jsonl");
        let key = work.key();

        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.inflight.insert(key.clone());
            state.dirty.insert(key.clone());
            state.item_by_key.insert(key.clone(), work.clone());
        }

        let reschedule = complete_work(&key, &dispatch);
        assert_eq!(
            reschedule.as_ref().map(|item| item.path.as_str()),
            Some(work.path.as_str())
        );

        let state = dispatch.lock().expect("dispatch mutex poisoned");
        assert!(!state.inflight.contains(&key));
        assert!(!state.dirty.contains(&key));
        assert!(state.pending.contains(&key));
        assert!(state.item_by_key.contains_key(&key));
    }

    #[test]
    fn complete_work_keeps_item_when_still_pending() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let work = sample_work("/tmp/pending.jsonl");
        let key = work.key();

        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.insert(key.clone());
            state.item_by_key.insert(key.clone(), work);
        }

        let reschedule = complete_work(&key, &dispatch);
        assert!(reschedule.is_none());

        let state = dispatch.lock().expect("dispatch mutex poisoned");
        assert!(state.pending.contains(&key));
        assert!(state.item_by_key.contains_key(&key));
    }

    #[test]
    fn source_inode_is_stable_for_same_file() {
        let path = unique_test_file("identity-stable");
        fs::write(&path, "{\"line\":1}\n").expect("write initial file");
        let source_file = path.to_string_lossy().to_string();

        let first_meta = fs::metadata(&path).expect("metadata for initial file");
        let first_id = source_inode_for_file(&source_file, &first_meta);
        assert_ne!(first_id, 0);

        fs::write(&path, "{\"line\":1}\n{\"line\":2}\n").expect("append file content");
        let second_meta = fs::metadata(&path).expect("metadata after append");
        let second_id = source_inode_for_file(&source_file, &second_meta);

        let _ = fs::remove_file(&path);
        assert_eq!(first_id, second_id);
    }

    #[test]
    fn source_inode_changes_when_file_is_replaced() {
        let path = unique_test_file("identity-replaced");
        let replacement = unique_test_file("identity-replacement");
        fs::write(&path, "{\"line\":1}\n").expect("write original file");
        let source_file = path.to_string_lossy().to_string();

        let original_meta = fs::metadata(&path).expect("metadata for original file");
        let original_id = source_inode_for_file(&source_file, &original_meta);
        assert_ne!(original_id, 0);

        fs::write(&replacement, "{\"line\":99}\n").expect("write replacement file");
        fs::rename(&replacement, &path).expect("replace file via rename");

        let replaced_meta = fs::metadata(&path).expect("metadata for replaced file");
        let replaced_id = source_inode_for_file(&source_file, &replaced_meta);

        let _ = fs::remove_file(&path);
        assert_ne!(original_id, replaced_id);
    }

    #[test]
    fn captured_source_boundary_accepts_growth_but_rejects_shrink() {
        let path = unique_test_file("scan-boundary");
        fs::write(&path, "one\n").expect("write captured source");
        let source_file = path.to_string_lossy().to_string();
        let metadata = fs::metadata(&path).expect("captured metadata");
        let inode = source_inode_for_file(&source_file, &metadata);
        let boundary = metadata.len();

        fs::write(&path, "one\ntwo\n").expect("grow source");
        super::source_scan_still_valid(&source_file, inode, boundary)
            .expect("growth beyond a captured boundary is later append work");

        fs::write(&path, "x").expect("shrink source");
        let error = super::source_scan_still_valid(&source_file, inode, boundary)
            .expect_err("shrink invalidates the replay scan");
        assert!(error.to_string().contains("shrank"));

        let _ = fs::remove_file(&path);
    }

    /// Own thread of the sub-agent rollout below; also the id in its filename.
    const CODEX_OWN_THREAD: &str = "019f81a9-8226-7c71-a7de-5a0992207ab6";
    /// Thread that spawned it, whose `session_meta` Codex replays into the
    /// child rollout as part of the forked context.
    const CODEX_PARENT_THREAD: &str = "019f7fe1-3b94-7fa2-856c-79946cb89dd2";

    /// Real Codex sub-agent rollout shape: the file's own header, then the
    /// PARENT thread's replayed header, then ordinary records, then the parent
    /// header again further down.
    fn codex_subagent_rollout_lines() -> Vec<String> {
        let own_header = json!({
            "type": "session_meta",
            "timestamp": "2026-07-20T18:33:17.019Z",
            "payload": {
                "id": CODEX_OWN_THREAD,
                "session_id": CODEX_PARENT_THREAD,
                "forked_from_id": CODEX_PARENT_THREAD,
                "parent_thread_id": CODEX_PARENT_THREAD,
                "thread_source": "subagent",
                "agent_nickname": "Beauvoir",
                "cwd": "/repo",
            }
        });
        let replayed_parent_header = json!({
            "type": "session_meta",
            "timestamp": "2026-07-20T18:33:18.019Z",
            "payload": {
                "id": CODEX_PARENT_THREAD,
                "session_id": CODEX_PARENT_THREAD,
                "thread_source": "user",
                "cwd": "/repo",
            }
        });
        let message = |seq: u64| {
            json!({
                "type": "event_msg",
                "timestamp": format!("2026-07-20T18:33:{:02}.019Z", 20 + seq),
                "payload": {"type": "agent_message", "message": format!("delegated step {seq}")}
            })
        };

        [
            own_header,
            replayed_parent_header.clone(),
            message(1),
            message(2),
            replayed_parent_header,
            message(3),
        ]
        .iter()
        .map(|record| record.to_string() + "\n")
        .collect()
    }

    fn unique_rollout_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("moraine-codex-{name}-{suffix}"));
        fs::create_dir_all(&dir).expect("create rollout directory");
        dir.join(format!(
            "rollout-2026-07-20T18-33-17-{CODEX_OWN_THREAD}.jsonl"
        ))
    }

    /// One `process_file` pass. Returns the session each source line was
    /// attributed to, the sub-agent lineage links emitted, and the durable
    /// checkpoint a following pass would resume from.
    async fn codex_attribution_pass(
        path: &Path,
        resume_from: Option<Checkpoint>,
    ) -> (
        std::collections::BTreeMap<u64, String>,
        Vec<Value>,
        Option<Checkpoint>,
    ) {
        let config = moraine_config::AppConfig::default();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        if let Some(resume_from) = resume_from {
            checkpoints.write().await.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
                resume_from,
            );
        }
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(64);

        let (result, messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                checkpoints,
                &VolatilePollMap::new(),
                sink_tx,
                &metrics,
            ),
            &mut sink_rx,
        )
        .await;
        result.expect("codex rollout ingests");

        let mut by_line = std::collections::BTreeMap::new();
        let mut links = Vec::new();
        let mut checkpoint = None;
        for batch in observed_batches(&messages) {
            for row in &batch.event_rows {
                let line = row
                    .get("source_line_no")
                    .and_then(Value::as_u64)
                    .expect("event carries its source line");
                let session = row
                    .get("session_id")
                    .and_then(Value::as_str)
                    .expect("event carries a session")
                    .to_string();
                by_line.insert(line, session);
            }
            links.extend(batch.link_rows.iter().cloned());
            if let Some(batch_checkpoint) = batch.checkpoint.clone() {
                checkpoint = Some(batch_checkpoint);
            }
        }

        (by_line, links, checkpoint)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn codex_subagent_rollout_attribution_is_independent_of_where_the_pass_started() {
        let lines = codex_subagent_rollout_lines();

        let whole = unique_rollout_path("whole");
        fs::write(&whole, lines.concat()).expect("write whole rollout");
        let (fresh, fresh_links, _) = codex_attribution_pass(&whole, None).await;

        // Same bytes, but consumed as a head pass plus an append pass — the
        // shape that produced one source line under two session ids.
        let appended = unique_rollout_path("appended");
        fs::write(&appended, lines[..2].concat()).expect("write rollout head");
        let (head, head_links, checkpoint) = codex_attribution_pass(&appended, None).await;
        let checkpoint = checkpoint.expect("head pass persists a checkpoint");
        assert!(
            checkpoint.last_offset > 0,
            "the append must resume mid-file"
        );
        fs::write(&appended, lines.concat()).expect("append the rest of the rollout");
        let (tail, tail_links, _) = codex_attribution_pass(&appended, Some(checkpoint)).await;

        let mut resumed = head;
        resumed.extend(tail);
        assert_eq!(
            fresh, resumed,
            "a line must resolve to the same session whether the pass started above it or at it"
        );
        assert_eq!(fresh.len(), lines.len(), "every line is attributed");
        for (line, session) in &fresh {
            assert_eq!(
                session, CODEX_OWN_THREAD,
                "line {line} must stay on the rollout's own thread"
            );
        }

        let mut resumed_links = head_links;
        resumed_links.extend(tail_links);
        for links in [&fresh_links, &resumed_links] {
            let lineage: Vec<&Value> = links
                .iter()
                .filter(|link| {
                    link.get("link_type").and_then(Value::as_str) == Some("subagent_parent")
                })
                .collect();
            assert_eq!(lineage.len(), 1, "one lineage edge per sub-agent rollout");
            assert_eq!(
                lineage[0].get("linked_external_id").and_then(Value::as_str),
                Some(CODEX_PARENT_THREAD),
                "the parent thread is preserved as a link, not as the session"
            );
            assert_eq!(
                lineage[0].get("session_id").and_then(Value::as_str),
                Some(CODEX_OWN_THREAD)
            );
        }

        let _ = fs::remove_file(&whole);
        let _ = fs::remove_file(&appended);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jsonl_rotation_is_bracketed_by_durable_replay_barriers() {
        let path = unique_test_file("rotation-publication");
        let replacement = unique_test_file("rotation-publication-next");
        let record = |uuid: &str, content: &str| {
            json!({
                "type": "user",
                "timestamp": "2026-04-18T20:43:51.069Z",
                "uuid": uuid,
                "sessionId": "rotation-session",
                "cwd": "/repo",
                "message": {"role": "user", "content": content}
            })
            .to_string()
                + "\n"
        };
        fs::write(&path, record("old", "old generation")).expect("write initial source");

        let config = moraine_config::AppConfig::default();
        let work = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("initial generation ingests");
        let initial = drain_batches(&mut sink_rx).await;
        let initial_checkpoint = initial[0].checkpoint.clone().expect("initial checkpoint");
        assert_eq!(initial_checkpoint.source_generation, 1);
        assert!(!initial_checkpoint.policy_fingerprint.is_empty());
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            initial_checkpoint,
        );

        fs::write(&replacement, record("new", "new generation")).expect("write replacement source");
        fs::rename(&replacement, &path).expect("rotate source atomically");

        let (result, messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                checkpoints,
                &VolatilePollMap::new(),
                sink_tx,
                &metrics,
            ),
            &mut sink_rx,
        )
        .await;
        result.expect("replacement generation ingests");

        assert_eq!(messages.len(), 3, "begin, replay batch, final publication");
        let ObservedSinkMessage::Begin(begin) = &messages[0] else {
            panic!("replacement rows must be preceded by BeginReplay");
        };
        assert_eq!(begin.checkpoint.source_generation, 2);
        assert_eq!(
            begin.checkpoint.lifecycle().unwrap(),
            CheckpointLifecycle::Replaying
        );
        assert!(!begin.checkpoint.final_scan_complete);

        let ObservedSinkMessage::Batch(batch) = &messages[1] else {
            panic!("replacement payload must follow BeginReplay");
        };
        let replay_checkpoint = batch.checkpoint.as_ref().expect("replay checkpoint");
        assert_eq!(replay_checkpoint.source_generation, 2);
        assert_eq!(
            replay_checkpoint.lifecycle().unwrap(),
            CheckpointLifecycle::Replaying
        );

        let ObservedSinkMessage::Finalize(finalize) = &messages[2] else {
            panic!("replacement payload must end with FinalizeReplay");
        };
        assert_eq!(finalize.checkpoint.source_generation, 2);
        assert_eq!(
            finalize.checkpoint.lifecycle().unwrap(),
            CheckpointLifecycle::Active
        );
        assert!(finalize.checkpoint.final_scan_complete);
        assert_eq!(
            finalize.checkpoint.scan_boundary,
            finalize.checkpoint.last_offset
        );

        // Simulate a crash after the replay payload checkpoint became durable
        // but before the publication acknowledgement. Restart must finalize
        // the same generation, not allocate generation 3 or silently return at
        // EOF.
        let resumed = Arc::new(RwLock::new(HashMap::from([(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            replay_checkpoint.clone(),
        )])));
        let (resume_tx, mut resume_rx) = mpsc::channel::<SinkMessage>(8);
        let (result, resumed_messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                resumed,
                &VolatilePollMap::new(),
                resume_tx,
                &metrics,
            ),
            &mut resume_rx,
        )
        .await;
        result.expect("restart finalizes the durable replay generation");
        assert!(matches!(
            resumed_messages.first(),
            Some(ObservedSinkMessage::Begin(_))
        ));
        let resumed_final = resumed_messages
            .iter()
            .find_map(|message| match message {
                ObservedSinkMessage::Finalize(transition) => Some(transition),
                _ => None,
            })
            .expect("resumed replay finalization");
        assert_eq!(resumed_final.checkpoint.source_generation, 2);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jsonl_replacement_quarantine_blocks_instead_of_publishing() {
        let path = unique_test_file("rotation-quarantine");
        let replacement = unique_test_file("rotation-quarantine-next");
        let record = |uuid: &str| {
            json!({
                "type": "user",
                "timestamp": "2026-04-18T20:43:51.069Z",
                "uuid": uuid,
                "sessionId": "rotation-session",
                "cwd": "/repo",
                "message": {"role": "user", "content": uuid}
            })
            .to_string()
                + "\n"
        };
        fs::write(&path, record("old")).expect("write initial source");

        let config = moraine_config::AppConfig::default();
        let work = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("initial generation ingests");
        let initial = drain_batches(&mut sink_rx).await;
        let initial_checkpoint = initial[0].checkpoint.clone().expect("initial checkpoint");
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            initial_checkpoint,
        );

        fs::write(&replacement, record("new") + "{malformed\n")
            .expect("write quarantined replacement");
        fs::rename(&replacement, &path).expect("rotate source atomically");

        let (result, messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                checkpoints,
                &VolatilePollMap::new(),
                sink_tx,
                &metrics,
            ),
            &mut sink_rx,
        )
        .await;
        result.expect("quarantined replacement is durably blocked");

        assert!(matches!(
            messages.first(),
            Some(ObservedSinkMessage::Begin(_))
        ));
        assert!(messages
            .iter()
            .any(|message| matches!(message, ObservedSinkMessage::Block(_))));
        assert!(!messages
            .iter()
            .any(|message| matches!(message, ObservedSinkMessage::Finalize(_))));
        let batch = observed_batches(&messages)
            .into_iter()
            .next()
            .expect("candidate replay batch");
        assert_eq!(
            batch
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.lifecycle().unwrap()),
            Some(CheckpointLifecycle::Replaying)
        );
        assert_eq!(batch.error_rows.len(), 1);

        let blocked_checkpoint = messages
            .iter()
            .find_map(|message| match message {
                ObservedSinkMessage::Block(transition) => Some(transition.checkpoint.clone()),
                _ => None,
            })
            .expect("durable blocked replacement checkpoint");
        assert_eq!(
            blocked_checkpoint.lifecycle().unwrap(),
            CheckpointLifecycle::Error
        );
        assert_eq!(
            blocked_checkpoint.last_offset,
            fs::metadata(&path).expect("replacement metadata").len(),
            "the durable error reproduces the terminal-cursor restart hazard"
        );

        // A later poll or process restart sees the durable error checkpoint.
        // It must rescan the whole candidate, rediscover the malformed row,
        // and remain blocked rather than treating terminal EOF as success.
        let resumed = Arc::new(RwLock::new(HashMap::from([(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            blocked_checkpoint,
        )])));
        let (retry_tx, mut retry_rx) = mpsc::channel::<SinkMessage>(16);
        let (result, retry_messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                resumed,
                &VolatilePollMap::new(),
                retry_tx,
                &metrics,
            ),
            &mut retry_rx,
        )
        .await;
        result.expect("unchanged malformed replacement remains durably blocked");
        assert!(matches!(
            retry_messages.first(),
            Some(ObservedSinkMessage::Begin(_))
        ));
        assert!(retry_messages
            .iter()
            .any(|message| matches!(message, ObservedSinkMessage::Block(_))));
        assert!(!retry_messages
            .iter()
            .any(|message| matches!(message, ObservedSinkMessage::Finalize(_))));
        let retried_batch = observed_batches(&retry_messages)
            .into_iter()
            .next()
            .expect("retried candidate replay batch");
        assert_eq!(retried_batch.error_rows.len(), 1);

        let _ = fs::remove_file(&path);
    }

    /// The policy fingerprint must carry the normalization-rules version.
    ///
    /// A fingerprint change is what turns an adapter rule change into a
    /// whole-source replacement replay; without this input, altering what a
    /// source line normalizes to leaves the old rows live alongside the new
    /// interpretation forever. Dropping the field from the payload is the
    /// regression this pins.
    #[test]
    fn policy_fingerprints_carry_the_normalization_rules_version() {
        let config = moraine_config::AppConfig::default();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: "/tmp/rollout.jsonl".to_string(),
            trigger: WorkTrigger::Watcher,
        };

        let mut exclusions = config.ingest.exclude_project_dirs.clone();
        exclusions.sort();
        let with_rules = serde_json::to_vec(&json!({
            "protocol": JSONL_PUBLICATION_PROTOCOL_VERSION,
            "normalization_rules": SOURCE_NORMALIZATION_RULES_VERSION,
            "source_format": work.format.to_string(),
            "harness": work.harness,
            "project_exclusions": exclusions.clone(),
        }))
        .expect("policy payload serializes");
        let expected: String = Sha256::digest(with_rules)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(
            jsonl_policy_fingerprint(&config, &work),
            expected,
            "the JSONL fingerprint must hash the normalization-rules version"
        );

        // A different rules version must be a different fingerprint, or a
        // future bump would not trigger the replacement replay it exists for.
        let without_rules = serde_json::to_vec(&json!({
            "protocol": JSONL_PUBLICATION_PROTOCOL_VERSION,
            "source_format": work.format.to_string(),
            "harness": work.harness,
            "project_exclusions": exclusions,
        }))
        .expect("policy payload serializes");
        let legacy: String = Sha256::digest(without_rules)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_ne!(
            jsonl_policy_fingerprint(&config, &work),
            legacy,
            "a corpus normalized under the previous rules must not fingerprint equal"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_work_item_releases_permit_before_reschedule_send() {
        let path = unique_test_file("reschedule-no-deadlock");
        fs::write(&path, "").expect("write empty jsonl");
        let work = WorkItem {
            source_name: "test-source".to_string(),
            harness: "test-harness".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        };
        let key = work.key();

        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.inflight.insert(key.clone());
            state.dirty.insert(key.clone());
            state.item_by_key.insert(key.clone(), work.clone());
        }

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());

        let (sink_tx, _sink_rx) = mpsc::channel::<SinkMessage>(8);
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(1);
        process_tx
            .send(work.clone())
            .await
            .expect("prime process_tx so reschedule send will block");

        let sem = Arc::new(Semaphore::new(1));
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("acquire initial permit");

        let task = tokio::spawn(run_work_item(
            config,
            work,
            permit,
            checkpoints,
            VolatilePollMap::new(),
            sink_tx,
            process_tx,
            dispatch,
            metrics,
        ));

        let released = timeout(Duration::from_millis(500), sem.acquire()).await;
        assert!(
            released.is_ok(),
            "permit must be released before the reschedule `send` blocks on a full channel"
        );

        process_rx.recv().await.expect("priming item");

        let rescheduled = timeout(Duration::from_millis(500), process_rx.recv())
            .await
            .expect("rescheduled send should complete once channel drains")
            .expect("rescheduled work item delivered");
        assert_eq!(rescheduled.key(), key);

        task.await.expect("run_work_item task should finish");

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn work_path_canonical_check_matches_format() {
        let jsonl = WorkItem {
            source_name: "s".to_string(),
            harness: "hermes".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: "/tmp/x.jsonl".to_string(),
            trigger: WorkTrigger::Watcher,
        };
        assert!(work_path_is_canonical(&jsonl));

        let session = WorkItem {
            source_name: "s".to_string(),
            harness: "hermes".to_string(),
            format: SourceFormat::SessionJson,
            source_glob: String::new(),
            path: "/tmp/session_x.json".to_string(),
            trigger: WorkTrigger::Watcher,
        };
        assert!(work_path_is_canonical(&session));
        // session_json format must NOT pick up .jsonl files
        let wrong = WorkItem {
            path: "/tmp/x.jsonl".to_string(),
            trigger: WorkTrigger::Watcher,
            ..session.clone()
        };
        assert!(!work_path_is_canonical(&wrong));

        let sqlite = WorkItem {
            source_name: "s".to_string(),
            harness: "cursor".to_string(),
            format: SourceFormat::CursorSqlite,
            source_glob: String::new(),
            path: "/tmp/User/state.vscdb".to_string(),
            trigger: WorkTrigger::Watcher,
        };
        assert!(work_path_is_canonical(&sqlite));
        // Sidecars are canonicalized upstream; a sidecar path reaching the
        // dispatcher directly is dropped rather than processed.
        let sidecar = WorkItem {
            path: "/tmp/User/state.vscdb-wal".to_string(),
            trigger: WorkTrigger::Watcher,
            ..sqlite.clone()
        };
        assert!(!work_path_is_canonical(&sidecar));
    }

    #[tokio::test]
    async fn process_file_rejects_unresolved_infer_format() {
        let config = moraine_config::AppConfig::default();
        let work = WorkItem {
            source_name: "unresolved".to_string(),
            harness: "hermes".to_string(),
            format: SourceFormat::Infer,
            source_glob: String::new(),
            path: "/tmp/unresolved.jsonl".to_string(),
            trigger: WorkTrigger::Watcher,
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, _sink_rx) = mpsc::channel(1);

        let error = process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect_err("Infer must not select an ingest processor");

        assert!(
            error.to_string().contains("must be normalized"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn workflow_journals_are_not_ingestable_but_sessions_and_subagents_are() {
        let claude = |path: &str| WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string(),
            trigger: WorkTrigger::Watcher,
        };
        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";

        // The orphan workflow journal is rejected even though it is the
        // canonical path for the jsonl format (issue #386).
        let journal = claude(&format!(
            "{proj}/{sid}/subagents/workflows/wf_12dc2994-7e9/journal.jsonl"
        ));
        assert!(work_path_is_canonical(&journal));
        assert!(!work_item_is_ingestable(&journal));

        // Real sessions and both kinds of subagent transcripts stay ingestible.
        assert!(work_item_is_ingestable(&claude(&format!(
            "{proj}/{sid}.jsonl"
        ))));
        assert!(work_item_is_ingestable(&claude(&format!(
            "{proj}/{sid}/subagents/workflows/wf_8dc1b543-8da/agent-a38ca143465605620.jsonl"
        ))));
        assert!(work_item_is_ingestable(&claude(&format!(
            "{proj}/{sid}/subagents/agent-a5a524a7f876aa747.jsonl"
        ))));

        // The exclusion is scoped to claude-code: the same path under another
        // harness/source must not be silently dropped.
        let codex_journal = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}/subagents/workflows/wf_x/journal.jsonl"),
            trigger: WorkTrigger::Watcher,
        };
        assert!(work_item_is_ingestable(&codex_journal));
    }

    #[test]
    fn cowork_gate_accepts_transcripts_and_rejects_audit_paths() {
        let root = "/Users/test/Library/Application Support/Claude/local-agent-mode-sessions/account/workspace/local_11111111-2222-4333-8444-555555555555";
        let cowork = |path: String| WorkItem {
            source_name: "claude-cowork".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path,
            trigger: WorkTrigger::Watcher,
        };
        assert!(work_item_is_ingestable(&cowork(format!(
            "{root}/.claude/projects/-sessions-demo/aaaaaaaa-1111-4333-8444-555555555555.jsonl"
        ))));
        assert!(!work_item_is_ingestable(&cowork(format!(
            "{root}/audit.jsonl"
        ))));
        assert!(!work_item_is_ingestable(&cowork(format!(
            "{root}/unrelated.jsonl"
        ))));
    }

    /// End-to-end through the dispatch gate: a workflow journal enqueued from
    /// any entry point (backfill/reconcile/watcher all call `enqueue_work`)
    /// must never reach the processor channel or the dispatch state, while a
    /// real session transcript does. This is the behavior that keeps the
    /// empty-`session_id` junk out of ClickHouse.
    #[tokio::test(flavor = "multi_thread")]
    async fn enqueue_work_drops_workflow_journals_before_processing() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let journal = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}/subagents/workflows/wf_12dc2994-7e9/journal.jsonl"),
            trigger: WorkTrigger::Watcher,
        };

        enqueue_work(journal.clone(), &process_tx, &dispatch, &metrics).await;

        assert!(
            process_rx.try_recv().is_err(),
            "workflow journal must not be forwarded to the processor"
        );
        {
            let state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(state.pending.is_empty(), "no pending work for a journal");
            assert!(
                !state.item_by_key.contains_key(&journal.key()),
                "journal must not be tracked in dispatch state"
            );
        }
        assert_eq!(metrics.queue_depth.load(Ordering::Relaxed), 0);

        // A real session transcript from the same source is forwarded.
        let session = WorkItem {
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
            ..journal.clone()
        };
        enqueue_work(session.clone(), &process_tx, &dispatch, &metrics).await;
        let forwarded = process_rx
            .try_recv()
            .expect("real session transcript must be forwarded");
        assert_eq!(forwarded.key(), session.key());
        assert_eq!(metrics.queue_depth.load(Ordering::Relaxed), 1);
    }

    /// Issue #601 §2.4 / WI-03. Trigger provenance exists so a reconciliation
    /// sweep can be attached to reconcile ticks only; every merge point must
    /// therefore be able to *downgrade* a trigger and never upgrade one.
    #[test]
    fn trigger_merge_keeps_the_least_reconciliation_eligible_trigger() {
        use WorkTrigger::{Reconcile, Startup, Watcher};
        assert_eq!(Watcher.merge(Reconcile), Watcher);
        assert_eq!(Reconcile.merge(Watcher), Watcher);
        assert_eq!(Startup.merge(Reconcile), Startup);
        assert_eq!(Reconcile.merge(Startup), Startup);
        assert_eq!(Watcher.merge(Startup), Watcher);
        assert_eq!(Reconcile.merge(Reconcile), Reconcile);
    }

    /// `work.key()` must not carry the trigger: if it did, a watcher event and
    /// a reconcile tick for the same file would occupy two queue slots and the
    /// debounce would stop coalescing anything.
    ///
    /// Fails for: adding the trigger to `WorkItem::key`.
    #[test]
    fn work_key_excludes_the_trigger() {
        let watcher = sample_work("/tmp/trigger-key.jsonl");
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        assert_eq!(
            watcher.key(),
            reconcile.key(),
            "the trigger is not part of the coalescing identity"
        );
    }

    /// A reconcile tick that lands while a watcher poll is inflight must not
    /// upgrade that poll — a burst must never retroactively become expensive —
    /// but it must not be *lost* either. `item_by_key` holds a key while it is
    /// pending, inflight or dirty, so on a continuously written database almost
    /// every reconcile tick lands on an occupied key; discarding them starves
    /// the sweep on exactly the source with the most history to sweep.
    ///
    /// So the inflight item stays `Watcher`, and the *next* poll — the dirty
    /// re-enqueue, which has not started yet — carries the owed tick.
    ///
    /// Fails for: dropping `reconcile_owed` (the re-enqueue comes back as
    /// `Watcher` and the tick is gone), or arming it on the inflight item.
    #[tokio::test]
    async fn a_reconcile_tick_landing_on_an_inflight_poll_survives_to_the_next_one() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let watcher = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
        };
        let key = watcher.key();

        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        let dispatched = process_rx.try_recv().expect("watcher item is forwarded");
        assert_eq!(dispatched.trigger, WorkTrigger::Watcher);
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }

        // A reconcile tick arrives mid-poll and only marks the item dirty.
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        enqueue_work(reconcile, &process_tx, &dispatch, &metrics).await;
        assert!(
            process_rx.try_recv().is_err(),
            "an inflight key is marked dirty, not re-sent"
        );
        assert_eq!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .item_by_key
                .get(&key)
                .map(|item| item.trigger),
            Some(WorkTrigger::Watcher),
            "the inflight poll is never upgraded mid-flight"
        );

        let rescheduled = complete_work(&key, &dispatch).expect("dirty item is re-enqueued");
        assert_eq!(
            rescheduled.trigger,
            WorkTrigger::Reconcile,
            "the swallowed reconcile tick rides the next poll instead of being lost"
        );

        // And it is drained on use: the tick buys one reconcile-eligible poll,
        // not a permanent upgrade of the key.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(state.reconcile_owed.is_empty());
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
            state.dirty.insert(key.clone());
        }
        let next = complete_work(&key, &dispatch).expect("second dirty re-enqueue");
        assert_eq!(
            next.trigger,
            WorkTrigger::Reconcile,
            "item_by_key carries the armed trigger forward; nothing re-arms it"
        );
        assert!(dispatch
            .lock()
            .expect("dispatch mutex poisoned")
            .reconcile_owed
            .is_empty());
    }

    /// Issue #601 §0/§4 — the **lower** bound on `reconcile_owed`. The sweep is
    /// the only mechanism guaranteeing coverage, and §4 promises a *finite*
    /// maximum complete-sweep interval. A key under continuous watcher churn is
    /// pending, inflight or dirty essentially all the time, so if every
    /// reconcile tick that lands there is discarded the interval on the busiest
    /// database is unbounded.
    ///
    /// Drive 20 churn cycles with one reconcile tick each and assert every tick
    /// produces exactly one reconcile-eligible poll, within one cycle of
    /// arriving.
    ///
    /// **This test cannot see an over-owing bug**, and saying so is the point:
    /// it feeds one tick per cycle, so a correct ledger and one that latches
    /// permanently sweep-eligible both produce 20. The upper bound is
    /// `one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll`, and
    /// neither guard is sufficient alone.
    ///
    /// Fails for: discarding the merged-away tick (zero reconcile-eligible
    /// polls in 20 cycles).
    #[tokio::test]
    async fn continuous_watcher_churn_still_yields_reconcile_eligible_polls() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(64);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let watcher = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
        };
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        let key = watcher.key();

        const CYCLES: usize = 20;
        let mut reconcile_eligible = 0usize;
        let mut polls = 0usize;

        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        let mut next = process_rx.try_recv().ok();
        while let Some(item) = next.take() {
            polls += 1;
            if item.trigger == WorkTrigger::Reconcile {
                reconcile_eligible += 1;
            }
            {
                let mut state = dispatch.lock().expect("dispatch mutex poisoned");
                state.pending.remove(&key);
                state.inflight.insert(key.clone());
            }
            if polls <= CYCLES {
                // The database is written continuously, so the key is busy for
                // the whole poll and a reconcile tick lands on top of it.
                enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
                enqueue_work(reconcile.clone(), &process_tx, &dispatch, &metrics).await;
            }
            next = complete_work(&key, &dispatch);
        }

        assert_eq!(
            polls,
            CYCLES + 1,
            "each cycle must reschedule exactly one poll"
        );
        assert_eq!(
            reconcile_eligible, CYCLES,
            "every reconcile tick that landed on a busy key must still buy one \
             reconcile-eligible poll; got {reconcile_eligible} in {polls} polls"
        );
    }

    /// Issue #601 §2.4 — the **upper** bound on `reconcile_owed`, and the half
    /// that a lower-bound-only guard let through.
    ///
    /// `reconcile_owed` is a debt ledger: one reconcile tick buys one
    /// reconcile-eligible poll, no more. The failure this pins is not "the tick
    /// was lost" but "the tick never stops being spent": `complete_work` arms
    /// `Reconcile` into `item_by_key`, the next watcher event merges it back
    /// down to `Watcher`, and a ledger that owes on *that* downgrade re-creates
    /// the debt the arming just settled. `complete_work` re-arms, and the key
    /// is permanently sweep-eligible — at the 50 ms watcher-debounce cadence
    /// instead of once per `reconcile_interval_seconds`. That inverts §2.4 in
    /// the expensive direction on the busiest database, which is the one with
    /// the most history to sweep and the largest slice cost (§0, WI-04).
    ///
    /// One tick on the first cycle, then twenty cycles of pure watcher churn.
    ///
    /// Fails for: owing a debt because a stored item's trigger was downgraded
    /// (21 polls, 20 of them reconcile-eligible, where the answer is 1).
    #[tokio::test]
    async fn one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(64);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let watcher = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
        };
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        let key = watcher.key();

        const CYCLES: usize = 20;
        let mut reconcile_eligible = 0usize;
        let mut polls = 0usize;

        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        let mut next = process_rx.try_recv().ok();
        while let Some(item) = next.take() {
            polls += 1;
            if item.trigger == WorkTrigger::Reconcile {
                reconcile_eligible += 1;
            }
            {
                let mut state = dispatch.lock().expect("dispatch mutex poisoned");
                state.pending.remove(&key);
                state.inflight.insert(key.clone());
            }
            if polls <= CYCLES {
                enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
                // Exactly one reconcile tick, on the first cycle. Everything
                // after this is a watcher event on a busy key.
                if polls == 1 {
                    enqueue_work(reconcile.clone(), &process_tx, &dispatch, &metrics).await;
                }
            }
            next = complete_work(&key, &dispatch);
        }

        assert_eq!(
            polls,
            CYCLES + 1,
            "each cycle must reschedule exactly one poll"
        );
        assert_eq!(
            reconcile_eligible, 1,
            "one reconcile tick must buy exactly one reconcile-eligible poll; \
             got {reconcile_eligible} in {polls} polls, which means watcher \
             churn alone is manufacturing sweep eligibility"
        );
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .is_empty(),
            "and the ledger must be settled, not carrying a standing debt"
        );
    }

    /// Issue #601 §0/§4 — the **pending window**, which neither existing bound
    /// exercises: `continuous_watcher_churn_still_yields_reconcile_eligible_polls`
    /// and `one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll` both
    /// move the key to `inflight` before the tick arrives, so both take
    /// `enqueue_work`'s dirty branch. Production has a third state.
    ///
    /// A debounced watcher poll is dispatched, so the key sits in `pending` —
    /// handed to `process_tx` but not yet inflight. The processor loop parks on
    /// `sem.acquire_owned()` before draining the next item, so a saturated
    /// worker pool holds keys there indefinitely. A reconcile tick bypasses the
    /// debounce entirely (`spawn_reconcile_task` calls `enqueue_work` directly),
    /// lands on the pending key, and takes the third branch: the debt is
    /// recorded and `dirty` is **not** set. The queued item still carries
    /// `Watcher`, so no sweep slice rides it, and when it completes there is
    /// nothing dirty behind it — the key leaves the dispatcher with the tick
    /// still unpaid.
    ///
    /// The tick must survive that, because nothing else in the system will ever
    /// re-issue it: `spawn_reconcile_task` fires once per
    /// `reconcile_interval_seconds` and does not remember what it dispatched.
    ///
    /// Direction bounded: **lower**, and that half is this test's whole
    /// contribution. It is the only guard on the pending window — the debt must
    /// outlive the dispatcher entry — and no other test reaches that window.
    ///
    /// The trailing upper-bound half (the revived debt is spent once, not
    /// latched) is kept for readability but claims nothing new: for any
    /// single-site mutation it is redundant with
    /// `one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll` and
    /// `a_reconcile_tick_landing_on_an_inflight_poll_survives_to_the_next_one`,
    /// both of which drain through `arm_owed_reconcile` too. It bites only if
    /// the ledger stops settling in *both* places at once.
    ///
    /// Fails for: pruning `reconcile_owed` when a key leaves the dispatcher
    /// (the debt is destroyed before any poll can carry it); dropping
    /// `item_by_key`'s prune along with it; or a ledger that never settles, in
    /// which case the revived poll's drain assertion goes.
    #[tokio::test]
    async fn a_tick_landing_on_a_queued_poll_survives_that_polls_completion() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let watcher = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
        };
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        let key = watcher.key();

        // The debounce dispatches a watcher poll. It is queued, not inflight:
        // the worker pool is saturated, so nothing has drained it yet.
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        let queued = process_rx.try_recv().expect("watcher item is forwarded");
        assert_eq!(queued.trigger, WorkTrigger::Watcher);
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .pending
                .contains(&key),
            "the dispatched item is pending until the processor drains it"
        );

        // The reconcile tick lands on the pending key.
        enqueue_work(reconcile, &process_tx, &dispatch, &metrics).await;
        assert!(
            process_rx.try_recv().is_err(),
            "a key already pending is not queued a second time"
        );
        {
            let state = dispatch.lock().expect("dispatch mutex poisoned");
            assert_eq!(
                state.item_by_key.get(&key).map(|item| item.trigger),
                Some(WorkTrigger::Watcher),
                "the queued poll is not upgraded — it has already been sent"
            );
            assert!(!state.dirty.contains(&key), "a pending key is not dirtied");
            assert!(
                state.reconcile_owed.contains(&key),
                "so the tick has to be owed instead"
            );
        }

        // The worker finally picks the queued item up and runs it. It carries
        // `Watcher`, so no sweep slice attaches — the tick is still unpaid.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(
            complete_work(&key, &dispatch).is_none(),
            "nothing was written during the poll, so there is no re-enqueue"
        );
        {
            let state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(
                !state.item_by_key.contains_key(&key),
                "the key has left the dispatcher, so its queued item goes"
            );
            assert!(
                state.reconcile_owed.contains(&key),
                "but the unpaid tick must not go with it — no poll ever carried \
                 it, and nothing re-issues a reconcile firing"
            );
        }

        // The writer comes back. The next poll of that key honours the debt.
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        let revived = process_rx.try_recv().expect("the key polls again");
        assert_eq!(
            revived.trigger,
            WorkTrigger::Reconcile,
            "the surviving tick rides the next poll of the key it belongs to"
        );
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .is_empty(),
            "and is drained on use"
        );

        // Upper bound: one tick, one reconcile-eligible poll. The cycle after
        // it is ordinary watcher work again.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());
        enqueue_work(watcher, &process_tx, &dispatch, &metrics).await;
        let after = process_rx.try_recv().expect("the key polls once more");
        assert_eq!(
            after.trigger,
            WorkTrigger::Watcher,
            "a surviving debt must be spent once, not latch the key sweep-eligible"
        );
        assert!(dispatch
            .lock()
            .expect("dispatch mutex poisoned")
            .reconcile_owed
            .is_empty());
    }

    /// Issue #601 §2.4 — `reconcile_owed` is a **set**, and several ticks
    /// landing on one key that never leaves the pending window between them
    /// therefore buy one sweep-eligible poll, not one each.
    ///
    /// This is a deliberate design choice and the type is where it is made, so
    /// it gets a guard rather than a comment. A sweep slice covers everything
    /// accumulated since the previous slice, so the debt is a flag and not an
    /// amount of work: N flags on one key would buy N identical sweeps of a key
    /// that needed one, and §4's complete-sweep interval is bounded by the poll
    /// rate either way.
    ///
    /// It is not covered by
    /// `two_reconcile_ticks_straddling_one_queued_poll_buy_two_polls`, which
    /// looks adjacent but is the opposite case: its first tick is *dispatched*,
    /// so only one tick is ever owed and a counting ledger and a set answer
    /// identically. Nor by
    /// `a_tick_landing_on_a_queued_poll_survives_that_polls_completion`, which
    /// lands exactly one tick on the pending window.
    ///
    /// Fails for: replacing `HashSet<String>` with a per-key count (the second
    /// poll after the debt is spent comes back `Reconcile`).
    #[tokio::test]
    async fn reconcile_ticks_landing_on_one_queued_poll_coalesce() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "1a5b0c93-3f77-4d2e-9b1c-0d2f6a8e4471";
        let watcher = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
        };
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        let key = watcher.key();

        // A watcher poll is dispatched and parks in `pending`: the worker pool
        // is saturated, so nothing drains it. Two reconcile firings land inside
        // that one window.
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        process_rx.try_recv().expect("watcher item is forwarded");
        enqueue_work(reconcile.clone(), &process_tx, &dispatch, &metrics).await;
        enqueue_work(reconcile, &process_tx, &dispatch, &metrics).await;
        assert!(
            process_rx.try_recv().is_err(),
            "a key already pending is not queued again"
        );
        assert_eq!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .len(),
            1,
            "two ticks inside one pending window are one debt, not two"
        );

        // The worker runs the queued watcher poll; the debt is still unpaid.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());

        // The next poll of the key carries the sweep…
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        assert_eq!(
            process_rx.try_recv().expect("the key polls again").trigger,
            WorkTrigger::Reconcile,
            "the coalesced debt is settled by the next poll"
        );

        // …and the poll after it is ordinary watcher work. A counting ledger
        // would make this one `Reconcile` too.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());
        enqueue_work(watcher, &process_tx, &dispatch, &metrics).await;
        assert_eq!(
            process_rx
                .try_recv()
                .expect("the key polls once more")
                .trigger,
            WorkTrigger::Watcher,
            "one sweep slice covers everything both ticks would have swept, so \
             the second tick must not buy a second sweep"
        );
        assert!(dispatch
            .lock()
            .expect("dispatch mutex poisoned")
            .reconcile_owed
            .is_empty());
    }

    /// Build `count` distinct canonical Claude Code session paths. Distinct
    /// paths mean distinct `work.key()`s, which is what makes a fleet-wide
    /// ledger assertion possible.
    fn sweep_fleet(count: usize) -> Vec<WorkItem> {
        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        (0..count)
            .map(|index| WorkItem {
                source_name: "claude".to_string(),
                harness: "claude-code".to_string(),
                format: SourceFormat::Jsonl,
                source_glob: String::new(),
                path: format!("{proj}/7e74512d-612b-4406-ae5e-{index:012}.jsonl"),
                trigger: WorkTrigger::Watcher,
            })
            .collect()
    }

    /// Issue #601 §2.4 — the **upper** bound on `enqueue_work`'s settle/owe
    /// condition, which since `complete_work` stopped pruning is the only thing
    /// bounding `reconcile_owed` at all.
    ///
    /// The ordinary case for a reconcile firing is the one this pins: the sweep
    /// enumerates the whole tracked-path set, and most of those paths are idle,
    /// so most ticks are handed straight to a poll. Such a tick is *paid on
    /// dispatch* and must leave no entry behind. Recording it anyway looks
    /// harmless one key at a time, but a firing touches every path, and with no
    /// reaper left the debts are permanent: the ledger grows to the full
    /// tracked-path set and every subsequent watcher poll of every path is
    /// upgraded to `Reconcile`, attaching a sweep slice to every filesystem
    /// event forever. That is worse than a per-key latch, and it is invisible to
    /// the existing bounds — they all drive one key.
    ///
    /// So: one sweep over 50 idle paths, then a watcher event on each after it
    /// has gone idle again.
    ///
    /// Not the only test that can *detect* over-owing —
    /// `two_reconcile_ticks_straddling_one_queued_poll_buy_two_polls` asserts an
    /// empty ledger after its first carried tick and trips on the same
    /// single-site mutation. What is unique here is that this test *also* pins
    /// the **consequence**: it is the only place a debt the sweep should never
    /// have recorded is shown upgrading an ordinary watcher poll, on every path
    /// at once, and that closing assertion fails independently of the mid-way
    /// one. Note the order, though — the mid-way ledger-size assertion runs
    /// first, so under a single-site over-owe mutation its message is the one a
    /// developer actually sees; the closing assertion is what that message
    /// *means*.
    ///
    /// Dispatch order is deliberately not asserted. The sweep's coverage claim
    /// is about which paths are polled, not the sequence they arrive in, and
    /// WI-04 is free to batch, dedup or reorder sweep dispatch; pinning the
    /// order here would fail that work for a reason unrelated to
    /// `reconcile_owed`.
    ///
    /// Fails for: owing on every incoming `Reconcile` regardless of whether the
    /// dispatch carried it — 50 standing debts, and 50 watcher polls that come
    /// back `Reconcile`.
    #[tokio::test]
    async fn a_reconcile_sweep_over_idle_paths_leaves_no_standing_debt() {
        const PATHS: usize = 50;

        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(PATHS * 2);
        let fleet = sweep_fleet(PATHS);

        // One reconcile firing. Every path is idle, so every tick is handed
        // straight to a poll.
        for item in &fleet {
            let tick = WorkItem {
                trigger: WorkTrigger::Reconcile,
                ..item.clone()
            };
            enqueue_work(tick, &process_tx, &dispatch, &metrics).await;
        }
        let mut swept_paths = std::collections::HashSet::new();
        for _ in &fleet {
            let swept = process_rx.try_recv().expect("every idle path is swept");
            assert_eq!(swept.trigger, WorkTrigger::Reconcile);
            swept_paths.insert(swept.path);
        }
        assert_eq!(
            swept_paths,
            fleet.iter().map(|item| item.path.clone()).collect(),
            "the sweep covers every idle path exactly once, in whatever order"
        );

        {
            let state = dispatch.lock().expect("dispatch mutex poisoned");
            let owed = state.reconcile_owed.len();
            assert_eq!(
                owed, 0,
                "a tick a dispatch carried is paid; {owed} of {PATHS} swept \
                 paths are carrying a standing debt, and nothing prunes it"
            );
        }

        // Each swept poll runs and finishes with nothing behind it, so every
        // key leaves the dispatcher.
        for item in &fleet {
            let key = item.key();
            {
                let mut state = dispatch.lock().expect("dispatch mutex poisoned");
                state.pending.remove(&key);
                state.inflight.insert(key.clone());
            }
            assert!(complete_work(&key, &dispatch).is_none());
        }

        // The writers come back. Ordinary watcher work must stay ordinary
        // watcher work: no standing debt means no sweep slice on a filesystem
        // event.
        for item in &fleet {
            enqueue_work(item.clone(), &process_tx, &dispatch, &metrics).await;
        }
        let mut upgraded = 0usize;
        for _ in 0..PATHS {
            let polled = process_rx.try_recv().expect("every path polls again");
            if polled.trigger == WorkTrigger::Reconcile {
                upgraded += 1;
            }
        }
        assert_eq!(
            upgraded, 0,
            "{upgraded} of {PATHS} watcher polls were upgraded to Reconcile by a \
             debt the sweep should never have recorded; a sweep slice on every \
             watcher event on every path is the cost"
        );
    }

    /// Issue #601 §0/§4 — the **lower** bound on `enqueue_work`'s settle/owe
    /// condition, and the half that its `should_send` conjunct carries alone.
    ///
    /// Two reconcile firings can straddle one queued poll. The first tick finds
    /// the path idle and is dispatched, so `item_by_key` now reads `Reconcile`;
    /// the worker pool is saturated, so that poll sits in `pending` rather than
    /// running. The second tick lands on top of it. Its merged trigger is
    /// `Reconcile` — inherited from the queued item, not carried by this call —
    /// and a settle condition that inspects the trigger without also requiring
    /// that *this* call dispatched it reads the second tick as already paid and
    /// discards it. One firing's worth of sweep coverage vanishes, and nothing
    /// re-issues it: `spawn_reconcile_task` does not remember what it
    /// dispatched.
    ///
    /// Two ticks must therefore buy two reconcile-eligible polls.
    ///
    /// Direction bounded: **lower**, by the closing assertion — the second tick
    /// must survive. The mid-way assertion that the *first* tick left an empty
    /// ledger is an incidental upper-bound check on the same condition, and is
    /// deliberately kept: it is what makes this test trip on an over-owing
    /// mutation too, so neither conjunct can be moved without something here
    /// going red.
    ///
    /// Fails for: dropping the `should_send` conjunct from the settle condition
    /// (the second tick takes the settled path and the revived poll comes back
    /// `Watcher`); or owing on every incoming `Reconcile` (the first tick's
    /// carried dispatch leaves a debt behind).
    #[tokio::test]
    async fn two_reconcile_ticks_straddling_one_queued_poll_buy_two_polls() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let watcher = sweep_fleet(1).remove(0);
        let reconcile = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };
        let key = watcher.key();

        // First firing: the path is idle, so the tick rides the poll it starts.
        enqueue_work(reconcile.clone(), &process_tx, &dispatch, &metrics).await;
        let first = process_rx.try_recv().expect("an idle path is swept");
        assert_eq!(first.trigger, WorkTrigger::Reconcile);
        {
            let state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(state.pending.contains(&key), "queued, not yet inflight");
            assert!(
                state.reconcile_owed.is_empty(),
                "the dispatch carried it, so nothing is owed"
            );
        }

        // Second firing, while the first poll is still queued. The stored item
        // reads `Reconcile`, but this call handed nothing to a poll.
        enqueue_work(reconcile, &process_tx, &dispatch, &metrics).await;
        assert!(
            process_rx.try_recv().is_err(),
            "a key already pending is not queued a second time"
        );
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .contains(&key),
            "an incoming tick this call did not dispatch is owed, whatever the \
             queued item's trigger happens to read"
        );

        // The queued poll runs and finishes with nothing behind it.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());

        // The second firing's tick is still owed, and the next poll carries it.
        enqueue_work(watcher, &process_tx, &dispatch, &metrics).await;
        let second = process_rx.try_recv().expect("the key polls again");
        assert_eq!(
            second.trigger,
            WorkTrigger::Reconcile,
            "two reconcile firings must buy two reconcile-eligible polls; the \
             second tick was swallowed by the first tick's queued poll"
        );
        assert!(dispatch
            .lock()
            .expect("dispatch mutex poisoned")
            .reconcile_owed
            .is_empty());
    }

    /// `arm_owed_reconcile` is the only place in the dispatcher that upgrades a
    /// trigger, so it is also the only place that can contradict
    /// `WorkTrigger::merge`. `merge` keeps `Startup` over `Reconcile` because
    /// `Startup`'s own doc says startup is the worst moment to add a broad
    /// scan; the arming path must agree.
    ///
    /// Direction bounded: **upper**, on which polls may become sweep-eligible —
    /// a `Startup` poll may not. And the debt is not spent by the refusal:
    /// refusing to carry a tick is not paying it, so the ledger keeps the entry
    /// for the key's next ordinary poll. That second assertion is what keeps
    /// the refusal from silently becoming a *lower*-bound violation.
    ///
    /// It does **not** bound the refusal's *width*: this test drives only a
    /// `Startup` carrier, so a predicate that refused every non-`Watcher`
    /// carrier would satisfy it. That direction is
    /// `a_standing_debt_is_settled_by_a_reconcile_poll_not_only_a_watcher_one`,
    /// immediately below; the pair is what makes the condition exactly
    /// `== Startup` and not merely "at least `Startup`".
    ///
    /// No production `Startup` item can hold a debt today (`arm_owed_reconcile`
    /// carries the reachability argument), which is exactly why this is a test
    /// rather than a comment: WI-04 gives the upgrade a real cost, and an
    /// unpinned branch is one a later change may flip without noticing.
    ///
    /// Fails for: dropping the `Startup` refusal (the backfill poll comes back
    /// `Reconcile`), or refusing while still draining the ledger (the tick is
    /// destroyed and the following watcher poll comes back `Watcher`).
    ///
    /// It also trips if `complete_work`'s deleted second reaper is reinstated,
    /// because parking a debt on an *idle* key is only reachable through the
    /// pending window. That is a side effect of the setup, not a claim: the
    /// prune itself is guarded by
    /// `a_tick_landing_on_a_queued_poll_survives_that_polls_completion`.
    #[tokio::test]
    async fn an_owed_tick_neither_upgrades_a_startup_poll_nor_is_spent_by_it() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let watcher = sweep_fleet(1).remove(0);
        let key = watcher.key();
        let startup = WorkItem {
            trigger: WorkTrigger::Startup,
            ..watcher.clone()
        };

        // Park an unpaid tick on an otherwise idle key: a reconcile tick lands
        // on a queued watcher poll, which then completes with nothing behind it.
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        process_rx.try_recv().expect("watcher item is forwarded");
        enqueue_work(
            WorkItem {
                trigger: WorkTrigger::Reconcile,
                ..watcher.clone()
            },
            &process_tx,
            &dispatch,
            &metrics,
        )
        .await;
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(state.reconcile_owed.contains(&key), "the tick is owed");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());

        // The startup backfill reaches the path. It must not pay sweep cost.
        enqueue_work(startup, &process_tx, &dispatch, &metrics).await;
        let backfill = process_rx.try_recv().expect("the startup poll is sent");
        assert_eq!(
            backfill.trigger,
            WorkTrigger::Startup,
            "startup is the worst moment to add a broad scan, and merge already \
             says so; the arming path must not override it"
        );
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .contains(&key),
            "and refusing to carry the tick is not paying it — the debt stands"
        );

        // The key's next ordinary poll settles it.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());
        enqueue_work(watcher, &process_tx, &dispatch, &metrics).await;
        let after = process_rx.try_recv().expect("the key polls again");
        assert_eq!(
            after.trigger,
            WorkTrigger::Reconcile,
            "the tick the startup poll declined to carry rides the next one"
        );
        assert!(dispatch
            .lock()
            .expect("dispatch mutex poisoned")
            .reconcile_owed
            .is_empty());
    }

    /// Issue #601 §2.4 — the **width** of `arm_owed_reconcile`'s refusal, the
    /// direction its own guard cannot see.
    ///
    /// `an_owed_tick_neither_upgrades_a_startup_poll_nor_is_spent_by_it` bounds
    /// the refusal from both of the usual sides — it must fire for `Startup`,
    /// and firing must not also spend the debt — but it drives only a `Startup`
    /// carrier. Widening the condition by one token, from `== Startup` to
    /// `!= Watcher`, therefore leaves it green, along with every other test in
    /// the suite. What that widening actually removes is the settlement of a
    /// standing debt onto a `Reconcile` carrier, and that is an ordinary steady
    /// state rather than a corner:
    ///
    ///   1. a reconcile tick lands on a `pending` key and is owed — the window
    ///      `a_tick_landing_on_a_queued_poll_survives_that_polls_completion`
    ///      models;
    ///   2. that poll completes with nothing behind it, so the key leaves the
    ///      dispatcher with the debt standing;
    ///   3. the *next* reconcile firing, one `reconcile_interval_seconds`
    ///      later, finds the key idle — and its carrier is already `Reconcile`.
    ///      That poll is the natural place to settle the debt, and this is the
    ///      step the widened predicate skips.
    ///
    /// Skipping it costs twice. The debt survives a firing that should have
    /// cleared it, so the next ordinary watcher event spends it instead and a
    /// filesystem event pays for a sweep slice nothing bought — precisely the
    /// inversion `one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll`
    /// exists to prevent. And on a path that never sees another watcher event —
    /// an archived session, a rotated log — no reconcile poll will ever clear
    /// the entry either, so it is a permanent leak, which the retention
    /// argument in `complete_work` does not cover: that argument assumes a
    /// tracked path's next reconcile firing drains its debt.
    ///
    /// Direction bounded: **width**. Narrowing the refusal is covered by the
    /// test above (dropping it makes a `Startup` poll sweep-eligible);
    /// widening it by one token is covered here and, at the time of writing,
    /// nowhere else in the suite.
    ///
    /// Fails for: refusing to settle on any carrier other than `Watcher` — the
    /// debt survives step 3 and is spent by the following watcher poll instead.
    #[tokio::test]
    async fn a_standing_debt_is_settled_by_a_reconcile_poll_not_only_a_watcher_one() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let watcher = sweep_fleet(1).remove(0);
        let key = watcher.key();
        let tick = WorkItem {
            trigger: WorkTrigger::Reconcile,
            ..watcher.clone()
        };

        // Step 1-2: a tick lands on a queued watcher poll and is owed; the poll
        // completes with nothing behind it, so the key leaves the dispatcher
        // with the debt standing.
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        process_rx.try_recv().expect("watcher item is forwarded");
        enqueue_work(tick.clone(), &process_tx, &dispatch, &metrics).await;
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            assert!(
                state.reconcile_owed.contains(&key),
                "a tick landing on a pending poll is owed"
            );
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());

        // Step 3: the next reconcile firing finds the key idle. The poll it
        // starts already carries `Reconcile`, and it is that poll — not some
        // later watcher event — that settles the standing debt.
        enqueue_work(tick, &process_tx, &dispatch, &metrics).await;
        let swept = process_rx.try_recv().expect("the idle key is swept");
        assert_eq!(swept.trigger, WorkTrigger::Reconcile);
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .is_empty(),
            "a reconcile poll settles a standing debt just as a watcher poll \
             does; refusing to settle on anything but a `Watcher` carrier \
             leaves the debt to be spent by a filesystem event instead"
        );

        // Consequence: with the debt already settled, the key's next ordinary
        // watcher poll is ordinary watcher work. If step 3 had skipped the
        // settlement this is where the sweep slice would attach.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());
        enqueue_work(watcher, &process_tx, &dispatch, &metrics).await;
        let after = process_rx.try_recv().expect("the key polls again");
        assert_eq!(
            after.trigger,
            WorkTrigger::Watcher,
            "a debt a reconcile poll already paid must not be spent a second \
             time by a filesystem event"
        );
    }

    /// Issue #601 §2.4 — the **width** of the settle/owe condition's *incoming*
    /// filter, the sibling hole to the one above and found the same way.
    ///
    /// `incoming_trigger == Reconcile` names one variant of three, so its
    /// extent needs pinning at both neighbours, not just at `Reconcile`.
    /// `debounce_coalesces_a_watcher_burst_into_one_watcher_poll` already
    /// covers `Watcher` ("watcher churn must not manufacture a reconcile
    /// debt"); this covers `Startup`, and without it widening the filter by one
    /// token to `!= Watcher` leaves the whole suite green.
    ///
    /// The widened form would let a startup backfill enqueue *create* a debt,
    /// which is the same §2.4 inversion `arm_owed_reconcile` refuses in the
    /// other direction — there a `Startup` poll may not *carry* a tick; here a
    /// `Startup` event may not *mint* one. Refusing only the first while
    /// permitting the second is strictly worse than permitting both, because
    /// the minted debt is then spent by some later watcher poll: a filesystem
    /// event pays sweep cost that a startup event bought.
    ///
    /// And it is far more reachable than the carrier case. `run_ingestor`
    /// spawns the watcher threads *before* the backfill loop, so during startup
    /// a path can have a queued watcher poll at the moment backfill enumerates
    /// it — an ordinary race on any harness that is writing while moraine
    /// starts, not the narrow window `arm_owed_reconcile`'s reachability note
    /// describes.
    ///
    /// Direction bounded: **width**, at the `Startup` neighbour. `Reconcile`
    /// itself is bounded by the owe path's own lower/upper guards
    /// (`two_reconcile_ticks_straddling_one_queued_poll_buy_two_polls` and
    /// `a_reconcile_sweep_over_idle_paths_leaves_no_standing_debt`).
    ///
    /// Fails for: owing on any incoming trigger that is not `Watcher` — the
    /// startup enqueue mints a debt and the following watcher poll spends it.
    #[tokio::test]
    async fn a_startup_poll_landing_on_a_queued_poll_owes_nothing() {
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);

        let watcher = sweep_fleet(1).remove(0);
        let key = watcher.key();
        let startup = WorkItem {
            trigger: WorkTrigger::Startup,
            ..watcher.clone()
        };

        // A watcher poll is queued for a path the startup backfill has not
        // reached yet.
        enqueue_work(watcher.clone(), &process_tx, &dispatch, &metrics).await;
        let queued = process_rx.try_recv().expect("watcher item is forwarded");
        assert_eq!(queued.trigger, WorkTrigger::Watcher);

        // Backfill reaches it. The key is already pending, so nothing is
        // dispatched — and a startup event is not a reconcile tick, so there is
        // nothing to remember either.
        enqueue_work(startup, &process_tx, &dispatch, &metrics).await;
        assert!(
            process_rx.try_recv().is_err(),
            "a key already pending is not queued a second time"
        );
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .is_empty(),
            "startup backfill must not manufacture a reconcile debt; only a \
             reconcile tick may put an entry in the ledger"
        );

        // Consequence: the key's next poll is ordinary watcher work. This is
        // where a minted debt would attach its sweep slice.
        {
            let mut state = dispatch.lock().expect("dispatch mutex poisoned");
            state.pending.remove(&key);
            state.inflight.insert(key.clone());
        }
        assert!(complete_work(&key, &dispatch).is_none());
        enqueue_work(watcher, &process_tx, &dispatch, &metrics).await;
        let after = process_rx.try_recv().expect("the key polls again");
        assert_eq!(
            after.trigger,
            WorkTrigger::Watcher,
            "a filesystem event must not pay for a sweep slice that a startup \
             event bought"
        );
    }

    /// The debounce window coalesces by `work.key()`, so a watcher burst on one
    /// session file becomes **one** poll rather than one poll per event.
    ///
    /// Only watcher events are fed, because only watcher events reach this
    /// window in production: its receiver is `watch_path_rx` and its only
    /// sender is `spawn_watcher_threads`. The earlier version of this test drove
    /// `Reconcile` through it to exercise a debt-owing branch that no producer
    /// could reach; that branch is deleted, and asserting on synthetic input is
    /// how it survived being dead. `spawn_debounce_task` now `debug_assert`s the
    /// invariant, so a future `Reconcile` producer trips there instead.
    ///
    /// The coalesced poll must stay `Watcher` and the ledger must stay empty:
    /// watcher churn alone never manufactures sweep eligibility that no
    /// reconcile tick paid for (the §2.4 upper bound, at the debounce).
    ///
    /// Fails for: a window that never drains its ready items (nothing is
    /// dispatched at all), or owing a debt from this window again — the owed
    /// tick is armed onto the very poll the burst dispatches, so it comes back
    /// `Reconcile` and watcher churn has bought itself a sweep slice.
    ///
    /// It does **not** falsify the debounce's choice of coalescing key: keying
    /// the window per-event still yields one poll, because `enqueue_work`'s
    /// `pending` set dedupes by `work.key()` underneath it (verified by
    /// mutation — the burst stays one entry). The single-entry assertion is
    /// therefore a statement about the pair, not about this map alone.
    ///
    /// It also does **not** bound the *width* of `enqueue_work`'s settle/owe
    /// filter, and an earlier revision of that filter's comment claimed it did.
    /// The burst coalesces into one `enqueue_work` call against an idle key, so
    /// `pending.insert` returns true, `carried` is true, and the owe branch is
    /// unreachable regardless of which triggers the filter admits: widening it
    /// to `incoming_trigger != WorkTrigger::Startup` leaves this test green
    /// (run alone under that mutation: 1 passed, 0 failed).
    /// `one_reconcile_tick_buys_exactly_one_reconcile_eligible_poll` is what
    /// catches that.
    #[tokio::test]
    async fn debounce_coalesces_a_watcher_burst_into_one_watcher_poll() {
        let proj = "/Users/x/.claude/projects/-Users-x-src-moraine";
        let sid = "7e74512d-612b-4406-ae5e-069e73d7f2dc";
        let base = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: format!("{proj}/{sid}.jsonl"),
            trigger: WorkTrigger::Watcher,
        };

        let mut config = moraine_config::AppConfig::default();
        config.ingest.debounce_ms = 10;
        let dispatch = Arc::new(Mutex::new(DispatchState::default()));
        let metrics = Arc::new(Metrics::default());
        let (debounce_tx, debounce_rx) = mpsc::unbounded_channel::<WorkItem>();
        let (process_tx, mut process_rx) = mpsc::channel::<WorkItem>(8);
        let task = spawn_debounce_task(
            config,
            debounce_rx,
            process_tx,
            dispatch.clone(),
            metrics.clone(),
        );

        for _ in 0..4 {
            debounce_tx
                .send(base.clone())
                .expect("watcher event reaches the debounce");
        }

        let dispatched = timeout(Duration::from_secs(2), process_rx.recv())
            .await
            .expect("debounce must emit within the window")
            .expect("debounce channel stays open");
        assert_eq!(
            dispatched.trigger,
            WorkTrigger::Watcher,
            "a burst of watcher events dispatches as watcher work"
        );
        assert!(
            timeout(Duration::from_millis(100), process_rx.recv())
                .await
                .is_err(),
            "the burst must coalesce into one queue entry"
        );
        assert!(
            dispatch
                .lock()
                .expect("dispatch mutex poisoned")
                .reconcile_owed
                .is_empty(),
            "watcher churn must not manufacture a reconcile debt"
        );
        drop(debounce_tx);
        task.abort();
    }

    #[test]
    fn compose_hermes_model_prepends_vendor_when_bare() {
        assert_eq!(
            compose_hermes_model("claude-opus-4-6", "https://api.anthropic.com"),
            "anthropic/claude-opus-4-6",
        );
        // Already vendor-qualified — leave it alone.
        assert_eq!(
            compose_hermes_model("openai/gpt-5", "https://api.anthropic.com"),
            "openai/gpt-5",
        );
        // No vendor we can recognize — bare model survives.
        assert_eq!(
            compose_hermes_model("some-model", "https://weird.local/"),
            "some-model",
        );
    }

    fn write_session_file(path: &PathBuf, messages: &[serde_json::Value]) {
        let doc = serde_json::json!({
            "session_id": "20260418_live_test",
            "model": "claude-opus-4-6",
            "base_url": "https://api.anthropic.com",
            "platform": "cli",
            "session_start": "2026-04-18T12:00:00.000000",
            "last_updated": "2026-04-18T12:00:00.000000",
            "system_prompt": "you are a test agent",
            "tools": [],
            "message_count": messages.len(),
            "messages": messages,
        });
        let body = serde_json::to_string_pretty(&doc).unwrap();
        std::fs::write(path, body).expect("write session file");
    }

    fn unique_session_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("moraine-session-{name}-{suffix}.json"))
    }

    async fn drain_batches(rx: &mut mpsc::Receiver<SinkMessage>) -> Vec<crate::model::RowBatch> {
        let mut out = Vec::new();
        while let Ok(Some(SinkMessage::Batch(batch))) =
            timeout(Duration::from_millis(50), rx.recv()).await
        {
            out.push(batch);
        }
        out
    }

    #[derive(Debug)]
    enum ObservedSinkMessage {
        Batch(crate::model::RowBatch),
        Begin(crate::CheckpointTransition),
        Finalize(crate::CheckpointTransition),
        Block(crate::CheckpointTransition),
        MirrorCaughtUp,
    }

    fn observe_and_ack(message: SinkMessage) -> ObservedSinkMessage {
        match message {
            SinkMessage::Batch(batch) => ObservedSinkMessage::Batch(batch),
            SinkMessage::BeginReplay { transition, ack } => {
                let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                    checkpoint_revision: 1,
                    operation_id: transition.checkpoint.operation_id.clone(),
                }));
                ObservedSinkMessage::Begin(transition)
            }
            SinkMessage::FinalizeReplay { transition, ack } => {
                let _ = ack.send(Ok(crate::publication::FinalizeReplayOutcome::Published(
                    crate::publication::PublicationAck {
                        checkpoint_revision: 2,
                        publication_revision: 1,
                        already_published: false,
                    },
                )));
                ObservedSinkMessage::Finalize(transition)
            }
            SinkMessage::BlockReplay { transition, ack } => {
                let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                    checkpoint_revision: 2,
                    operation_id: transition.checkpoint.operation_id.clone(),
                }));
                ObservedSinkMessage::Block(transition)
            }
            SinkMessage::MirrorCaughtUp { transition, ack } => {
                let _ = ack.send(Ok(crate::publication::ReplayBarrierAck {
                    checkpoint_revision: 2,
                    operation_id: transition.checkpoint.operation_id.clone(),
                }));
                ObservedSinkMessage::MirrorCaughtUp
            }
        }
    }

    async fn drive_with_barrier_acks<F>(
        process: F,
        rx: &mut mpsc::Receiver<SinkMessage>,
    ) -> (anyhow::Result<()>, Vec<ObservedSinkMessage>)
    where
        F: Future<Output = anyhow::Result<()>>,
    {
        tokio::pin!(process);
        let mut observed = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut process => break result,
                maybe_message = rx.recv() => {
                    let Some(message) = maybe_message else {
                        break Err(anyhow::anyhow!("sink channel closed while process was active"));
                    };
                    observed.push(observe_and_ack(message));
                }
            }
        };
        while let Ok(message) = rx.try_recv() {
            observed.push(observe_and_ack(message));
        }
        (result, observed)
    }

    fn observed_batches(messages: &[ObservedSinkMessage]) -> Vec<&crate::model::RowBatch> {
        messages
            .iter()
            .filter_map(|message| match message {
                ObservedSinkMessage::Batch(batch) => Some(batch),
                _ => None,
            })
            .collect()
    }

    fn kiro_sidecar(session_id: &str, title: &str, input_tokens: u64, credits: f64) -> Value {
        json!({
            "session_id": session_id,
            "cwd": "/work/kiro-demo",
            "title": title,
            "created_at": "2026-05-28T20:26:40Z",
            "updated_at": "2026-05-28T20:27:10Z",
            "session_state": {
                "agent_name": "kiro_default",
                "rts_model_state": {
                    "model_info": {"model_id": "claude-sonnet-4"}
                },
                "conversation_metadata": {
                    "user_turn_metadatas": [{
                        "input_token_count": input_tokens,
                        "output_token_count": 7,
                        "metering_usage": [{
                            "value": credits,
                            "unit": "credit",
                            "unitPlural": "credits"
                        }]
                    }]
                }
            }
        })
    }

    fn kiro_work(path: &Path) -> WorkItem {
        WorkItem {
            source_name: "kiro".to_string(),
            harness: "kiro-cli".to_string(),
            format: SourceFormat::KiroSession,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_kiro_session_replays_once_when_sidecar_first_appears() {
        let path = unique_test_file("kiro-late-sidecar");
        let sidecar_path = path.with_extension("json");
        let session_id = "11111111-2222-4333-8444-555555555555";
        let prompt = json!({
            "version": "v1",
            "kind": "Prompt",
            "data": {
                "message_id": "msg-user-1",
                "content": [{"kind": "text", "data": "Inspect src/lib.rs"}],
                "meta": {"timestamp": 1780000000u64}
            }
        });
        let assistant = json!({
            "version": "v1",
            "kind": "AssistantMessage",
            "data": {
                "message_id": "msg-assistant-1",
                "content": [{"kind": "text", "data": "The function returns 42."}]
            }
        });
        fs::write(&path, format!("{prompt}\n{assistant}\n")).expect("write Kiro transcript");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(8);
        let work = kiro_work(&path);

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("ingest Kiro transcript before sidecar exists");
        let first = drain_batches(&mut sink_rx).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].raw_rows.len(), 2);
        assert_eq!(first[0].event_rows.len(), 2);
        assert!(first[0].event_rows.iter().all(|row| {
            row.get("model").and_then(Value::as_str) == Some("")
                && row.get("cwd").and_then(Value::as_str) == Some("")
        }));
        let first_event_uids = first[0]
            .event_rows
            .iter()
            .filter_map(|row| row.get("event_uid").and_then(Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        let first_checkpoint = first[0].checkpoint.as_ref().expect("checkpoint").clone();
        let first_cursor = super::parse_kiro_checkpoint_cursor(&first_checkpoint.cursor_json);
        assert!(!first_cursor.kiro_sidecar_valid);
        assert!(!first_cursor.record_ts_hint.is_empty());
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            first_checkpoint,
        );

        fs::write(
            &sidecar_path,
            serde_json::to_vec_pretty(&kiro_sidecar(session_id, "Initial title", 11, 0.25))
                .expect("serialize Kiro sidecar"),
        )
        .expect("write Kiro sidecar");
        let (result, second_messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                checkpoints.clone(),
                &VolatilePollMap::new(),
                sink_tx.clone(),
                &metrics,
            ),
            &mut sink_rx,
        )
        .await;
        result.expect("refresh Kiro session after sidecar appears");
        assert!(matches!(
            second_messages.first(),
            Some(ObservedSinkMessage::Begin(_))
        ));
        assert!(matches!(
            second_messages.last(),
            Some(ObservedSinkMessage::Finalize(_))
        ));
        let second = observed_batches(&second_messages);
        assert_eq!(second.len(), 1);
        assert_eq!(
            second[0].raw_rows.len(),
            3,
            "metadata plus transcript replay"
        );
        assert_eq!(second[0].event_rows.len(), 3);
        assert!(second[0].event_rows.iter().all(|row| {
            row.get("model").and_then(Value::as_str) == Some("claude-sonnet-4")
                && row.get("cwd").and_then(Value::as_str) == Some("/work/kiro-demo")
        }));
        let replayed_event_uids = second[0]
            .event_rows
            .iter()
            .filter(|row| row.get("event_kind").and_then(Value::as_str) != Some("session_meta"))
            .filter_map(|row| row.get("event_uid").and_then(Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_ne!(
            replayed_event_uids, first_event_uids,
            "a sidecar-driven whole-source replay uses a checked replacement generation"
        );
        let second_checkpoint = second_messages
            .iter()
            .find_map(|message| match message {
                ObservedSinkMessage::Finalize(transition) => Some(transition.checkpoint.clone()),
                _ => None,
            })
            .expect("final replacement checkpoint");
        let second_cursor = super::parse_kiro_checkpoint_cursor(&second_checkpoint.cursor_json);
        assert!(second_cursor.kiro_sidecar_valid);
        assert_ne!(second_cursor.transcript_fingerprint, 0);
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            second_checkpoint,
        );

        fs::write(
            &sidecar_path,
            serde_json::to_vec_pretty(&kiro_sidecar(session_id, "Updated title", 19, 0.5))
                .expect("serialize updated Kiro sidecar"),
        )
        .expect("update Kiro sidecar");
        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("refresh valid Kiro sidecar");
        let third = drain_batches(&mut sink_rx).await;
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].raw_rows.len(), 1, "valid sidecar update only");
        assert_eq!(third[0].event_rows.len(), 1);
        assert_eq!(
            third[0].event_rows[0]
                .pointer("/token_usage_native_units/credits")
                .and_then(Value::as_f64),
            Some(0.5)
        );
        let third_checkpoint = third[0].checkpoint.as_ref().expect("checkpoint").clone();
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            third_checkpoint,
        );

        let mut changed_hints = kiro_sidecar(session_id, "Moved session", 19, 0.5);
        changed_hints["cwd"] = json!("/work/kiro-moved");
        changed_hints["session_state"]["rts_model_state"]["model_info"]["model_id"] =
            json!("claude-opus-4");
        fs::write(
            &sidecar_path,
            serde_json::to_vec_pretty(&changed_hints).expect("serialize changed Kiro hints"),
        )
        .expect("update Kiro transcript hints");
        let (result, fourth_messages) = drive_with_barrier_acks(
            process_file(
                &config,
                &work,
                checkpoints,
                &VolatilePollMap::new(),
                sink_tx,
                &metrics,
            ),
            &mut sink_rx,
        )
        .await;
        result.expect("replay Kiro transcript after metadata hints change");
        assert!(matches!(
            fourth_messages.first(),
            Some(ObservedSinkMessage::Begin(_))
        ));
        assert!(matches!(
            fourth_messages.last(),
            Some(ObservedSinkMessage::Finalize(_))
        ));
        let fourth = observed_batches(&fourth_messages);
        assert_eq!(fourth.len(), 1);
        assert_eq!(fourth[0].raw_rows.len(), 3, "metadata plus replay");
        assert_eq!(fourth[0].event_rows.len(), 3);
        assert!(fourth[0].event_rows.iter().all(|row| {
            row.get("model").and_then(Value::as_str) == Some("claude-opus-4")
                && row.get("cwd").and_then(Value::as_str) == Some("/work/kiro-moved")
        }));

        let _ = fs::remove_file(&sidecar_path);
        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_kiro_session_tail_with_sidecar_update_inherits_previous_prompt_timestamp() {
        let path = unique_test_file("kiro-tail-timestamp");
        let sidecar_path = path.with_extension("json");
        let session_id = "22222222-3333-4444-8555-666666666666";
        let prompt = json!({
            "version": "v1",
            "kind": "Prompt",
            "data": {
                "message_id": "msg-user-1",
                "content": [{"kind": "text", "data": "Inspect src/lib.rs"}],
                "meta": {"timestamp": 1780000000u64}
            }
        });
        let assistant = json!({
            "version": "v1",
            "kind": "AssistantMessage",
            "data": {
                "message_id": "msg-assistant-1",
                "content": [{"kind": "text", "data": "The function returns 42."}]
            }
        });
        fs::write(&path, format!("{prompt}\n")).expect("write initial Kiro prompt");
        fs::write(
            &sidecar_path,
            serde_json::to_vec_pretty(&kiro_sidecar(session_id, "Tail test", 11, 0.25))
                .expect("serialize Kiro sidecar"),
        )
        .expect("write Kiro sidecar");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(8);
        let work = kiro_work(&path);

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("ingest initial Kiro prompt");
        let first = drain_batches(&mut sink_rx).await;
        assert_eq!(first.len(), 1);
        let prompt_event_ts = first[0]
            .event_rows
            .iter()
            .find(|row| row.get("item_id").and_then(Value::as_str) == Some("msg-user-1"))
            .and_then(|row| row.get("event_ts"))
            .and_then(Value::as_str)
            .expect("prompt event timestamp")
            .to_string();
        let first_checkpoint = first[0].checkpoint.as_ref().expect("checkpoint").clone();
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            first_checkpoint,
        );

        fs::write(
            &sidecar_path,
            serde_json::to_vec_pretty(&kiro_sidecar(session_id, "Updated tail test", 19, 0.5))
                .expect("serialize updated Kiro sidecar"),
        )
        .expect("update Kiro sidecar alongside transcript");
        fs::write(&path, format!("{prompt}\n{assistant}\n")).expect("append Kiro assistant");
        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("ingest appended Kiro assistant");
        let second = drain_batches(&mut sink_rx).await;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].raw_rows.len(), 2, "metadata plus appended row");
        let assistant_event_ts = second[0]
            .event_rows
            .iter()
            .find(|row| row.get("item_id").and_then(Value::as_str) == Some("msg-assistant-1"))
            .and_then(|row| row.get("event_ts"))
            .and_then(Value::as_str)
            .expect("assistant event timestamp");
        assert_eq!(assistant_event_ts, prompt_event_ts);

        let _ = fs::remove_file(&sidecar_path);
        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_kiro_session_checkpoints_malformed_sidecar_error() {
        let path = unique_test_file("kiro-malformed-sidecar");
        let sidecar_path = path.with_extension("json");
        fs::write(
            &path,
            json!({
                "version": "v1",
                "kind": "Prompt",
                "data": {
                    "message_id": "msg-user-1",
                    "content": [{"kind": "text", "data": "hello"}],
                    "meta": {"timestamp": 1780000000u64}
                }
            })
            .to_string()
                + "\n",
        )
        .expect("write Kiro transcript");
        fs::write(&sidecar_path, "{not-json").expect("write malformed sidecar");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(8);
        let work = kiro_work(&path);

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("malformed sidecar should not fail transcript ingestion");
        let first = drain_batches(&mut sink_rx).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].raw_rows.len(), 1, "transcript still ingests");
        assert_eq!(first[0].error_rows.len(), 1);
        assert_eq!(
            first[0].error_rows[0]
                .get("error_kind")
                .and_then(Value::as_str),
            Some("kiro_session_metadata_error")
        );
        let checkpoint = first[0].checkpoint.as_ref().expect("checkpoint").clone();
        assert_ne!(checkpoint.source_fingerprint, 0);
        assert!(!super::parse_kiro_checkpoint_cursor(&checkpoint.cursor_json).kiro_sidecar_valid);
        checkpoints.write().await.insert(
            crate::checkpoint::checkpoint_key(&work.source_name, &work.path),
            checkpoint,
        );

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("unchanged malformed sidecar should be checkpointed");
        assert!(drain_batches(&mut sink_rx).await.is_empty());

        let _ = fs::remove_file(&sidecar_path);
        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_kiro_session_with_exclusions_skips_when_sidecar_cwd_is_unavailable() {
        for case in ["missing", "malformed", "relative"] {
            let path = unique_test_file(&format!("kiro-{case}-sidecar-with-exclusions"));
            let sidecar_path = path.with_extension("json");
            fs::write(
                &path,
                json!({
                    "version": "v1",
                    "kind": "Prompt",
                    "data": {
                        "message_id": "msg-user-1",
                        "content": [{"kind": "text", "data": "hello"}],
                        "meta": {"timestamp": 1780000000u64}
                    }
                })
                .to_string()
                    + "\n",
            )
            .expect("write Kiro transcript");
            match case {
                "missing" => {}
                "malformed" => {
                    fs::write(&sidecar_path, "{not-json").expect("write malformed Kiro sidecar");
                }
                "relative" => {
                    let mut sidecar = kiro_sidecar("kiro-relative", "Relative", 1, 0.25);
                    sidecar["cwd"] = json!(".");
                    fs::write(
                        &sidecar_path,
                        serde_json::to_vec(&sidecar).expect("serialize sidecar"),
                    )
                    .expect("write relative-cwd Kiro sidecar");
                }
                _ => unreachable!("fixed test case"),
            }

            let mut config = moraine_config::AppConfig::default();
            config.ingest.exclude_project_dirs = vec!["/work/excluded/**".to_string()];
            let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
            let metrics = Arc::new(Metrics::default());
            let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(4);
            let work = kiro_work(&path);

            process_file(
                &config,
                &work,
                checkpoints.clone(),
                &VolatilePollMap::new(),
                sink_tx,
                &metrics,
            )
            .await
            .expect("Kiro session without a trusted cwd should be skipped");
            assert!(drain_batches(&mut sink_rx).await.is_empty());
            assert!(checkpoints.read().await.is_empty());

            let _ = fs::remove_file(&sidecar_path);
            let _ = fs::remove_file(&path);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_kiro_session_uses_sidecar_cwd_for_project_exclusion() {
        let path = unique_test_file("kiro-excluded-sidecar-cwd");
        let sidecar_path = path.with_extension("json");
        fs::write(&path, "{}\n").expect("write Kiro transcript");
        let mut sidecar = kiro_sidecar("kiro-excluded", "Excluded", 1, 0.25);
        sidecar["cwd"] = json!("/work/excluded");
        fs::write(
            &sidecar_path,
            serde_json::to_vec(&sidecar).expect("serialize Kiro sidecar"),
        )
        .expect("write Kiro sidecar");

        let mut config = moraine_config::AppConfig::default();
        config.ingest.exclude_project_dirs = vec!["/work/excluded/**".to_string()];
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(4);
        let work = kiro_work(&path);

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("excluded Kiro session should be skipped cleanly");
        assert!(drain_batches(&mut sink_rx).await.is_empty());
        assert!(checkpoints.read().await.is_empty());

        let _ = fs::remove_file(&sidecar_path);
        let _ = fs::remove_file(&path);
    }

    fn cowork_fixture_transcripts() -> Vec<PathBuf> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("fixtures/claude-cowork/local-agent-mode-sessions/account-demo/workspace-demo")
            .join("local_11111111-2222-4333-8444-555555555555")
            .join(".claude/projects/-sessions-synthetic");
        vec![
            root.join("aaaaaaaa-1111-4333-8444-555555555555.jsonl"),
            root.join("bbbbbbbb-2222-4333-8444-555555555555.jsonl"),
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_normalizes_cowork_fixture_under_one_root() {
        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(8);

        for path in cowork_fixture_transcripts() {
            let work = WorkItem {
                source_name: "claude-cowork".to_string(),
                harness: "claude-code".to_string(),
                format: SourceFormat::Jsonl,
                source_glob: String::new(),
                path: path.to_string_lossy().to_string(),
                trigger: WorkTrigger::Watcher,
            };
            process_file(
                &config,
                &work,
                checkpoints.clone(),
                &VolatilePollMap::new(),
                sink_tx.clone(),
                &metrics,
            )
            .await
            .expect("Cowork fixture transcript should process");
        }
        drop(sink_tx);

        let batches = drain_batches(&mut sink_rx).await;
        let raw_rows = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .collect::<Vec<_>>();
        let event_rows = batches
            .iter()
            .flat_map(|batch| batch.event_rows.iter())
            .collect::<Vec<_>>();
        let tool_rows = batches
            .iter()
            .flat_map(|batch| batch.tool_rows.iter())
            .collect::<Vec<_>>();
        let error_rows = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter())
            .collect::<Vec<_>>();

        let root_session = "local_11111111-2222-4333-8444-555555555555";
        assert!(!raw_rows.is_empty());
        assert!(!event_rows.is_empty());
        assert!(
            raw_rows.iter().all(|row| row["session_id"] == root_session),
            "unexpected raw session ids: {:?}",
            raw_rows
                .iter()
                .map(|row| (&row["top_type"], &row["session_id"]))
                .collect::<Vec<_>>()
        );
        assert!(event_rows
            .iter()
            .all(|row| row["session_id"] == root_session));
        assert!(event_rows
            .iter()
            .all(|row| row["source_name"] == "claude-cowork"));
        assert!(error_rows.is_empty(), "Cowork metadata must not add errors");

        for raw_only_type in ["attachment", "last-prompt", "ai-title"] {
            assert!(raw_rows.iter().any(|row| row["top_type"] == raw_only_type));
            assert!(!event_rows
                .iter()
                .any(|row| row["payload_type"] == raw_only_type));
        }

        assert!(event_rows.iter().any(|row| {
            row["event_kind"] == "message"
                && row["text_content"] == "Inspect the synthetic project."
        }));
        assert!(event_rows.iter().any(|row| {
            row["event_kind"] == "reasoning"
                && row["text_content"] == "I should inspect the fixture."
        }));
        assert!(event_rows
            .iter()
            .any(|row| row["event_kind"] == "tool_call"));
        assert!(event_rows
            .iter()
            .any(|row| row["event_kind"] == "tool_result"));
        assert!(event_rows.iter().any(|row| {
            row["event_kind"] == "message"
                && row["text_content"] == "Continue with the synthetic project."
        }));
        assert_eq!(tool_rows.len(), 2);

        let session_meta = event_rows
            .iter()
            .filter(|row| row["event_kind"] == "session_meta")
            .collect::<Vec<_>>();
        assert_eq!(session_meta.len(), 2);
        assert_eq!(
            session_meta
                .iter()
                .map(|row| row["event_uid"].as_str().expect("metadata event uid"))
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2,
            "companion metadata is qualified by each published transcript source"
        );
        assert_eq!(
            session_meta
                .iter()
                .map(|row| {
                    row["source_file"]
                        .as_str()
                        .expect("metadata source file")
                        .to_string()
                })
                .collect::<std::collections::BTreeSet<_>>(),
            cowork_fixture_transcripts()
                .iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect(),
        );
        let payload: Value = serde_json::from_str(
            session_meta[0]["payload_json"]
                .as_str()
                .expect("session metadata payload"),
        )
        .expect("valid session metadata payload");
        assert_eq!(payload["sessionId"], root_session);
        assert_eq!(payload["title"], "Cowork fixture title");
        assert_eq!(payload["model"], "claude-opus-4-6");
        assert_eq!(
            payload
                .as_object()
                .expect("metadata object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "cliSessionId",
                "createdAt",
                "cwd",
                "isArchived",
                "isStarred",
                "lastActivityAt",
                "model",
                "sessionId",
                "timestamp",
                "title",
                "type",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        let event_json = serde_json::to_string(&event_rows).expect("serialize event rows");
        assert!(!event_json.contains("PRIVATE_ATTACHMENT_SENTINEL"));
        assert!(!event_json.contains("PRIVATE_ACCOUNT_SENTINEL"));
        assert!(!event_json.contains("PRIVATE_EMAIL_SENTINEL"));
        assert!(!event_json.contains("PRIVATE_SYSTEM_PROMPT_SENTINEL"));
        assert!(!event_json.contains("PRIVATE_MCP_SENTINEL"));
        let raw_json = serde_json::to_string(&raw_rows).expect("serialize raw rows");
        assert!(raw_json.contains("PRIVATE_ATTACHMENT_SENTINEL"));
        assert!(!raw_json.contains("PRIVATE_ACCOUNT_SENTINEL"));
        assert!(!raw_json.contains("PRIVATE_EMAIL_SENTINEL"));
        assert!(!raw_json.contains("PRIVATE_SYSTEM_PROMPT_SENTINEL"));
        assert!(!raw_json.contains("PRIVATE_MCP_SENTINEL"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_skips_codex_session_from_excluded_initial_cwd_before_sink() {
        let path = unique_test_file("excluded-codex-session");
        fs::write(
            &path,
            [
                json!({
                    "timestamp": "2026-07-14T11:59:59Z",
                    "type": "turn_context",
                    "payload": {"cwd": "."}
                })
                .to_string(),
                json!({
                    "timestamp": "2026-07-14T12:00:00Z",
                    "type": "session_meta",
                    "payload": {
                        "id": "excluded-session",
                        "cwd": "/work/excluded",
                    }
                })
                .to_string(),
                json!({
                    "timestamp": "2026-07-14T12:00:01Z",
                    "type": "turn_context",
                    "payload": {"cwd": "/work/included"}
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write excluded Codex session");

        let mut config = moraine_config::AppConfig::default();
        config.ingest.exclude_project_dirs = vec!["/work/excluded/**".to_string()];
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(4);
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints.clone(),
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("excluded Codex session should be skipped cleanly");

        assert!(
            drain_batches(&mut sink_rx).await.is_empty(),
            "excluded session must not reach the sink"
        );
        assert!(
            checkpoints.read().await.is_empty(),
            "excluded session must not create a checkpoint through the sink"
        );
        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_keeps_session_when_only_later_cwd_is_excluded() {
        let path = unique_test_file("later-excluded-cwd");
        fs::write(
            &path,
            [
                json!({
                    "timestamp": "2026-07-14T12:00:00Z",
                    "type": "session_meta",
                    "payload": {
                        "id": "included-session",
                        "cwd": "/work/included",
                    }
                })
                .to_string(),
                json!({
                    "timestamp": "2026-07-14T12:00:01Z",
                    "type": "turn_context",
                    "payload": {"cwd": "/work/excluded"}
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write included Codex session");

        let mut config = moraine_config::AppConfig::default();
        config.ingest.exclude_project_dirs = vec!["/work/excluded/**".to_string()];
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(4);
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: path.to_string_lossy().to_string(),
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("included Codex session should process");

        assert!(
            !drain_batches(&mut sink_rx).await.is_empty(),
            "a later cd must not change the session's initial inclusion decision"
        );
        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_inherits_codex_timestamp_for_legacy_rollout_records() {
        let path = unique_test_file("rollout-2025-09-21T17-12-48-legacy");
        fs::write(
            &path,
            [
                json!({
                    "id": "6ce8b66e-8a97-441b-a606-16d2a0c27083",
                    "timestamp": "2025-09-21T17:12:48.127Z",
                    "instructions": null
                })
                .to_string(),
                json!({
                    "type": "function_call",
                    "call_id": "call_legacy_rollout",
                    "name": "shell",
                    "arguments": "{}"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write legacy rollout fixture");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file,
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("legacy codex file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 2);
        assert!(
            batch
                .error_rows
                .iter()
                .all(|row| row.get("error_kind").and_then(Value::as_str)
                    != Some("timestamp_parse_error")),
            "legacy timestamp inheritance should avoid timestamp_parse_error rows"
        );
        assert_eq!(
            batch.raw_rows[1]
                .get("record_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2025-09-21T17:12:48.127Z"
        );
        assert_eq!(
            batch.event_rows[1]
                .get("event_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2025-09-21 17:12:48.127"
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_recovers_session_cwd_when_resuming_mid_file() {
        let path = unique_test_file("codex-resume-cwd");
        let header = serde_json::json!({
            "type": "session_meta",
            "timestamp": "2026-04-18T20:43:51.069Z",
            "payload": {
                "id": "codex-session-1",
                "cwd": "/repo"
            }
        })
        .to_string();
        let tail = serde_json::json!({
            "type": "function_call",
            "timestamp": "2026-04-18T20:43:52.069Z",
            "call_id": "call_resumed",
            "name": "shell",
            "arguments": "{}"
        })
        .to_string();
        fs::write(&path, format!("{header}\n{tail}\n")).expect("write codex resume fixture");

        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };

        // Simulate a restart that already ingested the session header: the
        // checkpoint sits past line 1, so the in-stream cwd hint chain never
        // sees `payload.cwd` and must be recovered from the file head.
        let meta = fs::metadata(&path).expect("fixture metadata");
        let inode = source_inode_for_file(&source_file, &meta);
        let committed = Checkpoint {
            source_name: work.source_name.clone(),
            source_file: source_file.clone(),
            source_inode: inode,
            source_generation: 1,
            last_offset: (header.len() + 1) as u64,
            last_line_no: 1,
            status: CheckpointLifecycle::Active.to_string(),
            ..Default::default()
        };
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        {
            let mut guard = checkpoints.write().await;
            guard.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &source_file),
                committed,
            );
        }

        let config = moraine_config::AppConfig::default();
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("resumed codex file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 1, "only the tail record re-emits");
        assert_eq!(
            batch.raw_rows[0].get("cwd").and_then(Value::as_str),
            Some("/repo"),
            "resumed records inherit the session cwd from the file head"
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_primes_omp_subagent_session_id_before_leading_title() {
        let path = unique_test_file("omp-named-subagent");
        let session_id = "019f4be3-7d9d-7005-85e0-d9527b0aad24";
        fs::write(
            &path,
            [
                json!({
                    "type": "title",
                    "v": 1,
                    "title": "ReviewCorrectness",
                    "updatedAt": "2026-07-10T11:57:07.869Z"
                })
                .to_string(),
                json!({
                    "type": "session",
                    "version": 3,
                    "id": session_id,
                    "timestamp": "2026-07-10T11:57:07.869Z",
                    "cwd": "/work/omp-project"
                })
                .to_string(),
                json!({
                    "type": "mode_change",
                    "id": "mode-1",
                    "parentId": null,
                    "timestamp": "2026-07-10T11:57:07.870Z",
                    "mode": "goal"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write OMP subagent fixture");

        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "omp".to_string(),
            harness: "pi-coding-agent".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file,
            trigger: WorkTrigger::Watcher,
        };
        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("OMP subagent file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 3);
        assert_eq!(batch.event_rows.len(), 3);
        assert!(batch
            .raw_rows
            .iter()
            .all(|row| { row.get("session_id").and_then(Value::as_str) == Some(session_id) }));
        assert!(batch
            .event_rows
            .iter()
            .all(|row| { row.get("session_id").and_then(Value::as_str) == Some(session_id) }));
        assert!(batch
            .raw_rows
            .iter()
            .all(|row| { row.get("cwd").and_then(Value::as_str) == Some("/work/omp-project") }));

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_infers_leading_claude_metadata_timestamp() {
        let path = unique_test_file("claude-leading-metadata");
        fs::write(
            &path,
            [
                json!({
                    "type": "permission-mode",
                    "sessionId": "session-with-leading-metadata",
                    "permissionMode": "acceptEdits"
                })
                .to_string(),
                json!({
                    "type": "file-history-snapshot",
                    "messageId": "msg_1",
                    "isSnapshotUpdate": true,
                    "snapshot": {}
                })
                .to_string(),
                json!({
                    "type": "user",
                    "timestamp": "2026-04-18T20:43:51.069Z",
                    "uuid": "00a635eb-f13f-4a0e-9898-a3ad7b71ca47",
                    "parentUuid": null,
                    "sessionId": "session-with-leading-metadata",
                    "message": {
                        "role": "user",
                        "content": "hello"
                    }
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .expect("write claude metadata fixture");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "claude".to_string(),
            harness: "claude-code".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file,
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("claude file should process");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 3);
        assert!(
            batch
                .error_rows
                .iter()
                .all(|row| row.get("error_kind").and_then(Value::as_str)
                    != Some("timestamp_parse_error")),
            "leading metadata should inherit the first parseable record timestamp"
        );
        assert_eq!(
            batch.raw_rows[0]
                .get("record_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2026-04-18T20:43:51.069Z"
        );
        assert_eq!(
            batch.raw_rows[1]
                .get("record_ts")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "2026-04-18T20:43:51.069Z"
        );
        assert!(
            batch.event_rows.iter().all(|row| {
                row.get("event_ts").and_then(Value::as_str) == Some("2026-04-18 20:43:51.069")
            }),
            "metadata and message events should share the inferred timestamp"
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_reports_pi_malformed_jsonl_without_dropping_valid_rows() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("fixtures")
            .join("pi")
            .join("malformed.jsonl");
        let source_file = path.to_string_lossy().to_string();

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let work = WorkItem {
            source_name: "pi".to_string(),
            harness: "pi-coding-agent".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file,
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("pi fixture should process around malformed line");

        let batches = drain_batches(&mut sink_rx).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.raw_rows.len(), 2);
        assert_eq!(batch.event_rows.len(), 2);
        assert_eq!(batch.error_rows.len(), 1);
        assert_eq!(
            batch.error_rows[0]
                .get("error_kind")
                .and_then(Value::as_str),
            Some("json_parse_error")
        );
        assert_eq!(
            batch.raw_rows[0].get("harness").and_then(Value::as_str),
            Some("pi-coding-agent")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_quarantines_oversized_jsonl_line_and_continues() {
        let path = unique_test_file("codex-oversized-line");
        let first = json!({
            "type": "session_meta",
            "timestamp": "2026-06-27T10:00:00.000Z",
            "payload": {
                "id": "codex-oversized-line-session",
                "cwd": "/repo"
            }
        })
        .to_string();
        let line_limit = 4096usize;
        let oversized_output = "x".repeat(line_limit + 1024);
        let oversized = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:01.000Z",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_too_large",
                "output": oversized_output,
            }
        })
        .to_string();
        let third = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:02.000Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "after the oversized line"
                    }
                ]
            }
        })
        .to_string();
        let body = format!("{first}\n{oversized}\n{third}\n");
        let oversized_offset = (first.len() + 1) as u64;
        let oversized_line_bytes = oversized.len() + 1;
        let final_offset = body.len() as u64;
        fs::write(&path, body).expect("write oversized codex fixture");

        let mut config = moraine_config::AppConfig::default();
        config.ingest.max_batch_bytes = line_limit;
        assert_eq!(jsonl_source_line_byte_limit(&config), line_limit);

        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("oversized codex file should process around the large line");

        let batches = drain_batches(&mut sink_rx).await;
        assert!(!batches.is_empty(), "expected at least one sink batch");
        let raw_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .collect();
        let event_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.event_rows.iter())
            .collect();
        let error_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter())
            .collect();

        assert_eq!(raw_rows.len(), 2, "the oversized line emits no raw row");
        assert!(
            raw_rows
                .iter()
                .all(|row| row.get("source_line_no").and_then(Value::as_u64) != Some(2)),
            "line 2 must be quarantined instead of normalized"
        );
        assert_eq!(
            raw_rows[1].get("source_line_no").and_then(Value::as_u64),
            Some(3),
            "the line after the oversized record must still normalize"
        );
        assert!(
            event_rows
                .iter()
                .any(|row| row.get("text_content").and_then(Value::as_str)
                    == Some("after the oversized line")),
            "subsequent JSONL lines must continue processing"
        );

        assert_eq!(error_rows.len(), 1);
        let error = error_rows[0];
        assert_eq!(
            error.get("error_kind").and_then(Value::as_str),
            Some(ERROR_KIND_SOURCE_LINE_TOO_LARGE)
        );
        assert_eq!(
            error.get("source_file").and_then(Value::as_str),
            Some(source_file.as_str())
        );
        assert_eq!(error.get("source_line_no").and_then(Value::as_u64), Some(2));
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(oversized_offset)
        );
        assert!(error
            .get("error_text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains(&oversized_line_bytes.to_string())));
        let raw_fragment = error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .expect("oversized line error should include compact metadata");
        assert!(
            raw_fragment.len() < 256,
            "oversized quarantine metadata must stay compact"
        );
        let fragment: Value =
            serde_json::from_str(raw_fragment).expect("raw_fragment should be JSON metadata");
        assert_eq!(
            fragment.get("line_bytes").and_then(Value::as_u64),
            Some(oversized_line_bytes as u64)
        );
        assert_eq!(
            fragment.get("limit_bytes").and_then(Value::as_u64),
            Some(line_limit as u64)
        );
        assert_eq!(
            fragment.get("action").and_then(Value::as_str),
            Some("skipped")
        );

        let final_checkpoint = batches
            .iter()
            .filter_map(|batch| batch.checkpoint.as_ref())
            .max_by_key(|checkpoint| checkpoint.last_offset)
            .expect("oversized line processing should emit a checkpoint");
        assert_eq!(final_checkpoint.last_offset, final_offset);
        assert_eq!(final_checkpoint.last_line_no, 3);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_quarantines_rows_that_expand_past_clickhouse_object_limit() {
        let path = unique_test_file("codex-expanded-row-too-large");
        let first = json!({
            "type": "session_meta",
            "timestamp": "2026-06-27T10:00:00.000Z",
            "payload": {
                "id": "codex-expanded-row-too-large-session",
                "cwd": "/repo"
            }
        })
        .to_string();
        let backslash_count = (CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT / 4) + 200_000;
        let escaped_heavy_output = "\\".repeat(backslash_count);
        let expanded = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:01.000Z",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_expands_too_large",
                "output": escaped_heavy_output,
            }
        })
        .to_string();
        assert!(
            expanded.len() < jsonl_source_line_byte_limit(&moraine_config::AppConfig::default()),
            "fixture must stay below the source-line cap to exercise serialized row sizing"
        );
        let third = json!({
            "type": "response_item",
            "timestamp": "2026-06-27T10:00:02.000Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "after the expanded row"
                    }
                ]
            }
        })
        .to_string();
        let body = format!("{first}\n{expanded}\n{third}\n");
        let expanded_offset = (first.len() + 1) as u64;
        let expanded_line_bytes = expanded.len() + 1;
        let final_offset = body.len() as u64;
        fs::write(&path, body).expect("write expanded-row codex fixture");

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);
        let source_file = path.to_string_lossy().to_string();
        let work = WorkItem {
            source_name: "codex".to_string(),
            harness: "codex".to_string(),
            format: SourceFormat::Jsonl,
            source_glob: String::new(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };

        process_file(
            &config,
            &work,
            checkpoints,
            &VolatilePollMap::new(),
            sink_tx,
            &metrics,
        )
        .await
        .expect("expanded-row codex file should process around the unsafe row");

        let batches = drain_batches(&mut sink_rx).await;
        assert!(!batches.is_empty(), "expected at least one sink batch");
        let raw_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.raw_rows.iter())
            .collect();
        let event_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.event_rows.iter())
            .collect();
        let error_rows: Vec<&Value> = batches
            .iter()
            .flat_map(|batch| batch.error_rows.iter())
            .collect();

        assert_eq!(raw_rows.len(), 2, "the expanded row emits no raw row");
        assert!(
            raw_rows
                .iter()
                .all(|row| row.get("source_line_no").and_then(Value::as_u64) != Some(2)),
            "line 2 must be quarantined instead of inserted"
        );
        assert!(
            event_rows
                .iter()
                .any(|row| row.get("text_content").and_then(Value::as_str)
                    == Some("after the expanded row")),
            "subsequent JSONL lines must continue processing"
        );

        assert_eq!(error_rows.len(), 1);
        let error = error_rows[0];
        assert_eq!(
            error.get("error_kind").and_then(Value::as_str),
            Some(ERROR_KIND_NORMALIZED_ROW_TOO_LARGE)
        );
        assert_eq!(
            error.get("source_offset").and_then(Value::as_u64),
            Some(expanded_offset)
        );
        let raw_fragment = error
            .get("raw_fragment")
            .and_then(Value::as_str)
            .expect("expanded row error should include compact metadata");
        assert!(
            raw_fragment.len() < 320,
            "expanded-row quarantine metadata must stay compact"
        );
        let fragment: Value =
            serde_json::from_str(raw_fragment).expect("raw_fragment should be JSON metadata");
        assert_eq!(
            fragment.get("line_bytes").and_then(Value::as_u64),
            Some(expanded_line_bytes as u64)
        );
        assert!(
            fragment
                .get("serialized_row_bytes")
                .and_then(Value::as_u64)
                .is_some_and(|bytes| bytes > CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT as u64),
            "quarantine should record the unsafe serialized row size"
        );
        assert_eq!(
            fragment.get("limit_bytes").and_then(Value::as_u64),
            Some(CLICKHOUSE_JSON_OBJECT_BYTE_LIMIT as u64)
        );

        let final_checkpoint = batches
            .iter()
            .filter_map(|batch| batch.checkpoint.as_ref())
            .max_by_key(|checkpoint| checkpoint.last_offset)
            .expect("expanded-row processing should emit a checkpoint");
        assert_eq!(final_checkpoint.last_offset, final_offset);
        assert_eq!(final_checkpoint.last_line_no, 3);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_session_json_emits_only_new_messages_on_growth() {
        let path = unique_session_file("growth");
        let source_file = path.to_string_lossy().to_string();

        // First snapshot: just a user turn.
        let msgs_v1 = vec![serde_json::json!({
            "role": "user",
            "content": "hello"
        })];
        write_session_file(&path, &msgs_v1);

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        let work = WorkItem {
            source_name: "hermes-live".to_string(),
            harness: "hermes".to_string(),
            format: SourceFormat::SessionJson,
            source_glob: String::new(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };

        process_session_json_file(
            &config,
            &work,
            checkpoints.clone(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("first session_json run");

        let batches_v1 = drain_batches(&mut sink_rx).await;
        assert_eq!(batches_v1.len(), 1, "single flushed batch on first run");
        let b1 = &batches_v1[0];
        // session_meta + user message = 2 event rows.
        assert_eq!(b1.event_rows.len(), 2, "session_meta + user message events");
        assert_eq!(
            b1.checkpoint.as_ref().expect("checkpoint").last_line_no,
            1,
            "checkpoint advances to message_count=1",
        );
        // Apply the checkpoint like the sink would.
        let cp = b1.checkpoint.as_ref().unwrap().clone();
        {
            let mut guard = checkpoints.write().await;
            guard.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &source_file),
                cp,
            );
        }

        let first_uids: Vec<String> = b1
            .event_rows
            .iter()
            .map(|r| {
                r.get("event_uid")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
            .collect();

        // Grow the file to 2 messages. We intentionally rewrite via plain write
        // (no atomic rename here since that's already covered by the fact that
        // we pin SESSION_JSON_INODE=0).
        let msgs_v2 = vec![
            serde_json::json!({ "role": "user", "content": "hello" }),
            serde_json::json!({ "role": "assistant", "content": "hi there" }),
        ];
        write_session_file(&path, &msgs_v2);

        process_session_json_file(&config, &work, checkpoints.clone(), sink_tx, &metrics)
            .await
            .expect("second session_json run");

        let batches_v2 = drain_batches(&mut sink_rx).await;
        assert_eq!(batches_v2.len(), 1, "second run flushed a single batch");
        let b2 = &batches_v2[0];
        // Only the newly-appeared assistant message should emit this time
        // (session_meta was already emitted on the first run).
        assert_eq!(
            b2.event_rows.len(),
            1,
            "only the new assistant message emits on the second run",
        );
        assert_eq!(
            b2.event_rows[0].get("actor_kind").and_then(Value::as_str),
            Some("assistant"),
        );
        assert_eq!(
            b2.checkpoint.as_ref().expect("checkpoint").last_line_no,
            2,
            "checkpoint advances to message_count=2",
        );

        // New row's uid must not collide with any of the first-run uids.
        let new_uid = b2.event_rows[0]
            .get("event_uid")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        assert!(
            !first_uids.contains(&new_uid),
            "second-run uid collides with first-run",
        );

        // Sanity: pinned synthetic inode/generation preserved on the checkpoint.
        let cp2 = b2.checkpoint.as_ref().unwrap();
        assert_eq!(cp2.source_inode, SESSION_JSON_INODE);
        assert_eq!(cp2.source_generation, SESSION_JSON_GENERATION);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_session_json_is_a_noop_when_nothing_changes() {
        let path = unique_session_file("noop");
        let source_file = path.to_string_lossy().to_string();
        let msgs = vec![serde_json::json!({
            "role": "user",
            "content": "stable"
        })];
        write_session_file(&path, &msgs);

        let config = moraine_config::AppConfig::default();
        let checkpoints = Arc::new(RwLock::new(HashMap::<String, Checkpoint>::new()));
        let metrics = Arc::new(Metrics::default());
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMessage>(16);

        let work = WorkItem {
            source_name: "hermes-live".to_string(),
            harness: "hermes".to_string(),
            format: SourceFormat::SessionJson,
            source_glob: String::new(),
            path: source_file.clone(),
            trigger: WorkTrigger::Watcher,
        };

        process_session_json_file(
            &config,
            &work,
            checkpoints.clone(),
            sink_tx.clone(),
            &metrics,
        )
        .await
        .expect("first run");
        let first = drain_batches(&mut sink_rx).await;
        assert_eq!(first.len(), 1);
        let cp = first[0].checkpoint.as_ref().unwrap().clone();
        {
            let mut guard = checkpoints.write().await;
            guard.insert(
                crate::checkpoint::checkpoint_key(&work.source_name, &source_file),
                cp,
            );
        }

        // Second run on unchanged file → no batches sent.
        process_session_json_file(&config, &work, checkpoints.clone(), sink_tx, &metrics)
            .await
            .expect("second run");
        let second = drain_batches(&mut sink_rx).await;
        assert!(
            second.is_empty(),
            "unchanged file should produce zero batches; got {} batches",
            second.len(),
        );

        let _ = fs::remove_file(&path);
    }

    fn event_row(session_id: &str, event_ts: &str, event_kind: &str, actor_kind: &str) -> Value {
        json!({
            "session_id": session_id,
            "event_ts": event_ts,
            "event_kind": event_kind,
            "actor_kind": actor_kind,
            "latency_ms": 0u32,
        })
    }

    fn latency_of(row: &Value) -> u64 {
        row.get("latency_ms").and_then(|v| v.as_u64()).unwrap_or(0)
    }

    #[test]
    fn latency_enrichment_stamps_assistant_after_tool_result() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s1";

        // 1) tool_result at T0.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "tool_result",
            "tool",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);
        assert_eq!(latency_of(&rows[0]), 0, "tool_result itself is untouched");

        // 2) assistant turn 4.25s later: thinking + tool_use, same event_ts.
        let mut rows = vec![
            event_row(session, "2026-04-19 12:00:04.250", "reasoning", "assistant"),
            event_row(session, "2026-04-19 12:00:04.250", "tool_call", "assistant"),
        ];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);
        assert_eq!(
            latency_of(&rows[0]),
            4250,
            "first assistant block carries the model latency"
        );
        assert_eq!(
            latency_of(&rows[1]),
            0,
            "subsequent blocks in the same turn are not double-stamped"
        );
    }

    #[test]
    fn latency_enrichment_skips_fresh_user_prompt() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s2";

        // User typed a prompt.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "message",
            "user",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        // Assistant replies 10s later — gap is human typing + model time.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:10.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        assert_eq!(
            latency_of(&rows[0]),
            0,
            "assistant after fresh user prompt must not be stamped (ambiguous wait)"
        );
    }

    #[test]
    fn latency_enrichment_resets_after_user_breaks_chain() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s3";

        // tool_result → user prompt → assistant: chain broken by user.
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "tool_result",
            "tool",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:05.000",
            "message",
            "user",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:07.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        assert_eq!(
            latency_of(&rows[0]),
            0,
            "intervening user prompt breaks the tool_result → assistant chain"
        );
    }

    #[test]
    fn latency_enrichment_skips_non_claude_harness() {
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s4";

        // Seed cursor as if a tool_result happened.
        cursors.insert(
            session.to_string(),
            SessionCursor {
                prev_event_ts_ms: 1_000,
                prev_was_tool_result: true,
            },
        );

        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:10.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("codex", &mut rows, &mut cursors);
        assert_eq!(latency_of(&rows[0]), 0, "non-claude harness is a no-op");
    }

    #[test]
    fn latency_enrichment_ignores_system_events_when_advancing_cursor() {
        // A progress/system event between tool_result and assistant must
        // NOT reset the cursor — otherwise we'd lose valid latency data.
        let mut cursors: HashMap<String, SessionCursor> = HashMap::new();
        let session = "s5";

        // 1) tool_result
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.000",
            "tool_result",
            "tool",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        // 2) out-of-band system event (no turn actor)
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:00.500",
            "system",
            "system",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        // 3) assistant response 3s after the tool_result
        let mut rows = vec![event_row(
            session,
            "2026-04-19 12:00:03.000",
            "message",
            "assistant",
        )];
        enrich_claude_model_latency("claude-code", &mut rows, &mut cursors);

        assert_eq!(
            latency_of(&rows[0]),
            3000,
            "system event should not reset the tool_result → assistant chain"
        );
    }
}
