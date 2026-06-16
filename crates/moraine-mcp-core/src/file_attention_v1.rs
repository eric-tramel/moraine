//! `file_attention` (Phase 0 / Tier 0): every captured session that touched a
//! file, across every worktree, drillable through `open`.
//!
//! Given a path, this suffix-matches the repo-relative tail against the raw
//! `file_path` (and `notebook_path` / `path`) recorded in `tool_io`, plus a
//! substring fallback for shell commands. Matching the tail is what unifies the
//! main checkout, sibling worktrees, and agent-isolation worktrees (which share
//! no leading path) into one answer — including work that never landed in git.
//! It returns typed `session:` / `event:` IDs that drill down through `open`;
//! it never reinvents inspection.

use super::{tool_ok_hybrid, AppState};
use crate::contract::{
    format_rfc3339_utc_millis, CanonicalFileAttentionArgs, ContractError, FileAttentionArgs,
    FileAttentionGranularity, FileAttentionScope, McpEventId, McpSessionId, McpTurnId, Performance,
    ToolEnvelope, ToolErrorCode, ToolErrorEnvelope, FILE_ATTENTION_BROAD_SLA_TARGET_MS,
    FILE_ATTENTION_DEADLINE_MS, FILE_ATTENTION_DEFAULT_SLA_TARGET_MS,
    FILE_ATTENTION_MIN_TAIL_SEGMENTS, FILE_ATTENTION_TOOL,
};
use anyhow::{Context, Result};
use moraine_conversations::{FileAttentionQuery, FileAttentionTouch, RepoError};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use tokio::time::{timeout, Duration};
use tracing::warn;

/// Hard cap on matched rows pulled from ClickHouse for one query. The summary,
/// root breakdown, and per-session rollups are computed over this scanned set;
/// when it is hit the response is flagged truncated. Far above any realistic
/// per-file touch count, but bounds memory on a pathological tail.
const FILE_ATTENTION_SCAN_CAP: usize = 2_000;

/// Marker files that identify a repo (worktree) root when stripping an absolute
/// query path to its repo-relative tail. `.moraine.toml` is the committed,
/// per-project moraine backend reference; `.git` (a dir in the main checkout, a
/// file in a worktree) is the universal fallback.
const REPO_ROOT_MARKERS: [&str; 2] = [".moraine.toml", ".git"];

impl AppState {
    pub(crate) async fn file_attention_v1(&self, arguments: Value) -> Result<Value> {
        let perf = Performance::builder(FILE_ATTENTION_DEFAULT_SLA_TARGET_MS);
        let raw_request = arguments.clone();

        let args = match parse_file_attention_args(arguments, self.cfg.mcp.max_results) {
            Ok(args) => args,
            Err(error) => return encode_error(raw_request, error, perf.finish()),
        };
        let canonical_request = canonical_request_json(&args);

        // A full-history scan (no datetime window) is the broad case and earns a
        // looser target than a windowed lookup.
        let has_window = args.start_unix_ms.is_some() || args.end_unix_ms.is_some();
        let perf = perf.with_sla_target(if has_window {
            FILE_ATTENTION_DEFAULT_SLA_TARGET_MS
        } else {
            FILE_ATTENTION_BROAD_SLA_TARGET_MS
        });

        let mut warnings: Vec<String> = Vec::new();
        let tail = resolve_tail(&args.path);
        if tail.tail_is_absolute {
            warnings.push(format!(
                "could not reduce {:?} to a repo-relative tail (no .moraine.toml/.git marker found above it); matching the absolute path literally, so other worktrees of the same file will not be unified. Pass a repo-relative path for cross-worktree coverage.",
                args.path
            ));
        }
        let depth = tail_segments(&tail.rel);
        if depth < FILE_ATTENTION_MIN_TAIL_SEGMENTS {
            warnings.push(format!(
                "{:?} is a generic tail (depth {depth}); results may include unrelated files. Pass a longer repo-relative path, and check the surfaced roots.",
                tail.rel
            ));
        }
        if args.scope == FileAttentionScope::Project && self.repo.config().session_scope.is_none() {
            // The default scope honors `--project-only`; without it the server
            // sees the whole backend, so "project" cannot narrow further.
            warnings.push(
                "scope=\"project\" but this server is not project-scoped (not launched with --project-only); results may span every project in the backend. The surfaced roots show the spread."
                    .to_string(),
            );
        }

        let repo_query = FileAttentionQuery {
            rel: tail.rel.clone(),
            apply_project_scope: args.scope == FileAttentionScope::Project,
            start_unix_ms: args.start_unix_ms,
            end_unix_ms: args.end_unix_ms,
            tool: args.tool.clone(),
            mutations_only: args.mutations_only,
            max_rows: FILE_ATTENTION_SCAN_CAP,
        };

        let touches = match timeout(
            Duration::from_millis(FILE_ATTENTION_DEADLINE_MS),
            self.repo.file_attention(repo_query),
        )
        .await
        {
            Ok(Ok(touches)) => touches,
            Ok(Err(error)) => {
                return encode_error(
                    canonical_request,
                    repo_error_to_contract_error(error),
                    perf.finish(),
                )
            }
            Err(_) => {
                return encode_error(
                    canonical_request,
                    ContractError::new(
                        ToolErrorCode::DeadlineExceeded,
                        "file_attention exceeded its response deadline",
                    )
                    .with_details(json!({ "deadline_ms": FILE_ATTENTION_DEADLINE_MS })),
                    perf.finish(),
                )
            }
        };

        let performance = perf.finish();
        let data = build_data(&args, &tail, touches, &mut warnings);

        let payload = serde_json::to_value(
            ToolEnvelope::success(FILE_ATTENTION_TOOL, canonical_request, data, performance)
                .with_warnings(warnings),
        )
        .context("failed to encode file_attention response envelope")?;
        Ok(tool_ok_hybrid(format_text(&payload), payload))
    }
}

fn parse_file_attention_args(
    arguments: Value,
    max_results: u16,
) -> Result<CanonicalFileAttentionArgs, ContractError> {
    serde_json::from_value::<FileAttentionArgs>(arguments)
        .map_err(|error| {
            ContractError::new(
                ToolErrorCode::InvalidRequest,
                "file_attention expects a JSON object with a required path",
            )
            .with_details(json!({ "serde_error": error.to_string() }))
        })?
        .validate(max_results)
}

fn canonical_request_json(args: &CanonicalFileAttentionArgs) -> Value {
    json!({
        "path": args.path,
        "scope": args.scope.as_str(),
        "granularity": args.granularity.as_str(),
        "start_datetime": args.start_datetime,
        "end_datetime": args.end_datetime,
        "tool": args.tool,
        "mutations_only": args.mutations_only,
        "limit": args.limit,
    })
}

/// The repo-relative tail a query path reduces to, plus the launch-side root it
/// was stripped against (informational).
#[derive(Debug, Clone, PartialEq, Eq)]
struct TailResolution {
    rel: String,
    root: Option<String>,
    /// The path was absolute and could not be reduced to a repo-relative tail,
    /// so `rel` is the absolute path matched literally.
    tail_is_absolute: bool,
}

/// Reduce a query `path` to the repo-relative tail used for suffix matching.
///
/// - A relative path is the tail as given.
/// - An absolute path is stripped of its worktree root, found by walking up to
///   a `.moraine.toml` / `.git` marker. When no marker is found (e.g. the file
///   was deleted, or lives in a foreign root), the absolute path is matched
///   literally and `tail_is_absolute` is set so the caller can warn.
fn resolve_tail(path: &str) -> TailResolution {
    let cleaned = path.trim();
    let cleaned = cleaned.strip_prefix("./").unwrap_or(cleaned);
    if !cleaned.starts_with('/') {
        return TailResolution {
            rel: cleaned.trim_start_matches('/').to_string(),
            root: None,
            tail_is_absolute: false,
        };
    }

    if let Some(root) = find_project_root(Path::new(cleaned)) {
        if let Some(rel) = strip_root(cleaned, &root) {
            if !rel.is_empty() {
                return TailResolution {
                    rel,
                    root: Some(root),
                    tail_is_absolute: false,
                };
            }
        }
    }

    TailResolution {
        rel: cleaned.to_string(),
        root: None,
        tail_is_absolute: true,
    }
}

/// Strip `root` (with one joining slash) from the front of `path`. Pure; no
/// filesystem access.
fn strip_root(path: &str, root: &str) -> Option<String> {
    let root = root.trim_end_matches('/');
    if root.is_empty() {
        return None;
    }
    path.strip_prefix(&format!("{root}/"))
        .map(|tail| tail.to_string())
}

/// Walk up from a file path's parent directory looking for a repo-root marker,
/// bounded at `$HOME` or the filesystem root. Returns the marker directory.
fn find_project_root(file_path: &Path) -> Option<String> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let mut dir = file_path.parent();
    while let Some(current) = dir {
        if REPO_ROOT_MARKERS
            .iter()
            .any(|marker| current.join(marker).exists())
        {
            return Some(current.to_string_lossy().to_string());
        }
        if home.as_deref() == Some(current) {
            break;
        }
        dir = current.parent();
    }
    None
}

/// Count non-empty, slash-separated segments of a tail. `mod.rs` is depth 1;
/// `src/lib.rs` is depth 2.
fn tail_segments(rel: &str) -> usize {
    rel.split('/').filter(|segment| !segment.is_empty()).count()
}

/// Per-session accumulator built while folding the time-ordered touch stream.
#[derive(Default)]
struct SessionAgg {
    first_ms: i64,
    last_ms: i64,
    touch_count: u64,
    tools: BTreeSet<String>,
    roots: BTreeSet<String>,
    match_kinds: BTreeSet<String>,
    harness: String,
    /// The most recent touch's event_uid (first seen, since rows arrive
    /// newest-first) — the handle that drills straight to the latest touch.
    latest_event_uid: String,
    latest_turn_index: u32,
    latest_has_ts: bool,
}

fn build_data(
    args: &CanonicalFileAttentionArgs,
    tail: &TailResolution,
    mut touches: Vec<FileAttentionTouch>,
    warnings: &mut Vec<String>,
) -> Value {
    let scan_truncated = touches.len() > FILE_ATTENTION_SCAN_CAP;
    if scan_truncated {
        touches.truncate(FILE_ATTENTION_SCAN_CAP);
        warnings.push(format!(
            "matched more than {FILE_ATTENTION_SCAN_CAP} touches; summary and roots are computed over the {FILE_ATTENTION_SCAN_CAP} most recent. Narrow with start_datetime/end_datetime or tool."
        ));
    }

    // --- summary + roots, over the full scanned set --------------------------
    let total_touches = touches.len() as u64;
    let mut first_ms: Option<i64> = None;
    let mut last_ms: Option<i64> = None;
    let mut distinct_sessions: BTreeSet<&str> = BTreeSet::new();
    let mut root_stats: HashMap<String, (u64, BTreeSet<String>)> = HashMap::new();
    for touch in &touches {
        if touch.event_unix_ms > 0 {
            first_ms = Some(first_ms.map_or(touch.event_unix_ms, |v| v.min(touch.event_unix_ms)));
            last_ms = Some(last_ms.map_or(touch.event_unix_ms, |v| v.max(touch.event_unix_ms)));
        }
        distinct_sessions.insert(touch.session_id.as_str());
        let key = root_label(&touch.worktree_root);
        let entry = root_stats.entry(key).or_default();
        entry.0 += 1;
        entry.1.insert(touch.session_id.clone());
    }

    let distinct_known_roots = root_stats
        .keys()
        .filter(|root| root.as_str() != UNKNOWN_ROOT)
        .count();
    let ambiguous = distinct_known_roots > 1;

    let mut roots: Vec<Value> = root_stats
        .into_iter()
        .map(|(root, (touch_count, sessions))| {
            json!({
                "root": root,
                "touch_count": touch_count,
                "session_count": sessions.len(),
            })
        })
        .collect();
    roots.sort_by(|a, b| {
        b["touch_count"]
            .as_u64()
            .cmp(&a["touch_count"].as_u64())
            .then_with(|| a["root"].as_str().cmp(&b["root"].as_str()))
    });

    let summary = json!({
        "total_touches": total_touches,
        "distinct_sessions": distinct_sessions.len(),
        "distinct_roots": distinct_known_roots,
        "first_touch": first_ms.map(format_rfc3339_utc_millis),
        "last_touch": last_ms.map(format_rfc3339_utc_millis),
        "ambiguous": ambiguous,
        "scan_truncated": scan_truncated,
    });

    // --- body: per-session rollups OR a flat event timeline ------------------
    let limit = args.limit as usize;
    let (body_key, body, available) = match args.granularity {
        FileAttentionGranularity::Sessions => {
            let (sessions, total) = session_rollups(&touches, limit);
            ("sessions", sessions, total)
        }
        FileAttentionGranularity::Events => {
            let (events, total) = event_timeline(&touches, limit);
            ("events", events, total)
        }
    };
    let result_count = body.len();
    let truncated = available > result_count;

    let mut data = json!({
        "path": args.path,
        "tail": tail.rel,
        "tail_is_absolute": tail.tail_is_absolute,
        "stripped_root": tail.root,
        "scope": args.scope.as_str(),
        "granularity": args.granularity.as_str(),
        "summary": summary,
        "roots": roots,
        "result_count": result_count,
        "limit": args.limit,
        "truncated": truncated,
    });
    // The body lands under a granularity-dependent key, which `json!` cannot
    // template, so insert it after the fact.
    if let Some(object) = data.as_object_mut() {
        object.insert(body_key.to_string(), Value::Array(body));
    }
    data
}

const UNKNOWN_ROOT: &str = "(unknown)";

fn root_label(root: &str) -> String {
    if root.is_empty() {
        UNKNOWN_ROOT.to_string()
    } else {
        root.to_string()
    }
}

/// Fold the newest-first touch stream into per-session rollups, preserving
/// most-recent-first order. Returns `(displayed_rollups, total_sessions)`.
fn session_rollups(touches: &[FileAttentionTouch], limit: usize) -> (Vec<Value>, usize) {
    let mut order: Vec<String> = Vec::new();
    let mut aggs: HashMap<String, SessionAgg> = HashMap::new();
    for touch in touches {
        let agg = aggs.entry(touch.session_id.clone()).or_insert_with(|| {
            order.push(touch.session_id.clone());
            SessionAgg {
                first_ms: i64::MAX,
                last_ms: i64::MIN,
                latest_event_uid: touch.event_uid.clone(),
                latest_turn_index: touch.turn_index,
                latest_has_ts: touch.event_unix_ms > 0,
                harness: touch.harness.clone(),
                ..SessionAgg::default()
            }
        });
        agg.touch_count += 1;
        if !touch.tool_name.is_empty() {
            agg.tools.insert(touch.tool_name.clone());
        }
        if !touch.worktree_root.is_empty() {
            agg.roots.insert(touch.worktree_root.clone());
        }
        if !touch.match_kind.is_empty() {
            agg.match_kinds.insert(touch.match_kind.clone());
        }
        if agg.harness.is_empty() {
            agg.harness = touch.harness.clone();
        }
        if touch.event_unix_ms > 0 {
            agg.first_ms = agg.first_ms.min(touch.event_unix_ms);
            agg.last_ms = agg.last_ms.max(touch.event_unix_ms);
        }
    }

    let total = order.len();
    let mut sessions = Vec::new();
    for session_id in order {
        if sessions.len() >= limit {
            break;
        }
        let agg = &aggs[&session_id];
        // Rank advances only on a kept row so it stays contiguous when a row's
        // identifier can't be encoded (repository data fault, not user input).
        let rank = sessions.len() + 1;
        match session_rollup_json(rank, &session_id, agg) {
            Ok(value) => sessions.push(value),
            Err(error) => warn!(
                session_id = %session_id,
                error = %error,
                "file_attention: skipping session rollup with an invalid identifier"
            ),
        }
    }
    (sessions, total)
}

fn session_rollup_json(
    rank: usize,
    session_id: &str,
    agg: &SessionAgg,
) -> Result<Value, ContractError> {
    let mcp_session_id = McpSessionId::from_raw_session_id(session_id)
        .map(|id| id.to_string())
        .map_err(internal_id_error)?;
    let latest_event_id = McpEventId::from_raw_event_uid(agg.latest_event_uid.as_str())
        .map(|id| id.to_string())
        .map_err(internal_id_error)?;

    let first = (agg.first_ms != i64::MAX).then_some(agg.first_ms);
    let last = (agg.last_ms != i64::MIN).then_some(agg.last_ms);

    let mut open = json!({
        "session_id": mcp_session_id,
        "event_id": latest_event_id,
    });
    if agg.latest_has_ts {
        if let Ok(turn_id) =
            McpTurnId::from_raw_session_id_and_turn_seq(session_id, agg.latest_turn_index)
        {
            open["turn_id"] = json!(turn_id.to_string());
        }
    }

    Ok(json!({
        "rank": rank,
        "id": mcp_session_id,
        "session": {
            "id": mcp_session_id,
            "harness": agg.harness,
            "first_touch": first.map(format_rfc3339_utc_millis),
            "last_touch": last.map(format_rfc3339_utc_millis),
            "touch_count": agg.touch_count,
            "tools": agg.tools.iter().collect::<Vec<_>>(),
            "worktree_roots": agg.roots.iter().collect::<Vec<_>>(),
            "match_kinds": agg.match_kinds.iter().collect::<Vec<_>>(),
        },
        "open": open,
    }))
}

/// The flat, newest-first touch-by-touch timeline. Returns
/// `(displayed_events, total_events)`.
fn event_timeline(touches: &[FileAttentionTouch], limit: usize) -> (Vec<Value>, usize) {
    let total = touches.len();
    let mut events = Vec::new();
    for touch in touches {
        if events.len() >= limit {
            break;
        }
        let rank = events.len() + 1;
        match event_json(rank, touch) {
            Ok(value) => events.push(value),
            Err(error) => warn!(
                event_uid = %touch.event_uid,
                error = %error,
                "file_attention: skipping touch with an invalid identifier"
            ),
        }
    }
    (events, total)
}

fn event_json(rank: usize, touch: &FileAttentionTouch) -> Result<Value, ContractError> {
    let event_id = McpEventId::from_raw_event_uid(touch.event_uid.as_str())
        .map(|id| id.to_string())
        .map_err(internal_id_error)?;
    let session_id = McpSessionId::from_raw_session_id(touch.session_id.as_str())
        .map(|id| id.to_string())
        .map_err(internal_id_error)?;

    let has_ts = touch.event_unix_ms > 0;
    let mut open = json!({
        "event_id": event_id,
        "session_id": session_id,
    });
    if has_ts {
        if let Ok(turn_id) =
            McpTurnId::from_raw_session_id_and_turn_seq(touch.session_id.as_str(), touch.turn_index)
        {
            open["turn_id"] = json!(turn_id.to_string());
        }
    }

    Ok(json!({
        "rank": rank,
        "id": event_id,
        "event": {
            "id": event_id,
            "session_id": session_id,
            "timestamp": has_ts.then(|| format_rfc3339_utc_millis(touch.event_unix_ms)),
            "tool_name": touch.tool_name,
            "phase": touch.tool_phase,
            "turn": has_ts.then_some(touch.turn_index),
            "match_kind": touch.match_kind,
            "worktree_root": (!touch.worktree_root.is_empty()).then_some(touch.worktree_root.as_str()),
            "action_preview": action_preview(touch),
        },
        "open": open,
    }))
}

/// A short, human-oriented preview of what the touch did, drawn from the
/// previews already on `tool_io` (we do not re-parse diffs in Phase 0).
fn action_preview(touch: &FileAttentionTouch) -> Option<String> {
    let raw = if !touch.input_preview.is_empty() {
        &touch.input_preview
    } else if !touch.output_preview.is_empty() {
        &touch.output_preview
    } else {
        return None;
    };
    let collapsed: String = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX: usize = 160;
    if collapsed.chars().count() > MAX {
        Some(format!(
            "{}…",
            collapsed.chars().take(MAX).collect::<String>()
        ))
    } else {
        Some(collapsed)
    }
}

fn encode_error(request: Value, error: ContractError, performance: Performance) -> Result<Value> {
    let payload = serde_json::to_value(ToolErrorEnvelope::error(
        FILE_ATTENTION_TOOL,
        request,
        error,
        performance,
    ))
    .context("failed to encode file_attention error envelope")?;
    Ok(error_hybrid(format_error_text(&payload), payload))
}

fn error_hybrid(text: String, payload: Value) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": payload,
        "isError": false
    })
}

fn format_text(payload: &Value) -> String {
    let data = payload.get("data").unwrap_or(&Value::Null);
    let tail = data.get("tail").and_then(Value::as_str).unwrap_or("");
    let scope = data
        .get("scope")
        .and_then(Value::as_str)
        .unwrap_or("project");
    let granularity = data
        .get("granularity")
        .and_then(Value::as_str)
        .unwrap_or("sessions");
    let summary = data.get("summary").unwrap_or(&Value::Null);
    let total = summary
        .get("total_touches")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let sessions = summary
        .get("distinct_sessions")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let roots = summary
        .get("distinct_roots")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let mut lines = vec![format!(
        "file_attention {tail} — {total} touch(es) across {sessions} session(s) in {roots} worktree root(s) [scope={scope}]."
    )];

    if summary
        .get("ambiguous")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        lines.push("Ambiguous tail: matched more than one worktree root (see roots).".to_string());
    }
    for warning in payload
        .get("warnings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        lines.push(format!("⚠ {warning}"));
    }

    if granularity == "events" {
        if let Some(events) = data.get("events").and_then(Value::as_array) {
            for event in events.iter().take(10) {
                let rank = event.get("rank").and_then(Value::as_u64).unwrap_or(0);
                let ts = event
                    .pointer("/event/timestamp")
                    .and_then(Value::as_str)
                    .unwrap_or("?");
                let tool = event
                    .pointer("/event/tool_name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let id = event
                    .pointer("/open/event_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                lines.push(format!("{rank}. {ts} {tool} ({id})"));
            }
        }
    } else if let Some(sessions) = data.get("sessions").and_then(Value::as_array) {
        for session in sessions.iter().take(10) {
            let rank = session.get("rank").and_then(Value::as_u64).unwrap_or(0);
            let last = session
                .pointer("/session/last_touch")
                .and_then(Value::as_str)
                .unwrap_or("?");
            let count = session
                .pointer("/session/touch_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let harness = session
                .pointer("/session/harness")
                .and_then(Value::as_str)
                .unwrap_or("");
            let id = session
                .pointer("/open/session_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            lines.push(format!("{rank}. {last} {harness} {count} touch(es) ({id})"));
        }
    }

    if data
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        lines.push("More results were truncated by limit.".to_string());
    }

    lines.join("\n")
}

fn format_error_text(payload: &Value) -> String {
    let error = payload.get("error").unwrap_or(&Value::Null);
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("internal_error");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("file_attention failed");
    format!("file_attention error ({code}): {message}")
}

fn repo_error_to_contract_error(error: RepoError) -> ContractError {
    match error {
        RepoError::InvalidArgument(message) | RepoError::InvalidCursor(message) => {
            ContractError::new(ToolErrorCode::InvalidRequest, message)
        }
        RepoError::Backend(message) | RepoError::Internal(message) => {
            ContractError::new(ToolErrorCode::InternalError, message)
        }
    }
}

fn internal_id_error(error: ContractError) -> ContractError {
    ContractError::new(
        ToolErrorCode::InternalError,
        format!("repository returned an invalid MCP identifier component: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(
        session: &str,
        event: &str,
        tool: &str,
        match_kind: &str,
        root: &str,
        ts: i64,
    ) -> FileAttentionTouch {
        FileAttentionTouch {
            session_id: session.to_string(),
            event_uid: event.to_string(),
            harness: "claude-code".to_string(),
            tool_name: tool.to_string(),
            tool_phase: "request".to_string(),
            match_kind: match_kind.to_string(),
            matched_path: if root.is_empty() {
                String::new()
            } else {
                format!("{root}/crates/foo/tee.rs")
            },
            worktree_root: root.to_string(),
            cwd: String::new(),
            event_unix_ms: ts,
            turn_index: 1,
            input_preview: "{\"file_path\":\"crates/foo/tee.rs\"}".to_string(),
            output_preview: String::new(),
        }
    }

    #[test]
    fn strip_root_removes_prefix_with_one_slash() {
        assert_eq!(
            strip_root("/a/b/c/crates/x/file.rs", "/a/b/c"),
            Some("crates/x/file.rs".to_string())
        );
        assert_eq!(
            strip_root("/a/b/c/crates/x/file.rs", "/a/b/c/"),
            Some("crates/x/file.rs".to_string())
        );
        // Not under root.
        assert_eq!(strip_root("/other/crates/x/file.rs", "/a/b/c"), None);
        // Empty root never strips.
        assert_eq!(strip_root("/a/file.rs", ""), None);
    }

    #[test]
    fn resolve_tail_passes_relative_paths_through() {
        let resolved = resolve_tail("crates/foo/tee.rs");
        assert_eq!(resolved.rel, "crates/foo/tee.rs");
        assert!(!resolved.tail_is_absolute);
        assert!(resolved.root.is_none());

        let dotted = resolve_tail("./crates/foo/tee.rs");
        assert_eq!(dotted.rel, "crates/foo/tee.rs");
    }

    #[test]
    fn resolve_tail_strips_absolute_path_to_marker_root() {
        let dir = std::env::temp_dir().join(format!("moraine-fa-test-{}", std::process::id()));
        let root = dir.join("repo");
        let nested = root.join("crates/foo");
        std::fs::create_dir_all(&nested).expect("mkdir nested");
        std::fs::write(root.join(".moraine.toml"), "backend = \"x\"\n").expect("write marker");
        let file = nested.join("tee.rs");
        std::fs::write(&file, "// test").expect("write file");

        let resolved = resolve_tail(file.to_str().expect("utf8 path"));
        assert_eq!(resolved.rel, "crates/foo/tee.rs");
        assert!(!resolved.tail_is_absolute);
        assert_eq!(resolved.root.as_deref(), root.to_str());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_tail_falls_back_to_absolute_when_no_marker() {
        // A path with no marker anywhere above it (a deep temp path) stays
        // absolute and flags itself.
        let resolved = resolve_tail("/nonexistent-xyz/deep/unmarked/file.rs");
        assert!(resolved.tail_is_absolute);
        assert_eq!(resolved.rel, "/nonexistent-xyz/deep/unmarked/file.rs");
    }

    #[test]
    fn tail_segments_counts_non_empty_components() {
        assert_eq!(tail_segments("mod.rs"), 1);
        assert_eq!(tail_segments("src/lib.rs"), 2);
        assert_eq!(tail_segments("crates/foo/tee.rs"), 3);
        assert_eq!(tail_segments("/a/b"), 2);
    }

    fn canonical(
        granularity: FileAttentionGranularity,
        scope: FileAttentionScope,
    ) -> CanonicalFileAttentionArgs {
        CanonicalFileAttentionArgs {
            path: "crates/foo/tee.rs".to_string(),
            scope,
            granularity,
            start_datetime: None,
            end_datetime: None,
            start_unix_ms: None,
            end_unix_ms: None,
            tool: None,
            mutations_only: false,
            limit: 50,
        }
    }

    #[test]
    fn build_data_surfaces_distinct_roots_and_flags_ambiguity() {
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch(
                "sess-main",
                "ev-main",
                "Edit",
                "path_suffix",
                "/repo/main",
                3_000,
            ),
            touch(
                "sess-sib",
                "ev-sib",
                "Edit",
                "path_suffix",
                "/repo/worktrees/foo",
                2_000,
            ),
            touch(
                "sess-iso",
                "ev-iso",
                "Read",
                "path_suffix",
                "/home/.claude/worktrees/agent-x",
                1_000,
            ),
        ];
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Sessions, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );

        assert_eq!(data["summary"]["total_touches"], json!(3));
        assert_eq!(data["summary"]["distinct_sessions"], json!(3));
        assert_eq!(data["summary"]["distinct_roots"], json!(3));
        assert_eq!(data["summary"]["ambiguous"], json!(true));
        // Three distinct roots are surfaced (the acceptance-criteria spread).
        assert_eq!(data["roots"].as_array().expect("roots").len(), 3);
        // Newest-first session rollups.
        assert_eq!(
            data["sessions"][0]["open"]["session_id"],
            json!("session:c2Vzcy1tYWlu")
        );
        assert_eq!(data["sessions"][0]["session"]["touch_count"], json!(1));
        assert!(data["sessions"][0]["open"]["event_id"].as_str().is_some());
    }

    #[test]
    fn build_data_events_granularity_returns_flat_timeline() {
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch("sess-a", "ev-2", "Edit", "path_suffix", "/repo/main", 2_000),
            touch("sess-a", "ev-1", "Read", "path_suffix", "/repo/main", 1_000),
        ];
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Events, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );

        assert_eq!(data["granularity"], json!("events"));
        let events = data["events"].as_array().expect("events");
        assert_eq!(events.len(), 2);
        // One root only → not ambiguous.
        assert_eq!(data["summary"]["ambiguous"], json!(false));
        assert_eq!(data["summary"]["distinct_roots"], json!(1));
        assert_eq!(events[0]["event"]["tool_name"], json!("Edit"));
        assert!(events[0]["open"]["event_id"].as_str().is_some());
        assert!(events[0]["open"]["turn_id"].as_str().is_some());
    }

    #[test]
    fn build_data_respects_display_limit_and_marks_truncated() {
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches: Vec<FileAttentionTouch> = (0..5)
            .map(|i| {
                touch(
                    &format!("sess-{i}"),
                    &format!("ev-{i}"),
                    "Edit",
                    "path_suffix",
                    "/repo/main",
                    (i as i64 + 1) * 1_000,
                )
            })
            .collect();
        let mut args = canonical(FileAttentionGranularity::Sessions, FileAttentionScope::All);
        args.limit = 2;
        let mut warnings = Vec::new();
        let data = build_data(&args, &tail, touches, &mut warnings);

        assert_eq!(data["result_count"], json!(2));
        assert_eq!(data["truncated"], json!(true));
        // Summary still reflects all five scanned sessions.
        assert_eq!(data["summary"]["distinct_sessions"], json!(5));
    }

    #[test]
    fn unknown_root_bucket_does_not_count_toward_ambiguity() {
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch("sess-a", "ev-a", "Edit", "path_suffix", "/repo/main", 2_000),
            // A bash substring match with no clean root → "(unknown)" bucket.
            touch("sess-b", "ev-b", "Bash", "bash_substring", "", 1_000),
        ];
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Sessions, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );

        assert_eq!(data["summary"]["distinct_roots"], json!(1));
        assert_eq!(data["summary"]["ambiguous"], json!(false));
        // The unknown bucket is still surfaced in roots for transparency.
        let labels: Vec<&str> = data["roots"]
            .as_array()
            .expect("roots")
            .iter()
            .filter_map(|r| r["root"].as_str())
            .collect();
        assert!(labels.contains(&UNKNOWN_ROOT));
    }
}
