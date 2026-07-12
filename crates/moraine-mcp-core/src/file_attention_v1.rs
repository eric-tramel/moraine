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

use super::{
    backend_query_id, handled_tool_error_result, internal_id_error, repo_error_to_contract_error,
    tool_success_result, AppState, QueryCancellationGuard,
};
use crate::contract::{
    format_rfc3339_utc_millis, CanonicalFileAttentionArgs, ContractError, FileAttentionArgs,
    FileAttentionGranularity, FileAttentionScope, McpEventId, McpSessionId, McpTurnId, Performance,
    ToolEnvelope, ToolErrorCode, ToolErrorEnvelope, FILE_ATTENTION_BROAD_SLA_TARGET_MS,
    FILE_ATTENTION_DEADLINE_MS, FILE_ATTENTION_DEFAULT_SLA_TARGET_MS,
    FILE_ATTENTION_MIN_TAIL_SEGMENTS, FILE_ATTENTION_TOOL,
};
use anyhow::{Context, Result};
use moraine_conversations::{FileAttentionQuery, FileAttentionTouch};
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

impl AppState {
    pub(crate) async fn file_attention_v1(&self, arguments: Value) -> Result<Value> {
        let perf = Performance::builder(FILE_ATTENTION_DEFAULT_SLA_TARGET_MS);
        let raw_request = arguments.clone();

        let args = match parse_file_attention_args(arguments, self.cfg.mcp.max_results) {
            Ok(args) => args,
            Err(error) => return encode_error(raw_request, error, perf.finish()),
        };
        let canonical_request = canonical_request_json(&args);

        // The Tier-0 plan still scans `tool_io` by path even when datetime
        // bounds are present, so report against the broad target until a future
        // indexed path/time plan can make windowed requests truly narrow.
        let perf = perf.with_sla_target(FILE_ATTENTION_BROAD_SLA_TARGET_MS);

        let mut warnings: Vec<String> = Vec::new();
        let tail = resolve_tail_from(&args.path, self.launch_dir.as_deref());
        if tail.normalized {
            warnings.push(format!(
                "normalized {:?} to {:?} before matching.",
                args.path, tail.rel
            ));
        }
        if tail.tail_is_absolute {
            warnings.push(format!(
                "could not reduce {:?} to a repo-relative tail (no .moraine.toml/.git marker found above it); matching the absolute path literally, so other worktrees of the same file will not be unified. Pass a repo-relative path for cross-worktree coverage.",
                args.path
            ));
        }
        if !tail.derive_worktree_roots {
            warnings.push(
                "could not prove the path is a repo-relative file in this checkout; roots are derived only from exact relative captures with cwd, otherwise reported as unknown to avoid mislabeling arbitrary suffix matches."
                    .to_string(),
            );
        }
        let depth = tail_segments(&tail.rel);
        if depth < FILE_ATTENTION_MIN_TAIL_SEGMENTS {
            warnings.push(format!(
                "{:?} is a generic tail (depth {depth}); results may include unrelated files. Pass a longer repo-relative path, and check the surfaced roots.",
                tail.rel
            ));
        }
        if args.scope == FileAttentionScope::Project && tail.project_id.is_none() {
            warnings.push(
                "scope=\"project\" could not establish the launch project's normalized identity; the repository query remains closed rather than widening across projects."
                    .to_string(),
            );
        }
        if args.scope == FileAttentionScope::Project {
            warnings.push(
                "pre-digest worktree roots pruned before durable project mapping was installed cannot be attributed safely and remain excluded; currently registered roots are migrated and future normalized roots remain durable."
                    .to_string(),
            );
        }

        let project_scoped_server = self.repo.config().session_scope.is_some();
        if args.scope == FileAttentionScope::All && project_scoped_server {
            warnings.push(
                "scope=\"all\" requested on a project-scoped server; the server's --project-only boundary remains enforced so returned handles stay openable."
                    .to_string(),
            );
        }

        let query_id = backend_query_id("file-attention");
        let repo_query = FileAttentionQuery {
            cancellation_token: query_id.clone(),
            rel: tail.rel.clone(),
            normalized_project_id: tail.project_id.clone(),
            normalized_project_roots: tail
                .root
                .as_deref()
                .and_then(moraine_config::worktree_roots_for_repo_root)
                .unwrap_or_default(),
            apply_project_scope: args.scope == FileAttentionScope::Project,
            start_unix_ms: args.start_unix_ms,
            end_unix_ms: args.end_unix_ms,
            tool: args.tool.clone(),
            mutations_only: args.mutations_only,
            max_rows: FILE_ATTENTION_SCAN_CAP,
            execution_budget_secs: FILE_ATTENTION_DEADLINE_MS.div_ceil(1_000),
        };

        let mut cancellation = QueryCancellationGuard::new(query_id.clone());
        let repo_result = timeout(
            Duration::from_millis(FILE_ATTENTION_DEADLINE_MS),
            self.repo.file_attention(repo_query),
        )
        .await;

        let touches = match repo_result {
            Ok(Ok(touches)) => {
                cancellation.disarm();
                touches
            }
            Ok(Err(error)) => {
                cancellation.disarm();
                return encode_error(
                    canonical_request,
                    repo_error_to_contract_error(error),
                    perf.finish(),
                );
            }
            Err(_) => {
                if let Err(error) = self.repo.cancel_query(&query_id).await {
                    warn!(
                        query_id = %query_id,
                        error = %error,
                        "file_attention: failed to cancel timed-out ClickHouse query"
                    );
                }
                cancellation.disarm();
                return encode_error(
                    canonical_request,
                    ContractError::new(
                        ToolErrorCode::DeadlineExceeded,
                        "file_attention exceeded its response deadline",
                    )
                    .with_details(json!({ "deadline_ms": FILE_ATTENTION_DEADLINE_MS })),
                    perf.finish(),
                );
            }
        };

        let performance = perf.finish();
        let data = build_data(&args, &tail, touches, &mut warnings);

        let payload = serde_json::to_value(
            ToolEnvelope::success(FILE_ATTENTION_TOOL, canonical_request, data, performance)
                .with_warnings(warnings),
        )
        .context("failed to encode file_attention response envelope")?;
        Ok(tool_success_result(format_text(&payload), payload))
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
                "file_attention expects a JSON object with valid fields",
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
    project_id: Option<String>,
    /// The path was absolute and could not be reduced to a repo-relative tail,
    /// so `rel` is the absolute path matched literally.
    tail_is_absolute: bool,
    /// Syntactic cleanup changed the path before matching.
    normalized: bool,
    /// Stripping the tail from historical matched paths is safe enough to
    /// report a worktree root.
    derive_worktree_roots: bool,
}

/// Reduce a query `path` to the repo-relative tail used for suffix matching.
///
/// Relative paths are resolved lexically against the launch directory, so a
/// deleted or not-yet-created file still carries the launch project's identity.
/// Absolute paths are stripped against their marker root. Compound shell text
/// is never interpreted as a path or repository root.
#[cfg(test)]
fn resolve_tail(path: &str) -> TailResolution {
    resolve_tail_from(path, std::env::current_dir().ok().as_deref())
}

fn resolve_tail_from(path: &str, launch_dir: Option<&Path>) -> TailResolution {
    let cleaned = path.strip_prefix("./").unwrap_or(path);
    let raw_is_single_path = is_single_path_candidate(cleaned);
    let normalized = normalize_path_text(cleaned);
    let normalized_changed = normalized != cleaned;
    let normalized_is_single_path = is_single_path_candidate(&normalized);
    let launch_project = launch_dir.and_then(|cwd| {
        let scope_probe = cwd.join(".moraine-project-scope");
        let root = find_project_root(&scope_probe)?;
        let project_id = project_id_for_root(&root)?;
        Some((root, project_id))
    });

    if !normalized.starts_with('/') {
        if raw_is_single_path && normalized_is_single_path {
            if let (Some(cwd), Some((root, project_id))) = (launch_dir, launch_project) {
                let absolute = normalize_path_text(&format!(
                    "{}/{}",
                    cwd.to_string_lossy().trim_end_matches('/'),
                    normalized
                ));
                if let Some(rel) = strip_root(&absolute, &root) {
                    if !rel.is_empty() {
                        return TailResolution {
                            rel,
                            project_id: Some(project_id),
                            root: Some(root),
                            tail_is_absolute: false,
                            normalized: normalized_changed,
                            derive_worktree_roots: true,
                        };
                    }
                }
            }
        }
        return TailResolution {
            rel: normalized.trim_start_matches('/').to_string(),
            root: None,
            project_id: None,
            tail_is_absolute: false,
            normalized: normalized_changed,
            derive_worktree_roots: false,
        };
    }

    if raw_is_single_path && normalized_is_single_path {
        if let Some(root) = find_project_root(Path::new(&normalized)) {
            if let Some(rel) = strip_root(&normalized, &root) {
                if !rel.is_empty() {
                    let target_project_id = project_id_for_root(&root);
                    let project_id = match (launch_dir, launch_project, target_project_id) {
                        (None, _, target_project_id) => target_project_id,
                        (Some(_), Some((_, launch_id)), Some(target_id))
                            if launch_id == target_id =>
                        {
                            Some(launch_id)
                        }
                        _ => None,
                    };
                    return TailResolution {
                        rel,
                        project_id,
                        root: Some(root),
                        tail_is_absolute: false,
                        normalized: normalized_changed,
                        derive_worktree_roots: true,
                    };
                }
            }
        }
    }

    TailResolution {
        rel: normalized,
        root: None,
        project_id: None,
        tail_is_absolute: true,
        normalized: normalized_changed,
        derive_worktree_roots: false,
    }
}

fn is_single_path_candidate(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\0')
        && !path
            .chars()
            .any(|character| matches!(character, '\n' | '\r' | ';' | '|' | '&' | '`'))
}

fn normalize_path_text(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if let Some(last) = parts.last() {
                    if *last != ".." {
                        parts.pop();
                        continue;
                    }
                }
                if !absolute {
                    parts.push(part);
                }
            }
            _ => parts.push(part),
        }
    }

    let body = parts.join("/");
    if absolute {
        if body.is_empty() {
            "/".to_string()
        } else {
            format!("/{body}")
        }
    } else {
        body
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

/// Walk up from a file path's parent directory looking for the nearest
/// repository boundary, bounded at `$HOME` or the filesystem root. A linked
/// worktree may inherit its `.moraine.toml` route from an enclosing checkout,
/// but its own `.git` marker still defines the root reported to callers.
fn find_project_root(file_path: &Path) -> Option<String> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let mut dir = file_path.parent();
    while let Some(current) = dir {
        if current.join(moraine_config::REPO_BACKEND_FILE).exists() || current.join(".git").exists()
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

fn project_id_for_root(root: &str) -> Option<String> {
    moraine_config::project_id_for_repo_root(root)
}

/// Count non-empty, slash-separated segments of a tail. `mod.rs` is depth 1;
/// `src/lib.rs` is depth 2.
fn tail_segments(rel: &str) -> usize {
    rel.split('/').filter(|segment| !segment.is_empty()).count()
}

/// Per-session accumulator built while folding the time-ordered touch stream.
#[derive(Default)]
struct SessionAgg {
    first_ms: Option<i64>,
    last_ms: Option<i64>,
    touch_count: u64,
    tools: BTreeSet<String>,
    roots: BTreeSet<String>,
    match_kinds: BTreeSet<String>,
    harness: String,
    /// The most recent touch's event_uid (first seen, since rows arrive
    /// newest-first) — the handle that drills straight to the latest touch.
    latest_event_uid: String,
    latest_turn_seq: Option<u32>,
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

    let mut valid_touches = Vec::with_capacity(touches.len());
    let mut skipped = 0usize;
    for touch in touches {
        match validate_touch_identifiers(&touch) {
            Ok(()) => valid_touches.push(touch),
            Err(error) => {
                skipped += 1;
                warn!(
                    session_id = %touch.session_id,
                    event_uid = %touch.event_uid,
                    error = %error,
                    "file_attention: skipping touch with an invalid identifier"
                );
            }
        }
    }
    let touches = valid_touches;
    if skipped > 0 {
        warnings.push(format!(
            "dropped {skipped} touch(es) whose repository identifiers could not be encoded; they are excluded from the result and its counts."
        ));
    }

    // --- summary + roots, over the full scanned set --------------------------
    let total_touches = touches.len() as u64;
    let mut first_ms: Option<i64> = None;
    let mut last_ms: Option<i64> = None;
    let mut distinct_sessions: BTreeSet<&str> = BTreeSet::new();
    let mut root_stats: HashMap<String, (u64, BTreeSet<String>)> = HashMap::new();
    for touch in &touches {
        if let Some(event_unix_ms) = touch.event_unix_ms {
            first_ms = Some(first_ms.map_or(event_unix_ms, |v| v.min(event_unix_ms)));
            last_ms = Some(last_ms.map_or(event_unix_ms, |v| v.max(event_unix_ms)));
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
    let unknown_root_touches = root_stats
        .get(UNKNOWN_ROOT)
        .map(|(touch_count, _)| *touch_count)
        .unwrap_or(0);
    let ambiguous = root_stats.len() > 1 || (unknown_root_touches > 0 && total_touches > 1);
    if unknown_root_touches > 0 {
        warnings.push(format!(
            "{unknown_root_touches} touch(es) have unknown worktree roots; check matched_path/open results before treating the root spread as complete."
        ));
    }

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
        "distinct_roots": roots.len(),
        "distinct_known_roots": distinct_known_roots,
        "unknown_root_touches": unknown_root_touches,
        "first_touch": first_ms.map(format_rfc3339_utc_millis),
        "last_touch": last_ms.map(format_rfc3339_utc_millis),
        "ambiguous": ambiguous,
        "scan_truncated": scan_truncated,
    });

    // --- body: per-session rollups OR a flat event timeline ------------------
    let limit = args.limit as usize;
    let (body_key, body, encodable, skipped) = match args.granularity {
        FileAttentionGranularity::Sessions => {
            let (sessions, encodable, skipped) = session_rollups(&touches, limit);
            ("sessions", sessions, encodable, skipped)
        }
        FileAttentionGranularity::Events => {
            let (events, encodable, skipped) = event_timeline(&touches, limit);
            ("events", events, encodable, skipped)
        }
    };
    let result_count = body.len();
    // `truncated` means the display limit hid encodable results — NOT that rows
    // were dropped for un-encodable identifiers. Those are a separate, surfaced
    // warning so the caller never reads a data fault as a limit cut.
    let truncated = encodable > limit || scan_truncated;
    if skipped > 0 {
        warnings.push(format!(
            "dropped {skipped} touch(es) whose repository identifiers could not be encoded; they are excluded from the result and its counts."
        ));
    }

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

fn validate_touch_identifiers(touch: &FileAttentionTouch) -> Result<(), ContractError> {
    McpSessionId::from_raw_session_id(touch.session_id.as_str()).map_err(internal_id_error)?;
    McpEventId::from_raw_event_uid(touch.event_uid.as_str()).map_err(internal_id_error)?;
    Ok(())
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
/// most-recent-first order. Returns `(displayed_rollups, encodable_sessions,
/// skipped_sessions)` where `encodable` counts every session whose identifiers
/// encode (used for the limit-truncation signal) and `skipped` counts sessions
/// dropped because a repository identifier could not be encoded.
fn session_rollups(touches: &[FileAttentionTouch], limit: usize) -> (Vec<Value>, usize, usize) {
    let mut order: Vec<String> = Vec::new();
    let mut aggs: HashMap<String, SessionAgg> = HashMap::new();
    for touch in touches {
        let agg = aggs.entry(touch.session_id.clone()).or_insert_with(|| {
            order.push(touch.session_id.clone());
            SessionAgg {
                latest_event_uid: touch.event_uid.clone(),
                latest_turn_seq: touch.turn_seq,
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
        if let Some(event_unix_ms) = touch.event_unix_ms {
            agg.first_ms = Some(
                agg.first_ms
                    .map_or(event_unix_ms, |first| first.min(event_unix_ms)),
            );
            agg.last_ms = Some(
                agg.last_ms
                    .map_or(event_unix_ms, |last| last.max(event_unix_ms)),
            );
        }
    }

    let mut sessions = Vec::new();
    let mut encodable = 0usize;
    let mut skipped = 0usize;
    for session_id in order {
        let agg = &aggs[&session_id];
        // Rank advances only on a kept row so it stays contiguous (1..=N).
        let rank = sessions.len() + 1;
        match session_rollup_json(rank, &session_id, agg) {
            Ok(value) => {
                encodable += 1;
                if sessions.len() < limit {
                    sessions.push(value);
                }
            }
            Err(error) => {
                skipped += 1;
                warn!(
                    session_id = %session_id,
                    error = %error,
                    "file_attention: skipping session rollup with an invalid identifier"
                );
            }
        }
    }
    (sessions, encodable, skipped)
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

    let mut open = json!({
        "session_id": mcp_session_id,
        "event_id": latest_event_id,
    });
    if let Some(turn_seq) = agg.latest_turn_seq {
        if let Ok(turn_id) = McpTurnId::from_raw_session_id_and_turn_seq(session_id, turn_seq) {
            open["turn_id"] = json!(turn_id.to_string());
        }
    }

    Ok(json!({
        "rank": rank,
        "id": mcp_session_id,
        "session": {
            "id": mcp_session_id,
            "harness": agg.harness,
            "first_touch": agg.first_ms.map(format_rfc3339_utc_millis),
            "last_touch": agg.last_ms.map(format_rfc3339_utc_millis),
            "touch_count": agg.touch_count,
            "tools": agg.tools.iter().collect::<Vec<_>>(),
            "worktree_roots": agg.roots.iter().collect::<Vec<_>>(),
            "match_kinds": agg.match_kinds.iter().collect::<Vec<_>>(),
        },
        "open": open,
    }))
}

/// The flat, newest-first touch-by-touch timeline. Returns `(displayed_events,
/// encodable_events, skipped_events)` — `encodable` drives limit-truncation and
/// `skipped` counts touches dropped for un-encodable identifiers.
fn event_timeline(touches: &[FileAttentionTouch], limit: usize) -> (Vec<Value>, usize, usize) {
    let mut events = Vec::new();
    let mut encodable = 0usize;
    let mut skipped = 0usize;
    for touch in touches {
        let rank = events.len() + 1;
        match event_json(rank, touch) {
            Ok(value) => {
                encodable += 1;
                if events.len() < limit {
                    events.push(value);
                }
            }
            Err(error) => {
                skipped += 1;
                warn!(
                    event_uid = %touch.event_uid,
                    error = %error,
                    "file_attention: skipping touch with an invalid identifier"
                );
            }
        }
    }
    (events, encodable, skipped)
}

fn event_json(rank: usize, touch: &FileAttentionTouch) -> Result<Value, ContractError> {
    let event_id = McpEventId::from_raw_event_uid(touch.event_uid.as_str())
        .map(|id| id.to_string())
        .map_err(internal_id_error)?;
    let session_id = McpSessionId::from_raw_session_id(touch.session_id.as_str())
        .map(|id| id.to_string())
        .map_err(internal_id_error)?;

    let mut open = json!({
        "event_id": event_id,
        "session_id": session_id,
    });
    if let Some(turn_seq) = touch.turn_seq {
        if let Ok(turn_id) =
            McpTurnId::from_raw_session_id_and_turn_seq(touch.session_id.as_str(), turn_seq)
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
            "timestamp": touch.event_unix_ms.map(format_rfc3339_utc_millis),
            "tool_name": touch.tool_name,
            "phase": touch.tool_phase,
            "turn": touch.turn_seq,
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
    Ok(handled_tool_error_result(
        format_error_text(&payload),
        payload,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_envelope_is_a_handled_tool_error() {
        let result = encode_error(
            json!({}),
            ContractError::new(
                ToolErrorCode::DeadlineExceeded,
                "file_attention exceeded its response deadline",
            ),
            Performance::builder(FILE_ATTENTION_DEFAULT_SLA_TARGET_MS).finish(),
        )
        .expect("deadline response");

        assert_eq!(result["isError"], true);
        assert_eq!(
            result["structuredContent"]["schema_version"],
            crate::contract::ERROR_SCHEMA_VERSION
        );
        assert_eq!(
            result["structuredContent"]["error"]["code"],
            "deadline_exceeded"
        );
    }

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
            tool_call_id: format!("call-{event}"),
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
            event_unix_ms: (ts > 0).then_some(ts),
            event_order: ts.max(0) as u64,
            turn_seq: (ts > 0).then_some(1),
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
    fn resolve_tail_passes_unscoped_relative_paths_through() {
        let resolved = resolve_tail_from("crates/foo/tee.rs", None);
        assert_eq!(resolved.rel, "crates/foo/tee.rs");
        assert!(!resolved.tail_is_absolute);
        assert!(resolved.root.is_none());

        let dotted = resolve_tail_from("./crates/foo/tee.rs", None);
        assert_eq!(dotted.rel, "crates/foo/tee.rs");
    }

    #[test]
    fn resolve_tail_carries_launch_project_identity_for_missing_relative_file() {
        let dir = std::env::temp_dir().join(format!("moraine-fa-relative-{}", std::process::id()));
        let root = dir.join("repo");
        std::fs::create_dir_all(&root).expect("mkdir root");
        std::fs::create_dir_all(root.join(".git")).expect("mkdir git dir");
        std::fs::write(root.join(".moraine.toml"), "backend = \"x\"\n").expect("write marker");

        let resolved = resolve_tail_from("crates/new/file.rs", Some(&root));
        assert_eq!(resolved.rel, "crates/new/file.rs");
        assert_eq!(resolved.root.as_deref(), root.to_str());
        assert_eq!(
            resolved.project_id,
            moraine_config::project_id_for_repo_root(&root)
        );
        assert!(resolved.derive_worktree_roots);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_tail_makes_nested_launch_paths_repository_relative() {
        let dir =
            std::env::temp_dir().join(format!("moraine-fa-nested-launch-{}", std::process::id()));
        let root = dir.join("repo");
        let launch = root.join("crates/foo");
        std::fs::create_dir_all(root.join(".git")).expect("mkdir git dir");
        std::fs::create_dir_all(&launch).expect("mkdir launch dir");
        std::fs::write(root.join(".moraine.toml"), "backend = \"x\"\n").expect("write marker");

        let nested = resolve_tail_from("src/lib.rs", Some(&launch));
        assert_eq!(nested.rel, "crates/foo/src/lib.rs");
        assert_eq!(nested.root.as_deref(), root.to_str());

        let parent = resolve_tail_from("../shared.rs", Some(&launch));
        assert_eq!(parent.rel, "crates/shared.rs");
        assert_eq!(parent.root.as_deref(), root.to_str());

        let outside = resolve_tail_from("../../../outside.rs", Some(&launch));
        assert!(outside.project_id.is_none());
        assert!(!outside.derive_worktree_roots);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_tail_never_derives_provenance_from_compound_input() {
        let dir = std::env::temp_dir().join(format!("moraine-fa-compound-{}", std::process::id()));
        let root = dir.join("repo");
        std::fs::create_dir_all(&root).expect("mkdir root");
        std::fs::create_dir_all(root.join(".git")).expect("mkdir git dir");
        std::fs::write(root.join(".moraine.toml"), "backend = \"x\"\n").expect("write marker");

        for delimiter in ["\0", "\n", "\r", ";", "|", "&", "`"] {
            let path = format!("bad{delimiter}command/../src/lib.rs");
            let resolved = resolve_tail_from(&path, Some(&root));
            assert!(resolved.root.is_none(), "delimiter {delimiter:?}");
            assert!(resolved.project_id.is_none(), "delimiter {delimiter:?}");
            assert!(!resolved.derive_worktree_roots, "delimiter {delimiter:?}");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_tail_strips_absolute_path_to_marker_root() {
        let dir = std::env::temp_dir().join(format!("moraine-fa-test-{}", std::process::id()));
        let root = dir.join("repo");
        let nested = root.join("crates/foo");
        std::fs::create_dir_all(&nested).expect("mkdir nested");
        std::fs::create_dir_all(root.join(".git")).expect("mkdir git dir");
        std::fs::write(root.join(".moraine.toml"), "backend = \"x\"\n").expect("write marker");
        let file = nested.join("tee.rs");
        std::fs::write(&file, "// test").expect("write file");

        let resolved = resolve_tail_from(file.to_str().expect("utf8 path"), None);
        assert_eq!(resolved.rel, "crates/foo/tee.rs");
        assert!(!resolved.tail_is_absolute);
        assert_eq!(resolved.root.as_deref(), root.to_str());
        assert_eq!(
            resolved.project_id,
            moraine_config::project_id_for_repo_root(&root)
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_tail_closes_project_scope_for_absolute_path_in_another_project() {
        let dir =
            std::env::temp_dir().join(format!("moraine-fa-cross-project-{}", std::process::id()));
        let launch = dir.join("launch");
        let other = dir.join("other");
        for root in [&launch, &other] {
            std::fs::create_dir_all(root.join(".git")).expect("mkdir git dir");
            std::fs::write(root.join(".moraine.toml"), "backend = \"shared\"\n")
                .expect("write marker");
        }
        let file = other.join("src/lib.rs");
        std::fs::create_dir_all(file.parent().expect("file parent")).expect("mkdir file parent");
        std::fs::write(&file, "// test").expect("write file");

        let resolved = resolve_tail_from(file.to_str().expect("utf8 path"), Some(&launch));
        assert_eq!(resolved.rel, "src/lib.rs");
        assert_eq!(resolved.root.as_deref(), other.to_str());
        assert!(resolved.project_id.is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_tail_keeps_linked_worktree_root_with_enclosing_project_marker() {
        let dir =
            std::env::temp_dir().join(format!("moraine-fa-linked-worktree-{}", std::process::id()));
        let root = dir.join("repo");
        let linked = root.join("worktrees/linked");
        let linked_git_dir = root.join(".git/worktrees/linked");
        std::fs::create_dir_all(&linked_git_dir).expect("mkdir linked git dir");
        std::fs::create_dir_all(&linked).expect("mkdir linked worktree");
        std::fs::write(linked_git_dir.join("commondir"), "../..\n").expect("write commondir");
        std::fs::write(
            linked.join(".git"),
            format!("gitdir: {}\n", linked_git_dir.to_string_lossy()),
        )
        .expect("write linked git marker");
        std::fs::write(root.join(".moraine.toml"), "backend = \"shared\"\n").expect("write marker");

        let resolved = resolve_tail_from("src/lib.rs", Some(&linked));
        assert_eq!(resolved.root.as_deref(), linked.to_str());
        assert_eq!(
            resolved.project_id,
            moraine_config::project_id_for_repo_root(&root)
        );

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
            touch(
                "sess-a",
                "ev-2",
                "Edit",
                "path_suffix",
                "/repo/main",
                1_777_464_001_123,
            ),
            touch(
                "sess-a",
                "ev-1",
                "Read",
                "path_suffix",
                "/repo/main",
                1_777_464_000_000,
            ),
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
        assert_eq!(
            events[0]["event"]["timestamp"],
            json!("2026-04-29T12:00:01.123Z")
        );
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
    fn build_data_skipped_invalid_id_is_a_warning_not_truncation() {
        // A touch whose session_id cannot be encoded (empty) is dropped, but
        // that must NOT read as a limit-induced truncation — it is its own
        // surfaced warning. (Regression for the truncation/skip conflation.)
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch(
                "sess-ok",
                "ev-ok",
                "Edit",
                "path_suffix",
                "/repo/main",
                2_000,
            ),
            touch("", "ev-bad", "Edit", "path_suffix", "/repo/main", 1_000),
        ];
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Sessions, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );

        // Only the encodable session is shown, and the limit (50) was not hit.
        assert_eq!(data["result_count"], json!(1));
        assert_eq!(data["truncated"], json!(false));
        assert!(
            warnings.iter().any(|w| w.contains("could not be encoded")),
            "expected a skipped-row warning, got {warnings:?}"
        );
    }

    #[test]
    fn unknown_root_bucket_marks_ambiguity_when_mixed_with_known_roots() {
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch("sess-a", "ev-a", "Edit", "path_suffix", "/repo/main", 2_000),
            // A shell path-token match with no clean root → "(unknown)" bucket.
            touch("sess-b", "ev-b", "Bash", "shell_path", "", 1_000),
        ];
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Sessions, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );

        assert_eq!(data["summary"]["distinct_roots"], json!(2));
        assert_eq!(data["summary"]["distinct_known_roots"], json!(1));
        assert_eq!(data["summary"]["unknown_root_touches"], json!(1));
        assert_eq!(data["summary"]["ambiguous"], json!(true));
        // The unknown bucket is still surfaced in roots for transparency.
        let labels: Vec<&str> = data["roots"]
            .as_array()
            .expect("roots")
            .iter()
            .filter_map(|r| r["root"].as_str())
            .collect();
        assert!(labels.contains(&UNKNOWN_ROOT));
    }

    #[test]
    fn build_data_session_touched_under_two_roots_lists_both() {
        // One session that edited the file in two worktrees (work moved between
        // checkouts) lists both roots under that session, and the spread reads
        // as ambiguous at the summary level.
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch("sess-x", "ev-2", "Edit", "path_suffix", "/repo/main", 2_000),
            touch(
                "sess-x",
                "ev-1",
                "Edit",
                "path_suffix",
                "/repo/worktrees/feat",
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
        assert_eq!(data["summary"]["distinct_sessions"], json!(1));
        assert_eq!(data["summary"]["distinct_roots"], json!(2));
        assert_eq!(data["summary"]["ambiguous"], json!(true));
        let roots = data["sessions"][0]["session"]["worktree_roots"]
            .as_array()
            .expect("roots");
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn build_data_all_touches_missing_timestamps_null_span() {
        // Touches that never joined a trace row (event_unix_ms == None) still
        // appear, but the span is null — never the 1970 sentinel — and an
        // event with no timestamp omits its turn handle.
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches = vec![
            touch("s1", "e1", "Edit", "path_suffix", "/r/main", 0),
            touch("s2", "e2", "Read", "path_suffix", "/r/main", 0),
        ];
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Events, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );
        assert_eq!(data["summary"]["total_touches"], json!(2));
        assert!(data["summary"]["first_touch"].is_null());
        assert!(data["summary"]["last_touch"].is_null());
        let ev0 = &data["events"][0];
        assert!(ev0["event"]["timestamp"].is_null());
        assert!(ev0["open"].get("turn_id").is_none());
    }

    #[test]
    fn action_preview_collapses_whitespace_and_truncates_by_chars() {
        // The cap is on chars, not bytes (multibyte-safe), and runs of
        // whitespace collapse to single spaces.
        let mut t = touch("s", "e", "Edit", "path_suffix", "/r/main", 1);
        t.input_preview = format!("  edit\n\t {}  ", "字".repeat(400));
        let preview = action_preview(&t).expect("preview");
        assert!(preview.ends_with('…'));
        assert_eq!(preview.chars().count(), 161); // 160 content chars + ellipsis
        assert!(!preview.contains('\n') && !preview.contains('\t'));
        assert!(!preview.contains("  "));
    }

    #[test]
    fn session_rollups_rank_contiguous_when_middle_row_skipped() {
        // An un-encodable session between two valid ones is dropped without
        // breaking rank contiguity, and is counted as skipped — not displayed.
        let touches = vec![
            touch("s-a", "e-a", "Edit", "path_suffix", "/r/main", 3_000),
            touch("", "e-bad", "Edit", "path_suffix", "/r/main", 2_000),
            touch("s-c", "e-c", "Edit", "path_suffix", "/r/main", 1_000),
        ];
        let (rollups, encodable, skipped) = session_rollups(&touches, 50);
        assert_eq!(encodable, 2);
        assert_eq!(skipped, 1);
        assert_eq!(rollups.len(), 2);
        assert_eq!(rollups[0]["rank"], json!(1));
        assert_eq!(rollups[1]["rank"], json!(2));
    }

    #[test]
    fn build_data_scan_cap_flags_truncation_with_warning() {
        // Hitting the scan cap marks scan_truncated and warns that summary and
        // roots are computed over a partial (capped) set.
        let tail = resolve_tail("crates/foo/tee.rs");
        let touches: Vec<FileAttentionTouch> = (0..(FILE_ATTENTION_SCAN_CAP + 1))
            .map(|i| {
                touch(
                    &format!("s-{i}"),
                    &format!("e-{i}"),
                    "Edit",
                    "path_suffix",
                    "/r/main",
                    (i as i64) + 1,
                )
            })
            .collect();
        let mut warnings = Vec::new();
        let data = build_data(
            &canonical(FileAttentionGranularity::Events, FileAttentionScope::All),
            &tail,
            touches,
            &mut warnings,
        );
        assert_eq!(data["summary"]["scan_truncated"], json!(true));
        assert_eq!(
            data["summary"]["total_touches"],
            json!(FILE_ATTENTION_SCAN_CAP)
        );
        assert!(warnings.iter().any(|w| w.contains("more than")));
        assert_eq!(data["truncated"], json!(true));
    }

    #[test]
    fn resolve_tail_normalizes_dotdot_and_repeated_slashes() {
        assert_eq!(
            resolve_tail_from("crates/foo/../foo.rs", None).rel,
            "crates/foo.rs"
        );
        assert_eq!(
            resolve_tail_from("crates//foo///bar.rs", None).rel,
            "crates/foo/bar.rs"
        );
        assert_eq!(
            resolve_tail_from("crates/foo dir/x.rs", None).rel,
            "crates/foo dir/x.rs"
        );
    }

    #[test]
    fn tail_segments_zero_for_only_slashes() {
        assert_eq!(tail_segments("////"), 0);
        assert_eq!(tail_segments("/"), 0);
        assert_eq!(tail_segments(""), 0);
    }
}
