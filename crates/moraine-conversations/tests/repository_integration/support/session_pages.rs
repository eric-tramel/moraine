//! Mock responses for the two-phase (page-first) session listing queries.

use axum::http::StatusCode;
use serde_json::{json, Value};

use super::responses::json_each_row;

/// Session ids in ascending `last_event_time` order.
const SESSIONS_ASC: [&str; 3] = ["sess_a", "sess_b", "sess_c"];

/// Phase 1 is the narrow `GROUP BY session_id` page selection.
pub(crate) fn is_session_page_select(query: &str) -> bool {
    query.starts_with("SELECT\n  session_id,\n  min(")
        && query.contains("AS last_event_time")
        && query.contains("FROM `moraine`.`events` FINAL")
}

fn quoted_after(query: &str, marker: &str) -> Option<String> {
    let rest = query.split_once(marker)?.1;
    let rest = rest.strip_prefix('\'')?;
    Some(rest.split('\'').next()?.to_string())
}

pub(crate) fn session_page_select_response(query: &str) -> (StatusCode, String) {
    let desc = query.contains("ORDER BY last_event_time DESC");
    let mut ids: Vec<&str> = SESSIONS_ASC.to_vec();
    if desc {
        ids.reverse();
    }
    if let Some(after) = quoted_after(query, "AND session_id < ") {
        ids.retain(|id| *id < after.as_str());
    }
    if let Some(after) = quoted_after(query, "AND session_id > ") {
        ids.retain(|id| *id > after.as_str());
    }
    let limit: usize = query
        .split("LIMIT ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .expect("phase 1 query must carry a LIMIT");
    ids.truncate(limit);
    let rows: Vec<Value> = ids.iter().map(|id| json!({ "session_id": id })).collect();
    (StatusCode::OK, json_each_row(Value::Array(rows)))
}

fn ids_in_list(query: &str) -> Vec<String> {
    let rest = query
        .split("session_id IN (")
        .nth(1)
        .expect("phase 2 query must restrict session_id IN (...)");
    let list = rest.split(')').next().unwrap_or_default();
    list.split(',')
        .map(|part| part.trim().trim_matches('\'').to_string())
        .collect()
}

fn monitor_row(id: &str) -> Value {
    match id {
        "sess_c" => json!({
            "session_id": "sess_c",
            "first_event_time": "2026-01-03 10:00:00",
            "first_event_unix_ms": 1767434400000_i64,
            "last_event_time": "2026-01-03 10:10:00",
            "last_event_unix_ms": 1767435000000_i64,
            "total_turns": 3, "total_events": 30,
            "user_messages": 6, "assistant_messages": 6,
            "tool_calls": 3, "tool_results": 3,
            "mode": "web_search",
            "session_slug": "project-c",
            "session_summary": "Session C summary"
        }),
        "sess_b" => json!({
            "session_id": "sess_b",
            "first_event_time": "2026-01-02 10:00:00",
            "first_event_unix_ms": 1767348000000_i64,
            "last_event_time": "2026-01-02 10:10:00",
            "last_event_unix_ms": 1767348600000_i64,
            "total_turns": 2, "total_events": 22,
            "user_messages": 4, "assistant_messages": 4,
            "tool_calls": 2, "tool_results": 2,
            "mode": "web_search"
        }),
        "sess_a" => json!({
            "session_id": "sess_a",
            "first_event_time": "2026-01-01 10:00:00",
            "first_event_unix_ms": 1767261600000_i64,
            "last_event_time": "2026-01-01 10:10:00",
            "last_event_unix_ms": 1767262200000_i64,
            "total_turns": 2, "total_events": 20,
            "user_messages": 4, "assistant_messages": 4,
            "tool_calls": 2, "tool_results": 2,
            "mode": "web_search"
        }),
        other => panic!("unexpected session id in mock: {other}"),
    }
}

fn mcp_row(id: &str) -> Value {
    match id {
        "sess_c" => json!({
            "session_id": "sess_c",
            "first_event_time": "2026-01-03 10:00:00",
            "first_event_unix_ms": 1767434400000_i64,
            "last_event_time": "2026-01-03 10:10:00",
            "last_event_unix_ms": 1767435000000_i64,
            "total_turns": 3, "total_events": 30,
            "mode": "web_search", "completed": 1_u8,
            "title": "Session C title",
            "source": "codex", "harness": "codex",
            "originator": "Codex Desktop",
            "origin_cwd": "/work/acme-secret-merger",
            "project": "acme-secret-merger",
            "session_slug": "project-c",
            "session_summary": "Session C summary",
            "explicit_title": "Session C title",
            "codex_user_message_preview": "Genuine C request"
        }),
        "sess_b" => json!({
            "session_id": "sess_b",
            "first_event_time": "2026-01-02 10:00:00",
            "first_event_unix_ms": 1767348000000_i64,
            "last_event_time": "2026-01-02 10:10:00",
            "last_event_unix_ms": 1767348600000_i64,
            "total_turns": 2, "total_events": 22,
            "mode": "web_search", "completed": 1_u8,
            "title": "Session B title",
            "source": "codex", "harness": "codex",
            "session_slug": "project-b",
            "session_summary": "Session B summary",
            "explicit_title": "",
            "codex_user_message_preview": "Genuine B request"
        }),
        "sess_a" => json!({
            "session_id": "sess_a",
            "first_event_time": "2026-01-01 10:00:00",
            "first_event_unix_ms": 1767261600000_i64,
            "last_event_time": "2026-01-01 10:10:00",
            "last_event_unix_ms": 1767262200000_i64,
            "total_turns": 2, "total_events": 20,
            "mode": "web_search", "completed": 0_u8,
            "title": "",
            "source": "codex", "harness": "codex",
            "session_slug": "", "session_summary": "",
            "explicit_title": "",
            "codex_user_message_preview": "Genuine A request"
        }),
        other => panic!("unexpected session id in mock: {other}"),
    }
}

/// Phase 2 summary queries (monitor and MCP). Rows come back only for the ids
/// the query restricts itself to, in the requested sort order.
pub(crate) fn session_page_summary_response(query: &str) -> Option<(StatusCode, String)> {
    let is_mcp = query.contains("AS completed") && query.contains("latest_terminal_payload_type");
    let is_monitor = query.contains("ANY LEFT JOIN")
        && query.contains("AS session_slug")
        && query.contains("FROM (SELECT\n  session_id,\n  min(event_time)")
        && !is_mcp;
    if !is_mcp && !is_monitor {
        return None;
    }
    let mut ids = ids_in_list(query);
    ids.sort();
    if query.contains(" DESC\n") || query.contains(" DESC,") {
        ids.reverse();
    }
    let rows: Vec<Value> = ids
        .iter()
        .map(|id| if is_mcp { mcp_row(id) } else { monitor_row(id) })
        .collect();
    Some((StatusCode::OK, json_each_row(Value::Array(rows))))
}
