use crate::model::NormalizedRecord;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const TEXT_LIMIT: usize = 200_000;
const PREVIEW_LIMIT: usize = 320;
const UNPARSEABLE_EVENT_TS: &str = "1970-01-01 00:00:00.000";

fn session_id_re() -> &'static Regex {
    static SESSION_ID_RE: OnceLock<Regex> = OnceLock::new();
    SESSION_ID_RE.get_or_init(|| {
        Regex::new(
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$",
        )
        .expect("valid session id regex")
    })
}

fn session_date_re() -> &'static Regex {
    static SESSION_DATE_RE: OnceLock<Regex> = OnceLock::new();
    SESSION_DATE_RE.get_or_init(|| {
        Regex::new(r"/(?:sessions|projects)/(\d{4})/(\d{2})/(\d{2})/")
            .expect("valid session date regex")
    })
}

fn to_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn to_u32(value: Option<&Value>) -> u32 {
    match value {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        Some(Value::String(s)) => s.parse::<u64>().unwrap_or(0).min(u32::MAX as u64) as u32,
        _ => 0,
    }
}

fn to_u16(value: Option<&Value>) -> u16 {
    to_u32(value).min(u16::MAX as u32) as u16
}

fn to_u8_bool(value: Option<&Value>) -> u8 {
    match value {
        Some(Value::Bool(v)) => {
            if *v {
                1
            } else {
                0
            }
        }
        Some(Value::Number(v)) => {
            if v.as_i64().unwrap_or(0) != 0 {
                1
            } else {
                0
            }
        }
        Some(Value::String(s)) => {
            let lower = s.to_ascii_lowercase();
            if lower == "true" || lower == "1" {
                1
            } else {
                0
            }
        }
        _ => 0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Provider {
    Codex,
    Claude,
}

impl Provider {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "codex" => Ok(Self::Codex),
            "claude" => Ok(Self::Claude),
            _ => Err(anyhow!(
                "unsupported provider `{}`; expected one of: codex, claude",
                raw.trim()
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

fn canonicalize_model(provider: &str, raw_model: &str) -> String {
    let mut model = raw_model.trim().to_ascii_lowercase();
    if model.is_empty() {
        return String::new();
    }

    model = model.replace(' ', "-");

    if provider == "codex" && model == "codex" {
        return "gpt-5.3-codex-xhigh".to_string();
    }

    model
}

fn resolve_model_hint(event_rows: &[Value], provider: &str, fallback: &str) -> String {
    for row in event_rows.iter().rev() {
        if let Some(model) = row.get("model").and_then(Value::as_str) {
            let normalized = canonicalize_model(provider, model);
            if !normalized.is_empty() {
                return normalized;
            }
        }
    }

    canonicalize_model(provider, fallback)
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        input.to_string()
    } else {
        input.chars().take(max_chars).collect()
    }
}

fn extract_message_text(content: &Value) -> String {
    fn walk(node: &Value, out: &mut Vec<String>) {
        match node {
            Value::String(s) => {
                if !s.trim().is_empty() {
                    out.push(s.clone());
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            Value::Object(map) => {
                for key in ["text", "message", "output", "thinking", "summary"] {
                    if let Some(Value::String(s)) = map.get(key) {
                        if !s.trim().is_empty() {
                            out.push(s.clone());
                        }
                    }
                }

                for key in ["content", "text_elements", "input"] {
                    if let Some(value) = map.get(key) {
                        walk(value, out);
                    }
                }
            }
            _ => {}
        }
    }

    let mut chunks = Vec::<String>::new();
    walk(content, &mut chunks);
    truncate_chars(&chunks.join("\n"), TEXT_LIMIT)
}

fn extract_content_types(content: &Value) -> Vec<String> {
    if let Value::Array(items) = content {
        let mut out = Vec::<String>::new();
        for item in items {
            if let Some(t) = item.get("type").and_then(|v| v.as_str()) {
                if !t.is_empty() {
                    out.push(t.to_string());
                }
            }
        }
        out.sort();
        out.dedup();
        return out;
    }
    Vec::new()
}

pub fn infer_session_id_from_file(source_file: &str) -> String {
    let stem = std::path::Path::new(source_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    session_id_re()
        .captures(stem)
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_default()
}

pub fn infer_session_date_from_file(source_file: &str, record_ts: &str) -> String {
    if let Some(cap) = session_date_re().captures(source_file) {
        return format!("{}-{}-{}", &cap[1], &cap[2], &cap[3]);
    }

    parse_record_ts(record_ts)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

fn parse_record_ts(record_ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(record_ts)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn format_event_ts(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn parse_event_ts(record_ts: &str) -> (String, bool) {
    if let Some(dt) = parse_record_ts(record_ts) {
        return (format_event_ts(&dt), false);
    }

    (UNPARSEABLE_EVENT_TS.to_string(), true)
}

fn event_uid(
    source_file: &str,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_fingerprint: &str,
    suffix: &str,
) -> String {
    let material = format!(
        "{}|{}|{}|{}|{}|{}",
        source_file, source_generation, source_line_no, source_offset, record_fingerprint, suffix
    );

    let mut hasher = Sha256::new();
    hasher.update(material.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn event_version() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    now as u64
}

fn raw_hash(raw_json: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(raw_json.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{:x}", digest);
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0)
}

fn io_hash(input_json: &str, output_json: &str) -> u64 {
    raw_hash(&format!("{}\n{}", input_json, output_json))
}

struct RecordContext<'a> {
    source_name: &'a str,
    provider: &'a str,
    session_id: &'a str,
    session_date: &'a str,
    source_file: &'a str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_ts: &'a str,
    event_ts: &'a str,
}

fn base_event_obj(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    event_kind: &str,
    payload_type: &str,
    actor_kind: &str,
    text_content: &str,
    payload_json: &str,
) -> Map<String, Value> {
    let text_content = truncate_chars(text_content, TEXT_LIMIT);
    let mut obj = Map::<String, Value>::new();
    obj.insert(
        "event_uid".to_string(),
        Value::String(event_uid.to_string()),
    );
    obj.insert(
        "session_id".to_string(),
        Value::String(ctx.session_id.to_string()),
    );
    obj.insert(
        "session_date".to_string(),
        Value::String(ctx.session_date.to_string()),
    );
    obj.insert(
        "source_name".to_string(),
        Value::String(ctx.source_name.to_string()),
    );
    obj.insert(
        "provider".to_string(),
        Value::String(ctx.provider.to_string()),
    );
    obj.insert(
        "source_file".to_string(),
        Value::String(ctx.source_file.to_string()),
    );
    obj.insert("source_inode".to_string(), json!(ctx.source_inode));
    obj.insert(
        "source_generation".to_string(),
        json!(ctx.source_generation),
    );
    obj.insert("source_line_no".to_string(), json!(ctx.source_line_no));
    obj.insert("source_offset".to_string(), json!(ctx.source_offset));
    obj.insert(
        "source_ref".to_string(),
        Value::String(format!(
            "{}:{}:{}",
            ctx.source_file, ctx.source_generation, ctx.source_line_no
        )),
    );
    obj.insert(
        "record_ts".to_string(),
        Value::String(ctx.record_ts.to_string()),
    );
    obj.insert(
        "event_ts".to_string(),
        Value::String(ctx.event_ts.to_string()),
    );
    obj.insert(
        "event_kind".to_string(),
        Value::String(event_kind.to_string()),
    );
    obj.insert(
        "actor_kind".to_string(),
        Value::String(actor_kind.to_string()),
    );
    obj.insert(
        "payload_type".to_string(),
        Value::String(payload_type.to_string()),
    );
    obj.insert("op_kind".to_string(), Value::String(String::new()));
    obj.insert("op_status".to_string(), Value::String(String::new()));
    obj.insert("request_id".to_string(), Value::String(String::new()));
    obj.insert("trace_id".to_string(), Value::String(String::new()));
    obj.insert("turn_index".to_string(), json!(0u32));
    obj.insert("item_id".to_string(), Value::String(String::new()));
    obj.insert("tool_call_id".to_string(), Value::String(String::new()));
    obj.insert(
        "parent_tool_call_id".to_string(),
        Value::String(String::new()),
    );
    obj.insert("origin_event_id".to_string(), Value::String(String::new()));
    obj.insert(
        "origin_tool_call_id".to_string(),
        Value::String(String::new()),
    );
    obj.insert("tool_name".to_string(), Value::String(String::new()));
    obj.insert("tool_phase".to_string(), Value::String(String::new()));
    obj.insert("tool_error".to_string(), json!(0u8));
    obj.insert("agent_run_id".to_string(), Value::String(String::new()));
    obj.insert("agent_label".to_string(), Value::String(String::new()));
    obj.insert("coord_group_id".to_string(), Value::String(String::new()));
    obj.insert(
        "coord_group_label".to_string(),
        Value::String(String::new()),
    );
    obj.insert("is_substream".to_string(), json!(0u8));
    obj.insert("model".to_string(), Value::String(String::new()));
    obj.insert("input_tokens".to_string(), json!(0u32));
    obj.insert("output_tokens".to_string(), json!(0u32));
    obj.insert("cache_read_tokens".to_string(), json!(0u32));
    obj.insert("cache_write_tokens".to_string(), json!(0u32));
    obj.insert("latency_ms".to_string(), json!(0u32));
    obj.insert("retry_count".to_string(), json!(0u16));
    obj.insert("service_tier".to_string(), Value::String(String::new()));
    obj.insert("content_types".to_string(), json!([]));
    obj.insert("has_reasoning".to_string(), json!(0u8));
    obj.insert(
        "text_content".to_string(),
        Value::String(text_content.clone()),
    );
    obj.insert(
        "text_preview".to_string(),
        Value::String(truncate_chars(&text_content, PREVIEW_LIMIT)),
    );
    obj.insert(
        "payload_json".to_string(),
        Value::String(payload_json.to_string()),
    );
    obj.insert("token_usage_json".to_string(), Value::String(String::new()));
    obj.insert("event_version".to_string(), json!(event_version()));
    obj
}

fn build_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_event_uid: &str,
    linked_external_id: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    json!({
        "event_uid": event_uid,
        "linked_event_uid": linked_event_uid,
        "linked_external_id": linked_external_id,
        "link_type": link_type,
        "session_id": ctx.session_id,
        "provider": ctx.provider,
        "source_name": ctx.source_name,
        "metadata_json": metadata_json,
        "event_version": event_version(),
    })
}

fn build_event_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_event_uid: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    build_link_row(
        ctx,
        event_uid,
        linked_event_uid,
        "",
        link_type,
        metadata_json,
    )
}

fn build_external_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_external_id: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    build_link_row(
        ctx,
        event_uid,
        "",
        linked_external_id,
        link_type,
        metadata_json,
    )
}

fn build_tool_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    tool_call_id: &str,
    parent_tool_call_id: &str,
    tool_name: &str,
    tool_phase: &str,
    tool_error: u8,
    input_json: &str,
    output_json: &str,
    output_text: &str,
) -> Value {
    let input_json = truncate_chars(input_json, TEXT_LIMIT);
    let output_json = truncate_chars(output_json, TEXT_LIMIT);
    let output_text = truncate_chars(output_text, TEXT_LIMIT);

    json!({
        "event_uid": event_uid,
        "session_id": ctx.session_id,
        "provider": ctx.provider,
        "source_name": ctx.source_name,
        "tool_call_id": tool_call_id,
        "parent_tool_call_id": parent_tool_call_id,
        "tool_name": tool_name,
        "tool_phase": tool_phase,
        "tool_error": tool_error,
        "input_json": input_json,
        "output_json": output_json,
        "output_text": output_text,
        "input_bytes": input_json.len() as u32,
        "output_bytes": output_json.len() as u32,
        "input_preview": truncate_chars(&input_json, PREVIEW_LIMIT),
        "output_preview": truncate_chars(&output_text, PREVIEW_LIMIT),
        "io_hash": io_hash(&input_json, &output_json),
        "source_ref": format!("{}:{}:{}", ctx.source_file, ctx.source_generation, ctx.source_line_no),
        "event_version": event_version(),
    })
}

fn normalize_codex_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let mut links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let payload = record.get("payload").cloned().unwrap_or(Value::Null);
    let payload_obj = payload.as_object().cloned().unwrap_or_else(Map::new);
    let payload_json = compact_json(&Value::Object(payload_obj.clone()));

    let push_parent_link = |links: &mut Vec<Value>, uid: &str, parent: &str| {
        if !parent.is_empty() {
            links.push(build_external_link_row(
                ctx,
                uid,
                parent,
                "parent_event",
                "{}",
            ));
        }
    };

    match top_type {
        "session_meta" => {
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "session_meta",
                "session_meta",
                "system",
                "",
                &payload_json,
            );
            row.insert("item_id".to_string(), json!(to_str(payload_obj.get("id"))));
            events.push(Value::Object(row));
        }
        "turn_context" => {
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "turn_context",
                "turn_context",
                "system",
                "",
                &payload_json,
            );
            row.insert(
                "turn_index".to_string(),
                json!(to_u32(payload_obj.get("turn_id"))),
            );
            let turn_id = to_str(payload_obj.get("turn_id"));
            if !turn_id.is_empty() {
                row.insert("request_id".to_string(), json!(turn_id.clone()));
                row.insert("item_id".to_string(), json!(turn_id));
            }
            let model = canonicalize_model("codex", &to_str(payload_obj.get("model")));
            if !model.is_empty() {
                row.insert("model".to_string(), json!(model));
            }
            events.push(Value::Object(row));
        }
        "response_item" => {
            let payload_type = to_str(payload_obj.get("type"));
            match payload_type.as_str() {
                "message" => {
                    let role = to_str(payload_obj.get("role"));
                    let content = payload_obj.get("content").cloned().unwrap_or(Value::Null);
                    let text = extract_message_text(&content);
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "message",
                        "message",
                        if role.is_empty() {
                            "assistant"
                        } else {
                            role.as_str()
                        },
                        &text,
                        &payload_json,
                    );
                    row.insert(
                        "content_types".to_string(),
                        json!(extract_content_types(&content)),
                    );
                    row.insert("item_id".to_string(), json!(to_str(payload_obj.get("id"))));
                    row.insert(
                        "op_status".to_string(),
                        json!(to_str(payload_obj.get("phase"))),
                    );
                    events.push(Value::Object(row));
                }
                "function_call" => {
                    let args = to_str(payload_obj.get("arguments"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let name = to_str(payload_obj.get("name"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "function_call",
                        "assistant",
                        &args,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("tool_name".to_string(), json!(name.clone()));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx, base_uid, &call_id, "", &name, "request", 0, &args, "", "",
                    ));
                }
                "function_call_output" => {
                    let output = to_str(payload_obj.get("output"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_result",
                        "function_call_output",
                        "tool",
                        &output,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx,
                        base_uid,
                        &call_id,
                        "",
                        "",
                        "response",
                        0,
                        "",
                        &compact_json(payload_obj.get("output").unwrap_or(&Value::Null)),
                        &output,
                    ));
                }
                "custom_tool_call" => {
                    let input = to_str(payload_obj.get("input"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let name = to_str(payload_obj.get("name"));
                    let status = to_str(payload_obj.get("status"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "custom_tool_call",
                        "assistant",
                        &input,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("tool_name".to_string(), json!(name.clone()));
                    row.insert("op_status".to_string(), json!(status));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx, base_uid, &call_id, "", &name, "request", 0, &input, "", "",
                    ));
                }
                "custom_tool_call_output" => {
                    let output = to_str(payload_obj.get("output"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let status = to_str(payload_obj.get("status"));
                    let output_json = serde_json::from_str::<Value>(&output)
                        .map(|parsed| compact_json(&parsed))
                        .unwrap_or_else(|_| {
                            compact_json(payload_obj.get("output").unwrap_or(&Value::Null))
                        });

                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_result",
                        "custom_tool_call_output",
                        "tool",
                        &output,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("op_status".to_string(), json!(status));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx,
                        base_uid,
                        &call_id,
                        "",
                        "",
                        "response",
                        0,
                        "",
                        &output_json,
                        &output,
                    ));
                }
                "web_search_call" => {
                    let action = payload_obj.get("action").cloned().unwrap_or(Value::Null);
                    let action_type = to_str(action.get("type"));
                    let status = to_str(payload_obj.get("status"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "web_search_call",
                        "assistant",
                        &extract_message_text(&action),
                        &payload_json,
                    );
                    row.insert("tool_name".to_string(), json!("web_search"));
                    row.insert("op_kind".to_string(), json!(action_type));
                    row.insert("op_status".to_string(), json!(status.clone()));
                    row.insert("tool_phase".to_string(), json!(status));
                    events.push(Value::Object(row));
                }
                "reasoning" => {
                    let summary = payload_obj.get("summary").cloned().unwrap_or(Value::Null);
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "reasoning",
                        "reasoning",
                        "assistant",
                        &extract_message_text(&summary),
                        &payload_json,
                    );
                    row.insert("has_reasoning".to_string(), json!(1u8));
                    row.insert("item_id".to_string(), json!(to_str(payload_obj.get("id"))));
                    events.push(Value::Object(row));
                }
                _ => {
                    events.push(Value::Object(base_event_obj(
                        ctx,
                        base_uid,
                        "unknown",
                        if payload_type.is_empty() {
                            "response_item"
                        } else {
                            payload_type.as_str()
                        },
                        "system",
                        &extract_message_text(&payload),
                        &payload_json,
                    )));
                }
            }
        }
        "event_msg" => {
            let payload_type = to_str(payload_obj.get("type"));
            let actor = match payload_type.as_str() {
                "user_message" => "user",
                "agent_message" | "agent_reasoning" => "assistant",
                _ => "system",
            };
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "event_msg",
                if payload_type.is_empty() {
                    "event_msg"
                } else {
                    payload_type.as_str()
                },
                actor,
                &extract_message_text(&payload),
                &payload_json,
            );
            let turn_id = to_str(payload_obj.get("turn_id"));
            if !turn_id.is_empty() {
                row.insert("request_id".to_string(), json!(turn_id.clone()));
                row.insert("item_id".to_string(), json!(turn_id));
            }
            let status = to_str(payload_obj.get("status"));
            if !status.is_empty() {
                row.insert("op_status".to_string(), json!(status));
            }
            if payload_type == "token_count" {
                let usage = payload_obj
                    .get("info")
                    .and_then(|v| v.get("last_token_usage"));
                let input_tokens = to_u32(usage.and_then(|v| v.get("input_tokens")));
                let output_tokens = to_u32(usage.and_then(|v| v.get("output_tokens")));
                let cache_read_tokens = to_u32(
                    usage
                        .and_then(|v| v.get("cached_input_tokens"))
                        .or_else(|| usage.and_then(|v| v.get("cache_read_input_tokens"))),
                );
                let cache_write_tokens = to_u32(
                    usage
                        .and_then(|v| v.get("cache_creation_input_tokens"))
                        .or_else(|| usage.and_then(|v| v.get("cache_write_input_tokens"))),
                );

                let model = to_str(
                    payload_obj
                        .get("rate_limits")
                        .and_then(|v| v.get("limit_name")),
                );
                let fallback_model = to_str(payload_obj.get("model"));
                let fallback_limit_id = to_str(
                    payload_obj
                        .get("rate_limits")
                        .and_then(|v| v.get("limit_id")),
                );
                let resolved_model = if !model.is_empty() {
                    canonicalize_model("codex", &model)
                } else if !fallback_model.is_empty() {
                    canonicalize_model("codex", &fallback_model)
                } else if !fallback_limit_id.is_empty() {
                    canonicalize_model("codex", &fallback_limit_id)
                } else {
                    canonicalize_model("codex", model_hint)
                };

                row.insert("input_tokens".to_string(), json!(input_tokens));
                row.insert("output_tokens".to_string(), json!(output_tokens));
                row.insert("cache_read_tokens".to_string(), json!(cache_read_tokens));
                row.insert("cache_write_tokens".to_string(), json!(cache_write_tokens));
                if !resolved_model.is_empty() {
                    row.insert("model".to_string(), json!(resolved_model));
                }
                row.insert(
                    "service_tier".to_string(),
                    json!(to_str(
                        payload_obj
                            .get("rate_limits")
                            .and_then(|v| v.get("plan_type"))
                    )),
                );
                row.insert(
                    "token_usage_json".to_string(),
                    json!(compact_json(&payload)),
                );
            } else if payload_type == "agent_reasoning" {
                row.insert("has_reasoning".to_string(), json!(1u8));
            }
            events.push(Value::Object(row));
        }
        "compacted" => {
            events.push(Value::Object(base_event_obj(
                ctx,
                base_uid,
                "compacted_raw",
                "compacted",
                "system",
                "",
                &payload_json,
            )));

            if let Some(Value::Array(items)) = payload_obj.get("replacement_history") {
                for (idx, item) in items.iter().enumerate() {
                    let item_uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &compact_json(item),
                        &format!("compacted:{}", idx),
                    );
                    let item_type = to_str(item.get("type"));

                    let (kind, payload_type, actor, text) = match item_type.as_str() {
                        "message" => (
                            "message",
                            "message",
                            to_str(item.get("role")),
                            extract_message_text(item.get("content").unwrap_or(&Value::Null)),
                        ),
                        "function_call" => (
                            "tool_call",
                            "function_call",
                            "assistant".to_string(),
                            to_str(item.get("arguments")),
                        ),
                        "function_call_output" => (
                            "tool_result",
                            "function_call_output",
                            "tool".to_string(),
                            to_str(item.get("output")),
                        ),
                        "reasoning" => (
                            "reasoning",
                            "reasoning",
                            "assistant".to_string(),
                            extract_message_text(item.get("summary").unwrap_or(&Value::Null)),
                        ),
                        _ => (
                            "unknown",
                            if item_type.is_empty() {
                                "unknown"
                            } else {
                                item_type.as_str()
                            },
                            "system".to_string(),
                            extract_message_text(item),
                        ),
                    };

                    let mut row = base_event_obj(
                        ctx,
                        &item_uid,
                        kind,
                        payload_type,
                        if actor.is_empty() {
                            "assistant"
                        } else {
                            actor.as_str()
                        },
                        &text,
                        &compact_json(item),
                    );
                    row.insert("origin_event_id".to_string(), json!(base_uid));
                    events.push(Value::Object(row));

                    links.push(build_event_link_row(
                        ctx,
                        &item_uid,
                        base_uid,
                        "compacted_parent",
                        "{}",
                    ));
                }
            }
        }
        "message" | "function_call" | "function_call_output" | "reasoning" => {
            let event = if top_type == "message" {
                let role = to_str(record.get("role"));
                let text = extract_message_text(record.get("content").unwrap_or(&Value::Null));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "message",
                    "message",
                    if role.is_empty() {
                        "assistant"
                    } else {
                        role.as_str()
                    },
                    &text,
                    &compact_json(record),
                );
                row.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(
                        record.get("content").unwrap_or(&Value::Null)
                    )),
                );
                Value::Object(row)
            } else if top_type == "function_call" {
                let args = to_str(record.get("arguments"));
                let call_id = to_str(record.get("call_id"));
                let name = to_str(record.get("name"));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "tool_call",
                    "function_call",
                    "assistant",
                    &args,
                    &compact_json(record),
                );
                row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                row.insert("tool_name".to_string(), json!(name.clone()));
                tools.push(build_tool_row(
                    ctx, base_uid, &call_id, "", &name, "request", 0, &args, "", "",
                ));
                Value::Object(row)
            } else if top_type == "function_call_output" {
                let output = to_str(record.get("output"));
                let call_id = to_str(record.get("call_id"));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "tool_result",
                    "function_call_output",
                    "tool",
                    &output,
                    &compact_json(record),
                );
                row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                tools.push(build_tool_row(
                    ctx,
                    base_uid,
                    &call_id,
                    "",
                    "",
                    "response",
                    0,
                    "",
                    &compact_json(record.get("output").unwrap_or(&Value::Null)),
                    &output,
                ));
                Value::Object(row)
            } else {
                let summary = record.get("summary").cloned().unwrap_or(Value::Null);
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "reasoning",
                    "reasoning",
                    "assistant",
                    &extract_message_text(&summary),
                    &compact_json(record),
                );
                row.insert("has_reasoning".to_string(), json!(1u8));
                Value::Object(row)
            };

            events.push(event);
        }
        _ => {
            events.push(Value::Object(base_event_obj(
                ctx,
                base_uid,
                "unknown",
                if top_type.is_empty() {
                    "unknown"
                } else {
                    top_type
                },
                "system",
                &extract_message_text(record),
                &compact_json(record),
            )));
        }
    }

    let payload_model = canonicalize_model("codex", &to_str(payload_obj.get("model")));
    let inherited_model = canonicalize_model("codex", model_hint);
    for event in &mut events {
        if let Some(row) = event.as_object_mut() {
            let row_model = canonicalize_model("codex", &to_str(row.get("model")));
            let resolved_model = if !row_model.is_empty() {
                row_model
            } else if !payload_model.is_empty() {
                payload_model.clone()
            } else {
                inherited_model.clone()
            };

            if !resolved_model.is_empty() {
                row.insert("model".to_string(), json!(resolved_model));
            }
        }
    }

    let parent = to_str(record.get("parent_id"));
    if !events.is_empty() && !parent.is_empty() {
        if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
            push_parent_link(&mut links, uid, &parent);
        }
    }

    (events, links, tools)
}

fn normalize_claude_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let mut links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let parent_uuid = to_str(record.get("parentUuid"));
    let request_id = to_str(record.get("requestId"));
    let trace_id = to_str(record.get("requestId"));
    let agent_run_id = to_str(record.get("agentId"));
    let agent_label = to_str(record.get("agentName"));
    let coord_group_label = to_str(record.get("teamName"));
    let is_substream = to_u8_bool(record.get("isSidechain"));

    let message = record.get("message").cloned().unwrap_or(Value::Null);
    let msg_role = to_str(message.get("role"));
    let model = canonicalize_model("claude", &to_str(message.get("model")));

    let usage = message.get("usage").cloned().unwrap_or(Value::Null);
    let input_tokens = to_u32(usage.get("input_tokens"));
    let output_tokens = to_u32(usage.get("output_tokens"));
    let cache_read_tokens = to_u32(usage.get("cache_read_input_tokens"));
    let cache_write_tokens = to_u32(usage.get("cache_creation_input_tokens"));
    let service_tier = to_str(usage.get("service_tier"));

    let stamp_common = |obj: &mut Map<String, Value>| {
        obj.insert("request_id".to_string(), json!(request_id.clone()));
        obj.insert("trace_id".to_string(), json!(trace_id.clone()));
        obj.insert("agent_run_id".to_string(), json!(agent_run_id.clone()));
        obj.insert("agent_label".to_string(), json!(agent_label.clone()));
        obj.insert(
            "coord_group_label".to_string(),
            json!(coord_group_label.clone()),
        );
        obj.insert("is_substream".to_string(), json!(is_substream));
        obj.insert("model".to_string(), json!(model.clone()));
        obj.insert("input_tokens".to_string(), json!(input_tokens));
        obj.insert("output_tokens".to_string(), json!(output_tokens));
        obj.insert("cache_read_tokens".to_string(), json!(cache_read_tokens));
        obj.insert("cache_write_tokens".to_string(), json!(cache_write_tokens));
        obj.insert("service_tier".to_string(), json!(service_tier.clone()));
        obj.insert("item_id".to_string(), json!(to_str(record.get("uuid"))));
        obj.insert(
            "origin_event_id".to_string(),
            json!(to_str(record.get("sourceToolAssistantUUID"))),
        );
        obj.insert(
            "origin_tool_call_id".to_string(),
            json!(to_str(record.get("sourceToolUseID"))),
        );
    };

    if top_type == "assistant" || top_type == "user" {
        let actor = if top_type == "assistant" {
            "assistant"
        } else if msg_role == "assistant" {
            "assistant"
        } else {
            "user"
        };

        let content = message.get("content").cloned().unwrap_or_else(|| {
            record
                .get("message")
                .and_then(|m| m.get("content"))
                .cloned()
                .unwrap_or(Value::Null)
        });

        match content {
            Value::Array(items) if !items.is_empty() => {
                for (idx, item) in items.iter().enumerate() {
                    let block_type = to_str(item.get("type"));
                    let suffix = format!("claude:block:{}", idx);
                    let block_uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &compact_json(item),
                        &suffix,
                    );

                    let mut row = match block_type.as_str() {
                        "thinking" => {
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "reasoning",
                                "thinking",
                                "assistant",
                                &extract_message_text(item),
                                &compact_json(item),
                            );
                            r.insert("has_reasoning".to_string(), json!(1u8));
                            r.insert("content_types".to_string(), json!(["thinking"]));
                            r
                        }
                        "tool_use" => {
                            let tool_call_id = to_str(item.get("id"));
                            let tool_name = to_str(item.get("name"));
                            let input_json =
                                compact_json(item.get("input").unwrap_or(&Value::Null));
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "tool_call",
                                "tool_use",
                                "assistant",
                                &extract_message_text(item.get("input").unwrap_or(&Value::Null)),
                                &compact_json(item),
                            );
                            r.insert("content_types".to_string(), json!(["tool_use"]));
                            r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                            r.insert("tool_name".to_string(), json!(tool_name.clone()));
                            tools.push(build_tool_row(
                                ctx,
                                &block_uid,
                                &tool_call_id,
                                &to_str(record.get("parentToolUseID")),
                                &tool_name,
                                "request",
                                0,
                                &input_json,
                                "",
                                "",
                            ));
                            r
                        }
                        "tool_result" => {
                            let tool_call_id = to_str(item.get("tool_use_id"));
                            let output_json =
                                compact_json(item.get("content").unwrap_or(&Value::Null));
                            let output_text =
                                extract_message_text(item.get("content").unwrap_or(&Value::Null));
                            let tool_error = to_u8_bool(item.get("is_error"));
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "tool_result",
                                "tool_result",
                                "tool",
                                &output_text,
                                &compact_json(item),
                            );
                            r.insert("content_types".to_string(), json!(["tool_result"]));
                            r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                            r.insert("tool_error".to_string(), json!(tool_error));
                            tools.push(build_tool_row(
                                ctx,
                                &block_uid,
                                &tool_call_id,
                                &to_str(record.get("parentToolUseID")),
                                "",
                                "response",
                                tool_error,
                                "",
                                &output_json,
                                &output_text,
                            ));
                            r
                        }
                        _ => {
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "message",
                                if block_type.is_empty() {
                                    "text"
                                } else {
                                    block_type.as_str()
                                },
                                actor,
                                &extract_message_text(item),
                                &compact_json(item),
                            );
                            if !block_type.is_empty() {
                                r.insert("content_types".to_string(), json!([block_type]));
                            }
                            r
                        }
                    };

                    stamp_common(&mut row);
                    row.insert(
                        "parent_tool_call_id".to_string(),
                        json!(to_str(record.get("parentToolUseID"))),
                    );
                    row.insert(
                        "origin_tool_call_id".to_string(),
                        json!(to_str(record.get("sourceToolUseID"))),
                    );
                    row.insert(
                        "tool_phase".to_string(),
                        json!(to_str(record.get("stop_reason"))),
                    );
                    events.push(Value::Object(row));

                    if !parent_uuid.is_empty() {
                        links.push(build_external_link_row(
                            ctx,
                            &block_uid,
                            &parent_uuid,
                            "parent_uuid",
                            "{}",
                        ));
                    }
                }
            }
            _ => {
                let text = extract_message_text(&message);
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "message",
                    "message",
                    actor,
                    &text,
                    &compact_json(record),
                );
                row.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(
                        message.get("content").unwrap_or(&Value::Null)
                    )),
                );
                stamp_common(&mut row);
                events.push(Value::Object(row));
                if !parent_uuid.is_empty() {
                    links.push(build_external_link_row(
                        ctx,
                        base_uid,
                        &parent_uuid,
                        "parent_uuid",
                        "{}",
                    ));
                }
            }
        }
    } else {
        let event_kind = match top_type {
            "progress" => "progress",
            "system" => "system",
            "summary" => "summary",
            "queue-operation" => "queue_operation",
            "file-history-snapshot" => "file_history_snapshot",
            _ => "unknown",
        };

        let payload_type = if top_type == "progress" {
            to_str(record.get("data").and_then(|d| d.get("type")))
        } else if top_type == "system" {
            to_str(record.get("subtype"))
        } else {
            top_type.to_string()
        };

        let mut row = base_event_obj(
            ctx,
            base_uid,
            event_kind,
            if payload_type.is_empty() {
                top_type
            } else {
                payload_type.as_str()
            },
            "system",
            &extract_message_text(record),
            &compact_json(record),
        );
        row.insert("op_kind".to_string(), json!(payload_type));
        row.insert("op_status".to_string(), json!(to_str(record.get("status"))));
        row.insert(
            "latency_ms".to_string(),
            json!(to_u32(record.get("durationMs"))),
        );
        row.insert(
            "retry_count".to_string(),
            json!(to_u16(record.get("retryAttempt"))),
        );
        stamp_common(&mut row);
        events.push(Value::Object(row));

        if !parent_uuid.is_empty() {
            links.push(build_external_link_row(
                ctx,
                base_uid,
                &parent_uuid,
                "parent_uuid",
                "{}",
            ));
        }
    }

    if !events.is_empty() {
        let tool_use_id = to_str(record.get("toolUseID"));
        if !tool_use_id.is_empty() {
            if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
                links.push(build_external_link_row(
                    ctx,
                    uid,
                    &tool_use_id,
                    "tool_use_id",
                    "{}",
                ));
            }
        }

        let source_tool_assistant = to_str(record.get("sourceToolAssistantUUID"));
        if !source_tool_assistant.is_empty() {
            if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
                links.push(build_external_link_row(
                    ctx,
                    uid,
                    &source_tool_assistant,
                    "source_tool_assistant",
                    "{}",
                ));
            }
        }
    }

    (events, links, tools)
}

pub fn normalize_record(
    record: &Value,
    source_name: &str,
    provider: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
    model_hint: &str,
) -> Result<NormalizedRecord> {
    let provider = Provider::parse(provider)?;
    let provider_name = provider.as_str();
    let record_ts = to_str(record.get("timestamp"));
    let (event_ts, event_ts_parse_failed) = parse_event_ts(&record_ts);
    let top_type = to_str(record.get("type"));

    let mut session_id = if provider == Provider::Claude {
        to_str(record.get("sessionId"))
    } else {
        String::new()
    };
    if session_id.is_empty() {
        session_id = if session_hint.is_empty() {
            infer_session_id_from_file(source_file)
        } else {
            session_hint.to_string()
        };
    }

    if provider == Provider::Codex && top_type == "session_meta" {
        let payload = record.get("payload").cloned().unwrap_or(Value::Null);
        let payload_id = to_str(payload.get("id"));
        if !payload_id.is_empty() {
            session_id = payload_id;
        }
    }

    let session_date = infer_session_date_from_file(source_file, &record_ts);

    let raw_json = compact_json(record);
    let base_uid = event_uid(
        source_file,
        source_generation,
        source_line_no,
        source_offset,
        &raw_json,
        "raw",
    );

    let raw_row = json!({
        "source_name": source_name,
        "provider": provider_name,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": source_offset,
        "record_ts": record_ts,
        "top_type": top_type,
        "session_id": session_id,
        "raw_json": raw_json,
        "raw_json_hash": raw_hash(&raw_json),
        "event_uid": base_uid,
    });

    let mut error_rows = Vec::<Value>::new();
    if event_ts_parse_failed {
        error_rows.push(json!({
            "source_name": source_name,
            "provider": provider_name,
            "source_file": source_file,
            "source_inode": source_inode,
            "source_generation": source_generation,
            "source_line_no": source_line_no,
            "source_offset": source_offset,
            "error_kind": "timestamp_parse_error",
            "error_text": format!(
                "timestamp is missing or not RFC3339; used {} UTC fallback",
                UNPARSEABLE_EVENT_TS
            ),
            "raw_fragment": truncate_chars(&record_ts, 20_000),
        }));
    }

    let ctx = RecordContext {
        source_name,
        provider: provider_name,
        session_id: &session_id,
        session_date: &session_date,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset,
        record_ts: &record_ts,
        event_ts: &event_ts,
    };

    let (event_rows, link_rows, tool_rows) = if provider == Provider::Claude {
        normalize_claude_event(record, &ctx, &top_type, &base_uid)
    } else {
        normalize_codex_event(record, &ctx, &top_type, &base_uid, model_hint)
    };
    let model_hint = resolve_model_hint(&event_rows, provider_name, model_hint);

    Ok(NormalizedRecord {
        raw_row,
        event_rows,
        link_rows,
        tool_rows,
        error_rows,
        session_hint: session_id,
        model_hint,
    })
}

#[cfg(test)]
mod tests {
    use super::normalize_record;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn codex_tool_call_normalization() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_123",
                "name": "Read",
                "arguments": "{\"path\":\"README.md\"}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/13/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
            123,
            1,
            42,
            1024,
            "",
            "",
        )
        .expect("codex tool call should normalize");

        assert_eq!(out.event_rows.len(), 1);
        assert_eq!(out.tool_rows.len(), 1);
        assert!(out.error_rows.is_empty());
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("event_kind").unwrap().as_str().unwrap(),
            "tool_call"
        );
        assert_eq!(row.get("tool_name").unwrap().as_str().unwrap(), "Read");
    }

    #[test]
    fn codex_turn_context_promotes_model_and_turn_id() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:42.191Z",
            "type": "turn_context",
            "payload": {
                "turn_id": "019c5f6a-49bd-7920-ac67-1dd8e33b0e95",
                "model": "gpt-5.3-codex"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            1,
            1,
            "",
            "",
        )
        .expect("codex turn context should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "turn_context"
        );
        assert_eq!(row.get("model").unwrap().as_str().unwrap(), "gpt-5.3-codex");
        assert_eq!(
            row.get("request_id").unwrap().as_str().unwrap(),
            "019c5f6a-49bd-7920-ac67-1dd8e33b0e95"
        );
        assert_eq!(
            row.get("item_id").unwrap().as_str().unwrap(),
            "019c5f6a-49bd-7920-ac67-1dd8e33b0e95"
        );
    }

    #[test]
    fn codex_token_count_promotes_usage_fields() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 65323,
                        "output_tokens": 445,
                        "cached_input_tokens": 58624
                    }
                },
                "rate_limits": {
                    "limit_name": "GPT-5.3-Codex-Spark",
                    "limit_id": "codex_bengalfox",
                    "plan_type": "pro"
                }
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            2,
            2,
            "",
            "",
        )
        .expect("codex token count should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "token_count"
        );
        assert_eq!(row.get("input_tokens").unwrap().as_u64().unwrap(), 65323);
        assert_eq!(row.get("output_tokens").unwrap().as_u64().unwrap(), 445);
        assert_eq!(
            row.get("cache_read_tokens").unwrap().as_u64().unwrap(),
            58624
        );
        assert_eq!(
            row.get("model").unwrap().as_str().unwrap(),
            "gpt-5.3-codex-spark"
        );
        assert_eq!(row.get("service_tier").unwrap().as_str().unwrap(), "pro");
        assert!(!row
            .get("token_usage_json")
            .unwrap()
            .as_str()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn codex_token_count_alias_codex_maps_to_xhigh() {
        let record = json!({
            "timestamp": "2026-02-15T04:52:55.538Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 72636,
                        "output_tokens": 285,
                        "cached_input_tokens": 70784
                    }
                },
                "rate_limits": {
                    "limit_id": "codex",
                    "limit_name": null,
                    "plan_type": "pro"
                }
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            4,
            4,
            "",
            "",
        )
        .expect("codex token count alias should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("model").unwrap().as_str().unwrap(),
            "gpt-5.3-codex-xhigh"
        );
    }

    #[test]
    fn codex_custom_tool_call_promotes_tool_fields() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "call_id": "call_abc",
                "name": "apply_patch",
                "status": "completed",
                "input": "*** Begin Patch\n*** End Patch\n"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            3,
            3,
            "",
            "",
        )
        .expect("codex custom tool call should normalize");

        assert_eq!(out.event_rows.len(), 1);
        assert_eq!(out.tool_rows.len(), 1);
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("event_kind").unwrap().as_str().unwrap(),
            "tool_call"
        );
        assert_eq!(
            row.get("tool_call_id").unwrap().as_str().unwrap(),
            "call_abc"
        );
        assert_eq!(
            row.get("tool_name").unwrap().as_str().unwrap(),
            "apply_patch"
        );
        assert_eq!(row.get("op_status").unwrap().as_str().unwrap(), "completed");
    }

    #[test]
    fn claude_tool_use_and_result_blocks() {
        let record = json!({
            "type": "assistant",
            "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
            "uuid": "assistant-1",
            "parentUuid": "user-1",
            "requestId": "req-1",
            "timestamp": "2026-01-19T15:58:41.421Z",
            "message": {
                "model": "claude-opus-4-5-20251101",
                "role": "assistant",
                "usage": {
                    "input_tokens": 9,
                    "output_tokens": 5,
                    "cache_creation_input_tokens": 19630,
                    "cache_read_input_tokens": 0,
                    "service_tier": "standard"
                },
                "content": [
                    {
                        "type": "tool_use",
                        "id": "toolu_1",
                        "name": "WebFetch",
                        "input": {"url": "https://example.com"}
                    },
                    {
                        "type": "text",
                        "text": "done"
                    }
                ]
            }
        });

        let out = normalize_record(
            &record,
            "claude",
            "claude",
            "/Users/eric/.claude/projects/p1/s1.jsonl",
            55,
            2,
            10,
            100,
            "",
            "",
        )
        .expect("claude event should normalize");

        assert_eq!(out.event_rows.len(), 2);
        assert_eq!(out.tool_rows.len(), 1);

        let first = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            first.get("event_kind").unwrap().as_str().unwrap(),
            "tool_call"
        );
        assert_eq!(first.get("provider").unwrap().as_str().unwrap(), "claude");
        assert!(out.error_rows.is_empty());
    }

    #[test]
    fn invalid_timestamp_uses_epoch_and_emits_timestamp_parse_error() {
        let record = json!({
            "timestamp": "not-a-timestamp",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_bad_ts",
                "name": "Read",
                "arguments": "{}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            9,
            2,
            7,
            99,
            "",
            "",
        )
        .expect("codex event with invalid timestamp should normalize");

        let event_row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            event_row.get("event_ts").unwrap().as_str().unwrap(),
            "1970-01-01 00:00:00.000"
        );
        assert_eq!(
            event_row.get("session_date").unwrap().as_str().unwrap(),
            "1970-01-01"
        );

        assert_eq!(out.error_rows.len(), 1);
        let error = out.error_rows[0].as_object().unwrap();
        assert_eq!(
            error.get("error_kind").unwrap().as_str().unwrap(),
            "timestamp_parse_error"
        );
        assert_eq!(
            error.get("raw_fragment").unwrap().as_str().unwrap(),
            "not-a-timestamp"
        );
    }

    #[test]
    fn invalid_timestamp_preserves_session_date_from_source_path() {
        let record = json!({
            "timestamp": "still-not-a-timestamp",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_bad_ts",
                "name": "Read",
                "arguments": "{}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/16/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            11,
            4,
            12,
            144,
            "",
            "",
        )
        .expect("codex event should normalize while preserving session date from path");

        let event_row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            event_row.get("event_ts").unwrap().as_str().unwrap(),
            "1970-01-01 00:00:00.000"
        );
        assert_eq!(
            event_row.get("session_date").unwrap().as_str().unwrap(),
            "2026-02-16"
        );
        assert_eq!(out.error_rows.len(), 1);
    }

    #[test]
    fn unknown_provider_is_rejected() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:42.191Z",
            "type": "turn_context",
        });

        let err = normalize_record(
            &record,
            "unknown",
            "unknown",
            "/tmp/sessions/session-1.jsonl",
            1,
            1,
            1,
            1,
            "",
            "",
        )
        .expect_err("unknown provider should be rejected");

        assert!(
            err.to_string().contains("unsupported provider"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn claude_links_split_event_uids_from_external_ids() {
        let record = json!({
            "type": "assistant",
            "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
            "uuid": "assistant-2",
            "parentUuid": "user-parent-2",
            "toolUseID": "toolu_42",
            "sourceToolAssistantUUID": "assistant-root-1",
            "requestId": "req-2",
            "timestamp": "2026-01-19T15:59:41.421Z",
            "message": {
                "role": "assistant",
                "content": "done"
            }
        });

        let out = normalize_record(
            &record,
            "claude",
            "claude",
            "/Users/eric/.claude/projects/p1/s1.jsonl",
            55,
            2,
            11,
            101,
            "",
            "",
        );

        assert_eq!(out.link_rows.len(), 3);

        let by_type = out
            .link_rows
            .iter()
            .map(|row| {
                let obj = row.as_object().expect("link row object");
                let link_type = obj
                    .get("link_type")
                    .and_then(|v| v.as_str())
                    .expect("link_type")
                    .to_string();
                (link_type, obj.clone())
            })
            .collect::<HashMap<_, _>>();

        let parent = by_type.get("parent_uuid").expect("parent_uuid link");
        assert_eq!(
            parent
                .get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            "user-parent-2"
        );
        assert_eq!(
            parent
                .get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );

        let tool_use = by_type.get("tool_use_id").expect("tool_use_id link");
        assert_eq!(
            tool_use
                .get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            "toolu_42"
        );
        assert_eq!(
            tool_use
                .get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );

        let source_tool = by_type
            .get("source_tool_assistant")
            .expect("source_tool_assistant link");
        assert_eq!(
            source_tool
                .get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            "assistant-root-1"
        );
        assert_eq!(
            source_tool
                .get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );
    }

    #[test]
    fn codex_compacted_parent_link_uses_event_uid_target() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "compacted",
            "payload": {
                "replacement_history": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": "hello"}
                        ]
                    }
                ]
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            12,
            12,
            "",
            "",
        );

        let compacted_uid = out.event_rows[0]
            .get("event_uid")
            .and_then(|v| v.as_str())
            .expect("compacted event uid");
        let link = out.link_rows[0].as_object().expect("compacted link");

        assert_eq!(
            link.get("link_type").and_then(|v| v.as_str()).unwrap(),
            "compacted_parent"
        );
        assert_eq!(
            link.get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            compacted_uid
        );
        assert_eq!(
            link.get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );
    }
}
