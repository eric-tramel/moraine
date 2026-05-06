use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use regex::Regex;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const TEXT_LIMIT: usize = 200_000;
pub(crate) const PREVIEW_LIMIT: usize = 320;
pub(crate) const UNPARSEABLE_EVENT_TS: &str = "1970-01-01 00:00:00.000";

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

pub(crate) fn to_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

pub(crate) fn to_u32(value: Option<&Value>) -> u32 {
    match value {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        Some(Value::String(s)) => s.parse::<u64>().unwrap_or(0).min(u32::MAX as u64) as u32,
        _ => 0,
    }
}

pub(crate) fn to_u64(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(Value::String(s)) => s.parse::<u64>().unwrap_or(0),
        _ => 0,
    }
}

pub(crate) fn to_u16(value: Option<&Value>) -> u16 {
    to_u32(value).min(u16::MAX as u32) as u16
}

pub(crate) fn to_u8_bool(value: Option<&Value>) -> u8 {
    match value {
        Some(Value::Bool(v)) => u8::from(*v),
        Some(Value::Number(v)) => u8::from(v.as_i64().unwrap_or(0) != 0),
        Some(Value::String(s)) => {
            let lower = s.to_ascii_lowercase();
            u8::from(lower == "true" || lower == "1")
        }
        _ => 0,
    }
}

const TOKEN_BUCKET_KEYS: &[&str] = &[
    "input_text",
    "output_text",
    "input_cache_read",
    "input_cache_write",
    "input_image",
    "output_image",
    "input_audio",
    "output_audio",
    "reasoning",
    "server_tool_use",
    "embedding_input_text",
    "embedding_input_image",
    "other",
];

const TOKEN_NATIVE_UNIT_KEYS: &[&str] = &[
    "input_image_pixels",
    "output_image_pixels",
    "input_audio_seconds",
    "output_audio_seconds",
    "input_images",
    "output_images",
];

fn zero_numeric_map(keys: &[&str]) -> Map<String, Value> {
    keys.iter()
        .map(|key| ((*key).to_string(), json!(0u64)))
        .collect()
}

fn zero_float_map(keys: &[&str]) -> Map<String, Value> {
    keys.iter()
        .map(|key| ((*key).to_string(), json!(0.0_f64)))
        .collect()
}

pub(crate) fn token_buckets(values: &[(&str, u64)]) -> Value {
    let mut map = zero_numeric_map(TOKEN_BUCKET_KEYS);
    for (key, value) in values {
        map.insert((*key).to_string(), json!(*value));
    }
    Value::Object(map)
}

pub(crate) fn token_native_units(values: &[(&str, f64)]) -> Value {
    let mut map = zero_float_map(TOKEN_NATIVE_UNIT_KEYS);
    for (key, value) in values {
        map.insert((*key).to_string(), json!(*value));
    }
    Value::Object(map)
}

#[derive(Debug, Clone)]
pub(crate) struct TokenBuckets {
    buckets: Map<String, Value>,
    native_units: Map<String, Value>,
}

impl TokenBuckets {
    pub(crate) fn generation(values: &[(&str, u64)]) -> Self {
        let mut buckets = zero_numeric_map(TOKEN_BUCKET_KEYS);
        for (key, value) in values {
            buckets.insert((*key).to_string(), json!(*value));
        }
        Self {
            buckets,
            native_units: zero_float_map(TOKEN_NATIVE_UNIT_KEYS),
        }
    }

    pub(crate) fn into_values(self) -> (Value, Value) {
        (
            Value::Object(self.buckets),
            Value::Object(self.native_units),
        )
    }
}

pub(crate) fn stamp_token_accounting(
    row: &mut Map<String, Value>,
    endpoint_kind: &str,
    buckets: Value,
    native_units: Value,
) {
    row.insert("endpoint_kind".to_string(), json!(endpoint_kind));
    row.insert("token_usage_buckets".to_string(), buckets);
    row.insert("token_usage_native_units".to_string(), native_units);
}

pub(crate) fn sum_numeric_object(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Object(obj)) => obj.values().map(|value| to_u64(Some(value))).sum(),
        _ => 0,
    }
}

pub(crate) fn generation_token_buckets(
    input_text: u64,
    output_text: u64,
    cache_read: u64,
    cache_write: u64,
) -> Value {
    let (buckets, _native_units) = TokenBuckets::generation(&[
        ("input_text", input_text),
        ("output_text", output_text),
        ("input_cache_read", cache_read),
        ("input_cache_write", cache_write),
    ])
    .into_values();
    buckets
}

pub(crate) fn openai_generation_token_buckets(usage: Option<&Value>) -> Value {
    let input_total = to_u64(usage.and_then(|v| {
        v.get("input_tokens")
            .or_else(|| v.get("prompt_tokens"))
            .or_else(|| v.get("total_input_tokens"))
    }));
    let output_total = to_u64(usage.and_then(|v| {
        v.get("output_tokens")
            .or_else(|| v.get("completion_tokens"))
            .or_else(|| v.get("total_output_tokens"))
    }));
    let input_details = usage.and_then(|v| {
        v.get("input_tokens_details")
            .or_else(|| v.get("prompt_tokens_details"))
    });
    let output_details = usage.and_then(|v| {
        v.get("output_tokens_details")
            .or_else(|| v.get("completion_tokens_details"))
    });

    let cache_read = to_u64(
        usage
            .and_then(|v| v.get("cached_input_tokens"))
            .or_else(|| usage.and_then(|v| v.get("cache_read_input_tokens")))
            .or_else(|| input_details.and_then(|v| v.get("cached_tokens"))),
    );
    let cache_write = to_u64(
        usage
            .and_then(|v| v.get("cache_creation_input_tokens"))
            .or_else(|| usage.and_then(|v| v.get("cache_write_input_tokens")))
            .or_else(|| input_details.and_then(|v| v.get("cache_creation_tokens"))),
    );
    let input_image = to_u64(input_details.and_then(|v| v.get("image_tokens")));
    let input_audio = to_u64(input_details.and_then(|v| v.get("audio_tokens")));
    let output_image = to_u64(output_details.and_then(|v| v.get("image_tokens")));
    let output_audio = to_u64(output_details.and_then(|v| v.get("audio_tokens")));
    let reasoning = to_u64(output_details.and_then(|v| v.get("reasoning_tokens")));
    let server_tool_use = sum_numeric_object(usage.and_then(|v| v.get("server_tool_use")));

    let input_text_detail = to_u64(input_details.and_then(|v| v.get("text_tokens")));
    let output_text_detail = to_u64(output_details.and_then(|v| v.get("text_tokens")));
    let input_text = if input_text_detail > 0 {
        input_text_detail
    } else {
        input_total.saturating_sub(cache_read + cache_write + input_image + input_audio)
    };
    let output_text = if output_text_detail > 0 {
        output_text_detail
    } else {
        output_total.saturating_sub(output_image + output_audio + reasoning + server_tool_use)
    };

    token_buckets(&[
        ("input_text", input_text),
        ("output_text", output_text),
        ("input_cache_read", cache_read),
        ("input_cache_write", cache_write),
        ("input_image", input_image),
        ("output_image", output_image),
        ("input_audio", input_audio),
        ("output_audio", output_audio),
        ("reasoning", reasoning),
        ("server_tool_use", server_tool_use),
    ])
}

pub(crate) fn canonicalize_model(harness: &str, raw_model: &str) -> String {
    let mut model = raw_model.trim().to_ascii_lowercase();
    if model.is_empty() {
        return String::new();
    }

    model = model.replace(' ', "-");

    if harness == "codex" && model == "codex" {
        return "gpt-5.3-codex-xhigh".to_string();
    }

    model
}

pub(crate) fn resolve_model_hint(event_rows: &[Value], harness: &str, fallback: &str) -> String {
    for row in event_rows.iter().rev() {
        if let Some(model) = row.get("model").and_then(Value::as_str) {
            let normalized = canonicalize_model(harness, model);
            if !normalized.is_empty() {
                return normalized;
            }
        }
    }

    canonicalize_model(harness, fallback)
}

/// Split a Hermes `vendor/model` string into `(inference_provider, model)`.
///
/// Hermes trajectories encode the LLM vendor in the `model` field, e.g.
/// `anthropic/claude-sonnet-4.6`. We split on the first slash only: everything
/// before becomes `inference_provider`, everything after is kept verbatim as
/// `model`. If there is no slash, the whole value is treated as a bare model
/// and `inference_provider` is empty.
///
/// Both pieces are lower-cased and trimmed but otherwise left alone — Hermes
/// model strings are already the canonical name in upstream catalogues, so we
/// do not apply dot-to-dash or snapshot-stripping mangling here. Cloud-prefixed
/// forms such as `bedrock/anthropic/claude-opus-4-5` split on the first slash
/// too; that leaves `bedrock` as the vendor and `anthropic/claude-opus-4-5` as
/// the model. Future work can re-nest those, but for now the grammar allows
/// slashes in the model string.
pub(crate) fn split_hermes_vendor_model(raw: &str) -> (String, String) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new());
    }

    match trimmed.split_once('/') {
        Some((vendor, model)) => (
            vendor.trim().to_ascii_lowercase(),
            model.trim().to_ascii_lowercase(),
        ),
        None => (String::new(), trimmed.to_ascii_lowercase()),
    }
}

pub(crate) fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

pub(crate) fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        input.to_string()
    } else {
        input.chars().take(max_chars).collect()
    }
}

pub(crate) fn extract_message_text(content: &Value) -> String {
    fn walk(node: &Value, out: &mut Vec<String>) {
        match node {
            Value::String(s) if !s.trim().is_empty() => {
                out.push(s.clone());
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

pub(crate) fn extract_content_types(content: &Value) -> Vec<String> {
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

pub(crate) fn parse_json_string(value: &str) -> Option<Value> {
    serde_json::from_str::<Value>(value.trim()).ok()
}

pub(crate) fn update_string_field(row: &mut Value, key: &str, value: &str) {
    if let Some(obj) = row.as_object_mut() {
        obj.insert(key.to_string(), json!(value));
    }
}

pub(crate) fn update_u8_field(row: &mut Value, key: &str, value: u8) {
    if let Some(obj) = row.as_object_mut() {
        obj.insert(key.to_string(), json!(value));
    }
}

pub(crate) fn mark_reasoning_metadata(row: &mut Map<String, Value>) {
    row.insert("has_reasoning".to_string(), json!(1u8));
    row.insert("content_types".to_string(), json!(["reasoning"]));
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

pub(crate) fn parse_record_ts(record_ts: &str) -> Option<DateTime<Utc>> {
    let trimmed = record_ts.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

pub(crate) fn format_event_ts(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

pub(crate) fn format_record_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub(crate) fn format_unix_seconds_decimal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.contains(['e', 'E']) {
        return None;
    }

    let (secs_part, frac_part) = trimmed.split_once('.').unwrap_or((trimmed, ""));
    let secs = secs_part.parse::<i64>().ok()?;
    let mut nanos = frac_part
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .take(9)
        .collect::<String>();
    while nanos.len() < 9 {
        nanos.push('0');
    }
    let nanos = nanos.parse::<u32>().ok()?.min(999_999_999);
    DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| format_record_ts(&dt))
}

pub(crate) fn format_unix_seconds_ts(seconds: f64) -> Option<String> {
    if !seconds.is_finite() {
        return None;
    }
    let secs = seconds.trunc() as i64;
    let nanos = (seconds.fract().abs() * 1_000_000_000.0).round() as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos.min(999_999_999)).map(|dt| format_record_ts(&dt))
}

pub(crate) fn parse_event_ts(record_ts: &str) -> (String, bool) {
    if let Some(dt) = parse_record_ts(record_ts) {
        return (format_event_ts(&dt), false);
    }

    (UNPARSEABLE_EVENT_TS.to_string(), true)
}

fn event_kind_in_domain(value: &str) -> bool {
    matches!(
        value,
        "session_meta"
            | "turn_context"
            | "message"
            | "tool_call"
            | "tool_result"
            | "reasoning"
            | "event_msg"
            | "compacted_raw"
            | "progress"
            | "system"
            | "summary"
            | "queue_operation"
            | "file_history_snapshot"
            | "unknown"
    )
}

fn payload_type_in_domain(value: &str) -> bool {
    matches!(
        value,
        "session_meta"
            | "turn_context"
            | "message"
            | "function_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "web_search_call"
            | "reasoning"
            | "response_item"
            | "event_msg"
            | "user_message"
            | "agent_message"
            | "agent_reasoning"
            | "token_count"
            | "task_started"
            | "task_complete"
            | "turn_aborted"
            | "item_completed"
            | "search_results_received"
            | "compacted"
            | "thinking"
            | "tool_use"
            | "tool_result"
            | "text"
            | "progress"
            | "system"
            | "summary"
            | "queue-operation"
            | "file-history-snapshot"
            | "unknown"
    )
}

fn link_type_in_domain(value: &str) -> bool {
    matches!(
        value,
        "parent_event"
            | "compacted_parent"
            | "parent_uuid"
            | "tool_use_id"
            | "source_tool_assistant"
            | "unknown"
    )
}

fn canonicalize_event_kind(value: &str) -> &str {
    if event_kind_in_domain(value) {
        value
    } else {
        "unknown"
    }
}

fn canonicalize_payload_type(value: &str) -> &str {
    if payload_type_in_domain(value) {
        value
    } else {
        "unknown"
    }
}

fn canonicalize_link_type(value: &str) -> &str {
    if link_type_in_domain(value) {
        value
    } else {
        "unknown"
    }
}

pub(crate) fn event_uid(
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

pub(crate) fn raw_hash(raw_json: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(raw_json.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{:x}", digest);
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0)
}

fn io_hash(input_json: &str, output_json: &str) -> u64 {
    raw_hash(&format!("{}\n{}", input_json, output_json))
}

pub(crate) struct RecordContext<'a> {
    pub(crate) source_name: &'a str,
    pub(crate) harness: &'a str,
    pub(crate) inference_provider: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) session_date: &'a str,
    pub(crate) source_file: &'a str,
    pub(crate) source_inode: u64,
    pub(crate) source_generation: u32,
    pub(crate) source_line_no: u64,
    pub(crate) source_offset: u64,
    pub(crate) record_ts: &'a str,
    pub(crate) event_ts: &'a str,
}

pub(crate) fn base_event_obj(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    event_kind: &str,
    payload_type: &str,
    actor_kind: &str,
    text_content: &str,
    payload_json: &str,
) -> Map<String, Value> {
    let text_content = truncate_chars(text_content, TEXT_LIMIT);
    let event_kind = canonicalize_event_kind(event_kind);
    let payload_type = canonicalize_payload_type(payload_type);
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
        "harness".to_string(),
        Value::String(ctx.harness.to_string()),
    );
    obj.insert(
        "inference_provider".to_string(),
        Value::String(ctx.inference_provider.to_string()),
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
    obj.insert("endpoint_kind".to_string(), json!("generation"));
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
    obj.insert("token_usage_buckets".to_string(), token_buckets(&[]));
    obj.insert(
        "token_usage_native_units".to_string(),
        token_native_units(&[]),
    );
    obj.insert("event_version".to_string(), json!(event_version()));
    obj
}

pub(crate) fn build_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_event_uid: &str,
    linked_external_id: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    let link_type = canonicalize_link_type(link_type);
    json!({
        "event_uid": event_uid,
        "linked_event_uid": linked_event_uid,
        "linked_external_id": linked_external_id,
        "link_type": link_type,
        "session_id": ctx.session_id,
        "harness": ctx.harness,
        "inference_provider": ctx.inference_provider,
        "source_name": ctx.source_name,
        "metadata_json": metadata_json,
        "event_version": event_version(),
    })
}

pub(crate) fn build_event_link_row(
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

pub(crate) fn build_external_link_row(
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

pub(crate) fn build_tool_row(
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
        "harness": ctx.harness,
        "inference_provider": ctx.inference_provider,
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

#[cfg(test)]
mod tests {
    use super::{build_link_row, RecordContext};

    #[test]
    fn link_type_is_canonicalized_to_domain() {
        let ctx = RecordContext {
            source_name: "codex",
            harness: "codex",
            inference_provider: "openai",
            session_id: "s1",
            session_date: "2026-02-15",
            source_file: "/tmp/s1.jsonl",
            source_inode: 1,
            source_generation: 1,
            source_line_no: 1,
            source_offset: 1,
            record_ts: "2026-02-15T03:50:50.838Z",
            event_ts: "2026-02-15 03:50:50.838",
        };

        let link = build_link_row(&ctx, "e1", "e2", "", "new_link_type", "{}");
        let link_obj = link.as_object().unwrap();
        assert_eq!(
            link_obj.get("link_type").unwrap().as_str().unwrap(),
            "unknown"
        );
    }
}
