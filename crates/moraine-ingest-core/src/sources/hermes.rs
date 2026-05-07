use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext,
};
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};
use std::collections::VecDeque;

pub(crate) static HERMES: Hermes = Hermes;

pub(crate) struct Hermes;

impl IngestSource for Hermes {
    fn harness(&self) -> &'static str {
        "hermes"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        let (vendor, model) = split_hermes_vendor_model(&to_str(record.get("model")));
        SourceMetadata {
            inference_provider: vendor,
            model_hint_fallback: model,
        }
    }

    fn top_type(&self, record: &Value) -> String {
        let explicit = to_str(record.get("type"));
        if explicit.is_empty() {
            "trajectory".to_string()
        } else {
            explicit
        }
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let explicit_session_id = to_str(record.get("session_id"));
        if explicit_session_id.is_empty() {
            hermes_session_id(ctx.base_uid)
        } else {
            format!("hermes:{explicit_session_id}")
        }
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        base_uid: &str,
        _model_hint: &str,
    ) -> NormalizedPartials {
        let metadata = self.source_metadata(record);
        match top_type {
            "session_meta" => normalize_hermes_session_meta(record, ctx, base_uid),
            "session_message" => normalize_hermes_session_message(
                record,
                ctx,
                base_uid,
                &metadata.model_hint_fallback,
            ),
            _ => normalize_hermes_trajectory(record, ctx, base_uid, &metadata.model_hint_fallback)
                .into(),
        }
    }
}

enum HermesSegment {
    Text(String),
    Think(String),
    ToolCall(String),
    ToolResponse(String),
}

#[derive(Debug)]
struct HermesPendingToolCall {
    event_idx: usize,
    tool_idx: usize,
    tool_call_id: String,
    tool_name: String,
}

fn parse_hermes_segments(input: &str) -> Vec<HermesSegment> {
    const TAGS: [(&str, &str); 3] = [
        ("<think>", "</think>"),
        ("<tool_call>", "</tool_call>"),
        ("<tool_response>", "</tool_response>"),
    ];

    let mut out = Vec::new();
    let mut cursor = 0usize;

    while cursor < input.len() {
        let next = TAGS
            .iter()
            .filter_map(|(start_tag, end_tag)| {
                input[cursor..]
                    .find(start_tag)
                    .map(|relative| (cursor + relative, *start_tag, *end_tag))
            })
            .min_by_key(|(idx, _, _)| *idx);

        let Some((start_idx, start_tag, end_tag)) = next else {
            let tail = input[cursor..].trim();
            if !tail.is_empty() {
                out.push(HermesSegment::Text(tail.to_string()));
            }
            break;
        };

        let prefix = input[cursor..start_idx].trim();
        if !prefix.is_empty() {
            out.push(HermesSegment::Text(prefix.to_string()));
        }

        let body_start = start_idx + start_tag.len();
        let Some(end_relative) = input[body_start..].find(end_tag) else {
            let tail = input[start_idx..].trim();
            if !tail.is_empty() {
                out.push(HermesSegment::Text(tail.to_string()));
            }
            break;
        };

        let body_end = body_start + end_relative;
        let body = input[body_start..body_end].trim().to_string();
        match start_tag {
            "<think>" => out.push(HermesSegment::Think(body)),
            "<tool_call>" => out.push(HermesSegment::ToolCall(body)),
            "<tool_response>" => out.push(HermesSegment::ToolResponse(body)),
            _ => {}
        }
        cursor = body_end + end_tag.len();
    }

    out
}

fn hermes_session_id(base_uid: &str) -> String {
    format!("hermes:{base_uid}")
}

fn hermes_status(record: &Value) -> String {
    if to_u8_bool(record.get("partial")) != 0 {
        "partial".to_string()
    } else if record.get("completed").is_some() {
        if to_u8_bool(record.get("completed")) != 0 {
            "completed".to_string()
        } else {
            "failed".to_string()
        }
    } else {
        String::new()
    }
}

fn hermes_metadata_payload(record: &Value) -> Value {
    let mut meta = record.as_object().cloned().unwrap_or_else(Map::new);
    meta.remove("conversations");
    Value::Object(meta)
}

fn hermes_event_dt(base_dt: Option<DateTime<Utc>>, index: usize) -> DateTime<Utc> {
    let base =
        base_dt.unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch"));
    base + Duration::microseconds(index as i64)
}

fn hermes_stamp_time(row: &mut Value, dt: &DateTime<Utc>) {
    update_string_field(row, "record_ts", &format_record_ts(dt));
    update_string_field(row, "event_ts", &format_event_ts(dt));
}

fn normalize_hermes_trajectory(
    record: &Value,
    ctx: &RecordContext<'_>,
    base_uid: &str,
    model: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut emitter = HermesTrajectoryEmitter::new(ctx, model);

    emit_hermes_trajectory_session_meta(record, base_uid, &mut emitter);
    let conversations = record.get("conversations").and_then(Value::as_array);
    if let Some(conversations) = conversations {
        for (conv_idx, item) in conversations.iter().enumerate() {
            handle_hermes_conversation_item(item, conv_idx, &mut emitter);
        }
    }

    emitter.finish()
}

struct HermesTrajectoryEmitter<'ctx, 'data> {
    ctx: &'ctx RecordContext<'data>,
    events: Vec<Value>,
    links: Vec<Value>,
    tools: Vec<Value>,
    pending_tool_calls: VecDeque<HermesPendingToolCall>,
    current_turn: u32,
    base_dt: Option<DateTime<Utc>>,
    model: String,
    event_index: usize,
}

impl<'ctx, 'data> HermesTrajectoryEmitter<'ctx, 'data> {
    fn new(ctx: &'ctx RecordContext<'data>, model: &str) -> Self {
        // The caller (`normalize_record`) has already split the record's
        // `vendor/model` string into `inference_provider` (stored on ctx) and the
        // verbatim `model` name. We intentionally do NOT call `canonicalize_model`
        // here: Hermes models flow through verbatim (modulo lowercase+trim) so
        // catalog strings like `claude-sonnet-4.6` are preserved end-to-end.
        Self {
            ctx,
            events: Vec::new(),
            links: Vec::new(),
            tools: Vec::new(),
            pending_tool_calls: VecDeque::new(),
            current_turn: 0,
            base_dt: parse_record_ts(ctx.record_ts),
            model: model.to_string(),
            event_index: 0,
        }
    }

    fn uid(&self, item: &Value, conv_idx: usize, suffix: &str) -> String {
        event_uid(
            self.ctx.source_file,
            self.ctx.source_generation,
            self.ctx.source_line_no,
            self.ctx.source_offset,
            &compact_json(item),
            &format!("hermes:{conv_idx}:{suffix}"),
        )
    }

    fn event(
        &self,
        event_uid: &str,
        event_kind: &str,
        payload_type: &str,
        actor_kind: &str,
        text_content: &str,
        payload_json: &str,
    ) -> Value {
        Value::Object(base_event_obj(
            self.ctx,
            event_uid,
            event_kind,
            payload_type,
            actor_kind,
            text_content,
            payload_json,
        ))
    }

    fn push_event(&mut self, mut row: Value) -> usize {
        hermes_stamp_time(&mut row, &hermes_event_dt(self.base_dt, self.event_index));
        self.events.push(row);
        self.event_index += 1;
        self.events.len() - 1
    }

    fn push_tool_request(
        &mut self,
        event_uid: &str,
        tool_call_id: &str,
        tool_name: &str,
        input_json: &str,
    ) -> usize {
        self.tools.push(build_tool_row(
            self.ctx,
            event_uid,
            tool_call_id,
            "",
            tool_name,
            "request",
            0,
            input_json,
            "",
            "",
        ));
        self.tools.len() - 1
    }

    fn push_tool_response(
        &mut self,
        event_uid: &str,
        tool_call_id: &str,
        tool_name: &str,
        output_json: &str,
        output_text: &str,
    ) {
        self.tools.push(build_tool_row(
            self.ctx,
            event_uid,
            tool_call_id,
            "",
            tool_name,
            "response",
            0,
            "",
            output_json,
            output_text,
        ));
    }

    fn finish(self) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
        (self.events, self.links, self.tools)
    }
}

fn emit_hermes_trajectory_session_meta(
    record: &Value,
    base_uid: &str,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let status = hermes_status(record);
    let metadata_payload = hermes_metadata_payload(record);
    let mut session_meta = emitter.event(
        base_uid,
        "session_meta",
        "session_meta",
        "system",
        "",
        &compact_json(&metadata_payload),
    );
    if !emitter.model.is_empty() {
        update_string_field(&mut session_meta, "model", &emitter.model);
    }
    if !status.is_empty() {
        update_string_field(&mut session_meta, "op_status", &status);
    }
    let prompt_index = to_str(record.get("prompt_index"));
    if !prompt_index.is_empty() {
        update_string_field(&mut session_meta, "item_id", &prompt_index);
    }
    emitter.push_event(session_meta);
}

fn handle_hermes_conversation_item(
    item: &Value,
    conv_idx: usize,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let role = to_str(item.get("from"));
    let value = to_str(item.get("value"));
    let current_turn_for_item = emitter.current_turn;

    match role.as_str() {
        "system" => handle_hermes_system_item(item, conv_idx, &value, emitter),
        "human" => handle_hermes_human_item(item, conv_idx, &value, emitter),
        "gpt" => handle_hermes_gpt_item(item, conv_idx, &value, current_turn_for_item, emitter),
        "tool" => handle_hermes_tool_item(item, conv_idx, &value, current_turn_for_item, emitter),
        _ => handle_hermes_unknown_item(item, conv_idx, &value, emitter),
    }
}

fn handle_hermes_system_item(
    item: &Value,
    conv_idx: usize,
    value: &str,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let segment_uid = emitter.uid(item, conv_idx, "system");
    let mut row = emitter.event(
        &segment_uid,
        "system",
        "system",
        "system",
        value,
        &compact_json(item),
    );
    if !emitter.model.is_empty() {
        update_string_field(&mut row, "model", &emitter.model);
    }
    emitter.push_event(row);
}

fn handle_hermes_human_item(
    item: &Value,
    conv_idx: usize,
    value: &str,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    emitter.current_turn = emitter.current_turn.saturating_add(1).max(1);
    let segment_uid = emitter.uid(item, conv_idx, "human");
    let mut row = emitter.event(
        &segment_uid,
        "message",
        "message",
        "user",
        value,
        &compact_json(item),
    );
    update_string_field(&mut row, "model", &emitter.model);
    hermes_content_metadata(&mut row, &["text"], emitter.current_turn);
    emitter.push_event(row);
}

fn handle_hermes_gpt_item(
    item: &Value,
    conv_idx: usize,
    value: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    for (segment_idx, segment) in parse_hermes_segments(value).into_iter().enumerate() {
        match segment {
            HermesSegment::Text(text) => handle_hermes_gpt_text_segment(
                item,
                conv_idx,
                segment_idx,
                &text,
                current_turn_for_item,
                emitter,
            ),
            HermesSegment::Think(thinking) => handle_hermes_gpt_think_segment(
                item,
                conv_idx,
                segment_idx,
                &thinking,
                current_turn_for_item,
                emitter,
            ),
            HermesSegment::ToolCall(raw_call) => handle_hermes_gpt_tool_call_segment(
                item,
                conv_idx,
                segment_idx,
                &raw_call,
                current_turn_for_item,
                emitter,
            ),
            HermesSegment::ToolResponse(raw_response) => {
                handle_hermes_gpt_tool_response_text_segment(
                    item,
                    conv_idx,
                    segment_idx,
                    &raw_response,
                    current_turn_for_item,
                    emitter,
                )
            }
        }
    }
}

fn handle_hermes_tool_item(
    item: &Value,
    conv_idx: usize,
    value: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    for (segment_idx, segment) in parse_hermes_segments(value).into_iter().enumerate() {
        match segment {
            HermesSegment::ToolResponse(raw_response) => handle_hermes_tool_response_segment(
                item,
                conv_idx,
                segment_idx,
                &raw_response,
                current_turn_for_item,
                emitter,
            ),
            HermesSegment::Text(text) => handle_hermes_tool_text_segment(
                item,
                conv_idx,
                segment_idx,
                &text,
                current_turn_for_item,
                emitter,
            ),
            HermesSegment::Think(_) | HermesSegment::ToolCall(_) => {}
        }
    }
}

fn handle_hermes_gpt_text_segment(
    item: &Value,
    conv_idx: usize,
    segment_idx: usize,
    text: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    if text.is_empty() {
        return;
    }

    let segment_uid = emitter.uid(item, conv_idx, &format!("assistant:text:{segment_idx}"));
    let payload = json!({
        "from": "gpt",
        "type": "text",
        "value": text,
    });
    let mut row = emitter.event(
        &segment_uid,
        "message",
        "message",
        "assistant",
        text,
        &compact_json(&payload),
    );
    hermes_content_metadata(&mut row, &["text"], current_turn_for_item);
    update_string_field(&mut row, "model", &emitter.model);
    emitter.push_event(row);
}

fn handle_hermes_gpt_think_segment(
    item: &Value,
    conv_idx: usize,
    segment_idx: usize,
    thinking: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let segment_uid = emitter.uid(item, conv_idx, &format!("assistant:think:{segment_idx}"));
    let payload = json!({
        "from": "gpt",
        "type": "thinking",
        "value": thinking,
    });
    let mut row = emitter.event(
        &segment_uid,
        "reasoning",
        "reasoning",
        "assistant",
        thinking,
        &compact_json(&payload),
    );
    if let Some(obj) = row.as_object_mut() {
        mark_reasoning_metadata(obj);
        obj.insert("turn_index".to_string(), json!(current_turn_for_item));
    }
    update_string_field(&mut row, "model", &emitter.model);
    emitter.push_event(row);
}

fn handle_hermes_gpt_tool_call_segment(
    item: &Value,
    conv_idx: usize,
    segment_idx: usize,
    raw_call: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let segment_uid = emitter.uid(
        item,
        conv_idx,
        &format!("assistant:tool_call:{segment_idx}"),
    );
    let parsed_call = parse_json_string(raw_call).unwrap_or_else(|| json!({ "raw": raw_call }));
    let tool_name = to_str(parsed_call.get("name"));
    let arguments = parsed_call.get("arguments").cloned().unwrap_or(Value::Null);
    let input_json = compact_json(&arguments);
    let input_text = {
        let extracted = extract_message_text(&arguments);
        if extracted.is_empty() {
            input_json.clone()
        } else {
            extracted
        }
    };
    let mut tool_call_id = to_str(parsed_call.get("tool_call_id"));
    if tool_call_id.is_empty() {
        tool_call_id = format!("hermes-call-{conv_idx}-{segment_idx}");
    }

    let mut row = emitter.event(
        &segment_uid,
        "tool_call",
        "tool_use",
        "assistant",
        &input_text,
        &compact_json(&parsed_call),
    );
    hermes_content_metadata(&mut row, &["tool_use"], current_turn_for_item);
    update_string_field(&mut row, "tool_call_id", &tool_call_id);
    update_string_field(&mut row, "tool_name", &tool_name);
    update_string_field(&mut row, "model", &emitter.model);
    let event_idx = emitter.push_event(row);

    let tool_idx = emitter.push_tool_request(&segment_uid, &tool_call_id, &tool_name, &input_json);
    emitter.pending_tool_calls.push_back(HermesPendingToolCall {
        event_idx,
        tool_idx,
        tool_call_id,
        tool_name,
    });
}

fn handle_hermes_gpt_tool_response_text_segment(
    item: &Value,
    conv_idx: usize,
    segment_idx: usize,
    raw_response: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let text = raw_response.trim();
    if text.is_empty() {
        return;
    }

    let segment_uid = emitter.uid(
        item,
        conv_idx,
        &format!("assistant:tool_response:{segment_idx}"),
    );
    let payload = json!({
        "from": "gpt",
        "type": "tool_response_text",
        "value": text,
    });
    let mut row = emitter.event(
        &segment_uid,
        "tool_result",
        "tool_result",
        "tool",
        text,
        &compact_json(&payload),
    );
    hermes_content_metadata(&mut row, &["tool_result"], current_turn_for_item);
    update_string_field(&mut row, "model", &emitter.model);
    update_u8_field(&mut row, "tool_error", 1);
    emitter.push_event(row);
}

fn handle_hermes_tool_response_segment(
    item: &Value,
    conv_idx: usize,
    segment_idx: usize,
    raw_response: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let parsed_response =
        parse_json_string(raw_response).unwrap_or_else(|| json!({ "content": raw_response }));
    let (tool_call_id, tool_name) =
        resolve_hermes_tool_response(emitter, &parsed_response, conv_idx, segment_idx);
    let content = parsed_response
        .get("content")
        .cloned()
        .unwrap_or(Value::Null);
    let output_json = compact_json(&content);
    let output_text = {
        let extracted = extract_message_text(&content);
        if extracted.is_empty() {
            output_json.clone()
        } else {
            extracted
        }
    };
    let segment_uid = emitter.uid(item, conv_idx, &format!("tool:response:{segment_idx}"));
    let mut row = emitter.event(
        &segment_uid,
        "tool_result",
        "tool_result",
        "tool",
        &output_text,
        &compact_json(&parsed_response),
    );
    hermes_content_metadata(&mut row, &["tool_result"], current_turn_for_item);
    update_string_field(&mut row, "tool_call_id", &tool_call_id);
    update_string_field(&mut row, "tool_name", &tool_name);
    update_string_field(&mut row, "model", &emitter.model);
    emitter.push_event(row);
    emitter.push_tool_response(
        &segment_uid,
        &tool_call_id,
        &tool_name,
        &output_json,
        &output_text,
    );
}

fn handle_hermes_tool_text_segment(
    item: &Value,
    conv_idx: usize,
    segment_idx: usize,
    text: &str,
    current_turn_for_item: u32,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    if text.is_empty() {
        return;
    }

    let segment_uid = emitter.uid(item, conv_idx, &format!("tool:text:{segment_idx}"));
    let payload = json!({
        "from": "tool",
        "type": "tool_text",
        "value": text,
    });
    let mut row = emitter.event(
        &segment_uid,
        "tool_result",
        "tool_result",
        "tool",
        text,
        &compact_json(&payload),
    );
    hermes_content_metadata(&mut row, &["tool_result"], current_turn_for_item);
    update_string_field(&mut row, "model", &emitter.model);
    update_u8_field(&mut row, "tool_error", 1);
    emitter.push_event(row);
}

fn handle_hermes_unknown_item(
    item: &Value,
    conv_idx: usize,
    value: &str,
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
) {
    let segment_uid = emitter.uid(item, conv_idx, "unknown");
    let mut row = emitter.event(
        &segment_uid,
        "unknown",
        "unknown",
        "system",
        value,
        &compact_json(item),
    );
    update_string_field(&mut row, "model", &emitter.model);
    emitter.push_event(row);
}

fn resolve_hermes_tool_response(
    emitter: &mut HermesTrajectoryEmitter<'_, '_>,
    parsed_response: &Value,
    conv_idx: usize,
    segment_idx: usize,
) -> (String, String) {
    let pending = emitter.pending_tool_calls.pop_front();
    let pending_tool_name = pending
        .as_ref()
        .map(|call| call.tool_name.clone())
        .unwrap_or_default();
    let response_call_id = to_str(parsed_response.get("tool_call_id"));
    let mut tool_call_id = response_call_id.clone();
    if tool_call_id.is_empty() {
        if let Some(call) = pending.as_ref() {
            tool_call_id = call.tool_call_id.clone();
        } else {
            tool_call_id = format!("hermes-result-{conv_idx}-{segment_idx}");
        }
    }

    if let Some(call) = pending {
        if response_call_id != call.tool_call_id && !response_call_id.is_empty() {
            update_string_field(
                &mut emitter.events[call.event_idx],
                "tool_call_id",
                &response_call_id,
            );
            update_string_field(
                &mut emitter.tools[call.tool_idx],
                "tool_call_id",
                &response_call_id,
            );
            tool_call_id = response_call_id;
        }
    }

    let tool_name = {
        let name = to_str(parsed_response.get("name"));
        if !name.is_empty() {
            name
        } else {
            pending_tool_name
        }
    };

    (tool_call_id, tool_name)
}

fn hermes_content_metadata(row: &mut Value, content_types: &[&str], turn_index: u32) {
    if let Some(obj) = row.as_object_mut() {
        obj.insert("content_types".to_string(), json!(content_types));
        obj.insert("turn_index".to_string(), json!(turn_index));
    }
}

/// Normalize a synthetic `session_meta` record emitted by the session_json
/// processor for Hermes live sessions. The record carries the session header
/// (session_id, model, base_url, platform, session_start, last_updated,
/// system_prompt, tools, message_count). We emit a single `session_meta` event.
fn normalize_hermes_session_meta(
    record: &Value,
    ctx: &RecordContext<'_>,
    base_uid: &str,
) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let base_dt = parse_record_ts(ctx.record_ts);
    let platform = to_str(record.get("platform"));
    let base_url = to_str(record.get("base_url"));
    let model_raw = to_str(record.get("model"));
    let (_vendor, model) = split_hermes_vendor_model(&model_raw);
    let _ = base_uid;

    let payload = build_hermes_session_meta_payload(record, &model, &base_url, &platform);
    let payload_json = compact_json(&payload);
    let uid = emitter.uid(&payload_json, "session_meta");
    let mut event = hermes_session_event_defaults(
        emitter.event(
            &uid,
            "session_meta",
            "session_meta",
            "system",
            "",
            &payload_json,
        ),
        &model,
    );
    event = stamp_hermes_session_event_time(event, base_dt, 0);
    if !platform.is_empty() {
        event = event.agent_label(platform);
    }
    emitter.push_event(event);

    emitter.finish()
}

fn build_hermes_session_meta_payload(
    record: &Value,
    model: &str,
    base_url: &str,
    platform: &str,
) -> Value {
    let mut meta_payload = Map::<String, Value>::new();
    meta_payload.insert(
        "session_id".to_string(),
        Value::String(to_str(record.get("session_id"))),
    );
    meta_payload.insert("model".to_string(), Value::String(model.to_string()));
    meta_payload.insert("base_url".to_string(), Value::String(base_url.to_string()));
    meta_payload.insert("platform".to_string(), Value::String(platform.to_string()));
    meta_payload.insert(
        "session_start".to_string(),
        Value::String(to_str(record.get("session_start"))),
    );
    meta_payload.insert(
        "last_updated".to_string(),
        Value::String(to_str(record.get("last_updated"))),
    );
    if let Some(system_prompt) = record.get("system_prompt") {
        meta_payload.insert("system_prompt".to_string(), system_prompt.clone());
    }
    if let Some(tools_value) = record.get("tools") {
        meta_payload.insert("tools".to_string(), tools_value.clone());
    }
    if let Some(message_count) = record.get("message_count") {
        meta_payload.insert("message_count".to_string(), message_count.clone());
    }

    Value::Object(meta_payload)
}

struct HermesSessionMessageContext<'a> {
    message: &'a Value,
    message_index: u64,
    turn_index: u32,
    role: String,
    content_value: Value,
    compact_message: String,
    model: &'a str,
    base_dt: Option<DateTime<Utc>>,
}

impl HermesSessionMessageContext<'_> {
    fn uid(&self, emitter: &SourceEmitter<'_>, suffix: &str) -> String {
        emitter.uid(
            &self.compact_message,
            &format!("hermes_session:{}:{suffix}", self.message_index),
        )
    }

    fn payload_json(&self) -> &str {
        &self.compact_message
    }
}

/// Normalize a synthetic `session_message` record: one OpenAI chat-completions
/// message from a live Hermes session (role ∈ {user, assistant, tool, system},
/// plus optional tool_calls / reasoning / tool_call_id). Tool call / result
/// correlation travels through `tool_call_id` on each emitted row — the
/// OpenAI schema carries it on both sides, so no in-record tracking is needed.
fn normalize_hermes_session_message(
    record: &Value,
    ctx: &RecordContext<'_>,
    base_uid: &str,
    model: &str,
) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let base_dt = parse_record_ts(ctx.record_ts);
    let _ = base_uid;

    let message = match record.get("message") {
        Some(Value::Object(_)) => record.get("message").unwrap(),
        _ => return emitter.finish(),
    };

    let message_index = record
        .get("message_index")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    // For turn_index we use a 1-based message index: plenty for ordering, and
    // ClickHouse schema uses UInt32.
    let turn_index: u32 = ((message_index + 1).min(u32::MAX as u64)) as u32;
    let message_ctx = HermesSessionMessageContext {
        message,
        message_index,
        turn_index,
        role: to_str(message.get("role")),
        content_value: message.get("content").cloned().unwrap_or(Value::Null),
        compact_message: compact_json(message),
        model,
        base_dt,
    };

    normalize_hermes_session_message_role(&message_ctx, &mut emitter);

    emitter.finish()
}

fn normalize_hermes_session_message_role(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
) {
    let mut sub_event_index = 0usize;
    match message_ctx.role.as_str() {
        "user" => emit_hermes_session_user_message(message_ctx, emitter, &mut sub_event_index),
        "assistant" => {
            // Emit reasoning first if present — matches the wall-clock order of
            // thinking → text → tool_calls in a single assistant turn.
            emit_hermes_session_assistant_reasoning(message_ctx, emitter, &mut sub_event_index);
            emit_hermes_session_assistant_text(message_ctx, emitter, &mut sub_event_index);
            if let Some(tool_calls) = message_ctx
                .message
                .get("tool_calls")
                .and_then(Value::as_array)
            {
                for (call_idx, call) in tool_calls.iter().enumerate() {
                    emit_hermes_session_tool_call(
                        message_ctx,
                        emitter,
                        &mut sub_event_index,
                        call_idx,
                        call,
                    );
                }
            }
        }
        "tool" => emit_hermes_session_tool_result(message_ctx, emitter, &mut sub_event_index),
        "system" => emit_hermes_session_system_message(message_ctx, emitter, &mut sub_event_index),
        _ => emit_hermes_session_unknown_message(message_ctx, emitter, &mut sub_event_index),
    }
}

fn emit_hermes_session_user_message(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
) {
    let text = extract_message_text(&message_ctx.content_value);
    let uid = message_ctx.uid(emitter, "user");
    let event = hermes_session_event_defaults(
        emitter.event(
            &uid,
            "message",
            "user_message",
            "user",
            &text,
            message_ctx.payload_json(),
        ),
        message_ctx.model,
    )
    .content_types(extract_content_types(&message_ctx.content_value))
    .turn_index(message_ctx.turn_index);
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
}

fn emit_hermes_session_assistant_reasoning(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
) {
    let reasoning = message_ctx
        .message
        .get("reasoning")
        .cloned()
        .unwrap_or(Value::Null);
    let reasoning_text = match &reasoning {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if reasoning_text.trim().is_empty() {
        return;
    }

    let uid = message_ctx.uid(emitter, "reasoning");
    let payload = json!({
        "role": "assistant",
        "reasoning": reasoning,
    });
    let event = hermes_session_event_defaults(
        emitter.event_for_json(
            &uid,
            "reasoning",
            "reasoning",
            "assistant",
            &reasoning_text,
            &payload,
        ),
        message_ctx.model,
    )
    .has_reasoning(true)
    .content_types(["reasoning"])
    .turn_index(message_ctx.turn_index);
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
}

fn emit_hermes_session_assistant_text(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
) {
    let text = extract_message_text(&message_ctx.content_value);
    if text.trim().is_empty() {
        return;
    }

    let uid = message_ctx.uid(emitter, "assistant");
    let payload = json!({
        "role": "assistant",
        "content": message_ctx.content_value,
    });
    let mut event = hermes_session_event_defaults(
        emitter.event_for_json(
            &uid,
            "message",
            "agent_message",
            "assistant",
            &text,
            &payload,
        ),
        message_ctx.model,
    )
    .content_types(extract_content_types(&message_ctx.content_value))
    .turn_index(message_ctx.turn_index);
    let finish_reason = to_str(message_ctx.message.get("finish_reason"));
    if !finish_reason.is_empty() {
        event = event.op_status(finish_reason);
    }
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
}

fn emit_hermes_session_tool_call(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
    call_idx: usize,
    call: &Value,
) {
    let tool_call_id = to_str(call.get("id"));
    let function = call.get("function").cloned().unwrap_or(Value::Null);
    let tool_name = to_str(function.get("name"));
    let arguments_raw = to_str(function.get("arguments"));
    let arguments = parse_json_string(&arguments_raw).unwrap_or_else(|| {
        if arguments_raw.is_empty() {
            Value::Object(Map::new())
        } else {
            json!({ "raw": arguments_raw })
        }
    });
    let input_json = compact_json(&arguments);
    let input_text = {
        let extracted = extract_message_text(&arguments);
        if extracted.is_empty() {
            input_json.clone()
        } else {
            extracted
        }
    };

    let uid = message_ctx.uid(emitter, &format!("tool_call:{call_idx}"));
    let event = hermes_session_event_defaults(
        emitter.event_for_json(
            &uid,
            "tool_call",
            "tool_use",
            "assistant",
            &input_text,
            call,
        ),
        message_ctx.model,
    )
    .tool_call_id(tool_call_id.clone())
    .tool_name(tool_name.clone())
    .content_types(["tool_use"])
    .turn_index(message_ctx.turn_index);
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
    emitter.push_tool_request(&uid, &tool_call_id, "", &tool_name, &input_json);
}

fn emit_hermes_session_tool_result(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
) {
    let tool_call_id = to_str(message_ctx.message.get("tool_call_id"));
    let tool_name = to_str(message_ctx.message.get("name"));
    let text = extract_message_text(&message_ctx.content_value);
    let output_json = compact_json(&message_ctx.content_value);

    let uid = message_ctx.uid(emitter, "tool_result");
    let event = hermes_session_event_defaults(
        emitter.event(
            &uid,
            "tool_result",
            "tool_result",
            "tool",
            &text,
            message_ctx.payload_json(),
        ),
        message_ctx.model,
    )
    .tool_call_id(tool_call_id.clone())
    .tool_name(tool_name.clone())
    .content_types(["tool_result"])
    .turn_index(message_ctx.turn_index);
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
    emitter.push_tool_response(
        &uid,
        &tool_call_id,
        "",
        &tool_name,
        0,
        "",
        &output_json,
        &text,
    );
}

fn emit_hermes_session_system_message(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
) {
    let text = extract_message_text(&message_ctx.content_value);
    let uid = message_ctx.uid(emitter, "system");
    let event = hermes_session_event_defaults(
        emitter.event(
            &uid,
            "system",
            "system",
            "system",
            &text,
            message_ctx.payload_json(),
        ),
        message_ctx.model,
    )
    .turn_index(message_ctx.turn_index);
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
}

fn emit_hermes_session_unknown_message(
    message_ctx: &HermesSessionMessageContext<'_>,
    emitter: &mut SourceEmitter<'_>,
    sub_event_index: &mut usize,
) {
    let text = extract_message_text(&message_ctx.content_value);
    let uid = message_ctx.uid(emitter, "unknown");
    let event = hermes_session_event_defaults(
        emitter.event(
            &uid,
            "unknown",
            "unknown",
            "system",
            &text,
            message_ctx.payload_json(),
        ),
        message_ctx.model,
    );
    push_hermes_session_event(emitter, event, message_ctx.base_dt, sub_event_index);
}

fn hermes_session_event_defaults(event: EventBuilder, model: &str) -> EventBuilder {
    let event = event.token_accounting(TokenAccounting::new(TokenEndpointKind::Generation));
    if model.is_empty() {
        event
    } else {
        event.model(model)
    }
}

fn stamp_hermes_session_event_time(
    event: EventBuilder,
    base_dt: Option<DateTime<Utc>>,
    sub_event_index: usize,
) -> EventBuilder {
    let dt = hermes_event_dt(base_dt, sub_event_index);
    event
        .record_ts(format_record_ts(&dt))
        .event_ts(format_event_ts(&dt))
}

fn push_hermes_session_event(
    emitter: &mut SourceEmitter<'_>,
    event: EventBuilder,
    base_dt: Option<DateTime<Utc>>,
    sub_event_index: &mut usize,
) {
    let event = stamp_hermes_session_event_time(event, base_dt, *sub_event_index);
    emitter.push_event(event);
    *sub_event_index += 1;
}
