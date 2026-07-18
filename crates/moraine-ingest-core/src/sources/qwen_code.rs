use super::shared::*;
use super::{
    emitter::{EventBuilder, SourceEmitter},
    IngestSource, NormalizedPartials, SourceFormat, SourceMetadata, SourceRecordContext,
};
use serde_json::{json, Map, Value};

pub(crate) static QWEN_CODE: QwenCode = QwenCode;

const MAX_ASSISTANT_PARTS: usize = 1024;

pub(crate) struct QwenCode;

impl IngestSource for QwenCode {
    fn harness(&self) -> &'static str {
        "qwen-code"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn format(&self) -> SourceFormat {
        SourceFormat::Jsonl
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        SourceMetadata {
            inference_provider: String::new(),
            model_hint_fallback: to_str(record.get("model")),
        }
    }

    fn jsonl_carries_cwd(&self) -> bool {
        true
    }

    fn cwd(&self, record: &Value) -> String {
        to_str(record.get("cwd"))
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let session_id = to_str(record.get("sessionId"));
        if !session_id.trim().is_empty() {
            return session_id;
        }
        if ctx.session_hint.is_empty() {
            infer_session_id_from_file(ctx.source_file)
        } else {
            ctx.session_hint.to_string()
        }
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        _base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        let record = QwenRecord::new(record, top_type, model_hint);
        let mut emitter = SourceEmitter::new(ctx);
        dispatch_qwen_record(&record, &mut emitter);
        emitter.finish()
    }
}

struct QwenRecord<'a> {
    record: &'a Value,
    top_type: &'a str,
    model_hint: &'a str,
}

impl<'a> QwenRecord<'a> {
    fn new(record: &'a Value, top_type: &'a str, model_hint: &'a str) -> Self {
        Self {
            record,
            top_type,
            model_hint,
        }
    }

    fn uuid(&self) -> String {
        to_str(self.record.get("uuid"))
    }

    fn parent_uuid(&self) -> String {
        to_str(self.record.get("parentUuid"))
    }

    fn subtype(&self) -> String {
        to_str(self.record.get("subtype"))
    }

    fn message(&self) -> &Value {
        self.record.get("message").unwrap_or_else(null_value)
    }

    fn parts(&self) -> &[Value] {
        self.message()
            .get("parts")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    fn model(&self) -> String {
        let recorded = to_str(self.record.get("model"));
        canonicalize_model(
            "qwen-code",
            if recorded.is_empty() {
                self.model_hint
            } else {
                &recorded
            },
        )
    }

    fn usage(&self) -> Option<&Value> {
        self.record
            .get("usageMetadata")
            .filter(|usage| !usage.is_null())
    }

    fn payload(&self, body_key: &str, body: Value) -> Value {
        let mut payload = Map::new();
        payload.insert(
            "uuid".to_string(),
            self.record.get("uuid").cloned().unwrap_or(Value::Null),
        );
        payload.insert(
            "parentUuid".to_string(),
            self.record
                .get("parentUuid")
                .cloned()
                .unwrap_or(Value::Null),
        );
        if let Some(subtype) = self.record.get("subtype") {
            payload.insert("subtype".to_string(), subtype.clone());
        }
        payload.insert(body_key.to_string(), body);
        Value::Object(payload)
    }

    fn stamp(&self, event: EventBuilder) -> EventBuilder {
        let model = self.model();
        let event = event.item_id(self.uuid());
        if model.is_empty() {
            event
        } else {
            event.model(model)
        }
    }
}

fn dispatch_qwen_record(record: &QwenRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    match record.top_type {
        "user" => handle_qwen_user(record, emitter),
        "assistant" => handle_qwen_assistant(record, emitter),
        "tool_result" => handle_qwen_tool_result(record, emitter),
        "system" => handle_qwen_system(record, emitter),
        _ => {}
    }
}

fn handle_qwen_user(record: &QwenRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let text_parts = Value::Array(
        record
            .parts()
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                    .map(|text| json!({ "text": text }))
            })
            .collect(),
    );
    let Some(parts) = text_parts.as_array().filter(|parts| !parts.is_empty()) else {
        return;
    };

    let text = parts
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    let subtype = record.subtype();
    let notification = matches!(subtype.as_str(), "notification" | "cron");
    let uid = emitter.uid_for_json(
        &text_parts,
        if notification {
            "qwen:user:system-text"
        } else {
            "qwen:user:text"
        },
    );
    let payload = record.payload("parts", text_parts);
    let event = record
        .stamp(emitter.event_for_json(
            &uid,
            if notification { "system" } else { "message" },
            if notification {
                "system"
            } else {
                "user_message"
            },
            if notification { "system" } else { "user" },
            &text,
            &payload,
        ))
        .content_types(["text"])
        .op_kind(subtype);
    push_qwen_event(record, &uid, event, emitter);
}

fn handle_qwen_assistant(record: &QwenRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let parts = record.parts();
    if parts.len() > MAX_ASSISTANT_PARTS {
        return;
    }

    let mut accounting = record
        .usage()
        .map(|usage| TokenAccounting::google_generation(Some(usage)));

    for (index, part) in parts.iter().enumerate() {
        let (part_kind, event_kind, payload_type, text, content_type, has_reasoning, body) =
            if let Some(function_call) = part.get("functionCall") {
                let args = function_call.get("args").unwrap_or_else(null_value);
                let text = extract_message_text(args);
                let text = if text.is_empty() {
                    compact_json(args)
                } else {
                    text
                };
                (
                    "function-call",
                    "tool_call",
                    "tool_use",
                    text,
                    "tool_use",
                    false,
                    json!({
                        "functionCall": {
                            "id": function_call.get("id").cloned().unwrap_or(Value::Null),
                            "name": function_call.get("name").cloned().unwrap_or(Value::Null),
                            "args": args,
                        }
                    }),
                )
            } else if part.get("thought").and_then(Value::as_bool) == Some(true) {
                let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                else {
                    continue;
                };
                (
                    "thought",
                    "reasoning",
                    "thinking",
                    text.to_string(),
                    "reasoning",
                    true,
                    json!({ "text": text, "thought": true }),
                )
            } else {
                let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                else {
                    continue;
                };
                (
                    "text",
                    "message",
                    "agent_message",
                    text.to_string(),
                    "text",
                    false,
                    json!({ "text": text }),
                )
            };

        let hashed_uid = emitter.uid_for_json(part, &format!("qwen:assistant:{part_kind}:{index}"));
        let uid = format!("{index:08x}{}", &hashed_uid[8..]);
        let payload = record.payload("part", body);
        let mut event = record
            .stamp(emitter.event_for_json(
                &uid,
                event_kind,
                payload_type,
                "assistant",
                &text,
                &payload,
            ))
            .content_types([content_type])
            .has_reasoning(has_reasoning);
        if let Some(token_accounting) = accounting.take() {
            event = event.token_accounting(token_accounting);
        }

        if let Some(function_call) = part.get("functionCall") {
            let tool_call_id = to_str(function_call.get("id"));
            let tool_name = to_str(function_call.get("name"));
            let input = function_call.get("args").unwrap_or_else(null_value);
            let input_json = compact_json(input);
            event = event
                .tool_call_id(tool_call_id.clone())
                .tool_name(tool_name.clone())
                .tool_phase("request");
            push_qwen_event(record, &uid, event, emitter);
            emitter.push_tool_request(&uid, &tool_call_id, "", &tool_name, &input_json);
        } else {
            push_qwen_event(record, &uid, event, emitter);
        }
    }
}

fn handle_qwen_tool_result(record: &QwenRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let Some(function_response) = record
        .parts()
        .iter()
        .find_map(|part| part.get("functionResponse"))
    else {
        return;
    };
    let metadata = record
        .record
        .get("toolCallResult")
        .unwrap_or_else(null_value);
    let mut tool_call_id = to_str(metadata.get("callId"));
    if tool_call_id.trim().is_empty() {
        tool_call_id = to_str(function_response.get("id"));
    }
    if tool_call_id.trim().is_empty() {
        tool_call_id = record.uuid();
    }
    let mut tool_name = to_str(function_response.get("name"));
    if tool_name.trim().is_empty() {
        tool_name = to_str(metadata.get("name"));
    }
    if tool_name.trim().is_empty() {
        tool_name = to_str(metadata.get("displayName"));
    }
    let response = function_response.get("response").unwrap_or_else(null_value);
    let output_json = compact_json(response);
    let output_text = {
        let extracted = extract_message_text(response);
        if extracted.is_empty() && !response.is_null() {
            output_json.clone()
        } else {
            extracted
        }
    };
    let status = to_str(metadata.get("status"));
    let tool_error = qwen_tool_error(metadata, response, &status);
    let duration_ms = to_u32(
        metadata
            .get("durationMs")
            .or_else(|| metadata.get("duration_ms")),
    );
    let uid = emitter.uid_for_json(function_response, "qwen:tool-result");
    let payload = record.payload(
        "toolResult",
        json!({
            "functionResponse": function_response,
            "metadata": bounded_tool_metadata(metadata),
        }),
    );
    let event = record
        .stamp(emitter.event_for_json(
            &uid,
            "tool_result",
            "tool_result",
            "tool",
            &output_text,
            &payload,
        ))
        .content_types(["tool_result"])
        .tool_call_id(tool_call_id.clone())
        .tool_name(tool_name.clone())
        .tool_phase("response")
        .tool_error(tool_error)
        .op_status(status)
        .latency_ms(duration_ms);
    push_qwen_event(record, &uid, event, emitter);
    emitter.push_tool_response(
        &uid,
        &tool_call_id,
        "",
        &tool_name,
        tool_error,
        "",
        &output_json,
        &output_text,
    );
}

fn handle_qwen_system(record: &QwenRecord<'_>, emitter: &mut SourceEmitter<'_>) {
    let subtype = record.subtype();
    let system_payload = record
        .record
        .get("systemPayload")
        .unwrap_or_else(null_value);
    match subtype.as_str() {
        "custom_title" => {
            let title = to_str(system_payload.get("customTitle"));
            if title.is_empty() {
                return;
            }
            let uid = emitter.uid_for_json(system_payload, "qwen:system:custom-title");
            let mut payload = record.payload(
                "systemPayload",
                json!({
                    "titleSource": system_payload.get("titleSource").cloned().unwrap_or(Value::Null),
                }),
            );
            payload
                .as_object_mut()
                .expect("Qwen event payload is an object")
                .insert("title".to_string(), Value::String(title.clone()));
            let event = record
                .stamp(emitter.event_for_json(
                    &uid,
                    "session_meta",
                    "session_meta",
                    "system",
                    &title,
                    &payload,
                ))
                .op_kind(subtype);
            push_qwen_event(record, &uid, event, emitter);
        }
        "chat_compression" => {
            let info = bounded_compression_info(system_payload.get("info"));
            let uid = emitter.uid_for_json(&info, "qwen:system:chat-compression");
            let payload = record.payload("systemPayload", json!({ "info": info }));
            let event = record
                .stamp(emitter.event_for_json(&uid, "summary", "compacted", "system", "", &payload))
                .op_kind(subtype);
            push_qwen_event(record, &uid, event, emitter);
        }
        "rewind" => {
            let truncated_count = to_u64(system_payload.get("truncatedCount"));
            let uid = emitter.uid_for_json(system_payload, "qwen:system:rewind");
            let payload = record.payload(
                "systemPayload",
                json!({
                    "subtype": "rewind",
                    "truncatedCount": truncated_count,
                }),
            );
            let event = record
                .stamp(emitter.event_for_json(&uid, "system", "system", "system", "", &payload))
                .op_kind(subtype);
            push_qwen_event(record, &uid, event, emitter);
        }
        _ => {}
    }
}

fn push_qwen_event(
    record: &QwenRecord<'_>,
    event_uid: &str,
    event: EventBuilder,
    emitter: &mut SourceEmitter<'_>,
) {
    emitter.push_event(event);
    let parent_uuid = record.parent_uuid();
    if !parent_uuid.is_empty() {
        emitter.push_external_link(event_uid, &parent_uuid, "parent_event", "{}");
    }
}

fn bounded_compression_info(info: Option<&Value>) -> Value {
    let Some(info) = info else {
        return json!({});
    };
    json!({
        "originalTokenCount": to_u64(info.get("originalTokenCount")),
        "newTokenCount": to_u64(info.get("newTokenCount")),
        "compressionStatus": to_str(info.get("compressionStatus")),
        "triggerReason": to_str(info.get("triggerReason")),
    })
}

fn bounded_tool_metadata(metadata: &Value) -> Value {
    json!({
        "callId": to_str(metadata.get("callId")),
        "status": to_str(metadata.get("status")),
        "error": metadata.get("error").cloned().unwrap_or(Value::Null),
        "errorType": metadata.get("errorType").cloned().unwrap_or(Value::Null),
        "durationMs": to_u32(metadata.get("durationMs").or_else(|| metadata.get("duration_ms"))),
    })
}

fn qwen_tool_error(metadata: &Value, response: &Value, status: &str) -> u8 {
    let failed_status = matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "error" | "failed" | "cancelled" | "canceled"
    );
    let metadata_error = metadata
        .get("error")
        .is_some_and(|error| !error.is_null() && error != &Value::Bool(false));
    let response_error = response.get("error").is_some_and(|error| !error.is_null());
    u8::from(failed_status || metadata_error || response_error)
}

fn null_value<'a>() -> &'a Value {
    static NULL_VALUE: Value = Value::Null;
    &NULL_VALUE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::normalize_record;

    fn normalize(record: Value) -> crate::model::NormalizedRecord {
        normalize_record(
            &record,
            "qwen-code",
            "qwen-code",
            "/fixtures/qwen-code/session.jsonl",
            1,
            1,
            1,
            0,
            "",
            "",
            "",
        )
        .expect("Qwen record normalizes")
    }

    #[test]
    fn assistant_parts_keep_order_links_and_single_usage_application() {
        let row = normalize(json!({
            "uuid": "assistant-1",
            "parentUuid": "user-1",
            "sessionId": "session-1",
            "timestamp": "2026-07-18T12:00:01Z",
            "type": "assistant",
            "cwd": "/repo",
            "model": "Qwen3-Coder",
            "message": {"role": "model", "parts": [
                {"text": "thinking", "thought": true},
                {"text": "answer"},
                {"functionCall": {"id": "call-1", "name": "mcp__moraine__search_sessions", "args": {"query": "needle"}, "futureSecret": "must-remain-raw-only"}},
                {"inlineData": {"mimeType": "image/png", "data": "not-indexed"}}
            ]},
            "usageMetadata": {
                "promptTokenCount": 20,
                "candidatesTokenCount": 10,
                "cachedContentTokenCount": 3,
                "thoughtsTokenCount": 2,
                "totalTokenCount": 30
            }
        }));

        assert_eq!(row.event_rows.len(), 3);
        assert_eq!(row.event_rows[0]["event_kind"], "reasoning");
        assert_eq!(row.event_rows[1]["text_content"], "answer");
        assert_eq!(row.event_rows[2]["event_kind"], "tool_call");
        assert_eq!(
            row.event_rows[2]["tool_name"],
            "mcp__moraine__search_sessions"
        );
        assert_eq!(row.link_rows.len(), 3);
        assert_eq!(row.tool_rows.len(), 1);
        assert_eq!(row.event_rows[0]["input_tokens"], 20);
        assert_eq!(row.event_rows[1]["input_tokens"], 0);
        assert_eq!(row.event_rows[2]["input_tokens"], 0);
        assert_eq!(row.event_rows[0]["model"], "qwen3-coder");
        assert_eq!(row.event_rows[0]["inference_provider"], "");
        let event_uids = row
            .event_rows
            .iter()
            .map(|event| event["event_uid"].as_str().expect("event uid"))
            .collect::<Vec<_>>();
        assert!(
            event_uids.windows(2).all(|pair| pair[0] < pair[1]),
            "tied source coordinates must sort assistant parts by their emitted UID"
        );
        assert!(!row.event_rows[2]["payload_json"]
            .as_str()
            .expect("tool payload")
            .contains("futureSecret"));
    }

    #[test]
    fn tool_result_uses_metadata_id_once_and_preserves_failure() {
        let row = normalize(json!({
            "uuid": "result-1",
            "parentUuid": "assistant-1",
            "sessionId": "session-1",
            "timestamp": "2026-07-18T12:00:02Z",
            "type": "tool_result",
            "cwd": "/repo",
            "message": {"role": "user", "parts": [{"functionResponse": {
                "id": "response-id",
                "name": "shell",
                "response": {"error": "boom"}
            }}]},
            "toolCallResult": {
                "callId": "call-id",
                "status": "failed",
                "durationMs": 42,
                "resultDisplay": {"veryLarge": "raw-only"}
            }
        }));

        assert_eq!(row.event_rows.len(), 1);
        assert_eq!(row.tool_rows.len(), 1);
        assert_eq!(row.event_rows[0]["tool_call_id"], "call-id");
        assert_eq!(row.event_rows[0]["tool_error"], 1);
        assert_eq!(row.event_rows[0]["latency_ms"], 42);
        assert_eq!(row.tool_rows[0]["tool_phase"], "response");
        assert!(!row.event_rows[0]["payload_json"]
            .as_str()
            .expect("payload json")
            .contains("veryLarge"));
    }

    #[test]
    fn oversized_assistant_part_lists_remain_raw_only() {
        let parts = (0..=MAX_ASSISTANT_PARTS)
            .map(|_| json!({"text": "bounded"}))
            .collect::<Vec<_>>();
        let row = normalize(json!({
            "uuid": "assistant-oversized",
            "sessionId": "session-1",
            "timestamp": "2026-07-18T12:00:03Z",
            "type": "assistant",
            "message": {"role": "model", "parts": parts}
        }));

        assert!(!row.raw_row.is_null());
        assert!(row.event_rows.is_empty());
        assert!(row.link_rows.is_empty());
        assert!(row.tool_rows.is_empty());
    }

    #[test]
    fn unknown_and_snapshot_records_are_raw_only() {
        for record in [
            json!({
                "uuid": "unknown-1",
                "parentUuid": null,
                "sessionId": "session-1",
                "timestamp": "2026-07-18T12:00:03Z",
                "type": "future_record",
                "cwd": "/repo",
                "payload": {"text": "must not become searchable"}
            }),
            json!({
                "uuid": "snapshot-1",
                "parentUuid": "unknown-1",
                "sessionId": "session-1",
                "timestamp": "2026-07-18T12:00:04Z",
                "type": "system",
                "subtype": "file_history_snapshot",
                "cwd": "/repo",
                "systemPayload": {"snapshots": [{"content": "must remain raw"}]}
            }),
        ] {
            let row = normalize(record);
            assert!(!row.raw_row.is_null());
            assert!(row.event_rows.is_empty());
            assert!(row.link_rows.is_empty());
            assert!(row.tool_rows.is_empty());
        }
    }
}
