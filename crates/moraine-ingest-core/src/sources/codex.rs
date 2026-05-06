use super::shared::*;
use super::{IngestSource, NormalizedPartials, SourceRecordContext};
use serde_json::{json, Map, Value};

pub(crate) static CODEX: Codex = Codex;

pub(crate) struct Codex;

impl IngestSource for Codex {
    fn harness(&self) -> &'static str {
        "codex"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        Some("openai")
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        if ctx.top_type == "session_meta" {
            let payload = record.get("payload").cloned().unwrap_or(Value::Null);
            let payload_id = to_str(payload.get("id"));
            if !payload_id.is_empty() {
                return payload_id;
            }
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
        base_uid: &str,
        model_hint: &str,
    ) -> NormalizedPartials {
        normalize_codex_event(record, ctx, top_type, base_uid, model_hint).into()
    }
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
                    mark_reasoning_metadata(&mut row);
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
                let canonical_buckets = openai_generation_token_buckets(usage);

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
                stamp_token_accounting(
                    &mut row,
                    "generation",
                    canonical_buckets,
                    token_native_units(&[]),
                );
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
                mark_reasoning_metadata(&mut row);
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
                    if kind == "reasoning" {
                        mark_reasoning_metadata(&mut row);
                    }
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
                mark_reasoning_metadata(&mut row);
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
