use super::emitter::SourceEmitter;
use super::shared::{
    compact_json, infer_vendor_from_base_url, parse_event_ts, to_str, to_u32, to_u64,
    RecordContext, TokenAccounting, TokenEndpointKind,
};
use super::{IngestSource, NormalizedPartials, SourceMetadata, SourceRecordContext};
use serde_json::Value;

pub(crate) struct NacSource;

pub(crate) static NAC: NacSource = NacSource;

impl IngestSource for NacSource {
    fn harness(&self) -> &'static str {
        "nac"
    }

    fn default_inference_provider(&self) -> Option<&'static str> {
        None
    }

    fn source_metadata(&self, record: &Value) -> SourceMetadata {
        SourceMetadata {
            inference_provider: infer_vendor_from_base_url(&to_str(record.get("base_url")))
                .to_string(),
            model_hint_fallback: to_str(record.get("model")),
        }
    }

    fn record_ts(&self, record: &Value) -> String {
        to_str(record.get("timestamp"))
    }

    fn top_type(&self, record: &Value) -> String {
        to_str(record.get("type"))
    }

    fn cwd(&self, record: &Value) -> String {
        if to_str(record.get("cwd_scope")) == "local" {
            to_str(record.get("cwd"))
        } else {
            String::new()
        }
    }

    fn session_id(&self, record: &Value, ctx: &SourceRecordContext<'_>) -> String {
        let id = to_str(record.get("session_id"));
        if id.is_empty() {
            ctx.session_hint.to_string()
        } else {
            id
        }
    }

    fn normalize(
        &self,
        record: &Value,
        ctx: &RecordContext<'_>,
        top_type: &str,
        _base_uid: &str,
        _model_hint: &str,
    ) -> NormalizedPartials {
        match top_type {
            "session_meta" => normalize_session_meta(record, ctx),
            "worker_session_meta" => normalize_worker_session_meta(record, ctx),
            "message" => normalize_message(record, ctx),
            "tool_request" => normalize_tool_request(record, ctx),
            "tool_response" => normalize_tool_response(record, ctx),
            "worker_event" => normalize_worker_event(record, ctx),
            _ => NormalizedPartials::default(),
        }
    }
}

fn stable_uid(emitter: &SourceEmitter<'_>, record: &Value, suffix: &str) -> String {
    let logical_id = to_str(record.get("logical_id"));
    emitter.uid(&logical_id, suffix)
}

fn apply_common_event_fields(
    event: super::emitter::EventBuilder,
    record: &Value,
) -> super::emitter::EventBuilder {
    let mut event = event;
    let model = to_str(record.get("model"));
    if !model.is_empty() {
        event = event.model(model);
    }
    let turn_index = to_u32(record.get("turn_index"));
    if turn_index > 0 {
        event = event.turn_index(turn_index);
    }
    event
}

fn token_accounting(record: &Value) -> TokenAccounting {
    let input = to_u64(record.get("input_tokens"));
    let output = to_u64(record.get("output_tokens"));
    let cache_read = to_u64(record.get("cache_read_tokens"));
    let cache_write = to_u64(record.get("cache_write_tokens"));
    let reasoning = to_u64(record.get("reasoning_tokens"));
    TokenAccounting::new(TokenEndpointKind::Generation)
        .with_buckets(&[
            (
                "input_text",
                input.saturating_sub(cache_read.saturating_add(cache_write)),
            ),
            ("output_text", output.saturating_sub(reasoning)),
            ("input_cache_read", cache_read),
            ("input_cache_write", cache_write),
            ("reasoning", reasoning),
        ])
        .with_legacy_scalars(input, output, cache_read, cache_write)
}

fn normalize_session_meta(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let uid = stable_uid(&emitter, record, "session_meta");
    let title = to_str(record.get("title"));
    let mut event = apply_common_event_fields(
        emitter.event_for_json(
            &uid,
            "session_meta",
            "session_meta",
            "system",
            &title,
            record,
        ),
        record,
    );
    let created_at = to_str(record.get("created_at"));
    if !created_at.is_empty() {
        // `record_ts` tracks the source's mutable session activity time, while
        // the table key needs a stable event timestamp so metadata updates
        // replace rather than duplicate the canonical session_meta row.
        event = event.event_ts(parse_event_ts(&created_at).0);
    }
    emitter.push_event(event);
    emitter.finish()
}

fn normalize_message(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let uid = stable_uid(&emitter, record, "message");
    let role = to_str(record.get("role"));
    let actor_kind = match role.as_str() {
        "assistant" => "assistant",
        "system" => "system",
        "tool" => "tool",
        _ => "user",
    };
    let text = to_str(record.get("content"));
    let payload_type = if record.get("reasoning").and_then(Value::as_bool) == Some(true) {
        "reasoning"
    } else {
        "message"
    };
    let mut event = apply_common_event_fields(
        emitter.event_for_json(&uid, "message", payload_type, actor_kind, &text, record),
        record,
    )
    .token_accounting(token_accounting(record))
    .latency_ms(to_u32(record.get("latency_ms")));
    if payload_type == "reasoning" {
        event = event.content_types(["reasoning"]);
    }
    emitter.push_event(event);
    emitter.finish()
}

fn normalize_tool_request(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let uid = stable_uid(&emitter, record, "tool_request");
    let call_id = to_str(record.get("tool_call_id"));
    let canonical_name = to_str(record.get("tool_name"));
    let raw_name = to_str(record.get("raw_tool_name"));
    let input = record.get("input").cloned().unwrap_or(Value::Null);
    let input_json = compact_json(&input);
    let event = apply_common_event_fields(
        emitter
            .event_for_json(&uid, "tool_call", "tool_use", "assistant", "", record)
            .tool_call_id(&call_id)
            .tool_name(&canonical_name)
            .tool_phase("request")
            .origin_tool_call_id(raw_name)
            .content_types(["tool_call"]),
        record,
    );
    emitter.push_event(event);
    emitter.push_tool_request(&uid, &call_id, "", &canonical_name, &input_json);
    emitter.finish()
}

fn normalize_tool_response(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let uid = stable_uid(&emitter, record, "tool_response");
    let request_uid = to_str(record.get("request_event_uid"));
    let call_id = to_str(record.get("tool_call_id"));
    let canonical_name = to_str(record.get("tool_name"));
    let output = record.get("output").cloned().unwrap_or(Value::Null);
    let output_json = compact_json(&output);
    let output_text = to_str(record.get("output_text"));
    let is_error = record
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let event = apply_common_event_fields(
        emitter
            .event_for_json(&uid, "tool_result", "tool_result", "tool", "", record)
            .tool_call_id(&call_id)
            .tool_name(&canonical_name)
            .tool_phase("response")
            .tool_error(u8::from(is_error))
            .origin_event_id(&request_uid)
            .content_types(["tool_result"]),
        record,
    );
    emitter.push_event(event);
    emitter.push_tool_response(
        &uid,
        &call_id,
        "",
        &canonical_name,
        u8::from(is_error),
        "",
        &output_json,
        &output_text,
    );
    if !request_uid.is_empty() {
        emitter.push_event_link(&uid, &request_uid, "tool_use_id", "{}");
    }
    emitter.finish()
}

fn normalize_worker_session_meta(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let uid = stable_uid(&emitter, record, "worker_session_meta");
    let title = to_str(record.get("thread_name"));
    let parent_session_id = to_str(record.get("parent_session_id"));
    let event = apply_common_event_fields(
        emitter
            .event_for_json(
                &uid,
                "session_meta",
                "session_meta",
                "system",
                &title,
                record,
            )
            .agent_label(&title)
            .substream(true),
        record,
    );
    emitter.push_event(event);
    emitter.push_external_link(&uid, &parent_session_id, "subagent_parent", "{}");
    emitter.finish()
}

fn normalize_worker_event(record: &Value, ctx: &RecordContext<'_>) -> NormalizedPartials {
    let mut emitter = SourceEmitter::new(ctx);
    let uid = stable_uid(&emitter, record, "worker_event");
    let action = to_str(record.get("action"));
    let actor_kind = if action == "action" {
        "user"
    } else {
        "assistant"
    };
    let text = to_str(record.get("content"));
    let label = to_str(record.get("thread_name"));
    let event = apply_common_event_fields(
        emitter
            .event_for_json(&uid, "message", "message", actor_kind, &text, record)
            .agent_label(label)
            .substream(true),
        record,
    );
    emitter.push_event(event);
    emitter.finish()
}

pub(crate) fn canonical_mcp_tool_name(raw: &str) -> String {
    const KNOWN_MORAINE_TOOLS: &[&str] = &[
        "search",
        "search_sessions",
        "open",
        "list_sessions",
        "file_attention",
    ];

    let Some(qualified) = raw.strip_prefix("mcp__moraine__") else {
        return raw.to_string();
    };
    let mut parts = qualified.split("__");
    let tool = parts.next().unwrap_or_default();
    let collision_suffix = parts.next();
    let has_extra_parts = parts.next().is_some();
    let valid_collision_suffix = collision_suffix.is_none_or(|suffix| {
        suffix
            .parse::<usize>()
            .is_ok_and(|collision_index| collision_index >= 2)
    });

    if KNOWN_MORAINE_TOOLS.contains(&tool) && !has_extra_parts && valid_collision_suffix {
        tool.to_string()
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::canonical_mcp_tool_name;

    #[test]
    fn canonicalizes_only_known_moraine_tools_and_allowed_collision_suffixes() {
        assert_eq!(canonical_mcp_tool_name("mcp__moraine__search"), "search");
        assert_eq!(
            canonical_mcp_tool_name("mcp__moraine__search_sessions__2"),
            "search_sessions"
        );
        assert_eq!(
            canonical_mcp_tool_name("mcp__moraine__unknown"),
            "mcp__moraine__unknown"
        );
        assert_eq!(
            canonical_mcp_tool_name("mcp__moraine__search__shadow"),
            "mcp__moraine__search__shadow"
        );
        assert_eq!(
            canonical_mcp_tool_name("mcp__moraine__search__1"),
            "mcp__moraine__search__1"
        );
    }

    #[test]
    fn preserves_non_moraine_tool_names_exactly_as_emitted() {
        assert_eq!(
            canonical_mcp_tool_name("mcp__github__search_code"),
            "mcp__github__search_code"
        );
        assert_eq!(canonical_mcp_tool_name("local_tool"), "local_tool");
        assert_eq!(
            canonical_mcp_tool_name(" mcp__github__search_code "),
            " mcp__github__search_code "
        );
    }
}
