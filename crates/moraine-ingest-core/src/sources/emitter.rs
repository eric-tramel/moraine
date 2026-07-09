#![allow(dead_code)]

use super::shared::{
    base_event_obj, build_event_link_row, build_external_link_row, build_tool_row, compact_json,
    event_uid, token_native_units, truncate_chars, RecordContext, TokenAccounting, PREVIEW_LIMIT,
    TEXT_LIMIT,
};
use super::NormalizedPartials;
use serde_json::{json, Map, Value};

pub(crate) struct SourceEmitter<'a> {
    ctx: &'a RecordContext<'a>,
    partials: NormalizedPartials,
}

impl<'a> SourceEmitter<'a> {
    pub(crate) fn new(ctx: &'a RecordContext<'a>) -> Self {
        Self {
            ctx,
            partials: NormalizedPartials::default(),
        }
    }

    pub(crate) fn uid(&self, record_fingerprint: &str, suffix: &str) -> String {
        event_uid(
            self.ctx.source_file,
            self.ctx.source_generation,
            self.ctx.source_line_no,
            self.ctx.source_offset,
            record_fingerprint,
            suffix,
        )
    }

    pub(crate) fn uid_for_json(&self, value: &Value, suffix: &str) -> String {
        self.uid(&compact_json(value), suffix)
    }

    pub(crate) fn event(
        &self,
        event_uid: &str,
        event_kind: &str,
        payload_type: &str,
        actor_kind: &str,
        text_content: &str,
        payload_json: &str,
    ) -> EventBuilder {
        EventBuilder::new(base_event_obj(
            self.ctx,
            event_uid,
            event_kind,
            payload_type,
            actor_kind,
            text_content,
            payload_json,
        ))
    }

    pub(crate) fn event_for_json(
        &self,
        event_uid: &str,
        event_kind: &str,
        payload_type: &str,
        actor_kind: &str,
        text_content: &str,
        payload: &Value,
    ) -> EventBuilder {
        self.event(
            event_uid,
            event_kind,
            payload_type,
            actor_kind,
            text_content,
            &compact_json(payload),
        )
    }

    pub(crate) fn push_event(&mut self, event: EventBuilder) -> &mut Self {
        self.partials.push_event(event.build());
        self
    }

    pub(crate) fn push_link(&mut self, row: Value) -> &mut Self {
        self.partials.push_link(row);
        self
    }

    pub(crate) fn push_tool(&mut self, row: Value) -> &mut Self {
        self.partials.push_tool(row);
        self
    }

    pub(crate) fn append(&mut self, partials: NormalizedPartials) -> &mut Self {
        self.partials.append(partials);
        self
    }

    pub(crate) fn event_link_row(
        &self,
        event_uid: &str,
        linked_event_uid: &str,
        link_type: &str,
        metadata_json: &str,
    ) -> Value {
        build_event_link_row(
            self.ctx,
            event_uid,
            linked_event_uid,
            link_type,
            metadata_json,
        )
    }

    pub(crate) fn external_link_row(
        &self,
        event_uid: &str,
        linked_external_id: &str,
        link_type: &str,
        metadata_json: &str,
    ) -> Value {
        build_external_link_row(
            self.ctx,
            event_uid,
            linked_external_id,
            link_type,
            metadata_json,
        )
    }

    pub(crate) fn push_event_link(
        &mut self,
        event_uid: &str,
        linked_event_uid: &str,
        link_type: &str,
        metadata_json: &str,
    ) -> &mut Self {
        let row = self.event_link_row(event_uid, linked_event_uid, link_type, metadata_json);
        self.push_link(row)
    }

    pub(crate) fn push_external_link(
        &mut self,
        event_uid: &str,
        linked_external_id: &str,
        link_type: &str,
        metadata_json: &str,
    ) -> &mut Self {
        let row = self.external_link_row(event_uid, linked_external_id, link_type, metadata_json);
        self.push_link(row)
    }

    pub(crate) fn tool_row(
        &self,
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
        build_tool_row(
            self.ctx,
            event_uid,
            tool_call_id,
            parent_tool_call_id,
            tool_name,
            tool_phase,
            tool_error,
            input_json,
            output_json,
            output_text,
        )
    }

    pub(crate) fn push_tool_request(
        &mut self,
        event_uid: &str,
        tool_call_id: &str,
        parent_tool_call_id: &str,
        tool_name: &str,
        input_json: &str,
    ) -> &mut Self {
        let row = self.tool_row(
            event_uid,
            tool_call_id,
            parent_tool_call_id,
            tool_name,
            "request",
            0,
            input_json,
            "",
            "",
        );
        self.push_tool(row)
    }

    pub(crate) fn push_tool_response(
        &mut self,
        event_uid: &str,
        tool_call_id: &str,
        parent_tool_call_id: &str,
        tool_name: &str,
        tool_error: u8,
        input_json: &str,
        output_json: &str,
        output_text: &str,
    ) -> &mut Self {
        let row = self.tool_row(
            event_uid,
            tool_call_id,
            parent_tool_call_id,
            tool_name,
            "response",
            tool_error,
            input_json,
            output_json,
            output_text,
        );
        self.push_tool(row)
    }

    pub(crate) fn finish(self) -> NormalizedPartials {
        self.partials
    }
}

pub(crate) struct EventBuilder {
    row: Map<String, Value>,
}

impl EventBuilder {
    fn new(row: Map<String, Value>) -> Self {
        Self { row }
    }

    pub(crate) fn event_uid(mut self, value: impl Into<String>) -> Self {
        self.string("event_uid", value);
        self
    }

    pub(crate) fn source_name(mut self, value: impl Into<String>) -> Self {
        self.string("source_name", value);
        self
    }

    pub(crate) fn harness(mut self, value: impl Into<String>) -> Self {
        self.string("harness", value);
        self
    }

    pub(crate) fn inference_provider(mut self, value: impl Into<String>) -> Self {
        self.string("inference_provider", value);
        self
    }

    pub(crate) fn record_ts(mut self, value: impl Into<String>) -> Self {
        self.string("record_ts", value);
        self
    }

    pub(crate) fn event_ts(mut self, value: impl Into<String>) -> Self {
        self.string("event_ts", value);
        self
    }

    pub(crate) fn event_kind(mut self, value: impl Into<String>) -> Self {
        self.string("event_kind", value);
        self
    }

    pub(crate) fn actor_kind(mut self, value: impl Into<String>) -> Self {
        self.string("actor_kind", value);
        self
    }

    pub(crate) fn payload_type(mut self, value: impl Into<String>) -> Self {
        self.string("payload_type", value);
        self
    }

    pub(crate) fn op_kind(mut self, value: impl Into<String>) -> Self {
        self.string("op_kind", value);
        self
    }

    pub(crate) fn op_status(mut self, value: impl Into<String>) -> Self {
        self.string("op_status", value);
        self
    }

    pub(crate) fn request_id(mut self, value: impl Into<String>) -> Self {
        self.string("request_id", value);
        self
    }

    pub(crate) fn trace_id(mut self, value: impl Into<String>) -> Self {
        self.string("trace_id", value);
        self
    }

    pub(crate) fn turn_index(mut self, value: u32) -> Self {
        self.u32("turn_index", value);
        self
    }

    pub(crate) fn item_id(mut self, value: impl Into<String>) -> Self {
        self.string("item_id", value);
        self
    }

    pub(crate) fn tool_call_id(mut self, value: impl Into<String>) -> Self {
        self.string("tool_call_id", value);
        self
    }

    pub(crate) fn parent_tool_call_id(mut self, value: impl Into<String>) -> Self {
        self.string("parent_tool_call_id", value);
        self
    }

    pub(crate) fn origin_event_id(mut self, value: impl Into<String>) -> Self {
        self.string("origin_event_id", value);
        self
    }

    pub(crate) fn origin_tool_call_id(mut self, value: impl Into<String>) -> Self {
        self.string("origin_tool_call_id", value);
        self
    }

    pub(crate) fn tool_name(mut self, value: impl Into<String>) -> Self {
        self.string("tool_name", value);
        self
    }

    pub(crate) fn tool_phase(mut self, value: impl Into<String>) -> Self {
        self.string("tool_phase", value);
        self
    }

    pub(crate) fn tool_error(mut self, value: u8) -> Self {
        self.u8("tool_error", value);
        self
    }

    pub(crate) fn agent_run_id(mut self, value: impl Into<String>) -> Self {
        self.string("agent_run_id", value);
        self
    }

    pub(crate) fn agent_label(mut self, value: impl Into<String>) -> Self {
        self.string("agent_label", value);
        self
    }

    pub(crate) fn coord_group_id(mut self, value: impl Into<String>) -> Self {
        self.string("coord_group_id", value);
        self
    }

    pub(crate) fn coord_group_label(mut self, value: impl Into<String>) -> Self {
        self.string("coord_group_label", value);
        self
    }

    pub(crate) fn substream(mut self, value: bool) -> Self {
        self.u8("is_substream", u8::from(value));
        self
    }

    pub(crate) fn model(mut self, value: impl Into<String>) -> Self {
        self.string("model", value);
        self
    }

    pub(crate) fn endpoint_kind(mut self, value: impl Into<String>) -> Self {
        self.string("endpoint_kind", value);
        self
    }

    pub(crate) fn input_tokens(mut self, value: u32) -> Self {
        self.u32("input_tokens", value);
        self
    }

    pub(crate) fn output_tokens(mut self, value: u32) -> Self {
        self.u32("output_tokens", value);
        self
    }

    pub(crate) fn cache_read_tokens(mut self, value: u32) -> Self {
        self.u32("cache_read_tokens", value);
        self
    }

    pub(crate) fn cache_write_tokens(mut self, value: u32) -> Self {
        self.u32("cache_write_tokens", value);
        self
    }

    pub(crate) fn latency_ms(mut self, value: u32) -> Self {
        self.u32("latency_ms", value);
        self
    }

    pub(crate) fn retry_count(mut self, value: u16) -> Self {
        self.row.insert("retry_count".to_string(), json!(value));
        self
    }

    pub(crate) fn service_tier(mut self, value: impl Into<String>) -> Self {
        self.string("service_tier", value);
        self
    }

    pub(crate) fn content_types<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.row.insert(
            "content_types".to_string(),
            Value::Array(
                values
                    .into_iter()
                    .map(|value| json!(value.into()))
                    .collect(),
            ),
        );
        self
    }

    pub(crate) fn has_reasoning(mut self, value: bool) -> Self {
        self.u8("has_reasoning", u8::from(value));
        self
    }

    pub(crate) fn text_content(mut self, value: &str) -> Self {
        let text_content = truncate_chars(value, TEXT_LIMIT);
        self.string("text_content", text_content.clone());
        self.string("text_preview", truncate_chars(&text_content, PREVIEW_LIMIT));
        self
    }

    pub(crate) fn payload_json(mut self, value: impl Into<String>) -> Self {
        self.string("payload_json", value);
        self
    }

    pub(crate) fn token_usage_json(mut self, value: impl Into<String>) -> Self {
        self.string("token_usage_json", value);
        self
    }

    pub(crate) fn token_usage_buckets(mut self, value: Value) -> Self {
        self.row.insert("token_usage_buckets".to_string(), value);
        self
    }

    pub(crate) fn token_usage_native_units(mut self, value: Value) -> Self {
        self.row
            .insert("token_usage_native_units".to_string(), value);
        self
    }

    pub(crate) fn zero_token_usage_native_units(self) -> Self {
        self.token_usage_native_units(token_native_units(&[]))
    }

    pub(crate) fn token_accounting(mut self, accounting: TokenAccounting) -> Self {
        accounting.stamp_event_row(&mut self.row);
        self
    }

    pub(crate) fn build(self) -> Value {
        Value::Object(self.row)
    }

    pub(crate) fn build_object(self) -> Map<String, Value> {
        self.row
    }

    fn string(&mut self, key: &str, value: impl Into<String>) {
        self.row
            .insert(key.to_string(), Value::String(value.into()));
    }

    fn u32(&mut self, key: &str, value: u32) {
        self.row.insert(key.to_string(), json!(value));
    }

    fn u8(&mut self, key: &str, value: u8) {
        self.row.insert(key.to_string(), json!(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::shared::{
        build_event_link_row, build_external_link_row, build_tool_row, event_uid, token_buckets,
    };

    fn ctx() -> RecordContext<'static> {
        RecordContext {
            source_name: "test-source",
            harness: "codex",
            inference_provider: "openai",
            session_id: "session-1",
            session_hint: "",
            session_date: "2026-05-07",
            cwd: "",
            source_file: "/fixtures/codex/session.jsonl",
            source_inode: 42,
            source_generation: 3,
            source_line_no: 7,
            source_offset: 91,
            record_ts: "2026-05-07T00:00:00Z",
            event_ts: "2026-05-07 00:00:00.000",
        }
    }

    fn stable_event_version(value: &mut Value) {
        if let Value::Object(row) = value {
            row.insert("event_version".to_string(), json!(0u64));
        }
    }

    #[test]
    fn emitter_uid_delegates_to_existing_material() {
        let ctx = ctx();
        let emitter = SourceEmitter::new(&ctx);
        let payload = json!({"alpha": 1, "beta": ["x", "y"]});
        let fingerprint = compact_json(&payload);

        assert_eq!(
            emitter.uid(&fingerprint, "message"),
            event_uid(
                ctx.source_file,
                ctx.source_generation,
                ctx.source_line_no,
                ctx.source_offset,
                &fingerprint,
                "message",
            )
        );
        assert_eq!(
            emitter.uid_for_json(&payload, "message"),
            event_uid(
                ctx.source_file,
                ctx.source_generation,
                ctx.source_line_no,
                ctx.source_offset,
                &fingerprint,
                "message",
            )
        );
    }

    #[test]
    fn event_builder_matches_base_event_obj_with_typed_setters() {
        let ctx = ctx();
        let emitter = SourceEmitter::new(&ctx);
        let buckets = token_buckets(&[("input_text", 11), ("output_text", 7), ("reasoning", 3)]);
        let native_units = token_native_units(&[]);
        let payload_json = r#"{"kind":"message"}"#;

        let mut expected = Value::Object(base_event_obj(
            &ctx,
            "event-1",
            "message",
            "message",
            "assistant",
            "hello",
            payload_json,
        ));
        if let Value::Object(row) = &mut expected {
            row.insert("turn_index".to_string(), json!(4u32));
            row.insert("item_id".to_string(), json!("item-1"));
            row.insert("request_id".to_string(), json!("req-1"));
            row.insert("trace_id".to_string(), json!("trace-1"));
            row.insert("op_kind".to_string(), json!("completion"));
            row.insert("op_status".to_string(), json!("ok"));
            row.insert("model".to_string(), json!("gpt-5.3-codex"));
            row.insert("tool_call_id".to_string(), json!("call-1"));
            row.insert("parent_tool_call_id".to_string(), json!("parent-call"));
            row.insert("origin_event_id".to_string(), json!("origin-event"));
            row.insert("origin_tool_call_id".to_string(), json!("origin-call"));
            row.insert("tool_name".to_string(), json!("shell"));
            row.insert("tool_phase".to_string(), json!("response"));
            row.insert("tool_error".to_string(), json!(1u8));
            row.insert("agent_run_id".to_string(), json!("run-1"));
            row.insert("agent_label".to_string(), json!("reviewer"));
            row.insert("coord_group_id".to_string(), json!("group-1"));
            row.insert("coord_group_label".to_string(), json!("workers"));
            row.insert("is_substream".to_string(), json!(1u8));
            row.insert("endpoint_kind".to_string(), json!("generation"));
            row.insert("input_tokens".to_string(), json!(11u32));
            row.insert("output_tokens".to_string(), json!(10u32));
            row.insert("cache_read_tokens".to_string(), json!(2u32));
            row.insert("cache_write_tokens".to_string(), json!(1u32));
            row.insert("latency_ms".to_string(), json!(250u32));
            row.insert("retry_count".to_string(), json!(2u16));
            row.insert("service_tier".to_string(), json!("flex"));
            row.insert("content_types".to_string(), json!(["text", "reasoning"]));
            row.insert("has_reasoning".to_string(), json!(1u8));
            row.insert("event_ts".to_string(), json!("2026-05-07 00:00:01.000"));
            row.insert("token_usage_json".to_string(), json!(r#"{"total":21}"#));
            row.insert("token_usage_buckets".to_string(), buckets.clone());
            row.insert("token_usage_native_units".to_string(), native_units.clone());
        }

        let mut actual = emitter
            .event(
                "event-1",
                "message",
                "message",
                "assistant",
                "hello",
                payload_json,
            )
            .turn_index(4)
            .item_id("item-1")
            .request_id("req-1")
            .trace_id("trace-1")
            .op_kind("completion")
            .op_status("ok")
            .model("gpt-5.3-codex")
            .tool_call_id("call-1")
            .parent_tool_call_id("parent-call")
            .origin_event_id("origin-event")
            .origin_tool_call_id("origin-call")
            .tool_name("shell")
            .tool_phase("response")
            .tool_error(1)
            .agent_run_id("run-1")
            .agent_label("reviewer")
            .coord_group_id("group-1")
            .coord_group_label("workers")
            .substream(true)
            .endpoint_kind("generation")
            .input_tokens(11)
            .output_tokens(10)
            .cache_read_tokens(2)
            .cache_write_tokens(1)
            .latency_ms(250)
            .retry_count(2)
            .service_tier("flex")
            .content_types(["text", "reasoning"])
            .has_reasoning(true)
            .event_ts("2026-05-07 00:00:01.000")
            .token_usage_json(r#"{"total":21}"#)
            .token_usage_buckets(buckets)
            .token_usage_native_units(native_units)
            .build();

        stable_event_version(&mut expected);
        stable_event_version(&mut actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn event_builder_stamps_token_accounting() {
        let ctx = ctx();
        let emitter = SourceEmitter::new(&ctx);
        let usage = json!({
            "input_tokens": 15,
            "output_tokens": 10,
            "input_tokens_details": {"cached_tokens": 3},
            "output_tokens_details": {"reasoning_tokens": 2}
        });
        let accounting = TokenAccounting::openai_generation(Some(&usage));

        let row = emitter
            .event("event-1", "message", "message", "assistant", "", "{}")
            .token_accounting(accounting)
            .build();

        assert_eq!(row.get("input_tokens").and_then(Value::as_u64), Some(15));
        assert_eq!(row.get("output_tokens").and_then(Value::as_u64), Some(10));
        assert_eq!(
            row.get("cache_read_tokens").and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            row.get("token_usage_json").and_then(Value::as_str),
            Some(compact_json(&usage).as_str())
        );
        assert_eq!(
            row.pointer("/token_usage_buckets/input_text")
                .and_then(Value::as_u64),
            Some(12)
        );
        assert_eq!(
            row.pointer("/token_usage_buckets/reasoning")
                .and_then(Value::as_u64),
            Some(2)
        );
    }

    #[test]
    fn link_and_tool_helpers_match_existing_row_builders() {
        let ctx = ctx();
        let mut emitter = SourceEmitter::new(&ctx);

        emitter
            .push_event_link("event-1", "event-0", "parent_event", "{}")
            .push_external_link("event-1", "call-1", "tool_call", "{}")
            .push_tool_request("event-1", "call-1", "", "shell", r#"{"cmd":"pwd"}"#)
            .push_tool_response(
                "event-2",
                "call-1",
                "",
                "shell",
                0,
                "",
                r#"{"exit":0}"#,
                "ok",
            );

        let mut partials = emitter.finish();
        let mut expected_link =
            build_event_link_row(&ctx, "event-1", "event-0", "parent_event", "{}");
        let mut expected_external =
            build_external_link_row(&ctx, "event-1", "call-1", "tool_call", "{}");
        let mut expected_request = build_tool_row(
            &ctx,
            "event-1",
            "call-1",
            "",
            "shell",
            "request",
            0,
            r#"{"cmd":"pwd"}"#,
            "",
            "",
        );
        let mut expected_response = build_tool_row(
            &ctx,
            "event-2",
            "call-1",
            "",
            "shell",
            "response",
            0,
            "",
            r#"{"exit":0}"#,
            "ok",
        );

        for value in [
            &mut expected_link,
            &mut expected_external,
            &mut expected_request,
            &mut expected_response,
        ] {
            stable_event_version(value);
        }
        for value in partials
            .link_rows
            .iter_mut()
            .chain(partials.tool_rows.iter_mut())
        {
            stable_event_version(value);
        }

        assert_eq!(partials.event_rows, Vec::<Value>::new());
        assert_eq!(partials.link_rows, vec![expected_link, expected_external]);
        assert_eq!(
            partials.tool_rows,
            vec![expected_request, expected_response]
        );
    }

    #[test]
    fn partials_push_and_append_preserve_row_order() {
        let mut left = NormalizedPartials::default();
        left.push_event(json!({"event_uid": "event-1"}));
        left.push_link(json!({"event_uid": "event-1"}));
        left.push_tool(json!({"tool_call_id": "call-1"}));

        let mut right = NormalizedPartials::default();
        right.push_event(json!({"event_uid": "event-2"}));
        right.push_link(json!({"event_uid": "event-2"}));
        right.push_tool(json!({"tool_call_id": "call-2"}));

        left.append(right);

        assert_eq!(
            left.event_rows,
            vec![
                json!({"event_uid": "event-1"}),
                json!({"event_uid": "event-2"})
            ]
        );
        assert_eq!(
            left.link_rows,
            vec![
                json!({"event_uid": "event-1"}),
                json!({"event_uid": "event-2"})
            ]
        );
        assert_eq!(
            left.tool_rows,
            vec![
                json!({"tool_call_id": "call-1"}),
                json!({"tool_call_id": "call-2"})
            ]
        );
    }
}
