#![allow(dead_code)]

//! Generic source-authoring helpers for reading records and routing handlers.
//!
//! This module should stay grammar-agnostic. It is fine to centralize common
//! JSON access, top-level/payload lookups, and handler dispatch plumbing here;
//! Codex payload semantics, Claude content-block rules, Kimi wire events, and
//! Hermes segment parsing belong in their source modules.

use super::emitter::SourceEmitter;
use super::shared::{compact_json, to_u32, to_u64};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordViewError {
    MissingField {
        path: String,
    },
    UnexpectedType {
        path: String,
        expected: &'static str,
        actual: &'static str,
    },
}

impl std::fmt::Display for RecordViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField { path } => write!(f, "missing JSON field `{path}`"),
            Self::UnexpectedType {
                path,
                expected,
                actual,
            } => write!(
                f,
                "expected JSON field `{path}` to be {expected}, found {actual}"
            ),
        }
    }
}

impl std::error::Error for RecordViewError {}

pub(crate) type HandlerResult = Result<(), RecordViewError>;
pub(crate) type SourceRecordHandler<'record, 'ctx> =
    fn(RecordView<'record>, &mut SourceEmitter<'ctx>) -> HandlerResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchOutcome {
    Handled,
    Miss,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordView<'a> {
    value: &'a Value,
}

impl<'a> RecordView<'a> {
    pub(crate) fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub(crate) fn value(self) -> &'a Value {
        self.value
    }

    pub(crate) fn compact_json(self) -> String {
        compact_json(self.value)
    }

    pub(crate) fn top_type(self) -> Option<&'a str> {
        self.str_field("type")
    }

    pub(crate) fn top_type_or_empty(self) -> &'a str {
        self.top_type().unwrap_or_default()
    }

    pub(crate) fn payload_type(self) -> Option<&'a str> {
        self.payload_object().str_field("type")
    }

    pub(crate) fn payload_type_or_empty(self) -> &'a str {
        self.payload_type().unwrap_or_default()
    }

    pub(crate) fn payload(self) -> Option<&'a Value> {
        self.get("payload")
    }

    pub(crate) fn payload_object(self) -> ObjectView<'a> {
        ObjectView::new("payload", self.payload().and_then(Value::as_object))
    }

    pub(crate) fn object(self) -> ObjectView<'a> {
        ObjectView::new("$", self.value.as_object())
    }

    pub(crate) fn get(self, key: &str) -> Option<&'a Value> {
        self.value.as_object().and_then(|object| object.get(key))
    }

    pub(crate) fn field(self, key: &str) -> ValueView<'a> {
        ValueView::new(key.to_string(), self.get(key))
    }

    pub(crate) fn str_field(self, key: &str) -> Option<&'a str> {
        self.get(key).and_then(Value::as_str)
    }

    pub(crate) fn string_field(self, key: &str) -> String {
        self.field(key).string()
    }

    pub(crate) fn required_str(self, key: &str) -> Result<&'a str, RecordViewError> {
        self.field(key).required_str()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectView<'a> {
    path: String,
    object: Option<&'a Map<String, Value>>,
}

impl<'a> ObjectView<'a> {
    pub(crate) fn new(path: impl Into<String>, object: Option<&'a Map<String, Value>>) -> Self {
        Self {
            path: path.into(),
            object,
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<&'a Value> {
        self.object.and_then(|object| object.get(key))
    }

    pub(crate) fn field(&self, key: &str) -> ValueView<'a> {
        ValueView::new(format!("{}.{}", self.path, key), self.get(key))
    }

    pub(crate) fn str_field(&self, key: &str) -> Option<&'a str> {
        self.get(key).and_then(Value::as_str)
    }

    pub(crate) fn string_field(&self, key: &str) -> String {
        self.field(key).string()
    }

    pub(crate) fn u32_field(&self, key: &str) -> u32 {
        self.field(key).u32()
    }

    pub(crate) fn u64_field(&self, key: &str) -> u64 {
        self.field(key).u64()
    }

    pub(crate) fn object_field(&self, key: &str) -> ObjectView<'a> {
        ObjectView::new(
            format!("{}.{}", self.path, key),
            self.get(key).and_then(Value::as_object),
        )
    }

    pub(crate) fn array_field(&self, key: &str) -> ArrayView<'a> {
        ArrayView::new(
            format!("{}.{}", self.path, key),
            self.get(key).and_then(Value::as_array),
        )
    }

    pub(crate) fn required_str(&self, key: &str) -> Result<&'a str, RecordViewError> {
        self.field(key).required_str()
    }

    pub(crate) fn required_object(&self, key: &str) -> Result<ObjectView<'a>, RecordViewError> {
        let path = format!("{}.{}", self.path, key);
        match self.get(key) {
            Some(Value::Object(object)) => Ok(ObjectView::new(path, Some(object))),
            Some(value) => Err(RecordViewError::UnexpectedType {
                path,
                expected: "an object",
                actual: json_type(value),
            }),
            None => Err(RecordViewError::MissingField { path }),
        }
    }

    pub(crate) fn required_array(&self, key: &str) -> Result<ArrayView<'a>, RecordViewError> {
        self.field(key).required_array()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ValueView<'a> {
    path: String,
    value: Option<&'a Value>,
}

impl<'a> ValueView<'a> {
    pub(crate) fn new(path: String, value: Option<&'a Value>) -> Self {
        Self { path, value }
    }

    pub(crate) fn value(&self) -> Option<&'a Value> {
        self.value
    }

    pub(crate) fn str(&self) -> Option<&'a str> {
        self.value.and_then(Value::as_str)
    }

    pub(crate) fn string(&self) -> String {
        match self.value {
            Some(Value::String(value)) => value.clone(),
            Some(Value::Null) | None => String::new(),
            Some(value) => value.to_string(),
        }
    }

    pub(crate) fn u32(&self) -> u32 {
        to_u32(self.value)
    }

    pub(crate) fn u64(&self) -> u64 {
        to_u64(self.value)
    }

    pub(crate) fn bool(&self) -> Option<bool> {
        self.value.and_then(Value::as_bool)
    }

    pub(crate) fn object(&self) -> ObjectView<'a> {
        ObjectView::new("$", self.value.and_then(Value::as_object))
    }

    pub(crate) fn array(&self) -> ArrayView<'a> {
        ArrayView::new(self.path.clone(), self.value.and_then(Value::as_array))
    }

    pub(crate) fn required_str(&self) -> Result<&'a str, RecordViewError> {
        match self.value {
            Some(Value::String(value)) => Ok(value),
            Some(value) => Err(RecordViewError::UnexpectedType {
                path: self.path.clone(),
                expected: "a string",
                actual: json_type(value),
            }),
            None => Err(RecordViewError::MissingField {
                path: self.path.clone(),
            }),
        }
    }

    pub(crate) fn required_array(&self) -> Result<ArrayView<'a>, RecordViewError> {
        match self.value {
            Some(Value::Array(values)) => Ok(ArrayView::new(self.path.clone(), Some(values))),
            Some(value) => Err(RecordViewError::UnexpectedType {
                path: self.path.clone(),
                expected: "an array",
                actual: json_type(value),
            }),
            None => Err(RecordViewError::MissingField {
                path: self.path.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArrayView<'a> {
    path: String,
    values: Option<&'a Vec<Value>>,
}

impl<'a> ArrayView<'a> {
    pub(crate) fn new(path: String, values: Option<&'a Vec<Value>>) -> Self {
        Self { path, values }
    }

    pub(crate) fn values(&self) -> &'a [Value] {
        self.values.map(Vec::as_slice).unwrap_or(&[])
    }

    pub(crate) fn required_values(&self) -> Result<&'a [Value], RecordViewError> {
        self.values
            .map(Vec::as_slice)
            .ok_or_else(|| RecordViewError::MissingField {
                path: self.path.clone(),
            })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values().is_empty()
    }
}

pub(crate) fn dispatch_by_key<'record, 'ctx>(
    key: &str,
    view: RecordView<'record>,
    emitter: &mut SourceEmitter<'ctx>,
    routes: &[(&str, SourceRecordHandler<'record, 'ctx>)],
) -> Result<DispatchOutcome, RecordViewError> {
    if let Some((_, handler)) = routes.iter().find(|(route_key, _)| *route_key == key) {
        handler(view, emitter)?;
        Ok(DispatchOutcome::Handled)
    } else {
        Ok(DispatchOutcome::Miss)
    }
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::emitter::SourceEmitter;
    use crate::sources::shared::RecordContext;
    use serde_json::json;

    fn ctx() -> RecordContext<'static> {
        RecordContext {
            source_name: "dummy",
            harness: "codex",
            inference_provider: "openai",
            session_id: "session-1",
            session_date: "2026-05-07",
            source_file: "/fixtures/dummy/session.jsonl",
            source_inode: 7,
            source_generation: 1,
            source_line_no: 2,
            source_offset: 3,
            record_ts: "2026-05-07T00:00:00Z",
            event_ts: "2026-05-07 00:00:00.000",
        }
    }

    fn handle_message(view: RecordView<'_>, emitter: &mut SourceEmitter<'_>) -> HandlerResult {
        let payload = view.payload_object();
        let item_id = payload.required_str("id")?;
        let text = payload.string_field("text");
        let uid = emitter.uid(&view.compact_json(), "message");
        let event = emitter
            .event_for_json(
                &uid,
                "message",
                "message",
                "assistant",
                &text,
                view.payload().unwrap_or(view.value()),
            )
            .item_id(item_id)
            .content_types(["text"]);
        emitter.push_event(event);
        Ok(())
    }

    #[test]
    fn record_view_reads_top_level_and_payload_fields() {
        let record = json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "item-1",
                "turn_index": "7",
                "content": [{"type": "text"}]
            }
        });
        let view = RecordView::new(&record);
        let payload = view.payload_object();

        assert_eq!(view.top_type(), Some("response_item"));
        assert_eq!(view.payload_type(), Some("message"));
        assert_eq!(payload.required_str("id").unwrap(), "item-1");
        assert_eq!(payload.u32_field("turn_index"), 7);
        assert!(!payload.required_array("content").unwrap().is_empty());
    }

    #[test]
    fn record_view_reports_missing_and_wrong_type_fields() {
        let record = json!({"payload": {"id": 42}});
        let payload = RecordView::new(&record).payload_object();

        assert_eq!(
            payload.required_str("missing"),
            Err(RecordViewError::MissingField {
                path: "payload.missing".to_string()
            })
        );
        assert_eq!(
            payload.required_str("id"),
            Err(RecordViewError::UnexpectedType {
                path: "payload.id".to_string(),
                expected: "a string",
                actual: "a number"
            })
        );
    }

    #[test]
    fn dispatch_helper_routes_handlers_and_appends_rows() {
        let record = json!({
            "type": "message",
            "payload": {"id": "item-1", "text": "hello"}
        });
        let ctx = ctx();
        let view = RecordView::new(&record);
        let mut emitter = SourceEmitter::new(&ctx);
        let routes: &[(&str, SourceRecordHandler<'_, '_>)] = &[("message", handle_message)];

        let outcome =
            dispatch_by_key(view.top_type_or_empty(), view, &mut emitter, routes).unwrap();
        let partials = emitter.finish();

        assert_eq!(outcome, DispatchOutcome::Handled);
        assert_eq!(partials.event_rows.len(), 1);
        assert_eq!(
            partials.event_rows[0]
                .get("item_id")
                .and_then(Value::as_str),
            Some("item-1")
        );
        assert_eq!(
            partials.event_rows[0]
                .pointer("/content_types/0")
                .and_then(Value::as_str),
            Some("text")
        );
    }

    #[test]
    fn dispatch_helper_propagates_handler_errors_and_reports_misses() {
        let record = json!({"type": "message", "payload": {"text": "missing id"}});
        let ctx = ctx();
        let view = RecordView::new(&record);
        let mut emitter = SourceEmitter::new(&ctx);
        let routes: &[(&str, SourceRecordHandler<'_, '_>)] = &[("message", handle_message)];

        assert_eq!(
            dispatch_by_key(view.top_type_or_empty(), view, &mut emitter, routes),
            Err(RecordViewError::MissingField {
                path: "payload.id".to_string()
            })
        );
        assert_eq!(emitter.finish().event_rows.len(), 0);

        let mut emitter = SourceEmitter::new(&ctx);
        let unknown_record = json!({"type": "unknown"});
        let view = RecordView::new(&unknown_record);
        assert_eq!(
            dispatch_by_key(view.top_type_or_empty(), view, &mut emitter, routes),
            Ok(DispatchOutcome::Miss)
        );
    }
}
